/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.intentlog.fileio;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;
import java.util.zip.CRC32C;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.intentlog.Capabilities;
import org.intentlog.LogConflictException;
import org.intentlog.LogFullException;
import org.intentlog.LogNotSentException;
import org.intentlog.LogTail;
import org.intentlog.LogThrottledException;
import org.intentlog.LogView;
import org.intentlog.PageStore;
import org.intentlog.Receipt;
import org.intentlog.SharedLog;
import org.intentlog.wire.ProtoWire;

/**
 * An intention log in one object of a store with conditional writes ({@link
 * SupportsAtomicOperations}: S3 and S3 Express, GCS, ADLS): a header, a checkpoint, and the
 * records appended after it.
 *
 * <pre>
 * [Magic: 4 bytes "LCAT"][Version: 4 bytes, big-endian int, 3]
 * [Checkpoint length: varint][Checkpoint: a log.proto LogCheckpoint][Trailer: 20 bytes]
 * [Record length: varint][Record: a log.proto LogRecord][Trailer: 20 bytes] ...
 *
 * Trailer: [Incarnation: fixed64][Offset: fixed64][CRC32C: fixed32], little-endian
 * </pre>
 *
 * <p>Each frame's trailer names the object it was written into and where it starts (docs
 * TAIL_READS.md, "The format"): the incarnation, a random number each replace draws for the object
 * it writes and every frame in it, which an append copies from the version its condition names; the
 * offset of the frame's first byte; and a CRC32C of the frame's length, body, incarnation and
 * offset. A read checks every frame: one incarnation throughout, each offset where the frame before
 * ended, each checksum. A read that fails a check was torn by a write that raced it, or served
 * wrong by the store; it fails as {@link FrameCheckException}, and the log reads again.
 *
 * <p>An append is conditioned on the object's length and version (S3 Express's write offset, an
 * Azure Append Blob's position); a replace writes a new object conditioned on the version it
 * replaces (an ETag or generation), or on its absence. Every replace writes a checkpoint and the
 * records after it in one write, so each record follows exactly one checkpoint. A store without
 * append (GCS, or a store whose {@code supportsAppend()} is false) commits by replace only, and a
 * record too large for one append to the store ({@code maxAppendBytes}, less the record's length
 * prefix and trailer: {@link Capabilities#maxRecordBytes}) goes in a replace.
 *
 * <p>A read through a write reads nothing when the write says what the object holds: a replace
 * holds what it wrote, and an append conditioned on a tail whose bytes this log read or wrote holds
 * those bytes and the record (errata D20). The condition names the version the tail is bound to,
 * which no write since can have left in place (each write lands a record with an id of its own, so
 * no version recurs); a read of the object between the lookup that bound it and the append read
 * that version. An append from a tail that only {@link #probe} found reads the object back.
 *
 * <p>A read is of one version (SupportsAtomicOperations): the lookup that binds it chooses the
 * version, and the GET asks for it. A write that lands between them fails the GET as stale ({@link
 * SupportsAtomicOperations.StaleReadException}) rather than return part of another object (errata
 * T6), and the log reads again. A store that breaks that contract (S5) serves bytes the frames'
 * checks reject.
 *
 * <p>An append frames its record from the version its condition names: from the view of it this
 * log read or wrote, or, for a tail only a {@link #probe} found, from that version's last trailer,
 * read pinned to it. So every frame names the object it is in (TAIL_READS.md, "Why a tail read is
 * sound").
 *
 * <p>The pages of the committed set ({@link #pages}) are objects beside the log's, under {@code
 * <location>.committed/<name>}: a log that moves takes them along under the same names. Listing
 * them, for the sweep, needs a store that lists by prefix ({@link SupportsPrefixOperations}).
 */
public final class AtomicFileLog implements SharedLog {

  public static final byte[] MAGIC = "LCAT".getBytes(StandardCharsets.US_ASCII);
  public static final int FORMAT_VERSION = 3;
  /** Format 2: frames without trailers, which {@link #decode(byte[])} still reads, for tools. */
  public static final int UNCHECKED_FORMAT_VERSION = 2;
  public static final int HEADER_SIZE = 8; // magic (4) + version (4)
  /** A frame's trailer: incarnation (8), offset (8), CRC32C (4). */
  public static final int TRAILER_SIZE = 20;

  /**
   * How many times a read reads again when it fails as stale or fails the frames' checks. Each
   * failure is a write that landed in the round trip between the read's lookup and its GET, so a
   * run of them is a store serving no version it reports, not contention.
   */
  static final int READ_ATTEMPTS = 10;

  private final SupportsAtomicOperations io;
  private final String location;
  private final long maxRecordBytes;
  private final PageStore pages;
  private final LongSupplier incarnations;

  public AtomicFileLog(SupportsAtomicOperations io, String location) {
    this(io, location, () -> ThreadLocalRandom.current().nextLong());
  }

  /** A log whose replaces draw their incarnations from {@code incarnations} (tests: a seed). */
  public AtomicFileLog(SupportsAtomicOperations io, String location, LongSupplier incarnations) {
    this.io = io;
    this.location = location;
    this.incarnations = incarnations;
    // An append writes the record after its length, a varint no longer than the store's limit's,
    // and before its trailer
    long maxAppend = io.maxAppendBytes(location);
    int prefix = ProtoWire.varintSize((int) Math.min(maxAppend, Integer.MAX_VALUE));
    this.maxRecordBytes = maxAppend > 0 ? Math.max(1L, maxAppend - prefix - TRAILER_SIZE) : 0L;
    this.pages = new FilePages(location + ".committed/");
  }

  @Override
  public PageStore pages() {
    return pages;
  }

  /** The object's location. */
  @Override
  public String toString() {
    return location;
  }

  /** The tail of the object {@code file} names, as it is bound (or will bind on first use). */
  public static LogTail tailOf(InputFile file) {
    return new FileTail(file);
  }

  /** The object a tail is bound to, or will bind to: the version a read found. */
  public static InputFile fileOf(LogTail tail) {
    return ((FileTail) tail).file;
  }

  @Override
  public Capabilities capabilities() {
    return new Capabilities(io.supportsAppend(), true, true, false, maxRecordBytes);
  }

  @Override
  public LogView read() {
    for (int attempt = 1; ; attempt += 1) {
      try {
        return parse(io.newInputFile(location));
      } catch (SupportsAtomicOperations.StaleReadException | FrameCheckException e) {
        if (attempt >= READ_ATTEMPTS) {
          throw e;
        }
      }
    }
  }

  /**
   * The view the write left, if it says what the object holds; else the version it wrote, or, if a
   * later write superseded that (as a store that serves only the live version reports, or one whose
   * read of it fails the frames' checks), the live object, whose records hold the write's unless a
   * replace folded them into a checkpoint (P3).
   */
  @Override
  public LogView readThrough(Receipt receipt, Duration wait) {
    FileReceipt written = (FileReceipt) receipt;
    if (written.view != null) {
      return written.view;
    }
    try {
      return parse(written.written);
    } catch (SupportsAtomicOperations.StaleReadException | FrameCheckException e) {
      return read();
    }
  }

  /**
   * The log after {@code since} (docs/TAIL_READS.md "Reads"): if this log read or wrote the bytes
   * at {@code since} (incarnation X, length N), a lookup of the object's version V and length L,
   * and a ranged GET of V's bytes from N - 20. If those 20 bytes are the trailer of the frame
   * {@code since} ends with, V is X extended (every frame names its object, and an append extends
   * only the version its condition names), and the view is {@code since}'s checkpoint and
   * records, the same arrays, and the frames after N, each checked: the runtime folds only those.
   * Else the object was replaced, and the view is V's, read whole. A tail a probe found, or bytes
   * that fail a check or a read that fails as stale, are read whole or again as {@link #read}
   * does.
   *
   * <p>Under {@code intentlog.fileio.tail.check} (the test suites), each tail read is checked
   * against a whole read of the version it found: the trailer stands in for a comparison of the
   * bytes before N, and this makes its argument the thing the suites test.
   */
  @Override
  public LogView readAfter(LogTail since) {
    FileTail from = (FileTail) since;
    if (from.view == null || from.view.trailer.length != TRAILER_SIZE) {
      return read();
    }
    for (int attempt = 1; ; attempt += 1) {
      try {
        FileView view = tailRead(from);
        if (Boolean.getBoolean(CHECK_TAIL_READS)) {
          checkAgainstWhole(view);
        }
        return view;
      } catch (SupportsAtomicOperations.StaleReadException | FrameCheckException e) {
        if (attempt >= READ_ATTEMPTS) {
          throw e;
        }
      }
    }
  }

  /** The system property that checks each tail read against a whole read (tests). */
  public static final String CHECK_TAIL_READS = "intentlog.fileio.tail.check";

  /** One tail read from {@code from} (see {@link #readAfter}). */
  private FileView tailRead(FileTail from) {
    FileView held = from.view;
    long n = from.length;
    InputFile file = io.newInputFile(location);
    byte[] bytes;
    // Open, then look up, then read, as a whole read does: the lookup binds the version the GET
    // asks for (step 0)
    try (SeekableInputStream in = file.newStream()) {
      long length = file.getLength();
      if (length < n) {
        return (FileView) parse(file); // shorter: replaced
      }
      in.seek(n - TRAILER_SIZE);
      bytes = readFully(in, (int) (length - n + TRAILER_SIZE));
    } catch (EOFException e) {
      throw new FrameCheckException(location + ": " + e.getMessage());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    if (!Arrays.equals(bytes, 0, TRAILER_SIZE, held.trailer, 0, TRAILER_SIZE)) {
      return (FileView) parse(file); // not X's bytes at N: replaced
    }
    List<byte[]> records = new ArrayList<>(held.records);
    int end = TRAILER_SIZE;
    for (int pos = TRAILER_SIZE; pos < bytes.length; pos = end) {
      end = frameEnd(bytes, pos, n - TRAILER_SIZE, held.incarnation);
      records.add(body(bytes, pos, end));
    }
    FileTail tail = new FileTail(file);
    tail.length = n - TRAILER_SIZE + bytes.length;
    FileView view = new FileView(tail, held.checkpoint, Collections.unmodifiableList(records),
        held.incarnation, Arrays.copyOfRange(bytes, end - TRAILER_SIZE, end));
    tail.view = view;
    return view;
  }

  /**
   * Fails unless {@code view}, a tail read's, is what a whole read of the version it found holds:
   * the same checkpoint and records. A whole read that fails (the version was superseded since, or
   * a store that does not pin reads tore it) cannot say, and checks nothing.
   */
  private void checkAgainstWhole(FileView view) {
    LogView whole;
    try {
      whole = parse(fileOf(view.tail));
    } catch (SupportsAtomicOperations.StaleReadException | FrameCheckException
        | UncheckedIOException e) {
      return;
    }
    boolean same = Arrays.equals(whole.checkpoint().orElseThrow(), view.checkpoint)
        && whole.records().size() == view.records.size();
    for (int i = 0; same && i < view.records.size(); i++) {
      same = Arrays.equals(whole.records().get(i), view.records.get(i));
    }
    if (!same) {
      throw new IllegalStateException("A tail read of " + location + " disagrees with a whole read"
          + " of the version it found: " + view.records.size() + " records, against "
          + whole.records().size());
    }
  }

  @Override
  public LogTail probe() {
    FileTail tail = new FileTail(io.newInputFile(location));
    try {
      tail.bytes();
    } catch (RuntimeException e) {
      // Each store reports a missing object its own way. The log does not exist yet if the object
      // does not; else the object could not be read.
      if (tail.exists()) {
        throw e;
      }
    }
    return tail;
  }

  /**
   * Appends {@code record} at {@code expected}, framed as part of the version it names: with the
   * incarnation of the view this log read or wrote at that tail, or else (a tail a probe found) of
   * that version's last trailer, read pinned to it. A version that a write has superseded since
   * cannot take the append: its trailer's read fails as a lost race, the append unsent.
   */
  @Override
  public Receipt append(byte[] record, LogTail expected) {
    if (expected == null) {
      throw new IllegalArgumentException("An object store appends only at an expected offset");
    }
    FileTail at = (FileTail) expected;
    FileView before = at.view;
    long offset = at.bytes();
    long incarnation = before != null ? before.incarnation : incarnationAt(at);
    byte[] frame = frame(record, incarnation, offset);
    InputFile written = write(at.file, frame, AtomicOutputFile.Strategy.APPEND);
    if (before == null) {
      return new FileReceipt(written, null); // the bytes at the tail are unknown: read them back
    }
    List<byte[]> records = new ArrayList<>(before.records.size() + 1);
    records.addAll(before.records);
    records.add(record);
    return new FileReceipt(written, view(written, offset + frame.length, before.checkpoint,
        Collections.unmodifiableList(records), incarnation, trailerOf(frame)));
  }

  /**
   * The incarnation of the version {@code at} is bound to, from its last frame's trailer: a ranged
   * read of its last {@link #TRAILER_SIZE} bytes, pinned to that version (step 0), as the append's
   * condition is. A read that fails as stale, or that the store serves from another version, finds
   * the version gone, and so would the append: it fails as a lost race, unsent.
   */
  private long incarnationAt(FileTail at) {
    long length = at.bytes();
    if (length < HEADER_SIZE + 1 + TRAILER_SIZE) {
      throw new IllegalStateException(
          "Cannot append to " + location + ": " + length + " bytes hold no frame");
    }
    byte[] trailer;
    try (SeekableInputStream in = at.file.newStream()) {
      in.seek(length - TRAILER_SIZE);
      trailer = readFully(in, TRAILER_SIZE);
    } catch (SupportsAtomicOperations.StaleReadException | EOFException e) {
      throw new LogConflictException(
          "APPEND write to " + location + " lost a race: the version a probe found is gone", e);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return getLong(trailer, 0);
  }

  /**
   * Replaces the object, as an object of an incarnation drawn for this write. The log keeps {@code
   * checkpoint} and {@code records} as the view a read through the write returns: the caller must
   * not change them.
   */
  @Override
  public Receipt replace(LogTail expected, byte[] checkpoint, List<byte[]> records) {
    long incarnation = incarnations.getAsLong();
    byte[] bytes = encode(incarnation, checkpoint, records);
    InputFile written = write(fileOf(expected), bytes, AtomicOutputFile.Strategy.CAS);
    return new FileReceipt(written, view(written, bytes.length, checkpoint, List.copyOf(records),
        incarnation, Arrays.copyOfRange(bytes, bytes.length - TRAILER_SIZE, bytes.length)));
  }

  /**
   * The view of {@code file}: {@code length} bytes, holding {@code checkpoint} and {@code records},
   * of {@code incarnation}, ending in {@code trailer}.
   */
  private static FileView view(InputFile file, long length, byte[] checkpoint,
      List<byte[]> records, long incarnation, byte[] trailer) {
    FileTail tail = new FileTail(file);
    tail.length = length;
    FileView view = new FileView(tail, checkpoint, records, incarnation, trailer);
    tail.view = view;
    return view;
  }

  /**
   * The object a replace writes, as an object of a fresh incarnation: the header, {@code
   * checkpoint}, and {@code records} after it.
   */
  public static byte[] encode(byte[] checkpoint, List<byte[]> records) {
    return encode(ThreadLocalRandom.current().nextLong(), checkpoint, records);
  }

  /** The object a replace writes, as an object of {@code incarnation}. */
  public static byte[] encode(long incarnation, byte[] checkpoint, List<byte[]> records) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.writeBytes(MAGIC);
    out.write((FORMAT_VERSION >>> 24) & 0xFF);
    out.write((FORMAT_VERSION >>> 16) & 0xFF);
    out.write((FORMAT_VERSION >>> 8) & 0xFF);
    out.write(FORMAT_VERSION & 0xFF);
    out.writeBytes(frame(checkpoint, incarnation, out.size()));
    for (byte[] record : records) {
      out.writeBytes(frame(record, incarnation, out.size()));
    }
    return out.toByteArray();
  }

  /**
   * The object an append of {@code record} to {@code object} writes: its bytes and a frame of the
   * record, of its incarnation (its last trailer's), at its end. For tests and tools.
   */
  public static byte[] extend(byte[] object, byte[] record) {
    if (object.length < HEADER_SIZE + 1 + TRAILER_SIZE) {
      throw new IllegalArgumentException(object.length + " bytes hold no frame");
    }
    long incarnation = getLong(object, object.length - TRAILER_SIZE);
    byte[] frame = frame(record, incarnation, object.length);
    byte[] extended = Arrays.copyOf(object, object.length + frame.length);
    System.arraycopy(frame, 0, extended, object.length, frame.length);
    return extended;
  }

  /**
   * The checkpoint and records of an object's bytes, which an append may have extended past what
   * {@link #encode} wrote, its frames checked; format 2's, unchecked, for tools that read captured
   * logs. The view's tail is the bytes' length, bound to no object. Bytes that fail a check throw
   * {@link FrameCheckException}.
   */
  public static LogView decode(byte[] bytes) {
    return decode(bytes, new BytesTail(bytes.length), true);
  }

  /**
   * Writes {@code bytes} conditioned on {@code current}, mapping the store's signals onto the
   * log's: a throttle, a failed condition (a CAS conflict or a lost append race), a full object.
   * A failure to prepare the write sent nothing; any other failure leaves its outcome unknown.
   * Returns the version written.
   */
  private InputFile write(InputFile current, byte[] bytes, AtomicOutputFile.Strategy strategy) {
    AtomicOutputFile out;
    CAS token;
    ByteArrayInputStream source = new ByteArrayInputStream(bytes);
    try {
      out = io.newOutputFile(current);
      source.mark(bytes.length);
      token = out.prepare(() -> source, strategy);
      source.reset();
    } catch (IOException | RuntimeException e) {
      throw new LogNotSentException(strategy + " write to " + location + " failed: " + e, e);
    }
    try {
      return out.writeAtomic(token, () -> source);
    } catch (SupportsAtomicOperations.StorageThrottleException e) {
      throw new LogThrottledException("Storage throttled a write to " + location, e);
    } catch (SupportsAtomicOperations.SegmentLimitException e) {
      throw new LogFullException(location + " holds as many writes as the store allows", e);
    } catch (SupportsAtomicOperations.CASException | SupportsAtomicOperations.AppendException e) {
      throw new LogConflictException(strategy + " write to " + location + " lost a race", e);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private LogView parse(InputFile file) {
    // The bytes read must be the version the tail binds, as a replace from the view needs:
    // InMemoryAtomicFileIO binds the version as it opens the stream and reads its bytes; S3's and
    // ADLS's streams open lazily, after the lookup (getLength) binds the version, and ask for it
    // (If-Match), as GCS's do (if-generation-match): a write after the lookup fails the read as
    // stale. So the view an append's receipt holds is the object (D20). (Looking up before opening
    // would make a store that reads by version fail a read a write raced, where opening first reads
    // the version it binds.)
    // A version shorter than the lookup found is a store serving another version: a torn read.
    byte[] bytes;
    try (SeekableInputStream in = file.newStream()) {
      bytes = readFully(in, (int) file.getLength());
    } catch (EOFException e) {
      throw new FrameCheckException(location + ": " + e.getMessage());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    FileTail tail = new FileTail(file);
    tail.length = bytes.length;
    FileView view = decode(bytes, tail, false);
    tail.view = view;
    return view;
  }

  /**
   * The view of {@code bytes}, every frame checked: format 3's, or, if {@code unchecked} allows
   * it, format 2's (no trailers; the view's incarnation 0 and its trailer empty).
   */
  private static FileView decode(byte[] bytes, LogTail tail, boolean unchecked) {
    if (bytes.length < HEADER_SIZE) {
      throw new FrameCheckException("A catalog file of " + bytes.length + " bytes has no header");
    }
    for (int i = 0; i < MAGIC.length; i++) {
      if (bytes[i] != MAGIC[i]) {
        throw new FrameCheckException("Invalid magic bytes");
      }
    }
    int version =
        ((bytes[4] & 0xFF) << 24) | ((bytes[5] & 0xFF) << 16) | ((bytes[6] & 0xFF) << 8)
            | (bytes[7] & 0xFF);
    if (unchecked && version == UNCHECKED_FORMAT_VERSION) {
      return decodeUnchecked(bytes, tail);
    }
    if (version != FORMAT_VERSION) {
      throw new IllegalStateException(
          "Unsupported format version: " + version + ", expected: " + FORMAT_VERSION);
    }
    int pos = HEADER_SIZE;
    int end = frameEnd(bytes, pos, null);
    long incarnation = getLong(bytes, end - TRAILER_SIZE);
    byte[] checkpoint = body(bytes, pos, end);
    List<byte[]> records = new ArrayList<>();
    for (pos = end; pos < bytes.length; pos = end) {
      end = frameEnd(bytes, pos, incarnation);
      records.add(body(bytes, pos, end));
    }
    return new FileView(tail, checkpoint, Collections.unmodifiableList(records), incarnation,
        Arrays.copyOfRange(bytes, end - TRAILER_SIZE, end));
  }

  private static FileView decodeUnchecked(byte[] bytes, LogTail tail) {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes, HEADER_SIZE, bytes.length);
    try {
      byte[] checkpoint = ProtoWire.readLengthDelimitedBytes(in);
      List<byte[]> records = new ArrayList<>();
      while (in.available() > 0) {
        records.add(ProtoWire.readLengthDelimitedBytes(in));
      }
      return new FileView(tail, checkpoint, Collections.unmodifiableList(records), 0L,
          new byte[0]);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * The end of the frame at {@code pos} in {@code bytes}, after checking it: it lies within the
   * bytes, its trailer's offset is {@code pos}, its checksum holds, and its incarnation is {@code
   * incarnation} (any, if null).
   */
  private static int frameEnd(byte[] bytes, int pos, Long incarnation) {
    return frameEnd(bytes, pos, 0L, incarnation);
  }

  /**
   * As {@link #frameEnd(byte[], int, Long)}, for {@code bytes} that start at offset {@code base}
   * of the object: a frame at {@code pos} must say it starts at {@code base + pos}.
   */
  private static int frameEnd(byte[] bytes, int pos, long base, Long incarnation) {
    long length = 0;
    int at = pos;
    for (int shift = 0; ; shift += 7) {
      if (at >= bytes.length || shift > 28) {
        throw new FrameCheckException("The frame at " + (base + pos) + " has no length");
      }
      byte b = bytes[at++];
      length |= (long) (b & 0x7F) << shift;
      if (b >= 0) {
        break;
      }
    }
    long end = at + length + TRAILER_SIZE;
    if (end > bytes.length) {
      throw new FrameCheckException("The frame at " + (base + pos) + " runs past the object's end, "
          + (base + bytes.length));
    }
    int trailer = (int) end - TRAILER_SIZE;
    CRC32C crc = new CRC32C();
    crc.update(bytes, pos, trailer + 16 - pos);
    if ((int) crc.getValue() != getInt(bytes, trailer + 16)) {
      throw new FrameCheckException("The frame at " + (base + pos) + " fails its checksum");
    }
    long offset = getLong(bytes, trailer + 8);
    if (offset != base + pos) {
      throw new FrameCheckException(
          "The frame at " + (base + pos) + " says it starts at " + offset);
    }
    long named = getLong(bytes, trailer);
    if (incarnation != null && named != incarnation) {
      throw new FrameCheckException("The frame at " + (base + pos) + " is of incarnation "
          + Long.toHexString(named) + ", the object's " + Long.toHexString(incarnation));
    }
    return (int) end;
  }

  /** The body of the checked frame from {@code pos} to {@code end}. */
  private static byte[] body(byte[] bytes, int pos, int end) {
    int at = pos;
    while (bytes[at] < 0) {
      at++;
    }
    return Arrays.copyOfRange(bytes, at + 1, end - TRAILER_SIZE);
  }

  /**
   * {@code body} framed: its length, the body, and its trailer (the incarnation, {@code offset},
   * where the frame starts in its object, and the checksum).
   */
  private static byte[] frame(byte[] body, long incarnation, long offset) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(body.length + 5 + TRAILER_SIZE);
    try {
      ProtoWire.writeRawVarint(out, body.length);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    out.writeBytes(body);
    byte[] frame = Arrays.copyOf(out.toByteArray(), out.size() + TRAILER_SIZE);
    int trailer = frame.length - TRAILER_SIZE;
    putLong(frame, trailer, incarnation);
    putLong(frame, trailer + 8, offset);
    CRC32C crc = new CRC32C();
    crc.update(frame, 0, trailer + 16);
    putInt(frame, trailer + 16, (int) crc.getValue());
    return frame;
  }

  private static byte[] trailerOf(byte[] frame) {
    return Arrays.copyOfRange(frame, frame.length - TRAILER_SIZE, frame.length);
  }

  private static long getLong(byte[] bytes, int at) {
    long value = 0;
    for (int i = 7; i >= 0; i--) {
      value = (value << 8) | (bytes[at + i] & 0xFFL);
    }
    return value;
  }

  private static int getInt(byte[] bytes, int at) {
    return (bytes[at] & 0xFF) | (bytes[at + 1] & 0xFF) << 8 | (bytes[at + 2] & 0xFF) << 16
        | (bytes[at + 3] & 0xFF) << 24;
  }

  private static void putLong(byte[] bytes, int at, long value) {
    for (int i = 0; i < 8; i++) {
      bytes[at + i] = (byte) (value >>> (8 * i));
    }
  }

  private static void putInt(byte[] bytes, int at, int value) {
    for (int i = 0; i < 4; i++) {
      bytes[at + i] = (byte) (value >>> (8 * i));
    }
  }

  /**
   * A read whose bytes fail the format's checks (a frame that runs past the end, a checksum, an
   * offset or an incarnation): torn by a write that raced it, or served wrong by the store. The
   * log reads again ({@link #READ_ATTEMPTS}).
   */
  public static final class FrameCheckException extends IllegalStateException {
    public FrameCheckException(String message) {
      super(message);
    }
  }

  /** The pages of the committed set, as objects under {@code prefix}. */
  private final class FilePages implements PageStore {
    private final String prefix;

    FilePages(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public void put(String name, byte[] page) {
      try (PositionOutputStream out = io.newOutputFile(prefix + name).createOrOverwrite()) {
        out.write(page);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public Optional<byte[]> get(String name) {
      InputFile file = io.newInputFile(prefix + name);
      try (SeekableInputStream in = file.newStream()) {
        return Optional.of(readFully(in, (int) file.getLength()));
      } catch (NotFoundException e) {
        return Optional.empty();
      } catch (IOException | RuntimeException e) {
        // Each store reports a missing object its own way: a page does not exist if a lookup finds
        // none; else it could not be read
        if (!io.newInputFile(prefix + name).exists()) {
          return Optional.empty();
        }
        throw e instanceof IOException ? new UncheckedIOException((IOException) e)
            : (RuntimeException) e;
      }
    }

    @Override
    public void delete(String name) {
      io.deleteFile(prefix + name);
    }

    @Override
    public String location(String name) {
      return prefix + name;
    }

    @Override
    public List<String> list() {
      if (!(io instanceof SupportsPrefixOperations)) {
        throw new UnsupportedOperationException(io + " cannot list " + prefix);
      }
      List<String> names = new ArrayList<>();
      for (FileInfo file : ((SupportsPrefixOperations) io).listPrefix(prefix)) {
        String found = file.location();
        int at = found.lastIndexOf('/');
        String name = found.substring(at + 1);
        if (found.equals(prefix + name)) {
          names.add(name);
        }
      }
      return names;
    }
  }

  private static byte[] readFully(InputStream in, int length) throws IOException {
    byte[] bytes = new byte[length];
    int read = 0;
    while (read < length) {
      int n = in.read(bytes, read, length - read);
      if (n < 0) {
        throw new EOFException("Unexpected end of stream: read " + read + " of " + length);
      }
      read += n;
    }
    return bytes;
  }

  /**
   * The object a read or probe found, bound to its version: storage conditions the next write on
   * it. A tail continues another if the object did not shrink: an append makes it longer, while a
   * replace (compaction) usually makes it shorter. A replace that makes it longer reads as an
   * append, which costs a resend at most: the record is the same, and replay applies it once, by
   * its id or, once the replace took the id out of the committed set, by the floor.
   */
  private static final class FileTail implements LogTail {
    private final InputFile file;
    private long length = -1;
    // The frames of the version, if this log read or wrote them; null for a tail a probe found
    private FileView view;

    FileTail(InputFile file) {
      this.file = file;
    }

    /**
     * Whether the object existed at this tail. A tail whose bytes the log read or wrote saw it; the
     * store is not asked again, since it may answer for its version now (GCS looks up the
     * generation a write returned, which a later write removes).
     */
    @Override
    public boolean exists() {
      return view != null || file.exists();
    }

    @Override
    public long bytes() {
      if (length < 0) {
        length = file.getLength();
      }
      return length;
    }

    /**
     * Whether the object at this tail continues the one at {@code earlier}: no shorter, and, where
     * this log read or wrote the bytes at both, the same incarnation (a replace that made the
     * object longer does not continue it). A tail a probe found is compared by length: a resend
     * from it frames its record from the probed version's last trailer.
     */
    @Override
    public boolean continues(LogTail earlier) {
      if (earlier instanceof FileTail && view != null && ((FileTail) earlier).view != null
          && view.incarnation != ((FileTail) earlier).view.incarnation) {
        return false;
      }
      return bytes() >= earlier.bytes();
    }

    /**
     * The incarnation and the length, 16 bytes little-endian, if this log read or wrote the bytes
     * at this tail; null for a tail a probe found, whose incarnation is in bytes no read has seen.
     * A checkpoint here names none: a replace writes it with its records in one write, whose
     * trailers check the joint.
     */
    @Override
    public byte[] position() {
      if (view == null) {
        return null;
      }
      byte[] position = new byte[16];
      putLong(position, 0, view.incarnation);
      putLong(position, 8, length);
      return position;
    }
  }

  /** The tail of bytes that {@link #decode} read, bound to no object. */
  private static final class BytesTail implements LogTail {
    private final long length;

    BytesTail(long length) {
      this.length = length;
    }

    @Override
    public boolean exists() {
      return true;
    }

    @Override
    public long bytes() {
      return length;
    }

    @Override
    public boolean continues(LogTail earlier) {
      return length >= earlier.bytes();
    }
  }

  private static final class FileView implements LogView {
    private final LogTail tail;
    private final byte[] checkpoint;
    private final List<byte[]> records;
    // The object's incarnation, and its last frame's trailer: the bytes a tail read from this
    // view's end finds first, if the object is still this one
    private final long incarnation;
    private final byte[] trailer;

    FileView(LogTail tail, byte[] checkpoint, List<byte[]> records, long incarnation,
        byte[] trailer) {
      this.tail = tail;
      this.checkpoint = checkpoint;
      this.records = records;
      this.incarnation = incarnation;
      this.trailer = trailer;
    }

    @Override
    public LogTail tail() {
      return tail;
    }

    @Override
    public Optional<byte[]> checkpoint() {
      return Optional.of(checkpoint);
    }

    @Override
    public List<byte[]> records() {
      return records;
    }
  }

  /**
   * The version a write wrote, and the view of it if the write says what it holds; else a read
   * through the write reads it.
   */
  private static final class FileReceipt implements Receipt {
    private final InputFile written;
    private final FileView view;

    FileReceipt(InputFile written, FileView view) {
      this.written = written;
      this.view = view;
    }
  }
}
