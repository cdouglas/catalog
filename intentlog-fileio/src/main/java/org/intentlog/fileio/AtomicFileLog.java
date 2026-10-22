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
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.intentlog.Capabilities;
import org.intentlog.LogConflictException;
import org.intentlog.LogFullException;
import org.intentlog.LogNotSentException;
import org.intentlog.LogTail;
import org.intentlog.LogThrottledException;
import org.intentlog.LogView;
import org.intentlog.Receipt;
import org.intentlog.SharedLog;
import org.intentlog.wire.ProtoWire;

/**
 * An intention log in one object of a store with conditional writes ({@link
 * SupportsAtomicOperations}: S3 and S3 Express, GCS, ADLS): a header, a checkpoint, and the
 * records appended after it.
 *
 * <pre>
 * [Magic: 4 bytes "LCAT"][Version: 4 bytes, big-endian int]
 * [Checkpoint length: varint][Checkpoint: a log.proto LogCheckpoint]
 * [Record length: varint][Record: a log.proto LogRecord] ...
 * </pre>
 *
 * <p>An append is conditioned on the object's length and version (S3 Express's write offset, an
 * Azure Append Blob's position); a replace writes a new object conditioned on the version it
 * replaces (an ETag or generation), or on its absence. Every replace writes a checkpoint and the
 * records after it in one write, so each record follows exactly one checkpoint. A store without
 * append (GCS, or a store whose {@code supportsAppend()} is false) commits by replace only.
 */
public final class AtomicFileLog implements SharedLog {

  public static final byte[] MAGIC = "LCAT".getBytes(StandardCharsets.US_ASCII);
  public static final int FORMAT_VERSION = 2;
  public static final int HEADER_SIZE = 8; // magic (4) + version (4)

  private final SupportsAtomicOperations io;
  private final String location;

  public AtomicFileLog(SupportsAtomicOperations io, String location) {
    this.io = io;
    this.location = location;
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
    return new Capabilities(io.supportsAppend(), true, true, false, true, 0L);
  }

  @Override
  public LogView read() {
    return parse(io.newInputFile(location));
  }

  @Override
  public LogView readThrough(Receipt receipt, Duration wait) {
    return parse(((FileReceipt) receipt).written);
  }

  @Override
  public LogTail probe() {
    FileTail tail = new FileTail(io.newInputFile(location));
    tail.bytes();
    return tail;
  }

  @Override
  public Receipt append(byte[] record, LogTail expected) {
    if (expected == null) {
      throw new IllegalArgumentException("An object store appends only at an expected offset");
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream(record.length + 5);
    frame(out, record);
    return write(fileOf(expected), out.toByteArray(), AtomicOutputFile.Strategy.APPEND);
  }

  @Override
  public Receipt replace(LogTail expected, byte[] checkpoint, List<byte[]> records) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.writeBytes(MAGIC);
    out.write((FORMAT_VERSION >>> 24) & 0xFF);
    out.write((FORMAT_VERSION >>> 16) & 0xFF);
    out.write((FORMAT_VERSION >>> 8) & 0xFF);
    out.write(FORMAT_VERSION & 0xFF);
    frame(out, checkpoint);
    for (byte[] record : records) {
      frame(out, record);
    }
    return write(fileOf(expected), out.toByteArray(), AtomicOutputFile.Strategy.CAS);
  }

  /**
   * Writes {@code bytes} conditioned on {@code current}, mapping the store's signals onto the
   * log's: a throttle, a failed condition (a CAS conflict or a lost append race), a full object.
   * A failure to prepare the write sent nothing; any other failure leaves its outcome unknown.
   */
  private Receipt write(InputFile current, byte[] bytes, AtomicOutputFile.Strategy strategy) {
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
      return new FileReceipt(out.writeAtomic(token, () -> source));
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
    byte[] bytes;
    try (SeekableInputStream in = file.newStream()) {
      bytes = readFully(in, (int) file.getLength());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    if (bytes.length < HEADER_SIZE) {
      throw new IllegalStateException("A catalog file of " + bytes.length + " bytes has no header");
    }
    for (int i = 0; i < MAGIC.length; i++) {
      if (bytes[i] != MAGIC[i]) {
        throw new IllegalStateException("Invalid magic bytes");
      }
    }
    int version =
        ((bytes[4] & 0xFF) << 24) | ((bytes[5] & 0xFF) << 16) | ((bytes[6] & 0xFF) << 8)
            | (bytes[7] & 0xFF);
    if (version != FORMAT_VERSION) {
      throw new IllegalStateException(
          "Unsupported format version: " + version + ", expected: " + FORMAT_VERSION);
    }
    ByteArrayInputStream in = new ByteArrayInputStream(bytes, HEADER_SIZE, bytes.length);
    try {
      byte[] checkpoint = ProtoWire.readLengthDelimitedBytes(in);
      List<byte[]> records = new ArrayList<>();
      while (in.available() > 0) {
        records.add(ProtoWire.readLengthDelimitedBytes(in));
      }
      FileTail tail = new FileTail(file);
      tail.length = bytes.length;
      return new FileView(tail, checkpoint, records);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static byte[] readFully(InputStream in, int length) throws IOException {
    byte[] bytes = new byte[length];
    int read = 0;
    while (read < length) {
      int n = in.read(bytes, read, length - read);
      if (n < 0) {
        throw new IOException("Unexpected end of stream: read " + read + " of " + length);
      }
      read += n;
    }
    return bytes;
  }

  private static void frame(ByteArrayOutputStream out, byte[] message) {
    try {
      ProtoWire.writeRawVarint(out, message.length);
      out.write(message);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * The object a read or probe found, bound to its version: storage conditions the next write on
   * it. A tail continues another if the object did not shrink: an append makes it longer, while a
   * replace (compaction) usually makes it shorter. A replace that makes it longer reads as an
   * append, which costs a resend at most: the record is the same, and replay applies it once.
   */
  private static final class FileTail implements LogTail {
    private final InputFile file;
    private long length = -1;

    FileTail(InputFile file) {
      this.file = file;
    }

    @Override
    public boolean exists() {
      return file.exists();
    }

    @Override
    public long bytes() {
      if (length < 0) {
        length = file.getLength();
      }
      return length;
    }

    @Override
    public boolean continues(LogTail earlier) {
      return bytes() >= earlier.bytes();
    }
  }

  private static final class FileView implements LogView {
    private final FileTail tail;
    private final byte[] checkpoint;
    private final List<byte[]> records;

    FileView(FileTail tail, byte[] checkpoint, List<byte[]> records) {
      this.tail = tail;
      this.checkpoint = checkpoint;
      this.records = records;
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

  /** The version a write wrote, which a read through it reads. */
  private static final class FileReceipt implements Receipt {
    private final InputFile written;

    FileReceipt(InputFile written) {
      this.written = written;
    }
  }
}
