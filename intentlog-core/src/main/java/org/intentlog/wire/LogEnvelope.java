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
package org.intentlog.wire;

import static org.intentlog.wire.ProtoWire.bytesToUuid;
import static org.intentlog.wire.ProtoWire.readFixed64;
import static org.intentlog.wire.ProtoWire.readLengthDelimitedBytes;
import static org.intentlog.wire.ProtoWire.readString;
import static org.intentlog.wire.ProtoWire.readVarint;
import static org.intentlog.wire.ProtoWire.readVarint64;
import static org.intentlog.wire.ProtoWire.skipField;
import static org.intentlog.wire.ProtoWire.uuidToBytes;
import static org.intentlog.wire.ProtoWire.writeBytes;
import static org.intentlog.wire.ProtoWire.writeFixed64;
import static org.intentlog.wire.ProtoWire.writeString;
import static org.intentlog.wire.ProtoWire.writeVarint;
import static org.intentlog.wire.ProtoWire.writeVarint64;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.UUID;

/**
 * The envelope of a log's checkpoints and records (log.proto): {@code LogCheckpoint} carries the
 * log's id, the committed set and the state machine's checkpoint; {@code LogRecord} carries a
 * record's id and the state machine's intention, a GC of the committed set ({@code
 * AdvanceHorizon}), a {@code PageOut} of its oldest block, or, in a segmented log's catalog
 * object, a segment's {@code InstallSegment} or {@code RetiredSegment} (provider records, which
 * no view carries). {@code CommittedPage} is a page's body, an object apart from the log. The
 * state machine's bytes are opaque here.
 */
public final class LogEnvelope {

  /**
   * The envelope reader version this build supports, stamped on every checkpoint. A reader
   * refuses a checkpoint that needs more. Bump it only for a change an older reader cannot skip.
   */
  public static final int SUPPORTED_READER_VERSION = 2;

  public static final int LOG_CHECKPOINT_MIN_READER_VERSION = 1;
  public static final int LOG_CHECKPOINT_LOG_ID = 2;
  public static final int LOG_CHECKPOINT_COMMITTED = 3;
  public static final int LOG_CHECKPOINT_STATE = 4;
  public static final int LOG_CHECKPOINT_THROUGH = 5;

  public static final int LOG_RECORD_ID = 1;
  public static final int LOG_RECORD_INTENTION = 2;
  public static final int LOG_RECORD_ADVANCE_HORIZON = 3;
  public static final int LOG_RECORD_PAGE_OUT = 4;
  public static final int LOG_RECORD_INSTALL_SEGMENT = 5;
  public static final int LOG_RECORD_RETIRED_SEGMENT = 6;
  public static final int LOG_RECORD_CHECKPOINT_THROUGH = 7;

  public static final int INSTALL_SEGMENT_ORDINAL = 1;
  public static final int INSTALL_SEGMENT_NAME = 2;
  public static final int INSTALL_SEGMENT_LOCATION = 3;
  public static final int INSTALL_SEGMENT_LIMIT = 4;

  public static final int RETIRED_SEGMENT_ORDINAL = 1;
  public static final int RETIRED_SEGMENT_NAME = 2;
  public static final int RETIRED_SEGMENT_LOCATION = 3;
  public static final int RETIRED_SEGMENT_END = 4;
  public static final int RETIRED_SEGMENT_RETIRED_AT_MS = 5;

  public static final int ADVANCE_HORIZON_WATERMARK_MS = 1;

  public static final int PAGE_OUT_LEVEL = 1;
  public static final int PAGE_OUT_INDEX = 2;
  public static final int PAGE_OUT_COUNT = 3;
  public static final int PAGE_OUT_DIGEST = 4;

  public static final int COMMITTED_PAGE_MIN_READER_VERSION = 1;
  public static final int COMMITTED_PAGE_LOG_ID = 2;
  public static final int COMMITTED_PAGE_LEVEL = 3;
  public static final int COMMITTED_PAGE_INDEX = 4;
  public static final int COMMITTED_PAGE_PACKED_ENTRIES = 5;

  private LogEnvelope() {}

  /** A decoded {@code LogCheckpoint}. */
  public static final class Checkpoint {
    private final UUID logId;
    private final byte[] committed;
    private final byte[] state;
    private final byte[] through;

    public Checkpoint(UUID logId, byte[] committed, byte[] state) {
      this(logId, committed, state, null);
    }

    public Checkpoint(UUID logId, byte[] committed, byte[] state, byte[] through) {
      this.logId = logId;
      this.committed = committed;
      this.state = state;
      this.through = through;
    }

    /**
     * The position the checkpoint covers, as its log's provider encodes it ({@code
     * LogTail.position}); null for a checkpoint written with its records in one write.
     */
    public byte[] through() {
      return through;
    }

    public UUID logId() {
      return logId;
    }

    /** The {@code CommittedTransactions} body; empty if the checkpoint has none. */
    public byte[] committed() {
      return committed;
    }

    public byte[] state() {
      return state;
    }
  }

  /**
   * A decoded {@code LogRecord}: an intention with its id, a GC record, a PageOut, or a segmented
   * log's provider record (an Install or a Retired entry).
   */
  public static final class Record {
    private enum Kind {
      INTENTION, ADVANCE_HORIZON, PAGE_OUT, INSTALL_SEGMENT, RETIRED_SEGMENT, CHECKPOINT_THROUGH
    }

    private final Kind kind;
    private final UUID id;
    private final byte[] intention;
    private final long watermarkMs;
    private final PageOut pageOut;
    private final InstallSegment installSegment;
    private final RetiredSegment retiredSegment;
    private byte[] checkpointThrough = null;

    private Record(Kind kind, UUID id, byte[] intention, long watermarkMs, PageOut pageOut,
        InstallSegment installSegment, RetiredSegment retiredSegment) {
      this.kind = kind;
      this.id = id;
      this.intention = intention;
      this.watermarkMs = watermarkMs;
      this.pageOut = pageOut;
      this.installSegment = installSegment;
      this.retiredSegment = retiredSegment;
    }

    public static Record intention(UUID id, byte[] intention) {
      return new Record(Kind.INTENTION, id, intention, -1L, null, null, null);
    }

    public static Record advanceHorizon(long watermarkMs) {
      return new Record(Kind.ADVANCE_HORIZON, null, null, watermarkMs, null, null, null);
    }

    public static Record pageOut(PageOut pageOut) {
      return new Record(Kind.PAGE_OUT, null, null, -1L, pageOut, null, null);
    }

    public static Record installSegment(InstallSegment install) {
      return new Record(Kind.INSTALL_SEGMENT, null, null, -1L, null, install, null);
    }

    public static Record retiredSegment(RetiredSegment retired) {
      return new Record(Kind.RETIRED_SEGMENT, null, null, -1L, null, null, retired);
    }

    public static Record checkpointThrough(byte[] position) {
      Record r = new Record(Kind.CHECKPOINT_THROUGH, null, null, -1L, null, null, null);
      r.checkpointThrough = position;
      return r;
    }

    /** Whether this is an intention, rather than a record of the committed set (GC, PageOut). */
    public boolean isIntention() {
      return kind == Kind.INTENTION;
    }

    /** Whether this is a GC record ({@code AdvanceHorizon}). */
    public boolean isAdvanceHorizon() {
      return kind == Kind.ADVANCE_HORIZON;
    }

    /** Whether this is a {@code PageOut}. */
    public boolean isPageOut() {
      return kind == Kind.PAGE_OUT;
    }

    /**
     * Whether this is a segmented log's provider record: an Install, a Retired entry, or its
     * checkpoint's position.
     */
    public boolean isSegmentRecord() {
      return kind == Kind.INSTALL_SEGMENT || kind == Kind.RETIRED_SEGMENT
          || kind == Kind.CHECKPOINT_THROUGH;
    }

    /** The intention's id; null for any other record. */
    public UUID id() {
      return id;
    }

    /** The intention; null for any other record. */
    public byte[] intention() {
      return intention;
    }

    /** A GC record's watermark: it drops the committed ids at or below it. */
    public long watermarkMs() {
      return watermarkMs;
    }

    /** A PageOut's body; null for another record. */
    public PageOut pageOut() {
      return pageOut;
    }

    /** An Install's body; null for another record. */
    public InstallSegment installSegment() {
      return installSegment;
    }

    /** A Retired entry's body; null for another record. */
    public RetiredSegment retiredSegment() {
      return retiredSegment;
    }

    /** The position a segmented log's checkpoint covers; null for another record. */
    public byte[] checkpointThrough() {
      return checkpointThrough;
    }
  }

  /** A decoded {@code CommittedPage}: the body of a page of the committed set. */
  public static final class Page {
    private final UUID logId;
    private final int level;
    private final long index;
    private final byte[] packedEntries;

    public Page(UUID logId, int level, long index, byte[] packedEntries) {
      this.logId = logId;
      this.level = level;
      this.index = index;
      this.packedEntries = packedEntries;
    }

    public UUID logId() {
      return logId;
    }

    public int level() {
      return level;
    }

    public long index() {
      return index;
    }

    /** The sealed block's packed entries, as {@code CommittedBlock.packed_entries}. */
    public byte[] packedEntries() {
      return packedEntries;
    }

    /** The digest of the block the page holds, as a {@link PageRef} names it. */
    public byte[] digest() {
      return CommittedSetCodec.digest(level, index, packedEntries, 0, packedEntries.length);
    }
  }

  /**
   * Encodes a {@code LogCheckpoint}. The committed set is omitted when {@code committed} is empty
   * (no ids and no horizon).
   */
  public static byte[] encodeCheckpoint(UUID logId, byte[] committed, byte[] state) {
    return encodeCheckpoint(logId, committed, state, null);
  }

  /**
   * Encodes a {@code LogCheckpoint} that covers the position {@code through} (none if null), which
   * it writes before the committed set and the state, so that {@link #checkpointThrough} finds it
   * without reading them.
   */
  public static byte[] encodeCheckpoint(
      UUID logId, byte[] committed, byte[] state, byte[] through) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream(state.length + committed.length + 48);
      writeVarint(out, LOG_CHECKPOINT_MIN_READER_VERSION, SUPPORTED_READER_VERSION);
      writeBytes(out, LOG_CHECKPOINT_LOG_ID, uuidToBytes(logId));
      if (through != null) {
        writeBytes(out, LOG_CHECKPOINT_THROUGH, through);
      }
      if (committed.length > 0) {
        writeBytes(out, LOG_CHECKPOINT_COMMITTED, committed);
      }
      writeBytes(out, LOG_CHECKPOINT_STATE, state);
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Decodes a {@code LogCheckpoint}. Fails if it lacks {@code min_reader_version} or needs a
   * newer reader than this one, or lacks the log id or state.
   */
  public static Checkpoint decodeCheckpoint(byte[] bytes) {
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(bytes);
      boolean sawMinReaderVersion = false;
      UUID logId = null;
      byte[] committed = new byte[0];
      byte[] state = null;
      byte[] through = null;
      while (in.available() > 0) {
        int tag = readVarint(in);
        switch (tag >>> 3) {
          case LOG_CHECKPOINT_THROUGH:
            through = readLengthDelimitedBytes(in);
            break;
          case LOG_CHECKPOINT_MIN_READER_VERSION:
            int minReaderVersion = readVarint(in);
            if (minReaderVersion > SUPPORTED_READER_VERSION) {
              throw new IllegalStateException(
                  "Checkpoint requires log reader version "
                      + minReaderVersion
                      + " but this build supports up to "
                      + SUPPORTED_READER_VERSION
                      + "; the log was written by a newer build");
            }
            sawMinReaderVersion = true;
            break;
          case LOG_CHECKPOINT_LOG_ID:
            logId = bytesToUuid(readLengthDelimitedBytes(in));
            break;
          case LOG_CHECKPOINT_COMMITTED:
            committed = readLengthDelimitedBytes(in);
            break;
          case LOG_CHECKPOINT_STATE:
            state = readLengthDelimitedBytes(in);
            break;
          default:
            skipField(in, tag & 0x7);
        }
      }
      check(sawMinReaderVersion, "Checkpoint missing required min_reader_version field");
      check(logId != null, "Checkpoint missing required log_id field");
      check(state != null, "Checkpoint missing required state field");
      return new Checkpoint(logId, committed, state, through);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * The position a {@code LogCheckpoint} covers ({@code through}), or null if it names none: a
   * scan of its fields that skips the committed set and the state.
   */
  public static byte[] checkpointThrough(byte[] bytes) {
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(bytes);
      while (in.available() > 0) {
        int tag = readVarint(in);
        if (tag >>> 3 == LOG_CHECKPOINT_THROUGH) {
          return readLengthDelimitedBytes(in);
        }
        skipField(in, tag & 0x7);
      }
      return null;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Encodes a {@code LogRecord} that carries an intention. */
  public static byte[] encodeRecord(UUID id, byte[] intention) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream(intention.length + 24);
      writeBytes(out, LOG_RECORD_ID, uuidToBytes(id));
      writeBytes(out, LOG_RECORD_INTENTION, intention);
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Encodes a {@code LogRecord} that carries a GC of the committed set at {@code watermarkMs}. */
  public static byte[] encodeAdvanceHorizon(long watermarkMs) {
    try {
      ByteArrayOutputStream gc = new ByteArrayOutputStream(9);
      writeFixed64(gc, ADVANCE_HORIZON_WATERMARK_MS, watermarkMs);
      ByteArrayOutputStream out = new ByteArrayOutputStream(11);
      writeBytes(out, LOG_RECORD_ADVANCE_HORIZON, gc.toByteArray());
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Encodes a {@code LogRecord} that carries a {@code PageOut}. */
  public static byte[] encodePageOut(PageOut pageOut) {
    try {
      ByteArrayOutputStream body = new ByteArrayOutputStream(40);
      if (pageOut.level() != 0) {
        writeVarint(body, PAGE_OUT_LEVEL, pageOut.level());
      }
      if (pageOut.index() != 0L) {
        writeVarint64(body, PAGE_OUT_INDEX, pageOut.index());
      }
      if (pageOut.count() != 0) {
        writeVarint(body, PAGE_OUT_COUNT, pageOut.count());
      }
      writeBytes(body, PAGE_OUT_DIGEST, pageOut.digest());
      ByteArrayOutputStream out = new ByteArrayOutputStream(body.size() + 2);
      writeBytes(out, LOG_RECORD_PAGE_OUT, body.toByteArray());
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Encodes a {@code LogRecord} that carries a segmented log's {@code InstallSegment}. */
  public static byte[] encodeInstallSegment(InstallSegment install) {
    try {
      ByteArrayOutputStream body = new ByteArrayOutputStream(64);
      if (install.ordinal() != 0L) {
        writeVarint64(body, INSTALL_SEGMENT_ORDINAL, install.ordinal());
      }
      if (!install.name().isEmpty()) {
        writeString(body, INSTALL_SEGMENT_NAME, install.name());
      }
      if (!install.location().isEmpty()) {
        writeString(body, INSTALL_SEGMENT_LOCATION, install.location());
      }
      writeVarint64(body, INSTALL_SEGMENT_LIMIT, install.limit());
      ByteArrayOutputStream out = new ByteArrayOutputStream(body.size() + 2);
      writeBytes(out, LOG_RECORD_INSTALL_SEGMENT, body.toByteArray());
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Encodes a {@code LogRecord} that carries a segmented log's {@code RetiredSegment}. */
  public static byte[] encodeRetiredSegment(RetiredSegment retired) {
    try {
      ByteArrayOutputStream body = new ByteArrayOutputStream(80);
      if (retired.ordinal() != 0L) {
        writeVarint64(body, RETIRED_SEGMENT_ORDINAL, retired.ordinal());
      }
      if (!retired.name().isEmpty()) {
        writeString(body, RETIRED_SEGMENT_NAME, retired.name());
      }
      if (!retired.location().isEmpty()) {
        writeString(body, RETIRED_SEGMENT_LOCATION, retired.location());
      }
      if (retired.end().length > 0) {
        writeBytes(body, RETIRED_SEGMENT_END, retired.end());
      }
      if (retired.retiredAtMs() != 0L) {
        writeFixed64(body, RETIRED_SEGMENT_RETIRED_AT_MS, retired.retiredAtMs());
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream(body.size() + 2);
      writeBytes(out, LOG_RECORD_RETIRED_SEGMENT, body.toByteArray());
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Encodes a {@code LogRecord} that carries a segmented log's checkpoint position. */
  public static byte[] encodeCheckpointThrough(byte[] position) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream(position.length + 2);
      writeBytes(out, LOG_RECORD_CHECKPOINT_THROUGH, position);
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Decodes a {@code LogRecord}. Fails if it has no body, or more than one, or an intention
   * without an id, or a segment record with one.
   */
  public static Record decodeRecord(byte[] bytes) {
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(bytes);
      UUID id = null;
      byte[] intention = null;
      Long watermarkMs = null;
      PageOut pageOut = null;
      InstallSegment install = null;
      RetiredSegment retired = null;
      byte[] through = null;
      int bodies = 0;
      while (in.available() > 0) {
        int tag = readVarint(in);
        switch (tag >>> 3) {
          case LOG_RECORD_ID:
            id = bytesToUuid(readLengthDelimitedBytes(in));
            break;
          case LOG_RECORD_INTENTION:
            intention = readLengthDelimitedBytes(in);
            bodies++;
            break;
          case LOG_RECORD_ADVANCE_HORIZON:
            watermarkMs = decodeAdvanceHorizon(readLengthDelimitedBytes(in));
            bodies++;
            break;
          case LOG_RECORD_PAGE_OUT:
            pageOut = decodePageOut(readLengthDelimitedBytes(in));
            bodies++;
            break;
          case LOG_RECORD_INSTALL_SEGMENT:
            install = decodeInstallSegment(readLengthDelimitedBytes(in));
            bodies++;
            break;
          case LOG_RECORD_RETIRED_SEGMENT:
            retired = decodeRetiredSegment(readLengthDelimitedBytes(in));
            bodies++;
            break;
          case LOG_RECORD_CHECKPOINT_THROUGH:
            through = readLengthDelimitedBytes(in);
            bodies++;
            break;
          default:
            skipField(in, tag & 0x7);
        }
      }
      check(bodies > 0, "Record has no body");
      check(bodies == 1, "Record has two bodies");
      if (watermarkMs != null) {
        return Record.advanceHorizon(watermarkMs);
      }
      if (pageOut != null) {
        return Record.pageOut(pageOut);
      }
      if (install != null || retired != null || through != null) {
        check(id == null, "A segment record carries an id");
        return install != null ? Record.installSegment(install)
            : retired != null ? Record.retiredSegment(retired) : Record.checkpointThrough(through);
      }
      check(id != null, "Record missing required id field");
      return Record.intention(id, intention);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static InstallSegment decodeInstallSegment(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    long ordinal = 0L;
    String name = "";
    String location = "";
    long limit = 0L;
    while (in.available() > 0) {
      int tag = readVarint(in);
      switch (tag >>> 3) {
        case INSTALL_SEGMENT_ORDINAL:
          ordinal = readVarint64(in);
          break;
        case INSTALL_SEGMENT_NAME:
          name = readString(in);
          break;
        case INSTALL_SEGMENT_LOCATION:
          location = readString(in);
          break;
        case INSTALL_SEGMENT_LIMIT:
          limit = readVarint64(in);
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    check(limit >= 1, "An Install sets no record limit");
    check(!name.isEmpty() || !location.isEmpty(), "An Install names no segment");
    return new InstallSegment(ordinal, name, location, limit);
  }

  private static RetiredSegment decodeRetiredSegment(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    long ordinal = 0L;
    String name = "";
    String location = "";
    byte[] end = new byte[0];
    long retiredAtMs = 0L;
    while (in.available() > 0) {
      int tag = readVarint(in);
      switch (tag >>> 3) {
        case RETIRED_SEGMENT_ORDINAL:
          ordinal = readVarint64(in);
          break;
        case RETIRED_SEGMENT_NAME:
          name = readString(in);
          break;
        case RETIRED_SEGMENT_LOCATION:
          location = readString(in);
          break;
        case RETIRED_SEGMENT_END:
          end = readLengthDelimitedBytes(in);
          break;
        case RETIRED_SEGMENT_RETIRED_AT_MS:
          retiredAtMs = readFixed64(in);
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    check(!name.isEmpty() || !location.isEmpty(), "A Retired entry names no segment");
    return new RetiredSegment(ordinal, name, location, end, retiredAtMs);
  }

  private static PageOut decodePageOut(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int level = 0;
    long index = 0L;
    long count = 0L;
    byte[] digest = null;
    while (in.available() > 0) {
      int tag = readVarint(in);
      switch (tag >>> 3) {
        case PAGE_OUT_LEVEL:
          level = readVarint(in);
          break;
        case PAGE_OUT_INDEX:
          index = readVarint64(in);
          break;
        case PAGE_OUT_COUNT:
          count = readVarint64(in);
          break;
        case PAGE_OUT_DIGEST:
          digest = readLengthDelimitedBytes(in);
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    check(digest != null && digest.length == CommittedSetCodec.DIGEST_BYTES,
        "PageOut missing a digest of " + CommittedSetCodec.DIGEST_BYTES + " bytes");
    check(level >= 0 && level <= CommittedSetCodec.MAX_LEVEL && index >= 0
        && (level == CommittedSetCodec.MAX_LEVEL
            ? index == 0L : index < (1L << (CommittedSetCodec.MAX_LEVEL - level))),
        "PageOut of node (" + level + ", " + index + ") off the grid");
    check(count > 0 && count <= Integer.MAX_VALUE, "PageOut of " + count + " ids");
    return new PageOut(level, index, (int) count, digest);
  }

  /**
   * Encodes a {@code CommittedPage}: the body of the page of the block at node {@code (level,
   * index)} with packed entries {@code packedEntries}. A function of the log and the block alone.
   */
  public static byte[] encodePage(UUID logId, int level, long index, byte[] packedEntries) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream(packedEntries.length + 48);
      writeVarint(out, COMMITTED_PAGE_MIN_READER_VERSION, SUPPORTED_READER_VERSION);
      writeBytes(out, COMMITTED_PAGE_LOG_ID, uuidToBytes(logId));
      if (level != 0) {
        writeVarint(out, COMMITTED_PAGE_LEVEL, level);
      }
      if (index != 0L) {
        writeVarint64(out, COMMITTED_PAGE_INDEX, index);
      }
      writeBytes(out, COMMITTED_PAGE_PACKED_ENTRIES, packedEntries);
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Decodes a {@code CommittedPage}. Fails if it lacks {@code min_reader_version} or needs a newer
   * reader, or lacks the log id or entries.
   */
  public static Page decodePage(byte[] bytes) {
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(bytes);
      boolean sawMinReaderVersion = false;
      UUID logId = null;
      int level = 0;
      long index = 0L;
      byte[] packed = null;
      while (in.available() > 0) {
        int tag = readVarint(in);
        switch (tag >>> 3) {
          case COMMITTED_PAGE_MIN_READER_VERSION:
            int minReaderVersion = readVarint(in);
            check(minReaderVersion <= SUPPORTED_READER_VERSION,
                "Page requires log reader version " + minReaderVersion
                    + " but this build supports up to " + SUPPORTED_READER_VERSION);
            sawMinReaderVersion = true;
            break;
          case COMMITTED_PAGE_LOG_ID:
            logId = bytesToUuid(readLengthDelimitedBytes(in));
            break;
          case COMMITTED_PAGE_LEVEL:
            level = readVarint(in);
            break;
          case COMMITTED_PAGE_INDEX:
            index = readVarint64(in);
            break;
          case COMMITTED_PAGE_PACKED_ENTRIES:
            packed = readLengthDelimitedBytes(in);
            break;
          default:
            skipField(in, tag & 0x7);
        }
      }
      check(sawMinReaderVersion, "Page missing required min_reader_version field");
      check(logId != null, "Page missing required log_id field");
      check(packed != null && packed.length > 0, "Page missing its entries");
      return new Page(logId, level, index, packed);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static long decodeAdvanceHorizon(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    Long watermarkMs = null;
    while (in.available() > 0) {
      int tag = readVarint(in);
      if (tag >>> 3 == ADVANCE_HORIZON_WATERMARK_MS) {
        watermarkMs = readFixed64(in);
      } else {
        skipField(in, tag & 0x7);
      }
    }
    check(watermarkMs != null, "AdvanceHorizon missing required watermark_ms field");
    return watermarkMs;
  }

  private static void check(boolean condition, String message) {
    if (!condition) {
      throw new IllegalStateException(message);
    }
  }
}
