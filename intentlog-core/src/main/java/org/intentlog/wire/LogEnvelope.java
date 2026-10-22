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
import static org.intentlog.wire.ProtoWire.readVarint;
import static org.intentlog.wire.ProtoWire.skipField;
import static org.intentlog.wire.ProtoWire.uuidToBytes;
import static org.intentlog.wire.ProtoWire.writeBytes;
import static org.intentlog.wire.ProtoWire.writeFixed64;
import static org.intentlog.wire.ProtoWire.writeVarint;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.UUID;

/**
 * The envelope of a log's checkpoints and records (log.proto): {@code LogCheckpoint} carries the
 * log's id, the committed set and the state machine's checkpoint; {@code LogRecord} carries a
 * record's id and the state machine's intention, or a GC of the committed set ({@code
 * AdvanceHorizon}). The state machine's bytes are opaque here.
 */
public final class LogEnvelope {

  /**
   * The envelope reader version this build supports, stamped on every checkpoint. A reader
   * refuses a checkpoint that needs more. Bump it only for a change an older reader cannot skip.
   */
  public static final int SUPPORTED_READER_VERSION = 1;

  public static final int LOG_CHECKPOINT_MIN_READER_VERSION = 1;
  public static final int LOG_CHECKPOINT_LOG_ID = 2;
  public static final int LOG_CHECKPOINT_COMMITTED = 3;
  public static final int LOG_CHECKPOINT_STATE = 4;

  public static final int LOG_RECORD_ID = 1;
  public static final int LOG_RECORD_INTENTION = 2;
  public static final int LOG_RECORD_ADVANCE_HORIZON = 3;

  public static final int ADVANCE_HORIZON_WATERMARK_MS = 1;

  private LogEnvelope() {}

  /** A decoded {@code LogCheckpoint}. */
  public static final class Checkpoint {
    private final UUID logId;
    private final byte[] committed;
    private final byte[] state;

    public Checkpoint(UUID logId, byte[] committed, byte[] state) {
      this.logId = logId;
      this.committed = committed;
      this.state = state;
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

  /** A decoded {@code LogRecord}: an intention with its id, or a GC record. */
  public static final class Record {
    private final UUID id;
    private final byte[] intention;
    private final long watermarkMs;

    private Record(UUID id, byte[] intention, long watermarkMs) {
      this.id = id;
      this.intention = intention;
      this.watermarkMs = watermarkMs;
    }

    public static Record intention(UUID id, byte[] intention) {
      return new Record(id, intention, -1L);
    }

    public static Record advanceHorizon(long watermarkMs) {
      return new Record(null, null, watermarkMs);
    }

    /** Whether this is a GC record ({@code AdvanceHorizon}) rather than an intention. */
    public boolean isAdvanceHorizon() {
      return intention == null;
    }

    /** The intention's id; null for a GC record. */
    public UUID id() {
      return id;
    }

    /** The intention; null for a GC record. */
    public byte[] intention() {
      return intention;
    }

    /** A GC record's watermark: it drops the committed ids at or below it. */
    public long watermarkMs() {
      return watermarkMs;
    }
  }

  /**
   * Encodes a {@code LogCheckpoint}. The committed set is omitted when {@code committed} is empty
   * (no ids and no horizon).
   */
  public static byte[] encodeCheckpoint(UUID logId, byte[] committed, byte[] state) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream(state.length + committed.length + 32);
      writeVarint(out, LOG_CHECKPOINT_MIN_READER_VERSION, SUPPORTED_READER_VERSION);
      writeBytes(out, LOG_CHECKPOINT_LOG_ID, uuidToBytes(logId));
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
      while (in.available() > 0) {
        int tag = readVarint(in);
        switch (tag >>> 3) {
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
      return new Checkpoint(logId, committed, state);
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

  /**
   * Decodes a {@code LogRecord}. Fails if it has no body, or more than one, or an intention
   * without an id.
   */
  public static Record decodeRecord(byte[] bytes) {
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(bytes);
      UUID id = null;
      byte[] intention = null;
      Long watermarkMs = null;
      while (in.available() > 0) {
        int tag = readVarint(in);
        switch (tag >>> 3) {
          case LOG_RECORD_ID:
            id = bytesToUuid(readLengthDelimitedBytes(in));
            break;
          case LOG_RECORD_INTENTION:
            intention = readLengthDelimitedBytes(in);
            break;
          case LOG_RECORD_ADVANCE_HORIZON:
            watermarkMs = decodeAdvanceHorizon(readLengthDelimitedBytes(in));
            break;
          default:
            skipField(in, tag & 0x7);
        }
      }
      check(intention != null || watermarkMs != null, "Record has no body");
      check(intention == null || watermarkMs == null, "Record has two bodies");
      if (watermarkMs != null) {
        return Record.advanceHorizon(watermarkMs);
      }
      check(id != null, "Record missing required id field");
      return Record.intention(id, intention);
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
