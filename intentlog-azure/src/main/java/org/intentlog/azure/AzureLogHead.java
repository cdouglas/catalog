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
package org.intentlog.azure;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.intentlog.wire.ProtoWire;

/**
 * The log's head (AZURE_LOG.md "The head"; azure_log.proto's {@code AzureLogHead}): its id, its
 * live segments in order, and its newest checkpoint. Immutable; each change makes a new head,
 * written by CAS on the old one's ETag.
 *
 * <pre>
 * message AzureLogHead {
 *   int32  min_reader_version = 1;   // 1
 *   bytes  log_id             = 2;   // 16 bytes
 *   uint64 generation         = 3;   // +1 at every CAS
 *   repeated Segment segments = 4;   // oldest first, ordinals consecutive
 *   CheckpointRef checkpoint  = 5;
 * }
 * message Segment {
 *   uint64 ordinal = 1; string name = 2; uint64 closed_length = 3;  // absent while open
 *   uint64 limit = 4; uint64 base = 5;                                // its start in the log
 * }
 * message CheckpointRef { bytes through = 1; string name = 2; }     // through: 8 bytes, big-endian
 * </pre>
 */
final class AzureLogHead {

  static final int MIN_READER_VERSION = 1;

  /**
   * A segment: its ordinal and blob name, its final length once closed (-1 while open), its
   * limit L, and its base, the position in the log its offset 0 is (the bases of the segments
   * before it plus their closed lengths). Positions are bases plus offsets, so the end of a closed
   * segment and the start of the next are one position.
   */
  record Segment(long ordinal, String name, long closedLength, long limit, long base) {
    boolean closed() {
      return closedLength >= 0;
    }

    /** The writer limit W: every append's size condition. */
    long writerLimit() {
      return limit - Frames.RESERVE;
    }

    Segment closedAt(long length) {
      return new Segment(ordinal, name, length, limit, base);
    }
  }

  /** The newest checkpoint: the position it covers, and its blob. */
  record CheckpointRef(long through, String name) {}

  private final UUID logId;
  private final long generation;
  private final List<Segment> segments;
  private final CheckpointRef checkpoint;

  AzureLogHead(UUID logId, long generation, List<Segment> segments, CheckpointRef checkpoint) {
    if (segments.isEmpty()) {
      throw new IllegalArgumentException("A head names at least one segment");
    }
    this.logId = logId;
    this.generation = generation;
    this.segments = List.copyOf(segments);
    this.checkpoint = checkpoint;
  }

  UUID logId() {
    return logId;
  }

  long generation() {
    return generation;
  }

  List<Segment> segments() {
    return segments;
  }

  CheckpointRef checkpoint() {
    return checkpoint;
  }

  Segment last() {
    return segments.get(segments.size() - 1);
  }

  Segment first() {
    return segments.get(0);
  }

  /** The live segment of this ordinal, or null. */
  Segment byOrdinal(long ordinal) {
    long i = ordinal - first().ordinal();
    return i < 0 || i >= segments.size() ? null : segments.get((int) i);
  }

  /**
   * The live segment a read from {@code position} starts in: the one it lies in, or at the end of
   * a closed segment, the next (or that segment, while no next is named). Null if the position is
   * before the first live segment (trimmed) or past the log's known end.
   */
  Segment at(long position) {
    for (int i = 0; i < segments.size(); i++) {
      Segment s = segments.get(i);
      if (position < s.base()) {
        return null;
      }
      if (!s.closed() || position < s.base() + s.closedLength()
          || (position == s.base() + s.closedLength() && i == segments.size() - 1)) {
        return s;
      }
    }
    return null;
  }

  /** This head with {@code full} closed at {@code length} and {@code next} named after it. */
  AzureLogHead rotated(Segment full, long length, Segment next) {
    if (!last().equals(full) || next.ordinal() != full.ordinal() + 1
        || next.base() != full.base() + length) {
      throw new IllegalArgumentException("Not a rotation of " + full + " to " + next);
    }
    List<Segment> s = new ArrayList<>(segments.subList(0, segments.size() - 1));
    s.add(full.closedAt(length));
    s.add(next);
    return new AzureLogHead(logId, generation + 1, s, checkpoint);
  }

  AzureLogHead withCheckpoint(CheckpointRef newer) {
    return new AzureLogHead(logId, generation + 1, segments, newer);
  }

  /** This head without the segments before {@code ordinal}. */
  AzureLogHead trimmedBefore(long ordinal) {
    List<Segment> s = new ArrayList<>();
    for (Segment seg : segments) {
      if (seg.ordinal() >= ordinal) {
        s.add(seg);
      }
    }
    return new AzureLogHead(logId, generation + 1, s, checkpoint);
  }

  // ============================================================
  // Encoding
  // ============================================================

  byte[] encode() {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ProtoWire.writeVarint(out, 1, MIN_READER_VERSION);
      ProtoWire.writeBytes(out, 2, ProtoWire.uuidToBytes(logId));
      ProtoWire.writeVarint64(out, 3, generation);
      for (Segment s : segments) {
        ByteArrayOutputStream seg = new ByteArrayOutputStream();
        ProtoWire.writeVarint64(seg, 1, s.ordinal());
        ProtoWire.writeString(seg, 2, s.name());
        if (s.closed()) {
          ProtoWire.writeVarint64(seg, 3, s.closedLength());
        }
        ProtoWire.writeVarint64(seg, 4, s.limit());
        ProtoWire.writeVarint64(seg, 5, s.base());
        ProtoWire.writeLengthDelimited(out, 4, seg.toByteArray());
      }
      ByteArrayOutputStream ck = new ByteArrayOutputStream();
      ProtoWire.writeBytes(ck, 1, position(checkpoint.through()));
      ProtoWire.writeString(ck, 2, checkpoint.name());
      ProtoWire.writeLengthDelimited(out, 5, ck.toByteArray());
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static AzureLogHead decode(byte[] bytes) {
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(bytes);
      Integer minReader = null;
      UUID logId = null;
      long generation = 0;
      List<Segment> segments = new ArrayList<>();
      CheckpointRef checkpoint = null;
      while (in.available() > 0) {
        int tag = ProtoWire.readVarint(in);
        int field = tag >>> 3;
        int wire = tag & 7;
        switch (field) {
          case 1 -> minReader = (int) ProtoWire.readVarint64(in);
          case 2 -> logId = ProtoWire.bytesToUuid(ProtoWire.readLengthDelimitedBytes(in));
          case 3 -> generation = ProtoWire.readVarint64(in);
          case 4 -> segments.add(decodeSegment(ProtoWire.readLengthDelimitedBytes(in)));
          case 5 -> checkpoint = decodeCheckpoint(ProtoWire.readLengthDelimitedBytes(in));
          default -> ProtoWire.skipField(in, wire);
        }
        if (field == 1 && minReader > MIN_READER_VERSION) {
          throw new IllegalStateException("The log's head needs reader version " + minReader);
        }
      }
      if (minReader == null || logId == null || segments.isEmpty() || checkpoint == null) {
        throw new IllegalStateException("A malformed head");
      }
      return new AzureLogHead(logId, generation, segments, checkpoint);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Segment decodeSegment(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    long ordinal = 0;
    String name = null;
    long closed = -1;
    long limit = 0;
    long base = 0;
    while (in.available() > 0) {
      int tag = ProtoWire.readVarint(in);
      switch (tag >>> 3) {
        case 1 -> ordinal = ProtoWire.readVarint64(in);
        case 2 -> name = ProtoWire.readString(in);
        case 3 -> closed = ProtoWire.readVarint64(in);
        case 4 -> limit = ProtoWire.readVarint64(in);
        case 5 -> base = ProtoWire.readVarint64(in);
        default -> ProtoWire.skipField(in, tag & 7);
      }
    }
    if (name == null || limit <= 0) {
      throw new IllegalStateException("A malformed segment in the head");
    }
    return new Segment(ordinal, name, closed, limit, base);
  }

  private static CheckpointRef decodeCheckpoint(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    long through = -1;
    String name = null;
    while (in.available() > 0) {
      int tag = ProtoWire.readVarint(in);
      switch (tag >>> 3) {
        case 1 -> through = position(ProtoWire.readLengthDelimitedBytes(in));
        case 2 -> name = ProtoWire.readString(in);
        default -> ProtoWire.skipField(in, tag & 7);
      }
    }
    if (through < 0 || name == null) {
      throw new IllegalStateException("A malformed checkpoint reference in the head");
    }
    return new CheckpointRef(through, name);
  }

  /** A position as this log encodes it: 8 bytes, big-endian. */
  static byte[] position(long position) {
    return ByteBuffer.allocate(8).putLong(position).array();
  }

  static long position(byte[] bytes) {
    if (bytes == null || bytes.length != 8) {
      throw new IllegalArgumentException("Not a position of an Azure log");
    }
    return ByteBuffer.wrap(bytes).getLong();
  }

  @Override
  public String toString() {
    return "head " + generation + " of " + logId + ": " + segments + ", " + checkpoint;
  }
}
