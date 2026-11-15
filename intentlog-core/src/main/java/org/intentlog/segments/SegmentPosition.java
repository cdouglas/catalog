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
package org.intentlog.segments;

import java.nio.ByteBuffer;

/**
 * A place in a segmented log (docs/LOG_SEGMENTS.md "Positions"): the end of the last record read,
 * or a checkpoint's place when no record follows it. {@code records} counts the segment's records
 * before it, which is what a segment's limit counts; {@code offset} is the byte offset in the
 * segment. Encoded as 24 bytes, each a big-endian u64 ({@code LogTail.position}). Ordered by
 * ordinal, then records. A position is never normalized to the start of the next segment, so two
 * tails at one place have equal bytes.
 */
public record SegmentPosition(long ordinal, long records, long offset)
    implements Comparable<SegmentPosition> {

  public static final int BYTES = 24;

  /** The first position of a log: segment 0, before any record. */
  public static final SegmentPosition START = new SegmentPosition(0, 0, 0);

  public SegmentPosition {
    if (ordinal < 0 || records < 0 || offset < 0) {
      throw new IllegalArgumentException("No position " + ordinal + "/" + records + "/" + offset);
    }
  }

  public byte[] encode() {
    return ByteBuffer.allocate(BYTES).putLong(ordinal).putLong(records).putLong(offset).array();
  }

  public static SegmentPosition decode(byte[] bytes) {
    if (bytes == null || bytes.length != BYTES) {
      throw new IllegalArgumentException("A segment position is " + BYTES + " bytes");
    }
    ByteBuffer b = ByteBuffer.wrap(bytes);
    return new SegmentPosition(b.getLong(), b.getLong(), b.getLong());
  }

  @Override
  public int compareTo(SegmentPosition o) {
    int c = Long.compare(ordinal, o.ordinal);
    return c != 0 ? c : Long.compare(records, o.records);
  }

  public boolean after(SegmentPosition o) {
    return compareTo(o) > 0;
  }

  @Override
  public String toString() {
    return ordinal + "/" + records + "@" + offset;
  }
}
