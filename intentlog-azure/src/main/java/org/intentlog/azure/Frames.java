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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * A segment's frames (AZURE_LOG.md "Segments"): each block one frame, a record or a pad.
 *
 * <pre>
 * frame   = varint body trailer        ; varint length n &gt; 0, then the n octets of a LogRecord
 * pad     = %x00 padlen filler trailer ; padlen u32le, the filler's length (zeros)
 * trailer = ordinal crc                ; the segment's ordinal (u64le), CRC32C (u32le) of all before
 * </pre>
 */
final class Frames {

  /** The trailer: the segment's ordinal and the CRC. */
  static final int TRAILER = 12;

  /** The smallest pad: its marker, its length and its trailer. */
  static final int PAD_MIN = 1 + 4 + TRAILER;

  /** How far below a segment's limit writers stop: room for a pad of any size. */
  static final int RESERVE = 32;

  /** The most bytes a frame adds to a record of up to 2 GiB. */
  static final int MAX_OVERHEAD = 5 + TRAILER;

  private Frames() {}

  /** One frame read: where it starts in its segment, its length, and its record (null: a pad). */
  record Frame(long offset, int length, byte[] body) {
    boolean pad() {
      return body == null;
    }
  }

  /** The frame of {@code body} in the segment {@code ordinal}. */
  static byte[] record(byte[] body, long ordinal) {
    if (body.length == 0) {
      throw new IllegalArgumentException("A record is never empty");
    }
    int head = varintSize(body.length);
    ByteBuffer b = ByteBuffer.allocate(head + body.length + TRAILER).order(ByteOrder.LITTLE_ENDIAN);
    putVarint(b, body.length);
    b.put(body);
    return seal(b, ordinal);
  }

  /** A pad of exactly {@code size} bytes in the segment {@code ordinal}. */
  static byte[] pad(long size, long ordinal) {
    if (size < PAD_MIN || size > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("No pad of " + size + " bytes");
    }
    ByteBuffer b = ByteBuffer.allocate((int) size).order(ByteOrder.LITTLE_ENDIAN);
    b.put((byte) 0);
    b.putInt((int) size - PAD_MIN);
    b.position((int) size - TRAILER);
    return seal(b, ordinal);
  }

  /** The length of {@code body}'s frame. */
  static int length(int bodyLength) {
    return varintSize(bodyLength) + bodyLength + TRAILER;
  }

  private static byte[] seal(ByteBuffer b, long ordinal) {
    b.putLong(ordinal);
    CRC32C crc = new CRC32C();
    crc.update(b.array(), 0, b.position());
    b.putInt((int) crc.getValue());
    return b.array();
  }

  /** The CRC a frame ends with. */
  static int crcOf(byte[] frame) {
    return ByteBuffer.wrap(frame, frame.length - 4, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
  }

  /**
   * The frames of {@code bytes}, which start at {@code from} in the segment {@code ordinal} and
   * end at a frame's end.
   *
   * @throws FrameCheckException if a frame fails its check, names another segment, or is cut off
   */
  static List<Frame> parse(byte[] bytes, int length, long from, long ordinal) {
    List<Frame> frames = new ArrayList<>();
    ByteBuffer b = ByteBuffer.wrap(bytes, 0, length).order(ByteOrder.LITTLE_ENDIAN);
    while (b.hasRemaining()) {
      int start = b.position();
      long at = from + start;
      int n = getVarint(b, at);
      byte[] body = null;
      if (n == 0) {
        need(b, 4, at);
        long filler = Integer.toUnsignedLong(b.getInt());
        need(b, filler, at);
        b.position(b.position() + (int) filler);
      } else {
        need(b, n, at);
        body = new byte[n];
        b.get(body);
      }
      need(b, TRAILER, at);
      int end = b.position();
      long frameOrdinal = b.getLong();
      int crc = b.getInt();
      CRC32C check = new CRC32C();
      check.update(bytes, start, end + 8 - start);
      if ((int) check.getValue() != crc) {
        throw new FrameCheckException("The frame at " + at + " of segment " + ordinal
            + " fails its check");
      }
      if (frameOrdinal != ordinal) {
        throw new FrameCheckException("The frame at " + at + " of segment " + ordinal
            + " names segment " + frameOrdinal);
      }
      frames.add(new Frame(at, b.position() - start, body));
    }
    return frames;
  }

  private static void need(ByteBuffer b, long n, long at) {
    if (b.remaining() < n) {
      throw new FrameCheckException("The frame at " + at + " is cut off");
    }
  }

  static int varintSize(int value) {
    int n = 1;
    while ((value & ~0x7F) != 0) {
      value >>>= 7;
      n++;
    }
    return n;
  }

  private static void putVarint(ByteBuffer b, int value) {
    while ((value & ~0x7F) != 0) {
      b.put((byte) ((value & 0x7F) | 0x80));
      value >>>= 7;
    }
    b.put((byte) value);
  }

  private static int getVarint(ByteBuffer b, long at) {
    int value = 0;
    for (int shift = 0; shift < 32; shift += 7) {
      need(b, 1, at);
      byte x = b.get();
      value |= (x & 0x7F) << shift;
      if ((x & 0x80) == 0) {
        if (value < 0) {
          throw new FrameCheckException("The frame at " + at + " has a negative length");
        }
        return value;
      }
    }
    throw new FrameCheckException("The frame at " + at + " has a malformed length");
  }

  /** A read that found a frame it cannot trust: the reader reads again. */
  static final class FrameCheckException extends RuntimeException {
    FrameCheckException(String message) {
      super(message);
    }
  }
}
