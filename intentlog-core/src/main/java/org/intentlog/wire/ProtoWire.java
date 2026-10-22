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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Protobuf wire primitives, written by hand: a field tag is {@code (fieldNumber << 3) |
 * wireType}. The codecs built on these write zero-valued fields and unpacked repeated scalars, and
 * a negative int32 as a 5-byte varint (protoc readers accept all three).
 */
public final class ProtoWire {

  public static final int WIRE_VARINT = 0;
  public static final int WIRE_64BIT = 1;
  public static final int WIRE_LENGTH_DELIMITED = 2;
  public static final int WIRE_32BIT = 5;

  private ProtoWire() {}

  public static void writeVarint(OutputStream out, int fieldNumber, int value) throws IOException {
    int tag = (fieldNumber << 3) | WIRE_VARINT;
    writeRawVarint(out, tag);
    writeRawVarint(out, value);
  }

  public static void writeString(OutputStream out, int fieldNumber, String value)
      throws IOException {
    writeLengthDelimited(out, fieldNumber, value.getBytes(StandardCharsets.UTF_8));
  }

  public static void writeBytes(OutputStream out, int fieldNumber, byte[] value)
      throws IOException {
    writeLengthDelimited(out, fieldNumber, value);
  }

  public static void writeLengthDelimited(OutputStream out, int fieldNumber, byte[] value)
      throws IOException {
    int tag = (fieldNumber << 3) | WIRE_LENGTH_DELIMITED;
    writeRawVarint(out, tag);
    writeRawVarint(out, value.length);
    out.write(value);
  }

  public static void writeVarint64(OutputStream out, int fieldNumber, long value)
      throws IOException {
    writeRawVarint(out, (fieldNumber << 3) | WIRE_VARINT);
    writeRawVarint64(out, value);
  }

  public static void writeFixed64(OutputStream out, int fieldNumber, long value)
      throws IOException {
    writeRawVarint(out, (fieldNumber << 3) | WIRE_64BIT);
    for (int i = 0; i < 8; i++) {
      out.write((int) (value & 0xFF));
      value >>>= 8;
    }
  }

  /** Writes an int as an unsigned varint: a negative value takes 5 bytes, not protoc's 10. */
  public static void writeRawVarint(OutputStream out, int value) throws IOException {
    while ((value & ~0x7F) != 0) {
      out.write((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    out.write(value);
  }

  public static void writeRawVarint64(OutputStream out, long value) throws IOException {
    while ((value & ~0x7FL) != 0) {
      out.write((int) (value & 0x7F) | 0x80);
      value >>>= 7;
    }
    out.write((int) value);
  }

  /** The number of bytes {@link #writeRawVarint(OutputStream, int)} writes for {@code value}. */
  public static int varintSize(int value) {
    int size = 1;
    while ((value & ~0x7F) != 0) {
      size++;
      value >>>= 7;
    }
    return size;
  }

  public static int readVarint(InputStream in) throws IOException {
    int result = 0;
    int shift = 0;
    int b;
    do {
      b = in.read();
      if (b < 0) {
        throw new IOException("Unexpected end of stream reading varint");
      }
      result |= (b & 0x7F) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    return result;
  }

  public static String readString(InputStream in) throws IOException {
    return new String(readLengthDelimitedBytes(in), StandardCharsets.UTF_8);
  }

  public static byte[] readLengthDelimitedBytes(InputStream in) throws IOException {
    int length = readVarint(in);
    byte[] bytes = new byte[length];
    int read = 0;
    while (read < length) {
      int n = in.read(bytes, read, length - read);
      if (n < 0) {
        throw new IOException("Unexpected end of stream");
      }
      read += n;
    }
    return bytes;
  }

  public static long readVarint64(InputStream in) throws IOException {
    long result = 0;
    int shift = 0;
    int b;
    do {
      b = in.read();
      if (b < 0) {
        throw new IOException("Unexpected end of stream");
      }
      result |= (long) (b & 0x7F) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    return result;
  }

  public static long readFixed64(InputStream in) throws IOException {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      int b = in.read();
      if (b < 0) {
        throw new IOException("Unexpected end of stream");
      }
      result |= (long) (b & 0xFF) << (i * 8);
    }
    return result;
  }

  public static void skipField(InputStream in, int wireType) throws IOException {
    switch (wireType) {
      case WIRE_VARINT:
        readVarint64(in);
        break;
      case WIRE_64BIT:
        in.skip(8);
        break;
      case WIRE_LENGTH_DELIMITED:
        int length = readVarint(in);
        in.skip(length);
        break;
      case WIRE_32BIT:
        in.skip(4);
        break;
      default:
        throw new IOException("Unknown wire type: " + wireType);
    }
  }

  public static byte[] uuidToBytes(UUID uuid) {
    ByteBuffer buffer = ByteBuffer.allocate(16);
    buffer.putLong(uuid.getMostSignificantBits());
    buffer.putLong(uuid.getLeastSignificantBits());
    return buffer.array();
  }

  public static UUID bytesToUuid(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    return new UUID(buffer.getLong(), buffer.getLong());
  }
}
