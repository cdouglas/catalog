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
package org.apache.iceberg.io;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Objects;

/**
 * A {@link ByteArrayOutputStream} for one thread: its writes take no lock. The codecs write a
 * checkpoint a varint byte at a time, and the JDK's stream locks itself on every write.
 */
final class BytesOut extends ByteArrayOutputStream {

  BytesOut() {
    super(32);
  }

  BytesOut(int size) {
    super(size);
  }

  @Override
  public void write(int b) {
    if (count == buf.length) {
      buf = Arrays.copyOf(buf, Math.max(32, buf.length << 1));
    }
    buf[count++] = (byte) b;
  }

  @Override
  public void write(byte[] b, int off, int len) {
    Objects.checkFromIndexSize(off, len, b.length);
    if (count + len > buf.length) {
      buf = Arrays.copyOf(buf, Math.max(buf.length << 1, count + len));
    }
    System.arraycopy(b, off, buf, count, len);
    count += len;
  }

  @Override
  public void writeBytes(byte[] b) {
    write(b, 0, b.length);
  }

  @Override
  public byte[] toByteArray() {
    return Arrays.copyOf(buf, count);
  }

  @Override
  public int size() {
    return count;
  }

  @Override
  public void reset() {
    count = 0;
  }
}
