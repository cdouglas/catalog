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
package org.apache.iceberg.io.bench.encoders;

import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/** Compression helpers shared by the bench encoders. */
final class EncoderUtil {
  private EncoderUtil() {}

  static byte[] gzip(byte[] in) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(in.length / 4)) {
      try (GZIPOutputStream gz = new GZIPOutputStream(baos)) {
        gz.write(in);
      }
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("gzip compression failed", e);
    }
  }

  static byte[] gunzip(byte[] in) {
    try (GZIPInputStream gz = new GZIPInputStream(new ByteArrayInputStream(in));
        ByteArrayOutputStream baos = new ByteArrayOutputStream(in.length * 4)) {
      byte[] buf = new byte[8192];
      int n;
      while ((n = gz.read(buf)) > 0) {
        baos.write(buf, 0, n);
      }
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("gunzip failed", e);
    }
  }

  static byte[] zstd(byte[] in) {
    ZstdCompressor c = new ZstdCompressor();
    byte[] out = new byte[c.maxCompressedLength(in.length)];
    int len = c.compress(in, 0, in.length, out, 0, out.length);
    return Arrays.copyOf(out, len);
  }

  static byte[] unzstd(byte[] in) {
    int decompressedSize = Math.toIntExact(ZstdDecompressor.getDecompressedSize(in, 0, in.length));
    byte[] out = new byte[decompressedSize];
    new ZstdDecompressor().decompress(in, 0, in.length, out, 0, out.length);
    return out;
  }
}
