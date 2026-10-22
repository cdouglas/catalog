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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/** Shared gzip helpers for the inline-TM codecs. */
final class InlineMetadataIO {
  private InlineMetadataIO() {}

  static byte[] gzip(byte[] in) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(Math.max(64, in.length / 4))) {
      try (GZIPOutputStream gz = new GZIPOutputStream(baos)) {
        gz.write(in);
      }
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException("gzip compression failed", e);
    }
  }

  static byte[] gunzip(byte[] in) {
    try (GZIPInputStream gz = new GZIPInputStream(new ByteArrayInputStream(in));
        ByteArrayOutputStream baos = new ByteArrayOutputStream(Math.max(64, in.length * 4))) {
      byte[] buf = new byte[8192];
      int n;
      while ((n = gz.read(buf)) > 0) {
        baos.write(buf, 0, n);
      }
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException("gunzip failed", e);
    }
  }
}
