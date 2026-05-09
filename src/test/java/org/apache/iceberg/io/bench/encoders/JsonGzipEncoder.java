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

/**
 * Baseline: gzip-compressed JSON, mirroring Iceberg's existing
 * {@code TableMetadataParser.Codec.GZIP} output. The bar every other candidate
 * has to clear (Phase F path 1) or match on bytes while winning on lazy
 * decode (path 2).
 */
public final class JsonGzipEncoder extends JsonEncoderBase {

  public JsonGzipEncoder() {
    super("json-gzip");
  }

  @Override
  byte[] compress(byte[] in) {
    return EncoderUtil.gzip(in);
  }

  @Override
  byte[] decompress(byte[] in) {
    return EncoderUtil.gunzip(in);
  }
}
