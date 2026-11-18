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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;

/**
 * A view's inline metadata (docs/VIEWS.md V4): Iceberg's view metadata JSON ({@link
 * ViewMetadataParser}), gzipped, tagged {@link InlineMetadataCodecs#TAG_JSON_GZIP}, the only codec
 * a view takes. Gzipped at the default level, not the fastest that tables' inline codec uses: a
 * checkpoint re-encodes changed tables, but never a view, whose bytes its writer encodes once, and
 * the default level makes a view's history of versions up to half the size (VIEWS.md "Sizes").
 */
final class InlineViewMetadata {
  private InlineViewMetadata() {}

  /** {@code metadata} as an inline view holds it. */
  static CatalogState.ViewMetadataRef encode(ViewMetadata metadata) {
    byte[] json = ViewMetadataParser.toJson(metadata).getBytes(StandardCharsets.UTF_8);
    return CatalogState.ViewMetadataRef.inline(gzip(json), InlineMetadataCodecs.TAG_JSON_GZIP);
  }

  /** The view metadata an inline view holds, bound to {@code metadataLocation}. */
  static ViewMetadata decode(byte[] encoded, byte codec, String metadataLocation) {
    Preconditions.checkArgument(
        codec == InlineMetadataCodecs.TAG_JSON_GZIP, "A view's inline codec is %s, not JSON", codec);
    String json = new String(InlineMetadataIO.gunzip(encoded), StandardCharsets.UTF_8);
    return ViewMetadataParser.fromJson(metadataLocation, json);
  }

  private static byte[] gzip(byte[] in) {
    try (ByteArrayOutputStream out = new BytesOut(Math.max(64, in.length / 4))) {
      try (GZIPOutputStream gz =
          new GZIPOutputStream(out) {
            {
              def.setLevel(Deflater.DEFAULT_COMPRESSION);
            }
          }) {
        gz.write(in);
      }
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException("gzip compression failed", e);
    }
  }
}
