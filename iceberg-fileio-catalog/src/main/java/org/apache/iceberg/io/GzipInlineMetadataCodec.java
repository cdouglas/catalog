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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;

/**
 * Inline-TM codec wrapping Iceberg-style gzip(JSON). Compatible with anything
 * that knows how to read a {@code metadata.json.gz} payload: gunzip the bytes
 * and parse the result as a {@link TableMetadata} JSON document.
 *
 * <p>Lazy decode paths ({@link #decodeCurrentSnapshotOnly},
 * {@link #decodeRecentSnapshots}) materialise the full TableMetadata first
 * and then select — there is no structure-aware shortcut. They exist purely
 * for interface parity; the structural codec is the lazy-fast path.
 */
public final class GzipInlineMetadataCodec implements InlineMetadataCodec {

  @Override
  public byte tag() {
    return InlineMetadataCodecs.TAG_JSON_GZIP;
  }

  @Override
  public String shortName() {
    return "gzip";
  }

  @Override
  public byte[] encode(TableMetadata meta) {
    String json = TableMetadataParser.toJson(meta);
    return InlineMetadataIO.gzip(json.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public TableMetadata decodeFull(byte[] encoded, String metadataLocation) {
    String json = new String(InlineMetadataIO.gunzip(encoded), StandardCharsets.UTF_8);
    return TableMetadataParser.fromJson(metadataLocation, json);
  }

  @Override
  public Snapshot decodeCurrentSnapshotOnly(byte[] encoded) {
    TableMetadata meta = decodeFull(encoded, null);
    return meta.currentSnapshot();
  }

  @Override
  public List<Snapshot> decodeRecentSnapshots(byte[] encoded, int k) {
    TableMetadata meta = decodeFull(encoded, null);
    Snapshot current = meta.currentSnapshot();
    if (current == null) {
      return new ArrayList<>();
    }
    List<Snapshot> out = new ArrayList<>(k);
    Snapshot s = current;
    for (int i = 0; i < k && s != null; i++) {
      out.add(s);
      Long parentId = s.parentId();
      s = parentId == null ? null : meta.snapshot(parentId);
    }
    return out;
  }
}
