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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.bench.MetadataDecoder;

/**
 * Shared decode logic for the JSON-on-the-wire encoders. The codec choice
 * (gzip vs zstd) is the only thing that varies; everything else — JSON parsing,
 * lazy snapshot lookup, parent-chain walk — is common.
 */
abstract class JsonEncoderBase implements MetadataDecoder {

  static final ObjectMapper MAPPER = new ObjectMapper();

  private final String name;

  JsonEncoderBase(String name) {
    this.name = name;
  }

  @Override
  public final String name() {
    return name;
  }

  abstract byte[] compress(byte[] in);

  abstract byte[] decompress(byte[] in);

  @Override
  public byte[] encode(TableMetadata meta) {
    return compress(TableMetadataParser.toJson(meta).getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  @Override
  public TableMetadata decodeFull(byte[] encoded) {
    byte[] json = decompress(encoded);
    return TableMetadataParser.fromJson(new String(json, java.nio.charset.StandardCharsets.UTF_8));
  }

  @Override
  public Snapshot decodeCurrentSnapshotOnly(byte[] encoded) {
    byte[] json = decompress(encoded);
    try {
      JsonNode root = MAPPER.readTree(json);
      JsonNode currentNode = root.get("current-snapshot-id");
      if (currentNode == null || currentNode.isNull()) {
        return null;
      }
      long currentId = currentNode.asLong();
      JsonNode snapshots = root.get("snapshots");
      if (snapshots == null || !snapshots.isArray()) {
        return null;
      }
      for (JsonNode snap : snapshots) {
        JsonNode idNode = snap.get("snapshot-id");
        if (idNode != null && idNode.asLong() == currentId) {
          return SnapshotParser.fromJson(snap.toString());
        }
      }
      return null;
    } catch (IOException e) {
      throw new RuntimeException("decodeCurrentSnapshotOnly failed", e);
    }
  }

  @Override
  public List<Snapshot> decodeRecentSnapshots(byte[] encoded, int k) {
    byte[] json = decompress(encoded);
    try {
      JsonNode root = MAPPER.readTree(json);
      JsonNode currentNode = root.get("current-snapshot-id");
      if (currentNode == null || currentNode.isNull()) {
        return new ArrayList<>();
      }
      long currentId = currentNode.asLong();

      JsonNode snapshots = root.get("snapshots");
      if (snapshots == null || !snapshots.isArray()) {
        return new ArrayList<>();
      }
      // Build id -> JsonNode index. O(N) — required for chain walk regardless of encoding.
      Map<Long, JsonNode> byId = new HashMap<>(snapshots.size() * 2);
      for (JsonNode snap : snapshots) {
        JsonNode idNode = snap.get("snapshot-id");
        if (idNode != null) {
          byId.put(idNode.asLong(), snap);
        }
      }

      List<Snapshot> out = new ArrayList<>(k);
      Long id = currentId;
      for (int i = 0; i < k && id != null; i++) {
        JsonNode snap = byId.get(id);
        if (snap == null) {
          break;
        }
        out.add(SnapshotParser.fromJson(snap.toString()));
        JsonNode parentNode = snap.get("parent-snapshot-id");
        id = (parentNode != null && !parentNode.isNull()) ? parentNode.asLong() : null;
      }
      return out;
    } catch (IOException e) {
      throw new RuntimeException("decodeRecentSnapshots failed", e);
    }
  }
}
