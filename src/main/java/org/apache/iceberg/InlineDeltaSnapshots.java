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
package org.apache.iceberg;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * Public factory for {@link BaseSnapshot} used by {@code InlineDeltaCodec}
 * during inline-TM delta replay. {@link BaseSnapshot} has a package-private
 * constructor; this class lives in the {@code org.apache.iceberg} package so
 * the codec can construct snapshots directly instead of hand-rolling snapshot
 * JSON just to parse it back through {@link SnapshotParser#fromJson(String)}.
 *
 * <p>The {@link #create} signature mirrors the field set
 * {@link SnapshotParser} writes — same field semantics, same null/optional
 * rules — so a snapshot built here is indistinguishable from one produced by
 * the JSON round-trip.
 */
public final class InlineDeltaSnapshots {
  private InlineDeltaSnapshots() {}

  /**
   * Constructs a {@link Snapshot} backed by an external manifest-list file.
   * The caller's {@code summary} map is split as
   * {@link SnapshotParser#fromJson(String)} would split it: the
   * {@code "operation"} entry becomes the snapshot's operation string,
   * remaining entries become the summary map.
   *
   * @param manifestList manifest-list location (may be the {@code inline://<id>}
   *     sentinel for inline-ML snapshots; the inline-ML reader path swaps the
   *     {@code BaseSnapshot} for an {@code InlineSnapshot} before consumers see it)
   */
  public static Snapshot create(
      long sequenceNumber,
      long snapshotId,
      Long parentId,
      long timestampMillis,
      Map<String, String> summary,
      Integer schemaId,
      String manifestList,
      Long firstRowId,
      Long addedRows,
      String keyId) {
    String operation = summary.getOrDefault("operation", "append");
    ImmutableMap.Builder<String, String> rest = ImmutableMap.builder();
    for (Map.Entry<String, String> entry : summary.entrySet()) {
      if (!"operation".equals(entry.getKey())) {
        rest.put(entry.getKey(), entry.getValue());
      }
    }
    return new BaseSnapshot(
        sequenceNumber,
        snapshotId,
        parentId,
        timestampMillis,
        operation,
        rest.build(),
        schemaId,
        manifestList,
        firstRowId,
        addedRows,
        keyId);
  }
}
