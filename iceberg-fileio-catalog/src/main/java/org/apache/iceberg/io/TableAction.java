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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.io.InlineDeltaCodec.AddManifestUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.AddSnapshotUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.DecodedDelta;
import org.apache.iceberg.io.InlineDeltaCodec.DeltaUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.RemoveManifestUpdate;

/**
 * One table action of an intention record, as a conflict encoding sees it: the table, the version
 * its writer built on (its base), and its payload, decoded once for every encoding that asks.
 *
 * <ul>
 *   <li>{@link Kind#DELTA}: the table metadata updates of an inline table's delta, with the
 *       manifest updates of its new snapshots' lists (the snapshots it adds, the manifests each
 *       adds with their partition summaries and counts, the manifests it removes);
 *   <li>{@link Kind#FULL}: full inline metadata;
 *   <li>{@link Kind#LOCATION}: a metadata file's location (a pointer table's update, or an
 *       eviction);
 *   <li>{@link Kind#READ}: a read pin of a catalog transaction;
 *   <li>{@link Kind#RENAME}: a rename, which moves the table's version; observed only.
 * </ul>
 *
 * Only a delta can be finalized on a table that changed since its base; every other kind, and a
 * {@linkplain #pinned pinned} delta, has the Lamport witness.
 */
final class TableAction {

  enum Kind {
    DELTA,
    FULL,
    LOCATION,
    READ,
    RENAME
  }

  private final Kind kind;
  private final int tableId;
  private final int baseVersion;
  private final boolean pinned;
  private final byte[] deltaBytes;
  private DecodedDelta delta;

  private TableAction(Kind kind, int tableId, int baseVersion, boolean pinned, byte[] deltaBytes) {
    this.kind = kind;
    this.tableId = tableId;
    this.baseVersion = baseVersion;
    this.pinned = pinned;
    this.deltaBytes = deltaBytes;
  }

  static TableAction delta(int tableId, int baseVersion, byte[] deltaBytes, boolean pinned) {
    return new TableAction(Kind.DELTA, tableId, baseVersion, pinned, deltaBytes);
  }

  static TableAction of(Kind kind, int tableId, int baseVersion) {
    return new TableAction(kind, tableId, baseVersion, true, null);
  }

  Kind kind() {
    return kind;
  }

  int tableId() {
    return tableId;
  }

  /** The version of the table the writer built the action on. */
  int baseVersion() {
    return baseVersion;
  }

  /**
   * Whether the action applies only on its base version, whatever the encoding: an action other
   * than a delta, or a delta its writer pinned, a table a catalog transaction writes (a write must
   * be at the version the transaction first read: errata D12).
   */
  boolean pinned() {
    return pinned;
  }

  /** Whether an encoding can see no more of the action than its table and base version. */
  boolean coarse() {
    return kind != Kind.DELTA;
  }

  /** The delta, decoded once; null unless {@link Kind#DELTA}. */
  DecodedDelta delta() {
    if (delta == null && deltaBytes != null) {
      delta = InlineDeltaCodec.decodeDelta(deltaBytes);
    }
    return delta;
  }

  /** The delta's updates of {@code type}, in its order; none unless {@link Kind#DELTA}. */
  <T extends DeltaUpdate> List<T> updates(Class<T> type) {
    DecodedDelta decoded = delta();
    if (decoded == null) {
      return List.of();
    }
    List<T> updates = new ArrayList<>();
    for (DeltaUpdate update : decoded.updates) {
      if (type.isInstance(update)) {
        updates.add(type.cast(update));
      }
    }
    return Collections.unmodifiableList(updates);
  }

  /** The snapshots the delta adds. */
  List<AddSnapshotUpdate> addedSnapshots() {
    return updates(AddSnapshotUpdate.class);
  }

  /** The manifests the delta adds to its new snapshots' lists. */
  List<AddManifestUpdate> addedManifests() {
    return updates(AddManifestUpdate.class);
  }

  /** The manifests the delta's new snapshots remove from their parents' lists. */
  List<RemoveManifestUpdate> removedManifests() {
    return updates(RemoveManifestUpdate.class);
  }

  @Override
  public String toString() {
    return kind + "(" + tableId + "@" + baseVersion + (pinned && kind == Kind.DELTA ? ", pinned" : "")
        + ")";
  }
}
