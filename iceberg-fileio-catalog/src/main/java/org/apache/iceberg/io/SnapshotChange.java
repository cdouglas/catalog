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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.InlineTableMetadata;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;

/**
 * What a commit of {@code metadata} on {@code base} does to the table's snapshots: those it adds,
 * in {@code metadata}'s order, and the ids of those it removes. A commit to a lazily loaded table
 * learns it from the commit's own changes, without making the table's other snapshots, whose
 * sealed blocks are in pages (docs/SNAPSHOT_PAGING.md, "Sizes": every merge append made the whole
 * history, and each client kept it); otherwise it compares the two versions' snapshots.
 */
final class SnapshotChange {
  final List<Snapshot> added;
  final List<Long> removed;
  private final Set<Long> addedIds = new HashSet<>();

  private SnapshotChange(List<Snapshot> added, List<Long> removed) {
    this.added = added;
    this.removed = removed;
    for (Snapshot snapshot : added) {
      addedIds.add(snapshot.snapshotId());
    }
  }

  /** The change from {@code base} (null: a create, which adds every snapshot) to {@code metadata}. */
  static SnapshotChange of(TableMetadata base, TableMetadata metadata) {
    if (base == null) {
      return new SnapshotChange(metadata.snapshots(), List.of());
    }
    List<Snapshot> added = InlineTableMetadata.addedSnapshots(base, metadata);
    if (added != null) {
      return new SnapshotChange(added, List.of());
    }
    Set<Long> baseIds = new HashSet<>();
    for (Snapshot snapshot : base.snapshots()) {
      baseIds.add(snapshot.snapshotId());
    }
    added = new ArrayList<>();
    Set<Long> ids = new HashSet<>();
    for (Snapshot snapshot : metadata.snapshots()) {
      ids.add(snapshot.snapshotId());
      if (!baseIds.contains(snapshot.snapshotId())) {
        added.add(snapshot);
      }
    }
    // In the base's id set's order, as computeDelta has always sent them
    List<Long> removed = new ArrayList<>();
    for (long id : baseIds) {
      if (!ids.contains(id)) {
        removed.add(id);
      }
    }
    return new SnapshotChange(added, removed);
  }

  /** Whether the commit adds snapshot {@code id}. */
  boolean adds(long id) {
    return addedIds.contains(id);
  }

  /**
   * {@code base}'s snapshot {@code id}, as {@code base.snapshot(id)} gives it, without making the
   * others for one the commit adds, which {@code base} cannot hold.
   */
  Snapshot inBase(TableMetadata base, long id) {
    return base == null || adds(id) ? null : base.snapshot(id);
  }
}
