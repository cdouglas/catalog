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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.io.InlineDeltaCodec.AddSnapshotUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.SetSnapshotRefUpdate;

/**
 * Replay's sealing of a table's old snapshot blocks, and what it rejects for them
 * (docs/SNAPSHOT_PAGING.md, "Blocks", "Bringing a sealed snapshot back", "Expiry"). A function of
 * the state: every replica seals the same blocks at the same records, with the same pages, and
 * reads no clock and no page.
 */
final class SnapshotSealing {
  private SnapshotSealing() {}

  /**
   * Seals the blocks of inline table {@code tblId} that its last sequence number passed since it
   * was {@code beforeLast}: its snapshots {@code fields} holds with sequence numbers in such a
   * block, but those a ref targets, which stay inline, go to the block's page, and their lists out
   * of the table's inline lists (with the pool entries no inline list names). Returns the fields
   * without them, or {@code fields} if nothing sealed.
   */
  static TableFields seal(
      CatalogState.Builder builder, int tblId, TableFields fields, long beforeLast) {
    int block = builder.snapshotBlock();
    if (block == 0 || fields.formatVersion < 2) {
      return fields;
    }
    int window = builder.snapshotWindow();
    long from = SnapshotPages.sealedThrough(beforeLast, block, window);
    long through = SnapshotPages.sealedThrough(fields.lastSequenceNumber, block, window);
    if (through <= from) {
      return fields;
    }
    Set<Long> targets = new HashSet<>();
    if (fields.currentSnapshotId >= 0) {
      targets.add(fields.currentSnapshotId);
    }
    for (SnapshotRef ref : fields.refs.values()) {
      targets.add(ref.snapshotId());
    }
    Map<Long, List<Snapshot>> byBlock = new TreeMap<>();
    List<Snapshot> kept = new ArrayList<>(fields.snapshots.size());
    for (Snapshot snapshot : fields.snapshots) {
      long k = SnapshotPages.blockOf(snapshot.sequenceNumber(), block);
      if (k > from && k <= through && !targets.contains(snapshot.snapshotId())) {
        byBlock.computeIfAbsent(k, b -> new ArrayList<>()).add(snapshot);
      } else {
        kept.add(snapshot);
      }
    }
    if (byBlock.isEmpty()) {
      return fields;
    }
    Map<Long, List<String>> lists = builder.snapshotManifests(tblId);
    Map<String, org.apache.iceberg.ManifestFile> pool = builder.manifestPool(tblId);
    String prefix = builder.manifestListPrefix(tblId);
    List<Long> sealedIds = new ArrayList<>();
    for (Map.Entry<Long, List<Snapshot>> entry : byBlock.entrySet()) {
      builder.setSealedBlock(tblId, SnapshotPages.seal(
          fields.uuid, tblId, entry.getKey(), entry.getValue(), lists, pool,
          prefix != null ? prefix : ""));
      for (Snapshot snapshot : entry.getValue()) {
        if (lists.containsKey(snapshot.snapshotId())) {
          sealedIds.add(snapshot.snapshotId());
        }
      }
    }
    if (!sealedIds.isEmpty()) {
      builder.removeSnapshotManifests(tblId, sealedIds);
    }
    return fields.withSnapshots(kept);
  }

  /** Takes {@code removed}, ids an expiry removed, out of the table's sealed blocks' live ids. */
  static void expire(CatalogState.Builder builder, int tblId, Collection<Long> removed) {
    if (builder.sealedBlocks(tblId).isEmpty()) {
      return;
    }
    for (long id : removed) {
      SnapshotPages.SealedBlock block = builder.sealedBlockOf(tblId, id);
      if (block != null) {
        builder.setSealedBlock(tblId, block.without(id));
      }
    }
  }

  /**
   * Why a delta on table {@code tblId}, landing on {@code table}, needs a sealed snapshot's
   * content without carrying it, or null: a new snapshot whose parent is sealed (a stale writer,
   * a stale branch); a ref moved onto a sealed snapshot; or, landing on a later version, a
   * branch's history back to the parent it built on that reaches a sealed snapshot (the window a
   * merging encoding reads). Blocks only become sealed, so a record rejected so where it lands
   * would be anywhere later (S2).
   */
  static String reason(
      InlineDeltaCodec.DecodedDelta delta, MetadataView table, CatalogState.Builder builder,
      int tblId, Set<Long> broughtBack) {
    if (delta == null || builder.sealedBlocks(tblId).isEmpty()) {
      return null;
    }
    // What the record brings back with its proof is inline where the delta applies
    Set<Long> added = new HashSet<>(broughtBack);
    for (InlineDeltaCodec.DeltaUpdate update : delta.updates) {
      if (update instanceof AddSnapshotUpdate) {
        added.add(((AddSnapshotUpdate) update).snapshotId);
      }
    }
    for (InlineDeltaCodec.DeltaUpdate update : delta.updates) {
      if (update instanceof AddSnapshotUpdate) {
        Long parent = ((AddSnapshotUpdate) update).parentSnapshotId;
        if (parent != null && !added.contains(parent) && sealedLive(builder, tblId, table, parent)) {
          return "sealed: parent " + parent;
        }
      } else if (update instanceof SetSnapshotRefUpdate) {
        SetSnapshotRefUpdate set = (SetSnapshotRefUpdate) update;
        if (!added.contains(set.snapshotId)
            && sealedLive(builder, tblId, table, set.snapshotId)) {
          return "sealed: ref " + set.refName + " to " + set.snapshotId;
        }
        // The branch's history on the landing table back to what the delta built on
        String window = window(delta, set, table, builder, tblId, added);
        if (window != null) {
          return window;
        }
      }
    }
    return null;
  }

  /**
   * Brings {@code back} inline, as a record carrying them applies (each proven against its block's
   * Merkle root by verify): each snapshot joins the inline snapshots in sequence order, with its
   * list and the pool entries it names (a pooled path's entry never changes, so an entry the pool
   * holds is the leaf's), and leaves its block's live ids.
   */
  static void bringBack(
      CatalogState.Builder builder, int tblId, List<SnapshotPages.BroughtBack> back) {
    String prefix = builder.manifestListPrefix(tblId);
    InlineTable table = builder.inlineTable(tblId);
    TableFields fields = table.fields();
    List<Snapshot> snapshots = new ArrayList<>(fields.snapshots);
    for (SnapshotPages.BroughtBack brought : back) {
      SnapshotPages.SealedBlock block = builder.sealedBlockOf(tblId, brought.snapshotId);
      SnapshotPages.Leaf leaf = SnapshotPages.decodeLeaf(brought.leaf, prefix != null ? prefix : "");
      snapshots.add(leaf.snapshot);
      if (leaf.manifests != null) {
        List<String> paths = new ArrayList<>(leaf.manifests.size());
        for (org.apache.iceberg.ManifestFile manifest : leaf.manifests) {
          builder.addManifestToPool(tblId, manifest);
          paths.add(manifest.path());
        }
        builder.setSnapshotManifests(tblId, brought.snapshotId, paths);
      }
      builder.setSealedBlock(tblId, block.without(brought.snapshotId));
    }
    snapshots.sort(java.util.Comparator.comparingLong(Snapshot::sequenceNumber));
    builder.updateInlineTable(tblId, builder.tableVersion(tblId),
        InlineTable.ofFields(fields.withSnapshots(snapshots), table.codec()));
  }

  /** Whether {@code id} is a sealed snapshot of the table, live, and not inline. */
  private static boolean sealedLive(
      CatalogState.Builder builder, int tblId, MetadataView table, long id) {
    return table.snapshot(id) == null && builder.sealedBlockOf(tblId, id) != null;
  }

  /**
   * For a delta that adds the snapshot {@code set} moves its ref to: the ref's history on the
   * landing table, from its head back to the new snapshot's parent; "sealed" if it reaches a
   * sealed snapshot first.
   */
  private static String window(
      InlineDeltaCodec.DecodedDelta delta, SetSnapshotRefUpdate set, MetadataView table,
      CatalogState.Builder builder, int tblId, Set<Long> added) {
    if (!added.contains(set.snapshotId)) {
      return null;
    }
    Long parent = null;
    for (InlineDeltaCodec.DeltaUpdate update : delta.updates) {
      if (update instanceof AddSnapshotUpdate
          && ((AddSnapshotUpdate) update).snapshotId == set.snapshotId) {
        parent = ((AddSnapshotUpdate) update).parentSnapshotId;
      }
    }
    if (parent == null || added.contains(parent)) {
      return null;
    }
    Set<Long> seen = new HashSet<>();
    for (Long at = headOf(table, set.refName); at != null && !at.equals(parent); ) {
      if (!seen.add(at)) {
        return null;
      }
      Snapshot snapshot = table.snapshot(at);
      if (snapshot == null) {
        return builder.sealedBlockOf(tblId, at) != null ? "sealed: window reaches " + at : null;
      }
      at = snapshot.parentId();
    }
    return null;
  }

  private static Long headOf(MetadataView table, String refName) {
    SnapshotRef ref = table.ref(refName);
    return ref != null ? ref.snapshotId() : null;
  }
}
