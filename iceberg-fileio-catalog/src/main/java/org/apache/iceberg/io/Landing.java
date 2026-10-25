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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.InlineDeltaCodec.AddManifestUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.AddSnapshotUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.DecodedDelta;
import org.apache.iceberg.io.InlineDeltaCodec.DeltaUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.RemoveManifestUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.SetSnapshotRefUpdate;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * Where a table update's delta lands: its late-bound fields, finalized on the table its record
 * lands on.
 *
 * <p>A writer computes a delta on the table as it read it, its base, and its record names the
 * base's version. Replay applies the delta to the table where the record lands: on its base when
 * the version still holds, and, when a conflict encoding admits it there, on a table that has
 * changed since (a stale record). On its base every field is the writer's. On a changed table the
 * fields that depend on the base are finalized as Iceberg's retry of the operation computes them
 * there ({@code SnapshotProducer.apply} and {@code summary}):
 *
 * <ul>
 *   <li>the delta's new snapshots are numbered after the table's last sequence number, as on the
 *       base, and the manifests they wrote take their numbers: a sequence number above the base's
 *       last one moves with them;
 *   <li>their first row ids start at the table's next row id, and the data manifests they wrote
 *       move with them;
 *   <li>the first new snapshot of a branch takes the branch's head as its parent, and its
 *       summary's totals start from the head's (from 0 if the branch has none);
 *   <li>their timestamps and the table's last-updated-ms are no earlier than the table's
 *       last-updated-ms;
 *   <li>a branch they advance keeps its retention.
 * </ul>
 *
 * The table's version becomes the landing version + 1 ({@link ProtoCodec}). {@link #unmergeable}
 * says when a delta cannot be finalized on a changed table; a conflict encoding admits a stale
 * record only where it can.
 */
final class Landing {

  /** The total-* properties of a snapshot summary, each with the properties that move it. */
  private static final List<List<String>> TOTALS =
      List.of(
          List.of(
              SnapshotSummary.TOTAL_RECORDS_PROP,
              SnapshotSummary.ADDED_RECORDS_PROP,
              SnapshotSummary.DELETED_RECORDS_PROP),
          List.of(
              SnapshotSummary.TOTAL_FILE_SIZE_PROP,
              SnapshotSummary.ADDED_FILE_SIZE_PROP,
              SnapshotSummary.REMOVED_FILE_SIZE_PROP),
          List.of(
              SnapshotSummary.TOTAL_DATA_FILES_PROP,
              SnapshotSummary.ADDED_FILES_PROP,
              SnapshotSummary.DELETED_FILES_PROP),
          List.of(
              SnapshotSummary.TOTAL_DELETE_FILES_PROP,
              SnapshotSummary.ADDED_DELETE_FILES_PROP,
              SnapshotSummary.REMOVED_DELETE_FILES_PROP),
          List.of(
              SnapshotSummary.TOTAL_POS_DELETES_PROP,
              SnapshotSummary.ADDED_POS_DELETES_PROP,
              SnapshotSummary.REMOVED_POS_DELETES_PROP),
          List.of(
              SnapshotSummary.TOTAL_EQ_DELETES_PROP,
              SnapshotSummary.ADDED_EQ_DELETES_PROP,
              SnapshotSummary.REMOVED_EQ_DELETES_PROP));

  private final TableMetadata table;
  private final boolean stale;
  private final Set<Long> added;
  private final Long baseLastSequenceNumber;
  private final long sequenceShift;
  private final Long baseNextRowId;
  private final long rowShift;
  // The first new snapshot of each branch whose head moved, by id: its parent there (null: none).
  private final Map<Long, Long> parents;
  // Each new snapshot's summary, its totals counted where it lands; none on its base.
  private final Map<Long, Map<String, String>> summaries;

  private Landing(
      TableMetadata table,
      boolean stale,
      Set<Long> added,
      Long baseLastSequenceNumber,
      long sequenceShift,
      Long baseNextRowId,
      long rowShift,
      Map<Long, Long> parents,
      Map<Long, Map<String, String>> summaries) {
    this.table = table;
    this.stale = stale;
    this.added = added;
    this.baseLastSequenceNumber = baseLastSequenceNumber;
    this.sequenceShift = sequenceShift;
    this.baseNextRowId = baseNextRowId;
    this.rowShift = rowShift;
    this.parents = parents;
    this.summaries = summaries;
  }

  /**
   * Where {@code delta} lands on {@code table}: its base if not {@code stale}, else a table that
   * changed since, where it must be mergeable ({@link #unmergeable} is null).
   */
  static Landing of(DecodedDelta delta, TableMetadata table, boolean stale) {
    Map<Long, AddSnapshotUpdate> adds = adds(delta);
    Long baseLastSequenceNumber = delta.baseLastSequenceNumber;
    long sequenceShift =
        table.formatVersion() >= 2 && baseLastSequenceNumber != null
            ? table.lastSequenceNumber() - baseLastSequenceNumber
            : 0L;
    Long baseNextRowId = delta.baseNextRowId;
    long rowShift =
        table.formatVersion() >= 3 && baseNextRowId != null
            ? table.nextRowId() - baseNextRowId
            : 0L;
    Map<Long, Long> parents = new HashMap<>();
    Map<Long, Map<String, String>> summaries = new HashMap<>();
    Map<Long, String> branches = stale ? branches(delta, adds, table) : null;
    if (branches != null) {
      for (AddSnapshotUpdate add : adds.values()) {
        // SnapshotProducer: the parent is the branch's latest snapshot (main's for a branch the
        // table lacks), and the totals count from the branch's head (from 0 if it has none).
        Map<String, String> previous;
        if (adds.containsKey(add.parentSnapshotId)) {
          previous = summaries.get(add.parentSnapshotId);
        } else {
          String branch = branches.getOrDefault(first(add.snapshotId, adds), SnapshotRef.MAIN_BRANCH);
          Long head = latest(table, branch);
          if (!Objects.equals(head, add.parentSnapshotId)) {
            parents.put(add.snapshotId, head);
          }
          SnapshotRef ref = table.ref(branch);
          previous = ref == null ? zeros() : summaryOrEmpty(table, ref.snapshotId());
        }
        summaries.put(add.snapshotId, withTotals(add.summary, previous));
      }
    }
    return new Landing(
        table,
        stale,
        adds.keySet(),
        baseLastSequenceNumber,
        sequenceShift,
        baseNextRowId,
        rowShift,
        parents,
        summaries);
  }

  /**
   * Why {@code delta} cannot be finalized on {@code table}, a version of the table other than its
   * base, or null if it can. {@code builder} holds the table's manifest lists ({@code tableId}).
   * These are what the finalization needs, not what makes the merge correct: a conflict encoding
   * judges that, and must make each of them stable where it admits.
   */
  static String unmergeable(
      DecodedDelta delta, TableMetadata table, CatalogState.Builder builder, int tableId) {
    Map<Long, AddSnapshotUpdate> adds = adds(delta);
    Map<Long, String> branches = branches(delta, adds, table);
    if (branches == null) {
      return "a ref it moves to a new snapshot is a tag, or two branches it moves share a chain";
    }
    Landing landing = of(delta, table, true);
    boolean moved = landing.sequenceShift != 0 || landing.rowShift != 0;
    if (adds.size() > 1 && (moved || !landing.parents.isEmpty())) {
      // A later snapshot's manifests may hold the earlier ones' entries, with their sequence
      // numbers and row ids written in the manifest files.
      return "it adds more than one snapshot to a table that has moved";
    }
    if (!adds.isEmpty() && table.formatVersion() >= 3 && delta.baseNextRowId == null) {
      return "its base had no row lineage";
    }
    Set<String> pool = builder.manifestPool(tableId).keySet();
    for (DeltaUpdate update : delta.updates) {
      if (update instanceof InlineDeltaCodec.AddSchemaUpdate
          || update instanceof InlineDeltaCodec.AddPartitionSpecUpdate
          || update instanceof InlineDeltaCodec.AddSortOrderUpdate) {
        return "it numbers a new schema, spec or sort order on its base";
      } else if (update instanceof InlineDeltaCodec.RemoveSchemasUpdate
          || update instanceof InlineDeltaCodec.RemovePartitionSpecsUpdate
          || update instanceof InlineDeltaCodec.RemoveSnapshotsUpdate) {
        return "it removes what it found unused on its base";
      } else if (update instanceof InlineDeltaCodec.SetCurrentSchemaUpdate) {
        if (table.schemasById().get(((InlineDeltaCodec.SetCurrentSchemaUpdate) update).schemaId)
            == null) {
          return "its schema is gone";
        }
      } else if (update instanceof InlineDeltaCodec.SetDefaultSpecUpdate) {
        if (table.spec(((InlineDeltaCodec.SetDefaultSpecUpdate) update).specId) == null) {
          return "its partition spec is gone";
        }
      } else if (update instanceof InlineDeltaCodec.SetDefaultSortOrderUpdate) {
        if (table.sortOrdersById().get(((InlineDeltaCodec.SetDefaultSortOrderUpdate) update).orderId)
            == null) {
          return "its sort order is gone";
        }
      } else if (update instanceof SetSnapshotRefUpdate) {
        long target = ((SetSnapshotRefUpdate) update).snapshotId;
        if (!adds.containsKey(target) && table.snapshot(target) == null) {
          return "it sets a ref to a snapshot the table no longer has";
        }
      } else if (update instanceof AddManifestUpdate) {
        ManifestFile manifest = ((AddManifestUpdate) update).manifest;
        if (table.spec(manifest.partitionSpecId()) == null) {
          return "a manifest's partition spec is gone";
        }
        if (adds.containsKey(manifest.snapshotId()) && pool.contains(manifest.path())) {
          return "a manifest it wrote is in the table already";
        }
      }
    }
    for (AddSnapshotUpdate add : adds.values()) {
      if (table.snapshot(add.snapshotId) != null) {
        return "its snapshot id is in the table already";
      }
      if (add.schemaId != null && table.schemasById().get(add.schemaId) == null) {
        return "its snapshot's schema is gone";
      }
      if (!add.manifestListSuffix.isEmpty()
          && (moved || landing.parents.containsKey(add.snapshotId))) {
        return "its manifest list is a file, written for its base";
      }
      if (!branches.containsKey(first(add.snapshotId, adds))
          && landing.parents.containsKey(add.snapshotId)) {
        // a staged snapshot: the branch it was staged on is unknown
        return "it stages a snapshot on a branch that has moved";
      }
    }
    Set<Long> listed = builder.snapshotManifestKeys(tableId);
    for (Map.Entry<Long, Long> head : landing.parents.entrySet()) {
      long first = head.getKey();
      Long parent = adds.get(first).parentSnapshotId;
      if (head.getValue() != null && !listed.contains(head.getValue())) {
        return "the head of its branch has no inline manifest list";
      }
      if (parent != null && !listed.contains(parent)) {
        // its delta's manifests are relative to the parent's inline list; without one it sent
        // its whole list
        return "its parent has no inline manifest list";
      }
      List<String> headList =
          head.getValue() != null
              ? builder.snapshotManifestPaths(tableId, head.getValue())
              : List.of();
      for (DeltaUpdate update : delta.updates) {
        if (update instanceof RemoveManifestUpdate
            && ((RemoveManifestUpdate) update).snapshotId == first) {
          String suffix = ((RemoveManifestUpdate) update).manifestPathSuffix;
          if (headList.stream().noneMatch(path -> path.endsWith(suffix))) {
            return "a manifest it removes is not in its branch's head";
          }
        }
      }
    }
    return null;
  }

  /** Whether the table's version moved since the delta's base. */
  boolean stale() {
    return stale;
  }

  /** The table's last-updated-ms after the delta: the writer's, no earlier than the table's. */
  long lastUpdatedMillis(long writer) {
    return stale ? Math.max(writer, table.lastUpdatedMillis()) : writer;
  }

  /** The timestamp of a snapshot the delta adds, at {@code writer} on its base. */
  long timestamp(long writer) {
    return stale ? Math.max(writer, table.lastUpdatedMillis()) : writer;
  }

  /** The parent of {@code add}'s snapshot. */
  Long parent(AddSnapshotUpdate add) {
    return parents.containsKey(add.snapshotId) ? parents.get(add.snapshotId) : add.parentSnapshotId;
  }

  /** The first row id of {@code add}'s snapshot. */
  Long firstRowId(AddSnapshotUpdate add) {
    return add.firstRowId != null ? add.firstRowId + rowShift : null;
  }

  /**
   * The summary of {@code add}'s snapshot: the writer's, with its totals counted from its branch's
   * head where it lands, as {@code SnapshotProducer.summary} counts them, if that moved.
   */
  Map<String, String> summary(AddSnapshotUpdate add) {
    Map<String, String> summary = summaries.get(add.snapshotId);
    return summary != null ? summary : add.summary;
  }

  /** {@code summary} with its totals counted from {@code previous}, a parent's summary. */
  private static Map<String, String> withTotals(
      Map<String, String> summary, Map<String, String> previous) {
    Map<String, String> counted = new LinkedHashMap<>(summary);
    for (List<String> total : TOTALS) {
      counted.remove(total.get(0));
      String value =
          total(previous.get(total.get(0)), summary.get(total.get(1)), summary.get(total.get(2)));
      if (value != null) {
        counted.put(total.get(0), value);
      }
    }
    return counted;
  }

  /** The totals a branch with no head starts from. */
  private static Map<String, String> zeros() {
    Map<String, String> zeros = new HashMap<>();
    for (List<String> total : TOTALS) {
      zeros.put(total.get(0), "0");
    }
    return zeros;
  }

  private static Map<String, String> summaryOrEmpty(TableMetadata table, long snapshotId) {
    Map<String, String> summary = table.snapshot(snapshotId).summary();
    return summary != null ? summary : ImmutableMap.of();
  }

  /** {@code SnapshotProducer.updateTotal}: a total moved by what a snapshot added and removed. */
  private static String total(String previous, String added, String removed) {
    if (previous == null) {
      return null;
    }
    try {
      long total = Long.parseLong(previous);
      if (total >= 0 && added != null) {
        total += Long.parseLong(added);
      }
      if (total >= 0 && removed != null) {
        total -= Long.parseLong(removed);
      }
      return total >= 0 ? String.valueOf(total) : null;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /**
   * {@code manifest}, as the delta lists it, with the sequence numbers and first row id the delta
   * assigned moved to where it lands: a manifest one of its snapshots wrote numbers its entries
   * after the base's last sequence number and ids from the base's next row id.
   */
  ManifestFile manifest(ManifestFile manifest) {
    if (!added.contains(manifest.snapshotId()) || (sequenceShift == 0 && rowShift == 0)) {
      return manifest;
    }
    long sequenceNumber = manifest.sequenceNumber();
    long minSequenceNumber = manifest.minSequenceNumber();
    if (baseLastSequenceNumber != null) {
      if (sequenceNumber > baseLastSequenceNumber) {
        sequenceNumber += sequenceShift;
      }
      if (minSequenceNumber > baseLastSequenceNumber) {
        minSequenceNumber += sequenceShift;
      }
    }
    Long firstRowId = manifest.firstRowId();
    if (firstRowId != null && baseNextRowId != null && firstRowId >= baseNextRowId) {
      firstRowId += rowShift;
    }
    return InlineDeltaCodec.manifestFile(
        manifest, manifest.path(), sequenceNumber, minSequenceNumber, firstRowId);
  }

  /** {@code update}, keeping the retention of the branch it advances where the table moved. */
  SetSnapshotRefUpdate ref(SetSnapshotRefUpdate update) {
    SnapshotRef current = stale ? table.ref(update.refName) : null;
    if (current == null || !added.contains(update.snapshotId)) {
      return update;
    }
    return new SetSnapshotRefUpdate(
        update.refName,
        update.snapshotId,
        update.refType,
        current.minSnapshotsToKeep() != null ? current.minSnapshotsToKeep() : 0,
        current.maxSnapshotAgeMs() != null ? current.maxSnapshotAgeMs() : 0L,
        current.maxRefAgeMs() != null ? current.maxRefAgeMs() : 0L);
  }

  /** The delta's new snapshots by id, in its order. */
  private static Map<Long, AddSnapshotUpdate> adds(DecodedDelta delta) {
    Map<Long, AddSnapshotUpdate> adds = new LinkedHashMap<>();
    for (DeltaUpdate update : delta.updates) {
      if (update instanceof AddSnapshotUpdate) {
        adds.put(((AddSnapshotUpdate) update).snapshotId, (AddSnapshotUpdate) update);
      }
    }
    return adds;
  }

  /**
   * The branch each chain the delta moves a ref to belongs to, by the chain's first new snapshot;
   * null if a ref it moves to a new snapshot is a tag, here or in the delta, or two branches share
   * a chain. A chain is a ref's new snapshot and its new ancestors.
   */
  private static Map<Long, String> branches(
      DecodedDelta delta, Map<Long, AddSnapshotUpdate> adds, TableMetadata table) {
    Map<Long, String> branches = new HashMap<>();
    for (DeltaUpdate update : delta.updates) {
      if (!(update instanceof SetSnapshotRefUpdate)
          || !adds.containsKey(((SetSnapshotRefUpdate) update).snapshotId)) {
        continue;
      }
      SetSnapshotRefUpdate ref = (SetSnapshotRefUpdate) update;
      SnapshotRef current = table.ref(ref.refName);
      if (!"branch".equals(ref.refType) || (current != null && !current.isBranch())) {
        return null;
      }
      long first = first(ref.snapshotId, adds);
      String other = branches.put(first, ref.refName);
      if (other != null && !other.equals(ref.refName)) {
        return null;
      }
    }
    return branches;
  }

  /**
   * {@code SnapshotUtil.latestSnapshot}: the head of {@code branch}, or main's for a branch the
   * table lacks; null if it has none.
   */
  private static Long latest(TableMetadata table, String branch) {
    SnapshotRef ref = table.ref(branch);
    return ref != null ? Long.valueOf(ref.snapshotId()) : currentSnapshotId(table);
  }

  /** The first new snapshot of the chain that ends at {@code snapshotId}. */
  private static long first(long snapshotId, Map<Long, AddSnapshotUpdate> adds) {
    long first = snapshotId;
    Set<Long> seen = new HashSet<>();
    while (seen.add(first)) {
      Long parent = adds.get(first).parentSnapshotId;
      if (parent == null || !adds.containsKey(parent)) {
        break;
      }
      first = parent;
    }
    return first;
  }

  private static Long currentSnapshotId(TableMetadata table) {
    return table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : null;
  }
}
