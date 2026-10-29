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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotSummary;
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
 *   <li>their first row ids start at the table's next row id, and the data manifests they wrote,
 *       or gave row ids to (ones written before the table's upgrade to v3), move with them;
 *   <li>the first new snapshot of a branch takes the branch's head as its parent, and its
 *       summary's totals start from the head's (from 0 if the branch has none);
 *   <li>their timestamps and the table's last-updated-ms are no earlier than the table's
 *       last-updated-ms;
 *   <li>a branch they advance keeps its retention.
 * </ul>
 *
 * The table's version becomes the landing version + 1 ({@link ProtoCodec}). {@link #unmergeable}
 * says when a delta cannot be finalized on a changed table: among others, where the retry would
 * change its snapshot's manifest list beyond what the delta says, by dropping delete files (a
 * merging producer drops those older than every data file it keeps) or by giving row ids to
 * manifests that have none. A conflict encoding admits a stale record only where it can be
 * finalized.
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

  private final MetadataView table;
  private final boolean stale;
  private final Set<Long> added;
  private final Long baseLastSequenceNumber;
  private final long sequenceShift;
  private final Long baseNextRowId;
  private final long rowShift;
  // The first new snapshot of each branch whose head moved, by id: its parent there (null: none).
  private final Map<Long, Long> parents;
  // Each new snapshot's summary where it lands: with its parent's extra entries, if it takes them,
  // and its totals counted there if the table moved; none if it is the one its update states.
  private final Map<Long, Map<String, String>> summaries;

  private Landing(
      MetadataView table,
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
  static Landing of(DecodedDelta delta, MetadataView table, boolean stale) {
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
    Map<Long, Map<String, String>> own = new HashMap<>();
    for (AddSnapshotUpdate add : adds.values()) {
      Map<String, String> summary = add.summary(table);
      own.put(add.snapshotId, summary);
      if (add.inheritsParentExtra && summary != null) {
        summaries.put(add.snapshotId, summary);
      }
    }
    Map<Long, String> branches = stale ? branches(delta, adds, table) : null;
    if (branches != null) {
      for (AddSnapshotUpdate add : adds.values()) {
        Map<String, String> summary = own.get(add.snapshotId);
        if (summary == null) {
          continue; // its parent is gone, so it cannot land here (unresolved)
        }
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
        if (previous != null) {
          summaries.put(add.snapshotId, withTotals(summary, previous));
        }
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
   * Why {@code delta} cannot replay on {@code table}, any version of the table, its base included,
   * or null if it can: what it names by reference is not there. Its manifests' names are under
   * the manifest prefix of the table its writer read (DecodedDelta#unresolvedNames). A
   * new snapshot may take its parent's extra summary entries, and name the manifests it removes by
   * their indices in its parent's list ({@code builder}'s lists of {@code tableId}): the parent its
   * writer read, which a delta on its base finds there, while a later version may have expired it.
   */
  static String unresolved(
      DecodedDelta delta, MetadataView table, CatalogState.Builder builder, int tableId) {
    String names = delta.unresolvedNames(builder.manifestListPrefix(tableId));
    if (names != null) {
      return names;
    }
    Map<Long, AddSnapshotUpdate> adds = adds(delta);
    for (AddSnapshotUpdate add : adds.values()) {
      if (add.inheritsParentExtra
          && (add.parentSnapshotId == null || adds.containsKey(add.parentSnapshotId)
              || table.snapshot(add.parentSnapshotId) == null)) {
        return "the parent whose summary entries its snapshot takes is gone";
      }
    }
    for (DeltaUpdate update : delta.updates) {
      if (update instanceof RemoveManifestUpdate
          && ((RemoveManifestUpdate) update).parentIndices != null
          && removed((RemoveManifestUpdate) update, adds, builder, tableId) == null) {
        return "the parent's list whose manifests it removes by index is gone";
      }
    }
    return null;
  }

  /**
   * What {@code remove} removes, matched as path ends ({@link RemoveManifestUpdate#removed}): its
   * indices name manifests of the list of its snapshot's parent as {@code adds} names it, which
   * must be one the table holds, not one the delta adds. Null if they name what is not there.
   */
  private static List<String> removed(
      RemoveManifestUpdate remove, Map<Long, AddSnapshotUpdate> adds,
      CatalogState.Builder builder, int tableId) {
    AddSnapshotUpdate add = adds.get(remove.snapshotId);
    Long parent = add != null ? add.parentSnapshotId : null;
    List<String> parentList =
        parent != null && !adds.containsKey(parent) && builder.hasSnapshotManifests(tableId, parent)
            ? builder.snapshotManifestPaths(tableId, parent)
            : null;
    return remove.removed(parentList);
  }

  /**
   * Why {@code delta} cannot be finalized on {@code table}, a version of the table other than its
   * base, or null if it can. {@code builder} holds the table's manifest lists ({@code tableId}).
   * These are what the finalization needs, not what makes the merge correct: a conflict encoding
   * judges that, and must make each of them stable where it admits.
   */
  static String unmergeable(
      DecodedDelta delta, MetadataView table, CatalogState.Builder builder, int tableId) {
    String unresolved = unresolved(delta, table, builder, tableId);
    if (unresolved != null) {
      return unresolved;
    }
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
    Map<String, ManifestFile> pool = builder.manifestPool(tableId);
    String prefix = Objects.requireNonNullElse(builder.manifestListPrefix(tableId), "");
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
        if (table.schema(((InlineDeltaCodec.SetCurrentSchemaUpdate) update).schemaId)
            == null) {
          return "its schema is gone";
        }
      } else if (update instanceof InlineDeltaCodec.SetDefaultSpecUpdate) {
        if (table.spec(((InlineDeltaCodec.SetDefaultSpecUpdate) update).specId) == null) {
          return "its partition spec is gone";
        }
      } else if (update instanceof InlineDeltaCodec.SetDefaultSortOrderUpdate) {
        if (table.sortOrder(((InlineDeltaCodec.SetDefaultSortOrderUpdate) update).orderId)
            == null) {
          return "its sort order is gone";
        }
      } else if (update instanceof SetSnapshotRefUpdate) {
        long target = ((SetSnapshotRefUpdate) update).snapshotId;
        if (!adds.containsKey(target) && table.snapshot(target) == null) {
          return "it sets a ref to a snapshot the table no longer has";
        }
      } else if (update instanceof AddManifestUpdate) {
        ManifestFile manifest =
            landing.manifest(InlineDeltaCodec.resolve(((AddManifestUpdate) update).manifest, prefix));
        if (table.spec(manifest.partitionSpecId()) == null) {
          return "a manifest's partition spec is gone";
        }
        ManifestFile held = pool.get(manifest.path());
        if (held != null && adds.containsKey(manifest.snapshotId())) {
          return "a manifest it wrote is in the table already";
        }
        if (held != null && !Objects.equals(held.firstRowId(), manifest.firstRowId())) {
          // its snapshot gave row ids to a manifest it kept, which the table's lists hold with
          // others: they share one entry per path (design.md I3a)
          return "it gives row ids to a manifest the table's lists hold with others";
        }
      }
    }
    for (AddSnapshotUpdate add : adds.values()) {
      if (table.snapshot(add.snapshotId) != null) {
        return "its snapshot id is in the table already";
      }
      if (add.schemaId != null && table.schema(add.schemaId) == null) {
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
    for (Map.Entry<Long, Long> head : landing.parents.entrySet()) {
      long first = head.getKey();
      Long parent = adds.get(first).parentSnapshotId;
      if (parent == null && head.getValue() != null) {
        // a snapshot with no parent starts its branch's history afresh (a replace), or was the
        // first of an empty table: either way not a commit on the head it would land on
        return "it starts a fresh history on a branch that has a head";
      }
      if (head.getValue() != null && !builder.hasSnapshotManifests(tableId, head.getValue())) {
        return "the head of its branch has no inline manifest list";
      }
      if (parent != null && !builder.hasSnapshotManifests(tableId, parent)) {
        // its delta's manifests are relative to the parent's inline list; without one it sent
        // its whole list
        return "its parent has no inline manifest list";
      }
      List<String> headList = landing.parentList(adds.get(first), builder, tableId);
      for (DeltaUpdate update : delta.updates) {
        if (update instanceof RemoveManifestUpdate
            && ((RemoveManifestUpdate) update).snapshotId == first) {
          for (String suffix : removed((RemoveManifestUpdate) update, adds, builder, tableId)) {
            if (headList.stream().noneMatch(path -> path.endsWith(suffix))) {
              return "a manifest it removes is not in its branch's head";
            }
          }
        }
      }
    }
    for (AddSnapshotUpdate add : adds.values()) {
      Long parent = landing.parent(add);
      if (add.manifestListSuffix.isEmpty() && parent != null && !adds.containsKey(parent)
          && !builder.hasSnapshotManifests(tableId, parent)) {
        // replay builds its list on its parent's inline list where it lands, and its delta's
        // manifests are relative to the one its writer read: an eviction since wrote that list to
        // a file and dropped it, which a return to inline storage does not bring back (Bug 79)
        return "its parent has no inline manifest list where it lands";
      }
    }
    if (adds.size() == 1 && (moved || !landing.parents.isEmpty())) {
      // its snapshot's list is not the one its writer committed: another parent's, or the same
      // on a table whose sequence numbers and row ids moved
      AddSnapshotUpdate add = adds.values().iterator().next();
      List<ManifestFile> list = landing.list(add, delta, builder, tableId, prefix);
      if (list == null) {
        return "its snapshot's list names a manifest the table lacks";
      }
      return retryChanges(list, add, table);
    }
    return null;
  }

  /**
   * Why Iceberg's retry would change {@code list}, the manifest list of {@code add}'s snapshot
   * where it lands on {@code table}, beyond what its delta says; null if it would not.
   *
   * <ul>
   *   <li>In v3, a commit gives row ids to the data manifests of its list that have none (written
   *       before the table's upgrade), which only a head the delta did not see can hold.
   *   <li>A merging producer (MergingSnapshotProducer) drops the delete files whose data sequence
   *       numbers are older than every data file its list keeps, and than the table's last
   *       sequence number: they can apply to no row. It reads the delete manifests that may hold
   *       files it removes, which, as it removes any data file, is every one (a deletion vector of
   *       the file may be in any). So where the oldest data, or the table's last sequence number,
   *       moved past a delete manifest's oldest file, its retry may drop files the delta does not.
   *       A blind producer (a fast or merge append) removes nothing, so drops nothing.
   * </ul>
   */
  private static String retryChanges(
      List<ManifestFile> list, AddSnapshotUpdate add, MetadataView table) {
    if (table.formatVersion() >= 3) {
      for (ManifestFile manifest : list) {
        if (manifest.content() == ManifestContent.DATA && manifest.firstRowId() == null) {
          return "its list keeps data manifests without row ids, which its retry would give ids";
        }
      }
    }
    if (add.footprint != null && add.footprint.kind == Footprint.Kind.BLIND) {
      return null;
    }
    // MergingSnapshotProducer.apply: the oldest data sequence number, of the data manifests it
    // keeps (a new data file's is the commit's, or one it keeps from an older snapshot)
    long oldestData = table.lastSequenceNumber();
    for (ManifestFile manifest : list) {
      if (manifest.content() == ManifestContent.DATA && manifest.minSequenceNumber() >= 0) {
        oldestData = Math.min(oldestData, manifest.minSequenceNumber());
      }
    }
    for (ManifestFile manifest : list) {
      if (manifest.content() == ManifestContent.DELETES
          && (manifest.hasAddedFiles() || manifest.hasExistingFiles())
          && manifest.minSequenceNumber() > 0
          && manifest.minSequenceNumber() < oldestData) {
        return "its retry would drop delete files older than every data file it keeps";
      }
    }
    return null;
  }

  /**
   * The paths of the list that {@code add}'s snapshot starts from: its parent's inline list where
   * it lands (empty if it has none).
   */
  private List<String> parentList(
      AddSnapshotUpdate add, CatalogState.Builder builder, int tableId) {
    Long parent = parent(add);
    return parent != null ? builder.snapshotManifestPaths(tableId, parent) : List.of();
  }

  /**
   * {@code add}'s snapshot's manifest list where it lands, as replay builds it ({@code
   * InlineDeltaCodec.NewSnapshotManifests}): its parent's inline list there, less the manifests
   * the delta removes from it, with those it adds, finalized; in no order. Null if the parent's
   * list names a manifest the table's pool lacks.
   */
  private List<ManifestFile> list(
      AddSnapshotUpdate add,
      DecodedDelta delta,
      CatalogState.Builder builder,
      int tableId,
      String prefix) {
    Map<String, ManifestFile> pool = builder.manifestPool(tableId);
    List<String> paths = new ArrayList<>(parentList(add, builder, tableId));
    List<ManifestFile> list = new ArrayList<>();
    Map<Long, AddSnapshotUpdate> adds = adds(delta);
    for (DeltaUpdate update : delta.updates) {
      if (update instanceof RemoveManifestUpdate
          && ((RemoveManifestUpdate) update).snapshotId == add.snapshotId) {
        for (String suffix : removed((RemoveManifestUpdate) update, adds, builder, tableId)) {
          paths.removeIf(path -> path.endsWith(suffix));
        }
      } else if (update instanceof AddManifestUpdate
          && ((AddManifestUpdate) update).snapshotId == add.snapshotId) {
        list.add(manifest(InlineDeltaCodec.resolve(((AddManifestUpdate) update).manifest, prefix)));
      }
    }
    for (String path : paths) {
      ManifestFile manifest = pool.get(path);
      if (manifest == null) {
        return null;
      }
      list.add(manifest);
    }
    return list;
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
    if (summary == null && add.inheritsParentExtra) {
      // an admitted delta resolves where it lands (unresolved)
      throw new IllegalStateException(
          "Snapshot " + add.snapshotId + " takes the summary of a parent the table lacks");
    }
    return summary != null ? summary : add.summary;
  }

  /** {@code summary} with its totals counted from {@code previous}, a parent's summary. */
  private static Map<String, String> withTotals(
      Map<String, String> summary, Map<String, String> previous) {
    // Each total keeps its place in the writer's summary, as SnapshotProducer.summary places it
    Map<String, String> counted = new LinkedHashMap<>(summary);
    for (List<String> total : TOTALS) {
      String value =
          total(previous.get(total.get(0)), summary.get(total.get(1)), summary.get(total.get(2)));
      if (value != null) {
        counted.put(total.get(0), value);
      } else {
        counted.remove(total.get(0));
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

  private static Map<String, String> summaryOrEmpty(MetadataView table, long snapshotId) {
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
   * after the base's last sequence number, and one it wrote, or kept without row ids, has ids from
   * the base's next row id.
   */
  ManifestFile manifest(ManifestFile manifest) {
    if (sequenceShift == 0 && rowShift == 0) {
      return manifest;
    }
    long sequenceNumber = manifest.sequenceNumber();
    long minSequenceNumber = manifest.minSequenceNumber();
    if (baseLastSequenceNumber != null && added.contains(manifest.snapshotId())) {
      if (sequenceNumber > baseLastSequenceNumber) {
        sequenceNumber += sequenceShift;
      }
      if (minSequenceNumber > baseLastSequenceNumber) {
        minSequenceNumber += sequenceShift;
      }
    }
    // ids from the base's next row id on are ones the delta gave: to a manifest it wrote, or to one
    // it kept that had none
    Long firstRowId = manifest.firstRowId();
    if (firstRowId != null && baseNextRowId != null && firstRowId >= baseNextRowId) {
      firstRowId += rowShift;
    }
    if (sequenceNumber == manifest.sequenceNumber()
        && minSequenceNumber == manifest.minSequenceNumber()
        && Objects.equals(firstRowId, manifest.firstRowId())) {
      return manifest;
    }
    return InlineDeltaCodec.manifestFile(
        manifest, manifest.path(), sequenceNumber, minSequenceNumber, firstRowId);
  }

  /**
   * {@code update}, where the table moved: a branch it advances keeps its retention there, and one
   * the table lacks there is created with none, as a producer's commit creates a branch (only
   * {@code manageSnapshots}, which no encoding admits on a changed table, sets retention).
   */
  SetSnapshotRefUpdate ref(SetSnapshotRefUpdate update) {
    if (!stale || !added.contains(update.snapshotId)) {
      return update;
    }
    SnapshotRef current = table.ref(update.refName);
    return new SetSnapshotRefUpdate(
        update.refName,
        update.snapshotId,
        update.refType,
        current != null && current.minSnapshotsToKeep() != null ? current.minSnapshotsToKeep() : 0,
        current != null && current.maxSnapshotAgeMs() != null ? current.maxSnapshotAgeMs() : 0L,
        current != null && current.maxRefAgeMs() != null ? current.maxRefAgeMs() : 0L);
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
      DecodedDelta delta, Map<Long, AddSnapshotUpdate> adds, MetadataView table) {
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
  private static Long latest(MetadataView table, String branch) {
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

  private static Long currentSnapshotId(MetadataView table) {
    return table.currentSnapshotId();
  }
}
