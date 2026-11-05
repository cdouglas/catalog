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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.InlineTableMetadata;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.RetryableValidationException;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.InlineDeltaCodec.AddManifestUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.AddPartitionSpecUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.AddSchemaUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.AddSnapshotUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.AddSortOrderUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.DecodedDelta;
import org.apache.iceberg.io.InlineDeltaCodec.DeltaUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.RemoveManifestUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.RemovePartitionSpecsUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.RemoveSchemasUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.RemoveSnapshotsUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.SetCurrentSchemaUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.SetDefaultSortOrderUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.SetDefaultSpecUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.SetLocationUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.SetPropertiesUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.SetSnapshotRefUpdate;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.PropertyUtil;

/**
 * Replay of a table update's delta on {@link TableFields}, never on the whole table through
 * Iceberg's metadata builder. The snapshot updates (snapshots added and removed, refs set), the
 * properties, the snapshot and metadata logs and the statistics apply here, field for field as
 * {@code TableMetadata.Builder} computes them (its checks included), sharing what they leave. The
 * head updates (location, schemas, specs, sort orders, and the removal of schemas and specs) go
 * through the builder on the table's head ({@link TableFields#head}): its metadata without
 * snapshots, refs, logs or statistics, none of which they read. So no replay of a record needs a
 * table's history beyond its snapshot ids, its refs' targets and its logs (SNAPSHOT_PAGING.md, step
 * 1).
 *
 * <p>A delta's table updates apply in passes, as {@link InlineDeltaCodec#applyDelta} builds them:
 * the updates between two of its manifest updates, each pass one build of the table. That replay,
 * the builder on the whole table, is the reference: with {@code -Dfileio.catalog.replay.check=true}
 * (the test suites set it) each replay here is also run there, and the two must make the same
 * metadata, or both reject the record.
 */
final class NativeDelta {
  private NativeDelta() {}

  private static final long ONE_MINUTE = TimeUnit.MINUTES.toMillis(1);

  /** Whether replays here are checked against the builder's. */
  static final boolean CHECK = Boolean.getBoolean("fileio.catalog.replay.check");

  /**
   * Applies {@code delta} to {@code base}, the table where its record lands, which has changed
   * since the delta's base if {@code stale}, and returns the table after it; its manifest updates
   * go to {@code builder}'s lists of {@code tableId}, as {@link InlineDeltaCodec#applyDelta} routes
   * them. {@code baseLocation} is the base's metadata location, which the metadata log records.
   */
  static TableFields apply(
      TableFields base,
      DecodedDelta delta,
      CatalogState.Builder builder,
      int tableId,
      boolean stale,
      String baseLocation) {
    if (!CHECK) {
      return replay(base, delta, builder, tableId, stale, baseLocation);
    }
    // The builder's replay first: it touches no catalog state, and a rejection there must be one
    // here
    String expected;
    RuntimeException rejected = null;
    try {
      TableMetadata from = base.toMetadata(baseLocation);
      Landing landing = Landing.of(delta, MetadataView.of(from), stale);
      expected = TableMetadataParser.toJson(reference(from, delta, landing));
    } catch (RuntimeException e) {
      expected = null;
      rejected = e;
    }
    TableFields applied;
    try {
      applied = replay(base, delta, builder, tableId, stale, baseLocation);
    } catch (RuntimeException e) {
      if (rejected == null) {
        throw new IllegalStateException(
            "Native replay rejects a delta the builder applies: " + e, e);
      }
      throw e;
    }
    if (rejected != null) {
      throw new IllegalStateException(
          "Native replay applies a delta the builder rejects: " + rejected, rejected);
    }
    String actual = TableMetadataParser.toJson(applied.toMetadata(null));
    if (!expected.equals(actual)) {
      throw new IllegalStateException(
          "Native replay differs from the builder's:\n  builder: " + expected + "\n  native:  "
              + actual);
    }
    return applied;
  }

  private static TableFields replay(
      TableFields base,
      DecodedDelta delta,
      CatalogState.Builder builder,
      int tableId,
      boolean stale,
      String baseLocation) {
    Landing landing = Landing.of(delta, base, stale);
    String prefix = builder.manifestListPrefix(tableId);
    if (prefix == null) {
      prefix = "";
    }
    // Each new inline-ML snapshot's list starts as its parent's, then takes the manifests the
    // delta adds and removes; the lists of the snapshots it removes go once those are settled
    InlineDeltaCodec.NewSnapshotManifests newSnapshots =
        new InlineDeltaCodec.NewSnapshotManifests(builder, tableId, delta);
    Set<Long> added = new HashSet<>();
    List<Long> removed = new ArrayList<>();
    TableFields current = base;
    // A delta writes manifest lists' paths whole; its manifests' names resolve under the prefix.
    // Only a pass that changes the table logs the base's location: the table it builds has none.
    String location = baseLocation;
    List<DeltaUpdate> pass = new ArrayList<>();
    for (DeltaUpdate update : delta.updates) {
      if (!(update instanceof AddManifestUpdate) && !(update instanceof RemoveManifestUpdate)) {
        pass.add(update);
        continue;
      }
      if (!pass.isEmpty()) {
        TableFields next =
            applyPass(current, pass, delta, landing, location, newSnapshots, added, removed);
        if (next != current) {
          location = null;
        }
        current = next;
        pass.clear();
      }
      if (update instanceof AddManifestUpdate) {
        AddManifestUpdate add = (AddManifestUpdate) update;
        if (current.snapshot(add.snapshotId) == null && !added.contains(add.snapshotId)) {
          throw new IllegalStateException(
              "AddManifestUpdate for snapshot " + add.snapshotId
                  + " before its AddSnapshotUpdate — violates delta ordering invariant");
        }
        ManifestFile resolved = landing.manifest(InlineDeltaCodec.resolve(add.manifest, prefix));
        builder.addManifestToPool(tableId, resolved);
        newSnapshots.addManifest(current, add.snapshotId, resolved.path(), add.position);
      } else {
        newSnapshots.removeManifest(current, (RemoveManifestUpdate) update);
      }
    }
    if (!pass.isEmpty()) {
      current = applyPass(current, pass, delta, landing, location, newSnapshots, added, removed);
    }
    newSnapshots.finish();
    if (!removed.isEmpty()) {
      builder.removeSnapshotManifests(tableId, removed);
    }
    return current;
  }

  /**
   * The table after a FULL update of an inline table: {@code record}, the table's head with only the
   * snapshots {@code base} (the version it read, where it lands) lacks and the ones its refs target,
   * with {@code base}'s other snapshots, minus {@code removed}, in their order, then the record's new
   * ones in its order. A writer's builder only appends snapshots to its base's and removes some, so
   * this is the writer's metadata (LogCatalog checks so under {@link #CHECK}).
   */
  static TableFields full(TableFields base, TableMetadata record, long[] removed) {
    Set<Long> drop = new HashSet<>();
    for (long id : removed) {
      drop.add(id);
    }
    List<Snapshot> snapshots = new ArrayList<>(base.snapshots.size() + record.snapshots().size());
    for (Snapshot snapshot : base.snapshots) {
      if (!drop.contains(snapshot.snapshotId())) {
        snapshots.add(snapshot);
      }
    }
    for (Snapshot snapshot : record.snapshots()) {
      if (base.snapshots.byKey(snapshot.snapshotId()) == null) {
        snapshots.add(snapshot);
      }
    }
    return TableFields.of(record).withSnapshots(snapshots);
  }

  /** Whether {@code update} changes only the table's head, which the builder applies. */
  private static boolean headUpdate(DeltaUpdate update) {
    return update instanceof SetLocationUpdate
        || update instanceof AddSchemaUpdate
        || update instanceof SetCurrentSchemaUpdate
        || update instanceof AddPartitionSpecUpdate
        || update instanceof SetDefaultSpecUpdate
        || update instanceof AddSortOrderUpdate
        || update instanceof SetDefaultSortOrderUpdate
        || update instanceof RemoveSchemasUpdate
        || update instanceof RemovePartitionSpecsUpdate;
  }

  /**
   * The table after one pass of the delta's table updates ({@code pass}): one {@code
   * TableMetadata.Builder} from {@code base}, pinned to the landing's last-updated-ms, then {@code
   * build()}. Registers the pass's new snapshots with {@code newSnapshots} and in {@code addedIds},
   * and the ids it removes in {@code removedIds}.
   */
  @SuppressWarnings("MethodLength")
  private static TableFields applyPass(
      TableFields base,
      List<DeltaUpdate> pass,
      DecodedDelta delta,
      Landing landing,
      String baseLocation,
      InlineDeltaCodec.NewSnapshotManifests newSnapshots,
      Set<Long> addedIds,
      List<Long> removedIds) {
    long writerLastUpdatedMillis = delta.lastUpdatedMillis;
    long lastUpdatedMillis = landing.lastUpdatedMillis(writerLastUpdatedMillis);
    int formatVersion = base.formatVersion;
    long lastSequenceNumber = base.lastSequenceNumber;
    long nextRowId = base.nextRowId;
    long currentSnapshotId = base.currentSnapshotId;
    Chain<Snapshot> snapshots = base.snapshots;
    Chain<HistoryEntry> snapshotLog = base.snapshotLog;
    Map<String, SnapshotRef> refs = null; // copied on the first change
    Map<String, String> properties = null; // likewise
    // The builder's statistics, by snapshot, as it indexes them: made on the first removal
    Map<Long, List<StatisticsFile>> statistics = null;
    Map<Long, List<PartitionStatisticsFile>> partitionStatistics = null;
    boolean changed = false;
    boolean removedAny = false;
    // The snapshots this pass added: main moved to one logs it at the snapshot's own timestamp
    Set<Long> addedHere = new LinkedHashSet<>();
    // Snapshots this pass added and moved main to, in order: intermediate ones leave the log
    List<Long> mainMoves = new ArrayList<>();
    List<DeltaUpdate> headUpdates = new ArrayList<>();

    for (DeltaUpdate update : pass) {
      if (update instanceof AddSnapshotUpdate) {
        AddSnapshotUpdate add = (AddSnapshotUpdate) update;
        long sequenceNumber = formatVersion >= 2 ? lastSequenceNumber + 1 : 0L;
        Snapshot snapshot =
            add.snapshot(
                sequenceNumber, base.currentSchemaId, "", writerLastUpdatedMillis, landing);
        // TableMetadata.Builder.addSnapshot
        ValidationException.check(
            snapshots.byKey(snapshot.snapshotId()) == null,
            "Snapshot already exists for id: %s",
            snapshot.snapshotId());
        RetryableValidationException.check(
            formatVersion == 1
                || snapshot.sequenceNumber() > lastSequenceNumber
                || snapshot.parentId() == null,
            "Cannot add snapshot with sequence number %s older than last sequence number %s",
            snapshot.sequenceNumber(),
            lastSequenceNumber);
        lastSequenceNumber = snapshot.sequenceNumber();
        snapshots = snapshots.append(snapshot);
        if (formatVersion >= 3) {
          ValidationException.check(
              snapshot.firstRowId() != null, "Cannot add a snapshot: first-row-id is null");
          RetryableValidationException.check(
              snapshot.firstRowId() != null && snapshot.firstRowId() >= nextRowId,
              "Cannot add a snapshot, first-row-id is behind table next-row-id: %s < %s",
              snapshot.firstRowId(),
              nextRowId);
          nextRowId += snapshot.addedRows();
        }
        addedHere.add(snapshot.snapshotId());
        addedIds.add(add.snapshotId);
        if (add.manifestListSuffix.isEmpty()) {
          newSnapshots.add(add.snapshotId, landing.parent(add));
        }
        changed = true;
      } else if (update instanceof SetSnapshotRefUpdate) {
        SetSnapshotRefUpdate set = landing.ref((SetSnapshotRefUpdate) update);
        SnapshotRef ref = set.toRef();
        Map<String, SnapshotRef> current = refs != null ? refs : base.refs;
        SnapshotRef existing = current.get(set.refName);
        if (existing != null && existing.equals(ref)) {
          continue;
        }
        // TableMetadata.Builder.setRef
        Snapshot snapshot = snapshots.byKey(ref.snapshotId());
        ValidationException.check(
            snapshot != null, "Cannot set %s to unknown snapshot: %s", set.refName,
            ref.snapshotId());
        ValidationException.check(
            !SnapshotRef.MAIN_BRANCH.equals(set.refName) || ref.isBranch(),
            "Cannot set %s to a tag, it must be a branch",
            SnapshotRef.MAIN_BRANCH);
        if (SnapshotRef.MAIN_BRANCH.equals(set.refName)) {
          currentSnapshotId = ref.snapshotId();
          boolean added = addedHere.contains(ref.snapshotId());
          long timeOfChange = added ? snapshot.timestampMillis() : lastUpdatedMillis;
          snapshotLog =
              snapshotLog.append(InlineTableMetadata.snapshotLogEntry(timeOfChange,
                  ref.snapshotId()));
          if (added) {
            mainMoves.add(ref.snapshotId());
          }
        }
        if (refs == null) {
          refs = new HashMap<>(base.refs);
        }
        refs.put(set.refName, ref);
        changed = true;
      } else if (update instanceof SetPropertiesUpdate) {
        SetPropertiesUpdate set = (SetPropertiesUpdate) update;
        if (!set.updated.isEmpty() || !set.removed.isEmpty()) {
          if (properties == null) {
            properties = new HashMap<>(base.properties);
          }
          properties.putAll(set.updated);
          set.removed.forEach(properties::remove);
          changed = true;
        }
      } else if (update instanceof RemoveSnapshotsUpdate) {
        // TableMetadata.Builder.removeSnapshots: the snapshots with their statistics, then every
        // ref left dangling (main's leaves no current snapshot)
        List<Long> ids = ((RemoveSnapshotsUpdate) update).snapshotIds;
        removedIds.addAll(ids);
        Set<Long> toRemove = new HashSet<>(ids);
        List<Snapshot> retained = new ArrayList<>(snapshots.size());
        boolean removedHere = false;
        for (Snapshot snapshot : snapshots) {
          if (toRemove.contains(snapshot.snapshotId())) {
            removedHere = true;
            if (statistics == null) {
              statistics = indexStatistics(base.statisticsFiles);
              partitionStatistics = indexPartitionStatistics(base.partitionStatisticsFiles);
            }
            statistics.remove(snapshot.snapshotId());
            partitionStatistics.remove(snapshot.snapshotId());
          } else {
            retained.add(snapshot);
          }
        }
        if (removedHere) {
          snapshots = Chain.of(retained, Snapshot::snapshotId);
          removedAny = true;
          changed = true;
        }
        Map<String, SnapshotRef> current = refs != null ? refs : base.refs;
        List<String> dangling = new ArrayList<>();
        for (Map.Entry<String, SnapshotRef> ref : current.entrySet()) {
          if (snapshots.byKey(ref.getValue().snapshotId()) == null) {
            dangling.add(ref.getKey());
          }
        }
        if (!dangling.isEmpty()) {
          if (refs == null) {
            refs = new HashMap<>(base.refs);
          }
          for (String name : dangling) {
            if (SnapshotRef.MAIN_BRANCH.equals(name)) {
              currentSnapshotId = -1L;
            }
            refs.remove(name);
          }
          changed = true;
        }
      } else if (headUpdate(update)) {
        headUpdates.add(update);
      } else {
        throw new IllegalStateException(
            "No native replay of " + update.getClass().getSimpleName());
      }
    }

    // The head's updates, on a builder that sees no snapshot, ref, log or statistics: a build
    // without changes returns its base
    TableMetadata head = null;
    if (!headUpdates.isEmpty()) {
      TableMetadata from = base.head();
      TableMetadata.Builder builder = TableMetadata.buildFrom(from);
      builder.setLastUpdatedMillis(lastUpdatedMillis);
      for (DeltaUpdate update : headUpdates) {
        update.applyTo(builder);
      }
      TableMetadata built = builder.discardChanges().build();
      if (built != from) {
        head = built;
        changed = true;
      }
    }
    if (!changed) {
      return base; // the builder has no changes: build() returns its base
    }

    // TableMetadata.Builder.build, which rebuilds the properties, refs and statistics from the
    // builder's hash maps: their order is the maps' even where the pass did not change them
    Map<String, String> newProperties =
        ImmutableMap.copyOf(properties != null ? properties : new HashMap<>(base.properties));
    Map<String, SnapshotRef> newRefs =
        ImmutableMap.copyOf(refs != null ? refs : new HashMap<>(base.refs));
    List<StatisticsFile> statisticsFiles =
        (statistics != null ? statistics : indexStatistics(base.statisticsFiles))
            .values()
            .stream()
            .flatMap(List::stream)
            .collect(Collectors.toList());
    List<PartitionStatisticsFile> partitionStatisticsFiles =
        (partitionStatistics != null
                ? partitionStatistics
                : indexPartitionStatistics(base.partitionStatisticsFiles))
            .values()
            .stream()
            .flatMap(List::stream)
            .collect(Collectors.toList());
    List<TableMetadata.MetadataLogEntry> previousFiles =
        addPreviousFile(base.previousFiles, baseLocation, base.lastUpdatedMillis, newProperties);
    Set<Long> intermediate = new HashSet<>();
    for (long moved : mainMoves) {
      if (moved != currentSnapshotId) {
        intermediate.add(moved);
      }
    }
    boolean rebuiltLog = !intermediate.isEmpty() || removedAny;
    if (rebuiltLog) {
      // TableMetadata.Builder.updateSnapshotLog: copy the entries that are still valid, of
      // snapshots the table has; an entry whose snapshot was removed clears the history before it
      List<HistoryEntry> kept = new ArrayList<>();
      for (HistoryEntry entry : snapshotLog) {
        if (snapshots.byKey(entry.snapshotId()) != null) {
          if (!intermediate.contains(entry.snapshotId())) {
            kept.add(entry);
          }
        } else if (removedAny) {
          kept.clear();
        }
      }
      if (snapshots.byKey(currentSnapshotId) != null) {
        ValidationException.check(
            Iterables.getLast(kept).snapshotId() == currentSnapshotId,
            "Cannot set invalid snapshot log: latest entry is not the current snapshot");
      }
      snapshotLog = Chain.of(kept);
    }

    // The checks of TableMetadata's constructor that the pass could break: a log it rebuilt has
    // new neighbours throughout
    int checkFrom = rebuiltLog ? 1 : Math.max(1, base.snapshotLog.size());
    for (int i = checkFrom; i < snapshotLog.size(); i++) {
      Preconditions.checkArgument(
          snapshotLog.get(i).timestampMillis() - snapshotLog.get(i - 1).timestampMillis()
              >= -ONE_MINUTE,
          "[BUG] Expected sorted snapshot log entries.");
    }
    HistoryEntry lastLog = snapshotLog.last();
    if (lastLog != null) {
      Preconditions.checkArgument(
          lastUpdatedMillis - lastLog.timestampMillis() >= -ONE_MINUTE,
          "Invalid update timestamp %s: before last snapshot log entry at %s",
          lastUpdatedMillis,
          lastLog.timestampMillis());
    }
    int files = previousFiles.size();
    if (files > 0) {
      TableMetadata.MetadataLogEntry last = previousFiles.get(files - 1);
      if (files > 1) {
        Preconditions.checkArgument(
            last.timestampMillis() - previousFiles.get(files - 2).timestampMillis() >= -ONE_MINUTE,
            "[BUG] Expected sorted previous metadata log entries.");
      }
      Preconditions.checkArgument(
          lastUpdatedMillis - last.timestampMillis() >= -ONE_MINUTE,
          "Invalid update timestamp %s: before the latest metadata log entry timestamp %s",
          lastUpdatedMillis,
          last.timestampMillis());
    }

    TableFields after =
        base.with(
            lastSequenceNumber,
            lastUpdatedMillis,
            newProperties,
            currentSnapshotId,
            snapshots,
            snapshotLog,
            previousFiles,
            newRefs,
            ImmutableList.copyOf(statisticsFiles),
            ImmutableList.copyOf(partitionStatisticsFiles),
            nextRowId);
    return head != null ? after.withHead(head) : after;
  }

  /** The builder's index of statistics files by snapshot, as {@code Builder(base)} makes it. */
  private static Map<Long, List<StatisticsFile>> indexStatistics(List<StatisticsFile> files) {
    return files.stream().collect(Collectors.groupingBy(StatisticsFile::snapshotId));
  }

  private static Map<Long, List<PartitionStatisticsFile>> indexPartitionStatistics(
      List<PartitionStatisticsFile> files) {
    return files.stream().collect(Collectors.groupingBy(PartitionStatisticsFile::snapshotId));
  }

  /** {@code TableMetadata.Builder.addPreviousFile}: the metadata log after one more build. */
  private static List<TableMetadata.MetadataLogEntry> addPreviousFile(
      List<TableMetadata.MetadataLogEntry> previousFiles,
      String previousFileLocation,
      long timestampMillis,
      Map<String, String> properties) {
    if (previousFileLocation == null) {
      return previousFiles;
    }
    int maxSize =
        Math.max(
            1,
            PropertyUtil.propertyAsInt(
                properties,
                TableProperties.METADATA_PREVIOUS_VERSIONS_MAX,
                TableProperties.METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT));
    // The log shares its buffer with the version before it, as replay extends it once per version
    @SuppressWarnings("unchecked")
    Window<TableMetadata.MetadataLogEntry> log =
        previousFiles instanceof Window
            ? (Window<TableMetadata.MetadataLogEntry>) previousFiles
            : Window.of(previousFiles);
    return log.append(
        InlineTableMetadata.metadataLogEntry(timestampMillis, previousFileLocation), maxSize);
  }

  /**
   * The table updates of {@code delta} on {@code base} through the builder, as {@link
   * InlineDeltaCodec#applyDelta} replays them: a pass per run of table updates between manifest
   * updates, each one build.
   */
  private static TableMetadata reference(TableMetadata base, DecodedDelta delta, Landing landing) {
    long writerLastUpdatedMillis = delta.lastUpdatedMillis;
    TableMetadata current = base;
    TableMetadata.Builder pending = null;
    long lastSequenceNumber = 0L;
    for (DeltaUpdate update : delta.updates) {
      if (update instanceof AddManifestUpdate || update instanceof RemoveManifestUpdate) {
        if (pending != null) {
          current = pending.discardChanges().build();
          pending = null;
        }
        continue;
      }
      if (pending == null) {
        pending = TableMetadata.buildFrom(current);
        pending.setLastUpdatedMillis(landing.lastUpdatedMillis(writerLastUpdatedMillis));
        lastSequenceNumber = current.lastSequenceNumber();
      }
      if (update instanceof AddSnapshotUpdate) {
        long sequenceNumber = current.formatVersion() >= 2 ? ++lastSequenceNumber : 0L;
        pending.addSnapshot(
            ((AddSnapshotUpdate) update).snapshot(
                sequenceNumber, current.currentSchemaId(), "", writerLastUpdatedMillis,
                landing));
      } else if (update instanceof SetSnapshotRefUpdate) {
        landing.ref((SetSnapshotRefUpdate) update).applyTo(pending);
      } else {
        update.applyTo(pending);
      }
    }
    return pending != null ? pending.discardChanges().build() : current;
  }
}
