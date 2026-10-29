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
import org.apache.iceberg.io.InlineDeltaCodec.AddSnapshotUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.DecodedDelta;
import org.apache.iceberg.io.InlineDeltaCodec.DeltaUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.RemoveManifestUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.SetPropertiesUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.SetSnapshotRefUpdate;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.PropertyUtil;

/**
 * Replay of a table update's delta on {@link TableFields}, without Iceberg's metadata builder:
 * the common records, which add snapshots, move refs and set properties, before their manifest
 * updates. {@link InlineDeltaCodec#applyDelta} replays every delta on a {@link TableMetadata}
 * through {@code TableMetadata.Builder}; for these, this computes the same fields, field for field
 * (the builder's checks and its snapshot and metadata logs included), and shares what they leave.
 * Any other delta, schema, spec, sort order, location, removals, takes the builder's path.
 *
 * <p>With {@code -Dfileio.catalog.replay.check=true} (the test suites set it), each replay here is
 * also run through the builder, and the two must make the same metadata.
 */
final class NativeDelta {
  private NativeDelta() {}

  private static final long ONE_MINUTE = TimeUnit.MINUTES.toMillis(1);

  /** Whether replays here are checked against the builder's. */
  static final boolean CHECK = Boolean.getBoolean("fileio.catalog.replay.check");

  /**
   * Whether {@code delta} replays here: its table updates add snapshots, set refs and set or
   * remove properties, and all of them precede its manifest updates.
   */
  static boolean applies(DecodedDelta delta) {
    boolean manifests = false;
    for (DeltaUpdate update : delta.updates) {
      if (update instanceof AddManifestUpdate || update instanceof RemoveManifestUpdate) {
        manifests = true;
      } else if (manifests) {
        return false;
      } else if (!(update instanceof AddSnapshotUpdate)
          && !(update instanceof SetSnapshotRefUpdate)
          && !(update instanceof SetPropertiesUpdate)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Applies {@code delta} ({@link #applies}) to {@code base}, the table where its record lands,
   * which has changed since the delta's base if {@code stale}, and returns the table after it; its
   * manifest updates go to {@code builder}'s lists of {@code tableId}, as {@link
   * InlineDeltaCodec#applyDelta} routes them. {@code baseLocation} is the base's metadata
   * location, which the metadata log records.
   */
  static TableFields apply(
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
    TableFields current = applyTable(base, delta, landing, prefix, baseLocation);

    // The manifest updates, as InlineDeltaCodec.applyDelta replays them: each new inline-ML
    // snapshot's list starts as its parent's, then takes the manifests the delta adds and removes
    InlineDeltaCodec.NewSnapshotManifests newSnapshots =
        new InlineDeltaCodec.NewSnapshotManifests(builder, tableId, delta);
    Set<Long> added = new HashSet<>();
    for (DeltaUpdate update : delta.updates) {
      if (update instanceof AddSnapshotUpdate) {
        AddSnapshotUpdate add = (AddSnapshotUpdate) update;
        added.add(add.snapshotId);
        if (add.manifestListSuffix.isEmpty()) {
          newSnapshots.add(add.snapshotId, landing.parent(add));
        }
      }
    }
    for (DeltaUpdate update : delta.updates) {
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
      } else if (update instanceof RemoveManifestUpdate) {
        newSnapshots.removeManifest(current, (RemoveManifestUpdate) update);
      }
    }
    newSnapshots.finish();
    return current;
  }

  /**
   * The table after the delta's table updates: one pass of {@code TableMetadata.Builder} from
   * {@code base}, pinned to the landing's last-updated-ms, then {@code build()}.
   */
  private static TableFields applyTable(
      TableFields base, DecodedDelta delta, Landing landing, String prefix, String baseLocation) {
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
    boolean changed = false;
    Set<Long> addedIds = new LinkedHashSet<>();
    // Snapshots this pass added and moved main to, in order: intermediate ones leave the log
    List<Long> mainMoves = new ArrayList<>();

    for (DeltaUpdate update : delta.updates) {
      if (update instanceof AddSnapshotUpdate) {
        AddSnapshotUpdate add = (AddSnapshotUpdate) update;
        long sequenceNumber = formatVersion >= 2 ? lastSequenceNumber + 1 : 0L;
        Snapshot snapshot =
            add.snapshot(
                sequenceNumber, base.currentSchemaId, prefix, writerLastUpdatedMillis, landing);
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
        addedIds.add(snapshot.snapshotId());
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
          boolean addedHere = addedIds.contains(ref.snapshotId());
          long timeOfChange = addedHere ? snapshot.timestampMillis() : lastUpdatedMillis;
          snapshotLog =
              snapshotLog.append(InlineTableMetadata.snapshotLogEntry(timeOfChange,
                  ref.snapshotId()));
          if (addedHere) {
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
        base.statisticsFiles.stream()
            .collect(Collectors.groupingBy(StatisticsFile::snapshotId))
            .values()
            .stream()
            .flatMap(List::stream)
            .collect(Collectors.toList());
    List<PartitionStatisticsFile> partitionStatisticsFiles =
        base.partitionStatisticsFiles.stream()
            .collect(Collectors.groupingBy(PartitionStatisticsFile::snapshotId))
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
    if (!intermediate.isEmpty()) {
      // updateSnapshotLog: copy the entries that are still valid, of snapshots the table has
      List<HistoryEntry> kept = new ArrayList<>();
      for (HistoryEntry entry : snapshotLog) {
        if (snapshots.byKey(entry.snapshotId()) != null
            && !intermediate.contains(entry.snapshotId())) {
          kept.add(entry);
        }
      }
      if (snapshots.byKey(currentSnapshotId) != null) {
        ValidationException.check(
            !kept.isEmpty() && kept.get(kept.size() - 1).snapshotId() == currentSnapshotId,
            "Cannot set invalid snapshot log: latest entry is not the current snapshot");
      }
      snapshotLog = Chain.of(kept);
    }

    // The checks of TableMetadata's constructor that the pass could break: a log it rebuilt has
    // new neighbours throughout
    int checkFrom = intermediate.isEmpty() ? Math.max(1, base.snapshotLog.size()) : 1;
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

    return base.with(
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
    int from = previousFiles.size() >= maxSize ? previousFiles.size() - maxSize + 1 : 0;
    ImmutableList.Builder<TableMetadata.MetadataLogEntry> log = ImmutableList.builder();
    log.addAll(previousFiles.subList(from, previousFiles.size()));
    log.add(InlineTableMetadata.metadataLogEntry(timestampMillis, previousFileLocation));
    return log.build();
  }

  /**
   * Checks the replay of {@code delta} here, {@code applied}, against the builder's replay of it
   * on {@code base} at {@code baseLocation} ({@link #CHECK}): they must make the same metadata.
   */
  static void check(
      TableFields base, String baseLocation, DecodedDelta delta, boolean stale, String prefix,
      TableFields applied) {
    TableMetadata from = base.toMetadata(baseLocation);
    Landing landing = Landing.of(delta, MetadataView.of(from), stale);
    TableMetadata reference = reference(from, delta, landing, prefix != null ? prefix : "");
    String expected = TableMetadataParser.toJson(reference);
    String actual = TableMetadataParser.toJson(applied.toMetadata(null));
    if (!expected.equals(actual)) {
      throw new IllegalStateException(
          "Native replay differs from the builder's:\n  builder: " + expected + "\n  native:  "
              + actual);
    }
  }

  /** The table updates of {@code delta} on {@code base}, through the builder, as applyDelta does. */
  private static TableMetadata reference(
      TableMetadata base, DecodedDelta delta, Landing landing, String prefix) {
    long writerLastUpdatedMillis = delta.lastUpdatedMillis;
    TableMetadata.Builder pending = null;
    long lastSequenceNumber = 0L;
    for (DeltaUpdate update : delta.updates) {
      if (update instanceof AddManifestUpdate || update instanceof RemoveManifestUpdate) {
        continue;
      }
      if (pending == null) {
        pending = TableMetadata.buildFrom(base);
        pending.setLastUpdatedMillis(landing.lastUpdatedMillis(writerLastUpdatedMillis));
        lastSequenceNumber = base.lastSequenceNumber();
      }
      if (update instanceof AddSnapshotUpdate) {
        long sequenceNumber = base.formatVersion() >= 2 ? ++lastSequenceNumber : 0L;
        pending.addSnapshot(
            ((AddSnapshotUpdate) update).snapshot(
                sequenceNumber, base.currentSchemaId(), prefix, writerLastUpdatedMillis,
                landing));
      } else if (update instanceof SetSnapshotRefUpdate) {
        landing.ref((SetSnapshotRefUpdate) update).applyTo(pending);
      } else {
        update.applyTo(pending);
      }
    }
    return pending != null ? pending.discardChanges().build() : base;
  }
}
