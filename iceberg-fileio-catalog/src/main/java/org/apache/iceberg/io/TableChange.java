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
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.InlineDeltaCodec.AddSnapshotUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.DeltaUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.SetSnapshotRefUpdate;

/**
 * What a table action changes, by concern: what conflict encodings that separate a table's
 * concerns judge a record by (its witness) and stamp when one applies (observe).
 *
 * <p>A delta's changes come from its updates. A ref it moves to a snapshot it adds is an advance;
 * any other ref change is a reset: a ref moved to an existing snapshot (a rollback, a fast-forward,
 * a cherry-pick that fast-forwards), a ref created at one, removed, or given new retention. Full
 * metadata's changes come from comparing the table before and after, where both are inline; every
 * ref it changes is a reset.
 */
final class TableChange {

  /** A table's concerns, apart from its refs. */
  enum Concern {
    PROPERTIES,
    SCHEMA,
    SPECS,
    SORT,
    /** The table itself: location, format version, uuid, encryption keys. */
    TABLE,
    STATISTICS,
    /** Snapshots removed: an expiry, or a removed ref's. */
    REMOVAL
  }

  final EnumSet<Concern> concerns;
  final Set<String> advanced;
  final Set<String> reset;
  /** The refs it gives new retention, on the snapshot each had. */
  final Set<String> retained;
  /** Whether it adds snapshots: a data commit. */
  final boolean addsSnapshots;
  /** Whether it adds a snapshot no ref moves to (staged), whose branch is not in the record. */
  final boolean stages;
  /**
   * Whether Iceberg commits it once, failing on any table that changed since it read it: a
   * schema or partition-spec update, or a ref change through {@code manageSnapshots} (a reset, or
   * an advance that also changes the branch's retention).
   */
  final boolean commitOnce;
  /** The branches it advances to a snapshot that drops manifests from its parent's list. */
  final Set<String> drops;
  /** The manifests its new snapshots wrote. */
  final List<ManifestFile> written;
  /**
   * Whether it is blind: snapshots whose producers say they validate nothing ({@link
   * Footprint#BLIND}), and their refs and manifest lists, and nothing else.
   */
  final boolean blind;
  /**
   * Whether it is validated: snapshots whose producers say what their retries check ({@link
   * Footprint.Kind#VALIDATED}, or blind), and their refs and manifest lists, and nothing else.
   */
  final boolean validated;
  /** What its validated snapshots' retries check, in their order. */
  final List<Footprint.Check> checks;
  /** The snapshots it adds, in its order. */
  final List<AddSnapshotUpdate> snapshots;

  private TableChange(
      EnumSet<Concern> concerns,
      Set<String> advanced,
      Set<String> reset,
      Set<String> retained,
      boolean addsSnapshots,
      boolean stages,
      boolean commitOnce,
      Set<String> drops,
      List<ManifestFile> written,
      boolean blind,
      boolean validated,
      List<Footprint.Check> checks,
      List<AddSnapshotUpdate> snapshots) {
    this.concerns = concerns;
    this.advanced = advanced;
    this.reset = reset;
    this.retained = retained;
    this.addsSnapshots = addsSnapshots;
    this.stages = stages;
    this.commitOnce = commitOnce;
    this.drops = drops;
    this.written = written;
    this.blind = blind;
    this.validated = validated;
    this.checks = checks;
    this.snapshots = snapshots;
  }

  /**
   * What the delta of {@code action} changes on {@code table}: the table it lands on, for a
   * record's witness, or the table before it, when it applies. {@code table} is decoded only for
   * the refs a delta sets or the snapshots it removes.
   */
  static TableChange of(TableAction action, java.util.function.Supplier<TableMetadata> metadata) {
    TableMetadata table = null;
    boolean needsTable = false;
    for (DeltaUpdate update : action.delta().updates) {
      needsTable |= update instanceof SetSnapshotRefUpdate
          || update instanceof InlineDeltaCodec.RemoveSnapshotsUpdate;
    }
    if (needsTable) {
      table = metadata.get();
    }
    EnumSet<Concern> concerns = EnumSet.noneOf(Concern.class);
    Set<String> advanced = new HashSet<>();
    Set<String> reset = new HashSet<>();
    Set<String> retained = new HashSet<>();
    Set<Long> added = new HashSet<>();
    for (AddSnapshotUpdate add : action.addedSnapshots()) {
      added.add(add.snapshotId);
    }
    Set<Long> onRefs = new HashSet<>();
    Map<String, Long> heads = new java.util.HashMap<>();
    boolean commitOnce = false;
    boolean blind = !added.isEmpty();
    boolean validated = !added.isEmpty();
    List<Footprint.Check> checks = new ArrayList<>();
    for (AddSnapshotUpdate add : action.addedSnapshots()) {
      Footprint.Kind kind = add.footprint != null ? add.footprint.kind : Footprint.Kind.OPAQUE;
      blind &= kind == Footprint.Kind.BLIND;
      validated &= kind != Footprint.Kind.OPAQUE;
      if (kind == Footprint.Kind.VALIDATED) {
        checks.addAll(add.footprint.checks);
      }
    }
    Set<Long> dropping = new HashSet<>();
    List<ManifestFile> written = new ArrayList<>();
    for (DeltaUpdate update : action.delta().updates) {
      boolean snapshotsOnly = update instanceof AddSnapshotUpdate
          || update instanceof SetSnapshotRefUpdate
          || update instanceof InlineDeltaCodec.AddManifestUpdate
          || update instanceof InlineDeltaCodec.RemoveManifestUpdate;
      blind &= snapshotsOnly;
      validated &= snapshotsOnly;
      if (update instanceof InlineDeltaCodec.RemoveManifestUpdate) {
        dropping.add(((InlineDeltaCodec.RemoveManifestUpdate) update).snapshotId);
      } else if (update instanceof InlineDeltaCodec.AddManifestUpdate) {
        ManifestFile manifest = ((InlineDeltaCodec.AddManifestUpdate) update).manifest;
        if (added.contains(manifest.snapshotId())) {
          written.add(manifest);
        }
      }
      if (update instanceof InlineDeltaCodec.SetPropertiesUpdate) {
        concerns.add(Concern.PROPERTIES);
      } else if (update instanceof InlineDeltaCodec.SetLocationUpdate) {
        concerns.add(Concern.TABLE);
      } else if (update instanceof InlineDeltaCodec.AddSchemaUpdate
          || update instanceof InlineDeltaCodec.SetCurrentSchemaUpdate
          || update instanceof InlineDeltaCodec.RemoveSchemasUpdate) {
        concerns.add(Concern.SCHEMA);
        commitOnce |= !(update instanceof InlineDeltaCodec.RemoveSchemasUpdate);
      } else if (update instanceof InlineDeltaCodec.AddPartitionSpecUpdate
          || update instanceof InlineDeltaCodec.SetDefaultSpecUpdate
          || update instanceof InlineDeltaCodec.RemovePartitionSpecsUpdate) {
        concerns.add(Concern.SPECS);
        commitOnce |= !(update instanceof InlineDeltaCodec.RemovePartitionSpecsUpdate);
      } else if (update instanceof InlineDeltaCodec.AddSortOrderUpdate
          || update instanceof InlineDeltaCodec.SetDefaultSortOrderUpdate) {
        concerns.add(Concern.SORT);
      } else if (update instanceof InlineDeltaCodec.RemoveSnapshotsUpdate) {
        concerns.add(Concern.REMOVAL);
        Set<Long> removed = new HashSet<>(((InlineDeltaCodec.RemoveSnapshotsUpdate) update).snapshotIds);
        if (table != null) {
          for (Map.Entry<String, SnapshotRef> ref : table.refs().entrySet()) {
            if (removed.contains(ref.getValue().snapshotId())) {
              reset.add(ref.getKey());
            }
          }
        }
      } else if (update instanceof SetSnapshotRefUpdate) {
        SetSnapshotRefUpdate ref = (SetSnapshotRefUpdate) update;
        SnapshotRef current = table != null ? table.ref(ref.refName) : null;
        if (added.contains(ref.snapshotId)
            && "branch".equals(ref.refType)
            && (current == null || (current.isBranch() && sameRetention(current, ref)))) {
          advanced.add(ref.refName);
          onRefs.add(ref.snapshotId);
          heads.put(ref.refName, ref.snapshotId);
        } else if (current != null && current.snapshotId() == ref.snapshotId
            && current.isBranch() == "branch".equals(ref.refType)) {
          retained.add(ref.refName);
          commitOnce = true;
        } else {
          reset.add(ref.refName);
          commitOnce = true;
        }
      }
    }
    // A snapshot on a chain to an advanced ref is not staged.
    boolean stages = false;
    Map<Long, Long> parents = new java.util.HashMap<>();
    for (AddSnapshotUpdate add : action.addedSnapshots()) {
      parents.put(add.snapshotId, add.parentSnapshotId);
    }
    Set<Long> chained = new HashSet<>();
    for (Long head : onRefs) {
      for (Long s = head; s != null && added.contains(s) && chained.add(s); s = parents.get(s)) {
        // walks the chain back to its first new snapshot
      }
    }
    for (Long id : added) {
      stages |= !chained.contains(id);
    }
    Set<String> drops = new HashSet<>();
    heads.forEach(
        (ref, head) -> {
          Set<Long> seen = new HashSet<>();
          for (Long s = head; s != null && added.contains(s) && seen.add(s); s = parents.get(s)) {
            if (dropping.contains(s)) {
              drops.add(ref);
              break;
            }
          }
        });
    return new TableChange(concerns, advanced, reset, retained, !added.isEmpty(), stages,
        commitOnce, drops, written, blind, validated, checks, action.addedSnapshots());
  }

  /**
   * What changed from {@code before} to {@code after}, two versions of a table, if it is only
   * their properties or statistics (a statistics change goes as full metadata); null for any other
   * change, which an encoding cannot see into.
   */
  static TableChange diff(TableMetadata before, TableMetadata after) {
    com.fasterxml.jackson.databind.node.ObjectNode was = json(before);
    com.fasterxml.jackson.databind.node.ObjectNode is = json(after);
    EnumSet<Concern> concerns = EnumSet.noneOf(Concern.class);
    if (!Objects.equals(was.remove("properties"), is.remove("properties"))) {
      concerns.add(Concern.PROPERTIES);
    }
    boolean statistics = !Objects.equals(was.remove("statistics"), is.remove("statistics"));
    statistics |= !Objects.equals(was.remove("partition-statistics"), is.remove("partition-statistics"));
    if (statistics) {
      concerns.add(Concern.STATISTICS);
    }
    for (String field : new String[] {"last-updated-ms", "metadata-log"}) {
      was.remove(field);
      is.remove(field);
    }
    if (!was.equals(is)) {
      return null;
    }
    return new TableChange(
        concerns, Set.of(), Set.of(), Set.of(), false, false, false, Set.of(), List.of(), false,
        false, List.of(), List.of());
  }

  private static com.fasterxml.jackson.databind.node.ObjectNode json(TableMetadata metadata) {
    try {
      return (com.fasterxml.jackson.databind.node.ObjectNode)
          org.apache.iceberg.util.JsonUtil.mapper()
              .readTree(org.apache.iceberg.TableMetadataParser.toJson(metadata));
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }

  private static boolean sameRetention(SnapshotRef current, SetSnapshotRefUpdate update) {
    return update.minSnapshotsToKeep == orZero(current.minSnapshotsToKeep())
        && update.maxSnapshotAgeMs == orZero(current.maxSnapshotAgeMs())
        && update.maxRefAgeMs == orZero(current.maxRefAgeMs());
  }

  private static long orZero(Number value) {
    return value != null ? value.longValue() : 0L;
  }

  /** Whether it changes nothing but properties. */
  boolean onlyProperties() {
    return concerns.equals(EnumSet.of(Concern.PROPERTIES))
        && advanced.isEmpty()
        && reset.isEmpty()
        && retained.isEmpty()
        && !addsSnapshots;
  }

  @Override
  public String toString() {
    return concerns + (advanced.isEmpty() ? "" : " advances " + advanced)
        + (reset.isEmpty() ? "" : " resets " + reset)
        + (retained.isEmpty() ? "" : " retains " + retained)
        + (addsSnapshots ? " adds snapshots" : "")
        + (stages ? " stages" : "") + (commitOnce ? " commit-once" : "");
  }
}
