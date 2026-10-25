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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;

/**
 * Concern-vector, with blind and validated records judged against the manifests their branch's
 * commits since wrote: a record that advances one branch with one snapshot whose producer says
 * what its retry would check ({@link Footprint.Kind#BLIND} or {@link Footprint.Kind#VALIDATED})
 * depends on no version of its branch, only on its checks, which it judges as Iceberg's retry
 * would: over the snapshots from its parent (exclusive) to its branch's head where it lands, each
 * manifest a snapshot wrote, by its content, its counts and its partition summaries, against each
 * check's scope (every file; a row filter, through {@link ManifestEvaluator}; partitions, by the
 * summaries' bounds). So it admits what {@link PartitionVectorEncoding} admits, and also a range
 * filter's commit, and a commit racing commits on other branches.
 *
 * <p>It keeps no partition stamps: the window is the table's own state. It reads, beyond the
 * window, the guards that make it stable (S2): its branch not reset (so its parent stays an
 * ancestor of the head), no snapshots removed (so the window's snapshots and manifest lists stay),
 * the specs and the table unchanged, and for a validated record the schema; a blind record that
 * merged manifests reads its branch's drops, as blind-append does. A window it cannot see (a
 * snapshot without an inline list, or longer than {@link #MAX_WINDOW}) conflicts. Every commit's
 * files count, whatever its operation, as in partition-vector. Any other record is judged as
 * concern-vector judges it.
 */
final class ManifestWindowEncoding extends ConcernVectorEncoding {

  static final String NAME = "manifest-window";

  static final ManifestWindowEncoding INSTANCE = new ManifestWindowEncoding();

  /** The most snapshots a window spans before a record conflicts. */
  static final int MAX_WINDOW = 1000;

  private ManifestWindowEncoding() {
    super(0);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  Witness reads(int base, TableChange change, ReplayView landing) {
    if (!PartitionVectorEncoding.validatedOnOneBranch(change) || change.snapshots.size() != 1) {
      return super.reads(base, change, landing);
    }
    String branch = change.advanced.iterator().next();
    Witness witness = BlindAppendEncoding.guards(base, change);
    if (change.blind) {
      if (!change.drops.isEmpty()) {
        witness.ref(branch, ConcernStamps.Ref.DROP);
      }
    } else {
      witness.concern(TableChange.Concern.SCHEMA);
    }
    List<ManifestFile> written = new ArrayList<>();
    for (ManifestFile manifest : change.written) {
      if (manifest.content() == ManifestContent.DATA && manifest.hasAddedFiles()) {
        written.add(manifest);
      }
    }
    return witness.window(
        new ChecksSince(branch, change.snapshots.get(0).parentSnapshotId, change.checks, written));
  }

  /** The checks of a record whose snapshot's parent is {@code parent}, over the window since. */
  private static final class ChecksSince implements Window {
    private final String branch;
    private final Long parent;
    private final List<Footprint.Check> checks;
    /** The data manifests the record adds files in, which, in v3, deletes since must not touch. */
    private final List<ManifestFile> written;

    ChecksSince(String branch, Long parent, List<Footprint.Check> checks,
        List<ManifestFile> written) {
      this.branch = branch;
      this.parent = parent;
      this.checks = checks;
      this.written = written;
    }

    @Override
    public boolean holds(ReplayView landing) {
      try {
        return judge(landing);
      } catch (RuntimeException e) {
        return false; // total: a window it cannot read conflicts
      }
    }

    private boolean judge(ReplayView landing) {
      TableMetadata table = landing.metadata();
      SnapshotRef ref = table.ref(branch);
      // a branch it creates lands on main's head, as Landing re-parents it
      Long at =
          ref != null
              ? Long.valueOf(ref.snapshotId())
              : table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : null;
      List<Snapshot> window = new ArrayList<>();
      while (!Objects.equals(at, parent)) {
        Snapshot snapshot = at != null ? table.snapshot(at) : null;
        if (snapshot == null || window.size() >= MAX_WINDOW) {
          return false; // its parent is not an ancestor of the head, or the window is unseen
        }
        window.add(snapshot);
        at = snapshot.parentId();
      }
      boolean v3 = table.formatVersion() >= 3;
      for (Snapshot snapshot : window) {
        long id = snapshot.snapshotId();
        if (!landing.hasInlineList(id)) {
          return false;
        }
        for (String path : landing.listPaths(id)) {
          ManifestFile manifest = landing.manifest(path);
          if (manifest == null) {
            return false;
          }
          if (manifest.snapshotId() != null && manifest.snapshotId() != id) {
            continue; // written by an earlier snapshot
          }
          if (!passes(manifest, table) || (v3 && touchesWritten(manifest, table))) {
            return false;
          }
        }
      }
      return true;
    }

    /** Whether no check reads what {@code manifest}'s files did. */
    private boolean passes(ManifestFile manifest, TableMetadata table) {
      for (Footprint.Check check : checks) {
        boolean did;
        switch (check.kind) {
          case NO_DATA_ADDED:
            did = manifest.content() == ManifestContent.DATA && manifest.hasAddedFiles();
            break;
          case NO_DELETES_ADDED:
            did = manifest.content() == ManifestContent.DELETES && manifest.hasAddedFiles();
            break;
          default:
            did = manifest.content() == ManifestContent.DATA && manifest.hasDeletedFiles();
        }
        if (did && inScope(manifest, check.scope, table)) {
          return false;
        }
      }
      return true;
    }

    /** Whether {@code manifest} adds delete files that may apply to the record's new files. */
    private boolean touchesWritten(ManifestFile manifest, TableMetadata table) {
      if (manifest.content() != ManifestContent.DELETES || !manifest.hasAddedFiles()) {
        return false;
      }
      for (ManifestFile mine : written) {
        if (overlaps(manifest, mine, table)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public String toString() {
      return "checks " + checks + " on " + branch + " since " + parent;
    }
  }

  /** Whether {@code manifest} may hold files in {@code scope}; true where it cannot tell. */
  static boolean inScope(ManifestFile manifest, Footprint.Scope scope, TableMetadata table) {
    PartitionSpec spec = table.specsById().get(manifest.partitionSpecId());
    if (scope.isEverything() || spec == null) {
      return true;
    }
    try {
      if (scope.filter != null) {
        return ManifestEvaluator.forRowFilter(scope.expression(), spec, true).eval(manifest);
      }
      if (manifest.content() == ManifestContent.DELETES && spec.isUnpartitioned()) {
        return true; // global deletes apply to every partition
      }
      for (Footprint.Partition partition : scope.partitions) {
        if (partition.specId == manifest.partitionSpecId() && contains(manifest, partition, spec)) {
          return true;
        }
      }
      return false;
    } catch (RuntimeException e) {
      return true;
    }
  }

  /** Whether {@code manifest}'s partition summaries admit {@code partition}, of its spec. */
  private static boolean contains(
      ManifestFile manifest, Footprint.Partition partition, PartitionSpec spec) {
    List<ManifestFile.PartitionFieldSummary> summaries = manifest.partitions();
    if (summaries == null || summaries.size() != partition.values.size()) {
      return true;
    }
    for (int i = 0; i < summaries.size(); i++) {
      ManifestFile.PartitionFieldSummary summary = summaries.get(i);
      ByteBuffer value = partition.values.get(i);
      Type type = spec.partitionType().fields().get(i).type();
      if (value == null) {
        if (!summary.containsNull()) {
          return false;
        }
      } else if (!inBounds(value, summary.lowerBound(), summary.upperBound(), type)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Whether two manifests' files may share a partition: true if either is an unpartitioned spec's
   * delete manifest, false for two specs, else whether every field's bounds meet.
   */
  static boolean overlaps(ManifestFile deletes, ManifestFile data, TableMetadata table) {
    PartitionSpec spec = table.specsById().get(deletes.partitionSpecId());
    if (spec == null || spec.isUnpartitioned()) {
      return true;
    }
    if (deletes.partitionSpecId() != data.partitionSpecId()) {
      return false;
    }
    List<ManifestFile.PartitionFieldSummary> a = deletes.partitions();
    List<ManifestFile.PartitionFieldSummary> b = data.partitions();
    if (a == null || b == null || a.size() != b.size()) {
      return true;
    }
    try {
      for (int i = 0; i < a.size(); i++) {
        Type type = spec.partitionType().fields().get(i).type();
        ManifestFile.PartitionFieldSummary x = a.get(i);
        ManifestFile.PartitionFieldSummary y = b.get(i);
        boolean nulls = x.containsNull() && y.containsNull();
        // two ranges meet if either's lower bound is within the other
        boolean values =
            x.lowerBound() != null
                && y.lowerBound() != null
                && (inBounds(x.lowerBound(), y.lowerBound(), y.upperBound(), type)
                    || inBounds(y.lowerBound(), x.lowerBound(), x.upperBound(), type));
        if (!nulls && !values) {
          return false;
        }
      }
      return true;
    } catch (RuntimeException e) {
      return true;
    }
  }

  /**
   * Whether {@code value} is within {@code lower} and {@code upper}; true for a floating-point
   * field (NaN is outside any bounds) and where a bound is missing only if both are.
   */
  private static boolean inBounds(ByteBuffer value, ByteBuffer lower, ByteBuffer upper, Type type) {
    if (type.typeId() == Type.TypeID.FLOAT || type.typeId() == Type.TypeID.DOUBLE) {
      return true;
    }
    if (lower == null || upper == null) {
      return false; // every value null, or no files
    }
    Comparator<Object> order = Comparators.forType(type.asPrimitiveType());
    Object v = Conversions.fromByteBuffer(type, value.duplicate());
    return order.compare(v, Conversions.fromByteBuffer(type, lower.duplicate())) >= 0
        && order.compare(v, Conversions.fromByteBuffer(type, upper.duplicate())) <= 0;
  }
}
