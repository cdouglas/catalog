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

import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;

/**
 * Concern-vector, with appends as base-free records: a commit whose producer says it validates
 * nothing ({@link org.apache.iceberg.CommitFootprint#BLIND}: a fast or merge append) depends on no
 * version of its branch, so concurrent appends to one branch all land, each on the one before
 * (streaming ingest). Its snapshot takes the branch's head as its parent where it lands, with its
 * sequence number, row ids and totals counted from there ({@link Landing}), as Iceberg's retry
 * commits it.
 *
 * <p>A blind record still conflicts with a change that would fail or change its retry, or that it
 * cannot see past:
 *
 * <ul>
 *   <li>its branch reset (a rollback, a fast-forward, the branch removed or recreated), or, if it
 *       merged manifests, an advance that dropped manifests from the branch's list (a delete, a
 *       rewrite, another merge);
 *   <li>snapshots removed (an expiry), so that a snapshot id or manifest path it names, if gone,
 *       is never new again;
 *   <li>the table's specs or the table itself (location, format, uuid);
 *   <li>in a v3 table, delete files added in the partitions of the files it adds.
 * </ul>
 *
 * Where its branch's head has no inline list, a snapshot it adds has a manifest list file, or
 * it adds more than one snapshot to a moved table, it cannot be finalized and conflicts. Any other
 * record is judged as {@link ConcernVectorEncoding} judges it.
 */
class BlindAppendEncoding extends ConcernVectorEncoding {

  static final String NAME = "blind-append";

  static final BlindAppendEncoding INSTANCE = new BlindAppendEncoding();

  /** The partition buckets: a partition's stamps are its bucket's. */
  static final int PARTITION_BUCKETS = 64;

  private BlindAppendEncoding() {
    this(PARTITION_BUCKETS);
  }

  /** An encoding of {@code partitionBuckets} partition buckets. */
  BlindAppendEncoding(int partitionBuckets) {
    super(partitionBuckets);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  Witness reads(int base, TableChange change, ReplayView landing) {
    if (!change.blind || change.advanced.size() != 1 || !change.reset.isEmpty()
        || change.stages) {
      return super.reads(base, change, landing);
    }
    Witness witness = guards(base, change);
    if (!change.drops.isEmpty()) {
      witness.ref(change.advanced.iterator().next(), ConcernStamps.Ref.DROP);
    }
    if (landing.metadata().formatVersion() >= 3) {
      for (ManifestFile manifest : change.written) {
        if (manifest.content() == ManifestContent.DATA) {
          witness.partition(
              ConcernStamps.Partition.DELETES_ADDED, PartitionKey.bucket(manifest, buckets()));
        }
      }
    }
    return witness;
  }

  /**
   * What a record that advances one branch with snapshots its producers judged (blind, or
   * validated) reads, whatever its producers check: its branch not reset, no snapshots removed,
   * its specs and the table unchanged.
   */
  static Witness guards(int base, TableChange change) {
    return Witness.of(base)
        .concern(TableChange.Concern.TABLE)
        .concern(TableChange.Concern.SPECS)
        .concern(TableChange.Concern.REMOVAL)
        .ref(change.advanced.iterator().next(), ConcernStamps.Ref.RESET);
  }

  /** The partition buckets. */
  int buckets() {
    return PARTITION_BUCKETS;
  }

  /**
   * Stamps, in the bucket of each manifest the change's snapshots wrote, what its files did: data
   * files added or removed, delete files added. A manifest with neither (a merged or rewritten
   * one's existing files) changes no partition; delete files removed are not a change any check
   * reads.
   */
  @Override
  void stamp(TableChange change, int version, ConcernStamps stamps) {
    super.stamp(change, version, stamps);
    for (ManifestFile manifest : change.written) {
      int bucket = PartitionKey.bucket(manifest, buckets());
      if (manifest.content() == ManifestContent.DELETES) {
        if (manifest.hasAddedFiles()) {
          stamps.stampPartition(ConcernStamps.Partition.DELETES_ADDED, bucket, version);
        }
      } else {
        if (manifest.hasAddedFiles()) {
          stamps.stampPartition(ConcernStamps.Partition.DATA_ADDED, bucket, version);
        }
        if (manifest.hasDeletedFiles()) {
          stamps.stampPartition(ConcernStamps.Partition.DATA_REMOVED, bucket, version);
        }
      }
    }
  }
}
