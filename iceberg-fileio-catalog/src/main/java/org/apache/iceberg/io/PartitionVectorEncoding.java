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
 * Blind-append, with validated records judged by partition: a commit whose producer says what its
 * retry would check ({@link Footprint.Kind#VALIDATED}: an overwrite, a row delta, a rewrite, a
 * delete, a dynamic partition overwrite) depends on no version of its branch, only on its checks.
 * Each check is that no commit since added data files, added delete files, or removed data files in
 * a scope; the encoding keeps, for each of 64 partition buckets (a partition's spec and values
 * hashed), the version at which each of those last happened there, and admits the record if none
 * did, in the buckets each check's scope covers, since its writer read the table:
 *
 * <ul>
 *   <li>every bucket, for a scope of every file;
 *   <li>each partition's bucket, for a scope of partitions;
 *   <li>for a row filter, the buckets of the partitions its inclusive projection onto each of the
 *       table's specs names by equality, or every bucket if it names none (a range filter).
 * </ul>
 *
 * Files are stamped by manifest: the bucket of a manifest's partition summaries, or every bucket
 * for one that spans partitions or holds delete files of an unpartitioned spec (global deletes).
 * Every commit's files count, whatever its operation and branch: Iceberg's validations skip some
 * operations (a compaction's added files are not data added), so this admits fewer than Iceberg's
 * retry would, never more.
 *
 * <p>A validated record also reads blind-append's guards (its branch not reset, no snapshots
 * removed, its specs and the table unchanged, and in v3 no delete files added in its partitions),
 * and the schema, which its retry binds its filters to. The manifests it removes must be in its
 * branch's head ({@link Landing#unmergeable}): another commit that removed or rewrote one changed
 * what it removes. Any other record is judged as blind-append judges it.
 */
final class PartitionVectorEncoding extends BlindAppendEncoding {

  static final String NAME = "partition-vector";

  static final PartitionVectorEncoding INSTANCE = new PartitionVectorEncoding();

  private PartitionVectorEncoding() {
    super(PARTITION_BUCKETS);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  Witness reads(int base, TableChange change, ReplayView landing) {
    if (change.blind || !validatedOnOneBranch(change)) {
      return super.reads(base, change, landing);
    }
    Witness witness = guards(base, change).concern(TableChange.Concern.SCHEMA);
    for (Footprint.Check check : change.checks) {
      ConcernStamps.Partition kind = kind(check.kind);
      for (int bucket : PartitionKey.buckets(check.scope, landing.metadata(), buckets())) {
        witness.partition(kind, bucket);
      }
    }
    if (landing.metadata().formatVersion() >= 3) {
      for (ManifestFile manifest : change.written) {
        if (manifest.content() == ManifestContent.DATA && manifest.hasAddedFiles()) {
          witness.partition(
              ConcernStamps.Partition.DELETES_ADDED, PartitionKey.bucket(manifest, buckets()));
        }
      }
    }
    return witness;
  }

  /** Whether the change is validated snapshots on one branch, none staged, no ref reset. */
  static boolean validatedOnOneBranch(TableChange change) {
    return change.validated
        && change.advanced.size() == 1
        && change.reset.isEmpty()
        && change.retained.isEmpty()
        && !change.stages;
  }

  /** The partition stamp a check reads. */
  static ConcernStamps.Partition kind(org.apache.iceberg.CommitFootprint.Check.Kind check) {
    switch (check) {
      case NO_DATA_ADDED:
        return ConcernStamps.Partition.DATA_ADDED;
      case NO_DELETES_ADDED:
        return ConcernStamps.Partition.DELETES_ADDED;
      default:
        return ConcernStamps.Partition.DATA_REMOVED;
    }
  }
}
