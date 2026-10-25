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
import java.util.List;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;

/**
 * The partition bucket of a manifest's files, from its partition summaries: one bucket if every
 * file is in one partition of its spec (each field's bounds equal, and no nulls), hashed with the
 * spec; {@link #ALL} if the files may span partitions, or for delete files of an unpartitioned
 * spec, which apply to every partition.
 */
final class PartitionKey {

  /** Every bucket. */
  static final int ALL = -1;

  private PartitionKey() {}

  /** The bucket, of {@code buckets}, of {@code manifest}'s files; {@link #ALL} for any. */
  static int bucket(ManifestFile manifest, int buckets) {
    List<ManifestFile.PartitionFieldSummary> fields = manifest.partitions();
    if (fields == null || buckets <= 0) {
      return ALL;
    }
    if (fields.isEmpty()) {
      return manifest.content() == ManifestContent.DELETES
          ? ALL
          : Math.floorMod(Integer.hashCode(manifest.partitionSpecId()), buckets);
    }
    int hash = Integer.hashCode(manifest.partitionSpecId());
    for (ManifestFile.PartitionFieldSummary field : fields) {
      ByteBuffer lower = field.lowerBound();
      ByteBuffer upper = field.upperBound();
      if (field.containsNull() || lower == null || !lower.equals(upper)
          || Boolean.TRUE.equals(field.containsNaN())) {
        return ALL;
      }
      hash = 31 * hash + lower.hashCode();
    }
    return Math.floorMod(hash, buckets);
  }
}
