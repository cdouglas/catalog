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

import org.apache.iceberg.SnapshotRef;

/**
 * A table versioned by a vector clock over its concerns: properties, schema, partition specs, sort
 * orders, the table itself (location, format, uuid), statistics, snapshot removal, and its refs,
 * whose stamps are hashed into buckets, each with an advance stamp and a reset stamp.
 *
 * <p>A record applies on a table that changed since its writer read it if none of what it reads
 * changed:
 *
 * <ul>
 *   <li>a data commit (one that adds snapshots) reads the table, the schema, the specs and snapshot
 *       removal, and each branch it advances, which must not have moved (or, for a snapshot it
 *       stages, main); it does not read properties, statistics or sort orders, nor other refs;
 *   <li>a property change reads properties, a location change the table, a sort-order change the
 *       sort orders and the schema.
 * </ul>
 *
 * So it admits a commit racing property, statistics and sort-order changes and commits and tags on
 * other refs (WAP). What Iceberg commits once (a schema or spec update, a ref reset or retention
 * change) and what it removes have the Lamport witness.
 */
class ConcernVectorEncoding extends ConcernEncoding {

  static final String NAME = "concern-vector";

  static final ConcernVectorEncoding INSTANCE = new ConcernVectorEncoding();

  /** The ref buckets: a ref's stamps are its bucket's. */
  static final int BUCKETS = 16;

  private ConcernVectorEncoding() {
    this(0);
  }

  /** An encoding that also keeps {@code partitionBuckets} partition buckets. */
  ConcernVectorEncoding(int partitionBuckets) {
    super(TableChange.Concern.values().length, BUCKETS, partitionBuckets);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  Witness reads(int base, TableChange change, ReplayView landing) {
    if (change.concerns.contains(TableChange.Concern.REMOVAL)
        || change.concerns.contains(TableChange.Concern.SCHEMA)
        || change.concerns.contains(TableChange.Concern.SPECS)
        || change.concerns.contains(TableChange.Concern.STATISTICS)
        || !change.reset.isEmpty() || !change.retained.isEmpty()) {
      return null;
    }
    Witness witness = Witness.of(base);
    for (String ref : change.advanced) {
      witness.ref(ref, ConcernStamps.Ref.ADVANCE, ConcernStamps.Ref.RESET, ConcernStamps.Ref.DROP);
    }
    if (change.addsSnapshots) {
      witness
          .concern(TableChange.Concern.TABLE)
          .concern(TableChange.Concern.SCHEMA)
          .concern(TableChange.Concern.SPECS)
          .concern(TableChange.Concern.REMOVAL);
    }
    if (change.stages) {
      witness.ref(SnapshotRef.MAIN_BRANCH, ConcernStamps.Ref.ADVANCE, ConcernStamps.Ref.RESET,
          ConcernStamps.Ref.DROP);
    }
    if (change.concerns.contains(TableChange.Concern.PROPERTIES)) {
      witness.concern(TableChange.Concern.PROPERTIES);
    }
    if (change.concerns.contains(TableChange.Concern.TABLE)) {
      witness.concern(TableChange.Concern.TABLE);
    }
    if (change.concerns.contains(TableChange.Concern.SORT)) {
      witness.concern(TableChange.Concern.SORT).concern(TableChange.Concern.SCHEMA);
    }
    return witness;
  }

  @Override
  void stamp(TableChange change, int version, ConcernStamps stamps) {
    for (TableChange.Concern concern : change.concerns) {
      stamps.stampConcern(concern.ordinal(), version);
    }
    for (String ref : change.advanced) {
      stamps.stampRef(ConcernStamps.Ref.ADVANCE, ref, version);
    }
    for (String ref : change.drops) {
      stamps.stampRef(ConcernStamps.Ref.DROP, ref, version);
    }
    for (String ref : change.reset) {
      stamps.stampRef(ConcernStamps.Ref.RESET, ref, version);
    }
    for (String ref : change.retained) {
      stamps.stampRef(ConcernStamps.Ref.RETENTION, ref, version);
    }
  }
}
