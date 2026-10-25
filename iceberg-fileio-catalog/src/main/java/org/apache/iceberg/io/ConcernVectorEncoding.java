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

import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;
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
final class ConcernVectorEncoding extends ConcernEncoding {

  static final String NAME = "concern-vector";

  static final ConcernVectorEncoding INSTANCE = new ConcernVectorEncoding();

  /** The ref buckets: a ref's stamps are its bucket's. */
  static final int BUCKETS = 16;

  private ConcernVectorEncoding() {
    super(TableChange.Concern.values().length, BUCKETS);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  Witness reads(int base, TableChange change) {
    if (change.concerns.contains(TableChange.Concern.REMOVAL)
        || change.concerns.contains(TableChange.Concern.SCHEMA)
        || change.concerns.contains(TableChange.Concern.SPECS)
        || change.concerns.contains(TableChange.Concern.STATISTICS)
        || !change.reset.isEmpty()) {
      return null;
    }
    BitSet concerns = new BitSet();
    Set<String> refs = new HashSet<>(change.advanced);
    if (change.addsSnapshots) {
      concerns.set(TableChange.Concern.TABLE.ordinal());
      concerns.set(TableChange.Concern.SCHEMA.ordinal());
      concerns.set(TableChange.Concern.SPECS.ordinal());
      concerns.set(TableChange.Concern.REMOVAL.ordinal());
    }
    if (change.stages) {
      refs.add(SnapshotRef.MAIN_BRANCH);
    }
    if (change.concerns.contains(TableChange.Concern.PROPERTIES)) {
      concerns.set(TableChange.Concern.PROPERTIES.ordinal());
    }
    if (change.concerns.contains(TableChange.Concern.TABLE)) {
      concerns.set(TableChange.Concern.TABLE.ordinal());
    }
    if (change.concerns.contains(TableChange.Concern.SORT)) {
      concerns.set(TableChange.Concern.SORT.ordinal());
      concerns.set(TableChange.Concern.SCHEMA.ordinal());
    }
    return new Witness(base, false, concerns, refs);
  }

  @Override
  void stamp(TableChange change, int version, ConcernStamps stamps) {
    for (TableChange.Concern concern : change.concerns) {
      stamps.stampConcern(concern.ordinal(), version);
    }
    for (String ref : change.advanced) {
      stamps.stampAdvance(ref, version);
    }
    for (String ref : change.reset) {
      stamps.stampReset(ref, version);
    }
  }
}
