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
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A conflict encoding that versions a table by its concerns rather than as a whole: it keeps the
 * version at which each concern, each ref bucket and each partition bucket last changed ({@link
 * ConcernStamps}), and admits a record on a changed table if nothing it reads has changed since its
 * writer read the table. Its subclasses say how changes map onto stamps: what a record reads, and
 * what an applied change moves.
 *
 * <p>Every encoding of this kind keeps the Lamport witness, the version read, for what it cannot
 * see into (a pointer table, full metadata, a read pin, a pinned update, a failure to decode) and
 * for what Iceberg commits once ({@link TableChange#commitOnce}).
 */
abstract class ConcernEncoding implements ConflictEncoding<ConcernEncoding.Witness> {

  /** What a record depends on: its base, and the stamps that must be at or below it. */
  static final class Witness {
    final int base;
    /** The Lamport witness: only the base version itself. */
    final boolean whole;
    final BitSet concerns = new BitSet();
    /** The kinds of change to each ref it reads. */
    final Map<String, EnumSet<ConcernStamps.Ref>> refs = new HashMap<>();
    /** The partition buckets it reads, by kind of change; {@link PartitionKey#ALL} for every one. */
    final Map<ConcernStamps.Partition, Set<Integer>> partitions =
        new EnumMap<>(ConcernStamps.Partition.class);
    /** What it reads beyond stamps, judged on the table where it lands; null for nothing. */
    Window window;

    private Witness(int base, boolean whole) {
      this.base = base;
      this.whole = whole;
    }

    static Witness whole(int base) {
      return new Witness(base, true);
    }

    static Witness of(int base) {
      return new Witness(base, false);
    }

    Witness concern(TableChange.Concern concern) {
      concerns.set(concern.ordinal());
      return this;
    }

    Witness concern(int concern) {
      concerns.set(concern);
      return this;
    }

    Witness ref(String ref, ConcernStamps.Ref... kinds) {
      EnumSet<ConcernStamps.Ref> read =
          refs.computeIfAbsent(ref, r -> EnumSet.noneOf(ConcernStamps.Ref.class));
      for (ConcernStamps.Ref kind : kinds) {
        read.add(kind);
      }
      return this;
    }

    Witness partition(ConcernStamps.Partition kind, int bucket) {
      partitions.computeIfAbsent(kind, k -> new HashSet<>()).add(bucket);
      return this;
    }

    Witness window(Window read) {
      this.window = read;
      return this;
    }

    @Override
    public String toString() {
      return whole ? "version " + base
          : "concerns " + concerns + ", refs " + refs + ", partitions " + partitions
              + (window != null ? ", " + window : "") + " since " + base;
    }
  }

  /**
   * What a witness reads of the table where its record lands beyond the stamps (the manifests
   * commits since wrote): a deterministic judgment of that table, which, once false, stays false
   * on any later version where the witness's stamps still hold (S2).
   */
  interface Window {
    boolean holds(ReplayView landing);
  }

  private final ConcernStamps.Codec codec;

  /** An encoding of {@code concerns} concerns and these numbers of ref and partition buckets. */
  ConcernEncoding(int concerns, int refBuckets, int partitionBuckets) {
    this.codec = new ConcernStamps.Codec(concerns, ConcernStamps.Ref.values().length, refBuckets,
        ConcernStamps.Partition.values().length, partitionBuckets);
  }

  /**
   * What {@code change}, a record's on {@code landing}, reads: the stamps that must be at or below
   * {@code base}; null for the Lamport witness.
   */
  abstract Witness reads(int base, TableChange change, ReplayView landing);

  /** Stamps {@code stamps} at {@code version} for {@code change}, applied. */
  abstract void stamp(TableChange change, int version, ConcernStamps stamps);

  @Override
  public Witness witness(TableAction action, ReplayView landing) {
    int base = action.baseVersion();
    if (action.coarse() || action.pinned() || landing.version() == base || !landing.inline()) {
      return Witness.whole(base);
    }
    try {
      TableChange change = TableChange.of(action, landing::table);
      if (change.commitOnce) {
        return Witness.whole(base);
      }
      Witness reads = reads(base, change, landing);
      return reads != null ? reads : Witness.whole(base);
    } catch (RuntimeException e) {
      return Witness.whole(base); // total: what it cannot decode, it does not see into
    }
  }

  @Override
  public boolean admits(Witness witness, ReplayView landing, EncodingState state) {
    if (landing.version() == witness.base) {
      return true;
    }
    if (witness.whole || landing.version() < witness.base) {
      return false;
    }
    ConcernStamps stamps = (ConcernStamps) state;
    for (int c = witness.concerns.nextSetBit(0); c >= 0; c = witness.concerns.nextSetBit(c + 1)) {
      if (stamps.concern(c) > witness.base) {
        return false;
      }
    }
    for (Map.Entry<String, EnumSet<ConcernStamps.Ref>> ref : witness.refs.entrySet()) {
      int bucket = stamps.bucket(ref.getKey());
      for (ConcernStamps.Ref kind : ref.getValue()) {
        if (stamps.ref(kind, bucket) > witness.base) {
          return false;
        }
      }
    }
    for (Map.Entry<ConcernStamps.Partition, Set<Integer>> kind : witness.partitions.entrySet()) {
      for (int bucket : kind.getValue()) {
        if (bucket == PartitionKey.ALL) {
          for (int b = 0; b < stamps.partitionBuckets(); b++) {
            if (stamps.partition(kind.getKey(), b) > witness.base) {
              return false;
            }
          }
        } else if (stamps.partition(kind.getKey(), bucket) > witness.base) {
          return false;
        }
      }
    }
    return witness.window == null || witness.window.holds(landing);
  }

  @Override
  public void observe(
      TableAction applied, ReplayView before, ReplayView after, EncodingState state) {
    ConcernStamps stamps = (ConcernStamps) state;
    int version = after.version();
    try {
      TableChange change;
      if (applied.kind() == TableAction.Kind.DELTA && before.inline() && after.inline()) {
        change = TableChange.of(applied, before::table);
      } else if (applied.kind() == TableAction.Kind.FULL && before.inline() && after.inline()) {
        change = TableChange.diff(before.metadata(), after.metadata());
      } else {
        change = null;
      }
      if (change == null) {
        stamps.stampAll(version);
      } else {
        stamp(change, version, stamps);
      }
    } catch (RuntimeException e) {
      stamps.stampAll(version); // total: what it cannot decode changed everything
    }
  }

  @Override
  public EncodingState.Codec stateCodec() {
    return codec;
  }
}
