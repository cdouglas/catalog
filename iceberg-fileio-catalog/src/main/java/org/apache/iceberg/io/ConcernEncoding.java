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
import java.util.List;
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
    /** Why the witness is the Lamport witness, for {@link #explain}; null if it is not. */
    final String why;
    final BitSet concerns = new BitSet();
    /** The kinds of change to each ref it reads. */
    final Map<String, EnumSet<ConcernStamps.Ref>> refs = new HashMap<>();
    /** The partition buckets it reads, by kind of change; {@link PartitionKey#ALL} for every one. */
    final Map<ConcernStamps.Partition, Set<Integer>> partitions =
        new EnumMap<>(ConcernStamps.Partition.class);
    /** What it reads beyond stamps, judged on the table where it lands; null for nothing. */
    Window window;

    private Witness(int base, boolean whole, String why) {
      this.base = base;
      this.whole = whole;
      this.why = why;
    }

    /** The Lamport witness, for the reason {@code why} (a constant). */
    static Witness whole(int base, String why) {
      return new Witness(base, true, why);
    }

    static Witness of(int base) {
      return new Witness(base, false, null);
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
    /** Why the judgment of {@code landing} is false; null if it holds. */
    String rejects(ReplayView landing);
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

  /**
   * What a read witness reads, a serializable transaction's that read {@code refs} of the table at
   * {@code base}: the stamps of every change to them that {@link #stamp} records (errata D12).
   */
  abstract Witness readWitness(int base, List<String> refs);

  /**
   * Why {@code change} has the Lamport witness where {@link #reads} gives none (a constant), for
   * {@link #explain}.
   */
  String unseparated(TableChange change) {
    return "unseparated";
  }

  /** The name of concern {@code concern}, for {@link #explain}. */
  String concernName(int concern) {
    return TableChange.Concern.values()[concern].label;
  }

  /** Why a pinned or commit-once delta has the Lamport witness. */
  private static String pinned(TableAction action, ReplayView landing) {
    try {
      TableChange change = TableChange.of(action, landing::table);
      if (change.commitOnce) {
        return commitOnce(change);
      }
    } catch (RuntimeException e) {
      // a pin it cannot see into is the writer's
    }
    return "pinned (a catalog transaction's table)";
  }

  /** What Iceberg commits once in {@code change}. */
  private static String commitOnce(TableChange change) {
    if (change.concerns.contains(TableChange.Concern.SPECS)) {
      return "commit once: a partition-spec change";
    } else if (change.concerns.contains(TableChange.Concern.SCHEMA)) {
      return "commit once: a schema change";
    } else if (!change.reset.isEmpty()) {
      return "commit once: a ref reset";
    }
    return "commit once: a ref's retention";
  }

  @Override
  public Witness witness(TableAction action, ReplayView landing) {
    int base = action.baseVersion();
    if (landing.version() == base) {
      return Witness.whole(base, "the version read");
    } else if (action.readWitness()) {
      // A pointer table's every change stamps everything; this says why
      return landing.inline() ? readWitness(base, action.readRefs())
          : Witness.whole(base, "a pointer table");
    } else if (action.coarse()) {
      return Witness.whole(base, action.kind() == TableAction.Kind.FULL ? "full metadata"
          : action.kind() == TableAction.Kind.READ ? "a read pin" : "a metadata location");
    } else if (action.pinned()) {
      return Witness.whole(base, pinned(action, landing));
    } else if (!landing.inline()) {
      return Witness.whole(base, "a pointer table");
    }
    try {
      TableChange change = TableChange.of(action, landing::table);
      if (change.commitOnce) {
        return Witness.whole(base, commitOnce(change));
      }
      Witness reads = reads(base, change, landing);
      return reads != null ? reads : Witness.whole(base, unseparated(change));
    } catch (RuntimeException e) {
      // total: what it cannot decode, it does not see into
      return Witness.whole(base, "undecodable");
    }
  }

  @Override
  public boolean admits(Witness witness, ReplayView landing, EncodingState state) {
    return rejection(witness, landing, state) == null;
  }

  @Override
  public String explain(Witness witness, ReplayView landing, EncodingState state) {
    return rejection(witness, landing, state);
  }

  /**
   * Why {@code witness} does not hold on {@code landing}: the first of what it reads that changed
   * since its base; null if nothing did. A reason is built only where it fails.
   */
  private String rejection(Witness witness, ReplayView landing, EncodingState state) {
    if (landing.version() == witness.base) {
      return null;
    }
    if (landing.version() < witness.base) {
      return "version: behind its base";
    } else if (witness.whole) {
      return "whole: " + witness.why;
    }
    ConcernStamps stamps = (ConcernStamps) state;
    for (int c = witness.concerns.nextSetBit(0); c >= 0; c = witness.concerns.nextSetBit(c + 1)) {
      if (stamps.concern(c) > witness.base) {
        return "concern: " + concernName(c);
      }
    }
    for (Map.Entry<String, EnumSet<ConcernStamps.Ref>> ref : witness.refs.entrySet()) {
      int bucket = stamps.bucket(ref.getKey());
      for (ConcernStamps.Ref kind : ref.getValue()) {
        if (stamps.ref(kind, bucket) > witness.base) {
          return "ref: " + kind.label;
        }
      }
    }
    for (Map.Entry<ConcernStamps.Partition, Set<Integer>> kind : witness.partitions.entrySet()) {
      for (int bucket : kind.getValue()) {
        if (bucket == PartitionKey.ALL) {
          for (int b = 0; b < stamps.partitionBuckets(); b++) {
            if (stamps.partition(kind.getKey(), b) > witness.base) {
              return "partition: " + kind.getKey().label + " (any)";
            }
          }
        } else if (stamps.partition(kind.getKey(), bucket) > witness.base) {
          return "partition: " + kind.getKey().label;
        }
      }
    }
    if (witness.window != null) {
      String why = witness.window.rejects(landing);
      if (why != null) {
        return "window: " + why;
      }
    }
    return null;
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
