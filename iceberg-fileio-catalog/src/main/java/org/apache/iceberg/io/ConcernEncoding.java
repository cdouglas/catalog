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
import java.util.Set;

/**
 * A conflict encoding that versions a table by its concerns rather than as a whole: it keeps the
 * version at which each concern last changed ({@link ConcernStamps}), and admits a record on a
 * changed table if nothing it reads has changed since its writer read the table. Its subclasses
 * say how changes map onto stamps: what a record reads, and what an applied change moves.
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
    final BitSet concerns;
    final Set<String> refs;

    Witness(int base, boolean whole, BitSet concerns, Set<String> refs) {
      this.base = base;
      this.whole = whole;
      this.concerns = concerns;
      this.refs = refs;
    }

    static Witness whole(int base) {
      return new Witness(base, true, new BitSet(), Set.of());
    }

    @Override
    public String toString() {
      return whole ? "version " + base : "concerns " + concerns + " and refs " + refs + " since "
          + base;
    }
  }

  private final int concerns;
  private final int buckets;
  private final ConcernStamps.Codec codec;

  ConcernEncoding(int concerns, int buckets) {
    this.concerns = concerns;
    this.buckets = buckets;
    this.codec = new ConcernStamps.Codec(concerns, buckets);
  }

  /**
   * The concerns of {@code change}, a record's, that a change since its base conflicts with, and
   * its refs, each a branch whose head must not have moved; null for the Lamport witness.
   */
  abstract Witness reads(int base, TableChange change);

  /** Stamps {@code stamps} at {@code version} for {@code change}, applied. */
  abstract void stamp(TableChange change, int version, ConcernStamps stamps);

  @Override
  public Witness witness(TableAction action, ReplayView landing) {
    int base = action.baseVersion();
    if (action.coarse() || action.pinned() || landing.version() == base || !landing.inline()) {
      return Witness.whole(base);
    }
    try {
      TableChange change = TableChange.of(action, landing::metadata);
      if (change.commitOnce) {
        return Witness.whole(base);
      }
      Witness reads = reads(base, change);
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
    for (String ref : witness.refs) {
      int bucket = stamps.bucket(ref);
      if (stamps.advance(bucket) > witness.base || stamps.reset(bucket) > witness.base) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void observe(
      TableAction applied, ReplayView before, ReplayView after, EncodingState state) {
    ConcernStamps stamps = (ConcernStamps) state;
    int version = after.version();
    try {
      TableChange change;
      if (applied.kind() == TableAction.Kind.DELTA && before.inline() && after.inline()) {
        change = TableChange.of(applied, before::metadata);
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

  int concerns() {
    return concerns;
  }

  int buckets() {
    return buckets;
  }
}
