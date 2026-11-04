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
package org.intentlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.intentlog.wire.CommittedSetCodec;
import org.intentlog.wire.UuidV7;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * {@link CommittedSet} in blocks against a reference that encodes the whole set from scratch
 * ({@link CommittedSetCodec#encode(java.util.Collection, long, long, int)}): walks of additions,
 * builds, GCs and decodes, at several block sizes N, must give the same bytes and the same answers
 * after every step, whatever the history (a build after every id or after many, decoded or
 * folded). The reference's blocks are checked against the rule (docs/COMMITTED_PAGING.md "Blocks:
 * the index"): the maximal nodes holding at most N ids, among the nodes holding any.
 *
 * <p>Each id's stamp is drawn against the marks of the set it meets (COMMITTED_PAGING.md
 * "Testing"): above the newest id, in the same ms as another, inside the span of the ids held (a
 * block's, often the newest's), at the horizon or below it, a copy of an id held, or far ahead of
 * every clock. Replay adds an id only if the set admits it (mayApply), as here; every drawn id is
 * also asked about.
 */
class TestCommittedSetBlocks {

  private static final long FAR_AHEAD_MS = 365L * 24 * 3600 * 1000;

  /** The set as the format defines it: the ids, the horizon, encoded whole. */
  private static final class Reference {
    final int blockIds;
    final Set<UUID> ids = new HashSet<>();
    long horizon = 0L;

    Reference(int blockIds) {
      this.blockIds = blockIds;
    }

    boolean mayApply(UUID id) {
      return !ids.contains(id) && (horizon == 0L || UuidV7.timestampMs(id) > horizon);
    }

    void gc(long watermarkMs) {
      if (watermarkMs <= 0L) {
        return;
      }
      ids.removeIf(id -> {
        long ts = UuidV7.timestampMs(id);
        if (ts <= watermarkMs) {
          horizon = Math.max(horizon, ts);
          return true;
        }
        return false;
      });
    }

    byte[] bytes() {
      return CommittedSetCodec.encode(ids, horizon, horizon, blockIds);
    }
  }

  private static final class Walk {
    final Random random;
    final int blockIds;
    final Reference reference;
    final List<UUID> held = new ArrayList<>();
    long now = 1_700_000_000_000L;
    CommittedSet.Builder subject;
    int steps = 0;

    Walk(long seed, int blockIds) {
      this.random = new Random(seed);
      this.blockIds = blockIds;
      this.reference = new Reference(blockIds);
      this.subject = CommittedSet.builder(blockIds);
    }

    /** A stamp drawn against the marks of the set as it is. */
    UUID draw() {
      int region = random.nextInt(20);
      if (region < 9 || held.isEmpty()) {
        // Above the newest id: the usual case, in bursts and lulls
        now += random.nextInt(10) < 8 ? random.nextInt(3) : random.nextInt(5_000);
        return UuidV7.newUuidV7(now, random);
      }
      UUID some = held.get(random.nextInt(held.size()));
      long someTs = UuidV7.timestampMs(some);
      switch (region) {
        case 9:
        case 10:
          // In the same ms as an id held
          return UuidV7.newUuidV7(someTs, random);
        case 11:
        case 12:
        case 13: {
          // Inside the span of the ids held: a late commit, landing in an older block
          long oldest = Math.max(reference.horizon + 1, now - 20_000);
          return UuidV7.newUuidV7(oldest + (long) (random.nextDouble() * (now - oldest)), random);
        }
        case 14:
          // At the horizon
          return UuidV7.newUuidV7(Math.max(1, reference.horizon), random);
        case 15:
          // Below the horizon
          return UuidV7.newUuidV7(Math.max(1, reference.horizon - 1 - random.nextInt(1_000)),
              random);
        case 16:
        case 17:
          // A copy of an id held
          return some;
        case 18:
          // Far ahead of every clock (errata D21): admitted; it stays until the clocks reach it
          return UuidV7.newUuidV7(now + FAR_AHEAD_MS + random.nextInt(1_000), random);
        default:
          // Just above the newest id held
          return UuidV7.newUuidV7(Math.max(now, someTs) + 1, random);
      }
    }

    void step() {
      steps++;
      int op = random.nextInt(100);
      if (op < 70) {
        // Mostly one id; now and then a batch, of up to a few blocks' worth at small N
        int count = 1 + (random.nextInt(4) == 0 ? random.nextInt(Math.min(4 * blockIds + 2, 96)) : 0);
        for (int i = 0; i < count; i++) {
          UUID id = draw();
          ask(id);
          if (reference.mayApply(id)) {
            assertThat(subject.mayApply(id)).as("admits %s", id).isTrue();
            subject.add(id);
            reference.ids.add(id);
            held.add(id);
          } else {
            assertThat(subject.mayApply(id)).as("refuses %s", id).isFalse();
          }
        }
      } else if (op < 85) {
        check(subject.build());
      } else if (op < 95) {
        long watermark = watermark();
        subject.advanceHorizon(watermark);
        reference.gc(watermark);
        held.removeIf(id -> !reference.ids.contains(id));
        check(subject.build());
      } else {
        // A fresh read: decode what the set encodes, and carry on from there
        byte[] bytes = subject.build().bytes();
        subject = CommittedSet.of(bytes, blockIds).toBuilder();
        check(subject.build());
      }
    }

    long watermark() {
      if (held.isEmpty()) {
        return now - random.nextInt(1_000);
      }
      UUID some = held.get(random.nextInt(held.size()));
      switch (random.nextInt(4)) {
        case 0:
          return Math.max(1, reference.horizon - random.nextInt(10)); // drops nothing new
        case 1:
          return UuidV7.timestampMs(some); // at an id
        case 2:
          return UuidV7.timestampMs(some) - 1; // just below one
        default:
          return now - random.nextInt(30_000); // somewhere in the recent past
      }
    }

    /**
     * The builder answers as the reference does, for an id drawn at any mark, without building
     * (so that builds still fall after many additions).
     */
    void ask(UUID id) {
      boolean held = reference.ids.contains(id);
      if (held || known(id)) {
        assertThat(subject.contains(id)).as("contains %s", id).isEqualTo(held);
      } else {
        assertThatThrownBy(() -> subject.contains(id))
            .isInstanceOf(DedupHorizonExceededException.class);
      }
      assertThat(subject.mayApply(id)).isEqualTo(reference.mayApply(id));
    }

    boolean known(UUID id) {
      return reference.horizon == 0L || UuidV7.timestampMs(id) > reference.horizon;
    }

    void check(CommittedSet built) {
      byte[] expected = reference.bytes();
      assertThat(built.bytes()).as("bytes after %d steps", steps).isEqualTo(expected);
      assertThat(built.encodedSize()).isEqualTo(expected.length);
      assertThat(built.horizon()).isEqualTo(reference.horizon);
      assertThat(built.floor()).isEqualTo(reference.horizon);
      assertThat(built.ids()).containsExactlyInAnyOrderElementsOf(reference.ids);
      followsTheRule(expected, blockIds);
      // The built set answers as the reference does: ids held, and ids drawn at every mark
      for (int i = 0; i < 16; i++) {
        UUID id = i % 2 == 0 && !held.isEmpty() ? held.get(random.nextInt(held.size())) : draw();
        boolean holds = reference.ids.contains(id);
        if (holds || known(id)) {
          assertThat(built.contains(id)).as("contains %s", id).isEqualTo(holds);
        } else {
          assertThatThrownBy(() -> built.contains(id))
              .isInstanceOf(DedupHorizonExceededException.class);
        }
        assertThat(built.mayApply(id)).isEqualTo(reference.mayApply(id));
      }
      // The encoding decodes to a set that encodes the same
      assertThat(CommittedSet.of(expected, blockIds).toBuilder().build().bytes())
          .isEqualTo(expected);
    }
  }

  /**
   * The blocks of a body are the rule's: each holds at least one id and at most N (or is a
   * millisecond), and its parent node holds more than N of the set's ids (or it is the root).
   */
  static void followsTheRule(byte[] body, int blockIds) {
    CommittedSetCodec.Decoded decoded = CommittedSetCodec.decodeBody(body);
    long[] ts = new long[decoded.ids.size()];
    for (int i = 0; i < ts.length; i++) {
      ts[i] = UuidV7.timestampMs(decoded.ids.get(i));
    }
    Arrays.sort(ts);
    for (long[] block : decoded.blocks) {
      int level = (int) block[0];
      long index = block[1];
      assertThat(block[2]).isPositive();
      if (level > 0) {
        assertThat(block[2]).as("block (%d, %d) holds at most N", level, index)
            .isLessThanOrEqualTo(blockIds);
      }
      if (level < CommittedSetCodec.MAX_LEVEL) {
        int parentLevel = level + 1;
        long parentIndex = index >> 1;
        long start = parentIndex << parentLevel;
        long top = CommittedSetCodec.top(parentLevel, parentIndex);
        long inParent = upperBound(ts, top) - upperBound(ts, start - 1);
        assertThat(inParent).as("block (%d, %d) is maximal", level, index)
            .isGreaterThan(blockIds);
      }
    }
  }

  /** How many of sorted {@code ts} are at or below {@code value}. */
  private static int upperBound(long[] ts, long value) {
    int lo = 0;
    int hi = ts.length;
    while (lo < hi) {
      int mid = (lo + hi) >>> 1;
      if (ts[mid] <= value) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  static Stream<Arguments> walks() {
    List<Arguments> walks = new ArrayList<>();
    for (int blockIds : new int[] {1, 2, 3, 8, 64, 1 << 16}) {
      for (long seed = 1; seed <= 12; seed++) {
        walks.add(Arguments.of(blockIds, seed * 7919 + blockIds));
      }
    }
    return walks.stream();
  }

  @ParameterizedTest(name = "N={0} seed={1}")
  @MethodSource("walks")
  void blocksReadAndEncodeAsTheWholeSet(int blockIds, long seed) {
    Walk walk = new Walk(seed, blockIds);
    for (int i = 0; i < 400; i++) {
      walk.step();
    }
    walk.check(walk.subject.build());
  }

  @Test
  void anIdOutsideEveryBlockOpensTheLargestFreeNode() {
    // N = 2: three ids far apart split the root; one between them, and then one beside each,
    // open the largest nodes that hold them and overlap no block, or split the block they join
    CommittedSet.Builder set = CommittedSet.builder(2);
    long base = 1L << 40;
    Random random = new Random(3);
    List<UUID> ids = new ArrayList<>();
    for (long ts : new long[] {base, base + (1L << 20), base + (1L << 30), base + (1L << 25),
        base + 1, base + (1L << 25) + 1, base + (1L << 30) + 7}) {
      UUID id = UuidV7.newUuidV7(ts, random);
      ids.add(id);
      set.add(id);
      CommittedSet built = set.build();
      assertThat(built.bytes()).isEqualTo(CommittedSetCodec.encode(ids, 0L, 0L, 2));
      followsTheRule(built.bytes(), 2);
    }
  }

  @Test
  void aMillisecondIsOneBlockWhateverItHolds() {
    CommittedSet.Builder set = CommittedSet.builder(2);
    Random random = new Random(5);
    List<UUID> ids = new ArrayList<>();
    for (int i = 0; i < 9; i++) {
      UUID id = UuidV7.newUuidV7(1_700_000_000_000L, random);
      ids.add(id);
      set.add(id);
    }
    CommittedSet built = set.build();
    assertThat(built.blockCount()).isOne();
    assertThat(CommittedSetCodec.decodeBody(built.bytes()).blocks.get(0)[0]).isZero();
    for (UUID id : ids) {
      assertThat(built.contains(id)).isTrue();
    }
    assertThat(built.bytes()).isEqualTo(CommittedSetCodec.encode(ids, 0L, 0L, 2));
  }

  @Test
  void aGcMergesTheOldestBlocks() {
    // N = 2: x1 and x2 one ms apart, y 1,024 ms later, in the node (11, base / 2^11) that holds
    // all three. That node holds more than N, so its halves are the blocks: {x1, x2} and {y}.
    // A GC that drops x1 leaves two ids, which every node holding both holds: the root.
    long base = 1L << 40;
    Random random = new Random(11);
    Reference reference = new Reference(2);
    CommittedSet.Builder set = CommittedSet.builder(2);
    for (long ts : new long[] {base, base + 1, base + 1024}) {
      UUID id = UuidV7.newUuidV7(ts, random);
      set.add(id);
      reference.ids.add(id);
    }
    CommittedSet before = set.build();
    assertThat(before.bytes()).isEqualTo(reference.bytes());
    assertThat(before.blockCount()).isEqualTo(2);
    List<long[]> blocks = CommittedSetCodec.decodeBody(before.bytes()).blocks;
    assertThat(blocks.get(0)[0]).isEqualTo(10L);
    assertThat(blocks.get(1)[0]).isEqualTo(10L);

    set.advanceHorizon(base);
    reference.gc(base);
    CommittedSet after = set.build();
    assertThat(after.bytes()).isEqualTo(reference.bytes());
    assertThat(after.blockCount()).isOne();
    assertThat(CommittedSetCodec.decodeBody(after.bytes()).blocks.get(0)[0])
        .isEqualTo(CommittedSetCodec.MAX_LEVEL);
    assertThat(after.horizon()).isEqualTo(base);
    followsTheRule(after.bytes(), 2);
  }
}
