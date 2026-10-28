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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.intentlog.wire.CommittedSetCodec;
import org.intentlog.wire.UuidV7;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * {@link CommittedSet} in runs against the set it replaced: one {@code CommittedTransactions}
 * body that every build and GC rewrites ({@link CommittedSetCodec#rewrite}). Random sequences of
 * additions (newer than every id, older, at the newest id's millisecond, copies of ids the set
 * holds), builds, GCs and decodes must give the same bytes and the same answers.
 */
class TestCommittedSetRuns {

  /** The set as it was: a body, and the additions since it was built. */
  private static final class Reference {
    byte[] bytes = new byte[0];
    final Set<UUID> added = new HashSet<>();

    void advanceHorizon(long watermarkMs) {
      if (watermarkMs > 0L) {
        byte[] merged = added.isEmpty() ? bytes : CommittedSetCodec.rewrite(bytes, added, 0L);
        bytes = CommittedSetCodec.rewrite(merged, Collections.emptySet(), watermarkMs);
        added.clear();
      }
    }

    byte[] build() {
      return added.isEmpty() ? bytes : CommittedSetCodec.rewrite(bytes, added, 0L);
    }
  }

  private static final class Walk {
    final Random random;
    final List<UUID> ids = new ArrayList<>();
    long now = 1_700_000_000_000L;
    CommittedSet.Builder subject = CommittedSet.builder();
    final Reference reference = new Reference();

    Walk(long seed) {
      this.random = new Random(seed);
    }

    UUID next() {
      int kind = random.nextInt(10);
      UUID id;
      if (kind < 6) {
        now += random.nextInt(4);
        id = UuidV7.newUuidV7(now, random);
      } else if (kind < 8) {
        // A commit minted before others landed, or a writer whose clock is behind
        id = UuidV7.newUuidV7(now - random.nextInt(300), random);
      } else if (kind < 9 && !ids.isEmpty()) {
        id = ids.get(random.nextInt(ids.size()));
      } else {
        id = UuidV7.newUuidV7(now, random);
      }
      ids.add(id);
      return id;
    }

    void add(int count) {
      for (int i = 0; i < count; i++) {
        UUID id = next();
        subject.add(id);
        reference.added.add(id);
      }
    }

    void gc(long watermarkMs) {
      subject.advanceHorizon(watermarkMs);
      reference.advanceHorizon(watermarkMs);
    }

    /** Builds both, checks them alike, and goes on from the built sets. */
    CommittedSet build() {
      CommittedSet built = subject.build();
      byte[] expected = reference.build();
      assertThat(built.bytes()).isEqualTo(expected);
      assertThat(built.encodedSize()).isEqualTo(expected.length);
      assertThat(built.horizon()).isEqualTo(CommittedSetCodec.horizon(expected));
      for (int i = 0; i < 40 && !ids.isEmpty(); i++) {
        sameAnswers(built, expected, ids.get(random.nextInt(ids.size())));
      }
      sameAnswers(built, expected, UuidV7.newUuidV7(now + 1, random));
      sameAnswers(built, expected, UuidV7.newUuidV7(now - random.nextInt(1000), random));
      subject = built.toBuilder();
      reference.bytes = expected;
      reference.added.clear();
      return built;
    }

    void decode() {
      subject = CommittedSet.of(subject.build().bytes()).toBuilder();
    }
  }

  /** The answers of the set against the scan of the body it replaced. */
  private static void sameAnswers(CommittedSet subject, byte[] reference, UUID id) {
    boolean held = CommittedSetCodec.contains(reference, id);
    long horizon = CommittedSetCodec.horizon(reference);
    boolean above = horizon == 0L || UuidV7.timestampMs(id) > horizon;
    assertThat(subject.mayApply(id)).as("mayApply %s", id).isEqualTo(!held && above);
    assertThat(answer(subject, id)).as("contains %s", id)
        .isEqualTo(held ? "true" : above ? "false" : "beyond the horizon");
  }

  private static String answer(CommittedSet set, UUID id) {
    try {
      return String.valueOf(set.contains(id));
    } catch (DedupHorizonExceededException e) {
      return "beyond the horizon";
    }
  }

  @ParameterizedTest
  @ValueSource(longs = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
  void runsReadAndEncodeAsTheRewrittenSet(long seed) {
    Walk walk = new Walk(seed);
    for (int step = 0; step < 400; step++) {
      int op = walk.random.nextInt(20);
      if (op < 12) {
        walk.add(1 + walk.random.nextInt(walk.random.nextBoolean() ? 2 : 40));
      } else if (op < 16) {
        walk.build();
      } else if (op < 18) {
        // A watermark among the ids, past them all, or before them
        long watermark = walk.ids.isEmpty()
            ? walk.now
            : UuidV7.timestampMs(walk.ids.get(walk.random.nextInt(walk.ids.size())))
                + walk.random.nextInt(3) - 1;
        walk.gc(walk.random.nextInt(10) == 0 ? walk.now + 10 : watermark);
      } else {
        walk.decode();
      }
    }
    walk.build();
  }

  @Test
  void aLargeDecodedSetIsCutAndMergedInPlace() {
    // A checkpoint's set past a run's size: additions newer than it, older than its newest id,
    // and copies of its ids; GCs inside it and past it
    Walk walk = new Walk(42);
    walk.add(300_000);
    CommittedSet built = walk.build();
    assertThat(built.encodedSize()).isGreaterThan(2L * CommittedSet.RUN_BYTES);
    walk.decode();
    for (int round = 0; round < 6; round++) {
      walk.add(1 + walk.random.nextInt(50));
      walk.build();
      long oldest = UuidV7.timestampMs(walk.ids.get(round * 40_000));
      walk.gc(oldest);
      walk.build();
    }
    walk.gc(walk.now + 1);
    assertThat(walk.build().runCount()).isZero();
  }

  @Test
  void additionsOneAtATimeStayAFewRuns() {
    // Each build of one newer id adds a run, and runs merge like a binary counter
    CommittedSet.Builder builder = CommittedSet.builder();
    Random random = new Random(7);
    long now = 1_700_000_000_000L;
    CommittedSet set = null;
    for (int i = 0; i < 20_000; i++) {
      builder.add(UuidV7.newUuidV7(now += 1 + random.nextInt(3), random));
      set = builder.build();
      builder = set.toBuilder();
    }
    assertThat(set.runCount()).isLessThanOrEqualTo(16);
    assertThat(set.ids()).hasSize(20_000);
  }
}
