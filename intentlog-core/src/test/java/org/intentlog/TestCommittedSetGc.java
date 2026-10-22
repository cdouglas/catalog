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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.intentlog.wire.CommittedSetCodec;
import org.intentlog.wire.UuidV7;
import org.junit.jupiter.api.Test;

/**
 * Tests for committed-set GC: {@link CommittedSetCodec#rewrite}, the
 * Builder.advanceHorizon path (a GC record's replay), and the tri-state
 * {@link CommittedSet#contains} semantic with
 * {@link DedupHorizonExceededException}.
 *
 * <p>End-to-end (catalog write → CAS rewrite with retention=0 → fresh read →
 * horizon-exceeded) lives in the catalog's TestProtoCommitKnobs; these tests
 * exercise the codec invariants in isolation.
 */
class TestCommittedSetGc {


  // ============================================================
  // rewrite — round-trip / pass-through
  // ============================================================

  @Test
  void rewriteEmptyInputsProducesEmpty() {
    byte[] result = CommittedSetCodec.rewrite(
        new byte[0], Collections.emptyList(), 0L);
    assertThat(result).isEmpty();
  }

  @Test
  void rewritePassThroughWhenNoAdditionsAndNoDrops() throws Exception {
    Random rand = new Random(0xC0DEC0DE);
    long base = System.currentTimeMillis();
    List<UUID> input = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      input.add(UuidV7.newUuidV7(base + i * 7L, rand));
    }
    byte[] body = CommittedSetCodec.encode(input);
    byte[] rewritten = CommittedSetCodec.rewrite(body, Collections.emptyList(), 0L);
    // dropBefore=0 means no inherited entry can be dropped (v7 ts > 0); no
    // additions either. The merge should reproduce identical entries; the wire
    // bytes match exactly because there's no horizon to add.
    assertThat(rewritten).isEqualTo(body);
  }

  @Test
  void rewriteWithAdditionsOnlyMergesDescending() throws Exception {
    Random rand = new Random(0xADD12345L);
    long base = System.currentTimeMillis();
    LinkedHashSet<UUID> inherited = new LinkedHashSet<>();
    for (int i = 0; i < 5; i++) {
      inherited.add(UuidV7.newUuidV7(base + i * 100L, rand));
    }
    LinkedHashSet<UUID> additions = new LinkedHashSet<>();
    for (int i = 0; i < 4; i++) {
      additions.add(UuidV7.newUuidV7(base + 1000L + i * 100L, rand));
    }
    byte[] inheritedBody = CommittedSetCodec.encode(inherited);
    byte[] merged = CommittedSetCodec.rewrite(inheritedBody, additions, 0L);

    Set<UUID> expected = new LinkedHashSet<>(inherited);
    expected.addAll(additions);
    Set<UUID> actual = new LinkedHashSet<>(CommittedSetCodec.decode(merged));
    assertThat(actual).isEqualTo(expected);
    // No horizon recorded — no drops happened.
    assertThat(CommittedSetCodec.horizon(merged)).isZero();
  }

  // ============================================================
  // rewrite — watermark drops
  // ============================================================

  @Test
  void rewriteDropsEntriesAtOrBelowWatermark() throws Exception {
    Random rand = new Random(0xD20D20DL);
    // Construct an inherited set spanning an obvious watermark boundary.
    long base = 100_000L;
    LinkedHashSet<UUID> inherited = new LinkedHashSet<>();
    inherited.add(UuidV7.newUuidV7(base + 50, rand));   // dropped (<= 100)
    inherited.add(UuidV7.newUuidV7(base + 100, rand));  // dropped (== watermark, exclusive on retention)
    inherited.add(UuidV7.newUuidV7(base + 150, rand));  // kept
    inherited.add(UuidV7.newUuidV7(base + 200, rand));  // kept
    byte[] body = CommittedSetCodec.encode(inherited);
    long watermark = base + 100;
    byte[] rewritten = CommittedSetCodec.rewrite(body, Collections.emptyList(), watermark);

    // Only entries with ts > watermark survive.
    Set<UUID> survivors = new LinkedHashSet<>(CommittedSetCodec.decode(rewritten));
    assertThat(survivors).hasSize(2);
    for (UUID u : survivors) {
      assertThat(UuidV7.timestampMs(u)).isGreaterThan(watermark);
    }
    // Horizon is the max ts of dropped entries.
    assertThat(CommittedSetCodec.horizon(rewritten)).isEqualTo(watermark);
  }

  @Test
  void rewriteAdditionsKeptUnconditionallyEvenBelowWatermark() throws Exception {
    // Constraint: log entries below the watermark must be preserved (they're
    // the just-applied transactions, not stale dedup entries).
    Random rand = new Random(0xCAFE);
    long base = 100_000L;
    LinkedHashSet<UUID> inherited = new LinkedHashSet<>();
    inherited.add(UuidV7.newUuidV7(base - 1000, rand)); // dropped
    byte[] body = CommittedSetCodec.encode(inherited);

    // Addition with timestamp BELOW watermark — survives.
    UUID staleAddition = UuidV7.newUuidV7(base - 500, rand);
    long watermark = base;
    byte[] rewritten = CommittedSetCodec.rewrite(
        body, Collections.singletonList(staleAddition), watermark);

    Set<UUID> survivors = new LinkedHashSet<>(CommittedSetCodec.decode(rewritten));
    assertThat(survivors).containsExactly(staleAddition);
    // Horizon equals dropped inherited's ts, NOT addition's ts.
    assertThat(CommittedSetCodec.horizon(rewritten)).isEqualTo(base - 1000);
  }

  @Test
  void rewriteHorizonCanExceedMaxTimestampMs() throws Exception {
    // Edge case from docs/GC.md: when all kept entries are stale additions
    // and inherited drops landed *above* them, highest_dropped > max_timestamp_ms.
    Random rand = new Random(0xEDDE);
    long base = 1_000_000L;
    LinkedHashSet<UUID> inherited = new LinkedHashSet<>();
    inherited.add(UuidV7.newUuidV7(base - 100, rand)); // dropped, ts = 999_900
    byte[] body = CommittedSetCodec.encode(inherited);

    UUID staleAddition = UuidV7.newUuidV7(base - 200, rand); // ts = 999_800
    long watermark = base; // 1_000_000
    byte[] rewritten = CommittedSetCodec.rewrite(
        body, Collections.singletonList(staleAddition), watermark);

    Set<UUID> survivors = new LinkedHashSet<>(CommittedSetCodec.decode(rewritten));
    assertThat(survivors).containsExactly(staleAddition);
    // max_timestamp_ms is the addition's ts (the only kept entry); horizon is
    // the dropped inherited's ts, which is GREATER than max_timestamp_ms.
    long horizon = CommittedSetCodec.horizon(rewritten);
    long maxTs = UuidV7.timestampMs(staleAddition);
    assertThat(horizon).as("horizon > max_timestamp_ms is the rare-but-legal case")
        .isGreaterThan(maxTs);
    assertThat(horizon).isEqualTo(base - 100);
  }

  @Test
  void rewriteHorizonIsMonotonicAcrossPasses() throws Exception {
    Random rand = new Random(0xCDCDL);
    long base = System.currentTimeMillis();
    LinkedHashSet<UUID> entries = new LinkedHashSet<>();
    for (int i = 0; i < 6; i++) {
      entries.add(UuidV7.newUuidV7(base + i * 10L, rand));
    }
    byte[] pass1 = CommittedSetCodec.encode(entries);

    // First pass: drop everything <= base + 20.
    long w1 = base + 20;
    byte[] pass2 = CommittedSetCodec.rewrite(pass1, Collections.emptyList(), w1);
    long h1 = CommittedSetCodec.horizon(pass2);
    assertThat(h1).isEqualTo(w1);

    // Second pass with a SMALLER watermark — should not move horizon backward.
    long w2 = base; // smaller than h1
    byte[] pass3 = CommittedSetCodec.rewrite(pass2, Collections.emptyList(), w2);
    long h2 = CommittedSetCodec.horizon(pass3);
    assertThat(h2).as("horizon must not regress when a later pass uses a smaller watermark")
        .isEqualTo(h1);

    // Third pass with a larger watermark — horizon advances.
    long w3 = base + 40;
    byte[] pass4 = CommittedSetCodec.rewrite(pass3, Collections.emptyList(), w3);
    long h3 = CommittedSetCodec.horizon(pass4);
    assertThat(h3).isEqualTo(w3);
  }

  @Test
  void rewriteEmptyResultButPreservedHorizon() throws Exception {
    // All inherited entries fall at or below watermark; result has no entries
    // but the horizon must be persisted so future readers know.
    Random rand = new Random(0xEEEE);
    LinkedHashSet<UUID> inherited = new LinkedHashSet<>();
    inherited.add(UuidV7.newUuidV7(50, rand));
    inherited.add(UuidV7.newUuidV7(100, rand));
    byte[] body = CommittedSetCodec.encode(inherited);
    byte[] rewritten = CommittedSetCodec.rewrite(body, Collections.emptyList(), 200L);

    assertThat(rewritten).isNotEmpty();
    assertThat(CommittedSetCodec.decode(rewritten)).isEmpty();
    assertThat(CommittedSetCodec.horizon(rewritten)).isEqualTo(100L);
  }

  @Test
  void rewriteDedupesAdditionThatCollidesWithInheritedEntry() throws Exception {
    // If an addition's UUID is byte-identical to an inherited entry (e.g.
    // a retried txn replayed twice), the merge collapses to a single entry.
    Random rand = new Random(0xDEAD);
    long base = System.currentTimeMillis();
    UUID shared = UuidV7.newUuidV7(base + 50, rand);
    LinkedHashSet<UUID> inherited = new LinkedHashSet<>();
    inherited.add(shared);
    inherited.add(UuidV7.newUuidV7(base + 100, rand));
    byte[] body = CommittedSetCodec.encode(inherited);

    byte[] rewritten = CommittedSetCodec.rewrite(
        body, Collections.singletonList(shared), 0L);
    Set<UUID> survivors = new LinkedHashSet<>(CommittedSetCodec.decode(rewritten));
    assertThat(survivors).hasSize(2).contains(shared);
  }

  // ============================================================
  // Tri-state contains
  // ============================================================

  @Test
  void containsTransactionFoundReturnsTrue() throws Exception {
    Random rand = new Random(0x111);
    long base = System.currentTimeMillis();
    UUID u = UuidV7.newUuidV7(base, rand);
    byte[] body = CommittedSetCodec.encode(List.of(u));
    CommittedSet cat = CommittedSet.of(body);
    assertThat(cat.contains(u)).isTrue();
  }

  @Test
  void containsTransactionAboveHorizonReturnsFalse() throws Exception {
    // Set horizon to base; query for a fresh UUID with ts > horizon → false (presumed abort).
    Random rand = new Random(0x222);
    long base = 1_000_000L;
    LinkedHashSet<UUID> inherited = new LinkedHashSet<>();
    inherited.add(UuidV7.newUuidV7(base - 100, rand));
    byte[] body = CommittedSetCodec.encode(inherited);
    byte[] rewritten = CommittedSetCodec.rewrite(body, Collections.emptyList(), base);

    CommittedSet cat = CommittedSet.of(rewritten);

    UUID neverCommitted = UuidV7.newUuidV7(base + 50, rand); // above horizon
    assertThat(cat.contains(neverCommitted))
        .as("a UUID newer than the horizon that's not in the set is presumed abort")
        .isFalse();
  }

  @Test
  void containsTransactionBelowHorizonThrows() throws Exception {
    Random rand = new Random(0x333);
    long base = 1_000_000L;
    LinkedHashSet<UUID> inherited = new LinkedHashSet<>();
    inherited.add(UuidV7.newUuidV7(base - 100, rand));
    byte[] body = CommittedSetCodec.encode(inherited);
    byte[] rewritten = CommittedSetCodec.rewrite(body, Collections.emptyList(), base);

    CommittedSet cat = CommittedSet.of(rewritten);

    UUID stale = UuidV7.newUuidV7(base - 500, rand); // below horizon
    assertThatThrownBy(() -> cat.contains(stale))
        .isInstanceOf(DedupHorizonExceededException.class)
        .satisfies(e -> {
          DedupHorizonExceededException ex = (DedupHorizonExceededException) e;
          assertThat(ex.queryId()).isEqualTo(stale);
          assertThat(ex.horizonMs()).isEqualTo(base - 100);
        });
  }

  @Test
  void containsTransactionNonV7DoesNotThrow() throws Exception {
    // Non-v7 UUIDs aren't in the dedup set by construction; the horizon
    // shouldn't be consulted, so containsTransaction returns false silently.
    Random rand = new Random(0x444);
    long base = 1_000_000L;
    LinkedHashSet<UUID> inherited = new LinkedHashSet<>();
    inherited.add(UuidV7.newUuidV7(base - 100, rand));
    byte[] body = CommittedSetCodec.encode(inherited);
    byte[] rewritten = CommittedSetCodec.rewrite(body, Collections.emptyList(), base);
    CommittedSet cat = CommittedSet.of(rewritten);

    UUID v4 = UUID.randomUUID(); // not v7
    assertThat(cat.contains(v4)).isFalse();
  }

  @Test
  void highestDroppedTimestampMsAccessor() throws Exception {
    Random rand = new Random(0x555);
    long base = 1_000_000L;
    LinkedHashSet<UUID> inherited = new LinkedHashSet<>();
    inherited.add(UuidV7.newUuidV7(base - 100, rand));
    byte[] body = CommittedSetCodec.encode(inherited);
    byte[] rewritten = CommittedSetCodec.rewrite(body, Collections.emptyList(), base);
    CommittedSet cat = CommittedSet.of(rewritten);
    assertThat(cat.horizon()).isEqualTo(base - 100);

    // Empty / no-GC case: 0.
    CommittedSet fresh = CommittedSet.EMPTY;
    assertThat(fresh.horizon()).isZero();
  }

  // ============================================================
  // Builder.advanceHorizon (a GC record)
  // ============================================================

  @Test
  void builderAdvanceHorizonAppliesGc() throws Exception {
    Random rand = new Random(0x666);
    long base = 1_000_000L;
    LinkedHashSet<UUID> inherited = new LinkedHashSet<>();
    inherited.add(UuidV7.newUuidV7(base - 100, rand));
    inherited.add(UuidV7.newUuidV7(base + 100, rand));
    byte[] body = CommittedSetCodec.encode(inherited);

    CommittedSet cat = CommittedSet.of(body).toBuilder().advanceHorizon(base).build();
    assertThat(cat.horizon()).isEqualTo(base - 100);
    assertThat(cat.ids()).hasSize(1);
  }

  /**
   * A GC record drops the ids replay added before it as well as the inherited ones: it sits at a
   * position in the log, and drops whatever the set holds there.
   */
  @Test
  void builderAdvanceHorizonDropsAddedIds() throws Exception {
    Random rand = new Random(0x888);
    long base = 1_000_000L;
    UUID inherited = UuidV7.newUuidV7(base - 300, rand);
    UUID addedOld = UuidV7.newUuidV7(base - 200, rand);
    UUID addedNew = UuidV7.newUuidV7(base + 200, rand);

    CommittedSet.Builder builder =
        CommittedSet.of(CommittedSetCodec.encode(List.of(inherited))).toBuilder();
    builder.add(addedOld).add(addedNew).advanceHorizon(base);
    UUID addedAfter = UuidV7.newUuidV7(base - 100, rand); // below the watermark, added after it
    builder.add(addedAfter);
    CommittedSet cat = builder.build();

    assertThat(cat.horizon()).isEqualTo(base - 200);
    assertThat(cat.ids()).containsExactlyInAnyOrder(addedNew, addedAfter);
    assertThatThrownBy(() -> cat.contains(inherited))
        .isInstanceOf(DedupHorizonExceededException.class);
  }

  /** Replaying the same GC record again, or an older one, changes nothing. */
  @Test
  void builderAdvanceHorizonIsIdempotent() throws Exception {
    Random rand = new Random(0x999);
    long base = 1_000_000L;
    LinkedHashSet<UUID> inherited = new LinkedHashSet<>();
    for (int i = 0; i < 10; i++) {
      inherited.add(UuidV7.newUuidV7(base - 500 + i * 100, rand));
    }
    byte[] once =
        CommittedSet.of(CommittedSetCodec.encode(inherited)).toBuilder()
            .advanceHorizon(base).build().bytes();
    byte[] again =
        CommittedSet.of(once).toBuilder().advanceHorizon(base).advanceHorizon(base - 300).build()
            .bytes();
    assertThat(again).isEqualTo(once);
  }

  @Test
  void builderBuildNoArgDoesNotGc() throws Exception {
    Random rand = new Random(0x777);
    long base = 1_000_000L;
    LinkedHashSet<UUID> inherited = new LinkedHashSet<>();
    inherited.add(UuidV7.newUuidV7(base - 100, rand));
    byte[] body = CommittedSetCodec.encode(inherited);

    CommittedSet cat = CommittedSet.of(body);
    assertThat(cat.horizon()).isZero();
    assertThat(cat.ids()).hasSize(1);
  }
}
