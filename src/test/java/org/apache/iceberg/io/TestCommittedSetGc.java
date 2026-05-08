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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Tests for committed-set GC: {@link ProtoCodec#rewriteCommittedBytes}, the
 * Builder/build(watermark) path, and the tri-state
 * {@link ProtoCatalogFile#containsTransaction} semantic with
 * {@link DedupHorizonExceededException}.
 *
 * <p>End-to-end (catalog write → CAS rewrite with retention=0 → fresh read →
 * horizon-exceeded) lives in {@link TestProtoCommitKnobs}; these tests
 * exercise the codec invariants in isolation.
 */
class TestCommittedSetGc {

  private static final InputFile LOC = new InputFile() {
    @Override public long getLength() { return 0; }
    @Override public SeekableInputStream newStream() { throw new UnsupportedOperationException(); }
    @Override public String location() { return "test://gc"; }
    @Override public boolean exists() { return false; }
  };

  // ============================================================
  // rewriteCommittedBytes — round-trip / pass-through
  // ============================================================

  @Test
  void rewriteEmptyInputsProducesEmpty() {
    byte[] result = ProtoCodec.rewriteCommittedBytes(
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
    byte[] body = ProtoCodec.encodeCommittedTransactions(input);
    byte[] rewritten = ProtoCodec.rewriteCommittedBytes(body, Collections.emptyList(), 0L);
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
    byte[] inheritedBody = ProtoCodec.encodeCommittedTransactions(inherited);
    byte[] merged = ProtoCodec.rewriteCommittedBytes(inheritedBody, additions, 0L);

    Set<UUID> expected = new LinkedHashSet<>(inherited);
    expected.addAll(additions);
    Set<UUID> actual = new LinkedHashSet<>(ProtoCodec.decodeCommittedTransactions(merged));
    assertThat(actual).isEqualTo(expected);
    // No horizon recorded — no drops happened.
    assertThat(ProtoCodec.committedSetHorizon(merged)).isZero();
  }

  // ============================================================
  // rewriteCommittedBytes — watermark drops
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
    byte[] body = ProtoCodec.encodeCommittedTransactions(inherited);
    long watermark = base + 100;
    byte[] rewritten = ProtoCodec.rewriteCommittedBytes(body, Collections.emptyList(), watermark);

    // Only entries with ts > watermark survive.
    Set<UUID> survivors = new LinkedHashSet<>(ProtoCodec.decodeCommittedTransactions(rewritten));
    assertThat(survivors).hasSize(2);
    for (UUID u : survivors) {
      assertThat(UuidV7.timestampMs(u)).isGreaterThan(watermark);
    }
    // Horizon is the max ts of dropped entries.
    assertThat(ProtoCodec.committedSetHorizon(rewritten)).isEqualTo(watermark);
  }

  @Test
  void rewriteAdditionsKeptUnconditionallyEvenBelowWatermark() throws Exception {
    // Constraint: log entries below the watermark must be preserved (they're
    // the just-applied transactions, not stale dedup entries).
    Random rand = new Random(0xCAFE);
    long base = 100_000L;
    LinkedHashSet<UUID> inherited = new LinkedHashSet<>();
    inherited.add(UuidV7.newUuidV7(base - 1000, rand)); // dropped
    byte[] body = ProtoCodec.encodeCommittedTransactions(inherited);

    // Addition with timestamp BELOW watermark — survives.
    UUID staleAddition = UuidV7.newUuidV7(base - 500, rand);
    long watermark = base;
    byte[] rewritten = ProtoCodec.rewriteCommittedBytes(
        body, Collections.singletonList(staleAddition), watermark);

    Set<UUID> survivors = new LinkedHashSet<>(ProtoCodec.decodeCommittedTransactions(rewritten));
    assertThat(survivors).containsExactly(staleAddition);
    // Horizon equals dropped inherited's ts, NOT addition's ts.
    assertThat(ProtoCodec.committedSetHorizon(rewritten)).isEqualTo(base - 1000);
  }

  @Test
  void rewriteHorizonCanExceedMaxTimestampMs() throws Exception {
    // Edge case from docs/GC.md: when all kept entries are stale additions
    // and inherited drops landed *above* them, highest_dropped > max_timestamp_ms.
    Random rand = new Random(0xEDDE);
    long base = 1_000_000L;
    LinkedHashSet<UUID> inherited = new LinkedHashSet<>();
    inherited.add(UuidV7.newUuidV7(base - 100, rand)); // dropped, ts = 999_900
    byte[] body = ProtoCodec.encodeCommittedTransactions(inherited);

    UUID staleAddition = UuidV7.newUuidV7(base - 200, rand); // ts = 999_800
    long watermark = base; // 1_000_000
    byte[] rewritten = ProtoCodec.rewriteCommittedBytes(
        body, Collections.singletonList(staleAddition), watermark);

    Set<UUID> survivors = new LinkedHashSet<>(ProtoCodec.decodeCommittedTransactions(rewritten));
    assertThat(survivors).containsExactly(staleAddition);
    // max_timestamp_ms is the addition's ts (the only kept entry); horizon is
    // the dropped inherited's ts, which is GREATER than max_timestamp_ms.
    long horizon = ProtoCodec.committedSetHorizon(rewritten);
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
    byte[] pass1 = ProtoCodec.encodeCommittedTransactions(entries);

    // First pass: drop everything <= base + 20.
    long w1 = base + 20;
    byte[] pass2 = ProtoCodec.rewriteCommittedBytes(pass1, Collections.emptyList(), w1);
    long h1 = ProtoCodec.committedSetHorizon(pass2);
    assertThat(h1).isEqualTo(w1);

    // Second pass with a SMALLER watermark — should not move horizon backward.
    long w2 = base; // smaller than h1
    byte[] pass3 = ProtoCodec.rewriteCommittedBytes(pass2, Collections.emptyList(), w2);
    long h2 = ProtoCodec.committedSetHorizon(pass3);
    assertThat(h2).as("horizon must not regress when a later pass uses a smaller watermark")
        .isEqualTo(h1);

    // Third pass with a larger watermark — horizon advances.
    long w3 = base + 40;
    byte[] pass4 = ProtoCodec.rewriteCommittedBytes(pass3, Collections.emptyList(), w3);
    long h3 = ProtoCodec.committedSetHorizon(pass4);
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
    byte[] body = ProtoCodec.encodeCommittedTransactions(inherited);
    byte[] rewritten = ProtoCodec.rewriteCommittedBytes(body, Collections.emptyList(), 200L);

    assertThat(rewritten).isNotEmpty();
    assertThat(ProtoCodec.decodeCommittedTransactions(rewritten)).isEmpty();
    assertThat(ProtoCodec.committedSetHorizon(rewritten)).isEqualTo(100L);
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
    byte[] body = ProtoCodec.encodeCommittedTransactions(inherited);

    byte[] rewritten = ProtoCodec.rewriteCommittedBytes(
        body, Collections.singletonList(shared), 0L);
    Set<UUID> survivors = new LinkedHashSet<>(ProtoCodec.decodeCommittedTransactions(rewritten));
    assertThat(survivors).hasSize(2).contains(shared);
  }

  // ============================================================
  // Tri-state containsTransaction
  // ============================================================

  @Test
  void containsTransactionFoundReturnsTrue() throws Exception {
    Random rand = new Random(0x111);
    long base = System.currentTimeMillis();
    UUID u = UuidV7.newUuidV7(base, rand);
    byte[] body = ProtoCodec.encodeCommittedTransactions(java.util.List.of(u));
    ProtoCatalogFile cat = ProtoCatalogFile.builder(LOC)
        .setCommittedTransactionsBytes(body)
        .build();
    assertThat(cat.containsTransaction(u)).isTrue();
  }

  @Test
  void containsTransactionAboveHorizonReturnsFalse() throws Exception {
    // Set horizon to base; query for a fresh UUID with ts > horizon → false (presumed abort).
    Random rand = new Random(0x222);
    long base = 1_000_000L;
    LinkedHashSet<UUID> inherited = new LinkedHashSet<>();
    inherited.add(UuidV7.newUuidV7(base - 100, rand));
    byte[] body = ProtoCodec.encodeCommittedTransactions(inherited);
    byte[] rewritten = ProtoCodec.rewriteCommittedBytes(body, Collections.emptyList(), base);

    ProtoCatalogFile cat = ProtoCatalogFile.builder(LOC)
        .setCommittedTransactionsBytes(rewritten)
        .build();

    UUID neverCommitted = UuidV7.newUuidV7(base + 50, rand); // above horizon
    assertThat(cat.containsTransaction(neverCommitted))
        .as("a UUID newer than the horizon that's not in the set is presumed abort")
        .isFalse();
  }

  @Test
  void containsTransactionBelowHorizonThrows() throws Exception {
    Random rand = new Random(0x333);
    long base = 1_000_000L;
    LinkedHashSet<UUID> inherited = new LinkedHashSet<>();
    inherited.add(UuidV7.newUuidV7(base - 100, rand));
    byte[] body = ProtoCodec.encodeCommittedTransactions(inherited);
    byte[] rewritten = ProtoCodec.rewriteCommittedBytes(body, Collections.emptyList(), base);

    ProtoCatalogFile cat = ProtoCatalogFile.builder(LOC)
        .setCommittedTransactionsBytes(rewritten)
        .build();

    UUID stale = UuidV7.newUuidV7(base - 500, rand); // below horizon
    assertThatThrownBy(() -> cat.containsTransaction(stale))
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
    byte[] body = ProtoCodec.encodeCommittedTransactions(inherited);
    byte[] rewritten = ProtoCodec.rewriteCommittedBytes(body, Collections.emptyList(), base);
    ProtoCatalogFile cat = ProtoCatalogFile.builder(LOC)
        .setCommittedTransactionsBytes(rewritten)
        .build();

    UUID v4 = UUID.randomUUID(); // not v7
    assertThat(cat.containsTransaction(v4)).isFalse();
  }

  @Test
  void highestDroppedTimestampMsAccessor() throws Exception {
    Random rand = new Random(0x555);
    long base = 1_000_000L;
    LinkedHashSet<UUID> inherited = new LinkedHashSet<>();
    inherited.add(UuidV7.newUuidV7(base - 100, rand));
    byte[] body = ProtoCodec.encodeCommittedTransactions(inherited);
    byte[] rewritten = ProtoCodec.rewriteCommittedBytes(body, Collections.emptyList(), base);
    ProtoCatalogFile cat = ProtoCatalogFile.builder(LOC)
        .setCommittedTransactionsBytes(rewritten)
        .build();
    assertThat(cat.highestDroppedTimestampMs()).isEqualTo(base - 100);

    // Empty / no-GC case: 0.
    ProtoCatalogFile fresh = ProtoCatalogFile.builder(LOC).build();
    assertThat(fresh.highestDroppedTimestampMs()).isZero();
  }

  // ============================================================
  // Builder.build(watermark)
  // ============================================================

  @Test
  void builderBuildWithWatermarkAppliesGc() throws Exception {
    Random rand = new Random(0x666);
    long base = 1_000_000L;
    LinkedHashSet<UUID> inherited = new LinkedHashSet<>();
    inherited.add(UuidV7.newUuidV7(base - 100, rand));
    inherited.add(UuidV7.newUuidV7(base + 100, rand));
    byte[] body = ProtoCodec.encodeCommittedTransactions(inherited);

    ProtoCatalogFile cat = ProtoCatalogFile.builder(LOC)
        .setCommittedTransactionsBytes(body)
        .build(base);
    assertThat(cat.highestDroppedTimestampMs()).isEqualTo(base - 100);
    assertThat(cat.committedTransactions()).hasSize(1);
  }

  @Test
  void builderBuildNoArgDoesNotGc() throws Exception {
    Random rand = new Random(0x777);
    long base = 1_000_000L;
    LinkedHashSet<UUID> inherited = new LinkedHashSet<>();
    inherited.add(UuidV7.newUuidV7(base - 100, rand));
    byte[] body = ProtoCodec.encodeCommittedTransactions(inherited);

    ProtoCatalogFile cat = ProtoCatalogFile.builder(LOC)
        .setCommittedTransactionsBytes(body)
        .build();
    assertThat(cat.highestDroppedTimestampMs()).isZero();
    assertThat(cat.committedTransactions()).hasSize(1);
  }
}
