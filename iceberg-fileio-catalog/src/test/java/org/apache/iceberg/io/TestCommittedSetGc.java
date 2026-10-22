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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.intentlog.DedupHorizonExceededException;
import org.intentlog.wire.CommittedSetCodec;
import org.intentlog.wire.UuidV7;
import org.junit.jupiter.api.Test;

/**
 * The catalog's committed set: the Builder's replay of a GC record (advanceHorizon) and the tri-state
 * {@link ProtoCatalogFile#containsTransaction}, which delegate to intentlog-core's
 * CommittedSet (whose own tests cover the codec and GC).
 *
 * <p>End-to-end (catalog write → CAS rewrite with retention=0 → fresh read →
 * horizon-exceeded) lives in {@link TestProtoCommitKnobs}.
 */
class TestCommittedSetGc {

  private static final InputFile LOC = new InputFile() {
    @Override public long getLength() { return 0; }
    @Override public SeekableInputStream newStream() { throw new UnsupportedOperationException(); }
    @Override public String location() { return "test://gc"; }
    @Override public boolean exists() { return false; }
  };

  // ============================================================
  // Tri-state containsTransaction
  // ============================================================

  @Test
  void containsTransactionFoundReturnsTrue() throws Exception {
    Random rand = new Random(0x111);
    long base = System.currentTimeMillis();
    UUID u = UuidV7.newUuidV7(base, rand);
    byte[] body = CommittedSetCodec.encode(java.util.List.of(u));
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
    byte[] body = CommittedSetCodec.encode(inherited);
    byte[] rewritten = CommittedSetCodec.rewrite(body, Collections.emptyList(), base);

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
    byte[] body = CommittedSetCodec.encode(inherited);
    byte[] rewritten = CommittedSetCodec.rewrite(body, Collections.emptyList(), base);

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
    byte[] body = CommittedSetCodec.encode(inherited);
    byte[] rewritten = CommittedSetCodec.rewrite(body, Collections.emptyList(), base);
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
    byte[] body = CommittedSetCodec.encode(inherited);
    byte[] rewritten = CommittedSetCodec.rewrite(body, Collections.emptyList(), base);
    ProtoCatalogFile cat = ProtoCatalogFile.builder(LOC)
        .setCommittedTransactionsBytes(rewritten)
        .build();
    assertThat(cat.highestDroppedTimestampMs()).isEqualTo(base - 100);

    // Empty / no-GC case: 0.
    ProtoCatalogFile fresh = ProtoCatalogFile.builder(LOC).build();
    assertThat(fresh.highestDroppedTimestampMs()).isZero();
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

    ProtoCatalogFile cat = ProtoCatalogFile.builder(LOC)
        .setCommittedTransactionsBytes(body)
        .advanceHorizon(base)
        .build();
    assertThat(cat.highestDroppedTimestampMs()).isEqualTo(base - 100);
    assertThat(cat.committedTransactions()).hasSize(1);
  }

  @Test
  void builderBuildNoArgDoesNotGc() throws Exception {
    Random rand = new Random(0x777);
    long base = 1_000_000L;
    LinkedHashSet<UUID> inherited = new LinkedHashSet<>();
    inherited.add(UuidV7.newUuidV7(base - 100, rand));
    byte[] body = CommittedSetCodec.encode(inherited);

    ProtoCatalogFile cat = ProtoCatalogFile.builder(LOC)
        .setCommittedTransactionsBytes(body)
        .build();
    assertThat(cat.highestDroppedTimestampMs()).isZero();
    assertThat(cat.committedTransactions()).hasSize(1);
  }
}
