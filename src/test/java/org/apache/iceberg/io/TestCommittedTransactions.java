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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Round-trip and defensive-decode tests for the compressed
 * {@code CommittedTransactions} sub-message that backs
 * {@code Checkpoint.committed_transaction_ids}.
 */
class TestCommittedTransactions {

  private static final InputFile LOC = new InputFile() {
    @Override public long getLength() { return 0; }
    @Override public SeekableInputStream newStream() { throw new UnsupportedOperationException(); }
    @Override public String location() { return "test://committed-txns"; }
    @Override public boolean exists() { return false; }
  };

  @Test
  void emptySetRoundTrips() throws IOException {
    byte[] body = ProtoCodec.encodeCommittedTransactions(java.util.Collections.emptyList());
    ProtoCatalogFile.Builder builder = ProtoCatalogFile.builder(LOC);
    ProtoCodec.decodeCommittedTransactions(body, builder);
    assertThat(builder.build().committedTransactions()).isEmpty();
  }

  @Test
  void singleEntryRoundTrips() throws IOException {
    UUID u = UuidV7.newUuidV7();
    Set<UUID> recovered = roundTripImpl(java.util.List.of(u));
    assertThat(recovered).containsExactly(u);
  }

  @Test
  void tenEntriesRoundTrip() throws IOException {
    Random rand = new Random(0xCAFE);
    Set<UUID> input = new LinkedHashSet<>();
    long ts = System.currentTimeMillis();
    for (int i = 0; i < 10; i++) {
      input.add(UuidV7.newUuidV7(ts + i * 17, rand));
    }
    Set<UUID> recovered = roundTripImpl(input);
    assertThat(recovered).isEqualTo(input);
  }

  @Test
  void thousandEntriesRoundTrip() throws IOException {
    Random rand = new Random(0xBEEF);
    Set<UUID> input = new LinkedHashSet<>();
    long base = System.currentTimeMillis();
    for (int i = 0; i < 1000; i++) {
      input.add(UuidV7.newUuidV7(base + rand.nextInt(60_000), rand));
    }
    Set<UUID> recovered = roundTripImpl(input);
    assertThat(recovered).hasSize(input.size()).isEqualTo(input);
  }

  @Test
  void determinismIndependentOfIterationOrder() throws IOException {
    Random rand = new Random(0xD00D);
    java.util.List<UUID> list = new ArrayList<>();
    long base = System.currentTimeMillis();
    for (int i = 0; i < 50; i++) {
      list.add(UuidV7.newUuidV7(base + i * 13, rand));
    }
    byte[] forward = ProtoCodec.encodeCommittedTransactions(list);
    java.util.List<UUID> reversed = new ArrayList<>(list);
    Collections.reverse(reversed);
    byte[] reversedBytes = ProtoCodec.encodeCommittedTransactions(reversed);
    assertThat(forward).as("encode is sort-stable: same set should produce identical bytes")
        .isEqualTo(reversedBytes);
  }

  @Test
  void compactnessAtThousandEntries() throws IOException {
    Random rand = new Random(0xABCD);
    java.util.List<UUID> input = new ArrayList<>();
    long base = System.currentTimeMillis();
    // Clustered within 60s — typical commit traffic.
    for (int i = 0; i < 1000; i++) {
      input.add(UuidV7.newUuidV7(base + rand.nextInt(60_000), rand));
    }
    byte[] body = ProtoCodec.encodeCommittedTransactions(input);
    // Status quo would be 1000 * 18 = 18000 bytes (with framing). Compressed
    // body should be substantially smaller. Allow up to 13.5 bytes/entry to
    // stay robust across distributions.
    assertThat(body.length).isLessThan(1000 * 14);
  }

  @Test
  void mismatchedLengthInvariantThrows() {
    // Hand-craft a payload with 2 deltas but only 10 bytes of random
    // (should be 20). Field tags constructed manually:
    //   field 1, fixed64 (wire 1):     tag 0x09
    //   field 2, varint (wire 0):      tag 0x10
    //   field 3, length-delim (wire 2): tag 0x1a
    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
    // max_timestamp_ms: 0x09 + 8 bytes
    out.write(0x09);
    for (int i = 0; i < 8; i++) out.write(0);
    // delta 1: 0x10 + varint(0)
    out.write(0x10);
    out.write(0);
    // delta 2: 0x10 + varint(1)
    out.write(0x10);
    out.write(1);
    // random_packed: 0x1a + length(10) + 10 bytes (only enough for ONE entry)
    out.write(0x1a);
    out.write(10);
    for (int i = 0; i < 10; i++) out.write(0);
    byte[] body = out.toByteArray();

    ProtoCatalogFile.Builder builder = ProtoCatalogFile.builder(LOC);
    assertThatThrownBy(() -> ProtoCodec.decodeCommittedTransactions(body, builder))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("CommittedTransactions");
  }

  @Test
  void roundTripPreservesUuidValuesExactly() throws IOException {
    // Use canonical fixture UUIDs the project uses elsewhere (all v7-shaped).
    java.util.List<UUID> input = java.util.List.of(
        UUID.fromString("01900000-0000-7000-8000-000000000001"),
        UUID.fromString("01900000-0000-7000-8000-000000000071"),
        UUID.fromString("01900000-0000-7000-8000-000000000081"),
        UUID.fromString("01900000-0000-7fff-bfff-ffffffffffff"));
    Set<UUID> recovered = roundTripImpl(input);
    assertThat(recovered).containsExactlyInAnyOrderElementsOf(input);
  }

  @Test
  void fullCheckpointRoundTripWithCommittedSet() throws IOException {
    // End-to-end: build a full ProtoCatalogFile with committed txns, encode
    // through the checkpoint codec (not just the inner sub-message), decode,
    // verify set equality. This exercises the wire-level integration.
    Random rand = new Random(0xFEEDFACE);
    long base = System.currentTimeMillis();
    Set<UUID> txns = new LinkedHashSet<>();
    for (int i = 0; i < 25; i++) {
      txns.add(UuidV7.newUuidV7(base + i * 31, rand));
    }
    ProtoCatalogFile.Builder b = ProtoCatalogFile.builder(LOC)
        .setCatalogUuid(UuidV7.newUuidV7(base, rand))
        .setNextNamespaceId(1)
        .setNextTableId(1);
    for (UUID u : txns) {
      b.addCommittedTransaction(u);
    }
    ProtoCatalogFile original = b.rebuildLookups().build();

    ProtoCatalogFormat.ProtoIdManager idManager = new ProtoCatalogFormat.ProtoIdManager();
    idManager.setGlobals(original.nextNamespaceId(), original.nextTableId());
    ProtoCatalogFormat.Mut mut = new ProtoCatalogFormat.Mut(original);
    byte[] checkpointBytes = ProtoCodec.encodeCheckpoint(original, mut, idManager);

    ProtoCatalogFile.Builder decodedBuilder = ProtoCatalogFile.builder(LOC);
    ProtoCodec.decodeCheckpoint(checkpointBytes, decodedBuilder);
    ProtoCatalogFile decoded = decodedBuilder.build();

    assertThat(new HashSet<>(decoded.committedTransactions())).isEqualTo(txns);
  }

  // ============================================================
  // Helpers
  // ============================================================

  private static Set<UUID> roundTripImpl(java.util.Collection<UUID> input) throws IOException {
    byte[] body = ProtoCodec.encodeCommittedTransactions(input);
    ProtoCatalogFile.Builder builder = ProtoCatalogFile.builder(LOC);
    ProtoCodec.decodeCommittedTransactions(body, builder);
    return new HashSet<>(builder.build().committedTransactions());
  }
}
