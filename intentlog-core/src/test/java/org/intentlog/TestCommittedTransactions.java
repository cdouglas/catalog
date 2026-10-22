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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.intentlog.wire.CommittedSetCodec;
import org.intentlog.wire.UuidV7;
import org.junit.jupiter.api.Test;

/**
 * Round-trip and defensive-decode tests for the compressed
 * {@code CommittedTransactions} sub-message that backs
 * {@code Checkpoint.committed_transaction_ids}.
 */
class TestCommittedTransactions {


  @Test
  void emptySetRoundTrips() throws IOException {
    byte[] body = CommittedSetCodec.encode(Collections.emptyList());
    CommittedSet cat = CommittedSet.of(body);
    assertThat(cat.ids()).isEmpty();
  }

  @Test
  void singleEntryRoundTrips() throws IOException {
    UUID u = UuidV7.newUuidV7();
    Set<UUID> recovered = roundTripImpl(List.of(u));
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
    List<UUID> list = new ArrayList<>();
    long base = System.currentTimeMillis();
    for (int i = 0; i < 50; i++) {
      list.add(UuidV7.newUuidV7(base + i * 13, rand));
    }
    byte[] forward = CommittedSetCodec.encode(list);
    List<UUID> reversed = new ArrayList<>(list);
    Collections.reverse(reversed);
    byte[] reversedBytes = CommittedSetCodec.encode(reversed);
    assertThat(forward).as("encode is sort-stable: same set should produce identical bytes")
        .isEqualTo(reversedBytes);
  }

  @Test
  void compactnessAtThousandEntries() throws IOException {
    Random rand = new Random(0xABCD);
    List<UUID> input = new ArrayList<>();
    long base = System.currentTimeMillis();
    // Clustered within 60s — typical commit traffic.
    for (int i = 0; i < 1000; i++) {
      input.add(UuidV7.newUuidV7(base + rand.nextInt(60_000), rand));
    }
    byte[] body = CommittedSetCodec.encode(input);
    // Status quo would be 1000 * 18 = 18000 bytes (with framing). Compressed
    // body should be substantially smaller. Allow up to 13.5 bytes/entry to
    // stay robust across distributions.
    assertThat(body.length).isLessThan(1000 * 14);
  }

  @Test
  void truncatedPackedEntryThrows() throws IOException {
    // Hand-craft a payload whose packed_entries blob declares a delta but
    // doesn't include the expected 10 random bytes after it. Field tags:
    //   field 1, fixed64 (wire 1):     tag 0x09
    //   field 2, length-delim (wire 2): tag 0x12
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(0x09);
    for (int i = 0; i < 8; i++) out.write(0);
    out.write(0x12);
    // packed_entries length = 4: one delta varint (0x00 = 0), then only 3
    // random bytes — should be 10. Decode must reject.
    out.write(4);
    out.write(0);
    out.write(0);
    out.write(0);
    out.write(0);
    byte[] body = out.toByteArray();

    assertThatThrownBy(() -> CommittedSetCodec.decode(body))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("CommittedTransactions");
  }

  @Test
  void roundTripPreservesUuidValuesExactly() throws IOException {
    // Use canonical fixture UUIDs the project uses elsewhere (all v7-shaped).
    List<UUID> input = List.of(
        UUID.fromString("01900000-0000-7000-8000-000000000001"),
        UUID.fromString("01900000-0000-7000-8000-000000000071"),
        UUID.fromString("01900000-0000-7000-8000-000000000081"),
        UUID.fromString("01900000-0000-7fff-bfff-ffffffffffff"));
    Set<UUID> recovered = roundTripImpl(input);
    assertThat(recovered).containsExactlyInAnyOrderElementsOf(input);
  }

  // ============================================================
  // Helpers
  // ============================================================

  /**
   * Round trip through the set: encode the input, wrap the bytes in a
   * CommittedSet, and decode them through its ids() getter.
   */
  private static Set<UUID> roundTripImpl(Collection<UUID> input) throws IOException {
    byte[] body = CommittedSetCodec.encode(input);
    CommittedSet cat = CommittedSet.of(body);
    return new HashSet<>(cat.ids());
  }
}
