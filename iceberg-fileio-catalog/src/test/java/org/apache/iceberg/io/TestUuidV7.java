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

import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.RepeatedTest;

class TestUuidV7 {

  @Test
  void mintIsV7() {
    UUID u = UuidV7.newUuidV7();
    assertThat(UuidV7.isV7(u)).isTrue();
    assertThat((u.getMostSignificantBits() >>> 12) & 0xF).isEqualTo(0x7L);
    assertThat((u.getLeastSignificantBits() >>> 62) & 0x3).isEqualTo(0x2L);
  }

  @Test
  void timestampMsRecoverable() {
    long before = System.currentTimeMillis();
    UUID u = UuidV7.newUuidV7();
    long after = System.currentTimeMillis();
    assertThat(UuidV7.timestampMs(u)).isBetween(before, after);
  }

  @Test
  void canonicalFixturesAreV7() {
    // The fixture UUIDs hand-rolled with version=7, variant=0b10.
    assertThat(UuidV7.isV7(UUID.fromString("01900000-0000-7000-8000-000000000001"))).isTrue();
    assertThat(UuidV7.isV7(UUID.fromString("01900000-0000-7000-8000-000000000071"))).isTrue();
    assertThat(UuidV7.isV7(UUID.fromString("01900000-0000-7fff-bfff-ffffffffffff"))).isTrue();
  }

  @Test
  void v4IsRejected() {
    // UUID.randomUUID() is v4 — variant is RFC 4122 (0b10) but version is 4.
    UUID v4 = UUID.randomUUID();
    assertThat(UuidV7.isV7(v4)).isFalse();
  }

  @Test
  void zeroAndAllOnesAreNotV7() {
    assertThat(UuidV7.isV7(new UUID(0L, 0L))).isFalse();
    assertThat(UuidV7.isV7(new UUID(-1L, -1L))).isFalse();
  }

  @Test
  void seededMintIsDeterministic() {
    long ts = 0x019E08E45F5EL;
    UUID a = UuidV7.newUuidV7(ts, new Random(42));
    UUID b = UuidV7.newUuidV7(ts, new Random(42));
    assertThat(a).isEqualTo(b);
    assertThat(UuidV7.timestampMs(a)).isEqualTo(ts);
    assertThat(UuidV7.isV7(a)).isTrue();
  }

  /**
   * Pack/unpack round-trip: for any v7 UUID, packing the random bits and
   * reconstructing from (timestamp, packed) bytes recovers the original.
   */
  @RepeatedTest(50)
  void packUnpackRoundTrip() {
    UUID u = UuidV7.newUuidV7();
    byte[] packed = UuidV7.packRandomBits(u);
    assertThat(packed).hasSize(10);
    UUID recovered = UuidV7.fromTimestampAndRandom(UuidV7.timestampMs(u), packed, 0);
    assertThat(recovered).isEqualTo(u);
  }

  /**
   * Pack respects the 6-bit zero pad: the bottom 6 bits of byte 9 must
   * always be zero (rand_b's bits 0-1 land in byte 9's top 2 bits).
   */
  @RepeatedTest(50)
  void packLowerSixBitsAreZero() {
    UUID u = UuidV7.newUuidV7();
    byte[] packed = UuidV7.packRandomBits(u);
    assertThat(packed[9] & 0x3F).isEqualTo(0);
  }

  /**
   * UUID.compareTo on v7 UUIDs orders by timestamp ascending, so a sort by
   * the natural ordering puts the oldest first / newest last. Used by the
   * codec when sorting the committed-set.
   */
  @Test
  void naturalOrderingTracksTimestamp() {
    Random rand = new Random(0xDEADBEEFL);
    UUID a = UuidV7.newUuidV7(1_000L, rand);
    UUID b = UuidV7.newUuidV7(2_000L, rand);
    UUID c = UuidV7.newUuidV7(3_000L, rand);
    assertThat(a.compareTo(b)).isNegative();
    assertThat(b.compareTo(c)).isNegative();
    assertThat(a.compareTo(c)).isNegative();
  }

  /** Offset != 0 must work for callers that pack many UUIDs into one buffer. */
  @Test
  void packUnpackAtNonZeroOffset() {
    Random rand = new Random(0xCAFEBABEL);
    UUID u1 = UuidV7.newUuidV7(1_700_000_000_000L, rand);
    UUID u2 = UuidV7.newUuidV7(1_700_000_001_000L, rand);
    byte[] buf = new byte[20];
    System.arraycopy(UuidV7.packRandomBits(u1), 0, buf, 0, 10);
    System.arraycopy(UuidV7.packRandomBits(u2), 0, buf, 10, 10);
    assertThat(UuidV7.fromTimestampAndRandom(UuidV7.timestampMs(u1), buf, 0)).isEqualTo(u1);
    assertThat(UuidV7.fromTimestampAndRandom(UuidV7.timestampMs(u2), buf, 10)).isEqualTo(u2);
  }
}
