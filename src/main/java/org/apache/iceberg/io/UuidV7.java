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

import java.security.SecureRandom;
import java.util.Random;
import java.util.UUID;

/**
 * UUIDv7 (RFC 9562) generation and bit-level helpers used by the catalog format.
 *
 * <p>Layout (128 bits):
 * <pre>
 *   bits  0..47  : Unix-millisecond timestamp (big-endian, top of MSB long)
 *   bits 48..51  : version nibble (= 0x7)
 *   bits 52..63  : rand_a (12 random bits)
 *   bits 64..65  : variant (= 0b10)
 *   bits 66..127 : rand_b (62 random bits)
 * </pre>
 *
 * <p>The codec exploits this layout to compress {@code committed_transaction_ids}:
 * sorted v7 UUIDs can be stored as a delta-encoded timestamp plus a 10-byte
 * random suffix, dropping the 6 fixed version+variant bits.
 */
public final class UuidV7 {

  private UuidV7() {}

  private static final SecureRandom RANDOM = new SecureRandom();

  /** Mints a fresh UUIDv7 from the current wall-clock time and {@code SecureRandom}. */
  public static UUID newUuidV7() {
    return newUuidV7(System.currentTimeMillis(), RANDOM);
  }

  /**
   * Test/seeded variant. Mints a UUIDv7 with the supplied timestamp and the
   * supplied {@code Random} as the entropy source. Use a {@code Random}
   * with a fixed seed for deterministic test fixtures.
   */
  public static UUID newUuidV7(long tsMs, Random source) {
    byte[] rand = new byte[10];
    source.nextBytes(rand);
    long randA = (((rand[0] & 0xFFL) << 8) | (rand[1] & 0xFFL)) & 0xFFFL;
    long msb = (tsMs << 16) | (0x7L << 12) | randA;
    long randB = 0L;
    for (int i = 2; i < 10; i++) {
      randB = (randB << 8) | (rand[i] & 0xFFL);
    }
    randB &= 0x3FFFFFFFFFFFFFFFL;
    long lsb = (0x2L << 62) | randB;
    return new UUID(msb, lsb);
  }

  /** True iff {@code u} has version=7 and the RFC 4122 variant (0b10). */
  public static boolean isV7(UUID u) {
    return ((u.getMostSignificantBits() >>> 12) & 0xFL) == 0x7L
        && ((u.getLeastSignificantBits() >>> 62) & 0x3L) == 0x2L;
  }

  /** Returns the 48-bit Unix-millisecond timestamp embedded in {@code u}. Caller must ensure {@link #isV7} first. */
  public static long timestampMs(UUID u) {
    return u.getMostSignificantBits() >>> 16;
  }

  /**
   * Packs the 12-bit {@code rand_a} and 62-bit {@code rand_b} of {@code u}
   * into a 10-byte big-endian sequence: 12 bits rand_a, 62 bits rand_b,
   * 6 zero pad bits. Drops the 4 version bits and 2 variant bits, which
   * are reconstructed by {@link #fromTimestampAndRandom}.
   */
  public static byte[] packRandomBits(UUID u) {
    long randA = u.getMostSignificantBits() & 0xFFFL;
    long randB = u.getLeastSignificantBits() & 0x3FFFFFFFFFFFFFFFL;
    // Treat the output as an 80-bit big-endian integer V where:
    //   V[0..11]  = randA  (rand_a's 12 bits, MSB-first)
    //   V[12..73] = randB  (rand_b's 62 bits, MSB-first)
    //   V[74..79] = 0      (padding)
    // Split V into V_high (top 64 bits, written as bytes 0..7) and V_low
    // (bottom 16 bits, written as bytes 8..9).
    long high = (randA << 52) | (randB >>> 10);
    int low = (int) ((randB << 6) & 0xFFFFL);
    byte[] out = new byte[10];
    for (int i = 0; i < 8; i++) {
      out[i] = (byte) ((high >>> (56 - 8 * i)) & 0xFFL);
    }
    out[8] = (byte) ((low >>> 8) & 0xFF);
    out[9] = (byte) (low & 0xFF);
    return out;
  }

  /**
   * Reconstructs a UUIDv7 from its 48-bit timestamp and the 10-byte packed
   * random sequence produced by {@link #packRandomBits}. Fixed version and
   * variant bits are filled in.
   */
  public static UUID fromTimestampAndRandom(long tsMs, byte[] packed, int offset) {
    long high = 0L;
    for (int i = 0; i < 8; i++) {
      high = (high << 8) | (packed[offset + i] & 0xFFL);
    }
    int low = ((packed[offset + 8] & 0xFF) << 8) | (packed[offset + 9] & 0xFF);
    long randA = high >>> 52;
    long randB = ((high & 0x000FFFFFFFFFFFFFL) << 10) | (((long) low & 0xFFFFL) >>> 6);
    long msb = (tsMs << 16) | (0x7L << 12) | (randA & 0xFFFL);
    long lsb = (0x2L << 62) | (randB & 0x3FFFFFFFFFFFFFFFL);
    return new UUID(msb, lsb);
  }
}
