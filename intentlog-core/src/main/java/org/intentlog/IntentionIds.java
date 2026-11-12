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

import java.util.Arrays;
import java.util.UUID;

/**
 * The ids of a view's intention records, in log order, as replay decoded them: what a fold looks
 * a commit's record up in among those a replica already holds, without decoding them again
 * (errata D25). Immutable.
 */
final class IntentionIds {
  static final IntentionIds EMPTY = new IntentionIds(new long[0], 0);

  // Each id as two longs, most significant first
  private final long[] bits;
  private final int size;

  private IntentionIds(long[] bits, int size) {
    this.bits = bits;
    this.size = size;
  }

  int size() {
    return size;
  }

  boolean contains(UUID id) {
    long most = id.getMostSignificantBits();
    long least = id.getLeastSignificantBits();
    for (int i = 0; i < 2 * size; i += 2) {
      if (bits[i] == most && bits[i + 1] == least) {
        return true;
      }
    }
    return false;
  }

  /** These ids, then {@code after}'s. */
  IntentionIds plus(IntentionIds after) {
    if (after.size == 0) {
      return this;
    }
    if (size == 0) {
      return after;
    }
    long[] joined = Arrays.copyOf(bits, 2 * (size + after.size));
    System.arraycopy(after.bits, 0, joined, 2 * size, 2 * after.size);
    return new IntentionIds(joined, size + after.size);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof IntentionIds)) {
      return false;
    }
    IntentionIds that = (IntentionIds) other;
    return size == that.size
        && Arrays.equals(bits, 0, 2 * size, that.bits, 0, 2 * that.size);
  }

  @Override
  public int hashCode() {
    int hash = size;
    for (int i = 0; i < 2 * size; i++) {
      hash = 31 * hash + Long.hashCode(bits[i]);
    }
    return hash;
  }

  /** Ids added in log order. */
  static final class Builder {
    private long[] bits = new long[16];
    private int size = 0;

    void add(UUID id) {
      if (2 * size == bits.length) {
        bits = Arrays.copyOf(bits, 2 * bits.length);
      }
      bits[2 * size] = id.getMostSignificantBits();
      bits[2 * size + 1] = id.getLeastSignificantBits();
      size += 1;
    }

    IntentionIds build() {
      return size == 0 ? EMPTY : new IntentionIds(bits, size);
    }
  }
}
