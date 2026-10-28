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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * {@link PersistentMap} against a {@link HashMap}: random additions, replacements and removals
 * over keys whose hashes share bits at every level of the trie, and keys whose hashes are the same
 * in every bit; every map made along the way must still be the one it was.
 */
class TestPersistentMap {

  /** A key with a chosen hash. */
  private static final class Key {
    final int hash;
    final int id;

    Key(int hash, int id) {
      this.hash = hash;
      this.id = id;
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Key && ((Key) other).id == id;
    }

    @Override
    public String toString() {
      return "Key(" + Integer.toHexString(hash) + ", " + id + ")";
    }
  }

  @ParameterizedTest
  @ValueSource(longs = {1, 2, 3, 4, 5, 6, 7, 8})
  void readsAsTheMapItWasMadeAs(long seed) {
    Random random = new Random(seed);
    List<Key> keys = new ArrayList<>();
    for (int i = 0; i < 400; i++) {
      int hash;
      switch (random.nextInt(4)) {
        case 0:
          hash = random.nextInt();
          break;
        case 1:
          // Low bits alike: deep tries
          hash = (random.nextInt() << 20) | 0x5A5A5;
          break;
        case 2:
          // Every bit alike: collision nodes
          hash = 0x12345678;
          break;
        default:
          hash = random.nextInt(64);
      }
      keys.add(new Key(hash, i));
    }
    List<PersistentMap<Key, Integer>> versions = new ArrayList<>();
    List<Map<Key, Integer>> expected = new ArrayList<>();
    PersistentMap<Key, Integer> map = PersistentMap.empty();
    Map<Key, Integer> reference = new HashMap<>();
    for (int step = 0; step < 4000; step++) {
      Key key = keys.get(random.nextInt(keys.size()));
      if (random.nextInt(3) == 0) {
        map = map.minus(key);
        reference.remove(key);
      } else {
        int value = random.nextInt(5);
        map = map.plus(key, value);
        reference.put(key, value);
      }
      assertThat(map.size()).isEqualTo(reference.size());
      if (step % 97 == 0) {
        versions.add(map);
        expected.add(new HashMap<>(reference));
      }
    }
    for (int i = 0; i < versions.size(); i++) {
      PersistentMap<Key, Integer> version = versions.get(i);
      Map<Key, Integer> want = expected.get(i);
      assertThat(version).isEqualTo(want);
      assertThat(new HashMap<>(version)).isEqualTo(want);
      for (Key key : keys) {
        assertThat(version.get(key)).isEqualTo(want.get(key));
        assertThat(version.containsKey(key)).isEqualTo(want.containsKey(key));
      }
    }
    for (Key key : keys) {
      map = map.minus(key);
    }
    assertThat(map).isEmpty();
    assertThat(map).isSameAs(PersistentMap.empty());
  }
}
