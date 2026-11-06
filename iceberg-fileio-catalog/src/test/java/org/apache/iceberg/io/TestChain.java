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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicIntegerArray;
import org.junit.jupiter.api.Test;

/** A {@link Chain} whose first elements are made on first use ({@link Chain#lazy}). */
class TestChain {

  /** An element: its key, and which making of it this is. */
  private static final class Element {
    private final long key;

    Element(long key) {
      this.key = key;
    }
  }

  private static long[] keys(int n) {
    long[] keys = new long[n];
    for (int i = 0; i < n; i++) {
      keys[i] = 100 + i;
    }
    return keys;
  }

  @Test
  void aLazyElementIsMadeOnceWhenFirstRead() {
    AtomicIntegerArray made = new AtomicIntegerArray(5);
    long[] keys = keys(5);
    Chain<Element> chain =
        Chain.lazy(keys, i -> {
          made.incrementAndGet(i);
          return new Element(keys[i]);
        }, e -> e.key);

    // Keys, membership and size need no element
    assertThat(chain).hasSize(5);
    assertThat(chain.keyAt(3)).isEqualTo(103);
    assertThat(chain.containsKey(104)).isTrue();
    assertThat(chain.containsKey(105)).isFalse();
    for (int i = 0; i < 5; i++) {
      assertThat(made.get(i)).isZero();
    }

    Element third = chain.byKey(102);
    assertThat(third.key).isEqualTo(102);
    assertThat(chain.get(2)).isSameAs(third);
    assertThat(made.get(2)).isEqualTo(1);
    assertThat(made.get(1)).isZero();
    assertThat(chain.byKey(999)).isNull();
  }

  @Test
  void listsThatDivergeShareTheMadeElements() {
    AtomicIntegerArray made = new AtomicIntegerArray(3);
    long[] keys = keys(3);
    Chain<Element> chain =
        Chain.lazy(keys, i -> {
          made.incrementAndGet(i);
          return new Element(keys[i]);
        }, e -> e.key);
    Chain<Element> first = chain.append(new Element(1));
    // A second append to the same list copies it to a buffer of its own
    Chain<Element> second = chain.append(new Element(2));

    assertThat(first.keyAt(3)).isEqualTo(1);
    assertThat(second.keyAt(3)).isEqualTo(2);
    assertThat(second.containsKey(1)).isFalse();
    assertThat(second.byKey(2).key).isEqualTo(2);
    assertThat(second.containsKey(101)).isTrue();
    assertThat(made.get(1)).isZero();

    assertThat(first.get(0)).isSameAs(second.get(0)).isSameAs(chain.get(0));
    assertThat(made.get(0)).isEqualTo(1);
    List<Long> all = new ArrayList<>();
    second.forEach(e -> all.add(e.key));
    assertThat(all).containsExactly(100L, 101L, 102L, 2L);
  }

  @Test
  void readersRacingForAnElementGetOne() throws Exception {
    AtomicIntegerArray made = new AtomicIntegerArray(1);
    CountDownLatch start = new CountDownLatch(1);
    Chain<Element> chain =
        Chain.lazy(new long[] {7}, i -> {
          made.incrementAndGet(i);
          return new Element(7);
        }, e -> e.key);
    Element[] seen = new Element[8];
    List<Thread> threads = new ArrayList<>();
    for (int t = 0; t < seen.length; t++) {
      int at = t;
      Thread thread = new Thread(() -> {
        try {
          start.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        seen[at] = chain.get(0);
      });
      thread.start();
      threads.add(thread);
    }
    start.countDown();
    for (Thread thread : threads) {
      thread.join();
    }
    assertThat(made.get(0)).isEqualTo(1);
    for (Element element : seen) {
      assertThat(element).isSameAs(seen[0]);
    }
  }

  /**
   * Lists that diverge again and again (a replica a writer folds others' records onto after its
   * own landed elsewhere) each find exactly their own elements by key, whether a copy looks the
   * shared ones up in the buffer it copied or, deep enough, indexes them all again.
   */
  @Test
  void divergingListsFindOnlyTheirOwnElements() {
    Chain<Element> base = Chain.of(List.of(new Element(1), new Element(2)), e -> e.key);
    List<Chain<Element>> lists = new ArrayList<>();
    List<List<Long>> expected = new ArrayList<>();
    Chain<Element> current = base;
    List<Long> keys = new ArrayList<>(List.of(1L, 2L));
    for (int level = 0; level < 20; level++) {
      // A sibling extends the buffer first, so the next append copies
      Chain<Element> sibling = current.append(new Element(1000 + level));
      List<Long> siblingKeys = new ArrayList<>(keys);
      siblingKeys.add(1000L + level);
      lists.add(sibling);
      expected.add(siblingKeys);
      current = current.append(new Element(10 + level));
      keys = new ArrayList<>(keys);
      keys.add(10L + level);
      lists.add(current);
      expected.add(keys);
    }
    for (int i = 0; i < lists.size(); i++) {
      Chain<Element> list = lists.get(i);
      List<Long> own = expected.get(i);
      for (long k : own) {
        assertThat(list.containsKey(k)).as("list %s holds %s", i, k).isTrue();
        assertThat(list.byKey(k).key).isEqualTo(k);
      }
      for (int level = 0; level < 20; level++) {
        for (long k : new long[] {1000 + level, 10 + level}) {
          if (!own.contains(k)) {
            assertThat(list.containsKey(k)).as("list %s lacks %s", i, k).isFalse();
            assertThat(list.byKey(k)).isNull();
          }
        }
      }
    }
  }
}
