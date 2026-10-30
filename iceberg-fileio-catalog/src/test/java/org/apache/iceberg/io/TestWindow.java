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
import java.util.Random;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * {@link Window} against TableMetadata.Builder's rule for the metadata log (keep the newest
 * {@code bound - 1} entries, then add one): appends mostly to the newest list, sometimes to an
 * older one, under a bound that sometimes changes; every list made along the way must still be
 * the one it was.
 */
class TestWindow {

  @ParameterizedTest
  @ValueSource(longs = {1, 2, 3, 4, 5, 6, 7, 8})
  void readsAsTheListItWasMadeAs(long seed) {
    Random random = new Random(seed);
    List<Window<Integer>> made = new ArrayList<>();
    List<List<Integer>> expected = new ArrayList<>();
    List<Integer> start = List.of(-3, -2, -1);
    made.add(Window.of(start));
    expected.add(start);
    int bound = 10;
    for (int i = 0; i < 2_000; i++) {
      if (random.nextInt(100) == 0) {
        bound = 1 + random.nextInt(20);
      }
      // Mostly the newest list, as replay extends a table's log; sometimes an older one
      int from = random.nextInt(10) == 0 ? random.nextInt(made.size()) : made.size() - 1;
      List<Integer> before = expected.get(from);
      List<Integer> after = new ArrayList<>(
          before.subList(Math.max(0, before.size() - (bound - 1)), before.size()));
      after.add(i);
      made.add(made.get(from).append(i, bound));
      expected.add(after);
    }
    for (int i = 0; i < made.size(); i++) {
      assertThat(made.get(i)).as("list %s", i).containsExactlyElementsOf(expected.get(i));
    }
  }
}
