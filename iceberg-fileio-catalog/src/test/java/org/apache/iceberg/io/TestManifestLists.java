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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Example;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

/**
 * A checkpoint's manifest list as a change of its parent's ({@link ManifestLists}): the change
 * makes the list exactly, whatever the two lists hold, and a list that only adds to or removes
 * from its parent's costs what it adds or removes.
 */
class TestManifestLists {

  /** Lists of manifests from a small universe, so that two lists share some. */
  @Provide
  Arbitrary<List<String>> lists() {
    return Arbitraries.integers().between(0, 24).map(i -> "m" + i).list().ofMaxSize(30);
  }

  @Property(tries = 2000)
  void aChangeMakesTheListOfItsBase(
      @ForAll("lists") List<String> base, @ForAll("lists") List<String> list) {
    ManifestLists.Change change = ManifestLists.diff(base, list);
    List<String> addedPaths = new ArrayList<>();
    for (int position : change.added) {
      addedPaths.add(list.get(position));
    }
    assertThat(ManifestLists.apply(base, change.removed, change.added, addedPaths))
        .isEqualTo(list);
    assertThat(ManifestLists.isChange(base.size(), change.removed, change.added)).isTrue();
    // Made on first use, of a base itself made on first use
    List<String> stored = ManifestLists.whole(base.toArray(new String[0]));
    List<String> made = ManifestLists.changed(stored, change.removed, change.added, addedPaths);
    assertThat(made).hasSize(list.size());
    assertThat(made).isEqualTo(list);
  }

  @Example
  void aMalformedChangeIsNotOne() {
    assertThat(ManifestLists.isChange(3, new int[] {0, 2}, new int[] {0})).isTrue();
    assertThat(ManifestLists.isChange(3, new int[] {2, 0}, new int[0])).isFalse();
    assertThat(ManifestLists.isChange(3, new int[] {3}, new int[0])).isFalse();
    assertThat(ManifestLists.isChange(3, new int[] {1, 1}, new int[0])).isFalse();
    assertThat(ManifestLists.isChange(3, new int[0], new int[] {4})).isFalse();
    assertThat(ManifestLists.isChange(3, new int[0], new int[] {3})).isTrue();
    assertThat(ManifestLists.isChange(3, new int[] {0}, new int[] {-1})).isFalse();
  }

  @Example
  void aFastAppendCostsTheManifestItAdds() {
    List<String> parent = IntStream.range(0, 500).mapToObj(i -> "m" + i).collect(Collectors.toList());
    List<String> child = new ArrayList<>();
    child.add("new");
    child.addAll(parent);
    ManifestLists.Change change = ManifestLists.diff(parent, child);
    assertThat(change.removed).isEmpty();
    assertThat(change.added).containsExactly(0);
    assertThat(change.cost()).isEqualTo(3);
  }

  @Example
  void aMergeCostsWhatItReplaces() {
    List<String> parent = List.of("a", "b", "c", "d", "e");
    List<String> child = List.of("merged", "d", "e");
    ManifestLists.Change change = ManifestLists.diff(parent, child);
    assertThat(change.removed).containsExactly(0, 1, 2);
    assertThat(change.added).containsExactly(0);
  }

  @Example
  void aReorderKeepsTheLongestRunInTheBasesOrder() {
    List<String> parent = List.of("a", "b", "c", "d");
    List<String> child = List.of("d", "a", "b", "c");
    ManifestLists.Change change = ManifestLists.diff(parent, child);
    assertThat(change.removed).containsExactly(3);
    assertThat(change.added).containsExactly(0);
  }

  @Example
  void anUnchangedListCostsItsBase() {
    List<String> list = List.of("a", "b", "c");
    ManifestLists.Change change = ManifestLists.diff(list, list);
    assertThat(change.removed).isEmpty();
    assertThat(change.added).isEmpty();
    assertThat(ManifestLists.apply(list, change.removed, change.added, List.of())).isEqualTo(list);
  }
}
