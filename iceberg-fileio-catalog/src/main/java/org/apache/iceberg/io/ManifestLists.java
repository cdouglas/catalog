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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A snapshot's inline manifest list as a checkpoint writes it: whole, or as a change of its
 * parent's list (SNAPSHOT_PAGING.md, "Lists: stable ordinals and deltas"). A change removes the
 * base's manifests at some positions and inserts others at positions of the new list; what it
 * keeps is the longest run of the base's manifests that the new list holds in the base's order, so
 * a list that reorders manifests is still a change. A function of the two lists alone, so every
 * replica encodes a list alike (I11).
 */
final class ManifestLists {
  private ManifestLists() {}

  /** A whole list starts every chain of this many lists, so that decoding one never walks far. */
  static final int CHAIN = 64;

  /** The change from a base list to a list. */
  static final class Change {
    /** The base's positions the list drops, ascending. */
    final int[] removed;
    /** The list's positions that the base does not supply, ascending. */
    final int[] added;

    private Change(int[] removed, int[] added) {
      this.removed = removed;
      this.added = added;
    }

    /** Its size on the wire, in numbers, against a whole list's {@code size}. */
    int cost() {
      return 1 + removed.length + 2 * added.length;
    }
  }

  /** The change that makes {@code list} of {@code base}. */
  static Change diff(List<String> base, List<String> list) {
    Map<String, Integer> at = new HashMap<>(2 * base.size());
    for (int i = 0; i < base.size(); i++) {
      at.put(base.get(i), i);
    }
    int n = list.size();
    int[] inBase = new int[n];
    for (int i = 0; i < n; i++) {
      Integer position = at.get(list.get(i));
      inBase[i] = position != null ? position : -1;
    }
    // The longest subsequence of the list whose base positions strictly increase (patience
    // sorting): ends[k] is the list position ending the best run of length k + 1 found so far
    int[] ends = new int[n];
    int[] previous = new int[n];
    int length = 0;
    for (int i = 0; i < n; i++) {
      if (inBase[i] < 0) {
        continue;
      }
      int low = 0;
      int high = length;
      while (low < high) {
        int mid = (low + high) >>> 1;
        if (inBase[ends[mid]] < inBase[i]) {
          low = mid + 1;
        } else {
          high = mid;
        }
      }
      previous[i] = low > 0 ? ends[low - 1] : -1;
      ends[low] = i;
      if (low == length) {
        length++;
      }
    }
    boolean[] keptInList = new boolean[n];
    boolean[] keptInBase = new boolean[base.size()];
    for (int i = length > 0 ? ends[length - 1] : -1; i >= 0; i = previous[i]) {
      keptInList[i] = true;
      keptInBase[inBase[i]] = true;
    }
    int[] removed = new int[base.size()];
    int removedCount = 0;
    for (int i = 0; i < base.size(); i++) {
      if (!keptInBase[i]) {
        removed[removedCount++] = i;
      }
    }
    int[] added = new int[n];
    int addedCount = 0;
    for (int i = 0; i < n; i++) {
      if (!keptInList[i]) {
        added[addedCount++] = i;
      }
    }
    return new Change(Arrays.copyOf(removed, removedCount), Arrays.copyOf(added, addedCount));
  }

  /**
   * The list a change makes of {@code base}: without the manifests at {@code removed} (ascending
   * positions in {@code base}), then with {@code addedPaths} inserted at {@code added} (ascending
   * positions in the list).
   */
  static List<String> apply(List<String> base, int[] removed, int[] added, List<String> addedPaths) {
    int size = base.size() - removed.length + added.length;
    List<String> list = new ArrayList<>(size);
    int nextRemoved = 0;
    int nextAdded = 0;
    int fromBase = 0;
    // One pass: each position takes the next added manifest if the change puts one there, else
    // the base's next manifest the change keeps
    for (int position = 0; position < size; position++) {
      if (nextAdded < added.length && added[nextAdded] == position) {
        list.add(addedPaths.get(nextAdded++));
        continue;
      }
      while (nextRemoved < removed.length && removed[nextRemoved] == fromBase) {
        nextRemoved++;
        fromBase++;
      }
      list.add(base.get(fromBase++));
    }
    return list;
  }
}
