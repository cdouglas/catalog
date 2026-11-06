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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;

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
    Map<String, Integer> ids = new HashMap<>();
    int[] baseIds = ids(base, ids);
    int[] listIds = ids(list, ids);
    return diff(baseIds, listIds, new int[ids.size()]);
  }

  private static int[] ids(List<String> paths, Map<String, Integer> ids) {
    int[] out = new int[paths.size()];
    for (int i = 0; i < out.length; i++) {
      out[i] = ids.computeIfAbsent(paths.get(i), path -> ids.size());
    }
    return out;
  }

  /**
   * The change that makes {@code list} of {@code base}, each a list of manifests' ordinals. {@code
   * at} is scratch indexed by ordinal, larger than any of the lists', all zeros; it is left so.
   */
  static Change diff(int[] base, int[] list, int[] at) {
    // A fast append's list: its new manifests, then the base's whole list
    int extra = list.length - base.length;
    if (extra >= 0
        && Arrays.equals(list, extra, list.length, base, 0, base.length)
        && distinct(list, extra, at)) {
      int[] added = new int[extra];
      for (int i = 0; i < extra; i++) {
        added[i] = i;
      }
      return new Change(new int[0], added);
    }
    // at[ordinal] is the ordinal's position in the base, plus one
    for (int i = 0; i < base.length; i++) {
      at[base[i]] = i + 1;
    }
    int n = list.length;
    int[] inBase = new int[n];
    for (int i = 0; i < n; i++) {
      inBase[i] = at[list[i]] - 1;
    }
    for (int ordinal : base) {
      at[ordinal] = 0;
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
      // A list that keeps the base's order extends the longest run at each step
      if (length > 0 && inBase[ends[length - 1]] < inBase[i]) {
        low = length;
      } else {
        while (low < high) {
          int mid = (low + high) >>> 1;
          if (inBase[ends[mid]] < inBase[i]) {
            low = mid + 1;
          } else {
            high = mid;
          }
        }
      }
      previous[i] = low > 0 ? ends[low - 1] : -1;
      ends[low] = i;
      if (low == length) {
        length++;
      }
    }
    boolean[] keptInList = new boolean[n];
    boolean[] keptInBase = new boolean[base.length];
    for (int i = length > 0 ? ends[length - 1] : -1; i >= 0; i = previous[i]) {
      keptInList[i] = true;
      keptInBase[inBase[i]] = true;
    }
    int[] removed = new int[base.length];
    int removedCount = 0;
    for (int i = 0; i < base.length; i++) {
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
   * Whether {@code list}'s first {@code count} ordinals are distinct from each other and from the
   * rest of it, the base: else the general diff decides what the base keeps. {@code at} is left
   * zero.
   */
  private static boolean distinct(int[] list, int count, int[] at) {
    if (count == 0) {
      return true;
    }
    boolean distinct = true;
    for (int i = 0; i < list.length && distinct; i++) {
      if (at[list[i]] != 0) {
        distinct = false;
      }
      at[list[i]] = 1;
    }
    for (int ordinal : list) {
      at[ordinal] = 0;
    }
    return distinct;
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

  /**
   * Whether {@code removed} and {@code added} are a change of a list of {@code baseSize}: each
   * strictly ascending, the removed positions in the base and the added ones in the list made.
   */
  static boolean isChange(int baseSize, int[] removed, int[] added) {
    return ascendingBelow(removed, baseSize)
        && ascendingBelow(added, baseSize - removed.length + added.length);
  }

  private static boolean ascendingBelow(int[] positions, int bound) {
    int previous = -1;
    for (int position : positions) {
      if (position <= previous || position >= bound) {
        return false;
      }
      previous = position;
    }
    return true;
  }

  /**
   * A list as the state holds it, immutable: {@link CatalogState.Builder#setSnapshotManifests}
   * keeps it without a copy.
   */
  abstract static class Stored extends AbstractList<String> implements RandomAccess {}

  /** A whole list of {@code paths}, which it keeps. */
  static List<String> whole(String[] paths) {
    return new Whole(paths);
  }

  /**
   * The list a change makes of {@code base} (as {@link #apply}), made on first use: a checkpoint
   * decodes its lists without making them, and replay and loads make the few they read.
   */
  static List<String> changed(
      List<String> base, int[] removed, int[] added, List<String> addedPaths) {
    return new Changed(base, removed, added, addedPaths);
  }

  private static final class Whole extends Stored {
    private final String[] paths;

    private Whole(String[] paths) {
      this.paths = paths;
    }

    @Override
    public String get(int index) {
      return paths[index];
    }

    @Override
    public int size() {
      return paths.length;
    }
  }

  private static final class Changed extends Stored {
    private final int size;
    // the change, until the list is made; then null, so that the base can be collected
    private List<String> base;
    private int[] removed;
    private int[] added;
    private List<String> addedPaths;
    private volatile String[] paths;

    private Changed(List<String> base, int[] removed, int[] added, List<String> addedPaths) {
      this.size = base.size() - removed.length + added.length;
      this.base = base;
      this.removed = removed;
      this.added = added;
      this.addedPaths = addedPaths;
    }

    private String[] paths() {
      String[] made = paths;
      if (made == null) {
        synchronized (this) {
          made = paths;
          if (made == null) {
            made = apply(base, removed, added, addedPaths).toArray(new String[0]);
            this.paths = made;
            this.base = null;
            this.removed = null;
            this.added = null;
            this.addedPaths = null;
          }
        }
      }
      return made;
    }

    @Override
    public String get(int index) {
      return paths()[index];
    }

    @Override
    public int size() {
      return size;
    }
  }
}
