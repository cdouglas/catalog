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

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * An immutable hash map whose changes ({@link #plus}, {@link #minus}) make a new map sharing all
 * but the path to the changed key: a hash array mapped trie of 32-way nodes, so a change copies a
 * few small nodes however large the map. A replica's state holds a table's manifest pool and
 * manifest lists in these, so a fold that changes a table does not copy them (design.md "Replay
 * State"). Iteration is in hash order. Keys and values are never null.
 */
final class PersistentMap<K, V> extends AbstractMap<K, V> {
  private static final PersistentMap<?, ?> EMPTY = new PersistentMap<>(Bitmap.EMPTY, 0);
  private static final int BITS = 5;
  private static final int MASK = (1 << BITS) - 1;

  private final Node root;
  private final int size;

  private PersistentMap(Node root, int size) {
    this.root = root;
    this.size = size;
  }

  @SuppressWarnings("unchecked")
  static <K, V> PersistentMap<K, V> empty() {
    return (PersistentMap<K, V>) EMPTY;
  }

  /** {@code map} as a persistent map: itself if it is one. */
  @SuppressWarnings("unchecked")
  static <K, V> PersistentMap<K, V> copyOf(Map<K, V> map) {
    if (map instanceof PersistentMap) {
      return (PersistentMap<K, V>) map;
    }
    PersistentMap<K, V> copy = empty();
    for (Map.Entry<K, V> entry : map.entrySet()) {
      copy = copy.plus(entry.getKey(), entry.getValue());
    }
    return copy;
  }

  /** This map with {@code key} mapped to {@code value}; this map if it already was. */
  PersistentMap<K, V> plus(K key, V value) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(value, "value");
    boolean[] added = {false};
    Node next = root.plus(key, value, key.hashCode(), 0, added);
    return next == root ? this : new PersistentMap<>(next, added[0] ? size + 1 : size);
  }

  /** This map without {@code key}; this map if it has none. */
  PersistentMap<K, V> minus(Object key) {
    if (key == null) {
      return this;
    }
    Node next = root.minus(key, key.hashCode(), 0);
    if (next == root) {
      return this;
    }
    return next == null ? empty() : new PersistentMap<>(next, size - 1);
  }

  @Override
  @SuppressWarnings("unchecked")
  public V get(Object key) {
    return key == null ? null : (V) root.find(key, key.hashCode(), 0);
  }

  @Override
  public boolean containsKey(Object key) {
    return get(key) != null;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    return new AbstractSet<>() {
      @Override
      public Iterator<Map.Entry<K, V>> iterator() {
        return new Entries<>(root);
      }

      @Override
      public int size() {
        return size;
      }
    };
  }

  private interface Node {
    Object find(Object key, int hash, int shift);

    Node plus(Object key, Object value, int hash, int shift, boolean[] added);

    /** This node without {@code key}: itself if it has none, null if it would be empty. */
    Node minus(Object key, int hash, int shift);

    /** The number of slots, each a key and value (a null key: the value is a node). */
    int slots();

    Object key(int slot);

    Object value(int slot);
  }

  /** The keys whose hash bits at {@code shift} index a bitmap of 32 slots: entries or nodes. */
  private static final class Bitmap implements Node {
    static final Bitmap EMPTY = new Bitmap(0, new Object[0]);

    private final int bitmap;
    // Per slot in index order: the key and the value, or null and the node below
    private final Object[] array;

    Bitmap(int bitmap, Object[] array) {
      this.bitmap = bitmap;
      this.array = array;
    }

    @Override
    public Object find(Object key, int hash, int shift) {
      int bit = 1 << ((hash >>> shift) & MASK);
      if ((bitmap & bit) == 0) {
        return null;
      }
      int at = 2 * Integer.bitCount(bitmap & (bit - 1));
      Object slotKey = array[at];
      if (slotKey == null) {
        return ((Node) array[at + 1]).find(key, hash, shift + BITS);
      }
      return key.equals(slotKey) ? array[at + 1] : null;
    }

    @Override
    public Node plus(Object key, Object value, int hash, int shift, boolean[] added) {
      int bit = 1 << ((hash >>> shift) & MASK);
      int at = 2 * Integer.bitCount(bitmap & (bit - 1));
      if ((bitmap & bit) == 0) {
        added[0] = true;
        Object[] next = new Object[array.length + 2];
        System.arraycopy(array, 0, next, 0, at);
        next[at] = key;
        next[at + 1] = value;
        System.arraycopy(array, at, next, at + 2, array.length - at);
        return new Bitmap(bitmap | bit, next);
      }
      Object slotKey = array[at];
      Object slotValue = array[at + 1];
      if (slotKey == null) {
        Node below = ((Node) slotValue).plus(key, value, hash, shift + BITS, added);
        return below == slotValue ? this : with(at + 1, null, below);
      }
      if (key.equals(slotKey)) {
        return slotValue == value ? this : with(at + 1, slotKey, value);
      }
      added[0] = true;
      Node below = pair(slotKey, slotValue, slotKey.hashCode(), key, value, hash, shift + BITS);
      return with(at + 1, null, below);
    }

    /** This node with slot {@code at - 1}'s key and value replaced. */
    private Bitmap with(int valueAt, Object key, Object value) {
      Object[] next = array.clone();
      next[valueAt - 1] = key;
      next[valueAt] = value;
      return new Bitmap(bitmap, next);
    }

    @Override
    public Node minus(Object key, int hash, int shift) {
      int bit = 1 << ((hash >>> shift) & MASK);
      if ((bitmap & bit) == 0) {
        return this;
      }
      int at = 2 * Integer.bitCount(bitmap & (bit - 1));
      Object slotKey = array[at];
      if (slotKey == null) {
        Node below = ((Node) array[at + 1]).minus(key, hash, shift + BITS);
        if (below == array[at + 1]) {
          return this;
        }
        if (below != null) {
          return with(at + 1, null, below);
        }
      } else if (!key.equals(slotKey)) {
        return this;
      }
      if (array.length == 2) {
        return null;
      }
      Object[] next = new Object[array.length - 2];
      System.arraycopy(array, 0, next, 0, at);
      System.arraycopy(array, at + 2, next, at, array.length - at - 2);
      return new Bitmap(bitmap & ~bit, next);
    }

    private static Node pair(
        Object key1, Object value1, int hash1, Object key2, Object value2, int hash2, int shift) {
      if (shift >= 32) {
        // Every bit of the two hashes is the same
        return new Collision(hash1, new Object[] {key1, value1, key2, value2});
      }
      int index1 = (hash1 >>> shift) & MASK;
      int index2 = (hash2 >>> shift) & MASK;
      if (index1 == index2) {
        return new Bitmap(1 << index1,
            new Object[] {null, pair(key1, value1, hash1, key2, value2, hash2, shift + BITS)});
      }
      Object[] array = index1 < index2
          ? new Object[] {key1, value1, key2, value2}
          : new Object[] {key2, value2, key1, value1};
      return new Bitmap((1 << index1) | (1 << index2), array);
    }

    @Override
    public int slots() {
      return array.length / 2;
    }

    @Override
    public Object key(int slot) {
      return array[2 * slot];
    }

    @Override
    public Object value(int slot) {
      return array[2 * slot + 1];
    }
  }

  /** Keys whose hashes are the same in every bit. */
  private static final class Collision implements Node {
    private final int hash;
    private final Object[] array;

    Collision(int hash, Object[] array) {
      this.hash = hash;
      this.array = array;
    }

    private int indexOf(Object key) {
      for (int at = 0; at < array.length; at += 2) {
        if (key.equals(array[at])) {
          return at;
        }
      }
      return -1;
    }

    @Override
    public Object find(Object key, int keyHash, int shift) {
      int at = keyHash == hash ? indexOf(key) : -1;
      return at < 0 ? null : array[at + 1];
    }

    @Override
    public Node plus(Object key, Object value, int keyHash, int shift, boolean[] added) {
      int at = indexOf(key);
      if (at >= 0) {
        if (array[at + 1] == value) {
          return this;
        }
        Object[] next = array.clone();
        next[at + 1] = value;
        return new Collision(hash, next);
      }
      added[0] = true;
      Object[] next = Arrays.copyOf(array, array.length + 2);
      next[array.length] = key;
      next[array.length + 1] = value;
      return new Collision(hash, next);
    }

    @Override
    public Node minus(Object key, int keyHash, int shift) {
      int at = keyHash == hash ? indexOf(key) : -1;
      if (at < 0) {
        return this;
      }
      if (array.length == 2) {
        return null;
      }
      Object[] next = new Object[array.length - 2];
      System.arraycopy(array, 0, next, 0, at);
      System.arraycopy(array, at + 2, next, at, array.length - at - 2);
      return new Collision(hash, next);
    }

    @Override
    public int slots() {
      return array.length / 2;
    }

    @Override
    public Object key(int slot) {
      return array[2 * slot];
    }

    @Override
    public Object value(int slot) {
      return array[2 * slot + 1];
    }
  }

  /** The entries of a trie, depth first. */
  private static final class Entries<K, V> implements Iterator<Map.Entry<K, V>> {
    // The trie is at most 7 levels of bitmaps and a collision node
    private final Node[] nodes = new Node[8];
    private final int[] slots = new int[8];
    private int depth;
    private Map.Entry<K, V> next;

    Entries(Node root) {
      nodes[0] = root;
      advance();
    }

    @SuppressWarnings("unchecked")
    private void advance() {
      next = null;
      while (depth >= 0) {
        Node node = nodes[depth];
        int slot = slots[depth];
        if (slot >= node.slots()) {
          depth--;
          continue;
        }
        slots[depth] = slot + 1;
        Object key = node.key(slot);
        if (key == null) {
          depth++;
          nodes[depth] = (Node) node.value(slot);
          slots[depth] = 0;
        } else {
          next = new AbstractMap.SimpleImmutableEntry<>((K) key, (V) node.value(slot));
          return;
        }
      }
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public Map.Entry<K, V> next() {
      if (next == null) {
        throw new NoSuchElementException();
      }
      Map.Entry<K, V> entry = next;
      advance();
      return entry;
    }
  }
}
