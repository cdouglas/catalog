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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.function.ToLongFunction;

/**
 * An immutable list that shares an append-only buffer with the lists it extends, and optionally
 * an index of its elements by a long key: a table's snapshots (by id) and its snapshot log, which
 * replay extends one record at a time. Appending to the longest list of a buffer writes in place
 * (O(1)); appending to a shorter one, a list another append has already extended, copies its
 * elements to a buffer of its own. Lists sharing a buffer are prefixes of one another, so each
 * reads only its own prefix, and a lookup by key finds an element only within it.
 *
 * <p>Thread-safe: a list may be read, or appended to, from any thread. Each slot of a buffer is
 * written once, by the append that claims it, before that append returns the list holding it.
 */
final class Chain<T> extends AbstractList<T> implements RandomAccess {

  private static final int MIN_CAPACITY = 8;

  /** The shared elements; {@code length} counts the slots claimed. */
  private static final class Buffer {
    private volatile Object[] items;
    private final AtomicInteger length;
    // Each element's index by key, if keyed; an element is in a list only below its size
    private final Map<Long, Integer> index;
    // The first elements, made on first use ({@link #lazy}); the lists of a buffer, and the
    // buffers copied from it, share them (every such list holds them all)
    private final Lazy lazy;
    // The buffer this one copied its first parentLength elements from, whose index finds them (a
    // copy indexes only the elements after), and how many copies deep that is
    private final Buffer parent;
    private final int parentLength;
    private final int depth;

    Buffer(Object[] items, int length, boolean keyed, Lazy lazy) {
      this(items, length, keyed, lazy, null, 0);
    }

    Buffer(Object[] items, int length, boolean keyed, Lazy lazy, Buffer parent, int parentLength) {
      this.items = items;
      this.length = new AtomicInteger(length);
      this.index = keyed ? new ConcurrentHashMap<>() : null;
      this.lazy = lazy;
      this.parent = parent;
      this.parentLength = parentLength;
      this.depth = parent != null ? parent.depth + 1 : 0;
    }

    /** The position of the element whose key is {@code k} in this buffer, or null. */
    Integer lookup(long k) {
      Integer at = index.get(k);
      if (at == null && parent != null) {
        at = parent.lookup(k);
        if (at != null && at >= parentLength) {
          at = null; // the parent's, past what this buffer copied
        }
      }
      return at;
    }
  }

  /** Copies deep before a copy indexes every element again, so a lookup stays short. */
  private static final int MAX_DEPTH = 8;

  /** Elements made on first use, each once: their keys, a maker, and what it made. */
  private static final class Lazy {
    private final long[] keys;
    private final IntFunction<?> make;
    private final Object[] made;

    Lazy(long[] keys, IntFunction<?> make) {
      this.keys = keys;
      this.make = make;
      this.made = new Object[keys.length];
    }

    Object get(int i) {
      Object item = made[i];
      if (item == null) {
        synchronized (this) {
          item = made[i];
          if (item == null) {
            item = Objects.requireNonNull(make.apply(i), "made element");
            made[i] = item;
          }
        }
      }
      return item;
    }
  }

  private final Buffer buffer;
  private final int size;
  private final ToLongFunction<? super T> key;

  private Chain(Buffer buffer, int size, ToLongFunction<? super T> key) {
    this.buffer = buffer;
    this.size = size;
    this.key = key;
  }

  /** A list of {@code elements}, unkeyed. */
  static <T> Chain<T> of(List<? extends T> elements) {
    return of(elements, null);
  }

  /**
   * A keyed list of {@code keys.length} elements, the i-th made by {@code make} the first time it
   * is read and kept: keys are known at once, an element only when needed (a table's snapshots,
   * decoded from columns). {@code key} must give each made element its key.
   */
  static <T> Chain<T> lazy(
      long[] keys, IntFunction<? extends T> make, ToLongFunction<? super T> key) {
    Objects.requireNonNull(key, "key");
    Buffer buffer =
        new Buffer(new Object[Math.max(MIN_CAPACITY, keys.length)], keys.length, true,
            new Lazy(keys, make));
    for (int i = 0; i < keys.length; i++) {
      buffer.index.put(keys[i], i);
    }
    return new Chain<>(buffer, keys.length, key);
  }

  /** A list of {@code elements}, indexed by {@code key} if it is not null. */
  static <T> Chain<T> of(List<? extends T> elements, ToLongFunction<? super T> key) {
    Object[] items = elements.toArray(new Object[Math.max(MIN_CAPACITY, elements.size())]);
    Buffer buffer = new Buffer(items, elements.size(), key != null, null);
    if (key != null) {
      for (int i = 0; i < elements.size(); i++) {
        buffer.index.put(key.applyAsLong(elements.get(i)), i);
      }
    }
    return new Chain<>(buffer, elements.size(), key);
  }

  /** This list with {@code element} at the end. */
  Chain<T> append(T element) {
    int at = size;
    if (buffer.length.get() == at && buffer.length.compareAndSet(at, at + 1)) {
      Object[] items = buffer.items;
      if (items.length <= at) {
        items = Arrays.copyOf(items, Math.max(MIN_CAPACITY, at * 2));
        buffer.items = items;
      }
      items[at] = element;
      if (key != null) {
        buffer.index.put(key.applyAsLong(element), at);
      }
      return new Chain<>(buffer, at + 1, key);
    }
    // Another list extends this buffer past this one: copy this one's elements, sharing the lazy
    // ones, which every list of the buffer holds
    Object[] items = Arrays.copyOf(buffer.items, Math.max(MIN_CAPACITY, (at + 1) * 2));
    items[at] = element;
    if (key == null) {
      return new Chain<>(new Buffer(items, at + 1, false, buffer.lazy), at + 1, key);
    }
    Buffer copy;
    if (buffer.depth < MAX_DEPTH) {
      // The first `at` elements are this buffer's, which finds them by its index
      copy = new Buffer(items, at + 1, true, buffer.lazy, buffer, at);
      copy.index.put(key.applyAsLong(element), at);
    } else {
      copy = new Buffer(items, at + 1, true, buffer.lazy);
      for (int i = 0; i <= at; i++) {
        copy.index.put(keyAt(copy, i), i);
      }
    }
    return new Chain<>(copy, at + 1, key);
  }

  /** The element whose key is {@code k}, or null; for a keyed list only. */
  T byKey(long k) {
    Integer at = buffer.lookup(k);
    if (at == null || at >= size) {
      return null;
    }
    return item(buffer, at);
  }

  /** Whether the list holds an element whose key is {@code k}, without making it. */
  boolean containsKey(long k) {
    Integer at = buffer.lookup(k);
    return at != null && at < size;
  }

  /** The key of the element at {@code i}, made or not; for a keyed list only. */
  long keyAt(int i) {
    if (i < 0 || i >= size) {
      throw new IndexOutOfBoundsException("Index " + i + " out of bounds for length " + size);
    }
    return keyAt(buffer, i);
  }

  private long keyAt(Buffer from, int i) {
    if (from.lazy != null && i < from.lazy.keys.length) {
      return from.lazy.keys[i];
    }
    return key.applyAsLong(item(from, i));
  }

  @SuppressWarnings("unchecked")
  private T item(Buffer from, int i) {
    if (from.lazy != null && i < from.lazy.keys.length) {
      return (T) from.lazy.get(i);
    }
    return (T) from.items[i];
  }

  /** The last element, or null if the list is empty. */
  T last() {
    return size > 0 ? get(size - 1) : null;
  }

  @Override
  public T get(int i) {
    if (i < 0 || i >= size) {
      throw new IndexOutOfBoundsException("Index " + i + " out of bounds for length " + size);
    }
    return item(buffer, i);
  }

  @Override
  public int size() {
    return size;
  }
}
