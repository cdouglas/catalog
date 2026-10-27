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
import java.util.RandomAccess;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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

    Buffer(Object[] items, int length, boolean keyed) {
      this.items = items;
      this.length = new AtomicInteger(length);
      this.index = keyed ? new ConcurrentHashMap<>() : null;
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

  /** A list of {@code elements}, indexed by {@code key} if it is not null. */
  static <T> Chain<T> of(List<? extends T> elements, ToLongFunction<? super T> key) {
    Object[] items = elements.toArray(new Object[Math.max(MIN_CAPACITY, elements.size())]);
    Buffer buffer = new Buffer(items, elements.size(), key != null);
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
    // Another list extends this buffer past this one: copy this one's elements
    Object[] items = Arrays.copyOf(buffer.items, Math.max(MIN_CAPACITY, (at + 1) * 2));
    items[at] = element;
    Buffer copy = new Buffer(items, at + 1, key != null);
    if (key != null) {
      for (int i = 0; i <= at; i++) {
        @SuppressWarnings("unchecked")
        T item = (T) items[i];
        copy.index.put(key.applyAsLong(item), i);
      }
    }
    return new Chain<>(copy, at + 1, key);
  }

  /** The element whose key is {@code k}, or null; for a keyed list only. */
  T byKey(long k) {
    Integer at = buffer.index.get(k);
    if (at == null || at >= size) {
      return null;
    }
    @SuppressWarnings("unchecked")
    T item = (T) buffer.items[at];
    return item;
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
    @SuppressWarnings("unchecked")
    T item = (T) buffer.items[i];
    return item;
  }

  @Override
  public int size() {
    return size;
  }
}
