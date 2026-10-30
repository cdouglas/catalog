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

import java.io.Serializable;
import java.util.AbstractList;
import java.util.List;
import java.util.RandomAccess;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The newest entries of a log that replay extends one entry per version, at most a bound each
 * append names: a table's metadata log, which keeps {@code write.metadata.previous-versions-max}
 * entries. Like {@link Chain}, an immutable list that shares an append-only buffer with the lists
 * it extends, so that appending to the newest list writes in place (O(1)) rather than copying the
 * log; each list reads only its own window of the buffer. An append past the buffer's end, or to a
 * list another append has already extended, copies its window to a buffer of its own, with room
 * for {@link #SLACK} times as many, so that a buffer no list reads any more is garbage.
 *
 * <p>Thread-safe: a list may be read, or appended to, from any thread. Each slot of a buffer is
 * written once, by the append that claims it, before that append returns the list holding it.
 */
final class Window<T> extends AbstractList<T> implements RandomAccess, Serializable {

  private static final int MIN_CAPACITY = 8;
  private static final int SLACK = 4;

  /** The shared elements; {@code length} counts the slots claimed. */
  private static final class Buffer {
    private final Object[] items;
    private final AtomicInteger length;

    Buffer(Object[] items, int length) {
      this.items = items;
      this.length = new AtomicInteger(length);
    }
  }

  private final Buffer buffer;
  private final int from;
  private final int to;

  private Window(Buffer buffer, int from, int to) {
    this.buffer = buffer;
    this.from = from;
    this.to = to;
  }

  /** A list of {@code elements}. */
  static <T> Window<T> of(List<? extends T> elements) {
    Object[] items = elements.toArray(new Object[capacity(elements.size())]);
    return new Window<>(new Buffer(items, elements.size()), 0, elements.size());
  }

  /** This list with {@code element} at the end, less its oldest elements beyond {@code bound}. */
  Window<T> append(T element, int bound) {
    int keep = Math.min(to - from, Math.max(1, bound) - 1);
    if (to < buffer.items.length
        && buffer.length.get() == to
        && buffer.length.compareAndSet(to, to + 1)) {
      buffer.items[to] = element;
      return new Window<>(buffer, to - keep, to + 1);
    }
    // The buffer is full, or another list extends it past this one: copy the window
    Object[] items = new Object[capacity(keep + 1)];
    System.arraycopy(buffer.items, to - keep, items, 0, keep);
    items[keep] = element;
    return new Window<>(new Buffer(items, keep + 1), 0, keep + 1);
  }

  private static int capacity(int size) {
    return Math.max(MIN_CAPACITY, SLACK * size);
  }

  @Override
  public T get(int i) {
    if (i < 0 || i >= to - from) {
      throw new IndexOutOfBoundsException("Index " + i + " out of bounds for length " + size());
    }
    @SuppressWarnings("unchecked")
    T item = (T) buffer.items[from + i];
    return item;
  }

  @Override
  public int size() {
    return to - from;
  }

  // A serialized copy (a table shipped to a worker) is a plain list of the elements: the buffer is
  // shared with the lists replay extends, which stay where they are
  private Object writeReplace() {
    return List.copyOf(this);
  }
}
