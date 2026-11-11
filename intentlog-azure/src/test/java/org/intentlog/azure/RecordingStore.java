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
package org.intentlog.azure;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.intentlog.azure.InMemoryAppendBlobStore.Appended;
import org.intentlog.azure.InMemoryAppendBlobStore.Fault;
import org.intentlog.azure.InMemoryAppendBlobStore.Interceptor;
import org.intentlog.azure.InMemoryAppendBlobStore.Op;

/**
 * Any store, its requests faulted as {@link InMemoryAppendBlobStore}'s are and what landed
 * recorded: for fuzzing the log against a real account. A fault before a request keeps it from
 * being sent; a lost reply or a throttle after one is reported once it landed; a delayed append is
 * held and sent at {@link #deliverDelayed}, under its conditions; a short read of a segment drops
 * the last block this store saw land in it. What the inner store fails on its own (a timeout, a
 * reset) is not recorded, so a run that meets one may report a landing as missing.
 */
final class RecordingStore implements ObservedStore {

  private record Delayed(String name, byte[] block, long maxSize, long appendPosition) {}

  private final AppendBlobStore inner;
  private final String prefix;
  private final List<Appended> appends = new ArrayList<>();
  private final Map<String, List<byte[]>> versions = new LinkedHashMap<>();
  private final List<Delayed> delayed = new ArrayList<>();
  private volatile Interceptor interceptor = (op, name) -> Fault.NONE;

  /** {@code inner}, whose blobs under {@code prefix} this store lists as its own. */
  RecordingStore(AppendBlobStore inner, String prefix) {
    this.inner = inner;
    this.prefix = prefix;
  }

  @Override
  public void intercept(Interceptor hooks) {
    this.interceptor = hooks != null ? hooks : (op, name) -> Fault.NONE;
  }

  /** The fault this request meets; throws for one that keeps it from being sent. */
  private Fault before(Op op, String name) {
    Fault fault = interceptor.before(op, name);
    boolean write = op != Op.READ && op != Op.PROPERTIES && op != Op.LIST;
    switch (fault) {
      case NONE:
        return fault;
      case SHORT_READ:
      case MISSING:
        if (op == Op.READ) {
          return fault;
        }
        break;
      case DELAYED:
        if (op == Op.APPEND) {
          return fault;
        }
        break;
      case REFUSED:
        if (write) {
          throw new Refused("A " + op + " of " + name + " refused (injected)", null);
        }
        break;
      case THROTTLED:
        if (write) {
          throw new Throttled("A " + op + " of " + name + " throttled (injected)", null);
        }
        break;
      case LANDED_THROTTLED:
      case LOST_REPLY:
        if (write) {
          return fault;
        }
        break;
      default:
        break;
    }
    throw new UncheckedIOException(new IOException("A " + op + " of " + name + " failed (injected)"));
  }

  private static void after(Fault fault, Op op, String name) {
    if (fault == Fault.LOST_REPLY) {
      throw new UncheckedIOException(
          new IOException("The reply to a " + op + " of " + name + " was lost (injected)"));
    }
    if (fault == Fault.LANDED_THROTTLED) {
      throw new Throttled("A " + op + " of " + name + " landed, then throttled (injected)", null);
    }
  }

  private synchronized void version(String name, byte[] bytes) {
    versions.computeIfAbsent(name, k -> new ArrayList<>()).add(bytes.clone());
  }

  @Override
  public boolean createAppendBlob(String name, Map<String, String> metadata) {
    Fault fault = before(Op.CREATE_APPEND, name);
    boolean created = inner.createAppendBlob(name, metadata);
    if (created) {
      version(name, new byte[0]);
      after(fault, Op.CREATE_APPEND, name);
    }
    return created;
  }

  @Override
  public long appendBlock(String name, byte[] block, long maxSize, long appendPosition) {
    Fault fault = before(Op.APPEND, name);
    if (fault == Fault.DELAYED) {
      synchronized (this) {
        delayed.add(new Delayed(name, block.clone(), maxSize, appendPosition));
      }
      throw new UncheckedIOException(
          new IOException("An append to " + name + " timed out (injected)"));
    }
    long offset = inner.appendBlock(name, block, maxSize, appendPosition);
    synchronized (this) {
      appends.add(new Appended(name, offset, block.clone()));
    }
    after(fault, Op.APPEND, name);
    return offset;
  }

  @Override
  public int deliverDelayed() {
    List<Delayed> pending;
    synchronized (this) {
      pending = new ArrayList<>(delayed);
      delayed.clear();
    }
    int landed = 0;
    for (Delayed d : pending) {
      try {
        long offset = inner.appendBlock(d.name(), d.block(), d.maxSize(), d.appendPosition());
        synchronized (this) {
          appends.add(new Appended(d.name(), offset, d.block()));
        }
        landed++;
      } catch (Full | PositionMismatch | NotFound e) {
        // it failed its conditions, and nobody hears of it
      }
    }
    return landed;
  }

  @Override
  public Blob read(String name, long from) {
    Fault fault = before(Op.READ, name);
    if (fault == Fault.MISSING) {
      return null;
    }
    Blob b = inner.read(name, from);
    if (fault != Fault.SHORT_READ || b == null) {
      return b;
    }
    // drop the last block, if it is the last this store saw land and the read keeps its start
    List<Long> ends = new ArrayList<>();
    synchronized (this) {
      for (Appended a : appends) {
        if (a.name().equals(name)) {
          ends.add(a.offset() + a.block().length);
        }
      }
    }
    ends.sort(Long::compare);
    if (ends.isEmpty() || ends.get(ends.size() - 1) != b.length()) {
      return b;
    }
    long cut = ends.size() == 1 ? 0 : ends.get(ends.size() - 2);
    if (cut < from) {
      return b;
    }
    return new Blob(Arrays.copyOf(b.bytes(), (int) (cut - from)), cut, b.blocks() - 1,
        b.metadata(), b.etag());
  }

  @Override
  public Props properties(String name) {
    before(Op.PROPERTIES, name);
    return inner.properties(name);
  }

  @Override
  public boolean putIfAbsent(String name, byte[] bytes) {
    Fault fault = before(Op.PUT_IF_ABSENT, name);
    boolean put = inner.putIfAbsent(name, bytes);
    if (put) {
      version(name, bytes);
      after(fault, Op.PUT_IF_ABSENT, name);
    }
    return put;
  }

  @Override
  public void put(String name, byte[] bytes) {
    Fault fault = before(Op.PUT, name);
    inner.put(name, bytes);
    version(name, bytes);
    after(fault, Op.PUT, name);
  }

  @Override
  public String putIfMatch(String name, byte[] bytes, String etag) {
    Fault fault = before(Op.PUT_IF_MATCH, name);
    String tag = inner.putIfMatch(name, bytes, etag);
    if (tag != null) {
      version(name, bytes);
      after(fault, Op.PUT_IF_MATCH, name);
    }
    return tag;
  }

  @Override
  public void delete(String name) {
    Fault fault = before(Op.DELETE, name);
    inner.delete(name);
    after(fault, Op.DELETE, name);
  }

  @Override
  public List<Entry> list(String listPrefix) {
    before(Op.LIST, null);
    return inner.list(listPrefix);
  }

  @Override
  public String location(String name) {
    return inner.location(name);
  }

  @Override
  public int maxBlocks() {
    return inner.maxBlocks();
  }

  @Override
  public long maxBlockBytes() {
    return inner.maxBlockBytes();
  }

  @Override
  public synchronized List<Appended> appends() {
    return List.copyOf(appends);
  }

  @Override
  public synchronized Map<String, List<byte[]>> versions() {
    Map<String, List<byte[]>> copy = new LinkedHashMap<>();
    versions.forEach((k, v) -> copy.put(k, List.copyOf(v)));
    return copy;
  }

  @Override
  public byte[] peek(String name) {
    Blob b = inner.read(name, 0);
    return b == null ? null : b.bytes();
  }

  @Override
  public List<String> names() {
    List<String> names = new ArrayList<>();
    inner.list(prefix).forEach(e -> names.add(e.name()));
    return names;
  }
}
