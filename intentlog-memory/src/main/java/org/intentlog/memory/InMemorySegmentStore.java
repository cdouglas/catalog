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
package org.intentlog.memory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.intentlog.segments.SegmentStore;

/**
 * Segments in memory, as Azure Append Blobs on an account with a hierarchical namespace serve a
 * segmented log's requests: blind appends of one block each, a block limit and a size limit, and
 * no seal or lease.
 *
 * <p>Test hooks ({@link Interceptor}) see every request before it is served: a write can fail
 * before it lands, be refused, be throttled before or after it lands, lose its reply, or (an
 * append) time out still in flight and land later ({@link #deliverDelayed}); a read can fail, be
 * served without the segment's last block, as a reader that lagged would see it, or find the
 * segment missing. The store keeps every append that landed, deleted segments' included, for
 * oracles ({@link #appends}).
 */
public final class InMemorySegmentStore implements SegmentStore {

  /** A request, as the hooks see it. */
  public enum Op {
    CREATE,
    APPEND,
    READ,
    DELETE,
    LIST
  }

  /** What happens to a request. Faults that do not apply to it act as {@link #FAIL_BEFORE}. */
  public enum Fault {
    NONE,
    /** It fails without effect (an exception of unknown outcome). */
    FAIL_BEFORE,
    /** The store refuses it; nothing lands ({@link Refused}). */
    REFUSED,
    /** The store is busy; nothing lands ({@link Throttled}). */
    THROTTLED,
    /** It lands; the store then reports itself busy ({@link Throttled}). */
    LANDED_THROTTLED,
    /** It lands; its reply is lost. */
    LOST_REPLY,
    /** An append times out still in flight: it lands later, at {@link #deliverDelayed}. */
    DELAYED,
    /** A read is served without the segment's last block, as one that lagged. */
    SHORT_READ,
    /** A read finds no segment, as if it had been deleted; another request fails. */
    MISSING
  }

  /** Test hooks, called on the requesting thread before the store serves the request. */
  public interface Interceptor {
    Fault before(Op op, String name);
  }

  /** An append that landed: the segment, where, and the block. */
  public record Landed(String name, long offset, byte[] block) {}

  private static final class Stored {
    byte[] bytes = new byte[0];
    int length = 0;
    final List<Long> blockEnds = new ArrayList<>();

    void append(byte[] block) {
      if (length + block.length > bytes.length) {
        bytes = Arrays.copyOf(bytes, Math.max(length + block.length, 2 * bytes.length));
      }
      System.arraycopy(block, 0, bytes, length, block.length);
      length += block.length;
      blockEnds.add((long) length);
    }
  }

  private record Delayed(String name, byte[] block) {}

  private final Map<String, Stored> segments = new TreeMap<>();
  private final List<Landed> landed = new ArrayList<>();
  private final List<Delayed> delayed = new ArrayList<>();
  private final long maxBlocks;
  private final long maxBlockBytes;
  private final long maxSegmentBytes;
  private Interceptor interceptor = (op, name) -> Fault.NONE;

  /** A store whose segments hold at most {@code maxBlocks} blocks and {@code maxSegmentBytes}. */
  public InMemorySegmentStore(long maxBlocks, long maxBlockBytes, long maxSegmentBytes) {
    if (maxBlocks < 1 || maxBlockBytes < 1 || maxSegmentBytes < 1) {
      throw new IllegalArgumentException("A store's limits are positive");
    }
    this.maxBlocks = maxBlocks;
    this.maxBlockBytes = maxBlockBytes;
    this.maxSegmentBytes = maxSegmentBytes;
  }

  /** A store with Azure's limits: 50,000 blocks of up to 4 MiB. */
  public InMemorySegmentStore() {
    this(50_000, 4L << 20, 50_000L * (4L << 20));
  }

  /** Installs test hooks (null: none). */
  public synchronized InMemorySegmentStore intercept(Interceptor hooks) {
    this.interceptor = hooks != null ? hooks : (op, name) -> Fault.NONE;
    return this;
  }

  @Override
  public long maxBlocks() {
    return maxBlocks;
  }

  @Override
  public long maxBlockBytes() {
    return maxBlockBytes;
  }

  @Override
  public long maxSegmentBytes() {
    return maxSegmentBytes;
  }

  @Override
  public synchronized boolean create(String name) {
    Fault fault = before(Op.CREATE, name);
    if (segments.containsKey(name)) {
      after(fault, Op.CREATE, name);
      return false;
    }
    segments.put(name, new Stored());
    after(fault, Op.CREATE, name);
    return true;
  }

  @Override
  public synchronized Appended append(String name, byte[] frame) {
    Fault fault = before(Op.APPEND, name);
    Stored s = segments.get(name);
    if (s == null) {
      throw new NotFound("No segment " + name);
    }
    if (frame.length > maxBlockBytes) {
      throw new Refused("A block of " + frame.length + " bytes is larger than " + maxBlockBytes,
          null);
    }
    if (fault == Fault.DELAYED) {
      delayed.add(new Delayed(name, frame.clone()));
      throw new UncheckedIOException(new IOException("An append to " + name + " timed out"));
    }
    Appended a = land(s, name, frame);
    after(fault, Op.APPEND, name);
    return a;
  }

  private Appended land(Stored s, String name, byte[] block) {
    if (s.blockEnds.size() >= maxBlocks) {
      throw new Full("BlockCountExceedsLimit: " + name + " holds " + maxBlocks + " blocks");
    }
    if (s.length + (long) block.length > maxSegmentBytes) {
      throw new Full(name + " holds " + s.length + " bytes; " + maxSegmentBytes + " at most");
    }
    long offset = s.length;
    s.append(block);
    landed.add(new Landed(name, offset, block.clone()));
    return new Appended(offset, s.blockEnds.size());
  }

  /**
   * Lands every append delayed so far, in the order they were sent, each if its segment exists and
   * has room then. Returns how many landed.
   */
  public synchronized int deliverDelayed() {
    List<Delayed> pending = new ArrayList<>(delayed);
    delayed.clear();
    int n = 0;
    for (Delayed d : pending) {
      Stored s = segments.get(d.name());
      if (s == null) {
        continue;
      }
      try {
        land(s, d.name(), d.block());
        n++;
      } catch (Full e) {
        // nobody hears of it
      }
    }
    return n;
  }

  public synchronized int delayedCount() {
    return delayed.size();
  }

  @Override
  public synchronized Segment read(String name, long from) {
    Fault fault = before(Op.READ, name);
    Stored s = segments.get(name);
    if (s == null || fault == Fault.MISSING) {
      return null;
    }
    long end = s.length;
    int blocks = s.blockEnds.size();
    if (fault == Fault.SHORT_READ && blocks > 0
        && (blocks == 1 ? 0 : s.blockEnds.get(blocks - 2)) >= from) {
      blocks--;
      end = blocks == 0 ? 0 : s.blockEnds.get(blocks - 1);
    }
    byte[] bytes = from >= end ? new byte[0] : Arrays.copyOfRange(s.bytes, (int) from, (int) end);
    return new Segment(bytes, end, blocks);
  }

  @Override
  public synchronized void delete(String name) {
    Fault fault = before(Op.DELETE, name);
    segments.remove(name);
    after(fault, Op.DELETE, name);
  }

  @Override
  public synchronized List<String> list(String prefix) {
    before(Op.LIST, null);
    List<String> names = new ArrayList<>();
    for (String name : segments.keySet()) {
      if (name.startsWith(prefix)) {
        names.add(name);
      }
    }
    return names;
  }

  @Override
  public String location(String name) {
    return "memory:/" + name;
  }

  // ============================================================
  // Faults
  // ============================================================

  private Fault before(Op op, String name) {
    Fault fault = interceptor.before(op, name);
    boolean write = op != Op.READ && op != Op.LIST;
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
          throw new Refused("The store refused a " + op + " of " + name, null);
        }
        break;
      case THROTTLED:
        if (write) {
          throw new Throttled("The store throttled a " + op + " of " + name, null);
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
    throw new UncheckedIOException(new IOException("A " + op + " of " + name + " failed"));
  }

  private static void after(Fault fault, Op op, String name) {
    if (fault == Fault.LOST_REPLY) {
      throw new UncheckedIOException(
          new IOException("The reply to a " + op + " of " + name + " was lost"));
    }
    if (fault == Fault.LANDED_THROTTLED) {
      throw new Throttled("The store throttled a " + op + " of " + name + " that landed", null);
    }
  }

  // ============================================================
  // What oracles see
  // ============================================================

  /** Every append that landed, in order, deleted segments' included. */
  public synchronized List<Landed> appends() {
    return List.copyOf(landed);
  }

  /** The names of the segments that exist. */
  public synchronized List<String> names() {
    return List.copyOf(segments.keySet());
  }

  /** A segment's bytes, with no hooks; null if it does not exist. */
  public synchronized byte[] peek(String name) {
    Stored s = segments.get(name);
    return s == null ? null : Arrays.copyOf(s.bytes, s.length);
  }
}
