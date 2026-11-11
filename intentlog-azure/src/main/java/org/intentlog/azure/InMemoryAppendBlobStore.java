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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.LongSupplier;

/**
 * A blob container in memory, as an account with a hierarchical namespace serves the requests
 * {@link AppendBlobLog} makes: Append Block's size and position conditions, the block limit,
 * If-None-Match and If-Match puts. Like that account, and unlike Azurite, it has no Seal and no
 * leases.
 *
 * <p>Test hooks ({@link Interceptor}) see every request before it is served: a write can fail
 * before it lands, be refused, be throttled before or after it lands, lose its reply, or (an
 * append) time out still in flight and land later ({@link #deliverDelayed}), under the conditions
 * it was sent with; a read can fail, or be served short of the blob's last block, as a reader that
 * lagged would see it. The store keeps every append that landed and every version of every blob
 * put, deleted ones included, for oracles ({@link #appends}, {@link #versions}), unless told not
 * to ({@link #recordHistory}, for a benchmark's long runs). Its contents can be saved and loaded
 * again ({@link #writeTo}, {@link #readFrom}), as a benchmark's aged packs are.
 */
public final class InMemoryAppendBlobStore implements AppendBlobStore {

  /** A request, as the hooks see it. */
  public enum Op {
    CREATE_APPEND,
    APPEND,
    READ,
    PROPERTIES,
    PUT_IF_ABSENT,
    PUT,
    PUT_IF_MATCH,
    DELETE,
    LIST
  }

  /** What happens to a request. Faults that do not apply to it act as {@link #FAIL_BEFORE}. */
  public enum Fault {
    NONE,
    /** It fails without effect (an exception of unknown outcome). */
    FAIL_BEFORE,
    /** The service refuses it; nothing lands ({@link Refused}). */
    REFUSED,
    /** The service is busy; nothing lands ({@link Throttled}). */
    THROTTLED,
    /** It lands; the service then reports itself busy ({@link Throttled}). */
    LANDED_THROTTLED,
    /** It lands; its reply is lost. */
    LOST_REPLY,
    /** An append times out still in flight: it lands later, at {@link #deliverDelayed}. */
    DELAYED,
    /** A read of an Append Blob is served without its last block, as one that lagged. */
    SHORT_READ,
    /** A read finds no blob, as if it had been deleted; another request fails. */
    MISSING
  }

  /** Test hooks, called on the requesting thread before the store serves the request. */
  public interface Interceptor {
    Fault before(Op op, String name);
  }

  /** An append that landed: the blob, where, and the block. */
  public record Appended(String name, long offset, byte[] block) {}

  private static final class Stored {
    final boolean append;
    // the blob is bytes[0, length): an Append Blob's buffer grows by doubling, so that appends
    // to a segment of many blocks take linear time in all
    byte[] bytes = new byte[0];
    int length = 0;
    final List<Long> blockEnds = new ArrayList<>();
    final Map<String, String> metadata;
    String etag;
    Instant modified;

    Stored(boolean append, Map<String, String> metadata) {
      this.append = append;
      this.metadata = Map.copyOf(metadata);
    }

    long length() {
      return length;
    }

    void append(byte[] block) {
      if (length + block.length > bytes.length) {
        bytes = Arrays.copyOf(bytes, Math.max(length + block.length, 2 * bytes.length));
      }
      System.arraycopy(block, 0, bytes, length, block.length);
      length += block.length;
    }

    byte[] copy(int from, int to) {
      return Arrays.copyOfRange(bytes, from, to);
    }
  }

  private record Delayed(String name, byte[] block, long maxSize, long appendPosition) {}

  private final Map<String, Stored> blobs = new TreeMap<>();
  private final List<Appended> appends = new ArrayList<>();
  private final Map<String, List<byte[]>> versions = new LinkedHashMap<>();
  private final List<Delayed> delayed = new ArrayList<>();
  private final int maxBlocks;
  private final LongSupplier clockMs;
  private long etags = 0;
  private boolean history = true;
  private Interceptor interceptor = (op, name) -> Fault.NONE;

  /** A store whose Append Blobs hold at most {@code maxBlocks} blocks, on the system clock. */
  public InMemoryAppendBlobStore(int maxBlocks) {
    this(maxBlocks, System::currentTimeMillis);
  }

  public InMemoryAppendBlobStore(int maxBlocks, LongSupplier clockMs) {
    this.maxBlocks = maxBlocks;
    this.clockMs = clockMs;
  }

  public InMemoryAppendBlobStore() {
    this(50_000);
  }

  /** Replaces the test hooks. */
  public synchronized InMemoryAppendBlobStore intercept(Interceptor hooks) {
    this.interceptor = hooks != null ? hooks : (op, name) -> Fault.NONE;
    return this;
  }

  /**
   * Whether the store keeps every append and every version put ({@link #appends}, {@link
   * #versions}; true at first): a benchmark's long runs turn it off, and the oracles then see
   * nothing.
   */
  public synchronized InMemoryAppendBlobStore recordHistory(boolean record) {
    this.history = record;
    if (!record) {
      appends.clear();
      versions.clear();
    }
    return this;
  }

  @Override
  public int maxBlocks() {
    return maxBlocks;
  }

  // ============================================================
  // Writes
  // ============================================================

  @Override
  public synchronized boolean createAppendBlob(String name, Map<String, String> metadata) {
    Fault fault = before(Op.CREATE_APPEND, name);
    if (blobs.containsKey(name)) {
      return false;
    }
    Stored s = new Stored(true, metadata);
    touch(s);
    blobs.put(name, s);
    if (history) {
      versions.computeIfAbsent(name, k -> new ArrayList<>()).add(new byte[0]);
    }
    after(fault, Op.CREATE_APPEND, name);
    return true;
  }

  @Override
  public synchronized long appendBlock(String name, byte[] block, long maxSize, long appendPosition) {
    Fault fault = before(Op.APPEND, name);
    Stored s = blobs.get(name);
    if (s == null || !s.append) {
      throw new NotFound("No Append Blob " + name);
    }
    if (fault == Fault.DELAYED) {
      delayed.add(new Delayed(name, block.clone(), maxSize, appendPosition));
      throw new UncheckedIOException(new IOException("An append to " + name + " timed out"));
    }
    long offset = land(s, name, block, maxSize, appendPosition);
    after(fault, Op.APPEND, name);
    return offset;
  }

  private long land(Stored s, String name, byte[] block, long maxSize, long appendPosition) {
    if (s.blockEnds.size() >= maxBlocks) {
      throw new Full("BlockCountExceedsLimit: " + name + " holds " + maxBlocks + " blocks");
    }
    if (appendPosition >= 0 && s.length() != appendPosition) {
      throw new PositionMismatch("AppendPositionConditionNotMet: " + name + " is " + s.length()
          + " bytes, not " + appendPosition);
    }
    if (maxSize > 0 && s.length() + block.length > maxSize) {
      throw new Full("MaxBlobSizeConditionNotMet: " + name + " is " + s.length() + " bytes");
    }
    long offset = s.length();
    s.append(block);
    s.blockEnds.add(s.length());
    touch(s);
    if (history) {
      appends.add(new Appended(name, offset, block.clone()));
    }
    return offset;
  }

  /**
   * Lands every append delayed so far, in the order they were sent, each only if its blob exists
   * and its conditions hold then. Returns how many landed.
   */
  public synchronized int deliverDelayed() {
    List<Delayed> pending = new ArrayList<>(delayed);
    delayed.clear();
    int landed = 0;
    for (Delayed d : pending) {
      Stored s = blobs.get(d.name());
      if (s == null) {
        continue;
      }
      try {
        land(s, d.name(), d.block(), d.maxSize(), d.appendPosition());
        landed++;
      } catch (Full | PositionMismatch e) {
        // it failed its conditions, and nobody hears of it
      }
    }
    return landed;
  }

  public synchronized int delayedCount() {
    return delayed.size();
  }

  @Override
  public synchronized boolean putIfAbsent(String name, byte[] bytes) {
    Fault fault = before(Op.PUT_IF_ABSENT, name);
    if (blobs.containsKey(name)) {
      return false;
    }
    putBlock(name, bytes);
    after(fault, Op.PUT_IF_ABSENT, name);
    return true;
  }

  @Override
  public synchronized void put(String name, byte[] bytes) {
    Fault fault = before(Op.PUT, name);
    putBlock(name, bytes);
    after(fault, Op.PUT, name);
  }

  @Override
  public synchronized String putIfMatch(String name, byte[] bytes, String etag) {
    Fault fault = before(Op.PUT_IF_MATCH, name);
    Stored s = blobs.get(name);
    if (s == null || !s.etag.equals(etag)) {
      return null;
    }
    String tag = putBlock(name, bytes).etag;
    after(fault, Op.PUT_IF_MATCH, name);
    return tag;
  }

  private Stored putBlock(String name, byte[] bytes) {
    Stored s = new Stored(false, Map.of());
    s.bytes = bytes.clone();
    s.length = bytes.length;
    touch(s);
    blobs.put(name, s);
    if (history) {
      versions.computeIfAbsent(name, k -> new ArrayList<>()).add(bytes.clone());
    }
    return s;
  }

  @Override
  public synchronized void delete(String name) {
    Fault fault = before(Op.DELETE, name);
    blobs.remove(name);
    after(fault, Op.DELETE, name);
  }

  private void touch(Stored s) {
    s.etag = "\"" + (++etags) + "\"";
    s.modified = Instant.ofEpochMilli(clockMs.getAsLong());
  }

  // ============================================================
  // Reads
  // ============================================================

  @Override
  public synchronized Blob read(String name, long from) {
    Fault fault = before(Op.READ, name);
    Stored s = blobs.get(name);
    if (s == null || fault == Fault.MISSING) {
      return null;
    }
    long end = s.length();
    int blocks = s.blockEnds.size();
    if (fault == Fault.SHORT_READ && s.append && blocks > 0
        && (blocks == 1 ? 0 : s.blockEnds.get(blocks - 2)) >= from) {
      blocks--;
      end = blocks == 0 ? 0 : s.blockEnds.get(blocks - 1);
    }
    byte[] bytes = from >= end ? new byte[0] : s.copy((int) from, (int) end);
    return new Blob(bytes, end, blocks, s.metadata, s.etag);
  }

  @Override
  public synchronized Props properties(String name) {
    before(Op.PROPERTIES, name);
    Stored s = blobs.get(name);
    return s == null ? null : new Props(s.length(), s.blockEnds.size(), s.metadata, s.modified);
  }

  @Override
  public synchronized List<Entry> list(String prefix) {
    before(Op.LIST, null);
    List<Entry> entries = new ArrayList<>();
    blobs.forEach((name, s) -> {
      if (name.startsWith(prefix)) {
        entries.add(new Entry(name, s.modified));
      }
    });
    return entries;
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

  /** Every append that landed, in order, deleted blobs' included. */
  public synchronized List<Appended> appends() {
    return List.copyOf(appends);
  }

  /** Every version of every block blob ever put (and each Append Blob's creation, empty). */
  public synchronized Map<String, List<byte[]>> versions() {
    Map<String, List<byte[]>> copy = new LinkedHashMap<>();
    versions.forEach((k, v) -> copy.put(k, List.copyOf(v)));
    return copy;
  }

  /** The blob's bytes now, read without hooks; null if it does not exist. */
  public synchronized byte[] peek(String name) {
    Stored s = blobs.get(name);
    return s == null ? null : s.copy(0, s.length);
  }

  /** The names of the blobs that exist now. */
  public synchronized List<String> names() {
    return new ArrayList<>(blobs.keySet());
  }

  /** The bytes the blobs that exist now hold, in all. */
  public synchronized long bytes() {
    long total = 0;
    for (Stored s : blobs.values()) {
      total += s.length;
    }
    return total;
  }

  // ============================================================
  // Saving and loading
  // ============================================================

  private static final int DUMP_VERSION = 1;

  /**
   * Writes the blobs that exist now (each one's kind, metadata, blocks or bytes), without hooks
   * or history, for {@link #readFrom}.
   */
  public synchronized void writeTo(DataOutputStream out) throws IOException {
    out.writeInt(DUMP_VERSION);
    out.writeInt(blobs.size());
    for (Map.Entry<String, Stored> e : blobs.entrySet()) {
      Stored s = e.getValue();
      out.writeUTF(e.getKey());
      out.writeBoolean(s.append);
      out.writeInt(s.metadata.size());
      for (Map.Entry<String, String> m : new TreeMap<>(s.metadata).entrySet()) {
        out.writeUTF(m.getKey());
        out.writeUTF(m.getValue());
      }
      out.writeInt(s.blockEnds.size());
      for (long end : s.blockEnds) {
        out.writeLong(end);
      }
      out.writeInt(s.length);
      out.write(s.bytes, 0, s.length);
    }
  }

  /**
   * Adds the blobs {@link #writeTo} wrote, as they were (an Append Blob's blocks kept), replacing
   * any of the same names: new ETags, modified now, no history.
   */
  public synchronized void readFrom(DataInputStream in) throws IOException {
    int version = in.readInt();
    if (version != DUMP_VERSION) {
      throw new IOException("Not a dump of this store: version " + version);
    }
    int count = in.readInt();
    for (int i = 0; i < count; i++) {
      String name = in.readUTF();
      boolean append = in.readBoolean();
      Map<String, String> metadata = new TreeMap<>();
      int pairs = in.readInt();
      for (int j = 0; j < pairs; j++) {
        metadata.put(in.readUTF(), in.readUTF());
      }
      Stored s = new Stored(append, metadata);
      int blocks = in.readInt();
      for (int j = 0; j < blocks; j++) {
        s.blockEnds.add(in.readLong());
      }
      s.length = in.readInt();
      s.bytes = new byte[s.length];
      in.readFully(s.bytes);
      touch(s);
      blobs.put(name, s);
    }
  }
}
