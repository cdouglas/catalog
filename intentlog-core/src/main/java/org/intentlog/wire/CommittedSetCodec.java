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
package org.intentlog.wire;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.UUID;

/**
 * The {@code CommittedTransactions} message, encoded and decoded whole: the reference that
 * {@code CommittedSet}, which keeps the set incrementally, must match byte for byte. See
 * docs/LOG.md "The committed set and GC" and docs/COMMITTED_PAGING.md "Blocks: the index".
 *
 * <p>The set's ids are cut into blocks on a grid over their timestamps: a node {@code (k, a)} is
 * the aligned span {@code [a * 2^k, (a + 1) * 2^k)} of milliseconds, and the blocks are the
 * maximal nodes that hold at most {@link #BLOCK_IDS} ids, among the nodes that hold any. A
 * millisecond (level 0) is a block however many ids it holds. The blocks are a function of the ids
 * alone. A block lists its entries newest first, each a varint gap in ms from the one before
 * (the first from the node's top, {@code (a + 1) * 2^k - 1}) and the id's 10 random bytes.
 *
 * <p>Wire fields: {@code highest_dropped_timestamp_ms} (3, the horizon H), {@code floor_ms} (4,
 * the floor F, absent when it equals H), {@code pages} (5, references to the ids paged out, none
 * yet) and {@code blocks} (6, oldest first), each a {@code CommittedBlock}. Fields 1 and 2 held the one-blob set of reader
 * version 1.
 */
public final class CommittedSetCodec {

  public static final int COMMITTED_TXNS_HIGHEST_DROPPED_TIMESTAMP_MS = 3;
  public static final int COMMITTED_TXNS_FLOOR_MS = 4;
  // Field 5, pages: references to the ids paged out (COMMITTED_PAGING.md), not yet written
  public static final int COMMITTED_TXNS_BLOCKS = 6;
  public static final int COMMITTED_BLOCK_LEVEL = 1;
  public static final int COMMITTED_BLOCK_INDEX = 2;
  public static final int COMMITTED_BLOCK_PACKED_ENTRIES = 3;

  /** The level of the grid's root: a UUIDv7's timestamp is 48 bits of ms. */
  public static final int MAX_LEVEL = 48;

  /** The random bytes an entry keeps of its id (the version and variant bits are implied). */
  public static final int RANDOM_BYTES = 10;

  /**
   * N, the most ids a block holds above level 0: a constant of the format (2^16, about 720 KB a
   * block). Every writer of a log must use the same N, or two replicas at one position could
   * write different checkpoints (I11). Tests set it smaller ({@code
   * intentlog.committed.block.ids}) so that blocks split and merge in every suite; the setting is
   * read once, for the whole JVM.
   */
  public static final int BLOCK_IDS = Integer.getInteger("intentlog.committed.block.ids", 1 << 16);

  private static final int TAG_HORIZON = (COMMITTED_TXNS_HIGHEST_DROPPED_TIMESTAMP_MS << 3) | 1;
  private static final int TAG_FLOOR = (COMMITTED_TXNS_FLOOR_MS << 3) | 1;
  private static final int TAG_BLOCK = (COMMITTED_TXNS_BLOCKS << 3) | 2;
  private static final int TAG_LEVEL = COMMITTED_BLOCK_LEVEL << 3;
  private static final int TAG_INDEX = COMMITTED_BLOCK_INDEX << 3;
  private static final int TAG_PACKED = (COMMITTED_BLOCK_PACKED_ENTRIES << 3) | 2;

  private CommittedSetCodec() {}

  /** A decoded body: the ids newest first, the horizon, the floor, and the blocks. */
  public static final class Decoded {
    public final List<UUID> ids;
    public final long horizon;
    public final long floor;
    /** Each block, oldest first: {level, index, ids}. */
    public final List<long[]> blocks;

    Decoded(List<UUID> ids, long horizon, long floor, List<long[]> blocks) {
      this.ids = ids;
      this.horizon = horizon;
      this.floor = floor;
      this.blocks = blocks;
    }
  }

  /** Encodes a set of UUIDv7s with no horizon. */
  public static byte[] encode(Collection<UUID> txns) throws IOException {
    return encode(txns, 0L);
  }

  /** Encodes a set of UUIDv7s with horizon {@code highestDroppedTimestampMs}, the floor there. */
  public static byte[] encode(Collection<UUID> txns, long highestDroppedTimestampMs)
      throws IOException {
    return encode(txns, highestDroppedTimestampMs, highestDroppedTimestampMs, BLOCK_IDS);
  }

  /**
   * Encodes a set of UUIDv7s, the horizon and the floor ({@code floorMs >= horizonMs}), cutting
   * blocks of at most {@code blockIds} ids. Empty iff there are no ids and the horizon is 0.
   */
  public static byte[] encode(Collection<UUID> txns, long horizonMs, long floorMs, int blockIds) {
    if (floorMs < horizonMs) {
      throw new IllegalArgumentException("floor " + floorMs + " below horizon " + horizonMs);
    }
    int n = txns.size();
    long[] ts = new long[n];
    byte[][] rand = new byte[n][];
    sortNewestFirst(txns, ts, rand);
    return encodeSorted(ts, rand, n, horizonMs, floorMs, blockIds);
  }

  private static byte[] encodeSorted(
      long[] ts, byte[][] rand, int n, long horizonMs, long floorMs, int blockIds) {
    if (n == 0 && horizonMs == 0L && floorMs == 0L) {
      return new byte[0];
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream(16 + 12 * n);
    if (horizonMs != 0L) {
      out.write(TAG_HORIZON);
      writeFixed64(out, horizonMs);
    }
    if (floorMs != horizonMs) {
      out.write(TAG_FLOOR);
      writeFixed64(out, floorMs);
    }
    List<long[]> blocks = new ArrayList<>();
    partition(ts, MAX_LEVEL, 0L, 0, n, blockIds, blocks);
    for (long[] block : blocks) {
      byte[] message = blockMessage((int) block[0], block[1], ts, rand, (int) block[2],
          (int) block[3]);
      out.write(TAG_BLOCK);
      writeVarint(out, message.length);
      out.write(message, 0, message.length);
    }
    return out.toByteArray();
  }

  /**
   * The blocks of entries {@code [lo, hi)} (sorted newest first, all within node {@code (level,
   * index)}), oldest first, as {@code {level, index, lo, hi}}.
   */
  private static void partition(
      long[] ts, int level, long index, int lo, int hi, int blockIds, List<long[]> out) {
    if (lo >= hi) {
      return;
    }
    if (hi - lo <= blockIds || level == 0) {
      out.add(new long[] {level, index, lo, hi});
      return;
    }
    // The upper child holds the timestamps at or above its start: the first entries
    long mid = (2 * index + 1) << (level - 1);
    int split = lo;
    while (split < hi && ts[split] >= mid) {
      split++;
    }
    partition(ts, level - 1, 2 * index, split, hi, blockIds, out);
    partition(ts, level - 1, 2 * index + 1, lo, split, blockIds, out);
  }

  private static byte[] blockMessage(
      int level, long index, long[] ts, byte[][] rand, int lo, int hi) {
    ByteArrayOutputStream packed = new ByteArrayOutputStream(12 * (hi - lo));
    long prev = top(level, index);
    for (int i = lo; i < hi; i++) {
      writeVarint(packed, prev - ts[i]);
      prev = ts[i];
      packed.write(rand[i], 0, RANDOM_BYTES);
    }
    ByteArrayOutputStream message = new ByteArrayOutputStream(packed.size() + 16);
    if (level != 0) {
      message.write(TAG_LEVEL);
      writeVarint(message, level);
    }
    if (index != 0L) {
      message.write(TAG_INDEX);
      writeVarint(message, index);
    }
    message.write(TAG_PACKED);
    writeVarint(message, packed.size());
    byte[] body = packed.toByteArray();
    message.write(body, 0, body.length);
    return message.toByteArray();
  }

  /** The newest timestamp node {@code (level, index)} spans. */
  public static long top(int level, long index) {
    return ((index + 1) << level) - 1;
  }

  /**
   * {@code inheritedBytes} with {@code addUnconditionally} added (copies of ids it holds are
   * kept once), then every inherited id at or below {@code dropBeforeTimestampMs} dropped (0
   * drops nothing), the horizon raised to the newest dropped and the floor to the horizon. The
   * additions are kept whatever their timestamps, as replay adds only ids it just applied.
   */
  public static byte[] rewrite(
      byte[] inheritedBytes, Collection<UUID> addUnconditionally, long dropBeforeTimestampMs) {
    Decoded inherited = decodeBody(inheritedBytes);
    LinkedHashSet<UUID> kept = new LinkedHashSet<>();
    long horizon = inherited.horizon;
    for (UUID id : inherited.ids) {
      long ts = UuidV7.timestampMs(id);
      if (dropBeforeTimestampMs > 0L && ts <= dropBeforeTimestampMs) {
        horizon = Math.max(horizon, ts);
      } else {
        kept.add(id);
      }
    }
    for (UUID id : addUnconditionally) {
      if (!UuidV7.isV7(id)) {
        throw new IllegalArgumentException("Committed transaction IDs must be UUIDv7; got " + id);
      }
      kept.add(id);
    }
    return encode(kept, horizon, Math.max(inherited.floor, horizon), BLOCK_IDS);
  }

  /** Whether the set holds {@code query}; false for a non-v7 id. Ignores the horizon. */
  public static boolean contains(byte[] body, UUID query) {
    if (body.length == 0 || !UuidV7.isV7(query)) {
      return false;
    }
    return decodeBody(body).ids.contains(query);
  }

  /** The horizon H: 0 when the body is empty or no GC has run. */
  public static long horizon(byte[] body) {
    return decodeBody(body).horizon;
  }

  /** The floor F: the horizon when the field is absent. */
  public static long floor(byte[] body) {
    return decodeBody(body).floor;
  }

  /** The ids, newest first. */
  public static LinkedHashSet<UUID> decode(byte[] body) throws IOException {
    try {
      return new LinkedHashSet<>(decodeBody(body).ids);
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }

  /** Decodes a whole body, checking its blocks: disjoint, oldest first, entries inside. */
  public static Decoded decodeBody(byte[] body) {
    long horizon = 0L;
    Long floor = null;
    List<List<UUID>> blocks = new ArrayList<>();
    List<long[]> nodes = new ArrayList<>();
    long prevTop = -1L;
    int[] at = {0};
    while (at[0] < body.length) {
      int tag = (int) readVarint(body, at, body.length);
      switch (tag) {
        case TAG_HORIZON:
          horizon = readFixed64(body, at);
          break;
        case TAG_FLOOR:
          floor = readFixed64(body, at);
          break;
        case TAG_BLOCK:
          int len = (int) readVarint(body, at, body.length);
          int end = at[0] + len;
          if (len < 0 || end > body.length) {
            throw malformed("truncated block");
          }
          long[] node = new long[2];
          List<UUID> entries = decodeBlock(body, at[0], end, node);
          long start = node[1] << node[0];
          if (start <= prevTop) {
            throw malformed("blocks out of order or overlapping");
          }
          prevTop = top((int) node[0], node[1]);
          blocks.add(entries);
          nodes.add(new long[] {node[0], node[1], entries.size()});
          at[0] = end;
          break;
        default:
          skipField(body, at, tag);
      }
    }
    // Blocks are oldest first; the ids are listed newest first
    List<UUID> ids = new ArrayList<>();
    for (int i = blocks.size() - 1; i >= 0; i--) {
      ids.addAll(blocks.get(i));
    }
    long f = floor == null ? horizon : floor;
    if (f < horizon) {
      throw malformed("floor below horizon");
    }
    return new Decoded(ids, horizon, f, nodes);
  }

  /** A block's entries, newest first; {@code node} gets its level and index. */
  private static List<UUID> decodeBlock(byte[] buf, int off, int end, long[] node) {
    int level = 0;
    long index = 0L;
    int packedStart = -1;
    int packedEnd = -1;
    int[] at = {off};
    while (at[0] < end) {
      int tag = (int) readVarint(buf, at, end);
      switch (tag) {
        case TAG_LEVEL:
          level = (int) readVarint(buf, at, end);
          break;
        case TAG_INDEX:
          index = readVarint(buf, at, end);
          break;
        case TAG_PACKED:
          int len = (int) readVarint(buf, at, end);
          packedStart = at[0];
          packedEnd = packedStart + len;
          if (len < 0 || packedEnd > end) {
            throw malformed("truncated packed entries");
          }
          at[0] = packedEnd;
          break;
        default:
          skipField(buf, at, tag);
          if (at[0] > end) {
            throw malformed("truncated block field");
          }
      }
    }
    if (level > MAX_LEVEL || index < 0 || (level < MAX_LEVEL && index >= (1L << (MAX_LEVEL - level)))
        || (level == MAX_LEVEL && index != 0L)) {
      throw malformed("block node (" + level + ", " + index + ")");
    }
    if (packedStart < 0 || packedStart == packedEnd) {
      throw malformed("empty block");
    }
    node[0] = level;
    node[1] = index;
    long start = index << level;
    List<UUID> out = new ArrayList<>();
    long ts = top(level, index);
    int[] p = {packedStart};
    while (p[0] < packedEnd) {
      long gap = readVarint(buf, p, packedEnd);
      if (gap < 0 || gap > ts - start) {
        throw malformed("entry outside its block");
      }
      ts -= gap;
      if (p[0] + RANDOM_BYTES > packedEnd) {
        throw malformed("truncated entry");
      }
      out.add(UuidV7.fromTimestampAndRandom(ts, buf, p[0]));
      p[0] += RANDOM_BYTES;
    }
    return out;
  }

  /** Sorts v7 ids newest first, as their timestamps and then random bits order them. */
  public static void sortNewestFirst(Collection<UUID> ids, long[] ts, byte[][] rand) {
    UUID[] all = ids.toArray(new UUID[0]);
    long[] keyTs = new long[all.length];
    byte[][] keyRand = new byte[all.length][];
    Integer[] order = new Integer[all.length];
    for (int i = 0; i < all.length; i++) {
      if (!UuidV7.isV7(all[i])) {
        throw new IllegalArgumentException("Committed transaction IDs must be UUIDv7; got " + all[i]);
      }
      keyTs[i] = UuidV7.timestampMs(all[i]);
      keyRand[i] = UuidV7.packRandomBits(all[i]);
      order[i] = i;
    }
    Arrays.sort(order, (a, b) -> {
      int c = Long.compare(keyTs[b], keyTs[a]);
      return c != 0 ? c : Arrays.compareUnsigned(keyRand[b], keyRand[a]);
    });
    for (int i = 0; i < all.length; i++) {
      ts[i] = keyTs[order[i]];
      rand[i] = keyRand[order[i]];
    }
  }

  private static UncheckedIOException malformed(String what) {
    return new UncheckedIOException(new IOException("Malformed CommittedTransactions: " + what));
  }

  private static long readVarint(byte[] buf, int[] at, int end) {
    long value = 0;
    int shift = 0;
    while (true) {
      if (at[0] >= end || shift > 63) {
        throw malformed("truncated varint");
      }
      int b = buf[at[0]++] & 0xFF;
      value |= ((long) (b & 0x7F)) << shift;
      if ((b & 0x80) == 0) {
        return value;
      }
      shift += 7;
    }
  }

  private static long readFixed64(byte[] buf, int[] at) {
    if (at[0] + 8 > buf.length) {
      throw malformed("truncated fixed64");
    }
    long value = 0;
    for (int i = 7; i >= 0; i--) {
      value = (value << 8) | (buf[at[0] + i] & 0xFF);
    }
    at[0] += 8;
    return value;
  }

  private static void skipField(byte[] buf, int[] at, int tag) {
    switch (tag & 0x7) {
      case 0:
        readVarint(buf, at, buf.length);
        break;
      case 1:
        at[0] += 8;
        break;
      case 2:
        at[0] += (int) readVarint(buf, at, buf.length);
        break;
      case 5:
        at[0] += 4;
        break;
      default:
        throw malformed("wire type " + (tag & 0x7));
    }
    if (at[0] > buf.length) {
      throw malformed("truncated field");
    }
  }

  private static void writeVarint(ByteArrayOutputStream out, long value) {
    while ((value & ~0x7FL) != 0) {
      out.write((int) ((value & 0x7F) | 0x80));
      value >>>= 7;
    }
    out.write((int) value);
  }

  private static void writeFixed64(ByteArrayOutputStream out, long value) {
    for (int i = 0; i < 8; i++) {
      out.write((int) (value & 0xFF));
      value >>>= 8;
    }
  }
}
