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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HexFormat;
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
 * the floor F, absent when it equals H), {@code pages} (5, oldest first, each a {@code
 * CommittedPageRef}: the pages that hold the ids in (H, F]) and {@code blocks} (6, oldest first,
 * each a {@code CommittedBlock}: the ids above F). Fields 1 and 2 held the one-blob set of reader
 * version 1.
 *
 * <p>A page is named by its node and digest ({@link #pageName}): the first {@link #DIGEST_BYTES}
 * bytes of the SHA-256 of its block's {@code CommittedBlock} message as this codec writes it
 * ({@link #digest}).
 */
public final class CommittedSetCodec {

  public static final int COMMITTED_TXNS_HIGHEST_DROPPED_TIMESTAMP_MS = 3;
  public static final int COMMITTED_TXNS_FLOOR_MS = 4;
  public static final int COMMITTED_TXNS_PAGES = 5;
  public static final int COMMITTED_TXNS_BLOCKS = 6;
  public static final int COMMITTED_BLOCK_LEVEL = 1;
  public static final int COMMITTED_BLOCK_INDEX = 2;
  public static final int COMMITTED_BLOCK_PACKED_ENTRIES = 3;
  public static final int COMMITTED_PAGE_REF_LEVEL = 1;
  public static final int COMMITTED_PAGE_REF_INDEX = 2;
  public static final int COMMITTED_PAGE_REF_COUNT = 3;
  public static final int COMMITTED_PAGE_REF_MAX_TIMESTAMP_MS = 4;
  public static final int COMMITTED_PAGE_REF_DIGEST = 5;

  /** The bytes of a page's digest: the first 16 of a SHA-256. */
  public static final int DIGEST_BYTES = 16;

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
  private static final int TAG_PAGE = (COMMITTED_TXNS_PAGES << 3) | 2;
  private static final int TAG_BLOCK = (COMMITTED_TXNS_BLOCKS << 3) | 2;
  private static final int TAG_LEVEL = COMMITTED_BLOCK_LEVEL << 3;
  private static final int TAG_INDEX = COMMITTED_BLOCK_INDEX << 3;
  private static final int TAG_PACKED = (COMMITTED_BLOCK_PACKED_ENTRIES << 3) | 2;
  private static final int TAG_REF_LEVEL = COMMITTED_PAGE_REF_LEVEL << 3;
  private static final int TAG_REF_INDEX = COMMITTED_PAGE_REF_INDEX << 3;
  private static final int TAG_REF_COUNT = COMMITTED_PAGE_REF_COUNT << 3;
  private static final int TAG_REF_MAX_TS = (COMMITTED_PAGE_REF_MAX_TIMESTAMP_MS << 3) | 1;
  private static final int TAG_REF_DIGEST = (COMMITTED_PAGE_REF_DIGEST << 3) | 2;

  private CommittedSetCodec() {}

  /** A decoded body: the inline ids newest first, the horizon, the floor, the pages, the blocks. */
  public static final class Decoded {
    public final List<UUID> ids;
    public final long horizon;
    public final long floor;
    /** The pages, oldest first. */
    public final List<PageRef> pages;
    /** Each block, oldest first: {level, index, ids}. */
    public final List<long[]> blocks;

    Decoded(List<UUID> ids, long horizon, long floor, List<PageRef> pages, List<long[]> blocks) {
      this.ids = ids;
      this.horizon = horizon;
      this.floor = floor;
      this.pages = pages;
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
    return encode(txns, horizonMs, floorMs, List.of(), blockIds);
  }

  /**
   * Encodes a set of UUIDv7s (the inline ids, all above {@code floorMs}), the horizon, the floor
   * ({@code floorMs >= horizonMs}) and the pages (oldest first), cutting blocks of at most {@code
   * blockIds} ids. Empty iff there are no ids, no pages and the horizon and floor are 0.
   */
  public static byte[] encode(
      Collection<UUID> txns, long horizonMs, long floorMs, List<PageRef> pages, int blockIds) {
    if (floorMs < horizonMs) {
      throw new IllegalArgumentException("floor " + floorMs + " below horizon " + horizonMs);
    }
    int n = txns.size();
    long[] ts = new long[n];
    byte[][] rand = new byte[n][];
    sortNewestFirst(txns, ts, rand);
    return encodeSorted(ts, rand, n, horizonMs, floorMs, pages, blockIds);
  }

  private static byte[] encodeSorted(
      long[] ts, byte[][] rand, int n, long horizonMs, long floorMs, List<PageRef> pages,
      int blockIds) {
    if (n == 0 && horizonMs == 0L && floorMs == 0L && pages.isEmpty()) {
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
    for (PageRef page : pages) {
      byte[] ref = new byte[pageRefSize(page)];
      writePageRef(ref, 0, page);
      out.write(TAG_PAGE);
      writeVarint(out, ref.length);
      out.write(ref, 0, ref.length);
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
    byte[] body = packed.toByteArray();
    return blockMessage(level, index, body, 0, body.length);
  }

  /** The newest timestamp node {@code (level, index)} spans. */
  public static long top(int level, long index) {
    return ((index + 1) << level) - 1;
  }

  /**
   * The {@code CommittedBlock} message of node {@code (level, index)} and its packed entries
   * {@code packed[off, end)}, as this codec writes it: the level and index unless 0, then the
   * entries.
   */
  public static byte[] blockMessage(int level, long index, byte[] packed, int off, int end) {
    ByteArrayOutputStream message = new ByteArrayOutputStream(end - off + 16);
    if (level != 0) {
      message.write(TAG_LEVEL);
      writeVarint(message, level);
    }
    if (index != 0L) {
      message.write(TAG_INDEX);
      writeVarint(message, index);
    }
    message.write(TAG_PACKED);
    writeVarint(message, end - off);
    message.write(packed, off, end - off);
    return message.toByteArray();
  }

  /**
   * A page's digest: the first {@link #DIGEST_BYTES} bytes of the SHA-256 of the block's {@code
   * CommittedBlock} message ({@link #blockMessage}).
   */
  public static byte[] digest(int level, long index, byte[] packed, int off, int end) {
    return digest(blockMessage(level, index, packed, off, end));
  }

  /** The first {@link #DIGEST_BYTES} bytes of the SHA-256 of {@code message}. */
  public static byte[] digest(byte[] message) {
    try {
      return Arrays.copyOf(MessageDigest.getInstance("SHA-256").digest(message), DIGEST_BYTES);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  /** A page's name: {@code <level>-<index>-<digest as hex>}. */
  public static String pageName(int level, long index, byte[] digest) {
    return level + "-" + index + "-" + HexFormat.of().formatHex(digest);
  }

  /** The top of the node a page's name ({@link #pageName}) names; -1 if it names no page. */
  public static long pageTop(String name) {
    String[] parts = name.split("-", -1);
    if (parts.length != 3 || parts[2].length() != 2 * DIGEST_BYTES) {
      return -1L;
    }
    try {
      int level = Integer.parseInt(parts[0]);
      long index = Long.parseLong(parts[1]);
      HexFormat.of().parseHex(parts[2]);
      if (level < 0 || level > MAX_LEVEL || index < 0
          || (level < MAX_LEVEL && index >= (1L << (MAX_LEVEL - level)))
          || (level == MAX_LEVEL && index != 0L)) {
        return -1L;
      }
      return top(level, index);
    } catch (IllegalArgumentException e) {
      return -1L;
    }
  }

  /**
   * The ids of node {@code (level, index)}'s packed entries, newest first (a page's {@code
   * packed_entries}), checking that each lies in the node.
   */
  public static List<UUID> decodeEntries(int level, long index, byte[] packed) {
    checkNode(level, index);
    if (packed.length == 0) {
      throw malformed("empty block");
    }
    return entries(level, index, packed, 0, packed.length);
  }

  /**
   * Whether node {@code (level, index)}'s packed entries (a page's {@code packed_entries}) hold
   * {@code id}: a scan, newest first, that stops below the id's timestamp.
   */
  public static boolean holds(int level, long index, byte[] packed, UUID id) {
    if (!UuidV7.isV7(id)) {
      return false;
    }
    long want = UuidV7.timestampMs(id);
    byte[] rand = UuidV7.packRandomBits(id);
    long ts = top(level, index);
    int[] p = {0};
    while (p[0] < packed.length) {
      ts -= readVarint(packed, p, packed.length);
      if (p[0] + RANDOM_BYTES > packed.length) {
        throw malformed("truncated entry");
      }
      if (ts < want) {
        return false;
      }
      if (ts == want && Arrays.equals(packed, p[0], p[0] + RANDOM_BYTES, rand, 0, RANDOM_BYTES)) {
        return true;
      }
      p[0] += RANDOM_BYTES;
    }
    return false;
  }

  /** The length of a {@code CommittedPageRef} message. */
  public static int pageRefSize(PageRef page) {
    return (page.level() != 0 ? 1 + varintSize(page.level()) : 0)
        + (page.index() != 0L ? 1 + varintSize(page.index()) : 0)
        + (page.count() != 0 ? 1 + varintSize(page.count()) : 0)
        + (page.maxTimestampMs() != 0L ? 9 : 0)
        + 1 + varintSize(DIGEST_BYTES) + DIGEST_BYTES;
  }

  /** Writes a {@code CommittedPageRef} message at {@code at}; returns the offset after it. */
  public static int writePageRef(byte[] out, int at, PageRef page) {
    if (page.level() != 0) {
      out[at++] = (byte) TAG_REF_LEVEL;
      at = writeVarint(out, at, page.level());
    }
    if (page.index() != 0L) {
      out[at++] = (byte) TAG_REF_INDEX;
      at = writeVarint(out, at, page.index());
    }
    if (page.count() != 0) {
      out[at++] = (byte) TAG_REF_COUNT;
      at = writeVarint(out, at, page.count());
    }
    if (page.maxTimestampMs() != 0L) {
      out[at++] = (byte) TAG_REF_MAX_TS;
      long v = page.maxTimestampMs();
      for (int i = 0; i < 8; i++) {
        out[at++] = (byte) (v & 0xFF);
        v >>>= 8;
      }
    }
    out[at++] = (byte) TAG_REF_DIGEST;
    at = writeVarint(out, at, DIGEST_BYTES);
    byte[] digest = page.digest();
    if (digest.length != DIGEST_BYTES) {
      throw new IllegalArgumentException("A page digest of " + digest.length + " bytes");
    }
    System.arraycopy(digest, 0, out, at, DIGEST_BYTES);
    return at + DIGEST_BYTES;
  }

  /** Decodes a {@code CommittedPageRef} message {@code buf[off, end)}. */
  public static PageRef decodePageRef(byte[] buf, int off, int end) {
    int level = 0;
    long index = 0L;
    long count = 0L;
    long maxTs = 0L;
    byte[] digest = null;
    int[] at = {off};
    while (at[0] < end) {
      int tag = (int) readVarint(buf, at, end);
      switch (tag) {
        case TAG_REF_LEVEL:
          level = (int) readVarint(buf, at, end);
          break;
        case TAG_REF_INDEX:
          index = readVarint(buf, at, end);
          break;
        case TAG_REF_COUNT:
          count = readVarint(buf, at, end);
          break;
        case TAG_REF_MAX_TS:
          if (at[0] + 8 > end) {
            throw malformed("truncated page reference");
          }
          maxTs = readFixed64(buf, at);
          break;
        case TAG_REF_DIGEST:
          int len = (int) readVarint(buf, at, end);
          if (len != DIGEST_BYTES || at[0] + len > end) {
            throw malformed("page digest of " + len + " bytes");
          }
          digest = Arrays.copyOfRange(buf, at[0], at[0] + len);
          at[0] += len;
          break;
        default:
          skipField(buf, at, tag);
          if (at[0] > end) {
            throw malformed("truncated page reference field");
          }
      }
    }
    checkNode(level, index);
    if (digest == null || count <= 0 || count > Integer.MAX_VALUE) {
      throw malformed("page reference without a digest or ids");
    }
    long top = top(level, index);
    if (maxTs > top || maxTs < (index << level)) {
      throw malformed("page reference's newest id outside its node");
    }
    return new PageRef(level, index, (int) count, maxTs, digest);
  }

  private static void checkNode(int level, long index) {
    if (level < 0 || level > MAX_LEVEL || index < 0
        || (level < MAX_LEVEL && index >= (1L << (MAX_LEVEL - level)))
        || (level == MAX_LEVEL && index != 0L)) {
      throw malformed("block node (" + level + ", " + index + ")");
    }
  }

  /**
   * {@code inheritedBytes} with {@code addUnconditionally} added (copies of ids it holds are
   * kept once), then every page whose top is at or below {@code dropBeforeTimestampMs} and every
   * inherited id at or below it dropped (0 drops nothing), the horizon raised to the newest id
   * dropped (a page's newest) and the floor to the horizon. The additions are kept whatever their
   * timestamps, as replay adds only ids it just applied.
   */
  public static byte[] rewrite(
      byte[] inheritedBytes, Collection<UUID> addUnconditionally, long dropBeforeTimestampMs) {
    Decoded inherited = decodeBody(inheritedBytes);
    LinkedHashSet<UUID> kept = new LinkedHashSet<>();
    long horizon = inherited.horizon;
    List<PageRef> pages = new ArrayList<>();
    for (PageRef page : inherited.pages) {
      if (dropBeforeTimestampMs > 0L && page.top() <= dropBeforeTimestampMs) {
        horizon = Math.max(horizon, page.maxTimestampMs());
      } else {
        pages.add(page);
      }
    }
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
    return encode(kept, horizon, Math.max(inherited.floor, horizon), pages, BLOCK_IDS);
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

  /**
   * Decodes a whole body, checking its blocks (disjoint, oldest first, entries inside) and pages
   * (oldest first, their tops in (H, F]).
   */
  public static Decoded decodeBody(byte[] body) {
    long horizon = 0L;
    Long floor = null;
    List<PageRef> pages = new ArrayList<>();
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
        case TAG_PAGE:
          int refLen = (int) readVarint(body, at, body.length);
          int refEnd = at[0] + refLen;
          if (refLen <= 0 || refEnd > body.length) {
            throw malformed("truncated page reference");
          }
          pages.add(decodePageRef(body, at[0], refEnd));
          at[0] = refEnd;
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
    long prevPageTop = horizon;
    for (PageRef page : pages) {
      if (page.top() <= prevPageTop || page.top() > f) {
        throw malformed("pages out of order, or outside (horizon, floor]");
      }
      prevPageTop = page.top();
    }
    return new Decoded(ids, horizon, f, pages, nodes);
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
    checkNode(level, index);
    if (packedStart < 0 || packedStart == packedEnd) {
      throw malformed("empty block");
    }
    node[0] = level;
    node[1] = index;
    return entries(level, index, buf, packedStart, packedEnd);
  }

  /** The ids of node {@code (level, index)}'s packed entries {@code buf[packedStart, packedEnd)}. */
  private static List<UUID> entries(
      int level, long index, byte[] buf, int packedStart, int packedEnd) {
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

  private static int writeVarint(byte[] buf, int at, long value) {
    while ((value & ~0x7FL) != 0) {
      buf[at++] = (byte) ((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    buf[at++] = (byte) value;
    return at;
  }

  private static int varintSize(long value) {
    int size = 1;
    while ((value & ~0x7FL) != 0) {
      size++;
      value >>>= 7;
    }
    return size;
  }

  private static void writeFixed64(ByteArrayOutputStream out, long value) {
    for (int i = 0; i < 8; i++) {
      out.write((int) (value & 0xFF));
      value >>>= 8;
    }
  }
}
