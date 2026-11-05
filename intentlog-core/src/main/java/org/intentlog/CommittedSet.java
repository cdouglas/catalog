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
package org.intentlog;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.intentlog.wire.CommittedSetCodec;
import org.intentlog.wire.LogEnvelope;
import org.intentlog.wire.PageOut;
import org.intentlog.wire.PageRef;
import org.intentlog.wire.UuidV7;

/**
 * The ids of the committed transactions a log's replay has applied, less those GC dropped; the
 * GC horizon H, the newest timestamp it dropped; and the floor F, at or above H, at or below
 * which replay admits no record. Replay applies a record once by its id, and a writer learns from
 * the set whether its commit landed. See docs/LOG.md "The committed set and GC" and
 * docs/COMMITTED_PAGING.md.
 *
 * <p>The ids above F are inline, in blocks. The ids in (H, F] are in pages, immutable objects
 * apart from the log that the set names by reference ({@link PageRef}): a {@code PageOut} record
 * ({@link Builder#pageOut}) seals the oldest block into its page and raises F to its top, and a GC
 * record drops the pages whose top it passes. Replay never reads a page, since it admits only ids
 * above F; a lookup of an id in (H, F] throws {@link PagedOutException}, naming the page that
 * holds it if it committed.
 *
 * <p>The ids are cut into blocks on a grid over their timestamps: the maximal aligned spans of
 * 2^k ms that hold at most {@link CommittedSetCodec#BLOCK_IDS} ids (a millisecond is a block
 * however many it holds). The blocks depend on the ids alone, not on the order replay added them
 * or where its builds fell, so every replica at a position encodes the same bytes (I11). An id
 * outside every block opens the largest span that holds it and overlaps no block; a block that
 * passes N ids splits into the maximal spans inside it; a GC, which removes the oldest ids, merges
 * the oldest blocks with their neighbours once their common span holds N or fewer.
 *
 * <p>A block keeps its entries (newest first, each a varint gap from the one before and 10
 * random bytes) in runs: slices of packed entries, newest run first. Queries find the block by
 * its span and scan its runs in place. Ids newer than a block's newest become a run of their own,
 * and runs merge by concatenation up to {@link #RUN_BYTES}; an id older than the newest merges
 * only with the entries at or above it. So building a set costs the ids added, not the set.
 * {@link #bytes()} copies each block's bytes: a block decoded and unchanged keeps its slice of the
 * body, so it matches {@link CommittedSetCodec}'s encoding of the same ids byte for byte.
 */
public final class CommittedSet {

  /** The bytes of packed entries runs reach by merging, and the chunks a GC cuts a run into. */
  static final int RUN_BYTES = 1 << 20;

  private static final int MAX_LEVEL = CommittedSetCodec.MAX_LEVEL;
  private static final int RANDOM_BYTES = CommittedSetCodec.RANDOM_BYTES;
  private static final int TAG_HORIZON = 0x19; // field 3, fixed64
  private static final int TAG_FLOOR = 0x21; // field 4, fixed64
  private static final int TAG_PAGE = 0x2a; // field 5, length-delimited
  private static final int TAG_BLOCK = 0x32; // field 6, length-delimited
  private static final int TAG_LEVEL = 0x08; // Block field 1, varint
  private static final int TAG_INDEX = 0x10; // Block field 2, varint
  private static final int TAG_PACKED = 0x1a; // Block field 3, length-delimited

  // Before EMPTY, which needs it
  private static final PageRef[] NO_PAGES = new PageRef[0];

  public static final CommittedSet EMPTY = empty(CommittedSetCodec.BLOCK_IDS);

  private final Block[] blocks; // oldest first, disjoint
  private final long horizon;
  private final long floor;
  private final PageRef[] pages; // oldest first: page i holds (top of page i - 1, its top]
  private final int blockIds;
  // The body this set was decoded from, while it is unchanged; else null
  private final byte[] encoded;

  private CommittedSet(
      Block[] blocks, long horizon, long floor, PageRef[] pages, int blockIds, byte[] encoded) {
    this.blocks = blocks;
    this.horizon = horizon;
    this.floor = floor;
    this.pages = pages;
    this.blockIds = blockIds;
    this.encoded = encoded;
  }

  static CommittedSet empty(int blockIds) {
    return new CommittedSet(new Block[0], 0L, 0L, NO_PAGES, blockIds, new byte[0]);
  }

  /** The set encoded by a {@code CommittedTransactions} body; empty for null or no bytes. */
  public static CommittedSet of(byte[] bytes) {
    return of(bytes, CommittedSetCodec.BLOCK_IDS);
  }

  /** {@link #of(byte[])}, with blocks of at most {@code blockIds} ids; for tests. */
  static CommittedSet of(byte[] bytes, int blockIds) {
    if (bytes == null || bytes.length == 0) {
      return empty(blockIds);
    }
    long horizon = 0L;
    Long floor = null;
    List<PageRef> pages = new ArrayList<>();
    List<Block> blocks = new ArrayList<>();
    int[] at = {0};
    while (at[0] < bytes.length) {
      int tag = (int) readVarint(bytes, at, bytes.length);
      switch (tag) {
        case TAG_HORIZON:
          horizon = readFixed64(bytes, at);
          break;
        case TAG_FLOOR:
          floor = readFixed64(bytes, at);
          break;
        case TAG_PAGE:
          int refLen = (int) readVarint(bytes, at, bytes.length);
          int refEnd = at[0] + refLen;
          if (refLen <= 0 || refEnd > bytes.length) {
            throw malformed("truncated page reference");
          }
          pages.add(CommittedSetCodec.decodePageRef(bytes, at[0], refEnd));
          at[0] = refEnd;
          break;
        case TAG_BLOCK:
          int len = (int) readVarint(bytes, at, bytes.length);
          int end = at[0] + len;
          if (len <= 0 || end > bytes.length) {
            throw malformed("truncated block");
          }
          Block block = Block.decode(bytes, at[0], end);
          if (!blocks.isEmpty() && block.start() <= blocks.get(blocks.size() - 1).top()) {
            throw malformed("blocks out of order or overlapping");
          }
          blocks.add(block);
          at[0] = end;
          break;
        default:
          skipField(bytes, at, tag);
      }
    }
    long f = floor == null ? horizon : floor;
    if (f < horizon) {
      throw malformed("floor below horizon");
    }
    long prevTop = horizon;
    for (PageRef page : pages) {
      if (page.top() <= prevTop || page.top() > f) {
        throw malformed("pages out of order, or outside (horizon, floor]");
      }
      prevTop = page.top();
    }
    return new CommittedSet(blocks.toArray(new Block[0]), horizon, f,
        pages.toArray(new PageRef[0]), blockIds, bytes);
  }

  /**
   * The {@code CommittedTransactions} body; empty iff the set holds no id, no page, no horizon and
   * no floor.
   */
  public byte[] bytes() {
    if (encoded != null) {
      return encoded;
    }
    if (blocks.length == 0 && pages.length == 0 && horizon == 0L && floor == 0L) {
      return new byte[0];
    }
    byte[] out = new byte[checkedSize()];
    int at = 0;
    if (horizon != 0L) {
      out[at++] = TAG_HORIZON;
      at = writeFixed64(out, at, horizon);
    }
    if (floor != horizon) {
      out[at++] = TAG_FLOOR;
      at = writeFixed64(out, at, floor);
    }
    for (PageRef page : pages) {
      out[at++] = TAG_PAGE;
      at = writeVarint(out, at, CommittedSetCodec.pageRefSize(page));
      at = CommittedSetCodec.writePageRef(out, at, page);
    }
    for (Block block : blocks) {
      out[at++] = TAG_BLOCK;
      at = writeVarint(out, at, block.messageSize());
      at = block.writeMessage(out, at);
    }
    return out;
  }

  /** The length of {@link #bytes()}, without encoding them. */
  public long encodedSize() {
    if (encoded != null) {
      return encoded.length;
    }
    return size();
  }

  private long size() {
    if (blocks.length == 0 && pages.length == 0 && horizon == 0L && floor == 0L) {
      return 0;
    }
    long size = (horizon != 0L ? 9 : 0) + (floor != horizon ? 9 : 0);
    for (PageRef page : pages) {
      int message = CommittedSetCodec.pageRefSize(page);
      size += 1 + varintSize(message) + message;
    }
    return size + inlineBytes();
  }

  /** The bytes of the inline blocks in {@link #bytes()}: what paging bounds. */
  public long inlineBytes() {
    long size = 0;
    for (Block block : blocks) {
      int message = block.messageSize();
      size += 1 + varintSize(message) + message;
    }
    return size;
  }

  private int checkedSize() {
    long size = size();
    if (size > Integer.MAX_VALUE - 32) {
      throw new IllegalStateException("A committed set of " + size + " bytes is too large");
    }
    return (int) size;
  }

  /** The newest timestamp GC dropped (0 before any GC). */
  public long horizon() {
    return horizon;
  }

  /** The floor: replay admits no record whose id is at or below it. At or above the horizon. */
  public long floor() {
    return floor;
  }

  /** The pages, oldest first: they hold the ids in (horizon, floor]. */
  public List<PageRef> pages() {
    return List.of(pages);
  }

  /**
   * Whether {@code id} committed (LOG.md, the tri-state lookup):
   *
   * <ul>
   *   <li>above the floor, or not a UUIDv7: whether the inline blocks hold it;
   *   <li>in (H, F]: if a page may hold it, this throws {@link PagedOutException} naming the
   *       page, which says; false if none can (no page's range holds it, or it is above its
   *       page's newest id). Either way its record never applies again;
   *   <li>at or below the horizon: the set cannot tell an aged-out commit from one that never
   *       landed, and this throws {@link DedupHorizonExceededException}.
   * </ul>
   */
  public boolean contains(UUID id) {
    return holds(id) || notInline(id);
  }

  /**
   * Whether an id the inline blocks do not hold committed: false above the floor; in (H, F], false
   * or {@link PagedOutException}; at or below H, {@link DedupHorizonExceededException}.
   */
  private boolean notInline(UUID id) {
    if (aboveFloor(id, floor)) {
      return false;
    }
    long ts = UuidV7.timestampMs(id);
    if (horizon != 0L && ts <= horizon) {
      throw new DedupHorizonExceededException(id, horizon);
    }
    PageRef page = pageFor(ts);
    if (page == null || ts > page.maxTimestampMs()) {
      // Above the newest id of a page GC dropped, or of the page whose range holds it: no id
      // applied there, and none can (COMMITTED_PAGING.md "Lookups")
      return false;
    }
    throw new PagedOutException(id, page);
  }

  /** The oldest page whose top is at or above {@code ts}, or null. */
  private PageRef pageFor(long ts) {
    int lo = 0;
    int hi = pages.length;
    while (lo < hi) {
      int mid = (lo + hi) >>> 1;
      if (pages[mid].top() < ts) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo < pages.length ? pages[lo] : null;
  }

  /**
   * Whether replay may apply the record of {@code id}: the set neither holds the id (the record
   * is a retry of one already applied) nor is the id at or below the floor. A record at or below
   * it may be a retry of a commit whose id aged out, so replay rejects it (Bug 64).
   */
  public boolean mayApply(UUID id) {
    return !holds(id) && aboveFloor(id, floor);
  }

  private boolean holds(UUID id) {
    if (blocks.length == 0 || !UuidV7.isV7(id)) {
      return false;
    }
    long ts = UuidV7.timestampMs(id);
    int b = blockFor(blocks, ts);
    return b >= 0 && blocks[b].holds(ts, UuidV7.packRandomBits(id));
  }

  /** Decodes every inline id in the set (those above the floor), newest first. */
  public Set<UUID> ids() {
    if (blocks.length == 0) {
      return Collections.emptySet();
    }
    try {
      return Collections.unmodifiableSet(CommittedSetCodec.decode(bytes()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** A builder that adds ids to this set. */
  public Builder toBuilder() {
    return new Builder(this);
  }

  public static Builder builder() {
    return new Builder(EMPTY);
  }

  /** A builder of an empty set with blocks of at most {@code blockIds} ids; for tests. */
  static Builder builder(int blockIds) {
    return new Builder(empty(blockIds));
  }

  /** The number of blocks; for tests. */
  int blockCount() {
    return blocks.length;
  }

  /** The number of runs in every block; for tests. */
  int runCount() {
    int runs = 0;
    for (Block block : blocks) {
      runs += block.runs.length;
    }
    return runs;
  }

  /** The block that holds {@code ts}'s span, or -1. */
  private static int blockFor(Block[] blocks, long ts) {
    int lo = 0;
    int hi = blocks.length - 1;
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      Block block = blocks[mid];
      if (ts < block.start()) {
        hi = mid - 1;
      } else if (ts > block.top()) {
        lo = mid + 1;
      } else {
        return mid;
      }
    }
    return -1;
  }

  /** {@link #blockFor} over a list. */
  private static int blockFor(List<Block> blocks, long ts) {
    int lo = 0;
    int hi = blocks.size() - 1;
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      Block block = blocks.get(mid);
      if (ts < block.start()) {
        hi = mid - 1;
      } else if (ts > block.top()) {
        lo = mid + 1;
      } else {
        return mid;
      }
    }
    return -(lo + 1);
  }

  /** This set with {@code added} too. */
  private CommittedSet with(Collection<UUID> added) {
    int n = added.size();
    long[] ts = new long[n];
    byte[][] rand = new byte[n][];
    CommittedSetCodec.sortNewestFirst(added, ts, rand);

    // Each block's additions are a range of the sorted ids, since blocks are disjoint spans. An
    // id outside every block opens the largest span that holds it and overlaps no block.
    List<Block> list = new ArrayList<>(Arrays.asList(blocks));
    List<Block> targets = new ArrayList<>();
    List<int[]> ranges = new ArrayList<>();
    int i = 0;
    while (i < n) {
      int b = blockFor(list, ts[i]);
      Block target;
      if (b >= 0) {
        target = list.get(b);
      } else {
        int insert = -(b + 1);
        target = Block.open(largestFreeNode(list, insert, ts[i]));
        list.add(insert, target);
      }
      int j = i + 1;
      while (j < n && ts[j] >= target.start()) {
        j++;
      }
      targets.add(target);
      ranges.add(new int[] {i, j});
      i = j;
    }
    for (int t = 0; t < targets.size(); t++) {
      Block target = targets.get(t);
      int[] range = ranges.get(t);
      Block grown = target.withAdds(ts, rand, range[0], range[1]);
      int at = indexOf(list, target);
      // A block decoded from a checkpoint has not counted its entries; its bytes bound them, so a
      // fold scans one only when it may have passed N
      if (grown.level > 0 && grown.countAtMost() > blockIds && grown.count() > blockIds) {
        List<Block> split = grown.split(blockIds);
        list.remove(at);
        list.addAll(at, split);
      } else {
        list.set(at, grown);
      }
    }
    return new CommittedSet(list.toArray(new Block[0]), horizon, floor, pages, blockIds, null);
  }

  private static int indexOf(List<Block> list, Block block) {
    int at = blockFor(list, block.start());
    if (at < 0 || list.get(at) != block) {
      throw new IllegalStateException("block not found");
    }
    return at;
  }

  /**
   * The largest node holding {@code ts} that overlaps no block of {@code list}, where {@code
   * insert} is where a block holding {@code ts} would go. A node holding {@code ts} overlaps a
   * block only by containing it, since no block holds {@code ts}.
   */
  private static long[] largestFreeNode(List<Block> list, int insert, long ts) {
    int level = 0;
    long index = ts;
    while (level < MAX_LEVEL) {
      int parentLevel = level + 1;
      long parentIndex = index >> 1;
      long start = parentIndex << parentLevel;
      long top = CommittedSetCodec.top(parentLevel, parentIndex);
      boolean before = insert > 0 && list.get(insert - 1).top() >= start;
      boolean after = insert < list.size() && list.get(insert).start() <= top;
      if (before || after) {
        break;
      }
      level = parentLevel;
      index = parentIndex;
    }
    return new long[] {level, index};
  }

  /**
   * Merges the oldest blocks with their neighbours while their common span holds at most N ids:
   * after a GC or a page-out, which remove ids only at the old end, these are the only spans
   * whose counts fell.
   */
  private static void mergeUp(List<Block> list, int blockIds) {
    while (!list.isEmpty()) {
      Block oldest = list.get(0);
      if (oldest.level >= MAX_LEVEL) {
        return;
      }
      int level = oldest.level + 1;
      long index = oldest.index >> 1;
      long top = CommittedSetCodec.top(level, index);
      int inside = 0;
      long count = 0;
      while (inside < list.size() && list.get(inside).top() <= top) {
        count += list.get(inside).count();
        inside++;
      }
      if (count > blockIds) {
        return;
      }
      List<Block> merged = new ArrayList<>(list.subList(0, inside));
      list.subList(0, inside).clear();
      list.add(0, Block.merge(level, index, merged, (int) count));
    }
  }

  /**
   * This set less every page whose top is at or below {@code watermarkMs} and every inline id at
   * or below it, the horizon raised to the newest id dropped (a page's newest), and the floor to at
   * least the horizon. A page that straddles the watermark is kept whole, so the ids it holds below
   * the watermark stay known.
   */
  private CommittedSet dropThrough(long watermarkMs) {
    long newestDropped = Long.MIN_VALUE;
    int dropPages = 0;
    while (dropPages < pages.length && pages[dropPages].top() <= watermarkMs) {
      newestDropped = Math.max(newestDropped, pages[dropPages].maxTimestampMs());
      dropPages++;
    }
    int drop = 0;
    while (drop < blocks.length && blocks[drop].maxTs() <= watermarkMs) {
      newestDropped = Math.max(newestDropped, blocks[drop].maxTs());
      drop++;
    }
    List<Block> list = new ArrayList<>(Arrays.asList(blocks).subList(drop, blocks.length));
    if (!list.isEmpty()) {
      Block.Cut cut = list.get(0).cutThrough(watermarkMs);
      if (cut != null) {
        list.set(0, cut.kept);
        newestDropped = Math.max(newestDropped, cut.newestDropped);
      }
    }
    if (newestDropped == Long.MIN_VALUE) {
      return this;
    }
    mergeUp(list, blockIds);
    long newHorizon = Math.max(horizon, newestDropped);
    return new CommittedSet(list.toArray(new Block[0]), newHorizon, Math.max(floor, newHorizon),
        Arrays.copyOfRange(pages, dropPages, pages.length), blockIds, null);
  }

  /**
   * The page a compactor would write of the oldest block, if it may seal it under {@code
   * ceilingMs}: the oldest block is not the newest, and its top is at or below the ceiling. Else
   * null.
   */
  public Seal sealable(long ceilingMs) {
    if (blocks.length < 2 || blocks[0].top() > ceilingMs) {
      return null;
    }
    Block oldest = blocks[0];
    return new Seal(new PageOut(oldest.level, oldest.index, oldest.count(), oldest.digest()),
        oldest.maxTs(), oldest.packed());
  }

  /**
   * This set after a {@code PageOut} record (docs/COMMITTED_PAGING.md "Paging out: the log
   * record"): if the oldest block is the one the record sealed (its node, count and digest) and is
   * not the newest block, the block's ids leave the set into a page reference, the blocks merge as
   * the rule says, and the floor rises to the block's top. Otherwise (a late id or a GC changed
   * the block since its writer read it, another PageOut sealed it, or it is the newest) this set,
   * unchanged. The checks read only this set, so every replica at a position agrees (I11).
   */
  public CommittedSet pagedOut(PageOut record) {
    if (blocks.length < 2) {
      return this;
    }
    Block oldest = blocks[0];
    if (oldest.level != record.level() || oldest.index != record.index()
        || oldest.count() != record.count() || !record.hasDigest(oldest.digest())) {
      return this;
    }
    List<Block> list = new ArrayList<>(Arrays.asList(blocks).subList(1, blocks.length));
    mergeUp(list, blockIds);
    PageRef[] next = Arrays.copyOf(pages, pages.length + 1);
    next[pages.length] =
        new PageRef(oldest.level, oldest.index, oldest.count(), oldest.maxTs(), oldest.digest());
    return new CommittedSet(list.toArray(new Block[0]), horizon, oldest.top(), next, blockIds,
        null);
  }

  /**
   * The page of the oldest block a compactor may seal ({@link #sealable}): the {@code PageOut} to
   * send, and the page to put before it.
   */
  public static final class Seal {
    private final PageOut record;
    private final long maxTimestampMs;
    private final byte[] packedEntries;

    Seal(PageOut record, long maxTimestampMs, byte[] packedEntries) {
      this.record = record;
      this.maxTimestampMs = maxTimestampMs;
      this.packedEntries = packedEntries;
    }

    /** The record that seals the block. */
    public PageOut record() {
      return record;
    }

    /** The reference the set gains when the record applies. */
    public PageRef ref() {
      return new PageRef(record.level(), record.index(), record.count(), maxTimestampMs,
          record.digest());
    }

    /** The page's name. */
    public String name() {
      return record.name();
    }

    /**
     * The page's body ({@code CommittedPage}) for the log {@code logId}: a function of the log and
     * the block alone, so every compactor that seals the block puts the same bytes.
     */
    public byte[] page(UUID logId) {
      return LogEnvelope.encodePage(logId, record.level(), record.index(), packedEntries);
    }

    @Override
    public String toString() {
      return record.toString();
    }
  }

  private static boolean aboveFloor(UUID id, long floor) {
    return !UuidV7.isV7(id) || floor == 0L || UuidV7.timestampMs(id) > floor;
  }

  /**
   * A block: node {@code (level, index)}, the span {@code [index * 2^level, (index + 1) *
   * 2^level)}, and its entries in runs, newest run first. Never changed once made.
   */
  private static final class Block {
    final int level;
    final long index;
    final Run[] runs; // newest first; empty only for a block just opened
    private volatile int count;
    // The page digest of the block, made on first use
    private volatile byte[] digest;
    // The Block message's bytes: a slice of the body it was decoded from, or made on first use
    private volatile byte[] messageBuf;
    private volatile int messageOff;
    private volatile int messageEnd;

    private Block(int level, long index, Run[] runs, int count) {
      this.level = level;
      this.index = index;
      this.runs = runs;
      this.count = count;
    }

    static Block open(long[] node) {
      return new Block((int) node[0], node[1], new Run[0], 0);
    }

    static Block decode(byte[] buf, int off, int end) {
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
      if (level > MAX_LEVEL || index < 0
          || (level == MAX_LEVEL ? index != 0L : index >= (1L << (MAX_LEVEL - level)))) {
        throw malformed("block node (" + level + ", " + index + ")");
      }
      if (packedStart < 0 || packedStart == packedEnd) {
        throw malformed("empty block");
      }
      long top = CommittedSetCodec.top(level, index);
      int[] first = {packedStart};
      long gap = readVarint(buf, first, packedEnd);
      if (gap < 0 || gap > top - (index << level)) {
        throw malformed("entry outside its block");
      }
      Block block = new Block(level, index,
          new Run[] {Run.slice(buf, packedStart, packedEnd, top - gap)}, -1);
      block.messageBuf = buf;
      block.messageOff = off;
      block.messageEnd = end;
      return block;
    }

    long start() {
      return index << level;
    }

    long top() {
      return CommittedSetCodec.top(level, index);
    }

    long maxTs() {
      return runs[0].maxTs;
    }

    int count() {
      int c = count;
      if (c < 0) {
        c = 0;
        for (Run run : runs) {
          c += run.count();
        }
        count = c;
      }
      return c;
    }

    /** At least {@link #count()}, without scanning a run that has not counted its entries. */
    int countAtMost() {
      int c = count;
      if (c >= 0) {
        return c;
      }
      long most = 0;
      for (Run run : runs) {
        most += run.countAtMost();
      }
      return (int) Math.min(most, Integer.MAX_VALUE);
    }

    boolean holds(long ts, byte[] rand) {
      for (Run run : runs) {
        if (ts > run.maxTs) {
          // Newer than this run and older than the one before it
          return false;
        }
        int found = run.find(ts, rand);
        if (found != 0) {
          return found > 0;
        }
      }
      return false;
    }

    /** This block with sorted ids {@code [from, to)} too, all within its span. */
    Block withAdds(long[] ts, byte[][] rand, int from, int to) {
      if (runs.length == 0) {
        return new Block(level, index, new Run[] {Run.of(ts, rand, from, to)}, to - from);
      }
      long oldest = ts[to - 1];
      List<Run> next = new ArrayList<>(runs.length + 2);
      int known = count;
      if (oldest > runs[0].maxTs) {
        next.add(Run.of(ts, rand, from, to));
        next.addAll(Arrays.asList(runs));
        return new Block(level, index, mergeHead(next), known < 0 ? -1 : known + to - from);
      }
      // Merge the ids with the entries at or above the oldest of them: whole runs, then the part
      // of the run the oldest falls in, whose entries below it stay a run
      List<Run> above = new ArrayList<>();
      Run below = null;
      int rest = 0;
      while (rest < runs.length && runs[rest].maxTs >= oldest) {
        Run[] split = runs[rest++].splitBelow(oldest);
        above.add(split[0]);
        if (split[1] != null) {
          below = split[1];
          break;
        }
      }
      Entries merged = Entries.merge(Entries.of(above), ts, rand, from, to);
      next.add(Run.of(merged.ts, merged.rand, 0, merged.size));
      if (below != null) {
        next.add(below);
      }
      next.addAll(Arrays.asList(runs).subList(rest, runs.length));
      int added = merged.size - merged.inherited;
      return new Block(level, index, mergeHead(next), known < 0 ? -1 : known + added);
    }

    /** This block, past N ids, as the maximal nodes inside it that hold at most N. */
    List<Block> split(int blockIds) {
      Entries all = Entries.of(Arrays.asList(runs));
      List<Block> out = new ArrayList<>();
      partition(all, level, index, 0, all.size, blockIds, out);
      return out;
    }

    private static void partition(
        Entries all, int level, long index, int lo, int hi, int blockIds, List<Block> out) {
      if (lo >= hi) {
        return;
      }
      if (hi - lo <= blockIds || level == 0) {
        out.add(new Block(level, index, new Run[] {Run.of(all.ts, all.rand, lo, hi)}, hi - lo));
        return;
      }
      long mid = (2 * index + 1) << (level - 1);
      int split = lo;
      while (split < hi && all.ts[split] >= mid) {
        split++;
      }
      partition(all, level - 1, 2 * index, split, hi, blockIds, out);
      partition(all, level - 1, 2 * index + 1, lo, split, blockIds, out);
    }

    /** Blocks {@code inside} (oldest first) merged into node {@code (level, index)}. */
    static Block merge(int level, long index, List<Block> inside, int count) {
      List<Run> runs = new ArrayList<>();
      for (int i = inside.size() - 1; i >= 0; i--) {
        runs.addAll(Arrays.asList(inside.get(i).runs));
      }
      return new Block(level, index, mergeHead(runs), count);
    }

    /** What a GC keeps of a block, and the newest id it dropped. */
    static final class Cut {
      final Block kept;
      final long newestDropped;

      Cut(Block kept, long newestDropped) {
        this.kept = kept;
        this.newestDropped = newestDropped;
      }
    }

    /**
     * The entries above {@code watermarkMs}, if the block holds some at or below it and its newest
     * is above it; else null.
     */
    Cut cutThrough(long watermarkMs) {
      List<Run> kept = new ArrayList<>();
      long newestDropped = Long.MIN_VALUE;
      for (Run run : runs) {
        if (run.maxTs <= watermarkMs) {
          newestDropped = run.maxTs;
          break;
        }
        Run.Cut cut = run.cutThrough(watermarkMs);
        if (cut == null) {
          kept.add(run);
          continue;
        }
        kept.addAll(Arrays.asList(cut.chunks));
        if (cut.newestDropped != Long.MIN_VALUE) {
          newestDropped = cut.newestDropped;
          break;
        }
      }
      if (newestDropped == Long.MIN_VALUE) {
        return null;
      }
      return new Cut(new Block(level, index, kept.toArray(new Run[0]), -1), newestDropped);
    }

    /** The digest a page of this block carries ({@link CommittedSetCodec#digest}). */
    byte[] digest() {
      byte[] d = digest;
      if (d == null) {
        byte[] packed = packed();
        d = CommittedSetCodec.digest(level, index, packed, 0, packed.length);
        digest = d;
      }
      return d;
    }

    /** The packed entries, as {@code CommittedBlock.packed_entries} carries them. */
    byte[] packed() {
      byte[] out = new byte[packedSize()];
      writePacked(out, 0);
      return out;
    }

    int messageSize() {
      byte[] buf = messageBuf;
      if (buf != null) {
        return messageEnd - messageOff;
      }
      int packed = packedSize();
      return (level != 0 ? 1 + varintSize(level) : 0) + (index != 0L ? 1 + varintSize(index) : 0)
          + 1 + varintSize(packed) + packed;
    }

    private int packedSize() {
      long size = varintSize(top() - runs[0].maxTs) + runs[0].end - runs[0].bodyOff;
      for (int i = 1; i < runs.length; i++) {
        Run run = runs[i];
        size += varintSize(runs[i - 1].minTs() - run.maxTs) + run.end - run.bodyOff;
      }
      if (size > Integer.MAX_VALUE - 32) {
        throw new IllegalStateException("A block of " + size + " bytes is too large");
      }
      return (int) size;
    }

    int writeMessage(byte[] out, int at) {
      byte[] buf = messageBuf;
      if (buf != null) {
        int len = messageEnd - messageOff;
        System.arraycopy(buf, messageOff, out, at, len);
        return at + len;
      }
      // A block made in memory writes its runs: the same copy as a cached message would cost,
      // without pinning the whole body a first write went into
      if (level != 0) {
        out[at++] = TAG_LEVEL;
        at = writeVarint(out, at, level);
      }
      if (index != 0L) {
        out[at++] = TAG_INDEX;
        at = writeVarint(out, at, index);
      }
      out[at++] = TAG_PACKED;
      at = writeVarint(out, at, packedSize());
      return writePacked(out, at);
    }

    private int writePacked(byte[] out, int at) {
      for (int i = 0; i < runs.length; i++) {
        Run run = runs[i];
        at = writeVarint(out, at, (i == 0 ? top() : runs[i - 1].minTs()) - run.maxTs);
        System.arraycopy(run.buf, run.bodyOff, out, at, run.end - run.bodyOff);
        at += run.end - run.bodyOff;
      }
      return at;
    }
  }

  /**
   * Merges the newest runs while the newest is at least as large as the next and both fit in a
   * run: the runs then grow like a binary counter, so an entry is copied a few times at most.
   */
  private static Run[] mergeHead(List<Run> runs) {
    while (runs.size() >= 2) {
      Run first = runs.get(0);
      Run second = runs.get(1);
      int firstBytes = first.end - first.off;
      int secondBytes = second.end - second.off;
      if (firstBytes < secondBytes || firstBytes + secondBytes > RUN_BYTES) {
        break;
      }
      runs.remove(0);
      runs.set(0, Run.concat(first, second));
    }
    return runs.toArray(new Run[0]);
  }

  /** Entries decoded from runs, newest first. */
  private static final class Entries {
    final long[] ts;
    final byte[][] rand;
    final int size;
    // How many of them were inherited (for a merge: the rest are additions)
    final int inherited;

    private Entries(long[] ts, byte[][] rand, int size, int inherited) {
      this.ts = ts;
      this.rand = rand;
      this.size = size;
      this.inherited = inherited;
    }

    static Entries of(List<Run> runs) {
      int n = 0;
      for (Run run : runs) {
        n += run.count();
      }
      long[] ts = new long[n];
      byte[][] rand = new byte[n][];
      int i = 0;
      for (Run run : runs) {
        long entryTs = run.maxTs;
        int[] at = {run.bodyOff};
        while (true) {
          ts[i] = entryTs;
          rand[i] = Arrays.copyOfRange(run.buf, at[0], at[0] + RANDOM_BYTES);
          i++;
          at[0] += RANDOM_BYTES;
          if (at[0] >= run.end) {
            break;
          }
          entryTs -= readVarint(run.buf, at, run.end);
        }
      }
      return new Entries(ts, rand, n, n);
    }

    /** {@code inherited} and sorted ids {@code [from, to)} merged newest first, copies once. */
    static Entries merge(Entries inherited, long[] ts, byte[][] rand, int from, int to) {
      int n = inherited.size + to - from;
      long[] outTs = new long[n];
      byte[][] outRand = new byte[n][];
      int a = 0;
      int b = from;
      int out = 0;
      int kept = 0;
      while (a < inherited.size || b < to) {
        int c;
        if (a >= inherited.size) {
          c = 1;
        } else if (b >= to) {
          c = -1;
        } else {
          c = Long.compare(ts[b], inherited.ts[a]);
          if (c == 0) {
            c = Arrays.compareUnsigned(rand[b], inherited.rand[a]);
          }
        }
        if (c < 0) {
          outTs[out] = inherited.ts[a];
          outRand[out++] = inherited.rand[a++];
          kept++;
        } else if (c > 0) {
          outTs[out] = ts[b];
          outRand[out++] = rand[b++];
        } else {
          // A copy of an id the block holds
          outTs[out] = inherited.ts[a];
          outRand[out++] = inherited.rand[a++];
          kept++;
          b++;
        }
      }
      return new Entries(outTs, outRand, out, kept);
    }
  }

  /**
   * Packed entries {@code [off, end)} of a buffer, never changed once made. The first entry's
   * timestamp is {@code maxTs}; its gap varint (at {@code off}) is from an entry or node top
   * before the run, if any, and is skipped: {@code bodyOff} is its random bytes.
   */
  private static final class Run {
    final byte[] buf;
    final int off;
    final int bodyOff;
    final int end;
    final long maxTs;
    // The oldest entry's timestamp and the entries, scanned on first use
    private volatile long minTs;
    private volatile int count;

    private Run(byte[] buf, int off, int end, long maxTs, long minTs, int count) {
      this.buf = buf;
      this.off = off;
      this.end = end;
      this.maxTs = maxTs;
      int[] at = {off};
      readVarint(buf, at, end);
      this.bodyOff = at[0];
      if (bodyOff + RANDOM_BYTES > end) {
        throw malformed("truncated entry");
      }
      this.minTs = minTs;
      this.count = count;
    }

    static Run slice(byte[] buf, int off, int end, long maxTs) {
      return new Run(buf, off, end, maxTs, Long.MIN_VALUE, -1);
    }

    /** The entries {@code [from, to)} of sorted arrays, as a run of their own. */
    static Run of(long[] ts, byte[][] rand, int from, int to) {
      int size = 0;
      for (int i = from; i < to; i++) {
        size += varintSize(i == from ? 0L : ts[i - 1] - ts[i]) + RANDOM_BYTES;
      }
      byte[] buf = new byte[size];
      int at = 0;
      for (int i = from; i < to; i++) {
        at = writeVarint(buf, at, i == from ? 0L : ts[i - 1] - ts[i]);
        System.arraycopy(rand[i], 0, buf, at, RANDOM_BYTES);
        at += RANDOM_BYTES;
      }
      return new Run(buf, 0, size, ts[from], ts[to - 1], to - from);
    }

    /** {@code newer} then {@code older}, whose entries are all older than {@code newer}'s. */
    static Run concat(Run newer, Run older) {
      long gap = newer.minTs() - older.maxTs;
      int olderBody = older.end - older.bodyOff;
      byte[] buf = new byte[newer.end - newer.off + varintSize(gap) + olderBody];
      System.arraycopy(newer.buf, newer.off, buf, 0, newer.end - newer.off);
      int at = writeVarint(buf, newer.end - newer.off, gap);
      System.arraycopy(older.buf, older.bodyOff, buf, at, olderBody);
      return new Run(buf, 0, buf.length, newer.maxTs, older.minTs(), newer.count() + older.count());
    }

    long minTs() {
      if (minTs == Long.MIN_VALUE) {
        scan();
      }
      return minTs;
    }

    int count() {
      if (count < 0) {
        scan();
      }
      return count;
    }

    /**
     * At least {@link #count()}: every entry after the first takes a gap varint and its random
     * bytes, 11 bytes or more.
     */
    int countAtMost() {
      int c = count;
      if (c >= 0) {
        return c;
      }
      return 1 + (end - bodyOff - RANDOM_BYTES) / (RANDOM_BYTES + 1);
    }

    private void scan() {
      long ts = maxTs;
      int n = 1;
      int[] at = {bodyOff + RANDOM_BYTES};
      while (at[0] < end) {
        ts -= readVarint(buf, at, end);
        at[0] += RANDOM_BYTES;
        n++;
        if (at[0] > end) {
          throw malformed("truncated entry");
        }
      }
      minTs = ts;
      count = n;
    }

    /**
     * Scans for an entry: 1 if the run holds it, -1 if the run passed below its timestamp (no
     * older entry anywhere holds it), 0 if every entry is at or above it.
     */
    int find(long ts, byte[] rand) {
      long entryTs = maxTs;
      int at = bodyOff;
      while (true) {
        if (entryTs == ts && matches(at, rand)) {
          return 1;
        }
        if (entryTs < ts) {
          return -1;
        }
        at += RANDOM_BYTES;
        if (at >= end) {
          return 0;
        }
        long gap = 0;
        int shift = 0;
        while (true) {
          int b = buf[at++] & 0xFF;
          gap |= ((long) (b & 0x7F)) << shift;
          if ((b & 0x80) == 0) {
            break;
          }
          shift += 7;
        }
        entryTs -= gap;
        if (at + RANDOM_BYTES > end) {
          throw malformed("truncated entry");
        }
      }
    }

    private boolean matches(int at, byte[] rand) {
      for (int i = 0; i < RANDOM_BYTES; i++) {
        if (buf[at + i] != rand[i]) {
          return false;
        }
      }
      return true;
    }

    /**
     * This run split at its first entry older than {@code ts}: the entries at or above it, and
     * the rest as a run (null if none).
     */
    Run[] splitBelow(long ts) {
      if (minTs != Long.MIN_VALUE && minTs >= ts) {
        return new Run[] {this, null};
      }
      long entryTs = maxTs;
      int[] at = {bodyOff + RANDOM_BYTES};
      while (at[0] < end) {
        int entry = at[0];
        long next = entryTs - readVarint(buf, at, end);
        if (next < ts) {
          return new Run[] {
            new Run(buf, off, entry, maxTs, entryTs, -1),
            new Run(buf, entry, end, next, minTs, -1)
          };
        }
        entryTs = next;
        at[0] += RANDOM_BYTES;
      }
      return new Run[] {this, null};
    }

    /** What a GC keeps of a run that holds entries above its watermark. */
    static final class Cut {
      final Run[] chunks;
      final long newestDropped;

      Cut(Run[] chunks, long newestDropped) {
        this.chunks = chunks;
        this.newestDropped = newestDropped;
      }
    }

    /**
     * The entries above {@code watermarkMs}, in chunks of about {@link #RUN_BYTES}, and the newest
     * dropped (Long.MIN_VALUE if none); null if the run drops nothing and needs no chunking. A
     * run's first entry is above the watermark.
     */
    Cut cutThrough(long watermarkMs) {
      if (minTs != Long.MIN_VALUE && minTs > watermarkMs && end - off <= RUN_BYTES) {
        return null;
      }
      List<Run> chunks = new ArrayList<>();
      int chunkStart = off;
      long chunkMax = maxTs;
      long entryTs = maxTs;
      int[] at = {bodyOff + RANDOM_BYTES};
      while (at[0] < end) {
        int entry = at[0];
        long next = entryTs - readVarint(buf, at, end);
        if (next <= watermarkMs) {
          chunks.add(new Run(buf, chunkStart, entry, chunkMax, entryTs, -1));
          return new Cut(chunks.toArray(new Run[0]), next);
        }
        if (entry - chunkStart >= RUN_BYTES) {
          chunks.add(new Run(buf, chunkStart, entry, chunkMax, entryTs, -1));
          chunkStart = entry;
          chunkMax = next;
        }
        entryTs = next;
        at[0] += RANDOM_BYTES;
      }
      if (chunks.isEmpty()) {
        // Nothing dropped, and the run fits in one chunk: it stays as it is, now scanned
        minTs = entryTs;
        return null;
      }
      chunks.add(new Run(buf, chunkStart, end, chunkMax, entryTs, -1));
      // Nothing dropped, but the run is now in chunks
      return new Cut(chunks.toArray(new Run[0]), Long.MIN_VALUE);
    }
  }

  /**
   * An inherited set plus the ids replay adds to it. The additions merge into the set when it is
   * built, costing the additions (and the entries above the oldest of them), not the set.
   */
  public static final class Builder {
    private CommittedSet base;
    private final Set<UUID> added = new HashSet<>();

    private Builder(CommittedSet base) {
      this.base = base;
    }

    /** Replaces the inherited set (a decoded checkpoint's); additions are kept. */
    public Builder inherit(byte[] bytes) {
      this.base = of(bytes, base.blockIds);
      return this;
    }

    public Builder add(UUID id) {
      if (!UuidV7.isV7(id)) {
        throw new IllegalArgumentException("Committed transaction IDs must be UUIDv7; got " + id);
      }
      added.add(id);
      return this;
    }

    /** {@link CommittedSet#contains}, counting the ids added so far; the floor is inherited. */
    public boolean contains(UUID id) {
      return added.contains(id) || base.holds(id) || base.notInline(id);
    }

    /** {@link CommittedSet#mayApply}, counting the ids added so far. */
    public boolean mayApply(UUID id) {
      return !added.contains(id) && !base.holds(id) && aboveFloor(id, base.floor);
    }

    public long horizon() {
      return base.horizon;
    }

    public long floor() {
      return base.floor;
    }

    /** The pages, oldest first. */
    public List<PageRef> pages() {
      return base.pages();
    }

    /**
     * Applies a {@code PageOut} record ({@link CommittedSet#pagedOut}): seals the oldest block, if
     * it is still the one the record names, counting the ids added so far.
     */
    public Builder pageOut(PageOut record) {
      this.base = build().pagedOut(record);
      return this;
    }

    /**
     * Applies a GC record ({@code AdvanceHorizon}): drops every page whose top is at or below
     * {@code watermarkMs} and every id at or below it, inherited or added, and raises the horizon
     * to the newest one dropped, and the floor to at least the horizon. A watermark at or below the
     * horizon drops nothing new.
     */
    public Builder advanceHorizon(long watermarkMs) {
      if (watermarkMs < 0L) {
        throw new IllegalArgumentException("watermarkMs must be >= 0, got " + watermarkMs);
      }
      if (watermarkMs > 0L) {
        this.base = build().dropThrough(watermarkMs);
        added.clear();
      }
      return this;
    }

    public CommittedSet build() {
      if (added.isEmpty()) {
        return base;
      }
      CommittedSet built = base.with(added);
      base = built;
      added.clear();
      return built;
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

  private static int writeVarint(byte[] buf, int at, long value) {
    while ((value & ~0x7FL) != 0) {
      buf[at++] = (byte) ((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    buf[at++] = (byte) value;
    return at;
  }

  private static int writeFixed64(byte[] buf, int at, long value) {
    for (int i = 0; i < 8; i++) {
      buf[at++] = (byte) (value & 0xFF);
      value >>>= 8;
    }
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
}
