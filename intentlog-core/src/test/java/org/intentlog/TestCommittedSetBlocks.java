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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.intentlog.wire.CommittedSetCodec;
import org.intentlog.wire.LogEnvelope;
import org.intentlog.wire.PageOut;
import org.intentlog.wire.PageRef;
import org.intentlog.wire.UuidV7;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * {@link CommittedSet} in blocks against a reference that encodes the whole set from scratch
 * ({@link CommittedSetCodec#encode(java.util.Collection, long, long, List, int)}): walks of
 * additions, builds, GCs, PageOuts and decodes, at several block sizes N, must give the same bytes
 * and the same answers after every step, whatever the history (a build after every id or after
 * many, decoded or folded). The reference's blocks are checked against the rule
 * (docs/COMMITTED_PAGING.md "Blocks: the index"): the maximal nodes holding at most N inline ids,
 * among the nodes holding any.
 *
 * <p>Each id's stamp is drawn against the marks of the set it meets (COMMITTED_PAGING.md
 * "Testing"): above the newest id, in the same ms as another, inside the span of the ids held (a
 * block's, often the newest's), at the floor or between the horizon and the floor, at the horizon
 * or below it, a copy of an id held, or far ahead of every clock. Replay adds an id only if the set
 * admits it (mayApply), as here; every drawn id is also asked about.
 *
 * <p>A PageOut is computed on an earlier version of the set than the one it is applied to, as
 * often as not, so stale PageOuts are the norm: one applies exactly when the reference's oldest
 * block is still the one its page holds (node, count, digest), and otherwise changes nothing. The
 * pages the walk wrote are its page store: each page a set references must be there, hold exactly
 * the ids that applied in its range, and answer every lookup of an id in (H, F] as the walk's
 * history does.
 */
class TestCommittedSetBlocks {

  private static final long FAR_AHEAD_MS = 365L * 24 * 3600 * 1000;

  /** A page the reference keeps: the reference to it, and the ids it holds. */
  private record Page(PageRef ref, Set<UUID> ids) {}

  /** The set as the format defines it: the inline ids, the pages, the floor, the horizon. */
  private static final class Reference {
    final int blockIds;
    final Set<UUID> ids = new HashSet<>();
    final List<Page> pages = new ArrayList<>();
    long horizon = 0L;
    long floor = 0L;

    Reference(int blockIds) {
      this.blockIds = blockIds;
    }

    boolean mayApply(UUID id) {
      return !ids.contains(id) && (floor == 0L || UuidV7.timestampMs(id) > floor);
    }

    void gc(long watermarkMs) {
      if (watermarkMs <= 0L) {
        return;
      }
      pages.removeIf(page -> {
        if (page.ref.top() <= watermarkMs) {
          horizon = Math.max(horizon, page.ref.maxTimestampMs());
          return true;
        }
        return false;
      });
      ids.removeIf(id -> {
        long ts = UuidV7.timestampMs(id);
        if (ts <= watermarkMs) {
          horizon = Math.max(horizon, ts);
          return true;
        }
        return false;
      });
      floor = Math.max(floor, horizon);
    }

    /**
     * Applies a PageOut: seals the oldest block if it is the record's (node, count, digest) and
     * not the newest. Returns whether it did.
     */
    boolean pageOut(PageOut record) {
      CommittedSetCodec.Decoded decoded = CommittedSetCodec.decodeBody(bytes());
      if (decoded.blocks.size() < 2) {
        return false;
      }
      long[] oldest = decoded.blocks.get(0);
      int level = (int) oldest[0];
      long index = oldest[1];
      long top = CommittedSetCodec.top(level, index);
      List<UUID> inside = new ArrayList<>();
      for (UUID id : ids) {
        if (UuidV7.timestampMs(id) <= top) {
          inside.add(id);
        }
      }
      if (level != record.level() || index != record.index() || inside.size() != record.count()
          || !record.hasDigest(digest(level, index, inside))) {
        return false;
      }
      long maxTs = inside.stream().mapToLong(UuidV7::timestampMs).max().getAsLong();
      pages.add(new Page(new PageRef(level, index, inside.size(), maxTs, record.digest()),
          new HashSet<>(inside)));
      ids.removeAll(inside);
      floor = top;
      return true;
    }

    byte[] bytes() {
      List<PageRef> refs = new ArrayList<>();
      for (Page page : pages) {
        refs.add(page.ref);
      }
      return CommittedSetCodec.encode(ids, horizon, floor, refs, blockIds);
    }
  }

  /**
   * A page's digest, computed apart from the codec: the block's entries newest first, each a
   * varint gap (the first from the node's top) and its 10 random bytes, in a {@code
   * CommittedBlock} message.
   */
  static byte[] digest(int level, long index, List<UUID> ids) {
    List<UUID> sorted = new ArrayList<>(ids);
    sorted.sort((a, b) -> {
      int c = Long.compare(UuidV7.timestampMs(b), UuidV7.timestampMs(a));
      return c != 0 ? c : Arrays.compareUnsigned(UuidV7.packRandomBits(b),
          UuidV7.packRandomBits(a));
    });
    ByteArrayOutputStream packed = new ByteArrayOutputStream();
    long prev = CommittedSetCodec.top(level, index);
    for (UUID id : sorted) {
      long ts = UuidV7.timestampMs(id);
      varint(packed, prev - ts);
      packed.writeBytes(UuidV7.packRandomBits(id));
      prev = ts;
    }
    ByteArrayOutputStream message = new ByteArrayOutputStream();
    if (level != 0) {
      message.write(0x08);
      varint(message, level);
    }
    if (index != 0L) {
      message.write(0x10);
      varint(message, index);
    }
    message.write(0x1a);
    varint(message, packed.size());
    message.writeBytes(packed.toByteArray());
    return Arrays.copyOf(sha256(message.toByteArray()), CommittedSetCodec.DIGEST_BYTES);
  }

  private static byte[] sha256(byte[] bytes) {
    try {
      return java.security.MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void varint(ByteArrayOutputStream out, long value) {
    while ((value & ~0x7FL) != 0) {
      out.write((int) ((value & 0x7F) | 0x80));
      value >>>= 7;
    }
    out.write((int) value);
  }

  private static final class Walk {
    static final UUID LOG = UUID.fromString("01900000-0000-7000-8000-00000000c0de");

    final Random random;
    final int blockIds;
    final Reference reference;
    final List<UUID> held = new ArrayList<>();
    // Every id the set ever admitted: what a page in (H, F] must hold exactly
    final Set<UUID> applied = new HashSet<>();
    // The pages computed so far (put before their PageOuts), by name
    final Map<String, byte[]> store = new HashMap<>();
    // Earlier versions of the set, which PageOuts are computed on
    final List<CommittedSet> versions = new ArrayList<>();
    long now = 1_700_000_000_000L;
    CommittedSet.Builder subject;
    int steps = 0;
    int pageOuts = 0;
    int sealed = 0;

    Walk(long seed, int blockIds) {
      this.random = new Random(seed);
      this.blockIds = blockIds;
      this.reference = new Reference(blockIds);
      this.subject = CommittedSet.builder(blockIds);
    }

    /** A stamp drawn against the marks of the set as it is. */
    UUID draw() {
      int region = random.nextInt(20);
      if (region < 9 || held.isEmpty()) {
        // Above the newest id: the usual case, in bursts and lulls
        now += random.nextInt(10) < 8 ? random.nextInt(3) : random.nextInt(5_000);
        return UuidV7.newUuidV7(now, random);
      }
      UUID some = held.get(random.nextInt(held.size()));
      long someTs = UuidV7.timestampMs(some);
      switch (region) {
        case 9:
        case 10:
          // In the same ms as an id held
          return UuidV7.newUuidV7(someTs, random);
        case 11:
        case 12:
        case 13: {
          // Inside the span of the ids held: a late commit, landing in an older block
          long oldest = Math.max(reference.horizon + 1, now - 20_000);
          return UuidV7.newUuidV7(oldest + (long) (random.nextDouble() * (now - oldest)), random);
        }
        case 14:
          // At the horizon, at the floor, or between them
          switch (random.nextInt(3)) {
            case 0:
              return UuidV7.newUuidV7(Math.max(1, reference.horizon), random);
            case 1:
              return UuidV7.newUuidV7(Math.max(1, reference.floor), random);
            default:
              return UuidV7.newUuidV7(reference.horizon + 1
                  + (long) (random.nextDouble() * Math.max(0, reference.floor - reference.horizon)),
                  random);
          }
        case 15:
          // Below the horizon
          return UuidV7.newUuidV7(Math.max(1, reference.horizon - 1 - random.nextInt(1_000)),
              random);
        case 16:
        case 17:
          // A copy of an id held
          return some;
        case 18:
          // Far ahead of every clock (errata D21): admitted; it stays until the clocks reach it
          return UuidV7.newUuidV7(now + FAR_AHEAD_MS + random.nextInt(1_000), random);
        default:
          // Just above the newest id held
          return UuidV7.newUuidV7(Math.max(now, someTs) + 1, random);
      }
    }

    void step() {
      steps++;
      int op = random.nextInt(100);
      if (op < 4) {
        versions.add(subject.build());
        if (versions.size() > 8) {
          versions.remove(0);
        }
      } else if (op < 12) {
        pageOut();
      } else if (op < 70) {
        // Mostly one id; now and then a batch, of up to a few blocks' worth at small N
        int count = 1 + (random.nextInt(4) == 0 ? random.nextInt(Math.min(4 * blockIds + 2, 96)) : 0);
        for (int i = 0; i < count; i++) {
          UUID id = draw();
          ask(id);
          if (reference.mayApply(id)) {
            assertThat(subject.mayApply(id)).as("admits %s", id).isTrue();
            subject.add(id);
            reference.ids.add(id);
            applied.add(id);
            held.add(id);
          } else {
            assertThat(subject.mayApply(id)).as("refuses %s", id).isFalse();
          }
        }
      } else if (op < 85) {
        check(subject.build());
      } else if (op < 95) {
        long watermark = watermark();
        subject.advanceHorizon(watermark);
        reference.gc(watermark);
        held.removeIf(id -> !reference.ids.contains(id));
        check(subject.build());
      } else {
        // A fresh read: decode what the set encodes, and carry on from there
        byte[] bytes = subject.build().bytes();
        subject = CommittedSet.of(bytes, blockIds).toBuilder();
        check(subject.build());
      }
    }

    /**
     * Seals the oldest block of the set as it is, or as an earlier version had it (a compactor
     * that read before the records since landed): puts its page, then applies its PageOut to the
     * subject and the reference, which must agree whether it applies.
     */
    void pageOut() {
      CommittedSet on = versions.isEmpty() || random.nextBoolean()
          ? subject.build() : versions.get(random.nextInt(versions.size()));
      long ceiling = random.nextInt(4) == 0 ? now - random.nextInt(20_000) : Long.MAX_VALUE;
      CommittedSet.Seal seal = on.sealable(ceiling);
      if (seal == null) {
        return;
      }
      byte[] page = seal.page(LOG);
      byte[] earlier = store.putIfAbsent(seal.name(), page);
      if (earlier != null) {
        assertThat(page).as("two puts of page %s", seal.name()).isEqualTo(earlier);
      }
      pageOuts++;
      byte[] before = subject.build().bytes();
      subject.pageOut(seal.record());
      boolean applies = reference.pageOut(seal.record());
      CommittedSet after = subject.build();
      if (applies) {
        sealed++;
        assertThat(after.pages()).last().isEqualTo(seal.ref());
        assertThat(after.floor()).isEqualTo(seal.record().top());
      } else {
        assertThat(after.bytes()).as("a refused PageOut changes nothing").isEqualTo(before);
      }
      held.removeIf(id -> !reference.ids.contains(id));
      check(after);
    }

    long watermark() {
      if (!reference.pages.isEmpty() && random.nextInt(4) == 0) {
        // At a page's top, inside one, or just below one
        PageRef page = reference.pages.get(random.nextInt(reference.pages.size())).ref;
        switch (random.nextInt(3)) {
          case 0:
            return page.top();
          case 1:
            return page.maxTimestampMs();
          default:
            return page.top() - 1;
        }
      }
      if (held.isEmpty()) {
        return now - random.nextInt(1_000);
      }
      UUID some = held.get(random.nextInt(held.size()));
      switch (random.nextInt(4)) {
        case 0:
          return Math.max(1, reference.horizon - random.nextInt(10)); // drops nothing new
        case 1:
          return UuidV7.timestampMs(some); // at an id
        case 2:
          return UuidV7.timestampMs(some) - 1; // just below one
        default:
          return now - random.nextInt(30_000); // somewhere in the recent past
      }
    }

    /**
     * The builder answers as the reference does, for an id drawn at any mark, without building
     * (so that builds still fall after many additions).
     */
    void ask(UUID id) {
      lookup(id, subject::contains, reference.ids.contains(id));
      assertThat(subject.mayApply(id)).isEqualTo(reference.mayApply(id));
    }

    /**
     * The tri-state lookup, with pages: inline ids above the floor; in (H, F], the page that holds
     * the id if it applied (it must, exactly when it applied), or false where no page can; at or
     * below the horizon, unknown.
     */
    void lookup(UUID id, java.util.function.Predicate<UUID> contains, boolean inline) {
      long ts = UuidV7.timestampMs(id);
      if (inline || reference.floor == 0L || ts > reference.floor) {
        assertThat(contains.test(id)).as("contains %s", id).isEqualTo(inline);
      } else if (reference.horizon != 0L && ts <= reference.horizon) {
        assertThatThrownBy(() -> contains.test(id))
            .isInstanceOf(DedupHorizonExceededException.class);
      } else {
        boolean answer;
        try {
          answer = contains.test(id);
          assertThat(answer).as("no page can hold %s", id).isFalse();
        } catch (PagedOutException e) {
          byte[] body = store.get(e.page().name());
          assertThat(body).as("page %s exists", e.page().name()).isNotNull();
          LogEnvelope.Page page = LogEnvelope.decodePage(body);
          assertThat(page.logId()).isEqualTo(LOG);
          assertThat(e.page().hasDigest(page.digest())).isTrue();
          assertThat(e.page().top()).isGreaterThanOrEqualTo(ts);
          answer = CommittedSetCodec.holds(page.level(), page.index(), page.packedEntries(), id);
        }
        assertThat(answer).as("the page answers for %s as the history does", id)
            .isEqualTo(applied.contains(id));
      }
    }

    void check(CommittedSet built) {
      byte[] expected = reference.bytes();
      assertThat(built.bytes()).as("bytes after %d steps", steps).isEqualTo(expected);
      assertThat(built.encodedSize()).isEqualTo(expected.length);
      assertThat(built.horizon()).isEqualTo(reference.horizon);
      assertThat(built.floor()).isEqualTo(reference.floor);
      assertThat(built.ids()).containsExactlyInAnyOrderElementsOf(reference.ids);
      followsTheRule(expected, blockIds);
      // Each page referenced exists, and holds exactly the ids that applied in its range
      long below = reference.horizon;
      for (Page page : reference.pages) {
        byte[] body = store.get(page.ref.name());
        assertThat(body).as("page %s exists", page.ref.name()).isNotNull();
        LogEnvelope.Page decoded = LogEnvelope.decodePage(body);
        assertThat(CommittedSetCodec.decodeEntries(decoded.level(), decoded.index(),
            decoded.packedEntries())).containsExactlyInAnyOrderElementsOf(page.ids);
        long top = page.ref.top();
        for (UUID id : applied) {
          long ts = UuidV7.timestampMs(id);
          if (ts > below && ts <= top) {
            assertThat(page.ids).as("page %s holds %s, which applied in its range",
                page.ref.name(), id).contains(id);
          }
        }
        below = top;
      }
      // The built set answers as the reference does: ids held, and ids drawn at every mark
      for (int i = 0; i < 16; i++) {
        UUID id = i % 2 == 0 && !held.isEmpty() ? held.get(random.nextInt(held.size())) : draw();
        lookup(id, built::contains, reference.ids.contains(id));
        assertThat(built.mayApply(id)).isEqualTo(reference.mayApply(id));
      }
      // The encoding decodes to a set that encodes the same
      assertThat(CommittedSet.of(expected, blockIds).toBuilder().build().bytes())
          .isEqualTo(expected);
    }
  }

  /**
   * The blocks of a body are the rule's: each holds at least one id and at most N (or is a
   * millisecond), and its parent node holds more than N of the set's ids (or it is the root).
   */
  static void followsTheRule(byte[] body, int blockIds) {
    CommittedSetCodec.Decoded decoded = CommittedSetCodec.decodeBody(body);
    long[] ts = new long[decoded.ids.size()];
    for (int i = 0; i < ts.length; i++) {
      ts[i] = UuidV7.timestampMs(decoded.ids.get(i));
    }
    Arrays.sort(ts);
    for (long[] block : decoded.blocks) {
      int level = (int) block[0];
      long index = block[1];
      assertThat(block[2]).isPositive();
      if (level > 0) {
        assertThat(block[2]).as("block (%d, %d) holds at most N", level, index)
            .isLessThanOrEqualTo(blockIds);
      }
      if (level < CommittedSetCodec.MAX_LEVEL) {
        int parentLevel = level + 1;
        long parentIndex = index >> 1;
        long start = parentIndex << parentLevel;
        long top = CommittedSetCodec.top(parentLevel, parentIndex);
        long inParent = upperBound(ts, top) - upperBound(ts, start - 1);
        assertThat(inParent).as("block (%d, %d) is maximal", level, index)
            .isGreaterThan(blockIds);
      }
    }
  }

  /** How many of sorted {@code ts} are at or below {@code value}. */
  private static int upperBound(long[] ts, long value) {
    int lo = 0;
    int hi = ts.length;
    while (lo < hi) {
      int mid = (lo + hi) >>> 1;
      if (ts[mid] <= value) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  static Stream<Arguments> walks() {
    List<Arguments> walks = new ArrayList<>();
    for (int blockIds : new int[] {1, 2, 3, 8, 64, 1 << 16}) {
      for (long seed = 1; seed <= 12; seed++) {
        walks.add(Arguments.of(blockIds, seed * 7919 + blockIds));
      }
    }
    return walks.stream();
  }

  @ParameterizedTest(name = "N={0} seed={1}")
  @MethodSource("walks")
  void blocksReadAndEncodeAsTheWholeSet(int blockIds, long seed) {
    Walk walk = new Walk(seed, blockIds);
    for (int i = 0; i < 400; i++) {
      walk.step();
    }
    walk.check(walk.subject.build());
  }

  @Test
  void walksSealBlocksAndRefuseStalePageOuts() {
    int pageOuts = 0;
    int sealed = 0;
    for (long seed = 1; seed <= 12; seed++) {
      Walk walk = new Walk(seed * 7919 + 2, 2);
      for (int i = 0; i < 400; i++) {
        walk.step();
      }
      pageOuts += walk.pageOuts;
      sealed += walk.sealed;
    }
    // The calibration's counts (N = 2): 231 PageOuts, 121 applied
    assertThat(sealed).as("PageOuts that applied").isGreaterThan(50);
    assertThat(pageOuts - sealed).as("PageOuts refused").isGreaterThan(50);
  }

  /**
   * Replay never seals the newest block, whatever a PageOut says: a compactor's fast clock, or a
   * bug, would otherwise raise the floor past the ids being minted now. A compactor never proposes
   * it ({@code sealable}), so the PageOut here is made by hand, with the block's true node, count
   * and digest.
   */
  @Test
  void theNewestBlockIsNeverSealed() {
    CommittedSet.Builder set = CommittedSet.builder(4);
    Random random = new Random(17);
    List<UUID> ids = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      UUID id = UuidV7.newUuidV7(1_700_000_000_000L + 10L * i, random);
      ids.add(id);
      set.add(id);
    }
    CommittedSet one = set.build();
    assertThat(one.blockCount()).isOne();
    assertThat(one.sealable(Long.MAX_VALUE)).isNull();
    long[] node = CommittedSetCodec.decodeBody(one.bytes()).blocks.get(0);
    PageOut record = new PageOut((int) node[0], node[1], ids.size(),
        digest((int) node[0], node[1], ids));
    assertThat(one.pagedOut(record).bytes()).isEqualTo(one.bytes());
    // With a second block, the newest is not the oldest, which is the only one a PageOut seals
    for (int i = 0; i < 6; i++) {
      set.add(UuidV7.newUuidV7(1_700_000_000_000L + 100_000L + i, random));
    }
    CommittedSet two = set.build();
    assertThat(two.blockCount()).isGreaterThan(1);
    CommittedSet.Seal oldest = two.sealable(Long.MAX_VALUE);
    assertThat(two.pagedOut(oldest.record()).floor()).isEqualTo(oldest.record().top());
    CommittedSet sealed = two.pagedOut(oldest.record());
    if (sealed.blockCount() == 1) {
      long[] last = CommittedSetCodec.decodeBody(sealed.bytes()).blocks.get(0);
      List<UUID> rest = new ArrayList<>(sealed.ids());
      PageOut newest = new PageOut((int) last[0], last[1], rest.size(),
          digest((int) last[0], last[1], rest));
      assertThat(sealed.pagedOut(newest).bytes()).isEqualTo(sealed.bytes());
    }
  }

  @Test
  void anIdOutsideEveryBlockOpensTheLargestFreeNode() {
    // N = 2: three ids far apart split the root; one between them, and then one beside each,
    // open the largest nodes that hold them and overlap no block, or split the block they join
    CommittedSet.Builder set = CommittedSet.builder(2);
    long base = 1L << 40;
    Random random = new Random(3);
    List<UUID> ids = new ArrayList<>();
    for (long ts : new long[] {base, base + (1L << 20), base + (1L << 30), base + (1L << 25),
        base + 1, base + (1L << 25) + 1, base + (1L << 30) + 7}) {
      UUID id = UuidV7.newUuidV7(ts, random);
      ids.add(id);
      set.add(id);
      CommittedSet built = set.build();
      assertThat(built.bytes()).isEqualTo(CommittedSetCodec.encode(ids, 0L, 0L, 2));
      followsTheRule(built.bytes(), 2);
    }
  }

  @Test
  void aMillisecondIsOneBlockWhateverItHolds() {
    CommittedSet.Builder set = CommittedSet.builder(2);
    Random random = new Random(5);
    List<UUID> ids = new ArrayList<>();
    for (int i = 0; i < 9; i++) {
      UUID id = UuidV7.newUuidV7(1_700_000_000_000L, random);
      ids.add(id);
      set.add(id);
    }
    CommittedSet built = set.build();
    assertThat(built.blockCount()).isOne();
    assertThat(CommittedSetCodec.decodeBody(built.bytes()).blocks.get(0)[0]).isZero();
    for (UUID id : ids) {
      assertThat(built.contains(id)).isTrue();
    }
    assertThat(built.bytes()).isEqualTo(CommittedSetCodec.encode(ids, 0L, 0L, 2));
  }

  @Test
  void aGcMergesTheOldestBlocks() {
    // N = 2: x1 and x2 one ms apart, y 1,024 ms later, in the node (11, base / 2^11) that holds
    // all three. That node holds more than N, so its halves are the blocks: {x1, x2} and {y}.
    // A GC that drops x1 leaves two ids, which every node holding both holds: the root.
    long base = 1L << 40;
    Random random = new Random(11);
    Reference reference = new Reference(2);
    CommittedSet.Builder set = CommittedSet.builder(2);
    for (long ts : new long[] {base, base + 1, base + 1024}) {
      UUID id = UuidV7.newUuidV7(ts, random);
      set.add(id);
      reference.ids.add(id);
    }
    CommittedSet before = set.build();
    assertThat(before.bytes()).isEqualTo(reference.bytes());
    assertThat(before.blockCount()).isEqualTo(2);
    List<long[]> blocks = CommittedSetCodec.decodeBody(before.bytes()).blocks;
    assertThat(blocks.get(0)[0]).isEqualTo(10L);
    assertThat(blocks.get(1)[0]).isEqualTo(10L);

    set.advanceHorizon(base);
    reference.gc(base);
    CommittedSet after = set.build();
    assertThat(after.bytes()).isEqualTo(reference.bytes());
    assertThat(after.blockCount()).isOne();
    assertThat(CommittedSetCodec.decodeBody(after.bytes()).blocks.get(0)[0])
        .isEqualTo(CommittedSetCodec.MAX_LEVEL);
    assertThat(after.horizon()).isEqualTo(base);
    followsTheRule(after.bytes(), 2);
  }
}
