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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.intentlog.CommitOutcomeUnknownException;
import org.intentlog.CommitRejectedException;
import org.intentlog.CommittedSet;
import org.intentlog.CompactionPolicy;
import org.intentlog.DedupHorizonExceededException;
import org.intentlog.LogRuntime;
import org.intentlog.Replay;
import org.intentlog.Replica;
import org.intentlog.RuntimeConfig;
import org.intentlog.memory.InMemorySharedLog.PageFault;
import org.intentlog.memory.InMemorySharedLog.Request;
import org.intentlog.memory.InMemorySharedLog.WriteFault;
import org.intentlog.testkit.EntityVersionsMachine;
import org.intentlog.testkit.EntityVersionsMachine.State;
import org.intentlog.testkit.EntityVersionsMachine.Txn;
import org.intentlog.wire.CommittedSetCodec;
import org.intentlog.wire.LogEnvelope;
import org.intentlog.wire.PageRef;
import org.intentlog.wire.UuidV7;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Paging the committed set on a shared log (docs/COMMITTED_PAGING.md): {@link LogRuntime#compact}
 * seals the oldest blocks older than the admission window into pages, lookups of their ids read the
 * pages, a GC record drops them and its compactor deletes them, and a sweep deletes the orphans
 * below the floor. Every id is stamped by the runtime's clock, which each step sets, so the stamps
 * fall where the test means them to. Blocks hold at most 16 ids, as surefire sets them.
 */
class TestSharedLogPaging {

  private static final EntityVersionsMachine MACHINE = EntityVersionsMachine.INSTANCE;
  private static final CompactionPolicy NEVER = log -> false;
  private static final long HOUR = 3_600_000L;
  /** The admission window: a compaction seals only blocks whose ids are all older. */
  private static final long A = 1_000L;
  private static final long T0 = 1_800_000_000_000L;

  /** Hooks: page faults, and other writers' requests before the writer's next append lands. */
  private final class Hooks implements InMemorySharedLog.Interceptor {
    Runnable beforeNextAppend = null;
    Runnable afterNextAppend = null;
    WriteFault nextAppend = WriteFault.NONE;
    Runnable beforeNextGet = null;
    PageFault gets = PageFault.NONE;
    boolean active = true;

    @Override
    public WriteFault beforeWrite(Request request, byte[] payload) {
      if (!active || request != Request.APPEND
          || !LogEnvelope.decodeRecord(payload).isIntention()) {
        return WriteFault.NONE;
      }
      if (beforeNextAppend != null) {
        Runnable other = beforeNextAppend;
        beforeNextAppend = null;
        others(other);
      }
      WriteFault fault = nextAppend;
      nextAppend = WriteFault.NONE;
      return fault;
    }

    @Override
    public void afterLanding(Request request) {
      if (active && request == Request.APPEND && afterNextAppend != null) {
        Runnable other = afterNextAppend;
        afterNextAppend = null;
        others(other);
      }
    }

    @Override
    public PageFault beforePage(Request request, String name) {
      if (!active || request != Request.GET_PAGE) {
        return PageFault.NONE;
      }
      if (beforeNextGet != null) {
        Runnable other = beforeNextGet;
        beforeNextGet = null;
        others(other);
      }
      return gets;
    }

    void others(Runnable requests) {
      boolean was = active;
      active = false;
      try {
        requests.run();
      } finally {
        active = was;
      }
    }
  }

  private final InMemorySharedLog log = new InMemorySharedLog("log");
  private final Hooks hooks = new Hooks();
  private final AtomicLong clock = new AtomicLong(T0 - 10_000L);

  private RuntimeConfig config(long retentionMs) {
    return new RuntimeConfig(4, 3, 0L, 0L, retentionMs, NEVER, Duration.ZERO, clock::get, A, 0L);
  }

  private LogRuntime<State, EntityVersionsMachine.Builder, Txn> runtime(long retentionMs) {
    return new LogRuntime<>(log, MACHINE, config(retentionMs));
  }

  private final LogRuntime<State, EntityVersionsMachine.Builder, Txn> writer = runtime(HOUR);

  @BeforeEach
  void create() {
    assumeThat(CommittedSetCodec.BLOCK_IDS).as("blocks of 16 ids (surefire)").isEqualTo(16);
    log.intercept(hooks);
    writer.bootstrap(new Txn("init", Map.of(), Set.of()));
  }

  /** Another writer's commit of {@code tag}, its id stamped at {@code ts}. */
  private Replica<State> commitAt(long ts, String tag) {
    clock.set(ts);
    Replica<State> on = writer.read();
    return writer.commit(on, Txn.on(on.state(), tag, Set.of(), Set.of(tag)));
  }

  /** Commits {@code n} records 10 ms apart from {@code from}, tagged {@code prefix}0, 1, ... */
  private void commits(long from, int n, String prefix) {
    for (int i = 0; i < n; i++) {
      commitAt(from + 10L * i, prefix + i);
    }
  }

  /** A compaction at {@code now} with retention {@code retentionMs}. */
  private Replica<State> compactAt(long now, long retentionMs) {
    clock.set(now);
    return runtime(retentionMs).compact();
  }

  private UUID idOf(String tag) {
    for (byte[] record : log.records()) {
      LogEnvelope.Record decoded = LogEnvelope.decodeRecord(record);
      if (decoded.isIntention() && MACHINE.decodeIntention(decoded.intention()).tag().equals(tag)) {
        return decoded.id();
      }
    }
    throw new AssertionError("no record of " + tag);
  }

  private byte[] recordOf(String tag) {
    for (byte[] record : log.records()) {
      LogEnvelope.Record decoded = LogEnvelope.decodeRecord(record);
      if (decoded.isIntention() && MACHINE.decodeIntention(decoded.intention()).tag().equals(tag)) {
        return record;
      }
    }
    throw new AssertionError("no record of " + tag);
  }

  /** The state a replay of every record ever appended reaches: what no trim can hide. */
  private State replayed() {
    return Replay.replay(MACHINE, log.checkpoints().get(0L), log.records(), null)
        .builder().build();
  }

  /** The ids a page holds. */
  private List<UUID> idsOf(PageRef page) {
    LogEnvelope.Page body = LogEnvelope.decodePage(log.storedPages().get(page.name()));
    return CommittedSetCodec.decodeEntries(body.level(), body.index(), body.packedEntries());
  }

  /** 40 commits from T0, then a compaction an hour's retention keeps them all through. */
  private Replica<State> paged() {
    commits(T0, 40, "t");
    return compactAt(T0 + 10_000L, HOUR);
  }

  @Test
  void aCompactionPagesTheBlocksOlderThanTheAdmissionWindow() {
    Replica<State> r = paged();
    CommittedSet set = r.committed();
    assertThat(set.pages()).isNotEmpty();
    assertThat(set.horizon()).isZero();
    assertThat(set.floor()).isEqualTo(set.pages().get(set.pages().size() - 1).top());
    // Every block was sealed but the newest, whose top is above every id
    assertThat(set.sealable(Long.MAX_VALUE)).isNull();
    // Each page exists, and the pages and the inline set hold every id once
    assertThat(log.storedPages().keySet())
        .containsExactlyInAnyOrderElementsOf(set.pages().stream().map(PageRef::name).toList());
    List<UUID> all = new ArrayList<>(set.ids());
    for (PageRef page : set.pages()) {
      List<UUID> ids = idsOf(page);
      assertThat(ids).hasSize(page.count());
      ids.forEach(id -> assertThat(UuidV7.timestampMs(id)).isLessThanOrEqualTo(page.top()));
      all.addAll(ids);
    }
    List<UUID> expected = new ArrayList<>(List.of(idOf("init")));
    for (int i = 0; i < 40; i++) {
      expected.add(idOf("t" + i));
    }
    assertThat(all).containsExactlyInAnyOrderElementsOf(expected);
    // The checkpoint holds the references, not the ids, and is the replay of the log (I11)
    byte[] checkpoint = log.checkpoints().get(log.checkpoints().keySet().stream()
        .max(Long::compare).orElseThrow());
    assertThat(LogEnvelope.decodeCheckpoint(checkpoint).committed()).isEqualTo(set.bytes());
    assertThat(log.violations()).isEmpty();
  }

  @Test
  void aCompactionLeavesBlocksYoungerThanTheAdmissionWindowInline() {
    commits(T0, 40, "t");
    long now = T0 + 1_200L;
    Replica<State> r = compactAt(now, HOUR);
    assertThat(r.committed().pages()).isNotEmpty();
    for (PageRef page : r.committed().pages()) {
      assertThat(page.top()).isLessThanOrEqualTo(now - A);
    }
    // The oldest inline block was too young to seal: its top is past the ceiling
    CommittedSet.Seal next = r.committed().sealable(Long.MAX_VALUE);
    assertThat(next).isNotNull();
    assertThat(next.record().top()).isGreaterThan(now - A);
    assertThat(r.committed().ids()).contains(idOf("t39"), idOf("t20"));
  }

  @Test
  void aWriterWhoseClockIsBehindTheFloorStillCommits() {
    Replica<State> r = paged();
    clock.set(r.committed().floor() - 5_000L); // a clock behind the floor it read
    Replica<State> on = writer.read();
    Replica<State> landed =
        writer.commit(on, Txn.on(on.state(), "behind", Set.of(), Set.of("behind")));
    assertThat(landed.state().history()).contains("behind");
    assertThat(UuidV7.timestampMs(idOf("behind"))).isGreaterThan(r.committed().floor());
  }

  @Test
  void aLookupOfAPagedIdReadsItsPage() {
    Replica<State> r = paged();
    for (int i = 0; i < 40; i++) {
      assertThat(writer.committed(r, idOf("t" + i))).as("t%d", i).isTrue();
    }
    // An id that never committed, in a page's range or above every id, is absent
    UUID never = UuidV7.newUuidV7(T0 + 15L, new java.util.Random(1));
    assertThat(writer.committed(r, never)).isFalse();
    assertThat(writer.committed(r, UuidV7.newUuidV7After(T0 + 50_000L))).isFalse();
    // A page that cannot be read fails the lookup: never "absent"
    hooks.gets = PageFault.FAIL;
    LogRuntime<State, EntityVersionsMachine.Builder, Txn> fresh = runtime(HOUR);
    assertThatThrownBy(() -> fresh.committed(r, idOf("t0")))
        .isInstanceOf(java.io.UncheckedIOException.class);
  }

  @Test
  void aCopyOfAPagedRecordIsRejectedWithoutBeingJudged() {
    Replica<State> r = paged();
    byte[] original = recordOf("t0");
    assertThat(UuidV7.timestampMs(idOf("t0"))).isLessThanOrEqualTo(r.committed().floor());
    log.append(original, null);
    Replica<State> after = writer.read();
    assertThat(after.state()).isEqualTo(r.state());
    assertThat(after.committed().bytes()).isEqualTo(r.committed().bytes());
    assertThat(replayed().history()).filteredOn("t0"::equals).hasSize(1);
  }

  @Test
  void aCommitSlowerThanTheAdmissionWindowIsRejectedBeyondTheFloor() {
    commits(T0, 20, "t");
    clock.set(T0 + 55L); // stamped among the first ids: in a block that will be sealed
    Replica<State> base = writer.read();
    hooks.beforeNextAppend = () -> {
      commits(T0 + 1_000L, 40, "u");
      compactAt(T0 + 100_000L, HOUR);
    };
    assertThatThrownBy(() -> writer.commit(base, Txn.on(base.state(), "slow", Set.of(),
        Set.of("slow"))))
        .isInstanceOfSatisfying(CommitRejectedException.class, e ->
            assertThat(e.reason()).isEqualTo(CommitRejectedException.Reason.BEYOND_FLOOR));
    assertThat(replayed().history()).doesNotContain("slow");
    assertThat(writer.read().state().history()).doesNotContain("slow");
  }

  @Test
  void aCommitWhoseIdWasPagedBeforeItsReadBackIsFoundInThePage() {
    commits(T0, 20, "t");
    clock.set(T0 + 55L);
    Replica<State> base = writer.read();
    hooks.nextAppend = WriteFault.LOST_REPLY;
    hooks.afterNextAppend = () -> {
      commits(T0 + 1_000L, 40, "u");
      compactAt(T0 + 100_000L, HOUR);
    };
    Replica<State> landed =
        writer.commit(base, Txn.on(base.state(), "w", Set.of(), Set.of("w")));
    assertThat(landed.state().history()).contains("w");
    assertThat(UuidV7.timestampMs(idOf("w"))).isLessThanOrEqualTo(landed.committed().floor());
    assertThat(replayed().history()).filteredOn("w"::equals).hasSize(1);
  }

  @Test
  void aPageThatCannotBeReadLeavesAnAmbiguousCommitUnknown() {
    commits(T0, 20, "t");
    clock.set(T0 + 55L);
    Replica<State> base = writer.read();
    hooks.nextAppend = WriteFault.LOST_REPLY;
    hooks.afterNextAppend = () -> {
      commits(T0 + 1_000L, 40, "u");
      compactAt(T0 + 100_000L, HOUR);
    };
    hooks.gets = PageFault.FAIL;
    assertThatThrownBy(() -> writer.commit(base, Txn.on(base.state(), "w", Set.of(),
        Set.of("w"))))
        .isInstanceOf(CommitOutcomeUnknownException.class);
    assertThat(replayed().history()).filteredOn("w"::equals).hasSize(1);
  }

  @Test
  void aPageDeletedSinceTheReadSendsTheLookupToAFreshRead() {
    commits(T0, 20, "t");
    clock.set(T0 + 55L);
    Replica<State> base = writer.read();
    hooks.nextAppend = WriteFault.LOST_REPLY;
    hooks.afterNextAppend = () -> {
      commits(T0 + 1_000L, 40, "u");
      compactAt(T0 + 100_000L, HOUR);
    };
    // Between the writer's read and its get of the page, GC drops the page and its compactor
    // deletes it: the get finds none, and a fresh read finds the id below the horizon
    hooks.beforeNextGet = () -> compactAt(T0 + 100_000L + HOUR, HOUR);
    assertThatThrownBy(() -> writer.commit(base, Txn.on(base.state(), "w", Set.of(),
        Set.of("w"))))
        .isInstanceOf(CommitOutcomeUnknownException.class)
        .hasRootCauseInstanceOf(java.io.IOException.class);
    assertThat(writer.read().committed().horizon())
        .isGreaterThanOrEqualTo(UuidV7.timestampMs(idOf("w")));
  }

  @Test
  void aGcRecordDropsPagesAndItsCompactorDeletesThem() {
    Replica<State> r = paged();
    List<PageRef> pages = r.committed().pages();
    PageRef newest = pages.get(pages.size() - 1);
    Replica<State> gc = compactAt(T0 + 10_000L + HOUR, HOUR);
    assertThat(gc.committed().pages()).isEmpty();
    assertThat(gc.committed().horizon()).isGreaterThanOrEqualTo(newest.maxTimestampMs());
    assertThat(log.storedPages()).isEmpty();
    // A replica from before the GC names a page that is gone: the lookup reads afresh, and finds
    // the id at or below the horizon
    assertThatThrownBy(() -> writer.committed(r, idOf("t0")))
        .isInstanceOf(DedupHorizonExceededException.class);
  }

  /**
   * A page that only a PageOut after the checkpoint references (another compactor's, which has not
   * checkpointed yet) is deleted by the compaction whose GC record drops it: the pages it deletes
   * are those its replay saw a GC record drop, not those its checkpoint named (found by
   * CommitProtocol's oracle that a compaction deletes the pages its GC record dropped).
   */
  @Test
  void aCompactionDeletesAPageItsGcDropsThoughOnlyARecordNamesIt() {
    commits(T0, 40, "t");
    Replica<State> before = writer.read();
    CommittedSet.Seal seal = before.committed().sealable(T0 + 10_000L);
    assertThat(seal).isNotNull();
    // Another compactor's first half: the page put, its PageOut appended, no checkpoint yet
    log.pages().put(seal.name(), seal.page(before.logId()));
    log.append(LogEnvelope.encodePageOut(seal.record()), null);
    assertThat(writer.read().committed().pages()).containsExactly(seal.ref());
    assertThat(LogEnvelope.decodeCheckpoint(log.checkpoints().get(0L)).committed())
        .as("the checkpoint names no page").isEqualTo(CommittedSet.EMPTY.bytes());
    Replica<State> gc = compactAt(T0 + 10_000L + HOUR, HOUR);
    assertThat(gc.committed().pages()).isEmpty();
    assertThat(log.storedPages()).isEmpty();
  }

  @Test
  void aGcRecordKeepsAPageThatStraddlesItsWatermark() {
    Replica<State> r = paged();
    PageRef first = r.committed().pages().get(0);
    // A watermark inside the first page: no page's top is at or below it
    long watermark = first.maxTimestampMs() - 1;
    Replica<State> gc = compactAt(watermark + HOUR, HOUR);
    assertThat(gc.committed().pages()).isEqualTo(r.committed().pages());
    assertThat(gc.committed().horizon()).isZero();
    assertThat(writer.committed(gc, idOf("t0"))).isTrue();
  }

  @Test
  void aSweepDeletesUnreferencedPagesBelowTheFloorOnly() {
    Replica<State> r = paged();
    long floor = r.committed().floor();
    String below = CommittedSetCodec.pageName(0, floor - 1, new byte[16]);
    String above = CommittedSetCodec.pageName(0, floor + 60_000L, new byte[16]);
    log.pages().put(below, new byte[] {1});
    log.pages().put(above, new byte[] {2});
    // More commits, and a compaction that pages them: it sweeps
    commits(T0 + 20_000L, 40, "u");
    Replica<State> again = compactAt(T0 + 30_000L, HOUR);
    assertThat(again.committed().pages().size()).isGreaterThan(r.committed().pages().size());
    Set<String> referenced = new HashSet<>();
    again.committed().pages().forEach(p -> referenced.add(p.name()));
    assertThat(log.storedPages().keySet()).doesNotContain(below).contains(above)
        .containsAll(referenced);
  }

  @Test
  void aCompactionPagesNothingWithoutARetentionAboveTheAdmissionWindow() {
    commits(T0, 40, "t");
    // Retention A: its own GC drops every id a page could take first
    Replica<State> r = compactAt(T0 + 10_000L, A);
    assertThat(r.committed().pages()).isEmpty();
    assertThat(log.storedPages()).isEmpty();
  }

  @Test
  void aLogWithoutPagesNeverPages() {
    InMemorySharedLog plain = new InMemorySharedLog("plain").withoutPages();
    LogRuntime<State, EntityVersionsMachine.Builder, Txn> w =
        new LogRuntime<>(plain, MACHINE, config(HOUR));
    clock.set(T0 - 1_000L);
    w.bootstrap(new Txn("init", Map.of(), Set.of()));
    for (int i = 0; i < 40; i++) {
      clock.set(T0 + 10L * i);
      Replica<State> on = w.read();
      w.commit(on, Txn.on(on.state(), "t" + i, Set.of(), Set.of("t" + i)));
    }
    clock.set(T0 + 10_000L);
    Replica<State> r = w.compact();
    assertThat(r.committed().pages()).isEmpty();
    assertThat(r.committed().ids()).hasSize(41);
  }
}
