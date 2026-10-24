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

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.intentlog.Capabilities;
import org.intentlog.CommitOutcomeUnknownException;
import org.intentlog.CommitRejectedException;
import org.intentlog.CompactionPolicy;
import org.intentlog.LogRuntime;
import org.intentlog.LogTail;
import org.intentlog.LogView;
import org.intentlog.Receipt;
import org.intentlog.Replay;
import org.intentlog.Replica;
import org.intentlog.RuntimeConfig;
import org.intentlog.SharedLog;
import org.intentlog.memory.InMemorySharedLog.ReadShape;
import org.intentlog.memory.InMemorySharedLog.Request;
import org.intentlog.memory.InMemorySharedLog.WriteFault;
import org.intentlog.testkit.EntityVersionsMachine;
import org.intentlog.testkit.EntityVersionsMachine.State;
import org.intentlog.testkit.EntityVersionsMachine.Txn;
import org.intentlog.wire.LogEnvelope;
import org.junit.jupiter.api.Test;

/**
 * {@link LogRuntime} on a shared log with checkpoints apart from its records: the commit loop's
 * append branch, creation, compaction and refresh, branch by branch, with scripted faults and
 * other writers. The schedules that SharedLogCommit.tla found are here (its bug switches in
 * brackets), with the proof that a copy of a record never applies again.
 */
class TestSharedLogRuntime {

  private static final EntityVersionsMachine MACHINE = EntityVersionsMachine.INSTANCE;
  private static final long NO_GC = Long.MAX_VALUE;
  private static final CompactionPolicy NEVER = (records, bytes, recordBytes) -> false;

  /** Scripted faults, for the writer under test only: hooks that run other writers pause them. */
  private static final class Script implements InMemorySharedLog.Interceptor {
    final Deque<WriteFault> appends = new ArrayDeque<>();
    final Deque<ReadShape> reads = new ArrayDeque<>();
    Runnable afterNextAppend = null;
    boolean active = true;

    @Override
    public WriteFault beforeWrite(Request request, byte[] payload) {
      return active && request == Request.APPEND && !appends.isEmpty()
          ? appends.poll() : WriteFault.NONE;
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
    public ReadShape beforeRead(Request request) {
      return active && (request == Request.READ || request == Request.READ_THROUGH)
          && !reads.isEmpty() ? reads.poll() : ReadShape.NORMAL;
    }

    /** Runs other writers' requests, which the script does not fault. */
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
  private final Script script = new Script();
  private final AtomicLong clock = new AtomicLong(System.currentTimeMillis());

  TestSharedLogRuntime() {
    log.intercept(script);
  }

  private RuntimeConfig config(long retentionMs, CompactionPolicy compaction) {
    return new RuntimeConfig(4, 3, 0L, retentionMs, compaction, Duration.ZERO, clock::get);
  }

  private LogRuntime<State, EntityVersionsMachine.Builder, Txn> runtime(
      long retentionMs, CompactionPolicy compaction) {
    return new LogRuntime<>(log, MACHINE, config(retentionMs, compaction));
  }

  private final LogRuntime<State, EntityVersionsMachine.Builder, Txn> writer =
      runtime(NO_GC, NEVER);

  private static Txn txn(Replica<State> on, String tag, Set<String> reads, Set<String> writes) {
    return Txn.on(on.state(), tag, reads, writes);
  }

  /** A commit by another writer, on a fresh read, outside the script. */
  private Replica<State> other(String tag, Set<String> reads, Set<String> writes) {
    Replica<State>[] landed = new Replica[1];
    script.others(() -> {
      Replica<State> fresh = writer.read();
      landed[0] = writer.commit(fresh, txn(fresh, tag, reads, writes));
    });
    return landed[0];
  }

  /** A compaction, outside the script, at retention 0 with the clock past every id so far. */
  private void compactDroppingEveryId() {
    script.others(() -> {
      clock.set(System.currentTimeMillis() + 1_000L);
      runtime(0L, NEVER).compact();
    });
  }

  private Replica<State> created() {
    writer.bootstrap(new Txn("init", java.util.Map.of(), Set.of()));
    return writer.read();
  }

  /** The state a replay of every record ever appended reaches: what no trim can hide. */
  private State replayed() {
    return Replay.replay(MACHINE, log.checkpoints().get(0L), log.records(), null)
        .builder().build();
  }

  private void assertAppliedOnce(String tag) {
    assertThat(replayed().history()).filteredOn(tag::equals).hasSize(1);
    assertThat(writer.read().state().history()).filteredOn(tag::equals).hasSize(1);
  }

  // ============================================================
  // Creation and the append branch
  // ============================================================

  @Test
  void bootstrapCreatesTheLogWithItsFirstCheckpoint() {
    assertThat(log.probe().exists()).isFalse();
    Replica<State> created = created();
    assertThat(log.checkpoints()).containsOnlyKeys(0L);
    assertThat(created.state().history()).containsExactly("init");
    writer.bootstrap(new Txn("again", java.util.Map.of(), Set.of()));
    assertThat(writer.read().state().history()).containsExactly("init");
  }

  @Test
  void aCreateThatAnotherWriterBeatIsRejected() {
    Replica<State> empty = writer.empty();
    script.others(() -> writer.bootstrap(new Txn("first", java.util.Map.of(), Set.of())));
    assertThatThrownBy(() -> writer.commit(empty, txn(empty, "late", Set.of(), Set.of("a"))))
        .isInstanceOfSatisfying(CommitRejectedException.class, e ->
            assertThat(e.reason()).isEqualTo(CommitRejectedException.Reason.CREATE_LOST));
  }

  @Test
  void aCommitAppendsItsRecordAndReadsItBack() { // [Send, ReadBack]
    Replica<State> base = created();
    Replica<State> r = writer.commit(base, txn(base, "t", Set.of("a"), Set.of("a")));
    assertThat(r.state().version("a")).isEqualTo(1);
    assertThat(log.records()).hasSize(2);
  }

  @Test
  void aRecordLargerThanTheLogStoresIsRejectedBeforeItIsSent() { // (Bug 71)
    Replica<State> base = created();
    log.limitRecordBytes(1);
    assertThatThrownBy(() -> writer.commit(base, txn(base, "t", Set.of(), Set.of("a"))))
        .isInstanceOfSatisfying(CommitRejectedException.class, e ->
            assertThat(e.reason()).isEqualTo(CommitRejectedException.Reason.TOO_LARGE));
    assertThat(log.records()).hasSize(1);
  }

  @Test
  void aRecordWhoseReadsAnotherCommitChangedIsRejectedWhereItLands() { // [ReadBack, Judge]
    Replica<State> base = created();
    other("o", Set.of(), Set.of("a"));
    assertThatThrownBy(() -> writer.commit(base, txn(base, "t", Set.of("a"), Set.of("b"))))
        .isInstanceOfSatisfying(CommitRejectedException.class, e ->
            assertThat(e.reason()).isEqualTo(CommitRejectedException.Reason.CONFLICT));
    assertThat(replayed().history()).doesNotContain("t");
  }

  @Test
  void aWriteWhoseReplyWasLostIsSettledByARead() { // [Resolve]
    Replica<State> base = created();
    script.appends.add(WriteFault.LOST_REPLY);
    writer.commit(base, txn(base, "t", Set.of(), Set.of("a")));
    assertAppliedOnce("t");
  }

  @Test
  void aConditionalAppendThatLostARaceIsSentAgainAtTheNewTail() {
    InMemorySharedLog conditional = new InMemorySharedLog("log", true, true);
    LogRuntime<State, EntityVersionsMachine.Builder, Txn> rt =
        new LogRuntime<>(conditional, MACHINE, config(NO_GC, NEVER));
    rt.bootstrap(new Txn("init", java.util.Map.of(), Set.of()));
    Replica<State> base = rt.read();
    Replica<State> fresh = rt.read();
    rt.commit(fresh, Txn.on(fresh.state(), "o", Set.of(), Set.of("b")));
    Replica<State> r = rt.commit(base, Txn.on(base.state(), "t", Set.of("a"), Set.of("a")));
    assertThat(r.state().history()).containsExactly("init", "o", "t");
  }

  // ============================================================
  // Copies of a record: delayed sends, duplicates, compaction
  // ============================================================

  @Test
  void aDelayedSendThatStillHoldsIsSentAgainAndAppliesOnce() { // [Resolve; freshid]
    Replica<State> base = created();
    // A blind write: only its id keeps a second copy from applying.
    script.appends.add(WriteFault.DELAYED);
    writer.commit(base, txn(base, "t", Set.of(), Set.of("a")));
    assertThat(log.deliverDelayed()).isEqualTo(1);
    assertThat(log.records()).filteredOn(r -> tagOf(r).equals("t")).hasSize(2);
    assertAppliedOnce("t");
  }

  @Test
  void aCommitFailedOnAReadStaysFailedWhenItsDelayedSendLands() { // [Resolve; noverify]
    Replica<State> base = created();
    script.appends.add(WriteFault.DELAYED);
    // While the send is in flight, another writer changes what it read, just before the read that
    // resolves it: the record is not there, and the transaction no longer holds.
    log.intercept(new InMemorySharedLog.Interceptor() {
      private boolean raced = false;

      @Override
      public WriteFault beforeWrite(Request request, byte[] payload) {
        return script.beforeWrite(request, payload);
      }

      @Override
      public ReadShape beforeRead(Request request) {
        if (!raced && request == Request.READ && script.active) {
          raced = true;
          other("o", Set.of(), Set.of("a"));
        }
        return ReadShape.NORMAL;
      }
    });
    assertThatThrownBy(() -> writer.commit(base, txn(base, "t", Set.of("a"), Set.of("b"))))
        .isInstanceOf(CommitRejectedException.class);
    log.deliverDelayed();
    assertThat(replayed().history()).containsExactly("init", "o");
  }

  @Test
  void aCopyOfARecordThatAppliedNeverAppliesAgainThoughCheckpointedAndTrimmed() {
    Replica<State> base = created();
    writer.commit(base, txn(base, "t", Set.of(), Set.of("a")));
    byte[] original = recordOf("t");
    runtime(NO_GC, NEVER).compact(); // a checkpoint past it, and a trim: the original is gone
    assertThat(log.trimmedThrough()).isGreaterThanOrEqualTo(2);
    State before = writer.read().state();
    log.append(original, null);
    assertThat(writer.read().state()).isEqualTo(before);
    assertAppliedOnce("t");
  }

  @Test
  void aCopyOfARecordThatAppliedNeverAppliesAgainThoughGcDroppedItsId() {
    Replica<State> base = created();
    writer.commit(base, txn(base, "t", Set.of(), Set.of("a")));
    byte[] original = recordOf("t");
    compactDroppingEveryId(); // the horizon passes its id, which the committed set no longer holds
    Replica<State> compacted = writer.read();
    assertThat(compacted.committed().ids()).isEmpty();
    assertThat(compacted.committed().horizon()).isPositive();
    log.append(original, null);
    assertThat(writer.read().state()).isEqualTo(compacted.state());
    assertAppliedOnce("t");
  }

  @Test
  void aCopyOfARecordThatWasRejectedNeverApplies() {
    Replica<State> base = created();
    other("o", Set.of(), Set.of("a"));
    assertThatThrownBy(() -> writer.commit(base, txn(base, "t", Set.of("a"), Set.of("b"))))
        .isInstanceOf(CommitRejectedException.class);
    byte[] rejected = recordOf("t");
    runtime(NO_GC, NEVER).compact();
    log.append(rejected, null);
    assertThat(replayed().history()).containsExactly("init", "o");
  }

  // ============================================================
  // What SharedLogCommit.tla found
  // ============================================================

  @Test
  void aRecordThatAppliedIsCommittedThoughAGcRecordDroppedItsIdBeforeTheReadBack() { // [setonly]
    Replica<State> base = created();
    script.afterNextAppend = () -> {
      clock.set(System.currentTimeMillis() + 1_000L);
      log.append(LogEnvelope.encodeAdvanceHorizon(clock.get()), null);
    };
    Replica<State> r = writer.commit(base, txn(base, "t", Set.of(), Set.of("a")));
    assertThat(r.state().history()).containsExactly("init", "t");
    assertThat(r.committed().ids()).isEmpty();
  }

  @Test
  void aThrottledSendThatLandedBesideTheAcknowledgedOneLeavesTheOutcomeUnknown() { // [dup]
    Replica<State> base = created();
    // The first send lands but is reported throttled; a compactor then drops every id, checkpoints
    // and trims; the resend lands after the checkpoint, where the horizon passed its id. The read
    // back starts from that checkpoint: replay skips the resend, and the committed set cannot say
    // whether the first applied. It did.
    script.appends.add(WriteFault.LANDED_THROTTLED);
    script.afterNextAppend = this::compactDroppingEveryId;
    assertThatThrownBy(() -> writer.commit(base, txn(base, "t", Set.of(), Set.of("a"))))
        .isInstanceOf(CommitOutcomeUnknownException.class);
    assertAppliedOnce("t");
  }

  @Test
  void aReadThatLagsBehindTheCommitsBaseIsReadAgain() { // [lagread]
    created();
    other("o", Set.of(), Set.of("a")); // moves a to version 1
    Replica<State> base = writer.read();
    // The reply is lost; the read that resolves it lags behind the base (a at version 0, where
    // the transaction does not hold yet), and the next read catches up and finds the record.
    script.appends.add(WriteFault.LOST_REPLY);
    script.reads.add(ReadShape.of(0, 3));
    Replica<State> r = writer.commit(base, txn(base, "t", Set.of("a"), Set.of("b")));
    assertThat(r.state().history()).containsExactly("init", "o", "t");
  }

  @Test
  void aCommitWhoseReadsKeepLaggingIsUnknownNotFailed() { // [lagread]
    created();
    other("o", Set.of(), Set.of("a"));
    Replica<State> base = writer.read();
    script.appends.add(WriteFault.LOST_REPLY);
    for (int i = 0; i < 10; i++) {
      script.reads.add(ReadShape.of(0, 3));
    }
    assertThatThrownBy(() -> writer.commit(base, txn(base, "t", Set.of("a"), Set.of("b"))))
        .isInstanceOf(CommitOutcomeUnknownException.class);
    script.reads.clear();
    assertAppliedOnce("t");
  }

  /** The log under test, but a read through a write answers from its newest checkpoint. */
  private final class LooseReadThrough implements SharedLog {
    @Override
    public Capabilities capabilities() {
      return log.capabilities();
    }

    @Override
    public LogView read() {
      return log.read();
    }

    @Override
    public LogView readThrough(Receipt receipt, Duration wait) {
      return log.read(); // may start past the record: no P3
    }

    @Override
    public LogTail probe() {
      return log.probe();
    }

    @Override
    public Receipt append(byte[] record, LogTail expected) {
      return log.append(record, expected);
    }

    @Override
    public void putCheckpoint(LogTail through, byte[] checkpoint) {
      log.putCheckpoint(through, checkpoint);
    }

    @Override
    public void trim(LogTail through) {
      log.trim(through);
    }
  }

  @Test
  void aReadThroughThatDoesNotIncludeTheRecordSettlesNothing() { // [p3]
    Replica<State> base = created();
    LogRuntime<State, EntityVersionsMachine.Builder, Txn> loose =
        new LogRuntime<>(new LooseReadThrough(), MACHINE, config(NO_GC, NEVER));
    // The record lands and applies; a compactor drops its id, checkpoints and trims. The read
    // through the write starts from that checkpoint: it cannot say whether the record applied,
    // so the write is ambiguous, and a read cannot settle it either.
    script.afterNextAppend = this::compactDroppingEveryId;
    assertThatThrownBy(() -> loose.commit(base, txn(base, "t", Set.of(), Set.of("a"))))
        .isInstanceOf(CommitOutcomeUnknownException.class);
    assertAppliedOnce("t");
  }

  @Test
  void aCommitThatLandedIsCommittedWhenTheLogWasTrimmedPastItBeforeTheReadBack() {
    Replica<State> base = created();
    // The record lands; a compactor checkpoints and trims past it before the read back, which
    // fails (TrimmedException): the write is ambiguous, and a read settles it.
    script.afterNextAppend = () -> runtime(NO_GC, NEVER).compact();
    Replica<State> r = writer.commit(base, txn(base, "t", Set.of(), Set.of("a")));
    assertThat(r.state().history()).containsExactly("init", "t");
  }

  // ============================================================
  // Compaction and refresh
  // ============================================================

  @Test
  void compactionCheckpointsTheReplayAndTrimsWhatItHolds() {
    Replica<State> base = created();
    Replica<State> r = writer.commit(base, txn(base, "t", Set.of(), Set.of("a")));
    Replica<State> compacted = runtime(NO_GC, NEVER).compact();
    assertThat(compacted.state()).isEqualTo(r.state());
    long through = log.trimmedThrough();
    assertThat(log.checkpoints()).containsKey(through);
    LogView view = log.read();
    assertThat(view.records()).isEmpty();
    assertThat(writer.read().state()).isEqualTo(r.state());
  }

  @Test
  void theWriterWhoseCommitLandsCompactsWhenThePolicySays() {
    LogRuntime<State, EntityVersionsMachine.Builder, Txn> compacting =
        runtime(NO_GC, CompactionPolicy.limits(2, 1 << 20));
    compacting.bootstrap(new Txn("init", java.util.Map.of(), Set.of()));
    for (String tag : List.of("t1", "t2", "t3")) {
      Replica<State> base = compacting.read();
      compacting.commit(base, Txn.on(base.state(), tag, Set.of(), Set.of("a")));
    }
    assertThat(log.trimmedThrough()).isPositive();
    assertThat(compacting.read().state().history()).containsExactly("init", "t1", "t2", "t3");
  }

  @Test
  void refreshFoldsTheRecordsSinceOntoAReplica() {
    Replica<State> base = created();
    other("o", Set.of(), Set.of("a"));
    Replica<State> refreshed = writer.refresh(base);
    assertThat(refreshed.state()).isEqualTo(writer.read().state());
    assertThat(refreshed.intentionRecords()).isEqualTo(writer.read().intentionRecords());
    runtime(NO_GC, NEVER).compact();
    other("p", Set.of(), Set.of("b"));
    assertThat(writer.refresh(base).state()).isEqualTo(writer.read().state()); // trimmed: afresh
  }

  // ============================================================

  private static String tagOf(byte[] record) {
    LogEnvelope.Record r = LogEnvelope.decodeRecord(record);
    return r.isAdvanceHorizon() ? "" : MACHINE.decodeIntention(r.intention()).tag();
  }

  private byte[] recordOf(String tag) {
    return log.records().stream().filter(r -> tagOf(r).equals(tag)).findFirst().orElseThrow();
  }
}
