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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.intentlog.ScriptedLog.Fault;
import org.intentlog.wire.LogEnvelope;
import org.intentlog.wire.UuidV7;
import org.junit.jupiter.api.Test;

/**
 * The commit loop of {@link LogRuntime}, branch by branch, on a scripted log laid out as one
 * object (a checkpoint and the records after it). Each test names the branch it drives; the TLA+
 * model's action of the same name is in brackets.
 */
class TestLogRuntime {

  private static final long NO_GC = Long.MAX_VALUE;

  private static RuntimeConfig config(int maxRecords, long retentionMs) {
    return config(maxRecords, retentionMs, 0L);
  }

  private static RuntimeConfig config(int maxRecords, long retentionMs, long raceMinWaitMs) {
    return new RuntimeConfig(4, 3, 0L, raceMinWaitMs, retentionMs,
        CompactionPolicy.limits(maxRecords, 1 << 20), java.time.Duration.ZERO,
        System::currentTimeMillis);
  }

  private final ScriptedLog log = new ScriptedLog();
  private LogRuntime<Map<String, Integer>, ToyMachine.Versions, ToyMachine.Bump> runtime =
      runtime(10, NO_GC);

  private LogRuntime<Map<String, Integer>, ToyMachine.Versions, ToyMachine.Bump> runtime(
      int maxRecords, long retentionMs) {
    return new LogRuntime<>(log, ToyMachine.INSTANCE, config(maxRecords, retentionMs));
  }

  /**
   * The runtime compacting at {@code maxRecords}: after a commit at the points {@code draws} give
   * (the last repeated), sending a compaction that lost to appends again on a draw below 1/2; or
   * fused with the commit.
   */
  private void compactingAt(int maxRecords, boolean after, double... draws) {
    int[] next = {0};
    CompactionPolicy policy = CompactionPolicy.limits(maxRecords, 1 << 20)
        .withDraws(() -> draws[Math.min(next[0]++, draws.length - 1)]);
    runtime = new LogRuntime<>(log, ToyMachine.INSTANCE,
        config(maxRecords, NO_GC).withCompaction(policy).withCompactionAfterCommit(after));
  }

  /** The runtime reading the log whole, and probing after a lost append race (tail reads off). */
  private void readWhole() {
    runtime = new LogRuntime<>(log, ToyMachine.INSTANCE, config(10, NO_GC).withTailReads(false));
  }

  private Replica<Map<String, Integer>> bump(Replica<Map<String, Integer>> base, String key) {
    return runtime.commit(base, ToyMachine.Bump.of(base.state(), key));
  }

  /** Another writer's commit, appended directly: a bump of {@code key} from the live state. */
  private void otherWriterBumps(String key) {
    Replica<Map<String, Integer>> live = runtime.read();
    log.appendDirectly(LogEnvelope.encodeRecord(UuidV7.newUuidV7(),
        ToyMachine.INSTANCE.encodeIntention(ToyMachine.Bump.of(live.state(), key))));
  }

  private Replica<Map<String, Integer>> created() {
    return bump(runtime.empty(), "init");
  }

  @Test
  void case0CreatesTheLog() {
    Replica<Map<String, Integer>> created = created();
    assertThat(created.state()).containsEntry("init", 1);
    assertThat(created.intentionRecords()).isEqualTo(1);
    assertThat(log.calls).containsExactly("probe", "replace", "readThrough");
  }

  @Test
  void case0ThatLostTheCreateIsRejected() {
    Replica<Map<String, Integer>> before = runtime.empty();
    created(); // another writer created the log first
    assertThatThrownBy(() -> runtime.commit(before, ToyMachine.Bump.of(before.state(), "k")))
        .isInstanceOfSatisfying(CommitRejectedException.class,
            e -> assertThat(e.reason()).isEqualTo(CommitRejectedException.Reason.CREATE_LOST));
  }

  @Test
  void emptyIsTheStateBeforeTheLogExists() {
    Replica<Map<String, Integer>> empty = runtime.empty();
    assertThat(empty.state()).isEmpty();
    assertThat(empty.tail().exists()).isFalse();
    created();
    assertThatThrownBy(runtime::empty).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void bootstrapCreatesTheLogOnce() {
    runtime.bootstrap(ToyMachine.Bump.of(Map.of(), "init"));
    runtime.bootstrap(ToyMachine.Bump.of(Map.of(), "init"));
    assertThat(runtime.read().state()).containsEntry("init", 1);
    assertThat(log.versionCount()).isEqualTo(1);
  }

  @Test
  void bootstrapThatAnotherWriterBeatFindsItsLog() {
    log.beforeNextWrite(this::created);
    runtime.bootstrap(ToyMachine.Bump.of(Map.of(), "mine"));
    assertThat(runtime.read().state()).containsEntry("init", 1).doesNotContainKey("mine");
  }

  @Test
  void bootstrapThatCreatedNothingFails() {
    log.faults(Fault.LOST_REQUEST);
    assertThatThrownBy(() -> runtime.bootstrap(ToyMachine.Bump.of(Map.of(), "init")))
        .isInstanceOf(CommitOutcomeUnknownException.class);
    log.faults(Fault.NOT_SENT);
    assertThatThrownBy(() -> runtime.bootstrap(ToyMachine.Bump.of(Map.of(), "init")))
        .isInstanceOfSatisfying(CommitRejectedException.class,
            e -> assertThat(e.reason()).isEqualTo(CommitRejectedException.Reason.NOT_SENT));
    assertThat(log.probe().exists()).isFalse();
  }

  @Test
  void case0WhoseReplyWasLostLanded() {
    log.faults(Fault.LOST_REPLY);
    assertThat(created().state()).containsEntry("init", 1);
  }

  @Test
  void appendsWhileThePolicyAllows() { // [Write(APPEND), ReadBack]
    Replica<Map<String, Integer>> r = bump(bump(created(), "a"), "b");
    assertThat(r.state()).containsEntry("a", 1).containsEntry("b", 1);
    assertThat(r.intentionRecords()).isEqualTo(3);
    assertThat(log.calls).containsSubsequence("append", "readThrough", "append", "readThrough");
  }

  @Test
  void aCommitFoldsTheRecordItReadsBackOntoItsBase() {
    Replica<Map<String, Integer>> base = created();
    int folds = ToyMachine.FOLDS.get();
    Replica<Map<String, Integer>> r = bump(base, "a");
    assertThat(ToyMachine.FOLDS.get() - folds).isEqualTo(1);
    assertThat(r.state()).containsEntry("init", 1).containsEntry("a", 1);
    assertThat(r.intentionRecords()).isEqualTo(2);
  }

  @Test
  void aReadThatContinuesTheFloorFoldsOnlyTheRecordsAppended() {
    Replica<Map<String, Integer>> floor = created();
    otherWriterBumps("a");
    otherWriterBumps("b");
    int folds = ToyMachine.FOLDS.get();
    Replica<Map<String, Integer>> r = runtime.read(floor);
    assertThat(ToyMachine.FOLDS.get() - folds).isEqualTo(1);
    assertThat(r.state()).containsEntry("init", 1).containsEntry("a", 1).containsEntry("b", 1);
    assertThat(r.intentionRecords()).isEqualTo(3);
    assertThat(r.tail().continues(floor.tail())).isTrue();
  }

  @Test
  void aReadOfALogReplacedSinceTheFloorReplaysItWhole() {
    Replica<Map<String, Integer>> floor = created();
    Replica<Map<String, Integer>> live = runtime.read();
    log.replaceDirectly(
        LogEnvelope.encodeCheckpoint(live.logId(), live.committed().bytes(),
            ToyMachine.INSTANCE.encodeCheckpoint(live.state())),
        List.of());
    otherWriterBumps("a");
    int folds = ToyMachine.FOLDS.get();
    Replica<Map<String, Integer>> r = runtime.read(floor);
    assertThat(ToyMachine.FOLDS.get() - folds).isZero();
    assertThat(r.state()).containsEntry("init", 1).containsEntry("a", 1);
    assertThat(r.intentionRecords()).isEqualTo(1);
  }

  @Test
  void aReadOntoAReplicaHoldingItsOwnRecordReplaysWhole() {
    // A commit adopts a read that holds its own record, rejected where it landed; a fold onto that
    // read would not see the record, and could not say what replay did with it. (The probe
    // path: a tail read would read the resend's landing before the replace.)
    readWhole();
    log.limitSegments(3);
    Replica<Map<String, Integer>> base = created();
    int[] writes = {0};
    log.beforeEveryWrite(() -> {
      writes[0]++;
      if (writes[0] == 1) {
        otherWriterBumps("k"); // the first append loses; the resend lands after this, rejected
      } else if (writes[0] == 4) {
        otherWriterBumps("other"); // the replace loses; the read after it holds the record
      }
    });
    // The resend lands and reports a conflict; the next append finds the log full, and the
    // commit replaces it on a read that holds its rejected record
    log.faults(Fault.NONE, Fault.LANDED_CONFLICT);
    int folds = ToyMachine.FOLDS.get();
    assertThatThrownBy(() -> bump(base, "k"))
        .isInstanceOfSatisfying(CommitRejectedException.class,
            e -> assertThat(e.reason()).isEqualTo(CommitRejectedException.Reason.CONFLICT));
    assertThat(ToyMachine.FOLDS.get() - folds).isEqualTo(1); // the read before the replace
    assertThat(log.calls).containsSubsequence("append", "append", "append", "read", "replace",
        "read");
  }

  @Test
  void aLostAppendRaceToAnAppendResendsWithoutReading() { // [Refresh]
    readWhole();
    Replica<Map<String, Integer>> base = created();
    log.beforeNextWrite(() -> otherWriterBumps("other"));
    log.calls.clear();
    Replica<Map<String, Integer>> r = bump(base, "mine");
    assertThat(r.state()).containsEntry("other", 1).containsEntry("mine", 1);
    // The racer's read runs inside this writer's first append.
    assertThat(log.calls).containsExactly("append", "read", "probe", "append", "readThrough");
  }

  @Test
  void aLostRaceBacksOffBeforeItProbes() {
    runtime = new LogRuntime<>(log, ToyMachine.INSTANCE,
        config(10, NO_GC, 5L).withTailReads(false));
    Replica<Map<String, Integer>> base = created();
    log.beforeNextWrite(() -> otherWriterBumps("other"));
    log.calls.clear();
    Replica<Map<String, Integer>> r = bump(base, "mine");
    assertThat(r.state()).containsEntry("other", 1).containsEntry("mine", 1);
    assertThat(log.calls).containsExactly("append", "read", "probe", "append", "readThrough");
  }

  @Test
  void anInterruptWhileBackingOffLeavesTheCommitUnknown() {
    // a bound of 10^6 ms: the wait drawn below it is 0 (no sleep) once in a million
    runtime = new LogRuntime<>(log, ToyMachine.INSTANCE, config(10, NO_GC, 1_000_000L));
    Replica<Map<String, Integer>> base = created();
    log.beforeNextWrite(() -> {
      otherWriterBumps("other");
      Thread.currentThread().interrupt();
    });
    try {
      // the write that lost may have landed (a transport retry of one that did fails too)
      assertThatThrownBy(() -> bump(base, "mine"))
          .isInstanceOf(CommitOutcomeUnknownException.class);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void aLostAppendRaceToAReplaceReadsAgain() { // [Shrink]
    readWhole();
    Replica<Map<String, Integer>> base = created();
    log.beforeNextWrite(() -> {
      Replica<Map<String, Integer>> live = runtime.read();
      log.replaceDirectly(
          LogEnvelope.encodeCheckpoint(live.logId(), live.committed().bytes(),
              ToyMachine.INSTANCE.encodeCheckpoint(Map.of("init", 1, "compacted", 1))),
          List.of());
    });
    log.calls.clear();
    Replica<Map<String, Integer>> r = bump(base, "mine");
    assertThat(r.state()).containsEntry("compacted", 1).containsEntry("mine", 1);
    assertThat(log.calls).containsSubsequence("append", "probe", "read", "append", "readThrough");
  }

  @Test
  void aFullLogIsReplacedOnAFreshRead() { // [Write with SegOK false, FreshForCAS] (Bug 66)
    log.limitSegments(2);
    Replica<Map<String, Integer>> r = bump(bump(created(), "a"), "b");
    assertThat(r.state()).containsEntry("a", 1).containsEntry("b", 1);
    assertThat(r.intentionRecords()).isEqualTo(1); // compacted: only its own record follows
    // the fresh read reads what followed the replica the commit holds (a tail read)
    assertThat(log.calls).containsSubsequence("append", "readAfter", "replace", "readThrough");
  }

  @Test
  void aRecordLargerThanAnAppendStoresIsReplaced() { // (Bug 71)
    Replica<Map<String, Integer>> base = created();
    log.limitRecordBytes(1);
    log.calls.clear();
    Replica<Map<String, Integer>> r = bump(base, "a");
    assertThat(r.state()).containsEntry("a", 1);
    assertThat(r.intentionRecords()).isEqualTo(1); // compacted: only its own record follows
    assertThat(log.calls).containsExactly("replace", "readThrough");
  }

  @Test
  void aReadOfALogReplacedSinceTheFloorIsNewer() { // (Bug 72)
    Replica<Map<String, Integer>> floor = bump(bump(created(), "a"), "b");
    Replica<Map<String, Integer>> live = runtime.read();
    log.replaceDirectly(
        LogEnvelope.encodeCheckpoint(live.logId(), live.committed().bytes(),
            ToyMachine.INSTANCE.encodeCheckpoint(Map.of("init", 1, "a", 1, "b", 1, "c", 1))),
        List.of());
    // the replace's tail does not continue the floor's, and is newer: the log replaced itself
    assertThat(runtime.read(floor).state()).containsEntry("c", 1);
  }

  @Test
  void aReplaceThatLostToACompatibleChangeRebases() { // [Rebase]
    runtime = runtime(0, NO_GC); // replace on every commit
    Replica<Map<String, Integer>> base = created();
    log.beforeNextWrite(() -> otherWriterBumps("other"));
    Replica<Map<String, Integer>> r = bump(base, "mine");
    assertThat(r.state()).containsEntry("other", 1).containsEntry("mine", 1);
  }

  @Test
  void aReplaceThatLostToAConflictingChangeIsRejected() { // [Rebase, Judge]
    runtime = runtime(0, NO_GC);
    Replica<Map<String, Integer>> base = created();
    log.beforeNextWrite(() -> otherWriterBumps("k"));
    assertThatThrownBy(() -> bump(base, "k"))
        .isInstanceOfSatisfying(CommitRejectedException.class, e -> {
          assertThat(e.reason()).isEqualTo(CommitRejectedException.Reason.CONFLICT);
          assertThat(e.<Map<String, Integer>>state().state()).containsEntry("k", 1);
        });
  }

  @Test
  void anAppendRejectedWhereItLandedIsAConflict() { // [ReadBack, Judge]
    Replica<Map<String, Integer>> base = created();
    otherWriterBumps("k"); // after base was read
    assertThatThrownBy(() -> bump(base, "k"))
        .isInstanceOfSatisfying(CommitRejectedException.class,
            e -> assertThat(e.reason()).isEqualTo(CommitRejectedException.Reason.CONFLICT));
  }

  @Test
  void aWriteWhoseReplyWasLostIsSettledByAReadThatFindsIt() { // [Resolve]
    Replica<Map<String, Integer>> base = created();
    log.faults(Fault.LOST_REPLY);
    assertThat(bump(base, "k").state()).containsEntry("k", 1);
    assertThat(log.versionCount()).isEqualTo(2); // the commit was written once
  }

  @Test
  void aWriteLostInFlightIsRetriedWhenItStillHolds() { // [Resolve]
    Replica<Map<String, Integer>> base = created();
    log.faults(Fault.LOST_REQUEST);
    assertThat(bump(base, "k").state()).containsEntry("k", 1);
  }

  @Test
  void aWriteWhoseOutcomeNoReadCanSettleIsUnknown() { // [Resolve, Unreadable]
    Replica<Map<String, Integer>> base = created();
    log.faults(Fault.LOST_REPLY).failReads(1);
    assertThatThrownBy(() -> bump(base, "k")).isInstanceOf(CommitOutcomeUnknownException.class);
  }

  @Test
  void aReadFailureWithNoWriteOutstandingIsThrownAsItIs() { // [Unreadable]
    Replica<Map<String, Integer>> base = created();
    log.limitSegments(2);
    bump(base, "a");
    Replica<Map<String, Integer>> stale = runtime.read();
    log.failReads(1); // the fresh read the replace needs (the log is full) fails
    assertThatThrownBy(() -> bump(stale, "b"))
        .isInstanceOf(java.io.UncheckedIOException.class);
  }

  @Test
  void aLandedWriteReportedAsAConflictIsFoundByTheResend() { // [Write landedConflict]
    readWhole();
    Replica<Map<String, Integer>> base = created();
    log.faults(Fault.LANDED_CONFLICT);
    Replica<Map<String, Integer>> r = bump(base, "k");
    assertThat(r.state()).containsEntry("k", 1); // applied once, though written twice
    assertThat(r.intentionRecords()).isEqualTo(3);
  }

  // Tail reads (docs/TAIL_READS.md step 5): after a lost append race the loop reads what
  // followed the replica it holds, and appends at that tail knowing what the log holds there.

  @Test
  void aLostAppendRaceToAnAppendReadsTheTailAndResends() { // [TailRead]
    Replica<Map<String, Integer>> base = created();
    log.beforeNextWrite(() -> otherWriterBumps("other"));
    log.calls.clear();
    Replica<Map<String, Integer>> r = bump(base, "mine");
    assertThat(r.state()).containsEntry("other", 1).containsEntry("mine", 1);
    assertThat(log.calls).containsExactly("append", "read", "readAfter", "append", "readThrough");
    assertThat(log.calls).doesNotContain("probe");
  }

  @Test
  void aTracedCommitReportsEachAttemptItsWritesAndReads() {
    Replica<Map<String, Integer>> base = created();
    // the other writer's record, staged now so that its read is not in this thread's trace
    byte[] other = LogEnvelope.encodeRecord(UuidV7.newUuidV7(),
        ToyMachine.INSTANCE.encodeIntention(ToyMachine.Bump.of(base.state(), "other")));
    log.beforeNextWrite(() -> log.appendDirectly(other));
    CommitTrace.open();
    try {
      bump(base, "mine");
      List<CommitTrace.Event> events = CommitTrace.drain();
      assertThat(events).extracting(CommitTrace.Event::kind).containsExactly(
          CommitTrace.Kind.COMMIT,
          CommitTrace.Kind.ATTEMPT, CommitTrace.Kind.SEND, CommitTrace.Kind.LOST,
          CommitTrace.Kind.READ, CommitTrace.Kind.VIEW, CommitTrace.Kind.REPLAYED,
          CommitTrace.Kind.ADOPTED,
          CommitTrace.Kind.ATTEMPT, CommitTrace.Kind.SEND, CommitTrace.Kind.LANDED,
          CommitTrace.Kind.REPLAYED,
          CommitTrace.Kind.END);
      // the second attempt appends; the scripted log's read after a tail is the whole log (the
      // creation's record and the other writer's), whose record after the replica's is folded;
      // one race lost
      assertThat(events.get(8).a()).isEqualTo(1);
      assertThat(events.get(8).b()).isZero();
      assertThat(events.get(5).a()).isEqualTo(1);
      assertThat(events.get(5).b()).isEqualTo(2);
      assertThat(events.get(6).a()).isEqualTo(1);
      assertThat(events.get(6).b()).isEqualTo(1);
      assertThat(events.get(12).a()).isZero();
      assertThat(events.get(12).b()).isEqualTo(1);
      assertThat(events).isSortedAccordingTo(
          java.util.Comparator.comparingLong(CommitTrace.Event::nanos));
      assertThat(CommitTrace.drain()).isEmpty();
    } finally {
      CommitTrace.close();
    }
    bump(base, "untraced");
    assertThat(CommitTrace.drain()).isEmpty();
  }

  @Test
  void aLostAppendRaceToAReplaceReadsTheReplaceWhole() { // [TailRead, replaced]
    Replica<Map<String, Integer>> base = created();
    log.beforeNextWrite(() -> {
      Replica<Map<String, Integer>> live = runtime.read();
      log.replaceDirectly(
          LogEnvelope.encodeCheckpoint(live.logId(), live.committed().bytes(),
              ToyMachine.INSTANCE.encodeCheckpoint(Map.of("init", 1, "compacted", 1))),
          List.of());
    });
    log.calls.clear();
    Replica<Map<String, Integer>> r = bump(base, "mine");
    assertThat(r.state()).containsEntry("compacted", 1).containsEntry("mine", 1);
    assertThat(log.calls).containsSubsequence("append", "readAfter", "append", "readThrough");
  }

  @Test
  void aLandedWriteReportedAsAConflictIsFoundByTheTailRead() { // [Write landedConflict]
    Replica<Map<String, Integer>> base = created();
    log.faults(Fault.LANDED_CONFLICT);
    Replica<Map<String, Integer>> r = bump(base, "k");
    assertThat(r.state()).containsEntry("k", 1);
    assertThat(r.intentionRecords()).as("written once: the read found it").isEqualTo(2);
  }

  @Test
  void aThrottledWriteIsSentAgain() { // [Write throttled]
    Replica<Map<String, Integer>> base = created();
    log.faults(Fault.THROTTLED, Fault.THROTTLED);
    assertThat(bump(base, "k").state()).containsEntry("k", 1);
  }

  @Test
  void aWriteThrottledUntilTheRetriesRunOutIsAmbiguous() { // [Write landedThrottled]
    Replica<Map<String, Integer>> base = created();
    log.faults(Fault.LANDED_THROTTLED, Fault.THROTTLED, Fault.THROTTLED);
    // The first attempt landed; the retries are throttled; a read settles it.
    assertThat(bump(base, "k").state()).containsEntry("k", 1);
  }

  @Test
  void aWriteNotSentIsRejected() {
    Replica<Map<String, Integer>> base = created();
    log.faults(Fault.NOT_SENT);
    assertThatThrownBy(() -> bump(base, "k"))
        .isInstanceOfSatisfying(CommitRejectedException.class,
            e -> assertThat(e.reason()).isEqualTo(CommitRejectedException.Reason.NOT_SENT));
  }

  @Test
  void aWriteNotSentAfterOneThatMayHaveLandedIsUnknown() {
    Replica<Map<String, Integer>> base = created();
    // A request lost in flight (it did not land, but the commit cannot know), then one not sent.
    log.faults(Fault.LOST_REQUEST, Fault.NOT_SENT);
    assertThatThrownBy(() -> bump(base, "k")).isInstanceOf(CommitOutcomeUnknownException.class);
  }

  @Test
  void aRecordBelowAHorizonARacerRaisedIsUnknownAfterALostRace() { // [Judge] (Bug 64 rule 4)
    Replica<Map<String, Integer>> base = created();
    log.beforeNextWrite(() -> {
      // Another writer's commit, minted an hour ahead, then a GC record that drops it and raises
      // the horizon above this writer's id. This writer's append loses the race, and its resend
      // lands at or below the horizon: replay rejects it, and the lost race may have been a
      // landing, so the outcome is unknown, not failed.
      long ahead = System.currentTimeMillis() + 3_600_000L;
      Replica<Map<String, Integer>> live = runtime.read();
      log.appendDirectly(LogEnvelope.encodeRecord(UuidV7.newUuidV7(ahead, new java.util.Random()),
          ToyMachine.INSTANCE.encodeIntention(ToyMachine.Bump.of(live.state(), "other"))));
      log.appendDirectly(LogEnvelope.encodeAdvanceHorizon(ahead));
    });
    assertThatThrownBy(() -> bump(base, "k")).isInstanceOf(CommitOutcomeUnknownException.class);
  }

  @Test
  void aRecordThatAppliedIsLandedThoughAGcDroppedItsId() { // the outcome rule
    Replica<Map<String, Integer>> base = created();
    // The append lands and its reply is lost; before the read that settles it, a compactor's GC
    // record drops its id (and every other). The committed set can no longer say; replay's
    // judgment of the record where it landed says it applied.
    log.faults(Fault.LOST_REPLY)
        .afterNextWrite(() -> log.appendDirectly(
            LogEnvelope.encodeAdvanceHorizon(System.currentTimeMillis() + 3_600_000L)));
    Replica<Map<String, Integer>> r = bump(base, "k");
    assertThat(r.state()).containsEntry("k", 1);
    assertThat(r.committed().ids()).isEmpty();
  }

  @Test
  void aCommitThatLosesEveryRaceRunsOutOfAttempts() { // [Exhaust, FinalRead]
    Replica<Map<String, Integer>> base = created();
    log.beforeEveryWrite(() -> otherWriterBumps("other"));
    assertThatThrownBy(() -> bump(base, "k"))
        .isInstanceOfSatisfying(CommitRejectedException.class,
            e -> assertThat(e.reason()).isEqualTo(CommitRejectedException.Reason.EXHAUSTED));
  }

  @Test
  void aReplaceWritesAGcRecordBelowItsOwnId() throws InterruptedException { // [CasPayload]
    runtime = runtime(0, 0L); // retention 0: every replace drops all ids older than its own
    Replica<Map<String, Integer>> created = created();
    Thread.sleep(5); // ids of different milliseconds: the watermark stays below the commit's own
    Replica<Map<String, Integer>> a = bump(created, "a");
    Thread.sleep(5);
    Replica<Map<String, Integer>> r = bump(a, "b");
    assertThat(r.state()).containsEntry("a", 1).containsEntry("b", 1);
    assertThat(r.committed().horizon()).isPositive();
    UUID own = r.committed().ids().iterator().next();
    assertThat(r.committed().horizon()).isLessThan(UuidV7.timestampMs(own));
  }

  @Test
  void pastTheLimitsACommitAppendsAndItsWriterCompacts() { // [Committed, Compact]
    compactingAt(2, true, 0.0);
    Replica<Map<String, Integer>> base = created();
    log.calls.clear();
    Replica<Map<String, Integer>> r = bump(base, "a");
    // the replica the commit landed in, which the compaction replaced with a checkpoint of it
    assertThat(r.intentionRecords()).isEqualTo(2);
    assertThat(log.calls).containsExactly("append", "readThrough", "replace");
    Replica<Map<String, Integer>> live = runtime.read();
    assertThat(live.state()).isEqualTo(r.state());
    assertThat(live.intentionRecords()).isZero();
    assertThat(live.committed().ids()).isEqualTo(r.committed().ids());
  }

  @Test
  void aCompactionThatLosesItsRaceIsDroppedAndTheCommitStands() { // [Compact, lost]
    compactingAt(2, true, 0.0, 0.75); // its point 1.0; not sent again
    Replica<Map<String, Integer>> base = created();
    log.afterNextWrite(() -> otherWriterBumps("other")); // lands before the compaction
    log.calls.clear();
    Replica<Map<String, Integer>> r = bump(base, "a");
    assertThat(r.state()).containsEntry("a", 1);
    assertThat(log.calls).containsSubsequence("append", "readThrough", "replace", "readAfter");
    assertThat(log.calls.stream().filter("replace"::equals).count()).isEqualTo(1);
    Replica<Map<String, Integer>> live = runtime.read();
    assertThat(live.state()).containsEntry("a", 1).containsEntry("other", 1);
    assertThat(live.intentionRecords()).isEqualTo(3); // not compacted
  }

  @Test
  void aCompactionThatLosesToAnAppendIsSentAgainWithItsRecord() { // [Compact, CompactRead]
    compactingAt(2, true, 0.0);
    Replica<Map<String, Integer>> base = created();
    log.afterNextWrite(() -> otherWriterBumps("other")); // lands before the compaction
    log.calls.clear();
    Replica<Map<String, Integer>> r = bump(base, "a");
    assertThat(r.intentionRecords()).isEqualTo(2);
    assertThat(log.calls).containsSubsequence(
        "append", "readThrough", "replace", "readAfter", "replace");
    // The checkpoint of the version the commit landed in, then the record that beat the
    // compaction, as it was written
    LogView view = log.read();
    assertThat(view.records()).hasSize(1);
    LogEnvelope.Record copied = LogEnvelope.decodeRecord(view.records().get(0));
    assertThat(copied.isIntention()).isTrue();
    Replica<Map<String, Integer>> live = runtime.read();
    assertThat(live.state()).containsEntry("a", 1).containsEntry("other", 1);
    assertThat(live.intentionRecords()).isEqualTo(1);
    assertThat(live.committed().ids()).contains(copied.id()).containsAll(r.committed().ids());
  }

  @Test
  void aCompactionThatLosesToAReplaceIsDropped() { // [CompactRead: replaced]
    compactingAt(2, true, 0.0);
    LogRuntime<Map<String, Integer>, ToyMachine.Versions, ToyMachine.Bump> fused =
        new LogRuntime<>(log, ToyMachine.INSTANCE,
            config(1, NO_GC).withCompactionAfterCommit(false));
    Replica<Map<String, Integer>> base = created();
    log.afterNextWrite(() -> { // another writer's commit replaces the log with its record
      Replica<Map<String, Integer>> live = fused.read();
      fused.commit(live, ToyMachine.Bump.of(live.state(), "other"));
    });
    log.calls.clear();
    bump(base, "a");
    assertThat(log.calls.get(log.calls.size() - 1)).isEqualTo("readAfter");
    Replica<Map<String, Integer>> live = runtime.read();
    assertThat(live.state()).containsEntry("a", 1).containsEntry("other", 1);
    assertThat(live.intentionRecords()).isEqualTo(1); // the other's record, after its checkpoint
  }

  @Test
  void aCompactionIsSentAgainAtMostFourTimes() { // [CompactResends]
    compactingAt(2, true, 0.0);
    Replica<Map<String, Integer>> base = created();
    int[] others = {0};
    // Once the commit landed, another append beats every write
    log.afterNextWrite(() -> log.beforeEveryWrite(() -> otherWriterBumps("other" + others[0]++)));
    log.calls.clear();
    bump(base, "a");
    assertThat(log.calls.stream().filter("replace"::equals).count())
        .isEqualTo(1 + LogRuntime.MAX_COMPACTION_RESENDS);
    Replica<Map<String, Integer>> live = runtime.read();
    assertThat(live.intentionRecords()).isEqualTo(2 + others[0]);
    assertThat(live.state()).containsKeys("a", "other0", "other" + (others[0] - 1));
  }

  @Test
  void aCompactionIsSentAgainWhenLessThanOneAppendIsExpected() { // CompactionPolicy.resend
    CompactionPolicy policy = CompactionPolicy.limits(100, 1 << 20);
    // 3 appends in 600 ms, at 5 a second: 0.5 expected over a 100 ms resend, 1.25 over 250 ms
    assertThat(policy.resend(3, 600_000_000L, 100_000_000L)).isTrue();
    assertThat(policy.resend(3, 600_000_000L, 250_000_000L)).isFalse();
    // At a knee: 50 appends over 3 s, against a 1 s send
    assertThat(policy.resend(50, 3_000_000_000L, 1_000_000_000L)).isFalse();
    assertThat(policy.resend(0, 600_000_000L, 1L)).isFalse();
    assertThat(policy.resend(1, 0L, 1L)).isFalse();
  }

  @Test
  void aWriterCompactsAtThePointItDrawsAndAfterALossWaitsTwiceAsLong() { // [CompactDue]
    compactingAt(4, true, 0.0, 0.75, 0.75, 0.25); // a point, no resend, the next, a point
    Replica<Map<String, Integer>> r = bump(bump(created(), "a"), "b"); // 3 records: not due
    assertThat(log.calls.stream().filter("replace"::equals).count()).isEqualTo(1); // the create
    log.afterNextWrite(() -> otherWriterBumps("other"));
    r = bump(r, "c"); // 4 records: its first point, 1.0; its compaction loses to the other's
    assertThat(runtime.read().intentionRecords()).isEqualTo(5);
    log.calls.clear();
    for (String key : List.of("d", "e", "f", "g")) { // up to 9 records, 2.25
      r = bump(r, key);
    }
    // short of its next point: 1.0 + 0.75, doubled for the loss
    assertThat(log.calls).doesNotContain("replace");
    r = bump(r, "h"); // 10 records, 2.5: at it
    assertThat(log.calls).containsSubsequence("append", "readThrough", "replace");
    assertThat(runtime.read().intentionRecords()).isZero();
    // compacted since: the point is drawn afresh at the next due commit, 1.0 + 0.25
    r = runtime.read();
    for (String key : List.of("i", "j", "k", "l")) {
      r = bump(r, key);
    }
    assertThat(runtime.read().intentionRecords()).isEqualTo(4);
    bump(r, "m"); // 5 records, 1.25
    assertThat(runtime.read().intentionRecords()).isZero();
    assertThat(runtime.read().state()).containsKeys("a", "c", "h", "m", "other");
  }

  @Test
  void fusedPastTheLimitsACommitReplacesTheLogWithItsRecord() { // [Write(CAS)]
    compactingAt(2, false);
    Replica<Map<String, Integer>> r = bump(created(), "a");
    log.calls.clear();
    r = bump(r, "b");
    assertThat(log.calls).containsExactly("replace", "readThrough");
    assertThat(r.intentionRecords()).isEqualTo(1); // its own record follows the checkpoint
  }

  @Test
  void aPolicyDueOnALogWithNoRecordsReplacesOnEveryCommit() { // [MustCAS] (CAS only)
    compactingAt(0, true, 0.0);
    Replica<Map<String, Integer>> r = bump(created(), "a");
    log.calls.clear();
    r = bump(r, "b");
    assertThat(log.calls).containsExactly("replace", "readThrough");
    assertThat(r.intentionRecords()).isEqualTo(1);
  }

  @Test
  void aTracedCompactionFollowsItsCommit() {
    compactingAt(2, true, 0.0);
    Replica<Map<String, Integer>> base = created();
    CommitTrace.open();
    try {
      bump(base, "a");
      List<CommitTrace.Kind> kinds =
          CommitTrace.drain().stream().map(CommitTrace.Event::kind).toList();
      assertThat(kinds).containsSubsequence(CommitTrace.Kind.END, CommitTrace.Kind.COMPACT,
          CommitTrace.Kind.PREPARED, CommitTrace.Kind.SEND, CommitTrace.Kind.LANDED);
    } finally {
      CommitTrace.close();
    }
  }
}
