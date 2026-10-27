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
  void aLostAppendRaceToAnAppendResendsWithoutReading() { // [Refresh]
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
    runtime = new LogRuntime<>(log, ToyMachine.INSTANCE, config(10, NO_GC, 5L));
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
    assertThat(log.calls).containsSubsequence("append", "read", "replace", "readThrough");
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
    Replica<Map<String, Integer>> base = created();
    log.faults(Fault.LANDED_CONFLICT);
    Replica<Map<String, Integer>> r = bump(base, "k");
    assertThat(r.state()).containsEntry("k", 1); // applied once, though written twice
    assertThat(r.intentionRecords()).isEqualTo(3);
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
}
