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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.intentlog.wire.CommittedSetCodec;
import org.intentlog.wire.LogEnvelope;
import org.intentlog.wire.UuidV7;
import org.junit.jupiter.api.Test;

/** Replay over a toy state machine: keys with versions, and intentions that bump one. */
class TestReplay {

  private static final ToyMachine MACHINE = ToyMachine.INSTANCE;


  private final Random random = new Random(42);

  private UUID id(long ts) {
    return UuidV7.newUuidV7(ts, random);
  }

  private static byte[] record(UUID id, String key, int read) {
    return LogEnvelope.encodeRecord(id, MACHINE.encodeIntention(new ToyMachine.Bump(key, read)));
  }

  @Test
  void appliesWhatHoldsWhereItLandsOnce() {
    UUID a = id(100);
    UUID b = id(101);
    UUID c = id(102);
    // b read k at version 0, as a did; a lands first, so b's precondition no longer holds.
    List<byte[]> log = List.of(record(a, "k", 0), record(b, "k", 0), record(a, "k", 0),
        record(c, "j", 0));
    Replay.Result<ToyMachine.Versions> result = Replay.replay(MACHINE, null, log, b);
    assertThat(result.builder().build()).containsEntry("k", 1).containsEntry("j", 1);
    assertThat(result.committed().build().ids()).containsExactlyInAnyOrder(a, c);
    assertThat(result.intentionRecords()).isEqualTo(4);
    assertThat(result.outcome()).isEqualTo(Replay.Outcome.REJECTED);
    assertThat(result.logId()).isNull();

    assertThat(Replay.replay(MACHINE, null, log, a).outcome()).isEqualTo(Replay.Outcome.APPLIED);
    assertThat(Replay.replay(MACHINE, null, log, id(103)).outcome())
        .isEqualTo(Replay.Outcome.NOT_SEEN);
  }

  /**
   * Replay reports the ids of the intention records it replays, in log order, applied, rejected
   * or skipped alike, and a fold's continue them: what a later fold looks a commit's record up in
   * (errata D25).
   */
  @Test
  void reportsTheIdsOfTheIntentionRecordsItReplays() {
    UUID a = id(100);
    UUID b = id(101);
    UUID c = id(102);
    UUID d = id(103);
    List<byte[]> log = List.of(record(a, "k", 0), record(b, "k", 0),
        LogEnvelope.encodeAdvanceHorizon(50), record(a, "k", 0), record(c, "j", 0));
    Replay.Result<ToyMachine.Versions> result = Replay.replay(MACHINE, null, log, null);
    IntentionIds ids = result.ids();
    assertThat(ids.size()).isEqualTo(4).isEqualTo(result.intentionRecords());
    assertThat(ids.contains(a)).isTrue();
    assertThat(ids.contains(b)).as("rejected").isTrue();
    assertThat(ids.contains(c)).isTrue();
    assertThat(ids.contains(d)).isFalse();

    List<byte[]> more = List.of(record(d, "i", 0));
    Replay.Result<ToyMachine.Versions> folded = Replay.replayOnto(MACHINE, result.builder(),
        result.committed(), null, result.intentionRecords(), more, null);
    List<byte[]> whole = new ArrayList<>(log);
    whole.addAll(more);
    assertThat(ids.plus(folded.ids())).isEqualTo(Replay.replay(MACHINE, null, whole, null).ids());
  }

  @Test
  void continuesFromACheckpoint() {
    UUID logId = id(1);
    UUID a = id(100);
    byte[] checkpoint = LogEnvelope.encodeCheckpoint(
        logId, CommittedSet.builder().add(a).build().bytes(),
        MACHINE.encodeCheckpoint(Map.of("k", 3)));
    List<byte[]> log = List.of(record(a, "k", 3), record(id(101), "k", 3));
    Replay.Result<ToyMachine.Versions> result = Replay.replay(MACHINE, checkpoint, log, null);
    // a is in the checkpoint's committed set: its copy is a retry, and is skipped.
    assertThat(result.builder().build()).containsEntry("k", 4);
    assertThat(result.logId()).isEqualTo(logId);
  }

  @Test
  void aGcRecordMovesTheHorizonWhereItLands() {
    UUID a = id(100);
    UUID late = id(80);
    UUID afterDrop = id(150);
    // The GC record drops a, and raises the horizon to a's time (the newest it drops), not to its
    // watermark: a record minted at or below 100 may be a retry of one aged out, one above may not.
    List<byte[]> log = List.of(
        record(a, "k", 0),
        LogEnvelope.encodeAdvanceHorizon(200),
        record(late, "j", 0),
        record(afterDrop, "i", 0));
    Replay.Result<ToyMachine.Versions> result = Replay.replay(MACHINE, null, log, late);
    assertThat(result.builder().build()).containsEntry("k", 1).containsEntry("i", 1)
        .doesNotContainKey("j");
    assertThat(result.committed().horizon()).isEqualTo(100);
    assertThat(result.outcome()).isEqualTo(Replay.Outcome.NOT_SEEN);
    assertThat(result.intentionRecords()).isEqualTo(3);
  }

  @Test
  void aPageOutSealsTheOldestBlockWhereItLands() {
    assumeThat(CommittedSetCodec.BLOCK_IDS).as("small blocks (surefire)").isLessThanOrEqualTo(64);
    List<byte[]> log = new ArrayList<>();
    List<UUID> ids = new ArrayList<>();
    for (int i = 0; i < 3 * CommittedSetCodec.BLOCK_IDS; i++) {
      UUID id = id(1_000_000L + 7L * i);
      ids.add(id);
      log.add(record(id, "k" + i, 0));
    }
    CommittedSet before = Replay.replay(MACHINE, null, log, null).committed().build();
    CommittedSet.Seal seal = before.sealable(Long.MAX_VALUE);
    assertThat(seal).isNotNull();
    byte[] pageOut = LogEnvelope.encodePageOut(seal.record());
    // A late id inside the sealed block (the oldest id's ms), and a copy of a sealed record
    UUID late = id(UuidV7.timestampMs(ids.get(0)));

    List<byte[]> sealedFirst = new ArrayList<>(log);
    sealedFirst.add(pageOut);
    sealedFirst.add(record(late, "late", 0));
    sealedFirst.add(log.get(0));
    Replay.Result<ToyMachine.Versions> result = Replay.replay(MACHINE, null, sealedFirst, late);
    CommittedSet set = result.committed().build();
    assertThat(set.floor()).isEqualTo(seal.record().top());
    assertThat(set.pages()).containsExactly(seal.ref());
    // Below the floor: neither the late id nor the copy is judged
    assertThat(result.builder().build()).doesNotContainKey("late").containsEntry("k0", 1);
    assertThat(result.outcome()).isEqualTo(Replay.Outcome.NOT_SEEN);
    assertThat(result.seen()).isTrue();
    assertThat(result.intentionRecords()).isEqualTo(log.size() + 2); // the PageOut not counted

    // The late id lands first: the block is not the one the PageOut sealed, which changes nothing
    List<byte[]> lateFirst = new ArrayList<>(log);
    lateFirst.add(record(late, "late", 0));
    CommittedSet unsealed = Replay.replay(MACHINE, null, lateFirst, null).committed().build();
    lateFirst.add(pageOut);
    Replay.Result<ToyMachine.Versions> refused = Replay.replay(MACHINE, null, lateFirst, null);
    assertThat(refused.builder().build()).containsEntry("late", 1);
    assertThat(refused.committed().build().bytes()).isEqualTo(unsealed.bytes());

    // Twice: the second changes nothing
    List<byte[]> twice = new ArrayList<>(log);
    twice.add(pageOut);
    CommittedSet once = Replay.replay(MACHINE, null, twice, null).committed().build();
    twice.add(pageOut);
    assertThat(Replay.replay(MACHINE, null, twice, null).committed().build().bytes())
        .isEqualTo(once.bytes());

    // From a checkpoint of the sealed set, replay goes on the same
    byte[] checkpoint = LogEnvelope.encodeCheckpoint(id(1), once.bytes(),
        MACHINE.encodeCheckpoint(Replay.replay(MACHINE, null, twice, null).builder().build()));
    Replay.Result<ToyMachine.Versions> resumed =
        Replay.replay(MACHINE, checkpoint, List.of(record(late, "late", 0)), late);
    assertThat(resumed.outcome()).isEqualTo(Replay.Outcome.NOT_SEEN);
    assertThat(resumed.committed().build().bytes()).isEqualTo(once.bytes());
  }

  @Test
  void aRecordWithoutAUuidV7IsRejected() {
    UUID v4 = UUID.randomUUID();
    Replay.Result<ToyMachine.Versions> result =
        Replay.replay(MACHINE, null, List.of(record(v4, "k", 0)), v4);
    assertThat(result.builder().build()).doesNotContainKey("k");
    assertThat(result.outcome()).isEqualTo(Replay.Outcome.REJECTED);
  }

  @Test
  void seesAWatchedRecordEvenWhenItSkipsIt() {
    UUID a = id(100);
    // A copy of a record a checkpoint holds: replay skips it, but saw it.
    byte[] checkpoint = LogEnvelope.encodeCheckpoint(id(1),
        CommittedSet.builder().add(a).build().bytes(), MACHINE.encodeCheckpoint(Map.of("k", 1)));
    Replay.Result<ToyMachine.Versions> result =
        Replay.replay(MACHINE, checkpoint, List.of(record(a, "k", 0)), a);
    assertThat(result.outcome()).isEqualTo(Replay.Outcome.NOT_SEEN);
    assertThat(result.seen()).isTrue();
    assertThat(Replay.replay(MACHINE, checkpoint, List.of(), a).seen()).isFalse();
  }

  @Test
  void replayOntoAReplicaContinuesWhereItsReadEnded() {
    List<byte[]> log = List.of(record(id(100), "k", 0), record(id(101), "j", 0),
        LogEnvelope.encodeAdvanceHorizon(100), record(id(102), "k", 1));
    Replay.Result<ToyMachine.Versions> whole = Replay.replay(MACHINE, null, log, null);
    Replay.Result<ToyMachine.Versions> first =
        Replay.replay(MACHINE, null, log.subList(0, 2), null);
    Replay.Result<ToyMachine.Versions> rest = Replay.replayOnto(MACHINE,
        MACHINE.toBuilder(first.builder().build()), first.committed().build().toBuilder(), null,
        first.intentionRecords(), log.subList(2, 4), null);
    assertThat(rest.builder().build()).isEqualTo(whole.builder().build());
    assertThat(rest.committed().build().bytes()).isEqualTo(whole.committed().build().bytes());
    assertThat(rest.intentionRecords()).isEqualTo(whole.intentionRecords());
  }

  /**
   * The records and bytes since the last GC record start again at each one, where a compaction
   * began, and continue from a replica's counts in a fold; the intention records after the
   * checkpoint do not start again.
   */
  @Test
  void countsSinceTheLastGcRecordStartAgainThere() {
    byte[] after = record(id(103), "j", 1);
    List<byte[]> log = List.of(record(id(100), "k", 0), record(id(101), "j", 0),
        LogEnvelope.encodeAdvanceHorizon(100), record(id(102), "k", 1), after);
    Replay.Result<ToyMachine.Versions> whole = Replay.replay(MACHINE, null, log, null);
    assertThat(whole.intentionRecords()).isEqualTo(4);
    assertThat(whole.recordsSinceGc()).isEqualTo(2);
    assertThat(whole.bytesSinceGc()).isEqualTo(log.get(3).length + after.length);

    Replay.Result<ToyMachine.Versions> first =
        Replay.replay(MACHINE, null, log.subList(0, 4), null);
    Replay.Result<ToyMachine.Versions> rest = Replay.replayOnto(MACHINE,
        MACHINE.toBuilder(first.builder().build()), first.committed().build().toBuilder(), null,
        first.intentionRecords(), first.recordsSinceGc(), first.bytesSinceGc(),
        log.subList(4, 5), null, null);
    assertThat(rest.recordsSinceGc()).isEqualTo(whole.recordsSinceGc());
    assertThat(rest.bytesSinceGc()).isEqualTo(whole.bytesSinceGc());

    Replay.Result<ToyMachine.Versions> none = Replay.replay(MACHINE, null, log.subList(0, 2), null);
    assertThat(none.recordsSinceGc()).as("no GC record: since the checkpoint").isEqualTo(2);
  }
}
