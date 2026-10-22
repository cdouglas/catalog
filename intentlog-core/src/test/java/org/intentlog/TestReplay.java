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

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
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
  void aRecordWithoutAUuidV7IsRejected() {
    UUID v4 = UUID.randomUUID();
    Replay.Result<ToyMachine.Versions> result =
        Replay.replay(MACHINE, null, List.of(record(v4, "k", 0)), v4);
    assertThat(result.builder().build()).doesNotContainKey("k");
    assertThat(result.outcome()).isEqualTo(Replay.Outcome.REJECTED);
  }
}
