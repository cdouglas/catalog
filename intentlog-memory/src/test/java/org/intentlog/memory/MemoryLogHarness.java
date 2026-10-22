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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.intentlog.CommittedSet;
import org.intentlog.Intention;
import org.intentlog.Replay;
import org.intentlog.SharedLog;
import org.intentlog.StateMachine;
import org.intentlog.memory.InMemorySharedLog.ReadShape;
import org.intentlog.memory.InMemorySharedLog.Request;
import org.intentlog.testkit.LogHarness;
import org.intentlog.wire.LogEnvelope;

/**
 * An {@link InMemorySharedLog} dressed for fuzzing. A stale read starts from the checkpoint before
 * the newest it could use, and lags the tail by two records. Checkpoints are checked against a
 * replay of every record ever appended, with {@code machine}.
 */
public final class MemoryLogHarness implements LogHarness {

  private final InMemorySharedLog log;
  private final StateMachine<?, ?, ?> machine;
  private Hooks hooks = null;

  public MemoryLogHarness(InMemorySharedLog log, StateMachine<?, ?, ?> machine) {
    this.log = log;
    this.machine = machine;
    log.intercept(new InMemorySharedLog.Interceptor() {
      @Override
      public InMemorySharedLog.WriteFault beforeWrite(Request request, byte[] payload) {
        return hooks == null
            ? InMemorySharedLog.WriteFault.NONE
            : InMemorySharedLog.WriteFault.valueOf(hooks.beforeWrite().name());
      }

      @Override
      public ReadShape beforeRead(Request request) {
        if (hooks == null) {
          return ReadShape.NORMAL;
        }
        switch (hooks.beforeRead()) {
          case FAIL:
            return ReadShape.FAIL;
          case STALE:
            return ReadShape.of(1, 2);
          case PAST_RECORD:
            return request == Request.READ_THROUGH ? ReadShape.PAST_RECORD : ReadShape.NORMAL;
          default:
            return ReadShape.NORMAL;
        }
      }
    });
  }

  public InMemorySharedLog memoryLog() {
    return log;
  }

  @Override
  public SharedLog log() {
    return log;
  }

  @Override
  public void hook(Hooks newHooks) {
    this.hooks = newHooks;
  }

  @Override
  public long landedWrites() {
    return log.history().stream().filter(w -> w.request() == Request.APPEND).count();
  }

  @Override
  public List<byte[]> landedRecords() {
    List<byte[]> intentions = new ArrayList<>();
    for (byte[] record : log.records()) {
      if (!LogEnvelope.decodeRecord(record).isAdvanceHorizon()) {
        intentions.add(record);
      }
    }
    return intentions;
  }

  @Override
  public void appendCopy(byte[] record) {
    Hooks was = hooks;
    hooks = null;
    try {
      log.append(record, null);
    } finally {
      hooks = was;
    }
  }

  @Override
  public int deliverDelayed() {
    return log.deliverDelayed();
  }

  @Override
  public List<String> checkpointViolations() {
    return violations(machine);
  }

  private <S, B extends StateMachine.Builder<S>, I extends Intention<B>> List<String> violations(
      StateMachine<S, B, I> sm) {
    List<String> violations = new ArrayList<>(log.violations());
    Map<Long, byte[]> checkpoints = log.checkpoints();
    List<byte[]> records = log.records();
    byte[] first = checkpoints.get(0L);
    // Every checkpoint is the replay of the log through its position (I11).
    for (Map.Entry<Long, byte[]> c : checkpoints.entrySet()) {
      byte[] replayed = checkpoint(sm, Replay.replay(sm, first,
          records.subList(0, c.getKey().intValue()), null));
      if (!Arrays.equals(replayed, c.getValue())) {
        violations.add(
            "the checkpoint at position " + c.getKey() + " is not the replay through it");
      }
    }
    // Readers from every usable checkpoint agree (Deterministic).
    byte[] whole = checkpoint(sm, Replay.replay(sm, first, records, null));
    for (Map.Entry<Long, byte[]> c : checkpoints.entrySet()) {
      if (c.getKey() >= log.trimmedThrough()) {
        byte[] from = checkpoint(sm, Replay.replay(sm, c.getValue(),
            records.subList(c.getKey().intValue(), records.size()), null));
        if (!Arrays.equals(from, whole)) {
          violations.add("a read from the checkpoint at " + c.getKey() + " disagrees");
        }
      }
    }
    return violations;
  }

  private static <S, B extends StateMachine.Builder<S>, I extends Intention<B>> byte[] checkpoint(
      StateMachine<S, B, I> sm, Replay.Result<B> result) {
    CommittedSet committed = result.committed().build();
    return LogEnvelope.encodeCheckpoint(
        result.logId(), committed.bytes(), sm.encodeCheckpoint(result.builder().build()));
  }
}
