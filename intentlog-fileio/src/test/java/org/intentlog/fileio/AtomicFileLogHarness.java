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
package org.intentlog.fileio;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.InMemoryAtomicFileIO;
import org.intentlog.CommittedSet;
import org.intentlog.Intention;
import org.intentlog.LogView;
import org.intentlog.Replay;
import org.intentlog.SharedLog;
import org.intentlog.StateMachine;
import org.intentlog.testkit.LogHarness;
import org.intentlog.wire.LogEnvelope;

/**
 * An {@link AtomicFileLog} over an {@link InMemoryAtomicFileIO}, dressed for fuzzing. The store
 * cannot delay a write or serve a stale read, so those faults act as a failure before the write and
 * as none. A copy of a record is appended at the object's offset, or, on a store without append,
 * by a CAS of the object with the copy after its records (another writer's write either way).
 * Every other CAS must write the replay of the version it replaced (I11), checked with {@code
 * machine}.
 */
public final class AtomicFileLogHarness implements LogHarness {

  private final InMemoryAtomicFileIO io;
  private final AtomicFileLog log;
  private final StateMachine<?, ?, ?> machine;
  private Hooks hooks = null;

  public AtomicFileLogHarness(InMemoryAtomicFileIO io, String location,
      StateMachine<?, ?, ?> machine) {
    this.io = io;
    this.log = new AtomicFileLog(io, location);
    this.machine = machine;
    io.injectFaults((loc, strategy) -> hooks == null ? InMemoryAtomicFileIO.Fault.NONE
        : fault(hooks.beforeWrite()));
    io.injectReadFaults(loc -> hooks == null ? InMemoryAtomicFileIO.ReadFault.NONE
        : hooks.beforeRead() == ReadFault.FAIL
            ? InMemoryAtomicFileIO.ReadFault.FAIL : InMemoryAtomicFileIO.ReadFault.NONE);
  }

  private static InMemoryAtomicFileIO.Fault fault(WriteFault fault) {
    switch (fault) {
      case LOST_REPLY:
        return InMemoryAtomicFileIO.Fault.LOST_REPLY;
      case THROTTLED:
        return InMemoryAtomicFileIO.Fault.THROTTLED;
      case LANDED_THROTTLED:
        return InMemoryAtomicFileIO.Fault.LANDED_THROTTLED;
      case LANDED_AS_CONFLICT:
        return InMemoryAtomicFileIO.Fault.LANDED_AS_CONFLICT;
      case FAIL_BEFORE:
      case NOT_SENT:
      case DELAYED:
        return InMemoryAtomicFileIO.Fault.FAIL_BEFORE;
      default:
        return InMemoryAtomicFileIO.Fault.NONE;
    }
  }

  public InMemoryAtomicFileIO io() {
    return io;
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
    return io.history().size();
  }

  @Override
  public List<byte[]> landedRecords() {
    Set<ByteBuffer> seen = new LinkedHashSet<>();
    for (InMemoryAtomicFileIO.Write write : io.history()) {
      for (byte[] record : AtomicFileLog.decode(write.after()).records()) {
        if (!LogEnvelope.decodeRecord(record).isAdvanceHorizon()) {
          seen.add(ByteBuffer.wrap(record));
        }
      }
    }
    List<byte[]> records = new ArrayList<>();
    seen.forEach(b -> records.add(b.array()));
    return records;
  }

  @Override
  public void appendCopy(byte[] record) {
    Hooks was = hooks;
    hooks = null;
    try {
      if (io.supportsAppend()) {
        log.append(record, log.probe());
      } else {
        LogView view = log.read();
        List<byte[]> records = new ArrayList<>(view.records());
        records.add(record);
        log.replace(view.tail(), view.checkpoint().orElseThrow(), records);
      }
    } finally {
      hooks = was;
    }
  }

  @Override
  public int deliverDelayed() {
    return 0;
  }

  @Override
  public List<String> checkpointViolations() {
    return violations(machine);
  }

  private <S, B extends StateMachine.Builder<S>, I extends Intention<B>> List<String> violations(
      StateMachine<S, B, I> sm) {
    List<String> violations = new ArrayList<>();
    List<InMemoryAtomicFileIO.Write> history = io.history();
    for (int i = 0; i < history.size(); i++) {
      InMemoryAtomicFileIO.Write write = history.get(i);
      if (write.strategy() != AtomicOutputFile.Strategy.CAS || write.before() == null) {
        continue;
      }
      LogView before = AtomicFileLog.decode(write.before());
      LogView after = AtomicFileLog.decode(write.after());
      if (Arrays.equals(before.checkpoint().orElseThrow(), after.checkpoint().orElseThrow())
          && startsWith(after.records(), before.records())) {
        continue; // an append by CAS (a copy appended on a store without append): no checkpoint
      }
      Replay.Result<B> replayed =
          Replay.replay(sm, before.checkpoint().orElse(null), before.records(), null);
      CommittedSet committed = replayed.committed().build();
      byte[] expected = LogEnvelope.encodeCheckpoint(replayed.logId(), committed.bytes(),
          sm.encodeCheckpoint(replayed.builder().build()));
      byte[] written = after.checkpoint().orElseThrow();
      if (!Arrays.equals(expected, written)) {
        violations.add("CAS #" + i + " did not checkpoint the replay of the version it replaced");
      }
    }
    return violations;
  }

  private static boolean startsWith(List<byte[]> records, List<byte[]> prefix) {
    if (records.size() < prefix.size()) {
      return false;
    }
    for (int i = 0; i < prefix.size(); i++) {
      if (!Arrays.equals(records.get(i), prefix.get(i))) {
        return false;
      }
    }
    return true;
  }
}
