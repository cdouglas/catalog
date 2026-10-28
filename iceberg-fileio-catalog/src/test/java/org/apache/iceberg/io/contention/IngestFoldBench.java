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
package org.apache.iceberg.io.contention;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.io.ContentionAccess;
import org.intentlog.CommittedSet;
import org.intentlog.Intention;
import org.intentlog.LogView;
import org.intentlog.Replay;
import org.intentlog.StateMachine;
import org.intentlog.fileio.AtomicFileLog;
import org.intentlog.wire.LogEnvelope;
import org.intentlog.wire.UuidV7;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * IngestBench's pass as a JMH benchmark, for profiling (async-profiler: {@code -prof
 * async:libPath=...}): one operation folds the next {@code batch} records of a captured log onto
 * the state and committed set the records before them reached, and builds both, as an ingest
 * thread that publishes after each batch does. The log's ids are retimed to {@code rate}; the
 * state starts after the first {@code start} records, with the committed set {@code empty} (only
 * the log's ids), {@code steady} (prefilled with rate x retention ids, as IngestRun's prefill), or
 * {@code discard} (no ids kept). At the log's end it starts again from there. The {@code records}
 * counter is records/s. {@link #encode} and {@link #decode} time a compaction's checkpoint of the
 * state at the start, and a cold reader's decode of it.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = {"-Xmx6g", "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn"})
public class IngestFoldBench {

  /** A capture's log.bin (IngestBench caches captures under bench/results/captures/). */
  @Param({""})
  public String log;

  @Param({"1000"})
  public double rate;

  @Param({"1", "64"})
  public int batch;

  @Param({"empty", "steady", "discard"})
  public String committed;

  @Param({"21600"})
  public double retentionS;

  @Param({"20000"})
  public int start;

  private Folder<?, ?, ?> folder;

  /** Records folded, reported as a rate. */
  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.OPERATIONS)
  public static class Records {
    public long records;
  }

  @Setup(Level.Trial)
  public void setup() throws IOException {
    if (log.isEmpty()) {
      throw new IllegalArgumentException("Pass a capture's log.bin: -p log=<path>");
    }
    LogView captured = AtomicFileLog.decode(Files.readAllBytes(Paths.get(log)));
    List<byte[]> records = IngestRun.retimed(captured.records(), rate);
    folder = Folder.of(ContentionAccess.machine(), captured.checkpoint().orElseThrow(), records,
        start, committed, rate, retentionS);
  }

  @Benchmark
  public Object fold(Records counter) {
    counter.records += folder.fold(batch);
    return folder.state;
  }

  @Benchmark
  public byte[] encode() {
    return folder.encode();
  }

  @Benchmark
  public Object decode() {
    return folder.decode();
  }

  /** The fold's state, with the state machine's types bound. */
  private static final class Folder<S, B extends StateMachine.Builder<S>, I extends Intention<B>> {
    private final StateMachine<S, B, I> machine;
    private final UUID logId;
    private final List<byte[]> records;
    private final boolean discard;
    private final S startState;
    private final CommittedSet startCommitted;
    private final long startIntentions;
    private S state;
    private CommittedSet set;
    private long intentions;
    private int at;

    private Folder(StateMachine<S, B, I> machine, UUID logId, List<byte[]> records,
        boolean discard, S state, CommittedSet set, long intentions) {
      this.machine = machine;
      this.logId = logId;
      this.records = records;
      this.discard = discard;
      this.startState = state;
      this.startCommitted = set;
      this.startIntentions = intentions;
      this.state = state;
      this.set = set;
      this.intentions = intentions;
    }

    static <S, B extends StateMachine.Builder<S>, I extends Intention<B>> Folder<S, B, I> of(
        StateMachine<S, B, I> machine, byte[] checkpoint, List<byte[]> records, int start,
        String committed, double rate, double retentionS) {
      boolean discard = committed.equals("discard");
      LogEnvelope.Checkpoint envelope = LogEnvelope.decodeCheckpoint(checkpoint);
      CommittedSet set = discard ? CommittedSet.EMPTY : CommittedSet.of(envelope.committed());
      if (committed.equals("steady")) {
        CommittedSet.Builder merged = IngestRun.prefill(
            UuidV7.timestampMs(LogEnvelope.decodeRecord(records.get(0)).id()), rate, retentionS,
            0).toBuilder();
        set.ids().forEach(merged::add);
        set = merged.build();
      } else if (!discard && !committed.equals("empty")) {
        throw new IllegalArgumentException("committed is empty, steady or discard: " + committed);
      }
      int from = Math.min(start, records.size() / 2);
      Replay.Result<B> before = Replay.replayOnto(machine,
          machine.decodeCheckpoint(envelope.state()), set.toBuilder(),
          envelope.logId(), 0, records.subList(0, from), null);
      return new Folder<>(machine, envelope.logId(), records.subList(from, records.size()),
          discard, before.builder().build(),
          discard ? CommittedSet.EMPTY : before.committed().build(), before.intentionRecords());
    }

    private byte[] checkpoint;

    byte[] encode() {
      return LogEnvelope.encodeCheckpoint(logId, startCommitted.bytes(),
          machine.encodeCheckpoint(startState));
    }

    S decode() {
      if (checkpoint == null) {
        checkpoint = encode();
      }
      return Replay.replay(machine, checkpoint, List.of(), null).builder().build();
    }

    int fold(int batch) {
      if (at + batch > records.size()) {
        state = startState;
        set = startCommitted;
        intentions = startIntentions;
        at = 0;
      }
      Replay.Result<B> result = Replay.replayOnto(machine, machine.toBuilder(state),
          set.toBuilder(), logId, intentions, records.subList(at, at + batch), null);
      state = result.builder().build();
      set = discard ? CommittedSet.EMPTY : result.committed().build();
      intentions = result.intentionRecords();
      at += batch;
      return batch;
    }
  }
}
