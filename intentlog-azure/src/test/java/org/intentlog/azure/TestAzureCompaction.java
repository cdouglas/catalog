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
package org.intentlog.azure;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.intentlog.CompactionPolicy;
import org.intentlog.LogRuntime;
import org.intentlog.Replica;
import org.intentlog.RuntimeConfig;
import org.intentlog.testkit.EntityVersionsMachine;
import org.intentlog.testkit.EntityVersionsMachine.Builder;
import org.intentlog.testkit.EntityVersionsMachine.State;
import org.intentlog.testkit.EntityVersionsMachine.Txn;
import org.intentlog.wire.LogEnvelope;
import org.junit.jupiter.api.Test;

/**
 * When the Azure log compacts (AZURE_LOG.md "When to compact"): writers that read only what
 * follows their own tails never see each other's checkpoints, but every compaction begins with a
 * GC record they replay, and each counts the records since it; past the limits each compacts at a
 * point it draws. So writers taking turns past the limits compact about once per threshold
 * between them, not once each.
 */
class TestAzureCompaction {

  private static final String ROOT = "catalog.log";
  private static final int WRITERS = 8;
  private static final int COMMITS = 1_600;
  private static final int THRESHOLD = 100;

  private final InMemoryAppendBlobStore store = new InMemoryAppendBlobStore(50_000, () -> 0L);

  private LogRuntime<State, Builder, Txn> writer(long seed) {
    AppendBlobLog log = new AppendBlobLog(store, ROOT, AppendBlobLog.Options.defaults()
        .withSegmentLimit(1 << 20).withTrimGraceMs(0).withClock(() -> 0L));
    Random draws = new Random(seed);
    // by count alone: the floor's bytes are never reached
    CompactionPolicy policy = CompactionPolicy.share(1.0, 1L << 40, 1L << 40, THRESHOLD)
        .withDraws(draws::nextDouble);
    // a retention of a minute: every compaction appends a GC record
    return new LogRuntime<>(log, EntityVersionsMachine.INSTANCE,
        RuntimeConfig.of(policy, 60_000L, 0L, 0L));
  }

  @Test
  void writersTakingTurnsCompactAboutOncePerThreshold() {
    List<LogRuntime<State, Builder, Txn>> writers = new ArrayList<>();
    List<Replica<State>> replicas = new ArrayList<>();
    for (int w = 0; w < WRITERS; w++) {
      writers.add(writer(w));
    }
    Replica<State> created = writers.get(0).commit(writers.get(0).empty(),
        Txn.on(State.empty(), "create", Set.of(), Set.of("e0")));
    for (int w = 0; w < WRITERS; w++) {
      replicas.add(w == 0 ? created : writers.get(w).read());
    }
    for (int i = 0; i < COMMITS; i++) {
      int w = i % WRITERS;
      Replica<State> base = replicas.get(w);
      replicas.set(w, writers.get(w).commit(base,
          Txn.on(base.state(), "c" + i, Set.of(), Set.of("e" + w))));
    }

    long compactions = 0;
    long records = 0;
    for (AzureLogHistory.Landed l : AzureLogHistory.of(store, ROOT).records()) {
      if (LogEnvelope.decodeRecord(l.record()).isAdvanceHorizon()) {
        compactions++;
      } else {
        records++;
      }
    }
    assertThat(records).isEqualTo(COMMITS + 1);
    // each between 1x and 2x the threshold past the last: 8 to 16 for 1,600 commits, give or
    // take one; writers that each compacted once per threshold would have compacted about 128
    // times
    assertThat(compactions)
        .as("compactions of %d commits by %d writers past %d records", COMMITS, WRITERS,
            THRESHOLD)
        .isBetween((long) COMMITS / (2 * THRESHOLD) - 1, (long) COMMITS / THRESHOLD + 1);
  }
}
