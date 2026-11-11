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

import java.util.ArrayList;
import java.util.List;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.intentlog.testkit.CommitProtocol;
import org.intentlog.testkit.EntityVersionsMachine;

/**
 * The commit-protocol fuzzer ({@link CommitProtocol}) on the Azure log over the in-memory store:
 * appends blind or conditional; segments small enough that the log moves on every few records,
 * and Append Blobs that hold few blocks, so that segments close by a pad and by the block limit;
 * every fault of every store request (head CASes, pads, candidates and checkpoints included),
 * delayed appends that land after a close, short reads of segments, and the sweep; and one to
 * three instances of the log taking requests in turn, as processes whose cached heads are stale.
 */
class TestAzureCommitProtocol {

  @Property(tries = 2000)
  void commitsAreHonestAndCopiesNeverApply(
      @ForAll boolean conditional,
      @ForAll("limits") long segmentLimit,
      @ForAll("blocks") int maxBlocks,
      @ForAll("instances") int instances,
      @ForAll("settings") CommitProtocol.Settings settings,
      @ForAll("schedules") List<CommitProtocol.Cmd> schedule) {
    InMemoryAppendBlobStore store = new InMemoryAppendBlobStore(maxBlocks, () -> 0L);
    List<AppendBlobLog> logs = new ArrayList<>();
    for (int i = 0; i < instances; i++) {
      logs.add(new AppendBlobLog(store, "catalog.log",
          AppendBlobLog.Options.defaults().withConditional(conditional)
              .withSegmentLimit(segmentLimit).withTrimGraceMs(0).withClock(() -> 0L)));
    }
    CommitProtocol.run(
        new AzureLogHarness(ObservedStore.of(store), logs, "catalog.log",
            EntityVersionsMachine.INSTANCE),
        settings, schedule);
  }

  @Provide
  Arbitrary<Long> limits() {
    return Arbitraries.of(300L, 600L, 1200L, 1L << 20);
  }

  /** Logs of one root in several processes, each with its own cached head. */
  @Provide
  Arbitrary<Integer> instances() {
    return Arbitraries.of(1, 2, 3);
  }

  @Provide
  Arbitrary<Integer> blocks() {
    return Arbitraries.of(2, 4, 50_000);
  }

  @Provide
  Arbitrary<CommitProtocol.Settings> settings() {
    return CommitProtocol.settings();
  }

  @Provide
  Arbitrary<List<CommitProtocol.Cmd>> schedules() {
    return CommitProtocol.schedules();
  }
}
