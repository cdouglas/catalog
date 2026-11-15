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

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.apache.iceberg.io.InMemoryAtomicFileIO;
import org.intentlog.azure.InMemoryAppendBlobStore;
import org.intentlog.memory.InMemorySegmentStore;
import org.intentlog.segments.SegmentStore;
import org.intentlog.testkit.CommitProtocol;
import org.intentlog.testkit.EntityVersionsMachine;

/**
 * The testkit's commit-protocol fuzzer on {@link SegmentedLog} (docs/LOG_SEGMENTS.md "Testing"):
 * faults on every request of both stores (the segments' in memory, or Azure Append Blobs in memory
 * through AppendBlobSegments), small record limits and block limits (segments end by
 * their limit, by seal frames and at the block limit), Installs ahead, copies near a segment's
 * end, appends delayed until after an end, one to three instances with stale catalog objects, and
 * sweeps at a retirement TTL of 0. At the drain, every read must read on to the log's end.
 */
class TestSegmentedCommitProtocol {

  @Property(tries = 2000)
  void commitsAreHonestAndCopiesNeverApply(
      @ForAll("limits") long limit,
      @ForAll("blocks") long extraBlocks,
      @ForAll boolean copies,
      @ForAll("stores") String stores,
      @ForAll("instances") int instances,
      @ForAll long seed,
      @ForAll("settings") CommitProtocol.Settings settings,
      @ForAll("schedules") List<CommitProtocol.Cmd> schedule) {
    run(limit, extraBlocks, copies, stores, instances, seed, settings, schedule);
  }

  static void run(long limit, long extraBlocks, boolean copies, String stores, int instances,
      long seed, CommitProtocol.Settings settings, List<CommitProtocol.Cmd> schedule) {
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    SegmentedLogHarness.Segments segments = stores.equals("segments")
        ? SegmentedLogHarness.Segments.of(
            new InMemorySegmentStore(limit + extraBlocks, 1L << 20, 64L << 20))
        : SegmentedLogHarness.Segments.of(
            new InMemoryAppendBlobStore((int) (limit + extraBlocks), () -> 0L),
            stores.equals("blobs"));
    SegmentStore store = segments.store();
    SplittableRandom random = new SplittableRandom(seed);
    List<SegmentedLog> logs = new ArrayList<>();
    for (int i = 0; i < instances; i++) {
      AtomicFileLog catalog =
          AtomicFileLog.segmented(io, "mem:///log", random.split()::nextLong, false);
      logs.add(new SegmentedLog(catalog, store, "segments",
          SegmentedLog.Options.defaults().withLimit(limit).withRetireTtlMs(0)
              .withClock(() -> 0L).withCopies(copies)));
    }
    CommitProtocol.run(new SegmentedLogHarness(io, segments, "mem:///log", "segments", logs,
        EntityVersionsMachine.INSTANCE), settings, schedule);
  }

  /**
   * The segments' store: in memory, or Azure Append Blobs in memory whose appends say their block
   * count or don't.
   */
  @Provide
  Arbitrary<String> stores() {
    return Arbitraries.of("segments", "blobs", "uncounted-blobs");
  }

  /** K: segments of one record, a few, or many (the store's block limit is K or more). */
  @Provide
  Arbitrary<Long> limits() {
    return Arbitraries.of(1L, 2L, 3L, 5L, 1000L);
  }

  /** Blocks past K a segment holds: none (K is the block limit), or room for void appends. */
  @Provide
  Arbitrary<Long> blocks() {
    return Arbitraries.of(0L, 1L, 4L);
  }

  /** Logs of one catalog in several processes, each with its own catalog object and views. */
  @Provide
  Arbitrary<Integer> instances() {
    return Arbitraries.of(1, 2, 3);
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
