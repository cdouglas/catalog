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
 * end, appends delayed until after an end, one to three instances with stale catalog objects, each
 * given its own segment prefix, the default or none (the catalog object's wins: D13), and sweeps
 * at a retirement TTL of 0. At the drain, every read must read on to the log's end.
 */
class TestSegmentedCommitProtocol {

  /**
   * Instances given a prefix their catalog object overrides warn once each (D13), which would be
   * thousands of lines a run; held here, as a level set on a logger nothing holds may be lost.
   */
  private static final java.util.logging.Logger QUIET = quiet();

  private static java.util.logging.Logger quiet() {
    java.util.logging.Logger jul = java.util.logging.Logger.getLogger(SegmentedLog.class.getName());
    jul.setLevel(java.util.logging.Level.SEVERE);
    return jul;
  }

  @Property(tries = 2000)
  void commitsAreHonestAndCopiesNeverApply(
      @ForAll("limits") long limit,
      @ForAll("blocks") long extraBlocks,
      @ForAll boolean copies,
      @ForAll("stores") String stores,
      @ForAll("instances") int instances,
      @ForAll("prefixes") String prefixes,
      @ForAll long seed,
      @ForAll("settings") CommitProtocol.Settings settings,
      @ForAll("schedules") List<CommitProtocol.Cmd> schedule) {
    run(limit, extraBlocks, copies, stores, instances, prefixes, seed, settings, schedule);
  }

  static void run(long limit, long extraBlocks, boolean copies, String stores, int instances,
      String prefixes, long seed, CommitProtocol.Settings settings,
      List<CommitProtocol.Cmd> schedule) {
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    SegmentedLogHarness.Segments segments = stores.equals("segments")
        ? SegmentedLogHarness.Segments.of(
            new InMemorySegmentStore(limit + extraBlocks, 1L << 20, 64L << 20))
        : SegmentedLogHarness.Segments.of(
            new InMemoryAppendBlobStore((int) (limit + extraBlocks), () -> 0L),
            stores.equals("blobs"));
    SegmentStore store = segments.store();
    SplittableRandom random = new SplittableRandom(seed);
    String defaultPrefix = prefixes.equals("configured") ? null : "segments";
    List<SegmentedLog> logs = new ArrayList<>();
    for (int i = 0; i < instances; i++) {
      AtomicFileLog catalog =
          AtomicFileLog.segmented(io, "mem:///log", random.split()::nextLong, false);
      String prefix = switch (prefixes) {
        case "configured" -> "segments";
        case "default" -> null;
        default -> new String[] {"segments", null, "elsewhere-" + i}[random.nextInt(3)];
      };
      logs.add(new SegmentedLog(catalog, store, prefix, defaultPrefix,
          SegmentedLog.Options.defaults().withLimit(limit).withRetireTtlMs(0)
              .withClock(() -> 0L).withCopies(copies)));
    }
    CommitProtocol.run(new SegmentedLogHarness(io, segments, "mem:///log", "segments",
        defaultPrefix, logs, EntityVersionsMachine.INSTANCE), settings, schedule);
  }

  /**
   * How the instances are given the segments' prefix (D13): each {@code segments} and no default
   * (the catalog object records it); none, and the default {@code segments} (it records none); or
   * the default and each its own, the default or none, so that whichever creates the log, the
   * others keep its segments where its catalog object says.
   */
  @Provide
  Arbitrary<String> prefixes() {
    return Arbitraries.of("configured", "default", "mixed");
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
