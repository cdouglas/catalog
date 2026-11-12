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

import java.util.List;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.apache.iceberg.io.InMemoryAtomicFileIO;
import org.intentlog.testkit.CommitProtocol;
import org.intentlog.testkit.EntityVersionsMachine;

/**
 * The commit-protocol fuzzer ({@link CommitProtocol}) on one object of a store, with offset
 * appends or CAS only, its writers compacting by the record count drawn, and duplicates of records
 * that compaction folded into a checkpoint, or GC'd. The store reads in a mode drawn per try:
 * binding at the open, or pinned, live or spliced after a lookup (TAIL_READS.md, errata T6); the
 * check of each tail read against a whole read runs under the first two. Whether its reads record
 * the version they find is drawn too: if they do (S3, ADLS), a read is one GET with no lookup
 * before it, and a tail read that finds the object replaced reads the rest of the version it found
 * by a second GET, which the modes after the first may serve live or spliced (step 7).
 */
class TestAtomicFileCommitProtocol {

  @Property(tries = 5000)
  void commitsAreHonestAndCopiesNeverApply(
      @ForAll boolean append,
      @ForAll InMemoryAtomicFileIO.ReadMode reads,
      @ForAll boolean recordingReads,
      @ForAll long seed,
      @ForAll("settings") CommitProtocol.Settings settings,
      @ForAll("schedules") List<CommitProtocol.Cmd> schedule) {
    boolean pinned = reads == InMemoryAtomicFileIO.ReadMode.OPEN
        || reads == InMemoryAtomicFileIO.ReadMode.PINNED;
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO(append);
    io.setReadsRecordVersion(recordingReads);
    withTailReadCheck(pinned, () -> CommitProtocol.run(new AtomicFileLogHarness(
        io, "mem:///log", EntityVersionsMachine.INSTANCE, reads, seed), settings, schedule));
  }

  /**
   * Runs {@code run} with the suites' check of each tail read against a whole read of the version
   * it found (AtomicFileLog.CHECK_TAIL_READS) on only if {@code on}. The check's read is a request
   * of its own, where this fuzzer may preempt the commit. A store that binds the version as the
   * stream opens serves that version, and one that pins the GET to it fails the read as stale,
   * where the check says nothing; one that serves the live version (LIVE, SPLICE) serves what the
   * preempting commit wrote, and a replace of the same length reads as a disagreement, the
   * commit's outcome unknown (seed 5557352659504107710 at 0a00685, whose whole read of a longer object on a store
   * without appends is a request fewer and moved the preemption onto the check's read; the view
   * the tail read returned was the version's). There the oracle of held states still checks every
   * view a read returns, as in the fault and multi-writer fuzzers, which run without the check.
   */
  private static void withTailReadCheck(boolean on, Runnable run) {
    String check = System.getProperty(AtomicFileLog.CHECK_TAIL_READS);
    if (!on) {
      System.setProperty(AtomicFileLog.CHECK_TAIL_READS, "false");
    }
    try {
      run.run();
    } finally {
      if (check == null) {
        System.clearProperty(AtomicFileLog.CHECK_TAIL_READS);
      } else {
        System.setProperty(AtomicFileLog.CHECK_TAIL_READS, check);
      }
    }
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
