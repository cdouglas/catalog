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
import net.jqwik.api.Example;
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
   * A compaction after a commit, which lost its race to a writer that replaced the log since, is
   * sent again only if the log continues the version it lost to; else it is dropped, and the
   * commits stand. Sent with the records past the count the lost version held (calibration mutant
   * compactreplaced, fuzz/mutants/compaction.sh), it left out a commit that had landed in between
   * ("w1.1 was committed and is not applied"). The fuzzer reached that at try 581,722 of 1,000,000
   * (probe p11-reach-creplaced, 2026-11-17), past the calibration's 300,000; this is its shrunk
   * sample (p11-shrink-creplaced, seed -8760416109953422105).
   */
  @Example
  void aCompactionLostToAReplaceIsNotSentAgainWithTheRecordsPastItsCount() {
    CommitProtocol.Settings settings =
        new CommitProtocol.Settings(2, true, CommitProtocol.NO_GC, 0L, 2, 0L, 48, 0L);
    List<CommitProtocol.Cmd> schedule = List.of(
        begin(0), commit(0), begin(0), begin(1), duplicate(0), commit(0), begin(0), begin(1),
        commit(0));
    commitsAreHonestAndCopiesNeverApply(
        true, InMemoryAtomicFileIO.ReadMode.OPEN, true, 0L, settings, schedule);
  }

  /**
   * Bug 122 (spliced read went backwards): on a store whose reads record their version, without
   * appends, w0's tail read after its lost replace found the object replaced (a copy of a record,
   * CAS'd after its records) and read that version whole by a second GET; w1's commit replaced it
   * with a shorter object first, and the store, splicing, served the version's bytes up to a
   * frame's end past the new object's end and nothing after. The commit loop took that prefix,
   * through p17, 6 records behind the replica w0 staged on, for the object, and rejected w0.0 as a
   * conflict against it (justify's oracle). Seed -8870318493459999813, try 3,746 of 5,000, shrunk.
   */
  @Example
  void aReadSplicedShortOfItsVersionIsReadAgain() {
    CommitProtocol.Settings settings =
        new CommitProtocol.Settings(2, true, CommitProtocol.NO_GC, 0L, -1, -1L, 24, 0L);
    List<CommitProtocol.Cmd> schedule = List.of(
        new CommitProtocol.Cmd(CommitProtocol.Step.BEGIN, 0, 1, 0, 0, 0, 0, 0),
        begin(0), begin(0), begin(0), begin(0), begin(0), begin(0), begin(1), duplicate(0),
        new CommitProtocol.Cmd(CommitProtocol.Step.COMMIT, 0, 13, 0, 0, 11, 0, 0));
    commitsAreHonestAndCopiesNeverApply(
        false, InMemoryAtomicFileIO.ReadMode.SPLICE, true, 3L, settings, schedule);
  }

  private static CommitProtocol.Cmd begin(int writer) {
    return new CommitProtocol.Cmd(CommitProtocol.Step.BEGIN, writer, 0, 0, 0, 0, 0, 0);
  }

  private static CommitProtocol.Cmd commit(int writer) {
    return new CommitProtocol.Cmd(CommitProtocol.Step.COMMIT, writer, 0, 0, 0, 0, 0, 0);
  }

  private static CommitProtocol.Cmd duplicate(int record) {
    return new CommitProtocol.Cmd(CommitProtocol.Step.DUPLICATE, 0, 0, 0, 0, 0, record, 0);
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
