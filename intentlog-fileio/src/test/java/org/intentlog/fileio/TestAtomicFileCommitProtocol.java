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
 * binding at the open, or pinned, live or spliced after a lookup (TAIL_READS.md, errata T6).
 */
class TestAtomicFileCommitProtocol {

  @Property(tries = 5000)
  void commitsAreHonestAndCopiesNeverApply(
      @ForAll boolean append,
      @ForAll InMemoryAtomicFileIO.ReadMode reads,
      @ForAll long seed,
      @ForAll("settings") CommitProtocol.Settings settings,
      @ForAll("schedules") List<CommitProtocol.Cmd> schedule) {
    CommitProtocol.run(new AtomicFileLogHarness(new InMemoryAtomicFileIO(append), "mem:///log",
        EntityVersionsMachine.INSTANCE, reads, seed), settings, schedule);
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
