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

import java.util.List;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.intentlog.testkit.CommitProtocol;
import org.intentlog.testkit.EntityVersionsMachine;

/**
 * The commit-protocol fuzzer ({@link CommitProtocol}) on the in-memory shared log: appends
 * unconditional or conditional, compaction by a compactor and by the writers, duplicates of
 * records that were checkpointed, trimmed and GC'd, delayed appends, and stale reads.
 */
class TestMemoryCommitProtocol {

  @Property(tries = 2000)
  void commitsAreHonestAndCopiesNeverApply(
      @ForAll boolean conditional,
      @ForAll("settings") CommitProtocol.Settings settings,
      @ForAll("schedules") List<CommitProtocol.Cmd> schedule) {
    InMemorySharedLog log = new InMemorySharedLog("log", conditional, true);
    CommitProtocol.run(new MemoryLogHarness(log, EntityVersionsMachine.INSTANCE), settings,
        schedule);
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
