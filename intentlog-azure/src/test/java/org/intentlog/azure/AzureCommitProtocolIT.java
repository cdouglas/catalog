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
import java.util.concurrent.atomic.AtomicInteger;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.ShrinkingMode;
import net.jqwik.api.lifecycle.AfterContainer;
import org.intentlog.testkit.CommitProtocol;
import org.intentlog.testkit.EntityVersionsMachine;

/**
 * The commit-protocol fuzzer with real requests ({@link CloudAccount}): each try a log of its
 * own, over {@link RecordingStore} on the account, whose faults are the in-memory store's
 * (injected around real requests) and whose oracles see what landed. Segments close every few
 * records (real pads, real CASes of the head); one to three instances of the log. No shrinking:
 * a failing try is reported as drawn. Run explicitly: {@code -Dtest=AzureCommitProtocolIT}.
 */
class AzureCommitProtocolIT {

  private static final String PREFIX = CloudAccount.RUN + "/fuzz";
  private static final AtomicInteger TRIES = new AtomicInteger();

  @Property(tries = 25, shrinking = ShrinkingMode.OFF)
  void commitsAreHonestAndCopiesNeverApply(
      @ForAll boolean conditional,
      @ForAll("limits") long segmentLimit,
      @ForAll("instances") int instances,
      @ForAll("settings") CommitProtocol.Settings settings,
      @ForAll("schedules") List<CommitProtocol.Cmd> schedule) {
    if (!CloudAccount.configured()) {
      return;
    }
    String root = PREFIX + "/" + TRIES.incrementAndGet();
    RecordingStore store = new RecordingStore(CloudAccount.store(), root + "/");
    List<AppendBlobLog> logs = new ArrayList<>();
    for (int i = 0; i < instances; i++) {
      logs.add(new AppendBlobLog(store, root,
          AppendBlobLog.Options.defaults().withConditional(conditional)
              .withSegmentLimit(segmentLimit).withTrimGraceMs(0)));
    }
    long start = System.nanoTime();
    CommitProtocol.run(
        new AzureLogHarness(store, logs, root, EntityVersionsMachine.INSTANCE), settings,
        schedule);
    long segments = store.appends().stream().map(a -> a.name()).distinct().count();
    long pads = store.appends().stream().filter(a -> a.block()[0] == 0).count();
    System.out.printf("try %s: %s, limit %d, %d instance(s): %d appends landed in %d segments,"
        + " %d pads, %.1f s%n", root, conditional ? "conditional" : "blind", segmentLimit,
        instances, store.appends().size(), segments, pads, (System.nanoTime() - start) / 1e9);
  }

  @AfterContainer
  static void deleteWhatItWrote() {
    if (CloudAccount.configured()) {
      System.out.println("deleted " + CloudAccount.deleteAll(PREFIX) + " blobs under " + PREFIX);
    }
  }

  @Provide
  Arbitrary<Long> limits() {
    return Arbitraries.of(600L, 1200L, 1L << 20);
  }

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
