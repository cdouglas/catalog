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
package org.intentlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** {@link CompactionPolicy#share}: a log with checkpoints apart compacting by its records' bytes. */
class TestCompactionPolicy {

  private static final long MIB = 1L << 20;

  /** A log whose checkpoint is {@code checkpoint} bytes, with {@code records} records of {@code tail} bytes after it. */
  private static CompactionPolicy.Input log(long records, long checkpoint, long tail) {
    return new CompactionPolicy.Input(records, checkpoint, checkpoint + tail, 0);
  }

  @Test
  void compactsWhenTheRecordsReachTheirShareOfTheCheckpoint() {
    CompactionPolicy policy = CompactionPolicy.share(1.0, MIB, 16 * MIB, 10_000);
    assertThat(policy.due(log(100, 4 * MIB, 4 * MIB - 1))).isFalse();
    assertThat(policy.due(log(100, 4 * MIB, 4 * MIB))).isTrue();
    assertThat(policy.progress(log(100, 4 * MIB, 2 * MIB))).isEqualTo(0.5);
    assertThat(policy.progress(log(100, 4 * MIB, 6 * MIB))).isEqualTo(1.5);
    // half a share: twice as often
    assertThat(CompactionPolicy.share(0.5, MIB, 16 * MIB, 10_000).due(log(100, 4 * MIB, 2 * MIB)))
        .isTrue();
  }

  @Test
  void aSmallCheckpointWaitsForTheFloorAndALargeOneForTheCeiling() {
    CompactionPolicy policy = CompactionPolicy.share(1.0, MIB, 16 * MIB, 10_000);
    // a pointer catalog's checkpoint of 20 KB: not every 20 KB of records, but every MiB
    assertThat(policy.due(log(100, 20_000, 500_000))).isFalse();
    assertThat(policy.due(log(100, 20_000, MIB))).isTrue();
    // an inline catalog's of 100 MiB: at 16 MiB of records, not 100
    assertThat(policy.due(log(100, 100 * MIB, 16 * MIB - 1))).isFalse();
    assertThat(policy.due(log(100, 100 * MIB, 16 * MIB))).isTrue();
  }

  @Test
  void theRecordCountBoundsItWhateverTheBytes() {
    CompactionPolicy policy = CompactionPolicy.share(1.0, MIB, 16 * MIB, 1_000);
    assertThat(policy.due(log(999, 100 * MIB, 1_000))).isFalse();
    assertThat(policy.due(log(1_000, 100 * MIB, 1_000))).isTrue();
    assertThat(policy.progress(log(2_000, 100 * MIB, 1_000))).isEqualTo(2.0);
    assertThat(CompactionPolicy.share(1.0, MIB, 16 * MIB, 0).due(log(0, MIB, 0)))
        .as("a count of 0: every commit").isTrue();
  }

  @Test
  void refusesAShareOfNothing() {
    assertThatThrownBy(() -> CompactionPolicy.share(0, MIB, 16 * MIB, 1_000))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
