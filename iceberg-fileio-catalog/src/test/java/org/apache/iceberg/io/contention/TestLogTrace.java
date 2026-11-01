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
package org.apache.iceberg.io.contention;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The log generator: a trace depends on its configuration and seed alone (the scheduler's turns,
 * not the host's threads, order the writers), and the oracle trace holds only entries that apply.
 */
class TestLogTrace {

  private static final String WORKLOAD =
      "{\"writers\": 4, \"tables\": 2, \"entries\": 60, \"preload_commits\": 5,"
          + " \"mix\": {\"fast_append\": 0.6, \"property\": 0.2, \"validated_overwrite\": 0.2},"
          + " \"runtime_steps\": {\"validated_overwrite\": 6}, \"expire_every\": 20,"
          + " \"expire_retain\": 5, \"encoding\": \"manifest-window\", \"oracle\": %s}";

  private static Map<String, Object> trace(Path dir, boolean oracle) throws Exception {
    BenchConfig config = BenchConfig.expand(
        BenchConfig.parse(String.format(WORKLOAD, oracle)), LogTrace.DEFAULTS,
        ContentionRun.LATER_KEYS).get(0);
    return new LogTrace(config, dir).run();
  }

  @Test
  void aTraceIsItsSeeds(@TempDir Path dir) throws Exception {
    Map<String, Object> first = trace(dir.resolve("a"), false);
    Map<String, Object> second = trace(dir.resolve("b"), false);

    for (String key : new String[] {"entries", "rejected", "rejection_reasons", "outcomes",
        "steps", "mean_window"}) {
      assertThat(second.get(key)).as(key).isEqualTo(first.get(key));
    }
    assertThat(Files.size(dir.resolve("a/log.bin"))).isPositive();
    assertThat(Files.readAllLines(dir.resolve("b/ops.jsonl")))
        .hasSameSizeAs(Files.readAllLines(dir.resolve("a/ops.jsonl")));
  }

  @Test
  void theOracleTraceHoldsOnlyEntriesThatApply(@TempDir Path dir) throws Exception {
    Map<String, Object> oracle = trace(dir, true);

    assertThat(oracle.get("rejected")).isEqualTo(0L);
    assertThat((Integer) oracle.get("entries")).isGreaterThanOrEqualTo(60);
  }
}
