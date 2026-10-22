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

import java.time.Duration;
import java.util.function.LongSupplier;

/**
 * How a {@link LogRuntime} commits.
 *
 * @param maxAttempts the writes a commit tries before it gives up (10)
 * @param maxThrottleAttempts the times a write is sent while storage throttles it (8)
 * @param throttleMinWaitMs the first backoff after a throttled write; later ones double, up to the
 *     larger of 5 s and this, each plus up to 50% jitter
 * @param committedRetentionMs how long the committed set keeps an id: a compaction's GC record
 *     drops those older ({@code Long.MAX_VALUE}: never)
 * @param compaction when a commit compacts
 * @param visibilityWait how long a read through a write waits for it to be visible
 * @param clockMs the clock GC watermarks are taken from
 */
public record RuntimeConfig(
    int maxAttempts,
    int maxThrottleAttempts,
    long throttleMinWaitMs,
    long committedRetentionMs,
    CompactionPolicy compaction,
    Duration visibilityWait,
    LongSupplier clockMs) {

  public static final int DEFAULT_MAX_ATTEMPTS = 10;
  public static final int DEFAULT_MAX_THROTTLE_ATTEMPTS = 8;

  /** The defaults, with the given compaction policy, retention and throttle backoff. */
  public static RuntimeConfig of(
      CompactionPolicy compaction, long committedRetentionMs, long throttleMinWaitMs) {
    return new RuntimeConfig(
        DEFAULT_MAX_ATTEMPTS,
        DEFAULT_MAX_THROTTLE_ATTEMPTS,
        throttleMinWaitMs,
        committedRetentionMs,
        compaction,
        Duration.ZERO,
        System::currentTimeMillis);
  }
}
