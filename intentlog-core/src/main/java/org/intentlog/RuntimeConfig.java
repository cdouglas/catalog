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
 * @param raceMinWaitMs the bound of the first wait after a lost race (a conditional write that
 *     failed its condition), before the commit reads or probes the log again; each later lost race
 *     of the commit doubles it, up to the larger of 1 s and this. The wait is drawn uniformly below
 *     the bound (full jitter), so that writers who lost to one winner spread out (0: none)
 * @param committedRetentionMs how long the committed set keeps an id (R): a compaction's GC record
 *     drops those older ({@code Long.MAX_VALUE}: never). Within it, a writer learns whether its
 *     record applied exactly, from the inline set or a page
 * @param compaction when a commit compacts
 * @param visibilityWait how long a read through a write waits for it to be visible
 * @param clockMs the clock GC watermarks, paging ceilings and record ids are taken from
 * @param admissionWindowMs the admission window A: a compaction pages only blocks whose ids are
 *     all older than this, so a commit slower than it may find its id paged and its record
 *     rejected (a retry, never a wrong answer). Paging needs a retention above it, since a
 *     compactor applies its own GC first (docs/COMMITTED_PAGING.md)
 * @param pageThresholdBytes the threshold T: a compaction pages the oldest blocks while the inline
 *     set is larger ({@code Long.MAX_VALUE}: never page). A log without a {@link PageStore} never
 *     pages
 */
public record RuntimeConfig(
    int maxAttempts,
    int maxThrottleAttempts,
    long throttleMinWaitMs,
    long raceMinWaitMs,
    long committedRetentionMs,
    CompactionPolicy compaction,
    Duration visibilityWait,
    LongSupplier clockMs,
    long admissionWindowMs,
    long pageThresholdBytes,
    boolean tailReads) {

  public static final int DEFAULT_MAX_ATTEMPTS = 10;
  public static final int DEFAULT_MAX_THROTTLE_ATTEMPTS = 8;

  /** A, 5 minutes: provisional, until the experiments sweep it (COMMITTED_PAGING.md). */
  public static final long DEFAULT_ADMISSION_WINDOW_MS = 5L * 60 * 1000;

  /** T, 1 MiB. */
  public static final long DEFAULT_PAGE_THRESHOLD_BYTES = 1L << 20;

  public RuntimeConfig {
    if (admissionWindowMs < 0 || pageThresholdBytes < 0) {
      throw new IllegalArgumentException(String.format(
          "admissionWindowMs (%d) and pageThresholdBytes (%d) must be >= 0", admissionWindowMs,
          pageThresholdBytes));
    }
  }

  /**
   * The configuration without paging settings, which pages at the defaults ({@link
   * #DEFAULT_ADMISSION_WINDOW_MS}, {@link #DEFAULT_PAGE_THRESHOLD_BYTES}).
   */
  public RuntimeConfig(
      int maxAttempts, int maxThrottleAttempts, long throttleMinWaitMs, long raceMinWaitMs,
      long committedRetentionMs, CompactionPolicy compaction, Duration visibilityWait,
      LongSupplier clockMs) {
    this(maxAttempts, maxThrottleAttempts, throttleMinWaitMs, raceMinWaitMs, committedRetentionMs,
        compaction, visibilityWait, clockMs, DEFAULT_ADMISSION_WINDOW_MS,
        DEFAULT_PAGE_THRESHOLD_BYTES);
  }

  /** The configuration with tail reads ({@link #tailReads}, the default). */
  public RuntimeConfig(
      int maxAttempts, int maxThrottleAttempts, long throttleMinWaitMs, long raceMinWaitMs,
      long committedRetentionMs, CompactionPolicy compaction, Duration visibilityWait,
      LongSupplier clockMs, long admissionWindowMs, long pageThresholdBytes) {
    this(maxAttempts, maxThrottleAttempts, throttleMinWaitMs, raceMinWaitMs, committedRetentionMs,
        compaction, visibilityWait, clockMs, admissionWindowMs, pageThresholdBytes, true);
  }

  /** This configuration, paging at admission window {@code admissionMs} and threshold {@code bytes}. */
  public RuntimeConfig withPaging(long admissionMs, long bytes) {
    return new RuntimeConfig(maxAttempts, maxThrottleAttempts, throttleMinWaitMs, raceMinWaitMs,
        committedRetentionMs, compaction, visibilityWait, clockMs, admissionMs, bytes, tailReads);
  }

  /** This configuration, on clock {@code clock}. */
  public RuntimeConfig withClock(LongSupplier clock) {
    return new RuntimeConfig(maxAttempts, maxThrottleAttempts, throttleMinWaitMs, raceMinWaitMs,
        committedRetentionMs, compaction, visibilityWait, clock, admissionWindowMs,
        pageThresholdBytes, tailReads);
  }

  /**
   * This configuration, reading the log's tail ({@code true}: a read from a replica, and the commit
   * loop's reads after a lost race, an ambiguous write or a conflict, read only what follows the
   * replica's tail, {@link SharedLog#readAfter}) or reading it whole ({@code false}, as before
   * docs/TAIL_READS.md step 5: the commit loop probes after a lost append race and resends).
   */
  public RuntimeConfig withTailReads(boolean tail) {
    return new RuntimeConfig(maxAttempts, maxThrottleAttempts, throttleMinWaitMs, raceMinWaitMs,
        committedRetentionMs, compaction, visibilityWait, clockMs, admissionWindowMs,
        pageThresholdBytes, tail);
  }

  /** The defaults, with the given compaction policy, retention, and throttle and race backoffs. */
  public static RuntimeConfig of(
      CompactionPolicy compaction, long committedRetentionMs, long throttleMinWaitMs,
      long raceMinWaitMs) {
    return new RuntimeConfig(
        DEFAULT_MAX_ATTEMPTS,
        DEFAULT_MAX_THROTTLE_ATTEMPTS,
        throttleMinWaitMs,
        raceMinWaitMs,
        committedRetentionMs,
        compaction,
        Duration.ZERO,
        System::currentTimeMillis);
  }
}
