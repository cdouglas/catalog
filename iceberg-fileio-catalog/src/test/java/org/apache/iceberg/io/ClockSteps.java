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
package org.apache.iceberg.io;

import java.util.Set;

/**
 * Watches the wall clock for steps: a time sync that moves it back or forward at once. Iceberg
 * stamps snapshots and metadata versions with the wall clock, and derives some metadata-table
 * columns from the order of those stamps ({@link #CLOCK_ORDERED}). Two runs of one schedule agree
 * on those columns only if the clock did not step back during either: a stamp taken after the step
 * can sort before one taken before it. Some machines step often. On WSL 2, where the host's time
 * sync and an NTP client in the guest take turns, the clock steps back about 1.9 s and forward
 * about 5.2 s every 30 s.
 */
final class ClockSteps {

  /**
   * The metadata-table columns Iceberg derives from the order of timestamps: METADATA_LOG_ENTRIES'
   * latest snapshot as of each metadata version's time (and its schema and sequence number), and
   * PARTITIONS' snapshot with the latest timestamp.
   */
  static final Set<String> CLOCK_ORDERED = Set.of(
      "latest_snapshot_id", "latest_schema_id", "latest_sequence_number",
      "last_updated_snapshot_id");

  // A step is a jump of the wall clock against System.nanoTime of more than this, plus a quarter
  // of the time since the last look: nanoTime may itself run at the wrong rate (10% slow on one
  // WSL 2 VM), so look often, around every write.
  private static final long STEP_MS = 100;

  private long wallMs = System.currentTimeMillis();
  private long nanos = System.nanoTime();
  private boolean stepped = false;

  /** Whether the wall clock has stepped since this watch began. Looks at it first. */
  boolean stepped() {
    long nowWallMs = System.currentTimeMillis();
    long nowNanos = System.nanoTime();
    long elapsedMs = (nowNanos - nanos) / 1_000_000;
    if (Math.abs((nowWallMs - wallMs) - elapsedMs) > STEP_MS + elapsedMs / 4) {
      stepped = true;
    }
    wallMs = nowWallMs;
    nanos = nowNanos;
    return stepped;
  }
}
