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

import java.util.UUID;

/**
 * A state machine's state as a read of its log reached it: the state, the committed set, the
 * tail the read ended at, and the number of intention records after the checkpoint. A writer
 * builds an intention from one, and commits it on top of it ({@link LogRuntime#commit}).
 */
public final class Replica<S> {
  private final S state;
  private final CommittedSet committed;
  private final LogTail tail;
  private final long intentionRecords;
  // The intention records and their bytes since the checkpoint or the last GC record after it
  private final long recordsSinceGc;
  private final long bytesSinceGc;
  private final UUID logId;
  // The size of the checkpoint the records follow (0 for a log not yet created)
  private final long checkpointBytes;
  // The whole view (a checkpoint and every record after it) this replica is the replay of, if a
  // runtime replayed it from one: a later read whose view continues it byte for byte replays only
  // the records after it (LogRuntime#replay).
  private final LogView view;
  // The ids of the view's intention records, as replay decoded them, if a runtime replayed it: what
  // a later fold looks a commit's record up in (LogRuntime#continued); null if unknown
  private final IntentionIds intentionIds;

  public Replica(
      S state, CommittedSet committed, LogTail tail, long intentionRecords, UUID logId) {
    this(state, committed, tail, intentionRecords, logId, 0L, null);
  }

  Replica(
      S state, CommittedSet committed, LogTail tail, long intentionRecords, UUID logId,
      long checkpointBytes, LogView view) {
    this(state, committed, tail, intentionRecords, intentionRecords, 0L, logId, checkpointBytes,
        view);
  }

  Replica(
      S state, CommittedSet committed, LogTail tail, long intentionRecords, long recordsSinceGc,
      long bytesSinceGc, UUID logId, long checkpointBytes, LogView view) {
    this(state, committed, tail, intentionRecords, recordsSinceGc, bytesSinceGc, logId,
        checkpointBytes, view, null);
  }

  Replica(
      S state, CommittedSet committed, LogTail tail, long intentionRecords, long recordsSinceGc,
      long bytesSinceGc, UUID logId, long checkpointBytes, LogView view,
      IntentionIds intentionIds) {
    this.state = state;
    this.committed = committed;
    this.tail = tail;
    this.intentionRecords = intentionRecords;
    this.recordsSinceGc = recordsSinceGc;
    this.bytesSinceGc = bytesSinceGc;
    this.logId = logId;
    this.checkpointBytes = checkpointBytes;
    this.view = view;
    this.intentionIds = intentionIds;
  }

  public S state() {
    return state;
  }

  /** The ids of the records replay applied, less those GC dropped. */
  public CommittedSet committed() {
    return committed;
  }

  /** The tail the read ended at: what the next conditional write names. */
  public LogTail tail() {
    return tail;
  }

  /** The intention records after the checkpoint: what the count knob compares. */
  public long intentionRecords() {
    return intentionRecords;
  }

  /**
   * The intention records after the checkpoint, or after the last GC record that follows it,
   * where a compaction began ({@link Replay.Result#recordsSinceGc}): what a log that keeps its
   * checkpoints apart compacts by, since its writers read only what follows their own tails.
   */
  public long recordsSinceGc() {
    return recordsSinceGc;
  }

  /** The bytes of the records {@link #recordsSinceGc} counts. */
  public long bytesSinceGc() {
    return bytesSinceGc;
  }

  /** The log's id, which its checkpoints carry. */
  public UUID logId() {
    return logId;
  }

  /**
   * The size of the checkpoint the intention records follow: with {@link LogTail#bytes}, what
   * the size knob compares (errata D19).
   */
  public long checkpointBytes() {
    return checkpointBytes;
  }

  /**
   * The whole view this is the replay of, or null (a fold of records alone onto another replica):
   * what a later read compares, and what oracles check the log held.
   */
  public LogView view() {
    return view;
  }

  /** The ids of {@link #view}'s intention records, in log order; null if unknown. */
  IntentionIds intentionIds() {
    return intentionIds;
  }
}
