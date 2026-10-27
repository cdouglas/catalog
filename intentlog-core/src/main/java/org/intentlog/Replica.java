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
  private final UUID logId;
  // The whole view (a checkpoint and every record after it) this replica is the replay of, if a
  // runtime replayed it from one: a later read whose view continues it byte for byte replays only
  // the records after it (LogRuntime#replay).
  private final LogView view;

  public Replica(
      S state, CommittedSet committed, LogTail tail, long intentionRecords, UUID logId) {
    this(state, committed, tail, intentionRecords, logId, null);
  }

  Replica(
      S state, CommittedSet committed, LogTail tail, long intentionRecords, UUID logId,
      LogView view) {
    this.state = state;
    this.committed = committed;
    this.tail = tail;
    this.intentionRecords = intentionRecords;
    this.logId = logId;
    this.view = view;
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

  /** The log's id, which its checkpoints carry. */
  public UUID logId() {
    return logId;
  }

  /** The whole view this is the replay of, or null. */
  LogView view() {
    return view;
  }
}
