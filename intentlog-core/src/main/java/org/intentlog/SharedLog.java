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
import java.util.List;

/**
 * A totally ordered log of records, opened by a checkpoint: the storage a {@link LogRuntime}
 * replicates a state machine through. Records and checkpoints are opaque bytes (log.proto's
 * envelopes); the provider stores and orders them, and says what it can do ({@link
 * Capabilities}).
 *
 * <p>Obligations:
 *
 * <ul>
 *   <li>P1: one total order of records, and every read returns a checkpoint and a prefix of the
 *       records after it.
 *   <li>P2: an acknowledged write (append, replace, checkpoint) is durable.
 *   <li>P3: {@link #readThrough} returns a view that contains the receipt's record.
 *   <li>P4: of several conditional writes on the same tail, at most one lands.
 *   <li>P5: no trim before a durable checkpoint covers the records trimmed.
 *   <li>P6: {@link LogFullException} and {@link LogNotSentException} mean nothing landed.
 * </ul>
 *
 * <p>Failures: a write throws {@link LogConflictException} when its condition does not hold (which
 * is no proof that it did not land: a transport retry of a write that landed fails its
 * condition), {@link LogThrottledException} under backpressure (it may have landed; the same
 * write may be sent again), and {@link LogFullException} or {@link LogNotSentException} when
 * nothing landed. Any other exception from a write leaves its outcome unknown; any exception from
 * a read or probe means the log could not be read.
 */
public interface SharedLog {

  Capabilities capabilities();

  /** The latest checkpoint and every record after it, at a fresh read. */
  LogView read();

  /**
   * A view that contains the record {@code receipt} acknowledged (P3): for a log whose reads lag
   * its appends, waiting up to {@code wait} for it. Any exception leaves the write's outcome
   * unknown.
   */
  LogView readThrough(Receipt receipt, Duration wait);

  /**
   * The tail of the log, without its records: the precondition for a conditional write. If the
   * log does not exist yet, a tail that says so ({@link LogTail#exists} is false), which a {@link
   * #replace} that creates the log names.
   */
  LogTail probe();

  /**
   * Appends {@code record}. With {@code expected} (which needs {@link
   * Capabilities#conditionalAppend}), only while the log's tail is {@code expected}.
   */
  Receipt append(byte[] record, LogTail expected);

  /**
   * Replaces the log at {@code expected} with {@code checkpoint} and {@code records} after it, in
   * one write, if its tail is still {@code expected}; creates the log if {@code expected} does not
   * {@link LogTail#exists}. Needs {@link Capabilities#fusedReplace}.
   */
  default Receipt replace(LogTail expected, byte[] checkpoint, List<byte[]> records) {
    throw new UnsupportedOperationException("This log cannot replace itself");
  }

  /**
   * Stores {@code checkpoint}, the state of the log through {@code through}, apart from the
   * records. Idempotent: every checkpoint at one position is the same (I11). Needs {@link
   * Capabilities#separateCheckpoints}.
   */
  default void putCheckpoint(LogTail through, byte[] checkpoint) {
    throw new UnsupportedOperationException("This log stores no separate checkpoints");
  }

  /** Drops the records a durable checkpoint at or after {@code through} covers (P5). */
  default void trim(LogTail through) {}

  /**
   * The records after the view {@code since}, as a view to fold onto it; may throw {@link
   * TrimmedException} if the log dropped them. By default, a fresh {@link #read}.
   */
  default LogView readAfter(LogView since) {
    return read();
  }
}
