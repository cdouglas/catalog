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

/**
 * The end of a log as a read or probe saw it: the precondition a conditional write names. Opaque
 * to the runtime but for what it asks here.
 */
public interface LogTail {

  /**
   * Whether the log existed at this tail, as the read, probe or write that made it found it (a log
   * that does not exist is created by a {@link SharedLog#replace}).
   */
  boolean exists();

  /** The size of the log in bytes, checkpoint included: what the size knob compares. */
  long bytes();

  /**
   * Whether this tail, a later one, continues {@code earlier}: records were appended after it. If
   * not, the log was replaced (compacted) since.
   *
   * <p>A log that replaces itself may also answer true for a replace that left the log no shorter,
   * where it cannot tell one from appends: it then means "did not shrink". {@code AtomicFileLog}
   * does so for a tail a probe found, whose version and length it knows but not its bytes, and
   * compares it with an earlier tail by length alone. Where it read or wrote the bytes at both
   * tails, the last frame's trailer names the object's incarnation, which every replace draws
   * anew, and tells a replace from appends. The runtime asks this of a log that replaces itself
   * only after a lost append race with tail reads off, where the looser reading is safe: it
   * appends the same record again at the probed tail, framed from the probed version's last
   * trailer, and replay judges the record where it lands and applies it at most once (a copy is
   * skipped by its id in the committed set, or rejected at or below the floor), even after a
   * checkpoint its writer never read. A log that cannot
   * replace itself must answer exactly: a commit's floor, its settling of a lost append and {@code
   * LogRuntime.newer} rest on it.
   */
  boolean continues(LogTail earlier);

  /**
   * The log's position at this tail, as the provider encodes it, or null if the provider cannot
   * say (a tail a probe found on a log whose positions need the log's bytes). Opaque to the
   * runtime, which compares positions only for equality: a checkpoint names the one it covers
   * ({@code LogCheckpoint.through}), and a view of records alone the one they follow ({@link
   * LogView#start}). Two tails of one log at one position have equal positions, and two at
   * different positions do not (docs/TAIL_READS.md "Positions").
   */
  default byte[] position() {
    return null;
  }
}
