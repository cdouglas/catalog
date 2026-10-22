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
package org.intentlog.testkit;

import java.util.List;
import org.intentlog.SharedLog;

/**
 * A {@link SharedLog} provider dressed for fuzzing: requests a fuzzer can fault and preempt, copies
 * of records it can append, and what an oracle needs to see that no trim or replace hides. Each
 * provider's tests implement one; {@link CommitProtocol} runs schedules on any of them.
 */
public interface LogHarness {

  /** A fault a write meets. A provider that cannot produce one acts as the nearest it can. */
  enum WriteFault {
    NONE,
    /** The request fails without landing. */
    FAIL_BEFORE,
    /** Nothing is sent ({@code LogNotSentException}). */
    NOT_SENT,
    /** The write lands; its reply is lost. */
    LOST_REPLY,
    /** The log throttles the write; nothing lands. */
    THROTTLED,
    /** The write lands; the log reports a throttle. */
    LANDED_THROTTLED,
    /** An append times out in flight and lands later ({@link #deliverDelayed}). */
    DELAYED,
    /** A conditional write lands; a transport retry of it fails its condition. */
    LANDED_AS_CONFLICT
  }

  /** A fault a read meets. */
  enum ReadFault {
    NONE,
    /** The read fails. */
    FAIL,
    /** The read is served stale: from an older checkpoint, lagging the tail, where it can be. */
    STALE,
    /**
     * A read through a write starts from a checkpoint that may hold its record already: a provider
     * that breaks P3, which the runtime must survive. Other reads, and providers that cannot, serve
     * it as {@link #NONE}.
     */
    PAST_RECORD
  }

  /** What the fuzzer decides of each request the log serves, before it serves it. */
  interface Hooks {
    WriteFault beforeWrite();

    ReadFault beforeRead();
  }

  /** The log the runtime commits to. */
  SharedLog log();

  /** Routes every request of {@link #log} through {@code hooks}. */
  void hook(Hooks hooks);

  /**
   * How many writes that change what a read finds have landed (records appended, the log
   * replaced): a commit that sees this change since its snapshot was raced.
   */
  long landedWrites();

  /** Every intention record that has landed, in order, including those a trim or replace hid. */
  List<byte[]> landedRecords();

  /** Appends a copy of {@code record} at the tail, as a writer's stray retry would, unfaulted. */
  void appendCopy(byte[] record);

  /** Lands the appends delayed so far; returns how many. */
  int deliverDelayed();

  /**
   * Checkpoints the log holds that are not the replay of the log through their position (I11), or
   * readers that would disagree; empty if none.
   */
  List<String> checkpointViolations();
}
