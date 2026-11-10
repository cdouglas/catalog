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
import java.util.Map;
import org.intentlog.LogView;
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
    PAST_RECORD,
    /**
     * The read is served with records missing after its checkpoint, or after the tail it reads
     * from, as a log that trimmed past its newest checkpoint or lost records would serve it; the
     * view says where its records start, and the runtime must fail the read (S4). Providers that
     * cannot serve one (one object, whose checkpoint and records are one write) serve it as {@link
     * #NONE}.
     */
    GAP
  }

  /** A request of the log's page store ({@link SharedLog#pages}). */
  enum PageRequest {
    PUT,
    GET,
    DELETE,
    LIST
  }

  /** A fault a page request meets. A provider that cannot produce one acts as the nearest it can. */
  enum PageFault {
    NONE,
    /** The request fails without effect: a put or delete does not land, a get or list reads nothing. */
    FAIL,
    /** A put or delete lands, and its reply is lost; a get or list fails. */
    LOST_REPLY,
    /** A get finds no page, as if GC had dropped it and its compactor deleted it; else FAIL. */
    MISSING
  }

  /** What the fuzzer decides of each request the log serves, before it serves it. */
  interface Hooks {
    WriteFault beforeWrite();

    ReadFault beforeRead();

    /** A request of the page store, of the page {@code name} (null for a listing). */
    default PageFault beforePage(PageRequest request, String name) {
      return PageFault.NONE;
    }
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

  /**
   * Every record that has landed (intentions, GC records, PageOuts), in order, including those a
   * trim or replace hid; each once.
   */
  List<byte[]> landedRecords();

  /**
   * Appends {@code record} at the tail, unfaulted: a copy of a record, as a writer's stray retry
   * would, or a record the fuzzer writes as a compactor would (a PageOut, a GC record).
   */
  void appendCopy(byte[] record);

  /** The pages the log's page store holds now, by name, read without faults; empty if none. */
  Map<String, byte[]> storedPages();

  /** Lands the appends delayed so far; returns how many. */
  int deliverDelayed();

  /**
   * Checkpoints the log holds that are not the replay of the log through their position (I11), or
   * readers that would disagree; empty if none.
   */
  List<String> checkpointViolations();

  /**
   * Whether {@code view}, one a replica was replayed from, is a state the log held: a checkpoint
   * the log stored and the records after its position, none missing and none another's (P1; the
   * oracle that no replica holds a state the log never held, docs/TAIL_READS.md "Testing"). Read
   * without faults. True by default, for a harness that cannot tell.
   */
  default boolean held(LogView view) {
    return true;
  }
}
