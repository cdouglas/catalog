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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;

/**
 * What the runtime did on a thread, event by event, for a benchmark to read per operation (LOG.md
 * "Tracing"). Off unless the thread opens a trace; then each event the runtime reports on it is
 * kept, with the time ({@link System#nanoTime}) and the thread's CPU time it came at, until the
 * thread drains them.
 */
public final class CommitTrace {
  /** An event: when, the thread's CPU time then, what, and two numbers that say more. */
  public record Event(long nanos, long cpuNanos, Kind kind, long a, long b) {}

  /** What happened; {@code a} and {@code b} as each says. */
  public enum Kind {
    /** A commit began: a = the records of the replica it stages on, b = its tail's bytes. */
    COMMIT,
    /** An attempt: a = its number from 0, b = 1 if it replaces, 0 if it appends. */
    ATTEMPT,
    /** A replace's payload is ready (its checkpoint encoded): a = the checkpoint's bytes. */
    PREPARED,
    /** A write is sent: a = 1 for a replace, 0 for an append. */
    SEND,
    /** The write landed and was read through. */
    LANDED,
    /** The write failed its condition: another writer's landed first. */
    LOST,
    /** The write's outcome is unknown (its reply lost, or reading it back failed). */
    AMBIGUOUS,
    /** A wait after a lost race: a = its length in ms. */
    BACKOFF,
    /**
     * A read began: a = 1 if it reads what follows a replica, 0 if whole; b = 1 if a commit's,
     * 0 if a client's (a load or refresh).
     */
    READ,
    /**
     * A read's view arrived: a = 1 if it holds a checkpoint, 0 if records alone; b = its records.
     * On one object a tail read's view holds the replica's checkpoint and records and the frames
     * after them, so only {@link #REPLAYED} says whether it was folded.
     */
    VIEW,
    /**
     * A view was replayed, its state not yet built: a = 1 if its records after a replica's were
     * folded onto it, 0 if it was replayed from its checkpoint; b = the records replayed.
     */
    REPLAYED,
    /** A client's read is built into a replica (its state built). */
    BUILT,
    /** The commit took a fresh read as the replica it continues from (its state built). */
    ADOPTED,
    /** A probe of the log's tail began. */
    PROBE,
    /** The probe answered: a = 1 if the log continues the tail the commit held. */
    PROBED,
    /**
     * The commit ended: a = 0 landed, 1 rejected, 2 unknown, 3 out of attempts, 4 failed otherwise
     * (a read failed); b = the races it lost.
     */
    END
  }

  private static final ThreadLocal<CommitTrace> CURRENT = new ThreadLocal<>();
  private static final ThreadMXBean CPU = ManagementFactory.getThreadMXBean();

  private final List<Event> events = new ArrayList<>();

  private CommitTrace() {}

  /** Starts keeping the calling thread's events, if it is not already. */
  public static void open() {
    if (CURRENT.get() == null) {
      CURRENT.set(new CommitTrace());
    }
  }

  /** Stops keeping the calling thread's events, and drops those kept. */
  public static void close() {
    CURRENT.remove();
  }

  /** The calling thread's events since it opened its trace or last drained it; none if closed. */
  public static List<Event> drain() {
    CommitTrace trace = CURRENT.get();
    if (trace == null || trace.events.isEmpty()) {
      return List.of();
    }
    List<Event> drained = List.copyOf(trace.events);
    trace.events.clear();
    return drained;
  }

  static void event(Kind kind, long a, long b) {
    CommitTrace trace = CURRENT.get();
    if (trace != null) {
      trace.events.add(new Event(System.nanoTime(), CPU.getCurrentThreadCpuTime(), kind, a, b));
    }
  }

  static void event(Kind kind) {
    event(kind, 0, 0);
  }
}
