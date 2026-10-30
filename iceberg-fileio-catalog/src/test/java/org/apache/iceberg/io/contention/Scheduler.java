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
package org.apache.iceberg.io.contention;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.TreeMap;

/**
 * Turns among writers that run as threads, one at a time, in an order a seed fixes: a writer runs
 * until it reaches a step (a read or an append of the log, or a job's work), waits there, and the
 * scheduler picks the next writer among those whose step is due. A step is due at once, unless
 * the writer said it comes after some number of the scheduler's steps (a job's runtime) or after
 * some number of entries landed (maintenance). So the log's contents depend on the seed alone,
 * not on the host or a clock: logical time is the scheduler's steps.
 */
final class Scheduler {

  /** Thrown to a waiting writer when another failed: the run is over. */
  static final class Stopped extends RuntimeException {
    Stopped(Throwable cause) {
      super("another writer failed", cause);
    }
  }

  /** What a writer waits to do. */
  enum Step {
    READ,
    APPEND,
    WORK
  }

  private static final class Waiting {
    final Step step;
    final long dueStep;
    final long dueEntries;

    Waiting(Step step, long dueStep, long dueEntries) {
      this.step = step;
      this.dueStep = dueStep;
      this.dueEntries = dueEntries;
    }
  }

  private final SplittableRandom random;
  private final TreeMap<Integer, Waiting> waiting = new TreeMap<>();
  private final java.util.Set<Integer> live = new java.util.TreeSet<>();
  private int running = -1;
  private long steps = 0;
  private long entries = 0;
  private Throwable failure = null;

  Scheduler(long seed) {
    this.random = new SplittableRandom(seed);
  }

  /** Registers writer {@code id}, before its thread starts. */
  synchronized void join(int id) {
    live.add(id);
  }

  /** Writer {@code id} is done: it takes no more turns. */
  synchronized void leave(int id) {
    live.remove(id);
    waiting.remove(id);
    if (running == id) {
      running = -1;
    }
    notifyAll();
  }

  /** Writer {@code id} failed with {@code e}: the run stops. */
  synchronized void fail(Throwable e) {
    if (failure == null) {
      failure = e;
    }
    notifyAll();
  }

  /** The scheduler's steps so far: its logical time. */
  synchronized long steps() {
    return steps;
  }

  /** The entries landed so far. */
  synchronized long entries() {
    return entries;
  }

  /** An entry landed (called by the running writer). */
  synchronized void landed() {
    entries++;
  }

  /** Writer {@code id} waits for its turn to take {@code step}, due now. */
  void await(int id, Step step) {
    await(id, step, 0, 0);
  }

  /**
   * Writer {@code id} waits for its turn to take {@code step}, due once {@code afterSteps} more of
   * the scheduler's steps have passed and {@code atEntries} entries have landed.
   */
  synchronized void await(int id, Step step, long afterSteps, long atEntries) {
    waiting.put(id, new Waiting(step, steps + afterSteps, atEntries));
    if (running == id) {
      running = -1;
    }
    notifyAll();
    while (running != id) {
      if (failure != null) {
        throw new Stopped(failure);
      }
      try {
        wait();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new Stopped(e);
      }
    }
    waiting.remove(id);
  }

  /**
   * Runs turns until every writer has left: picks a writer whose step is due, uniformly by the
   * seed, and waits until it reaches its next step or leaves. Where none is due, logical time
   * jumps to the next due step; where none ever will be (the writers left wait for entries no one
   * will land), those writers are let go, to find the run over. Throws a writer's failure.
   */
  synchronized void run() {
    while (true) {
      while (running != -1 || (waiting.size() < live.size() && failure == null)) {
        try {
          wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }
      if (failure != null) {
        throw new IllegalStateException("a writer failed", failure);
      }
      if (live.isEmpty()) {
        return;
      }
      List<Integer> due = new ArrayList<>();
      long nextStep = Long.MAX_VALUE;
      for (var entry : waiting.entrySet()) {
        Waiting w = entry.getValue();
        if (w.dueEntries > entries) {
          continue;
        }
        if (w.dueStep <= steps) {
          due.add(entry.getKey());
        } else {
          nextStep = Math.min(nextStep, w.dueStep);
        }
      }
      if (due.isEmpty() && nextStep != Long.MAX_VALUE) {
        steps = nextStep;
        continue;
      }
      if (due.isEmpty()) {
        due.addAll(waiting.keySet());
      }
      running = due.get(random.nextInt(due.size()));
      steps++;
      notifyAll();
    }
  }
}
