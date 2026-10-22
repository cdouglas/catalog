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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.intentlog.wire.LogEnvelope;
import org.intentlog.wire.UuidV7;

/**
 * Replicates a state machine through a {@link SharedLog}: reads it (a checkpoint and the records
 * after it, replayed), and commits intentions to it.
 *
 * <p>A commit reports one of three outcomes, each of which a caller can rely on: it returns (the
 * intention took effect, once), it throws {@link CommitRejectedException} (it did not and will
 * not), or {@link CommitOutcomeUnknownException} (either). A failure to read the log while none
 * of the commit's writes may have landed is rethrown as it is. A commit is reported rejected only
 * before any write, or after a read of the log that shows its record rejected or absent with no
 * write of it still able to land (I10).
 */
public final class LogRuntime<S, B extends StateMachine.Builder<S>, I extends Intention<B>> {

  private final SharedLog log;
  private final StateMachine<S, B, I> machine;
  private final RuntimeConfig config;

  public LogRuntime(SharedLog log, StateMachine<S, B, I> machine, RuntimeConfig config) {
    this.log = log;
    this.machine = machine;
    this.config = config;
  }

  /** A replica of the log as a fresh read finds it. A read failure is thrown as it is. */
  public Replica<S> read() {
    return replay(log.read(), null).replica();
  }

  /**
   * Commits {@code intention}, built from {@code base}, and returns the replica of the log its
   * record landed in.
   *
   * @throws CommitRejectedException if it did not take effect, and never will
   * @throws CommitOutcomeUnknownException if it may or may not have
   */
  public Replica<S> commit(Replica<S> base, I intention) {
    return new Commit(base, intention).run();
  }

  // ============================================================
  // Replay
  // ============================================================

  /** A replay of a view: the builder before it is built, so a commit can verify on it. */
  private final class Replayed {
    private final LogView view;
    private final Replay.Result<B> result;
    private final UUID logId;
    private Replica<S> replica;

    Replayed(LogView view, Replay.Result<B> result, UUID logId) {
      this.view = view;
      this.result = result;
      this.logId = logId;
    }

    B builder() {
      return result.builder();
    }

    Replica<S> replica() {
      if (replica == null) {
        replica =
            new Replica<>(
                result.builder().build(),
                result.committed().build(),
                view.tail(),
                result.intentionRecords(),
                result.logId() != null ? result.logId() : logId);
      }
      return replica;
    }
  }

  private Replayed replay(LogView view, UUID watch) {
    return replay(view, watch, null);
  }

  private Replayed replay(LogView view, UUID watch, UUID logId) {
    return new Replayed(
        view, Replay.replay(machine, view.checkpoint().orElse(null), view.records(), watch), logId);
  }

  // ============================================================
  // The commit loop
  // ============================================================

  /** A write whose outcome is unknown: its reply was lost, or reading back what it wrote failed. */
  private static final class AmbiguousWrite extends RuntimeException {
    AmbiguousWrite(Throwable cause) {
      super(cause);
    }
  }

  /** A read of the log that failed; {@link #failure} is what it threw. */
  private static final class UnreadableLog extends RuntimeException {
    UnreadableLog(RuntimeException cause) {
      super(cause);
    }

    RuntimeException failure() {
      return (RuntimeException) getCause();
    }
  }

  private final class Commit {
    private final Replica<S> base;
    private final I intention;
    private final UUID id;
    private final byte[] record;
    private final Capabilities caps = log.capabilities();

    // The replica of `tail`, unless stale: after a lost append race, the tail is refreshed
    // without reading the log.
    private Replica<S> current;
    private LogTail tail;
    private boolean stale = false;
    // The last write whose outcome is unknown, if any: its reply was lost, or reading back what
    // it wrote failed. It may still be in flight, so once one has happened, running out of
    // attempts no longer means the commit failed.
    private AmbiguousWrite ambiguous = null;
    // The last write that failed its condition, until a read of the log rules out that it landed.
    // A transport retry of a write that landed fails its condition, so the failure is no proof;
    // but the write landed before the reply, so a read without it settles it.
    private RuntimeException unresolved = null;
    // Whether storage refused an append because the log holds as many writes as it allows (S3
    // Express: 10,000 parts; an Azure Append Blob: 50,000 blocks): only a replace can commit
    // then, whatever the policy says.
    private boolean full = false;

    Commit(Replica<S> base, I intention) {
      this.base = base;
      this.intention = intention;
      // After the horizon of the log this writer read, where replay rejects a record (Bug 64).
      this.id = UuidV7.newUuidV7After(base.committed().horizon());
      this.record = LogEnvelope.encodeRecord(id, machine.encodeIntention(intention));
      this.current = base;
      this.tail = base.tail();
    }

    Replica<S> run() {
      try {
        return loop();
      } catch (LogNotSentException e) {
        // A write failed before anything was sent. The commit failed, unless an earlier write of
        // it may have landed.
        Throwable earlier = earlier();
        if (earlier != null) {
          earlier.addSuppressed(e);
          throw new CommitOutcomeUnknownException(earlier);
        }
        throw new CommitRejectedException(
            CommitRejectedException.Reason.NOT_SENT, null,
            "Cannot commit: a write failed before it was sent: " + e.getMessage(), e);
      }
    }

    private Replica<S> loop() {
      // Case 0: the log does not exist yet. Only a replace creates it.
      if (!tail.exists()) {
        try {
          return validate(write(() -> log.replace(tail, checkpoint(base), records(base))));
        } catch (AmbiguousWrite | LogConflictException e) {
          // The creation may have landed: its reply was lost, reading it back failed, or it failed
          // its condition, as a transport retry of a creation that landed does.
          Throwable failure = e instanceof AmbiguousWrite ? e.getCause() : e;
          Replayed fresh = resolve(failure);
          if (fresh != null && landed(fresh, failure)) {
            return fresh.replica(); // the creation landed
          }
          if (fresh != null && !(e instanceof AmbiguousWrite)) {
            throw new CommitRejectedException(
                CommitRejectedException.Reason.CREATE_LOST, fresh.replica(),
                "Cannot commit: another writer created the log first", e);
          }
          throw new CommitOutcomeUnknownException(failure);
        }
      }

      for (int attempt = 0; attempt < config.maxAttempts(); attempt++) {
        try {
          // Replace (compact) or append, by what the log can do and what the policy says of its
          // observable state. A log without append replaces on every commit.
          boolean mustReplace =
              caps.fusedReplace()
                  && (full
                      || !caps.append()
                      || config.compaction().due(current.intentionRecords(), bytes(tail),
                          record.length));

          if (mustReplace) {
            if (stale) {
              // The replace supersedes the log at its tail, so its checkpoint must hold the state
              // there: a checkpoint of an older read would erase every record committed since.
              // Our record follows the checkpoint and replay verifies it against that state, as it
              // would an append. Read afresh: storage fails a read pinned to a version another
              // writer has since replaced.
              Replayed fresh = read();
              if (landed(fresh, earlier())) {
                return fresh.replica(); // an append reported as a lost race landed
              }
              unresolved = null;
              adopt(fresh);
            }
            full = false;
            try {
              Replica<S> on = current;
              LogTail at = tail;
              return validate(write(() -> log.replace(at, checkpoint(on), records(on))));
            } catch (LogConflictException e) {
              unresolved = e;
            }
            // Replace conflict: another writer changed the log since we read it. Rebase onto the
            // new state. Replay checks this intention's preconditions against the state its
            // record lands on, so if they hold there, retry on top of it -- by append or replace,
            // as that state calls for -- and the commit is accepted exactly when a serial commit
            // would be. If not, fail without writing; the client refreshes and re-applies.
            Replayed fresh = read();
            if (landed(fresh, earlier())) {
              return fresh.replica(); // an earlier attempt landed
            }
            unresolved = null;
            boolean holds = intention.verify(fresh.builder());
            adopt(fresh);
            if (!holds) {
              throw conflict(fresh, "Cannot commit: the log changed concurrently");
            }
          } else {
            try {
              LogTail at = tail;
              return validate(write(() -> log.append(record, caps.conditionalAppend() ? at : null)));
            } catch (LogFullException e) {
              // Nothing landed, and no append to this log can: replace it, on a fresh read.
              // Without this the commit took the append for ambiguous, and with a count above the
              // store's limit no writer ever compacted (Bug 66).
              full = true;
              stale = true;
              continue;
            } catch (LogConflictException e) {
              unresolved = e;
            }
            // Another writer appended first. The record is the same and replay applies it once
            // (by its id), so append it again at the new tail without reading the log: only the
            // tail is needed.
            LogTail probed = probe();
            if (!probed.continues(tail)) {
              // The log was replaced (compacted). Read it, so that the next decision sees its
              // record count; afresh, as above, since the version may be replaced by now.
              Replayed fresh = read();
              if (landed(fresh, earlier())) {
                return fresh.replica(); // an append reported as a lost race landed
              }
              unresolved = null;
              adopt(fresh);
            } else {
              // The log grew (a concurrent append). Resend the same record at the new tail; the
              // record count is now a lower bound, which is fine for a soft limit. No read is
              // needed unless the next attempt replaces.
              tail = probed;
              stale = true;
            }
          }
        } catch (AmbiguousWrite e) {
          // The write may have landed: its reply was lost, or reading back what it wrote failed.
          // Its record carries this intention's id, so a fresh read says which. If it has not
          // landed, retry from the fresh state: a write still in flight carries the same record
          // (and, if conditional, a condition on the tail it saw), so replay applies the intention
          // once. A rejection stays a rejection: replay verifies a record against the state it
          // lands on, and once it fails there it fails at every later point (S2), so a late
          // landing of an intention that does not hold changes nothing.
          ambiguous = e;
          Replayed fresh = resolve(e.getCause());
          if (fresh == null) {
            throw new CommitOutcomeUnknownException(e.getCause());
          }
          if (landed(fresh, e.getCause())) {
            return fresh.replica(); // the write landed
          }
          unresolved = null;
          boolean holds = intention.verify(fresh.builder());
          adopt(fresh);
          if (!holds) {
            throw conflict(fresh, "Cannot commit: the log changed concurrently");
          }
        } catch (UnreadableLog e) {
          // A read failed. The commit failed too, unless one of its writes may have landed.
          Throwable write = earlier();
          if (write == null) {
            throw e.failure();
          }
          write.addSuppressed(e.failure());
          throw new CommitOutcomeUnknownException(write);
        }
      }

      if (ambiguous != null) {
        throw new CommitOutcomeUnknownException(ambiguous.getCause());
      }
      if (unresolved != null) {
        // The last attempt lost an append race, which its own landing would also report.
        Replayed fresh = resolve(unresolved);
        if (fresh == null) {
          throw new CommitOutcomeUnknownException(unresolved);
        }
        if (landed(fresh, unresolved)) {
          return fresh.replica();
        }
      }
      throw new CommitRejectedException(
          CommitRejectedException.Reason.EXHAUSTED, null, "Exceeded maximum commit attempts", null);
    }

    private void adopt(Replayed fresh) {
      current = fresh.replica();
      tail = current.tail();
      stale = false;
    }

    /** The last write of this commit that may have landed, or null. */
    private Throwable earlier() {
      return ambiguous != null ? ambiguous.getCause() : unresolved;
    }

    /** The checkpoint a replace of the log at {@code on} writes: exactly {@code on}'s state. */
    private byte[] checkpoint(Replica<S> on) {
      return LogEnvelope.encodeCheckpoint(
          on.logId(), on.committed().bytes(), machine.encodeCheckpoint(on.state()));
    }

    /**
     * The records a replace writes after its checkpoint: a GC record, then this commit's. GC of
     * the committed set is a record, never a filter on the checkpoint, so every checkpoint is a
     * replay of the log before it (I11). The watermark is now less the retention, clamped to 0
     * (a retention of {@code Long.MAX_VALUE} disables GC, and no record is written), and stays
     * below this commit's own time: the horizon must not reach the record that follows, or replay
     * rejects it (Bug 64). A short retention would otherwise drop the id of a commit this one
     * rebased on, minted after it.
     */
    private List<byte[]> records(Replica<S> on) {
      long now = config.clockMs().getAsLong();
      long retention = config.committedRetentionMs();
      long watermark = retention >= now ? 0L : now - retention;
      watermark = Math.max(0L, Math.min(watermark, UuidV7.timestampMs(id) - 1));
      List<byte[]> records = new ArrayList<>(2);
      if (watermark > 0L) {
        records.add(LogEnvelope.encodeAdvanceHorizon(watermark));
      }
      records.add(record);
      return records;
    }

    /**
     * Sends a write and reads back what it wrote, again after a backoff while storage applies
     * backpressure ({@link LogThrottledException}). Each retry carries the same condition, so at
     * most one attempt lands. A throttled write may have landed even so: a 5xx can follow a write
     * the service applied, and an SDK's own retries report only the last attempt. A retry of a
     * write that landed fails its condition, which the commit loop resolves by reading the log;
     * running out of retries leaves the outcome unknown in the same way, so it is an {@link
     * AmbiguousWrite}. An interrupt during the backoff stops the commit with its outcome unknown.
     * The protocol's conflict and full signals propagate; any other failure of the write, or of
     * reading back what it wrote, is an {@link AmbiguousWrite}.
     */
    private Replayed write(Supplier<Receipt> send) {
      long backoffMs = config.throttleMinWaitMs();
      LogThrottledException lastThrottle = null;
      for (int attempt = 0; attempt < config.maxThrottleAttempts(); attempt++) {
        Receipt receipt;
        try {
          receipt = send.get();
        } catch (LogThrottledException e) {
          lastThrottle = e;
          long sleepMs = backoffMs + ThreadLocalRandom.current().nextLong(backoffMs / 2 + 1);
          try {
            Thread.sleep(sleepMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            e.addSuppressed(ie);
            throw new CommitOutcomeUnknownException(e);
          }
          backoffMs = Math.min(backoffMs * 2, Math.max(5_000L, config.throttleMinWaitMs()));
          continue;
        } catch (LogNotSentException e) {
          // This attempt failed before writing; an earlier, throttled one may have landed.
          if (lastThrottle == null) {
            throw e;
          }
          e.addSuppressed(lastThrottle);
          throw new AmbiguousWrite(e);
        } catch (LogConflictException | LogFullException e) {
          throw e;
        } catch (RuntimeException e) {
          throw new AmbiguousWrite(e);
        }
        try {
          return replay(log.readThrough(receipt, config.visibilityWait()), id, base.logId());
        } catch (RuntimeException e) {
          throw new AmbiguousWrite(e);
        }
      }
      throw new AmbiguousWrite(
          new IOException(
              String.format("Storage throttled %d attempts", config.maxThrottleAttempts()),
              lastThrottle));
    }

    /** A fresh read; any failure becomes {@link UnreadableLog}. */
    private Replayed read() {
      try {
        return replay(log.read(), id, base.logId());
      } catch (RuntimeException e) {
        throw new UnreadableLog(e);
      }
    }

    /** The log's tail; a failure to probe it becomes {@link UnreadableLog}. */
    private LogTail probe() {
      try {
        return log.probe();
      } catch (RuntimeException e) {
        throw new UnreadableLog(e);
      }
    }

    /** The size of the log at {@code at}; a failure to learn it becomes {@link UnreadableLog}. */
    private long bytes(LogTail at) {
      try {
        return at.bytes();
      } catch (RuntimeException e) {
        throw new UnreadableLog(e);
      }
    }

    /**
     * Reads the log after a write that may have landed, or returns null if that fails too (the
     * read failure is attached to {@code writeFailure}).
     */
    private Replayed resolve(Throwable writeFailure) {
      try {
        return read();
      } catch (UnreadableLog readFailure) {
        writeFailure.addSuppressed(readFailure.failure());
        return null;
      }
    }

    /**
     * Whether {@code fresh} holds this commit's record. Replay's judgment of the record where it
     * landed comes first: on a log with checkpoints apart from its records, another writer's GC
     * record can land between this record and the read, and drop the id of a record that applied.
     * Else the committed set: if the id is at or below its horizon, the set cannot say, and replay
     * rejects the record from now on (Bug 64): the commit failed, unless {@code earlier}, a write
     * of it that may have landed, did, before its id aged out.
     */
    private boolean landed(Replayed fresh, Throwable earlier) {
      switch (fresh.result.outcome()) {
        case APPLIED:
          return true;
        case REJECTED:
          return false;
        default:
          break;
      }
      try {
        return fresh.result.committed().contains(id);
      } catch (DedupHorizonExceededException e) {
        if (earlier != null) {
          earlier.addSuppressed(e);
          throw new CommitOutcomeUnknownException(earlier);
        }
        throw new CommitRejectedException(
            CommitRejectedException.Reason.BEYOND_HORIZON, null,
            String.format(
                "Cannot commit: transaction %s is at or below the log's committed-set horizon",
                id),
            e);
      }
    }

    /** The replica of {@code result}, if it holds this commit's record; else a conflict. */
    private Replica<S> validate(Replayed result) {
      if (!landed(result, earlier())) {
        // The record landed, but replay rejected it: a concurrent commit changed what it depends on.
        throw conflict(result, "Cannot commit: a concurrent commit invalidated this one");
      }
      return result.replica();
    }

    private CommitRejectedException conflict(Replayed on, String message) {
      return new CommitRejectedException(
          CommitRejectedException.Reason.CONFLICT, on.replica(), message, null);
    }
  }
}
