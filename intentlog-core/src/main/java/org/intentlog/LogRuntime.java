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
   * A fresh read, or {@code floor} if the read lags behind it: a replica this reader read or
   * committed in, so a read of the log too (P1 lets a read return any prefix). A reader that
   * passes the newest replica it has seen never sees the log go back, past its own commit
   * included (Bug 72). A null floor is no floor.
   */
  public Replica<S> read(Replica<S> floor) {
    return newer(floor, read());
  }

  /**
   * {@code later}, unless it lags behind {@code earlier} (a replica read or committed in before
   * it was read): then {@code earlier}. Only a log that cannot replace itself has reads that lag,
   * as the commit loop's floor assumes; on one that replaces itself, a tail that does not continue
   * an earlier one is a replace since, which is newer.
   */
  public Replica<S> newer(Replica<S> earlier, Replica<S> later) {
    if (earlier == null || log.capabilities().fusedReplace()
        || later.tail().continues(earlier.tail())) {
      return later;
    }
    return earlier;
  }

  /**
   * {@code prior} brought up to date: the records appended since folded onto it, if the log reads
   * them apart ({@link SharedLog#readAfter}); else, or if it trimmed them, a fresh read. Either
   * way the same replica, since replay is deterministic and every checkpoint is the replay of the
   * log through it (I11). Never one behind {@code prior}, which a fresh read on a log whose reads
   * lag could be.
   */
  public Replica<S> refresh(Replica<S> prior) {
    LogView view;
    try {
      view = log.readAfter(prior.tail());
    } catch (TrimmedException e) {
      return read(prior);
    }
    if (view.checkpoint().isPresent()) {
      return newer(prior, replay(view, null, prior.logId()).replica());
    }
    Replay.Result<B> result =
        Replay.replayOnto(machine, machine.toBuilder(prior.state()), prior.committed().toBuilder(),
            prior.logId(), prior.intentionRecords(), view.records(), null);
    return new Replayed(view, result, prior.logId()).replica();
  }

  /**
   * Compacts a log that keeps its checkpoints apart from its records ({@link
   * Capabilities#separateCheckpoints}): appends a GC record at the clock less the retention (none
   * when GC is off), reads through it, stores the replay there as a checkpoint, and trims the
   * records it holds. Any compactor may run it at any time, and each step is safe to repeat or
   * to abandon: a GC record that lands twice drops nothing new, a checkpoint is the replay of the
   * log through its position whoever writes it (I11), and the trim follows a durable checkpoint
   * (P5). Returns the replica it checkpointed.
   */
  public Replica<S> compact() {
    if (!log.capabilities().separateCheckpoints()) {
      throw new UnsupportedOperationException(
          "A log that replaces itself compacts as it commits: " + log);
    }
    long watermark = watermark(Long.MAX_VALUE);
    LogView view;
    if (watermark > 0L) {
      byte[] gc = LogEnvelope.encodeAdvanceHorizon(watermark);
      view = log.readThrough(sendThrottled(() -> log.append(gc, null)), config.visibilityWait());
    } else {
      view = log.read();
    }
    Replica<S> through = replay(view, null).replica();
    log.putCheckpoint(view.tail(), checkpoint(through));
    log.trim(view.tail());
    return through;
  }

  /**
   * The GC watermark a compaction writes now: the clock less the retention, at most {@code
   * ceiling}; 0 (no GC record) when the retention reaches back past the epoch ({@code
   * Long.MAX_VALUE} disables GC).
   */
  private long watermark(long ceiling) {
    long now = config.clockMs().getAsLong();
    long retention = config.committedRetentionMs();
    long watermark = retention >= now ? 0L : now - retention;
    return Math.max(0L, Math.min(watermark, ceiling));
  }

  /** The checkpoint of {@code replica}: exactly its log id, committed set and state. */
  private byte[] checkpoint(Replica<S> replica) {
    return LogEnvelope.encodeCheckpoint(
        replica.logId(), replica.committed().bytes(), machine.encodeCheckpoint(replica.state()));
  }

  /** Sends a write again while the log throttles it, with the commit loop's backoff. */
  private Receipt sendThrottled(Supplier<Receipt> send) {
    long backoffMs = config.throttleMinWaitMs();
    for (int attempt = 1; ; attempt++) {
      try {
        return send.get();
      } catch (LogThrottledException e) {
        if (attempt >= config.maxThrottleAttempts()) {
          throw e;
        }
        sleep(backoffMs + ThreadLocalRandom.current().nextLong(backoffMs / 2 + 1), e);
        backoffMs = Math.min(backoffMs * 2, Math.max(5_000L, config.throttleMinWaitMs()));
      }
    }
  }

  /** Sleeps; an interrupt stops the caller, with {@code cause} (its outcome unknown) thrown. */
  private static void sleep(long ms, RuntimeException cause) {
    if (ms <= 0L) {
      return;
    }
    try {
      Thread.sleep(ms);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      cause.addSuppressed(ie);
      throw new CommitOutcomeUnknownException(cause);
    }
  }

  /**
   * A replica of the log before it exists: the empty state, with a new log id, at the tail of the
   * log not yet created. A commit on it creates the log, and is rejected ({@link
   * CommitRejectedException.Reason#CREATE_LOST}) if another writer created it first.
   *
   * @throws IllegalStateException if the log exists
   */
  public Replica<S> empty() {
    LogTail tail = log.probe();
    if (tail.exists()) {
      throw new IllegalStateException("The log exists");
    }
    return before(tail);
  }

  /**
   * Creates the log, with a record of {@code initial}, unless it exists. Writers may bootstrap the
   * same log at once: one creates it, and the others find it created. A create that fails or
   * whose outcome is unknown is no failure if the log exists afterwards: another writer created
   * it, or this create landed.
   */
  public void bootstrap(I initial) {
    LogTail tail = log.probe();
    if (tail.exists()) {
      return;
    }
    try {
      commit(before(tail), initial);
    } catch (CommitRejectedException | CommitOutcomeUnknownException e) {
      if (!log.probe().exists()) {
        throw e;
      }
    }
  }

  private Replica<S> before(LogTail absent) {
    return new Replica<>(machine.empty().build(), CommittedSet.EMPTY, absent, 0, UuidV7.newUuidV7());
  }

  /**
   * Commits {@code intention}, built from {@code base}, and returns the replica of the log its
   * record landed in.
   *
   * @throws CommitRejectedException if it did not take effect, and never will
   * @throws CommitOutcomeUnknownException if it may or may not have
   */
  public Replica<S> commit(Replica<S> base, I intention) {
    Replica<S> landed = new Commit(base, intention).run();
    Capabilities caps = log.capabilities();
    if (caps.separateCheckpoints() && !caps.fusedReplace()) {
      // The writer whose commit landed compacts, as the policy says of the log it read; the commit
      // stands whatever becomes of the compaction.
      try {
        if (config.compaction().due(landed.intentionRecords(), landed.tail().bytes(), 0)) {
          compact();
        }
      } catch (RuntimeException e) {
        LOG.log(System.Logger.Level.WARNING, "Compaction after a commit failed: " + log, e);
      }
    }
    return landed;
  }

  private static final System.Logger LOG = System.getLogger(LogRuntime.class.getName());

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
    // The races this commit lost (writes that failed their condition): each doubles the bound of
    // the wait before it reads or probes the log again (raceBackoff).
    private int lostRaces = 0;
    // Whether the record is larger than one append stores (an Azure AppendBlock: 4 MiB before
    // service version 2022-11-02, 100 MiB since): only a replace can commit it. An append of it
    // fails without landing, which the commit took for ambiguous on every attempt (Bug 71).
    private final boolean oversized;
    // A send that the log throttled before the one it acknowledged, if the send was unconditional:
    // it may have landed too, so the record may be in the log twice (on one object, the second
    // send fails its condition instead, P4).
    private Throwable duplicate = null;
    // On a log that cannot replace itself, the tail no read of this commit may fall behind: the
    // read it staged on, or a later one it adopted. On a read that lags behind it, the intention
    // may not hold yet while its record has applied.
    private LogTail floor;

    Commit(Replica<S> base, I intention) {
      this.base = base;
      this.intention = intention;
      // After the horizon of the log this writer read, where replay rejects a record (Bug 64).
      this.id = UuidV7.newUuidV7After(base.committed().horizon());
      this.record = LogEnvelope.encodeRecord(id, machine.encodeIntention(intention));
      this.oversized = caps.maxRecordBytes() > 0 && record.length > caps.maxRecordBytes();
      this.current = base;
      this.tail = base.tail();
      this.floor = !caps.fusedReplace() && tail.exists() ? tail : null;
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
      if (oversized && !caps.fusedReplace()) {
        throw new CommitRejectedException(
            CommitRejectedException.Reason.TOO_LARGE, null,
            String.format("Cannot commit: a record of %d bytes is larger than %s stores (%d)",
                record.length, log, caps.maxRecordBytes()),
            null);
      }

      // Case 0: the log does not exist yet. A log that replaces itself is created by a replace of
      // the absent log; one that keeps checkpoints apart, by its first checkpoint.
      if (!tail.exists() && !caps.fusedReplace()) {
        create();
      } else if (!tail.exists()) {
        try {
          return validate(write(() -> log.replace(tail, checkpoint(base), records(base)), false));
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
          // observable state. A log without append replaces on every commit, and a record larger
          // than an append stores goes in a replace.
          boolean mustReplace =
              caps.fusedReplace()
                  && (full
                      || oversized
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
              return validate(write(() -> log.replace(at, checkpoint(on), records(on)), false));
            } catch (LogConflictException e) {
              unresolved = e;
              raceBackoff(e);
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
              boolean conditional = caps.conditionalAppend();
              return validate(
                  write(() -> log.append(record, conditional ? at : null), !conditional));
            } catch (LogFullException e) {
              // Nothing landed, and no append to this log can: replace it, on a fresh read.
              // Without this the commit took the append for ambiguous, and with a count above the
              // store's limit no writer ever compacted (Bug 66).
              full = true;
              stale = true;
              continue;
            } catch (LogConflictException e) {
              unresolved = e;
              raceBackoff(e);
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

    /**
     * Waits after a lost race, before the commit reads or probes the log again: a time drawn
     * uniformly below a bound that starts at {@code raceMinWaitMs} and doubles with each race the
     * commit lost, up to the larger of 1 s and that (full jitter). The writers who lost to one
     * winner would otherwise all read and resend at once, and on a log that only replaces itself
     * most of them lose again: each loss costs a read of the whole log. An interrupt stops the
     * commit, its outcome unknown (the write that lost may have landed).
     */
    private void raceBackoff(RuntimeException lost) {
      lostRaces += 1;
      long base = config.raceMinWaitMs();
      if (base <= 0L) {
        return;
      }
      long bound = Math.min(base << Math.min(lostRaces - 1, 20), Math.max(1_000L, base));
      sleep(ThreadLocalRandom.current().nextLong(bound + 1), lost);
    }

    private void adopt(Replayed fresh) {
      current = fresh.replica();
      tail = current.tail();
      stale = false;
      if (!caps.fusedReplace()) {
        floor = tail;
      }
    }

    /** The last write of this commit, but one acknowledged, that may have landed, or null. */
    private Throwable earlier() {
      return ambiguous != null ? ambiguous.getCause() : unresolved != null ? unresolved : duplicate;
    }

    /**
     * Creates a log that keeps its checkpoints apart: stores the base's checkpoint (the empty
     * state, with the new log's id) as the log's first, and adopts a read of the log created.
     * Nothing of the intention has been sent, so a create that another writer beat, or whose
     * outcome no read settles, rejects the commit.
     */
    private void create() {
      RuntimeException failure = null;
      try {
        log.putCheckpoint(tail, checkpoint(base));
      } catch (RuntimeException e) {
        failure = e; // another writer's create, or one of ours that may have landed
      }
      Replayed fresh;
      try {
        fresh = replay(log.read(), id, base.logId());
      } catch (RuntimeException e) {
        if (failure != null) {
          e.addSuppressed(failure);
        }
        throw new CommitRejectedException(CommitRejectedException.Reason.NOT_SENT, null,
            "Cannot commit: creating the log failed, and reading it failed too: " + e, e);
      }
      if (!fresh.replica().logId().equals(base.logId())) {
        throw new CommitRejectedException(CommitRejectedException.Reason.CREATE_LOST,
            fresh.replica(), "Cannot commit: another writer created the log first", failure);
      }
      adopt(fresh);
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
      long watermark = watermark(UuidV7.timestampMs(id) - 1);
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
    private Replayed write(Supplier<Receipt> send, boolean unconditional) {
      long backoffMs = config.throttleMinWaitMs();
      LogThrottledException lastThrottle = null;
      for (int attempt = 0; attempt < config.maxThrottleAttempts(); attempt++) {
        Receipt receipt;
        try {
          receipt = send.get();
        } catch (LogThrottledException e) {
          lastThrottle = e;
          sleep(backoffMs + ThreadLocalRandom.current().nextLong(backoffMs / 2 + 1), e);
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
        if (unconditional && lastThrottle != null) {
          // The throttled send may have landed beside this one, and a view through this one may
          // start from a checkpoint that holds the other (dup in SharedLogCommit.tla).
          duplicate = lastThrottle;
        }
        Replayed written;
        try {
          written = replay(log.readThrough(receipt, config.visibilityWait()), id, base.logId());
        } catch (RuntimeException e) {
          throw new AmbiguousWrite(e);
        }
        if (!written.result.seen()) {
          // Only a view whose records include this one can settle it (P3): a checkpoint that holds
          // it cannot say whether it applied once GC drops its id.
          throw new AmbiguousWrite(new IllegalStateException(
              "A read through a write to " + log + " does not include its record"));
        }
        return written;
      }
      throw new AmbiguousWrite(
          new IOException(
              String.format("Storage throttled %d attempts", config.maxThrottleAttempts()),
              lastThrottle));
    }

    /**
     * A fresh read; any failure becomes {@link UnreadableLog}. On a log that cannot replace itself,
     * a read that lags behind {@link #floor} is read again, waiting up to the visibility wait in
     * all, and then fails ({@link LogLagException}).
     */
    private Replayed read() {
      try {
        LogView view = log.read();
        for (int attempt = 1; floor != null && !view.tail().continues(floor); attempt++) {
          if (attempt >= config.maxThrottleAttempts()) {
            throw new LogLagException("A read of " + log + " lags behind one this commit made");
          }
          sleep(config.visibilityWait().toMillis() / config.maxThrottleAttempts(),
              new LogLagException("Interrupted waiting for " + log));
          view = log.read();
        }
        return replay(view, id, base.logId());
      } catch (CommitOutcomeUnknownException e) {
        throw new UnreadableLog((RuntimeException) e.getCause()); // interrupted waiting
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
