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
import org.intentlog.wire.LogEnvelope;
import org.intentlog.wire.UuidV7;

/**
 * Replay of a log: a checkpoint, and the records after it folded into its state in log order.
 *
 * <p>A GC record ({@code AdvanceHorizon}) drops the committed ids and pages at or below its
 * watermark, and a {@code PageOut} seals the set's oldest block into a page if it is still the
 * block its writer paged (docs/COMMITTED_PAGING.md); neither carries an id, and both apply wherever
 * they land. An intention record applies once: replay skips one whose id the committed set holds
 * (a retry of one applied) or that is at or below the set's floor (it may be a retry of one whose
 * id was paged or aged out; replay never reads a page), and one whose id is not a UUIDv7; it
 * applies one whose preconditions hold where it lands, and adds its id to the set; it skips one
 * whose preconditions do not hold.
 */
public final class Replay {

  private Replay() {}

  /** What replay did with the first record of a watched id. */
  public enum Outcome {
    /** The replay held no record of the id it could judge (none, or one at or below the floor). */
    NOT_SEEN,
    /** Replay applied the record. */
    APPLIED,
    /** Replay judged the record and did not apply it: its preconditions did not hold. */
    REJECTED
  }

  /**
   * Told of each intention record replay rejects where it lands, with the reason its intention
   * gives ({@link Intention#explain}); for counting conflicts by their cause.
   */
  @FunctionalInterface
  public interface Rejections {
    /** Replay rejected the record of {@code id}: {@code reason}, or null if it cannot say. */
    void rejected(UUID id, String reason);
  }

  /** Why replay rejects a record whose id is not a UUIDv7. */
  public static final String NOT_V7 = "log: not a UUIDv7 record";

  /** The result of a replay: the builder replay folded the records into, and what it saw. */
  public static final class Result<B> {
    private final B builder;
    private final CommittedSet.Builder committed;
    private final UUID logId;
    private final long intentionRecords;
    private final long recordsSinceGc;
    private final long bytesSinceGc;
    private final Outcome outcome;
    private final String rejection;
    private final boolean seen;
    private final IntentionIds ids;

    private Result(
        B builder, CommittedSet.Builder committed, UUID logId, long intentionRecords,
        long recordsSinceGc, long bytesSinceGc, Outcome outcome, String rejection, boolean seen,
        IntentionIds ids) {
      this.builder = builder;
      this.committed = committed;
      this.logId = logId;
      this.intentionRecords = intentionRecords;
      this.recordsSinceGc = recordsSinceGc;
      this.bytesSinceGc = bytesSinceGc;
      this.outcome = outcome;
      this.rejection = rejection;
      this.seen = seen;
      this.ids = ids;
    }

    /**
     * The ids of the intention records replayed, in log order: applied, rejected or skipped (GC
     * and PageOut records have none).
     */
    IntentionIds ids() {
      return ids;
    }

    /** The state machine's builder, the records applied. */
    public B builder() {
      return builder;
    }

    /** The committed set, the ids of the records applied added. */
    public CommittedSet.Builder committed() {
      return committed;
    }

    /** The log's id, from its checkpoint; null if it has none. */
    public UUID logId() {
      return logId;
    }

    /** The number of intention records after the checkpoint (GC and PageOut records not counted). */
    public long intentionRecords() {
      return intentionRecords;
    }

    /**
     * The intention records after the checkpoint, or after the last GC record among them if one
     * is: a GC record is where a compaction began, and the checkpoint it puts covers it. On a log
     * that keeps its checkpoints apart, a writer that reads only what follows its own tail never
     * sees another writer's checkpoint, but replays its GC record.
     */
    public long recordsSinceGc() {
      return recordsSinceGc;
    }

    /** The bytes of the records {@link #recordsSinceGc} counts. */
    public long bytesSinceGc() {
      return bytesSinceGc;
    }

    /** What replay did with the first record of the watched id. */
    public Outcome outcome() {
      return outcome;
    }

    /**
     * Why replay rejected the watched record, as its intention explains it, if the outcome is
     * {@link Outcome#REJECTED}; else (or if it cannot say) null.
     */
    public String rejection() {
      return rejection;
    }

    /**
     * Whether a record of the watched id was among the records replayed, judged or skipped (a
     * retry of one applied, or at or below the floor). A read through a write whose record is not
     * among them cannot settle the write's outcome.
     */
    public boolean seen() {
      return seen;
    }
  }

  /**
   * Replays {@code records} onto the state {@code checkpoint} holds.
   *
   * @param checkpoint a log.proto LogCheckpoint, or null for a log that has none
   * @param records log.proto LogRecords, in log order
   * @param watch an id whose record's outcome to report, or null
   */
  public static <S, B extends StateMachine.Builder<S>, I extends Intention<B>> Result<B> replay(
      StateMachine<S, B, I> machine, byte[] checkpoint, Iterable<byte[]> records, UUID watch) {
    return replay(machine, checkpoint, records, watch, null);
  }

  /**
   * Replays {@code records} onto the state {@code checkpoint} holds, telling {@code rejections}
   * (if not null) of each record it rejects and why.
   */
  public static <S, B extends StateMachine.Builder<S>, I extends Intention<B>> Result<B> replay(
      StateMachine<S, B, I> machine, byte[] checkpoint, Iterable<byte[]> records, UUID watch,
      Rejections rejections) {
    if (checkpoint != null) {
      LogEnvelope.Checkpoint envelope = LogEnvelope.decodeCheckpoint(checkpoint);
      return replayOnto(machine, machine.decodeCheckpoint(envelope.state()),
          CommittedSet.of(envelope.committed()).toBuilder(), envelope.logId(), 0, records, watch,
          rejections);
    }
    return replayOnto(machine, machine.empty(), CommittedSet.builder(), null, 0, records, watch,
        rejections);
  }

  /**
   * Replays {@code records} onto a state and committed set already replayed (a replica that a
   * read reached, which the records follow), which this mutates.
   *
   * @param logId the log's id, or null if unknown
   * @param intentionRecords the intention records replayed before these, after the checkpoint
   */
  public static <S, B extends StateMachine.Builder<S>, I extends Intention<B>> Result<B> replayOnto(
      StateMachine<S, B, I> machine, B builder, CommittedSet.Builder committed, UUID logId,
      long intentionRecords, Iterable<byte[]> records, UUID watch) {
    return replayOnto(machine, builder, committed, logId, intentionRecords, records, watch, null);
  }

  /**
   * Replays {@code records} onto a replayed state and committed set, as {@link #replayOnto(
   * StateMachine, StateMachine.Builder, CommittedSet.Builder, UUID, long, Iterable, UUID)} does,
   * telling {@code rejections} (if not null) of each record it rejects and why.
   */
  public static <S, B extends StateMachine.Builder<S>, I extends Intention<B>> Result<B> replayOnto(
      StateMachine<S, B, I> machine, B builder, CommittedSet.Builder committed, UUID logId,
      long intentionRecords, Iterable<byte[]> records, UUID watch, Rejections rejections) {
    return replayOnto(machine, builder, committed, logId, intentionRecords, intentionRecords, 0L,
        records, watch, rejections);
  }

  /**
   * As {@link #replayOnto(StateMachine, StateMachine.Builder, CommittedSet.Builder, UUID, long,
   * Iterable, UUID, Rejections)}, continuing the counts since the last GC record ({@link
   * Result#recordsSinceGc}, {@link Result#bytesSinceGc}) from those of the replica the records
   * follow.
   */
  public static <S, B extends StateMachine.Builder<S>, I extends Intention<B>> Result<B> replayOnto(
      StateMachine<S, B, I> machine, B builder, CommittedSet.Builder committed, UUID logId,
      long intentionRecords, long recordsSinceGc, long bytesSinceGc, Iterable<byte[]> records,
      UUID watch, Rejections rejections) {
    Outcome outcome = Outcome.NOT_SEEN;
    String rejection = null;
    boolean seen = false;
    IntentionIds.Builder ids = new IntentionIds.Builder();
    for (byte[] bytes : records) {
      LogEnvelope.Record record = LogEnvelope.decodeRecord(bytes);
      if (record.isAdvanceHorizon()) {
        committed.advanceHorizon(record.watermarkMs());
        recordsSinceGc = 0;
        bytesSinceGc = 0;
        continue;
      }
      if (record.isPageOut()) {
        committed.pageOut(record.pageOut());
        continue;
      }
      intentionRecords++;
      recordsSinceGc++;
      bytesSinceGc += bytes.length;
      UUID id = record.id();
      ids.add(id);
      seen |= id.equals(watch);
      if (!committed.mayApply(id)) {
        // Already applied (a retry), or its id is at or below the committed set's floor, where
        // the set holds pages or nothing and cannot tell a retry of a paged or aged-out commit
        // from a new one without reading a page, which replay never does.
        continue;
      }
      boolean watched = id.equals(watch) && outcome == Outcome.NOT_SEEN;
      // The committed set holds only UUIDv7s (its encoding drops the version bits), so a record
      // with another id cannot apply: reject it, don't fail the read.
      I intention = UuidV7.isV7(id) ? machine.decodeIntention(record.intention()) : null;
      if (intention != null && intention.verify(builder)) {
        intention.apply(builder);
        committed.add(id);
        if (watched) {
          outcome = Outcome.APPLIED;
        }
      } else if (watched || rejections != null) {
        // Why, from the state it was judged on: only where it was rejected, so a replay that
        // rejects nothing pays nothing.
        String why = intention == null ? NOT_V7 : explain(intention, builder);
        if (watched) {
          outcome = Outcome.REJECTED;
          rejection = why;
        }
        if (rejections != null) {
          rejections.rejected(id, why);
        }
      }
    }
    return new Result<>(builder, committed, logId, intentionRecords, recordsSinceGc, bytesSinceGc,
        outcome, rejection, seen, ids.build());
  }

  /** Why {@code intention} does not verify on {@code builder}; null if it cannot say. */
  static <B> String explain(Intention<B> intention, B builder) {
    try {
      return intention.explain(builder);
    } catch (RuntimeException e) {
      return null; // an explanation must never fail a read
    }
  }
}
