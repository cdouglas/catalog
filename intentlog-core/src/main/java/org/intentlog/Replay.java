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
 * <p>A GC record ({@code AdvanceHorizon}) drops the committed ids at or below its watermark. An
 * intention record applies once: replay skips one whose id the committed set holds (a retry of
 * one applied) or that is at or below the set's horizon (it may be a retry of one whose id aged
 * out), and one whose id is not a UUIDv7; it applies one whose preconditions hold where it lands,
 * and adds its id to the set; it skips one whose preconditions do not hold.
 */
public final class Replay {

  private Replay() {}

  /** What replay did with the first record of a watched id. */
  public enum Outcome {
    /** The replay held no record of the id it could judge (none, or one at or below the horizon). */
    NOT_SEEN,
    /** Replay applied the record. */
    APPLIED,
    /** Replay judged the record and did not apply it: its preconditions did not hold. */
    REJECTED
  }

  /** The result of a replay: the builder replay folded the records into, and what it saw. */
  public static final class Result<B> {
    private final B builder;
    private final CommittedSet.Builder committed;
    private final UUID logId;
    private final long intentionRecords;
    private final Outcome outcome;
    private final boolean seen;

    private Result(
        B builder, CommittedSet.Builder committed, UUID logId, long intentionRecords,
        Outcome outcome, boolean seen) {
      this.builder = builder;
      this.committed = committed;
      this.logId = logId;
      this.intentionRecords = intentionRecords;
      this.outcome = outcome;
      this.seen = seen;
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

    /** The number of intention records after the checkpoint (GC records not counted). */
    public long intentionRecords() {
      return intentionRecords;
    }

    /** What replay did with the first record of the watched id. */
    public Outcome outcome() {
      return outcome;
    }

    /**
     * Whether a record of the watched id was among the records replayed, judged or skipped (a
     * retry of one applied, or at or below the horizon). A read through a write whose record is
     * not among them cannot settle the write's outcome.
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
    if (checkpoint != null) {
      LogEnvelope.Checkpoint envelope = LogEnvelope.decodeCheckpoint(checkpoint);
      return replayOnto(machine, machine.decodeCheckpoint(envelope.state()),
          CommittedSet.of(envelope.committed()).toBuilder(), envelope.logId(), 0, records, watch);
    }
    return replayOnto(machine, machine.empty(), CommittedSet.builder(), null, 0, records, watch);
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
    Outcome outcome = Outcome.NOT_SEEN;
    boolean seen = false;
    for (byte[] bytes : records) {
      LogEnvelope.Record record = LogEnvelope.decodeRecord(bytes);
      if (record.isAdvanceHorizon()) {
        committed.advanceHorizon(record.watermarkMs());
        continue;
      }
      intentionRecords++;
      UUID id = record.id();
      seen |= id.equals(watch);
      if (!committed.mayApply(id)) {
        // Already applied (a retry), or its id is at or below the committed set's GC horizon,
        // where the set cannot tell a retry of an aged-out commit from a new one.
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
      } else if (watched) {
        outcome = Outcome.REJECTED;
      }
    }
    return new Result<>(builder, committed, logId, intentionRecords, outcome, seen);
  }
}
