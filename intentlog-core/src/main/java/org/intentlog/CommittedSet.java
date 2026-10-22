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
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.intentlog.wire.CommittedSetCodec;
import org.intentlog.wire.UuidV7;

/**
 * The ids of the committed transactions a log's replay has applied, less those GC dropped, and
 * the GC horizon: the newest timestamp it dropped. Replay applies a record once by its id, and a
 * writer learns from the set whether its commit landed. Stored as the {@code
 * CommittedTransactions} wire bytes, which queries scan in place. See docs/GC.md.
 */
public final class CommittedSet {

  public static final CommittedSet EMPTY = new CommittedSet(new byte[0]);

  private final byte[] bytes;

  private CommittedSet(byte[] bytes) {
    this.bytes = bytes;
  }

  /** The set encoded by a {@code CommittedTransactions} body; empty for null or no bytes. */
  public static CommittedSet of(byte[] bytes) {
    return bytes == null || bytes.length == 0 ? EMPTY : new CommittedSet(bytes);
  }

  /** The {@code CommittedTransactions} body; empty iff the set holds no id and no horizon. */
  public byte[] bytes() {
    return bytes;
  }

  /** The newest timestamp GC dropped (0 before any GC). */
  public long horizon() {
    return CommittedSetCodec.horizon(bytes);
  }

  /**
   * Whether {@code id} committed. True if the set holds it; false if it does not and the id is
   * newer than the horizon, or not a UUIDv7; and if it does not and the id is at or below the
   * horizon, the set cannot tell an aged-out commit from one that never landed, and this throws
   * {@link DedupHorizonExceededException}.
   */
  public boolean contains(UUID id) {
    if (CommittedSetCodec.contains(bytes, id)) {
      return true;
    }
    checkAboveHorizon(id, horizon());
    return false;
  }

  /**
   * Whether replay may apply the record of {@code id}: the set neither holds the id (the record
   * is a retry of one already applied) nor dropped ids at or after its time. A record at or below
   * the horizon may be a retry of a commit whose id aged out, so replay rejects it (Bug 64).
   */
  public boolean mayApply(UUID id) {
    return !CommittedSetCodec.contains(bytes, id) && aboveHorizon(id, horizon());
  }

  /** Decodes every id in the set, newest first. */
  public Set<UUID> ids() {
    if (bytes.length == 0) {
      return Collections.emptySet();
    }
    try {
      return Collections.unmodifiableSet(CommittedSetCodec.decode(bytes));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** A builder that adds ids to this set. */
  public Builder toBuilder() {
    return new Builder(bytes);
  }

  public static Builder builder() {
    return new Builder(new byte[0]);
  }

  private static boolean aboveHorizon(UUID id, long horizon) {
    return !UuidV7.isV7(id) || horizon == 0L || UuidV7.timestampMs(id) > horizon;
  }

  private static void checkAboveHorizon(UUID id, long horizon) {
    if (!aboveHorizon(id, horizon)) {
      throw new DedupHorizonExceededException(id, horizon);
    }
  }

  /**
   * An inherited set plus the ids replay adds to it. The additions merge into the inherited bytes
   * only when the set is built, in one pass.
   */
  public static final class Builder {
    private byte[] inherited;
    private final Set<UUID> added = new HashSet<>();

    private Builder(byte[] inherited) {
      this.inherited = inherited;
    }

    /** Replaces the inherited set (a decoded checkpoint's); additions are kept. */
    public Builder inherit(byte[] bytes) {
      this.inherited = bytes != null ? bytes : new byte[0];
      return this;
    }

    public Builder add(UUID id) {
      if (!UuidV7.isV7(id)) {
        throw new IllegalArgumentException("Committed transaction IDs must be UUIDv7; got " + id);
      }
      added.add(id);
      return this;
    }

    /** {@link CommittedSet#contains}, counting the ids added so far; the horizon is inherited. */
    public boolean contains(UUID id) {
      if (added.contains(id) || CommittedSetCodec.contains(inherited, id)) {
        return true;
      }
      checkAboveHorizon(id, horizon());
      return false;
    }

    /** {@link CommittedSet#mayApply}, counting the ids added so far. */
    public boolean mayApply(UUID id) {
      return !added.contains(id)
          && !CommittedSetCodec.contains(inherited, id)
          && aboveHorizon(id, horizon());
    }

    public long horizon() {
      return CommittedSetCodec.horizon(inherited);
    }

    /**
     * Applies a GC record ({@code AdvanceHorizon}): drops every id at or below {@code
     * watermarkMs}, inherited or added, and raises the horizon to the newest one dropped. A
     * watermark at or below the horizon drops nothing new.
     */
    public Builder advanceHorizon(long watermarkMs) {
      if (watermarkMs < 0L) {
        throw new IllegalArgumentException("watermarkMs must be >= 0, got " + watermarkMs);
      }
      if (watermarkMs > 0L) {
        byte[] merged =
            added.isEmpty() ? inherited : CommittedSetCodec.rewrite(inherited, added, 0L);
        this.inherited = CommittedSetCodec.rewrite(merged, Collections.emptySet(), watermarkMs);
        added.clear();
      }
      return this;
    }

    public CommittedSet build() {
      if (added.isEmpty()) {
        return of(inherited);
      }
      return of(CommittedSetCodec.rewrite(inherited, added, 0L));
    }
  }
}
