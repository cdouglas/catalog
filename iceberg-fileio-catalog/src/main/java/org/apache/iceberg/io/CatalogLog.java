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
package org.apache.iceberg.io;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.intentlog.CommitOutcomeUnknownException;
import org.intentlog.CommitRejectedException;
import org.intentlog.LogRuntime;
import org.intentlog.Replica;
import org.intentlog.RuntimeConfig;
import org.intentlog.SharedLog;

/**
 * The catalog's state machine ({@link CatalogStateMachine}) replicated through a shared log by a
 * {@link LogRuntime}: reads of the log, and commits of transactions ({@link CatalogMutation}) to
 * it, with the runtime's outcomes mapped onto Iceberg's exceptions.
 */
public final class CatalogLog {

  private final SharedLog log;
  private final CatalogStateMachine machine;
  private final LogRuntime<CatalogState, CatalogState.Builder, ProtoCodec.CatalogIntention> runtime;
  // The newest replica this catalog has read or committed in. A read of a log whose reads lag
  // (a replica behind its appends) serves it instead when the read is behind it, so a reader
  // never sees the catalog go back past a commit or a read of its own: Iceberg refreshes a table
  // from the catalog right after committing to it (Bug 72).
  private final AtomicReference<Replica<CatalogState>> newest = new AtomicReference<>();
  private final CommitCounts counts = new CommitCounts();

  public CatalogLog(SharedLog log, RuntimeConfig config) {
    this(log, config, LogCatalogProperties.DEFAULT_CHECKPOINT_MEMOIZE);
  }

  /**
   * A catalog log whose checkpoints write the inline tables that have not changed as the bytes
   * they had, if {@code memoizeCheckpoints} ({@link LogCatalogProperties#CHECKPOINT_MEMOIZE}).
   */
  public CatalogLog(SharedLog log, RuntimeConfig config, boolean memoizeCheckpoints) {
    this.log = log;
    this.machine = CatalogStateMachine.forCatalog(memoizeCheckpoints);
    this.runtime = new LogRuntime<>(log, machine, config);
  }

  /**
   * The catalog as a fresh read of its log finds it, or as this catalog last read or committed in
   * it if the read lags behind that. A read failure is thrown as it is.
   */
  public Replica<CatalogState> read() {
    return seen(runtime.read(newest.get()));
  }

  /** Records {@code replica}, read or committed in, as the newest this catalog has seen. */
  private Replica<CatalogState> seen(Replica<CatalogState> replica) {
    newest.accumulateAndGet(replica, runtime::newer);
    return replica;
  }

  /** The log's page store, where sealed snapshot blocks' pages are; null if it has none. */
  org.intentlog.PageStore pages() {
    return log.pages();
  }

  /**
   * The catalog before its log exists: a transaction staged on it creates the log, and fails if
   * another writer created it first.
   *
   * @throws IllegalStateException if the log exists
   */
  public Replica<CatalogState> empty() {
    return runtime.empty();
  }

  /**
   * Creates the catalog's log, holding no namespaces or tables and judging table updates by
   * {@code conflictEncoding} ({@link ConflictEncoding}), unless it exists.
   */
  public void bootstrap(String conflictEncoding) {
    bootstrap(conflictEncoding, 0, 0);
  }

  /**
   * As {@link #bootstrap(String)}, paging its inline tables' snapshots in blocks of {@code
   * snapshotBlock} sequence numbers sealed {@code snapshotWindow} behind a table's last (0: none).
   */
  public void bootstrap(String conflictEncoding, int snapshotBlock, int snapshotWindow) {
    List<ProtoCodec.Action> actions = new java.util.ArrayList<>();
    actions.add(new ProtoCodec.SetConflictEncodingAction(conflictEncoding));
    if (snapshotBlock > 0) {
      actions.add(new ProtoCodec.SetSnapshotPagingAction(snapshotBlock, snapshotWindow));
    }
    try {
      runtime.bootstrap(new ProtoCodec.CatalogIntention(actions));
    } catch (CommitRejectedException e) {
      throw new CommitFailedException(e, "Cannot create the catalog at %s: %s", log, e.getMessage());
    } catch (CommitOutcomeUnknownException e) {
      throw new CommitStateUnknownException(e.getCause());
    }
  }

  /**
   * Commits {@code mutation} on top of the read it was staged on, and returns the read of the log
   * its record landed in.
   *
   * @throws CommitFailedException if it did not take effect, and never will; or a subclass, or
   *     {@link org.apache.iceberg.exceptions.AlreadyExistsException} or {@link
   *     org.apache.iceberg.exceptions.NoSuchTableException}, as the state it lost to explains it
   *     ({@link CatalogMutation#explain})
   * @throws CommitStateUnknownException if it may or may not have
   */
  public Replica<CatalogState> commit(CatalogMutation mutation) {
    Preconditions.checkNotNull(mutation.base(), "Cannot commit a transaction staged on no read");
    try {
      Replica<CatalogState> landed = seen(runtime.commit(mutation.base(), mutation.intention()));
      counts.committed.increment();
      return landed;
    } catch (CommitRejectedException e) {
      switch (e.reason()) {
        case CONFLICT:
          counts.conflicts.increment();
          counts.conflicted(e.rejection());
          Replica<CatalogState> lostTo = e.state();
          throw mutation.explain(lostTo.state(), e.getMessage() + " at " + log);
        case CREATE_LOST:
          counts.rejected.increment();
          throw new CommitFailedException(e, "Catalog creation failed: %s exists", log);
        default:
          counts.rejected.increment();
          throw new CommitFailedException(e, "%s", e.getMessage());
      }
    } catch (CommitOutcomeUnknownException e) {
      counts.unknown.increment();
      throw new CommitStateUnknownException(e.getCause());
    }
  }

  /** The memo of this catalog's checkpoint encodings, or null if it keeps none. */
  CheckpointMemo checkpointMemo() {
    return machine.memo();
  }

  /** How this catalog's commits ended, since it opened its log. */
  public CommitCounts counts() {
    return counts;
  }

  /**
   * Counts of a catalog's commits by outcome: each commit of a transaction ends committed, in
   * conflict (its record did not apply where it landed, and Iceberg may retry the operation on a
   * fresh read), rejected otherwise, or unknown; and conflicts by their reason, as replay or the
   * commit loop explained them ({@link CatalogMutation}'s intention's {@code explain}).
   */
  public static final class CommitCounts {
    /** The reason of a conflict that has none. */
    static final String UNEXPLAINED = "unexplained";

    private final LongAdder committed = new LongAdder();
    private final LongAdder conflicts = new LongAdder();
    private final Map<String, LongAdder> reasons = new ConcurrentHashMap<>();
    private final LongAdder rejected = new LongAdder();
    private final LongAdder unknown = new LongAdder();

    public long committed() {
      return committed.sum();
    }

    public long conflicts() {
      return conflicts.sum();
    }

    public long rejected() {
      return rejected.sum();
    }

    public long unknown() {
      return unknown.sum();
    }

    /** Conflicts by their reason, in its order. */
    public Map<String, Long> conflictReasons() {
      Map<String, Long> counts = new TreeMap<>();
      reasons.forEach((reason, count) -> counts.put(reason, count.sum()));
      return counts;
    }

    private void conflicted(String reason) {
      reasons.computeIfAbsent(reason != null ? reason : UNEXPLAINED, r -> new LongAdder())
          .increment();
    }

    @Override
    public String toString() {
      return String.format("committed=%d conflicts=%d rejected=%d unknown=%d",
          committed(), conflicts(), rejected(), unknown());
    }
  }

  /**
   * Compacts a log that keeps its checkpoints apart from its records (a GC record, a checkpoint
   * through it, a trim), as a background compactor would; a writer whose commit lands compacts by
   * the commit knobs anyway. Returns the catalog it checkpointed.
   *
   * @throws UnsupportedOperationException for a log that replaces itself, which compacts as it
   *     commits
   */
  public Replica<CatalogState> compact() {
    return seen(runtime.compact());
  }

  @Override
  public String toString() {
    return log.toString();
  }
}
