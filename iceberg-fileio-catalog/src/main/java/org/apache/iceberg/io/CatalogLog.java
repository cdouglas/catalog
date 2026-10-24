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
import java.util.concurrent.atomic.AtomicReference;
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
  private final LogRuntime<CatalogState, CatalogState.Builder, ProtoCodec.CatalogIntention> runtime;
  // The newest replica this catalog has read or committed in. A read of a log whose reads lag
  // (a replica behind its appends) serves it instead when the read is behind it, so a reader
  // never sees the catalog go back past a commit or a read of its own: Iceberg refreshes a table
  // from the catalog right after committing to it (Bug 72).
  private final AtomicReference<Replica<CatalogState>> newest = new AtomicReference<>();

  public CatalogLog(SharedLog log, RuntimeConfig config) {
    this.log = log;
    this.runtime = new LogRuntime<>(log, CatalogStateMachine.INSTANCE, config);
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

  /**
   * The catalog before its log exists: a transaction staged on it creates the log, and fails if
   * another writer created it first.
   *
   * @throws IllegalStateException if the log exists
   */
  public Replica<CatalogState> empty() {
    return runtime.empty();
  }

  /** Creates the catalog's log, holding no namespaces or tables, unless it exists. */
  public void bootstrap() {
    try {
      runtime.bootstrap(new ProtoCodec.CatalogIntention(List.of()));
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
      return seen(runtime.commit(mutation.base(), mutation.intention()));
    } catch (CommitRejectedException e) {
      switch (e.reason()) {
        case CONFLICT:
          Replica<CatalogState> lostTo = e.state();
          throw mutation.explain(lostTo.state(), e.getMessage() + " at " + log);
        case CREATE_LOST:
          throw new CommitFailedException(e, "Catalog creation failed: %s exists", log);
        default:
          throw new CommitFailedException(e, "%s", e.getMessage());
      }
    } catch (CommitOutcomeUnknownException e) {
      throw new CommitStateUnknownException(e.getCause());
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
