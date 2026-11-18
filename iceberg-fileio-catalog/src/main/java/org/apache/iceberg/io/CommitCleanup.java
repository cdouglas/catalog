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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.util.ThreadPools;

/**
 * Deletes what a catalog's operations wrote ahead of commits that definitely failed (Bug 119
 * (failed attempts leave metadata files)): a metadata file, and a manifest list file for each of
 * its inline-ML snapshots, written for an update that goes to a metadata file (an inline table's
 * that does not fit inline, a commit to a pointer table, an eviction to fit the log's record
 * bound) or for a create; and a dropped inline table's version, written for a drop that keeps its
 * files. Each has a name unique to the attempt that wrote it, so once that attempt's record cannot
 * take effect nothing will ever name it. Never a page: a page's name is its content's address,
 * which another writer's record may name; only the sweep deletes pages ({@link
 * LogCatalog#deleteOrphanPages}).
 *
 * <p>An operation hands its files over once its outcome is known, as Hive's and Glue's table
 * operations delete a metadata file whose commit failed; the deletes run off its thread, on a small
 * pool of daemon threads that every catalog in the JVM shares, so they add nothing to its latency.
 * A FileIO with bulk deletes ({@link SupportsBulkOperations}) deletes each operation's files in
 * one call, another one at a time. A delete that fails is logged, not retried: the file is left for
 * a GC by listing, as are deletes still queued when the JVM exits.
 *
 * <p>Closing a catalog closes its cleanup ({@link #close}): it waits up to {@link #CLOSE_WAIT_MS}
 * for the deletes handed over, then drops those not started, logging the files they leave.
 */
final class CommitCleanup implements java.io.Closeable {
  private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(CommitCleanup.class);

  /** How long {@link #close} waits for the deletes handed over before it drops the rest. */
  static final long CLOSE_WAIT_MS = 2_000;

  /** The threads of the pool every catalog in the JVM shares. */
  static final int POOL_SIZE = 2;

  /** The pool, made on first use. */
  private static final class SharedPool {
    private static final ExecutorService POOL =
        ThreadPools.newExitingWorkerPool("fileio-catalog-cleanup", POOL_SIZE);
  }

  private final FileIO io;
  private final Executor executor;
  private final long closeWaitMs;
  // The batches handed over and not yet done; guarded by this
  private int pending = 0;
  private volatile boolean closed = false;

  /**
   * The cleanup of a catalog whose tables' files are in {@code io}, deleting on {@code executor},
   * or on the pool every catalog shares if it is null.
   */
  CommitCleanup(FileIO io, Executor executor) {
    this(io, executor, CLOSE_WAIT_MS);
  }

  /** As {@link #CommitCleanup(FileIO, Executor)}, whose {@link #close} waits {@code closeWaitMs}. */
  CommitCleanup(FileIO io, Executor executor, long closeWaitMs) {
    this.io = io;
    this.executor = executor;
    this.closeWaitMs = closeWaitMs;
  }

  /**
   * Whether {@code failure}, which a commit threw, says its record did not take effect and never
   * will, so that nothing will ever name what was written for it: Iceberg's {@link
   * CleanableFailure}s, on which its own producers delete what they wrote ({@code
   * CommitFailedException} and the conflicts the catalog explains as {@code NoSuchTableException},
   * {@link RecordTooLargeException}, {@link CommitRefusedException}, the {@code
   * ServiceUnavailableException} of an operation each of whose attempts conflicted), and {@link
   * AlreadyExistsException}, which the catalog throws
   * only for a create or rename that lost to the name being taken. Not {@code
   * CommitStateUnknownException}, nor a failure the catalog does not classify: the record may have
   * landed, and name the files.
   */
  static boolean definitelyFailed(Throwable failure) {
    return failure instanceof CleanableFailure || failure instanceof AlreadyExistsException;
  }

  /**
   * Deletes {@code files}, written ahead of a commit that definitely failed, off the calling
   * thread (on the executor this cleanup was given). Never throws: what it cannot delete, or
   * cannot queue, it logs, and leaves for a GC by listing; after {@link #close}, it deletes
   * nothing.
   */
  void delete(Collection<String> files) {
    if (files.isEmpty()) {
      return;
    }
    List<String> batch = List.copyOf(files);
    if (closed) {
      left(batch, "the catalog is closed");
      return;
    }
    synchronized (this) {
      pending += 1;
    }
    try {
      (executor != null ? executor : SharedPool.POOL).execute(() -> run(batch));
    } catch (RuntimeException e) {
      // the shared pool shut down (the JVM exiting), or an executor that refuses work
      done();
      LOG.warn("Cannot queue the delete of {} files written for commits that did not land, left "
          + "for a GC by listing: {}", batch.size(), batch, e);
    }
  }

  private void run(List<String> batch) {
    try {
      if (closed) {
        left(batch, "the catalog closed before they were deleted");
      } else {
        deleteNow(batch);
      }
    } catch (RuntimeException e) {
      LOG.warn("Cannot delete {}, written for commits that did not land", batch, e);
    } finally {
      done();
    }
  }

  private void deleteNow(List<String> batch) {
    if (io instanceof SupportsBulkOperations) {
      try {
        ((SupportsBulkOperations) io).deleteFiles(batch);
      } catch (BulkDeletionFailureException e) {
        LOG.warn("Cannot delete {} of {}, written for commits that did not land",
            e.numberFailedObjects(), batch, e);
      }
      return;
    }
    for (String file : batch) {
      try {
        io.deleteFile(file);
      } catch (NotFoundException e) {
        // opened but never written: its write failed before it created the file
      } catch (RuntimeException e) {
        LOG.warn("Cannot delete {}, written for a commit that did not land", file, e);
      }
    }
  }

  private void left(List<String> batch, String why) {
    LOG.info("Leaving {} files written for commits that did not land, as {}, for a GC by "
        + "listing: {}", batch.size(), why, batch);
  }

  private synchronized void done() {
    pending -= 1;
    if (pending == 0) {
      notifyAll();
    }
  }

  /**
   * Waits up to {@code timeoutMs} for the deletes handed over so far, and those handed over while
   * it waits, to be done (deleted, failed and logged, or dropped). Returns whether they are.
   */
  synchronized boolean await(long timeoutMs) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    while (pending > 0) {
      long left = deadline - System.nanoTime();
      if (left <= 0) {
        return false;
      }
      TimeUnit.NANOSECONDS.timedWait(this, left);
    }
    return true;
  }

  /**
   * Waits up to {@link #CLOSE_WAIT_MS} for the deletes handed over, so they run before the
   * catalog closes its FileIO; then deletes nothing more. A delete queued and not yet started
   * drops its files, logged, for a GC by listing; one under way finishes, and may fail on a FileIO
   * the catalog closes. Never fails, and keeps the thread's interrupt.
   */
  @Override
  public void close() {
    boolean done;
    try {
      done = await(closeWaitMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      done = false;
    }
    closed = true;
    if (!done) {
      LOG.info("Closing with deletes of files written for commits that did not land still queued:"
          + " they are dropped, and their files left for a GC by listing");
    }
  }
}
