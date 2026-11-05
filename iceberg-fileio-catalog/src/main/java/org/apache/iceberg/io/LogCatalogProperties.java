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

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.intentlog.CompactionPolicy;
import org.intentlog.RuntimeConfig;

/** How a catalog over a shared log ({@link LogCatalog}) commits: its log's properties. */
public final class LogCatalogProperties {

  private LogCatalogProperties() {}

  /**
   * Maximum number of transaction records in the log portion before a new commit
   * must CAS (compact) instead of append. Default: 100, the best of 0, 100 and 1000 in the
   * contention benchmark on S3 Express (docs/design.md, "Choosing append vs CAS"): a reader
   * replays every record after the checkpoint, and a compaction races the others' appends. Set to
   * 0 to force CAS on every commit (for storage providers that don't support conditional append).
   *
   * <p>Provider hard limits: S3 Express ~10000, Azure AppendBlob 50000.
   */
  public static final String MAX_APPEND_COUNT = "fileio.catalog.max.append.count";
  public static final int DEFAULT_MAX_APPEND_COUNT = 100;

  /**
   * Maximum size in bytes of the records after the catalog's checkpoint (or, with {@link
   * #MAX_APPEND_SIZE_BASIS} {@code object}, of the whole catalog file) before a new commit must
   * CAS (compact) instead of append. This is a performance target: a reader replays the records,
   * and a compaction rewrites them. Default: 16 MB.
   */
  public static final String MAX_APPEND_SIZE = "fileio.catalog.max.append.size";
  public static final long DEFAULT_MAX_APPEND_SIZE = 16L * 1024 * 1024;

  /**
   * What {@link #MAX_APPEND_SIZE} bounds: {@code tail}, the records after the checkpoint, or
   * {@code object}, the whole catalog file, as before errata D19's stopgap, under which a
   * checkpoint past the bound makes every commit compact (docs/CHECKPOINT_PLAN.md R0.1; for
   * ablation). Default: {@code tail}.
   */
  public static final String MAX_APPEND_SIZE_BASIS = "fileio.catalog.max.append.size.basis";
  public static final String DEFAULT_MAX_APPEND_SIZE_BASIS = "tail";

  /**
   * Whether a checkpoint writes an inline table that has not changed since a decode or an
   * earlier encode of this catalog as the bytes it had then, instead of encoding it again
   * (docs/CHECKPOINT_PLAN.md R0.2). {@code false} encodes every table on every checkpoint, as
   * before (for ablation). Default: {@code true}.
   */
  public static final String CHECKPOINT_MEMOIZE = "fileio.catalog.checkpoint.memoize";
  public static final boolean DEFAULT_CHECKPOINT_MEMOIZE = true;

  /** Whether {@code properties} ask a catalog to memoize its checkpoints' table encodings. */
  public static boolean checkpointMemoize(Map<String, String> properties) {
    String val = properties == null ? null : properties.get(CHECKPOINT_MEMOIZE);
    if (val == null) {
      return DEFAULT_CHECKPOINT_MEMOIZE;
    }
    Preconditions.checkArgument(val.equals("true") || val.equals("false"),
        "%s must be true or false, got %s", CHECKPOINT_MEMOIZE, val);
    return Boolean.parseBoolean(val);
  }

  /** The size basis {@code properties} name ({@link #MAX_APPEND_SIZE_BASIS}). */
  static CompactionPolicy.SizeBasis sizeBasis(Map<String, String> properties) {
    String val = properties == null ? null : properties.get(MAX_APPEND_SIZE_BASIS);
    String basis = val != null ? val : DEFAULT_MAX_APPEND_SIZE_BASIS;
    switch (basis) {
      case "tail":
        return CompactionPolicy.SizeBasis.TAIL;
      case "object":
        return CompactionPolicy.SizeBasis.OBJECT;
      default:
        throw new IllegalArgumentException(
            String.format("%s must be tail or object, got %s", MAX_APPEND_SIZE_BASIS, basis));
    }
  }

  /**
   * Retention window for the committed-transaction dedup set, in milliseconds.
   * Every CAS / compaction commit writes a GC record (AdvanceHorizon) after its
   * checkpoint, which drops any UUIDv7 from {@code CommittedTransactions} whose
   * timestamp is at or below {@code now - committedRetentionMs} (and below its
   * own transaction's). The largest dropped timestamp becomes
   * {@code highest_dropped_timestamp_ms}: a
   * query for a UUID at or below it throws {@link org.intentlog.DedupHorizonExceededException},
   * and replay rejects a record whose id is at or below it (Bug 64). Default: 30
   * minutes, far longer than a commit's retries (it was 6 hours until 2026-10-28; errata
   * D16), short enough to bound the set on long-lived catalogs. {@code Long.MAX_VALUE} disables GC. A window
   * shorter than a commit takes is legal but lossy: a writer whose id falls
   * below the horizon while it commits fails, or cannot learn whether it
   * committed (CommitStateUnknownException); {@code 0} drops everything older
   * than the compacting commit. See {@code docs/LOG.md} "The committed set and GC".
   */
  public static final String COMMITTED_RETENTION_MS = "fileio.catalog.committed.retention.ms";
  public static final long DEFAULT_COMMITTED_RETENTION_MS = 30L * 60 * 1000;

  /**
   * The committed set's admission window A, in milliseconds: a compaction pages out (into objects
   * beside the catalog's, {@code <catalog>.committed/}) only the blocks of committed ids older than
   * this, and replay admits no record whose id is at or below the floor that paging raises. A
   * commit slower than A is rejected and retried with a new id ({@code CommitFailedException});
   * a writer still learns exactly whether a commit landed for the whole retention, from the pages.
   * Paging needs a retention above it. Default: 5 minutes (provisional). See {@code
   * docs/COMMITTED_PAGING.md}.
   */
  public static final String COMMITTED_ADMISSION_MS = "fileio.catalog.committed.admission.ms";
  public static final long DEFAULT_COMMITTED_ADMISSION_MS =
      RuntimeConfig.DEFAULT_ADMISSION_WINDOW_MS;

  /**
   * The committed set's page threshold T, in bytes: a compaction pages out the oldest blocks while
   * the ids kept in the checkpoint take more. Default: 1 MiB. {@code Long.MAX_VALUE}: never page.
   */
  public static final String COMMITTED_PAGE_THRESHOLD = "fileio.catalog.committed.page.threshold";
  public static final long DEFAULT_COMMITTED_PAGE_THRESHOLD =
      RuntimeConfig.DEFAULT_PAGE_THRESHOLD_BYTES;

  /**
   * First wait before retrying a write that storage throttled; each later wait doubles, up to 5 s.
   */
  public static final String THROTTLE_MIN_WAIT_MS = "fileio.catalog.throttle.retry.min-wait-ms";
  public static final long DEFAULT_THROTTLE_MIN_WAIT_MS = 200L;

  /**
   * The bound of the first wait after a commit loses a race for the catalog (another writer's
   * append or replace landed first), before it reads the catalog again and resends; each later
   * lost race of the commit doubles it, up to 1 s, and the wait is drawn below it at random, so
   * that the writers who lost spread out instead of racing again at once. 0: resend at once.
   */
  public static final String RACE_MIN_WAIT_MS = "fileio.catalog.race.retry.min-wait-ms";
  public static final long DEFAULT_RACE_MIN_WAIT_MS = 100L;

  /**
   * The conflict encoding of a catalog this one creates, by name: how replay decides whether a
   * table update conflicts with what landed since its writer read the table ({@link
   * ConflictEncoding}). The catalog's first record sets it, and every reader judges by it; a
   * catalog that opens an existing log uses that log's, whatever this says. Default: {@code
   * manifest-window}, which merges the most (docs/design.md, "The same table, per conflict
   * encoding"). {@code lamport} applies an update only on the version of the table it read; {@code
   * lamport-minus-properties} lets an update that does not touch properties apply on a table whose
   * properties alone changed; {@code concern-vector} versions a table by its concerns and refs;
   * {@code blind-append} also lets appends that validate nothing land on one branch concurrently;
   * {@code partition-vector} and {@code manifest-window} also let a commit that says what its
   * retry checks (an overwrite, a row delta, a rewrite, a delete) land on a moved branch where
   * those checks hold, judged by partition buckets or by the manifests written since
   * (docs/SPEC.md, "Conflict Encodings"). Every one commits only what Iceberg's retry would.
   */
  public static final String CONFLICT_ENCODING = "fileio.catalog.conflict.encoding";
  public static final String DEFAULT_CONFLICT_ENCODING = ManifestWindowEncoding.NAME;

  /**
   * The conflict encoding {@code properties} name for a catalog to create.
   *
   * @throws IllegalArgumentException if this build does not know it
   */
  public static String conflictEncoding(Map<String, String> properties) {
    String name =
        properties == null
            ? DEFAULT_CONFLICT_ENCODING
            : properties.getOrDefault(CONFLICT_ENCODING, DEFAULT_CONFLICT_ENCODING);
    Preconditions.checkArgument(
        ConflictEncodings.known(name),
        "Unknown %s: %s (known: %s)", CONFLICT_ENCODING, name, ConflictEncodings.names());
    return name;
  }

  /** The runtime configuration {@code properties} set; the defaults for those they do not. */
  public static RuntimeConfig runtimeConfig(Map<String, String> properties) {
    long admissionMs =
        longProperty(properties, COMMITTED_ADMISSION_MS, DEFAULT_COMMITTED_ADMISSION_MS);
    long threshold =
        longProperty(properties, COMMITTED_PAGE_THRESHOLD, DEFAULT_COMMITTED_PAGE_THRESHOLD);
    Preconditions.checkArgument(admissionMs >= 0,
        "%s must be >= 0, got %s", COMMITTED_ADMISSION_MS, admissionMs);
    Preconditions.checkArgument(threshold >= 0,
        "%s must be >= 0, got %s", COMMITTED_PAGE_THRESHOLD, threshold);
    return runtimeConfig(
        intProperty(properties, MAX_APPEND_COUNT, DEFAULT_MAX_APPEND_COUNT),
        longProperty(properties, MAX_APPEND_SIZE, DEFAULT_MAX_APPEND_SIZE),
        sizeBasis(properties),
        longProperty(properties, COMMITTED_RETENTION_MS, DEFAULT_COMMITTED_RETENTION_MS),
        longProperty(properties, THROTTLE_MIN_WAIT_MS, DEFAULT_THROTTLE_MIN_WAIT_MS),
        longProperty(properties, RACE_MIN_WAIT_MS, DEFAULT_RACE_MIN_WAIT_MS))
        .withPaging(admissionMs, threshold);
  }

  /**
   * The runtime configuration of a catalog that compacts at {@code maxAppendCount} records or
   * {@code maxAppendSize} bytes, keeps committed ids for {@code committedRetentionMs}, first
   * waits {@code throttleMinWaitMs} after a throttled write, and up to {@code raceMinWaitMs} after
   * a lost race.
   */
  public static RuntimeConfig runtimeConfig(
      int maxAppendCount, long maxAppendSize, long committedRetentionMs, long throttleMinWaitMs,
      long raceMinWaitMs) {
    return runtimeConfig(maxAppendCount, maxAppendSize, CompactionPolicy.SizeBasis.TAIL,
        committedRetentionMs, throttleMinWaitMs, raceMinWaitMs);
  }

  /**
   * As {@link #runtimeConfig(int, long, long, long, long)}, with {@code maxAppendSize} bounding
   * what {@code sizeBasis} counts.
   */
  public static RuntimeConfig runtimeConfig(
      int maxAppendCount, long maxAppendSize, CompactionPolicy.SizeBasis sizeBasis,
      long committedRetentionMs, long throttleMinWaitMs, long raceMinWaitMs) {
    Preconditions.checkArgument(maxAppendCount >= 0,
        "maxAppendCount must be >= 0, got %s", maxAppendCount);
    Preconditions.checkArgument(maxAppendSize >= 0,
        "maxAppendSize must be >= 0, got %s", maxAppendSize);
    Preconditions.checkArgument(committedRetentionMs >= 0,
        "committedRetentionMs must be >= 0, got %s", committedRetentionMs);
    Preconditions.checkArgument(throttleMinWaitMs >= 0,
        "throttleMinWaitMs must be >= 0, got %s", throttleMinWaitMs);
    Preconditions.checkArgument(raceMinWaitMs >= 0,
        "raceMinWaitMs must be >= 0, got %s", raceMinWaitMs);
    return RuntimeConfig.of(
        CompactionPolicy.limits(maxAppendCount, maxAppendSize, sizeBasis),
        committedRetentionMs,
        throttleMinWaitMs,
        raceMinWaitMs);
  }

  private static int intProperty(Map<String, String> properties, String key, int dflt) {
    String val = properties == null ? null : properties.get(key);
    return val != null ? Integer.parseInt(val) : dflt;
  }

  private static long longProperty(Map<String, String> properties, String key, long dflt) {
    String val = properties == null ? null : properties.get(key);
    return val != null ? Long.parseLong(val) : dflt;
  }
}
