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
   * Maximum size in bytes of the catalog file before a new commit must CAS (compact)
   * instead of append. This is a performance target; larger catalogs take longer to
   * read. Default: 16 MB.
   */
  public static final String MAX_APPEND_SIZE = "fileio.catalog.max.append.size";
  public static final long DEFAULT_MAX_APPEND_SIZE = 16L * 1024 * 1024;

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
    return runtimeConfig(
        intProperty(properties, MAX_APPEND_COUNT, DEFAULT_MAX_APPEND_COUNT),
        longProperty(properties, MAX_APPEND_SIZE, DEFAULT_MAX_APPEND_SIZE),
        longProperty(properties, COMMITTED_RETENTION_MS, DEFAULT_COMMITTED_RETENTION_MS),
        longProperty(properties, THROTTLE_MIN_WAIT_MS, DEFAULT_THROTTLE_MIN_WAIT_MS),
        longProperty(properties, RACE_MIN_WAIT_MS, DEFAULT_RACE_MIN_WAIT_MS));
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
        CompactionPolicy.limits(maxAppendCount, maxAppendSize),
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
