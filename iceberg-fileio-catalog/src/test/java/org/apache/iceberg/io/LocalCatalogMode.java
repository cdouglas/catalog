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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.io.AtomicOutputFile.Strategy;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.intentlog.memory.InMemorySharedLog;

/**
 * Commit policy and inline mode for the local contract-suite drivers ({@link TestLocalCatalog},
 * {@link TestLocalCatalogTransaction}). Each mode runs the full suite over an {@link
 * InMemoryAtomicFileIO}.
 *
 * <ul>
 *   <li>{@code APPEND*}: every commit after the first appends (max.append.count = 10000).
 *   <li>{@code CAS*}: every commit rewrites the file (max.append.count = 0).
 *   <li>{@code COMPACT*}: a CAS compaction after every two appends (max.append.count = 3: the
 *       record written by a CAS counts toward the limit), so compaction interleaves with the
 *       append log throughout each test.
 *   <li>{@code MEMLOG*}: a {@link LogCatalog} whose log is an {@link InMemorySharedLog} (the data
 *       stays in the store): every commit appends, and the writer whose commit takes the log to 3
 *       records past its checkpoint compacts it (a GC record, a checkpoint, a trim).
 * </ul>
 *
 * <p>The suffix selects table metadata storage: none for metadata.json pointers, {@code _TM} for
 * inline TableMetadata, {@code _ML} for inline TableMetadata and manifest lists.
 */
enum LocalCatalogMode {
  APPEND(10_000, false, false, false),
  APPEND_TM(10_000, true, false, false),
  APPEND_ML(10_000, true, true, false),
  CAS(0, false, false, false),
  CAS_TM(0, true, false, false),
  CAS_ML(0, true, true, false),
  COMPACT(3, false, false, false),
  COMPACT_TM(3, true, false, false),
  COMPACT_ML(3, true, true, false),
  MEMLOG(3, false, false, true),
  MEMLOG_TM(3, true, false, true),
  MEMLOG_ML(3, true, true, true);

  final int maxAppendCount;
  final boolean inlineTM;
  final boolean inlineML;
  final boolean sharedLog;

  LocalCatalogMode(int maxAppendCount, boolean inlineTM, boolean inlineML, boolean sharedLog) {
    this.maxAppendCount = maxAppendCount;
    this.inlineTM = inlineTM;
    this.inlineML = inlineML;
    this.sharedLog = sharedLog;
  }

  /** The conflict encodings a catalog can be created with, which fuzzers draw from. */
  static final List<String> ENCODINGS =
      List.of(
          LamportEncoding.NAME, LamportMinusPropertiesEncoding.NAME, ConcernVectorEncoding.NAME,
          BlindAppendEncoding.NAME, PartitionVectorEncoding.NAME, ManifestWindowEncoding.NAME);

  /** The modes whose catalog is one object of the store. */
  static List<LocalCatalogMode> fileModes() {
    return Arrays.stream(values()).filter(m -> !m.sharedLog).toList();
  }

  /** The modes whose catalog is a shared log. */
  static List<LocalCatalogMode> sharedLogModes() {
    return Arrays.stream(values()).filter(m -> m.sharedLog).toList();
  }

  /**
   * The key of the shared log of the catalog at a location over a store, kept beside the store
   * ({@link InMemoryAtomicFileIO#attachment}) for as long as it lives: catalogs there share one.
   */
  private static final String SHARED_LOG = "shared log ";

  /** The shared log of the catalog at {@code location} over {@code io}. */
  static InMemorySharedLog sharedLog(InMemoryAtomicFileIO io, String location) {
    return io.attachment(SHARED_LOG + location, key -> new InMemorySharedLog(location));
  }

  /** This mode's table metadata storage, on a shared log. */
  LocalCatalogMode onSharedLog() {
    return inlineML ? MEMLOG_ML : inlineTM ? MEMLOG_TM : MEMLOG;
  }

  /** A mode to read the catalog at {@code location} over {@code io} with, whichever log it has. */
  static LocalCatalogMode readerFor(InMemoryAtomicFileIO io, String location) {
    return io.hasAttachment(SHARED_LOG + location) ? MEMLOG : APPEND;
  }

  /** The shared logs of catalogs over {@code io}. */
  static List<InMemorySharedLog> sharedLogs(InMemoryAtomicFileIO io) {
    return io.attachments(SHARED_LOG, InMemorySharedLog.class);
  }

  /**
   * Builds a catalog over {@code io} rooted at {@code warehouse + "/catalog-" + name}: a {@link
   * FileIOCatalog} in the modes on one object, a {@link LogCatalog} in the shared-log modes (as
   * the caller types it). The table-default / table-override properties are the ones {@code
   * CatalogTests} expects of {@code catalog()}; {@code extra} is applied last.
   */
  @SuppressWarnings("unchecked")
  <C extends LogCatalog> C catalog(
      InMemoryAtomicFileIO io, String warehouse, String name, Map<String, String> extra) {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
    properties.put(LogCatalogProperties.MAX_APPEND_COUNT, String.valueOf(maxAppendCount));
    properties.put(LogCatalogProperties.RACE_MIN_WAIT_MS, "0"); // resend a lost race at once
    properties.put("fileio.catalog.inline", String.valueOf(inlineTM));
    properties.put("fileio.catalog.inline.manifests", String.valueOf(inlineML));
    properties.put(CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key1", "catalog-default-key1");
    properties.put(CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key2", "catalog-default-key2");
    properties.put(CatalogProperties.TABLE_DEFAULT_PREFIX + "override-key3", "catalog-default-key3");
    properties.put(CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key3", "catalog-override-key3");
    properties.put(CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key4", "catalog-override-key4");
    properties.putAll(extra);
    String location = warehouse + "/catalog-" + name;
    LogCatalog catalog = sharedLog
        ? new LogCatalog(name, sharedLog(io, location), io, Maps.newHashMap())
        : new FileIOCatalog(name, location, io, Maps.newHashMap());
    catalog.initialize(name, properties);
    return (C) catalog;
  }

  /**
   * Asserts that every catalog file in {@code io} was written under this mode's commit policy, so
   * a misspelled property cannot silently run the default policy. The first write creates the
   * file by CAS; each CAS leaves one transaction record in the log and each append adds one. A
   * later CAS (compaction) is legal only once the log holds {@code maxAppendCount} records: the
   * writer decided from the record count of the version it read, and the CAS took effect only
   * because that version was still live. The limit is soft for appends: a writer that lost an
   * append race retries without re-reading the count, so the log may run past it.
   */
  void assertCommitPolicy(InMemoryAtomicFileIO io) {
    if (sharedLog) {
      // The catalog wrote no object; its shared logs' checkpoints must be replays of them.
      assertThat(io.conditionalWrites()).as("conditional writes to the store").isEmpty();
      sharedLogs(io).forEach(CheckpointRoundTrip::assertEveryCheckpoint);
      return;
    }
    io.conditionalWrites()
        .forEach(
            (location, writes) -> {
              assertThat(writes).as("writes to %s", location).first().isEqualTo(Strategy.CAS);
              int records = 1;
              for (int i = 1; i < writes.size(); i++) {
                if (writes.get(i) == Strategy.APPEND) {
                  assertThat(maxAppendCount).as("append to %s in CAS-only mode", location).isPositive();
                  records += 1;
                } else {
                  assertThat(records)
                      .as("records before compaction #%d of %s: %s", i, location, writes)
                      .isGreaterThanOrEqualTo(maxAppendCount);
                  records = 1;
                }
              }
            });
  }
}
