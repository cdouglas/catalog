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

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.RandomDistribution;
import net.jqwik.api.Tuple;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.statistics.Statistics;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.CatalogTransaction;
import org.apache.iceberg.catalog.CatalogTransaction.IsolationLevel;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.InMemoryAtomicFileIO.Fault;
import org.apache.iceberg.io.InMemoryAtomicFileIO.ReadFault;
import org.apache.iceberg.types.Types;
import org.intentlog.Replica;
import org.intentlog.fileio.SegmentedLogHistory;
import org.intentlog.azure.InMemoryAppendBlobStore;
import org.intentlog.fileio.AtomicFileLog;
import org.intentlog.memory.InMemorySharedLog;
import org.intentlog.wire.LogEnvelope;

/**
 * Multi-writer fuzzer for {@link FileIOCatalog} (or a {@link LogCatalog} on a shared log, which a
 * run draws) on faulty storage, through Iceberg's table and catalog APIs: the tier above {@code
 * TestMultiWriterProperties}, which drives the catalog format directly.
 *
 * <p>A generated schedule runs, on one thread, operations by several writers, each with its own
 * catalog instance and table handles over shared {@link InMemoryAtomicFileIO} storage: fast and
 * merge appends, deletes, row-level deletes (equality, position, deletion vectors), property,
 * schema and partition-spec changes, table transactions, snapshot expiry (of everything older than
 * now or than one of the table's snapshots, of one snapshot by id, or of what the retention of the
 * table and its refs alone expires) and those retention settings, manifest rewrites, table
 * create / drop / rename (across namespaces too) / replace, upgrades from format v2 to v3, refs
 * moved and snapshots expired behind the sealing line of snapshot paging (rollbacks, tags of old
 * snapshots, expiries of sealed ones), sweeps of the log's orphaned pages (from a fresh read, or
 * from a writer's replica however stale),
 * namespace create / drop and property changes, and multi-table catalog transactions whose
 * steps (loads, appends, scans of a branch or tag, declared reads, table drops with purge or not,
 * renames, creates and property changes, namespace creates, drops and property changes, view
 * creates, replaces, property changes, renames and drops) and commit are separate commands, so
 * other writers' operations run in between. A
 * command may also preempt the one before it: it runs to completion just before one of that
 * operation's reads of the catalog or its first conditional write, so the two race, or at a
 * request of any kind after that write, which reaches what an operation does once its record
 * landed (a compaction after the commit, an expiry's cleanup, a purge): a preempted operation
 * whose effect the catalog then holds is settled as acknowledged first, and must not report a
 * failure after (the review's A7). Each writer
 * has its own catalog mode (append / CAS / compaction, pointer / inline TM / inline ML), or all
 * writers share one, and binds the names it creates by name or by the namespace's version, as
 * drawn ({@code fileio.catalog.create.condition}). A small limit on inline updates moves tables
 * out of inline storage and back:
 * a big property evicts a table to a metadata file, and a later update that fits in full returns
 * it. On Append Blobs at the larger limits, the segments' blocks are small instead, so a big
 * property's record is over the log's bound, which evicts it (SPEC_TM.md "Size Budget").
 *
 * <p>In {@link #faultyStorageNeverMisreportsACommit}, each operation's storage also fails as its
 * command says: its first two conditional writes (see {@link InMemoryAtomicFileIO.Fault}) and one
 * of its reads (see {@link InMemoryAtomicFileIO.ReadFault}; on a shared log, a read of a page
 * fails or finds no page, as if the store lost it, which must fail the operation, never read as an
 * empty block), and its plain requests: a page put or another put, a delete or a listing fails,
 * before it lands or after with its reply lost ({@link Cmd#plainPlans}). Reads of {@code
 * metadata.json} do not
 * fail, as Iceberg retries them after 100 ms; nor do reads on Iceberg's worker threads, which
 * would make the schedule nondeterministic. A preempting operation runs without faults. Storage may
 * also be strict about versions, as it is in practice: a read pinned to a replaced version fails
 * (see {@link InMemoryAtomicFileIO#failSupersededReads}).
 *
 * <p>Tables are created in format v2 or v3. A model holds the namespaces and their
 * properties, and each table's UUID, format version, live data and delete files, properties and
 * added columns. Data and delete files are one-byte objects, written as an engine writes them, and
 * every file a snapshot of a table reaches through its manifests must exist: a cleanup or a purge
 * deletes only what nothing reaches (A7). After every operation the catalog, read back without
 * faults, must match it:
 *
 * <ul>
 *   <li>An acknowledged operation took effect once, and nothing else changed.
 *   <li>An operation that failed any way but {@link CommitStateUnknownException} did not take
 *       effect. Iceberg deletes a commit's new files on a {@code CleanableFailure} such as {@link
 *       CommitFailedException}, so a commit reported that way that took effect loses them.
 *   <li>An operation whose outcome is unknown took effect entirely or not at all, and a fault was
 *       injected into it, unless a writer compacts with a retention of 0 (which can put a commit
 *       in flight at or below the committed set's horizon).
 *   <li>History (the 2026-11-17 review's A2): a table's snapshots leave it only across an
 *       acknowledged or unknown expiry, and never come back; an acknowledged expiry that nothing
 *       raced and no fault touched removes exactly the snapshots it chose; an operation rejected or
 *       failed, that nothing preempted, leaves every table's metadata as it was (its JSON, less
 *       the metadata log), whatever the model holds of it; a re-inlining changes no table's
 *       metadata but its manifest lists' locations, its location and its metadata log; and a tag
 *       or branch names the snapshot its operation read.
 *   <li>Every table is in a namespace; every table loads, and reads the same through a reader that
 *       has read nothing before; every snapshot has its manifest list, inline or in a file, and
 *       every manifest exists; a snapshot's manifest list never changes; no table is stored both
 *       inline and in a metadata file.
 *   <li>Numbering, from v2: a snapshot's sequence number is above its parent's, at most the
 *       table's last and no other snapshot's; its manifests' are at most its own, and those it
 *       wrote have its own. In v3, snapshots' row-id ranges are apart and below the next row id.
 *   <li>Every version of the catalog file that a writer wrote reads the same as a checkpoint of
 *       it, which the next compaction would write (see {@link CheckpointRoundTrip}); and every page
 *       the newest version references exists, as its reference says: the committed set's, and the
 *       sealed snapshot blocks' its root names. A sweep runs too at the first conditional write
 *       after a command put a page, before the root or record that names it lands.
 *   <li>The drain, after the schedule, on storage that no longer fails: a compaction lands (its
 *       root's pages put first, however earlier puts of them failed), a sweep from a fresh read
 *       leaves every page the newest root names, and with every table dropped, a compaction and a
 *       sweep leave no snapshot page in the log's page store.
 *   <li>Row lineage, in a v3 table: once a snapshot has row ids, every live data file has them; the
 *       files' ranges are disjoint and below the table's next row id; a file's first row id never
 *       changes along main's lineage, nor do a snapshot's first row id and added rows.
 *   <li>A catalog transaction's changes take effect all together at its commit, or none of them
 *       (the 2026-11-17 review's A6). An acknowledged one saw no concurrent change it must not
 *       miss: a table it changed was not replaced since it first loaded it (errata D12); and,
 *       under serializable isolation, every branch or tag it scanned names the snapshot it read,
 *       and a table whose read it declared is at the version it read (a load is not a read). A
 *       table it, or any drop, removed keeping its files can be registered again: a metadata file
 *       under its location holds its last version. A view it changed was at the version it first
 *       loaded (VIEWS.md V6), and a view it dropped keeping its metadata can be registered again.
 *   <li>Progress (the 2026-11-17 review's A5): an operation whose model effect applies, and no
 *       fault was injected into, is not rejected as a conflict ({@link CommitFailedException}, or
 *       {@code ServiceUnavailableException} once a catalog operation's attempts ran out) when
 *       nothing raced it: no command preempted it, and its table handle, if it uses one, was
 *       loaded fresh. Nor when its operation refreshes and applies itself again on a conflict
 *       (Iceberg's snapshot producers, property updates, expiries and simple transactions; the
 *       catalog's drops, renames and namespace operations), raced or stale: a command preempts
 *       once, so its attempts after the race are unraced. Nor is a catalog
 *       transaction, unpreempted, when no other writer changed a table it loaded since it loaded
 *       it; nor, before it sends its record, one of a writer that inlines that only appends to
 *       tables inline then and now, whose updates the conflict encoding judges where the record
 *       lands (errata D12), not the client.
 * </ul>
 */
class TestCatalogFaultProperties {

  private static final Namespace NS = Namespace.of("ns");
  private static final Namespace NS2 = Namespace.of("ns2");
  // A small universe, so writers collide often.
  private static final List<Namespace> NAMESPACES = List.of(NS, NS2);
  private static final List<TableIdentifier> TABLES =
      List.of(TableIdentifier.of(NS, "t0"), TableIdentifier.of(NS, "t1"), TableIdentifier.of(NS, "t2"),
          TableIdentifier.of(NS2, "t0"));
  // The names views take: the tables', which they collide with, and two of their own
  private static final List<TableIdentifier> VIEW_NAMES = java.util.stream.Stream.concat(
      TABLES.stream(), java.util.stream.Stream.of(
          TableIdentifier.of(NS, "v0"), TableIdentifier.of(NS, "v1"))).toList();
  private static final Schema SCHEMA = new Schema(required(1, "id", Types.IntegerType.get()));
  private static final String KEY_PREFIX = "fz.";
  private static final List<String> KEYS = List.of("fz.k1", "fz.k2");
  private static final String BIG_KEY = "fz.big";
  private static final String WAREHOUSE = "mem:///warehouse";
  private static final String CATALOG_NAME = "fuzz";
  private static final String CATALOG_LOCATION = WAREHOUSE + "/catalog-" + CATALOG_NAME;
  /**
   * K, a segment's record limit on Append Blobs where no block limit is drawn: a few of the
   * fuzzer's records (a run's are 10 to 60), so that the log moves on to a new segment often.
   */
  private static final int AZURE_SEGMENT_RECORDS = 4;
  /**
   * The share of tries, in percent, in which some table must seal a snapshot block when every
   * catalog pages at blocks of 2 ({@code -Dfileio.catalog.test.snapshot.block=2}, as a round's
   * faultA2 and faultB2 do): 19% of catalogMatchesModel's tries and 13% of
   * faultyStorageNeverMisreportsACommit's seal one at a window of 1 (300 tries each, 2026-11-19),
   * so a change that stops most of them sealing fails the job; no floor at other paging.
   */
  private static final double SEALING_FLOOR =
      Integer.valueOf(2).equals(LocalCatalogMode.SNAPSHOT_BLOCK) ? 10 : 0;
  /**
   * The segments' blocks on Append Blobs at an inline limit of 8 KiB or more: half the smallest of
   * the big properties of those limits (4, 9.6 and 24 KiB), so a record that holds one is over the
   * log's bound, and its update goes to a metadata file by the bound where the limit keeps it
   * inline.
   */
  private static final int AZURE_BLOCK_BYTES = 2048;

  /**
   * A command's preemption point at or past this is late: the request of the command it preempts
   * {@code preempt - LATE} after that command's first conditional write (the review's A7).
   */
  private static final int LATE = 100;

  // Retry commits, throttled writes and lost races without sleeping; merge manifests early, so
  // merge appends read them; keep two previous metadata files, so pointer-mode commits delete
  // older ones.
  private static final Map<String, String> PROPERTIES =
      Map.of(
          LogCatalogProperties.THROTTLE_MIN_WAIT_MS, "0",
          LogCatalogProperties.RACE_MIN_WAIT_MS, "0",
          CatalogProperties.TABLE_DEFAULT_PREFIX + TableProperties.COMMIT_MIN_RETRY_WAIT_MS, "1",
          CatalogProperties.TABLE_DEFAULT_PREFIX + TableProperties.COMMIT_MAX_RETRY_WAIT_MS, "1",
          CatalogProperties.TABLE_DEFAULT_PREFIX + TableProperties.MANIFEST_MIN_MERGE_COUNT, "2",
          CatalogProperties.TABLE_DEFAULT_PREFIX
              + TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED,
          "true",
          CatalogProperties.TABLE_DEFAULT_PREFIX + TableProperties.METADATA_PREVIOUS_VERSIONS_MAX,
          "2");

  // A command's writeFaults operand picks the faults for its first two conditional writes, by its
  // remainder and quotient mod 12; later writes succeed.
  private static final List<Fault> FIRST_WRITE_FAULTS =
      List.of(
          Fault.NONE, Fault.NONE, Fault.NONE, Fault.NONE, Fault.NONE, Fault.NONE,
          Fault.FAIL_BEFORE,
          Fault.LOST_REPLY, Fault.LOST_REPLY_RUNTIME,
          Fault.LANDED_AS_CONFLICT,
          Fault.THROTTLED, Fault.LANDED_THROTTLED);
  private static final List<Fault> SECOND_WRITE_FAULTS =
      List.of(
          Fault.NONE, Fault.NONE, Fault.NONE, Fault.NONE,
          Fault.NONE, Fault.NONE, Fault.NONE,
          Fault.FAIL_BEFORE, Fault.LOST_REPLY, Fault.LOST_REPLY_RUNTIME, Fault.LANDED_AS_CONFLICT,
          Fault.LANDED_THROTTLED);

  @Property(tries = 100)
  void catalogMatchesModel(
      @ForAll("logs") LocalCatalogMode.Log log,
      @ForAll("modes") List<LocalCatalogMode> modes,
      @ForAll @IntRange(min = 2, max = 3) int writers,
      @ForAll("maxUpdateSizes") int maxUpdateSize,
      @ForAll boolean strictVersions,
      @ForAll("readModes") InMemoryAtomicFileIO.ReadMode reads,
      @ForAll boolean recordingReads,
      @ForAll @IntRange(min = 0, max = 7) int gzipWriters,
      @ForAll @IntRange(min = 0, max = 7) int gcWriters,
      @ForAll @IntRange(min = 0, max = 7) int pagingWriters,
      @ForAll @IntRange(min = 0, max = 7) int wholeReaders,
      @ForAll @IntRange(min = 0, max = 7) int fusedWriters,
      @ForAll @IntRange(min = 0, max = 7) int namespaceWriters,
      @ForAll("clockOffsets") List<Long> clockOffsets,
      @ForAll("segmentLimits") int segmentLimit,
      @ForAll("encodings") String encoding,
      @ForAll("schedules") List<Cmd> schedule) {
    new Run(onLog(log, modes.subList(0, writers)), maxUpdateSize, false, strictVersions,
        reads, recordingReads, gzipWriters, gcWriters, pagingWriters, wholeReaders, fusedWriters,
        namespaceWriters, clockOffsets,
        segmentLimit, encoding)
        .execute(schedule);
  }

  @Property(tries = 100)
  void faultyStorageNeverMisreportsACommit(
      @ForAll("logs") LocalCatalogMode.Log log,
      @ForAll("modes") List<LocalCatalogMode> modes,
      @ForAll @IntRange(min = 2, max = 3) int writers,
      @ForAll("maxUpdateSizes") int maxUpdateSize,
      @ForAll boolean strictVersions,
      @ForAll("readModes") InMemoryAtomicFileIO.ReadMode reads,
      @ForAll boolean recordingReads,
      @ForAll @IntRange(min = 0, max = 7) int gzipWriters,
      @ForAll @IntRange(min = 0, max = 7) int gcWriters,
      @ForAll @IntRange(min = 0, max = 7) int pagingWriters,
      @ForAll @IntRange(min = 0, max = 7) int wholeReaders,
      @ForAll @IntRange(min = 0, max = 7) int fusedWriters,
      @ForAll @IntRange(min = 0, max = 7) int namespaceWriters,
      @ForAll("clockOffsets") List<Long> clockOffsets,
      @ForAll("segmentLimits") int segmentLimit,
      @ForAll("encodings") String encoding,
      @ForAll("schedules") List<Cmd> schedule) {
    new Run(onLog(log, modes.subList(0, writers)), maxUpdateSize, true, strictVersions,
        reads, recordingReads, gzipWriters, gcWriters, pagingWriters, wholeReaders, fusedWriters,
        namespaceWriters, clockOffsets,
        segmentLimit, encoding)
        .execute(schedule);
  }

  /**
   * How the store reads (TAIL_READS.md, errata T6): binding at the open, or pinned after a lookup,
   * a read of a superseded version failing as stale. Not live or spliced: this fuzzer runs a
   * preempting operation first in its model, which holds only while every request after the
   * preemption sees its effects, and a read served from the version before it (live bytes cut to
   * the length the lookup found are that version's when a write appended) is a legal prefix (P1)
   * that orders the preempted operation first. The other fuzzers draw those modes.
   */
  @Provide
  Arbitrary<InMemoryAtomicFileIO.ReadMode> readModes() {
    return Arbitraries.of(InMemoryAtomicFileIO.ReadMode.OPEN, InMemoryAtomicFileIO.ReadMode.PINNED);
  }

  /**
   * The writers' modes on one catalog object, or on a shared log or Append Blobs with the same
   * table metadata storage: writers of one catalog share its log.
   */
  private static List<LocalCatalogMode> onLog(LocalCatalogMode.Log log,
      List<LocalCatalogMode> modes) {
    return modes.stream().map(m -> m.on(log)).toList();
  }

  /**
   * The catalog's log: one object half the time, which most of the other draws are about (read
   * modes, segment limits, fused writers); a shared log in memory or Append Blobs in memory
   * (docs/AZURE_LOG.md) a quarter each.
   */
  @Provide
  Arbitrary<LocalCatalogMode.Log> logs() {
    return Arbitraries.frequency(
        Tuple.of(2, LocalCatalogMode.Log.OBJECT),
        Tuple.of(1, LocalCatalogMode.Log.SHARED),
        Tuple.of(1, LocalCatalogMode.Log.AZURE));
  }

  @Provide
  Arbitrary<String> encodings() {
    return Arbitraries.of(LocalCatalogMode.ENCODINGS);
  }

  /** Each writer's mode: one for all, a quarter of the time, else each its own. */
  @Provide
  Arbitrary<List<LocalCatalogMode>> modes() {
    Arbitrary<LocalCatalogMode> mode = Arbitraries.of(LocalCatalogMode.fileModes());
    return Arbitraries.frequencyOf(
        Tuple.of(1, mode.map(m -> List.of(m, m, m))),
        Tuple.of(3, mode.list().ofSize(3)));
  }

  /**
   * How far each writer's clock is off, in ms: mostly not at all; a few ms or a few hundred behind
   * or ahead, so its ids land in the blocks of others' older ids, or below a floor others raised;
   * or far ahead of every clock (errata D21). A writer mints after the floor it read whatever its
   * clock says (docs/COMMITTED_PAGING.md, "Testing").
   */
  @Provide
  Arbitrary<List<Long>> clockOffsets() {
    return Arbitraries.of(0L, 0L, 0L, 0L, -3L, -300L, 3L, 300L, 100_000L).list().ofSize(3);
  }

  /**
   * The most writes the catalog object holds, as S3 Express and Azure Append Blobs cap them (0: no
   * cap). A small cap makes appends hit it before the append count calls for a compaction.
   */
  @Provide
  Arbitrary<Integer> segmentLimits() {
    return Arbitraries.of(0, 0, 3, 7);
  }

  /**
   * The largest inline table update. Small limits move tables in and out of inline storage: a
   * big property evicts a table, and removing it lets the table back in.
   */
  @Provide
  Arbitrary<Integer> maxUpdateSizes() {
    return Arbitraries.of(768, 2048, 8192, InlineDeltaCodec.APPEND_LIMIT);
  }

  @Provide
  Arbitrary<List<Cmd>> schedules() {
    Arbitrary<Kind> kinds =
        Arbitraries.frequency(
            Tuple.of(2, Kind.CREATE_TABLE),
            Tuple.of(1, Kind.DROP_TABLE),
            Tuple.of(1, Kind.RENAME_TABLE),
            Tuple.of(6, Kind.FAST_APPEND),
            Tuple.of(2, Kind.MERGE_APPEND),
            Tuple.of(2, Kind.DELETE_FILE),
            Tuple.of(2, Kind.SET_PROPERTY),
            Tuple.of(1, Kind.SET_BIG_PROPERTY),
            Tuple.of(1, Kind.REMOVE_PROPERTY),
            Tuple.of(2, Kind.ADD_COLUMN),
            Tuple.of(1, Kind.UPGRADE_TABLE),
            Tuple.of(1, Kind.REPLACE_TABLE),
            Tuple.of(2, Kind.ROW_DELTA),
            Tuple.of(1, Kind.ADD_PARTITION_FIELD),
            Tuple.of(1, Kind.REMOVE_PARTITION_FIELD),
            Tuple.of(1, Kind.CREATE_TAG),
            Tuple.of(1, Kind.CREATE_BRANCH),
            Tuple.of(2, Kind.BRANCH_APPEND),
            Tuple.of(1, Kind.FAST_FORWARD),
            Tuple.of(1, Kind.ROLLBACK),
            Tuple.of(1, Kind.TAG_OLD),
            Tuple.of(1, Kind.REMOVE_REF),
            Tuple.of(1, Kind.STAGE_APPEND),
            Tuple.of(1, Kind.CHERRY_PICK),
            Tuple.of(2, Kind.TABLE_TXN),
            Tuple.of(2, Kind.EXPIRE_SNAPSHOTS),
            Tuple.of(2, Kind.EXPIRE_OLD),
            Tuple.of(1, Kind.SET_RETENTION),
            Tuple.of(1, Kind.REWRITE_MANIFESTS),
            Tuple.of(1, Kind.REINLINE),
            Tuple.of(2, Kind.SWEEP),
            Tuple.of(1, Kind.CREATE_NAMESPACE),
            Tuple.of(1, Kind.DROP_NAMESPACE),
            Tuple.of(2, Kind.SET_NAMESPACE_PROPERTY),
            Tuple.of(1, Kind.REMOVE_NAMESPACE_PROPERTY),
            Tuple.of(2, Kind.CREATE_VIEW),
            Tuple.of(1, Kind.REPLACE_VIEW),
            Tuple.of(1, Kind.VIEW_PROPERTIES),
            Tuple.of(1, Kind.RENAME_VIEW),
            Tuple.of(1, Kind.DROP_VIEW),
            Tuple.of(1, Kind.REGISTER_VIEW),
            Tuple.of(3, Kind.TXN_BEGIN),
            Tuple.of(4, Kind.TXN_APPEND),
            Tuple.of(2, Kind.TXN_READ),
            Tuple.of(1, Kind.TXN_DROP),
            Tuple.of(1, Kind.TXN_RENAME),
            Tuple.of(1, Kind.TXN_CREATE),
            Tuple.of(1, Kind.TXN_SET_PROPERTY),
            Tuple.of(1, Kind.TXN_CREATE_NAMESPACE),
            Tuple.of(1, Kind.TXN_DROP_NAMESPACE),
            Tuple.of(1, Kind.TXN_SET_NAMESPACE_PROPERTY),
            Tuple.of(1, Kind.TXN_REMOVE_NAMESPACE_PROPERTY),
            Tuple.of(1, Kind.TXN_CREATE_VIEW),
            Tuple.of(1, Kind.TXN_REPLACE_VIEW),
            Tuple.of(1, Kind.TXN_VIEW_PROPERTIES),
            Tuple.of(1, Kind.TXN_RENAME_VIEW),
            Tuple.of(1, Kind.TXN_DROP_VIEW),
            Tuple.of(3, Kind.TXN_COMMIT));
    Arbitrary<Integer> small = Arbitraries.integers().between(0, 11);
    // A preemption point: a request of the command preempted until its first conditional write
    // (0-4), or at or after it (LATE + k: its k-th request after that write, which reaches its
    // follow-up work once its record landed: a compaction after it, an expiry's cleanup)
    Arbitrary<Integer> preempt =
        Arbitraries.frequency(
            Tuple.of(12, -1),
            Tuple.of(2, 0),
            Tuple.of(2, 1),
            Tuple.of(2, 2),
            Tuple.of(1, 3),
            Tuple.of(1, 4),
            Tuple.of(1, LATE),
            Tuple.of(1, LATE + 1),
            Tuple.of(1, LATE + 3));
    // b and c together, and the read and plain faults: a combinator takes at most eight
    // arbitraries
    Arbitrary<int[]> operands = Combinators.combine(small, small).as((b, c) -> new int[] {b, c});
    Arbitrary<int[]> readAndPlain = Combinators.combine(
            small, Arbitraries.integers().between(0, 63), Arbitraries.integers().between(0, 7))
        .as((read, plain, page) -> new int[] {read, plain, page});
    return Combinators.combine(
            kinds,
            Arbitraries.integers().between(0, 2),
            small,
            operands,
            Arbitraries.of(true, false),
            preempt,
            Arbitraries.integers().between(0, 143),
            readAndPlain)
        .as((kind, writer, a, bc, fresh, pre, writeFaults, rp) ->
            new Cmd(kind, writer, a, bc[0], bc[1], fresh, pre, writeFaults, rp[0], rp[1], rp[2]))
        .list()
        .ofMinSize(1)
        .ofMaxSize(40)
        .withSizeDistribution(RandomDistribution.uniform());
  }

  /**
   * The kinds whose Iceberg operation, on a {@link CommitFailedException}, refreshes the table and
   * applies itself again (SnapshotProducer, PropertiesUpdate, RemoveSnapshots, a simple
   * BaseTransaction), and the catalog operations that stage themselves again on a fresh read
   * (drops, renames, namespace operations; Bug 107): a race before its first write, or a stale
   * handle, costs such an operation an attempt, never its commit. Not schema, partition-spec and
   * ref changes, which Iceberg commits once on the version they read, nor replace transactions,
   * which fail on a table changed since they began (Iceberg issue C23).
   */
  private static boolean retries(Cmd cmd) {
    switch (cmd.kind) {
      case FAST_APPEND:
      case MERGE_APPEND:
      case DELETE_FILE:
      case ROW_DELTA:
      case SET_PROPERTY:
      case SET_BIG_PROPERTY:
      case REMOVE_PROPERTY:
      case UPGRADE_TABLE:
      case BRANCH_APPEND:
      case STAGE_APPEND:
      case TABLE_TXN:
      case EXPIRE_SNAPSHOTS:
      case EXPIRE_OLD:
      case REWRITE_MANIFESTS:
      case DROP_TABLE:
      case RENAME_TABLE:
      case CREATE_NAMESPACE:
      case DROP_NAMESPACE:
      case SET_NAMESPACE_PROPERTY:
      case REMOVE_NAMESPACE_PROPERTY:
      // a view's catalog operations stage themselves again on a fresh read, and Iceberg's view
      // updates refresh and apply themselves again
      case CREATE_VIEW:
      case REPLACE_VIEW:
      case VIEW_PROPERTIES:
      case RENAME_VIEW:
      case DROP_VIEW:
      case REGISTER_VIEW:
        return true;
      case SET_RETENTION:
        return cmd.c % 4 == 3; // a table property; the others are ref changes
      default:
        return false;
    }
  }

  /**
   * The kinds that use no table handle: they read the catalog when they run, as a fresh handle
   * does.
   */
  private static boolean handleFree(Kind kind) {
    switch (kind) {
      case CREATE_TABLE:
      case DROP_TABLE:
      case RENAME_TABLE:
      case REPLACE_TABLE:
      case REINLINE:
      case SWEEP:
      case CREATE_NAMESPACE:
      case DROP_NAMESPACE:
      case SET_NAMESPACE_PROPERTY:
      case REMOVE_NAMESPACE_PROPERTY:
      case CREATE_VIEW:
      case REPLACE_VIEW:
      case VIEW_PROPERTIES:
      case RENAME_VIEW:
      case DROP_VIEW:
      case REGISTER_VIEW:
        return true;
      default:
        return false;
    }
  }

  // ============================================================
  // Schedule
  // ============================================================

  enum Kind {
    /** Creates a table in format v2 or v3, as {@code b} picks. */
    CREATE_TABLE,
    DROP_TABLE,
    RENAME_TABLE,
    FAST_APPEND,
    MERGE_APPEND,
    DELETE_FILE,
    SET_PROPERTY,
    /**
     * Sets a property to a random value of about half, 1.2 or 3 times the largest inline update,
     * as {@code b} picks: an update that fits as a delta, one that fits only in full (full
     * metadata is compressed, a delta is not), or one that evicts the table.
     */
    SET_BIG_PROPERTY,
    REMOVE_PROPERTY,
    ADD_COLUMN,
    /** Upgrades a table to format v3; a v3 table is unchanged. */
    UPGRADE_TABLE,
    /**
     * Replaces a table with one of the seed schema, in a replace transaction (a create-or-replace
     * one when {@code b % 3 == 0}) that appends a file when {@code b} is odd.
     */
    REPLACE_TABLE,
    /**
     * Adds an equality delete file, or when {@code b} is odd a position delete for a live data
     * file: in v3 a deletion vector, which a later delete of its data file removes with it.
     */
    ROW_DELTA,
    /** Partitions a table by identity, bucket[4] or truncate[10] of its id, as {@code b} picks. */
    ADD_PARTITION_FIELD,
    REMOVE_PARTITION_FIELD,
    /** Tags the current snapshot as t0 or t1, as {@code b} picks. */
    CREATE_TAG,
    /** Branches b0 or b1 from the current snapshot, as {@code b} picks. */
    CREATE_BRANCH,
    /** Appends a file to branch b0 or b1, which creates the branch from main if there is none. */
    BRANCH_APPEND,
    /** Fast-forwards main to branch b0 or b1. */
    FAST_FORWARD,
    /**
     * Moves main, of a table the last check found with sealed blocks if any has them, to one of
     * its snapshots that check found sealed, as {@code b} picks, or to one of its three oldest if
     * none is: an ancestor of main ({@code rollbackTo}) when {@code c} is even, else any snapshot
     * ({@code setCurrentSnapshot}). Behind the sealing line, the record brings the snapshot back
     * from its page with a Merkle proof (docs/SNAPSHOT_PAGING.md, "Testing").
     */
    ROLLBACK,
    /**
     * Tags a snapshot as {@link #ROLLBACK} picks one, as t0 or t1, as {@code c} picks: a tag the
     * handle's table has is replaced.
     */
    TAG_OLD,
    /** Removes a branch or tag the table has. */
    REMOVE_REF,
    /** Stages an append (write-audit-publish) under a new wap.id, leaving main as it is. */
    STAGE_APPEND,
    /** Publishes a staged append to main (a cherry-pick). */
    CHERRY_PICK,
    TABLE_TXN,
    /**
     * Expires snapshots as {@code c} picks (see {@link Expiry}); {@code b} picks whether it also
     * removes the schemas and partition specs no snapshot uses, and whether it keeps the files of
     * the snapshots it expires.
     */
    EXPIRE_SNAPSHOTS,
    /**
     * Expires one of the table's snapshots behind the sealing line, as the last check found it
     * ({@link Expiry#A_SNAPSHOT}, the first or second sealed, as {@code c} picks), cleaning up as
     * {@link #EXPIRE_SNAPSHOTS} does: a live id leaves its sealed block, or the block empties and
     * its page is dropped. Nothing, if the last check found none sealed.
     */
    EXPIRE_OLD,
    /**
     * Sets a retention, as {@code c} picks: a branch's minimum number of snapshots or maximum
     * snapshot age, a ref's maximum age, or a table property that sets one for all.
     */
    SET_RETENTION,
    REWRITE_MANIFESTS,
    /**
     * Brings a pointer table back inline ({@link LogCatalog#reinline}), its history behind the
     * sealing line in import pages when the catalog pages snapshots; it changes nothing a reader
     * sees, and in a writer that does not inline it does nothing.
     */
    REINLINE,
    /**
     * Deletes the log's orphaned pages ({@link CatalogLog#sweep}) as a fresh read holds the
     * catalog ({@link LogCatalog#deleteOrphanPages}) when {@code b % 3} is 0, else as the writer's
     * newest replica holds it (1) or the oldest it kept (2), however stale. Preempting another
     * writer's command, it runs at that command's first conditional write after it put a page (a
     * compactor's root, a re-inlining's record), and not at a request: a sweep from a replica
     * before the records that sealed a page must spare it.
     */
    SWEEP,
    /** Creates ns or ns2, as {@code b} picks, with a property when {@code b} is odd. */
    CREATE_NAMESPACE,
    /** Drops ns or ns2, as {@code b} picks, which fails unless it is empty. */
    DROP_NAMESPACE,
    /** Sets fz.k1 or fz.k2 on ns or ns2, as {@code b} picks. */
    SET_NAMESPACE_PROPERTY,
    /** Removes fz.k1 or fz.k2 from ns or ns2, as {@code b} picks. */
    REMOVE_NAMESPACE_PROPERTY,
    /**
     * Creates a view of a table name (docs/VIEWS.md: the two kinds share a namespace's names),
     * mostly one no table or view holds; with {@code gc.enabled} false when {@code b} is odd, so
     * its drop keeps its metadata in a file. A writer that inlines stores it inline.
     */
    CREATE_VIEW,
    /** Replaces a view's version (new SQL), as Iceberg's ReplaceViewVersion does. */
    REPLACE_VIEW,
    /** Sets fz.k1 or fz.k2 on a view, as {@code b} picks. */
    VIEW_PROPERTIES,
    /** Renames a view to a name {@code b} picks: mostly one no table or view holds. */
    RENAME_VIEW,
    /** Drops a view; one kept ({@code gc.enabled} false) leaves a metadata file to register. */
    DROP_VIEW,
    /** Registers, under a name {@code a} picks, a metadata file a kept view's drop left. */
    REGISTER_VIEW,
    /** Opens a catalog transaction; {@code b} picks the isolation level. */
    TXN_BEGIN,
    /** Loads a table in the open catalog transaction and stages an append to it. */
    TXN_APPEND,
    /**
     * Loads a table in the open catalog transaction and reads it, as {@code c % 4} picks: a scan
     * of main, of b0 or of t0 (main if the table has no such ref), or a declared read.
     */
    TXN_READ,
    /** Drops a table in the open catalog transaction, purging it when {@code b} is odd. */
    TXN_DROP,
    /**
     * Renames a table in the open catalog transaction to the one {@code b} picks: mostly a name
     * the table does not have, when {@code b < 8}.
     */
    TXN_RENAME,
    /** Creates a table in the open catalog transaction, in format v2 or v3 as {@code b} picks. */
    TXN_CREATE,
    /** Sets fz.k1 or fz.k2 on a table in the open catalog transaction. */
    TXN_SET_PROPERTY,
    /**
     * Namespace changes in the open catalog transaction, as their namesakes without TXN_; a drop,
     * when {@code c} is odd, drops the namespace's tables first, keeping their files.
     */
    TXN_CREATE_NAMESPACE,
    TXN_DROP_NAMESPACE,
    TXN_SET_NAMESPACE_PROPERTY,
    TXN_REMOVE_NAMESPACE_PROPERTY,
    /**
     * View changes in the open catalog transaction (VIEWS.md "Stage 2"), through its view catalog,
     * as their namesakes without TXN_ pick their operands: a create, a replace of its version, a
     * property, a rename and a drop. The commit applies them net, as BaseCatalogTransaction sends
     * them: a view it created as created last, one it had, changed only on the version it first
     * loaded; a create or rename may take a name the transaction frees, of either kind.
     */
    TXN_CREATE_VIEW,
    TXN_REPLACE_VIEW,
    TXN_VIEW_PROPERTIES,
    TXN_RENAME_VIEW,
    TXN_DROP_VIEW,
    TXN_COMMIT;

    /** Whether it stages a change or read in a catalog transaction, changing no catalog yet. */
    boolean stagesInATransaction() {
      return name().startsWith("TXN_") && this != TXN_COMMIT;
    }
  }

  /**
   * One generated command. {@code a} and {@code b} pick operands by index, interpreted against the
   * model when the command runs: a table picks from the model when {@code a < 8} and from the
   * whole universe otherwise, which reaches tables that do not exist. {@code fresh} reloads the
   * writer's handle on the table first; otherwise the writer reuses the handle it has, which may
   * be stale. {@code preempt}, if not negative, runs this command just before storage request
   * number {@code preempt} (reads of the catalog and the first conditional write) of the command
   * before it, if that is another writer's and gets that far. On faulty storage, {@code
   * writeFaults} and {@code readFault} pick the command's faults, and {@code pageFault} and {@code
   * plainFault} faults of its plain requests ({@link #plainPlans}).
   */
  static final class Cmd {
    final Kind kind;
    final int writer;
    final int a;
    final int b;
    final int c;
    final boolean fresh;
    final int preempt;
    final int writeFaults;
    final int readFault;
    final int plainFault;
    final int pageFault;

    Cmd(Kind kind, int writer, int a, int b, int c, boolean fresh, int preempt, int writeFaults,
        int readFault, int plainFault, int pageFault) {
      this.kind = kind;
      this.writer = writer;
      this.a = a;
      this.b = b;
      this.c = c;
      this.fresh = fresh;
      this.preempt = preempt;
      this.writeFaults = writeFaults;
      this.readFault = readFault;
      this.plainFault = plainFault;
      this.pageFault = pageFault;
    }

    /**
     * Whether another writer sweeps, from its kept replica or its newest, at this command's first
     * conditional write after it put a page ({@link Kind#SWEEP}): a quarter of the time, faults or
     * not.
     */
    boolean sweepsAfterPagePut() {
      return pageFault == 4 || pageFault == 5;
    }

    /**
     * Whether its first get of a page finds none, as if the store lost the page (on a shared log,
     * on faulty storage; {@code pageFault} 6): the load must fail, never read the block as empty.
     */
    boolean losesAPage() {
      return pageFault == 6;
    }

    /**
     * The plain requests to fail ({@link InMemoryAtomicFileIO.PlainRequest}: puts, deletes and
     * listings of the store, not conditional writes), each failed before it lands or landed with
     * its reply lost. Half the time the first or second page put (a log's: snapshot pages, import
     * pages, the committed set's), which only commands that compact or re-inline send ({@code
     * pageFault} 0-3; at 4 and 5 another writer sweeps after its first page put instead: {@link
     * #sweepsAfterPagePut}; at 6 its first page get finds none: {@link #losesAPage}); and apart from that, three times in eight, the first to fourth
     * delete, the first or second listing (a sweep's), or the first to third other put (the
     * tables' and views' metadata files, manifests and lists: a commit's, a materialized drop's,
     * an eviction's).
     */
    List<PlainPlan> plainPlans() {
      List<PlainPlan> plans = new ArrayList<>();
      if (pageFault < 4) {
        plans.add(new PlainPlan(PlainClass.PAGE_PUT, pageFault % 2,
            pageFault < 2 ? Fault.FAIL_BEFORE : Fault.LOST_REPLY));
      }
      if (plainFault >= 40) {
        int v = plainFault - 40;
        Fault fault = v % 2 == 0 ? Fault.FAIL_BEFORE : Fault.LOST_REPLY;
        if (v < 8) {
          plans.add(new PlainPlan(PlainClass.DELETE, v / 2, fault));
        } else if (v < 12) {
          plans.add(new PlainPlan(PlainClass.LIST, (v - 8) / 2, fault));
        } else if (v < 18) {
          plans.add(new PlainPlan(PlainClass.OTHER_PUT, (v - 12) / 2, fault));
        }
      }
      return plans;
    }

    List<Fault> writePlan() {
      return List.of(
          FIRST_WRITE_FAULTS.get(writeFaults % 12), SECOND_WRITE_FAULTS.get(writeFaults / 12));
    }

    /** The index of the read to fail, or -1: one of the first eight, two times in three. */
    int failingRead() {
      return readFault < 4 ? -1 : readFault - 4;
    }

    ReadFault readFaultKind() {
      return readFault % 2 == 0 ? ReadFault.FAIL : ReadFault.FAIL_MID_STREAM;
    }

    @Override
    public String toString() {
      // the faults too, when there are any, so that a failing sample can be replayed
      return String.format(
          "w%d:%s(%d,%d,%d%s)%s%s",
          writer, kind, a, b, c, fresh ? ",fresh" : "",
          preempt >= LATE ? "@late" + (preempt - LATE) : preempt >= 0 ? "@" + preempt : "",
          writeFaults != 0 || readFault != 0 || plainFault != 0 || pageFault != 0
              ? "!" + writeFaults + "/" + readFault + "/" + plainFault + "/" + pageFault : "");
    }
  }

  /** The classes of plain requests a command's plan counts and fails one of. */
  enum PlainClass {
    PAGE_PUT,
    OTHER_PUT,
    DELETE,
    LIST
  }

  /** The {@code index}th plain request of class {@code of} fails with {@code fault}. */
  static final class PlainPlan {
    final PlainClass of;
    final int index;
    final Fault fault;

    PlainPlan(PlainClass of, int index, Fault fault) {
      this.of = of;
      this.index = index;
      this.fault = fault;
    }
  }

  /** What an expiry expires, as its command's {@code c % 4} picks; {@code c / 4} is an operand. */
  enum Expiry {
    /** Every snapshot older than now that no ref retains, keeping the last 1 to 3 of a branch. */
    OLDER_THAN_NOW,
    /** The snapshots older than one of the table's, the operand picking which, oldest first. */
    OLDER_THAN_A_SNAPSHOT,
    /** One of the table's snapshots, the operand picking which, oldest first. */
    A_SNAPSHOT,
    /** What the retention settings of the table and its refs alone expire. */
    BY_RETENTION
  }

  // ============================================================
  // Model
  // ============================================================

  private static final Comparator<TableIdentifier> BY_NAME =
      Comparator.comparing(TableIdentifier::toString);

  /** The tables' states, by identifier, and the namespaces' properties. */
  static final class Model extends TreeMap<TableIdentifier, TableState> {
    final Map<Namespace, Map<String, String>> namespaces =
        new TreeMap<>(Comparator.comparing(Namespace::toString));
    // Views, by name (docs/VIEWS.md)
    final Map<TableIdentifier, ViewState> views = new TreeMap<>(BY_NAME);
    // The metadata files kept views' drops left, each with the view it holds: what a register may
    // bring back (not observed: the catalog does not name them)
    final Map<String, ViewState> keptViews = new TreeMap<>();

    Model() {
      super(BY_NAME);
    }

    @Override
    public String toString() {
      return "namespaces " + namespaces + ", tables " + super.toString()
          + (views.isEmpty() ? "" : ", views " + views);
    }
  }

  /** What the model tracks of a view, and what it observed of it at the last check. */
  static final class ViewState {
    final String sql;
    final int versionId;
    final boolean kept; // gc.enabled false: its drop keeps its metadata in a file
    final Map<String, String> props = new TreeMap<>();
    // Observed, not modeled: its metadata's location (a file, or the catalog's sentinel), and the
    // view's location, where its metadata files go
    String metadataLocation = null;
    String location = null;

    ViewState(String sql, int versionId, boolean kept) {
      this.sql = sql;
      this.versionId = versionId;
      this.kept = kept;
    }

    ViewState with(String newSql, int newVersionId) {
      ViewState copy = new ViewState(newSql, newVersionId, kept);
      copy.props.putAll(props);
      copy.metadataLocation = metadataLocation;
      copy.location = location;
      return copy;
    }

    ViewState copy() {
      return with(sql, versionId);
    }

    boolean sameAs(ViewState other) {
      return sql.equals(other.sql) && versionId == other.versionId && kept == other.kept
          && props.equals(other.props);
    }

    @Override
    public String toString() {
      return String.format("{%s v%d%s props=%s}", sql, versionId, kept ? " kept" : "", props);
    }
  }

  /** What the model tracks of a table, and what it observed of it at the last check. */
  static final class TableState {
    final String uuid;
    int formatVersion;
    final List<String> files = new ArrayList<>();
    // Live delete files, each to the data file it is a deletion vector for, or "". Those in
    // maybeDropped may have been dropped by the last commit (see ageOutDeletes).
    final Map<String, String> deletes = new TreeMap<>();
    final Set<String> maybeDropped = new TreeSet<>();
    final Map<String, String> props = new TreeMap<>();
    final Set<String> columns = new TreeSet<>();
    // Branches other than main, each to its live data files; tags, each to the live data files of
    // the snapshot it names, as when it was created, and that snapshot (observed then).
    final Map<String, Set<String>> branches = new TreeMap<>();
    final Map<String, Set<String>> tags = new TreeMap<>();
    final Map<String, Long> tagSnapshots = new TreeMap<>();
    // Staged appends not yet published nor expired, by wap.id, to the file each adds.
    final Map<String, String> staged = new TreeMap<>();
    // Refs given a short maximum age, and whether the table gives every ref one: an expiry may
    // remove them. A name stays here once added.
    final Set<String> agingRefs = new TreeSet<>();
    boolean refsAge = false;
    // Observed, not modeled: Iceberg chooses snapshot ids, metadata locations and sequence numbers
    // (of the table, and the data sequence numbers of its live data and delete files).
    Long snapshotId = null;
    String metadataLocation = null;
    long lastSequenceNumber = 0;
    final Map<String, Long> sequenceNumbers = new HashMap<>();
    // Observed at a check, for the history oracles: the table's metadata as JSON, its snapshots'
    // ids, and the snapshot each ref names.
    String json = null;
    final Set<Long> snapshotIds = new TreeSet<>();
    final Map<String, Long> refIds = new TreeMap<>();

    TableState(String uuid, int formatVersion) {
      this.uuid = uuid;
      this.formatVersion = formatVersion;
    }

    TableState copy() {
      return withUuid(uuid);
    }

    /** A copy of this state, as a table of UUID {@code uuid}. */
    TableState withUuid(String uuid) {
      TableState copy = new TableState(uuid, formatVersion);
      copy.files.addAll(files);
      copy.deletes.putAll(deletes);
      copy.maybeDropped.addAll(maybeDropped);
      copy.props.putAll(props);
      copy.columns.addAll(columns);
      branches.forEach((name, files) -> copy.branches.put(name, new TreeSet<>(files)));
      tags.forEach((name, files) -> copy.tags.put(name, new TreeSet<>(files)));
      copy.tagSnapshots.putAll(tagSnapshots);
      copy.staged.putAll(staged);
      copy.agingRefs.addAll(agingRefs);
      copy.refsAge = refsAge;
      copy.snapshotId = snapshotId;
      copy.metadataLocation = metadataLocation;
      copy.lastSequenceNumber = lastSequenceNumber;
      copy.sequenceNumbers.putAll(sequenceNumbers);
      copy.json = json;
      copy.snapshotIds.addAll(snapshotIds);
      copy.refIds.putAll(refIds);
      return copy;
    }

    /** Drops the refs that may have aged out and that {@code now}, observed, no longer has. */
    void ageOutRefs(TableState now) {
      branches.keySet().removeIf(ref -> mayAge(ref) && !now.branches.containsKey(ref));
      tags.keySet().removeIf(ref -> mayAge(ref) && !now.tags.containsKey(ref));
      tagSnapshots.keySet().retainAll(tags.keySet());
    }

    private boolean mayAge(String ref) {
      return refsAge || agingRefs.contains(ref);
    }

    List<String> sortedFiles() {
      List<String> sorted = new ArrayList<>(files);
      sorted.sort(Comparator.naturalOrder());
      return sorted;
    }

    @Override
    public String toString() {
      Map<String, String> shown = new TreeMap<>();
      props.forEach((k, v) -> shown.put(k, abbreviate(v)));
      return String.format(
          "{uuid=%s v%d files=%s deletes=%s props=%s columns=%s%s%s}",
          uuid.substring(0, 8), formatVersion, sortedFiles(), deletes, shown, columns,
          branches.isEmpty() ? "" : " branches=" + branches,
          tags.isEmpty() ? "" : " tags=" + tags);
    }
  }

  private static String abbreviate(String value) {
    return value.length() <= 16
        ? value
        : value.substring(0, 8) + "...(" + value.length() + " chars)";
  }

  private static Model copy(Model model) {
    Model copy = new Model();
    model.forEach((id, state) -> copy.put(id, state.copy()));
    model.namespaces.forEach((ns, props) -> copy.namespaces.put(ns, new TreeMap<>(props)));
    model.views.forEach((id, state) -> copy.views.put(id, state.copy()));
    model.keptViews.forEach((file, state) -> copy.keptViews.put(file, state.copy()));
    return copy;
  }

  /** How {@code actual} differs from {@code expected}, or null if it matches. */
  private static String diff(Model expected, Model actual) {
    if (!expected.namespaces.equals(actual.namespaces)) {
      return "namespaces " + expected.namespaces + " expected, found " + actual.namespaces;
    }
    if (!expected.views.keySet().equals(actual.views.keySet())) {
      return "views " + expected.views.keySet() + " expected, found " + actual.views.keySet();
    }
    for (TableIdentifier id : expected.views.keySet()) {
      if (!expected.views.get(id).sameAs(actual.views.get(id))) {
        return "view " + id + ": expected " + expected.views.get(id) + ", found "
            + actual.views.get(id);
      }
    }
    if (!expected.keySet().equals(actual.keySet())) {
      return "tables " + expected.keySet() + " expected, found " + actual.keySet();
    }
    for (TableIdentifier id : expected.keySet()) {
      TableState e = expected.get(id);
      TableState a = actual.get(id);
      if (!e.uuid.equals(a.uuid)
          || e.formatVersion != a.formatVersion
          || !e.sortedFiles().equals(a.sortedFiles())
          || !deletesMatch(e, a)
          || !e.branches.equals(a.branches)
          || !e.tags.equals(a.tags)
          || !e.tagSnapshots.equals(a.tagSnapshots)
          || !e.props.equals(a.props)
          || !e.columns.equals(a.columns)) {
        return id + ": expected " + e + ", found " + a;
      }
    }
    return null;
  }

  /** Whether {@code a}'s delete files are {@code e}'s, less any of those that may be dropped. */
  private static boolean deletesMatch(TableState e, TableState a) {
    for (Map.Entry<String, String> d : a.deletes.entrySet()) {
      if (!d.getValue().equals(e.deletes.get(d.getKey()))) {
        return false;
      }
    }
    for (String d : e.deletes.keySet()) {
      if (!a.deletes.containsKey(d) && !e.maybeDropped.contains(d)) {
        return false;
      }
    }
    return true;
  }

  /** An operation's intended change to the model; throws if it cannot apply. */
  @FunctionalInterface
  interface Effect {
    void apply(Model model, Model actual);
  }

  /** What an operation does to storage, through Iceberg. */
  @FunctionalInterface
  interface Action {
    void run();
  }

  /** An operation with concrete operands. */
  static final class Op {
    final String description;
    final Action action;
    final Effect effect;
    // For a catalog transaction's commit: the tables it loaded, as it first loaded them
    Map<TableIdentifier, Loaded> loaded = null;
    // For an expiry: its table, and the snapshots it chose to expire just before it committed
    TableIdentifier expiring = null;
    Set<Long> chosen = null;
    // The tables, by their names before it ran, it drops keeping their files
    List<TableIdentifier> keeping = List.of();
    // For a catalog transaction's commit: whether it changes views
    boolean changesViews = false;

    Op(String description, Action action, Effect effect) {
      this.description = description;
      this.action = action;
      this.effect = effect;
    }

    @Override
    public String toString() {
      return description;
    }
  }

  private static TableState existing(Map<TableIdentifier, TableState> model, TableIdentifier id) {
    TableState state = model.get(id);
    if (state == null) {
      throw new IllegalStateException(id + " does not exist");
    }
    return state;
  }

  /**
   * Renames each table of {@code renames} and each view of {@code viewRenames} from its name to
   * its new one, as LogCatalog stages a catalog transaction's renames: each out of a name before
   * any into it, of either kind, so a rename takes a name another of them frees whatever their
   * order; one into a name still held (a swap) fails. A table and a view share a namespace's
   * names (VIEWS.md V7).
   */
  private static void renameAll(Model m, List<Map.Entry<TableIdentifier, TableIdentifier>> renames,
      List<Map.Entry<TableIdentifier, TableIdentifier>> viewRenames) {
    List<Map.Entry<TableIdentifier, TableIdentifier>> left = new ArrayList<>(renames);
    left.addAll(viewRenames);
    while (!left.isEmpty()) {
      Map.Entry<TableIdentifier, TableIdentifier> next = left.stream()
          .filter(r -> !m.containsKey(r.getValue()) && !m.views.containsKey(r.getValue()))
          .findFirst()
          .orElse(left.get(0));
      left.remove(next);
      TableIdentifier from = next.getKey();
      TableIdentifier to = next.getValue();
      boolean view = viewRenames.contains(next);
      if (view) {
        existingView(m, from);
      } else {
        existing(m, from);
      }
      if (m.containsKey(to) || m.views.containsKey(to)) {
        throw new IllegalStateException(to + " exists");
      }
      if (!m.namespaces.containsKey(to.namespace())) {
        throw new IllegalStateException(to.namespace() + " does not exist");
      }
      if (view) {
        m.views.put(to, m.views.remove(from));
      } else {
        m.put(to, m.remove(from));
      }
    }
  }

  private static ViewState existingView(Model model, TableIdentifier id) {
    ViewState state = model.views.get(id);
    if (state == null) {
      throw new IllegalStateException("view " + id + " does not exist");
    }
    return state;
  }

  /** Fails the effect unless {@code id} is a name no table or view holds, in a namespace. */
  private static void checkBindable(Model model, TableIdentifier id) {
    if (model.containsKey(id) || model.views.containsKey(id)
        || !model.namespaces.containsKey(id.namespace())) {
      throw new IllegalStateException("cannot bind " + id);
    }
  }

  enum Outcome {
    ACKNOWLEDGED,
    /**
     * A definite no: CommitFailedException, a validation or existence failure, a write the store
     * refused (CommitRefusedException, Bug 118 (refused writes reported unknown)), or a catalog
     * operation that ran out of attempts (ServiceUnavailableException).
     */
    REJECTED,
    /** CommitStateUnknownException. */
    UNKNOWN,
    /** Any other exception, after an injected fault. */
    FAILED
  }

  // ============================================================
  // Execution
  // ============================================================

  /** A table loaded in a writer's open catalog transaction, as it was when first loaded. */
  private static final class Loaded {
    final String uuid;
    final String metadataLocation;
    final TableMetadata metadata;
    // Whether the transaction changed it (an update, a drop, a rename it did not undo: set at the
    // commit), and updated it (an append or a property, which the conflict encoding judges on an
    // inline table)
    boolean changed = false;
    boolean updated = false;
    // The refs it scanned, each to the snapshot it named then (null: none), and whether it
    // declared a read of the table
    final Map<String, Long> scans = new TreeMap<>();
    boolean declared = false;

    Loaded(TableMetadata metadata) {
      this.uuid = metadata.uuid();
      this.metadataLocation = metadata.metadataFileLocation();
      this.metadata = metadata;
    }
  }

  /**
   * A view a writer's open catalog transaction touched: one it had, as it first loaded it (its
   * name then, and the version it read), or one it created; what the transaction made of it, and
   * its name there.
   */
  private static final class TxnView {
    final TableIdentifier start; // null for a view it created
    final String baseLocation;
    ViewState current;
    TableIdentifier name;
    boolean updated = false;
    boolean dropped = false;

    TxnView(TableIdentifier name, org.apache.iceberg.view.ViewMetadata base) {
      this.start = name;
      this.baseLocation = base.metadataFileLocation();
      this.current = Run.viewState(base);
      this.name = name;
    }

    TxnView(TableIdentifier name, ViewState created) {
      this.start = null;
      this.baseLocation = null;
      this.current = created;
      this.name = name;
    }

    /** Whether the commit changes the view: a create, or a change to one it had. */
    boolean changes() {
      return start == null ? !dropped : dropped || updated || !name.equals(start);
    }

    @Override
    public String toString() {
      if (start == null) {
        return "create " + name;
      }
      return dropped ? "drop " + start
          : (updated ? "update " + start : "") + (updated && !name.equals(start) ? " and " : "")
              + (name.equals(start) ? "" : "rename " + start + " to " + name);
    }
  }

  /**
   * A change a writer staged in its open catalog transaction, and its effect at the commit. The
   * commit applies a transaction's changes as LogCatalog stages them, by phase: namespace creates
   * and properties; then the changes to the tables it loaded, each by the catalog's name the
   * table had when the transaction loaded it; then the drops and updates of the views it had
   * (LogCatalog.stageViewChanges); then the renames of those tables and views, net (a name to its
   * final one), each out of a name before any into it, of either kind; then its creates, with the
   * changes to the tables it created; then the views it created; then its namespace drops. Within
   * a phase, in the order the transaction staged them. So a create or rename takes a name the
   * transaction frees, of either kind (errata D29 (txn takes no freed name), fixed).
   */
  private static final class Staged {
    static final int NAMESPACES = 0;
    static final int TABLES = 1;
    static final int VIEWS = 2;
    static final int RENAMES = 3;
    static final int CREATES = 4;
    static final int VIEW_CREATES = 5;
    static final int NAMESPACE_DROPS = 6;

    final String description;
    final int phase;
    final Effect effect;

    Staged(String description, int phase, Effect effect) {
      this.description = description;
      this.phase = phase;
      this.effect = effect;
    }

    @Override
    public String toString() {
      return description;
    }
  }

  /**
   * The key a kept view a catalog transaction dropped has among the model's kept views until the
   * check after its commit finds its metadata file.
   */
  private static final String KEPT_BY_A_TRANSACTION = "dropped kept by a transaction: ";

  /**
   * A table a catalog transaction created, under the name it has in the transaction: the commit
   * creates it there, as BaseCatalogTransaction sends a created table, net of its renames (a
   * CatalogCommit names no rename of a table it creates). The names it passed through need not be
   * free at the commit, as a loaded table's need not ({@link #renameAll}); what the transaction
   * staged on it applies to it wherever it ends.
   */
  private static final class TxnCreated {
    TableIdentifier name;

    TxnCreated(TableIdentifier name) {
      this.name = name;
    }
  }

  /** The UUID the model gives a table a catalog transaction creates, until its commit lands. */
  private static final String CREATED_IN_TXN = "created in a transaction ";

  private static final class Writer {
    final int id;
    final LogCatalog catalog;
    // Whether it writes table metadata inline: its updates go as deltas, which the conflict
    // encoding can judge where they land; and manifest lists too
    final boolean inline;
    final boolean inlineLists;
    final Map<TableIdentifier, Table> handles = new HashMap<>();
    CatalogTransaction txn = null;
    // The tables the open transaction loaded, by the catalog's names they had then
    final Map<TableIdentifier, Loaded> loaded = new LinkedHashMap<>();
    final List<Staged> staged = new ArrayList<>();
    // Each name the transaction resolves to a table it holds, to the catalog's name that table had
    // when the transaction loaded it; a table it created, to null
    final Map<TableIdentifier, TableIdentifier> names = new HashMap<>();
    // The tables it created, by the names they have in it
    final Map<TableIdentifier, TxnCreated> created = new HashMap<>();
    // The tables it drops with purge, and keeping their files, by the catalog's names; the
    // namespaces it created
    final List<TableIdentifier> purging = new ArrayList<>();
    final List<TableIdentifier> keeping = new ArrayList<>();
    final Set<Namespace> createdNamespaces = new HashSet<>();
    // The views the open transaction touched, as BaseCatalogTransaction keeps them: each by the
    // name it has in the transaction, and the names it freed of views (dropped, renamed out)
    final List<TxnView> views = new ArrayList<>();
    final Map<TableIdentifier, TxnView> viewsByName = new HashMap<>();
    final Set<TableIdentifier> viewsGone = new HashSet<>();
    // The newest replica its log held after its first command, or after its last sweep from it: a
    // stale replica to sweep from (SWEEP)
    Replica<CatalogState> kept = null;

    Writer(int id, LogCatalog catalog, boolean inline, boolean inlineLists) {
      this.id = id;
      this.catalog = catalog;
      this.inline = inline;
      this.inlineLists = inlineLists;
    }

    void closeTxn() {
      txn = null;
      loaded.clear();
      staged.clear();
      names.clear();
      created.clear();
      purging.clear();
      keeping.clear();
      createdNamespaces.clear();
      views.clear();
      viewsByName.clear();
      viewsGone.clear();
    }

    /**
     * Loads view {@code name} in the open transaction, and returns how the transaction holds it:
     * the first load of a view it had records the version it read.
     */
    TxnView loadView(TableIdentifier name, org.apache.iceberg.view.View[] view) {
      view[0] = txn.asViewCatalog().loadView(name);
      TxnView held = viewsByName.get(name);
      if (held == null) {
        held = new TxnView(name,
            ((org.apache.iceberg.view.BaseView) view[0]).operations().current());
        views.add(held);
        viewsByName.put(name, held);
      }
      return held;
    }

    /**
     * Loads {@code id} in the open transaction, and returns how the transaction first loaded the
     * table it names there; null for a table it created.
     */
    Loaded load(TableIdentifier id, Table[] table) {
      table[0] = txn.asCatalog().loadTable(id);
      // A table the transaction has not touched is the catalog's; one it created maps to null,
      // which putIfAbsent would take for absent
      if (!names.containsKey(id)) {
        names.put(id, id);
      }
      TableIdentifier origin = names.get(id);
      return origin == null ? null : loaded.computeIfAbsent(
          origin, k -> new Loaded(((HasTableOperations) table[0]).operations().current()));
    }
  }

  private static final class Run {
    private final int maxUpdateSize;
    private final boolean faults;
    // whether a writer compacts with a retention of 0, which can leave a commit's outcome unknown
    private final boolean gc;
    private final InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    private final Writer[] writers;
    private final LogCatalog checker;
    private final Map<String, String> readerProperties;
    // The catalog's shared log, or null if the catalog is one object of the store.
    private final InMemorySharedLog sharedLog;
    // The Append Blobs of the catalog's log, or null if it keeps another log, and the blocks a
    // blob of it holds.
    private final InMemoryAppendBlobStore azure;
    private int azureBlocks;
    private Model model;
    // The catalog as the last check read it, and the snapshots each table (by UUID) lost since it
    // was created, which never come back
    private Model lastActual;
    private final Map<String, Set<Long>> goneSnapshots = new HashMap<>();
    private final StringBuilder trace = new StringBuilder();
    private int nextFile = 0;
    private int nextValue = 0;
    // Each snapshot's fields and manifest list when first seen, by table UUID and snapshot id: a
    // committed snapshot never changes, whether its list is inline or a file, and whichever writer
    // reads it.
    private final Map<String, Map<Long, List<String>>> manifestLists = new HashMap<>();
    // the data and delete files each manifest reaches (liveFiles), by its path
    private final Map<String, List<String>> manifestFiles = new HashMap<>();
    // Each table's storage at the last check, by UUID: inline or a metadata file.
    private final Map<String, String> storage = new HashMap<>();
    // Each data file's first row id, and the snapshot of main it was last seen in, by table UUID:
    // row ids never change along main's lineage.
    private final Map<String, Map<String, long[]>> rowIds = new HashMap<>();
    // The parent of every snapshot a check has seen, by table UUID, which outlives its expiry.
    private final Map<String, Map<Long, Long>> parents = new HashMap<>();
    // Conditional writes whose catalog file has been checked against a checkpoint of it.
    private int checkedWrites = 0;
    /** The most sealed snapshot blocks any table had at a check (docs/SNAPSHOT_PAGING.md). */
    private int mostSealed = 0;

    // The running top-level command's storage plan. Faults and preemption apply only while armed,
    // and only on the thread that runs the schedule.
    private volatile boolean armed = false;
    private Thread owner;
    private Cmd pending; // the command that may preempt the running one
    private int requests; // reads of the catalog and conditional writes, until the first write
    private boolean wrote;
    private int lateRequests; // requests of any kind after the first conditional write
    // The top-level command's operation, while it runs, and whether a late preemption found its
    // effect in the catalog and settled it then (settleEarly)
    private Op runningOp;
    private boolean settled;
    // A late preemption that did not settle the running command (its record had not landed, or
    // landed changing nothing a check reads): the model then, the preempting command's operation,
    // and whether it took effect (lateOrder); and the operation of the last command run nested
    private Model beforeLate;
    private Op lateOp;
    private boolean lateTookEffect;
    private Op lastNestedOp;
    // Whether the running command's effect is in the model already (lateOrder), for resolve
    private boolean effectApplied;
    private List<Fault> writePlan;
    private int writes;
    private int failingRead;
    private ReadFault readFault;
    private int reads;
    private final List<String> fired = new ArrayList<>();
    // The kinds of the commands that preempted the running one
    private final List<Kind> preempting = new ArrayList<>();
    // The running top-level command's plain-request faults, and its plain requests so far, by class
    private List<PlainPlan> plainPlans = List.of();
    private Cmd running; // the top-level command running, while it runs
    private boolean swept; // whether a sweep ran after one of its page puts
    private int pageGets; // its gets of a shared log's pages
    private final int[] plainRequests = new int[PlainClass.values().length];
    // The catalog as the last check read it: where the sealing line is, for statistics
    private CatalogState lastState = null;

    /**
     * Writer {@code i} writes inline metadata with the gzip codec if bit {@code i} of {@code
     * gzipWriters} is set, else the structural one: a delta keeps its base's codec, and a full
     * update writes the writer's. Writer {@code i} keeps no committed transaction ids older than
     * its own commit's when it compacts (a retention of 0) if bit {@code i} of {@code gcWriters}
     * is set, else the default 30 minutes, which no run reaches. Writer {@code i} pages the
     * committed set as it compacts, at an admission window and threshold of 0 (every block below
     * its own id but the newest), if bit {@code i} of {@code pagingWriters} is set, and its clock is
     * off by {@code clockOffsets.get(i)} ms. Writer {@code i} compacts with its commits past the
     * commit knobs, replacing the catalog instead of appending, if bit {@code i} of {@code
     * fusedWriters} is set, else after them, at its own point past the knobs. Writer {@code i}
     * binds the names it creates by the namespace's version ({@code
     * fileio.catalog.create.condition} namespace) if bit {@code i} of {@code namespaceWriters} is
     * set, else by name. With a {@code segmentLimit}, the catalog object holds
     * at most that many writes, and an append past it fails (Bug 66); on Append Blobs, each
     * segment holds at most that many blocks, its record limit K, else Azure's 50,000 with K
     * {@link #AZURE_SEGMENT_RECORDS}.
     */
    Run(List<LocalCatalogMode> modes, int maxUpdateSize, boolean faults, boolean strictVersions,
        InMemoryAtomicFileIO.ReadMode reads, boolean recordingReads, int gzipWriters,
        int gcWriters, int pagingWriters,
        int wholeReaders, int fusedWriters, int namespaceWriters, List<Long> clockOffsets,
        int segmentLimit,
        String encoding) {
      this.maxUpdateSize = maxUpdateSize;
      this.faults = faults;
      this.gc = (gcWriters & ((1 << modes.size()) - 1)) != 0;
      io.failSupersededReads(strictVersions);
      // Reads bind at the open, or are pinned, live or spliced after a lookup that a preempting
      // command can follow (TAIL_READS.md, errata T6)
      io.readMode(reads);
      Statistics.label("reads").collect(reads);
      // and record the version they find, one GET a read with no lookup, or not (step 7)
      io.setReadsRecordVersion(recordingReads);
      Statistics.label("reads record their version").collect(recordingReads);
      if (segmentLimit > 0 && !modes.get(0).azureLog) {
        // the one object's appends (Bug 66); a segmented log's segments take the limit instead
        io.limitSegments(segmentLimit);
      }
      Statistics.label("segment limit").collect(segmentLimit);
      Map<String, String> properties = new HashMap<>(PROPERTIES);
      properties.put(FileIOCatalog.INLINE_MAX_UPDATE_SIZE, String.valueOf(maxUpdateSize));
      // The first writer creates the catalog, with the encoding its log keeps.
      properties.put(LogCatalogProperties.CONFLICT_ENCODING, encoding);
      Statistics.label("encoding").collect(encoding);
      if (modes.get(0).azureLog) {
        // The store before the catalogs that open the log on it, with the block limit drawn
        this.azureBlocks = segmentLimit > 0 ? segmentLimit : 50_000;
        this.azure = LocalCatalogMode.azureStore(io, CATALOG_LOCATION, azureBlocks);
        boolean small = maxUpdateSize > AZURE_BLOCK_BYTES;
        if (small) {
          // The record's bound, not the inline limit, evicts big properties (SPEC_TM.md "Size
          // Budget")
          azure.limitBlockBytes(AZURE_BLOCK_BYTES);
        }
        Statistics.label("Append Blob blocks").collect(
            small ? AZURE_BLOCK_BYTES + " B" : "service's");
        // K: the block limit drawn, or segments of a few records, so that the log moves on
        properties.put(LogCatalogProperties.LOG_SEGMENT_RECORDS,
            String.valueOf(segmentLimit > 0 ? segmentLimit : AZURE_SEGMENT_RECORDS));
      } else {
        this.azure = null;
      }
      this.writers = new Writer[modes.size()];
      for (int i = 0; i < modes.size(); i++) {
        Map<String, String> writerProperties = new HashMap<>(properties);
        writerProperties.put(
            FileIOCatalog.INLINE_TM_CODEC, (gzipWriters >> i & 1) == 1 ? "gzip" : "structural");
        if ((gcWriters >> i & 1) == 1) {
          writerProperties.put(LogCatalogProperties.COMMITTED_RETENTION_MS, "0");
        }
        if ((pagingWriters >> i & 1) == 1) {
          writerProperties.put(LogCatalogProperties.COMMITTED_ADMISSION_MS, "0");
          writerProperties.put(LogCatalogProperties.COMMITTED_PAGE_THRESHOLD, "0");
        }
        if ((wholeReaders >> i & 1) == 1) { // tail reads off (TAIL_READS.md step 5)
          writerProperties.put(LogCatalogProperties.READ_TAIL, "false");
        }
        if ((fusedWriters >> i & 1) == 1) { // a commit past the limits replaces the catalog
          writerProperties.put(LogCatalogProperties.COMPACT_AFTER_COMMIT, "false");
        }
        if ((namespaceWriters >> i & 1) == 1) { // its creates bind by the namespace's version
          writerProperties.put(LogCatalogProperties.CREATE_CONDITION, "namespace");
        }
        long offset = clockOffsets.get(i);
        // Each writer compacts after its commits at its own point past the limits: writer 0 at
        // them, the others 0.4 and 0.8 of the limits further on
        double draw = (i * 0.4) % 1.0;
        this.writers[i] = new Writer(i, modes.get(i).catalog(io, WAREHOUSE, CATALOG_NAME,
            writerProperties, offset == 0 ? null : () -> System.currentTimeMillis() + offset,
            () -> draw), modes.get(i).inlineTM || modes.get(i).inlineML, modes.get(i).inlineML);
      }
      // Reading does not depend on a catalog's mode, only on its log.
      this.checker = LocalCatalogMode.readerFor(io, CATALOG_LOCATION)
          .catalog(io, WAREHOUSE, CATALOG_NAME, properties);
      this.readerProperties = properties;
      this.sharedLog = modes.get(0).sharedLog
          ? LocalCatalogMode.sharedLog(io, CATALOG_LOCATION) : null;
      LogCatalog seed = this.writers[0].catalog;
      seed.createNamespace(NS);
      create(seed, TABLES.get(0), 3);
      create(seed, TABLES.get(1), 2).newFastAppend().appendFile(newFile()).commit();
      // A view, kept (its drop leaves its metadata in a file to register), in the writer's form
      seed.buildView(VIEW_NAMES.get(TABLES.size()))
          .withSchema(SCHEMA)
          .withDefaultNamespace(NS)
          .withQuery("spark", "select 'seed'")
          .withProperty(TableProperties.GC_ENABLED, "false")
          .create();
      this.lastActual = actual();
      this.model = copy(lastActual);
      trace.append(String.format(
          "modes %s, max update size %d%s, reads %s, gzip writers %s, gc writers %s,"
              + " paging writers %s, writers creating by namespace %s, clocks off by %s ms,"
              + " segment limit %d%nseed: %s%n",
          modes, maxUpdateSize, strictVersions ? ", strict versions" : "", reads,
          Integer.toBinaryString(gzipWriters), Integer.toBinaryString(gcWriters),
          Integer.toBinaryString(pagingWriters), Integer.toBinaryString(namespaceWriters),
          clockOffsets.subList(0, modes.size()), segmentLimit, model));
      Statistics.label("gc writers")
          .collect(Integer.bitCount(gcWriters & ((1 << modes.size()) - 1)));
      Statistics.label("whole readers")
          .collect(Integer.bitCount(wholeReaders & ((1 << modes.size()) - 1)));
      Statistics.label("fused writers")
          .collect(Integer.bitCount(fusedWriters & ((1 << modes.size()) - 1)));
      Statistics.label("writers creating by namespace")
          .collect(Integer.bitCount(namespaceWriters & ((1 << modes.size()) - 1)));
      Statistics.label("paging writers")
          .collect(Integer.bitCount(pagingWriters & ((1 << modes.size()) - 1)));
      Statistics.label("clocks off")
          .collect(clockOffsets.subList(0, modes.size()).stream().anyMatch(o -> o != 0));
      Statistics.label("modes").collect(new TreeSet<>(modes).size() == 1 ? "one" : "mixed");
      Statistics.label("log").collect(
          sharedLog != null ? "shared log" : azure != null ? "Append Blobs" : "one object");
      io.injectFaults(this::onWrite);
      io.injectReadFaults(this::onRead);
      io.injectPlainFaults((request, location) ->
          onPlain(request, location, location != null && location.contains(".committed/")));
      if (azure != null) {
        azure.intercept(this::onAzure);
      }
      if (sharedLog != null) {
        // The log's requests meet the faults and preemption the catalog object's would.
        sharedLog.intercept(new InMemorySharedLog.Interceptor() {
          @Override
          public InMemorySharedLog.WriteFault beforeWrite(
              InMemorySharedLog.Request request, byte[] payload) {
            if (request != InMemorySharedLog.Request.APPEND) {
              if (request == InMemorySharedLog.Request.PUT_CHECKPOINT) {
                sweepAfterPagePut(); // a compactor's root
              }
              return InMemorySharedLog.WriteFault.NONE;
            }
            Fault fault = onWrite(CATALOG_LOCATION, AtomicOutputFile.Strategy.APPEND);
            return InMemorySharedLog.WriteFault.valueOf(
                fault == Fault.LOST_REPLY_RUNTIME ? "LOST_REPLY" : fault.name());
          }

          @Override
          public InMemorySharedLog.ReadShape beforeRead(InMemorySharedLog.Request request) {
            return onRead(CATALOG_LOCATION) == ReadFault.NONE
                ? InMemorySharedLog.ReadShape.NORMAL : InMemorySharedLog.ReadShape.FAIL;
          }

          // Its pages meet the plain-request faults and the read faults, as a store's do
          @Override
          public InMemorySharedLog.PageFault beforePage(
              InMemorySharedLog.Request request, String name) {
            Fault fault;
            switch (request) {
              case GET_PAGE:
                // a get that fails, or finds no page, as if the store lost it: a load must fail
                // then, and never take the block for empty
                if (faults && armed && Thread.currentThread() == owner && running != null
                    && running.losesAPage() && pageGets++ == 0) {
                  fired.add("page get #0 MISSING of " + name);
                  Statistics.label("plain fault fired").collect("PAGE_GET MISSING");
                  return InMemorySharedLog.PageFault.MISSING;
                }
                ReadFault read = onRead("page " + name);
                return read == ReadFault.NONE ? InMemorySharedLog.PageFault.NONE
                    : read == ReadFault.FAIL ? InMemorySharedLog.PageFault.FAIL
                        : InMemorySharedLog.PageFault.MISSING;
              case PUT_PAGE:
                fault = onPlain(InMemoryAtomicFileIO.PlainRequest.PUT, "page " + name, true);
                break;
              case DELETE_PAGE:
                fault = onPlain(InMemoryAtomicFileIO.PlainRequest.DELETE, "page " + name, true);
                break;
              default:
                fault = onPlain(InMemoryAtomicFileIO.PlainRequest.LIST, "pages", true);
            }
            return fault == Fault.FAIL_BEFORE ? InMemorySharedLog.PageFault.FAIL
                : fault == Fault.LOST_REPLY ? InMemorySharedLog.PageFault.LOST_REPLY
                    : InMemorySharedLog.PageFault.NONE;
          }
        });
      }
    }

    /**
     * Runs {@code schedule}, without the suites' check of each tail read against a whole read
     * (AtomicFileLog.CHECK_TAIL_READS): its read is a request after the tail read's, where a
     * preemption would land after the operation read the catalog, which this fuzzer's model
     * orders after the preempting one (the check's own oracle is the commit-protocol fuzzer's).
     */
    void execute(List<Cmd> schedule) {
      String check = System.getProperty(org.intentlog.fileio.AtomicFileLog.CHECK_TAIL_READS);
      System.setProperty(org.intentlog.fileio.AtomicFileLog.CHECK_TAIL_READS, "false");
      try {
        executeUnchecked(schedule);
      } finally {
        if (check == null) {
          System.clearProperty(org.intentlog.fileio.AtomicFileLog.CHECK_TAIL_READS);
        } else {
          System.setProperty(org.intentlog.fileio.AtomicFileLog.CHECK_TAIL_READS, check);
        }
      }
    }

    private void executeUnchecked(List<Cmd> schedule) {
      this.owner = Thread.currentThread();
      for (int i = 0; i < schedule.size(); i++) {
        Cmd cmd = schedule.get(i);
        Cmd next = i + 1 < schedule.size() ? schedule.get(i + 1) : null;
        boolean preempts = next != null && next.preempt >= 0 && writer(next) != writer(cmd);
        pending = preempts ? next : null;
        if (preempts) {
          i += 1;
        }
        runTopLevel(cmd);
        if (pending != null) {
          Cmd late = pending;
          pending = null;
          trace.append("  (did not preempt)\n");
          Statistics.label("preemption").collect("did not preempt");
          runTopLevel(late);
        }
      }
      checkCheckpoints();
      if (azure != null) {
        long segments = azure.appends().stream().map(InMemoryAppendBlobStore.Appended::name)
            .distinct().count();
        long seals = azure.appends().stream().filter(a -> a.block()[0] == 0).count();
        String limit = azureBlocks < 50_000 ? azureBlocks + " records" : AZURE_SEGMENT_RECORDS
            + " records";
        Statistics.label("Append Blob segments").collect(
            limit + ": " + (segments < 2 ? "1" : segments < 4 ? "2-3" : "4 or more"));
        Statistics.label("seal frames").collect(
            limit + ": " + (seals == 0 ? "none" : seals < 3 ? "1-2" : "3 or more"));
        long evicted = 0;
        for (Writer w : writers) {
          evicted += w.catalog.log().counts().evicted();
        }
        Statistics.label("updates evicted for their records to fit").collect(
            evicted == 0 ? "none" : evicted < 3 ? "1-2" : "3 or more");
      }
      // Conflicts by their reason's kind (ConflictEncodings.rejection: whole, concern, ref, ...)
      for (Writer w : writers) {
        w.catalog.log().counts().conflictReasons().forEach((why, count) -> {
          String kind = why.indexOf(':') > 0 ? why.substring(0, why.indexOf(':')) : why;
          for (long i = 0; i < count; i++) {
            Statistics.label("conflicts by reason").collect(kind);
          }
        });
      }
      long pageOuts = pageOuts();
      Statistics.label("PageOut records").collect(
          pageOuts == 0 ? "none" : pageOuts < 4 ? "1-3" : "4 or more");
      Statistics.label("most sealed blocks of a table").collect(
          mostSealed < 3 ? String.valueOf(mostSealed) : "3 or more");
      // A paged job (a round's faultA2 and faultB2, at blocks of 2 and a window of 1) must seal: a
      // run that seals nothing tests none of the paging, whatever it finds. Judged from 100 tries
      // on (a smoke run's 3 may all seal nothing)
      Statistics.label("a table sealed a block").collect(mostSealed > 0);
      if (SEALING_FLOOR > 0) {
        Statistics.label("a table sealed a block").coverage(c -> c.check(true)
            .count((sealed, all) -> all < 100 || 100.0 * sealed / all >= SEALING_FLOOR));
      }
      drain();
    }

    /**
     * The drain (the 2026-11-17 review's A4, oracles 2 and 3 of docs/SNAPSHOT_PAGING.md "Testing"),
     * on storage that no longer fails: a compaction lands, its root's pages put first, as it must
     * whatever puts of them failed or lost their replies before (a put that finds its page
     * succeeds: Bug 116); a sweep from a fresh read leaves every page the newest root names; and
     * once every table is dropped, a compaction and a sweep leave no snapshot page in the log's
     * page store: every page the run put was garbage by then, and found.
     */
    private void drain() {
      Map<String, String> properties = new HashMap<>(readerProperties);
      // On one object every commit replaces it with a checkpoint
      properties.put(LogCatalogProperties.MAX_APPEND_COUNT, "0");
      LogCatalog drainer = LocalCatalogMode.readerFor(io, CATALOG_LOCATION)
          .catalog(io, WAREHOUSE, CATALOG_NAME, properties);
      Namespace drained = NS;
      try {
        if (!drainer.namespaceExists(drained)) {
          drainer.createNamespace(drained);
        }
        drainer.setProperties(drained, Map.of(KEY_PREFIX + "drained", "1"));
        compact(drainer);
      } catch (RuntimeException e) {
        AssertionError error = fail("the drain's compaction on storage that no longer fails"
            + " failed: %s", e);
        error.initCause(e);
        throw error;
      }
      drainer.deleteOrphanPages();
      checkCheckpoints();
      int tables = 0;
      for (Namespace ns : drainer.listNamespaces(Namespace.empty())) {
        for (TableIdentifier id : drainer.listTables(ns)) {
          drainer.dropTable(id, true);
          tables += 1;
        }
      }
      compact(drainer);
      List<String> swept = drainer.deleteOrphanPages();
      List<String> left = new ArrayList<>();
      for (String name : pageNames()) {
        if (SnapshotPages.parseName(name) != null) {
          left.add(name);
        }
      }
      Statistics.label("the drain swept snapshot pages").collect(
          swept.stream().anyMatch(name -> SnapshotPages.parseName(name) != null));
      check(left.isEmpty(), "with its %d tables dropped, the catalog compacted and swept, the log"
          + " still holds snapshot pages %s", tables, left);
    }

    /** Compacts the catalog's log through {@code catalog}, if it is not one object (which did). */
    private void compact(LogCatalog catalog) {
      if (sharedLog != null || azure != null) {
        catalog.log().compact();
      }
    }

    /** The names of the pages the catalog's log stores now. */
    private List<String> pageNames() {
      if (sharedLog != null) {
        return new ArrayList<>(sharedLog.storedPages().keySet());
      }
      String prefix = CATALOG_LOCATION + ".committed/";
      List<String> names = new ArrayList<>();
      io.listPrefix(prefix).forEach(file -> names.add(file.location().substring(prefix.length())));
      return names;
    }

    /** The PageOut records the catalog's log ever held: the committed set's pages sealed. */
    private long pageOuts() {
      List<byte[]> records = new ArrayList<>();
      if (sharedLog != null) {
        records.addAll(sharedLog.records());
      } else if (azure != null) {
        azureHistory().records().forEach(landed -> records.add(landed.record()));
      } else {
        for (InMemoryAtomicFileIO.Write write : io.history()) {
          if (write.location().equals(CATALOG_LOCATION)) {
            records.addAll(AtomicFileLog.decode(write.after()).records());
          }
        }
      }
      return records.stream().map(java.nio.ByteBuffer::wrap).distinct()
          .filter(b -> LogEnvelope.decodeRecord(b.array()).isPageOut()).count();
    }

    private Writer writer(Cmd cmd) {
      return writers[cmd.writer % writers.length];
    }

    private void runTopLevel(Cmd cmd) {
      requests = 0;
      wrote = false;
      lateRequests = 0;
      settled = false;
      beforeLate = null;
      lateOp = null;
      effectApplied = false;
      writes = 0;
      reads = 0;
      fired.clear();
      preempting.clear();
      writePlan = faults ? cmd.writePlan() : List.of();
      failingRead = faults ? cmd.failingRead() : -1;
      readFault = cmd.readFaultKind();
      plainPlans = faults ? cmd.plainPlans() : List.of();
      java.util.Arrays.fill(plainRequests, 0);
      running = cmd;
      swept = false;
      pageGets = 0;
      run(cmd, true);
      running = null;
      Writer w = writer(cmd);
      if (w.kept == null) {
        w.kept = w.catalog.log().newest();
      }
    }

    // ---------------- storage hooks ----------------

    private Fault onWrite(String location, AtomicOutputFile.Strategy strategy) {
      if (!armed || Thread.currentThread() != owner) {
        return Fault.NONE;
      }
      sweepAfterPagePut();
      if (!wrote) {
        preemptAt(requests++);
        wrote = true;
      } else {
        preemptLate();
      }
      Fault fault = writes < writePlan.size() ? writePlan.get(writes) : Fault.NONE;
      if (fault != Fault.NONE) {
        fired.add("write #" + writes + " " + fault);
      }
      writes += 1;
      return fault;
    }

    private ReadFault onRead(String location) {
      if (!armed || Thread.currentThread() != owner) {
        return ReadFault.NONE;
      }
      if (location.equals(CATALOG_LOCATION) && !wrote) {
        preemptAt(requests++);
      } else if (wrote) {
        preemptLate();
      }
      if (location.endsWith(".metadata.json")) {
        return ReadFault.NONE; // Iceberg retries these after 100 ms
      }
      int read = reads++;
      if (read != failingRead) {
        return ReadFault.NONE;
      }
      fired.add("read #" + read + " " + readFault + " of " + location);
      return readFault;
    }

    /**
     * What a plain request meets ({@link Cmd#plainPlans}): a plan's fault if it is the planned one
     * of its class, counting the running top-level command's requests of each. {@code page}: a put
     * of a log's page store ({@code <log>.committed/}, or a shared log's pages).
     */
    private Fault onPlain(InMemoryAtomicFileIO.PlainRequest request, String location,
        boolean page) {
      if (!armed || Thread.currentThread() != owner) {
        return Fault.NONE;
      }
      if (wrote) {
        preemptLate();
      }
      PlainClass of;
      switch (request) {
        case PUT:
          of = page ? PlainClass.PAGE_PUT : PlainClass.OTHER_PUT;
          break;
        case DELETE:
          of = PlainClass.DELETE;
          break;
        default:
          of = PlainClass.LIST;
      }
      int n = plainRequests[of.ordinal()]++;
      for (PlainPlan plan : plainPlans) {
        if (plan.of == of && plan.index == n) {
          fired.add(of + " #" + n + " " + plan.fault + " of " + location);
          Statistics.label("plain fault fired").collect(of + " " + plan.fault);
          return plan.fault;
        }
      }
      return Fault.NONE;
    }

    /**
     * Runs a pending {@link Kind#SWEEP} now, as the running command sends a conditional write (or a
     * shared log's checkpoint) after it put a page: before the root or record that names the page
     * lands, from a replica that may not have sealed it.
     */
    private void sweepAfterPagePut() {
      if (!armed || Thread.currentThread() != owner
          || plainRequests[PlainClass.PAGE_PUT.ordinal()] == 0) {
        return;
      }
      if (pending != null && pending.kind == Kind.SWEEP) {
        runPending("after a page put");
      } else if (running != null && running.sweepsAfterPagePut() && !swept) {
        swept = true;
        Cmd preempted = pending;
        // the next writer's, from its kept replica (b 2) or its newest (b 1)
        pending = new Cmd(Kind.SWEEP, running.writer + 1, 0, running.pageFault == 4 ? 2 : 1, 0,
            false, 0, 0, 0, 0, 0);
        runPending("after a page put");
        pending = preempted;
      }
    }

    /**
     * What a request of the segmented log's segments meets (its catalog object's requests meet
     * the store's faults as one object's do): its reads of the segments are the catalog's reads,
     * which a preempting command may run before, as before a read of the catalog object. Every
     * write is one of the plan's: the records' appends, seal frames, new segments, and the sweep's
     * deletes. A blind append has no conflict to report, so a write that the plan has land as a
     * conflict lands with its reply lost.
     */
    private InMemoryAppendBlobStore.Fault onAzure(InMemoryAppendBlobStore.Op op, String name) {
      switch (op) {
        case LIST:
          return InMemoryAppendBlobStore.Fault.NONE;
        case READ:
        case PROPERTIES:
          return onRead(CATALOG_LOCATION) == ReadFault.NONE
              ? InMemoryAppendBlobStore.Fault.NONE : InMemoryAppendBlobStore.Fault.FAIL_BEFORE;
        default:
          switch (onWrite(CATALOG_LOCATION, AtomicOutputFile.Strategy.APPEND)) {
            case NONE:
              return InMemoryAppendBlobStore.Fault.NONE;
            case FAIL_BEFORE:
              return InMemoryAppendBlobStore.Fault.FAIL_BEFORE;
            case THROTTLED:
              return InMemoryAppendBlobStore.Fault.THROTTLED;
            case LANDED_THROTTLED:
              return InMemoryAppendBlobStore.Fault.LANDED_THROTTLED;
            default: // LOST_REPLY, LOST_REPLY_RUNTIME, LANDED_AS_CONFLICT
              return InMemoryAppendBlobStore.Fault.LOST_REPLY;
          }
      }
    }

    /** The segmented log as its stores hold it now, every write they ever took included. */
    private SegmentedLogHistory azureHistory() {
      return LocalCatalogMode.azureHistory(
          io, new LocalCatalogMode.AzureStore(CATALOG_LOCATION, azure));
    }

    /** Runs the pending command at request {@code request}, if that is its; a sweep waits. */
    private void preemptAt(int request) {
      if (pending != null && pending.kind != Kind.SWEEP && pending.preempt == request) {
        runPending("at request #" + request);
      }
    }

    /**
     * Runs the pending command at the running one's request after its first conditional write that
     * it names, if any ({@link #LATE}). The running command's record may have landed by then: if
     * the catalog holds its effect, it is settled first, as acknowledged ({@link #settleEarly}), so
     * the pending command is judged on a model that holds it.
     */
    private void preemptLate() {
      if (pending == null || pending.kind == Kind.SWEEP || pending.preempt < LATE) {
        return;
      }
      int request = lateRequests++;
      if (pending.preempt - LATE != request) {
        return;
      }
      armed = false;
      try {
        settleEarly();
      } finally {
        armed = true;
      }
      Statistics.label("a late preemption's command")
          .collect(running.kind + (settled ? ", landed" : ", not landed"));
      Model before = model; // resolve replaces it when the preempting command's effect applies
      Model unsettled = settled ? null : copy(model);
      lastNestedOp = null;
      runPending("at request #" + request + " after its first write"
          + (settled ? ", its record landed" : ""));
      if (!settled) {
        beforeLate = unsettled;
        lateOp = lastNestedOp;
        lateTookEffect = model != before;
      }
    }

    /**
     * Whether the catalog holds the running command's effect applied before the late preempting
     * command's, not after: its record landed before that command ran, changing nothing a check
     * reads then (a removal of a property the namespace lacked), so it was not settled. If so, sets
     * the model to that order, and resolve applies nothing more. The order is inferred from the
     * catalog's state, as settleEarly infers a landing: a record that landed after the preempting
     * command and lost its effect would read the same, an interleaving only late preemptions of a
     * command whose effect is void where it lands reach.
     */
    private boolean lateOrder(Op op) {
      Model now = actual();
      Model after = copy(model);
      try {
        op.effect.apply(after, now);
        if (diff(after, now) == null) {
          return false;
        }
      } catch (IllegalStateException e) { // an IsolationViolation too: try the other order
      }
      Model first = copy(beforeLate);
      try {
        op.effect.apply(first, now);
        if (lateTookEffect && lateOp != null) {
          lateOp.effect.apply(first, now);
        }
      } catch (IllegalStateException e) {
        return false;
      }
      if (diff(first, now) != null) {
        return false;
      }
      trace.append("    (its record landed before the command that preempted it ran)\n");
      Statistics.label("a late preemption's order").collect("the preempted command first");
      model = first;
      return true;
    }

    /**
     * Settles the running command as acknowledged now if its record has landed: the catalog changed
     * since the last check (a table's metadata, which the model does not hold all of, or what the
     * model holds), and holds the model with its effect. Otherwise the catalog must be as the last
     * check left it, and the model's.
     */
    private void settleEarly() {
      Model now = actual();
      Model with = copy(model);
      String applies = null;
      try {
        runningOp.effect.apply(with, now);
      } catch (IllegalStateException e) { // an IsolationViolation too
        applies = notApplied(e);
      }
      String withDiff = applies != null ? applies : diff(with, now);
      String withoutDiff = diff(model, now);
      boolean changed = withoutDiff != null || !sameMetadata(lastActual, now);
      if (!changed) {
        return; // not landed yet
      }
      check(withDiff == null, "%s changed the catalog after its first write, but not as its effect"
          + " would (%s): %s", runningOp, withDiff,
          withoutDiff != null ? withoutDiff : "the metadata of a table changed");
      trace.append("    (its record landed: settled as acknowledged)\n");
      resolve(running, runningOp, Outcome.ACKNOWLEDGED, "", null, false, fired.isEmpty());
      settled = true;
    }

    /** Whether the two checks found the same tables with the same metadata. */
    private static boolean sameMetadata(Model a, Model b) {
      if (!a.keySet().equals(b.keySet())) {
        return false;
      }
      for (Map.Entry<TableIdentifier, TableState> e : a.entrySet()) {
        if (!Objects.equals(e.getValue().json, b.get(e.getKey()).json)) {
          return false;
        }
      }
      return true;
    }

    private void runPending(String where) {
      Cmd nested = pending;
      pending = null;
      if (nested.kind != Kind.SWEEP) {
        preempting.add(nested.kind); // a sweep changes no table: it races nothing
      }
      armed = false;
      try {
        trace.append(String.format("  preempted %s by%n", where));
        Statistics.label("preemption").collect(where);
        run(nested, false);
      } finally {
        armed = true;
      }
    }

    // ---------------- operations ----------------

    /** A random value of {@code size} characters, which compresses little. */
    private String randomValue(int size) {
      java.util.Random random = new java.util.Random(nextValue++);
      String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
      StringBuilder value = new StringBuilder(size);
      for (int i = 0; i < size; i++) {
        value.append(alphabet.charAt(random.nextInt(alphabet.length())));
      }
      return value.toString();
    }

    /**
     * Writes the one-byte object a data or delete file names, as an engine writes a file before it
     * commits it, so a delete of a file a snapshot still reaches shows (the review's A7). Not a
     * request the faults reach: the engine's, not the catalog's.
     */
    private String written(String path) {
      io.seed(path, new byte[] {1});
      return path;
    }

    private DataFile newFile() {
      return DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath(written("mem:///data/f" + nextFile++ + ".parquet"))
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .build();
    }

    /** {@code file} in {@code table}'s current spec, which may have changed since it was named. */
    private static DataFile inSpec(Table table, DataFile file) {
      return DataFiles.builder(table.spec())
          .withPath(file.location())
          .withFileSizeInBytes(file.fileSizeInBytes())
          .withRecordCount(file.recordCount())
          .withPartitionPath(partitionPath(table.spec()))
          .build();
    }

    /** The one partition files are written to: 1 for identity, 0 for bucket and truncate. */
    private static String partitionPath(PartitionSpec spec) {
      List<String> path = new ArrayList<>();
      for (org.apache.iceberg.PartitionField field : spec.fields()) {
        String transform = field.transform().toString();
        String value = transform.equals("identity") ? "1" : transform.equals("void") ? "null" : "0";
        path.add(field.name() + "=" + value);
      }
      return String.join("/", path);
    }

    /**
     * The live data file at {@code path} in {@code table}, or null. A stale handle cannot plan once
     * an expiry has removed its current snapshot: a rejection.
     */
    private static DataFile liveFile(Table table, String path) {
      try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
        for (FileScanTask task : tasks) {
          if (task.file().location().equals(path)) {
            return task.file();
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      } catch (org.apache.iceberg.exceptions.NotFoundException e) {
        throw new ValidationException(e, "the handle's snapshot was expired since: %s", path);
      }
      return null;
    }

    private static Table create(Catalog catalog, TableIdentifier id, int formatVersion) {
      return catalog
          .buildTable(id, SCHEMA)
          .withProperty(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
          .create();
    }

    private TableIdentifier pickTable(int index) {
      List<TableIdentifier> tables = new ArrayList<>(model.keySet());
      return index >= 8 || tables.isEmpty()
          ? TABLES.get(index % TABLES.size())
          : tables.get(index % tables.size());
    }

    /**
     * The manifests and manifest list files of every snapshot of {@code id} as the checker reads it
     * now, before a drop that purges it: what the purge deletes (CatalogUtil.dropTableData). None
     * if the table cannot be read (it does not exist, and the drop is rejected).
     */
    private Set<String> purgeable(TableIdentifier id) {
      Set<String> files = new TreeSet<>();
      try {
        Table table = checker.loadTable(id);
        for (Snapshot snapshot : table.snapshots()) {
          String list = snapshot.manifestListLocation();
          if (list != null && !list.startsWith("inline://")) {
            files.add(list);
          }
          for (ManifestFile manifest : snapshot.allManifests(table.io())) {
            files.add(manifest.path());
          }
        }
      } catch (RuntimeException e) {
        return Set.of();
      }
      return files;
    }

    /**
     * Checks that a purge of {@code id} that landed deleted {@code purgeable}, the manifests and
     * lists its table had before the drop (Bug 94: a compaction after the drop collected the
     * table's pages first, and the purge, reading its history after, deleted nothing). Not checked
     * after an injected fault, which may fail a delete or the read of a list, nor after an expiry
     * that ran first and kept the files of the snapshots it removed, which the purge then no longer
     * names.
     */
    private void checkPurged(TableIdentifier id, Set<String> purgeable) {
      if (!fired.isEmpty()) {
        Statistics.label("purge").collect("not checked: a fault");
        return;
      }
      if (preempting.contains(Kind.EXPIRE_SNAPSHOTS) || preempting.contains(Kind.EXPIRE_OLD)) {
        Statistics.label("purge").collect("not checked: an expiry ran first");
        return;
      }
      List<String> left = new ArrayList<>();
      for (String file : purgeable) {
        if (io.newInputFile(file).exists()) {
          left.add(file);
        }
      }
      Statistics.label("purge").collect(purgeable.isEmpty() ? "checked: no files" : "checked");
      check(left.isEmpty(), "the purge of %s left %s of its %s manifests and lists: %s", id,
          left.size(), purgeable.size(), left);
    }

    /** The snapshots of {@code table}, oldest first. */
    private static List<Snapshot> oldestFirst(Table table) {
      List<Snapshot> history = new ArrayList<>();
      table.snapshots().forEach(history::add);
      history.sort(Comparator.comparingLong(Snapshot::timestampMillis)
          .thenComparingLong(Snapshot::sequenceNumber));
      return history;
    }

    private Table handle(Writer w, TableIdentifier id, boolean fresh) {
      Table table = fresh ? null : w.handles.get(id);
      if (table == null) {
        table = w.catalog.loadTable(id);
        w.handles.put(id, table);
      }
      return table;
    }

    /**
     * The snapshots of {@code id} the last check found behind the sealing line, live in its sealed
     * blocks (docs/SNAPSHOT_PAGING.md).
     */
    private Set<Long> sealedIds(TableIdentifier id) {
      CatalogState state = lastState;
      Integer tblId = state == null ? null : state.tableId(id);
      Set<Long> ids = new HashSet<>();
      if (tblId != null) {
        for (SnapshotPages.SealedBlock block : state.sealedBlocks(tblId).values()) {
          for (long live : block.liveIds) {
            ids.add(live);
          }
        }
      }
      return ids;
    }

    private boolean sealed(TableIdentifier id, long snapshotId) {
      return sealedIds(id).contains(snapshotId);
    }

    /**
     * A snapshot of {@code history} (oldest first, not empty) to move a ref to, as {@code b}
     * picks: one of those the last check found sealed, if any, else one of the three oldest.
     */
    private long target(TableIdentifier id, List<Snapshot> history, int b) {
      Set<Long> sealedIds = sealedIds(id);
      List<Snapshot> sealedOnes = new ArrayList<>(history);
      sealedOnes.removeIf(s -> !sealedIds.contains(s.snapshotId()));
      return sealedOnes.isEmpty()
          ? history.get(b % Math.min(3, history.size())).snapshotId()
          : sealedOnes.get(b % sealedOnes.size()).snapshotId();
    }

    /**
     * A table for a command aimed at the sealing line: one the last check found with sealed blocks
     * if there are any (by {@code index}), else as {@link #pickTable} picks.
     */
    private TableIdentifier pickSealedTable(int index) {
      List<TableIdentifier> sealedTables = new ArrayList<>();
      for (TableIdentifier id : model.keySet()) {
        if (!sealedIds(id).isEmpty()) {
          sealedTables.add(id);
        }
      }
      return sealedTables.isEmpty() ? pickTable(index)
          : sealedTables.get(index % sealedTables.size());
    }

    /** Concrete operands for {@code cmd}, or null if it does not apply. */
    private Op interpret(Cmd cmd, Writer w) {
      TableIdentifier id = cmd.kind == Kind.ROLLBACK || cmd.kind == Kind.TAG_OLD
          || cmd.kind == Kind.EXPIRE_OLD ? pickSealedTable(cmd.a) : pickTable(cmd.a);
      switch (cmd.kind) {
        case CREATE_TABLE:
          // Mostly a table that does not exist; else any, to race or collide.
          List<TableIdentifier> missing = new ArrayList<>(TABLES);
          missing.removeAll(model.keySet());
          TableIdentifier created =
              cmd.a >= 8 || missing.isEmpty()
                  ? TABLES.get(cmd.a % TABLES.size())
                  : missing.get(cmd.a % missing.size());
          int version = 2 + cmd.b % 2;
          return new Op(
              "createTable(" + created + ", v" + version + ")",
              () -> create(w.catalog, created, version),
              (m, actual) -> {
                if (m.containsKey(created) || m.views.containsKey(created)
                    || !actual.containsKey(created)
                    || !m.namespaces.containsKey(created.namespace())) {
                  throw new IllegalStateException("cannot create " + created);
                }
                m.put(created, new TableState(actual.get(created).uuid, version));
              });
        case DROP_TABLE:
          // Purging half the time: its deletes must spare every other table's files, and leave
          // none of the table's manifests and lists (Bug 94)
          boolean purge = cmd.b % 2 == 1;
          Set<String> purgeable = purge ? purgeable(id) : Set.of();
          boolean[] purged = {false};
          Op drop = new Op(
              "dropTable(" + id + (purge ? ", purge" : "") + ")",
              () -> {
                if (!w.catalog.dropTable(id, purge)) {
                  throw new NoSuchTableException("dropTable returned false: %s", id);
                }
                purged[0] = purge;
              },
              (m, actual) -> {
                existing(m, id);
                m.remove(id);
                if (purged[0]) {
                  checkPurged(id, purgeable);
                }
              });
          drop.keeping = purge ? List.of() : List.of(id);
          return drop;
        case RENAME_TABLE:
          TableIdentifier to = TABLES.get(cmd.b % TABLES.size());
          if (to.equals(id)) {
            return null;
          }
          return new Op(
              "renameTable(" + id + ", " + to + ")",
              () -> w.catalog.renameTable(id, to),
              (m, actual) -> {
                TableState state = existing(m, id);
                if (m.containsKey(to) || m.views.containsKey(to)) {
                  throw new IllegalStateException(to + " exists");
                }
                if (!m.namespaces.containsKey(to.namespace())) {
                  throw new IllegalStateException(to.namespace() + " does not exist");
                }
                m.remove(id);
                m.put(to, state);
              });
        case FAST_APPEND:
        case MERGE_APPEND:
          DataFile appended = newFile();
          boolean fast = cmd.kind == Kind.FAST_APPEND;
          return new Op(
              (fast ? "fastAppend(" : "mergeAppend(") + id + ", " + appended.location() + ")",
              () -> {
                Table table = handle(w, id, cmd.fresh);
                (fast ? table.newFastAppend() : table.newAppend())
                    .appendFile(inSpec(table, appended))
                    .commit();
              },
              (m, actual) -> {
                TableState state = existing(m, id);
                state.files.add(appended.location());
                if (!fast) {
                  ageOutDeletes(state, actual.get(id));
                }
              });
        case DELETE_FILE:
          // A quarter of the time a file the table does not have: a snapshot with no changes.
          TableState target = model.get(id);
          String path =
              target == null || target.files.isEmpty() || cmd.b % 4 == 3
                  ? "mem:///data/missing.parquet"
                  : target.sortedFiles().get(cmd.b % target.files.size());
          return new Op(
              "deleteFile(" + id + ", " + path + ")",
              () -> handle(w, id, cmd.fresh).newDelete().deleteFile(path).commit(),
              (m, actual) -> {
                TableState state = existing(m, id);
                if (state.files.removeIf(path::equals)) {
                  state.deletes.values().removeIf(path::equals); // its deletion vector
                }
                ageOutDeletes(state, actual.get(id));
              });
        case ROW_DELTA:
          TableState deleting = model.get(id);
          // A position delete for a live data file without a deletion vector, else an equality one.
          String dataPath = null;
          if (cmd.b % 2 == 1 && deleting != null) {
            for (String f : deleting.sortedFiles()) {
              if (!deleting.deletes.containsValue(f) && (dataPath == null || cmd.b % 4 == 3)) {
                dataPath = f;
              }
            }
          }
          String referenced = dataPath;
          int deleteFile = nextFile++;
          // A deletion vector if the table is v3 when the operation runs, which may be a table
          // another writer created since the model was read.
          String[] deletePath = {null};
          boolean[] dv = {false};
          return new Op(
              String.format("rowDelta(%s, %s%d%s)", id, referenced == null ? "eq" : "pos/dv",
                  deleteFile, referenced == null ? "" : " for " + referenced),
              () -> {
                Table table = handle(w, id, cmd.fresh);
                dv[0] = referenced != null
                    && ((HasTableOperations) table).operations().current().formatVersion() >= 3;
                deletePath[0] = written("mem:///data/"
                    + (referenced == null ? "eq" : dv[0] ? "dv" : "pos") + deleteFile
                    + (dv[0] ? ".puffin" : ".parquet"));
                org.apache.iceberg.FileMetadata.Builder delete =
                    org.apache.iceberg.FileMetadata.deleteFileBuilder(table.spec())
                        .withPath(deletePath[0])
                        .withFileSizeInBytes(10)
                        .withRecordCount(1);
                if (referenced == null) {
                  delete.ofEqualityDeletes(1).withPartitionPath(partitionPath(table.spec()));
                } else {
                  DataFile data = liveFile(table, referenced);
                  if (data == null) {
                    throw new ValidationException("%s is not live", referenced);
                  }
                  delete = org.apache.iceberg.FileMetadata.deleteFileBuilder(
                          table.specs().get(data.specId()))
                      .withPath(deletePath[0])
                      .withFileSizeInBytes(10)
                      .withRecordCount(1)
                      .ofPositionDeletes()
                      .withPartition(data.partition());
                  if (dv[0]) {
                    delete.withFormat(org.apache.iceberg.FileFormat.PUFFIN)
                        .withReferencedDataFile(referenced)
                        .withContentOffset(4)
                        .withContentSizeInBytes(6);
                  }
                }
                org.apache.iceberg.DeleteFile built = delete.build();
                try {
                  table.newRowDelta().addDeletes(built).commit();
                } catch (IllegalArgumentException e) {
                  // Iceberg checks a delete file against the format version of the metadata the
                  // commit lands on: a position delete for a v2 table is not one for a table
                  // another writer has upgraded to v3 since (A4 in docs/ICEBERG_ISSUES.md).
                  if (dv[0] || e.getMessage() == null
                      || !e.getMessage().startsWith("Must use DVs for position deletes")) {
                    throw e;
                  }
                  throw new ValidationException(e, "%s: upgraded to v3 since the handle", id);
                }
              },
              (m, actual) -> {
                TableState state = existing(m, id);
                if (deletePath[0] == null) {
                  throw new IllegalStateException("the row delta did not run");
                }
                state.deletes.put(deletePath[0], dv[0] ? referenced : "");
                ageOutDeletes(state, actual.get(id));
              });
        case ADD_PARTITION_FIELD:
          return new Op(
              "addPartitionField(" + id + ", " + new String[] {"id", "bucket", "truncate"}[cmd.b % 3]
                  + ")",
              () -> {
                org.apache.iceberg.UpdatePartitionSpec update =
                    handle(w, id, cmd.fresh).updateSpec();
                try {
                  switch (cmd.b % 3) {
                    case 0:
                      update.addField("id");
                      break;
                    case 1:
                      update.addField(org.apache.iceberg.expressions.Expressions.bucket("id", 4));
                      break;
                    default:
                      update.addField(
                          org.apache.iceberg.expressions.Expressions.truncate("id", 10));
                  }
                } catch (IllegalArgumentException e) {
                  throw new ValidationException(e, "%s", e.getMessage()); // already a field
                }
                update.commit();
              },
              (m, actual) -> existing(m, id));
        case REMOVE_PARTITION_FIELD:
          return new Op(
              "removePartitionField(" + id + ")",
              () -> {
                Table table = handle(w, id, cmd.fresh);
                List<org.apache.iceberg.PartitionField> fields = table.spec().fields();
                if (fields.isEmpty()) {
                  throw new ValidationException("%s is not partitioned", id);
                }
                table.updateSpec().removeField(fields.get(cmd.b % fields.size()).name()).commit();
              },
              (m, actual) -> existing(m, id));
        case CREATE_TAG:
        case CREATE_BRANCH:
          boolean tag = cmd.kind == Kind.CREATE_TAG;
          String refName = (tag ? "t" : "b") + cmd.b % 2;
          // What the ref holds is observed: it names the current snapshot of the writer's handle
          // when the operation runs, which may be stale. A tag's content must never change after.
          // The snapshot is the one the operation read (A2)
          long[] named = {-1};
          return new Op(
              (tag ? "createTag(" : "createBranch(") + id + ", " + refName + ")",
              () -> {
                Table table = handle(w, id, cmd.fresh);
                if (table.currentSnapshot() == null) {
                  throw new ValidationException("%s has no snapshot", id);
                }
                long snapshotId = table.currentSnapshot().snapshotId();
                named[0] = snapshotId;
                try {
                  if (tag) {
                    table.manageSnapshots().createTag(refName, snapshotId).commit();
                  } else {
                    table.manageSnapshots().createBranch(refName, snapshotId).commit();
                  }
                } catch (IllegalArgumentException e) {
                  throw new ValidationException(e, "%s", e.getMessage()); // the ref exists
                }
              },
              (m, actual) -> {
                TableState state = existing(m, id);
                TableState now = actual.get(id);
                if (state.branches.containsKey(refName) || state.tags.containsKey(refName)
                    || now == null || !(tag ? now.tags : now.branches).containsKey(refName)) {
                  throw new IllegalStateException("cannot create " + refName);
                }
                if (!Objects.equals(now.refIds.get(refName), named[0])) {
                  throw new IllegalStateException(String.format("%s names snapshot %s, not %d,"
                      + " which its operation read", refName, now.refIds.get(refName), named[0]));
                }
                if (tag) {
                  state.tags.put(refName, new TreeSet<>(now.tags.get(refName)));
                  state.tagSnapshots.put(refName, now.tagSnapshots.get(refName));
                } else {
                  state.branches.put(refName, new TreeSet<>(now.branches.get(refName)));
                }
              });
        case BRANCH_APPEND:
          String branch = "b" + cmd.b % 2;
          DataFile branchFile = newFile();
          return new Op(
              "branchAppend(" + id + ", " + branch + ", " + branchFile.location() + ")",
              () -> {
                Table table = handle(w, id, cmd.fresh);
                table.newFastAppend().appendFile(inSpec(table, branchFile)).toBranch(branch)
                    .commit();
              },
              (m, actual) -> {
                // Appending to a branch the table does not have creates it from main.
                TableState state = existing(m, id);
                state.branches.computeIfAbsent(branch, k -> new TreeSet<>(state.files))
                    .add(branchFile.location());
              });
        case FAST_FORWARD:
          String ffTarget = "b" + cmd.b % 2;
          return new Op(
              "fastForward(" + id + ", main to " + ffTarget + ")",
              () -> {
                try {
                  handle(w, id, cmd.fresh).manageSnapshots().fastForwardBranch("main", ffTarget)
                      .commit();
                } catch (IllegalArgumentException e) {
                  throw new ValidationException(e, "%s", e.getMessage()); // no such branch
                }
              },
              (m, actual) -> {
                TableState state = existing(m, id);
                Set<String> files = state.branches.get(ffTarget);
                if (files == null) {
                  throw new IllegalStateException("no branch " + ffTarget);
                }
                state.files.clear();
                state.files.addAll(files);
                // Main takes the branch's delete files, which the model does not follow.
                state.deletes.clear();
                state.deletes.putAll(actual.get(id).deletes);
              });
        case ROLLBACK:
          boolean toAncestor = cmd.c % 2 == 0;
          long[] mainTarget = {-1};
          boolean[] noOp = {false};
          return new Op(
              (toAncestor ? "rollbackTo(" : "setCurrentSnapshot(") + id + ", pick " + cmd.b + ")",
              () -> {
                Table table = handle(w, id, cmd.fresh);
                List<Snapshot> history = oldestFirst(table);
                if (toAncestor) {
                  Snapshot current = table.currentSnapshot();
                  Set<Long> ancestors = current == null ? Set.of() : new HashSet<>(
                      org.apache.iceberg.util.SnapshotUtil.ancestorIds(current, table::snapshot));
                  history.removeIf(s -> !ancestors.contains(s.snapshotId()));
                }
                if (history.isEmpty()) {
                  throw new ValidationException("%s has no snapshot to move main to", id);
                }
                long snapshotId = target(id, history, cmd.b);
                mainTarget[0] = snapshotId;
                Statistics.label("a ref moved behind the sealing line")
                    .collect(sealed(id, snapshotId));
                try {
                  // SnapshotManager refreshes the table: its operation is on main as it is now,
                  // which a stale handle's pick may already name
                  ManageSnapshots manage = table.manageSnapshots();
                  noOp[0] = table.currentSnapshot() != null
                      && table.currentSnapshot().snapshotId() == snapshotId;
                  (toAncestor ? manage.rollbackTo(snapshotId)
                      : manage.setCurrentSnapshot(snapshotId)).commit();
                } catch (IllegalArgumentException e) {
                  throw new ValidationException(e, "%s", e.getMessage()); // expired since
                }
              },
              (m, actual) -> {
                TableState state = existing(m, id);
                if (noOp[0]) {
                  // main named the snapshot already where the operation refreshed the table:
                  // Iceberg commits nothing, and the operation takes effect there, before any
                  // command that preempted it and moved main since (round p25-r0)
                  return;
                }
                TableState now = actual.get(id);
                if (now == null || !Objects.equals(now.refIds.get(SnapshotRef.MAIN_BRANCH),
                    mainTarget[0])) {
                  throw new IllegalStateException(String.format("main names snapshot %s, not %d,"
                      + " which its operation chose", now == null ? null
                      : now.refIds.get(SnapshotRef.MAIN_BRANCH), mainTarget[0]));
                }
                // Main holds what that snapshot holds, which the model does not follow
                state.files.clear();
                state.files.addAll(now.files);
                state.deletes.clear();
                state.deletes.putAll(now.deletes);
              });
        case TAG_OLD:
          String oldTag = "t" + cmd.c % 2;
          long[] tagged = {-1};
          boolean[] tagReplaced = {false};
          return new Op(
              "tagOld(" + id + ", " + oldTag + ", pick " + cmd.b + ")",
              () -> {
                Table table = handle(w, id, cmd.fresh);
                List<Snapshot> history = oldestFirst(table);
                if (history.isEmpty()) {
                  throw new ValidationException("%s has no snapshot", id);
                }
                long snapshotId = target(id, history, cmd.b);
                tagged[0] = snapshotId;
                Statistics.label("a ref moved behind the sealing line")
                    .collect(sealed(id, snapshotId));
                tagReplaced[0] = table.refs().containsKey(oldTag);
                try {
                  if (tagReplaced[0]) {
                    table.manageSnapshots().replaceTag(oldTag, snapshotId).commit();
                  } else {
                    table.manageSnapshots().createTag(oldTag, snapshotId).commit();
                  }
                } catch (IllegalArgumentException e) {
                  throw new ValidationException(e, "%s", e.getMessage()); // a ref, or expired
                }
              },
              (m, actual) -> {
                TableState state = existing(m, id);
                TableState now = actual.get(id);
                if (now == null || !now.tags.containsKey(oldTag)
                    || tagReplaced[0] != state.tags.containsKey(oldTag)) {
                  throw new IllegalStateException(
                      (tagReplaced[0] ? "cannot replace " : "cannot create ") + oldTag);
                }
                if (!Objects.equals(now.refIds.get(oldTag), tagged[0])) {
                  throw new IllegalStateException(String.format("%s names snapshot %s, not %d,"
                      + " which its operation chose", oldTag, now.refIds.get(oldTag), tagged[0]));
                }
                state.tags.put(oldTag, new TreeSet<>(now.tags.get(oldTag)));
                state.tagSnapshots.put(oldTag, now.tagSnapshots.get(oldTag));
              });
        case REMOVE_REF:
          TableState holding = model.get(id);
          if (holding == null || holding.branches.isEmpty() && holding.tags.isEmpty()) {
            return null;
          }
          List<String> refs = new ArrayList<>(holding.branches.keySet());
          refs.addAll(holding.tags.keySet());
          String ref = refs.get(cmd.b % refs.size());
          boolean isTag = holding.tags.containsKey(ref);
          return new Op(
              "removeRef(" + id + ", " + ref + ")",
              () -> {
                try {
                  if (isTag) {
                    handle(w, id, cmd.fresh).manageSnapshots().removeTag(ref).commit();
                  } else {
                    handle(w, id, cmd.fresh).manageSnapshots().removeBranch(ref).commit();
                  }
                } catch (IllegalArgumentException e) {
                  throw new ValidationException(e, "%s", e.getMessage()); // no such ref
                }
              },
              (m, actual) -> {
                TableState state = existing(m, id);
                if ((isTag ? state.tags.remove(ref) : state.branches.remove(ref)) == null) {
                  throw new IllegalStateException("no ref " + ref);
                }
                state.tagSnapshots.remove(ref);
              });
        case SET_PROPERTY:
          // Now and then the metadata log's bound, to which replay trims its log (Window); the
          // model keeps only fz.* properties
          boolean bound = cmd.b % 6 == 5;
          String key = bound ? TableProperties.METADATA_PREVIOUS_VERSIONS_MAX
              : KEYS.get(cmd.b % KEYS.size());
          String value = bound ? String.valueOf(cmd.c % 4) : "v" + nextValue++;
          return new Op(
              "setProperty(" + id + ", " + key + "=" + value + ")",
              () -> handle(w, id, cmd.fresh).updateProperties().set(key, value).commit(),
              (m, actual) -> {
                TableState table = existing(m, id);
                if (!bound) {
                  table.props.put(key, value);
                }
              });
        case SET_BIG_PROPERTY:
          // At the default limit, a big property stays well inside it.
          int scale = Math.min(maxUpdateSize, 8192);
          int size = (int) (scale * new double[] {0.5, 1.2, 3}[cmd.b % 3]);
          String big = randomValue(size);
          return new Op(
              "setProperty(" + id + ", " + BIG_KEY + "=" + abbreviate(big) + ")",
              () -> handle(w, id, cmd.fresh).updateProperties().set(BIG_KEY, big).commit(),
              (m, actual) -> existing(m, id).props.put(BIG_KEY, big));
        case REMOVE_PROPERTY:
          String removed = cmd.b % 3 == 2 ? BIG_KEY : KEYS.get(cmd.b % 3);
          return new Op(
              "removeProperty(" + id + ", " + removed + ")",
              () -> handle(w, id, cmd.fresh).updateProperties().remove(removed).commit(),
              (m, actual) -> existing(m, id).props.remove(removed));
        case ADD_COLUMN:
          String column = "c" + nextValue++;
          return new Op(
              "addColumn(" + id + ", " + column + ")",
              () ->
                  handle(w, id, cmd.fresh)
                      .updateSchema()
                      .addColumn(column, Types.IntegerType.get())
                      .commit(),
              (m, actual) -> existing(m, id).columns.add(column));
        case UPGRADE_TABLE:
          return new Op(
              "upgrade(" + id + ", v3)",
              () -> {
                handle(w, id, cmd.fresh)
                    .updateProperties()
                    .set(TableProperties.FORMAT_VERSION, "3")
                    .commit();
              },
              (m, actual) -> existing(m, id).formatVersion = 3);
        case REPLACE_TABLE:
          boolean orCreate = cmd.b % 3 == 0;
          DataFile replacing = cmd.b % 2 == 1 ? newFile() : null;
          // Without a table, a create-or-replace creates one: record the table it builds.
          TableState built = new TableState(null, 0);
          return new Op(
              String.format("%s(%s%s)", orCreate ? "createOrReplace" : "replace", id,
                  replacing == null ? "" : ", " + replacing.location()),
              () -> {
                Catalog.TableBuilder builder = w.catalog.buildTable(id, SCHEMA);
                Transaction replace =
                    orCreate ? builder.createOrReplaceTransaction() : builder.replaceTransaction();
                if (replacing != null) {
                  replace.newFastAppend().appendFile(inSpec(replace.table(), replacing)).commit();
                }
                TableMetadata replacement = ((HasTableOperations) replace.table()).operations()
                    .current();
                built.formatVersion = replacement.formatVersion();
                replacement.properties().forEach((k, v) -> {
                  if (k.startsWith(KEY_PREFIX)) {
                    built.props.put(k, v);
                  }
                });
                replace.commitTransaction();
              },
              (m, actual) -> {
                TableState before = m.get(id);
                if (before == null && (!orCreate || !m.namespaces.containsKey(id.namespace()))) {
                  throw new IllegalStateException(id + " does not exist");
                }
                TableState now = actual.get(id);
                if (now == null) {
                  throw new IllegalStateException(id + " is missing");
                }
                if (before == null) {
                  TableState fresh = new TableState(now.uuid, built.formatVersion);
                  fresh.props.putAll(built.props);
                  if (replacing != null) {
                    fresh.files.add(replacing.location());
                  }
                  m.put(id, fresh);
                  return;
                }
                // A replace replaces main's data and the schema, and keeps the rest of the table
                // as it is when the replace commits, what other writers committed since it began
                // included: it rebuilds its replacement on that, or fails (Iceberg issue C23).
                TableState replaced = before.copy();
                replaced.files.clear();
                replaced.deletes.clear();
                replaced.maybeDropped.clear();
                replaced.columns.clear();
                if (replacing != null) {
                  replaced.files.add(replacing.location());
                }
                m.put(id, replaced);
              });
        case TABLE_TXN:
          DataFile txnFile = newFile();
          String txnKey = KEYS.get(cmd.b % KEYS.size());
          String txnValue = "v" + nextValue++;
          return new Op(
              String.format(
                  "tableTxn(%s, %s, %s=%s)", id, txnFile.location(), txnKey, txnValue),
              () -> {
                Transaction txn = handle(w, id, cmd.fresh).newTransaction();
                txn.newFastAppend().appendFile(inSpec(txn.table(), txnFile)).commit();
                txn.updateProperties().set(txnKey, txnValue).commit();
                txn.commitTransaction();
              },
              (m, actual) -> {
                TableState state = existing(m, id);
                state.files.add(txnFile.location());
                state.props.put(txnKey, txnValue);
              });
        case EXPIRE_SNAPSHOTS:
        case EXPIRE_OLD:
          boolean cleanMetadata = cmd.b % 2 == 1;
          boolean keepFiles = cmd.b / 2 % 2 == 1;
          boolean old = cmd.kind == Kind.EXPIRE_OLD;
          if (old && sealedIds(id).isEmpty()) {
            return null;
          }
          Expiry expiry = old ? Expiry.A_SNAPSHOT : Expiry.values()[cmd.c % 4];
          int operand = old ? cmd.c % 2 : cmd.c / 4;
          String expired;
          switch (expiry) {
            case OLDER_THAN_NOW:
              expired = "older than now, retaining " + (operand + 1);
              break;
            case OLDER_THAN_A_SNAPSHOT:
              expired = "older than snapshot #" + operand;
              break;
            case A_SNAPSHOT:
              expired = "snapshot #" + operand;
              break;
            default:
              expired = "by retention";
          }
          Set<Long> chosen = new TreeSet<>();
          Op expiring = new Op(
              "expireSnapshots(" + id + ", " + expired
                  + (cleanMetadata ? ", cleaning metadata" : "")
                  + (keepFiles ? ", keeping files" : "") + ")",
              () -> {
                Table table = handle(w, id, cmd.fresh);
                ExpireSnapshots expire = table.expireSnapshots()
                    .cleanExpiredFiles(!keepFiles)
                    .cleanExpiredMetadata(cleanMetadata);
                List<Snapshot> history = oldestFirst(table);
                if (old) {
                  // the sealed ones, if the handle has any
                  Set<Long> sealedIds = sealedIds(id);
                  List<Snapshot> sealedOnes = new ArrayList<>(history);
                  sealedOnes.removeIf(s -> !sealedIds.contains(s.snapshotId()));
                  history = sealedOnes.isEmpty() ? history : sealedOnes;
                }
                Snapshot picked = history.isEmpty() ? null : history.get(operand % history.size());
                if (expiry == Expiry.OLDER_THAN_NOW) {
                  expire.expireOlderThan(System.currentTimeMillis() + 1).retainLast(operand + 1);
                } else if (expiry == Expiry.OLDER_THAN_A_SNAPSHOT && picked != null) {
                  expire.expireOlderThan(picked.timestampMillis());
                } else if (expiry == Expiry.A_SNAPSHOT && picked != null) {
                  expire.expireSnapshotId(picked.snapshotId());
                  if (old) {
                    Statistics.label("an expiry behind the sealing line")
                        .collect(sealed(id, picked.snapshotId()));
                  }
                }
                Statistics.label("expiry").collect(expiry);
                try {
                  // What it expires on the table as it is now: what it removes, if nothing races
                  // it (A2)
                  expire.apply().forEach(s -> chosen.add(s.snapshotId()));
                  expire.commit();
                } catch (IllegalArgumentException e) {
                  if (e.getMessage() == null || !e.getMessage().startsWith("Cannot expire ")) {
                    throw e;
                  }
                  throw new ValidationException(e, "%s", e.getMessage()); // a ref holds it
                }
              },
              (m, actual) -> {
                // An expiry with nothing to expire writes nothing, and its file cleanup reads the
                // catalog after: an operation that preempts that read, and drops or renames the
                // table, runs after the expiry, which leaves it nothing to apply to.
                if (writes == 0 && !m.containsKey(id)) {
                  return;
                }
                TableState state = existing(m, id);
                if (expiry == Expiry.OLDER_THAN_NOW) {
                  state.staged.clear(); // no ref holds a staged append, and each is old enough
                }
                if (actual.containsKey(id)) {
                  state.ageOutRefs(actual.get(id));
                }
              });
          expiring.expiring = id;
          expiring.chosen = chosen;
          return expiring;
        case SET_RETENTION:
          // An age of 1 ms expires what it covers at the next expiry; a day, never in a run.
          int setting = cmd.c % 4;
          int amount = cmd.c / 4;
          long age = amount == 0 ? 1 : TimeUnit.DAYS.toMillis(1);
          // a ref the table has: a branch, main too, or for a maximum age, a branch or tag
          TableState retaining = model.get(id);
          List<String> branchNames = new ArrayList<>(List.of(SnapshotRef.MAIN_BRANCH));
          List<String> refNames = new ArrayList<>();
          if (retaining != null) {
            branchNames.addAll(retaining.branches.keySet());
            refNames.addAll(retaining.branches.keySet());
            refNames.addAll(retaining.tags.keySet());
          }
          if (setting == 2 && refNames.isEmpty()) {
            return null; // main has no maximum age
          }
          String retained = setting == 2
              ? refNames.get(cmd.b % refNames.size())
              : branchNames.get(cmd.b % branchNames.size());
          String tableKey = List.of(TableProperties.MAX_REF_AGE_MS,
                  TableProperties.MIN_SNAPSHOTS_TO_KEEP, TableProperties.MAX_SNAPSHOT_AGE_MS)
              .get(amount);
          String tableValue = amount == 1 ? "2" : "1";
          String retention;
          switch (setting) {
            case 0:
              retention = "minSnapshotsToKeep(" + retained + ", " + (amount + 1) + ")";
              break;
            case 1:
              retention = "maxSnapshotAgeMs(" + retained + ", " + age + ")";
              break;
            case 2:
              retention = "maxRefAgeMs(" + retained + ", " + age + ")";
              break;
            default:
              retention = tableKey + "=" + tableValue;
          }
          return new Op(
              "setRetention(" + id + ", " + retention + ")",
              () -> {
                Table table = handle(w, id, cmd.fresh);
                if (setting == 3) {
                  table.updateProperties().set(tableKey, tableValue).commit();
                  return;
                }
                try {
                  ManageSnapshots manage = table.manageSnapshots();
                  if (setting == 0) {
                    manage.setMinSnapshotsToKeep(retained, amount + 1);
                  } else if (setting == 1) {
                    manage.setMaxSnapshotAgeMs(retained, age);
                  } else {
                    manage.setMaxRefAgeMs(retained, age);
                  }
                  manage.commit();
                } catch (IllegalArgumentException e) {
                  throw new ValidationException(e, "%s", e.getMessage()); // no such ref
                }
              },
              (m, actual) -> {
                TableState state = existing(m, id);
                if (setting == 3) {
                  state.refsAge |= amount == 0;
                  return;
                }
                if (!retained.equals(SnapshotRef.MAIN_BRANCH)
                    && !state.branches.containsKey(retained) && !state.tags.containsKey(retained)) {
                  throw new IllegalStateException("no ref " + retained);
                }
                if (setting == 2 && amount == 0) {
                  state.agingRefs.add(retained);
                }
              });
        case STAGE_APPEND:
          String wapId = "wap" + nextValue++;
          DataFile stagedFile = newFile();
          return new Op(
              "stageAppend(" + id + ", " + wapId + ", " + stagedFile.location() + ")",
              () -> {
                Table table = handle(w, id, cmd.fresh);
                table.newFastAppend().appendFile(inSpec(table, stagedFile)).set("wap.id", wapId)
                    .stageOnly().commit();
              },
              (m, actual) -> existing(m, id).staged.put(wapId, stagedFile.location()));
        case CHERRY_PICK:
          TableState staging = model.get(id);
          if (staging == null || staging.staged.isEmpty()) {
            return null;
          }
          List<String> wapIds = new ArrayList<>(staging.staged.keySet());
          String picked = wapIds.get(cmd.b % wapIds.size());
          return new Op(
              "cherryPick(" + id + ", " + picked + ")",
              () -> {
                Table table = handle(w, id, cmd.fresh);
                Long snapshotId = null;
                for (Snapshot snapshot : table.snapshots()) {
                  if (picked.equals(snapshot.summary().get("wap.id"))) {
                    snapshotId = snapshot.snapshotId();
                  }
                }
                if (snapshotId == null) {
                  throw new ValidationException("no snapshot staged as %s", picked);
                }
                table.manageSnapshots().cherrypick(snapshotId).commit();
              },
              (m, actual) -> {
                TableState state = existing(m, id);
                String file = state.staged.remove(picked); // a wap.id is published once
                if (file == null) {
                  throw new IllegalStateException(picked + " is not staged");
                }
                state.files.add(file);
                // a pick that commits the staged files anew may drop deletes older than every
                // live data file, as the other merging commits do
                ageOutDeletes(state, actual.get(id));
              });
        case REWRITE_MANIFESTS:
          return new Op(
              "rewriteManifests(" + id + ")",
              () -> handle(w, id, cmd.fresh).rewriteManifests().clusterBy(f -> "all").commit(),
              (m, actual) -> existing(m, id));
        case REINLINE:
          return new Op(
              "reinline(" + id + ")",
              () -> w.catalog.reinline(id),
              (m, actual) -> {
                // A re-inlining of a table already inline reads it and writes nothing: one a
                // preempting command removed after that read is acknowledged as of the read, the
                // table then in neither the model nor the catalog (p21-cal-pagefaults); one that
                // brought it back would leave it in the catalog
                if (!preempting.isEmpty() && !m.containsKey(id) && !actual.containsKey(id)) {
                  return;
                }
                existing(m, id);
              });
        case SWEEP:
          int from = cmd.b % 3;
          Replica<CatalogState> replica = from == 0 ? null
              : from == 1 ? w.catalog.log().newest() : w.kept;
          if (from == 2) {
            w.kept = w.catalog.log().newest(); // the next sweep from a kept replica, from this one
          }
          return new Op(
              "sweep(" + (replica == null ? "fresh read" : from == 1 ? "newest replica"
                  : "kept replica") + ")",
              () -> {
                List<String> deleted = replica == null ? w.catalog.deleteOrphanPages()
                    : w.catalog.log().sweep(replica);
                if (!deleted.isEmpty()) {
                  trace.append(String.format("      (deleted %s)%n", deleted));
                }
                long snapshotPages = deleted.stream()
                    .filter(name -> SnapshotPages.parseName(name) != null).count();
                Statistics.label("sweep deleted").collect(deleted.isEmpty() ? "nothing"
                    : snapshotPages == 0 ? "committed-set pages"
                        : snapshotPages == deleted.size() ? "snapshot pages" : "both");
              },
              // It changes no table: a page it deleted that any catalog from its read on names,
              // the next check's loads and its round trip of the newest root find gone
              (m, actual) -> { });
        case CREATE_NAMESPACE:
        case DROP_NAMESPACE:
        case SET_NAMESPACE_PROPERTY:
        case REMOVE_NAMESPACE_PROPERTY:
          return namespaceOp(cmd, w, NAMESPACES.get(cmd.b % NAMESPACES.size()));
        case CREATE_VIEW:
        case REPLACE_VIEW:
        case VIEW_PROPERTIES:
        case RENAME_VIEW:
        case DROP_VIEW:
        case REGISTER_VIEW:
          return viewOp(cmd, w);
        case TXN_BEGIN:
          if (w.txn != null) {
            return null;
          }
          IsolationLevel level = IsolationLevel.values()[cmd.b % 2];
          return new Op(
              "begin(" + level + ")",
              () -> w.txn = w.catalog.createTransaction(level),
              (m, actual) -> {});
        case TXN_APPEND:
          if (w.txn == null) {
            return null;
          }
          DataFile staged = newFile();
          return staging(w, "txn.append(" + id + ", " + staged.location() + ")", () -> {
            Table[] table = {null};
            Loaded at = w.load(id, table);
            table[0].newFastAppend().appendFile(inSpec(table[0], staged)).commit();
            if (at != null) {
              at.changed = true;
              at.updated = true;
            }
            TableIdentifier appendTo = at != null ? w.names.get(id) : null;
            TxnCreated appendToCreated = at != null ? null : w.created.get(id);
            return new Staged("append " + staged.location() + " to " + id,
                at != null ? Staged.TABLES : Staged.CREATES,
                (m, actual) -> existing(m, appendTo != null ? appendTo : appendToCreated.name)
                    .files.add(staged.location()));
          });
        case TXN_READ:
          if (w.txn == null) {
            return null;
          }
          String readRef = new String[] {SnapshotRef.MAIN_BRANCH, "b0", "t0", null}[cmd.c % 4];
          return staging(w, "txn.read(" + id + (readRef == null ? ", declared" : ", " + readRef)
              + ")", () -> {
                Table[] table = {null};
                Loaded at = w.load(id, table);
                if (readRef == null) {
                  w.txn.declareRead(id);
                  if (at != null) {
                    at.declared = true;
                  }
                  return null;
                }
                // A ref the table had when the transaction loaded it, else main
                String scanRef = at != null && at.metadata.ref(readRef) != null ? readRef
                    : SnapshotRef.MAIN_BRANCH;
                try (CloseableIterable<FileScanTask> tasks =
                    table[0].newScan().useRef(scanRef).planFiles()) {
                  tasks.forEach(task -> { });
                } catch (IllegalArgumentException e) {
                  // main of a table with no snapshot: its rows, none, are read all the same
                  table[0].newScan().planFiles();
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
                if (at != null) {
                  SnapshotRef readHead = at.metadata.ref(scanRef);
                  at.scans.put(scanRef, readHead == null ? null : readHead.snapshotId());
                }
                return null;
              });
        case TXN_DROP:
          if (w.txn == null) {
            return null;
          }
          boolean txnPurge = cmd.b % 2 == 1;
          return staging(w, "txn.dropTable(" + id + (txnPurge ? ", purge" : "") + ")", () -> {
            if (w.names.containsKey(id) && w.names.get(id) == null) {
              throw new ValidationException("%s was created in the transaction", id);
            }
            Table[] table = {null};
            Loaded at = w.load(id, table);
            if (!w.txn.asCatalog().dropTable(id, txnPurge)) {
              throw new NoSuchTableException("dropTable returned false: %s", id);
            }
            at.changed = true;
            TableIdentifier origin = w.names.remove(id);
            (txnPurge ? w.purging : w.keeping).add(origin);
            return new Staged("drop " + id + (txnPurge ? " with purge" : ""), Staged.TABLES,
                (m, actual) -> {
                  existing(m, origin);
                  m.remove(origin);
                });
          });
        case TXN_RENAME:
          if (w.txn == null) {
            return null;
          }
          // Mostly a name the model does not have; else any, to collide
          List<TableIdentifier> free = new ArrayList<>(TABLES);
          free.removeAll(model.keySet());
          TableIdentifier txnTo = cmd.b >= 8 || free.isEmpty()
              ? TABLES.get(cmd.b % TABLES.size()) : free.get(cmd.b % free.size());
          if (txnTo.equals(id)) {
            return null;
          }
          return staging(w, "txn.renameTable(" + id + ", " + txnTo + ")", () -> {
            Table[] table = {null};
            w.load(id, table);
            w.txn.asCatalog().renameTable(id, txnTo);
            TableIdentifier renameOf = w.names.remove(id);
            w.names.put(txnTo, renameOf);
            if (renameOf == null) {
              // One it created is created under its last name (TxnCreated)
              TxnCreated moved = w.created.remove(id);
              moved.name = txnTo;
              w.created.put(txnTo, moved);
              return new Staged("rename " + id + " to " + txnTo, Staged.CREATES, (m, actual) -> {});
            }
            // A table it loaded is renamed at the commit from its name then to its last (net)
            return new Staged("rename " + id + " to " + txnTo, Staged.RENAMES, (m, actual) -> {
              TableState state = existing(m, id);
              if (m.containsKey(txnTo) || m.views.containsKey(txnTo)) {
                throw new IllegalStateException(txnTo + " exists");
              }
              if (!m.namespaces.containsKey(txnTo.namespace())) {
                throw new IllegalStateException(txnTo.namespace() + " does not exist");
              }
              m.remove(id);
              m.put(txnTo, state);
            });
          });
        case TXN_CREATE:
          if (w.txn == null) {
            return null;
          }
          List<TableIdentifier> absent = new ArrayList<>(TABLES);
          absent.removeAll(model.keySet());
          TableIdentifier txnCreated = cmd.a >= 8 || absent.isEmpty()
              ? TABLES.get(cmd.a % TABLES.size()) : absent.get(cmd.a % absent.size());
          int txnVersion = 2 + cmd.b % 2;
          String placeholder = CREATED_IN_TXN + nextValue++;
          return staging(w, "txn.createTable(" + txnCreated + ", v" + txnVersion + ")", () -> {
            w.txn.asCatalog().buildTable(txnCreated, SCHEMA)
                .withProperty(TableProperties.FORMAT_VERSION, String.valueOf(txnVersion))
                .create();
            w.names.put(txnCreated, null);
            TxnCreated made = new TxnCreated(txnCreated);
            w.created.put(txnCreated, made);
            return new Staged("create " + txnCreated, Staged.CREATES, (m, actual) -> {
              if (m.containsKey(made.name) || m.views.containsKey(made.name)
                  || !m.namespaces.containsKey(made.name.namespace())) {
                throw new IllegalStateException("cannot create " + made.name);
              }
              m.put(made.name, new TableState(placeholder, txnVersion));
            });
          });
        case TXN_SET_PROPERTY:
          if (w.txn == null) {
            return null;
          }
          String txnPropKey = KEYS.get(cmd.b % KEYS.size());
          String txnPropValue = "v" + nextValue++;
          return staging(w, "txn.setProperty(" + id + ", " + txnPropKey + "=" + txnPropValue + ")",
              () -> {
                Table[] table = {null};
                Loaded at = w.load(id, table);
                table[0].updateProperties().set(txnPropKey, txnPropValue).commit();
                if (at != null) {
                  at.changed = true;
                  at.updated = true;
                }
                TableIdentifier setOn = at != null ? w.names.get(id) : null;
                TxnCreated setOnCreated = at != null ? null : w.created.get(id);
                return new Staged("set " + txnPropKey + " on " + id,
                    at != null ? Staged.TABLES : Staged.CREATES,
                    (m, actual) -> existing(m, setOn != null ? setOn : setOnCreated.name)
                        .props.put(txnPropKey, txnPropValue));
              });
        case TXN_CREATE_NAMESPACE:
        case TXN_DROP_NAMESPACE:
        case TXN_SET_NAMESPACE_PROPERTY:
        case TXN_REMOVE_NAMESPACE_PROPERTY:
          if (w.txn == null) {
            return null;
          }
          return txnNamespaceOp(cmd, w, NAMESPACES.get(cmd.b % NAMESPACES.size()));
        case TXN_CREATE_VIEW:
        case TXN_REPLACE_VIEW:
        case TXN_VIEW_PROPERTIES:
        case TXN_RENAME_VIEW:
        case TXN_DROP_VIEW:
          if (w.txn == null) {
            return null;
          }
          return txnViewOp(cmd, w);
        case TXN_COMMIT:
          if (w.txn == null) {
            return null;
          }
          CatalogTransaction txn = w.txn;
          Map<TableIdentifier, Loaded> loaded = new LinkedHashMap<>(w.loaded);
          List<Staged> changes = new ArrayList<>(w.staged);
          // The renames of the tables it loaded, net, as BaseCatalogTransaction sends them: each
          // table from the name it was loaded by to its last, if that is another (one renamed
          // back, or dropped, is renamed not at all). They need neither the table at the names
          // it passed through nor those names free at the commit, which serializes the
          // transaction before whatever took them since (a table's existence is no read). A
          // rename not undone is a change.
          List<Map.Entry<TableIdentifier, TableIdentifier>> renames = new ArrayList<>();
          w.names.forEach((name, origin) -> {
            if (origin != null && !name.equals(origin)) {
              renames.add(Map.entry(origin, name));
            }
          });
          renames.forEach(r -> {
            if (loaded.containsKey(r.getKey())) {
              loaded.get(r.getKey()).changed = true;
            }
          });
          // Its changes in the order the commit applies them (Staged)
          // Its view changes, net, as BaseCatalogTransaction sends them
          List<TxnView> viewChanges = new ArrayList<>();
          w.views.forEach(v -> {
            if (v.changes()) {
              viewChanges.add(v);
            }
          });
          List<Map.Entry<TableIdentifier, TableIdentifier>> viewRenames = new ArrayList<>();
          viewChanges.forEach(v -> {
            if (v.start != null && !v.dropped && !v.name.equals(v.start)) {
              viewRenames.add(Map.entry(v.start, v.name));
            }
          });
          List<Staged> effects = new ArrayList<>(changes);
          effects.removeIf(c -> c.phase == Staged.RENAMES);
          effects.add(new Staged("renames " + renames + (viewRenames.isEmpty() ? ""
              : ", views' " + viewRenames), Staged.RENAMES,
              (m, actual) -> renameAll(m, renames, viewRenames)));
          if (!viewChanges.isEmpty()) {
            effects.add(new Staged("views " + viewChanges, Staged.VIEWS,
                (m, actual) -> changeViews(m, viewChanges)));
            effects.add(new Staged("views created", Staged.VIEW_CREATES,
                (m, actual) -> createViews(m, viewChanges)));
          }
          effects.sort(java.util.Comparator.comparingInt(c -> c.phase));
          // What each purge deletes: the manifests and lists of the table as it is now, just
          // before the commit (Bug 96)
          Map<TableIdentifier, Set<String>> purging = new TreeMap<>(BY_NAME);
          w.purging.forEach(t -> purging.put(t, purgeable(t)));
          List<TableIdentifier> keepingFiles = new ArrayList<>(w.keeping);
          w.closeTxn();
          boolean[] landed = {false};
          Op commit = new Op(
              "txn.commit(" + txn.isolationLevel() + ", " + changes
                  + (viewChanges.isEmpty() ? "" : ", views " + viewChanges) + ")",
              () -> {
                txn.commitTransaction();
                landed[0] = true;
              },
              (m, actual) -> {
                // All of it, or nothing: the effects apply to the model's copy in the order the
                // transaction staged them, and any that does not apply fails the whole
                checkIsolation(txn.isolationLevel(), loaded, m);
                for (Staged change : effects) {
                  change.effect.apply(m, actual);
                }
                adoptCreated(m, actual);
                if (landed[0]) {
                  purging.forEach(this::checkPurged);
                }
              });
          commit.loaded = loaded;
          commit.keeping = keepingFiles;
          commit.changesViews = !viewChanges.isEmpty();
          return commit;
        default:
          throw new IllegalStateException("Unknown kind " + cmd.kind);
      }
    }

    /**
     * A name of {@link #VIEW_NAMES} no table or view holds, in a namespace that exists, mostly;
     * else any, to collide with a table or a view, or to name a namespace that does not exist.
     */
    private TableIdentifier pickName(int index) {
      List<TableIdentifier> free = new ArrayList<>(VIEW_NAMES);
      free.removeAll(model.keySet());
      free.removeAll(model.views.keySet());
      free.removeIf(name -> !model.namespaces.containsKey(name.namespace()));
      return index >= 8 || free.isEmpty()
          ? VIEW_NAMES.get(index % VIEW_NAMES.size())
          : free.get(index % free.size());
    }

    /** A view of the model, mostly; else any name, which may hold none. */
    private TableIdentifier pickView(int index) {
      List<TableIdentifier> views = new ArrayList<>(model.views.keySet());
      return index >= 8 || views.isEmpty()
          ? VIEW_NAMES.get(index % VIEW_NAMES.size())
          : views.get(index % views.size());
    }

    /** A view operation (docs/VIEWS.md). */
    private Op viewOp(Cmd cmd, Writer w) {
      String value = "v" + nextValue++;
      switch (cmd.kind) {
        case CREATE_VIEW: {
          TableIdentifier created = pickName(cmd.a);
          boolean kept = cmd.b % 2 == 1;
          String sql = "select '" + value + "'";
          return new Op(
              "createView(" + created + (kept ? ", kept" : "") + ")",
              () -> {
                org.apache.iceberg.view.ViewBuilder builder = w.catalog.buildView(created)
                    .withSchema(SCHEMA)
                    .withDefaultNamespace(created.namespace())
                    .withQuery("spark", sql);
                if (kept) {
                  builder = builder.withProperty(TableProperties.GC_ENABLED, "false");
                }
                builder.create();
              },
              (m, actual) -> {
                checkBindable(m, created);
                m.views.put(created, new ViewState(sql, 1, kept));
              });
        }
        case REPLACE_VIEW: {
          TableIdentifier view = pickView(cmd.a);
          String sql = "select '" + value + "'";
          return new Op(
              "replaceView(" + view + ")",
              () -> w.catalog.loadView(view).replaceVersion()
                  .withSchema(SCHEMA)
                  .withDefaultNamespace(view.namespace())
                  .withQuery("spark", sql)
                  .commit(),
              (m, actual) -> {
                ViewState state = existingView(m, view);
                m.views.put(view, state.with(sql, state.versionId + 1));
              });
        }
        case VIEW_PROPERTIES: {
          TableIdentifier view = pickView(cmd.a);
          String key = KEYS.get(cmd.b % KEYS.size());
          return new Op(
              "setViewProperty(" + view + ", " + key + "=" + value + ")",
              () -> w.catalog.loadView(view).updateProperties().set(key, value).commit(),
              (m, actual) -> existingView(m, view).props.put(key, value));
        }
        case RENAME_VIEW: {
          TableIdentifier view = pickView(cmd.a);
          TableIdentifier to = pickName(cmd.b);
          if (to.equals(view)) {
            return null; // refused as it is staged, as a table's rename to its name is
          }
          return new Op(
              "renameView(" + view + ", " + to + ")",
              () -> w.catalog.renameView(view, to),
              (m, actual) -> {
                ViewState state = existingView(m, view);
                checkBindable(m, to);
                m.views.remove(view);
                m.views.put(to, state);
              });
        }
        case DROP_VIEW: {
          TableIdentifier view = pickView(cmd.a);
          return new Op(
              "dropView(" + view + ")",
              () -> {
                if (!w.catalog.dropView(view)) {
                  throw new NoSuchViewException("dropView returned false: %s", view);
                }
              },
              (m, actual) -> {
                ViewState state = existingView(m, view);
                m.views.remove(view);
                if (state.kept) {
                  // Iceberg keeps the metadata of a view whose gc.enabled is false: a pointer
                  // view's file, or the file an inline view's drop writes (VIEWS.md V9)
                  m.keptViews.put(keptFile(view, state), state.copy());
                }
              });
        }
        default: {
          resolveKeptViews();
          if (model.keptViews.isEmpty()) {
            return null;
          }
          List<String> files = new ArrayList<>(model.keptViews.keySet());
          String file = files.get(cmd.b % files.size());
          ViewState kept = model.keptViews.get(file).copy();
          TableIdentifier registered = pickName(cmd.a);
          return new Op(
              "registerView(" + registered + ", " + file + ")",
              () -> w.catalog.registerView(registered, file),
              (m, actual) -> {
                checkBindable(m, registered);
                m.views.put(registered, kept);
              });
        }
      }
    }

    /**
     * A view change in {@code w}'s open catalog transaction (VIEWS.md "Stage 2"), through its view
     * catalog, with operands as {@link #viewOp}'s namesake picks them. It stages nothing the
     * commit's effect reads but {@code w}'s view of it ({@link Writer#views}), which follows
     * BaseCatalogTransaction's: a view it had, from its first load; one it created.
     */
    private Op txnViewOp(Cmd cmd, Writer w) {
      String value = "v" + nextValue++;
      org.apache.iceberg.catalog.ViewCatalog txViews = w.txn.asViewCatalog();
      org.apache.iceberg.view.View[] view = {null};
      switch (cmd.kind) {
        case TXN_CREATE_VIEW: {
          TableIdentifier created = pickName(cmd.a);
          boolean kept = cmd.b % 2 == 1;
          String sql = "select '" + value + "'";
          return staging(w, "txn.createView(" + created + (kept ? ", kept" : "") + ")", () -> {
            org.apache.iceberg.view.ViewBuilder builder = txViews.buildView(created)
                .withSchema(SCHEMA)
                .withDefaultNamespace(created.namespace())
                .withQuery("spark", sql);
            if (kept) {
              builder = builder.withProperty(TableProperties.GC_ENABLED, "false");
            }
            builder.create();
            TxnView staged = new TxnView(created, new ViewState(sql, 1, kept));
            w.views.add(staged);
            w.viewsByName.put(created, staged);
            w.viewsGone.remove(created);
            return null;
          });
        }
        case TXN_REPLACE_VIEW: {
          TableIdentifier name = pickView(cmd.a);
          String sql = "select '" + value + "'";
          return staging(w, "txn.replaceView(" + name + ")", () -> {
            TxnView at = w.loadView(name, view);
            view[0].replaceVersion()
                .withSchema(SCHEMA)
                .withDefaultNamespace(name.namespace())
                .withQuery("spark", sql)
                .commit();
            at.current = at.current.with(sql, at.current.versionId + 1);
            at.updated = true;
            return null;
          });
        }
        case TXN_VIEW_PROPERTIES: {
          TableIdentifier name = pickView(cmd.a);
          String key = KEYS.get(cmd.b % KEYS.size());
          return staging(w, "txn.setViewProperty(" + name + ", " + key + "=" + value + ")", () -> {
            TxnView at = w.loadView(name, view);
            view[0].updateProperties().set(key, value).commit();
            at.current = at.current.copy();
            at.current.props.put(key, value);
            at.updated = true;
            return null;
          });
        }
        case TXN_RENAME_VIEW: {
          TableIdentifier name = pickView(cmd.a);
          TableIdentifier to = pickName(cmd.b);
          if (to.equals(name)) {
            return null; // refused as it is staged
          }
          return staging(w, "txn.renameView(" + name + ", " + to + ")", () -> {
            TxnView at = w.loadView(name, view);
            txViews.renameView(name, to);
            w.viewsByName.remove(name);
            w.viewsGone.add(name);
            w.viewsByName.put(to, at);
            w.viewsGone.remove(to);
            at.name = to;
            return null;
          });
        }
        default: {
          TableIdentifier name = pickView(cmd.a);
          return staging(w, "txn.dropView(" + name + ")", () -> {
            TxnView at = w.loadView(name, view);
            if (!txViews.dropView(name)) {
              throw new NoSuchViewException("dropView returned false: %s", name);
            }
            at.dropped = true;
            w.viewsByName.remove(name);
            w.viewsGone.add(name);
            return null;
          });
        }
      }
    }

    /**
     * Applies a catalog transaction's changes to the views it had, net, to {@code m}, as LogCatalog
     * stages them (stageViewChanges), but their renames ({@link #renameAll}): each only on the
     * version it first loaded (VIEWS.md V6), or none of the transaction applies; drops and
     * updates. A kept view's drop ({@code gc.enabled} false) leaves a metadata file to register,
     * which {@link #resolve} finds once the commit took effect.
     */
    private void changeViews(Model m, List<TxnView> changes) {
      for (TxnView v : changes) {
        if (v.start != null) {
          ViewState now = existingView(m, v.start);
          if (!Objects.equals(now.metadataLocation, v.baseLocation)) {
            throw new IllegalStateException(String.format(
                "view %s changed since the transaction loaded it (%s -> %s)", v.start,
                v.baseLocation, now.metadataLocation));
          }
        }
      }
      for (TxnView v : changes) {
        if (v.start == null) {
          continue;
        }
        if (v.dropped) {
          ViewState now = m.views.remove(v.start);
          if (now.kept) {
            m.keptViews.put(KEPT_BY_A_TRANSACTION + v.start, now.copy());
          }
        } else if (v.updated) {
          m.views.put(v.start, v.current.copy());
        }
      }
    }

    /** Adds the views a catalog transaction created, at their names at its end. */
    private static void createViews(Model m, List<TxnView> changes) {
      for (TxnView v : changes) {
        if (v.start == null) {
          checkBindable(m, v.name);
          m.views.put(v.name, v.current.copy());
        }
      }
    }

    /**
     * The metadata file a drop of {@code view}, kept, left in its metadata directory, which holds
     * {@code state}: a pointer view's own, or the one its drop wrote. Throws if there is none,
     * which fails an acknowledged drop: the view could not be registered again.
     */
    private String keptFile(TableIdentifier view, ViewState state) {
      String file = heldFile(state);
      if (file == null) {
        // an acknowledged drop's effect then fails the run; a rejected one's does not apply
        throw new IllegalStateException(String.format(
            "the drop of %s, kept (gc.enabled false), left no metadata file of %s", view, state));
      }
      return file;
    }

    /**
     * A metadata file that holds {@code state}: a pointer view's own, or one in its metadata
     * directory; null if none does.
     */
    private String heldFile(ViewState state) {
      if (state.metadataLocation != null && !state.metadataLocation.startsWith("inline-view://")
          && holds(state.metadataLocation, state)) {
        return state.metadataLocation;
      }
      for (org.apache.iceberg.io.FileInfo info : io.listPrefix(state.location + "/metadata/")) {
        if (info.location().endsWith(".metadata.json") && holds(info.location(), state)) {
          return info.location();
        }
      }
      return null;
    }

    /**
     * Each kept view's file the model recorded that is gone, replaced by a file of its metadata
     * directory that holds it; the run fails if none does. The file recorded when a drop landed
     * may be one a racing drop of the same inline view wrote first, and deleted since, losing
     * (each writes the view to a file of its own before its commit, and a drop that fails deletes
     * what it wrote): both held the view when the drop that landed was applied to the model.
     */
    private void resolveKeptViews() {
      for (String file : new ArrayList<>(model.keptViews.keySet())) {
        if (io.newInputFile(file).exists()) {
          continue;
        }
        ViewState state = model.keptViews.remove(file);
        String held = heldFile(state);
        if (held == null) {
          throw fail("a view dropped keeping its metadata (gc.enabled false) cannot be registered"
              + " again: %s is gone, and no metadata file under %s holds %s", file,
              state.location, state);
        }
        model.keptViews.put(held, state);
      }
    }

    /** Whether the view metadata file {@code file} holds {@code state}. */
    private boolean holds(String file, ViewState state) {
      try {
        return viewState(org.apache.iceberg.view.ViewMetadataParser.read(io, file))
            .sameAs(state);
      } catch (RuntimeException e) {
        return false;
      }
    }

    /** A view as the model tracks it, from its metadata. */
    private static ViewState viewState(org.apache.iceberg.view.ViewMetadata metadata) {
      ViewState state = new ViewState(
          ((org.apache.iceberg.view.SQLViewRepresentation)
              metadata.currentVersion().representations().get(0)).sql(),
          metadata.currentVersionId(),
          "false".equals(metadata.properties().get(TableProperties.GC_ENABLED)));
      metadata.properties().forEach((k, v) -> {
        if (k.startsWith(KEY_PREFIX)) {
          state.props.put(k, v);
        }
      });
      state.metadataLocation = metadata.metadataFileLocation();
      state.location = metadata.location();
      return state;
    }

    /** A namespace operation on {@code ns}. */
    private Op namespaceOp(Cmd cmd, Writer w, Namespace ns) {
      String key = KEYS.get(cmd.b / 2 % KEYS.size());
      switch (cmd.kind) {
        case CREATE_NAMESPACE:
          Map<String, String> props = cmd.b % 2 == 1 ? Map.of(key, "v" + nextValue++) : Map.of();
          return new Op(
              "createNamespace(" + ns + ", " + props + ")",
              () -> w.catalog.createNamespace(ns, props),
              (m, actual) -> {
                if (m.namespaces.containsKey(ns)) {
                  throw new IllegalStateException(ns + " exists");
                }
                m.namespaces.put(ns, new TreeMap<>(props));
              });
        case DROP_NAMESPACE:
          return new Op(
              "dropNamespace(" + ns + ")",
              () -> {
                if (!w.catalog.dropNamespace(ns)) {
                  throw new NoSuchNamespaceException("dropNamespace returned false: %s", ns);
                }
              },
              (m, actual) -> {
                if (!m.namespaces.containsKey(ns)) {
                  throw new IllegalStateException(ns + " does not exist");
                }
                if (m.keySet().stream().anyMatch(id -> id.namespace().equals(ns))
                    || m.views.keySet().stream().anyMatch(id -> id.namespace().equals(ns))) {
                  throw new IllegalStateException(ns + " is not empty");
                }
                m.namespaces.remove(ns);
              });
        case SET_NAMESPACE_PROPERTY:
          String value = "v" + nextValue++;
          return new Op(
              "setProperties(" + ns + ", " + key + "=" + value + ")",
              () -> {
                if (!w.catalog.setProperties(ns, Map.of(key, value))) {
                  throw new IllegalStateException("setProperties returned false");
                }
              },
              (m, actual) -> {
                if (!m.namespaces.containsKey(ns)) {
                  throw new IllegalStateException(ns + " does not exist");
                }
                m.namespaces.get(ns).put(key, value);
              });
        default:
          return new Op(
              "removeProperties(" + ns + ", " + key + ")",
              () -> {
                if (!w.catalog.removeProperties(ns, Set.of(key))) {
                  throw new IllegalStateException("removeProperties returned false");
                }
              },
              (m, actual) -> {
                if (!m.namespaces.containsKey(ns)) {
                  throw new IllegalStateException(ns + " does not exist");
                }
                m.namespaces.get(ns).remove(key);
              });
      }
    }

    /** A step staging a change or a read in a catalog transaction. */
    @FunctionalInterface
    interface Stage {
      /**
       * Stages it; returns the change staged, or null for a read or a view's change (the commit
       * finds those in the writer's {@link Writer#views}).
       */
      Staged stage();
    }

    /**
     * {@code w}'s step {@code stage} in its open transaction, which changes no catalog: a change
     * joins the transaction's, for its commit. A step the transaction refuses (a table, view or
     * namespace it does not have, or has, or one not empty) leaves it open; any other failure
     * abandons it.
     */
    private Op staging(Writer w, String description, Stage stage) {
      return new Op(description, () -> {
        try {
          Staged change = stage.stage();
          if (change != null) {
            w.staged.add(change);
          }
        } catch (NoSuchTableException | AlreadyExistsException | NoSuchNamespaceException
            | NamespaceNotEmptyException | NoSuchViewException | ValidationException e) {
          throw e;
        } catch (UnsupportedOperationException | IllegalArgumentException e) {
          // a namespace it dropped created again, a ref it does not have
          throw new ValidationException(e, "%s", e.getMessage());
        } catch (org.apache.iceberg.exceptions.NotFoundException e) {
          // The transaction stages on the version it read; an expiry, or a drop with purge,
          // since may have deleted that version's manifest list, which the append reads. A
          // rejection, as for a stale handle (the transaction would fail at commit anyway: the
          // table changed since it read it). A live snapshot without its manifest list is still
          // caught by the check of every snapshot's list.
          w.closeTxn();
          throw new ValidationException(e, "the transaction's snapshot is gone");
        } catch (RuntimeException e) {
          w.closeTxn(); // abandon the transaction
          throw e;
        }
      }, (m, actual) -> {});
    }

    /** A namespace change on {@code ns} in {@code w}'s open transaction. */
    private Op txnNamespaceOp(Cmd cmd, Writer w, Namespace ns) {
      String key = KEYS.get(cmd.b / 2 % KEYS.size());
      org.apache.iceberg.catalog.SupportsNamespaces txc =
          (org.apache.iceberg.catalog.SupportsNamespaces) w.txn.asCatalog();
      switch (cmd.kind) {
        case TXN_CREATE_NAMESPACE:
          Map<String, String> props = cmd.b % 2 == 1 ? Map.of(key, "v" + nextValue++) : Map.of();
          return staging(w, "txn.createNamespace(" + ns + ", " + props + ")", () -> {
            txc.createNamespace(ns, props);
            w.createdNamespaces.add(ns);
            return new Staged("create " + ns, Staged.NAMESPACES, (m, actual) -> {
              if (m.namespaces.containsKey(ns)) {
                throw new IllegalStateException(ns + " exists");
              }
              m.namespaces.put(ns, new TreeMap<>(props));
            });
          });
        case TXN_DROP_NAMESPACE:
          if (w.createdNamespaces.contains(ns)) {
            return null; // created and dropped: neither happens, which the model does not follow
          }
          // Half the time a cascade: the namespace's tables dropped first, keeping their files
          boolean cascade = cmd.c % 2 == 1;
          return staging(w, "txn.dropNamespace(" + ns + (cascade ? ", its tables first" : "")
              + ")", () -> {
            if (cascade) {
              for (TableIdentifier t : w.txn.asCatalog().listTables(ns)) {
                if (w.names.containsKey(t) && w.names.get(t) == null) {
                  continue; // created in the transaction, which the model does not drop
                }
                Table[] table = {null};
                Loaded at = w.load(t, table);
                if (w.txn.asCatalog().dropTable(t, false)) {
                  at.changed = true;
                  TableIdentifier dropped = w.names.remove(t);
                  w.keeping.add(dropped);
                  w.staged.add(new Staged("drop " + t, Staged.TABLES, (m, actual) -> {
                    existing(m, dropped);
                    m.remove(dropped);
                  }));
                }
              }
            }
            if (!txc.dropNamespace(ns)) {
              throw new NoSuchNamespaceException("dropNamespace returned false: %s", ns);
            }
            return new Staged("drop " + ns, Staged.NAMESPACE_DROPS, (m, actual) -> {
              if (!m.namespaces.containsKey(ns)) {
                throw new IllegalStateException(ns + " does not exist");
              }
              // A view in it keeps it: the transaction's staging refuses the drop of a namespace
              // whose views it sees, and its commit refuses one with a view created since
              if (m.keySet().stream().anyMatch(t -> t.namespace().equals(ns))
                  || m.views.keySet().stream().anyMatch(v -> v.namespace().equals(ns))) {
                throw new IllegalStateException(ns + " is not empty");
              }
              m.namespaces.remove(ns);
            });
          });
        case TXN_SET_NAMESPACE_PROPERTY:
          String value = "v" + nextValue++;
          return staging(w, "txn.setProperties(" + ns + ", " + key + "=" + value + ")", () -> {
            txc.setProperties(ns, Map.of(key, value));
            return new Staged("set " + key + " on " + ns, Staged.NAMESPACES, (m, actual) -> {
              if (!m.namespaces.containsKey(ns)) {
                throw new IllegalStateException(ns + " does not exist");
              }
              m.namespaces.get(ns).put(key, value);
            });
          });
        default:
          return staging(w, "txn.removeProperties(" + ns + ", " + key + ")", () -> {
            txc.removeProperties(ns, Set.of(key));
            return new Staged("remove " + key + " from " + ns, Staged.NAMESPACES, (m, actual) -> {
              if (!m.namespaces.containsKey(ns)) {
                throw new IllegalStateException(ns + " does not exist");
              }
              m.namespaces.get(ns).remove(key);
            });
          });
      }
    }

    /**
     * Gives each table a catalog transaction created the UUID the catalog has for it, at the name
     * it has once the transaction's changes apply.
     */
    private static void adoptCreated(Model m, Model actual) {
      for (TableIdentifier name : new ArrayList<>(m.keySet())) {
        TableState state = m.get(name);
        if (state.uuid.startsWith(CREATED_IN_TXN)) {
          TableState now = actual.get(name);
          if (now == null) {
            throw new IllegalStateException(name + ", which the transaction created, is missing");
          }
          m.put(name, state.withUuid(now.uuid));
        }
      }
    }

    /**
     * Checks that {@code was}, a table dropped keeping its files, can be registered again (errata
     * T4): a metadata file under its location holds its last version, its UUID, snapshots and
     * current snapshot.
     */
    private void checkKept(TableIdentifier id, TableState was) {
      String location;
      try {
        location = org.apache.iceberg.util.JsonUtil.mapper().readTree(was.json).get("location")
            .asText();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      List<String> files = new ArrayList<>();
      for (org.apache.iceberg.io.FileInfo file : io.listPrefix(location + "/metadata/")) {
        if (!file.location().endsWith(".metadata.json")) {
          continue;
        }
        TableMetadata kept;
        try {
          kept = TableMetadataParser.read(io, file.location());
        } catch (IllegalArgumentException e) {
          // A view given the table's name since writes its metadata files here too: a table and
          // a view share names (VIEWS.md V7), and so default locations. Anything else fails.
          org.apache.iceberg.view.ViewMetadataParser.read(io, file.location());
          continue;
        }
        files.add(file.location());
        Set<Long> snapshots = new TreeSet<>();
        kept.snapshots().forEach(snapshot -> snapshots.add(snapshot.snapshotId()));
        Long current = kept.currentSnapshot() == null ? null : kept.currentSnapshot().snapshotId();
        if (kept.uuid().equals(was.uuid) && snapshots.equals(was.snapshotIds)
            && Objects.equals(current, was.snapshotId)) {
          Statistics.label("a table dropped keeping its files").collect("registerable");
          return;
        }
      }
      throw fail("%s was dropped keeping its files, but no metadata file under %s holds its last"
          + " version (snapshots %s, current %s): %s", id, location, was.snapshotIds,
          was.snapshotId, files);
    }

    /**
     * Throws if a catalog transaction that loaded {@code loaded} could not have committed on
     * {@code current}, the state its effects apply to.
     */
    private void checkIsolation(
        IsolationLevel level, Map<TableIdentifier, Loaded> loaded,
        Map<TableIdentifier, TableState> current) {
      loaded.forEach(
          (id, at) -> {
            boolean read = level == IsolationLevel.SERIALIZABLE
                && (!at.scans.isEmpty() || at.declared);
            if (!at.changed && !read) {
              return; // a load is not a read
            }
            TableState now = current.get(id);
            if (now == null || !now.uuid.equals(at.uuid)) {
              throw new IsolationViolation(level + ": " + id + " was replaced or dropped");
            }
            if (!read) {
              // a table it changed may have changed since: an inline table's updates are judged
              // where the record lands (errata D12), a drop or rename applies to the table as it
              // is, and the model checks what they did
              return;
            }
            // serializable: each ref it scanned names the snapshot it read (its rows: none, if
            // it named none then and names none now); a declared read, the version it read
            for (Map.Entry<String, Long> scan : at.scans.entrySet()) {
              Long named = now.refIds.get(scan.getKey());
              if (!Objects.equals(scan.getValue(), named)) {
                throw new IsolationViolation(String.format(
                    "%s: %s changed after it was scanned (%s at snapshot %s -> %s, metadata %s ->"
                        + " %s)", level, id, scan.getKey(), scan.getValue(), named,
                    at.metadataLocation, now.metadataLocation));
              }
            }
            if (at.declared && !Objects.equals(at.metadataLocation, now.metadataLocation)) {
              throw new IsolationViolation(String.format(
                  "%s: %s changed after its read was declared (metadata %s -> %s)", level, id,
                  at.metadataLocation, now.metadataLocation));
            }
          });
    }

    private void run(Cmd cmd, boolean topLevel) {
      Writer w = writer(cmd);
      Op op = interpret(cmd, w);
      if (!topLevel) {
        lastNestedOp = op;
      }
      if (op == null) {
        return;
      }
      String indent = topLevel ? "" : "    ";
      if (topLevel) {
        runningOp = op;
      }
      trace.append(String.format("%sw%d %s%n", indent, w.id, op));
      Outcome outcome;
      String reason = "";
      // A rejection as a conflict, which may need to be justified (see mustCommit), and the failure
      boolean conflict = false;
      RuntimeException failure = null;
      if (topLevel) {
        armed = true;
      }
      try {
        op.action.run();
        outcome = Outcome.ACKNOWLEDGED;
      } catch (CommitStateUnknownException e) {
        outcome = Outcome.UNKNOWN;
        reason = firstLine(e);
      } catch (CommitFailedException | ValidationException | AlreadyExistsException
          | NoSuchNamespaceException | NamespaceNotEmptyException | CommitRefusedException
          | org.apache.iceberg.exceptions.ServiceUnavailableException e) {
        // ServiceUnavailableException: a catalog operation whose attempts all conflicted (Bug 107
        // (create conflict reported as exists))
        outcome = Outcome.REJECTED;
        reason = e.getClass().getSimpleName() + ": " + firstLine(e);
        failure = e;
        // A catalog transaction's commit refused for a table or namespace it should not find,
        // or find: its model effect checks those (the review's A6)
        conflict = e instanceof CommitFailedException
            || e instanceof org.apache.iceberg.exceptions.ServiceUnavailableException
            || cmd.kind == Kind.TXN_COMMIT && !(e instanceof ValidationException)
                && !(e instanceof CommitRefusedException);
      } catch (NoSuchViewException e) {
        outcome = Outcome.REJECTED; // a view that is not there, or not any more
        reason = e.getClass().getSimpleName() + ": " + firstLine(e);
        failure = e;
      } catch (NoSuchTableException e) {
        outcome = Outcome.REJECTED;
        reason = e.getClass().getSimpleName() + ": " + firstLine(e);
        conflict = cmd.kind == Kind.TXN_COMMIT;
        failure = e;
        w.handles.clear(); // a handle to a dropped table is unusable; a caller reloads
      } catch (SnapshotPages.MissingPage e) {
        // A page its table names that a get did not find: lost, as an injected fault made it
        if (!topLevel || fired.isEmpty()) {
          throw unexpected(e);
        }
        outcome = Outcome.FAILED;
        reason = e.getClass().getSimpleName() + ": " + firstLine(e);
      } catch (IllegalStateException e) {
        if (e.getMessage() == null || !e.getMessage().startsWith("Table UUID does not match")) {
          throw unexpected(e);
        }
        outcome = Outcome.REJECTED; // a stale handle to a table replaced since
        reason = firstLine(e);
        w.handles.clear();
      } catch (RuntimeException e) {
        if (!topLevel || fired.isEmpty()) {
          throw unexpected(e);
        }
        outcome = Outcome.FAILED;
        reason = e.getClass().getSimpleName() + ": " + firstLine(e);
      } finally {
        if (topLevel) {
          armed = false;
        }
      }
      String faultsFired = topLevel && !fired.isEmpty() ? " after " + fired : "";
      // Bug 66 left an outcome unknown on storage that did not fail. A preempting operation runs
      // without faults.
      if (outcome == Outcome.UNKNOWN && faultsFired.isEmpty() && !gc) {
        throw fail("%s has an unknown outcome (%s), but no fault was injected and no writer drops "
            + "committed ids", op, reason);
      }
      trace.append(String.format("%s  -> %s%s%s%n", indent, outcome,
          reason.isEmpty() ? "" : " (" + reason + ")", faultsFired));
      Statistics.label("outcome").collect(cmd.kind + " " + outcome);
      if (cmd.kind.name().contains("VIEW") && outcome == Outcome.REJECTED) {
        Statistics.label("a view operation rejected")
            .collect(cmd.kind + " " + reason.replaceAll(":.*", ""));
      }
      if (!faultsFired.isEmpty()) {
        Statistics.label("outcome after a fault").collect(outcome);
      }
      if (op.changesViews) {
        Statistics.label("a catalog transaction's commit with view changes").collect(outcome
            + (outcome == Outcome.REJECTED ? " " + reason.replaceAll(":.*", "") : ""));
      }
      if (cmd.kind == Kind.TXN_BEGIN || cmd.kind.stagesInATransaction()) {
        return; // no change to the catalog
      }
      if (cmd.kind == Kind.SWEEP && !topLevel) {
        // No change to a table, and it may run after the record of the command it preempts landed
        // (in the compaction after it): that command's check reads the catalog after both
        return;
      }
      if (topLevel && settled) {
        // Its effect landed before the command that preempted it ran, and was settled then: it
        // took effect, and what it did after (a cleanup, a compaction) changes nothing modeled
        check(outcome == Outcome.ACKNOWLEDGED || outcome == Outcome.UNKNOWN,
            "%s reported %s (%s), but took effect before a later command ran", op, outcome,
            reason);
        Model after = actual();
        String changed = diff(model, after);
        check(changed == null, "%s, settled when its effect landed, then changed the catalog: %s",
            op, changed);
        lastActual = after;
        return;
      }
      // A preempting command runs without faults, and nothing preempts it
      String must = conflict && (!topLevel || fired.isEmpty())
          ? mustCommit(cmd, w, op, topLevel && !preempting.isEmpty(), topLevel && writes == 0,
              failure)
          : null;
      if (topLevel && lateOp != null
          && (outcome == Outcome.ACKNOWLEDGED || outcome == Outcome.UNKNOWN)) {
        effectApplied = lateOrder(op);
      }
      resolve(cmd, op, outcome, reason, must,
          topLevel && !preempting.isEmpty(), !topLevel || fired.isEmpty());
      effectApplied = false;
    }

    /**
     * Why {@code op}, {@code w}'s, rejected as a conflict with no fault injected, must have
     * committed if its model effect applies; null if it may conflict. {@code preempted}: a command
     * ran inside it; {@code unsent}: it wrote nothing to the catalog's log.
     */
    private String mustCommit(Cmd cmd, Writer w, Op op, boolean preempted, boolean unsent,
        RuntimeException failure) {
      if (op.loaded != null) { // a catalog transaction's commit
        if (preempted) {
          return null;
        }
        boolean unchanged = true;
        // A writer that inlines stages a delta, if it fits (a writer that does not, or an update
        // over the inline limit, writes a metadata file, which applies only on the version it
        // was built on)
        boolean judged = unsent && w.inline;
        for (Map.Entry<TableIdentifier, Loaded> e : op.loaded.entrySet()) {
          TableState now = model.get(e.getKey());
          Loaded at = e.getValue();
          if (now == null || !now.uuid.equals(at.uuid)) {
            return null; // the effect does not apply
          }
          unchanged &= Objects.equals(now.metadataLocation, at.metadataLocation);
          // An append is judged where the record lands if the table was inline when the
          // transaction loaded it and is the same inline table now (LogCatalog.stageJudged);
          // unless it is the first snapshot of a table upgraded to v3, which gives row ids to the
          // data manifests it keeps from before the upgrade: a writer that inlines manifest lists
          // writes a metadata file for it (Bug 36), which applies only on the version it read.
          // Nor if the table was upgraded to v3 since the transaction loaded it: its snapshot,
          // built without row lineage, keeps manifests without row ids, which a v3 snapshot's
          // list may have pooled since with them, and a writer that inlines manifest lists then
          // writes a metadata file for it too (the pool rule: Bug 137). Sent, replay would refuse
          // it as well: the upgrade is a concern, and Landing merges no snapshot built without
          // row lineage onto a v3 table
          int[] then = LogCatalog.inlineVersion(at.metadataLocation);
          int[] since = LogCatalog.inlineVersion(now.metadataLocation);
          boolean upgradedSince = at.metadata.formatVersion() < 3 && now.formatVersion >= 3;
          judged &= !at.updated || then != null && since != null && then[0] == since[0]
              && !(w.inlineLists && (givesRowIds(at.metadata) || upgradedSince));
          judged &= at.updated || !at.changed && at.scans.isEmpty() && !at.declared
              || Objects.equals(now.metadataLocation, at.metadataLocation);
          // A read of it (a scan, or a declared read), updated or not, is checked at the commit's
          // read too (TableRead.unchangedIn: its ref and the schema it read with): on a table
          // changed since, it may fail there, unsent
          judged &= at.scans.isEmpty() && !at.declared
              || Objects.equals(now.metadataLocation, at.metadataLocation);
        }
        if (unchanged || !(failure instanceof CommitFailedException)) {
          return unchanged ? "no other writer changed the tables it loaded since it loaded them"
              : "nothing raced it";
        }
        return judged ? "it appended only to inline tables, whose appends the conflict encoding"
            + " judges where the record lands, yet it sent no record" : null;
      }
      if (retries(cmd)) {
        return preempted
            ? "its operation applies itself again on a fresh read after a conflict, and the one"
                + " command that raced it ran before its first write"
            : "its operation applies itself again on a fresh read after a conflict, and nothing"
                + " raced it";
      }
      if (!preempted && (cmd.fresh || handleFree(cmd.kind))) {
        return "nothing raced it, and it read the table fresh";
      }
      return null;
    }

    /**
     * Whether a snapshot added to main of {@code metadata} gives row ids to manifests: main's head
     * of a v3 table was written before the upgrade (it has no first row id), so its data manifests
     * have none.
     */
    private boolean givesRowIds(TableMetadata metadata) {
      Snapshot head = metadata.currentSnapshot();
      return metadata.formatVersion() >= 3 && head != null && head.firstRowId() == null;
    }

    /**
     * Checks the catalog against the model, given what {@code op} reported; {@code must}, if not
     * null, says why {@code op} must not have been rejected if its model effect applies.
     */
    /**
     * Why a model effect did not apply: its exception's message, or the exception itself when it
     * has none. Never null: a null here would read as an effect that applied.
     */
    private static String notApplied(RuntimeException e) {
      return e.getMessage() != null ? e.getMessage() : e.toString();
    }

    private void resolve(Cmd cmd, Op op, Outcome outcome, String reason, String must,
        boolean preempted, boolean faultless) {
      Model actual = actual();
      Model with = copy(model);
      String applies = null;
      try {
        if (!effectApplied) {
          op.effect.apply(with, actual);
        }
      } catch (IsolationViolation e) {
        if (outcome == Outcome.ACKNOWLEDGED) {
          throw fail("acknowledged %s, but %s", op, e.getMessage());
        }
        applies = notApplied(e);
      } catch (IllegalStateException e) {
        applies = notApplied(e);
      }
      String withDiff = applies != null ? applies : diff(with, actual);
      String withoutDiff = diff(model, actual);
      if (outcome == Outcome.REJECTED && must != null) {
        Statistics.label("conflict that must not be").collect(applies == null
            ? "effect applies" : "effect does not apply");
        check(applies != null, "%s was rejected (%s), but %s, and its effect applies", op, reason,
            must);
      }
      switch (outcome) {
        case ACKNOWLEDGED:
          check(withDiff == null, "acknowledged %s, but %s", op, withDiff);
          model = with;
          break;
        case UNKNOWN:
          if (withDiff == null) {
            model = with;
            Statistics.label("unknown outcome").collect("took effect");
          } else {
            check(withoutDiff == null,
                "%s has unknown outcome, but the catalog is neither with it (%s) nor without it (%s)",
                op, withDiff, withoutDiff);
            Statistics.label("unknown outcome").collect("did not take effect");
          }
          break;
        default:
          check(withoutDiff == null || withDiff != null,
              "%s reported %s (%s) but took effect", op, outcome, reason);
          check(withoutDiff == null, "%s reported %s (%s), but %s", op, outcome, reason,
              withoutDiff);
      }
      // A kept view a catalog transaction dropped, with effect, left a metadata file that holds it
      for (String key : new ArrayList<>(model.keptViews.keySet())) {
        if (key.startsWith(KEPT_BY_A_TRANSACTION)) {
          ViewState kept = model.keptViews.remove(key);
          String file = heldFile(kept);
          check(file != null, "%s dropped %s, kept (gc.enabled false), but left no metadata file of"
              + " it", op, key.substring(KEPT_BY_A_TRANSACTION.length()));
          model.keptViews.put(file, kept);
        }
      }
      // Observe what the model cannot predict: a view's metadata location
      model.views.forEach((id, state) -> {
        ViewState now = actual.views.get(id);
        state.metadataLocation = now.metadataLocation;
        state.location = now.location;
      });
      // and a table's
      model.forEach(
          (id, state) -> {
            TableState now = actual.get(id);
            state.deletes.keySet().removeIf(d -> state.maybeDropped.contains(d)
                && !now.deletes.containsKey(d));
            state.maybeDropped.clear();
            state.snapshotId = now.snapshotId;
            state.metadataLocation = now.metadataLocation;
            state.lastSequenceNumber = now.lastSequenceNumber;
            state.sequenceNumbers.clear();
            state.sequenceNumbers.putAll(now.sequenceNumbers);
            state.refIds.clear();
            state.refIds.putAll(now.refIds);
          });
      // A table dropped keeping its files, by an operation that took effect, can be registered
      // again (errata T4)
      if (outcome == Outcome.ACKNOWLEDGED || outcome == Outcome.UNKNOWN && withDiff == null) {
        for (TableIdentifier kept : op.keeping) {
          TableState was = lastActual.get(kept);
          if (was != null) {
            checkKept(kept, was);
          }
        }
      }
      checkHistory(cmd, op, outcome, preempted, faultless, actual);
      lastActual = actual;
    }

    /**
     * The history oracles (the 2026-11-17 review's A2), from the last check to {@code actual}:
     * snapshots leave a table only across an acknowledged or unknown expiry, exactly those it chose
     * if nothing raced it and no fault touched it, and never come back; an operation rejected or
     * failed that nothing preempted leaves every table's metadata as it was; a re-inlining changes
     * none but its manifest lists' locations, its location and its metadata log.
     */
    private void checkHistory(Cmd cmd, Op op, Outcome outcome, boolean preempted,
        boolean faultless, Model actual) {
      Map<String, TableState> before = new HashMap<>();
      lastActual.values().forEach(state -> before.put(state.uuid, state));
      boolean expiry = (cmd.kind == Kind.EXPIRE_SNAPSHOTS || cmd.kind == Kind.EXPIRE_OLD)
          && (outcome == Outcome.ACKNOWLEDGED || outcome == Outcome.UNKNOWN);
      for (Map.Entry<TableIdentifier, TableState> e : actual.entrySet()) {
        TableState now = e.getValue();
        Set<Long> gone = goneSnapshots.computeIfAbsent(now.uuid, k -> new TreeSet<>());
        Set<Long> back = new TreeSet<>(now.snapshotIds);
        back.retainAll(gone);
        check(back.isEmpty(), "after %s, %s has snapshots %s again, which it lost before", op,
            e.getKey(), back);
        TableState was = before.get(now.uuid);
        if (was == null) {
          continue;
        }
        Set<Long> removed = new TreeSet<>(was.snapshotIds);
        removed.removeAll(now.snapshotIds);
        gone.addAll(removed);
        check(removed.isEmpty() || expiry, "%s (%s) removed snapshots %s of %s, which only an"
            + " expiry does", op, outcome, removed, e.getKey());
        if (expiry && outcome == Outcome.ACKNOWLEDGED && !preempted && faultless
            && e.getKey().equals(op.expiring)) {
          check(removed.equals(op.chosen), "%s, which nothing raced, chose %s and removed %s",
              op, op.chosen, removed);
          Statistics.label("expiry removed what it chose").collect(
              removed.isEmpty() ? "nothing" : "snapshots");
        }
        if ((outcome == Outcome.REJECTED || outcome == Outcome.FAILED) && !preempted) {
          String then = metadataJson(was.json, false);
          String nowJson = metadataJson(now.json, false);
          check(then.equals(nowJson), "%s reported %s, but the metadata of %s changed:%n  %s%n  %s",
              op, outcome, e.getKey(), then, nowJson);
        }
        if (cmd.kind == Kind.REINLINE) {
          String then = metadataJson(was.json, true);
          String nowJson = metadataJson(now.json, true);
          check(then.equals(nowJson), "%s (%s) changed the metadata of %s:%n  %s%n  %s", op,
              outcome, e.getKey(), then, nowJson);
        }
      }
    }

    /**
     * Checks each catalog file written since the last check against a checkpoint of it; on a
     * shared log, every checkpoint stored against a replay of the log through it.
     */
    private void checkCheckpoints() {
      if (sharedLog != null || azure != null) {
        try {
          if (sharedLog != null) {
            CheckpointRoundTrip.assertEveryCheckpoint(sharedLog);
          } else {
            CheckpointRoundTrip.assertEveryCheckpoint(azureHistory(), "the Azure log");
          }
        } catch (AssertionError e) {
          throw fail("%s", e.getMessage());
        }
        return;
      }
      List<InMemoryAtomicFileIO.Write> history = io.history();
      for (; checkedWrites < history.size(); checkedWrites++) {
        InMemoryAtomicFileIO.Write write = history.get(checkedWrites);
        if (write.location().equals(CATALOG_LOCATION)) {
          String difference;
          try {
            difference = CheckpointRoundTrip.difference(write);
          } catch (RuntimeException e) {
            AssertionError error = fail("catalog write #%d (%s) does not replay: %s",
                checkedWrites, write.strategy(), e);
            error.initCause(e);
            throw error;
          }
          check(difference == null,
              "a checkpoint of catalog write #%d (%s) differs from it: %s",
              checkedWrites, write.strategy(), difference);
        }
      }
      try {
        CheckpointRoundTrip.assertPagesExist(io);
      } catch (AssertionError e) {
        throw fail("%s", e.getMessage());
      }
    }

    /**
     * The catalog's state, read without faults; fails if a table does not load whole, or loads
     * differently through a fresh reader, if a table is stored both inline and in a metadata file,
     * if a snapshot's manifest list or row ids changed, if its numbering is off, if a v3 table's
     * row ids are missing or overlap, or if a catalog file written since the last check differs
     * from a checkpoint of it.
     */
    /**
     * The data or delete files {@code manifest} reaches (its entries but those it deletes), read
     * once: a manifest never changes.
     */
    private List<String> liveFiles(ManifestFile manifest, Map<Integer, PartitionSpec> specs) {
      return manifestFiles.computeIfAbsent(manifest.path(), path -> {
        List<String> files = new ArrayList<>();
        try (CloseableIterable<? extends org.apache.iceberg.ContentFile<?>> entries =
            manifest.content() == org.apache.iceberg.ManifestContent.DATA
                ? org.apache.iceberg.ManifestFiles.read(manifest, io, specs)
                : org.apache.iceberg.ManifestFiles.readDeleteManifest(manifest, io, specs)) {
          entries.forEach(file -> files.add(file.location()));
        } catch (java.io.IOException e) {
          throw new java.io.UncheckedIOException(e);
        }
        return files;
      });
    }

    private Model actual() {
      checkCheckpoints();
      Model tables = new Model();
      CatalogState file = (sharedLog != null
          ? new CatalogLog(sharedLog, LogCatalogProperties.runtimeConfig(PROPERTIES))
          : azure != null
              ? new CatalogLog(
                  AzureLogs.open(io, CATALOG_LOCATION, azure, CATALOG_LOCATION, readerProperties),
                  LogCatalogProperties.runtimeConfig(PROPERTIES))
              : CatalogTestSupport.log(io, CATALOG_LOCATION, PROPERTIES)).read().state();
      for (TableIdentifier id : file.tables()) {
        mostSealed = Math.max(mostSealed, file.sealedBlocks(file.tableId(id)).size());
      }
      lastState = file;
      List<TableIdentifier> ids = new ArrayList<>();
      for (Namespace ns : checker.listNamespaces(Namespace.empty())) {
        tables.namespaces.put(ns, new TreeMap<>(checker.loadNamespaceMetadata(ns)));
        ids.addAll(checker.listTables(ns));
      }
      check(file.tables().size() == ids.size(),
          "the catalog has tables %s, but its namespaces list %s", file.tables(), ids);
      // A reader that has read nothing before: the checker's loads reuse what its earlier loads
      // made (LogCatalog.InlineSnapshots, InlineTable's metadata per version), and must read alike
      LogCatalog cold = LocalCatalogMode.readerFor(io, CATALOG_LOCATION)
          .catalog(io, WAREHOUSE, CATALOG_NAME, readerProperties);
      for (TableIdentifier id : ids) {
        TableState state;
        try {
          Table table = checker.loadTable(id);
          TableMetadata metadata = ((HasTableOperations) table).operations().current();
          Table coldTable = cold.loadTable(id);
          TableMetadata coldMetadata = ((HasTableOperations) coldTable).operations().current();
          check(TableMetadataParser.toJson(coldMetadata).equals(TableMetadataParser.toJson(metadata)),
              "%s loads differently through a fresh reader:%n  %s%n  %s", id,
              TableMetadataParser.toJson(metadata), TableMetadataParser.toJson(coldMetadata));
          for (Snapshot snapshot : coldTable.snapshots()) {
            check(describe(snapshot, io).equals(describe(metadata.snapshot(snapshot.snapshotId()), io)),
                "%s: snapshot %d loads differently through a fresh reader: %s, not %s", id,
                snapshot.snapshotId(), describe(snapshot, io),
                describe(metadata.snapshot(snapshot.snapshotId()), io));
          }
          checkNumbering(id, metadata);
          state = new TableState(metadata.uuid(), metadata.formatVersion());
          state.json = TableMetadataParser.toJson(metadata);
          for (Snapshot snapshot : metadata.snapshots()) {
            state.snapshotIds.add(snapshot.snapshotId());
          }
          metadata.refs().forEach((name, ref) -> state.refIds.put(name, ref.snapshotId()));
          check(file.location(id) == null || !file.isInlineTable(id),
              "%s has both a metadata file and inline metadata", id);
          String stored = file.isInlineTable(id) ? "inline" : "metadata file";
          String before = storage.put(metadata.uuid(), stored);
          if (before != null && !before.equals(stored)) {
            Statistics.label("storage change").collect(before + " -> " + stored);
          }
          Statistics.label("tables read").collect(
              String.format("v%d %s%s", metadata.formatVersion(), stored,
                  table.currentSnapshot() == null ? ", no snapshot" : ""));
          Map<Long, List<String>> lists =
              manifestLists.computeIfAbsent(metadata.uuid(), k -> new HashMap<>());
          // Each ref names a snapshot the table has, each snapshot a schema it has, and each
          // manifest a partition spec it has (Iceberg issue C11, Bug 69): a reader that time
          // travels to a snapshot, or plans a branch, needs them.
          metadata.refs().forEach((name, ref) -> check(metadata.snapshot(ref.snapshotId()) != null,
              "%s: ref %s names snapshot %d, which the table does not have",
              id, name, ref.snapshotId()));
          for (Snapshot snapshot : table.snapshots()) {
            String list = snapshot.manifestListLocation();
            check(list == null ? snapshot instanceof org.apache.iceberg.InlineSnapshot
                    : io.newInputFile(list).exists(),
                "%s: snapshot %d has no manifest list: %s", id, snapshot.snapshotId(), list);
            check(snapshot.schemaId() == null
                    || metadata.schemasById().containsKey(snapshot.schemaId()),
                "%s: snapshot %d names schema %d, which the table does not have",
                id, snapshot.snapshotId(), snapshot.schemaId());
            for (ManifestFile manifest : snapshot.allManifests(io)) {
              check(io.newInputFile(manifest.path()).exists(),
                  "%s: snapshot %d references missing manifest %s",
                  id, snapshot.snapshotId(), manifest.path());
              // and every data and delete file it reaches (A7: an expiry's or a purge's cleanup
              // must spare the files a snapshot still reaches)
              for (String reached : liveFiles(manifest, metadata.specsById())) {
                check(io.newInputFile(reached).exists(),
                    "%s: snapshot %d reaches %s through manifest %s, but the file is gone", id,
                    snapshot.snapshotId(), reached, manifest.path());
              }
              check(metadata.specsById().containsKey(manifest.partitionSpecId()),
                  "%s: snapshot %d has manifest %s of partition spec %d, which the table does not "
                      + "have", id, snapshot.snapshotId(), manifest.path(),
                  manifest.partitionSpecId());
            }
            List<String> manifests = describe(snapshot, io);
            List<String> first = lists.putIfAbsent(snapshot.snapshotId(), manifests);
            check(first == null || first.equals(manifests),
                "%s: the manifest list of snapshot %d changed from %s to %s",
                id, snapshot.snapshotId(), first, manifests);
          }
          List<DataFile> live = new ArrayList<>();
          try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
              state.files.add(task.file().location());
              state.sequenceNumbers.put(task.file().location(), task.file().dataSequenceNumber());
              live.add(task.file());
            }
          }
          checkRowLineage(id, metadata, table.currentSnapshot(), live);
          for (Map.Entry<String, org.apache.iceberg.SnapshotRef> r : metadata.refs().entrySet()) {
            if (r.getKey().equals(org.apache.iceberg.SnapshotRef.MAIN_BRANCH)) {
              continue;
            }
            Set<String> refFiles = new TreeSet<>();
            try (CloseableIterable<FileScanTask> tasks =
                table.newScan().useRef(r.getKey()).planFiles()) {
              tasks.forEach(task -> refFiles.add(task.file().location()));
            }
            if (r.getValue().isBranch()) {
              state.branches.put(r.getKey(), refFiles);
            } else {
              state.tags.put(r.getKey(), refFiles);
              state.tagSnapshots.put(r.getKey(), r.getValue().snapshotId());
            }
          }
          if (table.currentSnapshot() != null) {
            for (ManifestFile m : table.currentSnapshot().deleteManifests(io)) {
              check(m.firstRowId() == null, "%s: delete manifest %s has first row id %s",
                  id, m.path(), m.firstRowId());
              try (org.apache.iceberg.ManifestReader<org.apache.iceberg.DeleteFile> reader =
                  org.apache.iceberg.ManifestFiles.readDeleteManifest(m, io, table.specs())) {
                for (org.apache.iceberg.DeleteFile f : reader) {
                  state.deletes.put(f.location(),
                      f.referencedDataFile() == null ? "" : f.referencedDataFile());
                  state.sequenceNumbers.put(f.location(), f.dataSequenceNumber());
                }
              }
            }
          }
          table.properties().forEach((k, v) -> {
            if (k.startsWith(KEY_PREFIX)) {
              state.props.put(k, v);
            }
          });
          table.schema().columns().forEach(c -> {
            if (!c.name().equals("id")) {
              state.columns.add(c.name());
            }
          });
          state.snapshotId =
              table.currentSnapshot() == null ? null : table.currentSnapshot().snapshotId();
          state.metadataLocation = metadata.metadataFileLocation();
          state.lastSequenceNumber = metadata.lastSequenceNumber();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        } catch (RuntimeException e) {
          throw fail("%s does not load: %s", id, e);
        }
        tables.put(id, state);
      }
      // Views (docs/VIEWS.md): each loads alike through the checker and a fresh reader
      List<TableIdentifier> views = new ArrayList<>();
      for (Namespace ns : tables.namespaces.keySet()) {
        views.addAll(checker.listViews(ns));
      }
      check(file.views().size() == views.size(),
          "the catalog has views %s, but its namespaces list %s", file.views(), views);
      for (TableIdentifier id : views) {
        try {
          org.apache.iceberg.view.ViewMetadata metadata =
              ((org.apache.iceberg.view.BaseView) checker.loadView(id)).operations().current();
          org.apache.iceberg.view.ViewMetadata coldMetadata =
              ((org.apache.iceberg.view.BaseView) cold.loadView(id)).operations().current();
          check(org.apache.iceberg.view.ViewMetadataParser.toJson(metadata).equals(
                  org.apache.iceberg.view.ViewMetadataParser.toJson(coldMetadata)),
              "view %s loads differently through a fresh reader", id);
          check(!tables.containsKey(id), "%s is a table and a view", id);
          tables.views.put(id, viewState(metadata));
          Statistics.label("views read").collect(
              metadata.metadataFileLocation().startsWith("inline-view://") ? "inline" : "file");
        } catch (RuntimeException e) {
          throw fail("view %s does not load: %s", id, e);
        }
      }
      return tables;
    }

    /**
     * Marks in {@code state} the delete files that a commit by a merging producer (a merge append,
     * a delete, a row delta) may drop: Iceberg drops those older than the base's last sequence
     * number and than the minimum sequence number of every data manifest, which can no longer
     * apply to a row. The manifests' minimums also count deleted entries, so they are not known
     * here; the minimum over live data files bounds them, so a delete file at or above it must stay
     * and one below it may go. {@code state}'s sequence numbers are as last observed, before the
     * commit; {@code now}'s after it.
     */
    private static void ageOutDeletes(TableState state, TableState now) {
      if (now == null) {
        return;
      }
      long min = state.lastSequenceNumber;
      for (String file : now.files) {
        Long seq = now.sequenceNumbers.get(file);
        if (seq != null) {
          min = Math.min(min, seq);
        }
      }
      for (String d : state.deletes.keySet()) {
        Long seq = state.sequenceNumbers.get(d);
        if (seq != null && seq < min) {
          state.maybeDropped.add(d);
        }
      }
    }

    /** Whether {@code snapshot} is {@code ancestor} or descends from it, by the parents seen. */
    private static boolean descendsFrom(Map<Long, Long> parentOf, long snapshot, long ancestor) {
      Set<Long> visited = new HashSet<>();
      for (Long s = snapshot; s != null && visited.add(s); s = parentOf.get(s)) {
        if (s == ancestor) {
          return true;
        }
      }
      return false;
    }

    /**
     * Checks the row ids of a table's live data files: in a v3 table whose current snapshot has
     * row ids, every file has them, and their ranges are disjoint and below the next row id. In
     * any table, a file's first row id never changes along main's lineage once it has one. A file
     * written before the table's upgrade to v3 gets its row ids in each lineage that first carries
     * it into a v3 snapshot, so after main moves to another lineage, such as a branch from before
     * the upgrade, it may have others (ICEBERG_ISSUES.md, A5).
     */
    private void checkRowLineage(
        TableIdentifier id, TableMetadata metadata, Snapshot current, List<DataFile> live) {
      Map<Long, Long> parentOf = parents.computeIfAbsent(metadata.uuid(), k -> new HashMap<>());
      metadata.snapshots().forEach(s -> parentOf.put(s.snapshotId(), s.parentId()));
      Map<String, long[]> seen = rowIds.computeIfAbsent(metadata.uuid(), k -> new HashMap<>());
      for (DataFile f : live) {
        if (f.firstRowId() != null) {
          long[] first = seen.get(f.location());
          if (first != null && first[0] != f.firstRowId()) {
            boolean sameLineage = descendsFrom(parentOf, current.snapshotId(), first[1]);
            check(!sameLineage, "%s: the first row id of %s changed from %d to %d",
                id, f.location(), first[0], f.firstRowId());
            Statistics.label("row ids of another lineage").collect(true);
          }
          seen.put(f.location(), new long[] {f.firstRowId(), current.snapshotId()});
        }
      }
      if (metadata.formatVersion() < 3 || current == null || current.firstRowId() == null) {
        return; // no row ids yet: a table upgraded to v3 gets them with its next snapshot
      }
      List<DataFile> byFirstRowId = new ArrayList<>();
      for (DataFile f : live) {
        check(f.firstRowId() != null, "%s: %s has no row ids in snapshot %d",
            id, f.location(), current.snapshotId());
        byFirstRowId.add(f);
      }
      byFirstRowId.sort(Comparator.comparing(DataFile::firstRowId));
      long next = 0;
      String previous = null;
      for (DataFile f : byFirstRowId) {
        check(f.firstRowId() >= next, "%s: the row ids of %s and %s overlap (%d < %d)",
            id, previous, f.location(), f.firstRowId(), next);
        next = f.firstRowId() + f.recordCount();
        previous = f.location();
      }
      check(next <= metadata.nextRowId(), "%s: row ids up to %d, but the next row id is %d",
          id, next, metadata.nextRowId());
      Statistics.label("row ids checked").collect(live.isEmpty() ? "no files" : "files");
    }

    /** A snapshot's fields, and each manifest of its list. */
    private static List<String> describe(Snapshot snapshot, FileIO io) {
      List<String> described = new ArrayList<>();
      described.add(String.format(
          "seq=%d parent=%s ts=%d op=%s schema=%s first-row-id=%s added-rows=%s summary=%s",
          snapshot.sequenceNumber(), snapshot.parentId(), snapshot.timestampMillis(),
          snapshot.operation(), snapshot.schemaId(), snapshot.firstRowId(),
          snapshot.addedRows(), new TreeMap<>(snapshot.summary())));
      for (ManifestFile manifest : snapshot.allManifests(io)) {
        described.add(describe(manifest));
      }
      return described;
    }

    /**
     * Checks the numbers a commit assigns, which replay assigns again to a commit it merges onto
     * a table that changed (Landing): from v2, a snapshot's sequence number is above its parent's,
     * at most the table's last, and no other snapshot's; its manifests' are at most its own and
     * the ones it wrote have its own. In v3, the row ids of its snapshots are apart, and below
     * the table's next.
     */
    private void checkNumbering(TableIdentifier id, TableMetadata metadata) {
      if (metadata.formatVersion() < 2) {
        return;
      }
      Map<Long, Long> numbered = new HashMap<>();
      TreeMap<Long, Snapshot> byFirstRowId = new TreeMap<>();
      for (Snapshot snapshot : metadata.snapshots()) {
        long seq = snapshot.sequenceNumber();
        check(seq <= metadata.lastSequenceNumber(),
            "%s: snapshot %d has sequence number %d, above the table's last, %d",
            id, snapshot.snapshotId(), seq, metadata.lastSequenceNumber());
        Long other = numbered.put(seq, snapshot.snapshotId());
        check(other == null, "%s: snapshots %d and %d have sequence number %d",
            id, other, snapshot.snapshotId(), seq);
        Snapshot parent =
            snapshot.parentId() == null ? null : metadata.snapshot(snapshot.parentId());
        check(parent == null || parent.sequenceNumber() < seq,
            "%s: snapshot %d has sequence number %d, its parent %d %d", id,
            snapshot.snapshotId(), seq, snapshot.parentId(),
            parent == null ? -1 : parent.sequenceNumber());
        for (ManifestFile m : snapshot.allManifests(io)) {
          check(m.minSequenceNumber() <= m.sequenceNumber() && m.sequenceNumber() <= seq,
              "%s: snapshot %d (sequence number %d) lists %s, sequence numbers %d/%d",
              id, snapshot.snapshotId(), seq, m.path(), m.minSequenceNumber(),
              m.sequenceNumber());
          check(m.snapshotId() == null || m.snapshotId() != snapshot.snapshotId()
                  || m.sequenceNumber() == seq,
              "%s: snapshot %d (sequence number %d) wrote %s with sequence number %d",
              id, snapshot.snapshotId(), seq, m.path(), m.sequenceNumber());
        }
        if (snapshot.firstRowId() != null && snapshot.addedRows() != null
            && snapshot.addedRows() > 0) {
          byFirstRowId.put(snapshot.firstRowId(), snapshot);
        }
      }
      long next = 0;
      Snapshot previous = null;
      for (Snapshot snapshot : byFirstRowId.values()) {
        check(snapshot.firstRowId() >= next,
            "%s: the row ids of snapshots %d and %d overlap (%d < %d)", id,
            previous == null ? null : previous.snapshotId(), snapshot.snapshotId(),
            snapshot.firstRowId(), next);
        next = snapshot.firstRowId() + snapshot.addedRows();
        previous = snapshot;
      }
      check(next <= metadata.nextRowId(),
          "%s: snapshots' row ids up to %d, but the next row id is %d", id, next,
          metadata.nextRowId());
    }

    private static String describe(ManifestFile m) {
      return String.format(
          "%s(%s spec=%d seq=%d/%d snap=%d files=%d/%d/%d rows=%d/%d/%d first-row-id=%s"
              + " partitions=%s)",
          m.path().substring(m.path().lastIndexOf('/') + 1), m.content(), m.partitionSpecId(),
          m.sequenceNumber(), m.minSequenceNumber(), m.snapshotId(),
          m.addedFilesCount(), m.existingFilesCount(), m.deletedFilesCount(),
          m.addedRowsCount(), m.existingRowsCount(), m.deletedRowsCount(), m.firstRowId(),
          m.partitions() == null ? null : m.partitions().stream()
              .map(p -> String.format("%s/%s/%s/%s", p.containsNull(), p.containsNaN(),
                  hex(p.lowerBound()), hex(p.upperBound())))
              .collect(java.util.stream.Collectors.toList()));
    }

    private static String hex(java.nio.ByteBuffer buffer) {
      if (buffer == null) {
        return null;
      }
      StringBuilder out = new StringBuilder();
      java.nio.ByteBuffer copy = buffer.duplicate();
      while (copy.hasRemaining()) {
        out.append(String.format("%02x", copy.get()));
      }
      return out.toString();
    }

    private AssertionError unexpected(RuntimeException e) {
      AssertionError error =
          new AssertionError("unexpected " + e + " without an injected fault\ntrace:\n" + trace);
      error.initCause(e);
      return error;
    }

    private void check(boolean condition, String format, Object... args) {
      if (!condition) {
        throw fail(format, args);
      }
    }

    private AssertionError fail(String format, Object... args) {
      return new AssertionError(String.format(format, args) + "\ntrace:\n" + trace);
    }
  }

  /** A catalog transaction acknowledged although a table it depends on changed. */
  private static final class IsolationViolation extends IllegalStateException {
    IsolationViolation(String message) {
      super(message);
    }
  }

  /**
   * A table's metadata as JSON ({@link TableMetadataParser#toJson}) less its metadata log, which a
   * commit's storage moves; and with {@code storage} less the table's location and its snapshots'
   * manifest lists' locations too, which a re-inlining moves (a list file's snapshot comes back
   * inline).
   */
  private static String metadataJson(String json, boolean storage) {
    try {
      com.fasterxml.jackson.databind.node.ObjectNode node =
          (com.fasterxml.jackson.databind.node.ObjectNode)
              org.apache.iceberg.util.JsonUtil.mapper().readTree(json);
      node.remove("metadata-log");
      if (storage) {
        node.remove("location");
        if (node.has("snapshots")) {
          for (com.fasterxml.jackson.databind.JsonNode snapshot : node.get("snapshots")) {
            ((com.fasterxml.jackson.databind.node.ObjectNode) snapshot).remove("manifest-list");
          }
        }
      }
      return org.apache.iceberg.util.JsonUtil.mapper().writeValueAsString(node);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String firstLine(Throwable e) {
    String message = String.valueOf(e.getMessage());
    int newline = message.indexOf('\n');
    return newline < 0 ? message : message.substring(0, newline);
  }
}
