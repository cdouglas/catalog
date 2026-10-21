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
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.InMemoryAtomicFileIO.Fault;
import org.apache.iceberg.io.InMemoryAtomicFileIO.ReadFault;
import org.apache.iceberg.types.Types;

/**
 * Multi-writer fuzzer for {@link FileIOCatalog} on faulty storage, through Iceberg's table and
 * catalog APIs: the tier above {@code TestMultiWriterProperties}, which drives the catalog format
 * directly.
 *
 * <p>A generated schedule runs, on one thread, operations by several writers, each with its own
 * catalog instance and table handles over shared {@link InMemoryAtomicFileIO} storage: fast and
 * merge appends, deletes, row-level deletes (equality, position, deletion vectors), property,
 * schema and partition-spec changes, table transactions, snapshot expiry (of everything older than
 * now or than one of the table's snapshots, of one snapshot by id, or of what the retention of the
 * table and its refs alone expires) and those retention settings, manifest rewrites, table
 * create / drop / rename (across namespaces too) / replace, upgrades from format v2 to v3,
 * namespace create / drop and property changes, and multi-table catalog transactions whose
 * loads, appends and commit are separate steps, so other writers' operations run in between. A
 * command may also preempt the one before it: it runs to completion just before one of that
 * operation's reads of the catalog or its first conditional write, so the two race. Each writer
 * has its own catalog mode (append / CAS / compaction, pointer / inline TM / inline ML), or all
 * writers share one. A small limit on inline updates moves tables out of inline storage and back:
 * a big property evicts a table to a metadata file, and a later update that fits in full returns
 * it.
 *
 * <p>In {@link #faultyStorageNeverMisreportsACommit}, each operation's storage also fails as its
 * command says: its first two conditional writes (see {@link InMemoryAtomicFileIO.Fault}) and one
 * of its reads (see {@link InMemoryAtomicFileIO.ReadFault}). Reads of {@code metadata.json} do not
 * fail, as Iceberg retries them after 100 ms; nor do reads on Iceberg's worker threads, which
 * would make the schedule nondeterministic. A preempting operation runs without faults. Storage may
 * also be strict about versions, as it is in practice: a read pinned to a replaced version fails
 * (see {@link InMemoryAtomicFileIO#failSupersededReads}).
 *
 * <p>Tables are created in format v2 or v3. A model holds the namespaces and their
 * properties, and each table's UUID, format version, live data and delete files, properties and
 * added columns. After every operation the catalog, read back without faults, must match it:
 *
 * <ul>
 *   <li>An acknowledged operation took effect once, and nothing else changed.
 *   <li>An operation that failed any way but {@link CommitStateUnknownException} did not take
 *       effect. Iceberg deletes a commit's new files on a {@code CleanableFailure} such as {@link
 *       CommitFailedException}, so a commit reported that way that took effect loses them.
 *   <li>An operation whose outcome is unknown took effect entirely or not at all, and a fault was
 *       injected into it, unless a writer compacts with a retention of 0 (which can put a commit
 *       in flight at or below the committed set's horizon).
 *   <li>Every table is in a namespace; every table loads; every snapshot has its manifest list,
 *       inline or in a file, and every manifest exists; a snapshot's manifest list never changes;
 *       no table is stored both inline and in a metadata file.
 *   <li>Every version of the catalog file that a writer wrote reads the same as a checkpoint of
 *       it, which the next compaction would write (see {@link CheckpointRoundTrip}).
 *   <li>Row lineage, in a v3 table: once a snapshot has row ids, every live data file has them; the
 *       files' ranges are disjoint and below the table's next row id; a file's first row id never
 *       changes along main's lineage, nor do a snapshot's first row id and added rows.
 *   <li>An acknowledged catalog transaction saw no concurrent change it must not miss: any change
 *       to a table it appended to, since it first loaded the table (errata D12); and, under
 *       serializable isolation, to the main branch (or, without one, the metadata) of any table it
 *       loaded.
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
  private static final Schema SCHEMA = new Schema(required(1, "id", Types.IntegerType.get()));
  private static final String KEY_PREFIX = "fz.";
  private static final List<String> KEYS = List.of("fz.k1", "fz.k2");
  private static final String BIG_KEY = "fz.big";
  private static final String WAREHOUSE = "mem:///warehouse";
  private static final String CATALOG_NAME = "fuzz";
  private static final String CATALOG_LOCATION = WAREHOUSE + "/catalog-" + CATALOG_NAME;

  // Retry commits and throttled writes without sleeping; merge manifests early, so merge appends
  // read them; keep two previous metadata files, so pointer-mode commits delete older ones.
  private static final Map<String, String> PROPERTIES =
      Map.of(
          ProtoCatalogFormat.THROTTLE_MIN_WAIT_MS, "0",
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
      @ForAll("modes") List<LocalCatalogMode> modes,
      @ForAll @IntRange(min = 2, max = 3) int writers,
      @ForAll("maxUpdateSizes") int maxUpdateSize,
      @ForAll boolean strictVersions,
      @ForAll @IntRange(min = 0, max = 7) int gzipWriters,
      @ForAll @IntRange(min = 0, max = 7) int gcWriters,
      @ForAll("segmentLimits") int segmentLimit,
      @ForAll("schedules") List<Cmd> schedule) {
    new Run(modes.subList(0, writers), maxUpdateSize, false, strictVersions, gzipWriters,
        gcWriters, segmentLimit)
        .execute(schedule);
  }

  @Property(tries = 100)
  void faultyStorageNeverMisreportsACommit(
      @ForAll("modes") List<LocalCatalogMode> modes,
      @ForAll @IntRange(min = 2, max = 3) int writers,
      @ForAll("maxUpdateSizes") int maxUpdateSize,
      @ForAll boolean strictVersions,
      @ForAll @IntRange(min = 0, max = 7) int gzipWriters,
      @ForAll @IntRange(min = 0, max = 7) int gcWriters,
      @ForAll("segmentLimits") int segmentLimit,
      @ForAll("schedules") List<Cmd> schedule) {
    new Run(modes.subList(0, writers), maxUpdateSize, true, strictVersions, gzipWriters,
        gcWriters, segmentLimit)
        .execute(schedule);
  }

  /** Each writer's mode: one for all, a quarter of the time, else each its own. */
  @Provide
  Arbitrary<List<LocalCatalogMode>> modes() {
    Arbitrary<LocalCatalogMode> mode = Arbitraries.of(LocalCatalogMode.class);
    return Arbitraries.frequencyOf(
        Tuple.of(1, mode.map(m -> List.of(m, m, m))),
        Tuple.of(3, mode.list().ofSize(3)));
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
            Tuple.of(4, Kind.FAST_APPEND),
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
            Tuple.of(1, Kind.REMOVE_REF),
            Tuple.of(1, Kind.STAGE_APPEND),
            Tuple.of(1, Kind.CHERRY_PICK),
            Tuple.of(2, Kind.TABLE_TXN),
            Tuple.of(2, Kind.EXPIRE_SNAPSHOTS),
            Tuple.of(1, Kind.SET_RETENTION),
            Tuple.of(1, Kind.REWRITE_MANIFESTS),
            Tuple.of(1, Kind.CREATE_NAMESPACE),
            Tuple.of(1, Kind.DROP_NAMESPACE),
            Tuple.of(2, Kind.SET_NAMESPACE_PROPERTY),
            Tuple.of(1, Kind.REMOVE_NAMESPACE_PROPERTY),
            Tuple.of(3, Kind.TXN_BEGIN),
            Tuple.of(4, Kind.TXN_APPEND),
            Tuple.of(2, Kind.TXN_READ),
            Tuple.of(3, Kind.TXN_COMMIT));
    Arbitrary<Integer> small = Arbitraries.integers().between(0, 11);
    Arbitrary<Integer> preempt =
        Arbitraries.frequency(
            Tuple.of(12, -1),
            Tuple.of(2, 0),
            Tuple.of(2, 1),
            Tuple.of(2, 2),
            Tuple.of(1, 3),
            Tuple.of(1, 4));
    // b and c together: a combinator takes at most eight arbitraries
    Arbitrary<int[]> operands = Combinators.combine(small, small).as((b, c) -> new int[] {b, c});
    return Combinators.combine(
            kinds,
            Arbitraries.integers().between(0, 2),
            small,
            operands,
            Arbitraries.of(true, false),
            preempt,
            Arbitraries.integers().between(0, 143),
            small)
        .as((kind, writer, a, bc, fresh, pre, writeFaults, readFault) ->
            new Cmd(kind, writer, a, bc[0], bc[1], fresh, pre, writeFaults, readFault))
        .list()
        .ofMinSize(1)
        .ofMaxSize(40)
        .withSizeDistribution(RandomDistribution.uniform());
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
     * Sets a retention, as {@code c} picks: a branch's minimum number of snapshots or maximum
     * snapshot age, a ref's maximum age, or a table property that sets one for all.
     */
    SET_RETENTION,
    REWRITE_MANIFESTS,
    /** Creates ns or ns2, as {@code b} picks, with a property when {@code b} is odd. */
    CREATE_NAMESPACE,
    /** Drops ns or ns2, as {@code b} picks, which fails unless it is empty. */
    DROP_NAMESPACE,
    /** Sets fz.k1 or fz.k2 on ns or ns2, as {@code b} picks. */
    SET_NAMESPACE_PROPERTY,
    /** Removes fz.k1 or fz.k2 from ns or ns2, as {@code b} picks. */
    REMOVE_NAMESPACE_PROPERTY,
    /** Opens a catalog transaction; {@code b} picks the isolation level. */
    TXN_BEGIN,
    /** Loads a table in the open catalog transaction and stages an append to it. */
    TXN_APPEND,
    /** Loads a table in the open catalog transaction. */
    TXN_READ,
    TXN_COMMIT
  }

  /**
   * One generated command. {@code a} and {@code b} pick operands by index, interpreted against the
   * model when the command runs: a table picks from the model when {@code a < 8} and from the
   * whole universe otherwise, which reaches tables that do not exist. {@code fresh} reloads the
   * writer's handle on the table first; otherwise the writer reuses the handle it has, which may
   * be stale. {@code preempt}, if not negative, runs this command just before storage request
   * number {@code preempt} (reads of the catalog and the first conditional write) of the command
   * before it, if that is another writer's and gets that far. On faulty storage, {@code
   * writeFaults} and {@code readFault} pick the command's faults.
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

    Cmd(Kind kind, int writer, int a, int b, int c, boolean fresh, int preempt, int writeFaults,
        int readFault) {
      this.kind = kind;
      this.writer = writer;
      this.a = a;
      this.b = b;
      this.c = c;
      this.fresh = fresh;
      this.preempt = preempt;
      this.writeFaults = writeFaults;
      this.readFault = readFault;
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
          writer, kind, a, b, c, fresh ? ",fresh" : "", preempt >= 0 ? "@" + preempt : "",
          writeFaults != 0 || readFault != 0 ? "!" + writeFaults + "/" + readFault : "");
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

    Model() {
      super(BY_NAME);
    }

    @Override
    public String toString() {
      return "namespaces " + namespaces + ", tables " + super.toString();
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
    // remove them. A name stays here once added, as a replace can put back a ref removed since.
    final Set<String> agingRefs = new TreeSet<>();
    boolean refsAge = false;
    // Observed, not modeled: Iceberg chooses snapshot ids, metadata locations and sequence numbers
    // (of the table, and the data sequence numbers of its live data and delete files).
    Long snapshotId = null;
    String metadataLocation = null;
    long lastSequenceNumber = 0;
    final Map<String, Long> sequenceNumbers = new HashMap<>();

    TableState(String uuid, int formatVersion) {
      this.uuid = uuid;
      this.formatVersion = formatVersion;
    }

    TableState copy() {
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
    return copy;
  }

  /** How {@code actual} differs from {@code expected}, or null if it matches. */
  private static String diff(Model expected, Model actual) {
    if (!expected.namespaces.equals(actual.namespaces)) {
      return "namespaces " + expected.namespaces + " expected, found " + actual.namespaces;
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

  enum Outcome {
    ACKNOWLEDGED,
    /** A definite no: CommitFailedException, a validation or existence failure. */
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
    final Long snapshotId;
    final String metadataLocation;
    boolean appended = false;

    Loaded(TableMetadata metadata) {
      this.uuid = metadata.uuid();
      this.snapshotId =
          metadata.currentSnapshot() == null ? null : metadata.currentSnapshot().snapshotId();
      this.metadataLocation = metadata.metadataFileLocation();
    }
  }

  private static final class Writer {
    final int id;
    final FileIOCatalog catalog;
    final Map<TableIdentifier, Table> handles = new HashMap<>();
    CatalogTransaction txn = null;
    final Map<TableIdentifier, Loaded> loaded = new LinkedHashMap<>();
    final List<Map.Entry<TableIdentifier, String>> staged = new ArrayList<>();

    Writer(int id, FileIOCatalog catalog) {
      this.id = id;
      this.catalog = catalog;
    }

    void closeTxn() {
      txn = null;
      loaded.clear();
      staged.clear();
    }
  }

  private static final class Run {
    private final int maxUpdateSize;
    private final boolean faults;
    // whether a writer compacts with a retention of 0, which can leave a commit's outcome unknown
    private final boolean gc;
    private final InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    private final Writer[] writers;
    private final FileIOCatalog checker;
    private Model model;
    private final StringBuilder trace = new StringBuilder();
    private int nextFile = 0;
    private int nextValue = 0;
    // Each snapshot's fields and manifest list when first seen, by table UUID and snapshot id: a
    // committed snapshot never changes, whether its list is inline or a file, and whichever writer
    // reads it.
    private final Map<String, Map<Long, List<String>>> manifestLists = new HashMap<>();
    // Each table's storage at the last check, by UUID: inline or a metadata file.
    private final Map<String, String> storage = new HashMap<>();
    // Each data file's first row id, and the snapshot of main it was last seen in, by table UUID:
    // row ids never change along main's lineage.
    private final Map<String, Map<String, long[]>> rowIds = new HashMap<>();
    // The parent of every snapshot a check has seen, by table UUID, which outlives its expiry.
    private final Map<String, Map<Long, Long>> parents = new HashMap<>();
    // Conditional writes whose catalog file has been checked against a checkpoint of it.
    private int checkedWrites = 0;

    // The running top-level command's storage plan. Faults and preemption apply only while armed,
    // and only on the thread that runs the schedule.
    private volatile boolean armed = false;
    private Thread owner;
    private Cmd pending; // the command that may preempt the running one
    private int requests; // reads of the catalog and conditional writes, until the first write
    private boolean wrote;
    private List<Fault> writePlan;
    private int writes;
    private int failingRead;
    private ReadFault readFault;
    private int reads;
    private final List<String> fired = new ArrayList<>();

    /**
     * Writer {@code i} writes inline metadata with the gzip codec if bit {@code i} of {@code
     * gzipWriters} is set, else the structural one: a delta keeps its base's codec, and a full
     * update writes the writer's. Writer {@code i} keeps no committed transaction ids older than
     * its own commit's when it compacts (a retention of 0) if bit {@code i} of {@code gcWriters}
     * is set, else the default 6 hours, which no run reaches. With a {@code segmentLimit}, the
     * catalog object holds at most that many writes, and an append past it fails (Bug 66).
     */
    Run(List<LocalCatalogMode> modes, int maxUpdateSize, boolean faults, boolean strictVersions,
        int gzipWriters, int gcWriters, int segmentLimit) {
      this.maxUpdateSize = maxUpdateSize;
      this.faults = faults;
      this.gc = (gcWriters & ((1 << modes.size()) - 1)) != 0;
      io.failSupersededReads(strictVersions);
      if (segmentLimit > 0) {
        io.limitSegments(segmentLimit);
      }
      Statistics.label("segment limit").collect(segmentLimit);
      Map<String, String> properties = new HashMap<>(PROPERTIES);
      properties.put(FileIOCatalog.INLINE_MAX_UPDATE_SIZE, String.valueOf(maxUpdateSize));
      this.writers = new Writer[modes.size()];
      for (int i = 0; i < modes.size(); i++) {
        Map<String, String> writerProperties = new HashMap<>(properties);
        writerProperties.put(
            FileIOCatalog.INLINE_TM_CODEC, (gzipWriters >> i & 1) == 1 ? "gzip" : "structural");
        if ((gcWriters >> i & 1) == 1) {
          writerProperties.put(ProtoCatalogFormat.COMMITTED_RETENTION_MS, "0");
        }
        this.writers[i] =
            new Writer(i, modes.get(i).catalog(io, WAREHOUSE, CATALOG_NAME, writerProperties));
      }
      // Reading does not depend on a catalog's mode.
      this.checker = LocalCatalogMode.APPEND.catalog(io, WAREHOUSE, CATALOG_NAME, properties);
      FileIOCatalog seed = this.writers[0].catalog;
      seed.createNamespace(NS);
      create(seed, TABLES.get(0), 3);
      create(seed, TABLES.get(1), 2).newFastAppend().appendFile(newFile()).commit();
      this.model = actual();
      trace.append(String.format(
          "modes %s, max update size %d%s, gzip writers %s, gc writers %s, segment limit %d%n"
              + "seed: %s%n",
          modes, maxUpdateSize, strictVersions ? ", strict versions" : "",
          Integer.toBinaryString(gzipWriters), Integer.toBinaryString(gcWriters), segmentLimit,
          model));
      Statistics.label("gc writers")
          .collect(Integer.bitCount(gcWriters & ((1 << modes.size()) - 1)));
      Statistics.label("modes").collect(new TreeSet<>(modes).size() == 1 ? "one" : "mixed");
      io.injectFaults(this::onWrite);
      io.injectReadFaults(this::onRead);
    }

    void execute(List<Cmd> schedule) {
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
    }

    private Writer writer(Cmd cmd) {
      return writers[cmd.writer % writers.length];
    }

    private void runTopLevel(Cmd cmd) {
      requests = 0;
      wrote = false;
      writes = 0;
      reads = 0;
      fired.clear();
      writePlan = faults ? cmd.writePlan() : List.of();
      failingRead = faults ? cmd.failingRead() : -1;
      readFault = cmd.readFaultKind();
      run(cmd, true);
    }

    // ---------------- storage hooks ----------------

    private Fault onWrite(String location, AtomicOutputFile.Strategy strategy) {
      if (!armed || Thread.currentThread() != owner) {
        return Fault.NONE;
      }
      if (!wrote) {
        preemptAt(requests++);
        wrote = true;
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

    private void preemptAt(int request) {
      if (pending != null && pending.preempt == request) {
        Cmd nested = pending;
        pending = null;
        armed = false;
        try {
          trace.append(String.format("  preempted at request #%d by%n", request));
          Statistics.label("preemption").collect("at request #" + request);
          run(nested, false);
        } finally {
          armed = true;
        }
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

    private DataFile newFile() {
      return DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("mem:///data/f" + nextFile++ + ".parquet")
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

    /** Concrete operands for {@code cmd}, or null if it does not apply. */
    private Op interpret(Cmd cmd, Writer w) {
      TableIdentifier id = pickTable(cmd.a);
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
                if (m.containsKey(created) || !actual.containsKey(created)
                    || !m.namespaces.containsKey(created.namespace())) {
                  throw new IllegalStateException("cannot create " + created);
                }
                m.put(created, new TableState(actual.get(created).uuid, version));
              });
        case DROP_TABLE:
          // Purging half the time: its deletes must spare every other table's files.
          boolean purge = cmd.b % 2 == 1;
          return new Op(
              "dropTable(" + id + (purge ? ", purge" : "") + ")",
              () -> {
                if (!w.catalog.dropTable(id, purge)) {
                  throw new NoSuchTableException("dropTable returned false: %s", id);
                }
              },
              (m, actual) -> {
                existing(m, id);
                m.remove(id);
              });
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
                if (m.containsKey(to)) {
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
                deletePath[0] = "mem:///data/" + (referenced == null ? "eq" : dv[0] ? "dv" : "pos")
                    + deleteFile + (dv[0] ? ".puffin" : ".parquet");
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
          return new Op(
              (tag ? "createTag(" : "createBranch(") + id + ", " + refName + ")",
              () -> {
                Table table = handle(w, id, cmd.fresh);
                if (table.currentSnapshot() == null) {
                  throw new ValidationException("%s has no snapshot", id);
                }
                long snapshotId = table.currentSnapshot().snapshotId();
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
          String key = KEYS.get(cmd.b % KEYS.size());
          String value = "v" + nextValue++;
          return new Op(
              "setProperty(" + id + ", " + key + "=" + value + ")",
              () -> handle(w, id, cmd.fresh).updateProperties().set(key, value).commit(),
              (m, actual) -> existing(m, id).props.put(key, value));
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
          // A replace commits the table it built at the start over any change since, keeping
          // that table's properties and format version: record them as the transaction saw them.
          TableState built = new TableState(null, 0);
          // The snapshot each of its refs other than main names, and the snapshots it committed:
          // those it built with, less any expired since it began.
          Map<String, Long> builtRefs = new HashMap<>();
          Set<Long> committed = new java.util.HashSet<>();
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
                // Its refs and snapshots, too: the commit puts back a ref removed since, unless
                // its snapshot was expired since, and drops one added since.
                replacement.refs().forEach((name, r) -> {
                  if (!name.equals(org.apache.iceberg.SnapshotRef.MAIN_BRANCH)) {
                    builtRefs.put(name, r.snapshotId());
                    if (r.isBranch()) {
                      built.branches.put(name, new TreeSet<>());
                    } else {
                      built.tagSnapshots.put(name, r.snapshotId());
                    }
                  }
                });
                for (Snapshot snapshot : replacement.snapshots()) {
                  String wap = snapshot.summary().get("wap.id");
                  if (wap != null) {
                    built.staged.put(wap, "");
                  }
                }
                // and the maximum ref ages it keeps, the table's and each ref's
                built.refsAge =
                    replacement.properties().containsKey(TableProperties.MAX_REF_AGE_MS);
                replacement.refs().forEach((name, r) -> {
                  if (r.maxRefAgeMs() != null) {
                    built.agingRefs.add(name);
                  }
                });
                replacement.properties().forEach((k, v) -> {
                  if (k.startsWith(KEY_PREFIX)) {
                    built.props.put(k, v);
                  }
                });
                try {
                  replace.commitTransaction();
                } finally {
                  // what it committed, or would have if it failed
                  ((org.apache.iceberg.BaseTransaction) replace).currentMetadata().snapshots()
                      .forEach(snapshot -> committed.add(snapshot.snapshotId()));
                }
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
                String uuid = before != null ? before.uuid : now.uuid;
                TableState replaced = new TableState(uuid, built.formatVersion);
                replaced.props.putAll(built.props);
                // An expiry may remove a ref it could before, or one the replacement ages out.
                replaced.refsAge = built.refsAge || before != null && before.refsAge;
                replaced.agingRefs.addAll(built.agingRefs);
                if (before != null) {
                  replaced.agingRefs.addAll(before.agingRefs);
                }
                // Branches as observed (the commit may have taken one back to where it was);
                // a tag the model had keeps the files it named, else as observed. A ref goes
                // with its snapshot.
                built.branches.keySet().forEach(n -> {
                  if (committed.contains(builtRefs.get(n))) {
                    replaced.branches.put(n, new TreeSet<>(now.branches.getOrDefault(n, Set.of())));
                  }
                });
                built.tagSnapshots.forEach((n, snapshotId) -> {
                  if (!committed.contains(snapshotId)) {
                    return;
                  }
                  boolean known = before != null && before.tags.containsKey(n)
                      && snapshotId.equals(before.tagSnapshots.get(n));
                  replaced.tags.put(n, new TreeSet<>(
                      known ? before.tags.get(n) : now.tags.getOrDefault(n, Set.of())));
                  replaced.tagSnapshots.put(n, snapshotId);
                });
                if (before != null) {
                  built.staged.keySet().forEach(wap -> {
                    if (before.staged.containsKey(wap)) {
                      replaced.staged.put(wap, before.staged.get(wap));
                    }
                  });
                }
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
          boolean cleanMetadata = cmd.b % 2 == 1;
          boolean keepFiles = cmd.b / 2 % 2 == 1;
          Expiry expiry = Expiry.values()[cmd.c % 4];
          int operand = cmd.c / 4;
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
          return new Op(
              "expireSnapshots(" + id + ", " + expired
                  + (cleanMetadata ? ", cleaning metadata" : "")
                  + (keepFiles ? ", keeping files" : "") + ")",
              () -> {
                Table table = handle(w, id, cmd.fresh);
                ExpireSnapshots expire = table.expireSnapshots()
                    .cleanExpiredFiles(!keepFiles)
                    .cleanExpiredMetadata(cleanMetadata);
                List<Snapshot> history = oldestFirst(table);
                Snapshot picked = history.isEmpty() ? null : history.get(operand % history.size());
                if (expiry == Expiry.OLDER_THAN_NOW) {
                  expire.expireOlderThan(System.currentTimeMillis() + 1).retainLast(operand + 1);
                } else if (expiry == Expiry.OLDER_THAN_A_SNAPSHOT && picked != null) {
                  expire.expireOlderThan(picked.timestampMillis());
                } else if (expiry == Expiry.A_SNAPSHOT && picked != null) {
                  expire.expireSnapshotId(picked.snapshotId());
                }
                Statistics.label("expiry").collect(expiry);
                try {
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
              });
        case REWRITE_MANIFESTS:
          return new Op(
              "rewriteManifests(" + id + ")",
              () -> handle(w, id, cmd.fresh).rewriteManifests().clusterBy(f -> "all").commit(),
              (m, actual) -> existing(m, id));
        case CREATE_NAMESPACE:
        case DROP_NAMESPACE:
        case SET_NAMESPACE_PROPERTY:
        case REMOVE_NAMESPACE_PROPERTY:
          return namespaceOp(cmd, w, NAMESPACES.get(cmd.b % NAMESPACES.size()));
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
        case TXN_READ:
          if (w.txn == null) {
            return null;
          }
          DataFile staged = cmd.kind == Kind.TXN_APPEND ? newFile() : null;
          return new Op(
              staged != null ? "txn.append(" + id + ", " + staged.location() + ")" : "txn.read(" + id + ")",
              () -> {
                try {
                  Catalog txc = w.txn.asCatalog();
                  Table table = txc.loadTable(id);
                  w.loaded.computeIfAbsent(
                      id, k -> new Loaded(((HasTableOperations) table).operations().current()));
                  if (staged != null) {
                    table.newFastAppend().appendFile(inSpec(table, staged)).commit();
                    w.loaded.get(id).appended = true;
                    w.staged.add(Map.entry(id, staged.location()));
                  }
                } catch (org.apache.iceberg.exceptions.NotFoundException e) {
                  // The transaction stages on the version it read; an expiry, or a drop with
                  // purge, since may have deleted that version's manifest list, which the append
                  // reads. A rejection, as for a stale handle (the transaction would fail at
                  // commit anyway: the table changed since it read it). A live snapshot without
                  // its manifest list is still caught by the check of every snapshot's list.
                  w.closeTxn();
                  throw new ValidationException(e, "the transaction's snapshot is gone");
                } catch (RuntimeException e) {
                  w.closeTxn(); // abandon the transaction
                  throw e;
                }
              },
              (m, actual) -> {});
        case TXN_COMMIT:
          if (w.txn == null) {
            return null;
          }
          CatalogTransaction txn = w.txn;
          Map<TableIdentifier, Loaded> loaded = new LinkedHashMap<>(w.loaded);
          List<Map.Entry<TableIdentifier, String>> appends = new ArrayList<>(w.staged);
          w.closeTxn();
          return new Op(
              "txn.commit(" + txn.isolationLevel() + ", appends " + appends + ")",
              txn::commitTransaction,
              (m, actual) -> {
                if (!appends.isEmpty()) {
                  checkIsolation(txn.isolationLevel(), loaded, m);
                }
                appends.forEach(e -> existing(m, e.getKey()).files.add(e.getValue()));
              });
        default:
          throw new IllegalStateException("Unknown kind " + cmd.kind);
      }
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
                if (m.keySet().stream().anyMatch(id -> id.namespace().equals(ns))) {
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

    /**
     * Throws if a catalog transaction that loaded {@code loaded} could not have committed on
     * {@code current}, the state its effects apply to.
     */
    private void checkIsolation(
        IsolationLevel level, Map<TableIdentifier, Loaded> loaded,
        Map<TableIdentifier, TableState> current) {
      loaded.forEach(
          (id, at) -> {
            if (level == IsolationLevel.SNAPSHOT && !at.appended) {
              return;
            }
            TableState now = current.get(id);
            if (now == null || !now.uuid.equals(at.uuid)) {
              throw new IsolationViolation(level + ": " + id + " was replaced or dropped");
            }
            // a table it wrote: any change since it first read it (errata D12); a table it only
            // read, serializable: its main branch or, without one, its metadata
            boolean unchanged =
                at.appended
                        || level == IsolationLevel.SERIALIZABLE
                            && (at.snapshotId == null || now.snapshotId == null)
                    ? Objects.equals(at.metadataLocation, now.metadataLocation)
                    : Objects.equals(at.snapshotId, now.snapshotId);
            if (!unchanged) {
              throw new IsolationViolation(
                  String.format(
                      "%s: %s changed after it was loaded (snapshot %s -> %s, metadata %s -> %s)",
                      level, id, at.snapshotId, now.snapshotId, at.metadataLocation,
                      now.metadataLocation));
            }
          });
    }

    private void run(Cmd cmd, boolean topLevel) {
      Writer w = writer(cmd);
      Op op = interpret(cmd, w);
      if (op == null) {
        return;
      }
      String indent = topLevel ? "" : "    ";
      trace.append(String.format("%sw%d %s%n", indent, w.id, op));
      Outcome outcome;
      String reason = "";
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
          | NoSuchNamespaceException | NamespaceNotEmptyException e) {
        outcome = Outcome.REJECTED;
        reason = e.getClass().getSimpleName() + ": " + firstLine(e);
      } catch (NoSuchTableException e) {
        outcome = Outcome.REJECTED;
        reason = e.getClass().getSimpleName() + ": " + firstLine(e);
        w.handles.clear(); // a handle to a dropped table is unusable; a caller reloads
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
      if (!faultsFired.isEmpty()) {
        Statistics.label("outcome after a fault").collect(outcome);
      }
      if (cmd.kind == Kind.TXN_BEGIN || cmd.kind == Kind.TXN_APPEND || cmd.kind == Kind.TXN_READ) {
        return; // no change to the catalog
      }
      resolve(op, outcome, reason);
    }

    /** Checks the catalog against the model, given what {@code op} reported. */
    private void resolve(Op op, Outcome outcome, String reason) {
      Model actual = actual();
      Model with = copy(model);
      String applies = null;
      try {
        op.effect.apply(with, actual);
      } catch (IsolationViolation e) {
        if (outcome == Outcome.ACKNOWLEDGED) {
          throw fail("acknowledged %s, but %s", op, e.getMessage());
        }
        applies = e.getMessage();
      } catch (IllegalStateException e) {
        applies = e.getMessage();
      }
      String withDiff = applies != null ? applies : diff(with, actual);
      String withoutDiff = diff(model, actual);
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
      // Observe what the model cannot predict.
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
          });
    }

    /** Checks each catalog file written since the last check against a checkpoint of it. */
    private void checkCheckpoints() {
      List<InMemoryAtomicFileIO.Write> history = io.history();
      for (; checkedWrites < history.size(); checkedWrites++) {
        InMemoryAtomicFileIO.Write write = history.get(checkedWrites);
        if (write.location().equals(CATALOG_LOCATION)) {
          String difference;
          try {
            difference = CheckpointRoundTrip.difference(write.after());
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
    }

    /**
     * The catalog's state, read without faults; fails if a table does not load whole, if a table
     * is stored both inline and in a metadata file, if a snapshot's manifest list or row ids
     * changed, if a v3 table's row ids are missing or overlap, or if a catalog file written since
     * the last check differs from a checkpoint of it.
     */
    private Model actual() {
      checkCheckpoints();
      Model tables = new Model();
      ProtoCatalogFile file =
          new ProtoCatalogFormat(PROPERTIES).read(io, io.newInputFile(CATALOG_LOCATION));
      List<TableIdentifier> ids = new ArrayList<>();
      for (Namespace ns : checker.listNamespaces(Namespace.empty())) {
        tables.namespaces.put(ns, new TreeMap<>(checker.loadNamespaceMetadata(ns)));
        ids.addAll(checker.listTables(ns));
      }
      check(file.tables().size() == ids.size(),
          "the catalog has tables %s, but its namespaces list %s", file.tables(), ids);
      for (TableIdentifier id : ids) {
        TableState state;
        try {
          Table table = checker.loadTable(id);
          TableMetadata metadata = ((HasTableOperations) table).operations().current();
          state = new TableState(metadata.uuid(), metadata.formatVersion());
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
            List<String> manifests = new ArrayList<>();
            manifests.add(String.format(
                "seq=%d parent=%s ts=%d op=%s schema=%s first-row-id=%s added-rows=%s summary=%s",
                snapshot.sequenceNumber(), snapshot.parentId(), snapshot.timestampMillis(),
                snapshot.operation(), snapshot.schemaId(), snapshot.firstRowId(),
                snapshot.addedRows(), new TreeMap<>(snapshot.summary())));
            for (ManifestFile manifest : snapshot.allManifests(io)) {
              check(io.newInputFile(manifest.path()).exists(),
                  "%s: snapshot %d references missing manifest %s",
                  id, snapshot.snapshotId(), manifest.path());
              check(metadata.specsById().containsKey(manifest.partitionSpecId()),
                  "%s: snapshot %d has manifest %s of partition spec %d, which the table does not "
                      + "have", id, snapshot.snapshotId(), manifest.path(),
                  manifest.partitionSpecId());
              manifests.add(describe(manifest));
            }
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

  private static String firstLine(Throwable e) {
    String message = String.valueOf(e.getMessage());
    int newline = message.indexOf('\n');
    return newline < 0 ? message : message.substring(0, newline);
  }
}
