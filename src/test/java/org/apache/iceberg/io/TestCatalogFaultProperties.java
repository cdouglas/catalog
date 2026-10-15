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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
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
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
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
 * merge appends, deletes, property and schema changes, table transactions, snapshot expiry,
 * manifest rewrites, table create / drop / rename, and multi-table catalog transactions whose
 * loads, appends and commit are separate steps, so other writers' operations run in between. A
 * command may also preempt the one before it: it runs to completion just before one of that
 * operation's reads of the catalog or its first conditional write, so the two race. Every catalog
 * mode runs (append / CAS / compaction, pointer / inline TM / inline ML).
 *
 * <p>In {@link #faultyStorageNeverMisreportsACommit}, each operation's storage also fails as its
 * command says: its first two conditional writes (see {@link InMemoryAtomicFileIO.Fault}) and one
 * of its reads (see {@link InMemoryAtomicFileIO.ReadFault}). Reads of {@code metadata.json} do not
 * fail, as Iceberg retries them after 100 ms; nor do reads on Iceberg's worker threads, which
 * would make the schedule nondeterministic. A preempting operation runs without faults.
 *
 * <p>A model holds each table's UUID, live data files, properties and added columns. After every
 * operation the catalog, read back without faults, must match it:
 *
 * <ul>
 *   <li>An acknowledged operation took effect once, and nothing else changed.
 *   <li>An operation that failed any way but {@link CommitStateUnknownException} did not take
 *       effect. Iceberg deletes a commit's new files on a {@code CleanableFailure} such as {@link
 *       CommitFailedException}, so a commit reported that way that took effect loses them.
 *   <li>An operation whose outcome is unknown took effect entirely or not at all.
 *   <li>Every table loads, and every manifest of every snapshot exists.
 *   <li>An acknowledged catalog transaction saw no concurrent change it must not miss: under
 *       serializable isolation, to the main branch (or, without one, the metadata) of any table it
 *       loaded; under snapshot isolation, to the main branch of any table it appended to.
 * </ul>
 */
class TestCatalogFaultProperties {

  private static final Namespace NS = Namespace.of("ns");
  // A small universe, so writers collide often.
  private static final List<TableIdentifier> TABLES =
      List.of(TableIdentifier.of(NS, "t0"), TableIdentifier.of(NS, "t1"), TableIdentifier.of(NS, "t2"));
  private static final Schema SCHEMA = new Schema(required(1, "id", Types.IntegerType.get()));
  private static final String KEY_PREFIX = "fz.";
  private static final List<String> KEYS = List.of("fz.k1", "fz.k2");
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
      @ForAll LocalCatalogMode mode,
      @ForAll @IntRange(min = 2, max = 3) int writers,
      @ForAll("schedules") List<Cmd> schedule) {
    new Run(mode, writers, false).execute(schedule);
  }

  @Property(tries = 100)
  void faultyStorageNeverMisreportsACommit(
      @ForAll LocalCatalogMode mode,
      @ForAll @IntRange(min = 2, max = 3) int writers,
      @ForAll("schedules") List<Cmd> schedule) {
    new Run(mode, writers, true).execute(schedule);
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
            Tuple.of(2, Kind.ADD_COLUMN),
            Tuple.of(2, Kind.TABLE_TXN),
            Tuple.of(1, Kind.EXPIRE_SNAPSHOTS),
            Tuple.of(1, Kind.REWRITE_MANIFESTS),
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
    return Combinators.combine(
            kinds,
            Arbitraries.integers().between(0, 2),
            small,
            small,
            Arbitraries.of(true, false),
            preempt,
            Arbitraries.integers().between(0, 143),
            small)
        .as(Cmd::new)
        .list()
        .ofMinSize(1)
        .ofMaxSize(40)
        .withSizeDistribution(RandomDistribution.uniform());
  }

  // ============================================================
  // Schedule
  // ============================================================

  enum Kind {
    CREATE_TABLE,
    DROP_TABLE,
    RENAME_TABLE,
    FAST_APPEND,
    MERGE_APPEND,
    DELETE_FILE,
    SET_PROPERTY,
    ADD_COLUMN,
    TABLE_TXN,
    EXPIRE_SNAPSHOTS,
    REWRITE_MANIFESTS,
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
    final boolean fresh;
    final int preempt;
    final int writeFaults;
    final int readFault;

    Cmd(Kind kind, int writer, int a, int b, boolean fresh, int preempt, int writeFaults,
        int readFault) {
      this.kind = kind;
      this.writer = writer;
      this.a = a;
      this.b = b;
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
      return String.format(
          "w%d:%s(%d,%d%s)%s",
          writer, kind, a, b, fresh ? ",fresh" : "", preempt >= 0 ? "@" + preempt : "");
    }
  }

  // ============================================================
  // Model
  // ============================================================

  private static final Comparator<TableIdentifier> BY_NAME =
      Comparator.comparing(TableIdentifier::toString);

  /** What the model tracks of a table, and what it observed of it at the last check. */
  static final class TableState {
    final String uuid;
    final List<String> files = new ArrayList<>();
    final Map<String, String> props = new TreeMap<>();
    final Set<String> columns = new TreeSet<>();
    // Observed, not modeled: Iceberg chooses snapshot ids and metadata locations.
    Long snapshotId = null;
    String metadataLocation = null;

    TableState(String uuid) {
      this.uuid = uuid;
    }

    TableState copy() {
      TableState copy = new TableState(uuid);
      copy.files.addAll(files);
      copy.props.putAll(props);
      copy.columns.addAll(columns);
      copy.snapshotId = snapshotId;
      copy.metadataLocation = metadataLocation;
      return copy;
    }

    List<String> sortedFiles() {
      List<String> sorted = new ArrayList<>(files);
      sorted.sort(Comparator.naturalOrder());
      return sorted;
    }

    @Override
    public String toString() {
      return String.format(
          "{uuid=%s files=%s props=%s columns=%s}",
          uuid.substring(0, 8), sortedFiles(), props, columns);
    }
  }

  private static Map<TableIdentifier, TableState> copy(Map<TableIdentifier, TableState> model) {
    Map<TableIdentifier, TableState> copy = new TreeMap<>(BY_NAME);
    model.forEach((id, state) -> copy.put(id, state.copy()));
    return copy;
  }

  /** How {@code actual} differs from {@code expected}, or null if it matches. */
  private static String diff(
      Map<TableIdentifier, TableState> expected, Map<TableIdentifier, TableState> actual) {
    if (!expected.keySet().equals(actual.keySet())) {
      return "tables " + expected.keySet() + " expected, found " + actual.keySet();
    }
    for (TableIdentifier id : expected.keySet()) {
      TableState e = expected.get(id);
      TableState a = actual.get(id);
      if (!e.uuid.equals(a.uuid)
          || !e.sortedFiles().equals(a.sortedFiles())
          || !e.props.equals(a.props)
          || !e.columns.equals(a.columns)) {
        return id + ": expected " + e + ", found " + a;
      }
    }
    return null;
  }

  /** An operation's intended change to the model; throws if it cannot apply. */
  @FunctionalInterface
  interface Effect {
    void apply(Map<TableIdentifier, TableState> model, Map<TableIdentifier, TableState> actual);
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
    private final LocalCatalogMode mode;
    private final boolean faults;
    private final InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    private final Writer[] writers;
    private final FileIOCatalog checker;
    private Map<TableIdentifier, TableState> model;
    private final StringBuilder trace = new StringBuilder();
    private int nextFile = 0;
    private int nextValue = 0;

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

    Run(LocalCatalogMode mode, int writers, boolean faults) {
      this.mode = mode;
      this.faults = faults;
      this.writers = new Writer[writers];
      for (int i = 0; i < writers; i++) {
        this.writers[i] =
            new Writer(i, mode.catalog(io, WAREHOUSE, CATALOG_NAME, PROPERTIES));
      }
      this.checker = mode.catalog(io, WAREHOUSE, CATALOG_NAME, PROPERTIES);
      FileIOCatalog seed = this.writers[0].catalog;
      seed.createNamespace(NS);
      seed.createTable(TABLES.get(0), SCHEMA);
      seed.createTable(TABLES.get(1), SCHEMA).newFastAppend().appendFile(newFile()).commit();
      this.model = actual();
      trace.append(String.format("mode %s, %d writers%nseed: %s%n", mode, writers, model));
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

    private DataFile newFile() {
      return DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("mem:///data/f" + nextFile++ + ".parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .build();
    }

    private TableIdentifier pickTable(int index) {
      List<TableIdentifier> tables = new ArrayList<>(model.keySet());
      return index >= 8 || tables.isEmpty()
          ? TABLES.get(index % TABLES.size())
          : tables.get(index % tables.size());
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
          return new Op(
              "createTable(" + created + ")",
              () -> w.catalog.createTable(created, SCHEMA),
              (m, actual) -> {
                if (m.containsKey(created) || !actual.containsKey(created)) {
                  throw new IllegalStateException("cannot create " + created);
                }
                m.put(created, new TableState(actual.get(created).uuid));
              });
        case DROP_TABLE:
          return new Op(
              "dropTable(" + id + ")",
              () -> {
                if (!w.catalog.dropTable(id, false)) {
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
                (fast ? table.newFastAppend() : table.newAppend()).appendFile(appended).commit();
              },
              (m, actual) -> existing(m, id).files.add(appended.location()));
        case DELETE_FILE:
          TableState target = model.get(id);
          String path =
              target == null || target.files.isEmpty()
                  ? "mem:///data/missing.parquet"
                  : target.sortedFiles().get(cmd.b % target.files.size());
          return new Op(
              "deleteFile(" + id + ", " + path + ")",
              () -> handle(w, id, cmd.fresh).newDelete().deleteFile(path).commit(),
              (m, actual) -> existing(m, id).files.removeIf(path::equals));
        case SET_PROPERTY:
          String key = KEYS.get(cmd.b % KEYS.size());
          String value = "v" + nextValue++;
          return new Op(
              "setProperty(" + id + ", " + key + "=" + value + ")",
              () -> handle(w, id, cmd.fresh).updateProperties().set(key, value).commit(),
              (m, actual) -> existing(m, id).props.put(key, value));
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
        case TABLE_TXN:
          DataFile txnFile = newFile();
          String txnKey = KEYS.get(cmd.b % KEYS.size());
          String txnValue = "v" + nextValue++;
          return new Op(
              String.format(
                  "tableTxn(%s, %s, %s=%s)", id, txnFile.location(), txnKey, txnValue),
              () -> {
                Transaction txn = handle(w, id, cmd.fresh).newTransaction();
                txn.newFastAppend().appendFile(txnFile).commit();
                txn.updateProperties().set(txnKey, txnValue).commit();
                txn.commitTransaction();
              },
              (m, actual) -> {
                TableState state = existing(m, id);
                state.files.add(txnFile.location());
                state.props.put(txnKey, txnValue);
              });
        case EXPIRE_SNAPSHOTS:
          return new Op(
              "expireSnapshots(" + id + ")",
              () ->
                  handle(w, id, cmd.fresh)
                      .expireSnapshots()
                      .expireOlderThan(System.currentTimeMillis() + 1)
                      .retainLast(1)
                      .commit(),
              (m, actual) -> existing(m, id));
        case REWRITE_MANIFESTS:
          if (model.containsKey(id) && model.get(id).snapshotId == null) {
            return null; // Iceberg's rewrite needs a current snapshot
          }
          return new Op(
              "rewriteManifests(" + id + ")",
              () -> handle(w, id, cmd.fresh).rewriteManifests().clusterBy(f -> "all").commit(),
              (m, actual) -> existing(m, id));
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
                    table.newFastAppend().appendFile(staged).commit();
                    w.loaded.get(id).appended = true;
                    w.staged.add(Map.entry(id, staged.location()));
                  }
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
            boolean unchanged =
                level == IsolationLevel.SERIALIZABLE
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
      } catch (CommitFailedException | ValidationException | AlreadyExistsException e) {
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
      Map<TableIdentifier, TableState> actual = actual();
      Map<TableIdentifier, TableState> with = copy(model);
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
            state.snapshotId = actual.get(id).snapshotId;
            state.metadataLocation = actual.get(id).metadataLocation;
          });
    }

    /** The catalog's state, read without faults; fails if a table does not load whole. */
    private Map<TableIdentifier, TableState> actual() {
      Map<TableIdentifier, TableState> tables = new TreeMap<>(BY_NAME);
      for (TableIdentifier id : checker.listTables(NS)) {
        TableState state;
        try {
          Table table = checker.loadTable(id);
          TableMetadata metadata = ((HasTableOperations) table).operations().current();
          state = new TableState(metadata.uuid());
          for (Snapshot snapshot : table.snapshots()) {
            for (ManifestFile manifest : snapshot.allManifests(io)) {
              check(io.newInputFile(manifest.path()).exists(),
                  "%s: snapshot %d references missing manifest %s",
                  id, snapshot.snapshotId(), manifest.path());
            }
          }
          try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
              state.files.add(task.file().location());
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
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        } catch (RuntimeException e) {
          throw fail("%s does not load: %s", id, e);
        }
        tables.put(id, state);
      }
      return tables;
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
