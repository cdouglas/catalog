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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.statistics.Statistics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.InMemoryAtomicFileIO.Fault;
import org.apache.iceberg.types.Types;

/**
 * Catalog operations race, and the outcome is a serial one. Writer A runs an operation, and
 * writer B's runs to completion just before A's first write to the catalog, after A has read it;
 * a third writer's, C's, if there is one, runs just before A's second write, which A makes only
 * if its first conflicted. A catalog operation depends on what it read only through its
 * preconditions, which it checks again if its write conflicts, so A must report what it would
 * have, and leave the catalog as it would, had it run after the operations that ran before its
 * writes: the same as a serial run of those, then A, then the rest, on another copy of the same
 * catalog. (If A does not write -- it fails on what it read, or has nothing to change -- B and C
 * run after it.) One exception: an operation through a table handle (a property change, an
 * append, a delete) that Iceberg refuses because the table it loaded was dropped and another
 * created under its name since (its UUID changed) must leave the catalog as the others alone do.
 * A catalog transaction (it loads one table or two and appends to the first) is not serial that
 * way either: it must fail, and leave the catalog as the others alone do, if an operation that
 * ran before its writes changed what it read -- under serializable isolation the main branch of
 * each table it loaded (or the table, if main had no snapshot), under snapshot isolation that of
 * the table it appended to. It must also fail when another commit changed the table it writes in
 * any other way, a property change included: a catalog transaction conflicts on the whole version
 * of each table it writes, from when it first read the table (errata D12).
 *
 * <p>The operations are namespace creates (of nested namespaces too, which create their parents),
 * drops and property changes; table creates, drops, renames and property changes; and appends and
 * deletes of every row, which Iceberg retries on a conflict as it does a property change (unlike
 * a schema change). They run on a small universe of names, from a generated catalog state; each
 * writer has its own catalog mode. A result is a return value or an exception's class; the
 * catalog's state is its namespaces with their properties and its tables with theirs (a table
 * created carries its creator's name) and their live data files.
 */
class TestCatalogRaceProperties {

  private static final String WAREHOUSE = "mem:///warehouse";
  private static final String CATALOG = WAREHOUSE + "/catalog-c";
  private static final Schema SCHEMA = new Schema(required(1, "id", Types.IntegerType.get()));
  private static final List<Namespace> NAMESPACES =
      List.of(Namespace.of("a"), Namespace.of("b"), Namespace.of("a", "c"));
  private static final List<TableIdentifier> TABLES =
      List.of(
          TableIdentifier.of(Namespace.of("a"), "t"),
          TableIdentifier.of(Namespace.of("a"), "u"),
          TableIdentifier.of(Namespace.of("b"), "t"),
          TableIdentifier.of(Namespace.of("a", "c"), "t"));
  private static final List<String> KEYS = List.of("k1", "k2");

  @Property(tries = 1000)
  void aRaceEndsAsASerialRun(
      @ForAll("modes") LocalCatalogMode modeA,
      @ForAll("modes") LocalCatalogMode modeB,
      @ForAll("modes") LocalCatalogMode modeC,
      @ForAll("states") Setup setup,
      @ForAll("ops") Op a,
      @ForAll("ops") Op b,
      @ForAll("maybeOps") Op c,
      @ForAll boolean gc) {
    List<Op> racers = c.kind == null ? List.of(b) : List.of(b, c);
    List<LocalCatalogMode> racerModes = List.of(modeB, modeC);
    // With gc, each CAS drops every committed transaction id older than its own (Bug 64).
    Map<String, String> props =
        gc ? Map.of(LogCatalogProperties.COMMITTED_RETENTION_MS, "0") : Map.of();
    Statistics.label("gc").collect(gc);

    // The race: the k-th racer runs just before A's k-th write to the catalog.
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    setup.apply(LocalCatalogMode.APPEND.catalog(io, WAREHOUSE, "c", Map.of()));
    FileIOCatalog writerA = modeA.catalog(io, WAREHOUSE, "c", props);
    List<FileIOCatalog> writers = List.of(
        modeB.catalog(io, WAREHOUSE, "c", props), modeC.catalog(io, WAREHOUSE, "c", props));
    String[] results = new String[racers.size()];
    int[] writes = {0};
    boolean[] nested = {false};
    boolean[] changedItsReads = {false}; // whether a racer changed what A, a transaction, read
    boolean[] changedItsTable = {false}; // whether a racer changed the table A appends to at all
    List<Map<TableIdentifier, String>> loaded = new java.util.ArrayList<>(); // as A loaded them
    io.injectFaults((location, strategy) -> {
      if (!nested[0] && location.equals(CATALOG)) {
        int k = writes[0]++;
        if (k < racers.size()) {
          nested[0] = true;
          try {
            if (a.kind == Kind.CATALOG_TXN && loaded.isEmpty()) {
              loaded.add(a.reads(io)); // nothing has written since A loaded its tables
            }
            results[k] = racers.get(k).run(writers.get(k), name(k));
            if (!loaded.isEmpty() && !a.unchanged(loaded.get(0), a.reads(io))) {
              changedItsReads[0] = true;
            }
            if (!loaded.isEmpty() && !a.sameVersion(loaded.get(0), a.reads(io))) {
              changedItsTable[0] = true;
            }
          } finally {
            nested[0] = false;
          }
        }
      }
      return Fault.NONE;
    });
    String raceA = a.run(writerA, "A");
    io.injectFaults((location, strategy) -> Fault.NONE);
    int before = Math.min(writes[0], racers.size()); // racers that ran before one of A's writes
    for (int k = before; k < racers.size(); k++) {
      results[k] = racers.get(k).run(writers.get(k), name(k));
    }
    String raceState = state(io);

    // A transaction whose reads a racer changed must fail, and so must one whose table a racer
    // changed in any way, a property change included: a transaction conflicts on the whole
    // version of each table it writes (errata D12).
    boolean txnFailed = a.kind == Kind.CATALOG_TXN && before > 0 && !raceA.startsWith("ok");
    if (a.kind == Kind.CATALOG_TXN && before > 0) {
      Statistics.label("transaction racing")
          .collect((changedItsReads[0] ? "reads changed, " : "reads held, ")
              + (changedItsTable[0] ? "table changed, " : "table held, ")
              + (txnFailed ? "failed" : "committed"));
      if (changedItsTable[0] && !txnFailed) {
        throw new AssertionError(String.format(
            "the transaction %s committed, but the racers before its writes changed the table it "
                + "appends to: %s, %s%nsetup %s",
            a, racers.get(0), before > 1 ? racers.get(1) : "", setup));
      }
      if (changedItsReads[0] && !txnFailed) {
        throw new AssertionError(String.format(
            "the transaction %s committed, but the racers before its writes changed what it "
                + "read: %s, %s%nsetup %s",
            a, racers.get(0), before > 1 ? racers.get(1) : "", setup));
      }
    }
    // The serial run on another copy: the racers that ran before A's writes, A, the rest; without
    // A if it failed as it may: a transaction, or a handle whose table was replaced since it was
    // loaded.
    boolean replaced = (a.kind.throughAHandle() && raceA.equals(REPLACED)) || txnFailed;
    // With gc, a racer's CAS may drop committed ids newer than A's transaction id, putting it at
    // or below the committed set's horizon: replay then rejects A's record, and A cannot learn
    // whether an earlier write of it landed (Bug 64). A fails or is unknown, and the race must
    // end as a serial run with A or without it. Only a racer that ran before one of A's writes
    // can do that: A mints its id above the horizon it read, and its own compaction keeps the
    // horizon below its record.
    boolean lost = gc && before > 0 && (raceA.equals("CommitFailedException")
        || raceA.equals("CommitStateUnknownException"));
    String[] serial = new String[racers.size()];
    String[] first = serialRun(setup, replaced || lost ? null : a, modeA, racers, racerModes,
        before, props, serial);
    String serialA = replaced || lost ? raceA : first[0];
    String serialState = first[1];
    boolean tookEffect = false; // A lost its outcome, but the race ends as the serial run with A
    if (lost && !raceState.equals(serialState)) {
      String[] withA = new String[racers.size()];
      String[] landed = serialRun(setup, a, modeA, racers, racerModes, before, props, withA);
      if (raceState.equals(landed[1])) {
        serial = withA;
        serialState = landed[1];
        tookEffect = true;
      }
    }
    Statistics.label("A failed as it may").collect(replaced);
    Statistics.label("A lost its outcome to gc").collect(lost);

    Statistics.label("racers before A's writes").collect(before + " of " + racers.size());
    Statistics.label("A").collect(a.kind + " " + (raceA.startsWith("ok") ? "ok" : "fails"));
    StringBuilder context = new StringBuilder(
        String.format("%nsetup %s%s%nA (%s): %s", setup, gc ? ", gc" : "", modeA, a));
    for (int k = 0; k < racers.size(); k++) {
      context.append(String.format("%n%s (%s): %s, %s", name(k), racerModes.get(k),
          racers.get(k), k < before ? "just before A's write #" + k : "after A"));
    }
    if (tookEffect && !raceA.equals("CommitStateUnknownException")) {
      throw new AssertionError(String.format("A reported %s, but took effect%s", raceA, context));
    }
    String race = "A=" + raceA + " " + java.util.Arrays.toString(results);
    String serialRun = "A=" + serialA + " " + java.util.Arrays.toString(serial);
    if (!race.equals(serialRun)) {
      throw new AssertionError(String.format(
          "the race reported %s; the serial run %s%s", race, serialRun, context));
    }
    if (!raceState.equals(serialState)) {
      throw new AssertionError(String.format(
          "the race left%n  %s%nthe serial run%n  %s%s", raceState, serialState, context));
    }
    CheckpointRoundTrip.assertEveryWrite(io);
  }

  /**
   * A serial run on a copy of {@code setup}: the racers that ran before A's writes, then A unless
   * it is null, then the rest. Fills {@code results} with the racers' results; returns A's result
   * and the catalog's state.
   */
  private static String[] serialRun(Setup setup, Op a, LocalCatalogMode modeA, List<Op> racers,
      List<LocalCatalogMode> racerModes, int before, Map<String, String> props,
      String[] results) {
    InMemoryAtomicFileIO serialIo = new InMemoryAtomicFileIO();
    setup.apply(LocalCatalogMode.APPEND.catalog(serialIo, WAREHOUSE, "c", Map.of()));
    for (int k = 0; k < before; k++) {
      results[k] = racers.get(k).run(
          racerModes.get(k).catalog(serialIo, WAREHOUSE, "c", props), name(k));
    }
    String resultA = a == null ? null : a.run(modeA.catalog(serialIo, WAREHOUSE, "c", props), "A");
    for (int k = before; k < racers.size(); k++) {
      results[k] = racers.get(k).run(
          racerModes.get(k).catalog(serialIo, WAREHOUSE, "c", props), name(k));
    }
    return new String[] {resultA, state(serialIo)};
  }

  private static String name(int racer) {
    return racer == 0 ? "B" : "C";
  }

  /** What Iceberg reports for a handle to a table that was dropped and created again. */
  private static final String REPLACED = "IllegalStateException: Table UUID does not match";

  @Provide
  Arbitrary<LocalCatalogMode> modes() {
    return Arbitraries.of(LocalCatalogMode.class);
  }

  /** A catalog state: some namespaces, with properties, and some tables in them. */
  @Provide
  Arbitrary<Setup> states() {
    return Combinators.combine(
            Arbitraries.integers().between(0, 7),
            Arbitraries.integers().between(0, 7),
            Arbitraries.integers().between(0, 15))
        .as(Setup::new);
  }

  @Provide
  Arbitrary<Op> ops() {
    return Combinators.combine(
            Arbitraries.of(Kind.class),
            Arbitraries.integers().between(0, 11),
            Arbitraries.integers().between(0, 11))
        .as(Op::new);
  }

  /** An operation half the time, else none (an Op of no kind). */
  @Provide
  Arbitrary<Op> maybeOps() {
    return Arbitraries.oneOf(ops(), Arbitraries.just(new Op(null, 0, 0)));
  }

  /** The namespaces in bitmask {@code namespaces}, those in {@code props} with k1, and tables. */
  static final class Setup {
    final int namespaces;
    final int props;
    final int tables;

    Setup(int namespaces, int props, int tables) {
      this.namespaces = namespaces;
      this.props = props;
      this.tables = tables;
    }

    void apply(FileIOCatalog catalog) {
      for (int i = 0; i < NAMESPACES.size(); i++) {
        Namespace ns = NAMESPACES.get(i);
        if ((namespaces >> i & 1) == 1 && !catalog.namespaceExists(ns)) {
          catalog.createNamespace(ns, (props >> i & 1) == 1 ? Map.of("k1", "v0") : Map.of());
        }
      }
      for (int i = 0; i < TABLES.size(); i++) {
        TableIdentifier id = TABLES.get(i);
        if ((tables >> i & 1) == 1 && catalog.namespaceExists(id.namespace())) {
          catalog.buildTable(id, SCHEMA).withProperty("origin", "setup").create();
        }
      }
    }

    @Override
    public String toString() {
      return state(this);
    }
  }

  enum Kind {
    CREATE_NAMESPACE,
    DROP_NAMESPACE,
    SET_NAMESPACE_PROPERTY,
    REMOVE_NAMESPACE_PROPERTY,
    CREATE_TABLE,
    /** Creates a table in a create transaction. */
    CREATE_TABLE_TXN,
    DROP_TABLE,
    RENAME_TABLE,
    SET_TABLE_PROPERTY,
    /** Appends a file named for the writer. */
    FAST_APPEND,
    /** Deletes every row: the files live when the delete applies, after any retry. */
    DELETE_ALL,
    /**
     * Loads one table, or two (the second if it exists), in a catalog transaction, appends to the
     * first and commits; serializable when {@code x / 4} is even, else snapshot isolation.
     */
    CATALOG_TXN;

    /** Whether the operation loads a table and commits through its handle. */
    boolean throughAHandle() {
      return this == SET_TABLE_PROPERTY || this == FAST_APPEND || this == DELETE_ALL;
    }
  }

  /** An operation; {@code x} and {@code y} pick its operands. */
  static final class Op {
    final Kind kind;
    final int x;
    final int y;

    Op(Kind kind, int x, int y) {
      this.kind = kind;
      this.x = x;
      this.y = y;
    }

    private Namespace ns() {
      return NAMESPACES.get(x % NAMESPACES.size());
    }

    private TableIdentifier table() {
      return TABLES.get(x % TABLES.size());
    }

    private TableIdentifier other() {
      return TABLES.get(y % TABLES.size());
    }

    private String key() {
      return KEYS.get(y % KEYS.size());
    }

    private boolean serializable() {
      return x / 4 % 2 == 0;
    }

    /** The tables a transaction reads: both it loads if serializable, else the appended one. */
    private List<TableIdentifier> readTables() {
      return serializable() && !other().equals(table()) ? List.of(table(), other())
          : List.of(table());
    }

    /** What the transaction's isolation checks of each table it reads, as stored now. */
    Map<TableIdentifier, String> reads(InMemoryAtomicFileIO io) {
      FileIOCatalog reader = LocalCatalogMode.APPEND.catalog(io, WAREHOUSE, "c", Map.of());
      Map<TableIdentifier, String> reads = new TreeMap<>(
          java.util.Comparator.comparing(TableIdentifier::toString));
      for (TableIdentifier id : readTables()) {
        if (!reader.tableExists(id)) {
          reads.put(id, "missing");
          continue;
        }
        org.apache.iceberg.TableMetadata metadata =
            ((org.apache.iceberg.HasTableOperations) reader.loadTable(id)).operations().current();
        org.apache.iceberg.SnapshotRef main = metadata.ref(org.apache.iceberg.SnapshotRef.MAIN_BRANCH);
        reads.put(id, metadata.uuid() + " " + (main == null ? "-" : main.snapshotId()) + " "
            + metadata.metadataFileLocation());
      }
      return reads;
    }

    /** Whether the table the transaction appends to is the version it read: the same metadata. */
    boolean sameVersion(Map<TableIdentifier, String> then, Map<TableIdentifier, String> now) {
      return then.get(table()).equals(now.get(table()));
    }

    /**
     * Whether the tables are as the transaction read them: the same table, on the same main
     * snapshot; or, under serializable isolation where main had or has no snapshot, the same
     * metadata.
     */
    boolean unchanged(Map<TableIdentifier, String> then, Map<TableIdentifier, String> now) {
      for (TableIdentifier id : then.keySet()) {
        String[] t = then.get(id).split(" ");
        String[] n = now.get(id).split(" ");
        if (t.length == 1) {
          continue; // missing then: the transaction did not load it
        }
        if (n.length == 1 || !t[0].equals(n[0])) {
          return false; // missing now, or another table
        }
        boolean snapshots = !t[1].equals("-") && !n[1].equals("-");
        if (snapshots ? !t[1].equals(n[1])
            : serializable() ? !t[2].equals(n[2]) : !t[1].equals(n[1])) {
          return false;
        }
      }
      return true;
    }

    /** Runs the operation as {@code who}; returns "ok" and its result, or the exception's class. */
    String run(FileIOCatalog catalog, String who) {
      try {
        switch (kind) {
          case CREATE_NAMESPACE:
            catalog.createNamespace(ns(), y % 2 == 1 ? Map.of(key(), who) : Map.of());
            return "ok";
          case DROP_NAMESPACE:
            return "ok " + catalog.dropNamespace(ns());
          case SET_NAMESPACE_PROPERTY:
            return "ok " + catalog.setProperties(ns(), Map.of(key(), who));
          case REMOVE_NAMESPACE_PROPERTY:
            return "ok " + catalog.removeProperties(ns(), Set.of(key()));
          case CREATE_TABLE:
            catalog.buildTable(table(), SCHEMA).withProperty("origin", who).create();
            return "ok";
          case CREATE_TABLE_TXN:
            catalog.buildTable(table(), SCHEMA).withProperty("origin", who)
                .createTransaction().commitTransaction();
            return "ok";
          case DROP_TABLE:
            return "ok " + catalog.dropTable(table(), false);
          case RENAME_TABLE:
            catalog.renameTable(table(), other());
            return "ok";
          case SET_TABLE_PROPERTY:
            catalog.loadTable(table()).updateProperties().set(key(), who).commit();
            return "ok";
          case FAST_APPEND:
            Table table = catalog.loadTable(table());
            table.newFastAppend()
                .appendFile(org.apache.iceberg.DataFiles.builder(table.spec())
                    .withPath("mem:///data/" + who + ".parquet")
                    .withFileSizeInBytes(10)
                    .withRecordCount(1)
                    .build())
                .commit();
            return "ok";
          case CATALOG_TXN:
            org.apache.iceberg.catalog.CatalogTransaction txn = catalog.createTransaction(
                serializable()
                    ? org.apache.iceberg.catalog.CatalogTransaction.IsolationLevel.SERIALIZABLE
                    : org.apache.iceberg.catalog.CatalogTransaction.IsolationLevel.SNAPSHOT);
            org.apache.iceberg.catalog.Catalog txCatalog = txn.asCatalog();
            Table appended = txCatalog.loadTable(table());
            if (!other().equals(table()) && catalog.tableExists(other())) {
              txCatalog.loadTable(other());
            }
            appended.newFastAppend()
                .appendFile(org.apache.iceberg.DataFiles.builder(appended.spec())
                    .withPath("mem:///data/" + who + "-txn.parquet")
                    .withFileSizeInBytes(10)
                    .withRecordCount(1)
                    .build())
                .commit();
            txn.commitTransaction();
            return "ok";
          default:
            catalog.loadTable(table()).newDelete()
                .deleteFromRowFilter(org.apache.iceberg.expressions.Expressions.alwaysTrue())
                .commit();
            return "ok";
        }
      } catch (RuntimeException e) {
        if (e instanceof IllegalStateException && e.getMessage() != null
            && e.getMessage().startsWith("Table UUID does not match")) {
          return REPLACED;
        }
        return e.getClass().getSimpleName();
      }
    }

    @Override
    public String toString() {
      if (kind == null) {
        return "none";
      }
      switch (kind) {
        case CREATE_NAMESPACE:
          return "createNamespace(" + ns() + (y % 2 == 1 ? ", " + key() : "") + ")";
        case DROP_NAMESPACE:
        case SET_NAMESPACE_PROPERTY:
        case REMOVE_NAMESPACE_PROPERTY:
          return kind + "(" + ns() + (kind == Kind.DROP_NAMESPACE ? "" : ", " + key()) + ")";
        case RENAME_TABLE:
          return "renameTable(" + table() + ", " + other() + ")";
        case SET_TABLE_PROPERTY:
          return "setTableProperty(" + table() + ", " + key() + ")";
        case FAST_APPEND:
          return "fastAppend(" + table() + ")";
        case DELETE_ALL:
          return "deleteAll(" + table() + ")";
        case CATALOG_TXN:
          return String.format("catalogTxn(%s, append to %s%s)",
              serializable() ? "serializable" : "snapshot", table(),
              other().equals(table()) ? "" : ", read " + other());
        default:
          return kind + "(" + table() + ")";
      }
    }
  }

  /** The catalog's namespaces with their properties, and its tables with theirs. */
  private static String state(InMemoryAtomicFileIO io) {
    FileIOCatalog reader = LocalCatalogMode.APPEND.catalog(io, WAREHOUSE, "c", Map.of());
    Map<String, String> state = new TreeMap<>();
    List<Namespace> pending = new java.util.ArrayList<>(reader.listNamespaces(Namespace.empty()));
    while (!pending.isEmpty()) {
      Namespace ns = pending.remove(0);
      state.put("namespace " + ns, new TreeMap<>(reader.loadNamespaceMetadata(ns)).toString());
      for (TableIdentifier id : reader.listTables(ns)) {
        Table table = reader.loadTable(id);
        Map<String, String> props = new TreeMap<>(table.properties());
        props.keySet().removeIf(k -> !k.equals("origin") && !KEYS.contains(k));
        java.util.Set<String> files = new java.util.TreeSet<>();
        try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
            table.newScan().planFiles()) {
          tasks.forEach(task -> files.add(task.file().location()));
        } catch (java.io.IOException e) {
          throw new java.io.UncheckedIOException(e);
        }
        state.put("table " + id, props + " files " + files);
      }
      pending.addAll(reader.listNamespaces(ns));
    }
    return state.toString();
  }

  private static String state(Setup setup) {
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    setup.apply(LocalCatalogMode.APPEND.catalog(io, WAREHOUSE, "c", Map.of()));
    return state(io);
  }
}
