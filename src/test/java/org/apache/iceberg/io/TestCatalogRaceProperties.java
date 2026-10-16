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
 * run after it.)
 *
 * <p>The operations are namespace creates (of nested namespaces too, which create their parents),
 * drops and property changes, and table creates, drops, renames and property changes, on a small
 * universe of names, from a generated catalog state; each writer has its own catalog mode. A
 * result is a return value or an exception's class; the catalog's state is its namespaces with
 * their properties and its tables with theirs (a table created carries its creator's name).
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
      @ForAll("maybeOps") Op c) {
    List<Op> racers = c.kind == null ? List.of(b) : List.of(b, c);
    List<LocalCatalogMode> racerModes = List.of(modeB, modeC);

    // The race: the k-th racer runs just before A's k-th write to the catalog.
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    setup.apply(LocalCatalogMode.APPEND.catalog(io, WAREHOUSE, "c", Map.of()));
    FileIOCatalog writerA = modeA.catalog(io, WAREHOUSE, "c", Map.of());
    List<FileIOCatalog> writers = List.of(
        modeB.catalog(io, WAREHOUSE, "c", Map.of()), modeC.catalog(io, WAREHOUSE, "c", Map.of()));
    String[] results = new String[racers.size()];
    int[] writes = {0};
    boolean[] nested = {false};
    io.injectFaults((location, strategy) -> {
      if (!nested[0] && location.equals(CATALOG)) {
        int k = writes[0]++;
        if (k < racers.size()) {
          nested[0] = true;
          try {
            results[k] = racers.get(k).run(writers.get(k), name(k));
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

    // The serial run on another copy: the racers that ran before A's writes, A, the rest.
    InMemoryAtomicFileIO serialIo = new InMemoryAtomicFileIO();
    setup.apply(LocalCatalogMode.APPEND.catalog(serialIo, WAREHOUSE, "c", Map.of()));
    String[] serial = new String[racers.size()];
    for (int k = 0; k < before; k++) {
      serial[k] = racers.get(k).run(
          racerModes.get(k).catalog(serialIo, WAREHOUSE, "c", Map.of()), name(k));
    }
    String serialA = a.run(modeA.catalog(serialIo, WAREHOUSE, "c", Map.of()), "A");
    for (int k = before; k < racers.size(); k++) {
      serial[k] = racers.get(k).run(
          racerModes.get(k).catalog(serialIo, WAREHOUSE, "c", Map.of()), name(k));
    }
    String serialState = state(serialIo);

    Statistics.label("racers before A's writes").collect(before + " of " + racers.size());
    Statistics.label("A").collect(a.kind + " " + (raceA.startsWith("ok") ? "ok" : "fails"));
    StringBuilder context = new StringBuilder(
        String.format("%nsetup %s%nA (%s): %s", setup, modeA, a));
    for (int k = 0; k < racers.size(); k++) {
      context.append(String.format("%n%s (%s): %s, %s", name(k), racerModes.get(k),
          racers.get(k), k < before ? "just before A's write #" + k : "after A"));
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
  }

  private static String name(int racer) {
    return racer == 0 ? "B" : "C";
  }

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
    SET_TABLE_PROPERTY
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
          default:
            Table table = catalog.loadTable(table());
            table.updateProperties().set(key(), who).commit();
            return "ok";
        }
      } catch (RuntimeException e) {
        return e.getClass().getSimpleName();
      }
    }

    @Override
    public String toString() {
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
        Map<String, String> props = new TreeMap<>(reader.loadTable(id).properties());
        props.keySet().removeIf(k -> !k.equals("origin") && !KEYS.contains(k));
        state.put("table " + id, props.toString());
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
