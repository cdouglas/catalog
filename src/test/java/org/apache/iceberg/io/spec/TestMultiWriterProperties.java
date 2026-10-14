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
package org.apache.iceberg.io.spec;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CatalogFile;
import org.apache.iceberg.io.InMemoryAtomicFileIO;
import org.apache.iceberg.io.ProtoCatalogFile;
import org.apache.iceberg.io.ProtoCatalogFormat;

/**
 * Multi-writer fuzzer for the commit protocol, at the catalog-format layer.
 *
 * <p>A generated schedule interleaves, on one thread, the steps of several writers. Each writer
 * reads a snapshot, stages a transaction of namespace, property and table operations against it,
 * and commits. The format's knobs are drawn per schedule, so commits mix appends, CAS and
 * compaction by count and by size. Every commit that changes the file is checked as one step from
 * the version it replaced ({@code pre}) to the version it wrote ({@code post}). Successive writes
 * chain, so the steps together cover the whole history:
 *
 * <ul>
 *   <li>Serial re-execution: an acknowledged transaction, staged again on a fresh read of {@code
 *       pre} and committed alone, must be accepted and reach the state {@code post}. Table updates
 *       and reads also require the table to be unchanged since the writer's snapshot (Iceberg's
 *       base-metadata check). The history is then the serial execution of the acknowledged
 *       transactions in log order.
 *   <li>Effects: {@code post} is {@code pre} plus the transaction's own changes and nothing else.
 *       This catches bugs that the serial and concurrent paths share.
 *   <li>Rejection: a rejected transaction whose record landed leaves the state unchanged.
 *   <li>Integrity: every table's namespace and every namespace's parent exist.
 * </ul>
 *
 * <p>Conflicts are not asserted: rejecting a commit is always safe. The outcome statistics report
 * how often a rejected transaction would have re-executed cleanly (a spurious conflict).
 */
class TestMultiWriterProperties {

  private static final String CATALOG = "mem:///warehouse/catalog";

  // Small universes, so writers collide often.
  private static final List<Namespace> NAMESPACES =
      List.of(
          Namespace.of("a"),
          Namespace.of("b"),
          Namespace.of("c"),
          Namespace.of("a", "x"),
          Namespace.of("a", "y"),
          Namespace.of("b", "x"),
          Namespace.of("a", "x", "p"));
  private static final List<String> TABLE_NAMES = List.of("t", "u");
  private static final List<TableIdentifier> TABLES = tablesOf(NAMESPACES);
  private static final List<String> KEYS = List.of("k1", "k2");

  @Property(tries = 300)
  void historyIsSerialExecutionOfAcknowledgedTransactions(
      @ForAll @IntRange(min = 2, max = 4) int writers,
      @ForAll("maxAppendCounts") int maxAppendCount,
      @ForAll("appendHeadrooms") long appendHeadroom,
      @ForAll("schedules") List<Cmd> schedule) {
    new Run(writers, maxAppendCount, appendHeadroom).execute(schedule);
  }

  @Provide
  Arbitrary<Integer> maxAppendCounts() {
    return Arbitraries.of(0, 1, 2, 3, 10_000);
  }

  /**
   * max.append.size as headroom over the seeded file, in bytes; negative for the 16 MiB default.
   * A record is roughly 50-150 bytes, so the small headrooms make writers compact by size,
   * including on the retry after a lost append race.
   */
  @Provide
  Arbitrary<Long> appendHeadrooms() {
    return Arbitraries.of(-1L, 100L, 200L, 400L, 800L);
  }

  @Provide
  Arbitrary<List<Cmd>> schedules() {
    Arbitrary<Step> steps = Arbitraries.frequency(
        Tuple.of(2, Step.BEGIN), Tuple.of(8, Step.OP), Tuple.of(2, Step.COMMIT));
    Arbitrary<Integer> small = Arbitraries.integers().between(0, 11);
    return Combinators.combine(
            steps,
            Arbitraries.integers().between(0, 3),
            Arbitraries.of(OpKind.values()),
            small,
            small)
        .as(Cmd::new)
        .list()
        .ofMinSize(1)
        .ofMaxSize(60)
        .withSizeDistribution(RandomDistribution.uniform());
  }

  // ============================================================
  // Schedule
  // ============================================================

  enum Step {
    BEGIN,
    OP,
    COMMIT
  }

  enum OpKind {
    CREATE_NAMESPACE,
    DROP_NAMESPACE,
    SET_PROPERTY,
    REMOVE_PROPERTY,
    CREATE_TABLE,
    DROP_TABLE,
    UPDATE_TABLE,
    RENAME_TABLE,
    READ_TABLE
  }

  /**
   * One generated command. {@code a} and {@code b} pick operands by index, interpreted against the
   * writer's snapshot when the command runs, so every list of commands is meaningful and shrinks
   * cleanly. An operation on an existing entity picks from the snapshot when {@code a < 8} and from
   * the whole universe otherwise, which reaches entities created earlier in the same transaction
   * and ones that do not exist. Commands that do not apply (a commit by an idle writer, an
   * operation the transaction rejects while staging) are skipped.
   */
  static final class Cmd {
    final Step step;
    final int writer;
    final OpKind op;
    final int a;
    final int b;

    Cmd(Step step, int writer, OpKind op, int a, int b) {
      this.step = step;
      this.writer = writer;
      this.op = op;
      this.a = a;
      this.b = b;
    }

    @Override
    public String toString() {
      return step == Step.OP
          ? String.format("w%d:%s(%d,%d)", writer, op, a, b)
          : String.format("w%d:%s", writer, step);
    }
  }

  /** A staged operation with concrete operands. */
  static final class Op {
    final OpKind kind;
    final Namespace namespace;
    final TableIdentifier table;
    final TableIdentifier to;
    final String key;
    final String value; // property value, or table location for create / update
    final String base; // table location in the writer's snapshot, for update / read

    private Op(
        OpKind kind,
        Namespace namespace,
        TableIdentifier table,
        TableIdentifier to,
        String key,
        String value,
        String base) {
      this.kind = kind;
      this.namespace = namespace;
      this.table = table;
      this.to = to;
      this.key = key;
      this.value = value;
      this.base = base;
    }

    static Op namespace(OpKind kind, Namespace ns, String key, String value) {
      return new Op(kind, ns, null, null, key, value, null);
    }

    static Op table(OpKind kind, TableIdentifier table, String location, String base) {
      return new Op(kind, null, table, null, null, location, base);
    }

    static Op rename(TableIdentifier from, TableIdentifier to) {
      return new Op(OpKind.RENAME_TABLE, null, from, to, null, null, null);
    }

    void stage(CatalogFile.Mut<ProtoCatalogFile, ?> mut) {
      switch (kind) {
        case CREATE_NAMESPACE:
          Map<String, String> props = new HashMap<>();
          if (key != null) {
            props.put(key, value);
          }
          mut.createNamespace(namespace, props);
          break;
        case DROP_NAMESPACE:
          mut.dropNamespace(namespace);
          break;
        case SET_PROPERTY:
          mut.updateProperties(namespace, new HashMap<>(Map.of(key, value)));
          break;
        case REMOVE_PROPERTY:
          mut.updateProperties(namespace, Collections.singletonMap(key, null));
          break;
        case CREATE_TABLE:
          mut.createTable(table, value);
          break;
        case DROP_TABLE:
          mut.dropTable(table);
          break;
        case UPDATE_TABLE:
          mut.updateTable(table, value);
          break;
        case RENAME_TABLE:
          mut.renameTable(table, to);
          break;
        case READ_TABLE:
          mut.readTable(table);
          break;
        default:
          throw new IllegalStateException("Unknown op " + kind);
      }
    }

    /** The intended change, applied to a logical view. */
    void applyTo(View view) {
      switch (kind) {
        case CREATE_NAMESPACE:
          // Staging a namespace creates its missing ancestors.
          for (Namespace anc = parentOf(namespace); !anc.isEmpty(); anc = parentOf(anc)) {
            view.namespaces.putIfAbsent(anc, new TreeMap<>());
          }
          Map<String, String> props = new TreeMap<>();
          if (key != null) {
            props.put(key, value);
          }
          view.namespaces.put(namespace, props);
          break;
        case DROP_NAMESPACE:
          view.namespaces.remove(namespace);
          break;
        case SET_PROPERTY:
          view.namespaces.get(namespace).put(key, value);
          break;
        case REMOVE_PROPERTY:
          view.namespaces.get(namespace).remove(key);
          break;
        case CREATE_TABLE:
        case UPDATE_TABLE:
          view.tables.put(table, value);
          break;
        case DROP_TABLE:
          view.tables.remove(table);
          break;
        case RENAME_TABLE:
          view.tables.put(to, view.tables.remove(table));
          break;
        case READ_TABLE:
          break;
        default:
          throw new IllegalStateException("Unknown op " + kind);
      }
    }

    /** Iceberg's base-metadata check: an update or read needs the table it saw. */
    boolean baseHolds(CatalogFile state) {
      return base == null || base.equals(state.location(table));
    }

    @Override
    public String toString() {
      switch (kind) {
        case CREATE_NAMESPACE:
          return String.format(
              "createNamespace(%s%s)", namespace, key == null ? "" : ", " + key + "=" + value);
        case DROP_NAMESPACE:
          return "dropNamespace(" + namespace + ")";
        case SET_PROPERTY:
          return String.format("setProperty(%s, %s=%s)", namespace, key, value);
        case REMOVE_PROPERTY:
          return String.format("removeProperty(%s, %s)", namespace, key);
        case RENAME_TABLE:
          return String.format("renameTable(%s, %s)", table, to);
        default:
          return String.format(
              "%s(%s%s)", kind.name().toLowerCase(), table, value == null ? "" : ", " + value);
      }
    }
  }

  /** The logical catalog state: namespaces with properties, and table locations. */
  static final class View {
    final Map<Namespace, Map<String, String>> namespaces = new TreeMap<>(NS_ORDER);
    final Map<TableIdentifier, String> tables = new TreeMap<>(TABLE_ORDER);

    static View of(CatalogFile state) {
      View view = new View();
      for (Namespace ns : state.namespaces()) {
        view.namespaces.put(ns, new TreeMap<>(state.namespaceProperties(ns)));
      }
      for (TableIdentifier t : state.tables()) {
        view.tables.put(t, state.location(t));
      }
      return view;
    }

    View copy() {
      View view = new View();
      namespaces.forEach((ns, props) -> view.namespaces.put(ns, new TreeMap<>(props)));
      view.tables.putAll(tables);
      return view;
    }

    /** Why the view is not referentially intact, or null if it is. */
    String integrityViolation() {
      for (Namespace ns : namespaces.keySet()) {
        Namespace parent = parentOf(ns);
        if (!parent.isEmpty() && !namespaces.containsKey(parent)) {
          return "namespace " + ns + " has no parent";
        }
      }
      for (TableIdentifier t : tables.keySet()) {
        if (!namespaces.containsKey(t.namespace())) {
          return "table " + t + " has no namespace";
        }
      }
      return null;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof View
          && namespaces.equals(((View) o).namespaces)
          && tables.equals(((View) o).tables);
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespaces, tables);
    }

    @Override
    public String toString() {
      return "namespaces=" + namespaces + " tables=" + tables;
    }
  }

  private static final Comparator<Namespace> NS_ORDER = Comparator.comparing(Namespace::toString);
  private static final Comparator<TableIdentifier> TABLE_ORDER =
      Comparator.comparing(TableIdentifier::toString);

  private static Namespace parentOf(Namespace ns) {
    return ns.length() > 1
        ? Namespace.of(Arrays.copyOfRange(ns.levels(), 0, ns.length() - 1))
        : Namespace.empty();
  }

  // ============================================================
  // Execution
  // ============================================================

  private static final class Writer {
    final int id;
    ProtoCatalogFile snapshot; // null when idle
    byte[] snapshotBytes;
    List<Op> ops;

    Writer(int id) {
      this.id = id;
    }
  }

  enum Outcome {
    ACKNOWLEDGED,
    REJECTED
  }

  private static final class Run {
    private final InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    private final ProtoCatalogFormat format;
    private final Writer[] writers;
    private final StringBuilder trace = new StringBuilder();
    private int nextValue = 0;

    Run(int writers, int maxAppendCount, long appendHeadroom) {
      CatalogFile.Mut<ProtoCatalogFile, ?> seed =
          new ProtoCatalogFormat().empty(io.newInputFile(CATALOG));
      seed.createNamespace(Namespace.of("a"));
      seed.createNamespace(Namespace.of("b"));
      seed.createTable(TableIdentifier.of("a", "t"), "mem:///a/t/seed");
      seed.commit(io);
      int seedLength = liveBytes().length;
      this.format =
          new ProtoCatalogFormat(
              maxAppendCount,
              appendHeadroom < 0
                  ? ProtoCatalogFormat.DEFAULT_MAX_APPEND_SIZE
                  : seedLength + appendHeadroom);
      this.writers = new Writer[writers];
      for (int i = 0; i < writers; i++) {
        this.writers[i] = new Writer(i);
      }
      trace.append(String.format("format: maxAppendCount=%d maxAppendSize=%d (seed %d bytes)%n",
          format.maxAppendCount(), format.maxAppendSize(), seedLength));
      trace.append("seed: ").append(View.of(read())).append('\n');
    }

    void execute(List<Cmd> schedule) {

      for (Cmd cmd : schedule) {
        Writer w = writers[cmd.writer % writers.length];
        switch (cmd.step) {
          case BEGIN:
            if (w.snapshot == null) {
              w.snapshot = read();
              w.snapshotBytes = liveBytes();
              w.ops = new ArrayList<>();
              trace.append(String.format("w%d begin%n", w.id));
            }
            break;
          case OP:
            if (w.snapshot != null) {
              Op op = interpret(cmd, w.snapshot);
              if (op != null && stages(w.snapshot, append(w.ops, op))) {
                w.ops.add(op);
                trace.append(String.format("w%d   %s%n", w.id, op));
                Statistics.label("staged").collect(op.kind);
              } else if (op != null) {
                Statistics.label("staged").collect("rejected while staging");
              }
            }
            break;
          case COMMIT:
            if (w.snapshot != null) {
              commit(w);
            }
            break;
          default:
            throw new IllegalStateException("Unknown step " + cmd.step);
        }
      }
      for (Writer w : writers) {
        if (w.snapshot != null) {
          commit(w);
        }
      }
    }

    /** Concrete operands for {@code cmd} from the writer's snapshot, or null if none fit. */
    private Op interpret(Cmd cmd, ProtoCatalogFile snap) {
      List<Namespace> namespaces = new ArrayList<>(snap.namespaces());
      namespaces.sort(NS_ORDER);
      List<TableIdentifier> tables = new ArrayList<>(snap.tables());
      tables.sort(TABLE_ORDER);
      String value = "v" + nextValue++;
      switch (cmd.op) {
        case CREATE_NAMESPACE:
          Namespace created = NAMESPACES.get(cmd.a % NAMESPACES.size());
          return cmd.b % 3 == 0
              ? Op.namespace(cmd.op, created, null, null)
              : Op.namespace(cmd.op, created, KEYS.get(cmd.b % 2), value);
        case DROP_NAMESPACE:
          return Op.namespace(cmd.op, pick(namespaces, NAMESPACES, cmd.a), null, null);
        case SET_PROPERTY:
        case REMOVE_PROPERTY:
          return Op.namespace(
              cmd.op,
              pick(namespaces, NAMESPACES, cmd.a),
              KEYS.get(cmd.b % 2),
              cmd.op == OpKind.SET_PROPERTY ? value : null);
        case CREATE_TABLE:
          TableIdentifier fresh =
              TableIdentifier.of(pick(namespaces, NAMESPACES, cmd.a), TABLE_NAMES.get(cmd.b % 2));
          return Op.table(cmd.op, fresh, "mem:///" + fresh + "/" + value, null);
        case DROP_TABLE:
          return Op.table(cmd.op, pick(tables, TABLES, cmd.a), null, null);
        case UPDATE_TABLE:
        case READ_TABLE:
          TableIdentifier t = pick(tables, TABLES, cmd.a);
          String location = cmd.op == OpKind.UPDATE_TABLE ? "mem:///" + t + "/" + value : null;
          return Op.table(cmd.op, t, location, snap.location(t));
        case RENAME_TABLE:
          return Op.rename(pick(tables, TABLES, cmd.a), pick(TABLES, cmd.b));
        default:
          throw new IllegalStateException("Unknown op " + cmd.op);
      }
    }

    private void commit(Writer w) {
      List<Op> ops = w.ops;
      byte[] liveBefore = liveBytes();
      int mark = io.history().size();
      int lostAppends = io.rejected(AtomicOutputFile.Strategy.APPEND);
      Outcome outcome;
      String reason = "";
      try {
        mut(w.snapshot, ops).commit(io);
        outcome = Outcome.ACKNOWLEDGED;
      } catch (CommitFailedException | AlreadyExistsException e) {
        outcome = Outcome.REJECTED;
        reason = " (" + e.getClass().getSimpleName() + ")";
      }
      List<InMemoryAtomicFileIO.Write> writes = io.history().subList(mark, io.history().size());
      InMemoryAtomicFileIO.Write write = writes.isEmpty() ? null : writes.get(0);
      String path =
          (io.rejected(AtomicOutputFile.Strategy.APPEND) > lostAppends ? "lost append race, " : "")
              + (write == null ? "no write" : write.strategy() + rebased(w, write));
      trace.append(String.format("w%d commit -> %s%s via %s%n", w.id, outcome, reason, path));
      Statistics.label("commit path").collect(path);
      Statistics.label("operations per transaction").collect(Math.min(ops.size(), 5));
      check(writes.size() <= 1, "one commit made %d conditional writes", writes.size());

      if (outcome == Outcome.ACKNOWLEDGED) {
        check(write != null, "acknowledged commit changed nothing");
        ProtoCatalogFile pre = parse(write.before());
        View post = View.of(parse(write.after()));
        String invalid = reexecutionViolation(pre, ops);
        check(invalid == null, "acknowledged transaction is invalid on the state it replaced: %s",
            invalid);
        InMemoryAtomicFileIO alone = scratch(write.before());
        ProtoCatalogFile fresh = format.read(alone, alone.newInputFile(CATALOG));
        View serial = View.of(mut(fresh, ops).commit(alone));
        check(serial.equals(post), "serial re-execution differs:%n  serial: %s%n  actual: %s",
            serial, post);
        View expected = View.of(pre).copy();
        ops.forEach(op -> op.applyTo(expected));
        check(expected.equals(post), "effects differ:%n  expected: %s%n  actual:   %s",
            expected, post);
        checkIntegrity(post);
        Statistics.label("outcome").collect("acknowledged");
      } else {
        if (write != null) {
          View post = View.of(parse(write.after()));
          check(post.equals(View.of(parse(write.before()))),
              "rejected transaction changed the state to %s", post);
          checkIntegrity(post);
        }
        boolean spurious = reexecutionViolation(parse(liveBefore), ops) == null;
        Statistics.label("outcome").collect(
            (write == null ? "rejected by CAS conflict" : "rejected at replay")
                + (spurious ? ", spurious" : ""));
      }
      w.snapshot = null;
      w.snapshotBytes = null;
      w.ops = null;
    }

    /** Why {@code ops} cannot be re-executed serially on {@code state}, or null if they can. */
    private String reexecutionViolation(ProtoCatalogFile state, List<Op> ops) {
      for (Op op : ops) {
        if (!op.baseHolds(state)) {
          return op + " expects " + op.base + ", found " + state.location(op.table);
        }
      }
      return stages(state, ops) ? null : "staging " + ops + " on a fresh read fails";
    }

    private String rebased(Writer w, InMemoryAtomicFileIO.Write write) {
      return write.strategy() == AtomicOutputFile.Strategy.CAS
              && !Arrays.equals(write.before(), w.snapshotBytes)
          ? " of a newer version"
          : "";
    }

    private void checkIntegrity(View view) {
      String violation = view.integrityViolation();
      check(violation == null, "integrity: %s in %s", violation, view);
    }

    private void check(boolean condition, String format, Object... args) {
      if (!condition) {
        throw new AssertionError(String.format(format, args) + "\ntrace:\n" + trace);
      }
    }

    private CatalogFile.Mut<ProtoCatalogFile, ?> mut(ProtoCatalogFile base, List<Op> ops) {
      CatalogFile.Mut<ProtoCatalogFile, ?> mut = format.from(base);
      ops.forEach(op -> op.stage(mut));
      return mut;
    }

    /** Whether {@code ops} stage cleanly on {@code base}: the client-side validity check. */
    private boolean stages(ProtoCatalogFile base, List<Op> ops) {
      try {
        mut(base, ops);
        return true;
      } catch (AlreadyExistsException
          | NoSuchNamespaceException
          | NoSuchTableException
          | NamespaceNotEmptyException
          | IllegalArgumentException
          | IllegalStateException e) {
        return false;
      }
    }

    private ProtoCatalogFile read() {
      return format.read(io, io.newInputFile(CATALOG));
    }

    private byte[] liveBytes() {
      try (InputStream in = io.newInputFile(CATALOG).newStream()) {
        return in.readAllBytes();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private InMemoryAtomicFileIO scratch(byte[] bytes) {
      InMemoryAtomicFileIO scratch = new InMemoryAtomicFileIO();
      scratch.seed(CATALOG, bytes);
      return scratch;
    }

    private ProtoCatalogFile parse(byte[] bytes) {
      InMemoryAtomicFileIO scratch = scratch(bytes);
      return format.read(scratch, scratch.newInputFile(CATALOG));
    }
  }

  private static <T> T pick(List<T> items, int index) {
    return items.get(index % items.size());
  }

  private static <T> T pick(List<T> snapshot, List<T> universe, int index) {
    return index >= 8 || snapshot.isEmpty() ? pick(universe, index) : pick(snapshot, index);
  }

  private static List<TableIdentifier> tablesOf(List<Namespace> namespaces) {
    List<TableIdentifier> tables = new ArrayList<>();
    for (Namespace ns : namespaces) {
      for (String name : TABLE_NAMES) {
        tables.add(TableIdentifier.of(ns, name));
      }
    }
    return tables;
  }

  private static <T> List<T> append(List<T> items, T item) {
    List<T> copy = new ArrayList<>(items);
    copy.add(item);
    return copy;
  }
}
