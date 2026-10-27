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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
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
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.CatalogLog;
import org.apache.iceberg.io.CatalogMutation;
import org.apache.iceberg.io.CatalogTestSupport;
import org.apache.iceberg.io.InMemoryAtomicFileIO;
import org.apache.iceberg.io.CatalogState;
import org.apache.iceberg.io.LogCatalogProperties;
import org.apache.iceberg.io.spec.MultiWriterStorage.ReadFault;
import org.apache.iceberg.io.spec.MultiWriterStorage.WriteFault;
import org.intentlog.Replica;
import org.intentlog.RuntimeConfig;

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
 *       pre} and committed alone, must be accepted and reach the state {@code post}. Updates of a
 *       table's metadata location and reads also require the table to be unchanged since the
 *       writer's snapshot (Iceberg's base-metadata check); a property or location delta of an
 *       inline table, which the catalog's conflict encoding (drawn per run) may apply on a table
 *       that changed since, is judged by its re-execution alone. The history is then the serial
 *       execution of the acknowledged transactions in log order.
 *   <li>Effects: {@code post} is {@code pre} plus the transaction's own changes and nothing else.
 *       This catches bugs that the serial and concurrent paths share.
 *   <li>Rejection: a rejected transaction whose record landed leaves the state unchanged.
 *   <li>Progress: a commit nothing raced (no commit since its snapshot, none inside it), on storage
 *       that did not fail, is acknowledged.
 *   <li>Integrity: every table's namespace and every namespace's parent exist.
 *   <li>Checkpoints: every version written reads the same as a checkpoint of it, which the next
 *       compaction would write (see {@link CheckpointRoundTrip}).
 * </ul>
 *
 * <p>Otherwise conflicts are not asserted: rejecting a commit that raced another is always safe.
 * The outcome statistics report how often a rejected transaction would have re-executed cleanly (a
 * spurious conflict).
 *
 * <p>A commit may also be preempted: another writer with a transaction open commits it to
 * completion just before one of the first commit's storage requests (its conditional writes and
 * reads of the catalog), so it lands inside the first commit's loop: before or after the first
 * commit's write, between a lost reply and the read that resolves it, between a conflict and the
 * rebase. Each commit is checked on its own writes. Storage may also be strict about
 * versions, as it is in practice: a read pinned to a version that has since been replaced fails
 * (a GCS read by generation, an S3 or ADLS read with If-Match), where the lenient model returns
 * the replaced bytes. With neither faults nor preemption, nothing in a commit fails.
 *
 * <p>{@link #faultyStorageNeverMisreportsACommit} runs the same schedules on storage that fails
 * (see {@link MultiWriterStorage.WriteFault}): a write may not land, or land with its reply lost,
 * or land and report a precondition failure, or be throttled before or after it lands; and a read
 * of the catalog during the commit may fail (see {@link MultiWriterStorage.ReadFault}). A writer
 * must be able to trust a definite answer.
 * Iceberg deletes a commit's new files on {@link CommitFailedException} (a {@code
 * CleanableFailure}) and, without strict cleanup, on any failure but {@link
 * CommitStateUnknownException}, so a transaction reported any other way as failed must not have
 * taken effect, and an acknowledged one took effect once. A commit may now retry, so it may write more than once; at most one of its writes changes
 * the state, and that one is checked as above.
 *
 * <p>Each property runs on one catalog object ({@link MultiWriterStorage.File}, as before the log
 * was split from the catalog) and on a shared log with checkpoints apart ({@link
 * MultiWriterStorage.Memory}). On the shared log a send may also be delayed in flight and land
 * later ({@link Step#DELIVER}, and at the end of the run): a landing that takes effect must be the
 * only effect of a commit whose outcome was unknown, and is checked as its write. A compactor may
 * run between commits ({@link Step#COMPACT}), and the checkpoints it stores must be replays of the
 * log through them. On both, a copy of any intention record that ever landed is appended again
 * ({@link Step#DUPLICATE}), whether its original is still a record, folded into a checkpoint, or
 * past the GC horizon: it must change nothing.
 */
class TestMultiWriterProperties {

  private static final String CATALOG = MultiWriterStorage.CATALOG;

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
      @ForAll boolean strictVersions,
      @ForAll boolean gc,
      @ForAll("segmentLimits") int segmentLimit,
      @ForAll("encodings") String encoding,
      @ForAll("schedules") List<Cmd> schedule) {
    new Run(new MultiWriterStorage.File(strictVersions, segmentLimit), writers, maxAppendCount,
        appendHeadroom, false, gc, encoding).execute(schedule);
  }

  @Property(tries = 300)
  void faultyStorageNeverMisreportsACommit(
      @ForAll @IntRange(min = 2, max = 4) int writers,
      @ForAll("maxAppendCounts") int maxAppendCount,
      @ForAll("appendHeadrooms") long appendHeadroom,
      @ForAll boolean strictVersions,
      @ForAll boolean gc,
      @ForAll("segmentLimits") int segmentLimit,
      @ForAll("encodings") String encoding,
      @ForAll("schedules") List<Cmd> schedule) {
    new Run(new MultiWriterStorage.File(strictVersions, segmentLimit), writers, maxAppendCount,
        appendHeadroom, true, gc, encoding).execute(schedule);
  }

  @Property(tries = 300)
  void onASharedLogHistoryIsSerialExecutionOfAcknowledgedTransactions(
      @ForAll @IntRange(min = 2, max = 4) int writers,
      @ForAll("maxAppendCounts") int maxAppendCount,
      @ForAll("appendHeadrooms") long appendHeadroom,
      @ForAll boolean conditional,
      @ForAll boolean gc,
      @ForAll("encodings") String encoding,
      @ForAll("schedules") List<Cmd> schedule) {
    new Run(new MultiWriterStorage.Memory(conditional), writers, maxAppendCount, appendHeadroom,
        false, gc, encoding).execute(schedule);
  }

  @Property(tries = 300)
  void onASharedLogFaultyStorageNeverMisreportsACommit(
      @ForAll @IntRange(min = 2, max = 4) int writers,
      @ForAll("maxAppendCounts") int maxAppendCount,
      @ForAll("appendHeadrooms") long appendHeadroom,
      @ForAll boolean conditional,
      @ForAll boolean gc,
      @ForAll("encodings") String encoding,
      @ForAll("schedules") List<Cmd> schedule) {
    new Run(new MultiWriterStorage.Memory(conditional), writers, maxAppendCount, appendHeadroom,
        true, gc, encoding).execute(schedule);
  }

  /**
   * A commit step's third operand picks a read of the catalog during the commit to fail, two times
   * in three: one of its first four read requests, which fails (outright or mid-stream on one
   * object; on a shared log, outright or served stale). Later reads succeed. Returns the index of
   * the failing read, or -1.
   */
  private static int failingRead(int c) {
    return c < 4 ? -1 : (c - 4) / 2;
  }

  /**
   * A commit step's fourth operand picks, half the time, the storage request of the commit (0 to 3)
   * at which another writer's open transaction commits. Returns -1 for none.
   */
  private static int preemptAt(int d) {
    return d < 6 ? -1 : (d - 6) % 4;
  }

  /** The catalog's conflict encoding: how replay judges a table update that raced another. */
  @Provide
  Arbitrary<String> encodings() {
    return Arbitraries.of(CatalogTestSupport.ENCODINGS);
  }

  @Provide
  Arbitrary<Integer> maxAppendCounts() {
    return Arbitraries.of(0, 1, 2, 3, 10_000);
  }

  /** The most writes the catalog object holds (0: no cap); below the append counts drawn. */
  @Provide
  Arbitrary<Integer> segmentLimits() {
    return Arbitraries.of(0, 0, 2, 4);
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
        Tuple.of(2, Step.BEGIN), Tuple.of(8, Step.OP), Tuple.of(2, Step.COMMIT),
        Tuple.of(1, Step.DUPLICATE), Tuple.of(1, Step.COMPACT), Tuple.of(1, Step.DELIVER));
    Arbitrary<Integer> small = Arbitraries.integers().between(0, 11);
    return Combinators.combine(
            steps,
            Arbitraries.integers().between(0, 3),
            opKinds(),
            small,
            small,
            small,
            small)
        .as(Cmd::new)
        .list()
        .ofMinSize(1)
        .ofMaxSize(60)
        .withSizeDistribution(RandomDistribution.uniform());
  }

  /** Each kind once, and an inline table's deltas, which encodings judge apart, three times. */
  private static Arbitrary<OpKind> opKinds() {
    List<Tuple.Tuple2<Integer, OpKind>> kinds = new ArrayList<>();
    for (OpKind kind : OpKind.values()) {
      boolean delta = kind == OpKind.SET_TABLE_PROPERTY || kind == OpKind.SET_TABLE_LOCATION;
      kinds.add(Tuple.of(delta ? 3 : 1, kind));
    }
    return Arbitraries.frequency(kinds.toArray(new Tuple.Tuple2[0]));
  }

  // ============================================================
  // Schedule
  // ============================================================

  enum Step {
    BEGIN,
    OP,
    COMMIT,
    /** A copy of an intention record that landed ({@code a} picks it) is appended again. */
    DUPLICATE,
    /** A compactor runs, on a log that keeps checkpoints apart. */
    COMPACT,
    /** What was delayed in flight lands. */
    DELIVER
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
    READ_TABLE,
    /** Sets a property of an inline table, by a delta. */
    SET_TABLE_PROPERTY,
    /** Moves an inline table's location, by a delta. */
    SET_TABLE_LOCATION
  }

  /**
   * One generated command. {@code a} and {@code b} pick operands by index, interpreted against the
   * writer's snapshot when the command runs, so every list of commands is meaningful and shrinks
   * cleanly. An operation on an existing entity picks from the snapshot when {@code a < 8} and from
   * the whole universe otherwise, which reaches entities created earlier in the same transaction
   * and ones that do not exist. Commands that do not apply (a commit by an idle writer, an
   * operation the transaction rejects while staging) are skipped. On faulty storage, a commit's
   * {@code a} and {@code b} pick its write faults and {@code c} its read fault; {@code d} picks
   * where another writer's commit may preempt it.
   */
  static final class Cmd {
    final Step step;
    final int writer;
    final OpKind op;
    final int a;
    final int b;
    final int c;
    final int d;

    Cmd(Step step, int writer, OpKind op, int a, int b, int c, int d) {
      this.step = step;
      this.writer = writer;
      this.op = op;
      this.a = a;
      this.b = b;
      this.c = c;
      this.d = d;
    }

    @Override
    public String toString() {
      if (step == Step.OP) {
        return String.format("w%d:%s(%d,%d)", writer, op, a, b);
      }
      switch (step) {
        case COMMIT:
          return String.format("w%d:COMMIT(%d,%d,%d,%d)", writer, a, b, c, d);
        case DUPLICATE:
          return String.format("DUPLICATE(%d)", a);
        case COMPACT:
        case DELIVER:
          return step.toString();
        default:
          return String.format("w%d:%s", writer, step);
      }
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
    // An inline table's metadata, for a create; the delta, for a property or location change
    final byte[] payload;
    // The table's version in the writer's snapshot, for a delta
    int read = -1;

    private Op(
        OpKind kind,
        Namespace namespace,
        TableIdentifier table,
        TableIdentifier to,
        String key,
        String value,
        String base,
        byte[] payload) {
      this.kind = kind;
      this.namespace = namespace;
      this.table = table;
      this.to = to;
      this.key = key;
      this.value = value;
      this.base = base;
      this.payload = payload;
    }

    static Op namespace(OpKind kind, Namespace ns, String key, String value) {
      return new Op(kind, ns, null, null, key, value, null, null);
    }

    static Op table(OpKind kind, TableIdentifier table, String location, String base) {
      return new Op(kind, null, table, null, null, location, base, null);
    }

    /** A create of an inline table at {@code location}. */
    static Op inlineTable(TableIdentifier table, String location) {
      return new Op(OpKind.CREATE_TABLE, null, table, null, null, location, null,
          CatalogTestSupport.newInlineTable(location));
    }

    /**
     * A delta of an inline table: a property set to {@code value}, or the location moved to it. It
     * names no base: a conflict encoding may apply it on a table changed since the writer read it,
     * which its re-execution judges.
     */
    static Op delta(OpKind kind, TableIdentifier table, String key, String value) {
      long now = System.currentTimeMillis();
      byte[] delta = kind == OpKind.SET_TABLE_PROPERTY
          ? CatalogTestSupport.propertyDelta(key, value, now)
          : CatalogTestSupport.locationDelta(value, now);
      return new Op(kind, null, table, null, key, value, null, delta);
    }

    static Op rename(TableIdentifier from, TableIdentifier to) {
      return new Op(OpKind.RENAME_TABLE, null, from, to, null, null, null, null);
    }

    void stage(CatalogMutation mut) {
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
          if (payload != null) {
            mut.createTableInline(table, payload, CatalogTestSupport.inlineTableCodec());
          } else {
            mut.createTable(table, value);
          }
          break;
        case DROP_TABLE:
          mut.dropTable(table);
          break;
        case SET_TABLE_PROPERTY:
        case SET_TABLE_LOCATION:
          mut.updateTableInlineDelta(table, payload);
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
          if (payload != null) {
            view.tables.put(table, View.INLINE + value);
            view.tableProperties.put(table, new TreeMap<>());
          } else {
            view.tables.put(table, value);
          }
          break;
        case UPDATE_TABLE:
          // a metadata file: the table leaves inline storage
          view.tables.put(table, value);
          view.tableProperties.remove(table);
          break;
        case DROP_TABLE:
          view.tables.remove(table);
          view.tableProperties.remove(table);
          break;
        case RENAME_TABLE:
          view.tables.put(to, view.tables.remove(table));
          Map<String, String> moved = view.tableProperties.remove(table);
          if (moved != null) {
            view.tableProperties.put(to, moved);
          }
          break;
        case SET_TABLE_PROPERTY:
          view.tableProperties.get(table).put(key, value);
          break;
        case SET_TABLE_LOCATION:
          view.tables.put(table, View.INLINE + value);
          break;
        case READ_TABLE:
          break;
        default:
          throw new IllegalStateException("Unknown op " + kind);
      }
    }

    /** Iceberg's base-metadata check: an update or read needs the table it saw. */
    boolean baseHolds(CatalogState state) {
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
        case SET_TABLE_PROPERTY:
          return String.format("setTableProperty(%s, %s=%s)", table, key, value);
        case CREATE_TABLE:
          return String.format("create_table(%s, %s%s)", table, payload != null ? "inline " : "",
              value);
        default:
          return String.format(
              "%s(%s%s)", kind.name().toLowerCase(), table, value == null ? "" : ", " + value);
      }
    }
  }

  /** The logical catalog state: namespaces with properties, and table locations. */
  static final class View {
    /** An inline table's entry in {@link #tables}: this, then its location. */
    static final String INLINE = "inline:";
    final Map<Namespace, Map<String, String>> namespaces = new TreeMap<>(NS_ORDER);
    // a table's metadata location, or INLINE and its location
    final Map<TableIdentifier, String> tables = new TreeMap<>(TABLE_ORDER);
    // an inline table's properties, of KEYS
    final Map<TableIdentifier, Map<String, String>> tableProperties = new TreeMap<>(TABLE_ORDER);

    static View of(CatalogState state) {
      View view = new View();
      for (Namespace ns : state.namespaces()) {
        view.namespaces.put(ns, new TreeMap<>(state.namespaceProperties(ns)));
      }
      for (TableIdentifier t : state.tables()) {
        org.apache.iceberg.TableMetadata inline = CatalogTestSupport.inlineMetadata(state, t);
        if (inline != null) {
          view.tables.put(t, INLINE + inline.location());
          Map<String, String> props = new TreeMap<>(inline.properties());
          props.keySet().retainAll(KEYS);
          view.tableProperties.put(t, props);
        } else {
          view.tables.put(t, state.location(t));
        }
      }
      return view;
    }

    View copy() {
      View view = new View();
      namespaces.forEach((ns, props) -> view.namespaces.put(ns, new TreeMap<>(props)));
      view.tables.putAll(tables);
      tableProperties.forEach((t, props) -> view.tableProperties.put(t, new TreeMap<>(props)));
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
          && tables.equals(((View) o).tables)
          && tableProperties.equals(((View) o).tableProperties);
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespaces, tables, tableProperties);
    }

    @Override
    public String toString() {
      return "namespaces=" + namespaces + " tables=" + tables
          + (tableProperties.isEmpty() ? "" : " table properties=" + tableProperties);
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
    Replica<CatalogState> snapshot; // null when idle
    byte[] snapshotBytes;
    List<Op> ops;

    Writer(int id) {
      this.id = id;
    }
  }

  enum Outcome {
    ACKNOWLEDGED,
    /**
     * CommitFailedException, AlreadyExistsException or NoSuchTableException (a rename whose
     * source is gone): a definite no.
     */
    REJECTED,
    /** CommitStateUnknownException: the commit may or may not have taken effect. */
    UNKNOWN,
    /** Any other exception, after an injected fault. */
    FAILED
  }

  /**
   * A commit in progress: its storage plan, the commit that may preempt it, and the history ranges
   * that preempting commits wrote. Faults and preemption apply only while it is active, that is,
   * while its own commit runs, not its checks or a preempting commit.
   */
  private static final class Context {
    final List<WriteFault> plan;
    final List<WriteFault> fired = new ArrayList<>();
    final int failingRead;
    final ReadFault readFault;
    final List<String> readsFailed = new ArrayList<>();
    final List<int[]> nested = new ArrayList<>();
    Writer pending;
    final int preemptAt;
    int preemptedAt = -1;
    int reads = 0;
    int requests = 0;
    boolean active = false;

    Context(List<WriteFault> plan, int failingRead, ReadFault readFault, Writer pending,
        int preemptAt) {
      this.plan = plan;
      this.failingRead = failingRead;
      this.readFault = readFault;
      this.pending = pending;
      this.preemptAt = preemptAt;
    }

    boolean faulted() {
      return fired.stream().anyMatch(f -> f != WriteFault.NONE) || !readsFailed.isEmpty();
    }
  }

  /** A commit whose send was delayed in flight: its copy may land after the commit ended. */
  private static final class Delayed {
    final int writer;
    final List<Op> ops;
    Outcome outcome = null; // until the commit ends
    boolean tookEffect = false;

    Delayed(int writer, List<Op> ops) {
      this.writer = writer;
      this.ops = ops;
    }
  }

  private static final class Run {
    private final MultiWriterStorage storage;
    private final RuntimeConfig config;
    private final CatalogLog log;
    private final boolean faults;
    private final boolean gc;
    private final Writer[] writers;
    private final StringBuilder trace = new StringBuilder();
    private final Deque<Context> commits = new ArrayDeque<>();
    // The commits whose sends were delayed, by the record sent; and the one running, if any.
    private final Map<ByteBuffer, Delayed> delayed = new HashMap<>();
    private Delayed running = null;
    private int nextValue = 0;

    /**
     * With {@code gc}, the retention of committed transaction ids is 0: every CAS drops every id
     * older than its own commit's. A commit whose id a preempting CAS puts at or below the
     * horizon fails, or cannot learn its outcome (Bug 64), so its outcome may be unknown without
     * a fault. With a {@code segmentLimit}, the catalog object holds at most that many writes, as
     * S3 Express and Azure Append Blobs cap them, and an append past it fails (Bug 66): the commit
     * compacts, and its outcome is never unknown for it.
     */
    Run(MultiWriterStorage storage, int writers, int maxAppendCount, long appendHeadroom,
        boolean faults, boolean gc, String encoding) {
      this.storage = storage;
      this.faults = faults;
      this.gc = gc;
      Statistics.label("storage").collect(storage.describe());
      Statistics.label("encoding").collect(encoding);
      CatalogLog seedLog = storage.log(LogCatalogProperties.runtimeConfig(Map.of()));
      // The catalog's first record sets its conflict encoding, as a catalog's creation does.
      seedLog.bootstrap(encoding);
      CatalogMutation seed = new CatalogMutation(seedLog.read());
      seed.createNamespace(Namespace.of("a"));
      seed.createNamespace(Namespace.of("b"));
      seed.createTable(TableIdentifier.of("a", "t"), "mem:///a/t/seed");
      seed.createTableInline(TableIdentifier.of("b", "t"),
          CatalogTestSupport.newInlineTable("mem:///b/t/seed"),
          CatalogTestSupport.inlineTableCodec());
      seed.commit(seedLog);
      int seedLength = storage.liveBytes().length;
      long maxAppendSize =
          appendHeadroom < 0
              ? LogCatalogProperties.DEFAULT_MAX_APPEND_SIZE
              : seedLength + appendHeadroom;
      this.config =
          LogCatalogProperties.runtimeConfig(
              maxAppendCount,
              maxAppendSize,
              gc ? 0L : LogCatalogProperties.DEFAULT_COMMITTED_RETENTION_MS,
              0, // retry throttled writes without sleeping
              0); // and lost races
      this.log = storage.log(config);
      if (storage instanceof MultiWriterStorage.Memory) {
        // A compactor a second ahead of the writers, with their retention.
        ((MultiWriterStorage.Memory) storage).compactWith(new RuntimeConfig(
            config.maxAttempts(), config.maxThrottleAttempts(), 0L, 0L,
            config.committedRetentionMs(),
            config.compaction(), config.visibilityWait(),
            () -> System.currentTimeMillis() + 1_000L));
      }
      this.writers = new Writer[writers];
      for (int i = 0; i < writers; i++) {
        this.writers[i] = new Writer(i);
      }
      trace.append(String.format(
          "log: %s, maxAppendCount=%d maxAppendSize=%d (seed %d bytes)%s%n",
          storage.describe(), maxAppendCount, maxAppendSize, seedLength,
          gc ? ", retention 0" : ""));
      Statistics.label("gc").collect(gc);
      trace.append("seed: ").append(View.of(read().state())).append('\n');
      storage.hook(new MultiWriterStorage.Hooks() {
        @Override
        public WriteFault beforeWrite(byte[] record) {
          return onWrite(record);
        }

        @Override
        public ReadFault beforeRead() {
          return onRead();
        }
      });
    }

    private WriteFault onWrite(byte[] record) {
      Context c = commits.peek();
      if (c == null || !c.active) {
        return WriteFault.NONE;
      }
      request(c);
      WriteFault fault =
          c.fired.size() < c.plan.size() ? c.plan.get(c.fired.size()) : WriteFault.NONE;
      c.fired.add(fault);
      if (fault == WriteFault.DELAYED && record != null) {
        delayed.put(ByteBuffer.wrap(record), running);
      }
      return fault;
    }

    private ReadFault onRead() {
      Context c = commits.peek();
      if (c == null || !c.active) {
        return ReadFault.NONE;
      }
      request(c);
      if (c.reads++ != c.failingRead) {
        return ReadFault.NONE;
      }
      c.readsFailed.add("read #" + c.failingRead + " " + c.readFault);
      return c.readFault;
    }

    /** Counts a storage request of {@code c}'s commit; runs the preempting commit before it. */
    private void request(Context c) {
      int request = c.requests++;
      if (c.pending == null || request != c.preemptAt) {
        return;
      }
      Writer other = c.pending;
      c.pending = null;
      c.preemptedAt = request;
      c.active = false;
      int start = storage.writes();
      Delayed was = running;
      trace.append(String.format("  preempted at request #%d by%n", request));
      commit(other, List.of(), -1, ReadFault.NONE, null, -1);
      running = was;
      c.nested.add(new int[] {start, storage.writes()});
      c.active = true;
    }

    private Writer writer(Cmd cmd) {
      return writers[cmd.writer % writers.length];
    }

    void execute(List<Cmd> schedule) {

      for (Cmd cmd : schedule) {
        Writer w = writer(cmd);
        switch (cmd.step) {
          case BEGIN:
            if (w.snapshot == null) {
              w.snapshot = read();
              w.snapshotBytes = storage.liveBytes();
              w.ops = new ArrayList<>();
              trace.append(String.format("w%d begin%n", w.id));
            }
            break;
          case OP:
            if (w.snapshot != null) {
              Op interpreted = interpret(cmd, w.snapshot.state());
              // a writer stages one update of a table per transaction
              Op op = interpreted != null && changes(interpreted) && w.ops.stream()
                  .anyMatch(o -> changes(o) && o.table.equals(interpreted.table))
                  ? null : interpreted;
              if (op != null && stages(w.snapshot.state(), append(w.ops, op))) {
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
              // The next writer with a transaction open may commit it inside this commit.
              Writer other = null;
              for (int k = 1; preemptAt(cmd.d) >= 0 && other == null && k < writers.length; k++) {
                Writer candidate = writers[(w.id + k) % writers.length];
                other = candidate.snapshot != null ? candidate : null;
              }
              commit(w, cmd, other);
            }
            break;
          case DUPLICATE:
            duplicate(cmd.a);
            break;
          case COMPACT:
            if (storage.compact()) {
              trace.append("compacted\n");
              Statistics.label("steps").collect("compact");
            }
            break;
          case DELIVER:
            deliver();
            break;
          default:
            throw new IllegalStateException("Unknown step " + cmd.step);
        }
      }
      for (Writer w : writers) {
        if (w.snapshot != null) {
          commit(w, List.of(), -1, ReadFault.NONE, null, -1);
        }
      }
      // The drain: what is still in flight lands, and is judged as it does.
      deliver();
      List<String> violations = storage.checkpointViolations();
      check(violations.isEmpty(), "checkpoints: %s", violations);
    }

    /** A copy of a record that landed is appended again: it must change nothing. */
    private void duplicate(int pick) {
      List<byte[]> landed = storage.landedRecords();
      if (landed.isEmpty()) {
        return;
      }
      Replica<CatalogState> before = read();
      if (!storage.appendCopy(landed.get(pick % landed.size()))) {
        return;
      }
      Replica<CatalogState> after = read();
      trace.append(String.format("duplicate of record #%d%n", pick % landed.size()));
      check(View.of(after.state()).equals(View.of(before.state())),
          "a copy of a record applied:%n  before: %s%n  after:  %s",
          View.of(before.state()), View.of(after.state()));
      check(Arrays.equals(after.committed().bytes(), before.committed().bytes()),
          "a copy of a record changed the committed set or its horizon");
      Statistics.label("steps").collect("duplicate");
    }

    /**
     * Lands what was delayed. A landing that takes effect is its commit's write, checked as one:
     * the commit's outcome was unknown, and nothing else of it took effect.
     */
    private void deliver() {
      for (MultiWriterStorage.Write write : storage.deliverDelayed()) {
        Delayed d = delayed.get(ByteBuffer.wrap(write.record));
        check(d != null && d.outcome != null, "a delayed record of no commit that ended landed");
        boolean effective =
            !View.of(parse(write.after)).equals(View.of(parse(write.before)));
        trace.append(String.format("delayed w%d record landed%s%n", d.writer,
            effective ? " and took effect" : ""));
        Statistics.label("delayed landing").collect(
            (effective ? "took effect" : "changed nothing") + ", outcome " + d.outcome);
        String difference = storage.difference(write);
        check(difference == null, "a checkpoint of the delayed write differs from it: %s",
            difference);
        if (effective) {
          check(d.outcome == Outcome.UNKNOWN,
              "a commit reported %s took effect when its delayed send landed", d.outcome);
          check(!d.tookEffect, "one commit changed the state twice (a delayed send landed)");
          d.tookEffect = true;
          checkIntegrity(View.of(parse(write.after)));
          checkTransition(write, d.ops);
        }
      }
    }

    /** Commits {@code w}'s transaction with the faults {@code cmd} picks, if storage is faulty. */
    private void commit(Writer w, Cmd cmd, Writer preempting) {
      if (faults) {
        commit(w, List.of(storage.firstWriteFaults().get(cmd.a % 12),
                storage.secondWriteFaults().get(cmd.b % 12)),
            failingRead(cmd.c), storage.readFault(cmd.c), preempting, preemptAt(cmd.d));
      } else {
        commit(w, List.of(), -1, ReadFault.NONE, preempting, preemptAt(cmd.d));
      }
    }

    /** Whether {@code op} updates a table: a metadata file, or a delta. */
    private static boolean changes(Op op) {
      return op.kind == OpKind.UPDATE_TABLE || op.kind == OpKind.SET_TABLE_PROPERTY
          || op.kind == OpKind.SET_TABLE_LOCATION;
    }

    /** Concrete operands for {@code cmd} from the writer's snapshot, or null if none fit. */
    private Op interpret(Cmd cmd, CatalogState snap) {
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
          return cmd.b / 2 % 2 == 1
              ? Op.inlineTable(fresh, "mem:///" + fresh + "/" + value)
              : Op.table(cmd.op, fresh, "mem:///" + fresh + "/" + value, null);
        case SET_TABLE_PROPERTY:
        case SET_TABLE_LOCATION:
          TableIdentifier inline = pick(tables, TABLES, cmd.a);
          if (!snap.isInlineTable(inline)) {
            return null; // a delta changes an inline table
          }
          Op delta = cmd.op == OpKind.SET_TABLE_PROPERTY
              ? Op.delta(cmd.op, inline, KEYS.get(cmd.b % 2), value)
              : Op.delta(cmd.op, inline, null, "mem:///" + inline + "/" + value);
          delta.read = snap.tableVersion(snap.tableId(inline));
          return delta;
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

    /**
     * Commits {@code w}'s transaction. The first writes fail as {@code plan} says, and read request
     * number {@code failingRead} of the catalog (counting from 0; -1 for none) fails as {@code
     * readFault} says. If {@code preempting} is not null, that writer commits just before storage
     * request number {@code preemptAt} of this commit.
     */
    private void commit(Writer w, List<WriteFault> plan, int failingRead, ReadFault readFault,
        Writer preempting, int preemptAt) {
      List<Op> ops = w.ops;
      byte[] liveBefore = storage.liveBytes();
      int mark = storage.writes();
      int lostAppends = storage.rejected("APPEND");
      int lostCas = storage.rejected("CAS");
      Context ctx = new Context(plan, failingRead, readFault, preempting, preemptAt);
      Delayed self = new Delayed(w.id, ops);
      running = self;
      List<WriteFault> fired = ctx.fired;
      List<String> readsFailed = ctx.readsFailed;
      commits.push(ctx);
      Outcome outcome;
      String reason = "";
      try {
        ctx.active = true;
        mut(w.snapshot, ops).commit(log);
        outcome = Outcome.ACKNOWLEDGED;
      } catch (CommitFailedException | AlreadyExistsException | NoSuchTableException e) {
        outcome = Outcome.REJECTED;
        reason = " (" + e.getClass().getSimpleName() + ")";
      } catch (CommitStateUnknownException e) {
        outcome = Outcome.UNKNOWN;
        reason = " (" + e.getMessage() + ")";
      } catch (RuntimeException e) {
        if (!ctx.faulted()) {
          throw new AssertionError(
              "commit failed without an injected fault\ntrace:\n" + trace, e); // a bug
        }
        outcome = Outcome.FAILED;
        reason = " (" + e.getClass().getSimpleName() + ": " + e.getMessage() + ")";
      } finally {
        ctx.active = false;
        commits.pop();
        running = null;
      }
      // This commit's writes: those since it started, less those of the commit that preempted it,
      // and less the GC records a compaction appended once it landed.
      List<MultiWriterStorage.Write> writes = new ArrayList<>();
      List<MultiWriterStorage.Write> history = storage.writes(mark);
      for (int i = 0; i < history.size(); i++) {
        final int at = mark + i;
        if (!history.get(i).gc() && ctx.nested.stream().noneMatch(r -> at >= r[0] && at < r[1])) {
          writes.add(history.get(i));
        }
      }
      List<MultiWriterStorage.Write> effective = new ArrayList<>();
      for (MultiWriterStorage.Write wr : writes) {
        View after = View.of(parse(wr.after));
        check(after.equals(View.of(parse(wr.before))) || effective.isEmpty(),
            "one commit changed the state twice");
        if (!after.equals(View.of(parse(wr.before)))) {
          effective.add(wr);
        }
        checkIntegrity(after);
        String difference = storage.difference(wr);
        check(difference == null, "a checkpoint of the %s write differs from it: %s",
            wr.kind, difference);
      }
      // The write the transaction's effects are checked on: the one that changed the state, else
      // (for a transaction without effects) the first.
      MultiWriterStorage.Write write =
          !effective.isEmpty() ? effective.get(0) : writes.isEmpty() ? null : writes.get(0);
      String faultsFired =
          (fired.stream().anyMatch(f -> f != WriteFault.NONE) ? " after faults " + fired : "")
              + (readsFailed.isEmpty() ? "" : " after " + readsFailed.get(0));
      String path =
          (storage.rejected("CAS") > lostCas ? "lost CAS race, " : "")
              + (storage.rejected("APPEND") > lostAppends ? "lost append race, " : "")
              + (write == null ? "no write" : write.kind + rebased(w, write));
      trace.append(String.format("w%d commit -> %s%s via %s%s%n",
          w.id, outcome, reason, path, faultsFired));
      if (ctx.preemptedAt >= 0) {
        Statistics.label("preemption").collect("at request #" + ctx.preemptedAt);
        Statistics.label("outcome when preempted").collect(outcome);
      } else if (preempting != null) {
        Statistics.label("preemption").collect("commit ended first");
      }
      // Storage that does not fail leaves no outcome unknown, races and replaced versions included,
      // unless a retention of 0 drops the ids of commits still in flight.
      check(outcome != Outcome.UNKNOWN || ctx.faulted() || gc,
          "commit outcome unknown without an injected fault%s", reason);
      if (outcome == Outcome.UNKNOWN && !ctx.faulted()) {
        Statistics.label("unknown without a fault (gc)").collect(true);
      }
      // Rejecting a commit nothing raced is no conflict: Bug 64's compaction rejected its own
      // record that way, with a retention of 0.
      boolean raced = ctx.preemptedAt >= 0 || !Arrays.equals(liveBefore, w.snapshotBytes);
      check(outcome == Outcome.ACKNOWLEDGED || raced || ctx.faulted(),
          "a commit nothing raced, on storage that did not fail, was %s%s", outcome, reason);
      Statistics.label("commit path").collect(path);
      Statistics.label("operations per transaction").collect(Math.min(ops.size(), 5));
      if (!faults) {
        check(writes.size() <= 1, "one commit made %d conditional writes", writes.size());
      }
      if (!faultsFired.isEmpty()) {
        Statistics.label("outcome after a fault").collect(outcome);
      }

      if (outcome == Outcome.ACKNOWLEDGED
          || (outcome == Outcome.UNKNOWN && !effective.isEmpty())) {
        check(write != null, "acknowledged commit changed nothing");
        checkTransition(write, ops);
        Statistics.label("outcome").collect(outcome.name().toLowerCase());
      } else if (outcome == Outcome.UNKNOWN) {
        Statistics.label("outcome").collect("unknown, did not take effect");
      } else {
        check(effective.isEmpty(),
            "commit reported %s%s but took effect: %s -> %s",
            outcome, reason,
            effective.isEmpty() ? null : View.of(parse(effective.get(0).before)),
            effective.isEmpty() ? null : View.of(parse(effective.get(0).after)));
        boolean spurious = reexecutionViolation(parse(liveBefore), ops) == null;
        Statistics.label("outcome").collect(
            (outcome == Outcome.FAILED ? "failed"
                : write == null ? "rejected by CAS conflict" : "rejected at replay")
                + (spurious ? ", spurious" : ""));
      }
      self.outcome = outcome;
      self.tookEffect = !effective.isEmpty();
      w.snapshot = null;
      w.snapshotBytes = null;
      w.ops = null;
    }

    /**
     * {@code write} took {@code ops} from the state it replaced to the state it wrote: the
     * transaction is valid on the former, re-executes serially to the latter, and changed exactly
     * its own effects.
     */
    private void checkTransition(MultiWriterStorage.Write write, List<Op> ops) {
      CatalogState pre = parse(write.before);
      View post = View.of(parse(write.after));
      String invalid = reexecutionViolation(pre, ops);
      check(invalid == null, "acknowledged transaction is invalid on the state it replaced: %s",
          invalid);
      InMemoryAtomicFileIO alone = scratch(write.before);
      CatalogLog aloneLog = CatalogTestSupport.log(alone, CATALOG, config);
      View serial = View.of(mut(aloneLog.read(), ops).commit(aloneLog).state());
      check(serial.equals(post), "serial re-execution differs:%n  serial: %s%n  actual: %s",
          serial, post);
      View expected = View.of(pre).copy();
      ops.forEach(op -> op.applyTo(expected));
      check(expected.equals(post), "effects differ:%n  expected: %s%n  actual:   %s",
          expected, post);
      for (Op op : ops) {
        if (op.read >= 0) {
          Statistics.label("an acknowledged delta landed")
              .collect(pre.tableVersion(pre.tableId(op.table)) == op.read
                  ? "on the table it read" : "on a changed table");
        }
      }
    }

    /** Why {@code ops} cannot be re-executed serially on {@code state}, or null if they can. */
    private String reexecutionViolation(CatalogState state, List<Op> ops) {
      for (Op op : ops) {
        if (!op.baseHolds(state)) {
          return op + " expects " + op.base + ", found " + state.location(op.table);
        }
      }
      return stages(state, ops) ? null : "staging " + ops + " on a fresh read fails";
    }

    private String rebased(Writer w, MultiWriterStorage.Write write) {
      return write.replace() && !Arrays.equals(write.before, w.snapshotBytes)
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

    private CatalogMutation mut(Replica<CatalogState> base, List<Op> ops) {
      CatalogMutation mut = new CatalogMutation(base);
      ops.forEach(op -> op.stage(mut));
      return mut;
    }

    /** Whether {@code ops} stage cleanly on {@code base}: the client-side validity check. */
    private boolean stages(CatalogState base, List<Op> ops) {
      try {
        mut(CatalogTestSupport.replicaOf(base), ops);
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

    private Replica<CatalogState> read() {
      return log.read();
    }

    private InMemoryAtomicFileIO scratch(byte[] bytes) {
      InMemoryAtomicFileIO scratch = new InMemoryAtomicFileIO();
      scratch.seed(CATALOG, bytes);
      return scratch;
    }

    private CatalogState parse(byte[] bytes) {
      return CatalogTestSupport.decodeFullFile(bytes);
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
