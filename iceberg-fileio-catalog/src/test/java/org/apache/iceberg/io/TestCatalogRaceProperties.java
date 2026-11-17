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
import net.jqwik.api.GenerationMode;
import net.jqwik.api.Property;
import net.jqwik.api.PropertyDefaults;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.statistics.Statistics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.InMemoryAtomicFileIO.Fault;
import org.apache.iceberg.types.Types;
import org.intentlog.CommitTrace;
import org.intentlog.azure.InMemoryAppendBlobStore;
import org.intentlog.fileio.AtomicFileLog;
import org.intentlog.memory.InMemorySharedLog;
import org.intentlog.wire.LogEnvelope;

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
 * A catalog transaction (it loads one table or two, scans them or not, and appends to the first)
 * is not serial that way either: it must fail, and leave the catalog as the others alone do, if
 * an operation that ran before its writes changed what it read -- under serializable isolation
 * the main branch of each table it scanned (a load is not a read), and in any case which table
 * it appended to (dropped, or another under its name). Otherwise it may commit or fail: an inline
 * table's append is judged where the record lands, and a serializable transaction's reads by the
 * branches it read (a read witness), so a racer's change that leaves those alone may land first
 * (errata D12); a commit must end as the serial run.
 *
 * <p>A fourth writer's operation on A's table, D's, if there is one, runs just after A's first
 * write to the catalog lands, at A's next request of its log: a read through a write reads on to
 * the log's end, so D's write may be in the catalog A's commit lands in, which A's table handle
 * then holds (errata D20, D26). D runs after A if A makes no request after a write that landed, or
 * lands none. Then A may commit once more through the handle its operation used (a table it
 * loaded or created): an append, which must keep whatever D did. The race must end as a serial run
 * with D after A, or, if D ran while A's operation did, before it: on a log of blind appends A's
 * record may land, fail its check, and land again after D's. Once D ran, no racer runs before A's
 * writes.
 *
 * <p>The operations are namespace creates (of nested namespaces too, which create their parents),
 * drops and property changes; table creates, drops, renames and property changes; and appends and
 * deletes of every row, which Iceberg retries on a conflict as it does a property change (unlike
 * a schema change). They run on a small universe of names, from a generated catalog state; each
 * writer has its own catalog mode. A result is a return value or an exception's class; the
 * catalog's state is its namespaces with their properties and its tables with theirs (a table
 * created carries its creator's name) and their live data files.
 *
 * <p>Generation is random, never exhaustive: jqwik (1.9) multiplies its parameters' exhaustive
 * counts unchecked, and this product (3e20) wraps to a negative count, below any tries, so at
 * tries of 3,072 or more (each parameter's count is below that) it would enumerate the first
 * combinations in order, only the last parameters varying (havoc round cp4-r0's race job: 200,000
 * failing namespace creates).
 */
@PropertyDefaults(generation = GenerationMode.RANDOMIZED)
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

  @Provide
  Arbitrary<String> encodings() {
    return Arbitraries.of(LocalCatalogMode.ENCODINGS);
  }

  @Property(tries = 1000)
  void aRaceEndsAsASerialRun(
      @ForAll("modes") LocalCatalogMode fileModeA,
      @ForAll("modes") LocalCatalogMode fileModeB,
      @ForAll("modes") LocalCatalogMode fileModeC,
      @ForAll("setupStorage") LocalCatalogMode setupStorage,
      @ForAll("logs") LocalCatalogMode.Log catalogLog,
      @ForAll("states") Setup setup,
      @ForAll("ops") Op a,
      @ForAll("ops") Op b,
      @ForAll("maybeOps") Op c,
      @ForAll boolean gc,
      @ForAll boolean paging,
      @ForAll("clockOffsets") List<Long> clockOffsets,
      @ForAll("encodings") String encoding,
      @ForAll InMemoryAtomicFileIO.ReadMode reads,
      @ForAll boolean recordingReads,
      @ForAll boolean tailReads,
      @ForAll boolean compactAfter,
      @ForAll("modes") LocalCatalogMode fileModeD,
      @ForAll("afterOps") Op after,
      @ForAll boolean again) {
    // The writers' modes on one catalog object, or on a shared log or Append Blobs with the same
    // table metadata storage.
    LocalCatalogMode modeA = fileModeA.on(catalogLog);
    LocalCatalogMode modeB = fileModeB.on(catalogLog);
    LocalCatalogMode modeC = fileModeC.on(catalogLog);
    LocalCatalogMode modeD = fileModeD.on(catalogLog);
    // D, if any, operates on A's table
    Op d = after.kind == null ? after : new Op(after.kind, a.x, after.y);
    // The setup's tables are pointer tables, or inline: a transaction then stages its append as a
    // delta, which the encoding judges where it lands (errata D12)
    LocalCatalogMode base = setupStorage.on(catalogLog);
    Statistics.label("setup's tables").collect(
        setupStorage.inlineML ? "inline ML" : setupStorage.inlineTM ? "inline TM" : "pointer");
    Statistics.label("log").collect(catalogLog);
    List<Op> racers = c.kind == null ? List.of(b) : List.of(b, c);
    List<LocalCatalogMode> racerModes = List.of(modeB, modeC);
    // With gc, each CAS drops every committed transaction id older than its own (Bug 64). With
    // paging, each compaction pages every block of the committed set below its own id but the
    // newest (an admission window and threshold of 0). A writer at a retention of 0 drops all it
    // would page, so with both the racers page and A drops their pages.
    Map<String, String> props = new java.util.HashMap<>();
    Map<String, String> propsA = new java.util.HashMap<>();
    if (gc) {
      propsA.put(LogCatalogProperties.COMMITTED_RETENTION_MS, "0");
      if (!paging) {
        props.put(LogCatalogProperties.COMMITTED_RETENTION_MS, "0");
      }
    }
    if (paging) {
      props.put(LogCatalogProperties.COMMITTED_ADMISSION_MS, "0");
      props.put(LogCatalogProperties.COMMITTED_PAGE_THRESHOLD, "0");
      if (!gc) {
        propsA.putAll(props);
      }
    }
    if (!tailReads) { // every writer reads the log whole (TAIL_READS.md step 5)
      props.put(LogCatalogProperties.READ_TAIL, "false");
      propsA.put(LogCatalogProperties.READ_TAIL, "false");
    }
    if (!compactAfter) { // a commit past the limits replaces the catalog (TAIL_READS.md)
      props.put(LogCatalogProperties.COMPACT_AFTER_COMMIT, "false");
      propsA.put(LogCatalogProperties.COMPACT_AFTER_COMMIT, "false");
    }
    Statistics.label("tail reads").collect(tailReads);
    Statistics.label("compaction").collect(compactAfter ? "after the commit" : "with it");
    Statistics.label("gc").collect(gc);
    Statistics.label("paging").collect(paging);
    Statistics.label("clocks off").collect(clockOffsets.stream().anyMatch(o -> o != 0));

    // The race: the k-th racer runs just before A's k-th write to the catalog.
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    // Reads bind at the open, or are pinned, live or spliced after a lookup (TAIL_READS.md); and
    // record the version they find, one GET a read with no lookup, or not (step 7)
    io.readMode(reads);
    io.setReadsRecordVersion(recordingReads);
    Statistics.label("reads").collect(reads);
    Statistics.label("reads record their version").collect(recordingReads);
    // The setup creates the catalog, with the encoding its log keeps. Its clock reads a minute
    // ago, 5 ms later at each reading: its ids are older than the race's, in blocks of their own
    // that a writer that pages seals in the race.
    long[] past = {System.currentTimeMillis() - 60_000L};
    setup.apply(base.catalog(io, WAREHOUSE, "c",
        Map.of(LogCatalogProperties.CONFLICT_ENCODING, encoding), () -> past[0] += 5));
    Statistics.label("encoding").collect(encoding);
    // In the race, each writer's clock is off as drawn: its commits may be retried, but end alike
    LogCatalog writerA = modeA.catalog(io, WAREHOUSE, "c", propsA, clock(clockOffsets.get(0)));
    List<LogCatalog> writers = List.of(
        modeB.catalog(io, WAREHOUSE, "c", props, clock(clockOffsets.get(1))),
        modeC.catalog(io, WAREHOUSE, "c", props, clock(clockOffsets.get(2))));
    LogCatalog writerD = modeD.catalog(io, WAREHOUSE, "c", props, clock(clockOffsets.get(3)));
    String[] results = new String[racers.size()];
    int[] writes = {0};
    boolean[] nested = {false};
    boolean[] changedItsReads = {false}; // whether a racer changed what A, a transaction, read
    boolean[] changedItsTable = {false}; // whether a racer changed the table A appends to at all
    List<Map<TableIdentifier, String>> loaded = new java.util.ArrayList<>(); // as A loaded them
    // On one object, whether A's write is a compaction after its commit landed, which is not one
    // of its commit's writes: A's trace says, its last commit or compaction begun
    boolean[] compacting = {false};
    // On Append Blobs, whether A's next append is a commit's send (the trace's SEND, a != 2)
    boolean[] sending = {false};
    boolean[] landedA = {false}; // whether a send of A's landed (the trace's LANDED)
    String[] resultD = {null};
    boolean[] dDuring = {false}; // whether D ran while A's operation did
    // A's trace, drained by every hook: whether its write is a commit's or a compaction's, and
    // whether one of its sends landed
    java.util.function.Consumer<List<CommitTrace.Event>> observe = events -> {
      for (CommitTrace.Event event : events) {
        switch (event.kind()) {
          case COMMIT:
            compacting[0] = false;
            break;
          case COMPACT:
            compacting[0] = true;
            sending[0] = false;
            break;
          case SEND:
            sending[0] = event.a() != 2;
            break;
          case LANDED:
            landedA[0] = true;
            break;
          default:
            break;
        }
      }
    };
    // D, once, at A's first request after a send of A's landed
    Runnable afterALanded = () -> {
      if (d.kind != null && resultD[0] == null && landedA[0]) {
        nested[0] = true;
        try {
          resultD[0] = d.run(writerD, "D");
          dDuring[0] = true;
        } finally {
          nested[0] = false;
          CommitTrace.drain(); // D's, on this thread
        }
      }
    };
    CommitTrace.open();
    Runnable beforeAWrite = () -> {
      if (dDuring[0]) {
        return; // after D, no racer: the serial runs put D after the racers before A's writes
      }
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
          CommitTrace.drain(); // the racer's, on this thread
        }
      }
    };
    InMemorySharedLog log = catalogLog == LocalCatalogMode.Log.SHARED
        ? LocalCatalogMode.sharedLog(io, CATALOG) : null;
    InMemoryAppendBlobStore blobs = catalogLog == LocalCatalogMode.Log.AZURE
        ? LocalCatalogMode.azureStore(io, CATALOG) : null;
    if (blobs != null) {
      // A's writes are its commits' sends (the trace's SEND), each just before its first append,
      // which may find its segment ended and install the next before it lands. A compaction's
      // appends (its GC record, PageOuts) are no commit's sends.
      blobs.intercept((op, name) -> {
        if (!nested[0]) {
          observe.accept(CommitTrace.drain());
          afterALanded.run();
          if (sending[0] && op == InMemoryAppendBlobStore.Op.APPEND) {
            sending[0] = false;
            beforeAWrite.run();
          }
        }
        return InMemoryAppendBlobStore.Fault.NONE;
      });
    } else if (log != null) {
      // A's writes are its intention records: the GC record of a compaction after its commit
      // landed is not one.
      log.intercept(new InMemorySharedLog.Interceptor() {
        @Override
        public InMemorySharedLog.WriteFault beforeWrite(
            InMemorySharedLog.Request request, byte[] payload) {
          if (!nested[0]) {
            observe.accept(CommitTrace.drain());
            afterALanded.run();
            if (request == InMemorySharedLog.Request.APPEND
                && LogEnvelope.decodeRecord(payload).isIntention()) {
              beforeAWrite.run();
            }
          }
          return InMemorySharedLog.WriteFault.NONE;
        }

        @Override
        public InMemorySharedLog.ReadShape beforeRead(InMemorySharedLog.Request request) {
          if (!nested[0]) {
            observe.accept(CommitTrace.drain()); // a read through a send that landed: D
            afterALanded.run();
          }
          return InMemorySharedLog.ReadShape.NORMAL;
        }
      });
    } else {
      io.injectFaults((location, strategy) -> {
        if (!nested[0] && location.equals(CATALOG)) {
          observe.accept(CommitTrace.drain());
          afterALanded.run();
          if (!compacting[0]) {
            beforeAWrite.run();
          }
        }
        return Fault.NONE;
      });
      // a read through a write that does not say what the object holds reads it: D before
      io.injectReadFaults(location -> {
        if (!nested[0] && location.equals(CATALOG)) {
          observe.accept(CommitTrace.drain());
          afterALanded.run();
        }
        return InMemoryAtomicFileIO.ReadFault.NONE;
      });
    }
    String raceA;
    Table[] handle = {null}; // the table A's operation committed through
    try {
      raceA = a.run(writerA, "A", handle);
    } finally {
      CommitTrace.close();
    }
    io.injectFaults((location, strategy) -> Fault.NONE);
    io.injectReadFaults(location -> InMemoryAtomicFileIO.ReadFault.NONE);
    if (log != null) {
      log.intercept(null);
    }
    if (blobs != null) {
      blobs.intercept(null);
    }
    if (d.kind != null && resultD[0] == null) {
      resultD[0] = d.run(writerD, "D"); // no send of A's landed, or no request followed it
    }
    String heldAfterD = dDuring[0] && handle[0] != null && raceA.startsWith("ok")
        ? holdsTheCatalogsVersion(handle[0], a.target(), io) : null;
    // A's second commit, through the handle that holds what its first landed in
    String raceAgain = again && handle[0] != null && raceA.startsWith("ok")
        ? Op.again(handle[0], "A") : null;
    int before = Math.min(writes[0], racers.size()); // racers that ran before one of A's writes
    for (int k = before; k < racers.size(); k++) {
      results[k] = racers.get(k).run(writers.get(k), name(k));
    }
    String raceState = state(io);

    // A transaction whose reads a racer changed must fail. One whose table a racer changed may
    // commit: an inline table's append is judged where the record lands, and commits where a
    // retry of it would (errata D12), which the serial run below checks; a pointer table's update
    // applies only on the version the transaction first read.
    boolean txnFailed = a.kind == Kind.CATALOG_TXN && before > 0 && !raceA.startsWith("ok");
    if (a.kind == Kind.CATALOG_TXN && before > 0) {
      Statistics.label("transaction racing")
          .collect((changedItsReads[0] ? "reads changed, " : "reads held, ")
              + (changedItsTable[0] ? "table changed, " : "table held, ")
              + (txnFailed ? "failed" : "committed"));
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
    // The serial runs the race may end as, the first that matches: D after A, or before it if D
    // ran while A's operation did; without A if it failed as it may, or, if it lost its outcome
    // to gc, with it too. A mismatch is reported against the first.
    boolean excluded = replaced || lost;
    String race = outcomes(raceA, results, d, resultD[0], raceAgain);
    List<Op> aOrNone = new java.util.ArrayList<>();
    aOrNone.add(excluded ? null : a);
    if (lost) {
      aOrNone.add(a);
    }
    String serialRun = null;
    String serialState = null;
    boolean tookEffect = false; // A lost its outcome, but the race ends as the serial run with A
    boolean dBeforeA = false;
    candidates:
    for (Op serialOp : aOrNone) {
      for (boolean dFirst : dDuring[0] ? new boolean[] {false, true} : new boolean[] {false}) {
        String[] serial = new String[racers.size()];
        String[] run = serialRun(setup, base, serialOp, modeA, racers, racerModes, before, props,
            serial, d, modeD, dFirst, raceAgain != null);
        String described = outcomes(excluded ? raceA : run[0], serial, d, run[2], run[3]);
        boolean matches = race.equals(described) && raceState.equals(run[1]);
        if (serialRun == null || matches) {
          serialRun = described;
          serialState = run[1];
          tookEffect = excluded && serialOp != null;
          dBeforeA = dFirst;
        }
        if (matches) {
          break candidates;
        }
      }
    }
    Statistics.label("A failed as it may").collect(replaced);
    Statistics.label("A lost its outcome to gc").collect(lost);

    Statistics.label("racers before A's writes")
        .collect(catalogLog + ": " + before + " of " + racers.size());
    Statistics.label("A").collect(a.kind + " " + (raceA.startsWith("ok") ? "ok" : "fails"));
    Statistics.label("D").collect(d.kind == null ? "none"
        : (dDuring[0] ? "just after A's write landed" : "after A")
            + (dBeforeA ? ", before A serially" : ""));
    Statistics.label("A's handle after D")
        .collect(heldAfterD == null ? "-" : heldAfterD);
    Statistics.label("A again")
        .collect(raceAgain == null ? "no" : raceAgain.startsWith("ok") ? "ok" : "fails");
    StringBuilder context = new StringBuilder(
        String.format("%nsetup %s%s%s, clocks off by %s ms%nA (%s): %s", setup, gc ? ", gc" : "",
            paging ? ", paging" : "", clockOffsets, modeA, a));
    for (int k = 0; k < racers.size(); k++) {
      context.append(String.format("%n%s (%s): %s, %s", name(k), racerModes.get(k),
          racers.get(k), k < before ? "just before A's write #" + k : "after A"));
    }
    if (d.kind != null) {
      context.append(String.format("%nD (%s): %s, %s", modeD, d,
          dDuring[0] ? "just after A's write landed" : "after A"));
    }
    if (raceAgain != null) {
      context.append(String.format("%nA again: an append through A's handle"));
    }
    if (tookEffect && !raceA.equals("CommitStateUnknownException")) {
      throw new AssertionError(String.format("A reported %s, but took effect%s", raceA, context));
    }
    if (!race.equals(serialRun)) {
      throw new AssertionError(String.format(
          "the race reported %s; the serial run %s%s", race, serialRun, context));
    }
    if (!raceState.equals(serialState)) {
      throw new AssertionError(String.format(
          "the race left%n  %s%nthe serial run%n  %s%s", raceState, serialState, context));
    }
    CheckpointRoundTrip.assertEveryWrite(io);
    if (log != null) {
      CheckpointRoundTrip.assertEveryCheckpoint(log);
    }
    if (blobs != null) {
      CheckpointRoundTrip.assertEveryCheckpoint(LocalCatalogMode.azureHistory(
          io, new LocalCatalogMode.AzureStore(CATALOG, blobs)), "the segmented log" + context);
    }
    Statistics.label("PageOut records").collect(pageOuts(io, log, blobs) > 0);
  }

  /** A clock off the system's by {@code offsetMs}; null (the system's) if it is not off. */
  private static java.util.function.LongSupplier clock(long offsetMs) {
    return offsetMs == 0 ? null : () -> System.currentTimeMillis() + offsetMs;
  }

  /**
   * How far each writer's clock is off, in ms: mostly not at all; a few ms or a few hundred behind
   * or ahead, so its ids land in older blocks or below a floor others raised; or far ahead of
   * every clock (errata D21).
   */
  @Provide
  Arbitrary<List<Long>> clockOffsets() {
    return Arbitraries.of(0L, 0L, 0L, 0L, -3L, -300L, 3L, 300L, 100_000L).list().ofSize(4);
  }

  /** The PageOut records the catalog's log ever held. */
  private static long pageOuts(InMemoryAtomicFileIO io, InMemorySharedLog log,
      InMemoryAppendBlobStore blobs) {
    List<byte[]> records = new java.util.ArrayList<>();
    if (log != null) {
      records.addAll(log.records());
    } else if (blobs != null) {
      LocalCatalogMode.azureHistory(io, new LocalCatalogMode.AzureStore(CATALOG, blobs)).records()
          .forEach(landed -> records.add(landed.record()));
    } else {
      for (InMemoryAtomicFileIO.Write write : io.history()) {
        if (write.location().equals(CATALOG)) {
          records.addAll(AtomicFileLog.decode(write.after()).records());
        }
      }
    }
    return records.stream().map(java.nio.ByteBuffer::wrap).distinct()
        .filter(b -> LogEnvelope.decodeRecord(b.array()).isPageOut()).count();
  }

  /**
   * A serial run on a copy of {@code setup}: the racers that ran before A's writes, then A unless
   * it is null, with D, if any, just before it ({@code dFirst}) or just after, then A's second
   * commit through its handle ({@code again}, if A's operation succeeded), then the rest. Fills
   * {@code results} with the racers' results; returns A's result, the catalog's state, D's result
   * and that of A's second commit.
   */
  private static String[] serialRun(Setup setup, LocalCatalogMode base, Op a,
      LocalCatalogMode modeA, List<Op> racers, List<LocalCatalogMode> racerModes, int before,
      Map<String, String> props, String[] results, Op d, LocalCatalogMode modeD, boolean dFirst,
      boolean again) {
    InMemoryAtomicFileIO serialIo = new InMemoryAtomicFileIO();
    setup.apply(base.catalog(serialIo, WAREHOUSE, "c", Map.of()));
    for (int k = 0; k < before; k++) {
      results[k] = racers.get(k).run(
          racerModes.get(k).catalog(serialIo, WAREHOUSE, "c", props), name(k));
    }
    String resultD = null;
    if (d.kind != null && dFirst) {
      resultD = d.run(modeD.catalog(serialIo, WAREHOUSE, "c", props), "D");
    }
    Table[] handle = {null};
    String resultA = a == null ? null
        : a.run(modeA.catalog(serialIo, WAREHOUSE, "c", props), "A", handle);
    if (d.kind != null && !dFirst) {
      resultD = d.run(modeD.catalog(serialIo, WAREHOUSE, "c", props), "D");
    }
    String resultAgain = again && handle[0] != null && resultA.startsWith("ok")
        ? Op.again(handle[0], "A") : null;
    for (int k = before; k < racers.size(); k++) {
      results[k] = racers.get(k).run(
          racerModes.get(k).catalog(serialIo, WAREHOUSE, "c", props), name(k));
    }
    return new String[] {resultA, state(serialIo), resultD, resultAgain};
  }

  /** What a run reported: A's result, the racers', D's if it ran and A's second commit's. */
  private static String outcomes(String a, String[] racers, Op d, String resultD, String again) {
    return "A=" + a + " " + java.util.Arrays.toString(racers)
        + (d.kind != null ? " D=" + resultD : "") + (again != null ? " again=" + again : "");
  }

  /**
   * Whether the table {@code handle} holds is the version the catalog holds now, after D: a
   * fresh reader's metadata location ("D's version"), or another ("an older version").
   */
  private static String holdsTheCatalogsVersion(
      Table handle, TableIdentifier id, InMemoryAtomicFileIO io) {
    LogCatalog reader =
        LocalCatalogMode.readerFor(io, CATALOG).catalog(io, WAREHOUSE, "c", Map.of());
    if (!reader.tableExists(id)) {
      return "the table is gone";
    }
    String now = ((org.apache.iceberg.HasTableOperations) reader.loadTable(id)).operations()
        .current().metadataFileLocation();
    String held = ((org.apache.iceberg.HasTableOperations) handle).operations().current()
        .metadataFileLocation();
    return java.util.Objects.equals(now, held) ? "D's version" : "an older version";
  }

  private static String name(int racer) {
    return racer == 0 ? "B" : "C";
  }

  /** What Iceberg reports for a handle to a table that was dropped and created again. */
  private static final String REPLACED = "IllegalStateException: Table UUID does not match";

  /**
   * The race above with A a catalog transaction and its racers writing the tables it appends to
   * and reads, on tables that each hold a snapshot, stored as A's mode stores them: where a
   * transaction's appends to an inline table are judged where its record lands, and commit where
   * a retry of them would (errata D12). Racer B writes A's table, and C, if any, the other table A
   * loads. It must end as a serial run, as above.
   */
  @Property(tries = 200)
  void aTransactionRaceEndsAsASerialRun(
      @ForAll("modes") LocalCatalogMode fileModeA,
      @ForAll("modes") LocalCatalogMode fileModeB,
      @ForAll("modes") LocalCatalogMode fileModeC,
      @ForAll("logs") LocalCatalogMode.Log catalogLog,
      @ForAll @IntRange(min = 0, max = 11) int x,
      @ForAll @IntRange(min = 0, max = 11) int y,
      @ForAll("tableRacers") Kind racerB,
      @ForAll("tableRacers") Kind racerC,
      @ForAll boolean twoRacers,
      @ForAll boolean gc,
      @ForAll boolean paging,
      @ForAll("clockOffsets") List<Long> clockOffsets,
      @ForAll("encodings") String encoding,
      @ForAll InMemoryAtomicFileIO.ReadMode reads,
      @ForAll boolean recordingReads,
      @ForAll boolean tailReads,
      @ForAll boolean compactAfter) {
    LocalCatalogMode storage = fileModeA.inlineML ? LocalCatalogMode.APPEND_ML
        : fileModeA.inlineTM ? LocalCatalogMode.APPEND_TM : LocalCatalogMode.APPEND;
    // every namespace and table, each table with a snapshot
    Setup setup = new Setup(7, 0, 15, 2, 15);
    Op a = new Op(Kind.CATALOG_TXN, x, y);
    Op b = new Op(racerB, x, x + y);
    Op c = twoRacers ? new Op(racerC, y, x) : new Op(null, 0, 0);
    aRaceEndsAsASerialRun(fileModeA, fileModeB, fileModeC, storage, catalogLog, setup, a, b, c,
        gc, paging, clockOffsets, encoding, reads, recordingReads, tailReads, compactAfter,
        fileModeA, new Op(null, 0, 0), false);
  }

  /** What a racer does to a table a transaction writes or reads. */
  @Provide
  Arbitrary<Kind> tableRacers() {
    return Arbitraries.of(
        Kind.FAST_APPEND, Kind.SET_TABLE_PROPERTY, Kind.DELETE_ALL, Kind.CATALOG_TXN);
  }

  /** The setup's tables: pointer tables, or inline TM or ML, on one object. */
  @Provide
  Arbitrary<LocalCatalogMode> setupStorage() {
    return Arbitraries.of(
        LocalCatalogMode.APPEND, LocalCatalogMode.APPEND_TM, LocalCatalogMode.APPEND_ML);
  }

  @Provide
  Arbitrary<LocalCatalogMode> modes() {
    return Arbitraries.of(LocalCatalogMode.fileModes());
  }

  /** The catalog's log: one object half the time, a shared log or Append Blobs a quarter each. */
  @Provide
  Arbitrary<LocalCatalogMode.Log> logs() {
    return Arbitraries.frequency(
        net.jqwik.api.Tuple.of(2, LocalCatalogMode.Log.OBJECT),
        net.jqwik.api.Tuple.of(1, LocalCatalogMode.Log.SHARED),
        net.jqwik.api.Tuple.of(1, LocalCatalogMode.Log.AZURE));
  }

  /** A catalog state: some namespaces, with properties, and some tables in them. */
  @Provide
  Arbitrary<Setup> states() {
    return Combinators.combine(
            Arbitraries.integers().between(0, 7),
            Arbitraries.integers().between(0, 7),
            Arbitraries.integers().between(0, 15),
            Arbitraries.of(2, 4, 6),
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

  /**
   * Racer D: half the time none, else an operation on A's table (its x is A's): a change through a
   * handle, a drop, a rename or a catalog transaction appending to it.
   */
  @Provide
  Arbitrary<Op> afterOps() {
    return Arbitraries.oneOf(
        Combinators.combine(
                Arbitraries.of(Kind.SET_TABLE_PROPERTY, Kind.FAST_APPEND, Kind.DELETE_ALL,
                    Kind.DROP_TABLE, Kind.RENAME_TABLE, Kind.CATALOG_TXN),
                Arbitraries.integers().between(0, 11))
            .as((kind, y) -> new Op(kind, 0, y)),
        Arbitraries.just(new Op(null, 0, 0)));
  }

  /** An operation half the time, else none (an Op of no kind). */
  @Provide
  Arbitrary<Op> maybeOps() {
    return Arbitraries.oneOf(ops(), Arbitraries.just(new Op(null, 0, 0)));
  }

  /**
   * The namespaces in bitmask {@code namespaces}, those in {@code props} with k1, and tables; after
   * {@code filler} commits that leave nothing (a namespace outside the operations' names created
   * and dropped), so that the committed set holds enough ids for blocks to seal.
   */
  static final class Setup {
    final int namespaces;
    final int props;
    final int tables;
    final int filler;
    /** The tables that hold a snapshot (a file appended) before the race. */
    final int seeded;

    Setup(int namespaces, int props, int tables, int filler, int seeded) {
      this.namespaces = namespaces;
      this.props = props;
      this.tables = tables;
      this.filler = filler;
      this.seeded = seeded;
    }

    void apply(LogCatalog catalog) {
      for (int i = 0; i < filler / 2; i++) {
        Namespace scratch = Namespace.of("filler" + i);
        catalog.createNamespace(scratch);
        catalog.dropNamespace(scratch);
      }
      for (int i = 0; i < NAMESPACES.size(); i++) {
        Namespace ns = NAMESPACES.get(i);
        if ((namespaces >> i & 1) == 1 && !catalog.namespaceExists(ns)) {
          catalog.createNamespace(ns, (props >> i & 1) == 1 ? Map.of("k1", "v0") : Map.of());
        }
      }
      for (int i = 0; i < TABLES.size(); i++) {
        TableIdentifier id = TABLES.get(i);
        if ((tables >> i & 1) == 1 && catalog.namespaceExists(id.namespace())) {
          Table table = catalog.buildTable(id, SCHEMA).withProperty("origin", "setup").create();
          // A table with a snapshot: an append to it in the race extends main's history, which
          // a merging encoding can land on a head another writer moved; a first snapshot starts
          // a history, which it cannot (Bug 73)
          if ((seeded >> i & 1) == 1) {
            table.newFastAppend()
                .appendFile(org.apache.iceberg.DataFiles.builder(table.spec())
                    .withPath("mem:///data/setup-" + i + ".parquet")
                    .withFileSizeInBytes(10)
                    .withRecordCount(1)
                    .build())
                .commit();
          }
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
     * Loads one table, or two (the second if it exists), in a catalog transaction, scans them when
     * {@code y / 4} is even, appends to the first and commits; serializable when {@code x / 4} is
     * even, else snapshot isolation.
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

    /** The table the operation loads, creates or changes. */
    TableIdentifier target() {
      return table();
    }

    private TableIdentifier other() {
      return TABLES.get(y % TABLES.size());
    }

    private String key() {
      return KEYS.get(y % KEYS.size());
    }

    boolean serializable() {
      return x / 4 % 2 == 0;
    }

    /** Whether a transaction scans the tables it loads. */
    boolean scans() {
      return y / 4 % 2 == 0;
    }

    /**
     * The tables whose isolation the transaction checks: the one it appends to, and the other it
     * scans if it is serializable.
     */
    private List<TableIdentifier> readTables() {
      return serializable() && scans() && !other().equals(table()) ? List.of(table(), other())
          : List.of(table());
    }

    /** What the transaction's isolation checks of each table it reads, as stored now. */
    Map<TableIdentifier, String> reads(InMemoryAtomicFileIO io) {
      LogCatalog reader =
          LocalCatalogMode.readerFor(io, CATALOG).catalog(io, WAREHOUSE, "c", Map.of());
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
     * Whether the tables are as the transaction read them: the same table and, under serializable
     * isolation if it scanned them, main on the same snapshot (or on none, then and now: its rows
     * are the same, none).
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
        if (!serializable() || !scans()) {
          continue; // only a serializable transaction's scans are reads (errata D12)
        }
        if (!t[1].equals(n[1])) {
          return false;
        }
      }
      return true;
    }

    /** Runs the operation as {@code who}; returns "ok" and its result, or the exception's class. */
    String run(LogCatalog catalog, String who) {
      return run(catalog, who, new Table[1]);
    }

    /**
     * As {@link #run(LogCatalog, String)}; {@code handle} gets the table the operation committed
     * through, one it loaded or created, if it did.
     */
    String run(LogCatalog catalog, String who, Table[] handle) {
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
            handle[0] = catalog.buildTable(table(), SCHEMA).withProperty("origin", who).create();
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
            handle[0] = catalog.loadTable(table());
            handle[0].updateProperties().set(key(), who).commit();
            return "ok";
          case FAST_APPEND:
            Table table = catalog.loadTable(table());
            handle[0] = table;
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
            if (scans()) {
              appended.newScan().planFiles();
            }
            if (!other().equals(table()) && catalog.tableExists(other())) {
              Table read = txCatalog.loadTable(other());
              if (scans()) {
                read.newScan().planFiles();
              }
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
            handle[0] = catalog.loadTable(table());
            handle[0].newDelete()
                .deleteFromRowFilter(org.apache.iceberg.expressions.Expressions.alwaysTrue())
                .commit();
            return "ok";
        }
      } catch (RuntimeException e) {
        return outcome(e);
      }
    }

    /** A second commit through {@code handle}: a file named for {@code who} appended. */
    static String again(Table handle, String who) {
      try {
        handle.newFastAppend()
            .appendFile(org.apache.iceberg.DataFiles.builder(handle.spec())
                .withPath("mem:///data/" + who + "-again.parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .build())
            .commit();
        return "ok";
      } catch (RuntimeException e) {
        return outcome(e);
      }
    }

    private static String outcome(RuntimeException e) {
      if (e instanceof IllegalStateException && e.getMessage() != null
          && e.getMessage().startsWith("Table UUID does not match")) {
        return REPLACED;
      }
      return e.getClass().getSimpleName();
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
          return String.format("catalogTxn(%s, %s %s%s)",
              serializable() ? "serializable" : "snapshot", scans() ? "scan and append to"
                  : "append to", table(),
              other().equals(table()) ? "" : (scans() ? ", scan " : ", load ") + other());
        default:
          return kind + "(" + table() + ")";
      }
    }
  }

  /** The catalog's namespaces with their properties, and its tables with theirs. */
  private static String state(InMemoryAtomicFileIO io) {
    LogCatalog reader =
        LocalCatalogMode.readerFor(io, CATALOG).catalog(io, WAREHOUSE, "c", Map.of());
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
