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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.statistics.Statistics;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.intentlog.Replay;
import org.intentlog.memory.InMemorySharedLog;
import org.intentlog.wire.LogEnvelope;

/**
 * The contract every conflict encoding meets ({@link ConflictEncoding}), which each encoding's
 * test extends, as a log provider's test extends {@code SharedLogContract}.
 *
 * <p>A generated schedule ({@link TableScript}) builds a table on a catalog under the encoding,
 * on a shared log. Then an operation R runs, and just before its first record is appended a
 * window of other commands commits through another catalog, so R's record lands on a table that
 * may have changed since R read it. The record is kept. After R, an extension of further commands
 * runs, which can take the table anywhere: expiry, rollbacks, schema and partition-spec changes
 * that reuse ids, eviction from inline storage and re-inlining (with a small inline update limit),
 * renames away and back, and R's own retry, whose snapshot and manifests R's record names too.
 * Each try checks that:
 *
 * <ul>
 *   <li>judging R's record where it landed, on a replay of the log up to it, is what the log's
 *       replay did with it (deterministic, S1), and so is a second judgment, with the same state
 *       after an admitted apply;
 *   <li>a record the encoding rejected where it landed is rejected again at every point of the
 *       extension, where a copy of it could land later (stable, S2);
 *   <li>a copy the encoding admits anywhere applies without throwing (total: admits implies the
 *       merge), and every replay of the log, through each observe, succeeds;
 *   <li>an action the encoding cannot see into (a pointer table's, a full or location payload, a
 *       read pin, a pinned update) is admitted only on its base version (coarsens when blind).
 * </ul>
 *
 * Whether an admitted record is what its operation would commit is {@code TestRebaseProperties}'s
 * to check.
 */
abstract class ConflictEncodingContract {

  private static final String WAREHOUSE = "mem:///warehouse";
  private static final String LOCATION = WAREHOUSE + "/catalog-c";
  private static final TableIdentifier AWAY = TableIdentifier.of("ns", "away");

  /** The name of the encoding under test. */
  abstract String encoding();

  @Property(tries = 150)
  void anEncodingIsDeterministicStableAndTotal(
      @ForAll @IntRange(min = 2, max = 3) int formatVersion,
      @ForAll boolean partitioned,
      @ForAll("modes") LocalCatalogMode mode,
      @ForAll boolean smallInlineUpdates,
      @ForAll("prefixes") List<TableScript.Cmd> prefix,
      @ForAll("windows") List<TableScript.Cmd> window,
      @ForAll("operations") TableScript.Cmd r,
      @ForAll("extensions") List<TableScript.Cmd> extension,
      @ForAll @IntRange(min = -1, max = 5) int renameAt) {
    StringBuilder trace = new StringBuilder(String.format(
        "%s v%d %s, %s%s%nprefix %s%nwindow %s%nR %s%nextension %s, rename at %d%n",
        encoding(), formatVersion, partitioned ? "partitioned" : "unpartitioned", mode,
        smallInlineUpdates ? ", small inline updates" : "", prefix, window, r, extension,
        renameAt));
    Map<String, String> properties = new HashMap<>();
    properties.put(LogCatalogProperties.CONFLICT_ENCODING, encoding());
    properties.put(
        CatalogProperties.TABLE_DEFAULT_PREFIX + TableProperties.COMMIT_MIN_RETRY_WAIT_MS, "0");
    if (smallInlineUpdates) {
      properties.put(LogCatalog.INLINE_MAX_UPDATE_SIZE, "2048");
    }
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    LogCatalog catalog = mode.catalog(io, WAREHOUSE, "c", properties);
    InMemorySharedLog log = LocalCatalogMode.sharedLog(io, LOCATION);
    catalog.createNamespace(Namespace.of("ns"));
    TableScript.Runner writer = new TableScript.Runner(catalog, formatVersion, partitioned, trace);
    for (List<TableScript.Cmd> step : TableScript.steps(prefix)) {
      writer.run(step);
    }
    LogCatalog other = mode.catalog(io, WAREHOUSE, "c", properties);
    TableScript.Runner racer = new TableScript.Runner(other, "w", trace);

    Capture capture = new Capture(log, racer, window, trace);
    log.intercept(capture);
    try {
      trace.append("R:\n");
      writer.run(List.of(r));
    } finally {
      log.intercept(new InMemorySharedLog.Interceptor() {});
    }
    if (capture.record == null) {
      Statistics.label("R's record").collect("none");
      return;
    }

    // Judged where it landed: as the log's replay did, twice alike.
    UUID id = LogEnvelope.decodeRecord(capture.record).id();
    int at = capture.position;
    Replay.Result<CatalogState.Builder> landed = replay(log, at + 1, id);
    boolean applied = landed.outcome() == Replay.Outcome.APPLIED;
    CatalogState.Builder first = replay(log, at, null).builder();
    CatalogState.Builder second = replay(log, at, null).builder();
    boolean admitted = judge(capture.record, first, trace);
    if (admitted != applied || judge(capture.record, second, trace) != admitted) {
      throw new AssertionError(String.format(
          "R's record judged %s where it landed, but the log's replay %s it%n%s",
          admitted ? "admitted" : "rejected", applied ? "applied" : "rejected", trace));
    }
    boolean stale = coarse(capture.record, first, admitted, trace);
    if (admitted) {
      intention(capture.record).apply(first);
      intention(capture.record).apply(second);
      String a = state(first);
      if (!a.equals(state(second)) || !a.equals(state(landed.builder()))) {
        throw new AssertionError("R's record applies differently on equal states\n" + trace);
      }
    }
    Statistics.label("R's record where it landed")
        .collect(admitted ? (stale ? "admitted on a changed table" : "admitted on its base")
            : "rejected");

    // Every later point a copy of it could land at.
    int judged = 0;
    for (int i = 0; i <= extension.size(); i++) {
      if (i == renameAt) {
        trace.append("rename away and back\n");
        other.renameTable(TableScript.ID, AWAY);
        other.renameTable(AWAY, TableScript.ID);
        judged += judgeCopy(log, capture.record, admitted, trace);
      }
      if (i < extension.size()) {
        racer.run(List.of(extension.get(i)));
        judged += judgeCopy(log, capture.record, admitted, trace);
      }
    }
    Statistics.label("copies judged").collect(judged);
    CheckpointRoundTrip.assertEveryCheckpoint(log);
  }

  /**
   * Judges a copy of {@code record} landing at the tail of {@code log}: rejected if the record
   * was ({@code admitted} false), and if admitted, applied without throwing.
   */
  private static int judgeCopy(
      InMemorySharedLog log, byte[] record, boolean admitted, StringBuilder trace) {
    CatalogState.Builder builder = replay(log, log.records().size(), null).builder();
    boolean again = judge(record, builder, trace);
    if (again && !admitted) {
      throw new AssertionError(
          "R's record, rejected where it landed, is admitted later (S2)\n" + trace);
    }
    if (again) {
      try {
        intention(record).apply(builder);
      } catch (RuntimeException e) {
        throw new AssertionError("a copy of R's record is admitted but does not apply\n" + trace,
            e);
      }
    }
    return 1;
  }

  /** Whether {@code record}'s intention verifies on {@code builder}, which it does not change. */
  private static boolean judge(byte[] record, CatalogState.Builder builder, StringBuilder trace) {
    try {
      return intention(record).verify(builder);
    } catch (RuntimeException e) {
      throw new AssertionError("judging R's record throws\n" + trace, e);
    }
  }

  /**
   * Checks that each table action of {@code record} it cannot see into was admitted only on its
   * base version; returns whether any action was admitted on a version other than its base.
   */
  private static boolean coarse(
      byte[] record, CatalogState.Builder landing, boolean admitted, StringBuilder trace) {
    boolean stale = false;
    for (ProtoCodec.Action action : intention(record).actions()) {
      TableAction table = tableAction(action);
      if (table == null || !admitted) {
        continue;
      }
      boolean moved = landing.tableVersion(table.tableId()) != table.baseVersion();
      if (moved && (table.coarse() || table.pinned())) {
        throw new AssertionError(table + " is admitted on version "
            + landing.tableVersion(table.tableId()) + ", but the encoding cannot see into it\n"
            + trace);
      }
      stale |= moved;
    }
    return stale;
  }

  private static TableAction tableAction(ProtoCodec.Action action) {
    if (action instanceof ProtoCodec.UpdateTableInlineAction) {
      return ((ProtoCodec.UpdateTableInlineAction) action).table();
    } else if (action instanceof ProtoCodec.UpdateTableLocationAction) {
      return ((ProtoCodec.UpdateTableLocationAction) action).table();
    } else if (action instanceof ProtoCodec.ReadTableAction) {
      ProtoCodec.ReadTableAction read = (ProtoCodec.ReadTableAction) action;
      return TableAction.of(TableAction.Kind.READ, read.id, read.version);
    }
    return null;
  }

  /** A fresh decode of {@code record}'s intention. */
  private static ProtoCodec.CatalogIntention intention(byte[] record) {
    return ProtoCodec.decodeIntention(LogEnvelope.decodeRecord(record).intention());
  }

  /** The replay of {@code log}'s first {@code records} records, watching {@code watch}. */
  private static Replay.Result<CatalogState.Builder> replay(
      InMemorySharedLog log, int records, UUID watch) {
    return Replay.replay(CatalogStateMachine.INSTANCE, log.checkpoints().get(0L),
        log.records().subList(0, records), watch);
  }

  private static String state(CatalogState.Builder builder) {
    return Arrays.toString(CatalogStateMachine.INSTANCE.encodeCheckpoint(builder.build()));
  }

  /**
   * Keeps R's first record, and runs the window just before it is appended; then notes where it
   * landed.
   */
  private static final class Capture implements InMemorySharedLog.Interceptor {
    private final InMemorySharedLog log;
    private final TableScript.Runner racer;
    private final List<TableScript.Cmd> window;
    private final StringBuilder trace;
    private boolean inWindow = false;
    private boolean landing = false;
    byte[] record;
    int position = -1;

    Capture(InMemorySharedLog log, TableScript.Runner racer, List<TableScript.Cmd> window,
        StringBuilder trace) {
      this.log = log;
      this.racer = racer;
      this.window = window;
      this.trace = trace;
    }

    @Override
    public InMemorySharedLog.WriteFault beforeWrite(
        InMemorySharedLog.Request request, byte[] payload) {
      if (request == InMemorySharedLog.Request.APPEND && record == null && !inWindow) {
        record = payload;
        inWindow = true;
        trace.append("window, just before R's first record:\n");
        List<String> results = new ArrayList<>();
        try {
          for (List<TableScript.Cmd> step : TableScript.steps(window)) {
            results.add(racer.run(step) == null ? "ok" : "rejected");
          }
        } finally {
          inWindow = false;
        }
        trace.append("window ").append(results).append('\n');
        landing = true;
      }
      return InMemorySharedLog.WriteFault.NONE;
    }

    @Override
    public void afterLanding(InMemorySharedLog.Request request) {
      if (landing && request == InMemorySharedLog.Request.APPEND) {
        landing = false;
        position = log.records().size() - 1; // R's record's index
      }
    }
  }

  @Provide
  Arbitrary<LocalCatalogMode> modes() {
    return Arbitraries.of(LocalCatalogMode.sharedLogModes());
  }

  @Provide
  Arbitrary<List<TableScript.Cmd>> prefixes() {
    return TableScript.commands(kind -> kind != TableScript.Kind.STALE).withoutEdgeCases()
        .list().ofMinSize(1).ofMaxSize(10);
  }

  @Provide
  Arbitrary<List<TableScript.Cmd>> windows() {
    return TableScript.commands(ConflictEncodingContract::single).withoutEdgeCases()
        .list().ofMinSize(0).ofMaxSize(3);
  }

  @Provide
  Arbitrary<TableScript.Cmd> operations() {
    return TableScript.commands(ConflictEncodingContract::single).withoutEdgeCases();
  }

  @Provide
  Arbitrary<List<TableScript.Cmd>> extensions() {
    return TableScript.commands(ConflictEncodingContract::single).withoutEdgeCases()
        .list().ofMinSize(1).ofMaxSize(6);
  }

  /** A kind that runs as one command, on a handle of its own. */
  private static boolean single(TableScript.Kind kind) {
    return kind != TableScript.Kind.TRANSACTION && kind != TableScript.Kind.CATALOG_TXN
        && kind != TableScript.Kind.STALE;
  }
}
