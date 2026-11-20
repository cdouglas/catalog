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
import java.util.List;
import java.util.Map;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.statistics.Statistics;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;

/**
 * An inline catalog reads like a pointer catalog. A generated schedule ({@link TableScript}) runs
 * step by step on a table in a catalog that keeps table metadata in metadata files and manifest
 * lists in Avro files, and on one in a catalog in an inline mode (inline metadata, with inline
 * manifest lists or not; appends, CAS or compaction; a small inline update limit that moves the
 * table out of inline storage and back; either inline codec). STALE commands run on a handle from a
 * second catalog on the same storage: on the inline side, in any mode, pointer included, with
 * either codec. No faults: each step must commit in both or neither, and after each a fresh reader
 * must see the same table in both: its format version, sequence numbers and next row id, schemas,
 * specs, sort orders and properties, its snapshots (sequence number, parent, operation, summary,
 * schema, first row id and added rows) with their manifest lists entry by entry, its refs and
 * snapshot log, and the live data and delete files of every ref with their partitions, sequence
 * numbers and row ids. Snapshot ids, UUIDs, timestamps and file names of manifests and metadata
 * differ between the two runs, so snapshots are compared by position and manifests by content.
 * Once the wall clock steps during a try, the metadata-table columns Iceberg derives from the
 * order of timestamps ({@link ClockSteps#CLOCK_ORDERED}) are not compared for the rest of it.
 *
 * <p>A paged catalog reads like one that never pages, the same way ({@link
 * #aPagedCatalogReadsLikeOneThatNeverPages}).
 */
class TestInlineDifferentialProperties {

  private static final String WAREHOUSE = "mem:///warehouse";

  @Property(tries = 200)
  void inlineCatalogReadsLikeAPointerCatalog(
      @ForAll @IntRange(min = 2, max = 3) int formatVersion,
      @ForAll boolean partitioned,
      @ForAll("inlineModes") LocalCatalogMode mode,
      @ForAll("maxUpdateSizes") int maxUpdateSize,
      @ForAll boolean gzip,
      @ForAll("otherModes") LocalCatalogMode otherMode,
      @ForAll boolean otherGzip,
      @ForAll("schedules") List<TableScript.Cmd> schedule) {
    Map<String, String> inlineProperties = properties(maxUpdateSize, gzip);
    ClockSteps clock = new ClockSteps();
    InMemoryAtomicFileIO pointerIo = new InMemoryAtomicFileIO();
    InMemoryAtomicFileIO inlineIo = new InMemoryAtomicFileIO();
    LogCatalog pointer = LocalCatalogMode.APPEND.catalog(pointerIo, WAREHOUSE, "c", Map.of());
    LogCatalog inline = mode.catalog(inlineIo, WAREHOUSE, "c", inlineProperties);
    pointer.createNamespace(Namespace.of("ns"));
    inline.createNamespace(Namespace.of("ns"));
    StringBuilder trace =
        new StringBuilder(String.format(
            "v%d %s, %s, max update size %d, %s codec; stale handles from %s, %s codec%n",
            formatVersion, partitioned ? "partitioned" : "unpartitioned", mode, maxUpdateSize,
            gzip ? "gzip" : "structural", otherMode, otherGzip ? "gzip" : "structural"));
    TableScript.Runner p =
        new TableScript.Runner(pointer,
            LocalCatalogMode.APPEND.catalog(pointerIo, WAREHOUSE, "c", Map.of()),
            formatVersion, partitioned, new StringBuilder());
    TableScript.Runner i =
        new TableScript.Runner(inline,
            otherMode.catalog(inlineIo, WAREHOUSE, "c", properties(maxUpdateSize, otherGzip)),
            formatVersion, partitioned, trace);
    compare(pointerIo, inlineIo, POINTER, trace, clockStepped(clock, trace));
    for (List<TableScript.Cmd> step : TableScript.steps(schedule)) {
      RuntimeException pointerRejected = p.run(step);
      clock.stepped();
      RuntimeException inlineRejected = i.run(step);
      if ((pointerRejected == null) != (inlineRejected == null)
          || (pointerRejected != null && pointerRejected.getClass() != inlineRejected.getClass())) {
        throw new AssertionError(String.format(
            "the step %s in the pointer catalog and %s in the inline one%ntrace:%n%s",
            pointerRejected == null ? "committed" : "was rejected (" + pointerRejected + ")",
            inlineRejected == null ? "committed" : "was rejected (" + inlineRejected + ")",
            trace));
      }
      Statistics.label("step")
          .collect(step.get(0).kind + (pointerRejected == null ? "" : " rejected"));
      compare(pointerIo, inlineIo, POINTER, trace, clockStepped(clock, trace));
    }
    Statistics.label("wall clock")
        .collect(clock.stepped() ? "stepped: clock-ordered columns not compared" : "steady");
    // Every catalog file either catalog wrote conforms to catalog.proto and survives a checkpoint.
    CheckpointRoundTrip.assertEveryWrite(pointerIo);
    CheckpointRoundTrip.assertEveryWrite(inlineIo);
  }

  /** The arms of {@link #inlineCatalogReadsLikeAPointerCatalog}, as {@link #compare} names them. */
  private static final String[] POINTER = {"pointer", "inline"};

  /** The arms of {@link #aPagedCatalogReadsLikeOneThatNeverPages}. */
  private static final String[] PAGED = {"unpaged", "paged"};

  /**
   * The snapshot paging the paged arm draws, block and window: every snapshot behind the newest
   * sealed at once (1, 0), and blocks that seal one, two or three commits behind.
   */
  private static final int[][] PAGINGS = {{1, 0}, {1, 1}, {2, 1}, {3, 2}};

  /**
   * A paged catalog reads like a catalog that never pages (SNAPSHOT_PAGING.md, "Testing"). A
   * generated schedule runs on two catalogs alike but for snapshot paging, in an inline mode with
   * its stale handles from a second catalog as above: one pages at a drawn block and window,
   * whatever the run's {@code fileio.catalog.test.snapshot.block}, the reference never does. Each
   * step must end alike in both, and the tables read the same after it ({@link #compare}, the
   * paged table also as a worker reads it, from its pages). The reference's replay checks each
   * delta against the builder, which replay skips on a table with sealed blocks. Iceberg's
   * producers refresh before they commit, so a record that needs a sealed snapshot it lacks is
   * rare here; {@link TestRebaseProperties} compares the admission of one that lands behind a
   * window, paged and not.
   */
  @Property(tries = 200)
  void aPagedCatalogReadsLikeOneThatNeverPages(
      @ForAll @IntRange(min = 2, max = 3) int formatVersion,
      @ForAll boolean partitioned,
      @ForAll("inlineModes") LocalCatalogMode mode,
      @ForAll("maxUpdateSizes") int maxUpdateSize,
      @ForAll boolean gzip,
      @ForAll("otherModes") LocalCatalogMode otherMode,
      @ForAll boolean otherGzip,
      @ForAll @IntRange(min = 0, max = 3) int pagingIndex,
      @ForAll("schedules") List<TableScript.Cmd> schedule) {
    int block = PAGINGS[pagingIndex][0];
    int window = PAGINGS[pagingIndex][1];
    ClockSteps clock = new ClockSteps();
    InMemoryAtomicFileIO referenceIo = new InMemoryAtomicFileIO();
    InMemoryAtomicFileIO pagedIo = new InMemoryAtomicFileIO();
    LogCatalog reference = mode.catalog(
        referenceIo, WAREHOUSE, "c", paging(properties(maxUpdateSize, gzip), 0, 0));
    LogCatalog paged = mode.catalog(
        pagedIo, WAREHOUSE, "c", paging(properties(maxUpdateSize, gzip), block, window));
    reference.createNamespace(Namespace.of("ns"));
    paged.createNamespace(Namespace.of("ns"));
    StringBuilder trace =
        new StringBuilder(String.format(
            "v%d %s, %s, max update size %d, %s codec, paged at block %d, window %d; stale handles"
                + " from %s, %s codec%n",
            formatVersion, partitioned ? "partitioned" : "unpartitioned", mode, maxUpdateSize,
            gzip ? "gzip" : "structural", block, window, otherMode,
            otherGzip ? "gzip" : "structural"));
    LogCatalog referenceOther = otherMode.catalog(
        referenceIo, WAREHOUSE, "c", paging(properties(maxUpdateSize, otherGzip), 0, 0));
    LogCatalog pagedOther = otherMode.catalog(
        pagedIo, WAREHOUSE, "c", paging(properties(maxUpdateSize, otherGzip), block, window));
    TableScript.Runner r = new TableScript.Runner(
        reference, referenceOther, formatVersion, partitioned, new StringBuilder());
    TableScript.Runner p =
        new TableScript.Runner(paged, pagedOther, formatVersion, partitioned, trace);
    compare(referenceIo, pagedIo, PAGED, trace, clockStepped(clock, trace));
    int sealed = 0;
    for (List<TableScript.Cmd> step : TableScript.steps(schedule)) {
      RuntimeException referenceRejected = r.run(step);
      clock.stepped();
      RuntimeException pagedRejected = p.run(step);
      if ((referenceRejected == null) != (pagedRejected == null)
          || (referenceRejected != null
              && referenceRejected.getClass() != pagedRejected.getClass())) {
        throw new AssertionError(String.format(
            "the step %s in the unpaged catalog and %s in the paged one%ntrace:%n%s",
            referenceRejected == null ? "committed" : "was rejected (" + referenceRejected + ")",
            pagedRejected == null ? "committed" : "was rejected (" + pagedRejected + ")",
            trace));
      }
      sealed = Math.max(sealed, sealedBlocks(pagedIo));
      compare(referenceIo, pagedIo, PAGED, trace, clockStepped(clock, trace));
    }
    Statistics.label("paging").collect("block " + block + ", window " + window);
    Statistics.label("most sealed blocks of a table").collect(Math.min(sealed, 4));
    CheckpointRoundTrip.assertEveryWrite(referenceIo);
    CheckpointRoundTrip.assertEveryWrite(pagedIo);
  }

  private static Map<String, String> paging(Map<String, String> properties, int block, int window) {
    Map<String, String> paged = new java.util.HashMap<>(properties);
    paged.put(LogCatalogProperties.SNAPSHOT_BLOCK, String.valueOf(block));
    paged.put(LogCatalogProperties.SNAPSHOT_WINDOW, String.valueOf(window));
    return paged;
  }

  /** The most blocks the newest catalog in {@code io} seals of one of the script's tables. */
  private static int sealedBlocks(InMemoryAtomicFileIO io) {
    CatalogState state =
        LocalCatalogMode.APPEND.catalog(io, WAREHOUSE, "c", Map.of()).log().read().state();
    int most = 0;
    for (org.apache.iceberg.catalog.TableIdentifier id :
        List.of(TableScript.ID, TableScript.OTHER)) {
      if (state.isInlineTable(id)) {
        most = Math.max(most, state.sealedBlocks(state.tableId(id)).size());
      }
    }
    return most;
  }

  @Provide
  Arbitrary<LocalCatalogMode> inlineModes() {
    return Arbitraries.of(LocalCatalogMode.fileModes()).filter(m -> m.inlineTM);
  }

  @Provide
  Arbitrary<LocalCatalogMode> otherModes() {
    return Arbitraries.of(LocalCatalogMode.fileModes());
  }

  private static Map<String, String> properties(int maxUpdateSize, boolean gzip) {
    return Map.of(
        FileIOCatalog.INLINE_MAX_UPDATE_SIZE, String.valueOf(maxUpdateSize),
        FileIOCatalog.INLINE_TM_CODEC, gzip ? "gzip" : "structural");
  }

  @Provide
  Arbitrary<Integer> maxUpdateSizes() {
    return Arbitraries.of(2048, 8192, InlineDeltaCodec.APPEND_LIMIT);
  }

  @Provide
  Arbitrary<List<TableScript.Cmd>> schedules() {
    return TableScript.schedules();
  }

  /** Whether the wall clock has stepped in this try; notes the first step in the trace. */
  private static boolean clockStepped(ClockSteps clock, StringBuilder trace) {
    boolean noted = trace.indexOf(CLOCK_STEPPED) >= 0;
    boolean stepped = clock.stepped();
    if (stepped && !noted) {
      trace.append(CLOCK_STEPPED).append('\n');
    }
    return stepped;
  }

  private static Table roundTripJava(Table table) {
    try {
      return org.apache.iceberg.TestHelpers.roundTripSerialize(table);
    } catch (java.io.IOException | ClassNotFoundException e) {
      throw new AssertionError("the table does not serialize", e);
    }
  }

  private static final String CLOCK_STEPPED =
      "(the wall clock stepped: clock-ordered metadata columns are not compared from here on)";

  /**
   * Reads the tables from each storage with a fresh pointer-mode reader and compares, all but the
   * clock-ordered metadata columns if the wall clock has stepped; and the second arm's table
   * with what a worker reads of it, sent by SerializableTable (by Java serialization and by Kryo).
   * {@code arms} names the two in a difference.
   */
  private static void compare(
      InMemoryAtomicFileIO pointerIo, InMemoryAtomicFileIO inlineIo, String[] arms,
      StringBuilder trace, boolean clockStepped) {
    LogCatalog pointerReader =
        LocalCatalogMode.APPEND.catalog(pointerIo, WAREHOUSE, "c", Map.of());
    LogCatalog inlineReader =
        LocalCatalogMode.APPEND.catalog(inlineIo, WAREHOUSE, "c", Map.of());
    Map<String, String> expected = TableViews.physical(pointerReader.loadTable(TableScript.ID), clockStepped);
    Table inlineTable = inlineReader.loadTable(TableScript.ID);
    Map<String, String> actual = TableViews.physical(inlineTable, clockStepped);
    // A worker sent the table (SerializableTable, as Spark and Flink send it) reads it as the table
    List<String> differences = new ArrayList<>();
    for (String serializer : List.of("java", "kryo")) {
      Table copy = org.apache.iceberg.SerializableTable.copyOf(inlineTable);
      Table shipped = serializer.equals("java") ? roundTripJava(copy)
          : TestSerializableInlineTable.kryoRoundTrip(copy);
      Map<String, String> view = TableViews.physical(shipped, clockStepped);
      java.util.Set<String> items = new java.util.TreeSet<>(actual.keySet());
      items.addAll(view.keySet());
      for (String item : items) {
        if (!java.util.Objects.equals(actual.get(item), view.get(item))) {
          differences.add(String.format("%s (shipped by %s):%n    table:   %s%n    shipped: %s",
              item, serializer, actual.get(item), view.get(item)));
        }
      }
    }
    TableViews.physical(pointerReader.loadTable(TableScript.OTHER), clockStepped)
        .forEach((key, value) -> expected.put(TableScript.OTHER.name() + ": " + key, value));
    TableViews.physical(inlineReader.loadTable(TableScript.OTHER), clockStepped)
        .forEach((key, value) -> actual.put(TableScript.OTHER.name() + ": " + key, value));
    java.util.Set<String> keys = new java.util.TreeSet<>(expected.keySet());
    keys.addAll(actual.keySet());
    for (String key : keys) {
      if (!java.util.Objects.equals(expected.get(key), actual.get(key))) {
        differences.add(String.format("%s:%n    %-8s %s%n    %-8s %s",
            key, arms[0] + ":", expected.get(key), arms[1] + ":", actual.get(key)));
      }
    }
    String stored =
        ((HasTableOperations) inlineTable).operations().current().metadataFileLocation();
    Statistics.label("inline table stored")
        .collect(stored != null && stored.startsWith("inline://") ? "inline" : "metadata file");
    if (!differences.isEmpty()) {
      throw new AssertionError("the " + arms[1] + " catalog reads differently:\n  "
          + String.join("\n  ", differences.subList(0, Math.min(4, differences.size())))
          + "\ntrace:\n" + trace);
    }
  }
}
