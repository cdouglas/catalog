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
    compare(pointerIo, inlineIo, trace, clockStepped(clock, trace));
    for (List<TableScript.Cmd> step : TableScript.steps(schedule)) {
      RuntimeException pointerRejected = p.run(step);
      clock.stepped();
      RuntimeException inlineRejected = i.run(step);
      if ((pointerRejected == null) != (inlineRejected == null)) {
        throw new AssertionError(String.format(
            "the step %s in the pointer catalog and %s in the inline one%ntrace:%n%s",
            pointerRejected == null ? "committed" : "was rejected (" + pointerRejected + ")",
            inlineRejected == null ? "committed" : "was rejected (" + inlineRejected + ")",
            trace));
      }
      Statistics.label("step")
          .collect(step.get(0).kind + (pointerRejected == null ? "" : " rejected"));
      compare(pointerIo, inlineIo, trace, clockStepped(clock, trace));
    }
    Statistics.label("wall clock")
        .collect(clock.stepped() ? "stepped: clock-ordered columns not compared" : "steady");
    // Every catalog file either catalog wrote conforms to catalog.proto and survives a checkpoint.
    CheckpointRoundTrip.assertEveryWrite(pointerIo);
    CheckpointRoundTrip.assertEveryWrite(inlineIo);
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

  private static final String CLOCK_STEPPED =
      "(the wall clock stepped: clock-ordered metadata columns are not compared from here on)";

  /**
   * Reads the tables from each storage with a fresh pointer-mode reader and compares, all but the
   * clock-ordered metadata columns if the wall clock has stepped.
   */
  private static void compare(
      InMemoryAtomicFileIO pointerIo, InMemoryAtomicFileIO inlineIo, StringBuilder trace,
      boolean clockStepped) {
    LogCatalog pointerReader =
        LocalCatalogMode.APPEND.catalog(pointerIo, WAREHOUSE, "c", Map.of());
    LogCatalog inlineReader =
        LocalCatalogMode.APPEND.catalog(inlineIo, WAREHOUSE, "c", Map.of());
    Map<String, String> expected = TableViews.physical(pointerReader.loadTable(TableScript.ID), clockStepped);
    Table inlineTable = inlineReader.loadTable(TableScript.ID);
    Map<String, String> actual = TableViews.physical(inlineTable, clockStepped);
    TableViews.physical(pointerReader.loadTable(TableScript.OTHER), clockStepped)
        .forEach((key, value) -> expected.put(TableScript.OTHER.name() + ": " + key, value));
    TableViews.physical(inlineReader.loadTable(TableScript.OTHER), clockStepped)
        .forEach((key, value) -> actual.put(TableScript.OTHER.name() + ": " + key, value));
    List<String> differences = new ArrayList<>();
    java.util.Set<String> keys = new java.util.TreeSet<>(expected.keySet());
    keys.addAll(actual.keySet());
    for (String key : keys) {
      if (!java.util.Objects.equals(expected.get(key), actual.get(key))) {
        differences.add(String.format("%s:%n    pointer: %s%n    inline:  %s",
            key, expected.get(key), actual.get(key)));
      }
    }
    String stored =
        ((HasTableOperations) inlineTable).operations().current().metadataFileLocation();
    Statistics.label("inline table stored")
        .collect(stored != null && stored.startsWith("inline://") ? "inline" : "metadata file");
    if (!differences.isEmpty()) {
      throw new AssertionError("the inline catalog reads differently:\n  "
          + String.join("\n  ", differences.subList(0, Math.min(4, differences.size())))
          + "\ntrace:\n" + trace);
    }
  }
}
