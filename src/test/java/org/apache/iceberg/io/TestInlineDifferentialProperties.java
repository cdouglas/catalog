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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.statistics.Statistics;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
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
    InMemoryAtomicFileIO pointerIo = new InMemoryAtomicFileIO();
    InMemoryAtomicFileIO inlineIo = new InMemoryAtomicFileIO();
    FileIOCatalog pointer = LocalCatalogMode.APPEND.catalog(pointerIo, WAREHOUSE, "c", Map.of());
    FileIOCatalog inline = mode.catalog(inlineIo, WAREHOUSE, "c", inlineProperties);
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
    compare(pointerIo, inlineIo, trace);
    for (List<TableScript.Cmd> step : TableScript.steps(schedule)) {
      RuntimeException pointerRejected = p.run(step);
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
      compare(pointerIo, inlineIo, trace);
    }
  }

  @Provide
  Arbitrary<LocalCatalogMode> inlineModes() {
    return Arbitraries.of(LocalCatalogMode.values()).filter(m -> m.inlineTM);
  }

  @Provide
  Arbitrary<LocalCatalogMode> otherModes() {
    return Arbitraries.of(LocalCatalogMode.values());
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

  /** Reads the table from each storage with a fresh pointer-mode reader and compares. */
  private static void compare(
      InMemoryAtomicFileIO pointerIo, InMemoryAtomicFileIO inlineIo, StringBuilder trace) {
    Map<String, String> expected =
        view(LocalCatalogMode.APPEND.catalog(pointerIo, WAREHOUSE, "c", Map.of())
            .loadTable(TableScript.ID));
    Table inlineTable =
        LocalCatalogMode.APPEND.catalog(inlineIo, WAREHOUSE, "c", Map.of())
            .loadTable(TableScript.ID);
    Map<String, String> actual = view(inlineTable);
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

  /** What a reader sees of {@code table}, by item, with snapshots by position. */
  private static Map<String, String> view(Table table) {
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    Map<String, String> items = new TreeMap<>();
    List<Snapshot> snapshots = metadata.snapshots();
    Map<Long, Integer> position = new HashMap<>();
    for (int i = 0; i < snapshots.size(); i++) {
      position.put(snapshots.get(i).snapshotId(), i);
    }
    items.put("format", metadata.formatVersion() + " last-seq=" + metadata.lastSequenceNumber()
        + " next-row-id=" + metadata.nextRowId() + " location=" + metadata.location());
    items.put("schemas", metadata.currentSchemaId() + " " + metadata.schemas().stream()
        .map(SchemaParser::toJson).collect(java.util.stream.Collectors.toList()));
    items.put("specs", metadata.defaultSpecId() + " " + metadata.specs().stream()
        .map(PartitionSpecParser::toJson).collect(java.util.stream.Collectors.toList()));
    items.put("sort orders", metadata.defaultSortOrderId() + " " + metadata.sortOrders().stream()
        .map(SortOrderParser::toJson).collect(java.util.stream.Collectors.toList()));
    items.put("properties", new TreeMap<>(metadata.properties()).toString());
    for (int i = 0; i < snapshots.size(); i++) {
      Snapshot s = snapshots.get(i);
      Map<String, String> summary = new TreeMap<>(s.summary());
      if (summary.containsKey("source-snapshot-id")) {
        summary.put("source-snapshot-id",
            "#" + position.get(Long.parseLong(summary.get("source-snapshot-id"))));
      }
      items.put(String.format("snapshot %02d", i), String.format(
          "seq=%d parent=%s op=%s schema=%s first-row-id=%s added-rows=%s summary=%s",
          s.sequenceNumber(), s.parentId() == null ? null : "#" + position.get(s.parentId()),
          s.operation(), s.schemaId(), s.firstRowId(), s.addedRows(), summary));
      List<String> manifests = new ArrayList<>();
      for (ManifestFile m : s.allManifests(table.io())) {
        manifests.add(String.format(
            "%s spec=%d seq=%d/%d by=%s files=%s/%s/%s rows=%s/%s/%s first-row-id=%s parts=%s",
            m.content(), m.partitionSpecId(), m.sequenceNumber(), m.minSequenceNumber(),
            m.snapshotId() == null ? null : "#" + position.get(m.snapshotId()),
            m.addedFilesCount(), m.existingFilesCount(), m.deletedFilesCount(),
            m.addedRowsCount(), m.existingRowsCount(), m.deletedRowsCount(), m.firstRowId(),
            partitions(m)));
      }
      items.put(String.format("snapshot %02d manifests", i), manifests.toString());
    }
    for (Map.Entry<String, SnapshotRef> r : metadata.refs().entrySet()) {
      SnapshotRef ref = r.getValue();
      items.put("ref " + r.getKey(), String.format("#%s %s %s/%s/%s",
          position.get(ref.snapshotId()), ref.isBranch() ? "branch" : "tag",
          ref.minSnapshotsToKeep(), ref.maxSnapshotAgeMs(), ref.maxRefAgeMs()));
      items.put("ref " + r.getKey() + " files", files(table, r.getKey()));
    }
    items.put("snapshot log", metadata.snapshotLog().stream()
        .map(e -> "#" + position.get(e.snapshotId()))
        .collect(java.util.stream.Collectors.toList()).toString());
    return items;
  }

  private static String partitions(ManifestFile m) {
    if (m.partitions() == null) {
      return "null";
    }
    List<String> out = new ArrayList<>();
    for (ManifestFile.PartitionFieldSummary p : m.partitions()) {
      out.add(p.containsNull() + "/" + p.containsNaN() + "/" + hex(p.lowerBound()) + "/"
          + hex(p.upperBound()));
    }
    return out.toString();
  }

  /** The live data files of {@code ref}, each with its delete files, sorted by path. */
  private static String files(Table table, String ref) {
    List<String> out = new ArrayList<>();
    try (org.apache.iceberg.io.CloseableIterable<FileScanTask> tasks =
        table.newScan().useRef(ref).planFiles()) {
      for (FileScanTask task : tasks) {
        List<String> deletes = new ArrayList<>();
        for (DeleteFile d : task.deletes()) {
          deletes.add(describe(d) + " ref=" + d.referencedDataFile());
        }
        deletes.sort(String::compareTo);
        out.add(describe(task.file()) + " first-row-id=" + task.file().firstRowId()
            + " deletes=" + deletes);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    out.sort(String::compareTo);
    return out.toString();
  }

  private static String describe(ContentFile<?> f) {
    return String.format("%s %s spec=%d partition=%s seq=%s/%s records=%d",
        f.location(), f.content(), f.specId(), f.partition(), f.dataSequenceNumber(),
        f.fileSequenceNumber(), f.recordCount());
  }

  private static String hex(ByteBuffer buffer) {
    if (buffer == null) {
      return null;
    }
    StringBuilder out = new StringBuilder();
    ByteBuffer copy = buffer.duplicate();
    while (copy.hasRemaining()) {
      out.append(String.format("%02x", copy.get()));
    }
    return out.toString();
  }
}
