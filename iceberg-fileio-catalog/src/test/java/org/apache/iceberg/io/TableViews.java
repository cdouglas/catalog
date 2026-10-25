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

/**
 * Views of a table as text, item by item, for properties that compare two tables built the same way
 * in different catalogs: snapshot ids, UUIDs, timestamps and manifest and metadata file names
 * differ between them, so snapshots appear by position and manifests by content. {@link #physical}
 * is what a reader sees, manifest lists entry by entry ({@link TestInlineDifferentialProperties});
 * {@link #logical} is what a table holds, whatever its manifests' layout.
 */
final class TableViews {

  private TableViews() {}

  /**
   * What a reader sees of {@code table}, by item, with snapshots by position: its metadata, every
   * snapshot with its manifest list entry by entry and its changes, every ref with its live files,
   * the snapshot log and every metadata table. With {@code clockStepped}, without the metadata
   * table columns Iceberg derives from the order of timestamps.
   */
  static Map<String, String> physical(Table table, boolean clockStepped) {
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
    items.put("statistics", metadata.statisticsFiles().stream()
        .map(f -> String.format("#%s %s %d/%d %s", position.get(f.snapshotId()), f.path(),
            f.fileSizeInBytes(), f.fileFooterSizeInBytes(), f.blobMetadata().stream()
                .map(b -> String.format("%s #%s seq=%d %s %s", b.type(),
                    position.get(b.sourceSnapshotId()), b.sourceSnapshotSequenceNumber(),
                    b.fields(), b.properties()))
                .collect(java.util.stream.Collectors.toList())))
        .sorted()
        .collect(java.util.stream.Collectors.toList()).toString());
    items.put("partition statistics", metadata.partitionStatisticsFiles().stream()
        .map(f -> String.format("#%s %s %d", position.get(f.snapshotId()), f.path(),
            f.fileSizeInBytes()))
        .sorted()
        .collect(java.util.stream.Collectors.toList()).toString());
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
      items.put(String.format("snapshot %02d changes", i), changes(table, s));
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
    for (org.apache.iceberg.MetadataTableType type : org.apache.iceberg.MetadataTableType.values()) {
      if (type != org.apache.iceberg.MetadataTableType.POSITION_DELETES) {
        items.put("metadata table " + type,
            metadataTable(table, type, position, clockStepped));
      }
    }
    return items;
  }

  /**
   * What {@code table} holds, by item, whatever its manifests' layout: its metadata, every snapshot
   * (sequence number, parent, operation, schema, summary without manifest counts) with the files it
   * added and removed, every ref with its live data files and their deletes (with partitions,
   * sequence numbers and record counts), and the snapshot log. Row ids appear only as an
   * invariant: the live files of each ref have disjoint row-id ranges below the next row id.
   */
  static Map<String, String> logical(Table table) {
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    Map<String, String> items = new TreeMap<>();
    List<Snapshot> snapshots = metadata.snapshots();
    Map<Long, Integer> position = new HashMap<>();
    for (int i = 0; i < snapshots.size(); i++) {
      position.put(snapshots.get(i).snapshotId(), i);
    }
    items.put("format", metadata.formatVersion() + " last-seq=" + metadata.lastSequenceNumber()
        + " location=" + metadata.location());
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
      summary.keySet().removeIf(key -> key.startsWith("manifests-"));
      if (summary.containsKey("source-snapshot-id")) {
        summary.put("source-snapshot-id",
            "#" + position.get(Long.parseLong(summary.get("source-snapshot-id"))));
      }
      items.put(String.format("snapshot %02d", i), String.format(
          "seq=%d parent=%s op=%s schema=%s summary=%s",
          s.sequenceNumber(), s.parentId() == null ? null : "#" + position.get(s.parentId()),
          s.operation(), s.schemaId(), summary));
      items.put(String.format("snapshot %02d changes", i), changes(table, s));
    }
    for (Map.Entry<String, SnapshotRef> r : metadata.refs().entrySet()) {
      SnapshotRef ref = r.getValue();
      items.put("ref " + r.getKey(), String.format("#%s %s %s/%s/%s",
          position.get(ref.snapshotId()), ref.isBranch() ? "branch" : "tag",
          ref.minSnapshotsToKeep(), ref.maxSnapshotAgeMs(), ref.maxRefAgeMs()));
      items.put("ref " + r.getKey() + " files", liveFiles(table, r.getKey()));
      items.put("ref " + r.getKey() + " row ids", rowIds(table, r.getKey(), metadata));
    }
    items.put("snapshot log", metadata.snapshotLog().stream()
        .map(e -> "#" + position.get(e.snapshotId()))
        .collect(java.util.stream.Collectors.toList()).toString());
    return items;
  }

  /**
   * The keys whose values differ between {@code expected} and {@code actual}, with both values, as
   * lines for an assertion message.
   */
  static List<String> differences(
      Map<String, String> expected, Map<String, String> actual, String expectedName,
      String actualName) {
    List<String> differences = new ArrayList<>();
    java.util.Set<String> keys = new java.util.TreeSet<>(expected.keySet());
    keys.addAll(actual.keySet());
    for (String key : keys) {
      if (!java.util.Objects.equals(expected.get(key), actual.get(key))) {
        differences.add(String.format("%s:%n    %s: %s%n    %s: %s",
            key, expectedName, expected.get(key), actualName, actual.get(key)));
      }
    }
    return differences;
  }

  /** The live data files of {@code ref}, each with its delete files, sorted, without row ids. */
  private static String liveFiles(Table table, String ref) {
    List<String> out = new ArrayList<>();
    try (org.apache.iceberg.io.CloseableIterable<FileScanTask> tasks =
        table.newScan().useRef(ref).planFiles()) {
      for (FileScanTask task : tasks) {
        List<String> deletes = new ArrayList<>();
        for (DeleteFile d : task.deletes()) {
          deletes.add(describe(d) + " ref=" + d.referencedDataFile());
        }
        deletes.sort(String::compareTo);
        out.add(describe(task.file()) + " deletes=" + deletes);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    out.sort(String::compareTo);
    return out.toString();
  }

  /**
   * Whether the live data files of {@code ref} that have row ids have disjoint row-id ranges below
   * the table's next row id: "ok" with the number of files without row ids (those a v3 commit has
   * not given ids yet, after an upgrade), or the first violation by path. Tables before v3 have
   * no row ids.
   */
  private static String rowIds(Table table, String ref, TableMetadata metadata) {
    if (metadata.formatVersion() < 3) {
      return "none";
    }
    List<org.apache.iceberg.DataFile> files = new ArrayList<>();
    try (org.apache.iceberg.io.CloseableIterable<FileScanTask> tasks =
        table.newScan().useRef(ref).planFiles()) {
      tasks.forEach(task -> files.add(task.file()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    files.sort(java.util.Comparator.comparing(org.apache.iceberg.DataFile::location));
    TreeMap<Long, Long> ranges = new TreeMap<>(); // first row id -> end
    int withoutIds = 0;
    for (org.apache.iceberg.DataFile file : files) {
      Long first = file.firstRowId();
      if (first == null) {
        withoutIds += 1;
        continue;
      }
      long end = first + file.recordCount();
      if (end > metadata.nextRowId()) {
        return "past the next row id: " + file.location();
      }
      Map.Entry<Long, Long> before = ranges.floorEntry(first);
      Map.Entry<Long, Long> after = ranges.ceilingEntry(first);
      if ((before != null && before.getValue() > first) || (after != null && after.getKey() < end)) {
        return "overlapping row ids: " + file.location();
      }
      ranges.put(first, end);
    }
    return "ok, " + withoutIds + " without row ids";
  }

  /**
   * The rows of {@code table}'s metadata table {@code type}, sorted, with snapshot ids as
   * positions and what differs between the two runs (times, file names, sizes) left out: with the
   * clock-ordered columns, if the wall clock stepped.
   */
  private static String metadataTable(
      Table table, org.apache.iceberg.MetadataTableType type, Map<Long, Integer> position,
      boolean clockStepped) {
    Table metadata = org.apache.iceberg.MetadataTableUtils.createMetadataTableInstance(table, type);
    List<String> rows = new ArrayList<>();
    try (org.apache.iceberg.io.CloseableIterable<FileScanTask> tasks =
        metadata.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.StructLike> data =
            task.asDataTask().rows()) {
          data.forEach(row -> rows.add(
              describe(metadata.schema().asStruct(), row, position, clockStepped)));
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (RuntimeException e) {
      return "fails: " + e;
    }
    rows.sort(String::compareTo);
    return rows.toString();
  }

  private static String describe(
      org.apache.iceberg.types.Types.StructType type, org.apache.iceberg.StructLike row,
      Map<Long, Integer> position, boolean clockStepped) {
    List<String> fields = new ArrayList<>();
    for (int i = 0; i < type.fields().size(); i++) {
      org.apache.iceberg.types.Types.NestedField field = type.fields().get(i);
      String name = field.name();
      Object value = row.get(i, Object.class);
      String shown;
      if (clockStepped && ClockSteps.CLOCK_ORDERED.contains(name)) {
        shown = "*";
      } else if (value != null && (name.endsWith("snapshot_id") || name.equals("parent_id"))) {
        shown = "#" + position.get((Long) value);
      } else if (name.contains("timestamp") || name.contains("committed_at")
          || name.contains("made_current_at") || name.contains("path") || name.equals("file")
          || name.contains("length") || name.equals("manifest_list")
          || name.contains("last_updated") || name.equals("readable_metrics")
          || name.equals("summary") || name.equals("file_size_in_bytes")) {
        shown = "*";
      } else if (value instanceof org.apache.iceberg.StructLike && field.type().isStructType()) {
        shown = "{" + describe(field.type().asStructType(), (org.apache.iceberg.StructLike) value,
            position, clockStepped) + "}";
      } else if (value instanceof List && field.type().isListType()
          && field.type().asListType().elementType().isStructType()) {
        List<String> elements = new ArrayList<>();
        for (Object element : (List<?>) value) {
          elements.add("{" + describe(field.type().asListType().elementType().asStructType(),
              (org.apache.iceberg.StructLike) element, position, clockStepped) + "}");
        }
        shown = elements.toString();
      } else if (value instanceof ByteBuffer) {
        shown = hex((ByteBuffer) value);
      } else {
        shown = String.valueOf(value);
      }
      fields.add(name + "=" + shown);
    }
    return String.join(" ", fields);
  }

  /** The data and delete files {@code s} added and removed, as incremental scans read them. */
  private static String changes(Table table, Snapshot s) {
    List<String> out = new ArrayList<>();
    try {
      s.addedDataFiles(table.io()).forEach(f -> out.add("+data " + f.location()));
      s.removedDataFiles(table.io()).forEach(f -> out.add("-data " + f.location()));
      s.addedDeleteFiles(table.io()).forEach(f -> out.add("+delete " + f.location()));
      s.removedDeleteFiles(table.io()).forEach(f -> out.add("-delete " + f.location()));
    } catch (RuntimeException e) {
      return "fails: " + e;
    }
    out.sort(String::compareTo);
    return out.toString();
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
  static String files(Table table, String ref) {
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
