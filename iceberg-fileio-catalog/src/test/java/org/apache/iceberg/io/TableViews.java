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
   * sequence numbers and record counts), the snapshot log, and its row ids.
   *
   * <p>A commit's row ids depend on its layout: a manifest it writes takes ids for every row it
   * holds, kept ones too (a merged manifest's), so a retry that merges otherwise gives its new rows
   * others. The table's first {@code earlier} snapshots are the ones before the commit compared:
   * the ids of their files, and their first row ids, are exact. A later snapshot's files show the
   * ids an earlier one gave them; an id it gives must be fresh, at or above {@code
   * nextRowIdBefore}, the table's next row id before it. Across all snapshots each file has one id
   * once it has one, and the files' ranges are disjoint below the next row id.
   */
  static Map<String, String> logical(Table table, int earlier, long nextRowIdBefore) {
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
          "seq=%d parent=%s op=%s schema=%s%s summary=%s",
          s.sequenceNumber(), s.parentId() == null ? null : "#" + position.get(s.parentId()),
          s.operation(), s.schemaId(),
          i < earlier ? " first-row-id=" + s.firstRowId() + " added-rows=" + s.addedRows() : "",
          summary));
      items.put(String.format("snapshot %02d changes", i), changes(table, s));
    }
    rowIds(table, metadata, earlier, nextRowIdBefore, items);
    for (Map.Entry<String, SnapshotRef> r : metadata.refs().entrySet()) {
      SnapshotRef ref = r.getValue();
      items.put("ref " + r.getKey(), String.format("#%s %s %s/%s/%s",
          position.get(ref.snapshotId()), ref.isBranch() ? "branch" : "tag",
          ref.minSnapshotsToKeep(), ref.maxSnapshotAgeMs(), ref.maxRefAgeMs()));
      items.put("ref " + r.getKey() + " files", liveFiles(table, r.getKey()));
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
   * Puts in {@code items} the row ids of each snapshot's live data files, by path ({@link
   * #logical}): exact for the first {@code earlier} snapshots; for a later one, the id its parent
   * gave a file, else "fresh" if at or above {@code nextRowIdBefore} ("stale" and the id if not,
   * null if none). And whether each snapshot keeps the ids its parent gave its files, and the
   * ranges of the ids given are disjoint below the next row id: "ok", or the first violation by
   * path. A file written before the table's upgrade to v3 takes ids in the first v3 commit of each
   * branch that keeps it, so on two branches it can have two. Tables before v3 have no row ids.
   */
  private static void rowIds(Table table, TableMetadata metadata, int earlier,
      long nextRowIdBefore, Map<String, String> items) {
    if (metadata.formatVersion() < 3) {
      items.put("row ids", "none");
      return;
    }
    Map<Long, Map<String, Long>> given = new HashMap<>(); // each snapshot's files' ids, by path
    Map<String, org.apache.iceberg.DataFile> assigned = new TreeMap<>(); // by path and id
    String violation = null;
    List<Snapshot> snapshots = metadata.snapshots();
    for (int i = 0; i < snapshots.size(); i++) {
      Snapshot snapshot = snapshots.get(i);
      Map<String, Long> parent =
          snapshot.parentId() != null ? given.get(snapshot.parentId()) : null;
      Map<String, Long> mine = new HashMap<>();
      List<String> ids = new ArrayList<>();
      for (org.apache.iceberg.DataFile file : liveDataFiles(table, snapshot)) {
        String path = file.location();
        Long first = file.firstRowId();
        Long was = parent != null ? parent.get(path) : null;
        mine.put(path, first);
        if (i < earlier || first == null) {
          ids.add(path + "=" + first);
        } else if (was != null) {
          ids.add(path + "=" + (first.equals(was) ? first : first + " (was " + was + ")"));
        } else {
          ids.add(path + (first >= nextRowIdBefore ? "=fresh" : "=stale " + first));
        }
        if (was != null && !was.equals(first) && violation == null) {
          violation = "a snapshot changed the row ids its parent gave: " + path;
        }
        if (first != null) {
          assigned.putIfAbsent(path + "@" + first, file);
        }
      }
      given.put(snapshot.snapshotId(), mine);
      ids.sort(String::compareTo);
      items.put(String.format("snapshot %02d row ids", i), ids.toString());
    }
    TreeMap<Long, Long> ranges = new TreeMap<>(); // first row id -> end
    for (org.apache.iceberg.DataFile file : assigned.values()) {
      if (violation != null) {
        break;
      }
      long first = file.firstRowId();
      long end = first + file.recordCount();
      Map.Entry<Long, Long> before = ranges.floorEntry(first);
      Map.Entry<Long, Long> after = ranges.ceilingEntry(first);
      if (end > metadata.nextRowId()) {
        violation = "past the next row id: " + file.location();
      } else if ((before != null && before.getValue() > first)
          || (after != null && after.getKey() < end)) {
        violation = "overlapping row ids: " + file.location();
      }
      ranges.put(first, end);
    }
    items.put("row ids", violation != null ? violation : "ok");
  }

  /** The live data files of {@code snapshot}. */
  private static List<org.apache.iceberg.DataFile> liveDataFiles(Table table, Snapshot snapshot) {
    List<org.apache.iceberg.DataFile> files = new ArrayList<>();
    try (org.apache.iceberg.io.CloseableIterable<FileScanTask> tasks =
        table.newScan().useSnapshot(snapshot.snapshotId()).planFiles()) {
      tasks.forEach(task -> files.add(task.file()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return files;
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
