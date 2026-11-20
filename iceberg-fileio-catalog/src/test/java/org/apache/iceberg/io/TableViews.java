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
 * {@link #logical} is what a table holds, whatever its manifests' layout; {@link #rows} is what a
 * scan of a ref under a row filter reads, whatever its files' layout, and the schema it reads it
 * with.
 */
final class TableViews {

  private TableViews() {}

  /**
   * What a scan of {@code ref} under {@code filter} reads in {@code table}: first the schema it
   * reads with, the field ids, names and types of the one the scan resolves (a branch's, or no
   * such ref's, the table's current schema; a tag's, its snapshot's), then its rows, sorted: none
   * if the ref names no snapshot (or there is no such ref). A row is a token, which {@code lineage}
   * says which data file holds, and the delete files that delete from it; two scans read the same
   * if they read with the same schema the same tokens, each deleted from by the same delete files.
   * A schema change that leaves the ref where it was (a rename, a drop and re-add, a type
   * promotion) changes what its scan reads (Bug 120 (schema change escaped read witness)).
   *
   * <p>The model is {@link TableScript}'s data: every row of a data file has the {@code cat} its
   * bounds give (each file's lower and upper bounds are equal), so a filter on {@code cat} (and its
   * transforms) selects whole files, exactly, whatever the table's partition spec. A data file's
   * rows are one token, its path, unless a rewrite (Iceberg's {@code REPLACE}) wrote it: then they
   * are the rows of the files it replaced, each with the delete files that deleted from it there
   * ({@link Lineage}), so a rewrite keeps the rows. A delete file deletes from every data file
   * Iceberg's planning applies it to (by partition, sequence number and referenced file); what it
   * deletes there is not modeled, so a delete file added where it applies to a row changes the row,
   * and one that applies to none changes nothing.
   */
  static List<String> rows(
      Table table, String ref, org.apache.iceberg.expressions.Expression filter,
      boolean caseSensitive, Lineage lineage) {
    String schema = schemaRead(table, ref);
    Snapshot snapshot = table.snapshot(ref);
    if (snapshot == null) {
      return List.of(schema);
    }
    org.apache.iceberg.expressions.Evaluator matches = new org.apache.iceberg.expressions.Evaluator(
        TableScript.SCHEMA.asStruct(), filter, caseSensitive);
    List<String> rows = new ArrayList<>();
    for (FileScanTask task : plan(table, snapshot.snapshotId())) {
      if (!matches.eval(new CatRow(cat(task.file())))) {
        continue;
      }
      java.util.Set<String> deletes = paths(task.deletes());
      lineage.rows(task.file().location()).forEach((token, deletedThere) -> {
        java.util.Set<String> by = new java.util.TreeSet<>(deletedThere);
        by.addAll(deletes);
        rows.add(token + " deleted by " + by);
      });
    }
    rows.sort(String::compareTo);
    rows.add(0, schema);
    return rows;
  }

  /**
   * The schema a scan of {@code ref} reads {@code table} with, as {@link #rows} gives it first: its
   * fields' ids, names and types.
   */
  static String schemaRead(Table table, String ref) {
    return "schema " + org.apache.iceberg.util.SnapshotUtil.schemaFor(table, ref).asStruct();
  }

  /**
   * Which rows each data file of a table holds ({@link #rows}): a file's own path, one token,
   * unless a rewrite wrote it. A rewrite's snapshot must be observed before an expiry removes its
   * parent: {@link #observe} after each commit.
   */
  static final class Lineage {
    // the rows of each data file a rewrite wrote: token -> the delete files that deleted from it
    private final Map<String, Map<String, java.util.Set<String>>> rewritten = new HashMap<>();
    private final java.util.Set<Long> seen = new java.util.HashSet<>();

    /** The rows {@code path} holds: token -> the delete files that deleted from it before. */
    Map<String, java.util.Set<String>> rows(String path) {
      Map<String, java.util.Set<String>> rows = rewritten.get(path);
      return rows != null ? rows : Map.of(path, java.util.Set.of());
    }

    /**
     * Records what each rewrite among {@code table}'s snapshots not observed before wrote: each file
     * it added holds the rows of the files of the same {@code cat} it removed, each with the delete
     * files that deleted from it in the rewrite's parent.
     */
    void observe(Table table) {
      for (Snapshot s : table.snapshots()) {
        if (!seen.add(s.snapshotId())
            || !org.apache.iceberg.DataOperations.REPLACE.equals(s.operation())) {
          continue;
        }
        if (s.parentId() == null || table.snapshot(s.parentId()) == null) {
          throw new IllegalStateException(
              "lineage: the parent of rewrite " + s.snapshotId() + " is gone");
        }
        // Read with the table's specs: a manifest's own spec need not bind to the schema the
        // manifest carries (a rewrite, after a replace dropped the source column of an older
        // spec, of a file in that spec), and Snapshot#removedDataFiles(FileIO) reads with it
        org.apache.iceberg.SnapshotChanges changes =
            org.apache.iceberg.SnapshotChanges.builderFor(table).snapshot(s).build();
        java.util.Set<String> removed = new java.util.HashSet<>();
        changes.removedDataFiles().forEach(f -> removed.add(f.location()));
        java.util.Set<String> added = new java.util.HashSet<>();
        changes.addedDataFiles().forEach(f -> added.add(f.location()));
        // the rows each cat's replaced files held, with what deleted from them there
        Map<Integer, Map<String, java.util.Set<String>>> replaced = new TreeMap<>();
        for (FileScanTask task : plan(table, s.parentId())) {
          if (!removed.remove(task.file().location())) {
            continue;
          }
          java.util.Set<String> deletes = paths(task.deletes());
          Map<String, java.util.Set<String>> rows =
              replaced.computeIfAbsent(cat(task.file()), k -> new TreeMap<>());
          rows(task.file().location()).forEach((token, before) -> {
            java.util.Set<String> by = new java.util.TreeSet<>(before);
            by.addAll(deletes);
            rows.put(token, by);
          });
        }
        if (!removed.isEmpty()) {
          throw new IllegalStateException(
              "lineage: rewrite " + s.snapshotId() + " removed files its parent lacks: " + removed);
        }
        for (FileScanTask task : plan(table, s.snapshotId())) {
          if (added.remove(task.file().location())) {
            // the first file a rewrite added in a cat holds the rows it replaced there
            Map<String, java.util.Set<String>> rows = replaced.remove(cat(task.file()));
            if (rows != null) {
              rewritten.put(task.file().location(), rows);
            }
          }
        }
      }
    }
  }

  /** The live data files of {@code snapshotId}, with their column bounds and delete files. */
  private static List<FileScanTask> plan(Table table, long snapshotId) {
    List<FileScanTask> planned = new ArrayList<>();
    try (org.apache.iceberg.io.CloseableIterable<FileScanTask> tasks =
        table.newScan().useSnapshot(snapshotId).includeColumnStats().planFiles()) {
      tasks.forEach(planned::add);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return planned;
  }

  private static java.util.Set<String> paths(List<DeleteFile> deletes) {
    java.util.Set<String> paths = new java.util.TreeSet<>();
    deletes.forEach(d -> paths.add(d.location()));
    return paths;
  }

  /** The {@code cat} of every row of {@code file}: its bounds, which {@link TableScript} sets. */
  private static int cat(org.apache.iceberg.DataFile file) {
    ByteBuffer lower = file.lowerBounds() == null ? null : file.lowerBounds().get(3);
    ByteBuffer upper = file.upperBounds() == null ? null : file.upperBounds().get(3);
    if (lower == null || !lower.equals(upper)) {
      throw new IllegalStateException("rows: " + file.location() + " has no one cat");
    }
    return org.apache.iceberg.types.Conversions.<Integer>fromByteBuffer(
        org.apache.iceberg.types.Types.IntegerType.get(), lower);
  }

  /** A row of {@link TableScript#SCHEMA} with only its {@code cat}, which filters read. */
  private static final class CatRow implements org.apache.iceberg.StructLike {
    private final Object[] values;

    CatRow(int cat) {
      this.values = new Object[TableScript.SCHEMA.columns().size()];
      values[TableScript.SCHEMA.columns().indexOf(TableScript.SCHEMA.findField("cat"))] = cat;
    }

    @Override
    public int size() {
      return values.length;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(values[pos]);
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("a row of the model is fixed");
    }
  }

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
    items.put("metadata log length", String.valueOf(metadata.previousFiles().size()));
    statistics(metadata, position, items);
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
   * The table's statistics and partition statistics files, each by the position of its snapshot
   * (and its blobs' source snapshots), as items of {@code items}.
   */
  private static void statistics(
      TableMetadata metadata, Map<Long, Integer> position, Map<String, String> items) {
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
  }

  /**
   * What {@code table} holds, by item, whatever its manifests' layout: its metadata, every snapshot
   * (sequence number, parent, operation, schema, summary without manifest counts) with the files it
   * added and removed, every ref with its live data files and their deletes (with partitions,
   * sequence numbers and record counts), the snapshot log, its row ids, and whether each manifest
   * says what its entries hold (its minimum sequence number, its row id range: {@link
   * #manifests}), which no layout changes. Also its statistics files, whether its times run
   * forward ({@link #order}), the live files of each snapshot no ref names, and the files a
   * snapshot reaches that are gone.
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
    List<Snapshot> all = ((HasTableOperations) table).operations().current().snapshots();
    java.util.Set<Long> ids = new java.util.HashSet<>();
    for (int i = 0; i < Math.min(earlier, all.size()); i++) {
      ids.add(all.get(i).snapshotId());
    }
    return logical(table, ids, nextRowIdBefore);
  }

  /**
   * As {@link #logical(Table, int, long)}, the snapshots before the commit compared named by id
   * ({@code earlier}, the table's snapshots then): a count by position shifts when the commit
   * removes an earlier snapshot (an expiry in a transaction), and a paged table's history need
   * not count alike at that point.
   */
  static Map<String, String> logical(
      Table table, java.util.Set<Long> earlier, long nextRowIdBefore) {
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
          earlier.contains(s.snapshotId())
              ? " first-row-id=" + s.firstRowId() + " added-rows=" + s.addedRows() : "",
          summary));
      items.put(String.format("snapshot %02d changes", i), changes(table, s));
    }
    rowIds(table, metadata, earlier, nextRowIdBefore, items);
    items.put("manifests", manifests(table, metadata));
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
    statistics(metadata, position, items);
    items.put(ORDER, order(metadata, position));
    // What a snapshot no ref names would make live (a cherry-pick of a staged one, a rollback to
    // one in a branch's history): its live data files and their deletes
    java.util.Set<Long> heads = new java.util.HashSet<>();
    metadata.refs().values().forEach(ref -> heads.add(ref.snapshotId()));
    for (int i = 0; i < snapshots.size(); i++) {
      long id = snapshots.get(i).snapshotId();
      if (!heads.contains(id)) {
        items.put(String.format("snapshot %02d live", i),
            liveFiles(table.newScan().useSnapshot(id)));
      }
    }
    items.put("files gone", filesGone(table, snapshots));
    return items;
  }

  /** The key of {@link #logical}'s item on whether the table's times run forward. */
  static final String ORDER = "order";

  /**
   * Whether the table's times run forward ("forward"), or where they go back: its snapshot log,
   * each snapshot against its parent, and its last update against the log's end. Iceberg stamps
   * them with the wall clock, and a record merged where it lands takes the landing table's times
   * where its writer's are older (SPEC_TM.md, "Late-bound fields"). A run during which the clock
   * stepped back may have them out of order however they were stamped ({@link ClockSteps}), so a
   * comparison across such a run drops the item.
   */
  static String order(TableMetadata metadata, Map<Long, Integer> position) {
    List<String> back = new ArrayList<>();
    long last = Long.MIN_VALUE;
    for (org.apache.iceberg.HistoryEntry entry : metadata.snapshotLog()) {
      if (entry.timestampMillis() < last) {
        back.add("log at #" + position.get(entry.snapshotId()));
      }
      last = Math.max(last, entry.timestampMillis());
    }
    for (Snapshot s : metadata.snapshots()) {
      Snapshot parent = s.parentId() == null ? null : metadata.snapshot(s.parentId());
      if (parent != null && s.timestampMillis() < parent.timestampMillis()) {
        back.add("#" + position.get(s.snapshotId()) + " before its parent");
      }
    }
    if (metadata.lastUpdatedMillis() < last) {
      back.add("last update before the log's end");
    }
    return back.isEmpty() ? "forward" : "back: " + back;
  }

  /**
   * The data and delete files some snapshot of {@code table} reaches that its storage no longer
   * holds, by path: "none" unless a cleanup or a purge deleted what a snapshot still reaches.
   */
  private static String filesGone(Table table, List<Snapshot> snapshots) {
    java.util.Set<String> gone = new java.util.TreeSet<>();
    java.util.Set<String> read = new java.util.HashSet<>();
    Map<Integer, org.apache.iceberg.PartitionSpec> specs = table.specs();
    for (Snapshot snapshot : snapshots) {
      for (ManifestFile manifest : snapshot.allManifests(table.io())) {
        if (!read.add(manifest.path())) {
          continue;
        }
        try (org.apache.iceberg.io.CloseableIterable<? extends org.apache.iceberg.ContentFile<?>>
            files = manifest.content() == org.apache.iceberg.ManifestContent.DATA
                ? org.apache.iceberg.ManifestFiles.read(manifest, table.io(), specs)
                : org.apache.iceberg.ManifestFiles.readDeleteManifest(
                    manifest, table.io(), specs)) {
          for (org.apache.iceberg.ContentFile<?> file : files) {
            if (!table.io().newInputFile(file.location()).exists()) {
              gone.add(file.location());
            }
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }
    return gone.isEmpty() ? "none" : gone.toString();
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
    return liveFiles(table.newScan().useRef(ref));
  }

  /** The live data files {@code scan} plans, each with its deletes, sorted, without row ids. */
  private static String liveFiles(org.apache.iceberg.TableScan scan) {
    List<String> out = new ArrayList<>();
    try (org.apache.iceberg.io.CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
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
  private static void rowIds(Table table, TableMetadata metadata, java.util.Set<Long> earlier,
      long nextRowIdBefore, Map<String, String> items) {
    if (metadata.formatVersion() < 3) {
      items.put("row ids", "none");
      return;
    }
    Map<Long, Map<String, Long>> given = new HashMap<>(); // each snapshot's files' ids, by path
    // what each compared snapshot (one not in earlier) printed for its files' ids, by path: a later
    // one that keeps an id it gave prints the same (a compared commit's ids depend on its layout)
    Map<Long, Map<String, String>> printed = new HashMap<>();
    Map<String, org.apache.iceberg.DataFile> assigned = new TreeMap<>(); // by path and id
    String violation = null;
    List<Snapshot> snapshots = metadata.snapshots();
    for (int i = 0; i < snapshots.size(); i++) {
      Snapshot snapshot = snapshots.get(i);
      Map<String, Long> parent =
          snapshot.parentId() != null ? given.get(snapshot.parentId()) : null;
      Map<String, Long> mine = new HashMap<>();
      Map<String, String> labels = new HashMap<>();
      Map<String, String> parentLabels =
          snapshot.parentId() != null ? printed.get(snapshot.parentId()) : null;
      List<String> ids = new ArrayList<>();
      for (org.apache.iceberg.DataFile file : liveDataFiles(table, snapshot)) {
        String path = file.location();
        Long first = file.firstRowId();
        Long was = parent != null ? parent.get(path) : null;
        mine.put(path, first);
        String label;
        if (earlier.contains(snapshot.snapshotId()) || first == null) {
          label = String.valueOf(first);
        } else if (was != null && first.equals(was) && parentLabels != null
            && parentLabels.containsKey(path)) {
          label = parentLabels.get(path); // kept from a compared snapshot: as it printed it
        } else if (was != null) {
          label = first.equals(was) ? String.valueOf(first) : first + " (was " + was + ")";
        } else {
          label = first >= nextRowIdBefore ? "fresh" : "stale " + first;
        }
        labels.put(path, label);
        ids.add(path + "=" + label);
        if (was != null && !was.equals(first) && violation == null) {
          violation = "a snapshot changed the row ids its parent gave: " + path;
        }
        if (first != null) {
          assigned.putIfAbsent(path + "@" + first, file);
        }
      }
      given.put(snapshot.snapshotId(), mine);
      if (!earlier.contains(snapshot.snapshotId())) {
        printed.put(snapshot.snapshotId(), labels);
      }
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

  /**
   * Whether every manifest of every snapshot's list says what its entries hold, whatever the
   * layout ({@link #logical}): "ok", or the first violation, by snapshot position. A manifest's
   * minimum sequence number is the least data sequence number of its live entries, as Iceberg's
   * {@code ManifestWriter} computes it, and at most its own: a merging commit drops the delete
   * files older than every data manifest's minimum, so one too high drops deletes still needed. In
   * v3, each data manifest of a snapshot with row ids has a first row id; the range of ids from
   * there, its added and existing rows (which {@code ManifestListWriter} sets aside), lies below
   * the table's next row id and apart from every other manifest's; and each live file's ids lie
   * inside it, or below it if the file keeps the ids an earlier manifest gave it (a merged or
   * rewritten manifest's existing files). A manifest a list names again with the same first row
   * id is checked once.
   */
  private static String manifests(Table table, TableMetadata metadata) {
    List<Snapshot> snapshots = metadata.snapshots();
    java.util.Set<String> checked = new java.util.HashSet<>();
    TreeMap<Long, Long> ranges = new TreeMap<>(); // first row id -> end, of distinct manifests
    try {
      for (int i = 0; i < snapshots.size(); i++) {
        Snapshot s = snapshots.get(i);
        for (ManifestFile m : s.allManifests(table.io())) {
          boolean data = m.content() == org.apache.iceberg.ManifestContent.DATA;
          if (data && s.firstRowId() != null && m.firstRowId() == null) {
            return String.format("snapshot %02d lists a data manifest without row ids", i);
          }
          if (!checked.add(m.path() + "@" + m.firstRowId())) {
            continue;
          }
          String problem = manifest(table, metadata, m, ranges);
          if (problem != null) {
            return String.format("snapshot %02d lists a manifest %s", i, problem);
          }
        }
      }
    } catch (RuntimeException e) {
      return "fails: " + e.getClass().getSimpleName();
    }
    return "ok";
  }

  /**
   * What is wrong with {@code m} ({@link #manifests}), adding its row id range to {@code ranges}
   * (first row id to end); null if nothing is.
   */
  private static String manifest(Table table, TableMetadata metadata, ManifestFile m,
      TreeMap<Long, Long> ranges) {
    if (m.minSequenceNumber() > m.sequenceNumber()) {
      return String.format("whose minimum sequence number %d is above its own, %d",
          m.minSequenceNumber(), m.sequenceNumber());
    }
    boolean data = m.content() == org.apache.iceberg.ManifestContent.DATA;
    Long first = data ? m.firstRowId() : null;
    long end = 0;
    if (first != null) {
      end = first + count(m.addedRowsCount()) + count(m.existingRowsCount());
      Map.Entry<Long, Long> before = ranges.floorEntry(first);
      Map.Entry<Long, Long> after = ranges.ceilingEntry(first);
      if (end > metadata.nextRowId()) {
        return String.format("whose row ids [%d, %d) pass the next row id %d", first, end,
            metadata.nextRowId());
      } else if (end > first && ((before != null && before.getValue() > first)
          || (after != null && after.getKey() < end))) {
        return String.format("whose row ids [%d, %d) overlap another manifest's", first, end);
      } else if (end > first) {
        ranges.put(first, end); // a manifest of no live rows sets no id aside
      }
    }
    // Read with the table's specs, as Lineage does: a manifest's own spec need not bind to the
    // schema it carries
    Long least = null;
    for (ContentFile<?> file : liveFiles(table, m)) {
      Long sequenceNumber = file.dataSequenceNumber();
      if (sequenceNumber != null && (least == null || sequenceNumber < least)) {
        least = sequenceNumber;
      }
      Long id = data ? file.firstRowId() : null;
      if (first != null && id != null && id + file.recordCount() > first
          && (id < first || id + file.recordCount() > end)) {
        return String.format("whose row ids [%d, %d) do not hold a file's, [%d, %d)", first,
            end, id, id + file.recordCount());
      }
    }
    if (least != null && least.longValue() != m.minSequenceNumber()) {
      return String.format(
          "whose minimum sequence number is %d, where its live files' least is %d",
          m.minSequenceNumber(), least);
    }
    return null;
  }

  private static long count(Long rows) {
    return rows == null ? 0 : rows;
  }

  /** The live files of manifest {@code m}, read with {@code table}'s specs. */
  private static List<ContentFile<?>> liveFiles(Table table, ManifestFile m) {
    List<ContentFile<?>> files = new ArrayList<>();
    try {
      if (m.content() == org.apache.iceberg.ManifestContent.DATA) {
        try (org.apache.iceberg.ManifestReader<org.apache.iceberg.DataFile> reader =
            org.apache.iceberg.ManifestFiles.read(m, table.io(), table.specs())) {
          reader.forEach(files::add);
        }
      } else {
        try (org.apache.iceberg.ManifestReader<DeleteFile> reader =
            org.apache.iceberg.ManifestFiles.readDeleteManifest(m, table.io(), table.specs())) {
          reader.forEach(files::add);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return files;
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

  /** {@link #metadataTable} of {@code table}, its snapshots positioned in the table's order. */
  static String metadataTableRows(Table table, org.apache.iceberg.MetadataTableType type) {
    Map<Long, Integer> position = new java.util.HashMap<>();
    for (Snapshot s : table.snapshots()) {
      position.put(s.snapshotId(), position.size());
    }
    return metadataTable(table, type, position, false);
  }

  /**
   * The rows of {@code table}'s metadata table {@code type}, sorted, with snapshot ids as
   * positions and what differs between the two runs (times, file names, sizes) left out: with the
   * clock-ordered columns, if the wall clock stepped. Planned on a direct executor: the manifests
   * are read in the order of the snapshot's manifest list (the physical view's), where Iceberg's
   * worker pool reads them in any order, so no row depends on that order (as PARTITIONS' did: C32
   * (partitions spec_id read-order dependent); TestMetadataTableOrder).
   */
  private static String metadataTable(
      Table table, org.apache.iceberg.MetadataTableType type, Map<Long, Integer> position,
      boolean clockStepped) {
    Table metadata = org.apache.iceberg.MetadataTableUtils.createMetadataTableInstance(table, type);
    List<String> rows = new ArrayList<>();
    try (org.apache.iceberg.io.CloseableIterable<FileScanTask> tasks =
        metadata.newScan()
            .planWith(org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors
                .newDirectExecutorService())
            .planFiles()) {
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
