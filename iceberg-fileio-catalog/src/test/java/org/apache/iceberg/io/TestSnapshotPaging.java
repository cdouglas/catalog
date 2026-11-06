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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** A catalog that pages its inline tables' old snapshots (docs/SNAPSHOT_PAGING.md, step 4). */
class TestSnapshotPaging {
  private static final String WAREHOUSE = "mem:///wh";
  private static final TableIdentifier TABLE = TableIdentifier.of("ns", "t");
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "cat", Types.IntegerType.get()));
  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("cat").build();

  private final InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();

  private LogCatalog catalog(Map<String, String> properties) {
    return LocalCatalogMode.APPEND_ML.catalog(io, WAREHOUSE, "paging", properties);
  }

  private static Map<String, String> paging(int block, int window) {
    return Map.of(
        LogCatalogProperties.SNAPSHOT_BLOCK, String.valueOf(block),
        LogCatalogProperties.SNAPSHOT_WINDOW, String.valueOf(window));
  }

  /**
   * The catalog's first record fixes its paging, as it does its conflict encoding: a catalog that
   * opens the log later uses the log's, whatever it is configured with, and a checkpoint keeps it.
   */
  @Test
  void theFirstRecordFixesThePaging() {
    LogCatalog first = catalog(paging(4, 8));
    CatalogState state = first.log().read().state();
    assertThat(state.snapshotBlock()).isEqualTo(4);
    assertThat(state.snapshotWindow()).isEqualTo(8);

    LogCatalog second = catalog(paging(256, 1024));
    assertThat(second.log().read().state().snapshotBlock()).isEqualTo(4);

    byte[] checkpoint = CatalogStateMachine.INSTANCE.encodeCheckpoint(state);
    CatalogState decoded = CatalogStateMachine.INSTANCE.decodeCheckpoint(checkpoint).build();
    assertThat(decoded.snapshotBlock()).isEqualTo(4);
    assertThat(decoded.snapshotWindow()).isEqualTo(8);
  }

  /** A catalog that pages nothing records nothing, and a later record cannot set its paging. */
  @Test
  void pagingIsSetOnlyOnANewCatalog() {
    LogCatalog unpaged = catalog(Map.of(LogCatalogProperties.SNAPSHOT_BLOCK, "0"));
    assertThat(unpaged.log().read().state().snapshotBlock()).isZero();
    unpaged.createNamespace(Namespace.of("ns"));
    unpaged.createTable(TABLE, SCHEMA);
    CatalogState.Builder builder = unpaged.log().read().state().toBuilder();
    assertThat(new ProtoCodec.SetSnapshotPagingAction(4, 8).verify(builder)).isFalse();

    CatalogState.Builder paged = CatalogState.builder().setSnapshotPaging(4, 8);
    assertThat(new ProtoCodec.SetSnapshotPagingAction(4, 8).verify(paged)).isFalse();
    assertThat(new ProtoCodec.SetSnapshotPagingAction(0, 8).verify(CatalogState.builder()))
        .isFalse();
    assertThat(new ProtoCodec.SetSnapshotPagingAction(4, 8).verify(CatalogState.builder()))
        .isTrue();
  }

  private static TableMetadata current(Table table) {
    return ((HasTableOperations) table).operations().current();
  }

  private static void append(Table table, int i) {
    table.newFastAppend()
        .appendFile(DataFiles.builder(SPEC)
            .withPath("mem:///data/f" + i + ".parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withPartitionPath("cat=" + (i % 4))
            .build())
        .commit();
  }

  /**
   * At B = 4 and W = 8, a table of 40 fast appends keeps its newest snapshots inline and the rest
   * in sealed blocks; a fresh catalog's load, lazy or not, reads every snapshot, in order, with its
   * list, from the pages a compaction stored or, before one, from the pages its replay sealed.
   */
  @ParameterizedTest
  @EnumSource(names = {"APPEND_ML", "COMPACT_ML", "CAS_TM", "COMPACT_TM", "MEMLOG_ML"})
  void sealedSnapshotsLoadFromTheirPages(LocalCatalogMode mode) throws Exception {
    LogCatalog writer = mode.catalog(io, WAREHOUSE, "sealing", paging(4, 8));
    writer.createNamespace(Namespace.of("ns"));
    Table table = writer.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    List<Long> ids = new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      append(table, i);
      ids.add(table.currentSnapshot().snapshotId());
    }
    CatalogState state = writer.log().read().state();
    int tblId = state.tableId(TABLE);
    assertThat(state.sealedBlocks(tblId)).isNotEmpty();
    assertThat(state.inlineTable(tblId).fields().snapshots.size()).isLessThan(16);
    if (mode != LocalCatalogMode.APPEND_ML && mode != LocalCatalogMode.MEMLOG_ML) {
      // The root the catalog file holds names sealed blocks, and its compactor stored their pages
      // before writing it
      CatalogState root =
          CatalogTestSupport.decodeCheckpoint(io.bytes(WAREHOUSE + "/catalog-sealing")).build();
      assertThat(root.sealedBlocks(tblId)).isNotEmpty();
      List<String> stored = writer.log().pages().list();
      for (SnapshotPages.SealedBlock block : root.sealedBlocks(tblId).values()) {
        assertThat(stored).contains(block.pageName(tblId));
      }
    }

    for (String lazy : List.of("true", "false")) {
      Map<String, String> properties = new HashMap<>(paging(4, 8));
      properties.put(LogCatalog.LOAD_LAZY, lazy);
      Table loaded = mode.catalog(io, WAREHOUSE, "sealing", properties).loadTable(TABLE);
      TableMetadata metadata = current(loaded);
      assertThat(metadata.snapshots()).extracting(Snapshot::snapshotId)
          .as("lazy=%s", lazy)
          .containsExactlyElementsOf(ids);
      for (int i = 0; i < ids.size(); i++) {
        Snapshot snapshot = metadata.snapshot(ids.get(i));
        assertThat(snapshot.allManifests(loaded.io()))
            .extracting(ManifestFile::path)
            .containsExactlyElementsOf(
                table.snapshot(ids.get(i)).allManifests(table.io()).stream()
                    .map(ManifestFile::path)
                    .collect(Collectors.toList()));
      }
      try (CloseableIterable<FileScanTask> tasks = loaded.newScan().planFiles()) {
        assertThat(Iterables.size(tasks)).isEqualTo(40);
      }
      // Time travel to a sealed snapshot
      try (CloseableIterable<FileScanTask> tasks =
          loaded.newScan().useSnapshot(ids.get(2)).planFiles()) {
        assertThat(Iterables.size(tasks)).isEqualTo(3);
      }
    }
  }

  /**
   * A rollback to a sealed snapshot, and a tag on one, bring it back inline with its proof: the
   * table then reads as its writer committed it, through a fresh catalog, and the snapshot leaves
   * its block's live ids. A proof for another place, or a forged leaf, proves nothing.
   */
  @ParameterizedTest
  @EnumSource(names = {"APPEND_ML", "COMPACT_ML", "CAS_TM", "MEMLOG_ML"})
  void aRefMovedOntoASealedSnapshotBringsItBack(LocalCatalogMode mode) throws Exception {
    LogCatalog writer = mode.catalog(io, WAREHOUSE, "back", paging(4, 8));
    writer.createNamespace(Namespace.of("ns"));
    Table table = writer.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    List<Long> ids = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      append(table, i);
      ids.add(table.currentSnapshot().snapshotId());
    }
    CatalogState state = writer.log().read().state();
    int tblId = state.tableId(TABLE);
    long tagged = ids.get(1);
    long rolledBack = ids.get(5);
    assertThat(state.sealedBlockOf(tblId, tagged)).isNotNull();
    assertThat(state.sealedBlockOf(tblId, rolledBack)).isNotNull();

    table.manageSnapshots().createTag("old", tagged).commit();
    table.manageSnapshots().rollbackTo(rolledBack).commit();

    state = writer.log().read().state();
    assertThat(state.sealedBlockOf(tblId, tagged)).isNull();
    assertThat(state.sealedBlockOf(tblId, rolledBack)).isNull();
    assertThat(state.inlineTable(tblId).fields().snapshot(rolledBack)).isNotNull();
    Table fresh = mode.catalog(io, WAREHOUSE, "back", paging(4, 8)).loadTable(TABLE);
    assertThat(fresh.currentSnapshot().snapshotId()).isEqualTo(rolledBack);
    assertThat(fresh.refs().get("old").snapshotId()).isEqualTo(tagged);
    assertThat(current(fresh).snapshots()).extracting(Snapshot::snapshotId)
        .containsExactlyElementsOf(ids);
    try (CloseableIterable<FileScanTask> tasks = fresh.newScan().planFiles()) {
      assertThat(Iterables.size(tasks)).isEqualTo(6);
    }
    // The next commit on main builds on the brought-back snapshot's list
    append(table, 100);
    Table again = mode.catalog(io, WAREHOUSE, "back", paging(4, 8)).loadTable(TABLE);
    try (CloseableIterable<FileScanTask> tasks = again.newScan().planFiles()) {
      assertThat(Iterables.size(tasks)).isEqualTo(7);
    }

    // A proof proves only its own leaf at its own place
    SnapshotPages.SealedBlock block = state.sealedBlocks(tblId).values().iterator().next();
    long live = block.liveIds[0];
    String prefix = state.allManifestPrefixes().getOrDefault(tblId, "");
    SnapshotPages.Page page = SnapshotPages.read(writer.log().pages(), tblId, block, prefix);
    SnapshotPages.BroughtBack proof = SnapshotPages.bringBack(page, live, prefix);
    assertThat(proof.proves(block, prefix)).isTrue();
    assertThat(new SnapshotPages.BroughtBack(
            live, block.block, proof.index ^ 1, proof.leaf, proof.proof).proves(block, prefix))
        .isFalse();
    byte[] forged = proof.leaf.clone();
    forged[forged.length - 1] ^= 1;
    assertThat(new SnapshotPages.BroughtBack(
            live, block.block, proof.index, forged, proof.proof).proves(block, prefix))
        .isFalse();
  }

  /**
   * An expiry that empties sealed blocks drops their pages, which the compaction that writes the
   * root without them deletes; the other blocks' pages stay, and the table reads its remaining
   * snapshots. A sweep deletes an orphan page of a version the root has reached, and keeps one of a
   * later version (a compactor ahead may write a root that names it).
   */
  @ParameterizedTest
  @EnumSource(names = {"CAS_ML", "CAS_TM"})
  void anExpiryThatEmptiesBlocksDropsTheirPages(LocalCatalogMode mode) throws Exception {
    LogCatalog writer = mode.catalog(io, WAREHOUSE, "gc", paging(4, 8));
    writer.createNamespace(Namespace.of("ns"));
    Table table = writer.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    List<Long> ids = new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      append(table, i);
      ids.add(table.currentSnapshot().snapshotId());
    }
    int tblId = writer.log().read().state().tableId(TABLE);
    // The root a commit writes is the state before its own record: compare with roots
    java.util.SortedMap<Long, SnapshotPages.SealedBlock> before = root("gc").sealedBlocks(tblId);
    assertThat(before.size()).isGreaterThan(2);
    org.intentlog.PageStore pages = writer.log().pages();
    for (SnapshotPages.SealedBlock block : before.values()) {
      assertThat(pages.list()).contains(block.pageName(tblId));
    }

    // Expire the first twelve snapshots: the oldest blocks empty
    long cutoff = table.snapshot(ids.get(12)).timestampMillis();
    table.expireSnapshots().expireOlderThan(cutoff).retainLast(1).cleanExpiredFiles(false)
        .commit();
    append(table, 40); // a compaction after the expiry (CAS: every commit)

    CatalogState state = root("gc");
    java.util.SortedMap<Long, SnapshotPages.SealedBlock> after = state.sealedBlocks(tblId);
    List<String> stored = pages.list();
    for (SnapshotPages.SealedBlock block : before.values()) {
      SnapshotPages.SealedBlock kept = after.get(block.block);
      if (kept == null) {
        assertThat(stored).doesNotContain(block.pageName(tblId));
      } else {
        assertThat(stored).contains(kept.pageName(tblId));
      }
    }
    assertThat(after.size()).isLessThan(before.size());
    Table fresh = mode.catalog(io, WAREHOUSE, "gc", paging(4, 8)).loadTable(TABLE);
    List<Long> remaining = current(fresh).snapshots().stream()
        .map(Snapshot::snapshotId).collect(Collectors.toList());
    assertThat(remaining).doesNotContainAnyElementsOf(ids.subList(0, 12));
    assertThat(remaining).containsAll(ids.subList(13, 40));

    // Orphans: one sealed at a version the root has reached, one at a later version
    int version = state.tableVersion(tblId);
    byte[] digest = new byte[SnapshotPages.DIGEST_BYTES];
    String old = SnapshotPages.pageName(tblId, version - 1, 0, digest);
    String ahead = SnapshotPages.pageName(tblId, version + 5, 99, digest);
    pages.put(old, new byte[] {1});
    pages.put(ahead, new byte[] {2});
    writer.log().sweep(writer.log().read());
    after = writer.log().read().state().sealedBlocks(tblId);
    for (SnapshotPages.SealedBlock block : root("gc").sealedBlocks(tblId).values()) {
      assertThat(pages.list()).contains(block.pageName(tblId));
    }
    assertThat(pages.list()).doesNotContain(old).contains(ahead);
  }

  /** The state the catalog file's checkpoint holds: the root its last compaction wrote. */
  private CatalogState root(String name) {
    return CatalogTestSupport.decodeCheckpoint(io.bytes(WAREHOUSE + "/catalog-" + name)).build();
  }

  /**
   * A handle loaded before its table's pages were collected (here, the table was evicted to a
   * pointer and a root without its blocks landed) cannot load the history it held, as Iceberg's
   * readers cannot once an expiry deletes a manifest list: its reads fail as not found, and a
   * commit staged on it fails as stale, which a refresh cures (fault round, B = 2, W = 1). A page
   * the table still names that the store lost is never taken for collected.
   */
  @ParameterizedTest
  @EnumSource(names = {"CAS_TM", "CAS_ML"})
  void aStaleHandleFindsCollectedPagesGone(LocalCatalogMode mode) throws Exception {
    LogCatalog writer = mode.catalog(io, WAREHOUSE, "stale", paging(2, 1));
    writer.createNamespace(Namespace.of("ns"));
    Table table = writer.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    for (int i = 0; i < 12; i++) {
      append(table, i);
    }
    Table stale = mode.catalog(io, WAREHOUSE, "stale", paging(2, 1)).loadTable(TABLE);
    int tblId = writer.log().read().state().tableId(TABLE);
    List<String> named = new ArrayList<>();
    for (SnapshotPages.SealedBlock block : root("stale").sealedBlocks(tblId).values()) {
      named.add(block.pageName(tblId));
    }
    assertThat(named).isNotEmpty();

    LogCatalog pointer = LocalCatalogMode.CAS.catalog(io, WAREHOUSE, "stale", paging(2, 1));
    append(pointer.loadTable(TABLE), 12); // evicts the table
    append(pointer.loadTable(TABLE), 13); // a root without its blocks: their pages go
    assertThat(writer.log().pages().list()).doesNotContainAnyElementsOf(named);

    assertThatThrownBy(() -> current(stale).snapshots())
        .isInstanceOf(org.apache.iceberg.exceptions.NotFoundException.class)
        .hasMessageContaining("are gone");
    // A schema update commits on the metadata it was made from (a properties update refreshes)
    assertThatThrownBy(
            () -> stale.updateSchema().addColumn("c", Types.IntegerType.get()).commit())
        .isInstanceOf(org.apache.iceberg.exceptions.CommitFailedException.class)
        .hasMessageContaining("are gone");
    stale.refresh();
    stale.updateSchema().addColumn("c", Types.IntegerType.get()).commit();
    assertThat(writer.loadTable(TABLE).schema().findField("c")).isNotNull();
    assertThat(Iterables.size(writer.loadTable(TABLE).snapshots())).isEqualTo(14);

    // Lost, not collected: the table still names the page (the re-inlining's import pages)
    CatalogState state = writer.log().read().state();
    assertThat(state.sealedBlocks(tblId)).isNotEmpty();
    Table loaded = mode.catalog(io, WAREHOUSE, "stale", paging(2, 1)).loadTable(TABLE);
    for (SnapshotPages.SealedBlock block : state.sealedBlocks(tblId).values()) {
      if (block.pending == null) {
        writer.log().pages().delete(block.pageName(tblId));
      }
    }
    assertThatThrownBy(() -> current(loaded).snapshots())
        .isInstanceOf(SnapshotPages.MissingPage.class);
  }

  /**
   * A copy of a paged table for a worker (SerializableTable, by Java serialization and by Kryo)
   * ships the snapshots its load holds and where the sealed blocks' pages are (with the bytes of
   * those no compactor has stored yet): the copy reads every snapshot from them, plans the files,
   * time-travels to a sealed snapshot, and the original's history stays unloaded.
   */
  @ParameterizedTest
  @EnumSource(names = {"APPEND_ML", "CAS_ML", "CAS_TM"})
  void aCopyForAWorkerReadsTheHistoryFromThePages(LocalCatalogMode mode) throws Exception {
    LogCatalog writer = mode.catalog(io, WAREHOUSE, "copies", paging(4, 8));
    writer.createNamespace(Namespace.of("ns"));
    Table table = writer.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    List<Long> ids = new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      append(table, i);
      ids.add(table.currentSnapshot().snapshotId());
    }
    Table loaded = mode.catalog(io, WAREHOUSE, "copies", paging(4, 8)).loadTable(TABLE);
    TableMetadata lazy = current(loaded);

    List<Table> copies = List.of(
        org.apache.iceberg.TestHelpers.roundTripSerialize(
            org.apache.iceberg.SerializableTable.copyOf(loaded)),
        TestSerializableInlineTable.kryoRoundTrip(
            org.apache.iceberg.SerializableTable.copyOf(loaded)));
    assertThat(lazy).extracting("snapshotsLoaded").isEqualTo(false);
    for (Table copy : copies) {
      TableMetadata metadata = current(copy);
      assertThat(metadata)
          .extracting("snapshots")
          .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.list(Snapshot.class))
          .hasSizeLessThan(ids.size());
      assertThat(metadata.snapshots()).extracting(Snapshot::snapshotId)
          .containsExactlyElementsOf(ids);
      try (CloseableIterable<FileScanTask> tasks = copy.newScan().planFiles()) {
        assertThat(Iterables.size(tasks)).isEqualTo(40);
      }
      try (CloseableIterable<FileScanTask> tasks =
          copy.newScan().useSnapshot(ids.get(2)).planFiles()) {
        assertThat(Iterables.size(tasks)).isEqualTo(3);
      }
    }
    assertThat(lazy).extracting("snapshotsLoaded").isEqualTo(false);
  }

  /** A pointer table of {@code count} fast appends, written by a catalog that does not inline. */
  private List<Long> pointerTable(LocalCatalogMode mode, String name, int count) {
    LocalCatalogMode pointer = mode.sharedLog ? LocalCatalogMode.MEMLOG : LocalCatalogMode.APPEND;
    LogCatalog writer = pointer.catalog(io, WAREHOUSE, name, paging(4, 8));
    writer.createNamespace(Namespace.of("ns"));
    Table table = writer.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    List<Long> ids = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      append(table, i);
      ids.add(table.currentSnapshot().snapshotId());
    }
    return ids;
  }

  /**
   * A pointer table 40 snapshots deep comes back inline at B = 4, W = 8 (step 5, "Re-inlining"):
   * its history behind the sealing line goes to import pages, stored before the record, which
   * holds the head with the rest. A fresh catalog reads every snapshot with its list file, plans
   * the files and time-travels into an imported block; a rollback brings an imported snapshot
   * back; later commits seal on above the imported blocks.
   */
  @ParameterizedTest
  @EnumSource(names = {"APPEND_ML", "CAS_ML", "CAS_TM", "COMPACT_TM", "MEMLOG_ML"})
  void aDeepPointerTableComesBackInlineWithImportPages(LocalCatalogMode mode) throws Exception {
    List<Long> ids = pointerTable(mode, "reinline", 40);
    LogCatalog writer = mode.catalog(io, WAREHOUSE, "reinline", paging(4, 8));
    Table before = writer.loadTable(TABLE);
    int tblId = writer.log().read().state().tableId(TABLE);
    assertThat(writer.log().read().state().isInlineTable(tblId)).isFalse();

    assertThat(writer.reinline(TABLE)).isTrue();
    CatalogState state = writer.log().read().state();
    assertThat(state.isInlineTable(tblId)).isTrue();
    java.util.SortedMap<Long, SnapshotPages.SealedBlock> sealed = state.sealedBlocks(tblId);
    // Through sequence number 40, blocks 0-7 ([0, 32)) are behind the line
    assertThat(sealed.keySet()).containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L);
    List<String> stored = writer.log().pages().list();
    List<Long> imported = new ArrayList<>();
    for (SnapshotPages.SealedBlock block : sealed.values()) {
      assertThat(block.version).isEqualTo(state.tableVersion(tblId));
      assertThat(block.pending).as("an import page is stored before its record").isNull();
      assertThat(stored).contains(block.pageName(tblId));
      for (long id : block.liveIds) {
        imported.add(id);
      }
    }
    List<Long> inline = new ArrayList<>();
    for (Snapshot snapshot : state.inlineTable(tblId).fields().snapshots) {
      inline.add(snapshot.snapshotId());
    }
    assertThat(inline).containsExactlyElementsOf(ids.subList(31, 40));
    assertThat(imported).containsExactlyInAnyOrderElementsOf(ids.subList(0, 31));
    assertThat(writer.reinline(TABLE)).as("an inline table stays as it is").isTrue();

    for (String lazy : List.of("true", "false")) {
      Map<String, String> properties = new HashMap<>(paging(4, 8));
      properties.put(LogCatalog.LOAD_LAZY, lazy);
      Table loaded = mode.catalog(io, WAREHOUSE, "reinline", properties).loadTable(TABLE);
      TableMetadata metadata = current(loaded);
      assertThat(metadata.snapshots()).extracting(Snapshot::snapshotId)
          .as("lazy=%s", lazy)
          .containsExactlyElementsOf(ids);
      for (long id : ids) {
        assertThat(metadata.snapshot(id).manifestListLocation())
            .isEqualTo(before.snapshot(id).manifestListLocation());
      }
      try (CloseableIterable<FileScanTask> tasks = loaded.newScan().planFiles()) {
        assertThat(Iterables.size(tasks)).isEqualTo(40);
      }
      try (CloseableIterable<FileScanTask> tasks =
          loaded.newScan().useSnapshot(ids.get(2)).planFiles()) {
        assertThat(Iterables.size(tasks)).isEqualTo(3);
      }
    }

    // A rollback into an imported block brings the snapshot back with its proof
    Table table = writer.loadTable(TABLE);
    table.manageSnapshots().rollbackTo(ids.get(5)).commit();
    state = writer.log().read().state();
    assertThat(state.sealedBlockOf(tblId, ids.get(5))).isNull();
    assertThat(state.inlineTable(tblId).fields().snapshot(ids.get(5))).isNotNull();
    for (int i = 40; i < 60; i++) {
      append(table, i);
    }
    state = writer.log().read().state();
    assertThat(state.sealedBlocks(tblId).values())
        .as("blocks sealed by replay after the import")
        .anyMatch(block -> block.version > sealed.get(0L).version);
    Table fresh = mode.catalog(io, WAREHOUSE, "reinline", paging(4, 8)).loadTable(TABLE);
    try (CloseableIterable<FileScanTask> tasks = fresh.newScan().planFiles()) {
      assertThat(Iterables.size(tasks)).isEqualTo(6 + 20);
    }
    List<Long> every = new ArrayList<>(ids);
    for (Snapshot snapshot : table.snapshots()) {
      if (!every.contains(snapshot.snapshotId())) {
        every.add(snapshot.snapshotId());
      }
    }
    assertThat(current(fresh).snapshots()).extracting(Snapshot::snapshotId)
        .containsExactlyElementsOf(every);
  }

  /**
   * Errata D22: the first commit after an upgrade to v3 gives the manifests it keeps row ids,
   * which would restate pooled entries, so it evicts an inline-ML table. The table's next commit
   * through an inline writer brings it back: a pointer table has no pool, so its lists' entries
   * start one anew, with row ids. In a catalog that pages, its history goes to import pages.
   */
  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(ints = {0, 4})
  void aTableUpgradedToV3ComesBackInline(int block) throws Exception {
    LogCatalog writer = LocalCatalogMode.CAS_ML.catalog(
        io, WAREHOUSE, "v3-" + block, block > 0 ? paging(block, 8) : Map.of());
    writer.createNamespace(Namespace.of("ns"));
    Table table = writer.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC)
        .withProperty(org.apache.iceberg.TableProperties.FORMAT_VERSION, "2").create();
    for (int i = 0; i < 30; i++) {
      append(table, i);
    }
    int tblId = writer.log().read().state().tableId(TABLE);
    table.updateProperties().set(org.apache.iceberg.TableProperties.FORMAT_VERSION, "3").commit();
    assertThat(writer.log().read().state().isInlineTable(tblId)).isTrue();
    append(table, 30);
    assertThat(writer.log().read().state().isInlineTable(tblId)).as("evicted (D22)").isFalse();
    append(table, 31);
    CatalogState state = writer.log().read().state();
    assertThat(state.isInlineTable(tblId)).as("back inline").isTrue();
    assertThat(state.sealedBlocks(tblId).isEmpty()).isEqualTo(block == 0);
    append(table, 32);
    assertThat(writer.log().read().state().isInlineTable(tblId)).isTrue();
    Table fresh = LocalCatalogMode.CAS_ML.catalog(io, WAREHOUSE, "v3-" + block, Map.of())
        .loadTable(TABLE);
    assertThat(current(fresh).snapshots()).hasSize(33);
    try (CloseableIterable<FileScanTask> tasks = fresh.newScan().planFiles()) {
      assertThat(Iterables.size(tasks)).isEqualTo(33);
    }
  }

  /**
   * An import whose record never lands leaves pages a sweep deletes once the table's version
   * passes theirs; a record staged on a version another commit has passed does not apply.
   */
  @ParameterizedTest
  @EnumSource(names = {"CAS_ML", "MEMLOG_TM"})
  void anImportThatDoesNotLandLeavesGarbage(LocalCatalogMode mode) throws Exception {
    pointerTable(mode, "lost", 40);
    LogCatalog writer = mode.catalog(io, WAREHOUSE, "lost", paging(4, 8));
    org.intentlog.Replica<CatalogState> read = writer.log().read();
    int tblId = read.state().tableId(TABLE);
    LogCatalog.FileIOTableOperations ops = writer.newTableOps(TABLE, read);
    CatalogMutation mut = new CatalogMutation(read);
    assertThat(ops.stageUpdate(mut, ops.current(), ops.current(), false)).isTrue();
    org.intentlog.PageStore pages = writer.log().pages();
    List<String> put = pages.list().stream()
        .filter(name -> name.startsWith("snap.")).collect(Collectors.toList());
    assertThat(put).hasSize(8);

    // A sweep while the record may still land keeps its pages
    writer.log().sweep(writer.log().read());
    assertThat(pages.list()).containsAll(put);

    // Another commit passes the version the import was staged for
    LogCatalog other = LocalCatalogMode.APPEND.catalog(io, WAREHOUSE, "lost", paging(4, 8));
    if (mode.sharedLog) {
      other = LocalCatalogMode.MEMLOG.catalog(io, WAREHOUSE, "lost", paging(4, 8));
    }
    append(other.loadTable(TABLE), 40);
    assertThatThrownBy(() -> mut.commit(writer.log()))
        .isInstanceOf(org.apache.iceberg.exceptions.CommitFailedException.class);
    assertThat(writer.log().read().state().isInlineTable(tblId)).isFalse();
    writer.log().sweep(writer.log().read());
    assertThat(pages.list()).doesNotContainAnyElementsOf(put);
  }

  /**
   * Replay takes an import's pages on the writer's word, but checks what it can: blocks of the
   * version the update makes, apart from each other and from the head, with no ref on them.
   */
  @Test
  void anImportIsCheckedAgainstItsHead() {
    List<Long> ids = pointerTable(LocalCatalogMode.CAS_ML, "checked", 40);
    LogCatalog writer = LocalCatalogMode.CAS_ML.catalog(io, WAREHOUSE, "checked", paging(4, 8));
    org.intentlog.Replica<CatalogState> read = writer.log().read();
    LogCatalog.FileIOTableOperations ops = writer.newTableOps(TABLE, read);
    CatalogMutation mut = new CatalogMutation(read);
    assertThat(ops.stageUpdate(mut, ops.current(), ops.current(), false)).isTrue();
    ProtoCodec.UpdateTableInlineAction action = mut.intention().actions().stream()
        .filter(ProtoCodec.UpdateTableInlineAction.class::isInstance)
        .map(ProtoCodec.UpdateTableInlineAction.class::cast)
        .findFirst()
        .orElseThrow();
    CatalogState.Builder builder = read.state().toBuilder();
    assertThat(action.explain(builder)).isNull();

    SnapshotPages.SealedBlock first = action.imported.get(0);
    action.importing(List.of(first, first));
    assertThat(action.explain(builder)).contains("out of order");
    action.importing(List.of(relive(first, first.version + 1, first.liveIds)));
    assertThat(action.explain(builder)).contains("is not of version");
    action.importing(List.of(relive(first, first.version, new long[] {ids.get(39)})));
    assertThat(action.explain(builder)).contains("both imported and inline");
    action.importing(List.of(first, new SnapshotPages.SealedBlock(first.block + 1, first.version,
        first.digest, first.merkleRoot, first.leaves, first.liveIds, null)));
    assertThat(action.explain(builder)).contains("in two blocks");
  }

  private static SnapshotPages.SealedBlock relive(
      SnapshotPages.SealedBlock block, int version, long[] live) {
    return new SnapshotPages.SealedBlock(
        block.block, version, block.digest, block.merkleRoot, block.leaves, live, null);
  }
}
