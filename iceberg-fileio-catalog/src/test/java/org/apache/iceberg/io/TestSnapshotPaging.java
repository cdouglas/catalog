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
}
