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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * A full update of an inline table (one a delta cannot express, such as a statistics change)
 * carries the table's head with only the snapshots the version it read lacks and its refs'
 * targets, and names the snapshots of that version it drops; replay keeps the rest
 * (SNAPSHOT_PAGING.md step 1, {@code NativeDelta.full}). A pointer table returning to inline
 * storage is sent whole.
 */
class TestFullUpdates {
  private static final TableIdentifier TABLE = TableIdentifier.of("ns", "t");
  private static final String WAREHOUSE = "mem:///wh";
  private static final String CATALOG = WAREHOUSE + "/catalog-full";
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "cat", Types.IntegerType.get()));
  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("cat").build();

  private final InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
  private int files = 0;

  private LogCatalog catalog(LocalCatalogMode mode) {
    return mode.catalog(io, WAREHOUSE, "full", Map.of());
  }

  private void append(Table table) {
    table.newFastAppend()
        .appendFile(DataFiles.builder(SPEC)
            .withPath("mem:///data/f" + files + ".parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withPartitionPath("cat=" + (files++ % 4))
            .build())
        .commit();
  }

  private static TableMetadata current(Table table) {
    return ((HasTableOperations) table).operations().current();
  }

  private static GenericStatisticsFile statistics(long snapshotId) {
    return new GenericStatisticsFile(
        snapshotId, "mem:///stats/" + snapshotId + ".puffin", 100, 10, List.of());
  }

  /** The full updates of the table the catalog's log holds, oldest first. */
  private List<ProtoCodec.UpdateTableInlineAction> fullUpdates() {
    List<ProtoCodec.UpdateTableInlineAction> full = new ArrayList<>();
    for (ProtoCodec.Transaction txn : CatalogTestSupport.decodeRecords(io.bytes(CATALOG))) {
      for (ProtoCodec.Action action : txn.actions()) {
        if (action instanceof ProtoCodec.UpdateTableInlineAction
            && ((ProtoCodec.UpdateTableInlineAction) action).fullMetadata != null) {
          full.add((ProtoCodec.UpdateTableInlineAction) action);
        }
      }
    }
    return full;
  }

  private static Set<Long> snapshotIds(ProtoCodec.UpdateTableInlineAction update) {
    TableMetadata sent = InlineMetadataCodecs.byTag(update.fullMetadataCodec)
        .decodeFull(update.fullMetadata, null);
    return sent.snapshots().stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
  }

  /** What {@code catalog} loads of the table is what a fresh catalog on the same log loads. */
  private void readsAsAFreshCatalog(LogCatalog catalog, LocalCatalogMode mode) {
    assertThat(TableViews.physical(catalog.loadTable(TABLE), true))
        .isEqualTo(TableViews.physical(catalog(mode).loadTable(TABLE), true));
  }

  @ParameterizedTest
  @EnumSource(names = {"APPEND_TM", "APPEND_ML", "MEMLOG_ML"})
  void aStatisticsChangeSendsOnlyTheSnapshotsItsRefsTarget(LocalCatalogMode mode) {
    LogCatalog catalog = catalog(mode);
    catalog.createNamespace(Namespace.of("ns"));
    Table table = catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    for (int i = 0; i < 8; i++) {
      append(table);
    }
    long head = table.currentSnapshot().snapshotId();
    table.updateStatistics().setStatistics(statistics(head)).commit();

    TableMetadata loaded = current(catalog(mode).loadTable(TABLE));
    assertThat(loaded.snapshots()).hasSize(8);
    assertThat(loaded.statisticsFiles()).extracting(f -> f.snapshotId()).containsExactly(head);
    readsAsAFreshCatalog(catalog, mode);
    if (!mode.sharedLog) {
      List<ProtoCodec.UpdateTableInlineAction> full = fullUpdates();
      assertThat(full).hasSize(1);
      assertThat(full.get(0).headOnly).isTrue();
      assertThat(snapshotIds(full.get(0))).containsExactly(head);
      assertThat(full.get(0).removedSnapshotIds).isEmpty();
    }
  }

  @ParameterizedTest
  @EnumSource(names = {"APPEND_TM", "APPEND_ML", "MEMLOG_ML"})
  void anExpiryWithAStatisticsChangeNamesWhatItDrops(LocalCatalogMode mode) {
    LogCatalog catalog = catalog(mode);
    catalog.createNamespace(Namespace.of("ns"));
    Table table = catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    for (int i = 0; i < 6; i++) {
      append(table);
    }
    List<Long> ids =
        current(table).snapshots().stream().map(Snapshot::snapshotId).collect(Collectors.toList());
    long head = table.currentSnapshot().snapshotId();
    Transaction txn = table.newTransaction();
    txn.expireSnapshots().expireSnapshotId(ids.get(0)).expireSnapshotId(ids.get(1))
        .cleanExpiredFiles(false).commit();
    txn.updateStatistics().setStatistics(statistics(head)).commit();
    txn.commitTransaction();

    TableMetadata loaded = current(catalog(mode).loadTable(TABLE));
    assertThat(loaded.snapshots()).extracting(Snapshot::snapshotId)
        .containsExactlyElementsOf(ids.subList(2, ids.size()));
    readsAsAFreshCatalog(catalog, mode);
    CatalogState state = catalog(mode).log().read().state();
    int tblId = state.tableId(TABLE);
    if (mode.inlineML) {
      assertThat(state.snapshotManifests(tblId).keySet()).doesNotContain(ids.get(0), ids.get(1));
    }
    if (!mode.sharedLog) {
      List<ProtoCodec.UpdateTableInlineAction> full = fullUpdates();
      assertThat(full).hasSize(1);
      assertThat(full.get(0).headOnly).isTrue();
      assertThat(snapshotIds(full.get(0))).containsExactly(head);
      assertThat(full.get(0).removedSnapshotIds).containsExactly(ids.get(0), ids.get(1));
    }
  }

  /**
   * A full update that adds a property, of a table whose inline snapshots it does not send (their
   * lists are inline, its head's a file, as a writer that does not inline lists leaves it): its
   * head goes as the whole table's would, properties in the order the whole table's encoding gives
   * them. Round sp1-r0 (a staged snapshot, which no ref targets): the whole table's encoding names
   * inline lists by sentinel through Iceberg's builder, which rebuilds the properties in a hash map
   * sized for them all; the head-only record, with no inline snapshot, kept the writer's map,
   * sized for the 11 it copied (16 buckets, against 32 for 12). For these keys (the round's) the
   * two orders differ, and so did the replays.
   */
  @ParameterizedTest
  @EnumSource(names = {"APPEND_ML", "MEMLOG_ML"})
  void aFullUpdateAddingAPropertyKeepsTheWholeTablesOrder(LocalCatalogMode mode) {
    LogCatalog ml = catalog(mode);
    ml.createNamespace(Namespace.of("ns"));
    Table table = ml.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    for (int i = 0; i < 2; i++) {
      append(table);
    }
    Table tm = catalog(mode.sharedLog ? LocalCatalogMode.MEMLOG_TM : LocalCatalogMode.APPEND_TM)
        .loadTable(TABLE);
    append(tm);
    tm.updateProperties()
        .set("commit.retry.max-wait-ms", "1")
        .set("commit.retry.min-wait-ms", "1")
        .set("write.metadata.delete-after-commit.enabled", "true")
        .set("write.metadata.previous-versions-max", "2")
        .set("commit.manifest.min-count-to-merge", "2")
        .set("fz.k2", "v4")
        .commit();
    assertThat(current(tm).properties()).hasSize(11);
    // A statistics change takes full mode; the property comes last, so the writer's builder adds
    // it to a map sized for the others
    Table again = ml.loadTable(TABLE);
    long head = again.currentSnapshot().snapshotId();
    Transaction txn = again.newTransaction();
    txn.updateStatistics().setStatistics(statistics(head)).commit();
    txn.updateProperties().set("fz.big", "v").commit();
    txn.commitTransaction();

    TableMetadata loaded = current(catalog(mode).loadTable(TABLE));
    assertThat(loaded.snapshots()).hasSize(3);
    assertThat(loaded.properties()).hasSize(12);
    assertThat(loaded.statisticsFiles()).hasSize(1);
    readsAsAFreshCatalog(ml, mode);
    if (!mode.sharedLog) {
      assertThat(fullUpdates()).last().satisfies(update -> assertThat(update.headOnly).isTrue());
    }
  }

  @org.junit.jupiter.api.Test
  void aHeadOnlyUpdateOfAPointerTableDoesNotApply() {
    LogCatalog pointer = catalog(LocalCatalogMode.APPEND);
    pointer.createNamespace(Namespace.of("ns"));
    Table table = pointer.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    append(table);
    byte[] head = InlineMetadataCodecs.byTag(InlineMetadataCodecs.TAG_JSON_GZIP)
        .encode(current(table));
    CatalogMutation mut = new CatalogMutation(pointer.log().read())
        .updateTableInline(TABLE, head, InlineMetadataCodecs.TAG_JSON_GZIP)
        .removingSnapshots(TABLE, new long[0]);
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> mut.commit(pointer.log()))
        .isInstanceOf(org.apache.iceberg.exceptions.CommitFailedException.class)
        .hasMessageContaining("head only on a pointer table");
    assertThat(pointer.log().read().state().isInlineTable(TABLE)).isFalse();
  }

  @ParameterizedTest
  @EnumSource(names = {"APPEND_TM", "APPEND_ML"})
  void aPointerTableReturnsToInlineStorageWhole(LocalCatalogMode mode) {
    LogCatalog pointer = catalog(LocalCatalogMode.APPEND);
    pointer.createNamespace(Namespace.of("ns"));
    Table table = pointer.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    for (int i = 0; i < 3; i++) {
      append(table);
    }
    LogCatalog inline = catalog(mode);
    Table returning = inline.loadTable(TABLE);
    append(returning);

    TableMetadata loaded = current(catalog(mode).loadTable(TABLE));
    assertThat(loaded.snapshots()).hasSize(4);
    readsAsAFreshCatalog(inline, mode);
    List<ProtoCodec.UpdateTableInlineAction> full = fullUpdates();
    assertThat(full).hasSize(1);
    assertThat(full.get(0).headOnly).isFalse();
    assertThat(snapshotIds(full.get(0))).hasSize(4);
    assertThat(full.get(0).removedSnapshotIds).isEmpty();
  }
}
