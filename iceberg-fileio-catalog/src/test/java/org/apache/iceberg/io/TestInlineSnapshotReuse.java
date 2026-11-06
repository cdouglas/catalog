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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.InlineSnapshot;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * {@link LogCatalog.InlineSnapshots}: a load of an inline-ML table reuses the InlineSnapshots an
 * earlier load made for the snapshots whose lists did not change, and reads what a fresh catalog's
 * load of the same log reads, also after a pool entry was replaced (by an equal copy: a record that
 * sends again a manifest the pool holds; a commit that would change one writes a metadata file,
 * LogCatalog.restatesAManifest) and after a compaction (a decoded state shares no list with the
 * last).
 */
class TestInlineSnapshotReuse {
  private static final TableIdentifier TABLE = TableIdentifier.of("ns", "t");
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "cat", Types.IntegerType.get()));
  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("cat").build();

  private final InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
  private int files = 0;

  private LogCatalog catalog(LocalCatalogMode mode) {
    return mode.catalog(io, "mem:///wh", "reuse", Map.of());
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

  /** What {@code catalog} loads of the table is what a fresh catalog on the same log loads. */
  private void readsAsAFreshCatalog(LogCatalog catalog, LocalCatalogMode mode) {
    assertThat(TableViews.physical(catalog.loadTable(TABLE), true))
        .isEqualTo(TableViews.physical(catalog(mode).loadTable(TABLE), true));
  }

  @Test
  void aLoadReusesTheSnapshotsAnEarlierLoadMade() {
    LogCatalog catalog = catalog(LocalCatalogMode.APPEND_ML);
    catalog.createNamespace(Namespace.of("ns"));
    Table table = catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    for (int i = 0; i < 5; i++) {
      append(table);
    }
    TableMetadata first = current(catalog.loadTable(TABLE));
    append(table);
    TableMetadata second = current(catalog.loadTable(TABLE));

    assertThat(second.snapshots()).hasSize(first.snapshots().size() + 1);
    for (Snapshot made : first.snapshots()) {
      assertThat(made).isInstanceOf(InlineSnapshot.class);
      assertThat(second.snapshot(made.snapshotId())).isSameAs(made);
    }
    assertThat(second.currentSnapshot()).isInstanceOf(InlineSnapshot.class);
    assertThat(first.snapshot(second.currentSnapshot().snapshotId())).isNull();
    readsAsAFreshCatalog(catalog, LocalCatalogMode.APPEND_ML);
  }

  /**
   * A lazy load ({@code fileio.catalog.load.lazy}, the default) makes a snapshot's list only when
   * its manifests are asked for, and reads as a load that makes them all.
   */
  @Test
  void aLazyLoadMakesAListOnlyWhenAskedFor() {
    LogCatalog catalog = catalog(LocalCatalogMode.APPEND_ML);
    catalog.createNamespace(Namespace.of("ns"));
    Table table = catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    for (int i = 0; i < 4; i++) {
      append(table);
    }
    Table loaded = catalog(LocalCatalogMode.APPEND_ML).loadTable(TABLE);
    Snapshot oldest = current(loaded).snapshots().get(0);
    assertThat(oldest).isInstanceOf(InlineSnapshot.class);
    assertThat(oldest.toString()).contains("not yet made");
    assertThat(oldest.allManifests(loaded.io())).hasSize(1);
    assertThat(oldest.toString()).doesNotContain("not yet made");

    LogCatalog eager =
        LocalCatalogMode.APPEND_ML.catalog(
            io, "mem:///wh", "reuse", Map.of(LogCatalog.LOAD_LAZY, "false"));
    Snapshot made = current(eager.loadTable(TABLE)).snapshots().get(0);
    assertThat(made.toString()).doesNotContain("not yet made");
    assertThat(TableViews.physical(eager.loadTable(TABLE), true))
        .isEqualTo(TableViews.physical(catalog(LocalCatalogMode.APPEND_ML).loadTable(TABLE), true));
  }

  /**
   * A lazy load holds the snapshots its refs target until something asks for another, and then
   * every one, as a whole load holds them; the supplier serializes what it made.
   */
  @ParameterizedTest
  @EnumSource(names = {"APPEND_TM", "APPEND_ML"})
  void aLazyLoadHoldsItsRefsTargetsUntilAskedForMore(LocalCatalogMode mode) throws Exception {
    LogCatalog catalog = catalog(mode);
    catalog.createNamespace(Namespace.of("ns"));
    Table table = catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    for (int i = 0; i < 5; i++) {
      append(table);
    }
    long tagged = current(table).snapshots().get(1).snapshotId();
    table.manageSnapshots().createTag("t", tagged).commit();

    TableMetadata lazy = current(catalog(mode).loadTable(TABLE));
    assertThat(lazy)
        .extracting("snapshots")
        .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.list(Snapshot.class))
        .extracting(Snapshot::snapshotId)
        .containsExactly(tagged, lazy.currentSnapshot().snapshotId());

    // The supplier (TableMetadata itself does not serialize): a copy holds what it made
    Object unserializable = new Object();
    LogCatalog.LazySnapshots supplier = new LogCatalog.LazySnapshots(() -> {
      assertThat(unserializable).isNotNull();
      return List.of(lazy.currentSnapshot());
    });
    java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
    try (java.io.ObjectOutputStream out = new java.io.ObjectOutputStream(bytes)) {
      out.writeObject(supplier);
    }
    Object copy;
    try (java.io.ObjectInputStream in = new java.io.ObjectInputStream(
        new java.io.ByteArrayInputStream(bytes.toByteArray()))) {
      copy = in.readObject();
    }
    assertThat(((LogCatalog.LazySnapshots) copy).get())
        .extracting(Snapshot::snapshotId)
        .containsExactly(lazy.currentSnapshot().snapshotId());

    TableMetadata whole = current(LocalCatalogMode.valueOf(mode.name())
        .catalog(io, "mem:///wh", "reuse", Map.of(LogCatalog.LOAD_LAZY, "false"))
        .loadTable(TABLE));
    assertThat(lazy.snapshots()).hasSize(5);
    assertThat(org.apache.iceberg.TableMetadataParser.toJson(lazy))
        .isEqualTo(org.apache.iceberg.TableMetadataParser.toJson(whole));
  }

  @Test
  void aPathAddedAgainKeepsItsEntryAndTheListsMadeFromIt() {
    LogCatalog catalog = catalog(LocalCatalogMode.APPEND_ML);
    catalog.createNamespace(Namespace.of("ns"));
    Table table = catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    for (int i = 0; i < 4; i++) {
      append(table);
    }
    CatalogState state = catalog.log().read().state();
    int tblId = state.tableId(TABLE);
    java.util.List<Snapshot> parsed = current(catalog.loadTable(TABLE)).snapshots();
    LogCatalog.InlineSnapshots made = new LogCatalog.InlineSnapshots();
    java.util.List<Snapshot> first = made.wrap(state, tblId, parsed, true);
    java.util.List<Snapshot> again = made.wrap(state, tblId, parsed, true);
    for (int i = 0; i < first.size(); i++) {
      assertThat(again.get(i)).isSameAs(first.get(i));
    }

    // An equal entry for a path the pool holds keeps the pool's entry, so every list made from it
    // still holds: a pooled path's entry never changes
    ManifestFile entry = state.manifestPool(tblId).values().iterator().next();
    CatalogState readded =
        state.toBuilder()
            .addManifestToPool(tblId, org.apache.iceberg.GenericManifestFile.copyOf(entry).build())
            .build();
    assertThat(readded.manifestPool(tblId).get(entry.path())).isSameAs(entry);
    java.util.List<Snapshot> kept = made.wrap(readded, tblId, parsed, true);
    for (int i = 0; i < first.size(); i++) {
      assertThat(kept.get(i)).isSameAs(first.get(i));
    }

    // Another entry for the path would change every list naming it: replay does not apply a record
    // that sends one (TestPooledManifests), and under the replay check reaching the pool is a bug
    ManifestFile other =
        org.apache.iceberg.GenericManifestFile.copyOf(entry)
            .withSnapshotId(entry.snapshotId() + 1)
            .build();
    assertThat(state.toBuilder().restatesPooledManifest(tblId, entry.path(), () -> other))
        .isTrue();
    assertThat(state.toBuilder().restatesPooledManifest(tblId, entry.path(), () -> entry))
        .isFalse();
    assertThatThrownBy(() -> state.toBuilder().addManifestToPool(tblId, other))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("restates pooled manifest " + entry.path());
  }

  @ParameterizedTest
  @EnumSource(value = LocalCatalogMode.class, names = {"APPEND_ML", "CAS_ML", "COMPACT_ML"})
  void everyLoadReadsAsAFreshCatalogsLoad(LocalCatalogMode mode) {
    LogCatalog catalog = catalog(mode);
    catalog.createNamespace(Namespace.of("ns"));
    Table table = catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    for (int i = 0; i < 8; i++) {
      append(table);
      if (i == 3) {
        table.expireSnapshots().retainLast(2).expireOlderThan(System.currentTimeMillis() + 1)
            .cleanExpiredFiles(false).commit();
      }
      if (i == 5) {
        table.newRewrite()
            .validateFromSnapshot(table.currentSnapshot().snapshotId())
            .rewriteFiles(
                java.util.Set.of(table.currentSnapshot().addedDataFiles(table.io()).iterator().next()),
                java.util.Set.of(DataFiles.builder(SPEC)
                    .withPath("mem:///data/r" + i + ".parquet")
                    .withFileSizeInBytes(10)
                    .withRecordCount(1)
                    .withPartitionPath("cat=" + ((files - 1) % 4))
                    .build()))
            .commit();
      }
      readsAsAFreshCatalog(catalog, mode);
    }
  }
}
