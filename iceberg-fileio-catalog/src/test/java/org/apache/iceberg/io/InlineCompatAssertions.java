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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assumptions;

/**
 * Shared inline-equivalent test bodies for the {@code *InlineTM} cloud subclasses (errata T4).
 *
 * <p>The upstream {@code CatalogTests} cases that we override here either resolve {@code inline://}
 * pseudo-URIs as filesystem paths (failing because no real file exists) or assert via accessors
 * that don't translate to inline metadata. Where the underlying semantic is identical and only the
 * accessor differs, we replicate the assertion verbatim with a different accessor and call it from
 * each cloud's {@code *InlineTM} subclass — keeping cloud-specific setup local while sharing the
 * inline-mode contract.
 */
public final class InlineCompatAssertions {
  private InlineCompatAssertions() {}

  /**
   * Inline-equivalent of upstream
   * {@link org.apache.iceberg.catalog.CatalogTests#testMetadataFileLocationsRemovalAfterCommit}:
   * bounded metadata-history retention is pure {@code TableMetadata.Builder} logic (the
   * {@code addPreviousFile} / {@code METADATA_PREVIOUS_VERSIONS_MAX} pair), so the assertion shape
   * is the same as upstream — but the accessor reads {@code metadata.previousFiles().size()}
   * directly instead of routing through {@code ReachableFileUtil.metadataFileLocations}, which
   * would resolve the synthetic {@code inline://} URIs as missing filesystem paths.
   *
   * <p>The upstream test's set size is {@code 1 + previousFiles().size()} (current location +
   * history); our assertion drops the {@code +1} since we're inspecting {@code previousFiles}
   * directly. Bounded retention semantics ({@code METADATA_DELETE_AFTER_COMMIT_ENABLED} +
   * {@code METADATA_PREVIOUS_VERSIONS_MAX}) are preserved verbatim.
   */
  public static void runMetadataFileLocationsRemovalAfterCommit(
      LogCatalog catalog,
      TableIdentifier tableId,
      Namespace ns,
      Schema schema,
      boolean createNamespace) {
    if (createNamespace) {
      catalog.createNamespace(ns);
    }

    Table table = catalog.buildTable(tableId, schema).create();
    table.updateSchema().addColumn("a", Types.LongType.get()).commit();
    table.updateSchema().addColumn("b", Types.LongType.get()).commit();
    table.updateSchema().addColumn("c", Types.LongType.get()).commit();

    TableMetadata meta = ((BaseTable) catalog.loadTable(tableId)).operations().current();
    assertThat(meta.previousFiles()).hasSize(3);

    int maxPreviousVersionsToKeep = 2;
    table
        .updateProperties()
        .set(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true")
        .set(
            TableProperties.METADATA_PREVIOUS_VERSIONS_MAX,
            Integer.toString(maxPreviousVersionsToKeep))
        .commit();

    meta = ((BaseTable) catalog.loadTable(tableId)).operations().current();
    assertThat(meta.previousFiles()).hasSize(maxPreviousVersionsToKeep);

    for (int i = 1; i <= 5; i++) {
      table.updateSchema().addColumn("d" + i, Types.LongType.get()).commit();
      meta = ((BaseTable) catalog.loadTable(tableId)).operations().current();
      assertThat(meta.previousFiles()).hasSize(maxPreviousVersionsToKeep);
    }

    maxPreviousVersionsToKeep = 4;
    table
        .updateProperties()
        .set(
            TableProperties.METADATA_PREVIOUS_VERSIONS_MAX,
            Integer.toString(maxPreviousVersionsToKeep))
        .commit();

    for (int i = 1; i <= 10; i++) {
      table.updateSchema().addColumn("e" + i, Types.LongType.get()).commit();
      meta = ((BaseTable) catalog.loadTable(tableId)).operations().current();
      assertThat(meta.previousFiles()).hasSize(maxPreviousVersionsToKeep);
    }
  }

  /**
   * FileIO equivalent of upstream {@link
   * org.apache.iceberg.catalog.CatalogTests#testLoadTableWithMissingMetadataFile}, which moves the
   * table's metadata file on the local file system: this deletes it through the table's FileIO, as
   * Iceberg's {@code TestInMemoryCatalog} does, and puts it back after. An inline table has no
   * metadata file to lose.
   */
  /**
   * CatalogTests' testRegisterTable for an inline table, whose metadata location before its drop is
   * the catalog's own ({@code inline://}) and names no file: a drop that keeps an inline table's
   * files writes it to a metadata file first ({@link LogCatalogProperties#DROP_MATERIALIZE}), the
   * newest in its metadata directory, which is what a user registers to bring back any Iceberg
   * catalog's dropped table. The registered table must be the dropped one, every manifest list a
   * file, and take commits.
   */
  public static void runRegisterTableAfterDrop(
      LogCatalog catalog,
      TableIdentifier tableId,
      Schema schema,
      org.apache.iceberg.PartitionSpec spec,
      org.apache.iceberg.SortOrder order,
      java.util.List<org.apache.iceberg.DataFile> files,
      boolean createNamespace) {
    if (createNamespace) {
      catalog.createNamespace(tableId.namespace());
    }
    java.util.Map<String, String> properties =
        java.util.Map.of("user", "someone", "created-at", "2023-01-15T00:00:01");
    catalog.buildTable(tableId, schema).withPartitionSpec(spec).withSortOrder(order)
        .withProperties(properties).create();
    Table original = catalog.loadTable(tableId);
    original.newFastAppend().appendFile(files.get(0)).commit();
    original.newFastAppend().appendFile(files.get(1)).commit();
    original.newDelete().deleteFile(files.get(0)).commit();
    original.newFastAppend().appendFile(files.get(2)).commit();
    original = catalog.loadTable(tableId);
    assertThat(((BaseTable) original).operations().current().metadataFileLocation())
        .startsWith("inline:");

    catalog.dropTable(tableId, false /* do not purge */);

    String newest = null;
    long newestAt = Long.MIN_VALUE;
    for (FileInfo info : ((SupportsPrefixOperations) catalog.io())
        .listPrefix(original.location() + "/metadata/")) {
      if (info.location().endsWith(".metadata.json") && info.createdAtMillis() >= newestAt) {
        newest = info.location();
        newestAt = info.createdAtMillis();
      }
    }
    assertThat(newest).as("the drop wrote the table to a metadata file").isNotNull();
    Table registered = catalog.registerTable(tableId, newest);

    assertThat(catalog.tableExists(tableId)).isTrue();
    assertThat(registered.properties()).containsAllEntriesOf(properties);
    assertThat(registered.schema().asStruct()).isEqualTo(original.schema().asStruct());
    assertThat(registered.specs()).isEqualTo(original.specs());
    assertThat(registered.sortOrders()).isEqualTo(original.sortOrders());
    assertThat(registered.currentSnapshot().snapshotId())
        .isEqualTo(original.currentSnapshot().snapshotId());
    java.util.List<Long> registeredIds = new java.util.ArrayList<>();
    registered.snapshots().forEach(s -> registeredIds.add(s.snapshotId()));
    java.util.List<Long> originalIds = new java.util.ArrayList<>();
    original.snapshots().forEach(s -> originalIds.add(s.snapshotId()));
    assertThat(registeredIds).isEqualTo(originalIds);
    assertThat(registered.history()).isEqualTo(original.history());
    for (org.apache.iceberg.Snapshot s : registered.snapshots()) {
      assertThat(s.manifestListLocation()).doesNotStartWith("inline:");
      assertThat(catalog.io().newInputFile(s.manifestListLocation()).exists()).isTrue();
    }
    assertThat(TableViews.physical(registered, false)).isEqualTo(TableViews.physical(original, false));
    registered.newFastAppend().appendFile(files.get(0)).commit();
    assertThat(catalog.dropTable(tableId)).isTrue();
  }

  public static void runLoadTableWithMissingMetadataFile(
      LogCatalog catalog, TableIdentifier tableId, Schema schema, boolean createNamespace)
      throws IOException {
    if (createNamespace) {
      catalog.createNamespace(tableId.namespace());
    }

    catalog.buildTable(tableId, schema).create();
    assertThat(catalog.tableExists(tableId)).as("Table should exist").isTrue();

    Table table = catalog.loadTable(tableId);
    String metadataFileLocation =
        ((BaseTable) table).operations().current().metadataFileLocation();
    Assumptions.assumeFalse(
        metadataFileLocation.startsWith("inline:"), "an inline table has no metadata file");
    byte[] metadata;
    try (InputStream in = table.io().newInputFile(metadataFileLocation).newStream()) {
      metadata = in.readAllBytes();
    }
    table.io().deleteFile(metadataFileLocation);
    try {
      assertThatThrownBy(() -> catalog.loadTable(tableId))
          .isInstanceOf(NotFoundException.class)
          .hasMessageContaining(metadataFileLocation);
    } finally {
      try (OutputStream out =
          table.io().newOutputFile(metadataFileLocation).createOrOverwrite()) {
        out.write(metadata);
      }
    }
  }
}
