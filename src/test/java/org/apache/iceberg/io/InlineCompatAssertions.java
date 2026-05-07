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

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

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
      FileIOCatalog catalog,
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
}
