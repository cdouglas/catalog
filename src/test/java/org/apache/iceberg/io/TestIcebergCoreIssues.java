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

import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Issues in Iceberg core, without this catalog: each test reproduces one on Iceberg's own
 * InMemoryCatalog and asserts what Iceberg does now; its Javadoc says what Iceberg should do. The
 * issues are in docs/ICEBERG_ISSUES.md, with the workarounds in this project's tests that each
 * one needs. A test that fails after a rebase onto a newer Iceberg means that release fixed or
 * changed its issue: remove the issue's workarounds, then make the test assert the fixed
 * behavior. The tests need only iceberg-core, so they also run in Iceberg's own test tree, as
 * the repro for a report or a fix upstream.
 */
class TestIcebergCoreIssues {

  private static final String FIXED =
      "Iceberg issue %s no longer reproduces: see docs/ICEBERG_ISSUES.md";
  private static final TableIdentifier ID = TableIdentifier.of("ns", "t");
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  /**
   * C2. A commit that retries after another commit gave row ids to a manifest it carries gives
   * that manifest a second range. SnapshotProducer caches the manifests it carries, with their
   * metadata, by ManifestFile equality (the path), and the retry reuses its copy from before the
   * other commit assigned the manifest's first_row_id. A data file's row ids should not change.
   */
  @Test
  void aRetriedCommitGivesAManifestASecondRowIdRange() {
    InMemoryCatalog catalog = catalog();
    Table table = catalog.createTable(ID, SCHEMA);
    table.newFastAppend().appendFile(file("f0")).commit();
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();

    AppendFiles append = catalog.loadTable(ID).newFastAppend().appendFile(file("f1"));
    append.apply(); // the first attempt, while f0's manifest has no row ids
    catalog.loadTable(ID).newDelete().deleteFile(path("missing")).commit();
    assertThat(rowIds(catalog.loadTable(ID))).containsEntry(path("f0"), 0L);
    append.commit(); // retries on the new base

    assertThat(rowIds(catalog.loadTable(ID)).get(path("f0")))
        .as(FIXED, "C2")
        .isEqualTo(2L); // should be 0
  }

  /**
   * C3. A replace transaction commits the table it built at its start, snapshots included, over
   * whatever changed since (BaseTransaction.commitReplaceTransaction), so a snapshot expired in
   * between comes back, though the expiry deleted its manifest list. A replace means to win over
   * concurrent changes, but it should not restore what an expiry removed.
   */
  @Test
  void aReplaceBringsBackASnapshotExpiredSinceItStarted() {
    InMemoryCatalog catalog = catalog();
    Table table = catalog.createTable(ID, SCHEMA);
    table.newFastAppend().appendFile(file("a")).commit();
    long first = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(file("b")).commit();

    Transaction replace = catalog.buildTable(ID, SCHEMA).replaceTransaction();
    replace.newFastAppend().appendFile(file("c")).commit();
    catalog.loadTable(ID).expireSnapshots().expireSnapshotId(first).commit();
    replace.commitTransaction();

    Snapshot expired = catalog.loadTable(ID).snapshot(first);
    assertThat(expired).as(FIXED, "C3").isNotNull(); // should be null
    assertThatThrownBy(() -> expired.allManifests(table.io())) // its manifest list is gone
        .isInstanceOf(NotFoundException.class);
  }

  /**
   * C6. Cherry-picking a snapshot again once its earlier pick has expired adds its data files a
   * second time, at a new sequence number, and once one of them has a deletion vector the table
   * cannot be planned. CherryPickOperation finds an earlier pick by the source-snapshot-id of the
   * current snapshot's ancestors, and an expiry removes the ancestor that has it. The pick should
   * be refused as a duplicate.
   */
  @Test
  void aCherryPickRepeatedAfterAnExpiryLeavesATableItCannotPlan() {
    Table table =
        catalog().buildTable(ID, SCHEMA).withProperty(TableProperties.FORMAT_VERSION, "3").create();
    table.newFastAppend().appendFile(file("x")).commit();
    DataFile staged = file("d");
    table.newFastAppend().appendFile(staged).stageOnly().commit();
    long stagedId = Iterables.getLast(table.snapshots()).snapshotId();
    table.newFastAppend().appendFile(file("y")).commit();
    table.manageSnapshots().cherrypick(stagedId).commit();
    long picked = table.currentSnapshot().snapshotId();
    table.newRowDelta().addDeletes(dv(table, staged)).commit();
    table.newFastAppend().appendFile(file("z")).commit();
    assertThatThrownBy(() -> table.manageSnapshots().cherrypick(stagedId).commit())
        .hasMessageContaining("already picked");

    table.expireSnapshots().expireSnapshotId(picked).commit();
    table.manageSnapshots().cherrypick(stagedId).commit(); // should be refused

    assertThatThrownBy(() -> rowIds(table))
        .as(FIXED, "C6")
        .hasMessageContaining("DV data sequence number");
  }

  private static InMemoryCatalog catalog() {
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize(
        "c", Map.of(CatalogProperties.WAREHOUSE_LOCATION, "mem:///" + UUID.randomUUID()));
    catalog.createNamespace(Namespace.of("ns"));
    return catalog;
  }

  private static String path(String name) {
    return "mem:///data/" + name + ".parquet";
  }

  private static DataFile file(String name) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(path(name))
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  /** A deletion vector of one row of {@code data}. */
  private static DeleteFile dv(Table table, DataFile data) {
    return FileMetadata.deleteFileBuilder(table.spec())
        .ofPositionDeletes()
        .withFormat(FileFormat.PUFFIN)
        .withPath(data.location() + ".puffin")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .withReferencedDataFile(data.location())
        .withContentOffset(4)
        .withContentSizeInBytes(6)
        .build();
  }

  /** The first row id of each data file a scan of the table's main branch plans. */
  private static Map<String, Long> rowIds(Table table) {
    Map<String, Long> ids = new TreeMap<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      tasks.forEach(task -> ids.put(task.file().location(), task.file().firstRowId()));
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
    return ids;
  }
}
