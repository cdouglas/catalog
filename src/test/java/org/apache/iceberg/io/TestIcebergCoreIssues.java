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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
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
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Canaries for the Iceberg core bugs this project's fuzzers found, C1 to C8 in
 * docs/ICEBERG_ISSUES.md: one test each, on Iceberg's own InMemoryCatalog. For C1 to C7 it asserts
 * the bug is absent: upstream fixed C1, and the fork's core-fixes branch fixes the others, so
 * these pass against the fork. C8 is not fixed, and its test asserts the bug: when it fails, the
 * bug is fixed and fileio-catalog's workaround for it can go. A test of C1 to C7 that fails means
 * its bug is back: a rebase dropped the fork's fix, or a release changed the behavior. Before
 * dropping a core-fixes commit that upstream seems to have merged, check that its test here still
 * passes. Each test asserts only that the bug is absent, not the fork's way of fixing it, so a
 * different upstream fix passes too. The fuzzers
 * do not reach some of these reliably (C3, C6). The tests need only iceberg-core, so they also run
 * in Iceberg's own test tree.
 */
class TestIcebergCoreIssues {

  private static final String BACK = "Iceberg issue %s is back: see docs/ICEBERG_ISSUES.md";
  private static final TableIdentifier ID = TableIdentifier.of("ns", "t");
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  /**
   * C1. A delete whose commit merges manifests must not give an existing data file a new
   * first_row_id. Fixed upstream by apache/iceberg#16263, in 1.11.0.
   */
  @Test
  void aDeleteThatMergesManifestsKeepsRowIds() {
    Table table =
        catalog()
            .buildTable(ID, SCHEMA)
            .withProperty(TableProperties.FORMAT_VERSION, "3")
            .withProperty(TableProperties.MANIFEST_MIN_MERGE_COUNT, "2")
            .create();
    table.newFastAppend().appendFile(file("f5")).commit();
    table.newFastAppend().appendFile(file("f6")).commit();
    table.rewriteManifests().clusterBy(file -> "all").commit(); // one manifest of f5 and f6
    table.newFastAppend().appendFile(file("f8")).commit();
    table.newFastAppend().appendFile(file("f10")).commit();
    assertThat(rowIds(table)).containsEntry(path("f6"), 1L);

    table.newDelete().deleteFile(path("f5")).commit(); // merges the three manifests

    assertThat(rowIds(table).get(path("f6"))).as(BACK, "C1").isEqualTo(1L);
  }

  /**
   * C2. A commit that retries after another commit gave row ids to a manifest it carries must keep
   * them: SnapshotProducer, the manifest filter and merge managers and BaseRewriteManifests reused
   * copies of such a manifest from an earlier attempt, without first_row_id.
   */
  @Test
  void aRetriedCommitKeepsAManifestsRowIds() {
    InMemoryCatalog catalog = catalog();
    Table table = catalog.createTable(ID, SCHEMA);
    table.newFastAppend().appendFile(file("f0")).commit();
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();

    AppendFiles append = catalog.loadTable(ID).newFastAppend().appendFile(file("f1"));
    append.apply(); // the first attempt, while f0's manifest has no row ids
    catalog.loadTable(ID).newDelete().deleteFile(path("missing")).commit();
    assertThat(rowIds(catalog.loadTable(ID))).containsEntry(path("f0"), 0L);
    append.commit(); // retries on the new base

    assertThat(rowIds(catalog.loadTable(ID)).get(path("f0"))).as(BACK, "C2").isEqualTo(0L);
  }

  /**
   * C3. A replace transaction must not bring back a snapshot expired since it began, whose
   * manifest list the expiry deleted (BaseTransaction.commitReplaceTransaction).
   */
  @Test
  void aReplaceDoesNotBringBackASnapshotExpiredSinceItStarted() {
    InMemoryCatalog catalog = catalog();
    Table table = catalog.createTable(ID, SCHEMA);
    table.newFastAppend().appendFile(file("a")).commit();
    long first = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(file("b")).commit();

    Transaction replace = catalog.buildTable(ID, SCHEMA).replaceTransaction();
    replace.newFastAppend().appendFile(file("c")).commit();
    catalog.loadTable(ID).expireSnapshots().expireSnapshotId(first).commit();
    replace.commitTransaction();

    assertThat(catalog.loadTable(ID).snapshot(first)).as(BACK, "C3").isNull();
  }

  /** Commits through a handle loaded before another writer added a partition spec. */
  enum StaleOp {
    DELETE_BY_PATH,
    OVERWRITE_BY_FILTER,
    REPLACE_PARTITIONS,
    REWRITE_MANIFESTS
  }

  /**
   * C4. A commit through a handle loaded before another writer added a partition spec must not
   * fail on a manifest in the new spec: MergingSnapshotProducer's filter managers and
   * BaseRewriteManifests kept the specs of the handle's metadata.
   */
  @ParameterizedTest
  @EnumSource(StaleOp.class)
  void aStaleHandleCommitsOnAManifestInANewerSpec(StaleOp op) {
    InMemoryCatalog catalog = catalog();
    Table stale = catalog.createTable(ID, SCHEMA);
    stale.refresh(); // else its first use refreshes it, after the other writer's changes
    Table other = catalog.loadTable(ID);
    other.updateSpec().addField("id").commit();
    other.newFastAppend().appendFile(file(other.spec(), "a")).commit();

    assertThatCode(
            () -> {
              switch (op) {
                case DELETE_BY_PATH:
                  stale.newDelete().deleteFile(path("a")).commit();
                  break;
                case OVERWRITE_BY_FILTER:
                  stale
                      .newOverwrite()
                      .overwriteByRowFilter(Expressions.equal("id", 1))
                      .addFile(file("b"))
                      .commit();
                  break;
                case REPLACE_PARTITIONS:
                  stale.newReplacePartitions().addFile(file("b")).commit();
                  break;
                case REWRITE_MANIFESTS:
                  stale.rewriteManifests().clusterBy(file -> "all").commit();
                  break;
              }
            })
        .as(BACK, "C4")
        .doesNotThrowAnyException();
  }

  /**
   * C5. Rewriting the manifests of a table without a current snapshot must not fail, whether the
   * table never had one or a replace left it with none after the rewrite began.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void aRewriteOfManifestsRunsOnATableWithoutASnapshot(boolean replacedSince) {
    InMemoryCatalog catalog = catalog();
    Table table = catalog.createTable(ID, SCHEMA);
    if (replacedSince) {
      table.newFastAppend().appendFile(file("a")).commit();
    }
    org.apache.iceberg.RewriteManifests rewrite =
        table.rewriteManifests().clusterBy(file -> "all");
    if (replacedSince) {
      catalog.buildTable(ID, SCHEMA).replaceTransaction().commitTransaction();
    }

    assertThatCode(rewrite::commit).as(BACK, "C5").doesNotThrowAnyException();
  }

  /**
   * C6. Cherry-picking a snapshot again once its earlier pick has expired must not add its data
   * files a second time: once one of them has a deletion vector, the table could not be planned.
   */
  @Test
  void aCherryPickRepeatedAfterAnExpiryIsRefused() {
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
    table.expireSnapshots().expireSnapshotId(picked).commit();

    assertThatThrownBy(() -> table.manageSnapshots().cherrypick(stagedId).commit())
        .as(BACK, "C6")
        .isInstanceOf(ValidationException.class);
    assertThat(rowIds(table)) // plans
        .as(BACK, "C6")
        .containsOnlyKeys(path("x"), path("d"), path("y"), path("z"));
  }

  /**
   * C7. The all_* metadata tables must report the row ids of a data file in a manifest carried
   * across a v3 upgrade: BaseAllMetadataTableScan kept whichever copy of a manifest it read first,
   * and the lists from before the upgrade have no first_row_id.
   */
  @Test
  void allDataFilesHasTheRowIdsOfAFileCarriedAcrossAnUpgrade() {
    Table table = catalog().createTable(ID, SCHEMA);
    table.newFastAppend().appendFile(file("f0")).commit();
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();
    table.newFastAppend().appendFile(file("f1")).commit();
    assertThat(firstRowIds(table, MetadataTableType.FILES)).containsEntry(path("f0"), 1L);

    assertThat(firstRowIds(table, MetadataTableType.ALL_DATA_FILES))
        .as(BACK, "C7")
        .containsEntry(path("f0"), 1L);
  }

  /**
   * C8, not fixed. A deletion vector for a data file that is not live where it commits (a row
   * delta that does not validate its referenced files, say through a stale handle) stays in the
   * table; a later commit that adds that file, at a newer sequence number, leaves a table Iceberg
   * cannot plan. This test asserts the bug: when it fails, the bug is fixed, and the tolerance in
   * TestInlineDifferentialProperties.files can go.
   */
  @Test
  void aDeletionVectorOlderThanItsDataFileLeavesATableItCannotPlan() {
    Table table =
        catalog().buildTable(ID, SCHEMA).withProperty(TableProperties.FORMAT_VERSION, "3").create();
    table.newFastAppend().appendFile(file("x")).commit();
    DataFile staged = file("d");
    table.newFastAppend().appendFile(staged).stageOnly().commit();
    long stagedId = Iterables.getLast(table.snapshots()).snapshotId();
    table.newRowDelta().addDeletes(dv(table, staged)).commit(); // d is not live on main
    table.manageSnapshots().cherrypick(stagedId).commit(); // adds d, newer than its DV

    assertThatThrownBy(() -> rowIds(table))
        .as("Iceberg issue C8 no longer reproduces: see docs/ICEBERG_ISSUES.md")
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("DV data sequence number");
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
    return file(PartitionSpec.unpartitioned(), name);
  }

  private static DataFile file(PartitionSpec spec, String name) {
    DataFiles.Builder file =
        DataFiles.builder(spec).withPath(path(name)).withFileSizeInBytes(10).withRecordCount(1);
    return spec.isPartitioned() ? file.withPartitionPath("id=1").build() : file.build();
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

  /** The first_row_id column of a files metadata table, by file_path. */
  private static Map<String, Long> firstRowIds(Table table, MetadataTableType type) {
    Table files = MetadataTableUtils.createMetadataTableInstance(table, type);
    List<Types.NestedField> fields = files.schema().columns();
    int path = fields.indexOf(files.schema().findField("file_path"));
    int rowId = fields.indexOf(files.schema().findField("first_row_id"));
    Map<String, Long> ids = new TreeMap<>();
    try (CloseableIterable<FileScanTask> tasks = files.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        try (CloseableIterable<StructLike> rows = task.asDataTask().rows()) {
          rows.forEach(row -> ids.put(row.get(path, String.class), row.get(rowId, Long.class)));
        }
      }
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
    return ids;
  }
}
