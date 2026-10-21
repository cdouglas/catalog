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
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
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
 * Canaries for the Iceberg core bugs this project's fuzzers and reviews found, C1 to C23 in
 * docs/ICEBERG_ISSUES.md: one test each, on Iceberg's own InMemoryCatalog, that asserts the bug is
 * absent. Upstream fixed C1, and the fork's core-fixes branch fixes the others, so every test
 * passes against the fork. A test that fails means its bug is back: a rebase dropped the fork's
 * fix, or a release changed the behavior. Before dropping a core-fixes commit that upstream seems
 * to have merged, check that its test here still passes. Each test asserts only that the bug is
 * absent, not the fork's way of fixing it, so a different upstream fix passes too. The fuzzers do
 * not reach some of these reliably (C3, C6). The tests need only iceberg-core, so they also run in
 * Iceberg's own test tree.
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
   * C8. A commit must not add a data file that a live deletion vector names: a DV committed for a
   * file not live where it commits (a row delta that does not validate its referenced files)
   * would be older than the file once a commit added it, and the table could not be planned.
   */
  @Test
  void aDataFileADeletionVectorNamesIsNotAddedAgain() {
    Table table =
        catalog().buildTable(ID, SCHEMA).withProperty(TableProperties.FORMAT_VERSION, "3").create();
    table.newFastAppend().appendFile(file("x")).commit();
    DataFile staged = file("d");
    table.newFastAppend().appendFile(staged).stageOnly().commit();
    long stagedId = Iterables.getLast(table.snapshots()).snapshotId();
    table.newRowDelta().addDeletes(dv(table, staged)).commit(); // d is not live on main

    assertThatThrownBy(() -> table.manageSnapshots().cherrypick(stagedId).commit())
        .as(BACK, "C8")
        .isInstanceOf(ValidationException.class);
    assertThat(rowIds(table)).as(BACK, "C8").containsOnlyKeys(path("x")); // plans
  }

  /**
   * C9. A create-or-replace transaction that began without its table must not replace a table
   * another writer created since with the one it built: the replacement had the transaction's new
   * UUID and none of the created table's history (BaseTransaction.commitReplaceTransaction).
   */
  @Test
  void aCreateOrReplaceKeepsATableCreatedSinceItBegan() {
    InMemoryCatalog catalog = catalog();
    Transaction createOrReplace = catalog.buildTable(ID, SCHEMA).createOrReplaceTransaction();
    createOrReplace.newFastAppend().appendFile(file("a")).commit();
    Table created = catalog.createTable(ID, SCHEMA);
    created.newFastAppend().appendFile(file("b")).commit();
    long appended = created.currentSnapshot().snapshotId();

    try {
      createOrReplace.commitTransaction();
    } catch (AlreadyExistsException e) {
      // failing, as a create does, keeps the created table too
    }

    Table table = catalog.loadTable(ID);
    assertThat(table.uuid()).as(BACK, "C9").isEqualTo(created.uuid());
    assertThat(table.snapshot(appended)).as(BACK, "C9").isNotNull();
  }

  /**
   * C10. An expiry whose commit retries after a conflict must work out what to remove on the
   * metadata it retries on: RemoveSnapshots kept the snapshots its earlier attempt removed, and
   * failed when another writer had expired them since (or tagged one).
   */
  @Test
  void anExpiryRetriesAfterAConcurrentExpiry() throws InterruptedException {
    InMemoryCatalog catalog = catalog();
    Table table = catalog.createTable(ID, SCHEMA);
    table.newFastAppend().appendFile(file("a")).commit();
    table.newFastAppend().appendFile(file("b")).commit();
    table.newFastAppend().appendFile(file("c")).commit();
    long current = table.currentSnapshot().snapshotId();
    Thread.sleep(2);
    long now = System.currentTimeMillis();

    boolean[] raced = {false};
    Table racing = racing(table, () -> {
      raced[0] = true; // another writer expires the same snapshots first
      catalog.loadTable(ID).expireSnapshots().expireOlderThan(now).retainLast(1).commit();
    });

    assertThatCode(() -> racing.expireSnapshots().expireOlderThan(now).retainLast(1).commit())
        .as(BACK, "C10")
        .doesNotThrowAnyException();
    assertThat(raced[0]).isTrue();
    assertThat(catalog.loadTable(ID).snapshots())
        .extracting(org.apache.iceberg.Snapshot::snapshotId)
        .containsExactly(current);
  }

  /**
   * C11. An append must not commit files of a partition spec another writer removed since they were
   * added, by an expiry that cleans expired metadata: the append failed with a NullPointerException
   * or, when the spec went between its attempts, committed a manifest of it that no scan could read.
   */
  @Test
  void anAppendNeverCommitsFilesOfARemovedSpec() {
    InMemoryCatalog catalog = catalog();
    TableIdentifier before = TableIdentifier.of("ns", "before");
    AppendFiles append = catalog.createTable(before, SCHEMA).newFastAppend().appendFile(file("a"));
    removeUnpartitionedSpec(catalog, before);

    assertThat(catchThrowable(append::commit))
        .as(BACK, "C11")
        .isNotInstanceOf(NullPointerException.class);

    TableIdentifier between = TableIdentifier.of("ns", "between");
    Table racing =
        racing(catalog.createTable(between, SCHEMA), () -> removeUnpartitionedSpec(catalog, between));
    catchThrowable(() -> racing.newFastAppend().appendFile(file("b")).commit());

    Table table = catalog.loadTable(between);
    assertThat(table.specs()).doesNotContainKey(0);
    for (org.apache.iceberg.Snapshot snapshot : table.snapshots()) {
      assertThat(snapshot.allManifests(table.io()))
          .extracting(ManifestFile::partitionSpecId)
          .as(BACK, "C11")
          .allMatch(table.specs()::containsKey);
    }
  }

  /**
   * C12. An expiry must follow the GC and retention settings of the metadata it commits on:
   * RemoveSnapshots read them when it was created, so it deleted files after another writer
   * disabled GC, and expired snapshots that retention raised since keeps.
   */
  @Test
  void anExpiryFollowsSettingsChangedSinceItWasCreated() throws InterruptedException {
    InMemoryCatalog catalog = catalog();
    Table table = catalog.createTable(ID, SCHEMA);
    table.newFastAppend().appendFile(file("a")).commit();
    table.newFastAppend().appendFile(file("b")).commit();
    Thread.sleep(2);
    long now = System.currentTimeMillis();

    List<String> deleted = new ArrayList<>();
    ExpireSnapshots expire = table.expireSnapshots().expireOlderThan(now).deleteWith(deleted::add);
    catalog.loadTable(ID).updateProperties().set(TableProperties.GC_ENABLED, "false").commit();
    catchThrowable(expire::commit);
    assertThat(deleted).as(BACK, "C12").isEmpty();

    catalog.loadTable(ID).updateProperties().set(TableProperties.GC_ENABLED, "true").commit();
    ExpireSnapshots retaining = catalog.loadTable(ID).expireSnapshots().expireOlderThan(now);
    catalog.loadTable(ID).updateProperties().set(TableProperties.MIN_SNAPSHOTS_TO_KEEP, "2")
        .commit();
    retaining.commit();
    assertThat(catalog.loadTable(ID).snapshots()).as(BACK, "C12").hasSize(2);
  }

  /**
   * C13. An expiry that committed must not report a failure because its cleanup of the expired
   * files failed: RemoveSnapshots let a failure to read the table back after the commit escape
   * commit().
   */
  @Test
  void anExpiryThatCommittedDoesNotFailOnItsCleanup() throws InterruptedException {
    InMemoryCatalog catalog = catalog();
    Table table = catalog.createTable(ID, SCHEMA);
    table.newFastAppend().appendFile(file("a")).commit();
    table.newFastAppend().appendFile(file("b")).commit();
    Thread.sleep(2);
    long now = System.currentTimeMillis();

    boolean[] committed = {false};
    Table failingRefresh = new BaseTable(new Forwarding(table) {
      @Override
      public void commit(TableMetadata base, TableMetadata metadata) {
        super.commit(base, metadata);
        committed[0] = true;
      }

      @Override
      public TableMetadata refresh() {
        if (committed[0]) {
          throw new UncheckedIOException(new IOException("injected: refresh failed"));
        }
        return super.refresh();
      }
    }, table.name());

    Throwable failure = catchThrowable(() -> failingRefresh.expireSnapshots()
        .expireOlderThan(now).commit());
    assertThat(committed[0]).isTrue();
    assertThat(failure).as(BACK, "C13").isNull();
  }

  /**
   * C14. A cherry-pick of a snapshot that another writer expired after the pick was set up must
   * fail cleanly: CherryPickOperation kept the snapshot it looked up then, and failed with a
   * NullPointerException on the refreshed table that no longer had it.
   */
  @Test
  void aCherryPickOfASnapshotExpiredSincePickedFailsCleanly() {
    InMemoryCatalog catalog = catalog();
    Table table = catalog.createTable(ID, SCHEMA);
    table.newFastAppend().appendFile(file("a")).commit();
    table.newFastAppend().appendFile(file("b")).stageOnly().commit();
    long staged = Iterables.getOnlyElement(Iterables.filter(table.snapshots(),
        s -> s.snapshotId() != table.currentSnapshot().snapshotId())).snapshotId();
    org.apache.iceberg.ManageSnapshots pick = table.manageSnapshots().cherrypick(staged);

    catalog.loadTable(ID).expireSnapshots().expireSnapshotId(staged).commit();

    assertThat(catchThrowable(pick::commit))
        .as(BACK, "C14")
        .isNotInstanceOf(NullPointerException.class);
  }

  /**
   * C15. An expiry in a transaction must not delete the files of the snapshots it removes before
   * the transaction commits: a retry after another writer tagged one of them kept the snapshot,
   * whose manifest list was gone.
   */
  @Test
  void aTransactionsExpiryDeletesFilesOnlyOnceItCommits() throws InterruptedException {
    InMemoryCatalog catalog = catalog();
    Table table = catalog.createTable(ID, SCHEMA);
    table.newFastAppend().appendFile(file("a")).commit();
    long tagged = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(file("b")).commit();
    Thread.sleep(2);
    long now = System.currentTimeMillis();
    Transaction txn = table.newTransaction();
    txn.expireSnapshots().expireOlderThan(now).retainLast(1).commit();

    catalog.loadTable(ID).manageSnapshots().createTag("tag", tagged).commit();
    txn.commitTransaction();

    Table reloaded = catalog.loadTable(ID);
    assertThat(reloaded.snapshot(tagged)).isNotNull();
    assertThat(catchThrowable(() -> reloaded.snapshot(tagged).allManifests(reloaded.io())))
        .as(BACK, "C15")
        .isNull();
  }

  /**
   * C16. A rollback must not move main forward past another writer's rollback to an older
   * snapshot: rollbackTo checked that its target was an ancestor only when it was called.
   */
  @Test
  void aRollbackDoesNotUndoAConcurrentRollbackPastIt() {
    InMemoryCatalog catalog = catalog();
    Table table = catalog.createTable(ID, SCHEMA);
    table.newFastAppend().appendFile(file("a")).commit();
    long first = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(file("b")).commit();
    long second = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(file("c")).commit();
    org.apache.iceberg.ManageSnapshots rollback = table.manageSnapshots().rollbackTo(second);

    catalog.loadTable(ID).manageSnapshots().rollbackTo(first).commit();
    catchThrowable(rollback::commit);

    assertThat(catalog.loadTable(ID).currentSnapshot().snapshotId())
        .as(BACK, "C16")
        .isEqualTo(first);
  }

  /**
   * C17. A location update must retry on the table as it is after another writer's commit:
   * SetLocation refreshed once, before its retries, and committed on that metadata each time.
   */
  @Test
  void aLocationUpdateRetriesOnARefreshedTable() {
    InMemoryCatalog catalog = catalog();
    Table table = catalog.createTable(ID, SCHEMA);
    Table racing =
        racing(table, () -> catalog.loadTable(ID).updateProperties().set("k", "v").commit());

    assertThatCode(() -> racing.updateLocation().setLocation(table.location() + "-moved").commit())
        .as(BACK, "C17")
        .doesNotThrowAnyException();
  }

  /**
   * C18. A replace of partitions whose spec another writer removed must fail cleanly: it looked the
   * spec up before C11's check, and failed with a NullPointerException.
   */
  @Test
  void aReplaceOfPartitionsOfARemovedSpecFailsCleanly() {
    InMemoryCatalog catalog = catalog();
    Table table = catalog.createTable(ID, SCHEMA);
    org.apache.iceberg.ReplacePartitions replace =
        table.newReplacePartitions().addFile(file("a"));

    removeUnpartitionedSpec(catalog, ID);

    assertThat(catchThrowable(replace::commit))
        .as(BACK, "C18")
        .isNotInstanceOf(NullPointerException.class);
  }

  /**
   * C19. A sort order replaced after another writer renamed its column must not fail with a
   * NullPointerException: the metadata builder looked the column up by its old name.
   */
  @Test
  void aSortOrderReplacedAfterARenameDoesNotFail() {
    InMemoryCatalog catalog = catalog();
    Table table = catalog.createTable(ID, SCHEMA);
    org.apache.iceberg.ReplaceSortOrder order = table.replaceSortOrder().asc("id");

    catalog.loadTable(ID).updateSchema().renameColumn("id", "key").commit();

    Throwable failure = catchThrowable(order::commit);
    assertThat(failure instanceof NullPointerException).as(BACK, "C19").isFalse();
  }

  /**
   * C20. A row delta whose starting snapshot another writer expired must fail with a validation
   * failure, not an IllegalArgumentException as for a bad argument.
   */
  @Test
  void aRowDeltaFromAnExpiredSnapshotFailsValidation() {
    InMemoryCatalog catalog = catalog();
    Table table =
        catalog.buildTable(ID, SCHEMA).withProperty(TableProperties.FORMAT_VERSION, "2").create();
    table.newFastAppend().appendFile(file("a")).commit();
    long read = table.currentSnapshot().snapshotId();
    org.apache.iceberg.RowDelta delta =
        table
            .newRowDelta()
            .addRows(file("b"))
            .validateFromSnapshot(read)
            .validateNoConflictingDataFiles();

    Table other = catalog.loadTable(ID);
    other.newFastAppend().appendFile(file("c")).commit();
    other.expireSnapshots().expireSnapshotId(read).commit();

    assertThat(catchThrowable(delta::commit))
        .as(BACK, "C20")
        .isInstanceOf(ValidationException.class);
  }

  /**
   * C21. Updates applied to a table's newer metadata, as a REST server applies them, must not lose
   * a commit to a branch they rename, nor remove a spec that a commit to main since uses: the
   * requirements derived for a ref removal named nothing, and those for a spec removal skipped
   * main.
   */
  @Test
  void removalsAppliedToNewerMetadataRequireTheirRefs() {
    InMemoryCatalog catalog = catalog();
    Table table = catalog.createTable(ID, SCHEMA);
    table.newFastAppend().appendFile(file("a")).commit();
    long first = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createBranch("b", first).commit();
    TableMetadata base = ((HasTableOperations) table).operations().current();
    TableMetadata renamed =
        TableMetadata.buildFrom(base)
            .removeRef("b")
            .setRef("c", org.apache.iceberg.SnapshotRef.branchBuilder(first).build())
            .build();

    Table other = catalog.loadTable(ID);
    other.newFastAppend().appendFile(file("b")).toBranch("b").commit();
    long branchCommit = other.refs().get("b").snapshotId();
    catchThrowable(() -> applyAsRestServer(catalog, base, renamed.changes()));

    assertThat(catalog.loadTable(ID).refs().values())
        .as(BACK, "C21")
        .extracting(org.apache.iceberg.SnapshotRef::snapshotId)
        .contains(branchCommit);

    // a spec removal, computed before the table's first commit, which adds a file of the spec
    TableIdentifier id = TableIdentifier.of("ns", "specs");
    Table specs = catalog.createTable(id, SCHEMA);
    specs.updateSpec().addField("id").commit();
    TableMetadata unpartitioned = ((HasTableOperations) specs).operations().current();
    catalog.loadTable(id).newFastAppend().appendFile(file("c")).commit();
    catchThrowable(
        () ->
            applyAsRestServer(
                catalog,
                id,
                unpartitioned,
                List.of(new org.apache.iceberg.MetadataUpdate.RemovePartitionSpecs(Set.of(0)))));

    assertThat(catalog.loadTable(id).specs()).as(BACK, "C21").containsKey(0);
  }

  /**
   * C22. A snapshot removal applied to a table's newer metadata, as a REST server applies an
   * expiry's updates, must not drop a ref another writer set on the snapshot since: removing a
   * snapshot removed the refs that point to it.
   */
  @Test
  void aRemovalAppliedToNewerMetadataKeepsAConcurrentTag() {
    InMemoryCatalog catalog = catalog();
    Table table = catalog.createTable(ID, SCHEMA);
    table.newFastAppend().appendFile(file("a")).commit();
    long expired = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(file("b")).commit();
    TableMetadata base = ((HasTableOperations) table).operations().current();

    catalog.loadTable(ID).manageSnapshots().createTag("tag", expired).commit();
    catchThrowable(
        () ->
            applyAsRestServer(
                catalog,
                base,
                List.of(new org.apache.iceberg.MetadataUpdate.RemoveSnapshots(Set.of(expired)))));

    Table reloaded = catalog.loadTable(ID);
    assertThat(reloaded.refs()).as(BACK, "C22").containsKey("tag");
    assertThat(reloaded.snapshot(expired)).as(BACK, "C22").isNotNull();
  }

  /**
   * C23. A replace transaction must not drop what another writer committed since it began: it
   * committed the replacement it built then, putting another branch back where it was and dropping
   * the other writer's snapshot.
   */
  @Test
  void aReplaceKeepsWhatOtherWritersCommittedSinceItBegan() {
    InMemoryCatalog catalog = catalog();
    Table table = catalog.createTable(ID, SCHEMA);
    table.newFastAppend().appendFile(file("a")).commit();
    long first = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createBranch("b", first).commit();
    Transaction replace = catalog.buildTable(ID, SCHEMA).replaceTransaction();
    replace.newFastAppend().appendFile(file("c")).commit();

    Table other = catalog.loadTable(ID);
    other.newFastAppend().appendFile(file("b")).toBranch("b").commit();
    long branchCommit = other.refs().get("b").snapshotId();
    catchThrowable(replace::commitTransaction);

    Table after = catalog.loadTable(ID);
    assertThat(after.snapshot(branchCommit)).as(BACK, "C23").isNotNull();
    assertThat(after.refs().get("b").snapshotId()).as(BACK, "C23").isEqualTo(branchCommit);
  }

  /** Applies updates computed on {@code base}, as a REST server applies a client's commit. */
  private static void applyAsRestServer(
      InMemoryCatalog catalog, TableMetadata base, List<org.apache.iceberg.MetadataUpdate> updates) {
    applyAsRestServer(catalog, ID, base, updates);
  }

  private static void applyAsRestServer(
      InMemoryCatalog catalog,
      TableIdentifier id,
      TableMetadata base,
      List<org.apache.iceberg.MetadataUpdate> updates) {
    org.apache.iceberg.rest.CatalogHandlers.updateTable(
        catalog,
        id,
        org.apache.iceberg.rest.requests.UpdateTableRequest.create(
            id, org.apache.iceberg.UpdateRequirements.forUpdateTable(base, updates), updates));
  }

  /** As another writer, partitions an unpartitioned table and expires its unpartitioned spec. */
  private static void removeUnpartitionedSpec(InMemoryCatalog catalog, TableIdentifier id) {
    Table table = catalog.loadTable(id);
    table.updateSpec().addField("id").commit();
    table.expireSnapshots().cleanExpiredMetadata(true).commit();
  }

  /**
   * {@code table}, through operations that run {@code otherWriter} just before their first commit,
   * whose attempt it then invalidates.
   */
  private static Table racing(Table table, Runnable otherWriter) {
    boolean[] raced = {false};
    return new BaseTable(new Forwarding(table) {
      @Override
      public void commit(TableMetadata base, TableMetadata metadata) {
        if (!raced[0]) {
          raced[0] = true;
          otherWriter.run();
        }
        super.commit(base, metadata);
      }
    }, table.name());
  }

  /** The operations of {@code table}, for a test to override some of. */
  private static class Forwarding implements TableOperations {
    private final TableOperations ops;

    Forwarding(Table table) {
      this.ops = ((HasTableOperations) table).operations();
    }

    @Override
    public TableMetadata current() {
      return ops.current();
    }

    @Override
    public TableMetadata refresh() {
      return ops.refresh();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      ops.commit(base, metadata);
    }

    @Override
    public org.apache.iceberg.io.FileIO io() {
      return ops.io();
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return ops.metadataFileLocation(fileName);
    }

    @Override
    public org.apache.iceberg.io.LocationProvider locationProvider() {
      return ops.locationProvider();
    }
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
