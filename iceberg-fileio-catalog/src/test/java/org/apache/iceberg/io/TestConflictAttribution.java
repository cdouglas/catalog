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
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.CatalogTransaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.types.Types;
import org.intentlog.LogView;
import org.intentlog.Replay;
import org.intentlog.fileio.AtomicFileLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Conflicts by their reason: a writer's catalog counts each conflict under the reason replay or its
 * commit loop gave ({@link CatalogLog.CommitCounts#conflictReasons}), and a replay of the log tells
 * a listener why it rejected each record that landed ({@link Replay.Rejections}). Each test races
 * one commit (B's) ahead of another (A's, staged on the table before B's landed).
 */
class TestConflictAttribution {
  private static final String WAREHOUSE = "mem:///w";
  private static final String CATALOG = WAREHOUSE + "/catalog-c";
  private static final TableIdentifier ID = TableIdentifier.of("ns", "t");
  private static final TableIdentifier OTHER = TableIdentifier.of("ns", "u");
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  private final InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();

  private LogCatalog writer(String encoding, Map<String, String> props) {
    return writer(LocalCatalogMode.APPEND_ML, encoding, props);
  }

  private LogCatalog writer(LocalCatalogMode mode, String encoding, Map<String, String> props) {
    Map<String, String> all = new TreeMap<>(props);
    all.put(LogCatalogProperties.CONFLICT_ENCODING, encoding);
    return mode.catalog(io, WAREHOUSE, "c", all);
  }

  /** A's and B's catalogs, under {@code encoding}, and the table with two snapshots. */
  private LogCatalog[] writers(String encoding) {
    return writers(encoding, Map.of());
  }

  private LogCatalog[] writers(String encoding, Map<String, String> props) {
    return writers(LocalCatalogMode.APPEND_ML, encoding, props);
  }

  private LogCatalog[] writers(
      LocalCatalogMode mode, String encoding, Map<String, String> props) {
    LogCatalog a = writer(mode, encoding, props);
    a.createNamespace(Namespace.of("ns"));
    Table table = a.createTable(ID, SCHEMA);
    table.newFastAppend().appendFile(file("f1")).commit();
    table.newFastAppend().appendFile(file("f2")).commit();
    return new LogCatalog[] {a, writer(mode, encoding, props)};
  }

  private static DataFile file(String name) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath("mem:///data/" + name + ".parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  private static Map<String, Long> reasons(LogCatalog catalog) {
    return catalog.log().counts().conflictReasons();
  }

  /** Why replay rejected each record that landed in the catalog's log, counted by reason. */
  private Map<String, Long> rejectedInTheLog() {
    LogView view;
    try (InputStream in = io.newInputFile(CATALOG).newStream()) {
      view = AtomicFileLog.decode(in.readAllBytes());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    Map<String, Long> reasons = new TreeMap<>();
    Replay.replay(CatalogStateMachine.INSTANCE, view.checkpoint().orElse(null), view.records(),
        null, (id, reason) -> reasons.merge(reason, 1L, Long::sum));
    return reasons;
  }

  /**
   * Runs {@code b} as A's first write of the catalog object is issued, after A's commit read the
   * table: its record then lands after B's.
   */
  private void beforeAsWrite(Runnable b) {
    AtomicBoolean raced = new AtomicBoolean();
    io.injectFaults((location, strategy) -> {
      if (location.equals(CATALOG) && raced.compareAndSet(false, true)) {
        b.run();
      }
      return InMemoryAtomicFileIO.Fault.NONE;
    });
  }

  @Test
  void underLamportAnyChangeIsTheVersion() {
    LogCatalog[] w = writers(LamportEncoding.NAME);
    beforeAsWrite(() -> w[1].loadTable(ID).newFastAppend().appendFile(file("b")).commit());
    w[0].loadTable(ID).newFastAppend().appendFile(file("a")).commit(); // Iceberg's retry commits

    assertThat(reasons(w[0])).containsExactly(Map.entry("whole: lamport", 1L));
    assertThat(rejectedInTheLog()).containsExactly(Map.entry("whole: lamport", 1L));
  }

  @Test
  void twoPropertyChangesShareOneConcern() {
    LogCatalog[] w = writers(ManifestWindowEncoding.NAME);
    beforeAsWrite(() -> w[1].loadTable(ID).updateProperties().set("k1", "b").commit());
    w[0].loadTable(ID).updateProperties().set("k2", "a").commit();

    assertThat(reasons(w[0])).containsExactly(Map.entry("concern: properties", 1L));
  }

  @Test
  void anAppendAfterAnExpiryReadsSnapshotRemoval() {
    LogCatalog[] w = writers(ManifestWindowEncoding.NAME);
    beforeAsWrite(() -> w[1].loadTable(ID).expireSnapshots().expireOlderThan(Long.MAX_VALUE)
        .retainLast(1).cleanExpiredFiles(false).commit());
    w[0].loadTable(ID).newFastAppend().appendFile(file("a")).commit();

    assertThat(reasons(w[0])).containsExactly(Map.entry("concern: snapshot removal", 1L));
  }

  /** The expiry A runs: all but main's last snapshot, which removes the table's first. */
  private static void expire(LogCatalog a) {
    a.loadTable(ID).expireSnapshots().expireOlderThan(Long.MAX_VALUE).retainLast(1)
        .cleanExpiredFiles(false).commit();
  }

  /**
   * An expiry lands after a commit that only added a snapshot, removing what it chose on the table
   * it read (errata D17): the first snapshot, where Iceberg's retry would remove the second too.
   */
  @Test
  void anExpiryAfterAnAppendLandsRemovingWhatItChose() {
    LogCatalog[] w = writers(ManifestWindowEncoding.NAME);
    long first = w[0].loadTable(ID).history().get(0).snapshotId();
    long second = w[0].loadTable(ID).currentSnapshot().snapshotId();
    beforeAsWrite(() -> w[1].loadTable(ID).newFastAppend().appendFile(file("b")).commit());
    expire(w[0]);

    assertThat(reasons(w[0])).isEmpty();
    Table now = w[1].loadTable(ID);
    assertThat(now.snapshot(first)).isNull();
    assertThat(now.snapshot(second)).isNotNull();
    assertThat(now.snapshots()).hasSize(2);
  }

  /** ... but not once a ref names a snapshot it removes: Iceberg's retry keeps that one. */
  @Test
  void anExpiryAfterATagOnASnapshotItRemovesHasTheWholeVersion() {
    LogCatalog[] w = writers(ManifestWindowEncoding.NAME);
    long first = w[0].loadTable(ID).history().get(0).snapshotId();
    beforeAsWrite(() -> w[1].loadTable(ID).manageSnapshots().createTag("t", first).commit());
    expire(w[0]);

    assertThat(reasons(w[0]))
        .containsExactly(Map.entry("whole: removes a snapshot a ref names", 1L));
    assertThat(w[1].loadTable(ID).snapshot(first)).isNotNull();
  }

  /**
   * ... nor after any ref's reset, though the ref names none of them: one may have named one and
   * moved on, and the judgment must hold on every later version (S2).
   */
  @Test
  void anExpiryAfterARefResetReadsEveryRefsResets() {
    LogCatalog[] w = writers(ManifestWindowEncoding.NAME);
    long second = w[0].loadTable(ID).currentSnapshot().snapshotId();
    beforeAsWrite(() -> w[1].loadTable(ID).manageSnapshots().createBranch("b", second).commit());
    expire(w[0]);

    assertThat(reasons(w[0])).containsExactly(Map.entry("ref: reset (any ref)", 1L));
  }

  /**
   * A merged expiry's writer cleans up files as Iceberg's RemoveSnapshots does, comparing the table
   * it read with a refresh after its commit, which holds the window: it reads the manifest lists of
   * every snapshot there, so it deletes nothing a commit since uses (here a merge append that
   * rewrote the table's manifests), and the table still plans.
   */
  @Test
  void aMergedExpiryCleansUpNothingTheWindowUses() {
    LogCatalog[] w = writers(ManifestWindowEncoding.NAME);
    w[0].loadTable(ID).updateProperties()
        .set(org.apache.iceberg.TableProperties.MANIFEST_MIN_MERGE_COUNT, "2").commit();
    beforeAsWrite(() -> w[1].loadTable(ID).newAppend().appendFile(file("b")).commit());
    w[0].loadTable(ID).expireSnapshots().expireOlderThan(Long.MAX_VALUE).retainLast(1)
        .cleanExpiredFiles(true).commit();

    assertThat(reasons(w[0])).isEmpty();
    Table now = writer(ManifestWindowEncoding.NAME, Map.of()).loadTable(ID);
    assertThat(now.snapshots()).hasSize(2);
    assertThat(now.currentSnapshot().allManifests(now.io())).as("the window merged them")
        .hasSize(1);
    java.util.Set<String> files = new java.util.TreeSet<>();
    try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
        now.newScan().planFiles()) {
      tasks.forEach(task -> files.add(task.file().location()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    assertThat(files).containsExactlyInAnyOrder(
        file("f1").location(), file("f2").location(), file("b").location());
  }

  /** With fileio.catalog.expiry.merge off, an expiry pins the version it read (an ablation). */
  @Test
  void anExpiryPinnedForAblationHasTheWholeVersion() {
    LogCatalog[] w = writers(ManifestWindowEncoding.NAME,
        Map.of(LogCatalogProperties.EXPIRY_MERGE, "false"));
    beforeAsWrite(() -> w[1].loadTable(ID).newFastAppend().appendFile(file("b")).commit());
    expire(w[0]);

    assertThat(reasons(w[0]))
        .containsExactly(Map.entry("whole: pinned: an expiry (fileio.catalog.expiry.merge off)", 1L));
  }

  @Test
  void aSchemaChangeIsCommittedOnce() {
    LogCatalog[] w = writers(ManifestWindowEncoding.NAME);
    beforeAsWrite(() -> w[1].loadTable(ID).newFastAppend().appendFile(file("b")).commit());
    assertThatThrownBy(
            () -> w[0].loadTable(ID).updateSchema().addColumn("c", Types.IntegerType.get())
                .commit())
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("commit once: a schema change");

    assertThat(reasons(w[0])).containsExactly(
        Map.entry("whole: commit once: a schema change", 1L));
  }

  /**
   * A catalog transaction's append to an inline table is judged where its record lands, as a
   * table commit's is (errata D12): another writer's append, landing as the transaction writes its
   * record, is no conflict.
   */
  @Test
  void aCatalogTransactionsAppendMerges() {
    LogCatalog[] w = writers(ManifestWindowEncoding.NAME);
    CatalogTransaction txn =
        w[0].createTransaction(CatalogTransaction.IsolationLevel.SNAPSHOT);
    txn.asCatalog().loadTable(ID).newFastAppend().appendFile(file("a")).commit();
    // B's append lands as A's transaction writes its record, after its checks
    beforeAsWrite(() -> w[1].loadTable(ID).newFastAppend().appendFile(file("b")).commit());
    txn.commitTransaction();

    assertThat(w[0].log().counts().conflicts()).isZero();
    assertThat(reasons(w[0])).isEmpty();
    assertThat(rejectedInTheLog()).isEmpty();
  }

  /**
   * A catalog transaction's schema change is pinned to the version its commit checked, as a table
   * commit's is: another writer's append, landing as the transaction writes its record, fails it,
   * attributed to the change Iceberg commits once.
   */
  @Test
  void aCatalogTransactionsSchemaChangeIsCommittedOnce() {
    LogCatalog[] w = writers(ManifestWindowEncoding.NAME);
    CatalogTransaction txn =
        w[0].createTransaction(CatalogTransaction.IsolationLevel.SNAPSHOT);
    txn.asCatalog().loadTable(ID).updateSchema().addColumn("c", Types.IntegerType.get()).commit();
    beforeAsWrite(() -> w[1].loadTable(ID).newFastAppend().appendFile(file("b")).commit());
    assertThatThrownBy(txn::commitTransaction).isInstanceOf(CommitFailedException.class);

    assertThat(reasons(w[0])).containsExactly(
        Map.entry("whole: commit once: a schema change", 1L));
    assertThat(rejectedInTheLog()).containsExactly(
        Map.entry("whole: commit once: a schema change", 1L));
  }

  /**
   * A serializable catalog transaction that scans table t and appends to table u, committing:
   * {@code b} runs as its record is written, after its checks.
   */
  private static void readTAppendU(LogCatalog a, Runnable b) {
    CatalogTransaction txn = a.createTransaction(CatalogTransaction.IsolationLevel.SERIALIZABLE);
    txn.asCatalog().loadTable(ID).newScan().planFiles();
    txn.asCatalog().loadTable(OTHER).newFastAppend().appendFile(file("a")).commit();
    b.run();
    txn.commitTransaction();
  }

  /** A change to {@code table} that leaves its main branch on the snapshot it names. */
  private static void leaveMain(Table table, String change) {
    switch (change) {
      case "property":
        table.updateProperties().set("k", "b").commit();
        break;
      case "expiry":
        table.expireSnapshots().expireOlderThan(Long.MAX_VALUE).retainLast(1)
            .cleanExpiredFiles(false).commit();
        break;
      case "branch":
        table.manageSnapshots().createBranch("b").commit();
        table.newFastAppend().appendFile(file("b")).toBranch("b").commit();
        break;
      default:
        throw new IllegalArgumentException(change);
    }
  }

  /**
   * A serializable catalog transaction's record names the refs it read of each table (a read
   * witness, errata D12), which the encoding judges by what changed of them: another writer's
   * change to a table it read that leaves main alone, landing as the transaction writes its
   * record, is no conflict.
   */
  @ParameterizedTest
  @ValueSource(strings = {"property", "expiry", "branch"})
  void aSerializableReadHoldsWhileTheRefsItReadDo(String change) {
    LogCatalog[] w = writers(ManifestWindowEncoding.NAME);
    w[0].createTable(OTHER, SCHEMA);
    readTAppendU(w[0], () -> beforeAsWrite(() -> leaveMain(w[1].loadTable(ID), change)));

    assertThat(w[0].log().counts().conflicts()).isZero();
    assertThat(rejectedInTheLog()).isEmpty();
    assertThat(w[1].loadTable(OTHER).currentSnapshot()).isNotNull();
  }

  /** Another writer's append to main of a table the transaction read fails it. */
  @Test
  void aSerializableReadFailsWhenARefItReadMoves() {
    LogCatalog[] w = writers(ManifestWindowEncoding.NAME);
    w[0].createTable(OTHER, SCHEMA);
    assertThatThrownBy(() -> readTAppendU(w[0], () -> beforeAsWrite(
            () -> w[1].loadTable(ID).newFastAppend().appendFile(file("b")).commit())))
        .isInstanceOf(CommitFailedException.class);

    assertThat(reasons(w[0])).containsExactly(Map.entry("ref: advanced", 1L));
    assertThat(rejectedInTheLog()).containsExactly(Map.entry("ref: advanced", 1L));
    assertThat(w[1].loadTable(OTHER).currentSnapshot()).isNull();
  }

  /**
   * A change to the table a serializable transaction scanned, between its scan and its commit: the
   * commit finds it on the read it stages on, writes nothing, and counts the conflict by what
   * changed (errata D12: an append to any partition and a rewrite, which a read judged by its
   * filter would tell apart; a reset; the table dropped or replaced).
   */
  @ParameterizedTest
  @CsvSource({
    "append, read: advanced by append",
    "rewrite, read: advanced by replace",
    "append and rewrite, read: advanced by append+replace",
    "rollback, read: reset",
    "drop, read: dropped",
    "replace, read: replaced"
  })
  void aReadChangedBeforeItsRecordIsCountedByWhatChanged(String change, String reason) {
    LogCatalog[] w = writers(ManifestWindowEncoding.NAME);
    w[0].createTable(OTHER, SCHEMA);
    assertThatThrownBy(() -> readTAppendU(w[0], () -> changeT(w[1], change)))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining(reason);

    assertThat(reasons(w[0])).containsExactly(Map.entry(reason, 1L));
    assertThat(w[0].log().counts().conflicts()).isEqualTo(1);
    assertThat(rejectedInTheLog()).as("nothing written").isEmpty();
    assertThat(w[1].loadTable(OTHER).currentSnapshot()).isNull();
  }

  /** A change to table t, which {@link #writers} made with two appends (files f1 and f2). */
  private static void changeT(LogCatalog catalog, String change) {
    Table table = catalog.loadTable(ID);
    switch (change) {
      case "append":
        table.newFastAppend().appendFile(file("b")).commit();
        break;
      case "rewrite":
        table.newRewrite().deleteFile(file("f1")).addFile(file("g1")).commit();
        break;
      case "append and rewrite":
        table.newFastAppend().appendFile(file("b")).commit();
        table.newRewrite().deleteFile(file("f1")).addFile(file("g1")).commit();
        break;
      case "rollback":
        table.manageSnapshots().rollbackTo(table.currentSnapshot().parentId()).commit();
        break;
      case "drop":
        catalog.dropTable(ID, false);
        break;
      case "replace":
        catalog.dropTable(ID, false);
        catalog.createTable(ID, SCHEMA);
        break;
      default:
        throw new IllegalArgumentException(change);
    }
  }

  /** Under {@code lamport} a read witness is the version: any change fails the transaction. */
  @Test
  void underLamportAReadWitnessIsTheVersion() {
    LogCatalog[] w = writers(LamportEncoding.NAME);
    w[0].createTable(OTHER, SCHEMA);
    assertThatThrownBy(() -> readTAppendU(w[0],
            () -> beforeAsWrite(() -> leaveMain(w[1].loadTable(ID), "property"))))
        .isInstanceOf(CommitFailedException.class);

    assertThat(reasons(w[0])).containsExactly(Map.entry("whole: lamport", 1L));
  }

  /**
   * A transaction read a table by its name, as its commit's check finds it: another writer's
   * rename of it fails the transaction, as the check would where the record lands. A rename is a
   * change the encoding does not see into, which stamps everything, the first stamp it reads
   * included.
   */
  @Test
  void aSerializableReadFailsWhenItsTableIsRenamed() {
    LogCatalog[] w = writers(ManifestWindowEncoding.NAME);
    w[0].createTable(OTHER, SCHEMA);
    assertThatThrownBy(() -> readTAppendU(w[0], () -> beforeAsWrite(
            () -> w[1].renameTable(ID, TableIdentifier.of("ns", "x")))))
        .isInstanceOf(CommitFailedException.class);

    assertThat(reasons(w[0])).containsExactly(Map.entry("ref: advanced", 1L));
  }

  /** Replay sees no ref of a pointer table: there only the version read holds the read. */
  @Test
  void aSerializableReadOfAPointerTableHoldsItsVersion() {
    LogCatalog[] w = writers(LocalCatalogMode.APPEND, ManifestWindowEncoding.NAME, Map.of());
    w[0].createTable(OTHER, SCHEMA);
    assertThatThrownBy(() -> readTAppendU(w[0],
            () -> beforeAsWrite(() -> leaveMain(w[1].loadTable(ID), "property"))))
        .isInstanceOf(CommitFailedException.class);

    assertThat(reasons(w[0])).containsExactly(Map.entry("whole: a pointer table", 1L));
  }

  /**
   * A read of a branch that had no snapshot holds the table's version, as {@code
   * TableRead.unchangedIn} does; so does every read with the witness off.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void aReadPinHoldsTheTablesVersion(boolean witness) {
    LogCatalog[] w = writers(ManifestWindowEncoding.NAME,
        Map.of(LogCatalogProperties.TXN_READ_WITNESS, String.valueOf(witness)));
    w[0].createTable(OTHER, SCHEMA);
    TableIdentifier read = witness ? OTHER : ID; // u has no snapshot yet
    CatalogTransaction txn = w[0].createTransaction(CatalogTransaction.IsolationLevel.SERIALIZABLE);
    txn.asCatalog().loadTable(read).newScan().planFiles();
    txn.asCatalog().loadTable(witness ? ID : OTHER).newFastAppend().appendFile(file("a"))
        .commit();
    beforeAsWrite(() -> w[1].loadTable(read).updateProperties().set("k", "b").commit());
    assertThatThrownBy(txn::commitTransaction).isInstanceOf(CommitFailedException.class);

    assertThat(reasons(w[0])).containsExactly(Map.entry("whole: a read pin", 1L));
  }

  @Test
  void aMergedAppendIsNoConflict() {
    LogCatalog[] w = writers(ManifestWindowEncoding.NAME);
    beforeAsWrite(() -> w[1].loadTable(ID).newFastAppend().appendFile(file("b")).commit());
    w[0].loadTable(ID).newFastAppend().appendFile(file("a")).commit();

    assertThat(w[0].log().counts().conflicts()).isZero();
    assertThat(reasons(w[0])).isEmpty();
    assertThat(rejectedInTheLog()).isEmpty();
  }

  /** {@code length} characters that no codec compresses below the inline update limit. */
  private static String incompressible(int length) {
    java.util.Random random = new java.util.Random(length);
    StringBuilder value = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      value.append((char) ('!' + random.nextInt(90)));
    }
    return value.toString();
  }

  /**
   * A record staged on an inline table that another writer evicts before the record lands (moves
   * it out of inline storage to a metadata file: here a property too big for the inline update
   * limit) does not apply, under any encoding: an eviction is a record, which changes the whole
   * table (every concern), and no record merges onto a pointer table. Replay decides it from the
   * log alone and never reads the metadata file the table now names (this test's replay has no
   * FileIO). Iceberg's retry then commits on the pointer table. docs/CHECKPOINT_PLAN.md keeps this
   * for pages: what moves a table out of the root is a record, and a record whose base predates it
   * conflicts.
   */
  @ParameterizedTest
  @ValueSource(strings = {LamportEncoding.NAME, LamportMinusPropertiesEncoding.NAME,
      ConcernVectorEncoding.NAME, BlindAppendEncoding.NAME, PartitionVectorEncoding.NAME,
      ManifestWindowEncoding.NAME})
  void aRecordOnATableEvictedBeforeItLandsDoesNotApply(String encoding) {
    LogCatalog[] w = writers(encoding, Map.of(LogCatalog.INLINE_MAX_UPDATE_SIZE, "2048"));
    assertThat(w[0].log().read().state().isInlineTable(ID)).isTrue();
    beforeAsWrite(() -> {
      w[1].loadTable(ID).updateProperties().set("big", incompressible(3000)).commit();
      assertThat(w[1].log().read().state().isInlineTable(ID)).as("evicted").isFalse();
    });
    w[0].loadTable(ID).newFastAppend().appendFile(file("a")).commit(); // Iceberg's retry commits

    String reason = encoding.equals(LamportEncoding.NAME)
        ? "whole: lamport" : "whole: a pointer table";
    assertThat(reasons(w[0])).containsExactly(Map.entry(reason, 1L));
    assertThat(rejectedInTheLog()).containsExactly(Map.entry(reason, 1L));
    Table table = writer(encoding, Map.of()).loadTable(ID);
    assertThat(table.snapshots()).hasSize(3);
    assertThat(table.currentSnapshot().addedDataFiles(io))
        .extracting(DataFile::location)
        .containsExactly(file("a").location());
    assertThat(table.properties()).containsKey("big");
  }
}
