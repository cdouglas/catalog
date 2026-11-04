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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.InlineDeltaCodec.AddManifestUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.AddSnapshotUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.DeltaUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.RemoveManifestUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.SetSnapshotRefUpdate;
import org.apache.iceberg.types.Types;
import org.intentlog.LogView;
import org.intentlog.Replay;
import org.intentlog.Replica;
import org.intentlog.fileio.AtomicFileLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A pooled manifest's entry never changes (SPEC_ML.md, AddManifestDelta; docs/CHECKPOINT_PLAN.md
 * decision 3): every inline list that names a path shares the pool's entry for it. A table update
 * that adds a pooled path with an entry that differs in any field does not apply, whether a delta
 * or a full update carries it, and one that adds it with an equal entry applies and leaves the
 * pool's entry as it was. Writers never send the first (an update that would change a pooled entry
 * evicts the table): these records are made by hand.
 */
class TestPooledManifests {
  private static final String CATALOG = "mem:///w/catalog-c";
  private final TableIdentifier id = TableIdentifier.of("ns", "t");
  private final InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();

  private TableMetadata base;
  private long head;
  private CatalogLog log;
  private Replica<CatalogState> read;
  private int tblId;
  private String prefix;
  private String moved; // the manifest the head lists last: the first append's
  private ManifestFile pooled;

  @BeforeEach
  void twoAppends() {
    LogCatalog catalog = LocalCatalogMode.APPEND_ML.catalog(io, "mem:///w", "c", Map.of());
    catalog.createNamespace(Namespace.of("ns"));
    Table table = catalog.createTable(
        id, new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));
    table.newFastAppend().appendFile(file("f1")).commit();
    table.newFastAppend().appendFile(file("f2")).commit();
    base = ((HasTableOperations) catalog.loadTable(id)).operations().current();
    head = base.currentSnapshot().snapshotId();

    log = CatalogTestSupport.log(io, CATALOG);
    read = log.read();
    CatalogState state = read.state();
    tblId = state.tableId(id);
    prefix = state.manifestListPrefix(tblId);
    List<String> list = state.snapshotManifests(tblId).get(head);
    assertThat(list).hasSize(2);
    moved = list.get(1);
    pooled = state.manifestPool(tblId).get(moved);
    assertThat(pooled.snapshotId()).isNotEqualTo(head);
  }

  @Test
  void aDeltaThatAddsAPooledManifestWithAnEqualEntryApplies() {
    ManifestFile equal = GenericManifestFile.copyOf(pooled).build();
    Replica<CatalogState> landed = commitDelta(moveToFront(equal));

    assertThat(landed.state().snapshotManifests(tblId).get(33L)).containsExactly(moved, other());
    assertThat(landed.state().manifestPool(tblId).get(moved))
        .as("the pool keeps its entry")
        .isSameAs(read.state().manifestPool(tblId).get(moved));
    assertThat(rejectedInTheLog()).isEmpty();
    CheckpointRoundTrip.assertEveryWrite(io);
  }

  @Test
  void aDeltaThatRestatesAPooledManifestDoesNotApply() {
    // the same move, its entry claiming the new snapshot added the manifest
    ManifestFile restated = GenericManifestFile.copyOf(pooled).withSnapshotId(33L).build();
    assertThatThrownBy(() -> commitDelta(moveToFront(restated)))
        .isInstanceOf(CommitFailedException.class);

    assertThat(log.counts().conflictReasons())
        .containsExactly(Map.entry(ProtoCodec.POOL_RESTATED, 1L));
    assertThat(rejectedInTheLog()).containsExactly(Map.entry(ProtoCodec.POOL_RESTATED, 1L));
    CatalogState after = CatalogTestSupport.log(io, CATALOG).read().state();
    assertThat(after.snapshotManifests(tblId)).doesNotContainKey(33L);
    assertThat(CatalogState.Builder.samePoolEntry(after.manifestPool(tblId).get(moved), pooled))
        .isTrue();
  }

  @Test
  void aFullUpdateThatRestatesAPooledManifestDoesNotApply() {
    // the table as it is, its manifest updates giving the head's manifest another entry
    ManifestFile restated = GenericManifestFile.copyOf(pooled).withSnapshotId(head).build();
    byte[] manifests = InlineDeltaCodec.encodeDelta(
        List.of(new AddManifestUpdate(head, restated, prefix, 0)), base.lastUpdatedMillis() + 1);
    CatalogState state = read.state();
    assertThatThrownBy(
            () ->
                new CatalogMutation(read)
                    .updateTableInlineWithManifests(
                        id, state.inlineMetadata(tblId), state.inlineMetadataCodec(tblId),
                        manifests)
                    .commit(log))
        .isInstanceOf(CommitFailedException.class);

    assertThat(rejectedInTheLog()).containsExactly(Map.entry(ProtoCodec.POOL_RESTATED, 1L));
    assertThat(CatalogTestSupport.log(io, CATALOG).read().state().manifestPool(tblId).get(moved)
            .snapshotId())
        .isEqualTo(pooled.snapshotId());
  }

  /** The head's other manifest, the second append's. */
  private String other() {
    return read.state().snapshotManifests(tblId).get(head).get(0);
  }

  /**
   * A new snapshot 33, main's new head, whose list is its parent's with the parent's last manifest
   * moved to the front, sent as {@code entry}.
   */
  private List<DeltaUpdate> moveToFront(ManifestFile entry) {
    Map<String, String> summary = new HashMap<>();
    summary.put("operation", "replace");
    List<DeltaUpdate> updates = new ArrayList<>();
    updates.add(new AddSnapshotUpdate(
        33L, "", summary, false, List.of(), 0L, null, null, head, null, null, null));
    updates.add(new SetSnapshotRefUpdate("main", 33L, "branch", 0, 0L, 0L));
    updates.add(new RemoveManifestUpdate(33L, new int[] {1}));
    updates.add(new AddManifestUpdate(33L, entry, prefix, 0));
    return updates;
  }

  private Replica<CatalogState> commitDelta(List<DeltaUpdate> updates) {
    byte[] delta = InlineDeltaCodec.encodeDelta(updates, base.lastUpdatedMillis() + 1, base);
    return new CatalogMutation(read).updateTableInlineDelta(id, delta).commit(log);
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
        null, (record, reason) -> reasons.merge(reason, 1L, Long::sum));
    return reasons;
  }

  private static org.apache.iceberg.DataFile file(String name) {
    return org.apache.iceberg.DataFiles.builder(org.apache.iceberg.PartitionSpec.unpartitioned())
        .withPath("mem:///data/" + name + ".parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }
}
