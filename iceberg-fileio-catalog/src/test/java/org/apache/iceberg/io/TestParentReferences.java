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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.InlineDeltaCodec.AddSnapshotUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.DeltaUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.RemoveManifestUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.SetSnapshotRefUpdate;
import org.apache.iceberg.types.Types;
import org.intentlog.Replica;
import org.junit.jupiter.api.Test;

/**
 * What a delta names by reference to a new snapshot's parent (catalog.proto CompactSummary and
 * RemoveManifestDelta): the parent's extra summary entries the snapshot keeps, and the manifests it
 * removes, by their indices in the parent's list. A writer names only what its base holds; replay
 * takes them where the record lands and refuses a record whose parent is not there.
 */
class TestParentReferences {
  private static final String PREFIX = "s3://bucket/db/tbl/metadata/";
  private static final String VERSION = "Apache Iceberg 1.12.0 (commit 0123456789abcdef)";
  private final TableIdentifier id = TableIdentifier.of("ns", "t");
  private final Schema schema =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  @Test
  void removalsByIndexRoundTrip() {
    byte[] bytes =
        InlineDeltaCodec.encodeDelta(
            List.of(new RemoveManifestUpdate(7L, new int[] {0, 3, 200})), TestInlineDelta.TEST_TS);
    RemoveManifestUpdate decoded =
        (RemoveManifestUpdate) InlineDeltaCodec.decodeDelta(bytes).updates.get(0);
    assertThat(decoded.snapshotId).isEqualTo(7L);
    assertThat(decoded.manifestPathSuffix).isNull();
    assertThat(decoded.parentIndices).containsExactly(0, 3, 200);
    assertThat(decoded.removed(List.of("a", "b", "c", "d"))).isNull();
    assertThat(decoded.removed(null)).isNull();
    List<String> parent = new ArrayList<>();
    for (int i = 0; i <= 200; i++) {
      parent.add("m" + i);
    }
    assertThat(decoded.removed(parent)).containsExactly("m0", "m3", "m200");
  }

  @Test
  void aWriterNamesRemovalsByTheirIndicesInItsParentsList() {
    List<String> parent = List.of(PREFIX + "a.avro", PREFIX + "b.avro", PREFIX + "c.avro");
    List<DeltaUpdate> delta = new ArrayList<>();
    InlineDeltaCodec.attachManifestDelta(
        delta, 9L, List.of(), null, List.of(PREFIX + "c.avro", PREFIX + "a.avro"), parent, PREFIX);
    assertThat(delta).hasSize(1);
    assertThat(((RemoveManifestUpdate) delta.get(0)).parentIndices).containsExactly(0, 2);

    // a path the list lacks is named by path, as is every removal without a list
    List<DeltaUpdate> byPath = new ArrayList<>();
    InlineDeltaCodec.attachManifestDelta(
        byPath, 9L, List.of(), null, List.of(PREFIX + "x.avro"), parent, PREFIX);
    assertThat(((RemoveManifestUpdate) byPath.get(0)).manifestPathSuffix).isEqualTo("x.avro");
  }

  @Test
  void aSummaryStatesWhatDiffersFromItsParents() {
    Map<String, String> parentSummary = new HashMap<>();
    parentSummary.put("operation", "append");
    parentSummary.put("added-data-files", "1");
    parentSummary.put("iceberg-version", VERSION);
    parentSummary.put("engine-name", "spark");
    parentSummary.put("app-id", "local-1");
    TableMetadata base = withSnapshot(TestInlineDelta.baseMetadata(), 11L, null, parentSummary);

    Map<String, String> summary = new HashMap<>();
    summary.put("operation", "overwrite");
    summary.put("added-data-files", "2");
    summary.put("iceberg-version", VERSION);
    summary.put("app-id", "local-2");
    summary.put("manifests-kept", "40");
    AddSnapshotUpdate child =
        new AddSnapshotUpdate(22L, "", summary, 0L, null, null, 11L, null, null);

    byte[] stated = InlineDeltaCodec.encodeDelta(List.of(child), TestInlineDelta.TEST_TS, base);
    byte[] whole = InlineDeltaCodec.encodeDelta(List.of(child), TestInlineDelta.TEST_TS);
    assertThat(stated.length).isLessThan(whole.length - VERSION.length());
    assertThat(new String(stated, StandardCharsets.ISO_8859_1))
        .doesNotContain("iceberg-version")
        .doesNotContain("manifests-kept");

    AddSnapshotUpdate decoded = decodeAdd(stated);
    assertThat(decoded.inheritsParentExtra).isTrue();
    assertThat(decoded.droppedExtra).containsExactly("engine-name");
    assertThat(decoded.summary).doesNotContainKey("iceberg-version").containsEntry("app-id", "local-2");
    assertThat(decoded.summary(MetadataView.of(base))).isEqualTo(summary);
    // without its parent there is nothing to take its entries from
    assertThat(decoded.summary(MetadataView.of(TestInlineDelta.baseMetadata()))).isNull();

    // a parent the base lacks: the summary is stated whole
    AddSnapshotUpdate orphan =
        decodeAdd(
            InlineDeltaCodec.encodeDelta(
                List.of(new AddSnapshotUpdate(23L, "", summary, 0L, null, null, 99L, null, null)),
                TestInlineDelta.TEST_TS, base));
    assertThat(orphan.inheritsParentExtra).isFalse();
    assertThat(orphan.summary).isEqualTo(summary);
  }

  @Test
  void theOperationIsAlwaysStated() {
    Map<String, String> custom = new HashMap<>();
    custom.put("operation", "rewrite-manifests-by-hand");
    custom.put("iceberg-version", VERSION);
    TableMetadata base = withSnapshot(TestInlineDelta.baseMetadata(), 11L, null, custom);
    AddSnapshotUpdate decoded =
        decodeAdd(
            InlineDeltaCodec.encodeDelta(
                List.of(new AddSnapshotUpdate(22L, "", custom, 0L, null, null, 11L, null, null)),
                TestInlineDelta.TEST_TS, base));
    assertThat(decoded.inheritsParentExtra).isTrue();
    assertThat(decoded.summary(MetadataView.of(base))).isEqualTo(custom);
  }

  /** A writer's records name removals by index and keep their parents' summary entries. */
  @Test
  void aWritersRecordsReferToTheirParents() throws IOException {
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    LogCatalog catalog = LocalCatalogMode.APPEND_ML.catalog(io, "mem:///w", "c", Map.of());
    catalog.createNamespace(Namespace.of("ns"));
    Table table =
        catalog.createTable(
            id, schema, org.apache.iceberg.PartitionSpec.unpartitioned(),
            Map.of(TableProperties.MANIFEST_MIN_MERGE_COUNT, "3"));
    for (int i = 0; i < 4; i++) {
      table.newAppend().appendFile(file("f" + i)).commit();
    }

    // every append but the first, which has no parent, keeps its parent's entries; the merges
    // name the manifests they remove by index
    List<DeltaUpdate> updates = new ArrayList<>();
    deltas(io).forEach(updates::addAll);
    assertThat(updates)
        .filteredOn(AddSnapshotUpdate.class::isInstance)
        .map(AddSnapshotUpdate.class::cast)
        .hasSize(4)
        .filteredOn(add -> add.parentSnapshotId != null)
        .hasSize(3)
        .allMatch(add -> add.inheritsParentExtra && !add.summary.containsKey("iceberg-version"));
    assertThat(updates)
        .filteredOn(RemoveManifestUpdate.class::isInstance)
        .map(u -> ((RemoveManifestUpdate) u).parentIndices)
        .isNotEmpty()
        .allMatch(indices -> indices != null && indices.length > 0);

    Table loaded = LocalCatalogMode.APPEND_ML.<LogCatalog>catalog(io, "mem:///w", "c", Map.of())
        .loadTable(id);
    for (Snapshot snapshot : table.snapshots()) {
      assertThat(loaded.snapshot(snapshot.snapshotId()).summary())
          .isEqualTo(snapshot.summary())
          .containsKey("iceberg-version");
      assertThat(paths(loaded.snapshot(snapshot.snapshotId()).allManifests(loaded.io())))
          .isEqualTo(paths(snapshot.allManifests(table.io())));
    }
  }

  @Test
  void aRecordRemovingByIndexRemovesTheParentsManifestsThere() {
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    Table table = twoAppends(io);
    TableMetadata base = ((HasTableOperations) table).operations().current();
    Snapshot head = base.currentSnapshot();
    List<String> before = paths(head.allManifests(table.io()));
    assertThat(before).hasSize(2);

    commit(io, base, List.of(
        add(33L, head.snapshotId(), false),
        new SetSnapshotRefUpdate("main", 33L, "branch", 0, 0L, 0L),
        new RemoveManifestUpdate(33L, new int[] {1})));

    Table loaded = reload(io);
    assertThat(loaded.currentSnapshot().snapshotId()).isEqualTo(33L);
    assertThat(paths(loaded.currentSnapshot().allManifests(loaded.io())))
        .containsExactly(before.get(0));
  }

  @Test
  void aRecordWhoseParentsListLacksAnIndexItNamesIsRefused() {
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    Table table = twoAppends(io);
    TableMetadata base = ((HasTableOperations) table).operations().current();
    long head = base.currentSnapshot().snapshotId();

    assertThatThrownBy(
            () ->
                commit(io, base, List.of(
                    add(33L, head, false),
                    new SetSnapshotRefUpdate("main", 33L, "branch", 0, 0L, 0L),
                    new RemoveManifestUpdate(33L, new int[] {5}))))
        .isInstanceOf(CommitFailedException.class);
    assertThat(reload(io).currentSnapshot().snapshotId()).isEqualTo(head);
  }

  @Test
  void aRecordKeepingTheSummaryOfAParentTheTableLacksIsRefused() {
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    Table table = twoAppends(io);
    TableMetadata base = ((HasTableOperations) table).operations().current();
    long head = base.currentSnapshot().snapshotId();

    assertThatThrownBy(
            () ->
                commit(io, base, List.of(
                    add(33L, 999L, true),
                    new SetSnapshotRefUpdate("main", 33L, "branch", 0, 0L, 0L))))
        .isInstanceOf(CommitFailedException.class);
    assertThat(reload(io).currentSnapshot().snapshotId()).isEqualTo(head);
  }

  /** A writer names the manifests it adds under its table's manifest prefix. */
  @Test
  void aWritersRecordsNameManifestsUnderTheTablesPrefix() throws IOException {
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    Table table = twoAppends(io);
    CatalogState state = CatalogTestSupport.log(io, "mem:///w/catalog-c").read().state();
    String prefix = state.manifestListPrefix(state.tableId(id));
    assertThat(prefix).isEqualTo(table.location() + "/metadata/");

    List<List<DeltaUpdate>> deltas = deltas(io);
    List<DeltaUpdate> last = deltas.get(deltas.size() - 1);
    assertThat(last)
        .filteredOn(InlineDeltaCodec.AddManifestUpdate.class::isInstance)
        .map(u -> ((InlineDeltaCodec.AddManifestUpdate) u).manifest)
        .isNotEmpty()
        .allMatch(m -> ((ProtoCodec.DecodedManifestFile) m).relative() && !m.path().contains("/"));
    assertThat(paths(reload(io).currentSnapshot().allManifests(table.io())))
        .isEqualTo(paths(table.currentSnapshot().allManifests(table.io())))
        .allMatch(path -> path.startsWith(prefix));
  }

  /** A delta whose manifests' names are under another prefix than the table's does not apply. */
  @Test
  void aRecordNamingManifestsUnderAnotherPrefixIsRefused() {
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    Table table = twoAppends(io);
    TableMetadata base = ((HasTableOperations) table).operations().current();
    long head = base.currentSnapshot().snapshotId();
    String elsewhere = "mem:///elsewhere/metadata/";
    ManifestFile named = new TestProtoActions.TestManifestFile(
        elsewhere + "0b1e9a6c-2f3d-4e5f-8a9b-0c1d2e3f4a5b-m0.avro", 100L, 0,
        org.apache.iceberg.ManifestContent.DATA, 0L, 0L, 33L, 1, 0, 0, 1L, 0L, 0L,
        List.of(), null, null);

    assertThatThrownBy(
            () ->
                commit(io, base, List.of(
                    add(33L, head, false),
                    new SetSnapshotRefUpdate("main", 33L, "branch", 0, 0L, 0L),
                    new InlineDeltaCodec.AddManifestUpdate(33L, named, elsewhere, 0))))
        .isInstanceOf(CommitFailedException.class);
    assertThat(reload(io).currentSnapshot().snapshotId()).isEqualTo(head);
  }

  /** A new inline-ML snapshot {@code snapshotId}, a child of {@code parentId}. */
  private static AddSnapshotUpdate add(long snapshotId, long parentId, boolean inherits) {
    Map<String, String> summary = new HashMap<>();
    summary.put("operation", "delete");
    return new AddSnapshotUpdate(
        snapshotId, "", summary, inherits, List.of(), 0L, null, null, parentId, null, null, null);
  }

  /** Commits {@code updates}, a delta from {@code base}, to the catalog of {@code io}. */
  private void commit(InMemoryAtomicFileIO io, TableMetadata base, List<DeltaUpdate> updates) {
    CatalogLog log = CatalogTestSupport.log(io, "mem:///w/catalog-c");
    Replica<CatalogState> read = log.read();
    byte[] delta = InlineDeltaCodec.encodeDelta(updates, base.lastUpdatedMillis() + 1, base);
    new CatalogMutation(read).updateTableInlineDelta(id, delta).commit(log);
  }

  private Table twoAppends(InMemoryAtomicFileIO io) {
    LogCatalog catalog = LocalCatalogMode.APPEND_ML.catalog(io, "mem:///w", "c", Map.of());
    catalog.createNamespace(Namespace.of("ns"));
    Table table = catalog.createTable(id, schema);
    table.newFastAppend().appendFile(file("f1")).commit();
    table.newFastAppend().appendFile(file("f2")).commit();
    return catalog.loadTable(id);
  }

  private Table reload(InMemoryAtomicFileIO io) {
    return LocalCatalogMode.APPEND_ML.<LogCatalog>catalog(io, "mem:///w", "c", Map.of())
        .loadTable(id);
  }

  /** The updates of each table delta in the catalog file of {@code io}, in order. */
  private static List<List<DeltaUpdate>> deltas(InMemoryAtomicFileIO io) throws IOException {
    byte[] bytes;
    try (InputStream in = io.newInputFile("mem:///w/catalog-c").newStream()) {
      bytes = in.readAllBytes();
    }
    List<List<DeltaUpdate>> deltas = new ArrayList<>();
    for (ProtoCodec.Transaction record : CatalogTestSupport.decodeRecords(bytes)) {
      for (Object action : record.actions()) {
        if (action instanceof ProtoCodec.UpdateTableInlineAction
            && ((ProtoCodec.UpdateTableInlineAction) action).deltaBytes != null) {
          deltas.add(
              InlineDeltaCodec.decodeDelta(((ProtoCodec.UpdateTableInlineAction) action).deltaBytes)
                  .updates);
        }
      }
    }
    return deltas;
  }

  private static AddSnapshotUpdate decodeAdd(byte[] delta) {
    return (AddSnapshotUpdate) InlineDeltaCodec.decodeDelta(delta).updates.get(0);
  }

  private static TableMetadata withSnapshot(
      TableMetadata base, long snapshotId, Long parentId, Map<String, String> summary) {
    return new AddSnapshotUpdate(snapshotId, "s.avro", summary, 0L, null, null, parentId, null, null)
        .applyTo(base, PREFIX);
  }

  private static List<String> paths(List<ManifestFile> manifests) {
    return manifests.stream().map(ManifestFile::path).collect(Collectors.toList());
  }

  private static org.apache.iceberg.DataFile file(String name) {
    return org.apache.iceberg.DataFiles.builder(org.apache.iceberg.PartitionSpec.unpartitioned())
        .withPath("mem:///data/" + name + ".parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }
}
