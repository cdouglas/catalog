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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** A sealed block's page and the Merkle tree over its snapshots ({@link SnapshotPages}). */
class TestSnapshotPages {
  private static final TableIdentifier TABLE = TableIdentifier.of("ns", "t");
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "cat", Types.IntegerType.get()));
  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("cat").build();

  private final InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
  private CatalogState state;
  private int tblId;
  private List<Snapshot> snapshots;
  private String prefix;

  @BeforeEach
  void table() {
    LogCatalog catalog = LocalCatalogMode.APPEND_ML.catalog(io, "mem:///wh", "pages", Map.of());
    catalog.createNamespace(Namespace.of("ns"));
    Table table = catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    for (int i = 0; i < 7; i++) {
      table.newFastAppend()
          .appendFile(DataFiles.builder(SPEC)
              .withPath("mem:///data/f" + i + ".parquet")
              .withFileSizeInBytes(10)
              .withRecordCount(1)
              .withPartitionPath("cat=" + (i % 3))
              .build())
          .commit();
    }
    state = catalog.log().read().state();
    tblId = state.tableId(TABLE);
    snapshots = new ArrayList<>(state.inlineTable(tblId).fields().snapshots);
    prefix = state.allManifestPrefixes().getOrDefault(tblId, "");
  }

  private SnapshotPages.SealedBlock seal(List<Snapshot> sealed) {
    return SnapshotPages.seal(
        "uuid", tblId, 0, sealed, state.snapshotManifests(tblId), state.manifestPool(tblId), prefix);
  }

  @Test
  void aPageHoldsItsSnapshotsAndTheirLists() {
    SnapshotPages.SealedBlock block = seal(snapshots);
    byte[] page = block.pending.bytes();
    SnapshotPages.Page decoded =
        SnapshotPages.decode(block.pageName(tblId), tblId, 0, page, prefix);

    assertThat(decoded.snapshots.stream().map(SnapshotParser::toJson))
        .containsExactlyElementsOf(snapshots.stream().map(SnapshotParser::toJson)
            .collect(Collectors.toList()));
    for (Snapshot snapshot : snapshots) {
      List<String> paths = state.snapshotManifests(tblId).get(snapshot.snapshotId());
      assertThat(decoded.lists.get(snapshot.snapshotId()))
          .extracting(ManifestFile::path)
          .containsExactlyElementsOf(paths);
    }
    assertThat(block.liveIds).isSorted().hasSize(snapshots.size());
    // Every replica that seals the block makes the same page
    assertThat(seal(snapshots).pending.bytes()).isEqualTo(page);
    assertThat(seal(snapshots).merkleRoot).isEqualTo(block.merkleRoot);
  }

  @Test
  void aPageIsTheOneItsNameNames() {
    SnapshotPages.SealedBlock block = seal(snapshots);
    byte[] other = seal(snapshots.subList(0, 3)).pending.bytes();
    assertThatThrownBy(() -> SnapshotPages.decode(block.pageName(tblId), tblId, 0, other, prefix))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("holds other bytes");
    assertThatThrownBy(() -> SnapshotPages.decode(
            block.pageName(tblId), tblId, 1, block.pending.bytes(), prefix))
        .isInstanceOf(IllegalStateException.class);
    // No committed-set page parses so: its sweep leaves snapshot pages alone
    assertThat(org.intentlog.wire.CommittedSetCodec.pageTop(block.pageName(tblId))).isEqualTo(-1L);
  }

  @Test
  void eachLeafProvesItsPlaceUnderTheRoot() {
    List<byte[]> leaves = new ArrayList<>();
    for (Snapshot snapshot : snapshots) {
      leaves.add(leaf(snapshot));
    }
    List<byte[]> hashes = leaves.stream().map(SnapshotPages::leafHash).collect(Collectors.toList());
    byte[] root = SnapshotPages.merkleRoot(hashes);
    assertThat(root).isEqualTo(seal(snapshots).merkleRoot);
    for (int i = 0; i < leaves.size(); i++) {
      List<byte[]> proof = SnapshotPages.proof(hashes, i);
      assertThat(SnapshotPages.verify(leaves.get(i), i, leaves.size(), proof, root)).isTrue();
      // Not at another place, nor another leaf, nor a shorter proof
      int other = (i + 1) % leaves.size();
      assertThat(SnapshotPages.verify(leaves.get(i), other, leaves.size(), proof, root)).isFalse();
      assertThat(SnapshotPages.verify(leaves.get(other), i, leaves.size(), proof, root)).isFalse();
      if (!proof.isEmpty()) {
        assertThat(SnapshotPages.verify(
            leaves.get(i), i, leaves.size(), proof.subList(1, proof.size()), root)).isFalse();
      }
    }
  }

  private byte[] leaf(Snapshot snapshot) {
    List<String> paths = state.snapshotManifests(tblId).get(snapshot.snapshotId());
    List<ManifestFile> manifests = new ArrayList<>();
    for (String path : paths) {
      manifests.add(state.manifestPool(tblId).get(path));
    }
    return SnapshotPages.leaf(snapshot, manifests, prefix);
  }

  @Test
  void blocksSealAWindowBehindTheLastSequenceNumber() {
    // B = 4, W = 8: block 0 ([0, 4)) seals at 12, block 1 at 16
    assertThat(SnapshotPages.sealedThrough(11, 4, 8)).isEqualTo(-1);
    assertThat(SnapshotPages.sealedThrough(12, 4, 8)).isEqualTo(0);
    assertThat(SnapshotPages.sealedThrough(15, 4, 8)).isEqualTo(0);
    assertThat(SnapshotPages.sealedThrough(16, 4, 8)).isEqualTo(1);
    assertThat(SnapshotPages.sealedThrough(0, 4, 0)).isEqualTo(-1);
    assertThat(SnapshotPages.sealedThrough(4, 4, 0)).isEqualTo(0);
    assertThat(SnapshotPages.blockOf(7, 4)).isEqualTo(1);
  }
}
