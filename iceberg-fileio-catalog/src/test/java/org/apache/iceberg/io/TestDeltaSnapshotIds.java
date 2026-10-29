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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.io.InlineDeltaCodec.AddManifestUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.AddSnapshotUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.DeltaUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.RemoveManifestUpdate;
import org.apache.iceberg.io.InlineDeltaCodec.SetSnapshotRefUpdate;
import org.junit.jupiter.api.Test;

/**
 * A delta that adds one snapshot leaves that snapshot's id out of its other updates
 * (catalog.proto AddManifestDelta): its manifest updates, refs and the manifests it wrote. A
 * manifest it kept from another snapshot, and one with no snapshot id, say so.
 */
class TestDeltaSnapshotIds {
  private static final String PREFIX = "s3://bucket/db/tbl/metadata/";
  private static final long SNAPSHOT = 0x5eed_1234_5678_9abcL;

  @Test
  void aDeltaOfOneSnapshotWritesItsIdOnce() {
    List<DeltaUpdate> delta = oneSnapshot();
    byte[] bytes = InlineDeltaCodec.encodeDelta(delta, TestInlineDelta.TEST_TS);
    assertThat(occurrences(bytes, SNAPSHOT)).isEqualTo(1);

    List<DeltaUpdate> decoded = InlineDeltaCodec.decodeDelta(bytes).updates;
    assertThat(((SetSnapshotRefUpdate) decoded.get(1)).snapshotId).isEqualTo(SNAPSHOT);
    AddManifestUpdate wrote = (AddManifestUpdate) decoded.get(2);
    assertThat(wrote.snapshotId).isEqualTo(SNAPSHOT);
    assertThat(wrote.manifest.snapshotId()).isEqualTo(SNAPSHOT);
    assertThat(((AddManifestUpdate) decoded.get(3)).manifest.snapshotId()).isEqualTo(77L);
    assertThat(((AddManifestUpdate) decoded.get(4)).manifest.snapshotId()).isNull();
    RemoveManifestUpdate removed = (RemoveManifestUpdate) decoded.get(5);
    assertThat(removed.snapshotId).isEqualTo(SNAPSHOT);
    assertThat(removed.parentIndices).containsExactly(1);
  }

  @Test
  void aDeltaOfTwoSnapshotsStatesEveryId() {
    long second = SNAPSHOT + 1;
    List<DeltaUpdate> delta = new ArrayList<>(oneSnapshot());
    delta.add(1, new AddSnapshotUpdate(second, "", Map.of("operation", "append"), 0L, null, null,
        SNAPSHOT, null, null));
    delta.add(new AddManifestUpdate(second, manifest("e", second), PREFIX, 0));
    byte[] bytes = InlineDeltaCodec.encodeDelta(delta, TestInlineDelta.TEST_TS);

    List<DeltaUpdate> decoded = InlineDeltaCodec.decodeDelta(bytes).updates;
    assertThat(((SetSnapshotRefUpdate) decoded.get(2)).snapshotId).isEqualTo(SNAPSHOT);
    assertThat(((AddManifestUpdate) decoded.get(3)).snapshotId).isEqualTo(SNAPSHOT);
    assertThat(((RemoveManifestUpdate) decoded.get(6)).snapshotId).isEqualTo(SNAPSHOT);
    AddManifestUpdate last = (AddManifestUpdate) decoded.get(7);
    assertThat(last.snapshotId).isEqualTo(second);
    assertThat(last.manifest.snapshotId()).isEqualTo(second);
  }

  @Test
  void aPoolEntryStatesItsSnapshotId() throws Exception {
    ManifestFile wrote = manifest("a", SNAPSHOT);
    ManifestFile none = manifest("b", null);
    assertThat(ProtoCodec.decodeManifestFileEntry(
            ProtoCodec.encodeManifestFileEntry(wrote, PREFIX), PREFIX).snapshotId())
        .isEqualTo(SNAPSHOT);
    assertThat(ProtoCodec.decodeManifestFileEntry(
            ProtoCodec.encodeManifestFileEntry(none, PREFIX), PREFIX).snapshotId())
        .isNull();
  }

  /** A snapshot, its ref, a manifest it wrote, one it kept, one with no id, and a removal. */
  private static List<DeltaUpdate> oneSnapshot() {
    List<DeltaUpdate> delta = new ArrayList<>();
    delta.add(new AddSnapshotUpdate(SNAPSHOT, "", Map.of("operation", "append"), 0L, null, null,
        11L, null, null));
    delta.add(new SetSnapshotRefUpdate("main", SNAPSHOT, "branch", 0, 0L, 0L));
    delta.add(new AddManifestUpdate(SNAPSHOT, manifest("a", SNAPSHOT), PREFIX, 0));
    delta.add(new AddManifestUpdate(SNAPSHOT, manifest("b", 77L), PREFIX, 1));
    delta.add(new AddManifestUpdate(SNAPSHOT, manifest("c", null), PREFIX, 2));
    delta.add(new RemoveManifestUpdate(SNAPSHOT, new int[] {1}));
    return delta;
  }

  private static ManifestFile manifest(String name, Long snapshotId) {
    return new TestProtoActions.TestManifestFile(
        PREFIX + name + "-m0.avro", 1024L, 0, ManifestContent.DATA,
        3L, 3L, snapshotId, 5, 0, 0, 50L, 0L, 0L, null, null, null);
  }

  /** How often {@code id}'s fixed64 encoding occurs in {@code bytes}. */
  private static int occurrences(byte[] bytes, long id) {
    byte[] le = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(id).array();
    int count = 0;
    for (int i = 0; i + le.length <= bytes.length; i++) {
      boolean match = true;
      for (int j = 0; j < le.length && match; j++) {
        match = bytes[i + j] == le[j];
      }
      count += match ? 1 : 0;
    }
    return count;
  }
}
