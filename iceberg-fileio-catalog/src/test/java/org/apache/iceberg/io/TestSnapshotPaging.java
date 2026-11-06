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

import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/** A catalog that pages its inline tables' old snapshots (docs/SNAPSHOT_PAGING.md, step 4). */
class TestSnapshotPaging {
  private static final String WAREHOUSE = "mem:///wh";
  private static final TableIdentifier TABLE = TableIdentifier.of("ns", "t");
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  private final InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();

  private LogCatalog catalog(Map<String, String> properties) {
    return LocalCatalogMode.APPEND_ML.catalog(io, WAREHOUSE, "paging", properties);
  }

  private static Map<String, String> paging(int block, int window) {
    return Map.of(
        LogCatalogProperties.SNAPSHOT_BLOCK, String.valueOf(block),
        LogCatalogProperties.SNAPSHOT_WINDOW, String.valueOf(window));
  }

  /**
   * The catalog's first record fixes its paging, as it does its conflict encoding: a catalog that
   * opens the log later uses the log's, whatever it is configured with, and a checkpoint keeps it.
   */
  @Test
  void theFirstRecordFixesThePaging() {
    LogCatalog first = catalog(paging(4, 8));
    CatalogState state = first.log().read().state();
    assertThat(state.snapshotBlock()).isEqualTo(4);
    assertThat(state.snapshotWindow()).isEqualTo(8);

    LogCatalog second = catalog(paging(256, 1024));
    assertThat(second.log().read().state().snapshotBlock()).isEqualTo(4);

    byte[] checkpoint = CatalogStateMachine.INSTANCE.encodeCheckpoint(state);
    CatalogState decoded = CatalogStateMachine.INSTANCE.decodeCheckpoint(checkpoint).build();
    assertThat(decoded.snapshotBlock()).isEqualTo(4);
    assertThat(decoded.snapshotWindow()).isEqualTo(8);
  }

  /** A catalog that pages nothing records nothing, and a later record cannot set its paging. */
  @Test
  void pagingIsSetOnlyOnANewCatalog() {
    LogCatalog unpaged = catalog(Map.of());
    assertThat(unpaged.log().read().state().snapshotBlock()).isZero();
    unpaged.createNamespace(Namespace.of("ns"));
    unpaged.createTable(TABLE, SCHEMA);
    CatalogState.Builder builder = unpaged.log().read().state().toBuilder();
    assertThat(new ProtoCodec.SetSnapshotPagingAction(4, 8).verify(builder)).isFalse();

    CatalogState.Builder paged = CatalogState.builder().setSnapshotPaging(4, 8);
    assertThat(new ProtoCodec.SetSnapshotPagingAction(4, 8).verify(paged)).isFalse();
    assertThat(new ProtoCodec.SetSnapshotPagingAction(0, 8).verify(CatalogState.builder()))
        .isFalse();
    assertThat(new ProtoCodec.SetSnapshotPagingAction(4, 8).verify(CatalogState.builder()))
        .isTrue();
  }
}
