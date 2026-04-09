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

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for InlineDeltaCodec: encoding, decoding, and application of
 * structured metadata deltas to TableMetadata.
 */
public class TestInlineDelta {

  /** Creates a minimal valid TableMetadata for testing. */
  static TableMetadata baseMetadata() {
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get()));
    return TableMetadata.buildFromEmpty()
        .setLocation("s3://bucket/warehouse/db/tbl")
        .setCurrentSchema(schema, 2)
        .addPartitionSpec(PartitionSpec.unpartitioned())
        .addSortOrder(SortOrder.unsorted())
        .assignUUID()
        .discardChanges()
        .build();
  }

  static byte[] metadataBytes(TableMetadata meta) {
    return TableMetadataParser.toJson(meta).getBytes(StandardCharsets.UTF_8);
  }

  // ============================================================
  // SetTableProperties
  // ============================================================

  @Nested
  class SetTablePropertiesTests {

    @Test
    void setPropertyApplies() {
      List<InlineDeltaCodec.DeltaUpdate> updates = List.of(
          new InlineDeltaCodec.SetPropertiesUpdate(
              Map.of("write.format.default", "parquet"), Set.of()));

      byte[] deltaBytes = InlineDeltaCodec.encodeDelta(updates);
      List<InlineDeltaCodec.DeltaUpdate> decoded = InlineDeltaCodec.decodeDelta(deltaBytes);
      assertThat(decoded).hasSize(1);
      assertThat(decoded.get(0)).isInstanceOf(InlineDeltaCodec.SetPropertiesUpdate.class);

      TableMetadata result = InlineDeltaCodec.applyUpdates(baseMetadata(), decoded);
      assertThat(result.properties()).containsEntry("write.format.default", "parquet");
    }

    @Test
    void removePropertyApplies() {
      // Start with a property
      TableMetadata base = TableMetadata.buildFrom(baseMetadata())
          .setProperties(Map.of("key1", "val1", "key2", "val2"))
          .discardChanges().build();

      List<InlineDeltaCodec.DeltaUpdate> updates = List.of(
          new InlineDeltaCodec.SetPropertiesUpdate(Map.of(), Set.of("key1")));

      TableMetadata result = InlineDeltaCodec.applyUpdates(base, updates);
      assertThat(result.properties()).doesNotContainKey("key1");
      assertThat(result.properties()).containsEntry("key2", "val2");
    }

    @Test
    void setAndRemoveInSameUpdate() {
      TableMetadata base = TableMetadata.buildFrom(baseMetadata())
          .setProperties(Map.of("old", "value"))
          .discardChanges().build();

      List<InlineDeltaCodec.DeltaUpdate> updates = List.of(
          new InlineDeltaCodec.SetPropertiesUpdate(
              Map.of("new", "value"), Set.of("old")));

      TableMetadata result = InlineDeltaCodec.applyUpdates(base, updates);
      assertThat(result.properties()).containsEntry("new", "value");
      assertThat(result.properties()).doesNotContainKey("old");
    }

    @Test
    void deltaAppliedViaActionOnInlineTable() {
      // Full integration: create inline table, apply delta via action
      TableMetadata base = baseMetadata();
      byte[] baseBytes = metadataBytes(base);

      // Build delta
      List<InlineDeltaCodec.DeltaUpdate> updates = List.of(
          new InlineDeltaCodec.SetPropertiesUpdate(
              Map.of("engine", "spark"), Set.of()));
      byte[] deltaBytes = InlineDeltaCodec.encodeDelta(updates);

      // Build catalog with inline table, apply delta action
      byte[] file = TestProtoActions.catalog()
          .ns(1, 0, "db", 1)
          .inlineTbl(1, 1, "tbl", 1, baseBytes, "")
          .build();

      ProtoCodec.Transaction txn = TestProtoActions.txn(
          ProtoCodec.UpdateTableInlineAction.delta(1, 1, deltaBytes));

      org.apache.iceberg.catalog.Namespace ns = org.apache.iceberg.catalog.Namespace.of("db");
      org.apache.iceberg.catalog.TableIdentifier tbl =
          org.apache.iceberg.catalog.TableIdentifier.of(ns, "tbl");

      ProtoCatalogFile result = TestProtoActions.apply(file, txn);
      assertThat(result.isInlineTable(result.tableId(tbl))).isTrue();
      assertThat(result.tableVersion(result.tableId(tbl))).isEqualTo(2);

      // Verify the updated metadata
      byte[] updatedMeta = result.inlineMetadata(result.tableId(tbl));
      TableMetadata updated = TableMetadataParser.fromJson(
          new String(updatedMeta, StandardCharsets.UTF_8));
      assertThat(updated.properties()).containsEntry("engine", "spark");
    }
  }

  // ============================================================
  // SetTableLocation
  // ============================================================

  @Nested
  class SetTableLocationTests {

    @Test
    void setLocationApplies() {
      List<InlineDeltaCodec.DeltaUpdate> updates = List.of(
          new InlineDeltaCodec.SetLocationUpdate("s3://new-bucket/table"));

      byte[] deltaBytes = InlineDeltaCodec.encodeDelta(updates);
      List<InlineDeltaCodec.DeltaUpdate> decoded = InlineDeltaCodec.decodeDelta(deltaBytes);
      assertThat(decoded).hasSize(1);

      TableMetadata result = InlineDeltaCodec.applyUpdates(baseMetadata(), decoded);
      assertThat(result.location()).isEqualTo("s3://new-bucket/table");
    }
  }

  // ============================================================
  // AddSchema / SetCurrentSchema
  // ============================================================

  @Nested
  class SchemaTests {

    @Test
    void addSchemaAndSetCurrent() {
      Schema newSchema = new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional(3, "ts", Types.TimestampType.withoutZone()));
      String schemaJson = org.apache.iceberg.SchemaParser.toJson(newSchema);

      List<InlineDeltaCodec.DeltaUpdate> updates = List.of(
          new InlineDeltaCodec.AddSchemaUpdate(1, 3, schemaJson.getBytes(StandardCharsets.UTF_8)),
          new InlineDeltaCodec.SetCurrentSchemaUpdate(1));

      byte[] deltaBytes = InlineDeltaCodec.encodeDelta(updates);
      List<InlineDeltaCodec.DeltaUpdate> decoded = InlineDeltaCodec.decodeDelta(deltaBytes);
      assertThat(decoded).hasSize(2);

      TableMetadata result = InlineDeltaCodec.applyUpdates(baseMetadata(), decoded);
      assertThat(result.currentSchemaId()).isEqualTo(1);
      assertThat(result.schema().columns()).hasSize(3);
    }

    @Test
    void encodeDecodeRoundtrip() {
      InlineDeltaCodec.SetCurrentSchemaUpdate update =
          new InlineDeltaCodec.SetCurrentSchemaUpdate(42);
      byte[] bytes = InlineDeltaCodec.encodeDelta(List.of(update));
      List<InlineDeltaCodec.DeltaUpdate> decoded = InlineDeltaCodec.decodeDelta(bytes);
      assertThat(decoded).hasSize(1);
      assertThat(decoded.get(0)).isInstanceOf(InlineDeltaCodec.SetCurrentSchemaUpdate.class);
      assertThat(((InlineDeltaCodec.SetCurrentSchemaUpdate) decoded.get(0)).schemaId).isEqualTo(42);
    }
  }

  // ============================================================
  // SetDefaultSortOrder / SetDefaultPartitionSpec
  // ============================================================

  @Nested
  class DefaultTests {

    @Test
    void setDefaultSortOrderRoundtrip() {
      InlineDeltaCodec.SetDefaultSortOrderUpdate update =
          new InlineDeltaCodec.SetDefaultSortOrderUpdate(5);
      byte[] bytes = InlineDeltaCodec.encodeDelta(List.of(update));
      List<InlineDeltaCodec.DeltaUpdate> decoded = InlineDeltaCodec.decodeDelta(bytes);
      assertThat(decoded).hasSize(1);
      assertThat(((InlineDeltaCodec.SetDefaultSortOrderUpdate) decoded.get(0)).orderId)
          .isEqualTo(5);
    }

    @Test
    void setDefaultSpecRoundtrip() {
      InlineDeltaCodec.SetDefaultSpecUpdate update =
          new InlineDeltaCodec.SetDefaultSpecUpdate(3);
      byte[] bytes = InlineDeltaCodec.encodeDelta(List.of(update));
      List<InlineDeltaCodec.DeltaUpdate> decoded = InlineDeltaCodec.decodeDelta(bytes);
      assertThat(decoded).hasSize(1);
      assertThat(((InlineDeltaCodec.SetDefaultSpecUpdate) decoded.get(0)).specId).isEqualTo(3);
    }
  }

  // ============================================================
  // SetSnapshotRef
  // ============================================================

  @Nested
  class SnapshotRefTests {

    @Test
    void encodeDecodeRoundtrip() {
      InlineDeltaCodec.SetSnapshotRefUpdate update =
          new InlineDeltaCodec.SetSnapshotRefUpdate(
              "main", 12345L, "branch", 10, 86400000L, 0);

      byte[] bytes = InlineDeltaCodec.encodeDelta(List.of(update));
      List<InlineDeltaCodec.DeltaUpdate> decoded = InlineDeltaCodec.decodeDelta(bytes);
      assertThat(decoded).hasSize(1);
      InlineDeltaCodec.SetSnapshotRefUpdate d =
          (InlineDeltaCodec.SetSnapshotRefUpdate) decoded.get(0);
      assertThat(d.refName).isEqualTo("main");
      assertThat(d.snapshotId).isEqualTo(12345L);
      assertThat(d.refType).isEqualTo("branch");
      assertThat(d.minSnapshotsToKeep).isEqualTo(10);
      assertThat(d.maxSnapshotAgeMs).isEqualTo(86400000L);
    }
  }

  // ============================================================
  // CompactSummary
  // ============================================================

  @Nested
  class CompactSummaryTests {

    @Test
    void addSnapshotEncodeDecodeRoundtrip() {
      Map<String, String> summary = Map.of(
          "operation", "append",
          "added-data-files", "5",
          "added-records", "1000",
          "total-data-files", "50",
          "total-records", "10000");

      InlineDeltaCodec.AddSnapshotUpdate update =
          new InlineDeltaCodec.AddSnapshotUpdate(
              99887766L, "123-uuid.avro", summary, 5000L, 0, 0);

      byte[] bytes = InlineDeltaCodec.encodeDelta(List.of(update));
      List<InlineDeltaCodec.DeltaUpdate> decoded = InlineDeltaCodec.decodeDelta(bytes);
      assertThat(decoded).hasSize(1);
      InlineDeltaCodec.AddSnapshotUpdate d =
          (InlineDeltaCodec.AddSnapshotUpdate) decoded.get(0);
      assertThat(d.snapshotId).isEqualTo(99887766L);
      assertThat(d.manifestListSuffix).isEqualTo("123-uuid.avro");
      assertThat(d.timestampDeltaMs).isEqualTo(5000L);
      assertThat(d.summary).containsEntry("operation", "append");
      assertThat(d.summary).containsEntry("added-data-files", "5");
      assertThat(d.summary).containsEntry("total-records", "10000");
    }

    @Test
    void extraSummaryFieldsPreserved() {
      Map<String, String> summary = new java.util.HashMap<>();
      summary.put("operation", "overwrite");
      summary.put("added-data-files", "1");
      summary.put("custom-engine-field", "spark-3.5");

      InlineDeltaCodec.AddSnapshotUpdate update =
          new InlineDeltaCodec.AddSnapshotUpdate(1L, "snap.avro", summary, 0, 0, 0);

      byte[] bytes = InlineDeltaCodec.encodeDelta(List.of(update));
      InlineDeltaCodec.AddSnapshotUpdate d =
          (InlineDeltaCodec.AddSnapshotUpdate) InlineDeltaCodec.decodeDelta(bytes).get(0);
      assertThat(d.summary).containsEntry("operation", "overwrite");
      assertThat(d.summary).containsEntry("custom-engine-field", "spark-3.5");
    }
  }

  // ============================================================
  // Multiple updates in single delta
  // ============================================================

  @Nested
  class MultiUpdateTests {

    @Test
    void multipleUpdatesApplyInOrder() {
      List<InlineDeltaCodec.DeltaUpdate> updates = List.of(
          new InlineDeltaCodec.SetPropertiesUpdate(
              Map.of("k1", "v1"), Set.of()),
          new InlineDeltaCodec.SetLocationUpdate("s3://new/location"),
          new InlineDeltaCodec.SetPropertiesUpdate(
              Map.of("k2", "v2"), Set.of()));

      byte[] deltaBytes = InlineDeltaCodec.encodeDelta(updates);
      List<InlineDeltaCodec.DeltaUpdate> decoded = InlineDeltaCodec.decodeDelta(deltaBytes);
      assertThat(decoded).hasSize(3);

      TableMetadata result = InlineDeltaCodec.applyUpdates(baseMetadata(), decoded);
      assertThat(result.properties()).containsEntry("k1", "v1");
      assertThat(result.properties()).containsEntry("k2", "v2");
      assertThat(result.location()).isEqualTo("s3://new/location");
    }

    @Test
    void deltaVersionMismatchRejectsTransaction() {
      TableMetadata base = baseMetadata();
      byte[] baseBytes = metadataBytes(base);

      List<InlineDeltaCodec.DeltaUpdate> updates = List.of(
          new InlineDeltaCodec.SetPropertiesUpdate(Map.of("k", "v"), Set.of()));
      byte[] deltaBytes = InlineDeltaCodec.encodeDelta(updates);

      byte[] file = TestProtoActions.catalog()
          .ns(1, 0, "db", 1)
          .inlineTbl(1, 1, "tbl", 3, baseBytes, "")
          .build();

      // Version 1 doesn't match actual 3
      ProtoCodec.Transaction txn = TestProtoActions.txn(
          ProtoCodec.UpdateTableInlineAction.delta(1, 1, deltaBytes));
      ProtoCatalogFile result = TestProtoActions.apply(file, txn);
      assertThat(result.containsTransaction(txn.id())).isFalse();
    }
  }
}
