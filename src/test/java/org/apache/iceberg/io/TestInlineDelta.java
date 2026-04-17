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

  // ============================================================
  // computeDelta
  // ============================================================

  @Nested
  class ComputeDeltaTests {

    @Test
    void identicalMetadataReturnsNull() {
      TableMetadata meta = baseMetadata();
      assertThat(InlineDeltaCodec.computeDelta(meta, meta, "")).isNull();
    }

    @Test
    void propertyChangeProducesDelta() {
      TableMetadata oldMeta = baseMetadata();
      TableMetadata newMeta = TableMetadata.buildFrom(oldMeta)
          .setProperties(Map.of("engine", "spark"))
          .discardChanges().build();

      List<InlineDeltaCodec.DeltaUpdate> delta =
          InlineDeltaCodec.computeDelta(oldMeta, newMeta, "");
      assertThat(delta).isNotNull();
      assertThat(delta).hasSize(1);
      assertThat(delta.get(0)).isInstanceOf(InlineDeltaCodec.SetPropertiesUpdate.class);
      InlineDeltaCodec.SetPropertiesUpdate u =
          (InlineDeltaCodec.SetPropertiesUpdate) delta.get(0);
      assertThat(u.updated).containsEntry("engine", "spark");
    }

    @Test
    void propertyRemovalProducesDelta() {
      TableMetadata oldMeta = TableMetadata.buildFrom(baseMetadata())
          .setProperties(Map.of("key", "val"))
          .discardChanges().build();
      TableMetadata newMeta = TableMetadata.buildFrom(oldMeta)
          .removeProperties(Set.of("key"))
          .discardChanges().build();

      List<InlineDeltaCodec.DeltaUpdate> delta =
          InlineDeltaCodec.computeDelta(oldMeta, newMeta, "");
      assertThat(delta).isNotNull();
      InlineDeltaCodec.SetPropertiesUpdate u =
          (InlineDeltaCodec.SetPropertiesUpdate) delta.get(0);
      assertThat(u.removed).contains("key");
    }

    @Test
    void locationChangeProducesDelta() {
      TableMetadata oldMeta = baseMetadata();
      TableMetadata newMeta = TableMetadata.buildFrom(oldMeta)
          .setLocation("s3://new-bucket/table")
          .discardChanges().build();

      List<InlineDeltaCodec.DeltaUpdate> delta =
          InlineDeltaCodec.computeDelta(oldMeta, newMeta, "");
      assertThat(delta).isNotNull();
      assertThat(delta.stream()
          .filter(d -> d instanceof InlineDeltaCodec.SetLocationUpdate)
          .count()).isEqualTo(1);
    }

    @Test
    void schemaEvolutionProducesDelta() {
      TableMetadata oldMeta = baseMetadata();
      Schema newSchema = new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional(3, "ts", Types.TimestampType.withoutZone()));
      TableMetadata newMeta = TableMetadata.buildFrom(oldMeta)
          .setCurrentSchema(newSchema, 3)
          .discardChanges().build();

      List<InlineDeltaCodec.DeltaUpdate> delta =
          InlineDeltaCodec.computeDelta(oldMeta, newMeta, "");
      assertThat(delta).isNotNull();
      // Should have AddSchema + SetCurrentSchema
      assertThat(delta.stream()
          .filter(d -> d instanceof InlineDeltaCodec.AddSchemaUpdate).count())
          .isGreaterThanOrEqualTo(1);
      assertThat(delta.stream()
          .filter(d -> d instanceof InlineDeltaCodec.SetCurrentSchemaUpdate).count())
          .isEqualTo(1);
    }

    @Test
    void deltaRoundtripPreservesProperties() {
      TableMetadata oldMeta = baseMetadata();
      TableMetadata newMeta = TableMetadata.buildFrom(oldMeta)
          .setProperties(Map.of("a", "1", "b", "2"))
          .setLocation("s3://moved")
          .discardChanges().build();

      List<InlineDeltaCodec.DeltaUpdate> delta =
          InlineDeltaCodec.computeDelta(oldMeta, newMeta, "");
      byte[] encoded = InlineDeltaCodec.encodeDelta(delta);
      List<InlineDeltaCodec.DeltaUpdate> decoded = InlineDeltaCodec.decodeDelta(encoded);
      TableMetadata applied = InlineDeltaCodec.applyUpdates(oldMeta, decoded);

      assertThat(applied.properties()).containsEntry("a", "1");
      assertThat(applied.properties()).containsEntry("b", "2");
      assertThat(applied.location()).isEqualTo("s3://moved");
    }
  }

  // ============================================================
  // selectMode
  // ============================================================

  @Nested
  class SelectModeTests {

    @Test
    void smallDeltaSelectsDelta() {
      List<InlineDeltaCodec.DeltaUpdate> delta = List.of(
          new InlineDeltaCodec.SetPropertiesUpdate(Map.of("k", "v"), Set.of()));
      assertThat(InlineDeltaCodec.selectMode(delta, baseMetadata(), 0)).isEqualTo("delta");
    }

    @Test
    void nullDeltaSelectsFull() {
      assertThat(InlineDeltaCodec.selectMode(null, baseMetadata(), 0)).isEqualTo("full");
    }

    @Test
    void oversizedSelectsPointer() {
      // txn already near the limit
      int nearLimit = InlineDeltaCodec.APPEND_LIMIT - 10;
      assertThat(InlineDeltaCodec.selectMode(null, baseMetadata(), nearLimit))
          .isEqualTo("pointer");
    }
  }

  // ============================================================
  // InlineManifestTableOperations tests
  // ============================================================

  @Nested
  class InlineManifestTableOpsTests {

    @Test
    void sinkStagesAndDrains() {
      var ops = new FileIOCatalog.InlineManifestTableOperations(
          org.apache.iceberg.catalog.TableIdentifier.of("db", "tbl"),
          "mem:///catalog", new ProtoCatalogFormat(), null, true);

      org.apache.iceberg.ManifestFile m1 = new TestProtoActions.TestManifestFile(
          "s3://b/m1.avro", 1024, 0, org.apache.iceberg.ManifestContent.DATA,
          1, 1, 100L, 5, 0, 0, 50L, 0L, 0L, null, null, null);

      ops.stageManifestListDelta(1L, 100L, null, null,
          new org.apache.iceberg.ManifestListSink.ManifestListDelta(
              List.of(m1), List.of()),
          null);
      ops.stageManifestListDelta(2L, 200L, 100L, null,
          new org.apache.iceberg.ManifestListSink.ManifestListDelta(
              List.of(m1), List.of("s3://b/old.avro")),
          null);

      var drained = ops.drainStagedDeltas();
      assertThat(drained).hasSize(2);
      assertThat(drained.get(100L).added()).hasSize(1);
      assertThat(drained.get(100L).removedPaths()).isEmpty();
      assertThat(drained.get(200L).removedPaths()).containsExactly("s3://b/old.avro");

      // drain clears
      assertThat(ops.drainStagedDeltas()).isEmpty();
    }

    @Test
    void isInstanceOfManifestListSink() {
      var ops = new FileIOCatalog.InlineManifestTableOperations(
          org.apache.iceberg.catalog.TableIdentifier.of("db", "tbl"),
          "mem:///catalog", new ProtoCatalogFormat(), null, true);
      assertThat(ops).isInstanceOf(org.apache.iceberg.ManifestListSink.class);
    }

    @Test
    void configValidationRejectsManifestsWithoutInline() {
      org.assertj.core.api.Assertions.assertThatThrownBy(() -> {
        FileIOCatalog catalog = new FileIOCatalog();
        Map<String, String> props = new java.util.HashMap<>();
        props.put("fileio.catalog.inline", "false");
        props.put("fileio.catalog.inline.manifests", "true");
        props.put(org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION, "mem:///wh");
        catalog.initialize("test", props);
      }).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("fileio.catalog.inline.manifests=true requires");
    }
  }

  // ============================================================
  // Delta replay with ML updates tests
  // ============================================================

  @Nested
  class DeltaReplayTests {
    private static final String PATH_PREFIX = "s3://bucket/db/tbl/metadata/";

    /**
     * §3.3/§2.4: A delta containing AddSnapshotUpdate + AddManifestUpdate must
     * survive encode → decode → apply. The manifest must appear in the catalog
     * builder's manifest pool after application.
     */
    @Test
    void applyDeltaWithAddSnapshotAndManifests() {
      org.apache.iceberg.ManifestFile mf = new TestProtoActions.TestManifestFile(
          PATH_PREFIX + "aaa-m0.avro", 2048L, 0,
          org.apache.iceberg.ManifestContent.DATA,
          1L, 1L, 500L,
          10, 0, 0, 100L, 0L, 0L,
          null, null, null);

      // Build a delta with AddSnapshot + AddManifest
      List<InlineDeltaCodec.DeltaUpdate> delta = new java.util.ArrayList<>();
      delta.add(new InlineDeltaCodec.AddSnapshotUpdate(
          500L, "", Map.of("operation", "append"),
          1000L, 0, 100L));
      InlineDeltaCodec.attachManifestDelta(
          delta, 500L, List.of(mf), List.of(), PATH_PREFIX);

      byte[] deltaBytes = InlineDeltaCodec.encodeDelta(delta);

      // Set up a catalog builder with an inline table
      byte[] baseMeta = TableMetadataParser.toJson(baseMetadata())
          .getBytes(java.nio.charset.StandardCharsets.UTF_8);
      InputFile loc = new InputFile() {
        @Override public long getLength() { return 0; }
        @Override public SeekableInputStream newStream() { return null; }
        @Override public String location() { return "test://catalog"; }
        @Override public boolean exists() { return true; }
      };
      ProtoCatalogFile.Builder builder = ProtoCatalogFile.builder(loc);
      builder.addNamespace(0, 0, "", 1);
      builder.addNamespace(1, 0, "db", 1);
      builder.setNextNamespaceId(2);
      builder.setNextTableId(2);
      builder.addInlineTable(1, 1, "tbl", 1, baseMeta, PATH_PREFIX);

      // Apply the delta — routes ML updates to the catalog builder
      byte[] updatedMeta = InlineDeltaCodec.applyDeltaWithManifests(
          baseMeta, deltaBytes, builder, 1);

      // The manifest pool should have the manifest
      assertThat(builder.hasManifestPool(1)).isTrue();
      // The snapshot refs should have the manifest path
      List<String> refs = builder.snapshotManifestPaths(1, 500L);
      assertThat(refs).hasSize(1);
      assertThat(refs.get(0)).isEqualTo(mf.path());

      // The updated metadata should parse and contain the new snapshot
      assertThat(updatedMeta).isNotNull();
      org.apache.iceberg.TableMetadata updated = TableMetadataParser.fromJson(
          new String(updatedMeta, java.nio.charset.StandardCharsets.UTF_8));
      assertThat(updated.snapshots()).hasSize(1);
      assertThat(updated.snapshots().get(0).snapshotId()).isEqualTo(500L);
    }

    /** Verify AddSnapshot + SetSnapshotRef results in a table with currentSnapshot set. */
    @Test
    void applyDeltaWithSnapshotRefSetsCurrentSnapshot() {
      List<InlineDeltaCodec.DeltaUpdate> delta = new java.util.ArrayList<>();
      delta.add(new InlineDeltaCodec.AddSnapshotUpdate(
          500L, "", Map.of("operation", "append"),
          1000L, 0, 100L));
      delta.add(new InlineDeltaCodec.SetSnapshotRefUpdate(
          "main", 500L, "branch", 0, 0L, 0L));

      byte[] deltaBytes = InlineDeltaCodec.encodeDelta(delta);

      byte[] baseMeta = TableMetadataParser.toJson(baseMetadata())
          .getBytes(java.nio.charset.StandardCharsets.UTF_8);
      InputFile loc = new InputFile() {
        @Override public long getLength() { return 0; }
        @Override public SeekableInputStream newStream() { return null; }
        @Override public String location() { return "test://catalog"; }
        @Override public boolean exists() { return true; }
      };
      ProtoCatalogFile.Builder builder = ProtoCatalogFile.builder(loc);
      builder.addNamespace(0, 0, "", 1);
      builder.addNamespace(1, 0, "db", 1);
      builder.setNextNamespaceId(2);
      builder.setNextTableId(2);
      builder.addInlineTable(1, 1, "tbl", 1, baseMeta, "");

      byte[] updatedMeta = InlineDeltaCodec.applyDeltaWithManifests(
          baseMeta, deltaBytes, builder, 1);

      org.apache.iceberg.TableMetadata updated = TableMetadataParser.fromJson(
          new String(updatedMeta, java.nio.charset.StandardCharsets.UTF_8));
      assertThat(updated.snapshots()).as("should have 1 snapshot").hasSize(1);
      assertThat(updated.currentSnapshot()).as("currentSnapshot should be set").isNotNull();
      assertThat(updated.currentSnapshot().snapshotId()).isEqualTo(500L);
    }
  }

  // ============================================================
  // Manifest list delta tests
  // ============================================================

  @Nested
  class ManifestDeltaTests {

    private static final String PATH_PREFIX = "s3://bucket/db/tbl/metadata/";

    @Test
    void addManifestRoundtrip() {
      org.apache.iceberg.ManifestFile mf = new TestProtoActions.TestManifestFile(
          PATH_PREFIX + "aaa-m0.avro", 2048L, 0,
          org.apache.iceberg.ManifestContent.DATA,
          5L, 5L, 100L,
          10, 0, 0, 100L, 0L, 0L,
          null, null, null);

      List<InlineDeltaCodec.DeltaUpdate> updates = List.of(
          new InlineDeltaCodec.AddManifestUpdate(100L, mf, PATH_PREFIX));

      byte[] deltaBytes = InlineDeltaCodec.encodeDelta(updates);
      List<InlineDeltaCodec.DeltaUpdate> decoded = InlineDeltaCodec.decodeDelta(deltaBytes);

      assertThat(decoded).hasSize(1);
      assertThat(decoded.get(0)).isInstanceOf(InlineDeltaCodec.AddManifestUpdate.class);
      InlineDeltaCodec.AddManifestUpdate add =
          (InlineDeltaCodec.AddManifestUpdate) decoded.get(0);
      assertThat(add.snapshotId).isEqualTo(100L);
      assertThat(add.manifest.length()).isEqualTo(2048L);
      assertThat(add.manifest.addedFilesCount()).isEqualTo(10);
      assertThat(add.manifest.addedRowsCount()).isEqualTo(100L);
    }

    @Test
    void removeManifestRoundtrip() {
      List<InlineDeltaCodec.DeltaUpdate> updates = List.of(
          new InlineDeltaCodec.RemoveManifestUpdate(200L, "bbb-m0.avro"));

      byte[] deltaBytes = InlineDeltaCodec.encodeDelta(updates);
      List<InlineDeltaCodec.DeltaUpdate> decoded = InlineDeltaCodec.decodeDelta(deltaBytes);

      assertThat(decoded).hasSize(1);
      assertThat(decoded.get(0)).isInstanceOf(InlineDeltaCodec.RemoveManifestUpdate.class);
      InlineDeltaCodec.RemoveManifestUpdate rm =
          (InlineDeltaCodec.RemoveManifestUpdate) decoded.get(0);
      assertThat(rm.snapshotId).isEqualTo(200L);
      assertThat(rm.manifestPathSuffix).isEqualTo("bbb-m0.avro");
    }

    @Test
    void fastAppendDelta() {
      // FastAppend: 1 add, 0 removes
      org.apache.iceberg.ManifestFile newManifest = new TestProtoActions.TestManifestFile(
          PATH_PREFIX + "new-m0.avro", 1024L, 0,
          org.apache.iceberg.ManifestContent.DATA,
          3L, 3L, 300L,
          5, 0, 0, 50L, 0L, 0L,
          null, null, null);

      List<InlineDeltaCodec.DeltaUpdate> delta = new java.util.ArrayList<>();
      InlineDeltaCodec.attachManifestDelta(
          delta, 300L,
          List.of(newManifest), List.of(), PATH_PREFIX);

      assertThat(delta).hasSize(1);
      assertThat(delta.get(0)).isInstanceOf(InlineDeltaCodec.AddManifestUpdate.class);

      // Round-trip
      byte[] encoded = InlineDeltaCodec.encodeDelta(delta);
      List<InlineDeltaCodec.DeltaUpdate> decoded = InlineDeltaCodec.decodeDelta(encoded);
      assertThat(decoded).hasSize(1);
    }

    @Test
    void deleteDelta() {
      // Delete: 1 add (rewritten manifest) + 1 remove (original)
      org.apache.iceberg.ManifestFile rewritten = new TestProtoActions.TestManifestFile(
          PATH_PREFIX + "rewritten-m0.avro", 2048L, 0,
          org.apache.iceberg.ManifestContent.DATA,
          4L, 1L, 400L,
          5, 85, 10, 50L, 850L, 100L,
          null, null, null);

      List<InlineDeltaCodec.DeltaUpdate> delta = new java.util.ArrayList<>();
      InlineDeltaCodec.attachManifestDelta(
          delta, 400L,
          List.of(rewritten),
          List.of(PATH_PREFIX + "original-m0.avro"),
          PATH_PREFIX);

      assertThat(delta).hasSize(2);
      assertThat(delta.get(0)).isInstanceOf(InlineDeltaCodec.AddManifestUpdate.class);
      assertThat(delta.get(1)).isInstanceOf(InlineDeltaCodec.RemoveManifestUpdate.class);

      InlineDeltaCodec.RemoveManifestUpdate rm =
          (InlineDeltaCodec.RemoveManifestUpdate) delta.get(1);
      assertThat(rm.manifestPathSuffix).isEqualTo("original-m0.avro");
    }

    /** §3.1: computeDelta must handle null manifestListLocation (InlineSnapshot). */
    @Test
    void computeDeltaHandlesNullManifestListLocation() {
      TableMetadata base = baseMetadata();

      // Construct metadata with an InlineSnapshot (manifestListLocation == null)
      org.apache.iceberg.ManifestFile mf = new TestProtoActions.TestManifestFile(
          "s3://bucket/m0.avro", 1024, 0, org.apache.iceberg.ManifestContent.DATA,
          1, 1, 99L, 5, 0, 0, 50L, 0L, 0L, null, null, null);
      org.apache.iceberg.Snapshot inlineSnap = new org.apache.iceberg.InlineSnapshot(
          1L, 99L, null, System.currentTimeMillis(), "append",
          Map.of("operation", "append"), base.currentSchemaId(), null, null, null,
          List.of(mf));
      TableMetadata newMeta = TableMetadata.buildFrom(base)
          .addSnapshot(inlineSnap)
          .discardChanges()
          .build();

      // computeDelta must not throw NPE
      List<InlineDeltaCodec.DeltaUpdate> delta =
          InlineDeltaCodec.computeDelta(base, newMeta, "s3://bucket/");
      assertThat(delta).isNotEmpty();
      // The AddSnapshotUpdate should have an empty-string suffix (no Avro file)
      InlineDeltaCodec.AddSnapshotUpdate addSnap = delta.stream()
          .filter(u -> u instanceof InlineDeltaCodec.AddSnapshotUpdate)
          .map(u -> (InlineDeltaCodec.AddSnapshotUpdate) u)
          .findFirst()
          .orElseThrow();
      assertThat(addSnap.manifestListSuffix).isEqualTo("");
    }

    @Test
    void mixedTmAndMlDelta() {
      // A typical data commit delta: AddSnapshot + SetSnapshotRef + AddManifest
      org.apache.iceberg.ManifestFile mf = new TestProtoActions.TestManifestFile(
          PATH_PREFIX + "xyz-m0.avro", 4096L, 0,
          org.apache.iceberg.ManifestContent.DATA,
          1L, 1L, 500L,
          20, 0, 0, 200L, 0L, 0L,
          null, null, null);

      List<InlineDeltaCodec.DeltaUpdate> delta = new java.util.ArrayList<>();
      delta.add(new InlineDeltaCodec.AddSnapshotUpdate(
          500L, "snap-500.avro", Map.of("operation", "append"),
          1000L, 0, 200L));
      delta.add(new InlineDeltaCodec.SetSnapshotRefUpdate(
          "main", 500L, "branch", 0, 0L, 0L));
      InlineDeltaCodec.attachManifestDelta(
          delta, 500L, List.of(mf), List.of(), PATH_PREFIX);

      assertThat(delta).hasSize(3);

      // Round-trip all together
      byte[] encoded = InlineDeltaCodec.encodeDelta(delta);
      List<InlineDeltaCodec.DeltaUpdate> decoded = InlineDeltaCodec.decodeDelta(encoded);
      assertThat(decoded).hasSize(3);
      assertThat(decoded.get(0)).isInstanceOf(InlineDeltaCodec.AddSnapshotUpdate.class);
      assertThat(decoded.get(1)).isInstanceOf(InlineDeltaCodec.SetSnapshotRefUpdate.class);
      assertThat(decoded.get(2)).isInstanceOf(InlineDeltaCodec.AddManifestUpdate.class);
    }
  }
}
