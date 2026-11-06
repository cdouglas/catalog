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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.InlineTableMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.util.SerializableSupplier;

/**
 * An inline table's metadata as replay holds it: the fields of a {@link TableMetadata}, without
 * one. Replay applies a record's common updates (snapshots, refs, properties) to these directly
 * ({@link NativeDelta}), sharing what they do not change, and builds the metadata only when a
 * client loads the table, a checkpoint encodes it, or a record's update needs Iceberg's builder
 * ({@link #toMetadata}, {@link #of}). Immutable; the snapshots and the snapshot log are {@link
 * Chain}s, so a record that appends to them copies neither.
 */
final class TableFields implements MetadataView {
  final int formatVersion;
  final String uuid;
  final String location;
  final long lastSequenceNumber;
  final long lastUpdatedMillis;
  final int lastColumnId;
  final int currentSchemaId;
  final List<Schema> schemas;
  final Map<Integer, Schema> schemasById;
  final int defaultSpecId;
  final List<PartitionSpec> specs;
  final Map<Integer, PartitionSpec> specsById;
  final int lastAssignedPartitionId;
  final int defaultSortOrderId;
  final List<SortOrder> sortOrders;
  final Map<Integer, SortOrder> sortOrdersById;
  final Map<String, String> properties;
  final long currentSnapshotId;
  final Chain<Snapshot> snapshots;
  final Chain<HistoryEntry> snapshotLog;
  final List<TableMetadata.MetadataLogEntry> previousFiles;
  final Map<String, SnapshotRef> refs;
  final List<StatisticsFile> statisticsFiles;
  final List<PartitionStatisticsFile> partitionStatisticsFiles;
  final long nextRowId;
  final List<EncryptedKey> encryptionKeys;

  @SuppressWarnings("checkstyle:ParameterNumber")
  private TableFields(
      int formatVersion,
      String uuid,
      String location,
      long lastSequenceNumber,
      long lastUpdatedMillis,
      int lastColumnId,
      int currentSchemaId,
      List<Schema> schemas,
      Map<Integer, Schema> schemasById,
      int defaultSpecId,
      List<PartitionSpec> specs,
      Map<Integer, PartitionSpec> specsById,
      int lastAssignedPartitionId,
      int defaultSortOrderId,
      List<SortOrder> sortOrders,
      Map<Integer, SortOrder> sortOrdersById,
      Map<String, String> properties,
      long currentSnapshotId,
      Chain<Snapshot> snapshots,
      Chain<HistoryEntry> snapshotLog,
      List<TableMetadata.MetadataLogEntry> previousFiles,
      Map<String, SnapshotRef> refs,
      List<StatisticsFile> statisticsFiles,
      List<PartitionStatisticsFile> partitionStatisticsFiles,
      long nextRowId,
      List<EncryptedKey> encryptionKeys) {
    this.formatVersion = formatVersion;
    this.uuid = uuid;
    this.location = location;
    this.lastSequenceNumber = lastSequenceNumber;
    this.lastUpdatedMillis = lastUpdatedMillis;
    this.lastColumnId = lastColumnId;
    this.currentSchemaId = currentSchemaId;
    this.schemas = schemas;
    this.schemasById = schemasById;
    this.defaultSpecId = defaultSpecId;
    this.specs = specs;
    this.specsById = specsById;
    this.lastAssignedPartitionId = lastAssignedPartitionId;
    this.defaultSortOrderId = defaultSortOrderId;
    this.sortOrders = sortOrders;
    this.sortOrdersById = sortOrdersById;
    this.properties = properties;
    this.currentSnapshotId = currentSnapshotId;
    this.snapshots = snapshots;
    this.snapshotLog = snapshotLog;
    this.previousFiles = previousFiles;
    this.refs = refs;
    this.statisticsFiles = statisticsFiles;
    this.partitionStatisticsFiles = partitionStatisticsFiles;
    this.nextRowId = nextRowId;
    this.encryptionKeys = encryptionKeys;
  }

  /** The fields of {@code metadata}. */
  static TableFields of(TableMetadata metadata) {
    Snapshot current = metadata.currentSnapshot();
    return new TableFields(
        metadata.formatVersion(),
        metadata.uuid(),
        metadata.location(),
        metadata.lastSequenceNumber(),
        metadata.lastUpdatedMillis(),
        metadata.lastColumnId(),
        metadata.currentSchemaId(),
        metadata.schemas(),
        metadata.schemasById(),
        metadata.defaultSpecId(),
        metadata.specs(),
        metadata.specsById(),
        metadata.lastAssignedPartitionId(),
        metadata.defaultSortOrderId(),
        metadata.sortOrders(),
        metadata.sortOrdersById(),
        metadata.properties(),
        current != null ? current.snapshotId() : -1L,
        Chain.of(metadata.snapshots(), Snapshot::snapshotId),
        Chain.of(metadata.snapshotLog()),
        metadata.previousFiles(),
        metadata.refs(),
        metadata.statisticsFiles(),
        metadata.partitionStatisticsFiles(),
        metadata.nextRowId(),
        metadata.encryptionKeys());
  }

  /**
   * The fields of a table whose {@code head} is its metadata parsed without its snapshots and refs
   * (and no current snapshot), with these: what the structural codec decodes, its snapshots made
   * from their columns when first read ({@link Chain#lazy}).
   */
  static TableFields of(
      TableMetadata head,
      long currentSnapshotId,
      Map<String, SnapshotRef> refs,
      Chain<Snapshot> snapshots) {
    TableFields fields = of(head);
    return new TableFields(
        fields.formatVersion,
        fields.uuid,
        fields.location,
        fields.lastSequenceNumber,
        fields.lastUpdatedMillis,
        fields.lastColumnId,
        fields.currentSchemaId,
        fields.schemas,
        fields.schemasById,
        fields.defaultSpecId,
        fields.specs,
        fields.specsById,
        fields.lastAssignedPartitionId,
        fields.defaultSortOrderId,
        fields.sortOrders,
        fields.sortOrdersById,
        fields.properties,
        currentSnapshotId,
        snapshots,
        fields.snapshotLog,
        fields.previousFiles,
        refs,
        fields.statisticsFiles,
        fields.partitionStatisticsFiles,
        fields.nextRowId,
        fields.encryptionKeys);
  }

  /**
   * The metadata these fields make, at {@code metadataFileLocation} (null: none). Builds it whole,
   * so call it where a whole one is needed, not per record.
   */
  TableMetadata toMetadata(String metadataFileLocation) {
    return InlineTableMetadata.create(
        metadataFileLocation,
        formatVersion,
        uuid,
        location,
        lastSequenceNumber,
        lastUpdatedMillis,
        lastColumnId,
        currentSchemaId,
        schemas,
        defaultSpecId,
        specs,
        lastAssignedPartitionId,
        defaultSortOrderId,
        sortOrders,
        properties,
        currentSnapshotId,
        List.copyOf(snapshots),
        List.copyOf(snapshotLog),
        List.copyOf(previousFiles),
        refs,
        statisticsFiles,
        partitionStatisticsFiles,
        nextRowId,
        encryptionKeys);
  }

  /**
   * The metadata these fields make at {@code metadataFileLocation}, holding only {@code held} (each
   * ref's target, at least) until something asks for the others, which {@code supplier} gives.
   */
  TableMetadata toMetadata(
      String metadataFileLocation,
      List<Snapshot> held,
      SerializableSupplier<List<Snapshot>> supplier) {
    return InlineTableMetadata.createLazy(
        metadataFileLocation,
        formatVersion,
        uuid,
        location,
        lastSequenceNumber,
        lastUpdatedMillis,
        lastColumnId,
        currentSchemaId,
        schemas,
        defaultSpecId,
        specs,
        lastAssignedPartitionId,
        defaultSortOrderId,
        sortOrders,
        properties,
        currentSnapshotId,
        held,
        supplier,
        List.copyOf(snapshotLog),
        List.copyOf(previousFiles),
        refs,
        statisticsFiles,
        partitionStatisticsFiles,
        nextRowId,
        encryptionKeys);
  }

  /**
   * These fields after a record's snapshots, refs and properties ({@link NativeDelta}), with the
   * collections a build of the metadata rebuilds; the rest are kept.
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  TableFields with(
      long newLastSequenceNumber,
      long newLastUpdatedMillis,
      Map<String, String> newProperties,
      long newCurrentSnapshotId,
      Chain<Snapshot> newSnapshots,
      Chain<HistoryEntry> newSnapshotLog,
      List<TableMetadata.MetadataLogEntry> newPreviousFiles,
      Map<String, SnapshotRef> newRefs,
      List<StatisticsFile> newStatisticsFiles,
      List<PartitionStatisticsFile> newPartitionStatisticsFiles,
      long newNextRowId) {
    return new TableFields(
        formatVersion,
        uuid,
        location,
        newLastSequenceNumber,
        newLastUpdatedMillis,
        lastColumnId,
        currentSchemaId,
        schemas,
        schemasById,
        defaultSpecId,
        specs,
        specsById,
        lastAssignedPartitionId,
        defaultSortOrderId,
        sortOrders,
        sortOrdersById,
        newProperties,
        newCurrentSnapshotId,
        newSnapshots,
        newSnapshotLog,
        newPreviousFiles,
        newRefs,
        newStatisticsFiles,
        newPartitionStatisticsFiles,
        newNextRowId,
        encryptionKeys);
  }

  /**
   * The table's head as metadata: these fields without snapshots, refs, logs or statistics, for the
   * builder to apply the updates that read none of them (location, schemas, specs, sort orders;
   * {@link NativeDelta}). No metadata location, so a build of it logs none.
   */
  TableMetadata head() {
    return InlineTableMetadata.create(
        null,
        formatVersion,
        uuid,
        location,
        lastSequenceNumber,
        lastUpdatedMillis,
        lastColumnId,
        currentSchemaId,
        schemas,
        defaultSpecId,
        specs,
        lastAssignedPartitionId,
        defaultSortOrderId,
        sortOrders,
        properties,
        -1L,
        List.of(),
        List.of(),
        List.of(),
        Map.of(),
        List.of(),
        List.of(),
        nextRowId,
        encryptionKeys);
  }

  /** These fields with the head of {@code head} ({@link #head}, after the builder's updates). */
  TableFields withHead(TableMetadata head) {
    return new TableFields(
        formatVersion,
        uuid,
        head.location(),
        lastSequenceNumber,
        lastUpdatedMillis,
        head.lastColumnId(),
        head.currentSchemaId(),
        head.schemas(),
        head.schemasById(),
        head.defaultSpecId(),
        head.specs(),
        head.specsById(),
        head.lastAssignedPartitionId(),
        head.defaultSortOrderId(),
        head.sortOrders(),
        head.sortOrdersById(),
        properties,
        currentSnapshotId,
        snapshots,
        snapshotLog,
        previousFiles,
        refs,
        statisticsFiles,
        partitionStatisticsFiles,
        nextRowId,
        encryptionKeys);
  }

  /** These fields with {@code newSnapshots}, the same snapshots by id, in their place. */
  TableFields withSnapshots(List<Snapshot> newSnapshots) {
    return new TableFields(
        formatVersion,
        uuid,
        location,
        lastSequenceNumber,
        lastUpdatedMillis,
        lastColumnId,
        currentSchemaId,
        schemas,
        schemasById,
        defaultSpecId,
        specs,
        specsById,
        lastAssignedPartitionId,
        defaultSortOrderId,
        sortOrders,
        sortOrdersById,
        properties,
        currentSnapshotId,
        Chain.of(newSnapshots, Snapshot::snapshotId),
        snapshotLog,
        previousFiles,
        refs,
        statisticsFiles,
        partitionStatisticsFiles,
        nextRowId,
        encryptionKeys);
  }

  @Override
  public int formatVersion() {
    return formatVersion;
  }

  @Override
  public long lastSequenceNumber() {
    return lastSequenceNumber;
  }

  @Override
  public long nextRowId() {
    return nextRowId;
  }

  @Override
  public long lastUpdatedMillis() {
    return lastUpdatedMillis;
  }

  @Override
  public Long currentSnapshotId() {
    return currentSnapshotId >= 0 && snapshots.containsKey(currentSnapshotId)
        ? currentSnapshotId
        : null;
  }

  @Override
  public SnapshotRef ref(String name) {
    return refs.get(name);
  }

  @Override
  public Map<String, SnapshotRef> refs() {
    return refs;
  }

  @Override
  public Snapshot snapshot(long snapshotId) {
    return snapshots.byKey(snapshotId);
  }

  @Override
  public Schema schema(int schemaId) {
    return schemasById.get(schemaId);
  }

  @Override
  public PartitionSpec spec(int specId) {
    return specsById.get(specId);
  }

  @Override
  public Map<Integer, PartitionSpec> specsById() {
    return specsById;
  }

  @Override
  public SortOrder sortOrder(int orderId) {
    return sortOrdersById.get(orderId);
  }

  /** The current schema's id, which a snapshot that names none takes. */
  int currentSchemaId() {
    return currentSchemaId;
  }

  /** The table's properties. */
  Map<String, String> properties() {
    return properties;
  }
}
