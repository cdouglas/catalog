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
package org.apache.iceberg;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.SerializableSupplier;

/**
 * Builds {@link TableMetadata} from its fields, for the catalog's inline tables: replay keeps a
 * table's fields in its own form and makes the metadata only when a client loads the table or a
 * checkpoint encodes it. {@link TableMetadata}'s constructor and its log entries' are
 * package-private; this class lives in {@code org.apache.iceberg} to reach them, as {@link
 * InlineDeltaSnapshots} does for snapshots. The metadata has no changes, as metadata read from a
 * file has none, and runs every check its constructor runs.
 */
public final class InlineTableMetadata {
  private InlineTableMetadata() {}

  /** Metadata at {@code metadataFileLocation} (null: none) with exactly these fields. */
  @SuppressWarnings("checkstyle:ParameterNumber")
  public static TableMetadata create(
      String metadataFileLocation,
      int formatVersion,
      String uuid,
      String location,
      long lastSequenceNumber,
      long lastUpdatedMillis,
      int lastColumnId,
      int currentSchemaId,
      List<Schema> schemas,
      int defaultSpecId,
      List<PartitionSpec> specs,
      int lastAssignedPartitionId,
      int defaultSortOrderId,
      List<SortOrder> sortOrders,
      Map<String, String> properties,
      long currentSnapshotId,
      List<Snapshot> snapshots,
      List<HistoryEntry> snapshotLog,
      List<TableMetadata.MetadataLogEntry> previousFiles,
      Map<String, SnapshotRef> refs,
      List<StatisticsFile> statisticsFiles,
      List<PartitionStatisticsFile> partitionStatisticsFiles,
      long nextRowId,
      List<EncryptedKey> encryptionKeys) {
    return new TableMetadata(
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
        snapshots,
        null,
        snapshotLog,
        previousFiles,
        refs,
        statisticsFiles,
        partitionStatisticsFiles,
        nextRowId,
        encryptionKeys,
        ImmutableList.of());
  }

  /**
   * As {@link #create}, holding only {@code snapshots} (which must include every ref's target) until
   * something asks for the others, when it takes them all from {@code snapshotsSupplier} (as REST's
   * refs mode loads a table).
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  public static TableMetadata createLazy(
      String metadataFileLocation,
      int formatVersion,
      String uuid,
      String location,
      long lastSequenceNumber,
      long lastUpdatedMillis,
      int lastColumnId,
      int currentSchemaId,
      List<Schema> schemas,
      int defaultSpecId,
      List<PartitionSpec> specs,
      int lastAssignedPartitionId,
      int defaultSortOrderId,
      List<SortOrder> sortOrders,
      Map<String, String> properties,
      long currentSnapshotId,
      List<Snapshot> snapshots,
      SerializableSupplier<List<Snapshot>> snapshotsSupplier,
      List<HistoryEntry> snapshotLog,
      List<TableMetadata.MetadataLogEntry> previousFiles,
      Map<String, SnapshotRef> refs,
      List<StatisticsFile> statisticsFiles,
      List<PartitionStatisticsFile> partitionStatisticsFiles,
      long nextRowId,
      List<EncryptedKey> encryptionKeys) {
    return new TableMetadata(
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
        snapshots,
        snapshotsSupplier,
        snapshotLog,
        previousFiles,
        refs,
        statisticsFiles,
        partitionStatisticsFiles,
        nextRowId,
        encryptionKeys,
        ImmutableList.of());
  }

  /** A snapshot-log entry: {@code snapshotId} became current at {@code timestampMillis}. */
  public static HistoryEntry snapshotLogEntry(long timestampMillis, long snapshotId) {
    return new TableMetadata.SnapshotLogEntry(timestampMillis, snapshotId);
  }

  /** A metadata-log entry: {@code file} was the table's metadata until {@code timestampMillis}. */
  public static TableMetadata.MetadataLogEntry metadataLogEntry(long timestampMillis, String file) {
    return new TableMetadata.MetadataLogEntry(timestampMillis, file);
  }

  /** Whether {@code metadata} has made all its snapshots: no supplier is left to call. */
  public static boolean snapshotsLoaded(TableMetadata metadata) {
    return metadata.snapshotsLoaded();
  }

  /**
   * The snapshots that {@code metadata}, built on {@code base}, adds to it, in the order it added
   * them, if that is known without making the snapshots either has not made; else null.
   *
   * <p>It is known when {@code metadata} is a lazy load's {@code base} carried through builders
   * that made no snapshot: both hold the same supplier, and neither has called it. A builder that
   * removes a snapshot, or moves a ref onto one it has not made, makes them all first, so such a
   * {@code metadata} holds every snapshot of {@code base} (the supplier's, and those {@code base}
   * added) and those its own changes past {@code base}'s add. Each of those is numbered past
   * {@code base}'s last sequence number ({@code Builder.addSnapshot}, format 2 and later), which
   * every snapshot of {@code base} is not, so none was in {@code base}. Format 1 numbers nothing,
   * so there it is not known.
   */
  public static List<Snapshot> addedSnapshots(TableMetadata base, TableMetadata metadata) {
    if (base == null
        || metadata.formatVersion() < 2
        || base.snapshotsLoaded()
        || metadata.snapshotsLoaded()
        || metadata.snapshotsSupplier() != base.snapshotsSupplier()) {
      return null;
    }
    List<MetadataUpdate> before = base.changes();
    List<MetadataUpdate> changes = metadata.changes();
    if (changes.size() < before.size()) {
      return null;
    }
    for (int i = 0; i < before.size(); i++) {
      if (changes.get(i) != before.get(i)) {
        return null;
      }
    }
    List<Snapshot> added = new ArrayList<>();
    for (MetadataUpdate change : changes.subList(before.size(), changes.size())) {
      if (change instanceof MetadataUpdate.RemoveSnapshots) {
        return null;
      }
      if (change instanceof MetadataUpdate.AddSnapshot) {
        long id = ((MetadataUpdate.AddSnapshot) change).snapshot().snapshotId();
        // The snapshot as metadata holds it, which its snapshots() would give
        Snapshot held = metadata.snapshotIfKnown(id);
        if (held == null || held.sequenceNumber() <= base.lastSequenceNumber()) {
          return null;
        }
        added.add(held);
      }
    }
    return added;
  }
}
