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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

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

  /** A snapshot-log entry: {@code snapshotId} became current at {@code timestampMillis}. */
  public static HistoryEntry snapshotLogEntry(long timestampMillis, long snapshotId) {
    return new TableMetadata.SnapshotLogEntry(timestampMillis, snapshotId);
  }

  /** A metadata-log entry: {@code file} was the table's metadata until {@code timestampMillis}. */
  public static TableMetadata.MetadataLogEntry metadataLogEntry(long timestampMillis, String file) {
    return new TableMetadata.MetadataLogEntry(timestampMillis, file);
  }
}
