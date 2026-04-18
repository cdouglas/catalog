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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.UnboundPartitionSpec;
import org.apache.iceberg.UnboundSortOrder;

/**
 * Encodes, decodes, and applies inline table metadata deltas.
 *
 * <p>A delta is a list of {@link DeltaUpdate} objects that are applied sequentially to
 * a {@link TableMetadata} base to produce a new metadata state. The wire format uses
 * protobuf-compatible encoding matching the messages in catalog.proto.
 */
public class InlineDeltaCodec {

  // Wire types
  private static final int WIRE_VARINT = 0;
  private static final int WIRE_64BIT = 1;
  private static final int WIRE_LENGTH_DELIMITED = 2;

  // TableMetadataDelta field numbers
  private static final int DELTA_UPDATES = 1;

  // TableMetadataUpdate oneof field numbers
  private static final int UPDATE_ADD_SNAPSHOT = 1;
  private static final int UPDATE_SET_SNAPSHOT_REF = 2;
  private static final int UPDATE_REMOVE_SNAPSHOTS = 3;
  private static final int UPDATE_ADD_SCHEMA = 4;
  private static final int UPDATE_SET_CURRENT_SCHEMA = 5;
  private static final int UPDATE_ADD_PARTITION_SPEC = 6;
  private static final int UPDATE_SET_DEFAULT_SPEC = 7;
  private static final int UPDATE_ADD_SORT_ORDER = 8;
  private static final int UPDATE_SET_DEFAULT_SORT_ORDER = 9;
  private static final int UPDATE_SET_PROPERTIES = 10;
  private static final int UPDATE_SET_TABLE_LOCATION = 11;

  // AddManifestDelta / RemoveManifestDelta field numbers
  private static final int UPDATE_ADD_MANIFEST = 12;
  private static final int UPDATE_REMOVE_MANIFEST = 13;
  private static final int ADD_MF_SNAPSHOT_ID = 1;
  private static final int ADD_MF_MANIFEST = 2;
  private static final int REMOVE_MF_SNAPSHOT_ID = 1;
  private static final int REMOVE_MF_PATH_SUFFIX = 2;

  // SetTableProperties field numbers
  private static final int SET_PROPS_UPDATED = 1;
  private static final int SET_PROPS_REMOVED = 2;

  // SetTableLocation field numbers
  private static final int SET_LOC_LOCATION = 1;

  // Map entry field numbers (proto map<string,string>)
  private static final int MAP_ENTRY_KEY = 1;
  private static final int MAP_ENTRY_VALUE = 2;

  // AddSchema field numbers
  private static final int ADD_SCHEMA_ID = 1;
  private static final int ADD_SCHEMA_LAST_COL_ID = 2;
  private static final int ADD_SCHEMA_JSON = 3;

  // SetCurrentSchema field numbers
  private static final int SET_SCHEMA_ID = 1;

  // AddPartitionSpec field numbers
  private static final int ADD_SPEC_ID = 1;
  private static final int ADD_SPEC_LAST_PART_ID = 2;
  private static final int ADD_SPEC_JSON = 3;

  // SetDefaultPartitionSpec field numbers
  private static final int SET_SPEC_ID = 1;

  // AddSortOrder field numbers
  private static final int ADD_ORDER_ID = 1;
  private static final int ADD_ORDER_JSON = 2;

  // SetDefaultSortOrder field numbers
  private static final int SET_ORDER_ID = 1;

  // SetSnapshotRef field numbers
  private static final int REF_NAME = 1;
  private static final int REF_SNAPSHOT_ID = 2;
  private static final int REF_TYPE = 3;
  private static final int REF_MIN_SNAPSHOTS = 4;
  private static final int REF_MAX_SNAPSHOT_AGE = 5;
  private static final int REF_MAX_REF_AGE = 6;

  // RemoveSnapshots field numbers
  private static final int REMOVE_SNAP_IDS = 1;

  // AddSnapshot field numbers
  private static final int SNAP_ID = 1;
  private static final int SNAP_MANIFEST_SUFFIX = 2;
  private static final int SNAP_SUMMARY = 3;
  private static final int SNAP_TIMESTAMP_DELTA = 4;
  private static final int SNAP_SCHEMA_ID = 5;
  private static final int SNAP_ADDED_ROWS = 6;
  private static final int SNAP_PARENT_ID = 7;    // optional; fixed64
  private static final int SNAP_FIRST_ROW_ID = 8; // optional; varint64
  private static final int SNAP_KEY_ID = 9;       // optional; string

  // CompactSummary field numbers
  private static final int CS_OPERATION = 1;
  private static final int CS_ADDED_DATA_FILES = 2;
  private static final int CS_ADDED_RECORDS = 3;
  private static final int CS_ADDED_FILES_SIZE = 4;
  private static final int CS_TOTAL_DATA_FILES = 5;
  private static final int CS_TOTAL_RECORDS = 6;
  private static final int CS_TOTAL_FILES_SIZE = 7;
  private static final int CS_ADDED_DELETE_FILES = 8;
  private static final int CS_TOTAL_DELETE_FILES = 9;
  private static final int CS_TOTAL_EQUALITY_DELETES = 10;
  private static final int CS_TOTAL_POSITION_DELETES = 11;
  private static final int CS_EXTRA = 15;

  // ============================================================
  // Public API
  // ============================================================

  /** Applies a delta (encoded bytes) to base metadata, returns updated metadata bytes. */
  public static byte[] applyDelta(byte[] baseMetadataJson, byte[] deltaBytes) {
    TableMetadata base = TableMetadataParser.fromJson(
        new String(baseMetadataJson, StandardCharsets.UTF_8));
    List<DeltaUpdate> updates = decodeDelta(deltaBytes);
    TableMetadata result = applyUpdates(base, updates);
    return TableMetadataParser.toJson(result).getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Applies a delta (encoded bytes) to base metadata, routing ML updates to the
   * ProtoCatalogFile.Builder and TM updates (including AddSnapshot) to TableMetadata.
   * This is the ML-aware replacement for {@link #applyDelta(byte[], byte[])}.
   *
   * <p><b>Ordering invariant:</b> {@code AddSnapshotUpdate} entries for a given
   * snapshot must appear in the delta list <em>before</em> any
   * {@code AddManifestUpdate} / {@code RemoveManifestUpdate} entries that
   * reference that snapshot. Manifest carry-forward (inheriting parent's refs
   * when a new snapshot's ref list is empty) reads
   * {@code current.snapshot(add.snapshotId).parentId()}, which only works if
   * the snapshot has already been added to {@code current}.
   * {@link #computeDelta} and {@link #attachManifestDelta} always emit updates
   * in this order; custom callers must maintain it.
   */
  public static byte[] applyDeltaWithManifests(
      byte[] baseMetadataJson, byte[] deltaBytes,
      ProtoCatalogFile.Builder catalogBuilder, int tableId) {
    TableMetadata base = TableMetadataParser.fromJson(
        new String(baseMetadataJson, StandardCharsets.UTF_8));
    List<DeltaUpdate> updates = decodeDelta(deltaBytes);
    String prefix = catalogBuilder.manifestListPrefix(tableId);
    if (prefix == null) {
      prefix = "";
    }

    // Apply updates type-by-type: TM updates to TableMetadata, ML updates to catalog builder.
    // Ordering: AddSnapshot must precede AddManifest/RemoveManifest for the same snapshotId
    // (see javadoc). Track added snapshot ids to assert invariant.
    java.util.Set<Long> addedInThisDelta = new java.util.HashSet<>();
    TableMetadata current = base;
    for (DeltaUpdate update : updates) {
      if (update instanceof AddSnapshotUpdate) {
        // Route through the prefix-accepting overload (avoids the throw in applyTo(Builder))
        AddSnapshotUpdate addSnap = (AddSnapshotUpdate) update;
        current = addSnap.applyTo(current, prefix);
        addedInThisDelta.add(addSnap.snapshotId);
      } else if (update instanceof AddManifestUpdate) {
        AddManifestUpdate add = (AddManifestUpdate) update;
        // Assert ordering: if we're adding a manifest for a snapshot not yet in
        // current, carry-forward lookup will fail to find the parent. Flag it.
        if (current.snapshot(add.snapshotId) == null
            && !addedInThisDelta.contains(add.snapshotId)) {
          throw new IllegalStateException(
              "AddManifestUpdate for snapshot " + add.snapshotId
                  + " before its AddSnapshotUpdate — violates delta ordering invariant");
        }
        // The manifest decoded from the wire has a suffix-only path; resolve full path
        String fullPath = add.manifest.path();
        if (!prefix.isEmpty() && !fullPath.startsWith(prefix)) {
          fullPath = prefix + fullPath;
        }
        ManifestFile resolved = new ProtoCodec.DecodedManifestFile(
            fullPath, add.manifest.length(), add.manifest.partitionSpecId(),
            add.manifest.content().id(),
            add.manifest.sequenceNumber(), add.manifest.minSequenceNumber(),
            add.manifest.snapshotId(),
            add.manifest.addedFilesCount() != null ? add.manifest.addedFilesCount() : 0,
            add.manifest.existingFilesCount() != null ? add.manifest.existingFilesCount() : 0,
            add.manifest.deletedFilesCount() != null ? add.manifest.deletedFilesCount() : 0,
            add.manifest.addedRowsCount() != null ? add.manifest.addedRowsCount() : 0,
            add.manifest.existingRowsCount() != null ? add.manifest.existingRowsCount() : 0,
            add.manifest.deletedRowsCount() != null ? add.manifest.deletedRowsCount() : 0,
            add.manifest.partitions(),
            add.manifest.keyMetadata() != null
                ? java.nio.ByteBuffer.allocate(add.manifest.keyMetadata().remaining())
                    .put(add.manifest.keyMetadata().duplicate()).array()
                : null,
            add.manifest.firstRowId());
        catalogBuilder.addManifestToPool(tableId, resolved);
        List<String> refs = new ArrayList<>(
            catalogBuilder.snapshotManifestPaths(tableId, add.snapshotId));
        // For a new snapshot, initialize refs from the parent snapshot's manifest list
        // (carry-forward: manifests not in the delta are inherited from the parent)
        if (refs.isEmpty()) {
          Snapshot snap = current.snapshot(add.snapshotId);
          if (snap != null && snap.parentId() != null) {
            refs.addAll(catalogBuilder.snapshotManifestPaths(tableId, snap.parentId()));
          }
        }
        refs.add(resolved.path());
        catalogBuilder.setSnapshotManifests(tableId, add.snapshotId, refs);
      } else if (update instanceof RemoveManifestUpdate) {
        RemoveManifestUpdate rm = (RemoveManifestUpdate) update;
        List<String> refs = new ArrayList<>(
            catalogBuilder.snapshotManifestPaths(tableId, rm.snapshotId));
        if (refs.isEmpty()) {
          Snapshot snap = current.snapshot(rm.snapshotId);
          if (snap != null && snap.parentId() != null) {
            refs.addAll(catalogBuilder.snapshotManifestPaths(tableId, snap.parentId()));
          }
        }
        refs.removeIf(p -> p.endsWith(rm.manifestPathSuffix));
        catalogBuilder.setSnapshotManifests(tableId, rm.snapshotId, refs);
      } else if (update instanceof RemoveSnapshotsUpdate) {
        // Cascade snapshot removal to the catalog pool: drop the snapshot's ref
        // list and GC any pool entries no longer referenced. Without this, the
        // pool grows unboundedly as snapshots are expired.
        // See ML_INLINE_REVIEW2.md §2.6 / ML_INLINE_DESIGN_NOTES.md Gap 1.
        RemoveSnapshotsUpdate rs = (RemoveSnapshotsUpdate) update;
        for (long snapId : rs.snapshotIds) {
          catalogBuilder.removeSnapshotManifests(tableId, snapId);
        }
        // Also remove from TableMetadata (Builder.removeSnapshots handles the
        // dangling-refs cleanup for the TM side).
        TableMetadata.Builder tmBuilder = TableMetadata.buildFrom(current);
        update.applyTo(tmBuilder);
        current = tmBuilder.discardChanges().build();
      } else {
        // Schema, properties, sort order, refs, etc. — apply to TableMetadata.Builder
        TableMetadata.Builder tmBuilder = TableMetadata.buildFrom(current);
        update.applyTo(tmBuilder);
        current = tmBuilder.discardChanges().build();
      }
    }
    return TableMetadataParser.toJson(current).getBytes(StandardCharsets.UTF_8);
  }

  /** Applies a list of decoded updates to a base metadata. */
  public static TableMetadata applyUpdates(TableMetadata base, List<DeltaUpdate> updates) {
    TableMetadata.Builder builder = TableMetadata.buildFrom(base);
    for (DeltaUpdate update : updates) {
      update.applyTo(builder);
    }
    return builder.discardChanges().build();
  }

  /** Encodes a list of delta updates to wire bytes. */
  public static byte[] encodeDelta(List<DeltaUpdate> updates) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      for (DeltaUpdate update : updates) {
        byte[] updateBytes = encodeUpdate(update);
        writeLengthDelimited(out, DELTA_UPDATES, updateBytes);
      }
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Decodes delta bytes into a list of updates. */
  public static List<DeltaUpdate> decodeDelta(byte[] bytes) {
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(bytes);
      List<DeltaUpdate> updates = new ArrayList<>();
      while (in.available() > 0) {
        int tag = readVarint(in);
        int fieldNumber = tag >>> 3;
        if (fieldNumber == DELTA_UPDATES) {
          updates.add(decodeUpdate(readLengthDelimitedBytes(in)));
        } else {
          skipField(in, tag & 0x7);
        }
      }
      return updates;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Maximum transaction record size (Azure AppendBlock limit minus safety margin). */
  public static final int APPEND_LIMIT = 4 * 1024 * 1024 - 4096; // ~4 MiB

  // ============================================================
  // computeDelta: diff two TableMetadata instances
  // ============================================================

  /**
   * Computes a minimal delta between old and new TableMetadata.
   *
   * @param oldMeta base metadata (from the inline table's current state)
   * @param newMeta target metadata (after applying the table operation)
   * @param manifestListPrefix shared prefix for manifest list paths
   * @return list of delta updates, or null if metadata is identical
   */
  public static List<DeltaUpdate> computeDelta(
      TableMetadata oldMeta, TableMetadata newMeta, String manifestListPrefix) {
    List<DeltaUpdate> updates = new ArrayList<>();

    // New snapshots (most common path: data commits)
    Set<Long> oldSnapIds = new HashSet<>();
    for (org.apache.iceberg.Snapshot s : oldMeta.snapshots()) {
      oldSnapIds.add(s.snapshotId());
    }
    for (org.apache.iceberg.Snapshot snap : newMeta.snapshots()) {
      if (!oldSnapIds.contains(snap.snapshotId())) {
        // InlineSnapshot has null manifestListLocation (no Avro file written);
        // use empty suffix — the actual ML data is in AddManifestUpdate entries.
        String manifestList = snap.manifestListLocation();
        String suffix = "";
        if (manifestList != null) {
          suffix = manifestList;
          if (manifestListPrefix != null && !manifestListPrefix.isEmpty()
              && manifestList.startsWith(manifestListPrefix)) {
            suffix = manifestList.substring(manifestListPrefix.length());
          }
        }
        long timestampDelta = snap.timestampMillis() - oldMeta.lastUpdatedMillis();
        int schemaId = snap.schemaId() != null
            && snap.schemaId() != oldMeta.currentSchemaId()
            ? snap.schemaId() : 0;
        // Always carry parent snapshot ID explicitly. This is required for
        // stage-only, branch, cherry-pick; reconstructing from base.currentSnapshot
        // on replay is wrong in those cases.
        Long parentId = snap.parentId();
        Long firstRowId = snap.firstRowId();
        String keyId = snap.keyId();
        long addedRowsValue = snap.addedRows() != null ? snap.addedRows() : 0L;
        updates.add(new AddSnapshotUpdate(
            snap.snapshotId(), suffix,
            snap.summary() != null ? snap.summary() : Map.of(),
            timestampDelta, schemaId, addedRowsValue,
            parentId, firstRowId, keyId));
      }
    }

    // Removed snapshots
    Set<Long> newSnapIds = new HashSet<>();
    for (org.apache.iceberg.Snapshot s : newMeta.snapshots()) {
      newSnapIds.add(s.snapshotId());
    }
    List<Long> removedSnaps = new ArrayList<>();
    for (long id : oldSnapIds) {
      if (!newSnapIds.contains(id)) {
        removedSnaps.add(id);
      }
    }
    if (!removedSnaps.isEmpty()) {
      updates.add(new RemoveSnapshotsUpdate(removedSnaps));
    }

    // Ref changes
    Map<String, org.apache.iceberg.SnapshotRef> oldRefs = oldMeta.refs();
    for (Map.Entry<String, org.apache.iceberg.SnapshotRef> entry : newMeta.refs().entrySet()) {
      org.apache.iceberg.SnapshotRef oldRef = oldRefs.get(entry.getKey());
      org.apache.iceberg.SnapshotRef newRef = entry.getValue();
      if (oldRef == null || !refsEqual(oldRef, newRef)) {
        updates.add(new SetSnapshotRefUpdate(
            entry.getKey(), newRef.snapshotId(),
            newRef.isBranch() ? "branch" : "tag",
            newRef.minSnapshotsToKeep() != null ? newRef.minSnapshotsToKeep() : 0,
            newRef.maxSnapshotAgeMs() != null ? newRef.maxSnapshotAgeMs() : 0,
            newRef.maxRefAgeMs() != null ? newRef.maxRefAgeMs() : 0));
      }
    }

    // Schema changes
    Set<Integer> oldSchemaIds = new HashSet<>();
    for (org.apache.iceberg.Schema s : oldMeta.schemas()) {
      oldSchemaIds.add(s.schemaId());
    }
    for (org.apache.iceberg.Schema schema : newMeta.schemas()) {
      if (!oldSchemaIds.contains(schema.schemaId())) {
        String json = org.apache.iceberg.SchemaParser.toJson(schema);
        updates.add(new AddSchemaUpdate(
            schema.schemaId(), newMeta.lastColumnId(),
            json.getBytes(StandardCharsets.UTF_8)));
      }
    }
    if (newMeta.currentSchemaId() != oldMeta.currentSchemaId()) {
      updates.add(new SetCurrentSchemaUpdate(newMeta.currentSchemaId()));
    }

    // Partition spec changes
    Set<Integer> oldSpecIds = new HashSet<>();
    for (org.apache.iceberg.PartitionSpec s : oldMeta.specs()) {
      oldSpecIds.add(s.specId());
    }
    for (org.apache.iceberg.PartitionSpec spec : newMeta.specs()) {
      if (!oldSpecIds.contains(spec.specId())) {
        String json = org.apache.iceberg.PartitionSpecParser.toJson(spec);
        updates.add(new AddPartitionSpecUpdate(
            spec.specId(), newMeta.lastAssignedPartitionId(),
            json.getBytes(StandardCharsets.UTF_8)));
      }
    }
    if (newMeta.defaultSpecId() != oldMeta.defaultSpecId()) {
      updates.add(new SetDefaultSpecUpdate(newMeta.defaultSpecId()));
    }

    // Sort order changes
    Set<Integer> oldOrderIds = new HashSet<>();
    for (org.apache.iceberg.SortOrder o : oldMeta.sortOrders()) {
      oldOrderIds.add(o.orderId());
    }
    for (org.apache.iceberg.SortOrder order : newMeta.sortOrders()) {
      if (!oldOrderIds.contains(order.orderId())) {
        String json = org.apache.iceberg.SortOrderParser.toJson(order);
        updates.add(new AddSortOrderUpdate(
            order.orderId(), json.getBytes(StandardCharsets.UTF_8)));
      }
    }
    if (newMeta.defaultSortOrderId() != oldMeta.defaultSortOrderId()) {
      updates.add(new SetDefaultSortOrderUpdate(newMeta.defaultSortOrderId()));
    }

    // Property changes
    Map<String, String> addedOrChanged = new HashMap<>();
    Set<String> removed = new HashSet<>();
    for (Map.Entry<String, String> e : newMeta.properties().entrySet()) {
      String oldVal = oldMeta.properties().get(e.getKey());
      if (!e.getValue().equals(oldVal)) {
        addedOrChanged.put(e.getKey(), e.getValue());
      }
    }
    for (String key : oldMeta.properties().keySet()) {
      if (!newMeta.properties().containsKey(key)) {
        removed.add(key);
      }
    }
    if (!addedOrChanged.isEmpty() || !removed.isEmpty()) {
      updates.add(new SetPropertiesUpdate(addedOrChanged, removed));
    }

    // Location change
    if (!newMeta.location().equals(oldMeta.location())) {
      updates.add(new SetLocationUpdate(newMeta.location()));
    }

    return updates.isEmpty() ? null : updates;
  }

  private static boolean refsEqual(
      org.apache.iceberg.SnapshotRef a, org.apache.iceberg.SnapshotRef b) {
    return a.snapshotId() == b.snapshotId()
        && a.isBranch() == b.isBranch()
        && java.util.Objects.equals(a.minSnapshotsToKeep(), b.minSnapshotsToKeep())
        && java.util.Objects.equals(a.maxSnapshotAgeMs(), b.maxSnapshotAgeMs())
        && java.util.Objects.equals(a.maxRefAgeMs(), b.maxRefAgeMs());
  }

  /**
   * Selects the delivery mode for an inline table update.
   *
   * @return "delta" if delta fits, "full" if full metadata fits, "pointer" otherwise
   */
  public static String selectMode(
      List<DeltaUpdate> delta, TableMetadata newMeta, int currentTxnSize) {
    if (delta != null) {
      byte[] deltaBytes = encodeDelta(delta);
      if (currentTxnSize + deltaBytes.length <= APPEND_LIMIT) {
        return "delta";
      }
    }
    String fullJson = TableMetadataParser.toJson(newMeta);
    if (currentTxnSize + fullJson.length() <= APPEND_LIMIT) {
      return "full";
    }
    return "pointer";
  }

  // ============================================================
  // DeltaUpdate interface and implementations
  // ============================================================

  /** A single metadata update that can be applied to a TableMetadata.Builder. */
  public interface DeltaUpdate {
    void applyTo(TableMetadata.Builder builder);
  }

  public static class SetPropertiesUpdate implements DeltaUpdate {
    public final Map<String, String> updated;
    public final Set<String> removed;

    public SetPropertiesUpdate(Map<String, String> updated, Set<String> removed) {
      this.updated = updated;
      this.removed = removed;
    }

    @Override
    public void applyTo(TableMetadata.Builder builder) {
      if (!updated.isEmpty()) {
        builder.setProperties(updated);
      }
      if (!removed.isEmpty()) {
        builder.removeProperties(removed);
      }
    }
  }

  public static class SetLocationUpdate implements DeltaUpdate {
    public final String location;

    public SetLocationUpdate(String location) {
      this.location = location;
    }

    @Override
    public void applyTo(TableMetadata.Builder builder) {
      builder.setLocation(location);
    }
  }

  public static class AddSchemaUpdate implements DeltaUpdate {
    public final int schemaId;
    public final int lastColumnId;
    public final byte[] schemaJson;

    public AddSchemaUpdate(int schemaId, int lastColumnId, byte[] schemaJson) {
      this.schemaId = schemaId;
      this.lastColumnId = lastColumnId;
      this.schemaJson = schemaJson;
    }

    @Override
    public void applyTo(TableMetadata.Builder builder) {
      Schema schema = SchemaParser.fromJson(new String(schemaJson, StandardCharsets.UTF_8));
      builder.addSchema(schema);
    }
  }

  public static class SetCurrentSchemaUpdate implements DeltaUpdate {
    public final int schemaId;

    public SetCurrentSchemaUpdate(int schemaId) {
      this.schemaId = schemaId;
    }

    @Override
    public void applyTo(TableMetadata.Builder builder) {
      builder.setCurrentSchema(schemaId);
    }
  }

  public static class AddPartitionSpecUpdate implements DeltaUpdate {
    public final int specId;
    public final int lastPartitionId;
    public final byte[] specJson;

    public AddPartitionSpecUpdate(int specId, int lastPartitionId, byte[] specJson) {
      this.specId = specId;
      this.lastPartitionId = lastPartitionId;
      this.specJson = specJson;
    }

    @Override
    public void applyTo(TableMetadata.Builder builder) {
      try {
        com.fasterxml.jackson.databind.JsonNode node =
            com.fasterxml.jackson.databind.ObjectMapper.class.newInstance()
                .readTree(new String(specJson, StandardCharsets.UTF_8));
        UnboundPartitionSpec spec = PartitionSpecParser.fromJson(node);
        builder.addPartitionSpec(spec);
      } catch (Exception e) {
        throw new RuntimeException("Failed to parse partition spec JSON", e);
      }
    }
  }

  public static class SetDefaultSpecUpdate implements DeltaUpdate {
    public final int specId;

    public SetDefaultSpecUpdate(int specId) {
      this.specId = specId;
    }

    @Override
    public void applyTo(TableMetadata.Builder builder) {
      builder.setDefaultPartitionSpec(specId);
    }
  }

  public static class AddSortOrderUpdate implements DeltaUpdate {
    public final int orderId;
    public final byte[] orderJson;

    public AddSortOrderUpdate(int orderId, byte[] orderJson) {
      this.orderId = orderId;
      this.orderJson = orderJson;
    }

    @Override
    public void applyTo(TableMetadata.Builder builder) {
      UnboundSortOrder order = SortOrderParser.fromJson(
          new String(orderJson, StandardCharsets.UTF_8));
      builder.addSortOrder(order);
    }

  }

  public static class SetDefaultSortOrderUpdate implements DeltaUpdate {
    public final int orderId;

    public SetDefaultSortOrderUpdate(int orderId) {
      this.orderId = orderId;
    }

    @Override
    public void applyTo(TableMetadata.Builder builder) {
      builder.setDefaultSortOrder(orderId);
    }
  }

  public static class SetSnapshotRefUpdate implements DeltaUpdate {
    public final String refName;
    public final long snapshotId;
    public final String refType;
    public final int minSnapshotsToKeep;
    public final long maxSnapshotAgeMs;
    public final long maxRefAgeMs;

    public SetSnapshotRefUpdate(
        String refName, long snapshotId, String refType,
        int minSnapshotsToKeep, long maxSnapshotAgeMs, long maxRefAgeMs) {
      this.refName = refName;
      this.snapshotId = snapshotId;
      this.refType = refType;
      this.minSnapshotsToKeep = minSnapshotsToKeep;
      this.maxSnapshotAgeMs = maxSnapshotAgeMs;
      this.maxRefAgeMs = maxRefAgeMs;
    }

    @Override
    public void applyTo(TableMetadata.Builder builder) {
      org.apache.iceberg.SnapshotRef.Builder refBuilder;
      if ("tag".equals(refType)) {
        refBuilder = org.apache.iceberg.SnapshotRef.tagBuilder(snapshotId);
      } else {
        refBuilder = org.apache.iceberg.SnapshotRef.branchBuilder(snapshotId);
      }
      if (minSnapshotsToKeep > 0) {
        refBuilder.minSnapshotsToKeep(minSnapshotsToKeep);
      }
      if (maxSnapshotAgeMs > 0) {
        refBuilder.maxSnapshotAgeMs(maxSnapshotAgeMs);
      }
      if (maxRefAgeMs > 0) {
        refBuilder.maxRefAgeMs(maxRefAgeMs);
      }
      builder.setRef(refName, refBuilder.build());
    }
  }

  public static class RemoveSnapshotsUpdate implements DeltaUpdate {
    public final List<Long> snapshotIds;

    public RemoveSnapshotsUpdate(List<Long> snapshotIds) {
      this.snapshotIds = snapshotIds;
    }

    @Override
    public void applyTo(TableMetadata.Builder builder) {
      builder.removeSnapshots(snapshotIds);
    }
  }

  public static class AddSnapshotUpdate implements DeltaUpdate {
    public final long snapshotId;
    public final String manifestListSuffix;
    public final Map<String, String> summary;
    public final long timestampDeltaMs;
    public final int schemaId;
    public final long addedRows;
    // Optional fields. Absent values:
    //   parentSnapshotId == null  -> fall back to base.currentSnapshot()
    //   firstRowId == null        -> no row lineage
    //   keyId == null             -> no encryption key
    public final Long parentSnapshotId;
    public final Long firstRowId;
    public final String keyId;

    /** Back-compat constructor: no explicit parent/firstRowId/keyId. */
    public AddSnapshotUpdate(
        long snapshotId, String manifestListSuffix, Map<String, String> summary,
        long timestampDeltaMs, int schemaId, long addedRows) {
      this(snapshotId, manifestListSuffix, summary, timestampDeltaMs, schemaId,
          addedRows, null, null, null);
    }

    /** Full constructor with optional parent snapshot id, first row id, key id. */
    public AddSnapshotUpdate(
        long snapshotId, String manifestListSuffix, Map<String, String> summary,
        long timestampDeltaMs, int schemaId, long addedRows,
        Long parentSnapshotId, Long firstRowId, String keyId) {
      this.snapshotId = snapshotId;
      this.manifestListSuffix = manifestListSuffix;
      this.summary = summary;
      this.timestampDeltaMs = timestampDeltaMs;
      this.schemaId = schemaId;
      this.addedRows = addedRows;
      this.parentSnapshotId = parentSnapshotId;
      this.firstRowId = firstRowId;
      this.keyId = keyId;
    }

    /**
     * Applies the snapshot to metadata using dictionary-derived fields.
     * Requires the manifest list prefix from the inline table's checkpoint entry.
     */
    public TableMetadata applyTo(TableMetadata base, String manifestListPrefix) {
      long seqNum = base.lastSequenceNumber() + 1;
      // Prefer explicit parentSnapshotId (correct for stage-only, branch, cherry-pick).
      // Fall back to base.currentSnapshot() for older-format deltas (backward compat).
      long resolvedParentId;
      if (parentSnapshotId != null) {
        resolvedParentId = parentSnapshotId;
      } else if (base.currentSnapshot() != null) {
        resolvedParentId = base.currentSnapshot().snapshotId();
      } else {
        resolvedParentId = -1;
      }
      long timestamp = base.lastUpdatedMillis() + timestampDeltaMs;
      int resolvedSchemaId = schemaId > 0 ? schemaId : base.currentSchemaId();
      // For inline snapshots (empty suffix), use a sentinel that SnapshotParser.toJson
      // will write as a manifest-list location (not the v1 embedded path)
      String manifestList = manifestListSuffix.isEmpty()
          ? "inline://" + snapshotId
          : manifestListPrefix + manifestListSuffix;

      // Build snapshot via JSON + SnapshotParser (BaseSnapshot is package-private)
      StringBuilder json = new StringBuilder("{");
      json.append("\"snapshot-id\":").append(snapshotId);
      if (resolvedParentId >= 0) {
        json.append(",\"parent-snapshot-id\":").append(resolvedParentId);
      }
      json.append(",\"sequence-number\":").append(seqNum);
      json.append(",\"timestamp-ms\":").append(timestamp);
      json.append(",\"schema-id\":").append(resolvedSchemaId);
      json.append(",\"manifest-list\":\"").append(escapeJson(manifestList)).append("\"");
      json.append(",\"summary\":{\"operation\":\"")
          .append(escapeJson(summary.getOrDefault("operation", "append")))
          .append("\"");
      for (Map.Entry<String, String> e : summary.entrySet()) {
        if (!"operation".equals(e.getKey())) {
          json.append(",\"").append(escapeJson(e.getKey()))
              .append("\":\"").append(escapeJson(e.getValue())).append("\"");
        }
      }
      json.append("}}");
      if (firstRowId != null) {
        // Insert first-row-id before the closing brace
        json.setLength(json.length() - 1);
        json.append(",\"first-row-id\":").append(firstRowId).append("}");
      }
      if (addedRows > 0) {
        // Insert added-rows (already tracked, but SnapshotParser may expect explicit field)
        // Note: SnapshotParser.toJson emits added_rows only if present; we preserve symmetry.
      }
      if (keyId != null && !keyId.isEmpty()) {
        json.setLength(json.length() - 1);
        json.append(",\"key-id\":\"").append(escapeJson(keyId)).append("\"}");
      }
      Snapshot snapshot = SnapshotParser.fromJson(json.toString());

      return TableMetadata.buildFrom(base)
          .addSnapshot(snapshot)
          .discardChanges()
          .build();
    }

    private static String escapeJson(String s) {
      return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    @Override
    public void applyTo(TableMetadata.Builder builder) {
      // AddSnapshot requires the manifest list prefix from the inline table entry,
      // which isn't available through the Builder interface alone.
      // Use applyTo(TableMetadata, String) instead for full reconstruction.
      throw new UnsupportedOperationException(
          "AddSnapshot requires manifest list prefix; use applyTo(TableMetadata, String)");
    }
  }

  /**
   * Adds a ManifestFile to a snapshot's inline manifest list. During log replay, this
   * adds the manifest to the table's pool and appends it to the snapshot's reference list.
   */
  public static class AddManifestUpdate implements DeltaUpdate {
    public final long snapshotId;
    public final ManifestFile manifest;
    public final String pathPrefix;

    public AddManifestUpdate(long snapshotId, ManifestFile manifest, String pathPrefix) {
      this.snapshotId = snapshotId;
      this.manifest = manifest;
      this.pathPrefix = pathPrefix;
    }

    @Override
    public void applyTo(TableMetadata.Builder builder) {
      // ML deltas are applied to ProtoCatalogFile.Builder, not TableMetadata.Builder.
      // This is a no-op here; handled by applyManifestDelta.
    }
  }

  /**
   * Removes a ManifestFile from a snapshot's inline manifest list by path suffix.
   */
  public static class RemoveManifestUpdate implements DeltaUpdate {
    public final long snapshotId;
    public final String manifestPathSuffix;

    public RemoveManifestUpdate(long snapshotId, String manifestPathSuffix) {
      this.snapshotId = snapshotId;
      this.manifestPathSuffix = manifestPathSuffix;
    }

    @Override
    public void applyTo(TableMetadata.Builder builder) {
      // ML deltas are applied to ProtoCatalogFile.Builder, not TableMetadata.Builder.
      // This is a no-op here; handled by applyManifestDelta.
    }
  }

  /**
   * Appends AddManifestUpdate/RemoveManifestUpdate entries to a delta for all manifests
   * in a ManifestListDelta from SnapshotProducer.
   *
   * @param delta existing list of delta updates to extend
   * @param snapshotId snapshot these manifests belong to
   * @param added manifests to add (finalized by SnapshotProducer)
   * @param removedPaths paths of manifests to remove
   * @param pathPrefix shared manifest path prefix for efficient encoding
   */
  public static void attachManifestDelta(
      List<DeltaUpdate> delta, long snapshotId,
      List<ManifestFile> added, List<String> removedPaths, String pathPrefix) {
    for (ManifestFile mf : added) {
      delta.add(new AddManifestUpdate(snapshotId, mf, pathPrefix));
    }
    for (String path : removedPaths) {
      String suffix = path;
      if (pathPrefix != null && !pathPrefix.isEmpty() && path.startsWith(pathPrefix)) {
        suffix = path.substring(pathPrefix.length());
      }
      delta.add(new RemoveManifestUpdate(snapshotId, suffix));
    }
  }

  // ============================================================
  // Encoding
  // ============================================================

  private static byte[] encodeUpdate(DeltaUpdate update) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    if (update instanceof SetPropertiesUpdate) {
      SetPropertiesUpdate u = (SetPropertiesUpdate) update;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      for (Map.Entry<String, String> entry : u.updated.entrySet()) {
        inner.write(encodeMapEntry(SET_PROPS_UPDATED, entry.getKey(), entry.getValue()));
      }
      for (String key : u.removed) {
        writeString(inner, SET_PROPS_REMOVED, key);
      }
      writeLengthDelimited(out, UPDATE_SET_PROPERTIES, inner.toByteArray());

    } else if (update instanceof SetLocationUpdate) {
      SetLocationUpdate u = (SetLocationUpdate) update;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeString(inner, SET_LOC_LOCATION, u.location);
      writeLengthDelimited(out, UPDATE_SET_TABLE_LOCATION, inner.toByteArray());

    } else if (update instanceof AddSchemaUpdate) {
      AddSchemaUpdate u = (AddSchemaUpdate) update;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeVarint(inner, ADD_SCHEMA_ID, u.schemaId);
      writeVarint(inner, ADD_SCHEMA_LAST_COL_ID, u.lastColumnId);
      writeBytes(inner, ADD_SCHEMA_JSON, u.schemaJson);
      writeLengthDelimited(out, UPDATE_ADD_SCHEMA, inner.toByteArray());

    } else if (update instanceof SetCurrentSchemaUpdate) {
      SetCurrentSchemaUpdate u = (SetCurrentSchemaUpdate) update;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeVarint(inner, SET_SCHEMA_ID, u.schemaId);
      writeLengthDelimited(out, UPDATE_SET_CURRENT_SCHEMA, inner.toByteArray());

    } else if (update instanceof AddPartitionSpecUpdate) {
      AddPartitionSpecUpdate u = (AddPartitionSpecUpdate) update;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeVarint(inner, ADD_SPEC_ID, u.specId);
      writeVarint(inner, ADD_SPEC_LAST_PART_ID, u.lastPartitionId);
      writeBytes(inner, ADD_SPEC_JSON, u.specJson);
      writeLengthDelimited(out, UPDATE_ADD_PARTITION_SPEC, inner.toByteArray());

    } else if (update instanceof SetDefaultSpecUpdate) {
      SetDefaultSpecUpdate u = (SetDefaultSpecUpdate) update;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeVarint(inner, SET_SPEC_ID, u.specId);
      writeLengthDelimited(out, UPDATE_SET_DEFAULT_SPEC, inner.toByteArray());

    } else if (update instanceof AddSortOrderUpdate) {
      AddSortOrderUpdate u = (AddSortOrderUpdate) update;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeVarint(inner, ADD_ORDER_ID, u.orderId);
      writeBytes(inner, ADD_ORDER_JSON, u.orderJson);
      writeLengthDelimited(out, UPDATE_ADD_SORT_ORDER, inner.toByteArray());

    } else if (update instanceof SetDefaultSortOrderUpdate) {
      SetDefaultSortOrderUpdate u = (SetDefaultSortOrderUpdate) update;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeVarint(inner, SET_ORDER_ID, u.orderId);
      writeLengthDelimited(out, UPDATE_SET_DEFAULT_SORT_ORDER, inner.toByteArray());

    } else if (update instanceof SetSnapshotRefUpdate) {
      SetSnapshotRefUpdate u = (SetSnapshotRefUpdate) update;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeString(inner, REF_NAME, u.refName);
      writeFixed64(inner, REF_SNAPSHOT_ID, u.snapshotId);
      writeString(inner, REF_TYPE, u.refType);
      if (u.minSnapshotsToKeep > 0) {
        writeVarint(inner, REF_MIN_SNAPSHOTS, u.minSnapshotsToKeep);
      }
      if (u.maxSnapshotAgeMs > 0) {
        writeVarint64(inner, REF_MAX_SNAPSHOT_AGE, u.maxSnapshotAgeMs);
      }
      if (u.maxRefAgeMs > 0) {
        writeVarint64(inner, REF_MAX_REF_AGE, u.maxRefAgeMs);
      }
      writeLengthDelimited(out, UPDATE_SET_SNAPSHOT_REF, inner.toByteArray());

    } else if (update instanceof RemoveSnapshotsUpdate) {
      RemoveSnapshotsUpdate u = (RemoveSnapshotsUpdate) update;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      for (long id : u.snapshotIds) {
        writeVarint64(inner, REMOVE_SNAP_IDS, id);
      }
      writeLengthDelimited(out, UPDATE_REMOVE_SNAPSHOTS, inner.toByteArray());

    } else if (update instanceof AddSnapshotUpdate) {
      AddSnapshotUpdate u = (AddSnapshotUpdate) update;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeFixed64(inner, SNAP_ID, u.snapshotId);
      writeString(inner, SNAP_MANIFEST_SUFFIX, u.manifestListSuffix);
      // encode summary as CompactSummary
      writeLengthDelimited(inner, SNAP_SUMMARY, encodeCompactSummary(u.summary));
      writeSVarint64(inner, SNAP_TIMESTAMP_DELTA, u.timestampDeltaMs);
      if (u.schemaId > 0) {
        writeVarint(inner, SNAP_SCHEMA_ID, u.schemaId);
      }
      if (u.addedRows > 0) {
        writeVarint64(inner, SNAP_ADDED_ROWS, u.addedRows);
      }
      // Optional fields for stage-only/branch/cherry-pick/v3+/encrypted tables
      if (u.parentSnapshotId != null) {
        writeFixed64(inner, SNAP_PARENT_ID, u.parentSnapshotId);
      }
      if (u.firstRowId != null) {
        writeVarint64(inner, SNAP_FIRST_ROW_ID, u.firstRowId);
      }
      if (u.keyId != null && !u.keyId.isEmpty()) {
        writeString(inner, SNAP_KEY_ID, u.keyId);
      }
      writeLengthDelimited(out, UPDATE_ADD_SNAPSHOT, inner.toByteArray());

    } else if (update instanceof AddManifestUpdate) {
      AddManifestUpdate u = (AddManifestUpdate) update;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeFixed64(inner, ADD_MF_SNAPSHOT_ID, u.snapshotId);
      byte[] mfBytes = ProtoCodec.encodeManifestFileEntry(u.manifest, u.pathPrefix);
      writeLengthDelimited(inner, ADD_MF_MANIFEST, mfBytes);
      writeLengthDelimited(out, UPDATE_ADD_MANIFEST, inner.toByteArray());

    } else if (update instanceof RemoveManifestUpdate) {
      RemoveManifestUpdate u = (RemoveManifestUpdate) update;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeFixed64(inner, REMOVE_MF_SNAPSHOT_ID, u.snapshotId);
      writeString(inner, REMOVE_MF_PATH_SUFFIX, u.manifestPathSuffix);
      writeLengthDelimited(out, UPDATE_REMOVE_MANIFEST, inner.toByteArray());
    }

    return out.toByteArray();
  }

  // ============================================================
  // Decoding
  // ============================================================

  private static DeltaUpdate decodeUpdate(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int tag = readVarint(in);
    int updateType = tag >>> 3;
    byte[] updateBytes = readLengthDelimitedBytes(in);

    switch (updateType) {
      case UPDATE_SET_PROPERTIES:
        return decodeSetProperties(updateBytes);
      case UPDATE_SET_TABLE_LOCATION:
        return decodeSetLocation(updateBytes);
      case UPDATE_ADD_SCHEMA:
        return decodeAddSchema(updateBytes);
      case UPDATE_SET_CURRENT_SCHEMA:
        return decodeSetCurrentSchema(updateBytes);
      case UPDATE_ADD_PARTITION_SPEC:
        return decodeAddPartitionSpec(updateBytes);
      case UPDATE_SET_DEFAULT_SPEC:
        return decodeSetDefaultSpec(updateBytes);
      case UPDATE_ADD_SORT_ORDER:
        return decodeAddSortOrder(updateBytes);
      case UPDATE_SET_DEFAULT_SORT_ORDER:
        return decodeSetDefaultSortOrder(updateBytes);
      case UPDATE_SET_SNAPSHOT_REF:
        return decodeSetSnapshotRef(updateBytes);
      case UPDATE_REMOVE_SNAPSHOTS:
        return decodeRemoveSnapshots(updateBytes);
      case UPDATE_ADD_SNAPSHOT:
        return decodeAddSnapshot(updateBytes);
      case UPDATE_ADD_MANIFEST:
        return decodeAddManifest(updateBytes);
      case UPDATE_REMOVE_MANIFEST:
        return decodeRemoveManifest(updateBytes);
      default:
        // Forward compatibility: skip unknown update types
        return builder -> {};
    }
  }

  private static AddManifestUpdate decodeAddManifest(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    long snapshotId = 0;
    ManifestFile manifest = null;

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fn = tag >>> 3;
      switch (fn) {
        case ADD_MF_SNAPSHOT_ID:
          snapshotId = readFixed64(in);
          break;
        case ADD_MF_MANIFEST:
          // Decode with empty prefix; full path will be resolved by the caller
          manifest = ProtoCodec.decodeManifestFileEntry(readLengthDelimitedBytes(in), "");
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    return new AddManifestUpdate(snapshotId, manifest, "");
  }

  private static RemoveManifestUpdate decodeRemoveManifest(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    long snapshotId = 0;
    String pathSuffix = "";

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fn = tag >>> 3;
      switch (fn) {
        case REMOVE_MF_SNAPSHOT_ID:
          snapshotId = readFixed64(in);
          break;
        case REMOVE_MF_PATH_SUFFIX:
          pathSuffix = readString(in);
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    return new RemoveManifestUpdate(snapshotId, pathSuffix);
  }

  private static SetPropertiesUpdate decodeSetProperties(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    Map<String, String> updated = new HashMap<>();
    Set<String> removed = new HashSet<>();

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fieldNumber = tag >>> 3;
      switch (fieldNumber) {
        case SET_PROPS_UPDATED:
          decodeMapEntry(readLengthDelimitedBytes(in), updated);
          break;
        case SET_PROPS_REMOVED:
          removed.add(readString(in));
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    return new SetPropertiesUpdate(updated, removed);
  }

  private static SetLocationUpdate decodeSetLocation(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    String location = "";
    while (in.available() > 0) {
      int tag = readVarint(in);
      int fieldNumber = tag >>> 3;
      if (fieldNumber == SET_LOC_LOCATION) {
        location = readString(in);
      } else {
        skipField(in, tag & 0x7);
      }
    }
    return new SetLocationUpdate(location);
  }

  private static AddSchemaUpdate decodeAddSchema(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int schemaId = 0, lastColId = 0;
    byte[] json = new byte[0];
    while (in.available() > 0) {
      int tag = readVarint(in);
      int fn = tag >>> 3;
      switch (fn) {
        case ADD_SCHEMA_ID: schemaId = readVarint(in); break;
        case ADD_SCHEMA_LAST_COL_ID: lastColId = readVarint(in); break;
        case ADD_SCHEMA_JSON: json = readLengthDelimitedBytes(in); break;
        default: skipField(in, tag & 0x7);
      }
    }
    return new AddSchemaUpdate(schemaId, lastColId, json);
  }

  private static SetCurrentSchemaUpdate decodeSetCurrentSchema(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int schemaId = 0;
    while (in.available() > 0) {
      int tag = readVarint(in);
      if ((tag >>> 3) == SET_SCHEMA_ID) { schemaId = readVarint(in); }
      else { skipField(in, tag & 0x7); }
    }
    return new SetCurrentSchemaUpdate(schemaId);
  }

  private static AddPartitionSpecUpdate decodeAddPartitionSpec(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int specId = 0, lastPartId = 0;
    byte[] json = new byte[0];
    while (in.available() > 0) {
      int tag = readVarint(in);
      int fn = tag >>> 3;
      switch (fn) {
        case ADD_SPEC_ID: specId = readVarint(in); break;
        case ADD_SPEC_LAST_PART_ID: lastPartId = readVarint(in); break;
        case ADD_SPEC_JSON: json = readLengthDelimitedBytes(in); break;
        default: skipField(in, tag & 0x7);
      }
    }
    return new AddPartitionSpecUpdate(specId, lastPartId, json);
  }

  private static SetDefaultSpecUpdate decodeSetDefaultSpec(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int specId = 0;
    while (in.available() > 0) {
      int tag = readVarint(in);
      if ((tag >>> 3) == SET_SPEC_ID) { specId = readVarint(in); }
      else { skipField(in, tag & 0x7); }
    }
    return new SetDefaultSpecUpdate(specId);
  }

  private static AddSortOrderUpdate decodeAddSortOrder(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int orderId = 0;
    byte[] json = new byte[0];
    while (in.available() > 0) {
      int tag = readVarint(in);
      int fn = tag >>> 3;
      switch (fn) {
        case ADD_ORDER_ID: orderId = readVarint(in); break;
        case ADD_ORDER_JSON: json = readLengthDelimitedBytes(in); break;
        default: skipField(in, tag & 0x7);
      }
    }
    return new AddSortOrderUpdate(orderId, json);
  }

  private static SetDefaultSortOrderUpdate decodeSetDefaultSortOrder(byte[] bytes)
      throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int orderId = 0;
    while (in.available() > 0) {
      int tag = readVarint(in);
      if ((tag >>> 3) == SET_ORDER_ID) { orderId = readVarint(in); }
      else { skipField(in, tag & 0x7); }
    }
    return new SetDefaultSortOrderUpdate(orderId);
  }

  private static SetSnapshotRefUpdate decodeSetSnapshotRef(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    String refName = "", refType = "branch";
    long snapshotId = 0;
    int minKeep = 0;
    long maxSnapAge = 0, maxRefAge = 0;
    while (in.available() > 0) {
      int tag = readVarint(in);
      int fn = tag >>> 3;
      switch (fn) {
        case REF_NAME: refName = readString(in); break;
        case REF_SNAPSHOT_ID: snapshotId = readFixed64(in); break;
        case REF_TYPE: refType = readString(in); break;
        case REF_MIN_SNAPSHOTS: minKeep = readVarint(in); break;
        case REF_MAX_SNAPSHOT_AGE: maxSnapAge = readVarint64(in); break;
        case REF_MAX_REF_AGE: maxRefAge = readVarint64(in); break;
        default: skipField(in, tag & 0x7);
      }
    }
    return new SetSnapshotRefUpdate(refName, snapshotId, refType, minKeep, maxSnapAge, maxRefAge);
  }

  private static RemoveSnapshotsUpdate decodeRemoveSnapshots(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    List<Long> ids = new ArrayList<>();
    while (in.available() > 0) {
      int tag = readVarint(in);
      if ((tag >>> 3) == REMOVE_SNAP_IDS) { ids.add(readVarint64(in)); }
      else { skipField(in, tag & 0x7); }
    }
    return new RemoveSnapshotsUpdate(ids);
  }

  private static AddSnapshotUpdate decodeAddSnapshot(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    long snapId = 0, timestampDelta = 0, addedRows = 0;
    String manifestSuffix = "";
    Map<String, String> summary = new HashMap<>();
    int schemaId = 0;
    Long parentId = null;
    Long firstRowId = null;
    String keyId = null;

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fn = tag >>> 3;
      switch (fn) {
        case SNAP_ID: snapId = readFixed64(in); break;
        case SNAP_MANIFEST_SUFFIX: manifestSuffix = readString(in); break;
        case SNAP_SUMMARY:
          summary = decodeCompactSummary(readLengthDelimitedBytes(in));
          break;
        case SNAP_TIMESTAMP_DELTA: timestampDelta = readSVarint64(in); break;
        case SNAP_SCHEMA_ID: schemaId = readVarint(in); break;
        case SNAP_ADDED_ROWS: addedRows = readVarint64(in); break;
        case SNAP_PARENT_ID: parentId = readFixed64(in); break;
        case SNAP_FIRST_ROW_ID: firstRowId = readVarint64(in); break;
        case SNAP_KEY_ID: keyId = readString(in); break;
        default: skipField(in, tag & 0x7);
      }
    }
    return new AddSnapshotUpdate(snapId, manifestSuffix, summary, timestampDelta,
        schemaId, addedRows, parentId, firstRowId, keyId);
  }

  // ============================================================
  // CompactSummary encode/decode
  // ============================================================

  private static final String[] OPERATION_NAMES = {"append", "overwrite", "delete", "replace"};

  private static byte[] encodeCompactSummary(Map<String, String> summary) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    String op = summary.getOrDefault("operation", "append");
    int opVal = 0;
    for (int i = 0; i < OPERATION_NAMES.length; i++) {
      if (OPERATION_NAMES[i].equals(op)) { opVal = i; break; }
    }
    if (opVal != 0) { writeVarint(out, CS_OPERATION, opVal); }

    writeSummaryLong(out, summary, "added-data-files", CS_ADDED_DATA_FILES);
    writeSummaryLong(out, summary, "added-records", CS_ADDED_RECORDS);
    writeSummaryLong(out, summary, "added-files-size", CS_ADDED_FILES_SIZE);
    writeSummaryLong(out, summary, "total-data-files", CS_TOTAL_DATA_FILES);
    writeSummaryLong(out, summary, "total-records", CS_TOTAL_RECORDS);
    writeSummaryLong(out, summary, "total-files-size", CS_TOTAL_FILES_SIZE);
    writeSummaryLong(out, summary, "added-delete-files", CS_ADDED_DELETE_FILES);
    writeSummaryLong(out, summary, "total-delete-files", CS_TOTAL_DELETE_FILES);
    writeSummaryLong(out, summary, "total-equality-deletes", CS_TOTAL_EQUALITY_DELETES);
    writeSummaryLong(out, summary, "total-position-deletes", CS_TOTAL_POSITION_DELETES);

    // Extra entries (non-standard)
    Set<String> standardKeys = Set.of("operation", "added-data-files", "added-records",
        "added-files-size", "total-data-files", "total-records", "total-files-size",
        "added-delete-files", "total-delete-files", "total-equality-deletes",
        "total-position-deletes");
    for (Map.Entry<String, String> e : summary.entrySet()) {
      if (!standardKeys.contains(e.getKey())) {
        out.write(encodeMapEntry(CS_EXTRA, e.getKey(), e.getValue()));
      }
    }
    return out.toByteArray();
  }

  private static Map<String, String> decodeCompactSummary(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    Map<String, String> summary = new HashMap<>();
    summary.put("operation", "append"); // default

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fn = tag >>> 3;
      switch (fn) {
        case CS_OPERATION:
          int op = readVarint(in);
          if (op >= 0 && op < OPERATION_NAMES.length) {
            summary.put("operation", OPERATION_NAMES[op]);
          }
          break;
        case CS_ADDED_DATA_FILES:
          summary.put("added-data-files", String.valueOf(readVarint64(in))); break;
        case CS_ADDED_RECORDS:
          summary.put("added-records", String.valueOf(readVarint64(in))); break;
        case CS_ADDED_FILES_SIZE:
          summary.put("added-files-size", String.valueOf(readVarint64(in))); break;
        case CS_TOTAL_DATA_FILES:
          summary.put("total-data-files", String.valueOf(readVarint64(in))); break;
        case CS_TOTAL_RECORDS:
          summary.put("total-records", String.valueOf(readVarint64(in))); break;
        case CS_TOTAL_FILES_SIZE:
          summary.put("total-files-size", String.valueOf(readVarint64(in))); break;
        case CS_ADDED_DELETE_FILES:
          summary.put("added-delete-files", String.valueOf(readVarint64(in))); break;
        case CS_TOTAL_DELETE_FILES:
          summary.put("total-delete-files", String.valueOf(readVarint64(in))); break;
        case CS_TOTAL_EQUALITY_DELETES:
          summary.put("total-equality-deletes", String.valueOf(readVarint64(in))); break;
        case CS_TOTAL_POSITION_DELETES:
          summary.put("total-position-deletes", String.valueOf(readVarint64(in))); break;
        case CS_EXTRA:
          decodeMapEntry(readLengthDelimitedBytes(in), summary); break;
        default: skipField(in, tag & 0x7);
      }
    }
    return summary;
  }

  private static void writeSummaryLong(
      ByteArrayOutputStream out, Map<String, String> summary, String key, int fieldNum)
      throws IOException {
    String val = summary.get(key);
    if (val != null) {
      long v = Long.parseLong(val);
      if (v != 0) { writeVarint64(out, fieldNum, v); }
    }
  }

  // ============================================================
  // Map entry encode/decode (proto map<string,string>)
  // ============================================================

  private static byte[] encodeMapEntry(int fieldNum, String key, String value) throws IOException {
    ByteArrayOutputStream entry = new ByteArrayOutputStream();
    writeString(entry, MAP_ENTRY_KEY, key);
    writeString(entry, MAP_ENTRY_VALUE, value);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeLengthDelimited(out, fieldNum, entry.toByteArray());
    return out.toByteArray();
  }

  private static void decodeMapEntry(byte[] bytes, Map<String, String> target) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    String key = "", value = "";
    while (in.available() > 0) {
      int tag = readVarint(in);
      int fn = tag >>> 3;
      if (fn == MAP_ENTRY_KEY) { key = readString(in); }
      else if (fn == MAP_ENTRY_VALUE) { value = readString(in); }
      else { skipField(in, tag & 0x7); }
    }
    target.put(key, value);
  }

  // ============================================================
  // Wire format utilities (same conventions as ProtoCodec)
  // ============================================================

  private static void writeVarint(OutputStream out, int fieldNumber, int value) throws IOException {
    writeRawVarint(out, (fieldNumber << 3) | WIRE_VARINT);
    writeRawVarint(out, value);
  }

  private static void writeVarint64(OutputStream out, int fieldNumber, long value)
      throws IOException {
    writeRawVarint(out, (fieldNumber << 3) | WIRE_VARINT);
    writeRawVarint64(out, value);
  }

  private static void writeSVarint64(OutputStream out, int fieldNumber, long value)
      throws IOException {
    // zigzag encoding for sint64
    writeVarint64(out, fieldNumber, (value << 1) ^ (value >> 63));
  }

  private static void writeFixed64(OutputStream out, int fieldNumber, long value)
      throws IOException {
    writeRawVarint(out, (fieldNumber << 3) | WIRE_64BIT);
    for (int i = 0; i < 8; i++) {
      out.write((int) (value & 0xFF));
      value >>>= 8;
    }
  }

  private static void writeString(OutputStream out, int fieldNumber, String value)
      throws IOException {
    if (value == null) {
      value = "";
    }
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    writeLengthDelimited(out, fieldNumber, bytes);
  }

  private static void writeBytes(OutputStream out, int fieldNumber, byte[] value)
      throws IOException {
    writeLengthDelimited(out, fieldNumber, value);
  }

  private static void writeLengthDelimited(OutputStream out, int fieldNumber, byte[] value)
      throws IOException {
    writeRawVarint(out, (fieldNumber << 3) | WIRE_LENGTH_DELIMITED);
    writeRawVarint(out, value.length);
    out.write(value);
  }

  private static void writeRawVarint(OutputStream out, int value) throws IOException {
    while ((value & ~0x7F) != 0) {
      out.write((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    out.write(value);
  }

  private static void writeRawVarint64(OutputStream out, long value) throws IOException {
    while ((value & ~0x7FL) != 0) {
      out.write((int) (value & 0x7F) | 0x80);
      value >>>= 7;
    }
    out.write((int) value);
  }

  private static int readVarint(InputStream in) throws IOException {
    int result = 0, shift = 0, b;
    do {
      b = in.read();
      if (b < 0) { throw new IOException("Unexpected end of stream"); }
      result |= (b & 0x7F) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    return result;
  }

  private static long readVarint64(InputStream in) throws IOException {
    long result = 0;
    int shift = 0, b;
    do {
      b = in.read();
      if (b < 0) { throw new IOException("Unexpected end of stream"); }
      result |= (long) (b & 0x7F) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    return result;
  }

  private static long readSVarint64(InputStream in) throws IOException {
    long raw = readVarint64(in);
    return (raw >>> 1) ^ -(raw & 1); // zigzag decode
  }

  private static long readFixed64(InputStream in) throws IOException {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      int b = in.read();
      if (b < 0) { throw new IOException("Unexpected end of stream"); }
      result |= (long) (b & 0xFF) << (i * 8);
    }
    return result;
  }

  private static String readString(InputStream in) throws IOException {
    return new String(readLengthDelimitedBytes(in), StandardCharsets.UTF_8);
  }

  private static byte[] readLengthDelimitedBytes(InputStream in) throws IOException {
    int length = readVarint(in);
    byte[] bytes = new byte[length];
    int read = 0;
    while (read < length) {
      int n = in.read(bytes, read, length - read);
      if (n < 0) { throw new IOException("Unexpected end of stream"); }
      read += n;
    }
    return bytes;
  }

  private static void skipField(InputStream in, int wireType) throws IOException {
    switch (wireType) {
      case WIRE_VARINT: readVarint64(in); break;
      case WIRE_LENGTH_DELIMITED: in.skip(readVarint(in)); break;
      case WIRE_64BIT: in.skip(8); break;
      case 5: in.skip(4); break;
      default: throw new IOException("Unknown wire type: " + wireType);
    }
  }
}
