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

    public AddSnapshotUpdate(
        long snapshotId, String manifestListSuffix, Map<String, String> summary,
        long timestampDeltaMs, int schemaId, long addedRows) {
      this.snapshotId = snapshotId;
      this.manifestListSuffix = manifestListSuffix;
      this.summary = summary;
      this.timestampDeltaMs = timestampDeltaMs;
      this.schemaId = schemaId;
      this.addedRows = addedRows;
    }

    /**
     * Applies the snapshot to metadata using dictionary-derived fields.
     * Requires the manifest list prefix from the inline table's checkpoint entry.
     */
    public TableMetadata applyTo(TableMetadata base, String manifestListPrefix) {
      long seqNum = base.lastSequenceNumber() + 1;
      long parentSnapshotId = base.currentSnapshot() != null
          ? base.currentSnapshot().snapshotId() : -1;
      long timestamp = base.lastUpdatedMillis() + timestampDeltaMs;
      int resolvedSchemaId = schemaId > 0 ? schemaId : base.currentSchemaId();
      String manifestList = manifestListPrefix + manifestListSuffix;

      // Build snapshot via JSON + SnapshotParser (BaseSnapshot is package-private)
      StringBuilder json = new StringBuilder("{");
      json.append("\"snapshot-id\":").append(snapshotId);
      if (parentSnapshotId >= 0) {
        json.append(",\"parent-snapshot-id\":").append(parentSnapshotId);
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
      writeLengthDelimited(out, UPDATE_ADD_SNAPSHOT, inner.toByteArray());
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
      default:
        // Forward compatibility: skip unknown update types
        return builder -> {};
    }
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
        default: skipField(in, tag & 0x7);
      }
    }
    return new AddSnapshotUpdate(snapId, manifestSuffix, summary, timestampDelta,
        schemaId, addedRows);
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
