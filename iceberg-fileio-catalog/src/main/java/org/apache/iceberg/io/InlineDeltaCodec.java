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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.InlineDeltaSnapshots;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.UnboundPartitionSpec;
import org.apache.iceberg.UnboundSortOrder;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.JsonUtil;

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
  // Writer's last-updated-ms, threaded through the wire so reconstruction is
  // byte-stable across replays. Required (this prototype has no deployed
  // history; a delta missing this field is malformed and decode raises).
  private static final int DELTA_LAST_UPDATED_MS = 2;
  // The base the writer computed the delta on: its last sequence number, and (v3) its next row id.
  // Replay moves the sequence numbers and row ids the delta assigned by how far the table it lands
  // on has moved past them (Landing). Absent in a delta of manifest updates only.
  private static final int DELTA_BASE_LAST_SEQUENCE_NUMBER = 3;
  private static final int DELTA_BASE_NEXT_ROW_ID = 4;

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
  private static final int ADD_MF_POSITION = 3; // optional; varint
  private static final int REMOVE_MF_SNAPSHOT_ID = 1;
  private static final int REMOVE_MF_PATH_SUFFIX = 2;
  private static final int REMOVE_MF_PARENT_INDICES = 3; // packed varints

  // Remove-schema / remove-spec field numbers (Iceberg metadata maintenance:
  // expireSnapshots().cleanExpiredMetadata(true) prunes unreachable schemas
  // and partition specs after snapshot expiration).
  private static final int UPDATE_REMOVE_SCHEMAS = 14;
  private static final int UPDATE_REMOVE_PARTITION_SPECS = 15;
  private static final int REMOVE_SCHEMA_IDS = 1;
  private static final int REMOVE_SPEC_IDS = 1;

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

  // AddSortOrder field numbers. The repeated SortField list (field 2) carries
  // a typed binary grammar — see SortFieldEntry. Schemas and partition specs
  // still embed gzip(JSON) because their Type system / transform vocabulary
  // is too large to mirror; sort order is small and closed enough to encode
  // structurally. See SPEC_TM.md for the format and errata.md §D8 for the
  // residual scope (PartitionSpec, Schema).
  private static final int ADD_ORDER_ID = 1;
  private static final int ADD_ORDER_FIELDS = 2;

  // SortField (inner submessage of AddSortOrder) field numbers. transform_kind
  // is 1-based so a missing kind is distinguishable from a recognized value;
  // ditto direction and null_order. transform_param is present only for the
  // parameterized transforms (BUCKET, TRUNCATE).
  private static final int SF_SOURCE_ID = 1;
  private static final int SF_TRANSFORM_KIND = 2;
  private static final int SF_TRANSFORM_PARAM = 3;
  private static final int SF_DIRECTION = 4;
  private static final int SF_NULL_ORDER = 5;

  // TransformKind wire values. Closed set; the encoder throws on an
  // unrecognized Transform.toString() (e.g., UnknownTransform or a future
  // Iceberg-added kind) so silent data loss on grammar evolution is
  // impossible.
  static final int TK_IDENTITY = 1;
  static final int TK_YEAR = 2;
  static final int TK_MONTH = 3;
  static final int TK_DAY = 4;
  static final int TK_HOUR = 5;
  static final int TK_BUCKET = 6;
  static final int TK_TRUNCATE = 7;
  static final int TK_VOID = 8;

  // Direction / NullOrder wire values (1-based; see comment on SortField field
  // numbers above for the rationale).
  private static final int DIR_ASC = 1;
  private static final int DIR_DESC = 2;
  private static final int NO_NULLS_FIRST = 1;
  private static final int NO_NULLS_LAST = 2;

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
  private static final int SNAP_SCHEMA_ID = 5;    // optional; varint
  private static final int SNAP_ADDED_ROWS = 6;   // optional; varint64
  private static final int SNAP_PARENT_ID = 7;    // optional; fixed64
  private static final int SNAP_FIRST_ROW_ID = 8; // optional; varint64
  private static final int SNAP_KEY_ID = 9;       // optional; string
  private static final int SNAP_FOOTPRINT = 10;   // optional; CommitFootprint

  // CommitFootprint field numbers, and FootprintKind wire values (1-based, so a missing kind is
  // told apart; the decoder treats it as no footprint)
  private static final int FP_KIND = 1;
  private static final int FP_CHECKS = 2;
  static final int FK_BLIND = 1;
  static final int FK_OPAQUE = 2;
  static final int FK_VALIDATED = 3;

  // FootprintCheck, PartitionScope, PartitionTuple and PartitionValue field numbers, and
  // CheckKind wire values (1-based; a kind the decoder does not know makes the footprint opaque)
  private static final int FC_KIND = 1;
  private static final int FC_FILTER = 2;
  private static final int FC_PARTITIONS = 3;
  private static final int PS_TUPLES = 1;
  private static final int PT_SPEC_ID = 1;
  private static final int PT_VALUES = 2;
  private static final int PV_VALUE = 1;
  static final int CK_NO_DATA_ADDED = 1;
  static final int CK_NO_DELETES_ADDED = 2;
  static final int CK_NO_DATA_REMOVED = 3;

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
  private static final int CS_CHANGED_PARTITION_COUNT = 12;
  private static final int CS_DELETED_DATA_FILES = 13;
  private static final int CS_DELETED_RECORDS = 14;
  private static final int CS_EXTRA = 15;
  private static final int CS_REMOVED_FILES_SIZE = 16;
  private static final int CS_MANIFESTS_CREATED = 17;
  private static final int CS_MANIFESTS_REPLACED = 18;
  private static final int CS_MANIFESTS_KEPT = 19;
  private static final int CS_ENTRIES_PROCESSED = 20;
  private static final int CS_REMOVED_DELETE_FILES = 21;
  private static final int CS_ADDED_POSITION_DELETES = 22;
  private static final int CS_REMOVED_POSITION_DELETES = 23;
  private static final int CS_ADDED_EQUALITY_DELETES = 24;
  private static final int CS_REMOVED_EQUALITY_DELETES = 25;
  private static final int CS_ADDED_POSITION_DELETE_FILES = 26;
  private static final int CS_REMOVED_POSITION_DELETE_FILES = 27;
  private static final int CS_ADDED_EQUALITY_DELETE_FILES = 28;
  private static final int CS_REMOVED_EQUALITY_DELETE_FILES = 29;
  private static final int CS_ADDED_DVS = 30;
  private static final int CS_REMOVED_DVS = 31;
  private static final int CS_INHERITS_PARENT_EXTRA = 32;
  private static final int CS_DROPPED_EXTRA = 33;

  // ============================================================
  // Public API
  // ============================================================

  /**
   * Applies a delta (encoded bytes) to base metadata, routing ML updates to the
   * CatalogState.Builder and TM updates (including AddSnapshot) to TableMetadata, and
   * returning the updated bytes encoded with the base's codec: a delta keeps the on-disk codec,
   * which changes only with full metadata. Replay applies an UpdateTableInline delta to the
   * table's fields or metadata instead ({@code ProtoCodec.UpdateTableInlineAction}, {@link
   * NativeDelta}), whose base location is its version's; this is the same apply on bytes alone.
   *
   * <p><b>Ordering invariant:</b> {@code AddSnapshotUpdate} entries for a given
   * snapshot must appear in the delta list <em>before</em> any
   * {@code AddManifestUpdate} / {@code RemoveManifestUpdate} entries that
   * reference that snapshot. A new snapshot's manifest list builds on its
   * parent's (see {@link NewSnapshotManifests}), found through
   * {@code current.snapshot(add.snapshotId).parentId()}, which only works if
   * the snapshot has already been added to {@code current}.
   * {@link #computeDelta} and {@link #attachManifestDelta} always emit updates
   * in this order; custom callers must maintain it.
   */
  public static byte[] applyDeltaWithManifests(
      byte[] baseMetadataBytes, InlineMetadataCodec baseCodec, byte[] deltaBytes,
      CatalogState.Builder catalogBuilder, int tableId) {
    return applyDeltaWithManifests(
        baseMetadataBytes, baseCodec, decodeDelta(deltaBytes), catalogBuilder, tableId, false);
  }

  /**
   * Applies {@code decoded} to the metadata {@code baseMetadataBytes} holds: its base, or with
   * {@code stale}, a version of the table that changed since, on which its late-bound fields are
   * finalized ({@link Landing}). A stale delta must be mergeable there ({@link
   * Landing#unmergeable}).
   */
  static byte[] applyDeltaWithManifests(
      byte[] baseMetadataBytes, InlineMetadataCodec baseCodec, DecodedDelta decoded,
      CatalogState.Builder catalogBuilder, int tableId, boolean stale) {
    return baseCodec.encode(
        applyDelta(
            decodeBase(baseMetadataBytes, baseCodec), decoded, catalogBuilder, tableId, stale));
  }

  /**
   * The metadata {@code bytes} hold, with a deterministic metadata-file location, so the metadata
   * a delta builds on it gains an entry for it in its `previousFiles()`, for an apply without a
   * catalog (replay uses the version's location instead, {@code inline://<table>#<id>.<version>}).
   * The string is opaque to FileIO (an `inline://` URI never resolves); only its stability for the
   * same bytes matters.
   */
  static TableMetadata decodeBase(byte[] bytes, InlineMetadataCodec codec) {
    return codec.decodeFull(bytes, "inline://#" + Integer.toHexString(Arrays.hashCode(bytes)));
  }

  /**
   * Applies {@code decoded} to {@code base}, as {@link #applyDeltaWithManifests} does, and returns
   * the new metadata; the manifest updates go to {@code catalogBuilder}'s lists of {@code
   * tableId}.
   */
  static TableMetadata applyDelta(
      TableMetadata base, DecodedDelta decoded, CatalogState.Builder catalogBuilder, int tableId,
      boolean stale) {
    List<DeltaUpdate> updates = decoded.updates;
    Landing landing = Landing.of(decoded, MetadataView.of(base), stale);
    // Pin the writer's last-updated-ms on every Builder pass before applyTo,
    // so `setRef(MAIN_BRANCH, ...)` (TableMetadata.java line 1324) reads the
    // writer's value rather than wall-clock when stamping new snapshot-log
    // entries. See TableMetadata.Builder#setLastUpdatedMillis for the upstream
    // rationale. A snapshot's timestamp is relative to it, the commit's.
    long writerLastUpdatedMillis = decoded.lastUpdatedMillis;
    long lastUpdatedMillis = landing.lastUpdatedMillis(writerLastUpdatedMillis);
    String prefix = catalogBuilder.manifestListPrefix(tableId);
    if (prefix == null) {
      prefix = "";
    }

    // Apply updates type-by-type: TM updates to TableMetadata, ML updates to catalog builder.
    // Ordering: AddSnapshot must precede AddManifest/RemoveManifest for the same snapshotId
    // (see javadoc). Track added snapshot ids to assert invariant.
    java.util.Set<Long> addedInThisDelta = new java.util.HashSet<>();
    NewSnapshotManifests newSnapshots = new NewSnapshotManifests(catalogBuilder, tableId, decoded);
    TableMetadata current = base;
    // Consecutive TableMetadata updates (snapshots, refs, schema, spec, sort order, properties,
    // ...) apply to one builder, built once, as the writer's were. build() checks the default spec
    // and sort order against the current schema, which an intermediate state, a new schema without
    // the column the old spec partitions by, would fail. And setRef logs main's move to a snapshot
    // at the snapshot's own timestamp only if the same builder added it (else at last-updated-ms),
    // as a writer's commit adds its snapshot and moves the branch in one builder.
    TableMetadata.Builder pending = null;
    // The last sequence number of the metadata pending builds, which numbers the next snapshot.
    long lastSequenceNumber = 0L;
    for (DeltaUpdate update : updates) {
      boolean tableMetadata =
          !(update instanceof AddManifestUpdate) && !(update instanceof RemoveManifestUpdate);
      if (tableMetadata) {
        if (pending == null) {
          pending = TableMetadata.buildFrom(current);
          pending.setLastUpdatedMillis(lastUpdatedMillis);
          lastSequenceNumber = current.lastSequenceNumber();
        }
        if (update instanceof AddSnapshotUpdate) {
          AddSnapshotUpdate addSnap = (AddSnapshotUpdate) update;
          // v1 tables don't carry sequence numbers; TableMetadata's constructor rejects any
          // non-zero value.
          long sequenceNumber = current.formatVersion() >= 2 ? ++lastSequenceNumber : 0L;
          pending.addSnapshot(
              addSnap.snapshot(
                  sequenceNumber, current.currentSchemaId(), prefix, writerLastUpdatedMillis,
                  landing));
          addedInThisDelta.add(addSnap.snapshotId);
          // Inline-ML snapshots (empty suffix → `inline://<snapId>` sentinel) must
          // have a pool entry, otherwise wrapInlineManifests treats the sentinel as
          // catalog state corruption. Register it now, even if no AddManifest or
          // RemoveManifest update follows.
          if (addSnap.manifestListSuffix.isEmpty()) {
            newSnapshots.add(addSnap.snapshotId, landing.parent(addSnap));
          }
        } else if (update instanceof RemoveSnapshotsUpdate) {
          // Cascade snapshot removal to the catalog pool: drop the snapshot's ref
          // list and GC any pool entries no longer referenced. Without this, the
          // pool grows unboundedly as snapshots are expired (SPEC_ML.md).
          catalogBuilder.removeSnapshotManifests(
              tableId, ((RemoveSnapshotsUpdate) update).snapshotIds);
          // Builder.removeSnapshots also cleans up the TM side's dangling refs.
          update.applyTo(pending);
        } else if (update instanceof SetSnapshotRefUpdate) {
          landing.ref((SetSnapshotRefUpdate) update).applyTo(pending);
        } else {
          update.applyTo(pending);
        }
        continue;
      }
      if (pending != null) {
        current = pending.discardChanges().build();
        pending = null;
      }
      if (update instanceof AddManifestUpdate) {
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
        ManifestFile resolved = landing.manifest(resolve(add.manifest, prefix));
        catalogBuilder.addManifestToPool(tableId, resolved);
        newSnapshots.addManifest(
            MetadataView.of(current), add.snapshotId, resolved.path(), add.position);
      } else if (update instanceof RemoveManifestUpdate) {
        newSnapshots.removeManifest(MetadataView.of(current), (RemoveManifestUpdate) update);
      }
    }
    if (pending != null) {
      current = pending.discardChanges().build();
    }
    newSnapshots.finish();
    return current;
  }

  /** {@code manifest} as the wire holds it, its path suffix resolved with {@code prefix}. */
  static ManifestFile resolve(ManifestFile manifest, String prefix) {
    String fullPath = manifest.path();
    if (!prefix.isEmpty() && !fullPath.startsWith(prefix)) {
      fullPath = prefix + fullPath;
    }
    return manifestFile(
        manifest, fullPath, manifest.sequenceNumber(), manifest.minSequenceNumber(),
        manifest.firstRowId());
  }

  /** A copy of {@code manifest} at {@code path}, with these sequence numbers and first row id. */
  static ManifestFile manifestFile(
      ManifestFile manifest, String path, long sequenceNumber, long minSequenceNumber,
      Long firstRowId) {
    return new ProtoCodec.DecodedManifestFile(
        path, manifest.length(), manifest.partitionSpecId(),
        manifest.content().id(),
        sequenceNumber, minSequenceNumber,
        manifest.snapshotId(),
        manifest.addedFilesCount() != null ? manifest.addedFilesCount() : 0,
        manifest.existingFilesCount() != null ? manifest.existingFilesCount() : 0,
        manifest.deletedFilesCount() != null ? manifest.deletedFilesCount() : 0,
        manifest.addedRowsCount() != null ? manifest.addedRowsCount() : 0,
        manifest.existingRowsCount() != null ? manifest.existingRowsCount() : 0,
        manifest.deletedRowsCount() != null ? manifest.deletedRowsCount() : 0,
        manifest.partitions(),
        manifest.keyMetadata() != null
            ? java.nio.ByteBuffer.allocate(manifest.keyMetadata().remaining())
                .put(manifest.keyMetadata().duplicate()).array()
            : null,
        firstRowId);
  }

  /**
   * Builds the manifest lists of the inline-ML snapshots one delta adds. SnapshotProducer stages
   * each list as a delta against the parent's: the AddManifest and RemoveManifest updates for the
   * snapshot. So a snapshot that changes no manifests (a delete that matched nothing) has no
   * updates and keeps its parent's list whole. Every AddSnapshot in a delta precedes every
   * manifest update, so when one delta adds a snapshot and its child (two appends to a table in
   * one catalog transaction), the parent's list is final only once the delta is through: {@link
   * #finish} settles them in order. Until then each snapshot's ref list holds what is known so far,
   * which keeps its parent's manifests in the pool if the same delta removes the parent snapshot.
   */
  static final class NewSnapshotManifests {
    private final CatalogState.Builder builder;
    private final int tableId;
    // Each new snapshot's parent (null for none), in the order added.
    private final Map<Long, Long> parents = new LinkedHashMap<>();
    // The parent's list when the snapshot was added: final unless the delta also adds the parent.
    private final Map<Long, List<String>> inherited = new HashMap<>();
    // Each new snapshot's added manifests, with their positions in its list (null: at the end).
    private final Map<Long, List<String>> added = new HashMap<>();
    private final Map<Long, List<Integer>> positions = new HashMap<>();
    private final Map<Long, List<String>> removedSuffixes = new HashMap<>();
    // The list each new snapshot's writer built on, its parent's, by snapshot: what the indices of
    // its RemoveManifestUpdate name. Taken before the delta changes any list (it may remove the
    // parent), from the lists before it; a snapshot's list never changes.
    private final Map<Long, List<String>> writerLists = new HashMap<>();

    NewSnapshotManifests(CatalogState.Builder builder, int tableId) {
      this(builder, tableId, null);
    }

    /** For {@code delta}'s new snapshots, before anything of {@code delta} is applied. */
    NewSnapshotManifests(CatalogState.Builder builder, int tableId, DecodedDelta delta) {
      this.builder = builder;
      this.tableId = tableId;
      if (delta != null) {
        for (DeltaUpdate update : delta.updates) {
          if (update instanceof AddSnapshotUpdate) {
            AddSnapshotUpdate add = (AddSnapshotUpdate) update;
            writerLists.put(add.snapshotId, list(add.parentSnapshotId));
          }
        }
      }
    }

    /** The inline list of {@code snapshotId} (null: none), or null if the table has none. */
    private List<String> list(Long snapshotId) {
      return snapshotId != null && builder.hasSnapshotManifests(tableId, snapshotId)
          ? builder.snapshotManifestPaths(tableId, snapshotId)
          : null;
    }

    /** Registers {@code snapshotId}, just added to {@code current}, unless it has a list. */
    void add(MetadataView current, long snapshotId) {
      Snapshot snap = current.snapshot(snapshotId);
      add(snapshotId, snap != null ? snap.parentId() : null);
    }

    /** Registers {@code snapshotId}, a child of {@code parentId} (null: none), unless it has a list. */
    void add(long snapshotId, Long parentId) {
      if (builder.hasSnapshotManifests(tableId, snapshotId)) {
        return;
      }
      List<String> base =
          parentId != null ? builder.snapshotManifestPaths(tableId, parentId) : List.of();
      parents.put(snapshotId, parentId);
      inherited.put(snapshotId, base);
      builder.setSnapshotManifests(tableId, snapshotId, base);
    }

    void addManifest(MetadataView current, long snapshotId, String path, Integer position) {
      if (parents.containsKey(snapshotId)) {
        added.computeIfAbsent(snapshotId, k -> new ArrayList<>()).add(path);
        positions.computeIfAbsent(snapshotId, k -> new ArrayList<>()).add(position);
      }
      List<String> refs = refs(current, snapshotId);
      insert(refs, path, position);
      builder.setSnapshotManifests(tableId, snapshotId, refs);
    }

    /** Inserts {@code path} at {@code position}, or at the end if it is null or past the end. */
    private static void insert(List<String> refs, String path, Integer position) {
      if (position != null && position <= refs.size()) {
        refs.add(position, path);
      } else {
        refs.add(path);
      }
    }

    void removeManifest(MetadataView current, RemoveManifestUpdate remove) {
      long snapshotId = remove.snapshotId;
      List<String> writerList;
      if (writerLists.containsKey(snapshotId)) {
        writerList = writerLists.get(snapshotId);
      } else {
        Snapshot snap = current.snapshot(snapshotId);
        writerList = list(snap != null ? snap.parentId() : null);
      }
      List<String> removed = remove.removed(writerList);
      if (removed == null) {
        throw new IllegalStateException(
            "RemoveManifestUpdate for snapshot " + snapshotId
                + " names an index its parent's list lacks");
      }
      for (String suffix : removed) {
        removeManifest(current, snapshotId, suffix);
      }
    }

    private void removeManifest(MetadataView current, long snapshotId, String suffix) {
      if (parents.containsKey(snapshotId)) {
        removedSuffixes.computeIfAbsent(snapshotId, k -> new ArrayList<>()).add(suffix);
      }
      List<String> refs = refs(current, snapshotId);
      refs.removeIf(p -> p.endsWith(suffix));
      builder.setSnapshotManifests(tableId, snapshotId, refs);
    }

    /** The snapshot's list so far; one this delta did not add starts as its parent's. */
    private List<String> refs(MetadataView current, long snapshotId) {
      if (!builder.hasSnapshotManifests(tableId, snapshotId)) {
        Snapshot snap = current.snapshot(snapshotId);
        Long parentId = snap != null ? snap.parentId() : null;
        return new ArrayList<>(
            parentId != null ? builder.snapshotManifestPaths(tableId, parentId) : List.of());
      }
      return new ArrayList<>(builder.snapshotManifestPaths(tableId, snapshotId));
    }

    /**
     * Settles each new snapshot's list: its parent's final list, less the manifests it removes,
     * with the ones it adds at their positions, in order (the positions are indices in the final
     * list, so each lands after every manifest that precedes it).
     */
    void finish() {
      Map<Long, List<String>> lists = new HashMap<>();
      parents.forEach(
          (snapshotId, parentId) -> {
            List<String> refs =
                new ArrayList<>(
                    parentId != null && lists.containsKey(parentId)
                        ? lists.get(parentId)
                        : inherited.get(snapshotId));
            for (String suffix : removedSuffixes.getOrDefault(snapshotId, List.of())) {
              refs.removeIf(p -> p.endsWith(suffix));
            }
            List<String> paths = added.getOrDefault(snapshotId, List.of());
            List<Integer> at = positions.getOrDefault(snapshotId, List.of());
            List<Integer> order = new ArrayList<>();
            for (int i = 0; i < paths.size(); i++) {
              order.add(i);
            }
            order.sort(java.util.Comparator.comparing(
                i -> at.get(i) == null ? Integer.MAX_VALUE : at.get(i)));
            for (int i : order) {
              insert(refs, paths.get(i), at.get(i));
            }
            lists.put(snapshotId, refs);
            if (builder.hasSnapshotManifests(tableId, snapshotId)) { // not removed by the delta
              builder.setSnapshotManifests(tableId, snapshotId, refs);
            }
          });
    }
  }

  /**
   * Applies only the ML-pool side of a delta payload to {@code catalogBuilder}'s
   * per-table state, leaving the inline TM bytes untouched. Used by a FULL-mode
   * UpdateTableInline and by CreateTableInline to land the new snapshots'
   * manifest lists atomically with the TM: whenever the commit writes full
   * metadata (a create, a pointer table coming back inline, a delta too big
   * for the update limit, or a change delta encoding cannot express).
   *
   * <p>{@code newMetadataBytes} are decoded once to find each new snapshot's
   * parent, whose manifest list it builds on (see {@link NewSnapshotManifests}).
   * AddSnapshotUpdate / SetSnapshotRefUpdate / schema / spec / sort-order /
   * properties / location / RemoveSchemas / RemovePartitionSpecs updates in the
   * payload are ignored — the TM replacement already carries those. We honour
   * AddManifestUpdate, RemoveManifestUpdate, and RemoveSnapshotsUpdate (cascading
   * pool removal for snapshots dropped from the new TM).
   */
  public static void applyManifestsOnly(
      byte[] deltaBytes, CatalogState.Builder catalogBuilder, int tableId,
      InlineMetadataCodec newMetadataCodec, byte[] newMetadataBytes) {
    String prefix = catalogBuilder.manifestListPrefix(tableId);
    if (prefix == null) {
      prefix = "";
    }
    DecodedDelta decoded = decodeDelta(deltaBytes);
    TableMetadata current =
        newMetadataCodec.decodeFull(newMetadataBytes, "inline://manifests-only");
    NewSnapshotManifests newSnapshots = new NewSnapshotManifests(catalogBuilder, tableId);
    // Register the new TM's inline snapshots that have no list yet (the ones this update adds),
    // parents first, so the invariant sentinel => pool entry holds even for a snapshot that changes
    // no manifests and so has no updates here.
    Set<Long> listed = catalogBuilder.snapshotManifestKeys(tableId);
    for (Snapshot snap : current.snapshots()) {
      String location = snap.manifestListLocation();
      if (location != null && location.startsWith("inline://")
          && !listed.contains(snap.snapshotId())) {
        newSnapshots.add(MetadataView.of(current), snap.snapshotId());
      }
    }
    for (DeltaUpdate update : decoded.updates) {
      if (update instanceof AddSnapshotUpdate) {
        AddSnapshotUpdate addSnap = (AddSnapshotUpdate) update;
        // Register sentinel snapshots so applyDeltaWithManifests's invariant
        // (sentinel ⇒ pool entry) holds even without manifest updates.
        if (addSnap.manifestListSuffix.isEmpty()) {
          newSnapshots.add(MetadataView.of(current), addSnap.snapshotId);
        }
      } else if (update instanceof AddManifestUpdate) {
        AddManifestUpdate add = (AddManifestUpdate) update;
        ManifestFile resolved = resolve(add.manifest, prefix);
        catalogBuilder.addManifestToPool(tableId, resolved);
        newSnapshots.addManifest(
            MetadataView.of(current), add.snapshotId, resolved.path(), add.position);
      } else if (update instanceof RemoveManifestUpdate) {
        newSnapshots.removeManifest(MetadataView.of(current), (RemoveManifestUpdate) update);
      } else if (update instanceof RemoveSnapshotsUpdate) {
        catalogBuilder.removeSnapshotManifests(
            tableId, ((RemoveSnapshotsUpdate) update).snapshotIds);
      }
      // All other update types are TM concerns and are already reflected in
      // newMetadataBytes — skip.
    }
    newSnapshots.finish();
  }

  /**
   * Applies a list of decoded updates to a base metadata, pinning the
   * writer's {@code last-updated-ms} on the builder so {@code setRef(MAIN_BRANCH, ...)}
   * stamps snapshot-log entries with the writer's value instead of the wall-clock
   * fallback at {@code TableMetadata.Builder.setRef} line 1324. Result is byte-
   * stable: a logically-identical replay produces identical {@link TableMetadata}
   * values, which serialize to identical JSON.
   */
  public static TableMetadata applyUpdates(
      TableMetadata base, List<DeltaUpdate> updates, long writerLastUpdatedMillis) {
    TableMetadata.Builder builder = TableMetadata.buildFrom(base);
    builder.setLastUpdatedMillis(writerLastUpdatedMillis);
    for (DeltaUpdate update : updates) {
      update.applyTo(builder);
    }
    return builder.discardChanges().build();
  }

  /**
   * Convenience overload that pins the builder's {@code last-updated-ms} to
   * {@code base.lastUpdatedMillis()} — useful for tests that don't care about
   * a specific writer timestamp but still want determinism.
   */
  public static TableMetadata applyUpdates(TableMetadata base, List<DeltaUpdate> updates) {
    return applyUpdates(base, updates, base.lastUpdatedMillis());
  }

  /**
   * Decoded delta record. Carries the structured {@link DeltaUpdate} list plus
   * the writer's {@code last-updated-ms} (encoded as wire field 2). On replay,
   * the timestamp is fed into {@code TableMetadata.Builder.setLastUpdatedMillis}
   * before applying updates so {@code setRef}-style stamping of new snapshot-log
   * entries reproduces the writer's commit verbatim.
   */
  public static final class DecodedDelta {
    public final List<DeltaUpdate> updates;
    public final long lastUpdatedMillis;
    /** The last sequence number of the delta's base; null in a delta of manifest updates only. */
    public final Long baseLastSequenceNumber;
    /** The next row id of the delta's base (v3); null if it has none or the delta does not say. */
    public final Long baseNextRowId;

    DecodedDelta(
        List<DeltaUpdate> updates, long lastUpdatedMillis, Long baseLastSequenceNumber,
        Long baseNextRowId) {
      this.updates = updates;
      this.lastUpdatedMillis = lastUpdatedMillis;
      this.baseLastSequenceNumber = baseLastSequenceNumber;
      this.baseNextRowId = baseNextRowId;
    }
  }

  /**
   * Encodes a list of delta updates plus the writer's {@code last-updated-ms}
   * to wire bytes. The timestamp is written as wire field 2 (varint64) and is
   * required: it threads the writer's actual value through the intention
   * record so every replay reproduces it exactly (top-level
   * {@code last-updated-ms} field plus any new {@code snapshot-log} entries
   * stamped by {@code setRef}).
   *
   * <p>Production callers (e.g. {@code FileIOTableOperations.stageUpdate}) pass the
   * just-built {@code metadata.lastUpdatedMillis()}.
   */
  public static byte[] encodeDelta(List<DeltaUpdate> updates, long lastUpdatedMillis) {
    return encodeDelta(updates, lastUpdatedMillis, null);
  }

  /**
   * Encodes {@code updates}, the delta of a commit from {@code base} with {@code
   * lastUpdatedMillis}, with the base's last sequence number and (v3) next row id, which replay
   * moves the sequence numbers and row ids the commit assigned from ({@link Landing}). A null base
   * writes neither: a delta of manifest updates only, which lands with full metadata.
   */
  public static byte[] encodeDelta(
      List<DeltaUpdate> updates, long lastUpdatedMillis, TableMetadata base) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      writeVarint64(out, DELTA_LAST_UPDATED_MS, lastUpdatedMillis);
      if (base != null) {
        writeVarint64(out, DELTA_BASE_LAST_SEQUENCE_NUMBER, base.lastSequenceNumber());
        if (base.formatVersion() >= 3) {
          writeVarint64(out, DELTA_BASE_NEXT_ROW_ID, base.nextRowId());
        }
      }
      Long only = onlyNewSnapshot(updates);
      for (DeltaUpdate update : updates) {
        byte[] updateBytes = encodeUpdate(update, base, only);
        writeLengthDelimited(out, DELTA_UPDATES, updateBytes);
      }
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Decodes delta bytes, returning the update list and the writer's
   * {@code last-updated-ms}. Field 2 is required; absence raises.
   */
  public static DecodedDelta decodeDelta(byte[] bytes) {
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(bytes);
      List<DeltaUpdate> updates = new ArrayList<>();
      Long lastUpdatedMillis = null;
      Long baseLastSequenceNumber = null;
      Long baseNextRowId = null;
      List<byte[]> raw = new ArrayList<>();
      while (in.available() > 0) {
        int tag = readVarint(in);
        int fieldNumber = tag >>> 3;
        int wireType = tag & 0x7;
        if (fieldNumber == DELTA_UPDATES) {
          raw.add(readLengthDelimitedBytes(in));
        } else if (fieldNumber == DELTA_LAST_UPDATED_MS) {
          lastUpdatedMillis = readVarint64(in);
        } else if (fieldNumber == DELTA_BASE_LAST_SEQUENCE_NUMBER) {
          baseLastSequenceNumber = readVarint64(in);
        } else if (fieldNumber == DELTA_BASE_NEXT_ROW_ID) {
          baseNextRowId = readVarint64(in);
        } else {
          skipField(in, wireType);
        }
      }
      if (lastUpdatedMillis == null) {
        throw new IllegalStateException(
            "Malformed TableMetadataDelta: required field 2 (last_updated_ms) missing");
      }
      // The snapshots first: an id the other updates leave out is the delta's only new snapshot's
      DeltaUpdate[] decoded = new DeltaUpdate[raw.size()];
      Long only = null;
      int added = 0;
      for (int i = 0; i < raw.size(); i++) {
        if (updateType(raw.get(i)) == UPDATE_ADD_SNAPSHOT) {
          decoded[i] = decodeUpdate(raw.get(i), null);
          only = ((AddSnapshotUpdate) decoded[i]).snapshotId;
          added++;
        }
      }
      if (added != 1) {
        only = null;
      }
      for (int i = 0; i < raw.size(); i++) {
        updates.add(decoded[i] != null ? decoded[i] : decodeUpdate(raw.get(i), only));
      }
      return new DecodedDelta(updates, lastUpdatedMillis, baseLastSequenceNumber, baseNextRowId);
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
   * @return list of delta updates, or null if a delta cannot express the change or has no update
   *     (the commit then sends the full metadata)
   */
  public static List<DeltaUpdate> computeDelta(
      TableMetadata oldMeta, TableMetadata newMeta, String manifestListPrefix) {
    // Concurrent-replace collision: if a schema, partition spec, or sort order
    // appears in both old and new metadata under the same id but with different
    // content, delta encoding can't express the in-place swap (Builder rejects
    // re-adding an existing id, and there's no public RemoveSortOrder /
    // ReplaceSortOrder). Force full mode -- the new TM JSON carries the right
    // shape and the replay never has to dispatch on ids.
    // testConcurrentReplaceTransactionSortOrderConflict triggers this: both
    // racing replace transactions independently assign sortOrderId=1 to
    // different SortOrder fields.
    if (idCollidesWithDifferentContent(oldMeta, newMeta)) {
      return null;
    }
    // A format-version upgrade has no DeltaUpdate type: full mode carries it. Without this, an
    // upgrade with any other change went as a delta of the other change alone.
    if (newMeta.formatVersion() != oldMeta.formatVersion()) {
      return null;
    }
    // Nor has a removed ref (a replace transaction's main branch, removeBranch, removeTag).
    if (!newMeta.refs().keySet().containsAll(oldMeta.refs().keySet())) {
      return null;
    }
    // Nor has a change to a v3 table's encryption keys.
    if (!encryptionKeyIds(newMeta).equals(encryptionKeyIds(oldMeta))) {
      return null;
    }
    // Replay numbers the commit's new snapshots after the base's last sequence number, in order,
    // and advances next-row-id by their added rows. A transaction that also expired a snapshot it
    // added left a gap, which only full mode carries.
    if (!sequenceAndRowIdsFollow(oldMeta, newMeta)) {
      return null;
    }
    // Replay adds the commit's new schemas and specs to the base's before it removes any, and
    // Iceberg numbers each one it adds afresh: the id of an equal one it holds, else one past the
    // highest. A transaction that removed unused ones (an expiry's metadata cleanup) before it
    // added one gave it another id, which only full mode carries.
    if (!replayKeepsTheWritersIds(oldMeta, newMeta)) {
      return null;
    }
    // Replay takes the base's last column and partition ids up to the highest in the schemas and
    // specs the commit added. A transaction that added one and removed it again with unused ones
    // (an expiry's metadata cleanup) left a last id above every schema's or spec's: only full mode
    // carries that.
    if (!replayKeepsTheLastIds(oldMeta, newMeta)) {
      return null;
    }
    // Replay adds the commit's new sort orders and specs with the builder, which binds each to the
    // schema current by then, the commit's. Iceberg binds orders other than the default unchecked:
    // a transaction that sorted by a column, sorted by another and dropped the first left an
    // order that does not bind, which only full mode carries (Bug 65).
    if (!replayBindsWhatItAdds(oldMeta, newMeta)) {
      return null;
    }
    // Replay logs one new snapshot-log entry if main moved: main's new snapshot, at the snapshot's
    // own timestamp if the commit added it, else (a rollback) at the commit's last-updated-ms. A
    // transaction that rolled main back and then changed something else (a branch) logged main at
    // the earlier time, and one that moved main and back (append, roll back) logged main again:
    // only full mode carries those.
    if (!replayLogsAsTheWriterDid(oldMeta, newMeta)) {
      return null;
    }
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
        // Relative to the commit's last-updated-ms, which the delta carries: replay finds it the
        // same on whatever version of the table the delta lands.
        long timestampDelta = snap.timestampMillis() - newMeta.lastUpdatedMillis();
        // Absent when it is the base's current schema, which replay falls back to.
        Integer schemaId = snap.schemaId() != null
            && snap.schemaId() != oldMeta.currentSchemaId()
            ? snap.schemaId() : null;
        // Always carry parent snapshot ID explicitly. This is required for
        // stage-only, branch, cherry-pick; reconstructing from base.currentSnapshot
        // on replay is wrong in those cases.
        Long parentId = snap.parentId();
        Long firstRowId = snap.firstRowId();
        String keyId = snap.keyId();
        // Ensure the operation name is carried in the summary. snap.operation()
        // is the authoritative source; snap.summary() may or may not include
        // the "operation" key depending on the producer, and encoding relies on
        // the summary map's entry.
        Map<String, String> summary = snap.summary() != null
            ? new HashMap<>(snap.summary()) : new HashMap<>();
        if (snap.operation() != null && !summary.containsKey("operation")) {
          summary.put("operation", snap.operation());
        }
        // The producer's footprint, which only an inline snapshot carries.
        Footprint footprint =
            snap instanceof org.apache.iceberg.InlineSnapshot
                ? Footprint.of(
                    ((org.apache.iceberg.InlineSnapshot) snap).footprint(), newMeta.specsById())
                : null;
        updates.add(new AddSnapshotUpdate(
            snap.snapshotId(), suffix, summary,
            timestampDelta, schemaId, snap.addedRows(),
            parentId, firstRowId, keyId, footprint));
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

    // Schema changes. Order matters: Add -> SetCurrent -> Remove. RemoveSchemas
    // (and the Builder.removeSchemas it routes to) rejects removing the current
    // schema id; if a replace transaction makes a new schema current and drops
    // the old one, we have to advance currentSchemaId before pruning.
    Set<Integer> oldSchemaIds = new HashSet<>();
    for (org.apache.iceberg.Schema s : oldMeta.schemas()) {
      oldSchemaIds.add(s.schemaId());
    }
    Set<Integer> newSchemaIds = new HashSet<>();
    for (org.apache.iceberg.Schema schema : newMeta.schemas()) {
      newSchemaIds.add(schema.schemaId());
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
    // Removed schemas (cleanExpiredMetadata=true after expireSnapshots prunes
    // schemas that no retained snapshot references; replace transactions also
    // drop the prior current schema). Without an explicit RemoveSchemasUpdate
    // the replay would re-introduce them via the carried base.schemas() list.
    List<Integer> removedSchemas = new ArrayList<>();
    for (int id : oldSchemaIds) {
      if (!newSchemaIds.contains(id)) {
        removedSchemas.add(id);
      }
    }
    if (!removedSchemas.isEmpty()) {
      updates.add(new RemoveSchemasUpdate(removedSchemas));
    }

    // Partition spec changes (same ordering rationale as schema changes above).
    Set<Integer> oldSpecIds = new HashSet<>();
    for (org.apache.iceberg.PartitionSpec s : oldMeta.specs()) {
      oldSpecIds.add(s.specId());
    }
    Set<Integer> newSpecIds = new HashSet<>();
    for (org.apache.iceberg.PartitionSpec spec : newMeta.specs()) {
      newSpecIds.add(spec.specId());
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
    // Removed specs (companion to removed schemas above).
    List<Integer> removedSpecs = new ArrayList<>();
    for (int id : oldSpecIds) {
      if (!newSpecIds.contains(id)) {
        removedSpecs.add(id);
      }
    }
    if (!removedSpecs.isEmpty()) {
      updates.add(new RemovePartitionSpecsUpdate(removedSpecs));
    }

    // Sort order changes
    Set<Integer> oldOrderIds = new HashSet<>();
    for (org.apache.iceberg.SortOrder o : oldMeta.sortOrders()) {
      oldOrderIds.add(o.orderId());
    }
    for (org.apache.iceberg.SortOrder order : newMeta.sortOrders()) {
      if (!oldOrderIds.contains(order.orderId())) {
        updates.add(AddSortOrderUpdate.fromSortOrder(order));
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

    // Statistics-file changes have no DeltaUpdate type yet. If they differ,
    // fall back to "full" mode by returning null; full-mode serializes the
    // entire TableMetadata (including stats), so the change survives.
    // Mixed commits (stats + a delta-representable change) lose the size
    // benefit of delta mode but never lose the stats themselves.
    if (statisticsChanged(oldMeta, newMeta)) {
      return null;
    }

    return updates.isEmpty() ? null : updates;
  }

  /**
   * Whether {@code newMeta}'s snapshot log has the entries replay adds to {@code oldMeta}'s: none
   * if main did not move, else main's new snapshot, last. Replay adds the commit's snapshots and
   * moves main in one builder, which logs a snapshot it added at the snapshot's timestamp and one
   * it did not (a rollback) at the commit's last-updated-ms.
   */
  private static boolean replayLogsAsTheWriterDid(TableMetadata oldMeta, TableMetadata newMeta) {
    Set<org.apache.iceberg.HistoryEntry> old = new HashSet<>(oldMeta.snapshotLog());
    List<org.apache.iceberg.HistoryEntry> log = newMeta.snapshotLog();
    List<org.apache.iceberg.HistoryEntry> added = new ArrayList<>();
    for (org.apache.iceberg.HistoryEntry entry : log) {
      if (!old.contains(entry)) {
        added.add(entry);
      }
    }
    org.apache.iceberg.Snapshot main = newMeta.currentSnapshot();
    org.apache.iceberg.Snapshot was = oldMeta.currentSnapshot();
    if (main == null || (was != null && was.snapshotId() == main.snapshotId())) {
      return added.isEmpty();
    }
    long loggedAt =
        oldMeta.snapshot(main.snapshotId()) == null
            ? main.timestampMillis()
            : newMeta.lastUpdatedMillis();
    return added.size() == 1
        && log.get(log.size() - 1) == added.get(0)
        && added.get(0).snapshotId() == main.snapshotId()
        && added.get(0).timestampMillis() == loggedAt;
  }

  /**
   * Whether each sort order and partition spec {@code newMeta} has and {@code oldMeta} lacks binds
   * to {@code newMeta}'s current schema, as replay binds it when it adds it.
   */
  private static boolean replayBindsWhatItAdds(TableMetadata oldMeta, TableMetadata newMeta) {
    Set<Integer> oldOrderIds = new HashSet<>();
    for (SortOrder order : oldMeta.sortOrders()) {
      oldOrderIds.add(order.orderId());
    }
    Set<Integer> oldSpecIds = new HashSet<>();
    for (org.apache.iceberg.PartitionSpec spec : oldMeta.specs()) {
      oldSpecIds.add(spec.specId());
    }
    try {
      for (SortOrder order : newMeta.sortOrders()) {
        if (!oldOrderIds.contains(order.orderId())) {
          order.toUnbound().bind(newMeta.schema());
        }
      }
      for (org.apache.iceberg.PartitionSpec spec : newMeta.specs()) {
        if (!oldSpecIds.contains(spec.specId())) {
          spec.toUnbound().bind(newMeta.schema());
        }
      }
    } catch (org.apache.iceberg.exceptions.ValidationException | IllegalArgumentException e) {
      return false;
    }
    return true;
  }

  /**
   * Whether replay derives {@code newMeta}'s last column and partition ids: the base's, raised to
   * the highest field id in the schemas and specs the commit adds, as TableMetadata.Builder does
   * when it adds one.
   */
  private static boolean replayKeepsTheLastIds(TableMetadata oldMeta, TableMetadata newMeta) {
    Set<Integer> oldSchemaIds = new HashSet<>();
    for (org.apache.iceberg.Schema s : oldMeta.schemas()) {
      oldSchemaIds.add(s.schemaId());
    }
    int lastColumnId = oldMeta.lastColumnId();
    for (org.apache.iceberg.Schema s : newMeta.schemas()) {
      if (!oldSchemaIds.contains(s.schemaId())) {
        lastColumnId = Math.max(lastColumnId, s.highestFieldId());
      }
    }
    Set<Integer> oldSpecIds = new HashSet<>();
    for (org.apache.iceberg.PartitionSpec s : oldMeta.specs()) {
      oldSpecIds.add(s.specId());
    }
    int lastPartitionId = oldMeta.lastAssignedPartitionId();
    for (org.apache.iceberg.PartitionSpec s : newMeta.specs()) {
      if (!oldSpecIds.contains(s.specId())) {
        for (org.apache.iceberg.PartitionField field : s.fields()) {
          lastPartitionId = Math.max(lastPartitionId, field.fieldId());
        }
      }
    }
    return lastColumnId == newMeta.lastColumnId()
        && lastPartitionId == newMeta.lastAssignedPartitionId();
  }

  /**
   * Whether replay, adding the new schemas and specs in order to the base's, numbers each as the
   * writer did. Mirrors TableMetadata.Builder: a schema takes the id of the first equal schema,
   * else one past the highest id it has passed, starting from the current schema's; a spec takes
   * the id of the first compatible spec, else one past the highest.
   */
  private static boolean replayKeepsTheWritersIds(TableMetadata oldMeta, TableMetadata newMeta) {
    Set<Integer> oldSchemaIds = new HashSet<>();
    List<org.apache.iceberg.Schema> schemas = new ArrayList<>(oldMeta.schemas());
    for (org.apache.iceberg.Schema s : schemas) {
      oldSchemaIds.add(s.schemaId());
    }
    for (org.apache.iceberg.Schema added : newMeta.schemas()) {
      if (oldSchemaIds.contains(added.schemaId())) {
        continue;
      }
      int id = oldMeta.currentSchemaId();
      for (org.apache.iceberg.Schema s : schemas) {
        if (s.sameSchema(added)) {
          id = s.schemaId();
          break;
        } else if (s.schemaId() >= id) {
          id = s.schemaId() + 1;
        }
      }
      if (id != added.schemaId()) {
        return false;
      }
      schemas.add(added);
    }
    Set<Integer> oldSpecIds = new HashSet<>();
    List<org.apache.iceberg.PartitionSpec> specs = new ArrayList<>(oldMeta.specs());
    for (org.apache.iceberg.PartitionSpec s : specs) {
      oldSpecIds.add(s.specId());
    }
    for (org.apache.iceberg.PartitionSpec added : newMeta.specs()) {
      if (oldSpecIds.contains(added.specId())) {
        continue;
      }
      int id = 0;
      for (org.apache.iceberg.PartitionSpec s : specs) {
        if (added.compatibleWith(s)) {
          id = s.specId();
          break;
        } else if (s.specId() >= id) {
          id = s.specId() + 1;
        }
      }
      if (id != added.specId()) {
        return false;
      }
      specs.add(added);
    }
    return true;
  }

  /**
   * Detects whether a schema, partition spec, or sort order id exists in both
   * old and new metadata with different content. Returns true if any such
   * collision is found -- the caller should fall back to full-mode write,
   * because delta encoding can't express an in-place id swap. See the
   * computeDelta header for the concurrent-replace scenario this guards.
   */
  private static boolean idCollidesWithDifferentContent(
      TableMetadata oldMeta, TableMetadata newMeta) {
    Map<Integer, org.apache.iceberg.Schema> oldSchemasById = new HashMap<>();
    for (org.apache.iceberg.Schema s : oldMeta.schemas()) {
      oldSchemasById.put(s.schemaId(), s);
    }
    for (org.apache.iceberg.Schema s : newMeta.schemas()) {
      org.apache.iceberg.Schema oldSchema = oldSchemasById.get(s.schemaId());
      if (oldSchema != null && !oldSchema.sameSchema(s)) {
        return true;
      }
    }
    Map<Integer, org.apache.iceberg.PartitionSpec> oldSpecsById = new HashMap<>();
    for (org.apache.iceberg.PartitionSpec s : oldMeta.specs()) {
      oldSpecsById.put(s.specId(), s);
    }
    for (org.apache.iceberg.PartitionSpec s : newMeta.specs()) {
      org.apache.iceberg.PartitionSpec oldSpec = oldSpecsById.get(s.specId());
      if (oldSpec != null && !oldSpec.compatibleWith(s)) {
        return true;
      }
    }
    Map<Integer, org.apache.iceberg.SortOrder> oldOrdersById = new HashMap<>();
    for (org.apache.iceberg.SortOrder o : oldMeta.sortOrders()) {
      oldOrdersById.put(o.orderId(), o);
    }
    for (org.apache.iceberg.SortOrder o : newMeta.sortOrders()) {
      org.apache.iceberg.SortOrder oldOrder = oldOrdersById.get(o.orderId());
      if (oldOrder != null && !oldOrder.sameOrder(o)) {
        return true;
      }
    }
    return false;
  }

  private static boolean statisticsChanged(TableMetadata oldMeta, TableMetadata newMeta) {
    return !new java.util.HashSet<>(oldMeta.statisticsFiles())
            .equals(new java.util.HashSet<>(newMeta.statisticsFiles()))
        || !new java.util.HashSet<>(oldMeta.partitionStatisticsFiles())
            .equals(new java.util.HashSet<>(newMeta.partitionStatisticsFiles()));
  }

  /**
   * Whether replay derives {@code newMeta}'s new snapshots' sequence numbers, its last sequence
   * number and (v3) its next row id from {@code oldMeta}: each new snapshot numbered one after the
   * one before, from the base's last sequence number, and row ids advanced by their added rows.
   */
  private static boolean sequenceAndRowIdsFollow(TableMetadata oldMeta, TableMetadata newMeta) {
    long sequenceNumber = oldMeta.lastSequenceNumber();
    long nextRowId = oldMeta.nextRowId();
    for (org.apache.iceberg.Snapshot s : newMeta.snapshots()) {
      if (oldMeta.snapshot(s.snapshotId()) != null) {
        continue;
      }
      if (newMeta.formatVersion() >= 2 && s.sequenceNumber() != ++sequenceNumber) {
        return false;
      }
      if (s.addedRows() != null) {
        nextRowId += s.addedRows();
      }
    }
    return (newMeta.formatVersion() < 2 || newMeta.lastSequenceNumber() == sequenceNumber)
        && (newMeta.formatVersion() < 3 || newMeta.nextRowId() == nextRowId);
  }

  private static Set<String> encryptionKeyIds(TableMetadata metadata) {
    Set<String> ids = new HashSet<>();
    for (org.apache.iceberg.encryption.EncryptedKey key : metadata.encryptionKeys()) {
      ids.add(key.keyId());
    }
    return ids;
  }

  private static boolean refsEqual(
      org.apache.iceberg.SnapshotRef a, org.apache.iceberg.SnapshotRef b) {
    return a.snapshotId() == b.snapshotId()
        && a.isBranch() == b.isBranch()
        && java.util.Objects.equals(a.minSnapshotsToKeep(), b.minSnapshotsToKeep())
        && java.util.Objects.equals(a.maxSnapshotAgeMs(), b.maxSnapshotAgeMs())
        && java.util.Objects.equals(a.maxRefAgeMs(), b.maxRefAgeMs());
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
      UnboundPartitionSpec spec = org.apache.iceberg.util.JsonUtil.parse(
          new String(specJson, StandardCharsets.UTF_8),
          PartitionSpecParser::fromJson);
      builder.addPartitionSpec(spec);
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

  /**
   * Sort field entry in the binary wire grammar. Mirrors the four accessors on
   * {@link SortField} that Iceberg's JSON serialization writes:
   * source-id, transform, direction, null-order. The transform is split into a
   * closed {@code TK_*} kind plus an optional int parameter (bucket N /
   * truncate N); on encode an unrecognized {@link Transform#toString()} fails
   * loudly rather than being silently encoded as an opaque string.
   */
  public static class SortFieldEntry {
    public final int sourceId;
    public final int transformKind;   // TK_* constants
    public final int transformParam;  // 0 unless BUCKET or TRUNCATE
    public final SortDirection direction;
    public final NullOrder nullOrder;

    public SortFieldEntry(
        int sourceId,
        int transformKind,
        int transformParam,
        SortDirection direction,
        NullOrder nullOrder) {
      this.sourceId = sourceId;
      this.transformKind = transformKind;
      this.transformParam = transformParam;
      this.direction = direction;
      this.nullOrder = nullOrder;
    }

    /** Builds an entry from an Iceberg {@link SortField}, validating the transform vocabulary. */
    public static SortFieldEntry fromSortField(SortField f) {
      String t = f.transform().toString();
      int kind;
      int param = 0;
      switch (t) {
        case "identity": kind = TK_IDENTITY; break;
        case "year":     kind = TK_YEAR; break;
        case "month":    kind = TK_MONTH; break;
        case "day":      kind = TK_DAY; break;
        case "hour":     kind = TK_HOUR; break;
        case "void":     kind = TK_VOID; break;
        default:
          if (t.startsWith("bucket[") && t.endsWith("]")) {
            kind = TK_BUCKET;
            param = Integer.parseInt(t.substring("bucket[".length(), t.length() - 1));
          } else if (t.startsWith("truncate[") && t.endsWith("]")) {
            kind = TK_TRUNCATE;
            param = Integer.parseInt(t.substring("truncate[".length(), t.length() - 1));
          } else {
            throw new IllegalArgumentException(
                "Cannot encode sort field with unrecognized transform: " + t
                    + " (catalog grammar covers identity, year, month, day, hour,"
                    + " bucket[N], truncate[N], void)");
          }
      }
      return new SortFieldEntry(f.sourceId(), kind, param, f.direction(), f.nullOrder());
    }

    /** Canonical Iceberg {@code Transform.toString()} form for this entry. */
    public String transformAsString() {
      switch (transformKind) {
        case TK_IDENTITY: return "identity";
        case TK_YEAR:     return "year";
        case TK_MONTH:    return "month";
        case TK_DAY:      return "day";
        case TK_HOUR:     return "hour";
        case TK_BUCKET:   return "bucket[" + transformParam + "]";
        case TK_TRUNCATE: return "truncate[" + transformParam + "]";
        case TK_VOID:     return "void";
        default:
          throw new IllegalStateException("Unknown sort transform kind: " + transformKind);
      }
    }
  }

  public static class AddSortOrderUpdate implements DeltaUpdate {
    public final int orderId;
    public final List<SortFieldEntry> fields;

    public AddSortOrderUpdate(int orderId, List<SortFieldEntry> fields) {
      this.orderId = orderId;
      this.fields = fields;
    }

    /** Builds an update from an Iceberg {@link SortOrder}, validating each field's transform. */
    public static AddSortOrderUpdate fromSortOrder(SortOrder order) {
      List<SortFieldEntry> entries = new ArrayList<>(order.fields().size());
      for (SortField f : order.fields()) {
        entries.add(SortFieldEntry.fromSortField(f));
      }
      return new AddSortOrderUpdate(order.orderId(), entries);
    }

    /**
     * Synthesizes the JSON form Iceberg's {@code SortOrderParser} expects.
     * Kept symmetric with {@link SortOrderParser#toJson(SortOrder)} so a
     * round-trip ({@code encode → decode → SortOrderParser.toJson}) is
     * byte-stable against the original input — a property test exercises that
     * to catch upstream Iceberg adding new sort-field fields.
     */
    String toJson() {
      return JsonUtil.generate(gen -> {
        gen.writeStartObject();
        gen.writeNumberField("order-id", orderId);
        gen.writeFieldName("fields");
        gen.writeStartArray();
        for (SortFieldEntry f : fields) {
          gen.writeStartObject();
          gen.writeStringField("transform", f.transformAsString());
          gen.writeNumberField("source-id", f.sourceId);
          gen.writeStringField("direction", f.direction.toString().toLowerCase(Locale.ENGLISH));
          gen.writeStringField(
              "null-order", f.nullOrder == NullOrder.NULLS_FIRST ? "nulls-first" : "nulls-last");
          gen.writeEndObject();
        }
        gen.writeEndArray();
        gen.writeEndObject();
      }, false);
    }

    @Override
    public void applyTo(TableMetadata.Builder builder) {
      UnboundSortOrder order = SortOrderParser.fromJson(toJson());
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
      builder.setRef(refName, toRef());
    }

    /** The ref this update sets. */
    org.apache.iceberg.SnapshotRef toRef() {
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
      return refBuilder.build();
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

  /**
   * Drops schemas no longer referenced by any retained snapshot. Emitted by
   * {@code computeDelta} when {@code expireSnapshots().cleanExpiredMetadata(true)}
   * prunes the schemas-by-id map.
   *
   * <p>Routes through {@link org.apache.iceberg.MetadataUpdate.RemoveSchemas}
   * because {@code TableMetadata.Builder.removeSchemas} is package-private.
   */
  public static class RemoveSchemasUpdate implements DeltaUpdate {
    public final List<Integer> schemaIds;

    public RemoveSchemasUpdate(List<Integer> schemaIds) {
      this.schemaIds = schemaIds;
    }

    @Override
    public void applyTo(TableMetadata.Builder builder) {
      new org.apache.iceberg.MetadataUpdate.RemoveSchemas(new HashSet<>(schemaIds))
          .applyTo(builder);
    }
  }

  /**
   * Drops partition specs no longer referenced by any retained snapshot.
   * Companion to {@link RemoveSchemasUpdate} for the
   * {@code cleanExpiredMetadata=true} path.
   *
   * <p>Routes through {@link org.apache.iceberg.MetadataUpdate.RemovePartitionSpecs}
   * because {@code TableMetadata.Builder.removeSpecs} is package-private.
   */
  public static class RemovePartitionSpecsUpdate implements DeltaUpdate {
    public final List<Integer> specIds;

    public RemovePartitionSpecsUpdate(List<Integer> specIds) {
      this.specIds = specIds;
    }

    @Override
    public void applyTo(TableMetadata.Builder builder) {
      new org.apache.iceberg.MetadataUpdate.RemovePartitionSpecs(new HashSet<>(specIds))
          .applyTo(builder);
    }
  }

  public static class AddSnapshotUpdate implements DeltaUpdate {
    public final long snapshotId;
    public final String manifestListSuffix;
    /**
     * The summary's entries the update states. With {@link #inheritsParentExtra}, the summary also
     * has its parent's extra entries, less {@link #droppedExtra} ({@link #summary(MetadataView)}).
     */
    public final Map<String, String> summary;
    /** Whether the summary has its parent's extra entries too (CompactSummary). */
    public final boolean inheritsParentExtra;
    /** The parent's extra entries the summary does not have. */
    public final List<String> droppedExtra;
    // The summary with its parent's extra entries, made once: a snapshot's summary never changes,
    // so it is the same on every version of the table that has the parent
    private volatile Map<String, String> withParentExtra;
    public final long timestampDeltaMs;
    // Optional fields. Absent values:
    //   schemaId == null          -> base.currentSchemaId() (schema 0 included when present)
    //   addedRows == null         -> no row lineage (a v3 snapshot has it, 0 included)
    //   parentSnapshotId == null  -> no parent (not base.currentSnapshot(); see snapshot())
    //   firstRowId == null        -> no row lineage
    //   keyId == null             -> no encryption key
    public final Integer schemaId;
    public final Long addedRows;
    public final Long parentSnapshotId;
    public final Long firstRowId;
    public final String keyId;
    /**
     * What the producer's commit depends on beyond what it writes, if its writer knew (an inline
     * snapshot's producer); null if not.
     */
    public final Footprint footprint;

    /**
     * Back-compat constructor: no explicit parent/firstRowId/keyId; schemaId and addedRows 0 are
     * absent.
     */
    public AddSnapshotUpdate(
        long snapshotId, String manifestListSuffix, Map<String, String> summary,
        long timestampDeltaMs, int schemaId, long addedRows) {
      this(snapshotId, manifestListSuffix, summary, timestampDeltaMs,
          schemaId > 0 ? schemaId : null, addedRows > 0 ? addedRows : null, null, null, null);
    }

    /** Full constructor with optional parent snapshot id, first row id, key id. */
    public AddSnapshotUpdate(
        long snapshotId, String manifestListSuffix, Map<String, String> summary,
        long timestampDeltaMs, Integer schemaId, Long addedRows,
        Long parentSnapshotId, Long firstRowId, String keyId) {
      this(snapshotId, manifestListSuffix, summary, timestampDeltaMs, schemaId, addedRows,
          parentSnapshotId, firstRowId, keyId, null);
    }

    /** Full constructor, with the producer's footprint (null: unknown). */
    public AddSnapshotUpdate(
        long snapshotId, String manifestListSuffix, Map<String, String> summary,
        long timestampDeltaMs, Integer schemaId, Long addedRows,
        Long parentSnapshotId, Long firstRowId, String keyId, Footprint footprint) {
      this(snapshotId, manifestListSuffix, summary, false, List.of(), timestampDeltaMs, schemaId,
          addedRows, parentSnapshotId, firstRowId, keyId, footprint);
    }

    /** As decoded: a summary that may have its parent's extra entries too. */
    AddSnapshotUpdate(
        long snapshotId, String manifestListSuffix, Map<String, String> summary,
        boolean inheritsParentExtra, List<String> droppedExtra,
        long timestampDeltaMs, Integer schemaId, Long addedRows,
        Long parentSnapshotId, Long firstRowId, String keyId, Footprint footprint) {
      this.snapshotId = snapshotId;
      this.manifestListSuffix = manifestListSuffix;
      this.summary = summary;
      this.inheritsParentExtra = inheritsParentExtra;
      this.droppedExtra = droppedExtra;
      this.timestampDeltaMs = timestampDeltaMs;
      this.schemaId = schemaId;
      this.addedRows = addedRows;
      this.parentSnapshotId = parentSnapshotId;
      this.firstRowId = firstRowId;
      this.keyId = keyId;
      this.footprint = footprint;
    }

    /**
     * Applies the snapshot to metadata using dictionary-derived fields.
     * Requires the manifest list prefix from the inline table's checkpoint entry.
     */
    public TableMetadata applyTo(TableMetadata base, String manifestListPrefix) {
      return applyTo(base, manifestListPrefix, base.lastUpdatedMillis());
    }

    /**
     * Adds the snapshot to {@code base}, which may already hold the delta's earlier updates. Its
     * timestamp is relative to {@code deltaUpdatedMillis}, the delta's last-updated-ms, which the
     * commit's metadata has: adding a snapshot moves last-updated-ms, so a commit's later snapshots
     * resolved against {@code base} would get the wrong time.
     */
    public TableMetadata applyTo(
        TableMetadata base, String manifestListPrefix, long deltaUpdatedMillis) {
      // v1 tables don't carry sequence numbers; TableMetadata's constructor rejects
      // any non-zero value. Synthesize seqNum only for v2+.
      long seqNum = base.formatVersion() >= 2 ? base.lastSequenceNumber() + 1 : 0;
      Map<String, String> resolvedSummary = summary(MetadataView.of(base));
      Preconditions.checkState(
          resolvedSummary != null, "Snapshot %s inherits the summary of a snapshot %s lacks",
          snapshotId, base.metadataFileLocation());
      Snapshot snapshot =
          snapshot(
              seqNum, base.currentSchemaId(), manifestListPrefix, deltaUpdatedMillis,
              parentSnapshotId, deltaUpdatedMillis + timestampDeltaMs, firstRowId,
              resolvedSummary);
      // Iceberg's builder no longer takes last-updated-ms from an added snapshot (since 1.12.0),
      // so pin it to the snapshot's time, as it was, rather than the wall clock.
      return TableMetadata.buildFrom(base)
          .setLastUpdatedMillis(snapshot.timestampMillis())
          .addSnapshot(snapshot)
          .discardChanges()
          .build();
    }

    /**
     * The snapshot's summary on {@code table}, the table its delta lands on: the entries the update
     * states, and, if it inherits them, its parent's extra entries there, less those it drops and
     * those it states. Null if it inherits them and {@code table} lacks its parent.
     */
    Map<String, String> summary(MetadataView table) {
      if (!inheritsParentExtra) {
        return summary;
      }
      Snapshot parent = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
      if (parent == null) {
        return null;
      }
      Map<String, String> resolved = withParentExtra;
      if (resolved == null) {
        resolved = new HashMap<>(summary);
        Map<String, String> parentSummary = parent.summary() != null ? parent.summary() : Map.of();
        // In key order, so every replica builds the same map whatever order its parent's has
        String[] keys = parentSummary.keySet().toArray(new String[0]);
        Arrays.sort(keys);
        for (String key : keys) {
          String value = parentSummary.get(key);
          if (inheritable(key, value) && !droppedExtra.contains(key)
              && !resolved.containsKey(key)) {
            resolved.put(key, value);
          }
        }
        withParentExtra = resolved;
      }
      return resolved;
    }

    /** The snapshot this update adds where its delta lands, its late-bound fields finalized. */
    Snapshot snapshot(
        long sequenceNumber,
        int currentSchemaId,
        String manifestListPrefix,
        long deltaUpdatedMillis,
        Landing landing) {
      return snapshot(
          sequenceNumber, currentSchemaId, manifestListPrefix, deltaUpdatedMillis,
          landing.parent(this), landing.timestamp(deltaUpdatedMillis + timestampDeltaMs),
          landing.firstRowId(this), landing.summary(this));
    }

    private Snapshot snapshot(
        long sequenceNumber,
        int currentSchemaId,
        String manifestListPrefix,
        long deltaUpdatedMillis,
        Long resolvedParentId,
        long timestamp,
        Long resolvedFirstRowId,
        Map<String, String> resolvedSummary) {
      // parentSnapshotId is authoritative. The encoder (computeDelta)
      // captures snap.parentId() verbatim and the wire treats field absence as
      // explicit null. Falling back to base.currentSnapshot() is wrong for
      // replace transactions, where the new snapshot is intentionally rooted
      // (parent=null) but base still carries the prior incarnation's snapshot
      // — the fallback re-parented the replacement onto it, which then made
      // the manifest carry-forward in applyDeltaWithManifests inherit the
      // prior snapshot's manifest list into the replacement.
      Integer resolvedSchemaId = schemaId != null ? schemaId : currentSchemaId;
      // For inline-ML snapshots (empty suffix), the manifest-list location is a
      // sentinel; the inline-ML read path swaps the BaseSnapshot for an
      // InlineSnapshot in wrapInlineManifests before any FileIO call resolves it.
      String manifestList = manifestListSuffix.isEmpty()
          ? "inline://" + snapshotId
          : manifestListPrefix + manifestListSuffix;

      return InlineDeltaSnapshots.create(
          sequenceNumber,
          snapshotId,
          resolvedParentId,
          timestamp,
          resolvedSummary,
          resolvedSchemaId,
          manifestList,
          resolvedFirstRowId,
          addedRows,
          keyId == null || keyId.isEmpty() ? null : keyId);
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
   * adds the manifest to the table's pool and inserts it in the snapshot's reference list at
   * {@link #position}, or at the end if that is null.
   */
  public static class AddManifestUpdate implements DeltaUpdate {
    public final long snapshotId;
    public final ManifestFile manifest;
    public final String pathPrefix;
    // The manifest's index in the snapshot's list; null to append it.
    public final Integer position;

    public AddManifestUpdate(long snapshotId, ManifestFile manifest, String pathPrefix) {
      this(snapshotId, manifest, pathPrefix, null);
    }

    public AddManifestUpdate(
        long snapshotId, ManifestFile manifest, String pathPrefix, Integer position) {
      this.snapshotId = snapshotId;
      this.manifest = manifest;
      this.pathPrefix = pathPrefix;
      this.position = position;
    }

    @Override
    public void applyTo(TableMetadata.Builder builder) {
      // ML deltas are applied to CatalogState.Builder, not TableMetadata.Builder.
      // This is a no-op here; handled by applyDeltaWithManifests / applyManifestsOnly.
    }
  }

  /**
   * Removes manifests from a new snapshot's inline manifest list: one by path suffix, or those at
   * {@link #parentIndices} in its parent's list, the list of the snapshot its AddSnapshotUpdate
   * names as parent ({@link #removed}).
   */
  public static class RemoveManifestUpdate implements DeltaUpdate {
    public final long snapshotId;
    /** The path suffix of the manifest it removes; null if it names them by index. */
    public final String manifestPathSuffix;
    /** The indices in the parent's list of the manifests it removes, ascending; null if none. */
    public final int[] parentIndices;

    public RemoveManifestUpdate(long snapshotId, String manifestPathSuffix) {
      this(snapshotId, manifestPathSuffix, null);
    }

    public RemoveManifestUpdate(long snapshotId, int[] parentIndices) {
      this(snapshotId, null, parentIndices);
    }

    private RemoveManifestUpdate(long snapshotId, String manifestPathSuffix, int[] parentIndices) {
      this.snapshotId = snapshotId;
      this.manifestPathSuffix = manifestPathSuffix;
      this.parentIndices = parentIndices;
    }

    /**
     * What it removes, each matched as the end of a path: its path suffix, and the paths at its
     * indices in {@code parentList}, its parent's list (null if the table has none). Null if it
     * names an index that list lacks, or names any without one.
     */
    List<String> removed(List<String> parentList) {
      List<String> removed = new ArrayList<>();
      if (manifestPathSuffix != null) {
        removed.add(manifestPathSuffix);
      }
      if (parentIndices != null) {
        if (parentList == null) {
          return null;
        }
        for (int index : parentIndices) {
          if (index < 0 || index >= parentList.size()) {
            return null;
          }
          removed.add(parentList.get(index));
        }
      }
      return removed;
    }

    @Override
    public void applyTo(TableMetadata.Builder builder) {
      // ML deltas are applied to CatalogState.Builder, not TableMetadata.Builder.
      // This is a no-op here; handled by applyDeltaWithManifests / applyManifestsOnly.
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
    attachManifestDelta(delta, snapshotId, added, null, removedPaths, pathPrefix);
  }

  /**
   * Like {@link #attachManifestDelta(List, long, List, List, String)}, with each added manifest's
   * index in the snapshot's final list ({@code positions}, parallel to {@code added}; null to
   * append them).
   */
  public static void attachManifestDelta(
      List<DeltaUpdate> delta, long snapshotId, List<ManifestFile> added,
      List<Integer> positions, List<String> removedPaths, String pathPrefix) {
    attachManifestDelta(delta, snapshotId, added, positions, removedPaths, null, pathPrefix);
  }

  /**
   * Like {@link #attachManifestDelta(List, long, List, List, List, String)}, naming the removed
   * manifests by their indices in {@code parentPaths}, the paths of the snapshot's parent's list,
   * if it is the list replay holds for the parent (the parent's inline list in the state the delta
   * is computed on, not one the commit sends); null to name them by path.
   */
  public static void attachManifestDelta(
      List<DeltaUpdate> delta, long snapshotId, List<ManifestFile> added,
      List<Integer> positions, List<String> removedPaths, List<String> parentPaths,
      String pathPrefix) {
    for (int i = 0; i < added.size(); i++) {
      delta.add(new AddManifestUpdate(
          snapshotId, added.get(i), pathPrefix, positions == null ? null : positions.get(i)));
    }
    if (parentPaths != null && !removedPaths.isEmpty()) {
      Map<String, Integer> index = new HashMap<>();
      for (int i = 0; i < parentPaths.size(); i++) {
        index.putIfAbsent(parentPaths.get(i), i);
      }
      if (index.size() == parentPaths.size() && index.keySet().containsAll(removedPaths)) {
        delta.add(new RemoveManifestUpdate(
            snapshotId,
            removedPaths.stream().mapToInt(index::get).distinct().sorted().toArray()));
        return;
      }
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

  /**
   * The id of the one snapshot {@code updates} add, which their other snapshot ids default to;
   * null if they add none or several.
   */
  private static Long onlyNewSnapshot(List<DeltaUpdate> updates) {
    Long only = null;
    for (DeltaUpdate update : updates) {
      if (update instanceof AddSnapshotUpdate) {
        if (only != null) {
          return null;
        }
        only = ((AddSnapshotUpdate) update).snapshotId;
      }
    }
    return only;
  }

  /**
   * Encodes {@code update}, of a delta from {@code base} (null: a delta of manifest updates only,
   * which lands with full metadata) that adds the one snapshot {@code only} (null: none or
   * several). A new snapshot whose parent is in {@code base} states only the summary entries that
   * differ from its parent's, where that is shorter, and a snapshot id that is {@code only} is left
   * out of a manifest update, a ref and an added manifest that names it.
   */
  private static byte[] encodeUpdate(DeltaUpdate update, TableMetadata base, Long only)
      throws IOException {
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
      writeBytes(inner, ADD_SCHEMA_JSON, InlineMetadataIO.gzip(u.schemaJson));
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
      writeBytes(inner, ADD_SPEC_JSON, InlineMetadataIO.gzip(u.specJson));
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
      for (SortFieldEntry f : u.fields) {
        writeLengthDelimited(inner, ADD_ORDER_FIELDS, encodeSortField(f));
      }
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
      if (only == null || u.snapshotId != only) {
        writeFixed64(inner, REF_SNAPSHOT_ID, u.snapshotId);
      }
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

    } else if (update instanceof RemoveSchemasUpdate) {
      RemoveSchemasUpdate u = (RemoveSchemasUpdate) update;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      for (int id : u.schemaIds) {
        writeVarint(inner, REMOVE_SCHEMA_IDS, id);
      }
      writeLengthDelimited(out, UPDATE_REMOVE_SCHEMAS, inner.toByteArray());

    } else if (update instanceof RemovePartitionSpecsUpdate) {
      RemovePartitionSpecsUpdate u = (RemovePartitionSpecsUpdate) update;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      for (int id : u.specIds) {
        writeVarint(inner, REMOVE_SPEC_IDS, id);
      }
      writeLengthDelimited(out, UPDATE_REMOVE_PARTITION_SPECS, inner.toByteArray());

    } else if (update instanceof AddSnapshotUpdate) {
      AddSnapshotUpdate u = (AddSnapshotUpdate) update;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeFixed64(inner, SNAP_ID, u.snapshotId);
      writeString(inner, SNAP_MANIFEST_SUFFIX, u.manifestListSuffix);
      DecodedSummary summary =
          u.inheritsParentExtra
              ? new DecodedSummary(u.summary, true, u.droppedExtra)
              : stated(u.summary, parentSummary(base, u.parentSnapshotId));
      writeLengthDelimited(inner, SNAP_SUMMARY, encodeCompactSummary(summary));
      writeSVarint64(inner, SNAP_TIMESTAMP_DELTA, u.timestampDeltaMs);
      if (u.schemaId != null) {
        writeVarint(inner, SNAP_SCHEMA_ID, u.schemaId);
      }
      if (u.addedRows != null) {
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
      if (u.footprint != null) {
        writeLengthDelimited(inner, SNAP_FOOTPRINT, encodeFootprint(u.footprint));
      }
      writeLengthDelimited(out, UPDATE_ADD_SNAPSHOT, inner.toByteArray());

    } else if (update instanceof AddManifestUpdate) {
      AddManifestUpdate u = (AddManifestUpdate) update;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      if (only == null || u.snapshotId != only) {
        writeFixed64(inner, ADD_MF_SNAPSHOT_ID, u.snapshotId);
      }
      byte[] mfBytes = ProtoCodec.encodeManifestFileEntry(u.manifest, u.pathPrefix, u.snapshotId);
      writeLengthDelimited(inner, ADD_MF_MANIFEST, mfBytes);
      if (u.position != null) {
        writeVarint(inner, ADD_MF_POSITION, u.position);
      }
      writeLengthDelimited(out, UPDATE_ADD_MANIFEST, inner.toByteArray());

    } else if (update instanceof RemoveManifestUpdate) {
      RemoveManifestUpdate u = (RemoveManifestUpdate) update;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      if (only == null || u.snapshotId != only) {
        writeFixed64(inner, REMOVE_MF_SNAPSHOT_ID, u.snapshotId);
      }
      if (u.manifestPathSuffix != null) {
        writeString(inner, REMOVE_MF_PATH_SUFFIX, u.manifestPathSuffix);
      }
      if (u.parentIndices != null) {
        ByteArrayOutputStream packed = new ByteArrayOutputStream();
        for (int index : u.parentIndices) {
          writeRawVarint(packed, index);
        }
        writeLengthDelimited(inner, REMOVE_MF_PARENT_INDICES, packed.toByteArray());
      }
      writeLengthDelimited(out, UPDATE_REMOVE_MANIFEST, inner.toByteArray());
    }

    return out.toByteArray();
  }

  // ============================================================
  // Decoding
  // ============================================================

  /** The kind of the TableMetadataUpdate in {@code bytes}: the field number of its update. */
  private static int updateType(byte[] bytes) throws IOException {
    return readVarint(new ByteArrayInputStream(bytes)) >>> 3;
  }

  /**
   * Decodes a TableMetadataUpdate of a delta that adds the one snapshot {@code only} (null: none
   * or several), which a snapshot id the update leaves out is.
   */
  private static DeltaUpdate decodeUpdate(byte[] bytes, Long only) throws IOException {
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
        return decodeSetSnapshotRef(updateBytes, only);
      case UPDATE_REMOVE_SNAPSHOTS:
        return decodeRemoveSnapshots(updateBytes);
      case UPDATE_REMOVE_SCHEMAS:
        return decodeRemoveSchemas(updateBytes);
      case UPDATE_REMOVE_PARTITION_SPECS:
        return decodeRemovePartitionSpecs(updateBytes);
      case UPDATE_ADD_SNAPSHOT:
        return decodeAddSnapshot(updateBytes);
      case UPDATE_ADD_MANIFEST:
        return decodeAddManifest(updateBytes, only);
      case UPDATE_REMOVE_MANIFEST:
        return decodeRemoveManifest(updateBytes, only);
      default:
        // Forward compatibility: skip unknown update types
        return builder -> {};
    }
  }

  private static AddManifestUpdate decodeAddManifest(byte[] bytes, Long only) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    long snapshotId = only != null ? only : 0;
    byte[] manifestBytes = null;
    Integer position = null;

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fn = tag >>> 3;
      switch (fn) {
        case ADD_MF_SNAPSHOT_ID:
          snapshotId = readFixed64(in);
          break;
        case ADD_MF_MANIFEST:
          manifestBytes = readLengthDelimitedBytes(in);
          break;
        case ADD_MF_POSITION:
          position = readVarint(in);
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    // With an empty prefix: the caller resolves the full path. An added snapshot id it leaves out
    // is the update's snapshot.
    ManifestFile manifest =
        manifestBytes != null
            ? ProtoCodec.decodeManifestFileEntry(manifestBytes, "", snapshotId)
            : null;
    return new AddManifestUpdate(snapshotId, manifest, "", position);
  }

  private static RemoveManifestUpdate decodeRemoveManifest(byte[] bytes, Long only)
      throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    long snapshotId = only != null ? only : 0;
    String pathSuffix = null;
    List<Integer> indices = null;

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
        case REMOVE_MF_PARENT_INDICES:
          if (indices == null) {
            indices = new ArrayList<>();
          }
          if ((tag & 0x7) == 2) { // packed
            ByteArrayInputStream packed = new ByteArrayInputStream(readLengthDelimitedBytes(in));
            while (packed.available() > 0) {
              indices.add(readVarint(packed));
            }
          } else {
            indices.add(readVarint(in));
          }
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    if (indices == null) {
      return new RemoveManifestUpdate(snapshotId, pathSuffix != null ? pathSuffix : "");
    }
    return new RemoveManifestUpdate(
        snapshotId, pathSuffix, indices.stream().mapToInt(Integer::intValue).toArray());
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
        case ADD_SCHEMA_JSON: json = InlineMetadataIO.gunzip(readLengthDelimitedBytes(in)); break;
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
        case ADD_SPEC_JSON: json = InlineMetadataIO.gunzip(readLengthDelimitedBytes(in)); break;
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
    List<SortFieldEntry> entries = new ArrayList<>();
    while (in.available() > 0) {
      int tag = readVarint(in);
      int fn = tag >>> 3;
      switch (fn) {
        case ADD_ORDER_ID: orderId = readVarint(in); break;
        case ADD_ORDER_FIELDS: entries.add(decodeSortField(readLengthDelimitedBytes(in))); break;
        default: skipField(in, tag & 0x7);
      }
    }
    return new AddSortOrderUpdate(orderId, entries);
  }

  private static byte[] encodeSortField(SortFieldEntry f) throws IOException {
    ByteArrayOutputStream inner = new ByteArrayOutputStream();
    writeVarint(inner, SF_SOURCE_ID, f.sourceId);
    writeVarint(inner, SF_TRANSFORM_KIND, f.transformKind);
    if (f.transformKind == TK_BUCKET || f.transformKind == TK_TRUNCATE) {
      writeVarint(inner, SF_TRANSFORM_PARAM, f.transformParam);
    }
    writeVarint(inner, SF_DIRECTION, f.direction == SortDirection.ASC ? DIR_ASC : DIR_DESC);
    writeVarint(
        inner, SF_NULL_ORDER, f.nullOrder == NullOrder.NULLS_FIRST ? NO_NULLS_FIRST : NO_NULLS_LAST);
    return inner.toByteArray();
  }

  private static SortFieldEntry decodeSortField(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int sourceId = 0;
    int kind = 0;
    int param = 0;
    int dir = 0;
    int nullOrd = 0;
    while (in.available() > 0) {
      int tag = readVarint(in);
      int fn = tag >>> 3;
      switch (fn) {
        case SF_SOURCE_ID:       sourceId = readVarint(in); break;
        case SF_TRANSFORM_KIND:  kind = readVarint(in); break;
        case SF_TRANSFORM_PARAM: param = readVarint(in); break;
        case SF_DIRECTION:       dir = readVarint(in); break;
        case SF_NULL_ORDER:      nullOrd = readVarint(in); break;
        default: skipField(in, tag & 0x7);
      }
    }
    SortDirection direction;
    switch (dir) {
      case DIR_ASC:  direction = SortDirection.ASC; break;
      case DIR_DESC: direction = SortDirection.DESC; break;
      default:
        throw new IllegalStateException("Sort field missing or invalid direction: " + dir);
    }
    NullOrder nullOrder;
    switch (nullOrd) {
      case NO_NULLS_FIRST: nullOrder = NullOrder.NULLS_FIRST; break;
      case NO_NULLS_LAST:  nullOrder = NullOrder.NULLS_LAST; break;
      default:
        throw new IllegalStateException("Sort field missing or invalid null order: " + nullOrd);
    }
    if (kind < TK_IDENTITY || kind > TK_VOID) {
      throw new IllegalStateException("Sort field missing or invalid transform kind: " + kind);
    }
    return new SortFieldEntry(sourceId, kind, param, direction, nullOrder);
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

  private static SetSnapshotRefUpdate decodeSetSnapshotRef(byte[] bytes, Long only)
      throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    String refName = "", refType = "branch";
    long snapshotId = only != null ? only : 0;
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

  private static RemoveSchemasUpdate decodeRemoveSchemas(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    List<Integer> ids = new ArrayList<>();
    while (in.available() > 0) {
      int tag = readVarint(in);
      if ((tag >>> 3) == REMOVE_SCHEMA_IDS) { ids.add(readVarint(in)); }
      else { skipField(in, tag & 0x7); }
    }
    return new RemoveSchemasUpdate(ids);
  }

  private static RemovePartitionSpecsUpdate decodeRemovePartitionSpecs(byte[] bytes)
      throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    List<Integer> ids = new ArrayList<>();
    while (in.available() > 0) {
      int tag = readVarint(in);
      if ((tag >>> 3) == REMOVE_SPEC_IDS) { ids.add(readVarint(in)); }
      else { skipField(in, tag & 0x7); }
    }
    return new RemovePartitionSpecsUpdate(ids);
  }

  private static AddSnapshotUpdate decodeAddSnapshot(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    long snapId = 0, timestampDelta = 0;
    Long addedRows = null;
    String manifestSuffix = "";
    DecodedSummary summary = new DecodedSummary(new HashMap<>(), false, List.of());
    Integer schemaId = null;
    Long parentId = null;
    Long firstRowId = null;
    String keyId = null;
    Footprint footprint = null;

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
        case SNAP_FOOTPRINT: footprint = decodeFootprint(readLengthDelimitedBytes(in)); break;
        default: skipField(in, tag & 0x7);
      }
    }
    return new AddSnapshotUpdate(snapId, manifestSuffix, summary.entries,
        summary.inheritsParentExtra, summary.droppedExtra, timestampDelta,
        schemaId, addedRows, parentId, firstRowId, keyId, footprint);
  }

  private static byte[] encodeFootprint(Footprint footprint) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    switch (footprint.kind) {
      case BLIND: writeVarint(out, FP_KIND, FK_BLIND); break;
      case VALIDATED: writeVarint(out, FP_KIND, FK_VALIDATED); break;
      default: writeVarint(out, FP_KIND, FK_OPAQUE);
    }
    for (Footprint.Check check : footprint.checks) {
      ByteArrayOutputStream c = new ByteArrayOutputStream();
      switch (check.kind) {
        case NO_DATA_ADDED: writeVarint(c, FC_KIND, CK_NO_DATA_ADDED); break;
        case NO_DELETES_ADDED: writeVarint(c, FC_KIND, CK_NO_DELETES_ADDED); break;
        default: writeVarint(c, FC_KIND, CK_NO_DATA_REMOVED);
      }
      if (check.scope.filter != null) {
        writeString(c, FC_FILTER, check.scope.filter);
      } else if (check.scope.partitions != null) {
        // present, if empty, for a scope of no partitions; absent with no filter: every file
        ByteArrayOutputStream scope = new ByteArrayOutputStream();
        for (Footprint.Partition partition : check.scope.partitions) {
          ByteArrayOutputStream tuple = new ByteArrayOutputStream();
          writeVarint(tuple, PT_SPEC_ID, partition.specId);
          for (ByteBuffer value : partition.values) {
            ByteArrayOutputStream v = new ByteArrayOutputStream();
            if (value != null) {
              writeLengthDelimited(v, PV_VALUE, ByteBuffers.toByteArray(value));
            }
            writeLengthDelimited(tuple, PT_VALUES, v.toByteArray());
          }
          writeLengthDelimited(scope, PS_TUPLES, tuple.toByteArray());
        }
        writeLengthDelimited(c, FC_PARTITIONS, scope.toByteArray());
      }
      writeLengthDelimited(out, FP_CHECKS, c.toByteArray());
    }
    return out.toByteArray();
  }

  /**
   * A footprint; null for a kind this build does not know (no footprint, which coarsens), and
   * opaque for a check it cannot read.
   */
  private static Footprint decodeFootprint(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int kind = 0;
    List<Footprint.Check> checks = new ArrayList<>();
    boolean unreadable = false;
    while (in.available() > 0) {
      int tag = readVarint(in);
      if (tag >>> 3 == FP_KIND) {
        kind = readVarint(in);
      } else if (tag >>> 3 == FP_CHECKS) {
        Footprint.Check check = decodeCheck(readLengthDelimitedBytes(in));
        unreadable |= check == null;
        if (check != null) {
          checks.add(check);
        }
      } else {
        skipField(in, tag & 0x7);
      }
    }
    switch (kind) {
      case FK_BLIND:
        return Footprint.BLIND;
      case FK_OPAQUE:
        return Footprint.OPAQUE;
      case FK_VALIDATED:
        return unreadable ? Footprint.OPAQUE : Footprint.validated(checks);
      default:
        return null;
    }
  }

  /** A check, or null for a kind this build does not know. */
  private static Footprint.Check decodeCheck(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int kind = 0;
    String filter = null;
    List<Footprint.Partition> partitions = null;
    while (in.available() > 0) {
      int tag = readVarint(in);
      switch (tag >>> 3) {
        case FC_KIND: kind = readVarint(in); break;
        case FC_FILTER: filter = readString(in); break;
        case FC_PARTITIONS:
          partitions = new ArrayList<>();
          ByteArrayInputStream scope = new ByteArrayInputStream(readLengthDelimitedBytes(in));
          while (scope.available() > 0) {
            int t = readVarint(scope);
            if (t >>> 3 == PS_TUPLES) {
              partitions.add(decodePartition(readLengthDelimitedBytes(scope)));
            } else {
              skipField(scope, t & 0x7);
            }
          }
          break;
        default: skipField(in, tag & 0x7);
      }
    }
    org.apache.iceberg.CommitFootprint.Check.Kind checkKind;
    switch (kind) {
      case CK_NO_DATA_ADDED:
        checkKind = org.apache.iceberg.CommitFootprint.Check.Kind.NO_DATA_ADDED;
        break;
      case CK_NO_DELETES_ADDED:
        checkKind = org.apache.iceberg.CommitFootprint.Check.Kind.NO_DELETES_ADDED;
        break;
      case CK_NO_DATA_REMOVED:
        checkKind = org.apache.iceberg.CommitFootprint.Check.Kind.NO_DATA_REMOVED;
        break;
      default:
        return null;
    }
    Footprint.Scope scope =
        filter != null
            ? Footprint.Scope.filter(filter)
            : partitions != null ? Footprint.Scope.partitions(partitions)
                : Footprint.Scope.EVERYTHING;
    return new Footprint.Check(checkKind, scope);
  }

  private static Footprint.Partition decodePartition(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int specId = 0;
    List<ByteBuffer> values = new ArrayList<>();
    while (in.available() > 0) {
      int tag = readVarint(in);
      if (tag >>> 3 == PT_SPEC_ID) {
        specId = readVarint(in);
      } else if (tag >>> 3 == PT_VALUES) {
        ByteArrayInputStream v = new ByteArrayInputStream(readLengthDelimitedBytes(in));
        ByteBuffer value = null;
        while (v.available() > 0) {
          int t = readVarint(v);
          if (t >>> 3 == PV_VALUE) {
            value = ByteBuffer.wrap(readLengthDelimitedBytes(v));
          } else {
            skipField(v, t & 0x7);
          }
        }
        values.add(value);
      } else {
        skipField(in, tag & 0x7);
      }
    }
    return new Footprint.Partition(specId, values);
  }

  // ============================================================
  // CompactSummary encode/decode
  // ============================================================

  private static final String[] OPERATION_NAMES = {"append", "overwrite", "delete", "replace"};

  /** The summary's counts CompactSummary's typed fields hold: each key's field number. */
  private static final Map<String, Integer> SUMMARY_COUNTS = new LinkedHashMap<>();
  /** The key of each typed count, by its field number. */
  private static final String[] SUMMARY_COUNT_KEYS = new String[CS_DROPPED_EXTRA + 1];

  static {
    SUMMARY_COUNTS.put("added-data-files", CS_ADDED_DATA_FILES);
    SUMMARY_COUNTS.put("added-records", CS_ADDED_RECORDS);
    SUMMARY_COUNTS.put("added-files-size", CS_ADDED_FILES_SIZE);
    SUMMARY_COUNTS.put("total-data-files", CS_TOTAL_DATA_FILES);
    SUMMARY_COUNTS.put("total-records", CS_TOTAL_RECORDS);
    SUMMARY_COUNTS.put("total-files-size", CS_TOTAL_FILES_SIZE);
    SUMMARY_COUNTS.put("added-delete-files", CS_ADDED_DELETE_FILES);
    SUMMARY_COUNTS.put("total-delete-files", CS_TOTAL_DELETE_FILES);
    SUMMARY_COUNTS.put("total-equality-deletes", CS_TOTAL_EQUALITY_DELETES);
    SUMMARY_COUNTS.put("total-position-deletes", CS_TOTAL_POSITION_DELETES);
    SUMMARY_COUNTS.put("changed-partition-count", CS_CHANGED_PARTITION_COUNT);
    SUMMARY_COUNTS.put("deleted-data-files", CS_DELETED_DATA_FILES);
    SUMMARY_COUNTS.put("deleted-records", CS_DELETED_RECORDS);
    SUMMARY_COUNTS.put("removed-files-size", CS_REMOVED_FILES_SIZE);
    SUMMARY_COUNTS.put("manifests-created", CS_MANIFESTS_CREATED);
    SUMMARY_COUNTS.put("manifests-replaced", CS_MANIFESTS_REPLACED);
    SUMMARY_COUNTS.put("manifests-kept", CS_MANIFESTS_KEPT);
    SUMMARY_COUNTS.put("entries-processed", CS_ENTRIES_PROCESSED);
    SUMMARY_COUNTS.put("removed-delete-files", CS_REMOVED_DELETE_FILES);
    SUMMARY_COUNTS.put("added-position-deletes", CS_ADDED_POSITION_DELETES);
    SUMMARY_COUNTS.put("removed-position-deletes", CS_REMOVED_POSITION_DELETES);
    SUMMARY_COUNTS.put("added-equality-deletes", CS_ADDED_EQUALITY_DELETES);
    SUMMARY_COUNTS.put("removed-equality-deletes", CS_REMOVED_EQUALITY_DELETES);
    SUMMARY_COUNTS.put("added-position-delete-files", CS_ADDED_POSITION_DELETE_FILES);
    SUMMARY_COUNTS.put("removed-position-delete-files", CS_REMOVED_POSITION_DELETE_FILES);
    SUMMARY_COUNTS.put("added-equality-delete-files", CS_ADDED_EQUALITY_DELETE_FILES);
    SUMMARY_COUNTS.put("removed-equality-delete-files", CS_REMOVED_EQUALITY_DELETE_FILES);
    SUMMARY_COUNTS.put("added-dvs", CS_ADDED_DVS);
    SUMMARY_COUNTS.put("removed-dvs", CS_REMOVED_DVS);
    SUMMARY_COUNTS.forEach((key, field) -> SUMMARY_COUNT_KEYS[field] = key);
  }

  /** A summary as CompactSummary holds it: its stated entries, and what it takes of its parent's. */
  private static final class DecodedSummary {
    private final Map<String, String> entries;
    private final boolean inheritsParentExtra;
    private final List<String> droppedExtra;

    private DecodedSummary(
        Map<String, String> entries, boolean inheritsParentExtra, List<String> droppedExtra) {
      this.entries = entries;
      this.inheritsParentExtra = inheritsParentExtra;
      this.droppedExtra = droppedExtra;
    }
  }

  /** Whether CompactSummary holds {@code key} with {@code value} in a typed field, not an extra. */
  private static boolean typed(String key, String value) {
    if ("operation".equals(key)) {
      return Arrays.asList(OPERATION_NAMES).contains(value);
    }
    return SUMMARY_COUNTS.containsKey(key) && plainNumber(value) != null;
  }

  /**
   * Whether a child's summary can take {@code key} with {@code value} from its parent's: an extra
   * entry, but the operation, which a summary always states (the decoder's default is "append").
   */
  static boolean inheritable(String key, String value) {
    return !"operation".equals(key) && !typed(key, value);
  }

  /** {@code value} as a number if it is one written plainly ("7", not "07" or "+7"); else null. */
  private static Long plainNumber(String value) {
    if (!plain(value)) {
      return null;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return null; // out of range
    }
  }

  /** Whether {@code value} is written as Long.toString writes a number, if it is one. */
  private static boolean plain(String value) {
    if (value == null || value.isEmpty()) {
      return false;
    }
    int start = value.charAt(0) == '-' ? 1 : 0;
    int length = value.length();
    if (start == length || length - start > 19) {
      return false;
    }
    if (value.charAt(start) == '0' && (length - start > 1 || start == 1)) {
      return false; // "07", "-0"
    }
    for (int i = start; i < length; i++) {
      char c = value.charAt(i);
      if (c < '0' || c > '9') {
        return false;
      }
    }
    return true;
  }

  /**
   * The summary {@code summary} of a snapshot whose parent's summary is {@code parent} (null: none
   * to inherit from), as a delta states it: whether it takes its parent's extra entries, if that
   * saves bytes, and then the entries it states and the parent's it drops.
   */
  private static DecodedSummary stated(Map<String, String> summary, Map<String, String> parent) {
    if (parent == null) {
      return new DecodedSummary(summary, false, List.of());
    }
    Map<String, String> stated = new LinkedHashMap<>(summary);
    List<String> dropped = new ArrayList<>();
    int saved = 0;
    int cost = 2; // the flag
    for (Map.Entry<String, String> entry : parent.entrySet()) {
      if (!inheritable(entry.getKey(), entry.getValue())) {
        continue;
      }
      String value = summary.get(entry.getKey());
      if (value == null) {
        dropped.add(entry.getKey());
        cost += entry.getKey().length() + 3;
      } else if (value.equals(entry.getValue())) {
        stated.remove(entry.getKey());
        saved += entry.getKey().length() + value.length() + 6;
      }
    }
    if (saved <= cost) {
      return new DecodedSummary(summary, false, List.of());
    }
    java.util.Collections.sort(dropped);
    return new DecodedSummary(stated, true, dropped);
  }

  /** The summary of snapshot {@code parentId} in {@code base}; null if either is null or lacks it. */
  private static Map<String, String> parentSummary(TableMetadata base, Long parentId) {
    Snapshot parent = base != null && parentId != null ? base.snapshot(parentId) : null;
    if (parent == null) {
      return null;
    }
    return parent.summary() != null ? parent.summary() : Map.of();
  }

  /**
   * Encodes a snapshot summary: the operation and the standard counts as typed fields, every other
   * entry as an extra. An entry a typed field cannot hold exactly -- an operation not in {@link
   * #OPERATION_NAMES}, a count not written as a plain number -- is an extra too. A count is written
   * whenever the summary has it, 0 included: an absent count is unknown, not zero. With {@code
   * inheritsParentExtra}, the summary also has its parent's extra entries, less {@code dropped}.
   */
  private static byte[] encodeCompactSummary(DecodedSummary summary) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Map<String, String> entries = summary.entries;
    String op = entries.getOrDefault("operation", "append");
    int opVal = Arrays.asList(OPERATION_NAMES).indexOf(op);
    if (opVal > 0) {
      writeVarint(out, CS_OPERATION, opVal); // "append" is the decoder's default
    }
    for (Map.Entry<String, Integer> count : SUMMARY_COUNTS.entrySet()) {
      Long value = plainNumber(entries.get(count.getKey()));
      if (value != null) {
        writeVarint64(out, count.getValue(), value);
      }
    }
    for (Map.Entry<String, String> e : entries.entrySet()) {
      if (!typed(e.getKey(), e.getValue())) {
        out.write(encodeMapEntry(CS_EXTRA, e.getKey(), e.getValue()));
      }
    }
    if (summary.inheritsParentExtra) {
      writeVarint(out, CS_INHERITS_PARENT_EXTRA, 1);
      for (String key : summary.droppedExtra) {
        writeString(out, CS_DROPPED_EXTRA, key);
      }
    }
    return out.toByteArray();
  }

  private static DecodedSummary decodeCompactSummary(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    Map<String, String> summary = new HashMap<>();
    summary.put("operation", "append"); // default
    boolean inherits = false;
    List<String> dropped = new ArrayList<>();

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fn = tag >>> 3;
      String count = fn > 0 && fn < SUMMARY_COUNT_KEYS.length ? SUMMARY_COUNT_KEYS[fn] : null;
      if (count != null) {
        summary.put(count, String.valueOf(readVarint64(in)));
        continue;
      }
      switch (fn) {
        case CS_OPERATION:
          int op = readVarint(in);
          if (op >= 0 && op < OPERATION_NAMES.length) {
            summary.put("operation", OPERATION_NAMES[op]);
          }
          break;
        case CS_EXTRA:
          decodeMapEntry(readLengthDelimitedBytes(in), summary); break;
        case CS_INHERITS_PARENT_EXTRA:
          inherits = readVarint(in) != 0; break;
        case CS_DROPPED_EXTRA:
          dropped.add(readString(in)); break;
        default: skipField(in, tag & 0x7);
      }
    }
    return new DecodedSummary(summary, inherits, dropped);
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
