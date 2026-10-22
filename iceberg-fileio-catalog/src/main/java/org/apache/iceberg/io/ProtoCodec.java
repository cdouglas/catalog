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

import static org.intentlog.wire.ProtoWire.readFixed64;
import static org.intentlog.wire.ProtoWire.readLengthDelimitedBytes;
import static org.intentlog.wire.ProtoWire.readString;
import static org.intentlog.wire.ProtoWire.readVarint;
import static org.intentlog.wire.ProtoWire.readVarint64;
import static org.intentlog.wire.ProtoWire.skipField;
import static org.intentlog.wire.ProtoWire.writeBytes;
import static org.intentlog.wire.ProtoWire.writeFixed64;
import static org.intentlog.wire.ProtoWire.writeLengthDelimited;
import static org.intentlog.wire.ProtoWire.writeString;
import static org.intentlog.wire.ProtoWire.writeVarint;
import static org.intentlog.wire.ProtoWire.writeVarint64;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.ManifestFile.PartitionFieldSummary;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.intentlog.Intention;
import org.intentlog.wire.LogEnvelope;
import org.intentlog.wire.UuidV7;

/**
 * Protobuf encoding/decoding for catalog state.
 *
 * <p>This codec implements protobuf-compatible wire format manually, allowing the format
 * to be validated before integrating generated protobuf classes. The encoding follows
 * protobuf conventions (varints, length-delimited fields, field tags) for compatibility.
 *
 * <p>Wire format reference:
 * <ul>
 *   <li>Field tag = (field_number << 3) | wire_type</li>
 *   <li>Wire type 0: Varint (int32, int64, bool)</li>
 *   <li>Wire type 2: Length-delimited (string, bytes, nested message)</li>
 * </ul>
 */
public class ProtoCodec {

  /**
   * The reader-format version this build supports. The encoder stamps this
   * value on every checkpoint via {@code CHECKPOINT_MIN_READER_VERSION}; the
   * decoder refuses to load a checkpoint whose stamp exceeds this value. Bump
   * only when introducing a wire-format change a prior reader cannot skip
   * safely (new action types it must interpret, repurposed field numbers,
   * altered semantics on an existing field). New tags an old reader can
   * skip via {@code default → skipField} do not require a bump.
   */
  public static final int SUPPORTED_READER_VERSION = 1;

  // Wire types

  // Checkpoint field numbers (from catalog.proto)
  private static final int CHECKPOINT_NEXT_NAMESPACE_ID = 2;
  private static final int CHECKPOINT_NEXT_TABLE_ID = 3;
  /**
   * Minimum reader version required to interpret this checkpoint. The writer
   * stamps {@link #SUPPORTED_READER_VERSION} on every encode; the reader
   * refuses to load a checkpoint whose value exceeds its own
   * {@link #SUPPORTED_READER_VERSION}. Bumped only on wire-format changes a
   * v1 reader cannot interpret correctly (new actions, repurposed field
   * numbers). Forward-compatible additions (new tag the decoder can skip
   * safely) do not bump the version.
   */
  private static final int CHECKPOINT_MIN_READER_VERSION = 4;
  private static final int CHECKPOINT_NAMESPACES = 10;
  private static final int CHECKPOINT_TABLES = 11;
  private static final int CHECKPOINT_NAMESPACE_PROPERTIES = 12;
  private static final int CHECKPOINT_INLINE_TABLES = 13;

  // CommittedTransactions field numbers (compressed UUIDv7 set).
  // Entries are interleaved (varint delta + 10 random bytes per entry,
  // back-to-back) into a single packed_entries blob so the scanner can walk
  // the wire bytes in one linear pass without allocating intermediates.

  // Namespace field numbers
  private static final int NS_ID = 1;
  private static final int NS_VERSION = 2;
  private static final int NS_PARENT_ID = 3;
  private static final int NS_NAME = 4;

  // Table field numbers
  private static final int TBL_ID = 1;
  private static final int TBL_VERSION = 2;
  private static final int TBL_NAMESPACE_ID = 3;
  private static final int TBL_NAME = 4;
  private static final int TBL_METADATA_LOCATION = 5;

  // NamespaceProperty field numbers
  private static final int NSPROP_NAMESPACE_ID = 1;
  private static final int NSPROP_KEY = 2;
  private static final int NSPROP_VALUE = 3;

  // InlineTable field numbers
  private static final int INLINE_TBL_ID = 1;
  private static final int INLINE_TBL_VERSION = 2;
  private static final int INLINE_TBL_NAMESPACE_ID = 3;
  private static final int INLINE_TBL_NAME = 4;
  private static final int INLINE_TBL_METADATA = 5;
  private static final int INLINE_TBL_MANIFEST_PREFIX = 6;
  private static final int INLINE_TBL_MANIFEST_POOL = 7;
  private static final int INLINE_TBL_SNAPSHOT_REFS = 8;
  private static final int INLINE_TBL_METADATA_CODEC = 9;

  // ManifestFileEntry field numbers
  private static final int MF_PATH_SUFFIX = 1;
  private static final int MF_LENGTH = 2;
  private static final int MF_SPEC_ID = 3;
  private static final int MF_CONTENT = 4;
  private static final int MF_SEQ_NUM = 5;
  private static final int MF_MIN_SEQ_NUM = 6;
  private static final int MF_SNAPSHOT_ID = 7;
  private static final int MF_ADDED_FILES = 8;
  private static final int MF_EXISTING_FILES = 9;
  private static final int MF_DELETED_FILES = 10;
  private static final int MF_ADDED_ROWS = 11;
  private static final int MF_EXISTING_ROWS = 12;
  private static final int MF_DELETED_ROWS = 13;
  private static final int MF_PARTITIONS = 14;
  private static final int MF_KEY_METADATA = 15;
  private static final int MF_FIRST_ROW_ID = 16;
  private static final int MF_NO_PARTITIONS = 17; // present (true) when partitions is null

  // PartitionFieldSummaryEntry field numbers
  private static final int PFS_CONTAINS_NULL = 1;
  private static final int PFS_CONTAINS_NAN = 2;
  private static final int PFS_LOWER_BOUND = 3;
  private static final int PFS_UPPER_BOUND = 4;

  // SnapshotManifestRefs field numbers
  private static final int SMR_SNAPSHOT_ID = 1;
  private static final int SMR_POOL_INDICES = 2;

  // Transaction field numbers
  private static final int INTENTION_ACTIONS = 3;

  // Action type field numbers (oneof)
  private static final int ACTION_CREATE_NAMESPACE = 1;
  private static final int ACTION_DROP_NAMESPACE = 2;
  private static final int ACTION_SET_NS_PROPERTY = 3;
  private static final int ACTION_REMOVE_NS_PROPERTY = 4;
  private static final int ACTION_CREATE_TABLE = 5;
  private static final int ACTION_DROP_TABLE = 6;
  private static final int ACTION_UPDATE_TABLE_LOCATION = 7;
  private static final int ACTION_READ_TABLE = 8;
  private static final int ACTION_UPDATE_TABLE_INLINE = 9;
  private static final int ACTION_CREATE_TABLE_INLINE = 10;
  private static final int ACTION_RENAME_TABLE = 11;

  // CreateNamespace field numbers
  private static final int CREATE_NS_ID = 1;
  private static final int CREATE_NS_VERSION = 2;
  private static final int CREATE_NS_PARENT_ID = 3;
  private static final int CREATE_NS_PARENT_VERSION = 4;
  private static final int CREATE_NS_NAME = 5;

  // DropNamespace field numbers
  private static final int DROP_NS_ID = 1;
  private static final int DROP_NS_VERSION = 2;

  // SetNamespaceProperty field numbers
  private static final int SET_NSPROP_NS_ID = 1;
  private static final int SET_NSPROP_NS_VERSION = 2;
  private static final int SET_NSPROP_KEY = 3;
  private static final int SET_NSPROP_VALUE = 4;

  // RemoveNamespaceProperty field numbers
  private static final int REMOVE_NSPROP_NS_ID = 1;
  private static final int REMOVE_NSPROP_NS_VERSION = 2;
  private static final int REMOVE_NSPROP_KEY = 3;

  // CreateTable field numbers
  private static final int CREATE_TBL_ID = 1;
  private static final int CREATE_TBL_VERSION = 2;
  private static final int CREATE_TBL_NS_ID = 3;
  private static final int CREATE_TBL_NS_VERSION = 4;
  private static final int CREATE_TBL_NAME = 5;
  private static final int CREATE_TBL_METADATA_LOCATION = 6;

  // DropTable field numbers
  private static final int DROP_TBL_ID = 1;
  private static final int DROP_TBL_VERSION = 2;

  // UpdateTableLocation field numbers
  private static final int UPDATE_TBL_ID = 1;
  private static final int UPDATE_TBL_VERSION = 2;
  private static final int UPDATE_TBL_METADATA_LOCATION = 3;

  // ReadTable field numbers
  private static final int READ_TBL_ID = 1;
  private static final int READ_TBL_VERSION = 2;

  // UpdateTableInline field numbers
  private static final int UPDATE_INLINE_TBL_ID = 1;
  private static final int UPDATE_INLINE_TBL_VERSION = 2;
  // oneof payload field numbers:
  private static final int UPDATE_INLINE_DELTA = 3;
  private static final int UPDATE_INLINE_FULL_METADATA = 4;
  private static final int UPDATE_INLINE_METADATA_LOCATION = 5;
  // Codec for full_metadata bytes; ignored in delta / pointer modes.
  private static final int UPDATE_INLINE_FULL_METADATA_CODEC = 6;
  // Optional ML pool delta bytes carried alongside FULL mode: the commit's
  // manifest-list updates, which must land in the pool atomically with the
  // TM replacement. Same wire shape as
  // {@link #CREATE_INLINE_TBL_DELTA}.
  private static final int UPDATE_INLINE_ML_DELTA = 7;

  // CreateTableInline field numbers
  private static final int CREATE_INLINE_TBL_ID = 1;
  private static final int CREATE_INLINE_TBL_VERSION = 2;
  private static final int CREATE_INLINE_TBL_NS_ID = 3;
  private static final int CREATE_INLINE_TBL_NS_VERSION = 4;
  private static final int CREATE_INLINE_TBL_NAME = 5;
  private static final int CREATE_INLINE_TBL_METADATA = 6;
  private static final int CREATE_INLINE_TBL_METADATA_CODEC = 7;
  // Optional delta bytes carrying initial manifest-pool entries for an
  // inline-ML create-with-snapshots commit.
  private static final int CREATE_INLINE_TBL_DELTA = 8;

  // RenameTable field numbers
  private static final int RENAME_TBL_ID = 1;
  private static final int RENAME_TBL_VERSION = 2;
  private static final int RENAME_TBL_NEW_NS_ID = 3;
  private static final int RENAME_TBL_NEW_NS_VERSION = 4;
  private static final int RENAME_TBL_NEW_NAME = 5;

  // ============================================================
  // Checkpoint encoding/decoding
  // ============================================================

  /**
   * Encodes a checkpoint of {@code original}, the state a replay reached, in the log's envelope.
   * It holds exactly that state, committed set included: a GC of the set is a record in the log
   * (AdvanceHorizon), never a filter on a checkpoint, so every checkpoint of a log is a replay of
   * it (I11).
   */
  public static byte[] encodeCheckpoint(
      ProtoCatalogFile original,
      CatalogFile.Mut<ProtoCatalogFile, ?> mut,
      ProtoCatalogFormat.ProtoIdManager idManager) {
    // The catalog's id and committed transactions (a compressed UUIDv7 set) are the log's: the
    // envelope (log.proto LogCheckpoint) carries them, and the catalog's Checkpoint as its state.
    return LogEnvelope.encodeCheckpoint(
        original.uuid(), original.committedTransactionsBytes(),
        encodeCheckpointState(original, idManager));
  }

  /** Encodes the catalog's state, the payload of a checkpoint (catalog.proto Checkpoint). */
  public static byte[] encodeCheckpointState(
      ProtoCatalogFile original, ProtoCatalogFormat.ProtoIdManager idManager) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();

      // Stamp first so the reader fails fast on a version mismatch before
      // chewing through the rest of the payload.
      writeVarint(out, CHECKPOINT_MIN_READER_VERSION, SUPPORTED_READER_VERSION);

      // Next IDs: the original state's. Replay allocates the ids of the
      // transaction record that follows this checkpoint.
      writeVarint(out, CHECKPOINT_NEXT_NAMESPACE_ID, idManager.getNextNsid());
      writeVarint(out, CHECKPOINT_NEXT_TABLE_ID, idManager.getNextTblid());

      // Namespaces - write from original state (mutations will be applied via transaction)
      for (Map.Entry<Integer, ProtoCatalogFile.NsEntry> entry : original.namespaceById().entrySet()) {
        byte[] nsBytes = encodeNamespace(entry.getKey(), entry.getValue());
        writeLengthDelimited(out, CHECKPOINT_NAMESPACES, nsBytes);
      }

      // Tables (pointer-mode only; inline tables encoded separately)
      for (Map.Entry<Integer, ProtoCatalogFile.TblEntry> entry : original.tableById().entrySet()) {
        if (!original.isInlineTable(entry.getKey())) {
          byte[] tblBytes = encodeTable(entry.getKey(), entry.getValue());
          writeLengthDelimited(out, CHECKPOINT_TABLES, tblBytes);
        }
      }

      // Namespace properties
      for (Map.Entry<Integer, Map<String, String>> nsProps : original.allNamespaceProperties().entrySet()) {
        int nsId = nsProps.getKey();
        for (Map.Entry<String, String> prop : nsProps.getValue().entrySet()) {
          byte[] propBytes = encodeNamespaceProperty(nsId, prop.getKey(), prop.getValue());
          writeLengthDelimited(out, CHECKPOINT_NAMESPACE_PROPERTIES, propBytes);
        }
      }

      // Inline tables (with optional manifest pool + snapshot refs)
      for (Map.Entry<Integer, byte[]> inlineEntry : original.allInlineMetadata().entrySet()) {
        int tblId = inlineEntry.getKey();
        ProtoCatalogFile.TblEntry tblEntry = original.tableById().get(tblId);
        if (tblEntry != null) {
          String prefix = original.allManifestPrefixes().getOrDefault(tblId, "");
          Map<String, ManifestFile> pool = original.manifestPool(tblId);
          Map<Long, List<String>> snapRefs = original.snapshotManifests(tblId);
          byte codecTag = original.inlineMetadataCodec(tblId);
          byte[] inlineBytes = encodeInlineTable(
              tblId, tblEntry, inlineEntry.getValue(), codecTag, prefix, pool, snapRefs);
          writeLengthDelimited(out, CHECKPOINT_INLINE_TABLES, inlineBytes);
        }
      }

      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Decodes a checkpoint (a log.proto LogCheckpoint whose state is the catalog's Checkpoint) into
   * a builder.
   */
  public static void decodeCheckpoint(byte[] bytes, ProtoCatalogFile.Builder builder) {
    LogEnvelope.Checkpoint envelope = LogEnvelope.decodeCheckpoint(bytes);
    builder.setCatalogUuid(envelope.logId());
    builder.setCommittedTransactionsBytes(envelope.committed());
    decodeCheckpointState(envelope.state(), builder);
  }

  /** Decodes the catalog's state, the payload of a checkpoint (catalog.proto Checkpoint). */
  public static void decodeCheckpointState(byte[] state, ProtoCatalogFile.Builder builder) {
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(state);
      boolean sawMinReaderVersion = false;
      while (in.available() > 0) {
        int tag = readVarint(in);
        int fieldNumber = tag >>> 3;
        int wireType = tag & 0x7;

        switch (fieldNumber) {
          case CHECKPOINT_MIN_READER_VERSION:
            int minReaderVersion = readVarint(in);
            if (minReaderVersion > SUPPORTED_READER_VERSION) {
              throw new IllegalStateException(
                  "Checkpoint requires reader version "
                      + minReaderVersion
                      + " but this build supports up to "
                      + SUPPORTED_READER_VERSION
                      + "; the file was written by a newer build");
            }
            sawMinReaderVersion = true;
            break;
          case CHECKPOINT_NEXT_NAMESPACE_ID:
            builder.setNextNamespaceId(readVarint(in));
            break;
          case CHECKPOINT_NEXT_TABLE_ID:
            builder.setNextTableId(readVarint(in));
            break;
          case CHECKPOINT_NAMESPACES:
            decodeNamespace(readLengthDelimitedBytes(in), builder);
            break;
          case CHECKPOINT_TABLES:
            decodeTable(readLengthDelimitedBytes(in), builder);
            break;
          case CHECKPOINT_NAMESPACE_PROPERTIES:
            decodeNamespaceProperty(readLengthDelimitedBytes(in), builder);
            break;
          case CHECKPOINT_INLINE_TABLES:
            decodeInlineTable(readLengthDelimitedBytes(in), builder);
            break;
          default:
            skipField(in, wireType);
        }
      }
      Preconditions.checkState(
          sawMinReaderVersion,
          "Checkpoint missing required min_reader_version field");
      // Rebuild lookups after all entries are loaded (handles out-of-order decoding)
      builder.rebuildLookups();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static byte[] encodeNamespace(int id, ProtoCatalogFile.NsEntry entry) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeVarint(out, NS_ID, id);
    writeVarint(out, NS_VERSION, entry.version);
    writeVarint(out, NS_PARENT_ID, entry.parentId);
    writeString(out, NS_NAME, entry.name);
    return out.toByteArray();
  }

  private static void decodeNamespace(byte[] bytes, ProtoCatalogFile.Builder builder) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int id = 0, version = 0, parentId = 0;
    String name = "";

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fieldNumber = tag >>> 3;
      switch (fieldNumber) {
        case NS_ID:
          id = readVarint(in);
          break;
        case NS_VERSION:
          version = readVarint(in);
          break;
        case NS_PARENT_ID:
          parentId = readVarint(in);
          break;
        case NS_NAME:
          name = readString(in);
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    builder.addNamespace(id, parentId, name, version);
  }

  private static byte[] encodeTable(int id, ProtoCatalogFile.TblEntry entry) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeVarint(out, TBL_ID, id);
    writeVarint(out, TBL_VERSION, entry.version);
    writeVarint(out, TBL_NAMESPACE_ID, entry.namespaceId);
    writeString(out, TBL_NAME, entry.name);
    writeString(out, TBL_METADATA_LOCATION, entry.metadataLocation);
    return out.toByteArray();
  }

  private static void decodeTable(byte[] bytes, ProtoCatalogFile.Builder builder) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int id = 0, version = 0, nsId = 0;
    String name = "", metadataLocation = "";

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fieldNumber = tag >>> 3;
      switch (fieldNumber) {
        case TBL_ID:
          id = readVarint(in);
          break;
        case TBL_VERSION:
          version = readVarint(in);
          break;
        case TBL_NAMESPACE_ID:
          nsId = readVarint(in);
          break;
        case TBL_NAME:
          name = readString(in);
          break;
        case TBL_METADATA_LOCATION:
          metadataLocation = readString(in);
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    builder.addTable(id, nsId, name, version, metadataLocation);
  }

  private static byte[] encodeNamespaceProperty(int nsId, String key, String value) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeVarint(out, NSPROP_NAMESPACE_ID, nsId);
    writeString(out, NSPROP_KEY, key);
    writeString(out, NSPROP_VALUE, value);
    return out.toByteArray();
  }

  private static void decodeNamespaceProperty(byte[] bytes, ProtoCatalogFile.Builder builder) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int nsId = 0;
    String key = "", value = "";

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fieldNumber = tag >>> 3;
      switch (fieldNumber) {
        case NSPROP_NAMESPACE_ID:
          nsId = readVarint(in);
          break;
        case NSPROP_KEY:
          key = readString(in);
          break;
        case NSPROP_VALUE:
          value = readString(in);
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    builder.setNamespaceProperty(nsId, key, value);
  }

  private static byte[] encodeInlineTable(
      int id, ProtoCatalogFile.TblEntry entry, byte[] metadata, byte metadataCodec,
      String manifestPrefix,
      Map<String, ManifestFile> pool, Map<Long, List<String>> snapRefs)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeVarint(out, INLINE_TBL_ID, id);
    writeVarint(out, INLINE_TBL_VERSION, entry.version);
    writeVarint(out, INLINE_TBL_NAMESPACE_ID, entry.namespaceId);
    writeString(out, INLINE_TBL_NAME, entry.name);
    writeBytes(out, INLINE_TBL_METADATA, metadata);
    writeString(out, INLINE_TBL_MANIFEST_PREFIX, manifestPrefix);
    writeVarint(out, INLINE_TBL_METADATA_CODEC, metadataCodec & 0xFF);

    // Manifest pool (field 7): encode each unique ManifestFile entry, and build the
    // positional index for snapshot refs
    Map<String, Integer> pathToIndex = new java.util.HashMap<>();
    if (pool != null) {
      List<String> poolOrder = new ArrayList<>(pool.keySet());
      for (int i = 0; i < poolOrder.size(); i++) {
        pathToIndex.put(poolOrder.get(i), i);
        ManifestFile mf = pool.get(poolOrder.get(i));
        byte[] mfBytes = encodeManifestFileEntry(mf, manifestPrefix);
        writeLengthDelimited(out, INLINE_TBL_MANIFEST_POOL, mfBytes);
      }
    }

    // Snapshot refs (field 8): per-snapshot index arrays. A snapshot without manifests (a delete
    // that matched nothing) has an empty array but still needs its entry, even when the pool is
    // empty: its inline:// manifest-list location resolves through it.
    if (snapRefs != null) {
      for (Map.Entry<Long, List<String>> snapEntry : snapRefs.entrySet()) {
        byte[] refBytes = encodeSnapshotManifestRefs(
            snapEntry.getKey(), snapEntry.getValue(), pathToIndex);
        writeLengthDelimited(out, INLINE_TBL_SNAPSHOT_REFS, refBytes);
      }
    }

    return out.toByteArray();
  }

  private static byte[] encodeSnapshotManifestRefs(
      long snapshotId, List<String> paths, Map<String, Integer> pathToIndex)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeFixed64(out, SMR_SNAPSHOT_ID, snapshotId);
    for (String path : paths) {
      Integer idx = pathToIndex.get(path);
      if (idx != null) {
        writeVarint(out, SMR_POOL_INDICES, idx);
      }
    }
    return out.toByteArray();
  }

  private static void decodeInlineTable(byte[] bytes, ProtoCatalogFile.Builder builder)
      throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int id = 0, version = 0, nsId = 0;
    String name = "", manifestPrefix = "";
    byte[] metadata = new byte[0];
    Byte metadataCodec = null;
    List<byte[]> poolEntryBytes = new ArrayList<>();
    List<byte[]> snapRefBytes = new ArrayList<>();

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fieldNumber = tag >>> 3;
      switch (fieldNumber) {
        case INLINE_TBL_ID:
          id = readVarint(in);
          break;
        case INLINE_TBL_VERSION:
          version = readVarint(in);
          break;
        case INLINE_TBL_NAMESPACE_ID:
          nsId = readVarint(in);
          break;
        case INLINE_TBL_NAME:
          name = readString(in);
          break;
        case INLINE_TBL_METADATA:
          metadata = readLengthDelimitedBytes(in);
          break;
        case INLINE_TBL_MANIFEST_PREFIX:
          manifestPrefix = readString(in);
          break;
        case INLINE_TBL_MANIFEST_POOL:
          poolEntryBytes.add(readLengthDelimitedBytes(in));
          break;
        case INLINE_TBL_SNAPSHOT_REFS:
          snapRefBytes.add(readLengthDelimitedBytes(in));
          break;
        case INLINE_TBL_METADATA_CODEC:
          metadataCodec = (byte) readVarint(in);
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    Preconditions.checkState(
        metadataCodec != null,
        "Inline table %s record missing required codec tag", id);
    builder.addInlineTable(id, nsId, name, version, metadata, metadataCodec, manifestPrefix);

    // Decode manifest pool (positional array) -> path-keyed map
    List<ManifestFile> poolList = new ArrayList<>(poolEntryBytes.size());
    for (byte[] entryBytes : poolEntryBytes) {
      poolList.add(decodeManifestFileEntry(entryBytes, manifestPrefix));
    }
    for (ManifestFile mf : poolList) {
      builder.addManifestToPool(id, mf);
    }

    // Decode snapshot refs (index arrays) -> path lists; a ref may be empty (see encode)
    for (byte[] refBytes : snapRefBytes) {
      decodeSnapshotManifestRefs(refBytes, id, poolList, builder);
    }
  }

  private static void decodeSnapshotManifestRefs(
      byte[] bytes, int tblId, List<ManifestFile> poolList,
      ProtoCatalogFile.Builder builder) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    long snapshotId = 0;
    List<Integer> indices = new ArrayList<>();

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fieldNumber = tag >>> 3;
      int wireType = tag & 0x7;
      switch (fieldNumber) {
        case SMR_SNAPSHOT_ID:
          snapshotId = readFixed64(in);
          break;
        case SMR_POOL_INDICES:
          indices.add(readVarint(in));
          break;
        default:
          skipField(in, wireType);
      }
    }

    List<String> paths = new ArrayList<>(indices.size());
    for (int idx : indices) {
      if (idx >= 0 && idx < poolList.size()) {
        paths.add(poolList.get(idx).path());
      }
    }
    builder.setSnapshotManifests(tblId, snapshotId, paths);
  }

  // ============================================================
  // Transaction encoding/decoding
  // ============================================================

  /**
   * Encodes a transaction as a log record (log.proto LogRecord): its id, and its actions as the
   * record's intention (the catalog's Intention).
   */
  public static byte[] encodeTransaction(Transaction txn) {
    return LogEnvelope.encodeRecord(txn.id(), encodeIntention(txn.intention()));
  }

  /** Encodes an intention, the payload of a log record (catalog.proto Intention). */
  public static byte[] encodeIntention(CatalogIntention intention) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      for (Action action : intention.actions()) {
        writeLengthDelimited(out, INTENTION_ACTIONS, encodeAction(action));
      }
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Decodes a transaction from a log record, which must carry an intention. */
  public static Transaction decodeTransaction(byte[] bytes) {
    LogEnvelope.Record record = LogEnvelope.decodeRecord(bytes);
    Preconditions.checkArgument(
        !record.isAdvanceHorizon(), "The record is a GC record, not a transaction");
    return decodeTransaction(record.id(), record.intention());
  }

  /** Decodes the transaction of id {@code id} from its intention (catalog.proto Intention). */
  public static Transaction decodeTransaction(UUID id, byte[] intention) {
    return new Transaction(id, decodeIntention(intention));
  }

  /** Decodes an intention, the payload of a log record (catalog.proto Intention). */
  public static CatalogIntention decodeIntention(byte[] intention) {
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(intention);
      List<Action> actions = new ArrayList<>();

      while (in.available() > 0) {
        int tag = readVarint(in);
        int fieldNumber = tag >>> 3;
        switch (fieldNumber) {
          case INTENTION_ACTIONS:
            actions.add(decodeAction(readLengthDelimitedBytes(in)));
            break;
          default:
            skipField(in, tag & 0x7);
        }
      }

      return new CatalogIntention(actions);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // ============================================================
  // Action encoding/decoding
  // ============================================================

  private static byte[] encodeAction(Action action) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    if (action instanceof CreateNamespaceAction) {
      CreateNamespaceAction a = (CreateNamespaceAction) action;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeVarint(inner, CREATE_NS_ID, a.id);
      writeVarint(inner, CREATE_NS_VERSION, a.version);
      writeVarint(inner, CREATE_NS_PARENT_ID, a.parentId);
      writeVarint(inner, CREATE_NS_PARENT_VERSION, a.parentVersion);
      writeString(inner, CREATE_NS_NAME, a.name);
      writeLengthDelimited(out, ACTION_CREATE_NAMESPACE, inner.toByteArray());
    } else if (action instanceof DropNamespaceAction) {
      DropNamespaceAction a = (DropNamespaceAction) action;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeVarint(inner, DROP_NS_ID, a.id);
      writeVarint(inner, DROP_NS_VERSION, a.version);
      writeLengthDelimited(out, ACTION_DROP_NAMESPACE, inner.toByteArray());
    } else if (action instanceof SetNamespacePropertyAction) {
      SetNamespacePropertyAction a = (SetNamespacePropertyAction) action;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeVarint(inner, SET_NSPROP_NS_ID, a.namespaceId);
      writeVarint(inner, SET_NSPROP_NS_VERSION, a.namespaceVersion);
      writeString(inner, SET_NSPROP_KEY, a.key);
      writeString(inner, SET_NSPROP_VALUE, a.value);
      writeLengthDelimited(out, ACTION_SET_NS_PROPERTY, inner.toByteArray());
    } else if (action instanceof RemoveNamespacePropertyAction) {
      RemoveNamespacePropertyAction a = (RemoveNamespacePropertyAction) action;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeVarint(inner, REMOVE_NSPROP_NS_ID, a.namespaceId);
      writeVarint(inner, REMOVE_NSPROP_NS_VERSION, a.namespaceVersion);
      writeString(inner, REMOVE_NSPROP_KEY, a.key);
      writeLengthDelimited(out, ACTION_REMOVE_NS_PROPERTY, inner.toByteArray());
    } else if (action instanceof CreateTableAction) {
      CreateTableAction a = (CreateTableAction) action;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeVarint(inner, CREATE_TBL_ID, a.id);
      writeVarint(inner, CREATE_TBL_VERSION, a.version);
      writeVarint(inner, CREATE_TBL_NS_ID, a.namespaceId);
      writeVarint(inner, CREATE_TBL_NS_VERSION, a.namespaceVersion);
      writeString(inner, CREATE_TBL_NAME, a.name);
      writeString(inner, CREATE_TBL_METADATA_LOCATION, a.metadataLocation);
      writeLengthDelimited(out, ACTION_CREATE_TABLE, inner.toByteArray());
    } else if (action instanceof DropTableAction) {
      DropTableAction a = (DropTableAction) action;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeVarint(inner, DROP_TBL_ID, a.id);
      writeVarint(inner, DROP_TBL_VERSION, a.version);
      writeLengthDelimited(out, ACTION_DROP_TABLE, inner.toByteArray());
    } else if (action instanceof UpdateTableLocationAction) {
      UpdateTableLocationAction a = (UpdateTableLocationAction) action;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeVarint(inner, UPDATE_TBL_ID, a.id);
      writeVarint(inner, UPDATE_TBL_VERSION, a.version);
      writeString(inner, UPDATE_TBL_METADATA_LOCATION, a.metadataLocation);
      writeLengthDelimited(out, ACTION_UPDATE_TABLE_LOCATION, inner.toByteArray());
    } else if (action instanceof ReadTableAction) {
      ReadTableAction a = (ReadTableAction) action;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeVarint(inner, READ_TBL_ID, a.id);
      writeVarint(inner, READ_TBL_VERSION, a.version);
      writeLengthDelimited(out, ACTION_READ_TABLE, inner.toByteArray());
    } else if (action instanceof UpdateTableInlineAction) {
      UpdateTableInlineAction a = (UpdateTableInlineAction) action;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeVarint(inner, UPDATE_INLINE_TBL_ID, a.id);
      writeVarint(inner, UPDATE_INLINE_TBL_VERSION, a.version);
      if (a.deltaBytes != null) {
        writeBytes(inner, UPDATE_INLINE_DELTA, a.deltaBytes);
      } else if (a.fullMetadata != null) {
        writeBytes(inner, UPDATE_INLINE_FULL_METADATA, a.fullMetadata);
        writeVarint(inner, UPDATE_INLINE_FULL_METADATA_CODEC, a.fullMetadataCodec & 0xFF);
        if (a.mlDeltaBytes != null) {
          writeBytes(inner, UPDATE_INLINE_ML_DELTA, a.mlDeltaBytes);
        }
      } else if (a.metadataLocation != null) {
        writeString(inner, UPDATE_INLINE_METADATA_LOCATION, a.metadataLocation);
      }
      writeLengthDelimited(out, ACTION_UPDATE_TABLE_INLINE, inner.toByteArray());
    } else if (action instanceof CreateTableInlineAction) {
      CreateTableInlineAction a = (CreateTableInlineAction) action;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeVarint(inner, CREATE_INLINE_TBL_ID, a.id);
      writeVarint(inner, CREATE_INLINE_TBL_VERSION, a.version);
      writeVarint(inner, CREATE_INLINE_TBL_NS_ID, a.namespaceId);
      writeVarint(inner, CREATE_INLINE_TBL_NS_VERSION, a.namespaceVersion);
      writeString(inner, CREATE_INLINE_TBL_NAME, a.name);
      writeBytes(inner, CREATE_INLINE_TBL_METADATA, a.metadata);
      writeVarint(inner, CREATE_INLINE_TBL_METADATA_CODEC, a.metadataCodec & 0xFF);
      if (a.deltaBytes != null) {
        writeBytes(inner, CREATE_INLINE_TBL_DELTA, a.deltaBytes);
      }
      writeLengthDelimited(out, ACTION_CREATE_TABLE_INLINE, inner.toByteArray());
    } else if (action instanceof RenameTableAction) {
      RenameTableAction a = (RenameTableAction) action;
      ByteArrayOutputStream inner = new ByteArrayOutputStream();
      writeVarint(inner, RENAME_TBL_ID, a.id);
      writeVarint(inner, RENAME_TBL_VERSION, a.version);
      writeVarint(inner, RENAME_TBL_NEW_NS_ID, a.newNamespaceId);
      writeVarint(inner, RENAME_TBL_NEW_NS_VERSION, a.newNamespaceVersion);
      writeString(inner, RENAME_TBL_NEW_NAME, a.newName);
      writeLengthDelimited(out, ACTION_RENAME_TABLE, inner.toByteArray());
    }
    return out.toByteArray();
  }

  private static Action decodeAction(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int tag = readVarint(in);
    int actionType = tag >>> 3;
    byte[] actionBytes = readLengthDelimitedBytes(in);

    switch (actionType) {
      case ACTION_CREATE_NAMESPACE:
        return decodeCreateNamespace(actionBytes);
      case ACTION_DROP_NAMESPACE:
        return decodeDropNamespace(actionBytes);
      case ACTION_SET_NS_PROPERTY:
        return decodeSetNamespaceProperty(actionBytes);
      case ACTION_REMOVE_NS_PROPERTY:
        return decodeRemoveNamespaceProperty(actionBytes);
      case ACTION_CREATE_TABLE:
        return decodeCreateTable(actionBytes);
      case ACTION_DROP_TABLE:
        return decodeDropTable(actionBytes);
      case ACTION_UPDATE_TABLE_LOCATION:
        return decodeUpdateTableLocation(actionBytes);
      case ACTION_READ_TABLE:
        return decodeReadTable(actionBytes);
      case ACTION_UPDATE_TABLE_INLINE:
        return decodeUpdateTableInline(actionBytes);
      case ACTION_CREATE_TABLE_INLINE:
        return decodeCreateTableInline(actionBytes);
      case ACTION_RENAME_TABLE:
        return decodeRenameTable(actionBytes);
      default:
        return new UnknownAction(actionType, actionBytes);
    }
  }

  private static CreateNamespaceAction decodeCreateNamespace(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int id = 0, version = 0, parentId = 0, parentVersion = 0;
    String name = "";

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fieldNumber = tag >>> 3;
      switch (fieldNumber) {
        case CREATE_NS_ID:
          id = readVarint(in);
          break;
        case CREATE_NS_VERSION:
          version = readVarint(in);
          break;
        case CREATE_NS_PARENT_ID:
          parentId = readVarint(in);
          break;
        case CREATE_NS_PARENT_VERSION:
          parentVersion = readVarint(in);
          break;
        case CREATE_NS_NAME:
          name = readString(in);
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    return new CreateNamespaceAction(id, version, parentId, parentVersion, name);
  }

  private static DropNamespaceAction decodeDropNamespace(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int id = 0, version = 0;

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fieldNumber = tag >>> 3;
      switch (fieldNumber) {
        case DROP_NS_ID:
          id = readVarint(in);
          break;
        case DROP_NS_VERSION:
          version = readVarint(in);
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    return new DropNamespaceAction(id, version);
  }

  private static SetNamespacePropertyAction decodeSetNamespaceProperty(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int nsId = 0, nsVersion = 0;
    String key = "", value = "";

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fieldNumber = tag >>> 3;
      switch (fieldNumber) {
        case SET_NSPROP_NS_ID:
          nsId = readVarint(in);
          break;
        case SET_NSPROP_NS_VERSION:
          nsVersion = readVarint(in);
          break;
        case SET_NSPROP_KEY:
          key = readString(in);
          break;
        case SET_NSPROP_VALUE:
          value = readString(in);
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    return new SetNamespacePropertyAction(nsId, nsVersion, key, value);
  }

  private static RemoveNamespacePropertyAction decodeRemoveNamespaceProperty(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int nsId = 0, nsVersion = 0;
    String key = "";

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fieldNumber = tag >>> 3;
      switch (fieldNumber) {
        case REMOVE_NSPROP_NS_ID:
          nsId = readVarint(in);
          break;
        case REMOVE_NSPROP_NS_VERSION:
          nsVersion = readVarint(in);
          break;
        case REMOVE_NSPROP_KEY:
          key = readString(in);
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    return new RemoveNamespacePropertyAction(nsId, nsVersion, key);
  }

  private static CreateTableAction decodeCreateTable(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int id = 0, version = 0, nsId = 0, nsVersion = 0;
    String name = "", metadataLocation = "";

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fieldNumber = tag >>> 3;
      switch (fieldNumber) {
        case CREATE_TBL_ID:
          id = readVarint(in);
          break;
        case CREATE_TBL_VERSION:
          version = readVarint(in);
          break;
        case CREATE_TBL_NS_ID:
          nsId = readVarint(in);
          break;
        case CREATE_TBL_NS_VERSION:
          nsVersion = readVarint(in);
          break;
        case CREATE_TBL_NAME:
          name = readString(in);
          break;
        case CREATE_TBL_METADATA_LOCATION:
          metadataLocation = readString(in);
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    return new CreateTableAction(id, version, nsId, nsVersion, name, metadataLocation);
  }

  private static DropTableAction decodeDropTable(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int id = 0, version = 0;

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fieldNumber = tag >>> 3;
      switch (fieldNumber) {
        case DROP_TBL_ID:
          id = readVarint(in);
          break;
        case DROP_TBL_VERSION:
          version = readVarint(in);
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    return new DropTableAction(id, version);
  }

  private static UpdateTableLocationAction decodeUpdateTableLocation(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int id = 0, version = 0;
    String metadataLocation = "";

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fieldNumber = tag >>> 3;
      switch (fieldNumber) {
        case UPDATE_TBL_ID:
          id = readVarint(in);
          break;
        case UPDATE_TBL_VERSION:
          version = readVarint(in);
          break;
        case UPDATE_TBL_METADATA_LOCATION:
          metadataLocation = readString(in);
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    return new UpdateTableLocationAction(id, version, metadataLocation);
  }

  private static ReadTableAction decodeReadTable(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int id = 0, version = 0;

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fieldNumber = tag >>> 3;
      switch (fieldNumber) {
        case READ_TBL_ID:
          id = readVarint(in);
          break;
        case READ_TBL_VERSION:
          version = readVarint(in);
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    return new ReadTableAction(id, version);
  }

  private static CreateTableInlineAction decodeCreateTableInline(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int id = 0, version = 0, nsId = 0, nsVersion = 0;
    String name = "";
    byte[] metadata = new byte[0];
    Byte metadataCodec = null;
    byte[] deltaBytes = null;

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fieldNumber = tag >>> 3;
      switch (fieldNumber) {
        case CREATE_INLINE_TBL_ID:
          id = readVarint(in);
          break;
        case CREATE_INLINE_TBL_VERSION:
          version = readVarint(in);
          break;
        case CREATE_INLINE_TBL_NS_ID:
          nsId = readVarint(in);
          break;
        case CREATE_INLINE_TBL_NS_VERSION:
          nsVersion = readVarint(in);
          break;
        case CREATE_INLINE_TBL_NAME:
          name = readString(in);
          break;
        case CREATE_INLINE_TBL_METADATA:
          metadata = readLengthDelimitedBytes(in);
          break;
        case CREATE_INLINE_TBL_METADATA_CODEC:
          metadataCodec = (byte) readVarint(in);
          break;
        case CREATE_INLINE_TBL_DELTA:
          deltaBytes = readLengthDelimitedBytes(in);
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    Preconditions.checkState(
        metadataCodec != null,
        "CreateTableInline action missing required codec tag (table %s)", id);
    return new CreateTableInlineAction(
        id, version, nsId, nsVersion, name, metadata, metadataCodec, deltaBytes);
  }

  private static UpdateTableInlineAction decodeUpdateTableInline(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int id = 0, version = 0;
    byte[] deltaBytes = null;
    byte[] fullMetadata = null;
    String metadataLocation = null;
    Byte fullMetadataCodec = null;
    byte[] mlDeltaBytes = null;

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fieldNumber = tag >>> 3;
      switch (fieldNumber) {
        case UPDATE_INLINE_TBL_ID:
          id = readVarint(in);
          break;
        case UPDATE_INLINE_TBL_VERSION:
          version = readVarint(in);
          break;
        case UPDATE_INLINE_DELTA:
          deltaBytes = readLengthDelimitedBytes(in);
          break;
        case UPDATE_INLINE_FULL_METADATA:
          fullMetadata = readLengthDelimitedBytes(in);
          break;
        case UPDATE_INLINE_METADATA_LOCATION:
          metadataLocation = readString(in);
          break;
        case UPDATE_INLINE_FULL_METADATA_CODEC:
          fullMetadataCodec = (byte) readVarint(in);
          break;
        case UPDATE_INLINE_ML_DELTA:
          mlDeltaBytes = readLengthDelimitedBytes(in);
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    Preconditions.checkState(
        fullMetadata == null || fullMetadataCodec != null,
        "UpdateTableInline action with full_metadata missing required codec tag "
            + "(table %s)", id);
    return new UpdateTableInlineAction(
        id, version, deltaBytes, fullMetadata,
        fullMetadataCodec != null ? fullMetadataCodec : InlineMetadataCodecs.TAG_JSON_GZIP,
        metadataLocation, mlDeltaBytes);
  }

  private static RenameTableAction decodeRenameTable(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int id = 0, version = 0, newNsId = 0, newNsVersion = 0;
    String newName = "";

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fieldNumber = tag >>> 3;
      switch (fieldNumber) {
        case RENAME_TBL_ID:
          id = readVarint(in);
          break;
        case RENAME_TBL_VERSION:
          version = readVarint(in);
          break;
        case RENAME_TBL_NEW_NS_ID:
          newNsId = readVarint(in);
          break;
        case RENAME_TBL_NEW_NS_VERSION:
          newNsVersion = readVarint(in);
          break;
        case RENAME_TBL_NEW_NAME:
          newName = readString(in);
          break;
        default:
          skipField(in, tag & 0x7);
      }
    }
    return new RenameTableAction(id, version, newNsId, newNsVersion, newName);
  }

  // ============================================================
  // Wire format utilities
  // ============================================================

  // ============================================================
  // ManifestFile encode/decode
  // ============================================================

  /**
   * Encodes a ManifestFile into protobuf-compatible bytes matching ManifestFileEntry.
   * The manifest path is stored as a suffix by stripping the given prefix.
   */
  public static byte[] encodeManifestFileEntry(ManifestFile manifest, String pathPrefix)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    String suffix = manifest.path();
    if (pathPrefix != null && !pathPrefix.isEmpty() && suffix.startsWith(pathPrefix)) {
      suffix = suffix.substring(pathPrefix.length());
    }
    writeString(out, MF_PATH_SUFFIX, suffix);
    writeVarint64(out, MF_LENGTH, manifest.length());
    if (manifest.partitionSpecId() != 0) {
      writeVarint(out, MF_SPEC_ID, manifest.partitionSpecId());
    }
    if (manifest.content() != ManifestContent.DATA) {
      writeVarint(out, MF_CONTENT, manifest.content().id());
    }
    if (manifest.sequenceNumber() != 0) {
      writeVarint64(out, MF_SEQ_NUM, manifest.sequenceNumber());
    }
    if (manifest.minSequenceNumber() != 0) {
      writeVarint64(out, MF_MIN_SEQ_NUM, manifest.minSequenceNumber());
    }
    if (manifest.snapshotId() != null) {
      writeFixed64(out, MF_SNAPSHOT_ID, manifest.snapshotId());
    }
    if (manifest.addedFilesCount() != null && manifest.addedFilesCount() != 0) {
      writeVarint(out, MF_ADDED_FILES, manifest.addedFilesCount());
    }
    if (manifest.existingFilesCount() != null && manifest.existingFilesCount() != 0) {
      writeVarint(out, MF_EXISTING_FILES, manifest.existingFilesCount());
    }
    if (manifest.deletedFilesCount() != null && manifest.deletedFilesCount() != 0) {
      writeVarint(out, MF_DELETED_FILES, manifest.deletedFilesCount());
    }
    if (manifest.addedRowsCount() != null && manifest.addedRowsCount() != 0) {
      writeVarint64(out, MF_ADDED_ROWS, manifest.addedRowsCount());
    }
    if (manifest.existingRowsCount() != null && manifest.existingRowsCount() != 0) {
      writeVarint64(out, MF_EXISTING_ROWS, manifest.existingRowsCount());
    }
    if (manifest.deletedRowsCount() != null && manifest.deletedRowsCount() != 0) {
      writeVarint64(out, MF_DELETED_ROWS, manifest.deletedRowsCount());
    }
    if (manifest.partitions() != null) {
      for (PartitionFieldSummary pfs : manifest.partitions()) {
        byte[] pfsBytes = encodePartitionFieldSummary(pfs);
        writeLengthDelimited(out, MF_PARTITIONS, pfsBytes);
      }
    } else {
      // Unlike an empty list (an unpartitioned spec), null means no summaries (a v1 manifest).
      writeVarint(out, MF_NO_PARTITIONS, 1);
    }
    if (manifest.keyMetadata() != null) {
      byte[] km = new byte[manifest.keyMetadata().remaining()];
      manifest.keyMetadata().duplicate().get(km);
      writeBytes(out, MF_KEY_METADATA, km);
    }
    // Present whenever the manifest has one: a v3 table's first data manifest starts at row 0,
    // and an absent first_row_id means its rows have no ids yet.
    if (manifest.firstRowId() != null) {
      writeVarint64(out, MF_FIRST_ROW_ID, manifest.firstRowId());
    }

    return out.toByteArray();
  }

  /**
   * Decodes a ManifestFileEntry from protobuf bytes, prepending the path prefix.
   */
  public static ManifestFile decodeManifestFileEntry(byte[] bytes, String pathPrefix)
      throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    String pathSuffix = "";
    long length = 0;
    int specId = 0;
    int content = 0;
    long seqNum = 0;
    long minSeqNum = 0;
    Long snapshotId = null;
    int addedFiles = 0;
    int existingFiles = 0;
    int deletedFiles = 0;
    long addedRows = 0;
    long existingRows = 0;
    long deletedRows = 0;
    List<PartitionFieldSummary> partitions = new ArrayList<>();
    byte[] keyMetadata = null;
    Long firstRowId = null;
    boolean noPartitions = false;

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fieldNumber = tag >>> 3;
      int wireType = tag & 0x7;
      switch (fieldNumber) {
        case MF_PATH_SUFFIX:
          pathSuffix = readString(in);
          break;
        case MF_LENGTH:
          length = readVarint64(in);
          break;
        case MF_SPEC_ID:
          specId = readVarint(in);
          break;
        case MF_CONTENT:
          content = readVarint(in);
          break;
        case MF_SEQ_NUM:
          seqNum = readVarint64(in);
          break;
        case MF_MIN_SEQ_NUM:
          minSeqNum = readVarint64(in);
          break;
        case MF_SNAPSHOT_ID:
          snapshotId = readFixed64(in);
          break;
        case MF_ADDED_FILES:
          addedFiles = readVarint(in);
          break;
        case MF_EXISTING_FILES:
          existingFiles = readVarint(in);
          break;
        case MF_DELETED_FILES:
          deletedFiles = readVarint(in);
          break;
        case MF_ADDED_ROWS:
          addedRows = readVarint64(in);
          break;
        case MF_EXISTING_ROWS:
          existingRows = readVarint64(in);
          break;
        case MF_DELETED_ROWS:
          deletedRows = readVarint64(in);
          break;
        case MF_PARTITIONS:
          partitions.add(decodePartitionFieldSummary(readLengthDelimitedBytes(in)));
          break;
        case MF_KEY_METADATA:
          keyMetadata = readLengthDelimitedBytes(in);
          break;
        case MF_FIRST_ROW_ID:
          firstRowId = readVarint64(in);
          break;
        case MF_NO_PARTITIONS:
          noPartitions = readVarint(in) != 0;
          break;
        default:
          skipField(in, wireType);
      }
    }

    String fullPath = (pathPrefix != null ? pathPrefix : "") + pathSuffix;
    return new DecodedManifestFile(
        fullPath, length, specId, content, seqNum, minSeqNum, snapshotId,
        addedFiles, existingFiles, deletedFiles, addedRows, existingRows, deletedRows,
        noPartitions ? null : ImmutableList.copyOf(partitions),
        keyMetadata, firstRowId);
  }

  private static byte[] encodePartitionFieldSummary(PartitionFieldSummary pfs) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    if (pfs.containsNull()) {
      writeVarint(out, PFS_CONTAINS_NULL, 1);
    }
    if (pfs.containsNaN() != null && pfs.containsNaN()) {
      writeVarint(out, PFS_CONTAINS_NAN, 1);
    }
    if (pfs.lowerBound() != null) {
      byte[] lb = new byte[pfs.lowerBound().remaining()];
      pfs.lowerBound().duplicate().get(lb);
      writeBytes(out, PFS_LOWER_BOUND, lb);
    }
    if (pfs.upperBound() != null) {
      byte[] ub = new byte[pfs.upperBound().remaining()];
      pfs.upperBound().duplicate().get(ub);
      writeBytes(out, PFS_UPPER_BOUND, ub);
    }
    return out.toByteArray();
  }

  private static PartitionFieldSummary decodePartitionFieldSummary(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    boolean containsNull = false;
    boolean containsNaN = false;
    byte[] lowerBound = null;
    byte[] upperBound = null;

    while (in.available() > 0) {
      int tag = readVarint(in);
      int fieldNumber = tag >>> 3;
      int wireType = tag & 0x7;
      switch (fieldNumber) {
        case PFS_CONTAINS_NULL:
          containsNull = readVarint(in) != 0;
          break;
        case PFS_CONTAINS_NAN:
          containsNaN = readVarint(in) != 0;
          break;
        case PFS_LOWER_BOUND:
          lowerBound = readLengthDelimitedBytes(in);
          break;
        case PFS_UPPER_BOUND:
          upperBound = readLengthDelimitedBytes(in);
          break;
        default:
          skipField(in, wireType);
      }
    }
    // Iceberg's own class: its Avro manifest list writer takes no other.
    return new org.apache.iceberg.GenericPartitionFieldSummary(
        containsNull, containsNaN,
        lowerBound != null ? ByteBuffer.wrap(lowerBound) : null,
        upperBound != null ? ByteBuffer.wrap(upperBound) : null);
  }

  // ============================================================
  // Decoded ManifestFile / PartitionFieldSummary implementations
  // ============================================================

  /**
   * Lightweight ManifestFile for data decoded from protobuf. Holds all fields
   * in memory; no Avro dependency.
   */
  public static class DecodedManifestFile implements ManifestFile {
    private final String path;
    private final long length;
    private final int specId;
    private final ManifestContent content;
    private final long sequenceNumber;
    private final long minSequenceNumber;
    private final Long snapshotId;
    private final Integer addedFilesCount;
    private final Integer existingFilesCount;
    private final Integer deletedFilesCount;
    private final Long addedRowsCount;
    private final Long existingRowsCount;
    private final Long deletedRowsCount;
    private final List<PartitionFieldSummary> partitions;
    private final ByteBuffer keyMetadata;
    private final Long firstRowId;

    DecodedManifestFile(
        String path, long length, int specId, int content,
        long sequenceNumber, long minSequenceNumber, Long snapshotId,
        int addedFilesCount, int existingFilesCount, int deletedFilesCount,
        long addedRowsCount, long existingRowsCount, long deletedRowsCount,
        List<PartitionFieldSummary> partitions, byte[] keyMetadata, Long firstRowId) {
      this.path = path;
      this.length = length;
      this.specId = specId;
      this.content = ManifestContent.values()[content];
      this.sequenceNumber = sequenceNumber;
      this.minSequenceNumber = minSequenceNumber;
      this.snapshotId = snapshotId;
      this.addedFilesCount = addedFilesCount;
      this.existingFilesCount = existingFilesCount;
      this.deletedFilesCount = deletedFilesCount;
      this.addedRowsCount = addedRowsCount;
      this.existingRowsCount = existingRowsCount;
      this.deletedRowsCount = deletedRowsCount;
      this.partitions = partitions;
      this.keyMetadata = keyMetadata != null ? ByteBuffer.wrap(keyMetadata) : null;
      this.firstRowId = firstRowId;
    }

    @Override public String path() { return path; }
    @Override public long length() { return length; }
    @Override public int partitionSpecId() { return specId; }
    @Override public ManifestContent content() { return content; }
    @Override public long sequenceNumber() { return sequenceNumber; }
    @Override public long minSequenceNumber() { return minSequenceNumber; }
    @Override public Long snapshotId() { return snapshotId; }
    @Override public Integer addedFilesCount() { return addedFilesCount; }
    @Override public Integer existingFilesCount() { return existingFilesCount; }
    @Override public Integer deletedFilesCount() { return deletedFilesCount; }
    @Override public Long addedRowsCount() { return addedRowsCount; }
    @Override public Long existingRowsCount() { return existingRowsCount; }
    @Override public Long deletedRowsCount() { return deletedRowsCount; }
    @Override public List<PartitionFieldSummary> partitions() { return partitions; }
    @Override public ByteBuffer keyMetadata() { return keyMetadata; }
    @Override public Long firstRowId() { return firstRowId; }
    @Override public ManifestFile copy() { return this; }
  }

  // ============================================================
  // Transaction class
  // ============================================================

  /**
   * A transaction: its id, which the log's committed set dedups, and its intention, the actions
   * replay verifies and applies.
   */
  public static class Transaction {
    private final UUID id;
    private final CatalogIntention intention;

    public Transaction(UUID id, List<Action> actions) {
      this(id, new CatalogIntention(actions));
    }

    public Transaction(UUID id, CatalogIntention intention) {
      this.id = id;
      this.intention = intention;
    }

    public static Transaction from(
        CatalogFile.Mut<ProtoCatalogFile, ?> mut,
        ProtoCatalogFormat.ProtoIdManager idManager) {
      // After the horizon of the catalog this writer read, where replay rejects a record (Bug 64).
      UUID txnId = UuidV7.newUuidV7After(mut.original.highestDroppedTimestampMs());
      List<Action> actions = new ArrayList<>();

      // Build actions from mutations accumulated in mut
      // This delegates to the Mut to extract its pending changes
      if (mut instanceof ProtoCatalogFormat.Mut) {
        ((ProtoCatalogFormat.Mut) mut).buildActions(actions, idManager);
      }

      return new Transaction(txnId, actions);
    }

    public UUID id() {
      return id;
    }

    public CatalogIntention intention() {
      return intention;
    }

    public List<Action> actions() {
      return intention.actions();
    }

    /**
     * Verifies that all actions in this transaction can be applied. A
     * non-UUIDv7 transaction id fails verification: the dedup set only
     * tolerates v7 ids (the encoder partitions on this), so accepting a
     * non-v7 record from the log would put a value in the committed-set
     * that the next checkpoint can't represent. Skip silently — same
     * read-time behavior as any other rejected transaction.
     */
    public boolean verify(ProtoCatalogFile.Builder builder) {
      return UuidV7.isV7(id) && intention.verify(builder);
    }

    /** Applies this transaction's intention (see {@link CatalogIntention#apply}). */
    public void apply(ProtoCatalogFile.Builder builder) {
      intention.apply(builder);
    }
  }

  /**
   * The catalog's intention: a transaction's actions. Each names the versions it depends on; replay
   * applies the whole intention where its record lands if every action's preconditions hold there,
   * binding the transaction's late-bound ids to real ones.
   */
  public static class CatalogIntention implements Intention<ProtoCatalogFile.Builder> {
    private final List<Action> actions;

    public CatalogIntention(List<Action> actions) {
      this.actions = actions;
    }

    public List<Action> actions() {
      return actions;
    }

    /**
     * Whether every action's preconditions hold on the state before the intention: each late-bound
     * reference follows the create that defines it, no action depends on what another drops, and
     * each action verifies.
     */
    @Override
    public boolean verify(ProtoCatalogFile.Builder builder) {
      // Every late-bound reference must follow the create that defines it;
      // otherwise apply could not bind it. Reject the record, don't fail the read.
      IdBinding ids = new IdBinding(builder);
      List<Action> bound = new ArrayList<>(actions.size());
      try {
        for (Action action : actions) {
          bound.add(action.bind(ids));
        }
      } catch (IllegalStateException e) {
        return false;
      }
      if (!selfConsistent(bound)) {
        return false;
      }
      for (Action action : actions) {
        if (!action.verify(builder)) {
          return false;
        }
      }
      return true;
    }

    /**
     * Whether no action references a namespace or table that another action of
     * this transaction drops. Every action verifies against the state before
     * the transaction, so such a record would pass verify and then apply to an
     * inconsistent state (a namespace or table whose parent is gone) that no
     * read could build. Reject the record, don't fail the read.
     */
    private static boolean selfConsistent(List<Action> actions) {
      Set<Integer> droppedNamespaces = new HashSet<>();
      Set<Integer> droppedTables = new HashSet<>();
      for (Action action : actions) {
        if (action instanceof DropNamespaceAction) {
          droppedNamespaces.add(((DropNamespaceAction) action).id);
        } else if (action instanceof DropTableAction) {
          droppedTables.add(((DropTableAction) action).id);
        }
      }
      if (droppedNamespaces.isEmpty() && droppedTables.isEmpty()) {
        return true;
      }
      for (Action action : actions) {
        final int namespace;
        final int table;
        if (action instanceof CreateNamespaceAction) {
          namespace = ((CreateNamespaceAction) action).parentId;
          table = -1;
        } else if (action instanceof SetNamespacePropertyAction) {
          namespace = ((SetNamespacePropertyAction) action).namespaceId;
          table = -1;
        } else if (action instanceof RemoveNamespacePropertyAction) {
          namespace = ((RemoveNamespacePropertyAction) action).namespaceId;
          table = -1;
        } else if (action instanceof CreateTableAction) {
          namespace = ((CreateTableAction) action).namespaceId;
          table = -1;
        } else if (action instanceof CreateTableInlineAction) {
          namespace = ((CreateTableInlineAction) action).namespaceId;
          table = -1;
        } else if (action instanceof RenameTableAction) {
          namespace = ((RenameTableAction) action).newNamespaceId;
          table = ((RenameTableAction) action).id;
        } else if (action instanceof UpdateTableLocationAction) {
          namespace = -1;
          table = ((UpdateTableLocationAction) action).id;
        } else if (action instanceof UpdateTableInlineAction) {
          namespace = -1;
          table = ((UpdateTableInlineAction) action).id;
        } else if (action instanceof ReadTableAction) {
          namespace = -1;
          table = ((ReadTableAction) action).id;
        } else {
          continue;
        }
        if (droppedNamespaces.contains(namespace) || droppedTables.contains(table)) {
          return false;
        }
      }
      return true;
    }

    /**
     * Applies all actions to the builder, binding late-bound ids to real ids as their creates are
     * applied.
     */
    @Override
    public void apply(ProtoCatalogFile.Builder builder) {
      IdBinding ids = new IdBinding(builder);
      for (Action action : actions) {
        action.bind(ids).apply(builder);
      }
    }
  }

  /**
   * Binds one transaction's late-bound (negative) namespace and table ids to
   * real ids at replay time. A create action defines a late-bound id; later
   * actions in the same transaction reference it.
   *
   * <p>Real ids come from the replaying builder's counters, so every reader of
   * the log assigns the same ids, and two writers that read the same snapshot
   * cannot claim the same id. Writers that bound ids from their own snapshot
   * did exactly that: two unrelated creates carried one id, and replay let the
   * second overwrite the first.
   */
  static final class IdBinding {
    private final ProtoCatalogFile.Builder builder;
    private final Map<Integer, Integer> namespaces = new HashMap<>();
    private final Map<Integer, Integer> tables = new HashMap<>();
    private int nextNamespaceId;
    private int nextTableId;

    IdBinding(ProtoCatalogFile.Builder builder) {
      this.builder = builder;
      this.nextNamespaceId = builder.nextNamespaceId();
      this.nextTableId = builder.nextTableId();
    }

    /**
     * Binds a namespace id at its defining create. A create must name its namespace by a
     * late-bound (negative) id: one with a concrete id could bring back an id a drop retired, at
     * version 1, and a stale record whose preconditions failed before the drop would then hold
     * again. Replay rejects such a record (verify fails), so a rejection is stable.
     */
    int defineNamespace(int id) {
      if (id >= 0) {
        throw new IllegalStateException("A create names a concrete namespace id: " + id);
      }
      int real = Math.max(nextNamespaceId, builder.nextNamespaceId());
      nextNamespaceId = real + 1;
      bind(namespaces, id, real);
      return real;
    }

    /** Resolves a namespace reference. Concrete ids (including the root, 0) pass through. */
    int namespace(int id) {
      return id >= 0 ? id : resolve(namespaces, id, "namespace");
    }

    /** Binds a table id at its defining create, which must be late-bound (as a namespace's). */
    int defineTable(int id) {
      if (id >= 0) {
        throw new IllegalStateException("A create names a concrete table id: " + id);
      }
      int real = Math.max(nextTableId, builder.nextTableId());
      nextTableId = real + 1;
      bind(tables, id, real);
      return real;
    }

    private static void bind(Map<Integer, Integer> bound, int lateId, int real) {
      if (bound.putIfAbsent(lateId, real) != null) {
        throw new IllegalStateException("Late-bound id defined twice: " + lateId);
      }
    }

    private static int resolve(Map<Integer, Integer> bound, int lateId, String kind) {
      Integer real = bound.get(lateId);
      if (real == null) {
        throw new IllegalStateException("Unbound late-bound " + kind + " id: " + lateId);
      }
      return real;
    }
  }

  // ============================================================
  // Action types
  // ============================================================

  public interface Action {
    boolean verify(ProtoCatalogFile.Builder builder);
    void apply(ProtoCatalogFile.Builder builder);

    /**
     * Returns this action with its late-bound ids bound to real ids. Actions
     * that only ever carry concrete ids return themselves.
     */
    default Action bind(IdBinding ids) {
      return this;
    }
  }

  public static class CreateNamespaceAction implements Action {
    final int id;
    final int version;
    final int parentId;
    final int parentVersion;
    final String name;

    public CreateNamespaceAction(int id, int version, int parentId, int parentVersion, String name) {
      this.id = id;
      this.version = version;
      this.parentId = parentId;
      this.parentVersion = parentVersion;
      this.name = name;
    }

    @Override
    public boolean verify(ProtoCatalogFile.Builder builder) {
      if (id == ProtoCatalogFile.ROOT_ID) {
        return false;  // the root row exists in every catalog; never replace it
      }
      // Parent version must match (skipped when the parent is late-bound).
      // The root is versioned like any other parent.
      int currentParentVersion = builder.namespaceVersion(parentId);
      if (parentVersion >= 0 && currentParentVersion != parentVersion) {
        return false;
      }
      return true;
    }

    @Override
    public Action bind(IdBinding ids) {
      // Resolve the parent first so a record naming itself as parent is unbound.
      int boundParentId = ids.namespace(parentId);
      return new CreateNamespaceAction(
          ids.defineNamespace(id), version, boundParentId, parentVersion, name);
    }

    @Override
    public void apply(ProtoCatalogFile.Builder builder) {
      builder.addNamespace(id, parentId, name, version);
      // Creating a child namespace mutates the parent's children set; bump so
      // concurrent mutations of the parent (create sibling, drop parent, set
      // property) fail verify if they captured the pre-bump version.
      builder.bumpNamespaceVersion(parentId);
    }
  }

  public static class DropNamespaceAction implements Action {
    final int id;
    final int version;

    public DropNamespaceAction(int id, int version) {
      this.id = id;
      this.version = version;
    }

    @Override
    public boolean verify(ProtoCatalogFile.Builder builder) {
      if (id == ProtoCatalogFile.ROOT_ID) {
        return false;  // the root is never dropped
      }
      int currentVersion = builder.namespaceVersion(id);
      return currentVersion == version;
    }

    @Override
    public void apply(ProtoCatalogFile.Builder builder) {
      // Capture parent before removing so we can bump the parent's version
      // (dropping a child mutates the parent's children set).
      ProtoCatalogFile.NsEntry entry = builder.namespaceEntry(id);
      int parentId = entry != null ? entry.parentId : 0;
      builder.removeNamespace(id);
      builder.bumpNamespaceVersion(parentId);
    }
  }

  public static class SetNamespacePropertyAction implements Action {
    final int namespaceId;
    final int namespaceVersion;
    final String key;
    final String value;

    public SetNamespacePropertyAction(int namespaceId, int namespaceVersion, String key, String value) {
      this.namespaceId = namespaceId;
      this.namespaceVersion = namespaceVersion;
      this.key = key;
      this.value = value;
    }

    @Override
    public boolean verify(ProtoCatalogFile.Builder builder) {
      if (namespaceVersion >= 0) {
        int currentVersion = builder.namespaceVersion(namespaceId);
        return currentVersion == namespaceVersion;
      }
      return true; // -1 means namespace was created in same transaction
    }

    @Override
    public Action bind(IdBinding ids) {
      return new SetNamespacePropertyAction(ids.namespace(namespaceId), namespaceVersion, key, value);
    }

    @Override
    public void apply(ProtoCatalogFile.Builder builder) {
      builder.setNamespaceProperty(namespaceId, key, value);
      builder.bumpNamespaceVersion(namespaceId);
    }
  }

  public static class RemoveNamespacePropertyAction implements Action {
    final int namespaceId;
    final int namespaceVersion;
    final String key;

    public RemoveNamespacePropertyAction(int namespaceId, int namespaceVersion, String key) {
      this.namespaceId = namespaceId;
      this.namespaceVersion = namespaceVersion;
      this.key = key;
    }

    @Override
    public boolean verify(ProtoCatalogFile.Builder builder) {
      if (namespaceVersion >= 0) {
        int currentVersion = builder.namespaceVersion(namespaceId);
        return currentVersion == namespaceVersion;
      }
      return true;
    }

    @Override
    public Action bind(IdBinding ids) {
      return new RemoveNamespacePropertyAction(ids.namespace(namespaceId), namespaceVersion, key);
    }

    @Override
    public void apply(ProtoCatalogFile.Builder builder) {
      builder.removeNamespaceProperty(namespaceId, key);
      builder.bumpNamespaceVersion(namespaceId);
    }
  }

  public static class CreateTableAction implements Action {
    final int id;
    final int version;
    final int namespaceId;
    final int namespaceVersion;
    final String name;
    final String metadataLocation;

    public CreateTableAction(int id, int version, int namespaceId, int namespaceVersion,
                             String name, String metadataLocation) {
      this.id = id;
      this.version = version;
      this.namespaceId = namespaceId;
      this.namespaceVersion = namespaceVersion;
      this.name = name;
      this.metadataLocation = metadataLocation;
    }

    @Override
    public boolean verify(ProtoCatalogFile.Builder builder) {
      if (namespaceVersion >= 0) {
        int currentNsVersion = builder.namespaceVersion(namespaceId);
        if (currentNsVersion != namespaceVersion) {
          return false;
        }
      }
      return true;
    }

    @Override
    public Action bind(IdBinding ids) {
      int boundNamespaceId = ids.namespace(namespaceId);
      return new CreateTableAction(
          ids.defineTable(id), version, boundNamespaceId, namespaceVersion, name, metadataLocation);
    }

    @Override
    public void apply(ProtoCatalogFile.Builder builder) {
      builder.addTable(id, namespaceId, name, version, metadataLocation);
      // Adding a table mutates the parent's children set; bump so concurrent
      // ns mutations (drop, set-property, create sibling table) fail verify.
      builder.bumpNamespaceVersion(namespaceId);
    }
  }

  public static class DropTableAction implements Action {
    final int id;
    final int version;

    public DropTableAction(int id, int version) {
      this.id = id;
      this.version = version;
    }

    @Override
    public boolean verify(ProtoCatalogFile.Builder builder) {
      int currentVersion = builder.tableVersion(id);
      return currentVersion == version;
    }

    @Override
    public void apply(ProtoCatalogFile.Builder builder) {
      // Capture parent before removing so we can bump the parent's version
      // (dropping a table mutates the parent's children set).
      ProtoCatalogFile.TblEntry entry = builder.tableEntry(id);
      int parentNsId = entry != null ? entry.namespaceId : 0;
      builder.removeTable(id);
      builder.bumpNamespaceVersion(parentNsId);
    }
  }

  public static class UpdateTableLocationAction implements Action {
    final int id;
    final int version;
    final String metadataLocation;

    public UpdateTableLocationAction(int id, int version, String metadataLocation) {
      this.id = id;
      this.version = version;
      this.metadataLocation = metadataLocation;
    }

    @Override
    public boolean verify(ProtoCatalogFile.Builder builder) {
      int currentVersion = builder.tableVersion(id);
      return currentVersion == version;
    }

    @Override
    public void apply(ProtoCatalogFile.Builder builder) {
      // A table is a pointer table or an inline table, never both: the metadata file is now the
      // table's whole state (its writer gave every snapshot a manifest list file). Inline state
      // left behind would be read again, by an inline writer's delta and by the next checkpoint,
      // which encodes an inline table without its location, losing this commit.
      builder.removeInlineMetadata(id);
      builder.updateTableLocation(id, version + 1, metadataLocation);
    }
  }

  public static class ReadTableAction implements Action {
    final int id;
    final int version;

    public ReadTableAction(int id, int version) {
      this.id = id;
      this.version = version;
    }

    @Override
    public boolean verify(ProtoCatalogFile.Builder builder) {
      // Fails if the table changed since the transaction read it (a serializable read)
      int currentVersion = builder.tableVersion(id);
      return currentVersion == version;
    }

    @Override
    public void apply(ProtoCatalogFile.Builder builder) {
      // Read actions don't modify state
    }
  }

  /**
   * Updates an inline table's metadata. Supports three modes:
   * - Delta: applies structured changes (deltaBytes != null)
   * - Full: replaces entire metadata (fullMetadata != null)
   * - Pointer: evicts to external file (metadataLocation != null)
   * Exactly one of deltaBytes, fullMetadata, or metadataLocation should be non-null.
   */
  public static class UpdateTableInlineAction implements Action {
    final int id;
    final int version;
    final byte[] deltaBytes;         // non-null for DELTA mode
    final byte[] fullMetadata;       // non-null for FULL mode
    /** Codec for {@link #fullMetadata}. Ignored for DELTA / POINTER modes. */
    final byte fullMetadataCodec;
    final String metadataLocation;   // non-null for POINTER mode
    /**
     * Optional ML pool delta bundled with FULL mode. Non-null only when
     * {@link #fullMetadata} is non-null and the commit had staged manifest-list
     * updates that need to land atomically with the TM replacement. See
     * {@link #full(int, int, byte[], byte, byte[])}.
     */
    final byte[] mlDeltaBytes;

    public UpdateTableInlineAction(
        int id, int version, byte[] deltaBytes, byte[] fullMetadata,
        byte fullMetadataCodec, String metadataLocation) {
      this(id, version, deltaBytes, fullMetadata, fullMetadataCodec, metadataLocation, null);
    }

    public UpdateTableInlineAction(
        int id, int version, byte[] deltaBytes, byte[] fullMetadata,
        byte fullMetadataCodec, String metadataLocation, byte[] mlDeltaBytes) {
      this.id = id;
      this.version = version;
      this.deltaBytes = deltaBytes;
      this.fullMetadata = fullMetadata;
      this.fullMetadataCodec = fullMetadataCodec;
      this.metadataLocation = metadataLocation;
      this.mlDeltaBytes = mlDeltaBytes;
    }

    /** Creates an UpdateTableInlineAction in DELTA mode (codec inherited from base). */
    public static UpdateTableInlineAction delta(int id, int version, byte[] deltaBytes) {
      return new UpdateTableInlineAction(id, version, deltaBytes, null, (byte) 0, null);
    }

    /** Creates an UpdateTableInlineAction in FULL mode with an explicit codec. */
    public static UpdateTableInlineAction full(
        int id, int version, byte[] fullMetadata, byte fullMetadataCodec) {
      return new UpdateTableInlineAction(
          id, version, null, fullMetadata, fullMetadataCodec, null);
    }

    /**
     * Creates an UpdateTableInlineAction in FULL mode that also carries an ML
     * pool delta. Used when the commit goes in full (re-inlining, a delta too large,
     * or one delta encoding cannot express) and has manifest-list updates that must
     * land in the per-table pool atomically with the TM replacement.
     */
    public static UpdateTableInlineAction full(
        int id, int version, byte[] fullMetadata, byte fullMetadataCodec, byte[] mlDeltaBytes) {
      return new UpdateTableInlineAction(
          id, version, null, fullMetadata, fullMetadataCodec, null, mlDeltaBytes);
    }

    /** Creates an UpdateTableInlineAction in POINTER mode (codec field unused). */
    public static UpdateTableInlineAction pointer(int id, int version, String metadataLocation) {
      return new UpdateTableInlineAction(id, version, null, null, (byte) 0, metadataLocation);
    }

    @Override
    public boolean verify(ProtoCatalogFile.Builder builder) {
      int currentVersion = builder.tableVersion(id);
      return currentVersion == version;
    }

    @Override
    public void apply(ProtoCatalogFile.Builder builder) {
      if (deltaBytes != null) {
        // DELTA mode: apply structured changes to current inline metadata.
        // Decode base, apply delta, re-encode with the SAME codec the base
        // used (delta mode preserves the on-disk codec — codec changes only
        // happen via FULL re-inline).
        byte[] currentMeta = builder.inlineMetadata(id);
        if (currentMeta != null) {
          byte baseCodecTag = builder.inlineMetadataCodec(id);
          InlineMetadataCodec baseCodec = InlineMetadataCodecs.byTag(baseCodecTag);
          // Apply delta — routes ML updates to builder's manifest pool
          byte[] updatedMeta = InlineDeltaCodec.applyDeltaWithManifests(
              currentMeta, baseCodec, deltaBytes, builder, id);
          // Rotate the inline metadata without clearing manifest pool/refs.
          ProtoCatalogFile.TblEntry old = builder.tableEntry(id);
          if (old != null) {
            builder.updateInlineMetadata(id, version + 1, updatedMeta, baseCodecTag);
          }
        }
      } else if (fullMetadata != null) {
        // FULL mode: replace inline metadata bytes in place, preserving the
        // manifest pool. `removeInlineMetadata` (used by the POINTER branch
        // below) also clears the pool — that's the correct behavior for
        // pointer-mode eviction, but would silently corrupt an ML-populated
        // table on a non-ML commit (schema evolution, etc.).
        ProtoCatalogFile.TblEntry old = builder.tableEntry(id);
        if (old != null) {
          builder.updateInlineMetadata(id, version + 1, fullMetadata, fullMetadataCodec);
          // Apply bundled ML pool delta first (if any), so any AddManifest
          // entries for new snapshots populate the pool before we reconcile
          // refs against the new TM. A full-mode commit carries one whenever it
          // adds an inline-ML snapshot or names one the table has no list for,
          // so the lists land atomically with the TM replacement.
          // applyManifestsOnly finds each snapshot's parent in the new TM bytes
          // and ignores any table metadata update, which the new TM holds.
          if (mlDeltaBytes != null) {
            InlineMetadataCodec codec = InlineMetadataCodecs.byTag(fullMetadataCodec);
            InlineDeltaCodec.applyManifestsOnly(mlDeltaBytes, builder, id, codec, fullMetadata);
          }
          // Reconcile snapshot manifest refs (and orphan'd pool entries)
          // against the snapshots present in the new TM. Delta-mode replay
          // handles this via RemoveSnapshotsUpdate (in InlineDeltaCodec
          // applyDeltaWithManifests); full-mode replay swaps the entire TM bytes in one
          // shot, so we re-derive the live snapshot set from the new bytes
          // and prune anything that's no longer referenced. Without this,
          // a full-mode commit that drops a snapshot (e.g. expireSnapshots
          // co-committed with a statistics change that flips computeDelta
          // to null) leaves orphan refs that bloat the catalog file and
          // reference storage paths storage GC may later delete.
          //
          // Only ML-populated tables have refs to reconcile; skip the decode
          // for TM-only inline tables (no pool entries to prune anyway).
          Set<Long> existingRefs = builder.snapshotManifestKeys(id);
          if (!existingRefs.isEmpty()) {
            InlineMetadataCodec codec = InlineMetadataCodecs.byTag(fullMetadataCodec);
            TableMetadata newMeta = codec.decodeFull(fullMetadata, "inline://reconcile");
            Set<Long> liveSnapIds = new HashSet<>();
            for (Snapshot s : newMeta.snapshots()) {
              liveSnapIds.add(s.snapshotId());
            }
            for (Long staleId : existingRefs) {
              if (!liveSnapIds.contains(staleId)) {
                builder.removeSnapshotManifests(id, staleId);
              }
            }
          }
        }
      } else if (metadataLocation != null) {
        // POINTER mode: evict from inline to pointer
        builder.removeInlineMetadata(id);
        builder.updateTableLocation(id, version + 1, metadataLocation);
      }
    }
  }

  /**
   * Creates an inline table (metadata stored in catalog, not external file).
   * Verification: namespace version must match (same as CreateTable).
   */
  public static class CreateTableInlineAction implements Action {
    final int id;
    final int version;
    final int namespaceId;
    final int namespaceVersion;
    final String name;
    final byte[] metadata;
    /** Codec used to encode {@link #metadata}. */
    final byte metadataCodec;
    /**
     * Optional initial manifest-pool delta. Set when an inline-ML
     * create-transaction commits with one or more staged snapshots — apply()
     * routes these entries through {@link InlineDeltaCodec#applyManifestsOnly}
     * so the per-table manifest pool and per-snapshot ref list are populated
     * atomically with the table create. {@code null} for non-inline-ML
     * creates and for inline-ML creates with no snapshots.
     */
    final byte[] deltaBytes;

    public CreateTableInlineAction(
        int id, int version, int namespaceId, int namespaceVersion,
        String name, byte[] metadata, byte metadataCodec) {
      this(id, version, namespaceId, namespaceVersion, name, metadata,
          metadataCodec, null);
    }

    public CreateTableInlineAction(
        int id, int version, int namespaceId, int namespaceVersion,
        String name, byte[] metadata, byte metadataCodec, byte[] deltaBytes) {
      this.id = id;
      this.version = version;
      this.namespaceId = namespaceId;
      this.namespaceVersion = namespaceVersion;
      this.name = name;
      this.metadata = metadata;
      this.metadataCodec = metadataCodec;
      this.deltaBytes = deltaBytes;
    }

    @Override
    public boolean verify(ProtoCatalogFile.Builder builder) {
      if (namespaceVersion >= 0) {
        int currentNsVersion = builder.namespaceVersion(namespaceId);
        if (currentNsVersion != namespaceVersion) {
          return false;
        }
      }
      return true;
    }

    @Override
    public Action bind(IdBinding ids) {
      int boundNamespaceId = ids.namespace(namespaceId);
      return new CreateTableInlineAction(
          ids.defineTable(id), version, boundNamespaceId, namespaceVersion, name, metadata,
          metadataCodec, deltaBytes);
    }

    @Override
    public void apply(ProtoCatalogFile.Builder builder) {
      // The manifest list prefix is always empty: nothing sets one, and a
      // checkpoint copies it.
      builder.addInlineTable(id, namespaceId, name, version, metadata, metadataCodec, "");
      if (deltaBytes != null) {
        // Inline-ML create: land the manifest lists of the create transaction's snapshots,
        // the way a full-mode update does. Decodes the just-stored metadata so each snapshot's
        // list builds on its parent's, and registers every inline snapshot, including one
        // with no manifest updates of its own.
        InlineDeltaCodec.applyManifestsOnly(
            deltaBytes, builder, id, InlineMetadataCodecs.byTag(metadataCodec), metadata);
      }
      // Adding a table mutates the parent's children set; bump so concurrent
      // ns mutations fail verify. Same rule as CreateTableAction.
      builder.bumpNamespaceVersion(namespaceId);
    }
  }

  /**
   * Renames a table in place: the table id stays put, only the
   * {@code (namespace, name)} pair changes. Inline state (manifest pool, snapshot
   * manifest refs, inline TM bytes) is id-keyed and follows by construction.
   *
   * <p>Verification: source {@code tbl_v} must match (catches concurrent drop /
   * update / rename of the same id). When {@code newNamespaceVersion >= 0}, the
   * destination namespace's version must match — caller passes {@code -1} to
   * skip the check when the destination ns was created in this same transaction
   * (late-bind), mirroring {@link CreateTableAction}.
   *
   * <p>Apply bumps the source and destination namespaces' children-set version,
   * matching the rule for {@link CreateTableAction} / {@link DropTableAction}.
   * See SPEC_TM.md "Rename" and design.md "Operation × Operation Conflict Matrix".
   */
  public static class RenameTableAction implements Action {
    final int id;
    final int version;
    final int newNamespaceId;
    final int newNamespaceVersion;
    final String newName;

    public RenameTableAction(
        int id, int version, int newNamespaceId, int newNamespaceVersion, String newName) {
      this.id = id;
      this.version = version;
      this.newNamespaceId = newNamespaceId;
      this.newNamespaceVersion = newNamespaceVersion;
      this.newName = newName;
    }

    @Override
    public boolean verify(ProtoCatalogFile.Builder builder) {
      // Source tbl_v must match. Catches concurrent drop / update / rename of
      // the same id.
      int currentVersion = builder.tableVersion(id);
      if (currentVersion != version) {
        return false;
      }
      // Destination ns_v must match (when not late-bound). Catches a concurrent
      // CreateTable / DropTable / RenameTable into the same destination ns —
      // any of those bumps the parent ns's children-set version.
      if (newNamespaceVersion >= 0) {
        int currentNsVersion = builder.namespaceVersion(newNamespaceId);
        if (currentNsVersion != newNamespaceVersion) {
          return false;
        }
      }
      return true;
    }

    @Override
    public Action bind(IdBinding ids) {
      return new RenameTableAction(
          id, version, ids.namespace(newNamespaceId), newNamespaceVersion, newName);
    }

    @Override
    public void apply(ProtoCatalogFile.Builder builder) {
      ProtoCatalogFile.TblEntry old = builder.tableEntry(id);
      int oldNsId = old != null ? old.namespaceId : 0;
      builder.renameTable(id, newNamespaceId, newName, version + 1);
      // Renaming mutates the source ns's children set (table left) and the
      // destination ns's children set (table arrived). Bump both, exactly like
      // DropTable + CreateTable would have.
      builder.bumpNamespaceVersion(oldNsId);
      if (newNamespaceId != oldNsId) {
        builder.bumpNamespaceVersion(newNamespaceId);
      }
    }
  }

  /**
   * Represents an unknown action type for forward compatibility.
   * Unknown actions are preserved but do not verify or apply.
   */
  public static class UnknownAction implements Action {
    final int actionType;
    final byte[] bytes;

    public UnknownAction(int actionType, byte[] bytes) {
      this.actionType = actionType;
      this.bytes = bytes;
    }

    @Override
    public boolean verify(ProtoCatalogFile.Builder builder) {
      // Unknown actions always verify (forward compatibility)
      return true;
    }

    @Override
    public void apply(ProtoCatalogFile.Builder builder) {
      // Unknown actions don't modify state
    }
  }
}
