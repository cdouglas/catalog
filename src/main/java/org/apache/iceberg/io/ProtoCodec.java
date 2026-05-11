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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFile.PartitionFieldSummary;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

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

  // Wire types
  private static final int WIRE_VARINT = 0;
  private static final int WIRE_64BIT = 1;
  private static final int WIRE_LENGTH_DELIMITED = 2;

  // Checkpoint field numbers (from catalog.proto)
  private static final int CHECKPOINT_CATALOG_UUID = 1;
  private static final int CHECKPOINT_NEXT_NAMESPACE_ID = 2;
  private static final int CHECKPOINT_NEXT_TABLE_ID = 3;
  private static final int CHECKPOINT_NAMESPACES = 10;
  private static final int CHECKPOINT_TABLES = 11;
  private static final int CHECKPOINT_NAMESPACE_PROPERTIES = 12;
  private static final int CHECKPOINT_INLINE_TABLES = 13;
  private static final int CHECKPOINT_COMMITTED_TXNS = 20;

  // CommittedTransactions field numbers (compressed UUIDv7 set).
  // Entries are interleaved (varint delta + 10 random bytes per entry,
  // back-to-back) into a single packed_entries blob so the scanner can walk
  // the wire bytes in one linear pass without allocating intermediates.
  private static final int COMMITTED_TXNS_MAX_TIMESTAMP_MS = 1;
  private static final int COMMITTED_TXNS_PACKED_ENTRIES = 2;
  private static final int COMMITTED_TXNS_HIGHEST_DROPPED_TIMESTAMP_MS = 3;

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

  // PartitionFieldSummaryEntry field numbers
  private static final int PFS_CONTAINS_NULL = 1;
  private static final int PFS_CONTAINS_NAN = 2;
  private static final int PFS_LOWER_BOUND = 3;
  private static final int PFS_UPPER_BOUND = 4;

  // SnapshotManifestRefs field numbers
  private static final int SMR_SNAPSHOT_ID = 1;
  private static final int SMR_POOL_INDICES = 2;

  // Transaction field numbers
  private static final int TXN_ID = 1;
  private static final int TXN_ACTIONS = 3;

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

  // CreateTableInline field numbers
  private static final int CREATE_INLINE_TBL_ID = 1;
  private static final int CREATE_INLINE_TBL_VERSION = 2;
  private static final int CREATE_INLINE_TBL_NS_ID = 3;
  private static final int CREATE_INLINE_TBL_NS_VERSION = 4;
  private static final int CREATE_INLINE_TBL_NAME = 5;
  private static final int CREATE_INLINE_TBL_METADATA = 6;
  private static final int CREATE_INLINE_TBL_METADATA_CODEC = 7;
  // Optional delta bytes carrying initial manifest-pool entries for an
  // inline-ML create-with-snapshots commit. See errata D9.
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
   * Encodes a checkpoint from the original catalog state plus pending mutations,
   * using {@code dropBeforeTimestampMs = 0} (no committed-set GC).
   */
  public static byte[] encodeCheckpoint(
      ProtoCatalogFile original,
      CatalogFile.Mut<ProtoCatalogFile, ?> mut,
      ProtoCatalogFormat.ProtoIdManager idManager) {
    return encodeCheckpoint(original, mut, idManager, 0L);
  }

  /**
   * Encodes a checkpoint, optionally GC-ing the inherited committed-transaction
   * set: any UUIDv7 with {@code timestamp_ms <= dropBeforeTimestampMs} is
   * dropped from the encoded body and contributes to
   * {@code highest_dropped_timestamp_ms}. Called from the CAS / compaction path
   * in {@link ProtoCatalogFormat.Mut} where {@code dropBeforeTimestampMs} is
   * computed as {@code now - committedRetentionMs}.
   */
  public static byte[] encodeCheckpoint(
      ProtoCatalogFile original,
      CatalogFile.Mut<ProtoCatalogFile, ?> mut,
      ProtoCatalogFormat.ProtoIdManager idManager,
      long dropBeforeTimestampMs) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();

      // Catalog UUID
      writeBytes(out, CHECKPOINT_CATALOG_UUID, uuidToBytes(original.uuid()));

      // Next IDs (from idManager which tracks allocations)
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

      // Committed transactions (compressed UUIDv7 set, see CommittedTransactions
      // proto). The catalog file stores the inner-message bytes directly. When
      // dropBeforeTimestampMs > 0, this is also the GC site: rewriteCommittedBytes
      // drops inherited entries whose v7 timestamp is below the watermark and
      // updates highest_dropped_timestamp_ms accordingly. Empty set with no
      // horizon is omitted for wire-byte parity with the prior encoding.
      byte[] inheritedBytes = original.committedTransactionsBytes();
      byte[] committedBytes =
          dropBeforeTimestampMs > 0L
              ? rewriteCommittedBytes(
                  inheritedBytes, java.util.Collections.emptyList(), dropBeforeTimestampMs)
              : inheritedBytes;
      if (committedBytes.length > 0) {
        writeLengthDelimited(out, CHECKPOINT_COMMITTED_TXNS, committedBytes);
      }

      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Decodes a checkpoint into a builder.
   */
  public static void decodeCheckpoint(byte[] bytes, ProtoCatalogFile.Builder builder) {
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(bytes);
      while (in.available() > 0) {
        int tag = readVarint(in);
        int fieldNumber = tag >>> 3;
        int wireType = tag & 0x7;

        switch (fieldNumber) {
          case CHECKPOINT_CATALOG_UUID:
            builder.setCatalogUuid(bytesToUuid(readLengthDelimitedBytes(in)));
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
          case CHECKPOINT_COMMITTED_TXNS:
            builder.setCommittedTransactionsBytes(readLengthDelimitedBytes(in));
            break;
          default:
            skipField(in, wireType);
        }
      }
      // Rebuild lookups after all entries are loaded (handles out-of-order decoding)
      builder.rebuildLookups();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Encodes a UUIDv7 set as a {@code CommittedTransactions} sub-message body
   * with no GC horizon. See {@link #encodeCommittedTransactions(java.util.Collection, long)}
   * for the GC-aware variant.
   */
  static byte[] encodeCommittedTransactions(java.util.Collection<UUID> txns)
      throws IOException {
    return encodeCommittedTransactions(txns, 0L);
  }

  /**
   * Encodes a UUIDv7 set as a {@code CommittedTransactions} sub-message body.
   * Sorts entries descending by UUID (= descending by timestamp for v7),
   * emits {@code max_timestamp_ms}, then a single packed bytes field whose
   * payload is the interleaved (varint delta + 10 random bytes) per entry,
   * and optionally a {@code highest_dropped_timestamp_ms} field when the
   * caller has performed a GC pass.
   *
   * <p>Caller is responsible for omitting the outer field when the set is empty
   * AND the horizon is zero (i.e., no observable state to record).
   */
  static byte[] encodeCommittedTransactions(java.util.Collection<UUID> txns,
                                             long highestDroppedTimestampMs)
      throws IOException {
    UUID[] sorted = txns.toArray(new UUID[0]);
    java.util.Arrays.sort(sorted, (a, b) -> b.compareTo(a));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    long maxTs = sorted.length == 0 ? 0L : UuidV7.timestampMs(sorted[0]);
    writeFixed64(out, COMMITTED_TXNS_MAX_TIMESTAMP_MS, maxTs);

    ByteArrayOutputStream packed = new ByteArrayOutputStream(13 * sorted.length);
    long prev = maxTs;
    for (UUID u : sorted) {
      long ts = UuidV7.timestampMs(u);
      writeRawVarint64(packed, prev - ts);
      prev = ts;
      packed.write(UuidV7.packRandomBits(u));
    }
    writeBytes(out, COMMITTED_TXNS_PACKED_ENTRIES, packed.toByteArray());
    if (highestDroppedTimestampMs != 0L) {
      writeFixed64(out, COMMITTED_TXNS_HIGHEST_DROPPED_TIMESTAMP_MS, highestDroppedTimestampMs);
    }
    return out.toByteArray();
  }

  /**
   * Single-pass rewrite of an inherited {@code CommittedTransactions} body.
   * Streams the inherited entries in descending order, drops any whose
   * {@code timestamp_ms <= dropBeforeTimestampMs} (updating
   * {@code highest_dropped_timestamp_ms}), and merges in the supplied
   * additions in descending order. Additions are emitted unconditionally —
   * the caller's invariant is that they correspond to just-applied log
   * transactions which must remain queryable regardless of the watermark.
   *
   * <p>An exact-duplicate addition (same UUID as an inherited entry) is
   * deduped: the merged output emits a single copy.
   *
   * <p>Returns an empty byte array iff there are no entries to emit AND the
   * resulting {@code highest_dropped_timestamp_ms} is zero (no observable
   * state). Otherwise returns the encoded body — including a horizon-only
   * body when every entry was dropped but the horizon is non-zero, so that
   * monotonicity is preserved across subsequent compactions.
   */
  static byte[] rewriteCommittedBytes(
      byte[] inheritedBytes,
      java.util.Collection<UUID> addUnconditionally,
      long dropBeforeTimestampMs) {
    UUID[] adds = addUnconditionally.toArray(new UUID[0]);
    long[] addTs = new long[adds.length];
    byte[][] addRand = new byte[adds.length][];
    for (int i = 0; i < adds.length; i++) {
      Preconditions.checkArgument(UuidV7.isV7(adds[i]),
          "Committed transaction IDs must be UUIDv7; got %s", adds[i]);
      addTs[i] = UuidV7.timestampMs(adds[i]);
      addRand[i] = UuidV7.packRandomBits(adds[i]);
    }
    // Sort additions descending: by ts desc, then by packed-random desc
    // (lexicographic unsigned matches UUID.compareTo for v7 — see UuidV7).
    Integer[] addIdx = new Integer[adds.length];
    for (int i = 0; i < adds.length; i++) {
      addIdx[i] = i;
    }
    java.util.Arrays.sort(addIdx, (a, b) -> {
      int c = Long.compare(addTs[b], addTs[a]);
      if (c != 0) return c;
      for (int k = 0; k < 10; k++) {
        int d = (addRand[b][k] & 0xFF) - (addRand[a][k] & 0xFF);
        if (d != 0) return d;
      }
      return 0;
    });
    long[] sortedAddTs = new long[adds.length];
    byte[][] sortedAddRand = new byte[adds.length][];
    for (int i = 0; i < adds.length; i++) {
      sortedAddTs[i] = addTs[addIdx[i]];
      sortedAddRand[i] = addRand[addIdx[i]];
    }

    // Parse the inherited body's top-level fields.
    long prevMaxTs = 0L;
    int packedStart = -1;
    int packedEnd = -1;
    long prevHorizon = 0L;
    if (inheritedBytes.length > 0) {
      try {
        ByteArrayInputStream in = new ByteArrayInputStream(inheritedBytes);
        while (in.available() > 0) {
          int tag = readVarint(in);
          int fieldNumber = tag >>> 3;
          int wireType = tag & 0x7;
          switch (fieldNumber) {
            case COMMITTED_TXNS_MAX_TIMESTAMP_MS:
              prevMaxTs = readFixed64(in);
              break;
            case COMMITTED_TXNS_PACKED_ENTRIES:
              int len = readVarint(in);
              packedStart = inheritedBytes.length - in.available();
              packedEnd = packedStart + len;
              long skipped = in.skip(len);
              if (skipped != len) {
                throw new IOException(
                    "Truncated CommittedTransactions: expected " + len
                        + " packed bytes, got " + skipped);
              }
              break;
            case COMMITTED_TXNS_HIGHEST_DROPPED_TIMESTAMP_MS:
              prevHorizon = readFixed64(in);
              break;
            default:
              skipField(in, wireType);
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    // Two-pointer merge of inherited entries (in descending order) against
    // the sorted additions. Output is buffered in (ts, rand) parallel arrays
    // because we don't know max_timestamp_ms until we've seen the first
    // emitted entry.
    int inheritedUpper = packedStart < 0 ? 0 : ((packedEnd - packedStart) / 11) + 1;
    int capacity = inheritedUpper + adds.length;
    long[] outTs = new long[capacity];
    byte[][] outRand = new byte[capacity][];
    int outCount = 0;
    long newHorizon = prevHorizon;

    long ts = prevMaxTs;
    int off = packedStart < 0 ? 0 : packedStart;
    int end = packedStart < 0 ? 0 : packedEnd;
    int aPtr = 0;

    while (off < end) {
      // Decode next inherited entry: varint delta + 10 random bytes.
      long delta = 0L;
      int shift = 0;
      while (true) {
        if (off >= end) {
          throw new UncheckedIOException(new IOException(
              "Malformed CommittedTransactions: truncated varint"));
        }
        int b = inheritedBytes[off++] & 0xFF;
        delta |= ((long) (b & 0x7F)) << shift;
        if ((b & 0x80) == 0) break;
        shift += 7;
      }
      ts -= delta;
      if (off + 10 > end) {
        throw new UncheckedIOException(new IOException(
            "Malformed CommittedTransactions: truncated random bytes"));
      }
      int randOff = off;
      off += 10;

      // Emit any additions strictly greater than the inherited entry first.
      while (aPtr < sortedAddTs.length
          && compareTsRand(sortedAddTs[aPtr], sortedAddRand[aPtr],
              ts, inheritedBytes, randOff) > 0) {
        outTs[outCount] = sortedAddTs[aPtr];
        outRand[outCount] = sortedAddRand[aPtr];
        outCount++;
        aPtr++;
      }

      if (ts <= dropBeforeTimestampMs) {
        // Drop this inherited entry; the addition (if any) at the same key
        // still stands and will be emitted from the addition stream.
        if (ts > newHorizon) {
          newHorizon = ts;
        }
      } else {
        // Dedup: an addition equal to this inherited entry collapses to one.
        if (aPtr < sortedAddTs.length
            && compareTsRand(sortedAddTs[aPtr], sortedAddRand[aPtr],
                ts, inheritedBytes, randOff) == 0) {
          aPtr++;
        }
        outTs[outCount] = ts;
        byte[] copy = new byte[10];
        System.arraycopy(inheritedBytes, randOff, copy, 0, 10);
        outRand[outCount] = copy;
        outCount++;
      }
    }

    // Drain remaining additions (older than the smallest inherited entry, or
    // older-than-watermark inherited that got dropped left holes in the
    // descending stream — additions still survive unconditionally).
    while (aPtr < sortedAddTs.length) {
      outTs[outCount] = sortedAddTs[aPtr];
      outRand[outCount] = sortedAddRand[aPtr];
      outCount++;
      aPtr++;
    }

    if (outCount == 0 && newHorizon == 0L) {
      return new byte[0];
    }

    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      long maxTs = outCount > 0 ? outTs[0] : 0L;
      writeFixed64(out, COMMITTED_TXNS_MAX_TIMESTAMP_MS, maxTs);

      ByteArrayOutputStream packed = new ByteArrayOutputStream(13 * outCount);
      long prev = maxTs;
      for (int i = 0; i < outCount; i++) {
        long entryTs = outTs[i];
        writeRawVarint64(packed, prev - entryTs);
        prev = entryTs;
        packed.write(outRand[i]);
      }
      writeBytes(out, COMMITTED_TXNS_PACKED_ENTRIES, packed.toByteArray());

      if (newHorizon != 0L) {
        writeFixed64(out, COMMITTED_TXNS_HIGHEST_DROPPED_TIMESTAMP_MS, newHorizon);
      }
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Compares an addition entry {@code (aTs, aRand)} against an inherited entry
   * stored in {@code body[bOff..bOff+10)} with timestamp {@code bTs}. Negative
   * iff a < b, zero iff equal, positive iff a > b.
   */
  private static int compareTsRand(long aTs, byte[] aRand, long bTs, byte[] body, int bOff) {
    if (aTs != bTs) {
      return Long.compare(aTs, bTs);
    }
    for (int i = 0; i < 10; i++) {
      int cmp = (aRand[i] & 0xFF) - (body[bOff + i] & 0xFF);
      if (cmp != 0) return cmp;
    }
    return 0;
  }

  /**
   * Scans the inner {@code CommittedTransactions} body for a v7 UUID. Returns
   * true iff the set contains the query. Walks the wire bytes linearly with
   * no per-call allocation: tracks the running timestamp by summing varint
   * deltas, compares the random-bits region of each entry against the
   * query's packed bits in place, and exits early when the running timestamp
   * passes the query's timestamp (descending order).
   *
   * <p>Returns false for non-v7 queries (the dedup set only stores v7 IDs).
   */
  static boolean committedSetContains(byte[] body, UUID query) {
    if (body.length == 0 || !UuidV7.isV7(query)) {
      return false;
    }
    long queryTs = UuidV7.timestampMs(query);
    byte[] queryRand = UuidV7.packRandomBits(query);
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(body);
      long maxTs = 0L;
      int packedStart = -1;
      int packedEnd = -1;
      while (in.available() > 0) {
        int tag = readVarint(in);
        int fieldNumber = tag >>> 3;
        int wireType = tag & 0x7;
        switch (fieldNumber) {
          case COMMITTED_TXNS_MAX_TIMESTAMP_MS:
            maxTs = readFixed64(in);
            break;
          case COMMITTED_TXNS_PACKED_ENTRIES:
            int len = readVarint(in);
            packedStart = body.length - in.available();
            packedEnd = packedStart + len;
            in.skip(len);
            break;
          // COMMITTED_TXNS_HIGHEST_DROPPED_TIMESTAMP_MS: ignored here; the
          // membership predicate doesn't consult the horizon. The horizon-aware
          // tri-state result lives in ProtoCatalogFile.containsTransaction,
          // which calls committedSetHorizon on a miss.
          default:
            skipField(in, wireType);
        }
      }
      if (packedStart < 0 || queryTs > maxTs) {
        return false;
      }
      long ts = maxTs;
      int off = packedStart;
      while (off < packedEnd) {
        // Inline varint read to avoid wrapping with InputStream.
        long delta = 0;
        int shift = 0;
        while (true) {
          int b = body[off++] & 0xFF;
          delta |= ((long) (b & 0x7F)) << shift;
          if ((b & 0x80) == 0) break;
          shift += 7;
        }
        ts -= delta;
        if (off + 10 > packedEnd) {
          return false; // malformed; treat as not-contained
        }
        if (ts == queryTs && randomMatches(body, off, queryRand)) {
          return true;
        }
        if (ts < queryTs) {
          return false; // descending order: any further entries are older
        }
        off += 10;
      }
      return false;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Reads the {@code highest_dropped_timestamp_ms} field from a committed-set
   * body. Returns 0 when the body is empty, the field is absent, or no GC has
   * ever fired on this set. Walks only the top-level fields and skips the
   * (potentially large) {@code packed_entries} payload, so this is cheap.
   */
  static long committedSetHorizon(byte[] body) {
    if (body.length == 0) {
      return 0L;
    }
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(body);
      long horizon = 0L;
      while (in.available() > 0) {
        int tag = readVarint(in);
        int fieldNumber = tag >>> 3;
        int wireType = tag & 0x7;
        if (fieldNumber == COMMITTED_TXNS_HIGHEST_DROPPED_TIMESTAMP_MS) {
          horizon = readFixed64(in);
        } else {
          skipField(in, wireType);
        }
      }
      return horizon;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static boolean randomMatches(byte[] body, int off, byte[] query) {
    for (int i = 0; i < 10; i++) {
      if (body[off + i] != query[i]) return false;
    }
    return true;
  }

  /**
   * Decodes a {@code CommittedTransactions} body into the canonical {@code Set<UUID>}.
   * Used by the {@code committedTransactions()} bulk-getter and during merge in
   * {@code Builder.build()}; not on the query hot path.
   */
  static java.util.LinkedHashSet<UUID> decodeCommittedTransactions(byte[] body)
      throws IOException {
    java.util.LinkedHashSet<UUID> out = new java.util.LinkedHashSet<>();
    if (body.length == 0) {
      return out;
    }
    ByteArrayInputStream in = new ByteArrayInputStream(body);
    long maxTs = 0L;
    byte[] packed = new byte[0];
    while (in.available() > 0) {
      int tag = readVarint(in);
      int fieldNumber = tag >>> 3;
      int wireType = tag & 0x7;
      switch (fieldNumber) {
        case COMMITTED_TXNS_MAX_TIMESTAMP_MS:
          maxTs = readFixed64(in);
          break;
        case COMMITTED_TXNS_PACKED_ENTRIES:
          packed = readLengthDelimitedBytes(in);
          break;
        // COMMITTED_TXNS_HIGHEST_DROPPED_TIMESTAMP_MS: skipped — this bulk
        // entrypoint surfaces only the entries, not the GC horizon. Callers
        // that need the horizon use committedSetHorizon (cheap) or peek at
        // it during a single-pass merge in rewriteCommittedBytes.
        default:
          skipField(in, wireType);
      }
    }
    long ts = maxTs;
    int off = 0;
    while (off < packed.length) {
      int start = off;
      long delta = 0;
      int shift = 0;
      while (true) {
        if (off >= packed.length) {
          throw new IOException("Malformed CommittedTransactions: truncated varint at " + start);
        }
        int b = packed[off++] & 0xFF;
        delta |= ((long) (b & 0x7F)) << shift;
        if ((b & 0x80) == 0) break;
        shift += 7;
      }
      ts -= delta;
      if (off + 10 > packed.length) {
        throw new IOException(
            "Malformed CommittedTransactions: entry at offset " + start
                + " missing 10 random bytes (have " + (packed.length - off) + ")");
      }
      out.add(UuidV7.fromTimestampAndRandom(ts, packed, off));
      off += 10;
    }
    return out;
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
    // Omit the field on the default value (CODEC_JSON_GZIP = 0) to keep wire
    // bytes tight; decoders default unset to TAG_JSON_GZIP.
    if (metadataCodec != InlineMetadataCodecs.TAG_JSON_GZIP) {
      writeVarint(out, INLINE_TBL_METADATA_CODEC, metadataCodec & 0xFF);
    }

    // Manifest pool (field 7): encode each unique ManifestFile entry
    if (pool != null && !pool.isEmpty()) {
      // Build positional index for snapshot refs
      List<String> poolOrder = new ArrayList<>(pool.keySet());
      Map<String, Integer> pathToIndex = new java.util.HashMap<>();
      for (int i = 0; i < poolOrder.size(); i++) {
        pathToIndex.put(poolOrder.get(i), i);
        ManifestFile mf = pool.get(poolOrder.get(i));
        byte[] mfBytes = encodeManifestFileEntry(mf, manifestPrefix);
        writeLengthDelimited(out, INLINE_TBL_MANIFEST_POOL, mfBytes);
      }

      // Snapshot refs (field 8): per-snapshot index arrays
      if (snapRefs != null) {
        for (Map.Entry<Long, List<String>> snapEntry : snapRefs.entrySet()) {
          byte[] refBytes = encodeSnapshotManifestRefs(
              snapEntry.getKey(), snapEntry.getValue(), pathToIndex);
          writeLengthDelimited(out, INLINE_TBL_SNAPSHOT_REFS, refBytes);
        }
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
    byte metadataCodec = InlineMetadataCodecs.TAG_JSON_GZIP;
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
    builder.addInlineTable(id, nsId, name, version, metadata, metadataCodec, manifestPrefix);

    // Decode manifest pool (positional array) -> path-keyed map
    if (!poolEntryBytes.isEmpty()) {
      List<ManifestFile> poolList = new ArrayList<>(poolEntryBytes.size());
      for (byte[] entryBytes : poolEntryBytes) {
        poolList.add(decodeManifestFileEntry(entryBytes, manifestPrefix));
      }
      for (ManifestFile mf : poolList) {
        builder.addManifestToPool(id, mf);
      }

      // Decode snapshot refs (index arrays) -> path lists
      for (byte[] refBytes : snapRefBytes) {
        decodeSnapshotManifestRefs(refBytes, id, poolList, builder);
      }
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
   * Encodes a transaction from accumulated mutations.
   */
  public static byte[] encodeTransaction(Transaction txn) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      writeBytes(out, TXN_ID, uuidToBytes(txn.id()));
      for (Action action : txn.actions()) {
        byte[] actionBytes = encodeAction(action);
        writeLengthDelimited(out, TXN_ACTIONS, actionBytes);
      }
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Decodes a transaction from bytes.
   */
  public static Transaction decodeTransaction(byte[] bytes) {
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(bytes);
      UUID txnId = null;
      List<Action> actions = new ArrayList<>();

      while (in.available() > 0) {
        int tag = readVarint(in);
        int fieldNumber = tag >>> 3;
        switch (fieldNumber) {
          case TXN_ID:
            txnId = bytesToUuid(readLengthDelimitedBytes(in));
            break;
          case TXN_ACTIONS:
            actions.add(decodeAction(readLengthDelimitedBytes(in)));
            break;
          default:
            skipField(in, tag & 0x7);
        }
      }

      Preconditions.checkNotNull(txnId, "Transaction ID is required");
      return new Transaction(txnId, actions);
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
        // Codec only meaningful when full_metadata is set; omit on default.
        if (a.fullMetadataCodec != InlineMetadataCodecs.TAG_JSON_GZIP) {
          writeVarint(inner, UPDATE_INLINE_FULL_METADATA_CODEC, a.fullMetadataCodec & 0xFF);
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
      if (a.metadataCodec != InlineMetadataCodecs.TAG_JSON_GZIP) {
        writeVarint(inner, CREATE_INLINE_TBL_METADATA_CODEC, a.metadataCodec & 0xFF);
      }
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
    byte metadataCodec = InlineMetadataCodecs.TAG_JSON_GZIP;
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
    return new CreateTableInlineAction(
        id, version, nsId, nsVersion, name, metadata, metadataCodec, deltaBytes);
  }

  private static UpdateTableInlineAction decodeUpdateTableInline(byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    int id = 0, version = 0;
    byte[] deltaBytes = null;
    byte[] fullMetadata = null;
    String metadataLocation = null;
    byte fullMetadataCodec = InlineMetadataCodecs.TAG_JSON_GZIP;

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
        default:
          skipField(in, tag & 0x7);
      }
    }
    return new UpdateTableInlineAction(
        id, version, deltaBytes, fullMetadata, fullMetadataCodec, metadataLocation);
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

  private static void writeVarint(OutputStream out, int fieldNumber, int value) throws IOException {
    int tag = (fieldNumber << 3) | WIRE_VARINT;
    writeRawVarint(out, tag);
    writeRawVarint(out, value);
  }

  private static void writeString(OutputStream out, int fieldNumber, String value) throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    writeLengthDelimited(out, fieldNumber, bytes);
  }

  private static void writeBytes(OutputStream out, int fieldNumber, byte[] value) throws IOException {
    writeLengthDelimited(out, fieldNumber, value);
  }

  private static void writeLengthDelimited(OutputStream out, int fieldNumber, byte[] value) throws IOException {
    int tag = (fieldNumber << 3) | WIRE_LENGTH_DELIMITED;
    writeRawVarint(out, tag);
    writeRawVarint(out, value.length);
    out.write(value);
  }

  private static void writeVarint64(OutputStream out, int fieldNumber, long value) throws IOException {
    writeRawVarint(out, (fieldNumber << 3) | WIRE_VARINT);
    writeRawVarint64(out, value);
  }

  private static void writeFixed64(OutputStream out, int fieldNumber, long value) throws IOException {
    writeRawVarint(out, (fieldNumber << 3) | WIRE_64BIT);
    for (int i = 0; i < 8; i++) {
      out.write((int) (value & 0xFF));
      value >>>= 8;
    }
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
    int result = 0;
    int shift = 0;
    int b;
    do {
      b = in.read();
      if (b < 0) {
        throw new IOException("Unexpected end of stream reading varint");
      }
      result |= (b & 0x7F) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    return result;
  }

  private static String readString(InputStream in) throws IOException {
    byte[] bytes = readLengthDelimitedBytes(in);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static byte[] readLengthDelimitedBytes(InputStream in) throws IOException {
    int length = readVarint(in);
    byte[] bytes = new byte[length];
    int read = 0;
    while (read < length) {
      int n = in.read(bytes, read, length - read);
      if (n < 0) {
        throw new IOException("Unexpected end of stream");
      }
      read += n;
    }
    return bytes;
  }

  private static long readVarint64(InputStream in) throws IOException {
    long result = 0;
    int shift = 0, b;
    do {
      b = in.read();
      if (b < 0) {
        throw new IOException("Unexpected end of stream");
      }
      result |= (long) (b & 0x7F) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    return result;
  }

  private static long readFixed64(InputStream in) throws IOException {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      int b = in.read();
      if (b < 0) {
        throw new IOException("Unexpected end of stream");
      }
      result |= (long) (b & 0xFF) << (i * 8);
    }
    return result;
  }

  private static void skipField(InputStream in, int wireType) throws IOException {
    switch (wireType) {
      case WIRE_VARINT:
        readVarint64(in);
        break;
      case WIRE_64BIT:
        in.skip(8);
        break;
      case WIRE_LENGTH_DELIMITED:
        int length = readVarint(in);
        in.skip(length);
        break;
      case 5: // 32-bit
        in.skip(4);
        break;
      default:
        throw new IOException("Unknown wire type: " + wireType);
    }
  }

  private static byte[] uuidToBytes(UUID uuid) {
    ByteBuffer buffer = ByteBuffer.allocate(16);
    buffer.putLong(uuid.getMostSignificantBits());
    buffer.putLong(uuid.getLeastSignificantBits());
    return buffer.array();
  }

  private static UUID bytesToUuid(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    long msb = buffer.getLong();
    long lsb = buffer.getLong();
    return new UUID(msb, lsb);
  }

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
    }
    if (manifest.keyMetadata() != null) {
      byte[] km = new byte[manifest.keyMetadata().remaining()];
      manifest.keyMetadata().duplicate().get(km);
      writeBytes(out, MF_KEY_METADATA, km);
    }
    if (manifest.firstRowId() != null && manifest.firstRowId() != 0) {
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
        default:
          skipField(in, wireType);
      }
    }

    String fullPath = (pathPrefix != null ? pathPrefix : "") + pathSuffix;
    return new DecodedManifestFile(
        fullPath, length, specId, content, seqNum, minSeqNum, snapshotId,
        addedFiles, existingFiles, deletedFiles, addedRows, existingRows, deletedRows,
        partitions.isEmpty() ? null : ImmutableList.copyOf(partitions),
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
    return new DecodedPartitionFieldSummary(
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

  /** Lightweight PartitionFieldSummary decoded from protobuf. */
  public static class DecodedPartitionFieldSummary implements PartitionFieldSummary {
    private final boolean containsNull;
    private final Boolean containsNaN;
    private final ByteBuffer lowerBound;
    private final ByteBuffer upperBound;

    DecodedPartitionFieldSummary(
        boolean containsNull, boolean containsNaN,
        ByteBuffer lowerBound, ByteBuffer upperBound) {
      this.containsNull = containsNull;
      this.containsNaN = containsNaN;
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
    }

    @Override public boolean containsNull() { return containsNull; }
    @Override public Boolean containsNaN() { return containsNaN; }
    @Override public ByteBuffer lowerBound() { return lowerBound; }
    @Override public ByteBuffer upperBound() { return upperBound; }
    @Override public PartitionFieldSummary copy() { return this; }
  }

  // ============================================================
  // Transaction class
  // ============================================================

  /**
   * Represents a decoded transaction with verify/apply semantics.
   */
  public static class Transaction {
    private final UUID id;
    private final List<Action> actions;

    public Transaction(UUID id, List<Action> actions) {
      this.id = id;
      this.actions = actions;
    }

    public static Transaction from(
        CatalogFile.Mut<ProtoCatalogFile, ?> mut,
        ProtoCatalogFormat.ProtoIdManager idManager) {
      UUID txnId = UuidV7.newUuidV7();
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

    public List<Action> actions() {
      return actions;
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
      if (!UuidV7.isV7(id)) {
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
     * Applies all actions in this transaction to the builder.
     */
    public void apply(ProtoCatalogFile.Builder builder) {
      for (Action action : actions) {
        action.apply(builder);
      }
    }
  }

  // ============================================================
  // Action types
  // ============================================================

  public interface Action {
    boolean verify(ProtoCatalogFile.Builder builder);
    void apply(ProtoCatalogFile.Builder builder);
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
      // Parent must exist (unless root) and version must match
      if (parentId != 0) {
        int currentParentVersion = builder.namespaceVersion(parentId);
        if (parentVersion >= 0 && currentParentVersion != parentVersion) {
          return false;
        }
      }
      return true;
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
      // Read action fails if table version has changed (read-your-writes validation)
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

    public UpdateTableInlineAction(
        int id, int version, byte[] fullMetadata, byte fullMetadataCodec,
        String metadataLocation) {
      this(id, version, null, fullMetadata, fullMetadataCodec, metadataLocation);
    }

    /** Back-compat 4-arg constructor: defaults full-metadata codec to gzip. */
    public UpdateTableInlineAction(
        int id, int version, byte[] fullMetadata, String metadataLocation) {
      this(id, version, null, fullMetadata,
          InlineMetadataCodecs.TAG_JSON_GZIP, metadataLocation);
    }

    /**
     * Convenience constructor that defaults the full-metadata codec to
     * {@link InlineMetadataCodecs#TAG_JSON_GZIP}. Used by tests and decode
     * paths that haven't observed a codec field yet.
     */
    public UpdateTableInlineAction(
        int id, int version, byte[] deltaBytes, byte[] fullMetadata, String metadataLocation) {
      this(id, version, deltaBytes, fullMetadata,
          InlineMetadataCodecs.TAG_JSON_GZIP, metadataLocation);
    }

    public UpdateTableInlineAction(
        int id, int version, byte[] deltaBytes, byte[] fullMetadata,
        byte fullMetadataCodec, String metadataLocation) {
      this.id = id;
      this.version = version;
      this.deltaBytes = deltaBytes;
      this.fullMetadata = fullMetadata;
      this.fullMetadataCodec = fullMetadataCodec;
      this.metadataLocation = metadataLocation;
    }

    /** Creates an UpdateTableInlineAction in DELTA mode. */
    public static UpdateTableInlineAction delta(int id, int version, byte[] deltaBytes) {
      return new UpdateTableInlineAction(
          id, version, deltaBytes, null,
          InlineMetadataCodecs.TAG_JSON_GZIP, null);
    }

    /** Creates an UpdateTableInlineAction in FULL mode with an explicit codec. */
    public static UpdateTableInlineAction full(
        int id, int version, byte[] fullMetadata, byte fullMetadataCodec) {
      return new UpdateTableInlineAction(
          id, version, null, fullMetadata, fullMetadataCodec, null);
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
        // manifest pool and snapshot refs. `removeInlineMetadata` (used by
        // the POINTER branch below) also clears the pool — that's the correct
        // behavior for pointer-mode eviction, but would silently corrupt an
        // ML-populated table on a non-ML commit (schema evolution, etc.).
        // See ML_INLINE_REVIEW2.md §1.1.
        ProtoCatalogFile.TblEntry old = builder.tableEntry(id);
        if (old != null) {
          builder.updateInlineMetadata(id, version + 1, fullMetadata, fullMetadataCodec);
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
     * routes these entries through {@link InlineDeltaCodec#applyDeltaWithManifests}
     * so the per-table manifest pool and per-snapshot ref list are populated
     * atomically with the table create. {@code null} for non-inline-ML
     * creates and for inline-ML creates with no snapshots. See errata D9.
     */
    final byte[] deltaBytes;

    public CreateTableInlineAction(
        int id, int version, int namespaceId, int namespaceVersion,
        String name, byte[] metadata) {
      this(id, version, namespaceId, namespaceVersion, name, metadata,
          InlineMetadataCodecs.TAG_JSON_GZIP, null);
    }

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
    public void apply(ProtoCatalogFile.Builder builder) {
      // manifest list prefix is derived from metadata at checkpoint time;
      // for newly created tables, we default to empty (set later on first snapshot)
      builder.addInlineTable(id, namespaceId, name, version, metadata, metadataCodec, "");
      if (deltaBytes != null) {
        // Inline-ML create: feed the initial manifest-pool delta through the
        // same machinery the update path uses. Decodes against the just-stored
        // metadata bytes so the per-snapshot ref list can carry forward from
        // each snapshot's parent within the same create transaction.
        InlineDeltaCodec.applyDeltaWithManifests(
            metadata, InlineMetadataCodecs.byTag(metadataCodec), deltaBytes, builder, id);
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
   * See errata.md §D5 and design.md "Operation × Operation Conflict Matrix".
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
