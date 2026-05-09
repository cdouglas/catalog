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
package org.apache.iceberg.io.bench.encoders;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.InlineDeltaSnapshots;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.bench.MetadataDecoder;

/**
 * The candidate. Strips the {@code snapshots} array from the JSON, encodes each
 * snapshot as a length-delimited protobuf message, and gzips the wrapped
 * {@code [json][snap_count][len-prefixed proto sn]+} stream.
 *
 * <p>Two ways to win versus {@link JsonGzipEncoder}:
 *
 * <ul>
 *   <li><b>Bytes</b>: protobuf strips JSON keys ({@code "snapshot-id"},
 *       {@code "timestamp-ms"}, etc.) and varint-packs IDs/timestamps. Phase A
 *       measured snapshots as 64.5% of post-gzip bytes at the median, so this
 *       has structural headroom — but gzip's 32 KB window already removes most
 *       of the repeated keys, so the post-gzip delta is what matters.
 *   <li><b>Lazy access</b>: each snapshot is length-prefixed inside the wrapper
 *       stream, so {@code decodeCurrentSnapshotOnly} can skip past non-matching
 *       snapshots in O(1) per skip rather than tokenizing JSON.
 * </ul>
 *
 * <p>Wire format inside the gzip stream:
 *
 * <pre>{@code
 * varint json_len
 * bytes  json (TableMetadata JSON minus the "snapshots" field)
 * varint snap_count
 * (varint blob_len, bytes blob)+   // snap_count repetitions
 * }</pre>
 *
 * <p>Each snapshot blob is a hand-rolled protobuf message:
 *
 * <pre>{@code
 *   field 1 (fixed64)     snapshot_id
 *   field 2 (varint)      sequence_number
 *   field 3 (fixed64)     parent_snapshot_id        // omitted if null
 *   field 4 (varint)      timestamp_ms
 *   field 5 (length-delim) operation                 // UTF-8
 *   field 6 (length-delim) manifest_list             // UTF-8
 *   field 7 (length-delim, repeated) summary entry  // {key=1, value=2}
 *   field 8 (varint)      schema_id                 // omitted if null
 *   field 9 (varint)      first_row_id              // omitted if null
 *   field 10 (varint)     added_rows                // omitted if null
 *   field 11 (length-delim) key_id                   // omitted if null
 * }</pre>
 *
 * <p>The wire format is intentionally hand-rolled (not generated from a
 * {@code .proto}) so the bench is self-contained — no test-scope codegen
 * infrastructure to maintain. The fields mirror what
 * {@link org.apache.iceberg.SnapshotParser} would produce / consume.
 */
public final class JsonGzipProtoSnapshotsEncoder implements MetadataDecoder {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // Wire types.
  private static final int WIRE_VARINT = 0;
  private static final int WIRE_FIXED64 = 1;
  private static final int WIRE_LEN_DELIM = 2;

  // Field numbers for the snapshot sub-message.
  private static final int F_SNAPSHOT_ID = 1;
  private static final int F_SEQUENCE_NUMBER = 2;
  private static final int F_PARENT_SNAPSHOT_ID = 3;
  private static final int F_TIMESTAMP_MS = 4;
  private static final int F_OPERATION = 5;
  private static final int F_MANIFEST_LIST = 6;
  private static final int F_SUMMARY_ENTRY = 7;
  private static final int F_SCHEMA_ID = 8;
  private static final int F_FIRST_ROW_ID = 9;
  private static final int F_ADDED_ROWS = 10;
  private static final int F_KEY_ID = 11;

  // Sub-fields of a summary entry.
  private static final int FE_KEY = 1;
  private static final int FE_VALUE = 2;

  @Override
  public String name() {
    return "json-gzip+proto-snapshots";
  }

  @Override
  public byte[] encode(TableMetadata meta) {
    String fullJson = TableMetadataParser.toJson(meta);
    ObjectNode root;
    try {
      root = (ObjectNode) MAPPER.readTree(fullJson);
    } catch (IOException e) {
      throw new RuntimeException("re-parse of TableMetadata JSON failed", e);
    }
    root.remove("snapshots");
    byte[] strippedJson;
    try {
      strippedJson = MAPPER.writeValueAsBytes(root);
    } catch (IOException e) {
      throw new RuntimeException("re-serialize stripped JSON failed", e);
    }

    List<Snapshot> snapshots = meta.snapshots();
    ByteArrayOutputStream wrapper = new ByteArrayOutputStream(
        strippedJson.length + 64 * snapshots.size() + 16);
    writeVarint(wrapper, strippedJson.length);
    writeBytes(wrapper, strippedJson);
    writeVarint(wrapper, snapshots.size());
    for (Snapshot snap : snapshots) {
      byte[] blob = encodeSnapshotProto(snap);
      writeVarint(wrapper, blob.length);
      writeBytes(wrapper, blob);
    }
    return EncoderUtil.gzip(wrapper.toByteArray());
  }

  @Override
  public TableMetadata decodeFull(byte[] encoded) {
    byte[] inner = EncoderUtil.gunzip(encoded);
    Cursor c = new Cursor(inner);

    int jsonLen = c.readVarint32();
    byte[] strippedJson = c.readBytes(jsonLen);
    int snapCount = c.readVarint32();

    ObjectNode root;
    try {
      root = (ObjectNode) MAPPER.readTree(strippedJson);
    } catch (IOException e) {
      throw new RuntimeException("decodeFull: parse stripped JSON failed", e);
    }

    ArrayNode snapsArr = MAPPER.createArrayNode();
    for (int i = 0; i < snapCount; i++) {
      int blobLen = c.readVarint32();
      ObjectNode snap = decodeSnapshotProtoToJsonNode(c.buf, c.pos, blobLen);
      c.pos += blobLen;
      snapsArr.add(snap);
    }
    root.set("snapshots", snapsArr);

    return TableMetadataParser.fromJson(root.toString());
  }

  @Override
  public Snapshot decodeCurrentSnapshotOnly(byte[] encoded) {
    byte[] inner = EncoderUtil.gunzip(encoded);
    Cursor c = new Cursor(inner);

    int jsonLen = c.readVarint32();
    Long currentId = readCurrentSnapshotId(c.buf, c.pos, jsonLen);
    c.pos += jsonLen;

    int snapCount = c.readVarint32();
    if (currentId == null) {
      return null;
    }
    for (int i = 0; i < snapCount; i++) {
      int blobLen = c.readVarint32();
      // Peek snapshot_id (field 1, fixed64): tag is 0x09, then 8 little-endian bytes.
      // The encoder writes fields in order, so field 1 is at the start of every blob.
      if (blobLen >= 9 && c.buf[c.pos] == 0x09) {
        long sid = readFixed64(c.buf, c.pos + 1);
        if (sid == currentId) {
          Snapshot s = decodeSnapshotProtoToSnapshot(c.buf, c.pos, blobLen);
          c.pos += blobLen;
          return s;
        }
      }
      c.pos += blobLen;
    }
    return null;
  }

  @Override
  public List<Snapshot> decodeRecentSnapshots(byte[] encoded, int k) {
    byte[] inner = EncoderUtil.gunzip(encoded);
    Cursor c = new Cursor(inner);

    int jsonLen = c.readVarint32();
    Long currentId = readCurrentSnapshotId(c.buf, c.pos, jsonLen);
    c.pos += jsonLen;

    int snapCount = c.readVarint32();
    if (currentId == null || snapCount == 0) {
      return new ArrayList<>();
    }

    // Build id -> blob_offset index. O(N) one pass.
    long[] ids = new long[snapCount];
    int[] offsets = new int[snapCount];
    int[] lengths = new int[snapCount];
    for (int i = 0; i < snapCount; i++) {
      int blobLen = c.readVarint32();
      lengths[i] = blobLen;
      offsets[i] = c.pos;
      // Snapshot id is the first field (fixed64, tag 0x09); peek without full decode.
      ids[i] = (blobLen >= 9 && c.buf[c.pos] == 0x09) ? readFixed64(c.buf, c.pos + 1) : Long.MIN_VALUE;
      c.pos += blobLen;
    }
    Map<Long, Integer> indexById = new HashMap<>(snapCount * 2);
    for (int i = 0; i < snapCount; i++) {
      indexById.put(ids[i], i);
    }

    List<Snapshot> out = new ArrayList<>(k);
    Long id = currentId;
    for (int i = 0; i < k && id != null; i++) {
      Integer idx = indexById.get(id);
      if (idx == null) {
        break;
      }
      Snapshot s = decodeSnapshotProtoToSnapshot(c.buf, offsets[idx], lengths[idx]);
      out.add(s);
      id = s.parentId();
    }
    return out;
  }

  // ---------------------------------------------------------------------------
  // Snapshot proto encode
  // ---------------------------------------------------------------------------

  private static byte[] encodeSnapshotProto(Snapshot snap) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(96);
    writeFixed64Field(out, F_SNAPSHOT_ID, snap.snapshotId());
    writeVarintField(out, F_SEQUENCE_NUMBER, snap.sequenceNumber());
    if (snap.parentId() != null) {
      writeFixed64Field(out, F_PARENT_SNAPSHOT_ID, snap.parentId());
    }
    writeVarintField(out, F_TIMESTAMP_MS, snap.timestampMillis());
    writeStringField(out, F_OPERATION,
        snap.operation() == null ? "append" : snap.operation());
    writeStringField(out, F_MANIFEST_LIST,
        snap.manifestListLocation() == null ? "" : snap.manifestListLocation());
    if (snap.summary() != null) {
      for (Map.Entry<String, String> e : snap.summary().entrySet()) {
        writeSummaryEntry(out, e.getKey(), e.getValue());
      }
    }
    if (snap.schemaId() != null) {
      writeVarintField(out, F_SCHEMA_ID, snap.schemaId());
    }
    if (snap.firstRowId() != null) {
      writeVarintField(out, F_FIRST_ROW_ID, snap.firstRowId());
    }
    if (snap.addedRows() != null) {
      writeVarintField(out, F_ADDED_ROWS, snap.addedRows());
    }
    if (snap.keyId() != null) {
      writeStringField(out, F_KEY_ID, snap.keyId());
    }
    return out.toByteArray();
  }

  private static void writeSummaryEntry(ByteArrayOutputStream out, String key, String value) {
    ByteArrayOutputStream entry = new ByteArrayOutputStream(key.length() + value.length() + 8);
    writeStringField(entry, FE_KEY, key);
    writeStringField(entry, FE_VALUE, value);
    byte[] entryBytes = entry.toByteArray();
    writeTag(out, F_SUMMARY_ENTRY, WIRE_LEN_DELIM);
    writeVarint(out, entryBytes.length);
    writeBytes(out, entryBytes);
  }

  // ---------------------------------------------------------------------------
  // Snapshot proto decode (two strategies, sharing one parser via SnapshotFields)
  // ---------------------------------------------------------------------------

  private static Snapshot decodeSnapshotProtoToSnapshot(byte[] buf, int off, int len) {
    SnapshotFields f = parseSnapshotFields(buf, off, len);
    Map<String, String> summaryWithOp = new LinkedHashMap<>();
    if (f.operation != null) {
      summaryWithOp.put("operation", f.operation);
    }
    if (f.summary != null) {
      summaryWithOp.putAll(f.summary);
    }
    return InlineDeltaSnapshots.create(
        f.sequenceNumber,
        f.snapshotId,
        f.parentId,
        f.timestampMs,
        summaryWithOp,
        f.schemaId,
        f.manifestList == null ? "" : f.manifestList,
        f.firstRowId,
        f.addedRows,
        f.keyId);
  }

  private static ObjectNode decodeSnapshotProtoToJsonNode(byte[] buf, int off, int len) {
    SnapshotFields f = parseSnapshotFields(buf, off, len);
    ObjectNode snap = MAPPER.createObjectNode();
    snap.put("snapshot-id", f.snapshotId);
    snap.put("sequence-number", f.sequenceNumber);
    if (f.parentId != null) {
      snap.put("parent-snapshot-id", f.parentId);
    }
    snap.put("timestamp-ms", f.timestampMs);
    ObjectNode summaryNode = MAPPER.createObjectNode();
    if (f.operation != null) {
      summaryNode.put("operation", f.operation);
    }
    if (f.summary != null) {
      for (Map.Entry<String, String> e : f.summary.entrySet()) {
        summaryNode.put(e.getKey(), e.getValue());
      }
    }
    snap.set("summary", summaryNode);
    if (f.manifestList != null && !f.manifestList.isEmpty()) {
      snap.put("manifest-list", f.manifestList);
    }
    if (f.schemaId != null) {
      snap.put("schema-id", f.schemaId);
    }
    if (f.firstRowId != null) {
      snap.put("first-row-id", f.firstRowId);
    }
    if (f.addedRows != null) {
      snap.put("added-rows", f.addedRows);
    }
    if (f.keyId != null) {
      snap.put("key-id", f.keyId);
    }
    return snap;
  }

  private static final class SnapshotFields {
    long snapshotId;
    long sequenceNumber;
    Long parentId;
    long timestampMs;
    String operation;
    String manifestList;
    Map<String, String> summary;
    Integer schemaId;
    Long firstRowId;
    Long addedRows;
    String keyId;
  }

  private static SnapshotFields parseSnapshotFields(byte[] buf, int off, int len) {
    SnapshotFields f = new SnapshotFields();
    int end = off + len;
    int p = off;
    while (p < end) {
      long tag = readVarint64(buf, p);
      p += varintLen(tag);
      int fieldNumber = (int) (tag >>> 3);
      int wireType = (int) (tag & 0x7);
      switch (fieldNumber) {
        case F_SNAPSHOT_ID:
          f.snapshotId = readFixed64(buf, p);
          p += 8;
          break;
        case F_SEQUENCE_NUMBER:
          f.sequenceNumber = readVarint64(buf, p);
          p += varintLen(f.sequenceNumber);
          break;
        case F_PARENT_SNAPSHOT_ID:
          f.parentId = readFixed64(buf, p);
          p += 8;
          break;
        case F_TIMESTAMP_MS:
          f.timestampMs = readVarint64(buf, p);
          p += varintLen(f.timestampMs);
          break;
        case F_OPERATION: {
          int sl = (int) readVarint64(buf, p); p += varintLen(sl);
          f.operation = new String(buf, p, sl, StandardCharsets.UTF_8);
          p += sl;
          break;
        }
        case F_MANIFEST_LIST: {
          int sl = (int) readVarint64(buf, p); p += varintLen(sl);
          f.manifestList = new String(buf, p, sl, StandardCharsets.UTF_8);
          p += sl;
          break;
        }
        case F_SUMMARY_ENTRY: {
          int el = (int) readVarint64(buf, p); p += varintLen(el);
          if (f.summary == null) {
            f.summary = new LinkedHashMap<>();
          }
          parseSummaryEntry(buf, p, el, f.summary);
          p += el;
          break;
        }
        case F_SCHEMA_ID:
          f.schemaId = (int) readVarint64(buf, p);
          p += varintLen(f.schemaId);
          break;
        case F_FIRST_ROW_ID:
          f.firstRowId = readVarint64(buf, p);
          p += varintLen(f.firstRowId);
          break;
        case F_ADDED_ROWS:
          f.addedRows = readVarint64(buf, p);
          p += varintLen(f.addedRows);
          break;
        case F_KEY_ID: {
          int sl = (int) readVarint64(buf, p); p += varintLen(sl);
          f.keyId = new String(buf, p, sl, StandardCharsets.UTF_8);
          p += sl;
          break;
        }
        default:
          // Unknown field — skip per wire type.
          p = skipField(buf, p, wireType);
      }
    }
    return f;
  }

  private static void parseSummaryEntry(byte[] buf, int off, int len, Map<String, String> out) {
    int end = off + len;
    int p = off;
    String key = null;
    String value = null;
    while (p < end) {
      long tag = readVarint64(buf, p);
      p += varintLen(tag);
      int fieldNumber = (int) (tag >>> 3);
      int wireType = (int) (tag & 0x7);
      if (wireType != WIRE_LEN_DELIM) {
        p = skipField(buf, p, wireType);
        continue;
      }
      int sl = (int) readVarint64(buf, p); p += varintLen(sl);
      String s = new String(buf, p, sl, StandardCharsets.UTF_8);
      p += sl;
      if (fieldNumber == FE_KEY) {
        key = s;
      } else if (fieldNumber == FE_VALUE) {
        value = s;
      }
    }
    if (key != null) {
      out.put(key, value == null ? "" : value);
    }
  }

  private static int skipField(byte[] buf, int p, int wireType) {
    switch (wireType) {
      case WIRE_VARINT:
        return p + varintLen(readVarint64(buf, p));
      case WIRE_FIXED64:
        return p + 8;
      case WIRE_LEN_DELIM: {
        int sl = (int) readVarint64(buf, p);
        return p + varintLen(sl) + sl;
      }
      case 5:
        return p + 4;
      default:
        throw new IllegalStateException("unknown wire type " + wireType);
    }
  }

  // ---------------------------------------------------------------------------
  // JSON helpers
  // ---------------------------------------------------------------------------

  private static Long readCurrentSnapshotId(byte[] buf, int off, int len) {
    // Parse the stripped JSON via Jackson tree just to read current-snapshot-id.
    // The tree-build cost is paid once; for the bench this is the apples-to-apples
    // baseline against JsonEncoderBase.decodeCurrentSnapshotOnly which also builds the tree.
    try {
      JsonNode root = MAPPER.readTree(buf, off, len);
      JsonNode id = root.get("current-snapshot-id");
      if (id == null || id.isNull()) {
        return null;
      }
      return id.asLong();
    } catch (IOException e) {
      throw new RuntimeException("readCurrentSnapshotId failed", e);
    }
  }

  // ---------------------------------------------------------------------------
  // Wire format primitives
  // ---------------------------------------------------------------------------

  private static void writeTag(ByteArrayOutputStream out, int field, int wire) {
    writeVarint(out, ((long) field << 3) | wire);
  }

  private static void writeFixed64Field(ByteArrayOutputStream out, int field, long v) {
    writeTag(out, field, WIRE_FIXED64);
    writeFixed64(out, v);
  }

  private static void writeVarintField(ByteArrayOutputStream out, int field, long v) {
    writeTag(out, field, WIRE_VARINT);
    writeVarint(out, v);
  }

  private static void writeStringField(ByteArrayOutputStream out, int field, String s) {
    byte[] b = s.getBytes(StandardCharsets.UTF_8);
    writeTag(out, field, WIRE_LEN_DELIM);
    writeVarint(out, b.length);
    writeBytes(out, b);
  }

  private static void writeVarint(ByteArrayOutputStream out, long v) {
    while ((v & ~0x7FL) != 0L) {
      out.write((int) ((v & 0x7F) | 0x80));
      v >>>= 7;
    }
    out.write((int) v);
  }

  private static void writeFixed64(ByteArrayOutputStream out, long v) {
    out.write((int) (v & 0xFF));
    out.write((int) ((v >>> 8) & 0xFF));
    out.write((int) ((v >>> 16) & 0xFF));
    out.write((int) ((v >>> 24) & 0xFF));
    out.write((int) ((v >>> 32) & 0xFF));
    out.write((int) ((v >>> 40) & 0xFF));
    out.write((int) ((v >>> 48) & 0xFF));
    out.write((int) ((v >>> 56) & 0xFF));
  }

  private static void writeBytes(ByteArrayOutputStream out, byte[] b) {
    out.write(b, 0, b.length);
  }

  private static long readVarint64(byte[] buf, int p) {
    long result = 0;
    int shift = 0;
    while (true) {
      byte b = buf[p++];
      result |= ((long) (b & 0x7F)) << shift;
      if ((b & 0x80) == 0) {
        return result;
      }
      shift += 7;
      if (shift >= 64) {
        throw new IllegalStateException("varint too long");
      }
    }
  }

  private static int varintLen(long v) {
    int n = 1;
    while ((v & ~0x7FL) != 0L) {
      n++;
      v >>>= 7;
    }
    return n;
  }

  private static long readFixed64(byte[] buf, int p) {
    return ((long) (buf[p] & 0xFF))
        | ((long) (buf[p + 1] & 0xFF)) << 8
        | ((long) (buf[p + 2] & 0xFF)) << 16
        | ((long) (buf[p + 3] & 0xFF)) << 24
        | ((long) (buf[p + 4] & 0xFF)) << 32
        | ((long) (buf[p + 5] & 0xFF)) << 40
        | ((long) (buf[p + 6] & 0xFF)) << 48
        | ((long) (buf[p + 7] & 0xFF)) << 56;
  }

  /** Tracks a read offset through a byte[]. */
  private static final class Cursor {
    final byte[] buf;
    int pos;

    Cursor(byte[] buf) {
      this.buf = buf;
      this.pos = 0;
    }

    int readVarint32() {
      long v = readVarint64(buf, pos);
      pos += varintLen(v);
      return (int) v;
    }

    byte[] readBytes(int n) {
      byte[] out = new byte[n];
      System.arraycopy(buf, pos, out, 0, n);
      pos += n;
      return out;
    }
  }
}
