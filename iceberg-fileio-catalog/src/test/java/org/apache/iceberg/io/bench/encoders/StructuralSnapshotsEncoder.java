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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.InlineDeltaSnapshots;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.bench.MetadataDecoder;

/**
 * "Structural Snapshot Stream" — a purpose-built columnar encoding that exploits
 * Iceberg-specific redundancy gzip can't see:
 *
 * <ul>
 *   <li><b>parent_offset</b> instead of parent_id — for non-branched tables, parent
 *       is always the previous snapshot, encoded in 1 byte.
 *   <li><b>manifest-list template</b> — paths share a long prefix and follow
 *       {@code snap-{id}-{N}-{uuid}.avro}; we store the template once and per
 *       snapshot only the {N, UUID} pair.
 *   <li><b>summary key table</b> — keys are uniform across snapshots; emitted once.
 *   <li><b>summary values as varints</b> when numeric, instead of decimal-string JSON.
 *   <li><b>timestamp/sequence deltas</b> from a base, varint-packed.
 *   <li><b>schema-id RLE</b> — usually one run.
 * </ul>
 *
 * <p>The same idea is applied to the {@code metadata-log} array — entries
 * follow the {@code .../v{N}-{uuid}.metadata.json} pattern, so the path is
 * stored as prefix+suffix template plus per-entry {N, UUID}, and
 * {@code timestamp-ms} as deltas-from-base.
 *
 * <p>Wire format (gzip-wrapped):
 *
 * <pre>{@code
 *   varint  json_len
 *   bytes   json (TableMetadata minus "snapshots" and "metadata-log")
 *   varint  snap_block_len
 *   bytes   snap_block
 *   varint  mdlog_block_len
 *   bytes   mdlog_block
 * }</pre>
 *
 * <p>{@code snap_block}:
 *
 * <pre>{@code
 *   varint  N (snapshot count)
 *   byte    path_mode (0 = template, 1 = raw-with-shared-affixes)
 *   string  path_prefix
 *   string  path_suffix
 *   if path_mode = 0: N × (varint N_part, 16 bytes uuid_le)
 *   if path_mode = 1: N × length-prefixed variable_middle
 *   varint  summary_mode (0 = uniform-keys, 1 = per-snapshot-fallback)
 *   if uniform-keys:
 *     varint summary_key_count
 *     for each key: string key, byte type (0=int,1=string)
 *     for each key:
 *       if type=0: N × signed varint
 *       if type=1: N × length-prefixed string
 *   if per-snapshot-fallback:
 *     N × length-prefixed JSON of the summary
 *   N × fixed64 snapshot_id
 *   N × varint  parent_offset (0=null, k=k-th-prior; 1=common case)
 *   fixed64    timestamp_base
 *   N × signed varint  timestamp_delta
 *   fixed64    sequence_base
 *   N × signed varint  sequence_delta
 *   varint    schema_id_run_count
 *   schema_id_runs: pairs (varint count, signed varint schema_id) where schema_id<0 = absent
 *   N × byte   has_optional_bitmap (4 bits per snapshot: first_row_id, added_rows, key_id, reserved)
 *   per-snapshot optional fields, only emitted if the corresponding bit is set
 * }</pre>
 */
public final class StructuralSnapshotsEncoder implements MetadataDecoder {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // 16-byte canonical UUID-as-bytes used inside templated path entries.
  private static final int UUID_BYTES = 16;

  @Override
  public String name() {
    return "json-gzip+structural-snapshots";
  }

  // ---------------------------------------------------------------------------
  // Encode
  // ---------------------------------------------------------------------------

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
    root.remove("metadata-log");
    byte[] strippedJson;
    try {
      strippedJson = MAPPER.writeValueAsBytes(root);
    } catch (IOException e) {
      throw new RuntimeException("re-serialize stripped JSON failed", e);
    }

    byte[] snapBlock = encodeSnapshotsBlock(meta.snapshots());
    byte[] mdlogBlock = encodeMetadataLogBlock(meta.previousFiles());

    ByteArrayOutputStream wrapper = new ByteArrayOutputStream(
        strippedJson.length + snapBlock.length + mdlogBlock.length + 32);
    writeVarint(wrapper, strippedJson.length);
    writeBytes(wrapper, strippedJson);
    writeVarint(wrapper, snapBlock.length);
    writeBytes(wrapper, snapBlock);
    writeVarint(wrapper, mdlogBlock.length);
    writeBytes(wrapper, mdlogBlock);
    return EncoderUtil.gzip(wrapper.toByteArray());
  }

  static byte[] encodeSnapshotsBlock(List<Snapshot> snaps) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(snaps.size() * 64 + 256);
    int n = snaps.size();
    writeVarint(out, n);

    if (n == 0) {
      return out.toByteArray();
    }

    // Path encoding.
    PathPlan plan = planPaths(snaps);
    out.write(plan.mode);
    writeString(out, plan.commonPrefix);
    writeString(out, plan.commonSuffix);
    if (plan.mode == 0) {
      // Templated: N × (varint N_part, 16 bytes uuid)
      for (int i = 0; i < n; i++) {
        writeVarint(out, plan.nParts[i]);
        writeBytes(out, plan.uuids[i]);
      }
    } else {
      // Raw fallback: N × length-prefixed variable middle.
      for (int i = 0; i < n; i++) {
        writeString(out, plan.middles[i]);
      }
    }

    // Summary encoding.
    SummaryPlan sp = planSummary(snaps);
    if (sp.mode == 0) {
      out.write(0);
      writeVarint(out, sp.keys.size());
      for (int k = 0; k < sp.keys.size(); k++) {
        writeString(out, sp.keys.get(k));
        out.write(sp.intLike[k] ? 0 : 1);
      }
      for (int k = 0; k < sp.keys.size(); k++) {
        if (sp.intLike[k]) {
          for (int i = 0; i < n; i++) {
            writeSignedVarint(out, sp.intValues[k][i]);
          }
        } else {
          for (int i = 0; i < n; i++) {
            writeString(out, sp.stringValues[k][i] == null ? "" : sp.stringValues[k][i]);
          }
        }
      }
    } else {
      out.write(1);
      for (Snapshot s : snaps) {
        Map<String, String> summaryWithOp = new LinkedHashMap<>();
        if (s.operation() != null) {
          summaryWithOp.put("operation", s.operation());
        }
        if (s.summary() != null) {
          summaryWithOp.putAll(s.summary());
        }
        try {
          writeBytesField(out, MAPPER.writeValueAsBytes(summaryWithOp));
        } catch (IOException e) {
          throw new RuntimeException("summary fallback serialization failed", e);
        }
      }
    }

    // snapshot_id: fixed64 packed.
    long[] ids = new long[n];
    for (int i = 0; i < n; i++) {
      ids[i] = snaps.get(i).snapshotId();
      writeFixed64(out, ids[i]);
    }

    // parent_offset.
    Map<Long, Integer> idIndex = new java.util.HashMap<>(n * 2);
    for (int i = 0; i < n; i++) {
      idIndex.put(ids[i], i);
    }
    for (int i = 0; i < n; i++) {
      Long pid = snaps.get(i).parentId();
      if (pid == null) {
        writeVarint(out, 0); // null
      } else {
        Integer pidx = idIndex.get(pid);
        if (pidx != null && pidx < i) {
          // Encode as forward distance: 1 = previous, 2 = two back, etc.
          writeVarint(out, i - pidx);
        } else {
          // Parent not found in this list (shouldn't happen for valid TableMetadata),
          // or refers to a later index — fall back to absolute id via a special marker.
          // Use very large offset value followed by absolute id; tag = 0xFFFF (way larger than N).
          writeVarint(out, 0xFFFFFFFFL);
          writeFixed64(out, pid);
        }
      }
    }

    // timestamp_ms with delta-from-base.
    long tsBase = snaps.get(0).timestampMillis();
    writeFixed64(out, tsBase);
    long prev = tsBase;
    for (int i = 0; i < n; i++) {
      long ts = snaps.get(i).timestampMillis();
      writeSignedVarint(out, ts - prev);
      prev = ts;
    }

    // sequence_number with delta-from-base.
    long seqBase = snaps.get(0).sequenceNumber();
    writeFixed64(out, seqBase);
    prev = seqBase;
    for (int i = 0; i < n; i++) {
      long s = snaps.get(i).sequenceNumber();
      writeSignedVarint(out, s - prev);
      prev = s;
    }

    // schema_id RLE: pairs (count, schema_id) where schema_id < 0 means null.
    List<int[]> runs = rleSchema(snaps);
    writeVarint(out, runs.size());
    for (int[] run : runs) {
      writeVarint(out, run[0]); // count
      writeSignedVarint(out, run[1]); // schema_id, -1 for null
    }

    // Optional fields: presence bitmap (2 bits per snapshot covering first_row_id+added_rows+key_id;
    // we use 1 byte per snapshot here for simplicity — this is rarely populated so the overhead
    // is small in practice).
    for (Snapshot s : snaps) {
      int flags = 0;
      if (s.firstRowId() != null) flags |= 1;
      if (s.addedRows() != null) flags |= 2;
      if (s.keyId() != null) flags |= 4;
      out.write(flags);
    }
    for (Snapshot s : snaps) {
      if (s.firstRowId() != null) writeSignedVarint(out, s.firstRowId());
      if (s.addedRows() != null) writeSignedVarint(out, s.addedRows());
      if (s.keyId() != null) writeString(out, s.keyId());
    }

    // operation column: same vocabulary across snapshots typically; emit per-snapshot string.
    // (operation also lives in summary["operation"] when present, but is not always there for
    // fallback-mode summary; carry it explicitly here for safety.)
    for (Snapshot s : snaps) {
      writeString(out, s.operation() == null ? "" : s.operation());
    }

    return out.toByteArray();
  }

  // ---------------------------------------------------------------------------
  // metadata-log block (timestamp-deltas + path template)
  // ---------------------------------------------------------------------------

  static byte[] encodeMetadataLogBlock(List<MetadataLogEntry> entries) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(entries.size() * 32 + 64);
    int n = entries.size();
    writeVarint(out, n);
    if (n == 0) {
      return out.toByteArray();
    }

    String[] paths = new String[n];
    for (int i = 0; i < n; i++) {
      paths[i] = entries.get(i).file();
    }
    String prefix = longestCommonPrefix(paths);
    String suffix = longestCommonSuffix(paths, prefix.length());

    // Try "<int>-<uuid>" template.
    long[] versions = new long[n];
    byte[][] uuids = new byte[n][];
    boolean templated = true;
    for (int i = 0; i < n; i++) {
      String mid = paths[i].substring(prefix.length(), paths[i].length() - suffix.length());
      int dash = mid.indexOf('-');
      if (dash < 0) {
        templated = false;
        break;
      }
      try {
        versions[i] = Long.parseLong(mid.substring(0, dash));
      } catch (NumberFormatException e) {
        templated = false;
        break;
      }
      byte[] uuidBytes = parseUuidBytes(mid.substring(dash + 1));
      if (uuidBytes == null) {
        templated = false;
        break;
      }
      uuids[i] = uuidBytes;
    }

    out.write(templated ? 0 : 1);
    writeString(out, prefix);
    writeString(out, suffix);
    if (templated) {
      for (int i = 0; i < n; i++) {
        writeVarint(out, versions[i]);
        writeBytes(out, uuids[i]);
      }
    } else {
      for (int i = 0; i < n; i++) {
        String mid = paths[i].substring(prefix.length(), paths[i].length() - suffix.length());
        writeString(out, mid);
      }
    }

    long tsBase = entries.get(0).timestampMillis();
    writeFixed64(out, tsBase);
    long prev = tsBase;
    for (int i = 0; i < n; i++) {
      long ts = entries.get(i).timestampMillis();
      writeSignedVarint(out, ts - prev);
      prev = ts;
    }
    return out.toByteArray();
  }

  // ---------------------------------------------------------------------------
  // Path planning — detect prefix/suffix and "snap-{id}-{N}-{UUID}.avro" template
  // ---------------------------------------------------------------------------

  private static final class PathPlan {
    int mode; // 0 = template, 1 = raw fallback
    String commonPrefix;
    String commonSuffix;
    long[] nParts;        // mode 0
    byte[][] uuids;       // mode 0
    String[] middles;     // mode 1
  }

  private static PathPlan planPaths(List<Snapshot> snaps) {
    PathPlan p = new PathPlan();
    int n = snaps.size();
    String[] paths = new String[n];
    for (int i = 0; i < n; i++) {
      paths[i] = snaps.get(i).manifestListLocation() == null ? "" : snaps.get(i).manifestListLocation();
    }
    p.commonPrefix = longestCommonPrefix(paths);
    p.commonSuffix = longestCommonSuffix(paths, p.commonPrefix.length());

    // Try template: each variable middle should be "<id_decimal>-<N>-<UUID>".
    long[] nParts = new long[n];
    byte[][] uuids = new byte[n][];
    boolean templated = true;
    for (int i = 0; i < n; i++) {
      String mid = paths[i].substring(p.commonPrefix.length(),
          paths[i].length() - p.commonSuffix.length());
      String idStr = Long.toString(snaps.get(i).snapshotId());
      if (!mid.startsWith(idStr + "-")) {
        templated = false;
        break;
      }
      String afterId = mid.substring(idStr.length() + 1); // strip "<id>-"
      int dash = afterId.indexOf('-');
      if (dash < 0) {
        templated = false;
        break;
      }
      String nStr = afterId.substring(0, dash);
      String uuidStr = afterId.substring(dash + 1);
      try {
        nParts[i] = Long.parseLong(nStr);
      } catch (NumberFormatException e) {
        templated = false;
        break;
      }
      byte[] uuidBytes = parseUuidBytes(uuidStr);
      if (uuidBytes == null) {
        templated = false;
        break;
      }
      uuids[i] = uuidBytes;
    }

    if (templated) {
      p.mode = 0;
      p.nParts = nParts;
      p.uuids = uuids;
    } else {
      p.mode = 1;
      String[] middles = new String[n];
      for (int i = 0; i < n; i++) {
        middles[i] = paths[i].substring(p.commonPrefix.length(),
            paths[i].length() - p.commonSuffix.length());
      }
      p.middles = middles;
    }
    return p;
  }

  private static String longestCommonPrefix(String[] paths) {
    if (paths.length == 0) return "";
    String first = paths[0];
    int len = first.length();
    for (int i = 1; i < paths.length; i++) {
      String s = paths[i];
      int j = 0;
      int min = Math.min(len, s.length());
      while (j < min && first.charAt(j) == s.charAt(j)) j++;
      len = j;
      if (len == 0) break;
    }
    return first.substring(0, len);
  }

  private static String longestCommonSuffix(String[] paths, int prefixGuard) {
    if (paths.length == 0) return "";
    String first = paths[0];
    int firstLen = first.length();
    int suffixLen = firstLen - prefixGuard;
    for (int i = 1; i < paths.length; i++) {
      String s = paths[i];
      int j = 0;
      int max = Math.min(suffixLen, s.length() - prefixGuard);
      while (j < max
          && first.charAt(firstLen - 1 - j) == s.charAt(s.length() - 1 - j)) {
        j++;
      }
      suffixLen = j;
      if (suffixLen == 0) break;
    }
    return first.substring(firstLen - suffixLen);
  }

  private static byte[] parseUuidBytes(String s) {
    if (s == null || s.length() != 36) return null;
    if (s.charAt(8) != '-' || s.charAt(13) != '-' || s.charAt(18) != '-' || s.charAt(23) != '-') {
      return null;
    }
    byte[] out = new byte[UUID_BYTES];
    int p = 0;
    for (int i = 0; i < 36; i++) {
      char c = s.charAt(i);
      if (c == '-') continue;
      int hi = hexVal(c);
      char c2 = s.charAt(++i);
      int lo = hexVal(c2);
      if (hi < 0 || lo < 0) return null;
      out[p++] = (byte) ((hi << 4) | lo);
    }
    return out;
  }

  private static String formatUuid(byte[] u) {
    StringBuilder sb = new StringBuilder(36);
    for (int i = 0; i < UUID_BYTES; i++) {
      if (i == 4 || i == 6 || i == 8 || i == 10) sb.append('-');
      sb.append(Character.forDigit((u[i] >> 4) & 0xF, 16));
      sb.append(Character.forDigit(u[i] & 0xF, 16));
    }
    return sb.toString();
  }

  private static int hexVal(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
  }

  // ---------------------------------------------------------------------------
  // Summary planning — detect uniform key set + per-key int-vs-string typing
  // ---------------------------------------------------------------------------

  private static final class SummaryPlan {
    int mode; // 0 = uniform-keys, 1 = per-snapshot fallback
    List<String> keys;
    boolean[] intLike;
    long[][] intValues;
    String[][] stringValues;
  }

  private static SummaryPlan planSummary(List<Snapshot> snaps) {
    SummaryPlan p = new SummaryPlan();
    int n = snaps.size();
    if (n == 0) {
      p.mode = 0;
      p.keys = new ArrayList<>();
      return p;
    }
    // Use the first snapshot's summary keys as the candidate template (insertion-ordered).
    Snapshot first = snaps.get(0);
    Set<String> firstKeys = new LinkedHashSet<>();
    if (first.operation() != null) firstKeys.add("operation");
    if (first.summary() != null) firstKeys.addAll(first.summary().keySet());

    boolean uniform = true;
    for (Snapshot s : snaps) {
      Set<String> keys = new LinkedHashSet<>();
      if (s.operation() != null) keys.add("operation");
      if (s.summary() != null) keys.addAll(s.summary().keySet());
      if (!keys.equals(firstKeys)) {
        uniform = false;
        break;
      }
    }

    if (!uniform) {
      p.mode = 1;
      return p;
    }

    p.mode = 0;
    p.keys = new ArrayList<>(firstKeys);
    int k = p.keys.size();
    p.intLike = new boolean[k];
    p.intValues = new long[k][];
    p.stringValues = new String[k][];
    for (int ki = 0; ki < k; ki++) {
      String key = p.keys.get(ki);
      // Decide int-vs-string by trying every value.
      boolean allInt = true;
      long[] ints = new long[n];
      String[] strs = new String[n];
      for (int i = 0; i < n; i++) {
        Snapshot s = snaps.get(i);
        String v = "operation".equals(key)
            ? s.operation()
            : (s.summary() == null ? null : s.summary().get(key));
        strs[i] = v == null ? "" : v;
        if (v == null) {
          allInt = false;
        } else {
          try {
            ints[i] = Long.parseLong(v);
          } catch (NumberFormatException e) {
            allInt = false;
          }
        }
      }
      p.intLike[ki] = allInt;
      if (allInt) p.intValues[ki] = ints;
      else p.stringValues[ki] = strs;
    }
    return p;
  }

  private static List<int[]> rleSchema(List<Snapshot> snaps) {
    List<int[]> runs = new ArrayList<>();
    int curId = Integer.MIN_VALUE;
    int curCount = 0;
    for (Snapshot s : snaps) {
      int sid = s.schemaId() == null ? -1 : s.schemaId();
      if (sid == curId) {
        curCount++;
      } else {
        if (curCount > 0) runs.add(new int[]{curCount, curId});
        curId = sid;
        curCount = 1;
      }
    }
    if (curCount > 0) runs.add(new int[]{curCount, curId});
    return runs;
  }

  // ---------------------------------------------------------------------------
  // Decode
  // ---------------------------------------------------------------------------

  @Override
  public TableMetadata decodeFull(byte[] encoded) {
    Decoded d = decode(encoded, /* lazyOnly */ false);
    ObjectNode root;
    try {
      root = (ObjectNode) MAPPER.readTree(d.strippedJson);
    } catch (IOException e) {
      throw new RuntimeException("decodeFull: parse stripped JSON failed", e);
    }
    ArrayNode snapsArr = MAPPER.createArrayNode();
    for (int i = 0; i < d.snapshotCount; i++) {
      snapsArr.add(buildSnapshotJson(d, i));
    }
    root.set("snapshots", snapsArr);

    if (d.mdlogCount > 0) {
      ArrayNode mdArr = MAPPER.createArrayNode();
      for (int i = 0; i < d.mdlogCount; i++) {
        ObjectNode entry = MAPPER.createObjectNode();
        entry.put("timestamp-ms", d.mdlogTimestamps[i]);
        entry.put("metadata-file", reconstructMdlogPath(d, i));
        mdArr.add(entry);
      }
      root.set("metadata-log", mdArr);
    }

    return TableMetadataParser.fromJson(root.toString());
  }

  private static String reconstructMdlogPath(Decoded d, int i) {
    if (d.mdlogPathMode == 0) {
      return d.mdlogPrefix + d.mdlogVersions[i] + "-" + formatUuid(d.mdlogUuids[i]) + d.mdlogSuffix;
    } else {
      return d.mdlogPrefix + d.mdlogMiddles[i] + d.mdlogSuffix;
    }
  }

  @Override
  public Snapshot decodeCurrentSnapshotOnly(byte[] encoded) {
    Decoded d = decode(encoded, /* lazyOnly */ true);
    Long currentId = d.currentSnapshotId;
    if (currentId == null) return null;
    for (int i = 0; i < d.snapshotCount; i++) {
      if (d.snapshotIds[i] == currentId) {
        return buildSnapshot(d, i);
      }
    }
    return null;
  }

  @Override
  public List<Snapshot> decodeRecentSnapshots(byte[] encoded, int k) {
    Decoded d = decode(encoded, /* lazyOnly */ true);
    Long currentId = d.currentSnapshotId;
    if (currentId == null) return new ArrayList<>();

    // Locate current id index in snapshotIds[].
    int curIdx = -1;
    for (int i = d.snapshotCount - 1; i >= 0; i--) {
      if (d.snapshotIds[i] == currentId) {
        curIdx = i;
        break;
      }
    }
    if (curIdx < 0) return new ArrayList<>();

    List<Snapshot> out = new ArrayList<>(k);
    int idx = curIdx;
    for (int i = 0; i < k && idx >= 0; i++) {
      out.add(buildSnapshot(d, idx));
      long offset = d.parentOffsets[idx];
      if (offset == 0L) break;          // no parent
      if (offset == 0xFFFFFFFFL) break; // absolute parent (rare); skip walk
      idx -= (int) offset;
    }
    return out;
  }

  private static final class Decoded {
    byte[] strippedJson;
    Long currentSnapshotId;

    int snapshotCount;
    int pathMode;
    String pathPrefix;
    String pathSuffix;
    long[] nParts;
    byte[][] uuids;
    String[] middles;

    int summaryMode;
    List<String> summaryKeys;
    boolean[] summaryIntLike;
    long[][] summaryInts;
    String[][] summaryStrings;
    String[] summaryFallbackJson;

    long[] snapshotIds;
    long[] parentOffsets;
    long[] absoluteParents; // parallel to snapshotIds; 0 if not used
    long timestampBase;
    long[] timestampDeltas;
    long sequenceBase;
    long[] sequenceDeltas;
    int[] schemaIds;          // per-index, -1 = null
    int[] optFlags;           // per-index
    Long[] firstRowIds;       // null if not present
    Long[] addedRowsArr;
    String[] keyIds;
    String[] operations;

    // metadata-log block (lazy-decoded only when decodeFull is called)
    int mdlogCount;
    int mdlogPathMode;
    String mdlogPrefix;
    String mdlogSuffix;
    long[] mdlogVersions;
    byte[][] mdlogUuids;
    String[] mdlogMiddles;
    long[] mdlogTimestamps;
  }

  private Decoded decode(byte[] encoded, boolean lazyOnly) {
    byte[] inner = EncoderUtil.gunzip(encoded);
    Cursor c = new Cursor(inner);
    Decoded d = new Decoded();

    int jsonLen = c.readVarint32();
    d.strippedJson = c.readBytes(jsonLen);
    if (lazyOnly) {
      try {
        JsonNode root = MAPPER.readTree(d.strippedJson);
        JsonNode cur = root.get("current-snapshot-id");
        d.currentSnapshotId = (cur == null || cur.isNull()) ? null : cur.asLong();
      } catch (IOException e) {
        throw new RuntimeException("readCurrentSnapshotId failed", e);
      }
    }

    int snapBlockLen = c.readVarint32();
    int blockEnd = c.pos + snapBlockLen;

    d.snapshotCount = c.readVarint32();
    if (d.snapshotCount == 0) {
      c.pos = blockEnd;
      return d;
    }

    d.pathMode = c.buf[c.pos++] & 0xFF;
    d.pathPrefix = c.readString();
    d.pathSuffix = c.readString();
    if (d.pathMode == 0) {
      d.nParts = new long[d.snapshotCount];
      d.uuids = new byte[d.snapshotCount][];
      for (int i = 0; i < d.snapshotCount; i++) {
        d.nParts[i] = c.readVarint64();
        d.uuids[i] = c.readBytes(UUID_BYTES);
      }
    } else {
      d.middles = new String[d.snapshotCount];
      for (int i = 0; i < d.snapshotCount; i++) {
        d.middles[i] = c.readString();
      }
    }

    d.summaryMode = c.buf[c.pos++] & 0xFF;
    if (d.summaryMode == 0) {
      int kn = c.readVarint32();
      d.summaryKeys = new ArrayList<>(kn);
      d.summaryIntLike = new boolean[kn];
      for (int ki = 0; ki < kn; ki++) {
        d.summaryKeys.add(c.readString());
        d.summaryIntLike[ki] = (c.buf[c.pos++] & 0xFF) == 0;
      }
      d.summaryInts = new long[kn][];
      d.summaryStrings = new String[kn][];
      for (int ki = 0; ki < kn; ki++) {
        if (d.summaryIntLike[ki]) {
          long[] vs = new long[d.snapshotCount];
          for (int i = 0; i < d.snapshotCount; i++) {
            vs[i] = c.readSignedVarint();
          }
          d.summaryInts[ki] = vs;
        } else {
          String[] vs = new String[d.snapshotCount];
          for (int i = 0; i < d.snapshotCount; i++) {
            vs[i] = c.readString();
          }
          d.summaryStrings[ki] = vs;
        }
      }
    } else {
      d.summaryFallbackJson = new String[d.snapshotCount];
      for (int i = 0; i < d.snapshotCount; i++) {
        int len = c.readVarint32();
        d.summaryFallbackJson[i] = new String(c.buf, c.pos, len, StandardCharsets.UTF_8);
        c.pos += len;
      }
    }

    d.snapshotIds = new long[d.snapshotCount];
    for (int i = 0; i < d.snapshotCount; i++) {
      d.snapshotIds[i] = c.readFixed64();
    }

    d.parentOffsets = new long[d.snapshotCount];
    d.absoluteParents = new long[d.snapshotCount];
    for (int i = 0; i < d.snapshotCount; i++) {
      long off = c.readVarint64();
      d.parentOffsets[i] = off;
      if (off == 0xFFFFFFFFL) {
        d.absoluteParents[i] = c.readFixed64();
      }
    }

    d.timestampBase = c.readFixed64();
    d.timestampDeltas = new long[d.snapshotCount];
    long prev = d.timestampBase;
    for (int i = 0; i < d.snapshotCount; i++) {
      long delta = c.readSignedVarint();
      prev += delta;
      d.timestampDeltas[i] = prev;
    }

    d.sequenceBase = c.readFixed64();
    d.sequenceDeltas = new long[d.snapshotCount];
    prev = d.sequenceBase;
    for (int i = 0; i < d.snapshotCount; i++) {
      long delta = c.readSignedVarint();
      prev += delta;
      d.sequenceDeltas[i] = prev;
    }

    int runCount = c.readVarint32();
    d.schemaIds = new int[d.snapshotCount];
    int idx = 0;
    for (int r = 0; r < runCount; r++) {
      int count = c.readVarint32();
      int sid = (int) c.readSignedVarint();
      for (int j = 0; j < count; j++) {
        d.schemaIds[idx++] = sid;
      }
    }

    d.optFlags = new int[d.snapshotCount];
    for (int i = 0; i < d.snapshotCount; i++) {
      d.optFlags[i] = c.buf[c.pos++] & 0xFF;
    }
    d.firstRowIds = new Long[d.snapshotCount];
    d.addedRowsArr = new Long[d.snapshotCount];
    d.keyIds = new String[d.snapshotCount];
    for (int i = 0; i < d.snapshotCount; i++) {
      int flags = d.optFlags[i];
      if ((flags & 1) != 0) d.firstRowIds[i] = c.readSignedVarint();
      if ((flags & 2) != 0) d.addedRowsArr[i] = c.readSignedVarint();
      if ((flags & 4) != 0) d.keyIds[i] = c.readString();
    }

    d.operations = new String[d.snapshotCount];
    for (int i = 0; i < d.snapshotCount; i++) {
      d.operations[i] = c.readString();
    }

    // metadata-log block follows. For lazy paths, just skip past it.
    int mdlogBlockLen = c.readVarint32();
    int mdlogEnd = c.pos + mdlogBlockLen;
    if (lazyOnly) {
      c.pos = mdlogEnd;
      return d;
    }
    d.mdlogCount = c.readVarint32();
    if (d.mdlogCount > 0) {
      d.mdlogPathMode = c.buf[c.pos++] & 0xFF;
      d.mdlogPrefix = c.readString();
      d.mdlogSuffix = c.readString();
      if (d.mdlogPathMode == 0) {
        d.mdlogVersions = new long[d.mdlogCount];
        d.mdlogUuids = new byte[d.mdlogCount][];
        for (int i = 0; i < d.mdlogCount; i++) {
          d.mdlogVersions[i] = c.readVarint64();
          d.mdlogUuids[i] = c.readBytes(UUID_BYTES);
        }
      } else {
        d.mdlogMiddles = new String[d.mdlogCount];
        for (int i = 0; i < d.mdlogCount; i++) {
          d.mdlogMiddles[i] = c.readString();
        }
      }
      long base = c.readFixed64();
      d.mdlogTimestamps = new long[d.mdlogCount];
      long mprev = base;
      for (int i = 0; i < d.mdlogCount; i++) {
        long delta = c.readSignedVarint();
        mprev += delta;
        d.mdlogTimestamps[i] = mprev;
      }
    }

    return d;
  }

  // ---------------------------------------------------------------------------
  // Snapshot reconstruction (per-index)
  // ---------------------------------------------------------------------------

  private static String reconstructPath(Decoded d, int i) {
    if (d.pathMode == 0) {
      return d.pathPrefix + d.snapshotIds[i] + "-" + d.nParts[i] + "-" + formatUuid(d.uuids[i])
          + d.pathSuffix;
    } else {
      return d.pathPrefix + d.middles[i] + d.pathSuffix;
    }
  }

  private static Map<String, String> reconstructSummary(Decoded d, int i) {
    if (d.summaryMode == 1) {
      try {
        Map<String, String> result = new LinkedHashMap<>();
        JsonNode node = MAPPER.readTree(d.summaryFallbackJson[i]);
        node.fields().forEachRemaining(e -> result.put(e.getKey(), e.getValue().asText()));
        return result;
      } catch (IOException e) {
        throw new RuntimeException("summary fallback decode failed", e);
      }
    }
    Map<String, String> out = new LinkedHashMap<>();
    for (int ki = 0; ki < d.summaryKeys.size(); ki++) {
      String key = d.summaryKeys.get(ki);
      String value;
      if (d.summaryIntLike[ki]) {
        value = Long.toString(d.summaryInts[ki][i]);
      } else {
        value = d.summaryStrings[ki][i];
      }
      out.put(key, value);
    }
    return out;
  }

  private static Snapshot buildSnapshot(Decoded d, int i) {
    Map<String, String> summary = reconstructSummary(d, i);
    // operation may be missing from summary in fallback mode; ensure key present using the
    // explicit operation column.
    summary.putIfAbsent("operation", d.operations[i]);
    int sid = d.schemaIds[i];
    Integer schemaId = sid < 0 ? null : sid;
    return InlineDeltaSnapshots.create(
        d.sequenceDeltas[i],
        d.snapshotIds[i],
        resolveParent(d, i),
        d.timestampDeltas[i],
        summary,
        schemaId,
        reconstructPath(d, i),
        d.firstRowIds[i],
        d.addedRowsArr[i],
        d.keyIds[i]);
  }

  private static Long resolveParent(Decoded d, int i) {
    long off = d.parentOffsets[i];
    if (off == 0L) return null;
    if (off == 0xFFFFFFFFL) return d.absoluteParents[i];
    int target = i - (int) off;
    if (target < 0) return null;
    return d.snapshotIds[target];
  }

  private static ObjectNode buildSnapshotJson(Decoded d, int i) {
    ObjectNode snap = MAPPER.createObjectNode();
    snap.put("snapshot-id", d.snapshotIds[i]);
    snap.put("sequence-number", d.sequenceDeltas[i]);
    Long parent = resolveParent(d, i);
    if (parent != null) snap.put("parent-snapshot-id", parent);
    snap.put("timestamp-ms", d.timestampDeltas[i]);

    ObjectNode summary = MAPPER.createObjectNode();
    Map<String, String> recon = reconstructSummary(d, i);
    // Ensure operation is the first key written (matches Iceberg's canonical order).
    if (recon.containsKey("operation")) {
      summary.put("operation", recon.get("operation"));
    } else if (d.operations[i] != null && !d.operations[i].isEmpty()) {
      summary.put("operation", d.operations[i]);
    }
    for (Map.Entry<String, String> e : recon.entrySet()) {
      if (!"operation".equals(e.getKey())) {
        summary.put(e.getKey(), e.getValue());
      }
    }
    snap.set("summary", summary);

    String path = reconstructPath(d, i);
    if (!path.isEmpty()) snap.put("manifest-list", path);

    int sid = d.schemaIds[i];
    if (sid >= 0) snap.put("schema-id", sid);
    if (d.firstRowIds[i] != null) snap.put("first-row-id", d.firstRowIds[i]);
    if (d.addedRowsArr[i] != null) snap.put("added-rows", d.addedRowsArr[i]);
    if (d.keyIds[i] != null) snap.put("key-id", d.keyIds[i]);
    return snap;
  }

  // ---------------------------------------------------------------------------
  // Wire primitives (zigzag-signed varints + standard varints + fixed64)
  // ---------------------------------------------------------------------------

  private static void writeVarint(ByteArrayOutputStream out, long v) {
    while ((v & ~0x7FL) != 0L) {
      out.write((int) ((v & 0x7F) | 0x80));
      v >>>= 7;
    }
    out.write((int) v);
  }

  private static void writeSignedVarint(ByteArrayOutputStream out, long v) {
    writeVarint(out, (v << 1) ^ (v >> 63));
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

  private static void writeString(ByteArrayOutputStream out, String s) {
    byte[] b = s.getBytes(StandardCharsets.UTF_8);
    writeVarint(out, b.length);
    writeBytes(out, b);
  }

  private static void writeBytesField(ByteArrayOutputStream out, byte[] b) {
    writeVarint(out, b.length);
    writeBytes(out, b);
  }

  private static final class Cursor {
    final byte[] buf;
    int pos;

    Cursor(byte[] buf) {
      this.buf = buf;
      this.pos = 0;
    }

    int readVarint32() {
      return (int) readVarint64();
    }

    long readVarint64() {
      long result = 0;
      int shift = 0;
      while (true) {
        byte b = buf[pos++];
        result |= ((long) (b & 0x7F)) << shift;
        if ((b & 0x80) == 0) return result;
        shift += 7;
        if (shift >= 64) throw new IllegalStateException("varint overflow");
      }
    }

    long readSignedVarint() {
      long u = readVarint64();
      return (u >>> 1) ^ -(u & 1L);
    }

    long readFixed64() {
      long v = ((long) (buf[pos] & 0xFF))
          | ((long) (buf[pos + 1] & 0xFF)) << 8
          | ((long) (buf[pos + 2] & 0xFF)) << 16
          | ((long) (buf[pos + 3] & 0xFF)) << 24
          | ((long) (buf[pos + 4] & 0xFF)) << 32
          | ((long) (buf[pos + 5] & 0xFF)) << 40
          | ((long) (buf[pos + 6] & 0xFF)) << 48
          | ((long) (buf[pos + 7] & 0xFF)) << 56;
      pos += 8;
      return v;
    }

    byte[] readBytes(int n) {
      byte[] out = new byte[n];
      System.arraycopy(buf, pos, out, 0, n);
      pos += n;
      return out;
    }

    String readString() {
      int len = readVarint32();
      String s = new String(buf, pos, len, StandardCharsets.UTF_8);
      pos += len;
      return s;
    }
  }
}
