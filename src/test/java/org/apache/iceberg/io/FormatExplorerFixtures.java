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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Generates byte-accurate wire-format fixtures for the docs/format-explorer
 * visualizer. Each scenario builds a real catalog file (header + checkpoint +
 * appended transactions) by calling {@link ProtoCodec} and
 * {@link InlineDeltaCodec} directly, then emits a JSON descriptor consumed by
 * the JS decoder in {@code docs/format-explorer/index.html}.
 *
 * <p>The generator also parses {@code catalog.proto} into a schema descriptor
 * so the visualizer can render every field's name, type, and leading comment
 * without hand-transcribing the schema. A pre-flight check asserts that field
 * numbers in the {@code .proto} agree with the {@code private static final
 * int} constants in {@code ProtoCodec.java} -- so wire/schema drift surfaces
 * as a build failure here rather than as silently wrong tooltips.
 *
 * <p>This class is excluded from the default test run (system-property gated).
 * To regenerate fixtures:
 * <pre>
 *   mvn test -Dtest=FormatExplorerFixtures -Dexplorer.regenerate=true
 *   docs/format-explorer/splice-fixtures.py
 * </pre>
 *
 * <p>Output: {@code target/format-explorer/fixtures.json}.
 */
public class FormatExplorerFixtures {

  private static final InputFile LOCATION = new MemInputFile("test://format-explorer");
  private static final UUID FIXED_CATALOG_UUID =
      UUID.fromString("01900000-0000-7000-8000-000000000001");
  private static final Path PROJECT_ROOT = Paths.get(".").toAbsolutePath().normalize();
  private static final Path PROTO_FILE =
      PROJECT_ROOT.resolve("src/main/proto/catalog.proto");
  private static final Path CODEC_FILE =
      PROJECT_ROOT.resolve("src/main/java/org/apache/iceberg/io/ProtoCodec.java");
  private static final Path INLINE_DELTA_FILE =
      PROJECT_ROOT.resolve("src/main/java/org/apache/iceberg/io/InlineDeltaCodec.java");
  private static final Path OUTPUT =
      PROJECT_ROOT.resolve("target/format-explorer/fixtures.json");

  @Test
  @EnabledIfSystemProperty(named = "explorer.regenerate", matches = "true")
  void regenerate() throws Exception {
    SchemaDescriptor schema = parseProto(PROTO_FILE);
    verifyFieldNumbersAgainstCodec(schema, CODEC_FILE, INLINE_DELTA_FILE);

    List<Scenario> scenarios = new ArrayList<>();
    scenarios.add(scenarioEmptyBootstrap());
    scenarios.add(scenarioPostCasMultiNamespace());
    scenarios.add(scenarioLateBindMultiAction());
    scenarios.add(scenarioCreateTableInline());
    scenarios.add(scenarioUpdateTableInlineDelta());
    scenarios.add(scenarioPointerMultiTableConflictRetry());
    scenarios.add(scenarioInlineMultiTableConflictRetry());

    for (Scenario s : scenarios) {
      verifyParses(s);
    }
    // The two conflict/retry scenarios are designed to exercise the full
    // read path (verify + apply). Their wire bytes must round-trip through
    // ProtoCatalogFormat.readInternal with the documented outcomes.
    verifyConflictRetryReplay(
        scenarios.get(5), /* aSucceeds */ true, /* bAtV2 */ "s3://lake/warehouse/B/v2.metadata.json",
        "s3://lake/warehouse/A/v3.metadata.json");
    verifyInlineConflictRetryReplay(scenarios.get(6));

    Files.createDirectories(OUTPUT.getParent());
    Files.writeString(OUTPUT, emitJson(schema, scenarios));
    System.out.println("Wrote fixtures to " + OUTPUT);
  }

  /**
   * Sanity check: each scenario's bytes must decode at the wire-format level.
   * Header + checkpoint are decoded via {@link ProtoCodec#decodeCheckpoint},
   * each transaction record via {@link ProtoCodec#decodeTransaction}. We
   * deliberately do NOT exercise the apply/replay path
   * ({@link ProtoCatalogFormat#readInternal}) because that requires the
   * carried inline metadata to be a fully-valid Iceberg TableMetadata --
   * which is orthogonal to "the bytes are well-formed protobuf the
   * visualizer can decode".
   */
  private static void verifyParses(Scenario s) {
    try {
      java.io.ByteArrayInputStream in = new java.io.ByteArrayInputStream(s.bytes);
      byte[] header = in.readNBytes(8);
      if (header.length != 8 || header[0] != 'L' || header[1] != 'C'
          || header[2] != 'A' || header[3] != 'T') {
        throw new IllegalStateException("Bad magic");
      }
      int chkLen = readVarintFromStream(in);
      byte[] chkBytes = in.readNBytes(chkLen);
      ProtoCatalogFile.Builder builder = ProtoCatalogFile.builder(LOCATION);
      ProtoCodec.decodeCheckpoint(chkBytes, builder);

      while (in.available() > 0) {
        int txnLen = readVarintFromStream(in);
        byte[] txnBytes = in.readNBytes(txnLen);
        ProtoCodec.Transaction txn = ProtoCodec.decodeTransaction(txnBytes);
        if (txn == null) {
          throw new IllegalStateException("Transaction decoded as null");
        }
      }
    } catch (Exception e) {
      throw new AssertionError(
          "Scenario " + s.name + " failed to wire-decode: " + e.getMessage(), e);
    }
  }

  /**
   * For the pointer-mode multi-table scenario: actually exercise the read
   * path and assert T1+T3 commit while T2 is silently skipped. This is the
   * strongest correctness guarantee the test can offer for the scenario.
   */
  private static void verifyConflictRetryReplay(
      Scenario s, boolean aSucceedsT3, String bExpectedLocationT3, String aExpectedLocationT3)
      throws IOException {
    ProtoCatalogFile result = ProtoCatalogFormat.readInternal(
        LOCATION,
        new java.io.ByteArrayInputStream(s.bytes),
        s.bytes.length);

    java.util.UUID t1 = java.util.UUID.fromString("01900000-0000-7000-8000-000000000071");
    java.util.UUID t2 = java.util.UUID.fromString("01900000-0000-7000-8000-000000000072");
    java.util.UUID t3 = java.util.UUID.fromString("01900000-0000-7000-8000-000000000073");
    if (!result.containsTransaction(t1)) {
      throw new AssertionError(s.name + ": T1 expected to commit, but didn't");
    }
    if (result.containsTransaction(t2)) {
      throw new AssertionError(s.name + ": T2 expected to be rejected, but was committed");
    }
    if (!result.containsTransaction(t3)) {
      throw new AssertionError(s.name + ": T3 expected to commit, but didn't");
    }
    org.apache.iceberg.catalog.Namespace warehouse =
        org.apache.iceberg.catalog.Namespace.of("warehouse");
    String aLoc = result.location(
        org.apache.iceberg.catalog.TableIdentifier.of(warehouse, "A"));
    String bLoc = result.location(
        org.apache.iceberg.catalog.TableIdentifier.of(warehouse, "B"));
    if (!aExpectedLocationT3.equals(aLoc)) {
      throw new AssertionError(s.name + ": A.location = " + aLoc
          + ", expected " + aExpectedLocationT3);
    }
    if (!bExpectedLocationT3.equals(bLoc)) {
      throw new AssertionError(s.name + ": B.location = " + bLoc
          + ", expected " + bExpectedLocationT3);
    }
  }

  /**
   * Same outcomes as {@link #verifyConflictRetryReplay} but for the
   * inline scenario: assert each table's inline metadata bytes match the
   * post-T3 expectation.
   */
  private static void verifyInlineConflictRetryReplay(Scenario s) throws IOException {
    ProtoCatalogFile result = ProtoCatalogFormat.readInternal(
        LOCATION,
        new java.io.ByteArrayInputStream(s.bytes),
        s.bytes.length);

    java.util.UUID t1 = java.util.UUID.fromString("01900000-0000-7000-8000-000000000081");
    java.util.UUID t2 = java.util.UUID.fromString("01900000-0000-7000-8000-000000000082");
    java.util.UUID t3 = java.util.UUID.fromString("01900000-0000-7000-8000-000000000083");
    if (!result.containsTransaction(t1) || result.containsTransaction(t2)
        || !result.containsTransaction(t3)) {
      throw new AssertionError(s.name + ": expected commit set {T1,T3}, got "
          + "T1=" + result.containsTransaction(t1)
          + " T2=" + result.containsTransaction(t2)
          + " T3=" + result.containsTransaction(t3));
    }
    org.apache.iceberg.catalog.Namespace warehouse =
        org.apache.iceberg.catalog.Namespace.of("warehouse");
    Integer aId = result.tableId(
        org.apache.iceberg.catalog.TableIdentifier.of(warehouse, "A"));
    Integer bId = result.tableId(
        org.apache.iceberg.catalog.TableIdentifier.of(warehouse, "B"));
    TableMetadata aMeta = TableMetadataParser.fromJson(
        new String(result.inlineMetadata(aId), StandardCharsets.UTF_8));
    TableMetadata bMeta = TableMetadataParser.fromJson(
        new String(result.inlineMetadata(bId), StandardCharsets.UTF_8));
    // After T3 commits, A holds {S1A, S3A} with main->S3A; B holds {S3B} with
    // main->S3B. Neither carries the snapshots from the rejected T2.
    long aT3Snap = 0x3333333333333333L;
    long bT3Snap = 0x3333333333333334L;
    long aT2StaleSnap = 0x2222222222222222L;
    long bT2StaleSnap = 0x2222222222222223L;
    if (aMeta.currentSnapshot() == null || aMeta.currentSnapshot().snapshotId() != aT3Snap) {
      throw new AssertionError(s.name + ": A.currentSnapshot != S3A; got " + aMeta.currentSnapshot());
    }
    if (bMeta.currentSnapshot() == null || bMeta.currentSnapshot().snapshotId() != bT3Snap) {
      throw new AssertionError(s.name + ": B.currentSnapshot != S3B; got " + bMeta.currentSnapshot());
    }
    if (aMeta.snapshot(aT2StaleSnap) != null) {
      throw new AssertionError(s.name + ": A retained the rejected T2 snapshot");
    }
    if (bMeta.snapshot(bT2StaleSnap) != null) {
      throw new AssertionError(s.name + ": B retained the rejected T2 snapshot");
    }
  }

  private static int readVarintFromStream(java.io.InputStream in) throws IOException {
    int result = 0;
    int shift = 0;
    int b;
    do {
      b = in.read();
      if (b < 0) {
        throw new IOException("EOF reading varint");
      }
      result |= (b & 0x7F) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    return result;
  }

  // ============================================================
  // Scenarios
  // ============================================================

  /**
   * Minimum viable catalog: header + a 1-namespace checkpoint + one txn that
   * creates a single namespace. Teaches header bytes, length framing, and
   * the {@code Action} oneof discriminator.
   */
  private Scenario scenarioEmptyBootstrap() {
    ProtoCatalogFile catalog = ProtoCatalogFile.builder(LOCATION)
        .setCatalogUuid(FIXED_CATALOG_UUID)
        .setNextNamespaceId(1)
        .setNextTableId(1)
        .build();
    byte[] file = encodeFile(catalog);
    UUID txnId = UUID.fromString("01900000-0000-7000-8000-000000000010");
    ProtoCodec.Transaction txn = new ProtoCodec.Transaction(
        txnId,
        List.of(new ProtoCodec.CreateNamespaceAction(1, 1, 0, -1, "analytics")));
    file = appendTxn(file, txn);

    List<Note> notes = List.of(
        new Note(0, 4, "Magic bytes \"LCAT\""),
        new Note(4, 8, "Format version (big-endian int32 = 1)"));
    return new Scenario(
        "empty-bootstrap",
        "Smallest realistic catalog: 8-byte header, a checkpoint that is "
            + "essentially just a UUID + ID counters, and a single transaction "
            + "with a single CreateNamespace action. Walk every byte to learn "
            + "how length framing and oneof discriminators work.",
        file, notes);
  }

  /**
   * Post-CAS catalog: 2 namespaces (one nested), 1 pointer-mode table, and
   * a namespace property. No appended transactions -- this is what a fresh
   * compaction commit looks like.
   */
  private Scenario scenarioPostCasMultiNamespace() {
    ProtoCatalogFile catalog = ProtoCatalogFile.builder(LOCATION)
        .setCatalogUuid(FIXED_CATALOG_UUID)
        .addNamespace(1, 0, "warehouse", 2)
        .addNamespace(2, 1, "bronze", 1)
        .addTable(1, 2, "events", 3, "s3://lake/warehouse/bronze/events/v3.metadata.json")
        .setNamespaceProperty(1, "owner", "data-platform")
        .setNextNamespaceId(3)
        .setNextTableId(2)
        .rebuildLookups()
        .build();
    byte[] file = encodeFile(catalog);
    return new Scenario(
        "namespaces-and-pointer-tables",
        "A compacted (CAS-only) catalog with two namespaces, one nested "
            + "under the other, a pointer-mode table referenced by S3 URL, "
            + "and one namespace property. Demonstrates repeated message "
            + "fields and parent_id chaining inside the Checkpoint message.",
        file, List.of());
  }

  /**
   * Append-mode commit with late binding: the txn creates a namespace AND a
   * table referencing it via negative ID sentinels. Highlights the 5-byte
   * non-canonical varint quirk and {@code parent_version=-1}.
   */
  private Scenario scenarioLateBindMultiAction() {
    ProtoCatalogFile catalog = ProtoCatalogFile.builder(LOCATION)
        .setCatalogUuid(FIXED_CATALOG_UUID)
        .setNextNamespaceId(1)
        .setNextTableId(1)
        .build();
    byte[] file = encodeFile(catalog);
    UUID txnId = UUID.fromString("01900000-0000-7000-8000-000000000020");
    ProtoCodec.Transaction txn = new ProtoCodec.Transaction(
        txnId,
        List.of(
            new ProtoCodec.CreateNamespaceAction(1, 1, 0, -1, "staging"),
            new ProtoCodec.CreateTableAction(
                1, 1, 1, -1, "raw_events", "s3://lake/staging/raw_events/v1.metadata.json")));
    file = appendTxn(file, txn);

    List<Note> notes = List.of(
        new Note(-1, -1, "QUIRK: parent_version = -1 (late-bind sentinel) "
            + "encodes as a 5-byte varint, not the 10-byte sign-extended form "
            + "proto3 specifies. See ProtoCodec.writeRawVarint(int) -- the "
            + "unsigned >>>= 7 on a 32-bit int terminates after 5 iterations."));
    return new Scenario(
        "txn-multi-action-with-late-bind",
        "Two actions in one transaction: CreateNamespace and CreateTable. "
            + "The CreateTable references the new namespace via "
            + "namespace_version = -1 (late-bind sentinel). Look for the "
            + "5-byte negative-int varint -- a quirk of the manual encoder.",
        file, notes);
  }

  /**
   * Inline table mode: CreateTableInline carries a real (small) Iceberg
   * TableMetadata JSON in the {@code metadata} bytes field. Shows oneof's
   * length-delimited variant and JSON-in-protobuf nesting.
   */
  private Scenario scenarioCreateTableInline() {
    ProtoCatalogFile catalog = ProtoCatalogFile.builder(LOCATION)
        .setCatalogUuid(FIXED_CATALOG_UUID)
        .addNamespace(1, 0, "warehouse", 1)
        .setNextNamespaceId(2)
        .setNextTableId(1)
        .rebuildLookups()
        .build();
    byte[] file = encodeFile(catalog);

    byte[] metadataJson = realTableMetadata(
        "019000aa-0000-7000-8000-000000000001",
        "s3://lake/warehouse/orders",
        Map.of("table-name", "orders", "owner", "commerce-team"),
        PINNED_TS);

    UUID txnId = UUID.fromString("01900000-0000-7000-8000-000000000030");
    ProtoCodec.Transaction txn = new ProtoCodec.Transaction(
        txnId,
        List.of(new ProtoCodec.CreateTableInlineAction(
            1, 1, 1, 1, "orders", metadataJson)));
    file = appendTxn(file, txn);

    return new Scenario(
        "create-table-inline-with-real-metadata",
        "CreateTableInline action carrying a real Iceberg TableMetadata "
            + "(format-version 2, schema with 6 fields, day-partitioned on "
            + "ts, four table properties). The JSON is opaque to the "
            + "catalog format -- protobuf treats it as a length-delimited "
            + "byte string -- but the detail panel pretty-prints it so you "
            + "can see what fresh-table metadata looks like in practice.",
        file, List.of());
  }

  /**
   * Inline delta mode: an UpdateTableInline action carrying a structured
   * TableMetadataDelta with AddSnapshot (containing CompactSummary),
   * SetSnapshotRef, and AddManifestDelta with one ManifestFileEntry. This is
   * the deepest message nesting in the format.
   */
  private Scenario scenarioUpdateTableInlineDelta() {
    byte[] baseMetadata = realTableMetadata(
        "019000bb-0000-7000-8000-000000000001",
        "s3://lake/warehouse/events",
        Map.of("table-name", "events", "owner", "telemetry-team"),
        PINNED_TS);

    ProtoCatalogFile catalog = ProtoCatalogFile.builder(LOCATION)
        .setCatalogUuid(FIXED_CATALOG_UUID)
        .addNamespace(1, 0, "warehouse", 1)
        .addInlineTable(1, 1, "events", 1, baseMetadata,
            "s3://lake/warehouse/events/metadata/snap-")
        .setNextNamespaceId(2)
        .setNextTableId(2)
        .rebuildLookups()
        .build();
    byte[] file = encodeFile(catalog);

    long snapshotId = 0x4f6c4a8e1234abcdL;
    Map<String, String> summary = new LinkedHashMap<>();
    summary.put("operation", "append");
    summary.put("added-data-files", "1");
    summary.put("added-records", "1024");

    InlineDeltaCodec.AddSnapshotUpdate addSnap =
        new InlineDeltaCodec.AddSnapshotUpdate(
            snapshotId, "", summary, 60_000L, 0, 1024L, null, null, null);
    InlineDeltaCodec.SetSnapshotRefUpdate setRef =
        new InlineDeltaCodec.SetSnapshotRefUpdate(
            "main", snapshotId, "branch", 0, 0L, 0L);
    ProtoCodec.DecodedManifestFile manifest = new ProtoCodec.DecodedManifestFile(
        "s3://lake/warehouse/events/metadata/snap-m0.avro",
        4096L,
        0,
        0,
        1L,
        1L,
        snapshotId,
        1,
        0,
        0,
        1024L,
        0L,
        0L,
        Collections.emptyList(),
        null,
        null);
    InlineDeltaCodec.AddManifestUpdate addManifest =
        new InlineDeltaCodec.AddManifestUpdate(
            snapshotId, manifest, "s3://lake/warehouse/events/metadata/snap-");

    byte[] deltaBytes = InlineDeltaCodec.encodeDelta(
        List.of(addSnap, setRef, addManifest), 1746518460000L);

    UUID txnId = UUID.fromString("01900000-0000-7000-8000-000000000040");
    ProtoCodec.Transaction txn = new ProtoCodec.Transaction(
        txnId,
        List.of(ProtoCodec.UpdateTableInlineAction.delta(1, 1, deltaBytes)));
    file = appendTxn(file, txn);

    return new Scenario(
        "update-table-inline-delta",
        "An UpdateTableInline action whose payload is a structured "
            + "TableMetadataDelta. The delta carries an AddSnapshot (with a "
            + "nested CompactSummary), a SetSnapshotRef pointing 'main' at "
            + "the new snapshot, and an AddManifestDelta with one "
            + "ManifestFileEntry. Look for fixed64 (snapshot_id), sint64 "
            + "(timestamp_delta_ms), and the deepest message nesting in the "
            + "format.",
        file, List.of());
  }

  /**
   * Two pointer-mode tables A and B in the same namespace, then three
   * appended UpdateTable transactions:
   * <ol>
   *   <li>T1 updates A — succeeds, A advances to v=2.</li>
   *   <li>T2 wants to update A and B together but captured A.version=1 —
   *       A's verify fails (current is now 2), the entire txn is rejected
   *       and B is NOT updated either (all-or-nothing).</li>
   *   <li>T3 retries with A.version=2 — both actions apply.</li>
   * </ol>
   * Demonstrates atomic multi-table commits and version-based optimistic
   * conflict detection.
   */
  private Scenario scenarioPointerMultiTableConflictRetry() {
    ProtoCatalogFile catalog = ProtoCatalogFile.builder(LOCATION)
        .setCatalogUuid(FIXED_CATALOG_UUID)
        .addNamespace(1, 0, "warehouse", 1)
        .addTable(1, 1, "A", 1, "s3://lake/warehouse/A/v1.metadata.json")
        .addTable(2, 1, "B", 1, "s3://lake/warehouse/B/v1.metadata.json")
        .setNextNamespaceId(2)
        .setNextTableId(3)
        .rebuildLookups()
        .build();
    byte[] file = encodeFile(catalog);

    UUID t1Id = UUID.fromString("01900000-0000-7000-8000-000000000071");
    UUID t2Id = UUID.fromString("01900000-0000-7000-8000-000000000072");
    UUID t3Id = UUID.fromString("01900000-0000-7000-8000-000000000073");

    ProtoCodec.Transaction t1 = new ProtoCodec.Transaction(
        t1Id,
        List.of(new ProtoCodec.UpdateTableLocationAction(
            1, 1, "s3://lake/warehouse/A/v2.metadata.json")));
    ProtoCodec.Transaction t2 = new ProtoCodec.Transaction(
        t2Id,
        List.of(
            new ProtoCodec.UpdateTableLocationAction(
                1, 1, "s3://lake/warehouse/A/v3.metadata.json"),  // STALE: captured v=1
            new ProtoCodec.UpdateTableLocationAction(
                2, 1, "s3://lake/warehouse/B/v2.metadata.json")));
    ProtoCodec.Transaction t3 = new ProtoCodec.Transaction(
        t3Id,
        List.of(
            new ProtoCodec.UpdateTableLocationAction(
                1, 2, "s3://lake/warehouse/A/v3.metadata.json"),  // RETRY: captured v=2
            new ProtoCodec.UpdateTableLocationAction(
                2, 1, "s3://lake/warehouse/B/v2.metadata.json")));

    int[] r1 = new int[2];
    int[] r2 = new int[2];
    int[] r3 = new int[2];
    file = appendTxnTrack(file, t1, r1);
    file = appendTxnTrack(file, t2, r2);
    file = appendTxnTrack(file, t3, r3);

    List<Note> notes = List.of(
        new Note(r1[0], r1[1], "T1: UpdateTableLocation(A, v=1) — APPLIES. A advances to v=2."),
        new Note(r2[0], r2[1], "T2: UpdateTableLocation(A, v=1) + UpdateTableLocation(B, v=1) — REJECTED. "
            + "A's captured version (1) no longer matches current (2) after T1, so the entire transaction "
            + "fails verify(); B is also not updated (all-or-nothing). The bytes still sit in the log on "
            + "disk; replay silently skips them."),
        new Note(r3[0], r3[1], "T3: UpdateTableLocation(A, v=2) + UpdateTableLocation(B, v=1) — APPLIES. "
            + "Same intent as T2, retried with A's correct current version."));
    return new Scenario(
        "pointer-multi-table-conflict-retry",
        "Two pointer-mode tables A and B in one namespace. Three appended transactions show how "
            + "version-based conflict detection and atomic multi-table commits interact: T1 succeeds, "
            + "T2 (multi-table) is rejected because A's captured version is stale, then T3 (the retry) "
            + "applies both actions. T2's bytes remain in the log — read replay skips them silently.",
        file, notes);
  }

  /**
   * Same A/B two-table scenario but for inline tables. Each
   * {@code UpdateTableInline} carries a real-shape data-commit delta:
   * an {@code AddSnapshot} plus a {@code SetSnapshotRef} pointing the
   * {@code main} branch at the new snapshot. Conflict detection is
   * identical to the pointer variant: T2 captures A.v=1, fails verify
   * after T1, T3 retries with A.v=2.
   */
  private Scenario scenarioInlineMultiTableConflictRetry() {
    // Seed each table at a distinct UUID; deltas only carry the
    // AddSnapshot/SetSnapshotRef pair, mirroring the production
    // (checkpoint + delta) replay model.
    byte[] aV1 = realTableMetadata(
        "019000a1-0000-7000-8000-000000000001",
        "s3://lake/warehouse/A",
        Map.of("table-name", "A"),
        PINNED_TS);
    byte[] bV1 = realTableMetadata(
        "019000b1-0000-7000-8000-000000000001",
        "s3://lake/warehouse/B",
        Map.of("table-name", "B"),
        PINNED_TS);

    long aT1Snap = 0x1111111111111111L;
    long aT2StaleSnap = 0x2222222222222222L;
    long bT2StaleSnap = 0x2222222222222223L;
    long aT3Snap = 0x3333333333333333L;
    long bT3Snap = 0x3333333333333334L;

    byte[] aT1Delta = encodeAppendSnapshotDelta(
        aT1Snap, "T1.avro", PINNED_TS, PINNED_TS_AFTER_T1, /* parentSnapshotId */ null);
    byte[] aT2StaleDelta = encodeAppendSnapshotDelta(
        aT2StaleSnap, "T2-stale.avro", PINNED_TS, PINNED_TS_AFTER_T2, null);
    byte[] bT2StaleDelta = encodeAppendSnapshotDelta(
        bT2StaleSnap, "T2-stale.avro", PINNED_TS, PINNED_TS_AFTER_T2, null);
    byte[] aT3Delta = encodeAppendSnapshotDelta(
        aT3Snap, "T3.avro", PINNED_TS_AFTER_T1, PINNED_TS_AFTER_T3,
        /* parentSnapshotId */ aT1Snap);
    byte[] bT3Delta = encodeAppendSnapshotDelta(
        bT3Snap, "T3.avro", PINNED_TS, PINNED_TS_AFTER_T3, null);

    ProtoCatalogFile catalog = ProtoCatalogFile.builder(LOCATION)
        .setCatalogUuid(FIXED_CATALOG_UUID)
        .addNamespace(1, 0, "warehouse", 1)
        .addInlineTable(1, 1, "A", 1, aV1, "s3://lake/warehouse/A/m/snap-")
        .addInlineTable(2, 1, "B", 1, bV1, "s3://lake/warehouse/B/m/snap-")
        .setNextNamespaceId(2)
        .setNextTableId(3)
        .rebuildLookups()
        .build();
    byte[] file = encodeFile(catalog);

    UUID t1Id = UUID.fromString("01900000-0000-7000-8000-000000000081");
    UUID t2Id = UUID.fromString("01900000-0000-7000-8000-000000000082");
    UUID t3Id = UUID.fromString("01900000-0000-7000-8000-000000000083");

    ProtoCodec.Transaction t1 = new ProtoCodec.Transaction(
        t1Id,
        List.of(ProtoCodec.UpdateTableInlineAction.delta(1, 1, aT1Delta)));
    ProtoCodec.Transaction t2 = new ProtoCodec.Transaction(
        t2Id,
        List.of(
            ProtoCodec.UpdateTableInlineAction.delta(1, 1, aT2StaleDelta),  // STALE
            ProtoCodec.UpdateTableInlineAction.delta(2, 1, bT2StaleDelta)));
    ProtoCodec.Transaction t3 = new ProtoCodec.Transaction(
        t3Id,
        List.of(
            ProtoCodec.UpdateTableInlineAction.delta(1, 2, aT3Delta),  // RETRY
            ProtoCodec.UpdateTableInlineAction.delta(2, 1, bT3Delta)));

    int[] r1 = new int[2];
    int[] r2 = new int[2];
    int[] r3 = new int[2];
    file = appendTxnTrack(file, t1, r1);
    file = appendTxnTrack(file, t2, r2);
    file = appendTxnTrack(file, t3, r3);

    List<Note> notes = List.of(
        new Note(r1[0], r1[1], "T1: UpdateTableInline(A, v=1, delta) — APPLIES. "
            + "Delta carries an AddSnapshot + SetSnapshotRef('main', S1A). A advances to v=2."),
        new Note(r2[0], r2[1], "T2: UpdateTableInline(A, v=1) + UpdateTableInline(B, v=1) — REJECTED. "
            + "Multi-table all-or-nothing: A's captured v=1 no longer matches current v=2 after T1, "
            + "so the entire transaction fails verify(). B's delta (a perfectly valid AddSnapshot) "
            + "never lands. The record stays in the log until the next CAS-mode commit reclaims the space."),
        new Note(r3[0], r3[1], "T3: UpdateTableInline(A, v=2) + UpdateTableInline(B, v=1) — APPLIES. "
            + "Each table's AddSnapshot delta lands atomically; A's main now points at S3A "
            + "(parent=S1A), B's at S3B."));
    return new Scenario(
        "inline-multi-table-conflict-retry",
        "Inline-mode counterpart of pointer-multi-table-conflict-retry. Each transaction is a "
            + "real-shape data commit: a TableMetadataDelta with an AddSnapshot + SetSnapshotRef "
            + "pinning the new snapshot to main. The structural shape is identical to the pointer "
            + "scenario except UpdateTableInline (Action field 9) replaces UpdateTableLocation "
            + "(Action field 7), and the payload is a length-delimited delta message instead of a "
            + "metadata-location string. Compare T2's wasted bytes between the two scenarios: in "
            + "pointer mode T2 references already-written metadata.json files; in inline mode T2's "
            + "delta is the only place its rejected snapshots appeared on the wire.",
        file, notes);
  }

  /**
   * Builds a {@code TableMetadataDelta} bytes payload representing a typical
   * data commit on an inline table: an {@code AddSnapshot} plus a
   * {@code SetSnapshotRef} pointing the {@code main} branch at the new
   * snapshot. The {@code manifest_list_suffix} is appended to the table's
   * stored {@code manifest_list_prefix} on replay; passing a real path
   * suffix (e.g., {@code "T1.avro"}) produces a regular external
   * manifest-list location, not an inline-ML sentinel.
   */
  private static byte[] encodeAppendSnapshotDelta(
      long snapshotId, String manifestListSuffix,
      long baseLastUpdatedMs, long deltaLastUpdatedMs, Long parentSnapshotId) {
    Map<String, String> summary = new LinkedHashMap<>();
    summary.put("operation", "append");
    summary.put("added-data-files", "1");
    summary.put("added-records", "1024");
    long timestampDeltaMs = deltaLastUpdatedMs - baseLastUpdatedMs;
    InlineDeltaCodec.AddSnapshotUpdate addSnap =
        new InlineDeltaCodec.AddSnapshotUpdate(
            snapshotId, manifestListSuffix, summary,
            timestampDeltaMs, /* schemaId */ 0, /* addedRows */ 1024L,
            parentSnapshotId, /* firstRowId */ null, /* keyId */ null);
    InlineDeltaCodec.SetSnapshotRefUpdate setRef =
        new InlineDeltaCodec.SetSnapshotRefUpdate(
            "main", snapshotId, "branch", 0, 0L, 0L);
    return InlineDeltaCodec.encodeDelta(List.of(addSnap, setRef), deltaLastUpdatedMs);
  }

  // ============================================================
  // Real Iceberg TableMetadata helpers
  //
  // The catalog format treats inline table metadata as opaque length-
  // delimited bytes -- it never parses the JSON. Most catalog tests
  // therefore use minimal "{}"-style placeholders. The format-explorer
  // visualizer is different: developers open it precisely to see what
  // realistic payloads look like on the wire. So we build genuine Iceberg
  // TableMetadata via TableMetadata.buildFromEmpty(), serialize via
  // TableMetadataParser.toJson, and pin both the UUID and last-updated-ms
  // so fixture regenerations produce stable bytes.
  // ============================================================

  private static final long PINNED_TS = 1_746_518_400_000L;     // 2025-05-06 12:00:00Z
  private static final long PINNED_TS_AFTER_T1 = PINNED_TS + 60_000L;
  private static final long PINNED_TS_AFTER_T2 = PINNED_TS + 120_000L;
  private static final long PINNED_TS_AFTER_T3 = PINNED_TS + 180_000L;

  private static Schema demoSchema() {
    return new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "ts", Types.TimestampType.withZone()),
        Types.NestedField.optional(3, "category", Types.StringType.get()),
        Types.NestedField.optional(4, "amount", Types.DecimalType.of(12, 2)),
        Types.NestedField.optional(5, "tags",
            Types.ListType.ofOptional(7, Types.StringType.get())),
        Types.NestedField.optional(6, "payload", Types.StringType.get()));
  }

  /** Real Iceberg TableMetadata for a fresh table; deterministic JSON bytes. */
  private static byte[] realTableMetadata(
      String tableUuid, String location, Map<String, String> extraProps, long timestamp) {
    Schema schema = demoSchema();
    PartitionSpec spec = PartitionSpec.builderFor(schema).day("ts").build();
    Map<String, String> props = new LinkedHashMap<>();
    props.put("write.format.default", "parquet");
    props.put("write.parquet.compression-codec", "zstd");
    props.put("write.target-file-size-bytes", "536870912");
    props.put("owner", "data-platform");
    if (extraProps != null) {
      props.putAll(extraProps);
    }
    TableMetadata meta = TableMetadata.buildFromEmpty()
        .setLocation(location)
        .setCurrentSchema(schema, /* lastColumnId */ 7)
        .addPartitionSpec(spec)
        .addSortOrder(SortOrder.unsorted())
        .setProperties(props)
        .assignUUID(tableUuid)
        .setLastUpdatedMillis(timestamp)
        .discardChanges()
        .build();
    return TableMetadataParser.toJson(meta).getBytes(StandardCharsets.UTF_8);
  }

  // ============================================================
  // File / transaction byte construction
  // ============================================================

  private static byte[] encodeFile(ProtoCatalogFile catalog) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(ProtoCatalogFormat.MAGIC);
      writeBigEndianInt(out, ProtoCatalogFormat.FORMAT_VERSION);
      ProtoCatalogFormat.ProtoIdManager idManager = new ProtoCatalogFormat.ProtoIdManager();
      idManager.setGlobals(catalog.nextNamespaceId(), catalog.nextTableId());
      ProtoCatalogFormat.Mut mut = new ProtoCatalogFormat.Mut(catalog);
      byte[] checkpoint = ProtoCodec.encodeCheckpoint(catalog, mut, idManager);
      writeVarint(out, checkpoint.length);
      out.write(checkpoint);
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static byte[] appendTxn(byte[] file, ProtoCodec.Transaction txn) {
    return appendTxnTrack(file, txn, null);
  }

  /**
   * Appends a transaction record and (optionally) records its byte range
   * (varint length + body, half-open) into {@code outRange[0..1]} for use
   * in scenario notes.
   */
  private static byte[] appendTxnTrack(byte[] file, ProtoCodec.Transaction txn, int[] outRange) {
    try {
      byte[] body = ProtoCodec.encodeTransaction(txn);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(file);
      int recordStart = file.length;
      writeVarint(out, body.length);
      out.write(body);
      byte[] result = out.toByteArray();
      if (outRange != null) {
        outRange[0] = recordStart;
        outRange[1] = result.length;
      }
      return result;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void writeBigEndianInt(ByteArrayOutputStream out, int v) {
    out.write((v >>> 24) & 0xFF);
    out.write((v >>> 16) & 0xFF);
    out.write((v >>> 8) & 0xFF);
    out.write(v & 0xFF);
  }

  private static void writeVarint(ByteArrayOutputStream out, int v) {
    while ((v & ~0x7F) != 0) {
      out.write((v & 0x7F) | 0x80);
      v >>>= 7;
    }
    out.write(v);
  }

  // ============================================================
  // Proto schema parser (text-driven; ~1 source file)
  // ============================================================

  static SchemaDescriptor parseProto(Path protoPath) throws IOException {
    String src = Files.readString(protoPath);
    String stripped = stripBlockComments(src);
    String[] lines = stripped.split("\n");

    Map<String, MessageDef> messages = new LinkedHashMap<>();
    Map<String, EnumDef> enums = new LinkedHashMap<>();

    Pattern messageStart = Pattern.compile("^\\s*message\\s+(\\w+)\\s*\\{");
    Pattern enumStart = Pattern.compile("^\\s*enum\\s+(\\w+)\\s*\\{");
    Pattern oneofStart = Pattern.compile("^\\s*oneof\\s+(\\w+)\\s*\\{");
    Pattern fieldDecl = Pattern.compile(
        "^\\s*(repeated\\s+)?([\\w.]+)\\s+(\\w+)\\s*=\\s*(\\d+)\\s*;");
    Pattern mapDecl = Pattern.compile(
        "^\\s*map<\\s*([\\w.]+)\\s*,\\s*([\\w.]+)\\s*>\\s+(\\w+)\\s*=\\s*(\\d+)\\s*;");
    Pattern enumValue = Pattern.compile("^\\s*(\\w+)\\s*=\\s*(\\d+)\\s*;");
    Pattern closeBrace = Pattern.compile("^\\s*\\}");

    String currentMessage = null;
    String currentEnum = null;
    String currentOneof = null;
    StringBuilder pendingComment = new StringBuilder();

    for (String rawLine : lines) {
      String line = rawLine;
      int slash = line.indexOf("//");
      String commentTail = slash >= 0 ? line.substring(slash + 2).trim() : null;
      String code = slash >= 0 ? line.substring(0, slash) : line;
      String trimmed = code.trim();

      if (trimmed.isEmpty()) {
        if (commentTail != null && !commentTail.isEmpty()) {
          if (pendingComment.length() > 0) {
            pendingComment.append(' ');
          }
          pendingComment.append(commentTail);
        }
        continue;
      }

      Matcher m;
      if ((m = messageStart.matcher(code)).find()) {
        currentMessage = m.group(1);
        messages.put(currentMessage, new MessageDef(currentMessage));
        pendingComment.setLength(0);
        continue;
      }
      if ((m = enumStart.matcher(code)).find()) {
        currentEnum = m.group(1);
        enums.put(currentEnum, new EnumDef(currentEnum));
        pendingComment.setLength(0);
        continue;
      }
      if ((m = oneofStart.matcher(code)).find() && currentMessage != null) {
        currentOneof = m.group(1);
        pendingComment.setLength(0);
        continue;
      }
      if (closeBrace.matcher(trimmed).find()) {
        if (currentOneof != null) {
          currentOneof = null;
        } else if (currentEnum != null) {
          currentEnum = null;
        } else {
          currentMessage = null;
        }
        pendingComment.setLength(0);
        continue;
      }

      if (currentEnum != null && (m = enumValue.matcher(code)).find()) {
        enums.get(currentEnum).values.add(
            new EnumValue(m.group(1), Integer.parseInt(m.group(2))));
        pendingComment.setLength(0);
        continue;
      }

      if (currentMessage != null) {
        if ((m = mapDecl.matcher(code)).find()) {
          String type = "map<" + m.group(1) + "," + m.group(2) + ">";
          int number = Integer.parseInt(m.group(4));
          String comment = consumeComment(pendingComment, commentTail);
          messages.get(currentMessage).fields.add(
              new FieldDef(number, m.group(3), type, false, currentOneof, comment));
          continue;
        }
        if ((m = fieldDecl.matcher(code)).find()) {
          boolean repeated = m.group(1) != null;
          String type = m.group(2);
          String name = m.group(3);
          int number = Integer.parseInt(m.group(4));
          String comment = consumeComment(pendingComment, commentTail);
          messages.get(currentMessage).fields.add(
              new FieldDef(number, name, type, repeated, currentOneof, comment));
          continue;
        }
      }

      if (commentTail != null && !commentTail.isEmpty()) {
        if (pendingComment.length() > 0) {
          pendingComment.append(' ');
        }
        pendingComment.append(commentTail);
      } else {
        pendingComment.setLength(0);
      }
    }

    return new SchemaDescriptor(messages, enums);
  }

  private static String consumeComment(StringBuilder pending, String tail) {
    StringBuilder sb = new StringBuilder();
    if (pending.length() > 0) {
      sb.append(pending);
      pending.setLength(0);
    }
    if (tail != null && !tail.isEmpty()) {
      if (sb.length() > 0) {
        sb.append(' ');
      }
      sb.append(tail);
    }
    return sb.toString();
  }

  private static String stripBlockComments(String src) {
    StringBuilder out = new StringBuilder(src.length());
    int i = 0;
    while (i < src.length()) {
      if (i + 1 < src.length() && src.charAt(i) == '/' && src.charAt(i + 1) == '*') {
        int end = src.indexOf("*/", i + 2);
        if (end < 0) {
          break;
        }
        for (int j = i; j < end + 2; j++) {
          if (src.charAt(j) == '\n') {
            out.append('\n');
          }
        }
        i = end + 2;
      } else {
        out.append(src.charAt(i));
        i++;
      }
    }
    return out.toString();
  }

  // ============================================================
  // Drift check: compare proto field numbers against codec source
  // ============================================================

  private static void verifyFieldNumbersAgainstCodec(
      SchemaDescriptor schema, Path codecPath, Path inlineDeltaPath) throws IOException {
    Map<String, Integer> codecConsts = new LinkedHashMap<>();
    extractIntConsts(codecPath, codecConsts);
    extractIntConsts(inlineDeltaPath, codecConsts);

    Map<String, Map<String, Integer>> protoByMessage = expectedConstants(schema);

    List<String> mismatches = new ArrayList<>();
    for (Map.Entry<String, Map<String, Integer>> messageEntry : protoByMessage.entrySet()) {
      for (Map.Entry<String, Integer> field : messageEntry.getValue().entrySet()) {
        Integer codecValue = codecConsts.get(field.getKey());
        if (codecValue != null && !codecValue.equals(field.getValue())) {
          mismatches.add(String.format(
              "%s: %s.%s = %d in codec, but %d in catalog.proto",
              messageEntry.getKey(), messageEntry.getKey(), field.getKey(),
              codecValue, field.getValue()));
        }
      }
    }
    if (!mismatches.isEmpty()) {
      throw new IllegalStateException(
          "Schema/code drift between catalog.proto and ProtoCodec/InlineDeltaCodec:\n  "
              + String.join("\n  ", mismatches));
    }
  }

  private static void extractIntConsts(Path javaPath, Map<String, Integer> sink)
      throws IOException {
    String src = Files.readString(javaPath);
    Pattern p = Pattern.compile(
        "(?:private|public|protected)?\\s*static\\s+final\\s+int\\s+(\\w+)\\s*=\\s*(-?\\d+)\\s*;");
    Matcher m = p.matcher(src);
    while (m.find()) {
      sink.put(m.group(1), Integer.parseInt(m.group(2)));
    }
  }

  /**
   * Maps proto message + field combinations to the expected codec constant
   * name. Only the messages whose constants appear in {@link ProtoCodec} or
   * {@link InlineDeltaCodec} are listed -- everything else is skipped.
   */
  private static Map<String, Map<String, Integer>> expectedConstants(SchemaDescriptor schema) {
    Map<String, String> messageToPrefix = new LinkedHashMap<>();
    messageToPrefix.put("Checkpoint", "CHECKPOINT");
    messageToPrefix.put("Namespace", "NS");
    messageToPrefix.put("Table", "TBL");
    messageToPrefix.put("NamespaceProperty", "NSPROP");
    messageToPrefix.put("InlineTable", "INLINE_TBL");
    messageToPrefix.put("ManifestFileEntry", "MF");
    messageToPrefix.put("PartitionFieldSummaryEntry", "PFS");
    messageToPrefix.put("SnapshotManifestRefs", "SMR");
    messageToPrefix.put("Transaction", "TXN");
    messageToPrefix.put("Action", "ACTION");
    messageToPrefix.put("CreateNamespace", "CREATE_NS");
    messageToPrefix.put("DropNamespace", "DROP_NS");
    messageToPrefix.put("SetNamespaceProperty", "SET_NSPROP");
    messageToPrefix.put("RemoveNamespaceProperty", "REMOVE_NSPROP");
    messageToPrefix.put("CreateTable", "CREATE_TBL");
    messageToPrefix.put("DropTable", "DROP_TBL");
    messageToPrefix.put("UpdateTableLocation", "UPDATE_TBL");
    messageToPrefix.put("ReadTable", "READ_TBL");
    messageToPrefix.put("UpdateTableInline", "UPDATE_INLINE");
    messageToPrefix.put("CreateTableInline", "CREATE_INLINE_TBL");
    messageToPrefix.put("RenameTable", "RENAME_TBL");

    Map<String, Map<String, Integer>> out = new LinkedHashMap<>();
    for (Map.Entry<String, String> e : messageToPrefix.entrySet()) {
      MessageDef def = schema.messages.get(e.getKey());
      if (def == null) {
        continue;
      }
      Map<String, Integer> fields = new LinkedHashMap<>();
      for (FieldDef f : def.fields) {
        fields.put(e.getValue() + "_" + f.name.toUpperCase(), f.number);
      }
      out.put(e.getKey(), fields);
    }
    return out;
  }

  // ============================================================
  // JSON emission (no external dependency -- the schema/scenarios are simple)
  // ============================================================

  private static String emitJson(SchemaDescriptor schema, List<Scenario> scenarios) {
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("  \"schema\": ");
    appendSchemaJson(sb, schema, 1);
    sb.append(",\n");
    sb.append("  \"scenarios\": [\n");
    for (int i = 0; i < scenarios.size(); i++) {
      Scenario s = scenarios.get(i);
      sb.append("    {\n");
      sb.append("      \"name\": ").append(jsonString(s.name)).append(",\n");
      sb.append("      \"description\": ").append(jsonString(s.description)).append(",\n");
      sb.append("      \"hex\": ").append(jsonString(toHex(s.bytes))).append(",\n");
      sb.append("      \"notes\": [\n");
      for (int j = 0; j < s.notes.size(); j++) {
        Note n = s.notes.get(j);
        sb.append("        {\"start\": ").append(n.start)
            .append(", \"end\": ").append(n.end)
            .append(", \"label\": ").append(jsonString(n.label)).append("}");
        if (j < s.notes.size() - 1) {
          sb.append(',');
        }
        sb.append('\n');
      }
      sb.append("      ]\n");
      sb.append("    }");
      if (i < scenarios.size() - 1) {
        sb.append(',');
      }
      sb.append('\n');
    }
    sb.append("  ]\n");
    sb.append("}\n");
    return sb.toString();
  }

  private static void appendSchemaJson(StringBuilder sb, SchemaDescriptor schema, int indent) {
    String pad = "  ".repeat(indent);
    sb.append("{\n");
    sb.append(pad).append("  \"messages\": {\n");
    int mi = 0;
    int total = schema.messages.size();
    for (Map.Entry<String, MessageDef> e : schema.messages.entrySet()) {
      MessageDef def = e.getValue();
      sb.append(pad).append("    ").append(jsonString(e.getKey())).append(": {\n");
      sb.append(pad).append("      \"fields\": [\n");
      for (int i = 0; i < def.fields.size(); i++) {
        FieldDef f = def.fields.get(i);
        sb.append(pad).append("        {")
            .append("\"number\": ").append(f.number)
            .append(", \"name\": ").append(jsonString(f.name))
            .append(", \"type\": ").append(jsonString(f.type))
            .append(", \"repeated\": ").append(f.repeated)
            .append(", \"oneof\": ").append(f.oneof == null ? "null" : jsonString(f.oneof))
            .append(", \"comment\": ").append(jsonString(f.comment))
            .append("}");
        if (i < def.fields.size() - 1) {
          sb.append(',');
        }
        sb.append('\n');
      }
      sb.append(pad).append("      ]\n");
      sb.append(pad).append("    }");
      if (mi < total - 1) {
        sb.append(',');
      }
      sb.append('\n');
      mi++;
    }
    sb.append(pad).append("  },\n");
    sb.append(pad).append("  \"enums\": {\n");
    int ei = 0;
    for (Map.Entry<String, EnumDef> e : schema.enums.entrySet()) {
      EnumDef def = e.getValue();
      sb.append(pad).append("    ").append(jsonString(e.getKey())).append(": [\n");
      for (int i = 0; i < def.values.size(); i++) {
        EnumValue v = def.values.get(i);
        sb.append(pad).append("      {")
            .append("\"name\": ").append(jsonString(v.name))
            .append(", \"number\": ").append(v.number)
            .append("}");
        if (i < def.values.size() - 1) {
          sb.append(',');
        }
        sb.append('\n');
      }
      sb.append(pad).append("    ]");
      if (ei < schema.enums.size() - 1) {
        sb.append(',');
      }
      sb.append('\n');
      ei++;
    }
    sb.append(pad).append("  }\n");
    sb.append(pad).append("}");
  }

  private static String jsonString(String s) {
    if (s == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder(s.length() + 2);
    sb.append('"');
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"':
          sb.append("\\\"");
          break;
        case '\\':
          sb.append("\\\\");
          break;
        case '\b':
          sb.append("\\b");
          break;
        case '\f':
          sb.append("\\f");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\r':
          sb.append("\\r");
          break;
        case '\t':
          sb.append("\\t");
          break;
        default:
          if (c < 0x20) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
          break;
      }
    }
    sb.append('"');
    return sb.toString();
  }

  private static String toHex(byte[] bytes) {
    char[] hex = "0123456789abcdef".toCharArray();
    char[] out = new char[bytes.length * 2];
    for (int i = 0; i < bytes.length; i++) {
      out[i * 2] = hex[(bytes[i] >> 4) & 0x0F];
      out[i * 2 + 1] = hex[bytes[i] & 0x0F];
    }
    return new String(out);
  }

  // ============================================================
  // Data classes
  // ============================================================

  static final class Scenario {
    final String name;
    final String description;
    final byte[] bytes;
    final List<Note> notes;

    Scenario(String name, String description, byte[] bytes, List<Note> notes) {
      this.name = name;
      this.description = description;
      this.bytes = bytes;
      this.notes = notes;
    }
  }

  static final class Note {
    final int start;
    final int end;
    final String label;

    Note(int start, int end, String label) {
      this.start = start;
      this.end = end;
      this.label = label;
    }
  }

  static final class SchemaDescriptor {
    final Map<String, MessageDef> messages;
    final Map<String, EnumDef> enums;

    SchemaDescriptor(Map<String, MessageDef> messages, Map<String, EnumDef> enums) {
      this.messages = messages;
      this.enums = enums;
    }
  }

  static final class MessageDef {
    final String name;
    final List<FieldDef> fields = new ArrayList<>();

    MessageDef(String name) {
      this.name = name;
    }
  }

  static final class FieldDef {
    final int number;
    final String name;
    final String type;
    final boolean repeated;
    final String oneof;
    final String comment;

    FieldDef(int number, String name, String type, boolean repeated,
        String oneof, String comment) {
      this.number = number;
      this.name = name;
      this.type = type;
      this.repeated = repeated;
      this.oneof = oneof;
      this.comment = comment;
    }
  }

  static final class EnumDef {
    final String name;
    final List<EnumValue> values = new ArrayList<>();

    EnumDef(String name) {
      this.name = name;
    }
  }

  static final class EnumValue {
    final String name;
    final int number;

    EnumValue(String name, int number) {
      this.name = name;
      this.number = number;
    }
  }

  // ============================================================
  // Minimal in-memory InputFile (we only need location()).
  // ============================================================

  private static final class MemInputFile implements InputFile {
    private final String location;

    MemInputFile(String location) {
      this.location = location;
    }

    @Override public long getLength() { return 0; }
    @Override public SeekableInputStream newStream() {
      throw new UnsupportedOperationException();
    }
    @Override public String location() { return location; }
    @Override public boolean exists() { return false; }
  }
}
