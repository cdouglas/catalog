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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.GenericPartitionFieldSummary;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Generates the fixtures of the format explorer, {@code docs/format-explorer/index.html}: the
 * schema it decodes with (catalog.proto, parsed by {@link CatalogProto}) and a set of scenarios,
 * each a catalog file with notes.
 *
 * <p>Each scenario's file is built the way a writer builds one. A record holds the actions {@link
 * ProtoCatalogFormat.Mut#buildActions} makes for a change to the snapshot its writer read (so
 * creates carry late-bound ids); a CAS write is a checkpoint of the base and the commit's record;
 * an inline delta is the one {@link InlineDeltaCodec#computeDelta} sends for its commit. Only the
 * transaction ids, table UUIDs and times are pinned, so the bytes are the same on every run. Each
 * file must conform to catalog.proto and replay to the outcome its notes describe.
 *
 * <p>{@code TestFormatExplorer} fails when the fixtures embedded in index.html differ from these.
 * To regenerate them in place:
 *
 * <pre>
 *   mvn test -Dtest=FormatExplorerFixtures -Dexplorer.regenerate=true
 * </pre>
 *
 * <p>which also writes {@code target/format-explorer/fixtures.json}.
 */
public class FormatExplorerFixtures {

  static final Path HTML =
      Paths.get(System.getProperty("repo.root", ".."), "docs/format-explorer/index.html");
  private static final Path OUTPUT = Paths.get("target/format-explorer/fixtures.json");
  private static final Pattern FIXTURES =
      Pattern.compile("(<script id=\"fixtures-data\"[^>]*>)(.*?)(</script>)", Pattern.DOTALL);

  private static final InputFile LOCATION = new MemInputFile("test://format-explorer");
  private static final UUID CATALOG_UUID = UUID.fromString("01900000-0000-7000-8000-000000000001");
  private static final long PINNED_TS = 1_746_518_400_000L; // 2025-05-06 08:00:00Z
  private static final Namespace WAREHOUSE = Namespace.of("warehouse");

  @Test
  @EnabledIfSystemProperty(named = "explorer.regenerate", matches = "true")
  void regenerate() throws IOException {
    String json = fixturesJson();
    Files.createDirectories(OUTPUT.getParent());
    Files.writeString(OUTPUT, json);
    String html = Files.readString(HTML);
    String spliced = splice(html, json);
    if (spliced.equals(html)) {
      System.out.println("The fixtures in " + HTML + " are current");
    } else {
      Files.writeString(HTML, spliced);
      System.out.println("Wrote " + json.length() + " bytes of fixtures to " + HTML);
    }
  }

  /**
   * The fixtures: catalog.proto's schema, the format versions the explorer checks, a deflate
   * probe and each scenario. Fails if a scenario does not conform to catalog.proto or does not
   * replay to its documented outcome.
   */
  static String fixturesJson() {
    List<Scenario> scenarios =
        List.of(
            emptyBootstrap(),
            lateBoundCreates(),
            createTableInline("create-table-inline-gzip", InlineMetadataCodecs.TAG_JSON_GZIP),
            createTableInline(
                "create-table-inline-structural", InlineMetadataCodecs.TAG_STRUCTURAL),
            updateTableInlineDelta(),
            evolveTableInlineDelta(),
            pointerMultiTableConflictRetry(),
            inlineMultiTableConflictRetry(),
            concurrentCreatesBoundAtReplay(),
            renameTableAcrossNamespaces(),
            inlineManifestList(),
            compactionWithCommittedSetGc());
    for (Scenario scenario : scenarios) {
      String violation = CatalogProto.violation(scenario.bytes);
      if (violation != null) {
        throw new AssertionError(scenario.name + " does not conform to catalog.proto: " + violation);
      }
      try {
        scenario.check.accept(replay(scenario.bytes));
      } catch (RuntimeException | AssertionError e) {
        throw new AssertionError(scenario.name + " does not replay as its notes say", e);
      }
    }
    return emitJson(CatalogProto.schema(), scenarios);
  }

  /** The fixtures embedded in {@code html}, or null if it has no fixtures block. */
  static String embeddedFixtures(String html) {
    Matcher m = FIXTURES.matcher(html);
    return m.find() ? m.group(2) : null;
  }

  static String splice(String html, String json) {
    Matcher m = FIXTURES.matcher(html);
    if (!m.find()) {
      throw new IllegalStateException("index.html has no <script id=\"fixtures-data\"> block");
    }
    return html.substring(0, m.start(2)) + json + html.substring(m.end(2));
  }

  /**
   * The gzip codec's encoding of a fixed table metadata. Gzip output depends on the JDK's zlib
   * build, so {@code TestFormatExplorer} compares the fixtures only on a JDK that encodes this as
   * the fixtures' generator did.
   */
  static String deflateProbe() {
    return toHex(
        InlineMetadataCodecs.JSON_GZIP.encode(
            tableMetadata("019000ff-0000-7000-8000-000000000001", "s3://lake/probe", Map.of())));
  }

  // ============================================================
  // Scenarios
  // ============================================================

  /**
   * The file {@code FileIOCatalog.initialize} creates, a CAS write of an empty catalog with an
   * empty transaction, and the first create appended to it.
   */
  private static Scenario emptyBootstrap() {
    Log log = Log.cas(emptyCatalog(), txn(1), mut -> {}, 0L,
        "Record 0: the empty transaction FileIOCatalog.initialize commits to create the file. "
            + "Its id joins the committed set, like any other.");
    log.append(txn(2), mut -> mut.createNamespace(Namespace.of("analytics")),
        "Record 1: CreateNamespace(id = -1, version = 1, parent_id = 0, parent_version = 0). "
            + "The id is late-bound: replay binds -1 to the checkpoint's next_namespace_id, 1. "
            + "parent_version is the root's version when the writer read it; replay bumps the "
            + "root to 1, so a concurrent top-level create or drop that read version 0 is "
            + "rejected.");
    return log.scenario(
        "empty-bootstrap",
        "The smallest catalog: an 8-byte header, a checkpoint with only the catalog UUID, the "
            + "id counters and the root namespace row (id 0, empty name), and two records. Walk "
            + "every byte to learn the length framing and the Action oneof.",
        List.of(
            "The checkpoint's first field is min_reader_version (field 4): written first so an "
                + "old reader fails before decoding the rest.",
            "Every catalog holds the root as an ordinary Namespace row, id 0. It is not "
                + "addressable by name, but its version orders top-level creates and drops."),
        state -> {
          expect(state.namespaceId(Namespace.of("analytics")), 1, "analytics id");
          expect(state.namespaceVersion(ProtoCatalogFile.ROOT_ID), 1, "root version");
        });
  }

  /** A namespace and a table in it, created in one transaction. */
  private static Scenario lateBoundCreates() {
    Log log = Log.cas(emptyCatalog(), txn(10), mut -> {}, 0L, "Record 0: initialize.");
    TableIdentifier rawEvents = TableIdentifier.of(Namespace.of("staging"), "raw_events");
    log.append(txn(11),
        mut -> mut.createNamespace(Namespace.of("staging"))
            .createTable(rawEvents, "s3://lake/staging/raw_events/v1.metadata.json"),
        "Record 1: CreateNamespace(id = -1) and CreateTable(id = -1, namespace_id = -1, "
            + "namespace_version = -1). The table names the namespace by its late-bound id; "
            + "replay binds the namespace first, then the table to it. A version of -1 means "
            + "the namespace is created in this transaction, so there is no version to check.");
    return log.scenario(
        "txn-multi-action-with-late-bind",
        "Two actions in one transaction, a CreateNamespace and a CreateTable in it. Look for "
            + "the 5-byte varints of the negative ids.",
        List.of(
            "QUIRK: a negative int32 (a late-bound id, or a -1 version) is a 5-byte varint, not "
                + "the 10-byte sign extension proto3 specifies: ProtoCodec.writeRawVarint(int) "
                + "shifts a 32-bit int right unsigned. Readers generated by protoc accept it."),
        state -> {
          expect(state.namespaceId(Namespace.of("staging")), 1, "staging id");
          expect(state.tableId(rawEvents), 1, "raw_events id");
        });
  }

  /** An inline table created with {@code codec}. */
  private static Scenario createTableInline(String name, byte codec) {
    boolean structural = codec == InlineMetadataCodecs.TAG_STRUCTURAL;
    String table = structural ? "products" : "orders";
    TableIdentifier ident = TableIdentifier.of(WAREHOUSE, table);
    ProtoCatalogFile base = warehouse(20 + codec);
    TableMetadata meta =
        tableMetadata(
            structural
                ? "019000cc-0000-7000-8000-000000000001"
                : "019000aa-0000-7000-8000-000000000001",
            "s3://lake/warehouse/" + table,
            Map.of("table-name", table));
    byte[] metadata = InlineMetadataCodecs.byTag(codec).encode(meta);
    Log log = Log.cas(base, txn(30 + codec), mut -> mut.createTableInline(ident, metadata, codec),
        0L,
        "The CAS write that creates the table: a checkpoint of the catalog before it, then the "
            + "CreateTableInline record. Replay binds the table's id -1 to 1.");
    String description =
        structural
            ? "CreateTableInline carrying a TableMetadata in the structural codec "
                + "(metadata_codec = CODEC_STRUCTURAL, 1): gzip around a columnar layout, a "
                + "varint layout version, the JSON without snapshots and metadata log, then "
                + "snap_block and mdlog_block, both empty here. 'Show compact form' lists the "
                + "segments. At the corpus median the layout is 40% smaller than gzip(JSON) and "
                + "4-7x faster to decode lazily (docs/TM_ENCODING_BENCH_RESULTS.md)."
            : "CreateTableInline carrying a TableMetadata in the gzip codec (metadata_codec = "
                + "CODEC_JSON_GZIP, 0): Iceberg's gzip-compressed metadata JSON, format version "
                + "2, six columns, partitioned by day(ts). Expand 'Show recovered JSON'.";
    return log.scenario(name, description,
        List.of("The codec field is written even when it is 0: the decoder requires it."),
        List.of(structural ? structuralView(metadata, meta) : gzipView(metadata, meta)),
        state -> {
          expect(state.tableId(ident), 1, table + " id");
          expect(state.inlineMetadataCodec(1), (int) codec, "codec");
        });
  }

  /** An append to an inline table, sent as a delta. */
  private static Scenario updateTableInlineDelta() {
    TableIdentifier events = TableIdentifier.of(WAREHOUSE, "events");
    TableMetadata meta =
        tableMetadata(
            "019000bb-0000-7000-8000-000000000001",
            "s3://lake/warehouse/events",
            Map.of("table-name", "events"));
    byte[] metadata = InlineMetadataCodecs.JSON_GZIP.encode(meta);
    ProtoCatalogFile base =
        Log.of(warehouse(40))
            .append(txn(41),
                mut -> mut.createTableInline(events, metadata, InlineMetadataCodecs.TAG_JSON_GZIP),
                null)
            .state();
    long snapshotId = 0x4f6c4a8e1234abcdL;
    byte[] delta =
        writerDelta(base, events,
            List.of(
                append(snapshotId, null, "events/metadata/snap-4f6c4a8e1234abcd-1-m0.avro",
                    60_000L),
                main(snapshotId)),
            PINNED_TS + 60_000L);
    Log log = Log.cas(base, txn(42), mut -> mut.updateTableInlineDelta(events, delta), 0L,
        "The append, as a TableMetadataDelta: an AddSnapshot (its summary a CompactSummary), "
            + "then a SetSnapshotRef moving main to it. The snapshot's time is a delta from the "
            + "base's last-updated-ms; the delta's own last_updated_ms is the committed one.");
    return log.scenario(
        "update-table-inline-delta",
        "An UpdateTableInline whose payload is the delta InlineDeltaCodec.computeDelta sends for "
            + "an append to an inline table. Look for fixed64 (snapshot_id), a zigzag sint64 "
            + "(timestamp_delta_ms) and the deepest nesting in the format. The checkpoint's "
            + "InlineTable.metadata is the base metadata, gzip-encoded; click it to see the JSON.",
        List.of(),
        List.of(gzipView(metadata, meta)),
        state -> {
          TableMetadata committed = inlineMetadata(state, events);
          expect(committed.currentSnapshot().snapshotId(), snapshotId, "current snapshot");
        });
  }

  /** A schema, partition spec, sort order and property change to an inline table, as a delta. */
  private static Scenario evolveTableInlineDelta() {
    TableIdentifier orders = TableIdentifier.of(WAREHOUSE, "orders");
    TableMetadata meta =
        tableMetadata(
            "019000ee-0000-7000-8000-000000000001",
            "s3://lake/warehouse/orders",
            Map.of("table-name", "orders", "commit.retry.num-retries", "4"));
    byte[] metadata = InlineMetadataCodecs.JSON_GZIP.encode(meta);
    ProtoCatalogFile base =
        Log.of(warehouse(45))
            .append(txn(46),
                mut -> mut.createTableInline(orders, metadata, InlineMetadataCodecs.TAG_JSON_GZIP),
                null)
            .state();
    Schema evolved =
        new Schema(
            org.apache.iceberg.relocated.com.google.common.collect.Lists.newArrayList(
                    Iterables.concat(
                        meta.schema().columns(),
                        List.of(Types.NestedField.optional(8, "region", Types.StringType.get()))))
                .stream()
                .collect(Collectors.toList()));
    Map<String, String> properties = new LinkedHashMap<>(meta.properties());
    properties.put("write.parquet.compression-codec", "lz4");
    properties.remove("commit.retry.num-retries");
    TableMetadata after =
        TableMetadata.buildFrom(replayBase(base, orders))
            .setCurrentSchema(evolved, 8)
            .setDefaultPartitionSpec(
                PartitionSpec.builderFor(evolved).day("ts").identity("region").build())
            .setDefaultSortOrder(SortOrder.builderFor(evolved).asc("region").desc("ts").build())
            .setProperties(properties)
            .removeProperties(java.util.Set.of("commit.retry.num-retries"))
            .setLastUpdatedMillis(PINNED_TS + 60_000L)
            .discardChanges()
            .build();
    byte[] delta = writerDelta(base, orders, after);
    Log log = Log.cas(base, txn(47), mut -> mut.updateTableInlineDelta(orders, delta), 0L,
        "The change as a TableMetadataDelta: AddSchema with the new schema's JSON (gzip) and "
            + "SetCurrentSchema; AddPartitionSpec and SetDefaultPartitionSpec; AddSortOrder, "
            + "structured (no JSON), and SetDefaultSortOrder; and SetTableProperties with the "
            + "updated and removed keys. Replay ignores the schema, spec and order ids the "
            + "record carries and lets Iceberg's builder assign them, so a commit whose ids "
            + "replay would assign differently goes in full mode.");
    return log.scenario(
        "evolve-table-inline-delta",
        "An UpdateTableInline delta that evolves a table: adds a column, partitions by it, sorts "
            + "by it and changes two properties. Click AddSchema.schema_json for the schema; "
            + "SortField's enums decode to their names.",
        List.of(),
        List.of(gzipView(metadata, meta)),
        state -> {
          TableMetadata committed = inlineMetadata(state, orders);
          expect(committed.schema().findField("region") != null, true, "region column");
          expect(committed.spec().fields().size(), 2, "partition fields");
          expect(committed.sortOrder().fields().size(), 2, "sort fields");
          expect(committed.properties().get("write.parquet.compression-codec"), "lz4", "codec");
          expect(committed.properties().containsKey("commit.retry.num-retries"), false,
              "removed property");
        });
  }

  /**
   * Two pointer tables, and three transactions: T1 updates A; T2, from the snapshot before T1,
   * updates A and B and is rejected as a whole; T3 retries it.
   */
  private static Scenario pointerMultiTableConflictRetry() {
    TableIdentifier a = TableIdentifier.of(WAREHOUSE, "A");
    TableIdentifier b = TableIdentifier.of(WAREHOUSE, "B");
    ProtoCatalogFile base =
        Log.of(warehouse(50))
            .append(txn(51), mut -> mut.createTable(a, "s3://lake/warehouse/A/v1.metadata.json")
                .createTable(b, "s3://lake/warehouse/B/v1.metadata.json"), null)
            .state();
    Log log = Log.checkpoint(base, 0L);
    log.append(txn(52), mut -> mut.updateTable(a, "s3://lake/warehouse/A/v2.metadata.json"),
        "T1: UpdateTableLocation(A, version = 1). Applies; A moves to version 2.");
    log.append(base, txn(53),
        mut -> mut.updateTable(a, "s3://lake/warehouse/A/v3.metadata.json")
            .updateTable(b, "s3://lake/warehouse/B/v2.metadata.json"),
        "T2, from the snapshot T1 did not see: UpdateTableLocation(A, version = 1) and "
            + "UpdateTableLocation(B, version = 1). Rejected: A is at version 2, and a "
            + "transaction applies all its actions or none, so B is not updated either. The "
            + "record stays in the log; replay skips it.");
    ProtoCatalogFile afterT1 = log.state(); // T2 was rejected
    log.append(afterT1, txn(54),
        mut -> mut.updateTable(a, "s3://lake/warehouse/A/v3.metadata.json")
            .updateTable(b, "s3://lake/warehouse/B/v2.metadata.json"),
        "T3: T2 retried on a fresh read, UpdateTableLocation(A, version = 2) and "
            + "UpdateTableLocation(B, version = 1). Applies.");
    return log.scenario(
        "pointer-multi-table-conflict-retry",
        "Two pointer tables A and B. Three appended transactions show version-based conflict "
            + "detection and atomic multi-table commits: T1 commits, T2 (both tables) is "
            + "rejected because A's version is stale, T3 (the retry) commits both.",
        List.of(),
        state -> {
          expect(state.containsTransaction(txn(52)), true, "T1 committed");
          expect(state.containsTransaction(txn(53)), false, "T2 committed");
          expect(state.containsTransaction(txn(54)), true, "T3 committed");
          expect(state.location(a), "s3://lake/warehouse/A/v3.metadata.json", "A");
          expect(state.location(b), "s3://lake/warehouse/B/v2.metadata.json", "B");
        });
  }

  /** The inline counterpart of {@link #pointerMultiTableConflictRetry}: each update a delta. */
  private static Scenario inlineMultiTableConflictRetry() {
    TableIdentifier a = TableIdentifier.of(WAREHOUSE, "A");
    TableIdentifier b = TableIdentifier.of(WAREHOUSE, "B");
    TableMetadata aMeta =
        tableMetadata("019000a1-0000-7000-8000-000000000001", "s3://lake/warehouse/A",
            Map.of("table-name", "A"));
    TableMetadata bMeta =
        tableMetadata("019000b1-0000-7000-8000-000000000001", "s3://lake/warehouse/B",
            Map.of("table-name", "B"));
    byte[] aBytes = InlineMetadataCodecs.JSON_GZIP.encode(aMeta);
    byte[] bBytes = InlineMetadataCodecs.JSON_GZIP.encode(bMeta);
    ProtoCatalogFile base =
        Log.of(warehouse(60))
            .append(txn(61),
                mut -> mut.createTableInline(a, aBytes, InlineMetadataCodecs.TAG_JSON_GZIP)
                    .createTableInline(b, bBytes, InlineMetadataCodecs.TAG_JSON_GZIP),
                null)
            .state();
    long s1 = 0x1111111111111111L;
    long s2a = 0x2222222222222222L;
    long s2b = 0x2222222222222223L;
    long s3a = 0x3333333333333333L;
    long s3b = 0x3333333333333334L;
    Log log = Log.checkpoint(base, 0L);
    byte[] t1 = writerDelta(base, a, appendToMain(s1, null, "A/metadata/snap-1.avro", 60_000L),
        PINNED_TS + 60_000L);
    log.append(txn(62), mut -> mut.updateTableInlineDelta(a, t1),
        "T1: UpdateTableInline(A, version = 1), a delta adding snapshot S1A and moving main to "
            + "it. Applies; A moves to version 2.");
    byte[] t2a = writerDelta(base, a, appendToMain(s2a, null, "A/metadata/snap-2.avro", 120_000L),
        PINNED_TS + 120_000L);
    byte[] t2b = writerDelta(base, b, appendToMain(s2b, null, "B/metadata/snap-2.avro", 120_000L),
        PINNED_TS + 120_000L);
    log.append(base, txn(63),
        mut -> mut.updateTableInlineDelta(a, t2a).updateTableInlineDelta(b, t2b),
        "T2, from the snapshot T1 did not see: UpdateTableInline(A, version = 1) and "
            + "UpdateTableInline(B, version = 1). Rejected as a whole: A is at version 2. B's "
            + "delta, valid on its own, never lands; this record is the only place its "
            + "snapshot appears, and the next CAS write drops it.");
    ProtoCatalogFile afterT1 = log.state(); // T2 was rejected
    byte[] t3a = writerDelta(afterT1, a, appendToMain(s3a, s1, "A/metadata/snap-3.avro", 120_000L),
        PINNED_TS + 180_000L);
    byte[] t3b = writerDelta(afterT1, b, appendToMain(s3b, null, "B/metadata/snap-3.avro", 180_000L),
        PINNED_TS + 180_000L);
    log.append(afterT1, txn(64),
        mut -> mut.updateTableInlineDelta(a, t3a).updateTableInlineDelta(b, t3b),
        "T3: the retry, UpdateTableInline(A, version = 2) with S3A on S1A, and "
            + "UpdateTableInline(B, version = 1). Applies.");
    return log.scenario(
        "inline-multi-table-conflict-retry",
        "The inline counterpart of pointer-multi-table-conflict-retry: each update is the delta "
            + "a writer sends for an append (AddSnapshot, SetSnapshotRef), in UpdateTableInline "
            + "(Action field 9) where the pointer scenario has UpdateTableLocation (field 7). In "
            + "pointer mode T2 names metadata files already written; here its delta is the only "
            + "copy of its snapshots. Click either InlineTable.metadata for the base JSON.",
        List.of(),
        List.of(gzipView(aBytes, aMeta), gzipView(bBytes, bMeta)),
        state -> {
          expect(state.containsTransaction(txn(63)), false, "T2 committed");
          TableMetadata aAfter = inlineMetadata(state, a);
          TableMetadata bAfter = inlineMetadata(state, b);
          expect(aAfter.currentSnapshot().snapshotId(), s3a, "A's current snapshot");
          expect(aAfter.currentSnapshot().parentId(), s1, "S3A's parent");
          expect(bAfter.currentSnapshot().snapshotId(), s3b, "B's current snapshot");
          expect(aAfter.snapshot(s2a) == null && bAfter.snapshot(s2b) == null, true,
              "T2's snapshots absent");
        });
  }

  /**
   * Three writers read one snapshot and each create a table. Replay binds their late-bound ids
   * in log order, so the two that commit get different ids; the third, in the namespace the
   * first changed, is rejected.
   */
  private static Scenario concurrentCreatesBoundAtReplay() {
    Namespace sales = Namespace.of("sales");
    Namespace ops = Namespace.of("ops");
    ProtoCatalogFile base =
        Log.of(emptyCatalog())
            .append(txn(70), mut -> mut.createNamespace(sales).createNamespace(ops), null)
            .state();
    Log log = Log.checkpoint(base, 0L);
    log.append(base, txn(71),
        mut -> mut.createTable(TableIdentifier.of(sales, "orders"),
            "s3://lake/sales/orders/v1.metadata.json"),
        "W1: CreateTable(sales.orders, id = -1, namespace_version = 1). Applies: replay binds "
            + "-1 to next_table_id, 1, and bumps sales to version 2.");
    log.append(base, txn(72),
        mut -> mut.createTable(TableIdentifier.of(ops, "alerts"),
            "s3://lake/ops/alerts/v1.metadata.json"),
        "W2, from the same snapshot: CreateTable(ops.alerts, id = -1). Also -1, but replay binds "
            + "it to the next id now, 2. Before late binding both writers took id 1 from their "
            + "snapshot, and the second create overwrote the first (Bug 10).");
    log.append(base, txn(73),
        mut -> mut.createTable(TableIdentifier.of(sales, "refunds"),
            "s3://lake/sales/refunds/v1.metadata.json"),
        "W3, from the same snapshot: CreateTable(sales.refunds, namespace_version = 1). "
            + "Rejected: W1 moved sales to version 2. Creates in one namespace serialize on "
            + "its version; creates in different namespaces do not conflict.");
    return log.scenario(
        "concurrent-creates-bound-at-replay",
        "Three writers read the same snapshot and each append a create. The records carry the "
            + "same late-bound id; replay binds them to different ids in log order. The parent "
            + "namespace's version orders creates under it.",
        List.of(),
        state -> {
          expect(state.tableId(TableIdentifier.of(sales, "orders")), 1, "sales.orders id");
          expect(state.tableId(TableIdentifier.of(ops, "alerts")), 2, "ops.alerts id");
          expect(state.containsTable(TableIdentifier.of(sales, "refunds")), false,
              "sales.refunds exists");
        });
  }

  /** A rename into another namespace, and a stale update of the renamed table. */
  private static Scenario renameTableAcrossNamespaces() {
    Namespace staging = Namespace.of("staging");
    Namespace prod = Namespace.of("prod");
    TableIdentifier from = TableIdentifier.of(staging, "daily");
    TableIdentifier to = TableIdentifier.of(prod, "daily_v2");
    ProtoCatalogFile base =
        Log.of(emptyCatalog())
            .append(txn(80), mut -> mut.createNamespace(staging).createNamespace(prod)
                .createTable(from, "s3://lake/staging/daily/v1.metadata.json"), null)
            .state();
    Log log = Log.checkpoint(base, 0L);
    log.append(base, txn(81), mut -> mut.renameTable(from, to),
        "RenameTable(id = 1, version = 1, new_namespace_id = 2, new_namespace_version = 1, "
            + "new_name = \"daily_v2\"). Applies: the table keeps its id and metadata, moves to "
            + "version 2, and both namespaces' versions are bumped, as a drop and a create "
            + "would bump them.");
    log.append(base, txn(82),
        mut -> mut.updateTable(from, "s3://lake/staging/daily/v2.metadata.json"),
        "An update from the snapshot before the rename: UpdateTableLocation(id = 1, version = "
            + "1). Rejected: the rename moved the table to version 2.");
    return log.scenario(
        "rename-table-across-namespaces",
        "A RenameTable (Action field 11) moving a table to another namespace, then a stale "
            + "writer's update of it. The table is addressed by id, so the update finds it, but "
            + "its version check fails.",
        List.of(),
        state -> {
          expect(state.tableId(to), 1, "prod.daily_v2 id");
          expect(state.containsTable(from), false, "staging.daily exists");
          expect(state.location(to), "s3://lake/staging/daily/v1.metadata.json", "location");
        });
  }

  /**
   * An inline-ML table: its snapshots' manifest lists live in the catalog, as a manifest pool
   * and each snapshot's indices into it.
   */
  private static Scenario inlineManifestList() {
    TableIdentifier clicks = TableIdentifier.of(WAREHOUSE, "clicks");
    TableMetadata meta =
        tableMetadata("019000dd-0000-7000-8000-000000000001", "s3://lake/warehouse/clicks",
            Map.of("table-name", "clicks"));
    byte[] metadata = InlineMetadataCodecs.JSON_GZIP.encode(meta);
    long s1 = 0x5151515151515151L;
    long s2 = 0x5252525252525252L;
    ManifestFile m1 = manifest("clicks/metadata/m1.avro", s1, 1, 20_200);
    ManifestFile m2 = manifest("clicks/metadata/m2.avro", s1, 1, 20_201);
    ManifestFile m3 = manifest("clicks/metadata/m3.avro", s2, 2, 20_200);
    // Inline-ML deltas are built by hand: the writer's manifest updates come from the
    // InlineSnapshot its producer returned, which only FileIOCatalog holds.
    byte[] first =
        InlineDeltaCodec.encodeDelta(
            List.of(
                append(s1, null, "", 60_000L),
                main(s1),
                new InlineDeltaCodec.AddManifestUpdate(s1, m1, "", 0),
                new InlineDeltaCodec.AddManifestUpdate(s1, m2, "", 1)),
            PINNED_TS + 60_000L);
    ProtoCatalogFile afterFirst =
        Log.of(warehouse(90))
            .append(txn(91),
                mut -> mut.createTableInline(clicks, metadata, InlineMetadataCodecs.TAG_JSON_GZIP),
                null)
            .append(txn(92), mut -> mut.updateTableInlineDelta(clicks, first), null)
            .state();
    byte[] second =
        InlineDeltaCodec.encodeDelta(
            List.of(
                append(s2, s1, "", 120_000L),
                main(s2),
                new InlineDeltaCodec.RemoveManifestUpdate(s2, m1.path()),
                new InlineDeltaCodec.AddManifestUpdate(s2, m3, "", 0)),
            PINNED_TS + 120_000L);
    Log log = Log.cas(afterFirst, txn(93), mut -> mut.updateTableInlineDelta(clicks, second), 0L,
        "A CAS write. The checkpoint's InlineTable holds the pool [m1, m2] and S1's indices "
            + "[0, 1]. The record's delta adds S2 on S1 with an empty manifest_list_suffix (its "
            + "list is inline), removes m1 from S2's list and inserts m3 at position 0: replay "
            + "builds S2's list from its parent's, less the removed, with the added at their "
            + "positions, [m3, m2].");
    return log.scenario(
        "inline-manifest-list",
        "An inline-ML table (fileio.catalog.inline.manifests): no manifest list files. The "
            + "checkpoint carries each table's manifest_pool and per-snapshot snapshot_refs; a "
            + "delta changes a snapshot's list with AddManifestDelta and RemoveManifestDelta.",
        List.of(
            "snapshot_refs.pool_indices is a repeated int32 the codec writes unpacked, one "
                + "varint each: catalog.proto declares it [packed = false]."),
        List.of(gzipView(metadata, meta)),
        state -> {
          int id = state.tableId(clicks);
          expect(paths(state.inlineManifests(id, s1)), List.of(m1.path(), m2.path()), "S1's list");
          expect(paths(state.inlineManifests(id, s2)), List.of(m3.path(), m2.path()), "S2's list");
        });
  }

  /**
   * A CAS write whose checkpoint drops the oldest committed transaction ids, as every CAS write
   * does for ids older than the retention window, and records the horizon.
   */
  private static Scenario compactionWithCommittedSetGc() {
    Namespace bronze = Namespace.of("warehouse", "bronze");
    TableIdentifier events = TableIdentifier.of(bronze, "events");
    Log history = Log.of(emptyCatalog());
    history.append(txn(100), mut -> mut.createNamespace(WAREHOUSE), null);
    history.append(txn(101), mut -> mut.createNamespace(bronze), null);
    history.append(txn(102),
        mut -> mut.createTable(events, "s3://lake/warehouse/bronze/events/v1.metadata.json"),
        null);
    history.append(txn(103),
        mut -> mut.updateProperties(WAREHOUSE, Map.of("owner", "data-platform")), null);
    history.append(txn(104),
        mut -> mut.updateTable(events, "s3://lake/warehouse/bronze/events/v2.metadata.json"),
        null);
    ProtoCatalogFile base = history.state();
    long watermark = UuidV7.timestampMs(txn(101));
    Log log = Log.cas(base, txn(105),
        mut -> mut.updateTable(events, "s3://lake/warehouse/bronze/events/v3.metadata.json"),
        watermark,
        "The compacting commit's own record, UpdateTableLocation(events, version = 2). Its id "
            + "joins the committed set when replay applies it.");
    return log.scenario(
        "compaction-with-committed-set-gc",
        "A CAS write: the checkpoint holds two nested namespaces, a pointer table and a "
            + "namespace property, then the commit's record follows. The checkpoint's "
            + "CommittedTransactions (field 20) is the dedup set: UUIDv7s sorted descending, each "
            + "a varint gap from the previous timestamp plus 10 random bytes, in one "
            + "packed_entries blob the catalog scans without allocating. Click it to see the "
            + "UUIDs.",
        List.of(
            "GC: this CAS write dropped the ids at or below its watermark (now - "
                + "fileio.catalog.committed.retention.ms, 6 hours by default) and recorded the "
                + "largest dropped timestamp as highest_dropped_timestamp_ms. A lookup of an id "
                + "at or below it is unknown (DedupHorizonExceededException), not 'absent': "
                + "the commit may have landed and aged out."),
        state -> {
          expect(state.highestDroppedTimestampMs(), watermark, "horizon");
          expect(state.containsTransaction(txn(105)), true, "the compacting commit");
          expect(state.containsTransaction(txn(102)), true, "a retained id");
          try {
            state.containsTransaction(txn(100));
            throw new AssertionError("expected an id below the horizon to be unknown");
          } catch (DedupHorizonExceededException expected) {
            // an id at or below the horizon is neither committed nor absent
          }
        });
  }

  // ============================================================
  // Building catalog files
  // ============================================================

  /**
   * A catalog file as writers build it: a header and checkpoint, then records, each the
   * transaction a writer makes for a change to the snapshot it read.
   */
  private static final class Log {
    private byte[] file;
    private final List<Note> notes = new ArrayList<>();

    private Log(byte[] file) {
      this.file = file;
    }

    /** A file of a checkpoint of {@code state} and no records. */
    static Log checkpoint(ProtoCatalogFile state, long watermark) {
      return new Log(header(checkpointBytes(state, watermark)));
    }

    /** A file of a checkpoint of {@code state}, to build a later state with records. */
    static Log of(ProtoCatalogFile state) {
      return checkpoint(state, 0L);
    }

    /**
     * A CAS write of the change {@code change} makes to {@code base}: a checkpoint of {@code
     * base}, dropping committed ids at or below {@code watermark}, and the change's record.
     */
    static Log cas(ProtoCatalogFile base, UUID id, Consumer<ProtoCatalogFormat.Mut> change,
        long watermark, String note) {
      return checkpoint(base, watermark).append(base, id, change, note);
    }

    /** Appends the record of a writer that read the current state. */
    Log append(UUID id, Consumer<ProtoCatalogFormat.Mut> change, String note) {
      return append(state(), id, change, note);
    }

    /** Appends the record of a writer that read {@code snapshot}. */
    Log append(ProtoCatalogFile snapshot, UUID id, Consumer<ProtoCatalogFormat.Mut> change,
        String note) {
      ProtoCatalogFormat.Mut mut = new ProtoCatalogFormat.Mut(snapshot);
      change.accept(mut);
      List<ProtoCodec.Action> actions = new ArrayList<>();
      mut.buildActions(actions, new ProtoCatalogFormat.ProtoIdManager());
      byte[] body = ProtoCodec.encodeTransaction(new ProtoCodec.Transaction(id, actions));
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.writeBytes(file);
      writeVarint(out, body.length);
      out.writeBytes(body);
      if (note != null) {
        notes.add(new Note(file.length, out.size(), note));
      }
      file = out.toByteArray();
      return this;
    }

    /** The state the file replays to. */
    ProtoCatalogFile state() {
      return replay(file);
    }

    Scenario scenario(String name, String description, List<String> fileNotes,
        Consumer<ProtoCatalogFile> check) {
      return scenario(name, description, fileNotes, List.of(), check);
    }

    Scenario scenario(String name, String description, List<String> fileNotes,
        List<StructuralView> views, Consumer<ProtoCatalogFile> check) {
      List<Note> all = new ArrayList<>(notes);
      for (String note : fileNotes) {
        all.add(new Note(-1, -1, note));
      }
      return new Scenario(name, description, file, all, views, check);
    }
  }

  private static ProtoCatalogFile replay(byte[] file) {
    try {
      return ProtoCatalogFormat.readInternal(LOCATION, new ByteArrayInputStream(file), file.length);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static byte[] header(byte[] checkpoint) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.writeBytes(ProtoCatalogFormat.MAGIC);
    out.writeBytes(ByteBuffer.allocate(4).putInt(ProtoCatalogFormat.FORMAT_VERSION).array());
    writeVarint(out, checkpoint.length);
    out.writeBytes(checkpoint);
    return out.toByteArray();
  }

  private static byte[] checkpointBytes(ProtoCatalogFile state, long watermark) {
    ProtoCatalogFormat.ProtoIdManager ids = new ProtoCatalogFormat.ProtoIdManager();
    ids.setGlobals(state.nextNamespaceId(), state.nextTableId());
    return ProtoCodec.encodeCheckpoint(state, new ProtoCatalogFormat.Mut(state), ids, watermark);
  }

  private static void writeVarint(ByteArrayOutputStream out, int value) {
    while ((value & ~0x7F) != 0) {
      out.write((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    out.write(value);
  }

  private static ProtoCatalogFile emptyCatalog() {
    return ProtoCatalogFile.builder(LOCATION)
        .setCatalogUuid(CATALOG_UUID)
        .setNextNamespaceId(1)
        .setNextTableId(1)
        .build();
  }

  /** A catalog holding namespace {@code warehouse}, created by an initialized catalog. */
  private static ProtoCatalogFile warehouse(int seq) {
    return Log.cas(emptyCatalog(), txn(1000 + seq), mut -> {}, 0L, null)
        .append(txn(2000 + seq), mut -> mut.createNamespace(WAREHOUSE), null)
        .state();
  }

  /**
   * A UUIDv7 transaction id, pinned: {@code n} seconds after the fixtures' epoch, its random bits
   * seeded by {@code n}.
   */
  private static UUID txn(int n) {
    return UuidV7.newUuidV7(PINNED_TS + n * 1000L, new Random(n));
  }

  // ============================================================
  // Inline table metadata
  // ============================================================

  /**
   * The delta a writer sends for the commit {@code updates} make to {@code table} in {@code
   * snapshot}: {@link InlineDeltaCodec#computeDelta} of the metadata before and after, as {@code
   * FileIOCatalog} stages it. Fails if the updates do not commit, or if replay of the computed
   * delta does not produce the same metadata.
   */
  private static byte[] writerDelta(ProtoCatalogFile snapshot, TableIdentifier table,
      List<InlineDeltaCodec.DeltaUpdate> updates, long lastUpdatedMillis) {
    byte[] intended = InlineDeltaCodec.encodeDelta(updates, lastUpdatedMillis);
    TableMetadata after =
        inlineMetadata(
            Log.of(snapshot).append(txn(9999), mut -> mut.updateTableInlineDelta(table, intended),
                null).state(),
            table);
    return writerDelta(snapshot, table, after);
  }

  /**
   * The delta a writer sends to commit {@code after} to {@code table} in {@code snapshot}. Fails
   * if the commit goes in full mode, or if replay of the delta does not produce {@code after}.
   */
  private static byte[] writerDelta(
      ProtoCatalogFile snapshot, TableIdentifier table, TableMetadata after) {
    TableMetadata before = inlineMetadata(snapshot, table);
    List<InlineDeltaCodec.DeltaUpdate> delta =
        InlineDeltaCodec.computeDelta(before, after, snapshot.manifestListPrefix(
            snapshot.tableId(table)));
    if (delta == null) {
      throw new AssertionError("computeDelta sends " + table + "'s commit in full mode");
    }
    byte[] bytes = InlineDeltaCodec.encodeDelta(delta, after.lastUpdatedMillis());
    TableMetadata replayed =
        inlineMetadata(
            Log.of(snapshot).append(txn(9999), mut -> mut.updateTableInlineDelta(table, bytes),
                null).state(),
            table);
    if (!TableMetadataParser.toJson(replayed).equals(TableMetadataParser.toJson(after))) {
      throw new AssertionError("the computed delta of " + table + " replays differently:\n"
          + TableMetadataParser.toJson(after) + "\n" + TableMetadataParser.toJson(replayed)
          + "\n" + delta);
    }
    return bytes;
  }

  /** An append moving main to it, {@code timestampDeltaMs} after the base's last update. */
  private static List<InlineDeltaCodec.DeltaUpdate> appendToMain(
      long snapshotId, Long parentId, String manifestList, long timestampDeltaMs) {
    return List.of(append(snapshotId, parentId, manifestList, timestampDeltaMs), main(snapshotId));
  }

  /** An append snapshot of format version 2: no row lineage. */
  private static InlineDeltaCodec.AddSnapshotUpdate append(
      long snapshotId, Long parentId, String manifestList, long timestampDeltaMs) {
    Map<String, String> summary = new LinkedHashMap<>();
    summary.put("operation", "append");
    summary.put("added-data-files", "1");
    summary.put("added-records", "1024");
    summary.put("total-data-files", "1");
    summary.put("total-records", "1024");
    return new InlineDeltaCodec.AddSnapshotUpdate(
        snapshotId, manifestList, summary, timestampDeltaMs, null, null, parentId, null, null);
  }

  private static InlineDeltaCodec.SetSnapshotRefUpdate main(long snapshotId) {
    return new InlineDeltaCodec.SetSnapshotRefUpdate("main", snapshotId, "branch", 0, 0L, 0L);
  }

  /** A data manifest of one file in day {@code day} of the demo spec. */
  private static ManifestFile manifest(String path, long snapshotId, long sequenceNumber,
      int day) {
    ByteBuffer bound = ByteBuffer.allocate(4).order(java.nio.ByteOrder.LITTLE_ENDIAN).putInt(0, day);
    return new ProtoCodec.DecodedManifestFile(
        path, 6_144L, 0, 0, sequenceNumber, sequenceNumber, snapshotId,
        1, 0, 0, 1_024L, 0L, 0L,
        List.of(new GenericPartitionFieldSummary(false, false, bound, bound)),
        null, null);
  }

  private static List<String> paths(List<ManifestFile> manifests) {
    return manifests.stream().map(ManifestFile::path).collect(Collectors.toList());
  }

  /**
   * {@code table}'s metadata as replay decodes it to apply a delta: under the synthetic location
   * that the committed metadata's log then names as its previous file.
   */
  private static TableMetadata replayBase(ProtoCatalogFile state, TableIdentifier table) {
    int id = state.tableId(table);
    byte[] bytes = state.inlineMetadata(id);
    return InlineMetadataCodecs.byTag(state.inlineMetadataCodec(id))
        .decodeFull(bytes, "inline://#" + Integer.toHexString(Arrays.hashCode(bytes)));
  }

  private static TableMetadata inlineMetadata(ProtoCatalogFile state, TableIdentifier table) {
    int id = state.tableId(table);
    return InlineMetadataCodecs.byTag(state.inlineMetadataCodec(id))
        .decodeFull(state.inlineMetadata(id), null);
  }

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

  /**
   * A table's first metadata, format version 2, day-partitioned on ts, with the UUID and
   * last-updated-ms pinned so the encoding is the same on every run.
   */
  private static TableMetadata tableMetadata(
      String tableUuid, String location, Map<String, String> extraProps) {
    Schema schema = demoSchema();
    Map<String, String> props = new LinkedHashMap<>();
    props.put("write.format.default", "parquet");
    props.put("write.parquet.compression-codec", "zstd");
    props.put("write.target-file-size-bytes", "536870912");
    props.put("owner", "data-platform");
    props.putAll(extraProps);
    return TableMetadata.buildFromEmpty()
        .setLocation(location)
        .setCurrentSchema(schema, /* lastColumnId */ 7)
        .addPartitionSpec(PartitionSpec.builderFor(schema).day("ts").build())
        .addSortOrder(SortOrder.unsorted())
        .setProperties(props)
        .assignUUID(tableUuid)
        .setLastUpdatedMillis(PINNED_TS)
        .discardChanges()
        .build();
  }

  /** The explorer's view of gzip-codec metadata: the JSON, and the gzip envelope as a segment. */
  private static StructuralView gzipView(byte[] gzipBytes, TableMetadata meta) {
    String json = TableMetadataParser.toJson(meta);
    return new StructuralView(
        gzipBytes,
        prettyJson(json),
        List.of(new StructuralSegment(
            "gzip-wrapped JSON (decompresses to " + json.length() + " bytes of TableMetadata)",
            gzipBytes)));
  }

  /** The explorer's view of structural-codec metadata: the JSON, and a segment per field. */
  private static StructuralView structuralView(byte[] structBytes, TableMetadata meta) {
    return new StructuralView(
        structBytes, prettyJson(TableMetadataParser.toJson(meta)), structuralSegments(structBytes));
  }

  private static String prettyJson(String json) {
    try {
      com.fasterxml.jackson.databind.ObjectMapper mapper =
          new com.fasterxml.jackson.databind.ObjectMapper();
      return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mapper.readTree(json));
    } catch (IOException e) {
      return json;
    }
  }

  /**
   * The top-level segments of a structural payload ({@link StructuralInlineMetadataCodec}): after
   * gunzip, a varint layout version, the JSON without snapshots and metadata log, then the
   * snapshot and metadata-log blocks, each after a varint length. The blocks are not opened.
   */
  private static List<StructuralSegment> structuralSegments(byte[] gzipped) {
    byte[] inner = InlineMetadataIO.gunzip(gzipped);
    List<StructuralSegment> out = new ArrayList<>();
    int[] pos = {0};
    long version = segmentVarint(inner, pos, out, "format_version");
    if (version != StructuralInlineMetadataCodec.FORMAT_VERSION) {
      throw new AssertionError("structural layout version " + version);
    }
    long jsonLength = segmentVarint(inner, pos, out, "json_len");
    segment(inner, pos, (int) jsonLength, out,
        "stripped_json (TableMetadata minus snapshots and metadata-log)");
    long snapLength = segmentVarint(inner, pos, out, "snap_block_len");
    segment(inner, pos, (int) snapLength, out,
        snapLength == 1
            ? "snap_block (empty: N=0)"
            : "snap_block (columnar: snapshot ids, parents, summaries, times, schema ids, "
                + "manifest-list templates)");
    long logLength = segmentVarint(inner, pos, out, "mdlog_block_len");
    segment(inner, pos, (int) logLength, out,
        logLength == 1
            ? "mdlog_block (empty: N=0)"
            : "mdlog_block (columnar: times and path templates of the metadata log)");
    return out;
  }

  private static long segmentVarint(byte[] buf, int[] pos, List<StructuralSegment> out,
      String label) {
    int start = pos[0];
    long value = 0;
    int shift = 0;
    byte b;
    do {
      b = buf[pos[0]++];
      value |= (long) (b & 0x7F) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    out.add(new StructuralSegment(label + " (varint = " + value + ")",
        Arrays.copyOfRange(buf, start, pos[0])));
    return value;
  }

  private static void segment(byte[] buf, int[] pos, int length, List<StructuralSegment> out,
      String label) {
    out.add(new StructuralSegment(label, Arrays.copyOfRange(buf, pos[0], pos[0] + length)));
    pos[0] += length;
  }

  private static void expect(Object actual, Object expected, String what) {
    Object a = actual instanceof Number && expected instanceof Number
        ? (Object) ((Number) actual).longValue() : actual;
    Object e = actual instanceof Number && expected instanceof Number
        ? (Object) ((Number) expected).longValue() : expected;
    if (!java.util.Objects.equals(a, e)) {
      throw new AssertionError(what + ": expected " + expected + ", got " + actual);
    }
  }

  // ============================================================
  // JSON emission (no external dependency -- the schema/scenarios are simple)
  // ============================================================

  private static String emitJson(CatalogProto.Schema schema, List<Scenario> scenarios) {
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("  \"formatVersion\": ").append(ProtoCatalogFormat.FORMAT_VERSION).append(",\n");
    sb.append("  \"readerVersion\": ").append(ProtoCodec.SUPPORTED_READER_VERSION).append(",\n");
    sb.append("  \"structuralFormatVersion\": ")
        .append(StructuralInlineMetadataCodec.FORMAT_VERSION).append(",\n");
    sb.append("  \"deflateProbe\": ").append(jsonString(deflateProbe())).append(",\n");
    sb.append("  \"schema\": ");
    appendSchemaJson(sb, schema);
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
        sb.append(j < s.notes.size() - 1 ? ",\n" : "\n");
      }
      sb.append("      ]");
      if (!s.structuralViews.isEmpty()) {
        sb.append(",\n      \"structuralViews\": [\n");
        for (int v = 0; v < s.structuralViews.size(); v++) {
          StructuralView view = s.structuralViews.get(v);
          sb.append("        {\n");
          sb.append("          \"bytesHex\": ").append(jsonString(view.bytesHex)).append(",\n");
          sb.append("          \"recoveredJson\": ")
              .append(jsonString(view.recoveredJson)).append(",\n");
          sb.append("          \"compactSegments\": [\n");
          for (int j = 0; j < view.compactSegments.size(); j++) {
            StructuralSegment seg = view.compactSegments.get(j);
            sb.append("            {\"label\": ").append(jsonString(seg.label))
                .append(", \"hex\": ").append(jsonString(seg.hex))
                .append(", \"byteCount\": ").append(seg.byteCount).append("}");
            sb.append(j < view.compactSegments.size() - 1 ? ",\n" : "\n");
          }
          sb.append("          ]\n");
          sb.append("        }");
          sb.append(v < s.structuralViews.size() - 1 ? ",\n" : "\n");
        }
        sb.append("      ]");
      }
      sb.append("\n    }");
      sb.append(i < scenarios.size() - 1 ? ",\n" : "\n");
    }
    sb.append("  ]\n");
    sb.append("}\n");
    return sb.toString();
  }

  private static void appendSchemaJson(StringBuilder sb, CatalogProto.Schema schema) {
    String pad = "  ";
    sb.append("{\n");
    sb.append(pad).append("  \"messages\": {\n");
    int mi = 0;
    for (Map.Entry<String, CatalogProto.Message> e : schema.messages.entrySet()) {
      sb.append(pad).append("    ").append(jsonString(e.getKey())).append(": {\n");
      sb.append(pad).append("      \"fields\": [\n");
      List<CatalogProto.Field> fields = e.getValue().fields;
      for (int i = 0; i < fields.size(); i++) {
        CatalogProto.Field f = fields.get(i);
        sb.append(pad).append("        {")
            .append("\"number\": ").append(f.number)
            .append(", \"name\": ").append(jsonString(f.name))
            .append(", \"type\": ").append(jsonString(f.type))
            .append(", \"repeated\": ").append(f.repeated)
            .append(", \"packed\": ").append(f.packed)
            .append(", \"oneof\": ").append(f.oneof == null ? "null" : jsonString(f.oneof))
            .append(", \"comment\": ").append(jsonString(f.comment))
            .append("}");
        sb.append(i < fields.size() - 1 ? ",\n" : "\n");
      }
      sb.append(pad).append("      ]\n");
      sb.append(pad).append("    }");
      sb.append(++mi < schema.messages.size() ? ",\n" : "\n");
    }
    sb.append(pad).append("  },\n");
    sb.append(pad).append("  \"enums\": {\n");
    int ei = 0;
    for (Map.Entry<String, CatalogProto.EnumDef> e : schema.enums.entrySet()) {
      sb.append(pad).append("    ").append(jsonString(e.getKey())).append(": [\n");
      int vi = 0;
      for (Map.Entry<String, Integer> v : e.getValue().values.entrySet()) {
        sb.append(pad).append("      {")
            .append("\"name\": ").append(jsonString(v.getKey()))
            .append(", \"number\": ").append(v.getValue())
            .append("}");
        sb.append(++vi < e.getValue().values.size() ? ",\n" : "\n");
      }
      sb.append(pad).append("    ]");
      sb.append(++ei < schema.enums.size() ? ",\n" : "\n");
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
        case '\n':
          sb.append("\\n");
          break;
        case '\r':
          sb.append("\\r");
          break;
        case '\t':
          sb.append("\\t");
          break;
        case '<':
          // Keeps "</script>" in a value from closing the fixtures block.
          sb.append("\\u003c");
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

  static String toHex(byte[] bytes) {
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
    /**
     * One {@link StructuralView} per inline-TM bytes field carried in this scenario. The explorer
     * matches a clicked bytes node to its view by the hex of its bytes; other bytes fall back to
     * the standard "<N bytes>" display, or to the explorer's own decode.
     */
    final List<StructuralView> structuralViews;
    /** Checks the state the file replays to against the scenario's notes. */
    final Consumer<ProtoCatalogFile> check;

    Scenario(String name, String description, byte[] bytes, List<Note> notes,
        List<StructuralView> structuralViews, Consumer<ProtoCatalogFile> check) {
      this.name = name;
      this.description = description;
      this.bytes = bytes;
      this.notes = notes;
      this.structuralViews = structuralViews;
      this.check = check;
    }
  }

  /**
   * Decoded preview of an inline-TM bytes record: the recovered canonical JSON plus labelled
   * segments of its (gzip-decompressed) wire layout, keyed by {@link #bytesHex}.
   */
  static final class StructuralView {
    final String bytesHex;
    final String recoveredJson;
    final List<StructuralSegment> compactSegments;

    StructuralView(byte[] bytes, String recoveredJson, List<StructuralSegment> compactSegments) {
      this.bytesHex = toHex(bytes);
      this.recoveredJson = recoveredJson;
      this.compactSegments = compactSegments;
    }
  }

  static final class StructuralSegment {
    final String label;
    final String hex;
    final int byteCount;

    StructuralSegment(String label, byte[] bytes) {
      this.label = label;
      this.hex = toHex(bytes);
      this.byteCount = bytes.length;
    }
  }

  /** A note on bytes {@code [start, end)}, or on the whole file if both are -1. */
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

  /** A minimal in-memory InputFile; replay needs only its location. */
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
