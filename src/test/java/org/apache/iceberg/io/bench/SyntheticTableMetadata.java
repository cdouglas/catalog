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
package org.apache.iceberg.io.bench;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.InlineDeltaSnapshots;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Generates synthetic Iceberg {@link TableMetadata} for benchmarking metadata
 * encodings. Two interfaces:
 *
 * <ul>
 *   <li>{@link Knobs} — direct parameter control for grid-search benchmarks.
 *   <li>{@link Knobs#forScenario(Scenario, Intensity)} — preset combinations
 *       that emphasize one production pathology while keeping the rest realistic.
 * </ul>
 *
 * <p>Output is constructed via Iceberg's public {@link TableMetadata.Builder}
 * API, matching the same code path the catalog uses on commit. All randomness
 * is seeded; identical {@code Knobs} produce identical bytes.
 *
 * <p>Sanity-check via {@code main()}:
 * <pre>{@code
 * mvn test-compile exec:java \
 *   -Dexec.mainClass=org.apache.iceberg.io.bench.SyntheticTableMetadata \
 *   -Dexec.classpathScope=test
 * }</pre>
 */
public final class SyntheticTableMetadata {

  /** Realistic snapshot summary keys borrowed from TableMetadataV2Valid.json. */
  private static final String[] SUMMARY_KEYS = {
      "added-data-files",
      "added-records",
      "added-files-size",
      "total-records",
      "total-files-size",
      "total-data-files",
      "total-delete-files",
      "total-position-deletes",
      "total-equality-deletes",
      "spark.app.id"
  };

  /**
   * Property key prefixes seen on real Iceberg tables for *user-defined* properties (not
   * Iceberg's own typed config — those constrain values, e.g. numeric or boolean). These mimic
   * what ETL pipelines stuff into table properties: pipeline IDs, descriptions, business tags.
   */
  private static final String[] USER_PROPERTY_KEY_PREFIXES = {
      "owner",
      "team",
      "pipeline.id",
      "description",
      "data.quality.last-check",
      "lineage.upstream",
      "lineage.downstream",
      "schedule.cron",
      "tag.environment",
      "tag.classification",
      "config.spark.dump",
      "config.airflow.dag",
  };

  /** Distribution of snapshot timestamps — affects compressibility and parent chain. */
  public enum Distribution {
    /** All snapshots within 60 seconds — streaming ingest. */
    CLUSTERED_60S(60_000L),
    /** Uniform over 24 hours — typical batch. */
    UNIFORM_24H(86_400_000L),
    /** Uniform over 1 year — sparse, long-lived catalogs. */
    SPARSE_1Y(365L * 86_400_000L);

    final long spanMs;

    Distribution(long spanMs) {
      this.spanMs = spanMs;
    }
  }

  /** Intensity dial for {@link Knobs#forScenario(Scenario, Intensity)}. */
  public enum Intensity {
    SMALL,
    MEDIUM,
    LARGE
  }

  /** Generator inputs. Public mutable fields keep call sites short. */
  public static final class Knobs {
    public int snapshots = 10;
    public int schemaFields = 10;
    public int schemaVersions = 1;
    public int partitionSpecs = 1;
    public int sortOrders = 1;
    public int refs = 1;
    public int properties = 5;
    public int propertyValueSize = 32;
    public boolean statsPerSnapshot = false;
    public int blobsPerStats = 0;
    public int metadataLogEntries = 0;
    public Distribution commitDistribution = Distribution.UNIFORM_24H;
    public long seed = 0xC0FFEEL;

    public Knobs() {}

    /** Build a Knobs preset tuned to the given scenario at the given intensity. */
    public static Knobs forScenario(Scenario scenario, Intensity intensity) {
      Knobs k = new Knobs();
      int s = intensity.ordinal();
      switch (scenario) {
        case NORMAL:
          k.snapshots = pick(s, 10, 100, 1000);
          break;
        case SNAPSHOT_ACCUMULATION:
          k.snapshots = pick(s, 1_000, 10_000, 50_000);
          break;
        case SCHEMA_SPEC_CHURN:
          k.snapshots = 500;
          k.schemaVersions = pick(s, 10, 50, 500);
          k.partitionSpecs = pick(s, 10, 50, 500);
          k.sortOrders = pick(s, 10, 50, 500);
          break;
        case MANY_REFS:
          k.snapshots = 500;
          k.refs = pick(s, 10, 200, 5_000);
          break;
        case VERBOSE_PROPERTIES:
          k.properties = pick(s, 20, 50, 200);
          k.propertyValueSize = 4_096;
          break;
        case STATS_PUFFIN_REFS:
          k.snapshots = pick(s, 100, 1_000, 5_000);
          k.statsPerSnapshot = true;
          k.blobsPerStats = 2;
          break;
        case COMMIT_FREQUENCY:
          k.snapshots = 1_000;
          k.commitDistribution =
              pick(s, Distribution.CLUSTERED_60S, Distribution.UNIFORM_24H, Distribution.SPARSE_1Y);
          break;
        case METADATA_LOG_RETENTION:
          k.metadataLogEntries = pick(s, 50, 500, 5_000);
          break;
        case COMBINED_WORST_CASE:
          k.snapshots = 5_000;
          k.schemaVersions = 50;
          k.partitionSpecs = 10;
          k.sortOrders = 10;
          k.refs = 200;
          k.properties = 50;
          k.propertyValueSize = 1_024;
          k.statsPerSnapshot = true;
          k.blobsPerStats = 2;
          k.metadataLogEntries = 200;
          k.commitDistribution = Distribution.SPARSE_1Y;
          break;
      }
      k.seed = 0xC0FFEEL ^ ((long) scenario.ordinal() * 31L + intensity.ordinal());
      return k;
    }

    @Override
    public String toString() {
      return String.format(
          Locale.ROOT,
          "Knobs(snap=%d, schemaFields=%d, schemaVer=%d, specs=%d, orders=%d, refs=%d, "
              + "props=%d × %dB, stats=%s, mdLog=%d, dist=%s)",
          snapshots, schemaFields, schemaVersions, partitionSpecs, sortOrders, refs,
          properties, propertyValueSize, statsPerSnapshot, metadataLogEntries, commitDistribution);
    }
  }

  private SyntheticTableMetadata() {}

  /** Generate a TableMetadata according to {@code k}. Deterministic given {@code k.seed}. */
  public static TableMetadata generate(Knobs k) {
    Random rand = new Random(k.seed);
    long now = 1_700_000_000_000L;

    List<Schema> schemas = generateSchemas(k.schemaVersions, k.schemaFields, rand);
    Schema currentSchema = schemas.get(schemas.size() - 1);

    // Iceberg requires every retained PartitionSpec/SortOrder to reference field IDs that
    // exist in the *current* schema (build-time check at TableMetadata.Builder.build).
    // Real tables accumulate spec/sort-order versions through partition evolution against the
    // then-current schema; for synthetic generation it's sufficient (and valid) to build
    // every retained version against the latest schema.
    List<PartitionSpec> specs = new ArrayList<>();
    for (int i = 0; i < Math.max(1, k.partitionSpecs); i++) {
      specs.add(generatePartitionSpec(currentSchema, i, rand));
    }
    List<SortOrder> sortOrders = new ArrayList<>();
    for (int i = 0; i < Math.max(1, k.sortOrders); i++) {
      sortOrders.add(generateSortOrder(currentSchema, i, rand));
    }

    String location =
        "s3://example-bucket/warehouse/db/synthetic_" + Math.abs(rand.nextInt());
    String tableUuid = new UUID(rand.nextLong(), rand.nextLong()).toString();

    TableMetadata.Builder builder = TableMetadata.buildFromEmpty()
        .assignUUID(tableUuid)
        .setLocation(location);

    for (Schema schema : schemas) {
      builder.addSchema(schema);
    }
    builder.setCurrentSchema(currentSchema.schemaId());
    for (PartitionSpec spec : specs) {
      builder.addPartitionSpec(spec);
    }
    builder.setDefaultPartitionSpec(specs.get(specs.size() - 1));
    for (SortOrder so : sortOrders) {
      builder.addSortOrder(so);
    }
    builder.setDefaultSortOrder(sortOrders.get(sortOrders.size() - 1));
    Map<String, String> initialProps = new LinkedHashMap<>();
    if (k.properties > 0) {
      initialProps.putAll(generateProperties(k.properties, k.propertyValueSize, rand));
    }
    if (k.metadataLogEntries > 0) {
      // Iceberg caps the previous-metadata-log at this property's value (default 100). The cap
      // must be set BEFORE the entries are appended, so include it in the initial properties.
      initialProps.put("write.metadata.previous-versions-max",
          Integer.toString(k.metadataLogEntries + 1));
    }
    if (!initialProps.isEmpty()) {
      builder.setProperties(initialProps);
    }
    builder.setLastUpdatedMillis(now);
    TableMetadata metadata = builder.build();

    long[] timestamps = generateTimestamps(k.snapshots, now, k.commitDistribution, rand);
    long[] snapshotIds = new long[k.snapshots];
    for (int i = 0; i < k.snapshots; i++) {
      // Avoid 0 / negative-id collisions; iceberg snapshot ids are signed but real ids are positive.
      snapshotIds[i] = Math.abs(rand.nextLong() | 1L);
    }

    for (int i = 0; i < k.snapshots; i++) {
      Long parentId = i > 0 ? snapshotIds[i - 1] : null;
      Schema snapSchema = schemas.get(i % schemas.size());
      Snapshot snap = InlineDeltaSnapshots.create(
          i + 1L,
          snapshotIds[i],
          parentId,
          timestamps[i],
          generateSnapshotSummary(rand),
          snapSchema.schemaId(),
          generateManifestListPath(snapshotIds[i], rand),
          /* firstRowId */ null,
          /* addedRows */ null,
          /* keyId */ null);

      TableMetadata.Builder b = TableMetadata.buildFrom(metadata)
          .setLastUpdatedMillis(timestamps[i])
          .addSnapshot(snap);

      if (i == k.snapshots - 1) {
        b.setRef("main", SnapshotRef.branchBuilder(snapshotIds[i]).build());
      }
      if (k.statsPerSnapshot) {
        b.setStatistics(generateStatisticsFile(snapshotIds[i], i + 1L, k.blobsPerStats, rand));
      }
      metadata = b.build();
    }

    if (k.refs > 1 && k.snapshots > 0) {
      TableMetadata.Builder b = TableMetadata.buildFrom(metadata);
      for (int i = 1; i < k.refs; i++) {
        long target = snapshotIds[rand.nextInt(snapshotIds.length)];
        SnapshotRef ref;
        if (rand.nextBoolean()) {
          ref = SnapshotRef.branchBuilder(target)
              .minSnapshotsToKeep(2)
              .maxSnapshotAgeMs(7L * 86_400_000L)
              .maxRefAgeMs(30L * 86_400_000L)
              .build();
        } else {
          ref = SnapshotRef.tagBuilder(target).maxRefAgeMs(30L * 86_400_000L).build();
        }
        b.setRef(refName(i, rand), ref);
      }
      // setRef rewrites lastUpdatedMillis if its target was added in any builder pass carried over
      // via buildFrom (changes accumulate); set the post-refs timestamp after the loop so it sticks.
      b.setLastUpdatedMillis(now + 1);
      metadata = b.build();
    }

    if (k.metadataLogEntries > 0) {
      metadata = appendMetadataLog(metadata, k.metadataLogEntries, now, rand);
    }

    return metadata;
  }

  // ---------------------------------------------------------------------------
  // Schema / spec / sort order
  // ---------------------------------------------------------------------------

  private static List<Schema> generateSchemas(int versions, int fieldCount, Random rand) {
    int n = Math.max(1, versions);
    List<Schema> out = new ArrayList<>(n);
    int nextFieldId = 1;
    for (int v = 0; v < n; v++) {
      List<Types.NestedField> columns = new ArrayList<>(fieldCount);
      for (int f = 0; f < fieldCount; f++) {
        Type t = pickFieldType(f, rand);
        columns.add(Types.NestedField.optional(nextFieldId++, "col_" + f, t));
      }
      out.add(new Schema(v, columns));
    }
    return out;
  }

  private static Type pickFieldType(int fieldOrdinal, Random rand) {
    int kind = (fieldOrdinal * 7 + rand.nextInt(11)) % 13;
    switch (kind) {
      case 0: case 1: return Types.IntegerType.get();
      case 2: case 3: return Types.LongType.get();
      case 4: case 5: return Types.StringType.get();
      case 6: return Types.DoubleType.get();
      case 7: return Types.FloatType.get();
      case 8: return Types.DecimalType.of(12, 2);
      case 9: return Types.DateType.get();
      case 10: return Types.TimestampType.withZone();
      case 11: return Types.BooleanType.get();
      default: return Types.BinaryType.get();
    }
  }

  private static PartitionSpec generatePartitionSpec(Schema schema, int ordinal, Random rand) {
    PartitionSpec.Builder b = PartitionSpec.builderFor(schema);
    if (ordinal == 0 && schema.columns().size() > 0) {
      // unpartitioned baseline
      return b.build();
    }
    // Pick 1-2 partition columns.
    int picks = 1 + rand.nextInt(2);
    int chosen = 0;
    for (Types.NestedField field : schema.columns()) {
      if (chosen >= picks) break;
      Type type = field.type();
      try {
        if (type.equals(Types.IntegerType.get()) || type.equals(Types.LongType.get())) {
          b.bucket(field.name(), 16);
          chosen++;
        } else if (type.equals(Types.StringType.get())) {
          b.identity(field.name());
          chosen++;
        } else if (type.equals(Types.DateType.get())) {
          b.day(field.name());
          chosen++;
        } else if (type.equals(Types.TimestampType.withZone())) {
          b.hour(field.name());
          chosen++;
        }
      } catch (IllegalArgumentException ignored) {
        // skip incompatible
      }
    }
    if (chosen == 0) {
      return PartitionSpec.builderFor(schema).build();
    }
    return b.build();
  }

  private static SortOrder generateSortOrder(Schema schema, int ordinal, Random rand) {
    if (ordinal == 0) {
      return SortOrder.unsorted();
    }
    SortOrder.Builder b = SortOrder.builderFor(schema);
    int picks = 0;
    for (Types.NestedField field : schema.columns()) {
      if (picks >= 2) break;
      Type type = field.type();
      if (type.isPrimitiveType()) {
        if (rand.nextBoolean()) {
          b.asc(field.name());
        } else {
          b.desc(field.name());
        }
        picks++;
      }
    }
    if (picks == 0) {
      return SortOrder.unsorted();
    }
    return b.build();
  }

  // ---------------------------------------------------------------------------
  // Snapshots
  // ---------------------------------------------------------------------------

  private static long[] generateTimestamps(int count, long now, Distribution dist, Random rand) {
    long[] ts = new long[count];
    for (int i = 0; i < count; i++) {
      ts[i] = now - (long) (rand.nextDouble() * dist.spanMs);
    }
    Arrays.sort(ts);
    return ts;
  }

  private static Map<String, String> generateSnapshotSummary(Random rand) {
    Map<String, String> m = new LinkedHashMap<>();
    m.put("operation", "append");
    for (String key : SUMMARY_KEYS) {
      if (key.startsWith("spark.")) {
        m.put(key, "spark-application-" + (1_000_000 + rand.nextInt(9_000_000)));
      } else {
        m.put(key, Long.toString(rand.nextInt(1_000_000) + 1L));
      }
    }
    return m;
  }

  private static String generateManifestListPath(long snapshotId, Random rand) {
    return "s3://example-bucket/warehouse/db/metadata/snap-"
        + snapshotId
        + "-1-"
        + new UUID(rand.nextLong(), rand.nextLong())
        + ".avro";
  }

  // ---------------------------------------------------------------------------
  // Refs / properties / statistics / metadata-log
  // ---------------------------------------------------------------------------

  private static String refName(int i, Random rand) {
    String[] kinds = {"feature", "release", "hotfix", "snapshot", "tag"};
    return kinds[rand.nextInt(kinds.length)] + "/" + Integer.toHexString(i) + "_" + rand.nextInt(0x10000);
  }

  private static Map<String, String> generateProperties(int count, int meanValueSize, Random rand) {
    Map<String, String> props = new LinkedHashMap<>();
    for (int i = 0; i < count; i++) {
      String prefix = USER_PROPERTY_KEY_PREFIXES[i % USER_PROPERTY_KEY_PREFIXES.length];
      String key = i < USER_PROPERTY_KEY_PREFIXES.length
          ? prefix
          : prefix + "." + Integer.toHexString(rand.nextInt());
      props.put(key, randomValue(meanValueSize, rand));
    }
    return props;
  }

  private static String randomValue(int meanSize, Random rand) {
    if (meanSize <= 16) {
      return Integer.toString(rand.nextInt(1_000_000));
    }
    // Roughly normal around meanSize with ±25% jitter.
    int size = Math.max(1, meanSize + rand.nextInt(meanSize / 2 + 1) - meanSize / 4);
    StringBuilder sb = new StringBuilder(size);
    // Mostly alphanumeric so the bytes are ASCII and play well with JSON.
    for (int i = 0; i < size; i++) {
      int c = rand.nextInt(62);
      if (c < 10) sb.append((char) ('0' + c));
      else if (c < 36) sb.append((char) ('a' + c - 10));
      else sb.append((char) ('A' + c - 36));
    }
    return sb.toString();
  }

  private static StatisticsFile generateStatisticsFile(
      long snapshotId, long sequenceNumber, int blobs, Random rand) {
    List<BlobMetadata> blobMeta = new ArrayList<>(blobs);
    for (int i = 0; i < blobs; i++) {
      Map<String, String> blobProps = new LinkedHashMap<>();
      blobProps.put("ndv", Long.toString(rand.nextInt(1_000_000) + 1L));
      blobMeta.add(
          new GenericBlobMetadata(
              "apache-datasketches-theta-v1",
              snapshotId,
              sequenceNumber,
              List.of(i + 1),
              blobProps));
    }
    String path =
        "s3://example-bucket/warehouse/db/metadata/"
            + new UUID(rand.nextLong(), rand.nextLong())
            + "-stats.puffin";
    long fileSize = 4096L + rand.nextInt(64 * 1024);
    long footerSize = 256L + rand.nextInt(1024);
    return new GenericStatisticsFile(snapshotId, path, fileSize, footerSize, blobMeta);
  }

  /**
   * Append {@code count} entries to the table's previous-metadata-log. Each iteration
   * does a no-op-ish commit (synthetic property bump) so the builder's
   * {@code hasChanges()} fires and {@link TableMetadata.Builder#build()} appends the
   * new entry.
   */
  private static TableMetadata appendMetadataLog(
      TableMetadata base, int count, long now, Random rand) {
    TableMetadata m = base;
    for (int i = 0; i < count; i++) {
      String prevLoc =
          "s3://example-bucket/warehouse/db/metadata/v"
              + i
              + "-"
              + new UUID(rand.nextLong(), rand.nextLong())
              + ".metadata.json";
      m = TableMetadata.buildFrom(m)
          .setLastUpdatedMillis(now + (i + 1L) * 1000L)
          .setPreviousFileLocation(prevLoc)
          .setProperties(Map.of("synthetic.metadata-log.counter", Integer.toString(i)))
          .build();
    }
    return m;
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static int pick(int s, int small, int medium, int large) {
    switch (s) {
      case 0: return small;
      case 1: return medium;
      default: return large;
    }
  }

  private static <T> T pick(int s, T small, T medium, T large) {
    switch (s) {
      case 0: return small;
      case 1: return medium;
      default: return large;
    }
  }

  // ---------------------------------------------------------------------------
  // Sanity-check entry point: prints raw-JSON byte counts per scenario / intensity.
  // Run via:
  //   mvn test-compile exec:java \
  //     -Dexec.mainClass=org.apache.iceberg.io.bench.SyntheticTableMetadata \
  //     -Dexec.classpathScope=test
  // ---------------------------------------------------------------------------

  public static void main(String[] args) {
    System.out.printf("%-26s %-7s %12s %12s%n", "scenario", "size", "raw_bytes", "round_trip");
    System.out.println("-".repeat(62));
    for (Scenario sc : Scenario.values()) {
      for (Intensity in : Intensity.values()) {
        if (sc == Scenario.SNAPSHOT_ACCUMULATION && in == Intensity.LARGE) {
          // 50k snapshots × O(N²) builder pattern is too slow for a sanity check; skip.
          System.out.printf("%-26s %-7s %12s %12s%n", sc, in, "skipped", "—");
          continue;
        }
        long t0 = System.nanoTime();
        TableMetadata meta = generate(Knobs.forScenario(sc, in));
        byte[] json = TableMetadataParser.toJson(meta).getBytes(StandardCharsets.UTF_8);
        TableMetadata roundTripped = TableMetadataParser.fromJson(new String(json, StandardCharsets.UTF_8));
        byte[] rt = TableMetadataParser.toJson(roundTripped).getBytes(StandardCharsets.UTF_8);
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
        boolean stable = json.length == rt.length;
        System.out.printf(
            "%-26s %-7s %12d %12s  (%dms)%n",
            sc, in, json.length, stable ? "ok" : "DRIFT", elapsedMs);
      }
    }
  }
}
