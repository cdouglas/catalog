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
package org.apache.iceberg.io.sample;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.CatalogTransaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIOCatalog;
import org.apache.iceberg.io.ProtoCatalogFormat;
import org.apache.iceberg.types.Types;

/**
 * Generates the catalog-footprint sample artifacts under {@code docs/sample/}.
 *
 * <p>For each scenario in {@link SampleConfig} a single {@link WorkloadPlan} is
 * replayed under all three {@link SampleConfig.Mode}s over a fresh
 * {@link SampleFileIO}. Every file the catalog wrote is dumped byte-exact, and a
 * {@code manifest.json}/{@code sizes.csv} records per-file sizes by category plus
 * the per-commit append-log record sizes.
 *
 * <p>Run:
 *
 * <pre>{@code
 * mvn test-compile exec:java \
 *   -Dexec.mainClass=org.apache.iceberg.io.sample.SampleGenerator \
 *   -Dexec.classpathScope=test \
 *   -Dexec.args="--out docs/sample"          # optionally: --scenario 1tbl-50x50
 * }</pre>
 */
public final class SampleGenerator {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();

  private static final long PHASE_B_MAX_APPEND_COUNT = 1_000_000L;

  // Fixed across every mode (and every example) so the only difference between
  // runs is the encoding. Each run uses its own fresh SampleFileIO, so a shared
  // warehouse string never collides. Note: the gzip metadata-file extension
  // (".gz.metadata.json" under orig-gz) is part of the encoding and legitimately
  // makes those pointer strings 3 bytes longer than orig's.
  private static final String WAREHOUSE = "mem:///warehouse";

  private SampleGenerator() {}

  public static void main(String[] args) throws IOException {
    String out = "docs/sample";
    String only = null;
    Long seed = null;
    for (int i = 0; i < args.length; i++) {
      if ("--out".equals(args[i]) && i + 1 < args.length) {
        out = args[++i];
      } else if ("--scenario".equals(args[i]) && i + 1 < args.length) {
        only = args[++i];
      } else if ("--seed".equals(args[i]) && i + 1 < args.length) {
        seed = Long.decode(args[++i]); // accepts decimal or 0x-hex
      }
    }

    List<SampleConfig> configs = seed != null ? SampleConfig.all(seed) : SampleConfig.all();
    System.out.println("master seed: " + (seed != null ? seed : SampleConfig.DEFAULT_SEED));
    Path outRoot = Paths.get(out);
    for (SampleConfig config : configs) {
      if (only != null && !only.equals(config.name)) {
        continue;
      }
      generate(config, outRoot.resolve(config.name));
    }
  }

  static void generate(SampleConfig config, Path exampleDir) throws IOException {
    Files.createDirectories(exampleDir);
    Files.writeString(exampleDir.resolve("config.json"), config.toJson());

    WorkloadPlan plan = WorkloadPlan.generate(config);
    System.out.println("== " + config.name + " ==");
    System.out.println(config.description);

    Map<String, Map<String, Long>> totalsByMode = new LinkedHashMap<>();
    for (SampleConfig.Mode mode : SampleConfig.Mode.values()) {
      Map<String, Long> totals = runMode(config, mode, plan, exampleDir.resolve(mode.dir));
      totalsByMode.put(mode.dir, totals);
    }
    printSummary(config, totalsByMode);
    System.out.println();
  }

  // -------------------------------------------------------------------------
  // Replay
  // -------------------------------------------------------------------------

  private static Map<String, Long> runMode(
      SampleConfig config, SampleConfig.Mode mode, WorkloadPlan plan, Path modeDir)
      throws IOException {
    SampleFileIO io = new SampleFileIO();
    String warehouse = WAREHOUSE;
    String catalogLoc = warehouse + "/catalog";
    Map<String, String> props = mode.catalogProperties(warehouse, config.tmCodec);
    props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);

    // Per-table live data-file lists; persist across the two phases.
    List<List<DataFile>> live = new ArrayList<>();
    for (int t = 0; t < config.tables; t++) {
      live.add(new ArrayList<>());
    }
    int[] fileSeq = {0};

    // Phase A: CAS every commit (maxAppendCount=0) so state folds into the checkpoint.
    FileIOCatalog catalog = newCatalog(catalogLoc, props, io, 0);
    catalog.createNamespace(Namespace.of("db"));
    for (int t = 0; t < config.tables; t++) {
      Catalog.TableBuilder builder =
          catalog.buildTable(TableIdentifier.of("db", "t" + t), SCHEMA).withPartitionSpec(SPEC);
      if (mode.metadataCompression != null) {
        builder.withProperty(TableProperties.METADATA_COMPRESSION, mode.metadataCompression);
      }
      builder.create();
    }
    applyTxns(catalog, plan.checkpointTxns, config, live, fileSeq);

    // After CAS compaction the checkpoint holds the full setup state; exactly one
    // carry-over record remains (the compaction also records its own txn id for
    // dedup). The append-log records below are: 1 carry-over + plan.logTxns.size().

    // Phase B: append mode (high maxAppendCount) so commits become log records.
    catalog = newCatalog(catalogLoc, props, io, PHASE_B_MAX_APPEND_COUNT);
    applyTxns(catalog, plan.logTxns, config, live, fileSeq);

    return dump(io, catalogLoc, modeDir, mode);
  }

  private static FileIOCatalog newCatalog(
      String catalogLoc, Map<String, String> props, SampleFileIO io, long maxAppendCount) {
    ProtoCatalogFormat format =
        new ProtoCatalogFormat(
            (int) Math.min(maxAppendCount, Integer.MAX_VALUE),
            ProtoCatalogFormat.DEFAULT_MAX_APPEND_SIZE);
    FileIOCatalog catalog = new FileIOCatalog("sample", catalogLoc, format, io, props);
    catalog.initialize("sample", props);
    return catalog;
  }

  private static void applyTxns(
      Catalog catalog,
      List<WorkloadPlan.Txn> txns,
      SampleConfig config,
      List<List<DataFile>> live,
      int[] fileSeq) {
    for (WorkloadPlan.Txn txn : txns) {
      if (config.atomicGroups) {
        CatalogTransaction ct =
            ((org.apache.iceberg.catalog.SupportsCatalogTransactions) catalog)
                .createTransaction(CatalogTransaction.IsolationLevel.SNAPSHOT);
        Catalog tc = ct.asCatalog();
        for (WorkloadPlan.Op op : txn.ops) {
          applyOp(tc, op, live, fileSeq);
        }
        ct.commitTransaction();
      } else {
        applyOp(catalog, txn.ops.get(0), live, fileSeq);
      }
    }
  }

  private static void applyOp(
      Catalog catalog, WorkloadPlan.Op op, List<List<DataFile>> live, int[] fileSeq) {
    Table tbl = catalog.loadTable(TableIdentifier.of("db", "t" + op.table));
    List<DataFile> tableLive = live.get(op.table);
    DataFile df = dataFile(op, fileSeq[0]++);

    switch (op.kind) {
      case FAST_APPEND:
        tbl.newFastAppend().appendFile(df).commit();
        tableLive.add(df);
        break;
      case OVERWRITE:
        if (tableLive.isEmpty()) {
          tbl.newFastAppend().appendFile(df).commit();
          tableLive.add(df);
        } else {
          DataFile old = tableLive.remove(op.removeOrdinal % tableLive.size());
          tbl.newOverwrite().addFile(df).deleteFile(old).commit();
          tableLive.add(df);
        }
        break;
      case DELETE:
        if (tableLive.isEmpty()) {
          tbl.newFastAppend().appendFile(df).commit();
          tableLive.add(df);
        } else {
          DataFile old = tableLive.remove(op.removeOrdinal % tableLive.size());
          tbl.newDelete().deleteFile(old).commit();
        }
        break;
      default:
        throw new IllegalStateException("unknown op " + op.kind);
    }
  }

  private static DataFile dataFile(WorkloadPlan.Op op, int fileSeq) {
    return DataFiles.builder(SPEC)
        .withPath("mem:///data/t" + op.table + "/f" + fileSeq + ".parquet")
        .withFileSizeInBytes(Math.max(1L, op.records * 80L))
        .withPartitionPath("data_bucket=" + op.partition)
        .withRecordCount(op.records)
        .build();
  }

  // -------------------------------------------------------------------------
  // Dump + measurement
  // -------------------------------------------------------------------------

  private static Map<String, Long> dump(
      SampleFileIO io, String catalogLoc, Path modeDir, SampleConfig.Mode mode)
      throws IOException {
    Path filesDir = modeDir.resolve("files");
    Files.createDirectories(filesDir);

    Map<String, Long> totals = new LinkedHashMap<>();
    Map<String, Integer> counts = new LinkedHashMap<>();
    List<String[]> rows = new ArrayList<>(); // category, path, bytes

    List<String> sorted = new ArrayList<>(io.files.keySet());
    sorted.sort(String::compareTo);
    for (String path : sorted) {
      byte[] bytes = io.files.get(path);
      String category = categorize(path, catalogLoc);
      totals.merge(category, (long) bytes.length, Long::sum);
      counts.merge(category, 1, Integer::sum);
      rows.add(new String[] {category, path, String.valueOf(bytes.length)});

      Path dest = filesDir.resolve(sanitize(path));
      Files.createDirectories(dest.getParent());
      Files.write(dest, bytes);
    }

    // Sanity: inline modes must not write catalog-layer sidecars.
    if (mode.inlineTm && counts.getOrDefault("table-metadata-json", 0) != 0) {
      throw new IllegalStateException(
          mode.dir + " unexpectedly wrote metadata.json files");
    }
    if (mode.inlineMl && counts.getOrDefault("manifest-list-avro", 0) != 0) {
      throw new IllegalStateException(mode.dir + " unexpectedly wrote snap-*.avro files");
    }

    int[] logRecordSizes = parseLogRecordSizes(io.files.get(catalogLoc));
    writeManifestJson(modeDir.resolve("manifest.json"), mode, totals, counts, rows, logRecordSizes);
    writeSizesCsv(modeDir.resolve("sizes.csv"), rows);
    return totals;
  }

  private static String categorize(String path, String catalogLoc) {
    if (path.equals(catalogLoc)) {
      return "catalog";
    }
    if (path.endsWith(".metadata.json")) {
      return "table-metadata-json";
    }
    String name = path.substring(path.lastIndexOf('/') + 1);
    if (name.startsWith("snap-") && path.endsWith(".avro")) {
      return "manifest-list-avro";
    }
    if (path.endsWith(".avro")) {
      return "manifest-avro";
    }
    return "other";
  }

  private static String sanitize(String path) {
    String p = path;
    int scheme = p.indexOf(":///");
    if (scheme >= 0) {
      p = p.substring(scheme + 4);
    }
    p = p.replace(':', '_');
    while (p.startsWith("/")) {
      p = p.substring(1);
    }
    return p;
  }

  /** Reads the framed catalog file and returns each appended transaction record's byte length. */
  private static int[] parseLogRecordSizes(byte[] catalog) {
    if (catalog == null) {
      return new int[0];
    }
    int[] pos = {ProtoCatalogFormat.HEADER_SIZE};
    int checkpointLen = readVarint(catalog, pos);
    pos[0] += checkpointLen; // skip checkpoint payload
    List<Integer> sizes = new ArrayList<>();
    while (pos[0] < catalog.length) {
      int txnLen = readVarint(catalog, pos);
      sizes.add(txnLen);
      pos[0] += txnLen;
    }
    int[] out = new int[sizes.size()];
    for (int i = 0; i < out.length; i++) {
      out[i] = sizes.get(i);
    }
    return out;
  }

  private static int readVarint(byte[] b, int[] pos) {
    int result = 0;
    int shift = 0;
    while (true) {
      int x = b[pos[0]++] & 0xff;
      result |= (x & 0x7f) << shift;
      if ((x & 0x80) == 0) {
        break;
      }
      shift += 7;
    }
    return result;
  }

  // -------------------------------------------------------------------------
  // Output files
  // -------------------------------------------------------------------------

  private static void writeManifestJson(
      Path file,
      SampleConfig.Mode mode,
      Map<String, Long> totals,
      Map<String, Integer> counts,
      List<String[]> rows,
      int[] logRecordSizes)
      throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("  \"mode\": \"").append(mode.dir).append("\",\n");
    sb.append("  \"inlineTm\": ").append(mode.inlineTm).append(",\n");
    sb.append("  \"inlineMl\": ").append(mode.inlineMl).append(",\n");

    long grand = totals.values().stream().mapToLong(Long::longValue).sum();
    sb.append("  \"totalBytes\": ").append(grand).append(",\n");

    sb.append("  \"byCategory\": {\n");
    int i = 0;
    for (Map.Entry<String, Long> e : totals.entrySet()) {
      sb.append("    \"").append(e.getKey()).append("\": {\"bytes\": ").append(e.getValue())
          .append(", \"count\": ").append(counts.getOrDefault(e.getKey(), 0)).append("}");
      sb.append(++i < totals.size() ? ",\n" : "\n");
    }
    sb.append("  },\n");

    sb.append("  \"logRecordSizes\": [");
    for (int k = 0; k < logRecordSizes.length; k++) {
      sb.append(logRecordSizes[k]).append(k + 1 < logRecordSizes.length ? ", " : "");
    }
    sb.append("],\n");

    sb.append("  \"files\": [\n");
    for (int r = 0; r < rows.size(); r++) {
      String[] row = rows.get(r);
      sb.append("    {\"category\": \"").append(row[0]).append("\", \"bytes\": ").append(row[2])
          .append(", \"path\": \"").append(jsonEscape(row[1])).append("\"}");
      sb.append(r + 1 < rows.size() ? ",\n" : "\n");
    }
    sb.append("  ]\n");
    sb.append("}\n");
    Files.writeString(file, sb.toString());
  }

  private static void writeSizesCsv(Path file, List<String[]> rows) throws IOException {
    StringBuilder sb = new StringBuilder("category,bytes,path\n");
    for (String[] row : rows) {
      sb.append(row[0]).append(',').append(row[2]).append(',').append(row[1]).append('\n');
    }
    Files.writeString(file, sb.toString());
  }

  private static String jsonEscape(String s) {
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private static void printSummary(
      SampleConfig config, Map<String, Map<String, Long>> totalsByMode) {
    List<SampleConfig.Mode> modes = List.of(SampleConfig.Mode.values());
    StringBuilder hdr = new StringBuilder(String.format("  %-22s", "category"));
    for (SampleConfig.Mode m : modes) {
      hdr.append(String.format(" %12s", m.dir));
    }
    System.out.println(hdr);

    List<String> cats =
        List.of("catalog", "table-metadata-json", "manifest-list-avro", "manifest-avro", "other");
    for (String cat : cats) {
      long sum = 0;
      for (SampleConfig.Mode m : modes) {
        sum += totalsByMode.get(m.dir).getOrDefault(cat, 0L);
      }
      if (sum == 0) {
        continue;
      }
      StringBuilder row = new StringBuilder(String.format("  %-22s", cat));
      for (SampleConfig.Mode m : modes) {
        row.append(String.format(" %12d", totalsByMode.get(m.dir).getOrDefault(cat, 0L)));
      }
      System.out.println(row);
    }

    StringBuilder total = new StringBuilder(String.format("  %-22s", "TOTAL"));
    StringBuilder layer = new StringBuilder(String.format("  %-22s", "catalog-layer"));
    for (SampleConfig.Mode m : modes) {
      Map<String, Long> t = totalsByMode.get(m.dir);
      long all = t.values().stream().mapToLong(Long::longValue).sum();
      total.append(String.format(" %12d", all));
      layer.append(String.format(" %12d", all - t.getOrDefault("manifest-avro", 0L)));
    }
    System.out.println(total);
    System.out.println(layer + "   (excludes manifest-avro data layer)");
  }
}
