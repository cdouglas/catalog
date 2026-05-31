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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Deterministic, seeded configuration for a catalog-footprint sample scenario.
 *
 * <p>A single {@code SampleConfig} drives one {@link WorkloadPlan}, which is then
 * replayed under each of the three {@link Mode}s ({@code orig}/{@code tm}/{@code
 * tmml}) so the catalog representation is the only variable. The seed fixes the
 * op sequence and data-file specs, hence all measured byte <em>sizes</em>
 * deterministically. (Iceberg injects random snapshot IDs and wall-clock
 * timestamps into the live commit path, so exact bytes are not reproducible
 * run-to-run; sizes are. See {@code docs/sample/README.md}.)
 */
public final class SampleConfig {

  /** The four catalog representations under comparison. */
  public enum Mode {
    /** Vanilla Iceberg: pointer mode, external {@code metadata.json} + {@code snap-*.avro}. */
    ORIG("orig", false, false, null),
    /**
     * Vanilla Iceberg with Iceberg's native gzip metadata compression
     * ({@code write.metadata.compression-codec=gzip}), so {@code metadata.json}
     * files are written gzip-compressed. This is the fair vanilla baseline — the
     * uncompressed {@code orig} mode is Iceberg's out-of-the-box default.
     */
    ORIG_GZ("orig-gz", false, false, "gzip"),
    /** Inline table metadata; manifest lists stay external ({@code snap-*.avro}). */
    TM("tm", true, false, null),
    /** Inline table metadata and manifest lists; no external catalog-layer files. */
    TMML("tmml", true, true, null);

    public final String dir;
    public final boolean inlineTm;
    public final boolean inlineMl;
    /** Iceberg {@code write.metadata.compression-codec} for written metadata files, or null. */
    public final String metadataCompression;

    Mode(String dir, boolean inlineTm, boolean inlineMl, String metadataCompression) {
      this.dir = dir;
      this.inlineTm = inlineTm;
      this.inlineMl = inlineMl;
      this.metadataCompression = metadataCompression;
    }

    /** Catalog properties for this mode, rooted at {@code warehouse}. */
    public Map<String, String> catalogProperties(String warehouse, String tmCodec) {
      Map<String, String> props = new LinkedHashMap<>();
      props.put("warehouse", warehouse);
      props.put("fileio.catalog.inline", String.valueOf(inlineTm));
      props.put("fileio.catalog.inline.manifests", String.valueOf(inlineMl));
      if (inlineTm && tmCodec != null) {
        props.put("fileio.catalog.inline.tm.codec", tmCodec);
      }
      return props;
    }
  }

  public final String name;
  public final String description;
  public final long seed;

  /** Number of tables created in the catalog. */
  public final int tables;

  /**
   * When true, each log transaction is one atomic {@code CatalogTransaction}
   * that updates <em>all</em> tables — the multi-table commit vanilla Iceberg
   * cannot express. When false, each transaction updates a single table.
   */
  public final boolean atomicGroups;

  /** Transactions applied in CAS mode (folded into the compacted checkpoint). */
  public final int checkpointTxns;

  /** Transactions applied in append mode (kept as records in the append log). */
  public final int logTxns;

  /** Relative op weights for a single-table commit. */
  public final int wFastAppend;
  public final int wOverwrite;
  public final int wDelete;

  /** Inline-TM codec ({@code "structural"} or {@code "gzip"}); ignored by ORIG. */
  public final String tmCodec;

  SampleConfig(Builder b) {
    this.name = b.name;
    this.description = b.description;
    this.seed = b.seed;
    this.tables = b.tables;
    this.atomicGroups = b.atomicGroups;
    this.checkpointTxns = b.checkpointTxns;
    this.logTxns = b.logTxns;
    this.wFastAppend = b.wFastAppend;
    this.wOverwrite = b.wOverwrite;
    this.wDelete = b.wDelete;
    this.tmCodec = b.tmCodec;
  }

  /** Serializes the config to a stable JSON object (for {@code config.json}). */
  public String toJson() {
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("  \"name\": \"").append(name).append("\",\n");
    sb.append("  \"description\": \"").append(description.replace("\"", "\\\"")).append("\",\n");
    sb.append("  \"seed\": ").append(seed).append(",\n");
    sb.append("  \"tables\": ").append(tables).append(",\n");
    sb.append("  \"atomicGroups\": ").append(atomicGroups).append(",\n");
    sb.append("  \"checkpointTxns\": ").append(checkpointTxns).append(",\n");
    sb.append("  \"logTxns\": ").append(logTxns).append(",\n");
    sb.append("  \"opWeights\": {\"fastAppend\": ").append(wFastAppend)
        .append(", \"overwrite\": ").append(wOverwrite)
        .append(", \"delete\": ").append(wDelete).append("},\n");
    sb.append("  \"tmCodec\": \"").append(tmCodec).append("\"\n");
    sb.append("}\n");
    return sb.toString();
  }

  public static final class Builder {
    private String name;
    private String description = "";
    private long seed = 0xC0FFEEL;
    private int tables = 1;
    private boolean atomicGroups = false;
    private int checkpointTxns = 0;
    private int logTxns = 0;
    private int wFastAppend = 80;
    private int wOverwrite = 15;
    private int wDelete = 5;
    private String tmCodec = "structural";

    public Builder(String name) {
      this.name = name;
    }

    public Builder description(String d) { this.description = d; return this; }
    public Builder seed(long s) { this.seed = s; return this; }
    public Builder tables(int t) { this.tables = t; return this; }
    public Builder atomicGroups(boolean a) { this.atomicGroups = a; return this; }
    public Builder checkpointTxns(int n) { this.checkpointTxns = n; return this; }
    public Builder logTxns(int n) { this.logTxns = n; return this; }
    public Builder opWeights(int fastAppend, int overwrite, int delete) {
      this.wFastAppend = fastAppend;
      this.wOverwrite = overwrite;
      this.wDelete = delete;
      return this;
    }
    public Builder tmCodec(String c) { this.tmCodec = c; return this; }

    public SampleConfig build() {
      return new SampleConfig(this);
    }
  }

  // -------------------------------------------------------------------------
  // Scenario presets. Each maps to docs/sample/<name>/.
  // -------------------------------------------------------------------------

  /** Default master seed when none is supplied on the command line. */
  public static final long DEFAULT_SEED = 0xC0FFEEL;

  /** Headline: one table, 50 snapshots in the checkpoint + 50 appended transactions. */
  public static SampleConfig oneTable50x50(long seed) {
    return new Builder("1tbl-50x50")
        .description(
            "One table. 50 snapshots compacted into the checkpoint, then 50 "
                + "transactions appended to the log. Append-biased mix that also "
                + "removes manifests (overwrite/delete) with net row growth.")
        .seed(seed)
        .tables(1)
        .atomicGroups(false)
        .checkpointTxns(50)
        .logTxns(50)
        .opWeights(80, 15, 5)
        .build();
  }

  /** Multi-table atomic transactions: each log txn updates all N tables in one commit. */
  public static SampleConfig multiTableAtomic(long seed) {
    return new Builder("multi-table-atomic")
        .description(
            "Four tables updated together. Each transaction is one atomic "
                + "CatalogTransaction touching all four tables — a single append "
                + "record where vanilla Iceberg needs four independent commits.")
        .seed(seed)
        .tables(4)
        .atomicGroups(true)
        .checkpointTxns(10)
        .logTxns(20)
        .opWeights(100, 0, 0)
        .build();
  }

  /** Catalog scale (static): many small tables compacted into the checkpoint. */
  public static SampleConfig manyTables(long seed) {
    return new Builder("many-tables")
        .description(
            "256 tables, one append each, all folded into the checkpoint. Shows "
                + "how per-table state (pointer vs inlined metadata) scales the "
                + "checkpoint, independent of per-commit deltas.")
        .seed(seed)
        .tables(256)
        .atomicGroups(false)
        .checkpointTxns(256) // one append per table
        .logTxns(0)
        .opWeights(100, 0, 0)
        .build();
  }

  /** Catalog scale (dynamic): many tables under a sustained append workload. */
  public static SampleConfig manyTablesAppend(long seed) {
    return new Builder("many-tables-append")
        .description(
            "256 tables created into the checkpoint, then 1024 append "
                + "transactions distributed uniformly at random across them "
                + "(~4 per table) in the append log. A dynamic catalog-scale "
                + "workload: the steady-state per-commit cost across a wide "
                + "catalog, not just the static per-table checkpoint state.")
        .seed(seed)
        .tables(256)
        .atomicGroups(false)
        .checkpointTxns(0) // tables are only created; all appends go to the log
        .logTxns(1024) // uniform random over the 256 tables (see WorkloadPlan)
        .opWeights(100, 0, 0)
        .build();
  }

  /** ML carry-forward: long FastAppend chain isolating manifest-pool dedup. */
  public static SampleConfig mlCarryForward(long seed) {
    return new Builder("ml-carry-forward")
        .description(
            "One table, 200 pure FastAppend commits (no removals). Isolates "
                + "manifest-pool dedup: vanilla rewrites the full manifest list "
                + "every commit (O(n^2) bytes); tmml stores each manifest once.")
        .seed(seed)
        .tables(1)
        .atomicGroups(false)
        .checkpointTxns(0)
        .logTxns(200)
        .opWeights(100, 0, 0)
        .build();
  }

  /** All scenarios, each given a distinct seed derived from one master seed. */
  public static List<SampleConfig> all(long masterSeed) {
    return List.of(
        oneTable50x50(derive(masterSeed, 0)),
        multiTableAtomic(derive(masterSeed, 1)),
        manyTables(derive(masterSeed, 2)),
        mlCarryForward(derive(masterSeed, 3)),
        manyTablesAppend(derive(masterSeed, 4)));
  }

  public static List<SampleConfig> all() {
    return all(DEFAULT_SEED);
  }

  /** SplitMix-style mix so adjacent ordinals yield well-separated per-scenario seeds. */
  private static long derive(long masterSeed, int ordinal) {
    return masterSeed + (ordinal + 1) * 0x9E3779B97F4A7C15L;
  }
}
