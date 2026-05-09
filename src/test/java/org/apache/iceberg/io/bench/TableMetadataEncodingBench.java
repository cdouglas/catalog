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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.bench.encoders.JsonGzipEncoder;
import org.apache.iceberg.io.bench.encoders.JsonGzipProtoSnapshotsEncoder;
import org.apache.iceberg.io.bench.encoders.JsonZstdEncoder;
import org.apache.iceberg.io.bench.encoders.StructuralSnapshotsEncoder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * JMH bench for inline TableMetadata encodings.
 *
 * <p>Three candidates × four workloads (encode, decodeFull, decodeCurrentSnapshotOnly,
 * decodeRecentSnapshots(10)) across the eight pathology scenarios. The
 * {@link #main(String[])} runner aggregates results into four markdown tables
 * matching the Phase D report shape.
 *
 * <p>Run via:
 * <pre>{@code
 * mvn test-compile exec:java \
 *   -Dexec.mainClass=org.apache.iceberg.io.bench.TableMetadataEncodingBench \
 *   -Dexec.classpathScope=test
 * }</pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
public class TableMetadataEncodingBench {

  // Scenarios that fit comfortably in a 1s @Measurement at MEDIUM intensity. Excludes
  // SNAPSHOT_ACCUMULATION (10k snapshots: ~5.5s generation, slow @Setup) and
  // STATS_PUFFIN_REFS LARGE (5k stats files: similar). Adjust via -p scenario=...
  // when running specific cases.
  @Param({
      "NORMAL",
      "SCHEMA_SPEC_CHURN",
      "MANY_REFS",
      "VERBOSE_PROPERTIES",
      "STATS_PUFFIN_REFS",
      "COMMIT_FREQUENCY",
      "METADATA_LOG_RETENTION",
      "COMBINED_WORST_CASE"
  })
  public String scenario;

  @Param({"MEDIUM"})
  public String intensity;

  // The metadata and pre-encoded bytes — pinned for the trial.
  TableMetadata meta;
  byte[] encJsonGzip;
  byte[] encJsonZstd;
  byte[] encJsonGzipProto;
  byte[] encStructural;

  JsonGzipEncoder jsonGzip = new JsonGzipEncoder();
  JsonZstdEncoder jsonZstd = new JsonZstdEncoder();
  JsonGzipProtoSnapshotsEncoder jsonGzipProto = new JsonGzipProtoSnapshotsEncoder();
  StructuralSnapshotsEncoder structural = new StructuralSnapshotsEncoder();

  @Setup(Level.Trial)
  public void setup() {
    SyntheticTableMetadata.Knobs knobs = SyntheticTableMetadata.Knobs.forScenario(
        Scenario.valueOf(scenario), SyntheticTableMetadata.Intensity.valueOf(intensity));
    meta = SyntheticTableMetadata.generate(knobs);
    encJsonGzip = jsonGzip.encode(meta);
    encJsonZstd = jsonZstd.encode(meta);
    encJsonGzipProto = jsonGzipProto.encode(meta);
    encStructural = structural.encode(meta);

    // Round-trip sanity (decodeFull only — lazy paths are checked in the smoke runner).
    String origJson = TableMetadataParser.toJson(meta);
    assertJsonEquivalent(origJson, jsonGzip.decodeFull(encJsonGzip), "json-gzip");
    assertJsonEquivalent(origJson, jsonZstd.decodeFull(encJsonZstd), "json-zstd");
    assertJsonEquivalent(origJson, jsonGzipProto.decodeFull(encJsonGzipProto), "json-gzip+proto");
    assertJsonEquivalent(origJson, structural.decodeFull(encStructural), "structural");
  }

  private static void assertJsonEquivalent(String origJson, TableMetadata decoded, String label) {
    String roundTripped = TableMetadataParser.toJson(decoded);
    if (!origJson.equals(roundTripped)) {
      // Find first divergent character to make debugging tractable.
      int p = 0;
      int n = Math.min(origJson.length(), roundTripped.length());
      while (p < n && origJson.charAt(p) == roundTripped.charAt(p)) p++;
      int from = Math.max(0, p - 40);
      int to = Math.min(p + 80, Math.min(origJson.length(), roundTripped.length()));
      throw new AssertionError(label + ": round-trip JSON differs at char " + p
          + " (orig.len=" + origJson.length() + " vs decoded.len=" + roundTripped.length() + ")\n"
          + "  orig...:  " + origJson.substring(from, Math.min(to, origJson.length())) + "\n"
          + "  decoded:  " + roundTripped.substring(from, Math.min(to, roundTripped.length())));
    }
  }

  // ===========================================================================
  // Encode
  // ===========================================================================

  @Benchmark
  public byte[] encode_jsonGzip() {
    return jsonGzip.encode(meta);
  }

  @Benchmark
  public byte[] encode_jsonZstd() {
    return jsonZstd.encode(meta);
  }

  @Benchmark
  public byte[] encode_jsonGzipProto() {
    return jsonGzipProto.encode(meta);
  }

  @Benchmark
  public byte[] encode_structural() {
    return structural.encode(meta);
  }

  // ===========================================================================
  // decodeFull
  // ===========================================================================

  @Benchmark
  public TableMetadata decodeFull_jsonGzip() {
    return jsonGzip.decodeFull(encJsonGzip);
  }

  @Benchmark
  public TableMetadata decodeFull_jsonZstd() {
    return jsonZstd.decodeFull(encJsonZstd);
  }

  @Benchmark
  public TableMetadata decodeFull_jsonGzipProto() {
    return jsonGzipProto.decodeFull(encJsonGzipProto);
  }

  @Benchmark
  public TableMetadata decodeFull_structural() {
    return structural.decodeFull(encStructural);
  }

  // ===========================================================================
  // decodeCurrentSnapshotOnly
  // ===========================================================================

  @Benchmark
  public void decodeCurrentSnapshotOnly_jsonGzip(Blackhole bh) {
    bh.consume(jsonGzip.decodeCurrentSnapshotOnly(encJsonGzip));
  }

  @Benchmark
  public void decodeCurrentSnapshotOnly_jsonZstd(Blackhole bh) {
    bh.consume(jsonZstd.decodeCurrentSnapshotOnly(encJsonZstd));
  }

  @Benchmark
  public void decodeCurrentSnapshotOnly_jsonGzipProto(Blackhole bh) {
    bh.consume(jsonGzipProto.decodeCurrentSnapshotOnly(encJsonGzipProto));
  }

  @Benchmark
  public void decodeCurrentSnapshotOnly_structural(Blackhole bh) {
    bh.consume(structural.decodeCurrentSnapshotOnly(encStructural));
  }

  // ===========================================================================
  // decodeRecentSnapshots(10)
  // ===========================================================================

  @Benchmark
  public void decodeRecentSnapshots_jsonGzip(Blackhole bh) {
    bh.consume(jsonGzip.decodeRecentSnapshots(encJsonGzip, 10));
  }

  @Benchmark
  public void decodeRecentSnapshots_jsonZstd(Blackhole bh) {
    bh.consume(jsonZstd.decodeRecentSnapshots(encJsonZstd, 10));
  }

  @Benchmark
  public void decodeRecentSnapshots_jsonGzipProto(Blackhole bh) {
    bh.consume(jsonGzipProto.decodeRecentSnapshots(encJsonGzipProto, 10));
  }

  @Benchmark
  public void decodeRecentSnapshots_structural(Blackhole bh) {
    bh.consume(structural.decodeRecentSnapshots(encStructural, 10));
  }

  // ===========================================================================
  // main: run the bench, collect results, emit 4 markdown tables
  // ===========================================================================

  public static void main(String[] args) throws Exception {
    // .forks(0) runs the workloads in the same JVM as the JMH plumbing — the only mode that
    // works under `mvn exec:java`, which doesn't propagate the test classpath to forked JVMs.
    // Relative comparisons between candidates remain apples-to-apples (same JIT state, same GC).
    // For higher-rigor measurements, run via a fat-jar (`mvn package` + `java -jar`) and pass
    // `-f 1` to restore forking.
    Options opts = new OptionsBuilder()
        .include(TableMetadataEncodingBench.class.getSimpleName())
        .forks(0)
        .build();
    java.util.Collection<RunResult> results = new Runner(opts).run();
    Report report = collect(results);
    report.printSizesTable();
    report.printEncodeDecodeFullTable();
    report.printLazyAccessTable();
    report.printSnapshotsPerBudgetTable();
  }

  // ---------------------------------------------------------------------------
  // Reporting
  // ---------------------------------------------------------------------------

  private static final String[] CANDIDATES =
      {"jsonGzip", "jsonZstd", "jsonGzipProto", "structural"};
  private static final String[] CANDIDATE_LABELS =
      {"json-gzip", "json-zstd", "json-gzip+proto-snapshots", "structural"};

  private static Report collect(java.util.Collection<RunResult> results) {
    Report r = new Report();
    for (RunResult rr : results) {
      String full = rr.getParams().getBenchmark();
      String method = full.substring(full.lastIndexOf('.') + 1);
      String scenario = rr.getParams().getParam("scenario");
      Result<?> primary = rr.getPrimaryResult();
      double score = primary.getScore();
      String[] parts = method.split("_", 2);
      String workload = parts[0];
      String candidate = parts[1];
      r.put(scenario, workload, candidate, score);
    }
    // Compute encoded sizes once (no @Benchmark needed — sizes are deterministic).
    for (Scenario sc : Scenario.values()) {
      if (!r.scenarios.contains(sc.name())) continue;
      SyntheticTableMetadata.Knobs k = SyntheticTableMetadata.Knobs.forScenario(
          sc, SyntheticTableMetadata.Intensity.MEDIUM);
      TableMetadata meta = SyntheticTableMetadata.generate(k);
      r.snapshotCounts.put(sc.name(), meta.snapshots().size());
      Map<String, Integer> sizes = new LinkedHashMap<>();
      sizes.put("jsonGzip", new JsonGzipEncoder().encode(meta).length);
      sizes.put("jsonZstd", new JsonZstdEncoder().encode(meta).length);
      sizes.put("jsonGzipProto", new JsonGzipProtoSnapshotsEncoder().encode(meta).length);
      sizes.put("structural", new StructuralSnapshotsEncoder().encode(meta).length);
      r.encodedSizes.put(sc.name(), sizes);
    }
    return r;
  }

  private static final class Report {
    // workload -> candidate -> scenario -> score (microseconds, AverageTime)
    final Map<String, Map<String, Map<String, Double>>> scores = new LinkedHashMap<>();
    final Map<String, Map<String, Integer>> encodedSizes = new LinkedHashMap<>();
    final Map<String, Integer> snapshotCounts = new LinkedHashMap<>();
    final java.util.Set<String> scenarios = new java.util.LinkedHashSet<>();

    void put(String scenario, String workload, String candidate, double score) {
      scenarios.add(scenario);
      scores.computeIfAbsent(workload, k -> new LinkedHashMap<>())
          .computeIfAbsent(candidate, k -> new LinkedHashMap<>())
          .put(scenario, score);
    }

    void printSizesTable() {
      System.out.println();
      System.out.println("## Encoded sizes (bytes)");
      System.out.println();
      System.out.print("| scenario |");
      for (String label : CANDIDATE_LABELS) {
        System.out.printf(Locale.ROOT, " %s | %s vs gzip |", label, label);
      }
      System.out.println();
      System.out.print("|---|");
      for (int i = 0; i < CANDIDATE_LABELS.length; i++) System.out.print("---:|---:|");
      System.out.println();
      for (String sc : scenarios) {
        Map<String, Integer> sizes = encodedSizes.get(sc);
        if (sizes == null) continue;
        int gzipSize = sizes.get("jsonGzip");
        System.out.printf(Locale.ROOT, "| %s |", sc);
        for (String c : CANDIDATES) {
          int s = sizes.get(c);
          double ratio = (double) s / gzipSize;
          System.out.printf(Locale.ROOT, " %,d | %.2fx |", s, ratio);
        }
        System.out.println();
      }
      System.out.println();
    }

    void printEncodeDecodeFullTable() {
      System.out.println("## Encode + decodeFull latencies (µs/op)");
      System.out.println();
      System.out.print("| scenario |");
      for (String label : CANDIDATE_LABELS) {
        System.out.printf(Locale.ROOT, " encode %s | decodeFull %s |", label, label);
      }
      System.out.println();
      System.out.print("|---|");
      for (int i = 0; i < CANDIDATE_LABELS.length; i++) System.out.print("---:|---:|");
      System.out.println();
      for (String sc : scenarios) {
        System.out.printf(Locale.ROOT, "| %s |", sc);
        for (String c : CANDIDATES) {
          Double enc = score("encode", c, sc);
          Double dec = score("decodeFull", c, sc);
          System.out.printf(Locale.ROOT, " %s | %s |", fmt(enc), fmt(dec));
        }
        System.out.println();
      }
      System.out.println();
    }

    void printLazyAccessTable() {
      System.out.println("## Lazy-access latencies (µs/op)");
      System.out.println();
      System.out.print("| scenario |");
      for (String label : CANDIDATE_LABELS) {
        System.out.printf(Locale.ROOT, " current %s | recent10 %s |", label, label);
      }
      System.out.println();
      System.out.print("|---|");
      for (int i = 0; i < CANDIDATE_LABELS.length; i++) System.out.print("---:|---:|");
      System.out.println();
      for (String sc : scenarios) {
        System.out.printf(Locale.ROOT, "| %s |", sc);
        for (String c : CANDIDATES) {
          Double cur = score("decodeCurrentSnapshotOnly", c, sc);
          Double rec = score("decodeRecentSnapshots", c, sc);
          System.out.printf(Locale.ROOT, " %s | %s |", fmt(cur), fmt(rec));
        }
        System.out.println();
      }
      System.out.println();
    }

    void printSnapshotsPerBudgetTable() {
      System.out.println("## Snapshots-per-4-MiB headroom (median pin-point cell)");
      System.out.println();
      // Use NORMAL/MEDIUM as the pin-point cell.
      String pin = "NORMAL";
      if (!scenarios.contains(pin)) {
        pin = scenarios.iterator().next();
      }
      Map<String, Integer> sizes = encodedSizes.get(pin);
      Integer snapshotCount = snapshotCounts.get(pin);
      if (sizes == null || snapshotCount == null || snapshotCount == 0) {
        System.out.println("*(insufficient data)*");
        return;
      }
      System.out.println("| candidate | bytes/snapshot | snapshots-in-4-MiB | vs json-gzip |");
      System.out.println("|---|---:|---:|---:|");
      double bytesPerGzip = (double) sizes.get("jsonGzip") / snapshotCount;
      for (int i = 0; i < CANDIDATES.length; i++) {
        double bps = (double) sizes.get(CANDIDATES[i]) / snapshotCount;
        long fit = (long) Math.floor((4L * 1024 * 1024) / bps);
        double ratio = bps / bytesPerGzip;
        System.out.printf(Locale.ROOT,
            "| %s | %.1f | %,d | %.2f×%s |%n",
            CANDIDATE_LABELS[i], bps, fit, ratio,
            ratio < 1.0 ? " (smaller)" : "");
      }
      System.out.println();
      System.out.println("*Pin-point: " + pin + " / MEDIUM, " + snapshotCount + " snapshots*");
    }

    Double score(String workload, String candidate, String scenario) {
      Map<String, Map<String, Double>> w = scores.get(workload);
      if (w == null) return null;
      Map<String, Double> c = w.get(candidate);
      if (c == null) return null;
      return c.get(scenario);
    }

    String fmt(Double v) {
      if (v == null) return "—";
      if (v < 100) return String.format(Locale.ROOT, "%.2f", v);
      return String.format(Locale.ROOT, "%,.0f", v);
    }
  }
}
