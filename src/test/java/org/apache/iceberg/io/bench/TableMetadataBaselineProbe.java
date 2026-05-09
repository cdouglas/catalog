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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airlift.compress.Compressor;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.snappy.SnappyCompressor;
import io.airlift.compress.zstd.ZstdCompressor;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;

/**
 * Phase A probe — measures the byte-share contribution of each top-level
 * TableMetadata field, raw and after each available compression codec.
 *
 * <p>The output answers "where do the bytes actually go?" The decision gates
 * baked into the bench plan trigger off these numbers:
 *
 * <ul>
 *   <li>If snapshots are &lt;30% of post-zstd total, the protobuf-for-snapshots
 *       candidate has nowhere to win and gets dropped.
 *   <li>If schemas are &lt;10% of post-zstd total, type-signature schema
 *       encoding doesn't move the needle.
 *   <li>If post-zstd is already &lt;25% of raw on the median cell, the bar for
 *       beating zstd-on-JSON has to rise.
 * </ul>
 *
 * <p>Two complementary measurements per component:
 *
 * <ul>
 *   <li><b>raw isolated</b> — bytes of just that field's JSON value.
 *   <li><b>zstd marginal</b> — {@code zstd(full) - zstd(full minus field)}.
 *       This is the bytes saved if the field were removed, which is what
 *       actually matters for the encoding-replacement question. Marginals can
 *       be slightly negative when removing a field exposes more repetition
 *       elsewhere; that's informative, not a bug.
 * </ul>
 *
 * <p>Run via:
 * <pre>{@code
 * mvn test-compile exec:java \
 *   -Dexec.mainClass=org.apache.iceberg.io.bench.TableMetadataBaselineProbe \
 *   -Dexec.classpathScope=test
 * }</pre>
 */
public final class TableMetadataBaselineProbe {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Top-level fields to isolate. Anything not listed lands in "(other)". */
  private static final String[] FIELDS_OF_INTEREST = {
      "snapshots",
      "schemas",
      "partition-specs",
      "sort-orders",
      "properties",
      "snapshot-log",
      "metadata-log",
      "refs",
      "statistics",
      "partition-statistics"
  };

  private enum Codec {
    NONE("raw"),
    GZIP("gzip"),
    SNAPPY("snappy"),
    LZ4("lz4"),
    ZSTD("zstd");

    final String label;

    Codec(String label) {
      this.label = label;
    }

    byte[] compress(byte[] in) {
      switch (this) {
        case NONE:
          return in;
        case GZIP:
          return gzipCompress(in);
        case SNAPPY:
          return airliftCompress(new SnappyCompressor(), in);
        case LZ4:
          return airliftCompress(new Lz4Compressor(), in);
        case ZSTD:
          return airliftCompress(new ZstdCompressor(), in);
        default:
          throw new IllegalStateException();
      }
    }
  }

  private TableMetadataBaselineProbe() {}

  public static void main(String[] args) throws IOException {
    System.out.println("# TableMetadata byte-share probe");
    System.out.println();

    // Aggregated medians per field across the corpus, for the closing decision-gate table.
    Map<String, List<Double>> rawShares = new LinkedHashMap<>();
    Map<String, List<Double>> zstdShares = new LinkedHashMap<>();
    for (String f : FIELDS_OF_INTEREST) {
      rawShares.put(f, new ArrayList<>());
      zstdShares.put(f, new ArrayList<>());
    }
    List<Double> zstdRatios = new ArrayList<>();

    for (Scenario sc : Scenario.values()) {
      for (SyntheticTableMetadata.Intensity in : SyntheticTableMetadata.Intensity.values()) {
        // Skip the SNAPSHOT_ACCUMULATION LARGE case: 50k snapshots × O(N²) builder, ~minutes.
        if (sc == Scenario.SNAPSHOT_ACCUMULATION
            && in == SyntheticTableMetadata.Intensity.LARGE) {
          continue;
        }

        SyntheticTableMetadata.Knobs knobs = SyntheticTableMetadata.Knobs.forScenario(sc, in);
        TableMetadata meta = SyntheticTableMetadata.generate(knobs);
        String json = TableMetadataParser.toJson(meta);
        byte[] raw = json.getBytes(StandardCharsets.UTF_8);

        // Whole-file compressed sizes per codec.
        Map<Codec, byte[]> compressed = new LinkedHashMap<>();
        for (Codec c : Codec.values()) {
          compressed.put(c, c.compress(raw));
        }

        zstdRatios.add((double) compressed.get(Codec.ZSTD).length / raw.length);

        printScenarioHeader(sc, in, raw, compressed);
        printPerComponentTable(json, raw, compressed.get(Codec.ZSTD).length,
            rawShares, zstdShares);
      }
    }

    printDecisionGateSummary(rawShares, zstdShares, zstdRatios);
  }

  // ---------------------------------------------------------------------------
  // Reporting
  // ---------------------------------------------------------------------------

  private static void printScenarioHeader(
      Scenario sc, SyntheticTableMetadata.Intensity in, byte[] raw,
      Map<Codec, byte[]> compressed) {
    System.out.printf(Locale.ROOT, "## %s / %s%n%n", sc, in);
    System.out.println("| codec | bytes | ratio vs raw |");
    System.out.println("|---|---:|---:|");
    for (Codec c : Codec.values()) {
      int len = compressed.get(c).length;
      System.out.printf(Locale.ROOT,
          "| %-6s | %,12d | %5.2f× |%n",
          c.label, len, (double) len / raw.length);
    }
    System.out.println();
  }

  private static void printPerComponentTable(
      String fullJson, byte[] rawFull, int zstdFullLen,
      Map<String, List<Double>> rawShares, Map<String, List<Double>> zstdShares)
      throws IOException {

    ObjectNode root = (ObjectNode) MAPPER.readTree(fullJson);

    System.out.println("| component | raw bytes | raw % | zstd marginal | zstd marginal % |");
    System.out.println("|---|---:|---:|---:|---:|");

    long isolatedRawSum = 0;
    long marginalZstdSum = 0;
    for (String field : FIELDS_OF_INTEREST) {
      if (!root.has(field) || root.get(field).isNull()) {
        System.out.printf(Locale.ROOT, "| %-22s | %10s | %6s | %14s | %7s |%n",
            field, "—", "—", "—", "—");
        continue;
      }

      byte[] isolatedRaw = MAPPER.writeValueAsBytes(root.get(field));

      ObjectNode minus = root.deepCopy();
      minus.remove(field);
      byte[] minusBytes = MAPPER.writeValueAsBytes(minus);
      byte[] minusZstd = Codec.ZSTD.compress(minusBytes);
      long marginalZstd = (long) zstdFullLen - minusZstd.length;

      double rawPct = 100.0 * isolatedRaw.length / rawFull.length;
      double zstdPct = 100.0 * marginalZstd / zstdFullLen;

      System.out.printf(Locale.ROOT,
          "| %-22s | %,10d | %5.1f%% | %,14d | %6.1f%% |%n",
          field, isolatedRaw.length, rawPct, marginalZstd, zstdPct);

      rawShares.get(field).add(rawPct);
      zstdShares.get(field).add(zstdPct);

      isolatedRawSum += isolatedRaw.length;
      marginalZstdSum += marginalZstd;
    }

    long otherRaw = (long) rawFull.length - isolatedRawSum;
    long otherZstd = (long) zstdFullLen - marginalZstdSum;
    System.out.printf(Locale.ROOT,
        "| %-22s | %,10d | %5.1f%% | %,14d | %6.1f%% |%n%n",
        "(other + JSON overhead)",
        otherRaw, 100.0 * otherRaw / rawFull.length,
        otherZstd, 100.0 * otherZstd / zstdFullLen);
  }

  private static void printDecisionGateSummary(
      Map<String, List<Double>> rawShares,
      Map<String, List<Double>> zstdShares,
      List<Double> zstdRatios) {

    System.out.println("## Median byte share across corpus");
    System.out.println();
    System.out.println("| component | median raw % | p95 raw % | median zstd marginal % | p95 zstd marginal % |");
    System.out.println("|---|---:|---:|---:|---:|");
    for (String f : FIELDS_OF_INTEREST) {
      System.out.printf(Locale.ROOT,
          "| %-22s | %5.1f%% | %5.1f%% | %6.1f%% | %6.1f%% |%n",
          f,
          percentile(rawShares.get(f), 50),
          percentile(rawShares.get(f), 95),
          percentile(zstdShares.get(f), 50),
          percentile(zstdShares.get(f), 95));
    }
    System.out.println();

    double medZstdRatio = percentile(zstdRatios, 50);
    System.out.printf(Locale.ROOT, "Median zstd ratio (full JSON): %.2f×%n", medZstdRatio);
    System.out.println();

    System.out.println("## Decision gates");
    System.out.println();
    double snapMed = percentile(zstdShares.get("snapshots"), 50);
    double schemaMed = percentile(zstdShares.get("schemas"), 50);
    System.out.printf(Locale.ROOT,
        "- snapshots median zstd marginal %% = %.1f%% — %s%n",
        snapMed,
        snapMed >= 30.0
            ? "**KEEP** protobuf-for-snapshots candidate"
            : "**DROP** protobuf-for-snapshots candidate (under 30% threshold)");
    System.out.printf(Locale.ROOT,
        "- schemas median zstd marginal %% = %.1f%% — %s%n",
        schemaMed,
        schemaMed >= 10.0
            ? "**KEEP** type-signature schema candidate"
            : "**DROP** type-signature schema candidate (under 10% threshold)");
    System.out.printf(Locale.ROOT,
        "- median zstd ratio = %.2f× — %s%n",
        medZstdRatio,
        medZstdRatio <= 0.25
            ? "**RAISE** Phase F bytes threshold (zstd already <25% of raw)"
            : "Phase F bytes threshold unchanged");
  }

  // ---------------------------------------------------------------------------
  // Codec helpers
  // ---------------------------------------------------------------------------

  private static byte[] airliftCompress(Compressor compressor, byte[] in) {
    int max = compressor.maxCompressedLength(in.length);
    byte[] out = new byte[max];
    int len = compressor.compress(in, 0, in.length, out, 0, out.length);
    return Arrays.copyOf(out, len);
  }

  private static byte[] gzipCompress(byte[] in) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(in.length / 4)) {
      try (GZIPOutputStream gz = new GZIPOutputStream(baos)) {
        gz.write(in);
      }
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("gzip compression failed", e);
    }
  }

  private static double percentile(List<Double> values, double p) {
    if (values.isEmpty()) {
      return 0.0;
    }
    List<Double> sorted = new ArrayList<>(values);
    Collections.sort(sorted);
    int idx = (int) Math.min(sorted.size() - 1L, Math.round(p / 100.0 * (sorted.size() - 1)));
    return sorted.get(idx);
  }
}
