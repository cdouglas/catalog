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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.bench.encoders.JsonGzipEncoder;
import org.apache.iceberg.io.bench.encoders.JsonGzipProtoSnapshotsEncoder;
import org.apache.iceberg.io.bench.encoders.JsonZstdEncoder;
import org.apache.iceberg.io.bench.encoders.StructuralSnapshotsEncoder;

/**
 * Strict round-trip equivalence check across all scenarios × intensities for every
 * encoder. Validates three things per (encoder, scenario, intensity) cell:
 *
 * <ol>
 *   <li><b>decodeFull</b>: serialize the decoded {@link TableMetadata} and compare byte-for-byte
 *       against the original JSON. This is the strongest possible check — any drift in any
 *       field, summary entry, snapshot, or metadata-log entry shows up as a diff.
 *   <li><b>decodeCurrentSnapshotOnly</b>: compare the lazy-decoded {@link Snapshot}'s
 *       {@code SnapshotParser.toJson} output against the original
 *       {@code meta.currentSnapshot()}.
 *   <li><b>decodeRecentSnapshots(10)</b>: walk the parent chain on the original metadata,
 *       compare per-element {@code SnapshotParser.toJson} against the encoder's chain.
 * </ol>
 *
 * <p>Run via:
 * <pre>{@code
 * mvn test-compile exec:java \
 *   -Dexec.mainClass=org.apache.iceberg.io.bench.EncoderValidator \
 *   -Dexec.classpathScope=test
 * }</pre>
 *
 * <p>Exit code 0 if all cells pass; non-zero with a printed report otherwise.
 */
public final class EncoderValidator {

  private EncoderValidator() {}

  public static void main(String[] args) {
    Map<String, MetadataDecoder> encoders = new LinkedHashMap<>();
    encoders.put("json-gzip", new JsonGzipEncoder());
    encoders.put("json-zstd", new JsonZstdEncoder());
    encoders.put("json-gzip+proto-snapshots", new JsonGzipProtoSnapshotsEncoder());
    encoders.put("structural", new StructuralSnapshotsEncoder());

    List<Failure> failures = new ArrayList<>();
    int totalChecks = 0;

    System.out.printf("%-26s %-7s %-30s %-12s%n", "scenario", "size", "encoder", "result");
    System.out.println("-".repeat(80));

    for (Scenario sc : Scenario.values()) {
      for (SyntheticTableMetadata.Intensity in : SyntheticTableMetadata.Intensity.values()) {
        // Skip the slowest cell (50k snapshots × O(N²) builder).
        if (sc == Scenario.SNAPSHOT_ACCUMULATION
            && in == SyntheticTableMetadata.Intensity.LARGE) {
          continue;
        }

        SyntheticTableMetadata.Knobs knobs = SyntheticTableMetadata.Knobs.forScenario(sc, in);
        TableMetadata meta;
        try {
          meta = SyntheticTableMetadata.generate(knobs);
        } catch (Throwable t) {
          System.out.printf("%-26s %-7s %-30s %-12s  (generation failed: %s)%n",
              sc, in, "—", "GEN_FAIL", t.getMessage());
          failures.add(new Failure(sc, in, "—", "generation failed: " + t.getMessage()));
          continue;
        }

        String origJson = TableMetadataParser.toJson(meta);
        Snapshot origCurrent = meta.currentSnapshot();
        String origCurrentJson = origCurrent == null ? null : SnapshotParser.toJson(origCurrent);
        List<Snapshot> origChain = walkChain(meta, 10);
        List<String> origChainJson = new ArrayList<>(origChain.size());
        for (Snapshot s : origChain) {
          origChainJson.add(SnapshotParser.toJson(s));
        }

        for (Map.Entry<String, MetadataDecoder> e : encoders.entrySet()) {
          totalChecks += 3;
          String name = e.getKey();
          MetadataDecoder enc = e.getValue();
          List<String> errs = new ArrayList<>();

          byte[] encoded;
          try {
            encoded = enc.encode(meta);
          } catch (Throwable t) {
            errs.add("encode threw: " + t);
            report(sc, in, name, errs, failures);
            continue;
          }

          // 1. decodeFull strict JSON equality.
          try {
            TableMetadata decoded = enc.decodeFull(encoded);
            String roundTripped = TableMetadataParser.toJson(decoded);
            if (!origJson.equals(roundTripped)) {
              int p = firstDiffIndex(origJson, roundTripped);
              errs.add(String.format(
                  "decodeFull JSON differs at char %d (orig.len=%d, decoded.len=%d): "
                      + "...%s... vs ...%s...",
                  p, origJson.length(), roundTripped.length(),
                  excerpt(origJson, p), excerpt(roundTripped, p)));
            }
          } catch (Throwable t) {
            errs.add("decodeFull threw: " + t);
          }

          // 2. decodeCurrentSnapshotOnly equivalence.
          try {
            Snapshot decoded = enc.decodeCurrentSnapshotOnly(encoded);
            if (origCurrent == null) {
              if (decoded != null) {
                errs.add("decodeCurrentSnapshotOnly: expected null, got id=" + decoded.snapshotId());
              }
            } else {
              if (decoded == null) {
                errs.add("decodeCurrentSnapshotOnly: expected snapshot id="
                    + origCurrent.snapshotId() + ", got null");
              } else {
                String dj = SnapshotParser.toJson(decoded);
                if (!origCurrentJson.equals(dj)) {
                  int p = firstDiffIndex(origCurrentJson, dj);
                  errs.add("decodeCurrentSnapshotOnly differs at char " + p
                      + ": ..." + excerpt(origCurrentJson, p) + "... vs ..." + excerpt(dj, p) + "...");
                }
              }
            }
          } catch (Throwable t) {
            errs.add("decodeCurrentSnapshotOnly threw: " + t);
          }

          // 3. decodeRecentSnapshots(10) equivalence.
          try {
            List<Snapshot> decoded = enc.decodeRecentSnapshots(encoded, 10);
            if (decoded.size() != origChain.size()) {
              errs.add("decodeRecentSnapshots: expected " + origChain.size()
                  + " snapshots, got " + decoded.size());
            } else {
              for (int i = 0; i < decoded.size(); i++) {
                String dj = SnapshotParser.toJson(decoded.get(i));
                if (!origChainJson.get(i).equals(dj)) {
                  errs.add("decodeRecentSnapshots[" + i + "] differs: "
                      + "expected id=" + origChain.get(i).snapshotId()
                      + ", got id=" + decoded.get(i).snapshotId());
                  break;
                }
              }
            }
          } catch (Throwable t) {
            errs.add("decodeRecentSnapshots threw: " + t);
          }

          report(sc, in, name, errs, failures);
        }
      }
    }

    System.out.println();
    System.out.println("---");
    System.out.printf("Total checks: %d, Failures: %d%n", totalChecks, failures.size());
    if (!failures.isEmpty()) {
      System.out.println();
      System.out.println("Failures:");
      for (Failure f : failures) {
        System.out.println("  " + f);
      }
      System.exit(1);
    }
    System.out.println("ALL PASS");
  }

  private static void report(
      Scenario sc, SyntheticTableMetadata.Intensity in,
      String name, List<String> errs, List<Failure> failures) {
    if (errs.isEmpty()) {
      System.out.printf("%-26s %-7s %-30s %-12s%n", sc, in, name, "ok");
    } else {
      System.out.printf("%-26s %-7s %-30s %-12s%n", sc, in, name, "FAIL");
      for (String err : errs) {
        System.out.println("    " + err);
        failures.add(new Failure(sc, in, name, err));
      }
    }
  }

  private static List<Snapshot> walkChain(TableMetadata meta, int k) {
    List<Snapshot> out = new ArrayList<>();
    Snapshot cur = meta.currentSnapshot();
    while (cur != null && out.size() < k) {
      out.add(cur);
      Long parent = cur.parentId();
      if (parent == null) break;
      cur = meta.snapshot(parent);
    }
    return out;
  }

  private static int firstDiffIndex(String a, String b) {
    int n = Math.min(a.length(), b.length());
    for (int i = 0; i < n; i++) {
      if (a.charAt(i) != b.charAt(i)) return i;
    }
    return n;
  }

  private static String excerpt(String s, int p) {
    int from = Math.max(0, p - 30);
    int to = Math.min(s.length(), p + 30);
    return s.substring(from, to).replace('\n', ' ');
  }

  private static final class Failure {
    final Scenario scenario;
    final SyntheticTableMetadata.Intensity intensity;
    final String encoder;
    final String message;

    Failure(Scenario sc, SyntheticTableMetadata.Intensity in, String enc, String msg) {
      this.scenario = sc;
      this.intensity = in;
      this.encoder = enc;
      this.message = msg;
    }

    @Override
    public String toString() {
      return scenario + "/" + intensity + " [" + encoder + "]: " + message;
    }
  }
}
