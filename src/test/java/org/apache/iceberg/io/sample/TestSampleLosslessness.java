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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Verifies the footprint samples are <em>lossless</em>: replaying a workload under
 * the inline modes ({@code tm}, {@code tmml}) must reconstruct exactly the same
 * catalog state that vanilla Iceberg ({@code orig}) writes to external
 * {@code metadata.json} + {@code snap-*.avro} files — so the footprint comparison
 * in {@code docs/sample/} is fair (the single inline file really does contain
 * everything the external files do, not a lossy subset).
 *
 * <p>"Lossless" is checked per table at two granularities, against {@code orig} as
 * ground truth, for several seeds:
 *
 * <ol>
 *   <li><b>All snapshots present.</b> The reloaded table has the same ordered
 *       snapshot history (same count, same per-snapshot operation).
 *   <li><b>The manifest list in every snapshot is the same.</b> For each snapshot,
 *       the set of manifests it points to — and the live data files within each
 *       manifest — is identical to {@code orig}.
 * </ol>
 *
 * <p>Comparison is by logical content, not bytes: the live Iceberg commit path
 * stamps random snapshot IDs/timestamps and random manifest-file UUIDs per run, so
 * those differ across modes. The data-file paths ({@code f<seq>.parquet}) are
 * deterministic from the seed (see {@link WorkloadPlan}/{@link SampleGenerator}),
 * so the manifest-list contents are directly comparable across modes.
 *
 * <p>Replaying the full scenario matrix takes minutes (the 256-table scenarios
 * replay a wide catalog under three modes), so this suite is gated off the default
 * {@code mvn test} run. Enable it explicitly:
 *
 * <pre>{@code
 * mvn test -Dtest=TestSampleLosslessness \
 *   -Dsample.lossless=true \
 *   -DargLine="-Dorg.slf4j.simpleLogger.defaultLogLevel=warn"
 * }</pre>
 */
@EnabledIfSystemProperty(named = "sample.lossless", matches = "true")
class TestSampleLosslessness {

  /**
   * Seeds to exercise. The default sample seed plus a spread of others so a
   * losslessness bug that only trips on a particular op interleaving is caught.
   */
  private static final long[] SEEDS = {
    0xC0FFEEL, 1L, 42L, 0xDEADBEEFL, 0x123456789ABCL
  };

  /**
   * The 256-table scenarios are an order of magnitude heavier to replay; one
   * representative seed exercises the same reconstruction code paths the light
   * scenarios sweep across all {@link #SEEDS}.
   */
  private static final long[] HEAVY_SEEDS = {0xC0FFEEL};

  /** Scenarios at/above this table count run under {@link #HEAVY_SEEDS} only. */
  private static final int HEAVY_TABLE_THRESHOLD = 64;

  /** The inline modes under test; each is compared against {@link SampleConfig.Mode#ORIG}. */
  private static final SampleConfig.Mode[] INLINE_MODES = {
    SampleConfig.Mode.TM, SampleConfig.Mode.TMML
  };

  /** Light scenarios (few tables): full seed sweep. */
  @Test
  void lightScenariosAreLosslessAcrossSeeds() {
    for (long seed : SEEDS) {
      for (SampleConfig config : SampleConfig.all(seed)) {
        if (config.tables < HEAVY_TABLE_THRESHOLD) {
          verifyScenario(seed, config);
        }
      }
    }
  }

  /** Heavy scenarios (many tables): one representative seed. */
  @Test
  void heavyScenariosAreLossless() {
    for (long seed : HEAVY_SEEDS) {
      for (SampleConfig config : SampleConfig.all(seed)) {
        if (config.tables >= HEAVY_TABLE_THRESHOLD) {
          verifyScenario(seed, config);
        }
      }
    }
  }

  private void verifyScenario(long seed, SampleConfig config) {
    WorkloadPlan plan = WorkloadPlan.generate(config);

    SampleGenerator.Replay orig = SampleGenerator.replay(config, SampleConfig.Mode.ORIG, plan);
    List<List<String>> origTables = signatures(orig, config.tables);

    for (SampleConfig.Mode mode : INLINE_MODES) {
      SampleGenerator.Replay other = SampleGenerator.replay(config, mode, plan);
      List<List<String>> otherTables = signatures(other, config.tables);

      for (int t = 0; t < config.tables; t++) {
        List<String> expected = origTables.get(t);
        List<String> actual = otherTables.get(t);
        String where =
            String.format(
                "seed=0x%X scenario=%s mode=%s table=db.t%d", seed, config.name, mode.dir, t);

        // (1) All snapshots present, in the same order.
        assertThat(actual)
            .as("%s: snapshot count must match vanilla (orig)", where)
            .hasSameSizeAs(expected);

        // (2) The manifest list in every snapshot is the same as vanilla's.
        for (int s = 0; s < expected.size(); s++) {
          assertThat(actual.get(s))
              .as("%s: manifest list of snapshot #%d must match vanilla (orig)", where, s)
              .isEqualTo(expected.get(s));
        }
      }
    }
  }

  /** Per-table list of per-snapshot manifest-list signatures, in snapshot order. */
  private static List<List<String>> signatures(SampleGenerator.Replay replay, int tables) {
    List<List<String>> perTable = new ArrayList<>(tables);
    for (int t = 0; t < tables; t++) {
      Table table = replay.catalog.loadTable(TableIdentifier.of("db", "t" + t));
      List<String> snapSigs = new ArrayList<>();
      for (Snapshot snapshot : orderedSnapshots(table)) {
        snapSigs.add(snapshotSignature(snapshot, replay.io));
      }
      perTable.add(snapSigs);
    }
    return perTable;
  }

  /** Snapshots in ancestry order (oldest first), independent of random snapshot IDs. */
  private static List<Snapshot> orderedSnapshots(Table table) {
    List<Snapshot> ordered = new ArrayList<>();
    if (table.currentSnapshot() == null) {
      return ordered;
    }
    // SnapshotUtil-style walk up the parent chain, then reverse to oldest-first.
    Snapshot s = table.currentSnapshot();
    while (s != null) {
      ordered.add(s);
      Long parent = s.parentId();
      s = parent == null ? null : table.snapshot(parent);
    }
    java.util.Collections.reverse(ordered);
    return ordered;
  }

  /**
   * Content signature of a snapshot's manifest list: the manifest set, each
   * manifest represented by its content type and the sorted live data files it
   * carries. Order-independent across manifests (Iceberg does not promise a stable
   * manifest order), so the outer list is sorted.
   */
  private static String snapshotSignature(Snapshot snapshot, FileIO io) {
    List<String> manifestSigs = new ArrayList<>();
    for (ManifestFile manifest : snapshot.allManifests(io)) {
      // The sample workload only adds/removes data files (FastAppend/Overwrite/
      // Delete), so every manifest is a DATA manifest; a delete manifest here would
      // mean the workload changed and this signature no longer captures it.
      if (manifest.content() != ManifestContent.DATA) {
        throw new IllegalStateException(
            "unexpected " + manifest.content() + " manifest in sample workload: " + manifest.path());
      }
      List<String> files = new ArrayList<>();
      try (CloseableIterable<DataFile> reader = ManifestFiles.read(manifest, io)) {
        for (DataFile df : reader) {
          files.add(df.path() + "|" + df.recordCount());
        }
      } catch (Exception e) {
        throw new RuntimeException("failed reading manifest " + manifest.path(), e);
      }
      java.util.Collections.sort(files);
      manifestSigs.add(files.toString());
    }
    java.util.Collections.sort(manifestSigs);
    return "op=" + snapshot.operation() + ";manifests=" + manifestSigs;
  }
}
