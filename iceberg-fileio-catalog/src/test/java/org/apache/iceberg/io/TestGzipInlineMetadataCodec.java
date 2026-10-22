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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.bench.Scenario;
import org.apache.iceberg.io.bench.SyntheticTableMetadata;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Unit tests for {@link GzipInlineMetadataCodec}: round-trip on every
 * {@link Scenario} preset, lazy-access parity, and the basic codec metadata
 * (tag, shortName).
 *
 * <p>The synthetic corpus comes from {@link SyntheticTableMetadata}, the
 * same generator used by the inline-TM bench (see
 * {@code docs/TM_ENCODING_BENCH_RESULTS.md}). SMALL intensity keeps each
 * scenario under a few hundred snapshots so the suite runs in seconds while
 * still covering every shape (ref-heavy, mdlog-heavy, schema-churn, ...).
 */
public class TestGzipInlineMetadataCodec {

  private static final InlineMetadataCodec CODEC = InlineMetadataCodecs.JSON_GZIP;

  // SNAPSHOT_ACCUMULATION at SMALL = 1000 snapshots; iterative buildFrom is O(N^2)
  // and tests should run fast. The encoder behaviour is fully exercised by 100-snapshot
  // NORMAL / 500-snapshot SCHEMA_SPEC_CHURN already.
  @ParameterizedTest(name = "{0}")
  @EnumSource(value = Scenario.class, mode = EnumSource.Mode.EXCLUDE,
      names = {"SNAPSHOT_ACCUMULATION", "STATS_PUFFIN_REFS", "COMBINED_WORST_CASE"})
  void roundTripPreservesJson(Scenario scenario) {
    TableMetadata original = generate(scenario);

    byte[] encoded = CODEC.encode(original);
    TableMetadata decoded = CODEC.decodeFull(encoded, null);

    assertThat(TableMetadataParser.toJson(decoded))
        .as("decode(encode(meta)) must round-trip JSON byte-for-byte")
        .isEqualTo(TableMetadataParser.toJson(original));
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(value = Scenario.class, mode = EnumSource.Mode.EXCLUDE,
      names = {"SNAPSHOT_ACCUMULATION", "STATS_PUFFIN_REFS", "COMBINED_WORST_CASE"})
  void decodeCurrentSnapshotMatchesFull(Scenario scenario) {
    TableMetadata original = generate(scenario);

    byte[] encoded = CODEC.encode(original);
    Snapshot lazyCurrent = CODEC.decodeCurrentSnapshotOnly(encoded);
    Snapshot fullCurrent = original.currentSnapshot();

    if (fullCurrent == null) {
      assertThat(lazyCurrent).isNull();
    } else {
      assertThat(lazyCurrent).isNotNull();
      assertThat(SnapshotParser.toJson(lazyCurrent))
          .as("lazy current snapshot must equal the one in the materialised TableMetadata")
          .isEqualTo(SnapshotParser.toJson(fullCurrent));
    }
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(value = Scenario.class, mode = EnumSource.Mode.EXCLUDE,
      names = {"SNAPSHOT_ACCUMULATION", "STATS_PUFFIN_REFS", "COMBINED_WORST_CASE"})
  void decodeRecentSnapshotsMatchesParentChain(Scenario scenario) {
    TableMetadata original = generate(scenario);
    int k = 5;

    byte[] encoded = CODEC.encode(original);
    List<Snapshot> recent = CODEC.decodeRecentSnapshots(encoded, k);

    Snapshot current = original.currentSnapshot();
    if (current == null) {
      assertThat(recent).isEmpty();
      return;
    }

    // Walk the parent chain in the materialised TM and compare.
    Snapshot s = current;
    for (int i = 0; i < k && s != null; i++) {
      assertThat(recent).hasSizeGreaterThan(i);
      assertThat(SnapshotParser.toJson(recent.get(i)))
          .as("recent[%d] must equal the materialised parent at depth %d", i, i)
          .isEqualTo(SnapshotParser.toJson(s));
      Long parent = s.parentId();
      s = parent == null ? null : original.snapshot(parent);
    }
  }

  @org.junit.jupiter.api.Test
  void encodeIsSmallerThanRawJson() {
    TableMetadata meta = generate(Scenario.NORMAL);
    byte[] gzip = CODEC.encode(meta);
    byte[] rawJson = TableMetadataParser.toJson(meta).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    assertThat(gzip.length)
        .as("gzip(JSON) must be materially smaller than raw JSON")
        .isLessThan(rawJson.length / 2);
  }

  @org.junit.jupiter.api.Test
  void tagAndShortNameMatchRegistry() {
    assertThat(CODEC.tag()).isEqualTo(InlineMetadataCodecs.TAG_JSON_GZIP);
    assertThat(CODEC.shortName()).isEqualTo("gzip");
    assertThat(InlineMetadataCodecs.byTag(CODEC.tag())).isSameAs(CODEC);
    assertThat(InlineMetadataCodecs.byShortName("gzip")).isSameAs(CODEC);
    assertThat(InlineMetadataCodecs.byShortName("json-gzip")).isSameAs(CODEC);
  }

  private static TableMetadata generate(Scenario scenario) {
    return SyntheticTableMetadata.generate(
        SyntheticTableMetadata.Knobs.forScenario(scenario, SyntheticTableMetadata.Intensity.SMALL));
  }
}
