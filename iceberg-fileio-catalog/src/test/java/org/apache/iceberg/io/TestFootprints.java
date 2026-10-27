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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CommitFootprint;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.GenericPartitionFieldSummary;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.Test;

/**
 * Producers' footprints as records carry them: their wire form, and how the encodings that judge
 * validated commits read a check's scope (partition-vector's buckets, manifest-window's manifests).
 */
class TestFootprints {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          required(2, "cat", Types.IntegerType.get()),
          optional(3, "name", Types.StringType.get()));

  private static final PartitionSpec BY_CAT =
      PartitionSpec.builderFor(SCHEMA).identity("cat").withSpecId(0).build();

  private static final int BUCKETS = 64;

  private static ByteBuffer cat(int value) {
    return Conversions.toByteBuffer(Types.IntegerType.get(), value);
  }

  private static Footprint.Scope filter(Expression filter) {
    return Footprint.Scope.filter(ExpressionParser.toJson(filter));
  }

  private static Footprint.Check check(
      CommitFootprint.Check.Kind kind, Footprint.Scope scope) {
    return new Footprint.Check(kind, scope);
  }

  /** Each footprint reads back as it was written, through a delta's snapshot. */
  @Test
  void footprintsRoundTrip() {
    List<Footprint> footprints =
        List.of(
            Footprint.BLIND,
            Footprint.OPAQUE,
            Footprint.validated(List.of()),
            Footprint.validated(
                List.of(
                    check(CommitFootprint.Check.Kind.NO_DATA_ADDED, Footprint.Scope.EVERYTHING),
                    check(CommitFootprint.Check.Kind.NO_DELETES_ADDED,
                        filter(Expressions.equal("cat", 3))),
                    check(CommitFootprint.Check.Kind.NO_DATA_REMOVED,
                        Footprint.Scope.partitions(
                            List.of(
                                new Footprint.Partition(0, List.of(cat(1))),
                                new Footprint.Partition(1, Arrays.asList(cat(2), null)),
                                new Footprint.Partition(2, List.of())))),
                    check(CommitFootprint.Check.Kind.NO_DATA_ADDED,
                        Footprint.Scope.partitions(List.of())))));
    for (Footprint footprint : footprints) {
      InlineDeltaCodec.AddSnapshotUpdate add =
          new InlineDeltaCodec.AddSnapshotUpdate(
              7L, "", Map.of("operation", "overwrite"), 0L, 0, 1L, 6L, null, null, footprint);
      byte[] bytes = InlineDeltaCodec.encodeDelta(List.of(add), 0L);
      InlineDeltaCodec.AddSnapshotUpdate decoded =
          (InlineDeltaCodec.AddSnapshotUpdate) InlineDeltaCodec.decodeDelta(bytes).updates.get(0);
      assertThat(decoded.footprint).as("%s", footprint).isEqualTo(footprint);
    }
  }

  /** A producer's partitions are serialized with the table's specs; an unknown spec is opaque. */
  @Test
  void producerFootprintsTakeTheTablesSpecs() {
    DataFile file =
        DataFiles.builder(BY_CAT)
            .withPath("/f.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withPartitionPath("cat=5")
            .build();
    CommitFootprint producer =
        CommitFootprint.validated(
            List.of(
                CommitFootprint.Check.of(
                    CommitFootprint.Check.Kind.NO_DELETES_ADDED,
                    CommitFootprint.Scope.partitions(List.of(Pair.of(0, file.partition())))),
                CommitFootprint.Check.of(
                    CommitFootprint.Check.Kind.NO_DATA_ADDED,
                    CommitFootprint.Scope.filter(Expressions.lessThan("id", 4L)))));
    assertThat(Footprint.of(producer, Map.of(0, BY_CAT)))
        .isEqualTo(
            Footprint.validated(
                List.of(
                    check(CommitFootprint.Check.Kind.NO_DELETES_ADDED,
                        Footprint.Scope.partitions(
                            List.of(new Footprint.Partition(0, List.of(cat(5)))))),
                    check(CommitFootprint.Check.Kind.NO_DATA_ADDED,
                        filter(Expressions.lessThan("id", 4L))))));
    assertThat(Footprint.of(producer, Map.of())).isEqualTo(Footprint.OPAQUE);
    assertThat(Footprint.of(CommitFootprint.BLIND, Map.of())).isEqualTo(Footprint.BLIND);
    assertThat(Footprint.of(null, Map.of())).isNull();
  }

  /** A filter names partitions by equality on every field, else none (every bucket). */
  @Test
  void filtersNamePartitionsByEquality() {
    assertThat(PartitionKey.partitions(Expressions.equal("cat", 1), BY_CAT))
        .containsExactly(new Footprint.Partition(0, List.of(cat(1))));
    assertThat(PartitionKey.partitions(Expressions.in("cat", 1, 2), BY_CAT)).hasSize(2);
    assertThat(PartitionKey.partitions(
            Expressions.and(Expressions.equal("cat", 1), Expressions.greaterThan("id", 5L)),
            BY_CAT))
        .containsExactly(new Footprint.Partition(0, List.of(cat(1))));
    assertThat(PartitionKey.partitions(
            Expressions.or(Expressions.equal("cat", 1), Expressions.equal("cat", 2)), BY_CAT))
        .hasSize(2);
    assertThat(PartitionKey.partitions(
            Expressions.and(Expressions.equal("cat", 1), Expressions.equal("cat", 2)), BY_CAT))
        .isEmpty();
    assertThat(PartitionKey.partitions(Expressions.alwaysFalse(), BY_CAT)).isEmpty();
    assertThat(PartitionKey.partitions(Expressions.greaterThan("cat", 1), BY_CAT)).isNull();
    assertThat(PartitionKey.partitions(Expressions.alwaysTrue(), BY_CAT)).isNull();
    assertThat(PartitionKey.partitions(Expressions.equal("id", 3L), BY_CAT)).isNull();
    assertThat(PartitionKey.partitions(
            Expressions.or(Expressions.equal("cat", 1), Expressions.equal("id", 3L)), BY_CAT))
        .isNull();

    PartitionSpec two =
        PartitionSpec.builderFor(SCHEMA).identity("cat").bucket("id", 4).withSpecId(1).build();
    assertThat(PartitionKey.partitions(Expressions.equal("cat", 1), two)).isNull();
    List<Footprint.Partition> both =
        PartitionKey.partitions(
            Expressions.and(Expressions.equal("cat", 1), Expressions.equal("id", 9L)), two);
    assertThat(both).hasSize(1);
    assertThat(both.get(0).values).hasSize(2);

    PartitionSpec unpartitioned = PartitionSpec.unpartitioned();
    assertThat(PartitionKey.partitions(Expressions.equal("cat", 1), unpartitioned))
        .containsExactly(new Footprint.Partition(0, List.of()));
  }

  /**
   * A file's partition, a filter naming it, and a manifest of files in it alone share a bucket; a
   * range filter or a manifest that spans partitions reads or stamps every bucket.
   */
  @Test
  void aPartitionFilterAndManifestShareABucket() {
    MetadataView table = table(BY_CAT);
    DataFile file =
        DataFiles.builder(BY_CAT)
            .withPath("/f.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withPartitionPath("cat=5")
            .build();
    int bucket = PartitionKey.bucket(Footprint.Partition.of(BY_CAT, file.partition()), BUCKETS);
    assertThat(bucket).isNotEqualTo(PartitionKey.ALL);
    assertThat(PartitionKey.bucket(data(0, 5, 5, false), BUCKETS)).isEqualTo(bucket);
    assertThat(PartitionKey.buckets(filter(Expressions.equal("cat", 5)), table, BUCKETS))
        .containsExactly(bucket);
    assertThat(PartitionKey.bucket(data(0, 5, 6, false), BUCKETS)).isEqualTo(PartitionKey.ALL);
    assertThat(PartitionKey.buckets(filter(Expressions.greaterThan("cat", 5)), table, BUCKETS))
        .containsExactly(PartitionKey.ALL);
    assertThat(PartitionKey.buckets(Footprint.Scope.EVERYTHING, table, BUCKETS))
        .containsExactly(PartitionKey.ALL);
    assertThat(PartitionKey.buckets(Footprint.Scope.partitions(List.of()), table, BUCKETS))
        .isEmpty();
    assertThat(PartitionKey.buckets(partitions(0, (ByteBuffer) null), table, BUCKETS))
        .containsExactly(PartitionKey.ALL);
  }

  /** Manifest-window reads a scope against a manifest's partition summaries. */
  @Test
  void aScopeReadsAManifestsSummaries() {
    MetadataView table = table(BY_CAT);
    ManifestFile fiveToSeven = data(0, 5, 7, false);
    assertThat(ManifestWindowEncoding.inScope(fiveToSeven, Footprint.Scope.EVERYTHING, table))
        .isTrue();
    assertThat(ManifestWindowEncoding.inScope(
            fiveToSeven, filter(Expressions.equal("cat", 6)), table)).isTrue();
    assertThat(ManifestWindowEncoding.inScope(
            fiveToSeven, filter(Expressions.equal("cat", 8)), table)).isFalse();
    assertThat(ManifestWindowEncoding.inScope(
            fiveToSeven, filter(Expressions.greaterThan("cat", 7)), table)).isFalse();
    assertThat(ManifestWindowEncoding.inScope(
            fiveToSeven, filter(Expressions.lessThan("cat", 6)), table)).isTrue();
    assertThat(ManifestWindowEncoding.inScope(
            fiveToSeven, partitions(0, cat(7)), table)).isTrue();
    assertThat(ManifestWindowEncoding.inScope(
            fiveToSeven, partitions(0, cat(4)), table)).isFalse();
    assertThat(ManifestWindowEncoding.inScope(
            fiveToSeven, partitions(0, (ByteBuffer) null), table)).isFalse();
    assertThat(ManifestWindowEncoding.inScope(
            data(0, 5, 7, true), partitions(0, (ByteBuffer) null), table)).isTrue();
    // another spec's partitions are not this manifest's
    assertThat(ManifestWindowEncoding.inScope(
            fiveToSeven, partitions(1, cat(6)), table)).isFalse();
  }

  /** In v3, delete files that may share a partition with the files a record adds conflict. */
  @Test
  void deletesOverlapDataByPartition() {
    MetadataView table = table(BY_CAT);
    assertThat(ManifestWindowEncoding.overlaps(deletes(0, 5, 7), data(0, 7, 9, false), table))
        .isTrue();
    assertThat(ManifestWindowEncoding.overlaps(deletes(0, 5, 7), data(0, 8, 9, false), table))
        .isFalse();
    assertThat(ManifestWindowEncoding.overlaps(deletes(0, 8, 9), data(0, 5, 8, false), table))
        .isTrue();
  }

  private static Footprint.Scope partitions(int specId, ByteBuffer... values) {
    return Footprint.Scope.partitions(
        List.of(new Footprint.Partition(specId, Arrays.asList(values))));
  }

  private static MetadataView table(PartitionSpec spec) {
    return MetadataView.of(TableMetadata.newTableMetadata(
        SCHEMA, spec, SortOrder.unsorted(), "mem:///t", Map.of("format-version", "2")));
  }

  private static ManifestFile data(int specId, int lower, int upper, boolean nulls) {
    return manifest(specId, 0, lower, upper, nulls);
  }

  private static ManifestFile deletes(int specId, int lower, int upper) {
    return manifest(specId, 1, lower, upper, false);
  }

  private static ManifestFile manifest(
      int specId, int content, int lower, int upper, boolean nulls) {
    List<ManifestFile.PartitionFieldSummary> summaries = new ArrayList<>();
    summaries.add(new GenericPartitionFieldSummary(nulls, false, cat(lower), cat(upper)));
    return new ProtoCodec.DecodedManifestFile(
        "/m-" + lower + "-" + upper + ".avro", 6_144L, specId, content, 1L, 1L, 1L,
        1, 0, 0, 1L, 0L, 0L, summaries, null, null);
  }
}
