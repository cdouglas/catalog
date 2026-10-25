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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * The partition bucket of a manifest's files, from its partition summaries: one bucket if every
 * file is in one partition of its spec (each field's bounds equal, and no nulls), hashed with the
 * spec; {@link #ALL} if the files may span partitions, or for delete files of an unpartitioned
 * spec, which apply to every partition. A partition's bucket is the same hash of its spec and
 * values, so a scope of partitions, or of a filter on partition columns, reads the buckets its
 * files are stamped in.
 */
final class PartitionKey {

  /** Every bucket. */
  static final int ALL = -1;

  private PartitionKey() {}

  /** The bucket, of {@code buckets}, of {@code manifest}'s files; {@link #ALL} for any. */
  static int bucket(ManifestFile manifest, int buckets) {
    List<ManifestFile.PartitionFieldSummary> fields = manifest.partitions();
    if (fields == null || buckets <= 0) {
      return ALL;
    }
    if (fields.isEmpty()) {
      return manifest.content() == ManifestContent.DELETES
          ? ALL
          : Math.floorMod(Integer.hashCode(manifest.partitionSpecId()), buckets);
    }
    int hash = Integer.hashCode(manifest.partitionSpecId());
    for (ManifestFile.PartitionFieldSummary field : fields) {
      ByteBuffer lower = field.lowerBound();
      ByteBuffer upper = field.upperBound();
      if (field.containsNull() || lower == null || !lower.equals(upper)
          || Boolean.TRUE.equals(field.containsNaN())) {
        return ALL;
      }
      hash = 31 * hash + lower.hashCode();
    }
    return Math.floorMod(hash, buckets);
  }

  /** The bucket of {@code partition}; {@link #ALL} if a value is null. */
  static int bucket(Footprint.Partition partition, int buckets) {
    int hash = Integer.hashCode(partition.specId);
    for (ByteBuffer value : partition.values) {
      if (value == null) {
        return ALL;
      }
      hash = 31 * hash + value.hashCode();
    }
    return Math.floorMod(hash, buckets);
  }

  /** The most partitions a filter's scope is enumerated to before it reads every bucket. */
  static final int MAX_PARTITIONS = 64;

  /**
   * The buckets of {@code buckets} a scope's files may be in, on {@code table}: {@link #ALL} for
   * every file, each partition's for partitions, and for a filter, those of the partitions of each
   * of the table's specs that the filter's inclusive projection names by equality (on every
   * partition field, at most {@link #MAX_PARTITIONS}), or {@link #ALL} where it does not.
   */
  static Set<Integer> buckets(Footprint.Scope scope, TableMetadata table, int buckets) {
    Set<Integer> read = new HashSet<>();
    if (scope.isEverything()) {
      read.add(ALL);
    } else if (scope.partitions != null) {
      for (Footprint.Partition partition : scope.partitions) {
        read.add(bucket(partition, buckets));
      }
    } else {
      try {
        Expression filter = scope.expression();
        for (PartitionSpec spec : table.specs()) {
          List<Footprint.Partition> partitions = partitions(filter, spec);
          if (partitions == null) {
            return Set.of(ALL);
          }
          for (Footprint.Partition partition : partitions) {
            read.add(bucket(partition, buckets));
          }
        }
      } catch (RuntimeException e) {
        return Set.of(ALL); // a filter it cannot project may hold rows anywhere
      }
    }
    return read.contains(ALL) ? Set.of(ALL) : read;
  }

  /**
   * The partitions of {@code spec} that rows matching {@code filter} may be in, if its inclusive
   * projection names them by equality on every field; null if it does not (a range, a field it
   * leaves free, too many).
   */
  static List<Footprint.Partition> partitions(Expression filter, PartitionSpec spec) {
    if (spec.isUnpartitioned()) {
      return List.of(new Footprint.Partition(spec.specId(), List.of())); // its one partition
    }
    Expression projected = Projections.inclusive(spec, true).project(filter);
    Types.StructType type = spec.partitionType();
    List<Map<Integer, ByteBuffer>> assignments =
        assignments(Binder.bind(type, projected, true), type);
    if (assignments == null) {
      return null;
    }
    List<Footprint.Partition> partitions = new ArrayList<>();
    for (Map<Integer, ByteBuffer> assignment : assignments) {
      if (assignment.size() != type.fields().size()) {
        return null;
      }
      List<ByteBuffer> values = new ArrayList<>();
      for (int i = 0; i < type.fields().size(); i++) {
        values.add(assignment.get(i));
      }
      partitions.add(new Footprint.Partition(spec.specId(), values));
    }
    return partitions;
  }

  /**
   * A bound partition filter as a disjunction of assignments, each of some fields (by position)
   * to a value: what rows matching it must satisfy; one empty assignment for a filter that names
   * no value, none for one no row matches, null for more than {@link #MAX_PARTITIONS}.
   */
  private static List<Map<Integer, ByteBuffer>> assignments(
      Expression expr, Types.StructType type) {
    switch (expr.op()) {
      case TRUE:
        return List.of(Map.of());
      case FALSE:
        return List.of();
      case AND:
        {
          List<Map<Integer, ByteBuffer>> left = assignments(((And) expr).left(), type);
          List<Map<Integer, ByteBuffer>> right = assignments(((And) expr).right(), type);
          if (left == null || right == null) {
            return left == null ? right : left; // either side alone bounds the rows
          }
          List<Map<Integer, ByteBuffer>> both = new ArrayList<>();
          for (Map<Integer, ByteBuffer> l : left) {
            for (Map<Integer, ByteBuffer> r : right) {
              Map<Integer, ByteBuffer> merged = new HashMap<>(l);
              boolean consistent = true;
              for (Map.Entry<Integer, ByteBuffer> field : r.entrySet()) {
                ByteBuffer was = merged.putIfAbsent(field.getKey(), field.getValue());
                consistent &= was == null || was.equals(field.getValue());
              }
              if (consistent) {
                both.add(merged);
              }
            }
          }
          return both.size() <= MAX_PARTITIONS ? both : null;
        }
      case OR:
        {
          List<Map<Integer, ByteBuffer>> left = assignments(((Or) expr).left(), type);
          List<Map<Integer, ByteBuffer>> right = assignments(((Or) expr).right(), type);
          if (left == null || right == null || left.size() + right.size() > MAX_PARTITIONS) {
            return null;
          }
          List<Map<Integer, ByteBuffer>> either = new ArrayList<>(left);
          either.addAll(right);
          return either;
        }
      case EQ:
      case IN:
        {
          BoundPredicate<?> pred = (BoundPredicate<?>) expr;
          if (!(pred.term() instanceof BoundReference)) {
            return List.of(Map.of());
          }
          int position = position(type, ((BoundReference<?>) pred.term()).fieldId());
          Type fieldType = type.fields().get(position).type();
          List<Object> values = new ArrayList<>();
          if (pred.isLiteralPredicate()) {
            values.add(pred.asLiteralPredicate().literal().value());
          } else if (pred.isSetPredicate()) {
            values.addAll(pred.asSetPredicate().literalSet());
          } else {
            return List.of(Map.of());
          }
          if (values.size() > MAX_PARTITIONS) {
            return null;
          }
          List<Map<Integer, ByteBuffer>> each = new ArrayList<>();
          for (Object value : values) {
            each.add(Map.of(position, Conversions.toByteBuffer(fieldType, value)));
          }
          return each;
        }
      default:
        return List.of(Map.of()); // a range, a null test, a negation: no value named
    }
  }

  private static int position(Types.StructType type, int fieldId) {
    List<Types.NestedField> fields = type.fields();
    for (int i = 0; i < fields.size(); i++) {
      if (fields.get(i).fieldId() == fieldId) {
        return i;
      }
    }
    throw new IllegalArgumentException("No partition field " + fieldId);
  }
}
