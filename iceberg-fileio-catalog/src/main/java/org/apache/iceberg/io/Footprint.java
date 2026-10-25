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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.CommitFootprint;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;

/**
 * A producer's footprint as a record carries it ({@link CommitFootprint}, catalog.proto's
 * CommitFootprint): its kind, and a validated commit's checks, each a kind of change and a scope:
 * every file, a row filter (as Iceberg's expression JSON), or partitions, each a spec id and its
 * values in Iceberg's single-value serialization, the form a manifest's partition summaries bound
 * them in.
 */
final class Footprint {

  enum Kind {
    BLIND,
    VALIDATED,
    OPAQUE
  }

  static final Footprint BLIND = new Footprint(Kind.BLIND, List.of());
  static final Footprint OPAQUE = new Footprint(Kind.OPAQUE, List.of());

  final Kind kind;
  /** A validated commit's checks, in its producer's order; empty for another kind. */
  final List<Check> checks;

  private Footprint(Kind kind, List<Check> checks) {
    this.kind = kind;
    this.checks = checks;
  }

  static Footprint validated(List<Check> checks) {
    return new Footprint(Kind.VALIDATED, Collections.unmodifiableList(new ArrayList<>(checks)));
  }

  /**
   * The footprint a producer gave its commit, its partitions serialized with {@code specs}, the
   * table's; null for none, and opaque if a scope cannot be carried (a spec the table lacks, a
   * filter the expression JSON cannot hold).
   */
  static Footprint of(CommitFootprint footprint, Map<Integer, PartitionSpec> specs) {
    if (footprint == null) {
      return null;
    }
    switch (footprint.kind()) {
      case BLIND:
        return BLIND;
      case VALIDATED:
        try {
          List<Check> checks = new ArrayList<>();
          for (CommitFootprint.Check check : footprint.checks()) {
            checks.add(new Check(check.kind(), Scope.of(check.scope(), specs)));
          }
          return validated(checks);
        } catch (RuntimeException e) {
          return OPAQUE;
        }
      default:
        return OPAQUE;
    }
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof Footprint
        && ((Footprint) other).kind == kind
        && ((Footprint) other).checks.equals(checks);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kind, checks);
  }

  @Override
  public String toString() {
    return kind == Kind.VALIDATED ? "validated " + checks : kind.toString().toLowerCase();
  }

  /** That no commit since added data files, added delete files, or removed data files in a scope. */
  static final class Check {
    final CommitFootprint.Check.Kind kind;
    final Scope scope;

    Check(CommitFootprint.Check.Kind kind, Scope scope) {
      this.kind = Objects.requireNonNull(kind);
      this.scope = Objects.requireNonNull(scope);
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Check
          && ((Check) other).kind == kind
          && ((Check) other).scope.equals(scope);
    }

    @Override
    public int hashCode() {
      return Objects.hash(kind, scope);
    }

    @Override
    public String toString() {
      return kind + " in " + scope;
    }
  }

  /** Every file, the files that may hold rows matching a filter, or the files in partitions. */
  static final class Scope {
    static final Scope EVERYTHING = new Scope(null, null);

    /** The row filter as Iceberg's expression JSON; null unless a filter. */
    final String filter;
    /** The partitions; null unless partitions. */
    final List<Partition> partitions;
    private Expression expression;

    private Scope(String filter, List<Partition> partitions) {
      this.filter = filter;
      this.partitions = partitions;
    }

    static Scope filter(String json) {
      return new Scope(Objects.requireNonNull(json), null);
    }

    static Scope partitions(List<Partition> partitions) {
      return new Scope(null, Collections.unmodifiableList(new ArrayList<>(partitions)));
    }

    static Scope of(CommitFootprint.Scope scope, Map<Integer, PartitionSpec> specs) {
      if (scope.isEverything()) {
        return EVERYTHING;
      } else if (scope.filter() != null) {
        return filter(ExpressionParser.toJson(scope.filter()));
      }
      List<Partition> partitions = new ArrayList<>();
      for (Pair<Integer, StructLike> partition : scope.partitions()) {
        PartitionSpec spec = specs.get(partition.first());
        if (spec == null) {
          throw new IllegalArgumentException("Unknown partition spec " + partition.first());
        }
        partitions.add(Partition.of(spec, partition.second()));
      }
      return partitions(partitions);
    }

    boolean isEverything() {
      return filter == null && partitions == null;
    }

    /** The filter, parsed once; throws if the JSON is not an expression this build reads. */
    Expression expression() {
      if (expression == null && filter != null) {
        expression = ExpressionParser.fromJson(filter);
      }
      return expression;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Scope
          && Objects.equals(((Scope) other).filter, filter)
          && Objects.equals(((Scope) other).partitions, partitions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(filter, partitions);
    }

    @Override
    public String toString() {
      return isEverything() ? "everything" : filter != null ? filter : partitions.toString();
    }
  }

  /** A partition: a spec id and each field's value, serialized; a null element for null. */
  static final class Partition {
    final int specId;
    final List<ByteBuffer> values;

    Partition(int specId, List<ByteBuffer> values) {
      this.specId = specId;
      this.values = Collections.unmodifiableList(new ArrayList<>(values));
    }

    static Partition of(PartitionSpec spec, StructLike partition) {
      List<Types.NestedField> fields = spec.partitionType().fields();
      if (partition.size() != fields.size()) {
        throw new IllegalArgumentException(
            "Partition of " + partition.size() + " fields for spec " + spec.specId());
      }
      List<ByteBuffer> values = new ArrayList<>(fields.size());
      for (int i = 0; i < fields.size(); i++) {
        Object value = partition.get(i, Object.class);
        values.add(value != null ? Conversions.toByteBuffer(fields.get(i).type(), value) : null);
      }
      return new Partition(spec.specId(), values);
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Partition
          && ((Partition) other).specId == specId
          && ((Partition) other).values.equals(values);
    }

    @Override
    public int hashCode() {
      return Objects.hash(specId, values);
    }

    @Override
    public String toString() {
      StringBuilder out = new StringBuilder().append(specId).append('(');
      for (int i = 0; i < values.size(); i++) {
        ByteBuffer value = values.get(i);
        out.append(i > 0 ? ", " : "");
        if (value == null) {
          out.append("null");
        } else {
          for (int b = value.position(); b < value.limit(); b++) {
            out.append(String.format("%02x", value.get(b)));
          }
        }
      }
      return out.append(')').toString();
    }
  }
}
