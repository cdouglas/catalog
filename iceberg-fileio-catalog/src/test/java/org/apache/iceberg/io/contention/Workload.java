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
package org.apache.iceberg.io.contention;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

/**
 * What a writer's next operation is, as a configuration's mix and distributions choose it: its
 * kind ({@code mix}), its tables ({@code tables}, {@code table_dist}; a catalog transaction's
 * width from {@code txn_tables}) and each table's partitions ({@code partitions_per_txn} of
 * {@code partitions}, {@code partition_dist}). Shared by the contention driver and the log
 * generator ({@link LogTrace}).
 */
final class Workload {
  private final BenchConfig config;
  private final List<String> kinds = new ArrayList<>();
  private final double[] cumulative;

  Workload(BenchConfig config) {
    this.config = config;
    Map<String, Double> mix = config.mix();
    double total = mix.values().stream().mapToDouble(Double::doubleValue).sum();
    cumulative = new double[mix.size()];
    double sum = 0;
    int i = 0;
    for (Map.Entry<String, Double> e : mix.entrySet()) {
      kinds.add(e.getKey());
      sum += e.getValue() / total;
      cumulative[i++] = sum;
    }
  }

  /** An operation's kind, by the mix's weights. */
  String pick(SplittableRandom random) {
    double u = random.nextDouble();
    for (int i = 0; i < cumulative.length; i++) {
      if (u < cumulative[i]) {
        return kinds.get(i);
      }
    }
    return kinds.get(kinds.size() - 1);
  }

  /**
   * The tables an operation of {@code kind} writes, each with the partitions it touches there: a
   * catalog transaction several, each in partitions of its own.
   */
  Map<Integer, List<Integer>> targets(SplittableRandom random, String kind) {
    List<Integer> tables = tables(random, kind.equals("catalog_txn") ? txnWidth(random) : 1);
    Map<Integer, List<Integer>> partitions = new LinkedHashMap<>();
    for (int t : tables) {
      partitions.put(t, partitions(random));
    }
    return partitions;
  }

  /** {@code n} distinct tables, as {@code table_dist} chooses them. */
  List<Integer> tables(SplittableRandom random, int n) {
    return choose(random, n, config.integer("tables"), config.string("table_dist"),
        config.number("table_zipf_alpha"));
  }

  /** The partitions an operation on a table touches, as {@code partition_dist} chooses them. */
  List<Integer> partitions(SplittableRandom random) {
    return choose(random, config.integer("partitions_per_txn"), config.integer("partitions"),
        config.string("partition_dist"), config.number("zipf_alpha"));
  }

  /**
   * The tables a catalog transaction writes: {@code txn_tables}, or with {@code txn_tables_dist}
   * exponential a draw of that mean, rounded down; at least 1, at most {@code txn_tables_max}.
   */
  int txnWidth(SplittableRandom random) {
    String dist = config.string("txn_tables_dist");
    int k;
    if (dist.equals("fixed")) {
      k = config.integer("txn_tables");
    } else if (dist.equals("exponential")) {
      k = (int) Math.floor(exponential(random, config.number("txn_tables")));
    } else {
      throw new IllegalArgumentException("txn_tables_dist is fixed or exponential, not " + dist);
    }
    return Math.max(1, Math.min(k, config.integer("txn_tables_max")));
  }

  /** {@code n} distinct values of {@code [0, of)}, uniform or Zipf of exponent {@code alpha}. */
  static List<Integer> choose(SplittableRandom random, int n, int of, String dist, double alpha) {
    List<Integer> chosen = new ArrayList<>(n);
    n = Math.min(n, of);
    while (chosen.size() < n) {
      int v = dist.equals("zipf") ? zipf(random, of, alpha) : random.nextInt(of);
      if (!chosen.contains(v)) {
        chosen.add(v);
      }
    }
    return chosen;
  }

  /** A truncated Zipf draw of {@code [0, n)}: P(k) proportional to 1/(k+1)^alpha. */
  static int zipf(SplittableRandom random, int n, double alpha) {
    double norm = 0;
    for (int k = 1; k <= n; k++) {
      norm += 1.0 / Math.pow(k, alpha);
    }
    double u = random.nextDouble() * norm;
    double sum = 0;
    for (int k = 1; k <= n; k++) {
      sum += 1.0 / Math.pow(k, alpha);
      if (u < sum) {
        return k - 1;
      }
    }
    return n - 1;
  }

  static double exponential(SplittableRandom random, double mean) {
    return -mean * Math.log(1.0 - random.nextDouble());
  }

  /** A standard normal draw (Box-Muller). */
  static double gaussian(SplittableRandom random) {
    double u1 = 1.0 - random.nextDouble();
    double u2 = random.nextDouble();
    return Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
  }
}
