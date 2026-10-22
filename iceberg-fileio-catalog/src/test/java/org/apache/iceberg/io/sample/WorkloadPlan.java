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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Turns a {@link SampleConfig} into a deterministic, ordered list of catalog
 * transactions. The plan is mode-independent: the identical sequence is replayed
 * under {@code orig}/{@code tm}/{@code tmml} so footprint differences are
 * attributable to the catalog representation alone.
 *
 * <p>Each transaction is a list of {@link Op}s (one per table for atomic groups,
 * exactly one otherwise). The plan is split into a checkpoint phase (folded into
 * the compacted checkpoint) and a log phase (kept as append records).
 */
public final class WorkloadPlan {

  public enum OpKind {
    /** Append a new data file (adds a manifest, grows row count). */
    FAST_APPEND,
    /** Add a new data file and remove a previously added one (rewrites manifests). */
    OVERWRITE,
    /** Remove a previously added data file (drops a manifest). */
    DELETE
  }

  /** A single operation against one table. */
  public static final class Op {
    public final int table;
    public final OpKind kind;
    public final long records;
    public final int partition;
    /**
     * For OVERWRITE/DELETE: selects which currently-live data file to remove, as
     * {@code ordinal % liveCount} evaluated at replay. Identical live-set
     * evolution across modes makes the selection deterministic. -1 for appends.
     */
    public final int removeOrdinal;

    Op(int table, OpKind kind, long records, int partition, int removeOrdinal) {
      this.table = table;
      this.kind = kind;
      this.records = records;
      this.partition = partition;
      this.removeOrdinal = removeOrdinal;
    }
  }

  /** One catalog transaction (single-table, or all tables for atomic groups). */
  public static final class Txn {
    public final List<Op> ops;

    Txn(List<Op> ops) {
      this.ops = ops;
    }
  }

  public final SampleConfig config;
  public final List<Txn> checkpointTxns;
  public final List<Txn> logTxns;

  private WorkloadPlan(SampleConfig config, List<Txn> checkpointTxns, List<Txn> logTxns) {
    this.config = config;
    this.checkpointTxns = checkpointTxns;
    this.logTxns = logTxns;
  }

  public static WorkloadPlan generate(SampleConfig config) {
    Random rnd = new Random(config.seed);
    // global commit counter drives the row-growth ramp
    int[] seq = {0};
    List<Txn> checkpoint = new ArrayList<>();
    for (int i = 0; i < config.checkpointTxns; i++) {
      checkpoint.add(nextTxn(config, rnd, seq));
    }
    List<Txn> log = new ArrayList<>();
    for (int i = 0; i < config.logTxns; i++) {
      log.add(nextTxn(config, rnd, seq));
    }
    return new WorkloadPlan(config, checkpoint, log);
  }

  private static Txn nextTxn(SampleConfig config, Random rnd, int[] seq) {
    List<Op> ops = new ArrayList<>();
    if (config.atomicGroups) {
      for (int t = 0; t < config.tables; t++) {
        ops.add(nextOp(config, rnd, seq, t));
      }
    } else {
      int table = config.tables == 1 ? 0 : rnd.nextInt(config.tables);
      ops.add(nextOp(config, rnd, seq, table));
    }
    return new Txn(ops);
  }

  private static Op nextOp(SampleConfig config, Random rnd, int[] seq, int table) {
    int n = seq[0]++;
    // Row-growth ramp: base grows with commit ordinal, plus seeded jitter.
    long records = 1_000L + 200L * n + rnd.nextInt(1_000);
    int partition = rnd.nextInt(16);
    OpKind kind = pickKind(config, rnd);
    int removeOrdinal = (kind == OpKind.FAST_APPEND) ? -1 : rnd.nextInt(Integer.MAX_VALUE);
    return new Op(table, kind, records, partition, removeOrdinal);
  }

  private static OpKind pickKind(SampleConfig config, Random rnd) {
    int total = config.wFastAppend + config.wOverwrite + config.wDelete;
    if (total <= 0) {
      return OpKind.FAST_APPEND;
    }
    int r = rnd.nextInt(total);
    if (r < config.wFastAppend) {
      return OpKind.FAST_APPEND;
    } else if (r < config.wFastAppend + config.wOverwrite) {
      return OpKind.OVERWRITE;
    } else {
      return OpKind.DELETE;
    }
  }
}
