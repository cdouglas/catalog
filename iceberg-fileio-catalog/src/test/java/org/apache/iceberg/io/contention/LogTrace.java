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

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.LongConsumer;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.CatalogTransaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CatalogLog;
import org.apache.iceberg.io.ContentionAccess;
import org.apache.iceberg.io.InMemoryAtomicFileIO;
import org.apache.iceberg.io.LogCatalog;
import org.apache.iceberg.io.LogCatalogProperties;
import org.intentlog.Capabilities;
import org.intentlog.CommittedSet;
import org.intentlog.Intention;
import org.intentlog.LogNotSentException;
import org.intentlog.LogTail;
import org.intentlog.LogView;
import org.intentlog.Receipt;
import org.intentlog.Replay;
import org.intentlog.Replica;
import org.intentlog.SharedLog;
import org.intentlog.StateMachine;
import org.intentlog.fileio.AtomicFileLog;
import org.intentlog.memory.InMemorySharedLog;
import org.intentlog.wire.LogEnvelope;
import org.intentlog.wire.UuidV7;

/**
 * The log benchmark's generator: the entries writers' commits insert in the catalog's log, made
 * with no storage and no clock, so that the log's contents depend on the workload, the encoding
 * and a seed alone. What it measures is the log, not the storage under it (a catalog benchmark
 * measures that).
 *
 * <p>Writers are threads, each a {@link LogCatalog} of its own over one {@link InMemorySharedLog},
 * whose appends land unconditionally. They take turns as a {@link Scheduler} picks them, one at a
 * time, at each read and append of the log and at a job's work. An entry is stale by the entries
 * other writers landed between its writer's read of the log and its append (its window): the more
 * writers in flight ({@code writers}), the longer a job works between its read of a table and its
 * commit ({@code runtime_steps}), and the longer a commit takes from its read to its append
 * ({@code commit_steps}), the wider. Replay judges each entry where it lands: a rejected entry
 * stays in the log, and Iceberg's retry (or a catalog transaction's job) adds another.
 *
 * <p>With {@code oracle}, an entry lands only if it applies where it lands: the generator judges
 * it on the log's tail first and refuses it otherwise, before it is sent, and Iceberg's retry
 * restages it on a fresh read, still stale by the window it then waits. The oracle trace holds
 * only entries that apply, each judged by the encoding as the contended trace's are; ingested, its
 * rate bounds the encoding's goodput.
 *
 * <p>Writes log.bin (the checkpoint after setup and every entry after it, as a catalog object
 * that was never compacted: what {@link IngestRun} reads) and ops.jsonl (an operation per row:
 * its entries, their windows, its conflicts by reason, its refusals), and returns the summary.
 */
final class LogTrace {

  /** What a refused append's failure says, ahead of the reason its entry would not apply. */
  static final String REFUSED = "refused by the oracle: ";

  static final String DEFAULTS =
      "{"
          + "\"label\": \"trace\","
          + "\"repetition\": 0,"
          + "\"seed\": 1,"
          // the catalog: its conflict encoding, and inline TM+ML (ml), TM (tm) or pointer tables
          + "\"encoding\": \"manifest-window\","
          + "\"mode\": \"ml\","
          // the tables, as the contention driver's
          + "\"tables\": 16,"
          + "\"table_dist\": \"uniform\","
          + "\"table_zipf_alpha\": 1.5,"
          + "\"table_properties\": {},"
          + "\"partitions\": 32,"
          + "\"format_version\": 2,"
          + "\"preload_commits\": 50,"
          // writers in flight, each running one operation after another
          + "\"writers\": 16,"
          // the trace: entries landed after setup, and a bound on the scheduler's steps
          + "\"entries\": 20000,"
          + "\"max_steps\": 10000000,"
          // only entries that apply where they land are appended (the oracle trace)
          + "\"oracle\": false,"
          // the operations, as the contention driver's
          + "\"mix\": {\"fast_append\": 1.0},"
          + "\"partitions_per_txn\": 1,"
          + "\"partition_dist\": \"uniform\","
          + "\"zipf_alpha\": 1.5,"
          + "\"txn_tables\": 2,"
          + "\"txn_tables_dist\": \"fixed\","
          + "\"txn_tables_max\": 8,"
          + "\"txn_merge\": false,"
          + "\"txn_isolation\": \"serializable\","
          + "\"files_per_partition\": 1,"
          // a job's work between its read of a table and its commit, in the scheduler's steps
          // (logical time), by kind; and a commit's steps between its read and its append
          + "\"runtime_steps\": {},"
          + "\"commit_steps\": 0,"
          // Iceberg's commit retries, with no waits: the log has no clock
          + "\"retry_num\": 4,"
          // maintenance: every expire_every entries (0: never), all but the last expire_retain
          // snapshots of each table expire, with expire_retry_num retries
          + "\"expire_every\": 0,"
          + "\"expire_retain\": 50,"
          + "\"expire_retry_num\": 4,"
          // the format of the entries (IngestBench.CAPTURE_FORMAT)
          + "\"capture_format\": 1"
          + "}";

  private static final String WAREHOUSE = "mem:///warehouse";

  private final BenchConfig config;
  private final Path dir;
  private final Workload workload;
  private final InMemoryAtomicFileIO store = new InMemoryAtomicFileIO();
  private final InMemorySharedLog log = new InMemorySharedLog("catalog");
  private final Scheduler scheduler;
  private final List<byte[]> entries = new ArrayList<>();
  private final long target;
  private final long maxSteps;
  private Tail<?, ?, ?> tail;
  private BufferedWriter ops;
  private long opIds = 0;

  LogTrace(BenchConfig config, Path dir) {
    this.config = config;
    this.dir = dir;
    this.workload = new Workload(config);
    this.scheduler = new Scheduler(seed(-1));
    this.target = config.longValue("entries");
    this.maxSteps = config.longValue("max_steps");
    store.recordHistory(false);
  }

  private long seed(int writer) {
    return config.longValue("seed") * 1_000_003L + config.integer("repetition") * 7_919L + writer;
  }

  private static TableIdentifier table(int t) {
    return TableIdentifier.of("ns", "t" + t);
  }

  /** The catalog properties of every client; {@code retries} of each table commit. */
  private Map<String, String> catalogProperties(int retries) {
    Map<String, String> props = new HashMap<>();
    props.put(CatalogProperties.WAREHOUSE_LOCATION, WAREHOUSE);
    String mode = config.string("mode");
    props.put("fileio.catalog.inline", String.valueOf(!mode.equals("pointer")));
    props.put("fileio.catalog.inline.manifests", String.valueOf(mode.equals("ml")));
    // never compacted: the trace is every entry, and the ingest benchmark models compaction
    props.put(LogCatalogProperties.MAX_APPEND_COUNT, String.valueOf(Integer.MAX_VALUE));
    props.put(LogCatalogProperties.MAX_APPEND_SIZE, String.valueOf(Long.MAX_VALUE));
    props.put(LogCatalogProperties.CONFLICT_ENCODING, config.string("encoding"));
    props.put(LogCatalogProperties.RACE_MIN_WAIT_MS, "0");
    props.put(LogCatalogProperties.THROTTLE_MIN_WAIT_MS, "0");
    String prefix = CatalogProperties.TABLE_DEFAULT_PREFIX;
    props.put(prefix + TableProperties.COMMIT_NUM_RETRIES, String.valueOf(retries));
    props.put(prefix + TableProperties.COMMIT_MIN_RETRY_WAIT_MS, "0");
    props.put(prefix + TableProperties.COMMIT_MAX_RETRY_WAIT_MS, "0");
    props.put(prefix + TableProperties.FORMAT_VERSION, config.string("format_version"));
    return props;
  }

  private LogCatalog client(String name, SharedLog sharedLog, int retries) {
    LogCatalog catalog = new LogCatalog(name, sharedLog, store, new HashMap<>());
    catalog.initialize(name, catalogProperties(retries));
    return catalog;
  }

  /** Generates the trace; writes log.bin and ops.jsonl; returns the summary. */
  Map<String, Object> run() throws Exception {
    Files.createDirectories(dir);
    long started = System.nanoTime();
    byte[] checkpoint = setUp();
    ops = Files.newBufferedWriter(dir.resolve("ops.jsonl"), StandardCharsets.UTF_8);
    List<Thread> threads = new ArrayList<>();
    int writers = config.integer("writers");
    for (int w = 0; w < writers; w++) {
      int id = w;
      threads.add(participant(id, () -> write(id)));
    }
    if (config.integer("expire_every") > 0) {
      threads.add(participant(writers, () -> maintain(writers)));
    }
    threads.forEach(Thread::start);
    try {
      scheduler.run();
    } finally {
      for (Thread thread : threads) {
        thread.join();
      }
      ops.close();
    }
    Files.write(dir.resolve("log.bin"), AtomicFileLog.encode(checkpoint, entries));

    Map<String, Object> summary = new LinkedHashMap<>();
    summary.put("entries", entries.size());
    summary.put("entry_bytes", entries.stream().mapToLong(e -> e.length).sum());
    summary.put("rejected", tail.rejected);
    summary.put("rejection_reasons", tail.rejections);
    summary.put("refused", refusals.values().stream().mapToLong(Long::longValue).sum());
    summary.put("refusal_reasons", refusals);
    summary.put("outcomes", outcomes);
    summary.put("steps", scheduler.steps());
    summary.put("mean_window", windows == 0 ? 0 : (double) windowSum / windows);
    summary.put("elapsed_s", (System.nanoTime() - started) / 1e9);
    return summary;
  }

  /**
   * Creates the namespace, the tables (with their preloaded commits and the writers' branches),
   * and returns the log's checkpoint after them, where the trace starts.
   */
  private byte[] setUp() {
    LogCatalog admin = client("admin", log, config.integer("retry_num"));
    admin.createNamespace(Namespace.of("ns"));
    for (int t = 0; t < config.integer("tables"); t++) {
      Table table =
          admin.buildTable(table(t), Operations.SCHEMA).withPartitionSpec(Operations.SPEC)
              .withProperties(config.stringMap("table_properties"))
              .create();
      for (int i = 0; i < config.integer("preload_commits"); i++) {
        table.newFastAppend()
            .appendFile(Operations.newFile(table, i % config.integer("partitions"), 100))
            .commit();
      }
      if (config.mix().getOrDefault("wap", 0.0) > 0) {
        for (int b = 0; b < config.integer("writers"); b++) {
          table.manageSnapshots().createBranch("wap-" + b).commit();
        }
      }
    }
    this.tail = Tail.of(ContentionAccess.machine(), ContentionAccess.read(admin), seed(-1));
    return tail.checkpoint();
  }

  private boolean stopping() {
    return scheduler.entries() >= target || scheduler.steps() >= maxSteps;
  }

  private Thread participant(int id, Runnable body) {
    scheduler.join(id);
    return new Thread(
        () -> {
          // manifests written inline: nothing in the log waits on a pool
          Operations.MANIFESTS.set(
              org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors
                  .newDirectExecutorService());
          try {
            body.run();
          } catch (Scheduler.Stopped e) {
            // another writer failed, and the scheduler reports it
          } catch (Throwable e) {
            scheduler.fail(e);
          } finally {
            scheduler.leave(id);
          }
        },
        "writer-" + id);
  }

  // ============================================================
  // Writers
  // ============================================================

  private void write(int id) {
    Turns turns = new Turns(id);
    LogCatalog catalog = client("w" + id, turns, config.integer("retry_num"));
    SplittableRandom random = new SplittableRandom(seed(id));
    while (!stopping()) {
      String kind = workload.pick(random);
      Map<Integer, List<Integer>> targets = workload.targets(random, kind);
      com.fasterxml.jackson.databind.JsonNode byKind =
          config.node().get("runtime_steps").get(kind);
      long runtime = byKind != null ? byKind.asLong() : 0;
      operate(id, catalog, turns, kind, targets, runtime, random);
    }
  }

  private void operate(
      int id, LogCatalog catalog, Turns turns, String kind, Map<Integer, List<Integer>> targets,
      long runtime, SplittableRandom random) {
    List<Integer> tables = new ArrayList<>(targets.keySet());
    List<Integer> partitions = targets.get(tables.get(0));
    CatalogLog.CommitCounts counts = ContentionAccess.counts(catalog);
    long committed0 = counts.committed();
    long conflicts0 = counts.conflicts();
    long rejected0 = counts.rejected();
    Map<String, Long> reasons0 = counts.conflictReasons();
    Map<String, Long> refused0 = new TreeMap<>(turns.refused);
    long landed0 = turns.landed;
    turns.windows.clear();
    Map<String, Long> txnFailures = new TreeMap<>();
    long started = scheduler.steps();
    LongConsumer work =
        ignored -> {
          if (runtime > 0) {
            scheduler.await(id, Scheduler.Step.WORK, runtime, 0);
          }
        };
    int filesPerPartition = config.integer("files_per_partition");
    String outcome = "committed";
    String error = null;
    try {
      Operations.Skip skip;
      Table table = kind.equals("catalog_txn") ? null : catalog.loadTable(table(tables.get(0)));
      switch (kind) {
        case "fast_append":
          skip = Operations.fastAppend(table, partitions, filesPerPartition, false, null, work);
          break;
        case "merge_append":
          skip = Operations.fastAppend(table, partitions, filesPerPartition, true, null, work);
          break;
        case "wap":
          skip = Operations.fastAppend(
              table, partitions, filesPerPartition, false, "wap-" + id, work);
          break;
        case "validated_overwrite":
          skip = Operations.validatedOverwrite(table, partitions, filesPerPartition, work);
          break;
        case "compaction":
          skip = Operations.compaction(table, partitions.get(0), work);
          break;
        case "property":
          skip = Operations.property(table, partitions.get(0) % 8, work);
          break;
        case "schema":
          skip = Operations.schema(table, work);
          break;
        case "catalog_txn":
          List<TableIdentifier> ids = new ArrayList<>();
          Map<TableIdentifier, List<Integer>> byId = new HashMap<>();
          for (int t : tables) {
            ids.add(table(t));
            byId.put(table(t), targets.get(t));
          }
          skip = Operations.catalogTransaction(catalog, ids, byId::get, filesPerPartition,
              config.bool("txn_merge"),
              CatalogTransaction.IsolationLevel.valueOf(
                  config.string("txn_isolation").toUpperCase(java.util.Locale.ROOT)),
              new Operations.Retry(config.integer("retry_num"), 0, 0), random,
              ContentionRun.failedAttempt(counts, txnFailures), work);
          break;
        default:
          throw new IllegalArgumentException("Unknown operation: " + kind);
      }
      if (skip == Operations.Skip.NOTHING_TO_DO) {
        outcome = "nothing_to_do";
      }
    } catch (Scheduler.Stopped e) {
      throw e;
    } catch (ValidationException e) {
      outcome = "validation_failed";
      error = e.getMessage();
    } catch (CommitStateUnknownException e) {
      outcome = "unknown";
      error = e.getMessage();
    } catch (CommitFailedException e) {
      outcome = "retries_exhausted";
      error = e.getMessage();
    } catch (RuntimeException e) {
      outcome = "error";
      error = e.toString();
    }
    Map<String, Object> op = new LinkedHashMap<>();
    op.put("op_id", ++opIds);
    op.put("writer", id);
    op.put("kind", kind);
    op.put("table", tables.get(0));
    op.put("partitions", partitions);
    if (kind.equals("catalog_txn")) {
      op.put("tables", tables);
      op.put("txn_failures", txnFailures);
    }
    op.put("outcome", outcome);
    op.put("started_step", started);
    op.put("ended_step", scheduler.steps());
    op.put("runtime_steps", runtime);
    op.put("entries", turns.landed - landed0);
    op.put("windows", new ArrayList<>(turns.windows));
    op.put("catalog_commits", counts.committed() - committed0);
    op.put("conflicts", counts.conflicts() - conflicts0);
    op.put("refused", counts.rejected() - rejected0);
    op.put("conflict_reasons", ContentionRun.since(reasons0, counts.conflictReasons()));
    op.put("refusal_reasons", ContentionRun.since(refused0, turns.refused));
    if (error != null) {
      op.put("error", error.length() > 300 ? error.substring(0, 300) : error);
    }
    record(kind, outcome, op);
  }

  /**
   * The maintenance client: every {@code expire_every} entries, expires all but the last {@code
   * expire_retain} snapshots of each table, with {@code expire_retry_num} retries.
   */
  private void maintain(int id) {
    Turns turns = new Turns(id);
    LogCatalog catalog = client("maintenance", turns, config.integer("expire_retry_num"));
    long every = config.longValue("expire_every");
    long next = every;
    while (true) {
      scheduler.await(id, Scheduler.Step.WORK, 0, Math.min(next, target));
      if (stopping()) {
        return;
      }
      next = scheduler.entries() + every;
      for (int t = 0; t < config.integer("tables"); t++) {
        expire(catalog, turns, id, t);
      }
    }
  }

  private void expire(LogCatalog catalog, Turns turns, int id, int t) {
    CatalogLog.CommitCounts counts = ContentionAccess.counts(catalog);
    long conflicts0 = counts.conflicts();
    long refused0 = counts.rejected();
    Map<String, Long> reasons0 = counts.conflictReasons();
    long landed0 = turns.landed;
    turns.windows.clear();
    long started = scheduler.steps();
    String outcome = "committed";
    String error = null;
    try {
      catalog.loadTable(table(t)).expireSnapshots()
          .retainLast(config.integer("expire_retain"))
          .expireOlderThan(Long.MAX_VALUE)
          .cleanExpiredFiles(false)
          .commit();
    } catch (Scheduler.Stopped e) {
      throw e;
    } catch (RuntimeException e) {
      outcome = e instanceof CommitFailedException ? "retries_exhausted" : "error";
      error = e.toString();
    }
    Map<String, Object> op = new LinkedHashMap<>();
    op.put("op_id", ++opIds);
    op.put("writer", id);
    op.put("kind", "expire");
    op.put("table", t);
    op.put("outcome", outcome);
    op.put("started_step", started);
    op.put("ended_step", scheduler.steps());
    op.put("entries", turns.landed - landed0);
    op.put("windows", new ArrayList<>(turns.windows));
    op.put("conflicts", counts.conflicts() - conflicts0);
    op.put("refused", counts.rejected() - refused0);
    op.put("conflict_reasons", ContentionRun.since(reasons0, counts.conflictReasons()));
    if (error != null) {
      op.put("error", error.length() > 300 ? error.substring(0, 300) : error);
    }
    record("expire", outcome, op);
  }

  // Written by the running writer only (one at a time), read once every writer is done
  private final Map<String, Map<String, Long>> outcomes = new TreeMap<>();
  private final Map<String, Long> refusals = new TreeMap<>();
  private long windowSum = 0;
  private long windows = 0;

  private void record(String kind, String outcome, Map<String, Object> op) {
    outcomes.computeIfAbsent(kind, k -> new TreeMap<>()).merge(outcome, 1L, Long::sum);
    try {
      ops.write(BenchConfig.MAPPER.writeValueAsString(op));
      ops.newLine();
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }

  // ============================================================
  // The log, as one writer sees it
  // ============================================================

  /**
   * The shared log as writer {@code id} reaches it: it takes a turn before each read and append;
   * an append lands at the log's tail, where the generator's own replay ({@link Tail}) judges it
   * (and, with {@code oracle}, refuses it first if it would not apply). Reads through a write and
   * probes take no turn: they are the same request's.
   */
  private final class Turns implements SharedLog {
    private final int id;
    private final boolean oracle = config.bool("oracle");
    private long entriesAtRead = 0;
    private long landed = 0;
    private final List<Long> windows = new ArrayList<>();
    private final Map<String, Long> refused = new TreeMap<>();

    Turns(int id) {
      this.id = id;
    }

    private void read(Scheduler.Step step) {
      scheduler.await(id, step);
      entriesAtRead = scheduler.entries();
    }

    @Override
    public Capabilities capabilities() {
      return log.capabilities();
    }

    @Override
    public LogView read() {
      read(Scheduler.Step.READ);
      return log.read();
    }

    @Override
    public LogView readAfter(LogTail since) {
      read(Scheduler.Step.READ);
      return log.readAfter(since);
    }

    @Override
    public LogView readThrough(Receipt receipt, Duration wait) {
      return log.readThrough(receipt, wait);
    }

    @Override
    public LogTail probe() {
      return log.probe();
    }

    @Override
    public Receipt append(byte[] record, LogTail expected) {
      scheduler.await(id, Scheduler.Step.APPEND, config.longValue("commit_steps"), 0);
      if (oracle) {
        String why = tail.rejection(record);
        if (why != null) {
          refused.merge(why, 1L, Long::sum);
          refusals.merge(why, 1L, Long::sum);
          throw new LogNotSentException(REFUSED + why, null);
        }
      }
      Receipt receipt = log.append(record, expected);
      long window = scheduler.entries() - entriesAtRead;
      windows.add(window);
      windowSum += window;
      LogTrace.this.windows++;
      landed++;
      entries.add(record);
      tail.fold(record);
      scheduler.landed();
      return receipt;
    }

    @Override
    public Receipt replace(LogTail expected, byte[] checkpoint, List<byte[]> records) {
      throw new UnsupportedOperationException("the trace's log is never compacted");
    }

    @Override
    public void putCheckpoint(LogTail through, byte[] checkpoint) {
      throw new UnsupportedOperationException("the trace's log is never compacted");
    }
  }

  /**
   * The log's state at its tail, folded entry by entry as replay folds it: why an entry would not
   * apply there, and the entries replay rejected, by reason.
   */
  private static final class Tail<S, B extends StateMachine.Builder<S>, I extends Intention<B>> {
    private final StateMachine<S, B, I> machine;
    private final UUID logId;
    private final S start;
    private final CommittedSet startCommitted;
    private B builder;
    private CommittedSet.Builder committed;
    private long intentionRecords;
    private long rejected = 0;
    private final Map<String, Long> rejections = new TreeMap<>();

    private Tail(StateMachine<S, B, I> machine, Replica<S> replica, long seed) {
      this.machine = machine;
      // a log that names no id gets one, as a checkpoint must
      this.logId = replica.logId() != null ? replica.logId() : new UUID(0x7ace, seed);
      this.start = replica.state();
      this.startCommitted = replica.committed();
      this.builder = machine.toBuilder(replica.state());
      this.committed = replica.committed().toBuilder();
      this.intentionRecords = replica.intentionRecords();
    }

    static <S, B extends StateMachine.Builder<S>, I extends Intention<B>> Tail<S, B, I> of(
        StateMachine<S, B, I> machine, Replica<S> replica, long seed) {
      return new Tail<>(machine, replica, seed);
    }

    /** The log's checkpoint where the trace starts: the state read after setup. */
    byte[] checkpoint() {
      return LogEnvelope.encodeCheckpoint(
          logId, startCommitted.bytes(), machine.encodeCheckpoint(start));
    }

    /** Why {@code record} would not apply at the tail; null if it would. */
    String rejection(byte[] record) {
      LogEnvelope.Record decoded = LogEnvelope.decodeRecord(record);
      if (decoded.isAdvanceHorizon()) {
        return null;
      } else if (!UuidV7.isV7(decoded.id())) {
        return Replay.NOT_V7;
      } else if (!committed.mayApply(decoded.id())) {
        return "log: a copy of an entry that applied";
      }
      I intention = machine.decodeIntention(decoded.intention());
      if (intention.verify(builder)) {
        return null;
      }
      String why = intention.explain(builder);
      return why != null ? why : "unexplained";
    }

    /** Folds {@code record}, landed at the tail. */
    void fold(byte[] record) {
      Replay.Result<B> result = Replay.replayOnto(machine, builder, committed, logId,
          intentionRecords, List.of(record), null,
          (id, why) -> {
            rejected++;
            rejections.merge(why != null ? why : "unexplained", 1L, Long::sum);
          });
      builder = result.builder();
      committed = result.committed();
      intentionRecords = result.intentionRecords();
    }
  }
}
