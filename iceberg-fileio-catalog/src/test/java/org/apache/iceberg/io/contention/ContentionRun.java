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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CatalogLog;
import org.apache.iceberg.io.ContentionAccess;
import org.apache.iceberg.io.FileIOCatalog;
import org.apache.iceberg.io.InMemoryAtomicFileIO;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * One run of the contention driver ({@link ContentionBench}): writers commit operations to tables
 * of one catalog over a store with a provider's latencies, for a warmup and then the measured
 * duration, and every operation is recorded.
 *
 * <p>Writers are the catalog's clients, each with its own {@link FileIOCatalog} instance, as each
 * would be a process of its own. Under a closed load, each of {@code writers} threads runs one
 * operation after another, with an exponential think time of mean {@code think_ms} between them.
 * Under an open load ({@code arrival_rate} above 0), operations arrive as a Poisson process and
 * wait for one of {@code max_inflight} writers. An operation records when it arrived, started and
 * ended, how it ended, the attempts its catalog made to commit it (each conflict replay reported
 * is one more attempt by Iceberg's retry), and the requests it made of the store.
 */
final class ContentionRun {
  private static final java.lang.management.ThreadMXBean CPU =
      java.lang.management.ManagementFactory.getThreadMXBean();
  private static final String WAREHOUSE = "mem:///wh";
  private static final String CATALOG = WAREHOUSE + "/catalog";

  private final BenchConfig config;
  private final Path dir;
  private final InMemoryAtomicFileIO store;
  private final LatencyFileIO io;
  private final AtomicBoolean measuring = new AtomicBoolean(false);
  private final AtomicBoolean stopping = new AtomicBoolean(false);
  private final AtomicLong opIds = new AtomicLong();
  private final List<String> weights = new ArrayList<>();
  private final double[] cumulative;
  private long startNanos;
  private BufferedWriter ops;

  ContentionRun(BenchConfig config, Path dir) {
    this.config = config;
    this.dir = dir;
    StoreProfile profile = StoreProfile.named(config.string("store"));
    this.store = new InMemoryAtomicFileIO(profile.supportsAppend);
    store.recordHistory(false);
    this.io =
        new LatencyFileIO(store, profile, CATALOG, config.number("time_scale"),
            config.longValue("seed") * 7919 + config.integer("repetition"));
    Map<String, Double> mix = config.mix();
    double total = mix.values().stream().mapToDouble(Double::doubleValue).sum();
    cumulative = new double[mix.size()];
    double sum = 0;
    int i = 0;
    for (Map.Entry<String, Double> e : mix.entrySet()) {
      weights.add(e.getKey());
      sum += e.getValue() / total;
      cumulative[i++] = sum;
    }
  }

  /** The catalog properties of every client. */
  private Map<String, String> catalogProperties() {
    Map<String, String> props = new HashMap<>();
    props.put(CatalogProperties.WAREHOUSE_LOCATION, WAREHOUSE);
    String mode = config.string("mode");
    props.put("fileio.catalog.inline", String.valueOf(!mode.equals("pointer")));
    props.put("fileio.catalog.inline.manifests", String.valueOf(mode.equals("ml")));
    props.put("fileio.catalog.max.append.count", config.string("max_append_count"));
    props.put("fileio.catalog.max.append.size", config.string("max_append_size"));
    props.put("fileio.catalog.conflict.encoding", config.string("encoding"));
    String prefix = CatalogProperties.TABLE_DEFAULT_PREFIX;
    props.put(prefix + TableProperties.COMMIT_NUM_RETRIES, config.string("retry_num"));
    props.put(prefix + TableProperties.COMMIT_MIN_RETRY_WAIT_MS, config.string("retry_min_wait_ms"));
    props.put(prefix + TableProperties.COMMIT_MAX_RETRY_WAIT_MS, config.string("retry_max_wait_ms"));
    props.put(prefix + TableProperties.FORMAT_VERSION, config.string("format_version"));
    return props;
  }

  private FileIOCatalog client(String name) {
    FileIOCatalog catalog = new FileIOCatalog(name, CATALOG, io, new HashMap<>());
    catalog.initialize(name, catalogProperties());
    return catalog;
  }

  private static TableIdentifier table(int t) {
    return TableIdentifier.of("ns", "t" + t);
  }

  /** Runs, writes ops.jsonl and samples.jsonl into the run's directory; returns the summary. */
  Map<String, Object> run() throws Exception {
    Files.createDirectories(dir);
    setUp();
    ops = Files.newBufferedWriter(dir.resolve("ops.jsonl"), StandardCharsets.UTF_8);
    List<Thread> threads = new ArrayList<>();
    startNanos = System.nanoTime();
    long warmupNanos = TimeUnit.SECONDS.toNanos(config.integer("warmup_s"));
    long endNanos = startNanos + warmupNanos + TimeUnit.SECONDS.toNanos(config.integer("duration_s"));

    Thread sampler = new Thread(() -> sample(endNanos), "sampler");
    sampler.start();
    if (config.integer("expire_every_s") > 0) {
      Thread maintenance = new Thread(() -> maintain(endNanos), "maintenance");
      maintenance.start();
      threads.add(maintenance);
    }
    if (config.number("arrival_rate") > 0) {
      threads.addAll(openLoad(endNanos));
    } else {
      for (int w = 0; w < config.integer("writers"); w++) {
        int writer = w;
        Thread thread = new Thread(() -> closedWriter(writer, endNanos), "writer-" + w);
        thread.start();
        threads.add(thread);
      }
    }
    LockSupport.parkNanos(warmupNanos);
    measuring.set(true);
    Requests requestsAtStart = Requests.total();
    long cpuAtStart = processCpuNanos();
    while (System.nanoTime() < endNanos) {
      LockSupport.parkNanos(Math.min(endNanos - System.nanoTime(), 100_000_000L));
    }
    stopping.set(true);
    Requests requestsMeasured = Requests.total().minus(requestsAtStart);
    long cpuMeasured = processCpuNanos() - cpuAtStart;
    long drainDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(config.integer("drain_s"));
    int stragglers = 0;
    for (Thread thread : threads) {
      long left = drainDeadline - System.nanoTime();
      thread.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(left)));
      if (thread.isAlive()) {
        stragglers++;
        thread.interrupt();
      }
    }
    sampler.join();
    synchronized (this) {
      ops.close();
    }
    Map<String, Object> summary = new LinkedHashMap<>();
    summary.put("stragglers", stragglers);
    summary.put("catalog_bytes", catalogBytes().length);
    // every thread's requests and the process's CPU over the measured duration
    summary.put("requests", requestsMeasured.fields());
    summary.put("process_cpu_s", cpuMeasured / 1e9);
    return summary;
  }

  private static long processCpuNanos() {
    java.lang.management.OperatingSystemMXBean os =
        java.lang.management.ManagementFactory.getOperatingSystemMXBean();
    return os instanceof com.sun.management.OperatingSystemMXBean
        ? ((com.sun.management.OperatingSystemMXBean) os).getProcessCpuTime()
        : -1L;
  }

  private void setUp() {
    FileIOCatalog admin = client("admin");
    admin.createNamespace(Namespace.of("ns"));
    for (int t = 0; t < config.integer("tables"); t++) {
      Table table =
          admin.buildTable(table(t), Operations.SCHEMA).withPartitionSpec(Operations.SPEC)
              .create();
      for (int i = 0; i < config.integer("preload_commits"); i++) {
        table.newFastAppend()
            .appendFile(Operations.newFile(table, i % config.integer("partitions"), 100))
            .commit();
      }
      if (config.mix().getOrDefault("wap", 0.0) > 0) {
        int branches = Math.max(config.integer("writers"), config.integer("max_inflight"));
        for (int b = 0; b < branches; b++) {
          table.manageSnapshots().createBranch("wap-" + b).commit();
        }
      }
    }
  }

  private byte[] catalogBytes() {
    try (SeekableInputStream in = store.newInputFile(CATALOG).newStream()) {
      return in.readAllBytes();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // ============================================================
  // Load
  // ============================================================

  private void closedWriter(int writer, long endNanos) {
    FileIOCatalog catalog = client("w" + writer);
    SplittableRandom random = new SplittableRandom(seed(writer));
    double think = config.number("think_ms");
    while (!stopping.get() && System.nanoTime() < endNanos) {
      operate(catalog, writer, random, System.nanoTime());
      if (think > 0 && !stopping.get()) {
        LockSupport.parkNanos((long) (exponential(random, think) * 1e6));
      }
    }
  }

  private List<Thread> openLoad(long endNanos) {
    int slots = config.integer("max_inflight");
    BlockingQueue<Long> arrivals = new ArrayBlockingQueue<>(100_000);
    List<Thread> threads = new ArrayList<>();
    for (int w = 0; w < slots; w++) {
      int writer = w;
      Thread thread =
          new Thread(
              () -> {
                FileIOCatalog catalog = client("w" + writer);
                SplittableRandom random = new SplittableRandom(seed(writer));
                try {
                  while (true) {
                    Long arrived = arrivals.poll(100, TimeUnit.MILLISECONDS);
                    if (arrived == null) {
                      if (stopping.get()) {
                        return;
                      }
                      continue;
                    }
                    if (arrived < 0) {
                      return;
                    }
                    operate(catalog, writer, random, arrived);
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              },
              "writer-" + w);
      thread.start();
      threads.add(thread);
    }
    Thread arrival =
        new Thread(
            () -> {
              SplittableRandom random = new SplittableRandom(seed(-1));
              double meanMs = 1000.0 / config.number("arrival_rate");
              long next = System.nanoTime();
              while (next < endNanos && !stopping.get()) {
                next += (long) (exponential(random, meanMs) * 1e6);
                LockSupport.parkNanos(Math.max(0, next - System.nanoTime()));
                if (!arrivals.offer(next)) {
                  record(dropped(next));
                }
              }
              for (int w = 0; w < slots; w++) {
                arrivals.offer(-1L);
              }
            },
            "arrivals");
    arrival.start();
    threads.add(arrival);
    return threads;
  }

  private long seed(int writer) {
    return config.longValue("seed") * 1_000_003L + config.integer("repetition") * 7_919L + writer;
  }

  private static double exponential(SplittableRandom random, double mean) {
    return -mean * Math.log(1.0 - random.nextDouble());
  }

  // ============================================================
  // Operations
  // ============================================================

  private String pick(SplittableRandom random) {
    double u = random.nextDouble();
    for (int i = 0; i < cumulative.length; i++) {
      if (u < cumulative[i]) {
        return weights.get(i);
      }
    }
    return weights.get(weights.size() - 1);
  }

  /** {@code n} distinct values of {@code [0, of)}, uniform or Zipf. */
  private List<Integer> choose(SplittableRandom random, int n, int of, String dist) {
    List<Integer> chosen = new ArrayList<>(n);
    n = Math.min(n, of);
    while (chosen.size() < n) {
      int v = dist.equals("zipf") ? zipf(random, of, config.number("zipf_alpha")) : random.nextInt(of);
      if (!chosen.contains(v)) {
        chosen.add(v);
      }
    }
    return chosen;
  }

  /** A truncated Zipf draw of {@code [0, n)}: P(k) proportional to 1/(k+1)^alpha. */
  private static int zipf(SplittableRandom random, int n, double alpha) {
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

  private void operate(FileIOCatalog catalog, int writer, SplittableRandom random, long arrived) {
    String kind = pick(random);
    int tableIndex = choose(random, 1, config.integer("tables"), config.string("table_dist")).get(0);
    List<Integer> partitions =
        choose(random, config.integer("partitions_per_txn"), config.integer("partitions"),
            config.string("partition_dist"));
    com.fasterxml.jackson.databind.JsonNode byKind = config.node().get("runtime_ms").get(kind);
    double median = byKind != null ? byKind.asDouble() : config.number("runtime_median_ms");
    double runtimeMs =
        median > 0 ? median * Math.exp(config.number("runtime_sigma") * gaussian(random)) : 0;
    run(catalog, kind, tableIndex, partitions, runtimeMs, writer, arrived);
  }

  private void run(
      FileIOCatalog catalog, String kind, int tableIndex, List<Integer> partitions,
      double runtimeMs, int writer, long arrived) {
    boolean measured = measuring.get() && !stopping.get();
    CatalogLog.CommitCounts counts = ContentionAccess.counts(catalog);
    long committed0 = counts.committed();
    long conflicts0 = counts.conflicts();
    long rejected0 = counts.rejected();
    long unknown0 = counts.unknown();
    Requests requests0 = Requests.mine().copy();
    long cpu0 = CPU.getCurrentThreadCpuTime();
    long started = System.nanoTime();
    long[] commitStart = {started};
    Runnable runtime =
        () -> {
          if (runtimeMs > 0) {
            LockSupport.parkNanos((long) (runtimeMs * 1e6));
          }
          commitStart[0] = System.nanoTime();
        };
    String outcome = "committed";
    String error = null;
    try {
      Table table = catalog.loadTable(table(tableIndex));
      Operations.Skip skip;
      switch (kind) {
        case "fast_append":
          skip = Operations.fastAppend(table, partitions, config.integer("files_per_partition"),
              false, null, ignored -> runtime.run());
          break;
        case "merge_append":
          skip = Operations.fastAppend(table, partitions, config.integer("files_per_partition"),
              true, null, ignored -> runtime.run());
          break;
        case "wap":
          skip = Operations.fastAppend(table, partitions, config.integer("files_per_partition"),
              false, "wap-" + writer, ignored -> runtime.run());
          break;
        case "validated_overwrite":
          skip = Operations.validatedOverwrite(table, partitions,
              config.integer("files_per_partition"), ignored -> runtime.run());
          break;
        case "compaction":
          skip = Operations.compaction(table, partitions.get(0), ignored -> runtime.run());
          break;
        case "property":
          skip = Operations.property(table, partitions.get(0) % 8, ignored -> runtime.run());
          break;
        default:
          throw new IllegalArgumentException("Unknown operation: " + kind);
      }
      if (skip == Operations.Skip.NOTHING_TO_DO) {
        outcome = "nothing_to_do";
      }
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
    long ended = System.nanoTime();
    long cpu = CPU.getCurrentThreadCpuTime() - cpu0;
    Map<String, Object> op = new LinkedHashMap<>();
    op.put("op_id", opIds.incrementAndGet());
    op.put("writer", writer);
    op.put("kind", kind);
    op.put("table", tableIndex);
    op.put("partitions", partitions);
    op.put("measured", measured);
    op.put("arrived_ms", (arrived - startNanos) / 1e6);
    op.put("started_ms", (started - startNanos) / 1e6);
    op.put("ended_ms", (ended - startNanos) / 1e6);
    op.put("queue_ms", (started - arrived) / 1e6);
    op.put("runtime_ms", (commitStart[0] - started) / 1e6);
    op.put("commit_ms", (ended - commitStart[0]) / 1e6);
    op.put("latency_ms", (ended - arrived) / 1e6);
    op.put("outcome", outcome);
    op.put("catalog_commits", counts.committed() - committed0);
    op.put("conflicts", counts.conflicts() - conflicts0);
    op.put("rejected", counts.rejected() - rejected0);
    op.put("unknown", counts.unknown() - unknown0);
    op.put("attempts",
        counts.committed() - committed0 + counts.conflicts() - conflicts0
            + counts.rejected() - rejected0 + counts.unknown() - unknown0);
    op.put("cpu_ms", cpu / 1e6);
    op.putAll(Requests.mine().minus(requests0).fields());
    if (error != null) {
      op.put("error", error.length() > 300 ? error.substring(0, 300) : error);
    }
    record(op);
  }

  private Map<String, Object> dropped(long arrived) {
    Map<String, Object> op = new LinkedHashMap<>();
    op.put("op_id", opIds.incrementAndGet());
    op.put("kind", "arrival");
    op.put("measured", measuring.get() && !stopping.get());
    op.put("arrived_ms", (arrived - startNanos) / 1e6);
    op.put("outcome", "dropped");
    return op;
  }

  private synchronized void record(Map<String, Object> op) {
    try {
      ops.write(BenchConfig.MAPPER.writeValueAsString(op));
      ops.newLine();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void maintain(long endNanos) {
    FileIOCatalog catalog = client("maintenance");
    long every = TimeUnit.SECONDS.toNanos(config.integer("expire_every_s"));
    long next = System.nanoTime() + every;
    while (!stopping.get() && next < endNanos) {
      LockSupport.parkNanos(Math.max(0, next - System.nanoTime()));
      next += every;
      for (int t = 0; t < config.integer("tables"); t++) {
        expire(catalog, t);
      }
    }
  }

  private void expire(FileIOCatalog catalog, int tableIndex) {
    boolean measured = measuring.get() && !stopping.get();
    CatalogLog.CommitCounts counts = ContentionAccess.counts(catalog);
    long conflicts0 = counts.conflicts();
    long committed0 = counts.committed();
    long started = System.nanoTime();
    String outcome = "committed";
    String error = null;
    try {
      catalog.loadTable(table(tableIndex)).expireSnapshots()
          .retainLast(config.integer("expire_retain"))
          .expireOlderThan(System.currentTimeMillis())
          .cleanExpiredFiles(false)
          .commit();
    } catch (RuntimeException e) {
      outcome = e instanceof CommitFailedException ? "retries_exhausted" : "error";
      error = e.toString();
    }
    long ended = System.nanoTime();
    Map<String, Object> op = new LinkedHashMap<>();
    op.put("op_id", opIds.incrementAndGet());
    op.put("writer", -1);
    op.put("kind", "expire");
    op.put("table", tableIndex);
    op.put("measured", measured);
    op.put("arrived_ms", (started - startNanos) / 1e6);
    op.put("started_ms", (started - startNanos) / 1e6);
    op.put("ended_ms", (ended - startNanos) / 1e6);
    op.put("latency_ms", (ended - started) / 1e6);
    op.put("outcome", outcome);
    op.put("catalog_commits", counts.committed() - committed0);
    op.put("conflicts", counts.conflicts() - conflicts0);
    if (error != null) {
      op.put("error", error.length() > 300 ? error.substring(0, 300) : error);
    }
    record(op);
  }

  /** Samples the catalog object (size, records since its checkpoint) until the run ends. */
  private void sample(long endNanos) {
    long every = TimeUnit.MILLISECONDS.toNanos(config.integer("sample_every_ms"));
    try (BufferedWriter out =
        Files.newBufferedWriter(dir.resolve("samples.jsonl"), StandardCharsets.UTF_8)) {
      while (System.nanoTime() < endNanos && !stopping.get()) {
        LockSupport.parkNanos(every);
        byte[] bytes = catalogBytes();
        Map<String, Object> sample = new LinkedHashMap<>();
        sample.put("t_ms", (System.nanoTime() - startNanos) / 1e6);
        sample.put("measured", measuring.get());
        sample.put("catalog_bytes", bytes.length);
        sample.put("checkpoint_bytes", ContentionAccess.checkpointBytes(bytes));
        sample.put("records", ContentionAccess.records(bytes));
        sample.put("objects", store.size());
        Runtime rt = Runtime.getRuntime();
        sample.put("heap_used", rt.totalMemory() - rt.freeMemory());
        out.write(BenchConfig.MAPPER.writeValueAsString(sample));
        out.newLine();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static double gaussian(SplittableRandom random) {
    double u = 1.0 - random.nextDouble();
    double v = random.nextDouble();
    return Math.sqrt(-2.0 * Math.log(u)) * Math.cos(2.0 * Math.PI * v);
  }
}
