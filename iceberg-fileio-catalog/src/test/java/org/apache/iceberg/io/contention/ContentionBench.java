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

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.IcebergBuild;

/**
 * The multi-writer contention driver: runs each configuration ({@link BenchConfig}) of the files
 * it is given ({@link ContentionRun}) and writes each run's results under the output directory as
 * {@code <label>-<config hash>/<UTC start>/}: {@code manifest.json} (the configuration, versions,
 * host and a summary), {@code ops.jsonl} (one record per operation) and {@code samples.jsonl} (the
 * catalog object, sampled). labbook reads them (fileio-catalog lab/).
 *
 * <pre>{@code
 * java -Xmx4g -Diceberg.worker.num-threads=256 -cp <test classpath> \
 *   org.apache.iceberg.io.contention.ContentionBench [--out DIR] [--shard I/N] [--dry-run] \
 *   config.json...
 * }</pre>
 *
 * <p>{@code --shard I/N} runs the runs whose index (over every file's runs, in order) is I modulo
 * N, so N processes share a campaign.
 *
 * <p>Environment for the manifest: FILEIO_GIT (commit), FILEIO_DIRTY (true/false), set by
 * bench/run.sh.
 */
public final class ContentionBench {
  private static final DateTimeFormatter STAMP =
      DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

  private ContentionBench() {}

  public static void main(String[] args) throws Exception {
    Path out = Path.of("bench", "results");
    boolean dryRun = false;
    int shard = 0;
    int shards = 1;
    List<Path> files = new ArrayList<>();
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("--out")) {
        out = Path.of(args[++i]);
      } else if (args[i].equals("--dry-run")) {
        dryRun = true;
      } else if (args[i].equals("--shard")) {
        String[] parts = args[++i].split("/");
        shard = Integer.parseInt(parts[0]);
        shards = Integer.parseInt(parts[1]);
      } else {
        files.add(Path.of(args[i]));
      }
    }
    List<BenchConfig> all = new ArrayList<>();
    for (Path file : files) {
      all.addAll(BenchConfig.expand(BenchConfig.MAPPER.readTree(file.toFile())));
    }
    // A shard runs every shards-th run, from its index: shards interleave over each file's axes
    List<BenchConfig> runs = new ArrayList<>();
    for (int i = shard; i < all.size(); i += shards) {
      runs.add(all.get(i));
    }
    System.out.printf("%d runs%n", runs.size());
    int n = 0;
    for (BenchConfig config : runs) {
      n++;
      String name = config.string("label") + "-" + config.hash();
      if (dryRun) {
        System.out.printf("[%d/%d] %s %s%n", n, runs.size(), name, config.canonical());
        continue;
      }
      Instant started = Instant.now();
      Path dir = out.resolve(name).resolve(STAMP.format(started));
      System.out.printf("[%d/%d] %s -> %s%n", n, runs.size(), name, dir);
      Map<String, Object> manifest = new LinkedHashMap<>();
      manifest.put("label", config.string("label"));
      manifest.put("config_hash", config.hash());
      manifest.put("config", config.asMap());
      manifest.put("started", started.toString());
      manifest.put("versions", versions());
      manifest.put("host", host());
      Map<String, Object> summary;
      try {
        summary = new ContentionRun(config, dir).run();
      } catch (Exception e) {
        manifest.put("failed", e.toString());
        write(dir, manifest);
        throw e;
      }
      Instant finished = Instant.now();
      manifest.put("summary", summary);
      manifest.put("finished", finished.toString());
      manifest.put("elapsed_s", (finished.toEpochMilli() - started.toEpochMilli()) / 1000.0);
      write(dir, manifest);
      System.gc();
    }
  }

  private static void write(Path dir, Map<String, Object> manifest) throws Exception {
    Files.createDirectories(dir);
    BenchConfig.MAPPER.writerWithDefaultPrettyPrinter()
        .writeValue(dir.resolve("manifest.json").toFile(), manifest);
  }

  private static Map<String, Object> versions() {
    Map<String, Object> versions = new LinkedHashMap<>();
    versions.put("fileio_git", System.getenv().getOrDefault("FILEIO_GIT", "unknown"));
    versions.put("fileio_dirty", Boolean.parseBoolean(System.getenv("FILEIO_DIRTY")));
    versions.put("iceberg", IcebergBuild.fullVersion());
    versions.put("java", System.getProperty("java.version"));
    versions.put("jvm_args", ManagementFactory.getRuntimeMXBean().getInputArguments());
    return versions;
  }

  private static Map<String, Object> host() throws Exception {
    Map<String, Object> host = new LinkedHashMap<>();
    host.put("name", InetAddress.getLocalHost().getHostName());
    host.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version"));
    host.put("cpus", Runtime.getRuntime().availableProcessors());
    host.put("max_heap_bytes", Runtime.getRuntime().maxMemory());
    return host;
  }
}
