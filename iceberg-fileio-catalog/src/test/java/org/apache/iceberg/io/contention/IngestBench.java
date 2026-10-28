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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The ingest benchmark: runs each configuration ({@link IngestRun#DEFAULTS}) of the files it is
 * given. A configuration's {@code capture} is a contention configuration ({@link BenchConfig}); its
 * log is captured once, by the contention driver with no store latency ({@code store} none) and
 * {@code capture_log}, under {@code captures/<label>-<hash>/} of the output directory, and reused
 * by every run that names it. Each run's results go under {@code
 * ingest/<label>-<hash>/<UTC start>-r<repetition>/}: manifest.json (the configuration, the
 * capture's, versions, host and a summary), windows.jsonl and compactions.jsonl.
 *
 * <pre>{@code
 * java -Xmx4g -Diceberg.worker.num-threads=256 -cp <test classpath> \
 *   org.apache.iceberg.io.contention.IngestBench [--out DIR] [--dry-run] config.json...
 * }</pre>
 */
public final class IngestBench {
  private IngestBench() {}

  public static void main(String[] args) throws Exception {
    Path out = Path.of("bench", "results");
    List<Path> files = new ArrayList<>();
    boolean dryRun = false;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("--out")) {
        out = Path.of(args[++i]);
      } else if (args[i].equals("--dry-run")) {
        dryRun = true;
      } else {
        files.add(Path.of(args[i]));
      }
    }
    List<BenchConfig> runs = new ArrayList<>();
    for (Path file : files) {
      runs.addAll(
          BenchConfig.expand(BenchConfig.MAPPER.readTree(file.toFile()), IngestRun.DEFAULTS));
    }
    System.out.printf("%d runs%n", runs.size());
    int n = 0;
    for (BenchConfig config : runs) {
      n++;
      BenchConfig workload = workload(config);
      String name = config.string("label") + "-" + config.hash();
      if (dryRun) {
        System.out.printf("[%d/%d] %s (capture %s-%s) %s%n", n, runs.size(), name,
            workload.string("label"), workload.hash(), config.canonical());
        continue;
      }
      Path captured = capture(out, workload);
      Instant started = Instant.now();
      Path dir = ContentionBench.newRunDir(
          out.resolve("ingest").resolve(name), ContentionBench.STAMP.format(started), config);
      System.out.printf("[%d/%d] %s -> %s%n", n, runs.size(), name, dir);
      Map<String, Object> manifest = new LinkedHashMap<>();
      manifest.put("label", config.string("label"));
      manifest.put("config_hash", config.hash());
      manifest.put("config", config.asMap());
      manifest.put("capture", Map.of(
          "dir", out.relativize(captured).toString(),
          "config_hash", workload.hash(),
          "manifest", BenchConfig.MAPPER.readValue(
              captured.resolve("manifest.json").toFile(), LinkedHashMap.class)));
      manifest.put("started", started.toString());
      manifest.put("versions", ContentionBench.versions());
      manifest.put("host", ContentionBench.host());
      Map<String, Object> summary;
      try {
        summary = new IngestRun(config, dir, captured.resolve("log.bin")).run();
      } catch (Exception e) {
        manifest.put("failed", e.toString());
        ContentionBench.write(dir, manifest);
        throw e;
      }
      Instant finished = Instant.now();
      manifest.put("summary", summary);
      manifest.put("finished", finished.toString());
      manifest.put("elapsed_s", (finished.toEpochMilli() - started.toEpochMilli()) / 1000.0);
      ContentionBench.write(dir, manifest);
      System.out.printf("  %s%n", summary);
      System.gc();
    }
  }

  /** The run's workload: its capture configuration, with no store latency, capturing its log. */
  private static BenchConfig workload(BenchConfig config) {
    ObjectNode capture = ((ObjectNode) config.node().get("capture")).deepCopy();
    capture.put("store", "none");
    capture.put("capture_log", true);
    return BenchConfig.expand(capture).get(0);
  }

  /** The directory of the workload's captured log: captured now, unless an earlier run did. */
  private static Path capture(Path out, BenchConfig workload) throws Exception {
    Path dir = out.resolve("captures").resolve(workload.string("label") + "-" + workload.hash());
    if (Files.exists(dir.resolve("log.bin")) && Files.exists(dir.resolve("manifest.json"))) {
      return dir;
    }
    System.out.printf("capturing %s -> %s%n", workload.canonical(), dir);
    Files.createDirectories(dir);
    Instant started = Instant.now();
    Map<String, Object> manifest = new LinkedHashMap<>();
    manifest.put("label", workload.string("label"));
    manifest.put("config_hash", workload.hash());
    manifest.put("config", workload.asMap());
    manifest.put("started", started.toString());
    manifest.put("versions", ContentionBench.versions());
    manifest.put("host", ContentionBench.host());
    Map<String, Object> summary = new ContentionRun(workload, dir).run();
    Instant finished = Instant.now();
    manifest.put("summary", summary);
    manifest.put("finished", finished.toString());
    manifest.put("elapsed_s", (finished.toEpochMilli() - started.toEpochMilli()) / 1000.0);
    ContentionBench.write(dir, manifest);
    System.out.printf("  captured %s%n", summary);
    return dir;
  }
}
