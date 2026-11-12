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

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import jdk.jfr.Configuration;
import jdk.jfr.Recording;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.intentlog.CommitTrace;
import org.intentlog.Replica;

/**
 * The resend path of a commit past E1's knee, alone: a catalog of {@code tables} pointer tables
 * in one object's checkpoint and {@code records} records after it, and two writers that take
 * turns committing a table's update from a replica the other just made stale. Each commit's append
 * loses, then the commit tail-reads, folds the record that beat it, adopts the replica and resends:
 * what campaign tr6y's resends after a tail read did. Prints each step's CPU from the commit trace
 * (fold: VIEW to REPLAYED; build: REPLAYED to ADOPTED) and records the loop with JFR.
 *
 * <p>Args: tables records rounds jfr-file. Run on the module's test classpath with a capped heap
 * (errata D25: {@code java -Xmx2g -cp <test classpath> org.apache.iceberg.io.FoldProfile 3000 5000
 * 1000 fold.jfr}, then {@code jfr print --events jdk.ExecutionSample} for the samples).
 */
public final class FoldProfile {
  private static final String LOCATION = "mem:///wh/catalog";

  private FoldProfile() {}

  public static void main(String[] args) throws Exception {
    int tables = Integer.parseInt(args[0]);
    int records = Integer.parseInt(args[1]);
    int rounds = Integer.parseInt(args[2]);
    Path jfr = Path.of(args[3]);

    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    io.setReadsRecordVersion(true);
    io.recordHistory(false);

    // the tables, then a compaction: a checkpoint of them
    CatalogLog setup = CatalogTestSupport.log(io, LOCATION);
    setup.bootstrap(LogCatalogProperties.DEFAULT_CONFLICT_ENCODING);
    Replica<CatalogState> replica = setup.read();
    List<TableIdentifier> ids = new ArrayList<>();
    for (int ns = 0; ns * 50 < tables; ns++) {
      Namespace namespace = Namespace.of("ns" + ns);
      CatalogMutation m = new CatalogMutation(replica).createNamespace(namespace);
      for (int t = ns * 50; t < Math.min(tables, ns * 50 + 50); t++) {
        TableIdentifier id = TableIdentifier.of(namespace, "t" + t);
        ids.add(id);
        m.createTable(id, "s3://bucket/wh/" + id + "/metadata/00000.metadata.json");
      }
      replica = m.commit(setup);
    }
    // a commit that replaces the object with a checkpoint (CAS only, fused)
    CatalogLog replacing = CatalogTestSupport.log(io, LOCATION, Map.of(
        LogCatalogProperties.MAX_APPEND_COUNT, "0",
        LogCatalogProperties.COMPACT_AFTER_COMMIT, "false"));
    new CatalogMutation(replacing.read()).createNamespace(Namespace.of("checkpointed"))
        .commit(replacing);

    // the records after it, from writers that never compact
    Map<String, String> props = Map.of(LogCatalogProperties.MAX_APPEND_COUNT, "1000000");
    CatalogLog a = CatalogTestSupport.log(io, LOCATION, props);
    CatalogLog b = CatalogTestSupport.log(io, LOCATION, props);
    Replica<CatalogState> ra = a.read();
    long version = 1;
    for (int i = 0; i < records; i++) {
      TableIdentifier id = ids.get(i % tables);
      ra = new CatalogMutation(ra).updateTable(id, location(id, version++)).commit(a);
    }
    Replica<CatalogState> rb = b.read();
    System.out.printf("setup: %d tables, %d records after the checkpoint, object %d bytes%n",
        tables, ra.intentionRecords(), io.newInputFile(LOCATION).getLength());

    List<Long> fold = new ArrayList<>();
    List<Long> build = new ArrayList<>();
    List<Long> commitCpu = new ArrayList<>();
    CommitTrace.open();
    Recording recording = new Recording(Configuration.getConfiguration("profile"));
    recording.enable("jdk.ExecutionSample").withPeriod(Duration.ofMillis(1));
    recording.start();
    long start = System.nanoTime();
    for (int i = 0; i < rounds; i++) {
      TableIdentifier x = ids.get((2 * i) % tables);
      TableIdentifier y = ids.get((2 * i + 1) % tables);
      rb = new CatalogMutation(rb).updateTable(y, location(y, version++)).commit(b);
      collect(CommitTrace.drain(), fold, build, commitCpu);
      ra = new CatalogMutation(ra).updateTable(x, location(x, version++)).commit(a);
      collect(CommitTrace.drain(), fold, build, commitCpu);
    }
    long elapsed = System.nanoTime() - start;
    recording.stop();
    recording.dump(jfr);
    CommitTrace.close();

    System.out.printf("%d commits in %.1f s; records after the checkpoint now %d%n",
        2 * rounds, elapsed / 1e9, ra.intentionRecords());
    report("fold (VIEW to REPLAYED) cpu ms", fold);
    report("build (REPLAYED to ADOPTED) cpu ms", build);
    report("commit cpu ms", commitCpu);
  }

  private static String location(TableIdentifier id, long version) {
    return "s3://bucket/wh/" + id + "/metadata/" + version + ".metadata.json";
  }

  /** Each commit read's fold and build CPU, and the commit's CPU in all. */
  private static void collect(
      List<CommitTrace.Event> events, List<Long> fold, List<Long> build, List<Long> commit) {
    long viewCpu = -1;
    long replayedCpu = -1;
    boolean commitRead = false;
    for (CommitTrace.Event e : events) {
      switch (e.kind()) {
        case READ:
          commitRead = e.b() == 1;
          viewCpu = -1;
          replayedCpu = -1;
          break;
        case VIEW:
          viewCpu = e.cpuNanos();
          break;
        case REPLAYED:
          if (commitRead && viewCpu >= 0) {
            fold.add(e.cpuNanos() - viewCpu);
            replayedCpu = e.cpuNanos();
          }
          break;
        case ADOPTED:
          if (replayedCpu >= 0) {
            build.add(e.cpuNanos() - replayedCpu);
            replayedCpu = -1;
          }
          break;
        default:
          break;
      }
    }
    if (!events.isEmpty()) {
      commit.add(events.get(events.size() - 1).cpuNanos() - events.get(0).cpuNanos());
    }
  }

  private static void report(String what, List<Long> nanos) {
    if (nanos.isEmpty()) {
      System.out.printf("%s: none%n", what);
      return;
    }
    List<Long> sorted = new ArrayList<>(nanos);
    Collections.sort(sorted);
    System.out.printf("%s: n %d, p50 %.2f, p90 %.2f, mean %.2f%n", what, sorted.size(),
        sorted.get(sorted.size() / 2) / 1e6, sorted.get(sorted.size() * 9 / 10) / 1e6,
        sorted.stream().mapToLong(Long::longValue).average().orElse(0) / 1e6);
  }
}
