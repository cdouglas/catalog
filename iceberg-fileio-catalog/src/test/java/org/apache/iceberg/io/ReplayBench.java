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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.intentlog.CommittedSet;
import org.intentlog.LogView;
import org.intentlog.Replay;
import org.intentlog.fileio.AtomicFileLog;
import org.intentlog.wire.LogEnvelope;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * How fast replay tests and applies a catalog's records, from a log held in memory.
 *
 * <p>The log is captured once per parameter set and cached under {@code target/replay-bench}: one
 * writer commits {@code commits} appends ({@code op}: {@code fast} appends, which never merge
 * manifests, or {@code merge} appends, which merge them as Iceberg's defaults say; each of {@code
 * files} files in as many of {@code partitions} partitions, round robin over {@code tables} tables;
 * every {@code expire} commits, when above 0, an expiry of all but the last {@code expire}
 * snapshots) to a catalog that appends to its object and never compacts it, and the log is the
 * object's checkpoint (taken after the tables were created) and every record after it. The
 * benchmarks then work
 * from arrays: {@link #decodeRecords} decodes the records alone, {@link #replay} decodes, tests and
 * applies them onto the checkpoint the log started from, {@link #encodeCheckpoint} and {@link
 * #decodeCheckpoint} code the final state, and {@link #decodeTable} decodes its first table's
 * inline metadata, as a table load does. Each {@code records} counter is records per second.
 *
 * <p>The others work on the log as a catalog's object holds it: a checkpoint, the {@code tail}
 * records after it that a reader has read, and the {@code fresh} records appended since. {@link
 * #read} reads the object as it was (decodes the checkpoint, replays the tail and builds the
 * state), as each commit attempt does twice; {@link #reread} reads it as it is, the fresh records
 * too, as a reader that reads afresh brings itself up to date; {@link #fold} folds the fresh
 * records onto the state the reader holds instead, as {@link org.intentlog.LogRuntime#refresh}
 * does where the log reads them apart. {@link #load} loads the first table through a {@link
 * FileIOCatalog} on the object as it was, as a client's refresh that finds a new version does (the
 * read, then the table's metadata and manifest lists made). Setup prints the sizes of the
 * checkpoint, the tail and the fresh records.
 *
 * <p>Run with the test classpath (after {@code mvn test-compile}):
 *
 * <pre>{@code
 * java -Xmx2g -cp <test classpath> org.openjdk.jmh.Main ReplayBench -p mode=ml,tm -prof gc
 * }</pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = {"-Xmx2g", "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn"})
public class ReplayBench {
  private static final String CATALOG = "mem:///wh/catalog";

  /** Table metadata storage: {@code ml} (inline TM and manifest lists), {@code tm}, {@code pointer}. */
  @Param({"ml"})
  public String mode;

  @Param({"300"})
  public int commits;

  /** The appends committed: {@code fast} or {@code merge}. */
  @Param({"fast"})
  public String op;

  @Param({"0"})
  public int expire;

  @Param({"1"})
  public int tables;

  @Param({"1"})
  public int files;

  @Param({"32"})
  public int partitions;

  @Param({LamportEncoding.NAME})
  public String encoding;

  /** Records after the checkpoint that the reader has read. */
  @Param({"100"})
  public int tail;

  /** Records appended after the tail, for {@link #reread} and {@link #fold}. */
  @Param({"1"})
  public int fresh;

  private byte[] checkpoint;
  private List<byte[]> records;
  private CatalogState finalState;
  private byte[] finalCheckpoint;
  private byte[] tailCheckpoint;
  private List<byte[]> tailRecords;
  private List<byte[]> allRecords;
  private List<byte[]> freshRecords;
  private CatalogState tailState;
  private CommittedSet tailCommitted;
  private long tailIntentions;
  private InMemoryAtomicFileIO tailIO;

  /** Records processed, reported as a rate. */
  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.OPERATIONS)
  public static class Records {
    public long records;
  }

  @Setup(Level.Trial)
  public void setup() {
    LogView log = AtomicFileLog.decode(capturedLog());
    checkpoint = log.checkpoint().orElseThrow();
    records = new ArrayList<>(log.records());
    finalState = replayAll().builder().build();
    finalCheckpoint = CatalogStateMachine.INSTANCE.encodeCheckpoint(finalState);

    // The log as an object holds it: a checkpoint, the `tail` records a reader read, and the
    // `fresh` records appended since
    int end = Math.max(0, records.size() - fresh);
    int at = Math.max(0, end - tail);
    LogEnvelope.Checkpoint envelope = LogEnvelope.decodeCheckpoint(checkpoint);
    Replay.Result<CatalogState.Builder> before =
        Replay.replayOnto(
            CatalogStateMachine.INSTANCE,
            CatalogStateMachine.INSTANCE.decodeCheckpoint(envelope.state()),
            CommittedSet.of(envelope.committed()).toBuilder(), envelope.logId(), 0,
            records.subList(0, at), null);
    tailCheckpoint =
        LogEnvelope.encodeCheckpoint(
            envelope.logId(), before.committed().build().bytes(),
            CatalogStateMachine.INSTANCE.encodeCheckpoint(before.builder().build()));
    tailRecords = new ArrayList<>(records.subList(at, end));
    freshRecords = new ArrayList<>(records.subList(end, records.size()));
    allRecords = new ArrayList<>(records.subList(at, records.size()));
    Replay.Result<CatalogState.Builder> held =
        Replay.replay(CatalogStateMachine.INSTANCE, tailCheckpoint, tailRecords, null);
    tailState = held.builder().build();
    tailCommitted = held.committed().build();
    tailIntentions = held.intentionRecords();
    tailIO = new InMemoryAtomicFileIO();
    tailIO.recordHistory(false);
    tailIO.seed(CATALOG, AtomicFileLog.encode(tailCheckpoint, tailRecords));

    Table table = catalog().loadTable(TableIdentifier.of("ns", "t0"));
    System.out.printf(
        "ReplayBench sizes: checkpoint %d B, tail %d records %d B, fresh %d records %d B;"
            + " t0 %d snapshots, %d manifests in its current list%n",
        tailCheckpoint.length, tailRecords.size(), bytes(tailRecords), freshRecords.size(),
        bytes(freshRecords), Iterables.size(table.snapshots()),
        // Only inline lists are at hand: the capture's store, which held the files, is gone
        mode.equals("ml") ? table.currentSnapshot().allManifests(table.io()).size() : -1);
  }

  private static long bytes(List<byte[]> records) {
    return records.stream().mapToLong(r -> r.length).sum();
  }

  @Benchmark
  public CatalogState read() {
    return Replay.replay(CatalogStateMachine.INSTANCE, tailCheckpoint, tailRecords, null)
        .builder()
        .build();
  }

  @Benchmark
  public CatalogState reread() {
    return Replay.replay(CatalogStateMachine.INSTANCE, tailCheckpoint, allRecords, null)
        .builder()
        .build();
  }

  @Benchmark
  public CatalogState fold() {
    return Replay.replayOnto(
            CatalogStateMachine.INSTANCE, CatalogStateMachine.INSTANCE.toBuilder(tailState),
            tailCommitted.toBuilder(), null, tailIntentions, freshRecords, null)
        .builder()
        .build();
  }

  @Benchmark
  public TableMetadata load() {
    return ((org.apache.iceberg.HasTableOperations) catalog().loadTable(
            TableIdentifier.of("ns", "t0")))
        .operations()
        .current();
  }

  /** A catalog on the object as the reader read it. */
  private FileIOCatalog catalog() {
    Map<String, String> props = new HashMap<>();
    props.put(CatalogProperties.WAREHOUSE_LOCATION, "mem:///wh");
    props.put("fileio.catalog.inline", String.valueOf(!mode.equals("pointer")));
    props.put("fileio.catalog.inline.manifests", String.valueOf(mode.equals("ml")));
    FileIOCatalog catalog = new FileIOCatalog("c", CATALOG, tailIO, new HashMap<>());
    catalog.initialize("c", props);
    return catalog;
  }

  @Benchmark
  public void decodeRecords(Records counter, Blackhole bh) {
    for (byte[] bytes : records) {
      LogEnvelope.Record record = LogEnvelope.decodeRecord(bytes);
      if (!record.isAdvanceHorizon()) {
        bh.consume(CatalogStateMachine.INSTANCE.decodeIntention(record.intention()));
      }
    }
    counter.records += records.size();
  }

  @Benchmark
  public Replay.Result<CatalogState.Builder> replay(Records counter) {
    counter.records += records.size();
    return replayAll();
  }

  @Benchmark
  public byte[] encodeCheckpoint() {
    return CatalogStateMachine.INSTANCE.encodeCheckpoint(finalState);
  }

  @Benchmark
  public CatalogState.Builder decodeCheckpoint() {
    return CatalogStateMachine.INSTANCE.decodeCheckpoint(finalCheckpoint);
  }

  @Benchmark
  public TableMetadata decodeTable() {
    return CatalogTestSupport.inlineMetadata(finalState, TableIdentifier.of("ns", "t0"));
  }

  private Replay.Result<CatalogState.Builder> replayAll() {
    LogEnvelope.Checkpoint envelope = LogEnvelope.decodeCheckpoint(checkpoint);
    return Replay.replayOnto(
        CatalogStateMachine.INSTANCE,
        CatalogStateMachine.INSTANCE.decodeCheckpoint(envelope.state()),
        CommittedSet.of(envelope.committed()).toBuilder(), envelope.logId(), 0, records, null);
  }

  /** The log for these parameters: cached, or captured and cached. */
  private byte[] capturedLog() {
    Path cache =
        Path.of("target", "replay-bench",
            String.format("%s-%s-%s-c%d-e%d-t%d-f%d-p%d.log", mode, encoding, op, commits, expire,
                tables, files, partitions));
    try {
      if (Files.exists(cache)) {
        return Files.readAllBytes(cache);
      }
      byte[] log = capture();
      Files.createDirectories(cache.getParent());
      Path tmp = Files.createTempFile(cache.getParent(), "capture", ".tmp");
      Files.write(tmp, log);
      Files.move(tmp, cache, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
      return log;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Commits the workload and returns the log: the checkpoint after setup, then every record. The
   * tables are created by a catalog that replaces its object on every commit, so the log starts
   * from a checkpoint that holds them; the workload commits through one that appends and never
   * compacts.
   */
  private byte[] capture() {
    if (!op.equals("fast") && !op.equals("merge")) {
      throw new IllegalArgumentException("Unknown op: " + op);
    }
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    io.recordHistory(false);
    Map<String, String> props = new HashMap<>();
    props.put(CatalogProperties.WAREHOUSE_LOCATION, "mem:///wh");
    props.put("fileio.catalog.inline", String.valueOf(!mode.equals("pointer")));
    props.put("fileio.catalog.inline.manifests", String.valueOf(mode.equals("ml")));
    props.put(LogCatalogProperties.MAX_APPEND_COUNT, "0");
    props.put(LogCatalogProperties.CONFLICT_ENCODING, encoding);
    props.put(LogCatalogProperties.RACE_MIN_WAIT_MS, "0");
    FileIOCatalog setup = new FileIOCatalog("c", CATALOG, io, new HashMap<>());
    setup.initialize("c", props);
    setup.createNamespace(Namespace.of("ns"));
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "cat", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("cat").build();
    for (int t = 0; t < tables; t++) {
      setup.buildTable(TableIdentifier.of("ns", "t" + t), schema).withPartitionSpec(spec).create();
    }

    props.put(LogCatalogProperties.MAX_APPEND_COUNT, String.valueOf(Integer.MAX_VALUE));
    props.put(LogCatalogProperties.MAX_APPEND_SIZE, String.valueOf(Long.MAX_VALUE));
    FileIOCatalog catalog = new FileIOCatalog("c", CATALOG, io, new HashMap<>());
    catalog.initialize("c", props);
    List<Table> handles = new ArrayList<>();
    for (int t = 0; t < tables; t++) {
      handles.add(catalog.loadTable(TableIdentifier.of("ns", "t" + t)));
    }
    for (int i = 0; i < commits; i++) {
      Table table = handles.get(i % tables);
      org.apache.iceberg.AppendFiles append =
          op.equals("fast") ? table.newFastAppend() : table.newAppend();
      for (int f = 0; f < files; f++) {
        append.appendFile(
            DataFiles.builder(spec)
                .withPath("mem:///data/f" + i + "-" + f + ".parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .withPartitionPath("cat=" + ((i * files + f) % partitions))
                .build());
      }
      append.commit();
      if (expire > 0 && (i + 1) % expire == 0) {
        table.expireSnapshots().retainLast(expire).expireOlderThan(System.currentTimeMillis())
            .commit();
      }
    }
    return read(io);
  }

  private static byte[] read(InMemoryAtomicFileIO io) {
    try (SeekableInputStream in = io.newInputFile(CATALOG).newStream()) {
      return in.readAllBytes();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
