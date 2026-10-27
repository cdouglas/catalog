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
 * writer commits {@code commits} fast appends (each of {@code files} files in as many of {@code
 * partitions} partitions, round robin over {@code tables} tables; every {@code expire} commits,
 * when above 0, an expiry of all but the last {@code expire} snapshots) to a catalog that replaces
 * its object on every commit, and the record each commit wrote is kept. The benchmarks then work
 * from arrays: {@link #decodeRecords} decodes the records alone, {@link #replay} decodes, tests and
 * applies them onto the checkpoint the log started from, {@link #encodeCheckpoint} and {@link
 * #decodeCheckpoint} code the final state, and {@link #decodeTable} decodes its first table's
 * inline metadata, as a table load does. Each {@code records} counter is records per second.
 *
 * <p>Two benchmarks work on the log as a catalog's object holds it, a checkpoint {@code tail}
 * records before the end and the records after it: {@link #read} reads it (decodes the checkpoint,
 * replays the tail and builds the state), as each commit attempt does twice; {@link #load} loads
 * the first table through a {@link FileIOCatalog} on it, as a client's refresh that finds a new
 * version does (the read, then the table's metadata and manifest lists made).
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

  /** Records after the checkpoint, for {@link #read} and {@link #load}. */
  @Param({"100"})
  public int tail;

  private byte[] checkpoint;
  private List<byte[]> records;
  private CatalogState finalState;
  private byte[] finalCheckpoint;
  private byte[] tailCheckpoint;
  private List<byte[]> tailRecords;
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

    // The log as an object holds it: a checkpoint, then the last `tail` records
    int at = Math.max(0, records.size() - tail);
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
    tailRecords = new ArrayList<>(records.subList(at, records.size()));
    tailIO = new InMemoryAtomicFileIO();
    tailIO.recordHistory(false);
    tailIO.seed(CATALOG, AtomicFileLog.encode(tailCheckpoint, tailRecords));
  }

  @Benchmark
  public CatalogState read() {
    return Replay.replay(CatalogStateMachine.INSTANCE, tailCheckpoint, tailRecords, null)
        .builder()
        .build();
  }

  @Benchmark
  public TableMetadata load() {
    Map<String, String> props = new HashMap<>();
    props.put(CatalogProperties.WAREHOUSE_LOCATION, "mem:///wh");
    props.put("fileio.catalog.inline", String.valueOf(!mode.equals("pointer")));
    props.put("fileio.catalog.inline.manifests", String.valueOf(mode.equals("ml")));
    FileIOCatalog catalog = new FileIOCatalog("c", CATALOG, tailIO, new HashMap<>());
    catalog.initialize("c", props);
    return ((org.apache.iceberg.HasTableOperations) catalog.loadTable(
            TableIdentifier.of("ns", "t0")))
        .operations()
        .current();
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
            String.format("%s-%s-c%d-e%d-t%d-f%d-p%d.log", mode, encoding, commits, expire, tables,
                files, partitions));
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

  /** Commits the workload and returns the log: the checkpoint after setup, then every record. */
  private byte[] capture() {
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    io.recordHistory(false);
    Map<String, String> props = new HashMap<>();
    props.put(CatalogProperties.WAREHOUSE_LOCATION, "mem:///wh");
    props.put("fileio.catalog.inline", String.valueOf(!mode.equals("pointer")));
    props.put("fileio.catalog.inline.manifests", String.valueOf(mode.equals("ml")));
    props.put(LogCatalogProperties.MAX_APPEND_COUNT, "0");
    props.put(LogCatalogProperties.CONFLICT_ENCODING, encoding);
    FileIOCatalog catalog = new FileIOCatalog("c", CATALOG, io, new HashMap<>());
    catalog.initialize("c", props);
    catalog.createNamespace(Namespace.of("ns"));
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "cat", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("cat").build();
    List<Table> handles = new ArrayList<>();
    for (int t = 0; t < tables; t++) {
      handles.add(
          catalog.buildTable(TableIdentifier.of("ns", "t" + t), schema).withPartitionSpec(spec)
              .create());
    }

    LogView start = AtomicFileLog.decode(read(io));
    List<byte[]> log = new ArrayList<>(start.records());
    for (int i = 0; i < commits; i++) {
      Table table = handles.get(i % tables);
      org.apache.iceberg.AppendFiles append = table.newFastAppend();
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
      log.add(lastRecord(io));
      if (expire > 0 && (i + 1) % expire == 0) {
        table.expireSnapshots().retainLast(expire).expireOlderThan(System.currentTimeMillis())
            .commit();
        log.add(lastRecord(io));
      }
    }
    return AtomicFileLog.encode(start.checkpoint().orElseThrow(), log);
  }

  private static byte[] read(InMemoryAtomicFileIO io) {
    try (SeekableInputStream in = io.newInputFile(CATALOG).newStream()) {
      return in.readAllBytes();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** The intention record the last commit wrote: the object's last. */
  private static byte[] lastRecord(InMemoryAtomicFileIO io) {
    List<byte[]> written = AtomicFileLog.decode(read(io)).records();
    byte[] last = written.get(written.size() - 1);
    if (LogEnvelope.decodeRecord(last).isAdvanceHorizon()) {
      throw new IllegalStateException("The last record is a GC record");
    }
    return last;
  }
}
