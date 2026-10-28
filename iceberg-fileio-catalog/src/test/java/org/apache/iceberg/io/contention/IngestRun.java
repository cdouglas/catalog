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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.io.ContentionAccess;
import org.intentlog.CommittedSet;
import org.intentlog.Intention;
import org.intentlog.LogView;
import org.intentlog.Replay;
import org.intentlog.StateMachine;
import org.intentlog.fileio.AtomicFileLog;
import org.intentlog.wire.LogEnvelope;
import org.intentlog.wire.UuidV7;

/**
 * One run of the ingest benchmark: a log the contention driver captured ({@code capture_log}: every
 * record real writers' commits landed, in order) replayed by one thread as fast as it can, as an
 * ingest service that tails the log would replay it, with no storage at all. It answers how fast a
 * reader can consume the log as the catalog grows, and how fast the catalog grows in bytes.
 *
 * <p>The log's ids are retimed to {@code rate} records/s (0: as captured): a production rate sets
 * how many ids the committed set holds within its retention. Records land at the rate, evenly or as
 * a Poisson process ({@code arrivals}), and an id's time is when its commit began, by its writer's
 * clock: its landing less a lognormal lateness ({@code lateness_p50_ms}, {@code lateness_p99_ms}),
 * plus the offset of one of {@code clocks} writer clocks ({@code clock_skew_ms}, the offsets'
 * standard deviation). So ids reach the committed set out of order, as they do in production. The captured GC records are dropped;
 * the run writes its own. Each {@code batch} records are folded onto the state, which is then built
 * (published, as a tailing reader would after each read; a batch ends where a compaction is due).
 * Every {@code compact_every} records a
 * compaction, as a compacting writer's: a GC record at the last record's time less {@code
 * retention_s}, then the checkpoint encoded, measured and discarded; every {@code decode_every}-th
 * one is also decoded, as a reader that starts from it would (timed apart from the ingest).
 *
 * <p>With {@code prefill}, the committed set starts as it is at steady state for the rate: {@code
 * rate} x {@code retention_s} ids, spread over the retention window before the log's first record,
 * which the run's GC records then age out. The measured pass stops after {@code max_ingest_s} of
 * ingest time.
 *
 * <p>The first {@code warmup_records} are replayed once first (without the prefill), for the JIT.
 * A run writes
 * windows.jsonl (per {@code window} records: the time folding and compacting them took) and
 * compactions.jsonl (per compaction: the checkpoint's size and where its bytes go, the tail it
 * replaced, the time to encode and to decode it), and checks that its final state is the one a
 * replay of the log as captured reaches.
 */
final class IngestRun {

  /** Every key, with its default. */
  static final String DEFAULTS =
      "{"
          + "\"label\": \"ingest\","
          + "\"repetition\": 0,"
          // the workload: a contention configuration, run with store none and capture_log
          + "\"capture\": {},"
          // the log's production rate, records/s, its ids retimed to it (0: as captured)
          + "\"rate\": 1000,"
          // the committed set's GC window (fileio.catalog.committed.retention.ms: 30 min)
          + "\"retention_s\": 1800,"
          // when records land: uniform (one every 1/rate s) or poisson (a Poisson process)
          + "\"arrivals\": \"poisson\","
          // how long before landing a record's commit minted its id: a lognormal of this median
          // and 99th percentile (0: none). The default is the latency of a fast append with 16
          // writers on S3 Express under manifest-window (campaign c8-fold at 56facd8): an
          // operation's latency, which bounds its last commit's from above
          + "\"lateness_p50_ms\": 585,"
          + "\"lateness_p99_ms\": 2980,"
          // the writers' clocks, each offset by a normal of this standard deviation
          + "\"clocks\": 16,"
          + "\"clock_skew_ms\": 10,"
          // records between compactions (fileio.catalog.max.append.count); 0: none
          + "\"compact_every\": 100,"
          // records folded before the state is built, as one read of a tailing reader returns
          + "\"batch\": 1,"
          + "\"window\": 2000,"
          + "\"warmup_records\": 20000,"
          // compactions between decodes of the checkpoint (a cold reader); 0: none
          + "\"decode_every\": 10,"
          // start with the committed set the rate holds at steady state: rate x retention ids,
          // spread over the retention window before the log's first record
          + "\"prefill\": false,"
          // keep: the committed set as the catalog keeps it; discard: no ids kept (replay dedups
          // within a batch only) and checkpoints carry none, what the set costs: a bound for
          // keeping it out of the checkpoint and the replica
          + "\"committed\": \"keep\","
          // stop the measured pass after this much ingest time (the windows say how far it got)
          + "\"max_ingest_s\": 300"
          + "}";

  private final BenchConfig config;
  private final Path dir;
  private final Path log;

  IngestRun(BenchConfig config, Path dir, Path log) {
    this.config = config;
    this.dir = dir;
    this.log = log;
  }

  Map<String, Object> run() throws IOException {
    LogView captured = AtomicFileLog.decode(Files.readAllBytes(log));
    byte[] checkpoint = captured.checkpoint().orElseThrow();
    List<byte[]> records = retimed(captured.records(), Ids.of(config));
    return ingest(ContentionAccess.machine(), checkpoint, captured.records(), records);
  }

  private <S, B extends StateMachine.Builder<S>, I extends Intention<B>> Map<String, Object> ingest(
      StateMachine<S, B, I> machine, byte[] checkpoint, List<byte[]> original, List<byte[]> records)
      throws IOException {
    int warmup = Math.min(records.size(), config.integer("warmup_records"));
    pass(machine, checkpoint, records.subList(0, warmup), null, null, null, Long.MAX_VALUE);
    System.gc();

    CommittedSet prefill = config.bool("prefill") && !discard()
        ? prefill(UuidV7.timestampMs(LogEnvelope.decodeRecord(records.get(0)).id()),
            Ids.of(config), config.number("retention_s"), config.longValue("repetition"))
        : null;
    Pass<S> measured;
    try (BufferedWriter windows = writer("windows.jsonl");
        BufferedWriter compactions = writer("compactions.jsonl")) {
      measured = pass(machine, checkpoint, records, prefill, windows, compactions,
          (long) (config.number("max_ingest_s") * 1e9));
    }

    Map<String, Object> summary = new LinkedHashMap<>();
    summary.put("records", records.size());
    summary.put("record_bytes", records.stream().mapToLong(r -> r.length).sum());
    summary.put("ingested", measured.records);
    summary.put("ingest_s", measured.nanos / 1e9);
    summary.put("records_s", measured.records / (measured.nanos / 1e9));
    summary.put("compactions", measured.compactions);
    summary.put("prefill_ids", prefill != null ? prefill.ids().size() : 0);
    summary.put("committed", config.string("committed"));
    if (measured.records == records.size()) {
      // The same state as a replay of the log as captured: retiming and the run's own GC records
      // change the committed set, not what applies
      S whole = Replay.replay(machine, checkpoint, original, null).builder().build();
      summary.put("verified",
          Arrays.equals(machine.encodeCheckpoint(whole), machine.encodeCheckpoint(measured.state)));
    }
    return summary;
  }

  private boolean discard() {
    String committed = config.string("committed");
    if (!committed.equals("keep") && !committed.equals("discard")) {
      throw new IllegalArgumentException("committed is keep or discard, not " + committed);
    }
    return committed.equals("discard");
  }

  /**
   * How a run times its ids: records land at {@code rate} records/s, evenly or as a Poisson
   * process, and an id's time is its landing less its commit's lateness (lognormal, from its median
   * and 99th percentile; none if the median is 0), plus the offset of its writer's clock (one of
   * {@code clocks}, each a normal of standard deviation {@code skewMs}), in ms.
   */
  record Ids(double rate, boolean poisson, double latenessP50Ms, double latenessP99Ms, int clocks,
      double skewMs) {
    /** One every 1/rate s, minted as it lands: the ids in order, evenly spaced. */
    static Ids uniform(double rate) {
      return new Ids(rate, false, 0, 0, 1, 0);
    }

    static Ids of(BenchConfig config) {
      String arrivals = config.string("arrivals");
      if (!arrivals.equals("uniform") && !arrivals.equals("poisson")) {
        throw new IllegalArgumentException("arrivals is uniform or poisson, not " + arrivals);
      }
      return new Ids(config.number("rate"), arrivals.equals("poisson"),
          config.number("lateness_p50_ms"), config.number("lateness_p99_ms"),
          config.integer("clocks"), config.number("clock_skew_ms"));
    }

    /** Each record's time, relative to the first's landing, as {@link #next} draws them. */
    Clock clock(long seed) {
      return new Clock(this, new java.util.Random(seed));
    }
  }

  /** Draws the times of successive records' ids: see {@link Ids}. */
  static final class Clock {
    // Standard normal quantile of 0.99, to fit a lognormal to a median and 99th percentile
    private static final double Z99 = 2.3263478740408408;

    private final Ids ids;
    private final java.util.Random random;
    private final double[] offsets;
    private final double mu;
    private final double sigma;
    private long count = 0;
    private double landing = 0;

    private Clock(Ids ids, java.util.Random random) {
      this.ids = ids;
      this.random = random;
      this.offsets = new double[Math.max(1, ids.clocks())];
      for (int i = 0; i < offsets.length && ids.skewMs() > 0; i++) {
        offsets[i] = random.nextGaussian() * ids.skewMs();
      }
      this.mu = ids.latenessP50Ms() > 0 ? Math.log(ids.latenessP50Ms()) : 0;
      this.sigma = ids.latenessP50Ms() > 0
          ? Math.max(0, Math.log(ids.latenessP99Ms() / ids.latenessP50Ms())) / Z99
          : 0;
    }

    /** The landing time of the next record, ms after the first's. */
    double land() {
      if (ids.poisson()) {
        if (count > 0) {
          landing += -Math.log(1 - random.nextDouble()) * 1000.0 / ids.rate();
        }
      } else {
        landing = count * 1000.0 / ids.rate();
      }
      count++;
      return landing;
    }

    /** The next record's id time, ms after the first's landing. */
    double next() {
      return land() + jitter();
    }

    /** How far an id's time is from its record's landing: less its lateness, plus its skew. */
    double jitter() {
      double by = 0;
      if (ids.latenessP50Ms() > 0) {
        by -= Math.exp(mu + sigma * random.nextGaussian());
      }
      if (ids.skewMs() > 0) {
        by += offsets[random.nextInt(offsets.length)];
      }
      return by;
    }
  }

  /**
   * The committed set at steady state: the ids of the records that landed in the retention window
   * before the first record, timed by {@code ids} (seeded, so every run of a configuration has the
   * same).
   */
  static CommittedSet prefill(long first, Ids ids, double retentionS, long repetition) {
    long retentionMs = (long) (1000 * retentionS);
    java.util.Random random = new java.util.Random(repetition * 31 + 7);
    Clock clock = ids.clock(repetition * 31 + 11);
    CommittedSet set = CommittedSet.EMPTY;
    boolean more = true;
    while (more) {
      CommittedSet.Builder chunk = set.toBuilder();
      for (int i = 0; i < 1_000_000; i++) {
        double landing = clock.land();
        if (landing >= retentionMs) {
          more = false;
          break;
        }
        long at = first - retentionMs + (long) Math.floor(landing + clock.jitter());
        chunk.add(UuidV7.newUuidV7(Math.max(1L, at), random));
      }
      set = chunk.build();
    }
    return set;
  }

  private static final class Pass<S> {
    private final S state;
    private final int records;
    private final long nanos;
    private final int compactions;

    Pass(S state, int records, long nanos, int compactions) {
      this.state = state;
      this.records = records;
      this.nanos = nanos;
      this.compactions = compactions;
    }
  }

  /**
   * Replays {@code records} onto the checkpoint (its committed set merged with {@code prefill} if
   * given), writing windows and compactions if given, until {@code maxNanos} of ingest time.
   */
  private <S, B extends StateMachine.Builder<S>, I extends Intention<B>> Pass<S> pass(
      StateMachine<S, B, I> machine, byte[] checkpoint, List<byte[]> records,
      CommittedSet prefill, BufferedWriter windows, BufferedWriter compactions, long maxNanos)
      throws IOException {
    int batch = config.integer("batch");
    int compactEvery = config.integer("compact_every");
    int window = config.integer("window");
    int decodeEvery = config.integer("decode_every");
    long retentionMs = 1000L * config.longValue("retention_s");

    LogEnvelope.Checkpoint envelope = LogEnvelope.decodeCheckpoint(checkpoint);
    UUID logId = envelope.logId();
    S state = machine.decodeCheckpoint(envelope.state()).build();
    boolean discard = discard();
    CommittedSet committed = discard ? CommittedSet.EMPTY : CommittedSet.of(envelope.committed());
    if (prefill != null) {
      CommittedSet.Builder merged = prefill.toBuilder();
      committed.ids().forEach(merged::add);
      committed = merged.build();
    }
    long intentions = 0;

    long totalNanos = 0;
    int compactionCount = 0;
    int sinceCompaction = 0;
    long tailBytes = 0;
    long foldNanos = 0;
    long compactNanos = 0;
    int windowRecords = 0;
    long windowBytes = 0;
    int at = 0;
    while (at < records.size() && totalNanos < maxNanos) {
      // A batch ends where a compaction is due, so compactions come every compact_every records
      // whatever the batch
      int end = Math.min(records.size(),
          compactEvery > 0 ? Math.min(at + batch, at + compactEvery - sinceCompaction) : at + batch);
      List<byte[]> chunk = records.subList(at, end);
      long t0 = System.nanoTime();
      Replay.Result<B> result =
          Replay.replayOnto(machine, machine.toBuilder(state), committed.toBuilder(), logId,
              intentions, chunk, null);
      state = result.builder().build();
      committed = discard ? CommittedSet.EMPTY : result.committed().build();
      intentions = result.intentionRecords();
      foldNanos += System.nanoTime() - t0;

      long bytes = 0;
      for (byte[] record : chunk) {
        bytes += record.length;
      }
      windowRecords += chunk.size();
      windowBytes += bytes;
      tailBytes += bytes;
      sinceCompaction += chunk.size();
      at = end;

      if (compactEvery > 0 && sinceCompaction >= compactEvery) {
        long c0 = System.nanoTime();
        long watermark =
            UuidV7.timestampMs(LogEnvelope.decodeRecord(records.get(end - 1)).id()) - retentionMs;
        if (watermark > 0 && !discard) {
          committed = committed.toBuilder().advanceHorizon(watermark).build();
        }
        byte[] stateBytes = machine.encodeCheckpoint(state);
        byte[] encoded = LogEnvelope.encodeCheckpoint(logId, committed.bytes(), stateBytes);
        long encodeNanos = System.nanoTime() - c0;
        compactNanos += encodeNanos;
        compactionCount++;
        if (compactions != null) {
          Map<String, Object> row = new LinkedHashMap<>();
          row.put("position", end);
          row.put("checkpoint_bytes", encoded.length);
          row.put("committed_bytes", committed.encodedSize());
          row.put("state_bytes", stateBytes.length);
          ContentionAccess.checkpointSizes(stateBytes).forEach(row::put);
          row.put("tail_records", sinceCompaction);
          row.put("tail_bytes", tailBytes);
          row.put("encode_ms", encodeNanos / 1e6);
          if (decodeEvery > 0 && compactionCount % decodeEvery == 0) {
            long d0 = System.nanoTime();
            Replay.replay(machine, encoded, List.of(), null).builder().build();
            row.put("decode_ms", (System.nanoTime() - d0) / 1e6);
          }
          write(compactions, row);
        }
        sinceCompaction = 0;
        tailBytes = 0;
        intentions = 0;
      }

      // A window also ends when the pass's time is up, so a slow pass stops within a batch of it
      boolean timeUp = totalNanos + foldNanos + compactNanos >= maxNanos;
      if (windowRecords >= window || at == records.size() || timeUp) {
        totalNanos += foldNanos + compactNanos;
        if (windows != null) {
          Map<String, Object> row = new LinkedHashMap<>();
          row.put("position", at);
          row.put("records", windowRecords);
          row.put("record_bytes", windowBytes);
          row.put("fold_ms", foldNanos / 1e6);
          row.put("compact_ms", compactNanos / 1e6);
          row.put("elapsed_s", totalNanos / 1e9);
          row.put("records_s", windowRecords / ((foldNanos + compactNanos) / 1e9));
          row.put("committed_bytes", committed.encodedSize());
          row.put("heap_used",
              Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
          write(windows, row);
        }
        foldNanos = 0;
        compactNanos = 0;
        windowRecords = 0;
        windowBytes = 0;
      }
    }
    return new Pass<>(state, at, totalNanos, compactionCount);
  }

  /**
   * The intention records, their ids retimed by {@code ids} from the first one's time (keeping
   * each id's random bits; a copy of a record keeps its copy's id), without the GC records.
   */
  static List<byte[]> retimed(List<byte[]> original, Ids ids) {
    double rate = ids.rate();
    Clock clock = ids.clock(17);
    List<byte[]> records = new ArrayList<>(original.size());
    Map<UUID, UUID> retimed = new HashMap<>();
    long start = -1;
    for (byte[] bytes : original) {
      LogEnvelope.Record record = LogEnvelope.decodeRecord(bytes);
      if (record.isAdvanceHorizon()) {
        continue;
      }
      if (rate <= 0) {
        records.add(bytes);
        continue;
      }
      if (start < 0) {
        start = UuidV7.timestampMs(record.id());
      }
      UUID id = retimed.get(record.id());
      if (id == null) {
        long at = Math.max(1L, start + (long) Math.floor(clock.next()));
        id = UuidV7.fromTimestampAndRandom(at, UuidV7.packRandomBits(record.id()), 0);
        retimed.put(record.id(), id);
      }
      records.add(LogEnvelope.encodeRecord(id, record.intention()));
    }
    return records;
  }

  private BufferedWriter writer(String name) throws IOException {
    return Files.newBufferedWriter(dir.resolve(name), StandardCharsets.UTF_8);
  }

  private static void write(BufferedWriter out, Map<String, Object> row) throws IOException {
    out.write(BenchConfig.MAPPER.writeValueAsString(row));
    out.newLine();
  }
}
