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
package org.apache.iceberg.io.bench;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.io.UuidV7;
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
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmarks four encodings for {@code Checkpoint.committed_transaction_ids}:
 *
 * <ul>
 *   <li>D — status quo: 16 raw bytes per UUID, materialized to {@code HashSet} on decode.</li>
 *   <li>A — successive deltas (varint), descending sort, linear scan with early exit.</li>
 *   <li>B — fixed-width base+delta (uint32 packed), descending sort, binary search.</li>
 *   <li>C — indexed varint with skip offsets every 16 entries.</li>
 * </ul>
 *
 * <p>Three timestamp distributions × four sizes × encode/lookup workloads.
 * Output is the encoded size (bytes) and per-operation latencies (ns).
 *
 * <p>Run via: {@code mvn test-compile exec:java
 *   -Dexec.mainClass=org.apache.iceberg.io.bench.CommittedTxnsBench
 *   -Dexec.classpathScope=test}
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
public class CommittedTxnsBench {

  public enum Distribution {
    /** Commits within a 60-second window — typical clustered traffic. */
    CLUSTERED_60S,
    /** Commits spanning a 24-hour window — mixed traffic. */
    UNIFORM_24H,
    /** Commits spanning a 1-year window — sparse, long-lived catalogs. */
    SPARSE_1Y
  }

  @Param({"100", "1000", "10000"})
  public int n;

  @Param({"CLUSTERED_60S", "UNIFORM_24H", "SPARSE_1Y"})
  public Distribution dist;

  // Generated set under benchmark.
  UUID[] uuids;
  // Pre-encoded forms (for lookup benchmarks).
  byte[] encodedD;
  byte[] encodedA;
  byte[] encodedB;
  byte[] encodedC;
  // Pre-decoded HashSet (for D's lookup baseline).
  HashSet<UUID> hashSetD;
  // Recency-skewed query targets (Zipfian over recency rank).
  UUID[] queryRecencySkewed;
  // Uniform-random query targets.
  UUID[] queryUniform;

  @Setup(Level.Trial)
  public void setup() {
    Random rand = new Random(0xC0FFEE * (long) n + dist.ordinal());
    long now = 1_700_000_000_000L;
    long span;
    switch (dist) {
      case CLUSTERED_60S: span = 60_000L; break;
      case UNIFORM_24H:   span = 86_400_000L; break;
      case SPARSE_1Y:     span = 365L * 86_400_000L; break;
      default: throw new IllegalStateException();
    }

    uuids = new UUID[n];
    long[] timestamps = new long[n];
    for (int i = 0; i < n; i++) {
      timestamps[i] = now - (long) (rand.nextDouble() * span);
    }
    for (int i = 0; i < n; i++) {
      uuids[i] = UuidV7.newUuidV7(timestamps[i], rand);
    }

    encodedD = encodeD(uuids);
    encodedA = encodeA(uuids);
    encodedB = encodeB(uuids);
    encodedC = encodeC(uuids);

    // Decode-once HashSet baseline.
    hashSetD = new HashSet<>();
    for (UUID u : uuids) {
      hashSetD.add(u);
    }

    // Build query workloads. Recency-skewed: pick the first 10% of entries
    // (most recent in descending sort order) with high probability.
    queryRecencySkewed = new UUID[1024];
    queryUniform = new UUID[1024];
    UUID[] sortedDesc = uuids.clone();
    Arrays.sort(sortedDesc, (a, b) -> b.compareTo(a));
    for (int i = 0; i < queryRecencySkewed.length; i++) {
      // Zipfian-like: pick rank with skew toward 0.
      int rank = (int) (Math.pow(rand.nextDouble(), 4) * n);
      if (rank >= n) rank = n - 1;
      queryRecencySkewed[i] = sortedDesc[rank];
      queryUniform[i] = sortedDesc[rand.nextInt(n)];
    }
  }

  // ============================================================
  // Encode benchmarks
  // ============================================================

  @Benchmark
  public byte[] encode_D() { return encodeD(uuids); }

  @Benchmark
  public byte[] encode_A() { return encodeA(uuids); }

  @Benchmark
  public byte[] encode_B() { return encodeB(uuids); }

  @Benchmark
  public byte[] encode_C() { return encodeC(uuids); }

  // ============================================================
  // Lookup benchmarks (recency-skewed)
  // ============================================================

  @Benchmark
  public void lookup_D_recent(Blackhole bh) {
    for (UUID q : queryRecencySkewed) {
      bh.consume(hashSetD.contains(q));
    }
  }

  @Benchmark
  public void lookup_A_recent(Blackhole bh) {
    for (UUID q : queryRecencySkewed) {
      bh.consume(containsA(encodedA, q));
    }
  }

  @Benchmark
  public void lookup_B_recent(Blackhole bh) {
    for (UUID q : queryRecencySkewed) {
      bh.consume(containsB(encodedB, q));
    }
  }

  @Benchmark
  public void lookup_C_recent(Blackhole bh) {
    for (UUID q : queryRecencySkewed) {
      bh.consume(containsC(encodedC, q));
    }
  }

  // ============================================================
  // Lookup benchmarks (uniform)
  // ============================================================

  @Benchmark
  public void lookup_D_uniform(Blackhole bh) {
    for (UUID q : queryUniform) {
      bh.consume(hashSetD.contains(q));
    }
  }

  @Benchmark
  public void lookup_A_uniform(Blackhole bh) {
    for (UUID q : queryUniform) {
      bh.consume(containsA(encodedA, q));
    }
  }

  @Benchmark
  public void lookup_B_uniform(Blackhole bh) {
    for (UUID q : queryUniform) {
      bh.consume(containsB(encodedB, q));
    }
  }

  @Benchmark
  public void lookup_C_uniform(Blackhole bh) {
    for (UUID q : queryUniform) {
      bh.consume(containsC(encodedC, q));
    }
  }

  // ============================================================
  // Encoding D — status quo (16 raw bytes per UUID, no sort).
  // ============================================================

  static byte[] encodeD(UUID[] uuids) {
    ByteBuffer buf = ByteBuffer.allocate(uuids.length * 16);
    for (UUID u : uuids) {
      buf.putLong(u.getMostSignificantBits());
      buf.putLong(u.getLeastSignificantBits());
    }
    return buf.array();
  }

  // ============================================================
  // Encoding A — successive deltas (varint), descending sort, linear scan with early exit.
  // Layout:
  //   fixed64 max_timestamp_ms
  //   varint count
  //   for each entry: varint delta_ms (gap from previous descending entry)
  //                  10 random bytes
  // ============================================================

  static byte[] encodeA(UUID[] uuids) {
    UUID[] sorted = uuids.clone();
    Arrays.sort(sorted, (a, b) -> b.compareTo(a));
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      long maxTs = sorted.length == 0 ? 0L : UuidV7.timestampMs(sorted[0]);
      writeFixed64(out, maxTs);
      writeVarint(out, sorted.length);
      long prev = maxTs;
      for (UUID u : sorted) {
        long ts = UuidV7.timestampMs(u);
        writeVarint(out, prev - ts); // descending: gap is prev - current >= 0
        prev = ts;
        out.write(UuidV7.packRandomBits(u));
      }
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static boolean containsA(byte[] data, UUID query) {
    if (!UuidV7.isV7(query)) return false;
    long queryTs = UuidV7.timestampMs(query);
    byte[] queryRand = UuidV7.packRandomBits(query);
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(data);
      long maxTs = readFixed64(in);
      if (queryTs > maxTs) return false;
      int count = (int) readVarint(in);
      long ts = maxTs;
      for (int i = 0; i < count; i++) {
        long delta = readVarint(in);
        ts -= delta;
        byte[] rand = in.readNBytes(10);
        if (ts == queryTs && Arrays.equals(rand, queryRand)) {
          return true;
        }
        if (ts < queryTs) {
          // Early exit: descending order, query's timestamp is already past.
          // Skip the rest of this and subsequent entries.
          return false;
        }
      }
      return false;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // ============================================================
  // Encoding B — fixed-width base+delta (fixed64), binary search.
  // Layout:
  //   fixed64 max_timestamp_ms
  //   fixed32 count
  //   for each entry (descending): fixed64 delta_ms (max - current)
  //                                10 random bytes
  // Total per-entry: 8 + 10 = 18 bytes (same as status quo, but
  // O(log N) lookup via binary search instead of O(1) HashSet, with
  // no in-memory materialization needed).
  // ============================================================

  static byte[] encodeB(UUID[] uuids) {
    UUID[] sorted = uuids.clone();
    Arrays.sort(sorted, (a, b) -> b.compareTo(a));
    int count = sorted.length;
    ByteBuffer buf = ByteBuffer.allocate(8 + 4 + count * 18);
    long maxTs = count == 0 ? 0L : UuidV7.timestampMs(sorted[0]);
    buf.putLong(maxTs);
    buf.putInt(count);
    for (UUID u : sorted) {
      long delta = maxTs - UuidV7.timestampMs(u);
      buf.putLong(delta);
      buf.put(UuidV7.packRandomBits(u));
    }
    return buf.array();
  }

  static boolean containsB(byte[] data, UUID query) {
    if (!UuidV7.isV7(query)) return false;
    long queryTs = UuidV7.timestampMs(query);
    byte[] queryRand = UuidV7.packRandomBits(query);
    ByteBuffer buf = ByteBuffer.wrap(data);
    long maxTs = buf.getLong();
    int count = buf.getInt();
    long queryDelta = maxTs - queryTs;
    if (queryDelta < 0) return false;
    final int entryStart = 12;
    final int entrySize = 18;
    int lo = 0, hi = count - 1;
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      int off = entryStart + mid * entrySize;
      long midDelta = buf.getLong(off);
      if (midDelta == queryDelta) {
        int walk = mid;
        while (walk >= 0) {
          int o = entryStart + walk * entrySize;
          if (buf.getLong(o) != queryDelta) break;
          if (matches(data, o + 8, queryRand)) return true;
          walk--;
        }
        walk = mid + 1;
        while (walk < count) {
          int o = entryStart + walk * entrySize;
          if (buf.getLong(o) != queryDelta) break;
          if (matches(data, o + 8, queryRand)) return true;
          walk++;
        }
        return false;
      } else if (midDelta < queryDelta) {
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return false;
  }

  static boolean matches(byte[] data, int off, byte[] rand) {
    for (int i = 0; i < 10; i++) {
      if (data[off + i] != rand[i]) return false;
    }
    return true;
  }

  // ============================================================
  // Encoding C — varint with skip-list. Every K entries, record both the
  // byte offset of the bucket's first entry AND the cumulative delta to
  // the start of that bucket. Lookup: binary-search skip table on
  // cumulative delta, then linear-scan within the bucket (≤K entries).
  //
  // Layout:
  //   fixed64 max_timestamp_ms
  //   varint count
  //   varint entries_len
  //   entries: per entry varint delta_ms + 10 random bytes (descending sort)
  //   fixed32 n_skips
  //   per skip: fixed32 byte_offset, fixed64 cumulative_delta_to_bucket_start
  // ============================================================

  static final int SKIP_INTERVAL = 16;

  static byte[] encodeC(UUID[] uuids) {
    UUID[] sorted = uuids.clone();
    Arrays.sort(sorted, (a, b) -> b.compareTo(a));
    try {
      int count = sorted.length;
      ByteArrayOutputStream entriesBuf = new ByteArrayOutputStream();
      long maxTs = count == 0 ? 0L : UuidV7.timestampMs(sorted[0]);
      int nSkips = (count + SKIP_INTERVAL - 1) / SKIP_INTERVAL;
      int[] skipOffsets = new int[nSkips];
      long[] skipCumulatives = new long[nSkips];
      long cumulative = 0;
      long prev = maxTs;
      for (int i = 0; i < count; i++) {
        if (i % SKIP_INTERVAL == 0) {
          skipOffsets[i / SKIP_INTERVAL] = entriesBuf.size();
          skipCumulatives[i / SKIP_INTERVAL] = cumulative;
        }
        UUID u = sorted[i];
        long ts = UuidV7.timestampMs(u);
        long delta = prev - ts;
        cumulative += delta;
        writeVarint(entriesBuf, delta);
        prev = ts;
        entriesBuf.write(UuidV7.packRandomBits(u));
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      writeFixed64(out, maxTs);
      writeVarint(out, count);
      byte[] entryBytes = entriesBuf.toByteArray();
      writeVarint(out, entryBytes.length);
      out.write(entryBytes);
      writeFixed32(out, nSkips);
      for (int i = 0; i < nSkips; i++) {
        writeFixed32(out, skipOffsets[i]);
        writeFixed64(out, skipCumulatives[i]);
      }
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static boolean containsC(byte[] data, UUID query) {
    if (!UuidV7.isV7(query)) return false;
    long queryTs = UuidV7.timestampMs(query);
    byte[] queryRand = UuidV7.packRandomBits(query);
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(data);
      long maxTs = readFixed64(in);
      if (queryTs > maxTs) return false;
      int count = (int) readVarint(in);
      int entriesLen = (int) readVarint(in);
      int entryStart = data.length - in.available();
      in.skipNBytes(entriesLen);
      int nSkips = readFixed32(in);
      int[] skipOffsets = new int[nSkips];
      long[] skipCumulatives = new long[nSkips];
      for (int i = 0; i < nSkips; i++) {
        skipOffsets[i] = readFixed32(in);
        skipCumulatives[i] = readFixed64(in);
      }
      long queryDelta = maxTs - queryTs;
      // Binary search for the largest bucket b with skipCumulatives[b] <= queryDelta.
      int lo = 0, hi = nSkips - 1, bucket = 0;
      while (lo <= hi) {
        int mid = (lo + hi) >>> 1;
        if (skipCumulatives[mid] <= queryDelta) {
          bucket = mid;
          lo = mid + 1;
        } else {
          hi = mid - 1;
        }
      }
      // Scan within bucket: entries [bucket*K, min((bucket+1)*K, count)).
      ByteArrayInputStream scan = new ByteArrayInputStream(
          data, entryStart + skipOffsets[bucket],
          entriesLen - skipOffsets[bucket]);
      long ts = maxTs - skipCumulatives[bucket];
      int end = Math.min((bucket + 1) * SKIP_INTERVAL, count);
      for (int i = bucket * SKIP_INTERVAL; i < end; i++) {
        long delta = readVarint(scan);
        ts -= delta;
        byte[] rand = scan.readNBytes(10);
        if (ts == queryTs && Arrays.equals(rand, queryRand)) return true;
        if (ts < queryTs) return false;
      }
      return false;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // ============================================================
  // Wire helpers
  // ============================================================

  static void writeFixed64(OutputStream out, long v) throws IOException {
    for (int i = 0; i < 8; i++) {
      out.write((int) ((v >>> (i * 8)) & 0xFF));
    }
  }

  static long readFixed64(InputStream in) throws IOException {
    long v = 0;
    for (int i = 0; i < 8; i++) {
      int b = in.read();
      v |= ((long) (b & 0xFF)) << (i * 8);
    }
    return v;
  }

  static void writeFixed32(OutputStream out, int v) throws IOException {
    for (int i = 0; i < 4; i++) {
      out.write((v >>> (i * 8)) & 0xFF);
    }
  }

  static int readFixed32(InputStream in) throws IOException {
    int v = 0;
    for (int i = 0; i < 4; i++) {
      int b = in.read();
      v |= (b & 0xFF) << (i * 8);
    }
    return v;
  }

  static void writeVarint(OutputStream out, long v) throws IOException {
    while ((v & ~0x7FL) != 0) {
      out.write((int) ((v & 0x7F) | 0x80));
      v >>>= 7;
    }
    out.write((int) v);
  }

  static long readVarint(InputStream in) throws IOException {
    long v = 0;
    int shift = 0;
    int b;
    do {
      b = in.read();
      if (b < 0) throw new IOException("EOF reading varint");
      v |= ((long) (b & 0x7F)) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    return v;
  }

  // ============================================================
  // Runner: prints a markdown table of encoded sizes and benchmark medians.
  // ============================================================

  public static void main(String[] args) throws Exception {
    CommandLineOptions cli = new CommandLineOptions(args);
    Options opt = new OptionsBuilder()
        .parent(cli)
        .include(CommittedTxnsBench.class.getSimpleName())
        .build();
    java.util.Collection<RunResult> results = new Runner(opt).run();

    System.out.println();
    System.out.println("## Encoded sizes (bytes)");
    System.out.println();
    System.out.println("| n | distribution | D (bytes/UUID, total) | A (b/UUID, total) | B (b/UUID, total) | C (b/UUID, total) |");
    System.out.println("|---|---|---|---|---|---|");
    for (Distribution d : Distribution.values()) {
      for (int n : new int[]{100, 1000, 10000}) {
        // Build a representative dataset and dump encoded sizes.
        Random rand = new Random(0xC0FFEE * (long) n + d.ordinal());
        long now = 1_700_000_000_000L;
        long span;
        switch (d) {
          case CLUSTERED_60S: span = 60_000L; break;
          case UNIFORM_24H:   span = 86_400_000L; break;
          case SPARSE_1Y:     span = 365L * 86_400_000L; break;
          default: throw new IllegalStateException();
        }
        UUID[] uuids = new UUID[n];
        for (int i = 0; i < n; i++) {
          long ts = now - (long) (rand.nextDouble() * span);
          uuids[i] = UuidV7.newUuidV7(ts, rand);
        }
        int dSize = encodeD(uuids).length;
        int aSize = encodeA(uuids).length;
        int bSize = encodeB(uuids).length;
        int cSize = encodeC(uuids).length;
        System.out.printf("| %d | %s | %.1f, %d | %.1f, %d | %.1f, %d | %.1f, %d |%n",
            n, d, dSize / (double) n, dSize,
            aSize / (double) n, aSize,
            bSize / (double) n, bSize,
            cSize / (double) n, cSize);
      }
    }

    System.out.println();
    System.out.println("## Benchmark results (avg ns/op)");
    System.out.println();
    for (RunResult rr : results) {
      Result<?> primary = rr.getPrimaryResult();
      String label = rr.getParams().getBenchmark()
          .replace("org.apache.iceberg.io.bench.CommittedTxnsBench.", "");
      String n = rr.getParams().getParam("n");
      String dist = rr.getParams().getParam("dist");
      System.out.printf("%-20s n=%-6s dist=%-15s  %.1f %s%n",
          label, n, dist, primary.getScore(), primary.getScoreUnit());
    }
  }
}
