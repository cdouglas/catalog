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
package org.intentlog.bench;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.intentlog.CommittedSet;
import org.intentlog.wire.CommittedSetCodec;
import org.intentlog.wire.UuidV7;
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

/**
 * {@link CommittedSet} on the paths a log takes, at a steady state's size: a set of {@code size}
 * ids spread over the retention (30 minutes) at a constant rate, decoded from a checkpoint.
 * Uses only the public API, so the same source measures any encoding of the set.
 *
 * <ul>
 *   <li>{@code decode}: a fresh read: decode the body and look up the newest id.
 *   <li>{@code fold}: replay folds a batch of {@code batch} new ids onto the decoded set.
 *   <li>{@code foldAndEncode}: that, then the checkpoint a compaction writes.
 *   <li>{@code gcAndEncode}: a GC record that drops the oldest {@code batch} ms worth, then the
 *       checkpoint.
 *   <li>{@code lookupRecent}, {@code lookupOld}, {@code lookupAbsent}: a writer's tri-state lookup
 *       of an id near the newest, near the oldest, and one never committed.
 * </ul>
 *
 * <p>Run: {@code java -cp <intentlog-core test classpath> org.openjdk.jmh.Main CommittedSetBench}
 * with a capped heap; no {@code intentlog.committed.block.ids}, so blocks are the format's.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgs = {"-Xmx2g"})
public class CommittedSetBench {

  private static final long RETENTION_MS = 30 * 60 * 1000L;

  /** Ids in the set: 100 a second (180k) and 1,000 a second (1.8M) over 30 minutes. */
  @Param({"180000", "1800000"})
  public int size;

  /** Ids a fold adds; ms a GC drops. */
  @Param({"64"})
  public int batch;

  private byte[] body;
  private List<UUID> fresh;
  private long gcWatermark;
  private UUID recent;
  private UUID old;
  private UUID absent;
  private CommittedSet decoded;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    Random random = new Random(size);
    long now = 1_700_000_000_000L;
    double perMs = size / (double) RETENTION_MS;
    List<UUID> ids = new ArrayList<>(size);
    long oldest = now - RETENTION_MS;
    for (int i = 0; i < size; i++) {
      long ts = oldest + (long) (i / perMs);
      ids.add(UuidV7.newUuidV7(ts, random));
    }
    body = CommittedSetCodec.encode(ids, oldest - 1);
    recent = ids.get(size - 1 - random.nextInt(Math.max(1, size / 1000)));
    old = ids.get(random.nextInt(Math.max(1, size / 1000)));
    absent = UuidV7.newUuidV7(UuidV7.timestampMs(ids.get(size / 2)), random);
    fresh = new ArrayList<>(batch);
    for (int i = 0; i < batch; i++) {
      fresh.add(UuidV7.newUuidV7(now + 1 + i, random));
    }
    gcWatermark = oldest + batch;
    decoded = CommittedSet.of(body);
  }

  @Benchmark
  public boolean decode() {
    return CommittedSet.of(body).contains(recent);
  }

  @Benchmark
  public CommittedSet fold() {
    CommittedSet.Builder builder = CommittedSet.of(body).toBuilder();
    for (UUID id : fresh) {
      builder.add(id);
    }
    return builder.build();
  }

  @Benchmark
  public byte[] foldAndEncode() {
    return fold().bytes();
  }

  @Benchmark
  public byte[] gcAndEncode() {
    return CommittedSet.of(body).toBuilder().advanceHorizon(gcWatermark).build().bytes();
  }

  @Benchmark
  public boolean lookupRecent() {
    return decoded.contains(recent);
  }

  @Benchmark
  public boolean lookupOld() {
    return decoded.contains(old);
  }

  @Benchmark
  public boolean lookupAbsent() {
    return decoded.contains(absent);
  }
}
