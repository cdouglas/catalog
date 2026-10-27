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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.intentlog.CompactionPolicy;
import org.intentlog.DedupHorizonExceededException;
import org.intentlog.Replica;
import org.intentlog.wire.UuidV7;
import org.junit.jupiter.api.Test;

/**
 * Tests for the commit protocol knobs: {@code maxAppendCount} and
 * {@code maxAppendSize}. Verifies that each knob independently triggers
 * compaction, and that {@code maxAppendCount=0} forces CAS on every commit
 * (for storage providers without append support).
 */
public class TestProtoCommitKnobs {

  private static final String CATALOG_LOC = "memory://catalog";

  /**
   * Mock FileIO harness: tracks CAS vs APPEND strategy selection per call and
   * serves content from a shared AtomicReference. Uses an InMemoryInputFile for
   * the actual byte-level read path.
   */
  static class MockIO {
    final SupportsAtomicOperations fileIO;
    final AtomicReference<byte[]> content = new AtomicReference<>();
    int casCount = 0;
    int appendCount = 0;
    int readCount = 0;   // number of newStream() calls (proxy for catalog reads)

    // When > 0, the next N appends throw AppendException (simulating concurrent writers).
    // Each injected failure also simulates the concurrent writer's append by growing the file.
    int injectAppendFailures = 0;

    @SuppressWarnings("unchecked")
    MockIO() {
      fileIO = mock(SupportsAtomicOperations.class);
      // Mockito returns false for unstubbed boolean methods, but the log
      // interprets false as "no append primitive" and forces CAS-only. These
      // knob tests exercise the append branch, so opt back into the interface
      // default.
      when(fileIO.supportsAppend()).thenReturn(true);
      when(fileIO.newInputFile(any(String.class))).thenAnswer(inv -> trackedInput());
      when(fileIO.newOutputFile(any(InputFile.class))).thenAnswer(inv -> {
        AtomicOutputFile out = mock(AtomicOutputFile.class);
        AtomicReference<AtomicOutputFile.Strategy> lastStrategy = new AtomicReference<>();

        when(out.prepare(any(), any(AtomicOutputFile.Strategy.class))).thenAnswer(prepareInv -> {
          lastStrategy.set(prepareInv.getArgument(1));
          return mock(CAS.class);
        });

        when(out.writeAtomic(any(CAS.class), any())).thenAnswer(writeInv -> {
          Supplier<InputStream> supplier = writeInv.getArgument(1);
          byte[] written = readAll(supplier.get());

          AtomicOutputFile.Strategy strategy = lastStrategy.get();
          if (strategy == AtomicOutputFile.Strategy.CAS) {
            casCount++;
            content.set(written);
          } else if (strategy == AtomicOutputFile.Strategy.APPEND) {
            if (injectAppendFailures > 0) {
              injectAppendFailures--;
              // Simulate a concurrent writer's append that won the offset race:
              // grow the file with dummy bytes so length increases.
              byte[] existing = content.get();
              if (existing != null) {
                byte[] grown = new byte[existing.length + written.length];
                System.arraycopy(existing, 0, grown, 0, existing.length);
                // Fill with the concurrent writer's data (we just use our bytes
                // as a stand-in; content doesn't matter for offset-conflict tests)
                System.arraycopy(written, 0, grown, existing.length, written.length);
                content.set(grown);
              }
              throw new SupportsAtomicOperations.AppendException(
                  "injected offset conflict", new IOException("offset mismatch"));
            }
            appendCount++;
            byte[] existing = content.get();
            byte[] merged;
            if (existing == null) {
              merged = written;
            } else {
              merged = new byte[existing.length + written.length];
              System.arraycopy(existing, 0, merged, 0, existing.length);
              System.arraycopy(written, 0, merged, existing.length, written.length);
            }
            content.set(merged);
          }
          return trackedInput();
        });

        return out;
      });
    }

    InputFile trackedInput() {
      return new InputFile() {
        @Override
        public long getLength() {
          byte[] b = content.get();
          return b != null ? b.length : 0;
        }

        @Override
        public SeekableInputStream newStream() {
          readCount++;
          byte[] b = content.get();
          if (b == null) {
            throw new RuntimeException("File does not exist");
          }
          return new InMemoryInputFile(CATALOG_LOC, b).newStream();
        }

        @Override
        public String location() {
          return CATALOG_LOC;
        }

        @Override
        public boolean exists() {
          return content.get() != null;
        }
      };
    }

    private static byte[] readAll(InputStream is) throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buf = new byte[4096];
      int n;
      while ((n = is.read(buf)) > 0) {
        out.write(buf, 0, n);
      }
      is.close();
      return out.toByteArray();
    }
  }

  /** The catalog at {@link #CATALOG_LOC} of {@code io}, with these commit knobs. */
  private static CatalogLog log(MockIO io, int maxAppendCount, long maxAppendSize) {
    return CatalogTestSupport.log(io.fileIO, CATALOG_LOC, maxAppendCount, maxAppendSize);
  }

  /** Performs N createNamespace commits on the given log, returns the final catalog. */
  private Replica<CatalogState> commitN(CatalogLog log, int n) {
    Replica<CatalogState> current = null;
    for (int i = 0; i < n; i++) {
      Replica<CatalogState> base = current == null ? log.empty() : log.read();
      current = new CatalogMutation(base).createNamespace(Namespace.of("ns" + i)).commit(log);
    }
    return current;
  }

  /** Asserts that {@code policy} compacts at {@code maxRecords} records or {@code maxBytes}. */
  private static void assertLimits(CompactionPolicy policy, int maxRecords, long maxBytes) {
    assertThat(policy.due(maxRecords - 1, 0, 0)).isFalse();
    assertThat(policy.due(maxRecords, 0, 0)).isTrue();
    assertThat(policy.due(0, maxBytes - 1, 1)).isFalse();
    assertThat(policy.due(0, maxBytes, 1)).isTrue();
  }

  @Test
  void defaultConfigUsesLargeLimits() {
    assertLimits(
        LogCatalogProperties.runtimeConfig(Map.of()).compaction(),
        LogCatalogProperties.DEFAULT_MAX_APPEND_COUNT,
        LogCatalogProperties.DEFAULT_MAX_APPEND_SIZE);
  }

  @Test
  void configFromProperties() {
    Map<String, String> props = new HashMap<>();
    props.put(LogCatalogProperties.MAX_APPEND_COUNT, "100");
    props.put(LogCatalogProperties.MAX_APPEND_SIZE, "4096");
    assertLimits(LogCatalogProperties.runtimeConfig(props).compaction(), 100, 4096L);
  }

  @Test
  void casOnlyMode_zeroAppendCount() {
    // maxAppendCount=0 forces every commit through CAS
    MockIO io = new MockIO();
    CatalogLog log = log(io, 0, Long.MAX_VALUE);

    commitN(log, 5);

    // All 5 commits should be CAS, no appends
    assertThat(io.casCount).isEqualTo(5);
    assertThat(io.appendCount).isEqualTo(0);
  }

  @Test
  void normalMode_usesAppendAfterInitialCAS() {
    // Unlimited -> all commits after the first should append
    MockIO io = new MockIO();
    CatalogLog log = log(io, Integer.MAX_VALUE, Long.MAX_VALUE);

    commitN(log, 5);

    // First is CAS (initial), remaining 4 are appends
    assertThat(io.casCount).isEqualTo(1);
    assertThat(io.appendCount).isEqualTo(4);
  }

  @Test
  void appendCountLimitTriggersCAS() {
    // maxAppendCount=3 means we allow up to 3 log records before CAS.
    // Commit 1: initial CAS -> file has 1 txn. count=1.
    // Commit 2: count(1) < 3, append. count=2.
    // Commit 3: count(2) < 3, append. count=3.
    // Commit 4: count(3) >= 3, CAS. count=1.
    // Commit 5: count(1) < 3, append. count=2.
    // Commit 6: count(2) < 3, append. count=3.
    MockIO io = new MockIO();
    CatalogLog log = log(io, 3, Long.MAX_VALUE);

    commitN(log, 6);

    // 2 CASes (commit 1, commit 4), 4 appends
    assertThat(io.casCount).isEqualTo(2);
    assertThat(io.appendCount).isEqualTo(4);
  }

  @Test
  void appendSizeLimitTriggersCAS() {
    // Small size budget -> compaction kicks in after a few appends
    MockIO io = new MockIO();
    CatalogLog log = log(io, Integer.MAX_VALUE, 200L);

    commitN(log, 6);

    // Expect at least one CAS beyond the initial one (size-triggered)
    assertThat(io.casCount).isGreaterThanOrEqualTo(2);
  }

  @Test
  void appendCountTracking_incrementsInReadState() {
    MockIO io = new MockIO();
    CatalogLog log = log(io, Integer.MAX_VALUE, Long.MAX_VALUE);

    Replica<CatalogState> cat = commitN(log, 4);
    // After 4 commits (1 CAS + 3 appends), log should have 4 records
    assertThat(cat.intentionRecords()).isEqualTo(4);
  }

  @Test
  void appendCountResetsAfterCAS() {
    MockIO io = new MockIO();
    CatalogLog log = log(io, 3, Long.MAX_VALUE);

    // With limit=3, the 4th commit CASes. After CAS, count=1 (just the new txn).
    Replica<CatalogState> cat = commitN(log, 4);
    assertThat(cat.intentionRecords()).isEqualTo(1);
  }

  /**
   * The committed-txn dedup set's load-bearing use case: a writer that
   * committed transaction X reads a later checkpoint and confirms via
   * {@code containsTransaction(X)} that its commit landed (i.e., made it
   * across a CAS-mode rewrite by another writer). Negative case for an
   * uncommitted UUID confirms the scan returns false rather than spuriously
   * matching.
   *
   * <p>Exercises the full pipeline: the test writer commits → the new
   * checkpoint persists the txn UUID inside the {@code CommittedTransactions}
   * sub-message → a fresh read's committed set ({@link
   * org.intentlog.CommittedSet#contains}) scans the wire bytes directly
   * without HashSet materialization.
   */
  @Test
  void writerLearnsCommitOutcomeAfterCompaction() {
    // CAS-only mode: every commit is a full file rewrite. Equivalent to
    // "compaction happens on every commit" — strongest version of the
    // after-compaction question.
    MockIO io = new MockIO();
    CatalogLog log = log(io, 0, Long.MAX_VALUE);

    // Initial commit creates the catalog and adds a namespace. The commit
    // mints a v7 UUID; we recover it by inspecting the returned replica's
    // committed set (size == 1 here).
    CatalogMutation mut = new CatalogMutation(log.empty());
    mut.createNamespace(Namespace.of("warehouse"));
    Replica<CatalogState> afterCommit = mut.commit(log);

    assertThat(afterCommit.committed().ids())
        .as("CAS-mode commit must record exactly the one new txn UUID")
        .hasSize(1);
    UUID txnId = afterCommit.committed().ids().iterator().next();
    assertThat(UuidV7.isV7(txnId)).isTrue();
    // Sanity check: the replica the commit returned should also
    // confirm via the scan-on-bytes path.
    assertThat(afterCommit.committed().contains(txnId)).isTrue();

    // Fresh read: simulate a different writer (or the same writer at a later
    // time) loading the catalog file and querying for the original txn.
    Replica<CatalogState> fresh = log.read();

    // Confirms the commit landed in the post-CAS checkpoint and is still
    // visible via the lazy scan.
    assertThat(fresh.committed().contains(txnId))
        .as("a committed txn UUID must be findable in the post-compaction checkpoint")
        .isTrue();

    // Negative: an unrelated v7 UUID was never committed; scan must return false.
    UUID never = UuidV7.newUuidV7();
    assertThat(fresh.committed().contains(never))
        .as("an uncommitted UUID must not match")
        .isFalse();

    // After two more commits (each a CAS rewrite — i.e., two more compactions),
    // the original txn must STILL be findable: the dedup set is monotonic
    // across compactions.
    CatalogMutation mut2 = new CatalogMutation(fresh);
    mut2.createNamespace(Namespace.of("bronze"));
    Replica<CatalogState> afterSecond = mut2.commit(log);

    CatalogMutation mut3 = new CatalogMutation(afterSecond);
    mut3.createNamespace(Namespace.of("silver"));
    mut3.commit(log);

    Replica<CatalogState> afterMore = log.read();
    assertThat(afterMore.committed().contains(txnId))
        .as("the original txn must survive subsequent compactions")
        .isTrue();
    assertThat(afterMore.committed().ids()).hasSize(3);
  }

  @Test
  void invalidNegativeConfigRejected() {
    org.assertj.core.api.Assertions.assertThatThrownBy(
        () -> LogCatalogProperties.runtimeConfig(-1, 1024L, 0L, 0L, 0L))
        .isInstanceOf(IllegalArgumentException.class);
    org.assertj.core.api.Assertions.assertThatThrownBy(
        () -> LogCatalogProperties.runtimeConfig(100, -1L, 0L, 0L, 0L))
        .isInstanceOf(IllegalArgumentException.class);
    org.assertj.core.api.Assertions.assertThatThrownBy(
        () -> LogCatalogProperties.runtimeConfig(100, 1024L, -1L, 0L, 0L))
        .isInstanceOf(IllegalArgumentException.class);
    org.assertj.core.api.Assertions.assertThatThrownBy(
        () -> LogCatalogProperties.runtimeConfig(100, 1024L, 0L, -1L, 0L))
        .isInstanceOf(IllegalArgumentException.class);
    org.assertj.core.api.Assertions.assertThatThrownBy(
        () -> LogCatalogProperties.runtimeConfig(100, 1024L, 0L, 0L, -1L))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void committedRetentionConfigFromProperties() {
    Map<String, String> props = new HashMap<>();
    props.put(LogCatalogProperties.COMMITTED_RETENTION_MS, "300000");
    assertThat(LogCatalogProperties.runtimeConfig(props).committedRetentionMs())
        .isEqualTo(300_000L);
  }

  @Test
  void defaultCommittedRetentionIsSixHours() {
    assertThat(LogCatalogProperties.runtimeConfig(Map.of()).committedRetentionMs())
        .isEqualTo(LogCatalogProperties.DEFAULT_COMMITTED_RETENTION_MS)
        .isEqualTo(6L * 60 * 60 * 1000);
  }

  /**
   * Aggressive-retention end-to-end: with {@code committedRetentionMs = 0}
   * every CAS rewrite GCs all prior dedup entries. After Writer A commits
   * txn_X and Writer B follows with another CAS, the fresh read's
   * {@code containsTransaction(txn_X)} throws
   * {@link DedupHorizonExceededException} because txn_X's timestamp is at or
   * below the now-advanced horizon. The exception's {@code queryId} and
   * {@code horizonMs} are checked for diagnostic correctness.
   *
   * <p>Negative case: a freshly-minted UUID whose timestamp is above the
   * horizon and never committed returns {@code false} (presumed abort) — no
   * exception.
   */
  @Test
  void retentionZeroGcsDedupSetOnEachCompaction() throws InterruptedException {
    // CAS-only mode (so every commit is a rewrite) + retention=0 (so every
    // rewrite drops everything older than now).
    MockIO io = new MockIO();
    CatalogLog log = CatalogTestSupport.log(io.fileIO, CATALOG_LOC, 0, Long.MAX_VALUE, 0L);

    // Writer A: initial commit, mints txn_X.
    CatalogMutation mutA = new CatalogMutation(log.empty());
    mutA.createNamespace(Namespace.of("warehouse"));
    Replica<CatalogState> afterA = mutA.commit(log);
    assertThat(afterA.committed().ids()).hasSize(1);
    UUID txnX = afterA.committed().ids().iterator().next();
    long txnXTs = UuidV7.timestampMs(txnX);

    // Bump the wall clock so Writer B's watermark moves past txn_X.
    Thread.sleep(5);

    // Writer B: CAS commit, which rewrites the checkpoint with watermark =
    // now > txnXTs → drops txn_X from the dedup set, sets horizon ≈ txnXTs.
    CatalogMutation mutB = new CatalogMutation(afterA);
    mutB.createNamespace(Namespace.of("bronze"));
    mutB.commit(log);

    // Fresh read.
    Replica<CatalogState> fresh = log.read();

    // Horizon now covers txn_X.
    assertThat(fresh.committed().horizon())
        .as("horizon after retention=0 compaction must overtake txn_X")
        .isGreaterThanOrEqualTo(txnXTs);

    // Querying for txn_X is UNKNOWN — throws.
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> fresh.committed().contains(txnX))
        .isInstanceOf(DedupHorizonExceededException.class)
        .satisfies(e -> {
          DedupHorizonExceededException ex = (DedupHorizonExceededException) e;
          assertThat(ex.queryId()).isEqualTo(txnX);
          assertThat(ex.horizonMs()).isEqualTo(fresh.committed().horizon());
        });

    // Negative case: a UUID minted *after* the horizon that was never
    // committed returns false (presumed abort), no throw.
    UUID neverCommitted = UuidV7.newUuidV7();
    assertThat(UuidV7.timestampMs(neverCommitted))
        .isGreaterThan(fresh.committed().horizon());
    assertThat(fresh.committed().contains(neverCommitted)).isFalse();
  }

  /**
   * Default-retention end-to-end: with the 6-hour default, txn_X stays
   * findable across many compactions. Differs from
   * {@link #writerLearnsCommitOutcomeAfterCompaction} only in that this test
   * explicitly verifies {@code highestDroppedTimestampMs == 0} (no GC fired
   * because the watermark is hours back, before the test's commits).
   */
  @Test
  void defaultRetentionPreservesRecentCommits() {
    MockIO io = new MockIO();
    CatalogLog log = log(io, 0, Long.MAX_VALUE); // CAS-only, default retention

    CatalogMutation mut = new CatalogMutation(log.empty());
    mut.createNamespace(Namespace.of("warehouse"));
    Replica<CatalogState> afterCommit = mut.commit(log);
    UUID txnId = afterCommit.committed().ids().iterator().next();

    // Several more CAS rewrites — none of them GC anything because the
    // watermark = now - 6h is well before the test's start.
    for (int i = 0; i < 3; i++) {
      Replica<CatalogState> current = log.read();
      CatalogMutation m = new CatalogMutation(current);
      m.createNamespace(Namespace.of("ns" + i));
      m.commit(log);
    }

    Replica<CatalogState> fresh = log.read();
    assertThat(fresh.committed().contains(txnId)).isTrue();
    assertThat(fresh.committed().horizon())
        .as("default retention is far enough back that no GC pass should fire on test-time commits")
        .isZero();
  }

  // ============================================================
  // Append retry: no catalog read between offset-conflict retries
  // ============================================================

  @Test
  void appendRetry_doesNotReadCatalogOnOffsetConflict() {
    // Unlimited limits so we stay in the append path.
    MockIO io = new MockIO();
    CatalogLog log = log(io, Integer.MAX_VALUE, Long.MAX_VALUE);

    // Initial commit (CAS, creates the file)
    CatalogMutation mut0 = new CatalogMutation(log.empty());
    mut0.createNamespace(Namespace.of("seed"));
    mut0.commit(log);
    // Reset counters after initial setup
    int readsAfterInit = io.readCount;
    io.casCount = 0;
    io.appendCount = 0;

    // Now: inject 3 consecutive append failures (offset conflicts),
    // then the 4th attempt succeeds.
    io.injectAppendFailures = 3;

    Replica<CatalogState> current = log.read();
    int readsAfterLoad = io.readCount;

    CatalogMutation mut = new CatalogMutation(current);
    mut.createNamespace(Namespace.of("test_ns"));
    Replica<CatalogState> result = mut.commit(log);

    // The commit should have succeeded
    assertThat(result.state().containsNamespace(Namespace.of("test_ns"))).isTrue();

    // 3 failed appends + 1 successful append
    assertThat(io.appendCount).isEqualTo(1);  // only the successful one counts

    // Key assertion: no catalog reads between append retries.
    // The only reads should be from:
    //   1. The successful append's read-back (tryAppend reads after write)
    // The 3 failed attempts should NOT have triggered reads.
    int readsDuringCommit = io.readCount - readsAfterLoad;
    assertThat(readsDuringCommit)
        .as("should only read once (post-success validation), not on each retry")
        .isEqualTo(1);
  }

  @Test
  void appendRetry_sameBytesSucceedAfterConflict() {
    // Verify the retried append produces a valid catalog with the intended change.
    MockIO io = new MockIO();
    CatalogLog log = log(io, Integer.MAX_VALUE, Long.MAX_VALUE);

    // Setup: create initial catalog with one namespace
    CatalogMutation mut0 = new CatalogMutation(log.empty());
    mut0.createNamespace(Namespace.of("existing"));
    mut0.commit(log);

    // Inject 5 offset conflicts, then succeed
    io.injectAppendFailures = 5;

    Replica<CatalogState> current = log.read();
    CatalogMutation mut = new CatalogMutation(current);
    mut.createNamespace(Namespace.of("new_ns"));
    Replica<CatalogState> result = mut.commit(log);

    // Both the original and new namespace should be present
    assertThat(result.state().containsNamespace(Namespace.of("existing"))).isTrue();
    assertThat(result.state().containsNamespace(Namespace.of("new_ns"))).isTrue();
  }

  @Test
  void appendRetry_manyConflictsThenSuccess() {
    // Verify commit succeeds even after many consecutive offset conflicts.
    MockIO io = new MockIO();
    CatalogLog log = log(io, Integer.MAX_VALUE, Long.MAX_VALUE);

    // Setup
    CatalogMutation mut0 = new CatalogMutation(log.empty());
    mut0.createNamespace(Namespace.of("existing"));
    mut0.commit(log);

    // Inject 8 consecutive failures (just under the 10-attempt limit)
    io.injectAppendFailures = 8;
    Replica<CatalogState> current = log.read();
    CatalogMutation mut = new CatalogMutation(current);
    mut.createNamespace(Namespace.of("final"));
    Replica<CatalogState> result = mut.commit(log);

    assertThat(result.state().containsNamespace(Namespace.of("final"))).isTrue();
    assertThat(result.state().containsNamespace(Namespace.of("existing"))).isTrue();
  }

  @Test
  void appendRetry_idempotentTransactionId() {
    // Even if the same txn bytes are appended twice (shouldn't happen, but
    // testing the idempotency guarantee), the transaction should only apply once.
    MockIO io = new MockIO();
    CatalogLog log = log(io, Integer.MAX_VALUE, Long.MAX_VALUE);

    // Setup
    CatalogMutation mut0 = new CatalogMutation(log.empty());
    mut0.createNamespace(Namespace.of("base"));
    Replica<CatalogState> initial = mut0.commit(log);

    // Commit with 1 retry (inject 1 failure)
    io.injectAppendFailures = 1;
    Replica<CatalogState> current = log.read();
    CatalogMutation mut = new CatalogMutation(current);
    mut.createNamespace(Namespace.of("new_ns"));
    Replica<CatalogState> result = mut.commit(log);

    // The namespace should appear exactly once
    assertThat(result.state().containsNamespace(Namespace.of("new_ns"))).isTrue();
    // The committed transaction set should contain the txn ID
    assertThat(result.committed().ids().size())
        .isGreaterThan(initial.committed().ids().size());
  }
}
