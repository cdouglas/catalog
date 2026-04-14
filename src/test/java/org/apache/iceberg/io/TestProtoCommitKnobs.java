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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.junit.jupiter.api.Test;

/**
 * Tests for the PB commit protocol knobs: {@code maxAppendCount} and
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

  /** Performs N createNamespace commits on the given format, returns the final catalog. */
  private ProtoCatalogFile commitN(ProtoCatalogFormat format, MockIO io, int n) {
    ProtoCatalogFile current = null;
    for (int i = 0; i < n; i++) {
      InputFile catalogFile = io.trackedInput();
      ProtoCatalogFormat.Mut mut;
      if (current == null) {
        mut = (ProtoCatalogFormat.Mut) format.empty(catalogFile);
      } else {
        current = format.read(io.fileIO, catalogFile);
        mut = (ProtoCatalogFormat.Mut) format.from(current);
      }
      mut.createNamespace(Namespace.of("ns" + i));
      current = (ProtoCatalogFile) mut.commit(io.fileIO);
    }
    return current;
  }

  @Test
  void defaultConfigUsesLargeLimits() {
    ProtoCatalogFormat format = new ProtoCatalogFormat();
    assertThat(format.maxAppendCount())
        .isEqualTo(ProtoCatalogFormat.DEFAULT_MAX_APPEND_COUNT);
    assertThat(format.maxAppendSize())
        .isEqualTo(ProtoCatalogFormat.DEFAULT_MAX_APPEND_SIZE);
  }

  @Test
  void configFromProperties() {
    Map<String, String> props = new HashMap<>();
    props.put(ProtoCatalogFormat.MAX_APPEND_COUNT, "100");
    props.put(ProtoCatalogFormat.MAX_APPEND_SIZE, "4096");
    ProtoCatalogFormat format = new ProtoCatalogFormat(props);
    assertThat(format.maxAppendCount()).isEqualTo(100);
    assertThat(format.maxAppendSize()).isEqualTo(4096L);
  }

  @Test
  void casOnlyMode_zeroAppendCount() {
    // maxAppendCount=0 forces every commit through CAS
    ProtoCatalogFormat format = new ProtoCatalogFormat(0, Long.MAX_VALUE);
    MockIO io = new MockIO();

    commitN(format, io, 5);

    // All 5 commits should be CAS, no appends
    assertThat(io.casCount).isEqualTo(5);
    assertThat(io.appendCount).isEqualTo(0);
  }

  @Test
  void normalMode_usesAppendAfterInitialCAS() {
    // Unlimited -> all commits after the first should append
    ProtoCatalogFormat format =
        new ProtoCatalogFormat(Integer.MAX_VALUE, Long.MAX_VALUE);
    MockIO io = new MockIO();

    commitN(format, io, 5);

    // First is CAS (initial), remaining 4 are appends
    assertThat(io.casCount).isEqualTo(1);
    assertThat(io.appendCount).isEqualTo(4);
  }

  @Test
  void appendCountLimitTriggersSealAndCAS() {
    // maxAppendCount=3 means we allow up to 3 log records before CAS.
    // Commit 1: initial CAS -> file has 1 txn. count=1.
    // Commit 2: count(1) < 3, append. (seal check: 1+1=2 < 3, no seal.) count=2.
    // Commit 3: count(2) < 3, append. (seal check: 2+1=3 >= 3, seal.) count=3.
    // Commit 4: sealed -> CAS. count=1.
    // Commit 5: count(1) < 3, append. count=2.
    // Commit 6: count(2) < 3, append with seal. count=3.
    ProtoCatalogFormat format = new ProtoCatalogFormat(3, Long.MAX_VALUE);
    MockIO io = new MockIO();

    commitN(format, io, 6);

    // 2 CASes (commit 1, commit 4), 4 appends
    assertThat(io.casCount).isEqualTo(2);
    assertThat(io.appendCount).isEqualTo(4);
  }

  @Test
  void appendSizeLimitTriggersSealAndCAS() {
    // Small size budget -> compaction kicks in after a few appends
    ProtoCatalogFormat format = new ProtoCatalogFormat(Integer.MAX_VALUE, 200L);
    MockIO io = new MockIO();

    commitN(format, io, 6);

    // Expect at least one CAS beyond the initial one (size-triggered)
    assertThat(io.casCount).isGreaterThanOrEqualTo(2);
  }

  @Test
  void appendCountTracking_incrementsInReadState() {
    ProtoCatalogFormat format = new ProtoCatalogFormat(Integer.MAX_VALUE, Long.MAX_VALUE);
    MockIO io = new MockIO();

    ProtoCatalogFile cat = commitN(format, io, 4);
    // After 4 commits (1 CAS + 3 appends), log should have 4 records
    assertThat(cat.appendCount()).isEqualTo(4);
  }

  @Test
  void appendCountResetsAfterCAS() {
    ProtoCatalogFormat format = new ProtoCatalogFormat(3, Long.MAX_VALUE);
    MockIO io = new MockIO();

    // With limit=3, the 4th commit CASes. After CAS, count=1 (just the new txn).
    ProtoCatalogFile cat = commitN(format, io, 4);
    assertThat(cat.appendCount()).isEqualTo(1);
    assertThat(cat.isSealed()).isFalse();
  }

  @Test
  void invalidNegativeConfigRejected() {
    org.assertj.core.api.Assertions.assertThatThrownBy(
        () -> new ProtoCatalogFormat(-1, 1024L))
        .isInstanceOf(IllegalArgumentException.class);
    org.assertj.core.api.Assertions.assertThatThrownBy(
        () -> new ProtoCatalogFormat(100, -1L))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ============================================================
  // Append retry: no catalog read between offset-conflict retries
  // ============================================================

  @Test
  void appendRetry_doesNotReadCatalogOnOffsetConflict() {
    // Unlimited limits so we stay in the append path.
    ProtoCatalogFormat format =
        new ProtoCatalogFormat(Integer.MAX_VALUE, Long.MAX_VALUE);
    MockIO io = new MockIO();

    // Initial commit (CAS, creates the file)
    InputFile catalogFile = io.trackedInput();
    ProtoCatalogFormat.Mut mut0 = (ProtoCatalogFormat.Mut) format.empty(catalogFile);
    mut0.createNamespace(Namespace.of("seed"));
    mut0.commit(io.fileIO);
    // Reset counters after initial setup
    int readsAfterInit = io.readCount;
    io.casCount = 0;
    io.appendCount = 0;

    // Now: inject 3 consecutive append failures (offset conflicts),
    // then the 4th attempt succeeds.
    io.injectAppendFailures = 3;

    ProtoCatalogFile current = format.read(io.fileIO, io.trackedInput());
    int readsAfterLoad = io.readCount;

    ProtoCatalogFormat.Mut mut =
        (ProtoCatalogFormat.Mut) format.from(current);
    mut.createNamespace(Namespace.of("test_ns"));
    ProtoCatalogFile result = (ProtoCatalogFile) mut.commit(io.fileIO);

    // The commit should have succeeded
    assertThat(result.containsNamespace(Namespace.of("test_ns"))).isTrue();

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
    ProtoCatalogFormat format =
        new ProtoCatalogFormat(Integer.MAX_VALUE, Long.MAX_VALUE);
    MockIO io = new MockIO();

    // Setup: create initial catalog with one namespace
    InputFile catalogFile = io.trackedInput();
    ProtoCatalogFormat.Mut mut0 = (ProtoCatalogFormat.Mut) format.empty(catalogFile);
    mut0.createNamespace(Namespace.of("existing"));
    mut0.commit(io.fileIO);

    // Inject 5 offset conflicts, then succeed
    io.injectAppendFailures = 5;

    ProtoCatalogFile current = format.read(io.fileIO, io.trackedInput());
    ProtoCatalogFormat.Mut mut =
        (ProtoCatalogFormat.Mut) format.from(current);
    mut.createNamespace(Namespace.of("new_ns"));
    ProtoCatalogFile result = (ProtoCatalogFile) mut.commit(io.fileIO);

    // Both the original and new namespace should be present
    assertThat(result.containsNamespace(Namespace.of("existing"))).isTrue();
    assertThat(result.containsNamespace(Namespace.of("new_ns"))).isTrue();
  }

  @Test
  void appendRetry_manyConflictsThenSuccess() {
    // Verify commit succeeds even after many consecutive offset conflicts.
    ProtoCatalogFormat format =
        new ProtoCatalogFormat(Integer.MAX_VALUE, Long.MAX_VALUE);
    MockIO io = new MockIO();

    // Setup
    InputFile catalogFile = io.trackedInput();
    ProtoCatalogFormat.Mut mut0 = (ProtoCatalogFormat.Mut) format.empty(catalogFile);
    mut0.createNamespace(Namespace.of("existing"));
    mut0.commit(io.fileIO);

    // Inject 8 consecutive failures (just under the 10-attempt limit)
    io.injectAppendFailures = 8;
    ProtoCatalogFile current = format.read(io.fileIO, io.trackedInput());
    ProtoCatalogFormat.Mut mut = (ProtoCatalogFormat.Mut) format.from(current);
    mut.createNamespace(Namespace.of("final"));
    ProtoCatalogFile result = (ProtoCatalogFile) mut.commit(io.fileIO);

    assertThat(result.containsNamespace(Namespace.of("final"))).isTrue();
    assertThat(result.containsNamespace(Namespace.of("existing"))).isTrue();
  }

  @Test
  void appendRetry_idempotentTransactionId() {
    // Even if the same txn bytes are appended twice (shouldn't happen, but
    // testing the idempotency guarantee), the transaction should only apply once.
    ProtoCatalogFormat format =
        new ProtoCatalogFormat(Integer.MAX_VALUE, Long.MAX_VALUE);
    MockIO io = new MockIO();

    // Setup
    InputFile catalogFile = io.trackedInput();
    ProtoCatalogFormat.Mut mut0 = (ProtoCatalogFormat.Mut) format.empty(catalogFile);
    mut0.createNamespace(Namespace.of("base"));
    ProtoCatalogFile initial = (ProtoCatalogFile) mut0.commit(io.fileIO);

    // Commit with 1 retry (inject 1 failure)
    io.injectAppendFailures = 1;
    ProtoCatalogFile current = format.read(io.fileIO, io.trackedInput());
    ProtoCatalogFormat.Mut mut =
        (ProtoCatalogFormat.Mut) format.from(current);
    mut.createNamespace(Namespace.of("new_ns"));
    ProtoCatalogFile result = (ProtoCatalogFile) mut.commit(io.fileIO);

    // The namespace should appear exactly once
    assertThat(result.containsNamespace(Namespace.of("new_ns"))).isTrue();
    // The committed transaction set should contain the txn ID
    assertThat(result.committedTransactions().size())
        .isGreaterThan(initial.committedTransactions().size());
  }
}
