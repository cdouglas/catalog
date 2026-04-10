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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
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
}
