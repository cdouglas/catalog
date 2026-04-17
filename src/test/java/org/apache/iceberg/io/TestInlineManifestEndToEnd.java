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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests for inline manifest list support. Verifies that commits
 * through FileIOCatalog with inline.manifests=true produce InlineSnapshot
 * instances (no snap-*.avro files) and that manifest lists survive catalog
 * reload.
 *
 * <p>Uses a memory-backed FileIO that tracks all written files, enabling
 * assertions about which files were and were not created.
 */
public class TestInlineManifestEndToEnd {

  /**
   * In-memory FileIO supporting multiple named files with atomic operations.
   * Tracks all file paths that have been written for test assertions.
   */
  static class MemoryFileIO implements SupportsAtomicOperations {
    final ConcurrentHashMap<String, byte[]> files = new ConcurrentHashMap<>();

    @Override
    public InputFile newInputFile(String location) {
      return new InputFile() {
        @Override
        public long getLength() {
          byte[] b = files.get(location);
          return b != null ? b.length : 0;
        }

        @Override
        public SeekableInputStream newStream() {
          byte[] b = files.get(location);
          if (b == null) {
            throw new RuntimeException("File not found: " + location);
          }
          return new InMemoryInputFile(location, b).newStream();
        }

        @Override
        public String location() {
          return location;
        }

        @Override
        public boolean exists() {
          return files.containsKey(location);
        }
      };
    }

    @Override
    public OutputFile newOutputFile(String location) {
      MemoryFileIO self = this;
      return new OutputFile() {
        @Override
        public PositionOutputStream create() {
          return new MemoryOutputStream(location, files);
        }

        @Override
        public PositionOutputStream createOrOverwrite() {
          return create();
        }

        @Override
        public String location() {
          return location;
        }

        @Override
        public InputFile toInputFile() {
          return self.newInputFile(location);
        }
      };
    }

    @Override
    public void deleteFile(String location) {
      files.remove(location);
    }

    @Override
    public AtomicOutputFile newOutputFile(InputFile replace) {
      String location = replace.location();
      MemoryFileIO self = this;
      return new AtomicOutputFile() {
        private AtomicOutputFile.Strategy lastStrategy;

        @Override
        public CAS prepare(Supplier<InputStream> content, AtomicOutputFile.Strategy strategy) {
          this.lastStrategy = strategy;
          return new CAS() {
            @Override public long contentLength() { return 0; }
            @Override public byte[] contentChecksumBytes() { return new byte[0]; }
            @Override public String contentHeaderString() { return ""; }
          };
        }

        @Override
        public InputFile writeAtomic(CAS cas, Supplier<InputStream> content) throws IOException {
          byte[] data = readAll(content.get());
          if (lastStrategy == Strategy.CAS) {
            files.put(location, data);
          } else if (lastStrategy == Strategy.APPEND) {
            files.merge(location, data, (old, added) -> {
              byte[] merged = new byte[old.length + added.length];
              System.arraycopy(old, 0, merged, 0, old.length);
              System.arraycopy(added, 0, merged, old.length, added.length);
              return merged;
            });
          }
          return self.newInputFile(location);
        }

        // OutputFile methods (AtomicOutputFile extends OutputFile)
        @Override public PositionOutputStream create() {
          return new MemoryOutputStream(location, files);
        }
        @Override public PositionOutputStream createOrOverwrite() { return create(); }
        @Override public String location() { return location; }
        @Override public InputFile toInputFile() { return self.newInputFile(location); }
      };
    }

    boolean hasFile(String location) {
      return files.containsKey(location);
    }

    /** Returns all file paths that contain the given substring. */
    List<String> filesMatching(String substring) {
      return files.keySet().stream()
          .filter(k -> k.contains(substring))
          .sorted()
          .collect(java.util.stream.Collectors.toList());
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

  /** PositionOutputStream backed by a byte array, stored to the file map on close. */
  static class MemoryOutputStream extends PositionOutputStream {
    private final String location;
    private final ConcurrentHashMap<String, byte[]> files;
    private final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    private long pos = 0;

    MemoryOutputStream(String location, ConcurrentHashMap<String, byte[]> files) {
      this.location = location;
      this.files = files;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void write(int b) {
      buf.write(b);
      pos++;
    }

    @Override
    public void write(byte[] b, int off, int len) {
      buf.write(b, off, len);
      pos += len;
    }

    @Override
    public void close() throws IOException {
      super.close();
      files.put(location, buf.toByteArray());
    }
  }

  // ============================================================
  // Inline config enum
  // ============================================================

  enum InlineConfig {
    BASELINE(false, false),
    TM_ONLY(true, false),
    TM_ML(true, true);

    final boolean inlineTM;
    final boolean inlineML;

    InlineConfig(boolean inlineTM, boolean inlineML) {
      this.inlineTM = inlineTM;
      this.inlineML = inlineML;
    }

    Map<String, String> catalogProperties(String warehouseLocation) {
      Map<String, String> props = new HashMap<>();
      props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
      props.put("fileio.catalog.inline", String.valueOf(inlineTM));
      props.put("fileio.catalog.inline.manifests", String.valueOf(inlineML));
      return props;
    }
  }

  // ============================================================
  // Tests per config
  // ============================================================

  static final Schema TEST_SCHEMA = new Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "data", Types.StringType.get()));
  static final PartitionSpec TEST_SPEC = PartitionSpec.builderFor(TEST_SCHEMA)
      .bucket("data", 16).build();
  static final DataFile FILE_A = DataFiles.builder(TEST_SPEC)
      .withPath("/path/to/data-a.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("data_bucket=0")
      .withRecordCount(1)
      .build();
  static final DataFile FILE_B = DataFiles.builder(TEST_SPEC)
      .withPath("/path/to/data-b.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("data_bucket=1")
      .withRecordCount(1)
      .build();

  static final TableIdentifier TBL = TableIdentifier.of("db", "tbl");

  abstract static class ConfiguredTests {
    MemoryFileIO io;
    FileIOCatalog catalog;

    abstract InlineConfig config();

    void setUp() {
      io = new MemoryFileIO();
      String wh = "mem:///warehouse";
      Map<String, String> props = config().catalogProperties(wh);
      catalog = new FileIOCatalog(
          "test", wh + "/catalog", null, new ProtoCatalogFormat(), io, props);
      catalog.initialize("test", props);
    }

    void createNamespaceAndTable() {
      catalog.createNamespace(Namespace.of("db"));
      catalog.buildTable(TBL, TEST_SCHEMA).create();
    }

    /** Construct a fresh catalog over the same MemoryFileIO (forces checkpoint+log replay). */
    FileIOCatalog reloadCatalog() {
      String wh = "mem:///warehouse";
      Map<String, String> props = config().catalogProperties(wh);
      FileIOCatalog fresh = new FileIOCatalog(
          "test2", wh + "/catalog", null, new ProtoCatalogFormat(), io, props);
      fresh.initialize("test2", props);
      return fresh;
    }
  }

  @Nested
  class BaselineTests extends ConfiguredTests {
    @Override InlineConfig config() { return InlineConfig.BASELINE; }

    @BeforeEach void setUp() { super.setUp(); }

    @Test
    void createTableWritesMetadataFile() {
      createNamespaceAndTable();
      Table tbl = catalog.loadTable(TableIdentifier.of("db", "tbl"));
      assertThat(tbl).isNotNull();
      // In baseline mode, metadata is written to an external file
      List<String> metadataFiles = io.filesMatching("metadata.json");
      assertThat(metadataFiles).isNotEmpty();
    }
  }

  @Nested
  class TmOnlyTests extends ConfiguredTests {
    @Override InlineConfig config() { return InlineConfig.TM_ONLY; }

    @BeforeEach void setUp() { super.setUp(); }

    @Test
    void createTableInline() {
      createNamespaceAndTable();
      Table tbl = catalog.loadTable(TableIdentifier.of("db", "tbl"));
      assertThat(tbl).isNotNull();
      // In TM-only mode, no separate metadata.json should be written
      // (metadata is stored in the catalog file itself)
      List<String> metadataFiles = io.filesMatching("metadata.json");
      assertThat(metadataFiles).isEmpty();
    }
  }

  @Nested
  class TmMlTests extends ConfiguredTests {
    @Override InlineConfig config() { return InlineConfig.TM_ML; }

    @BeforeEach void setUp() { super.setUp(); }

    @Test
    void createTableInlineNoMetadataFile() {
      createNamespaceAndTable();
      Table tbl = catalog.loadTable(TableIdentifier.of("db", "tbl"));
      assertThat(tbl).isNotNull();
      List<String> metadataFiles = io.filesMatching("metadata.json");
      assertThat(metadataFiles).isEmpty();
    }

    @Test
    void tableOperationsIsManifestListSink() {
      createNamespaceAndTable();
      var ops = catalog.newTableOps(TBL);
      assertThat(ops).isInstanceOf(org.apache.iceberg.ManifestListSink.class);
    }

    /** §3.1/§3.2: FastAppend must not crash with NPE on null manifestListLocation. */
    @Test
    void fastAppendCommitsWithoutCrash() {
      createNamespaceAndTable();
      Table tbl = catalog.loadTable(TBL);
      tbl.newFastAppend().appendFile(FILE_A).commit();
      assertThat(tbl.currentSnapshot()).isNotNull();
    }

    /** §3.3/§2.4: After FastAppend + catalog reload, manifest list must survive replay. */
    @Test
    void fastAppendReloadManifests() {
      createNamespaceAndTable();
      Table tbl = catalog.loadTable(TBL);
      tbl.newFastAppend().appendFile(FILE_A).commit();

      // Reload from a fresh catalog (forces checkpoint + log replay)
      FileIOCatalog fresh = reloadCatalog();
      Table reloaded = fresh.loadTable(TBL);
      Snapshot snap = reloaded.currentSnapshot();
      assertThat(snap).isNotNull();
      List<ManifestFile> manifests = snap.allManifests(io);
      assertThat(manifests).hasSize(1);
      assertThat(manifests.get(0).addedFilesCount()).isEqualTo(1);
    }

    /** Pool carryover: two FastAppends, reload, verify both snapshots' manifest lists. */
    @Test
    void twoFastAppendsReloadManifests() {
      createNamespaceAndTable();
      Table tbl = catalog.loadTable(TBL);
      tbl.newFastAppend().appendFile(FILE_A).commit();
      long firstSnapId = tbl.currentSnapshot().snapshotId();
      tbl.newFastAppend().appendFile(FILE_B).commit();
      long secondSnapId = tbl.currentSnapshot().snapshotId();

      FileIOCatalog fresh = reloadCatalog();
      Table reloaded = fresh.loadTable(TBL);

      // Current snapshot has 2 manifests (one per append)
      Snapshot current = reloaded.currentSnapshot();
      assertThat(current.snapshotId()).isEqualTo(secondSnapId);
      assertThat(current.allManifests(io)).hasSize(2);

      // First snapshot has 1 manifest
      Snapshot first = reloaded.snapshot(firstSnapId);
      assertThat(first).isNotNull();
      assertThat(first.allManifests(io)).hasSize(1);
    }

    /** Sink path: no snap-*.avro files should be written when inline.manifests=true. */
    @Test
    void noSnapAvroFilesWritten() {
      createNamespaceAndTable();
      Table tbl = catalog.loadTable(TBL);
      tbl.newFastAppend().appendFile(FILE_A).commit();
      tbl.newFastAppend().appendFile(FILE_B).commit();

      List<String> snapFiles = io.filesMatching("snap-");
      assertThat(snapFiles).isEmpty();
    }
  }
}
