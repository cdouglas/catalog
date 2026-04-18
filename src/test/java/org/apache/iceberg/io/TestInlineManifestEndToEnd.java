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
import org.apache.iceberg.InlineSnapshot;
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

      // Verify via fresh catalog (forces full re-read from storage)
      FileIOCatalog fresh = reloadCatalog();
      Table reloaded = fresh.loadTable(TBL);
      assertThat(reloaded.snapshots()).as("table should have snapshots after FastAppend").isNotEmpty();
      assertThat(reloaded.currentSnapshot()).as("table should have a current snapshot").isNotNull();
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

    /**
     * §1.2: A catalog transaction FastAppend must preserve ML data across
     * commitTransaction. Without the commitTransaction ML integration, the
     * committed snapshot would have an empty manifest list on reload.
     *
     * <p>Caveat: commitTransaction uses BaseTransaction's TransactionTableOperations
     * wrapper, which does not forward the ManifestListSink interface. The
     * SnapshotProducer therefore takes the Avro path and writes a snap-*.avro
     * file. Our commitTransaction path extracts the ML delta from the resulting
     * snapshot's allManifests(io) list (reading the Avro file back) and stores
     * it in the catalog pool — so the end state on reload is correct, but a
     * transient snap-*.avro file is written. Eliminating that file requires
     * forwarding the sink through TransactionTableOperations (iceberg core).
     *
     * <p>Note: multi-table transactions not tested here due to a pre-existing
     * bug where consecutive buildTable().create() calls overwrite each other
     * (unrelated to ML); tracked separately.
     */
    @Test
    void commitTransactionWithML() {
      createNamespaceAndTable();

      org.apache.iceberg.catalog.CatalogTransaction txn =
          catalog.createTransaction(
              org.apache.iceberg.catalog.CatalogTransaction.IsolationLevel.SNAPSHOT);
      org.apache.iceberg.catalog.Catalog txCatalog = txn.asCatalog();
      txCatalog.loadTable(TBL).newFastAppend().appendFile(FILE_A).commit();
      txn.commitTransaction();

      // Reload via fresh catalog; manifest list must survive
      FileIOCatalog fresh = reloadCatalog();
      Table reloaded = fresh.loadTable(TBL);
      assertThat(reloaded.currentSnapshot())
          .as("snapshot should survive commitTransaction")
          .isNotNull();
      assertThat(reloaded.currentSnapshot().allManifests(io))
          .as("manifest list survives commitTransaction")
          .hasSize(1);
      // Note: snap-*.avro IS written because the transaction wrapper bypasses
      // the sink; the ML delta is still captured correctly via allManifests.
    }

/**
     * §2.3: wrapInlineManifests must reject catalog state where a snapshot
     * has an inline:// sentinel location but the pool has no entry for it.
     * This is the signature of a §1.1-class bug (pool cleared by a non-ML
     * commit) — silent delegation to FileIO would yield a confusing
     * File not found. Direct check: construct a poisoned catalog file and
     * verify loadTable throws IllegalStateException.
     */
    @Test
    void sentinelWithoutPoolFailsLoudly() throws Exception {
      createNamespaceAndTable();
      Table tbl = catalog.loadTable(TBL);
      tbl.newFastAppend().appendFile(FILE_A).commit();

      // Surgically clobber the manifest pool: reconstruct the catalog file
      // with inline metadata intact but empty pool/snapshotRefs
      ProtoCatalogFormat fmt = new ProtoCatalogFormat();
      ProtoCatalogFile original = (ProtoCatalogFile) fmt.read(
          io, io.newInputFile("mem:///warehouse/catalog"));
      Integer tblId = original.tableId(TBL);
      byte[] inlineMeta = original.inlineMetadata(tblId);
      String prefix = original.manifestListPrefix(tblId);
      var tblEntry = original.tableById().get(tblId);

      // Build a catalog file with ALL original state except the pool
      ProtoCatalogFile.Builder b = ProtoCatalogFile.builder(
          io.newInputFile("mem:///warehouse/catalog"));
      b.setCatalogUuid(original.uuid());
      for (var e : original.namespaceById().entrySet()) {
        b.addNamespace(e.getKey(), e.getValue().parentId,
            e.getValue().name, e.getValue().version);
      }
      // Keep inline metadata (with inline:// sentinel) but NO pool
      b.addInlineTable(tblId, tblEntry.namespaceId, tblEntry.name,
          tblEntry.version, inlineMeta, prefix);
      b.setNextNamespaceId(original.nextNamespaceId());
      b.setNextTableId(original.nextTableId());
      ProtoCatalogFile poisoned = b.build();

      // Serialize the poisoned file back to io, overwriting the catalog
      ProtoCatalogFormat.Mut mut = new ProtoCatalogFormat.Mut(poisoned);
      byte[] header = new byte[8];
      System.arraycopy(ProtoCatalogFormat.MAGIC, 0, header, 0,
          ProtoCatalogFormat.MAGIC.length);
      header[4] = 0; header[5] = 0; header[6] = 0;
      header[7] = (byte) ProtoCatalogFormat.FORMAT_VERSION;
      ProtoCatalogFormat.ProtoIdManager idmgr = new ProtoCatalogFormat.ProtoIdManager();
      idmgr.setGlobals(poisoned.nextNamespaceId(), poisoned.nextTableId());
      byte[] checkpoint = ProtoCodec.encodeCheckpoint(poisoned, mut, idmgr);

      java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
      out.write(header);
      // varint-encode checkpoint length
      int len = checkpoint.length;
      while ((len & ~0x7F) != 0) {
        out.write((len & 0x7F) | 0x80);
        len >>>= 7;
      }
      out.write(len);
      out.write(checkpoint);
      io.files.put("mem:///warehouse/catalog", out.toByteArray());

      // Attempt to load via a fresh catalog — should throw
      FileIOCatalog fresh = reloadCatalog();
      org.assertj.core.api.Assertions.assertThatThrownBy(() -> fresh.loadTable(TBL))
          .isInstanceOfAny(IllegalStateException.class, RuntimeException.class)
          .hasMessageContaining("sentinel manifest-list");
    }

    /**
     * §2.6: Expiring a snapshot must cascade to the catalog pool — remove the
     * snapshot's ref list and GC pool entries no longer referenced. Without
     * the cascade, the pool grows indefinitely with expired-snapshot count.
     */
    @Test
    void expireSnapshotsGcsPool() {
      createNamespaceAndTable();
      Table tbl = catalog.loadTable(TBL);
      tbl.newFastAppend().appendFile(FILE_A).commit();
      long firstSnapId = tbl.currentSnapshot().snapshotId();
      tbl.newFastAppend().appendFile(FILE_B).commit();
      long secondSnapId = tbl.currentSnapshot().snapshotId();

      // Expire the first snapshot; second is current (retained)
      tbl.expireSnapshots().expireSnapshotId(firstSnapId).commit();

      // Reload via fresh catalog and inspect the pool
      FileIOCatalog fresh = reloadCatalog();
      Table reloaded = fresh.loadTable(TBL);
      assertThat(reloaded.snapshot(firstSnapId))
          .as("expired snapshot should be gone from metadata")
          .isNull();
      assertThat(reloaded.currentSnapshot().snapshotId())
          .as("second snapshot retained")
          .isEqualTo(secondSnapId);
      // Current snapshot still has its 2 manifests (carry-forward M_A + new M_B)
      assertThat(reloaded.currentSnapshot().allManifests(io)).hasSize(2);

      // Inspect pool directly: should not have entries for the expired snapshot
      ProtoCatalogFormat fmt = new ProtoCatalogFormat();
      ProtoCatalogFile proto = (ProtoCatalogFile) fmt.read(
          io, io.newInputFile("mem:///warehouse/catalog"));
      Integer tblId = proto.tableId(TBL);
      // The expired snapshot's ref list must be gone
      assertThat(proto.hasInlineManifests(tblId, firstSnapId))
          .as("expired snapshot's ref list should be GC'd")
          .isFalse();
      // The retained snapshot still has refs
      assertThat(proto.hasInlineManifests(tblId, secondSnapId)).isTrue();
    }

    /**
     * §1.1: After a FastAppend populates the pool, a non-ML commit (e.g.
     * property change) that would naturally go through full mode must NOT
     * clobber the pool. Validates both the commit-time guard (forces delta
     * mode on ML-populated tables) and the replay-time fix (updateInlineMetadata
     * preserves the pool) together.
     */
    @Test
    void fullModePreservesPool() {
      createNamespaceAndTable();
      Table tbl = catalog.loadTable(TBL);
      tbl.newFastAppend().appendFile(FILE_A).commit();
      long snapId = tbl.currentSnapshot().snapshotId();

      // A property-only commit would naturally take full mode if the delta
      // were too small to justify delta encoding — but our commit-time guard
      // should force delta mode when the pool is populated.
      tbl.updateProperties().set("read.split.target-size", "134217728").commit();

      // Reload via fresh catalog; the pool must still have an entry
      FileIOCatalog fresh = reloadCatalog();
      Table reloaded = fresh.loadTable(TBL);
      Snapshot snap = reloaded.snapshot(snapId);
      assertThat(snap).as("snapshot should still exist").isNotNull();
      List<ManifestFile> manifests = snap.allManifests(io);
      assertThat(manifests).as("manifest pool preserved across non-ML commit").hasSize(1);
      assertThat(reloaded.properties())
          .as("property update applied")
          .containsEntry("read.split.target-size", "134217728");
    }
  }
}
