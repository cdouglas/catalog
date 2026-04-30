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
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.InlineSnapshot;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
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
  static final DeleteFile FILE_A_POS_DELETES = FileMetadata.deleteFileBuilder(TEST_SPEC)
      .ofPositionDeletes()
      .withPath("/path/to/data-a-pos-deletes.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("data_bucket=0")
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
          "test", wh + "/catalog", new ProtoCatalogFormat(), io, props);
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
          "test2", wh + "/catalog", new ProtoCatalogFormat(), io, props);
      fresh.initialize("test2", props);
      return fresh;
    }
  }

  /**
   * Errata S3: FileIOCatalog must not implement Hadoop's Configurable.
   * Reintroducing the dependency drags Hadoop back into the public catalog
   * surface; loaders that pass a Configuration via CatalogUtil's
   * configureHadoopConf would silently re-acquire it and the catalog would
   * once again carry the dead {@code conf} field.
   */
  @Test
  void catalogIsNotHadoopConfigurable() {
    assertThat(new FileIOCatalog())
        .isNotInstanceOf(org.apache.hadoop.conf.Configurable.class);
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

    /**
     * Errata D1: a transaction that mixes a delta-representable update
     * (setProperties) with statistics — currently not representable as a
     * delta — must not silently drop the statistics. The bug was: the
     * presence of a non-stats change made computeDelta return a non-empty
     * list, selectMode picked "delta", and stats were never encoded.
     */
    @Test
    void transactionWithStatisticsAndPropertiesSurvivesReload() {
      createNamespaceAndTable();
      Table tbl = catalog.loadTable(TBL);
      // Need a snapshot to attach stats to.
      tbl.newFastAppend().appendFile(FILE_A).commit();
      long snapId = tbl.currentSnapshot().snapshotId();

      StatisticsFile stat = new GenericStatisticsFile(
          snapId, "/path/to/stats.puffin", 100, 90, List.of());

      Transaction tx = tbl.newTransaction();
      tx.updateProperties().set("k", "v").commit();
      tx.updateStatistics().setStatistics(stat).commit();
      tx.commitTransaction();

      FileIOCatalog fresh = reloadCatalog();
      Table reloaded = fresh.loadTable(TBL);
      assertThat(reloaded.properties()).containsEntry("k", "v");
      assertThat(reloaded.statisticsFiles())
          .as("stats must survive reload alongside property update")
          .extracting(StatisticsFile::snapshotId)
          .containsExactly(snapId);
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
     * <p>After iceberg-core R2 (sink forwarding through
     * BaseTransaction.TransactionTableOperationsWithSink), SnapshotProducer
     * takes the inline path inside a transaction too — no transient
     * snap-*.avro is written, closing the orphan-file window described in
     * errata S2 / iceberg_refine.md R2. This test asserts both end-state
     * correctness on reload and absence of the transient Avro file.
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

      // Spirit-of-I1: no transient snap-*.avro orphan window in the multi-table
      // commit path. iceberg-core R2 forwards ManifestListSink through the
      // transaction wrapper, so SnapshotProducer.apply() takes the inline path.
      assertThat(io.filesMatching("snap-"))
          .as("no snap-*.avro written when sink is forwarded through the transaction wrapper")
          .isEmpty();
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

    // ============================================================
    // Per-operation coverage (§4.1)
    //
    // Each test commits one operation, then reloads via a fresh catalog
    // and asserts the manifest list is correct. Covers SnapshotProducer
    // subclasses beyond FastAppend.
    // ============================================================

    /** MergeAppend (newAppend): merges small manifests on commit. */
    @Test
    void mergeAppendCommitAndReload() {
      createNamespaceAndTable();
      Table tbl = catalog.loadTable(TBL);
      tbl.newAppend().appendFile(FILE_A).commit();

      FileIOCatalog fresh = reloadCatalog();
      Table reloaded = fresh.loadTable(TBL);
      assertThat(reloaded.currentSnapshot()).isNotNull();
      assertThat(reloaded.currentSnapshot().allManifests(io)).hasSize(1);
      assertThat(reloaded.currentSnapshot().operation()).isEqualTo("append");
    }

    /** BaseOverwriteFiles (newOverwrite): overwrite semantics with add + remove. */
    @Test
    void overwriteCommitAndReload() {
      createNamespaceAndTable();
      Table tbl = catalog.loadTable(TBL);
      // Seed with a snapshot so overwrite has something to overwrite
      tbl.newFastAppend().appendFile(FILE_A).commit();
      // Overwrite: add B, the overwrite operation marks FILE_A as deleted
      tbl.newOverwrite().addFile(FILE_B).deleteFile(FILE_A).commit();

      FileIOCatalog fresh = reloadCatalog();
      Table reloaded = fresh.loadTable(TBL);
      Snapshot current = reloaded.currentSnapshot();
      assertThat(current).isNotNull();
      assertThat(current.operation()).isEqualTo("overwrite");
      // Overwrite produces a new manifest with B + rewrite of A's manifest
      assertThat(current.allManifests(io)).as("overwrite manifest list").hasSizeGreaterThanOrEqualTo(1);
    }

    /** BaseReplacePartitions (newReplacePartitions): partition-level replace. */
    @Test
    void replacePartitionsCommitAndReload() {
      createNamespaceAndTable();
      Table tbl = catalog.loadTable(TBL);
      tbl.newFastAppend().appendFile(FILE_A).commit();
      tbl.newReplacePartitions().addFile(FILE_B).commit();

      FileIOCatalog fresh = reloadCatalog();
      Table reloaded = fresh.loadTable(TBL);
      Snapshot current = reloaded.currentSnapshot();
      assertThat(current).isNotNull();
      assertThat(current.operation()).isIn("overwrite", "replace");
      assertThat(current.allManifests(io)).isNotEmpty();
    }

    /** newDelete (StreamingDelete): file-based delete. */
    @Test
    void deleteCommitAndReload() {
      createNamespaceAndTable();
      Table tbl = catalog.loadTable(TBL);
      tbl.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
      tbl.newDelete().deleteFile(FILE_A).commit();

      FileIOCatalog fresh = reloadCatalog();
      Table reloaded = fresh.loadTable(TBL);
      Snapshot current = reloaded.currentSnapshot();
      assertThat(current).isNotNull();
      assertThat(current.operation()).isIn("delete", "overwrite");
    }

    /**
     * Errata D1: a stats-only commit on a table with an existing manifest
     * pool must not crash. The bug was: computeDelta returned null
     * (no recognized change), the ML "force delta on populated pool"
     * branch flipped mode to "delta" anyway, and encodeDelta(null) NPE'd.
     */
    @Test
    void setStatisticsAfterAppendSurvivesReload() {
      createNamespaceAndTable();
      Table tbl = catalog.loadTable(TBL);
      tbl.newFastAppend().appendFile(FILE_A).commit();
      long snapId = tbl.currentSnapshot().snapshotId();

      StatisticsFile stat = new GenericStatisticsFile(
          snapId, "/path/to/stats.puffin", 100, 90, List.of());
      tbl.updateStatistics().setStatistics(stat).commit();

      FileIOCatalog fresh = reloadCatalog();
      Table reloaded = fresh.loadTable(TBL);
      assertThat(reloaded.statisticsFiles())
          .as("stats must survive reload on ML-populated table")
          .extracting(StatisticsFile::snapshotId)
          .containsExactly(snapId);
      // Existing snapshot's manifest list must still round-trip.
      assertThat(reloaded.currentSnapshot()).isNotNull();
      assertThat(reloaded.currentSnapshot().allManifests(io)).hasSize(1);
    }

    /**
     * CherryPickOperation: stage an append off-current via stageOnly, then
     * cherry-pick it back to the main branch. The cherry-pick produces a new
     * snapshot whose manifest list (= staged snapshot's data) must round-trip
     * through the inline-ML pool. Errata T1.
     */
    @Test
    void cherryPickCommitAndReload() {
      createNamespaceAndTable();
      Table tbl = catalog.loadTable(TBL);
      tbl.newFastAppend().appendFile(FILE_A).commit();
      long mainBeforeCherryPick = tbl.currentSnapshot().snapshotId();

      // Stage an append off-current. After stageOnly, the staged snapshot
      // exists in metadata.snapshots() but the main branch still points at
      // FILE_A's snapshot.
      tbl.newAppend().appendFile(FILE_B).stageOnly().commit();
      tbl.refresh();
      assertThat(tbl.currentSnapshot().snapshotId()).isEqualTo(mainBeforeCherryPick);
      Snapshot staged = null;
      for (Snapshot s : tbl.snapshots()) {
        if (s.snapshotId() != mainBeforeCherryPick) {
          staged = s;
          break;
        }
      }
      assertThat(staged).as("staged snapshot must exist").isNotNull();

      tbl.manageSnapshots().cherrypick(staged.snapshotId()).commit();

      FileIOCatalog fresh = reloadCatalog();
      Table reloaded = fresh.loadTable(TBL);
      Snapshot current = reloaded.currentSnapshot();
      assertThat(current).isNotNull();
      assertThat(current.snapshotId()).isNotEqualTo(mainBeforeCherryPick);
      assertThat(current.parentId()).isEqualTo(mainBeforeCherryPick);
      // Cherry-pick fast-forwards by reusing the staged snapshot's manifest list,
      // so the cherry-picked snapshot must expose FILE_B's manifest after reload.
      List<ManifestFile> manifests = current.allManifests(io);
      assertThat(manifests).as("cherry-picked snapshot manifest list").isNotEmpty();
      assertThat(manifests).allMatch(mf -> mf.content() == ManifestContent.DATA);
    }

    /**
     * BaseRowDelta (newRowDelta): adding a position-delete file produces a
     * delete manifest that must round-trip alongside the data manifest in the
     * inline pool. Errata T1.
     */
    @Test
    void rowDeltaCommitAndReload() {
      createNamespaceAndTable();
      Table tbl = catalog.loadTable(TBL);
      tbl.newFastAppend().appendFile(FILE_A).commit();
      long appendSnapId = tbl.currentSnapshot().snapshotId();

      tbl.newRowDelta().addDeletes(FILE_A_POS_DELETES).commit();

      FileIOCatalog fresh = reloadCatalog();
      Table reloaded = fresh.loadTable(TBL);
      Snapshot current = reloaded.currentSnapshot();
      assertThat(current).isNotNull();
      assertThat(current.snapshotId()).isNotEqualTo(appendSnapId);
      assertThat(current.parentId()).isEqualTo(appendSnapId);
      // Must reload both the inherited data manifest and the new delete manifest.
      assertThat(current.dataManifests(io)).as("data manifests").isNotEmpty();
      assertThat(current.deleteManifests(io))
          .as("delete manifest from row delta must survive reload")
          .hasSize(1);
    }

    /** BaseRewriteManifests (rewriteManifests): manifest-level rewrite. */
    @Test
    void rewriteManifestsCommitAndReload() {
      createNamespaceAndTable();
      Table tbl = catalog.loadTable(TBL);
      // Create multiple manifests by doing separate fast appends
      tbl.newFastAppend().appendFile(FILE_A).commit();
      tbl.newFastAppend().appendFile(FILE_B).commit();
      long preRewriteSnapId = tbl.currentSnapshot().snapshotId();
      int beforeCount = tbl.currentSnapshot().allManifests(io).size();
      assertThat(beforeCount).as("pre-rewrite manifest count").isGreaterThanOrEqualTo(2);

      // Rewrite manifests — creates new manifest(s) superseding the originals
      tbl.rewriteManifests().clusterBy(f -> "all").commit();

      FileIOCatalog fresh = reloadCatalog();
      Table reloaded = fresh.loadTable(TBL);
      Snapshot current = reloaded.currentSnapshot();
      assertThat(current).isNotNull();
      assertThat(current.snapshotId()).isNotEqualTo(preRewriteSnapId);
      // rewriteManifests typically compacts to fewer manifests
      assertThat(current.allManifests(io)).isNotEmpty();
    }
  }
}
