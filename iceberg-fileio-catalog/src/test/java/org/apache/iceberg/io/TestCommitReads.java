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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.InlineSnapshot;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.intentlog.fileio.AtomicFileLog;
import org.intentlog.memory.InMemorySharedLog;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * How often a commit through Iceberg's API reads the catalog object (errata D20): once, for the
 * refresh {@code SnapshotProducer.apply} makes. The read through its own write takes the object
 * from the write, and the table's operations take the catalog the commit landed in, in which
 * {@code SnapshotProducer} finds its snapshot.
 */
class TestCommitReads {

  private static final String WAREHOUSE = "mem:///warehouse";
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
  private static final TableIdentifier TABLE = TableIdentifier.of("db", "t");

  private static DataFile file(int i) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(WAREHOUSE + "/db/t/data/file-" + i + ".parquet")
        .withFileSizeInBytes(100)
        .withRecordCount(10)
        .build();
  }

  /**
   * Requests that reach the catalog object while {@code action} runs: a read is one (a GET, which
   * binds the version it reads), and so is a lookup. The suites' check of each tail read against a
   * whole read (AtomicFileLog.CHECK_TAIL_READS) is off meanwhile: its reads are the test's, not
   * the commit's.
   */
  private static int catalogRequests(InMemoryAtomicFileIO io, String catalog, Runnable action) {
    AtomicInteger requests = new AtomicInteger();
    io.injectReadFaults(location -> {
      if (location.equals(catalog)) {
        requests.incrementAndGet();
      }
      return InMemoryAtomicFileIO.ReadFault.NONE;
    });
    String check = System.getProperty(AtomicFileLog.CHECK_TAIL_READS);
    System.setProperty(AtomicFileLog.CHECK_TAIL_READS, "false");
    try {
      action.run();
    } finally {
      io.injectReadFaults(location -> InMemoryAtomicFileIO.ReadFault.NONE);
      if (check == null) {
        System.clearProperty(AtomicFileLog.CHECK_TAIL_READS);
      } else {
        System.setProperty(AtomicFileLog.CHECK_TAIL_READS, check);
      }
    }
    return requests.get();
  }

  @ParameterizedTest
  @EnumSource(value = LocalCatalogMode.class, names = {"APPEND", "APPEND_TM", "APPEND_ML", "CAS",
      "CAS_TM", "CAS_ML"})
  void aCommitReadsTheCatalogOnce(LocalCatalogMode mode) {
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    FileIOCatalog catalog = mode.catalog(io, WAREHOUSE, "c", Map.of());
    String object = WAREHOUSE + "/catalog-c";
    catalog.createNamespace(Namespace.of("db"));
    catalog.createTable(TABLE, SCHEMA).newAppend().appendFile(file(0)).commit();

    Table[] table = new Table[1];
    assertThat(catalogRequests(io, object, () -> table[0] = catalog.loadTable(TABLE)))
        .as("a load reads the catalog")
        .isEqualTo(1);
    for (int i = 1; i <= 3; i++) {
      DataFile added = file(i);
      assertThat(catalogRequests(io, object, () -> table[0].newAppend().appendFile(added).commit()))
          .as("merge append %d reads the catalog once, to refresh before it applies", i)
          .isEqualTo(1);
      assertThat(table[0].currentSnapshot().addedDataFiles(table[0].io()))
          .extracting(f -> f.location())
          .containsExactly(file(i).location());
    }

    // The appends landed, and a fresh reader sees them
    assertThat(mode.<FileIOCatalog>catalog(io, WAREHOUSE, "c", Map.of()).loadTable(TABLE)
            .currentSnapshot().snapshotId())
        .isEqualTo(table[0].currentSnapshot().snapshotId());
  }

  /** GETs of metadata files while {@code action} runs. */
  private static int metadataReads(InMemoryAtomicFileIO io, Runnable action) {
    AtomicInteger reads = new AtomicInteger();
    io.injectReadFaults(location -> {
      if (location.endsWith(".metadata.json")) {
        reads.incrementAndGet();
      }
      return InMemoryAtomicFileIO.ReadFault.NONE;
    });
    try {
      action.run();
    } finally {
      io.injectReadFaults(location -> InMemoryAtomicFileIO.ReadFault.NONE);
    }
    return reads.get();
  }

  /**
   * A pointer table's operations hold the metadata file their commit wrote, as JDBC's on core-perf
   * do and REST's hold a commit's response (errata D26): a commit reads no metadata file back, nor
   * does a look at the table after it, and a create's table is the file it wrote.
   */
  @ParameterizedTest
  @EnumSource(value = LocalCatalogMode.class, names = {"APPEND", "CAS"})
  void aPointerCommitReadsNoMetadataFileBack(LocalCatalogMode mode) {
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    FileIOCatalog catalog = mode.catalog(io, WAREHOUSE, "c", Map.of());
    catalog.createNamespace(Namespace.of("db"));
    Table[] table = new Table[1];
    assertThat(metadataReads(io, () -> {
      table[0] = catalog.createTable(TABLE, SCHEMA);
      table[0].currentSnapshot();
    })).as("a create holds the file it wrote").isZero();
    for (int i = 0; i < 3; i++) {
      DataFile added = file(i);
      assertThat(metadataReads(io, () -> {
        table[0].newAppend().appendFile(added).commit();
        table[0].currentSnapshot();
      })).as("merge append %d reads no metadata file", i).isZero();
      assertThat(table[0].currentSnapshot().addedDataFiles(table[0].io()))
          .extracting(f -> f.location())
          .containsExactly(added.location());
    }

    assertHoldsWhatAFreshReaderParses(table[0], mode.catalog(io, WAREHOUSE, "c", Map.of()));
  }

  /**
   * What {@code table} holds is the file the catalog names, exactly as a fresh reader parses it:
   * the same location, the same metadata, as JSON, and no changes.
   */
  private static void assertHoldsWhatAFreshReaderParses(Table table, LogCatalog reader) {
    TableMetadata held = ((HasTableOperations) table).operations().current();
    TableMetadata parsed = ((HasTableOperations) reader.loadTable(TABLE)).operations().current();
    assertThat(held.metadataFileLocation()).isEqualTo(parsed.metadataFileLocation());
    assertThat(TableMetadataParser.toJson(held)).isEqualTo(TableMetadataParser.toJson(parsed));
    assertThat(held.changes()).isEmpty();
  }

  /**
   * A commit that writes an inline-ML table to a metadata file writes its snapshots' lists to
   * files first, and the table's operations hold what the metadata file holds, which names those
   * files, not the lists the commit staged (errata D26): here the table leaves inline storage, an
   * update too large for it.
   */
  @ParameterizedTest
  @EnumSource(value = LocalCatalogMode.class, names = {"APPEND_ML", "CAS_ML", "MEMLOG_ML"})
  void aTableLeavingInlineStorageHoldsItsFile(LocalCatalogMode mode) {
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    Map<String, String> small = Map.of(FileIOCatalog.INLINE_MAX_UPDATE_SIZE, "2048");
    LogCatalog catalog = mode.catalog(io, WAREHOUSE, "c", small);
    catalog.createNamespace(Namespace.of("db"));
    Table table = catalog.createTable(TABLE, SCHEMA);
    for (int i = 0; i < 2; i++) {
      table.newFastAppend().appendFile(file(i)).commit();
    }
    LogCatalog reader = mode.catalog(io, WAREHOUSE, "c", small);
    assertThat(reader.loadTable(TABLE).currentSnapshot())
        .as("the table is inline").isInstanceOf(InlineSnapshot.class);

    // an update too large to inline, compressed as updates are: random hex, ~4 KB
    StringBuilder big = new StringBuilder();
    java.util.Random random = new java.util.Random(0);
    while (big.length() < 8192) {
      big.append(Long.toHexString(random.nextLong()));
    }
    table.updateProperties().set("big", big.toString()).commit();

    assertThat(reader.loadTable(TABLE).snapshots())
        .as("the table left inline storage: its metadata file names list files")
        .allSatisfy(s -> assertThat(s.manifestListLocation()).isNotNull());
    assertHoldsWhatAFreshReaderParses(table, reader);
  }

  /** As above, a writer that does not inline committing to an inline-ML table. */
  @ParameterizedTest
  @EnumSource(value = LocalCatalogMode.class, names = {"APPEND", "CAS", "MEMLOG"})
  void aWriterThatDoesNotInlineHoldsTheFileItWrote(LocalCatalogMode mode) {
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    LocalCatalogMode inlining = LocalCatalogMode.valueOf(mode.name() + "_ML");
    LogCatalog creator = inlining.catalog(io, WAREHOUSE, "c", Map.of());
    creator.createNamespace(Namespace.of("db"));
    Table created = creator.createTable(TABLE, SCHEMA);
    for (int i = 0; i < 2; i++) {
      created.newFastAppend().appendFile(file(i)).commit();
    }

    LogCatalog writer = mode.catalog(io, WAREHOUSE, "c", Map.of());
    Table table = writer.loadTable(TABLE);
    assertThat(table.currentSnapshot()).as("the table is inline")
        .isInstanceOf(InlineSnapshot.class);
    table.newFastAppend().appendFile(file(2)).commit();

    LogCatalog reader = mode.catalog(io, WAREHOUSE, "c", Map.of());
    assertThat(reader.loadTable(TABLE).snapshots())
        .as("the metadata file names list files")
        .allSatisfy(s -> assertThat(s.manifestListLocation()).isNotNull());
    assertHoldsWhatAFreshReaderParses(table, reader);
  }

  /**
   * A commit's read through its own write may read on past its record: on a shared log, another
   * writer's update of the table that lands just after it. The table's operations then hold the
   * table as that writer left it, reading the file it names, not the one this commit wrote
   * (errata D26: they hold the written file's metadata only where the catalog names that file),
   * and a commit on the handle after keeps the other writer's update. Holding this commit's
   * metadata under the other's file would hide that update from refreshes (Iceberg reads a
   * metadata file again only when the location changes), and the next commit, made on it and
   * checked against the version the handle read, would drop it.
   */
  @ParameterizedTest
  @EnumSource(value = LocalCatalogMode.class, names = {"MEMLOG", "MEMLOG_TM"})
  void anUpdateLandingJustAfterACommitIsHeldAndKept(LocalCatalogMode mode) {
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    LogCatalog catalog = mode.catalog(io, WAREHOUSE, "c", Map.of());
    catalog.createNamespace(Namespace.of("db"));
    catalog.createTable(TABLE, SCHEMA);
    Table table = catalog.loadTable(TABLE);
    // a pointer table's writer, whose update names a metadata file
    LogCatalog other = LocalCatalogMode.MEMLOG.catalog(io, WAREHOUSE, "c", Map.of());
    InMemorySharedLog log = LocalCatalogMode.sharedLog(io, WAREHOUSE + "/catalog-c");
    boolean[] raced = {false};
    log.intercept(new InMemorySharedLog.Interceptor() {
      @Override
      public InMemorySharedLog.ReadShape beforeRead(InMemorySharedLog.Request request) {
        if (request == InMemorySharedLog.Request.READ_THROUGH && !raced[0]) {
          raced[0] = true;
          other.loadTable(TABLE).updateProperties().set("k", "other").commit();
        }
        return InMemorySharedLog.ReadShape.NORMAL;
      }
    });
    try {
      table.newFastAppend().appendFile(file(0)).commit();
    } finally {
      log.intercept(null);
    }
    assertThat(raced[0]).as("the other writer's update landed just after the append").isTrue();
    LogCatalog reader = mode.catalog(io, WAREHOUSE, "c", Map.of());
    assertThat(reader.loadTable(TABLE).properties()).containsEntry("k", "other");
    assertThat(table.properties()).as("the handle holds the other writer's update")
        .containsEntry("k", "other");

    table.newFastAppend().appendFile(file(1)).commit();

    Table now = reader.loadTable(TABLE);
    assertThat(now.properties()).as("the next commit kept it").containsEntry("k", "other");
    java.util.Set<String> files = new java.util.TreeSet<>();
    try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
        now.newScan().planFiles()) {
      tasks.forEach(task -> files.add(task.file().location()));
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
    assertThat(files).containsExactlyInAnyOrder(file(0).location(), file(1).location());
    assertHoldsWhatAFreshReaderParses(table, reader);
  }

  /**
   * An update staged on the table a commit left, which another writer's commit raced, conflicts
   * as one staged on a read does (CatalogTests.testUpdateTableSchemaConflict, which failed on GCS
   * when the table's operations first took the replica the create landed in: the tail the write
   * returned named a generation that GCS no longer found, and the commit took the log for absent).
   * Strict versions look a replaced generation up as GCS does.
   */
  @ParameterizedTest
  @EnumSource(value = LocalCatalogMode.class, names = {"APPEND", "APPEND_TM", "CAS", "CAS_TM",
      "CAS_ML"})
  void anUpdateOnTheTableACommitLeftConflictsWhenRaced(LocalCatalogMode mode) {
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    io.failSupersededReads(true);
    FileIOCatalog catalog = mode.catalog(io, WAREHOUSE, "c", Map.of());
    catalog.createNamespace(Namespace.of("db"));
    Table table = catalog.buildTable(TABLE, SCHEMA).create();
    org.apache.iceberg.UpdateSchema update =
        table.updateSchema().addColumn("new_col", Types.LongType.get());

    catalog.loadTable(TABLE).updateSchema().addColumn("another_col", Types.StringType.get())
        .commit();

    org.assertj.core.api.Assertions.assertThatThrownBy(update::commit)
        .isInstanceOf(org.apache.iceberg.exceptions.CommitFailedException.class)
        .hasMessageContaining("Cannot commit");
  }
}
