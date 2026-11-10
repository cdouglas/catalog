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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.intentlog.fileio.AtomicFileLog;
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
