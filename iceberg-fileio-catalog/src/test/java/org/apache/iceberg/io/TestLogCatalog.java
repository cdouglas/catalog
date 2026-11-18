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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.types.Types;
import org.intentlog.fileio.AtomicFileLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A {@link LogCatalog} given its log and a FileIO of its own: the catalog's state goes to the log,
 * and its tables' files (metadata files, manifest lists, manifests) to the FileIO, which needs no
 * conditional writes.
 */
class TestLogCatalog {

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  private final InMemoryAtomicFileIO logIO = new InMemoryAtomicFileIO();
  private final FileIO dataIO = new InMemoryFileIO();
  private final String warehouse = "mem:///warehouse-" + UUID.randomUUID();
  private final String logLocation = warehouse + "/catalog";

  private LogCatalog catalog(String name, Map<String, String> extra) {
    Map<String, String> properties = new HashMap<>(extra);
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
    LogCatalog catalog =
        new LogCatalog(name, new AtomicFileLog(logIO, logLocation), dataIO, new HashMap<>());
    catalog.initialize(name, properties);
    return catalog;
  }

  @ParameterizedTest(name = "inline={0}")
  @ValueSource(strings = {"none", "metadata", "manifests"})
  void tablesFilesGoToTheirOwnFileIO(String inline) {
    Map<String, String> mode =
        Map.of(
            "fileio.catalog.inline", String.valueOf(!inline.equals("none")),
            "fileio.catalog.inline.manifests", String.valueOf(inline.equals("manifests")));
    LogCatalog writer = catalog("writer", mode);
    Namespace ns = Namespace.of("db");
    TableIdentifier id = TableIdentifier.of(ns, "t");
    writer.createNamespace(ns);
    Table table = writer.createTable(id, SCHEMA);
    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(warehouse + "/db/t/data/a.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    table.newFastAppend().appendFile(file).commit();

    // The log is one object; every file of the table is in the table's FileIO.
    assertThat(logIO.size()).isEqualTo(1);
    assertThat(logIO.conditionalWrites()).containsOnlyKeys(logLocation);

    LogCatalog reader = catalog("reader", mode);
    assertThat(reader.listTables(ns)).containsExactly(id);
    Table read = reader.loadTable(id);
    assertThat(read.currentSnapshot().allManifests(read.io())).hasSize(1);
    assertThat(read.currentSnapshot().addedDataFiles(read.io()))
        .extracting(DataFile::location)
        .containsExactly(file.location());
  }

  /**
   * The catalog's own parallel work, here a drop that keeps an inline-ML table's files writing its
   * manifest lists, runs on Iceberg's worker pool, or on the executor the catalog is given ({@link
   * LogCatalog#executeWith}): a direct one runs it on the calling thread, where a benchmark
   * attributes its requests to the operation.
   */
  @ParameterizedTest(name = "direct={0}")
  @ValueSource(booleans = {true, false})
  void aDropWritesItsListsOnTheCatalogsExecutor(boolean direct) {
    Set<Thread> writers = ConcurrentHashMap.newKeySet();
    List<String> lists = new CopyOnWriteArrayList<>();
    FileIO io = new InMemoryFileIO() {
      @Override
      public OutputFile newOutputFile(String location) {
        if (location.contains("/metadata/snap-")) {
          writers.add(Thread.currentThread());
          lists.add(location);
        }
        return super.newOutputFile(location);
      }
    };
    LogCatalog catalog =
        new LogCatalog("c", new AtomicFileLog(logIO, logLocation), io, new HashMap<>());
    catalog.initialize("c", Map.of(
        CatalogProperties.WAREHOUSE_LOCATION, warehouse,
        "fileio.catalog.inline", "true",
        "fileio.catalog.inline.manifests", "true"));
    if (direct) {
      catalog.executeWith(MoreExecutors.newDirectExecutorService());
    }
    Namespace ns = Namespace.of("db");
    TableIdentifier id = TableIdentifier.of(ns, "t");
    catalog.createNamespace(ns);
    Table table = catalog.createTable(id, SCHEMA);
    for (String name : List.of("a", "b", "c")) {
      table.newFastAppend()
          .appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath(warehouse + "/db/t/data/" + name + ".parquet")
              .withFileSizeInBytes(10)
              .withRecordCount(1)
              .build())
          .commit();
    }
    assertThat(lists).as("the catalog holds the lists").isEmpty();

    assertThat(catalog.dropTable(id, false)).isTrue();

    assertThat(lists).hasSize(3);
    if (direct) {
      assertThat(writers).containsExactly(Thread.currentThread());
    } else {
      assertThat(writers).isNotEmpty().doesNotContain(Thread.currentThread());
    }
  }

  @Test
  void aCatalogWithoutALogCannotBeInitialized() {
    LogCatalog catalog = new LogCatalog("c", new HashMap<>());
    assertThatThrownBy(
            () -> catalog.initialize("c", Map.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("has no log");
  }
}
