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
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryFileIO;
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

  @Test
  void aCatalogWithoutALogCannotBeInitialized() {
    LogCatalog catalog = new LogCatalog("c", new HashMap<>());
    assertThatThrownBy(
            () -> catalog.initialize("c", Map.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("has no log");
  }
}
