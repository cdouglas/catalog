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
package org.apache.iceberg.rest;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * C22 (expiry drops a concurrent ref) end to end, through a REST client and Iceberg's REST server
 * ({@code RESTCatalogAdapter}, {@code CatalogHandlers}): an expiry picks the snapshots to remove on
 * the version it refreshed and sends them as a {@code remove-snapshots} update, for which {@code
 * UpdateRequirements} derives no requirement, and the server applies it to the table as it is then.
 * Stock Iceberg (1.12.0, and the Polaris server catalog-bench compares) removes a snapshot a ref
 * came to name since, and the ref with it. The fork's core-fixes (a2a7847) fails to apply such a
 * removal, so the client computes the expiry again and keeps them, as a catalog that swaps metadata
 * does. TestIcebergCoreIssues' C22 canary applies the update as the server does; this one runs the
 * client's expiry and its retry. docs/iceberg-issues/C22.md; iceberg-go met the same race
 * (apache/iceberg-go#722, closed unmerged 2026-05-21).
 */
class TestRestExpiryRace {
  private static final Namespace NS = Namespace.of("ns");
  private static final TableIdentifier ID = TableIdentifier.of(NS, "t");
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  private InMemoryCatalog backend;
  private RESTCatalog rest;
  /** Run on the server, once, just before it applies the next table update. */
  private final AtomicReference<Runnable> beforeUpdate = new AtomicReference<>();

  @BeforeEach
  void before() {
    backend = new InMemoryCatalog();
    backend.initialize("backend", Map.of(CatalogProperties.WAREHOUSE_LOCATION, "mem://wh"));
    RESTCatalogAdapter adapter =
        new RESTCatalogAdapter(backend) {
          @Override
          public <T extends RESTResponse> T handleRequest(
              Route route,
              Map<String, String> vars,
              HTTPRequest httpRequest,
              Class<T> responseType,
              Consumer<Map<String, String>> responseHeaders) {
            if (route == Route.UPDATE_TABLE) {
              Runnable race = beforeUpdate.getAndSet(null);
              if (race != null) {
                race.run();
              }
            }
            return super.handleRequest(route, vars, httpRequest, responseType, responseHeaders);
          }
        };
    rest = new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), config -> adapter);
    rest.initialize(
        "rest",
        Map.of(
            CatalogProperties.URI, "http://localhost:8181",
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));
    rest.createNamespace(NS);
  }

  @AfterEach
  void after() throws Exception {
    rest.close();
    backend.close();
  }

  private static DataFile file(int i) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath("mem://wh/data/f" + i + ".parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  /** A table of three snapshots, the oldest first. */
  private Table threeSnapshots(org.apache.iceberg.catalog.Catalog catalog) {
    Table table = catalog.createTable(ID, SCHEMA);
    for (int i = 0; i < 3; i++) {
      table.newFastAppend().appendFile(file(i)).commit();
    }
    return catalog.loadTable(ID);
  }

  private static long oldest(Table table) {
    return Lists.newArrayList(table.snapshots()).get(0).snapshotId();
  }

  /**
   * An expiry keeping the last two snapshots picks the oldest; a tag lands on it before the server
   * applies the removal. The server refuses it, and the expiry's retry keeps the tagged snapshot.
   */
  @Test
  void aRestExpiryRacingATagKeepsTheTaggedSnapshotAndTheTag() {
    Table table = threeSnapshots(rest);
    long oldest = oldest(table);
    beforeUpdate.set(() -> backend.loadTable(ID).manageSnapshots().createTag("t", oldest).commit());

    table.expireSnapshots()
        .expireOlderThan(System.currentTimeMillis() + 1)
        .retainLast(2)
        .cleanExpiredFiles(false)
        .commit();

    assertThat(beforeUpdate.get()).as("the tag raced the expiry").isNull();
    Table now = backend.loadTable(ID);
    assertThat(now.refs()).as("C22 is back: the tag went with the snapshot").containsKey("t");
    assertThat(now.snapshot(oldest)).as("C22 is back").isNotNull();
  }

  /**
   * The same race through a catalog that swaps metadata: the tag lands between the expiry's apply
   * and its commit, the commit fails on the changed table, and Iceberg's retry, on a fresh read,
   * keeps the snapshot the tag names.
   */
  @Test
  void aMetadataSwappingCatalogsExpiryKeepsTheTaggedSnapshotAndTheTag() {
    long oldest = oldest(threeSnapshots(backend));
    TableOperations ops = ((HasTableOperations) backend.loadTable(ID)).operations();
    AtomicReference<Runnable> race =
        new AtomicReference<>(
            () -> backend.loadTable(ID).manageSnapshots().createTag("t", oldest).commit());
    TableOperations racing =
        new TableOperations() {
          @Override
          public TableMetadata current() {
            return ops.current();
          }

          @Override
          public TableMetadata refresh() {
            return ops.refresh();
          }

          @Override
          public void commit(TableMetadata base, TableMetadata metadata) {
            Runnable once = race.getAndSet(null);
            if (once != null) {
              once.run();
            }
            ops.commit(base, metadata);
          }

          @Override
          public FileIO io() {
            return ops.io();
          }

          @Override
          public String metadataFileLocation(String fileName) {
            return ops.metadataFileLocation(fileName);
          }

          @Override
          public LocationProvider locationProvider() {
            return ops.locationProvider();
          }
        };

    new BaseTable(racing, "t").expireSnapshots()
        .expireOlderThan(System.currentTimeMillis() + 1)
        .retainLast(2)
        .cleanExpiredFiles(false)
        .commit();

    assertThat(race.get()).as("the tag raced the expiry").isNull();
    Table now = backend.loadTable(ID);
    assertThat(now.refs()).containsKey("t");
    assertThat(now.snapshot(oldest)).isNotNull();
  }
}
