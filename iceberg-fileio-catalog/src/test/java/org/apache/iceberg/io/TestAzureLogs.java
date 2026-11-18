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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.intentlog.azure.AppendBlobStore;
import org.intentlog.azure.AzureAppendBlobStore;
import org.intentlog.azure.InMemoryAppendBlobStore;
import org.intentlog.fileio.AtomicFileLog;
import org.intentlog.wire.LogEnvelope;
import org.junit.jupiter.api.Test;

/** Opening the catalog's Azure log from its properties ({@link AzureLogs}). */
class TestAzureLogs {

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  private static final String ACCOUNT = "https://acct.blob.core.windows.net";

  /**
   * D13: the default prefix is the Blob URL of the catalog object's account, container and path,
   * with {@code .segments} appended, whichever way the ADLS FileIO's location is written.
   */
  @Test
  void theDefaultPrefixIsTheCatalogObjectsBlobUrlWithASuffix() {
    String expected = ACCOUNT + "/lake/warehouse/catalog.segments";
    for (String location : List.of("abfss://lake@acct.dfs.core.windows.net/warehouse/catalog",
        "abfs://lake@acct.dfs.core.windows.net/warehouse/catalog",
        "wasbs://lake@acct.blob.core.windows.net/warehouse/catalog",
        "ABFSS://Lake@ACCT.DFS.core.windows.net//warehouse//catalog")) {
      assertThat(AzureLogs.defaultPrefix(location)).as(location).isEqualTo(expected);
    }
    assertThat(AzureLogs.defaultPrefix("abfss://lake@acct.dfs.core.chinacloudapi.cn/wh/catalog"))
        .isEqualTo("https://acct.blob.core.chinacloudapi.cn/lake/wh/catalog.segments");
    assertThat(AzureLogs.defaultPrefix("abfss://lake@acct.dfs.core.windows.net/my wh/catalog"))
        .isEqualTo(ACCOUNT + "/lake/my%20wh/catalog.segments");
    for (String elsewhere : List.of("mem:///warehouse/catalog", "s3://bucket/warehouse/catalog",
        "gs://bucket/warehouse/catalog", "abfss://acct.dfs.core.windows.net/warehouse/catalog",
        "abfss://lake@acct.dfs.core.windows.net:443/warehouse/catalog",
        "abfss://lake@acct.queue.core.windows.net/warehouse/catalog",
        "abfss://lake@acct.dfs.core.windows.net/")) {
      assertThat(AzureLogs.defaultPrefix(elsewhere)).as(elsewhere).isNull();
    }
  }

  /** A configured location is a Blob URL of a container and a path, kept in canonical form. */
  @Test
  void aLocationNeedsAContainerAndAPath() {
    assertThat(AzureLogs.prefix("https://ACCT.blob.core.windows.net/logs/warehouse/catalog.log/"))
        .isEqualTo(ACCOUNT + "/logs/warehouse/catalog.log");
    assertThat(AzureLogs.prefix("http://127.0.0.1:10000/devstoreaccount1/dev/catalog.log"))
        .isEqualTo("http://127.0.0.1:10000/devstoreaccount1/dev/catalog.log");
    assertThatThrownBy(() -> AzureLogs.prefix(ACCOUNT + "/logs"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("no container and path");
  }

  /** A signature for the account, by its host as the ADLS FileIO names it, or by its name. */
  @Test
  void aSignatureIsFoundByTheAccountsHostOrName() {
    assertThat(AzureLogs.sas(Map.of("adls.sas-token.acct.dfs.core.windows.net", "?sv=1"), "acct"))
        .isEqualTo("sv=1");
    assertThat(AzureLogs.sas(Map.of("adls.sas-token.acct.blob.core.windows.net", "sv=2"), "acct"))
        .isEqualTo("sv=2");
    assertThat(AzureLogs.sas(Map.of("adls.sas-token.acct", "sv=3"), "acct")).isEqualTo("sv=3");
    assertThat(AzureLogs.sas(Map.of("adls.sas-token.other", "sv=4"), "acct")).isNull();
  }

  /**
   * A container's store is signed with the key of its own account, or a signature for it; a
   * container of an account the properties give nothing for fails, saying what to give.
   */
  @Test
  void aContainerNeedsCredentialsForItsOwnAccount() {
    Map<String, String> keyed = Map.of(AzureLogs.SHARED_KEY_ACCOUNT_NAME, "acct",
        AzureLogs.SHARED_KEY_ACCOUNT_KEY, "a2V5", AzureLogs.SAS_TOKEN_PREFIX + "signed", "sv=1");
    assertThat(AzureLogs.containers(keyed).apply(ACCOUNT + "/logs"))
        .isInstanceOf(AzureAppendBlobStore.class);
    assertThat(AzureLogs.containers(keyed).apply("https://signed.blob.core.windows.net/logs"))
        .isInstanceOf(AzureAppendBlobStore.class);
    assertThatThrownBy(
            () -> AzureLogs.containers(keyed).apply("https://other.blob.core.windows.net/logs"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("of the account other")
        .hasMessageContaining("no credentials")
        .hasMessageContaining("adls.sas-token.other");
    assertThatThrownBy(() -> AzureLogs.open(new InMemoryAtomicFileIO(), "mem:///wh/catalog",
            Map.of(LogCatalogProperties.LOG, LogCatalogProperties.LOG_AZURE_APPEND)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("account key");
  }

  // D13: where the catalog object says, whatever a client is configured with

  private final InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
  private final Map<String, InMemoryAppendBlobStore> containers = new TreeMap<>();

  /** The containers' stores in memory, by URL, as an account holds them. */
  private AppendBlobStore container(String url) {
    return containers.computeIfAbsent(url, u -> new InMemoryAppendBlobStore());
  }

  /** A catalog at {@code catalogLocation} whose log's segments are at {@code location}, or none. */
  private LogCatalog catalog(String catalogLocation, String location) {
    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, "mem:///warehouse");
    properties.put(LogCatalogProperties.LOG, LogCatalogProperties.LOG_AZURE_APPEND);
    properties.put(LogCatalogProperties.LOG_SEGMENT_RECORDS, "2"); // the log moves on often
    properties.put(LogCatalogProperties.LOG_CHECKPOINT_RECORDS, "5"); // and compacts
    properties.put(LogCatalogProperties.RACE_MIN_WAIT_MS, "0");
    if (location != null) {
      properties.put(LogCatalogProperties.LOG_LOCATION, location);
    }
    LogCatalog catalog = new LogCatalog("azure",
        AzureLogs.open(io, catalogLocation, this::container, properties), io, Maps.newHashMap());
    catalog.initialize("azure", properties);
    return catalog;
  }

  /** The names of the blobs in each container that holds any, by the container's URL. */
  private Map<String, List<String>> blobs() {
    Map<String, List<String>> out = new TreeMap<>();
    containers.forEach((url, store) -> {
      if (!store.names().isEmpty()) {
        out.put(url, store.names());
      }
    });
    return out;
  }

  /** Whether the catalog object at {@code location} records its segments' prefix. */
  private boolean recordsAPrefix(String location) {
    for (byte[] frame : AtomicFileLog.decode(io.bytes(location)).records()) {
      if (LogEnvelope.decodeRecord(frame).segmentPrefix() != null) {
        return true;
      }
    }
    return false;
  }

  /** Each catalog creates a table, then appends to every table: the log moves on and compacts. */
  private static List<TableIdentifier> commitThrough(List<LogCatalog> catalogs) {
    Namespace ns = Namespace.of("ns");
    List<TableIdentifier> tables = new ArrayList<>();
    for (int i = 0; i < catalogs.size(); i++) {
      TableIdentifier id = TableIdentifier.of(ns, "t" + i);
      catalogs.get(i).createTable(id, SCHEMA);
      tables.add(id);
    }
    for (LogCatalog catalog : catalogs) {
      for (TableIdentifier id : tables) {
        catalog.loadTable(id).updateProperties().set("by", catalog.toString()).commit();
      }
    }
    return tables;
  }

  /**
   * D13: clients configured with another location, or none, read and commit to the log the first
   * created: its catalog object records where its segments are, and nothing lands where the others
   * were configured. Each client was configured with the location before, and one given another
   * found no segment where it looked.
   */
  @Test
  void clientsConfiguredWithAnotherLocationOrNoneShareTheCreatorsLog() {
    String catalogLocation = "mem:///warehouse/catalog"; // no default: the object records it
    LogCatalog first = catalog(catalogLocation, ACCOUNT + "/c1/logs-a");
    first.createNamespace(Namespace.of("ns"));
    LogCatalog other = catalog(catalogLocation, ACCOUNT + "/c2/logs-b");
    LogCatalog none = catalog(catalogLocation, null);
    List<TableIdentifier> tables = commitThrough(List.of(first, other, none));
    for (LogCatalog catalog : List.of(first, other, none, catalog(catalogLocation, null))) {
      assertThat(catalog.listTables(Namespace.of("ns"))).containsExactlyInAnyOrderElementsOf(tables);
      for (TableIdentifier id : tables) {
        assertThat(catalog.loadTable(id).properties()).containsKey("by");
      }
    }
    assertThat(recordsAPrefix(catalogLocation)).isTrue();
    assertThat(blobs()).containsOnlyKeys(ACCOUNT + "/c1");
    assertThat(blobs().get(ACCOUNT + "/c1")).isNotEmpty()
        .allSatisfy(name -> assertThat(name).startsWith("logs-a/"));
  }

  /**
   * D13: a log created where no location is configured, of a catalog object on Azure, keeps its
   * segments at the default, beside the object, and its catalog object records nothing more;
   * clients configured with another location follow it.
   */
  @Test
  void aLogOnAzureWithoutALocationKeepsItsSegmentsBesideItsCatalogObject() {
    String catalogLocation = "abfss://lake@acct.dfs.core.windows.net/warehouse/catalog";
    LogCatalog first = catalog(catalogLocation, null);
    first.createNamespace(Namespace.of("ns"));
    LogCatalog other = catalog(catalogLocation, ACCOUNT + "/c2/logs-b");
    List<TableIdentifier> tables = commitThrough(List.of(first, other));
    assertThat(catalog(catalogLocation, null).listTables(Namespace.of("ns")))
        .containsExactlyInAnyOrderElementsOf(tables);
    assertThat(recordsAPrefix(catalogLocation)).isFalse();
    assertThat(blobs()).containsOnlyKeys(ACCOUNT + "/lake");
    assertThat(blobs().get(ACCOUNT + "/lake")).isNotEmpty()
        .allSatisfy(name -> assertThat(name).startsWith("warehouse/catalog.segments/"));
  }

  /**
   * A client without credentials for the account the catalog object names fails, saying which
   * account and what to give, before it sends a request.
   */
  @Test
  void aClientWithoutCredentialsForTheRecordedAccountFailsClearly() {
    String catalogLocation = "mem:///warehouse/catalog";
    LogCatalog first = catalog(catalogLocation, ACCOUNT + "/c1/logs-a");
    first.createNamespace(Namespace.of("ns"));
    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, "mem:///warehouse");
    properties.put(LogCatalogProperties.LOG, LogCatalogProperties.LOG_AZURE_APPEND);
    properties.put(LogCatalogProperties.LOG_LOCATION, "https://other.blob.core.windows.net/c/l");
    properties.put(AzureLogs.SHARED_KEY_ACCOUNT_NAME, "other");
    properties.put(AzureLogs.SHARED_KEY_ACCOUNT_KEY, "a2V5");
    assertThatThrownBy(() -> {
      LogCatalog stranger = new LogCatalog("azure", AzureLogs.open(io, catalogLocation, properties),
          io, Maps.newHashMap());
      stranger.initialize("azure", properties);
      stranger.listNamespaces();
    }).hasStackTraceContaining("of the account acct, and this catalog has no credentials for it");
  }
}
