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

import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;

/**
 * Drives the abstract {@link CatalogTests} suite against {@link FileIOCatalog} configured
 * with TM+ML inline mode, backed by the in-memory {@code MemoryFileIO} from
 * {@link TestInlineManifestEndToEnd}. The cloud {@code *CatalogInlineML} subclasses are
 * the only tests that exercise this surface against real storage; this driver runs the
 * same suite without credentials so concurrent-replace and other regressions surface
 * locally instead of only in the 55-min cloud loop.
 */
public class TestFileIOCatalogInlineMLLocal extends CatalogTests<FileIOCatalog> {

  private TestInlineManifestEndToEnd.MemoryFileIO io;
  private FileIOCatalog catalog;
  private String warehouseLocation;

  @BeforeEach
  public void before() {
    io = new TestInlineManifestEndToEnd.MemoryFileIO();
    warehouseLocation = "mem:///warehouse-" + UUID.randomUUID();
    catalog = buildCatalog("test", Maps.newHashMap());
  }

  private FileIOCatalog buildCatalog(String catalogName, Map<String, String> extra) {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
    properties.put("fileio.catalog.inline", "true");
    properties.put("fileio.catalog.inline.manifests", "true");
    properties.put(CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key1", "catalog-default-key1");
    properties.put(CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key2", "catalog-default-key2");
    properties.put(CatalogProperties.TABLE_DEFAULT_PREFIX + "override-key3", "catalog-default-key3");
    properties.put(CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key3", "catalog-override-key3");
    properties.put(CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key4", "catalog-override-key4");
    properties.putAll(extra);
    String location = warehouseLocation + "/catalog-" + catalogName;
    CatalogFormat<?, ?> format = new ProtoCatalogFormat(properties);
    FileIOCatalog c = new FileIOCatalog(catalogName, location, format, io, Maps.newHashMap());
    c.initialize(catalogName, properties);
    return c;
  }

  @Override
  protected FileIOCatalog catalog() {
    return catalog;
  }

  @Override
  protected FileIOCatalog initCatalog(String catalogName, Map<String, String> additionalProperties) {
    return buildCatalog(catalogName, additionalProperties);
  }

  @Override
  protected String baseTableLocation(TableIdentifier identifier) {
    return warehouseLocation + "/" + identifier.namespace() + "/" + identifier.name();
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsNestedNamespaces() {
    return true;
  }

  // drop+registerTable requires register-from-bytes API for inline TM (errata T4);
  // no equivalent on-disk metadata.json exists after drop. Same disable as the
  // cloud TM/TM+ML drivers.
  @Override
  @org.junit.jupiter.api.Test
  @org.junit.jupiter.api.Disabled("inline TM has no metadata.json to re-register; errata T4")
  public void testRegisterTable() {}

  // The metadata-file-locations API is meaningless for inline tables (no
  // metadata.json files on disk to track). Use the inline-aware variant.
  @Override
  @org.junit.jupiter.api.Test
  public void testMetadataFileLocationsRemovalAfterCommit() {
    InlineCompatAssertions.runMetadataFileLocationsRemovalAfterCommit(
        catalog(), TABLE, NS, SCHEMA, requiresNamespaceCreate());
  }

}
