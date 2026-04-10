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
package org.apache.iceberg.aws.s3;

import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.io.CatalogFormat;
import org.apache.iceberg.io.FileIOCatalog;
import org.apache.iceberg.io.ProtoCatalogFormat;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

@ExtendWith(TestS3Catalog.SuccessCleanupExtension.class)
public class TestS3Catalog extends CatalogTests<FileIOCatalog> {
  private static final String TEST_BUCKET = "casalog";
  private static final String EXPR_BUCKET = "lst-pbafvfgrapl--usw2-az3--x-s3";

  private static String uniqTestRun;
  private static String warehouseLocation;

  private FileIOCatalog catalog;

  // Don't keep artifacts from successful tests
  static class SuccessCleanupExtension implements TestWatcher {
    @Override
    public void testSuccessful(ExtensionContext ctxt) {
      cleanupWarehouseLocation();
    }
  }

  static void cleanupWarehouseLocation() {
    // TODO use FileIO
  }

  @BeforeAll
  public static void initStorage() {
    uniqTestRun = UUID.randomUUID().toString();
    // LOG.info("TEST RUN: {}", uniqTestRun);
    System.err.println("TEST RUN: " + uniqTestRun); // (logging disabled in tests)
  }

  @BeforeEach
  public void before(TestInfo info) {
    final String testName = info.getTestMethod().orElseThrow(RuntimeException::new).getName();
    warehouseLocation =
        "s3://" + EXPR_BUCKET + "/" + uniqTestRun + "/" + testName + "_" + info.getDisplayName();
    cleanupWarehouseLocation();

    // s3 = AwsClientFactories.defaultFactory().s3();
    // StaticClientFactory.client = s3;
    final S3FileIO io = new S3FileIO();
    io.initialize(Maps.newHashMap());
    final String location = warehouseLocation + "/catalog";
    // Note from:
    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-tutorial-Upload.html
    // "If you're uploading a single object that's less than 16 MB in size, you can also specify a
    // pre-calculated checksum value. When you provide a pre-calculated value, Amazon S3 compares it
    // with the value that it calculates by using the selected checksum function. If the values
    // don't match, the upload won't start."
    final CatalogFormat<?, ?> format = new ProtoCatalogFormat();
    catalog = new FileIOCatalog("test", location, null, format, io, Maps.newHashMap());

    final Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
    catalog.initialize(testName, properties);
  }

  @AfterEach
  public void after() {
    // TODO
  }

  @Override
  protected String cannedTableLocation() {
    return warehouseLocation + "/tmp/ns/table";
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsConcurrentCreate() {
    return false;
  }

  @Override
  protected FileIOCatalog catalog() {
    return catalog;
  }
}
