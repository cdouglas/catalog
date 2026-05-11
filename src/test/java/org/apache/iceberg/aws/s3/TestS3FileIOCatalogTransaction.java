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
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.catalog.CatalogTransactionTests;
import org.apache.iceberg.io.CatalogFormat;
import org.apache.iceberg.io.CloudMode;
import org.apache.iceberg.io.FileIOCatalog;
import org.apache.iceberg.io.IntegTestEnv;
import org.apache.iceberg.io.ProtoCatalogFormat;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.testcontainers.containers.MinIOContainer;
import software.amazon.awssdk.services.s3.S3Client;

@ExtendWith(TestS3Catalog.SuccessCleanupExtension.class)
public class TestS3FileIOCatalogTransaction extends CatalogTransactionTests<FileIOCatalog> {

  protected static CloudMode mode;
  private static MinIOContainer minioContainer;
  private static S3Client s3Client;
  private static String testBucket;
  private static String uniqTestRun;
  private static String warehouseLocation;

  private FileIOCatalog catalog;

  /**
   * Catalog format's max.append.count for this suite. Subclasses override to flip the
   * commit policy: 10000 (default) exercises the append branch on S3 Express One Zone;
   * 0 forces CAS on every commit.
   */
  protected int maxAppendCount() {
    return 10_000;
  }

  /** Whether the catalog inlines TableMetadata. Default off; subclasses opt in. */
  protected boolean inlineTM() {
    return false;
  }

  /**
   * Whether the catalog inlines manifest lists. Requires {@link #inlineTM()} to be true; the
   * format rejects {@code inline.manifests=true} without {@code inline=true}.
   */
  protected boolean inlineML() {
    return false;
  }

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
    System.err.println("TEST RUN: " + uniqTestRun); // (logging disabled in tests)
    if (IntegTestEnv.isSet(IntegTestEnv.AWS_ACCESS_KEY_ID)) {
      mode = CloudMode.REAL_S3;
      s3Client = AwsClientFactories.defaultFactory().s3();
      testBucket = IntegTestEnv.require(IntegTestEnv.AWS_TEST_BUCKET);
      System.err.println("Using real S3, bucket=" + testBucket);
    } else if (IntegTestEnv.requireRealCloud()) {
      throw new IllegalStateException(
          "-Preal-cloud requires "
              + IntegTestEnv.AWS_ACCESS_KEY_ID
              + " and "
              + IntegTestEnv.AWS_TEST_BUCKET
              + ". See docs/INTEGRATION_TESTING.md.");
    } else {
      mode = CloudMode.MINIO;
      minioContainer = MinioUtil.createContainer();
      minioContainer.start();
      s3Client = MinioUtil.createS3Client(minioContainer);
      testBucket = "casalog-" + UUID.randomUUID().toString().substring(0, 8);
      s3Client.createBucket(b -> b.bucket(testBucket));
      System.err.println("Using MinIO Testcontainers emulator, bucket=" + testBucket);
    }
  }

  @AfterAll
  public static void tearDownStorage() {
    if (s3Client != null) {
      s3Client.close();
      s3Client = null;
    }
    if (minioContainer != null) {
      minioContainer.stop();
      minioContainer = null;
    }
  }

  @BeforeEach
  public void before(TestInfo info) {
    if (mode == CloudMode.MINIO && maxAppendCount() > 0) {
      mode.assumeRealCloud("S3 Express writeOffsetBytes (APPEND-mode commits)");
    }
    final String testName = info.getTestMethod().orElseThrow(RuntimeException::new).getName();
    warehouseLocation =
        "s3://" + testBucket + "/" + uniqTestRun + "/" + testName + "_" + info.getDisplayName();
    cleanupWarehouseLocation();

    final S3Client client = s3Client;
    final S3FileIO io = new S3FileIO(() -> client);
    io.initialize(Maps.newHashMap());
    final String location = warehouseLocation + "/catalog";

    final Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
    properties.put("fileio.catalog.max.append.count", String.valueOf(maxAppendCount()));
    properties.put("fileio.catalog.inline", String.valueOf(inlineTM()));
    properties.put("fileio.catalog.inline.manifests", String.valueOf(inlineML()));
    final CatalogFormat<?, ?> format = new ProtoCatalogFormat(properties);
    catalog = new FileIOCatalog("test", location, format, io, Maps.newHashMap());
    catalog.initialize(testName, properties);
  }

  @AfterEach
  public void after() {
    // TODO
  }

  @Override
  protected FileIOCatalog catalog() {
    return catalog;
  }
}
