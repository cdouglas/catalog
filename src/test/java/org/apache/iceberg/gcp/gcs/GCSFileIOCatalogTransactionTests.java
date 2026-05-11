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
package org.apache.iceberg.gcp.gcs;

import static org.assertj.core.api.Assertions.setMaxStackTraceElementsDisplayed;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.Random;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.CatalogTransactionTests;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.CloudMode;
import org.apache.iceberg.io.FileIOCatalog;
import org.apache.iceberg.io.ProtoCatalogFormat;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class GCSFileIOCatalogTransactionTests extends CatalogTransactionTests<FileIOCatalog> {
  private static final String TEST_BUCKET = "lst-consistency/TEST_BUCKET";
  private static final String GCS_BUCKET = "lst-consistency";
  private static final Logger LOG = LoggerFactory.getLogger(GCSCatalogTest.class);
  private static final String FAKE_GCS_IMAGE = "fsouza/fake-gcs-server:1.49.2";
  private static final int FAKE_GCS_PORT = 4443;

  protected static CloudMode mode;
  private static Storage storage;
  private static GenericContainer<?> fakeGcsContainer;
  private FileIOCatalog catalog;
  private static String warehouseLocation;
  private static String uniqTestRun;

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
    try (GCSFileIO io = new GCSFileIO(() -> storage, new GCPProperties())) {
      if (io.listPrefix(warehouseLocation).iterator().hasNext()) {
        io.deletePrefix(warehouseLocation);
      }
    }
  }

  @BeforeAll
  public static void initStorage() throws IOException {
    uniqTestRun = randomAlphabetic(8);
    LOG.info("TEST RUN: " + uniqTestRun);
    String credsPath = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
    File credFile = credsPath != null ? new File(credsPath) : null;
    if (credFile != null && credFile.exists()) {
      mode = CloudMode.REAL_GCS;
      try (FileInputStream creds = new FileInputStream(credFile)) {
        storage = RemoteStorageHelper.create(GCS_BUCKET, creds).getOptions().getService();
        LOG.info("Using remote GCS (creds from {})", credsPath);
      }
    } else {
      mode = CloudMode.FAKE_GCS;
      fakeGcsContainer =
          new GenericContainer<>(DockerImageName.parse(FAKE_GCS_IMAGE))
              .withExposedPorts(FAKE_GCS_PORT)
              .withCommand(
                  "-scheme", "http",
                  "-port", String.valueOf(FAKE_GCS_PORT));
      fakeGcsContainer.start();
      String endpoint =
          "http://"
              + fakeGcsContainer.getHost()
              + ":"
              + fakeGcsContainer.getMappedPort(FAKE_GCS_PORT);
      updateFakeGcsExternalUrl(endpoint);
      storage =
          StorageOptions.newBuilder()
              .setHost(endpoint)
              .setProjectId("test-project")
              .setCredentials(NoCredentials.getInstance())
              .build()
              .getService();
      try {
        storage.create(BucketInfo.of(GCS_BUCKET));
      } catch (StorageException e) {
        if (e.getCode() != 409) { // already exists
          throw e;
        }
      }
      LOG.info("Using fake-gcs-server Testcontainers emulator at {}", endpoint);
    }
    // show ridiculous stack traces
    setMaxStackTraceElementsDisplayed(Integer.MAX_VALUE);
  }

  @AfterAll
  public static void tearDownStorage() {
    if (fakeGcsContainer != null) {
      fakeGcsContainer.stop();
      fakeGcsContainer = null;
    }
  }

  private static void updateFakeGcsExternalUrl(String externalUrl) throws IOException {
    URL url = new URL(externalUrl + "/_internal/config");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("PUT");
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setDoOutput(true);
    byte[] body = ("{\"externalUrl\":\"" + externalUrl + "\"}").getBytes();
    try (OutputStream out = conn.getOutputStream()) {
      out.write(body);
    }
    int code = conn.getResponseCode();
    if (code / 100 != 2) {
      throw new IOException("fake-gcs-server /_internal/config returned HTTP " + code);
    }
  }

  @BeforeEach
  public void before(TestInfo info) {
    // XXX don't call io.initialize(), as it will overwrite this config
    GCSFileIO io = new GCSFileIO(() -> storage, new GCPProperties());

    final String testName = info.getTestMethod().orElseThrow(RuntimeException::new).getName();
    warehouseLocation = "gs://" + TEST_BUCKET + "/" + uniqTestRun + "/" + testName;
    cleanupWarehouseLocation();
    final Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
    // GCSFileIO.supportsAppend() returns false, so the format auto-coerces every
    // commit to CAS regardless of fileio.catalog.max.append.count.
    properties.put("fileio.catalog.inline", String.valueOf(inlineTM()));
    properties.put("fileio.catalog.inline.manifests", String.valueOf(inlineML()));
    final String location = warehouseLocation + "/catalog";
    catalog =
        new FileIOCatalog(
            "test", location, new ProtoCatalogFormat(properties), io, Maps.newHashMap());
    catalog.initialize(testName, properties);
  }

  @Override
  protected FileIOCatalog catalog() {
    return catalog;
  }

  static final Random RANDOM = new Random();

  static String randomAlphabetic(int count) {
    assert count >= 0;
    StringBuilder ret = new StringBuilder();
    for (int i = 0; i < count; ++i) {
      ret.append((char) ('a' + RANDOM.nextInt(26)));
    }
    return ret.toString();
  }
}
