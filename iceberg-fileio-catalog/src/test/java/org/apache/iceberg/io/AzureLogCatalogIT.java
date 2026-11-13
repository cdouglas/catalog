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

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Iceberg's catalog and catalog-transaction suites ({@link TestLocalCatalog}, {@link
 * TestLocalCatalogTransaction}) on a catalog whose log is on Append Blobs of a real account
 * (docs/AZURE_LOG.md "Testing", steps 4 and 5): a {@link FileIOCatalog} that opens its log from its
 * properties ({@code fileio.catalog.log} {@code azure-append}, an account key), as a deployment's
 * does, with blind appends, in each table metadata storage. The tables' files stay in memory.
 * Segments of 1 KiB (2 KiB for inline tables) move the log on every few records, closing them with
 * real pads, and a trim grace of 0 lets a sweep delete trimmed segments that other instances'
 * cached heads still name.
 *
 * <p>Run explicitly, with {@code AZURE_LOG_IT_CONTAINER_URL} (a container's URL on the primary
 * endpoint) and {@code AZURE_STORAGE_KEY} (its account key) set: {@code mvn -o -pl
 * iceberg-fileio-catalog test -Dtest='AzureLogCatalogIT*'}. Each run writes under {@code
 * catalog-it/<UTC>-<random>}, a log per catalog of each test, and deletes it afterwards.
 */
@EnabledIfEnvironmentVariable(named = "AZURE_LOG_IT_CONTAINER_URL", matches = ".+")
class AzureLogCatalogIT {

  private static final String URL = System.getenv("AZURE_LOG_IT_CONTAINER_URL");
  private static final String KEY = System.getenv("AZURE_STORAGE_KEY");
  private static final String RUN = "catalog-it/"
      + DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC)
          .format(Instant.now())
      + "-" + Long.toHexString(ThreadLocalRandom.current().nextLong() >>> 16);

  /**
   * A segment's limit: a few of the suites' records, so that their logs move on. Records of
   * inline tables are bigger; a record no segment holds would be rejected (TOO_LARGE).
   */
  private static long segmentBytes(LocalCatalogMode mode) {
    return mode.inlineTM ? 2 << 10 : 1 << 10;
  }

  /**
   * The log of the catalog {@code name} in {@code warehouse} on the account: under this run's
   * prefix, the warehouse's and the catalog's names, so that catalogs of one name in one test share
   * a log.
   */
  static Map<String, String> onAccount(LocalCatalogMode mode, String warehouse, String name) {
    String root = RUN + "/" + warehouse.replaceFirst("^mem:///", "") + "/catalog-" + name;
    Map<String, String> properties = new HashMap<>();
    properties.put(LogCatalogProperties.LOG, LogCatalogProperties.LOG_AZURE_APPEND);
    properties.put(LogCatalogProperties.LOG_LOCATION, trimSlash(URL) + "/" + root);
    properties.put(LogCatalogProperties.LOG_APPEND, "blind");
    properties.put(LogCatalogProperties.LOG_SEGMENT_MAX_BYTES,
        String.valueOf(segmentBytes(mode)));
    properties.put(LogCatalogProperties.LOG_TRIM_GRACE_MS, "0");
    properties.put(AzureLogs.SHARED_KEY_ACCOUNT_NAME, BlobUrlParts.parse(URL).getAccountName());
    properties.put(AzureLogs.SHARED_KEY_ACCOUNT_KEY, KEY);
    return properties;
  }

  private static String trimSlash(String url) {
    return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
  }

  /**
   * Deletes every blob under this run's prefix, then the directories a hierarchical namespace made
   * for them, deepest first, and the prefix's own while they are empty.
   */
  @AfterAll
  static void deleteWhatItWrote() {
    BlobContainerClient c = new BlobContainerClientBuilder().endpoint(URL)
        .credential(new StorageSharedKeyCredential(BlobUrlParts.parse(URL).getAccountName(), KEY))
        .buildClient();
    List<String> files = new ArrayList<>();
    List<String> folders = new ArrayList<>();
    for (BlobItem item : c.listBlobs(new ListBlobsOptions().setPrefix(RUN)
        .setDetails(new BlobListDetails().setRetrieveMetadata(true)), null)) {
      boolean folder = item.getMetadata() != null
          && "true".equalsIgnoreCase(item.getMetadata().get("hdi_isfolder"));
      (folder ? folders : files).add(item.getName());
    }
    // A log's newest segment outlives the sweeps: its ordinal is how often the log moved on
    Map<String, Long> last = new HashMap<>();
    for (String f : files) {
      int seg = f.indexOf("/seg/");
      if (seg >= 0) {
        long ordinal = Long.parseLong(f.substring(seg + 5, f.indexOf('-', seg + 5)));
        last.merge(f.substring(0, seg), ordinal, Math::max);
      }
    }
    long logs = files.stream().filter(f -> f.endsWith("/head")).count();
    long moved = last.values().stream().mapToLong(Long::longValue).sum();
    for (String f : files) {
      c.getBlobClient(f).deleteIfExists();
    }
    System.out.println(logs + " logs moved on " + moved + " times, "
        + last.values().stream().filter(o -> o > 0).count() + " of them at least once");
    folders.sort(Comparator.comparingInt((String f) -> f.split("/").length).reversed());
    for (String dir = RUN; !dir.isEmpty();
        dir = dir.contains("/") ? dir.substring(0, dir.lastIndexOf('/')) : "") {
      folders.add(dir);
    }
    for (String f : folders) {
      try {
        c.getBlobClient(f).deleteIfExists();
      } catch (RuntimeException e) {
        // not empty (another run's is still there), or not a directory: harmless
      }
    }
    System.out.println("deleted " + files.size() + " blobs under " + RUN);
  }

  @Nested
  class Catalog extends TestLocalCatalog.Suite {
    @Override
    LocalCatalogMode mode() {
      return LocalCatalogMode.AZLOG;
    }

    @Override
    Map<String, String> logProperties(String warehouse, String name) {
      return onAccount(mode(), warehouse, name);
    }
  }

  @Nested
  class CatalogInlineTM extends TestLocalCatalog.Suite {
    @Override
    LocalCatalogMode mode() {
      return LocalCatalogMode.AZLOG_TM;
    }

    @Override
    Map<String, String> logProperties(String warehouse, String name) {
      return onAccount(mode(), warehouse, name);
    }
  }

  @Nested
  class CatalogInlineML extends TestLocalCatalog.Suite {
    @Override
    LocalCatalogMode mode() {
      return LocalCatalogMode.AZLOG_ML;
    }

    @Override
    Map<String, String> logProperties(String warehouse, String name) {
      return onAccount(mode(), warehouse, name);
    }
  }

  @Nested
  class Transactions extends TestLocalCatalogTransaction.Suite {
    @Override
    LocalCatalogMode mode() {
      return LocalCatalogMode.AZLOG;
    }

    @Override
    Map<String, String> logProperties(String warehouse, String name) {
      return onAccount(mode(), warehouse, name);
    }
  }

  @Nested
  class TransactionsInlineTM extends TestLocalCatalogTransaction.Suite {
    @Override
    LocalCatalogMode mode() {
      return LocalCatalogMode.AZLOG_TM;
    }

    @Override
    Map<String, String> logProperties(String warehouse, String name) {
      return onAccount(mode(), warehouse, name);
    }
  }

  @Nested
  class TransactionsInlineML extends TestLocalCatalogTransaction.Suite {
    @Override
    LocalCatalogMode mode() {
      return LocalCatalogMode.AZLOG_ML;
    }

    @Override
    Map<String, String> logProperties(String warehouse, String name) {
      return onAccount(mode(), warehouse, name);
    }
  }
}
