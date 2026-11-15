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

import com.azure.core.credential.AzureSasCredential;
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
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import org.intentlog.fileio.SegmentedLogHistory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Iceberg's catalog and catalog-transaction suites ({@link TestLocalCatalog}, {@link
 * TestLocalCatalogTransaction}) on a catalog whose log's segments are Append Blobs of a real
 * account (docs/LOG_SEGMENTS.md "Testing"): a {@link FileIOCatalog} that opens its log from its
 * properties ({@code fileio.catalog.log} {@code azure-append}, an account key or a shared access
 * signature), as a deployment's does, in each table metadata storage. The catalog object and the
 * tables' files stay in memory. Segments of {@link #SEGMENT_RECORDS} records move the log on every
 * few commits, and a retirement TTL of 0 lets a sweep delete retired segments that other
 * instances' catalog objects still name.
 *
 * <p>Run explicitly, with {@code AZURE_LOG_IT_CONTAINER_URL} (a container's URL on the primary
 * endpoint) and {@code AZURE_STORAGE_KEY} (its account key) or {@code AZURE_STORAGE_SAS} (a
 * signature for its objects) set: {@code mvn -o -pl iceberg-fileio-catalog test
 * -Dtest='AzureLogCatalogIT*'}. Each run writes under {@code catalog-it/<UTC>-<random>}, a log per
 * catalog of each test, and deletes it afterwards: by listing the prefix, or, with a signature
 * that cannot list, by the names every catalog object installed or retired (a sweep that cannot
 * list deletes nothing, so an installer's blob that lost its Install, which it deletes itself,
 * is all that could be left).
 */
@EnabledIfEnvironmentVariable(named = "AZURE_LOG_IT_CONTAINER_URL", matches = ".+")
class AzureLogCatalogIT {

  private static final String URL = System.getenv("AZURE_LOG_IT_CONTAINER_URL");
  private static final String KEY = System.getenv("AZURE_STORAGE_KEY");
  private static final String SAS = System.getenv("AZURE_STORAGE_SAS");
  private static final String RUN = "catalog-it/"
      + DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC)
          .format(Instant.now())
      + "-" + Long.toHexString(ThreadLocalRandom.current().nextLong() >>> 16);

  /** K: a few of the suites' records, so that their logs move on to new segments. */
  private static final int SEGMENT_RECORDS = 4;

  /** Every segment a test's catalog objects named, as a blob name, for the cleanup. */
  private static final Set<String> NAMED = new TreeSet<>();

  /** The prefix of the log of the catalog at {@code location}, a warehouse's catalog. */
  private static String root(String location) {
    return RUN + "/" + location.replaceFirst("^mem:///", "");
  }

  /**
   * The log of the catalog {@code name} in {@code warehouse} on the account: under this run's
   * prefix, the warehouse's and the catalog's names, so that catalogs of one name in one test share
   * a log.
   */
  static Map<String, String> onAccount(LocalCatalogMode mode, String warehouse, String name) {
    Map<String, String> properties = new HashMap<>();
    properties.put(LogCatalogProperties.LOG, LogCatalogProperties.LOG_AZURE_APPEND);
    properties.put(LogCatalogProperties.LOG_LOCATION,
        trimSlash(URL) + "/" + root(warehouse + "/catalog-" + name));
    properties.put(LogCatalogProperties.LOG_SEGMENT_RECORDS, String.valueOf(SEGMENT_RECORDS));
    properties.put(LogCatalogProperties.LOG_RETIRE_TTL_MS, "0");
    String account = BlobUrlParts.parse(URL).getAccountName();
    if (KEY != null && !KEY.isEmpty()) {
      properties.put(AzureLogs.SHARED_KEY_ACCOUNT_NAME, account);
      properties.put(AzureLogs.SHARED_KEY_ACCOUNT_KEY, KEY);
    } else {
      properties.put(AzureLogs.SAS_TOKEN_PREFIX + account, SAS);
    }
    return properties;
  }

  private static String trimSlash(String url) {
    return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
  }

  /** Keeps the names of the segments the catalog objects in {@code io} ever named. */
  static void collect(InMemoryAtomicFileIO io) {
    for (String location : io.conditionalWrites().keySet()) {
      List<byte[]> versions = new ArrayList<>();
      for (InMemoryAtomicFileIO.Write w : io.history()) {
        if (w.location().equals(location) && CheckpointRoundTrip.segmented(w.after())) {
          versions.add(w.after());
        }
      }
      if (!versions.isEmpty()) {
        String root = root(location);
        new SegmentedLogHistory(versions, List.of(), root, Map.of()).names()
            .forEach(n -> NAMED.add(root + "/" + n));
      }
    }
  }

  private static BlobContainerClient container() {
    BlobContainerClientBuilder builder = new BlobContainerClientBuilder().endpoint(URL);
    if (KEY != null && !KEY.isEmpty()) {
      builder.credential(
          new StorageSharedKeyCredential(BlobUrlParts.parse(URL).getAccountName(), KEY));
    } else {
      builder.credential(new AzureSasCredential(SAS.startsWith("?") ? SAS.substring(1) : SAS));
    }
    return builder.buildClient();
  }

  /**
   * Deletes every blob under this run's prefix (listed, or else by the names its catalogs named),
   * then the directories a hierarchical namespace made for them, deepest first, and the prefix's
   * own while they are empty.
   */
  @AfterAll
  static void deleteWhatItWrote() {
    BlobContainerClient c = container();
    List<String> files = new ArrayList<>();
    Set<String> folders = new TreeSet<>();
    boolean listed = true;
    try {
      for (BlobItem item : c.listBlobs(new ListBlobsOptions().setPrefix(RUN)
          .setDetails(new BlobListDetails().setRetrieveMetadata(true)), null)) {
        boolean folder = item.getMetadata() != null
            && "true".equalsIgnoreCase(item.getMetadata().get("hdi_isfolder"));
        (folder ? folders : files).add(item.getName());
      }
    } catch (RuntimeException e) {
      listed = false; // a signature that cannot list the container
      files.addAll(NAMED);
    }
    // A log's newest segment outlives the sweeps: its ordinal is how often the log moved on
    Map<String, Long> last = new HashMap<>();
    for (String f : files) {
      int seg = f.indexOf("/seg-");
      if (seg >= 0) {
        long ordinal = Long.parseLong(f.substring(seg + 5, f.indexOf('-', seg + 5)));
        last.merge(f.substring(0, seg), ordinal, Math::max);
        // the directories above it, which a hierarchical namespace made
        for (String dir = f.substring(0, seg); dir.contains("/");
            dir = dir.substring(0, dir.lastIndexOf('/'))) {
          folders.add(dir);
        }
      }
    }
    int deleted = 0;
    for (String f : files) {
      if (c.getBlobClient(f).deleteIfExists()) {
        deleted++;
      }
    }
    System.out.println(last.size() + " logs moved on "
        + last.values().stream().mapToLong(Long::longValue).sum() + " times, "
        + last.values().stream().filter(o -> o > 0).count() + " of them at least once");
    List<String> dirs = new ArrayList<>(folders);
    dirs.sort(Comparator.comparingInt((String f) -> f.split("/").length).reversed());
    for (String dir = RUN; !dir.isEmpty();
        dir = dir.contains("/") ? dir.substring(0, dir.lastIndexOf('/')) : "") {
      dirs.add(dir);
    }
    for (String f : dirs) {
      try {
        c.getBlobClient(f).deleteIfExists();
      } catch (RuntimeException e) {
        // not empty (another run's is still there), or not a directory: harmless
      }
    }
    System.out.println("deleted " + deleted + " blobs under " + RUN
        + (listed ? " (listed)" : " (by name: the signature cannot list)"));
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

    @AfterEach
    void collectSegments() {
      collect(io());
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

    @AfterEach
    void collectSegments() {
      collect(io());
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

    @AfterEach
    void collectSegments() {
      collect(io());
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

    @AfterEach
    void collectSegments() {
      collect(io());
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

    @AfterEach
    void collectSegments() {
      collect(io());
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

    @AfterEach
    void collectSegments() {
      collect(io());
    }
  }
}
