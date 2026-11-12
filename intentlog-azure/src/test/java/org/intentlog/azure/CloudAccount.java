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
package org.intentlog.azure;

import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The account the cloud tests run on: {@code AZURE_LOG_IT_CONTAINER_URL} (a container's URL on
 * the primary endpoint) and {@code AZURE_STORAGE_KEY}, its account key. Unset, the tests do
 * nothing. Each run writes under its own prefix, {@code intentlog-it/<UTC>-<random>}, and deletes
 * it afterwards.
 */
final class CloudAccount {

  static final String URL = System.getenv("AZURE_LOG_IT_CONTAINER_URL");
  static final String KEY = System.getenv("AZURE_STORAGE_KEY");
  static final String RUN = "intentlog-it/"
      + DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC)
          .format(Instant.now())
      + "-" + Long.toHexString(ThreadLocalRandom.current().nextLong() >>> 16);

  private static AzureAppendBlobStore store;

  private CloudAccount() {}

  static boolean configured() {
    return URL != null && !URL.isEmpty() && KEY != null && !KEY.isEmpty();
  }

  private static StorageSharedKeyCredential credential() {
    return new StorageSharedKeyCredential(BlobUrlParts.parse(URL).getAccountName(), KEY);
  }

  /** A client of the account's container, one try per request, through {@code policies}. */
  static BlobContainerClient container(HttpPipelinePolicy... policies) {
    BlobContainerClientBuilder builder = new BlobContainerClientBuilder().endpoint(URL)
        .credential(credential())
        .retryOptions(
            new RequestRetryOptions(RetryPolicyType.FIXED, 1, (Duration) null, null, null, null));
    for (HttpPipelinePolicy policy : policies) {
      builder.addPolicy(policy);
    }
    return builder.buildClient();
  }

  /** The store on the account: one try per request. */
  static synchronized AzureAppendBlobStore store() {
    if (store == null) {
      store = AzureAppendBlobStore.create(URL, credential());
    }
    return store;
  }

  /**
   * Deletes every blob under {@code prefix}, then the directories a hierarchical namespace made
   * for them, deepest first, and the directories above it that are left empty. Returns how many
   * blobs it deleted.
   */
  static int deleteAll(String prefix) {
    BlobContainerClient c = new BlobContainerClientBuilder().endpoint(URL)
        .credential(credential()).buildClient();
    List<String> files = new ArrayList<>();
    List<String> folders = new ArrayList<>();
    for (BlobItem item : c.listBlobs(new ListBlobsOptions().setPrefix(prefix)
        .setDetails(new BlobListDetails().setRetrieveMetadata(true)), null)) {
      boolean folder = item.getMetadata() != null
          && "true".equalsIgnoreCase(item.getMetadata().get("hdi_isfolder"));
      (folder ? folders : files).add(item.getName());
    }
    for (String f : files) {
      c.getBlobClient(f).deleteIfExists();
    }
    folders.sort(Comparator.comparingInt((String f) -> f.split("/").length).reversed());
    for (String f : folders) {
      try {
        c.getBlobClient(f).deleteIfExists();
      } catch (RuntimeException e) {
        // a directory left: empty, and harmless
      }
    }
    // the prefix's own directory, then each directory above it, while they are empty
    for (String dir = prefix; !dir.isEmpty();
        dir = dir.contains("/") ? dir.substring(0, dir.lastIndexOf('/')) : "") {
      try {
        c.getBlobClient(dir).deleteIfExists();
      } catch (RuntimeException e) {
        break; // not empty: another test's, or another run's, is still there
      }
    }
    return files.size();
  }
}
