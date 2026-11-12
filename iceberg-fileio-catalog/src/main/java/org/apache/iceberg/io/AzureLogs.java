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

import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.common.StorageSharedKeyCredential;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.intentlog.SharedLog;
import org.intentlog.azure.AppendBlobLog;
import org.intentlog.azure.AppendBlobStore;
import org.intentlog.azure.AzureAppendBlobStore;

/**
 * Opens a catalog's log on Azure Append Blobs ({@code fileio.catalog.log} {@code azure-append},
 * docs/AZURE_LOG.md "Configuration") from its properties. The catalog's only use of {@code
 * intentlog-azure}, which it depends on optionally: a catalog that keeps another log loads none of
 * it.
 */
public final class AzureLogs {

  /** The account key's properties, as Iceberg's ADLS FileIO names them ({@code AzureProperties}). */
  static final String SHARED_KEY_ACCOUNT_NAME = "adls.auth.shared-key.account.name";

  static final String SHARED_KEY_ACCOUNT_KEY = "adls.auth.shared-key.account.key";

  private AzureLogs() {}

  /** The log's options {@code properties} set (LogCatalogProperties' {@code fileio.catalog.log.*}). */
  public static AppendBlobLog.Options options(Map<String, String> properties) {
    AppendBlobLog.Options options = AppendBlobLog.Options.defaults();
    String append = properties.getOrDefault(LogCatalogProperties.LOG_APPEND, "blind");
    Preconditions.checkArgument(append.equals("blind") || append.equals("conditional"),
        "%s must be blind or conditional, got %s", LogCatalogProperties.LOG_APPEND, append);
    options = options.withConditional(append.equals("conditional"));
    String limit = properties.get(LogCatalogProperties.LOG_SEGMENT_MAX_BYTES);
    if (limit != null) {
      options = options.withSegmentLimit(Long.parseLong(limit));
    }
    String keep = properties.get(LogCatalogProperties.LOG_CHECKPOINT_KEEP);
    if (keep != null) {
      options = options.withCheckpointKeep(Integer.parseInt(keep));
    }
    String grace = properties.get(LogCatalogProperties.LOG_TRIM_GRACE_MS);
    if (grace != null) {
      options = options.withTrimGraceMs(Long.parseLong(grace));
    }
    return options;
  }

  /** The log at {@code root} of {@code store}, as {@code properties} configure it. */
  public static AppendBlobLog open(
      AppendBlobStore store, String root, Map<String, String> properties) {
    return new AppendBlobLog(store, root, options(properties));
  }

  /**
   * The URL of the container a log's location names: its scheme, host (or, on an emulator, host
   * and account) and container, without the path in it.
   *
   * @throws IllegalArgumentException if it names no container and path
   */
  static String containerUrl(String location) {
    BlobUrlParts parts = BlobUrlParts.parse(location);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(parts.getBlobContainerName())
            && !Strings.isNullOrEmpty(parts.getBlobName()),
        "%s names no container and path: %s", LogCatalogProperties.LOG_LOCATION, location);
    String url = parts.setBlobName(null).setSnapshot(null).setVersionId(null).toUrl().toString();
    return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
  }

  /**
   * The log {@code properties} locate ({@code fileio.catalog.log.location}, an {@code https} Blob
   * URL of a container and a path in it), signed with the account key they give.
   *
   * @throws IllegalArgumentException if they give no location or no account key
   */
  static SharedLog open(Map<String, String> properties) {
    String location = properties.get(LogCatalogProperties.LOG_LOCATION);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(location),
        "A catalog on an Azure log needs %s: https://<account>.blob.core.windows.net/<container>/"
            + "<path>", LogCatalogProperties.LOG_LOCATION);
    BlobUrlParts parts = BlobUrlParts.parse(location);
    String path = parts.getBlobName();
    String containerUrl = containerUrl(location);
    String account = properties.get(SHARED_KEY_ACCOUNT_NAME);
    String key = properties.get(SHARED_KEY_ACCOUNT_KEY);
    Preconditions.checkArgument(account != null && key != null,
        "A catalog on an Azure log needs an account key (%s and %s)", SHARED_KEY_ACCOUNT_NAME,
        SHARED_KEY_ACCOUNT_KEY);
    AzureAppendBlobStore store =
        AzureAppendBlobStore.create(containerUrl, new StorageSharedKeyCredential(account, key));
    return open(store, path, properties);
  }
}
