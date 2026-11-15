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
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.common.StorageSharedKeyCredential;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.intentlog.SharedLog;
import org.intentlog.azure.AppendBlobSegments;
import org.intentlog.azure.AppendBlobStore;
import org.intentlog.azure.AzureAppendBlobStore;
import org.intentlog.fileio.AtomicFileLog;
import org.intentlog.fileio.SegmentedLog;

/**
 * Opens a catalog's segmented log on Azure Append Blobs ({@code fileio.catalog.log} {@code
 * azure-append}, docs/LOG_SEGMENTS.md and docs/AZURE_LOG.md "Configuration") from its properties:
 * the catalog object at the catalog's location in its FileIO, in format 4, and the segments
 * Append Blobs at {@code fileio.catalog.log.location}. The catalog's only use of {@code
 * intentlog-azure}, which it depends on optionally: a catalog that keeps another log loads none of
 * it.
 */
public final class AzureLogs {

  /** The account key's properties, as Iceberg's ADLS FileIO names them ({@code AzureProperties}). */
  static final String SHARED_KEY_ACCOUNT_NAME = "adls.auth.shared-key.account.name";

  static final String SHARED_KEY_ACCOUNT_KEY = "adls.auth.shared-key.account.key";

  /**
   * A shared access signature's property, as Iceberg's ADLS FileIO names it, followed by the
   * account's host ({@code <account>.dfs.core.windows.net} or {@code .blob.}) or its name.
   */
  static final String SAS_TOKEN_PREFIX = "adls.sas-token.";

  private AzureLogs() {}

  /** The log's options {@code properties} set (LogCatalogProperties' {@code fileio.catalog.log.*}). */
  public static SegmentedLog.Options options(Map<String, String> properties) {
    SegmentedLog.Options options = SegmentedLog.Options.defaults();
    String records = properties.get(LogCatalogProperties.LOG_SEGMENT_RECORDS);
    if (records != null) {
      options = options.withLimit(Long.parseLong(records));
    }
    String ttl = properties.get(LogCatalogProperties.LOG_RETIRE_TTL_MS);
    if (ttl != null) {
      options = options.withRetireTtlMs(Long.parseLong(ttl));
    }
    return options.withCopies(
        Boolean.parseBoolean(properties.getOrDefault(LogCatalogProperties.LOG_COPIES, "false")));
  }

  /**
   * The segmented log whose catalog object is at {@code catalogLocation} in {@code io} and whose
   * segments are named under {@code prefix} in {@code store}, as {@code properties} configure it.
   */
  public static SegmentedLog open(SupportsAtomicOperations io, String catalogLocation,
      AppendBlobStore store, String prefix, Map<String, String> properties) {
    AtomicFileLog catalog = AtomicFileLog.segmented(io, catalogLocation,
        () -> ThreadLocalRandom.current().nextLong(), LogCatalogProperties.readLookup(properties));
    return new SegmentedLog(catalog, new AppendBlobSegments(store), prefix, options(properties));
  }

  /** The shared access signature {@code properties} give for {@code account}, or null. */
  static String sas(Map<String, String> properties, String account) {
    for (String host : new String[] {account + ".dfs.core.windows.net",
        account + ".blob.core.windows.net", account}) {
      String sas = properties.get(SAS_TOKEN_PREFIX + host);
      if (sas != null) {
        return sas.startsWith("?") ? sas.substring(1) : sas;
      }
    }
    return null;
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
   * The log of the catalog at {@code catalogLocation} in {@code io}, its segments where {@code
   * properties} locate them ({@code fileio.catalog.log.location}, an {@code https} Blob URL of a
   * container and a path in it), signed with the account key they give, or else authorized by a
   * shared access signature they give for the account.
   *
   * @throws IllegalArgumentException if they give no location, and no account key or signature
   */
  static SharedLog open(
      SupportsAtomicOperations io, String catalogLocation, Map<String, String> properties) {
    String location = properties.get(LogCatalogProperties.LOG_LOCATION);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(location),
        "A catalog on an Azure log needs %s: https://<account>.blob.core.windows.net/<container>/"
            + "<path>", LogCatalogProperties.LOG_LOCATION);
    BlobUrlParts parts = BlobUrlParts.parse(location);
    String path = parts.getBlobName();
    String containerUrl = containerUrl(location);
    String account = properties.get(SHARED_KEY_ACCOUNT_NAME);
    String key = properties.get(SHARED_KEY_ACCOUNT_KEY);
    String sas = sas(properties, parts.getAccountName());
    Preconditions.checkArgument((account != null && key != null) || sas != null,
        "A catalog on an Azure log needs an account key (%s and %s) or a shared access "
            + "signature (%s<account>)", SHARED_KEY_ACCOUNT_NAME, SHARED_KEY_ACCOUNT_KEY,
        SAS_TOKEN_PREFIX);
    AzureAppendBlobStore store = account != null && key != null
        ? AzureAppendBlobStore.create(containerUrl, new StorageSharedKeyCredential(account, key))
        : AzureAppendBlobStore.create(containerUrl, new AzureSasCredential(sas));
    return open(io, catalogLocation, store, path, properties);
  }
}
