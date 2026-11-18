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
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.intentlog.SharedLog;
import org.intentlog.azure.AppendBlobSegments;
import org.intentlog.azure.AppendBlobStore;
import org.intentlog.azure.AzureAppendBlobStore;
import org.intentlog.azure.BlobUrlSegments;
import org.intentlog.fileio.AtomicFileLog;
import org.intentlog.fileio.SegmentedLog;

/**
 * Opens a catalog's segmented log on Azure Append Blobs ({@code fileio.catalog.log} {@code
 * azure-append}, docs/LOG_SEGMENTS.md and docs/AZURE_LOG.md "Configuration") from its properties:
 * the catalog object at the catalog's location in its FileIO, in format 4, and the segments
 * Append Blobs where the catalog object says (D13): under the prefix its creator recorded, or the
 * default every client derives from the catalog object's location ({@link #defaultPrefix}). A
 * client needs only the account's credentials; {@code fileio.catalog.log.location} is where a log
 * it creates keeps its segments. The catalog's only use of {@code intentlog-azure}, which it
 * depends on optionally: a catalog that keeps another log loads none of it.
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

  /**
   * What the default prefix appends to the catalog object's path: a sibling of the object, as its
   * pages' {@code <catalog>.committed/} is, never a path under it (on a hierarchical namespace the
   * object is a file, which no blob can be under).
   */
  static final String DEFAULT_SUFFIX = ".segments";

  /** An ADLS location of a container: {@code abfs[s]://} or {@code wasb[s]://<container>@<host>/<path>}. */
  private static final Pattern ADLS =
      Pattern.compile("(?i)^(abfss?|wasbs?)://([^/?#@]+)@([^/?#@:]+)(/[^?#]*)?$");

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
   * segments are named in {@code store}, one container: a log it creates keeps them under {@code
   * prefix}, which its catalog object records, and a log that exists where its catalog object
   * says.
   */
  public static SegmentedLog open(SupportsAtomicOperations io, String catalogLocation,
      AppendBlobStore store, String prefix, Map<String, String> properties) {
    AtomicFileLog catalog = AtomicFileLog.segmented(io, catalogLocation,
        () -> ThreadLocalRandom.current().nextLong(), LogCatalogProperties.readLookup(properties));
    return new SegmentedLog(catalog, new AppendBlobSegments(store), prefix, options(properties));
  }

  /**
   * The segmented log whose catalog object is at {@code catalogLocation} in {@code io}, its
   * segments named by their Blob URLs in the containers {@code containers} opens (a container's
   * URL to its store): where the catalog object says, under the prefix it records or else the
   * default {@link #defaultPrefix} derives. A log it creates keeps them at {@code
   * fileio.catalog.log.location}, or the default if {@code properties} give none.
   *
   * @throws IllegalArgumentException if the location {@code properties} give is no Blob URL of a
   *     container and a path in it
   */
  public static SegmentedLog open(SupportsAtomicOperations io, String catalogLocation,
      Function<String, AppendBlobStore> containers, Map<String, String> properties) {
    String location = properties.get(LogCatalogProperties.LOG_LOCATION);
    String configured = Strings.isNullOrEmpty(location) ? null : prefix(location);
    AtomicFileLog catalog = AtomicFileLog.segmented(io, catalogLocation,
        () -> ThreadLocalRandom.current().nextLong(), LogCatalogProperties.readLookup(properties));
    return new SegmentedLog(catalog, new BlobUrlSegments(containers), configured,
        defaultPrefix(catalogLocation), options(properties));
  }

  /**
   * The segments' prefix {@code location} gives, in canonical form ({@link
   * BlobUrlSegments#canonical}), without a trailing slash.
   *
   * @throws IllegalArgumentException if it is no Blob URL of a container and a path in it
   */
  static String prefix(String location) {
    String prefix;
    try {
      prefix = BlobUrlSegments.canonical(location);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(LogCatalogProperties.LOG_LOCATION + " names no container"
          + " and path: " + location + " (https://<account>.blob.core.windows.net/<container>/"
          + "<path>)", e);
    }
    while (prefix.endsWith("/")) {
      prefix = prefix.substring(0, prefix.length() - 1);
    }
    return prefix;
  }

  /**
   * The prefix of the segments of a log whose catalog object at {@code catalogLocation} records
   * none (docs/LOG_SEGMENTS.md D13): for an object on Azure, {@code
   * abfs[s]://<container>@<account>.dfs.<suffix>/<path>} (or {@code wasb[s]://} and {@code
   * .blob.}, as the ADLS FileIO names locations), the Blob URL of the same account, container and
   * path with {@value #DEFAULT_SUFFIX} appended, {@code
   * https://<account>.blob.<suffix>/<container>/<path>.segments}, in canonical form. Every form of
   * one location gives one prefix: the scheme, the endpoint ({@code dfs} or {@code blob}) and
   * letter case in the host and container do not matter, nor repeated slashes in the path. Null for
   * a location elsewhere, without a container, with a port, or of no other host.
   */
  public static String defaultPrefix(String catalogLocation) {
    if (catalogLocation == null) {
      return null;
    }
    Matcher m = ADLS.matcher(catalogLocation);
    if (!m.matches()) {
      return null;
    }
    String container = m.group(2).toLowerCase(Locale.ROOT);
    String[] labels = m.group(3).toLowerCase(Locale.ROOT).split("\\.", -1);
    if (labels.length < 3 || labels[0].isEmpty()
        || !(labels[1].equals("dfs") || labels[1].equals("blob"))) {
      return null;
    }
    labels[1] = "blob";
    String path = m.group(4) == null ? "" : m.group(4).replaceAll("/+", "/");
    path = path.startsWith("/") ? path.substring(1) : path;
    path = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
    if (path.isEmpty()) {
      return null;
    }
    return BlobUrlSegments.url(new BlobUrlSegments.Blob(
        "https://" + String.join(".", labels) + "/" + container, path + DEFAULT_SUFFIX));
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
   * The stores of the containers a log's segments may be in, signed with the account key {@code
   * properties} give for the container's account, or else authorized by a shared access signature
   * they give for it.
   *
   * @throws IllegalArgumentException from a container of an account they give no credentials for
   */
  static Function<String, AppendBlobStore> containers(Map<String, String> properties) {
    String keyAccount = properties.get(SHARED_KEY_ACCOUNT_NAME);
    String key = properties.get(SHARED_KEY_ACCOUNT_KEY);
    return containerUrl -> {
      String account = BlobUrlParts.parse(containerUrl).getAccountName();
      if (keyAccount != null && key != null && keyAccount.equalsIgnoreCase(account)) {
        return AzureAppendBlobStore.create(
            containerUrl, new StorageSharedKeyCredential(keyAccount, key));
      }
      String sas = account == null ? null : sas(properties, account);
      if (sas != null) {
        return AzureAppendBlobStore.create(containerUrl, new AzureSasCredential(sas));
      }
      throw new IllegalArgumentException("The catalog's log keeps its segments in " + containerUrl
          + ", of the account " + account + ", and this catalog has no credentials for it: give "
          + "it the account's key (" + SHARED_KEY_ACCOUNT_NAME + " " + account + " and "
          + SHARED_KEY_ACCOUNT_KEY + ") or a shared access signature (" + SAS_TOKEN_PREFIX
          + account + ")");
    };
  }

  /**
   * The log of the catalog at {@code catalogLocation} in {@code io}, its segments where its catalog
   * object says, each container's requests signed with the account key {@code properties} give
   * for its account, or else authorized by a shared access signature they give for it; a log it
   * creates keeps them at {@code fileio.catalog.log.location} ({@code https://<account>.blob.
   * core.windows.net/<container>/<path>}), or the default ({@link #defaultPrefix}).
   *
   * @throws IllegalArgumentException if they give no account key or signature, or a location
   *     that is no Blob URL of a container and a path in it
   */
  static SharedLog open(
      SupportsAtomicOperations io, String catalogLocation, Map<String, String> properties) {
    boolean key = properties.get(SHARED_KEY_ACCOUNT_NAME) != null
        && properties.get(SHARED_KEY_ACCOUNT_KEY) != null;
    boolean signature = properties.keySet().stream().anyMatch(k -> k.startsWith(SAS_TOKEN_PREFIX));
    Preconditions.checkArgument(key || signature,
        "A catalog on an Azure log needs an account key (%s and %s) or a shared access "
            + "signature (%s<account>)", SHARED_KEY_ACCOUNT_NAME, SHARED_KEY_ACCOUNT_KEY,
        SAS_TOKEN_PREFIX);
    return open(io, catalogLocation, containers(properties), properties);
  }
}
