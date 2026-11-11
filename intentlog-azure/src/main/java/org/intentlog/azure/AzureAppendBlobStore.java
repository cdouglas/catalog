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

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.rest.Response;
import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.BlobServiceVersion;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.blob.models.AppendBlobItem;
import com.azure.storage.blob.models.AppendBlobRequestConditions;
import com.azure.storage.blob.models.BlobDownloadHeaders;
import com.azure.storage.blob.models.BlobDownloadResponse;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.models.DownloadRetryOptions;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.AppendBlobCreateOptions;
import com.azure.storage.blob.options.BlockBlobSimpleUploadOptions;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.Utility;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * An {@link AppendBlobStore} on one Azure Blob Storage container, through the Azure SDK. Names
 * are blob paths in the container.
 *
 * <p>Each request is sent once. The factories build a client whose retry policy makes one try
 * and names no secondary host; a client given to the constructor must be built the same way
 * ({@code RequestRetryOptions} with {@code maxTries} 1), which this class cannot check. Reads
 * also turn off the SDK's resumption of a broken download. The SDK's default HTTP client
 * (reactor-netty) resends a request only when the connection failed before its headers were sent.
 * A geo-secondary endpoint ({@code <account>-secondary.blob.core.windows.net}) is refused: it
 * serves reads that lag the primary's writes.
 *
 * <p>Failures map as {@link AppendBlobStore} says: {@code 503}, and {@code 500 OperationTimedOut}
 * or {@code InternalError}, are {@link Throttled}; any other {@code 4xx} that a method does not
 * handle is {@link Refused}; other service errors propagate as the SDK's {@link
 * BlobStorageException}, and transport failures (timeouts, resets) as the SDK throws them.
 */
public final class AzureAppendBlobStore implements AppendBlobStore {

  /** How long the one try of a request may take. */
  public static final Duration TRY_TIMEOUT = Duration.ofSeconds(60);

  private static final HttpHeaderName APPEND_OFFSET =
      HttpHeaderName.fromString("x-ms-blob-append-offset");
  private static final DownloadRetryOptions NO_DOWNLOAD_RETRIES =
      new DownloadRetryOptions().setMaxRetryRequests(0);
  /** The metadata that marks a directory in a listing of a hierarchical namespace. */
  private static final String FOLDER = "hdi_isfolder";
  /** How often a read starting at the blob's end reads again when the blob grew meanwhile. */
  private static final int GROWN_REREADS = 3;

  private final BlobContainerClient container;

  /**
   * A store on {@code container}, whose pipeline must make one try per request and name no
   * secondary host. Use {@link #create(String, StorageSharedKeyCredential)} or {@link
   * #create(String, TokenCredential)} to build one that does.
   *
   * @throws IllegalArgumentException if the container is on a geo-secondary endpoint
   */
  public AzureAppendBlobStore(BlobContainerClient container) {
    this.container = Objects.requireNonNull(container, "container");
    refuseSecondary(container.getBlobContainerUrl());
  }

  /**
   * A store on the container at {@code containerUrl} ({@code
   * https://<account>.blob.core.windows.net/<container>}; for an emulator, {@code
   * http://127.0.0.1:10000/<account>/<container>}), signed with an account key.
   *
   * @throws IllegalArgumentException if the URL names no container or a geo-secondary endpoint
   */
  public static AzureAppendBlobStore create(
      String containerUrl, StorageSharedKeyCredential credential) {
    return new AzureAppendBlobStore(
        builder(containerUrl).credential(Objects.requireNonNull(credential)).buildClient());
  }

  /**
   * A store on the container at {@code containerUrl}, authorized by Microsoft Entra ID tokens
   * (which the SDK sends over HTTPS only).
   *
   * @throws IllegalArgumentException if the URL names no container or a geo-secondary endpoint
   */
  public static AzureAppendBlobStore create(String containerUrl, TokenCredential credential) {
    return new AzureAppendBlobStore(
        builder(containerUrl).credential(Objects.requireNonNull(credential)).buildClient());
  }

  private static BlobContainerClientBuilder builder(String containerUrl) {
    refuseSecondary(containerUrl);
    String name = BlobUrlParts.parse(containerUrl).getBlobContainerName();
    if (name == null || name.isEmpty()) {
      // the builder would fall back to the account's $root container
      throw new IllegalArgumentException("No container in " + containerUrl);
    }
    return new BlobContainerClientBuilder()
        .endpoint(containerUrl)
        .retryOptions(
            new RequestRetryOptions(RetryPolicyType.EXPONENTIAL, 1, TRY_TIMEOUT, null, null, null));
  }

  /**
   * Refuses a URL on a geo-secondary endpoint: a host {@code <account>-secondary.<...>}, or an
   * emulator's account {@code <account>-secondary} in the path. Account names have no hyphens.
   * The host is checked itself because the SDK's parse drops the suffix from the account it
   * finds there.
   */
  static void refuseSecondary(String url) {
    BlobUrlParts parts = BlobUrlParts.parse(url);
    String host = parts.getHost() == null ? "" : parts.getHost().toLowerCase(Locale.ROOT);
    String account =
        parts.getAccountName() == null ? "" : parts.getAccountName().toLowerCase(Locale.ROOT);
    int dot = host.indexOf('.');
    if ((dot > 0 && host.substring(0, dot).endsWith("-secondary"))
        || account.endsWith("-secondary")) {
      throw new IllegalArgumentException(
          "A geo-secondary endpoint serves stale reads; use the primary: " + url);
    }
  }

  @Override
  public boolean createAppendBlob(String name, Map<String, String> metadata) {
    AppendBlobCreateOptions options =
        new AppendBlobCreateOptions()
            .setMetadata(metadata)
            .setRequestConditions(new BlobRequestConditions().setIfNoneMatch("*"));
    try {
      appendBlob(name).createWithResponse(options, null, Context.NONE);
      return true;
    } catch (BlobStorageException e) {
      if (exists(e)) {
        return false;
      }
      throw failure("create append blob", name, e);
    }
  }

  @Override
  public long appendBlock(String name, byte[] block, long maxSize, long appendPosition) {
    AppendBlobRequestConditions conditions = new AppendBlobRequestConditions();
    if (maxSize > 0) {
      conditions.setMaxSize(maxSize);
    }
    if (appendPosition >= 0) {
      conditions.setAppendPosition(appendPosition);
    }
    Response<AppendBlobItem> response;
    try {
      response =
          appendBlob(name)
              .appendBlockWithResponse(
                  new ByteArrayInputStream(block), block.length, null, conditions, null,
                  Context.NONE);
    } catch (BlobStorageException e) {
      BlobErrorCode code = e.getErrorCode();
      if (BlobErrorCode.MAX_BLOB_SIZE_CONDITION_NOT_MET.equals(code)
          || BlobErrorCode.BLOCK_COUNT_EXCEEDS_LIMIT.equals(code)) {
        throw new Full(describe("append block", name, e));
      }
      if (BlobErrorCode.APPEND_POSITION_CONDITION_NOT_MET.equals(code)) {
        throw new PositionMismatch(describe("append block", name, e));
      }
      if (e.getStatusCode() == 404) {
        throw new NotFound(describe("append block", name, e));
      }
      throw failure("append block", name, e);
    }
    String offset = response.getHeaders().getValue(APPEND_OFFSET);
    return Long.parseLong(offset != null ? offset : response.getValue().getBlobAppendOffset());
  }

  @Override
  public Blob read(String name, long from) {
    if (from < 0) {
      throw new IllegalArgumentException("Negative offset " + from + " in " + name);
    }
    BlobClient blob = container.getBlobClient(name);
    for (int reread = 0; ; reread++) {
      ByteArrayOutputStream body = new ByteArrayOutputStream();
      BlobDownloadResponse response;
      try {
        // from 0 the SDK sends no range: a whole Get Blob, which an empty blob answers with 200
        response =
            blob.downloadStreamWithResponse(
                body, new BlobRange(from), NO_DOWNLOAD_RETRIES, null, false, null, Context.NONE);
      } catch (BlobStorageException e) {
        if (e.getStatusCode() == 404) {
          return null;
        }
        if (e.getStatusCode() != 416) {
          throw failure("read", name, e);
        }
        // 416 InvalidRange: the range starts at or past the blob's end, so read its properties
        BlobProperties props = getProperties(name);
        if (props == null) {
          return null;
        }
        if (props.getBlobSize() <= from) {
          return new Blob(
              new byte[0], props.getBlobSize(), blocks(props.getCommittedBlockCount()),
              metadata(props.getMetadata()), props.getETag());
        }
        if (reread >= GROWN_REREADS) {
          throw new IllegalStateException(
              "Read of " + name + " from " + from + " kept missing its end");
        }
        continue; // it grew after the Get Blob: read again
      }
      return blob(name, from, body.toByteArray(), response.getDeserializedHeaders());
    }
  }

  /** The blob a Get Blob from {@code from} answered: the bytes from there and the whole length. */
  private static Blob blob(String name, long from, byte[] bytes, BlobDownloadHeaders headers) {
    Long contentLength = headers.getContentLength();
    if (contentLength != null && contentLength != bytes.length) {
      throw new IllegalStateException(
          "Read " + bytes.length + " of " + contentLength + " bytes of " + name);
    }
    String range = headers.getContentRange();
    long length;
    if (range != null) {
      length = total(range);
    } else {
      // no Content-Range: the body is the whole blob
      length = bytes.length;
      bytes = from >= length ? new byte[0] : Arrays.copyOfRange(bytes, (int) from, bytes.length);
    }
    return new Blob(
        bytes, length, blocks(headers.getBlobCommittedBlockCount()),
        metadata(headers.getMetadata()), headers.getETag());
  }

  /** The total of a {@code Content-Range}: {@code bytes <first>-<last>/<total>}. */
  private static long total(String contentRange) {
    int slash = contentRange.lastIndexOf('/');
    if (slash < 0 || slash == contentRange.length() - 1) {
      throw new IllegalStateException("No total length in Content-Range " + contentRange);
    }
    return Long.parseLong(contentRange.substring(slash + 1).trim());
  }

  @Override
  public Props properties(String name) {
    BlobProperties props = getProperties(name);
    if (props == null) {
      return null;
    }
    return new Props(
        props.getBlobSize(), blocks(props.getCommittedBlockCount()),
        metadata(props.getMetadata()), props.getLastModified().toInstant());
  }

  /** Get Blob Properties; null if the blob does not exist. */
  private BlobProperties getProperties(String name) {
    try {
      return container.getBlobClient(name).getPropertiesWithResponse(null, null, Context.NONE)
          .getValue();
    } catch (BlobStorageException e) {
      if (e.getStatusCode() == 404) {
        return null;
      }
      throw failure("properties", name, e);
    }
  }

  @Override
  public boolean putIfAbsent(String name, byte[] bytes) {
    try {
      upload(name, bytes, new BlobRequestConditions().setIfNoneMatch("*"));
      return true;
    } catch (BlobStorageException e) {
      if (exists(e)) {
        return false;
      }
      throw failure("put if absent", name, e);
    }
  }

  @Override
  public void put(String name, byte[] bytes) {
    try {
      upload(name, bytes, null);
    } catch (BlobStorageException e) {
      throw failure("put", name, e);
    }
  }

  @Override
  public String putIfMatch(String name, byte[] bytes, String etag) {
    // a null If-Match would be no condition at all
    Objects.requireNonNull(etag, "etag");
    try {
      return upload(name, bytes, new BlobRequestConditions().setIfMatch(etag)).getETag();
    } catch (BlobStorageException e) {
      if (e.getStatusCode() == 404 || conditionNotMet(e)) {
        return null;
      }
      throw failure("put if match", name, e);
    }
  }

  /** Put Blob of a block blob, in one request. */
  private BlockBlobItem upload(String name, byte[] bytes, BlobRequestConditions conditions) {
    BlockBlobSimpleUploadOptions options =
        new BlockBlobSimpleUploadOptions(BinaryData.fromBytes(bytes))
            .setRequestConditions(conditions);
    return container.getBlobClient(name).getBlockBlobClient()
        .uploadWithResponse(options, null, Context.NONE)
        .getValue();
  }

  @Override
  public void delete(String name) {
    try {
      container.getBlobClient(name).deleteWithResponse(null, null, null, Context.NONE);
    } catch (BlobStorageException e) {
      if (e.getStatusCode() != 404) {
        throw failure("delete", name, e);
      }
    }
  }

  @Override
  public List<Entry> list(String prefix) {
    ListBlobsOptions options =
        new ListBlobsOptions()
            .setPrefix(prefix)
            .setDetails(new BlobListDetails().setRetrieveMetadata(true));
    List<Entry> entries = new ArrayList<>();
    try {
      for (BlobItem item : container.listBlobs(options, null)) {
        if (Boolean.TRUE.equals(item.isPrefix()) || isFolder(item.getMetadata())) {
          continue;
        }
        entries.add(new Entry(item.getName(), item.getProperties().getLastModified().toInstant()));
      }
    } catch (BlobStorageException e) {
      if (e.getStatusCode() == 404) {
        return List.of(); // no container, so no blobs
      }
      throw failure("list", prefix, e);
    }
    return entries;
  }

  /** A directory placeholder that a flat listing returns on a hierarchical namespace. */
  private static boolean isFolder(Map<String, String> metadata) {
    if (metadata == null) {
      return false;
    }
    for (Map.Entry<String, String> e : metadata.entrySet()) {
      if (FOLDER.equalsIgnoreCase(e.getKey()) && "true".equalsIgnoreCase(e.getValue())) {
        return true;
      }
    }
    return false;
  }

  /** The blob's URL, its name's slashes kept (the SDK's {@code getBlobUrl} escapes them). */
  @Override
  public String location(String name) {
    return container.getBlobContainerUrl() + "/" + Utility.urlEncode(name).replace("%2F", "/");
  }

  /** 4 MiB for a client pinned to a service version before 2022-11-02, 100 MiB since. */
  @Override
  public long maxBlockBytes() {
    BlobServiceVersion version = container.getServiceVersion();
    return version != null && version.compareTo(BlobServiceVersion.V2022_11_02) < 0
        ? 4L << 20
        : AppendBlobStore.super.maxBlockBytes();
  }

  private AppendBlobClient appendBlob(String name) {
    return container.getBlobClient(name).getAppendBlobClient();
  }

  /** A Put Blob with {@code If-None-Match: *} found a blob of that name. */
  private static boolean exists(BlobStorageException e) {
    return BlobErrorCode.BLOB_ALREADY_EXISTS.equals(e.getErrorCode()) || conditionNotMet(e);
  }

  /** {@code 412 ConditionNotMet}: a request's {@code If-Match} or {@code If-None-Match} failed. */
  private static boolean conditionNotMet(BlobStorageException e) {
    BlobErrorCode code = e.getErrorCode();
    return e.getStatusCode() == 412
        && (code == null || BlobErrorCode.CONDITION_NOT_MET.equals(code));
  }

  /**
   * The exception for a failure a method does not handle: {@link Throttled} for a busy or timed
   * out service, {@link NotFound} for a missing blob or container, {@link Refused} for any other
   * {@code 4xx}, and the SDK's own exception otherwise.
   */
  private static RuntimeException failure(String op, String name, BlobStorageException e) {
    int status = e.getStatusCode();
    BlobErrorCode code = e.getErrorCode();
    if (status == 503
        || (status == 500
            && (BlobErrorCode.OPERATION_TIMED_OUT.equals(code)
                || BlobErrorCode.INTERNAL_ERROR.equals(code)))) {
      return new Throttled(describe(op, name, e), e);
    }
    if (status == 404) {
      return new NotFound(describe(op, name, e));
    }
    if (status >= 400 && status < 500) {
      return new Refused(describe(op, name, e), e);
    }
    return e;
  }

  private static String describe(String op, String name, BlobStorageException e) {
    return op + " " + name + ": " + e.getStatusCode() + " " + e.getErrorCode();
  }

  private static int blocks(Integer committed) {
    return committed == null ? 0 : committed;
  }

  private static Map<String, String> metadata(Map<String, String> metadata) {
    return metadata == null ? Map.of() : Map.copyOf(metadata);
  }
}
