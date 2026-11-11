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

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * The requests {@link AppendBlobLog} makes of a blob container, and nothing more: Append Blobs
 * created with metadata and appended to with Append Block's conditions, block blobs put once or
 * by ETag, ranged reads, properties, deletes and listings. Names are paths in one container.
 *
 * <p>Failures are the exceptions below, by what they say of a write: {@link Full}, {@link
 * PositionMismatch}, {@link NotFound} and {@link Refused} mean nothing landed; {@link Throttled}
 * that it may have (the same request may be sent again); any other exception leaves a write's
 * outcome unknown and means a read failed. A store sends each request once: a retry inside it
 * could land an append twice and report only the second (AZURE_LOG.md "Failures").
 */
public interface AppendBlobStore {

  /** A blob read from an offset: the bytes from there to its end, and what Get Blob says of it. */
  record Blob(byte[] bytes, long length, int blocks, Map<String, String> metadata, String etag) {}

  /** What Get Blob Properties says of a blob. */
  record Props(long length, int blocks, Map<String, String> metadata, Instant lastModified) {}

  /** A blob a listing found. */
  record Entry(String name, Instant lastModified) {}

  /**
   * Creates an empty Append Blob with {@code metadata} (Put Blob, {@code If-None-Match: *}).
   * Returns false if a blob of that name exists.
   */
  boolean createAppendBlob(String name, Map<String, String> metadata);

  /**
   * Append Block: appends {@code block} to the Append Blob {@code name}, only if the blob stays
   * within {@code maxSize} bytes ({@code x-ms-blob-condition-maxsize}; 0 or less: no condition)
   * and its length is {@code appendPosition} ({@code x-ms-blob-condition-appendpos}; negative: no
   * condition). Returns the offset it landed at ({@code x-ms-blob-append-offset}).
   *
   * @throws Full if the size condition failed or the blob has as many blocks as it may hold
   * @throws PositionMismatch if the position condition failed
   * @throws NotFound if the blob does not exist
   */
  long appendBlock(String name, byte[] block, long maxSize, long appendPosition);

  /**
   * Get Blob from {@code from} to its end; null if the blob does not exist. A blob no longer than
   * {@code from} reads as no bytes, with its length.
   */
  Blob read(String name, long from);

  /** Get Blob Properties; null if the blob does not exist. */
  Props properties(String name);

  /** Puts a block blob if none of that name exists ({@code If-None-Match: *}); false if one does. */
  boolean putIfAbsent(String name, byte[] bytes);

  /** Puts a block blob, replacing any of that name. */
  void put(String name, byte[] bytes);

  /**
   * Puts a block blob only if the blob of that name has ETag {@code etag} ({@code If-Match}).
   * Returns its new ETag, or null if the blob's ETag differs or it does not exist.
   */
  String putIfMatch(String name, byte[] bytes, String etag);

  /** Deletes the blob {@code name}, if it exists. */
  void delete(String name);

  /** The blobs whose names start with {@code prefix}. */
  List<Entry> list(String prefix);

  /** Where {@code name} is, as a URL a reader of files can open; null if the store cannot say. */
  default String location(String name) {
    return null;
  }

  /** The most blocks an Append Blob holds (Azure: 50,000). */
  default int maxBlocks() {
    return 50_000;
  }

  /** The largest block one Append Block takes (Azure: 4 MiB before 2022-11-02, 100 MiB since). */
  default long maxBlockBytes() {
    return 100L << 20;
  }

  /**
   * The append did not land: the blob would pass its size condition ({@code 412
   * MaxBlobSizeConditionNotMet}), or holds as many blocks as it may ({@code 409
   * BlockCountExceedsLimit}).
   */
  final class Full extends RuntimeException {
    public Full(String message) {
      super(message);
    }
  }

  /** The append did not land: the blob's length was not the position named ({@code 412}). */
  final class PositionMismatch extends RuntimeException {
    public PositionMismatch(String message) {
      super(message);
    }
  }

  /** The blob does not exist ({@code 404}); nothing landed. */
  final class NotFound extends RuntimeException {
    public NotFound(String message) {
      super(message);
    }
  }

  /** The service refused the request ({@code 400}, {@code 403}, ...); nothing landed. */
  final class Refused extends RuntimeException {
    public Refused(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * The service is busy or timed out ({@code 503 ServerBusy}, {@code 500 OperationTimedOut}, {@code
   * 500 InternalError}): a write may have landed, and may be sent again; a read failed.
   */
  final class Throttled extends RuntimeException {
    public Throttled(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
