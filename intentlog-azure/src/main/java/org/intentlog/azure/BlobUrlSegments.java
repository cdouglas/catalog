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

import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.common.Utility;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.intentlog.segments.SegmentStore;

/**
 * A segmented log's segments named by their Blob URLs ({@code
 * https://<account>.blob.core.windows.net/<container>/<path>}), each in the container its URL
 * names: the store of a log whose catalog object says where its segments are
 * (docs/LOG_SEGMENTS.md D13), so a client reaches them in whichever container of whichever
 * account the object names, without being configured with it. A container's requests go through
 * {@link AppendBlobSegments} over the {@link AppendBlobStore} that {@code containers} opens for
 * it on first use, which fails, saying why, for a container this client cannot reach (no
 * credentials for its account).
 *
 * <p>Names are canonical URLs ({@link #canonical}): the container's URL in lower case, then the
 * blob's name percent-encoded but for its slashes, as {@code AzureAppendBlobStore.location} writes
 * it. A canonical prefix extended by unreserved characters (a slash, a log's id, a segment's
 * name) stays canonical, so the names a listing returns start with the prefix it was given.
 */
public final class BlobUrlSegments implements SegmentStore {

  /** A blob's URL as its container's URL and the blob's name in it, decoded. */
  public record Blob(String container, String name) {}

  private final Function<String, AppendBlobStore> containers;
  private final Map<String, AppendBlobSegments> open = new ConcurrentHashMap<>();
  private final long maxBlocks;
  private final long maxBlockBytes;
  private final long maxSegmentBytes;

  /**
   * Segments in the containers {@code containers} opens, by URL, with Azure's limits at the
   * service versions since 2022-11-02 (blocks of 100 MiB), as {@code AzureAppendBlobStore.create}
   * builds its clients.
   */
  public BlobUrlSegments(Function<String, AppendBlobStore> containers) {
    this(containers, 50_000, 100L << 20, 50_000L * (4L << 20));
  }

  /** Segments in the containers {@code containers} opens, by URL, each with these limits. */
  public BlobUrlSegments(Function<String, AppendBlobStore> containers, long maxBlocks,
      long maxBlockBytes, long maxSegmentBytes) {
    this.containers = containers;
    this.maxBlocks = maxBlocks;
    this.maxBlockBytes = maxBlockBytes;
    this.maxSegmentBytes = maxSegmentBytes;
  }

  /**
   * The container and the blob {@code url} names.
   *
   * @throws IllegalArgumentException if it is no Blob URL of a container and a path in it
   */
  public static Blob blob(String url) {
    BlobUrlParts parts;
    try {
      parts = BlobUrlParts.parse(url);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Not a Blob URL: " + url, e);
    }
    String container = parts.getBlobContainerName();
    String name = parts.getBlobName();
    if (container == null || container.isEmpty() || name == null || name.isEmpty()) {
      throw new IllegalArgumentException("Not the URL of a container and a path in it: " + url);
    }
    String base = parts.setBlobName(null).setSnapshot(null).setVersionId(null)
        .setCommonSasQueryParameters(null).toUrl().toString();
    while (base.endsWith("/")) {
      base = base.substring(0, base.length() - 1);
    }
    return new Blob(base.toLowerCase(Locale.ROOT), name);
  }

  /** The canonical URL of {@code blob}: its container's, then its name, encoded but for slashes. */
  public static String url(Blob blob) {
    return blob.container() + "/" + Utility.urlEncode(blob.name()).replace("%2F", "/");
  }

  /**
   * {@code url} in canonical form: the URL every client writes for the blob it names.
   *
   * @throws IllegalArgumentException if it is no Blob URL of a container and a path in it
   */
  public static String canonical(String url) {
    return url(blob(url));
  }

  private AppendBlobSegments in(String container) {
    return open.computeIfAbsent(container, c -> new AppendBlobSegments(containers.apply(c)));
  }

  @Override
  public boolean create(String name) {
    Blob b = blob(name);
    return in(b.container()).create(b.name());
  }

  @Override
  public Appended append(String name, byte[] frame) {
    Blob b = blob(name);
    return in(b.container()).append(b.name(), frame);
  }

  @Override
  public Segment read(String name, long from) {
    Blob b = blob(name);
    return in(b.container()).read(b.name(), from);
  }

  @Override
  public void delete(String name) {
    Blob b = blob(name);
    in(b.container()).delete(b.name());
  }

  @Override
  public List<String> list(String prefix) {
    Blob b = blob(prefix);
    List<String> names = new ArrayList<>();
    for (String name : in(b.container()).list(b.name())) {
      names.add(url(new Blob(b.container(), name)));
    }
    return names;
  }

  /** A segment's name is its URL. */
  @Override
  public String location(String name) {
    return name;
  }

  /** The segment at {@code location}, a Blob URL in any container: its canonical URL. */
  @Override
  public String nameAt(String location) {
    return canonical(location);
  }

  @Override
  public long maxBlocks() {
    return maxBlocks;
  }

  @Override
  public long maxBlockBytes() {
    return maxBlockBytes;
  }

  @Override
  public long maxSegmentBytes() {
    return maxSegmentBytes;
  }

  @Override
  public String toString() {
    return "Append Blobs by URL";
  }
}
