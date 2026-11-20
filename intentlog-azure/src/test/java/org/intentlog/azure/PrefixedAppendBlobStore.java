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

import java.util.List;
import java.util.Map;

/**
 * A store's blobs under a prefix, named without it: a test of its own in a container other tests
 * share (a real account's).
 */
final class PrefixedAppendBlobStore implements AppendBlobStore {
  private final AppendBlobStore store;
  private final String prefix;

  PrefixedAppendBlobStore(AppendBlobStore store, String prefix) {
    this.store = store;
    this.prefix = prefix;
  }

  @Override
  public boolean createAppendBlob(String name, Map<String, String> metadata) {
    return store.createAppendBlob(prefix + name, metadata);
  }

  @Override
  public long appendBlock(String name, byte[] block, long maxSize, long appendPosition) {
    return store.appendBlock(prefix + name, block, maxSize, appendPosition);
  }

  @Override
  public Landed appendBlockCounted(String name, byte[] block) {
    return store.appendBlockCounted(prefix + name, block);
  }

  @Override
  public Blob read(String name, long from) {
    return store.read(prefix + name, from);
  }

  @Override
  public Props properties(String name) {
    return store.properties(prefix + name);
  }

  @Override
  public boolean putIfAbsent(String name, byte[] bytes) {
    return store.putIfAbsent(prefix + name, bytes);
  }

  @Override
  public void put(String name, byte[] bytes) {
    store.put(prefix + name, bytes);
  }

  @Override
  public String putIfMatch(String name, byte[] bytes, String etag) {
    return store.putIfMatch(prefix + name, bytes, etag);
  }

  @Override
  public void delete(String name) {
    store.delete(prefix + name);
  }

  @Override
  public List<Entry> list(String start) {
    return store.list(prefix + start).stream()
        .map(e -> new Entry(e.name().substring(prefix.length()), e.lastModified()))
        .toList();
  }

  @Override
  public String location(String name) {
    return store.location(prefix + name);
  }

  @Override
  public int maxBlocks() {
    return store.maxBlocks();
  }

  @Override
  public long maxBlobBytes() {
    return store.maxBlobBytes();
  }

  @Override
  public long maxBlockBytes() {
    return store.maxBlockBytes();
  }
}
