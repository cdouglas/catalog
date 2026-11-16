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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.intentlog.segments.SegmentStore;

/**
 * A segmented log's segments on Azure Append Blobs (docs/LOG_SEGMENTS.md): each segment an Append
 * Blob, appended blind (Append Block with no condition, its reply's offset and committed block
 * count the frame's place and index), read from an offset, listed and deleted by the sweep.
 * Failures keep their meaning: {@link AppendBlobStore.Full} (the block limit) and {@link
 * AppendBlobStore.NotFound} are nothing landed; {@link AppendBlobStore.Throttled}, that it may
 * have.
 */
public final class AppendBlobSegments implements SegmentStore {

  private final AppendBlobStore store;

  public AppendBlobSegments(AppendBlobStore store) {
    this.store = store;
  }

  @Override
  public boolean create(String name) {
    try {
      return store.createAppendBlob(name, Map.of());
    } catch (AppendBlobStore.Refused e) {
      throw new Refused(e.getMessage(), e);
    } catch (AppendBlobStore.Throttled e) {
      throw new Throttled(e.getMessage(), e);
    }
  }

  @Override
  public Appended append(String name, byte[] frame) {
    try {
      AppendBlobStore.Landed landed = store.appendBlockCounted(name, frame);
      return new Appended(landed.offset(), landed.blocks());
    } catch (AppendBlobStore.Full e) {
      throw new Full(e.getMessage());
    } catch (AppendBlobStore.NotFound e) {
      throw new NotFound(e.getMessage());
    } catch (AppendBlobStore.Refused e) {
      throw new Refused(e.getMessage(), e);
    } catch (AppendBlobStore.Throttled e) {
      throw new Throttled(e.getMessage(), e);
    }
  }

  @Override
  public Segment read(String name, long from) {
    AppendBlobStore.Blob b = store.read(name, from);
    return b == null ? null : new Segment(b.bytes(), b.length(), b.blocks());
  }

  @Override
  public void delete(String name) {
    store.delete(name);
  }

  @Override
  public List<String> list(String prefix) {
    List<String> names = new ArrayList<>();
    for (AppendBlobStore.Entry e : store.list(prefix)) {
      names.add(e.name());
    }
    return names;
  }

  @Override
  public String location(String name) {
    return store.location(name);
  }

  @Override
  public String nameAt(String location) {
    return store.nameAt(location);
  }

  @Override
  public long maxBlocks() {
    return store.maxBlocks();
  }

  @Override
  public long maxBlockBytes() {
    return store.maxBlockBytes();
  }

  @Override
  public long maxSegmentBytes() {
    return store.maxBlobBytes();
  }
}
