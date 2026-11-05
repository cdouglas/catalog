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
package org.intentlog.wire;

import java.util.Arrays;

/**
 * A reference to a page of the committed set ({@code CommittedPageRef}, log.proto): the block a
 * {@link PageOut} sealed. The page holds every id that applied in (the floor before it, {@link
 * #top}], and no other.
 *
 * @param level the sealed block's node: level ...
 * @param index ... and index on the grid
 * @param count the ids the page holds
 * @param maxTimestampMs the newest id's timestamp: the horizon once GC drops the page. No id above
 *     it and at or below the top applied
 * @param digest as {@link PageOut#digest}
 */
public record PageRef(int level, long index, int count, long maxTimestampMs, byte[] digest) {

  public PageRef {
    digest = digest.clone();
  }

  /** The newest timestamp the node spans: the floor the page raised. */
  public long top() {
    return CommittedSetCodec.top(level, index);
  }

  /** The page's name: its node and digest. */
  public String name() {
    return CommittedSetCodec.pageName(level, index, digest);
  }

  @Override
  public byte[] digest() {
    return digest.clone();
  }

  /** Whether {@code other} is this page's digest. */
  public boolean hasDigest(byte[] other) {
    return Arrays.equals(digest, other);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof PageRef p && level == p.level && index == p.index && count == p.count
        && maxTimestampMs == p.maxTimestampMs && Arrays.equals(digest, p.digest);
  }

  @Override
  public int hashCode() {
    return 31 * (31 * (31 * (31 * level + Long.hashCode(index)) + count)
        + Long.hashCode(maxTimestampMs)) + Arrays.hashCode(digest);
  }

  @Override
  public String toString() {
    return "PageRef(" + name() + ", " + count + " ids, newest " + maxTimestampMs + ")";
  }
}
