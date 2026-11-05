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
 * A {@code PageOut} record (log.proto): seals the oldest inline block of the committed set into
 * its page, which the record's writer put before it sent the record, and raises the floor to the
 * block's top. Replay applies it only if the oldest block is still the one its writer paged: the
 * node, the count and the digest (docs/COMMITTED_PAGING.md "Paging out: the log record").
 *
 * @param level the block's node: level ...
 * @param index ... and index on the grid
 * @param count the block's ids
 * @param digest the first {@link CommittedSetCodec#DIGEST_BYTES} bytes of the SHA-256 of the
 *     block's {@code CommittedBlock} message
 */
public record PageOut(int level, long index, int count, byte[] digest) {

  public PageOut {
    digest = digest.clone();
  }

  /** The newest timestamp the block spans: the floor the record raises. */
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

  /** Whether {@code digest} is this record's. */
  public boolean hasDigest(byte[] other) {
    return Arrays.equals(digest, other);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof PageOut p && level == p.level && index == p.index && count == p.count
        && Arrays.equals(digest, p.digest);
  }

  @Override
  public int hashCode() {
    return 31 * (31 * (31 * level + Long.hashCode(index)) + count) + Arrays.hashCode(digest);
  }

  @Override
  public String toString() {
    return "PageOut(" + name() + ", " + count + " ids)";
  }
}
