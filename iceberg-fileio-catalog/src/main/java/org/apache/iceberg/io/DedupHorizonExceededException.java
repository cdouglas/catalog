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

import java.util.UUID;

/**
 * Thrown by {@link ProtoCatalogFile#containsTransaction} when the queried
 * UUIDv7's timestamp falls at or below the catalog's
 * {@code highest_dropped_timestamp_ms}: the dedup oracle can no longer
 * distinguish "was committed and aged out" from "never committed."
 *
 * <p>Callers must fall back to a different oracle. See {@code docs/GC.md}
 * §"Two oracles" — the writer-side snapshot oracle is the documented pattern
 * for data writers that need definitive confirmation past the dedup horizon.
 */
public class DedupHorizonExceededException extends RuntimeException {
  private final UUID queryId;
  private final long horizonMs;

  public DedupHorizonExceededException(UUID queryId, long horizonMs) {
    super(String.format(
        "Cannot determine commit status for %s: timestamp %d ms <= dedup horizon %d ms",
        queryId, queryId == null ? 0L : UuidV7.timestampMs(queryId), horizonMs));
    this.queryId = queryId;
    this.horizonMs = horizonMs;
  }

  public UUID queryId() {
    return queryId;
  }

  public long horizonMs() {
    return horizonMs;
  }
}
