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
package org.intentlog;

/** When a commit to a log that can replace itself writes a checkpoint instead of appending. */
@FunctionalInterface
public interface CompactionPolicy {

  /** Whether the next commit compacts the log it would extend. */
  boolean due(Input log);

  /**
   * What a policy sees of the log a commit would extend: the intention records after its
   * checkpoint, the checkpoint's size, the log's size (checkpoint included), and the size of the
   * record the commit would add.
   */
  record Input(long intentionRecords, long checkpointBytes, long logBytes, int recordBytes) {
    /** The bytes after the checkpoint: the records, and the provider's framing of the log. */
    public long tailBytes() {
      return Math.max(0L, logBytes - checkpointBytes);
    }
  }

  /** What a size bound counts. */
  enum SizeBasis {
    /**
     * The records after the checkpoint: a log compacts by what it would replay and rewrite beyond
     * its checkpoint, whatever the checkpoint's size (errata D19).
     */
    TAIL,
    /**
     * The whole log, checkpoint included, as before D19's stopgap: a checkpoint past the bound
     * makes every commit compact.
     */
    OBJECT
  }

  /**
   * Compacts once the log holds {@code maxRecords} intention records (0: every commit), or the
   * record would take the records after the checkpoint past {@code maxBytes}.
   */
  static CompactionPolicy limits(int maxRecords, long maxBytes) {
    return limits(maxRecords, maxBytes, SizeBasis.TAIL);
  }

  /**
   * Compacts once the log holds {@code maxRecords} intention records (0: every commit), or the
   * record would take what {@code basis} counts past {@code maxBytes}. Both are soft: writers that
   * lose an append race resend without reading the count again.
   */
  static CompactionPolicy limits(int maxRecords, long maxBytes, SizeBasis basis) {
    return log -> {
      long bytes = basis == SizeBasis.TAIL ? log.tailBytes() : log.logBytes();
      return log.intentionRecords() >= maxRecords || bytes + log.recordBytes() > maxBytes;
    };
  }
}
