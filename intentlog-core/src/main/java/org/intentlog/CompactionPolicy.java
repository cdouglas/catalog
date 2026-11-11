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

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * When a log that can replace itself is compacted: by the commit that would extend it, which writes
 * a checkpoint and its record instead of appending ({@link RuntimeConfig#compactAfterCommit} off),
 * or after a commit landed in it, by the commit's writer ({@link
 * RuntimeConfig#compactAfterCommit}, the default). A log that keeps its checkpoints apart is
 * compacted after a commit too ({@link LogRuntime#compact}).
 */
@FunctionalInterface
public interface CompactionPolicy {

  /** Whether the log is due for compaction: the next commit compacts it, or the last one's writer. */
  boolean due(Input log);

  /**
   * How far the log has come toward compaction: 1 or more once it is due, by how far it is past
   * the limits. A writer that compacts after its commit compacts at a progress it draws in [1, 2),
   * so that writers do not all compact at the same point (LogRuntime's compaction after a commit).
   * A policy that says only whether it is due is at 1 once it is, else 0.
   */
  default double progress(Input log) {
    return due(log) ? 1.0 : 0.0;
  }

  /**
   * A draw in [0, 1): where past the limits a writer compacts after its commit, as a fraction of
   * the limits. Uniform; a test passes its own draws ({@link #withDraws}).
   */
  default double draw() {
    return ThreadLocalRandom.current().nextDouble();
  }

  /**
   * Whether a compaction after a commit that lost its race to {@code beat} appends is sent again,
   * conditioned on the log as it grew, with their records copied in front of its own (LogRuntime's
   * compaction after a commit, a few times at most): if, at the rate they landed over the {@code
   * windowNs} it was open to them (from the version it was conditioned on, through its encoding,
   * or its read, and its send), less than one append is expected over the {@code exposureNs} a
   * resend is (a read of the log after it, and its send). Appends arriving at that rate, the resend
   * lands with probability above 1/e. Its checkpoint is encoded already, so a resend costs a read
   * and a send where a compaction from scratch would encode again; at the knee of a log whose
   * checkpoint takes longer to send than the gap between appends, it is not sent again.
   */
  default boolean resend(int beat, long windowNs, long exposureNs) {
    return beat > 0 && windowNs > 0L && beat * (double) exposureNs / windowNs < 1.0;
  }

  /**
   * This policy, with its random choices taken from {@code draws} (each in [0, 1)) for tests: the
   * points past the limits writers compact at ({@link #draw}), and whether a compaction that lost
   * is sent again ({@link #resend}: a draw below 1/2), not the rate of appends a writer saw.
   */
  default CompactionPolicy withDraws(DoubleSupplier draws) {
    CompactionPolicy policy = this;
    return new CompactionPolicy() {
      @Override
      public boolean due(Input log) {
        return policy.due(log);
      }

      @Override
      public double progress(Input log) {
        return policy.progress(log);
      }

      @Override
      public double draw() {
        return draws.getAsDouble();
      }

      @Override
      public boolean resend(int beat, long windowNs, long exposureNs) {
        return draws.getAsDouble() < 0.5;
      }

      @Override
      public String toString() {
        return policy + " with drawn points";
      }
    };
  }

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
    return new CompactionPolicy() {
      @Override
      public boolean due(Input log) {
        return log.intentionRecords() >= maxRecords || bytes(log) + log.recordBytes() > maxBytes;
      }

      /** The larger of the record count's and the size's fraction of their limits. */
      @Override
      public double progress(Input log) {
        double records = maxRecords == 0 ? Double.POSITIVE_INFINITY
            : (double) log.intentionRecords() / maxRecords;
        double bytes = maxBytes == 0 ? Double.POSITIVE_INFINITY
            : (double) (bytes(log) + log.recordBytes()) / maxBytes;
        return due(log) ? Math.max(1.0, Math.max(records, bytes)) : Math.max(records, bytes);
      }

      private long bytes(Input log) {
        return basis == SizeBasis.TAIL ? log.tailBytes() : log.logBytes();
      }

      @Override
      public String toString() {
        return String.format("limits(%d records, %d bytes of the %s)", maxRecords, maxBytes,
            basis == SizeBasis.TAIL ? "tail" : "object");
      }
    };
  }
}
