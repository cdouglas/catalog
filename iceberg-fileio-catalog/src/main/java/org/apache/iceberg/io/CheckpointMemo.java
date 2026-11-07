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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import org.apache.iceberg.ManifestFile;

/**
 * The encodings of a catalog's inline tables, kept so that a checkpoint writes a table that has
 * not changed as the bytes it had (docs/CHECKPOINT_PLAN.md R0.2): a decode records each table's
 * bytes beside the objects it made of them, and an encode records what it wrote. A table's
 * encoding is a function of its row, inline metadata, manifest prefix, pool, lists and conflict
 * state, and each of those is immutable once a state holds it (a builder copies a pool, a list
 * map or a conflict state before it changes one, and replaces a row or an inline table), so the
 * same objects encode to the same bytes (I11): the memo matches them by identity, never by
 * content. One memo serves one catalog's state machine, whose threads share it.
 *
 * <p>An entry is kept on the table's row ({@code TblEntry#encoded}), which no other table holds and
 * a table's next version replaces, and lives as long as some state holds the row: each line of
 * states folded from one decode keeps its tables' encodings, whether it is a writer's or a replay
 * of the whole log beside it (the runtime's check makes one per fold), and a line no state
 * continues takes its entries with it, in the same collection. It was a WeakHashMap keyed by the
 * row (Bug 85): a weak key in the old generation is cleared only by an old-generation collection,
 * which still marks the value live, and the entry went only at the map's next access, so each
 * row's table, pool and lists outlived it by an old-generation cycle or more. In a CAS mode, where
 * every commit encodes and every read decodes, so every state has rows of its own, that kept every
 * state's tables between cycles (catalog-bench's e3-sp2 at 1,000 snapshots on S3 Standard; its
 * configuration run locally in a 12 GB heap held 1,885 entries for 80 live rows). A hit
 * needs the very objects the entry was made from, so an entry is good whichever machine reads it;
 * the counts are this memo's. With {@code -Dfileio.catalog.replay.check=true} (the
 * test suites set it) each reuse is checked against a fresh encoding.
 */
final class CheckpointMemo {

  static final boolean CHECK = Boolean.getBoolean("fileio.catalog.replay.check");

  private final LongAdder hits = new LongAdder();
  private final LongAdder misses = new LongAdder();

  /**
   * What a table's encoding is a function of, as a state (or a builder about to build one) holds
   * it. Compared by identity: see the class comment.
   */
  static final class Inputs {
    private final CatalogState.TblEntry row;
    private final InlineTable metadata;
    private final String prefix;
    private final Map<String, ManifestFile> pool;
    private final Map<Long, List<String>> lists;
    private final Map<String, Integer> ordinals;
    private final int nextOrdinal;
    private final EncodingState conflict;
    private final EncodingState.Codec codec;
    private final Map<Long, SnapshotPages.SealedBlock> sealed;

    Inputs(
        CatalogState.TblEntry row, InlineTable metadata, String prefix,
        Map<String, ManifestFile> pool, Map<Long, List<String>> lists,
        Map<String, Integer> ordinals, int nextOrdinal, EncodingState conflict,
        EncodingState.Codec codec, Map<Long, SnapshotPages.SealedBlock> sealed) {
      this.row = row;
      this.metadata = metadata;
      this.prefix = prefix;
      this.pool = pool;
      this.lists = lists;
      this.ordinals = ordinals;
      this.nextOrdinal = nextOrdinal;
      this.conflict = conflict;
      this.codec = codec;
      this.sealed = sealed;
    }

    /** Whether these are the very objects {@code other} holds, but for the row. */
    boolean sameContent(Inputs other) {
      return metadata == other.metadata
          && prefix.equals(other.prefix)
          && pool == other.pool
          && lists == other.lists
          && ordinals == other.ordinals
          && nextOrdinal == other.nextOrdinal
          && conflict == other.conflict
          && codec == other.codec
          && sealed == other.sealed;
    }
  }

  /** A table's encoding, with what it is a function of but the row, which holds it. */
  static final class Encoded {
    private final int id;
    private final Inputs inputs;
    private final byte[] bytes;

    Encoded(int id, Inputs inputs, byte[] bytes) {
      this.id = id;
      // Without the row, which holds it
      this.inputs = new Inputs(null, inputs.metadata, inputs.prefix, inputs.pool, inputs.lists,
          inputs.ordinals, inputs.nextOrdinal, inputs.conflict, inputs.codec, inputs.sealed);
      this.bytes = bytes;
    }
  }

  /** The bytes table {@code id} encoded to when it held {@code inputs}, or null. */
  byte[] get(int id, Inputs inputs) {
    Encoded encoded = inputs.row != null ? inputs.row.encoded : null;
    if (encoded != null && encoded.id == id && encoded.inputs.sameContent(inputs)) {
      hits.increment();
      return encoded.bytes;
    }
    misses.increment();
    return null;
  }

  /** Records that table {@code id}, holding {@code inputs}, encodes to {@code bytes}. */
  void put(int id, Inputs inputs, byte[] bytes) {
    if (inputs.row != null) {
      inputs.row.encoded = new Encoded(id, inputs, bytes);
    }
  }

  /** Tables encoded from the memo, since it was made. */
  long hits() {
    return hits.sum();
  }

  /** Tables the memo did not hold as they were, since it was made. */
  long misses() {
    return misses.sum();
  }
}
