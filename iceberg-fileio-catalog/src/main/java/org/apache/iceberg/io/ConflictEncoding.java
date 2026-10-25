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

/**
 * Decides, at replay, whether a table action conflicts with what landed since its base: a conflict
 * encoding. It takes a table action of an intention record ({@link TableAction}), extracts a
 * conflict witness from it (what the action depends on), and validates the witness against the
 * table where the record lands ({@link ReplayView}) and against any state the encoding keeps for
 * the table ({@link EncodingState}). The Lamport clock ({@link LamportEncoding}: apply only on the
 * version read) is one configuration; others separate a table's concerns, so that replay applies
 * a commit Iceberg's retry would have repaired, finalized on the commits that landed first
 * ({@link Landing}).
 *
 * <p>A catalog's encoding is fixed when it is created ({@code SetConflictEncoding} in its first
 * record) and kept in its checkpoints, so every reader replays the log under the same one (I11).
 * Writers write the same records under every encoding.
 *
 * <p>The contract, which {@code ConflictEncodingContract} checks:
 *
 * <ol>
 *   <li>Deterministic (S1): the witness and the judgment are functions of the record and the
 *       state alone.
 *   <li>Stable (S2): a rejection never turns into an admission later. A condition on the landing
 *       state that could flip back (a ref rolled back, a spec id reused, a snapshot expired) must
 *       be guarded by evidence that only accumulates: monotone stamps or counters in the encoding's
 *       state. A value check of the REST kind ("main is at S5") is not enough on its own.
 *   <li>Total: {@link #witness}, {@link #admits} and {@link #observe} never throw; a throw at
 *       replay fails every read of the log. Anything unexpected coarsens.
 *   <li>Coarsens when blind: where it cannot derive a finer witness (a pointer table, a full or
 *       location payload, a snapshot without an inline list, a record without the producer's
 *       footprint) it uses the Lamport witness, the version read.
 *   <li>Admits only what merges exactly: a stale record it admits merges onto the landing table
 *       ({@link Landing#unmergeable} is null), and is what its operation would commit there.
 *   <li>No false negatives: a record it admits on a changed table is what Iceberg's retry of the
 *       operation commits there ({@code TestRebaseProperties}). A false positive, a conflict
 *       where the retry would have committed, is allowed: the client's retry absorbs it.
 * </ol>
 *
 * <p>Every encoding can express "don't apply if anything changed", and keeps it for what Iceberg
 * commits once (a schema or partition-spec update, a ref change through {@code
 * manageSnapshots}) and for a catalog transaction's tables ({@link TableAction#pinned}).
 *
 * @param <W> the witness
 */
interface ConflictEncoding<W> {

  /** The encoding's name, which the catalog's checkpoints record. */
  String name();

  /** The witness of {@code action}: what it depends on, from the record (S1). */
  W witness(TableAction action, ReplayView landing);

  /**
   * Whether {@code witness} holds where its record lands, on {@code landing} with {@code state},
   * the encoding's state of the table. Stable: once false, false at every later point (S2).
   */
  boolean admits(W witness, ReplayView landing, EncodingState state);

  /**
   * Updates {@code state} for {@code applied}, which changed the table from {@code before} to
   * {@code after}. Total: never throws.
   */
  void observe(TableAction applied, ReplayView before, ReplayView after, EncodingState state);

  /** The codec of the encoding's state of a table, which checkpoints hold (S4, I11). */
  EncodingState.Codec stateCodec();
}
