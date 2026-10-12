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
package org.apache.iceberg.io.spec;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.UUID;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.From;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import org.apache.iceberg.io.CatalogFile;
import org.apache.iceberg.io.DedupHorizonExceededException;
import org.apache.iceberg.io.ProtoCodec;
import org.apache.iceberg.io.ProtoCatalogFile;
import org.apache.iceberg.io.UuidV7;

/**
 * P7 — Retention-vs-retry bound (V1 design §4.1).
 *
 * <p><b>Claim:</b> For every reachable catalog state and every writer holding an
 * outstanding retry carrying a committed transaction UUID {@code u}, {@code u} is
 * present in the {@code committedTransactions} set of the catalog state that the
 * retry observes. Equivalently: once a transaction is successfully committed its
 * UUID must survive in the dedup set for at least as long as a writer might still
 * be retrying that transaction.
 *
 * <p><b>Simulation model.</b> The full temporal statement ("UUID stays in the set
 * across TLC-style interleavings until the GC watermark overtakes it") is not
 * directly expressible as a single-execution jqwik property. We validate the
 * <i>structural</i> core of the claim with a bounded-retry-window simulation:
 * <ol>
 *   <li>Start from a generated catalog state {@code seed}.</li>
 *   <li>Walk a sequence of candidate transactions. Maintain a FIFO
 *       {@code outstandingRetries} queue of UUIDs that have been committed and
 *       are still "within the retry window" — i.e. a writer could still be
 *       retrying them.</li>
 *   <li>When the queue exceeds {@code retryWindow}, drop the oldest entry (that
 *       writer is modelled as having given up; it is no longer waiting on
 *       retention).</li>
 *   <li><b>After every step</b> assert that every UUID still inside the retry
 *       window remains in the catalog's committed-transaction set.</li>
 * </ol>
 *
 * <p><b>Why this is non-tautological.</b> The retry window is a real bound on
 * which UUIDs the property is actively tracking: as new commits arrive, old
 * ones leave the window and stop being checked. The per-step assertion then
 * compares the test's "still-retrying" cohort against the catalog's dedup set.
 * The {@code ProtoCatalogFile.Builder} today has no path that drops UUIDs from
 * the committed set, so the property passes — but it gains real teeth the
 * moment any future retention/GC logic can evict a UUID: if that logic ever
 * drops a UUID while the test's window still holds it, this property fails.
 *
 * <p>Each simulated transaction is a single action wrapped in a
 * {@link ProtoCodec.Transaction} with a fresh UUIDv7 (the unit the dedup set
 * stores). A transaction is "committed" iff {@code txn.verify(builder)} returns
 * {@code true}; we then call {@code txn.apply(builder)} and
 * {@code builder.addCommittedTransaction(txn.id())} to mirror the production
 * commit path.
 *
 * <p><b>API adaptations from the plan template.</b>
 * <ul>
 *   <li>{@code Action.verify/apply} take {@link ProtoCatalogFile.Builder} — we
 *       maintain a {@code Builder} across the window and call these directly on
 *       the {@code Transaction} wrapper, which delegates to its actions.</li>
 *   <li>{@code transactionUuid} — lives on {@link ProtoCodec.Transaction#id()},
 *       not on individual {@link ProtoCodec.Action} instances. We wrap each
 *       generated action in a fresh single-action {@code Transaction} with a
 *       {@link UuidV7#newUuidV7()} UUID, one per retry-window step.</li>
 *   <li>{@code committedTxns} — exposed as {@link ProtoCatalogFile#committedTransactions()}
 *       (returns {@code Set<UUID>}) and as the membership predicate
 *       {@link ProtoCatalogFile#containsTransaction(UUID)}. The {@code Builder}
 *       exposes a mirror {@link ProtoCatalogFile.Builder#containsTransaction(UUID)}
 *       that checks the pending committed set without materializing a full
 *       {@link ProtoCatalogFile}; we use the Builder predicate for the per-step
 *       check to avoid rebuilding the catalog on every iteration.</li>
 * </ul>
 *
 * <p><b>Deferred to V1a.</b> The full temporal statement (GC retention vs
 * retry across TLC-style interleavings) is deferred to V1a (TLA+ model) if that
 * side chooses to lift it. The structural per-step check here is the jqwik
 * counterpart.
 */
class RetentionBoundProperty {

  @Property(tries = 200)
  void p7_retainedWhileRetrying(
      @ForAll @From("catalogStates") CatalogFile seed,
      @ForAll @From("actionLists") List<ProtoCodec.Action> actions,
      @ForAll @IntRange(min = 1, max = 5) int retryWindow) {

    if (!(seed instanceof ProtoCatalogFile)) {
      // catalogStates() always produces ProtoCatalogFile; guard for safety.
      return;
    }

    // Build a mutable builder from the seed state — mirrors the production
    // checkpoint-replay path.
    ProtoCatalogFile.Builder builder = SpecUtils.stateToBuilder((ProtoCatalogFile) seed);

    // FIFO of UUIDs that have been committed and are still "inside the retry
    // window" — i.e. a writer could still be retrying them. When the queue
    // exceeds retryWindow we evict the oldest entry (that writer is modelled
    // as having given up and is no longer waiting on retention).
    Deque<UUID> outstandingRetries = new ArrayDeque<>();

    // Walk every candidate action; the retryWindow bounds which committed
    // UUIDs the per-step check is actively tracking, not the loop length.
    // (Reading the Builder's pending committed set via
    // Builder#containsTransaction avoids rebuilding ProtoCatalogFile on each
    // step; at 200 tries * up to 5 steps this is ~1000 iterations, so the
    // choice is about clarity, not throughput.)
    for (ProtoCodec.Action action : actions) {

      // Wrap the single action in a Transaction with a fresh UUIDv7.
      // This is the unit the dedup set tracks; the UUID represents "this
      // writer's outstanding retry token" for the purpose of P7.
      UUID txnId = UuidV7.newUuidV7();
      ProtoCodec.Transaction txn =
          new ProtoCodec.Transaction(txnId, java.util.Collections.singletonList(action));

      // Verify + apply, catching any exception as a verify-false (same
      // exception-swallowing-as-skip pattern used in prior properties).
      try {
        if (txn.verify(builder)) {
          txn.apply(builder);
          builder.addCommittedTransaction(txnId);
          outstandingRetries.addLast(txnId);
        }
      } catch (Exception ignored) {
        // verify/apply threw — treat as a verification failure and skip.
      }

      // Retention trimming: UUIDs older than retryWindow leave the window.
      // No eviction from the catalog happens here (the Builder has no API
      // for that today); we simply stop tracking them.
      while (outstandingRetries.size() > retryWindow) {
        outstandingRetries.pollFirst();
      }

      // P7 per-step check: every UUID still inside the retry window must
      // remain present in the catalog's committed-transaction set. This is
      // the non-tautological core of the property — if a future
      // retention/GC path ever drops a UUID while the window still holds
      // it, this assertion fails.
      for (UUID u : outstandingRetries) {
        try {
          assertThat(builder.containsTransaction(u))
              .as("P7 violated: UUID %s dropped while still in retry window", u)
              .isTrue();
        } catch (DedupHorizonExceededException horizon) {
          // Builder#containsTransaction throws when a v7 UUID is missing
          // AND at/below the GC horizon. For UUIDs just minted in this
          // test the horizon branch is unreachable in normal operation;
          // treat an occurrence as a P7 violation (the UUID is gone and
          // the horizon has moved past it).
          throw new AssertionError(
              "P7 violated: UUID " + u + " missing below GC horizon", horizon);
        }
      }
    }
  }

  @Provide
  Arbitrary<CatalogFile> catalogStates() {
    return CatalogStateGenerators.catalogStates();
  }

  @Provide
  Arbitrary<List<ProtoCodec.Action>> actionLists() {
    // Generate 1–5 actions from a fresh empty state so we always have a
    // non-empty list to draw from. The verify guard inside the property handles
    // actions that are inapplicable to the actual seed state.
    return CatalogStateGenerators
        .catalogStates()
        .flatMap(
            s -> CatalogStateGenerators.actions(s).list().ofMinSize(1).ofMaxSize(5));
  }
}
