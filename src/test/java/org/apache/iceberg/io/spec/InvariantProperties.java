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

import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.From;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.statistics.Statistics;
import org.apache.iceberg.io.CatalogFile;
import org.apache.iceberg.io.ProtoCodec;
import org.apache.iceberg.io.ProtoCatalogFile;

/**
 * Single-state structural invariants for V1b.
 *
 * <p>P4 — Well-formedness preservation: {@code wellFormed(s) ∧ verify(a, s) ⇒ wellFormed(apply(a, s))}.
 * <br>P5 — Virtual-ID resolution: {@code resolve ∘ allocate = id} on virtual IDs.
 *
 * <p>Adaptations from the plan spec:
 * <ul>
 *   <li>{@code Action.verify} and {@code Action.apply} operate on
 *       {@link ProtoCatalogFile.Builder}, not on {@code CatalogFile} directly.
 *       P4 therefore builds a mutable Builder snapshot of the state, calls
 *       {@code verify(builder)} to test pre-conditions, and — when they hold —
 *       calls {@code apply(builder)} then {@code builder.build()} to obtain the
 *       successor {@code CatalogFile}.</li>
 *   <li>P5 is SKIPPED_PENDING_CLARIFICATION: the plan references
 *       {@code ProtoIdManager.allocate(txn)} / {@code ProtoIdManager.resolve(allocated)}
 *       as a symmetric round-trip pair, but the real
 *       {@link org.apache.iceberg.io.ProtoCatalogFormat.ProtoIdManager} is a
 *       stateful sequential allocator that remaps negative virtual IDs to real
 *       IDs during {@code Mut.buildActions()}. No inverse
 *       {@code resolve(Transaction)} exists — once a virtual ID is remapped to
 *       a real ID the mapping is one-way within that {@code ProtoIdManager}
 *       instance. P5 is recorded here as a disabled test with an explanatory
 *       comment; it will be re-enabled when/if the production API gains a
 *       symmetric round-trip.</li>
 * </ul>
 */
class InvariantProperties {

  /**
   * P4 — Well-formedness preservation.
   *
   * <p>For every generated catalog state {@code s} and action {@code a}:
   * <ol>
   *   <li>The state itself is well-formed ({@link WellFormed#check}).</li>
   *   <li>If {@code a.verify(builder)} succeeds (pre-conditions hold), then
   *       the successor state produced by {@code a.apply(builder)} is also
   *       well-formed.</li>
   * </ol>
   *
   * <p>jqwik statistics are collected to surface coverage: the label
   * {@code "apply-path"} counts the fraction of tries where {@code verify}
   * returned true and the apply path was exercised.
   */
  @Property
  void p4_wellFormednessPreserved(
      @ForAll @From("catalogStates") CatalogFile state,
      @ForAll @From("actionsFor") ProtoCodec.Action action) {
    assertThat(WellFormed.check(state))
        .as("generated catalog state must itself be well-formed: %s", state)
        .isTrue();

    if (!(state instanceof ProtoCatalogFile)) {
      // Defensive: all generated states are ProtoCatalogFile; skip if not.
      Statistics.label("apply-path").collect("skipped-not-proto");
      return;
    }
    ProtoCatalogFile pcf = (ProtoCatalogFile) state;

    // Build a mutable Builder snapshot of the state — mirrors what the
    // production transaction-replay path does in ProtoCatalogFormat.readInternal.
    ProtoCatalogFile.Builder builder = stateToBuilder(pcf);

    boolean applicable = action.verify(builder);
    Statistics.label("apply-path").collect(applicable ? "applied" : "skipped-verify-false");

    if (applicable) {
      action.apply(builder);
      ProtoCatalogFile next = builder.build();
      assertThat(WellFormed.check(next))
          .as("action %s broke well-formedness starting from %s", action, state)
          .isTrue();
    }
  }

  /**
   * P5 — Virtual-ID resolution.
   *
   * <p>SKIPPED_PENDING_CLARIFICATION: the plan's P5 spec calls
   * {@code ProtoIdManager.allocate(txn)} / {@code ProtoIdManager.resolve(allocated)}
   * as a symmetric pair (the round-trip {@code resolve(allocate(txn)) ≡ txn}).
   * The real {@code ProtoIdManager} is a stateful sequential allocator whose
   * {@code remap(virtualId)} method emits a fresh real ID for each negative virtual
   * ID encountered during {@code Mut.buildActions()}; there is no inverse
   * {@code resolve(Transaction)} that maps real IDs back to virtual IDs.
   *
   * <p>The late-binding mechanism for virtual IDs works correctly in production —
   * negative parent IDs in {@link ProtoCodec.CreateNamespaceAction} are resolved
   * to real IDs when the transaction is replayed — but the round-trip property
   * cannot be expressed against the current API without adding new production
   * code. P5 is therefore not implemented as a jqwik {@code @Property}.
   *
   * <p>This comment serves as the SKIPPED marker. The transactions() generator
   * in {@link CatalogStateGenerators#transactions()} is provided for forward
   * compatibility; it generates transactions with real (positive) IDs only
   * since the virtual-ID API surface is not exposed for testing.
   */
  // @Property  -- P5 SKIPPED_PENDING_CLARIFICATION (see Javadoc above)
  // void p5_virtualIdResolution(@ForAll @From("transactions") ProtoCodec.Transaction txn) { ... }

  @Provide
  Arbitrary<CatalogFile> catalogStates() {
    return CatalogStateGenerators.catalogStates();
  }

  @Provide
  Arbitrary<ProtoCodec.Action> actionsFor() {
    return catalogStates().flatMap(CatalogStateGenerators::actions);
  }

  @Provide
  Arbitrary<ProtoCodec.Transaction> transactions() {
    return CatalogStateGenerators.transactions();
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  /**
   * Delegates to {@link SpecUtils#stateToBuilder(ProtoCatalogFile)}.
   *
   * <p>The implementation was extracted to {@link SpecUtils} so that P6
   * ({@link ModelEquivalenceProperty}) can reuse it without duplication.
   */
  private static ProtoCatalogFile.Builder stateToBuilder(ProtoCatalogFile pcf) {
    return SpecUtils.stateToBuilder(pcf);
  }
}
