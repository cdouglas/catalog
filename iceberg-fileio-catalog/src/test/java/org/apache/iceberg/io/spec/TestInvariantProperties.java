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
import net.jqwik.api.Tuple;
import net.jqwik.api.statistics.Statistics;
import org.apache.iceberg.io.CatalogFile;
import org.apache.iceberg.io.CatalogState;
import org.apache.iceberg.io.ProtoCodec;

/**
 * Single-state structural invariants.
 *
 * <p>P4 — Well-formedness preservation: {@code wellFormed(s) ∧ verify(a, s) ⇒ wellFormed(apply(a, s))}.
 *
 * <p>{@code Action.verify} and {@code Action.apply} operate on a
 * {@link CatalogState.Builder}, so P4 builds a Builder from the state, calls
 * {@code verify}, and when it holds calls {@code apply} then {@code build()} to
 * obtain the successor state.
 *
 * <p>Known limitation: {@link #actionsFor()} draws each action from its own
 * generated state, not from the state under test, so many actions do not apply
 * (see the {@code apply-path} statistics), and {@link WellFormed} checks lookup
 * consistency only, not referential integrity.
 */
class TestInvariantProperties {

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
      @ForAll @From("statesWithActions") Tuple.Tuple2<CatalogFile, ProtoCodec.Action> drawn) {
    CatalogFile state = drawn.get1();
    ProtoCodec.Action action = drawn.get2();
    assertThat(WellFormed.check(state))
        .as("generated catalog state must itself be well-formed: %s", state)
        .isTrue();

    CatalogState.Builder builder = SpecUtils.stateToBuilder((CatalogState) state);

    boolean applicable = action.verify(builder);
    Statistics.label("apply-path").collect(applicable ? "applied" : "skipped-verify-false");

    if (applicable) {
      action.apply(builder);
      CatalogState next = builder.build();
      assertThat(WellFormed.check(next))
          .as("action %s broke well-formedness starting from %s", action, state)
          .isTrue();
    }
  }

  @Provide
  Arbitrary<CatalogFile> catalogStates() {
    return CatalogStateGenerators.catalogStates();
  }

  /** A state and an action drawn for it (one drawn for another state may verify by chance). */
  @Provide
  Arbitrary<Tuple.Tuple2<CatalogFile, ProtoCodec.Action>> statesWithActions() {
    return catalogStates()
        .flatMap(s -> CatalogStateGenerators.actions(s).map(a -> Tuple.of(s, a)));
  }
}
