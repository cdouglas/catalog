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

import java.util.List;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.From;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.Size;
import org.apache.iceberg.io.CatalogState;
import org.apache.iceberg.io.CatalogTestSupport;
import org.apache.iceberg.io.ProtoCodec;

/**
 * P6 — Builder state survives the codec.
 *
 * <p>Replays a list of actions two ways from the same seed: incrementally on a
 * {@link CatalogState.Builder} ({@code stateToBuilder → verify → apply →
 * build}), and on a {@link CanonicalState}, which additionally encodes and
 * decodes after every step. Both use the same action code, so this is a check
 * that encode/decode is stable along action sequences, not a comparison against
 * an independent model: if the Builder accumulates state that encodes
 * differently from a freshly decoded state, the final bytes differ.
 *
 * <p>Each action is drawn for the state it applies to, from one of {@code seeds}. An
 * action drawn from an unrelated state could pass verify by coincidence (its parent's
 * version number happening to match) and then add a second namespace of a name the
 * state has, which no single catalog history can produce.
 */
class TestCanonicalStateProperty {

  @Property(tries = 200)
  void p6_builderStateSurvivesCodec(
      @ForAll @From("catalogStates") CatalogState seed,
      @ForAll @Size(min = 1, max = 6) List<Long> seeds) {
    CanonicalState canonical = CanonicalState.from(seed);
    CatalogState real = seed;

    for (long actionSeed : seeds) {
      ProtoCodec.Action a = CatalogStateGenerators.action(real, new java.util.Random(actionSeed));
      CatalogState.Builder builder = SpecUtils.stateToBuilder(real);
      if (!a.verify(builder)) {
        continue;
      }
      // An action that verifies must apply cleanly on both paths; an exception
      // here is a finding, not an inapplicable action.
      a.apply(builder);
      real = builder.build();
      canonical = canonical.apply(a);
    }

    assertThat(canonical.encode()).isEqualTo(CatalogTestSupport.encodeFullFile(real));
  }

  @Provide
  Arbitrary<CatalogState> catalogStates() {
    return CatalogStateGenerators.catalogStates();
  }
}
