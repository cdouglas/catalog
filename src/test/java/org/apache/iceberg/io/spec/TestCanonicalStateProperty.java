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
import org.apache.iceberg.io.CatalogFile;
import org.apache.iceberg.io.ProtoCatalogFile;
import org.apache.iceberg.io.ProtoCatalogFormatTestSupport;
import org.apache.iceberg.io.ProtoCodec;

/**
 * P6 — Builder state survives the codec.
 *
 * <p>Replays a list of actions two ways from the same seed: incrementally on a
 * {@link ProtoCatalogFile.Builder} ({@code stateToBuilder → verify → apply →
 * build}), and on a {@link CanonicalState}, which additionally encodes and
 * decodes after every step. Both use the same action code, so this is a check
 * that encode/decode is stable along action sequences, not a comparison against
 * an independent model: if the Builder accumulates state that encodes
 * differently from a freshly decoded state, the final bytes differ.
 *
 * <p>Known limitation: {@link #actionLists()} draws each action from its own
 * generated state, so many actions do not apply to the seed.
 */
class TestCanonicalStateProperty {

  @Property(tries = 200)
  void p6_builderStateSurvivesCodec(
      @ForAll @From("catalogStates") CatalogFile seed,
      @ForAll @From("actionLists") List<ProtoCodec.Action> actions) {
    CanonicalState canonical = CanonicalState.from(seed);
    ProtoCatalogFile real = (ProtoCatalogFile) seed;

    for (ProtoCodec.Action a : actions) {
      ProtoCatalogFile.Builder builder = SpecUtils.stateToBuilder(real);
      if (!a.verify(builder)) {
        continue;
      }
      // An action that verifies must apply cleanly on both paths; an exception
      // here is a finding, not an inapplicable action.
      a.apply(builder);
      real = builder.build();
      canonical = canonical.apply(a);
    }

    assertThat(canonical.encode()).isEqualTo(ProtoCatalogFormatTestSupport.encodeFullFile(real));
  }

  @Provide
  Arbitrary<CatalogFile> catalogStates() {
    return CatalogStateGenerators.catalogStates();
  }

  @Provide
  Arbitrary<List<ProtoCodec.Action>> actionLists() {
    return CatalogStateGenerators.catalogStates()
        .flatMap(CatalogStateGenerators::actions)
        .list()
        .ofMinSize(1)
        .ofMaxSize(6);
  }
}
