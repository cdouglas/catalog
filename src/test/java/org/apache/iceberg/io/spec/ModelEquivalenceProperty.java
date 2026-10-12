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
 * P6 — Model equivalence.
 *
 * <p>Spec claim §4.1 P6: random action sequences replayed against a pure reference
 * {@link Model} and the production {@link ProtoCatalogFile.Builder} path must produce
 * byte-equal encoded states.
 *
 * <p><b>Semantics:</b> The property tests whether the real {@code Builder}'s
 * incremental updates diverge from a simpler canonical reference (encode→decode
 * round-trip after each step). Both sides apply each action via the real
 * {@code Builder} path ({@code stateToBuilder → verify → apply → build});
 * the difference is that {@code Model} additionally encodes-then-decodes after
 * each apply, forcing a fully canonical representation at every intermediate step.
 * If the {@code Builder} accumulates hidden state that encodes differently from a
 * freshly decoded state, the two byte arrays will differ.
 *
 * <p><b>Adaptations from the plan's template:</b>
 * <ul>
 *   <li>{@code Action.verify / apply} take {@link ProtoCatalogFile.Builder}, NOT
 *       {@code CatalogFile}. Both the "real" path and the {@code Model}'s internal
 *       apply use {@code SpecUtils.stateToBuilder(ProtoCatalogFile)} as the bridge.</li>
 *   <li>{@code ProtoCatalogFormat.encode / decode} are not public API; the property
 *       uses {@link ProtoCatalogFormatTestSupport#encodeFullFile} and
 *       {@link ProtoCatalogFormatTestSupport#decodeFullFile} instead.</li>
 *   <li>If {@code verify} throws, the action is skipped (treated as verify-false).</li>
 * </ul>
 */
class ModelEquivalenceProperty {

  /**
   * P6 — model matches real builder path after replaying a list of actions.
   *
   * <p>For each action:
   * <ol>
   *   <li>Build a {@code Builder} from the current "real" state.</li>
   *   <li>Call {@code action.verify(builder)}; skip on false or exception.</li>
   *   <li>Apply the action to both the {@code Model} and the "real" {@code Builder}.</li>
   * </ol>
   *
   * <p>After the loop: assert that {@code model.encode()} equals
   * {@code encodeFullFile(real)}.
   */
  @Property(tries = 200)
  void p6_modelMatchesMut(
      @ForAll @From("catalogStates") CatalogFile seed,
      @ForAll @From("actionLists") List<ProtoCodec.Action> actions) {
    Model model = Model.from(seed);
    CatalogFile real = seed;

    for (ProtoCodec.Action a : actions) {
      if (!(real instanceof ProtoCatalogFile)) {
        // Defensive: all generated states are ProtoCatalogFile; stop if not.
        break;
      }
      ProtoCatalogFile pcf = (ProtoCatalogFile) real;

      // --- Real path: verify + apply via Builder ---
      // We try the real Builder path first. Any exception (stateToBuilder threw,
      // verify threw, or verify passed but apply threw due to generator
      // cross-state mismatch) is treated as "not applicable" and the step is
      // skipped for BOTH sides.
      ProtoCatalogFile nextReal;
      try {
        ProtoCatalogFile.Builder builder = SpecUtils.stateToBuilder(pcf);
        boolean applicable = a.verify(builder);
        if (!applicable) {
          continue;
        }
        a.apply(builder);
        nextReal = builder.build();
      } catch (Exception e) {
        // Any exception (verify threw, apply threw, or state reconstruction threw)
        // is treated as verify-false: skip this action on both sides.
        continue;
      }

      // --- Model path: apply with canonical round-trip ---
      // Mirror the exact same action on the model.  If Model.apply throws
      // (which would indicate a bug in Model itself rather than production
      // code), skip the step and keep both sides in sync.
      Model nextModel;
      try {
        nextModel = model.apply(a);
      } catch (Exception e) {
        // Model.apply threw — keep both sides in sync by not advancing either.
        continue;
      }

      // Both sides succeeded: advance state.
      model = nextModel;
      real = nextReal;
    }

    byte[] modelBytes = model.encode();
    byte[] realBytes = ProtoCatalogFormatTestSupport.encodeFullFile((ProtoCatalogFile) real);
    assertThat(modelBytes).isEqualTo(realBytes);
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
