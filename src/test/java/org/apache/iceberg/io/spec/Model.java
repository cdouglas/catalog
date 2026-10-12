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

import org.apache.iceberg.io.CatalogFile;
import org.apache.iceberg.io.ProtoCatalogFile;
import org.apache.iceberg.io.ProtoCatalogFormatTestSupport;
import org.apache.iceberg.io.ProtoCodec;

/**
 * Pure, canonical reference implementation of catalog state for V1b P6.
 *
 * <p>The purpose of this class is to serve as a specification-level model against
 * which the production {@link ProtoCatalogFile.Builder}'s incremental updates are
 * checked. Every {@link #apply} call rebuilds the full state via an encode→decode
 * round-trip, ensuring the model always holds a fully canonical representation —
 * the same bytes a reader of the committed file would see.
 *
 * <p><b>Design rationale:</b> If the real {@code Builder} accumulates hidden
 * intermediate state that encodes differently from a freshly decoded snapshot,
 * {@link ModelEquivalenceProperty} will detect the divergence by comparing
 * {@link #encode()} (model bytes, always canonical) against
 * {@link ProtoCatalogFormatTestSupport#encodeFullFile} applied to the builder-derived
 * snapshot. Intentionally simpler and slower than the production path — correctness
 * matters here, not performance.
 *
 * <p><b>Adaptation note:</b> The plan spec's template called
 * {@code Action.verify(CatalogFile)} / {@code Action.apply(CatalogFile)}, but the
 * real {@code ProtoCodec.Action} API takes a {@link ProtoCatalogFile.Builder}.
 * {@code Model.apply} therefore builds a {@code Builder} from the current snapshot
 * via {@link SpecUtils#stateToBuilder}, applies the action, builds back to a
 * snapshot, and then encodes→decodes for canonicalisation.
 */
final class Model {

  private final CatalogFile state;

  private Model(CatalogFile state) {
    this.state = state;
  }

  /** Creates a {@code Model} seeded with the given catalog snapshot. */
  static Model from(CatalogFile state) {
    return new Model(state);
  }

  /**
   * Applies {@code action} via the real Builder path and returns a new
   * {@code Model} whose state is the canonical round-tripped result.
   *
   * <p>The round-trip (encode→decode) guarantees that the model never carries
   * builder-internal transient state: what is stored is exactly what would be
   * written to disk and re-read by a clean reader.
   *
   * <p>Callers are responsible for checking {@code action.verify(builder)} before
   * calling this method; the precondition is assumed to hold.
   *
   * @param action the action to apply; its preconditions must already be satisfied
   * @return a new {@code Model} with the canonical post-apply state
   */
  Model apply(ProtoCodec.Action action) {
    ProtoCatalogFile pcf = (ProtoCatalogFile) state;
    ProtoCatalogFile.Builder builder = SpecUtils.stateToBuilder(pcf);
    action.apply(builder);
    ProtoCatalogFile next = builder.build();

    // Canonical round-trip: encode then decode forces a clean-slate snapshot.
    byte[] canonical = ProtoCatalogFormatTestSupport.encodeFullFile(next);
    return new Model(ProtoCatalogFormatTestSupport.decodeFullFile(canonical));
  }

  /**
   * Encodes the current model state to full LCAT wire bytes.
   *
   * @return the encoded bytes, identical to what a committed file would contain
   */
  byte[] encode() {
    return ProtoCatalogFormatTestSupport.encodeFullFile((ProtoCatalogFile) state);
  }
}
