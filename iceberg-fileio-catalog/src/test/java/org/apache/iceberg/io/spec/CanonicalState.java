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

import org.apache.iceberg.io.CatalogState;
import org.apache.iceberg.io.CatalogTestSupport;
import org.apache.iceberg.io.ProtoCodec;

/**
 * A catalog state that is re-canonicalised after every action: each
 * {@link #apply} applies the action through the production
 * {@link CatalogState.Builder} path and then round-trips the result through
 * encode→decode, so the held state is exactly what a reader of the encoded
 * bytes would see.
 *
 * <p>This is not an independent reference model — it runs the same action code
 * as the state it is compared against. {@link TestCanonicalStateProperty} uses
 * it to detect builder state that does not survive the codec.
 */
final class CanonicalState {

  private final CatalogState state;

  private CanonicalState(CatalogState state) {
    this.state = state;
  }

  /** Creates a {@code CanonicalState} seeded with the given catalog snapshot. */
  static CanonicalState from(CatalogState state) {
    return new CanonicalState(state);
  }

  /**
   * Applies {@code action} via the Builder path and returns a new
   * {@code CanonicalState} holding the round-tripped result. Callers must have
   * checked {@code action.verify(builder)} first.
   */
  CanonicalState apply(ProtoCodec.Action action) {
    CatalogState.Builder builder = SpecUtils.stateToBuilder(state);
    action.apply(builder);
    CatalogState next = builder.build();

    byte[] canonical = CatalogTestSupport.encodeFullFile(next);
    return new CanonicalState(CatalogTestSupport.decodeFullFile(canonical));
  }

  /** Encodes the current state to full LCAT wire bytes. */
  byte[] encode() {
    return CatalogTestSupport.encodeFullFile(state);
  }
}
