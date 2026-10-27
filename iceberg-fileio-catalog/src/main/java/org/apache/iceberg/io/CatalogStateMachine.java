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

import org.intentlog.StateMachine;

/**
 * The catalog as a state machine over an intention log: its state is namespaces, their
 * properties, and tables with their metadata (a pointer to a metadata file, or inline metadata
 * and manifest lists); an intention is a transaction's actions, with the versions they depend on.
 * Its checkpoints and intentions are catalog.proto's {@code Checkpoint} and {@code Intention}.
 */
final class CatalogStateMachine
    implements StateMachine<CatalogState, CatalogState.Builder, ProtoCodec.CatalogIntention> {

  static final CatalogStateMachine INSTANCE = new CatalogStateMachine();

  private CatalogStateMachine() {}

  @Override
  public CatalogState.Builder empty() {
    return CatalogState.builder();
  }

  @Override
  public CatalogState.Builder decodeCheckpoint(byte[] state) {
    CatalogState.Builder builder = CatalogState.builder();
    ProtoCodec.decodeCheckpointState(state, builder);
    return builder;
  }

  /** A builder that continues from {@code state}, sharing it: no checkpoint round trip. */
  @Override
  public CatalogState.Builder toBuilder(CatalogState state) {
    return state.toBuilder();
  }

  @Override
  public byte[] encodeCheckpoint(CatalogState state) {
    return ProtoCodec.encodeCheckpointState(state);
  }

  @Override
  public ProtoCodec.CatalogIntention decodeIntention(byte[] intention) {
    return ProtoCodec.decodeIntention(intention);
  }

  @Override
  public byte[] encodeIntention(ProtoCodec.CatalogIntention intention) {
    return ProtoCodec.encodeIntention(intention);
  }
}
