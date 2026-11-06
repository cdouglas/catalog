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

  /** The state machine without a memo: every checkpoint encodes every table. */
  static final CatalogStateMachine INSTANCE = new CatalogStateMachine(null);

  // The encodings of inline tables a checkpoint may write again, or null (CheckpointMemo)
  private final CheckpointMemo memo;

  private CatalogStateMachine(CheckpointMemo memo) {
    this.memo = memo;
  }

  /**
   * A state machine for one catalog: with its own {@link CheckpointMemo} if {@code memoize}, else
   * {@link #INSTANCE}.
   */
  static CatalogStateMachine forCatalog(boolean memoize) {
    return memoize ? new CatalogStateMachine(new CheckpointMemo()) : INSTANCE;
  }

  /** This machine's memo, or null. */
  CheckpointMemo memo() {
    return memo;
  }

  @Override
  public CatalogState.Builder empty() {
    return CatalogState.builder();
  }

  @Override
  public CatalogState.Builder decodeCheckpoint(byte[] state) {
    CatalogState.Builder builder = CatalogState.builder();
    ProtoCodec.decodeCheckpointState(state, builder, memo);
    return builder;
  }

  /** A builder that continues from {@code state}, sharing it: no checkpoint round trip. */
  @Override
  public CatalogState.Builder toBuilder(CatalogState state) {
    return state.toBuilder();
  }

  @Override
  public byte[] encodeCheckpoint(CatalogState state) {
    return ProtoCodec.encodeCheckpointState(state, memo);
  }

  @Override
  public ProtoCodec.CatalogIntention decodeIntention(byte[] intention) {
    return ProtoCodec.decodeIntention(intention);
  }

  @Override
  public byte[] encodeIntention(ProtoCodec.CatalogIntention intention) {
    return ProtoCodec.encodeIntention(intention);
  }

  /**
   * The pages of the blocks this state's records sealed that no compactor has stored yet: a
   * checkpoint of it names them (SNAPSHOT_PAGING.md, "The root and the pages").
   */
  @Override
  public java.util.Map<String, byte[]> pages(CatalogState state) {
    java.util.Map<String, byte[]> pages = new java.util.LinkedHashMap<>();
    state.allSealedBlocks().forEach((tblId, blocks) -> {
      for (SnapshotPages.SealedBlock block : blocks.values()) {
        byte[] bytes = block.pending != null ? block.pending.bytes() : null;
        if (bytes != null) {
          pages.put(block.pageName(tblId), bytes);
        }
      }
    });
    return pages;
  }

  @Override
  public java.util.Collection<String> droppedPages(CatalogState state) {
    java.util.List<String> names = new java.util.ArrayList<>();
    for (CatalogState.DroppedPage page : state.droppedPages()) {
      if (!page.deleted) {
        names.add(page.name);
      }
    }
    return names;
  }

  @Override
  public void pagesDeleted(CatalogState state, java.util.Collection<String> names) {
    java.util.Set<String> deleted = new java.util.HashSet<>(names);
    for (CatalogState.DroppedPage page : state.droppedPages()) {
      if (deleted.contains(page.name)) {
        page.deleted = true;
      }
    }
  }

  /**
   * The snapshot pages among {@code names} that are garbage under {@code state} and any state after
   * it (SNAPSHOT_PAGING.md, "Page GC"): of a table it dropped, or sealed at a version of the table
   * it has reached and not among its sealed blocks (the block emptied, or the table left inline
   * storage, whose blocks seal under new names if it comes back). A page of a table it does not
   * know yet, or sealed at a later version than its own, is not.
   */
  @Override
  public java.util.Set<String> garbagePages(
      CatalogState state, java.util.Collection<String> names) {
    java.util.Set<String> garbage = new java.util.HashSet<>();
    for (String name : names) {
      long[] page = SnapshotPages.parseName(name);
      if (page == null) {
        continue;
      }
      int tblId = (int) page[0];
      if (tblId >= state.nextTableId()) {
        continue; // a table created after this state
      }
      CatalogState.TblEntry table = state.tableById().get(tblId);
      if (table == null) {
        garbage.add(name); // dropped: ids are not reused
        continue;
      }
      if (page[1] > table.version) {
        continue; // sealed by a record after this state
      }
      SnapshotPages.SealedBlock block = state.sealedBlocks(tblId).get(page[2]);
      if (block == null || !block.pageName(tblId).equals(name)) {
        garbage.add(name);
      }
    }
    return garbage;
  }

  /** Drops the stored pages' bytes: every later state shares the entries that hold them. */
  @Override
  public void pagesPut(CatalogState state, java.util.Collection<String> names) {
    java.util.Set<String> stored = new java.util.HashSet<>(names);
    state.allSealedBlocks().forEach((tblId, blocks) -> {
      for (SnapshotPages.SealedBlock block : blocks.values()) {
        if (block.pending != null && stored.contains(block.pageName(tblId))) {
          block.pending.stored();
        }
      }
    });
  }
}
