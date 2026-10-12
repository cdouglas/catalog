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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CatalogFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.ProtoCodec;
import org.apache.iceberg.io.ProtoCatalogFile;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * jqwik generators for V1b. Each generator supports shrinking. Keep generators
 * expansive (not just empty states) — see docs/superpowers/specs/
 * 2026-10-10-v1-protocol-correctness-design.md §9 "jqwik generator realism".
 *
 * <p>This file starts with a minimal catalogStates() that returns an
 * empty-but-well-formed catalog. Downstream tasks (Task 4+) add richer
 * generation of namespaces, tables, inline metadata, and actions.
 */
final class CatalogStateGenerators {

  private CatalogStateGenerators() {}

  /** Sentinel InputFile used when no real file path is needed (generator/test context). */
  static final InputFile STUB_LOCATION = new InputFile() {
    @Override public long getLength() { return 0; }
    @Override public SeekableInputStream newStream() { throw new UnsupportedOperationException(); }
    @Override public String location() { return "test://catalog-state-generator"; }
    @Override public boolean exists() { return false; }
  };

  /** Namespace names: 1–8 lowercase ASCII letters. */
  static Arbitrary<String> namespaceNames() {
    return Arbitraries.strings().withCharRange('a', 'z').ofMinLength(1).ofMaxLength(8);
  }

  /** Table names share the same character range as namespace names. */
  static Arbitrary<String> tableNames() {
    return namespaceNames();
  }

  /**
   * Catalog states: 0–3 namespaces, each with 0–3 pointer-mode tables.
   * Pointer-mode means the table's metadata is referenced by a URI, not inlined.
   *
   * <p>Uses {@link Arbitraries#randomValue} so jqwik treats the stream as
   * inexhaustible and runs all 1000 tries (Arbitraries.create() only runs 1).
   */
  static Arbitrary<CatalogFile> catalogStates() {
    return Arbitraries.randomValue(rng -> {
      ProtoCatalogFile.Builder builder =
          ProtoCatalogFile.builder(STUB_LOCATION).setCatalogUuid(UUID.randomUUID());

      // Draw 0–3 namespaces using the PRNG jqwik supplies.
      int nsCount = rng.nextInt(4); // 0, 1, 2, or 3
      int nextNsId = 1;
      int nextTblId = 1;

      // Track generated namespace names to avoid duplicate collisions.
      List<String> generatedNsNames = new ArrayList<>();

      for (int n = 0; n < nsCount; n++) {
        String nsName = randomLowercaseName(rng, 1, 8, generatedNsNames);
        if (nsName == null) {
          // Exhausted unique names for this draw; stop adding namespaces.
          break;
        }
        generatedNsNames.add(nsName);

        int nsId = nextNsId++;
        // parentId = 0 (root-level namespace, keeps the tree flat for Task 4).
        builder.addNamespace(nsId, 0, nsName, 1);

        int tblCount = rng.nextInt(4); // 0, 1, 2, or 3 tables per namespace
        List<String> generatedTblNames = new ArrayList<>();

        for (int t = 0; t < tblCount; t++) {
          String tblName = randomLowercaseName(rng, 1, 8, generatedTblNames);
          if (tblName == null) {
            break;
          }
          generatedTblNames.add(tblName);

          int tblId = nextTblId++;
          // Pointer-mode: metadataLocation is a URI, not null/inline bytes.
          String metadataLocation =
              "test://warehouse/" + nsName + "/" + tblName + "/v1.metadata.json";
          builder.addTable(tblId, nsId, tblName, 1, metadataLocation);
        }
      }

      builder.setNextNamespaceId(nextNsId);
      builder.setNextTableId(nextTblId);
      // rebuildLookups() is called inside build() already, but call it explicitly
      // after bulk-loading entries via addNamespace/addTable.
      return builder.build();
    });
  }

  /**
   * Generates a random lowercase name of length [minLen, maxLen] that does not
   * already appear in {@code existing}. Returns {@code null} if 20 attempts all
   * collide (extremely unlikely with name space >= 26^1 = 26 options).
   */
  private static String randomLowercaseName(
      java.util.Random rng, int minLen, int maxLen, List<String> existing) {
    for (int attempt = 0; attempt < 20; attempt++) {
      int len = minLen + (maxLen > minLen ? rng.nextInt(maxLen - minLen + 1) : 0);
      StringBuilder sb = new StringBuilder(len);
      for (int i = 0; i < len; i++) {
        sb.append((char) ('a' + rng.nextInt(26)));
      }
      String candidate = sb.toString();
      if (!existing.contains(candidate)) {
        return candidate;
      }
    }
    return null; // give up; caller skips this entry
  }

  /**
   * Action generator parameterised on a catalog state.
   *
   * <p>Produces actions that are plausibly applicable to {@code state}:
   * <ul>
   *   <li>{@link ProtoCodec.CreateNamespaceAction} — always applicable (creates
   *       a fresh root-level namespace with a freshly allocated ID).</li>
   *   <li>{@link ProtoCodec.CreateTableAction} — applicable only when at least
   *       one namespace exists; picks one at random.</li>
   *   <li>{@link ProtoCodec.DropTableAction} — applicable only when at least
   *       one pointer-mode table exists; picks one at random.</li>
   *   <li>{@link ProtoCodec.UpdateTableLocationAction} — applicable only when
   *       at least one pointer-mode table exists; picks one at random.</li>
   * </ul>
   *
   * <p>All ID / version values are taken from the current state so
   * {@code action.verify(builder)} succeeds on the first try in the majority
   * of cases.  The property's {@code if (action.verify(...))} guard still
   * handles the minority of races where the generator draws from an empty
   * set or produces a version mismatch.
   *
   * <p>Adaptation: the plan spec uses {@code action.verify(CatalogFile)} and
   * {@code action.apply(CatalogFile)}, but the real API is
   * {@code Action.verify(ProtoCatalogFile.Builder)} /
   * {@code Action.apply(ProtoCatalogFile.Builder)}.  The property class builds
   * a fresh Builder from the ProtoCatalogFile snapshot to mirror what the
   * production replay path does.
   *
   * <p>This generator uses only public accessors on {@code ProtoCatalogFile}:
   * {@code namespaces()}, {@code namespaceId(Namespace)},
   * {@code namespaceVersion(int)}, {@code tables()}, {@code tableId(TableIdentifier)},
   * {@code tableVersion(int)}, {@code tableEntry(int)}, {@code isInlineTable(int)},
   * {@code nextNamespaceId()}, {@code nextTableId()}.
   */
  static Arbitrary<ProtoCodec.Action> actions(CatalogFile state) {
    return Arbitraries.randomValue(rng -> {
      if (!(state instanceof ProtoCatalogFile)) {
        return freshCreateNamespace(rng, state);
      }
      ProtoCatalogFile pcf = (ProtoCatalogFile) state;

      // Collect ns IDs and versions using public accessors
      List<int[]> nsIdVersions = new ArrayList<>(); // [nsId, nsVersion]
      for (Namespace ns : pcf.namespaces()) {
        Integer nsId = pcf.namespaceId(ns);
        if (nsId != null) {
          nsIdVersions.add(new int[]{nsId, pcf.namespaceVersion(nsId)});
        }
      }

      // Collect pointer-mode table IDs and versions
      List<int[]> ptrTblIdVersions = new ArrayList<>(); // [tblId, tblVersion]
      for (TableIdentifier tbl : pcf.tables()) {
        Integer tblId = pcf.tableId(tbl);
        if (tblId != null && !pcf.isInlineTable(tblId)) {
          ptrTblIdVersions.add(new int[]{tblId, pcf.tableVersion(tblId)});
        }
      }

      // Weight toward actions that are applicable
      int maxKind = 0; // always at least CreateNamespace
      if (!nsIdVersions.isEmpty()) maxKind = 1;
      if (!ptrTblIdVersions.isEmpty()) maxKind = 3;

      int kind = rng.nextInt(maxKind + 1);

      switch (kind) {
        case 0:
          return freshCreateNamespace(rng, state);

        case 1: {
          // CreateTable in a randomly chosen existing namespace
          int[] nsIdVer = nsIdVersions.get(rng.nextInt(nsIdVersions.size()));
          int nsId = nsIdVer[0];
          int nsVersion = nsIdVer[1];
          int newTblId = pcf.nextTableId() + rng.nextInt(100) + 1;
          String tblName = randomLowercaseName(rng, 1, 8, java.util.Collections.emptyList());
          if (tblName == null) tblName = "tbl" + newTblId;
          String loc = "test://warehouse/ns" + nsId + "/" + tblName + "/v1.metadata.json";
          return new ProtoCodec.CreateTableAction(newTblId, 1, nsId, nsVersion, tblName, loc);
        }

        case 2: {
          // DropTable on a randomly chosen pointer-mode table
          int[] tblIdVer = ptrTblIdVersions.get(rng.nextInt(ptrTblIdVersions.size()));
          return new ProtoCodec.DropTableAction(tblIdVer[0], tblIdVer[1]);
        }

        default: {
          // UpdateTableLocation on a randomly chosen pointer-mode table
          int[] tblIdVer = ptrTblIdVersions.get(rng.nextInt(ptrTblIdVersions.size()));
          int tblId = tblIdVer[0];
          int tblVersion = tblIdVer[1];
          String newLoc = "test://warehouse/tbl" + tblId + "/v" + (tblVersion + 1) + ".metadata.json";
          return new ProtoCodec.UpdateTableLocationAction(tblId, tblVersion, newLoc);
        }
      }
    });
  }

  /**
   * Helper: creates a {@link ProtoCodec.CreateNamespaceAction} for a fresh
   * root-level namespace, choosing an ID that is one beyond the catalog's
   * current {@code nextNamespaceId}.
   */
  private static ProtoCodec.CreateNamespaceAction freshCreateNamespace(
      java.util.Random rng, CatalogFile state) {
    int baseNsId = (state instanceof ProtoCatalogFile)
        ? ((ProtoCatalogFile) state).nextNamespaceId()
        : 100;
    int newNsId = baseNsId + rng.nextInt(50);
    String name = randomLowercaseName(rng, 1, 8, java.util.Collections.emptyList());
    if (name == null) name = "ns" + newNsId;
    return new ProtoCodec.CreateNamespaceAction(newNsId, 1, 0, 0, name);
  }

  /**
   * Transaction generator for P5 (virtual-ID resolution).
   *
   * <p>Adaptation / SKIPPED note: The plan's P5 spec calls
   * {@code ProtoIdManager.allocate(txn)} / {@code ProtoIdManager.resolve(allocated)}
   * as a symmetric pair. The real {@link org.apache.iceberg.io.ProtoCatalogFormat.ProtoIdManager}
   * does not support this interface — it is a stateful sequential allocator that
   * remaps negative virtual IDs to real IDs during {@code buildActions()}; there
   * is no inverse {@code resolve(Transaction)} that maps real IDs back to virtuals.
   *
   * <p>Consequently, P5 is implemented as a compile-time placeholder that
   * returns a trivial no-op arbitray and is tagged SKIPPED_PENDING_CLARIFICATION
   * in the property class.  The generator is still provided here for forward
   * compatibility when/if the production API gains a symmetric round-trip.
   *
   * <p>Returns 1–4 {@link ProtoCodec.Transaction} instances built from
   * {@link ProtoCodec.CreateNamespaceAction}s with real (positive) IDs.
   */
  static Arbitrary<ProtoCodec.Transaction> transactions() {
    return Arbitraries.randomValue(rng -> {
      int count = 1 + rng.nextInt(4);
      List<ProtoCodec.Action> actions = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        int nsId = i + 1;
        String name = randomLowercaseName(rng, 1, 8, java.util.Collections.emptyList());
        if (name == null) name = "ns" + nsId;
        actions.add(new ProtoCodec.CreateNamespaceAction(nsId, 1, 0, 0, name));
      }
      return new ProtoCodec.Transaction(UUID.randomUUID(), actions);
    });
  }
}
