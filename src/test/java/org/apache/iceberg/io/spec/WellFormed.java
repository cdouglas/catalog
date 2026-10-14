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

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CatalogFile;
import org.apache.iceberg.io.ProtoCatalogFile;

/**
 * Structural well-formedness check for P4 ({@link TestInvariantProperties}).
 * Encodes invariants that hold on a single state:
 *
 * <ul>
 *   <li>I5: every inline-ML sentinel has a matching pool entry (not checked
 *       here — inline tables use opaque byte[] for metadata, and the manifest
 *       pool / sentinel structure is only observable through package-private
 *       accessors not reachable from {@code CatalogFile}; see proxy note
 *       below).</li>
 *   <li>Namespace lookup consistency: every namespace accessible via
 *       {@code namespaces()} is round-trip consistent with
 *       {@code containsNamespace()}.</li>
 *   <li>Table lookup consistency: every table in {@code tables()} appears
 *       exactly once, and {@code location()} or {@code isInlineTable()} is
 *       consistent for each.</li>
 *   <li>Inline/pointer disjointness (SPEC.md §State Model): a table's metadata
 *       lives in {@code tblInlineMetadata} OR has non-null
 *       {@code metadataLocation}, never both.</li>
 * </ul>
 *
 * <p>Each check returns false (not throws) so jqwik's shrinker can minimise
 * the counterexample.
 *
 * <p>Proxy note — {@code sentinelImpliesPoolEntry}: The inline-ML manifest
 * pool and per-snapshot sentinel list are stored in package-private fields
 * ({@code allManifestPrefixes}, {@code allInlineMetadata}, etc.) that are not
 * accessible from the {@code CatalogFile} public API. Rather than adding new
 * production accessors, we perform the check via {@link ProtoCatalogFile}'s
 * existing public {@code isInlineTable(TableIdentifier)} and
 * {@code inlineMetadata(TableIdentifier)} which confirm that the inline byte
 * blob is non-empty for each inline table — a necessary (though not
 * sufficient) proxy for "the pool entry exists". The full sentinel-pool
 * consistency check would require exposing manifest-pool internals, so it is
 * not checked here.
 *
 * <p>Not checked: referential integrity (every table's namespace and every
 * namespace's parent exist), ids below the next-id counters, and name
 * uniqueness within a parent.
 */
final class WellFormed {

  private WellFormed() {}

  static boolean check(CatalogFile state) {
    return sentinelImpliesPoolEntry(state)
        && namespaceLookupConsistent(state)
        && tableLookupConsistent(state)
        && inlinePointerDisjoint(state);
  }

  /**
   * Proxy check: every inline table has non-null, non-empty metadata bytes.
   *
   * <p>The full I5 invariant ("every inline-ML sentinel in the stored
   * TableMetadata JSON has a matching entry in the per-table manifest pool")
   * requires decoding the opaque inline metadata bytes and cross-referencing
   * the manifest pool, both of which are package-private. The public API
   * only guarantees that {@code inlineMetadata(table)} returns the bytes. We
   * check the necessary proxy: if the table is inline, the bytes must be
   * non-null and non-empty. The deep sentinel check is not implemented.
   */
  private static boolean sentinelImpliesPoolEntry(CatalogFile state) {
    for (TableIdentifier tbl : state.tables()) {
      if (state.isInlineTable(tbl)) {
        byte[] meta = state.inlineMetadata(tbl);
        if (meta == null || meta.length == 0) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Every namespace accessible via {@code namespaces()} is also recognized by
   * {@code containsNamespace()}, and vice-versa for the count.
   */
  private static boolean namespaceLookupConsistent(CatalogFile state) {
    for (Namespace ns : state.namespaces()) {
      if (!state.containsNamespace(ns)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Every table in {@code tables()} is recognized by {@code containsTable()},
   * and no duplicate identifiers exist in the list.
   */
  private static boolean tableLookupConsistent(CatalogFile state) {
    java.util.Set<TableIdentifier> seen = new java.util.HashSet<>();
    for (TableIdentifier tbl : state.tables()) {
      if (!state.containsTable(tbl)) {
        return false;
      }
      if (!seen.add(tbl)) {
        return false; // duplicate
      }
    }
    return true;
  }

  /**
   * Inline/pointer disjointness (SPEC.md §State Model): a table's metadata lives
   * in {@code tblInlineMetadata} OR has a non-null {@code metadataLocation},
   * never both.
   *
   * <p>Implementation: if {@code isInlineTable(tbl)} is true then
   * {@code location(tbl)} must be null (inline tables store null in
   * {@code TblEntry.metadataLocation}). If {@code isInlineTable} is false,
   * location must be non-null.
   *
   * <p>Note: For ProtoCatalogFile, inline tables are stored with
   * {@code metadataLocation = null} in tableById, so location() returns null.
   * Non-inline tables must have a non-null location. This is the disjointness
   * invariant.
   */
  private static boolean inlinePointerDisjoint(CatalogFile state) {
    if (!(state instanceof ProtoCatalogFile)) {
      // Only ProtoCatalogFile enforces this layout; skip for other impls.
      return true;
    }
    for (TableIdentifier tbl : state.tables()) {
      boolean isInline = state.isInlineTable(tbl);
      String loc = state.location(tbl);
      // Inline: location must be null; Pointer: location must be non-null
      if (isInline && loc != null) {
        return false;
      }
      if (!isInline && loc == null) {
        return false;
      }
    }
    return true;
  }
}
