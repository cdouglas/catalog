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
import org.apache.iceberg.io.CatalogState;
import org.apache.iceberg.io.InlineMetadataCodecs;

/**
 * Structural well-formedness check for P4 ({@link TestInvariantProperties}).
 * Encodes invariants that hold on a single state:
 *
 * <ul>
 *   <li>I5: every inline-ML sentinel has a matching pool entry (not checked
 *       here — inline tables use opaque byte[] for metadata, and the manifest
 *       pool / sentinel structure is only observable through package-private
 *       accessors not reachable from {@code CatalogState}; see proxy note
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
 *   <li>Referential integrity: every table's namespace and every namespace's
 *       parent exist.</li>
 *   <li>Names unique within a parent: no two tables of a namespace share a
 *       name, and no two namespaces of a parent ({@link #namesUnique}).</li>
 *   <li>Ids below the next-id counters: every entry the lookups name has an id
 *       below its counter.</li>
 *   <li>Views (docs/VIEWS.md): every view's namespace exists, its metadata is
 *       a file's location or inline JSON, never both or neither, no two views
 *       of a namespace share a name, and no namespace holds a table and a view
 *       of one name, the two kinds sharing one set of names.</li>
 * </ul>
 *
 * <p>Each check returns false (not throws) so jqwik's shrinker can minimise
 * the counterexample.
 *
 * <p>Proxy note — {@code sentinelImpliesPoolEntry}: The inline-ML manifest
 * pool and per-snapshot sentinel list are stored in package-private fields
 * ({@code allManifestPrefixes}, {@code allInlineMetadata}, etc.) that are not
 * accessible from the {@code CatalogState} public API. Rather than adding new
 * production accessors, we perform the check via {@link CatalogState}'s
 * existing public {@code isInlineTable(TableIdentifier)} and
 * {@code inlineMetadata(TableIdentifier)} which confirm that the inline byte
 * blob is non-empty for each inline table — a necessary (though not
 * sufficient) proxy for "the pool entry exists". The full sentinel-pool
 * consistency check would require exposing manifest-pool internals, so it is
 * not checked here.
 *
 * <p>The entries are reached through the public accessors by id ({@code
 * namespaceEntry}, {@code tableEntry}) below the counters: the lookups by name
 * hold one id per name, so two entries of one name would show as one there.
 */
final class WellFormed {

  private WellFormed() {}

  static boolean check(CatalogState state) {
    return violation(state) == null;
  }

  /** The first invariant {@code state} breaks, or null if it is well-formed. */
  static String violation(CatalogState state) {
    if (!sentinelImpliesPoolEntry(state)) {
      return "an inline table without metadata";
    }
    if (!namespaceLookupConsistent(state)) {
      return "the namespace lookup";
    }
    if (!tableLookupConsistent(state)) {
      return "the table lookup";
    }
    if (!inlinePointerDisjoint(state)) {
      return "a table both inline and a pointer";
    }
    return namesUnique(state);
  }

  /**
   * Why the entries below the id counters are not referentially intact with unique names within
   * each parent, or are not all the lookups name; null if they are.
   */
  static String namesUnique(CatalogState state) {
    java.util.Set<String> namespaceNames = new java.util.HashSet<>();
    int namespaces = 0;
    for (int id = 1; id < state.nextNamespaceId(); id++) {
      CatalogState.NsEntry entry = state.namespaceEntry(id);
      if (entry == null) {
        continue;
      }
      namespaces += 1;
      if (state.namespaceEntry(entry.parentId) == null) {
        return "namespace " + id + " (" + entry.name + ") has no parent " + entry.parentId;
      }
      if (!namespaceNames.add(entry.parentId + "/" + entry.name)) {
        return "two namespaces of parent " + entry.parentId + " are named " + entry.name;
      }
    }
    if (namespaces != state.namespaces().size()) {
      return namespaces + " namespaces below the counter, " + state.namespaces().size()
          + " by name";
    }
    java.util.Set<String> tableNames = new java.util.HashSet<>();
    int tables = 0;
    for (int id = 1; id < state.nextTableId(); id++) {
      CatalogState.TblEntry entry = state.tableEntry(id);
      if (entry == null) {
        continue;
      }
      tables += 1;
      if (entry.namespaceId == 0 || state.namespaceEntry(entry.namespaceId) == null) {
        return "table " + id + " (" + entry.name + ") has no namespace " + entry.namespaceId;
      }
      if (!tableNames.add(entry.namespaceId + "/" + entry.name)) {
        return "two tables of namespace " + entry.namespaceId + " are named " + entry.name;
      }
    }
    if (tables != state.tables().size()) {
      return tables + " tables below the counter, " + state.tables().size() + " by name";
    }
    java.util.Set<String> viewNames = new java.util.HashSet<>();
    int views = 0;
    for (int id = 1; id < state.nextViewId(); id++) {
      CatalogState.ViewEntry entry = state.viewEntry(id);
      if (entry == null) {
        continue;
      }
      views += 1;
      if (entry.namespaceId == 0 || state.namespaceEntry(entry.namespaceId) == null) {
        return "view " + id + " (" + entry.name + ") has no namespace " + entry.namespaceId;
      }
      if ((entry.metadataLocation == null) == (entry.inlineMetadata() == null)) {
        return "view " + id + " is " + (entry.isInline() ? "both inline and a pointer" : "neither");
      }
      if (entry.isInline() && entry.codec != InlineMetadataCodecs.TAG_JSON_GZIP) {
        return "view " + id + " has inline codec " + entry.codec;
      }
      if (!viewNames.add(entry.namespaceId + "/" + entry.name)) {
        return "two views of namespace " + entry.namespaceId + " are named " + entry.name;
      }
      if (tableNames.contains(entry.namespaceId + "/" + entry.name)) {
        return "namespace " + entry.namespaceId + " holds a table and a view named " + entry.name;
      }
    }
    if (views != state.views().size()) {
      return views + " views below the counter, " + state.views().size() + " by name";
    }
    return null;
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
  private static boolean sentinelImpliesPoolEntry(CatalogState state) {
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
  private static boolean namespaceLookupConsistent(CatalogState state) {
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
  private static boolean tableLookupConsistent(CatalogState state) {
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
   * <p>Note: For CatalogState, inline tables are stored with
   * {@code metadataLocation = null} in tableById, so location() returns null.
   * Non-inline tables must have a non-null location. This is the disjointness
   * invariant.
   */
  private static boolean inlinePointerDisjoint(CatalogState state) {
    if (!(state instanceof CatalogState)) {
      // Only CatalogState enforces this layout; skip for other impls.
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
