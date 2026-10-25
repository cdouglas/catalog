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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.intentlog.Replica;

/**
 * A transaction on the catalog, staged on a read of its log ({@link #base}): namespaces and
 * tables created, dropped, updated or renamed, namespace properties set, and the tables it read.
 * Each change is checked against the base as it is staged. Its {@link #intention} names the
 * version of each namespace and table it depends on, and a commit ({@link CatalogLog#commit})
 * takes effect only where those versions still hold.
 */
public class CatalogMutation {

  private final Replica<CatalogState> base;
  private final CatalogState original;
  private final Set<TableIdentifier> readTables;
  private final Map<TableIdentifier, String> tables;
  private final Map<TableIdentifier, String> tableUpdates;
  private final Map<Namespace, Boolean> namespaces;
  private final Map<Namespace, Map<String, String>> namespaceProperties;
  // Inline table mutations (metadata stored in catalog, not external file)
  private final Map<TableIdentifier, byte[]> inlineTables;
  private final Map<TableIdentifier, byte[]> inlineTableUpdates;
  private final Map<TableIdentifier, byte[]> inlineTableDeltaUpdates;
  // Initial manifest-pool delta for inline-ML create-with-snapshots. Parallel
  // to {@link #inlineTables}: an entry here carries the AddManifestUpdate
  // payload that {@link ProtoCodec.CreateTableInlineAction#apply} applies
  // immediately after the table create. {@code null} / absent for non-ML
  // creates and for inline-ML creates with no snapshots.
  private final Map<TableIdentifier, byte[]> inlineTableCreateDeltas;
  // Companion to {@link #inlineTableUpdates}: optional ML-pool delta carried
  // alongside a FULL-mode update. Set when delta-mode TM encoding wasn't
  // possible (e.g. concurrent-replace id collision) but the commit still has
  // staged manifest-list updates that must land atomically with the TM
  // replacement.
  private final Map<TableIdentifier, byte[]> inlineTableUpdateMlDeltas;
  // Codec tags parallel to inlineTables / inlineTableUpdates.
  private final Map<TableIdentifier, Byte> inlineTableCodecs;
  private final Map<TableIdentifier, Byte> inlineTableUpdateCodecs;
  // Pending renames: from-identifier -> to-identifier. The id stays put; only
  // the (namespace, name) pair on TblEntry changes. See SPEC_TM.md "Rename".
  private final Map<TableIdentifier, TableIdentifier> tableRenames;
  // Tables whose inline update applies only on the version read, whatever the catalog's conflict
  // encoding (ProtoCodec.UpdateTableInlineAction.pinned).
  private final Set<TableIdentifier> pinned;

  /** A transaction staged on {@code base}, a read of the catalog's log. */
  public CatalogMutation(Replica<CatalogState> base) {
    this.base = base;
    this.original = base.state();
    this.tables = Maps.newHashMap();
    this.readTables = Sets.newHashSet();
    this.tableUpdates = Maps.newHashMap();
    this.namespaces = Maps.newHashMap();
    this.namespaceProperties = Maps.newHashMap();
    this.inlineTables = Maps.newHashMap();
    this.inlineTableUpdates = Maps.newHashMap();
    this.inlineTableDeltaUpdates = Maps.newHashMap();
    this.inlineTableCreateDeltas = Maps.newHashMap();
    this.inlineTableUpdateMlDeltas = Maps.newHashMap();
    this.inlineTableCodecs = Maps.newHashMap();
    this.inlineTableUpdateCodecs = Maps.newHashMap();
    this.tableRenames = Maps.newHashMap();
    this.pinned = Sets.newHashSet();
  }

  /** The read of the log this transaction is staged on, and commits on top of. */
  public Replica<CatalogState> base() {
    return base;
  }

  public CatalogMutation createNamespace(Namespace namespace) {
    return createNamespace(namespace, Collections.emptyMap());
  }

  public CatalogMutation createNamespace(Namespace namespace, Map<String, String> properties) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    Preconditions.checkNotNull(properties, "Properties cannot be null");
    Preconditions.checkArgument(
        !namespace.equals(Namespace.empty()), "Cannot create empty namespace");
    if (original.containsNamespace(namespace)
        || (namespaces.containsKey(namespace) && !namespaces.get(namespace))) {
      throw new AlreadyExistsException(
          "Cannot create namespace %s. Namespace already exists", namespace);
    }
    // Missing ancestors are created with the namespace. An ancestor that this
    // transaction drops cannot gain a child, whether or not the snapshot has it.
    List<Namespace> missing = Lists.newArrayList();
    for (Namespace ancestor = parentOf(namespace);
        !ancestor.isEmpty();
        ancestor = parentOf(ancestor)) {
      Boolean pending = namespaces.get(ancestor);
      if (Boolean.FALSE.equals(pending)) {
        throw new IllegalStateException(
            String.format(
                "Cannot create namespace %s. Parent namespace %s is marked for deletion",
                namespace, ancestor));
      }
      if (pending != null || original.containsNamespace(ancestor)) {
        break;
      }
      missing.add(ancestor);
    }
    missing.forEach(ancestor -> namespaces.put(ancestor, true));
    namespaces.put(namespace, true);
    namespaceProperties.put(namespace, properties);
    return this;
  }

  public CatalogMutation updateProperties(Namespace namespace, Map<String, String> properties) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    Preconditions.checkNotNull(properties, "Properties cannot be null");
    if (checkNamespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    namespaceProperties.compute(
        namespace,
        (ignored, old) -> {
          if (old != null) {
            old.putAll(properties);
            return old;
          }
          return Maps.newHashMap(properties);
        });
    return this;
  }

  static String nameOf(Namespace ns) {
    final int levels = ns.length();
    return levels > 0 ? ns.levels()[levels - 1] : Namespace.empty().toString();
  }

  static Namespace parentOf(Namespace ns) {
    final int levels = ns.length();
    return levels > 1
        ? Namespace.of(Arrays.copyOfRange(ns.levels(), 0, levels - 1))
        : Namespace.empty();
  }

  public CatalogMutation dropNamespace(Namespace namespace) {
    Preconditions.checkArgument(
        !Namespace.empty().equals(namespace), "Cannot drop empty namespace");
    if (checkNamespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    // As for tables, only a namespace in the snapshot can be dropped. Dropping
    // one this transaction creates would cancel the create and, with it, the
    // create's precondition that the namespace was absent: the empty record
    // would commit even after a concurrent create of the same namespace.
    Preconditions.checkArgument(
        original.containsNamespace(namespace),
        "Cannot drop namespace %s created in this transaction",
        namespace);
    final boolean noNsChild =
        original.namespaces().stream()
                .filter(
                    ns ->
                        namespaces.getOrDefault(
                            ns, true)) // filter out children marked for deletion
                .map(CatalogMutation::parentOf)
                .noneMatch(parent -> parent.equals(namespace))
            && namespaces.entrySet().stream()
                .filter(Map.Entry::getValue) // only new namespaces
                .noneMatch(e -> parentOf(e.getKey()).equals(namespace));
    final boolean noTblChild =
        original.tables().stream()
                .filter(tbl -> tables.get(tbl) == null)
                .map(TableIdentifier::namespace)
                .noneMatch(ns -> ns.equals(namespace))
            && tables.entrySet().stream()
                .filter(e -> e.getValue() != null) // only table creations
                .map(e -> e.getKey().namespace())
                .noneMatch(ns -> ns.equals(namespace))
            && tableRenames.values().stream()
                .map(TableIdentifier::namespace)
                .noneMatch(ns -> ns.equals(namespace));
    if (!noNsChild || !noTblChild) {
      throw new NamespaceNotEmptyException("Namespace %s is not empty", namespace);
    }
    namespaces.put(namespace, false);
    namespaceProperties.remove(namespace);
    return this;
  }

  public CatalogMutation createTable(TableIdentifier table, String location) {
    // TODO: fix for swap (a -> b; b -> a)
    if (checkNamespaceExists(table.namespace())) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", table.namespace());
    }
    if (original.containsTable(table) || tables.get(table) != null
        || inlineTables.get(table) != null || tableRenames.containsValue(table)) {
      throw new AlreadyExistsException("Table already exists: %s", table);
    }
    tables.put(table, location);
    return this;
  }

  public CatalogMutation readTable(TableIdentifier table) {
    if (markedForDrop(table)) {
      // TODO eh... this should be legal.
      throw new IllegalArgumentException(
          "Cannot include read dependency on table marked for deletion: " + table);
    }
    readTables.add(table);
    return this;
  }

  public CatalogMutation updateTable(TableIdentifier table, String location) {
    if (!original.containsTable(table)) {
      throw new NoSuchNamespaceException("Table does not exist: %s", table);
    }
    checkNotRenamed(table);
    if (markedForDrop(table)) {
      throw new IllegalArgumentException("Cannot update table marked for deletion: " + table);
    }
    final String newloc = tables.get(table);
    if (newloc != null) {
      tables.put(table, location);
    } else {
      tableUpdates.put(table, location);
    }
    return this;
  }

  /**
   * Renames a table in place: the table id stays put, only the
   * {@code (namespace, name)} pair on {@code TblEntry} changes. Inline state
   * keyed by id (manifest pool, snapshot manifest refs, inline TM bytes)
   * follows by construction. See SPEC_TM.md "Rename".
   */
  public CatalogMutation renameTable(TableIdentifier from, TableIdentifier to) {
    Preconditions.checkNotNull(from, "Source table cannot be null");
    Preconditions.checkNotNull(to, "Destination table cannot be null");
    Preconditions.checkArgument(!from.equals(to), "Source and destination identifiers are equal");
    // Source must exist and not already be marked for delete in this transaction.
    if (!original.containsTable(from) || (tables.containsKey(from) && tables.get(from) == null)) {
      throw new NoSuchTableException("Table does not exist: %s", from);
    }
    checkNotRenamed(from);
    // Destination namespace must exist (in original or pending creates).
    if (checkNamespaceExists(to.namespace())) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", to.namespace());
    }
    // Destination must not already exist (in original or pending creates).
    boolean destPendingCreate =
        (tables.containsKey(to) && tables.get(to) != null)
            || inlineTables.containsKey(to)
            || tableRenames.containsValue(to);
    if (original.containsTable(to) || destPendingCreate) {
      throw new AlreadyExistsException("Table already exists: %s", to);
    }
    tableRenames.put(from, to);
    return this;
  }

  public CatalogMutation dropTable(TableIdentifier tableId) {
    if (!original.containsTable(tableId)) {
      throw new NoSuchTableException("Table does not exist: %s", tableId);
    }
    checkNotRenamed(tableId);
    // The drop supersedes what this transaction read or changed of the table, as dropNamespace
    // does a namespace's property changes: its precondition is the same table version, and
    // replay rejects a record that drops a table it also reads or updates.
    readTables.remove(tableId);
    tableUpdates.remove(tableId);
    inlineTableUpdates.remove(tableId);
    inlineTableUpdateCodecs.remove(tableId);
    inlineTableUpdateMlDeltas.remove(tableId);
    inlineTableDeltaUpdates.remove(tableId);
    tables.put(tableId, null);
    return this;
  }

  /**
   * Creates a table with metadata stored inline; {@code codecTag} identifies
   * the codec used to encode {@code metadata}.
   */
  public CatalogMutation createTableInline(TableIdentifier table, byte[] metadata, byte codecTag) {
    if (checkNamespaceExists(table.namespace())) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", table.namespace());
    }
    if (original.containsTable(table) || tables.get(table) != null
        || inlineTables.get(table) != null || tableRenames.containsValue(table)) {
      throw new AlreadyExistsException("Table already exists: %s", table);
    }
    inlineTables.put(table, metadata);
    inlineTableCodecs.put(table, codecTag);
    return this;
  }

  /**
   * Creates an inline-ML table whose initial commit contains one or more
   * staged snapshots. {@code deltaBytes} carries the AddManifestUpdate entries
   * that populate the per-table manifest pool atomically with the table
   * create — required so the on-disk encoded TableMetadata's
   * {@code inline://&lt;snapId&gt;} sentinels resolve on reload.
   */
  public CatalogMutation createTableInlineWithManifests(
      TableIdentifier table, byte[] metadata, byte codecTag, byte[] deltaBytes) {
    createTableInline(table, metadata, codecTag);
    inlineTableCreateDeltas.put(table, deltaBytes);
    return this;
  }

  /**
   * Updates an inline table's metadata (full replacement) with the given codec.
   */
  public CatalogMutation updateTableInline(TableIdentifier table, byte[] metadata, byte codecTag) {
    if (!original.containsTable(table)) {
      throw new NoSuchTableException("Table does not exist: %s", table);
    }
    checkNotRenamed(table);
    if (markedForDrop(table)) {
      throw new IllegalArgumentException("Cannot update table marked for deletion: " + table);
    }
    inlineTableUpdates.put(table, metadata);
    inlineTableUpdateCodecs.put(table, codecTag);
    return this;
  }

  /**
   * Full-mode inline update bundled with an ML pool delta. Used when delta-
   * mode TM encoding can't represent the diff (e.g. concurrent-replace id
   * collision) but the commit still has staged manifest-list updates that
   * must land in the per-table pool atomically with the TM replacement.
   */
  public CatalogMutation updateTableInlineWithManifests(
      TableIdentifier table, byte[] metadata, byte codecTag, byte[] mlDeltaBytes) {
    updateTableInline(table, metadata, codecTag);
    inlineTableUpdateMlDeltas.put(table, mlDeltaBytes);
    return this;
  }

  /** Updates an inline table via structured delta (encoded delta bytes). */
  public CatalogMutation updateTableInlineDelta(TableIdentifier table, byte[] deltaBytes) {
    if (!original.containsTable(table)) {
      throw new NoSuchTableException("Table does not exist: %s", table);
    }
    checkNotRenamed(table);
    if (markedForDrop(table)) {
      throw new IllegalArgumentException("Cannot update table marked for deletion: " + table);
    }
    // Store with a marker prefix to distinguish from full mode in buildActions
    inlineTableDeltaUpdates.put(table, deltaBytes);
    return this;
  }

  /**
   * Makes the update of {@code table} apply only on the version this transaction read, whatever
   * the catalog's conflict encoding: a catalog transaction may have read anything in a table it
   * writes, which must be at the version it first read (errata D12). An update of a pointer table
   * applies only there anyway.
   */
  public CatalogMutation pin(TableIdentifier table) {
    pinned.add(table);
    return this;
  }

  // True when the namespace does NOT exist once this transaction's pending
  // creates and drops apply. The empty namespace never exists: the format
  // cannot hold tables or properties on the root.
  private boolean checkNamespaceExists(Namespace namespace) {
    Boolean pending = namespaces.get(namespace);
    return pending != null ? !pending : !original.containsNamespace(namespace);
  }

  // A table this transaction drops: replay rejects a record that drops a table it also reads or
  // updates (Bug 14), whether the table is inline or not.
  private boolean markedForDrop(TableIdentifier table) {
    return tables.containsKey(table) && tables.get(table) == null;
  }

  // A table renamed earlier in this transaction no longer exists under its old name.
  private void checkNotRenamed(TableIdentifier table) {
    if (tableRenames.containsKey(table)) {
      throw new NoSuchTableException(
          "Table does not exist: %s (renamed to %s in this transaction)",
          table, tableRenames.get(table));
    }
  }

  /**
   * The intention of this transaction: its actions, each naming the version of what it depends
   * on in {@link #base}, which replay checks where the record lands.
   */
  public ProtoCodec.CatalogIntention intention() {
    List<ProtoCodec.Action> actions = new ArrayList<>();
    // New namespaces and tables get late-bound (negative) ids, which replay
    // binds to real ids in log order (ProtoCodec.IdBinding). Allocating real
    // ids here, from this writer's snapshot, let two concurrent writers claim
    // the same id for unrelated creates.
    int nextLateNsid = -1;
    int nextLateTblid = -1;

    // Map namespace created in this transaction to its late-bound ID
    Map<Namespace, Integer> nsIdMap = new HashMap<>();

    // Sort namespaces by depth (parents before children) to ensure correct ID assignment
    List<Namespace> sortedNamespaces = namespaces.entrySet().stream()
        .filter(Map.Entry::getValue)  // only creates
        .map(Map.Entry::getKey)
        .sorted(Comparator.comparingInt(Namespace::length))
        .collect(Collectors.toList());

    // Process namespace creates in topological order
    for (Namespace ns : sortedNamespaces) {
      // Create namespace
      Namespace parent = parentOf(ns);
      String name = nameOf(ns);

      int parentId = 0;
      int parentVersion = original.namespaceVersion(0);
      if (!parent.isEmpty()) {
        Integer existingParentId = original.namespaceId(parent);
        if (existingParentId != null) {
          parentId = existingParentId;
          parentVersion = original.namespaceVersion(existingParentId);
        } else {
          // Parent created in same transaction
          Integer lateBoundParentId = nsIdMap.get(parent);
          if (lateBoundParentId != null) {
            parentId = lateBoundParentId;
            parentVersion = -1; // late-bound
          }
        }
      }

      int nsId = nextLateNsid--;
      nsIdMap.put(ns, nsId);
      actions.add(new ProtoCodec.CreateNamespaceAction(nsId, 1, parentId, parentVersion, name));

      // Add properties for this namespace. Null values are remove markers
      // from a subsequent updateProperties call; skip them since there is
      // nothing to remove on a freshly-created namespace.
      Map<String, String> props = namespaceProperties.get(ns);
      if (props != null) {
        for (Map.Entry<String, String> prop : props.entrySet()) {
          if (prop.getValue() == null) {
            continue;
          }
          actions.add(new ProtoCodec.SetNamespacePropertyAction(
              nsId, -1, prop.getKey(), prop.getValue()));
        }
      }
    }

    // Process namespace drops (false = drop)
    for (Map.Entry<Namespace, Boolean> entry : namespaces.entrySet()) {
      if (!entry.getValue()) {
        Namespace ns = entry.getKey();
        Integer nsId = original.namespaceId(ns);
        if (nsId != null) {
          int version = original.namespaceVersion(nsId);
          actions.add(new ProtoCodec.DropNamespaceAction(nsId, version));
        }
      }
    }

    // Process namespace property updates (for existing namespaces).
    // updateProperties merges into the same map for both
    // sets and removes; a null value is the remove marker.
    for (Map.Entry<Namespace, Map<String, String>> entry : namespaceProperties.entrySet()) {
      Namespace ns = entry.getKey();
      if (!namespaces.containsKey(ns)) {
        // Properties for existing namespace
        Integer nsId = original.namespaceId(ns);
        if (nsId != null) {
          int version = original.namespaceVersion(nsId);
          for (Map.Entry<String, String> prop : entry.getValue().entrySet()) {
            if (prop.getValue() == null) {
              actions.add(new ProtoCodec.RemoveNamespacePropertyAction(
                  nsId, version, prop.getKey()));
            } else {
              actions.add(new ProtoCodec.SetNamespacePropertyAction(
                  nsId, version, prop.getKey(), prop.getValue()));
            }
          }
        }
      }
    }

    // Process table creates and drops. This transaction uses the `tables` map
    // for both: a non-null value is a create-with-location, a null value is
    // a drop marker. Inline-table creates use a separate inlineTables map.
    for (Map.Entry<TableIdentifier, String> entry : tables.entrySet()) {
      TableIdentifier ident = entry.getKey();
      String metadataLocation = entry.getValue();

      if (metadataLocation == null) {
        Integer tblId = original.tableId(ident);
        if (tblId != null) {
          int version = original.tableVersion(tblId);
          actions.add(new ProtoCodec.DropTableAction(tblId, version));
        }
        continue;
      }

      Namespace ns = ident.namespace();
      Integer nsId = original.namespaceId(ns);
      int nsVersion = -1;
      if (nsId == null) {
        // Namespace created in same transaction
        nsId = nsIdMap.get(ns);
      } else {
        nsVersion = original.namespaceVersion(nsId);
      }

      int tblId = nextLateTblid--;
      actions.add(new ProtoCodec.CreateTableAction(
          tblId, 1, nsId, nsVersion, ident.name(), metadataLocation));
    }

    // Process table updates
    for (Map.Entry<TableIdentifier, String> entry : tableUpdates.entrySet()) {
      TableIdentifier ident = entry.getKey();
      String metadataLocation = entry.getValue();
      Integer tblId = original.tableId(ident);
      if (tblId != null) {
        int version = original.tableVersion(tblId);
        actions.add(new ProtoCodec.UpdateTableLocationAction(tblId, version, metadataLocation));
      }
    }

    // Process read tables (for optimistic concurrency)
    for (TableIdentifier ident : readTables) {
      Integer tblId = original.tableId(ident);
      if (tblId != null) {
        int version = original.tableVersion(tblId);
        actions.add(new ProtoCodec.ReadTableAction(tblId, version));
      }
    }

    // Process inline table creates
    for (Map.Entry<TableIdentifier, byte[]> entry : inlineTables.entrySet()) {
      TableIdentifier ident = entry.getKey();
      byte[] metadata = entry.getValue();
      Byte codecTag = inlineTableCodecs.get(ident);
      byte codec = codecTag != null ? codecTag : InlineMetadataCodecs.TAG_JSON_GZIP;
      Namespace ns = ident.namespace();

      Integer nsId = original.namespaceId(ns);
      int nsVersion = -1;
      if (nsId == null) {
        nsId = nsIdMap.get(ns);
      } else {
        nsVersion = original.namespaceVersion(nsId);
      }

      int tblId = nextLateTblid--;
      byte[] createDelta = inlineTableCreateDeltas.get(ident);
      actions.add(new ProtoCodec.CreateTableInlineAction(
          tblId, 1, nsId, nsVersion, ident.name(), metadata, codec, createDelta));
    }

    // Process inline table updates (full mode, optionally with bundled ML delta)
    for (Map.Entry<TableIdentifier, byte[]> entry : inlineTableUpdates.entrySet()) {
      TableIdentifier ident = entry.getKey();
      byte[] metadata = entry.getValue();
      Byte codecTag = inlineTableUpdateCodecs.get(ident);
      byte codec = codecTag != null ? codecTag : InlineMetadataCodecs.TAG_JSON_GZIP;
      byte[] mlDelta = inlineTableUpdateMlDeltas.get(ident);
      Integer tblId = original.tableId(ident);
      if (tblId != null) {
        int version = original.tableVersion(tblId);
        ProtoCodec.UpdateTableInlineAction update = mlDelta != null
            ? ProtoCodec.UpdateTableInlineAction.full(tblId, version, metadata, codec, mlDelta)
            : ProtoCodec.UpdateTableInlineAction.full(tblId, version, metadata, codec);
        actions.add(pinned.contains(ident) ? update.pin() : update);
      }
    }

    // Process inline table delta updates
    for (Map.Entry<TableIdentifier, byte[]> entry : inlineTableDeltaUpdates.entrySet()) {
      TableIdentifier ident = entry.getKey();
      byte[] deltaBytes = entry.getValue();
      Integer tblId = original.tableId(ident);
      if (tblId != null) {
        int version = original.tableVersion(tblId);
        ProtoCodec.UpdateTableInlineAction update =
            ProtoCodec.UpdateTableInlineAction.delta(tblId, version, deltaBytes);
        actions.add(pinned.contains(ident) ? update.pin() : update);
      }
    }

    // Process table renames (in-place, id-stable). See SPEC_TM.md "Rename".
    for (Map.Entry<TableIdentifier, TableIdentifier> entry : tableRenames.entrySet()) {
      TableIdentifier from = entry.getKey();
      TableIdentifier to = entry.getValue();
      Integer tblId = original.tableId(from);
      if (tblId == null) {
        continue;
      }
      int version = original.tableVersion(tblId);
      Namespace newNs = to.namespace();
      Integer newNsId = original.namespaceId(newNs);
      int newNsVersion;
      if (newNsId == null) {
        // Destination namespace was created in this same transaction.
        newNsId = nsIdMap.get(newNs);
        newNsVersion = -1;
      } else {
        newNsVersion = original.namespaceVersion(newNsId);
      }
      actions.add(new ProtoCodec.RenameTableAction(
          tblId, version, newNsId, newNsVersion, to.name()));
    }
    return new ProtoCodec.CatalogIntention(actions);
  }

  /**
   * The most informative error for this transaction, if it does not hold on {@code state}, the
   * state it lost to: AlreadyExistsException if a pending create or rename destination now exists
   * (Iceberg's create-race semantic), NoSuchTableException if a rename's source is gone,
   * CommitFailedException otherwise, so the caller can refresh and re-apply. A rename is judged
   * as renameTable checks it, source first: one that lost a race to the same rename reports the
   * source gone, as it would have after it.
   */
  RuntimeException explain(CatalogState state, String message) {
    for (Map.Entry<TableIdentifier, String> e : tables.entrySet()) {
      if (e.getValue() != null && state.containsTable(e.getKey())) {
        return new AlreadyExistsException("Table already exists: %s", e.getKey());
      }
    }
    for (Map.Entry<TableIdentifier, byte[]> e : inlineTables.entrySet()) {
      if (state.containsTable(e.getKey())) {
        return new AlreadyExistsException("Table already exists: %s", e.getKey());
      }
    }
    for (Map.Entry<TableIdentifier, TableIdentifier> rename : tableRenames.entrySet()) {
      if (!state.containsTable(rename.getKey())) {
        return new NoSuchTableException("Table does not exist: %s", rename.getKey());
      }
      if (state.containsTable(rename.getValue())) {
        return new AlreadyExistsException("Table already exists: %s", rename.getValue());
      }
    }
    return new CommitFailedException("%s", message);
  }

  /** Commits this transaction to {@code log}: {@link CatalogLog#commit}. */
  public Replica<CatalogState> commit(CatalogLog log) {
    return log.commit(this);
  }
}
