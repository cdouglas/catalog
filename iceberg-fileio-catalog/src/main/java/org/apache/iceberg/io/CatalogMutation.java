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
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.intentlog.Replica;

/**
 * A transaction on the catalog, staged on a read of its log ({@link #base}): namespaces, tables
 * and views created, dropped, updated or renamed, namespace properties set, and the tables it read.
 * Each change is checked against the base as it is staged. Its {@link #intention} names the
 * version of each namespace and table it depends on, and what each name it binds (a create, a
 * rename's new name) requires of its namespace ({@link CreateCondition}); a commit ({@link
 * CatalogLog#commit}) takes effect only where those still hold.
 */
public class CatalogMutation {

  /**
   * What an action that binds a name (a table or namespace created, a table's new name) requires
   * of the namespace it binds it in, where its record lands ({@code
   * fileio.catalog.create.condition}).
   */
  public enum CreateCondition {
    /**
     * The namespace exists, has freed no name since the read (its removals), and the name is
     * free: creates of different names in one namespace do not conflict.
     */
    NAME("name"),
    /**
     * The namespace is at the version read: any change there since (a sibling created, dropped
     * or renamed, a property set) fails the record, so creates in one namespace land one at a
     * time.
     */
    NAMESPACE("namespace");

    private final String value;

    CreateCondition(String value) {
      this.value = value;
    }

    /** The condition's name as the catalog property gives it. */
    public String value() {
      return value;
    }

    /** The condition named {@code value}. */
    public static CreateCondition of(String value) {
      for (CreateCondition condition : values()) {
        if (condition.value.equals(value)) {
          return condition;
        }
      }
      throw new IllegalArgumentException("Unknown create condition: " + value);
    }
  }

  private final Replica<CatalogState> base;
  private final CreateCondition createCondition;
  private final CatalogState original;
  // Tables a serializable transaction read: the refs it read (a read witness), or none, a read of
  // the whole version (a read pin)
  private final Map<TableIdentifier, java.util.SortedSet<String>> readTables;
  // Tables created, each to its metadata file's location
  private final Map<TableIdentifier, String> tables;
  // Tables of the read dropped, by their names there: a name a drop frees may be taken by a create
  // of this transaction (errata D29 (txn takes no freed name), fixed), so drops and creates are
  // kept apart
  private final Set<TableIdentifier> tableDrops;
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
  // Companion to {@link #inlineTableUpdates} for an inline table: the snapshots of the version
  // read that the update drops, its metadata holding only those that version lacks
  private final Map<TableIdentifier, long[]> inlineTableUpdateRemovals;
  // The sealed snapshots an inline update brings back, with their proofs (SNAPSHOT_PAGING.md)
  private final Map<TableIdentifier, List<SnapshotPages.BroughtBack>> inlineTableBroughtBack;
  // A re-inlined pointer table's imported blocks, whose pages its writer put
  private final Map<TableIdentifier, List<SnapshotPages.SealedBlock>> inlineTableImports;
  // Codec tags parallel to inlineTables / inlineTableUpdates.
  private final Map<TableIdentifier, Byte> inlineTableCodecs;
  private final Map<TableIdentifier, Byte> inlineTableUpdateCodecs;
  // Pending renames: from-identifier -> to-identifier. The id stays put; only
  // the (namespace, name) pair on TblEntry changes. See SPEC_TM.md "Rename".
  private final Map<TableIdentifier, TableIdentifier> tableRenames;
  // Tables whose inline update applies only on the version read, whatever the catalog's conflict
  // encoding (ProtoCodec.UpdateTableInlineAction.pinned).
  private final Set<TableIdentifier> pinned;
  // The version a delta was built on, where not this mutation's read's (a catalog transaction's)
  private final Map<TableIdentifier, Integer> deltaBases;
  // How each inline update or create staged here goes to a metadata file instead (evict)
  private final Map<TableIdentifier, Eviction> evictions;
  // The files the evictions so far wrote, each with the eviction that deletes them
  private final List<Map.Entry<Eviction, List<String>>> evicted;
  // Views created, updated (each with its whole metadata), dropped and renamed (docs/VIEWS.md).
  // A view's identifier is never a table's, so these never share a key with the tables' maps.
  private final Map<TableIdentifier, CatalogState.ViewMetadataRef> viewCreates;
  private final Map<TableIdentifier, CatalogState.ViewMetadataRef> viewUpdates;
  private final Set<TableIdentifier> viewDrops;
  private final Map<TableIdentifier, TableIdentifier> viewRenames;
  // How each inline view create or update staged here goes to a metadata file instead (evict)
  private final Map<TableIdentifier, Eviction> viewEvictions;
  // The files written ahead of this transaction's record for the changes staged on it (a metadata
  // file and its lists, a dropped table's version), which nothing names unless the record lands;
  // written by the worker pool's threads too
  private final java.util.Queue<String> writtenAhead =
      new java.util.concurrent.ConcurrentLinkedQueue<>();
  // Whether a commit of this transaction ended in a way that leaves its record possibly landed
  private volatile boolean mayHaveLanded = false;

  /**
   * A transaction staged on {@code base}, a read of the catalog's log, whose names bind by {@link
   * CreateCondition#NAME}, the catalog's default.
   */
  public CatalogMutation(Replica<CatalogState> base) {
    this(base, CreateCondition.NAME);
  }

  /** A transaction staged on {@code base}, whose names bind by {@code createCondition}. */
  public CatalogMutation(Replica<CatalogState> base, CreateCondition createCondition) {
    this.base = base;
    this.createCondition = createCondition;
    this.original = base.state();
    this.tables = Maps.newHashMap();
    this.tableDrops = Sets.newHashSet();
    this.readTables = Maps.newHashMap();
    this.tableUpdates = Maps.newHashMap();
    this.namespaces = Maps.newHashMap();
    this.namespaceProperties = Maps.newHashMap();
    this.inlineTables = Maps.newHashMap();
    this.inlineTableUpdates = Maps.newHashMap();
    this.inlineTableDeltaUpdates = Maps.newHashMap();
    this.inlineTableCreateDeltas = Maps.newHashMap();
    this.inlineTableUpdateMlDeltas = Maps.newHashMap();
    this.inlineTableUpdateRemovals = Maps.newHashMap();
    this.inlineTableBroughtBack = Maps.newHashMap();
    this.inlineTableImports = Maps.newHashMap();
    this.inlineTableCodecs = Maps.newHashMap();
    this.inlineTableUpdateCodecs = Maps.newHashMap();
    this.tableRenames = Maps.newLinkedHashMap();
    this.pinned = Sets.newHashSet();
    this.deltaBases = Maps.newHashMap();
    this.evictions = Maps.newHashMap();
    this.evicted = new ArrayList<>();
    this.viewCreates = Maps.newHashMap();
    this.viewUpdates = Maps.newHashMap();
    this.viewDrops = Sets.newHashSet();
    this.viewRenames = Maps.newHashMap();
    this.viewEvictions = Maps.newHashMap();
  }

  /** The read of the log this transaction is staged on, and commits on top of. */
  public Replica<CatalogState> base() {
    return base;
  }

  /** What the names this transaction binds require of their namespaces. */
  public CreateCondition createCondition() {
    return createCondition;
  }

  // What a name bound in existing namespace nsId requires of it, as this transaction read it
  private int bindingRead(int nsId) {
    return createCondition == CreateCondition.NAME
        ? original.namespaceRemovals(nsId)
        : original.namespaceVersion(nsId);
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
    // a map of the transaction's own, which a later change of the namespace's properties merges
    // into: the caller's may be immutable, or change under it
    namespaceProperties.put(namespace, Maps.newHashMap(properties));
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
                // a table this transaction drops, or renames, is no child by its old name (a
                // rename's new name is, below)
                .filter(tbl -> !markedForDrop(tbl) && !tableRenames.containsKey(tbl))
                .map(TableIdentifier::namespace)
                .noneMatch(ns -> ns.equals(namespace))
            && tables.keySet().stream()
                .map(TableIdentifier::namespace)
                .noneMatch(ns -> ns.equals(namespace))
            // inline tables this transaction creates (Bug 74: these were missed, so the drop
            // staged, and replay rejected the commit as a conflict)
            && inlineTables.keySet().stream()
                .map(TableIdentifier::namespace)
                .noneMatch(ns -> ns.equals(namespace))
            && tableRenames.values().stream()
                .map(TableIdentifier::namespace)
                .noneMatch(ns -> ns.equals(namespace));
    // a view is a child too (docs/VIEWS.md V8), unless this transaction drops or renames it out
    final boolean noViewChild =
        original.views().stream()
                .filter(view -> !viewDrops.contains(view) && !viewRenames.containsKey(view))
                .map(TableIdentifier::namespace)
                .noneMatch(ns -> ns.equals(namespace))
            && viewCreates.keySet().stream()
                .map(TableIdentifier::namespace)
                .noneMatch(ns -> ns.equals(namespace))
            && viewRenames.values().stream()
                .map(TableIdentifier::namespace)
                .noneMatch(ns -> ns.equals(namespace));
    if (!noNsChild || !noTblChild || !noViewChild) {
      throw new NamespaceNotEmptyException("Namespace %s is not empty", namespace);
    }
    namespaces.put(namespace, false);
    namespaceProperties.remove(namespace);
    return this;
  }

  public CatalogMutation createTable(TableIdentifier table, String location) {
    if (checkNamespaceExists(table.namespace())) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", table.namespace());
    }
    // a name this transaction frees, of a table or a view, is free (errata D29 (txn takes no freed
    // name), fixed)
    if (holds(table)) {
      throw new AlreadyExistsException("Table already exists: %s", table);
    }
    checkNoView(table);
    tables.put(table, location);
    return this;
  }

  /**
   * Makes the commit apply only where {@code table} is at the version this mutation read: a read
   * pin, of a serializable transaction that read the whole table.
   */
  public CatalogMutation readTable(TableIdentifier table) {
    checkReadable(table);
    readTables.put(table, new java.util.TreeSet<>());
    return this;
  }

  /**
   * Makes the commit apply only where {@code ref} of {@code table} names the snapshot it does in
   * this mutation's read, as a serializable transaction read it: a read witness (errata D12),
   * which the catalog's conflict encoding judges by what changed of the refs read since that
   * version (under {@code lamport}, any change). A read pin of the table ({@link #readTable})
   * holds it to its version instead.
   */
  public CatalogMutation readRef(TableIdentifier table, String ref) {
    checkReadable(table);
    java.util.SortedSet<String> refs = readTables.get(table);
    if (refs == null) {
      readTables.put(table, new java.util.TreeSet<>(Set.of(ref)));
    } else if (!refs.isEmpty()) {
      refs.add(ref);
    }
    return this;
  }

  private void checkReadable(TableIdentifier table) {
    if (markedForDrop(table)) {
      // TODO eh... this should be legal.
      throw new IllegalArgumentException(
          "Cannot include read dependency on table marked for deletion: " + table);
    }
  }

  public CatalogMutation updateTable(TableIdentifier table, String location) {
    if (tables.containsKey(table)) {
      tables.put(table, location); // a table this transaction creates
      return this;
    }
    if (!original.containsTable(table)) {
      throw new NoSuchTableException("Table does not exist: %s", table);
    }
    checkNotRenamed(table);
    if (markedForDrop(table)) {
      throw new IllegalArgumentException("Cannot update table marked for deletion: " + table);
    }
    tableUpdates.put(table, location);
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
    if (!original.containsTable(from) || tableDrops.contains(from)) {
      throw new NoSuchTableException("Table does not exist: %s", from);
    }
    checkNotRenamed(from);
    // Destination namespace must exist (in original or pending creates).
    if (checkNamespaceExists(to.namespace())) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", to.namespace());
    }
    // Destination must not already exist (in original or pending creates): a name this
    // transaction frees, by a drop or a rename out of it staged before, of a table (Bug 124) or a
    // view (errata D29 (txn takes no freed name), fixed), is free
    if (holds(to)) {
      throw new AlreadyExistsException("Table already exists: %s", to);
    }
    if (viewHolds(to)) {
      throw new AlreadyExistsException("Cannot rename %s to %s. View already exists", from, to);
    }
    tableRenames.put(from, to);
    return this;
  }

  /**
   * Whether {@code name} names a table in this transaction's view: one its read holds and it does
   * not free, or one it creates or renames in.
   */
  boolean holds(TableIdentifier name) {
    return (original.containsTable(name) && !frees(name))
        || tables.containsKey(name)
        || inlineTables.containsKey(name)
        || tableRenames.containsValue(name);
  }

  /** Whether a table or a view has {@code name} in this transaction's view ({@link #holds}). */
  boolean nameHeld(TableIdentifier name) {
    return holds(name) || viewHolds(name);
  }

  /** Whether this transaction frees {@code name}, which its read holds: a drop, or a rename out. */
  private boolean frees(TableIdentifier name) {
    return original.containsTable(name)
        && (tableDrops.contains(name) || tableRenames.containsKey(name));
  }

  /** As {@link #frees}, of a view: a drop, or a rename out. */
  private boolean viewFrees(TableIdentifier name) {
    return original.containsView(name)
        && (viewDrops.contains(name) || viewRenames.containsKey(name));
  }

  /**
   * Whether a create or rename into {@code name} takes a name this transaction frees, of a table or
   * a view: the record frees it before it takes it, and binds it by the namespace's version, as
   * every action verifies on the state before the record, which holds the name (errata D29 (txn
   * takes no freed name), fixed; Bug 124).
   */
  private boolean takesAFreedName(TableIdentifier name) {
    return original.containsTable(name) || original.containsView(name);
  }

  // ============================================================
  // Views (docs/VIEWS.md)
  // ============================================================

  /**
   * Creates a view with its whole metadata: a metadata file's location, or the metadata inline
   * ({@link CatalogState.ViewMetadataRef}). Its name must be free of tables and views alike.
   */
  public CatalogMutation createView(TableIdentifier view, CatalogState.ViewMetadataRef metadata) {
    Preconditions.checkArgument(metadata.wellFormed(), "Malformed view metadata for %s", view);
    if (checkNamespaceExists(view.namespace())) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", view.namespace());
    }
    if (viewHolds(view)) {
      throw new AlreadyExistsException("View already exists: %s", view);
    }
    if (holds(view)) {
      throw new AlreadyExistsException("Table with same name already exists: %s", view);
    }
    viewCreates.put(view, metadata);
    return this;
  }

  /** Sets a view's whole next metadata, applied only on the version this transaction read. */
  public CatalogMutation updateView(TableIdentifier view, CatalogState.ViewMetadataRef metadata) {
    Preconditions.checkArgument(metadata.wellFormed(), "Malformed view metadata for %s", view);
    if (viewCreates.containsKey(view)) {
      // a view this transaction creates: its create carries the metadata
      viewCreates.put(view, metadata);
      return this;
    }
    checkViewExists(view);
    viewUpdates.put(view, metadata);
    return this;
  }

  public CatalogMutation dropView(TableIdentifier view) {
    checkViewExists(view);
    viewUpdates.remove(view);
    viewEvictions.remove(view);
    viewDrops.add(view);
    return this;
  }

  public CatalogMutation renameView(TableIdentifier from, TableIdentifier to) {
    Preconditions.checkNotNull(from, "Source view cannot be null");
    Preconditions.checkNotNull(to, "Destination view cannot be null");
    Preconditions.checkArgument(!from.equals(to), "Source and destination identifiers are equal");
    checkViewExists(from);
    if (checkNamespaceExists(to.namespace())) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", to.namespace());
    }
    if (viewHolds(to)) {
      throw new AlreadyExistsException("Cannot rename %s to %s. View already exists", from, to);
    }
    if (holds(to)) {
      throw new AlreadyExistsException("Cannot rename %s to %s. Table already exists", from, to);
    }
    viewRenames.put(from, to);
    return this;
  }

  /**
   * Makes {@code eviction} how the inline create or update of view {@code view} staged on this
   * transaction goes to a metadata file, if the record must shrink to fit its log.
   */
  CatalogMutation viewEvictable(TableIdentifier view, Eviction eviction) {
    viewEvictions.put(view, eviction);
    return this;
  }

  /** Whether the transaction stages a view's create or update. */
  boolean stagesView(TableIdentifier view) {
    return viewCreates.containsKey(view) || viewUpdates.containsKey(view);
  }

  // Whether a view holds the name, so far as this transaction stages it: one in the read it does
  // not drop or rename out, one created, or one renamed in
  private boolean viewHolds(TableIdentifier ident) {
    return (original.containsView(ident) && !viewFrees(ident)) || viewCreates.containsKey(ident)
        || viewRenames.containsValue(ident);
  }

  private void checkNoView(TableIdentifier table) {
    if (viewHolds(table)) {
      throw new AlreadyExistsException("View with same name already exists: %s", table);
    }
  }

  // A view of the read, not dropped or renamed away by this transaction
  private void checkViewExists(TableIdentifier view) {
    if (!original.containsView(view) || viewDrops.contains(view)
        || viewRenames.containsKey(view)) {
      throw new NoSuchViewException("View does not exist: %s", view);
    }
  }

  public CatalogMutation dropTable(TableIdentifier tableId) {
    // A name a rename staged here took names the renamed table, which this record cannot drop: it
    // drops tables by their names in the read and renames after them, and dropping the table in
    // the rename's place would lose the conditions the rename binds its name by, which the record
    // must meet where it lands. And a table this transaction dropped is gone from its view. Both
    // were taken as a drop of what the read held under the name, again, leaving a renamed table
    // there (Bug 126). The catalog-transaction API drops a renamed table by its name in the read,
    // with no rename.
    if (tableRenames.containsValue(tableId)) {
      throw new IllegalArgumentException(
          "Cannot drop " + tableId + ": a table renamed into it in this transaction");
    }
    if (markedForDrop(tableId)) {
      throw new NoSuchTableException("Table does not exist: %s (dropped in this transaction)",
          tableId);
    }
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
    inlineTableUpdateRemovals.remove(tableId);
    inlineTableBroughtBack.remove(tableId);
    inlineTableImports.remove(tableId);
    inlineTableDeltaUpdates.remove(tableId);
    evictions.remove(tableId);
    tableDrops.add(tableId);
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
    if (holds(table)) {
      throw new AlreadyExistsException("Table already exists: %s", table);
    }
    checkNoView(table);
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

  /**
   * Makes the full update of {@code table}, an inline table ({@link #updateTableInline}), one whose
   * metadata holds only the snapshots the version this transaction read lacks, and its refs'
   * targets, dropping {@code removed} of that version's snapshots (SNAPSHOT_PAGING.md step 1).
   */
  public CatalogMutation removingSnapshots(TableIdentifier table, long[] removed) {
    inlineTableUpdateRemovals.put(table, removed);
    return this;
  }

  /**
   * Makes the inline update of {@code table} (delta or full) bring {@code back} inline first:
   * sealed snapshots its refs move onto, each with the proof replay checks against the root
   * (SNAPSHOT_PAGING.md, "Bringing a sealed snapshot back").
   */
  public CatalogMutation bringingBack(
      TableIdentifier table, List<SnapshotPages.BroughtBack> back) {
    if (back.isEmpty()) {
      inlineTableBroughtBack.remove(table);
    } else {
      inlineTableBroughtBack.put(table, back);
    }
    return this;
  }

  /**
   * Makes the full update of {@code table}, a pointer table coming back inline, install {@code
   * blocks} as its sealed blocks: its history, which its writer put as import pages before this
   * commit (SNAPSHOT_PAGING.md, "Re-inlining").
   */
  public CatalogMutation importingBlocks(
      TableIdentifier table, List<SnapshotPages.SealedBlock> blocks) {
    if (blocks.isEmpty()) {
      inlineTableImports.remove(table);
    } else {
      inlineTableImports.put(table, blocks);
    }
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
   * Makes the update of {@code table} apply only on the version this mutation read, whatever the
   * catalog's conflict encoding: an update Iceberg commits only on the version it was built on (a
   * schema, partition-spec or ref change), or a catalog transaction's update of a table that is
   * not an inline delta, which must find the table as the transaction first read it (Bug 70). An
   * update of a pointer table applies only there anyway.
   */
  public CatalogMutation pin(TableIdentifier table) {
    pinned.add(table);
    return this;
  }

  /**
   * Whether the update staged for {@code table} is an inline delta, which the catalog's conflict
   * encoding judges where the record lands, rather than full metadata or a metadata file, which
   * apply only on the version they were built on.
   */
  boolean stagedADelta(TableIdentifier table) {
    return inlineTableDeltaUpdates.containsKey(table);
  }

  /**
   * Makes the delta staged for {@code table} name {@code version} as the version it was built on,
   * rather than the table's version in this mutation's read: a catalog transaction stages a table's
   * updates on the version it first read, and the catalog's conflict encoding judges them from
   * there. {@code version} is the table's at or before this mutation's read.
   */
  CatalogMutation builtOn(TableIdentifier table, int version) {
    Preconditions.checkState(
        inlineTableDeltaUpdates.containsKey(table), "No delta staged for %s", table);
    Integer tblId = original.tableId(table);
    Preconditions.checkArgument(
        tblId != null && version <= original.tableVersion(tblId),
        "Version %s of %s is not at or before the version read", version, table);
    deltaBases.put(table, version);
    return this;
  }

  /**
   * How an inline update or create staged on a transaction goes to a metadata file instead, when
   * the transaction's record is larger than its log stores ({@link #evictLargest}): the table's
   * operations write its metadata as a pointer table's (each inline-ML snapshot's list to a file,
   * then a metadata file), as they do an update too large to inline.
   */
  interface Eviction {
    /**
     * Writes the table's metadata file and returns its location, adding each file it wrote to
     * {@code written}. A write that fails has what was written before it deleted, and throws.
     */
    String write(List<String> written);

    /**
     * Has the catalog's cleanup delete {@code files}, written for a record that did not land
     * ({@link CommitCleanup}).
     */
    void delete(List<String> files);
  }

  /**
   * Makes {@code eviction} how the inline update or create of {@code table} staged on this
   * transaction goes to a metadata file, if the record must shrink to fit its log.
   */
  CatalogMutation evictable(TableIdentifier table, Eviction eviction) {
    evictions.put(table, eviction);
    return this;
  }

  /**
   * Stages the largest inline update or create of this transaction that can go to a metadata file
   * ({@link #evictable}) as one: its eviction writes the file, and the update names it (an {@code
   * UpdateTableLocation} action, or a create of a pointer table), which takes the table out of
   * inline storage as an update too large to inline does. For a record larger than its log stores
   * ({@link CatalogLog#commit}). Largest by the bytes it holds inline. Returns false if there is
   * none left.
   *
   * <p>A delta a catalog transaction built on a version of its table older than this
   * transaction's read ({@link #builtOn}) goes last: a metadata file applies only on the version
   * it was built on, and the table has changed since, so its eviction fails the transaction as a
   * conflict, as such an update that never fit inline does ({@link CommitFailedException}).
   */
  boolean evictLargest() {
    evictions.keySet().removeIf(table -> !stagedInline(table));
    viewEvictions.keySet().removeIf(view -> inlineViewBytes(view) == 0);
    TableIdentifier largest = null;
    for (TableIdentifier table : evictions.keySet()) {
      if (largest == null || evictsBefore(table, largest)) {
        largest = table;
      }
    }
    // A view's inline metadata goes first when it is larger: a view is never behind its read
    TableIdentifier largestView = null;
    for (TableIdentifier view : viewEvictions.keySet()) {
      if (largestView == null || evictsBefore(inlineViewBytes(view), view,
          inlineViewBytes(largestView), largestView)) {
        largestView = view;
      }
    }
    if (largestView != null && (largest == null || behindRead(largest)
        || evictsBefore(inlineViewBytes(largestView), largestView, inlineBytes(largest), largest))) {
      Eviction eviction = viewEvictions.remove(largestView);
      List<String> written = new ArrayList<>();
      CatalogState.ViewMetadataRef location =
          CatalogState.ViewMetadataRef.location(eviction.write(written));
      evicted.add(Map.entry(eviction, written));
      if (viewCreates.containsKey(largestView)) {
        viewCreates.put(largestView, location);
      } else {
        viewUpdates.put(largestView, location);
      }
      return true;
    }
    if (largest == null) {
      return false;
    }
    if (behindRead(largest)) {
      throw new CommitFailedException(
          "Cannot commit: table %s changed since the transaction read it, and its update must "
              + "go to a metadata file, which applies only on the version it was built on, for "
              + "the transaction's record to fit in its log", largest);
    }
    Eviction eviction = evictions.remove(largest);
    List<String> written = new ArrayList<>();
    String location = eviction.write(written);
    evicted.add(Map.entry(eviction, written));
    if (inlineTables.containsKey(largest)) {
      inlineTables.remove(largest);
      inlineTableCodecs.remove(largest);
      inlineTableCreateDeltas.remove(largest);
      tables.put(largest, location);
    } else {
      inlineTableUpdates.remove(largest);
      inlineTableUpdateCodecs.remove(largest);
      inlineTableUpdateMlDeltas.remove(largest);
      inlineTableUpdateRemovals.remove(largest);
      inlineTableBroughtBack.remove(largest);
      inlineTableImports.remove(largest);
      inlineTableDeltaUpdates.remove(largest);
      deltaBases.remove(largest);
      tableUpdates.put(largest, location);
    }
    return true;
  }

  /**
   * Records {@code file}, written ahead of this transaction's record for a change staged on it:
   * garbage if the record never takes effect ({@link #abandonedFiles}).
   */
  void wroteAhead(String file) {
    writtenAhead.add(file);
  }

  /**
   * The files written ahead of this transaction's record ({@link #wroteAhead}), for the operation
   * that staged it to hand to its catalog's cleanup once it failed: all of them, if no commit of
   * the transaction may have landed (none was tried, or each definitely failed: {@link
   * CommitCleanup#definitelyFailed}); none if one may have, since its record may name them. The
   * files of the transaction's evictions are not among them: its commit deletes those itself
   * ({@link CatalogLog#commit}).
   */
  List<String> abandonedFiles() {
    return mayHaveLanded ? List.of() : List.copyOf(writtenAhead);
  }

  /**
   * Whether a commit of this transaction may have landed: one ended in a way that does not say its
   * record never takes effect ({@link CommitCleanup#definitelyFailed}), or returned.
   */
  boolean mayHaveLanded() {
    return mayHaveLanded;
  }

  /**
   * Has the catalog's cleanup delete the files this transaction's evictions wrote ({@link
   * #evictLargest}), for a record that did not land: none of them is named by the catalog.
   */
  void deleteEvicted() {
    for (Map.Entry<Eviction, List<String>> e : evicted) {
      e.getKey().delete(e.getValue());
    }
    evicted.clear();
  }

  private boolean stagedInline(TableIdentifier table) {
    return inlineTables.containsKey(table)
        || inlineTableUpdates.containsKey(table)
        || inlineTableDeltaUpdates.containsKey(table);
  }

  // Whether table goes before other: one that can go first, then the larger, then by name
  private boolean evictsBefore(TableIdentifier table, TableIdentifier other) {
    boolean behind = behindRead(table);
    if (behind != behindRead(other)) {
      return !behind;
    }
    return evictsBefore(inlineBytes(table), table, inlineBytes(other), other);
  }

  // The larger first, then by name
  private static boolean evictsBefore(
      long bytes, TableIdentifier ident, long otherBytes, TableIdentifier other) {
    return bytes != otherBytes
        ? bytes > otherBytes
        : ident.toString().compareTo(other.toString()) < 0;
  }

  // The bytes a view's staged inline create or update holds; 0 if none is staged inline
  private long inlineViewBytes(TableIdentifier view) {
    CatalogState.ViewMetadataRef metadata = viewCreates.containsKey(view)
        ? viewCreates.get(view) : viewUpdates.get(view);
    return metadata != null && metadata.inline() != null ? metadata.inline().length : 0;
  }

  // A delta built on a version of the table older than this transaction's read
  private boolean behindRead(TableIdentifier table) {
    Integer version = deltaBases.get(table);
    Integer tblId = original.tableId(table);
    return version != null && tblId != null && version != original.tableVersion(tblId);
  }

  // The bytes the table's inline update or create holds: what its eviction takes off the record
  private long inlineBytes(TableIdentifier table) {
    long bytes = length(inlineTables.get(table))
        + length(inlineTableCreateDeltas.get(table))
        + length(inlineTableUpdates.get(table))
        + length(inlineTableUpdateMlDeltas.get(table))
        + length(inlineTableDeltaUpdates.get(table));
    long[] removed = inlineTableUpdateRemovals.get(table);
    bytes += removed != null ? 8L * removed.length : 0;
    for (SnapshotPages.BroughtBack back : inlineTableBroughtBack.getOrDefault(table, List.of())) {
      bytes += back.leaf.length;
      for (byte[] hash : back.proof) {
        bytes += hash.length;
      }
    }
    for (SnapshotPages.SealedBlock block : inlineTableImports.getOrDefault(table, List.of())) {
      bytes += block.digest.length + block.merkleRoot.length + 8L * block.liveIds.length;
    }
    return bytes;
  }

  private static long length(byte[] bytes) {
    return bytes != null ? bytes.length : 0;
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
    return tableDrops.contains(table);
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
   * on in {@link #base}, which replay checks where the record lands; a create, and a rename's new
   * name, by its {@link #createCondition}: its namespace's removals, or its version, as the base
   * holds them (-1 for a namespace this transaction creates).
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
      int parentVersion = bindingRead(0);
      if (!parent.isEmpty()) {
        Integer existingParentId = original.namespaceId(parent);
        if (existingParentId != null) {
          parentId = existingParentId;
          parentVersion = bindingRead(existingParentId);
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
      actions.add(new ProtoCodec.CreateNamespaceAction(
          nsId, 1, parentId, byName(), parentVersion, name));

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

    // The names this record frees, first: replay verifies every action on the state before the
    // record, where a name a drop or rename out frees is still held, and applies them in order, so
    // a create or rename into a freed name (bound by the namespace's version: errata D29 (txn takes
    // no freed name), fixed; Bug 124) follows what frees it. Then the updates; the renames, each
    // out of a name before any into it, of either kind; the creates; and the namespace drops.
    for (TableIdentifier ident : tableDrops.stream()
        .sorted(Comparator.comparing(TableIdentifier::toString)).collect(Collectors.toList())) {
      Integer tblId = original.tableId(ident);
      if (tblId != null) {
        actions.add(new ProtoCodec.DropTableAction(tblId, original.tableVersion(tblId)));
      }
    }
    for (TableIdentifier view : viewDrops.stream()
        .sorted(Comparator.comparing(TableIdentifier::toString)).collect(Collectors.toList())) {
      int viewId = original.viewId(view);
      actions.add(new ProtoCodec.DropViewAction(viewId, original.viewVersion(viewId)));
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
    for (Map.Entry<TableIdentifier, java.util.SortedSet<String>> read : readTables.entrySet()) {
      Integer tblId = original.tableId(read.getKey());
      if (tblId != null) {
        int version = original.tableVersion(tblId);
        actions.add(new ProtoCodec.ReadTableAction(tblId, version, List.copyOf(read.getValue())));
      }
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
        long[] removed = inlineTableUpdateRemovals.get(ident);
        if (removed != null) {
          update = update.headOnly(removed);
        }
        update = update.bringingBack(inlineTableBroughtBack.getOrDefault(ident, List.of()))
            .importing(inlineTableImports.getOrDefault(ident, List.of()));
        actions.add(pinned.contains(ident) ? update.pin() : update);
      }
    }

    // Process inline table delta updates
    for (Map.Entry<TableIdentifier, byte[]> entry : inlineTableDeltaUpdates.entrySet()) {
      TableIdentifier ident = entry.getKey();
      byte[] deltaBytes = entry.getValue();
      Integer tblId = original.tableId(ident);
      if (tblId != null) {
        int version = deltaBases.getOrDefault(ident, original.tableVersion(tblId));
        ProtoCodec.UpdateTableInlineAction update =
            ProtoCodec.UpdateTableInlineAction.delta(tblId, version, deltaBytes)
                .bringingBack(inlineTableBroughtBack.getOrDefault(ident, List.of()));
        actions.add(pinned.contains(ident) ? update.pin() : update);
      }
    }

    for (Map.Entry<TableIdentifier, CatalogState.ViewMetadataRef> entry
        : sortedByName(viewUpdates).entrySet()) {
      int viewId = original.viewId(entry.getKey());
      actions.add(new ProtoCodec.UpdateViewAction(
          viewId, original.viewVersion(viewId), entry.getValue()));
    }

    // Renames (in place, id-stable; SPEC_TM.md "Rename"), tables' and views', each after any rename
    // out of the name it takes; staging refused a cycle (a swap)
    for (Map.Entry<TableIdentifier, TableIdentifier> rename : renameOrder()) {
      TableIdentifier from = rename.getKey();
      TableIdentifier to = rename.getValue();
      boolean view = viewRenames.containsKey(from);
      Integer newNsId = original.namespaceId(to.namespace());
      boolean byName = byName();
      int newNsRead;
      if (newNsId == null) {
        // Destination namespace was created in this same transaction.
        newNsId = nsIdMap.get(to.namespace());
        newNsRead = -1;
      } else if (takesAFreedName(to)) {
        byName = false;
        newNsRead = original.namespaceVersion(newNsId);
      } else {
        newNsRead = bindingRead(newNsId);
      }
      if (view) {
        int viewId = original.viewId(from);
        actions.add(new ProtoCodec.RenameViewAction(
            viewId, original.viewVersion(viewId), newNsId, byName, newNsRead, to.name()));
      } else {
        Integer tblId = original.tableId(from);
        if (tblId != null) {
          actions.add(new ProtoCodec.RenameTableAction(
              tblId, original.tableVersion(tblId), newNsId, byName, newNsRead, to.name()));
        }
      }
    }

    // Creates: tables (a metadata file's, then inline), then views (late-bound ids of their own)
    for (Map.Entry<TableIdentifier, String> entry : sortedByName(tables).entrySet()) {
      TableIdentifier ident = entry.getKey();
      Namespace ns = ident.namespace();
      Integer nsId = original.namespaceId(ns);
      boolean byName = byName();
      int nsVersion = -1;
      if (nsId == null) {
        // Namespace created in same transaction
        nsId = nsIdMap.get(ns);
      } else if (takesAFreedName(ident)) {
        byName = false;
        nsVersion = original.namespaceVersion(nsId);
      } else {
        nsVersion = bindingRead(nsId);
      }

      int tblId = nextLateTblid--;
      actions.add(new ProtoCodec.CreateTableAction(
          tblId, 1, nsId, byName, nsVersion, ident.name(), entry.getValue()));
    }

    // Process inline table creates
    for (Map.Entry<TableIdentifier, byte[]> entry : sortedByName(inlineTables).entrySet()) {
      TableIdentifier ident = entry.getKey();
      byte[] metadata = entry.getValue();
      Byte codecTag = inlineTableCodecs.get(ident);
      byte codec = codecTag != null ? codecTag : InlineMetadataCodecs.TAG_JSON_GZIP;
      Namespace ns = ident.namespace();

      Integer nsId = original.namespaceId(ns);
      boolean byName = byName();
      int nsVersion = -1;
      if (nsId == null) {
        nsId = nsIdMap.get(ns);
      } else if (takesAFreedName(ident)) {
        byName = false;
        nsVersion = original.namespaceVersion(nsId);
      } else {
        nsVersion = bindingRead(nsId);
      }

      int tblId = nextLateTblid--;
      byte[] createDelta = inlineTableCreateDeltas.get(ident);
      actions.add(new ProtoCodec.CreateTableInlineAction(
          tblId, 1, nsId, byName, nsVersion, ident.name(), metadata, codec, createDelta));
    }

    int nextLateViewId = -1;
    for (Map.Entry<TableIdentifier, CatalogState.ViewMetadataRef> entry
        : sortedByName(viewCreates).entrySet()) {
      TableIdentifier ident = entry.getKey();
      Integer nsId = original.namespaceId(ident.namespace());
      boolean byName = byName();
      int nsRead = -1;
      if (nsId == null) {
        nsId = nsIdMap.get(ident.namespace());
      } else if (takesAFreedName(ident)) {
        byName = false;
        nsRead = original.namespaceVersion(nsId);
      } else {
        nsRead = bindingRead(nsId);
      }
      actions.add(new ProtoCodec.CreateViewAction(
          nextLateViewId--, 1, nsId, byName, nsRead, ident.name(), entry.getValue()));
    }

    // Namespace drops last, children before parents: a namespace this transaction drops is empty
    // once its tables' drops and renames, and its children's drops, apply (dropNamespace)
    List<Namespace> dropped = namespaces.entrySet().stream()
        .filter(e -> !e.getValue())
        .map(Map.Entry::getKey)
        .sorted(Comparator.comparingInt(Namespace::length).reversed()
            .thenComparing(Namespace::toString))
        .collect(Collectors.toList());
    for (Namespace ns : dropped) {
      Integer nsId = original.namespaceId(ns);
      if (nsId != null) {
        int version = original.namespaceVersion(nsId);
        actions.add(new ProtoCodec.DropNamespaceAction(nsId, version));
      }
    }
    return new ProtoCodec.CatalogIntention(actions);
  }

  private boolean byName() {
    return createCondition == CreateCondition.NAME;
  }

  /**
   * This transaction's renames, tables' and views', in the order its record applies them: each
   * after any rename out of the name it takes, else in order of their identifiers.
   */
  private List<Map.Entry<TableIdentifier, TableIdentifier>> renameOrder() {
    List<Map.Entry<TableIdentifier, TableIdentifier>> left = new ArrayList<>();
    left.addAll(tableRenames.entrySet());
    left.addAll(viewRenames.entrySet());
    left.sort(Comparator.comparing(rename -> rename.getKey().toString()));
    List<Map.Entry<TableIdentifier, TableIdentifier>> ordered = new ArrayList<>();
    while (!left.isEmpty()) {
      Map.Entry<TableIdentifier, TableIdentifier> next = left.stream()
          .filter(r -> left.stream().noneMatch(o -> o.getKey().equals(r.getValue())))
          .findFirst()
          .orElse(left.get(0));
      left.remove(next);
      ordered.add(next);
    }
    return ordered;
  }

  // In order of their identifiers, so that a transaction's record is a function of what it staged
  private static <V> java.util.SortedMap<TableIdentifier, V> sortedByName(
      Map<TableIdentifier, V> map) {
    java.util.SortedMap<TableIdentifier, V> sorted =
        new java.util.TreeMap<>(Comparator.comparing(TableIdentifier::toString));
    sorted.putAll(map);
    return sorted;
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
    for (TableIdentifier table : tables.keySet()) {
      RuntimeException taken = tableNameTaken(state, table);
      if (taken != null) {
        return taken;
      }
    }
    for (TableIdentifier table : inlineTables.keySet()) {
      RuntimeException taken = tableNameTaken(state, table);
      if (taken != null) {
        return taken;
      }
    }
    for (Map.Entry<TableIdentifier, TableIdentifier> rename : tableRenames.entrySet()) {
      if (!state.containsTable(rename.getKey())) {
        return new NoSuchTableException("Table does not exist: %s", rename.getKey());
      }
      TableIdentifier to = rename.getValue();
      if (tableThere(state, to)) {
        return new AlreadyExistsException("Table already exists: %s", to);
      }
      if (viewThere(state, to)) {
        return new AlreadyExistsException(
            "Cannot rename %s to %s. View already exists", rename.getKey(), rename.getValue());
      }
    }
    for (TableIdentifier view : viewCreates.keySet()) {
      if (viewThere(state, view)) {
        return new AlreadyExistsException("View already exists: %s", view);
      }
      if (tableThere(state, view)) {
        return new AlreadyExistsException("Table with same name already exists: %s", view);
      }
    }
    for (Map.Entry<TableIdentifier, TableIdentifier> rename : viewRenames.entrySet()) {
      if (!state.containsView(rename.getKey())) {
        return new NoSuchViewException("View does not exist: %s", rename.getKey());
      }
      if (viewThere(state, rename.getValue())) {
        return new AlreadyExistsException(
            "Cannot rename %s to %s. View already exists", rename.getKey(), rename.getValue());
      }
      if (tableThere(state, rename.getValue())) {
        return new AlreadyExistsException(
            "Cannot rename %s to %s. Table already exists", rename.getKey(), rename.getValue());
      }
    }
    return new CommitFailedException("%s", message);
  }

  // Why a table create of the name fails on state, if a table or a view holds it
  private RuntimeException tableNameTaken(CatalogState state, TableIdentifier table) {
    if (tableThere(state, table)) {
      return new AlreadyExistsException("Table already exists: %s", table);
    }
    if (viewThere(state, table)) {
      return new AlreadyExistsException("View with same name already exists: %s", table);
    }
    return null;
  }

  // Whether a table holds the name on state, other than the one of the read this transaction frees
  // there, which holds it until the record applies
  private boolean tableThere(CatalogState state, TableIdentifier name) {
    return state.containsTable(name)
        && !(frees(name) && java.util.Objects.equals(state.tableId(name), original.tableId(name)));
  }

  // As tableThere, of a view
  private boolean viewThere(CatalogState state, TableIdentifier name) {
    return state.containsView(name)
        && !(viewFrees(name)
            && java.util.Objects.equals(state.viewId(name), original.viewId(name)));
  }

  /**
   * Commits this transaction to {@code log}: {@link CatalogLog#commit}. Notes an outcome that may
   * have landed ({@link #mayHaveLanded}).
   */
  public Replica<CatalogState> commit(CatalogLog log) {
    try {
      Replica<CatalogState> landed = log.commit(this);
      mayHaveLanded = true;
      return landed;
    } catch (RuntimeException e) {
      if (!CommitCleanup.definitelyFailed(e)) {
        mayHaveLanded = true;
      }
      throw e;
    }
  }
}
