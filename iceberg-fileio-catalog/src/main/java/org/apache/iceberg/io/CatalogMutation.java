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
            && tables.entrySet().stream()
                .filter(e -> e.getValue() != null) // only table creations
                .map(e -> e.getKey().namespace())
                .noneMatch(ns -> ns.equals(namespace))
            // inline tables this transaction creates (Bug 74: these were missed, so the drop
            // staged, and replay rejected the commit as a conflict)
            && inlineTables.keySet().stream()
                .map(TableIdentifier::namespace)
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
    if (!original.containsTable(table)) {
      throw new NoSuchTableException("Table does not exist: %s", table);
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
    // Destination must not already exist (in original or pending creates): a name this
    // transaction frees, by a drop or a rename out of it staged before, is free (Bug 124)
    if (holds(to)) {
      throw new AlreadyExistsException("Table already exists: %s", to);
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
        || tables.get(name) != null
        || inlineTables.containsKey(name)
        || tableRenames.containsValue(name);
  }

  /** Whether this transaction frees {@code name}, which its read holds: a drop, or a rename out. */
  private boolean frees(TableIdentifier name) {
    return original.containsTable(name)
        && ((tables.containsKey(name) && tables.get(name) == null)
            || tableRenames.containsKey(name));
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
    TableIdentifier largest = null;
    for (TableIdentifier table : evictions.keySet()) {
      if (largest == null || evictsBefore(table, largest)) {
        largest = table;
      }
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
    long bytes = inlineBytes(table);
    long otherBytes = inlineBytes(other);
    return bytes != otherBytes
        ? bytes > otherBytes
        : table.toString().compareTo(other.toString()) < 0;
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
        nsVersion = bindingRead(nsId);
      }

      int tblId = nextLateTblid--;
      actions.add(new ProtoCodec.CreateTableAction(
          tblId, 1, nsId, byName(), nsVersion, ident.name(), metadataLocation));
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
        nsVersion = bindingRead(nsId);
      }

      int tblId = nextLateTblid--;
      byte[] createDelta = inlineTableCreateDeltas.get(ident);
      actions.add(new ProtoCodec.CreateTableInlineAction(
          tblId, 1, nsId, byName(), nsVersion, ident.name(), metadata, codec, createDelta));
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
      boolean byName = byName();
      if (newNsId == null) {
        // Destination namespace was created in this same transaction.
        newNsId = nsIdMap.get(newNs);
        newNsVersion = -1;
      } else if (original.containsTable(to)) {
        // A name this transaction frees (renameTable), by a drop or a rename out that the record
        // applies first, is taken where every action verifies, the state before the record: it
        // binds by the namespace's version, which every change of the namespace's names moves
        // (Bug 124)
        byName = false;
        newNsVersion = original.namespaceVersion(newNsId);
      } else {
        newNsVersion = bindingRead(newNsId);
      }
      actions.add(new ProtoCodec.RenameTableAction(
          tblId, version, newNsId, byName, newNsVersion, to.name()));
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
      // A name this transaction frees is held by the table it frees until the record applies
      TableIdentifier to = rename.getValue();
      if (state.containsTable(to)
          && !(frees(to) && java.util.Objects.equals(state.tableId(to), original.tableId(to)))) {
        return new AlreadyExistsException("Table already exists: %s", to);
      }
    }
    return new CommitFailedException("%s", message);
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
