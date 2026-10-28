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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.intentlog.StateMachine;

/**
 * The catalog's state, as replay of its log reaches it ({@link CatalogStateMachine}): namespaces
 * and their properties, and tables, each with a pointer to its metadata file or its metadata (and
 * manifest lists) inline. Every namespace and table has an id and a version, which each change to
 * it bumps; a transaction's intention names the versions it read. Immutable; the log's id and
 * committed set are the {@link org.intentlog.Replica}'s, not the state's.
 */
public class CatalogState {

  /** Id of the root namespace's row. Every catalog has one; it is never dropped. */
  static final int ROOT_ID = 0;

  // ID tracking
  private final int nextNamespaceId;
  private final int nextTableId;

  // Namespace state: id -> (parentId, name, version)
  private final Map<Integer, NsEntry> namespaceById;
  private final Map<Namespace, Integer> namespaceLookup;

  // Table state: id -> (nsId, name, version, metadataLocation)
  private final Map<Integer, TblEntry> tableById;
  private final Map<TableIdentifier, Integer> tableLookup;

  // Namespace properties: nsId -> (key -> value)
  private final Map<Integer, Map<String, String>> nsProperties;

  // Inline table metadata: tblId -> the table's metadata (encoded, or as replay keeps it).
  // Inline tables also appear in tableById with metadataLocation = null for lookup purposes.
  private final Map<Integer, InlineTable> tblInlineMetadata;
  private final Map<Integer, String> tblManifestPrefix;

  // Inline manifest list state: per-table pool of unique ManifestFile entries keyed by path,
  // and per-snapshot references into the pool. Present only when inline.manifests is enabled.
  // tblId -> (manifest path -> ManifestFile)
  private final Map<Integer, Map<String, ManifestFile>> manifestPool;
  // tblId -> (snapshotId -> list of manifest paths in the pool)
  private final Map<Integer, Map<Long, List<String>>> snapshotManifests;
  // tblId -> how many times a pool entry was replaced by another for the same path: not state
  // (no checkpoint holds it), but what tells a load that a list it resolved before may resolve to
  // other entries now (LogCatalog.InlineSnapshots)
  private final Map<Integer, Long> poolReplacements;

  // The catalog's conflict encoding, null until its first record sets one (then Lamport), and
  // the encoding's state of each table that has one: tblId -> state.
  private final String conflictEncoding;
  private final Map<Integer, EncodingState> conflictStates;

  private CatalogState(Builder builder) {
    this.nextNamespaceId = builder.nextNamespaceId;
    this.nextTableId = builder.nextTableId;
    this.namespaceById = ImmutableMap.copyOf(builder.namespaceById);
    this.namespaceLookup = ImmutableMap.copyOf(builder.namespaceLookup);
    this.tableById = ImmutableMap.copyOf(builder.tableById);
    this.tableLookup = ImmutableMap.copyOf(builder.tableLookup);
    this.nsProperties = deepCopyProperties(builder.nsProperties);
    this.tblInlineMetadata = ImmutableMap.copyOf(builder.tblInlineMetadata);
    this.tblManifestPrefix = ImmutableMap.copyOf(builder.tblManifestPrefix);
    builder.freezeManifestLists();
    this.manifestPool = ImmutableMap.copyOf(builder.manifestPool);
    this.snapshotManifests = ImmutableMap.copyOf(builder.snapshotManifests);
    this.poolReplacements = ImmutableMap.copyOf(builder.poolReplacements);
    this.conflictEncoding = builder.conflictEncoding;
    ImmutableMap.Builder<Integer, EncodingState> states = ImmutableMap.builder();
    builder.conflictStates.forEach((id, state) -> states.put(id, state.copy()));
    this.conflictStates = states.build();
  }

  private static Map<Integer, Map<String, String>> deepCopyProperties(
      Map<Integer, Map<String, String>> props) {
    ImmutableMap.Builder<Integer, Map<String, String>> builder = ImmutableMap.builder();
    for (Map.Entry<Integer, Map<String, String>> entry : props.entrySet()) {
      builder.put(entry.getKey(), ImmutableMap.copyOf(entry.getValue()));
    }
    return builder.build();
  }

  /**
   * A builder that continues from this state, sharing what it holds: a table's manifest lists are
   * copied on the builder's first change to them, and inline metadata is immutable.
   */
  Builder toBuilder() {
    Builder builder = new Builder();
    builder.nextNamespaceId = nextNamespaceId;
    builder.nextTableId = nextTableId;
    builder.namespaceById.clear();
    builder.namespaceById.putAll(namespaceById);
    builder.namespaceLookup.putAll(namespaceLookup);
    builder.tableById.putAll(tableById);
    builder.tableLookup.putAll(tableLookup);
    nsProperties.forEach((id, props) -> builder.nsProperties.put(id, new HashMap<>(props)));
    builder.tblInlineMetadata.putAll(tblInlineMetadata);
    builder.tblManifestPrefix.putAll(tblManifestPrefix);
    builder.manifestPool.putAll(manifestPool);
    builder.snapshotManifests.putAll(snapshotManifests);
    builder.poolReplacements.putAll(poolReplacements);
    builder.conflictEncoding = conflictEncoding;
    conflictStates.forEach((id, state) -> builder.conflictStates.put(id, state.copy()));
    return builder;
  }

  /** The state of a catalog with no namespaces or tables: a log before its first record. */
  public static CatalogState empty() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  // ============================================================
  // Namespaces and tables by name
  // ============================================================

  /** The location of {@code table}'s metadata file; null if it is inline or does not exist. */
  public String location(TableIdentifier table) {
    Integer id = tableLookup.get(table);
    if (id == null) {
      return null;
    }
    TblEntry entry = tableById.get(id);
    return entry != null ? entry.metadataLocation : null;
  }

  public Set<Namespace> namespaces() {
    return namespaceLookup.keySet();
  }

  public boolean containsNamespace(Namespace ns) {
    return namespaceLookup.containsKey(ns);
  }

  public Map<String, String> namespaceProperties(Namespace ns) {
    Integer id = namespaceLookup.get(ns);
    if (id == null) {
      return ImmutableMap.of();
    }
    Map<String, String> props = nsProperties.get(id);
    return props != null ? props : ImmutableMap.of();
  }

  public List<TableIdentifier> tables() {
    return new ArrayList<>(tableLookup.keySet());
  }

  /** Whether the table exists, inline or with a metadata file. */
  public boolean containsTable(TableIdentifier table) {
    return tableLookup.containsKey(table);
  }

  /** Whether the table's metadata is inline, in the catalog. */
  public boolean isInlineTable(TableIdentifier table) {
    Integer id = tableLookup.get(table);
    return id != null && tblInlineMetadata.containsKey(id);
  }

  /** The table's inline metadata (encoded by its codec), or null if it is not inline. */
  public byte[] inlineMetadata(TableIdentifier table) {
    Integer id = tableLookup.get(table);
    return id != null ? inlineMetadata(id) : null;
  }

  // ============================================================
  // Ids, versions and inline storage
  // ============================================================

  public int nextNamespaceId() {
    return nextNamespaceId;
  }

  public int nextTableId() {
    return nextTableId;
  }

  public Integer namespaceId(Namespace ns) {
    return namespaceLookup.get(ns);
  }

  public int namespaceVersion(int nsId) {
    NsEntry entry = namespaceById.get(nsId);
    return entry != null ? entry.version : -1;
  }

  public NsEntry namespaceEntry(int nsId) {
    return namespaceById.get(nsId);
  }

  public Integer tableId(TableIdentifier ident) {
    return tableLookup.get(ident);
  }

  public int tableVersion(int tblId) {
    TblEntry entry = tableById.get(tblId);
    return entry != null ? entry.version : -1;
  }

  public TblEntry tableEntry(int tblId) {
    return tableById.get(tblId);
  }

  Map<Integer, NsEntry> namespaceById() {
    return namespaceById;
  }

  Map<Integer, TblEntry> tableById() {
    return tableById;
  }

  Map<Integer, Map<String, String>> allNamespaceProperties() {
    return nsProperties;
  }

  /**
   * Returns inline metadata bytes for the given table ID, or null if not inline. A table replay
   * changed is encoded when first asked for.
   */
  public byte[] inlineMetadata(int tblId) {
    InlineTable table = tblInlineMetadata.get(tblId);
    return table != null ? table.bytes() : null;
  }

  /** The table's inline metadata, or null if it is not inline. */
  InlineTable inlineTable(int tblId) {
    return tblInlineMetadata.get(tblId);
  }

  /**
   * Returns the codec used to encode the bytes of {@link #inlineMetadata(int)}.
   * Only valid for inline tables; the wire format requires the codec tag to
   * be present alongside the inline metadata bytes.
   */
  public byte inlineMetadataCodec(int tblId) {
    InlineTable table = tblInlineMetadata.get(tblId);
    Preconditions.checkState(
        table != null, "Not an inline table or missing codec tag: %s", tblId);
    return table.codec();
  }

  /** Returns the manifest list prefix for the given inline table, or null. */
  public String manifestListPrefix(int tblId) {
    return tblManifestPrefix.get(tblId);
  }

  /** Returns true if the table is stored inline (metadata in catalog, not external file). */
  public boolean isInlineTable(int tblId) {
    return tblInlineMetadata.containsKey(tblId);
  }

  Map<Integer, InlineTable> allInlineTables() {
    return tblInlineMetadata;
  }

  Map<Integer, String> allManifestPrefixes() {
    return tblManifestPrefix;
  }

  /** Returns true if the table has inline manifest list data for the given snapshot. */
  public boolean hasInlineManifests(int tblId, long snapshotId) {
    Map<Long, List<String>> refs = snapshotManifests.get(tblId);
    return refs != null && refs.containsKey(snapshotId);
  }

  /** Returns the inline manifest list for the given table and snapshot, resolving from the pool. */
  public List<ManifestFile> inlineManifests(int tblId, long snapshotId) {
    Map<Long, List<String>> refs = snapshotManifests.get(tblId);
    if (refs == null) {
      return null;
    }
    List<String> paths = refs.get(snapshotId);
    if (paths == null) {
      return null;
    }
    Map<String, ManifestFile> pool = manifestPool.get(tblId);
    if (pool == null) {
      return ImmutableList.of();
    }
    ImmutableList.Builder<ManifestFile> result = ImmutableList.builder();
    for (String path : paths) {
      ManifestFile mf = pool.get(path);
      if (mf != null) {
        result.add(mf);
      }
    }
    return result.build();
  }

  /** Returns the manifest pool for the given table, or empty map. */
  Map<String, ManifestFile> manifestPool(int tblId) {
    Map<String, ManifestFile> pool = manifestPool.get(tblId);
    return pool != null ? pool : ImmutableMap.of();
  }

  /** Returns the snapshot manifest refs for the given table, or empty map. */
  Map<Long, List<String>> snapshotManifests(int tblId) {
    Map<Long, List<String>> refs = snapshotManifests.get(tblId);
    return refs != null ? refs : ImmutableMap.of();
  }

  /**
   * How many times an entry of the table's manifest pool was replaced by another for the same path
   * in the states this one descends from since it was decoded: while it is unchanged, a manifest
   * path resolves to the entry it did.
   */
  long poolReplacements(int tblId) {
    return poolReplacements.getOrDefault(tblId, 0L);
  }

  /** Returns the full manifest pool map for checkpoint encoding. */
  Map<Integer, Map<String, ManifestFile>> allManifestPools() {
    return manifestPool;
  }

  /** Returns the full snapshot manifest refs map for checkpoint encoding. */
  Map<Integer, Map<Long, List<String>>> allSnapshotManifests() {
    return snapshotManifests;
  }

  /**
   * The name of the catalog's conflict encoding, or null if no record has set one: then replay
   * judges by the Lamport clock ({@link ConflictEncoding}).
   */
  public String conflictEncodingName() {
    return conflictEncoding;
  }

  /** The catalog's conflict encoding. */
  ConflictEncoding<Object> conflictEncoding() {
    return ConflictEncodings.byName(conflictEncoding != null ? conflictEncoding : LamportEncoding.NAME);
  }

  /** The encoding's state of each table that has one, for checkpoint encoding. */
  Map<Integer, EncodingState> conflictStates() {
    return conflictStates;
  }

  // ============================================================
  // Entry types
  // ============================================================

  public static class NsEntry {
    public final int parentId;
    public final String name;
    public final int version;

    public NsEntry(int parentId, String name, int version) {
      this.parentId = parentId;
      this.name = name;
      this.version = version;
    }
  }

  public static class TblEntry {
    public final int namespaceId;
    public final String name;
    public final int version;
    public final String metadataLocation;

    public TblEntry(int namespaceId, String name, int version, String metadataLocation) {
      this.namespaceId = namespaceId;
      this.name = name;
      this.version = version;
      this.metadataLocation = metadataLocation;
    }
  }

  // ============================================================
  // Builder
  // ============================================================

  public static class Builder implements StateMachine.Builder<CatalogState> {
    private int nextNamespaceId = 1;
    private int nextTableId = 1;

    private final Map<Integer, NsEntry> namespaceById = new HashMap<>();
    private final Map<Namespace, Integer> namespaceLookup = new HashMap<>();
    private final Map<Integer, TblEntry> tableById = new HashMap<>();
    private final Map<TableIdentifier, Integer> tableLookup = new HashMap<>();
    private final Map<Integer, Map<String, String>> nsProperties = new HashMap<>();
    private final Map<Integer, InlineTable> tblInlineMetadata = new HashMap<>();
    private final Map<Integer, String> tblManifestPrefix = new HashMap<>();
    // A table's manifest pool and lists: immutable, shared with the state this builder continues,
    // until its first change here copies them (ownedPools, ownedLists). Each list is immutable.
    private final Map<Integer, Map<String, ManifestFile>> manifestPool = new HashMap<>();
    private final Map<Integer, Map<Long, List<String>>> snapshotManifests = new HashMap<>();
    private final Set<Integer> ownedPools = new HashSet<>();
    private final Set<Integer> ownedLists = new HashSet<>();
    private final Map<Integer, Long> poolReplacements = new HashMap<>();
    private String conflictEncoding;
    private final Map<Integer, EncodingState> conflictStates = new HashMap<>();

    Builder() {
      // The root namespace is an ordinary row with id 0, so it is versioned
      // like any other parent: creating or dropping a top-level namespace bumps
      // it. It cannot be created or dropped, and it is not addressable by name
      // (see rebuildLookups). A decoded checkpoint replaces this default row.
      namespaceById.put(ROOT_ID, new NsEntry(ROOT_ID, "", 0));
    }

    public Builder setNextNamespaceId(int id) {
      this.nextNamespaceId = id;
      return this;
    }

    public Builder setNextTableId(int id) {
      this.nextTableId = id;
      return this;
    }

    public Builder addNamespace(int id, int parentId, String name, int version) {
      NsEntry entry = new NsEntry(parentId, name, version);
      namespaceById.put(id, entry);
      // Keep nextNamespaceId monotone: a transaction-replayed CreateNamespaceAction
      // adds an id that wasn't reflected in the (older) checkpoint header, so the
      // checkpoint-derived nextNamespaceId would otherwise hand the same id out
      // again on the next commit and overwrite this entry.
      if (id >= nextNamespaceId) {
        nextNamespaceId = id + 1;
      }
      // Lookup is built by rebuildLookups() after all entries are loaded.
      // During checkpoint decoding, namespaces may arrive in arbitrary order
      // (HashMap iteration), so the full ancestor chain may not be available yet.
      return this;
    }

    /**
     * Rebuilds namespace and table lookups after bulk loading from checkpoint.
     * Call this after decoding a checkpoint to ensure all lookups are populated.
     */
    public Builder rebuildLookups() {
      namespaceLookup.clear();
      for (Integer id : namespaceById.keySet()) {
        if (id != ROOT_ID) {  // the root is not a namespace callers can name
          namespaceLookup.put(buildNamespace(id), id);
        }
      }
      tableLookup.clear();
      for (Map.Entry<Integer, TblEntry> entry : tableById.entrySet()) {
        Namespace ns = buildNamespace(entry.getValue().namespaceId);
        tableLookup.put(TableIdentifier.of(ns, entry.getValue().name), entry.getKey());
      }
      return this;
    }

    public Builder removeNamespace(int id) {
      Preconditions.checkArgument(id != ROOT_ID, "Cannot remove the root namespace");
      // Find the lookup key before removing from namespaceById, since the path
      // needs the entry.
      if (namespaceById.containsKey(id)) {
        removeLookup(namespaceLookup, pathOrNull(id), id);
        namespaceById.remove(id);
        nsProperties.remove(id);
      }
      return this;
    }

    /**
     * The path of namespace {@code id}, or null if it or an ancestor is gone. A
     * transaction that drops a namespace and its child may apply the drops in
     * either order, so a removal cannot rely on the parent chain.
     */
    private Namespace pathOrNull(int id) {
      List<String> levels = new ArrayList<>();
      for (int cur = id; cur != ROOT_ID; ) {
        NsEntry entry = namespaceById.get(cur);
        if (entry == null) {
          return null;
        }
        levels.add(entry.name);
        cur = entry.parentId;
      }
      Collections.reverse(levels);
      return levels.isEmpty() ? Namespace.empty() : Namespace.of(levels.toArray(new String[0]));
    }

    private static <K> void removeLookup(Map<K, Integer> lookup, K key, int id) {
      if (key != null) {
        lookup.remove(key);
      } else {
        lookup.values().remove(id);
      }
    }

    private TableIdentifier tableKeyOrNull(TblEntry entry) {
      Namespace ns = pathOrNull(entry.namespaceId);
      return ns == null ? null : TableIdentifier.of(ns, entry.name);
    }

    public Builder updateNamespaceVersion(int id, int newVersion) {
      NsEntry old = namespaceById.get(id);
      if (old != null) {
        namespaceById.put(id, new NsEntry(old.parentId, old.name, newVersion));
      }
      return this;
    }

    /**
     * Increments a namespace's version by one. Called from action applies that mutate
     * the namespace (children set, properties) so that concurrent / replayed actions
     * whose preconditions captured the pre-bump version fail {@code verify}.
     */
    public Builder bumpNamespaceVersion(int id) {
      NsEntry old = namespaceById.get(id);
      if (old != null) {
        namespaceById.put(id, new NsEntry(old.parentId, old.name, old.version + 1));
      }
      return this;
    }

    public Builder addTable(int id, int namespaceId, String name, int version, String metadataLocation) {
      TblEntry entry = new TblEntry(namespaceId, name, version, metadataLocation);
      tableById.put(id, entry);

      Namespace ns = buildNamespace(namespaceId);
      tableLookup.put(TableIdentifier.of(ns, name), id);
      // Same monotonicity argument as addNamespace: keep nextTableId ahead of
      // any id that arrived via transaction replay.
      if (id >= nextTableId) {
        nextTableId = id + 1;
      }
      return this;
    }

    /**
     * Removes a table, with its inline metadata and manifest lists if it has them, and its
     * conflict encoding's state.
     */
    public Builder removeTable(int id) {
      TblEntry entry = tableById.remove(id);
      if (entry != null) {
        removeLookup(tableLookup, tableKeyOrNull(entry), id);
      }
      conflictStates.remove(id);
      return removeInlineMetadata(id);
    }

    public Builder updateTableLocation(int id, int newVersion, String metadataLocation) {
      TblEntry old = tableById.get(id);
      if (old != null) {
        tableById.put(id, new TblEntry(old.namespaceId, old.name, newVersion, metadataLocation));
      }
      return this;
    }

    /**
     * Renames a table in place: keeps the id, swaps namespaceId/name/version, and
     * preserves the existing metadataLocation (inline tables carry null here).
     * Inline state (tblInlineMetadata, tblManifestPrefix, manifestPool,
     * snapshotManifests) is id-keyed and intentionally untouched. See SPEC_TM.md
     * "Rename".
     */
    public Builder renameTable(int id, int newNamespaceId, String newName, int newVersion) {
      TblEntry old = tableById.get(id);
      if (old == null) {
        return this;
      }
      removeLookup(tableLookup, tableKeyOrNull(old), id);
      tableById.put(id, new TblEntry(newNamespaceId, newName, newVersion, old.metadataLocation));
      Namespace newNs = buildNamespace(newNamespaceId);
      tableLookup.put(TableIdentifier.of(newNs, newName), id);
      return this;
    }

    /**
     * Adds an inline table (metadata stored in catalog, not external file).
     * The table is also added to tableById with null metadataLocation for lookup.
     */
    public Builder addInlineTable(
        int id, int namespaceId, String name, int version,
        byte[] metadata, String manifestListPrefix) {
      return addInlineTable(
          id, namespaceId, name, version, metadata,
          InlineMetadataCodecs.TAG_JSON_GZIP, manifestListPrefix);
    }

    /**
     * Adds an inline table with an explicit codec tag for the encoded metadata
     * bytes.
     */
    public Builder addInlineTable(
        int id, int namespaceId, String name, int version,
        byte[] metadata, byte metadataCodec, String manifestListPrefix) {
      // Add to tableById for lookup (null location marks it as inline)
      TblEntry entry = new TblEntry(namespaceId, name, version, null);
      tableById.put(id, entry);
      Namespace ns = buildNamespace(namespaceId);
      tableLookup.put(TableIdentifier.of(ns, name), id);
      // Store inline-specific data
      tblInlineMetadata.put(id, InlineTable.ofBytes(metadata, metadataCodec));
      tblManifestPrefix.put(id, manifestListPrefix);
      // Same monotonicity argument as addTable: keep nextTableId ahead of any id
      // that arrived via transaction replay. Without this, a fresh checkpoint
      // (post-CAS) carries nextTableId from before the inline table was created;
      // the next replay allocates the same id again and the second
      // createTableInline overwrites the first in tableById / tblInlineMetadata.
      // testListTables, testRenameTableDestinationTableAlreadyExists, and the
      // append+TM transaction tests' NoSuchTable errors all trace back to this.
      if (id >= nextTableId) {
        nextTableId = id + 1;
      }
      return this;
    }

    /**
     * Updates inline metadata and version, recording the codec used to encode
     * {@code metadata}. Preserves the manifest pool. A table that returns to
     * inline storage gets the empty manifest list prefix, as a created one does.
     */
    public Builder updateInlineMetadata(int id, int newVersion, byte[] metadata, byte codec) {
      return updateInlineTable(id, newVersion, InlineTable.ofBytes(metadata, codec));
    }

    /** As {@link #updateInlineMetadata}, with the table's metadata as replay keeps it. */
    Builder updateInlineTable(int id, int newVersion, InlineTable table) {
      tblInlineMetadata.put(id, table);
      tblManifestPrefix.putIfAbsent(id, "");
      TblEntry old = tableById.get(id);
      if (old != null) {
        tableById.put(id, new TblEntry(old.namespaceId, old.name, newVersion, null));
      }
      return this;
    }

    /** Removes inline metadata for a table (e.g., when transitioning to pointer mode). */
    public Builder removeInlineMetadata(int id) {
      tblInlineMetadata.remove(id);
      tblManifestPrefix.remove(id);
      manifestPool.remove(id);
      snapshotManifests.remove(id);
      poolReplacements.remove(id);
      ownedPools.remove(id);
      ownedLists.remove(id);
      return this;
    }

    /** The table's manifest pool, to change: this builder's own. */
    private Map<String, ManifestFile> poolToChange(int tblId) {
      Map<String, ManifestFile> pool = manifestPool.get(tblId);
      if (pool == null || ownedPools.add(tblId)) {
        pool = pool != null ? new LinkedHashMap<>(pool) : new LinkedHashMap<>();
        manifestPool.put(tblId, pool);
        ownedPools.add(tblId);
      }
      return pool;
    }

    /** The table's manifest lists, to change: this builder's own. */
    private Map<Long, List<String>> listsToChange(int tblId) {
      Map<Long, List<String>> lists = snapshotManifests.get(tblId);
      if (lists == null || ownedLists.add(tblId)) {
        lists = lists != null ? new HashMap<>(lists) : new HashMap<>();
        snapshotManifests.put(tblId, lists);
        ownedLists.add(tblId);
      }
      return lists;
    }

    /**
     * Freezes the manifest pools and lists this builder changed, which a state built now shares;
     * a later change copies them again.
     */
    private void freezeManifestLists() {
      for (int tblId : ownedPools) {
        manifestPool.computeIfPresent(tblId, (id, pool) -> ImmutableMap.copyOf(pool));
      }
      for (int tblId : ownedLists) {
        snapshotManifests.computeIfPresent(tblId, (id, lists) -> ImmutableMap.copyOf(lists));
      }
      ownedPools.clear();
      ownedLists.clear();
    }

    /** Adds a ManifestFile to the table's pool. */
    public Builder addManifestToPool(int tblId, ManifestFile manifest) {
      ManifestFile replaced = poolToChange(tblId).put(manifest.path(), manifest);
      if (replaced != null && replaced != manifest) {
        poolReplacements.merge(tblId, 1L, Long::sum);
      }
      return this;
    }

    /** Sets the manifest list for a snapshot (list of manifest paths in the pool). */
    public Builder setSnapshotManifests(int tblId, long snapshotId, List<String> paths) {
      listsToChange(tblId).put(snapshotId, List.copyOf(paths));
      return this;
    }

    /** Whether the table has an inline manifest list for {@code snapshotId}. */
    public boolean hasSnapshotManifests(int tblId, long snapshotId) {
      Map<Long, List<String>> refs = snapshotManifests.get(tblId);
      return refs != null && refs.containsKey(snapshotId);
    }

    /** The table's manifest pool, by path: every manifest its inline lists name. */
    Map<String, ManifestFile> manifestPool(int tblId) {
      Map<String, ManifestFile> pool = manifestPool.get(tblId);
      return pool != null ? Collections.unmodifiableMap(pool) : Map.of();
    }

    /** Returns true if the table has any manifest pool entries. */
    public boolean hasManifestPool(int tblId) {
      Map<String, ManifestFile> pool = manifestPool.get(tblId);
      return pool != null && !pool.isEmpty();
    }

    /** Returns the manifest paths for a snapshot, or empty list. */
    public List<String> snapshotManifestPaths(int tblId, long snapshotId) {
      Map<Long, List<String>> refs = snapshotManifests.get(tblId);
      if (refs == null) {
        return List.of();
      }
      List<String> paths = refs.get(snapshotId);
      return paths != null ? paths : List.of();
    }

    /**
     * Returns the set of snapshot ids for which this table has manifest ref
     * lists in the pool. Used by full-mode replay to reconcile pool refs
     * against the new {@code TableMetadata} (drop refs for snapshots that
     * are no longer present).
     */
    public Set<Long> snapshotManifestKeys(int tblId) {
      Map<Long, List<String>> refs = snapshotManifests.get(tblId);
      if (refs == null) {
        return java.util.Collections.emptySet();
      }
      return new HashSet<>(refs.keySet());
    }

    /**
     * Drops a snapshot's manifest ref list and GCs any pool entries no longer
     * referenced. Called from RemoveSnapshotsUpdate replay to prevent unbounded
     * pool growth after snapshot expiration (SPEC_ML.md).
     */
    public Builder removeSnapshotManifests(int tblId, long snapshotId) {
      return removeSnapshotManifests(tblId, List.of(snapshotId));
    }

    /**
     * Drops the manifest ref lists of {@code snapshotIds} and GCs the pool entries no remaining
     * list names, scanning the remaining lists once.
     */
    public Builder removeSnapshotManifests(int tblId, java.util.Collection<Long> snapshotIds) {
      Map<Long, List<String>> refs = snapshotManifests.get(tblId);
      if (refs == null) {
        return this;
      }
      Set<String> dropped = new HashSet<>();
      boolean any = false;
      for (long snapshotId : snapshotIds) {
        if (refs.containsKey(snapshotId)) {
          any = true;
          dropped.addAll(listsToChange(tblId).remove(snapshotId));
          refs = snapshotManifests.get(tblId);
        }
      }
      if (!any || dropped.isEmpty()) {
        return this;
      }
      // Paths still referenced by any remaining snapshot stay
      for (List<String> snapRefs : refs.values()) {
        for (String path : snapRefs) {
          dropped.remove(path);
        }
        if (dropped.isEmpty()) {
          return this;
        }
      }
      if (manifestPool.containsKey(tblId)) {
        Map<String, ManifestFile> pool = poolToChange(tblId);
        for (String path : dropped) {
          pool.remove(path);
        }
      }
      return this;
    }

    public boolean isInlineTable(int id) {
      return tblInlineMetadata.containsKey(id);
    }

    public byte[] inlineMetadata(int id) {
      InlineTable table = tblInlineMetadata.get(id);
      return table != null ? table.bytes() : null;
    }

    /** The table's inline metadata, or null if it is not inline. */
    InlineTable inlineTable(int id) {
      return tblInlineMetadata.get(id);
    }

    /**
     * The metadata location of inline table {@code id} at its version, as a client's load names
     * it ({@code LogCatalog.metadataLocation}); the metadata log of its next version records it.
     */
    String inlineLocation(int id) {
      return inlineLocation(id, tableVersion(id));
    }

    /** {@link #inlineLocation(int)} at version {@code version} of the table. */
    String inlineLocation(int id, int version) {
      TblEntry entry = tableById.get(id);
      TableIdentifier ident = entry != null ? tableKeyOrNull(entry) : null;
      return "inline://" + ident + "#" + id + "." + version;
    }

    /** Codec tag for the bytes returned by {@link #inlineMetadata(int)}. */
    public byte inlineMetadataCodec(int id) {
      InlineTable table = tblInlineMetadata.get(id);
      Preconditions.checkState(
          table != null, "Not an inline table or missing codec tag: %s", id);
      return table.codec();
    }

    public String manifestListPrefix(int id) {
      return tblManifestPrefix.get(id);
    }

    public Builder setNamespaceProperty(int namespaceId, String key, String value) {
      nsProperties.computeIfAbsent(namespaceId, k -> new HashMap<>()).put(key, value);
      return this;
    }

    public Builder removeNamespaceProperty(int namespaceId, String key) {
      Map<String, String> props = nsProperties.get(namespaceId);
      if (props != null) {
        props.remove(key);
      }
      return this;
    }

    public Integer namespaceId(Namespace ns) {
      return namespaceLookup.get(ns);
    }

    public int namespaceVersion(int nsId) {
      NsEntry entry = namespaceById.get(nsId);
      return entry != null ? entry.version : -1;
    }

    public NsEntry namespaceEntry(int nsId) {
      return namespaceById.get(nsId);
    }

    public int nextNamespaceId() {
      return nextNamespaceId;
    }

    public int nextTableId() {
      return nextTableId;
    }

    public Integer tableId(TableIdentifier ident) {
      return tableLookup.get(ident);
    }

    public int tableVersion(int tblId) {
      TblEntry entry = tableById.get(tblId);
      return entry != null ? entry.version : -1;
    }

    public TblEntry tableEntry(int tblId) {
      return tableById.get(tblId);
    }

    public boolean namespaceExists(Namespace ns) {
      return namespaceLookup.containsKey(ns);
    }

    /** Sets the catalog's conflict encoding ({@link CatalogState#conflictEncodingName}). */
    public Builder setConflictEncoding(String name) {
      this.conflictEncoding = name;
      return this;
    }

    public String conflictEncodingName() {
      return conflictEncoding;
    }

    /** The catalog's conflict encoding: the one its records set, else the Lamport clock. */
    ConflictEncoding<Object> conflictEncoding() {
      return ConflictEncodings.byName(
          conflictEncoding != null ? conflictEncoding : LamportEncoding.NAME);
    }

    /**
     * The encoding's state of table {@code tblId}, which the encoding may change: its codec's empty
     * state if the encoding has not observed the table, which is kept from then on.
     */
    EncodingState conflictState(int tblId) {
      EncodingState.Codec codec = conflictEncoding().stateCodec();
      if (codec == EncodingState.Codec.NONE) {
        return EncodingState.NONE;
      }
      return conflictStates.computeIfAbsent(tblId, id -> codec.empty());
    }

    /**
     * The encoding's state of table {@code tblId}, without keeping it: for a judgment, which does
     * not change the state.
     */
    EncodingState conflictStateOrEmpty(int tblId) {
      EncodingState state = conflictStates.get(tblId);
      return state != null ? state : conflictEncoding().stateCodec().empty();
    }

    /** Sets the encoding's state of table {@code tblId}, as a checkpoint holds it. */
    Builder setConflictState(int tblId, EncodingState state) {
      conflictStates.put(tblId, state);
      return this;
    }

    public boolean tableExists(TableIdentifier ident) {
      return tableLookup.containsKey(ident);
    }

    private Namespace buildNamespace(int id) {
      if (id == 0) {
        return Namespace.empty();
      }
      NsEntry entry = namespaceById.get(id);
      if (entry == null) {
        throw new IllegalStateException("Unknown namespace ID: " + id);
      }
      Namespace parent = buildNamespace(entry.parentId);
      if (parent.isEmpty()) {
        return Namespace.of(entry.name);
      }
      String[] levels = new String[parent.length() + 1];
      System.arraycopy(parent.levels(), 0, levels, 0, parent.length());
      levels[parent.length()] = entry.name;
      return Namespace.of(levels);
    }

    @Override
    public CatalogState build() {
      // Ensure all lookups are complete before building immutable file
      rebuildLookups();
      return new CatalogState(this);
    }
  }
}
