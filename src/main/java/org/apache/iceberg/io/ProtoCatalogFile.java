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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

/**
 * Immutable snapshot of catalog state in protobuf format.
 *
 * <p>This class holds the catalog contents read from a protobuf-encoded file:
 * namespaces, tables, namespace properties, and transaction tracking state.
 */
public class ProtoCatalogFile extends CatalogFile {

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

  // Inline table metadata: tblId -> opaque JSON bytes (TableMetadata)
  // A table ID appears in either tableById (pointer) or tblInlineMetadata (inline), not both.
  // Inline tables also appear in tableById with metadataLocation = null for lookup purposes.
  private final Map<Integer, byte[]> tblInlineMetadata;
  private final Map<Integer, String> tblManifestPrefix;

  // Inline manifest list state: per-table pool of unique ManifestFile entries keyed by path,
  // and per-snapshot references into the pool. Present only when inline.manifests is enabled.
  // tblId -> (manifest path -> ManifestFile)
  private final Map<Integer, Map<String, ManifestFile>> manifestPool;
  // tblId -> (snapshotId -> list of manifest paths in the pool)
  private final Map<Integer, Map<Long, List<String>>> snapshotManifests;

  // Committed transactions for deduplication. Stored as the raw inner
  // CommittedTransactions wire bytes (compressed UUIDv7 set). Queries scan
  // the bytes directly via ProtoCodec.committedSetContains; the canonical
  // Set<UUID> is materialized only on demand.
  private final byte[] committedTransactionsBytes;

  // Number of transaction records in the log portion of the catalog file.
  // Computed during readInternal; used by commit() to enforce max-append-count.
  private final int appendCount;

  private ProtoCatalogFile(Builder builder) {
    super(builder.catalogUuid, builder.location);
    this.nextNamespaceId = builder.nextNamespaceId;
    this.nextTableId = builder.nextTableId;
    this.namespaceById = ImmutableMap.copyOf(builder.namespaceById);
    this.namespaceLookup = ImmutableMap.copyOf(builder.namespaceLookup);
    this.tableById = ImmutableMap.copyOf(builder.tableById);
    this.tableLookup = ImmutableMap.copyOf(builder.tableLookup);
    this.nsProperties = deepCopyProperties(builder.nsProperties);
    this.tblInlineMetadata = ImmutableMap.copyOf(builder.tblInlineMetadata);
    this.tblManifestPrefix = ImmutableMap.copyOf(builder.tblManifestPrefix);
    this.manifestPool = deepCopyManifestPool(builder.manifestPool);
    this.snapshotManifests = deepCopySnapshotManifests(builder.snapshotManifests);
    this.committedTransactionsBytes = builder.materializeCommittedBytes(builder.dropBeforeTimestampMs);
    this.appendCount = builder.appendCount;
  }

  private static Map<Integer, Map<String, String>> deepCopyProperties(
      Map<Integer, Map<String, String>> props) {
    ImmutableMap.Builder<Integer, Map<String, String>> builder = ImmutableMap.builder();
    for (Map.Entry<Integer, Map<String, String>> entry : props.entrySet()) {
      builder.put(entry.getKey(), ImmutableMap.copyOf(entry.getValue()));
    }
    return builder.build();
  }

  private static Map<Integer, Map<String, ManifestFile>> deepCopyManifestPool(
      Map<Integer, Map<String, ManifestFile>> pool) {
    ImmutableMap.Builder<Integer, Map<String, ManifestFile>> builder = ImmutableMap.builder();
    for (Map.Entry<Integer, Map<String, ManifestFile>> entry : pool.entrySet()) {
      builder.put(entry.getKey(), ImmutableMap.copyOf(entry.getValue()));
    }
    return builder.build();
  }

  private static Map<Integer, Map<Long, List<String>>> deepCopySnapshotManifests(
      Map<Integer, Map<Long, List<String>>> refs) {
    ImmutableMap.Builder<Integer, Map<Long, List<String>>> outer = ImmutableMap.builder();
    for (Map.Entry<Integer, Map<Long, List<String>>> entry : refs.entrySet()) {
      ImmutableMap.Builder<Long, List<String>> inner = ImmutableMap.builder();
      for (Map.Entry<Long, List<String>> snap : entry.getValue().entrySet()) {
        inner.put(snap.getKey(), ImmutableList.copyOf(snap.getValue()));
      }
      outer.put(entry.getKey(), inner.build());
    }
    return outer.build();
  }

  public static ProtoCatalogFile empty(InputFile location) {
    return builder(location).build();
  }

  public static Builder builder(InputFile location) {
    return new Builder(location);
  }

  // ============================================================
  // CatalogFile implementation
  // ============================================================

  @Override
  public boolean createsHierarchicalNamespaces() {
    return true;
  }

  @Override
  public String location(TableIdentifier table) {
    Integer id = tableLookup.get(table);
    if (id == null) {
      return null;
    }
    TblEntry entry = tableById.get(id);
    return entry != null ? entry.metadataLocation : null;
  }

  @Override
  public Set<Namespace> namespaces() {
    return namespaceLookup.keySet();
  }

  @Override
  public boolean containsNamespace(Namespace ns) {
    return namespaceLookup.containsKey(ns);
  }

  @Override
  public Map<String, String> namespaceProperties(Namespace ns) {
    Integer id = namespaceLookup.get(ns);
    if (id == null) {
      return ImmutableMap.of();
    }
    Map<String, String> props = nsProperties.get(id);
    return props != null ? props : ImmutableMap.of();
  }

  @Override
  public List<TableIdentifier> tables() {
    return new ArrayList<>(tableLookup.keySet());
  }

  @Override
  public boolean containsTable(TableIdentifier table) {
    return tableLookup.containsKey(table);
  }

  @Override
  public boolean isInlineTable(TableIdentifier table) {
    Integer id = tableLookup.get(table);
    return id != null && tblInlineMetadata.containsKey(id);
  }

  @Override
  public byte[] inlineMetadata(TableIdentifier table) {
    Integer id = tableLookup.get(table);
    return id != null ? tblInlineMetadata.get(id) : null;
  }

  @Override
  Map<Namespace, Map<String, String>> namespaceProperties() {
    ImmutableMap.Builder<Namespace, Map<String, String>> builder = ImmutableMap.builder();
    for (Map.Entry<Integer, Map<String, String>> entry : nsProperties.entrySet()) {
      Namespace ns = namespaceForId(entry.getKey());
      if (ns != null) {
        builder.put(ns, entry.getValue());
      }
    }
    return builder.build();
  }

  @Override
  Map<TableIdentifier, String> locations() {
    ImmutableMap.Builder<TableIdentifier, String> builder = ImmutableMap.builder();
    for (Map.Entry<TableIdentifier, Integer> entry : tableLookup.entrySet()) {
      TblEntry tbl = tableById.get(entry.getValue());
      if (tbl != null) {
        builder.put(entry.getKey(), tbl.metadataLocation);
      }
    }
    return builder.build();
  }

  private Namespace namespaceForId(int id) {
    for (Map.Entry<Namespace, Integer> entry : namespaceLookup.entrySet()) {
      if (entry.getValue() == id) {
        return entry.getKey();
      }
    }
    return null;
  }

  // ============================================================
  // Proto-specific accessors
  // ============================================================

  public int nextNamespaceId() {
    return nextNamespaceId;
  }

  public int nextTableId() {
    return nextTableId;
  }

  /**
   * Returns whether {@code txnId} is in the committed-transaction dedup set.
   *
   * <p>Tri-state semantic, with the third state surfaced as an exception:
   * <ul>
   *   <li>{@code true} — UUID found in the set; definitively committed.</li>
   *   <li>{@code false} — UUID not found AND its v7 timestamp is greater than
   *       {@link #highestDroppedTimestampMs()}. Definitively not committed
   *       (presumed abort): no record exists, and we know we would still have
   *       it if it had committed (newer than the GC horizon).</li>
   *   <li><em>throws {@link DedupHorizonExceededException}</em> — UUID not
   *       found AND its v7 timestamp is at or below the GC horizon. The
   *       catalog can no longer distinguish "committed and aged out" from
   *       "never committed." Callers should fall back to the snapshot-oracle
   *       pattern (see {@code docs/GC.md} §"Two oracles") or treat as a
   *       hard error.</li>
   * </ul>
   *
   * <p>For non-v7 UUIDs (the dedup set only stores v7), returns {@code false}
   * without consulting the horizon.
   */
  public boolean containsTransaction(UUID txnId) {
    if (ProtoCodec.committedSetContains(committedTransactionsBytes, txnId)) {
      return true;
    }
    if (UuidV7.isV7(txnId)) {
      long horizon = ProtoCodec.committedSetHorizon(committedTransactionsBytes);
      if (horizon > 0L && UuidV7.timestampMs(txnId) <= horizon) {
        throw new DedupHorizonExceededException(txnId, horizon);
      }
    }
    return false;
  }

  /**
   * The {@code highest_dropped_timestamp_ms} from the committed-transaction
   * set: the largest v7 timestamp ever GC'd from this catalog. Queries for
   * UUIDs at or below this watermark are UNKNOWN. Returns 0 when no GC pass
   * has fired yet.
   */
  public long highestDroppedTimestampMs() {
    return ProtoCodec.committedSetHorizon(committedTransactionsBytes);
  }

  /** Raw inner-message bytes of the committed-txn set. Empty array iff the set is empty. */
  public byte[] committedTransactionsBytes() {
    return committedTransactionsBytes;
  }

  /**
   * Materializes the committed-txn set on demand. Allocates a fresh
   * {@code Set<UUID>} each call by decoding the compressed bytes; intended
   * for tests and tooling, not query-hot paths. Use {@link #containsTransaction}
   * for membership checks.
   */
  public Set<UUID> committedTransactions() {
    if (committedTransactionsBytes.length == 0) {
      return ImmutableSet.of();
    }
    try {
      return ImmutableSet.copyOf(
          ProtoCodec.decodeCommittedTransactions(committedTransactionsBytes));
    } catch (IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
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

  /** Returns inline metadata bytes for the given table ID, or null if not inline. */
  public byte[] inlineMetadata(int tblId) {
    return tblInlineMetadata.get(tblId);
  }

  /** Returns the manifest list prefix for the given inline table, or null. */
  public String manifestListPrefix(int tblId) {
    return tblManifestPrefix.get(tblId);
  }

  /** Returns true if the table is stored inline (metadata in catalog, not external file). */
  public boolean isInlineTable(int tblId) {
    return tblInlineMetadata.containsKey(tblId);
  }

  /** Returns the number of transaction records in the log portion of the catalog file. */
  public int appendCount() {
    return appendCount;
  }

  Map<Integer, byte[]> allInlineMetadata() {
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

  /** Returns the full manifest pool map for checkpoint encoding. */
  Map<Integer, Map<String, ManifestFile>> allManifestPools() {
    return manifestPool;
  }

  /** Returns the full snapshot manifest refs map for checkpoint encoding. */
  Map<Integer, Map<Long, List<String>>> allSnapshotManifests() {
    return snapshotManifests;
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

  public static class Builder {
    private final InputFile location;
    private UUID catalogUuid;
    private int nextNamespaceId = 1;
    private int nextTableId = 1;

    private final Map<Integer, NsEntry> namespaceById = new HashMap<>();
    private final Map<Namespace, Integer> namespaceLookup = new HashMap<>();
    private final Map<Integer, TblEntry> tableById = new HashMap<>();
    private final Map<TableIdentifier, Integer> tableLookup = new HashMap<>();
    private final Map<Integer, Map<String, String>> nsProperties = new HashMap<>();
    private final Map<Integer, byte[]> tblInlineMetadata = new HashMap<>();
    private final Map<Integer, String> tblManifestPrefix = new HashMap<>();
    private final Map<Integer, Map<String, ManifestFile>> manifestPool = new HashMap<>();
    private final Map<Integer, Map<Long, List<String>>> snapshotManifests = new HashMap<>();
    // Committed-txn dedup state. originalCommittedBytes is the inner
    // CommittedTransactions message body inherited from a decoded checkpoint
    // (empty if built from scratch). addedCommittedTransactions accumulates
    // UUIDs added via addCommittedTransaction during the lifetime of this
    // builder. build() merges them.
    private byte[] originalCommittedBytes = new byte[0];
    private final Set<UUID> addedCommittedTransactions = new HashSet<>();
    private int appendCount = 0;
    // GC watermark passed via build(long); 0 means "no GC pass on this build".
    // Set by Mut prior to invoking build during a CAS / compaction commit.
    private long dropBeforeTimestampMs = 0L;

    Builder(InputFile location) {
      this.location = location;
      this.catalogUuid = UuidV7.newUuidV7();
    }

    public Builder setCatalogUuid(UUID uuid) {
      this.catalogUuid = uuid;
      return this;
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
        namespaceLookup.put(buildNamespace(id), id);
      }
      tableLookup.clear();
      for (Map.Entry<Integer, TblEntry> entry : tableById.entrySet()) {
        Namespace ns = buildNamespace(entry.getValue().namespaceId);
        tableLookup.put(TableIdentifier.of(ns, entry.getValue().name), entry.getKey());
      }
      return this;
    }

    public Builder removeNamespace(int id) {
      // Build the Namespace key before removing from namespaceById,
      // since buildNamespace needs the entry to reconstruct the path.
      Namespace ns = namespaceById.containsKey(id) ? buildNamespace(id) : null;
      NsEntry entry = namespaceById.remove(id);
      if (entry != null) {
        namespaceLookup.remove(ns);
        nsProperties.remove(id);
      }
      return this;
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
     * whose preconditions captured the pre-bump version fail {@code verify}. No-op on
     * the root namespace (id 0), which has no explicit entry.
     */
    public Builder bumpNamespaceVersion(int id) {
      if (id == 0) {
        return this;
      }
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

    public Builder removeTable(int id) {
      TblEntry entry = tableById.remove(id);
      if (entry != null) {
        Namespace ns = buildNamespace(entry.namespaceId);
        tableLookup.remove(TableIdentifier.of(ns, entry.name));
      }
      return this;
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
     * snapshotManifests) is id-keyed and intentionally untouched. See errata.md
     * §D5.
     */
    public Builder renameTable(int id, int newNamespaceId, String newName, int newVersion) {
      TblEntry old = tableById.get(id);
      if (old == null) {
        return this;
      }
      Namespace oldNs = buildNamespace(old.namespaceId);
      tableLookup.remove(TableIdentifier.of(oldNs, old.name));
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
      // Add to tableById for lookup (null location marks it as inline)
      TblEntry entry = new TblEntry(namespaceId, name, version, null);
      tableById.put(id, entry);
      Namespace ns = buildNamespace(namespaceId);
      tableLookup.put(TableIdentifier.of(ns, name), id);
      // Store inline-specific data
      tblInlineMetadata.put(id, metadata);
      tblManifestPrefix.put(id, manifestListPrefix);
      // Same monotonicity argument as addTable: keep nextTableId ahead of any id
      // that arrived via transaction replay. Without this, a fresh checkpoint
      // (post-CAS) carries nextTableId from before the inline table was created;
      // the next Mut allocates the same id again and the second
      // createTableInline overwrites the first in tableById / tblInlineMetadata.
      // testListTables, testRenameTableDestinationTableAlreadyExists, and the
      // append+TM transaction tests' NoSuchTable errors all trace back to this.
      if (id >= nextTableId) {
        nextTableId = id + 1;
      }
      return this;
    }

    /** Updates inline metadata and version without clearing the manifest pool. */
    public Builder updateInlineMetadata(int id, int newVersion, byte[] metadata) {
      tblInlineMetadata.put(id, metadata);
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
      return this;
    }

    /** Adds a ManifestFile to the table's pool. */
    public Builder addManifestToPool(int tblId, ManifestFile manifest) {
      manifestPool.computeIfAbsent(tblId, k -> new LinkedHashMap<>())
          .put(manifest.path(), manifest);
      return this;
    }

    /** Sets the manifest list for a snapshot (list of manifest paths in the pool). */
    public Builder setSnapshotManifests(int tblId, long snapshotId, List<String> paths) {
      snapshotManifests.computeIfAbsent(tblId, k -> new HashMap<>())
          .put(snapshotId, new ArrayList<>(paths));
      return this;
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
     * Drops a snapshot's manifest ref list and GCs any pool entries no longer
     * referenced. Called from RemoveSnapshotsUpdate replay to prevent unbounded
     * pool growth after snapshot expiration. See ML_INLINE_DESIGN_NOTES.md Gap 1.
     */
    public Builder removeSnapshotManifests(int tblId, long snapshotId) {
      Map<Long, List<String>> refs = snapshotManifests.get(tblId);
      if (refs == null) {
        return this;
      }
      List<String> dropped = refs.remove(snapshotId);
      if (dropped == null || dropped.isEmpty()) {
        return this;
      }
      // Collect paths still referenced by any remaining snapshot
      Set<String> stillReferenced = new HashSet<>();
      for (List<String> snapRefs : refs.values()) {
        stillReferenced.addAll(snapRefs);
      }
      Map<String, ManifestFile> pool = manifestPool.get(tblId);
      if (pool != null) {
        for (String path : dropped) {
          if (!stillReferenced.contains(path)) {
            pool.remove(path);
          }
        }
      }
      return this;
    }

    public boolean isInlineTable(int id) {
      return tblInlineMetadata.containsKey(id);
    }

    public byte[] inlineMetadata(int id) {
      return tblInlineMetadata.get(id);
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

    public Builder addCommittedTransaction(UUID txnId) {
      Preconditions.checkArgument(UuidV7.isV7(txnId),
          "Committed transaction IDs must be UUIDv7; got %s", txnId);
      addedCommittedTransactions.add(txnId);
      return this;
    }

    /**
     * Bulk-load entrypoint: install the inner CommittedTransactions message
     * body inherited from a decoded checkpoint, without materializing the
     * canonical Set. Subsequent {@link #addCommittedTransaction} calls layer
     * additions on top; build-time merge happens in {@link #materializeCommittedBytes}.
     */
    public Builder setCommittedTransactionsBytes(byte[] bytes) {
      this.originalCommittedBytes = bytes != null ? bytes : new byte[0];
      return this;
    }

    /**
     * Tri-state membership check, mirroring
     * {@link ProtoCatalogFile#containsTransaction}: returns {@code true} if
     * found among in-flight additions or in the inherited bytes; returns
     * {@code false} for a v7 UUID whose timestamp is above the GC horizon;
     * throws {@link DedupHorizonExceededException} when the UUID is missing
     * and at or below the horizon.
     *
     * <p>Replay-path note: the just-appended txn UUID is minted at append
     * time (minutes ago at most) while the watermark is hours back, so the
     * throw branch is unreachable in normal operation. If a malformed /
     * skewed writer manages to append a UUID below the watermark, the
     * exception surfaces the bug rather than silently mis-deduping.
     */
    public boolean containsTransaction(UUID txnId) {
      if (addedCommittedTransactions.contains(txnId)) {
        return true;
      }
      if (ProtoCodec.committedSetContains(originalCommittedBytes, txnId)) {
        return true;
      }
      if (UuidV7.isV7(txnId)) {
        long horizon = ProtoCodec.committedSetHorizon(originalCommittedBytes);
        if (horizon > 0L && UuidV7.timestampMs(txnId) <= horizon) {
          throw new DedupHorizonExceededException(txnId, horizon);
        }
      }
      return false;
    }

    /**
     * Produces the final inner CommittedTransactions wire bytes for the
     * resulting ProtoCatalogFile. Single-pass merge of inherited entries +
     * additions, with optional GC drop of inherited entries whose v7 timestamp
     * is &lt;= {@code dropBeforeTimestampMs}. {@code 0} disables the GC pass
     * (pass-through when there are no additions).
     */
    byte[] materializeCommittedBytes(long dropBeforeTimestampMs) {
      if (addedCommittedTransactions.isEmpty() && dropBeforeTimestampMs == 0L) {
        return originalCommittedBytes;
      }
      return ProtoCodec.rewriteCommittedBytes(
          originalCommittedBytes, addedCommittedTransactions, dropBeforeTimestampMs);
    }

    public Builder setAppendCount(int count) {
      this.appendCount = count;
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

    public ProtoCatalogFile build() {
      return build(0L);
    }

    /**
     * Builds a ProtoCatalogFile, performing a GC pass on the inherited
     * committed-set when {@code dropBeforeTimestampMs > 0}: any inherited
     * UUIDv7 with {@code timestamp_ms <= dropBeforeTimestampMs} is dropped
     * and contributes to {@code highest_dropped_timestamp_ms}. Additions
     * via {@link #addCommittedTransaction} are kept unconditionally.
     *
     * <p>Callers (notably the CAS / compaction path in
     * {@link ProtoCatalogFormat.Mut}) compute {@code dropBeforeTimestampMs}
     * as {@code now - committedRetentionMs}. The no-arg {@link #build()}
     * variant disables the GC pass and is used for read-only rebuilds (decode
     * round-trips, tests).
     */
    public ProtoCatalogFile build(long dropBeforeTimestampMs) {
      Preconditions.checkArgument(dropBeforeTimestampMs >= 0L,
          "dropBeforeTimestampMs must be >= 0, got %s", dropBeforeTimestampMs);
      this.dropBeforeTimestampMs = dropBeforeTimestampMs;
      // Ensure all lookups are complete before building immutable file
      rebuildLookups();
      return new ProtoCatalogFile(this);
    }
  }
}
