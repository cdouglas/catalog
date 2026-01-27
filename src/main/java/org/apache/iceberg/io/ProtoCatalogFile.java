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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

/**
 * Immutable snapshot of catalog state in protobuf format.
 *
 * <p>This class holds the catalog contents read from a protobuf-encoded file:
 * namespaces, tables, namespace properties, and transaction tracking state.
 */
public class ProtoCatalogFile extends CatalogFile {

  private final boolean sealed;

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

  // Committed transactions for deduplication
  private final Set<UUID> committedTransactions;

  private ProtoCatalogFile(Builder builder) {
    super(builder.catalogUuid, builder.location);
    this.sealed = builder.sealed;
    this.nextNamespaceId = builder.nextNamespaceId;
    this.nextTableId = builder.nextTableId;
    this.namespaceById = ImmutableMap.copyOf(builder.namespaceById);
    this.namespaceLookup = ImmutableMap.copyOf(builder.namespaceLookup);
    this.tableById = ImmutableMap.copyOf(builder.tableById);
    this.tableLookup = ImmutableMap.copyOf(builder.tableLookup);
    this.nsProperties = deepCopyProperties(builder.nsProperties);
    this.committedTransactions = ImmutableSet.copyOf(builder.committedTransactions);
  }

  private static Map<Integer, Map<String, String>> deepCopyProperties(
      Map<Integer, Map<String, String>> props) {
    ImmutableMap.Builder<Integer, Map<String, String>> builder = ImmutableMap.builder();
    for (Map.Entry<Integer, Map<String, String>> entry : props.entrySet()) {
      builder.put(entry.getKey(), ImmutableMap.copyOf(entry.getValue()));
    }
    return builder.build();
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

  public boolean isSealed() {
    return sealed;
  }

  public int nextNamespaceId() {
    return nextNamespaceId;
  }

  public int nextTableId() {
    return nextTableId;
  }

  public boolean containsTransaction(UUID txnId) {
    return committedTransactions.contains(txnId);
  }

  public Set<UUID> committedTransactions() {
    return committedTransactions;
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
    private boolean sealed = false;
    private int nextNamespaceId = 1;
    private int nextTableId = 1;

    private final Map<Integer, NsEntry> namespaceById = new HashMap<>();
    private final Map<Namespace, Integer> namespaceLookup = new HashMap<>();
    private final Map<Integer, TblEntry> tableById = new HashMap<>();
    private final Map<TableIdentifier, Integer> tableLookup = new HashMap<>();
    private final Map<Integer, Map<String, String>> nsProperties = new HashMap<>();
    private final Set<UUID> committedTransactions = new HashSet<>();

    Builder(InputFile location) {
      this.location = location;
      this.catalogUuid = UUID.randomUUID();
    }

    public Builder setCatalogUuid(UUID uuid) {
      this.catalogUuid = uuid;
      return this;
    }

    public Builder setSealed(boolean sealed) {
      this.sealed = sealed;
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
      namespaceLookup.put(buildNamespace(id), id);
      return this;
    }

    public Builder removeNamespace(int id) {
      NsEntry entry = namespaceById.remove(id);
      if (entry != null) {
        namespaceLookup.remove(buildNamespace(id));
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

    public Builder addTable(int id, int namespaceId, String name, int version, String metadataLocation) {
      TblEntry entry = new TblEntry(namespaceId, name, version, metadataLocation);
      tableById.put(id, entry);

      Namespace ns = buildNamespace(namespaceId);
      tableLookup.put(TableIdentifier.of(ns, name), id);
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
      committedTransactions.add(txnId);
      return this;
    }

    public boolean containsTransaction(UUID txnId) {
      return committedTransactions.contains(txnId);
    }

    public Integer namespaceId(Namespace ns) {
      return namespaceLookup.get(ns);
    }

    public int namespaceVersion(int nsId) {
      NsEntry entry = namespaceById.get(nsId);
      return entry != null ? entry.version : -1;
    }

    public Integer tableId(TableIdentifier ident) {
      return tableLookup.get(ident);
    }

    public int tableVersion(int tblId) {
      TblEntry entry = tableById.get(tblId);
      return entry != null ? entry.version : -1;
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
      return new ProtoCatalogFile(this);
    }
  }
}
