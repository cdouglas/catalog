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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/** Snapshot of Catalog state used in FileIOCatalog. */
public abstract class CatalogFile {

  private final UUID uuid;
  private final InputFile location;

  /** Constructor for empty CatalogFile instances. Assigns a fresh UUIDv7. */
  CatalogFile(InputFile location) {
    this(UuidV7.newUuidV7(), location);
  }

  /** Construct a CatalogFile instance from an existing Catalog. */
  CatalogFile(UUID uuid, InputFile fromFile) {
    this.uuid = uuid;
    this.location = fromFile;
  }

  public UUID uuid() {
    return uuid;
  }

  public InputFile location() {
    return location;
  }

  // hack to account for recording namespaces as a hierarchy, rather than a flat list
  public abstract boolean createsHierarchicalNamespaces();

  public abstract String location(TableIdentifier table);

  public abstract Set<Namespace> namespaces();

  public abstract boolean containsNamespace(Namespace namespace);

  public abstract Map<String, String> namespaceProperties(Namespace namespace);

  public abstract List<TableIdentifier> tables();

  /** Returns true if the table exists in the catalog (pointer or inline). */
  public boolean containsTable(TableIdentifier table) {
    return location(table) != null;
  }

  /** Returns true if the table is stored inline (metadata in catalog). Default: false. */
  public boolean isInlineTable(TableIdentifier table) {
    return false;
  }

  /** Returns inline metadata bytes for the table, or null if not inline. */
  public byte[] inlineMetadata(TableIdentifier table) {
    return null;
  }

  abstract Map<Namespace, Map<String, String>> namespaceProperties();

  abstract Map<TableIdentifier, String> locations();

  public abstract static class Mut<C extends CatalogFile, T extends Mut<C, T>> {

    protected final C original;
    protected final Set<TableIdentifier> readTables;
    protected final Map<TableIdentifier, String> tables;
    protected final Map<TableIdentifier, String> tableUpdates;
    protected final Map<Namespace, Boolean> namespaces;
    protected final Map<Namespace, Map<String, String>> namespaceProperties;
    // Inline table mutations (metadata stored in catalog, not external file)
    protected final Map<TableIdentifier, byte[]> inlineTables;
    protected final Map<TableIdentifier, byte[]> inlineTableUpdates;
    protected final Map<TableIdentifier, byte[]> inlineTableDeltaUpdates;
    // Codec tags parallel to inlineTables / inlineTableUpdates. Entries default
    // to InlineMetadataCodecs.TAG_JSON_GZIP when missing (see the no-codec
    // overloads of createTableInline / updateTableInline below).
    protected final Map<TableIdentifier, Byte> inlineTableCodecs;
    protected final Map<TableIdentifier, Byte> inlineTableUpdateCodecs;
    // Pending renames: from-identifier -> to-identifier. The id stays put; only
    // the (namespace, name) pair on TblEntry changes. See errata.md §D5.
    protected final Map<TableIdentifier, TableIdentifier> tableRenames;

    protected Mut(C original) {
      this.original = original;
      this.tables = Maps.newHashMap();
      this.readTables = Sets.newHashSet();
      this.tableUpdates = Maps.newHashMap();
      this.namespaces = Maps.newHashMap();
      this.namespaceProperties = Maps.newHashMap();
      this.inlineTables = Maps.newHashMap();
      this.inlineTableUpdates = Maps.newHashMap();
      this.inlineTableDeltaUpdates = Maps.newHashMap();
      this.inlineTableCodecs = Maps.newHashMap();
      this.inlineTableUpdateCodecs = Maps.newHashMap();
      this.tableRenames = Maps.newHashMap();
    }

    @SuppressWarnings("unchecked")
    private T self() {
      return (T) this;
    }

    public T createNamespace(Namespace namespace) {
      return createNamespace(namespace, Collections.emptyMap());
    }

    public T createNamespace(Namespace namespace, Map<String, String> properties) {
      Preconditions.checkNotNull(namespace, "Namespace cannot be null");
      Preconditions.checkNotNull(properties, "Properties cannot be null");
      Preconditions.checkArgument(
          !namespace.equals(Namespace.empty()), "Cannot create empty namespace");
      if (original.containsNamespace(namespace)
          || (namespaces.containsKey(namespace) && !namespaces.get(namespace))) {
        throw new AlreadyExistsException(
            "Cannot create namespace %s. Namespace already exists", namespace);
      }
      for (Namespace ancestor = parentOf(namespace);
          !ancestor.isEmpty() && !original.containsNamespace(ancestor);
          ancestor = parentOf(ancestor)) {
        if (namespaces.containsKey(ancestor)) {
          if (!namespaces.get(ancestor)) {
            throw new IllegalStateException(
                String.format(
                    "Cannot create namespace %s. Parent namespace %s is marked for deletion",
                    namespace, ancestor));
          }
          break;
        }
        namespaces.put(ancestor, true);
      }
      namespaces.put(namespace, true);
      namespaceProperties.put(namespace, properties);
      return self();
    }

    public T updateProperties(Namespace namespace, Map<String, String> properties) {
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
      return self();
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

    public T dropNamespace(Namespace namespace) {
      Preconditions.checkArgument(
          !Namespace.empty().equals(namespace), "Cannot drop empty namespace");
      if (checkNamespaceExists(namespace)) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
      }
      final boolean noNsChild =
          original.namespaces().stream()
                  .filter(
                      ns ->
                          namespaces.getOrDefault(
                              ns, true)) // filter out children marked for deletion
                  .map(Mut::parentOf)
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
                  .noneMatch(ns -> ns.equals(namespace));
      if (!noNsChild || !noTblChild) {
        throw new NamespaceNotEmptyException("Namespace %s is not empty", namespace);
      }
      namespaces.put(namespace, false);
      namespaceProperties.remove(namespace);
      return self();
    }

    public T createTable(TableIdentifier table, String location) {
      // TODO: fix for swap (a -> b; b -> a)
      if (checkNamespaceExists(table.namespace())) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", table.namespace());
      }
      if (original.containsTable(table) || tables.get(table) != null
          || inlineTables.get(table) != null) {
        throw new AlreadyExistsException("Table already exists: %s", table);
      }
      tables.put(table, location);
      return self();
    }

    public T readTable(TableIdentifier table) {
      final String newloc = tables.get(table);
      if (original.location(table) != null && tables.containsKey(table) && newloc == null) {
        // TODO eh... this should be legal.
        throw new IllegalArgumentException(
            "Cannot include read dependency on table marked for deletion: " + table);
      }
      readTables.add(table);
      return self();
    }

    public T updateTable(TableIdentifier table, String location) {
      if (!original.containsTable(table)) {
        throw new NoSuchNamespaceException("Table does not exist: %s", table);
      }
      final String newloc = tables.get(table);
      if (original.location(table) != null && tables.containsKey(table) && newloc == null) {
        throw new IllegalArgumentException("Cannot update table marked for deletion: " + table);
      }
      if (newloc != null) {
        tables.put(table, location);
      } else {
        tableUpdates.put(table, location);
      }
      return self();
    }

    /**
     * Renames a table in place: the table id stays put, only the
     * {@code (namespace, name)} pair on {@code TblEntry} changes. Inline state
     * keyed by id (manifest pool, snapshot manifest refs, inline TM bytes)
     * follows by construction. See errata.md §D5.
     */
    public T renameTable(TableIdentifier from, TableIdentifier to) {
      Preconditions.checkNotNull(from, "Source table cannot be null");
      Preconditions.checkNotNull(to, "Destination table cannot be null");
      Preconditions.checkArgument(!from.equals(to), "Source and destination identifiers are equal");
      // Source must exist and not already be marked for delete in this Mut.
      if (!original.containsTable(from) || (tables.containsKey(from) && tables.get(from) == null)) {
        throw new NoSuchTableException("Table does not exist: %s", from);
      }
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
      return self();
    }

    public T dropTable(TableIdentifier tableId) {
      if (!original.containsTable(tableId)) {
        throw new NoSuchTableException("Table does not exist: %s", tableId);
      }
      tables.put(tableId, null);
      return self();
    }

    /**
     * Creates a table with its metadata stored inline in the catalog. Defaults
     * the codec to {@link InlineMetadataCodecs#TAG_JSON_GZIP}; use the
     * codec-aware overload to record a different codec.
     */
    public T createTableInline(TableIdentifier table, byte[] metadata) {
      return createTableInline(table, metadata, InlineMetadataCodecs.TAG_JSON_GZIP);
    }

    /**
     * Creates a table with metadata stored inline; {@code codecTag} identifies
     * the codec used to encode {@code metadata}.
     */
    public T createTableInline(TableIdentifier table, byte[] metadata, byte codecTag) {
      if (checkNamespaceExists(table.namespace())) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", table.namespace());
      }
      if (original.containsTable(table) || tables.get(table) != null
          || inlineTables.get(table) != null) {
        throw new AlreadyExistsException("Table already exists: %s", table);
      }
      inlineTables.put(table, metadata);
      inlineTableCodecs.put(table, codecTag);
      return self();
    }

    /** Updates an inline table's metadata (full replacement). */
    public T updateTableInline(TableIdentifier table, byte[] metadata) {
      return updateTableInline(table, metadata, InlineMetadataCodecs.TAG_JSON_GZIP);
    }

    /**
     * Updates an inline table's metadata (full replacement) with the given codec.
     */
    public T updateTableInline(TableIdentifier table, byte[] metadata, byte codecTag) {
      if (!original.containsTable(table)) {
        throw new NoSuchTableException("Table does not exist: %s", table);
      }
      inlineTableUpdates.put(table, metadata);
      inlineTableUpdateCodecs.put(table, codecTag);
      return self();
    }

    /** Updates an inline table via structured delta (encoded delta bytes). */
    public T updateTableInlineDelta(TableIdentifier table, byte[] deltaBytes) {
      if (!original.containsTable(table)) {
        throw new NoSuchTableException("Table does not exist: %s", table);
      }
      // Store with a marker prefix to distinguish from full mode in buildActions
      inlineTableDeltaUpdates.put(table, deltaBytes);
      return self();
    }

    private boolean checkNamespaceExists(Namespace namespace) {
      return !Namespace.empty().equals(namespace)
          && !original.containsNamespace(namespace)
          && !namespaces.getOrDefault(namespace, false);
    }

    public abstract C commit(SupportsAtomicOperations fileIO);
  }
}
