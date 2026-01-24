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
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/** Snapshot of Catalog state used in FileIOCatalog. */
public abstract class CatalogFile {

  private final UUID uuid;
  private final InputFile location;

  /** Constructor for empty CatalogFile instances. Assigns a random UUID. */
  CatalogFile(InputFile location) {
    // consistent iteration order; UUIDv7
    this(UUID.randomUUID(), location);
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

  abstract Map<Namespace, Map<String, String>> namespaceProperties();

  abstract Map<TableIdentifier, String> locations();

  public abstract static class Mut<C extends CatalogFile, T extends Mut<C, T>> {

    protected final C original;
    protected final Set<TableIdentifier> readTables;
    protected final Map<TableIdentifier, String> tables;
    protected final Map<TableIdentifier, String> tableUpdates; // TODO extend this to metadata
    protected final Map<Namespace, Boolean> namespaces;
    protected final Map<Namespace, Map<String, String>> namespaceProperties;

    protected Mut(C original) {
      this.original = original;
      this.tables = Maps.newHashMap();
      this.readTables = Sets.newHashSet();
      this.tableUpdates = Maps.newHashMap();
      this.namespaces = Maps.newHashMap();
      this.namespaceProperties = Maps.newHashMap();
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
          !original.containsNamespace(ancestor);
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
        throw new IllegalArgumentException("Cannot drop non-empty namespace: " + namespace);
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
      if (original.location(table) != null || tables.get(table) != null) {
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
      if (null == original.location(table)) {
        throw new NoSuchNamespaceException("Table does not exist: %s", table);
      }
      final String newloc = tables.get(table);
      if (original.location(table) != null && tables.containsKey(table) && newloc == null) {
        throw new IllegalArgumentException("Cannot update table marked for deletion: " + table);
      }
      if (newloc != null) {
        tables.put(table, location);
      } else {
        // TODO implement w.r.t. tableID to follow table renames (currently implemented as drop/add)
        tableUpdates.put(table, location);
      }
      return self();
    }

    public T dropTable(TableIdentifier tableId) {
      if (null == original.location(tableId)) {
        throw new NoSuchTableException("Table does not exist: %s", tableId);
      }
      tables.put(tableId, null);
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
