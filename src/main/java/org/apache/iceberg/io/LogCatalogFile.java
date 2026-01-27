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
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * Immutable snapshot of catalog state for the log-based catalog format.
 *
 * <p>This class represents the complete state of the catalog at a given point in time, including:
 *
 * <ul>
 *   <li>Namespaces and their properties
 *   <li>Tables and their metadata locations
 *   <li>Internal versioning for optimistic concurrency control
 *   <li>Committed transaction IDs for deduplication
 * </ul>
 */
public class LogCatalogFile extends CatalogFile {
  final int nextNsid;
  final int nextTblid;
  final boolean sealed;

  private final Set<UUID> committedTxn;

  final Map<Namespace, Integer> nsids;
  final Map<Integer, Integer> nsVersion;
  private final Map<Integer, Namespace> nsLookup;

  private final Map<Integer, Map<String, String>> nsProperties;

  final Map<TableIdentifier, Integer> tblIds;
  final Map<Integer, Integer> tblVersion;
  final Map<Integer, String> tblLocations;

  // empty LogCatalogFile
  LogCatalogFile(InputFile location) {
    super(location);
    this.nextNsid = 1;
    this.nextTblid = 1;
    this.sealed = false;
    this.nsids = Maps.newHashMap();
    this.nsVersion = Maps.newHashMap();
    this.nsProperties = Maps.newHashMap();
    this.tblIds = Maps.newHashMap();
    this.tblVersion = Maps.newHashMap();
    this.tblLocations = Maps.newHashMap();
    this.nsLookup = Maps.newHashMap();
    this.committedTxn = Sets.newHashSet();
    this.nsids.put(Namespace.empty(), 0);
    this.nsVersion.put(0, 1);
    this.nsLookup.put(0, Namespace.empty());
  }

  LogCatalogFile(
      InputFile location,
      UUID catalogUUID,
      int nextNsid,
      int nextTblid,
      boolean sealed,
      Map<Namespace, Integer> nsids,
      Map<Integer, Integer> nsVersion,
      Map<Integer, Map<String, String>> nsProperties,
      Map<TableIdentifier, Integer> tblIds,
      Map<Integer, Integer> tblVersion,
      Map<Integer, String> tblLocations,
      Set<UUID> committedTxn) {
    super(catalogUUID, location);
    this.sealed = sealed;
    this.nextNsid = nextNsid;
    this.nextTblid = nextTblid;
    this.nsids = nsids;
    this.nsVersion = nsVersion;
    this.nsProperties = nsProperties;
    this.tblIds = tblIds;
    this.tblVersion = tblVersion;
    this.tblLocations = tblLocations;
    this.committedTxn = committedTxn;
    this.nsLookup =
        nsids.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    if (!nsids.containsKey(Namespace.empty())) {
      throw new IllegalStateException("Missing root/empty namespace");
    }
  }

  // TODO move this to format (why does this here?)
  @Override
  public boolean createsHierarchicalNamespaces() {
    return true;
  }

  public boolean containsTransaction(UUID txnId) {
    return committedTxn.contains(txnId);
  }

  public int getNextNsid() {
    return nextNsid;
  }

  public int getNextTblid() {
    return nextTblid;
  }

  public Set<UUID> getCommittedTransactions() {
    return Collections.unmodifiableSet(committedTxn);
  }

  /** Returns the namespace lookup map (nsid -> Namespace). Package-private for Mut access. */
  Map<Integer, Namespace> getNsLookup() {
    return nsLookup;
  }

  @Override
  public String location(TableIdentifier table) {
    final Integer tblId = tblIds.get(table);
    if (tblId == null) {
      return null;
    }
    return tblLocations.get(tblId);
  }

  @Override
  public Set<Namespace> namespaces() {
    return Collections.unmodifiableSet(nsids.keySet());
  }

  @Override
  public boolean containsNamespace(Namespace namespace) {
    return nsids.containsKey(namespace);
  }

  @Override
  public Map<String, String> namespaceProperties(Namespace namespace) {
    final Integer nsid = nsids.get(namespace);
    if (nsid == null) {
      return null;
    }
    return Collections.unmodifiableMap(nsProperties.getOrDefault(nsid, Collections.emptyMap()));
  }

  @Override
  public List<TableIdentifier> tables() {
    return Lists.newArrayList(tblIds.keySet().iterator());
  }

  @Override
  Map<Namespace, Map<String, String>> namespaceProperties() {
    return nsids.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e -> nsProperties.getOrDefault(e.getValue(), Collections.emptyMap())));
  }

  @Override
  Map<TableIdentifier, String> locations() {
    return tblIds.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> tblLocations.get(e.getValue())));
  }

  // emit stream of actions that recreate this catalog, NOT including Checkpoint action
  List<LogCatalogFormat.LogAction> checkpointStream() {
    List<LogCatalogFormat.LogAction> actions = Lists.newArrayList();
    // TODO regions
    // TODO ensure properties of deleted namespaces are removed
    // sort by nsid; sufficient for parentId, since namespaces never move, are created in order
    for (Map.Entry<Namespace, Integer> e :
        nsids.entrySet().stream()
            .sorted(Comparator.comparingInt(Map.Entry::getValue))
            .collect(Collectors.toList())) {
      final Namespace ns = e.getKey();
      final int nsid = e.getValue();
      final int version = nsVersion.get(nsid);
      final int levels = ns.length();
      final Namespace parent =
          levels > 1
              ? Namespace.of(Arrays.copyOfRange(ns.levels(), 0, levels - 1))
              : Namespace.empty();
      final int parentId = nsids.get(parent);
      actions.add(
          new LogCatalogFormat.LogAction.CreateNamespace(
              0 == levels ? "" : ns.level(levels - 1),
              nsid,
              version,
              parentId,
              nsVersion.get(parentId)));
    }
    for (Map.Entry<Integer, Map<String, String>> e : nsProperties.entrySet()) {
      final int nsid = e.getKey();
      for (Map.Entry<String, String> prop : e.getValue().entrySet()) {
        actions.add(
            new LogCatalogFormat.LogAction.AddNamespaceProperty(
                nsid, prop.getKey(), prop.getValue()));
      }
    }
    for (Map.Entry<TableIdentifier, Integer> e : tblIds.entrySet()) {
      final TableIdentifier ti = e.getKey();
      final int tblId = e.getValue();
      final int version = tblVersion.get(tblId);
      final int nsid = nsids.get(ti.namespace());
      actions.add(
          new LogCatalogFormat.LogAction.CreateTable(
              ti.name(), tblId, version, nsid, nsVersion.get(nsid), tblLocations.get(tblId)));
    }
    return actions;
  }

  void writeCheckpoint(OutputStream out) throws IOException {
    LogSerializer.writeFullCheckpoint(this, out);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LogCatalogFile that = (LogCatalogFile) o;
    return nextNsid == that.nextNsid
        && nextTblid == that.nextTblid
        && Objects.equals(uuid(), that.uuid())
        && Objects.equals(nsids, that.nsids)
        && Objects.equals(nsVersion, that.nsVersion)
        && Objects.equals(nsProperties, that.nsProperties)
        && Objects.equals(tblIds, that.tblIds)
        && Objects.equals(tblVersion, that.tblVersion)
        && Objects.equals(tblLocations, that.tblLocations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        uuid(),
        nextNsid,
        nextTblid,
        nsids,
        nsVersion,
        nsProperties,
        tblIds,
        tblVersion,
        tblLocations);
  }

  @Override
  public String toString() {
    return "LogCatalogFile{"
        + "catalogUUID="
        + uuid()
        + ", nextNsid="
        + nextNsid
        + ", nextTblid="
        + nextTblid
        + ", nsids="
        + nsids
        + ", nsVersion="
        + nsVersion
        + ", nsProperties="
        + nsProperties
        + ", tblIds="
        + tblIds
        + ", tblVersion="
        + tblVersion
        + ", tblLocations="
        + tblLocations
        + '}';
  }
}
