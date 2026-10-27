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

import java.util.List;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.TableMetadata;

/**
 * A table as replay has it at one point: its version, and its metadata, an inline table's decoded
 * when first asked for. Its version and metadata are fixed when the view is taken; its manifest
 * lists are read from the replaying state, so they are the view's only while that state is
 * unchanged (a view taken before an apply has no lists after it).
 */
final class ReplayView {

  private final CatalogState.Builder builder;
  private final int tableId;
  private final int version;
  private final InlineTable inline;
  private final String location;
  private String inlineLocation;

  private ReplayView(CatalogState.Builder builder, int tableId) {
    this.builder = builder;
    this.tableId = tableId;
    CatalogState.TblEntry entry = builder.tableEntry(tableId);
    this.version = entry != null ? entry.version : -1;
    this.location = entry != null ? entry.metadataLocation : null;
    this.inline = builder.inlineTable(tableId);
  }

  /** Table {@code tableId} as {@code builder} holds it now. */
  static ReplayView of(CatalogState.Builder builder, int tableId) {
    return new ReplayView(builder, tableId);
  }

  int tableId() {
    return tableId;
  }

  /** Whether the table exists. A dropped table's id is never used again. */
  boolean exists() {
    return version >= 0;
  }

  /** The table's version: 1 when created, bumped by every change. */
  int version() {
    return version;
  }

  /** Whether the table's metadata is inline. */
  boolean inline() {
    return inline != null;
  }

  /** A pointer table's metadata file; null for an inline table. */
  String metadataLocation() {
    return location;
  }

  /**
   * An inline table's metadata, at the location a client's load names it; null for a pointer
   * table. Builds it whole: a judgment reads {@link #table} instead.
   */
  TableMetadata metadata() {
    if (inline == null) {
      return null;
    }
    if (inlineLocation == null) {
      inlineLocation = builder.inlineLocation(tableId, version);
    }
    return inline.metadata(inlineLocation);
  }

  /** What replay reads of an inline table's metadata; null for a pointer table. */
  MetadataView table() {
    return inline != null ? inline.fields() : null;
  }

  /** Whether the table has an inline manifest list for {@code snapshotId}. */
  boolean hasInlineList(long snapshotId) {
    return builder.snapshotManifestKeys(tableId).contains(snapshotId);
  }

  /** The paths of {@code snapshotId}'s inline manifest list, in order; empty if it has none. */
  List<String> listPaths(long snapshotId) {
    return builder.snapshotManifestPaths(tableId, snapshotId);
  }

  /** The manifest the table's lists name by {@code path}, or null. */
  ManifestFile manifest(String path) {
    return builder.manifestPool(tableId).get(path);
  }

  @Override
  public String toString() {
    return "table " + tableId + "@" + version + (inline() ? " inline" : " " + location);
  }
}
