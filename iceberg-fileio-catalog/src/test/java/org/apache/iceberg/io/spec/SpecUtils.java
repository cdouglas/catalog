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
package org.apache.iceberg.io.spec;

import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.ProtoCatalogFile;

/**
 * Shared test utilities for the {@code org.apache.iceberg.io.spec} property tests.
 *
 * <p>Lives in this package so that multiple property-test classes can reuse
 * helpers without duplicating code.
 */
final class SpecUtils {

  private SpecUtils() {}

  /**
   * Reconstructs a {@link ProtoCatalogFile.Builder} from an immutable
   * {@link ProtoCatalogFile} snapshot using only public accessors.
   *
   * <p>This is the canonical bridge between the immutable snapshot world
   * ({@code CatalogFile}) and the mutable builder world
   * ({@code ProtoCatalogFile.Builder}) that {@code Action.verify} /
   * {@code Action.apply} operate on.  It mirrors what the production decoder
   * does after loading a checkpoint and replaying the transaction log, just
   * without the I/O layer.
   *
   * <p>Proxy note: {@code namespaceById()} and {@code tableById()} are
   * package-private in {@code org.apache.iceberg.io}. Since this class is in
   * {@code org.apache.iceberg.io.spec}, we iterate via the public
   * {@code namespaces()} / {@code tables()} sets and resolve IDs, entries, and
   * properties through public accessors.
   */
  static ProtoCatalogFile.Builder stateToBuilder(ProtoCatalogFile pcf) {
    ProtoCatalogFile.Builder builder =
        ProtoCatalogFile.builder(CatalogStateGenerators.STUB_LOCATION)
            .setCatalogUuid(pcf.uuid())
            .setNextNamespaceId(pcf.nextNamespaceId())
            .setNextTableId(pcf.nextTableId());

    // The root namespace row (id 0) is not listed by namespaces(); carry its
    // version so top-level creates verify against the right value.
    builder.addNamespace(0, 0, "", pcf.namespaceVersion(0));

    // Namespaces
    for (Namespace ns : pcf.namespaces()) {
      Integer nsId = pcf.namespaceId(ns);
      if (nsId == null) continue;
      ProtoCatalogFile.NsEntry entry = pcf.namespaceEntry(nsId);
      if (entry == null) continue;
      builder.addNamespace(nsId, entry.parentId, entry.name, entry.version);

      Map<String, String> props = pcf.namespaceProperties(ns);
      if (props != null) {
        for (Map.Entry<String, String> prop : props.entrySet()) {
          builder.setNamespaceProperty(nsId, prop.getKey(), prop.getValue());
        }
      }
    }

    // Tables
    for (TableIdentifier tbl : pcf.tables()) {
      Integer tblId = pcf.tableId(tbl);
      if (tblId == null) continue;
      ProtoCatalogFile.TblEntry entry = pcf.tableEntry(tblId);
      if (entry == null) continue;
      if (pcf.isInlineTable(tblId)) {
        byte[] meta = pcf.inlineMetadata(tblId);
        byte codec = pcf.inlineMetadataCodec(tblId);
        String prefix = pcf.manifestListPrefix(tblId);
        builder.addInlineTable(
            tblId, entry.namespaceId, entry.name, entry.version, meta, codec,
            prefix != null ? prefix : "");
      } else {
        builder.addTable(
            tblId, entry.namespaceId, entry.name, entry.version,
            entry.metadataLocation);
      }
    }

    // Committed transactions (raw bytes; re-installed as original bytes)
    builder.setCommittedTransactionsBytes(pcf.committedTransactionsBytes());

    // Rebuild lookups after bulk-loading
    builder.rebuildLookups();
    return builder;
  }
}
