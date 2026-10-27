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

import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;

/**
 * What replay reads of an inline table's metadata: a record's witness, its landing ({@link
 * Landing}), and the encodings' judgments read these, so that replay need not build a {@link
 * TableMetadata} for each record.
 */
interface MetadataView {

  int formatVersion();

  long lastSequenceNumber();

  long nextRowId();

  long lastUpdatedMillis();

  /** The current snapshot's id (main's head), or null if the table has none. */
  Long currentSnapshotId();

  /** The ref named {@code name}, or null. */
  SnapshotRef ref(String name);

  Map<String, SnapshotRef> refs();

  /** The snapshot {@code snapshotId}, or null if the table has none by that id. */
  Snapshot snapshot(long snapshotId);

  /** The schema {@code schemaId}, or null. */
  Schema schema(int schemaId);

  /** The partition spec {@code specId}, or null. */
  PartitionSpec spec(int specId);

  Map<Integer, PartitionSpec> specsById();

  /** The sort order {@code orderId}, or null. */
  SortOrder sortOrder(int orderId);

  /** {@code metadata} as a view. */
  static MetadataView of(TableMetadata metadata) {
    return new OfMetadata(metadata);
  }

  /** A view of a {@link TableMetadata}. */
  final class OfMetadata implements MetadataView {
    private final TableMetadata metadata;

    private OfMetadata(TableMetadata metadata) {
      this.metadata = metadata;
    }

    @Override
    public int formatVersion() {
      return metadata.formatVersion();
    }

    @Override
    public long lastSequenceNumber() {
      return metadata.lastSequenceNumber();
    }

    @Override
    public long nextRowId() {
      return metadata.nextRowId();
    }

    @Override
    public long lastUpdatedMillis() {
      return metadata.lastUpdatedMillis();
    }

    @Override
    public Long currentSnapshotId() {
      Snapshot current = metadata.currentSnapshot();
      return current != null ? current.snapshotId() : null;
    }

    @Override
    public SnapshotRef ref(String name) {
      return metadata.ref(name);
    }

    @Override
    public Map<String, SnapshotRef> refs() {
      return metadata.refs();
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
      return metadata.snapshot(snapshotId);
    }

    @Override
    public Schema schema(int schemaId) {
      return metadata.schemasById().get(schemaId);
    }

    @Override
    public PartitionSpec spec(int specId) {
      return metadata.spec(specId);
    }

    @Override
    public Map<Integer, PartitionSpec> specsById() {
      return metadata.specsById();
    }

    @Override
    public SortOrder sortOrder(int orderId) {
      return metadata.sortOrdersById().get(orderId);
    }
  }
}
