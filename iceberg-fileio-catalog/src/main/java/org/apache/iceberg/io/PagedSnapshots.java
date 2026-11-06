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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.util.SerializableSupplier;

/**
 * A paged table's snapshots as a copy of the table carries them to a worker
 * (org.apache.iceberg.ShippableSnapshots; docs/SNAPSHOT_PAGING.md, "Loads"): its inline snapshots
 * and their lists, encoded as a page, and where each sealed block's page is, which the worker
 * reads through the table's FileIO when something asks for the history. A block the driver
 * sealed that no compactor has stored yet goes with its page's bytes. Only data and a FileIO, so
 * Java serialization and Kryo both copy it.
 */
final class PagedSnapshots implements SerializableSupplier<List<Snapshot>> {
  private final int tableId;
  private final String prefix;
  private final byte[] inline;
  private final long[] blocks;
  private final String[] names;
  // Each block's page as a file, or (a block not stored yet) its bytes
  private final String[] locations;
  private final byte[][] pending;
  private final long[][] liveIds;
  private final FileIO io;
  private transient volatile List<Snapshot> made;

  private PagedSnapshots(
      int tableId, String prefix, byte[] inline, long[] blocks, String[] names,
      String[] locations, byte[][] pending, long[][] liveIds, FileIO io) {
    this.tableId = tableId;
    this.prefix = prefix;
    this.inline = inline;
    this.blocks = blocks;
    this.names = names;
    this.locations = locations;
    this.pending = pending;
    this.liveIds = liveIds;
    this.io = io;
  }

  /**
   * The shipped form of table {@code tableId}'s snapshots: {@code inlineSnapshots} with their
   * {@code lists} of {@code pool}'s entries, and the {@code sealed} blocks' pages, as {@code
   * store} places them for {@code io}; null if the store's pages are not files.
   */
  static PagedSnapshots of(
      String uuid, int tableId, String prefix, List<Snapshot> inlineSnapshots,
      Map<Long, List<String>> lists, Map<String, ManifestFile> pool,
      Map<Long, SnapshotPages.SealedBlock> sealed, org.intentlog.PageStore store, FileIO io) {
    int count = sealed.size();
    long[] blocks = new long[count];
    String[] names = new String[count];
    String[] locations = new String[count];
    byte[][] pending = new byte[count][];
    long[][] live = new long[count][];
    int i = 0;
    for (SnapshotPages.SealedBlock block : sealed.values()) {
      blocks[i] = block.block;
      names[i] = block.pageName(tableId);
      pending[i] = block.pending != null ? block.pending.bytes() : null;
      if (pending[i] == null) {
        locations[i] = store != null ? store.location(names[i]) : null;
        if (locations[i] == null) {
          return null;
        }
      }
      live[i] = block.liveIds;
      i++;
    }
    byte[] page = SnapshotPages.seal(
            uuid, tableId, 0, -1L, inlineSnapshots, lists, pool, prefix)
        .pending.bytes();
    return new PagedSnapshots(tableId, prefix, page, blocks, names, locations, pending, live, io);
  }

  @Override
  public List<Snapshot> get() {
    List<Snapshot> snapshots = made;
    if (snapshots == null) {
      synchronized (this) {
        snapshots = made;
        if (snapshots == null) {
          snapshots = make();
          made = snapshots;
        }
      }
    }
    return snapshots;
  }

  private List<Snapshot> make() {
    List<Snapshot> every = new ArrayList<>(
        SnapshotPages.decode(
                SnapshotPages.pageName(tableId, 0, -1L, SnapshotPages.digest(inline)), tableId,
                -1L, inline, prefix)
            .loaded());
    for (int i = 0; i < blocks.length; i++) {
      byte[] bytes = pending[i] != null ? pending[i] : read(locations[i]);
      SnapshotPages.Page page = SnapshotPages.decode(names[i], tableId, blocks[i], bytes, prefix);
      for (Snapshot snapshot : page.loaded()) {
        if (Arrays.binarySearch(liveIds[i], snapshot.snapshotId()) >= 0) {
          every.add(snapshot);
        }
      }
    }
    every.sort(Comparator.comparingLong(Snapshot::sequenceNumber));
    return List.copyOf(every);
  }

  /** A page's bytes; a page that does not exist fails the read, never reads as empty. */
  private byte[] read(String location) {
    InputFile file = io.newInputFile(location);
    try (SeekableInputStream in = file.newStream()) {
      return in.readAllBytes();
    } catch (IOException e) {
      throw new UncheckedIOException("Reading page " + location + " failed", e);
    }
  }
}
