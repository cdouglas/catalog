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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.intentlog.wire.ProtoWire;

/**
 * A table's sealed snapshot blocks (docs/SNAPSHOT_PAGING.md, "The root and the pages"): the
 * entry the root keeps of each ({@link SealedBlock}: its live ids, the Merkle root of its
 * snapshots and the digest of its page), and the page that holds its snapshots, their manifest
 * lists and the pool entries those name (catalog.proto SnapshotPage). A page is a function of the
 * block's snapshots, lists and entries, so every replica that seals a block makes the same bytes,
 * under the same name. Replay never reads a page.
 */
final class SnapshotPages {
  private SnapshotPages() {}

  /** Bytes of a digest and of a Merkle hash: SHA-256, truncated, as the committed set's. */
  static final int DIGEST_BYTES = 16;

  /** The page layout this build reads (SnapshotPage.min_reader_version). */
  static final int READER_VERSION = 1;

  // SnapshotPage field numbers
  static final int PAGE_MIN_READER_VERSION = 1;
  static final int PAGE_TABLE_UUID = 2;
  static final int PAGE_TABLE_ID = 3;
  static final int PAGE_BLOCK = 4;
  static final int PAGE_SNAPSHOTS = 5;
  static final int PAGE_POOL = 6;
  static final int PAGE_LISTS = 7;

  // SnapshotManifestRefs field numbers, as ProtoCodec's (a page's lists name its pool by index)
  private static final int SMR_SNAPSHOT_ID = 1;
  private static final int SMR_ORDINALS = 2;
  private static final int SMR_BASE = 3;
  private static final int SMR_REMOVED = 4;
  private static final int SMR_ADDED = 5;

  /** A sealed block as the root keeps it. Immutable but for its pending page's bytes. */
  static final class SealedBlock {
    final long block;
    final byte[] digest;
    final byte[] merkleRoot;
    /** How many snapshots it sealed: the leaves under its Merkle root. */
    final int leaves;
    /** The ids of its snapshots that have not expired, ascending. */
    final long[] liveIds;
    /** The page, if this replica sealed the block and no compactor has stored it yet. */
    final Pending pending;

    SealedBlock(
        long block, byte[] digest, byte[] merkleRoot, int leaves, long[] liveIds,
        Pending pending) {
      Preconditions.checkArgument(digest.length == DIGEST_BYTES, "Invalid digest");
      Preconditions.checkArgument(merkleRoot.length == DIGEST_BYTES, "Invalid Merkle root");
      Preconditions.checkArgument(liveIds.length <= leaves, "More live ids than leaves");
      this.block = block;
      this.digest = digest;
      this.merkleRoot = merkleRoot;
      this.leaves = leaves;
      this.liveIds = liveIds;
      this.pending = pending;
    }

    /** The page's name in the log's page store. */
    String pageName(int tableId) {
      return SnapshotPages.pageName(tableId, block, digest);
    }

    boolean isLive(long snapshotId) {
      return Arrays.binarySearch(liveIds, snapshotId) >= 0;
    }

    /** This block without {@code snapshotId} among its live ids (it expired, or came back). */
    SealedBlock without(long snapshotId) {
      int at = Arrays.binarySearch(liveIds, snapshotId);
      if (at < 0) {
        return this;
      }
      long[] kept = new long[liveIds.length - 1];
      System.arraycopy(liveIds, 0, kept, 0, at);
      System.arraycopy(liveIds, at + 1, kept, at, liveIds.length - at - 1);
      return new SealedBlock(block, digest, merkleRoot, leaves, kept, pending);
    }

    /** As the root encodes it: whether {@code other} names the same page and live ids. */
    boolean sameEntry(SealedBlock other) {
      return block == other.block && Arrays.equals(digest, other.digest)
          && Arrays.equals(merkleRoot, other.merkleRoot) && leaves == other.leaves
          && Arrays.equals(liveIds, other.liveIds);
    }
  }

  /** A page a replica sealed, held until a compactor stores it; then its bytes are dropped. */
  static final class Pending {
    private volatile byte[] bytes;

    Pending(byte[] bytes) {
      this.bytes = bytes;
    }

    /** The page's bytes, or null once stored. */
    byte[] bytes() {
      return bytes;
    }

    void stored() {
      this.bytes = null;
    }
  }

  /** A page, decoded. */
  static final class Page {
    final String tableUuid;
    final int tableId;
    final long block;
    /** Its snapshots, in the table's order. */
    final List<Snapshot> snapshots;
    /** Each snapshot's manifest list, if it has an inline one. */
    final Map<Long, List<ManifestFile>> lists;

    // Its snapshots as a load holds them, made once
    private List<Snapshot> loaded;

    Page(String tableUuid, int tableId, long block, List<Snapshot> snapshots,
        Map<Long, List<ManifestFile>> lists) {
      this.tableUuid = tableUuid;
      this.tableId = tableId;
      this.block = block;
      this.snapshots = snapshots;
      this.lists = lists;
    }

    /**
     * Its snapshots as a load holds them: each with a list as an InlineSnapshot holding
     * GenericManifestFile copies of its entries (Iceberg compares manifests by path), one copy
     * per entry; the others as they are.
     */
    synchronized List<Snapshot> loaded() {
      if (loaded == null) {
        Map<ManifestFile, ManifestFile> copies = new java.util.IdentityHashMap<>();
        List<Snapshot> made = new ArrayList<>(snapshots.size());
        for (Snapshot s : snapshots) {
          List<ManifestFile> list = lists.get(s.snapshotId());
          if (list == null) {
            made.add(s);
            continue;
          }
          List<ManifestFile> manifests = new ArrayList<>(list.size());
          for (ManifestFile entry : list) {
            manifests.add(copies.computeIfAbsent(
                entry, m -> org.apache.iceberg.GenericManifestFile.copyOf(m).build()));
          }
          made.add(new org.apache.iceberg.InlineSnapshot(
              s.sequenceNumber(), s.snapshotId(), s.parentId(), s.timestampMillis(),
              s.operation(), s.summary(), s.schemaId(), s.firstRowId(), s.addedRows(), s.keyId(),
              manifests));
        }
        loaded = List.copyOf(made);
      }
      return loaded;
    }
  }

  /**
   * Reads and decodes the page of {@code block}, sealed block of table {@code tableId}: the bytes
   * this replica sealed it with, if no compactor has stored them yet (until the next root, a
   * page exists only in the replicas that replayed its seal), else from {@code store}. A page the
   * store lacks is never taken for empty: its block's snapshots are live, and a load needs them.
   *
   * @throws IllegalStateException if the store holds no such page
   */
  static Page read(
      org.intentlog.PageStore store, int tableId, SealedBlock block, String prefix) {
    String name = block.pageName(tableId);
    byte[] bytes = block.pending != null ? block.pending.bytes() : null;
    if (bytes == null) {
      Preconditions.checkState(store != null, "No page store holds page %s", name);
      bytes = store.get(name).orElseThrow(() -> new IllegalStateException(
          "Page " + name + " of a sealed block with live snapshots does not exist"));
    }
    return decode(name, tableId, block.block, bytes, prefix);
  }

  /** {@code snap.<table id>.<block>.<digest>}: no committed-set page parses so (pageTop). */
  static String pageName(int tableId, long block, byte[] digest) {
    return "snap." + tableId + "." + block + "." + java.util.HexFormat.of().formatHex(digest);
  }

  /** The block a sequence number falls in: blocks of {@code block} sequence numbers. */
  static long blockOf(long sequenceNumber, int block) {
    return sequenceNumber / block;
  }

  /**
   * The highest block sealed at a table's last sequence number {@code last}: block k seals once
   * (k + 1) * block + window <= last. -1 if none.
   */
  static long sealedThrough(long last, int block, int window) {
    long behind = last - window;
    return behind < block ? -1L : behind / block - 1L;
  }

  /**
   * Seals block {@code block} of table {@code tableId}: {@code snapshots}, in the table's order,
   * each with its list in {@code lists} (paths, if it has an inline one) of entries in {@code
   * pool}, whose paths are under {@code prefix}. The entry holds the page, pending.
   */
  static SealedBlock seal(
      String tableUuid,
      int tableId,
      long block,
      List<Snapshot> snapshots,
      Map<Long, List<String>> lists,
      Map<String, ManifestFile> pool,
      String prefix) {
    // The page's pool: each entry a list names, once, in the order first named
    Map<String, Integer> index = new LinkedHashMap<>();
    for (Snapshot snapshot : snapshots) {
      List<String> list = lists.get(snapshot.snapshotId());
      if (list != null) {
        for (String path : list) {
          Preconditions.checkState(pool.containsKey(path),
              "Table %s lists a manifest it has not pooled: %s", tableId, path);
          index.putIfAbsent(path, index.size());
        }
      }
    }
    try {
      ByteArrayOutputStream out = new BytesOut();
      ProtoWire.writeVarint(out, PAGE_MIN_READER_VERSION, READER_VERSION);
      ProtoWire.writeString(out, PAGE_TABLE_UUID, tableUuid);
      ProtoWire.writeVarint(out, PAGE_TABLE_ID, tableId);
      ProtoWire.writeVarint64(out, PAGE_BLOCK, block);
      ProtoWire.writeBytes(out, PAGE_SNAPSHOTS,
          InlineMetadataIO.gzip(StructuralInlineMetadataCodec.encodeSnapshotsBlock(snapshots)));
      for (String path : index.keySet()) {
        ProtoWire.writeLengthDelimited(
            out, PAGE_POOL, ProtoCodec.encodeManifestFileEntry(pool.get(path), prefix));
      }
      // Lists in page order, each whole or as a change of its parent's in the page
      Map<Long, Integer> at = new HashMap<>();
      List<int[]> encoded = new ArrayList<>();
      int[] scratch = new int[index.size()];
      int position = 0;
      for (Snapshot snapshot : snapshots) {
        List<String> list = lists.get(snapshot.snapshotId());
        if (list == null) {
          continue;
        }
        int[] listed = new int[list.size()];
        for (int i = 0; i < listed.length; i++) {
          listed[i] = index.get(list.get(i));
        }
        Integer base = snapshot.parentId() != null ? at.get(snapshot.parentId()) : null;
        ManifestLists.Change change = null;
        if (base != null) {
          change = ManifestLists.diff(encoded.get(base), listed, scratch);
          if (change.cost() >= listed.length) {
            change = null;
          }
        }
        ProtoWire.writeLengthDelimited(out, PAGE_LISTS, change != null
            ? encodeChange(snapshot.snapshotId(), position - base, change, listed)
            : encodeWhole(snapshot.snapshotId(), listed));
        at.put(snapshot.snapshotId(), position);
        encoded.add(listed);
        position++;
      }
      byte[] page = out.toByteArray();
      long[] live = snapshots.stream().mapToLong(Snapshot::snapshotId).sorted().toArray();
      List<byte[]> leaves = new ArrayList<>(snapshots.size());
      for (Snapshot snapshot : snapshots) {
        leaves.add(leafHash(leaf(snapshot, resolve(lists.get(snapshot.snapshotId()), pool), prefix)));
      }
      return new SealedBlock(
          block, digest(page), merkleRoot(leaves), snapshots.size(), live, new Pending(page));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static List<ManifestFile> resolve(List<String> list, Map<String, ManifestFile> pool) {
    if (list == null) {
      return null;
    }
    List<ManifestFile> manifests = new ArrayList<>(list.size());
    for (String path : list) {
      manifests.add(pool.get(path));
    }
    return manifests;
  }

  private static byte[] encodeWhole(long snapshotId, int[] indices) throws IOException {
    ByteArrayOutputStream out = new BytesOut();
    ProtoWire.writeFixed64(out, SMR_SNAPSHOT_ID, snapshotId);
    if (indices.length > 0) {
      ProtoWire.writeLengthDelimited(out, SMR_ORDINALS, packed(indices));
    }
    return out.toByteArray();
  }

  private static byte[] encodeChange(
      long snapshotId, int base, ManifestLists.Change change, int[] indices) throws IOException {
    ByteArrayOutputStream out = new BytesOut();
    ProtoWire.writeFixed64(out, SMR_SNAPSHOT_ID, snapshotId);
    ProtoWire.writeVarint(out, SMR_BASE, base);
    if (change.removed.length > 0) {
      ProtoWire.writeLengthDelimited(out, SMR_REMOVED, packed(change.removed));
    }
    if (change.added.length > 0) {
      int[] pairs = new int[2 * change.added.length];
      for (int i = 0; i < change.added.length; i++) {
        pairs[2 * i] = change.added[i];
        pairs[2 * i + 1] = indices[change.added[i]];
      }
      ProtoWire.writeLengthDelimited(out, SMR_ADDED, packed(pairs));
    }
    return out.toByteArray();
  }

  private static byte[] packed(int[] values) throws IOException {
    ByteArrayOutputStream out = new BytesOut();
    for (int value : values) {
      ProtoWire.writeRawVarint(out, value);
    }
    return out.toByteArray();
  }

  /**
   * The page {@code bytes}, decoded, its paths under {@code prefix}; checks that it is the page
   * {@code name} names: its digest, its table and its block.
   *
   * @throws IllegalStateException if it is not
   */
  static Page decode(String name, int tableId, long block, byte[] bytes, String prefix) {
    Preconditions.checkState(pageName(tableId, block, digest(bytes)).equals(name),
        "Page %s holds other bytes (table %s, block %s)", name, tableId, block);
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(bytes);
      String uuid = null;
      int table = -1;
      long pageBlock = -1;
      List<Snapshot> snapshots = List.of();
      List<ManifestFile> pool = new ArrayList<>();
      List<byte[]> lists = new ArrayList<>();
      while (in.available() > 0) {
        int tag = ProtoWire.readVarint(in);
        switch (tag >>> 3) {
          case PAGE_MIN_READER_VERSION:
            int version = ProtoWire.readVarint(in);
            Preconditions.checkState(version <= READER_VERSION,
                "Page %s needs reader version %s; this build reads %s", name, version,
                READER_VERSION);
            break;
          case PAGE_TABLE_UUID:
            uuid = ProtoWire.readString(in);
            break;
          case PAGE_TABLE_ID:
            table = ProtoWire.readVarint(in);
            break;
          case PAGE_BLOCK:
            pageBlock = ProtoWire.readVarint64(in);
            break;
          case PAGE_SNAPSHOTS:
            snapshots = StructuralInlineMetadataCodec.decodeSnapshotsBlock(
                InlineMetadataIO.gunzip(ProtoWire.readLengthDelimitedBytes(in)));
            break;
          case PAGE_POOL:
            pool.add(ProtoCodec.decodeManifestFileEntry(
                ProtoWire.readLengthDelimitedBytes(in), prefix));
            break;
          case PAGE_LISTS:
            lists.add(ProtoWire.readLengthDelimitedBytes(in));
            break;
          default:
            ProtoWire.skipField(in, tag & 0x7);
        }
      }
      Preconditions.checkState(table == tableId && pageBlock == block,
          "Page %s is of table %s block %s", name, table, pageBlock);
      Map<Long, List<ManifestFile>> decoded = new HashMap<>();
      List<int[]> before = new ArrayList<>();
      for (byte[] list : lists) {
        decodeList(name, list, pool, before, decoded);
      }
      return new Page(uuid, table, pageBlock, snapshots, decoded);
    } catch (IOException e) {
      throw new UncheckedIOException("Page " + name + " does not decode", e);
    }
  }

  private static void decodeList(String name, byte[] bytes, List<ManifestFile> pool,
      List<int[]> before, Map<Long, List<ManifestFile>> decoded) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    long snapshotId = 0;
    int base = 0;
    List<Integer> whole = new ArrayList<>();
    List<Integer> removed = new ArrayList<>();
    List<Integer> added = new ArrayList<>();
    while (in.available() > 0) {
      int tag = ProtoWire.readVarint(in);
      switch (tag >>> 3) {
        case SMR_SNAPSHOT_ID:
          snapshotId = ProtoWire.readFixed64(in);
          break;
        case SMR_ORDINALS:
          unpack(ProtoWire.readLengthDelimitedBytes(in), whole);
          break;
        case SMR_BASE:
          base = ProtoWire.readVarint(in);
          break;
        case SMR_REMOVED:
          unpack(ProtoWire.readLengthDelimitedBytes(in), removed);
          break;
        case SMR_ADDED:
          unpack(ProtoWire.readLengthDelimitedBytes(in), added);
          break;
        default:
          ProtoWire.skipField(in, tag & 0x7);
      }
    }
    int[] indices;
    if (base == 0) {
      indices = whole.stream().mapToInt(Integer::intValue).toArray();
    } else {
      Preconditions.checkState(base <= before.size() && added.size() % 2 == 0,
          "Page %s: a malformed list for snapshot %s", name, snapshotId);
      int[] baseList = before.get(before.size() - base);
      int[] removedAt = removed.stream().mapToInt(Integer::intValue).toArray();
      int[] positions = new int[added.size() / 2];
      int[] addedIndices = new int[positions.length];
      for (int i = 0; i < positions.length; i++) {
        positions[i] = added.get(2 * i);
        addedIndices[i] = added.get(2 * i + 1);
      }
      Preconditions.checkState(ManifestLists.isChange(baseList.length, removedAt, positions),
          "Page %s: a malformed list for snapshot %s", name, snapshotId);
      indices = applyIndices(baseList, removedAt, positions, addedIndices);
    }
    List<ManifestFile> manifests = new ArrayList<>(indices.length);
    for (int index : indices) {
      Preconditions.checkState(index >= 0 && index < pool.size(),
          "Page %s: snapshot %s lists entry %s of %s", name, snapshotId, index, pool.size());
      manifests.add(pool.get(index));
    }
    before.add(indices);
    decoded.put(snapshotId, List.copyOf(manifests));
  }

  private static int[] applyIndices(int[] base, int[] removed, int[] positions, int[] added) {
    int size = base.length - removed.length + positions.length;
    int[] list = new int[size];
    int nextRemoved = 0;
    int nextAdded = 0;
    int fromBase = 0;
    for (int position = 0; position < size; position++) {
      if (nextAdded < positions.length && positions[nextAdded] == position) {
        list[position] = added[nextAdded++];
        continue;
      }
      while (nextRemoved < removed.length && removed[nextRemoved] == fromBase) {
        nextRemoved++;
        fromBase++;
      }
      list[position] = base[fromBase++];
    }
    return list;
  }

  private static void unpack(byte[] bytes, List<Integer> values) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    while (in.available() > 0) {
      values.add(ProtoWire.readVarint(in));
    }
  }

  /**
   * A sealed snapshot a record brings back inline ("Bringing a sealed snapshot back"): its leaf,
   * its place among its block's leaves, and the sibling hashes up to the block's Merkle root, so
   * replay checks it against the root without reading the page.
   */
  static final class BroughtBack {
    final long snapshotId;
    final long block;
    final int index;
    final byte[] leaf;
    final List<byte[]> proof;

    BroughtBack(long snapshotId, long block, int index, byte[] leaf, List<byte[]> proof) {
      this.snapshotId = snapshotId;
      this.block = block;
      this.index = index;
      this.leaf = leaf;
      this.proof = List.copyOf(proof);
    }

    /**
     * Whether this proves its snapshot is in {@code sealed}, live: the leaf names the snapshot,
     * and hashes up to the block's root from its place.
     */
    boolean proves(SealedBlock sealed, String prefix) {
      if (sealed == null || sealed.block != block || !sealed.isLive(snapshotId)
          || !verify(leaf, index, sealed.leaves, proof, sealed.merkleRoot)) {
        return false;
      }
      try {
        return decodeLeaf(leaf, prefix).snapshot.snapshotId() == snapshotId;
      } catch (RuntimeException e) {
        return false;
      }
    }
  }

  /** What a leaf holds: a snapshot, and its list (null if it had no inline one). */
  static final class Leaf {
    final Snapshot snapshot;
    final List<ManifestFile> manifests;

    Leaf(Snapshot snapshot, List<ManifestFile> manifests) {
      this.snapshot = snapshot;
      this.manifests = manifests;
    }
  }

  /** The proof a writer sends to bring back snapshot {@code snapshotId} of {@code page}. */
  static BroughtBack bringBack(Page page, long snapshotId, String prefix) {
    List<byte[]> leaves = new ArrayList<>(page.snapshots.size());
    int index = -1;
    for (int i = 0; i < page.snapshots.size(); i++) {
      Snapshot snapshot = page.snapshots.get(i);
      byte[] leaf = leaf(snapshot, page.lists.get(snapshot.snapshotId()), prefix);
      leaves.add(leafHash(leaf));
      if (snapshot.snapshotId() == snapshotId) {
        index = i;
      }
    }
    Preconditions.checkArgument(index >= 0, "Page of block %s lacks snapshot %s", page.block,
        snapshotId);
    Snapshot snapshot = page.snapshots.get(index);
    return new BroughtBack(snapshotId, page.block, index,
        leaf(snapshot, page.lists.get(snapshotId), prefix), proof(leaves, index));
  }

  /** The snapshot and list a leaf holds, paths under {@code prefix}. */
  static Leaf decodeLeaf(byte[] leaf, String prefix) {
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(leaf);
      Snapshot snapshot = null;
      List<ManifestFile> manifests = null;
      while (in.available() > 0) {
        int tag = ProtoWire.readVarint(in);
        switch (tag >>> 3) {
          case 1:
            List<Snapshot> block = StructuralInlineMetadataCodec.decodeSnapshotsBlock(
                ProtoWire.readLengthDelimitedBytes(in));
            Preconditions.checkState(block.size() == 1, "A leaf holds %s snapshots", block.size());
            snapshot = block.get(0);
            break;
          case 2:
            manifests = new ArrayList<>(ProtoWire.readVarint(in));
            break;
          case 3:
            Preconditions.checkState(manifests != null, "A leaf's entry before its count");
            manifests.add(ProtoCodec.decodeManifestFileEntry(
                ProtoWire.readLengthDelimitedBytes(in), prefix));
            break;
          default:
            ProtoWire.skipField(in, tag & 0x7);
        }
      }
      Preconditions.checkState(snapshot != null, "A leaf without a snapshot");
      return new Leaf(snapshot, manifests);
    } catch (IOException e) {
      throw new UncheckedIOException("A leaf does not decode", e);
    }
  }

  // ============================================================
  // Leaves and the Merkle tree ("Bringing a sealed snapshot back")
  // ============================================================

  /**
   * A snapshot's leaf: its canonical encoding, standalone (a block of it alone), then each entry
   * of its list resolved (none if it has no inline list), paths under {@code prefix}.
   */
  static byte[] leaf(Snapshot snapshot, List<ManifestFile> manifests, String prefix) {
    try {
      ByteArrayOutputStream out = new BytesOut();
      ProtoWire.writeBytes(out, 1, StructuralInlineMetadataCodec.encodeSnapshotsBlock(
          List.of(snapshot)));
      if (manifests != null) {
        ProtoWire.writeVarint(out, 2, manifests.size());
        for (ManifestFile manifest : manifests) {
          ProtoWire.writeLengthDelimited(out, 3, ProtoCodec.encodeManifestFileEntry(manifest,
              prefix));
        }
      }
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static byte[] leafHash(byte[] leaf) {
    return hash((byte) 0, leaf);
  }

  /**
   * The Merkle root of {@code leaves} (their hashes, in order): each level pairs neighbours,
   * hashing the two; an odd last node rises as it is. An empty block's root hashes nothing.
   */
  static byte[] merkleRoot(List<byte[]> leaves) {
    if (leaves.isEmpty()) {
      return hash((byte) 2, new byte[0]);
    }
    List<byte[]> level = leaves;
    while (level.size() > 1) {
      List<byte[]> up = new ArrayList<>((level.size() + 1) / 2);
      for (int i = 0; i < level.size(); i += 2) {
        up.add(i + 1 < level.size() ? node(level.get(i), level.get(i + 1)) : level.get(i));
      }
      level = up;
    }
    return level.get(0);
  }

  /** The siblings leaf {@code index} hashes with on its way to the root, bottom first. */
  static List<byte[]> proof(List<byte[]> leaves, int index) {
    List<byte[]> siblings = new ArrayList<>();
    List<byte[]> level = leaves;
    int at = index;
    while (level.size() > 1) {
      int sibling = at ^ 1;
      if (sibling < level.size()) {
        siblings.add(level.get(sibling));
      }
      List<byte[]> up = new ArrayList<>((level.size() + 1) / 2);
      for (int i = 0; i < level.size(); i += 2) {
        up.add(i + 1 < level.size() ? node(level.get(i), level.get(i + 1)) : level.get(i));
      }
      level = up;
      at /= 2;
    }
    return siblings;
  }

  /**
   * Whether {@code leaf}, the {@code index}-th of {@code count}, hashes up to {@code root} with
   * {@code proof}.
   */
  static boolean verify(byte[] leaf, int index, int count, List<byte[]> proof, byte[] root) {
    if (index < 0 || index >= count) {
      return false;
    }
    byte[] hash = leafHash(leaf);
    int at = index;
    int size = count;
    int used = 0;
    while (size > 1) {
      int sibling = at ^ 1;
      if (sibling < size) {
        if (used >= proof.size()) {
          return false;
        }
        byte[] other = proof.get(used++);
        hash = (at & 1) == 0 ? node(hash, other) : node(other, hash);
      }
      at /= 2;
      size = (size + 1) / 2;
    }
    return used == proof.size() && Arrays.equals(hash, root);
  }

  private static byte[] node(byte[] left, byte[] right) {
    byte[] both = new byte[left.length + right.length];
    System.arraycopy(left, 0, both, 0, left.length);
    System.arraycopy(right, 0, both, left.length, right.length);
    return hash((byte) 1, both);
  }

  /** SHA-256 of a domain byte and {@code bytes}, truncated. */
  private static byte[] hash(byte domain, byte[] bytes) {
    MessageDigest sha = sha256();
    sha.update(domain);
    sha.update(bytes);
    return Arrays.copyOf(sha.digest(), DIGEST_BYTES);
  }

  /** A page's digest: SHA-256 of its bytes, truncated. */
  static byte[] digest(byte[] page) {
    return Arrays.copyOf(sha256().digest(page), DIGEST_BYTES);
  }

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
