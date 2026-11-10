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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.zip.CRC32;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.intentlog.CommittedSet;
import org.intentlog.DedupHorizonExceededException;
import org.intentlog.PagedOutException;
import org.intentlog.Replica;
import org.intentlog.fileio.AtomicFileLog;
import org.intentlog.memory.InMemorySharedLog;
import org.intentlog.wire.CommittedSetCodec;
import org.intentlog.wire.LogEnvelope;
import org.intentlog.wire.PageRef;
import org.intentlog.wire.UuidV7;

/**
 * The checkpoint round trip: the state a reader replays from a catalog file is the state it
 * decodes from a checkpoint of that state. And a compaction's checkpoint is the state a reader
 * replays from the version it replaced, committed set included (I11): a GC is a record that
 * follows the checkpoint, not a filter on it, so every checkpoint is a function of the log. Compaction writes such a checkpoint, so a difference is
 * a change the next compaction makes. Bug 27 was one: a table with both a metadata file and inline
 * metadata read as the file until a checkpoint kept only the inline metadata.
 *
 * <p>The comparison covers everything a checkpoint holds, by id: namespaces, their properties,
 * tables with their metadata locations, inline metadata and its codec, manifest list prefixes,
 * manifest pools and each snapshot's references into them, the name lookups, the id counters and
 * the committed-transaction set. Only the log's record count differs by design.
 *
 * <p>A catalog file is also checked against the compaction that wrote its checkpoint: the first
 * intention record after the checkpoint (past a GC record) is that commit's own, and if it holds
 * on the checkpoint, replay applies it. A compaction keeps its GC record's watermark below its
 * own record; Bug 64 was a compaction whose horizon reached its record, so replay rejected the
 * commit it had just written.
 *
 * <p>And the pages the newest catalog file's committed set references are in its page store, as
 * the set says ({@link #pagesProblem}): replay never reads a page, so nothing else sees one that a
 * sweep deleted while in use (spec/tla/README.md, "The oracles, with paging").
 */
public final class CheckpointRoundTrip {
  private static final int MAX_REPORTED = 4;
  /** With {@code -Dexplorer.dump=<dir>}, each checked file is written there for check.mjs. */
  private static final String DUMP = System.getProperty("explorer.dump");
  private static final AtomicInteger DUMPED = new AtomicInteger();

  private CheckpointRoundTrip() {}

  /**
   * Fails if a catalog file written to {@code io} differs from a checkpoint of it, or a
   * compaction's checkpoint differs from a replay of the version it replaced, or the newest
   * references a page its page store lacks ({@link #assertPagesExist}).
   */
  public static void assertEveryWrite(InMemoryAtomicFileIO io) {
    List<InMemoryAtomicFileIO.Write> history = io.history();
    for (int i = 0; i < history.size(); i++) {
      InMemoryAtomicFileIO.Write write = history.get(i);
      String difference = difference(write);
      if (difference != null) {
        throw new AssertionError(
            String.format(
                "a checkpoint of write #%d (%s) to %s differs from it: %s",
                i, write.strategy(), write.location(), difference));
      }
    }
    assertPagesExist(io);
  }

  /**
   * Fails if the newest version of a catalog file in {@code io} references a page that its page
   * store ({@code <location>.committed/}) lacks, or that is not as the reference says ({@link
   * #pagesProblem}). Only the newest: a GC record drops a page from the set before its compactor
   * deletes it, so older versions may name pages that are rightly gone.
   */
  public static void assertPagesExist(InMemoryAtomicFileIO io) {
    for (String location : io.conditionalWrites().keySet()) {
      byte[] file = io.bytes(location);
      if (file == null) {
        continue;
      }
      String problem = pagesProblem(CatalogTestSupport.replay(file),
          name -> io.bytes(location + ".committed/" + name));
      if (problem != null) {
        throw new AssertionError("the committed set of " + location + ": " + problem);
      }
      String snapshots = snapshotPagesProblem(CatalogTestSupport.decodeCheckpoint(file).build(),
          CatalogTestSupport.replay(file).state(), name -> io.bytes(location + ".committed/" + name));
      if (snapshots != null) {
        throw new AssertionError("the root of " + location + ": " + snapshots);
      }
    }
  }

  /**
   * Why the snapshot pages the root {@code state} (a checkpoint's) names, and the log replayed
   * through its tail ({@code replayed}) still names, are not as they say, or null if they are
   * (SNAPSHOT_PAGING.md, "Testing"; TLA+ StatePagesExist): each such sealed block's page exists
   * ({@code pages} by name: a compactor puts a root's pages before the root), decodes as the page
   * its name names, of the block's table and block, holds the block's live snapshots, and its
   * leaves hash to the block's Merkle root. A page a record after the root dropped is garbage
   * once that record is in the log, which a reader replays before it loads.
   */
  static String snapshotPagesProblem(
      CatalogState state, CatalogState replayed, Function<String, byte[]> pages) {
    java.util.Set<String> named = new java.util.HashSet<>();
    replayed.allSealedBlocks().forEach((tblId, blocks) ->
        blocks.values().forEach(block -> named.add(block.pageName(tblId))));
    for (Map.Entry<Integer, java.util.SortedMap<Long, SnapshotPages.SealedBlock>> table :
        state.allSealedBlocks().entrySet()) {
      int tblId = table.getKey();
      String prefix = state.allManifestPrefixes().getOrDefault(tblId, "");
      for (SnapshotPages.SealedBlock block : table.getValue().values()) {
        String name = block.pageName(tblId);
        if (!named.contains(name)) {
          continue;
        }
        byte[] body = pages.apply(name);
        if (body == null) {
          return "page " + name + ", which the root names, does not exist";
        }
        SnapshotPages.Page page;
        try {
          page = SnapshotPages.decode(name, tblId, block.block, body, prefix);
        } catch (RuntimeException e) {
          return "page " + name + " does not decode as its name says: " + e.getMessage();
        }
        java.util.Set<Long> held = new java.util.HashSet<>();
        List<byte[]> leaves = new ArrayList<>();
        for (org.apache.iceberg.Snapshot snapshot : page.snapshots) {
          held.add(snapshot.snapshotId());
          leaves.add(SnapshotPages.leafHash(SnapshotPages.leaf(
              snapshot, page.lists.get(snapshot.snapshotId()), prefix)));
        }
        for (long live : block.liveIds) {
          if (!held.contains(live)) {
            return "page " + name + " lacks live snapshot " + live;
          }
        }
        if (leaves.size() != block.leaves
            || !java.util.Arrays.equals(SnapshotPages.merkleRoot(leaves), block.merkleRoot)) {
          return "page " + name + "'s snapshots do not hash to its block's Merkle root";
        }
      }
    }
    return null;
  }

  /**
   * Why the pages the committed set of {@code replica} references are not as it says, or null if
   * they are: each exists ({@code pages} by name; TLA+ PagesExist), belongs to the replica's log,
   * holds the block its reference names (its node, count and digest), and holds only ids above the
   * floor it was sealed at (the page before it's top, or the horizon) and at or below its top, in
   * (H, F] (the shape of PagesExact, which needs the history to check whole).
   */
  static String pagesProblem(Replica<CatalogState> replica, Function<String, byte[]> pages) {
    CommittedSet set = replica.committed();
    long below = set.horizon();
    for (PageRef ref : set.pages()) {
      String name = ref.name();
      if (ref.top() <= below || ref.top() > set.floor()) {
        return String.format("page %s ends outside (%d, %d], after the pages before it and at or"
            + " below the floor", name, below, set.floor());
      }
      byte[] body = pages.apply(name);
      if (body == null) {
        return "page " + name + ", which the committed set references, does not exist";
      }
      LogEnvelope.Page page = LogEnvelope.decodePage(body);
      if (!page.logId().equals(replica.logId()) || page.level() != ref.level()
          || page.index() != ref.index() || !ref.hasDigest(page.digest())) {
        return "page " + name + " does not hold the block its reference names";
      }
      List<UUID> entries =
          CommittedSetCodec.decodeEntries(ref.level(), ref.index(), page.packedEntries());
      if (entries.size() != ref.count()) {
        return String.format("page %s holds %d ids, its reference %d", name, entries.size(),
            ref.count());
      }
      for (UUID id : entries) {
        long ts = UuidV7.timestampMs(id);
        if (ts <= below || ts > ref.top()) {
          return String.format("page %s holds %s, outside (%d, %d]", name, id, below, ref.top());
        }
      }
      below = ref.top();
    }
    return null;
  }

  /**
   * Fails if a checkpoint {@code log} stores is not the replay of the log through its position
   * (I11), or the log, as a catalog file, differs from a checkpoint of it, or references a page
   * the log's page store lacks ({@link #pagesProblem}).
   */
  public static void assertEveryCheckpoint(InMemorySharedLog log) {
    if (log.checkpoints().isEmpty()) {
      return;
    }
    List<String> violations = new ArrayList<>(log.violations());
    byte[] first = log.checkpoints().get(0L);
    List<byte[]> records = log.records();
    String whole = difference(AtomicFileLog.encode(first, records));
    if (whole != null) {
      violations.add("the log: " + whole);
    }
    Map<String, byte[]> stored = log.storedPages();
    String pages = pagesProblem(
        CatalogTestSupport.replay(AtomicFileLog.encode(first, records)), stored::get);
    if (pages != null) {
      violations.add("the log's committed set: " + pages);
    }
    byte[] newest = new java.util.TreeMap<>(log.checkpoints()).lastEntry().getValue();
    String snapshots = snapshotPagesProblem(
        CatalogTestSupport.decodeCheckpoint(AtomicFileLog.encode(newest, List.of())).build(),
        CatalogTestSupport.replay(AtomicFileLog.encode(first, records)).state(), stored::get);
    if (snapshots != null) {
      violations.add("the newest checkpoint's root: " + snapshots);
    }
    for (Map.Entry<Long, byte[]> c : log.checkpoints().entrySet()) {
      if (c.getKey() > 0L) {
        String difference = checkpointIsReplay(
            AtomicFileLog.encode(first, records.subList(0, c.getKey().intValue())),
            AtomicFileLog.encode(c.getValue(), List.of()));
        if (difference != null) {
          violations.add("the checkpoint at position " + c.getKey() + ": " + difference);
        }
      }
    }
    if (!violations.isEmpty()) {
      throw new AssertionError("checkpoints of " + log + ": " + violations);
    }
  }

  /**
   * {@link #difference(byte[])} of the catalog file {@code write} wrote, and for a CAS, how its
   * checkpoint and a replay of the version it replaced differ ({@link #checkpointIsReplay}).
   */
  public static String difference(InMemoryAtomicFileIO.Write write) {
    return difference(write.strategy() == AtomicOutputFile.Strategy.CAS, write.before(),
        write.after());
  }

  /**
   * {@link #difference(byte[])} of the catalog file {@code after}, a write wrote over {@code
   * before}; for a compaction ({@code replace}), also how its checkpoint and a replay of {@code
   * before} differ. Only a replace's own record, written with its checkpoint, must apply where
   * it holds: an append's record may follow a compaction after a commit, whose GC record (no
   * record of its own to stay below) passed the record's id ({@link #compactionRecordRejected}).
   */
  public static String difference(boolean replace, byte[] before, byte[] after) {
    String difference = difference(after, replace);
    if (difference == null && replace) {
      difference = checkpointIsReplay(before, after);
    }
    return difference;
  }

  /**
   * How the checkpoint a CAS wrote ({@code after}) and a replay of the version it replaced
   * ({@code before}) differ, or null if they do not or the CAS created the catalog. A compaction
   * checkpoints the state it replayed of the version it replaces, and nothing else: its clock
   * shows only in the GC record that follows (I11).
   */
  public static String checkpointIsReplay(byte[] before, byte[] after) {
    if (before == null || before.length == 0) {
      return null;
    }
    SortedMap<String, String> expected = contents(CatalogTestSupport.replay(before));
    SortedMap<String, String> actual = contents(CatalogTestSupport.checkpointOf(after));
    if (expected.equals(actual)) {
      return null;
    }
    TreeSet<String> items = new TreeSet<>(expected.keySet());
    items.addAll(actual.keySet());
    List<String> differences = new ArrayList<>();
    for (String item : items) {
      if (!Objects.equals(expected.get(item), actual.get(item))) {
        differences.add(String.format(
            "%s: %s -> %s", item, expected.get(item), actual.get(item)));
      }
    }
    return "the compaction's checkpoint is not a replay of the version it replaced: "
        + String.join("; ", differences.subList(0, Math.min(differences.size(), MAX_REPORTED)));
  }

  /**
   * How the state in {@code catalogFile} and a checkpoint of it differ, or null if they do not.
   * Also checks that the file conforms to catalog.proto ({@link CatalogProto#violation}), so
   * everything a suite writes keeps the schema of record in step with the codecs.
   */
  public static String difference(byte[] catalogFile) {
    return difference(catalogFile, true);
  }

  /**
   * As {@link #difference(byte[])}; with {@code replaced} false (the file's last write was an
   * append), without asking that the first record after the checkpoint apply where it holds.
   */
  private static String difference(byte[] catalogFile, boolean replaced) {
    dump(catalogFile);
    String violation = CatalogProto.violation(catalogFile);
    if (violation != null) {
      return "the file does not conform to catalog.proto: " + violation;
    }
    String rejected = replaced ? compactionRecordRejected(catalogFile) : null;
    if (rejected != null) {
      return rejected;
    }
    Replica<CatalogState> replica = CatalogTestSupport.replay(catalogFile);
    String malformed = malformedLists(replica.state());
    if (malformed != null) {
      return malformed;
    }
    return difference(replica);
  }

  /**
   * How {@code state}'s inline manifest lists are malformed, or null if they are not: a table with
   * a pool or lists is inline; each pool entry is under its own path, and some list names it; each
   * list names manifests of its table's pool (a load skips a path the pool lacks), each once; and
   * the lists are exactly those of the table's snapshots whose manifest list is inline (an
   * {@code inline://} sentinel).
   */
  static String malformedLists(CatalogState state) {
    List<String> problems = new ArrayList<>();
    TreeSet<Integer> tables = new TreeSet<>(state.allInlineTables().keySet());
    tables.addAll(state.allManifestPools().keySet());
    tables.addAll(state.allSnapshotManifests().keySet());
    for (int id : tables) {
      Map<String, ManifestFile> pool = state.manifestPool(id);
      Map<Long, List<String>> lists = state.snapshotManifests(id);
      InlineTable inline = state.allInlineTables().get(id);
      if (inline == null) {
        problems.add(String.format("table %d has %d pool entries and %d lists but is not inline",
            id, pool.size(), lists.size()));
        continue;
      }
      pool.forEach((path, manifest) -> {
        if (!path.equals(manifest.path())) {
          problems.add(String.format("table %d pools %s under %s", id, manifest.path(), path));
        }
      });
      Set<String> named = new HashSet<>();
      lists.forEach((snapshot, paths) -> {
        Set<String> listed = new HashSet<>();
        for (String path : paths) {
          if (!pool.containsKey(path)) {
            problems.add(String.format("table %d snapshot %d lists %s, not in the pool",
                id, snapshot, path));
          }
          if (!listed.add(path)) {
            problems.add(String.format("table %d snapshot %d lists %s twice", id, snapshot, path));
          }
        }
        named.addAll(paths);
      });
      pool.forEach((path, manifest) -> {
        if (!named.contains(path)) {
          problems.add(String.format(
              "table %d pools %s (added by snapshot %s), which no list names (lists of %s)",
              id, path, manifest.snapshotId(), new TreeSet<>(lists.keySet())));
        }
      });
      Set<Long> sentinels = new HashSet<>();
      for (Snapshot snapshot : inline.fields().snapshots) {
        String list = snapshot.manifestListLocation();
        if (list != null && list.startsWith("inline://")) {
          sentinels.add(snapshot.snapshotId());
        }
      }
      if (!sentinels.equals(lists.keySet())) {
        problems.add(String.format("table %d has lists for snapshots %s, inline snapshots %s",
            id, new TreeSet<>(lists.keySet()), new TreeSet<>(sentinels)));
      }
    }
    if (problems.isEmpty()) {
      return null;
    }
    int shown = Math.min(problems.size(), MAX_REPORTED);
    return "malformed inline manifest lists: " + String.join("; ", problems.subList(0, shown))
        + (problems.size() > shown ? String.format(" (and %d more)", problems.size() - shown) : "");
  }

  /**
   * Why replay rejects the first intention record after the checkpoint of {@code catalogFile}
   * (past the compaction's GC and PageOut records) though it holds on the checkpoint, or null if it
   * does not: the record is at or below the committed set's floor (Bug 64; TLA+ pageown). Asked of
   * a file a replace wrote, whose own record that is (a compaction after a commit writes none).
   */
  static String compactionRecordRejected(byte[] catalogFile) {
    CatalogState.Builder checkpoint = CatalogTestSupport.decodeCheckpoint(catalogFile);
    CommittedSet.Builder committed =
        CommittedSet.builder()
            .inherit(CatalogTestSupport.decodeCheckpointEnvelope(catalogFile).committed());
    ProtoCodec.Transaction own = null;
    for (LogEnvelope.Record record : CatalogTestSupport.decodeLogRecords(catalogFile)) {
      if (record.isIntention()) {
        own = ProtoCodec.decodeTransaction(record.id(), record.intention());
        break;
      } else if (record.isPageOut()) {
        committed.pageOut(record.pageOut());
      } else {
        committed.advanceHorizon(record.watermarkMs());
      }
    }
    if (own == null) {
      return null;
    }
    if (committed.mayApply(own.id()) || !own.verify(checkpoint)) {
      return null;
    }
    try {
      if (committed.contains(own.id())) {
        return null; // the checkpoint holds it already
      }
    } catch (DedupHorizonExceededException | PagedOutException e) {
      // at or below the floor: below
    }
    return String.format(
        "the record that follows the checkpoint, %s, holds on it, but replay rejects it: it is at "
            + "or below the committed set's floor (%d; horizon %d)",
        own.id(),
        committed.floor(),
        committed.horizon());
  }

  private static void dump(byte[] catalogFile) {
    if (DUMP == null) {
      return;
    }
    try {
      Path dir = Paths.get(DUMP);
      Files.createDirectories(dir);
      Files.write(dir.resolve(String.format("%07d.lcat", DUMPED.incrementAndGet())), catalogFile);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * How {@code replica} (a state, with its log's id and committed set) and a checkpoint of it
   * differ, or null if they do not.
   */
  public static String difference(Replica<CatalogState> replica) {
    Replica<CatalogState> checkpoint;
    try {
      checkpoint = CatalogTestSupport.replay(CatalogTestSupport.encodeFullFile(replica));
    } catch (RuntimeException e) {
      return "a checkpoint of the state does not decode: " + e;
    }
    SortedMap<String, String> replayed = contents(replica);
    SortedMap<String, String> decoded = contents(checkpoint);
    TreeSet<String> items = new TreeSet<>(replayed.keySet());
    items.addAll(decoded.keySet());
    List<String> differences = new ArrayList<>();
    for (String item : items) {
      String before = replayed.get(item);
      String after = decoded.get(item);
      if (!Objects.equals(before, after)) {
        differences.add(String.format("%s: %s -> %s", item, before, after));
      }
    }
    if (differences.isEmpty()) {
      return null;
    }
    int shown = Math.min(differences.size(), MAX_REPORTED);
    return String.join("; ", differences.subList(0, shown))
        + (differences.size() > shown ? String.format(" (and %d more)", differences.size() - shown)
            : "");
  }

  /** Everything a checkpoint of {@code replica} holds, by item. */
  static SortedMap<String, String> contents(Replica<CatalogState> replica) {
    CatalogState file = replica.state();
    SortedMap<String, String> items = new TreeMap<>();
    items.put("catalog uuid", String.valueOf(replica.logId()));
    items.put("next ids", file.nextNamespaceId() + "/" + file.nextTableId());
    items.put("committed transactions", fingerprint(replica.committed().bytes()));
    for (Map.Entry<Integer, CatalogState.NsEntry> e : file.namespaceById().entrySet()) {
      CatalogState.NsEntry ns = e.getValue();
      items.put(
          "namespace " + e.getKey(),
          String.format("parent=%d name=%s version=%d", ns.parentId, ns.name, ns.version));
    }
    for (Map.Entry<Integer, Map<String, String>> e : file.allNamespaceProperties().entrySet()) {
      e.getValue().forEach((k, v) -> items.put("namespace " + e.getKey() + " property " + k, v));
    }
    for (Namespace ns : file.namespaces()) {
      items.put("namespace name " + ns, String.valueOf(file.namespaceId(ns)));
    }
    for (Map.Entry<Integer, CatalogState.TblEntry> e : file.tableById().entrySet()) {
      CatalogState.TblEntry t = e.getValue();
      items.put(
          "table " + e.getKey(),
          String.format(
              "namespace=%d name=%s version=%d location=%s",
              t.namespaceId, t.name, t.version, t.metadataLocation));
    }
    for (TableIdentifier t : file.tables()) {
      items.put("table name " + t, String.valueOf(file.tableId(t)));
    }
    file.allInlineTables()
        .forEach(
            (id, table) ->
                items.put(
                    "table " + id + " inline metadata",
                    String.format("codec=%s %s", table.codec(), fingerprint(table.bytes()))));
    file.allManifestPrefixes()
        .forEach((id, prefix) -> items.put("table " + id + " manifest prefix", prefix));
    file.allManifestPools()
        .forEach(
            (id, pool) ->
                pool.forEach(
                    (path, manifest) ->
                        items.put("table " + id + " manifest " + path, describe(manifest))));
    file.allPoolOrdinals()
        .forEach(
            (id, ordinals) ->
                ordinals.forEach(
                    (path, ordinal) ->
                        items.put("table " + id + " manifest " + path + " ordinal",
                            String.valueOf(ordinal))));
    file.allNextOrdinals()
        .forEach((id, next) -> items.put("table " + id + " next ordinal", String.valueOf(next)));
    file.allSnapshotManifests()
        .forEach(
            (id, refs) ->
                refs.forEach(
                    (snapshot, paths) ->
                        items.put(
                            "table " + id + " snapshot " + snapshot + " manifests",
                            paths.toString())));
    items.put("conflict encoding", String.valueOf(file.conflictEncodingName()));
    EncodingState.Codec codec = file.conflictEncoding().stateCodec();
    file.conflictStates()
        .forEach(
            (id, state) -> {
              byte[] bytes = codec.encode(state); // a state never observed is none
              if (bytes.length > 0) {
                items.put("table " + id + " conflict state", state.toString());
              }
            });
    return items;
  }

  /** Every field of a manifest list entry. */
  static String describe(ManifestFile m) {
    StringBuilder partitions = new StringBuilder("[");
    if (m.partitions() != null) {
      for (ManifestFile.PartitionFieldSummary p : m.partitions()) {
        partitions.append(
            String.format(
                "(null=%s nan=%s lower=%s upper=%s)",
                p.containsNull(), p.containsNaN(), hex(p.lowerBound()), hex(p.upperBound())));
      }
    }
    partitions.append(']');
    return String.format(
        "path=%s length=%d spec=%d content=%s seq=%d min-seq=%d snapshot=%s"
            + " files=%s/%s/%s rows=%s/%s/%s partitions=%s key=%s first-row-id=%s",
        m.path(), m.length(), m.partitionSpecId(), m.content(), m.sequenceNumber(),
        m.minSequenceNumber(), m.snapshotId(), m.addedFilesCount(), m.existingFilesCount(),
        m.deletedFilesCount(), m.addedRowsCount(), m.existingRowsCount(), m.deletedRowsCount(),
        m.partitions() == null ? null : partitions, hex(m.keyMetadata()), m.firstRowId());
  }

  private static String fingerprint(byte[] bytes) {
    CRC32 crc = new CRC32();
    crc.update(bytes);
    return String.format("%d bytes, crc %08x", bytes.length, crc.getValue());
  }

  private static String hex(ByteBuffer buffer) {
    if (buffer == null) {
      return null;
    }
    StringBuilder out = new StringBuilder();
    ByteBuffer copy = buffer.duplicate();
    while (copy.hasRemaining()) {
      out.append(String.format("%02x", copy.get()));
    }
    return out.toString();
  }
}
