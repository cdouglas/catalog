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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.intentlog.CommittedSet;
import org.intentlog.DedupHorizonExceededException;
import org.intentlog.Replica;
import org.intentlog.fileio.AtomicFileLog;
import org.intentlog.memory.InMemorySharedLog;
import org.intentlog.wire.LogEnvelope;

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
 */
public final class CheckpointRoundTrip {
  private static final int MAX_REPORTED = 4;
  /** With {@code -Dexplorer.dump=<dir>}, each checked file is written there for check.mjs. */
  private static final String DUMP = System.getProperty("explorer.dump");
  private static final AtomicInteger DUMPED = new AtomicInteger();

  private CheckpointRoundTrip() {}

  /**
   * Fails if a catalog file written to {@code io} differs from a checkpoint of it, or a
   * compaction's checkpoint differs from a replay of the version it replaced.
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
  }

  /**
   * Fails if a checkpoint {@code log} stores is not the replay of the log through its position
   * (I11), or the log, as a catalog file, differs from a checkpoint of it.
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
   * before} differ.
   */
  public static String difference(boolean replace, byte[] before, byte[] after) {
    String difference = difference(after);
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
    dump(catalogFile);
    String violation = CatalogProto.violation(catalogFile);
    if (violation != null) {
      return "the file does not conform to catalog.proto: " + violation;
    }
    String rejected = compactionRecordRejected(catalogFile);
    if (rejected != null) {
      return rejected;
    }
    return difference(CatalogTestSupport.replay(catalogFile));
  }

  /**
   * Why replay rejects the first intention record after the checkpoint of {@code catalogFile}
   * (past the compaction's GC record) though it holds on the checkpoint, or null if it does not:
   * the record is at or below the committed set's horizon.
   */
  static String compactionRecordRejected(byte[] catalogFile) {
    CatalogState.Builder checkpoint = CatalogTestSupport.decodeCheckpoint(catalogFile);
    CommittedSet.Builder committed =
        CommittedSet.builder()
            .inherit(CatalogTestSupport.decodeCheckpointEnvelope(catalogFile).committed());
    ProtoCodec.Transaction own = null;
    for (LogEnvelope.Record record : CatalogTestSupport.decodeLogRecords(catalogFile)) {
      if (!record.isAdvanceHorizon()) {
        own = ProtoCodec.decodeTransaction(record.id(), record.intention());
        break;
      }
      committed.advanceHorizon(record.watermarkMs());
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
    } catch (DedupHorizonExceededException e) {
      // at or below the horizon: below
    }
    return String.format(
        "the record that follows the checkpoint, %s, holds on it, but replay rejects it: it is at "
            + "or below the committed set's horizon (%d)",
        own.id(),
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
