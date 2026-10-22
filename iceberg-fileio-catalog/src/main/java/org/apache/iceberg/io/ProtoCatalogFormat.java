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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.intentlog.CommitOutcomeUnknownException;
import org.intentlog.CommitRejectedException;
import org.intentlog.CompactionPolicy;
import org.intentlog.LogRuntime;
import org.intentlog.Replay;
import org.intentlog.Replica;
import org.intentlog.RuntimeConfig;
import org.intentlog.fileio.AtomicFileLog;

/**
 * Protobuf-based catalog format with checkpoint + append log structure.
 *
 * <p>File structure:
 *
 * <pre>
 * [Magic: 4 bytes "LCAT"]
 * [Version: 4 bytes, big-endian int]
 * [Checkpoint length: varint]
 * [Checkpoint: protobuf bytes, a log.proto LogCheckpoint]
 * [Record 1 length: varint][Record 1: protobuf bytes, a log.proto LogRecord]
 * [Record 2 length: varint][Record 2: protobuf bytes, a log.proto LogRecord]
 * ...
 * </pre>
 *
 * <p>The envelopes (intentlog-core's LogEnvelope) carry the catalog's checkpoint and each
 * transaction's actions (catalog.proto's Checkpoint and Intention).
 */
public class ProtoCatalogFormat
    implements CatalogFormat<CatalogState, ProtoCatalogFormat.Mut> {

  public static final byte[] MAGIC = "LCAT".getBytes(StandardCharsets.US_ASCII);
  public static final int FORMAT_VERSION = 2;
  public static final int HEADER_SIZE = 8; // magic (4) + version (4)

  /**
   * Maximum number of transaction records in the log portion before a new commit
   * must CAS (compact) instead of append. Default: 10000. Set to 0 to force CAS on
   * every commit (for storage providers that don't support conditional append).
   *
   * <p>Provider hard limits: S3 Express ~10000, Azure AppendBlob 50000.
   */
  public static final String MAX_APPEND_COUNT = "fileio.catalog.max.append.count";
  public static final int DEFAULT_MAX_APPEND_COUNT = 10_000;

  /**
   * Maximum size in bytes of the catalog file before a new commit must CAS (compact)
   * instead of append. This is a performance target; larger catalogs take longer to
   * read. Default: 16 MB.
   */
  public static final String MAX_APPEND_SIZE = "fileio.catalog.max.append.size";
  public static final long DEFAULT_MAX_APPEND_SIZE = 16L * 1024 * 1024;

  /**
   * Retention window for the committed-transaction dedup set, in milliseconds.
   * Every CAS / compaction commit writes a GC record (AdvanceHorizon) after its
   * checkpoint, which drops any UUIDv7 from {@code CommittedTransactions} whose
   * timestamp is at or below {@code now - committedRetentionMs} (and below its
   * own transaction's). The largest dropped timestamp becomes
   * {@code highest_dropped_timestamp_ms}: a
   * query for a UUID at or below it throws {@link DedupHorizonExceededException},
   * and replay rejects a record whose id is at or below it (Bug 64). Default: 6
   * hours, long enough to cover any commit's retries, short enough to bound the
   * set on long-lived catalogs. {@code Long.MAX_VALUE} disables GC. A window
   * shorter than a commit takes is legal but lossy: a writer whose id falls
   * below the horizon while it commits fails, or cannot learn whether it
   * committed (CommitStateUnknownException); {@code 0} drops everything older
   * than the compacting commit. See {@code docs/GC.md}.
   */
  public static final String COMMITTED_RETENTION_MS = "fileio.catalog.committed.retention.ms";
  public static final long DEFAULT_COMMITTED_RETENTION_MS = 6L * 60 * 60 * 1000;

  /**
   * First wait before retrying a write that storage throttled; each later wait doubles, up to 5 s.
   */
  public static final String THROTTLE_MIN_WAIT_MS = "fileio.catalog.throttle.retry.min-wait-ms";
  public static final long DEFAULT_THROTTLE_MIN_WAIT_MS = 200L;

  private final int maxAppendCount;
  private final long maxAppendSize;
  private final long committedRetentionMs;
  private final long throttleMinWaitMs;

  public ProtoCatalogFormat() {
    this(DEFAULT_MAX_APPEND_COUNT, DEFAULT_MAX_APPEND_SIZE, DEFAULT_COMMITTED_RETENTION_MS);
  }

  public ProtoCatalogFormat(Map<String, String> properties) {
    this(
        parseIntProperty(properties, MAX_APPEND_COUNT, DEFAULT_MAX_APPEND_COUNT),
        parseLongProperty(properties, MAX_APPEND_SIZE, DEFAULT_MAX_APPEND_SIZE),
        parseLongProperty(
            properties, COMMITTED_RETENTION_MS, DEFAULT_COMMITTED_RETENTION_MS),
        parseLongProperty(properties, THROTTLE_MIN_WAIT_MS, DEFAULT_THROTTLE_MIN_WAIT_MS));
  }

  public ProtoCatalogFormat(int maxAppendCount, long maxAppendSize) {
    this(maxAppendCount, maxAppendSize, DEFAULT_COMMITTED_RETENTION_MS);
  }

  public ProtoCatalogFormat(int maxAppendCount, long maxAppendSize, long committedRetentionMs) {
    this(maxAppendCount, maxAppendSize, committedRetentionMs, DEFAULT_THROTTLE_MIN_WAIT_MS);
  }

  public ProtoCatalogFormat(
      int maxAppendCount, long maxAppendSize, long committedRetentionMs, long throttleMinWaitMs) {
    Preconditions.checkArgument(maxAppendCount >= 0,
        "maxAppendCount must be >= 0, got %s", maxAppendCount);
    Preconditions.checkArgument(maxAppendSize >= 0,
        "maxAppendSize must be >= 0, got %s", maxAppendSize);
    Preconditions.checkArgument(committedRetentionMs >= 0,
        "committedRetentionMs must be >= 0, got %s", committedRetentionMs);
    Preconditions.checkArgument(throttleMinWaitMs >= 0,
        "throttleMinWaitMs must be >= 0, got %s", throttleMinWaitMs);
    this.maxAppendCount = maxAppendCount;
    this.maxAppendSize = maxAppendSize;
    this.committedRetentionMs = committedRetentionMs;
    this.throttleMinWaitMs = throttleMinWaitMs;
  }

  public int maxAppendCount() {
    return maxAppendCount;
  }

  public long maxAppendSize() {
    return maxAppendSize;
  }

  public long committedRetentionMs() {
    return committedRetentionMs;
  }

  public long throttleMinWaitMs() {
    return throttleMinWaitMs;
  }

  private static int parseIntProperty(Map<String, String> properties, String key, int dflt) {
    if (properties == null) return dflt;
    String val = properties.get(key);
    return val != null ? Integer.parseInt(val) : dflt;
  }

  private static long parseLongProperty(Map<String, String> properties, String key, long dflt) {
    if (properties == null) return dflt;
    String val = properties.get(key);
    return val != null ? Long.parseLong(val) : dflt;
  }

  @Override
  public CatalogFile.Mut<CatalogState, Mut> empty(InputFile input) {
    return new Mut(input, maxAppendCount, maxAppendSize, committedRetentionMs, throttleMinWaitMs);
  }

  @Override
  public CatalogFile.Mut<CatalogState, Mut> from(CatalogFile other) {
    if (!(other instanceof CatalogState)) {
      throw new IllegalArgumentException("Cannot convert to CatalogState: " + other);
    }
    return new Mut(
        (CatalogState) other,
        maxAppendCount,
        maxAppendSize,
        committedRetentionMs,
        throttleMinWaitMs);
  }

  @Override
  public CatalogState read(SupportsAtomicOperations fileIO, InputFile catalogLocation) {
    return CatalogState.of(
        runtime(fileIO, catalogLocation.location(), maxAppendCount, maxAppendSize,
            committedRetentionMs, throttleMinWaitMs).read());
  }

  /**
   * The intention-log runtime over the catalog file at {@code location}: the catalog's state
   * machine, replicated through one object of a store with conditional writes.
   */
  static LogRuntime<CatalogState, CatalogState.Builder, ProtoCodec.CatalogIntention>
      runtime(
          SupportsAtomicOperations fileIO, String location, int maxAppendCount,
          long maxAppendSize, long committedRetentionMs, long throttleMinWaitMs) {
    return new LogRuntime<>(
        new AtomicFileLog(fileIO, location),
        new CatalogStateMachine(fileIO.newInputFile(location)),
        RuntimeConfig.of(
            CompactionPolicy.limits(maxAppendCount, maxAppendSize),
            committedRetentionMs,
            throttleMinWaitMs));
  }

  static CatalogState readInternal(InputFile location, InputStream in, int fileLength)
      throws IOException {
    return replay(location, in, fileLength).build();
  }

  /** Decodes the checkpoint and replays the log, returning the builder before {@code build()}. */
  static CatalogState.Builder replay(InputFile location, InputStream in, int fileLength)
      throws IOException {
    // Read and validate header
    byte[] header = new byte[HEADER_SIZE];
    IOUtil.readFully(in, header, 0, HEADER_SIZE);
    validateMagic(header);
    int version = readInt(header, 4);
    if (version != FORMAT_VERSION) {
      throw new IllegalStateException(
          "Unsupported format version: " + version + ", expected: " + FORMAT_VERSION);
    }

    // Read the checkpoint and the records after it
    int checkpointLen = readVarint(in);
    byte[] checkpointBytes = new byte[checkpointLen];
    IOUtil.readFully(in, checkpointBytes, 0, checkpointLen);
    int logStart = HEADER_SIZE + varintSize(checkpointLen) + checkpointLen;
    int remaining = fileLength - logStart;
    List<byte[]> records = new ArrayList<>();
    if (remaining > 0) {
      byte[] logBytes = new byte[remaining];
      IOUtil.readFully(in, logBytes, 0, remaining);
      ByteArrayInputStream logStream = new ByteArrayInputStream(logBytes);
      while (logStream.available() > 0) {
        int recordLen = readVarint(logStream);
        byte[] recordBytes = new byte[recordLen];
        IOUtil.readFully(logStream, recordBytes, 0, recordLen);
        records.add(recordBytes);
      }
    }

    // Replay: GC records advance the committed set's horizon; each intention record applies once,
    // where its preconditions hold.
    Replay.Result<CatalogState.Builder> replayed =
        Replay.replay(new CatalogStateMachine(location), checkpointBytes, records, null);
    CatalogState.Builder builder = replayed.builder();
    builder.setCatalogUuid(replayed.logId());
    builder.useCommitted(replayed.committed());
    builder.setAppendCount((int) replayed.intentionRecords());
    return builder;
  }

  private static void validateMagic(byte[] header) {
    for (int i = 0; i < MAGIC.length; i++) {
      if (header[i] != MAGIC[i]) {
        throw new IllegalStateException("Invalid magic bytes");
      }
    }
  }

  private static int readInt(byte[] bytes, int offset) {
    return ((bytes[offset] & 0xFF) << 24)
        | ((bytes[offset + 1] & 0xFF) << 16)
        | ((bytes[offset + 2] & 0xFF) << 8)
        | (bytes[offset + 3] & 0xFF);
  }

  private static int readVarint(InputStream in) throws IOException {
    int result = 0;
    int shift = 0;
    int b;
    do {
      b = in.read();
      if (b < 0) {
        throw new IOException("Unexpected end of stream reading varint");
      }
      result |= (b & 0x7F) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    return result;
  }

  private static int varintSize(int value) {
    int size = 0;
    while ((value & ~0x7F) != 0) {
      size++;
      value >>>= 7;
    }
    return size + 1;
  }

  private static void writeVarint(OutputStream out, int value) throws IOException {
    while ((value & ~0x7F) != 0) {
      out.write((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    out.write(value);
  }

  private static void writeInt(OutputStream out, int value) throws IOException {
    out.write((value >> 24) & 0xFF);
    out.write((value >> 16) & 0xFF);
    out.write((value >> 8) & 0xFF);
    out.write(value & 0xFF);
  }

  // ============================================================
  // Mutable builder
  // ============================================================

  /**
   * Mutable builder for CatalogState that accumulates changes and commits atomically.
   */
  public static class Mut extends CatalogFile.Mut<CatalogState, Mut> {

    private final ProtoIdManager idManager = new ProtoIdManager();
    private final int maxAppendCount;
    private final long maxAppendSize;
    private final long committedRetentionMs;
    private final long throttleMinWaitMs;
    private ProtoCodec.Transaction pendingTransaction;

    Mut(InputFile input) {
      this(CatalogState.empty(input));
    }

    Mut(CatalogState other) {
      this(other, DEFAULT_MAX_APPEND_COUNT, DEFAULT_MAX_APPEND_SIZE,
          DEFAULT_COMMITTED_RETENTION_MS, DEFAULT_THROTTLE_MIN_WAIT_MS);
    }

    Mut(InputFile input, int maxAppendCount, long maxAppendSize, long committedRetentionMs,
        long throttleMinWaitMs) {
      this(CatalogState.empty(input), maxAppendCount, maxAppendSize, committedRetentionMs,
          throttleMinWaitMs);
    }

    Mut(CatalogState other, int maxAppendCount, long maxAppendSize,
        long committedRetentionMs, long throttleMinWaitMs) {
      super(other);
      this.maxAppendCount = maxAppendCount;
      this.maxAppendSize = maxAppendSize;
      this.committedRetentionMs = committedRetentionMs;
      this.throttleMinWaitMs = throttleMinWaitMs;
    }

    /**
     * Builds a transaction from the accumulated changes.
     */
    ProtoCodec.Transaction buildTransaction() {
      return ProtoCodec.Transaction.from(this, idManager);
    }

    /**
     * Builds actions from accumulated mutations for transaction encoding.
     *
     * <p>Called by ProtoCodec.Transaction.from() to extract pending changes.
     */
    void buildActions(List<ProtoCodec.Action> actions, ProtoIdManager idManager) {
      // A checkpoint of original carries these next ids (the CAS path reads
      // them from its base); replay allocates the ids this transaction creates.
      idManager.setGlobals(original.nextNamespaceId(), original.nextTableId());

      // New namespaces and tables get late-bound (negative) ids, which replay
      // binds to real ids in log order (ProtoCodec.IdBinding). Allocating real
      // ids here, from this writer's snapshot, let two concurrent writers claim
      // the same id for unrelated creates.
      int nextLateNsid = -1;
      int nextLateTblid = -1;

      // Map namespace created in this transaction to its late-bound ID
      Map<Namespace, Integer> nsIdMap = new java.util.HashMap<>();

      // Sort namespaces by depth (parents before children) to ensure correct ID assignment
      List<Namespace> sortedNamespaces = namespaces.entrySet().stream()
          .filter(Map.Entry::getValue)  // only creates
          .map(Map.Entry::getKey)
          .sorted(java.util.Comparator.comparingInt(Namespace::length))
          .collect(java.util.stream.Collectors.toList());

      // Process namespace creates in topological order
      for (Namespace ns : sortedNamespaces) {
        // Create namespace
        Namespace parent = CatalogFile.Mut.parentOf(ns);
        String name = CatalogFile.Mut.nameOf(ns);

        int parentId = 0;
        int parentVersion = original.namespaceVersion(0);
        if (!parent.isEmpty()) {
          Integer existingParentId = original.namespaceId(parent);
          if (existingParentId != null) {
            parentId = existingParentId;
            parentVersion = original.namespaceVersion(existingParentId);
          } else {
            // Parent created in same transaction
            Integer lateBoundParentId = nsIdMap.get(parent);
            if (lateBoundParentId != null) {
              parentId = lateBoundParentId;
              parentVersion = -1; // late-bound
            }
          }
        }

        int nsId = nextLateNsid--;
        nsIdMap.put(ns, nsId);
        actions.add(new ProtoCodec.CreateNamespaceAction(nsId, 1, parentId, parentVersion, name));

        // Add properties for this namespace. Null values are remove markers
        // from a subsequent updateProperties call; skip them since there is
        // nothing to remove on a freshly-created namespace.
        Map<String, String> props = namespaceProperties.get(ns);
        if (props != null) {
          for (Map.Entry<String, String> prop : props.entrySet()) {
            if (prop.getValue() == null) {
              continue;
            }
            actions.add(new ProtoCodec.SetNamespacePropertyAction(
                nsId, -1, prop.getKey(), prop.getValue()));
          }
        }
      }

      // Process namespace drops (false = drop)
      for (Map.Entry<Namespace, Boolean> entry : namespaces.entrySet()) {
        if (!entry.getValue()) {
          Namespace ns = entry.getKey();
          Integer nsId = original.namespaceId(ns);
          if (nsId != null) {
            int version = original.namespaceVersion(nsId);
            actions.add(new ProtoCodec.DropNamespaceAction(nsId, version));
          }
        }
      }

      // Process namespace property updates (for existing namespaces).
      // CatalogFile.Mut.updateProperties merges into the same map for both
      // sets and removes; a null value is the remove marker.
      for (Map.Entry<Namespace, Map<String, String>> entry : namespaceProperties.entrySet()) {
        Namespace ns = entry.getKey();
        if (!namespaces.containsKey(ns)) {
          // Properties for existing namespace
          Integer nsId = original.namespaceId(ns);
          if (nsId != null) {
            int version = original.namespaceVersion(nsId);
            for (Map.Entry<String, String> prop : entry.getValue().entrySet()) {
              if (prop.getValue() == null) {
                actions.add(new ProtoCodec.RemoveNamespacePropertyAction(
                    nsId, version, prop.getKey()));
              } else {
                actions.add(new ProtoCodec.SetNamespacePropertyAction(
                    nsId, version, prop.getKey(), prop.getValue()));
              }
            }
          }
        }
      }

      // Process table creates and drops. CatalogFile.Mut uses the `tables` map
      // for both: a non-null value is a create-with-location, a null value is
      // a drop marker. Inline-table creates use a separate inlineTables map.
      for (Map.Entry<TableIdentifier, String> entry : tables.entrySet()) {
        TableIdentifier ident = entry.getKey();
        String metadataLocation = entry.getValue();

        if (metadataLocation == null) {
          Integer tblId = original.tableId(ident);
          if (tblId != null) {
            int version = original.tableVersion(tblId);
            actions.add(new ProtoCodec.DropTableAction(tblId, version));
          }
          continue;
        }

        Namespace ns = ident.namespace();
        Integer nsId = original.namespaceId(ns);
        int nsVersion = -1;
        if (nsId == null) {
          // Namespace created in same transaction
          nsId = nsIdMap.get(ns);
        } else {
          nsVersion = original.namespaceVersion(nsId);
        }

        int tblId = nextLateTblid--;
        actions.add(new ProtoCodec.CreateTableAction(
            tblId, 1, nsId, nsVersion, ident.name(), metadataLocation));
      }

      // Process table updates
      for (Map.Entry<TableIdentifier, String> entry : tableUpdates.entrySet()) {
        TableIdentifier ident = entry.getKey();
        String metadataLocation = entry.getValue();
        Integer tblId = original.tableId(ident);
        if (tblId != null) {
          int version = original.tableVersion(tblId);
          actions.add(new ProtoCodec.UpdateTableLocationAction(tblId, version, metadataLocation));
        }
      }

      // Process read tables (for optimistic concurrency)
      for (TableIdentifier ident : readTables) {
        Integer tblId = original.tableId(ident);
        if (tblId != null) {
          int version = original.tableVersion(tblId);
          actions.add(new ProtoCodec.ReadTableAction(tblId, version));
        }
      }

      // Process inline table creates
      for (Map.Entry<TableIdentifier, byte[]> entry : inlineTables.entrySet()) {
        TableIdentifier ident = entry.getKey();
        byte[] metadata = entry.getValue();
        Byte codecTag = inlineTableCodecs.get(ident);
        byte codec = codecTag != null ? codecTag : InlineMetadataCodecs.TAG_JSON_GZIP;
        Namespace ns = ident.namespace();

        Integer nsId = original.namespaceId(ns);
        int nsVersion = -1;
        if (nsId == null) {
          nsId = nsIdMap.get(ns);
        } else {
          nsVersion = original.namespaceVersion(nsId);
        }

        int tblId = nextLateTblid--;
        byte[] createDelta = inlineTableCreateDeltas.get(ident);
        actions.add(new ProtoCodec.CreateTableInlineAction(
            tblId, 1, nsId, nsVersion, ident.name(), metadata, codec, createDelta));
      }

      // Process inline table updates (full mode, optionally with bundled ML delta)
      for (Map.Entry<TableIdentifier, byte[]> entry : inlineTableUpdates.entrySet()) {
        TableIdentifier ident = entry.getKey();
        byte[] metadata = entry.getValue();
        Byte codecTag = inlineTableUpdateCodecs.get(ident);
        byte codec = codecTag != null ? codecTag : InlineMetadataCodecs.TAG_JSON_GZIP;
        byte[] mlDelta = inlineTableUpdateMlDeltas.get(ident);
        Integer tblId = original.tableId(ident);
        if (tblId != null) {
          int version = original.tableVersion(tblId);
          if (mlDelta != null) {
            actions.add(ProtoCodec.UpdateTableInlineAction.full(
                tblId, version, metadata, codec, mlDelta));
          } else {
            actions.add(ProtoCodec.UpdateTableInlineAction.full(
                tblId, version, metadata, codec));
          }
        }
      }

      // Process inline table delta updates
      for (Map.Entry<TableIdentifier, byte[]> entry : inlineTableDeltaUpdates.entrySet()) {
        TableIdentifier ident = entry.getKey();
        byte[] deltaBytes = entry.getValue();
        Integer tblId = original.tableId(ident);
        if (tblId != null) {
          int version = original.tableVersion(tblId);
          actions.add(ProtoCodec.UpdateTableInlineAction.delta(tblId, version, deltaBytes));
        }
      }

      // Process table renames (in-place, id-stable). See SPEC_TM.md "Rename".
      for (Map.Entry<TableIdentifier, TableIdentifier> entry : tableRenames.entrySet()) {
        TableIdentifier from = entry.getKey();
        TableIdentifier to = entry.getValue();
        Integer tblId = original.tableId(from);
        if (tblId == null) {
          continue;
        }
        int version = original.tableVersion(tblId);
        Namespace newNs = to.namespace();
        Integer newNsId = original.namespaceId(newNs);
        int newNsVersion;
        if (newNsId == null) {
          // Destination namespace was created in this same transaction.
          newNsId = nsIdMap.get(newNs);
          newNsVersion = -1;
        } else {
          newNsVersion = original.namespaceVersion(newNsId);
        }
        actions.add(new ProtoCodec.RenameTableAction(
            tblId, version, newNsId, newNsVersion, to.name()));
      }
    }

    /**
     * Commits this transaction's intention through the catalog file's intention log ({@link
     * LogRuntime}), and returns the catalog its record landed in. Maps the runtime's outcomes onto
     * Iceberg's: a conflict to the most informative error ({@link #conflict}), any other rejection
     * to {@link CommitFailedException}, an unknown outcome to {@link CommitStateUnknownException}.
     */
    @Override
    public CatalogState commit(SupportsAtomicOperations fileIO) {
      String location = original.location().location();
      List<ProtoCodec.Action> actions = new ArrayList<>();
      buildActions(actions, idManager);
      try {
        return CatalogState.of(
            runtime(fileIO, location, maxAppendCount, maxAppendSize, committedRetentionMs,
                throttleMinWaitMs)
                .commit(original.replica(), new ProtoCodec.CatalogIntention(actions)));
      } catch (CommitRejectedException e) {
        switch (e.reason()) {
          case CONFLICT:
            Replica<CatalogState> lostTo = e.state();
            throw conflict(lostTo.state(), e.getMessage() + " at " + location);
          case CREATE_LOST:
            throw new CommitFailedException(e, "Catalog creation failed: %s exists", location);
          default:
            throw new CommitFailedException(e, "%s", e.getMessage());
        }
      } catch (CommitOutcomeUnknownException e) {
        throw new CommitStateUnknownException(e.getCause());
      }
    }

    /**
     * The most informative error for a transaction that does not hold on {@code state}:
     * AlreadyExistsException if a pending create or rename destination now exists (Iceberg's
     * create-race semantic), NoSuchTableException if a rename's source is gone,
     * CommitFailedException otherwise, so the caller can refresh and re-apply. A rename is judged
     * as renameTable checks it, source first: one that lost a race to the same rename reports the
     * source gone, as it would have after it.
     */
    private RuntimeException conflict(CatalogState state, String message) {
      for (Map.Entry<TableIdentifier, String> e : tables.entrySet()) {
        if (e.getValue() != null && state.containsTable(e.getKey())) {
          return new AlreadyExistsException("Table already exists: %s", e.getKey());
        }
      }
      for (Map.Entry<TableIdentifier, byte[]> e : inlineTables.entrySet()) {
        if (state.containsTable(e.getKey())) {
          return new AlreadyExistsException("Table already exists: %s", e.getKey());
        }
      }
      for (Map.Entry<TableIdentifier, TableIdentifier> rename : tableRenames.entrySet()) {
        if (!state.containsTable(rename.getKey())) {
          return new NoSuchTableException("Table does not exist: %s", rename.getKey());
        }
        if (state.containsTable(rename.getValue())) {
          return new AlreadyExistsException("Table already exists: %s", rename.getValue());
        }
      }
      return new CommitFailedException("%s", message);
    }

  }

  // ============================================================
  // Next-id counters for the checkpoint header
  // ============================================================

  /**
   * Carries the next namespace / table ids written to a checkpoint header.
   * Writers do not allocate ids: creates carry late-bound ids that replay
   * binds (see {@link ProtoCodec.IdBinding}).
   */
  static class ProtoIdManager {
    private int nextNsid = 1;
    private int nextTblid = 1;

    public int getNextNsid() {
      return nextNsid;
    }

    public int getNextTblid() {
      return nextTblid;
    }

    public void setGlobals(int nextNsid, int nextTblid) {
      this.nextNsid = nextNsid;
      this.nextTblid = nextTblid;
    }
  }
}
