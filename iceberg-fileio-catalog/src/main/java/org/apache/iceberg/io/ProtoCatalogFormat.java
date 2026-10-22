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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Protobuf-based catalog format with checkpoint + append log structure.
 *
 * <p>File structure:
 *
 * <pre>
 * [Magic: 4 bytes "LCAT"]
 * [Version: 4 bytes, big-endian int]
 * [Checkpoint length: varint]
 * [Checkpoint: protobuf bytes]
 * [Transaction 1 length: varint][Transaction 1: protobuf bytes]
 * [Transaction 2 length: varint][Transaction 2: protobuf bytes]
 * ...
 * </pre>
 */
public class ProtoCatalogFormat
    implements CatalogFormat<ProtoCatalogFile, ProtoCatalogFormat.Mut> {

  public static final byte[] MAGIC = "LCAT".getBytes(StandardCharsets.US_ASCII);
  public static final int FORMAT_VERSION = 1;
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
   * On every CAS / compaction commit the writer drops any UUIDv7 from
   * {@code CommittedTransactions} whose timestamp is at or below
   * {@code now - committedRetentionMs} (and below its own transaction's). The
   * largest dropped timestamp becomes {@code highest_dropped_timestamp_ms}: a
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
  public CatalogFile.Mut<ProtoCatalogFile, Mut> empty(InputFile input) {
    return new Mut(input, maxAppendCount, maxAppendSize, committedRetentionMs, throttleMinWaitMs);
  }

  @Override
  public CatalogFile.Mut<ProtoCatalogFile, Mut> from(CatalogFile other) {
    if (!(other instanceof ProtoCatalogFile)) {
      throw new IllegalArgumentException("Cannot convert to ProtoCatalogFile: " + other);
    }
    return new Mut(
        (ProtoCatalogFile) other,
        maxAppendCount,
        maxAppendSize,
        committedRetentionMs,
        throttleMinWaitMs);
  }

  @Override
  public ProtoCatalogFile read(SupportsAtomicOperations fileIO, InputFile catalogLocation) {
    InputFile refresh = fileIO.newInputFile(catalogLocation.location());
    try (SeekableInputStream in = refresh.newStream()) {
      return readInternal(refresh, in, (int) refresh.getLength());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static ProtoCatalogFile readInternal(InputFile location, InputStream in, int fileLength)
      throws IOException {
    return replay(location, in, fileLength).build();
  }

  /** Decodes the checkpoint and replays the log, returning the builder before {@code build()}. */
  static ProtoCatalogFile.Builder replay(InputFile location, InputStream in, int fileLength)
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

    // Read checkpoint
    int checkpointLen = readVarint(in);
    byte[] checkpointBytes = new byte[checkpointLen];
    IOUtil.readFully(in, checkpointBytes, 0, checkpointLen);
    ProtoCatalogFile.Builder builder = ProtoCatalogFile.builder(location);
    ProtoCodec.decodeCheckpoint(checkpointBytes, builder);

    // Read and apply transaction log
    int logStart = HEADER_SIZE + varintSize(checkpointLen) + checkpointLen;
    int remaining = fileLength - logStart;
    int appendCount = 0;
    if (remaining > 0) {
      byte[] logBytes = new byte[remaining];
      IOUtil.readFully(in, logBytes, 0, remaining);
      ByteArrayInputStream logStream = new ByteArrayInputStream(logBytes);

      while (logStream.available() > 0) {
        int txnLen = readVarint(logStream);
        byte[] txnBytes = new byte[txnLen];
        IOUtil.readFully(logStream, txnBytes, 0, txnLen);
        appendCount++;

        ProtoCodec.Transaction txn = ProtoCodec.decodeTransaction(txnBytes);
        if (!builder.mayApply(txn.id())) {
          // Already applied (a retry), or its id is at or below the committed set's GC horizon,
          // where the set cannot tell a retry of an aged-out commit from a new one.
          continue;
        }
        if (txn.verify(builder)) {
          txn.apply(builder);
          builder.addCommittedTransaction(txn.id());
        }
      }
    }
    builder.setAppendCount(appendCount);

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
   * Mutable builder for ProtoCatalogFile that accumulates changes and commits atomically.
   */
  public static class Mut extends CatalogFile.Mut<ProtoCatalogFile, Mut> {

    private final ProtoIdManager idManager = new ProtoIdManager();
    private final int maxAppendCount;
    private final long maxAppendSize;
    private final long committedRetentionMs;
    private final long throttleMinWaitMs;
    private ProtoCodec.Transaction pendingTransaction;

    Mut(InputFile input) {
      this(ProtoCatalogFile.empty(input));
    }

    Mut(ProtoCatalogFile other) {
      this(other, DEFAULT_MAX_APPEND_COUNT, DEFAULT_MAX_APPEND_SIZE,
          DEFAULT_COMMITTED_RETENTION_MS, DEFAULT_THROTTLE_MIN_WAIT_MS);
    }

    Mut(InputFile input, int maxAppendCount, long maxAppendSize, long committedRetentionMs,
        long throttleMinWaitMs) {
      this(ProtoCatalogFile.empty(input), maxAppendCount, maxAppendSize, committedRetentionMs,
          throttleMinWaitMs);
    }

    Mut(ProtoCatalogFile other, int maxAppendCount, long maxAppendSize,
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

    @Override
    public ProtoCatalogFile commit(SupportsAtomicOperations fileIO) {
      final int MAX_ATTEMPTS = 10;

      // baseCatalog is the parsed state of current unless baseIsCurrent is false:
      // after a lost append race, current is refreshed without reading it.
      ProtoCatalogFile baseCatalog = original;
      InputFile current = original.location();
      boolean baseIsCurrent = true;
      ProtoCodec.Transaction txn = buildTransaction();
      byte[] txnBytes = ProtoCodec.encodeTransaction(txn);

      // Case 0: Initial commit (file doesn't exist)
      if (!current.exists()) {
        try {
          return tryCAS(original, current, txn, txnBytes, fileIO);
        } catch (AmbiguousWrite | SupportsAtomicOperations.CASException e) {
          // The creation may have landed: its reply was lost, reading it back failed, or it
          // failed its precondition, as a transport retry of a creation that landed does.
          Throwable failure = e instanceof AmbiguousWrite ? e.getCause() : e;
          ProtoCatalogFile.Builder fresh = resolve(failure, fileIO.newInputFile(current.location()));
          if (fresh != null && landed(fresh::containsTransaction, txn, failure)) {
            return fresh.build(); // the creation landed
          }
          if (fresh != null && !(e instanceof AmbiguousWrite)) {
            throw new CommitFailedException(e, "Catalog creation failed: %s exists", current.location());
          }
          throw new CommitStateUnknownException(failure);
        }
      }

      // Backends without an append primitive (e.g. GCS) coerce every commit to CAS,
      // regardless of how the format was configured. Without this guard, a catalog
      // pointed at GCS with a non-zero max.append.count would silently overwrite the
      // catalog file in tryAppend (writeAtomic with Strategy.APPEND replaces the
      // object on GCS) -- and now GCSOutputFile.prepare() rejects APPEND outright,
      // so an unguarded append commit would surface as IllegalArgumentException.
      final boolean appendSupported = fileIO.supportsAppend();

      // The last write whose outcome is unknown, if any: its reply was lost, or reading back what
      // it wrote failed. It may still be in flight, so once one has happened, running out of
      // attempts no longer means the commit failed.
      AmbiguousWrite ambiguous = null;
      // The last write that failed its precondition, until a read of the catalog rules out that it
      // landed. A transport retry of a write that landed fails its precondition, so the failure is
      // no proof; but the write landed before the reply, so a read without it settles it.
      SupportsAtomicOperations.AtomicOperationException unresolved = null;
      // Whether storage refused an append because the object holds as many writes as it allows (S3
      // Express: 10,000 parts; an Azure Append Blob: 50,000 blocks): only a CAS can commit then,
      // whatever the append count says.
      boolean full = false;

      for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
        try {
          // Decide whether to CAS (compact) or append based on observable state
          // of the live file: backend support, append count, and current size.
          // Either limit (count or size) being met forces CAS on the next commit.
          // Setting maxAppendCount=0 forces CAS on every commit.
          boolean mustCAS =
              full
                  || !appendSupported
                  || baseCatalog.appendCount() >= maxAppendCount
                  || length(current) + txnBytes.length > maxAppendSize;

          if (mustCAS) {
            if (!baseIsCurrent) {
              // The CAS replaces current, so its checkpoint must hold the state
              // current names: a checkpoint of original would erase every record
              // committed since. Our transaction record follows the checkpoint and
              // replay verifies it against that state, as it would an append.
              // Read the live version afresh rather than the one current was bound
              // to: storage fails a read pinned to a version another writer has since
              // replaced (a GCS read by generation, an S3 or ADLS read with If-Match).
              current = fileIO.newInputFile(current.location());
              ProtoCatalogFile.Builder fresh = replay(current);
              if (landed(fresh::containsTransaction, txn, earlier(ambiguous, unresolved))) {
                return fresh.build(); // an append reported as a lost race landed
              }
              unresolved = null;
              baseCatalog = fresh.build();
              baseIsCurrent = true;
            }
            full = false;
            try {
              return validateCommit(
                  tryCAS(baseCatalog, current, txn, txnBytes, fileIO),
                  txn,
                  earlier(ambiguous, unresolved));
            } catch (SupportsAtomicOperations.CASException e) {
              unresolved = e;
            }
            // CAS conflict: another writer changed the file since we read it.
            // Rebase onto the new state. Replay checks this transaction's
            // preconditions against the state its record lands on, so if they
            // hold on the new state, retry on top of it -- by append or CAS, as
            // that state calls for -- and the commit is accepted exactly when a
            // serial commit would be. If not, fail without writing; the client
            // refreshes and re-applies.
            current = fileIO.newInputFile(current.location());
            ProtoCatalogFile.Builder fresh = replay(current);
            if (landed(fresh::containsTransaction, txn, earlier(ambiguous, unresolved))) {
              return fresh.build(); // an earlier attempt landed
            }
            unresolved = null;
            boolean holds = txn.verify(fresh);
            baseCatalog = fresh.build();
            baseIsCurrent = true;
            if (!holds) {
              throw conflict(
                  baseCatalog, "Cannot commit: catalog file changed concurrently at " + current.location());
            }
          } else {
            try {
              return validateCommit(
                  tryAppend(current, txnBytes, fileIO), txn, earlier(ambiguous, unresolved));
            } catch (SupportsAtomicOperations.SegmentLimitException e) {
              // Nothing landed, and no append to this object can: compact it by CAS, on a fresh
              // read. Without this the commit took the append for ambiguous, and with an append
              // count above the store's limit no writer ever compacted (Bug 66).
              full = true;
              baseIsCurrent = false;
              continue;
            } catch (SupportsAtomicOperations.AppendException e) {
              unresolved = e;
            }
            // Append failed: another writer appended first (offset mismatch).
            // The transaction bytes are identical and idempotent (UUID-deduplicated),
            // so we can retry the append at the new offset without re-reading the
            // catalog. Only need to refresh the InputFile to get the current length.
            long oldLength = length(current);
            current = fileIO.newInputFile(current.location());
            if (length(current) < oldLength) {
              // File shrank -- another writer compacted. Re-read to refresh
              // appendCount so the next mustCAS decision is accurate; afresh, as
              // above, since current's version may be replaced by now.
              current = fileIO.newInputFile(current.location());
              ProtoCatalogFile.Builder fresh = replay(current);
              if (landed(fresh::containsTransaction, txn, earlier(ambiguous, unresolved))) {
                return fresh.build(); // an append reported as a lost race landed
              }
              unresolved = null;
              baseCatalog = fresh.build();
              baseIsCurrent = true;
            } else {
              // File grew (normal concurrent append). Retry with same bytes at the
              // new offset; the append count is now a lower bound, which is fine
              // for a soft limit. No read needed unless the next attempt CASes.
              baseIsCurrent = false;
            }
          }
        } catch (AmbiguousWrite e) {
          // The write may have landed: its reply was lost, or reading back what it wrote failed.
          // Its record carries this transaction's id, so a fresh read says which. If it has not
          // landed, retry from the fresh state: a write still in flight carries the same
          // transaction and a precondition on the version it saw, so at most one of the two lands,
          // and replay applies the transaction once. A definite rejection stays definite: replay
          // verifies a record against the state it lands on, so a late landing of a transaction
          // that does not hold changes nothing.
          ambiguous = e;
          current = fileIO.newInputFile(current.location());
          ProtoCatalogFile.Builder fresh = resolve(e.getCause(), current);
          if (fresh == null) {
            throw new CommitStateUnknownException(e.getCause());
          }
          if (landed(fresh::containsTransaction, txn, e.getCause())) {
            return fresh.build(); // the write landed
          }
          unresolved = null;
          boolean holds = txn.verify(fresh);
          baseCatalog = fresh.build();
          baseIsCurrent = true;
          if (!holds) {
            throw conflict(
                baseCatalog, "Cannot commit: catalog file changed concurrently at " + current.location());
          }
        } catch (UnreadableCatalog e) {
          // A read failed. The commit failed too, unless one of its writes may have landed.
          Throwable write = ambiguous != null ? ambiguous.getCause() : unresolved;
          if (write == null) {
            throw e.failure();
          }
          write.addSuppressed(e.failure());
          throw new CommitStateUnknownException(write);
        }
      }

      if (ambiguous != null) {
        throw new CommitStateUnknownException(ambiguous.getCause());
      }
      if (unresolved != null) {
        // The last attempt lost an append race, which its own landing would also report.
        ProtoCatalogFile.Builder fresh = resolve(unresolved, fileIO.newInputFile(current.location()));
        if (fresh == null) {
          throw new CommitStateUnknownException(unresolved);
        }
        if (landed(fresh::containsTransaction, txn, unresolved)) {
          return fresh.build();
        }
      }
      throw new CommitFailedException("Exceeded maximum commit attempts");
    }

    /**
     * Re-reads the catalog after a write that may have landed, or returns null if that fails too
     * (the read failure is attached to {@code writeFailure}).
     */
    private ProtoCatalogFile.Builder resolve(Throwable writeFailure, InputFile file) {
      try {
        return replay(file);
      } catch (UnreadableCatalog readFailure) {
        writeFailure.addSuppressed(readFailure.failure());
        return null;
      }
    }

    /**
     * Whether a read holds {@code txn}, by {@code contains}. If its id is at or below the
     * committed set's GC horizon, the set cannot say, and replay rejects its record from now on
     * (Bug 64): the commit failed, unless {@code earlier}, a write of it that may have landed,
     * did, before its id aged out.
     */
    private boolean landed(
        java.util.function.Predicate<UUID> contains, ProtoCodec.Transaction txn, Throwable earlier) {
      try {
        return contains.test(txn.id());
      } catch (DedupHorizonExceededException e) {
        if (earlier != null) {
          earlier.addSuppressed(e);
          throw new CommitStateUnknownException(earlier);
        }
        throw new CommitFailedException(
            e, "Cannot commit: transaction %s is at or below the catalog's committed-set horizon",
            txn.id());
      }
    }

    /** The last write of this commit that may have landed, or null. */
    private static Throwable earlier(
        AmbiguousWrite ambiguous, SupportsAtomicOperations.AtomicOperationException unresolved) {
      return ambiguous != null ? ambiguous.getCause() : unresolved;
    }

    private ProtoCatalogFile validateCommit(
        ProtoCatalogFile result, ProtoCodec.Transaction txn, Throwable earlier) {
      if (!landed(result::containsTransaction, txn, earlier)) {
        // Our record landed but at least one action's verify rejected it
        // (e.g., a concurrent writer bumped the table version).
        throw conflict(result, "Cannot commit: concurrent transaction invalidated this update");
      }
      return result;
    }

    /**
     * The most informative error for a transaction that does not hold on {@code state}:
     * AlreadyExistsException if a pending create or rename destination now exists (Iceberg's
     * create-race semantic), NoSuchTableException if a rename's source is gone,
     * CommitFailedException otherwise, so the caller can refresh and re-apply. A rename is judged
     * as renameTable checks it, source first: one that lost a race to the same rename reports the
     * source gone, as it would have after it.
     */
    private RuntimeException conflict(ProtoCatalogFile state, String message) {
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

    /** Reads and parses {@code file}; any failure becomes {@link UnreadableCatalog}. */
    private ProtoCatalogFile.Builder replay(InputFile file) {
      try (SeekableInputStream in = file.newStream()) {
        return ProtoCatalogFormat.replay(file, in, (int) file.getLength());
      } catch (IOException e) {
        throw new UnreadableCatalog(new UncheckedIOException(e));
      } catch (RuntimeException e) {
        throw new UnreadableCatalog(e);
      }
    }

    /** The length of {@code file}; a failure to read it becomes {@link UnreadableCatalog}. */
    private static long length(InputFile file) {
      try {
        return file.getLength();
      } catch (RuntimeException e) {
        throw new UnreadableCatalog(e);
      }
    }

    /**
     * Replaces {@code current}, whose parsed state is {@code base}, with base + txnBytes. Throws
     * {@link SupportsAtomicOperations.CASException} if {@code current} is no longer live.
     */
    private ProtoCatalogFile tryCAS(
        ProtoCatalogFile base,
        InputFile current,
        ProtoCodec.Transaction txn,
        byte[] txnBytes,
        SupportsAtomicOperations fileIO) {
      // Build the wire bytes once; retries reuse the same payload.
      final byte[] fullFile;
      try {
        fullFile = buildFullFile(base, txn, txnBytes);
      } catch (IOException e) {
        throw new CommitFailedException(e, "CAS commit failed: %s", e.getMessage());
      }

      return retryThrottled(
          current.location(),
          () -> {
            AtomicOutputFile outputFile = fileIO.newOutputFile(current);
            try (ByteArrayInputStream serBytes = new ByteArrayInputStream(fullFile)) {
              serBytes.mark(fullFile.length);
              CAS token = outputFile.prepare(() -> serBytes, AtomicOutputFile.Strategy.CAS);
              serBytes.reset();
              return writeAndRead(outputFile, token, () -> serBytes);
            } catch (IOException e) {
              throw new CommitFailedException(e, "CAS commit failed: %s", e.getMessage());
            }
          });
    }

    /**
     * Appends txnBytes to {@code current}. Throws {@link SupportsAtomicOperations.AppendException}
     * if {@code current} is no longer live.
     */
    private ProtoCatalogFile tryAppend(
        InputFile current, byte[] txnBytes, SupportsAtomicOperations fileIO) {
      final byte[] record;
      try {
        record = buildTransactionRecord(txnBytes);
      } catch (IOException e) {
        throw new UncheckedIOException("Append commit failed: " + e.getMessage(), e);
      }
      return retryThrottled(
          current.location(),
          () -> {
            AtomicOutputFile outputFile = fileIO.newOutputFile(current);
            try (ByteArrayInputStream serBytes = new ByteArrayInputStream(record)) {
              serBytes.mark(record.length);
              CAS token = outputFile.prepare(() -> serBytes, AtomicOutputFile.Strategy.APPEND);
              serBytes.reset();
              return writeAndRead(outputFile, token, () -> serBytes);
            } catch (IOException e) {
              throw new UncheckedIOException("Append commit failed: " + e.getMessage(), e);
            }
          });
    }

    /**
     * Performs {@code write}, retrying it after a backoff while storage applies backpressure
     * (StorageThrottleException: a 429, a 5xx, the GCS per-object update rate). Each retry carries
     * the same precondition, so at most one attempt lands. A throttled write may have landed even
     * so: a 5xx can follow a write the service applied, and an SDK's own retries report only the
     * last attempt. A retry of a write that landed fails its precondition, which the commit loop
     * resolves by reading the catalog; running out of retries leaves the outcome unknown in the
     * same way, so it is an {@link AmbiguousWrite}. An interrupt during the backoff stops the
     * commit with its outcome unknown.
     */
    private ProtoCatalogFile retryThrottled(String location, Supplier<ProtoCatalogFile> write) {
      final int maxThrottleAttempts = 8;
      long backoffMs = throttleMinWaitMs;
      SupportsAtomicOperations.StorageThrottleException lastThrottle = null;
      for (int attempt = 0; attempt < maxThrottleAttempts; attempt++) {
        try {
          return write.get();
        } catch (SupportsAtomicOperations.StorageThrottleException e) {
          lastThrottle = e;
          long sleepMs =
              backoffMs + java.util.concurrent.ThreadLocalRandom.current().nextLong(backoffMs / 2 + 1);
          try {
            Thread.sleep(sleepMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            e.addSuppressed(ie);
            throw new CommitStateUnknownException(e);
          }
          backoffMs = Math.min(backoffMs * 2, Math.max(5_000L, throttleMinWaitMs));
        } catch (CommitFailedException | UncheckedIOException e) {
          // This attempt failed before writing; an earlier, throttled one may have landed.
          if (lastThrottle == null) {
            throw e;
          }
          e.addSuppressed(lastThrottle);
          throw new AmbiguousWrite(e);
        }
      }
      throw new AmbiguousWrite(
          new IOException(
              String.format(
                  "Storage throttled %d attempts at %s", maxThrottleAttempts, location),
              lastThrottle));
    }

    /**
     * Performs a conditional write and parses what it wrote. The protocol's conflict and throttling
     * signals propagate; any other failure of the write, or of reading back what it wrote, leaves
     * its outcome unknown and becomes {@link AmbiguousWrite}.
     */
    private ProtoCatalogFile writeAndRead(
        AtomicOutputFile outputFile, CAS token, Supplier<InputStream> bytes) {
      InputFile written;
      try {
        written = outputFile.writeAtomic(token, bytes);
      } catch (SupportsAtomicOperations.AtomicOperationException e) {
        throw e;
      } catch (IOException | RuntimeException e) {
        throw new AmbiguousWrite(e);
      }
      try (SeekableInputStream in = written.newStream()) {
        return readInternal(written, in, (int) written.getLength());
      } catch (IOException | RuntimeException e) {
        throw new AmbiguousWrite(e);
      }
    }

    private byte[] buildFullFile(ProtoCatalogFile base, ProtoCodec.Transaction txn, byte[] txnBytes)
        throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();

      // Header
      out.write(MAGIC);
      writeInt(out, FORMAT_VERSION);

      // Checkpoint of the base state; pending changes go in the record below. CAS / compaction
      // is the only place the committed-set is rewritten, so this is also the GC
      // site: any inherited UUIDv7 with timestamp_ms <= watermark is dropped.
      // watermark = now - committedRetentionMs, clamped to 0 (never negative);
      // Long.MAX_VALUE retention disables GC. It stays below this transaction's
      // own time: the horizon must not reach the record that follows, or replay
      // rejects it (Bug 64). A short retention would otherwise drop the id of a
      // commit this one rebased on, minted after it.
      long now = System.currentTimeMillis();
      long watermark = committedRetentionMs >= now ? 0L : now - committedRetentionMs;
      watermark = Math.max(0L, Math.min(watermark, UuidV7.timestampMs(txn.id()) - 1));
      ProtoIdManager baseIds = new ProtoIdManager();
      baseIds.setGlobals(base.nextNamespaceId(), base.nextTableId());
      byte[] checkpointBytes = ProtoCodec.encodeCheckpoint(base, this, baseIds, watermark);
      writeVarint(out, checkpointBytes.length);
      out.write(checkpointBytes);

      // Transaction: the checkpoint holds the base state only; replay verifies
      // and applies this record on top of it (binding any late-bound ids).
      writeVarint(out, txnBytes.length);
      out.write(txnBytes);

      return out.toByteArray();
    }

    private byte[] buildTransactionRecord(byte[] txnBytes) throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      writeVarint(out, txnBytes.length);
      out.write(txnBytes);
      return out.toByteArray();
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

  /** A conditional write that may or may not have taken effect. */
  private static class AmbiguousWrite extends RuntimeException {
    AmbiguousWrite(Exception cause) {
      super(cause);
    }
  }

  /** A failed read of the catalog during a commit. */
  private static class UnreadableCatalog extends RuntimeException {
    UnreadableCatalog(RuntimeException cause) {
      super(cause);
    }

    RuntimeException failure() {
      return (RuntimeException) getCause();
    }
  }
}
