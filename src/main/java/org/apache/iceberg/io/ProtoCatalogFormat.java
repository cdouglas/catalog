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
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
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

  public ProtoCatalogFormat() {}

  public ProtoCatalogFormat(Map<String, String> properties) {}

  @Override
  public CatalogFile.Mut<ProtoCatalogFile, Mut> empty(InputFile input) {
    return new Mut(input);
  }

  @Override
  public CatalogFile.Mut<ProtoCatalogFile, Mut> from(CatalogFile other) {
    if (!(other instanceof ProtoCatalogFile)) {
      throw new IllegalArgumentException("Cannot convert to ProtoCatalogFile: " + other);
    }
    return new Mut((ProtoCatalogFile) other);
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
    if (remaining > 0) {
      byte[] logBytes = new byte[remaining];
      IOUtil.readFully(in, logBytes, 0, remaining);
      ByteArrayInputStream logStream = new ByteArrayInputStream(logBytes);

      while (logStream.available() > 0) {
        int txnLen = readVarint(logStream);
        byte[] txnBytes = new byte[txnLen];
        IOUtil.readFully(logStream, txnBytes, 0, txnLen);

        ProtoCodec.Transaction txn = ProtoCodec.decodeTransaction(txnBytes);
        if (builder.containsTransaction(txn.id())) {
          // Already applied (duplicate from retry)
          continue;
        }
        if (txn.verify(builder)) {
          txn.apply(builder);
          builder.addCommittedTransaction(txn.id());
        }
        if (txn.isSealed()) {
          builder.setSealed(true);
          break;
        }
      }
    }

    return builder.build();
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
    private ProtoCodec.Transaction pendingTransaction;

    Mut(InputFile input) {
      this(ProtoCatalogFile.empty(input));
    }

    Mut(ProtoCatalogFile other) {
      super(other);
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
      // Initialize ID manager from original catalog state
      idManager.setGlobals(original.nextNamespaceId(), original.nextTableId());

      // Map namespace to assigned ID (for late-binding)
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
        int parentVersion = -1;
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

        int nsId = idManager.allocateNsid();
        nsIdMap.put(ns, nsId);
        actions.add(new ProtoCodec.CreateNamespaceAction(nsId, 1, parentId, parentVersion, name));

        // Add properties for this namespace
        Map<String, String> props = namespaceProperties.get(ns);
        if (props != null) {
          for (Map.Entry<String, String> prop : props.entrySet()) {
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

      // Process namespace property updates (for existing namespaces)
      for (Map.Entry<Namespace, Map<String, String>> entry : namespaceProperties.entrySet()) {
        Namespace ns = entry.getKey();
        if (!namespaces.containsKey(ns)) {
          // Properties for existing namespace
          Integer nsId = original.namespaceId(ns);
          if (nsId != null) {
            int version = original.namespaceVersion(nsId);
            for (Map.Entry<String, String> prop : entry.getValue().entrySet()) {
              actions.add(new ProtoCodec.SetNamespacePropertyAction(
                  nsId, version, prop.getKey(), prop.getValue()));
            }
          }
        }
      }

      // Process table creates
      for (Map.Entry<TableIdentifier, String> entry : tables.entrySet()) {
        TableIdentifier ident = entry.getKey();
        String metadataLocation = entry.getValue();
        Namespace ns = ident.namespace();

        Integer nsId = original.namespaceId(ns);
        int nsVersion = -1;
        if (nsId == null) {
          // Namespace created in same transaction
          nsId = nsIdMap.get(ns);
        } else {
          nsVersion = original.namespaceVersion(nsId);
        }

        int tblId = idManager.allocateTblid();
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
        Namespace ns = ident.namespace();

        Integer nsId = original.namespaceId(ns);
        int nsVersion = -1;
        if (nsId == null) {
          nsId = nsIdMap.get(ns);
        } else {
          nsVersion = original.namespaceVersion(nsId);
        }

        int tblId = idManager.allocateTblid();
        actions.add(new ProtoCodec.CreateTableInlineAction(
            tblId, 1, nsId, nsVersion, ident.name(), metadata));
      }

      // Process inline table updates (full mode)
      for (Map.Entry<TableIdentifier, byte[]> entry : inlineTableUpdates.entrySet()) {
        TableIdentifier ident = entry.getKey();
        byte[] metadata = entry.getValue();
        Integer tblId = original.tableId(ident);
        if (tblId != null) {
          int version = original.tableVersion(tblId);
          actions.add(new ProtoCodec.UpdateTableInlineAction(
              tblId, version, metadata, null));
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
    }

    @Override
    public ProtoCatalogFile commit(SupportsAtomicOperations fileIO) {
      final long MAX_CATALOG_SIZE = 16L * 1024 * 1024;
      final int MAX_ATTEMPTS = 10;

      ProtoCatalogFile baseCatalog = original;
      InputFile current = original.location();
      ProtoCodec.Transaction txn = buildTransaction();
      byte[] txnBytes = ProtoCodec.encodeTransaction(txn);

      // Case 0: Initial commit (file doesn't exist)
      if (!current.exists()) {
        return tryCAS(current, txn, txnBytes, fileIO)
            .orElseThrow(() -> new CommitFailedException("Catalog creation failed"));
      }

      for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
        // Case 1: Catalog is sealed - must compact via CAS
        if (baseCatalog.isSealed()) {
          ProtoCodec.unsealTransaction(txnBytes);
          Optional<ProtoCatalogFile> result = tryCAS(current, txn, txnBytes, fileIO);
          if (result.isPresent()) {
            return validateCommit(result.get(), txn);
          }
        } else {
          // Case 2: Normal append
          if (current.getLength() + txnBytes.length > MAX_CATALOG_SIZE) {
            ProtoCodec.sealTransaction(txnBytes);
          }
          Optional<ProtoCatalogFile> result = tryAppend(current, txn, txnBytes, fileIO);
          if (result.isPresent()) {
            return validateCommit(result.get(), txn);
          }
        }

        // Refresh and retry
        long oldLength = current.getLength();
        current = fileIO.newInputFile(current.location());
        if (current.getLength() < oldLength) {
          // File was compacted by another writer
          ProtoCodec.unsealTransaction(txnBytes);
        }
        try (SeekableInputStream in = current.newStream()) {
          baseCatalog = readInternal(current, in, (int) current.getLength());
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      throw new CommitFailedException("Exceeded maximum commit attempts");
    }

    private ProtoCatalogFile validateCommit(ProtoCatalogFile result, ProtoCodec.Transaction txn) {
      if (!result.containsTransaction(txn.id())) {
        throw new CommitFailedException("Conflicting concurrent transaction");
      }
      return result;
    }

    private Optional<ProtoCatalogFile> tryCAS(
        InputFile current,
        ProtoCodec.Transaction txn,
        byte[] txnBytes,
        SupportsAtomicOperations fileIO) {
      try {
        AtomicOutputFile outputFile = fileIO.newOutputFile(current);
        byte[] fullFile = buildFullFile(txnBytes);

        try (ByteArrayInputStream serBytes = new ByteArrayInputStream(fullFile)) {
          serBytes.mark(fullFile.length);
          CAS token = outputFile.prepare(() -> serBytes, AtomicOutputFile.Strategy.CAS);
          serBytes.reset();
          InputFile newCatalog = outputFile.writeAtomic(token, () -> serBytes);

          try (SeekableInputStream in = newCatalog.newStream()) {
            return Optional.of(readInternal(newCatalog, in, (int) newCatalog.getLength()));
          }
        }
      } catch (SupportsAtomicOperations.CASException e) {
        return Optional.empty();
      } catch (IOException e) {
        throw new CommitFailedException(e, "CAS commit failed: %s", e.getMessage());
      }
    }

    private Optional<ProtoCatalogFile> tryAppend(
        InputFile current,
        ProtoCodec.Transaction txn,
        byte[] txnBytes,
        SupportsAtomicOperations fileIO) {
      try {
        AtomicOutputFile outputFile = fileIO.newOutputFile(current);
        byte[] record = buildTransactionRecord(txnBytes);

        try (ByteArrayInputStream serBytes = new ByteArrayInputStream(record)) {
          serBytes.mark(record.length);
          CAS token = outputFile.prepare(() -> serBytes, AtomicOutputFile.Strategy.APPEND);
          serBytes.reset();
          InputFile newCatalog = outputFile.writeAtomic(token, () -> serBytes);

          try (SeekableInputStream in = newCatalog.newStream()) {
            return Optional.of(readInternal(newCatalog, in, (int) newCatalog.getLength()));
          }
        }
      } catch (SupportsAtomicOperations.AppendException e) {
        return Optional.empty();
      } catch (IOException e) {
        throw new UncheckedIOException("Append commit failed: " + e.getMessage(), e);
      }
    }

    private byte[] buildFullFile(byte[] txnBytes) throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();

      // Header
      out.write(MAGIC);
      writeInt(out, FORMAT_VERSION);

      // Checkpoint (from current state + pending changes merged)
      byte[] checkpointBytes = ProtoCodec.encodeCheckpoint(original, this, idManager);
      writeVarint(out, checkpointBytes.length);
      out.write(checkpointBytes);

      // Transaction (empty actions since checkpoint includes the changes)
      // But we still write it for the transaction ID tracking
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
  // ID Manager for late-binding
  // ============================================================

  static class ProtoIdManager {
    private int nextNsid = 1;
    private int nextTblid = 1;
    private final java.util.Map<Integer, Integer> nsRemap =
        new java.util.HashMap<>();

    public int getNextNsid() {
      return nextNsid;
    }

    public int getNextTblid() {
      return nextTblid;
    }

    public int allocateNsid() {
      return nextNsid++;
    }

    public int allocateTblid() {
      return nextTblid++;
    }

    public int remap(int virtualId) {
      Preconditions.checkArgument(virtualId < 0, "Not a virtual ID: %d", virtualId);
      int realId = allocateNsid();
      nsRemap.put(virtualId, realId);
      return realId;
    }

    public int resolve(int id) {
      if (id < 0) {
        Integer remapped = nsRemap.get(id);
        Preconditions.checkNotNull(remapped, "Virtual ID not remapped: %d", id);
        return remapped;
      }
      return id;
    }

    public void setGlobals(int nextNsid, int nextTblid) {
      this.nextNsid = nextNsid;
      this.nextTblid = nextTblid;
    }
  }
}
