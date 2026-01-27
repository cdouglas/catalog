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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Handles all serialization and deserialization for the log-based catalog format.
 *
 * <p>Binary format documentation:
 *
 * <h3>Checkpoint Header (37 bytes)</h3>
 *
 * <pre>
 * [opcode: 1 byte]     - Type.CHECKPOINT (0)
 * [uuid: 16 bytes]     - Catalog UUID (msb + lsb)
 * [nextNsid: 4 bytes]  - Next namespace ID
 * [nextTblid: 4 bytes] - Next table ID
 * [chkLen: 4 bytes]    - Checkpoint data length
 * [tblEmbedEnd: 4 bytes] - Table embed region end (reserved)
 * [committedTxnLen: 4 bytes] - Committed transaction data length
 * </pre>
 *
 * <h3>Transaction Header</h3>
 *
 * <pre>
 * [opcode: 1 byte]     - Type.TRANSACTION (9)
 * [uuid: 16 bytes]     - Transaction UUID
 * [sealed: 1 byte]     - Seal flag at offset 17
 * [nActions: 4 bytes]  - Number of actions
 * [actions...]         - Action records
 * </pre>
 */
public class LogSerializer {

  /** Offset of the sealed flag in serialized transaction bytes */
  public static final int SEALED_OFFSET = 17;

  private LogSerializer() {}

  // ==================== Checkpoint Serialization ====================

  public static void writeCheckpoint(
      LogCatalogFormat.LogAction.Checkpoint checkpoint, DataOutputStream dos) throws IOException {
    dos.writeByte(LogCatalogFormat.LogAction.Type.CHECKPOINT.opcode);
    dos.writeLong(checkpoint.catalogUUID.getMostSignificantBits());
    dos.writeLong(checkpoint.catalogUUID.getLeastSignificantBits());
    dos.writeInt(checkpoint.nextNsid);
    dos.writeInt(checkpoint.nextTblid);
    dos.writeInt(checkpoint.chkLen);
    dos.writeInt(checkpoint.tblEmbedEnd);
    dos.writeInt(checkpoint.committedTxnLen);
  }

  public static LogCatalogFormat.LogAction.Checkpoint readCheckpoint(DataInputStream dis)
      throws IOException {
    long msb = dis.readLong();
    long lsb = dis.readLong();
    UUID catalogUUID = new UUID(msb, lsb);
    int nextNsid = dis.readInt();
    int nextTblid = dis.readInt();
    int chkLen = dis.readInt();
    int tblEmbedEnd = dis.readInt();
    int committedTxnLen = dis.readInt();
    return new LogCatalogFormat.LogAction.Checkpoint(
        catalogUUID, nextNsid, nextTblid, chkLen, tblEmbedEnd, committedTxnLen);
  }

  // ==================== Namespace Serialization ====================

  public static void writeCreateNamespace(
      LogCatalogFormat.LogAction.CreateNamespace action, DataOutputStream dos) throws IOException {
    dos.writeByte(LogCatalogFormat.LogAction.Type.CREATE_NAMESPACE.opcode);
    dos.writeUTF(action.name);
    dos.writeInt(action.logNsid);
    dos.writeInt(action.logVersion);
    dos.writeInt(action.logParentId);
    dos.writeInt(action.logParentVersion);
  }

  public static LogCatalogFormat.LogAction.CreateNamespace readCreateNamespace(DataInputStream dis)
      throws IOException {
    String name = dis.readUTF();
    int nsid = dis.readInt();
    int version = dis.readInt();
    int parentId = dis.readInt();
    int parentVersion = dis.readInt();
    return new LogCatalogFormat.LogAction.CreateNamespace(name, nsid, version, parentId, parentVersion);
  }

  public static void writeDropNamespace(
      LogCatalogFormat.LogAction.DropNamespace action, DataOutputStream dos) throws IOException {
    dos.writeByte(LogCatalogFormat.LogAction.Type.DROP_NAMESPACE.opcode);
    dos.writeInt(action.nsid);
    dos.writeInt(action.version);
  }

  public static LogCatalogFormat.LogAction.DropNamespace readDropNamespace(DataInputStream dis)
      throws IOException {
    int nsid = dis.readInt();
    int version = dis.readInt();
    return new LogCatalogFormat.LogAction.DropNamespace(nsid, version);
  }

  public static void writeAddNamespaceProperty(
      LogCatalogFormat.LogAction.AddNamespaceProperty action, DataOutputStream dos)
      throws IOException {
    dos.writeByte(LogCatalogFormat.LogAction.Type.ADD_NAMESPACE_PROPERTY.opcode);
    dos.writeInt(action.logNsid);
    dos.writeInt(action.logVersion);
    dos.writeUTF(action.key);
    dos.writeUTF(action.value);
  }

  public static LogCatalogFormat.LogAction.AddNamespaceProperty readAddNamespaceProperty(
      DataInputStream dis) throws IOException {
    int nsid = dis.readInt();
    int version = dis.readInt();
    String key = dis.readUTF();
    String value = dis.readUTF();
    return new LogCatalogFormat.LogAction.AddNamespaceProperty(nsid, version, key, value);
  }

  public static void writeDropNamespaceProperty(
      LogCatalogFormat.LogAction.DropNamespaceProperty action, DataOutputStream dos)
      throws IOException {
    dos.writeByte(LogCatalogFormat.LogAction.Type.DROP_NAMESPACE_PROPERTY.opcode);
    dos.writeInt(action.nsid);
    dos.writeInt(action.version);
    dos.writeUTF(action.key);
  }

  public static LogCatalogFormat.LogAction.DropNamespaceProperty readDropNamespaceProperty(
      DataInputStream dis) throws IOException {
    int nsid = dis.readInt();
    int version = dis.readInt();
    String key = dis.readUTF();
    return new LogCatalogFormat.LogAction.DropNamespaceProperty(nsid, version, key);
  }

  // ==================== Table Serialization ====================

  public static void writeCreateTable(
      LogCatalogFormat.LogAction.CreateTable action, DataOutputStream dos) throws IOException {
    dos.writeByte(LogCatalogFormat.LogAction.Type.CREATE_TABLE.opcode);
    dos.writeUTF(action.name);
    dos.writeInt(action.logTblId);
    dos.writeInt(action.logTblVersion);
    dos.writeInt(action.logNsid);
    dos.writeInt(action.logNsVersion);
    dos.writeUTF(action.location);
  }

  public static LogCatalogFormat.LogAction.CreateTable readCreateTable(DataInputStream dis)
      throws IOException {
    String name = dis.readUTF();
    int tblId = dis.readInt();
    int tblVersion = dis.readInt();
    int nsid = dis.readInt();
    int nsVersion = dis.readInt();
    String location = dis.readUTF();
    return new LogCatalogFormat.LogAction.CreateTable(name, tblId, tblVersion, nsid, nsVersion, location);
  }

  public static void writeDropTable(
      LogCatalogFormat.LogAction.DropTable action, DataOutputStream dos) throws IOException {
    dos.writeByte(LogCatalogFormat.LogAction.Type.DROP_TABLE.opcode);
    dos.writeInt(action.tblId);
    dos.writeInt(action.version);
  }

  public static LogCatalogFormat.LogAction.DropTable readDropTable(DataInputStream dis)
      throws IOException {
    int tblId = dis.readInt();
    int version = dis.readInt();
    return new LogCatalogFormat.LogAction.DropTable(tblId, version);
  }

  public static void writeReadTable(
      LogCatalogFormat.LogAction.ReadTable action, DataOutputStream dos) throws IOException {
    dos.writeByte(LogCatalogFormat.LogAction.Type.READ_TABLE.opcode);
    dos.writeInt(action.tblId);
    dos.writeInt(action.version);
  }

  public static LogCatalogFormat.LogAction.ReadTable readReadTable(DataInputStream dis)
      throws IOException {
    int tblId = dis.readInt();
    int version = dis.readInt();
    return new LogCatalogFormat.LogAction.ReadTable(tblId, version);
  }

  public static void writeUpdateTable(
      LogCatalogFormat.LogAction.UpdateTable action, DataOutputStream dos) throws IOException {
    dos.writeByte(LogCatalogFormat.LogAction.Type.UPDATE_TABLE.opcode);
    dos.writeInt(action.tblId);
    dos.writeInt(action.version);
    dos.writeUTF(action.location);
  }

  public static LogCatalogFormat.LogAction.UpdateTable readUpdateTable(DataInputStream dis)
      throws IOException {
    int tblId = dis.readInt();
    int version = dis.readInt();
    String location = dis.readUTF();
    return new LogCatalogFormat.LogAction.UpdateTable(tblId, version, location);
  }

  // ==================== Transaction Serialization ====================

  public static void writeTransaction(
      LogCatalogFormat.LogAction.Transaction txn, DataOutputStream dos) throws IOException {
    if (txn.actions.isEmpty()) {
      return;
    }
    dos.writeByte(LogCatalogFormat.LogAction.Type.TRANSACTION.opcode);
    dos.writeLong(txn.txnId.getMostSignificantBits());
    dos.writeLong(txn.txnId.getLeastSignificantBits());
    dos.writeBoolean(txn.sealed);
    dos.writeInt(txn.actions.size());
    for (LogCatalogFormat.LogAction action : txn.actions) {
      writeAction(action, dos);
    }
  }

  public static LogCatalogFormat.LogAction.Transaction readTransaction(DataInputStream dis)
      throws IOException {
    long msb = dis.readLong();
    long lsb = dis.readLong();
    final UUID uuid = new UUID(msb, lsb);
    boolean sealed = dis.readBoolean();
    final int nActions = dis.readInt();
    List<LogCatalogFormat.LogAction> actions = new ArrayList<>(nActions);
    for (int i = 0; i < nActions; ++i) {
      LogCatalogFormat.LogAction.Type type =
          LogCatalogFormat.LogAction.Type.from(dis.readByte());
      switch (type) {
        case CREATE_TABLE:
          actions.add(readCreateTable(dis));
          break;
        case UPDATE_TABLE:
          actions.add(readUpdateTable(dis));
          break;
        case READ_TABLE:
          actions.add(readReadTable(dis));
          break;
        case DROP_TABLE:
          actions.add(readDropTable(dis));
          break;
        case CREATE_NAMESPACE:
          actions.add(readCreateNamespace(dis));
          break;
        case ADD_NAMESPACE_PROPERTY:
          actions.add(readAddNamespaceProperty(dis));
          break;
        case DROP_NAMESPACE_PROPERTY:
          actions.add(readDropNamespaceProperty(dis));
          break;
        case DROP_NAMESPACE:
          actions.add(readDropNamespace(dis));
          break;
        case TRANSACTION:
          throw new IllegalStateException("Nested transactions are not supported");
        default:
          throw new IllegalArgumentException("Unknown action type in transaction: " + type);
      }
    }
    return new LogCatalogFormat.LogAction.Transaction(uuid, actions, sealed);
  }

  // ==================== Generic Action Serialization ====================

  /** Writes any LogAction to the output stream with its opcode prefix. */
  public static void writeAction(LogCatalogFormat.LogAction action, DataOutputStream dos)
      throws IOException {
    if (action instanceof LogCatalogFormat.LogAction.Checkpoint) {
      writeCheckpoint((LogCatalogFormat.LogAction.Checkpoint) action, dos);
    } else if (action instanceof LogCatalogFormat.LogAction.CreateNamespace) {
      writeCreateNamespace((LogCatalogFormat.LogAction.CreateNamespace) action, dos);
    } else if (action instanceof LogCatalogFormat.LogAction.DropNamespace) {
      writeDropNamespace((LogCatalogFormat.LogAction.DropNamespace) action, dos);
    } else if (action instanceof LogCatalogFormat.LogAction.AddNamespaceProperty) {
      writeAddNamespaceProperty((LogCatalogFormat.LogAction.AddNamespaceProperty) action, dos);
    } else if (action instanceof LogCatalogFormat.LogAction.DropNamespaceProperty) {
      writeDropNamespaceProperty((LogCatalogFormat.LogAction.DropNamespaceProperty) action, dos);
    } else if (action instanceof LogCatalogFormat.LogAction.CreateTable) {
      writeCreateTable((LogCatalogFormat.LogAction.CreateTable) action, dos);
    } else if (action instanceof LogCatalogFormat.LogAction.DropTable) {
      writeDropTable((LogCatalogFormat.LogAction.DropTable) action, dos);
    } else if (action instanceof LogCatalogFormat.LogAction.ReadTable) {
      writeReadTable((LogCatalogFormat.LogAction.ReadTable) action, dos);
    } else if (action instanceof LogCatalogFormat.LogAction.UpdateTable) {
      writeUpdateTable((LogCatalogFormat.LogAction.UpdateTable) action, dos);
    } else if (action instanceof LogCatalogFormat.LogAction.Transaction) {
      writeTransaction((LogCatalogFormat.LogAction.Transaction) action, dos);
    } else {
      throw new IllegalArgumentException("Unknown action type: " + action.getClass());
    }
  }

  /** Reads the next action from the checkpoint stream (expects opcode prefix). */
  public static LogCatalogFormat.LogAction readCheckpointAction(DataInputStream dis)
      throws IOException {
    LogCatalogFormat.LogAction.Type type = LogCatalogFormat.LogAction.Type.from(dis.readByte());
    switch (type) {
      case CHECKPOINT:
        return readCheckpoint(dis);
      case CREATE_NAMESPACE:
        return readCreateNamespace(dis);
      case ADD_NAMESPACE_PROPERTY:
        return readAddNamespaceProperty(dis);
      case CREATE_TABLE:
        return readCreateTable(dis);
      default:
        throw new IllegalArgumentException("Unknown action type in checkpoint: " + type);
    }
  }

  /** Reads the next transaction from the log stream (expects TRANSACTION opcode prefix). */
  public static LogCatalogFormat.LogAction.Transaction readLogTransaction(DataInputStream dis)
      throws IOException {
    LogCatalogFormat.LogAction.Type type = LogCatalogFormat.LogAction.Type.from(dis.readByte());
    switch (type) {
      case TRANSACTION:
        return readTransaction(dis);
      case CHECKPOINT:
      case CREATE_TABLE:
      case UPDATE_TABLE:
      case READ_TABLE:
      case DROP_TABLE:
      case CREATE_NAMESPACE:
      case ADD_NAMESPACE_PROPERTY:
      case DROP_NAMESPACE_PROPERTY:
      case DROP_NAMESPACE:
        throw new IllegalStateException("Action not in transaction");
      default:
        throw new IllegalArgumentException("Unknown action type: " + type);
    }
  }

  // ==================== Seal/Unseal ====================

  /**
   * Marks a serialized transaction as sealed by setting the sealed flag byte.
   *
   * @param serTxn serialized transaction bytes
   */
  public static void sealTransaction(byte[] serTxn) {
    serTxn[SEALED_OFFSET] = 1;
  }

  /**
   * Clears the sealed flag on a serialized transaction.
   *
   * @param serTxn serialized transaction bytes
   */
  public static void unsealTransaction(byte[] serTxn) {
    serTxn[SEALED_OFFSET] = 0;
  }

  // ==================== Iterators ====================

  public static Iterable<LogCatalogFormat.LogAction> checkpointIterable(final DataInputStream dis) {
    return () -> new CheckpointIterator(dis);
  }

  public static Iterable<LogCatalogFormat.LogAction.Transaction> logIterable(
      final DataInputStream dis) {
    return () -> new LogIterator(dis);
  }

  /** Iterator over checkpoint actions. */
  static class CheckpointIterator implements Iterator<LogCatalogFormat.LogAction> {
    private final DataInputStream dis;

    CheckpointIterator(DataInputStream dis) {
      this.dis = dis;
    }

    @Override
    public boolean hasNext() {
      try {
        return dis.available() > 0;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public LogCatalogFormat.LogAction next() {
      try {
        return readCheckpointAction(dis);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  /** Iterator over log transactions. */
  static class LogIterator implements Iterator<LogCatalogFormat.LogAction.Transaction> {
    private final DataInputStream dis;

    LogIterator(DataInputStream dis) {
      this.dis = dis;
    }

    @Override
    public boolean hasNext() {
      try {
        return dis.available() > 0;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public LogCatalogFormat.LogAction.Transaction next() {
      try {
        return readLogTransaction(dis);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  // ==================== Committed Transaction Serialization ====================

  public static void readCommittedTransactions(Set<UUID> committedTxn, byte[] txnBytes) {
    try (java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(txnBytes);
        DataInputStream txndis = new DataInputStream(bais)) {
      int nTxn = txndis.readInt();
      for (int i = 0; i < nTxn; ++i) {
        long msb = txndis.readLong();
        long lsb = txndis.readLong();
        committedTxn.add(new UUID(msb, lsb));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static void writeCommittedTransactions(Set<UUID> committedTxn, DataOutputStream txndos)
      throws IOException {
    txndos.writeInt(committedTxn.size());
    for (UUID u : committedTxn.stream().sorted().collect(Collectors.toList())) {
      txndos.writeLong(u.getMostSignificantBits());
      txndos.writeLong(u.getLeastSignificantBits());
    }
  }

  // ==================== Full Checkpoint Writing ====================

  /**
   * Writes a complete checkpoint for the given catalog file.
   *
   * @param catalogFile the catalog file to checkpoint
   * @param out the output stream to write to
   */
  public static void writeFullCheckpoint(
      LogCatalogFile catalogFile, OutputStream out) throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream chk = new DataOutputStream(out)) {
      final byte[] chkData;
      try (DataOutputStream chkdos = new DataOutputStream(bos)) {
        for (LogCatalogFormat.LogAction action : catalogFile.checkpointStream()) {
          writeAction(action, chkdos);
        }
        chkData = bos.toByteArray();
      }

      bos.reset();

      final byte[] txnData;
      try (DataOutputStream txndos = new DataOutputStream(bos)) {
        writeCommittedTransactions(catalogFile.getCommittedTransactions(), txndos);
        txnData = bos.toByteArray();
      }
      final LogCatalogFormat.LogAction.Checkpoint chkAction =
          new LogCatalogFormat.LogAction.Checkpoint(
              catalogFile.uuid(),
              catalogFile.getNextNsid(),
              catalogFile.getNextTblid(),
              chkData.length,
              0,
              txnData.length);
      writeCheckpoint(chkAction, chk);
      out.write(chkData);
      out.write(txnData);
    }
  }

  // ==================== Transaction Byte Conversion ====================

  public static byte[] transactionToBytes(LogCatalogFormat.LogAction.Transaction txn) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos)) {
      writeTransaction(txn, dos);
      return bos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize transaction", e);
    }
  }
}
