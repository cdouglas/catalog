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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.util.LimitInputStream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

@SuppressWarnings("checkstyle:VisibilityModifier")
public class LogCatalogFormat
    implements CatalogFormat<LogCatalogFormat.LogCatalogFile, LogCatalogFormat.Mut> {
  // UUID generation
  private static final Random random = new Random();

  public LogCatalogFormat() {
    this(Collections.emptyMap());
  }

  public LogCatalogFormat(Map<String, String> properties) {}

  @Override
  public CatalogFile.Mut<LogCatalogFile, Mut> empty(InputFile input) {
    return new Mut(input);
  }

  @Override
  public CatalogFile.Mut<LogCatalogFile, Mut> from(CatalogFile other) {
    if (!(other instanceof LogCatalogFile)) {
      throw new IllegalArgumentException("Cannot convert to LogCatalogFile: " + other);
    }
    return new Mut((LogCatalogFile) other);
  }

  @Override
  public LogCatalogFile read(SupportsAtomicOperations fileIO, InputFile catalogLocation) {
    // intentionally drop metadata cached on InputFile
    InputFile refresh = fileIO.newInputFile(catalogLocation.location());
    Mut catalog = new Mut(refresh);
    try (SeekableInputStream in = refresh.newStream()) {
      return readInternal(catalog, in, (int) refresh.getLength());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static LogCatalogFile readInternal(Mut catalog, InputStream in) throws IOException {
    return readInternal(catalog, in, Integer.MAX_VALUE);
  }

  // TODO this is written very... badly.
  @VisibleForTesting
  static LogCatalogFile readInternal(Mut catalog, InputStream in, int catalogLen)
      throws IOException {
    try (DataInputStream din = new DataInputStream(in)) {
      if (din.readByte() != LogAction.Type.CHECKPOINT.opcode) {
        throw new IllegalStateException("Invalid magic bits");
      }
      LogAction.Checkpoint chk = LogAction.Checkpoint.read(din);
      chk.apply(catalog);
      byte[] chkBytes = new byte[(int) chk.chkLen];
      IOUtil.readFully(in, chkBytes, 0, chkBytes.length);
      InputStream chkStream = new DataInputStream(new ByteArrayInputStream(chkBytes));
      for (LogAction action : LogAction.chkIterator(new DataInputStream(chkStream))) {
        // no validation necessary in this interval
        action.apply(catalog);
      }
      if (chk.tblEmbedEnd != 0) {
        // TODO embed table region
        throw new IllegalStateException("TODO");
      }
      byte[] txnBytes = new byte[(int) chk.committedTxnLen];
      IOUtil.readFully(in, txnBytes, 0, txnBytes.length);
      catalog.committedTransactionBytes(txnBytes);
      final InputStream logStream;
      if (catalogLen == Integer.MAX_VALUE) {
        // TODO HACK to get around current callers in tests backed by byte arrays
        logStream = new LimitInputStream(in, Integer.MAX_VALUE);
      } else {
        byte[] logBytes =
            new byte
                [catalogLen - chk.length() - chk.chkLen - chk.tblEmbedEnd - chk.committedTxnLen];
        IOUtil.readFully(in, logBytes, 0, logBytes.length);
        logStream = new ByteArrayInputStream(logBytes);
      }
      for (LogAction.Transaction txn : LogAction.logIterator(new DataInputStream(logStream))) {
        if (txn.verify(catalog)) {
          txn.apply(catalog);
        }
        if (txn.sealed) {
          catalog.setSealed();
          break;
        }
      }
      return catalog.merge();
    }
  }

  // Log format
  // <len><op><payload>
  abstract static class LogAction {
    enum Type {
      CHECKPOINT(
          0), // <UUID> <next_nsid> <next_tblid> <chk_end> <tbl_embed_end> <committed_txn_end>
      CREATE_TABLE(1), // <nsid> <ns_ver> <name> <location>
      UPDATE_TABLE(2), // <tbl_id> <tbl_ver> <location>
      READ_TABLE(3), // <tbl_id> <tbl_ver>
      DROP_TABLE(4), // <tbl_id> <tbl_ver>
      CREATE_NAMESPACE(5), // <parent_id> <parent_version> <name>
      DROP_NAMESPACE(6), // <nsid> <version>
      ADD_NAMESPACE_PROPERTY(7), // <nsid> <version> <key> <value>
      DROP_NAMESPACE_PROPERTY(8), // <nsid> <version> <key>
      TRANSACTION(9); // <txid> <sealed> <n_actions> <action>*

      // TODO YOU FUCKING IDIOT
      // TODO include READ_TABLE operations as constraints on serializable transactions

      final int opcode;

      Type(int opcode) {
        this.opcode = opcode;
      }

      static Type from(int opcode) {
        for (Type t : Type.values()) {
          if (t.opcode == opcode) {
            return t;
          }
        }
        throw new IllegalArgumentException("Unknown opcode: " + opcode);
      }
    }

    abstract boolean verify(Mut catalog);

    abstract void apply(Mut catalog);

    abstract void write(DataOutputStream dos) throws IOException;

    static class Checkpoint extends LogAction {
      final UUID catalogUUID;
      final int nextNsid;
      final int nextTblid;
      final int chkLen;
      final int tblEmbedEnd;
      final int committedTxnLen;

      Checkpoint(
          UUID catalogUUID,
          int nextNsid,
          int nextTblid,
          int chkLen,
          int tblEmbedEnd,
          int committedTxnLen) {
        this.catalogUUID = catalogUUID;
        this.nextNsid = nextNsid;
        this.nextTblid = nextTblid;
        this.chkLen = chkLen;
        this.tblEmbedEnd = tblEmbedEnd;
        this.committedTxnLen = committedTxnLen;
      }

      int length() {
        // opcode UUID [fields]
        return 1 + 16 + 5 * Integer.BYTES;
      }

      @Override
      boolean verify(Mut catalog) {
        return true;
      }

      @Override
      void apply(Mut catalog) {
        catalog.setGlobals(catalogUUID, nextNsid, nextTblid);
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        dos.writeByte(Type.CHECKPOINT.opcode);
        // TODO write additional, magic bits + version
        dos.writeLong(catalogUUID.getMostSignificantBits());
        dos.writeLong(catalogUUID.getLeastSignificantBits());
        dos.writeInt(nextNsid);
        dos.writeInt(nextTblid);
        dos.writeInt(chkLen);
        dos.writeInt(tblEmbedEnd);
        dos.writeInt(committedTxnLen);
      }

      static Checkpoint read(DataInputStream dis) throws IOException {
        long msb = dis.readLong();
        long lsb = dis.readLong();
        UUID catalogUUID = new UUID(msb, lsb);
        int nextNsid = dis.readInt();
        int nextTblid = dis.readInt();
        int chkLen = dis.readInt();
        int tblEmbedEnd = dis.readInt();
        int committedTxnLen = dis.readInt();
        return new Checkpoint(
            catalogUUID, nextNsid, nextTblid, chkLen, tblEmbedEnd, committedTxnLen);
      }
    }

    static class CreateNamespace extends LogAction {
      private static final int LATE_BIND = -1;
      final String name;
      final int logNsid;
      final int logVersion;
      final int logParentId;
      final int logParentVersion;

      // TODO clean up constructors...

      // log, parent created in this transaction
      CreateNamespace(String name, int logVersion, int logParentId) {
        this(name, logVersion, LATE_BIND, logParentId, LATE_BIND);
      }

      // log, parent exists
      CreateNamespace(String name, int logNsid, int logParentId, int logParentVersion) {
        this(name, logNsid, LATE_BIND, logParentId, logParentVersion);
      }

      // checkpoint
      CreateNamespace(
          String name, int logNsid, int logVersion, int logParentId, int logParentVersion) {
        this.name = name;
        this.logNsid = logNsid;
        this.logVersion = logVersion;
        this.logParentId = logParentId;
        this.logParentVersion = logParentVersion;
      }

      @Override
      boolean verify(Mut catalog) {
        // concurrent creates are conflicts, but can be retried
        if (logParentVersion < 0) {
          return true;
        }
        Integer version = catalog.nsVersion.get(logParentId);
        return version != null && version == logParentVersion;
      }

      @Override
      void apply(Mut catalog) {
        // increment parent version, assign uniq nsid
        final int nsid, parentId, version;
        if (logNsid < 0) {
          // assign late-bound NSID, record remap
          nsid = catalog.remap(logNsid);
          parentId = logParentId < 0 ? catalog.nsRemap.get(logParentId) : logParentId;
          version = 1;
          catalog.nsVersion.compute(
              parentId,
              (k, ver) -> {
                if (null == ver) {
                  ver = catalog.original.nsVersion.get(k);
                  Preconditions.checkNotNull(ver, "Parent namespace not found: %s", k);
                }
                return ver + 1;
              });
        } else {
          // restore NSID, version from log (checkpoint)
          nsid = logNsid;
          parentId = logParentId;
          version = logVersion;
        }
        catalog.addNamespaceInternal(name, parentId, nsid, version);
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        dos.writeByte(Type.CREATE_NAMESPACE.opcode);
        dos.writeUTF(name);
        dos.writeInt(logNsid);
        dos.writeInt(logVersion);
        dos.writeInt(logParentId);
        dos.writeInt(logParentVersion);
      }

      static CreateNamespace read(DataInputStream dis) throws IOException {
        String name = dis.readUTF();
        int nsid = dis.readInt();
        int version = dis.readInt();
        int parentId = dis.readInt();
        int parentVersion = dis.readInt();
        return new CreateNamespace(name, nsid, version, parentId, parentVersion);
      }
    }

    static class DropNamespace extends LogAction {
      final int nsid;
      final int version;

      DropNamespace(int nsid, int version) {
        this.nsid = nsid;
        this.version = version;
      }

      @Override
      boolean verify(Mut catalog) {
        Integer version = catalog.nsVersion.get(nsid);
        return version != null && version == this.version;
      }

      @Override
      void apply(Mut catalog) {
        catalog.dropNamespaceInternal(nsid);
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        dos.writeByte(Type.DROP_NAMESPACE.opcode);
        dos.writeInt(nsid);
        dos.writeInt(version);
      }

      static DropNamespace read(DataInputStream dis) throws IOException {
        int nsid = dis.readInt();
        int version = dis.readInt();
        return new DropNamespace(nsid, version);
      }
    }

    static class AddNamespaceProperty extends LogAction {
      private static final int LATE_BIND = -1;
      final int logNsid;
      final int logVersion;
      final String key;
      final String value;

      // checkpoint, ns created in txn
      AddNamespaceProperty(int logNsid, String key, String value) {
        this(logNsid, LATE_BIND, key, value);
      }

      // log, ns exists
      AddNamespaceProperty(int logNsid, int logVersion, String key, String value) {
        this.logNsid = logNsid;
        this.logVersion = logVersion;
        this.key = key;
        this.value = value;
      }

      @Override
      boolean verify(Mut catalog) {
        if (logVersion < 0) {
          return true;
        }
        Integer version = catalog.nsVersion.get(logNsid);
        return version != null && version == this.logVersion;
      }

      @Override
      void apply(Mut catalog) {
        final int nsid;
        if (logNsid < 0) {
          // created with namespace; don't increment the namespace version
          nsid = catalog.nsRemap.get(logNsid);
        } else {
          nsid = logNsid;
          if (logVersion >= 0) {
            // assign (possibly multiple times) updated version
            catalog.nsVersion.put(nsid, logVersion + 1);
          }
        }
        catalog.addNamespacePropertyInternal(nsid, key, value);
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        dos.writeByte(Type.ADD_NAMESPACE_PROPERTY.opcode);
        dos.writeInt(logNsid);
        dos.writeInt(logVersion);
        dos.writeUTF(key);
        dos.writeUTF(value);
      }

      static AddNamespaceProperty read(DataInputStream dis) throws IOException {
        int nsid = dis.readInt();
        int version = dis.readInt();
        String key = dis.readUTF();
        String value = dis.readUTF();
        return new AddNamespaceProperty(nsid, version, key, value);
      }
    }

    static class DropNamespaceProperty extends LogAction {
      final int nsid;
      final int version;
      final String key;

      DropNamespaceProperty(int nsid, int version, String key) {
        this.nsid = nsid;
        this.version = version;
        this.key = key;
      }

      @Override
      boolean verify(Mut catalog) {
        Integer version = catalog.nsVersion.get(nsid);
        return version != null && version == this.version;
      }

      @Override
      void apply(Mut catalog) {
        catalog.nsVersion.put(nsid, version + 1);
        catalog.dropNamespacePropertyInternal(nsid, key);
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        dos.writeByte(Type.DROP_NAMESPACE_PROPERTY.opcode);
        dos.writeInt(nsid);
        dos.writeInt(version);
        dos.writeUTF(key);
      }

      static DropNamespaceProperty read(DataInputStream dis) throws IOException {
        int nsid = dis.readInt();
        int version = dis.readInt();
        String key = dis.readUTF();
        return new DropNamespaceProperty(nsid, version, key);
      }
    }

    static class CreateTable extends LogAction {
      private static final int LATE_BIND = -1;
      final String name;
      final int logTblId;
      final int logTblVersion;
      final int logNsid;
      final int logNsVersion;
      final String location;

      // Namespace created in this transaction
      CreateTable(String name, int logNsid, String location) {
        this(name, LATE_BIND, 1, logNsid, LATE_BIND, location);
        Preconditions.checkArgument(logNsid < 0, "Namespace must be late-bound");
      }

      // Namespace exists
      CreateTable(String name, int logNsid, int logNsVersion, String location) {
        this(name, LATE_BIND, 1, logNsid, logNsVersion, location);
      }

      // created from checkpoint
      CreateTable(
          String name,
          int logTblId,
          int logTblVersion,
          int logNsid,
          int logNsVersion,
          String location) {
        this.name = name;
        this.logTblId = logTblId;
        this.logTblVersion = logTblVersion;
        this.logNsid = logNsid;
        this.logNsVersion = logNsVersion;
        this.location = location;
      }

      @Override
      boolean verify(Mut catalog) {
        if (logNsVersion < 0) {
          // contained in a namespace created in this transaction
          return true;
        }
        Integer version = catalog.nsVersion.get(logNsid);
        return version != null && version == logNsVersion;
      }

      @Override
      void apply(Mut catalog) {
        // restore NSID, version from log (checkpoint)
        // TODO ID remapping needs an abstraction
        // TODO reaching into internal maps is grotesque, clean this up
        // TODO XXX create table should also increment the namespace version, so concurrent creates
        // fail validation
        final int nsid = logNsVersion < 0 ? catalog.nsRemap.get(logNsid) : logNsid;
        final int tblId = this.logTblId == LATE_BIND ? catalog.nextTblid++ : this.logTblId;
        catalog.addTableInternal(tblId, nsid, logTblVersion, name, location);
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        dos.writeByte(Type.CREATE_TABLE.opcode);
        dos.writeUTF(name);
        dos.writeInt(logTblId);
        dos.writeInt(logTblVersion);
        dos.writeInt(logNsid);
        dos.writeInt(logNsVersion);
        dos.writeUTF(location);
      }

      static CreateTable read(DataInputStream dis) throws IOException {
        String name = dis.readUTF();
        int tblId = dis.readInt();
        int tblVersion = dis.readInt();
        int nsid = dis.readInt();
        int nsVersion = dis.readInt();
        String location = dis.readUTF();
        return new CreateTable(name, tblId, tblVersion, nsid, nsVersion, location);
      }
    }

    static class DropTable extends LogAction {
      final int tblId;
      final int version;

      DropTable(int tblId, int version) {
        this.tblId = tblId;
        this.version = version;
      }

      @Override
      boolean verify(Mut catalog) {
        Integer version = catalog.tblVersion.get(tblId);
        return version != null && version == this.version;
      }

      @Override
      void apply(Mut catalog) {
        catalog.dropTableInternal(tblId);
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        dos.writeByte(Type.DROP_TABLE.opcode);
        dos.writeInt(tblId);
        dos.writeInt(version);
      }

      static DropTable read(DataInputStream dis) throws IOException {
        int tblId = dis.readInt();
        int version = dis.readInt();
        return new DropTable(tblId, version);
      }
    }

    static class ReadTable extends LogAction {
      final int tblId;
      final int version;

      ReadTable(int tblId, int version) {
        this.tblId = tblId;
        this.version = version;
      }

      @Override
      boolean verify(Mut catalog) {
        Integer version = catalog.tblVersion.get(tblId);
        return version != null && version == this.version;
      }

      @Override
      void apply(Mut catalog) {
        // do nothing
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        dos.writeByte(Type.READ_TABLE.opcode);
        dos.writeInt(tblId);
        dos.writeInt(version);
      }

      static ReadTable read(DataInputStream dis) throws IOException {
        int tblId = dis.readInt();
        int version = dis.readInt();
        return new ReadTable(tblId, version);
      }
    }

    static class UpdateTable extends LogAction {
      final int tblId;
      final int version;
      final String location;

      UpdateTable(int tblId, int version, String location) {
        this.tblId = tblId;
        this.version = version;
        this.location = location;
      }

      @Override
      boolean verify(Mut catalog) {
        Integer version = catalog.tblVersion.get(tblId);
        return version != null && version == this.version;
      }

      @Override
      void apply(Mut catalog) {
        catalog.updateTableInternal(tblId, version + 1, location);
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        dos.writeByte(Type.UPDATE_TABLE.opcode);
        dos.writeInt(tblId);
        dos.writeInt(version);
        dos.writeUTF(location);
      }

      static UpdateTable read(DataInputStream dis) throws IOException {
        int tblId = dis.readInt();
        int version = dis.readInt();
        String location = dis.readUTF();
        return new UpdateTable(tblId, version, location);
      }
    }

    static class Transaction extends LogAction {
      boolean sealed;
      final UUID txnId;
      final List<LogAction> actions;

      Transaction(List<LogAction> actions) {
        this(generate(), actions, false);
      }

      Transaction(UUID txnId, List<LogAction> actions, boolean sealed) {
        this.txnId = txnId;
        this.actions = actions;
        this.sealed = sealed;
      }

      boolean isSealed() {
        return sealed;
      }

      void seal() {
        this.sealed = true;
      }

      @Override
      boolean verify(Mut catalog) {
        return actions.stream().map(a -> a.verify(catalog)).reduce(true, (a, b) -> a && b);
      }

      @Override
      void apply(Mut catalog) {
        actions.forEach(a -> a.apply(catalog));
        catalog.addCommittedTxn(txnId);
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        if (actions.isEmpty()) {
          return;
        }
        dos.writeByte(Type.TRANSACTION.opcode);
        dos.writeLong(txnId.getMostSignificantBits());
        dos.writeLong(txnId.getLeastSignificantBits());
        dos.writeBoolean(sealed);
        dos.writeInt(actions.size());
        for (LogAction action : actions) {
          action.write(dos);
        }
      }

      static void seal(byte[] serTxn) {
        serTxn[17] = 1;
      }

      static void unseal(byte[] serTxn) {
        serTxn[17] = 0;
      }

      static Transaction read(DataInputStream dis) throws IOException {
        long msb = dis.readLong();
        long lsb = dis.readLong();
        final UUID uuid = new UUID(msb, lsb);
        boolean sealed = dis.readBoolean();
        final int nActions = dis.readInt();
        List<LogAction> actions = new ArrayList<>(nActions);
        for (int i = 0; i < nActions; ++i) {
          Type type = Type.from(dis.readByte());
          switch (type) {
            case CREATE_TABLE:
              actions.add(CreateTable.read(dis));
              break;
            case UPDATE_TABLE:
              actions.add(UpdateTable.read(dis));
              break;
            case READ_TABLE:
              actions.add(ReadTable.read(dis));
              break;
            case DROP_TABLE:
              actions.add(DropTable.read(dis));
              break;
            case CREATE_NAMESPACE:
              actions.add(CreateNamespace.read(dis));
              break;
            case ADD_NAMESPACE_PROPERTY:
              actions.add(AddNamespaceProperty.read(dis));
              break;
            case DROP_NAMESPACE_PROPERTY:
              actions.add(DropNamespaceProperty.read(dis));
              break;
            case DROP_NAMESPACE:
              actions.add(DropNamespace.read(dis));
              break;
            case TRANSACTION:
              throw new IllegalStateException("Nested transactions are not supported");
          }
        }
        return new Transaction(uuid, actions, sealed);
      }
    }

    static Iterable<LogAction> chkIterator(final DataInputStream dis) throws IOException {
      return () -> new ChkStream(dis);
    }

    static Iterable<Transaction> logIterator(final DataInputStream dis) throws IOException {
      return () -> new LogStream(dis);
    }

    static class ChkStream implements Iterator<LogAction> {
      private final DataInputStream dis;

      ChkStream(DataInputStream dis) {
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
      public LogAction next() {
        try {
          Type type = Type.from(dis.readByte());
          switch (type) {
            case CHECKPOINT:
              return Checkpoint.read(dis);
            case CREATE_NAMESPACE:
              return CreateNamespace.read(dis);
            case ADD_NAMESPACE_PROPERTY:
              return AddNamespaceProperty.read(dis);
            case CREATE_TABLE:
              return CreateTable.read(dis);
            default:
              throw new IllegalArgumentException("Unknown action type: " + type);
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }

    static class LogStream implements Iterator<Transaction> {
      private final DataInputStream dis;

      LogStream(DataInputStream dis) {
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
      public Transaction next() {
        try {
          Type type = Type.from(dis.readByte());
          switch (type) {
            case TRANSACTION:
              return Transaction.read(dis);
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
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }
  }

  // TODO move this to LogCatalogFile
  public static class Mut extends CatalogFile.Mut<LogCatalogFile, Mut> {
    // namespace IDs are internal to the catalog format

    private UUID uuid = null;
    private int nextNsid = 1;
    private int nextTblid = 1;
    private boolean sealed = false;
    private final Set<UUID> committedTxn = Sets.newHashSet();

    // CREATE TABLE namespaces (
    //   nsid INT,
    //   version INT,
    //   parentId INT,
    //   name VARCHAR(255) NOT NULL,
    //   PRIMARY KEY (nsid, version),
    //   FOREIGN KEY (parentId) REFERENCES namespaces(nsid)
    // );
    private final Map<Namespace, Integer> nsids = Maps.newHashMap();
    private final Map<Integer, Integer> nsVersion = Maps.newHashMap();
    private final Map<Integer, Integer> nsRemap = Maps.newHashMap();
    private final Map<Integer, Namespace> nsLookup = Maps.newHashMap();

    // CREATE TABLE ns_prop (
    //   nsid INT,
    //   key VARCHAR(255) NOT NULL,
    //   value VARCHAR(1024) NOT NULL
    // );
    private final Map<Integer, Map<String, String>> nsProperties = Maps.newHashMap();

    // CREATE TABLE tables (
    //   tbl_id INT,
    //   version INT,
    //   nsid INT,
    //   name VARCHAR(255) NOT NULL,
    //   location VARCHAR(1024 NOT NULL),
    //   PRIMARY KEY (tbl_id, version),
    //   FOREIGN KEY (nsid) REFERENCES namespaces(nsid)
    // );
    private final Map<TableIdentifier, Integer> tblIds = Maps.newHashMap();
    private final Map<Integer, Integer> tblVersion = Maps.newHashMap();
    private final Map<Integer, String> tblLocations = Maps.newHashMap();

    // empty
    Mut(InputFile input) {
      this(new LogCatalogFile(input));
    }

    // empty w/ checkpoint data (useful for merge)
    Mut(InputFile input, UUID uuid, int nextNsid, int nextTblid) {
      this(new LogCatalogFile(input));
      this.uuid = uuid;
      this.nextNsid = nextNsid;
      this.nextTblid = nextTblid;
    }

    // changes to be applied to this catalog
    Mut(LogCatalogFile other) {
      super(other);
    }

    void setSealed() {
      this.sealed = true;
    }

    int remap(int nsid) {
      Preconditions.checkArgument(nsid < 0, "Attempting to remap non-virtual namespace: %d", nsid);
      final int assignedNsid = nextNsid++;
      nsRemap.put(nsid, assignedNsid);
      return assignedNsid;
    }

    void setGlobals(UUID uuid, int nextNsid, int nextTblid) {
      Preconditions.checkArgument(this.uuid == null, "UUID already set");
      this.uuid = uuid;
      this.nextNsid = nextNsid;
      this.nextTblid = nextTblid;
      // TODO add compaction parameters so clients use the same criteria?
    }

    void addCommittedTxn(UUID txnId) {
      committedTxn.add(txnId);
    }

    void addNamespaceInternal(String name, int parentId, int nsid, int version) {
      Preconditions.checkNotNull(name, "Namespace name cannot be null");
      final Namespace ns;
      if (nsid == 0) {
        Preconditions.checkArgument(nsids.isEmpty(), "Root namespace already exists");
        Preconditions.checkArgument(
            parentId == 0, "Invalid parent id for root namespace: %d", parentId);
        Preconditions.checkArgument(name.isEmpty(), "Invalid name for root namespace: %s", name);
        ns = Namespace.empty();
      } else {
        Namespace parent = nsLookup.get(parentId);
        if (null == parent) {
          parent = original.nsLookup.get(parentId);
          if (null == parent) {
            throw new IllegalStateException("Invalid parent namespace: " + parentId);
          }
        }
        String[] levels = Arrays.copyOf(parent.levels(), parent.levels().length + 1);
        levels[levels.length - 1] = name;
        ns = Namespace.of(levels);
      }
      if (nsids.put(ns, nsid) != null) {
        throw new IllegalStateException("Duplicate namespace: " + ns);
      }
      nsLookup.put(nsid, ns);
      nsVersion.put(nsid, version);
    }

    // add property to namespace without incrementing version
    void addNamespacePropertyInternal(int nsid, String key, String value) {
      Preconditions.checkArgument(nsLookup.containsKey(nsid), "Invalid namespace: %s", nsid);
      Map<String, String> props = nsProperties.computeIfAbsent(nsid, k -> Maps.newHashMap());
      props.put(key, value);
    }

    void dropNamespacePropertyInternal(int nsid, String key) {
      Preconditions.checkArgument(nsLookup.containsKey(nsid));
      Map<String, String> props = nsProperties.get(nsid);
      props.remove(key);
    }

    void dropNamespaceInternal(int nsid) {
      Namespace ns = nsLookup.remove(nsid);
      if (null == ns) {
        throw new IllegalStateException("Invalid namespace: " + nsid);
      }
      nsids.remove(ns);
      nsVersion.remove(nsid);
    }

    void addTableInternal(int tblId, int nsid, int version, String name, String location) {
      Namespace ns = nsLookup.get(nsid);
      if (null == ns) {
        throw new IllegalStateException("Invalid namespace: " + nsid);
      }
      TableIdentifier ti = TableIdentifier.of(ns, name);
      if (tblIds.put(ti, tblId) != null) {
        throw new IllegalStateException("Duplicate table: " + ti);
      }
      tblLocations.put(tblId, location);
      tblVersion.put(tblId, version);
    }

    void dropTableInternal(int tblId) {
      tblLocations.remove(tblId);
      tblVersion.remove(tblId);
      tblIds.values().removeIf(id -> id.equals(tblId));
    }

    void updateTableInternal(int tblId, int version, String location) {
      tblLocations.put(tblId, location);
      tblVersion.put(tblId, version);
    }

    // TODO consider doing a lot of work to make this into a stream::reduce
    // i.e., put some thought into making LogActions composable, use ACI for batching and parallel
    // evaluation
    // or even better, defer all that until you start on Hydro. Table format transactions can be a
    // baseline
    // and you can build a more general-purpose transaction system- including reordering with DBSP-
    // on top of that.

    // store the serialized bytes for committted transactions (lazily resolve)
    void committedTransactionBytes(byte[] committedTxnBytes) {
      LogCatalogFile.readCommittedTxn(committedTxn, committedTxnBytes);
    }

    LogCatalogFile merge() {
      return new LogCatalogFile(
          original.location(),
          uuid,
          nextNsid,
          nextTblid,
          sealed,
          Maps.newHashMap(nsids),
          Maps.newHashMap(nsVersion),
          Maps.newHashMap(nsProperties),
          Maps.newHashMap(tblIds),
          Maps.newHashMap(tblVersion),
          Maps.newHashMap(tblLocations),
          Sets.newHashSet(committedTxn));
    }

    /**
     * Compute a transaction that represents the difference between the original catalog state and
     * the changes applied subsequently.
     */
    LogAction.Transaction diff() {
      List<LogAction> actions = Lists.newArrayList();
      // create/delete namespaces
      int nsVirtId = 0;
      for (Map.Entry<Namespace, Boolean> e :
          namespaces.entrySet().stream()
              .sorted(Comparator.comparing(e -> e.getKey().toString()))
              .collect(Collectors.toList())) {
        final Namespace ns = e.getKey();
        if (e.getValue()) {
          // create
          final Namespace parent = parentOf(ns);
          final Integer parentId = original.nsids.get(parent);
          if (null == parentId) {
            // parent namespace is part of this transaction; assign virt ID
            Preconditions.checkNotNull(
                namespaces.get(parent), "Parent namespace not found: %s", parent);
            nsids.put(ns, --nsVirtId);
            actions.add(new LogAction.CreateNamespace(nameOf(ns), nsVirtId, nsids.get(parent)));
          } else {
            // parent namespace is already in the catalog
            Preconditions.checkNotNull(
                original.nsids.get(parent), "Parent namespace not found: %s", parent);
            nsids.put(ns, --nsVirtId);
            actions.add(
                new LogAction.CreateNamespace(
                    nameOf(ns), nsVirtId, parentId, original.nsVersion.get(parentId)));
          }
        } else {
          // drop
          final int nsid = original.nsids.get(e.getKey());
          actions.add(new LogAction.DropNamespace(nsid, original.nsVersion.get(nsid)));
        }
      }
      // update/delete properties
      // TODO this increments the version on every property change. Not wrong, but excessive.
      for (Map.Entry<Namespace, Map<String, String>> e : namespaceProperties.entrySet()) {
        final Namespace ns = e.getKey();
        final int nsid = original.nsids.getOrDefault(ns, nsids.get(ns));
        for (Map.Entry<String, String> prop : e.getValue().entrySet()) {
          if (prop.getValue() != null) {
            if (nsid < 0) {
              // namespace is created in this transaction; late-bind
              actions.add(new LogAction.AddNamespaceProperty(nsid, prop.getKey(), prop.getValue()));
            } else {
              // namespcae exists; cite version
              actions.add(
                  new LogAction.AddNamespaceProperty(
                      nsid, original.nsVersion.get(nsid), prop.getKey(), prop.getValue()));
            }
          } else {
            actions.add(
                new LogAction.DropNamespaceProperty(
                    nsid, original.nsVersion.get(nsid), prop.getKey()));
          }
        }
      }
      for (Map.Entry<TableIdentifier, String> t : tables.entrySet()) {
        final TableIdentifier ti = t.getKey();
        final String location = t.getValue();
        final Namespace ns = ti.namespace();
        final Integer parentId = original.nsids.get(ns);
        if (location != null) {
          // creating/updating a table
          if (null == parentId) {
            // parent namespace is part of this transaction; assign virt ID
            Preconditions.checkArgument(namespaces.get(ns), "Parent namespace not found: %s", ns);
            actions.add(new LogAction.CreateTable(ti.name(), nsids.get(ns), location));
          } else {
            // parent namespace is already in the catalog
            actions.add(
                new LogAction.CreateTable(
                    ti.name(), parentId, original.nsVersion.get(parentId), location));
          }
        } else {
          // dropping a table
          final int tblId = original.tblIds.get(ti);
          actions.add(new LogAction.DropTable(tblId, original.tblVersion.get(tblId)));
        }
      }
      for (TableIdentifier ti : readTables) {
        if (original.tblIds.containsKey(ti)) {
          // tbl existed when txn started, rely on that version
          final int tblId = original.tblIds.get(ti);
          actions.add(new LogAction.ReadTable(tblId, original.tblVersion.get(tblId)));
        }
        // else: created and read this table, create will fail validation
      }
      for (Map.Entry<TableIdentifier, String> t : tableUpdates.entrySet()) {
        final TableIdentifier ti = t.getKey();
        final String location = t.getValue();
        final int tblId = original.tblIds.get(ti);
        actions.add(new LogAction.UpdateTable(tblId, original.tblVersion.get(tblId), location));
      }
      return new LogAction.Transaction(actions);
    }

    // write the original- which could include a long log- as a checkpoint
    // write the diff as a transaction
    // TODO absurd, redundant computation of merge state
    // TODO ensure InputFile includes accurate length (should be)
    private Optional<LogCatalogFile> tryCAS(
        InputFile current, byte[] txnBytes, SupportsAtomicOperations fileIO) {
      try {
        // SIGH
        Preconditions.checkArgument(current.location().equals(original.location().location()));
        AtomicOutputFile outputFile = fileIO.newOutputFile(current);
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
          original.writeCheckpoint(baos); // original is sealed, create, all the same case
          baos.write(txnBytes);
          final byte[] checkpointBytes = baos.toByteArray();
          try (ByteArrayInputStream serBytes = new ByteArrayInputStream(checkpointBytes)) {
            serBytes.mark(checkpointBytes.length);
            CAS token = outputFile.prepare(() -> serBytes, AtomicOutputFile.Strategy.CAS);
            serBytes.reset();
            InputFile newCatalog = outputFile.writeAtomic(token, () -> serBytes);
            final Mut merged = new Mut(newCatalog);
            // TODO: newCatalog should have the offset of the checkpoint - txn bytes
            return Optional.of(
                LogCatalogFormat.readInternal(merged, new ByteArrayInputStream(checkpointBytes)));
          }
        }
      } catch (SupportsAtomicOperations.CASException e) {
        return Optional.empty();
      } catch (IOException e) {
        throw new CommitFailedException(e, "Cannot commit: %s", e.getMessage());
      }
    }

    // TODO obviously, these should be combined
    private Optional<LogCatalogFile> tryAppend(
        InputFile current,
        LogAction.Transaction txn,
        byte[] txnBytes,
        SupportsAtomicOperations fileIO) {
      try {
        AtomicOutputFile outputFile = fileIO.newOutputFile(current);
        try (ByteArrayInputStream serBytes = new ByteArrayInputStream(txnBytes)) {
          serBytes.mark(txnBytes.length);
          CAS token = outputFile.prepare(() -> serBytes, AtomicOutputFile.Strategy.APPEND);
          serBytes.reset();
          InputFile newCatalog = outputFile.writeAtomic(token, () -> serBytes);
          // TODO temporary; reading back what we wrote
          final Mut merged = new Mut(newCatalog);
          try (SeekableInputStream in = newCatalog.newStream()) {
            return Optional.of(readInternal(merged, in, (int) newCatalog.getLength()));
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
      } catch (SupportsAtomicOperations.AppendException e) {
        // annoyingly, metadata is not included in the error message (i.e., the correct length/etag)
        return Optional.empty();
      } catch (IOException e) {
        throw new UncheckedIOException("Cannot commit: " + e.getMessage(), e);
      }
    }

    static byte[] toBytes(LogCatalogFormat.LogAction.Transaction diffActions) {
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
          DataOutputStream dos = new DataOutputStream(bos)) {
        diffActions.write(dos);
        return bos.toByteArray();
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to write/read diff", e);
      }
    }

    @Override
    public LogCatalogFile commit(SupportsAtomicOperations fileIO) {
      final long IO_ATTEMPTS = 10;
      final long MAX_CATALOG_SIZE = 16L * 1024 * 1024; // TODO from config/global
      LogCatalogFile baseCatalog = original;
      InputFile current = original.location();
      final LogAction.Transaction txn = diff();
      final byte[] txnBytes = toBytes(txn);

      // case 0: initial commit of the catalog
      if (!current.exists()) {
        return tryCAS(current, txnBytes, fileIO)
            .orElseThrow(() -> new CommitFailedException("Cannot commit: catalog creation failed"));
      }

      for (int attempts = 0; attempts < IO_ATTEMPTS; ++attempts) {
        // case 1: original LogCatalogFile is sealed
        if (baseCatalog.sealed) {
          LogAction.Transaction.unseal(txnBytes);
          Optional<LogCatalogFile> rslt = tryCAS(current, txnBytes, fileIO);
          if (rslt.isPresent()) {
            LogCatalogFile newCatalog = rslt.get();
            if (!newCatalog.containsTransaction(txn.txnId)) {
              throw new CommitFailedException("Cannot commit: conflicting, concurrent transaction");
            }
            return newCatalog;
          }
        } else {
          if (current.getLength() + txnBytes.length > MAX_CATALOG_SIZE) {
            LogAction.Transaction.seal(txnBytes);
          }
          Optional<LogCatalogFile> rslt = tryAppend(current, txn, txnBytes, fileIO);
          if (rslt.isPresent()) {
            LogCatalogFile newCatalog = rslt.get();
            if (!newCatalog.containsTransaction(txn.txnId)) {
              throw new CommitFailedException("Cannot commit: conflicting, concurrent transaction");
            }
            return newCatalog;
          }
        }
        long oldLength = current.getLength();
        current = fileIO.newInputFile(current.location());
        if (current.getLength() < oldLength) {
          // TODO are there any metadata that identify the old/new blob? etag always changes
          LogAction.Transaction.unseal(txnBytes);
          Mut catalog = new Mut(current);
          try (SeekableInputStream in = current.newStream()) {
            return readInternal(catalog, in, (int) current.getLength());
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
      }
      throw new CommitFailedException("Cannot commit: exceeded atomic IO attempts");
      // case 2: appending to the end of the file is below the max size
      //   // tryAppend
      // case 3: appending will exceed the max size
      //   // rewrite bytes w/ seal, tryAppend
      // case 4: original file already exceeds threshold, but not sealed?
      //   //  tryCAS for position append is sufficient
      //   //  should append w/ seal instead
    }
  }

  // UUIDv7 generator ; useful for transaction IDs
  // TODO omit 2 bits of entropy to fit align to 9 bytes
  static UUID generate() {
    long timestamp = System.currentTimeMillis();
    long unixTsMs = timestamp & 0xFFFFFFFFFFFFL; // 48 bits for timestamp

    // Randomness: 12 bits for unique sequencing within the millisecond
    long randA = random.nextInt(0x1000) & 0x0FFF; // 12 bits

    // Construct the most significant 64 bits
    long msb = (unixTsMs << 16) | (0x7L << 12) | randA; // Version 7 (0111)

    // 62 bits of randomness + UUID variant
    long randB = random.nextLong() & 0x3FFFFFFFFFFFFFFFL; // 62 bits
    long lsb = (0x2L << 62) | randB; // Variant bits: 10x (RFC 4122)

    return new UUID(msb, lsb);
  }

  public static class LogCatalogFile extends CatalogFile {
    final int nextNsid;
    final int nextTblid;
    final boolean sealed;

    private final Set<UUID> committedTxn;

    private final Map<Namespace, Integer> nsids;
    private final Map<Integer, Integer> nsVersion;
    private final Map<Integer, Namespace> nsLookup;

    private final Map<Integer, Map<String, String>> nsProperties;

    private final Map<TableIdentifier, Integer> tblIds;
    private final Map<Integer, Integer> tblVersion;
    private final Map<Integer, String> tblLocations;

    // empty LogCatalogFile
    LogCatalogFile(InputFile location) {
      super(location);
      this.nextNsid = 1;
      this.nextTblid = 1;
      this.sealed = false;
      this.nsids = Maps.newHashMap();
      this.nsVersion = Maps.newHashMap();
      this.nsProperties = Maps.newHashMap();
      this.tblIds = Maps.newHashMap();
      this.tblVersion = Maps.newHashMap();
      this.tblLocations = Maps.newHashMap();
      this.nsLookup = Maps.newHashMap();
      this.committedTxn = Sets.newHashSet();
      this.nsids.put(Namespace.empty(), 0);
      this.nsVersion.put(0, 1);
      this.nsLookup.put(0, Namespace.empty());
    }

    LogCatalogFile(
        InputFile location,
        UUID catalogUUID,
        int nextNsid,
        int nextTblid,
        boolean sealed,
        Map<Namespace, Integer> nsids,
        Map<Integer, Integer> nsVersion,
        Map<Integer, Map<String, String>> nsProperties,
        Map<TableIdentifier, Integer> tblIds,
        Map<Integer, Integer> tblVersion,
        Map<Integer, String> tblLocations,
        Set<UUID> committedTxn) {
      super(catalogUUID, location);
      this.sealed = sealed;
      this.nextNsid = nextNsid;
      this.nextTblid = nextTblid;
      this.nsids = nsids;
      this.nsVersion = nsVersion;
      this.nsProperties = nsProperties;
      this.tblIds = tblIds;
      this.tblVersion = tblVersion;
      this.tblLocations = tblLocations;
      this.committedTxn = committedTxn;
      this.nsLookup =
          nsids.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
      if (!nsids.containsKey(Namespace.empty())) {
        throw new IllegalStateException("Missing root/empty namespace");
      }
    }

    // TODO move this to format (why does this here?)
    @Override
    public boolean createsHierarchicalNamespaces() {
      return true;
    }

    public boolean containsTransaction(UUID txnId) {
      return committedTxn.contains(txnId);
    }

    @Override
    public String location(TableIdentifier table) {
      final Integer tblId = tblIds.get(table);
      if (tblId == null) {
        return null;
      }
      return tblLocations.get(tblId);
    }

    @Override
    public Set<Namespace> namespaces() {
      return Collections.unmodifiableSet(nsids.keySet());
    }

    @Override
    public boolean containsNamespace(Namespace namespace) {
      return nsids.containsKey(namespace);
    }

    @Override
    public Map<String, String> namespaceProperties(Namespace namespace) {
      final Integer nsid = nsids.get(namespace);
      if (nsid == null) {
        return null;
      }
      return Collections.unmodifiableMap(nsProperties.getOrDefault(nsid, Collections.emptyMap()));
    }

    @Override
    public List<TableIdentifier> tables() {
      return Lists.newArrayList(tblIds.keySet().iterator());
    }

    @Override
    Map<Namespace, Map<String, String>> namespaceProperties() {
      return nsids.entrySet().stream()
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey,
                  e -> nsProperties.getOrDefault(e.getValue(), Collections.emptyMap())));
    }

    @Override
    Map<TableIdentifier, String> locations() {
      return tblIds.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> tblLocations.get(e.getValue())));
    }

    // emit stream of actions that recreate this catalog, NOT including Checkpoint action
    List<LogAction> checkpointStream() {
      List<LogAction> actions = Lists.newArrayList();
      // TODO regions
      // TODO ensure properties of deleted namespaces are removed
      // sort by nsid; sufficient for parentId, since namespaces never move, are created in order
      for (Map.Entry<Namespace, Integer> e :
          nsids.entrySet().stream()
              .sorted(Comparator.comparingInt(Map.Entry::getValue))
              .collect(Collectors.toList())) {
        final Namespace ns = e.getKey();
        final int nsid = e.getValue();
        final int version = nsVersion.get(nsid);
        final int levels = ns.length();
        final Namespace parent =
            levels > 1
                ? Namespace.of(Arrays.copyOfRange(ns.levels(), 0, levels - 1))
                : Namespace.empty();
        final int parentId = nsids.get(parent);
        actions.add(
            new LogAction.CreateNamespace(
                0 == levels ? "" : ns.level(levels - 1),
                nsid,
                version,
                parentId,
                nsVersion.get(parentId)));
      }
      for (Map.Entry<Integer, Map<String, String>> e : nsProperties.entrySet()) {
        final int nsid = e.getKey();
        for (Map.Entry<String, String> prop : e.getValue().entrySet()) {
          actions.add(new LogAction.AddNamespaceProperty(nsid, prop.getKey(), prop.getValue()));
        }
      }
      for (Map.Entry<TableIdentifier, Integer> e : tblIds.entrySet()) {
        final TableIdentifier ti = e.getKey();
        final int tblId = e.getValue();
        final int version = tblVersion.get(tblId);
        final int nsid = nsids.get(ti.namespace());
        actions.add(
            new LogAction.CreateTable(
                ti.name(), tblId, version, nsid, nsVersion.get(nsid), tblLocations.get(tblId)));
      }
      return actions;
    }

    // absolutely disgusting
    static void readCommittedTxn(Set<UUID> committedTxn, byte[] txnBytes) {
      try (ByteArrayInputStream bais = new ByteArrayInputStream(txnBytes);
          DataInputStream txndis = new DataInputStream(bais)) {
        int nTxn = txndis.readInt();
        for (int i = 0; i < nTxn; ++i) {
          long msb = txndis.readLong();
          long lsb = txndis.readLong();
          committedTxn.add(new UUID(msb, lsb));
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e); // not possible
      }
    }

    void writeCommittedTxn(DataOutputStream txndos) throws IOException {
      txndos.writeInt(committedTxn.size());
      for (UUID u : committedTxn.stream().sorted().collect(Collectors.toList())) {
        txndos.writeLong(u.getMostSignificantBits());
        txndos.writeLong(u.getLeastSignificantBits());
      }
    }

    void writeCheckpoint(OutputStream out) throws IOException {
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
          DataOutputStream chk = new DataOutputStream(out)) {
        final byte[] chkData;
        try (DataOutputStream chkdos = new DataOutputStream(bos)) {
          for (LogAction action : checkpointStream()) {
            action.write(chkdos);
          }
          chkData = bos.toByteArray(); // SIGH. You suck.
        }

        bos.reset();

        final byte[] txnData;
        try (DataOutputStream txndos = new DataOutputStream(bos)) {
          writeCommittedTxn(txndos);
          txnData = bos.toByteArray();
        }
        final LogAction.Checkpoint chkAction =
            new LogAction.Checkpoint(
                uuid(), nextNsid, nextTblid, chkData.length, 0, txnData.length);
        chkAction.write(chk);
        out.write(chkData);
        out.write(txnData);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      LogCatalogFile that = (LogCatalogFile) o;
      return nextNsid == that.nextNsid
          && nextTblid == that.nextTblid
          && Objects.equals(uuid(), that.uuid())
          && Objects.equals(nsids, that.nsids)
          && Objects.equals(nsVersion, that.nsVersion)
          && Objects.equals(nsProperties, that.nsProperties)
          && Objects.equals(tblIds, that.tblIds)
          && Objects.equals(tblVersion, that.tblVersion)
          && Objects.equals(tblLocations, that.tblLocations);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          uuid(),
          nextNsid,
          nextTblid,
          nsids,
          nsVersion,
          nsProperties,
          tblIds,
          tblVersion,
          tblLocations);
    }

    @Override
    public String toString() {
      return "LogCatalogFile{"
          + "catalogUUID="
          + uuid()
          + ", nextNsid="
          + nextNsid
          + ", nextTblid="
          + nextTblid
          + ", nsids="
          + nsids
          + ", nsVersion="
          + nsVersion
          + ", nsProperties="
          + nsProperties
          + ", tblIds="
          + tblIds
          + ", tblVersion="
          + tblVersion
          + ", tblLocations="
          + tblLocations
          + '}';
    }
  }
}
