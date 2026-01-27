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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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
public class LogCatalogFormat implements CatalogFormat<LogCatalogFile, LogCatalogFormat.Mut> {
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
    LogMutator mutator = new LogMutator(catalog);
    try (DataInputStream din = new DataInputStream(in)) {
      if (din.readByte() != LogAction.Type.CHECKPOINT.opcode) {
        throw new IllegalStateException("Invalid magic bits");
      }
      LogAction.Checkpoint chk = LogAction.Checkpoint.read(din);
      mutator.apply(chk);
      byte[] chkBytes = new byte[(int) chk.chkLen];
      IOUtil.readFully(in, chkBytes, 0, chkBytes.length);
      InputStream chkStream = new DataInputStream(new ByteArrayInputStream(chkBytes));
      for (LogAction action : LogAction.chkIterator(new DataInputStream(chkStream))) {
        // no validation necessary in this interval
        mutator.apply(action);
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
        mutator.verifyAndApply(txn);
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
        LogSerializer.writeCheckpoint(this, dos);
      }

      static Checkpoint read(DataInputStream dis) throws IOException {
        return LogSerializer.readCheckpoint(dis);
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
          parentId = logParentId < 0 ? catalog.idManager.getRemapped(logParentId) : logParentId;
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
        LogSerializer.writeCreateNamespace(this, dos);
      }

      static CreateNamespace read(DataInputStream dis) throws IOException {
        return LogSerializer.readCreateNamespace(dis);
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
        LogSerializer.writeDropNamespace(this, dos);
      }

      static DropNamespace read(DataInputStream dis) throws IOException {
        return LogSerializer.readDropNamespace(dis);
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
          nsid = catalog.idManager.getRemapped(logNsid);
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
        LogSerializer.writeAddNamespaceProperty(this, dos);
      }

      static AddNamespaceProperty read(DataInputStream dis) throws IOException {
        return LogSerializer.readAddNamespaceProperty(dis);
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
        LogSerializer.writeDropNamespaceProperty(this, dos);
      }

      static DropNamespaceProperty read(DataInputStream dis) throws IOException {
        return LogSerializer.readDropNamespaceProperty(dis);
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
        // TODO reaching into internal maps is grotesque, clean this up
        // TODO XXX create table should also increment the namespace version, so concurrent creates
        // fail validation
        final int nsid = logNsVersion < 0 ? catalog.idManager.getRemapped(logNsid) : logNsid;
        final int tblId = this.logTblId == LATE_BIND ? catalog.idManager.allocateTblid() : this.logTblId;
        catalog.addTableInternal(tblId, nsid, logTblVersion, name, location);
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        LogSerializer.writeCreateTable(this, dos);
      }

      static CreateTable read(DataInputStream dis) throws IOException {
        return LogSerializer.readCreateTable(dis);
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
        LogSerializer.writeDropTable(this, dos);
      }

      static DropTable read(DataInputStream dis) throws IOException {
        return LogSerializer.readDropTable(dis);
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
        LogSerializer.writeReadTable(this, dos);
      }

      static ReadTable read(DataInputStream dis) throws IOException {
        return LogSerializer.readReadTable(dis);
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
        LogSerializer.writeUpdateTable(this, dos);
      }

      static UpdateTable read(DataInputStream dis) throws IOException {
        return LogSerializer.readUpdateTable(dis);
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
        LogSerializer.writeTransaction(this, dos);
      }

      static void seal(byte[] serTxn) {
        LogSerializer.sealTransaction(serTxn);
      }

      static void unseal(byte[] serTxn) {
        LogSerializer.unsealTransaction(serTxn);
      }

      static Transaction read(DataInputStream dis) throws IOException {
        return LogSerializer.readTransaction(dis);
      }
    }

    static Iterable<LogAction> chkIterator(final DataInputStream dis) {
      return LogSerializer.checkpointIterable(dis);
    }

    static Iterable<Transaction> logIterator(final DataInputStream dis) {
      return LogSerializer.logIterable(dis);
    }
  }

  /**
   * Mutable builder for LogCatalogFile that accumulates changes and commits atomically.
   *
   * <p>TODO (Future refactoring): The format-specific state fields below (nsids, nsVersion,
   * nsLookup, nsProperties, tblIds, tblVersion, tblLocations, committedTxn) could be moved to a
   * separate LogMutState class to further separate "what user requested" (base class fields) from
   * "how format implements it" (format-specific state). This was deferred as a higher-risk change.
   */
  public static class Mut extends CatalogFile.Mut<LogCatalogFile, Mut> {
    // namespace IDs are internal to the catalog format

    private UUID uuid = null;
    private final LogIdManager idManager = new LogIdManager();
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
      this.idManager.setGlobals(nextNsid, nextTblid);
    }

    // changes to be applied to this catalog
    Mut(LogCatalogFile other) {
      super(other);
    }

    void setSealed() {
      this.sealed = true;
    }

    int remap(int nsid) {
      return idManager.remap(nsid);
    }

    void setGlobals(UUID uuid, int nextNsid, int nextTblid) {
      Preconditions.checkArgument(this.uuid == null, "UUID already set");
      this.uuid = uuid;
      idManager.setGlobals(nextNsid, nextTblid);
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
          parent = original.getNsLookup().get(parentId);
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
      LogSerializer.readCommittedTransactions(committedTxn, committedTxnBytes);
    }

    LogCatalogFile merge() {
      return new LogCatalogFile(
          original.location(),
          uuid,
          idManager.getNextNsid(),
          idManager.getNextTblid(),
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
      return LogSerializer.transactionToBytes(diffActions);
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

}
