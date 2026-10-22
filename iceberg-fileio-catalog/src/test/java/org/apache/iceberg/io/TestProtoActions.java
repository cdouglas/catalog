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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.intentlog.CommittedSet;
import org.intentlog.LogView;
import org.intentlog.Replica;
import org.intentlog.fileio.AtomicFileLog;
import org.intentlog.wire.UuidV7;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/**
 * Systematic tests for all catalog action types, covering both positive
 * (action applies, state updated) and negative (version mismatch, state unchanged) cases.
 *
 * <p>Uses {@link CatalogBuilder} to construct serialized catalog files and
 * {@link #apply(byte[], ProtoCodec.Transaction...)} to append transactions and read
 * the result -- exercising the full checkpoint-parse / transaction-replay path.
 */
public class TestProtoActions {


  // ============================================================
  // CatalogBuilder -- ergonomic test fixture construction
  // ============================================================

  /**
   * Fluent builder for constructing serialized protobuf catalog files for testing.
   *
   * <p>Usage:
   * <pre>
   *   byte[] file = catalog()
   *       .ns(1, 0, "db", 1)
   *       .ns(2, 1, "schema", 1)
   *       .tbl(1, 2, "users", 1, "s3://bucket/users/v1")
   *       .prop(1, "owner", "alice")
   *       .build();
   * </pre>
   */
  static class CatalogBuilder {
    private final CatalogState.Builder inner;
    private final CommittedSet.Builder committed = CommittedSet.builder();
    private int maxNsId = 0;
    private int maxTblId = 0;

    CatalogBuilder() {
      this.inner = CatalogState.builder();
    }

    /** Adds a namespace. Tracks max IDs automatically. */
    CatalogBuilder ns(int id, int parentId, String name, int version) {
      inner.addNamespace(id, parentId, name, version);
      maxNsId = Math.max(maxNsId, id);
      return this;
    }

    /** Adds a table. Tracks max IDs automatically. */
    CatalogBuilder tbl(int id, int nsId, String name, int version, String location) {
      inner.addTable(id, nsId, name, version, location);
      maxTblId = Math.max(maxTblId, id);
      return this;
    }

    /** Adds an inline table (metadata stored in catalog). Tracks max IDs automatically. */
    CatalogBuilder inlineTbl(
        int id, int nsId, String name, int version, byte[] metadata, String manifestPrefix) {
      return inlineTbl(id, nsId, name, version, metadata,
          InlineMetadataCodecs.TAG_JSON_GZIP, manifestPrefix);
    }

    /** Adds an inline table with an explicit codec for the encoded metadata bytes. */
    CatalogBuilder inlineTbl(
        int id, int nsId, String name, int version, byte[] metadata, byte codec,
        String manifestPrefix) {
      inner.addInlineTable(id, nsId, name, version, metadata, codec, manifestPrefix);
      maxTblId = Math.max(maxTblId, id);
      return this;
    }

    /** Sets a namespace property. */
    CatalogBuilder prop(int nsId, String key, String value) {
      inner.setNamespaceProperty(nsId, key, value);
      return this;
    }

    /** Records a committed transaction ID. */
    CatalogBuilder committed(UUID txnId) {
      committed.add(txnId);
      return this;
    }

    /** Builds the catalog and serializes it to file bytes (header + checkpoint). */
    byte[] build() {
      inner.setNextNamespaceId(maxNsId + 1);
      inner.setNextTableId(maxTblId + 1);
      return CatalogTestSupport.encodeFullFile(
          CatalogTestSupport.LOG_ID, committed.build(), inner.build());
    }
  }

  /** Starts an empty catalog builder. */
  static CatalogBuilder catalog() {
    return new CatalogBuilder();
  }

  // ============================================================
  // Transaction / Action factories
  // ============================================================

  static ProtoCodec.Transaction txn(ProtoCodec.Action... actions) {
    return txn(UuidV7.newUuidV7(), actions);
  }

  /**
   * A transaction of {@code actions}, its creates' ids late-bound as a writer's are: replay rejects
   * a create that names a concrete id, so that a rejection is stable. A create's id here is the id
   * the test expects replay to bind (the catalog's next id, in the order of the transaction's
   * creates); the record carries -1, -2, ... in its place, per kind, and the transaction's
   * references to it are rewritten to match. A transaction that already names a late-bound id is
   * left as it is.
   */
  static ProtoCodec.Transaction txn(UUID id, ProtoCodec.Action... actions) {
    return new ProtoCodec.Transaction(id, lateBound(List.of(actions)));
  }

  /** {@code actions} with their creates' concrete ids late-bound (see {@link #txn}). */
  static List<ProtoCodec.Action> lateBound(List<ProtoCodec.Action> actions) {
    Map<Integer, Integer> ns = new HashMap<>();
    Map<Integer, Integer> tbl = new HashMap<>();
    for (ProtoCodec.Action a : actions) {
      if (a instanceof ProtoCodec.CreateNamespaceAction) {
        int created = ((ProtoCodec.CreateNamespaceAction) a).id;
        if (created < 0) {
          return actions;
        }
        ns.put(created, -(ns.size() + 1));
      } else if (a instanceof ProtoCodec.CreateTableAction
          || a instanceof ProtoCodec.CreateTableInlineAction) {
        int created = a instanceof ProtoCodec.CreateTableAction
            ? ((ProtoCodec.CreateTableAction) a).id
            : ((ProtoCodec.CreateTableInlineAction) a).id;
        if (created < 0) {
          return actions;
        }
        tbl.put(created, -(tbl.size() + 1));
      }
    }
    if (ns.isEmpty() && tbl.isEmpty()) {
      return actions;
    }
    List<ProtoCodec.Action> bound = new ArrayList<>(actions.size());
    for (ProtoCodec.Action a : actions) {
      if (a instanceof ProtoCodec.CreateNamespaceAction) {
        ProtoCodec.CreateNamespaceAction c = (ProtoCodec.CreateNamespaceAction) a;
        bound.add(new ProtoCodec.CreateNamespaceAction(
            ns.get(c.id), c.version, ns.getOrDefault(c.parentId, c.parentId), c.parentVersion,
            c.name));
      } else if (a instanceof ProtoCodec.CreateTableAction) {
        ProtoCodec.CreateTableAction c = (ProtoCodec.CreateTableAction) a;
        bound.add(new ProtoCodec.CreateTableAction(
            tbl.get(c.id), c.version, ns.getOrDefault(c.namespaceId, c.namespaceId),
            c.namespaceVersion, c.name, c.metadataLocation));
      } else if (a instanceof ProtoCodec.CreateTableInlineAction) {
        ProtoCodec.CreateTableInlineAction c = (ProtoCodec.CreateTableInlineAction) a;
        bound.add(new ProtoCodec.CreateTableInlineAction(
            tbl.get(c.id), c.version, ns.getOrDefault(c.namespaceId, c.namespaceId),
            c.namespaceVersion, c.name, c.metadata, c.metadataCodec, c.deltaBytes));
      } else if (a instanceof ProtoCodec.SetNamespacePropertyAction) {
        ProtoCodec.SetNamespacePropertyAction c = (ProtoCodec.SetNamespacePropertyAction) a;
        bound.add(new ProtoCodec.SetNamespacePropertyAction(
            ns.getOrDefault(c.namespaceId, c.namespaceId), c.namespaceVersion, c.key, c.value));
      } else if (a instanceof ProtoCodec.RemoveNamespacePropertyAction) {
        ProtoCodec.RemoveNamespacePropertyAction c = (ProtoCodec.RemoveNamespacePropertyAction) a;
        bound.add(new ProtoCodec.RemoveNamespacePropertyAction(
            ns.getOrDefault(c.namespaceId, c.namespaceId), c.namespaceVersion, c.key));
      } else if (a instanceof ProtoCodec.RenameTableAction) {
        ProtoCodec.RenameTableAction c = (ProtoCodec.RenameTableAction) a;
        bound.add(new ProtoCodec.RenameTableAction(
            tbl.getOrDefault(c.id, c.id), c.version,
            ns.getOrDefault(c.newNamespaceId, c.newNamespaceId), c.newNamespaceVersion,
            c.newName));
      } else {
        bound.add(a);
      }
    }
    return bound;
  }

  static ProtoCodec.CreateNamespaceAction createNs(
      int id, int parentId, String name, int version, int parentVersion) {
    return new ProtoCodec.CreateNamespaceAction(id, version, parentId, parentVersion, name);
  }

  static ProtoCodec.DropNamespaceAction dropNs(int id, int version) {
    return new ProtoCodec.DropNamespaceAction(id, version);
  }

  static ProtoCodec.SetNamespacePropertyAction setNsProp(
      int nsId, int nsVersion, String key, String value) {
    return new ProtoCodec.SetNamespacePropertyAction(nsId, nsVersion, key, value);
  }

  static ProtoCodec.RemoveNamespacePropertyAction removeNsProp(
      int nsId, int nsVersion, String key) {
    return new ProtoCodec.RemoveNamespacePropertyAction(nsId, nsVersion, key);
  }

  static ProtoCodec.CreateTableAction createTbl(
      int id, int nsId, String name, int version, int nsVersion, String location) {
    return new ProtoCodec.CreateTableAction(id, version, nsId, nsVersion, name, location);
  }

  static ProtoCodec.DropTableAction dropTbl(int id, int version) {
    return new ProtoCodec.DropTableAction(id, version);
  }

  static ProtoCodec.UpdateTableLocationAction updateTbl(
      int id, int version, String location) {
    return new ProtoCodec.UpdateTableLocationAction(id, version, location);
  }

  static ProtoCodec.ReadTableAction readTbl(int id, int version) {
    return new ProtoCodec.ReadTableAction(id, version);
  }

  static ProtoCodec.CreateTableInlineAction createTblInline(
      int id, int nsId, String name, int version, int nsVersion, byte[] metadata) {
    return new ProtoCodec.CreateTableInlineAction(
        id, version, nsId, nsVersion, name, metadata, InlineMetadataCodecs.TAG_JSON_GZIP);
  }

  /** CreateTableInline action with an explicit codec tag. */
  static ProtoCodec.CreateTableInlineAction createTblInline(
      int id, int nsId, String name, int version, int nsVersion, byte[] metadata, byte codec) {
    return new ProtoCodec.CreateTableInlineAction(
        id, version, nsId, nsVersion, name, metadata, codec);
  }

  /** UpdateTableInline in FULL mode (replace inline metadata). */
  static ProtoCodec.UpdateTableInlineAction updateTblInlineFull(
      int id, int version, byte[] fullMetadata) {
    return ProtoCodec.UpdateTableInlineAction.full(
        id, version, fullMetadata, InlineMetadataCodecs.TAG_JSON_GZIP);
  }

  /** UpdateTableInline in FULL mode with an explicit codec tag. */
  static ProtoCodec.UpdateTableInlineAction updateTblInlineFull(
      int id, int version, byte[] fullMetadata, byte codec) {
    return ProtoCodec.UpdateTableInlineAction.full(id, version, fullMetadata, codec);
  }

  /** UpdateTableInline in POINTER mode (evict to external file). */
  static ProtoCodec.UpdateTableInlineAction updateTblInlinePointer(
      int id, int version, String metadataLocation) {
    return ProtoCodec.UpdateTableInlineAction.pointer(id, version, metadataLocation);
  }

  static ProtoCodec.RenameTableAction renameTbl(
      int id, int version, int newNsId, int newNsVersion, String newName) {
    return new ProtoCodec.RenameTableAction(id, version, newNsId, newNsVersion, newName);
  }

  // ============================================================
  // File construction / replay helpers
  // ============================================================

  /** A catalog file holding a checkpoint of {@code catalog} (and no records). */
  static byte[] toFileBytes(CatalogState catalog) {
    return CatalogTestSupport.encodeFullFile(catalog);
  }

  /** A catalog file holding a checkpoint of {@code catalog}, with its log id and committed set. */
  static byte[] toFileBytes(Replica<CatalogState> catalog) {
    return CatalogTestSupport.encodeFullFile(catalog);
  }

  /**
   * Replays {@code fileBytes} with {@code txns} appended to its log: the checkpoint, the records
   * the file holds, then these, as a reader of the file with them appended would.
   */
  static Replica<CatalogState> apply(byte[] fileBytes, ProtoCodec.Transaction... txns) {
    LogView view = AtomicFileLog.decode(fileBytes);
    List<byte[]> records = new ArrayList<>(view.records());
    for (ProtoCodec.Transaction txn : txns) {
      records.add(ProtoCodec.encodeTransaction(txn));
    }
    return CatalogTestSupport.replay(AtomicFileLog.encode(view.checkpoint().orElseThrow(), records));
  }

  // ============================================================
  // CreateNamespace
  // ============================================================

  @Nested
  class CreateNamespaceTests {

    @Test
    void appliesUnderRoot() {
      byte[] file = catalog().build();
      Replica<CatalogState> result = apply(file, txn(createNs(1, 0, "db", 1, -1)));

      assertThat(result.state().containsNamespace(Namespace.of("db"))).isTrue();
      assertThat(result.state().namespaceVersion(result.state().namespaceId(Namespace.of("db")))).isEqualTo(1);
    }

    @Test
    void appliesUnderExistingParent() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      Replica<CatalogState> result = apply(file, txn(createNs(2, 1, "schema", 1, 1)));

      assertThat(result.state().containsNamespace(Namespace.of("db", "schema"))).isTrue();
    }

    @Test
    void rejectsOnParentVersionMismatch() {
      byte[] file = catalog().ns(1, 0, "db", 3).build();
      // parent_version=1 doesn't match db's actual version=3
      ProtoCodec.Transaction t = txn(createNs(2, 1, "schema", 1, 1));
      Replica<CatalogState> result = apply(file, t);

      assertThat(result.state().containsNamespace(Namespace.of("db", "schema"))).isFalse();
      assertThat(result.committed().contains(t.id())).isFalse();
    }

    @Test
    void appliesWithLateBoundParentVersion() {
      byte[] file = catalog().ns(1, 0, "db", 5).build();
      // parent_version=-1 means "don't check" (late-bound)
      Replica<CatalogState> result = apply(file, txn(createNs(2, 1, "schema", 1, -1)));

      assertThat(result.state().containsNamespace(Namespace.of("db", "schema"))).isTrue();
    }

    @Test
    void multipleInSameTransaction() {
      byte[] file = catalog().build();
      Replica<CatalogState> result = apply(file, txn(
          createNs(1, 0, "a", 1, -1),
          createNs(2, 0, "b", 1, -1),
          createNs(3, 1, "child", 1, -1)));

      assertThat(result.state().containsNamespace(Namespace.of("a"))).isTrue();
      assertThat(result.state().containsNamespace(Namespace.of("b"))).isTrue();
      assertThat(result.state().containsNamespace(Namespace.of("a", "child"))).isTrue();
    }
  }

  // ============================================================
  // DropNamespace
  // ============================================================

  @Nested
  class DropNamespaceTests {

    @Test
    void appliesWithMatchingVersion() {
      byte[] file = catalog().ns(1, 0, "db", 2).build();
      ProtoCodec.Transaction t = txn(dropNs(1, 2));
      Replica<CatalogState> result = apply(file, t);

      assertThat(result.state().containsNamespace(Namespace.of("db"))).isFalse();
      assertThat(result.committed().contains(t.id())).isTrue();
    }

    @Test
    void rejectsOnVersionMismatch() {
      byte[] file = catalog().ns(1, 0, "db", 2).build();
      ProtoCodec.Transaction t = txn(dropNs(1, 1));
      Replica<CatalogState> result = apply(file, t);

      assertThat(result.state().containsNamespace(Namespace.of("db"))).isTrue();
      assertThat(result.committed().contains(t.id())).isFalse();
    }

    @Test
    void removesPropertiesWhenDropped() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .prop(1, "owner", "alice")
          .prop(1, "comment", "test")
          .build();
      Replica<CatalogState> result = apply(file, txn(dropNs(1, 1)));

      assertThat(result.state().containsNamespace(Namespace.of("db"))).isFalse();
    }
  }

  // ============================================================
  // SetNamespaceProperty
  // ============================================================

  @Nested
  class SetNamespacePropertyTests {

    @Test
    void appliesWithMatchingVersion() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      Replica<CatalogState> result = apply(file, txn(setNsProp(1, 1, "owner", "bob")));

      assertThat(result.state().namespaceProperties(Namespace.of("db")))
          .containsEntry("owner", "bob");
    }

    @Test
    void rejectsOnVersionMismatch() {
      byte[] file = catalog()
          .ns(1, 0, "db", 3)
          .prop(1, "owner", "alice")
          .build();
      ProtoCodec.Transaction t = txn(setNsProp(1, 1, "owner", "bob"));
      Replica<CatalogState> result = apply(file, t);

      assertThat(result.state().namespaceProperties(Namespace.of("db")))
          .containsEntry("owner", "alice");
      assertThat(result.committed().contains(t.id())).isFalse();
    }

    @Test
    void appliesWithLateBoundVersion() {
      byte[] file = catalog().ns(1, 0, "db", 5).build();
      // version=-1 means namespace was created in same txn (skip version check)
      Replica<CatalogState> result = apply(file, txn(setNsProp(1, -1, "k", "v")));

      assertThat(result.state().namespaceProperties(Namespace.of("db")))
          .containsEntry("k", "v");
    }

    @Test
    void overwritesExistingProperty() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .prop(1, "owner", "alice")
          .build();
      Replica<CatalogState> result = apply(file, txn(setNsProp(1, 1, "owner", "bob")));

      assertThat(result.state().namespaceProperties(Namespace.of("db")))
          .containsEntry("owner", "bob");
    }

    @Test
    void addsMultiplePropertiesInSameTransaction() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      Replica<CatalogState> result = apply(file, txn(
          setNsProp(1, 1, "owner", "alice"),
          setNsProp(1, 1, "env", "prod")));

      Map<String, String> props = result.state().namespaceProperties(Namespace.of("db"));
      assertThat(props).containsEntry("owner", "alice");
      assertThat(props).containsEntry("env", "prod");
    }
  }

  // ============================================================
  // RemoveNamespaceProperty
  // ============================================================

  @Nested
  class RemoveNamespacePropertyTests {

    @Test
    void appliesWithMatchingVersion() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .prop(1, "owner", "alice")
          .prop(1, "env", "prod")
          .build();
      Replica<CatalogState> result = apply(file, txn(removeNsProp(1, 1, "owner")));

      Map<String, String> props = result.state().namespaceProperties(Namespace.of("db"));
      assertThat(props).doesNotContainKey("owner");
      assertThat(props).containsEntry("env", "prod");
    }

    @Test
    void rejectsOnVersionMismatch() {
      byte[] file = catalog()
          .ns(1, 0, "db", 3)
          .prop(1, "owner", "alice")
          .build();
      ProtoCodec.Transaction t = txn(removeNsProp(1, 1, "owner"));
      Replica<CatalogState> result = apply(file, t);

      assertThat(result.state().namespaceProperties(Namespace.of("db")))
          .containsEntry("owner", "alice");
      assertThat(result.committed().contains(t.id())).isFalse();
    }

    @Test
    void appliesWithLateBoundVersion() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .prop(1, "owner", "alice")
          .build();
      Replica<CatalogState> result = apply(file, txn(removeNsProp(1, -1, "owner")));

      assertThat(result.state().namespaceProperties(Namespace.of("db")))
          .doesNotContainKey("owner");
    }

    @Test
    void noopWhenPropertyDoesNotExist() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      ProtoCodec.Transaction t = txn(removeNsProp(1, 1, "nonexistent"));
      Replica<CatalogState> result = apply(file, t);

      assertThat(result.committed().contains(t.id())).isTrue();
    }
  }

  // ============================================================
  // CreateTable
  // ============================================================

  @Nested
  class CreateTableTests {

    @Test
    void appliesInExistingNamespace() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      Replica<CatalogState> result = apply(file,
          txn(createTbl(1, 1, "users", 1, 1, "s3://bucket/users/v1")));

      TableIdentifier users = TableIdentifier.of(Namespace.of("db"), "users");
      assertThat(result.state().location(users)).isEqualTo("s3://bucket/users/v1");
    }

    @Test
    void rejectsOnNamespaceVersionMismatch() {
      byte[] file = catalog().ns(1, 0, "db", 3).build();
      // ns_version=1 doesn't match actual version=3
      ProtoCodec.Transaction t = txn(createTbl(1, 1, "users", 1, 1, "s3://bucket/users/v1"));
      Replica<CatalogState> result = apply(file, t);

      TableIdentifier users = TableIdentifier.of(Namespace.of("db"), "users");
      assertThat(result.state().location(users)).isNull();
      assertThat(result.committed().contains(t.id())).isFalse();
    }

    @Test
    void appliesWithLateBoundNamespaceVersion() {
      byte[] file = catalog().ns(1, 0, "db", 5).build();
      // ns_version=-1 skips check (namespace created in same txn)
      Replica<CatalogState> result = apply(file,
          txn(createTbl(1, 1, "users", 1, -1, "s3://bucket/users/v1")));

      TableIdentifier users = TableIdentifier.of(Namespace.of("db"), "users");
      assertThat(result.state().location(users)).isEqualTo("s3://bucket/users/v1");
    }

    @Test
    void createNamespaceAndTableInSameTransaction() {
      byte[] file = catalog().build();
      Replica<CatalogState> result = apply(file, txn(
          createNs(1, 0, "db", 1, -1),
          createTbl(1, 1, "users", 1, -1, "s3://bucket/users/v1")));

      assertThat(result.state().containsNamespace(Namespace.of("db"))).isTrue();
      TableIdentifier users = TableIdentifier.of(Namespace.of("db"), "users");
      assertThat(result.state().location(users)).isEqualTo("s3://bucket/users/v1");
    }

    @Test
    void multipleTablesInSameNamespace() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      Replica<CatalogState> result = apply(file, txn(
          createTbl(1, 1, "users", 1, 1, "s3://bucket/users/v1"),
          createTbl(2, 1, "orders", 1, 1, "s3://bucket/orders/v1")));

      Namespace db = Namespace.of("db");
      assertThat(result.state().location(TableIdentifier.of(db, "users")))
          .isEqualTo("s3://bucket/users/v1");
      assertThat(result.state().location(TableIdentifier.of(db, "orders")))
          .isEqualTo("s3://bucket/orders/v1");
    }
  }

  // ============================================================
  // DropTable
  // ============================================================

  @Nested
  class DropTableTests {

    @Test
    void appliesWithMatchingVersion() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "users", 2, "s3://bucket/users/v2")
          .build();
      ProtoCodec.Transaction t = txn(dropTbl(1, 2));
      Replica<CatalogState> result = apply(file, t);

      TableIdentifier users = TableIdentifier.of(Namespace.of("db"), "users");
      assertThat(result.state().location(users)).isNull();
      assertThat(result.state().tables()).doesNotContain(users);
      assertThat(result.committed().contains(t.id())).isTrue();
    }

    @Test
    void rejectsOnVersionMismatch() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "users", 2, "s3://bucket/users/v2")
          .build();
      ProtoCodec.Transaction t = txn(dropTbl(1, 1));
      Replica<CatalogState> result = apply(file, t);

      TableIdentifier users = TableIdentifier.of(Namespace.of("db"), "users");
      assertThat(result.state().location(users)).isEqualTo("s3://bucket/users/v2");
      assertThat(result.committed().contains(t.id())).isFalse();
    }

    @Test
    void preservesOtherTablesInSameNamespace() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "users", 1, "s3://bucket/users/v1")
          .tbl(2, 1, "orders", 1, "s3://bucket/orders/v1")
          .build();
      Replica<CatalogState> result = apply(file, txn(dropTbl(1, 1)));

      Namespace db = Namespace.of("db");
      assertThat(result.state().location(TableIdentifier.of(db, "users"))).isNull();
      assertThat(result.state().location(TableIdentifier.of(db, "orders")))
          .isEqualTo("s3://bucket/orders/v1");
    }
  }

  // ============================================================
  // UpdateTableLocation
  // ============================================================

  @Nested
  class UpdateTableLocationTests {

    @Test
    void appliesWithMatchingVersion() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "users", 1, "s3://bucket/users/v1")
          .build();
      Replica<CatalogState> result = apply(file,
          txn(updateTbl(1, 1, "s3://bucket/users/v2")));

      TableIdentifier users = TableIdentifier.of(Namespace.of("db"), "users");
      assertThat(result.state().location(users)).isEqualTo("s3://bucket/users/v2");
      // Version should be incremented
      assertThat(result.state().tableVersion(result.state().tableId(users))).isEqualTo(2);
    }

    @Test
    void rejectsOnVersionMismatch() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "users", 3, "s3://bucket/users/v3")
          .build();
      ProtoCodec.Transaction t = txn(updateTbl(1, 1, "s3://bucket/users/v4"));
      Replica<CatalogState> result = apply(file, t);

      TableIdentifier users = TableIdentifier.of(Namespace.of("db"), "users");
      assertThat(result.state().location(users)).isEqualTo("s3://bucket/users/v3");
      assertThat(result.committed().contains(t.id())).isFalse();
    }

    @Test
    void sequentialUpdatesIncrementVersion() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "users", 1, "s3://v1")
          .build();

      // First update: version 1 -> 2
      ProtoCodec.Transaction t1 = txn(updateTbl(1, 1, "s3://v2"));
      // Second update: version 2 -> 3
      ProtoCodec.Transaction t2 = txn(updateTbl(1, 2, "s3://v3"));
      Replica<CatalogState> result = apply(file, t1, t2);

      TableIdentifier users = TableIdentifier.of(Namespace.of("db"), "users");
      assertThat(result.state().location(users)).isEqualTo("s3://v3");
      assertThat(result.state().tableVersion(result.state().tableId(users))).isEqualTo(3);
    }

    @Test
    void concurrentUpdatesSecondRejected() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "users", 1, "s3://v1")
          .build();

      // Both try to update from version 1
      ProtoCodec.Transaction t1 = txn(updateTbl(1, 1, "s3://writer-a"));
      ProtoCodec.Transaction t2 = txn(updateTbl(1, 1, "s3://writer-b"));
      Replica<CatalogState> result = apply(file, t1, t2);

      TableIdentifier users = TableIdentifier.of(Namespace.of("db"), "users");
      assertThat(result.state().location(users)).isEqualTo("s3://writer-a");
      assertThat(result.committed().contains(t1.id())).isTrue();
      assertThat(result.committed().contains(t2.id())).isFalse();
    }
  }

  // ============================================================
  // ReadTable
  // ============================================================

  @Nested
  class ReadTableTests {

    @Test
    void verifiesWithMatchingVersion() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "users", 1, "s3://v1")
          .build();
      // ReadTable at correct version, paired with a create to prove the txn applied
      ProtoCodec.Transaction t = txn(
          readTbl(1, 1),
          createNs(2, 0, "other", 1, -1));
      Replica<CatalogState> result = apply(file, t);

      assertThat(result.committed().contains(t.id())).isTrue();
      assertThat(result.state().containsNamespace(Namespace.of("other"))).isTrue();
    }

    @Test
    void rejectsOnVersionMismatch() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "users", 3, "s3://v3")
          .build();
      // ReadTable expects version 1, actual is 3
      ProtoCodec.Transaction t = txn(
          readTbl(1, 1),
          createNs(2, 0, "other", 1, -1));
      Replica<CatalogState> result = apply(file, t);

      assertThat(result.committed().contains(t.id())).isFalse();
      assertThat(result.state().containsNamespace(Namespace.of("other"))).isFalse();
    }

    @Test
    void failsAfterConcurrentUpdate() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "users", 1, "s3://v1")
          .build();

      // t1: updates table version 1 -> 2
      ProtoCodec.Transaction t1 = txn(updateTbl(1, 1, "s3://v2"));
      // t2: read-your-writes check at version 1, should fail
      ProtoCodec.Transaction t2 = txn(
          readTbl(1, 1),
          createNs(2, 0, "marker", 1, -1));
      Replica<CatalogState> result = apply(file, t1, t2);

      assertThat(result.committed().contains(t1.id())).isTrue();
      assertThat(result.committed().contains(t2.id())).isFalse();
      assertThat(result.state().containsNamespace(Namespace.of("marker"))).isFalse();
    }

    @Test
    void doesNotMutateState() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "users", 1, "s3://v1")
          .build();
      Replica<CatalogState> result = apply(file, txn(readTbl(1, 1)));

      // Table version unchanged after ReadTable
      TableIdentifier users = TableIdentifier.of(Namespace.of("db"), "users");
      assertThat(result.state().tableVersion(result.state().tableId(users))).isEqualTo(1);
      assertThat(result.state().location(users)).isEqualTo("s3://v1");
    }
  }

  // ============================================================
  // RenameTable
  // ============================================================

  @Nested
  class RenameTableTests {

    @Test
    void renamesPointerTableInPlace() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "users", 1, "s3://users/v1")
          .build();
      Replica<CatalogState> r = apply(file, txn(renameTbl(1, 1, 1, 1, "people")));

      Namespace db = Namespace.of("db");
      // Old name gone, new name resolves to the SAME id, location preserved.
      assertThat(r.state().tableId(TableIdentifier.of(db, "users"))).isNull();
      assertThat(r.state().tableId(TableIdentifier.of(db, "people"))).isEqualTo(1);
      assertThat(r.state().location(TableIdentifier.of(db, "people"))).isEqualTo("s3://users/v1");
      assertThat(r.state().tableVersion(1)).isEqualTo(2);
    }

    @Test
    void renamesAcrossNamespaces() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .ns(2, 0, "archive", 1)
          .tbl(1, 1, "users", 1, "s3://users/v1")
          .build();
      Replica<CatalogState> r = apply(file, txn(renameTbl(1, 1, 2, 1, "users")));

      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("db"), "users"))).isNull();
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("archive"), "users"))).isEqualTo(1);
      assertThat(r.state().location(TableIdentifier.of(Namespace.of("archive"), "users")))
          .isEqualTo("s3://users/v1");
      // Both source and destination ns versions bumped (children-set rule).
      assertThat(r.state().namespaceVersion(1)).isEqualTo(2);
      assertThat(r.state().namespaceVersion(2)).isEqualTo(2);
    }

    @Test
    void renameInlineTablePreservesInlineState() throws IOException {
      byte[] meta = "{\"format-version\":2}".getBytes();
      CatalogState.Builder builder = CatalogState.builder();
      builder.addNamespace(1, 0, "db", 1);
      builder.addInlineTable(1, 1, "users", 1, meta, "s3://users/metadata/snap-");
      // Synthetic manifest pool entry + snapshot ref so we can prove they survive.
      builder.addManifestToPool(1, new ProtoCodec.DecodedManifestFile(
          "s3://users/metadata/snap-m0.avro", 1234, 0, 0, 1, 1, 100L,
          1, 0, 0, 10L, 0, 0, null, null, null));
      builder.setSnapshotManifests(1, 100L, java.util.Collections.singletonList(
          "s3://users/metadata/snap-m0.avro"));
      byte[] file = toFileBytes(builder.build());

      Replica<CatalogState> r = apply(file, txn(renameTbl(1, 1, 1, 1, "people")));

      // Id is stable, so the inline maps follow for free.
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("db"), "people"))).isEqualTo(1);
      assertThat(r.state().isInlineTable(1)).isTrue();
      assertThat(r.state().inlineMetadata(1)).isEqualTo(meta);
      assertThat(r.state().manifestListPrefix(1)).isEqualTo("s3://users/metadata/snap-");
      assertThat(r.state().hasInlineManifests(1, 100L)).isTrue();
      assertThat(r.state().inlineManifests(1, 100L)).hasSize(1);
    }

    @Test
    void rejectsOnTableVersionMismatch() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "users", 3, "s3://users/v3")
          .build();
      // Action captured tbl_v=1 but real version is 3.
      ProtoCodec.Transaction t = txn(renameTbl(1, 1, 1, 1, "people"));
      Replica<CatalogState> r = apply(file, t);

      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("db"), "users"))).isEqualTo(1);
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("db"), "people"))).isNull();
      assertThat(r.committed().contains(t.id())).isFalse();
    }

    @Test
    void rejectsOnDestNamespaceVersionMismatch() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .ns(2, 0, "archive", 5)
          .tbl(1, 1, "users", 1, "s3://users/v1")
          .build();
      // Action captured archive ns_v=1 but it's actually 5.
      ProtoCodec.Transaction t = txn(renameTbl(1, 1, 2, 1, "users"));
      Replica<CatalogState> r = apply(file, t);

      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("db"), "users"))).isEqualTo(1);
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("archive"), "users"))).isNull();
      assertThat(r.committed().contains(t.id())).isFalse();
    }

    @Test
    void lateBindDestNamespaceSkipsVersionCheck() {
      // Action carries newNamespaceVersion=-1 (dest ns created in same txn),
      // so apply doesn't gate on ns_v.
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "users", 1, "s3://users/v1")
          .build();
      ProtoCodec.Transaction t = txn(
          createNs(2, 0, "archive", 1, -1),
          renameTbl(1, 1, 2, -1, "users"));
      Replica<CatalogState> r = apply(file, t);

      assertThat(r.state().containsNamespace(Namespace.of("archive"))).isTrue();
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("archive"), "users"))).isEqualTo(1);
      assertThat(r.committed().contains(t.id())).isTrue();
    }
  }

  // ============================================================
  // Transaction-level behavior
  // ============================================================

  @Nested
  class TransactionTests {

    @Test
    void entireTransactionRejectedWhenAnyActionFails() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "users", 3, "s3://v3")
          .build();

      // First action is valid, second has version mismatch
      ProtoCodec.Transaction t = txn(
          createNs(2, 0, "newns", 1, -1),
          updateTbl(1, 1, "s3://v4"));  // expects version 1, actual is 3
      Replica<CatalogState> result = apply(file, t);

      // Neither action should have taken effect
      assertThat(result.state().containsNamespace(Namespace.of("newns"))).isFalse();
      assertThat(result.state().location(TableIdentifier.of(Namespace.of("db"), "users")))
          .isEqualTo("s3://v3");
      assertThat(result.committed().contains(t.id())).isFalse();
    }

    @Test
    void independentTransactionsApplySequentially() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();

      ProtoCodec.Transaction t1 = txn(createNs(2, 0, "analytics", 1, -1));
      ProtoCodec.Transaction t2 = txn(
          createTbl(1, 1, "users", 1, 1, "s3://users/v1"));
      ProtoCodec.Transaction t3 = txn(
          createTbl(2, 2, "events", 1, -1, "s3://events/v1"));
      Replica<CatalogState> result = apply(file, t1, t2, t3);

      assertThat(result.state().containsNamespace(Namespace.of("analytics"))).isTrue();
      assertThat(result.state().location(TableIdentifier.of(Namespace.of("db"), "users")))
          .isEqualTo("s3://users/v1");
      assertThat(result.state().location(TableIdentifier.of(Namespace.of("analytics"), "events")))
          .isEqualTo("s3://events/v1");
      assertThat(result.committed().contains(t1.id())).isTrue();
      assertThat(result.committed().contains(t2.id())).isTrue();
      assertThat(result.committed().contains(t3.id())).isTrue();
    }

    @Test
    void duplicateTransactionIdIsIdempotent() {
      byte[] file = catalog().build();
      UUID sharedId = UuidV7.newUuidV7();

      ProtoCodec.Transaction t = txn(sharedId, createNs(1, 0, "db", 1, -1));
      // Same ID appended twice
      Replica<CatalogState> result = apply(file, t, t);

      assertThat(result.state().containsNamespace(Namespace.of("db"))).isTrue();
      assertThat(result.state().namespaceVersion(result.state().namespaceId(Namespace.of("db")))).isEqualTo(1);
    }

    @Test
    void committedTransactionInCheckpointIsSkipped() {
      UUID priorTxnId = UuidV7.newUuidV7();
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .committed(priorTxnId)
          .build();

      // Transaction with the same ID as one already in the checkpoint
      ProtoCodec.Transaction t = txn(priorTxnId, createNs(2, 0, "should_not_appear", 1, -1));
      Replica<CatalogState> result = apply(file, t);

      assertThat(result.state().containsNamespace(Namespace.of("should_not_appear"))).isFalse();
    }

  }

  // ============================================================
  // Multi-operation integration scenarios
  // ============================================================

  @Nested
  class IntegrationTests {

    @Test
    void lateBoundNestedNamespaceHierarchy() {
      byte[] file = catalog().build();
      Replica<CatalogState> result = apply(file, txn(
          createNs(1, 0, "a", 1, -1),
          createNs(2, 1, "b", 1, -1),
          createNs(3, 2, "c", 1, -1),
          createTbl(1, 3, "leaf", 1, -1, "s3://deep/leaf")));

      assertThat(result.state().containsNamespace(Namespace.of("a"))).isTrue();
      assertThat(result.state().containsNamespace(Namespace.of("a", "b"))).isTrue();
      assertThat(result.state().containsNamespace(Namespace.of("a", "b", "c"))).isTrue();
      assertThat(result.state().location(TableIdentifier.of(Namespace.of("a", "b", "c"), "leaf")))
          .isEqualTo("s3://deep/leaf");
    }

    @Test
    void dropAndRecreateDifferentNamespace() {
      byte[] file = catalog()
          .ns(1, 0, "staging", 1)
          .tbl(1, 1, "raw", 1, "s3://staging/raw")
          .build();

      // Drop table then namespace, then create a new namespace.
      // Dropping the table bumps the parent ns from v=1 to v=2 (children-set
      // mutation), so the subsequent dropNs must capture v=2.
      ProtoCodec.Transaction t1 = txn(dropTbl(1, 1));
      ProtoCodec.Transaction t2 = txn(dropNs(1, 2));
      ProtoCodec.Transaction t3 = txn(createNs(2, 0, "production", 1, -1));
      Replica<CatalogState> result = apply(file, t1, t2, t3);

      assertThat(result.state().containsNamespace(Namespace.of("staging"))).isFalse();
      assertThat(result.state().containsNamespace(Namespace.of("production"))).isTrue();
      assertThat(result.state().tables()).isEmpty();
    }

    @Test
    void createNamespaceWithPropertiesInSameTransaction() {
      byte[] file = catalog().build();
      Replica<CatalogState> result = apply(file, txn(
          createNs(1, 0, "db", 1, -1),
          setNsProp(1, -1, "owner", "alice"),
          setNsProp(1, -1, "env", "prod")));

      Map<String, String> props = result.state().namespaceProperties(Namespace.of("db"));
      assertThat(props).containsEntry("owner", "alice");
      assertThat(props).containsEntry("env", "prod");
    }

    @Test
    void setAndRemovePropertyInSequentialTransactions() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();

      // setNsProp bumps ns "db" from v=1 to v=2; the subsequent removeNsProp
      // must capture v=2 to verify against the post-set state.
      ProtoCodec.Transaction t1 = txn(setNsProp(1, 1, "owner", "alice"));
      ProtoCodec.Transaction t2 = txn(removeNsProp(1, 2, "owner"));
      Replica<CatalogState> result = apply(file, t1, t2);

      assertThat(result.state().namespaceProperties(Namespace.of("db")))
          .doesNotContainKey("owner");
    }

    @Test
    void fullCatalogLifecycle() {
      byte[] file = catalog().build();

      // Step 1: create namespace structure
      ProtoCodec.Transaction t1 = txn(
          createNs(1, 0, "warehouse", 1, -1),
          createNs(2, 1, "bronze", 1, -1),
          createNs(3, 1, "silver", 1, -1),
          setNsProp(1, -1, "owner", "data-team"));

      // Step 2: create tables
      ProtoCodec.Transaction t2 = txn(
          createTbl(1, 2, "raw_events", 1, -1, "s3://wh/bronze/raw_events/v1"),
          createTbl(2, 3, "enriched", 1, -1, "s3://wh/silver/enriched/v1"));

      // Step 3: update a table
      ProtoCodec.Transaction t3 = txn(
          updateTbl(1, 1, "s3://wh/bronze/raw_events/v2"));

      // Step 4: drop one table, read-your-writes on the other
      ProtoCodec.Transaction t4 = txn(
          readTbl(2, 1),
          dropTbl(1, 2));

      Replica<CatalogState> result = apply(file, t1, t2, t3, t4);

      // Verify final state
      assertThat(result.state().containsNamespace(Namespace.of("warehouse"))).isTrue();
      assertThat(result.state().containsNamespace(Namespace.of("warehouse", "bronze"))).isTrue();
      assertThat(result.state().containsNamespace(Namespace.of("warehouse", "silver"))).isTrue();
      assertThat(result.state().namespaceProperties(Namespace.of("warehouse")))
          .containsEntry("owner", "data-team");

      // raw_events was dropped
      assertThat(result.state().location(TableIdentifier.of(
          Namespace.of("warehouse", "bronze"), "raw_events"))).isNull();
      // enriched still exists at original location
      assertThat(result.state().location(TableIdentifier.of(
          Namespace.of("warehouse", "silver"), "enriched")))
          .isEqualTo("s3://wh/silver/enriched/v1");
    }

    @Test
    void checkpointRoundtripPreservesAllState() {
      byte[] file = catalog()
          .ns(1, 0, "db", 2)
          .ns(2, 1, "schema", 1)
          .tbl(1, 2, "t1", 3, "s3://t1/v3")
          .tbl(2, 2, "t2", 1, "s3://t2/v1")
          .prop(1, "owner", "alice")
          .prop(1, "env", "prod")
          .prop(2, "comment", "test schema")
          .build();

      // Read from bytes to verify checkpoint roundtrip
      Replica<CatalogState> result = apply(file);

      assertThat(result.state().containsNamespace(Namespace.of("db"))).isTrue();
      assertThat(result.state().containsNamespace(Namespace.of("db", "schema"))).isTrue();
      assertThat(result.state().namespaceVersion(result.state().namespaceId(Namespace.of("db")))).isEqualTo(2);
      assertThat(result.state().namespaceVersion(result.state().namespaceId(Namespace.of("db", "schema")))).isEqualTo(1);

      TableIdentifier t1 = TableIdentifier.of(Namespace.of("db", "schema"), "t1");
      TableIdentifier t2 = TableIdentifier.of(Namespace.of("db", "schema"), "t2");
      assertThat(result.state().location(t1)).isEqualTo("s3://t1/v3");
      assertThat(result.state().tableVersion(result.state().tableId(t1))).isEqualTo(3);
      assertThat(result.state().location(t2)).isEqualTo("s3://t2/v1");

      assertThat(result.state().namespaceProperties(Namespace.of("db")))
          .containsEntry("owner", "alice")
          .containsEntry("env", "prod");
      assertThat(result.state().namespaceProperties(Namespace.of("db", "schema")))
          .containsEntry("comment", "test schema");
    }

    @Test
    void emptyTransactionApplies() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      ProtoCodec.Transaction t = new ProtoCodec.Transaction(UuidV7.newUuidV7(), List.of());
      Replica<CatalogState> result = apply(file, t);

      // No-op transaction should still be recorded
      assertThat(result.committed().contains(t.id())).isTrue();
      assertThat(result.state().containsNamespace(Namespace.of("db"))).isTrue();
    }
  }

  // ============================================================
  // Inline table checkpoint tests
  // ============================================================

  private static final byte[] SAMPLE_INLINE_METADATA =
      "{\"format-version\":2,\"table-uuid\":\"abc\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
  private static final String SAMPLE_MANIFEST_PREFIX = "s3://bucket/db/tbl/metadata/snap-";

  @Nested
  class InlineTableTests {

    @Test
    void checkpointRoundtripWithInlineTable() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .inlineTbl(1, 1, "events", 1, SAMPLE_INLINE_METADATA, SAMPLE_MANIFEST_PREFIX)
          .build();
      Replica<CatalogState> result = apply(file);

      // Table exists and is accessible
      TableIdentifier events = TableIdentifier.of(Namespace.of("db"), "events");
      assertThat(result.state().tables()).contains(events);
      Integer tblId = result.state().tableId(events);
      assertThat(tblId).isNotNull();

      // It's inline (no pointer location, has inline metadata)
      assertThat(result.state().location(events)).isNull();
      assertThat(result.state().isInlineTable(tblId)).isTrue();
      assertThat(result.state().inlineMetadata(tblId)).isEqualTo(SAMPLE_INLINE_METADATA);
      assertThat(result.state().manifestListPrefix(tblId)).isEqualTo(SAMPLE_MANIFEST_PREFIX);
    }

    @Test
    void mixedInlineAndPointerTables() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "pointer_tbl", 1, "s3://bucket/pointer/v1")
          .inlineTbl(2, 1, "inline_tbl", 1, SAMPLE_INLINE_METADATA, SAMPLE_MANIFEST_PREFIX)
          .build();
      Replica<CatalogState> result = apply(file);

      TableIdentifier pointer = TableIdentifier.of(Namespace.of("db"), "pointer_tbl");
      TableIdentifier inline = TableIdentifier.of(Namespace.of("db"), "inline_tbl");

      // Both tables visible
      assertThat(result.state().tables()).containsExactlyInAnyOrder(pointer, inline);

      // Pointer table has location, not inline
      assertThat(result.state().location(pointer)).isEqualTo("s3://bucket/pointer/v1");
      assertThat(result.state().isInlineTable(result.state().tableId(pointer))).isFalse();

      // Inline table has metadata, no location
      assertThat(result.state().location(inline)).isNull();
      assertThat(result.state().isInlineTable(result.state().tableId(inline))).isTrue();
      assertThat(result.state().inlineMetadata(result.state().tableId(inline))).isEqualTo(SAMPLE_INLINE_METADATA);
    }

    @Test
    void inlineTableVersionTracking() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .inlineTbl(1, 1, "tbl", 3, SAMPLE_INLINE_METADATA, SAMPLE_MANIFEST_PREFIX)
          .build();
      Replica<CatalogState> result = apply(file);

      Integer tblId = result.state().tableId(TableIdentifier.of(Namespace.of("db"), "tbl"));
      assertThat(result.state().tableVersion(tblId)).isEqualTo(3);
    }

    @Test
    void inlineTableNotAPointer() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .inlineTbl(1, 1, "tbl", 1, SAMPLE_INLINE_METADATA, SAMPLE_MANIFEST_PREFIX)
          .build();
      Replica<CatalogState> result = apply(file);

      // location() returns null for inline tables -- they don't have an external file
      TableIdentifier tbl = TableIdentifier.of(Namespace.of("db"), "tbl");
      assertThat(result.state().location(tbl)).isNull();
      assertThat(result.state().isInlineTable(result.state().tableId(tbl))).isTrue();

      // A pointer table would return a location string
      assertThat(result.state().inlineMetadata(result.state().tableId(tbl))).isNotNull();
    }

    @Test
    void emptyMetadataBytes() {
      byte[] emptyMeta = "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .inlineTbl(1, 1, "tbl", 1, emptyMeta, "")
          .build();
      Replica<CatalogState> result = apply(file);

      Integer tblId = result.state().tableId(TableIdentifier.of(Namespace.of("db"), "tbl"));
      assertThat(result.state().inlineMetadata(tblId)).isEqualTo(emptyMeta);
      assertThat(result.state().manifestListPrefix(tblId)).isEmpty();
    }

    // --- CreateTableInline action tests ---

    @Test
    void createTableInlineApplies() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      Replica<CatalogState> result = apply(file,
          txn(createTblInline(1, 1, "events", 1, 1, SAMPLE_INLINE_METADATA)));

      TableIdentifier events = TableIdentifier.of(Namespace.of("db"), "events");
      assertThat(result.state().tables()).contains(events);
      assertThat(result.state().isInlineTable(result.state().tableId(events))).isTrue();
      assertThat(result.state().inlineMetadata(result.state().tableId(events))).isEqualTo(SAMPLE_INLINE_METADATA);
      assertThat(result.state().location(events)).isNull();
    }

    @Test
    void createTableInlineRejectsNamespaceVersionMismatch() {
      byte[] file = catalog().ns(1, 0, "db", 3).build();
      ProtoCodec.Transaction t = txn(
          createTblInline(1, 1, "events", 1, 1, SAMPLE_INLINE_METADATA));
      Replica<CatalogState> result = apply(file, t);

      assertThat(result.committed().contains(t.id())).isFalse();
      assertThat(result.state().tables()).isEmpty();
    }

    @Test
    void createTableInlineWithLateBoundNamespace() {
      byte[] file = catalog().build();
      Replica<CatalogState> result = apply(file, txn(
          createNs(1, 0, "db", 1, -1),
          createTblInline(1, 1, "events", 1, -1, SAMPLE_INLINE_METADATA)));

      assertThat(result.state().containsNamespace(Namespace.of("db"))).isTrue();
      TableIdentifier events = TableIdentifier.of(Namespace.of("db"), "events");
      assertThat(result.state().isInlineTable(result.state().tableId(events))).isTrue();
    }

    @Test
    void mixedInlineAndPointerCreatesInSameTransaction() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      Replica<CatalogState> result = apply(file, txn(
          createTbl(1, 1, "pointer", 1, 1, "s3://bucket/pointer/v1"),
          createTblInline(2, 1, "inline", 1, 1, SAMPLE_INLINE_METADATA)));

      TableIdentifier pointer = TableIdentifier.of(Namespace.of("db"), "pointer");
      TableIdentifier inline = TableIdentifier.of(Namespace.of("db"), "inline");

      assertThat(result.state().location(pointer)).isEqualTo("s3://bucket/pointer/v1");
      assertThat(result.state().isInlineTable(result.state().tableId(pointer))).isFalse();
      assertThat(result.state().isInlineTable(result.state().tableId(inline))).isTrue();
    }

    @Test
    void createTableInlineEncodeDecode() {
      // Verify the action survives encode/decode roundtrip
      ProtoCodec.CreateTableInlineAction action =
          createTblInline(1, 1, "tbl", 1, 1, SAMPLE_INLINE_METADATA);
      ProtoCodec.Transaction txn = new ProtoCodec.Transaction(
          UuidV7.newUuidV7(), List.of(action));

      byte[] encoded = ProtoCodec.encodeTransaction(txn);
      ProtoCodec.Transaction decoded = ProtoCodec.decodeTransaction(encoded);

      assertThat(decoded.actions()).hasSize(1);
      assertThat(decoded.actions().get(0)).isInstanceOf(ProtoCodec.CreateTableInlineAction.class);
      ProtoCodec.CreateTableInlineAction decodedAction =
          (ProtoCodec.CreateTableInlineAction) decoded.actions().get(0);
      assertThat(decodedAction.name).isEqualTo("tbl");
      assertThat(decodedAction.metadata).isEqualTo(SAMPLE_INLINE_METADATA);
    }

    // --- UpdateTableInline action tests ---

    @Test
    void updateInlineTableFullMode() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .inlineTbl(1, 1, "tbl", 1, SAMPLE_INLINE_METADATA, SAMPLE_MANIFEST_PREFIX)
          .build();
      byte[] newMeta = "{\"format-version\":2,\"updated\":true}".getBytes(
          java.nio.charset.StandardCharsets.UTF_8);
      Replica<CatalogState> result = apply(file, txn(updateTblInlineFull(1, 1, newMeta)));

      TableIdentifier tbl = TableIdentifier.of(Namespace.of("db"), "tbl");
      assertThat(result.state().isInlineTable(result.state().tableId(tbl))).isTrue();
      assertThat(result.state().inlineMetadata(result.state().tableId(tbl))).isEqualTo(newMeta);
      assertThat(result.state().tableVersion(result.state().tableId(tbl))).isEqualTo(2);
    }

    @Test
    void updateInlineTablePointerModeEvicts() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .inlineTbl(1, 1, "tbl", 1, SAMPLE_INLINE_METADATA, SAMPLE_MANIFEST_PREFIX)
          .build();
      Replica<CatalogState> result = apply(file,
          txn(updateTblInlinePointer(1, 1, "s3://bucket/evicted/v1.metadata.json")));

      TableIdentifier tbl = TableIdentifier.of(Namespace.of("db"), "tbl");
      // Table is now a pointer, no longer inline
      assertThat(result.state().isInlineTable(result.state().tableId(tbl))).isFalse();
      assertThat(result.state().location(tbl)).isEqualTo("s3://bucket/evicted/v1.metadata.json");
      assertThat(result.state().inlineMetadata(result.state().tableId(tbl))).isNull();
      assertThat(result.state().tableVersion(result.state().tableId(tbl))).isEqualTo(2);
    }

    @Test
    void updateInlineTableRejectsVersionMismatch() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .inlineTbl(1, 1, "tbl", 3, SAMPLE_INLINE_METADATA, SAMPLE_MANIFEST_PREFIX)
          .build();
      byte[] newMeta = "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
      ProtoCodec.Transaction t = txn(updateTblInlineFull(1, 1, newMeta));
      Replica<CatalogState> result = apply(file, t);

      // Version 1 doesn't match actual 3
      assertThat(result.committed().contains(t.id())).isFalse();
      assertThat(result.state().inlineMetadata(result.state().tableId(
          TableIdentifier.of(Namespace.of("db"), "tbl")))).isEqualTo(SAMPLE_INLINE_METADATA);
    }

    @Test
    void concurrentInlineUpdatesSecondRejected() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .inlineTbl(1, 1, "tbl", 1, SAMPLE_INLINE_METADATA, SAMPLE_MANIFEST_PREFIX)
          .build();
      byte[] meta1 = "{\"writer\":\"a\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
      byte[] meta2 = "{\"writer\":\"b\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8);

      ProtoCodec.Transaction t1 = txn(updateTblInlineFull(1, 1, meta1));
      ProtoCodec.Transaction t2 = txn(updateTblInlineFull(1, 1, meta2));
      Replica<CatalogState> result = apply(file, t1, t2);

      assertThat(result.committed().contains(t1.id())).isTrue();
      assertThat(result.committed().contains(t2.id())).isFalse();
      assertThat(result.state().inlineMetadata(result.state().tableId(
          TableIdentifier.of(Namespace.of("db"), "tbl")))).isEqualTo(meta1);
    }

    @Test
    void multipleInlineTablesInDifferentNamespaces() {
      byte[] meta1 = "{\"id\":1}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
      byte[] meta2 = "{\"id\":2}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
      byte[] file = catalog()
          .ns(1, 0, "db1", 1)
          .ns(2, 0, "db2", 1)
          .inlineTbl(1, 1, "t1", 1, meta1, "s3://b/db1/t1/metadata/snap-")
          .inlineTbl(2, 2, "t2", 1, meta2, "s3://b/db2/t2/metadata/snap-")
          .build();
      Replica<CatalogState> result = apply(file);

      TableIdentifier t1 = TableIdentifier.of(Namespace.of("db1"), "t1");
      TableIdentifier t2 = TableIdentifier.of(Namespace.of("db2"), "t2");
      assertThat(result.state().inlineMetadata(result.state().tableId(t1))).isEqualTo(meta1);
      assertThat(result.state().inlineMetadata(result.state().tableId(t2))).isEqualTo(meta2);
      assertThat(result.state().manifestListPrefix(result.state().tableId(t1)))
          .isEqualTo("s3://b/db1/t1/metadata/snap-");
      assertThat(result.state().manifestListPrefix(result.state().tableId(t2)))
          .isEqualTo("s3://b/db2/t2/metadata/snap-");
    }

    // --- Mut-level inline API tests (Stage 4) ---

    @Test
    void mutCreateTableInlineProducesCorrectAction() throws IOException {
      CatalogState orig = CatalogState.builder()
          .addNamespace(0, 0, "", 1)
          .addNamespace(1, 0, "db", 1)
          .setNextNamespaceId(2)
          .setNextTableId(1)
          .build();
      byte[] origBytes = toFileBytes(orig);
      Replica<CatalogState> fresh = CatalogTestSupport.replay(origBytes);

      CatalogMutation mut = new CatalogMutation(fresh);
      mut.createTableInline(
          TableIdentifier.of(Namespace.of("db"), "events"),
          SAMPLE_INLINE_METADATA, InlineMetadataCodecs.TAG_JSON_GZIP);
      ProtoCodec.Transaction txn = ProtoCodec.Transaction.from(mut);

      // Should produce a CreateTableInlineAction
      assertThat(txn.actions()).hasSize(1);
      assertThat(txn.actions().get(0))
          .isInstanceOf(ProtoCodec.CreateTableInlineAction.class);

      // Apply and verify
      Replica<CatalogState> result = apply(origBytes, txn);
      TableIdentifier events = TableIdentifier.of(Namespace.of("db"), "events");
      assertThat(result.state().isInlineTable(events)).isTrue();
      assertThat(result.state().inlineMetadata(events)).isEqualTo(SAMPLE_INLINE_METADATA);
    }

    @Test
    void mutUpdateTableInlineProducesCorrectAction() throws IOException {
      CatalogState orig = CatalogState.builder()
          .addNamespace(0, 0, "", 1)
          .addNamespace(1, 0, "db", 1)
          .setNextNamespaceId(2)
          .setNextTableId(2)
          .build();
      // Add inline table directly
      CatalogState.builder()
          .addNamespace(0, 0, "", 1)
          .addNamespace(1, 0, "db", 1)
          .addInlineTable(1, 1, "events", 1, SAMPLE_INLINE_METADATA, SAMPLE_MANIFEST_PREFIX)
          .setNextNamespaceId(2)
          .setNextTableId(2)
          .build();

      // Build from checkpoint with inline table
      byte[] origBytes = catalog()
          .ns(1, 0, "db", 1)
          .inlineTbl(1, 1, "events", 1, SAMPLE_INLINE_METADATA, SAMPLE_MANIFEST_PREFIX)
          .build();
      Replica<CatalogState> fresh = CatalogTestSupport.replay(origBytes);

      byte[] newMeta = "{\"updated\":true}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
      CatalogMutation mut = new CatalogMutation(fresh);
      mut.updateTableInline(
          TableIdentifier.of(Namespace.of("db"), "events"),
          newMeta, InlineMetadataCodecs.TAG_JSON_GZIP);
      ProtoCodec.Transaction txn = ProtoCodec.Transaction.from(mut);

      // Should produce an UpdateTableInlineAction
      assertThat(txn.actions()).hasSize(1);
      assertThat(txn.actions().get(0))
          .isInstanceOf(ProtoCodec.UpdateTableInlineAction.class);

      // Apply and verify
      Replica<CatalogState> result = apply(origBytes, txn);
      TableIdentifier events = TableIdentifier.of(Namespace.of("db"), "events");
      assertThat(result.state().isInlineTable(events)).isTrue();
      assertThat(result.state().inlineMetadata(events)).isEqualTo(newMeta);
      assertThat(result.state().tableVersion(result.state().tableId(events))).isEqualTo(2);
    }

    @Test
    void mutMixedInlineAndPointerInSameTransaction() throws IOException {
      byte[] origBytes = catalog().ns(1, 0, "db", 1).build();
      Replica<CatalogState> fresh = CatalogTestSupport.replay(origBytes);

      CatalogMutation mut = new CatalogMutation(fresh);
      mut.createTable(
          TableIdentifier.of(Namespace.of("db"), "pointer_tbl"), "s3://bucket/pointer/v1");
      mut.createTableInline(
          TableIdentifier.of(Namespace.of("db"), "inline_tbl"),
          SAMPLE_INLINE_METADATA, InlineMetadataCodecs.TAG_JSON_GZIP);
      ProtoCodec.Transaction txn = ProtoCodec.Transaction.from(mut);

      assertThat(txn.actions()).hasSize(2);

      Replica<CatalogState> result = apply(origBytes, txn);
      TableIdentifier pointer = TableIdentifier.of(Namespace.of("db"), "pointer_tbl");
      TableIdentifier inline = TableIdentifier.of(Namespace.of("db"), "inline_tbl");
      assertThat(result.state().location(pointer)).isEqualTo("s3://bucket/pointer/v1");
      assertThat(result.state().isInlineTable(inline)).isTrue();
    }
  }

  // ============================================================
  // Inline-TM codec round-trip tests
  // ============================================================

  /**
   * Codec discriminator must round-trip through encode → wire → decode for
   * each of the three sites that carry full inline TableMetadata bytes:
   * CreateTableInlineAction, UpdateTableInlineAction (full mode), and the
   * checkpoint InlineTable record. Mixed-codec catalogs (table A with one
   * codec, table B with another) must also round-trip.
   */
  @Nested
  class InlineTableCodecTests {

    @Test
    void createInlineActionPreservesCodec() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      byte[] meta = new byte[] {1, 2, 3, 4, 5};

      ProtoCodec.Transaction txn = txn(
          createTblInline(-1, 1, "tbl", 1, 1, meta, InlineMetadataCodecs.TAG_STRUCTURAL));
      Replica<CatalogState> result = apply(file, txn);

      Integer id = result.state().tableId(TableIdentifier.of(Namespace.of("db"), "tbl"));
      assertThat(id).isNotNull();
      assertThat(result.state().inlineMetadata(id)).isEqualTo(meta);
      assertThat(result.state().inlineMetadataCodec(id))
          .isEqualTo(InlineMetadataCodecs.TAG_STRUCTURAL);
    }

    @Test
    void updateInlineFullActionPreservesCodec() {
      byte[] meta0 = new byte[] {0x10, 0x20};
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .inlineTbl(1, 1, "tbl", 1, meta0, InlineMetadataCodecs.TAG_JSON_GZIP, "")
          .build();

      byte[] meta1 = new byte[] {0x30, 0x40, 0x50};
      ProtoCodec.Transaction txn = txn(
          updateTblInlineFull(1, 1, meta1, InlineMetadataCodecs.TAG_STRUCTURAL));
      Replica<CatalogState> result = apply(file, txn);

      Integer id = result.state().tableId(TableIdentifier.of(Namespace.of("db"), "tbl"));
      assertThat(result.state().inlineMetadata(id)).isEqualTo(meta1);
      assertThat(result.state().inlineMetadataCodec(id))
          .as("full-mode update must record the codec on the new inline bytes")
          .isEqualTo(InlineMetadataCodecs.TAG_STRUCTURAL);
    }

    @Test
    void checkpointInlineTablePreservesCodec() {
      byte[] meta = new byte[] {(byte) 0xAA, (byte) 0xBB};
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .inlineTbl(1, 1, "tbl", 1, meta, InlineMetadataCodecs.TAG_STRUCTURAL, "prefix/")
          .build();

      // Reading the file back exercises the checkpoint encode + decode path.
      CatalogState decoded = decodeFile(file);
      Integer id = decoded.tableId(TableIdentifier.of(Namespace.of("db"), "tbl"));
      assertThat(decoded.inlineMetadata(id)).isEqualTo(meta);
      assertThat(decoded.inlineMetadataCodec(id))
          .isEqualTo(InlineMetadataCodecs.TAG_STRUCTURAL);
    }

    @Test
    void mixedCodecsRoundTrip() {
      byte[] gzipMeta = new byte[] {1, 2, 3};
      byte[] structMeta = new byte[] {7, 8, 9, 10};
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .inlineTbl(1, 1, "a", 1, gzipMeta, InlineMetadataCodecs.TAG_JSON_GZIP, "")
          .inlineTbl(2, 1, "b", 1, structMeta, InlineMetadataCodecs.TAG_STRUCTURAL, "")
          .build();

      CatalogState decoded = decodeFile(file);
      Integer aId = decoded.tableId(TableIdentifier.of(Namespace.of("db"), "a"));
      Integer bId = decoded.tableId(TableIdentifier.of(Namespace.of("db"), "b"));

      assertThat(decoded.inlineMetadata(aId)).isEqualTo(gzipMeta);
      assertThat(decoded.inlineMetadataCodec(aId))
          .isEqualTo(InlineMetadataCodecs.TAG_JSON_GZIP);
      assertThat(decoded.inlineMetadata(bId)).isEqualTo(structMeta);
      assertThat(decoded.inlineMetadataCodec(bId))
          .isEqualTo(InlineMetadataCodecs.TAG_STRUCTURAL);
    }

    @Test
    void unsetCodecDecodesAsGzip() {
      // Default proto3 enum value is 0 = CODEC_JSON_GZIP. The encoder omits
      // the field when the value matches the default, so a record with no
      // codec field on the wire must decode as gzip.
      byte[] meta = new byte[] {0x42};
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .inlineTbl(1, 1, "tbl", 1, meta, "")  // no-codec overload → defaults to gzip
          .build();

      CatalogState decoded = decodeFile(file);
      Integer id = decoded.tableId(TableIdentifier.of(Namespace.of("db"), "tbl"));
      assertThat(decoded.inlineMetadataCodec(id))
          .as("missing codec field on the wire must decode as the proto3 default")
          .isEqualTo(InlineMetadataCodecs.TAG_JSON_GZIP);
    }
  }

  /** Decodes a serialized catalog file back to a CatalogState. */
  private static CatalogState decodeFile(byte[] file) {
    return CatalogTestSupport.decodeFullFile(file);
  }

  // ============================================================
  // Inline manifest list checkpoint tests
  // ============================================================

  @Nested
  class InlineManifestListTests {

    private static final String MANIFEST_PREFIX = "s3://bucket/db/tbl/metadata/";

    @Test
    void checkpointRoundtripWithManifestPool() {
      // Build a catalog with an inline table that has manifest pool + snapshot refs
      CatalogState.Builder b = CatalogState.builder();
      b.addNamespace(0, 0, "", 1);
      b.addNamespace(1, 0, "db", 1);
      b.setNextNamespaceId(2);
      b.setNextTableId(2);
      b.addInlineTable(1, 1, "tbl", 1, SAMPLE_INLINE_METADATA, MANIFEST_PREFIX);

      // Add 3 manifests to the pool
      org.apache.iceberg.ManifestFile m1 = new TestManifestFile(
          MANIFEST_PREFIX + "aaa-m0.avro", 1024, 0,
          org.apache.iceberg.ManifestContent.DATA, 1, 1, 100L,
          5, 0, 0, 50L, 0L, 0L, null, null, null);
      org.apache.iceberg.ManifestFile m2 = new TestManifestFile(
          MANIFEST_PREFIX + "bbb-m0.avro", 2048, 0,
          org.apache.iceberg.ManifestContent.DATA, 2, 2, 200L,
          10, 0, 0, 100L, 0L, 0L, null, null, null);
      org.apache.iceberg.ManifestFile m3 = new TestManifestFile(
          MANIFEST_PREFIX + "ccc-m0.avro", 4096, 0,
          org.apache.iceberg.ManifestContent.DATA, 3, 3, 300L,
          8, 5, 0, 80L, 50L, 0L, null, null, null);
      b.addManifestToPool(1, m1);
      b.addManifestToPool(1, m2);
      b.addManifestToPool(1, m3);

      // Snapshot 100 has manifests m1 only; snapshot 200 has m1+m2; snapshot 300 has m1+m2+m3
      b.setSnapshotManifests(1, 100L, List.of(m1.path()));
      b.setSnapshotManifests(1, 200L, List.of(m2.path(), m1.path()));
      b.setSnapshotManifests(1, 300L, List.of(m3.path(), m2.path(), m1.path()));

      CatalogState original = b.build();

      // Serialize to file bytes and re-read
      byte[] fileBytes = toFileBytes(original);
      Replica<CatalogState> result = apply(fileBytes);

      // Verify table exists and is inline
      TableIdentifier tbl = TableIdentifier.of(Namespace.of("db"), "tbl");
      Integer tblId = result.state().tableId(tbl);
      assertThat(tblId).isNotNull();
      assertThat(result.state().isInlineTable(tblId)).isTrue();

      // Verify manifest pool round-tripped
      assertThat(result.state().hasInlineManifests(tblId, 100L)).isTrue();
      assertThat(result.state().hasInlineManifests(tblId, 200L)).isTrue();
      assertThat(result.state().hasInlineManifests(tblId, 300L)).isTrue();
      assertThat(result.state().hasInlineManifests(tblId, 999L)).isFalse();

      // Snapshot 100: just m1
      List<org.apache.iceberg.ManifestFile> snap100 = result.state().inlineManifests(tblId, 100L);
      assertThat(snap100).hasSize(1);
      assertThat(snap100.get(0).path()).isEqualTo(m1.path());
      assertThat(snap100.get(0).length()).isEqualTo(1024);
      assertThat(snap100.get(0).snapshotId()).isEqualTo(100L);
      assertThat(snap100.get(0).addedFilesCount()).isEqualTo(5);

      // Snapshot 200: m2, m1
      List<org.apache.iceberg.ManifestFile> snap200 = result.state().inlineManifests(tblId, 200L);
      assertThat(snap200).hasSize(2);
      assertThat(snap200.get(0).path()).isEqualTo(m2.path());
      assertThat(snap200.get(1).path()).isEqualTo(m1.path());

      // Snapshot 300: m3, m2, m1 -- pool sharing verified (m1 + m2 stored once)
      List<org.apache.iceberg.ManifestFile> snap300 = result.state().inlineManifests(tblId, 300L);
      assertThat(snap300).hasSize(3);
      assertThat(snap300.get(0).path()).isEqualTo(m3.path());
      assertThat(snap300.get(0).existingFilesCount()).isEqualTo(5);
      assertThat(snap300.get(0).existingRowsCount()).isEqualTo(50L);
    }

    @Test
    void emptyManifestPoolRoundtrip() {
      // Inline table with no manifest pool data (backward compat)
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .inlineTbl(1, 1, "tbl", 1, SAMPLE_INLINE_METADATA, MANIFEST_PREFIX)
          .build();
      Replica<CatalogState> result = apply(file);

      Integer tblId = result.state().tableId(TableIdentifier.of(Namespace.of("db"), "tbl"));
      assertThat(result.state().hasInlineManifests(tblId, 1L)).isFalse();
      assertThat(result.state().inlineManifests(tblId, 1L)).isNull();
    }

    @Test
    void multipleTablesWithIndependentPools() {
      CatalogState.Builder b = CatalogState.builder();
      b.addNamespace(0, 0, "", 1);
      b.addNamespace(1, 0, "db", 1);
      b.setNextNamespaceId(2);
      b.setNextTableId(3);

      String prefix1 = "s3://bucket/db/t1/metadata/";
      String prefix2 = "s3://bucket/db/t2/metadata/";
      b.addInlineTable(1, 1, "t1", 1, SAMPLE_INLINE_METADATA, prefix1);
      b.addInlineTable(2, 1, "t2", 1, SAMPLE_INLINE_METADATA, prefix2);

      org.apache.iceberg.ManifestFile m1 = new TestManifestFile(
          prefix1 + "aaa-m0.avro", 1024, 0,
          org.apache.iceberg.ManifestContent.DATA, 1, 1, 10L,
          1, 0, 0, 10L, 0L, 0L, null, null, null);
      org.apache.iceberg.ManifestFile m2 = new TestManifestFile(
          prefix2 + "bbb-m0.avro", 2048, 0,
          org.apache.iceberg.ManifestContent.DATA, 1, 1, 20L,
          2, 0, 0, 20L, 0L, 0L, null, null, null);

      b.addManifestToPool(1, m1);
      b.setSnapshotManifests(1, 10L, List.of(m1.path()));
      b.addManifestToPool(2, m2);
      b.setSnapshotManifests(2, 20L, List.of(m2.path()));

      byte[] fileBytes = toFileBytes(b.build());
      Replica<CatalogState> result = apply(fileBytes);

      Integer t1Id = result.state().tableId(TableIdentifier.of(Namespace.of("db"), "t1"));
      Integer t2Id = result.state().tableId(TableIdentifier.of(Namespace.of("db"), "t2"));

      assertThat(result.state().hasInlineManifests(t1Id, 10L)).isTrue();
      assertThat(result.state().hasInlineManifests(t2Id, 20L)).isTrue();
      assertThat(result.state().hasInlineManifests(t1Id, 20L)).isFalse();
      assertThat(result.state().hasInlineManifests(t2Id, 10L)).isFalse();

      assertThat(result.state().inlineManifests(t1Id, 10L).get(0).path()).isEqualTo(m1.path());
      assertThat(result.state().inlineManifests(t2Id, 20L).get(0).path()).isEqualTo(m2.path());
    }
  }

  // ============================================================
  // InlineSnapshot equals/hashCode tests (§2.1)
  // ============================================================

  @Nested
  class InlineSnapshotEqualityTests {

    @Test
    void inlineSnapshotEqualsItself() {
      org.apache.iceberg.InlineSnapshot snap = new org.apache.iceberg.InlineSnapshot(
          1L, 100L, null, 1000L, "append",
          Map.of("operation", "append"), 0, null, null, null, List.of());
      assertThat(snap).isEqualTo(snap);
    }

    @Test
    void twoInlineSnapshotsWithSameFieldsAreEqual() {
      org.apache.iceberg.InlineSnapshot a = new org.apache.iceberg.InlineSnapshot(
          5L, 200L, 100L, 2000L, "append",
          Map.of("operation", "append"), 1, null, null, null, List.of());
      org.apache.iceberg.InlineSnapshot b = new org.apache.iceberg.InlineSnapshot(
          5L, 200L, 100L, 2000L, "overwrite",
          Map.of("operation", "overwrite"), 1, null, null, null, List.of());
      // equals compares snapshotId, parentId, sequenceNumber, timestampMillis, schemaId
      assertThat(a).isEqualTo(b);
      assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void inlineSnapshotNotEqualDifferentId() {
      org.apache.iceberg.InlineSnapshot a = new org.apache.iceberg.InlineSnapshot(
          1L, 100L, null, 1000L, "append",
          Map.of("operation", "append"), 0, null, null, null, List.of());
      org.apache.iceberg.InlineSnapshot b = new org.apache.iceberg.InlineSnapshot(
          1L, 999L, null, 1000L, "append",
          Map.of("operation", "append"), 0, null, null, null, List.of());
      assertThat(a).isNotEqualTo(b);
    }
  }

  // ============================================================
  // ManifestFileEntry codec tests
  // ============================================================

  @Nested
  class ManifestFileEntryCodecTests {

    private static final String MANIFEST_PREFIX = "s3://bucket/db/tbl/metadata/";

    @Test
    void roundtripAllFields() throws Exception {
      java.nio.ByteBuffer lb = java.nio.ByteBuffer.wrap(new byte[]{1, 2, 3, 4});
      java.nio.ByteBuffer ub = java.nio.ByteBuffer.wrap(new byte[]{5, 6, 7, 8});
      java.nio.ByteBuffer km = java.nio.ByteBuffer.wrap(new byte[]{10, 20, 30});

      org.apache.iceberg.ManifestFile original = new TestManifestFile(
          MANIFEST_PREFIX + "abc-m0.avro", 4096L, 1,
          org.apache.iceberg.ManifestContent.DATA,
          7L, 5L, 12345L,
          10, 90, 3, 100L, 900L, 30L,
          List.of(new TestPartitionFieldSummary(true, false, lb, ub)),
          km, 42L);

      byte[] encoded = ProtoCodec.encodeManifestFileEntry(original, MANIFEST_PREFIX);
      org.apache.iceberg.ManifestFile decoded =
          ProtoCodec.decodeManifestFileEntry(encoded, MANIFEST_PREFIX);

      assertThat(decoded.path()).isEqualTo(original.path());
      assertThat(decoded.length()).isEqualTo(original.length());
      assertThat(decoded.partitionSpecId()).isEqualTo(original.partitionSpecId());
      assertThat(decoded.content()).isEqualTo(original.content());
      assertThat(decoded.sequenceNumber()).isEqualTo(original.sequenceNumber());
      assertThat(decoded.minSequenceNumber()).isEqualTo(original.minSequenceNumber());
      assertThat(decoded.snapshotId()).isEqualTo(original.snapshotId());
      assertThat(decoded.addedFilesCount()).isEqualTo(original.addedFilesCount());
      assertThat(decoded.existingFilesCount()).isEqualTo(original.existingFilesCount());
      assertThat(decoded.deletedFilesCount()).isEqualTo(original.deletedFilesCount());
      assertThat(decoded.addedRowsCount()).isEqualTo(original.addedRowsCount());
      assertThat(decoded.existingRowsCount()).isEqualTo(original.existingRowsCount());
      assertThat(decoded.deletedRowsCount()).isEqualTo(original.deletedRowsCount());
      assertThat(decoded.firstRowId()).isEqualTo(original.firstRowId());

      // Partition summaries
      assertThat(decoded.partitions()).hasSize(1);
      var pfs = decoded.partitions().get(0);
      assertThat(pfs.containsNull()).isTrue();
      assertThat(pfs.containsNaN()).isFalse();
      assertThat(pfs.lowerBound()).isEqualTo(lb.duplicate());
      assertThat(pfs.upperBound()).isEqualTo(ub.duplicate());

      // Key metadata
      assertThat(decoded.keyMetadata()).isEqualTo(km.duplicate());
    }

    @Test
    void roundtripMinimalManifest() throws Exception {
      // Brand-new manifest from FastAppend: many fields are zero/null/default
      org.apache.iceberg.ManifestFile original = new TestManifestFile(
          MANIFEST_PREFIX + "def-m0.avro", 1024L, 0,
          org.apache.iceberg.ManifestContent.DATA,
          1L, 1L, 99L,
          5, 0, 0, 50L, 0L, 0L,
          null, null, null);

      byte[] encoded = ProtoCodec.encodeManifestFileEntry(original, MANIFEST_PREFIX);
      org.apache.iceberg.ManifestFile decoded =
          ProtoCodec.decodeManifestFileEntry(encoded, MANIFEST_PREFIX);

      assertThat(decoded.path()).isEqualTo(original.path());
      assertThat(decoded.length()).isEqualTo(1024L);
      assertThat(decoded.partitionSpecId()).isEqualTo(0);
      assertThat(decoded.content()).isEqualTo(org.apache.iceberg.ManifestContent.DATA);
      assertThat(decoded.sequenceNumber()).isEqualTo(1L);
      assertThat(decoded.snapshotId()).isEqualTo(99L);
      assertThat(decoded.addedFilesCount()).isEqualTo(5);
      assertThat(decoded.existingFilesCount()).isEqualTo(0);
      assertThat(decoded.deletedFilesCount()).isEqualTo(0);
      assertThat(decoded.addedRowsCount()).isEqualTo(50L);
      assertThat(decoded.existingRowsCount()).isEqualTo(0L);
      assertThat(decoded.deletedRowsCount()).isEqualTo(0L);
      assertThat(decoded.partitions()).isNull();
      assertThat(decoded.keyMetadata()).isNull();
      assertThat(decoded.firstRowId()).isNull();
    }

    @Test
    void roundtripDeleteManifest() throws Exception {
      org.apache.iceberg.ManifestFile original = new TestManifestFile(
          MANIFEST_PREFIX + "ghi-m0.avro", 2048L, 2,
          org.apache.iceberg.ManifestContent.DELETES,
          10L, 8L, 200L,
          3, 0, 0, 15L, 0L, 0L,
          null, null, null);

      byte[] encoded = ProtoCodec.encodeManifestFileEntry(original, MANIFEST_PREFIX);
      org.apache.iceberg.ManifestFile decoded =
          ProtoCodec.decodeManifestFileEntry(encoded, MANIFEST_PREFIX);

      assertThat(decoded.content()).isEqualTo(org.apache.iceberg.ManifestContent.DELETES);
      assertThat(decoded.partitionSpecId()).isEqualTo(2);
    }

    @Test
    void roundtripMultiplePartitionSummaries() throws Exception {
      java.nio.ByteBuffer lb1 = java.nio.ByteBuffer.wrap(new byte[]{0});
      java.nio.ByteBuffer ub1 = java.nio.ByteBuffer.wrap(new byte[]{100});
      java.nio.ByteBuffer lb2 = java.nio.ByteBuffer.wrap("aaa".getBytes());
      java.nio.ByteBuffer ub2 = java.nio.ByteBuffer.wrap("zzz".getBytes());

      org.apache.iceberg.ManifestFile original = new TestManifestFile(
          MANIFEST_PREFIX + "multi-m0.avro", 8192L, 3,
          org.apache.iceberg.ManifestContent.DATA,
          5L, 5L, 300L,
          20, 0, 0, 200L, 0L, 0L,
          List.of(
              new TestPartitionFieldSummary(false, true, lb1, ub1),
              new TestPartitionFieldSummary(true, false, lb2, ub2)),
          null, null);

      byte[] encoded = ProtoCodec.encodeManifestFileEntry(original, MANIFEST_PREFIX);
      org.apache.iceberg.ManifestFile decoded =
          ProtoCodec.decodeManifestFileEntry(encoded, MANIFEST_PREFIX);

      assertThat(decoded.partitions()).hasSize(2);
      assertThat(decoded.partitions().get(0).containsNull()).isFalse();
      assertThat(decoded.partitions().get(0).containsNaN()).isTrue();
      assertThat(decoded.partitions().get(0).lowerBound()).isEqualTo(lb1.duplicate());
      assertThat(decoded.partitions().get(1).containsNull()).isTrue();
      assertThat(decoded.partitions().get(1).containsNaN()).isFalse();
      assertThat(decoded.partitions().get(1).lowerBound()).isEqualTo(lb2.duplicate());
      assertThat(decoded.partitions().get(1).upperBound()).isEqualTo(ub2.duplicate());
    }

    @Test
    void pathPrefixStrippingAndRestore() throws Exception {
      String prefix = "gs://my-bucket/warehouse/db/tbl/metadata/";
      String fullPath = prefix + "a1b2c3-m0.avro";

      org.apache.iceberg.ManifestFile original = new TestManifestFile(
          fullPath, 512L, 0,
          org.apache.iceberg.ManifestContent.DATA,
          1L, 1L, 1L,
          1, 0, 0, 10L, 0L, 0L,
          null, null, null);

      byte[] encoded = ProtoCodec.encodeManifestFileEntry(original, prefix);
      org.apache.iceberg.ManifestFile decoded =
          ProtoCodec.decodeManifestFileEntry(encoded, prefix);

      assertThat(decoded.path()).isEqualTo(fullPath);

      // Verify the suffix was actually stripped (encoded bytes should be smaller)
      org.apache.iceberg.ManifestFile decodedNoPrefix =
          ProtoCodec.decodeManifestFileEntry(encoded, "");
      assertThat(decodedNoPrefix.path()).isEqualTo("a1b2c3-m0.avro");
    }

    @Test
    void emptyPrefix() throws Exception {
      org.apache.iceberg.ManifestFile original = new TestManifestFile(
          "s3://bucket/path/m0.avro", 100L, 0,
          org.apache.iceberg.ManifestContent.DATA,
          1L, 1L, 1L,
          1, 0, 0, 1L, 0L, 0L,
          null, null, null);

      byte[] encoded = ProtoCodec.encodeManifestFileEntry(original, "");
      org.apache.iceberg.ManifestFile decoded =
          ProtoCodec.decodeManifestFileEntry(encoded, "");

      assertThat(decoded.path()).isEqualTo("s3://bucket/path/m0.avro");
    }
  }

  // ============================================================
  // Test ManifestFile / PartitionFieldSummary implementations
  // ============================================================

  /** Minimal ManifestFile for test construction. */
  static class TestManifestFile implements org.apache.iceberg.ManifestFile {
    private final String path;
    private final long length;
    private final int specId;
    private final org.apache.iceberg.ManifestContent content;
    private final long sequenceNumber;
    private final long minSequenceNumber;
    private final Long snapshotId;
    private final Integer addedFilesCount;
    private final Integer existingFilesCount;
    private final Integer deletedFilesCount;
    private final Long addedRowsCount;
    private final Long existingRowsCount;
    private final Long deletedRowsCount;
    private final List<org.apache.iceberg.ManifestFile.PartitionFieldSummary> partitions;
    private final java.nio.ByteBuffer keyMetadata;
    private final Long firstRowId;

    TestManifestFile(
        String path, long length, int specId,
        org.apache.iceberg.ManifestContent content,
        long sequenceNumber, long minSequenceNumber, Long snapshotId,
        int addedFilesCount, int existingFilesCount, int deletedFilesCount,
        long addedRowsCount, long existingRowsCount, long deletedRowsCount,
        List<org.apache.iceberg.ManifestFile.PartitionFieldSummary> partitions,
        java.nio.ByteBuffer keyMetadata, Long firstRowId) {
      this.path = path;
      this.length = length;
      this.specId = specId;
      this.content = content;
      this.sequenceNumber = sequenceNumber;
      this.minSequenceNumber = minSequenceNumber;
      this.snapshotId = snapshotId;
      this.addedFilesCount = addedFilesCount;
      this.existingFilesCount = existingFilesCount;
      this.deletedFilesCount = deletedFilesCount;
      this.addedRowsCount = addedRowsCount;
      this.existingRowsCount = existingRowsCount;
      this.deletedRowsCount = deletedRowsCount;
      this.partitions = partitions;
      this.keyMetadata = keyMetadata;
      this.firstRowId = firstRowId;
    }

    @Override public String path() { return path; }
    @Override public long length() { return length; }
    @Override public int partitionSpecId() { return specId; }
    @Override public org.apache.iceberg.ManifestContent content() { return content; }
    @Override public long sequenceNumber() { return sequenceNumber; }
    @Override public long minSequenceNumber() { return minSequenceNumber; }
    @Override public Long snapshotId() { return snapshotId; }
    @Override public Integer addedFilesCount() { return addedFilesCount; }
    @Override public Integer existingFilesCount() { return existingFilesCount; }
    @Override public Integer deletedFilesCount() { return deletedFilesCount; }
    @Override public Long addedRowsCount() { return addedRowsCount; }
    @Override public Long existingRowsCount() { return existingRowsCount; }
    @Override public Long deletedRowsCount() { return deletedRowsCount; }
    @Override public List<org.apache.iceberg.ManifestFile.PartitionFieldSummary> partitions() { return partitions; }
    @Override public java.nio.ByteBuffer keyMetadata() { return keyMetadata; }
    @Override public Long firstRowId() { return firstRowId; }
    @Override public org.apache.iceberg.ManifestFile copy() { return this; }
  }

  /** Minimal PartitionFieldSummary for test construction. */
  static class TestPartitionFieldSummary implements org.apache.iceberg.ManifestFile.PartitionFieldSummary {
    private final boolean containsNull;
    private final Boolean containsNaN;
    private final java.nio.ByteBuffer lowerBound;
    private final java.nio.ByteBuffer upperBound;

    TestPartitionFieldSummary(boolean containsNull, boolean containsNaN,
        java.nio.ByteBuffer lowerBound, java.nio.ByteBuffer upperBound) {
      this.containsNull = containsNull;
      this.containsNaN = containsNaN;
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
    }

    @Override public boolean containsNull() { return containsNull; }
    @Override public Boolean containsNaN() { return containsNaN; }
    @Override public java.nio.ByteBuffer lowerBound() { return lowerBound; }
    @Override public java.nio.ByteBuffer upperBound() { return upperBound; }
    @Override public org.apache.iceberg.ManifestFile.PartitionFieldSummary copy() { return this; }
  }

  // ============================================================
  // Idempotency tests -- each action's apply() mutates state so that the
  // same intention record, replayed with a fresh UUID (bypassing the
  // committed-txn dedup set), fails verify against the post-apply state.
  // See Invariant I2 in docs/catalog_errata.md.
  // ============================================================

  @Nested
  class IdempotencyTests {

    @Test
    void createNamespaceRejectedWhenParentVersionMoved() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      // First txn: create child under "db" at parentVersion=1
      ProtoCodec.Transaction first = txn(createNs(2, 1, "schema", 1, 1));
      byte[] afterFirst = replayAndSerialize(file, first);

      // Replay the same logical action against post-apply state with fresh
      // UUID: parent "db" is now v=2 (bumped by child create), action captured
      // parentVersion=1, must be rejected.
      ProtoCodec.Transaction replay = txn(createNs(3, 1, "schema2", 1, 1));
      Replica<CatalogState> result = apply(afterFirst, replay);
      assertThat(result.state().containsNamespace(Namespace.of("db", "schema"))).isTrue();
      assertThat(result.state().containsNamespace(Namespace.of("db", "schema2"))).isFalse();
      assertThat(result.committed().contains(replay.id())).isFalse();
    }

    @Test
    void createTableReplayAfterDropDoesNotRecreate() {
      // User-specified scenario: "create with record A, delete with B,
      // re-appending A should not recreate the table."
      byte[] file = catalog().ns(1, 0, "db", 1).build();

      // A: create table t1 under "db" capturing nsVersion=1
      ProtoCodec.Transaction recordA = txn(
          createTbl(1, 1, "t1", 1, 1, "s3://t1/v1"));
      // B: drop t1 capturing tbl version=1
      ProtoCodec.Transaction recordB = txn(dropTbl(1, 1));

      byte[] afterAB = replayAndSerialize(file, recordA, recordB);

      // Re-apply content of A with a fresh UUID. "db" is now at v=3 (create
      // and drop each bumped it); the captured nsVersion=1 must be rejected.
      ProtoCodec.Transaction replayA = txn(
          createTbl(1, 1, "t1", 1, 1, "s3://t1/v1"));
      Replica<CatalogState> result = apply(afterAB, replayA);

      assertThat(result.state().tableId(TableIdentifier.of(Namespace.of("db"), "t1")))
          .as("replayed create must not resurrect the dropped table")
          .isNull();
      assertThat(result.committed().contains(replayA.id())).isFalse();
    }

    @Test
    void createTableInlineReplayAfterDropDoesNotRecreate() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      byte[] md = "{\"format-version\":2}".getBytes();

      ProtoCodec.Transaction recordA = txn(createTblInline(1, 1, "t1", 1, 1, md));
      ProtoCodec.Transaction recordB = txn(dropTbl(1, 1));
      byte[] afterAB = replayAndSerialize(file, recordA, recordB);

      ProtoCodec.Transaction replayA = txn(createTblInline(1, 1, "t1", 1, 1, md));
      Replica<CatalogState> result = apply(afterAB, replayA);

      assertThat(result.state().tableId(TableIdentifier.of(Namespace.of("db"), "t1")))
          .isNull();
      assertThat(result.committed().contains(replayA.id())).isFalse();
    }

    @Test
    void setNamespacePropertyReplayRejectedAfterBump() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      ProtoCodec.Transaction first = txn(setNsProp(1, 1, "owner", "alice"));
      byte[] afterFirst = replayAndSerialize(file, first);

      // Replay: ns version moved 1 -> 2; captured version=1 must be rejected.
      ProtoCodec.Transaction replay = txn(setNsProp(1, 1, "owner", "bob"));
      Replica<CatalogState> result = apply(afterFirst, replay);
      assertThat(result.state().namespaceProperties(Namespace.of("db")))
          .containsEntry("owner", "alice");
      assertThat(result.committed().contains(replay.id())).isFalse();
    }

    @Test
    void removeNamespacePropertyReplayRejectedAfterBump() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .prop(1, "owner", "alice")
          .prop(1, "env", "prod")
          .build();
      ProtoCodec.Transaction first = txn(removeNsProp(1, 1, "owner"));
      byte[] afterFirst = replayAndSerialize(file, first);

      // ns bumped 1 -> 2; captured v=1 must be rejected.
      ProtoCodec.Transaction replay = txn(removeNsProp(1, 1, "env"));
      Replica<CatalogState> result = apply(afterFirst, replay);
      assertThat(result.state().namespaceProperties(Namespace.of("db")))
          .doesNotContainKey("owner")
          .containsEntry("env", "prod");
      assertThat(result.committed().contains(replay.id())).isFalse();
    }

    @Test
    void dropNamespaceReplayRejectedOnMissingNamespace() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      ProtoCodec.Transaction first = txn(dropNs(1, 1));
      byte[] afterFirst = replayAndSerialize(file, first);

      // Replay: ns is gone; namespaceVersion returns -1, which != captured 1.
      ProtoCodec.Transaction replay = txn(dropNs(1, 1));
      Replica<CatalogState> result = apply(afterFirst, replay);
      assertThat(result.state().containsNamespace(Namespace.of("db"))).isFalse();
      assertThat(result.committed().contains(replay.id())).isFalse();
    }

    @Test
    void dropTableReplayRejectedOnMissingTable() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "t1", 1, "s3://t1/v1")
          .build();
      ProtoCodec.Transaction first = txn(dropTbl(1, 1));
      byte[] afterFirst = replayAndSerialize(file, first);

      // tbl is gone; tableVersion returns -1, captured=1, rejected.
      ProtoCodec.Transaction replay = txn(dropTbl(1, 1));
      Replica<CatalogState> result = apply(afterFirst, replay);
      assertThat(result.committed().contains(replay.id())).isFalse();
    }

    @Test
    void updateTableLocationReplayRejectedAfterVersionBump() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "t1", 1, "s3://t1/v1")
          .build();
      ProtoCodec.Transaction first = txn(updateTbl(1, 1, "s3://t1/v2"));
      byte[] afterFirst = replayAndSerialize(file, first);

      // tbl version bumped 1 -> 2; replay with captured v=1 rejected.
      ProtoCodec.Transaction replay = txn(updateTbl(1, 1, "s3://t1/v3"));
      Replica<CatalogState> result = apply(afterFirst, replay);
      assertThat(result.state().location(TableIdentifier.of(Namespace.of("db"), "t1")))
          .isEqualTo("s3://t1/v2");
      assertThat(result.committed().contains(replay.id())).isFalse();
    }

    @Test
    void renameTableReplayRejectedAfterVersionBump() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "users", 1, "s3://users/v1")
          .build();
      ProtoCodec.Transaction first = txn(renameTbl(1, 1, 1, 1, "people"));
      byte[] afterFirst = replayAndSerialize(file, first);

      // tbl version bumped 1 -> 2; replay capturing tbl_v=1 must be rejected.
      ProtoCodec.Transaction replay = txn(renameTbl(1, 1, 1, 2, "humans"));
      Replica<CatalogState> result = apply(afterFirst, replay);
      Namespace db = Namespace.of("db");
      assertThat(result.state().tableId(TableIdentifier.of(db, "people"))).isEqualTo(1);
      assertThat(result.state().tableId(TableIdentifier.of(db, "humans"))).isNull();
      assertThat(result.committed().contains(replay.id())).isFalse();
    }

    @Test
    void duplicateUuidIsSkippedAtReplay() {
      // Complementary to the content-level idempotency above: the committedTxn
      // set dedups replays where the UUID has already been seen. Both lines
      // of defense must hold (invariant I2).
      byte[] file = catalog().build();
      UUID sharedId = UuidV7.newUuidV7();
      ProtoCodec.Transaction t = txn(sharedId, createNs(1, 0, "db", 1, -1));
      Replica<CatalogState> result = apply(file, t, t);
      assertThat(result.state().containsNamespace(Namespace.of("db"))).isTrue();
      assertThat(result.state().namespaceVersion(result.state().namespaceId(Namespace.of("db"))))
          .isEqualTo(1);
    }

    /** Applies {@code txns} to {@code file} and returns the result reserialized. */
    private byte[] replayAndSerialize(byte[] file, ProtoCodec.Transaction... txns) {
      return toFileBytes(apply(file, txns));
    }
  }

  // ============================================================
  // Conflict-matrix tests -- every pair (A, B) of actions where B captures
  // the pre-A version and is applied to post-A state. Each test asserts
  // whether verify returns true (compose) or false (conflict) and,
  // on conflict, which field drives the rejection.
  // The results are tabulated in docs/catalog_errata.md.
  // ============================================================

  @Nested
  class ConflictTests {

    /** Catalog with two namespaces and two tables under ns1 for conflict scenarios. */
    private byte[] baseFile() {
      return catalog()
          .ns(1, 0, "db", 1)
          .ns(2, 0, "other", 1)
          .tbl(1, 1, "t1", 1, "s3://t1/v1")
          .tbl(2, 1, "t2", 1, "s3://t2/v1")
          .prop(1, "owner", "alice")
          .build();
    }

    // --- CreateNamespace vs X ---

    @Test
    void createNsSiblingVsCreateNs_conflict_parentVersion() {
      // Two concurrent creates under the same parent. Parent is the root here
      // (id=0), which is versioned like any other parent: both capture v=0;
      // first to commit wins, second must be rejected on replay.
      byte[] file = baseFile();
      ProtoCodec.Transaction t1 = txn(createNs(3, 0, "a", 1, 0));
      ProtoCodec.Transaction t2 = txn(createNs(4, 0, "b", 1, 0));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.state().containsNamespace(Namespace.of("a"))).isTrue();
      assertThat(r.state().containsNamespace(Namespace.of("b"))).isFalse();
      assertThat(r.committed().contains(t2.id())).isFalse();
    }

    @Test
    void createNsChildVsCreateNsSibling_conflict_parentVersion() {
      // Two concurrent creates under NON-root parent "db". Both capture v=1;
      // first to commit wins, second must be rejected on replay.
      byte[] file = baseFile();
      ProtoCodec.Transaction t1 = txn(createNs(3, 1, "child1", 1, 1));
      ProtoCodec.Transaction t2 = txn(createNs(4, 1, "child2", 1, 1));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.state().containsNamespace(Namespace.of("db", "child1"))).isTrue();
      assertThat(r.state().containsNamespace(Namespace.of("db", "child2"))).isFalse();
      assertThat(r.committed().contains(t2.id())).isFalse();
    }

    @Test
    void createNsChildVsSetNsProperty_conflict_nsVersion() {
      byte[] file = baseFile();
      ProtoCodec.Transaction t1 = txn(createNs(3, 1, "child", 1, 1));
      ProtoCodec.Transaction t2 = txn(setNsProp(1, 1, "env", "prod"));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.state().containsNamespace(Namespace.of("db", "child"))).isTrue();
      assertThat(r.state().namespaceProperties(Namespace.of("db")))
          .doesNotContainKey("env");
      assertThat(r.committed().contains(t2.id())).isFalse();
    }

    @Test
    void createNsChildVsDropNs_conflict_nsVersion() {
      byte[] file = baseFile();
      ProtoCodec.Transaction t1 = txn(createNs(3, 1, "child", 1, 1));
      ProtoCodec.Transaction t2 = txn(dropNs(1, 1));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.state().containsNamespace(Namespace.of("db"))).isTrue();
      assertThat(r.committed().contains(t2.id())).isFalse();
    }

    @Test
    void createNsChildVsCreateTbl_conflict_nsVersion() {
      byte[] file = baseFile();
      ProtoCodec.Transaction t1 = txn(createNs(3, 1, "child", 1, 1));
      ProtoCodec.Transaction t2 = txn(createTbl(3, 1, "t3", 1, 1, "s3://t3"));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("db"), "t3"))).isNull();
      assertThat(r.committed().contains(t2.id())).isFalse();
    }

    // --- DropNamespace vs X ---

    @Test
    void dropNsVsSetNsProperty_conflict_nsVersion() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      ProtoCodec.Transaction t1 = txn(dropNs(1, 1));
      ProtoCodec.Transaction t2 = txn(setNsProp(1, 1, "k", "v"));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.state().containsNamespace(Namespace.of("db"))).isFalse();
      assertThat(r.committed().contains(t2.id())).isFalse();
    }

    @Test
    void dropNsVsCreateTbl_conflict_nsVersion() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      ProtoCodec.Transaction t1 = txn(dropNs(1, 1));
      ProtoCodec.Transaction t2 = txn(createTbl(1, 1, "t1", 1, 1, "s3://t1"));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("db"), "t1"))).isNull();
      assertThat(r.committed().contains(t2.id())).isFalse();
    }

    // --- SetNamespaceProperty vs X ---

    @Test
    void setNsPropertyVsSetNsProperty_conflict_nsVersion() {
      byte[] file = baseFile();
      ProtoCodec.Transaction t1 = txn(setNsProp(1, 1, "owner", "bob"));
      ProtoCodec.Transaction t2 = txn(setNsProp(1, 1, "env", "prod"));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.state().namespaceProperties(Namespace.of("db")))
          .containsEntry("owner", "bob")
          .doesNotContainKey("env");
      assertThat(r.committed().contains(t2.id())).isFalse();
    }

    @Test
    void setNsPropertyVsRemoveNsProperty_conflict_nsVersion() {
      byte[] file = baseFile();
      ProtoCodec.Transaction t1 = txn(setNsProp(1, 1, "env", "prod"));
      ProtoCodec.Transaction t2 = txn(removeNsProp(1, 1, "owner"));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.state().namespaceProperties(Namespace.of("db")))
          .containsEntry("owner", "alice")
          .containsEntry("env", "prod");
      assertThat(r.committed().contains(t2.id())).isFalse();
    }

    @Test
    void setNsPropertyVsCreateTbl_conflict_nsVersion() {
      byte[] file = baseFile();
      ProtoCodec.Transaction t1 = txn(setNsProp(1, 1, "env", "prod"));
      ProtoCodec.Transaction t2 = txn(createTbl(3, 1, "t3", 1, 1, "s3://t3"));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("db"), "t3"))).isNull();
      assertThat(r.committed().contains(t2.id())).isFalse();
    }

    @Test
    void setNsPropertyVsDropTbl_conflict_nsVersion() {
      byte[] file = baseFile();
      ProtoCodec.Transaction t1 = txn(setNsProp(1, 1, "env", "prod"));
      ProtoCodec.Transaction t2 = txn(dropTbl(1, 1));
      Replica<CatalogState> r = apply(file, t1, t2);
      // Note: DropTable.verify checks TABLE version (not ns version), so the
      // drop succeeds. The ns-version bump from setNsProp doesn't directly
      // protect DropTable; that's by design — concurrent table-ops on
      // different tables in the same ns compose.
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("db"), "t1")))
          .as("DropTable is independent of ns version; it succeeds")
          .isNull();
      assertThat(r.committed().contains(t2.id())).isTrue();
    }

    // --- CreateTable vs CreateTable / UpdateTable / DropTable ---

    @Test
    void createTblSameNsVsCreateTbl_conflict_nsVersion() {
      // Two creates under the same ns from independent transactions — the
      // second captured the pre-first nsVersion and must be rejected.
      byte[] file = baseFile();
      ProtoCodec.Transaction t1 = txn(createTbl(3, 1, "tA", 1, 1, "s3://tA"));
      ProtoCodec.Transaction t2 = txn(createTbl(4, 1, "tB", 1, 1, "s3://tB"));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("db"), "tA"))).isNotNull();
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("db"), "tB"))).isNull();
      assertThat(r.committed().contains(t2.id())).isFalse();
    }

    @Test
    void createTblInDifferentNs_compose() {
      // Two creates under DIFFERENT namespaces — independent, both succeed.
      byte[] file = baseFile();
      ProtoCodec.Transaction t1 = txn(createTbl(3, 1, "tA", 1, 1, "s3://tA"));
      ProtoCodec.Transaction t2 = txn(createTbl(4, 2, "tB", 1, 1, "s3://tB"));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("db"), "tA"))).isNotNull();
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("other"), "tB"))).isNotNull();
    }

    @Test
    void updateTblVsUpdateTbl_conflict_tblVersion() {
      byte[] file = baseFile();
      ProtoCodec.Transaction t1 = txn(updateTbl(1, 1, "s3://t1/v2"));
      ProtoCodec.Transaction t2 = txn(updateTbl(1, 1, "s3://t1/v3"));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.state().location(TableIdentifier.of(Namespace.of("db"), "t1")))
          .isEqualTo("s3://t1/v2");
      assertThat(r.committed().contains(t2.id())).isFalse();
    }

    @Test
    void updateTblVsDropTbl_conflict_tblVersion() {
      byte[] file = baseFile();
      ProtoCodec.Transaction t1 = txn(updateTbl(1, 1, "s3://t1/v2"));
      ProtoCodec.Transaction t2 = txn(dropTbl(1, 1));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("db"), "t1"))).isNotNull();
      assertThat(r.committed().contains(t2.id())).isFalse();
    }

    @Test
    void updateTblDifferentTables_compose() {
      // Independent tables — no conflict.
      byte[] file = baseFile();
      ProtoCodec.Transaction t1 = txn(updateTbl(1, 1, "s3://t1/v2"));
      ProtoCodec.Transaction t2 = txn(updateTbl(2, 1, "s3://t2/v2"));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.state().location(TableIdentifier.of(Namespace.of("db"), "t1")))
          .isEqualTo("s3://t1/v2");
      assertThat(r.state().location(TableIdentifier.of(Namespace.of("db"), "t2")))
          .isEqualTo("s3://t2/v2");
    }

    @Test
    void readTblVsUpdateTbl_conflict_tblVersion() {
      // ReadTable fails if the table has been updated since the read.
      byte[] file = baseFile();
      ProtoCodec.Transaction t1 = txn(updateTbl(1, 1, "s3://t1/v2"));
      ProtoCodec.Transaction t2 = txn(readTbl(1, 1));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.committed().contains(t2.id())).isFalse();
    }

    @Test
    void readTblVsIndependentUpdate_compose() {
      // ReadTable on t1, update on t2 — no interaction.
      byte[] file = baseFile();
      ProtoCodec.Transaction t1 = txn(updateTbl(2, 1, "s3://t2/v2"));
      ProtoCodec.Transaction t2 = txn(readTbl(1, 1));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.committed().contains(t2.id())).isTrue();
    }

    // --- RenameTable vs X ---

    @Test
    void renameTblVsRenameTbl_sameId_conflict_tblVersion() {
      // Two concurrent renames of the same table — second's tbl_v=1 fails.
      byte[] file = baseFile();
      ProtoCodec.Transaction t1 = txn(renameTbl(1, 1, 1, 1, "renamed_a"));
      ProtoCodec.Transaction t2 = txn(renameTbl(1, 1, 1, 1, "renamed_b"));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("db"), "renamed_a"))).isEqualTo(1);
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("db"), "renamed_b"))).isNull();
      assertThat(r.committed().contains(t2.id())).isFalse();
    }

    @Test
    void renameTblVsDropTbl_sameId_conflict_tblVersion() {
      byte[] file = baseFile();
      ProtoCodec.Transaction t1 = txn(dropTbl(1, 1));
      ProtoCodec.Transaction t2 = txn(renameTbl(1, 1, 1, 1, "renamed"));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("db"), "t1"))).isNull();
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("db"), "renamed"))).isNull();
      assertThat(r.committed().contains(t2.id())).isFalse();
    }

    @Test
    void renameTblVsUpdateTbl_sameId_conflict_tblVersion() {
      byte[] file = baseFile();
      ProtoCodec.Transaction t1 = txn(updateTbl(1, 1, "s3://t1/v2"));
      ProtoCodec.Transaction t2 = txn(renameTbl(1, 1, 1, 1, "renamed"));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.state().location(TableIdentifier.of(Namespace.of("db"), "t1"))).isEqualTo("s3://t1/v2");
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("db"), "renamed"))).isNull();
      assertThat(r.committed().contains(t2.id())).isFalse();
    }

    @Test
    void renameTblVsReadTbl_sameId_conflict_tblVersion() {
      byte[] file = baseFile();
      ProtoCodec.Transaction t1 = txn(renameTbl(1, 1, 1, 1, "renamed"));
      ProtoCodec.Transaction t2 = txn(readTbl(1, 1));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.committed().contains(t1.id())).isTrue();
      assertThat(r.committed().contains(t2.id())).isFalse();
    }

    @Test
    void renameTblDifferentTables_compose() {
      byte[] file = baseFile();
      ProtoCodec.Transaction t1 = txn(renameTbl(1, 1, 1, 1, "renamed1"));
      ProtoCodec.Transaction t2 = txn(renameTbl(2, 1, 1, 2, "renamed2"));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("db"), "renamed1"))).isEqualTo(1);
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("db"), "renamed2"))).isEqualTo(2);
    }

    @Test
    void renameTblVsCreateTblInDestNs_conflict_nsVersion() {
      // Concurrent CreateTable in the destination ns bumps that ns's version;
      // the rename's captured ns_v fails.
      byte[] file = baseFile();
      // ns1 ("db") starts at v=1. CreateTable bumps it to v=2. Rename
      // captured newNamespaceVersion=1, so verify fails.
      ProtoCodec.Transaction t1 = txn(createTbl(3, 1, "newcomer", 1, 1, "s3://newcomer/v1"));
      ProtoCodec.Transaction t2 = txn(renameTbl(1, 1, 1, 1, "renamed"));
      Replica<CatalogState> r = apply(file, t1, t2);
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("db"), "newcomer"))).isEqualTo(3);
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("db"), "renamed"))).isNull();
      assertThat(r.committed().contains(t2.id())).isFalse();
    }

    @Test
    void renameTblBumpsSourceAndDestNs() {
      // After a cross-namespace rename, both source and destination ns
      // versions bump (children-set rule). A concurrent set-property on
      // either ns at the captured version is rejected.
      byte[] file = baseFile();
      ProtoCodec.Transaction t1 = txn(renameTbl(1, 1, 2, 1, "t1"));
      ProtoCodec.Transaction t2 = txn(setNsProp(1, 1, "owner", "carol"));  // src ns
      ProtoCodec.Transaction t3 = txn(setNsProp(2, 1, "team", "data"));    // dest ns
      Replica<CatalogState> r = apply(file, t1, t2, t3);
      assertThat(r.state().tableId(TableIdentifier.of(Namespace.of("other"), "t1"))).isEqualTo(1);
      assertThat(r.committed().contains(t1.id())).isTrue();
      assertThat(r.committed().contains(t2.id())).isFalse();
      assertThat(r.committed().contains(t3.id())).isFalse();
    }
  }

  // ============================================================
  // Randomized tests (ported from TestLogCatalogFormat)
  // ============================================================

  @Nested
  class RandomizedTests {

    /**
     * Generates a random catalog (a state, with its log's id and committed set) with reproducible
     * state from a seed. Mirrors generateRandomLogCatalogFile from TestLogCatalogFormat.
     */
    Replica<CatalogState> generateRandom(long seed) {
      Random rand = new Random(seed);
      CatalogState.Builder builder = CatalogState.builder();
      UUID logId = UuidV7.newUuidV7(rand.nextLong() & 0xFFFFFFFFFFFFL, rand);

      int nextNsid = rand.nextInt(20) + 10;
      int nextTblid = rand.nextInt(100) + 10;
      builder.setNextNamespaceId(nextNsid);
      builder.setNextTableId(nextTblid);

      Map<Integer, String> nsNames = new HashMap<>();
      nsNames.put(0, ""); // root

      builder.addNamespace(CatalogState.ROOT_ID, 0, "", rand.nextInt(3) + 1);

      for (int nsid = 1; nsid < nextNsid; nsid += rand.nextInt(5) + 1) {
        int parentId = rand.nextInt(nsid);
        if (!nsNames.containsKey(parentId)) {
          parentId = 0;
        }
        String name = "ns" + nsid;
        nsNames.put(nsid, name);
        int version = rand.nextInt(5) + 1;
        builder.addNamespace(nsid, parentId, name, version);

        for (int j = 0; j < rand.nextInt(3); j++) {
          builder.setNamespaceProperty(nsid, "key" + j, "value" + j);
        }
      }

      List<Integer> nsIds = new ArrayList<>(nsNames.keySet());
      nsIds.remove(Integer.valueOf(0));
      if (!nsIds.isEmpty()) {
        for (int tblid = 1; tblid < nextTblid; tblid += rand.nextInt(10) + 1) {
          int nsid = nsIds.get(rand.nextInt(nsIds.size()));
          if (rand.nextInt(4) == 0) {
            // ~25% chance of inline table
            byte[] meta = ("{\"tbl\":" + tblid + "}").getBytes(java.nio.charset.StandardCharsets.UTF_8);
            String prefix = "s3://t" + tblid + "/metadata/snap-";
            builder.addInlineTable(tblid, nsid, "tbl" + tblid, rand.nextInt(5) + 1, meta, prefix);
          } else {
            builder.addTable(tblid, nsid, "tbl" + tblid, rand.nextInt(5) + 1, "s3://t" + tblid);
          }
        }
      }

      Set<UUID> txns = IntStream.range(0, rand.nextInt(20) + 5)
          .mapToObj(i -> UuidV7.newUuidV7(rand.nextLong() & 0xFFFFFFFFFFFFL, rand))
          .collect(Collectors.toSet());
      CommittedSet.Builder committed = CommittedSet.builder();
      for (UUID txnId : txns) {
        committed.add(txnId);
      }

      return new Replica<>(builder.build(), committed.build(), null, 0, logId);
    }

    /**
     * Asserts that two catalogs have identical observable state, log id and committed set.
     * Does not compare nextNamespaceId / nextTableId, which may differ between a replay-built
     * catalog and a fresh one.
     */
    void assertCatalogEquals(Replica<CatalogState> expectedRead, Replica<CatalogState> actualRead) {
      assertThat(actualRead.logId()).isEqualTo(expectedRead.logId());
      assertThat(actualRead.committed().ids())
          .containsExactlyInAnyOrderElementsOf(expectedRead.committed().ids());
      CatalogState expected = expectedRead.state();
      CatalogState actual = actualRead.state();
      assertThat(actual.namespaces()).containsExactlyInAnyOrderElementsOf(expected.namespaces());
      assertThat(actual.tables()).containsExactlyInAnyOrderElementsOf(expected.tables());

      for (Namespace ns : expected.namespaces()) {
        assertThat(actual.namespaceProperties(ns))
            .as("properties for namespace %s", ns)
            .isEqualTo(expected.namespaceProperties(ns));
        Integer eid = expected.namespaceId(ns);
        Integer aid = actual.namespaceId(ns);
        assertThat(aid).as("namespace ID for %s", ns).isEqualTo(eid);
        assertThat(actual.namespaceVersion(aid))
            .as("namespace version for %s", ns)
            .isEqualTo(expected.namespaceVersion(eid));
      }

      for (TableIdentifier tbl : expected.tables()) {
        assertThat(actual.location(tbl))
            .as("location for table %s", tbl)
            .isEqualTo(expected.location(tbl));
        Integer eid = expected.tableId(tbl);
        Integer aid = actual.tableId(tbl);
        assertThat(aid).as("table ID for %s", tbl).isEqualTo(eid);
        assertThat(actual.tableVersion(aid))
            .as("table version for %s", tbl)
            .isEqualTo(expected.tableVersion(eid));
        assertThat(actual.isInlineTable(aid))
            .as("inline status for %s", tbl)
            .isEqualTo(expected.isInlineTable(eid));
        if (expected.isInlineTable(eid)) {
          assertThat(actual.inlineMetadata(aid))
              .as("inline metadata for %s", tbl)
              .isEqualTo(expected.inlineMetadata(eid));
          assertThat(actual.manifestListPrefix(aid))
              .as("manifest prefix for %s", tbl)
              .isEqualTo(expected.manifestListPrefix(eid));
        }
      }
    }

    /**
     * Random checkpoint roundtrip: generate random state, serialize through PB file bytes,
     * deserialize, verify all fields match. Same seed produces identical catalogs.
     * Ported from LCF testEquivalenceSerde.
     */
    @RepeatedTest(5)
    void checkpointRoundtripFromRandomState() throws IOException {
      long seed = System.nanoTime();
      Replica<CatalogState> original = generateRandom(seed);
      Replica<CatalogState> sameSeed = generateRandom(seed);
      assertCatalogEquals(original, sameSeed);

      byte[] fileBytes = toFileBytes(original);
      assertThat(fileBytes.length).isGreaterThan(AtomicFileLog.HEADER_SIZE);

      Replica<CatalogState> roundtripped = CatalogTestSupport.replay(fileBytes);
      assertCatalogEquals(original, roundtripped);
      // Pure checkpoint roundtrip (no transactions) should also preserve counters
      assertThat(roundtripped.state().nextNamespaceId())
          .isEqualTo(original.state().nextNamespaceId());
      assertThat(roundtripped.state().nextTableId()).isEqualTo(original.state().nextTableId());
    }

    /**
     * Build transactions through the Mut API against random base state, serialize, append,
     * and verify correct application and conflict detection.
     * Ported from LCF testApplyTransaction.
     */
    @RepeatedTest(5)
    void applyMutTransactionOnRandomState() throws IOException {
      long seed = System.nanoTime();
      Replica<CatalogState> orig = generateRandom(seed);
      byte[] origBytes = toFileBytes(orig);

      Namespace dingos = Namespace.of("dingos");
      Namespace dingos_yaks = Namespace.of("dingos", "yaks");
      TableIdentifier tblY = TableIdentifier.of(dingos_yaks, "tblY");
      TableIdentifier tblD = TableIdentifier.of(dingos, "tblD");

      // txnA: create dingos -> dingos.yaks -> tblY
      CatalogMutation mutA = new CatalogMutation(orig);
      mutA.createNamespace(dingos);
      mutA.createNamespace(dingos_yaks);
      mutA.createTable(tblY, "yak://chinchilla/tblY");
      ProtoCodec.Transaction txnA = ProtoCodec.Transaction.from(mutA);

      // Append and read
      Replica<CatalogState> afterA = apply(origBytes, txnA);
      assertThat(afterA.state().containsNamespace(dingos)).isTrue();
      assertThat(afterA.state().containsNamespace(dingos_yaks)).isTrue();
      assertThat(afterA.state().location(tblY)).isEqualTo("yak://chinchilla/tblY");
      assertThat(afterA.committed().contains(txnA.id())).isTrue();

      // Second round: re-checkpoint and re-read (as a concurrent writer would)
      byte[] afterABytes = toFileBytes(afterA);
      Replica<CatalogState> afterAFresh = CatalogTestSupport.replay(afterABytes);

      // txnB: update tblY
      CatalogMutation mutB = new CatalogMutation(afterAFresh);
      mutB.updateTable(tblY, "yak://chinchilla/tblY2");
      ProtoCodec.Transaction txnB = ProtoCodec.Transaction.from(mutB);

      // txnC: create tblD (independent, built from same snapshot)
      CatalogMutation mutC = new CatalogMutation(afterAFresh);
      mutC.createTable(tblD, "yak://chinchilla/tblD");
      ProtoCodec.Transaction txnC = ProtoCodec.Transaction.from(mutC);

      // txnD: also tries to update tblY from same snapshot -> should fail
      // because txnB already updated it
      CatalogMutation mutD = new CatalogMutation(afterAFresh);
      mutD.updateTable(tblY, "yak://chinchilla/tblY3");
      ProtoCodec.Transaction txnD = ProtoCodec.Transaction.from(mutD);

      Replica<CatalogState> afterAll = apply(afterABytes, txnB, txnC, txnD);

      // txnB succeeded: tblY updated
      assertThat(afterAll.state().location(tblY)).isEqualTo("yak://chinchilla/tblY2");
      assertThat(afterAll.committed().contains(txnB.id())).isTrue();

      // txnC succeeded: tblD created
      assertThat(afterAll.state().location(tblD)).isEqualTo("yak://chinchilla/tblD");
      assertThat(afterAll.committed().contains(txnC.id())).isTrue();

      // txnD failed: stale table version
      assertThat(afterAll.committed().contains(txnD.id())).isFalse();
    }

    /**
     * Full roundtrip: random state → checkpoint bytes → append Mut-built transaction →
     * re-checkpoint the result → read back → verify state matches.
     */
    @RepeatedTest(5)
    void mutTransactionSurvivesReCheckpoint() throws IOException {
      Replica<CatalogState> orig = generateRandom(System.nanoTime());
      byte[] origBytes = toFileBytes(orig);

      Namespace ns = Namespace.of("roundtrip_ns");
      TableIdentifier tbl = TableIdentifier.of(ns, "roundtrip_tbl");

      // Build and apply a transaction via Mut
      CatalogMutation mut = new CatalogMutation(orig);
      mut.createNamespace(ns);
      mut.createTable(tbl, "s3://roundtrip/v1");
      ProtoCodec.Transaction txn = ProtoCodec.Transaction.from(mut);

      Replica<CatalogState> afterTxn = apply(origBytes, txn);

      // Re-checkpoint (as would happen on compaction)
      byte[] reCheckpointed = toFileBytes(afterTxn);
      Replica<CatalogState> reRead = CatalogTestSupport.replay(reCheckpointed);

      // Verify all original state + new additions survived
      assertCatalogEquals(afterTxn, reRead);
      assertThat(reRead.state().containsNamespace(ns)).isTrue();
      assertThat(reRead.state().location(tbl)).isEqualTo("s3://roundtrip/v1");
    }
  }
}
