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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/**
 * Systematic tests for all ProtoCatalogFormat action types, covering both positive
 * (action applies, state updated) and negative (version mismatch, state unchanged) cases.
 *
 * <p>Uses {@link CatalogBuilder} to construct serialized catalog files and
 * {@link #apply(byte[], ProtoCodec.Transaction...)} to append transactions and read
 * the result -- exercising the full checkpoint-parse / transaction-replay path.
 */
public class TestProtoActions {

  private static final InputFile LOCATION = new TestInputFile("test://catalog");

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
    private final ProtoCatalogFile.Builder inner;
    private int maxNsId = 0;
    private int maxTblId = 0;

    CatalogBuilder() {
      this.inner = ProtoCatalogFile.builder(LOCATION);
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
      inner.addInlineTable(id, nsId, name, version, metadata, manifestPrefix);
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
      inner.addCommittedTransaction(txnId);
      return this;
    }

    /** Builds the catalog and serializes it to file bytes (header + checkpoint). */
    byte[] build() {
      inner.setNextNamespaceId(maxNsId + 1);
      inner.setNextTableId(maxTblId + 1);
      ProtoCatalogFile catalog = inner.build();
      return toFileBytes(catalog);
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
    return new ProtoCodec.Transaction(UUID.randomUUID(), false, List.of(actions));
  }

  static ProtoCodec.Transaction txn(UUID id, ProtoCodec.Action... actions) {
    return new ProtoCodec.Transaction(id, false, List.of(actions));
  }

  static ProtoCodec.Transaction sealedTxn(ProtoCodec.Action... actions) {
    return new ProtoCodec.Transaction(UUID.randomUUID(), true, List.of(actions));
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

  // ============================================================
  // File construction / replay helpers
  // ============================================================

  /** Serializes a ProtoCatalogFile to full file bytes (header + checkpoint). */
  static byte[] toFileBytes(ProtoCatalogFile catalog) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(ProtoCatalogFormat.MAGIC);
      writeInt(out, ProtoCatalogFormat.FORMAT_VERSION);

      // Compute correct next IDs from actual entries -- readInternal doesn't
      // update the counters during transaction replay, so after applying
      // transactions the stored nextId values may be stale.
      int nextNsId = catalog.namespaceById().keySet().stream()
          .mapToInt(i -> i).max().orElse(0) + 1;
      int nextTblId = catalog.tableById().keySet().stream()
          .mapToInt(i -> i).max().orElse(0) + 1;
      nextNsId = Math.max(nextNsId, catalog.nextNamespaceId());
      nextTblId = Math.max(nextTblId, catalog.nextTableId());

      ProtoCatalogFormat.ProtoIdManager idManager = new ProtoCatalogFormat.ProtoIdManager();
      idManager.setGlobals(nextNsId, nextTblId);
      ProtoCatalogFormat.Mut mut = new ProtoCatalogFormat.Mut(catalog);
      byte[] chk = ProtoCodec.encodeCheckpoint(catalog, mut, idManager);
      writeVarint(out, chk.length);
      out.write(chk);
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Serializes a transaction to a varint-length-prefixed record. */
  static byte[] toRecord(ProtoCodec.Transaction txn) {
    try {
      byte[] txnBytes = ProtoCodec.encodeTransaction(txn);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      writeVarint(out, txnBytes.length);
      out.write(txnBytes);
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Appends transactions to serialized file bytes and reads the result.
   * This exercises the full read path: header parse, checkpoint decode, log replay.
   */
  static ProtoCatalogFile apply(byte[] fileBytes, ProtoCodec.Transaction... txns) {
    try {
      byte[] combined = fileBytes;
      for (ProtoCodec.Transaction txn : txns) {
        combined = appendBytes(combined, toRecord(txn));
      }
      return ProtoCatalogFormat.readInternal(
          LOCATION, new ByteArrayInputStream(combined), combined.length);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static byte[] appendBytes(byte[] a, byte[] b) {
    byte[] result = Arrays.copyOf(a, a.length + b.length);
    System.arraycopy(b, 0, result, a.length, b.length);
    return result;
  }

  private static void writeInt(ByteArrayOutputStream out, int value) {
    out.write((value >> 24) & 0xFF);
    out.write((value >> 16) & 0xFF);
    out.write((value >> 8) & 0xFF);
    out.write(value & 0xFF);
  }

  private static void writeVarint(ByteArrayOutputStream out, int value) {
    while ((value & ~0x7F) != 0) {
      out.write((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    out.write(value);
  }

  // ============================================================
  // CreateNamespace
  // ============================================================

  @Nested
  class CreateNamespaceTests {

    @Test
    void appliesUnderRoot() {
      byte[] file = catalog().build();
      ProtoCatalogFile result = apply(file, txn(createNs(1, 0, "db", 1, -1)));

      assertThat(result.containsNamespace(Namespace.of("db"))).isTrue();
      assertThat(result.namespaceVersion(result.namespaceId(Namespace.of("db")))).isEqualTo(1);
    }

    @Test
    void appliesUnderExistingParent() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      ProtoCatalogFile result = apply(file, txn(createNs(2, 1, "schema", 1, 1)));

      assertThat(result.containsNamespace(Namespace.of("db", "schema"))).isTrue();
    }

    @Test
    void rejectsOnParentVersionMismatch() {
      byte[] file = catalog().ns(1, 0, "db", 3).build();
      // parent_version=1 doesn't match db's actual version=3
      ProtoCodec.Transaction t = txn(createNs(2, 1, "schema", 1, 1));
      ProtoCatalogFile result = apply(file, t);

      assertThat(result.containsNamespace(Namespace.of("db", "schema"))).isFalse();
      assertThat(result.containsTransaction(t.id())).isFalse();
    }

    @Test
    void appliesWithLateBoundParentVersion() {
      byte[] file = catalog().ns(1, 0, "db", 5).build();
      // parent_version=-1 means "don't check" (late-bound)
      ProtoCatalogFile result = apply(file, txn(createNs(2, 1, "schema", 1, -1)));

      assertThat(result.containsNamespace(Namespace.of("db", "schema"))).isTrue();
    }

    @Test
    void multipleInSameTransaction() {
      byte[] file = catalog().build();
      ProtoCatalogFile result = apply(file, txn(
          createNs(1, 0, "a", 1, -1),
          createNs(2, 0, "b", 1, -1),
          createNs(3, 1, "child", 1, -1)));

      assertThat(result.containsNamespace(Namespace.of("a"))).isTrue();
      assertThat(result.containsNamespace(Namespace.of("b"))).isTrue();
      assertThat(result.containsNamespace(Namespace.of("a", "child"))).isTrue();
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
      ProtoCatalogFile result = apply(file, t);

      assertThat(result.containsNamespace(Namespace.of("db"))).isFalse();
      assertThat(result.containsTransaction(t.id())).isTrue();
    }

    @Test
    void rejectsOnVersionMismatch() {
      byte[] file = catalog().ns(1, 0, "db", 2).build();
      ProtoCodec.Transaction t = txn(dropNs(1, 1));
      ProtoCatalogFile result = apply(file, t);

      assertThat(result.containsNamespace(Namespace.of("db"))).isTrue();
      assertThat(result.containsTransaction(t.id())).isFalse();
    }

    @Test
    void removesPropertiesWhenDropped() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .prop(1, "owner", "alice")
          .prop(1, "comment", "test")
          .build();
      ProtoCatalogFile result = apply(file, txn(dropNs(1, 1)));

      assertThat(result.containsNamespace(Namespace.of("db"))).isFalse();
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
      ProtoCatalogFile result = apply(file, txn(setNsProp(1, 1, "owner", "bob")));

      assertThat(result.namespaceProperties(Namespace.of("db")))
          .containsEntry("owner", "bob");
    }

    @Test
    void rejectsOnVersionMismatch() {
      byte[] file = catalog()
          .ns(1, 0, "db", 3)
          .prop(1, "owner", "alice")
          .build();
      ProtoCodec.Transaction t = txn(setNsProp(1, 1, "owner", "bob"));
      ProtoCatalogFile result = apply(file, t);

      assertThat(result.namespaceProperties(Namespace.of("db")))
          .containsEntry("owner", "alice");
      assertThat(result.containsTransaction(t.id())).isFalse();
    }

    @Test
    void appliesWithLateBoundVersion() {
      byte[] file = catalog().ns(1, 0, "db", 5).build();
      // version=-1 means namespace was created in same txn (skip version check)
      ProtoCatalogFile result = apply(file, txn(setNsProp(1, -1, "k", "v")));

      assertThat(result.namespaceProperties(Namespace.of("db")))
          .containsEntry("k", "v");
    }

    @Test
    void overwritesExistingProperty() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .prop(1, "owner", "alice")
          .build();
      ProtoCatalogFile result = apply(file, txn(setNsProp(1, 1, "owner", "bob")));

      assertThat(result.namespaceProperties(Namespace.of("db")))
          .containsEntry("owner", "bob");
    }

    @Test
    void addsMultiplePropertiesInSameTransaction() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      ProtoCatalogFile result = apply(file, txn(
          setNsProp(1, 1, "owner", "alice"),
          setNsProp(1, 1, "env", "prod")));

      Map<String, String> props = result.namespaceProperties(Namespace.of("db"));
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
      ProtoCatalogFile result = apply(file, txn(removeNsProp(1, 1, "owner")));

      Map<String, String> props = result.namespaceProperties(Namespace.of("db"));
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
      ProtoCatalogFile result = apply(file, t);

      assertThat(result.namespaceProperties(Namespace.of("db")))
          .containsEntry("owner", "alice");
      assertThat(result.containsTransaction(t.id())).isFalse();
    }

    @Test
    void appliesWithLateBoundVersion() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .prop(1, "owner", "alice")
          .build();
      ProtoCatalogFile result = apply(file, txn(removeNsProp(1, -1, "owner")));

      assertThat(result.namespaceProperties(Namespace.of("db")))
          .doesNotContainKey("owner");
    }

    @Test
    void noopWhenPropertyDoesNotExist() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      ProtoCodec.Transaction t = txn(removeNsProp(1, 1, "nonexistent"));
      ProtoCatalogFile result = apply(file, t);

      assertThat(result.containsTransaction(t.id())).isTrue();
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
      ProtoCatalogFile result = apply(file,
          txn(createTbl(1, 1, "users", 1, 1, "s3://bucket/users/v1")));

      TableIdentifier users = TableIdentifier.of(Namespace.of("db"), "users");
      assertThat(result.location(users)).isEqualTo("s3://bucket/users/v1");
    }

    @Test
    void rejectsOnNamespaceVersionMismatch() {
      byte[] file = catalog().ns(1, 0, "db", 3).build();
      // ns_version=1 doesn't match actual version=3
      ProtoCodec.Transaction t = txn(createTbl(1, 1, "users", 1, 1, "s3://bucket/users/v1"));
      ProtoCatalogFile result = apply(file, t);

      TableIdentifier users = TableIdentifier.of(Namespace.of("db"), "users");
      assertThat(result.location(users)).isNull();
      assertThat(result.containsTransaction(t.id())).isFalse();
    }

    @Test
    void appliesWithLateBoundNamespaceVersion() {
      byte[] file = catalog().ns(1, 0, "db", 5).build();
      // ns_version=-1 skips check (namespace created in same txn)
      ProtoCatalogFile result = apply(file,
          txn(createTbl(1, 1, "users", 1, -1, "s3://bucket/users/v1")));

      TableIdentifier users = TableIdentifier.of(Namespace.of("db"), "users");
      assertThat(result.location(users)).isEqualTo("s3://bucket/users/v1");
    }

    @Test
    void createNamespaceAndTableInSameTransaction() {
      byte[] file = catalog().build();
      ProtoCatalogFile result = apply(file, txn(
          createNs(1, 0, "db", 1, -1),
          createTbl(1, 1, "users", 1, -1, "s3://bucket/users/v1")));

      assertThat(result.containsNamespace(Namespace.of("db"))).isTrue();
      TableIdentifier users = TableIdentifier.of(Namespace.of("db"), "users");
      assertThat(result.location(users)).isEqualTo("s3://bucket/users/v1");
    }

    @Test
    void multipleTablesInSameNamespace() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      ProtoCatalogFile result = apply(file, txn(
          createTbl(1, 1, "users", 1, 1, "s3://bucket/users/v1"),
          createTbl(2, 1, "orders", 1, 1, "s3://bucket/orders/v1")));

      Namespace db = Namespace.of("db");
      assertThat(result.location(TableIdentifier.of(db, "users")))
          .isEqualTo("s3://bucket/users/v1");
      assertThat(result.location(TableIdentifier.of(db, "orders")))
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
      ProtoCatalogFile result = apply(file, t);

      TableIdentifier users = TableIdentifier.of(Namespace.of("db"), "users");
      assertThat(result.location(users)).isNull();
      assertThat(result.tables()).doesNotContain(users);
      assertThat(result.containsTransaction(t.id())).isTrue();
    }

    @Test
    void rejectsOnVersionMismatch() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "users", 2, "s3://bucket/users/v2")
          .build();
      ProtoCodec.Transaction t = txn(dropTbl(1, 1));
      ProtoCatalogFile result = apply(file, t);

      TableIdentifier users = TableIdentifier.of(Namespace.of("db"), "users");
      assertThat(result.location(users)).isEqualTo("s3://bucket/users/v2");
      assertThat(result.containsTransaction(t.id())).isFalse();
    }

    @Test
    void preservesOtherTablesInSameNamespace() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "users", 1, "s3://bucket/users/v1")
          .tbl(2, 1, "orders", 1, "s3://bucket/orders/v1")
          .build();
      ProtoCatalogFile result = apply(file, txn(dropTbl(1, 1)));

      Namespace db = Namespace.of("db");
      assertThat(result.location(TableIdentifier.of(db, "users"))).isNull();
      assertThat(result.location(TableIdentifier.of(db, "orders")))
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
      ProtoCatalogFile result = apply(file,
          txn(updateTbl(1, 1, "s3://bucket/users/v2")));

      TableIdentifier users = TableIdentifier.of(Namespace.of("db"), "users");
      assertThat(result.location(users)).isEqualTo("s3://bucket/users/v2");
      // Version should be incremented
      assertThat(result.tableVersion(result.tableId(users))).isEqualTo(2);
    }

    @Test
    void rejectsOnVersionMismatch() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "users", 3, "s3://bucket/users/v3")
          .build();
      ProtoCodec.Transaction t = txn(updateTbl(1, 1, "s3://bucket/users/v4"));
      ProtoCatalogFile result = apply(file, t);

      TableIdentifier users = TableIdentifier.of(Namespace.of("db"), "users");
      assertThat(result.location(users)).isEqualTo("s3://bucket/users/v3");
      assertThat(result.containsTransaction(t.id())).isFalse();
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
      ProtoCatalogFile result = apply(file, t1, t2);

      TableIdentifier users = TableIdentifier.of(Namespace.of("db"), "users");
      assertThat(result.location(users)).isEqualTo("s3://v3");
      assertThat(result.tableVersion(result.tableId(users))).isEqualTo(3);
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
      ProtoCatalogFile result = apply(file, t1, t2);

      TableIdentifier users = TableIdentifier.of(Namespace.of("db"), "users");
      assertThat(result.location(users)).isEqualTo("s3://writer-a");
      assertThat(result.containsTransaction(t1.id())).isTrue();
      assertThat(result.containsTransaction(t2.id())).isFalse();
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
      ProtoCatalogFile result = apply(file, t);

      assertThat(result.containsTransaction(t.id())).isTrue();
      assertThat(result.containsNamespace(Namespace.of("other"))).isTrue();
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
      ProtoCatalogFile result = apply(file, t);

      assertThat(result.containsTransaction(t.id())).isFalse();
      assertThat(result.containsNamespace(Namespace.of("other"))).isFalse();
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
      ProtoCatalogFile result = apply(file, t1, t2);

      assertThat(result.containsTransaction(t1.id())).isTrue();
      assertThat(result.containsTransaction(t2.id())).isFalse();
      assertThat(result.containsNamespace(Namespace.of("marker"))).isFalse();
    }

    @Test
    void doesNotMutateState() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "users", 1, "s3://v1")
          .build();
      ProtoCatalogFile result = apply(file, txn(readTbl(1, 1)));

      // Table version unchanged after ReadTable
      TableIdentifier users = TableIdentifier.of(Namespace.of("db"), "users");
      assertThat(result.tableVersion(result.tableId(users))).isEqualTo(1);
      assertThat(result.location(users)).isEqualTo("s3://v1");
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
      ProtoCatalogFile result = apply(file, t);

      // Neither action should have taken effect
      assertThat(result.containsNamespace(Namespace.of("newns"))).isFalse();
      assertThat(result.location(TableIdentifier.of(Namespace.of("db"), "users")))
          .isEqualTo("s3://v3");
      assertThat(result.containsTransaction(t.id())).isFalse();
    }

    @Test
    void independentTransactionsApplySequentially() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();

      ProtoCodec.Transaction t1 = txn(createNs(2, 0, "analytics", 1, -1));
      ProtoCodec.Transaction t2 = txn(
          createTbl(1, 1, "users", 1, 1, "s3://users/v1"));
      ProtoCodec.Transaction t3 = txn(
          createTbl(2, 2, "events", 1, -1, "s3://events/v1"));
      ProtoCatalogFile result = apply(file, t1, t2, t3);

      assertThat(result.containsNamespace(Namespace.of("analytics"))).isTrue();
      assertThat(result.location(TableIdentifier.of(Namespace.of("db"), "users")))
          .isEqualTo("s3://users/v1");
      assertThat(result.location(TableIdentifier.of(Namespace.of("analytics"), "events")))
          .isEqualTo("s3://events/v1");
      assertThat(result.containsTransaction(t1.id())).isTrue();
      assertThat(result.containsTransaction(t2.id())).isTrue();
      assertThat(result.containsTransaction(t3.id())).isTrue();
    }

    @Test
    void duplicateTransactionIdIsIdempotent() {
      byte[] file = catalog().build();
      UUID sharedId = UUID.randomUUID();

      ProtoCodec.Transaction t = txn(sharedId, createNs(1, 0, "db", 1, -1));
      // Same ID appended twice
      ProtoCatalogFile result = apply(file, t, t);

      assertThat(result.containsNamespace(Namespace.of("db"))).isTrue();
      assertThat(result.namespaceVersion(result.namespaceId(Namespace.of("db")))).isEqualTo(1);
    }

    @Test
    void committedTransactionInCheckpointIsSkipped() {
      UUID priorTxnId = UUID.randomUUID();
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .committed(priorTxnId)
          .build();

      // Transaction with the same ID as one already in the checkpoint
      ProtoCodec.Transaction t = txn(priorTxnId, createNs(2, 0, "should_not_appear", 1, -1));
      ProtoCatalogFile result = apply(file, t);

      assertThat(result.containsNamespace(Namespace.of("should_not_appear"))).isFalse();
    }

    @Test
    void sealedTransactionStopsLogReplay() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();

      ProtoCodec.Transaction sealed = sealedTxn(
          createTbl(1, 1, "sealed_tbl", 1, 1, "s3://sealed"));
      ProtoCodec.Transaction after = txn(
          createNs(2, 0, "ignored", 1, -1));
      ProtoCatalogFile result = apply(file, sealed, after);

      // Sealed transaction applied
      assertThat(result.location(TableIdentifier.of(Namespace.of("db"), "sealed_tbl")))
          .isEqualTo("s3://sealed");
      assertThat(result.isSealed()).isTrue();

      // Transaction after seal not applied
      assertThat(result.containsNamespace(Namespace.of("ignored"))).isFalse();
      assertThat(result.containsTransaction(after.id())).isFalse();
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
      ProtoCatalogFile result = apply(file, txn(
          createNs(1, 0, "a", 1, -1),
          createNs(2, 1, "b", 1, -1),
          createNs(3, 2, "c", 1, -1),
          createTbl(1, 3, "leaf", 1, -1, "s3://deep/leaf")));

      assertThat(result.containsNamespace(Namespace.of("a"))).isTrue();
      assertThat(result.containsNamespace(Namespace.of("a", "b"))).isTrue();
      assertThat(result.containsNamespace(Namespace.of("a", "b", "c"))).isTrue();
      assertThat(result.location(TableIdentifier.of(Namespace.of("a", "b", "c"), "leaf")))
          .isEqualTo("s3://deep/leaf");
    }

    @Test
    void dropAndRecreateDifferentNamespace() {
      byte[] file = catalog()
          .ns(1, 0, "staging", 1)
          .tbl(1, 1, "raw", 1, "s3://staging/raw")
          .build();

      // Drop table then namespace, then create a new namespace
      ProtoCodec.Transaction t1 = txn(dropTbl(1, 1));
      ProtoCodec.Transaction t2 = txn(dropNs(1, 1));
      ProtoCodec.Transaction t3 = txn(createNs(2, 0, "production", 1, -1));
      ProtoCatalogFile result = apply(file, t1, t2, t3);

      assertThat(result.containsNamespace(Namespace.of("staging"))).isFalse();
      assertThat(result.containsNamespace(Namespace.of("production"))).isTrue();
      assertThat(result.tables()).isEmpty();
    }

    @Test
    void createNamespaceWithPropertiesInSameTransaction() {
      byte[] file = catalog().build();
      ProtoCatalogFile result = apply(file, txn(
          createNs(1, 0, "db", 1, -1),
          setNsProp(1, -1, "owner", "alice"),
          setNsProp(1, -1, "env", "prod")));

      Map<String, String> props = result.namespaceProperties(Namespace.of("db"));
      assertThat(props).containsEntry("owner", "alice");
      assertThat(props).containsEntry("env", "prod");
    }

    @Test
    void setAndRemovePropertyInSequentialTransactions() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();

      ProtoCodec.Transaction t1 = txn(setNsProp(1, 1, "owner", "alice"));
      ProtoCodec.Transaction t2 = txn(removeNsProp(1, 1, "owner"));
      ProtoCatalogFile result = apply(file, t1, t2);

      assertThat(result.namespaceProperties(Namespace.of("db")))
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

      ProtoCatalogFile result = apply(file, t1, t2, t3, t4);

      // Verify final state
      assertThat(result.containsNamespace(Namespace.of("warehouse"))).isTrue();
      assertThat(result.containsNamespace(Namespace.of("warehouse", "bronze"))).isTrue();
      assertThat(result.containsNamespace(Namespace.of("warehouse", "silver"))).isTrue();
      assertThat(result.namespaceProperties(Namespace.of("warehouse")))
          .containsEntry("owner", "data-team");

      // raw_events was dropped
      assertThat(result.location(TableIdentifier.of(
          Namespace.of("warehouse", "bronze"), "raw_events"))).isNull();
      // enriched still exists at original location
      assertThat(result.location(TableIdentifier.of(
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
      ProtoCatalogFile result = apply(file);

      assertThat(result.containsNamespace(Namespace.of("db"))).isTrue();
      assertThat(result.containsNamespace(Namespace.of("db", "schema"))).isTrue();
      assertThat(result.namespaceVersion(result.namespaceId(Namespace.of("db")))).isEqualTo(2);
      assertThat(result.namespaceVersion(result.namespaceId(Namespace.of("db", "schema")))).isEqualTo(1);

      TableIdentifier t1 = TableIdentifier.of(Namespace.of("db", "schema"), "t1");
      TableIdentifier t2 = TableIdentifier.of(Namespace.of("db", "schema"), "t2");
      assertThat(result.location(t1)).isEqualTo("s3://t1/v3");
      assertThat(result.tableVersion(result.tableId(t1))).isEqualTo(3);
      assertThat(result.location(t2)).isEqualTo("s3://t2/v1");

      assertThat(result.namespaceProperties(Namespace.of("db")))
          .containsEntry("owner", "alice")
          .containsEntry("env", "prod");
      assertThat(result.namespaceProperties(Namespace.of("db", "schema")))
          .containsEntry("comment", "test schema");
    }

    @Test
    void emptyTransactionApplies() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      ProtoCodec.Transaction t = new ProtoCodec.Transaction(UUID.randomUUID(), false, List.of());
      ProtoCatalogFile result = apply(file, t);

      // No-op transaction should still be recorded
      assertThat(result.containsTransaction(t.id())).isTrue();
      assertThat(result.containsNamespace(Namespace.of("db"))).isTrue();
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
      ProtoCatalogFile result = apply(file);

      // Table exists and is accessible
      TableIdentifier events = TableIdentifier.of(Namespace.of("db"), "events");
      assertThat(result.tables()).contains(events);
      Integer tblId = result.tableId(events);
      assertThat(tblId).isNotNull();

      // It's inline (no pointer location, has inline metadata)
      assertThat(result.location(events)).isNull();
      assertThat(result.isInlineTable(tblId)).isTrue();
      assertThat(result.inlineMetadata(tblId)).isEqualTo(SAMPLE_INLINE_METADATA);
      assertThat(result.manifestListPrefix(tblId)).isEqualTo(SAMPLE_MANIFEST_PREFIX);
    }

    @Test
    void mixedInlineAndPointerTables() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "pointer_tbl", 1, "s3://bucket/pointer/v1")
          .inlineTbl(2, 1, "inline_tbl", 1, SAMPLE_INLINE_METADATA, SAMPLE_MANIFEST_PREFIX)
          .build();
      ProtoCatalogFile result = apply(file);

      TableIdentifier pointer = TableIdentifier.of(Namespace.of("db"), "pointer_tbl");
      TableIdentifier inline = TableIdentifier.of(Namespace.of("db"), "inline_tbl");

      // Both tables visible
      assertThat(result.tables()).containsExactlyInAnyOrder(pointer, inline);

      // Pointer table has location, not inline
      assertThat(result.location(pointer)).isEqualTo("s3://bucket/pointer/v1");
      assertThat(result.isInlineTable(result.tableId(pointer))).isFalse();

      // Inline table has metadata, no location
      assertThat(result.location(inline)).isNull();
      assertThat(result.isInlineTable(result.tableId(inline))).isTrue();
      assertThat(result.inlineMetadata(result.tableId(inline))).isEqualTo(SAMPLE_INLINE_METADATA);
    }

    @Test
    void inlineTableVersionTracking() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .inlineTbl(1, 1, "tbl", 3, SAMPLE_INLINE_METADATA, SAMPLE_MANIFEST_PREFIX)
          .build();
      ProtoCatalogFile result = apply(file);

      Integer tblId = result.tableId(TableIdentifier.of(Namespace.of("db"), "tbl"));
      assertThat(result.tableVersion(tblId)).isEqualTo(3);
    }

    @Test
    void inlineTableNotAPointer() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .inlineTbl(1, 1, "tbl", 1, SAMPLE_INLINE_METADATA, SAMPLE_MANIFEST_PREFIX)
          .build();
      ProtoCatalogFile result = apply(file);

      // location() returns null for inline tables -- they don't have an external file
      TableIdentifier tbl = TableIdentifier.of(Namespace.of("db"), "tbl");
      assertThat(result.location(tbl)).isNull();
      assertThat(result.isInlineTable(result.tableId(tbl))).isTrue();

      // A pointer table would return a location string
      assertThat(result.inlineMetadata(result.tableId(tbl))).isNotNull();
    }

    @Test
    void emptyMetadataBytes() {
      byte[] emptyMeta = "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .inlineTbl(1, 1, "tbl", 1, emptyMeta, "")
          .build();
      ProtoCatalogFile result = apply(file);

      Integer tblId = result.tableId(TableIdentifier.of(Namespace.of("db"), "tbl"));
      assertThat(result.inlineMetadata(tblId)).isEqualTo(emptyMeta);
      assertThat(result.manifestListPrefix(tblId)).isEmpty();
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
      ProtoCatalogFile result = apply(file);

      TableIdentifier t1 = TableIdentifier.of(Namespace.of("db1"), "t1");
      TableIdentifier t2 = TableIdentifier.of(Namespace.of("db2"), "t2");
      assertThat(result.inlineMetadata(result.tableId(t1))).isEqualTo(meta1);
      assertThat(result.inlineMetadata(result.tableId(t2))).isEqualTo(meta2);
      assertThat(result.manifestListPrefix(result.tableId(t1)))
          .isEqualTo("s3://b/db1/t1/metadata/snap-");
      assertThat(result.manifestListPrefix(result.tableId(t2)))
          .isEqualTo("s3://b/db2/t2/metadata/snap-");
    }
  }

  // ============================================================
  // Randomized tests (ported from TestLogCatalogFormat)
  // ============================================================

  @Nested
  class RandomizedTests {

    /**
     * Generates a random ProtoCatalogFile with reproducible state from a seed.
     * Mirrors generateRandomLogCatalogFile from TestLogCatalogFormat.
     */
    ProtoCatalogFile generateRandom(long seed) {
      Random rand = new Random(seed);
      ProtoCatalogFile.Builder builder = ProtoCatalogFile.builder(LOCATION);
      builder.setCatalogUuid(new UUID(rand.nextLong(), rand.nextLong()));

      int nextNsid = rand.nextInt(20) + 10;
      int nextTblid = rand.nextInt(100) + 10;
      builder.setNextNamespaceId(nextNsid);
      builder.setNextTableId(nextTblid);

      Map<Integer, String> nsNames = new HashMap<>();
      nsNames.put(0, ""); // root

      // Root namespace must be present for Mut.createNamespace() to work
      // correctly -- without it, auto-created root serializes as name=""
      // which becomes Namespace.of("") on replay instead of Namespace.empty().
      int rootVersion = rand.nextInt(3) + 1;
      builder.addNamespace(0, 0, "", rootVersion);

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
          .mapToObj(i -> new UUID(rand.nextLong(), rand.nextLong()))
          .collect(Collectors.toSet());
      for (UUID txnId : txns) {
        builder.addCommittedTransaction(txnId);
      }

      return builder.build();
    }

    /**
     * Asserts that two ProtoCatalogFile instances have identical observable state.
     * Does not compare nextNamespaceId / nextTableId because readInternal does not
     * update those counters during transaction replay -- they are corrected on
     * re-checkpoint but may differ between a replay-built catalog and a fresh one.
     */
    void assertCatalogEquals(ProtoCatalogFile expected, ProtoCatalogFile actual) {
      assertThat(actual.uuid()).isEqualTo(expected.uuid());
      assertThat(actual.isSealed()).isEqualTo(expected.isSealed());
      assertThat(actual.namespaces()).containsExactlyInAnyOrderElementsOf(expected.namespaces());
      assertThat(actual.tables()).containsExactlyInAnyOrderElementsOf(expected.tables());
      assertThat(actual.committedTransactions())
          .containsExactlyInAnyOrderElementsOf(expected.committedTransactions());

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
      ProtoCatalogFile original = generateRandom(seed);
      ProtoCatalogFile sameSeed = generateRandom(seed);
      assertCatalogEquals(original, sameSeed);

      byte[] fileBytes = toFileBytes(original);
      assertThat(fileBytes.length).isGreaterThan(ProtoCatalogFormat.HEADER_SIZE);

      ProtoCatalogFile roundtripped = ProtoCatalogFormat.readInternal(
          LOCATION, new ByteArrayInputStream(fileBytes), fileBytes.length);
      assertCatalogEquals(original, roundtripped);
      // Pure checkpoint roundtrip (no transactions) should also preserve counters
      assertThat(roundtripped.nextNamespaceId()).isEqualTo(original.nextNamespaceId());
      assertThat(roundtripped.nextTableId()).isEqualTo(original.nextTableId());
    }

    /**
     * Build transactions through the Mut API against random base state, serialize, append,
     * and verify correct application and conflict detection.
     * Ported from LCF testApplyTransaction.
     */
    @RepeatedTest(5)
    void applyMutTransactionOnRandomState() throws IOException {
      long seed = System.nanoTime();
      ProtoCatalogFile orig = generateRandom(seed);
      byte[] origBytes = toFileBytes(orig);

      Namespace dingos = Namespace.of("dingos");
      Namespace dingos_yaks = Namespace.of("dingos", "yaks");
      TableIdentifier tblY = TableIdentifier.of(dingos_yaks, "tblY");
      TableIdentifier tblD = TableIdentifier.of(dingos, "tblD");

      // txnA: create dingos -> dingos.yaks -> tblY
      ProtoCatalogFormat.Mut mutA = new ProtoCatalogFormat.Mut(orig);
      mutA.createNamespace(dingos);
      mutA.createNamespace(dingos_yaks);
      mutA.createTable(tblY, "yak://chinchilla/tblY");
      ProtoCodec.Transaction txnA = mutA.buildTransaction();

      // Append and read
      ProtoCatalogFile afterA = apply(origBytes, txnA);
      assertThat(afterA.containsNamespace(dingos)).isTrue();
      assertThat(afterA.containsNamespace(dingos_yaks)).isTrue();
      assertThat(afterA.location(tblY)).isEqualTo("yak://chinchilla/tblY");
      assertThat(afterA.containsTransaction(txnA.id())).isTrue();

      // Second round: re-checkpoint and re-read (as a concurrent writer would)
      byte[] afterABytes = toFileBytes(afterA);
      ProtoCatalogFile afterAFresh = ProtoCatalogFormat.readInternal(
          LOCATION, new ByteArrayInputStream(afterABytes), afterABytes.length);

      // txnB: update tblY
      ProtoCatalogFormat.Mut mutB = new ProtoCatalogFormat.Mut(afterAFresh);
      mutB.updateTable(tblY, "yak://chinchilla/tblY2");
      ProtoCodec.Transaction txnB = mutB.buildTransaction();

      // txnC: create tblD (independent, built from same snapshot)
      ProtoCatalogFormat.Mut mutC = new ProtoCatalogFormat.Mut(afterAFresh);
      mutC.createTable(tblD, "yak://chinchilla/tblD");
      ProtoCodec.Transaction txnC = mutC.buildTransaction();

      // txnD: also tries to update tblY from same snapshot -> should fail
      // because txnB already updated it
      ProtoCatalogFormat.Mut mutD = new ProtoCatalogFormat.Mut(afterAFresh);
      mutD.updateTable(tblY, "yak://chinchilla/tblY3");
      ProtoCodec.Transaction txnD = mutD.buildTransaction();

      ProtoCatalogFile afterAll = apply(afterABytes, txnB, txnC, txnD);

      // txnB succeeded: tblY updated
      assertThat(afterAll.location(tblY)).isEqualTo("yak://chinchilla/tblY2");
      assertThat(afterAll.containsTransaction(txnB.id())).isTrue();

      // txnC succeeded: tblD created
      assertThat(afterAll.location(tblD)).isEqualTo("yak://chinchilla/tblD");
      assertThat(afterAll.containsTransaction(txnC.id())).isTrue();

      // txnD failed: stale table version
      assertThat(afterAll.containsTransaction(txnD.id())).isFalse();
    }

    /**
     * Sealed transaction on random state: build from Mut, seal, verify later
     * transactions are not applied. Ported from LCF testSealTransaction.
     */
    @RepeatedTest(5)
    void sealedMutTransactionOnRandomState() throws IOException {
      ProtoCatalogFile orig = generateRandom(System.nanoTime());
      byte[] origBytes = toFileBytes(orig);

      Namespace dingos = Namespace.of("dingos");
      Namespace dingos_yaks = Namespace.of("dingos", "yaks");
      Namespace yaks = Namespace.of("yaks");
      TableIdentifier tblY = TableIdentifier.of(dingos_yaks, "tblY");
      TableIdentifier tblD = TableIdentifier.of(dingos, "tblD");

      // txnA: create structure
      ProtoCatalogFormat.Mut mutA = new ProtoCatalogFormat.Mut(orig);
      mutA.createNamespace(dingos);
      mutA.createNamespace(dingos_yaks);
      mutA.createTable(tblY, "yak://chinchilla/tblY");
      ProtoCodec.Transaction txnA = mutA.buildTransaction();

      ProtoCatalogFile afterA = apply(origBytes, txnA);
      byte[] afterABytes = toFileBytes(afterA);
      ProtoCatalogFile afterAFresh = ProtoCatalogFormat.readInternal(
          LOCATION, new ByteArrayInputStream(afterABytes), afterABytes.length);

      // txnB: update tblY, SEALED
      ProtoCatalogFormat.Mut mutB = new ProtoCatalogFormat.Mut(afterAFresh);
      mutB.updateTable(tblY, "yak://chinchilla/tblY2");
      ProtoCodec.Transaction txnBRaw = mutB.buildTransaction();
      // Make it sealed
      ProtoCodec.Transaction txnB = new ProtoCodec.Transaction(
          txnBRaw.id(), true, txnBRaw.actions());

      // txnC: would create yaks (valid, but after seal)
      ProtoCatalogFormat.Mut mutC = new ProtoCatalogFormat.Mut(afterAFresh);
      mutC.createNamespace(yaks);
      ProtoCodec.Transaction txnC = mutC.buildTransaction();

      // txnD: would create tblD (valid, but after seal)
      ProtoCatalogFormat.Mut mutD = new ProtoCatalogFormat.Mut(afterAFresh);
      mutD.createTable(tblD, "yak://chinchilla/tblD");
      ProtoCodec.Transaction txnD = mutD.buildTransaction();

      ProtoCatalogFile result = apply(afterABytes, txnB, txnC, txnD);

      // txnB applied, catalog is sealed
      assertThat(result.location(tblY)).isEqualTo("yak://chinchilla/tblY2");
      assertThat(result.containsTransaction(txnB.id())).isTrue();
      assertThat(result.isSealed()).isTrue();

      // txnC, txnD not applied (after seal)
      assertThat(result.containsNamespace(yaks)).isFalse();
      assertThat(result.location(tblD)).isNull();
      assertThat(result.containsTransaction(txnC.id())).isFalse();
      assertThat(result.containsTransaction(txnD.id())).isFalse();
    }

    /**
     * Full roundtrip: random state → checkpoint bytes → append Mut-built transaction →
     * re-checkpoint the result → read back → verify state matches.
     */
    @RepeatedTest(5)
    void mutTransactionSurvivesReCheckpoint() throws IOException {
      ProtoCatalogFile orig = generateRandom(System.nanoTime());
      byte[] origBytes = toFileBytes(orig);

      Namespace ns = Namespace.of("roundtrip_ns");
      TableIdentifier tbl = TableIdentifier.of(ns, "roundtrip_tbl");

      // Build and apply a transaction via Mut
      ProtoCatalogFormat.Mut mut = new ProtoCatalogFormat.Mut(orig);
      mut.createNamespace(ns);
      mut.createTable(tbl, "s3://roundtrip/v1");
      ProtoCodec.Transaction txn = mut.buildTransaction();

      ProtoCatalogFile afterTxn = apply(origBytes, txn);

      // Re-checkpoint (as would happen on compaction)
      byte[] reCheckpointed = toFileBytes(afterTxn);
      ProtoCatalogFile reRead = ProtoCatalogFormat.readInternal(
          LOCATION, new ByteArrayInputStream(reCheckpointed), reCheckpointed.length);

      // Verify all original state + new additions survived
      assertCatalogEquals(afterTxn, reRead);
      assertThat(reRead.containsNamespace(ns)).isTrue();
      assertThat(reRead.location(tbl)).isEqualTo("s3://roundtrip/v1");
    }
  }

  // ============================================================
  // TestInputFile (same as in TestProtoCatalogFormat)
  // ============================================================

  private static class TestInputFile implements InputFile {
    private final String location;

    TestInputFile(String location) {
      this.location = location;
    }

    @Override
    public long getLength() {
      return 0;
    }

    @Override
    public SeekableInputStream newStream() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public boolean exists() {
      return false;
    }
  }
}
