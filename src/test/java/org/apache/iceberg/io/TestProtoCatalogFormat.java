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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Tests for ProtoCatalogFormat and related classes.
 */
public class TestProtoCatalogFormat {

  private final Random random = new Random();

  private final Namespace dingos = Namespace.of("dingos");
  private final Namespace dingos_yaks = Namespace.of("dingos", "yaks");
  private final Namespace yaks = Namespace.of("yaks");
  private final Namespace yaks_dingos = Namespace.of("yaks", "dingos");
  private final TableIdentifier tblY = TableIdentifier.of(dingos_yaks, "tblY");
  private final TableIdentifier tblD = TableIdentifier.of(dingos, "tblD");

  @BeforeEach
  public void before(TestInfo info) {
    final String testName = info.getTestMethod().orElseThrow(RuntimeException::new).getName();
    random.setSeed(System.currentTimeMillis());
    System.out.println(testName + " seed: " + random.nextLong());
  }

  @Test
  public void testProtoCodecRoundtrip() throws IOException {
    // Create a catalog file with some state
    InputFile location = new TestInputFile("test://catalog");
    ProtoCatalogFile.Builder builder = ProtoCatalogFile.builder(location);

    builder.setNextNamespaceId(3);
    builder.setNextTableId(2);
    builder.addNamespace(1, 0, "db", 1);
    builder.addNamespace(2, 1, "schema", 1);
    builder.addTable(1, 2, "test_table", 1, "s3://bucket/table/metadata.json");
    builder.setNamespaceProperty(1, "owner", "alice");
    builder.addCommittedTransaction(UUID.randomUUID());

    ProtoCatalogFile original = builder.build();

    // Encode checkpoint
    ProtoCatalogFormat.ProtoIdManager idManager = new ProtoCatalogFormat.ProtoIdManager();
    idManager.setGlobals(original.nextNamespaceId(), original.nextTableId());

    // Create a Mut to use with encodeCheckpoint
    ProtoCatalogFormat.Mut mut = new ProtoCatalogFormat.Mut(original);
    byte[] checkpointBytes = ProtoCodec.encodeCheckpoint(original, mut, idManager);

    // Decode into a new builder
    ProtoCatalogFile.Builder newBuilder = ProtoCatalogFile.builder(location);
    ProtoCodec.decodeCheckpoint(checkpointBytes, newBuilder);
    ProtoCatalogFile decoded = newBuilder.build();

    // Verify the decoded state matches
    assertThat(decoded.namespaces()).containsExactlyInAnyOrderElementsOf(original.namespaces());
    assertThat(decoded.tables()).containsExactlyInAnyOrderElementsOf(original.tables());
    assertThat(decoded.nextNamespaceId()).isEqualTo(original.nextNamespaceId());
    assertThat(decoded.nextTableId()).isEqualTo(original.nextTableId());

    TableIdentifier tableId = TableIdentifier.of(Namespace.of("db", "schema"), "test_table");
    assertThat(decoded.location(tableId)).isEqualTo(original.location(tableId));

    Namespace db = Namespace.of("db");
    assertThat(decoded.namespaceProperties(db)).isEqualTo(original.namespaceProperties(db));
  }

  @Test
  public void testTransactionEncodeDecode() {
    // Create a transaction with some actions
    List<ProtoCodec.Action> actions = List.of(
        new ProtoCodec.CreateNamespaceAction(1, 1, 0, -1, "mydb"),
        new ProtoCodec.CreateTableAction(1, 1, 1, -1, "mytable", "s3://bucket/metadata.json")
    );

    UUID txnId = UUID.randomUUID();
    ProtoCodec.Transaction txn = new ProtoCodec.Transaction(txnId, false, actions);

    // Encode
    byte[] bytes = ProtoCodec.encodeTransaction(txn);

    // Decode
    ProtoCodec.Transaction decoded = ProtoCodec.decodeTransaction(bytes);

    assertThat(decoded.id()).isEqualTo(txnId);
    assertThat(decoded.isSealed()).isFalse();
    assertThat(decoded.actions()).hasSize(2);
    assertThat(decoded.actions().get(0)).isInstanceOf(ProtoCodec.CreateNamespaceAction.class);
    assertThat(decoded.actions().get(1)).isInstanceOf(ProtoCodec.CreateTableAction.class);
  }

  @Test
  public void testTransactionVerifyAndApply() {
    InputFile location = new TestInputFile("test://catalog");
    ProtoCatalogFile.Builder builder = ProtoCatalogFile.builder(location);

    // Create a transaction that creates a namespace and table
    List<ProtoCodec.Action> actions = List.of(
        new ProtoCodec.CreateNamespaceAction(1, 1, 0, -1, "mydb"),
        new ProtoCodec.CreateTableAction(1, 1, 1, -1, "mytable", "s3://bucket/metadata.json")
    );

    ProtoCodec.Transaction txn = new ProtoCodec.Transaction(UUID.randomUUID(), false, actions);

    // Verify should pass on empty catalog
    assertThat(txn.verify(builder)).isTrue();

    // Apply
    txn.apply(builder);

    ProtoCatalogFile result = builder.build();
    assertThat(result.containsNamespace(Namespace.of("mydb"))).isTrue();
    assertThat(result.location(TableIdentifier.of(Namespace.of("mydb"), "mytable")))
        .isEqualTo("s3://bucket/metadata.json");
  }

  @Test
  public void testDropNamespaceVerifyFailsOnVersionMismatch() {
    InputFile location = new TestInputFile("test://catalog");
    ProtoCatalogFile.Builder builder = ProtoCatalogFile.builder(location);
    builder.addNamespace(1, 0, "mydb", 2);  // version 2

    // Try to drop with wrong version
    ProtoCodec.DropNamespaceAction drop = new ProtoCodec.DropNamespaceAction(1, 1);  // expects version 1
    assertThat(drop.verify(builder)).isFalse();

    // Correct version should verify
    ProtoCodec.DropNamespaceAction correctDrop = new ProtoCodec.DropNamespaceAction(1, 2);
    assertThat(correctDrop.verify(builder)).isTrue();
  }

  @Test
  public void testFileStructureReadWrite() throws IOException {
    // Test the full file structure: header + checkpoint + transactions
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    // Write header
    out.write(ProtoCatalogFormat.MAGIC);
    writeInt(out, ProtoCatalogFormat.FORMAT_VERSION);

    // Write empty checkpoint
    InputFile location = new TestInputFile("test://catalog");
    ProtoCatalogFile empty = ProtoCatalogFile.empty(location);
    ProtoCatalogFormat.ProtoIdManager idManager = new ProtoCatalogFormat.ProtoIdManager();
    ProtoCatalogFormat.Mut mut = new ProtoCatalogFormat.Mut(empty);
    byte[] checkpointBytes = ProtoCodec.encodeCheckpoint(empty, mut, idManager);
    writeVarint(out, checkpointBytes.length);
    out.write(checkpointBytes);

    // Write a transaction
    List<ProtoCodec.Action> actions = List.of(
        new ProtoCodec.CreateNamespaceAction(1, 1, 0, -1, "testns")
    );
    ProtoCodec.Transaction txn = new ProtoCodec.Transaction(UUID.randomUUID(), false, actions);
    byte[] txnBytes = ProtoCodec.encodeTransaction(txn);
    writeVarint(out, txnBytes.length);
    out.write(txnBytes);

    // Read it back
    byte[] fileBytes = out.toByteArray();
    ByteArrayInputStream in = new ByteArrayInputStream(fileBytes);

    ProtoCatalogFile result = ProtoCatalogFormat.readInternal(
        location, in, fileBytes.length);

    // Verify the transaction was applied
    assertThat(result.containsNamespace(Namespace.of("testns"))).isTrue();
    assertThat(result.containsTransaction(txn.id())).isTrue();
  }

  @Test
  public void testBuildActionsFromMut() {
    InputFile location = new TestInputFile("test://catalog");
    ProtoCatalogFile empty = ProtoCatalogFile.empty(location);
    ProtoCatalogFormat.Mut mut = new ProtoCatalogFormat.Mut(empty);

    // Accumulate some changes
    mut.createNamespace(Namespace.of("db"), Collections.singletonMap("owner", "bob"));
    mut.createTable(TableIdentifier.of(Namespace.of("db"), "table1"), "s3://bucket/t1");

    // Build transaction
    ProtoCodec.Transaction txn = mut.buildTransaction();

    assertThat(txn.id()).isNotNull();
    assertThat(txn.actions()).isNotEmpty();

    // Find the create namespace action
    boolean hasCreateNs = txn.actions().stream()
        .anyMatch(a -> a instanceof ProtoCodec.CreateNamespaceAction);
    assertThat(hasCreateNs).isTrue();

    // Find the create table action
    boolean hasCreateTable = txn.actions().stream()
        .anyMatch(a -> a instanceof ProtoCodec.CreateTableAction);
    assertThat(hasCreateTable).isTrue();
  }

  // ============================================================
  // Conditional Transaction Application Tests
  // ============================================================

  @Test
  public void testApplyTransactionConditionally() throws IOException {
    // Create an empty base catalog
    InputFile location = new TestInputFile("test://catalog");
    ProtoCatalogFile orig = ProtoCatalogFile.empty(location);
    byte[] origBytes = toFullFileBytes(orig);

    // Transaction A: create dingos -> dingos.yaks -> tblY
    // Build actions directly to control the exact IDs
    List<ProtoCodec.Action> actionsA = List.of(
        new ProtoCodec.CreateNamespaceAction(1, 1, 0, -1, "dingos"),
        new ProtoCodec.CreateNamespaceAction(2, 1, 1, -1, "yaks"),
        new ProtoCodec.CreateTableAction(1, 1, 2, -1, "tblY", "yak://chinchilla/tblY")
    );
    ProtoCodec.Transaction txnA = new ProtoCodec.Transaction(UUID.randomUUID(), false, actionsA);
    byte[] txnABytes = toTransactionRecord(txnA);

    // Append transaction to the file
    byte[] combined = appendBytes(origBytes, txnABytes);

    // Read the combined file
    ProtoCatalogFile result = ProtoCatalogFormat.readInternal(
        location, new ByteArrayInputStream(combined), combined.length);

    // Verify txnA was applied
    assertThat(result.containsNamespace(dingos)).isTrue();
    assertThat(result.containsNamespace(dingos_yaks)).isTrue();
    assertThat(result.location(tblY)).isEqualTo("yak://chinchilla/tblY");
    assertThat(result.containsTransaction(txnA.id())).isTrue();
  }

  @Test
  public void testApplyMultipleTransactionsWithConflicts() throws IOException {
    InputFile location = new TestInputFile("test://catalog");

    // Start with a catalog that has dingos.yaks namespace and tblY at version 1
    ProtoCatalogFile.Builder builder = ProtoCatalogFile.builder(location);
    builder.setNextNamespaceId(3);
    builder.setNextTableId(2);
    builder.addNamespace(1, 0, "dingos", 1);
    builder.addNamespace(2, 1, "yaks", 1);
    builder.addTable(1, 2, "tblY", 1, "yak://chinchilla/tblY");
    ProtoCatalogFile orig = builder.build();
    byte[] origBytes = toFullFileBytes(orig);

    // txnA: update tblY (from version 1 to 2) - should succeed
    List<ProtoCodec.Action> actionsA = List.of(
        new ProtoCodec.UpdateTableLocationAction(1, 1, "yak://chinchilla/tblY2")
    );
    ProtoCodec.Transaction txnA = new ProtoCodec.Transaction(UUID.randomUUID(), false, actionsA);

    // txnB: create tblD in dingos - should succeed
    List<ProtoCodec.Action> actionsB = List.of(
        new ProtoCodec.CreateTableAction(2, 1, 1, 1, "tblD", "yak://chinchilla/tblD")
    );
    ProtoCodec.Transaction txnB = new ProtoCodec.Transaction(UUID.randomUUID(), false, actionsB);

    // txnC: also tries to update tblY from version 1 - should FAIL
    // because txnA already updated it to version 2
    List<ProtoCodec.Action> actionsC = List.of(
        new ProtoCodec.CreateNamespaceAction(3, 1, 0, -1, "yaks"),
        new ProtoCodec.UpdateTableLocationAction(1, 1, "yak://chinchilla/tblY3")
    );
    ProtoCodec.Transaction txnC = new ProtoCodec.Transaction(UUID.randomUUID(), false, actionsC);

    // Combine all transactions
    byte[] allTxns = appendBytes(
        origBytes,
        toTransactionRecord(txnA),
        toTransactionRecord(txnB),
        toTransactionRecord(txnC)
    );

    ProtoCatalogFile result = ProtoCatalogFormat.readInternal(
        location, new ByteArrayInputStream(allTxns), allTxns.length);

    // txnA succeeded - tblY updated to version 2
    assertThat(result.location(tblY)).isEqualTo("yak://chinchilla/tblY2");
    assertThat(result.containsTransaction(txnA.id())).isTrue();

    // txnB succeeded - tblD created
    assertThat(result.location(tblD)).isEqualTo("yak://chinchilla/tblD");
    assertThat(result.containsTransaction(txnB.id())).isTrue();

    // txnC failed due to version conflict on tblY update
    assertThat(result.containsNamespace(yaks)).isFalse();  // yaks at root level, different from dingos.yaks
    assertThat(result.location(tblY)).isNotEqualTo("yak://chinchilla/tblY3");
    assertThat(result.containsTransaction(txnC.id())).isFalse();
  }

  @Test
  public void testSealedTransactionStopsProcessing() throws IOException {
    InputFile location = new TestInputFile("test://catalog");

    // Start with a catalog that has dingos.yaks namespace and tblY
    ProtoCatalogFile.Builder builder = ProtoCatalogFile.builder(location);
    builder.setNextNamespaceId(3);
    builder.setNextTableId(2);
    builder.addNamespace(1, 0, "dingos", 1);
    builder.addNamespace(2, 1, "yaks", 1);
    builder.addTable(1, 2, "tblY", 1, "yak://chinchilla/tblY");
    ProtoCatalogFile orig = builder.build();
    byte[] origBytes = toFullFileBytes(orig);

    // txnA: update tblY - this transaction is SEALED
    List<ProtoCodec.Action> actionsA = List.of(
        new ProtoCodec.UpdateTableLocationAction(1, 1, "yak://chinchilla/tblY2")
    );
    // Create as sealed directly
    ProtoCodec.Transaction txnA = new ProtoCodec.Transaction(UUID.randomUUID(), true, actionsA);
    byte[] txnARecord = toTransactionRecord(txnA);

    // txnB: create yaks namespace (valid transaction, but comes after sealed)
    List<ProtoCodec.Action> actionsB = List.of(
        new ProtoCodec.CreateNamespaceAction(3, 1, 0, -1, "yaks_root")
    );
    ProtoCodec.Transaction txnB = new ProtoCodec.Transaction(UUID.randomUUID(), false, actionsB);

    // txnC: create tblD (valid transaction, but comes after sealed)
    List<ProtoCodec.Action> actionsC = List.of(
        new ProtoCodec.CreateTableAction(2, 1, 1, 1, "tblD", "yak://chinchilla/tblD")
    );
    ProtoCodec.Transaction txnC = new ProtoCodec.Transaction(UUID.randomUUID(), false, actionsC);

    // Combine all transactions
    byte[] allTxns = appendBytes(
        origBytes,
        txnARecord,
        toTransactionRecord(txnB),
        toTransactionRecord(txnC)
    );

    ProtoCatalogFile result = ProtoCatalogFormat.readInternal(
        location, new ByteArrayInputStream(allTxns), allTxns.length);

    // Verify txnA was applied and catalog is sealed
    assertThat(result.location(tblY)).isEqualTo("yak://chinchilla/tblY2");
    assertThat(result.containsTransaction(txnA.id())).isTrue();
    assertThat(result.isSealed()).isTrue();

    // Verify txnB and txnC were NOT applied (after sealed transaction)
    assertThat(result.containsNamespace(Namespace.of("yaks_root"))).isFalse();
    assertThat(result.location(tblD)).isNull();
    assertThat(result.containsTransaction(txnB.id())).isFalse();
    assertThat(result.containsTransaction(txnC.id())).isFalse();
  }

  @Test
  public void testDuplicateTransactionIdIgnored() throws IOException {
    InputFile location = new TestInputFile("test://catalog");
    ProtoCatalogFile orig = ProtoCatalogFile.empty(location);
    byte[] origBytes = toFullFileBytes(orig);

    // txnA: create namespace with explicit ID
    List<ProtoCodec.Action> actionsA = List.of(
        new ProtoCodec.CreateNamespaceAction(1, 1, 0, -1, "dingos")
    );
    ProtoCodec.Transaction txnA = new ProtoCodec.Transaction(UUID.randomUUID(), false, actionsA);
    byte[] txnARecord = toTransactionRecord(txnA);

    // Duplicate the same transaction (simulating retry)
    byte[] withDuplicates = appendBytes(origBytes, txnARecord, txnARecord, txnARecord);

    ProtoCatalogFile result = ProtoCatalogFormat.readInternal(
        location, new ByteArrayInputStream(withDuplicates), withDuplicates.length);

    // Transaction should only be applied once
    assertThat(result.containsNamespace(dingos)).isTrue();
    assertThat(result.containsTransaction(txnA.id())).isTrue();

    // Namespace version should be 1, not 3 (if it were applied 3 times)
    Integer nsId = result.namespaceId(dingos);
    assertThat(nsId).isNotNull();
    assertThat(result.namespaceVersion(nsId)).isEqualTo(1);
  }

  @Test
  public void testTableVersionConflict() throws IOException {
    InputFile location = new TestInputFile("test://catalog");

    // Start with a catalog that has a table
    ProtoCatalogFile.Builder builder = ProtoCatalogFile.builder(location);
    builder.setNextNamespaceId(3);
    builder.setNextTableId(2);
    builder.addNamespace(1, 0, "dingos", 2);
    builder.addNamespace(2, 1, "yaks", 1);
    builder.addTable(1, 2, "tblY", 1, "yak://original");
    ProtoCatalogFile orig = builder.build();
    byte[] origBytes = toFullFileBytes(orig);

    // txnA: update table to version 2
    List<ProtoCodec.Action> actionsA = List.of(
        new ProtoCodec.UpdateTableLocationAction(1, 1, "yak://updated1")
    );
    ProtoCodec.Transaction txnA = new ProtoCodec.Transaction(UUID.randomUUID(), false, actionsA);

    // txnB: also tries to update from version 1 - should fail
    List<ProtoCodec.Action> actionsB = List.of(
        new ProtoCodec.UpdateTableLocationAction(1, 1, "yak://updated2")
    );
    ProtoCodec.Transaction txnB = new ProtoCodec.Transaction(UUID.randomUUID(), false, actionsB);

    byte[] combined = appendBytes(origBytes,
        toTransactionRecord(txnA),
        toTransactionRecord(txnB));

    ProtoCatalogFile result = ProtoCatalogFormat.readInternal(
        location, new ByteArrayInputStream(combined), combined.length);

    // txnA succeeds
    assertThat(result.location(tblY)).isEqualTo("yak://updated1");
    assertThat(result.containsTransaction(txnA.id())).isTrue();

    // txnB fails due to version mismatch
    assertThat(result.containsTransaction(txnB.id())).isFalse();
  }

  @Test
  public void testReadTableAction() throws IOException {
    InputFile location = new TestInputFile("test://catalog");

    // Start with a catalog that has a table at version 1
    ProtoCatalogFile.Builder builder = ProtoCatalogFile.builder(location);
    builder.setNextNamespaceId(2);
    builder.setNextTableId(2);
    builder.addNamespace(1, 0, "ns", 1);
    builder.addTable(1, 1, "tbl", 1, "s3://original");
    ProtoCatalogFile orig = builder.build();
    byte[] origBytes = toFullFileBytes(orig);

    // txnA: update table
    List<ProtoCodec.Action> actionsA = List.of(
        new ProtoCodec.UpdateTableLocationAction(1, 1, "s3://updated")
    );
    ProtoCodec.Transaction txnA = new ProtoCodec.Transaction(UUID.randomUUID(), false, actionsA);

    // txnB: read table at version 1 - should fail because txnA updated it
    List<ProtoCodec.Action> actionsB = List.of(
        new ProtoCodec.ReadTableAction(1, 1),
        new ProtoCodec.CreateNamespaceAction(2, 1, 0, -1, "newns")
    );
    ProtoCodec.Transaction txnB = new ProtoCodec.Transaction(UUID.randomUUID(), false, actionsB);

    byte[] combined = appendBytes(origBytes,
        toTransactionRecord(txnA),
        toTransactionRecord(txnB));

    ProtoCatalogFile result = ProtoCatalogFormat.readInternal(
        location, new ByteArrayInputStream(combined), combined.length);

    // txnA succeeds
    assertThat(result.containsTransaction(txnA.id())).isTrue();

    // txnB fails because read-your-writes validation fails (table was modified)
    assertThat(result.containsTransaction(txnB.id())).isFalse();
    assertThat(result.containsNamespace(Namespace.of("newns"))).isFalse();
  }

  @Test
  public void testEquivalenceSerde() throws IOException {
    InputFile location = new TestInputFile("test://catalog");

    // Create a catalog with known structure
    ProtoCatalogFile.Builder builder = ProtoCatalogFile.builder(location);
    builder.setNextNamespaceId(5);
    builder.setNextTableId(3);
    builder.addNamespace(1, 0, "db1", 2);
    builder.addNamespace(2, 1, "schema1", 1);
    builder.addNamespace(3, 0, "db2", 1);
    builder.addNamespace(4, 3, "schema2", 1);
    builder.addTable(1, 2, "table1", 1, "s3://bucket/t1");
    builder.addTable(2, 4, "table2", 2, "s3://bucket/t2");
    builder.setNamespaceProperty(1, "owner", "alice");
    builder.setNamespaceProperty(2, "comment", "test schema");
    builder.addCommittedTransaction(UUID.randomUUID());
    builder.addCommittedTransaction(UUID.randomUUID());

    ProtoCatalogFile original = builder.build();

    // Roundtrip through bytes
    byte[] bytes = toFullFileBytes(original);
    assertThat(bytes.length).isGreaterThan(ProtoCatalogFormat.HEADER_SIZE);

    ProtoCatalogFile roundtripped = ProtoCatalogFormat.readInternal(
        location, new ByteArrayInputStream(bytes), bytes.length);

    // Verify all state matches
    assertThat(roundtripped.namespaces()).containsExactlyInAnyOrderElementsOf(original.namespaces());
    assertThat(roundtripped.tables()).containsExactlyInAnyOrderElementsOf(original.tables());
    assertThat(roundtripped.nextNamespaceId()).isEqualTo(original.nextNamespaceId());
    assertThat(roundtripped.nextTableId()).isEqualTo(original.nextTableId());

    // Check specific entries
    Namespace db1 = Namespace.of("db1");
    assertThat(roundtripped.namespaceProperties(db1)).isEqualTo(original.namespaceProperties(db1));

    TableIdentifier table1 = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    assertThat(roundtripped.location(table1)).isEqualTo(original.location(table1));

    TableIdentifier table2 = TableIdentifier.of(Namespace.of("db2", "schema2"), "table2");
    assertThat(roundtripped.location(table2)).isEqualTo(original.location(table2));
  }

  // ============================================================
  // Helper Methods
  // ============================================================

  private ProtoCatalogFile generateRandomProtoCatalogFile(long seed, InputFile location) {
    final Random rand = new Random(seed);
    ProtoCatalogFile.Builder builder = ProtoCatalogFile.builder(location);

    final int nextNsid = rand.nextInt(20) + 10;
    final int nextTblid = rand.nextInt(100) + 10;
    builder.setNextNamespaceId(nextNsid);
    builder.setNextTableId(nextTblid);

    Map<Integer, String> nsNames = new HashMap<>();
    nsNames.put(0, "");  // root

    // Generate namespaces
    for (int nsid = 1; nsid < nextNsid; nsid += rand.nextInt(5) + 1) {
      int parentId = rand.nextInt(nsid);
      if (!nsNames.containsKey(parentId)) {
        parentId = 0;
      }
      String name = "ns" + nsid;
      nsNames.put(nsid, name);
      int version = rand.nextInt(5) + 1;
      builder.addNamespace(nsid, parentId, name, version);

      // Random properties
      for (int j = 0; j < rand.nextInt(3); j++) {
        builder.setNamespaceProperty(nsid, "key" + j, "value" + j);
      }
    }

    // Generate tables
    List<Integer> nsIds = new ArrayList<>(nsNames.keySet());
    nsIds.remove(Integer.valueOf(0));  // don't put tables in root
    if (!nsIds.isEmpty()) {
      for (int tblid = 1; tblid < nextTblid; tblid += rand.nextInt(10) + 1) {
        int nsid = nsIds.get(rand.nextInt(nsIds.size()));
        String name = "tbl" + tblid;
        int version = rand.nextInt(5) + 1;
        String loc = "s3://bucket/table" + tblid;
        builder.addTable(tblid, nsid, name, version, loc);
      }
    }

    // Random committed transactions
    Set<UUID> committedTxns = IntStream.range(0, rand.nextInt(20) + 5)
        .mapToObj(i -> new UUID(rand.nextLong(), rand.nextLong()))
        .collect(Collectors.toSet());
    for (UUID txnId : committedTxns) {
      builder.addCommittedTransaction(txnId);
    }

    return builder.build();
  }

  private byte[] toFullFileBytes(ProtoCatalogFile catalog) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    // Header
    out.write(ProtoCatalogFormat.MAGIC);
    writeInt(out, ProtoCatalogFormat.FORMAT_VERSION);

    // Checkpoint
    ProtoCatalogFormat.ProtoIdManager idManager = new ProtoCatalogFormat.ProtoIdManager();
    idManager.setGlobals(catalog.nextNamespaceId(), catalog.nextTableId());
    ProtoCatalogFormat.Mut mut = new ProtoCatalogFormat.Mut(catalog);
    byte[] checkpointBytes = ProtoCodec.encodeCheckpoint(catalog, mut, idManager);
    writeVarint(out, checkpointBytes.length);
    out.write(checkpointBytes);

    return out.toByteArray();
  }

  private byte[] toTransactionRecord(ProtoCodec.Transaction txn) throws IOException {
    byte[] txnBytes = ProtoCodec.encodeTransaction(txn);
    return toTransactionRecordRaw(txnBytes);
  }

  private byte[] toTransactionRecordRaw(byte[] txnBytes) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeVarint(out, txnBytes.length);
    out.write(txnBytes);
    return out.toByteArray();
  }

  private static byte[] appendBytes(byte[] orig, byte[]... append) {
    int totalLen = orig.length + Arrays.stream(append).mapToInt(a -> a.length).sum();
    byte[] result = Arrays.copyOf(orig, totalLen);
    int offset = orig.length;
    for (byte[] a : append) {
      System.arraycopy(a, 0, result, offset, a.length);
      offset += a.length;
    }
    return result;
  }

  private static void writeInt(ByteArrayOutputStream out, int value) throws IOException {
    out.write((value >> 24) & 0xFF);
    out.write((value >> 16) & 0xFF);
    out.write((value >> 8) & 0xFF);
    out.write(value & 0xFF);
  }

  private static void writeVarint(ByteArrayOutputStream out, int value) throws IOException {
    while ((value & ~0x7F) != 0) {
      out.write((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    out.write(value);
  }

  // Simple test InputFile implementation
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
