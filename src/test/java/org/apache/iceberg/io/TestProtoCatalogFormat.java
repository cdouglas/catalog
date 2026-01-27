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
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;

/**
 * Tests for ProtoCatalogFormat and related classes.
 */
public class TestProtoCatalogFormat {

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

  // Helper methods
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
