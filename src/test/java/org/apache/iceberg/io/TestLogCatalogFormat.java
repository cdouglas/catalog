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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.LogCatalogFormat.LogCatalogFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.ArgumentCaptor;

public class TestLogCatalogFormat {

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
  public void testLogStream() throws IOException {
    // Create a sample transaction to write to the stream
    UUID txnId = UUID.randomUUID();
    LogCatalogFormat.LogAction.CreateNamespace createNamespace =
        new LogCatalogFormat.LogAction.CreateNamespace("testNamespace", -1, 0, 1);
    final List<LogCatalogFormat.LogAction> actions = new ArrayList<>();
    actions.add(createNamespace);
    LogCatalogFormat.LogAction.Transaction transaction =
        new LogCatalogFormat.LogAction.Transaction(txnId, actions, true);

    // Write the transaction to a byte array
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    transaction.write(dos);
    byte[] data = baos.toByteArray();

    // Read the transaction from the byte array using LogStream
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    DataInputStream dis = new DataInputStream(bais);
    LogCatalogFormat.LogAction.LogStream logStream = new LogCatalogFormat.LogAction.LogStream(dis);

    // Verify that the LogStream correctly reads the transaction
    assertTrue(logStream.hasNext());
    LogCatalogFormat.LogAction.Transaction readTransaction = logStream.next();
    assertEquals(txnId, readTransaction.txnId);
    assertTrue(readTransaction.isSealed());
    assertEquals(1, readTransaction.actions.size());
    assertInstanceOf(
        LogCatalogFormat.LogAction.CreateNamespace.class, readTransaction.actions.get(0));
    assertFalse(logStream.hasNext());
  }

  @Test
  public void testEquivalenceSerde() throws IOException {
    final long seed = random.nextLong();
    LogCatalogFile a = generateRandomLogCatalogFile(seed);
    LogCatalogFile b = generateRandomLogCatalogFile(seed);
    assertEquals(a, b);

    byte[] aBytes = toBytes(a);
    byte[] bBytes = toBytes(b);
    assertThat(aBytes.length).isGreaterThan(0);
    assertArrayEquals(aBytes, bBytes);

    InputFile mockFile = mock(InputFile.class);
    LogCatalogFormat.Mut catalog = new LogCatalogFormat.Mut(mockFile);
    LogCatalogFormat format = new LogCatalogFormat();
    try (ByteArrayInputStream bis = new ByteArrayInputStream(aBytes)) {
      LogCatalogFile c = LogCatalogFormat.readInternal(catalog, bis);
      assertEquals(a, c);
    }
  }

  @Test
  public void testApplyTransaction() throws IOException {
    // prototyping commit
    LogCatalogFile orig = generateRandomLogCatalogFile(random.nextLong());
    final byte[] origBytes = toBytes(orig);
    final int origLen = origBytes.length;
    LogCatalogFormat.LogAction.Transaction txnA =
        new LogCatalogFormat.Mut(orig)
            .createNamespace(dingos)
            .createNamespace(dingos_yaks)
            .createTable(tblY, "yak://chinchilla/tblY")
            .diff();
    final byte[] txnABytes = toBytes(txnA);
    // should NOT apply; version conflict at root
    LogCatalogFormat.LogAction.Transaction txnB =
        new LogCatalogFormat.Mut(orig).createNamespace(yaks).createNamespace(yaks_dingos).diff();
    final byte[] txnBBytes = toBytes(txnB);
    final byte[] appended =
        Arrays.copyOf(origBytes, origBytes.length + txnABytes.length + txnBBytes.length);
    System.arraycopy(txnABytes, 0, appended, origLen, txnABytes.length);
    System.arraycopy(txnBBytes, 0, appended, origLen + txnABytes.length, txnBBytes.length);
    assertThat(appendBytes(origBytes, txnABytes, txnBBytes)).isEqualTo(appended);

    InputFile mockFile = mock(InputFile.class);
    LogCatalogFormat.Mut catalog = new LogCatalogFormat.Mut(mockFile);
    LogCatalogFormat format = new LogCatalogFormat();
    final LogCatalogFile c =
        LogCatalogFormat.readInternal(catalog, new ByteArrayInputStream(appended));
    assertThat(c.containsNamespace(dingos)).isTrue();
    assertThat(c.containsNamespace(dingos_yaks)).isTrue();
    assertThat(c.containsNamespace(yaks)).isFalse();
    assertThat(c.containsNamespace(yaks_dingos)).isFalse();
    assertThat(c.location(tblY)).isEqualTo("yak://chinchilla/tblY");
    // (amid random CatalogFile)
    // dingos.yaks.tblY exists

    catalog = new LogCatalogFormat.Mut(mockFile);
    final LogCatalogFile d =
        LogCatalogFormat.readInternal(
            catalog,
            new ByteArrayInputStream(
                appendBytes(
                    appended,
                    // update dingos.yaks.tblY
                    toBytes(
                        new LogCatalogFormat.Mut(c)
                            .updateTable(tblY, "yak://chinchilla/tblY2")
                            .diff()),
                    // create "dingos.tblD"
                    toBytes(
                        new LogCatalogFormat.Mut(c)
                            .createTable(tblD, "yak://chinchilla/tblD")
                            .diff()),
                    // attempt to create "yaks", "dingos.tblD", update "dingos.yaks.tblY"
                    // should fail, table already updated
                    toBytes(
                        new LogCatalogFormat.Mut(c)
                            .createNamespace(yaks)
                            .updateTable(tblY, "yak://chinchilla/tblY3")
                            .diff()))));
    assertThat(d.location(tblY)).isEqualTo("yak://chinchilla/tblY2");
    assertThat(d.location(tblD)).isEqualTo("yak://chinchilla/tblD");
    assertThat(d.containsNamespace(yaks)).isFalse();
  }

  @Test
  public void testSealTransaction() throws IOException {
    LogCatalogFile orig = generateRandomLogCatalogFile(random.nextLong());
    InputFile mockFile = mock(InputFile.class);
    LogCatalogFormat.Mut catalog = new LogCatalogFormat.Mut(mockFile);
    LogCatalogFormat format = new LogCatalogFormat();
    final LogCatalogFormat.LogAction.Transaction txnA =
        new LogCatalogFormat.Mut(orig)
            .createNamespace(dingos)
            .createNamespace(dingos_yaks)
            .createTable(tblY, "yak://chinchilla/tblY")
            .diff();
    final byte[] bBytes = appendBytes(toBytes(orig), toBytes(txnA));
    final LogCatalogFile b =
        LogCatalogFormat.readInternal(catalog, new ByteArrayInputStream(bBytes));

    catalog = new LogCatalogFormat.Mut(mockFile);
    final LogCatalogFormat.LogAction.Transaction txnB =
        new LogCatalogFormat.Mut(b).updateTable(tblY, "yak://chinchilla/tblY2").diff();
    txnB.seal(); // SEAL LOG at this point
    final LogCatalogFormat.LogAction.Transaction txnC =
        new LogCatalogFormat.Mut(b).createNamespace(yaks).createNamespace(yaks_dingos).diff();
    final LogCatalogFormat.LogAction.Transaction txnD =
        new LogCatalogFormat.Mut(b).createTable(tblD, "yak://chinchilla/tblD").diff();
    final byte[] cBytes = appendBytes(toBytes(b), toBytes(txnB), toBytes(txnC), toBytes(txnD));
    final LogCatalogFile c =
        LogCatalogFormat.readInternal(catalog, new ByteArrayInputStream(cBytes));

    assertThat(c.containsNamespace(dingos)).isTrue();
    assertThat(c.containsNamespace(dingos_yaks)).isTrue();
    assertThat(c.location(tblY)).isEqualTo("yak://chinchilla/tblY2");
    // TODO checkpoint needs to preserve committed transactions
    // assertThat(c.containsTransaction(txnA.txnId)).isTrue();
    assertThat(c.containsTransaction(txnB.txnId)).isTrue();
    // valid transactions after sealed are ignored
    assertThat(c.containsNamespace(yaks)).isFalse();
    assertThat(c.containsNamespace(yaks_dingos)).isFalse();
    assertThat(c.location(tblD)).isNull();
    assertThat(c.sealed).isTrue();
    assertThat(c.containsTransaction(txnC.txnId)).isFalse();
    assertThat(c.containsTransaction(txnD.txnId)).isFalse();
  }

  @Test
  public void testUnsealSerializedTransaction() throws IOException {
    // you're going to hell
    LogCatalogFile orig = generateRandomLogCatalogFile(random.nextLong());
    final LogCatalogFormat.LogAction.Transaction txnA =
        new LogCatalogFormat.Mut(orig)
            .createNamespace(dingos)
            .createNamespace(dingos_yaks)
            .createTable(tblY, "yak://chinchilla/tblY")
            .diff();
    assertThat(txnA.isSealed()).isFalse();
    byte[] txnABytes = toBytes(txnA);
    try (ByteArrayInputStream bais = new ByteArrayInputStream(txnABytes);
        DataInputStream dis = new DataInputStream(bais)) {
      assertEquals(LogCatalogFormat.LogAction.Type.TRANSACTION.opcode, dis.readByte());
      dis.mark(txnABytes.length - 1);
      assertThat(LogCatalogFormat.LogAction.Transaction.read(dis).isSealed()).isFalse();
      LogCatalogFormat.LogAction.Transaction.seal(txnABytes);
      dis.reset();
      assertThat(LogCatalogFormat.LogAction.Transaction.read(dis).isSealed()).isTrue();
      LogCatalogFormat.LogAction.Transaction.unseal(txnABytes);
      dis.reset();
      assertThat(LogCatalogFormat.LogAction.Transaction.read(dis).isSealed()).isFalse();
    }
  }

  @Test
  @SuppressWarnings("unchecked") // mocks
  public void testCASEmptyCommitPath() throws IOException {
    final String CATALOG_LOC = "yak://chinchilla/prod/catalog";
    InputFile initFile = mock(InputFile.class);
    when(initFile.exists()).thenReturn(false);
    when(initFile.location()).thenReturn(CATALOG_LOC);

    SupportsAtomicOperations fileIO = mock(SupportsAtomicOperations.class);
    AtomicOutputFile outputFile = mock(AtomicOutputFile.class);
    when(fileIO.newOutputFile(eq(initFile))).thenReturn(outputFile);

    CAS cas = mock(CAS.class);
    InputFile casFile = mock(InputFile.class);
    when(outputFile.prepare(any(), eq(AtomicOutputFile.Strategy.CAS))).thenReturn(cas);
    ArgumentCaptor<Supplier<InputStream>> inputStreamCaptor =
        ArgumentCaptor.forClass(Supplier.class);
    when(outputFile.writeAtomic(any(), inputStreamCaptor.capture())).thenReturn(casFile);

    // COMMIT init update
    LogCatalogFormat format = new LogCatalogFormat();
    LogCatalogFile init = format.empty(initFile).commit(fileIO);
    Supplier<InputStream> capturedSupplier = inputStreamCaptor.getValue();

    final byte[] capturedBytes;
    try (InputStream is = capturedSupplier.get();
        ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      byte[] buffer = new byte[1024];
      int length;
      while ((length = is.read(buffer)) != -1) {
        baos.write(buffer, 0, length);
      }
      capturedBytes = baos.toByteArray();
    }

    // VERIFY init LogCatalogFile returned matches bytes written
    LogCatalogFile written =
        LogCatalogFormat.readInternal(
            new LogCatalogFormat.Mut(casFile), new ByteArrayInputStream(capturedBytes));
    assertThat(written).isEqualTo(init);
    assertThat(written.containsNamespace(Namespace.empty())).isTrue();

    AtomicOutputFile updateFile = mock(AtomicOutputFile.class);
    when(casFile.exists()).thenReturn(true);
    when(casFile.location()).thenReturn(CATALOG_LOC);
    // Set up casFile.newStream() using InMemoryInputFile to get proper SeekableInputStream
    final byte[] finalCapturedBytes = capturedBytes;
    org.apache.iceberg.inmemory.InMemoryInputFile casInMemory =
        new org.apache.iceberg.inmemory.InMemoryInputFile(CATALOG_LOC, finalCapturedBytes);
    when(casFile.newStream()).thenAnswer(inv -> casInMemory.newStream());
    when(casFile.getLength()).thenReturn((long) finalCapturedBytes.length);
    // refreshed, so not eq(casFile)
    when(fileIO.newOutputFile((InputFile) any())).thenReturn(updateFile);

    // Use AtomicReference to capture and return written bytes for the APPEND path
    final java.util.concurrent.atomic.AtomicReference<org.apache.iceberg.inmemory.InMemoryInputFile> resultFile =
        new java.util.concurrent.atomic.AtomicReference<>();
    when(updateFile.prepare(any(), eq(AtomicOutputFile.Strategy.APPEND))).thenReturn(cas);
    when(updateFile.writeAtomic(any(), any())).thenAnswer(inv -> {
      Supplier<InputStream> supplier = inv.getArgument(1);
      try (InputStream is = supplier.get(); ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        byte[] buffer = new byte[1024];
        int length;
        while ((length = is.read(buffer)) != -1) {
          baos.write(buffer, 0, length);
        }
        // The appended content is the original + new transaction
        byte[] newBytes = baos.toByteArray();
        byte[] combined = new byte[finalCapturedBytes.length + newBytes.length];
        System.arraycopy(finalCapturedBytes, 0, combined, 0, finalCapturedBytes.length);
        System.arraycopy(newBytes, 0, combined, finalCapturedBytes.length, newBytes.length);
        resultFile.set(new org.apache.iceberg.inmemory.InMemoryInputFile(CATALOG_LOC, combined));
      }
      return resultFile.get();
    });

    // COMMIT via from, check bytes written match LogCatalogFile instance returned
    LogCatalogFile upd = format.from(written).createNamespace(yaks).commit(fileIO);
    assertThat(upd.containsNamespace(yaks)).isTrue();

    // VERIFY updated LogCatalogFile matches bytes written
    InputFile updFile = resultFile.get();
    LogCatalogFile writtenUpdate =
        LogCatalogFormat.readInternal(
            new LogCatalogFormat.Mut(updFile), updFile.newStream());
    assertThat(writtenUpdate).isEqualTo(upd);
    assertThat(writtenUpdate.containsNamespace(Namespace.empty())).isTrue();
    assertThat(writtenUpdate.containsNamespace(yaks)).isTrue();
  }

  @Test
  @SuppressWarnings("unchecked") // mocks
  public void testCASInitCommitPath() throws IOException {
    final String CATALOG_LOC = "yak://chinchilla/prod/catalog";
    InputFile initFile = mock(InputFile.class);
    when(initFile.exists()).thenReturn(false);
    when(initFile.location()).thenReturn(CATALOG_LOC);

    SupportsAtomicOperations fileIO = mock(SupportsAtomicOperations.class);
    AtomicOutputFile outputFile = mock(AtomicOutputFile.class);
    when(fileIO.newOutputFile(eq(initFile))).thenReturn(outputFile);

    CAS cas = mock(CAS.class);
    InputFile casFile = mock(InputFile.class);
    when(outputFile.prepare(any(), eq(AtomicOutputFile.Strategy.CAS))).thenReturn(cas);
    ArgumentCaptor<Supplier<InputStream>> inputStreamCaptor =
        ArgumentCaptor.forClass(Supplier.class);
    when(outputFile.writeAtomic(any(), inputStreamCaptor.capture())).thenReturn(casFile);

    // COMMIT init update
    LogCatalogFormat format = new LogCatalogFormat();
    LogCatalogFile init =
        format
            .empty(initFile)
            .createNamespace(dingos)
            .createNamespace(dingos_yaks)
            .createTable(tblY, "yak://chinchilla/tblY")
            .commit(fileIO);
    Supplier<InputStream> capturedSupplier = inputStreamCaptor.getValue();

    final byte[] capturedBytes;
    try (InputStream is = capturedSupplier.get();
        ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      byte[] buffer = new byte[1024];
      int length;
      while ((length = is.read(buffer)) != -1) {
        baos.write(buffer, 0, length);
      }
      capturedBytes = baos.toByteArray();
    }

    // VERIFY init LogCatalogFile returned matches bytes written
    LogCatalogFile written =
        LogCatalogFormat.readInternal(
            new LogCatalogFormat.Mut(casFile), new ByteArrayInputStream(capturedBytes));
    assertThat(written).isEqualTo(init);

    AtomicOutputFile updateFile = mock(AtomicOutputFile.class);
    when(casFile.exists()).thenReturn(true);
    when(casFile.location()).thenReturn(CATALOG_LOC);
    // Set up casFile.newStream() using InMemoryInputFile to get proper SeekableInputStream
    final byte[] finalCapturedBytes = capturedBytes;
    org.apache.iceberg.inmemory.InMemoryInputFile casInMemory =
        new org.apache.iceberg.inmemory.InMemoryInputFile(CATALOG_LOC, finalCapturedBytes);
    when(casFile.newStream()).thenAnswer(inv -> casInMemory.newStream());
    when(casFile.getLength()).thenReturn((long) finalCapturedBytes.length);
    // refreshed, so not eq(casFile)
    when(fileIO.newOutputFile((InputFile) any())).thenReturn(updateFile);

    // Use AtomicReference to capture and return written bytes for the APPEND path
    final java.util.concurrent.atomic.AtomicReference<org.apache.iceberg.inmemory.InMemoryInputFile> resultFile =
        new java.util.concurrent.atomic.AtomicReference<>();
    when(updateFile.prepare(any(), eq(AtomicOutputFile.Strategy.APPEND))).thenReturn(cas);
    when(updateFile.writeAtomic(any(), any())).thenAnswer(inv -> {
      Supplier<InputStream> supplier = inv.getArgument(1);
      try (InputStream is = supplier.get(); ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        byte[] buffer = new byte[1024];
        int length;
        while ((length = is.read(buffer)) != -1) {
          baos.write(buffer, 0, length);
        }
        // The appended content is the original + new transaction
        byte[] newBytes = baos.toByteArray();
        byte[] combined = new byte[finalCapturedBytes.length + newBytes.length];
        System.arraycopy(finalCapturedBytes, 0, combined, 0, finalCapturedBytes.length);
        System.arraycopy(newBytes, 0, combined, finalCapturedBytes.length, newBytes.length);
        resultFile.set(new org.apache.iceberg.inmemory.InMemoryInputFile(CATALOG_LOC, combined));
      }
      return resultFile.get();
    });

    // COMMIT via from, check bytes written match LogCatalogFile instance returned
    LogCatalogFile upd = format.from(written).createNamespace(yaks).commit(fileIO);
    assertThat(upd.containsNamespace(yaks)).isTrue();

    // VERIFY updated LogCatalogFile matches bytes written
    InputFile updFile = resultFile.get();
    LogCatalogFile writtenUpdate =
        LogCatalogFormat.readInternal(
            new LogCatalogFormat.Mut(updFile), updFile.newStream());
    assertThat(writtenUpdate).isEqualTo(upd);
    assertThat(writtenUpdate.containsNamespace(yaks)).isTrue();
  }

  private LogCatalogFile generateRandomLogCatalogFile(long seed) {
    final Random rand = new Random(seed);
    rand.setSeed(seed);
    InputFile location = mock(InputFile.class);
    final long msb = rand.nextLong();
    final long lsb = rand.nextLong();
    final UUID uuid = new UUID(msb, lsb);
    final int nextNsid = rand.nextInt(20) + 10;
    final int nextTblid = rand.nextInt(100) + 10;

    final Map<Namespace, Integer> nsids = new HashMap<>();
    final Map<Integer, Integer> nsVersion = new HashMap<>();
    final Map<Integer, Map<String, String>> nsProperties = new HashMap<>();
    final Map<TableIdentifier, Integer> tblIds = new HashMap<>();
    final Map<Integer, Integer> tblVersion = new HashMap<>();
    final Map<Integer, String> tblLocations = new HashMap<>();

    final Map<Integer, Namespace> nsLookup = new HashMap<>();

    nsids.put(Namespace.empty(), 0);
    nsLookup.put(0, Namespace.empty());
    nsVersion.put(0, rand.nextInt(1) + 1);
    // ns version \geq #children + #prop
    for (int nsid = 1; nsid < nextNsid; nsid += rand.nextInt(5) + 1) {
      int parentId = rand.nextInt(nsid) + 1;
      final Namespace ns;
      final Namespace parentNs = nsLookup.get(parentId);
      if (null == parentNs) { // root ns
        parentId = 0;
        ns = Namespace.of("ns" + nsid);
      } else {
        String[] levels = Arrays.copyOf(parentNs.levels(), parentNs.levels().length + 1);
        levels[levels.length - 1] = "ns" + nsid;
        ns = Namespace.of(levels);
      }
      final int parentVersion = nsVersion.get(parentId) + rand.nextInt(2) + 1;
      nsVersion.put(nsid, rand.nextInt(parentVersion - 1) + 1);
      nsVersion.put(parentId, parentVersion);

      nsids.put(ns, nsid);
      nsLookup.put(nsid, ns);
      Map<String, String> properties = new HashMap<>();
      for (int j = 0; j < rand.nextInt(5); j++) {
        properties.put("key" + j, nsid + "value" + j);
      }
      if (!properties.isEmpty()) {
        nsProperties.put(nsid, properties);
      }
      nsVersion.put(nsid, nsVersion.get(nsid) + rand.nextInt(2) + 1);
    }

    List<Integer> namespaces = new ArrayList<>(nsids.values());
    for (int i = 0; i < nextTblid; i++) {
      final int nsid = namespaces.get(rand.nextInt(namespaces.size()));
      final Namespace namespace = nsLookup.get(nsid);
      TableIdentifier tblId = TableIdentifier.of(namespace, "tbl" + i);
      tblIds.put(tblId, i);
      tblVersion.put(i, rand.nextInt(10));
      tblLocations.put(i, "location" + i);
      nsVersion.put(nsid, nsVersion.get(nsid) + rand.nextInt(2) + 1);
    }

    Set<UUID> committedTxn =
        IntStream.range(0, rand.nextInt(20) + 10)
            .mapToObj(ignored -> new UUID(rand.nextLong(), rand.nextLong()))
            .collect(Collectors.toSet());

    return new LogCatalogFile(
        location,
        uuid,
        nextNsid,
        nextTblid,
        false,
        nsids,
        nsVersion,
        nsProperties,
        tblIds,
        tblVersion,
        tblLocations,
        committedTxn);
  }

  static byte[] appendBytes(byte[] orig, byte[]... append) {
    final byte[] appended =
        Arrays.copyOf(orig, orig.length + Arrays.stream(append).mapToInt(a -> a.length).sum());
    int offset = orig.length;
    for (byte[] a : append) {
      System.arraycopy(a, 0, appended, offset, a.length);
      offset += a.length;
    }
    return appended;
  }

  static byte[] toBytes(LogCatalogFormat.LogAction.Transaction diffActions) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos)) {
      diffActions.write(dos);
      return bos.toByteArray();
    } catch (IOException e) {
      fail("Failed to write/read catalog file", e);
      throw new IllegalStateException("Failed to write/read diff", e);
    }
  }

  static byte[] toBytes(LogCatalogFile catalog) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      catalog.writeCheckpoint(bos);
      return bos.toByteArray();
    } catch (IOException e) {
      fail("Failed to write/read catalog file", e);
      throw new IllegalStateException("Failed to write/read catalog file", e);
    }
  }
}
