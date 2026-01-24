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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestCatalogFile {

  private static final Namespace NS1 = Namespace.of("db", "dingos", "yaks", "prod");
  private static final Namespace NS2 = Namespace.of("db", "dingos", "yaks", "qa");
  private static final Namespace NS3 = Namespace.of("db", "dingos", "yaks", "dev");
  private static final Namespace NS4 = Namespace.of("db", "dingos", "yaks", "ved", "staging");
  private static final TableIdentifier TBL1 = TableIdentifier.of(NS1, "table1");
  private static final TableIdentifier TBL2 = TableIdentifier.of(NS1, "table2");
  private static final TableIdentifier TBL3 = TableIdentifier.of(NS2, "table3");
  private static final TableIdentifier TBL4 = TableIdentifier.of(NS3, "table4");
  private static final TableIdentifier TBL5 = TableIdentifier.of(Namespace.empty(), "table4");

  static Stream<CatalogFormat<?, ?>> catalogFormats() {
    return Stream.of(new CASCatalogFormat(), new LogCatalogFormat());
  }

  private final Random random = new Random();
  private InputFile nullFile;
  private SupportsAtomicOperations fileIO;

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void before(TestInfo info) throws IOException {
    final String testName = info.getTestMethod().orElseThrow(RuntimeException::new).getName();
    random.setSeed(System.currentTimeMillis());
    System.out.println(testName + " seed: " + random.nextLong());
    nullFile = mock(InputFile.class);
    when(nullFile.location()).thenReturn("null://null/null/null");
    AtomicOutputFile outputFile = mock(AtomicOutputFile.class);
    CAS token = mock(CAS.class);
    when(outputFile.prepare(any(), eq(AtomicOutputFile.Strategy.CAS))).thenReturn(token);
    when(outputFile.writeAtomic(any(), any())).thenReturn(nullFile);
    fileIO = mock(SupportsAtomicOperations.class);
    when(fileIO.newOutputFile(any(InputFile.class))).thenReturn(outputFile);
  }

  @ParameterizedTest
  @MethodSource("catalogFormats")
  public void testEmptyCatalog(CatalogFormat<?, ?> format) {
    CatalogFile empty = format.empty(nullFile).commit(fileIO);
    assertThat(empty.containsNamespace(Namespace.empty())).isTrue();

    CatalogFile step =
        format
            .from(empty)
            .createNamespace(NS1, Collections.singletonMap("key0", "value0"))
            .commit(fileIO);
    checkNamespaces(step, Namespace.empty(), NS1);
  }

  @ParameterizedTest
  @MethodSource("catalogFormats")
  public void testCatalogNamespace(CatalogFormat<?, ?> format) {
    final Map<String, String> ns1PropsInit = Collections.singletonMap("key0", "value0");
    CatalogFile catalogFile =
        format
            .empty(nullFile)
            .createNamespace(NS1, ns1PropsInit)
            .createNamespace(NS2, Collections.emptyMap())
            .createTable(TBL1, "gs://bucket/path/to/table1")
            .createTable(TBL2, "gs://bucket/path/to/table2")
            .commit(fileIO); // ignored; just passing info between CatalogFile

    checkNamespaces(catalogFile, Namespace.empty(), NS1, NS2);
    assertThat(catalogFile.namespaceProperties(NS1)).containsExactlyEntriesOf(ns1PropsInit);

    final Map<String, String> ns1Props = Collections.singletonMap("key1", "value1");
    CatalogFile updateProp =
        format
            .from(catalogFile)
            .updateProperties(NS1, Collections.singletonMap("key0", null))
            .updateProperties(NS1, ns1Props)
            .commit(fileIO);
    assertThat(updateProp).isNotEqualTo(catalogFile);
    checkNamespaces(updateProp, Namespace.empty(), NS1, NS2);
    assertThat(updateProp.namespaceProperties(NS1)).containsExactlyEntriesOf(ns1Props);

    CatalogFile drop = format.from(updateProp).dropNamespace(NS2).commit(fileIO);
    checkNamespaces(drop, Namespace.empty(), NS1);
    assertThat(drop.namespaceProperties(NS1)).containsExactlyEntriesOf(ns1Props);
  }

  @ParameterizedTest
  @MethodSource("catalogFormats")
  public void testNamespaceTransaction(CatalogFormat<?, ?> format) {
    final Map<String, String> ns1PropsInit = Collections.singletonMap("key0", "value0");
    CatalogFile catalogFile =
        format
            .empty(nullFile)
            .createNamespace(NS1, ns1PropsInit)
            .createNamespace(NS2, Collections.emptyMap())
            .createTable(TBL1, "gs://bucket/path/to/table1")
            .createTable(TBL2, "gs://bucket/path/to/table2")
            .commit(fileIO); // ignored; just passing info between CatalogFile

    checkNamespaces(catalogFile, Namespace.empty(), NS1, NS2);
    assertThat(catalogFile.namespaceProperties(NS1)).containsExactlyEntriesOf(ns1PropsInit);

    final Map<String, String> ns1Props = Collections.singletonMap("key1", "value1");
    CatalogFile updateProp =
        format
            .from(catalogFile)
            .updateProperties(NS1, Collections.singletonMap("key0", null)) // remove prop
            .updateProperties(NS1, ns1Props) // add prop, separate actoin
            .updateTable(TBL2, "gs://bucket/path/to/table2.1")
            .createTable(TBL3, "gs://bucket/path/to/table3") // add table
            .createNamespace(NS4, Collections.emptyMap()) // empty namespace
            .createTable(TBL5, "gs://bucket/path/to/table5") // add in root namespace
            .createNamespace(NS3, Collections.emptyMap()) // add namespace
            .createTable(TBL4, "gs://bucket/path/to/table4") // add tbl w/ namespace
            .commit(fileIO);
    assertThat(updateProp).isNotEqualTo(catalogFile);
    checkNamespaces(updateProp, Namespace.empty(), NS1, NS2, NS3, NS4);
    assertThat(updateProp.namespaceProperties(NS1)).containsExactlyEntriesOf(ns1Props);
    assertThat(updateProp.tables()).containsExactlyInAnyOrder(TBL1, TBL2, TBL3, TBL4, TBL5);

    assertThatThrownBy(() -> format.from(updateProp).dropNamespace(NS2).commit(fileIO))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot drop non-empty namespace");
    if (updateProp.createsHierarchicalNamespaces()) {
      // fail to drop parent before child
      final Namespace NS4parent =
          Namespace.of(Arrays.copyOfRange(NS4.levels(), 0, NS4.length() - 1));
      assertThatThrownBy(() -> format.from(updateProp).dropNamespace(NS4parent).commit(fileIO))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Cannot drop non-empty namespace");
      // drop empty namespace and (after drop) empty parent
      CatalogFile drop =
          format.from(updateProp).dropNamespace(NS4).dropNamespace(NS4parent).commit(fileIO);
      checkNamespaces(drop, Namespace.empty(), NS1, NS2, NS3);
    }
  }

  @ParameterizedTest
  @MethodSource("catalogFormats")
  public void testTableSwap(CatalogFormat<?, ?> format) {
    // TODO: Example transaction we do NOT support
    // TODO: tracking create/delete in CatalogFile.Mut is insufficient to support this
    // TODO: since the same TableIdentifier is both deleted and created in the same transaction
    // TODO: would need to track operations + merge
    assumeTrue(
        Boolean.getBoolean("dingos"),
        "Requires changes in CatalogFile.Mut to support versioned changes");

    CatalogFile catalogFile =
        format
            .empty(nullFile)
            .createNamespace(NS1, Collections.emptyMap())
            .createTable(TBL1, "gs://bucket/path/to/table1")
            .createTable(TBL2, "gs://bucket/path/to/table2")
            .commit(fileIO); // ignored; just passing info between CatalogFile

    CatalogFile swap =
        format
            .from(catalogFile)
            .dropTable(TBL1)
            .dropTable(TBL2)
            .createTable(TBL1, "gs://bucket/path/to/table2")
            .createTable(TBL2, "gs://bucket/path/to/table1")
            .commit(fileIO);
    assertThat(swap).isNotEqualTo(catalogFile);
    checkNamespaces(swap, Namespace.empty(), NS1);
    assertThat(swap.tables()).containsExactlyInAnyOrder(TBL1, TBL2);
    assertThat(swap.location(TBL1)).isEqualTo("gs://bucket/path/to/table2");
    assertThat(swap.location(TBL2)).isEqualTo("gs://bucket/path/to/table1");
  }

  private static void checkNamespaces(CatalogFile catalogFile, Namespace... namespaces) {
    if (catalogFile.createsHierarchicalNamespaces()) {
      assertThat(catalogFile.namespaces()).containsExactlyInAnyOrder(allNamespaces(namespaces));
    } else {
      assertThat(catalogFile.namespaces()).containsExactlyInAnyOrder(namespaces);
    }
  }

  // return ancestors of Namespaces as a set
  static Namespace[] allNamespaces(Namespace... namespaces) {
    Set<Namespace> allAncestors = Sets.newHashSet();
    for (Namespace ns : namespaces) {
      String[] levels = ns.levels();
      for (int i = 0; i <= levels.length; i++) {
        allAncestors.add(Namespace.of(Arrays.copyOfRange(levels, 0, i)));
      }
    }
    return allAncestors.toArray(new Namespace[0]);
  }
}
