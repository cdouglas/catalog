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
package org.apache.iceberg.io.spec;

import static org.assertj.core.api.Assertions.assertThat;

import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.From;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CatalogFile;
import org.apache.iceberg.io.ProtoCatalogFile;
import org.apache.iceberg.io.ProtoCatalogFormatTestSupport;

/**
 * Codec-level properties of the checkpoint encoding.
 *
 * <p>P1 — Codec roundtrip: {@code decode(encode(s)) ≡ s}.
 * <br>P2 — Encode determinism: {@code encode(s) == encode(s)} byte-equal.
 *
 * <p>Equality note: {@link CatalogFile} does not override {@code equals()}, so P1
 * compares structural fields directly: catalog UUID, namespace set, table list,
 * namespace properties, and table metadata locations. The file-path ({@code location()})
 * is intentionally excluded — it changes after encode/decode because the decoded
 * file is bound to a fresh stub location.
 *
 * <p>Generated states contain pointer-mode tables only, so neither property
 * exercises the inline-metadata codecs yet.
 */
class TestCodecProperties {

  /**
   * P1 — Codec roundtrip. Encodes the catalog state to full LCAT wire bytes
   * (header + checkpoint) and decodes it back, asserting that the logical state
   * is preserved.
   */
  @Property
  void p1_codecRoundtrip(@ForAll @From("catalogStates") CatalogFile cat) {
    byte[] bytes = ProtoCatalogFormatTestSupport.encodeFullFile((ProtoCatalogFile) cat);
    CatalogFile decoded = ProtoCatalogFormatTestSupport.decodeFullFile(bytes);
    assertCatalogEquals(cat, decoded);
  }

  /**
   * P2 — Encode determinism.
   *
   * <p>Calls {@code encodeFullFile} twice on the same {@link ProtoCatalogFile}
   * instance and asserts that the produced wire bytes are bit-for-bit identical.
   * This would fail if the encoder iterated over a collection whose traversal
   * order can differ across calls. It does not check determinism across JVMs or
   * across independently built but equal states.
   */
  @Property
  void p2_encodeIsDeterministic(@ForAll @From("catalogStates") CatalogFile cat) {
    byte[] a = ProtoCatalogFormatTestSupport.encodeFullFile((ProtoCatalogFile) cat);
    byte[] b = ProtoCatalogFormatTestSupport.encodeFullFile((ProtoCatalogFile) cat);
    assertThat(a).isEqualTo(b);
  }

  @Provide
  Arbitrary<CatalogFile> catalogStates() {
    return CatalogStateGenerators.catalogStates();
  }

  /**
   * Structural equality for {@link CatalogFile}: compares UUID, namespace set,
   * table list, namespace properties, and pointer-table metadata locations.
   *
   * <p>The file-path ({@link CatalogFile#location()}) is excluded: after
   * encode/decode the decoded file is bound to a fresh stub location, while the
   * original is bound to the generator's stub. This is intentional and does not
   * represent a semantic difference.
   */
  private static void assertCatalogEquals(CatalogFile expected, CatalogFile actual) {
    assertThat(actual.uuid())
        .as("catalog UUID")
        .isEqualTo(expected.uuid());

    assertThat(actual.namespaces())
        .as("namespace set")
        .containsExactlyInAnyOrderElementsOf(expected.namespaces());

    assertThat(actual.tables())
        .as("table list")
        .containsExactlyInAnyOrderElementsOf(expected.tables());

    for (Namespace ns : expected.namespaces()) {
      assertThat(actual.namespaceProperties(ns))
          .as("namespace properties for " + ns)
          .isEqualTo(expected.namespaceProperties(ns));
    }

    for (TableIdentifier tbl : expected.tables()) {
      // Pointer-mode tables: location must round-trip exactly.
      assertThat(actual.location(tbl))
          .as("metadata location for " + tbl)
          .isEqualTo(expected.location(tbl));
    }
  }
}
