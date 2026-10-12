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
import org.apache.iceberg.io.InlineMetadataCodecs;
import org.apache.iceberg.io.ProtoCatalogFile;
import org.apache.iceberg.io.ProtoCatalogFormatTestSupport;

/**
 * Codec-level properties for V1b.
 *
 * <p>P1 — Codec roundtrip: {@code decode(encode(s)) ≡ s} for every codec.
 * <br>P2 — Encode determinism: {@code encode(s) == encode(s)} byte-equal.
 * <br>P3 — Cross-codec equivalence: structural ≡ gzip under decode-after-encode.
 *
 * <p>Equality note: {@link CatalogFile} does not override {@code equals()}, so P1
 * compares structural fields directly: catalog UUID, namespace set, table list,
 * namespace properties, and table metadata locations. The file-path ({@code location()})
 * is intentionally excluded — it changes after encode/decode because the decoded
 * file is bound to a fresh stub location.
 */
class CodecProperties {

  /**
   * P1 — Structural codec roundtrip.
   *
   * <p>Encodes the catalog state to full LCAT wire bytes (header + checkpoint)
   * and decodes it back, asserting that the logical state is preserved.
   *
   * <p>In Task 4, catalog states contain only pointer-mode tables, so the
   * structural vs gzip distinction does not affect the checkpoint encoding —
   * both variants exercise the same code path. The two properties are kept
   * separate so that Task 5+ can extend the generators to pass a codec tag
   * and make the distinction meaningful.
   */
  @Property
  void p1_structuralCodecRoundtrip(@ForAll @From("catalogStates") CatalogFile cat) {
    // Adaptation: ProtoCatalogFormat has no static encode/decode pair.
    // ProtoCatalogFormatTestSupport (test-only, in org.apache.iceberg.io) exposes
    // the package-private encodeCheckpoint + readInternal pair as a public bridge.
    byte[] bytes = ProtoCatalogFormatTestSupport.encodeFullFile((ProtoCatalogFile) cat);
    CatalogFile decoded = ProtoCatalogFormatTestSupport.decodeFullFile(bytes);
    assertCatalogEquals(cat, decoded);
  }

  /**
   * P1 — Gzip codec roundtrip.
   *
   * <p>Same roundtrip as {@link #p1_structuralCodecRoundtrip}. In Task 4 the
   * two properties are equivalent because the generated states contain only
   * pointer-mode tables (no inline metadata). When inline-table generators are
   * added in later tasks, the codec tag passed to {@code createTableInline} will
   * differ between the two properties.
   */
  @Property
  void p1_gzipCodecRoundtrip(@ForAll @From("catalogStates") CatalogFile cat) {
    byte[] bytes = ProtoCatalogFormatTestSupport.encodeFullFile((ProtoCatalogFile) cat);
    CatalogFile decoded = ProtoCatalogFormatTestSupport.decodeFullFile(bytes);
    assertCatalogEquals(cat, decoded);
  }

  /**
   * P2 — Encode determinism (invariant I4).
   *
   * <p>Calls {@code encodeFullFile} twice on the same {@link ProtoCatalogFile}
   * instance and asserts that the produced wire bytes are bit-for-bit identical.
   *
   * <p>This property would fail if the encoder iterated over any unordered
   * collection (e.g., a raw {@link java.util.HashMap}) whose traversal order
   * can differ across calls.  The production encoder uses Guava
   * {@code ImmutableMap}, which preserves insertion order, so encoding the
   * same object twice must be deterministic within a single JVM run.
   *
   * <p>If this property reports a counterexample the encoder has a real
   * nondeterminism defect (invariant I4 broken) — do NOT weaken the
   * assertion; fix the encoder.
   */
  @Property
  void p2_encodeIsDeterministic(@ForAll @From("catalogStates") CatalogFile cat) {
    byte[] a = ProtoCatalogFormatTestSupport.encodeFullFile((ProtoCatalogFile) cat);
    byte[] b = ProtoCatalogFormatTestSupport.encodeFullFile((ProtoCatalogFile) cat);
    assertThat(a).isEqualTo(b);
  }

  /**
   * P3 — Cross-codec equivalence.
   *
   * <p>Encodes the same catalog state twice — once with the
   * {@link InlineMetadataCodecs#TAG_STRUCTURAL structural} codec tag and once
   * with the {@link InlineMetadataCodecs#TAG_JSON_GZIP gzip} codec tag — then
   * decodes both and asserts that the decoded logical states are equivalent.
   *
   * <p><b>Current limitation (pointer-mode states):</b> the generator
   * produces only pointer-mode tables (no inline metadata), so the two codec
   * tags have no observable effect on the checkpoint bytes — the inline-table
   * codec field is never written, and both encode calls produce identical
   * bytes.  The property still passes: the round-trip equality holds
   * trivially for both paths.  Once inline-table generators are added in a
   * later task, the two encodings will differ in the inline-metadata bytes
   * and the property will become non-trivially meaningful.
   *
   * <p>The mechanism ({@link ProtoCatalogFormatTestSupport#encodeFullFile(
   * ProtoCatalogFile, byte)}) validates the codec tag against
   * {@link InlineMetadataCodecs#byTag} so the two code paths are genuinely
   * distinct; the equivalence assertion is not vacuous even now.
   */
  @Property
  void p3_crossCodecEquivalence(@ForAll @From("catalogStates") CatalogFile cat) {
    ProtoCatalogFile pcat = (ProtoCatalogFile) cat;
    byte[] structural =
        ProtoCatalogFormatTestSupport.encodeFullFile(pcat, InlineMetadataCodecs.TAG_STRUCTURAL);
    byte[] gzip =
        ProtoCatalogFormatTestSupport.encodeFullFile(pcat, InlineMetadataCodecs.TAG_JSON_GZIP);
    CatalogFile fromStructural = ProtoCatalogFormatTestSupport.decodeFullFile(structural);
    CatalogFile fromGzip = ProtoCatalogFormatTestSupport.decodeFullFile(gzip);
    // Use the hand-rolled comparison: CatalogFile has no equals() override.
    assertCatalogEquals(fromStructural, fromGzip);
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
