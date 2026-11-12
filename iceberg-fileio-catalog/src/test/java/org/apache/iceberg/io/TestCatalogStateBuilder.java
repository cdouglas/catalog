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
import static org.assertj.core.api.Assertions.entry;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;

/**
 * A builder that continues a state (a fold's, a commit's) changes it record by record without
 * rebuilding what the record did not change (errata D25).
 */
class TestCatalogStateBuilder {
  private static final Namespace A = Namespace.of("a");
  private static final Namespace AB = Namespace.of("a", "b");
  private static final Namespace C = Namespace.of("c");

  /** Namespaces a (properties k and gone), a.b and c; tables a.t1, a.b.t2 and c.t3. */
  private static CatalogState sample() {
    return CatalogState.builder()
        .addNamespace(1, 0, "a", 1)
        .addNamespace(2, 1, "b", 1)
        .addNamespace(3, 0, "c", 1)
        .setNamespaceProperty(1, "k", "v")
        .setNamespaceProperty(1, "gone", "x")
        .addTable(1, 1, "t1", 1, "s3://w/a/t1/1.metadata.json")
        .addTable(2, 2, "t2", 1, "s3://w/a/b/t2/1.metadata.json")
        .addTable(3, 3, "t3", 1, "s3://w/c/t3/1.metadata.json")
        .build();
  }

  private static byte[] bytes(CatalogState state) {
    return ProtoCodec.encodeCheckpointState(state);
  }

  /**
   * The lookups a builder keeps through each change are those its entries make: the tables and
   * namespaces of the state it builds are found by name. (Under the replay check, which surefire
   * sets, every build that keeps them also compares them with a rebuild.)
   */
  @Test
  void aBuilderKeepsItsLookupsThroughEveryChange() {
    CatalogState.Builder builder = sample().toBuilder();
    builder.addTable(4, 2, "t4", 1, "s3://w/a/b/t4/1.metadata.json");
    builder.renameTable(1, 3, "t1x", 2);
    builder.removeTable(3);
    builder.updateTableLocation(2, 2, "s3://w/a/b/t2/2.metadata.json");
    builder.addInlineTable(5, 1, "i5", 1, new byte[] {1}, "s3://w/a/i5/metadata");
    CatalogState changed = builder.build();

    assertThat(changed.tables())
        .containsExactlyInAnyOrder(
            TableIdentifier.of(AB, "t2"), TableIdentifier.of(AB, "t4"),
            TableIdentifier.of(C, "t1x"), TableIdentifier.of(A, "i5"));
    assertThat(changed.location(TableIdentifier.of(AB, "t2")))
        .isEqualTo("s3://w/a/b/t2/2.metadata.json");
    assertThat(changed.location(TableIdentifier.of(C, "t1x")))
        .isEqualTo("s3://w/a/t1/1.metadata.json");

    // a namespace added, and a table in it: the builder rebuilds its lookups
    CatalogState.Builder adding = changed.toBuilder();
    adding.addNamespace(4, 3, "d", 1);
    adding.addTable(6, 4, "t6", 1, "s3://w/c/d/t6/1.metadata.json");
    CatalogState added = adding.build();
    assertThat(added.containsNamespace(Namespace.of("c", "d"))).isTrue();
    assertThat(added.containsTable(TableIdentifier.of(Namespace.of("c", "d"), "t6"))).isTrue();

    // namespaces removed parent first, so the child's path is gone when it goes
    CatalogState.Builder removing = changed.toBuilder();
    removing.removeTable(2).removeTable(4).removeTable(5).removeTable(1);
    removing.removeNamespace(1).removeNamespace(2);
    CatalogState removed = removing.build();
    assertThat(removed.namespaces()).containsExactly(C);
    assertThat(removed.tables()).isEmpty();

    // and child first
    CatalogState.Builder childFirst = changed.toBuilder();
    childFirst.removeTable(2).removeTable(4).removeNamespace(2);
    CatalogState withoutChild = childFirst.build();
    assertThat(withoutChild.namespaces()).containsExactlyInAnyOrder(A, C);
    assertThat(withoutChild.tables())
        .containsExactlyInAnyOrder(TableIdentifier.of(C, "t1x"), TableIdentifier.of(A, "i5"));
  }

  /**
   * A builder shares the maps of the state it continues until it changes them: a state it builds
   * holds the very maps a change left alone, so a fold of a record copies only what the record
   * changed.
   */
  @Test
  void aStateBuiltAfterAChangeSharesWhatTheChangeLeftAlone() {
    CatalogState base = sample();
    CatalogState.Builder builder = base.toBuilder();
    builder.updateTableLocation(2, 2, "s3://w/a/b/t2/2.metadata.json");
    CatalogState changed = builder.build();

    assertThat(changed.tableById()).isNotSameAs(base.tableById());
    assertThat(changed.namespaceById()).isSameAs(base.namespaceById());
    assertThat(changed.allNamespaceProperties()).isSameAs(base.allNamespaceProperties());
    assertThat(changed.allInlineTables()).isSameAs(base.allInlineTables());
    assertThat(changed.allManifestPools()).isSameAs(base.allManifestPools());
    assertThat(changed.conflictStates()).isSameAs(base.conflictStates());
  }

  /**
   * A builder's changes never reach a state it shares maps with: not the state it continues, not
   * one it built before a later change, nor one a builder continuing it shares.
   */
  @Test
  void aBuildersChangesLeaveEveryStateItSharesWithAsItWas() {
    CatalogState base = sample();
    byte[] baseBytes = bytes(base);

    CatalogState.Builder builder = base.toBuilder();
    builder.updateTableLocation(2, 2, "s3://w/a/b/t2/2.metadata.json");
    CatalogState first = builder.build();
    byte[] firstBytes = bytes(first);

    // the same builder, changed again after it built
    builder.updateTableLocation(2, 3, "s3://w/a/b/t2/3.metadata.json");
    builder.setNamespaceProperty(1, "k", "changed");
    builder.removeNamespaceProperty(1, "gone");
    builder.addTable(4, 3, "t4", 1, "s3://w/c/t4/1.metadata.json");
    builder.removeTable(1);
    builder.addNamespace(4, 3, "d", 1);
    CatalogState second = builder.build();

    assertThat(bytes(base)).isEqualTo(baseBytes);
    assertThat(bytes(first)).isEqualTo(firstBytes);
    assertThat(base.namespaceProperties(A)).containsOnly(entry("k", "v"), entry("gone", "x"));
    assertThat(second.namespaceProperties(A)).containsOnly(entry("k", "changed"));
    assertThat(second.location(TableIdentifier.of(AB, "t2")))
        .isEqualTo("s3://w/a/b/t2/3.metadata.json");
    assertThat(second.containsTable(TableIdentifier.of(C, "t4"))).isTrue();
    assertThat(second.containsTable(TableIdentifier.of(A, "t1"))).isFalse();
    assertThat(first.containsTable(TableIdentifier.of(A, "t1"))).isTrue();

    // a builder continuing the second state changes none of it either
    byte[] secondBytes = bytes(second);
    CatalogState.Builder next = second.toBuilder();
    next.setNamespaceProperty(1, "k", "again");
    next.setNamespaceProperty(3, "new", "y");
    next.updateTableLocation(2, 4, "s3://w/a/b/t2/4.metadata.json");
    next.removeTable(4);
    next.removeNamespace(4);
    next.build();
    assertThat(bytes(second)).isEqualTo(secondBytes);
    assertThat(bytes(first)).isEqualTo(firstBytes);
    assertThat(bytes(base)).isEqualTo(baseBytes);
  }

  /** A state's maps are read only, as its builder hands them over. */
  @Test
  void aStatesMapsAreReadOnly() {
    CatalogState.Builder builder = sample().toBuilder();
    builder.updateTableLocation(2, 2, "s3://w/a/b/t2/2.metadata.json");
    builder.setNamespaceProperty(1, "k", "changed");
    CatalogState state = builder.build();

    assertThatThrownBy(() -> state.tableById().remove(2))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> state.namespaceProperties(A).put("k", "x"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> state.namespaceById().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
