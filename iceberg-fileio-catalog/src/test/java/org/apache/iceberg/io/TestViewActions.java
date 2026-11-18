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

import static org.apache.iceberg.io.TestProtoActions.apply;
import static org.apache.iceberg.io.TestProtoActions.catalog;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.io.CatalogState.ViewMetadataRef;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;
import org.intentlog.Replica;
import org.intentlog.wire.UuidV7;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * The view actions (docs/VIEWS.md, catalog.proto CreateView, UpdateView, DropView, RenameView):
 * what each requires where its record lands and what it changes, the one set of names tables and
 * views share, the codec, and the staging of views on a {@link CatalogMutation}.
 */
public class TestViewActions {
  private static final Namespace DB = Namespace.of("db");
  private static final TableIdentifier V = TableIdentifier.of(DB, "v");

  static ViewMetadata viewMetadata(String location, String sql) {
    Schema schema = new Schema(Types.NestedField.optional(1, "x", Types.LongType.get()));
    return ViewMetadata.builder()
        .assignUUID("01900000-0000-7000-8000-00000000beef")
        .setLocation(location)
        .setCurrentVersion(
            ImmutableViewVersion.builder()
                .versionId(1)
                .schemaId(schema.schemaId())
                .timestampMillis(1_700_000_000_000L)
                .defaultNamespace(DB)
                .addRepresentations(
                    ImmutableSQLViewRepresentation.builder().dialect("spark").sql(sql).build())
                .build(),
            schema)
        .build();
  }

  static ViewMetadataRef inline(String sql) {
    return InlineViewMetadata.encode(viewMetadata("s3://bucket/db/v", sql));
  }

  static ViewMetadataRef pointer(String location) {
    return ViewMetadataRef.location(location);
  }

  static ProtoCodec.Transaction txn(ProtoCodec.Action... actions) {
    return new ProtoCodec.Transaction(UuidV7.newUuidV7(), List.of(actions));
  }

  /** A view create by name, its namespace's removals as read: the default create condition. */
  static ProtoCodec.CreateViewAction createView(
      int lateId, int nsId, int removals, String name, ViewMetadataRef metadata) {
    return new ProtoCodec.CreateViewAction(lateId, 1, nsId, true, removals, name, metadata);
  }

  static ProtoCodec.CreateTableAction createTable(int lateId, int nsId, int removals, String name) {
    return new ProtoCodec.CreateTableAction(
        lateId, 1, nsId, true, removals, name, "s3://bucket/" + name + "/v1.metadata.json");
  }

  static boolean landed(Replica<CatalogState> result, ProtoCodec.Transaction txn) {
    return result.committed().contains(txn.id());
  }

  @Nested
  class Creates {
    @Test
    void anInlineViewIsCreatedWithItsMetadata() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      ViewMetadataRef metadata = inline("SELECT 1");
      ProtoCodec.Transaction t = txn(createView(-1, 1, 0, "v", metadata));
      CatalogState state = apply(file, t).state();

      assertThat(state.containsView(V)).isTrue();
      assertThat(state.containsTable(V)).isFalse();
      CatalogState.ViewEntry entry = state.viewEntry(V);
      assertThat(entry.isInline()).isTrue();
      assertThat(entry.inlineMetadata()).isEqualTo(metadata.inline());
      assertThat(entry.version).isEqualTo(1);
      assertThat(state.viewId(V)).isEqualTo(1);
      assertThat(state.nextViewId()).isEqualTo(2);
      // a child of the namespace: its version moves
      assertThat(state.namespaceVersion(1)).isEqualTo(2);
      ViewMetadata decoded = InlineViewMetadata.decode(entry.inlineMetadata(), entry.codec, "x");
      assertThat(((org.apache.iceberg.view.SQLViewRepresentation)
              decoded.currentVersion().representations().get(0)).sql())
          .isEqualTo("SELECT 1");
    }

    @Test
    void aPointerViewIsCreatedWithItsLocation() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      CatalogState state =
          apply(file, txn(createView(-1, 1, 0, "v", pointer("s3://bucket/db/v/m1.json")))).state();

      assertThat(state.viewEntry(V).isInline()).isFalse();
      assertThat(state.viewEntry(V).metadataLocation).isEqualTo("s3://bucket/db/v/m1.json");
    }

    @Test
    void viewIdsAreTheirOwnAndLateBound() {
      byte[] file = catalog().ns(1, 0, "db", 1).tbl(1, 1, "t", 1, "s3://t").build();
      ProtoCodec.Transaction t = txn(
          createTable(-1, 1, 0, "u"),
          createView(-1, 1, 0, "v", inline("SELECT 1")),
          createView(-2, 1, 0, "w", inline("SELECT 2")));
      CatalogState state = apply(file, t).state();

      assertThat(state.tableId(TableIdentifier.of(DB, "u"))).isEqualTo(2);
      assertThat(state.viewId(V)).isEqualTo(1);
      assertThat(state.viewId(TableIdentifier.of(DB, "w"))).isEqualTo(2);
    }

    @Test
    void aCreateNamingAConcreteIdIsRejected() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      ProtoCodec.Transaction t = txn(createView(1, 1, 0, "v", inline("SELECT 1")));
      Replica<CatalogState> result = apply(file, t);

      assertThat(landed(result, t)).isFalse();
      assertThat(result.state().containsView(V)).isFalse();
    }

    @Test
    void aViewIsCreatedInANamespaceItsTransactionCreates() {
      byte[] file = catalog().build();
      ProtoCodec.Transaction t = txn(
          new ProtoCodec.CreateNamespaceAction(-1, 1, 0, true, 0, "db"),
          createView(-1, -1, -1, "v", inline("SELECT 1")));
      CatalogState state = apply(file, t).state();

      assertThat(state.containsView(V)).isTrue();
    }

    @Test
    void aViewRefusesTheStructuralCodecAndMalformedMetadata() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      ViewMetadataRef structural =
          ViewMetadataRef.inline(inline("SELECT 1").inline(), InlineMetadataCodecs.TAG_STRUCTURAL);
      ProtoCodec.Transaction a = txn(createView(-1, 1, 0, "v", structural));
      ProtoCodec.Transaction b =
          txn(createView(-1, 1, 0, "v", new ViewMetadataRef(null, null, (byte) 0)));
      Replica<CatalogState> result = apply(file, a, b);

      assertThat(landed(result, a)).isFalse();
      assertThat(landed(result, b)).isFalse();
      assertThat(result.state().containsView(V)).isFalse();
    }

    @Test
    void aCreateByVersionFailsOnceTheNamespaceMoved() {
      byte[] file = catalog().ns(1, 0, "db", 3).build();
      ProtoCodec.Transaction t = txn(
          new ProtoCodec.CreateViewAction(-1, 1, 1, false, 2, "v", inline("SELECT 1")));

      assertThat(landed(apply(file, t), t)).isFalse();
    }
  }

  /** Tables and views share one set of names (V7), and a name is freed only by removals (S2). */
  @Nested
  class SharedNames {
    @Test
    void aViewCreateFailsOnATablesName() {
      byte[] file = catalog().ns(1, 0, "db", 1).tbl(1, 1, "v", 1, "s3://t").build();
      ProtoCodec.Transaction t = txn(createView(-1, 1, 0, "v", inline("SELECT 1")));
      Replica<CatalogState> result = apply(file, t);

      assertThat(landed(result, t)).isFalse();
      assertThat(t.intention().explain(result.state().toBuilder())).isEqualTo("name: taken");
    }

    @Test
    void aTableCreateFailsOnAViewsName() {
      byte[] file = catalog().ns(1, 0, "db", 1).view(1, 1, "v", 1, inline("SELECT 1")).build();
      ProtoCodec.Transaction pointer = txn(createTable(-1, 1, 0, "v"));
      ProtoCodec.Transaction inlined = txn(new ProtoCodec.CreateTableInlineAction(
          -1, 1, 1, true, 0, "v", new byte[] {1}, InlineMetadataCodecs.TAG_JSON_GZIP, null));
      Replica<CatalogState> result = apply(file, pointer, inlined);

      assertThat(landed(result, pointer)).isFalse();
      assertThat(landed(result, inlined)).isFalse();
      assertThat(result.state().containsTable(V)).isFalse();
    }

    @Test
    void renamesFailOntoTheOtherKindsName() {
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "t", 1, "s3://t")
          .view(1, 1, "v", 1, inline("SELECT 1"))
          .build();
      ProtoCodec.Transaction table = txn(new ProtoCodec.RenameTableAction(1, 1, 1, true, 0, "v"));
      ProtoCodec.Transaction view = txn(new ProtoCodec.RenameViewAction(1, 1, 1, true, 0, "t"));
      Replica<CatalogState> result = apply(file, table, view);

      assertThat(landed(result, table)).isFalse();
      assertThat(landed(result, view)).isFalse();
    }

    @Test
    void aTableCreateThatFoundAViewStaysRejectedOnceTheViewIsDropped() {
      // the cross-kind create ABA: a table create by name that lost to a view of its name must not
      // bind it after the view's drop frees it again
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      ProtoCodec.Transaction view = txn(createView(-1, 1, 0, "v", inline("SELECT 1")));
      ProtoCodec.Transaction table = txn(createTable(-1, 1, 0, "v"));
      ProtoCodec.Transaction drop = txn(new ProtoCodec.DropViewAction(1, 1));
      Replica<CatalogState> result = apply(file, view, table, drop, table);

      assertThat(landed(result, view)).isTrue();
      assertThat(landed(result, drop)).isTrue();
      assertThat(landed(result, table)).isFalse();
      assertThat(result.state().containsTable(V)).isFalse();
      assertThat(result.state().namespaceRemovals(1)).isEqualTo(1);
    }

    @Test
    void aViewCreateThatFoundATableStaysRejectedOnceTheTableIsRenamedAway() {
      byte[] file = catalog().ns(1, 0, "db", 1).tbl(1, 1, "v", 1, "s3://t").build();
      ProtoCodec.Transaction view = txn(createView(-1, 1, 0, "v", inline("SELECT 1")));
      ProtoCodec.Transaction rename = txn(new ProtoCodec.RenameTableAction(1, 1, 1, true, 0, "t"));
      Replica<CatalogState> result = apply(file, view, rename, view);

      assertThat(landed(result, rename)).isTrue();
      assertThat(landed(result, view)).isFalse();
    }
  }

  @Nested
  class Updates {
    @Test
    void anUpdateReplacesTheWholeMetadataAndMayChangeForm() {
      byte[] file = catalog().ns(1, 0, "db", 1).view(1, 1, "v", 1, inline("SELECT 1")).build();
      ProtoCodec.Transaction toPointer =
          txn(new ProtoCodec.UpdateViewAction(1, 1, pointer("s3://bucket/db/v/m2.json")));
      ViewMetadataRef again = inline("SELECT 3");
      ProtoCodec.Transaction toInline = txn(new ProtoCodec.UpdateViewAction(1, 2, again));
      Replica<CatalogState> pointerResult = apply(file, toPointer);
      Replica<CatalogState> inlineResult = apply(file, toPointer, toInline);

      assertThat(pointerResult.state().viewEntry(V).metadataLocation)
          .isEqualTo("s3://bucket/db/v/m2.json");
      assertThat(pointerResult.state().viewEntry(V).isInline()).isFalse();
      assertThat(inlineResult.state().viewEntry(V).inlineMetadata()).isEqualTo(again.inline());
      assertThat(inlineResult.state().viewVersion(1)).isEqualTo(3);
      // a view's update leaves its namespace alone
      assertThat(inlineResult.state().namespaceVersion(1)).isEqualTo(1);
    }

    @Test
    void anUpdateAppliesOnlyOnTheVersionItRead() {
      byte[] file = catalog().ns(1, 0, "db", 1).view(1, 1, "v", 1, inline("SELECT 1")).build();
      ProtoCodec.Transaction first = txn(new ProtoCodec.UpdateViewAction(1, 1, inline("SELECT 2")));
      ProtoCodec.Transaction second = txn(new ProtoCodec.UpdateViewAction(1, 1, inline("SELECT 3")));
      Replica<CatalogState> result = apply(file, first, second);

      assertThat(landed(result, first)).isTrue();
      assertThat(landed(result, second)).isFalse();
      assertThat(second.intention().explain(result.state().toBuilder())).isEqualTo("view: version");
    }

    @Test
    void anUpdateOfADroppedViewFails() {
      byte[] file = catalog().ns(1, 0, "db", 1).view(1, 1, "v", 1, inline("SELECT 1")).build();
      ProtoCodec.Transaction drop = txn(new ProtoCodec.DropViewAction(1, 1));
      ProtoCodec.Transaction update = txn(new ProtoCodec.UpdateViewAction(1, 1, inline("SELECT 2")));

      assertThat(landed(apply(file, drop, update), update)).isFalse();
    }
  }

  @Nested
  class DropsAndRenames {
    @Test
    void aDropFreesTheNameAndCountsIt() {
      byte[] file = catalog().ns(1, 0, "db", 1).view(1, 1, "v", 1, inline("SELECT 1")).build();
      ProtoCodec.Transaction drop = txn(new ProtoCodec.DropViewAction(1, 1));
      CatalogState state = apply(file, drop).state();

      assertThat(state.containsView(V)).isFalse();
      assertThat(state.namespaceVersion(1)).isEqualTo(2);
      assertThat(state.namespaceRemovals(1)).isEqualTo(1);
    }

    @Test
    void aStaleDropFails() {
      byte[] file = catalog().ns(1, 0, "db", 1).view(1, 1, "v", 2, inline("SELECT 1")).build();
      ProtoCodec.Transaction drop = txn(new ProtoCodec.DropViewAction(1, 1));

      assertThat(landed(apply(file, drop), drop)).isFalse();
    }

    @Test
    void aRenameAcrossNamespacesKeepsTheIdAndMetadata() {
      ViewMetadataRef metadata = inline("SELECT 1");
      byte[] file = catalog()
          .ns(1, 0, "db", 1)
          .ns(2, 0, "archive", 1)
          .view(1, 1, "v", 1, metadata)
          .build();
      CatalogState state =
          apply(file, txn(new ProtoCodec.RenameViewAction(1, 1, 2, true, 0, "old_v"))).state();

      TableIdentifier moved = TableIdentifier.of(Namespace.of("archive"), "old_v");
      assertThat(state.containsView(V)).isFalse();
      assertThat(state.viewId(moved)).isEqualTo(1);
      assertThat(state.viewEntry(moved).inlineMetadata()).isEqualTo(metadata.inline());
      assertThat(state.viewVersion(1)).isEqualTo(2);
      assertThat(state.namespaceVersion(1)).isEqualTo(2);
      assertThat(state.namespaceRemovals(1)).isEqualTo(1);
      assertThat(state.namespaceVersion(2)).isEqualTo(2);
      assertThat(state.namespaceRemovals(2)).isEqualTo(0);
    }

    @Test
    void aTransactionThatDropsAViewCannotUpdateOrRenameIt() {
      byte[] file = catalog().ns(1, 0, "db", 1).view(1, 1, "v", 1, inline("SELECT 1")).build();
      ProtoCodec.Transaction update = txn(
          new ProtoCodec.DropViewAction(1, 1),
          new ProtoCodec.UpdateViewAction(1, 1, inline("SELECT 2")));
      ProtoCodec.Transaction rename = txn(
          new ProtoCodec.DropViewAction(1, 1),
          new ProtoCodec.RenameViewAction(1, 1, 1, true, 0, "w"));
      Replica<CatalogState> result = apply(file, update, rename);

      assertThat(landed(result, update)).isFalse();
      assertThat(landed(result, rename)).isFalse();
      assertThat(result.state().containsView(V)).isTrue();
    }

    @Test
    void aTransactionCannotCreateAViewInANamespaceItDrops() {
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      ProtoCodec.Transaction t = txn(
          createView(-1, 1, 0, "v", inline("SELECT 1")), new ProtoCodec.DropNamespaceAction(1, 1));

      assertThat(landed(apply(file, t), t)).isFalse();
    }

    @Test
    void aNamespaceDropStagedBeforeAViewWasCreatedFails() {
      // V8: the view's create bumps the namespace's version, which the drop names
      byte[] file = catalog().ns(1, 0, "db", 1).build();
      ProtoCodec.Transaction view = txn(createView(-1, 1, 0, "v", inline("SELECT 1")));
      ProtoCodec.Transaction drop = txn(new ProtoCodec.DropNamespaceAction(1, 1));
      Replica<CatalogState> result = apply(file, view, drop);

      assertThat(landed(result, drop)).isFalse();
      assertThat(result.state().containsNamespace(DB)).isTrue();
    }
  }

  @Nested
  class Codec {
    @Test
    void everyViewActionRoundTrips() {
      ViewMetadataRef metadata = inline("SELECT 1");
      List<ProtoCodec.Action> actions = List.of(
          new ProtoCodec.CreateViewAction(-1, 1, 3, true, 7, "v", metadata),
          new ProtoCodec.CreateViewAction(-2, 1, -1, false, -1, "w", pointer("s3://m.json")),
          new ProtoCodec.UpdateViewAction(4, 2, metadata),
          new ProtoCodec.UpdateViewAction(4, 3, pointer("s3://m2.json")),
          new ProtoCodec.DropViewAction(5, 6),
          new ProtoCodec.RenameViewAction(6, 2, 3, true, 7, "x"),
          new ProtoCodec.RenameViewAction(6, 2, 3, false, 9, "y"));
      byte[] bytes = ProtoCodec.encodeIntention(new ProtoCodec.CatalogIntention(actions));
      List<ProtoCodec.Action> decoded = ProtoCodec.decodeIntention(bytes).actions();

      assertThat(ProtoCodec.encodeIntention(new ProtoCodec.CatalogIntention(decoded)))
          .isEqualTo(bytes);
      ProtoCodec.CreateViewAction create = (ProtoCodec.CreateViewAction) decoded.get(0);
      assertThat(create.metadata.inline()).isEqualTo(metadata.inline());
      assertThat(create.byName).isTrue();
      assertThat(create.namespaceRead).isEqualTo(7);
      ProtoCodec.CreateViewAction byVersion = (ProtoCodec.CreateViewAction) decoded.get(1);
      assertThat(byVersion.metadata.location()).isEqualTo("s3://m.json");
      assertThat(byVersion.byName).isFalse();
      assertThat(byVersion.namespaceRead).isEqualTo(-1);
      assertThat(((ProtoCodec.RenameViewAction) decoded.get(6)).newNamespaceRead).isEqualTo(9);
    }

    @Test
    void aCheckpointRoundTripsViewsAndSaysItsReaderVersion() {
      byte[] withViews = catalog()
          .ns(1, 0, "db", 1)
          .view(1, 1, "v", 2, inline("SELECT 1"))
          .view(3, 1, "w", 1, pointer("s3://bucket/db/w/m.json"))
          .build();
      Replica<CatalogState> replica = CatalogTestSupport.replay(withViews);
      byte[] state = ProtoCodec.encodeCheckpointState(replica.state());
      CatalogState.Builder decoded = CatalogState.builder();
      ProtoCodec.decodeCheckpointState(state, decoded);

      assertThat(ProtoCodec.encodeCheckpointState(decoded.build())).isEqualTo(state);
      assertThat(decoded.build().nextViewId()).isEqualTo(4);
      assertThat(decoded.build().viewEntry(TableIdentifier.of(DB, "w")).metadataLocation)
          .isEqualTo("s3://bucket/db/w/m.json");
      // min_reader_version, the checkpoint's first field: 2 with views, 1 without
      assertThat(state[1]).isEqualTo((byte) ProtoCodec.VIEWS_READER_VERSION);
      byte[] without = ProtoCodec.encodeCheckpointState(
          CatalogTestSupport.replay(catalog().ns(1, 0, "db", 1).build()).state());
      assertThat(without[1]).isEqualTo((byte) 1);
    }

    @Test
    void aViewsMetadataDecodesAsIcebergsParserReadsIt() {
      ViewMetadata metadata = viewMetadata("s3://bucket/db/v", "SELECT 42");
      ViewMetadataRef encoded = InlineViewMetadata.encode(metadata);
      ViewMetadata decoded =
          InlineViewMetadata.decode(encoded.inline(), encoded.codec(), "inline-view://db.v#1.1");

      assertThat(ViewMetadataParser.toJson(decoded)).isEqualTo(ViewMetadataParser.toJson(metadata));
      assertThat(decoded.metadataFileLocation()).isEqualTo("inline-view://db.v#1.1");
    }
  }

  /** Staging views on a transaction: the writer's checks, which replay's back. */
  @Nested
  class Staging {
    private Replica<CatalogState> read(byte[] file) {
      return CatalogTestSupport.replay(file);
    }

    @Test
    void namesAreCheckedAcrossKindsWithIcebergsMessages() {
      Replica<CatalogState> read = read(catalog()
          .ns(1, 0, "db", 1)
          .tbl(1, 1, "t", 1, "s3://t")
          .view(1, 1, "v", 1, inline("SELECT 1"))
          .build());
      TableIdentifier t = TableIdentifier.of(DB, "t");

      assertThatThrownBy(() -> new CatalogMutation(read).createView(V, inline("SELECT 2")))
          .isInstanceOf(AlreadyExistsException.class)
          .hasMessage("View already exists: db.v");
      assertThatThrownBy(() -> new CatalogMutation(read).createView(t, inline("SELECT 2")))
          .isInstanceOf(AlreadyExistsException.class)
          .hasMessage("Table with same name already exists: db.t");
      assertThatThrownBy(() -> new CatalogMutation(read).createTable(V, "s3://x"))
          .isInstanceOf(AlreadyExistsException.class)
          .hasMessage("View with same name already exists: db.v");
      assertThatThrownBy(() -> new CatalogMutation(read).renameTable(t, V))
          .isInstanceOf(AlreadyExistsException.class)
          .hasMessage("Cannot rename db.t to db.v. View already exists");
      assertThatThrownBy(() -> new CatalogMutation(read).renameView(V, t))
          .isInstanceOf(AlreadyExistsException.class)
          .hasMessage("Cannot rename db.v to db.t. Table already exists");
      assertThatThrownBy(() -> new CatalogMutation(read).dropView(t))
          .isInstanceOf(NoSuchViewException.class);
    }

    @Test
    void aNamespaceWithAViewIsNotEmpty() {
      Replica<CatalogState> read =
          read(catalog().ns(1, 0, "db", 1).view(1, 1, "v", 1, inline("SELECT 1")).build());

      assertThatThrownBy(() -> new CatalogMutation(read).dropNamespace(DB))
          .isInstanceOf(NamespaceNotEmptyException.class);
      // emptied in the same transaction: the drop goes after the view's
      CatalogMutation emptied = new CatalogMutation(read).dropView(V).dropNamespace(DB);
      Replica<CatalogState> result = apply(
          TestProtoActions.toFileBytes(read),
          new ProtoCodec.Transaction(UuidV7.newUuidV7(), emptied.intention()));
      assertThat(result.state().containsNamespace(DB)).isFalse();
    }

    @Test
    void aMutationsViewsLandAsStaged() {
      Replica<CatalogState> read = read(catalog()
          .ns(1, 0, "db", 1)
          .view(1, 1, "v", 1, inline("SELECT 1"))
          .view(2, 1, "w", 1, inline("SELECT 2"))
          .build());
      ViewMetadataRef replaced = pointer("s3://bucket/db/v/m2.json");
      CatalogMutation mutation = new CatalogMutation(read)
          .createNamespace(Namespace.of("new"))
          .createView(TableIdentifier.of(Namespace.of("new"), "n"), inline("SELECT 3"))
          .updateView(V, replaced)
          .renameView(TableIdentifier.of(DB, "w"), TableIdentifier.of(DB, "w2"));
      Replica<CatalogState> result = apply(
          TestProtoActions.toFileBytes(read),
          new ProtoCodec.Transaction(UuidV7.newUuidV7(), mutation.intention()));

      CatalogState state = result.state();
      assertThat(state.containsView(TableIdentifier.of(Namespace.of("new"), "n"))).isTrue();
      assertThat(state.viewEntry(V).metadataLocation).isEqualTo("s3://bucket/db/v/m2.json");
      assertThat(state.viewId(TableIdentifier.of(DB, "w2"))).isEqualTo(2);
    }

    @Test
    void aLostCreateIsExplainedAsIcebergReportsIt() {
      Replica<CatalogState> read = read(catalog().ns(1, 0, "db", 1).build());
      CatalogMutation view = new CatalogMutation(read).createView(V, inline("SELECT 1"));
      CatalogMutation table = new CatalogMutation(read).createTable(V, "s3://x");
      CatalogState viewLanded = apply(TestProtoActions.toFileBytes(read),
          new ProtoCodec.Transaction(UuidV7.newUuidV7(), view.intention())).state();

      assertThat(table.explain(viewLanded, "lost"))
          .isInstanceOf(AlreadyExistsException.class)
          .hasMessage("View with same name already exists: db.v");
      assertThat(view.explain(viewLanded, "lost"))
          .isInstanceOf(AlreadyExistsException.class)
          .hasMessage("View already exists: db.v");
      assertThat(view.explain(read.state(), "lost")).isInstanceOf(CommitFailedException.class);
    }
  }
}
