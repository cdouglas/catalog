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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.CatalogMutation;
import org.apache.iceberg.io.CatalogTestSupport;
import org.apache.iceberg.io.ProtoCodec;
import org.apache.iceberg.io.CatalogState;

/**
 * jqwik generators for catalog states and actions.
 *
 * <p>Generators are built on {@link Arbitraries#randomValue}, so they produce no
 * edge cases and do not shrink: a failure reports the drawn value as is.
 * States are flat (root-level namespaces only) with pointer-mode tables at
 * version 1, no namespace properties, and no inline tables.
 */
final class CatalogStateGenerators {

  private CatalogStateGenerators() {}

  /** Namespace names: 1–8 lowercase ASCII letters. */
  static Arbitrary<String> namespaceNames() {
    return Arbitraries.strings().withCharRange('a', 'z').ofMinLength(1).ofMaxLength(8);
  }

  /** Table names share the same character range as namespace names. */
  static Arbitrary<String> tableNames() {
    return namespaceNames();
  }

  /**
   * Catalog states: 0–3 namespaces, each with 0–3 pointer-mode tables.
   * Pointer-mode means the table's metadata is referenced by a URI, not inlined.
   *
   * <p>Uses {@link Arbitraries#randomValue} so jqwik treats the stream as
   * inexhaustible and runs all 1000 tries (Arbitraries.create() only runs 1).
   */
  static Arbitrary<CatalogState> catalogStates() {
    return Arbitraries.randomValue(rng -> {
      CatalogState.Builder builder =
          CatalogState.builder();

      // Draw 0–3 namespaces using the PRNG jqwik supplies.
      int nsCount = rng.nextInt(4); // 0, 1, 2, or 3
      int nextNsId = 1;
      int nextTblId = 1;

      // Track generated namespace names to avoid duplicate collisions.
      List<String> generatedNsNames = new ArrayList<>();

      for (int n = 0; n < nsCount; n++) {
        String nsName = randomLowercaseName(rng, 1, 8, generatedNsNames);
        if (nsName == null) {
          // Exhausted unique names for this draw; stop adding namespaces.
          break;
        }
        generatedNsNames.add(nsName);

        int nsId = nextNsId++;
        // parentId = 0: root-level namespace, keeping the tree flat.
        builder.addNamespace(nsId, 0, nsName, 1);

        int tblCount = rng.nextInt(4); // 0, 1, 2, or 3 tables per namespace
        List<String> generatedTblNames = new ArrayList<>();

        for (int t = 0; t < tblCount; t++) {
          String tblName = randomLowercaseName(rng, 1, 8, generatedTblNames);
          if (tblName == null) {
            break;
          }
          generatedTblNames.add(tblName);

          int tblId = nextTblId++;
          // Pointer-mode: metadataLocation is a URI, not null/inline bytes.
          String metadataLocation =
              "test://warehouse/" + nsName + "/" + tblName + "/v1.metadata.json";
          builder.addTable(tblId, nsId, tblName, 1, metadataLocation);
        }
      }

      builder.setNextNamespaceId(nextNsId);
      builder.setNextTableId(nextTblId);
      // rebuildLookups() is called inside build() already, but call it explicitly
      // after bulk-loading entries via addNamespace/addTable.
      return builder.build();
    });
  }

  /**
   * Generates a random lowercase name of length [minLen, maxLen] that does not
   * already appear in {@code existing}. Returns {@code null} if 20 attempts all
   * collide (extremely unlikely with name space >= 26^1 = 26 options).
   */
  private static String randomLowercaseName(
      java.util.Random rng, int minLen, int maxLen, List<String> existing) {
    for (int attempt = 0; attempt < 20; attempt++) {
      int len = minLen + (maxLen > minLen ? rng.nextInt(maxLen - minLen + 1) : 0);
      StringBuilder sb = new StringBuilder(len);
      for (int i = 0; i < len; i++) {
        sb.append((char) ('a' + rng.nextInt(26)));
      }
      String candidate = sb.toString();
      if (!existing.contains(candidate)) {
        return candidate;
      }
    }
    return null; // give up; caller skips this entry
  }

  /**
   * Action generator parameterised on a catalog state.
   *
   * <p>Produces actions that are plausibly applicable to {@code state}:
   * <ul>
   *   <li>{@link ProtoCodec.CreateNamespaceAction} — always applicable (creates
   *       a fresh root-level namespace with a freshly allocated ID).</li>
   *   <li>{@link ProtoCodec.CreateTableAction} — applicable only when at least
   *       one namespace exists; picks one at random.</li>
   *   <li>{@link ProtoCodec.DropTableAction} — applicable only when at least
   *       one pointer-mode table exists; picks one at random.</li>
   *   <li>{@link ProtoCodec.UpdateTableLocationAction} — applicable only when
   *       at least one pointer-mode table exists; picks one at random.</li>
   * </ul>
   *
   * <p>All ID / version values are taken from the current state so
   * {@code action.verify(builder)} succeeds on the first try in the majority
   * of cases.  The property's {@code if (action.verify(...))} guard still
   * handles the minority of races where the generator draws from an empty
   * set or produces a version mismatch.
   *
   * <p>{@code Action.verify} / {@code Action.apply} take a
   * {@link CatalogState.Builder}; property classes build one from the
   * snapshot with {@link SpecUtils#stateToBuilder}.
   *
   * <p>This generator uses only public accessors on {@code CatalogState}:
   * {@code namespaces()}, {@code namespaceId(Namespace)},
   * {@code namespaceVersion(int)}, {@code tables()}, {@code tableId(TableIdentifier)},
   * {@code tableVersion(int)}, {@code tableEntry(int)}, {@code isInlineTable(int)},
   * {@code nextNamespaceId()}, {@code nextTableId()}.
   */
  static Arbitrary<ProtoCodec.Action> actions(CatalogState state) {
    return Arbitraries.randomValue(rng -> action(state, rng));
  }

  /** An action for {@code state}, drawn with {@code rng}; see {@link #actions}. */
  static ProtoCodec.Action action(CatalogState state, java.util.Random rng) {
    {
      if (!(state instanceof CatalogState)) {
        return freshCreateNamespace(rng, state);
      }
      CatalogState pcf = state;

      // Collect ns IDs and versions using public accessors
      List<int[]> nsIdVersions = new ArrayList<>(); // [nsId, nsVersion]
      for (Namespace ns : pcf.namespaces()) {
        Integer nsId = pcf.namespaceId(ns);
        if (nsId != null) {
          nsIdVersions.add(new int[]{nsId, pcf.namespaceVersion(nsId)});
        }
      }

      // Collect pointer-mode table IDs and versions
      List<int[]> ptrTblIdVersions = new ArrayList<>(); // [tblId, tblVersion]
      for (TableIdentifier tbl : pcf.tables()) {
        Integer tblId = pcf.tableId(tbl);
        if (tblId != null && !pcf.isInlineTable(tblId)) {
          ptrTblIdVersions.add(new int[]{tblId, pcf.tableVersion(tblId)});
        }
      }

      // Weight toward actions that are applicable
      int maxKind = 0; // always at least CreateNamespace
      if (!nsIdVersions.isEmpty()) maxKind = 1;
      if (!ptrTblIdVersions.isEmpty()) maxKind = 3;

      int kind = rng.nextInt(maxKind + 1);

      switch (kind) {
        case 0:
          return freshCreateNamespace(rng, state);

        case 1: {
          // CreateTable in a randomly chosen existing namespace
          int[] nsIdVer = nsIdVersions.get(rng.nextInt(nsIdVersions.size()));
          int nsId = nsIdVer[0];
          int nsVersion = nsIdVer[1];
          int newTblId = pcf.nextTableId() + rng.nextInt(100) + 1;
          // A name the namespace does not have: a generated state's versions need not count its
          // entries, so a create of a name in use could verify, which no history allows.
          List<String> inUse = new ArrayList<>();
          for (TableIdentifier tbl : pcf.tables()) {
            Integer tblId = pcf.tableId(tbl);
            if (tblId != null && pcf.tableEntry(tblId).namespaceId == nsId) {
              inUse.add(tbl.name());
            }
          }
          String tblName = randomLowercaseName(rng, 1, 8, inUse);
          if (tblName == null) tblName = "tbl" + newTblId;
          String loc = "test://warehouse/ns" + nsId + "/" + tblName + "/v1.metadata.json";
          return new ProtoCodec.CreateTableAction(newTblId, 1, nsId, nsVersion, tblName, loc);
        }

        case 2: {
          // DropTable on a randomly chosen pointer-mode table
          int[] tblIdVer = ptrTblIdVersions.get(rng.nextInt(ptrTblIdVersions.size()));
          return new ProtoCodec.DropTableAction(tblIdVer[0], tblIdVer[1]);
        }

        default: {
          // UpdateTableLocation on a randomly chosen pointer-mode table
          int[] tblIdVer = ptrTblIdVersions.get(rng.nextInt(ptrTblIdVersions.size()));
          int tblId = tblIdVer[0];
          int tblVersion = tblIdVer[1];
          String newLoc = "test://warehouse/tbl" + tblId + "/v" + (tblVersion + 1) + ".metadata.json";
          return new ProtoCodec.UpdateTableLocationAction(tblId, tblVersion, newLoc);
        }
      }
    }
  }

  /**
   * Helper: creates a {@link ProtoCodec.CreateNamespaceAction} for a fresh
   * root-level namespace, choosing an ID that is one beyond the catalog's
   * current {@code nextNamespaceId}.
   */
  private static ProtoCodec.CreateNamespaceAction freshCreateNamespace(
      java.util.Random rng, CatalogState state) {
    int baseNsId = (state instanceof CatalogState)
        ? (state).nextNamespaceId()
        : 100;
    int newNsId = baseNsId + rng.nextInt(50);
    // A name no top-level namespace has (see the table names in action).
    List<String> inUse = new ArrayList<>();
    for (Namespace ns : state.namespaces()) {
      if (ns.length() == 1) {
        inUse.add(ns.level(0));
      }
    }
    String name = randomLowercaseName(rng, 1, 8, inUse);
    if (name == null) name = "ns" + newNsId;
    return new ProtoCodec.CreateNamespaceAction(newNsId, 1, 0, 0, name);
  }

  // ============================================================
  // Writers' intentions, over a small universe
  // ============================================================

  /** The namespaces a writer names: two levels of two names, so that names collide often. */
  static final List<Namespace> NAMESPACES =
      List.of(
          Namespace.of("a"),
          Namespace.of("b"),
          Namespace.of("a", "a"),
          Namespace.of("a", "b"),
          Namespace.of("b", "a"));

  /** The table names a writer names. */
  static final List<String> TABLE_NAMES = List.of("t", "u");

  private static final List<String> KEYS = List.of("k1", "k2");

  /** What a writer stages: each a change {@link CatalogMutation} stages, or a bare action. */
  enum WriterOp {
    CREATE_NAMESPACE,
    DROP_NAMESPACE,
    SET_PROPERTY,
    REMOVE_PROPERTY,
    CREATE_TABLE,
    CREATE_TABLE_INLINE,
    DROP_TABLE,
    UPDATE_TABLE,
    READ_TABLE,
    UPDATE_INLINE_FULL,
    UPDATE_INLINE_DELTA,
    RENAME_TABLE,
    /** A bare SetConflictEncoding, as a catalog's first record carries. */
    SET_CONFLICT_ENCODING,
    /** A bare SetSnapshotPaging, as a catalog's first record may carry. */
    SET_SNAPSHOT_PAGING,
    /** An action of a type this build does not know. */
    UNKNOWN
  }

  /** The ops a writer's transaction is drawn from, weighted toward names that collide. */
  private static final List<WriterOp> WEIGHTED = weighted();

  private static List<WriterOp> weighted() {
    List<WriterOp> ops = new ArrayList<>();
    for (WriterOp op : WriterOp.values()) {
      int weight;
      switch (op) {
        case CREATE_NAMESPACE:
        case CREATE_TABLE:
        case CREATE_TABLE_INLINE:
        case DROP_TABLE:
        case RENAME_TABLE:
          weight = 4;
          break;
        case DROP_NAMESPACE:
          weight = 3;
          break;
        case SET_CONFLICT_ENCODING:
        case SET_SNAPSHOT_PAGING:
        case UNKNOWN:
          weight = 0; // drawn apart (BARE)
          break;
        default:
          weight = 2;
      }
      for (int i = 0; i < weight; i++) {
        ops.add(op);
      }
    }
    return List.copyOf(ops);
  }

  /** The bare actions, one of which a writer's transaction is now and then. */
  private static final List<WriterOp> BARE =
      List.of(WriterOp.SET_CONFLICT_ENCODING, WriterOp.SET_SNAPSHOT_PAGING, WriterOp.UNKNOWN);

  /**
   * A writer's transaction: its intention, the ops it staged, for a trace, and the names it binds
   * (tables created or renamed to, and namespaces created).
   */
  static final class Staged {
    final ProtoCodec.CatalogIntention intention;
    final List<String> ops;
    final List<TableIdentifier> boundTables = new ArrayList<>();
    final List<Namespace> boundNamespaces = new ArrayList<>();

    Staged(ProtoCodec.CatalogIntention intention, List<String> ops) {
      this.intention = intention;
      this.ops = ops;
    }

    /** Whether one of the names it binds is free on {@code state}, in a namespace that exists. */
    boolean aNameIsFree(CatalogState state) {
      for (TableIdentifier table : boundTables) {
        if (!state.containsTable(table) && state.containsNamespace(table.namespace())) {
          return true;
        }
      }
      for (Namespace ns : boundNamespaces) {
        Namespace parent = ns.length() > 1
            ? Namespace.of(java.util.Arrays.copyOf(ns.levels(), ns.length() - 1))
            : Namespace.empty();
        if (!state.containsNamespace(ns)
            && (parent.isEmpty() || state.containsNamespace(parent))) {
          return true;
        }
      }
      return false;
    }

    @Override
    public String toString() {
      return String.join("; ", ops);
    }
  }

  /**
   * A transaction a writer stages on {@code read} as {@link CatalogMutation} stages it, its names
   * binding by {@code condition} (each op checked against the read, with the versions, and the
   * removals or versions of the namespaces it binds names in, as the read holds them):
   * one to three ops drawn with {@code rng} from {@link #NAMESPACES}, {@link #TABLE_NAMES} and
   * the read's tables, skipping those the read refuses; or, now and then, one bare action of the
   * kinds no catalog operation stages (a conflict encoding, snapshot paging, an unknown action).
   * Null if nothing staged.
   */
  static Staged writerTransaction(
      CatalogState read, Random rng, CatalogMutation.CreateCondition condition) {
    if (rng.nextInt(20) == 0) {
      switch (BARE.get(rng.nextInt(BARE.size()))) {
        case SET_CONFLICT_ENCODING: {
          String encoding =
              CatalogTestSupport.ENCODINGS.get(rng.nextInt(CatalogTestSupport.ENCODINGS.size()));
          return bare(new ProtoCodec.SetConflictEncodingAction(encoding),
              "setConflictEncoding(" + encoding + ")");
        }
        case SET_SNAPSHOT_PAGING:
          return bare(new ProtoCodec.SetSnapshotPagingAction(2, 1), "setSnapshotPaging(2, 1)");
        default:
          return bare(new ProtoCodec.UnknownAction(99, new byte[] {8, 1}), "unknown(99)");
      }
    }
    CatalogMutation mutation =
        new CatalogMutation(CatalogTestSupport.replicaOf(read), condition);
    List<String> staged = new ArrayList<>();
    List<Object> bound = new ArrayList<>();
    Map<TableIdentifier, Boolean> updated = new HashMap<>();
    int ops = 1 + rng.nextInt(3);
    for (int i = 0; i < ops; i++) {
      String done = stage(
          mutation, read, WEIGHTED.get(rng.nextInt(WEIGHTED.size())), rng, updated, bound);
      if (done != null) {
        staged.add(done);
      }
    }
    if (staged.isEmpty()) {
      return null;
    }
    if (condition == CatalogMutation.CreateCondition.NAMESPACE) {
      staged.add(0, "by namespace:");
    }
    Staged transaction = new Staged(mutation.intention(), staged);
    for (Object name : bound) {
      if (name instanceof TableIdentifier) {
        transaction.boundTables.add((TableIdentifier) name);
      } else {
        transaction.boundNamespaces.add((Namespace) name);
      }
    }
    return transaction;
  }

  private static Staged bare(ProtoCodec.Action action, String what) {
    return new Staged(new ProtoCodec.CatalogIntention(List.of(action)), List.of(what));
  }

  /** Stages {@code op} on {@code mutation}; what it staged, or null if the read refuses it. */
  private static String stage(CatalogMutation mutation, CatalogState read, WriterOp op,
      Random rng, Map<TableIdentifier, Boolean> updated, List<Object> bound) {
    Namespace ns = NAMESPACES.get(rng.nextInt(NAMESPACES.size()));
    TableIdentifier fresh =
        TableIdentifier.of(ns, TABLE_NAMES.get(rng.nextInt(TABLE_NAMES.size())));
    List<TableIdentifier> tables = new ArrayList<>(read.tables());
    tables.sort(java.util.Comparator.comparing(TableIdentifier::toString));
    TableIdentifier existing = tables.isEmpty() ? fresh : tables.get(rng.nextInt(tables.size()));
    String key = KEYS.get(rng.nextInt(KEYS.size()));
    String value = "v" + rng.nextInt(1000);
    try {
      switch (op) {
        case CREATE_NAMESPACE:
          // a map of its own: a later change of its properties in the transaction merges into it
          mutation.createNamespace(
              ns, rng.nextBoolean() ? new HashMap<>() : new HashMap<>(Map.of(key, value)));
          bound.add(ns);
          return "createNamespace(" + ns + ")";
        case DROP_NAMESPACE:
          mutation.dropNamespace(ns);
          return "dropNamespace(" + ns + ")";
        case SET_PROPERTY:
          mutation.updateProperties(ns, new HashMap<>(Map.of(key, value)));
          return "setProperty(" + ns + ", " + key + ")";
        case REMOVE_PROPERTY:
          Map<String, String> removal = new HashMap<>();
          removal.put(key, null);
          mutation.updateProperties(ns, removal);
          return "removeProperty(" + ns + ", " + key + ")";
        case CREATE_TABLE:
          mutation.createTable(fresh, "mem:///" + fresh + "/" + value);
          bound.add(fresh);
          return "createTable(" + fresh + ")";
        case CREATE_TABLE_INLINE:
          mutation.createTableInline(fresh,
              CatalogTestSupport.newInlineTable("mem:///" + fresh + "/" + value),
              CatalogTestSupport.inlineTableCodec());
          bound.add(fresh);
          return "createTableInline(" + fresh + ")";
        case DROP_TABLE:
          mutation.dropTable(existing);
          return "dropTable(" + existing + ")";
        case UPDATE_TABLE:
          if (updated.put(existing, true) != null) {
            return null; // one update of a table per transaction
          }
          mutation.updateTable(existing, "mem:///" + existing + "/" + value);
          return "updateTable(" + existing + ")";
        case READ_TABLE:
          if (rng.nextBoolean()) {
            mutation.readTable(existing);
            return "readTable(" + existing + ")";
          }
          mutation.readRef(existing, "main");
          return "readRef(" + existing + ", main)";
        case UPDATE_INLINE_FULL:
          if (!read.isInlineTable(existing) || updated.put(existing, true) != null) {
            return null;
          }
          mutation.updateTableInline(existing,
              CatalogTestSupport.newInlineTable("mem:///" + existing + "/" + value),
              CatalogTestSupport.inlineTableCodec());
          return "updateTableInline(" + existing + ")";
        case UPDATE_INLINE_DELTA:
          if (!read.isInlineTable(existing) || updated.put(existing, true) != null) {
            return null;
          }
          mutation.updateTableInlineDelta(existing,
              CatalogTestSupport.propertyDelta(key, value, System.currentTimeMillis()));
          if (rng.nextInt(4) == 0) {
            mutation.pin(existing);
            return "updateTableInlineDelta(" + existing + ", pinned)";
          }
          return "updateTableInlineDelta(" + existing + ")";
        case RENAME_TABLE:
          mutation.renameTable(existing, fresh);
          bound.add(fresh);
          return "renameTable(" + existing + ", " + fresh + ")";
        default:
          throw new IllegalStateException("Not a staged op: " + op);
      }
    } catch (AlreadyExistsException
        | NoSuchNamespaceException
        | NoSuchTableException
        | NamespaceNotEmptyException
        | IllegalArgumentException
        | IllegalStateException e) {
      return null; // the read refuses it, as a writer's staging would
    }
  }
}
