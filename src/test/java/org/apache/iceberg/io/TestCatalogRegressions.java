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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.io.InMemoryAtomicFileIO.Fault;
import org.apache.iceberg.io.InMemoryAtomicFileIO.ReadFault;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Focused regression tests for catalog-side bugs surfaced when running the
 * abstract {@code CatalogTests} / {@code CatalogTransactionTests} suites
 * against real S3 (see {@code TestS3Catalog} / {@code TestS3FileIOCatalogTransaction}).
 *
 * <p>Each {@code @Nested} group below targets one bug and covers the smallest
 * scenario that reproduces it. The local in-memory {@link CasFileIO}
 * implements only the slice of {@link SupportsAtomicOperations} needed by
 * these scenarios — including a real CAS / APPEND check — so the conflict
 * tests don't need a cloud emulator.
 */
public class TestCatalogRegressions {

  private static final Schema SCHEMA = new Schema(
      Types.NestedField.required(1, "id", Types.LongType.get()),
      Types.NestedField.optional(2, "data", Types.StringType.get()));

  // ============================================================
  // CAS-aware in-memory FileIO
  // ============================================================

  /**
   * In-memory FileIO whose {@link #newOutputFile(InputFile)} preserves the
   * snapshot of the file at the time the InputFile was created. CAS rejects
   * the write if the live bytes have diverged from that snapshot; APPEND
   * rejects if the live length has grown beyond it.
   */
  static class CasFileIO implements SupportsAtomicOperations {
    final ConcurrentHashMap<String, byte[]> files = new ConcurrentHashMap<>();

    @Override
    public InputFile newInputFile(String location) {
      // Snapshot live bytes right now; the InputFile keeps that view for
      // subsequent CAS / APPEND comparisons.
      byte[] snapshot = files.get(location);
      return new SnapshotInputFile(location, snapshot);
    }

    @Override
    public OutputFile newOutputFile(String location) {
      return new PlainOutputFile(location);
    }

    @Override
    public void deleteFile(String location) {
      files.remove(location);
    }

    @Override
    public AtomicOutputFile newOutputFile(InputFile replace) {
      String location = replace.location();
      byte[] snapshot = ((SnapshotInputFile) replace).snapshot;
      return new SnapshotAtomicOutputFile(location, snapshot);
    }

    class SnapshotInputFile implements InputFile {
      final String location;
      final byte[] snapshot;

      SnapshotInputFile(String location, byte[] snapshot) {
        this.location = location;
        this.snapshot = snapshot;
      }

      @Override public long getLength() {
        byte[] b = snapshot;
        return b != null ? b.length : 0;
      }

      @Override public SeekableInputStream newStream() {
        byte[] b = files.get(location);
        if (b == null) {
          throw new RuntimeException("File not found: " + location);
        }
        return new InMemoryInputFile(location, b).newStream();
      }

      @Override public String location() { return location; }
      @Override public boolean exists() { return files.containsKey(location); }
    }

    class PlainOutputFile implements OutputFile {
      final String location;

      PlainOutputFile(String location) {
        this.location = location;
      }

      @Override public PositionOutputStream create() {
        return new BufferingOutputStream(location);
      }
      @Override public PositionOutputStream createOrOverwrite() { return create(); }
      @Override public String location() { return location; }
      @Override public InputFile toInputFile() { return newInputFile(location); }
    }

    class BufferingOutputStream extends PositionOutputStream {
      final String location;
      final ByteArrayOutputStream buf = new ByteArrayOutputStream();
      long pos = 0;

      BufferingOutputStream(String location) { this.location = location; }
      @Override public long getPos() { return pos; }
      @Override public void write(int b) { buf.write(b); pos++; }
      @Override public void write(byte[] b, int off, int len) { buf.write(b, off, len); pos += len; }
      @Override public void close() throws IOException {
        super.close();
        files.put(location, buf.toByteArray());
      }
    }

    class SnapshotAtomicOutputFile implements AtomicOutputFile {
      final String location;
      final byte[] snapshot;
      AtomicOutputFile.Strategy strategy;

      SnapshotAtomicOutputFile(String location, byte[] snapshot) {
        this.location = location;
        this.snapshot = snapshot;
      }

      @Override
      public CAS prepare(Supplier<InputStream> content, AtomicOutputFile.Strategy s) {
        this.strategy = s;
        return new CAS() {
          @Override public long contentLength() { return 0; }
          @Override public byte[] contentChecksumBytes() { return new byte[0]; }
          @Override public String contentHeaderString() { return ""; }
        };
      }

      @Override
      public InputFile writeAtomic(CAS cas, Supplier<InputStream> content) throws IOException {
        byte[] data = readAll(content.get());
        byte[] live = files.get(location);
        if (strategy == Strategy.CAS) {
          if (!Arrays.equals(snapshot, live)) {
            throw new SupportsAtomicOperations.CASException(
                "Live bytes differ from snapshot",
                new IOException("snapshot mismatch"));
          }
          files.put(location, data);
        } else if (strategy == Strategy.APPEND) {
          int liveLen = live != null ? live.length : 0;
          int snapLen = snapshot != null ? snapshot.length : 0;
          if (liveLen != snapLen) {
            throw new SupportsAtomicOperations.AppendException(
                "Live length differs from snapshot",
                new IOException("offset mismatch"));
          }
          byte[] merged = new byte[liveLen + data.length];
          if (live != null) {
            System.arraycopy(live, 0, merged, 0, live.length);
          }
          System.arraycopy(data, 0, merged, liveLen, data.length);
          files.put(location, merged);
        } else {
          throw new IllegalStateException("Unsupported strategy: " + strategy);
        }
        return newInputFile(location);
      }

      @Override public PositionOutputStream create() {
        return new BufferingOutputStream(location);
      }
      @Override public PositionOutputStream createOrOverwrite() { return create(); }
      @Override public String location() { return location; }
      @Override public InputFile toInputFile() { return newInputFile(location); }
    }

    private static byte[] readAll(InputStream is) throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buf = new byte[4096];
      int n;
      while ((n = is.read(buf)) > 0) {
        out.write(buf, 0, n);
      }
      is.close();
      return out.toByteArray();
    }
  }

  // ============================================================
  // Catalog construction helpers
  // ============================================================

  static FileIOCatalog catalog(CasFileIO io, int maxAppendCount, Map<String, String> extra) {
    String wh = "mem:///wh";
    Map<String, String> props = new HashMap<>();
    props.put(CatalogProperties.WAREHOUSE_LOCATION, wh);
    props.put(ProtoCatalogFormat.MAX_APPEND_COUNT, String.valueOf(maxAppendCount));
    props.putAll(extra);
    ProtoCatalogFormat fmt = new ProtoCatalogFormat(props);
    FileIOCatalog c = new FileIOCatalog("test", wh + "/catalog", fmt, io, new HashMap<>());
    c.initialize("test", props);
    return c;
  }

  static FileIOCatalog catalog(CasFileIO io) {
    return catalog(io, 0, new HashMap<>());  // CAS-only by default
  }

  // ============================================================
  // Bug 1: Mut.dropTable populates `tables.put(tableId, null)`; buildActions
  // iterated `tables` and unconditionally emitted CreateTableAction with the
  // null value as metadataLocation, NPE on writeString.
  // ============================================================

  @Nested
  class DropTableEncoding {

    @Test
    void dropTableViaCatalogPersistsAndDoesNotNpe() {
      CasFileIO io = new CasFileIO();
      FileIOCatalog cat = catalog(io);
      cat.createNamespace(Namespace.of("db"));
      cat.buildTable(TableIdentifier.of("db", "t"), SCHEMA).create();

      // Before fix: this NPE'd in ProtoCodec.writeString because Mut.dropTable
      // marked the table with a null value in `tables` and buildActions
      // emitted CreateTableAction with that null as metadataLocation.
      assertThat(cat.dropTable(TableIdentifier.of("db", "t"))).isTrue();

      assertThat(cat.tableExists(TableIdentifier.of("db", "t"))).isFalse();
    }

    @Test
    void droppingMissingTableReturnsFalse() {
      CasFileIO io = new CasFileIO();
      FileIOCatalog cat = catalog(io);
      cat.createNamespace(Namespace.of("db"));
      assertThat(cat.dropTable(TableIdentifier.of("db", "ghost"))).isFalse();
    }
  }

  // ============================================================
  // Bug 2: setProperties accepts null values to mean "remove"; buildActions
  // emitted SetNamespacePropertyAction with the null value, NPE.
  // ============================================================

  @Nested
  class RemoveNamespacePropertyEncoding {

    @Test
    void removePropertiesUsesNullValuesAsRemoveMarker() {
      CasFileIO io = new CasFileIO();
      FileIOCatalog cat = catalog(io);
      cat.createNamespace(Namespace.of("db"), Map.of("k1", "v1", "k2", "v2"));

      // Before fix: removeProperties calls setProperties with null values,
      // which buildActions encoded as SetNamespacePropertyAction(... null) and
      // NPE'd in writeString.
      assertThat(cat.removeProperties(Namespace.of("db"), java.util.Set.of("k1"))).isTrue();

      Map<String, String> live = cat.loadNamespaceMetadata(Namespace.of("db"));
      assertThat(live).doesNotContainKey("k1").containsEntry("k2", "v2");
    }
  }

  // ============================================================
  // Bug 3: createNamespace's ancestor loop was missing `!ancestor.isEmpty()`,
  // so creating a top-level namespace inserted a phantom Namespace.empty()
  // into the catalog state.
  // ============================================================

  @Nested
  class CreateNamespaceAncestorLoop {

    @Test
    void topLevelCreateDoesNotInsertEmptyNamespace() {
      CasFileIO io = new CasFileIO();
      FileIOCatalog cat = catalog(io);
      cat.createNamespace(Namespace.of("db"));

      assertThat(cat.listNamespaces())
          .containsExactly(Namespace.of("db"))
          .noneMatch(Namespace::isEmpty);
    }

    @Test
    void multipleTopLevelCreatesAreAllPreserved() {
      // Bug 8 (id monotonicity) and Bug 3 interacted: with #3 creating phantom
      // empty namespaces, and #8 not bumping nextNamespaceId on replay, the
      // second top-level createNamespace silently overwrote the first.
      CasFileIO io = new CasFileIO();
      FileIOCatalog cat = catalog(io, /* maxAppendCount */ 10, new HashMap<>());
      cat.createNamespace(Namespace.of("a"));
      cat.createNamespace(Namespace.of("b"));
      cat.createNamespace(Namespace.of("c"));

      assertThat(cat.listNamespaces())
          .containsExactlyInAnyOrder(Namespace.of("a"), Namespace.of("b"), Namespace.of("c"));
    }
  }

  // ============================================================
  // Bug 4: listNamespaces returned ALL namespaces (including the phantom
  // empty one), without filtering by parent or rejecting unknown parents.
  // ============================================================

  @Nested
  class ListNamespacesFiltering {

    @Test
    void emptyParentReturnsTopLevelOnly() {
      CasFileIO io = new CasFileIO();
      FileIOCatalog cat = catalog(io, 10, new HashMap<>());
      cat.createNamespace(Namespace.of("a"));
      cat.createNamespace(Namespace.of("a", "child"));
      cat.createNamespace(Namespace.of("b"));

      assertThat(cat.listNamespaces())
          .containsExactlyInAnyOrder(Namespace.of("a"), Namespace.of("b"));
    }

    @Test
    void parentReturnsDirectChildrenOnly() {
      CasFileIO io = new CasFileIO();
      FileIOCatalog cat = catalog(io, 10, new HashMap<>());
      cat.createNamespace(Namespace.of("a"));
      cat.createNamespace(Namespace.of("a", "child1"));
      cat.createNamespace(Namespace.of("a", "child2"));
      cat.createNamespace(Namespace.of("a", "child1", "grandchild"));
      cat.createNamespace(Namespace.of("b"));

      assertThat(cat.listNamespaces(Namespace.of("a")))
          .containsExactlyInAnyOrder(
              Namespace.of("a", "child1"), Namespace.of("a", "child2"));
    }

    @Test
    void missingParentThrowsNoSuchNamespace() {
      CasFileIO io = new CasFileIO();
      FileIOCatalog cat = catalog(io);
      cat.createNamespace(Namespace.of("a"));

      assertThatThrownBy(() -> cat.listNamespaces(Namespace.of("does_not_exist")))
          .isInstanceOf(NoSuchNamespaceException.class)
          .hasMessageContaining("Namespace does not exist");
    }
  }

  // ============================================================
  // Bug 5: dropNamespace threw IllegalArgumentException for non-empty
  // namespaces; the spec wants NamespaceNotEmptyException with "is not empty".
  // ============================================================

  @Nested
  class DropNamespaceException {

    @Test
    void nonEmptyNamespaceThrowsTypedException() {
      CasFileIO io = new CasFileIO();
      FileIOCatalog cat = catalog(io, 10, new HashMap<>());
      cat.createNamespace(Namespace.of("db"));
      cat.buildTable(TableIdentifier.of("db", "t"), SCHEMA).create();

      assertThatThrownBy(() -> cat.dropNamespace(Namespace.of("db")))
          .isInstanceOf(NamespaceNotEmptyException.class)
          .hasMessageContaining("is not empty");
    }
  }

  // ============================================================
  // Bug 6: BaseMetastoreCatalog.properties() defaults to ImmutableMap.of();
  // FileIOCatalog never overrode it, so table-default.* / table-override.*
  // prefixes from catalog properties never reached TableBuilder.
  // ============================================================

  @Nested
  class TableDefaultsPropagation {

    @Test
    void tableDefaultPrefixReachesNewTable() {
      CasFileIO io = new CasFileIO();
      Map<String, String> props = new HashMap<>();
      props.put(CatalogProperties.TABLE_DEFAULT_PREFIX + "write.format.default", "parquet");
      FileIOCatalog cat = catalog(io, 0, props);
      cat.createNamespace(Namespace.of("db"));

      Table t = cat.buildTable(TableIdentifier.of("db", "t"), SCHEMA).create();

      assertThat(t.properties()).containsEntry("write.format.default", "parquet");
    }

    @Test
    void tableOverridePrefixWinsOverUserProperty() {
      CasFileIO io = new CasFileIO();
      Map<String, String> props = new HashMap<>();
      props.put(CatalogProperties.TABLE_OVERRIDE_PREFIX + "owner", "platform");
      FileIOCatalog cat = catalog(io, 0, props);
      cat.createNamespace(Namespace.of("db"));

      Table t = cat.buildTable(TableIdentifier.of("db", "t"), SCHEMA)
          .withProperty("owner", "alice") // user attempts to set their own
          .create();

      // Catalog override wins.
      assertThat(t.properties()).containsEntry("owner", "platform");
    }
  }

  // ============================================================
  // Bug 8: replayed CreateNamespaceAction / CreateTableAction added entries
  // to namespaceById / tableById without bumping nextNamespaceId / nextTableId,
  // so the next commit's idManager handed out a colliding id.
  // ============================================================

  @Nested
  class IdMonotonicity {

    @Test
    void appendingTwoNamespacesPreservesBoth() {
      // The append path is exactly what failed on S3 before the monotonicity
      // fix: every createNamespace was an append (count below the CAS limit),
      // and the second create reused id=1 because the checkpoint-derived
      // nextNamespaceId had not been bumped during replay.
      CasFileIO io = new CasFileIO();
      FileIOCatalog cat = catalog(io, /* maxAppendCount */ 10, new HashMap<>());
      cat.createNamespace(Namespace.of("ns1"));
      cat.createNamespace(Namespace.of("ns2"));

      assertThat(cat.listNamespaces())
          .as("both namespaces must survive the second append")
          .containsExactlyInAnyOrder(Namespace.of("ns1"), Namespace.of("ns2"));
    }

    @Test
    void appendingTwoTablesPreservesBoth() {
      CasFileIO io = new CasFileIO();
      FileIOCatalog cat = catalog(io, 10, new HashMap<>());
      cat.createNamespace(Namespace.of("db"));
      cat.buildTable(TableIdentifier.of("db", "t1"), SCHEMA).create();
      cat.buildTable(TableIdentifier.of("db", "t2"), SCHEMA).create();

      assertThat(cat.listTables(Namespace.of("db")))
          .containsExactlyInAnyOrder(
              TableIdentifier.of("db", "t1"), TableIdentifier.of("db", "t2"));
    }
  }

  // ============================================================
  // Bug 7+9: Mut.commit's CAS-failure retry silently rewrote the file from
  // the stale `original` checkpoint and overwrote the concurrent commit;
  // and when an action's verify rejected our append, validateCommit threw a
  // generic "Conflicting concurrent transaction" instead of a typed
  // AlreadyExistsException for create-races / a "Cannot commit"
  // CommitFailedException for everything else.
  //
  // These race tests share a setup: open two FileIOCatalog instances over
  // the same {@link CasFileIO}, let the first commit, then verify the
  // second's commit fails in the right way.
  // ============================================================

  @Nested
  class ConflictDetection {

    @Test
    void casUpdateRaceSurfacesAsCommitFailedException() {
      CasFileIO io = new CasFileIO();
      FileIOCatalog a = catalog(io); // CAS-only
      a.createNamespace(Namespace.of("db"));
      a.buildTable(TableIdentifier.of("db", "t"), SCHEMA).create();

      // Both `a` and `b` see the same starting state; both build pending
      // updates against it. `a` commits first; `b`'s commit must fail.
      FileIOCatalog b = catalog(io);
      Table tableA = a.loadTable(TableIdentifier.of("db", "t"));
      Table tableB = b.loadTable(TableIdentifier.of("db", "t"));

      tableA.updateSchema().addColumn("ca", Types.LongType.get()).commit();

      assertThatThrownBy(
              () -> tableB.updateSchema().addColumn("cb", Types.LongType.get()).commit())
          .isInstanceOf(CommitFailedException.class)
          .hasMessageContaining("Cannot commit");
    }

    @Test
    void casCreateRaceSurfacesAsAlreadyExists() {
      CasFileIO io = new CasFileIO();
      FileIOCatalog a = catalog(io);
      a.createNamespace(Namespace.of("db"));

      FileIOCatalog b = catalog(io);
      // Both have read the same catalog state (no table). `a` creates first.
      a.buildTable(TableIdentifier.of("db", "t"), SCHEMA).create();

      assertThatThrownBy(
              () -> b.buildTable(TableIdentifier.of("db", "t"), SCHEMA).create())
          .isInstanceOfAny(AlreadyExistsException.class, CommitFailedException.class);
      // Either way the second create must not silently overwrite the first;
      // `a`'s metadata location must still be the live one.
      Table reloaded = a.loadTable(TableIdentifier.of("db", "t"));
      assertThat(reloaded).isNotNull();
    }

    @Test
    void appendUpdateRaceSurfacesAsCommitFailedException() {
      CasFileIO io = new CasFileIO();
      // maxAppendCount=10000 so individual commits stay on the append branch.
      FileIOCatalog a = catalog(io, 10_000, new HashMap<>());
      a.createNamespace(Namespace.of("db"));
      a.buildTable(TableIdentifier.of("db", "t"), SCHEMA).create();

      FileIOCatalog b = catalog(io, 10_000, new HashMap<>());
      Table tableA = a.loadTable(TableIdentifier.of("db", "t"));
      Table tableB = b.loadTable(TableIdentifier.of("db", "t"));

      tableA.updateSchema().addColumn("ca", Types.LongType.get()).commit();

      // Append succeeds at the storage layer (file just grows), but our
      // CreateTableUpdate's verify against the post-append state finds the
      // bumped table version and rejects the action; validateCommit must
      // surface that as a typed failure.
      assertThatThrownBy(
              () -> tableB.updateSchema().addColumn("cb", Types.LongType.get()).commit())
          .isInstanceOf(CommitFailedException.class)
          .hasMessageContaining("Cannot commit");
    }

    @Test
    void appendCreateRaceSurfacesAsAlreadyExists() {
      CasFileIO io = new CasFileIO();
      FileIOCatalog a = catalog(io, 10_000, new HashMap<>());
      a.createNamespace(Namespace.of("db"));

      FileIOCatalog b = catalog(io, 10_000, new HashMap<>());
      a.buildTable(TableIdentifier.of("db", "t"), SCHEMA).create();

      assertThatThrownBy(
              () -> b.buildTable(TableIdentifier.of("db", "t"), SCHEMA).create())
          .isInstanceOfAny(AlreadyExistsException.class, CommitFailedException.class);
    }
  }

  // ============================================================
  // Race helpers for Bug 10 / Bug 11. FileIOCatalog re-reads the catalog for
  // every namespace op, so these tests drive ProtoCatalogFormat directly: both
  // writers mutate the same snapshot, then commit in sequence, as two
  // concurrent processes would.
  // ============================================================

  private static final String RACE_LOC = "mem:///wh/catalog";
  private static final int APPEND = 10_000;

  private static ProtoCatalogFormat format(int maxAppendCount) {
    return new ProtoCatalogFormat(
        Map.of(ProtoCatalogFormat.MAX_APPEND_COUNT, String.valueOf(maxAppendCount)));
  }

  /** Commits {@code namespaces} and returns the snapshot both racing writers start from. */
  private static ProtoCatalogFile seed(
      SupportsAtomicOperations io, ProtoCatalogFormat fmt, String... namespaces) {
    ProtoCatalogFormat.Mut mut = (ProtoCatalogFormat.Mut) fmt.empty(io.newInputFile(RACE_LOC));
    for (String ns : namespaces) {
      mut.createNamespace(Namespace.of(ns));
    }
    mut.commit(io);
    return reread(io, fmt);
  }

  private static ProtoCatalogFile reread(SupportsAtomicOperations io, ProtoCatalogFormat fmt) {
    return fmt.read(io, io.newInputFile(RACE_LOC));
  }

  // ============================================================
  // Bug 10: buildActions bound new namespace / table ids eagerly from the
  // writer's snapshot (nextNamespaceId / nextTableId). Two writers that read
  // the same snapshot and created unrelated entities wrote records carrying
  // the same id. On the append path both records verified -- their parents
  // differed, or the parent was the then-unversioned root (Bug 11) -- and
  // replay let the second overwrite the first. Both writers were told they
  // committed.
  // ============================================================

  @Nested
  class ConcurrentCreateIds {

    @Test
    void appendCreatesInDifferentNamespacesBothSurvive() {
      CasFileIO io = new CasFileIO();
      ProtoCatalogFormat fmt = format(APPEND);
      ProtoCatalogFile snap = seed(io, fmt, "a", "b");
      TableIdentifier t1 = TableIdentifier.of("a", "t1");
      TableIdentifier t2 = TableIdentifier.of("b", "t2");

      fmt.from(snap).createTable(t1, "mem:///a/t1/v1.metadata.json").commit(io);
      fmt.from(snap).createTable(t2, "mem:///b/t2/v1.metadata.json").commit(io);

      ProtoCatalogFile result = reread(io, fmt);
      assertThat(result.tables()).containsExactlyInAnyOrder(t1, t2);
      assertThat(result.location(t1)).isEqualTo("mem:///a/t1/v1.metadata.json");
      assertThat(result.location(t2)).isEqualTo("mem:///b/t2/v1.metadata.json");
      assertThat(result.tableId(t1)).isNotEqualTo(result.tableId(t2));
    }

    @Test
    void appendCreatesWithNestedReferencesBindToTheirOwnParents() {
      // Each writer creates a namespace, a child namespace, and a table in the
      // child, all in one transaction: the child and the table reference ids
      // allocated in the same transaction.
      CasFileIO io = new CasFileIO();
      ProtoCatalogFormat fmt = format(APPEND);
      ProtoCatalogFile snap = seed(io, fmt, "a", "b");
      TableIdentifier at = TableIdentifier.of("a", "p", "c", "t");
      TableIdentifier bt = TableIdentifier.of("b", "q", "c", "t");

      fmt.from(snap)
          .createNamespace(Namespace.of("a", "p"))
          .createNamespace(Namespace.of("a", "p", "c"))
          .createTable(at, "mem:///a/p/c/t/v1.metadata.json")
          .commit(io);
      fmt.from(snap)
          .createNamespace(Namespace.of("b", "q"))
          .createNamespace(Namespace.of("b", "q", "c"))
          .createTable(bt, "mem:///b/q/c/t/v1.metadata.json")
          .commit(io);

      ProtoCatalogFile result = reread(io, fmt);
      assertThat(result.namespaces())
          .containsExactlyInAnyOrder(
              Namespace.of("a"), Namespace.of("a", "p"), Namespace.of("a", "p", "c"),
              Namespace.of("b"), Namespace.of("b", "q"), Namespace.of("b", "q", "c"));
      assertThat(result.tables()).containsExactlyInAnyOrder(at, bt);
      assertThat(result.location(at)).isEqualTo("mem:///a/p/c/t/v1.metadata.json");
      assertThat(result.location(bt)).isEqualTo("mem:///b/q/c/t/v1.metadata.json");
    }

    @ParameterizedTest(name = "maxAppendCount={0}")
    @ValueSource(ints = {0, APPEND})
    void acknowledgedCreatesAreNeverLost(int maxAppendCount) {
      // The invariant behind the tests above, for both commit paths: a writer
      // whose commit returned normally must find its create in the catalog.
      // The CAS path may reject the second writer; it must not drop the first.
      CasFileIO io = new CasFileIO();
      ProtoCatalogFormat fmt = format(maxAppendCount);
      ProtoCatalogFile snap = seed(io, fmt, "a", "b");
      Map<TableIdentifier, String> acknowledged = new HashMap<>();
      Map<TableIdentifier, String> creates =
          Map.of(
              TableIdentifier.of("a", "t1"), "mem:///a/t1/v1.metadata.json",
              TableIdentifier.of("b", "t2"), "mem:///b/t2/v1.metadata.json");

      for (Map.Entry<TableIdentifier, String> create : creates.entrySet()) {
        try {
          fmt.from(snap).createTable(create.getKey(), create.getValue()).commit(io);
          acknowledged.put(create.getKey(), create.getValue());
        } catch (CommitFailedException e) {
          // Rejected, not acknowledged: nothing to check for this writer.
        }
      }

      ProtoCatalogFile result = reread(io, fmt);
      assertThat(acknowledged).isNotEmpty();
      acknowledged.forEach(
          (ident, location) -> assertThat(result.location(ident)).as("%s", ident).isEqualTo(location));
    }
  }

  // ============================================================
  // Bug 11: the root namespace (id 0) had no row and so no version.
  // bumpNamespaceVersion(0) was a no-op and CreateNamespaceAction.verify
  // skipped the parent check for the root, so top-level creates never
  // conflicted -- not even two creates of the same name. The root is now an
  // ordinary namespace row that cannot be named, created, or dropped.
  // Separately, CatalogFile.Mut treated the empty namespace as existing, so a
  // table in the root passed validation and then hit an NPE in buildActions.
  // ============================================================

  @Nested
  class RootNamespace {

    @Test
    void appendCreatesUnderRootConflictOnRootVersion() {
      // Top-level creates serialize on the root's version, as sibling creates
      // under any other parent do.
      CasFileIO io = new CasFileIO();
      ProtoCatalogFormat fmt = format(APPEND);
      ProtoCatalogFile snap = seed(io, fmt, "seed");

      fmt.from(snap).createNamespace(Namespace.of("x")).commit(io);
      assertThatThrownBy(() -> fmt.from(snap).createNamespace(Namespace.of("y")).commit(io))
          .isInstanceOf(CommitFailedException.class);

      assertThat(reread(io, fmt).namespaces())
          .containsExactlyInAnyOrder(Namespace.of("seed"), Namespace.of("x"));
    }

    @Test
    void appendCreatesOfSameRootNamespaceConflict() {
      CasFileIO io = new CasFileIO();
      ProtoCatalogFormat fmt = format(APPEND);
      ProtoCatalogFile snap = seed(io, fmt, "seed");

      fmt.from(snap).createNamespace(Namespace.of("x")).commit(io);
      assertThatThrownBy(() -> fmt.from(snap).createNamespace(Namespace.of("x")).commit(io))
          .isInstanceOfAny(AlreadyExistsException.class, CommitFailedException.class);

      ProtoCatalogFile result = reread(io, fmt);
      assertThat(result.namespaceById().values().stream().filter(e -> e.name.equals("x")))
          .hasSize(1);
    }

    @Test
    void droppingTopLevelNamespaceConflictsWithTopLevelCreate() {
      CasFileIO io = new CasFileIO();
      ProtoCatalogFormat fmt = format(APPEND);
      ProtoCatalogFile snap = seed(io, fmt, "a");

      fmt.from(snap).dropNamespace(Namespace.of("a")).commit(io);
      assertThatThrownBy(() -> fmt.from(snap).createNamespace(Namespace.of("b")).commit(io))
          .isInstanceOf(CommitFailedException.class);
    }

    @Test
    void rootVersionSurvivesCompaction() {
      // CAS-only: every commit writes a fresh checkpoint, so the root version
      // must round-trip through the checkpoint encoding.
      CasFileIO io = new CasFileIO();
      ProtoCatalogFormat fmt = format(0);
      seed(io, fmt, "a");
      fmt.from(reread(io, fmt)).createNamespace(Namespace.of("b")).commit(io);
      fmt.from(reread(io, fmt)).createNamespace(Namespace.of("c")).commit(io);

      assertThat(reread(io, fmt).namespaceVersion(ProtoCatalogFile.ROOT_ID)).isEqualTo(3);
    }

    @Test
    void rootIsNotAddressableByName() {
      CasFileIO io = new CasFileIO();
      FileIOCatalog cat = catalog(io);
      cat.createNamespace(Namespace.of("a"));

      ProtoCatalogFile file = reread(io, format(0));
      assertThat(file.namespaceEntry(ProtoCatalogFile.ROOT_ID)).isNotNull();
      assertThat(file.namespaces()).containsExactly(Namespace.of("a"));
      assertThat(cat.namespaceExists(Namespace.empty())).isFalse();
      assertThat(cat.listNamespaces()).containsExactly(Namespace.of("a"));
    }

    @Test
    void replayNeverDropsOrReplacesTheRoot() {
      ProtoCatalogFile.Builder builder =
          ProtoCatalogFile.builder(new InMemoryInputFile(new byte[0]));
      int rootVersion = builder.namespaceVersion(ProtoCatalogFile.ROOT_ID);

      assertThat(new ProtoCodec.DropNamespaceAction(ProtoCatalogFile.ROOT_ID, rootVersion)
              .verify(builder))
          .isFalse();
      assertThat(new ProtoCodec.CreateNamespaceAction(ProtoCatalogFile.ROOT_ID, 1, 0, -1, "x")
              .verify(builder))
          .isFalse();
      assertThatThrownBy(() -> builder.removeNamespace(ProtoCatalogFile.ROOT_ID))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void tableInRootIsRejected() {
      CasFileIO io = new CasFileIO();
      ProtoCatalogFormat fmt = format(APPEND);
      ProtoCatalogFile snap = seed(io, fmt, "a");

      assertThatThrownBy(
              () -> fmt.from(snap).createTable(TableIdentifier.of("t"), "mem:///t/v1.metadata.json"))
          .isInstanceOf(NoSuchNamespaceException.class);

      FileIOCatalog cat = catalog(io);
      assertThatThrownBy(() -> cat.buildTable(TableIdentifier.of("t"), SCHEMA))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Invalid table identifier");
    }

    @Test
    void propertiesOnRootAreRejected() {
      CasFileIO io = new CasFileIO();
      FileIOCatalog cat = catalog(io);
      cat.createNamespace(Namespace.of("a"));

      assertThatThrownBy(() -> cat.setProperties(Namespace.empty(), Map.of("k", "v")))
          .isInstanceOf(NoSuchNamespaceException.class);
    }
  }
  // ============================================================
  // Bug 12: when an append lost a race, the commit loop refreshed `current`
  // to the newer file and kept going without re-reading it. If the next
  // attempt then had to CAS (the refreshed length crossed max.append.size, or
  // a compaction had shrunk the file and the re-read count reached
  // max.append.count), tryCAS wrote a checkpoint of the writer's original
  // snapshot conditioned on the refreshed version: the CAS succeeded and
  // erased every record committed in between.
  // ============================================================

  @Nested
  class AppendRetry {

    @Test
    void compactionAfterLostAppendRaceKeepsTheWinner() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      ProtoCatalogFile snap = seed(io, format(APPEND), "a", "b");
      // A small transaction fits under the size limit; the winner's large one
      // does not, so the loser's retry sees a file that must be compacted.
      ProtoCatalogFormat fmt =
          new ProtoCatalogFormat(APPEND, snap.location().getLength() + 1_000);
      TableIdentifier winner = TableIdentifier.of("a", "t1");
      TableIdentifier loser = TableIdentifier.of("b", "t2");
      String winnerLocation = "mem:///a/t1/" + "x".repeat(4_000) + ".metadata.json";

      fmt.from(snap).createTable(winner, winnerLocation).commit(io);
      try {
        fmt.from(snap).createTable(loser, "mem:///b/t2/v1.metadata.json").commit(io);
      } catch (CommitFailedException e) {
        // Rejecting the loser is fine; dropping the winner is not.
      }

      assertThat(reread(io, fmt).location(winner)).isEqualTo(winnerLocation);
    }

    @Test
    void compactionAfterRaceWithCompactionKeepsTheLog() {
      // The loser's snapshot holds a large namespace that is then dropped and
      // compacted away, so the loser's retry finds a shorter file: it re-reads
      // the record count, which forces a CAS.
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      ProtoCatalogFormat fmt = format(3);
      seed(io, fmt, "a", "b");
      fmt.from(reread(io, fmt))
          .createNamespace(Namespace.of("big"), Map.of("k", "x".repeat(4_000)))
          .commit(io);
      ProtoCatalogFile snap = reread(io, fmt);

      fmt.from(reread(io, fmt)).dropNamespace(Namespace.of("big")).commit(io);
      for (String ns : new String[] {"y", "z", "v"}) {
        fmt.from(reread(io, fmt)).createNamespace(Namespace.of("a", ns)).commit(io);
      }
      assertThat(io.conditionalWrites().get(RACE_LOC))
          .as("the drop is compacted away and two appends follow")
          .endsWith(
              AtomicOutputFile.Strategy.CAS,
              AtomicOutputFile.Strategy.APPEND,
              AtomicOutputFile.Strategy.APPEND);

      List<Namespace> expected =
          Lists.newArrayList(
              Namespace.of("a"), Namespace.of("b"),
              Namespace.of("a", "y"), Namespace.of("a", "z"), Namespace.of("a", "v"));
      try {
        fmt.from(snap).createNamespace(Namespace.of("b", "w")).commit(io);
        expected.add(Namespace.of("b", "w"));
      } catch (CommitFailedException e) {
        // Rejecting the loser is fine; resurrecting "big" or dropping the
        // namespaces committed since its snapshot is not.
      }

      assertThat(reread(io, fmt).namespaces()).containsExactlyInAnyOrderElementsOf(expected);
    }
  }

  // ============================================================
  // Bug 13: CatalogFile.Mut validated each staged operation against the
  // writer's snapshot, ignoring the transaction's own pending renames and
  // namespace drops. A transaction could rename a table and then drop, update
  // or rename it again under its old name, create a second table at the
  // rename destination, drop the namespace a table was renamed into, or
  // create, rename into, or set properties on a namespace it had dropped.
  // The commit was acknowledged with some of those changes silently lost or
  // with a table left without its namespace. Found by
  // TestMultiWriterProperties.
  // ============================================================

  @Nested
  class CompoundTransactionValidation {

    private final TableIdentifier at = TableIdentifier.of("a", "t");
    private final TableIdentifier bt = TableIdentifier.of("b", "t");

    private CatalogFile.Mut<ProtoCatalogFile, ?> txn() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      ProtoCatalogFormat fmt = format(APPEND);
      CatalogFile.Mut<ProtoCatalogFile, ?> seed = fmt.empty(io.newInputFile(RACE_LOC));
      seed.createNamespace(Namespace.of("a"));
      seed.createNamespace(Namespace.of("b"));
      seed.createTable(at, "mem:///a/t/v1.metadata.json");
      seed.commit(io);
      return fmt.from(reread(io, fmt));
    }

    @Test
    void renamedTableCannotBeDroppedUnderItsOldName() {
      CatalogFile.Mut<ProtoCatalogFile, ?> txn = txn();
      txn.renameTable(at, bt);
      assertThatThrownBy(() -> txn.dropTable(at)).isInstanceOf(NoSuchTableException.class);
    }

    @Test
    void renamedTableCannotBeRenamedAgainUnderItsOldName() {
      CatalogFile.Mut<ProtoCatalogFile, ?> txn = txn();
      txn.renameTable(at, bt);
      assertThatThrownBy(() -> txn.renameTable(at, TableIdentifier.of("b", "u")))
          .isInstanceOf(NoSuchTableException.class);
    }

    @Test
    void renamedTableCannotBeUpdatedUnderItsOldName() {
      CatalogFile.Mut<ProtoCatalogFile, ?> txn = txn();
      txn.renameTable(at, bt);
      assertThatThrownBy(() -> txn.updateTable(at, "mem:///a/t/v2.metadata.json"))
          .isInstanceOf(NoSuchTableException.class);
    }

    @Test
    void renameDestinationCannotBeCreated() {
      CatalogFile.Mut<ProtoCatalogFile, ?> txn = txn();
      txn.renameTable(at, bt);
      assertThatThrownBy(() -> txn.createTable(bt, "mem:///b/t/v1.metadata.json"))
          .isInstanceOf(AlreadyExistsException.class);
    }

    @Test
    void namespaceWithRenamedInTableCannotBeDropped() {
      CatalogFile.Mut<ProtoCatalogFile, ?> txn = txn();
      txn.renameTable(at, bt);
      assertThatThrownBy(() -> txn.dropNamespace(Namespace.of("b")))
          .isInstanceOf(NamespaceNotEmptyException.class);
    }

    @Test
    void droppedNamespaceCannotReceiveTables() {
      CatalogFile.Mut<ProtoCatalogFile, ?> txn = txn();
      txn.dropNamespace(Namespace.of("b"));
      assertThatThrownBy(() -> txn.createTable(bt, "mem:///b/t/v1.metadata.json"))
          .isInstanceOf(NoSuchNamespaceException.class);
      assertThatThrownBy(() -> txn.renameTable(at, bt))
          .isInstanceOf(NoSuchNamespaceException.class);
    }

    @Test
    void droppedNamespaceCannotTakeProperties() {
      CatalogFile.Mut<ProtoCatalogFile, ?> txn = txn();
      txn.dropNamespace(Namespace.of("b"));
      assertThatThrownBy(() -> txn.updateProperties(Namespace.of("b"), Map.of("k", "v")))
          .isInstanceOf(NoSuchNamespaceException.class);
    }
  }

  // ============================================================
  // Bug 14: a transaction could drop a namespace and create a child of it.
  // CatalogFile.Mut checked only ancestors missing from the snapshot for a
  // pending drop, and replay verifies every action against the state before
  // the transaction, so both actions verified. The child landed with no
  // parent; building the state threw, so the commit failed after its write
  // landed and every later read of the catalog failed. Found by
  // TestMultiWriterProperties.
  // ============================================================

  @Nested
  class DropAndReferenceInOneTransaction {

    @Test
    void droppedNamespaceCannotGainAChild() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      ProtoCatalogFormat fmt = format(APPEND);
      CatalogFile.Mut<ProtoCatalogFile, ?> txn = fmt.from(seed(io, fmt, "a", "b"));
      txn.dropNamespace(Namespace.of("b"));
      assertThatThrownBy(() -> txn.createNamespace(Namespace.of("b", "x")))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("marked for deletion");
    }

    @Test
    void replayRejectsARecordThatDropsWhatItReferences() throws IOException {
      // Bypass the Mut: append the record it used to write.
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      ProtoCatalogFormat fmt = format(APPEND);
      ProtoCatalogFile snap = seed(io, fmt, "a", "b");
      int b = snap.namespaceId(Namespace.of("b"));
      int version = snap.namespaceVersion(b);
      ProtoCodec.Transaction txn =
          new ProtoCodec.Transaction(
              UuidV7.newUuidV7(),
              List.of(
                  new ProtoCodec.DropNamespaceAction(b, version),
                  new ProtoCodec.CreateNamespaceAction(-1, 1, b, version, "x")));
      byte[] record = ProtoCodec.encodeTransaction(txn);
      ByteArrayOutputStream file = new ByteArrayOutputStream();
      try (InputStream in = io.newInputFile(RACE_LOC).newStream()) {
        file.write(in.readAllBytes());
      }
      for (int v = record.length; ; v >>>= 7) {
        if ((v & ~0x7F) == 0) {
          file.write(v);
          break;
        }
        file.write((v & 0x7F) | 0x80);
      }
      file.write(record);
      io.seed(RACE_LOC, file.toByteArray());

      ProtoCatalogFile result = reread(io, fmt);
      assertThat(result.containsTransaction(txn.id())).isFalse();
      assertThat(result.namespaces()).containsExactlyInAnyOrder(Namespace.of("a"), Namespace.of("b"));
    }
  }

  // ============================================================
  // Bug 15: a transaction may drop a namespace and its child; CatalogFile.Mut
  // allows it and buildActions emits the drops in HashMap order. Replay
  // removed each namespace's lookup entry by rebuilding its path, so a drop of
  // the child after its parent threw: the commit failed after its write
  // landed and every later read of the catalog failed. Removing a table whose
  // namespace was already gone failed the same way. Found by
  // TestMultiWriterProperties.
  // ============================================================

  @Nested
  class DropNamespaceAndChild {

    @Test
    void transactionDropsNamespaceAndChild() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      ProtoCatalogFormat fmt = format(APPEND);
      seed(io, fmt, "a", "b");
      fmt.from(reread(io, fmt)).createNamespace(Namespace.of("a", "x")).commit(io);

      CatalogFile.Mut<ProtoCatalogFile, ?> txn = fmt.from(reread(io, fmt));
      txn.dropNamespace(Namespace.of("a", "x"));
      txn.dropNamespace(Namespace.of("a"));
      txn.commit(io);

      assertThat(reread(io, fmt).namespaces()).containsExactly(Namespace.of("b"));
    }

    @ParameterizedTest(name = "parentFirst={0}")
    @ValueSource(booleans = {true, false})
    void replayAppliesTheDropsInEitherOrder(boolean parentFirst) {
      ProtoCatalogFile.Builder builder =
          ProtoCatalogFile.builder(new InMemoryInputFile(new byte[0]));
      builder.addNamespace(1, ProtoCatalogFile.ROOT_ID, "a", 1);
      builder.addNamespace(2, 1, "x", 1);
      builder.addTable(1, 2, "t", 1, "mem:///a/x/t/v1.metadata.json");
      builder.rebuildLookups();

      builder.removeTable(1);
      if (parentFirst) {
        builder.removeNamespace(1);
        builder.removeNamespace(2);
      } else {
        builder.removeNamespace(2);
        builder.removeNamespace(1);
      }

      ProtoCatalogFile result = builder.build();
      assertThat(result.namespaces()).isEmpty();
      assertThat(result.tables()).isEmpty();
    }

    @Test
    void tableRemovedAfterItsNamespace() {
      ProtoCatalogFile.Builder builder =
          ProtoCatalogFile.builder(new InMemoryInputFile(new byte[0]));
      builder.addNamespace(1, ProtoCatalogFile.ROOT_ID, "a", 1);
      builder.addTable(1, 1, "t", 1, "mem:///a/t/v1.metadata.json");
      builder.rebuildLookups();

      builder.removeNamespace(1);
      builder.removeTable(1);

      assertThat(builder.build().tables()).isEmpty();
    }
  }

  // ============================================================
  // Bug 16: a transaction could create a namespace and drop it again. The
  // Mut cancelled the two, so the record carried no action and no
  // precondition that the namespace was absent, and it committed even after
  // a concurrent create of the same namespace -- although, run serially after
  // that create, its createNamespace fails. Append mode always accepted the
  // empty record; the CAS rebase made CAS mode accept it too. dropTable
  // already rejected tables the transaction created. Found by
  // TestMultiWriterProperties.
  // ============================================================

  @Nested
  class CreateThenDropNamespace {

    @Test
    void namespaceCreatedInTheTransactionCannotBeDropped() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      ProtoCatalogFormat fmt = format(APPEND);
      CatalogFile.Mut<ProtoCatalogFile, ?> txn = fmt.from(seed(io, fmt, "a"));
      txn.createNamespace(Namespace.of("a", "x"));
      assertThatThrownBy(() -> txn.dropNamespace(Namespace.of("a", "x")))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("created in this transaction");
    }
  }

  // ============================================================
  // Bug 17: a conditional write whose outcome the commit could not know -- it
  // landed but the reply was lost, or reading back what it wrote failed --
  // was reported as a definite failure: CommitFailedException from the CAS
  // path, a raw UncheckedIOException from the append path. Iceberg deletes a
  // commit's new files on either, so a commit that had landed lost the
  // files its snapshot references. The commit loop now re-reads after such a
  // failure: the record carries the transaction's id, so the commit returns
  // as committed if it landed and retries if not, and throws
  // CommitStateUnknownException when it cannot tell. Found by
  // TestMultiWriterProperties with fault injection.
  // ============================================================

  @Nested
  class AmbiguousWrites {

    private void failNext(InMemoryAtomicFileIO io, Fault... faults) {
      Iterator<Fault> plan = List.of(faults).iterator();
      io.injectFaults((location, strategy) -> plan.hasNext() ? plan.next() : Fault.NONE);
    }

    @Test
    void casWhoseReplyWasLostIsCommitted() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      ProtoCatalogFormat fmt = format(0);
      ProtoCatalogFile snap = seed(io, fmt, "a");
      int writes = io.history().size();

      failNext(io, Fault.LOST_REPLY);
      ProtoCatalogFormat.Mut mut = (ProtoCatalogFormat.Mut) fmt.from(snap);
      mut.createNamespace(Namespace.of("c"));
      mut.commit(io);

      assertThat(io.history()).hasSize(writes + 1); // landed once, not written again
      assertThat(reread(io, fmt).namespaces()).contains(Namespace.of("c"));
    }

    @Test
    void appendWhoseReplyWasLostIsCommitted() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      ProtoCatalogFormat fmt = format(10_000);
      ProtoCatalogFile snap = seed(io, fmt, "a");
      int writes = io.history().size();

      failNext(io, Fault.LOST_REPLY_RUNTIME);
      ProtoCatalogFormat.Mut mut = (ProtoCatalogFormat.Mut) fmt.from(snap);
      mut.createNamespace(Namespace.of("c"));
      mut.commit(io);

      assertThat(io.history()).hasSize(writes + 1);
      assertThat(io.history().get(writes).strategy()).isEqualTo(AtomicOutputFile.Strategy.APPEND);
      assertThat(reread(io, fmt).namespaces()).contains(Namespace.of("c"));
    }

    @Test
    void writeThatFailedBeforeLandingIsRetried() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      ProtoCatalogFormat fmt = format(10_000);
      ProtoCatalogFile snap = seed(io, fmt, "a");
      int writes = io.history().size();

      failNext(io, Fault.FAIL_BEFORE, Fault.FAIL_BEFORE);
      ProtoCatalogFormat.Mut mut = (ProtoCatalogFormat.Mut) fmt.from(snap);
      mut.createNamespace(Namespace.of("c"));
      mut.commit(io);

      assertThat(io.history()).hasSize(writes + 1);
      assertThat(reread(io, fmt).namespaces()).contains(Namespace.of("c"));
    }

    @Test
    void outcomeThatNeverResolvesIsCommitStateUnknown() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      ProtoCatalogFormat fmt = format(0);
      ProtoCatalogFile snap = seed(io, fmt, "a");
      int writes = io.history().size();

      io.injectFaults((location, strategy) -> Fault.FAIL_BEFORE);
      ProtoCatalogFormat.Mut mut = (ProtoCatalogFormat.Mut) fmt.from(snap);
      mut.createNamespace(Namespace.of("c"));
      // Nothing landed, but the commit cannot know that: every write might yet.
      assertThatThrownBy(() -> mut.commit(io)).isInstanceOf(CommitStateUnknownException.class);

      io.injectFaults((location, strategy) -> Fault.NONE);
      assertThat(io.history()).hasSize(writes);
      assertThat(reread(io, fmt).namespaces()).doesNotContain(Namespace.of("c"));
    }
  }

  // ============================================================
  // Bug 18: a conditional write that failed its precondition was taken as
  // proof that it had not landed. A transport retry of a write that landed
  // fails its precondition too, so when the read that followed failed, a
  // commit that had landed was reported as a raw failure; and creating the
  // catalog file reported CommitFailedException. The commit loop now keeps
  // such a write unresolved until a read of the catalog rules it out, and a
  // failed read while one is unresolved is CommitStateUnknownException. Found
  // by TestMultiWriterProperties with read faults.
  // ============================================================

  @Nested
  class ConflictsThatLanded {

    /** Lands the first conditional write as a conflict; then fails reads if {@code failReads}. */
    private void landFirstAsConflict(InMemoryAtomicFileIO io, boolean failReads) {
      boolean[] landed = {false};
      io.injectFaults(
          (location, strategy) -> {
            if (landed[0]) {
              return Fault.NONE;
            }
            landed[0] = true;
            return Fault.LANDED_AS_CONFLICT;
          });
      io.injectReadFaults(location -> failReads && landed[0] ? ReadFault.FAIL : ReadFault.NONE);
    }

    private void healed(InMemoryAtomicFileIO io) {
      io.injectFaults((location, strategy) -> Fault.NONE);
      io.injectReadFaults(location -> ReadFault.NONE);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 10_000})
    void writeReportedAsConflictThatLandedIsCommitted(int maxAppendCount) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      ProtoCatalogFormat fmt = format(maxAppendCount);
      ProtoCatalogFile snap = seed(io, fmt, "a");

      landFirstAsConflict(io, false);
      fmt.from(snap).createNamespace(Namespace.of("c")).commit(io);

      healed(io);
      assertThat(reread(io, fmt).namespaces()).contains(Namespace.of("c"));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 10_000})
    void unreadableCatalogAfterAConflictThatLandedIsCommitStateUnknown(int maxAppendCount) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      ProtoCatalogFormat fmt = format(maxAppendCount);
      ProtoCatalogFile snap = seed(io, fmt, "a");

      landFirstAsConflict(io, true);
      assertThatThrownBy(() -> fmt.from(snap).createNamespace(Namespace.of("c")).commit(io))
          .isInstanceOf(CommitStateUnknownException.class);

      healed(io);
      assertThat(reread(io, fmt).namespaces()).contains(Namespace.of("c"));
    }

    @Test
    void catalogCreationReportedAsConflictThatLandedIsCommitted() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      ProtoCatalogFormat fmt = format(0);

      landFirstAsConflict(io, false);
      fmt.empty(io.newInputFile(RACE_LOC)).createNamespace(Namespace.of("a")).commit(io);

      healed(io);
      assertThat(reread(io, fmt).namespaces()).contains(Namespace.of("a"));
    }
  }

  // ============================================================
  // CAS rebase: a CAS that loses a race re-reads the file and retries on top
  // of the new state when the transaction's preconditions still hold there,
  // instead of failing every concurrent CAS-mode commit. A transaction that
  // no longer holds fails without writing.
  // ============================================================

  @Nested
  class CasRebase {

    @Test
    void casConflictRebasesWhenTheTransactionStillHolds() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      ProtoCatalogFormat fmt = format(0);
      ProtoCatalogFile snap = seed(io, fmt, "a", "b");
      TableIdentifier t1 = TableIdentifier.of("a", "t1");
      TableIdentifier t2 = TableIdentifier.of("b", "t2");

      fmt.from(snap).createTable(t1, "mem:///a/t1/v1.metadata.json").commit(io);
      fmt.from(snap).createTable(t2, "mem:///b/t2/v1.metadata.json").commit(io);

      assertThat(io.rejected(AtomicOutputFile.Strategy.CAS)).isEqualTo(1);
      ProtoCatalogFile result = reread(io, fmt);
      assertThat(result.location(t1)).isEqualTo("mem:///a/t1/v1.metadata.json");
      assertThat(result.location(t2)).isEqualTo("mem:///b/t2/v1.metadata.json");
    }

    @Test
    void casConflictThatNoLongerHoldsFailsWithoutWriting() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      ProtoCatalogFormat fmt = format(0);
      ProtoCatalogFile snap = seed(io, fmt, "a", "b");

      fmt.from(snap).updateProperties(Namespace.of("a"), new HashMap<>(Map.of("k", "1"))).commit(io);
      int writes = io.history().size();
      assertThatThrownBy(
              () ->
                  fmt.from(snap)
                      .updateProperties(Namespace.of("a"), new HashMap<>(Map.of("k", "2")))
                      .commit(io))
          .isInstanceOf(CommitFailedException.class);

      assertThat(io.history()).hasSize(writes);
      assertThat(reread(io, fmt).namespaceProperties(Namespace.of("a"))).containsEntry("k", "1");
    }

    @Test
    void casCreateRaceIsAlreadyExists() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      ProtoCatalogFormat fmt = format(0);
      ProtoCatalogFile snap = seed(io, fmt, "a");
      TableIdentifier t = TableIdentifier.of("a", "t");

      fmt.from(snap).createTable(t, "mem:///a/t/v1.metadata.json").commit(io);
      assertThatThrownBy(() -> fmt.from(snap).createTable(t, "mem:///a/t/v2.metadata.json").commit(io))
          .isInstanceOf(AlreadyExistsException.class);

      assertThat(reread(io, fmt).location(t)).isEqualTo("mem:///a/t/v1.metadata.json");
    }
  }
}
