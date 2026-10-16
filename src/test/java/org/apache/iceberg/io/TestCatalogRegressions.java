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

  /** An unpartitioned data file at mem:///data/{name}.parquet. */
  private static org.apache.iceberg.DataFile file(String name) {
    return org.apache.iceberg.DataFiles.builder(org.apache.iceberg.PartitionSpec.unpartitioned())
        .withPath("mem:///data/" + name + ".parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
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
  // Bug 19: an inline-ML snapshot without manifests (a delete that matched
  // nothing) lost its manifest-list entry when a CAS rewrote the checkpoint,
  // if the table's manifest pool was empty: the checkpoint wrote and read
  // snapshot refs only alongside pool entries. The table then failed to
  // load ("sentinel manifest-list location but no pool entry"). Found by
  // TestCatalogFaultProperties.
  // ============================================================

  @Nested
  class EmptyInlineManifestList {

    @Test
    void snapshotWithoutManifestsSurvivesCompaction() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog =
          LocalCatalogMode.COMPACT_ML.catalog(io, "mem:///warehouse", "c", Map.of());
      Namespace ns = Namespace.of("ns");
      catalog.createNamespace(ns);
      TableIdentifier id = TableIdentifier.of(ns, "t");
      Table table =
          catalog.createTable(id, new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())));
      table.newDelete().deleteFile("mem:///missing.parquet").commit();
      long snapshotId = catalog.loadTable(id).currentSnapshot().snapshotId();

      int compactions = compactions(io);
      while (compactions(io) == compactions) {
        catalog.setProperties(ns, Map.of("k", "v" + io.history().size()));
      }

      Table reloaded = catalog.loadTable(id);
      assertThat(reloaded.currentSnapshot().snapshotId()).isEqualTo(snapshotId);
      assertThat(reloaded.currentSnapshot().allManifests(io)).isEmpty();
    }

    private int compactions(InMemoryAtomicFileIO io) {
      return (int)
          io.history().stream().filter(w -> w.strategy() == AtomicOutputFile.Strategy.CAS).count();
    }
  }

  // ============================================================
  // Bug 20: replay built an inline-ML snapshot's manifest list from its
  // AddManifest / RemoveManifest updates, copying the parent's list only when
  // the first such update arrived. SnapshotProducer stages the list as a delta
  // against the parent, so a snapshot that changes no manifests (a delete
  // that matched nothing) got an empty list, and the table lost every data
  // file. Racing writers hit it without such a delete: a delete on an empty
  // table, retried after a concurrent append, rebases onto the append and
  // changes none of its manifests. Found by TestCatalogFaultProperties.
  // ============================================================

  @Nested
  class UnchangedInlineManifestList {

    private final Schema schema =
        new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    private final TableIdentifier id = TableIdentifier.of("ns", "t");

    private org.apache.iceberg.DataFile file(String name) {
      return org.apache.iceberg.DataFiles.builder(org.apache.iceberg.PartitionSpec.unpartitioned())
          .withPath("mem:///data/" + name + ".parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .build();
    }

    private long liveFiles(Table table) throws IOException {
      try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = table.newScan().planFiles()) {
        return org.apache.iceberg.relocated.com.google.common.collect.Iterables.size(tasks);
      }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class,
        names = {"APPEND_ML", "CAS_ML", "COMPACT_ML"})
    void deleteThatMatchesNothingKeepsTheParentsManifests(LocalCatalogMode mode)
        throws IOException {
      FileIOCatalog catalog = mode.catalog(new InMemoryAtomicFileIO(), "mem:///w", "c", Map.of());
      catalog.createNamespace(Namespace.of("ns"));
      Table table = catalog.createTable(id, schema);
      table.newFastAppend().appendFile(file("a")).commit();

      table.newDelete().deleteFile("mem:///data/missing.parquet").commit();

      assertThat(liveFiles(catalog.loadTable(id))).isEqualTo(1);
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class,
        names = {"APPEND_ML", "CAS_ML", "COMPACT_ML"})
    void deleteRebasedOntoARacingAppendKeepsIt(LocalCatalogMode mode) throws IOException {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog first = mode.catalog(io, "mem:///w", "c", Map.of());
      FileIOCatalog second = mode.catalog(io, "mem:///w", "c", Map.of());
      first.createNamespace(Namespace.of("ns"));
      first.createTable(id, schema);
      Table table = first.loadTable(id);

      // The append lands just before the delete's first write, which then loses the race.
      boolean[] raced = {false};
      io.injectFaults(
          (location, strategy) -> {
            if (!raced[0]) {
              raced[0] = true;
              second.loadTable(id).newFastAppend().appendFile(file("a")).commit();
            }
            return Fault.NONE;
          });
      table.newDelete().deleteFile("mem:///data/missing.parquet").commit();
      io.injectFaults((location, strategy) -> Fault.NONE);

      assertThat(liveFiles(first.loadTable(id))).isEqualTo(1);
    }

    // One commit can add a snapshot and its child; the child builds on the parent's final list.
    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class,
        names = {"APPEND_ML", "CAS_ML", "COMPACT_ML"})
    void twoAppendsInOneCatalogTransactionKeepBoth(LocalCatalogMode mode) throws IOException {
      FileIOCatalog catalog = mode.catalog(new InMemoryAtomicFileIO(), "mem:///w", "c", Map.of());
      catalog.createNamespace(Namespace.of("ns"));
      catalog.createTable(id, schema).newFastAppend().appendFile(file("a")).commit();

      org.apache.iceberg.catalog.CatalogTransaction txn =
          catalog.createTransaction(
              org.apache.iceberg.catalog.CatalogTransaction.IsolationLevel.SNAPSHOT);
      txn.asCatalog().loadTable(id).newFastAppend().appendFile(file("b")).commit();
      txn.asCatalog().loadTable(id).newFastAppend().appendFile(file("c")).commit();
      txn.commitTransaction();

      assertThat(liveFiles(catalog.loadTable(id))).isEqualTo(3);
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class,
        names = {"APPEND_ML", "CAS_ML", "COMPACT_ML"})
    void appendAfterADeleteThatMatchesNothingInOneTransaction(LocalCatalogMode mode)
        throws IOException {
      FileIOCatalog catalog = mode.catalog(new InMemoryAtomicFileIO(), "mem:///w", "c", Map.of());
      catalog.createNamespace(Namespace.of("ns"));
      Table table = catalog.createTable(id, schema);
      table.newFastAppend().appendFile(file("a")).commit();

      org.apache.iceberg.Transaction txn = table.newTransaction();
      txn.newDelete().deleteFile("mem:///data/missing.parquet").commit();
      txn.newFastAppend().appendFile(file("b")).commit();
      txn.commitTransaction();

      assertThat(liveFiles(catalog.loadTable(id))).isEqualTo(2);
    }
  }

  // ============================================================
  // Bug 21: a serializable catalog transaction pinned the tables it read at
  // the versions FileIOCatalog.commitTransaction read, not the versions the
  // transaction read. BaseCatalogTransaction validates the read tables just
  // before calling it, so a change landing between that check and the
  // commit's own read went unseen, and the transaction committed on a table
  // that had changed under it (write skew). The transaction now passes the
  // metadata of each branch it read; the commit checks them, by the rule
  // BaseCatalogTransaction uses, on the state it builds on, and pins those
  // tables. Found by TestCatalogFaultProperties.
  // ============================================================

  @Nested
  class SerializableReadSkew {

    private org.apache.iceberg.DataFile file(String name) {
      return org.apache.iceberg.DataFiles.builder(org.apache.iceberg.PartitionSpec.unpartitioned())
          .withPath("mem:///data/" + name + ".parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .build();
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(LocalCatalogMode.class)
    void changeToAReadTableAfterValidationFailsTheCommit(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog first = mode.catalog(io, "mem:///w", "c", Map.of());
      FileIOCatalog second = mode.catalog(io, "mem:///w", "c", Map.of());
      Namespace ns = Namespace.of("ns");
      first.createNamespace(ns);
      Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
      TableIdentifier read = TableIdentifier.of(ns, "read");
      TableIdentifier written = TableIdentifier.of(ns, "written");
      first.createTable(read, schema).newFastAppend().appendFile(file("r0")).commit();
      first.createTable(written, schema);

      org.apache.iceberg.catalog.CatalogTransaction txn =
          first.createTransaction(
              org.apache.iceberg.catalog.CatalogTransaction.IsolationLevel.SERIALIZABLE);
      org.apache.iceberg.catalog.Catalog txc = txn.asCatalog();
      txc.loadTable(read);
      txc.loadTable(written).newFastAppend().appendFile(file("w1")).commit();

      // The commit reads the catalog once per table to validate, then once to commit. Another
      // writer changes the read table just before that last read.
      int[] reads = {0};
      Thread committer = Thread.currentThread();
      io.injectReadFaults(
          location -> {
            if (location.equals("mem:///w/catalog-c")
                && Thread.currentThread() == committer
                && reads[0]++ == 2) {
              second.loadTable(read).newFastAppend().appendFile(file("r1")).commit();
            }
            return ReadFault.NONE;
          });
      assertThatThrownBy(txn::commitTransaction)
          .isInstanceOf(org.apache.iceberg.exceptions.ValidationException.class);
      io.injectReadFaults(location -> ReadFault.NONE);

      assertThat(first.loadTable(written).currentSnapshot()).isNull();
    }
  }

  // ============================================================
  // Bug 22: an inline table's synthetic metadata location was
  // inline://<name>#<version>, and versions count from 1 for each table, so
  // a table created or renamed under a dropped table's name could reach the
  // location a handle to the dropped table had cached. BaseMetastoreTable-
  // Operations reloads only when the location changes, so the stale handle
  // kept the dropped table's metadata, skipped the UUID check, and committed
  // a delta built on it to the new table, which lost its snapshot. The
  // location now carries the table's id. Found by TestCatalogFaultProperties.
  // ============================================================

  @Nested
  class StaleInlineHandle {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class,
        names = {"APPEND_TM", "CAS_TM", "COMPACT_TM", "APPEND_ML", "CAS_ML", "COMPACT_ML"})
    void handleToADroppedTableCannotCommitToItsSuccessor(LocalCatalogMode mode)
        throws IOException {
      FileIOCatalog catalog = mode.catalog(new InMemoryAtomicFileIO(), "mem:///w", "c", Map.of());
      catalog.createNamespace(Namespace.of("ns"));
      TableIdentifier id = TableIdentifier.of("ns", "t");
      Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
      catalog.createTable(id, schema);
      Table stale = catalog.loadTable(id);
      stale.updateProperties().set("k", "v").commit();
      stale.refresh(); // caches the dropped table's version 2

      catalog.dropTable(id, false);
      catalog.createTable(id, schema);
      catalog.loadTable(id).newFastAppend().appendFile(file("a")).commit(); // also version 2

      assertThatThrownBy(() -> stale.newFastAppend().appendFile(file("b")).commit())
          .isInstanceOfAny(IllegalStateException.class, CommitFailedException.class);
      try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
          catalog.loadTable(id).newScan().planFiles()) {
        assertThat(tasks).extracting(t -> t.file().location())
            .containsExactly("mem:///data/a.parquet");
      }
    }
  }

  // ============================================================
  // Bug 24: committing a catalog transaction after another writer dropped a
  // table it updates threw NullPointerException from the requirement checks.
  // It now throws NoSuchTableException. Found by TestCatalogFaultProperties.
  // ============================================================

  @Nested
  class TransactionOnADroppedTable {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(LocalCatalogMode.class)
    void commitAfterTheTableWasDroppedIsNoSuchTable(LocalCatalogMode mode) {
      FileIOCatalog catalog = mode.catalog(new InMemoryAtomicFileIO(), "mem:///w", "c", Map.of());
      catalog.createNamespace(Namespace.of("ns"));
      TableIdentifier id = TableIdentifier.of("ns", "t");
      catalog.createTable(id, new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())));

      org.apache.iceberg.catalog.CatalogTransaction txn =
          catalog.createTransaction(
              org.apache.iceberg.catalog.CatalogTransaction.IsolationLevel.SNAPSHOT);
      txn.asCatalog().loadTable(id).newFastAppend().appendFile(file("a")).commit();
      catalog.dropTable(id, false);

      assertThatThrownBy(txn::commitTransaction).isInstanceOf(NoSuchTableException.class);
      assertThat(catalog.tableExists(id)).isFalse();
    }
  }

  // ============================================================
  // Bug 25: a CAS that storage throttled until the retries ran out was
  // reported as CommitFailedException, and a throttled append escaped as a
  // raw failure. GCS reports a 429 or a 5xx as throttling, and the write
  // may have landed regardless: a 5xx can follow a write the service
  // applied, and the SDK's own retries report only the last attempt.
  // Iceberg deletes a commit's files on CommitFailedException, so a commit
  // that landed lost them. Running out of throttle retries is now an
  // ambiguous write, which the commit loop resolves by reading the catalog.
  // ============================================================

  @Nested
  class ThrottledWrites {

    private ProtoCatalogFormat format(int maxAppendCount, long throttleMinWaitMs) {
      return new ProtoCatalogFormat(
          maxAppendCount,
          ProtoCatalogFormat.DEFAULT_MAX_APPEND_SIZE,
          ProtoCatalogFormat.DEFAULT_COMMITTED_RETENTION_MS,
          throttleMinWaitMs);
    }

    /** Fails conditional writes as {@code faults} says, in order; later writes succeed. */
    private void plan(InMemoryAtomicFileIO io, List<Fault> faults) {
      Iterator<Fault> next = faults.iterator();
      io.injectFaults((location, strategy) -> next.hasNext() ? next.next() : Fault.NONE);
    }

    private List<Fault> throttled(Fault first, int times) {
      List<Fault> faults = Lists.newArrayList(first);
      for (int i = 1; i < times; i++) {
        faults.add(Fault.THROTTLED);
      }
      return faults;
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 10_000})
    void writeThatLandedBeforeThrottlingRanOutIsCommitted(int maxAppendCount) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      ProtoCatalogFormat fmt = format(maxAppendCount, 0);
      ProtoCatalogFile snap = seed(io, fmt, "a");
      int writes = io.history().size();

      plan(io, throttled(Fault.LANDED_THROTTLED, 8)); // every retry throttled
      fmt.from(snap).createNamespace(Namespace.of("c")).commit(io);

      assertThat(io.history()).hasSize(writes + 1);
      assertThat(reread(io, fmt).namespaces()).contains(Namespace.of("c"));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 10_000})
    void writeThrottledUntilTheRetriesRanOutIsRetried(int maxAppendCount) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      ProtoCatalogFormat fmt = format(maxAppendCount, 0);
      ProtoCatalogFile snap = seed(io, fmt, "a");
      int writes = io.history().size();

      plan(io, throttled(Fault.THROTTLED, 8));
      fmt.from(snap).createNamespace(Namespace.of("c")).commit(io);

      assertThat(io.history()).hasSize(writes + 1);
      assertThat(reread(io, fmt).namespaces()).contains(Namespace.of("c"));
    }

    @Test
    void throttlingThatNeverEndsIsCommitStateUnknown() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      ProtoCatalogFormat fmt = format(0, 0);
      ProtoCatalogFile snap = seed(io, fmt, "a");
      int writes = io.history().size();

      io.injectFaults((location, strategy) -> Fault.THROTTLED);
      // Nothing landed, but every throttled write might have.
      assertThatThrownBy(() -> fmt.from(snap).createNamespace(Namespace.of("c")).commit(io))
          .isInstanceOf(CommitStateUnknownException.class);

      io.injectFaults((location, strategy) -> Fault.NONE);
      assertThat(io.history()).hasSize(writes);
    }

    @Test
    void interruptedBackoffIsCommitStateUnknown() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      ProtoCatalogFormat fmt = format(0, 1);
      ProtoCatalogFile snap = seed(io, fmt, "a");

      plan(io, List.of(Fault.LANDED_THROTTLED));
      Thread.currentThread().interrupt(); // the backoff's sleep throws at once
      try {
        assertThatThrownBy(() -> fmt.from(snap).createNamespace(Namespace.of("c")).commit(io))
            .isInstanceOf(CommitStateUnknownException.class);
      } finally {
        Thread.interrupted();
      }
      assertThat(reread(io, fmt).namespaces()).contains(Namespace.of("c"));
    }
  }

  // ============================================================
  // Bug 26: after an append lost its race, the commit bound the catalog's
  // new version with a metadata request (getLength), and if the retry had to
  // CAS (by size or count) it then read that version's content. Real storage
  // fails a read pinned to a version another writer has replaced in between
  // (a GCS read by generation, an S3 or ADLS read with If-Match), and with
  // the lost append still unresolved the commit reported its outcome
  // unknown: a concurrent compaction alone left a commit unknown. The commit
  // now reads the live version afresh. Found by TestMultiWriterProperties
  // with preemption and strict versions.
  // ============================================================

  @Nested
  class ReplacedVersionRead {

    @Test
    void compactionBetweenBindingAndReadingTheCatalogIsNotAmbiguous() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      io.failSupersededReads(true);
      ProtoCatalogFormat appending = format(10_000);
      ProtoCatalogFile stale = seed(io, appending, "a");

      // Another writer appends a large record.
      ProtoCatalogFormat.Mut big = (ProtoCatalogFormat.Mut) appending.from(stale);
      for (String ns : List.of("x1", "x2", "x3", "x4")) {
        big.createNamespace(Namespace.of(ns));
      }
      big.commit(io);
      ProtoCatalogFile afterBig = reread(io, appending);
      long length = io.newInputFile(RACE_LOC).getLength();

      // The stale writer's small append fits its view but loses the race; at the new length it
      // must CAS. Just before it reads the version it bound, a third writer compacts.
      ProtoCatalogFormat tight =
          new ProtoCatalogFormat(
              10_000, length, ProtoCatalogFormat.DEFAULT_COMMITTED_RETENTION_MS, 0);
      int[] reads = {0};
      io.injectReadFaults(
          location -> {
            if (reads[0]++ == 1) {
              format(0).from(afterBig).createNamespace(Namespace.of("z")).commit(io);
            }
            return ReadFault.NONE;
          });
      tight.from(stale).updateProperties(Namespace.of("a"), new HashMap<>(Map.of("k", "v")))
          .commit(io);
      io.injectReadFaults(location -> ReadFault.NONE);

      ProtoCatalogFile result = reread(io, appending);
      assertThat(result.namespaces()).contains(Namespace.of("x1"), Namespace.of("z"));
      assertThat(result.namespaceProperties(Namespace.of("a"))).containsEntry("k", "v");
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

  // ============================================================
  // Writers that store table metadata differently, on one catalog -- a
  // pointer writer (metadata files), an inline-TM writer and an inline-ML
  // writer -- and tables that leave inline storage and come back. Found by
  // reading the commit paths for errata D3, and by TestCatalogFaultProperties
  // with a storage mode per writer.
  // ============================================================

  private static final String WAREHOUSE = "mem:///w";
  private static final String CATALOG = WAREHOUSE + "/catalog-c";

  private static FileIOCatalog catalog(InMemoryAtomicFileIO io, LocalCatalogMode mode) {
    return mode.catalog(io, WAREHOUSE, "c", Map.of());
  }

  /** A catalog whose inline updates must fit in 2 KiB. */
  private static FileIOCatalog small(InMemoryAtomicFileIO io, LocalCatalogMode mode) {
    return mode.catalog(io, WAREHOUSE, "c", Map.of(FileIOCatalog.INLINE_MAX_UPDATE_SIZE, "2048"));
  }

  /** A value of {@code size} random characters, which compresses little. */
  private static String bigValue(int size) {
    java.util.Random random = new java.util.Random(size);
    StringBuilder value = new StringBuilder(size);
    for (int i = 0; i < size; i++) {
      value.append((char) ('!' + random.nextInt(90)));
    }
    return value.toString();
  }

  private static org.apache.iceberg.DataFile dataFile(String name) {
    return org.apache.iceberg.DataFiles.builder(org.apache.iceberg.PartitionSpec.unpartitioned())
        .withPath("mem:///data/" + name + ".parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  private static List<String> dataFiles(Table table) {
    List<String> files = Lists.newArrayList();
    try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
        table.newScan().planFiles()) {
      tasks.forEach(task -> files.add(task.file().location()));
    } catch (IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
    files.sort(String::compareTo);
    return files;
  }

  /** Each snapshot's manifest list, with what a reader takes from each manifest's entry. */
  private static Map<Long, List<String>> manifestLists(InMemoryAtomicFileIO io, Table table) {
    Map<Long, List<String>> lists = new HashMap<>();
    for (org.apache.iceberg.Snapshot snapshot : table.snapshots()) {
      String list = snapshot.manifestListLocation();
      assertThat(list == null ? snapshot instanceof org.apache.iceberg.InlineSnapshot
              : io.newInputFile(list).exists())
          .as("snapshot %d has its manifest list (%s)", snapshot.snapshotId(), list)
          .isTrue();
      List<String> manifests = Lists.newArrayList();
      for (org.apache.iceberg.ManifestFile m : snapshot.allManifests(io)) {
        manifests.add(String.format("%s spec=%d seq=%d/%d snap=%d files=%d/%d/%d",
            m.path(), m.partitionSpecId(), m.sequenceNumber(), m.minSequenceNumber(),
            m.snapshotId(), m.addedFilesCount(), m.existingFilesCount(), m.deletedFilesCount()));
      }
      lists.put(snapshot.snapshotId(), manifests);
    }
    return lists;
  }

  private static ProtoCatalogFile catalogFile(InMemoryAtomicFileIO io) {
    return new ProtoCatalogFormat().read(io, io.newInputFile(CATALOG));
  }

  private final Namespace mixedNs = Namespace.of("ns");
  private final TableIdentifier mixed = TableIdentifier.of(mixedNs, "t");
  private final Schema intSchema =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  // ============================================================
  // Bug 27: a commit that gave an inline table a metadata file -- a pointer
  // writer's, or an inline writer's eviction -- set the location but kept
  // the inline metadata. Readers took the metadata file, but the next
  // checkpoint encoded the table as inline without its location, losing the
  // commit and every one after it; and an inline writer, seeing the table
  // still inline, applied its delta to the stale inline copy, losing the
  // same commits. A metadata-file commit now clears the inline state: a
  // table is a pointer table or an inline table, never both (SPEC.md).
  // ============================================================

  @Nested
  class MetadataFileCommitToAnInlineTable {

    @Test
    void pointerCommitSurvivesCompaction() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog inline = catalog(io, LocalCatalogMode.APPEND_TM);
      FileIOCatalog pointer = catalog(io, LocalCatalogMode.APPEND);
      FileIOCatalog compacting = catalog(io, LocalCatalogMode.CAS);
      inline.createNamespace(mixedNs);
      inline.createTable(mixed, intSchema).updateProperties().set("k", "1").commit();
      pointer.loadTable(mixed).updateProperties().set("k", "2").commit();

      compacting.createNamespace(Namespace.of("other"));

      assertThat(compacting.loadTable(mixed).properties()).containsEntry("k", "2");
      assertThat(catalogFile(io).isInlineTable(mixed)).isFalse();
    }

    @Test
    void inlineCommitBuildsOnAPointerCommit() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog inline = catalog(io, LocalCatalogMode.APPEND_TM);
      FileIOCatalog pointer = catalog(io, LocalCatalogMode.APPEND);
      inline.createNamespace(mixedNs);
      inline.createTable(mixed, intSchema).updateProperties().set("k", "1").commit();
      pointer.loadTable(mixed).updateProperties().set("k", "2").commit();

      inline.loadTable(mixed).updateProperties().set("j", "1").commit();

      assertThat(pointer.loadTable(mixed).properties())
          .containsEntry("k", "2")
          .containsEntry("j", "1");
    }

    @Test
    void evictionSurvivesCompaction() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog inline = small(io, LocalCatalogMode.APPEND_TM);
      FileIOCatalog compacting = small(io, LocalCatalogMode.CAS_TM);
      inline.createNamespace(mixedNs);
      inline.createTable(mixed, intSchema);
      String big = bigValue(6000);
      inline.loadTable(mixed).updateProperties().set("big", big).commit();
      assertThat(catalogFile(io).isInlineTable(mixed)).as("evicted").isFalse();

      compacting.createNamespace(Namespace.of("other"));

      assertThat(compacting.loadTable(mixed).properties()).containsEntry("big", big);
    }
  }

  // ============================================================
  // Bug 28 (errata D3): a metadata file written for a table with inline
  // manifest lists -- by a writer that does not inline them, or by an
  // eviction -- named no manifest list file for the inline snapshots.
  // Iceberg wrote their manifests as a v1 embedded list of paths, which
  // reads back with sequence number 0, spec 0 and the snapshot's own id for
  // every manifest. The writer now writes each inline snapshot's list to an
  // Avro file first, and an inline-ML table can leave inline storage (it
  // used to be kept inline past the size limit instead).
  // ============================================================

  @Nested
  class MetadataFileForInlineManifestLists {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND", "CAS"})
    void pointerWriterKeepsTheManifestLists(LocalCatalogMode pointerMode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog ml = catalog(io, LocalCatalogMode.APPEND_ML);
      FileIOCatalog pointer = catalog(io, pointerMode);
      ml.createNamespace(mixedNs);
      ml.createTable(mixed, intSchema).newFastAppend().appendFile(dataFile("a")).commit();
      ml.loadTable(mixed).newAppend().appendFile(dataFile("b")).commit();
      Map<Long, List<String>> before = manifestLists(io, ml.loadTable(mixed));

      pointer.loadTable(mixed).updateProperties().set("k", "v").commit();

      Table table = ml.loadTable(mixed);
      assertThat(manifestLists(io, table)).isEqualTo(before);
      assertThat(dataFiles(table))
          .containsExactly("mem:///data/a.parquet", "mem:///data/b.parquet");
    }

    @Test
    void evictionWritesTheManifestListsAndTheTableReturns() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog ml = small(io, LocalCatalogMode.APPEND_ML);
      FileIOCatalog compacting = small(io, LocalCatalogMode.CAS_ML);
      ml.createNamespace(mixedNs);
      ml.createTable(mixed, intSchema).newFastAppend().appendFile(dataFile("a")).commit();
      Map<Long, List<String>> lists = manifestLists(io, ml.loadTable(mixed));

      // Too big for a delta or in full: the table leaves inline storage.
      ml.loadTable(mixed).updateProperties().set("big", bigValue(6000)).commit();
      assertThat(catalogFile(io).isInlineTable(mixed)).as("evicted").isFalse();
      assertThat(manifestLists(io, ml.loadTable(mixed))).isEqualTo(lists);

      // Full metadata still does not fit, so a new snapshot gets a manifest list file.
      ml.loadTable(mixed).newFastAppend().appendFile(dataFile("b")).commit();
      assertThat(catalogFile(io).isInlineTable(mixed)).isFalse();
      lists = manifestLists(io, ml.loadTable(mixed));
      compacting.createNamespace(Namespace.of("other"));
      assertThat(manifestLists(io, ml.loadTable(mixed))).isEqualTo(lists);

      // Once it fits in full, the next commit brings the table back, with an inline list for its
      // new snapshot.
      ml.loadTable(mixed).updateProperties().remove("big").commit();
      assertThat(catalogFile(io).isInlineTable(mixed)).as("re-inlined").isTrue();
      ml.loadTable(mixed).newAppend().appendFile(dataFile("c")).commit();
      compacting.createNamespace(Namespace.of("another"));

      Table table = ml.loadTable(mixed);
      Map<Long, List<String>> after = manifestLists(io, table);
      assertThat(after).containsAllEntriesOf(lists);
      assertThat(table.currentSnapshot()).isInstanceOf(org.apache.iceberg.InlineSnapshot.class);
      assertThat(dataFiles(table))
          .containsExactly(
              "mem:///data/a.parquet", "mem:///data/b.parquet", "mem:///data/c.parquet");
    }

    @Test
    void tableTooBigToCreateInlineGetsAMetadataFile() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog ml = small(io, LocalCatalogMode.APPEND_ML);
      ml.createNamespace(mixedNs);
      String big = bigValue(6000);

      ml.buildTable(mixed, intSchema).withProperty("big", big).create();

      assertThat(catalogFile(io).isInlineTable(mixed)).isFalse();
      assertThat(ml.loadTable(mixed).properties()).containsEntry("big", big);
    }

    @Test
    void createTransactionTooBigToInlineWritesItsManifestLists() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog ml = small(io, LocalCatalogMode.APPEND_ML);
      ml.createNamespace(mixedNs);

      org.apache.iceberg.Transaction create =
          ml.buildTable(mixed, intSchema).withProperty("big", bigValue(6000))
              .createTransaction();
      create.newFastAppend().appendFile(dataFile("a")).commit();
      create.newFastAppend().appendFile(dataFile("b")).commit();
      create.commitTransaction();

      assertThat(catalogFile(io).isInlineTable(mixed)).isFalse();
      Table table = ml.loadTable(mixed);
      assertThat(manifestLists(io, table)).hasSize(2);
      assertThat(dataFiles(table))
          .containsExactly("mem:///data/a.parquet", "mem:///data/b.parquet");
    }
  }

  // ============================================================
  // An inline-ML writer's commit to a pointer table brings the table back
  // inline in full mode. The old commit path chose full mode for it but
  // sent the full metadata without the new snapshot's manifest list, so the
  // table no longer loaded ("sentinel manifest-list location ... but no
  // pool entry"). Full mode now carries the manifest updates. Fixed with
  // Bug 28 by the single commit path.
  // ============================================================

  @Nested
  class InlineManifestListsIntoAPointerTable {

    @Test
    void firstAppendBringsThePointerTableInline() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog pointer = catalog(io, LocalCatalogMode.APPEND);
      FileIOCatalog ml = catalog(io, LocalCatalogMode.APPEND_ML);
      pointer.createNamespace(mixedNs);
      pointer.createTable(mixed, intSchema);

      ml.loadTable(mixed).newFastAppend().appendFile(dataFile("a")).commit();

      assertThat(catalogFile(io).isInlineTable(mixed)).isTrue();
      Table table = pointer.loadTable(mixed);
      assertThat(table.currentSnapshot()).isInstanceOf(org.apache.iceberg.InlineSnapshot.class);
      assertThat(dataFiles(table)).containsExactly("mem:///data/a.parquet");
    }
  }

  // ============================================================
  // Bug 29: an inline-ML snapshot's list is staged, and replayed, as a
  // delta against its parent's inline list. When the parent's list was a
  // file -- the snapshot of an inline-TM writer, which writes manifest list
  // files, or of a pointer writer, as when an inline-ML writer brings a
  // pointer table back inline -- the replay found no parent list and the
  // new snapshot lost every manifest it carried over. Such a snapshot now
  // carries its whole list.
  // ============================================================

  @Nested
  class InlineSnapshotOnAManifestListFile {

    @Test
    void appendThatReinlinesKeepsItsManifestList() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog pointer = catalog(io, LocalCatalogMode.APPEND);
      FileIOCatalog ml = catalog(io, LocalCatalogMode.APPEND_ML);
      pointer.createNamespace(mixedNs);
      pointer.createTable(mixed, intSchema).newFastAppend().appendFile(dataFile("a")).commit();

      ml.loadTable(mixed).newFastAppend().appendFile(dataFile("b")).commit();

      assertThat(catalogFile(io).isInlineTable(mixed)).isTrue();
      assertThat(dataFiles(pointer.loadTable(mixed)))
          .containsExactly("mem:///data/a.parquet", "mem:///data/b.parquet");
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND", "APPEND_TM"})
    void inlineSnapshotKeepsItsParentsManifests(LocalCatalogMode other) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog ml = catalog(io, LocalCatalogMode.APPEND_ML);
      FileIOCatalog writer = catalog(io, other);
      ml.createNamespace(mixedNs);
      ml.createTable(mixed, intSchema).newFastAppend().appendFile(dataFile("a")).commit();
      writer.loadTable(mixed).newFastAppend().appendFile(dataFile("b")).commit();

      ml.loadTable(mixed).newAppend().appendFile(dataFile("c")).commit();

      assertThat(dataFiles(writer.loadTable(mixed)))
          .containsExactly(
              "mem:///data/a.parquet", "mem:///data/b.parquet", "mem:///data/c.parquet");
    }
  }

  // ============================================================
  // Bug 30: a commit that writes full metadata -- a create, or a pointer
  // table coming back inline -- sent the manifest updates of its new inline
  // snapshots only if there were any. A snapshot that changes no manifests
  // (a delete that matched nothing) has none, so the replay registered no
  // list for it and the table no longer loaded ("sentinel manifest-list
  // location ... but no pool entry"). Found by TestCatalogFaultProperties.
  // Such a commit now sends the updates whenever it adds an inline
  // snapshot, and the replay registers every new inline snapshot's list.
  // ============================================================

  @Nested
  class InlineSnapshotWithNoManifestChanges {

    @Test
    void deleteThatMatchesNothingReinlines() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog pointer = catalog(io, LocalCatalogMode.APPEND);
      FileIOCatalog ml = catalog(io, LocalCatalogMode.APPEND_ML);
      pointer.createNamespace(mixedNs);
      pointer.createTable(mixed, intSchema);

      ml.loadTable(mixed).newDelete().deleteFile("mem:///data/missing.parquet").commit();

      Table table = pointer.loadTable(mixed);
      assertThat(table.currentSnapshot()).isNotNull();
      assertThat(manifestLists(io, table).get(table.currentSnapshot().snapshotId())).isEmpty();
    }

    @Test
    void createTransactionWithASnapshotThatChangesNothing() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog ml = catalog(io, LocalCatalogMode.APPEND_ML);
      ml.createNamespace(mixedNs);

      org.apache.iceberg.Transaction create = ml.newCreateTableTransaction(mixed, intSchema);
      create.newDelete().deleteFile("mem:///data/missing.parquet").commit();
      create.commitTransaction();

      Table table = ml.loadTable(mixed);
      assertThat(table.currentSnapshot()).isNotNull();
      assertThat(manifestLists(io, table).get(table.currentSnapshot().snapshotId())).isEmpty();
    }
  }

  // ============================================================
  // Bug 31: a commit applied its staged inline manifest lists to any
  // snapshot in its metadata, including ones already committed: a rule for
  // in-place manifest rewrites, which no producer performs. Lists must stay
  // staged on the table handle until a commit lands, since Iceberg's
  // transaction retry re-stages them only if the table changed; then a
  // commit whose outcome its writer did not learn -- it landed, but the
  // catalog could not be read back (CommitStateUnknownException) -- leaves
  // its stage behind, the handle's next commit sends it again for the now
  // committed snapshot, and the replay lists that snapshot's manifests a
  // second time. Found by TestCatalogFaultProperties once the lists were
  // kept. Stages now apply only to the snapshots a commit adds; a committed
  // snapshot's list never changes.
  // ============================================================

  @Nested
  class StageOfACommitWithUnknownOutcome {

    @Test
    void nextCommitDoesNotReplayIt() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog ml = catalog(io, LocalCatalogMode.APPEND_ML);
      ml.createNamespace(mixedNs);
      Table table = ml.createTable(mixed, intSchema);
      table.newFastAppend().appendFile(dataFile("a")).commit();

      // The append lands, but its writer cannot read the catalog back to learn so.
      boolean[] landed = {false};
      io.injectFaults(
          (location, strategy) -> {
            if (landed[0]) {
              return Fault.NONE;
            }
            landed[0] = true;
            return Fault.LANDED_AS_CONFLICT;
          });
      io.injectReadFaults(
          location -> landed[0] && location.equals(CATALOG) ? ReadFault.FAIL : ReadFault.NONE);
      assertThatThrownBy(() -> table.newAppend().appendFile(dataFile("b")).commit())
          .isInstanceOf(CommitStateUnknownException.class);
      io.injectFaults((location, strategy) -> Fault.NONE);
      io.injectReadFaults(location -> ReadFault.NONE);
      Map<Long, List<String>> lists = manifestLists(io, ml.loadTable(mixed));

      table.newFastAppend().appendFile(dataFile("c")).commit();

      Table reloaded = ml.loadTable(mixed);
      assertThat(manifestLists(io, reloaded)).containsAllEntriesOf(lists);
      assertThat(dataFiles(reloaded))
          .containsExactly(
              "mem:///data/a.parquet", "mem:///data/b.parquet", "mem:///data/c.parquet");
    }
  }

  // ============================================================
  // A checkpoint of a replayed catalog file held less than the replay did,
  // in two ways readers do not see. Dropping a table kept its inline
  // metadata and manifest lists, under an id never used again, until a
  // checkpoint left them out. A table that returned to inline storage had
  // no manifest list prefix, where a created one and a checkpoint have the
  // empty prefix; readers treat the two alike. Dropping a table now removes
  // its inline state, and a returning table gets the empty prefix. Found by
  // TestCatalogFaultProperties' checkpoint round trip.
  // ============================================================

  @Nested
  class CheckpointOfAReplayedCatalog {

    @Test
    void droppedInlineTableLeavesNothing() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog ml = catalog(io, LocalCatalogMode.APPEND_ML);
      ml.createNamespace(mixedNs);
      ml.createTable(mixed, intSchema).newFastAppend().appendFile(dataFile("a")).commit();
      int id = catalogFile(io).tableId(mixed);

      ml.dropTable(mixed, false);

      ProtoCatalogFile file = catalogFile(io);
      assertThat(file.isInlineTable(id)).isFalse();
      assertThat(file.snapshotManifests(id)).isEmpty();
      assertThat(CheckpointRoundTrip.difference(file)).isNull();
    }

    @Test
    void tableReturningInlineHasTheEmptyPrefix() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog inline = small(io, LocalCatalogMode.APPEND_TM);
      inline.createNamespace(mixedNs);
      inline.createTable(mixed, intSchema);
      inline.loadTable(mixed).updateProperties().set("big", bigValue(6000)).commit();
      assertThat(catalogFile(io).isInlineTable(mixed)).as("evicted").isFalse();

      inline.loadTable(mixed).updateProperties().remove("big").commit();

      ProtoCatalogFile file = catalogFile(io);
      assertThat(file.isInlineTable(mixed)).as("returned").isTrue();
      assertThat(file.manifestListPrefix(file.tableId(mixed))).isEmpty();
      assertThat(CheckpointRoundTrip.difference(file)).isNull();
    }
  }

  // ============================================================
  // Bug 32: a catalog transaction committed its tables' new inline-ML
  // snapshots with manifest lists staged on whichever handle to the table
  // the thread loaded first and that still held a stage: the transaction's
  // own if none, but another's if that one held a stage left from a table
  // transaction that was abandoned, or whose commit failed before it landed
  // (Bug 31 keeps such stages). The new snapshot then took no list of its
  // own: replay gave it its parent's, and the acknowledged append was lost.
  // A commit now takes each new snapshot's list from the snapshot, which
  // holds it, and nothing is staged. Found by TestCatalogFaultProperties.
  // ============================================================

  @Nested
  class CatalogTransactionWithAnotherHandleStaged {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void keepsItsOwnAppend(boolean failedCommit) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog ml = catalog(io, LocalCatalogMode.APPEND_ML);
      ml.createNamespace(mixedNs);
      ml.createTable(mixed, intSchema).newFastAppend().appendFile(dataFile("a")).commit();
      // Another handle stages an append in a table transaction that does not land.
      org.apache.iceberg.Transaction other = ml.loadTable(mixed).newTransaction();
      other.newFastAppend().appendFile(dataFile("b")).commit();
      if (failedCommit) {
        io.injectReadFaults(location -> ReadFault.FAIL);
        assertThatThrownBy(other::commitTransaction).isInstanceOf(RuntimeException.class);
        io.injectReadFaults(location -> ReadFault.NONE);
      }

      org.apache.iceberg.catalog.CatalogTransaction txn =
          ml.createTransaction(org.apache.iceberg.catalog.CatalogTransaction.IsolationLevel.SNAPSHOT);
      txn.asCatalog().loadTable(mixed).newFastAppend().appendFile(dataFile("c")).commit();
      txn.commitTransaction();

      assertThat(dataFiles(ml.loadTable(mixed)))
          .containsExactly("mem:///data/a.parquet", "mem:///data/c.parquet");
    }
  }

  // ============================================================
  // Bug 33: a v3 snapshot that adds no rows (a delete that matched nothing,
  // or of a table's last file) has added-rows 0. An inline delta wrote the
  // field only when positive and replay rebuilt it as absent, which
  // BaseSnapshot rejects when first-row-id is set: no reader could replay
  // the catalog file, so the commit made every table unreadable. The delta
  // now writes added-rows whenever the snapshot has it. Found by
  // TestCatalogFaultProperties with v3 tables.
  // ============================================================

  @Nested
  class V3SnapshotThatAddsNoRows {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND_TM", "APPEND_ML"})
    void replays(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      catalog.createNamespace(mixedNs);
      v3(catalog).newDelete().deleteFile("mem:///data/missing.parquet").commit();

      Table table = catalog(io, LocalCatalogMode.APPEND).loadTable(mixed);
      assertThat(table.currentSnapshot().firstRowId()).isEqualTo(0L);
      assertThat(table.currentSnapshot().addedRows()).isEqualTo(0L);
      assertThat(CheckpointRoundTrip.difference(catalogFile(io))).isNull();
    }
  }

  // ============================================================
  // Bug 34: a manifest list entry's first_row_id was stored only when not
  // zero, so the first data manifest of a v3 table, whose rows start at 0,
  // lost it: read inline, its files had no row ids, and a manifest list
  // file written for it (by a pointer writer or an eviction) gave it a new
  // range, which a later manifest's already held. The entry now stores
  // first_row_id whenever the manifest has one. Found by
  // TestCatalogFaultProperties with v3 tables.
  // ============================================================

  @Nested
  class V3FirstManifest {

    @Test
    void keepsItsRowIds() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog ml = catalog(io, LocalCatalogMode.APPEND_ML);
      FileIOCatalog pointer = catalog(io, LocalCatalogMode.APPEND);
      ml.createNamespace(mixedNs);
      v3(ml).newFastAppend().appendFile(dataFile("a")).commit();
      ml.loadTable(mixed).newFastAppend().appendFile(dataFile("b")).commit();
      Map<String, Long> expected = Map.of("mem:///data/a.parquet", 0L, "mem:///data/b.parquet", 1L);
      assertThat(rowIds(ml.loadTable(mixed))).as("inline").isEqualTo(expected);

      pointer.loadTable(mixed).updateProperties().set("k", "v").commit();

      assertThat(rowIds(ml.loadTable(mixed))).as("manifest list files").isEqualTo(expected);
    }
  }

  // ============================================================
  // Bug 35: a manifest list file for an inline snapshot of a v3 table was
  // written in v3, which needs the snapshot's first row id. A table
  // upgraded from v2 keeps the snapshots it had, which have none, so no
  // metadata file could be written for it: it could not leave inline
  // storage, and a writer that does not inline could not commit to it. A
  // snapshot without row ids now gets a v2 list, as it had when written.
  // Found by TestCatalogFaultProperties with v3 tables.
  // ============================================================

  @Nested
  class TableUpgradedToV3 {

    @Test
    void leavesInlineStorage() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog ml = catalog(io, LocalCatalogMode.APPEND_ML);
      FileIOCatalog pointer = catalog(io, LocalCatalogMode.APPEND);
      ml.createNamespace(mixedNs);
      ml.createTable(mixed, intSchema).newFastAppend().appendFile(dataFile("a")).commit();
      ml.loadTable(mixed)
          .updateProperties()
          .set(org.apache.iceberg.TableProperties.FORMAT_VERSION, "3")
          .commit();
      Map<Long, List<String>> before = manifestLists(io, ml.loadTable(mixed));

      pointer.loadTable(mixed).updateProperties().set("k", "v").commit();

      assertThat(manifestLists(io, ml.loadTable(mixed))).isEqualTo(before);
      // The next snapshot gives a's manifest row ids, after b's, which a fast append lists first.
      ml.loadTable(mixed).newFastAppend().appendFile(dataFile("b")).commit();
      assertThat(rowIds(ml.loadTable(mixed)))
          .isEqualTo(Map.of("mem:///data/a.parquet", 1L, "mem:///data/b.parquet", 0L));
    }
  }

  // ============================================================
  // Bug 36: the first snapshot of a table upgraded to v3 gives row ids to
  // the data manifests it keeps from before the upgrade. The catalog's
  // inline lists hold one entry per manifest path, shared by the table's
  // snapshots, and the snapshot sent only the manifests its parent's list
  // lacked: the kept manifests' new first row ids were dropped, so their
  // files never had row ids inline, and a manifest list file written for
  // the snapshot later gave them a range another manifest held. Updating
  // the shared entry would change the parent's list (design.md I3a), so
  // such a commit now writes a metadata file, which holds each snapshot's
  // list exactly; the table returns inline with a later update. Found by
  // TestCatalogFaultProperties with v3 tables.
  // ============================================================

  @Nested
  class FirstSnapshotAfterAnUpgradeToV3 {

    @Test
    void givesRowIdsToTheFilesItKeeps() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog ml = catalog(io, LocalCatalogMode.APPEND_ML);
      ml.createNamespace(mixedNs);
      ml.createTable(mixed, intSchema).newFastAppend().appendFile(dataFile("a")).commit();
      ml.loadTable(mixed)
          .updateProperties()
          .set(org.apache.iceberg.TableProperties.FORMAT_VERSION, "3")
          .commit();
      Map<Long, List<String>> before = manifestLists(io, ml.loadTable(mixed));

      ml.loadTable(mixed).newFastAppend().appendFile(dataFile("b")).commit();

      // a's manifest gets row ids after b's, which a fast append lists first.
      Map<String, Long> expected = Map.of("mem:///data/a.parquet", 1L, "mem:///data/b.parquet", 0L);
      assertThat(rowIds(ml.loadTable(mixed))).isEqualTo(expected);
      assertThat(manifestLists(io, ml.loadTable(mixed))).containsAllEntriesOf(before);

      ml.loadTable(mixed).updateProperties().set("k", "v").commit();

      assertThat(catalogFile(io).isInlineTable(mixed)).as("returned").isTrue();
      assertThat(rowIds(ml.loadTable(mixed))).isEqualTo(expected);
      assertThat(manifestLists(io, ml.loadTable(mixed))).containsAllEntriesOf(before);
    }
  }

  // ============================================================
  // Bug 37: an inline delta has no update for the format version, and
  // computeDelta did not look at it. An upgrade alone left the delta
  // empty, which takes full mode, so it landed; an upgrade with any other
  // change in the same commit went as a delta without it, and was
  // acknowledged but lost. A format-version change now takes full mode.
  // Found by TestCatalogFaultProperties with v3 tables.
  // ============================================================

  @Nested
  class UpgradeWithAnotherChange {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND_TM", "APPEND_ML"})
    void lands(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      catalog.createNamespace(mixedNs);
      catalog.createTable(mixed, intSchema);

      catalog.loadTable(mixed)
          .updateProperties()
          .set(org.apache.iceberg.TableProperties.FORMAT_VERSION, "3")
          .set("k", "v")
          .commit();

      Table table = catalog(io, LocalCatalogMode.APPEND).loadTable(mixed);
      assertThat(((org.apache.iceberg.HasTableOperations) table).operations().current()
              .formatVersion())
          .isEqualTo(3);
      assertThat(table.properties()).containsEntry("k", "v");
    }
  }

  // ============================================================
  // Bug 38: an inline delta wrote a new snapshot's schema id only when it
  // was positive and not the base's current schema, and replay took an
  // absent id as the base's current schema. A snapshot written with
  // schema 0 on a base whose current schema is another -- a replace
  // transaction back to a table's first schema -- replayed with the other
  // schema. The schema id is now written whenever it is not the base's
  // current schema, 0 included. Found by reading the delta codec while
  // adding v3 tables to TestCatalogFaultProperties.
  // ============================================================

  @Nested
  class SnapshotOfAReplaceToTheFirstSchema {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND_TM", "APPEND_ML"})
    void keepsItsSchemaId(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      catalog.createNamespace(mixedNs);
      catalog.createTable(mixed, intSchema)
          .updateSchema()
          .addColumn("c", Types.IntegerType.get())
          .commit();

      org.apache.iceberg.Transaction replace =
          catalog.buildTable(mixed, intSchema).replaceTransaction();
      replace.newFastAppend().appendFile(dataFile("a")).commit();
      replace.commitTransaction();

      Table table = catalog(io, LocalCatalogMode.APPEND).loadTable(mixed);
      assertThat(table.schema().schemaId()).isEqualTo(0);
      assertThat(table.currentSnapshot().schemaId()).isEqualTo(0);
    }
  }

  // ============================================================
  // Bug 39: an inline delta carried added and changed refs but not removed
  // ones. A commit that only removed refs left the delta empty, which takes
  // full mode; one with another change lost the removal: a replace
  // transaction that also reset the schema kept the replaced table's
  // current snapshot (its main branch), and a branch removed in a
  // transaction that set a property came back. A commit that removes a
  // ref now takes full mode. Found by TestCatalogFaultProperties with
  // replace transactions.
  // ============================================================

  @Nested
  class RemovedRef {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND_TM", "APPEND_ML"})
    void replaceDropsTheCurrentSnapshot(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      catalog.createNamespace(mixedNs);
      catalog.createTable(mixed, intSchema).newFastAppend().appendFile(dataFile("a")).commit();
      catalog.loadTable(mixed).updateSchema().addColumn("c", Types.IntegerType.get()).commit();

      catalog.buildTable(mixed, intSchema).replaceTransaction().commitTransaction();

      Table table = catalog(io, LocalCatalogMode.APPEND).loadTable(mixed);
      assertThat(table.currentSnapshot()).isNull();
      assertThat(dataFiles(table)).isEmpty();
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND_TM", "APPEND_ML"})
    void removeBranchAndTagWithAProperty(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      catalog.createNamespace(mixedNs);
      Table table = catalog.createTable(mixed, intSchema);
      table.newFastAppend().appendFile(dataFile("a")).commit();
      long snapshot = table.currentSnapshot().snapshotId();
      table.manageSnapshots().createBranch("b", snapshot).createTag("t", snapshot).commit();

      org.apache.iceberg.Transaction txn = catalog.loadTable(mixed).newTransaction();
      txn.manageSnapshots().removeBranch("b").removeTag("t").commit();
      txn.updateProperties().set("k", "v").commit();
      txn.commitTransaction();

      assertThat(catalog(io, LocalCatalogMode.APPEND).loadTable(mixed).refs())
          .containsOnlyKeys("main");
    }
  }

  // ============================================================
  // Bug 40: an inline delta's compact snapshot summary wrote a standard
  // count only when it was not zero, so a reader's summary lacked the
  // zero totals the writer's had ("total-delete-files": "0" and the like),
  // reading them as unknown. The typed fields now hold every count, 0
  // included; an entry they cannot hold exactly (an operation without a
  // name in the codec, a count not written as a plain number) is kept as
  // an extra entry rather than read back as "append" or failing the
  // commit. Found by TestInlineDeltaProperties.
  // ============================================================

  @Nested
  class SnapshotSummaryInADelta {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND_TM", "APPEND_ML"})
    void keepsEveryEntry(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      catalog.createNamespace(mixedNs);
      catalog.createTable(mixed, intSchema);

      catalog.loadTable(mixed)
          .newFastAppend()
          .appendFile(dataFile("a"))
          .commit();

      Map<String, String> summary =
          catalog(io, LocalCatalogMode.APPEND).loadTable(mixed).currentSnapshot().summary();
      assertThat(summary)
          .containsEntry("total-delete-files", "0")
          .containsEntry("total-position-deletes", "0")
          .containsEntry("total-equality-deletes", "0");
    }
  }

  // ============================================================
  // Bug 41: an inline delta stores each new snapshot's timestamp relative
  // to the base's last-updated-ms, and replay resolved it against the
  // metadata as the delta's earlier updates left it, whose last-updated-ms
  // the first new snapshot had moved: a commit's later snapshots replayed
  // at the wrong times. Replay also stamps the snapshot-log entry of a
  // main-branch change with the commit's last-updated-ms, which is not the
  // time main moved in a transaction that changed a branch after it. Both
  // are now exact: timestamps resolve against the delta's base, and a
  // commit whose log entry has another time takes full mode. Found by
  // TestInlineDeltaProperties.
  // ============================================================

  @Nested
  class TimesInACommitOfSeveralChanges {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND_TM", "APPEND_ML"})
    void replayAsWritten(LocalCatalogMode mode) throws InterruptedException {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      catalog.createNamespace(mixedNs);
      catalog.createTable(mixed, intSchema).newFastAppend().appendFile(dataFile("a")).commit();

      org.apache.iceberg.Transaction txn = catalog.loadTable(mixed).newTransaction();
      txn.newFastAppend().appendFile(dataFile("b")).commit();
      Thread.sleep(5);
      txn.newFastAppend().appendFile(dataFile("c")).commit();
      Thread.sleep(5);
      txn.newFastAppend().appendFile(dataFile("d")).toBranch("b").commit();
      org.apache.iceberg.TableMetadata written =
          ((org.apache.iceberg.HasTableOperations) txn.table()).operations().current();
      txn.commitTransaction();

      org.apache.iceberg.TableMetadata read =
          ((org.apache.iceberg.HasTableOperations) catalog(io, LocalCatalogMode.APPEND)
                  .loadTable(mixed))
              .operations()
              .current();
      for (org.apache.iceberg.Snapshot s : written.snapshots()) {
        assertThat(read.snapshot(s.snapshotId()).timestampMillis())
            .as("snapshot %d", s.snapshotId())
            .isEqualTo(s.timestampMillis());
      }
      assertThat(read.snapshotLog()).isEqualTo(written.snapshotLog());
    }
  }

  // ============================================================
  // Bug 42: replay numbers a delta's new snapshots after the base's last
  // sequence number, in order, and advances next-row-id by their added
  // rows; the delta carries neither. A transaction that added a snapshot
  // and expired it before adding another left a gap, so the later
  // snapshot replayed with a sequence number one lower than its manifests
  // carry, and the table's last sequence number and next row id fell
  // behind: the next commit reused them. A commit whose numbers do not
  // follow now takes full mode. Found by TestInlineDeltaProperties.
  // ============================================================

  @Nested
  class SnapshotExpiredInTheTransactionThatAddedIt {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND_TM", "APPEND_ML"})
    void leavesItsNumbersUsed(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      catalog.createNamespace(mixedNs);
      v3(catalog).newFastAppend().appendFile(dataFile("a")).commit();

      org.apache.iceberg.Transaction txn = catalog.loadTable(mixed).newTransaction();
      txn.newFastAppend().appendFile(dataFile("b")).stageOnly().commit();
      long staged =
          ((org.apache.iceberg.HasTableOperations) txn.table()).operations().current()
              .snapshots().stream()
              .mapToLong(org.apache.iceberg.Snapshot::snapshotId)
              .filter(id -> id != txn.table().currentSnapshot().snapshotId())
              .findFirst()
              .getAsLong();
      txn.expireSnapshots().expireSnapshotId(staged).cleanExpiredFiles(false).commit();
      txn.newFastAppend().appendFile(dataFile("c")).commit();
      org.apache.iceberg.TableMetadata written =
          ((org.apache.iceberg.HasTableOperations) txn.table()).operations().current();
      txn.commitTransaction();

      org.apache.iceberg.TableMetadata read =
          ((org.apache.iceberg.HasTableOperations) catalog(io, LocalCatalogMode.APPEND)
                  .loadTable(mixed))
              .operations()
              .current();
      assertThat(read.currentSnapshot().sequenceNumber())
          .isEqualTo(written.currentSnapshot().sequenceNumber());
      assertThat(read.lastSequenceNumber()).isEqualTo(written.lastSequenceNumber());
      assertThat(read.nextRowId()).isEqualTo(written.nextRowId());
    }
  }

  // ============================================================
  // Bug 43: the structural inline codec kept a metadata-log path as a
  // shared prefix plus a version number, and a number-like snapshot
  // summary value as an integer, rebuilding both from the number: a
  // prefix that took some of a zero-padded version's zeros ("00009" and
  // "00010" share "000") lost one, so a table that returned inline listed
  // metadata files that do not exist; and a summary value "007" read
  // back as "7". The templates are now used only if they rebuild every
  // path exactly (versions keep their padding width), and a value is an
  // integer only if it reads back as the same text. Found by
  // TestInlineDeltaProperties.
  // ============================================================

  @Nested
  class StructuralCodecRebuildsWhatItStores {

    @Test
    void metadataLogOfATableReturningInline() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog pointer = catalog(io, LocalCatalogMode.APPEND);
      FileIOCatalog inline = catalog(io, LocalCatalogMode.APPEND_TM);
      pointer.createNamespace(mixedNs);
      pointer.createTable(mixed, intSchema);
      for (int i = 0; i < 11; i++) {
        pointer.loadTable(mixed).updateProperties().set("k", "v" + i).commit();
      }

      inline.loadTable(mixed).updateProperties().set("k", "inline").commit();

      assertThat(catalogFile(io).isInlineTable(mixed)).isTrue();
      List<org.apache.iceberg.TableMetadata.MetadataLogEntry> log =
          ((org.apache.iceberg.HasTableOperations) inline.loadTable(mixed)).operations()
              .current().previousFiles();
      assertThat(log).hasSizeGreaterThan(10);
      for (org.apache.iceberg.TableMetadata.MetadataLogEntry entry : log) {
        assertThat(io.newInputFile(entry.file()).exists()).as(entry.file()).isTrue();
      }
    }

    @Test
    void summaryValueThatLooksLikeANumber() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog inline = catalog(io, LocalCatalogMode.APPEND_TM);
      inline.createNamespace(mixedNs);
      inline.createTable(mixed, intSchema);

      inline.loadTable(mixed).newFastAppend().appendFile(dataFile("a")).set("custom", "007")
          .commit();

      assertThat(catalog(io, LocalCatalogMode.APPEND).loadTable(mixed).currentSnapshot()
              .summary())
          .containsEntry("custom", "007");
    }
  }

  // ============================================================
  // Bug 44: snapshot expiry deletes the manifests of expired snapshots
  // that no retained snapshot lists. With more refs than main, Iceberg's
  // ReachableFileCleanup finds those by removing each retained snapshot's
  // manifests from a set of candidates, comparing ManifestFile objects;
  // GenericManifestFile is equal by path, but an inline snapshot's
  // manifests were the catalog's own DecodedManifestFile, equal only to
  // itself. So no inline manifest was ever found in use: expiry deleted
  // every manifest an expired snapshot shared with a retained inline-ML
  // snapshot, and the table could no longer be read. Inline snapshots now
  // hold GenericManifestFile copies. Found by TestCatalogFaultProperties.
  // ============================================================

  @Nested
  class ExpiryOfATableWithATag {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND_ML", "APPEND_TM", "APPEND"})
    void keepsTheManifestsOfRetainedSnapshots(LocalCatalogMode expiring) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog ml = catalog(io, LocalCatalogMode.APPEND_ML);
      ml.createNamespace(mixedNs);
      Table table = ml.createTable(mixed, intSchema);
      table.newFastAppend().appendFile(dataFile("a")).commit();
      long first = table.currentSnapshot().snapshotId();
      table.newFastAppend().appendFile(dataFile("b")).commit();
      table.manageSnapshots().createTag("t", table.currentSnapshot().snapshotId()).commit();

      catalog(io, expiring).loadTable(mixed).expireSnapshots().expireSnapshotId(first).commit();

      Table reloaded = ml.loadTable(mixed);
      for (org.apache.iceberg.ManifestFile m : reloaded.currentSnapshot().allManifests(io)) {
        assertThat(io.newInputFile(m.path()).exists()).as(m.path()).isTrue();
      }
      assertThat(dataFiles(reloaded))
          .containsExactly("mem:///data/a.parquet", "mem:///data/b.parquet");
    }
  }

  // ============================================================
  // Bug 45: the catalog decoded an inline manifest's partition summaries
  // as its own PartitionFieldSummary class, and Iceberg's Avro manifest
  // list writer takes only its GenericPartitionFieldSummary records. So a
  // manifest list file could not be written with a manifest of a
  // partitioned inline-ML table: a writer that does not inline could not
  // commit to the table (ClassCastException), nor could the table leave
  // inline storage. The summaries are now decoded as
  // GenericPartitionFieldSummary. Found by TestCatalogFaultProperties
  // with partition-spec changes.
  // ============================================================

  @Nested
  class PartitionedInlineManifestLists {

    private final org.apache.iceberg.PartitionSpec byId =
        org.apache.iceberg.PartitionSpec.builderFor(intSchema).identity("id").build();

    private org.apache.iceberg.DataFile partitioned(String name) {
      return org.apache.iceberg.DataFiles.builder(byId)
          .withPath("mem:///data/" + name + ".parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .withPartitionPath("id=1")
          .build();
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND", "APPEND_TM"})
    void takeACommitFromAWriterThatDoesNotInlineThem(LocalCatalogMode writer) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog ml = catalog(io, LocalCatalogMode.APPEND_ML);
      ml.createNamespace(mixedNs);
      ml.buildTable(mixed, intSchema).withPartitionSpec(byId).create()
          .newFastAppend().appendFile(partitioned("a")).commit();

      catalog(io, writer).loadTable(mixed).newFastAppend().appendFile(partitioned("b")).commit();

      assertThat(dataFiles(ml.loadTable(mixed)))
          .containsExactly("mem:///data/a.parquet", "mem:///data/b.parquet");
    }

    @Test
    void leaveInlineStorage() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog ml = small(io, LocalCatalogMode.APPEND_ML);
      ml.createNamespace(mixedNs);
      ml.buildTable(mixed, intSchema).withPartitionSpec(byId).create()
          .newFastAppend().appendFile(partitioned("a")).commit();

      ml.loadTable(mixed).updateProperties().set("big", bigValue(6000)).commit();

      assertThat(catalogFile(io).isInlineTable(mixed)).as("evicted").isFalse();
      assertThat(dataFiles(ml.loadTable(mixed))).containsExactly("mem:///data/a.parquet");
    }
  }

  // ============================================================
  // Bug 46: an inline delta has no update for a v3 table's encryption keys,
  // and computeDelta did not compare them, so a key added or removed with
  // another change in the same commit was lost on replay (a commit that
  // changed only keys left the delta empty, which takes full mode). A
  // commit that changes the keys now takes full mode. Found by reading
  // computeDelta while adding TestInlineDeltaProperties; FileIOCatalog has
  // no table encryption of its own, so this replays the delta directly.
  // ============================================================

  @Nested
  class EncryptionKeysInADelta {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void replay(boolean structural) {
      InlineMetadataCodec codec =
          structural ? InlineMetadataCodecs.STRUCTURAL : InlineMetadataCodecs.JSON_GZIP;
      org.apache.iceberg.TableMetadata base =
          codec.decodeFull(
              codec.encode(
                  org.apache.iceberg.TableMetadata.newTableMetadata(
                      intSchema, org.apache.iceberg.PartitionSpec.unpartitioned(),
                      org.apache.iceberg.SortOrder.unsorted(), "mem:///w/ns/t",
                      Map.of("format-version", "3"))),
              "inline://base");
      org.apache.iceberg.TableMetadata next =
          org.apache.iceberg.TableMetadata.buildFrom(base)
              .addEncryptionKey(
                  new org.apache.iceberg.encryption.BaseEncryptedKey(
                      "k1", java.nio.ByteBuffer.wrap(new byte[] {1, 2, 3}), "kek", Map.of()))
              .setProperties(Map.of("k", "v"))
              .build();

      List<InlineDeltaCodec.DeltaUpdate> delta = InlineDeltaCodec.computeDelta(base, next, "");

      if (delta != null) {
        ProtoCatalogFile.Builder builder =
            ProtoCatalogFile.builder(new InMemoryInputFile("mem:///stub", new byte[0]))
                .addInlineTable(1, 0, "t", 1, codec.encode(base), codec.tag(), "");
        ProtoCodec.UpdateTableInlineAction.delta(
                1, 1, InlineDeltaCodec.encodeDelta(delta, next.lastUpdatedMillis()))
            .apply(builder);
        org.apache.iceberg.TableMetadata replayed =
            codec.decodeFull(builder.inlineMetadata(1), "inline://replayed");
        assertThat(replayed.encryptionKeys()).hasSize(1);
      }
    }
  }

  // ============================================================
  // Bug 47: dropTable returned false, which Catalog defines as "the table
  // did not exist", when its record conflicted with a concurrent commit to
  // the table (the record names the version it read). A drop does not
  // depend on the table's state, so it now retries on a fresh read until
  // it lands or the table is gone. Found by reading dropTable.
  // ============================================================

  @Nested
  class DropRacingACommit {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND", "CAS", "APPEND_TM"})
    void dropsTheTable(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog dropping = catalog(io, mode);
      FileIOCatalog other = catalog(io, mode);
      dropping.createNamespace(mixedNs);
      dropping.createTable(mixed, intSchema);
      boolean[] raced = {false};
      io.injectFaults((location, strategy) -> {
        if (!raced[0] && location.equals(CATALOG)) {
          raced[0] = true; // just before the drop's write, another writer commits to the table
          other.loadTable(mixed).updateProperties().set("k", "v").commit();
        }
        return Fault.NONE;
      });

      boolean dropped = dropping.dropTable(mixed, false);

      io.injectFaults((location, strategy) -> Fault.NONE);
      assertThat(raced[0]).isTrue();
      assertThat(dropped).isTrue();
      assertThat(other.tableExists(mixed)).isFalse();
    }
  }

  // ============================================================
  // Bug 48: dropTable ignored purge, so a purged table's data files,
  // manifests, manifest lists and metadata files stayed in storage. A purge
  // now deletes them once the drop has landed, as BaseMetastoreCatalog
  // does (through CatalogUtil.dropTableData, skipping an inline table's
  // inline:// pseudo-locations). Found by reading dropTable.
  // ============================================================

  @Nested
  class DropWithPurge {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND", "APPEND_TM", "APPEND_ML"})
    void deletesTheTablesFiles(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      catalog.createNamespace(mixedNs);
      Table table = catalog.createTable(mixed, intSchema);
      table.newFastAppend().appendFile(dataFile("a")).commit();
      table.newFastAppend().appendFile(dataFile("b")).commit();
      Table loaded = catalog.loadTable(mixed);
      List<String> files = Lists.newArrayList();
      for (org.apache.iceberg.Snapshot s : loaded.snapshots()) {
        s.allManifests(io).forEach(m -> files.add(m.path()));
        if (s.manifestListLocation() != null) {
          files.add(s.manifestListLocation());
        }
      }
      org.apache.iceberg.TableMetadata metadata =
          ((org.apache.iceberg.HasTableOperations) loaded).operations().current();
      if (!metadata.metadataFileLocation().startsWith("inline://")) {
        files.add(metadata.metadataFileLocation());
      }
      assertThat(files).isNotEmpty().allMatch(f -> io.newInputFile(f).exists());

      assertThat(catalog.dropTable(mixed, true)).isTrue();

      assertThat(files).noneMatch(f -> io.newInputFile(f).exists());
    }
  }

  // ============================================================
  // Bug 49: replay of an inline delta built table metadata after each of its
  // updates, and TableMetadata.Builder.build checks the default partition
  // spec and sort order against the current schema. The writer applies all
  // of a commit's changes before one build, so a commit that changed the
  // schema and the spec together -- a replace transaction to a schema
  // without the column the old spec partitioned by -- left an intermediate
  // state that does not validate: replay threw, and no reader could load
  // the catalog. Replay now applies a delta's consecutive metadata updates
  // to one builder and builds it once. Found by TestInlineDeltaProperties.
  // ============================================================

  @Nested
  class ReplaceToASchemaWithoutThePartitionColumn {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND_TM", "APPEND_ML"})
    void replays(LocalCatalogMode mode) {
      Schema withTs =
          new Schema(
              Types.NestedField.required(1, "id", Types.IntegerType.get()),
              Types.NestedField.optional(2, "ts", Types.TimestampType.withZone()));
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      catalog.createNamespace(mixedNs);
      catalog.buildTable(mixed, withTs)
          .withPartitionSpec(org.apache.iceberg.PartitionSpec.builderFor(withTs).day("ts").build())
          .create();

      catalog.buildTable(mixed, intSchema).replaceTransaction().commitTransaction();

      Table table = catalog(io, LocalCatalogMode.APPEND).loadTable(mixed);
      assertThat(table.schema().asStruct()).isEqualTo(intSchema.asStruct());
      assertThat(table.spec().isUnpartitioned()).isTrue();
    }
  }

  // ============================================================
  // Bug 50: an inline delta sent a new snapshot's list as the manifests it
  // adds to and removes from its parent's, and replay appended the added
  // ones to the parent's list. Iceberg lists a fast append's new manifest
  // first, so readers of an inline-ML table saw each list in another order
  // than its writer -- and than a pointer catalog -- which changes which
  // manifests a merge append later bins together. Each added manifest now
  // carries its position, and a snapshot whose kept manifests are not in
  // its parent's order replaces the list whole. Found by
  // TestInlineDifferentialProperties.
  // ============================================================

  @Nested
  class InlineManifestListOrder {

    @Test
    void isTheWritersOrder() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog ml = catalog(io, LocalCatalogMode.APPEND_ML);
      ml.createNamespace(mixedNs);
      Table table = ml.createTable(mixed, intSchema);
      table.newFastAppend().appendFile(dataFile("a")).commit();
      table.newFastAppend().appendFile(dataFile("b")).commit();
      List<Long> written =
          Lists.transform(table.currentSnapshot().allManifests(io),
              org.apache.iceberg.ManifestFile::snapshotId);

      Table read = catalog(io, LocalCatalogMode.APPEND).loadTable(mixed);

      assertThat(Lists.transform(read.currentSnapshot().allManifests(io),
              org.apache.iceberg.ManifestFile::snapshotId))
          .containsExactlyElementsOf(written)
          .first()
          .isEqualTo(read.currentSnapshot().snapshotId());
    }
  }

  // ============================================================
  // Bug 51: a manifest list entry's partition summaries were not written
  // when there were none, and decoded as null either way, so an
  // unpartitioned manifest, which lists none, read back as one with no
  // summaries (null), as a v1 manifest has. The entry now marks a null
  // list, and an empty one reads back empty. Found by
  // TestInlineDifferentialProperties.
  // ============================================================

  @Nested
  class UnpartitionedInlineManifest {

    @Test
    void hasAnEmptyListOfSummaries() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog ml = catalog(io, LocalCatalogMode.APPEND_ML);
      ml.createNamespace(mixedNs);
      ml.createTable(mixed, intSchema).newFastAppend().appendFile(dataFile("a")).commit();

      Table read = catalog(io, LocalCatalogMode.APPEND).loadTable(mixed);

      assertThat(read.currentSnapshot().allManifests(io))
          .allSatisfy(m -> assertThat(m.partitions()).isNotNull().isEmpty());
    }
  }

  private Table v3(FileIOCatalog catalog) {
    return catalog
        .buildTable(mixed, intSchema)
        .withProperty(org.apache.iceberg.TableProperties.FORMAT_VERSION, "3")
        .create();
  }

  private static Map<String, Long> rowIds(Table table) {
    Map<String, Long> ids = new HashMap<>();
    try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
        table.newScan().planFiles()) {
      tasks.forEach(task -> ids.put(task.file().location(), task.file().firstRowId()));
    } catch (IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
    return ids;
  }
}
