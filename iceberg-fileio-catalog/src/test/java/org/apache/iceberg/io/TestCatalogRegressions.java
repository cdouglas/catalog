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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.CatalogTransaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.io.InMemoryAtomicFileIO.Fault;
import org.apache.iceberg.io.InMemoryAtomicFileIO.ReadFault;
import org.intentlog.CommittedSet;
import org.intentlog.DedupHorizonExceededException;
import org.intentlog.Replica;
import org.intentlog.wire.UuidV7;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
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
    props.put(LogCatalogProperties.MAX_APPEND_COUNT, String.valueOf(maxAppendCount));
    props.putAll(extra);
    FileIOCatalog c = new FileIOCatalog("test", wh + "/catalog", io, new HashMap<>());
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
  // every namespace op, so these tests drive the catalog's log directly: both
  // writers mutate the same snapshot, then commit in sequence, as two
  // concurrent processes would.
  // ============================================================

  private static final String RACE_LOC = "mem:///wh/catalog";
  private static final int APPEND = 10_000;

  private static CatalogLog log(SupportsAtomicOperations io, int maxAppendCount) {
    return CatalogTestSupport.log(
        io, RACE_LOC, Map.of(LogCatalogProperties.MAX_APPEND_COUNT, String.valueOf(maxAppendCount)));
  }

  /** Commits {@code namespaces} and returns the snapshot both racing writers start from. */
  private static Replica<CatalogState> seed(CatalogLog log, String... namespaces) {
    CatalogMutation mut = new CatalogMutation(log.empty());
    for (String ns : namespaces) {
      mut.createNamespace(Namespace.of(ns));
    }
    mut.commit(log);
    return log.read();
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
      CatalogLog log = log(io, APPEND);
      Replica<CatalogState> snap = seed(log, "a", "b");
      TableIdentifier t1 = TableIdentifier.of("a", "t1");
      TableIdentifier t2 = TableIdentifier.of("b", "t2");

      new CatalogMutation(snap).createTable(t1, "mem:///a/t1/v1.metadata.json").commit(log);
      new CatalogMutation(snap).createTable(t2, "mem:///b/t2/v1.metadata.json").commit(log);

      Replica<CatalogState> result = log.read();
      assertThat(result.state().tables()).containsExactlyInAnyOrder(t1, t2);
      assertThat(result.state().location(t1)).isEqualTo("mem:///a/t1/v1.metadata.json");
      assertThat(result.state().location(t2)).isEqualTo("mem:///b/t2/v1.metadata.json");
      assertThat(result.state().tableId(t1)).isNotEqualTo(result.state().tableId(t2));
    }

    @Test
    void appendCreatesWithNestedReferencesBindToTheirOwnParents() {
      // Each writer creates a namespace, a child namespace, and a table in the
      // child, all in one transaction: the child and the table reference ids
      // allocated in the same transaction.
      CasFileIO io = new CasFileIO();
      CatalogLog log = log(io, APPEND);
      Replica<CatalogState> snap = seed(log, "a", "b");
      TableIdentifier at = TableIdentifier.of("a", "p", "c", "t");
      TableIdentifier bt = TableIdentifier.of("b", "q", "c", "t");

      new CatalogMutation(snap)
          .createNamespace(Namespace.of("a", "p"))
          .createNamespace(Namespace.of("a", "p", "c"))
          .createTable(at, "mem:///a/p/c/t/v1.metadata.json")
          .commit(log);
      new CatalogMutation(snap)
          .createNamespace(Namespace.of("b", "q"))
          .createNamespace(Namespace.of("b", "q", "c"))
          .createTable(bt, "mem:///b/q/c/t/v1.metadata.json")
          .commit(log);

      Replica<CatalogState> result = log.read();
      assertThat(result.state().namespaces())
          .containsExactlyInAnyOrder(
              Namespace.of("a"), Namespace.of("a", "p"), Namespace.of("a", "p", "c"),
              Namespace.of("b"), Namespace.of("b", "q"), Namespace.of("b", "q", "c"));
      assertThat(result.state().tables()).containsExactlyInAnyOrder(at, bt);
      assertThat(result.state().location(at)).isEqualTo("mem:///a/p/c/t/v1.metadata.json");
      assertThat(result.state().location(bt)).isEqualTo("mem:///b/q/c/t/v1.metadata.json");
    }

    @ParameterizedTest(name = "maxAppendCount={0}")
    @ValueSource(ints = {0, APPEND})
    void acknowledgedCreatesAreNeverLost(int maxAppendCount) {
      // The invariant behind the tests above, for both commit paths: a writer
      // whose commit returned normally must find its create in the catalog.
      // The CAS path may reject the second writer; it must not drop the first.
      CasFileIO io = new CasFileIO();
      CatalogLog log = log(io, maxAppendCount);
      Replica<CatalogState> snap = seed(log, "a", "b");
      Map<TableIdentifier, String> acknowledged = new HashMap<>();
      Map<TableIdentifier, String> creates =
          Map.of(
              TableIdentifier.of("a", "t1"), "mem:///a/t1/v1.metadata.json",
              TableIdentifier.of("b", "t2"), "mem:///b/t2/v1.metadata.json");

      for (Map.Entry<TableIdentifier, String> create : creates.entrySet()) {
        try {
          new CatalogMutation(snap).createTable(create.getKey(), create.getValue()).commit(log);
          acknowledged.put(create.getKey(), create.getValue());
        } catch (CommitFailedException e) {
          // Rejected, not acknowledged: nothing to check for this writer.
        }
      }

      Replica<CatalogState> result = log.read();
      assertThat(acknowledged).isNotEmpty();
      acknowledged.forEach(
          (ident, location) -> assertThat(result.state().location(ident)).as("%s", ident).isEqualTo(location));
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
      CatalogLog log = log(io, APPEND);
      Replica<CatalogState> snap = seed(log, "seed");

      new CatalogMutation(snap).createNamespace(Namespace.of("x")).commit(log);
      assertThatThrownBy(() -> new CatalogMutation(snap).createNamespace(Namespace.of("y")).commit(log))
          .isInstanceOf(CommitFailedException.class);

      assertThat(log.read().state().namespaces())
          .containsExactlyInAnyOrder(Namespace.of("seed"), Namespace.of("x"));
    }

    @Test
    void appendCreatesOfSameRootNamespaceConflict() {
      CasFileIO io = new CasFileIO();
      CatalogLog log = log(io, APPEND);
      Replica<CatalogState> snap = seed(log, "seed");

      new CatalogMutation(snap).createNamespace(Namespace.of("x")).commit(log);
      assertThatThrownBy(() -> new CatalogMutation(snap).createNamespace(Namespace.of("x")).commit(log))
          .isInstanceOfAny(AlreadyExistsException.class, CommitFailedException.class);

      Replica<CatalogState> result = log.read();
      assertThat(result.state().namespaceById().values().stream().filter(e -> e.name.equals("x")))
          .hasSize(1);
    }

    @Test
    void droppingTopLevelNamespaceConflictsWithTopLevelCreate() {
      CasFileIO io = new CasFileIO();
      CatalogLog log = log(io, APPEND);
      Replica<CatalogState> snap = seed(log, "a");

      new CatalogMutation(snap).dropNamespace(Namespace.of("a")).commit(log);
      assertThatThrownBy(() -> new CatalogMutation(snap).createNamespace(Namespace.of("b")).commit(log))
          .isInstanceOf(CommitFailedException.class);
    }

    @Test
    void rootVersionSurvivesCompaction() {
      // CAS-only: every commit writes a fresh checkpoint, so the root version
      // must round-trip through the checkpoint encoding.
      CasFileIO io = new CasFileIO();
      CatalogLog log = log(io, 0);
      seed(log, "a");
      new CatalogMutation(log.read()).createNamespace(Namespace.of("b")).commit(log);
      new CatalogMutation(log.read()).createNamespace(Namespace.of("c")).commit(log);

      assertThat(log.read().state().namespaceVersion(CatalogState.ROOT_ID)).isEqualTo(3);
    }

    @Test
    void rootIsNotAddressableByName() {
      CasFileIO io = new CasFileIO();
      FileIOCatalog cat = catalog(io);
      cat.createNamespace(Namespace.of("a"));

      Replica<CatalogState> file = log(io, 0).read();
      assertThat(file.state().namespaceEntry(CatalogState.ROOT_ID)).isNotNull();
      assertThat(file.state().namespaces()).containsExactly(Namespace.of("a"));
      assertThat(cat.namespaceExists(Namespace.empty())).isFalse();
      assertThat(cat.listNamespaces()).containsExactly(Namespace.of("a"));
    }

    @Test
    void replayNeverDropsOrReplacesTheRoot() {
      CatalogState.Builder builder =
          CatalogState.builder();
      int rootVersion = builder.namespaceVersion(CatalogState.ROOT_ID);

      assertThat(new ProtoCodec.DropNamespaceAction(CatalogState.ROOT_ID, rootVersion)
              .verify(builder))
          .isFalse();
      assertThat(new ProtoCodec.CreateNamespaceAction(CatalogState.ROOT_ID, 1, 0, -1, "x")
              .verify(builder))
          .isFalse();
      assertThatThrownBy(() -> builder.removeNamespace(CatalogState.ROOT_ID))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void tableInRootIsRejected() {
      CasFileIO io = new CasFileIO();
      CatalogLog log = log(io, APPEND);
      Replica<CatalogState> snap = seed(log, "a");

      assertThatThrownBy(
              () -> new CatalogMutation(snap).createTable(TableIdentifier.of("t"), "mem:///t/v1.metadata.json"))
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
      Replica<CatalogState> snap = seed(log(io, APPEND), "a", "b");
      // A small transaction fits under the size limit; the winner's large one
      // does not, so the loser's retry sees a file that must be compacted.
      CatalogLog log = CatalogTestSupport.log(io, RACE_LOC, APPEND, snap.tail().bytes() + 1_000);
      TableIdentifier winner = TableIdentifier.of("a", "t1");
      TableIdentifier loser = TableIdentifier.of("b", "t2");
      String winnerLocation = "mem:///a/t1/" + "x".repeat(4_000) + ".metadata.json";

      new CatalogMutation(snap).createTable(winner, winnerLocation).commit(log);
      try {
        new CatalogMutation(snap).createTable(loser, "mem:///b/t2/v1.metadata.json").commit(log);
      } catch (CommitFailedException e) {
        // Rejecting the loser is fine; dropping the winner is not.
      }

      assertThat(log.read().state().location(winner)).isEqualTo(winnerLocation);
    }

    @Test
    void compactionAfterRaceWithCompactionKeepsTheLog() {
      // The loser's snapshot holds a large namespace that is then dropped and
      // compacted away, so the loser's retry finds a shorter file: it re-reads
      // the record count, which forces a CAS.
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      CatalogLog log = log(io, 3);
      seed(log, "a", "b");
      new CatalogMutation(log.read())
          .createNamespace(Namespace.of("big"), Map.of("k", "x".repeat(4_000)))
          .commit(log);
      Replica<CatalogState> snap = log.read();

      new CatalogMutation(log.read()).dropNamespace(Namespace.of("big")).commit(log);
      for (String ns : new String[] {"y", "z", "v"}) {
        new CatalogMutation(log.read()).createNamespace(Namespace.of("a", ns)).commit(log);
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
        new CatalogMutation(snap).createNamespace(Namespace.of("b", "w")).commit(log);
        expected.add(Namespace.of("b", "w"));
      } catch (CommitFailedException e) {
        // Rejecting the loser is fine; resurrecting "big" or dropping the
        // namespaces committed since its snapshot is not.
      }

      assertThat(log.read().state().namespaces()).containsExactlyInAnyOrderElementsOf(expected);
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

    private CatalogMutation txn() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      CatalogLog log = log(io, APPEND);
      CatalogMutation seed = new CatalogMutation(log.empty());
      seed.createNamespace(Namespace.of("a"));
      seed.createNamespace(Namespace.of("b"));
      seed.createTable(at, "mem:///a/t/v1.metadata.json");
      seed.commit(log);
      return new CatalogMutation(log.read());
    }

    @Test
    void renamedTableCannotBeDroppedUnderItsOldName() {
      CatalogMutation txn = txn();
      txn.renameTable(at, bt);
      assertThatThrownBy(() -> txn.dropTable(at)).isInstanceOf(NoSuchTableException.class);
    }

    @Test
    void renamedTableCannotBeRenamedAgainUnderItsOldName() {
      CatalogMutation txn = txn();
      txn.renameTable(at, bt);
      assertThatThrownBy(() -> txn.renameTable(at, TableIdentifier.of("b", "u")))
          .isInstanceOf(NoSuchTableException.class);
    }

    @Test
    void renamedTableCannotBeUpdatedUnderItsOldName() {
      CatalogMutation txn = txn();
      txn.renameTable(at, bt);
      assertThatThrownBy(() -> txn.updateTable(at, "mem:///a/t/v2.metadata.json"))
          .isInstanceOf(NoSuchTableException.class);
    }

    @Test
    void renameDestinationCannotBeCreated() {
      CatalogMutation txn = txn();
      txn.renameTable(at, bt);
      assertThatThrownBy(() -> txn.createTable(bt, "mem:///b/t/v1.metadata.json"))
          .isInstanceOf(AlreadyExistsException.class);
    }

    @Test
    void namespaceWithRenamedInTableCannotBeDropped() {
      CatalogMutation txn = txn();
      txn.renameTable(at, bt);
      assertThatThrownBy(() -> txn.dropNamespace(Namespace.of("b")))
          .isInstanceOf(NamespaceNotEmptyException.class);
    }

    @Test
    void droppedNamespaceCannotReceiveTables() {
      CatalogMutation txn = txn();
      txn.dropNamespace(Namespace.of("b"));
      assertThatThrownBy(() -> txn.createTable(bt, "mem:///b/t/v1.metadata.json"))
          .isInstanceOf(NoSuchNamespaceException.class);
      assertThatThrownBy(() -> txn.renameTable(at, bt))
          .isInstanceOf(NoSuchNamespaceException.class);
    }

    @Test
    void droppedNamespaceCannotTakeProperties() {
      CatalogMutation txn = txn();
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
      CatalogLog log = log(io, APPEND);
      CatalogMutation txn = new CatalogMutation(seed(log, "a", "b"));
      txn.dropNamespace(Namespace.of("b"));
      assertThatThrownBy(() -> txn.createNamespace(Namespace.of("b", "x")))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("marked for deletion");
    }

    @Test
    void replayRejectsARecordThatDropsWhatItReferences() throws IOException {
      // Bypass the Mut: append the record it used to write.
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      CatalogLog log = log(io, APPEND);
      Replica<CatalogState> snap = seed(log, "a", "b");
      int b = snap.state().namespaceId(Namespace.of("b"));
      int version = snap.state().namespaceVersion(b);
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

      Replica<CatalogState> result = log.read();
      assertThat(result.committed().contains(txn.id())).isFalse();
      assertThat(result.state().namespaces()).containsExactlyInAnyOrder(Namespace.of("a"), Namespace.of("b"));
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
      CatalogLog log = log(io, APPEND);
      seed(log, "a", "b");
      new CatalogMutation(log.read()).createNamespace(Namespace.of("a", "x")).commit(log);

      CatalogMutation txn = new CatalogMutation(log.read());
      txn.dropNamespace(Namespace.of("a", "x"));
      txn.dropNamespace(Namespace.of("a"));
      txn.commit(log);

      assertThat(log.read().state().namespaces()).containsExactly(Namespace.of("b"));
    }

    @ParameterizedTest(name = "parentFirst={0}")
    @ValueSource(booleans = {true, false})
    void replayAppliesTheDropsInEitherOrder(boolean parentFirst) {
      CatalogState.Builder builder =
          CatalogState.builder();
      builder.addNamespace(1, CatalogState.ROOT_ID, "a", 1);
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

      CatalogState result = builder.build();
      assertThat(result.namespaces()).isEmpty();
      assertThat(result.tables()).isEmpty();
    }

    @Test
    void tableRemovedAfterItsNamespace() {
      CatalogState.Builder builder =
          CatalogState.builder();
      builder.addNamespace(1, CatalogState.ROOT_ID, "a", 1);
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
      CatalogLog log = log(io, APPEND);
      CatalogMutation txn = new CatalogMutation(seed(log, "a"));
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
      CatalogLog log = log(io, 0);
      Replica<CatalogState> snap = seed(log, "a");
      int writes = io.history().size();

      failNext(io, Fault.LOST_REPLY);
      CatalogMutation mut = new CatalogMutation(snap);
      mut.createNamespace(Namespace.of("c"));
      mut.commit(log);

      assertThat(io.history()).hasSize(writes + 1); // landed once, not written again
      assertThat(log.read().state().namespaces()).contains(Namespace.of("c"));
    }

    @Test
    void appendWhoseReplyWasLostIsCommitted() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      CatalogLog log = log(io, 10_000);
      Replica<CatalogState> snap = seed(log, "a");
      int writes = io.history().size();

      failNext(io, Fault.LOST_REPLY_RUNTIME);
      CatalogMutation mut = new CatalogMutation(snap);
      mut.createNamespace(Namespace.of("c"));
      mut.commit(log);

      assertThat(io.history()).hasSize(writes + 1);
      assertThat(io.history().get(writes).strategy()).isEqualTo(AtomicOutputFile.Strategy.APPEND);
      assertThat(log.read().state().namespaces()).contains(Namespace.of("c"));
    }

    @Test
    void writeThatFailedBeforeLandingIsRetried() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      CatalogLog log = log(io, 10_000);
      Replica<CatalogState> snap = seed(log, "a");
      int writes = io.history().size();

      failNext(io, Fault.FAIL_BEFORE, Fault.FAIL_BEFORE);
      CatalogMutation mut = new CatalogMutation(snap);
      mut.createNamespace(Namespace.of("c"));
      mut.commit(log);

      assertThat(io.history()).hasSize(writes + 1);
      assertThat(log.read().state().namespaces()).contains(Namespace.of("c"));
    }

    @Test
    void outcomeThatNeverResolvesIsCommitStateUnknown() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      CatalogLog log = log(io, 0);
      Replica<CatalogState> snap = seed(log, "a");
      int writes = io.history().size();

      io.injectFaults((location, strategy) -> Fault.FAIL_BEFORE);
      CatalogMutation mut = new CatalogMutation(snap);
      mut.createNamespace(Namespace.of("c"));
      // Nothing landed, but the commit cannot know that: every write might yet.
      assertThatThrownBy(() -> mut.commit(log)).isInstanceOf(CommitStateUnknownException.class);

      io.injectFaults((location, strategy) -> Fault.NONE);
      assertThat(io.history()).hasSize(writes);
      assertThat(log.read().state().namespaces()).doesNotContain(Namespace.of("c"));
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
      CatalogLog log = log(io, maxAppendCount);
      Replica<CatalogState> snap = seed(log, "a");

      landFirstAsConflict(io, false);
      new CatalogMutation(snap).createNamespace(Namespace.of("c")).commit(log);

      healed(io);
      assertThat(log.read().state().namespaces()).contains(Namespace.of("c"));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 10_000})
    void unreadableCatalogAfterAConflictThatLandedIsCommitStateUnknown(int maxAppendCount) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      CatalogLog log = log(io, maxAppendCount);
      Replica<CatalogState> snap = seed(log, "a");

      landFirstAsConflict(io, true);
      assertThatThrownBy(() -> new CatalogMutation(snap).createNamespace(Namespace.of("c")).commit(log))
          .isInstanceOf(CommitStateUnknownException.class);

      healed(io);
      assertThat(log.read().state().namespaces()).contains(Namespace.of("c"));
    }

    @Test
    void catalogCreationReportedAsConflictThatLandedIsCommitted() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      CatalogLog log = log(io, 0);

      landFirstAsConflict(io, false);
      new CatalogMutation(log.empty()).createNamespace(Namespace.of("a")).commit(log);

      healed(io);
      assertThat(log.read().state().namespaces()).contains(Namespace.of("a"));
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
      LogCatalog catalog = mode.catalog(new InMemoryAtomicFileIO(), "mem:///w", "c", Map.of());
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
      LogCatalog first = mode.catalog(io, "mem:///w", "c", Map.of());
      LogCatalog second = mode.catalog(io, "mem:///w", "c", Map.of());
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
      LogCatalog catalog = mode.catalog(new InMemoryAtomicFileIO(), "mem:///w", "c", Map.of());
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
      LogCatalog catalog = mode.catalog(new InMemoryAtomicFileIO(), "mem:///w", "c", Map.of());
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
      LogCatalog first = mode.catalog(io, "mem:///w", "c", Map.of());
      LogCatalog second = mode.catalog(io, "mem:///w", "c", Map.of());
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
      // writer changes the read table just before that last read: a read of the catalog object,
      // or of the shared log.
      int[] reads = {0};
      Thread committer = Thread.currentThread();
      Runnable race = () -> {
        if (Thread.currentThread() == committer && reads[0]++ == 2) {
          second.loadTable(read).newFastAppend().appendFile(file("r1")).commit();
        }
      };
      if (mode.sharedLog) {
        LocalCatalogMode.sharedLog(io, "mem:///w/catalog-c").intercept(
            new org.intentlog.memory.InMemorySharedLog.Interceptor() {
              @Override
              public org.intentlog.memory.InMemorySharedLog.ReadShape beforeRead(
                  org.intentlog.memory.InMemorySharedLog.Request request) {
                if (request == org.intentlog.memory.InMemorySharedLog.Request.READ) {
                  race.run();
                }
                return org.intentlog.memory.InMemorySharedLog.ReadShape.NORMAL;
              }
            });
      } else {
        io.injectReadFaults(
            location -> {
              if (location.equals("mem:///w/catalog-c")) {
                race.run();
              }
              return ReadFault.NONE;
            });
      }
      assertThatThrownBy(txn::commitTransaction)
          .isInstanceOf(org.apache.iceberg.exceptions.ValidationException.class);
      io.injectReadFaults(location -> ReadFault.NONE);
      if (mode.sharedLog) {
        LocalCatalogMode.sharedLog(io, "mem:///w/catalog-c").intercept(null);
      }

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
      LogCatalog catalog = mode.catalog(new InMemoryAtomicFileIO(), "mem:///w", "c", Map.of());
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
      LogCatalog catalog = mode.catalog(new InMemoryAtomicFileIO(), "mem:///w", "c", Map.of());
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

    private CatalogLog log(SupportsAtomicOperations io, int maxAppendCount, long throttleMinWaitMs) {
      return CatalogTestSupport.log(
          io,
          RACE_LOC,
          maxAppendCount,
          LogCatalogProperties.DEFAULT_MAX_APPEND_SIZE,
          LogCatalogProperties.DEFAULT_COMMITTED_RETENTION_MS,
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
      CatalogLog log = log(io, maxAppendCount, 0);
      Replica<CatalogState> snap = seed(log, "a");
      int writes = io.history().size();

      plan(io, throttled(Fault.LANDED_THROTTLED, 8)); // every retry throttled
      new CatalogMutation(snap).createNamespace(Namespace.of("c")).commit(log);

      assertThat(io.history()).hasSize(writes + 1);
      assertThat(log.read().state().namespaces()).contains(Namespace.of("c"));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 10_000})
    void writeThrottledUntilTheRetriesRanOutIsRetried(int maxAppendCount) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      CatalogLog log = log(io, maxAppendCount, 0);
      Replica<CatalogState> snap = seed(log, "a");
      int writes = io.history().size();

      plan(io, throttled(Fault.THROTTLED, 8));
      new CatalogMutation(snap).createNamespace(Namespace.of("c")).commit(log);

      assertThat(io.history()).hasSize(writes + 1);
      assertThat(log.read().state().namespaces()).contains(Namespace.of("c"));
    }

    @Test
    void throttlingThatNeverEndsIsCommitStateUnknown() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      CatalogLog log = log(io, 0, 0);
      Replica<CatalogState> snap = seed(log, "a");
      int writes = io.history().size();

      io.injectFaults((location, strategy) -> Fault.THROTTLED);
      // Nothing landed, but every throttled write might have.
      assertThatThrownBy(() -> new CatalogMutation(snap).createNamespace(Namespace.of("c")).commit(log))
          .isInstanceOf(CommitStateUnknownException.class);

      io.injectFaults((location, strategy) -> Fault.NONE);
      assertThat(io.history()).hasSize(writes);
    }

    @Test
    void interruptedBackoffIsCommitStateUnknown() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      CatalogLog log = log(io, 0, 1);
      Replica<CatalogState> snap = seed(log, "a");

      plan(io, List.of(Fault.LANDED_THROTTLED));
      Thread.currentThread().interrupt(); // the backoff's sleep throws at once
      try {
        assertThatThrownBy(() -> new CatalogMutation(snap).createNamespace(Namespace.of("c")).commit(log))
            .isInstanceOf(CommitStateUnknownException.class);
      } finally {
        Thread.interrupted();
      }
      assertThat(log.read().state().namespaces()).contains(Namespace.of("c"));
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
      CatalogLog appending = log(io, 10_000);
      Replica<CatalogState> stale = seed(appending, "a");

      // Another writer appends a large record.
      CatalogMutation big = new CatalogMutation(stale);
      for (String ns : List.of("x1", "x2", "x3", "x4")) {
        big.createNamespace(Namespace.of(ns));
      }
      big.commit(appending);
      Replica<CatalogState> afterBig = appending.read();
      long length = io.newInputFile(RACE_LOC).getLength();

      // The stale writer's small append fits its view but loses the race; at the new length it
      // must CAS. Just before it reads the version it bound, a third writer compacts.
      CatalogLog tight =
          CatalogTestSupport.log(
              io, RACE_LOC, 10_000, length, LogCatalogProperties.DEFAULT_COMMITTED_RETENTION_MS, 0);
      int[] reads = {0};
      io.injectReadFaults(
          location -> {
            if (reads[0]++ == 1) {
              new CatalogMutation(afterBig).createNamespace(Namespace.of("z")).commit(log(io, 0));
            }
            return ReadFault.NONE;
          });
      new CatalogMutation(stale).updateProperties(Namespace.of("a"), new HashMap<>(Map.of("k", "v")))
          .commit(tight);
      io.injectReadFaults(location -> ReadFault.NONE);

      Replica<CatalogState> result = appending.read();
      assertThat(result.state().namespaces()).contains(Namespace.of("x1"), Namespace.of("z"));
      assertThat(result.state().namespaceProperties(Namespace.of("a"))).containsEntry("k", "v");
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
      CatalogLog log = log(io, 0);
      Replica<CatalogState> snap = seed(log, "a", "b");
      TableIdentifier t1 = TableIdentifier.of("a", "t1");
      TableIdentifier t2 = TableIdentifier.of("b", "t2");

      new CatalogMutation(snap).createTable(t1, "mem:///a/t1/v1.metadata.json").commit(log);
      new CatalogMutation(snap).createTable(t2, "mem:///b/t2/v1.metadata.json").commit(log);

      assertThat(io.rejected(AtomicOutputFile.Strategy.CAS)).isEqualTo(1);
      Replica<CatalogState> result = log.read();
      assertThat(result.state().location(t1)).isEqualTo("mem:///a/t1/v1.metadata.json");
      assertThat(result.state().location(t2)).isEqualTo("mem:///b/t2/v1.metadata.json");
    }

    @Test
    void casConflictThatNoLongerHoldsFailsWithoutWriting() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      CatalogLog log = log(io, 0);
      Replica<CatalogState> snap = seed(log, "a", "b");

      new CatalogMutation(snap).updateProperties(Namespace.of("a"), new HashMap<>(Map.of("k", "1"))).commit(log);
      int writes = io.history().size();
      assertThatThrownBy(
              () ->
                  new CatalogMutation(snap)
                      .updateProperties(Namespace.of("a"), new HashMap<>(Map.of("k", "2")))
                      .commit(log))
          .isInstanceOf(CommitFailedException.class);

      assertThat(io.history()).hasSize(writes);
      assertThat(log.read().state().namespaceProperties(Namespace.of("a"))).containsEntry("k", "1");
    }

    @Test
    void casCreateRaceIsAlreadyExists() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      CatalogLog log = log(io, 0);
      Replica<CatalogState> snap = seed(log, "a");
      TableIdentifier t = TableIdentifier.of("a", "t");

      new CatalogMutation(snap).createTable(t, "mem:///a/t/v1.metadata.json").commit(log);
      assertThatThrownBy(() -> new CatalogMutation(snap).createTable(t, "mem:///a/t/v2.metadata.json").commit(log))
          .isInstanceOf(AlreadyExistsException.class);

      assertThat(log.read().state().location(t)).isEqualTo("mem:///a/t/v1.metadata.json");
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

  private static <C extends LogCatalog> C catalog(
      InMemoryAtomicFileIO io, LocalCatalogMode mode) {
    return mode.catalog(io, WAREHOUSE, "c", Map.of());
  }

  /** A catalog whose inline updates must fit in 2 KiB. */
  private static <C extends LogCatalog> C small(
      InMemoryAtomicFileIO io, LocalCatalogMode mode) {
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

  private static CatalogState catalogFile(InMemoryAtomicFileIO io) {
    return catalogRead(io).state();
  }

  private static Replica<CatalogState> catalogRead(InMemoryAtomicFileIO io) {
    return CatalogTestSupport.log(io, CATALOG).read();
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
  // its inline state, and a returning table gets the prefix a created one
  // does (now its metadata directory). Found by TestCatalogFaultProperties'
  // checkpoint round trip.
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

      CatalogState file = catalogFile(io);
      assertThat(file.isInlineTable(id)).isFalse();
      assertThat(file.snapshotManifests(id)).isEmpty();
      assertThat(CheckpointRoundTrip.difference(catalogRead(io))).isNull();
    }

    @Test
    void tableReturningInlineHasItsMetadataDirectoryAsPrefix() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog inline = small(io, LocalCatalogMode.APPEND_TM);
      inline.createNamespace(mixedNs);
      inline.createTable(mixed, intSchema);
      inline.loadTable(mixed).updateProperties().set("big", bigValue(6000)).commit();
      assertThat(catalogFile(io).isInlineTable(mixed)).as("evicted").isFalse();

      inline.loadTable(mixed).updateProperties().remove("big").commit();

      CatalogState file = catalogFile(io);
      assertThat(file.isInlineTable(mixed)).as("returned").isTrue();
      // as a created one has (the empty prefix until 2026-10-29, when it became the dictionary
      // entry manifests' names are under)
      assertThat(file.manifestListPrefix(file.tableId(mixed))).isEqualTo("mem:///w/ns/t/metadata/");
      assertThat(CheckpointRoundTrip.difference(catalogRead(io))).isNull();
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
      assertThat(CheckpointRoundTrip.difference(catalogRead(io))).isNull();
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
        CatalogState.Builder builder =
            CatalogState.builder()
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

  // ============================================================
  // Bug 52: replay of an inline delta adds the commit's new schemas and
  // partition specs before it removes the ones the commit dropped, and
  // TableMetadata.Builder numbers each schema or spec it adds: the id of an
  // equal one it holds, else one past the highest. A transaction whose
  // expiry cleaned up unused specs and schemas and then went back to one of
  // them (dropped the partition field or column the removed one lacked)
  // had a fresh id from its writer, but on replay took the removed one's:
  // the default spec or current schema was missing, the writer reported
  // its commit's state unknown, and no reader could load the catalog. Such
  // a commit now goes in full. Found by TestInlineDeltaProperties.
  // ============================================================

  @Nested
  class ReturnToARemovedSpecOrSchema {

    private final Schema twoColumns =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND_TM", "APPEND_ML"})
    void spec(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      catalog.createNamespace(mixedNs);
      Table table = catalog.buildTable(mixed, twoColumns)
          .withPartitionSpec(
              org.apache.iceberg.PartitionSpec.builderFor(twoColumns).identity("id").build())
          .create();
      table.updateSpec().addField("data").commit();

      org.apache.iceberg.Transaction txn = table.newTransaction();
      txn.expireSnapshots().cleanExpiredMetadata(true).commit();
      txn.updateSpec().removeField("data").commit();
      txn.commitTransaction();

      Table read = catalog(io, LocalCatalogMode.APPEND).loadTable(mixed);
      assertThat(read.spec().specId()).isEqualTo(table.spec().specId()).isEqualTo(2);
      assertThat(read.specs().keySet()).containsExactlyInAnyOrder(1, 2);
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND_TM", "APPEND_ML"})
    void schema(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      catalog.createNamespace(mixedNs);
      Table table = catalog.createTable(mixed, intSchema);
      table.updateSchema().addColumn("data", Types.StringType.get()).commit();

      org.apache.iceberg.Transaction txn = table.newTransaction();
      txn.expireSnapshots().cleanExpiredMetadata(true).commit();
      txn.updateSchema().deleteColumn("data").commit();
      txn.commitTransaction();

      Table read = catalog(io, LocalCatalogMode.APPEND).loadTable(mixed);
      assertThat(read.schema().schemaId()).isEqualTo(table.schema().schemaId()).isEqualTo(2);
      assertThat(read.schemas().keySet()).containsExactlyInAnyOrder(1, 2);
    }
  }

  // ============================================================
  // Bug 53: FileIOCatalog's namespace operations and renameTable committed
  // once. Each record names the versions it read -- of its namespace, of
  // the parent of a namespace it creates or drops, of a table it renames --
  // and any change there bumps them: a table created in the namespace, a
  // sibling namespace created, its properties set. Such a race failed the
  // operation with CommitFailedException, though it depends on nothing the
  // other changed (two writers creating top-level namespaces at once), and
  // setProperties and removeProperties instead returned false, which
  // callers such as Spark ignore: the update was lost. Each now retries on
  // a fresh read, where it checks existence again. Found by reading
  // setProperties.
  // ============================================================

  @Nested
  class CatalogOperationsRacingAChange {

    private final Namespace other = Namespace.of("other");

    /** Runs {@code race} on another catalog just before {@code catalog}'s next write. */
    private void before(InMemoryAtomicFileIO io, java.util.function.Consumer<FileIOCatalog> race,
        LocalCatalogMode mode, boolean[] raced) {
      FileIOCatalog racer = catalog(io, mode);
      io.injectFaults((location, strategy) -> {
        if (!raced[0] && location.equals(CATALOG)) {
          raced[0] = true;
          race.accept(racer);
        }
        return Fault.NONE;
      });
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND", "CAS"})
    void setAndRemoveProperties(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      catalog.createNamespace(mixedNs, Map.of("old", "v"));
      boolean[] raced = {false};
      before(io, c -> c.createTable(TableIdentifier.of(mixedNs, "a"), intSchema), mode, raced);
      assertThat(catalog.setProperties(mixedNs, Map.of("k", "v"))).isTrue();
      assertThat(raced[0]).isTrue();

      raced[0] = false;
      before(io, c -> c.createTable(TableIdentifier.of(mixedNs, "b"), intSchema), mode, raced);
      assertThat(catalog.removeProperties(mixedNs, java.util.Set.of("old"))).isTrue();
      assertThat(raced[0]).isTrue();

      assertThat(catalog(io, mode).loadNamespaceMetadata(mixedNs)).isEqualTo(Map.of("k", "v"));
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND", "CAS"})
    void createAndDropNamespaces(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      boolean[] raced = {false};
      before(io, c -> c.createNamespace(other), mode, raced);
      catalog.createNamespace(mixedNs);
      assertThat(raced[0]).isTrue();

      raced[0] = false;
      before(io, c -> c.setProperties(mixedNs, Map.of("k", "v")), mode, raced);
      assertThat(catalog.dropNamespace(mixedNs)).isTrue();
      assertThat(raced[0]).isTrue();

      assertThat(catalog(io, mode).listNamespaces(Namespace.empty())).containsExactly(other);
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND", "CAS"})
    void renameTable(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      catalog.createNamespace(mixedNs);
      catalog.createNamespace(other);
      catalog.createTable(mixed, intSchema);
      boolean[] raced = {false};
      before(io, c -> c.createTable(TableIdentifier.of(other, "a"), intSchema), mode, raced);

      catalog.renameTable(mixed, TableIdentifier.of(other, "t"));

      assertThat(raced[0]).isTrue();
      assertThat(catalog(io, mode).listTables(other))
          .containsExactly(TableIdentifier.of(other, "a"), TableIdentifier.of(other, "t"));
    }

    /** A retry checks existence again: a race the operation does depend on still fails it. */
    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND", "CAS"})
    void failOnARaceTheyDependOn(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      boolean[] raced = {false};
      before(io, c -> c.createNamespace(mixedNs), mode, raced);
      assertThatThrownBy(() -> catalog.createNamespace(mixedNs))
          .isInstanceOf(AlreadyExistsException.class);

      raced[0] = false;
      before(io, c -> c.createTable(mixed, intSchema), mode, raced);
      assertThatThrownBy(() -> catalog.dropNamespace(mixedNs))
          .isInstanceOf(NamespaceNotEmptyException.class);

      catalog.createNamespace(other);
      raced[0] = false;
      before(io, c -> c.dropNamespace(other), mode, raced);
      assertThatThrownBy(() -> catalog.setProperties(other, Map.of("k", "v")))
          .isInstanceOf(NoSuchNamespaceException.class);

      raced[0] = false;
      before(io, c -> c.createTable(TableIdentifier.of(mixedNs, "u"), intSchema), mode, raced);
      assertThatThrownBy(() -> catalog.renameTable(mixed, TableIdentifier.of(mixedNs, "u")))
          .isInstanceOf(AlreadyExistsException.class);
      assertThat(raced[0]).isTrue();
    }
  }

  // ============================================================
  // Bug 54: a table create's record names the version of the namespace it
  // read, which any change there bumps -- another table created or dropped
  // in it, its properties set -- and Iceberg takes a create's conflict to
  // mean the table exists: two writers creating different tables in a
  // namespace at once, and one got AlreadyExistsException ("Table was
  // created concurrently") for a table that did not exist. A create now
  // stages again on a fresh read, which fails if the table exists by then.
  // Found while fixing Bug 53.
  // ============================================================

  @Nested
  class CreateRacingAChangeToItsNamespace {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND", "CAS", "APPEND_TM", "APPEND_ML"})
    void createsTheTable(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      FileIOCatalog racer = catalog(io, mode);
      catalog.createNamespace(mixedNs);
      TableIdentifier first = TableIdentifier.of(mixedNs, "first");
      boolean[] raced = {false};
      io.injectFaults((location, strategy) -> {
        if (!raced[0] && location.equals(CATALOG)) {
          raced[0] = true;
          racer.createTable(first, intSchema);
        }
        return Fault.NONE;
      });

      catalog.buildTable(mixed, intSchema).createTransaction().commitTransaction();
      Table table = catalog.loadTable(mixed);

      io.injectFaults((location, strategy) -> Fault.NONE);
      assertThat(raced[0]).isTrue();
      assertThat(table.schema().asStruct()).isEqualTo(intSchema.asStruct());
      assertThat(catalog(io, LocalCatalogMode.APPEND).listTables(mixedNs))
          .containsExactly(first, mixed);
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND", "CAS", "APPEND_TM"})
    void failsIfTheTableWasCreated(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      FileIOCatalog racer = catalog(io, mode);
      catalog.createNamespace(mixedNs);
      boolean[] raced = {false};
      io.injectFaults((location, strategy) -> {
        if (!raced[0] && location.equals(CATALOG)) {
          raced[0] = true;
          racer.createTable(mixed, intSchema);
        }
        return Fault.NONE;
      });

      assertThatThrownBy(() -> catalog.createTable(mixed, intSchema))
          .isInstanceOf(AlreadyExistsException.class);

      io.injectFaults((location, strategy) -> Fault.NONE);
      assertThat(raced[0]).isTrue();
      assertThat(catalog(io, LocalCatalogMode.APPEND).listTables(mixedNs)).containsExactly(mixed);
    }
  }

  // ============================================================
  // Bug 55: of two writers renaming a table to the same name at once, the
  // one that lost got AlreadyExistsException: a rename that conflicted
  // was judged by its destination first, which the winner had just
  // created. Run after the other, it fails because its source is gone
  // (NoSuchTableException), as renameTable checks the source first; the
  // conflict now does too. Found by TestCatalogRaceProperties.
  // ============================================================

  @Nested
  class RenameRacingTheSameRename {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND", "CAS", "APPEND_TM"})
    void reportsTheSourceGone(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      FileIOCatalog racer = catalog(io, mode);
      catalog.createNamespace(mixedNs);
      catalog.createTable(mixed, intSchema);
      TableIdentifier to = TableIdentifier.of(mixedNs, "u");
      boolean[] raced = {false};
      io.injectFaults((location, strategy) -> {
        if (!raced[0] && location.equals(CATALOG)) {
          raced[0] = true;
          racer.renameTable(mixed, to);
        }
        return Fault.NONE;
      });

      assertThatThrownBy(() -> catalog.renameTable(mixed, to))
          .isInstanceOf(NoSuchTableException.class);

      io.injectFaults((location, strategy) -> Fault.NONE);
      assertThat(raced[0]).isTrue();
      assertThat(catalog(io, mode).listTables(mixedNs)).containsExactly(to);
    }
  }

  // ============================================================
  // Bug 56: FileIOCatalog.initialize creates the catalog file if there is
  // none, and of two catalogs starting on a new warehouse at once, the one
  // whose create lost failed to initialize (CommitFailedException:
  // "Catalog creation failed: ... exists"); so did one whose create landed
  // with its reply lost (CommitStateUnknownException). Either now uses the
  // file that exists. Found by reading initialize.
  // ============================================================

  @Nested
  class InitializeRacingAnotherInitialize {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND", "CAS"})
    void usesTheOthersCatalog(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      boolean[] raced = {false};
      FileIOCatalog[] other = {null};
      io.injectFaults((location, strategy) -> {
        if (!raced[0]) {
          raced[0] = true;
          other[0] = catalog(io, mode); // just before this catalog's create
        }
        return Fault.NONE;
      });

      FileIOCatalog catalog = catalog(io, mode);

      io.injectFaults((location, strategy) -> Fault.NONE);
      assertThat(other[0]).isNotNull();
      other[0].createNamespace(mixedNs);
      assertThat(catalog.namespaceExists(mixedNs)).isTrue();
    }

    @Test
    void usesItsOwnCatalogWhoseReplyWasLost() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      boolean[] faulted = {false};
      int[] failingReads = {0};
      io.injectReadFaults(location -> failingReads[0]-- > 0 ? ReadFault.FAIL : ReadFault.NONE);
      io.injectFaults((location, strategy) -> {
        if (!faulted[0]) {
          faulted[0] = true;
          failingReads[0] = 1; // and the read that would settle it fails
          return Fault.LOST_REPLY;
        }
        return Fault.NONE;
      });

      FileIOCatalog catalog = catalog(io, LocalCatalogMode.APPEND);

      io.injectFaults((location, strategy) -> Fault.NONE);
      io.injectReadFaults(location -> ReadFault.NONE);
      assertThat(faulted[0]).isTrue();
      catalog.createNamespace(mixedNs);
      assertThat(catalog(io, LocalCatalogMode.APPEND).namespaceExists(mixedNs)).isTrue();
    }
  }

  // ============================================================
  // Bug 57: Iceberg's metadata tables (snapshots, history, refs,
  // partitions, metadata_log_entries; with inline manifest lists also
  // manifests and all_manifests) take the length of the table's metadata
  // file, and an inline table's metadata location is synthetic: each
  // failed with NotFoundException ("File does not exist: inline://..."),
  // so SELECT * FROM t.snapshots did not work on an inline table. A
  // table's FileIO now reads its current inline metadata location as a
  // file of the stored metadata's length (which cannot be opened), and is
  // the catalog's FileIO otherwise. Found by probing metadata tables.
  // ============================================================

  @Nested
  class MetadataTablesOfInlineTables {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND_TM", "APPEND_ML", "CAS_ML"})
    void readLikeAPointerTables(LocalCatalogMode mode) throws IOException {
      // latest_sequence_number derives from the order of timestamps, which a step of the wall
      // clock changes (ClockSteps). The runs take a fraction of a second: run them again then.
      ClockSteps clock = new ClockSteps();
      Map<String, List<String>> inline = metadataTables(mode, clock);
      Map<String, List<String>> pointer = metadataTables(LocalCatalogMode.APPEND, clock);
      for (int attempt = 1; clock.stepped() && attempt < 5; attempt++) {
        clock = new ClockSteps();
        inline = metadataTables(mode, clock);
        pointer = metadataTables(LocalCatalogMode.APPEND, clock);
      }
      org.junit.jupiter.api.Assumptions.assumeFalse(
          clock.stepped(), "the wall clock stepped during each of 5 attempts");
      assertThat(inline.keySet()).isEqualTo(pointer.keySet());
      for (String type : pointer.keySet()) {
        assertThat(inline.get(type)).as(type).isEqualTo(pointer.get(type));
      }
      assertThat(inline.get("SNAPSHOTS")).hasSize(3);
      assertThat(inline.get("ALL_MANIFESTS")).hasSize(6);
    }

    /**
     * A table's FileIO goes to Spark's workers serialized: there it is the catalog's FileIO, which
     * gives the metadata the table had when serialized its length, and no other.
     */
    @Test
    void ioSerializes() throws Exception {
      CasFileIO catalogIO = new CasFileIO();
      catalogIO.newOutputFile("mem:///data/a").createOrOverwrite().close();
      String location = "inline://ns.t#1.2";
      InlineMetadataFileIO io = InlineMetadataFileIO.of(catalogIO);
      io.current(new InlineMetadataFileIO.Metadata(location, 42L));
      assertThat(io.newInputFile(location).getLength()).isEqualTo(42L);
      assertThatThrownBy(() -> io.newInputFile(location).newStream())
          .isInstanceOf(UnsupportedOperationException.class);

      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (java.io.ObjectOutputStream out = new java.io.ObjectOutputStream(bytes)) {
        out.writeObject(io);
      }
      FileIO copy;
      try (java.io.ObjectInputStream in =
          new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray()))) {
        copy = (FileIO) in.readObject();
      }

      assertThat(copy).isInstanceOf(InlineMetadataFileIO.class);
      assertThat(copy.newInputFile("mem:///data/a").exists()).isTrue();
      assertThat(copy.newInputFile(location).getLength()).isEqualTo(42L);
      assertThatThrownBy(() -> copy.newInputFile(location).newStream())
          .isInstanceOf(UnsupportedOperationException.class);
      assertThat(copy.newInputFile("inline://ns.t#1.3").exists()).isFalse();
    }

    /**
     * Each metadata table's rows, as text, after three appends and a property change: snapshot
     * ids, timestamps and file names, which differ between catalogs, are left out. {@code clock}
     * looks at the wall clock after each commit.
     */
    private Map<String, List<String>> metadataTables(LocalCatalogMode mode, ClockSteps clock)
        throws IOException {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      catalog.createNamespace(mixedNs);
      Table table = catalog.createTable(mixed, intSchema);
      clock.stepped();
      for (String name : List.of("a", "b", "c")) {
        table.newFastAppend().appendFile(dataFile(name)).commit();
        clock.stepped();
      }
      table.updateProperties().set("k", "v").commit();
      clock.stepped();
      Map<String, List<String>> rows = new java.util.TreeMap<>();
      for (org.apache.iceberg.MetadataTableType type : org.apache.iceberg.MetadataTableType.values()) {
        Table metadata = catalog.loadTable(
            TableIdentifier.of("ns", "t", type.name().toLowerCase(java.util.Locale.ROOT)));
        List<String> found = Lists.newArrayList();
        if (type == org.apache.iceberg.MetadataTableType.POSITION_DELETES) {
          try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.ScanTask> tasks =
              metadata.newBatchScan().planFiles()) {
            tasks.forEach(task -> found.add("task"));
          }
        } else {
          try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
              metadata.newScan().planFiles()) {
            for (org.apache.iceberg.FileScanTask task : tasks) {
              try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.StructLike> data =
                  task.asDataTask().rows()) {
                data.forEach(row -> found.add(describe(metadata.schema(), row)));
              }
            }
          }
        }
        found.sort(String::compareTo);
        rows.put(type.name(), found);
      }
      return rows;
    }

    /** The row's fields that are the same in any catalog: counts, operations, names, sizes. */
    private String describe(Schema schema, org.apache.iceberg.StructLike row) {
      List<String> fields = Lists.newArrayList();
      List<Types.NestedField> columns = schema.columns();
      for (int i = 0; i < columns.size(); i++) {
        String name = columns.get(i).name();
        Object value = row.get(i, Object.class);
        boolean differs = name.contains("snapshot_id") || name.contains("timestamp")
            || name.contains("committed_at") || name.contains("made_current_at")
            || name.contains("path") || name.equals("file") || name.contains("summary")
            || name.equals("readable_metrics") || name.contains("length")
            || name.equals("data_file") || name.contains("last_updated")
            || name.equals("latest_schema_id") || name.equals("parent_id")
            || name.equals("manifest_list") || name.equals("added_snapshot_id")
            || name.equals("reference_snapshot_id");
        fields.add(name + "=" + (differs ? "*" : value));
      }
      return String.join(" ", fields);
    }
  }

  // ============================================================
  // Bug 58: replay of an inline delta logs one snapshot-log entry, main's
  // new snapshot at the commit's time, if main moved. A transaction that
  // moved main and back -- an append rolled back to where main was --
  // left main's ref as it was, so its delta set no ref, but Iceberg logs
  // main again: readers of an inline table lost that entry. computeDelta
  // checked only the time of the log's last entry; it now checks that
  // the log gained only what replay adds, and else sends the full
  // metadata. Found by TestInlineDifferentialProperties with catalog
  // transactions; a table transaction does it too.
  // ============================================================

  @Nested
  class MainMovedAndBack {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND", "APPEND_TM", "APPEND_ML"})
    void logsMainAgain(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      catalog.createNamespace(mixedNs);
      Table table = catalog.createTable(mixed, intSchema);
      table.newFastAppend().appendFile(dataFile("a")).commit();
      long first = table.currentSnapshot().snapshotId();

      org.apache.iceberg.Transaction txn = table.newTransaction();
      txn.newFastAppend().appendFile(dataFile("b")).commit();
      txn.manageSnapshots().rollbackTo(first).commit();
      txn.commitTransaction();

      List<Long> log = Lists.transform(
          catalog(io, LocalCatalogMode.APPEND).loadTable(mixed).history(),
          org.apache.iceberg.HistoryEntry::snapshotId);
      assertThat(log).containsExactly(first, first);
    }
  }

  // ============================================================
  // Bug 59: FileIOCatalog did not override Catalog.name(), whose default
  // is toString(): a catalog was "FileIOCatalog{}" whatever it was named,
  // and so began its tables' names ("FileIOCatalog{}.ns.t"), which Iceberg
  // reports in scan and commit metrics. Overriding it needed a fix in the
  // iceberg fork first: BaseCatalogTransaction found a table by removing
  // the catalog's name, wherever it occurred, from the table's, so a
  // transaction on table t in namespace "c" of catalog "c" ("c.c.t")
  // looked for a table "t". It now knows a table by the identifier it was
  // loaded by. Found in a probe's output.
  // ============================================================

  @Nested
  class CatalogName {

    @Test
    void namesTheCatalogAndItsTables() {
      FileIOCatalog catalog = catalog(new InMemoryAtomicFileIO(), LocalCatalogMode.APPEND);
      catalog.createNamespace(mixedNs);

      assertThat(catalog.name()).isEqualTo("c");
      assertThat(catalog.createTable(mixed, intSchema).name()).isEqualTo("c.ns.t");
      assertThat(catalog.loadTable(mixed).name()).isEqualTo("c.ns.t");
      assertThat(catalog.loadTable(TableIdentifier.of("ns", "t", "snapshots")).name())
          .isEqualTo("c.ns.t.snapshots");
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(LocalCatalogMode.class)
    void commitsATransactionInANamespaceNamedLikeTheCatalog(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      LogCatalog catalog = catalog(io, mode);
      TableIdentifier table = TableIdentifier.of("c", "t");
      catalog.createNamespace(table.namespace());
      catalog.createTable(table, intSchema);

      org.apache.iceberg.catalog.CatalogTransaction txn =
          catalog.createTransaction(
              org.apache.iceberg.catalog.CatalogTransaction.IsolationLevel.SERIALIZABLE);
      txn.asCatalog().loadTable(table).newFastAppend().appendFile(dataFile("a")).commit();
      txn.commitTransaction();

      assertThat(dataFiles(catalog(io, mode).loadTable(table)))
          .containsExactly(dataFile("a").location());
    }
  }

  // ============================================================
  // Bug 60: Iceberg 1.12.0 (apache/iceberg#14504) no longer takes a
  // table's last-updated-ms from a snapshot it adds, and logs main's move
  // to a snapshot at the snapshot's own time if the same builder added
  // it. Replay of an inline delta added each snapshot in a builder of its
  // own, without the writer's last-updated-ms: a commit that only added a
  // snapshot (a staged append) replayed at the reader's clock, and the
  // times of the next delta's snapshots, which it stores relative to that,
  // drifted. And computeDelta expected main's new log entry at the
  // commit's last-updated-ms, so on 1.12.0 nearly every append went in
  // full. Replay now adds a delta's snapshots in the builder that sets its
  // refs, at the writer's last-updated-ms, and computeDelta expects the
  // entry Iceberg logs. Found by TestInlineDeltaProperties,
  // TestInlineDifferentialProperties and TestCatalogFaultProperties on the
  // rebase onto Iceberg 1.12.0.
  // ============================================================

  @Nested
  class TimesOfAddedSnapshots {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND_TM", "APPEND_ML"})
    void aCommitThatOnlyAddsASnapshotReplaysAsWritten(LocalCatalogMode mode)
        throws InterruptedException {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      catalog.createNamespace(mixedNs);
      catalog.createTable(mixed, intSchema).newFastAppend().appendFile(dataFile("a")).commit();

      for (String file : List.of("b", "c")) {
        org.apache.iceberg.Transaction txn = catalog.loadTable(mixed).newTransaction();
        txn.newFastAppend().appendFile(dataFile(file)).stageOnly().commit();
        org.apache.iceberg.TableMetadata written =
            ((org.apache.iceberg.HasTableOperations) txn.table()).operations().current();
        txn.commitTransaction();
        Thread.sleep(5);

        org.apache.iceberg.TableMetadata read =
            ((org.apache.iceberg.HasTableOperations) catalog(io, LocalCatalogMode.APPEND)
                    .loadTable(mixed))
                .operations()
                .current();
        assertThat(read.lastUpdatedMillis())
            .as("last-updated-ms after staging %s", file)
            .isEqualTo(written.lastUpdatedMillis());
        for (org.apache.iceberg.Snapshot s : written.snapshots()) {
          assertThat(read.snapshot(s.snapshotId()).timestampMillis())
              .as("snapshot %d", s.snapshotId())
              .isEqualTo(s.timestampMillis());
        }
      }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void anAppendGoesAsADelta(boolean structural) {
      InlineMetadataCodec codec =
          structural ? InlineMetadataCodecs.STRUCTURAL : InlineMetadataCodecs.JSON_GZIP;
      org.apache.iceberg.TableMetadata base =
          codec.decodeFull(
              codec.encode(
                  org.apache.iceberg.TableMetadata.newTableMetadata(
                      intSchema, org.apache.iceberg.PartitionSpec.unpartitioned(),
                      org.apache.iceberg.SortOrder.unsorted(), "mem:///w/ns/t",
                      Map.of("format-version", "2"))),
              "inline://base");
      long created = base.lastUpdatedMillis() + 1;
      org.apache.iceberg.Snapshot snapshot =
          org.apache.iceberg.InlineDeltaSnapshots.create(
              1, 7L, null, created, Map.of("operation", "append"), 0,
              "mem:///w/ns/t/metadata/snap-7.avro", null, null, null);
      // A commit a second after its snapshot was made: Iceberg logs main's move at the
      // snapshot's time and the commit at the commit's.
      org.apache.iceberg.TableMetadata next =
          org.apache.iceberg.TableMetadata.buildFrom(base)
              .setLastUpdatedMillis(created + 1000)
              .setBranchSnapshot(snapshot, org.apache.iceberg.SnapshotRef.MAIN_BRANCH)
              .build();
      assertThat(next.snapshotLog().get(0).timestampMillis()).isEqualTo(created);

      List<InlineDeltaCodec.DeltaUpdate> delta = InlineDeltaCodec.computeDelta(base, next, "");
      assertThat(delta).as("the append's delta").isNotNull();

      CatalogState.Builder builder =
          CatalogState.builder()
              .addInlineTable(1, 0, "t", 1, codec.encode(base), codec.tag(), "");
      ProtoCodec.UpdateTableInlineAction.delta(
              1, 1, InlineDeltaCodec.encodeDelta(delta, next.lastUpdatedMillis()))
          .apply(builder);
      org.apache.iceberg.TableMetadata replayed =
          codec.decodeFull(builder.inlineMetadata(1), "inline://replayed");
      assertThat(replayed.lastUpdatedMillis()).isEqualTo(next.lastUpdatedMillis());
      assertThat(replayed.snapshotLog()).isEqualTo(next.snapshotLog());
      assertThat(replayed.currentSnapshot().timestampMillis()).isEqualTo(created);
    }
  }

  // ============================================================
  // Bug 61: once a table commit lands, FileIOCatalog deletes the metadata
  // files that dropped out of the table's metadata log
  // (write.metadata.delete-after-commit.enabled). Through a FileIO with bulk
  // deletes (S3, GCS, ADLS), a delete that failed threw out of the commit,
  // so Iceberg reported a commit that had landed as failed, and a caller
  // that retried it committed twice. The non-bulk path logged and went on.
  // It now deletes with CatalogUtil.deleteFiles, which logs either kind of
  // failure, as Iceberg's own catalogs do. Found by reading FileIOCatalog
  // against Iceberg 1.12.0, which made deleteFiles public.
  // ============================================================

  @Nested
  class MetadataCleanupThatFails {

    @Test
    void leavesTheCommitThatLanded() {
      BulkDeletesFail io = new BulkDeletesFail();
      FileIOCatalog catalog = catalog(io, LocalCatalogMode.APPEND);
      catalog.createNamespace(mixedNs);
      Table table =
          catalog
              .buildTable(mixed, intSchema)
              .withProperty(
                  org.apache.iceberg.TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true")
              .withProperty(org.apache.iceberg.TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "1")
              .create();
      table.newFastAppend().appendFile(dataFile("a")).commit();
      // Drops the table's first metadata file from its log, so the commit deletes it.
      table.newFastAppend().appendFile(dataFile("b")).commit();

      assertThat(io.bulkDeletes).as("bulk deletes").isPositive();
      assertThat(dataFiles(catalog(io, LocalCatalogMode.APPEND).loadTable(mixed)))
          .containsExactly(dataFile("a").location(), dataFile("b").location());
    }
  }

  // ============================================================
  // Bug 62: a replace transaction commits the table it built at its
  // start, snapshots included (Iceberg behavior A1). If the table was
  // inline with inline manifest lists, the handle held its snapshots as
  // inline snapshots. When another writer evicted the table in between
  // (a commit too big for the inline update limit, which writes the
  // snapshots' lists to files and drops them from the catalog's pool),
  // the replace brought the table back inline in full, naming those
  // snapshots by their sentinels, but sent lists only for the snapshots
  // it added: the table no longer loaded ("sentinel ... but no pool
  // entry"). A commit of full metadata now also sends the list of each
  // inline snapshot the table holds no list for. Found by
  // TestCatalogFaultProperties in a long run; it reproduces on the fork
  // before the rebase onto Iceberg 1.12.0 too.
  // ============================================================

  @Nested
  class ReplaceOfATableEvictedSinceItBegan {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void keepsTheListsOfItsSnapshots(boolean orCreate) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = small(io, LocalCatalogMode.APPEND_ML);
      catalog.createNamespace(mixedNs);
      Table table = catalog.createTable(mixed, intSchema);
      table.newFastAppend().appendFile(dataFile("a")).commit();
      long first = table.currentSnapshot().snapshotId();

      org.apache.iceberg.Transaction replace =
          catalog.newReplaceTableTransaction(mixed, intSchema, orCreate);
      FileIOCatalog evicts = small(io, LocalCatalogMode.APPEND_ML);
      boolean[] raced = {false};
      io.injectFaults((location, strategy) -> {
        if (!raced[0] && location.equals(CATALOG)) {
          raced[0] = true;
          // Too big for the 2 KiB inline update limit: the table leaves inline storage.
          evicts.loadTable(mixed).updateProperties().set("big", bigValue(2457)).commit();
        }
        return Fault.NONE;
      });
      replace.commitTransaction();
      assertThat(raced[0]).isTrue();

      Table read = catalog(io, LocalCatalogMode.APPEND).loadTable(mixed);
      assertThat(manifestLists(io, read).get(first)).hasSize(1);
      assertThat(read.snapshot(first).addedDataFiles(io))
          .extracting(org.apache.iceberg.ContentFile::location)
          .containsExactly(dataFile("a").location());
    }
  }

  // ============================================================
  // Bug 63: an inline delta carries the schemas and partition specs a
  // commit added, and replay takes the table's last column and partition
  // ids up to the highest among them, as Iceberg does when it adds one. A
  // transaction that added a partition field, removed it again and expired
  // snapshots with metadata cleanup, which dropped the spec it had added,
  // left the last partition id above every spec's: the delta replayed to
  // the earlier one. Such a commit now goes in full. Found by
  // TestInlineDeltaProperties in a long run.
  // ============================================================

  @Nested
  class SpecAddedAndRemovedInOneCommit {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND_TM", "APPEND_ML"})
    void replaysToItsLastPartitionId(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      catalog.createNamespace(mixedNs);
      Table table = catalog.createTable(mixed, intSchema);
      table.newFastAppend().appendFile(dataFile("a")).commit();
      table.newFastAppend().appendFile(dataFile("b")).commit();

      org.apache.iceberg.Transaction txn = table.newTransaction();
      txn.updateSpec().addField("id").commit();
      txn.updateSpec().removeField("id").commit();
      txn.expireSnapshots()
          .expireOlderThan(System.currentTimeMillis() + 1)
          .retainLast(1)
          .cleanExpiredMetadata(true)
          .commit();
      org.apache.iceberg.TableMetadata written =
          ((org.apache.iceberg.HasTableOperations) txn.table()).operations().current();
      txn.commitTransaction();
      assertThat(written.specs()).as("the specs after the cleanup").hasSize(1);
      assertThat(written.lastAssignedPartitionId()).as("the written id").isEqualTo(1000);

      org.apache.iceberg.TableMetadata read =
          ((org.apache.iceberg.HasTableOperations) catalog(io, LocalCatalogMode.APPEND)
                  .loadTable(mixed))
              .operations()
              .current();
      assertThat(read.lastAssignedPartitionId())
          .as("the replayed id")
          .isEqualTo(written.lastAssignedPartitionId());
    }
  }

  // ============================================================
  // Bug 65: Iceberg binds a table's default sort order to its current
  // schema, and its other sort orders unchecked, so an earlier order may
  // name a column dropped since. Replay of an inline delta adds each new
  // order with TableMetadata.Builder.addSortOrder, which binds it to the
  // schema current by then: the commit's. A transaction that sorted by a
  // column, sorted by another, and dropped the first committed a delta
  // whose replay threw ("Cannot find source column for sort field") on
  // every read of the catalog. Such a commit, and one whose new partition
  // spec does not bind to the commit's schema, now goes in full. Found by
  // TestInlineDeltaProperties in a long run.
  // ============================================================

  @Nested
  class SortOrderOnAColumnDroppedInOneCommit {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
        value = LocalCatalogMode.class, names = {"APPEND_TM", "APPEND_ML"})
    void readsBack(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, mode);
      catalog.createNamespace(mixedNs);
      Table table = catalog.createTable(mixed, intSchema);
      table.updateSchema().addColumn("extra", Types.IntegerType.get()).commit();

      org.apache.iceberg.Transaction txn = table.newTransaction();
      txn.replaceSortOrder().asc("extra").commit();
      txn.replaceSortOrder().asc("id").commit();
      txn.updateSchema().deleteColumn("extra").commit();
      org.apache.iceberg.TableMetadata written =
          ((org.apache.iceberg.HasTableOperations) txn.table()).operations().current();
      txn.commitTransaction();

      Table read = catalog(io, LocalCatalogMode.APPEND).loadTable(mixed);
      assertThat(read.sortOrders()).hasSameSizeAs(written.sortOrders());
      assertThat(read.sortOrder().orderId()).isEqualTo(written.defaultSortOrderId());
      assertThat(read.schema().findField("extra")).isNull();
    }
  }

  // ============================================================
  // Bug 66: S3 Express caps an object at 10,000 parts, and Azure an
  // Append Blob at 50,000 blocks: an append past the cap fails. The
  // providers' FileIOs let the SDK error out raw, so the commit loop took
  // the append for ambiguous, re-read the catalog and appended again,
  // compacting only once the append count reached
  // fileio.catalog.max.append.count. With that set above the store's cap,
  // no writer ever compacted: every commit ended CommitStateUnknown, for
  // good. At the cap, an append that lost a race overshot it and recovered
  // only through the ambiguous path. The FileIOs now throw
  // SegmentLimitException, and the commit loop compacts by CAS. Found by
  // testing the hard limits TOO_MANY_PARTS.md described.
  // ============================================================

  @Nested
  class AppendsPastTheStoresLimit {

    @Test
    void anAppendCountAboveTheLimitCompacts() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      io.limitSegments(5);
      FileIOCatalog catalog =
          LocalCatalogMode.APPEND.catalog(
              io, WAREHOUSE, "c", Map.of(LogCatalogProperties.MAX_APPEND_COUNT, "100"));
      for (int i = 0; i < 12; i++) {
        catalog.createNamespace(Namespace.of("n" + i));
      }

      assertThat(catalog(io, LocalCatalogMode.APPEND).listNamespaces()).hasSize(12);
      assertThat(io.conditionalWrites().get(CATALOG))
          .as("compactions when the object is full")
          .containsSubsequence(
              AtomicOutputFile.Strategy.CAS, AtomicOutputFile.Strategy.APPEND, AtomicOutputFile.Strategy.APPEND, AtomicOutputFile.Strategy.APPEND, AtomicOutputFile.Strategy.APPEND,
              AtomicOutputFile.Strategy.CAS, AtomicOutputFile.Strategy.APPEND, AtomicOutputFile.Strategy.APPEND, AtomicOutputFile.Strategy.APPEND, AtomicOutputFile.Strategy.APPEND,
              AtomicOutputFile.Strategy.CAS);
    }

    @Test
    void anAppendThatLosesARaceToTheLimitCompacts() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      io.limitSegments(5);
      Map<String, String> atTheLimit = Map.of(LogCatalogProperties.MAX_APPEND_COUNT, "5");
      FileIOCatalog a = LocalCatalogMode.APPEND.catalog(io, WAREHOUSE, "c", atTheLimit);
      FileIOCatalog b = LocalCatalogMode.APPEND.catalog(io, WAREHOUSE, "c", atTheLimit);
      for (int i = 0; i < 3; i++) {
        a.createNamespace(Namespace.of("n" + i)); // a CAS and three appends: four writes
      }
      boolean[] raced = {false};
      io.injectFaults((location, strategy) -> {
        if (!raced[0] && location.equals(CATALOG)) {
          raced[0] = true;
          // the fifth, and no conflict with A's (a create under the root conflicts with one)
          b.setProperties(Namespace.of("n0"), Map.of("k", "v"));
        }
        return Fault.NONE;
      });

      // A read four records, appends, loses the race, and retries its append at the new length.
      a.createNamespace(Namespace.of("a"));

      io.injectFaults((location, strategy) -> Fault.NONE);
      assertThat(catalog(io, LocalCatalogMode.APPEND).namespaceExists(Namespace.of("a"))).isTrue();
      assertThat(io.conditionalWrites().get(CATALOG))
          .as("A compacted, its own record in the CAS")
          .hasSize(6)
          .last()
          .isEqualTo(AtomicOutputFile.Strategy.CAS);
    }
  }

  // ============================================================
  // Bug 71: a store caps the bytes of one append (an Azure AppendBlock:
  // 4 MiB before service version 2022-11-02, 100 MiB since), and an append
  // past the cap fails before landing, with the raw error the ADLS FileIO
  // lets out. The commit loop took the append for ambiguous, read the
  // catalog, found its record absent and appended it again, and after its
  // last attempt reported the commit unknown; every retry of it did the
  // same, unless the record took the file past
  // fileio.catalog.max.append.size, which compacts by CAS (a CAS is not
  // capped). fileio.catalog.inline.max.update.size does not keep a record
  // under the cap: it bounds each table's update, and a catalog
  // transaction's record holds one per table. SharedLog's
  // Capabilities.maxRecordBytes, which would say what the cap is, was never
  // read, and every provider reported 0. A FileIO now reports its cap
  // (SupportsAtomicOperations.maxAppendBytes; the ADLS FileIO's is its
  // client's AppendBlock limit), AtomicFileLog reports it less the record's
  // length prefix, and the commit loop writes a larger record by CAS; on a
  // log that cannot replace itself, it rejects the commit before sending it
  // (TOO_LARGE). Found by a review of the SharedLog SPI.
  // ============================================================

  @Nested
  class AppendsPastTheStoresSizeLimit {

    @Test
    void aRecordAboveTheLimitCompacts() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      io.limitAppendBytes(4096);
      FileIOCatalog catalog = catalog(io, LocalCatalogMode.APPEND);
      catalog.createNamespace(mixedNs);

      catalog.setProperties(mixedNs, Map.of("k", bigValue(8192)));

      assertThat(catalog(io, LocalCatalogMode.APPEND).loadNamespaceMetadata(mixedNs))
          .containsEntry("k", bigValue(8192));
      assertThat(io.conditionalWrites().get(CATALOG))
          .as("the commit compacted, its own record in the CAS")
          .last()
          .isEqualTo(AtomicOutputFile.Strategy.CAS);
    }

    @Test
    void aTransactionOfInlineUpdatesThatEachFitCompacts() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      io.limitAppendBytes(4096);
      Map<String, String> fits = Map.of(FileIOCatalog.INLINE_MAX_UPDATE_SIZE, "3072");
      FileIOCatalog catalog = LocalCatalogMode.APPEND_TM.catalog(io, WAREHOUSE, "c", fits);
      TableIdentifier other = TableIdentifier.of(mixedNs, "u");
      catalog.createNamespace(mixedNs);
      catalog.createTable(mixed, intSchema);
      catalog.createTable(other, intSchema);
      assertThat(List.of(mixed, other))
          .as("both tables inline")
          .allMatch(id -> metadataLocation(catalog.loadTable(id)).startsWith("inline://"));
      CatalogTransaction txn = catalog.createTransaction(CatalogTransaction.IsolationLevel.SNAPSHOT);
      txn.asCatalog().loadTable(mixed).updateProperties().set("k", bigValue(2500)).commit();
      txn.asCatalog().loadTable(other).updateProperties().set("k", bigValue(2501)).commit();

      // each update fits in 3 KiB, and the record that holds both does not fit in 4
      txn.commitTransaction();

      FileIOCatalog reader = LocalCatalogMode.APPEND_TM.catalog(io, WAREHOUSE, "c", fits);
      assertThat(reader.loadTable(mixed).properties()).containsEntry("k", bigValue(2500));
      assertThat(reader.loadTable(other).properties()).containsEntry("k", bigValue(2501));
      assertThat(List.of(mixed, other))
          .as("each update fit the inline limit")
          .allMatch(id -> metadataLocation(reader.loadTable(id)).startsWith("inline://"));
      assertThat(io.conditionalWrites().get(CATALOG))
          .as("the commit compacted, its own record in the CAS")
          .last()
          .isEqualTo(AtomicOutputFile.Strategy.CAS);
    }

    private String metadataLocation(Table table) {
      return ((org.apache.iceberg.HasTableOperations) table)
          .operations()
          .current()
          .metadataFileLocation();
    }
  }

  // ============================================================
  // Bug 72: a shared log may serve a read from a replica that lags its
  // appends (P1: a read returns a prefix of the log). The commit loop
  // reads its own record through its receipt (P3), but a table handle
  // threw away the replica its commit landed in, and Iceberg refreshes the
  // handle right after a commit, by a fresh read of the log: on a replica
  // that had not caught up, the writer's table went back to the version
  // before its commit, and Iceberg, which refreshes to find the snapshot
  // it committed, skipped cleaning up its manifests.
  // Capabilities.readYourAppends, which said whether reads lag, was read by
  // nothing, and is gone. CatalogLog now keeps the newest replica its
  // catalog read or committed in, and a read that lags behind it serves it
  // instead (LogRuntime.read(floor)), on a log that cannot replace itself.
  // Found by a review of the SharedLog SPI.
  // ============================================================

  @Nested
  class ReadsThatLagACommit {

    /** Once armed, serves each fresh read one record behind the log, as a lagging replica would. */
    private final class Lagging implements org.intentlog.memory.InMemorySharedLog.Interceptor {
      boolean armOnNextAppend = false;
      boolean armed = false;

      @Override
      public void afterLanding(org.intentlog.memory.InMemorySharedLog.Request request) {
        if (armOnNextAppend && request == org.intentlog.memory.InMemorySharedLog.Request.APPEND) {
          armOnNextAppend = false;
          armed = true;
        }
      }

      @Override
      public org.intentlog.memory.InMemorySharedLog.ReadShape beforeRead(
          org.intentlog.memory.InMemorySharedLog.Request request) {
        return armed && request == org.intentlog.memory.InMemorySharedLog.Request.READ
            ? org.intentlog.memory.InMemorySharedLog.ReadShape.of(0, 1)
            : org.intentlog.memory.InMemorySharedLog.ReadShape.NORMAL;
      }
    }

    @Test
    void aWriterSeesItsOwnCommit() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      // no compaction, whose checkpoint would hold the commit and so hide the lag
      LogCatalog catalog = LocalCatalogMode.MEMLOG.catalog(
          io, WAREHOUSE, "c", Map.of(LogCatalogProperties.MAX_APPEND_COUNT, "10000"));
      catalog.createNamespace(mixedNs);
      Table table = catalog.createTable(mixed, intSchema);
      table.newFastAppend().appendFile(dataFile("a")).commit();
      Lagging lagging = new Lagging();
      LocalCatalogMode.sharedLog(io, CATALOG).intercept(lagging);

      // the replica falls behind once the commit's record lands, after the commit read its base
      lagging.armOnNextAppend = true;
      table.newFastAppend().appendFile(dataFile("b")).commit();

      // the handle Iceberg refreshed after the commit, from the replica behind it
      assertThat(dataFiles(table))
          .as("the writer's table after its commit")
          .containsExactly("mem:///data/a.parquet", "mem:///data/b.parquet");
      assertThat(dataFiles(catalog.loadTable(mixed)))
          .as("the table, loaded again from the writer's catalog")
          .containsExactly("mem:///data/a.parquet", "mem:///data/b.parquet");
      lagging.armed = false;
      LogCatalog reader = LocalCatalogMode.MEMLOG.catalog(io, WAREHOUSE, "c", Map.of());
      assertThat(dataFiles(reader.loadTable(mixed)))
          .as("the log, read in full")
          .containsExactly("mem:///data/a.parquet", "mem:///data/b.parquet");
    }
  }

  /**
   * Storage with prefix and bulk operations, as S3, GCS and ADLS have (a table's FileIO passes
   * bulk deletes on only for those), whose bulk deletes fail.
   */
  private static class BulkDeletesFail extends InMemoryAtomicFileIO
      implements org.apache.iceberg.io.DelegateFileIO {
    private int bulkDeletes = 0;

    @Override
    public void deleteFiles(Iterable<String> paths) {
      bulkDeletes += 1;
      throw new org.apache.iceberg.io.BulkDeletionFailureException(
          org.apache.iceberg.relocated.com.google.common.collect.Iterables.size(paths));
    }

    @Override
    public Iterable<org.apache.iceberg.io.FileInfo> listPrefix(String prefix) {
      throw new UnsupportedOperationException("listPrefix");
    }

    @Override
    public void deletePrefix(String prefix) {
      throw new UnsupportedOperationException("deletePrefix");
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

  // ============================================================
  // Bug 64: a CAS write drops the committed transaction ids at or below its
  // watermark (now less fileio.catalog.committed.retention.ms) and records
  // the largest as the set's horizon, below which an id is unknown. Replay
  // asked the set about every record, so a record whose id was at or below
  // the horizon threw DedupHorizonExceededException on every read: the
  // catalog could not be read, nor compacted, by anyone. The record of a
  // writer whose clock was behind by more than the retention landed so; and
  // with retention 0, so did a writer's own CAS after a conflict, which
  // dropped the newer id of the commit it rebased on. Commits let the
  // exception escape too. Replay now rejects a record the set cannot vouch
  // for; a CAS write keeps the ids at or after its own; writers mint ids
  // above the horizon; and a commit whose id falls below it fails, or is
  // unknown if a write of it may have landed. Found by a review of the
  // committed-set docs (GC.md said the throw was unreachable).
  // ============================================================

  @Nested
  class CommittedSetHorizon {
    private final InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    private final String location = "mem:///wh/catalog-gc";
    /** CAS only, and every CAS drops every id older than now. */
    private final CatalogLog log = CatalogTestSupport.log(io, location, 0, Long.MAX_VALUE, 0L);

    private Replica<CatalogState> read() {
      return log.read();
    }

    private Replica<CatalogState> commit(
        Replica<CatalogState> base, java.util.function.Consumer<CatalogMutation> change) {
      CatalogMutation mut = new CatalogMutation(base);
      change.accept(mut);
      return mut.commit(log);
    }

    private Replica<CatalogState> catalogWithTwoNamespaces() throws InterruptedException {
      new CatalogMutation(log.empty()).commit(log);
      commit(read(), mut -> mut.createNamespace(Namespace.of("x")).createNamespace(Namespace.of("y")));
      Thread.sleep(2);
      return read();
    }

    @Test
    void aCasAfterAConflictKeepsItsOwnRecord() throws InterruptedException {
      Replica<CatalogState> base = catalogWithTwoNamespaces();
      boolean[] raced = {false};
      io.injectFaults((loc, strategy) -> {
        if (!raced[0]) {
          raced[0] = true; // just before the first writer's CAS, a second commits
          try {
            Thread.sleep(2);
            commit(base, mut -> mut.updateProperties(Namespace.of("y"), Map.of("k", "second")));
            Thread.sleep(2);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        return Fault.NONE;
      });

      // Its CAS conflicts, rebases, and drops the second writer's newer id.
      commit(base, mut -> mut.updateProperties(Namespace.of("x"), Map.of("k", "first")));

      Replica<CatalogState> after = read();
      assertThat(after.state().namespaceProperties(Namespace.of("x"))).containsEntry("k", "first");
      assertThat(after.state().namespaceProperties(Namespace.of("y"))).containsEntry("k", "second");
    }

    @Test
    void aRecordBelowTheHorizonIsRejectedNotUnreadable() throws Exception {
      Replica<CatalogState> base = catalogWithTwoNamespaces();
      commit(base, mut -> mut.updateProperties(Namespace.of("x"), Map.of("k", "v")));
      Replica<CatalogState> compacted = read();
      long horizon = compacted.committed().horizon();
      assertThat(horizon).isPositive();

      // A record minted at the horizon, as by a writer whose clock is behind, or one that
      // stalled past the retention between minting it and appending it.
      ProtoCodec.Transaction late =
          new ProtoCodec.Transaction(
              UuidV7.newUuidV7(horizon, new java.util.Random(64)),
              List.of(new ProtoCodec.SetNamespacePropertyAction(
                  compacted.state().namespaceId(Namespace.of("y")),
                  compacted.state().namespaceVersion(compacted.state().namespaceId(Namespace.of("y"))),
                  "late", "v")));
      byte[] record = ProtoCodec.encodeTransaction(late);
      ByteArrayOutputStream file = new ByteArrayOutputStream();
      try (InputStream in = io.newInputFile(location).newStream()) {
        file.write(in.readAllBytes());
      }
      int length = record.length;
      while ((length & ~0x7F) != 0) {
        file.write((length & 0x7F) | 0x80);
        length >>>= 7;
      }
      file.write(length);
      file.write(record);
      io.seed(location, file.toByteArray());

      Replica<CatalogState> after = read();
      assertThat(after.state().namespaceProperties(Namespace.of("y"))).doesNotContainKey("late");
      // ...and the catalog still takes commits.
      commit(after, mut -> mut.updateProperties(Namespace.of("y"), Map.of("next", "v")));
      assertThat(read().state().namespaceProperties(Namespace.of("y"))).containsEntry("next", "v");
    }

    @Test
    void aWriterBehindTheHorizonMintsAboveIt() throws Exception {
      Replica<CatalogState> base = catalogWithTwoNamespaces();
      // A checkpoint whose horizon is an hour ahead of this writer's clock, as a compaction by a
      // writer whose clock is ahead would leave.
      long ahead = System.currentTimeMillis() + 3_600_000L;
      Replica<CatalogState> current = read();
      CommittedSet withFuture =
          CommittedSet.builder()
              .inherit(current.committed().bytes())
              .add(UuidV7.newUuidV7(ahead, new java.util.Random(1)))
              .advanceHorizon(ahead) // a GC record of that writer, replayed into the checkpoint
              .build();
      io.seed(
          location,
          CatalogTestSupport.encodeFullFile(current.logId(), withFuture, current.state()));
      assertThat(read().committed().horizon()).isEqualTo(ahead);

      commit(read(), mut -> mut.updateProperties(Namespace.of("x"), Map.of("k", "v")));
      assertThat(read().state().namespaceProperties(Namespace.of("x"))).containsEntry("k", "v");
    }
  }

  // ============================================================
  // Bug 67: a transaction that read or updated a table and then dropped it
  // could never commit. CatalogFile.Mut staged the drop beside the read or
  // update, and replay rejects a record that drops a table it also reads or
  // updates (Bug 14), so the commit failed with CommitFailedException even
  // with nothing racing it, every time. updateTable and readTable already
  // refused a table the transaction dropped; dropTable now drops what the
  // transaction read or changed of the table, as dropNamespace drops its
  // pending property changes. The drop carries the same precondition, the
  // table's version, so the record means what the operations do in order.
  // Found by TestMultiWriterProperties' check that a commit nothing raced
  // is acknowledged, which the TLA+ model's oracle analysis motivated.
  // ============================================================

  @Nested
  class UpdateThenDropTable {
    private final InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    private final CatalogLog log = log(io, APPEND);
    private final TableIdentifier table = TableIdentifier.of("a", "t");

    private Replica<CatalogState> seeded() {
      CatalogMutation create = new CatalogMutation(seed(log, "a"));
      create.createTable(table, "mem:///a/t/v0");
      create.commit(log);
      return log.read();
    }

    @Test
    void aTableUpdatedThenDroppedInOneTransactionIsDropped() {
      CatalogMutation txn = new CatalogMutation(seeded());
      txn.updateTable(table, "mem:///a/t/v1");
      txn.dropTable(table);
      txn.commit(log);
      assertThat(log.read().state().containsTable(table)).isFalse();
    }

    @Test
    void aTableReadThenDroppedInOneTransactionIsDropped() {
      CatalogMutation txn = new CatalogMutation(seeded());
      txn.readTable(table);
      txn.dropTable(table);
      txn.commit(log);
      assertThat(log.read().state().containsTable(table)).isFalse();
    }

    @Test
    void theDropStillConflictsWithAConcurrentUpdate() {
      CatalogMutation txn = new CatalogMutation(seeded());
      CatalogMutation other = new CatalogMutation(log.read());
      other.updateTable(table, "mem:///a/t/other");
      other.commit(log);
      txn.updateTable(table, "mem:///a/t/v1");
      txn.dropTable(table);
      assertThatThrownBy(() -> txn.commit(log)).isInstanceOf(CommitFailedException.class);
      assertThat(log.read().state().location(table)).isEqualTo("mem:///a/t/other");
    }
  }

  // ============================================================
  // Bug 68: a replace of a table another writer evicted since it began
  // sends the lists of the snapshots it loaded inline (Bug 62). A snapshot
  // whose parent it also sent sent its whole list, as if the parent's were
  // a file, and replay built that list on the parent's, which the same
  // commit had just sent: the parent's manifests appeared twice in the
  // child's list. The parent of a snapshot now counts as inline when the
  // commit sends its list. Found by TestCatalogFaultProperties in a long run
  // (an inline-ML writer's replace, with a pointer writer's row delta
  // inside its commit).
  // ============================================================

  @Nested
  class ReplaceThatSendsAParentsList {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void keepsEachSnapshotsList(boolean byAPointerWriter) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = small(io, LocalCatalogMode.APPEND_ML);
      catalog.createNamespace(mixedNs);
      Table table = catalog.createTable(mixed, intSchema);
      table.newFastAppend().appendFile(dataFile("a")).commit();
      table.newFastAppend().appendFile(dataFile("b")).commit();
      Map<Long, List<String>> before = manifestLists(io, catalog.loadTable(mixed));

      org.apache.iceberg.Transaction replace =
          catalog.newReplaceTableTransaction(mixed, intSchema, false);
      boolean[] raced = {false};
      io.injectFaults((location, strategy) -> {
        if (!raced[0] && location.equals(CATALOG)) {
          raced[0] = true;
          // Either takes the table out of inline storage, its lists to files.
          if (byAPointerWriter) {
            catalog(io, LocalCatalogMode.APPEND).loadTable(mixed)
                .newFastAppend().appendFile(dataFile("c")).commit();
          } else {
            small(io, LocalCatalogMode.APPEND_ML).loadTable(mixed)
                .updateProperties().set("big", bigValue(2457)).commit();
          }
        }
        return Fault.NONE;
      });
      replace.commitTransaction();
      assertThat(raced[0]).isTrue();

      Map<Long, List<String>> after =
          manifestLists(io, catalog(io, LocalCatalogMode.APPEND).loadTable(mixed));
      before.forEach((snapshot, list) -> assertThat(after.get(snapshot))
          .as("the manifest list of snapshot %d", snapshot)
          .isEqualTo(list));
    }
  }

  // ============================================================
  // Bug 69: a catalog transaction commits its updates of a table on the
  // table's current metadata, checking only the requirements Iceberg derives
  // from them; for an append, that its branch is unchanged. An expiry that
  // cleaned expired metadata since could have removed the partition spec of
  // the appended files, or the schema the snapshot names: the commit added a
  // snapshot the table could not read. It now checks each snapshot it adds
  // against the metadata it commits. Found by TestCatalogFaultProperties
  // once its expiries varied (fuzz5); the client-side commit of the same
  // append fails since Iceberg issue C11.
  // ============================================================

  @Nested
  class TransactionOverARemovedSpecOrSchema {

    /**
     * An append staged on the table as it is now, committed as a table commit with no read set,
     * which applies its updates to the table's current metadata. A catalog transaction would fail
     * on any change to the table since (Bug 70).
     */
    private org.apache.iceberg.catalog.TableCommit stagedAppend(FileIOCatalog catalog) {
      catalog.createNamespace(mixedNs);
      catalog.createTable(mixed, intSchema);
      org.apache.iceberg.BaseTransaction txn =
          (org.apache.iceberg.BaseTransaction) catalog.loadTable(mixed).newTransaction();
      txn.newFastAppend().appendFile(dataFile("a")).commit();
      return org.apache.iceberg.catalog.TableCommit.create(
          mixed, txn.startMetadata(), txn.currentMetadata());
    }

    @ParameterizedTest
    @EnumSource(value = LocalCatalogMode.class, names = {"APPEND", "APPEND_ML"})
    void aRemovedSpecFailsTheCommit(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      org.apache.iceberg.catalog.TableCommit commit = stagedAppend(catalog(io, mode));

      // another writer partitions the table and expires its unpartitioned spec
      Table other = catalog(io, mode).loadTable(mixed);
      other.updateSpec().addField("id").commit();
      other.expireSnapshots().cleanExpiredMetadata(true).commit();
      assertThat(other.specs()).doesNotContainKey(0);

      assertThatThrownBy(() -> catalog(io, mode).commitTransaction(commit))
          .isInstanceOf(ValidationException.class)
          .hasMessageEndingWith("its partition spec 0 was removed concurrently");
      assertThat(catalog(io, mode).loadTable(mixed).currentSnapshot()).isNull();
    }

    @ParameterizedTest
    @EnumSource(value = LocalCatalogMode.class, names = {"APPEND", "APPEND_ML"})
    void aRemovedSchemaFailsTheCommit(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      org.apache.iceberg.catalog.TableCommit commit = stagedAppend(catalog(io, mode));

      // another writer adds a column and expires the schema before it
      Table other = catalog(io, mode).loadTable(mixed);
      other.updateSchema().addColumn("c", Types.IntegerType.get()).commit();
      other.expireSnapshots().cleanExpiredMetadata(true).commit();
      assertThat(other.schemas()).doesNotContainKey(0);

      assertThatThrownBy(() -> catalog(io, mode).commitTransaction(commit))
          .isInstanceOf(ValidationException.class)
          .hasMessageEndingWith("its schema 0 was removed concurrently");
      assertThat(catalog(io, mode).loadTable(mixed).currentSnapshot()).isNull();
    }
  }

  /**
   * Bug 70: a catalog transaction checked a table it writes only by the requirements Iceberg
   * derives from its commit, between its first read of the table and the commit. A change those
   * requirements don't name went through and was lost or broke the table: another writer's
   * property change (a name mapping a schema update rewrote), a commit to another branch of it,
   * or a tag on a snapshot the transaction expired. A transaction now conflicts on any change to
   * a table it writes since it first read it (errata D12).
   */
  @Nested
  class WrittenTableChangedSinceRead {

    @Test
    void aPropertyChangeFailsTheTransaction() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, LocalCatalogMode.APPEND);
      catalog.createNamespace(mixedNs);
      catalog.createTable(mixed, intSchema);
      // snapshot isolation: a serializable one fails first on its read of the table, which has
      // no snapshot
      CatalogTransaction txn = catalog.createTransaction(CatalogTransaction.IsolationLevel.SNAPSHOT);
      txn.asCatalog().loadTable(mixed).newFastAppend().appendFile(dataFile("a")).commit();

      // another writer sets a property, which no requirement of the append names
      catalog(io, LocalCatalogMode.APPEND).loadTable(mixed).updateProperties().set("k", "v").commit();

      assertThatThrownBy(txn::commitTransaction)
          .isInstanceOf(CommitFailedException.class)
          .hasMessage("Cannot commit transaction: table ns.t changed since the transaction read it");
      Table table = catalog(io, LocalCatalogMode.APPEND).loadTable(mixed);
      assertThat(table.properties()).containsEntry("k", "v");
      assertThat(table.currentSnapshot()).isNull();
    }

    @ParameterizedTest
    @EnumSource(CatalogTransaction.IsolationLevel.class)
    void aTagOnASnapshotTheTransactionExpiresFailsIt(CatalogTransaction.IsolationLevel isolation) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      FileIOCatalog catalog = catalog(io, LocalCatalogMode.APPEND);
      catalog.createNamespace(mixedNs);
      Table table = catalog.createTable(mixed, intSchema);
      table.newFastAppend().appendFile(dataFile("a")).commit();
      long first = table.currentSnapshot().snapshotId();
      table.newFastAppend().appendFile(dataFile("b")).commit();
      CatalogTransaction txn = catalog.createTransaction(isolation);
      txn.asCatalog().loadTable(mixed).expireSnapshots().expireSnapshotId(first).commit();

      // another writer tags the snapshot the transaction expires
      catalog(io, LocalCatalogMode.APPEND)
          .loadTable(mixed)
          .manageSnapshots()
          .createTag("tag", first)
          .commit();

      assertThatThrownBy(txn::commitTransaction).isInstanceOf(CommitFailedException.class);
      Table after = catalog(io, LocalCatalogMode.APPEND).loadTable(mixed);
      assertThat(after.refs()).containsKey("tag");
      assertThat(after.snapshot(first)).isNotNull();
    }
  }

  /**
   * Bug 73: a replace transaction's record merged as an append. A replace starts its table's
   * history afresh: its snapshot has no parent, and main moves to it. The record carries no ref
   * removal (a delta sends refs' final values), so it reads as an append to a table that had no
   * snapshot. Landing it on a changed table, the merge re-parented the snapshot onto main's head
   * and built its manifest list from the head's: the replaced files stayed. The fault fuzzer
   * found it at phase 2, before a writer pinned what Iceberg commits once (a replace removes
   * main). Replay no longer relies on the pin: a record that moves a ref to a snapshot starting
   * a fresh history resets the ref (TableChange), and the merge refuses to put a parentless
   * snapshot on a branch with a head (Landing).
   */
  @Nested
  class ReplaceIsNotAnAppend {
    private final TableIdentifier table = TableIdentifier.of("db", "t");
    private final long first = 0x1111111111111111L;
    private final long replacement = 0x5555555555555555L;

    private ManifestFile manifest(String name, long snapshotId) {
      return new ProtoCodec.DecodedManifestFile(
          "mem:///db/t/metadata/" + name + ".avro", 4_096L, 0, 0, 0L, 0L, snapshotId,
          1, 0, 0, 10L, 0L, 0L, List.of(), null, null);
    }

    /** A snapshot's delta: a first snapshot of main (no parent), with its one manifest. */
    private byte[] firstSnapshot(long snapshotId, String manifest, String operation,
        org.apache.iceberg.TableMetadata base) {
      Map<String, String> summary = new HashMap<>();
      summary.put("operation", operation);
      summary.put("added-data-files", "1");
      summary.put("added-records", "10");
      summary.put("total-data-files", "1");
      summary.put("total-records", "10");
      return InlineDeltaCodec.encodeDelta(
          List.of(
              new InlineDeltaCodec.AddSnapshotUpdate(
                  snapshotId, "", summary, 0L, null, null, null, null, null, Footprint.BLIND),
              new InlineDeltaCodec.SetSnapshotRefUpdate("main", snapshotId, "branch", 0, 0L, 0L),
              new InlineDeltaCodec.AddManifestUpdate(snapshotId, manifest(manifest, snapshotId),
                  "", 0)),
          base.lastUpdatedMillis() + 1_000L,
          base);
    }

    @ParameterizedTest
    @ValueSource(strings = {"lamport-minus-properties", "concern-vector", "blind-append",
        "partition-vector", "manifest-window"})
    void aStaleReplaceConflicts(String encoding) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      CatalogLog log = CatalogTestSupport.log(io, "mem:///catalog");
      log.bootstrap(encoding);
      CatalogMutation create = new CatalogMutation(log.read());
      create.createNamespace(Namespace.of("db"));
      create.createTableInline(table, CatalogTestSupport.newInlineTable("mem:///db/t"),
          CatalogTestSupport.inlineTableCodec());
      create.commit(log);
      Replica<CatalogState> empty = log.read();
      CatalogMutation append = new CatalogMutation(empty);
      append.updateTableInlineDelta(table, firstSnapshot(first, "f2", "append",
          CatalogTestSupport.inlineMetadata(empty.state(), table)));
      append.commit(log);

      // a replace staged on main at the first snapshot, unpinned
      Replica<CatalogState> read = log.read();
      CatalogMutation replace = new CatalogMutation(read);
      replace.updateTableInlineDelta(table, firstSnapshot(replacement, "f5", "overwrite",
          CatalogTestSupport.inlineMetadata(read.state(), table)));
      // another writer's property change lands first
      CatalogMutation property = new CatalogMutation(log.read());
      property.updateTableInlineDelta(table, CatalogTestSupport.propertyDelta("k", "v",
          CatalogTestSupport.inlineMetadata(read.state(), table).lastUpdatedMillis() + 2_000L));
      property.commit(log);

      assertThatThrownBy(() -> replace.commit(log)).isInstanceOf(CommitFailedException.class);
      CatalogState state = log.read().state();
      org.apache.iceberg.TableMetadata after = CatalogTestSupport.inlineMetadata(state, table);
      assertThat(after.currentSnapshot().snapshotId()).isEqualTo(first);
      assertThat(after.properties()).containsEntry("k", "v");
    }
  }

  /**
   * Bug 74: a transaction could stage a namespace drop after creating an inline table in it.
   * dropNamespace counted the pointer tables the transaction creates, not the inline ones, so
   * the drop staged, and replay rejected the commit: a CommitFailedException with nothing
   * racing, where staging should have refused the drop. The multi-writer fuzzer found it once
   * its transactions created inline tables.
   */
  @Nested
  class DropNamespaceWithANewInlineTable {
    @Test
    void stagingRefusesTheDrop() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      CatalogLog log = CatalogTestSupport.log(io, "mem:///catalog");
      log.bootstrap(LamportEncoding.NAME);
      CatalogMutation create = new CatalogMutation(log.read());
      create.createNamespace(Namespace.of("c"));
      create.commit(log);

      CatalogMutation txn = new CatalogMutation(log.read());
      txn.createTableInline(TableIdentifier.of("c", "t"),
          CatalogTestSupport.newInlineTable("mem:///c/t"), CatalogTestSupport.inlineTableCodec());
      assertThatThrownBy(() -> txn.dropNamespace(Namespace.of("c")))
          .isInstanceOf(NamespaceNotEmptyException.class);
    }
  }

  /** A command of a rebase-oracle sample ({@link TestRebaseProperties}). */
  private static TableScript.Cmd cmd(TableScript.Kind kind, int a, int b, int c) {
    return new TableScript.Cmd(kind, a, b, c);
  }

  /**
   * Bug 75: a merged record kept delete files that Iceberg's retry drops. A merging producer
   * (MergingSnapshotProducer: a delete, an overwrite, a row delta, a rewrite) drops the delete
   * files older than every data file it keeps, which can apply to no row, from the delete manifests
   * it reads, which, as it removes any data file, is every one. Which ones depends on the table it
   * commits on: there, a delete of the last data file that lands after another commit added a
   * delete file, in another partition, must drop it too. The rebase oracle found it under
   * partition-vector. The merge now refuses a snapshot whose list keeps a delete manifest older
   * than its oldest data (Landing).
   */
  @Nested
  class DeleteFilesARetryDrops {
    @ParameterizedTest
    @ValueSource(strings = {PartitionVectorEncoding.NAME, ManifestWindowEncoding.NAME})
    void aDeleteOfTheLastDataFile(String encoding) {
      TestRebaseProperties.verify(encoding, 2, true, LocalCatalogMode.APPEND_ML,
          List.of(cmd(TableScript.Kind.FAST_APPEND, 0, 0, 2)),
          List.of(cmd(TableScript.Kind.EQUALITY_DELETE, 9, 5, 3),
              cmd(TableScript.Kind.BRANCH_APPEND, 4, 0, 5)),
          cmd(TableScript.Kind.DELETE_PARTITION, 10, 11, 10));
    }
  }

  /**
   * Bug 76: a merged record's row ids, in a table upgraded to v3. A commit gives row ids to every
   * data manifest of its snapshot's list that has none (one written before the upgrade), from the
   * table's next row id. A branch append on a head from before the upgrade did, and merged on a
   * table whose next row id had moved, it kept the ids it gave on its base, which the commits since
   * had given out too. Its record carried the manifest whole (its parent's list was a file), so
   * replay also changed the entry the table's other lists share for its path. And an append that
   * landed on a head whose manifests had none (in the merge's own probe, after a rollback) left
   * them without. The rebase oracle found both. The merge now moves the ids a record gave with its
   * own, and refuses a record that would change a shared entry, or that lands on manifests without
   * row ids (Landing).
   */
  @Nested
  class RowIdsOfManifestsWrittenBeforeTheUpgrade {
    @Test
    void aBranchAppendOnAHeadWithoutRowIds() {
      TestRebaseProperties.verify(BlindAppendEncoding.NAME, 2, true, LocalCatalogMode.APPEND_ML,
          List.of(cmd(TableScript.Kind.FAST_APPEND, 0, 0, 7), cmd(TableScript.Kind.UPGRADE, 11, 1, 10),
              cmd(TableScript.Kind.CREATE_BRANCH, 0, 10, 9),
              cmd(TableScript.Kind.EQUALITY_DELETE, 1, 4, 6),
              cmd(TableScript.Kind.SET_PROPERTY, 3, 10, 6)),
          List.of(cmd(TableScript.Kind.MERGE_APPEND, 9, 2, 2),
              cmd(TableScript.Kind.REPLACE_PARTITIONS, 4, 6, 1)),
          cmd(TableScript.Kind.BRANCH_APPEND, 11, 10, 0));
    }

    @Test
    void anAppendMergedOntoARollback() {
      TestRebaseProperties.verify(AdmitDeltasEncoding.register(), 2, true,
          LocalCatalogMode.APPEND_ML,
          List.of(cmd(TableScript.Kind.FAST_APPEND, 0, 0, 0),
              cmd(TableScript.Kind.MERGE_APPEND, 6, 6, 10),
              cmd(TableScript.Kind.PARTITION_STATISTICS, 9, 0, 8),
              cmd(TableScript.Kind.SET_PROPERTY, 8, 2, 11),
              cmd(TableScript.Kind.ADD_PARTITION_FIELD, 8, 9, 8),
              cmd(TableScript.Kind.UPGRADE, 0, 10, 11), cmd(TableScript.Kind.RENAME_COLUMN, 4, 2, 0),
              cmd(TableScript.Kind.REPLACE_PARTITIONS, 3, 0, 9)),
          List.of(cmd(TableScript.Kind.ROLLBACK, 7, 4, 3)),
          cmd(TableScript.Kind.FAST_APPEND, 6, 7, 1));
    }
  }

  /**
   * Bug 77: the fault-free rebase oracle read a table with two deletion vectors for one data file.
   * A fuzzer's bug, not the catalog's: TableScript's merge row delta took its data file from a
   * scan, which drops column bounds unless it includes column stats, so it read the file's
   * partition as the default and gave its row delta a conflict detection filter for another one.
   * Iceberg's checks for concurrent delete files, deletion vectors among them, are scoped by that
   * filter, so Iceberg's own retry committed a second DV for the file, as did the merge under
   * partition-vector, which judges the same checks. The script's scans now include column stats
   * (a rewrite's new file also took the wrong bounds).
   */
  @Nested
  class ConcurrentDeletionVectors {
    @ParameterizedTest
    @ValueSource(strings = {LamportEncoding.NAME, PartitionVectorEncoding.NAME,
        ManifestWindowEncoding.NAME})
    void aRowDeltaAfterAnotherDvForItsFile(String encoding) {
      TestRebaseProperties.verify(encoding, 3, true, LocalCatalogMode.APPEND_ML,
          List.of(cmd(TableScript.Kind.FAST_APPEND, 0, 0, 1)),
          List.of(cmd(TableScript.Kind.POSITION_DELETE, 3, 7, 0)),
          cmd(TableScript.Kind.MERGE_ROW_DELTA, 5, 11, 9));
    }
  }

  /**
   * Bug 78: a table handle whose table was dropped committed with no base. After a drop, a
   * handle's refresh threw NoSuchTableException once and left it no metadata; the next refresh
   * took the path of a table not created yet and returned none (Iceberg's other metastore
   * operations do the same). An update on the handle then read a null base: a properties update
   * failed with a NullPointerException, and a commit with no base would have created the table
   * again. The fault fuzzer found it at phase 3, where an expiry's file cleanup, which the fork
   * lets fail after the expiry commits, swallowed the first NoSuchTableException. A handle that
   * has loaded its table now reports the drop on every refresh.
   */
  @Nested
  class HandleToADroppedTable {
    @ParameterizedTest
    @EnumSource(LocalCatalogMode.class)
    void keepsReportingTheDrop(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      LogCatalog catalog = mode.catalog(io, "mem:///warehouse", "c", Map.of());
      catalog.createNamespace(Namespace.of("ns"));
      TableIdentifier id = TableIdentifier.of("ns", "t");
      catalog.createTable(id, new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())));
      Table handle = mode.catalog(io, "mem:///warehouse", "c", Map.of()).loadTable(id);
      catalog.dropTable(id, false);

      assertThatThrownBy(handle::refresh).isInstanceOf(NoSuchTableException.class);
      assertThatThrownBy(() -> handle.updateProperties().set("k", "v").commit())
          .isInstanceOf(NoSuchTableException.class);
      assertThatThrownBy(() -> handle.newTransaction().updateProperties().set("k", "v").commit())
          .isInstanceOf(NoSuchTableException.class);
      assertThat(catalog.tableExists(id)).isFalse();
    }
  }

  /**
   * Bug 79: a merged snapshot built its manifest list on a parent's inline list that was gone. A
   * delta's manifests are relative to its new snapshot's parent's inline list, which replay builds
   * the snapshot's list on where it lands. An eviction (here the first v3 commit of a branch, which
   * gave row ids to a manifest the inline lists held) writes the table's lists to files and drops
   * them, and a return to inline storage (the location change's full metadata) does not bring them
   * back. A staged append on main, whose parent was still main's head where it landed, merged
   * there with only its own manifest: its snapshot lost the parent's files. The merge checked the
   * parent's list only for a snapshot it moved onto another head. The merge's probe found it on a
   * table upgraded to v3 (TestRebaseProperties, once the oracle compared each snapshot's files);
   * every conflict encoding stamps every concern on an eviction, so none admitted such a record.
   * The merge now refuses a snapshot whose parent has no inline list where it lands (Landing).
   */
  @Nested
  class ParentListEvicted {
    @ParameterizedTest
    @EnumSource(value = LocalCatalogMode.class, names = {"APPEND_ML", "CAS_ML"})
    void aStagedAppendAfterAnEvictionAndAReturn(LocalCatalogMode mode) {
      TestRebaseProperties.verify(AdmitDeltasEncoding.register(), 2, false, mode,
          List.of(cmd(TableScript.Kind.BRANCH_APPEND, 9, 1, 11),
              cmd(TableScript.Kind.UPGRADE, 0, 0, 0),
              cmd(TableScript.Kind.LONG_OVERWRITE, 6, 6, 3)),
          List.of(cmd(TableScript.Kind.BRANCH_APPEND, 10, 7, 3),
              cmd(TableScript.Kind.SET_LOCATION, 10, 9, 8)),
          cmd(TableScript.Kind.STAGED_APPEND, 9, 10, 1));
    }
  }

  /**
   * Bug 80: a checkpoint's bytes depended on how its state's maps were built. A table's map of
   * snapshots to inline manifest lists became a HashMap when a builder changed it, and the
   * checkpoint wrote it in the map's iteration order, which depends on the map's history: a state
   * replayed from a checkpoint and the same state built on a replica (copied, then changed) wrote
   * different bytes. A catalog object never compared two writers' checkpoints, but every
   * checkpoint at one position must be the same (I11), as a shared log's compactors need; the
   * runtime's check of each fold of a read onto a replica against a replay of the whole view found
   * it. A checkpoint now writes everything it holds in maps in the order of the keys (ProtoCodec).
   */
  @Nested
  class CheckpointOrderOfMaps {
    @Test
    void theOrderAStatesMapsWereBuiltInLeavesItsCheckpoint() {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      LogCatalog catalog = LocalCatalogMode.APPEND_ML.catalog(io, "mem:///wh", "bug80", Map.of());
      catalog.createNamespace(Namespace.of("ns"));
      Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
      Table table = catalog.createTable(TableIdentifier.of("ns", "t"), schema);
      for (int i = 0; i < 24; i++) {
        table.newFastAppend()
            .appendFile(org.apache.iceberg.DataFiles.builder(table.spec())
                .withPath("mem:///data/f" + i + ".parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .build())
            .commit();
      }
      CatalogState state = catalog.log().read().state();
      int tblId = state.tableId(TableIdentifier.of("ns", "t"));
      Map<String, ManifestFile> pool = state.manifestPool(tblId);
      Map<Long, List<String>> lists = state.snapshotManifests(tblId);
      assertThat(lists).hasSizeGreaterThan(20);

      // The same state, its table's pool and lists put back in the reverse order
      CatalogState.Builder reordered = CatalogStateMachine.INSTANCE.toBuilder(state);
      reordered.removeSnapshotManifests(tblId, lists.keySet());
      List<String> paths = new ArrayList<>(pool.keySet());
      Collections.reverse(paths);
      paths.forEach(path -> reordered.addManifestToPool(tblId, pool.get(path)));
      List<Long> snapshots = new ArrayList<>(lists.keySet());
      Collections.reverse(snapshots);
      snapshots.forEach(id -> reordered.setSnapshotManifests(tblId, id, lists.get(id)));
      CatalogState other = reordered.build();

      assertThat(other.manifestPool(tblId)).isEqualTo(pool);
      assertThat(other.snapshotManifests(tblId)).isEqualTo(lists);
      assertThat(CatalogStateMachine.INSTANCE.encodeCheckpoint(other))
          .isEqualTo(CatalogStateMachine.INSTANCE.encodeCheckpoint(state));
    }
  }

  /**
   * Bug 81: a replace transaction's record merged as an append onto a racing schema change. A
   * replace restates the table's schema, partition spec and sort order, but its record is a delta
   * from the version it read, which carries only what differs there: on a table with no snapshot
   * whose schema was already the replacement's, a new snapshot, main and the properties. Landing
   * it on a version where another writer had added a column, a merging encoding found nothing in
   * common with the column's change and applied it, and the column stayed current; Iceberg's
   * retry of the replace rebuilds the replacement on that version and makes its schema current
   * again. Replay cannot tell such a record from a first append to an empty table (Bug 73), whose
   * merge is right, so the writer says what it is: a replace's delta restates its schema, partition
   * spec and sort order, as Iceberg's REST client does, and replay applies a delta that sets them
   * only on the version it read. The fault fuzzer found it (round ir2).
   */
  @Nested
  class ReplaceRestatesTheSchema {
    private final TableIdentifier id = TableIdentifier.of("ns", "t");
    private final Schema seed =
        new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    @ParameterizedTest
    @ValueSource(strings = {"lamport", "lamport-minus-properties", "concern-vector",
        "blind-append", "partition-vector", "manifest-window"})
    void aReplaceRacingASchemaChangeKeepsItsSchema(String encoding) {
      for (LocalCatalogMode mode : List.of(LocalCatalogMode.APPEND_TM, LocalCatalogMode.APPEND_ML,
          LocalCatalogMode.CAS_TM, LocalCatalogMode.CAS_ML)) {
        // A writer that pins what Iceberg commits once, and one that leaves it to replay
        for (boolean pins : new boolean[] {true, false}) {
          racingReplace(encoding, mode, pins);
        }
      }
    }

    private void racingReplace(String encoding, LocalCatalogMode mode, boolean pins) {
      String described = mode.name() + (pins ? "" : ", unpinned");
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      Map<String, String> props = Map.of(LogCatalogProperties.CONFLICT_ENCODING, encoding);
      LogCatalog first = mode.catalog(io, "mem:///w", "c", props);
      if (!pins) {
        first.leavingCommitOnceUnpinned();
      }
      LogCatalog second = mode.catalog(io, "mem:///w", "c", props);
      first.createNamespace(Namespace.of("ns"));
      first.createTable(id, seed);

      org.apache.iceberg.Transaction replace = first.buildTable(id, seed).replaceTransaction();
      replace.newAppend().appendFile(file("f1")).commit();
      // The column lands just before the replace's first write to the catalog
      boolean[] raced = {false};
      io.injectFaults(
          (location, strategy) -> {
            if (!raced[0]) {
              raced[0] = true;
              second.loadTable(id).updateSchema().addColumn("c0", Types.IntegerType.get())
                  .commit();
            }
            return Fault.NONE;
          });
      replace.commitTransaction();
      io.injectFaults((location, strategy) -> Fault.NONE);

      Table table = mode.<LogCatalog>catalog(io, "mem:///w", "c", props).loadTable(id);
      assertThat(raced[0]).as(described).isTrue();
      assertThat(table.schema().findField("c0")).as(described).isNull();
      assertThat(table.currentSnapshot().addedDataFiles(table.io()))
          .as(described)
          .extracting(f -> f.location())
          .containsExactly("mem:///data/f1.parquet");
    }

    @Test
    void aReplaceCommitsADeltaThatRestatesItsSchemaSpecAndSortOrder() throws IOException {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      LogCatalog catalog = LocalCatalogMode.APPEND_TM.catalog(io, "mem:///w", "c", Map.of());
      catalog.createNamespace(Namespace.of("ns"));
      catalog.createTable(id, seed);

      org.apache.iceberg.Transaction replace = catalog.buildTable(id, seed).replaceTransaction();
      replace.newAppend().appendFile(file("f1")).commit();
      replace.commitTransaction();

      byte[] bytes;
      try (InputStream in = io.newInputFile("mem:///w/catalog-c").newStream()) {
        bytes = in.readAllBytes();
      }
      List<ProtoCodec.Transaction> records = CatalogTestSupport.decodeRecords(bytes);
      ProtoCodec.UpdateTableInlineAction action = (ProtoCodec.UpdateTableInlineAction)
          records.get(records.size() - 1).actions().get(0);
      assertThat(action.deltaBytes).isNotNull();
      assertThat(action.pinned).isTrue();
      assertThat(InlineDeltaCodec.decodeDelta(action.deltaBytes).updates)
          .hasAtLeastOneElementOfType(InlineDeltaCodec.SetCurrentSchemaUpdate.class)
          .hasAtLeastOneElementOfType(InlineDeltaCodec.SetDefaultSpecUpdate.class)
          .hasAtLeastOneElementOfType(InlineDeltaCodec.SetDefaultSortOrderUpdate.class);
    }
  }

  /**
   * Bug 82: an expiry in the same table transaction as commits left a manifest of the expired
   * snapshot in an inline-ML table's pool, though no list named it any more. Replay dropped the
   * expired snapshot's list and collected the pool as it met the expiry, while a new snapshot's
   * list was still its parent's as it was when the snapshot was added, which named the manifest;
   * the list was settled after the whole delta, without it. A leak, not a misread (a load resolves
   * only listed paths), but one no later commit collects. Replay now drops a delta's removed
   * snapshots once its new snapshots' lists are settled. The rebase oracle found it, by the
   * checkpoint check of every write (CheckpointRoundTrip.malformedLists).
   */
  @Nested
  class ExpiryInATransactionCollectsThePool {
    private final TableIdentifier id = TableIdentifier.of("ns", "t");

    @ParameterizedTest
    @EnumSource(names = {"APPEND_ML", "CAS_ML", "COMPACT_ML"})
    void anExpiryBesideCommitsLeavesNoManifestNoListNames(LocalCatalogMode mode) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      LogCatalog catalog = mode.catalog(io, "mem:///w", "c", Map.of());
      catalog.createNamespace(Namespace.of("ns"));
      Table table = catalog.createTable(id, intSchema);
      table.newFastAppend().appendFile(file("f1")).commit();

      // The delete rewrites the first snapshot's manifest; the append drops the rewritten one, all
      // deletes; the expiry removes the first snapshot, whose manifest no list names after it
      org.apache.iceberg.Transaction txn = table.newTransaction();
      txn.newDelete().deleteFile("mem:///data/f1.parquet").commit();
      txn.newFastAppend().appendFile(file("f2")).commit();
      txn.expireSnapshots().expireOlderThan(Long.MAX_VALUE).retainLast(2)
          .cleanExpiredFiles(false).commit();
      txn.commitTransaction();

      CatalogState state = CatalogTestSupport.log(io, "mem:///w/catalog-c").read().state();
      int tableId = state.tableId(id);
      assertThat(state.snapshotManifests(tableId)).as(mode.name()).hasSize(2);
      assertThat(CheckpointRoundTrip.malformedLists(state)).as(mode.name()).isNull();
    }
  }
}
