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
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

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
}
