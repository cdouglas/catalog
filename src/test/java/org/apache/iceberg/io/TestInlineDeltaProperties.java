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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.RandomDistribution;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.statistics.Statistics;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;

/**
 * The inline delta round trip: a commit that an inline catalog sends as a delta replays to the
 * metadata the writer committed. {@link InlineDeltaCodec#computeDelta} must either return null,
 * which sends the full metadata, or a delta that holds every change; a change it has no update
 * for and does not refuse is lost on replay (Bugs 37 and 39: a format-version upgrade and a
 * removed ref, each lost when the commit made another change too).
 *
 * <p>A generated schedule runs Iceberg operations, alone or several in a table transaction, on a
 * table in an {@link InMemoryCatalog}: appends, deletes, schema, partition-spec and sort-order
 * evolution, properties, branches and tags, staged appends and cherry-picks, rollbacks, snapshot
 * expiry, location changes, upgrades and replace transactions. Each commit takes the table from
 * {@code base} to {@code next}; its delta, encoded and replayed by {@link
 * ProtoCodec.UpdateTableInlineAction} on {@code base} as the inline codec stores it, must equal
 * {@code next} as the codec stores it (less the metadata log, which inline storage synthesizes).
 * Operations Iceberg rejects are skipped. Every committed version must also survive the inline
 * codec exactly, metadata log included.
 */
class TestInlineDeltaProperties {

  private static final TableIdentifier ID = TableIdentifier.of("ns", "t");
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          optional(2, "data", Types.StringType.get()),
          optional(3, "cat", Types.IntegerType.get()));
  private static final ObjectMapper JSON = new ObjectMapper();

  @Property(tries = 400)
  void deltaReplaysToTheCommittedMetadata(
      @ForAll @IntRange(min = 2, max = 3) int formatVersion,
      @ForAll boolean partitioned,
      @ForAll boolean structural,
      @ForAll("schedules") List<Cmd> schedule) {
    new Run(formatVersion, partitioned,
            structural ? InlineMetadataCodecs.STRUCTURAL : InlineMetadataCodecs.JSON_GZIP)
        .execute(schedule);
  }

  @Provide
  Arbitrary<List<Cmd>> schedules() {
    Arbitrary<Integer> small = Arbitraries.integers().between(0, 11);
    return Combinators.combine(Arbitraries.of(Kind.values()), small, small, small)
        .as(Cmd::new)
        .list()
        .ofMinSize(1)
        .ofMaxSize(30)
        .withSizeDistribution(RandomDistribution.uniform());
  }

  enum Kind {
    FAST_APPEND,
    MERGE_APPEND,
    STAGED_APPEND,
    BRANCH_APPEND,
    DELETE_FILE,
    /** Adds an equality delete file. */
    EQUALITY_DELETE,
    /** Adds a position delete file for a live data file: in v3, a deletion vector. */
    POSITION_DELETE,
    ADD_COLUMN,
    DELETE_COLUMN,
    RENAME_COLUMN,
    ADD_PARTITION_FIELD,
    REMOVE_PARTITION_FIELD,
    SORT_ORDER,
    SET_PROPERTY,
    REMOVE_PROPERTY,
    CREATE_BRANCH,
    CREATE_TAG,
    REMOVE_REF,
    FAST_FORWARD,
    ROLLBACK,
    CHERRY_PICK,
    EXPIRE_SNAPSHOTS,
    SET_LOCATION,
    UPGRADE,
    REPLACE,
    /** Runs the next {@code a % 3 + 2} commands in one table transaction. */
    TRANSACTION
  }

  /** One generated command; {@code a}, {@code b} and {@code c} pick operands by index. */
  static final class Cmd {
    final Kind kind;
    final int a;
    final int b;
    final int c;

    Cmd(Kind kind, int a, int b, int c) {
      this.kind = kind;
      this.a = a;
      this.b = b;
      this.c = c;
    }

    @Override
    public String toString() {
      return String.format("%s(%d,%d,%d)", kind, a, b, c);
    }
  }

  private static final class Run {
    private final InMemoryCatalog catalog = new InMemoryCatalog();
    private final InlineMetadataCodec codec;
    private final StringBuilder trace = new StringBuilder();
    private Table table;
    private int nextFile = 0;
    private int nextValue = 0;

    Run(int formatVersion, boolean partitioned, InlineMetadataCodec codec) {
      this.codec = codec;
      catalog.initialize("c", Map.of());
      catalog.createNamespace(Namespace.of("ns"));
      PartitionSpec spec =
          partitioned
              ? PartitionSpec.builderFor(SCHEMA).identity("cat").build()
              : PartitionSpec.unpartitioned();
      this.table =
          catalog
              .buildTable(ID, SCHEMA)
              .withPartitionSpec(spec)
              .withProperty(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
              .create();
      trace.append(String.format("v%d %s, %s%n", formatVersion,
          partitioned ? "partitioned" : "unpartitioned", codec.getClass().getSimpleName()));
    }

    void execute(List<Cmd> schedule) {
      for (int i = 0; i < schedule.size(); i++) {
        Cmd cmd = schedule.get(i);
        TableMetadata base = ops().current();
        if (cmd.kind == Kind.TRANSACTION) {
          int n = Math.min(cmd.a % 3 + 2, schedule.size() - i - 1);
          List<Cmd> group = schedule.subList(i + 1, i + 1 + n);
          i += n;
          trace.append("transaction ").append(group).append('\n');
          try {
            Transaction txn = table.newTransaction();
            int staged = 0;
            for (Cmd member : group) {
              if (member.kind == Kind.TRANSACTION || member.kind == Kind.REPLACE) {
                continue;
              }
              try {
                run(member, txn.table());
                staged += 1;
              } catch (RuntimeException e) {
                trace.append("  member skipped: ").append(e).append('\n');
              }
            }
            if (staged == 0) {
              throw new IllegalStateException("no member staged");
            }
            txn.commitTransaction();
          } catch (RuntimeException e) {
            trace.append("  skipped: ").append(e).append('\n');
            Statistics.label("transaction").collect("rejected");
            continue;
          }
          Statistics.label("transaction").collect("committed");
        } else {
          trace.append(cmd).append('\n');
          try {
            if (cmd.kind == Kind.REPLACE) {
              replace(cmd);
            } else {
              run(cmd, table);
            }
          } catch (RuntimeException e) {
            trace.append("  skipped: ").append(e).append('\n');
            Statistics.label("operation").collect(cmd.kind + " rejected");
            continue;
          }
          Statistics.label("operation").collect(cmd.kind);
        }
        TableMetadata next = ops().refresh();
        check(base, next);
      }
    }

    private TableOperations ops() {
      return ((HasTableOperations) table).operations();
    }

    /**
     * Checks that the inline codec stores {@code next} exactly, metadata log included, and that the
     * delta of the commit from {@code base} replays to it.
     */
    private void check(TableMetadata base, TableMetadata next) {
      if (next.metadataFileLocation().equals(base.metadataFileLocation())) {
        return;
      }
      String lossy =
          difference(json(next), json(codec.decodeFull(codec.encode(next), "inline://next")));
      if (lossy != null) {
        throw new AssertionError("the codec does not store the metadata exactly: " + lossy
            + "\ntrace:\n" + trace);
      }
      TableMetadata stored = codec.decodeFull(codec.encode(base), "inline://base");
      List<InlineDeltaCodec.DeltaUpdate> delta = InlineDeltaCodec.computeDelta(stored, next, "");
      if (delta == null) {
        Statistics.label("commit").collect("full");
        return;
      }
      Statistics.label("commit").collect("delta");
      byte[] deltaBytes = InlineDeltaCodec.encodeDelta(delta, next.lastUpdatedMillis());
      ProtoCatalogFile.Builder builder =
          ProtoCatalogFile.builder(CatalogStateGeneratorsStub.LOCATION)
              .addInlineTable(1, 0, "t", 1, codec.encode(stored), codec.tag(), "");
      ProtoCodec.UpdateTableInlineAction.delta(1, 1, deltaBytes).apply(builder);
      TableMetadata replayed =
          codec.decodeFull(builder.inlineMetadata(1), "inline://replayed");
      TableMetadata expected = codec.decodeFull(codec.encode(next), "inline://expected");
      String difference = difference(canonical(expected), canonical(replayed));
      if (difference != null) {
        throw new AssertionError(
            "the delta replays to other metadata: " + difference + "\ntrace:\n" + trace);
      }
    }

    private void run(Cmd cmd, Table target) {
      List<Snapshot> snapshots = new ArrayList<>();
      target.snapshots().forEach(snapshots::add);
      switch (cmd.kind) {
        case FAST_APPEND:
          target.newFastAppend().appendFile(newFile(target)).commit();
          break;
        case MERGE_APPEND:
          target.newAppend().appendFile(newFile(target)).commit();
          break;
        case STAGED_APPEND:
          target.newFastAppend().appendFile(newFile(target)).stageOnly().commit();
          break;
        case BRANCH_APPEND:
          target.newFastAppend().appendFile(newFile(target)).toBranch(ref(cmd.b)).commit();
          break;
        case DELETE_FILE:
          target.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();
          break;
        case EQUALITY_DELETE:
          target.newRowDelta().addDeletes(
                  org.apache.iceberg.FileMetadata.deleteFileBuilder(target.spec())
                      .ofEqualityDeletes(1)
                      .withPath("mem:///data/eq" + nextFile++ + ".parquet")
                      .withFileSizeInBytes(10)
                      .withRecordCount(1)
                      .withPartitionPath(partitionPath(target.spec()))
                      .build())
              .commit();
          break;
        case POSITION_DELETE:
          List<DataFile> live = new ArrayList<>();
          try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
              target.newScan().planFiles()) {
            tasks.forEach(task -> live.add(task.file()));
          } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
          }
          if (live.isEmpty()) {
            throw new IllegalStateException("no live data file");
          }
          live.sort(java.util.Comparator.comparing(DataFile::location));
          DataFile data = live.get(cmd.b % live.size());
          org.apache.iceberg.FileMetadata.Builder delete =
              org.apache.iceberg.FileMetadata.deleteFileBuilder(target.specs().get(data.specId()))
                  .ofPositionDeletes()
                  .withFileSizeInBytes(10)
                  .withRecordCount(1)
                  .withPartition(data.partition());
          if (((HasTableOperations) target).operations().current().formatVersion() >= 3) {
            delete.withFormat(org.apache.iceberg.FileFormat.PUFFIN)
                .withPath("mem:///data/dv" + nextFile++ + ".puffin")
                .withReferencedDataFile(data.location())
                .withContentOffset(4)
                .withContentSizeInBytes(6);
          } else {
            delete.withPath("mem:///data/pos" + nextFile++ + ".parquet");
          }
          target.newRowDelta().addDeletes(delete.build()).commit();
          break;
        case ADD_COLUMN:
          target.updateSchema().addColumn("c" + nextValue++, Types.IntegerType.get()).commit();
          break;
        case DELETE_COLUMN:
          target.updateSchema().deleteColumn(addedColumn(target, cmd.b)).commit();
          break;
        case RENAME_COLUMN:
          target.updateSchema().renameColumn(addedColumn(target, cmd.b), "r" + nextValue++)
              .commit();
          break;
        case ADD_PARTITION_FIELD:
          switch (cmd.b % 3) {
            case 0:
              target.updateSpec().addField("cat").commit();
              break;
            case 1:
              target.updateSpec().addField(Expressions.bucket("id", 4)).commit();
              break;
            default:
              target.updateSpec().addField(Expressions.truncate("id", 10)).commit();
          }
          break;
        case REMOVE_PARTITION_FIELD:
          List<PartitionField> fields = target.spec().fields();
          if (fields.isEmpty()) {
            throw new IllegalStateException("no partition field");
          }
          target.updateSpec().removeField(fields.get(cmd.b % fields.size()).name()).commit();
          break;
        case SORT_ORDER:
          if (cmd.b % 3 == 0) {
            target.replaceSortOrder().asc("id").commit();
          } else if (cmd.b % 3 == 1) {
            target.replaceSortOrder().desc("cat").asc("id").commit();
          } else {
            target.replaceSortOrder().commit(); // unsorted
          }
          break;
        case SET_PROPERTY:
          target.updateProperties().set("k" + cmd.b % 3, "v" + nextValue++).commit();
          break;
        case REMOVE_PROPERTY:
          target.updateProperties().remove("k" + cmd.b % 3).commit();
          break;
        case CREATE_BRANCH:
          target.manageSnapshots().createBranch(ref(cmd.b), pick(snapshots, cmd.c).snapshotId())
              .commit();
          break;
        case CREATE_TAG:
          target.manageSnapshots().createTag("t" + cmd.b % 2, pick(snapshots, cmd.c).snapshotId())
              .commit();
          break;
        case REMOVE_REF:
          List<String> refs = new ArrayList<>(target.refs().keySet());
          refs.remove("main");
          if (refs.isEmpty()) {
            throw new IllegalStateException("no ref to remove");
          }
          refs.sort(String::compareTo);
          String removed = refs.get(cmd.b % refs.size());
          if (target.refs().get(removed).isBranch()) {
            target.manageSnapshots().removeBranch(removed).commit();
          } else {
            target.manageSnapshots().removeTag(removed).commit();
          }
          break;
        case FAST_FORWARD:
          if (cmd.c % 2 == 0) {
            target.manageSnapshots().fastForwardBranch("main", ref(cmd.b)).commit();
          } else {
            target.manageSnapshots().fastForwardBranch(ref(cmd.b), "main").commit();
          }
          break;
        case ROLLBACK:
          target.manageSnapshots().rollbackTo(pick(snapshots, cmd.b).snapshotId()).commit();
          break;
        case CHERRY_PICK:
          // A snapshot off main's history: a staged append or a branch's.
          java.util.Set<Long> mainHistory = new java.util.HashSet<>();
          if (target.currentSnapshot() != null) {
            for (Snapshot s : org.apache.iceberg.util.SnapshotUtil.ancestorsOf(
                target.currentSnapshot().snapshotId(), target::snapshot)) {
              mainHistory.add(s.snapshotId());
            }
          }
          List<Snapshot> off = new ArrayList<>();
          snapshots.forEach(s -> {
            if (!mainHistory.contains(s.snapshotId())) {
              off.add(s);
            }
          });
          target.manageSnapshots().cherrypick(pick(off, cmd.b).snapshotId()).commit();
          break;
        case EXPIRE_SNAPSHOTS:
          target.expireSnapshots()
              .expireOlderThan(System.currentTimeMillis() + 1)
              .retainLast(cmd.b % 3 + 1)
              .cleanExpiredFiles(false)
              .cleanExpiredMetadata(cmd.c % 2 == 0)
              .commit();
          break;
        case SET_LOCATION:
          target.updateLocation().setLocation("mem:///loc" + nextValue++).commit();
          break;
        case UPGRADE:
          target.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();
          break;
        default:
          throw new IllegalStateException("Unknown kind " + cmd.kind);
      }
    }

    /** A replace transaction to one of three schemas and specs, with an append if b is odd. */
    private void replace(Cmd cmd) {
      Schema schema =
          cmd.a % 2 == 0
              ? SCHEMA
              : new Schema(
                  required(1, "id", Types.IntegerType.get()),
                  optional(2, "data", Types.StringType.get()),
                  optional(3, "cat", Types.IntegerType.get()),
                  optional(4, "extra", Types.LongType.get()));
      PartitionSpec spec =
          cmd.c % 3 == 0
              ? PartitionSpec.unpartitioned()
              : cmd.c % 3 == 1
                  ? PartitionSpec.builderFor(schema).identity("cat").build()
                  : PartitionSpec.builderFor(schema).bucket("id", 4).build();
      Catalog.TableBuilder builder =
          catalog.buildTable(ID, schema).withPartitionSpec(spec)
              .withSortOrder(cmd.a % 3 == 0 ? SortOrder.unsorted()
                  : SortOrder.builderFor(schema).asc("id").build());
      Transaction replace = builder.replaceTransaction();
      if (cmd.b % 2 == 1) {
        replace.newFastAppend().appendFile(newFile(replace.table())).commit();
      }
      replace.commitTransaction();
    }

    private static String ref(int index) {
      return "b" + index % 2;
    }

    private static String addedColumn(Table target, int index) {
      List<String> added = new ArrayList<>();
      target.schema().columns().forEach(c -> {
        if (c.fieldId() > 3) {
          added.add(c.name());
        }
      });
      if (added.isEmpty()) {
        throw new IllegalStateException("no added column");
      }
      return added.get(index % added.size());
    }

    private static Snapshot pick(List<Snapshot> snapshots, int index) {
      if (snapshots.isEmpty()) {
        throw new IllegalStateException("no snapshot");
      }
      return snapshots.get(index % snapshots.size());
    }

    /** A one-row file in the table's current spec, in partition 1 / bucket 0 / truncation 0. */
    private DataFile newFile(Table target) {
      return DataFiles.builder(target.spec())
          .withPath("mem:///data/f" + nextFile++ + ".parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .withPartitionPath(partitionPath(target.spec()))
          .build();
    }

    /** The partition of {@link #newFile}'s files: 1 for identity, 0 for bucket and truncate. */
    private static String partitionPath(PartitionSpec spec) {
      List<String> path = new ArrayList<>();
      for (PartitionField field : spec.fields()) {
        String transform = field.transform().toString();
        String value = transform.equals("identity") ? "1" : transform.equals("void") ? "null" : "0";
        path.add(field.name() + "=" + value);
      }
      return String.join("/", path);
    }
  }

  /** The metadata as JSON, less the metadata log, which inline storage synthesizes. */
  private static JsonNode canonical(TableMetadata metadata) {
    ObjectNode node = (ObjectNode) json(metadata);
    node.remove("metadata-log");
    return node;
  }

  private static JsonNode json(TableMetadata metadata) {
    try {
      return JSON.readTree(TableMetadataParser.toJson(metadata));
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }

  /** The first field where {@code replayed} differs from {@code expected}, or null. */
  private static String difference(JsonNode expected, JsonNode replayed) {
    return difference("", expected, replayed);
  }

  private static String difference(String path, JsonNode expected, JsonNode replayed) {
    if (expected.equals(replayed)) {
      return null;
    }
    if (expected.isObject() && replayed.isObject()) {
      java.util.Set<String> names = new java.util.TreeSet<>();
      expected.fieldNames().forEachRemaining(names::add);
      replayed.fieldNames().forEachRemaining(names::add);
      for (String name : names) {
        JsonNode e = expected.get(name);
        JsonNode r = replayed.get(name);
        if (e == null || r == null) {
          return path + "/" + name + ": " + e + " -> " + r;
        }
        String d = difference(path + "/" + name, e, r);
        if (d != null) {
          return d;
        }
      }
    }
    if (expected.isArray() && replayed.isArray() && expected.size() == replayed.size()) {
      for (int i = 0; i < expected.size(); i++) {
        String d = difference(path + "[" + i + "]", expected.get(i), replayed.get(i));
        if (d != null) {
          return d;
        }
      }
    }
    return path + ": " + abbreviate(expected) + " -> " + abbreviate(replayed);
  }

  private static String abbreviate(JsonNode node) {
    String s = node.toString();
    return s.length() <= 300 ? s : s.substring(0, 300) + "...";
  }

  /** A location for builders that never read a file. */
  private static final class CatalogStateGeneratorsStub {
    static final InputFile LOCATION =
        new InputFile() {
          @Override
          public long getLength() {
            return 0;
          }

          @Override
          public SeekableInputStream newStream() {
            throw new UnsupportedOperationException();
          }

          @Override
          public String location() {
            return "test://delta-round-trip";
          }

          @Override
          public boolean exists() {
            return false;
          }
        };
  }
}
