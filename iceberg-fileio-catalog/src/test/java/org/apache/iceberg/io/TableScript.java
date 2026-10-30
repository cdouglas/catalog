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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.RandomDistribution;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;

/**
 * Generated schedules of Iceberg operations on one table, for the properties that check how an
 * inline catalog stores what they commit ({@link TestInlineDeltaProperties}, {@link
 * TestInlineDifferentialProperties}): appends (fast, merge, staged, to a branch), deletes,
 * equality and position deletes (deletion vectors in v3), schema, partition-spec and sort-order
 * evolution, properties (some too big to keep inline), branches and tags, rollbacks, cherry-picks,
 * snapshot expiry, location changes, upgrades to v3 and replace transactions, alone or several in
 * a table transaction. Some validate against concurrent commits as engines configure them: an
 * overwrite of a partition and a merge-on-read row delta as Spark's copy-on-write and
 * merge-on-read writes do, an insert-only row delta that validates the data added under its
 * filter, a compaction that keeps its starting sequence number, a validated dynamic overwrite, and
 * an append with an exactly-once sink's ancestry check. New files go to one of four partitions,
 * with bounds for {@code cat}. A {@link Runner} runs them on a table of a catalog; two runners
 * given one schedule make the same calls with the same file names and values.
 */
final class TableScript {
  static final TableIdentifier ID = TableIdentifier.of("ns", "t");
  /** A second table, which the odd members of a catalog transaction change. */
  static final TableIdentifier OTHER = TableIdentifier.of("ns", "u");
  static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          optional(2, "data", Types.StringType.get()),
          optional(3, "cat", Types.IntegerType.get()),
          optional(4, "ts", Types.TimestampType.withZone()));

  private TableScript() {}

  /** Schedules of 1 to 30 commands. */
  static Arbitrary<List<Cmd>> schedules() {
    Arbitrary<Integer> small = Arbitraries.integers().between(0, 11);
    return Combinators.combine(Arbitraries.of(Kind.values()), small, small, small)
        .as(Cmd::new)
        .list()
        .ofMinSize(1)
        .ofMaxSize(30)
        .withSizeDistribution(RandomDistribution.uniform());
  }

  /**
   * The kinds of change conflict encodings tell apart from a table's other changes: property,
   * statistics, sort-order and location changes, commits and refs on other branches, staged
   * snapshots, appends, and a branch's retention.
   */
  static final java.util.Set<Kind> SEPARABLE =
      java.util.EnumSet.of(Kind.SET_PROPERTY, Kind.REMOVE_PROPERTY, Kind.STATISTICS,
          Kind.PARTITION_STATISTICS, Kind.SORT_ORDER, Kind.SET_LOCATION, Kind.BRANCH_APPEND,
          Kind.STAGED_APPEND, Kind.CREATE_BRANCH, Kind.CREATE_TAG, Kind.FAST_APPEND,
          Kind.RETENTION);

  /**
   * Single commands of the kinds {@code kinds} admits, half of them of the {@link #SEPARABLE}
   * kinds: windows a commit races, which conflict encodings may let it land after.
   */
  static Arbitrary<Cmd> windowCommands(java.util.function.Predicate<Kind> kinds) {
    return Arbitraries.oneOf(commands(kinds), commands(k -> kinds.test(k) && SEPARABLE.contains(k)));
  }

  /** Single commands of the kinds {@code kinds} admits. */
  static Arbitrary<Cmd> commands(java.util.function.Predicate<Kind> kinds) {
    Arbitrary<Integer> small = Arbitraries.integers().between(0, 11);
    return Combinators.combine(Arbitraries.of(Kind.values()).filter(kinds::test), small, small,
            small)
        .as(Cmd::new);
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
    /** Widens an added int column to long, or makes one optional. */
    UPDATE_COLUMN,
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
    /** Sets a property to random characters of about 3 KiB. */
    BIG_PROPERTY,
    /** Replaces a live data file with a new one, as compaction does. */
    REWRITE_FILES,
    /** Overwrites every row with a new file. */
    OVERWRITE,
    /** Replaces the partitions of a new file (a dynamic overwrite). */
    REPLACE_PARTITIONS,
    /** Sets a snapshot's statistics file, or removes one when {@code c} is odd. */
    STATISTICS,
    /** Sets a snapshot's partition statistics file, or removes one when {@code c} is odd. */
    PARTITION_STATISTICS,
    /**
     * Overwrites partition {@code c % 4} (rows with that {@code cat}) with a new file, validating
     * from the current snapshot as Spark's copy-on-write overwrite does: no conflicting deletes,
     * and under serializable isolation ({@code b} even) no conflicting data.
     */
    OVERWRITE_PARTITION,
    /**
     * A merge-on-read update of a live data file: a position delete (a deletion vector in v3) for
     * it and a new file in its partition, validated as Spark's merge-on-read write does: the file
     * still exists and isn't deleted, no conflicting delete files, and under serializable isolation
     * ({@code c} even) no conflicting data under the file's partition.
     */
    MERGE_ROW_DELTA,
    /**
     * An insert-only row delta into partition {@code c % 4} that validates no data was added under
     * its filter since the current snapshot: a serializable MERGE whose matches were all inserts.
     * Its snapshot's operation is append.
     */
    INSERT_ROW_DELTA,
    /** Deletes the rows of partition {@code c % 4} by a row filter, which a retry re-evaluates. */
    DELETE_PARTITION,
    /**
     * Appends a file as checkpoint {@code b % 3} of an exactly-once sink, which validates that no
     * ancestor committed that checkpoint.
     */
    CHECKPOINT_APPEND,
    /**
     * Overwrites partition {@code c % 4} as a job that read the table before its latest commit
     * does: validating from the current snapshot's parent that no conflicting deletes (and, {@code
     * b} even, data) were added since. A retry after that parent expired cannot tell the history
     * and fails.
     */
    LONG_OVERWRITE,
    /** Sets the retention of main ({@code b} even) or a branch: snapshots to keep, max age. */
    RETENTION,
    REPLACE,
    /** Runs the next {@code a % 3 + 2} commands in one table transaction. */
    TRANSACTION,
    /**
     * Runs the next {@code a % 3 + 2} commands in a catalog transaction, the odd ones on {@link
     * #OTHER}, serializable if {@code b} is even, else snapshot isolation; in a table transaction
     * in a catalog without them.
     */
    CATALOG_TXN,
    /**
     * Runs the next command on a handle loaded at the previous STALE step (or at creation), from
     * the runner's other catalog, whose commit retries on the current metadata or fails.
     */
    STALE
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


  /**
   * The steps of {@code schedule}: each command alone, but a TRANSACTION or CATALOG_TXN command
   * with the next {@code a % 3 + 2} commands, which run in one transaction, and a STALE command
   * with the next.
   */
  static List<List<Cmd>> steps(List<Cmd> schedule) {
    List<List<Cmd>> steps = new ArrayList<>();
    for (int i = 0; i < schedule.size(); i++) {
      Cmd cmd = schedule.get(i);
      if (cmd.kind == Kind.TRANSACTION || cmd.kind == Kind.CATALOG_TXN
          || cmd.kind == Kind.STALE) {
        int n = Math.min(
            cmd.kind == Kind.STALE ? 1 : cmd.a % 3 + 2, schedule.size() - i - 1);
        steps.add(new ArrayList<>(schedule.subList(i, i + 1 + n)));
        i += n;
      } else {
        steps.add(List.of(cmd));
      }
    }
    return steps;
  }

  /** Runs commands on table {@link #ID} of a catalog. */
  static final class Runner {
    private final Catalog catalog;
    private final Catalog other;
    final StringBuilder trace;
    Table table;
    private Table stale;
    /** Where the files this runner adds go: the start of their paths. */
    private final String names;
    private int nextFile = 0;
    private int nextValue = 0;
    // A transaction's members leave the metadata log's bound alone (ICEBERG_ISSUES.md A6)
    private boolean inTransaction = false;

    /**
     * Creates the tables, in {@code formatVersion}, partitioned by cat if {@code partitioned}.
     */
    Runner(Catalog catalog, int formatVersion, boolean partitioned, StringBuilder trace) {
      this(catalog, catalog, formatVersion, partitioned, trace);
    }

    /** Like the other, with STALE commands loading handles from {@code other}. */
    Runner(Catalog catalog, Catalog other, int formatVersion, boolean partitioned,
        StringBuilder trace) {
      this.catalog = catalog;
      this.other = other;
      this.trace = trace;
      this.names = "mem:///data/";
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
      catalog
          .buildTable(OTHER, SCHEMA)
          .withPartitionSpec(spec)
          .withProperty(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
          .create();
      this.stale = other.loadTable(ID);
    }

    /**
     * Runs commands on the tables another runner created, through {@code catalog}, with the files it
     * adds under {@code mem:///data/<names>/}, apart from that runner's.
     */
    Runner(Catalog catalog, String names, StringBuilder trace) {
      this.catalog = catalog;
      this.other = catalog;
      this.trace = trace;
      this.names = "mem:///data/" + names + "/";
      this.table = catalog.loadTable(ID);
      this.stale = table;
    }

    /** Runs {@code step}; returns the exception that rejected it, or null if it committed. */
    RuntimeException run(List<Cmd> step) {
      Cmd first = step.get(0);
      if (first.kind == Kind.STALE) {
        trace.append("stale ").append(step.subList(1, step.size())).append('\n');
        Table handle = stale;
        this.stale = other.loadTable(ID);
        try {
          Cmd member = step.size() > 1 ? step.get(1) : null;
          if (member == null || member.kind == Kind.REPLACE) {
            throw new IllegalStateException("no command for the stale handle");
          }
          run(member, handle);
        } catch (RuntimeException e) {
          trace.append("  skipped: ").append(e).append('\n');
          return e;
        }
        return null;
      }
      if (first.kind == Kind.TRANSACTION || first.kind == Kind.CATALOG_TXN) {
        List<Cmd> group = step.subList(1, step.size());
        org.apache.iceberg.catalog.CatalogTransaction catalogTxn =
            first.kind == Kind.CATALOG_TXN
                && catalog instanceof org.apache.iceberg.catalog.SupportsCatalogTransactions
                ? ((org.apache.iceberg.catalog.SupportsCatalogTransactions) catalog)
                    .createTransaction(first.b % 2 == 0
                        ? org.apache.iceberg.catalog.CatalogTransaction.IsolationLevel.SERIALIZABLE
                        : org.apache.iceberg.catalog.CatalogTransaction.IsolationLevel.SNAPSHOT)
                : null;
        trace.append(catalogTxn != null ? "catalog transaction (" + catalogTxn.isolationLevel()
            + ") " : "transaction ").append(group).append('\n');
        try {
          Transaction txn = catalogTxn == null ? table.newTransaction() : null;
          Table target = txn != null ? txn.table() : catalogTxn.asCatalog().loadTable(ID);
          Table second = txn != null ? target : catalogTxn.asCatalog().loadTable(OTHER);
          int staged = 0;
          inTransaction = true;
          try {
            for (int i = 0; i < group.size(); i++) {
              Cmd member = group.get(i);
              if (member.kind == Kind.TRANSACTION || member.kind == Kind.CATALOG_TXN
                  || member.kind == Kind.REPLACE || member.kind == Kind.STALE) {
                continue;
              }
              try {
                run(member, i % 2 == 1 ? second : target);
                staged += 1;
              } catch (RuntimeException e) {
                trace.append("  member skipped: ").append(e).append('\n');
              }
            }
          } finally {
            inTransaction = false;
          }
          if (staged == 0) {
            throw new IllegalStateException("no member staged");
          }
          if (txn != null) {
            txn.commitTransaction();
          } else {
            catalogTxn.commitTransaction();
          }
        } catch (RuntimeException e) {
          trace.append("  skipped: ").append(e).append('\n');
          return e;
        }
      } else {
        trace.append(first).append('\n');
        try {
          if (first.kind == Kind.REPLACE) {
            replace(first);
          } else {
            run(first, table);
          }
        } catch (RuntimeException e) {
          trace.append("  skipped: ").append(e).append('\n');
          return e;
        }
      }
      return null;
    }

    private void run(Cmd cmd, Table target) {
      List<Snapshot> snapshots = new ArrayList<>();
      target.snapshots().forEach(snapshots::add);
      switch (cmd.kind) {
        case FAST_APPEND:
          target.newFastAppend().appendFile(newFile(target, cmd.c)).commit();
          break;
        case MERGE_APPEND:
          target.newAppend().appendFile(newFile(target, cmd.c)).commit();
          break;
        case STAGED_APPEND:
          target.newFastAppend().appendFile(newFile(target, cmd.c)).stageOnly().commit();
          break;
        case BRANCH_APPEND:
          target.newFastAppend().appendFile(newFile(target, cmd.c)).toBranch(ref(cmd.b))
              .commit();
          break;
        case DELETE_FILE:
          target.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();
          break;
        case EQUALITY_DELETE:
          target.newRowDelta().addDeletes(
                  org.apache.iceberg.FileMetadata.deleteFileBuilder(target.spec())
                      .ofEqualityDeletes(1)
                      .withPath(names + "eq" + nextFile++ + ".parquet")
                      .withFileSizeInBytes(10)
                      .withRecordCount(1)
                      .withPartitionPath(partitionPath(target.spec(), cmd.c))
                      .build())
              .commit();
          break;
        case REWRITE_FILES:
          List<DataFile> rewritten = liveFiles(target);
          if (rewritten.isEmpty()) {
            throw new IllegalStateException("no live data file");
          }
          DataFile compacted = rewritten.get(cmd.b % rewritten.size());
          org.apache.iceberg.RewriteFiles rewrite =
              target.newRewrite().validateFromSnapshot(target.currentSnapshot().snapshotId());
          if (cmd.c % 2 == 1) {
            // a compaction that keeps its starting sequence number, as Spark's does by default
            rewrite.dataSequenceNumber(target.currentSnapshot().sequenceNumber());
          }
          rewrite
              .deleteFile(compacted)
              .addFile(DataFiles.builder(target.specs().get(compacted.specId()))
                  .withPath(names + "r" + nextFile++ + ".parquet")
                  .withFileSizeInBytes(10)
                  .withRecordCount(compacted.recordCount())
                  .withPartition(compacted.partition())
                  .withMetrics(catMetrics(compacted.recordCount(), cat(compacted)))
                  .build())
              .commit();
          break;
        case OVERWRITE:
          target.newOverwrite()
              .overwriteByRowFilter(Expressions.alwaysTrue())
              .addFile(newFile(target, cmd.c))
              .commit();
          break;
        case REPLACE_PARTITIONS:
          org.apache.iceberg.ReplacePartitions replace =
              target.newReplacePartitions().addFile(newFile(target, cmd.c));
          if (cmd.b % 2 == 1 && target.currentSnapshot() != null) {
            replace.validateFromSnapshot(target.currentSnapshot().snapshotId())
                .validateNoConflictingData()
                .validateNoConflictingDeletes();
          }
          replace.commit();
          break;
        case OVERWRITE_PARTITION:
          org.apache.iceberg.expressions.Expression partition = inPartition(cmd.c);
          org.apache.iceberg.OverwriteFiles overwrite =
              target.newOverwrite().overwriteByRowFilter(partition).addFile(newFile(target, cmd.c));
          if (target.currentSnapshot() != null) {
            overwrite.validateFromSnapshot(target.currentSnapshot().snapshotId());
          }
          overwrite.conflictDetectionFilter(partition).validateNoConflictingDeletes();
          if (cmd.b % 2 == 0) {
            overwrite.validateNoConflictingData();
          }
          overwrite.commit();
          break;
        case RETENTION:
          String retained = cmd.b % 2 == 0 ? org.apache.iceberg.SnapshotRef.MAIN_BRANCH : ref(cmd.b);
          target.manageSnapshots()
              .setMinSnapshotsToKeep(retained, cmd.c % 3 + 1)
              .setMaxSnapshotAgeMs(retained, (cmd.c + 1) * 3_600_000L)
              .commit();
          break;
        case LONG_OVERWRITE:
          org.apache.iceberg.expressions.Expression overwritten = inPartition(cmd.c);
          org.apache.iceberg.OverwriteFiles job =
              target.newOverwrite().overwriteByRowFilter(overwritten)
                  .addFile(newFile(target, cmd.c));
          Snapshot read = target.currentSnapshot();
          if (read != null) {
            job.validateFromSnapshot(
                read.parentId() != null ? read.parentId() : read.snapshotId());
          }
          job.conflictDetectionFilter(overwritten).validateNoConflictingDeletes();
          if (cmd.b % 2 == 0) {
            job.validateNoConflictingData();
          }
          job.commit();
          break;
        case MERGE_ROW_DELTA:
          org.apache.iceberg.FileScanTask updatedTask = pickLive(target, cmd.b);
          DataFile updated = updatedTask.file();
          org.apache.iceberg.expressions.Expression updatedPartition =
              Expressions.equal("cat", cat(updated));
          org.apache.iceberg.RowDelta merge = target.newRowDelta();
          // a writer merges the file's deletion vector into the new one, which replaces it
          for (org.apache.iceberg.DeleteFile previous : updatedTask.deletes()) {
            if (previous.format() == org.apache.iceberg.FileFormat.PUFFIN) {
              merge.removeDeletes(previous);
            }
          }
          merge
              .addDeletes(positionDelete(target, updated))
              .addRows(newFile(target, cat(updated) - 1))
              .validateFromSnapshot(target.currentSnapshot().snapshotId())
              .conflictDetectionFilter(updatedPartition)
              .validateDataFilesExist(List.of(updated.location()))
              .validateDeletedFiles()
              .validateNoConflictingDeleteFiles();
          if (cmd.c % 2 == 0) {
            merge.validateNoConflictingDataFiles();
          }
          merge.commit();
          break;
        case INSERT_ROW_DELTA:
          org.apache.iceberg.RowDelta insert =
              target.newRowDelta().addRows(newFile(target, cmd.c));
          if (target.currentSnapshot() != null) {
            insert.validateFromSnapshot(target.currentSnapshot().snapshotId());
          }
          insert.conflictDetectionFilter(inPartition(cmd.c)).validateNoConflictingDataFiles()
              .commit();
          break;
        case DELETE_PARTITION:
          target.newDelete().deleteFromRowFilter(inPartition(cmd.c)).commit();
          break;
        case CHECKPOINT_APPEND:
          String checkpoint = "cp" + cmd.b % 3;
          target.newFastAppend()
              .appendFile(newFile(target, cmd.c))
              .set("checkpoint", checkpoint)
              .validateWith(new org.apache.iceberg.SnapshotAncestryValidator() {
                @Override
                public boolean validate(Iterable<Snapshot> ancestors) {
                  for (Snapshot ancestor : ancestors) {
                    if (checkpoint.equals(ancestor.summary().get("checkpoint"))) {
                      return false;
                    }
                  }
                  return true;
                }

                @Override
                public String errorMessage() {
                  return "checkpoint " + checkpoint + " is already committed";
                }
              })
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
          target.newRowDelta().addDeletes(positionDelete(target, data)).commit();
          break;
        case ADD_COLUMN:
          target.updateSchema().addColumn("c" + nextValue++, Types.IntegerType.get()).commit();
          break;
        case DELETE_COLUMN:
          target.updateSchema().deleteColumn(addedColumn(target, cmd.b)).commit();
          break;
        case UPDATE_COLUMN:
          if (cmd.c % 2 == 0) {
            target.updateSchema().updateColumn(addedColumn(target, cmd.b), Types.LongType.get())
                .commit();
          } else {
            target.updateSchema().makeColumnOptional(addedColumn(target, cmd.b)).commit();
          }
          break;
        case RENAME_COLUMN:
          target.updateSchema().renameColumn(addedColumn(target, cmd.b), "r" + nextValue++)
              .commit();
          break;
        case ADD_PARTITION_FIELD:
          switch (cmd.b % 6) {
            case 0:
              target.updateSpec().addField("cat").commit();
              break;
            case 1:
              target.updateSpec().addField(Expressions.bucket("id", 4)).commit();
              break;
            case 2:
              target.updateSpec().addField(Expressions.truncate("id", 10)).commit();
              break;
            case 3:
              target.updateSpec().addField(Expressions.day("ts")).commit();
              break;
            case 4:
              target.updateSpec().addField(Expressions.hour("ts")).commit();
              break;
            default:
              target.updateSpec().addField(Expressions.truncate("data", 3)).commit();
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
          org.apache.iceberg.NullOrder nulls =
              cmd.c % 2 == 0 ? org.apache.iceberg.NullOrder.NULLS_FIRST
                  : org.apache.iceberg.NullOrder.NULLS_LAST;
          switch (cmd.b % 5) {
            case 0:
              target.replaceSortOrder().asc("id", nulls).commit();
              break;
            case 1:
              target.replaceSortOrder().desc("cat", nulls).asc("id").commit();
              break;
            case 2:
              target.replaceSortOrder().asc(Expressions.bucket("id", 8), nulls).commit();
              break;
            case 3:
              target.replaceSortOrder().desc(Expressions.truncate("data", 4), nulls)
                  .asc(Expressions.day("ts")).commit();
              break;
            default:
              target.replaceSortOrder().commit(); // unsorted
          }
          break;
        case SET_PROPERTY:
          if (cmd.b % 6 == 5 && !inTransaction) { // the metadata log's bound (replay's Window)
            target.updateProperties()
                .set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, String.valueOf(cmd.c % 4))
                .commit();
          } else {
            target.updateProperties().set("k" + cmd.b % 3, "v" + nextValue++).commit();
          }
          break;
        case REMOVE_PROPERTY:
          boolean bound = cmd.b % 6 == 5 && !inTransaction;
          target.updateProperties()
              .remove(bound ? TableProperties.METADATA_PREVIOUS_VERSIONS_MAX
                  : cmd.b % 4 == 3 ? "big" : "k" + cmd.b % 3)
              .commit();
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
          // Every snapshot is old enough: the wall clock is not consulted, since after a step back
          // "now" would precede the snapshots stamped just before the step (ClockSteps), and two
          // runs of one schedule would expire different snapshots.
          target.expireSnapshots()
              .expireOlderThan(Long.MAX_VALUE)
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
        case BIG_PROPERTY:
          java.util.Random random = new java.util.Random(nextValue++);
          StringBuilder big = new StringBuilder();
          for (int i = 0; i < 3000; i++) {
            big.append((char) ('!' + random.nextInt(90)));
          }
          target.updateProperties().set("big", big.toString()).commit();
          break;
        case STATISTICS:
          Snapshot measured = pick(snapshots, cmd.b);
          if (cmd.c % 2 == 1) {
            target.updateStatistics().removeStatistics(measured.snapshotId()).commit();
          } else {
            target.updateStatistics()
                .setStatistics(new org.apache.iceberg.GenericStatisticsFile(
                    measured.snapshotId(), names + "stats-s" + nextFile++ + ".puffin", 100, 10,
                    List.of(new org.apache.iceberg.GenericBlobMetadata(
                        "apache-datasketches-theta-v1", measured.snapshotId(),
                        measured.sequenceNumber(), List.of(1), Map.of("ndv", "3")))))
                .commit();
          }
          break;
        case PARTITION_STATISTICS:
          Snapshot partitioned = pick(snapshots, cmd.b);
          if (cmd.c % 2 == 1) {
            target.updatePartitionStatistics()
                .removePartitionStatistics(partitioned.snapshotId()).commit();
          } else {
            target.updatePartitionStatistics()
                .setPartitionStatistics(
                    org.apache.iceberg.ImmutableGenericPartitionStatisticsFile.builder()
                        .snapshotId(partitioned.snapshotId())
                        .path(names + "stats-p" + nextFile++ + ".parquet")
                        .fileSizeInBytes(42)
                        .build())
                .commit();
          }
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
        replace.newFastAppend().appendFile(newFile(replace.table(), cmd.c)).commit();
      }
      replace.commitTransaction();
    }

    /** The live data files of the main branch, by path, with their column bounds ({@link #cat}). */
    private static List<DataFile> liveFiles(Table target) {
      List<DataFile> live = new ArrayList<>();
      try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
          target.newScan().includeColumnStats().planFiles()) {
        tasks.forEach(task -> live.add(task.file()));
      } catch (java.io.IOException e) {
        throw new java.io.UncheckedIOException(e);
      }
      live.sort(java.util.Comparator.comparing(DataFile::location));
      return live;
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

    /**
     * A one-row file in the table's current spec, in partition {@code index % 4}: its {@code cat} is
     * {@code index % 4 + 1}, with bounds that say so, and each partition field has the value of
     * that row ({@link #partitionPath}).
     */
    private DataFile newFile(Table target, int index) {
      int k = Math.floorMod(index, 4);
      return DataFiles.builder(target.spec())
          .withPath(names + "f" + nextFile++ + ".parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .withPartitionPath(partitionPath(target.spec(), k))
          .withMetrics(catMetrics(1, k + 1))
          .build();
    }

    /** Metrics of a file of {@code records} rows whose {@code cat} is {@code cat}. */
    private static org.apache.iceberg.Metrics catMetrics(long records, int cat) {
      java.nio.ByteBuffer bound = org.apache.iceberg.types.Conversions.toByteBuffer(
          Types.IntegerType.get(), cat);
      return new org.apache.iceberg.Metrics(
          records, null, Map.of(3, records), Map.of(3, 0L), null, Map.of(3, bound),
          Map.of(3, bound));
    }

    /**
     * The {@code cat} of a file {@link #newFile} wrote: its lower bound, else 1. A scan's files
     * have their bounds only if it includes column stats.
     */
    private static int cat(DataFile file) {
      java.nio.ByteBuffer bound = file.lowerBounds() == null ? null : file.lowerBounds().get(3);
      return bound == null ? 1
          : org.apache.iceberg.types.Conversions.<Integer>fromByteBuffer(
              Types.IntegerType.get(), bound);
    }

    /** The rows of partition {@code index % 4}: those whose {@code cat} is {@code index % 4 + 1}. */
    private static org.apache.iceberg.expressions.Expression inPartition(int index) {
      return Expressions.equal("cat", Math.floorMod(index, 4) + 1);
    }

    /**
     * The scan task of a live data file of the main branch, the {@code index}-th by path, its file
     * with its column bounds ({@link #cat}).
     */
    private static org.apache.iceberg.FileScanTask pickLive(Table target, int index) {
      List<org.apache.iceberg.FileScanTask> live = new ArrayList<>();
      try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
          target.newScan().includeColumnStats().planFiles()) {
        tasks.forEach(live::add);
      } catch (java.io.IOException e) {
        throw new java.io.UncheckedIOException(e);
      }
      if (live.isEmpty()) {
        throw new IllegalStateException("no live data file");
      }
      live.sort(java.util.Comparator.comparing(t -> t.file().location()));
      return live.get(Math.floorMod(index, live.size()));
    }

    /** A position delete of one row of {@code data}: in v3, a deletion vector. */
    private org.apache.iceberg.DeleteFile positionDelete(Table target, DataFile data) {
      org.apache.iceberg.FileMetadata.Builder delete =
          org.apache.iceberg.FileMetadata.deleteFileBuilder(target.specs().get(data.specId()))
              .ofPositionDeletes()
              .withFileSizeInBytes(10)
              .withRecordCount(1)
              .withPartition(data.partition());
      if (((HasTableOperations) target).operations().current().formatVersion() >= 3) {
        delete.withFormat(org.apache.iceberg.FileFormat.PUFFIN)
            .withPath(names + "dv" + nextFile++ + ".puffin")
            .withReferencedDataFile(data.location())
            .withContentOffset(4)
            .withContentSizeInBytes(6);
      } else {
        delete.withPath(names + "pos" + nextFile++ + ".parquet");
      }
      return delete.build();
    }

    /**
     * The partition of a row whose {@code cat} is {@code k + 1}, as {@link #newFile} writes it: one
     * value for each transform, the k-th of four.
     */
    private static String partitionPath(PartitionSpec spec, int k) {
      List<String> path = new ArrayList<>();
      for (PartitionField field : spec.fields()) {
        String transform = field.transform().toString();
        String value;
        if (transform.equals("identity")) {
          value = String.valueOf(k + 1);
        } else if (transform.equals("void")) {
          value = "null";
        } else if (transform.equals("day")) {
          value = "2026-10-1" + (6 + k);
        } else if (transform.equals("hour")) {
          value = "2026-10-16-0" + (3 + k);
        } else if (transform.startsWith("truncate") && spec.schema().findType(field.sourceId())
            .typeId() == org.apache.iceberg.types.Type.TypeID.STRING) {
          value = "ab" + (char) ('c' + k);
        } else if (transform.startsWith("truncate")) {
          value = String.valueOf(10 * k);
        } else {
          value = String.valueOf(k); // a bucket of four or more
        }
        path.add(field.name() + "=" + value);
      }
      return String.join("/", path);
    }
  }
}
