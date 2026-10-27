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
package org.apache.iceberg.io.contention;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

/**
 * The operations a writer runs, as engines configure them. Each takes the table at the start of
 * its job, waits out the job's runtime (the time it would spend reading and writing data, here
 * {@code runtime}), then commits. Data files are metadata only: their paths are new and their
 * contents never read.
 *
 * <ul>
 *   <li>{@code fast_append}, {@code merge_append}: a streaming or batch insert of one file per
 *       partition; Iceberg's retry repairs any conflict, and nothing validates.
 *   <li>{@code validated_overwrite}: a copy-on-write MERGE/UPDATE/DELETE of its partitions, as
 *       Spark's serializable overwrite configures it: from the snapshot the job read, no data or
 *       deletes may have been added in them (a real conflict ends it).
 *   <li>{@code compaction}: a rewrite of one partition's live files into one, from the snapshot it
 *       planned on and keeping its sequence number, as Spark's rewrite_data_files does; only
 *       deletes of the files it rewrites conflict.
 *   <li>{@code property}: a property set (table maintenance, tuning).
 *   <li>{@code wap}: a write-audit-publish insert, to the writer's own branch.
 * </ul>
 */
final class Operations {
  static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "cat", Types.IntegerType.get()));
  static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("cat").build();

  private static final AtomicLong FILES = new AtomicLong();

  private Operations() {}

  /** A new data file of {@code records} records in partition {@code partition}. */
  static DataFile newFile(Table table, int partition, long records) {
    return DataFiles.builder(table.spec())
        .withPath("mem:///data/f" + FILES.incrementAndGet() + ".parquet")
        .withFileSizeInBytes(1024 * records)
        .withRecordCount(records)
        .withPartitionPath("cat=" + partition)
        .build();
  }

  private static Expression inPartitions(List<Integer> partitions) {
    Expression filter = Expressions.alwaysFalse();
    for (int p : partitions) {
      filter = Expressions.or(filter, Expressions.equal("cat", p));
    }
    return filter;
  }

  /** What an operation did, if it did not commit: why. */
  enum Skip {
    NONE,
    /** Nothing to do (a compaction of a partition with one file). */
    NOTHING_TO_DO
  }

  static Skip fastAppend(
      Table table, List<Integer> partitions, int filesPerPartition, boolean merge,
      String branch, LongConsumer runtime) {
    runtime.accept(0);
    AppendFiles append = merge ? table.newAppend() : table.newFastAppend();
    for (int p : partitions) {
      for (int f = 0; f < filesPerPartition; f++) {
        append.appendFile(newFile(table, p, 100));
      }
    }
    if (branch != null) {
      append.toBranch(branch);
    }
    append.commit();
    return Skip.NONE;
  }

  static Skip validatedOverwrite(
      Table table, List<Integer> partitions, int filesPerPartition, LongConsumer runtime) {
    Snapshot read = table.currentSnapshot();
    runtime.accept(0);
    Expression filter = inPartitions(partitions);
    OverwriteFiles overwrite = table.newOverwrite().overwriteByRowFilter(filter);
    for (int p : partitions) {
      for (int f = 0; f < filesPerPartition; f++) {
        overwrite.addFile(newFile(table, p, 100));
      }
    }
    if (read != null) {
      overwrite.validateFromSnapshot(read.snapshotId());
    }
    overwrite
        .conflictDetectionFilter(filter)
        .validateNoConflictingData()
        .validateNoConflictingDeletes()
        .commit();
    return Skip.NONE;
  }

  static Skip compaction(Table table, int partition, LongConsumer runtime) {
    Snapshot read = table.currentSnapshot();
    if (read == null) {
      return Skip.NOTHING_TO_DO;
    }
    List<DataFile> files = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks =
        table.newScan().useSnapshot(read.snapshotId())
            .filter(Expressions.equal("cat", partition)).planFiles()) {
      tasks.forEach(task -> files.add(task.file()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    if (files.size() < 2) {
      return Skip.NOTHING_TO_DO;
    }
    runtime.accept(0);
    long records = 0;
    RewriteFiles rewrite =
        table.newRewrite()
            .validateFromSnapshot(read.snapshotId())
            .dataSequenceNumber(read.sequenceNumber());
    for (DataFile file : files) {
      rewrite.deleteFile(file);
      records += file.recordCount();
    }
    rewrite.addFile(newFile(table, partition, records)).commit();
    return Skip.NONE;
  }

  static Skip property(Table table, int key, LongConsumer runtime) {
    runtime.accept(0);
    table.updateProperties().set("bench.key" + key, String.valueOf(FILES.incrementAndGet()))
        .commit();
    return Skip.NONE;
  }
}
