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
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Iceberg's PARTITIONS metadata table aggregates a partition's files (as the table's unified
 * partition type sees them) and takes its {@code spec_id}, {@code last_updated_at} and {@code
 * last_updated_snapshot_id} from the file of the newest snapshot it reads, the first it reads on a
 * tie of commit times ({@code PartitionsTable.Partition.update}). It reads the manifests in
 * parallel on the scan's plan executor ({@code PartitionsTable.planEntries}: a {@code
 * ParallelIterable}), so where one snapshot adds files of one partition under two specs, the
 * columns follow which manifest a worker read first: two scans of one table can disagree. Round
 * p12-r0p's differential failure was this: PARTITIONS {@code spec_id} 0 against 1 for {@code
 * {cat=null}} (a data file and a position delete file of one snapshot, under two specs),
 * between the pointer and the inline catalog and between the inline table and its own serialized
 * copy, whose metadata is the same. {@link TableViews} plans metadata tables on a direct executor,
 * in the order of the snapshot's manifest list, which its physical view compares.
 */
class TestMetadataTableOrder {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), optional(2, "cat", Types.IntegerType.get()));

  @Test
  void aPartitionsSpecIdFollowsTheOrderItsManifestsAreRead() {
    Table table = tableWithOneSnapshotOverTwoSpecs();
    int manifests = table.currentSnapshot().allManifests(table.io()).size();
    assertThat(manifests).isEqualTo(2); // the data manifest and the delete manifest

    String inOrder = partitionSpecIds(table, MoreExecutors.newDirectExecutorService());
    String reversed = partitionSpecIds(table, new Reversing(manifests));
    assertThat(inOrder).isEqualTo("{cat=null} spec_id=0");
    assertThat(reversed).isEqualTo("{cat=null} spec_id=1");
    // In order, the same every time: what TableViews compares
    for (int i = 0; i < 20; i++) {
      assertThat(partitionSpecIds(table, MoreExecutors.newDirectExecutorService()))
          .isEqualTo(inOrder);
    }
  }

  @Test
  void tableViewsReadsPartitionsInManifestOrder() {
    Table table = tableWithOneSnapshotOverTwoSpecs();
    String first = TableViews.metadataTableRows(table, MetadataTableType.PARTITIONS);
    assertThat(first).isEqualTo("[partition={category=null} spec_id=0 record_count=1 file_count=1"
        + " total_data_file_size_in_bytes=10 position_delete_record_count=1"
        + " position_delete_file_count=1 equality_delete_record_count=0"
        + " equality_delete_file_count=0 last_updated_at=* last_updated_snapshot_id=#0]");
    for (int i = 0; i < 20; i++) {
      assertThat(TableViews.metadataTableRows(table, MetadataTableType.PARTITIONS))
          .isEqualTo(first);
    }
  }

  /**
   * A v2 table partitioned on {@code cat} (spec 0), then with the field renamed (spec 1: the same
   * field, so one partition in the unified type), with one snapshot: a row delta adding a data file
   * of spec 0 and a position delete file of spec 1, both in partition {@code cat=null}. (An
   * unpartitioned spec's files are a partition of their own: they and spec 1's {@code cat=null}
   * are two rows.)
   */
  private static Table tableWithOneSnapshotOverTwoSpecs() {
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    LogCatalog catalog =
        LocalCatalogMode.APPEND.catalog(io, "mem:///warehouse", "c", Map.of());
    catalog.createNamespace(Namespace.of("ns"));
    Table table =
        catalog
            .buildTable(TableIdentifier.of("ns", "t"), SCHEMA)
            .withPartitionSpec(PartitionSpec.builderFor(SCHEMA).identity("cat").build())
            .withProperty(TableProperties.FORMAT_VERSION, "2")
            .create();
    PartitionSpec first = table.spec();
    table.updateSpec().renameField("cat", "category").commit();
    PartitionSpec renamed = table.spec();
    assertThat(renamed.specId()).isEqualTo(1);
    org.apache.iceberg.PartitionData catNull =
        new org.apache.iceberg.PartitionData(first.partitionType());
    catNull.set(0, null);
    DataFile data =
        DataFiles.builder(first)
            .withPath("mem:///data/a.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withPartition(catNull)
            .build();
    DeleteFile delete =
        FileMetadata.deleteFileBuilder(renamed)
            .ofPositionDeletes()
            .withPath("mem:///data/d.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withPartition(catNull)
            .withReferencedDataFile(data.location())
            .build();
    table.newRowDelta().addRows(data).addDeletes(delete).commit();
    return table;
  }

  /** Each PARTITIONS row's partition and spec id, sorted, scanned on {@code executor}. */
  private static String partitionSpecIds(Table table, ExecutorService executor) {
    Table partitions =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.PARTITIONS);
    List<String> rows = new ArrayList<>();
    try (org.apache.iceberg.io.CloseableIterable<FileScanTask> tasks =
        partitions.newScan().planWith(executor).planFiles()) {
      for (FileScanTask task : tasks) {
        try (org.apache.iceberg.io.CloseableIterable<StructLike> data = task.asDataTask().rows()) {
          for (StructLike row : data) {
            StructLike partition = row.get(0, StructLike.class);
            rows.add("{cat=" + partition.get(0, Integer.class) + "} spec_id="
                + row.get(1, Integer.class));
          }
        }
      }
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
    Collections.sort(rows);
    return String.join(", ", rows);
  }

  /**
   * An executor that holds the first {@code n} tasks it is given and then runs them on one thread
   * in the reverse order: the manifests' reads, which {@code ParallelIterable} submits together,
   * last first.
   */
  private static final class Reversing extends AbstractExecutorService {
    private final int n;
    private final List<Runnable> held = new ArrayList<>();
    private volatile boolean shutdown = false;

    Reversing(int n) {
      this.n = n;
    }

    @Override
    public synchronized void execute(Runnable task) {
      held.add(task);
      if (held.size() == n) {
        List<Runnable> batch = new ArrayList<>(held);
        held.clear();
        Collections.reverse(batch);
        Thread runner = new Thread(() -> batch.forEach(Runnable::run), "reversing-executor");
        runner.setDaemon(true);
        runner.start();
      } else if (held.size() > n) {
        throw new IllegalStateException("more tasks than expected: " + held.size());
      }
    }

    @Override
    public void shutdown() {
      shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
      shutdown = true;
      return List.of();
    }

    @Override
    public boolean isShutdown() {
      return shutdown;
    }

    @Override
    public boolean isTerminated() {
      return shutdown;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return true;
    }
  }
}
