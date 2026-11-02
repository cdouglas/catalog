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

import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * A checkpoint writes the inline tables that have not changed as the bytes they had: from the
 * checkpoint a replica was decoded from, or the last one its catalog encoded (CheckpointMemo,
 * docs/CHECKPOINT_PLAN.md R0.2). The suites run with {@code fileio.catalog.replay.check}, under
 * which every table written from the memo is checked against its fresh encoding.
 */
class TestCheckpointMemo {

  private static final String WAREHOUSE = "mem:///warehouse";
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
  private static final int TABLES = 6;

  private static TableIdentifier table(int t) {
    return TableIdentifier.of("db", "t" + t);
  }

  private static DataFile file(int t, int i) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(WAREHOUSE + "/db/t" + t + "/data/file-" + i + ".parquet")
        .withFileSizeInBytes(100)
        .withRecordCount(10)
        .build();
  }

  /** Commits {@code n} merge appends to table {@code t} through {@code catalog}. */
  private static void append(FileIOCatalog catalog, int t, int from, int n) {
    Table loaded = catalog.loadTable(table(t));
    for (int i = from; i < from + n; i++) {
      loaded.newAppend().appendFile(file(t, i)).commit();
    }
  }

  @ParameterizedTest
  @EnumSource(value = LocalCatalogMode.class, names = {"COMPACT_TM", "COMPACT_ML"})
  void untouchedTablesAreWrittenAsTheyWere(LocalCatalogMode mode) {
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    FileIOCatalog writer = mode.catalog(io, WAREHOUSE, "c", Map.of());
    writer.createNamespace(Namespace.of("db"));
    for (int t = 0; t < TABLES; t++) {
      writer.createTable(table(t), SCHEMA);
      append(writer, t, 0, 1);
    }

    // The mode compacts every 3 records: 9 commits to one table compact 3 times, and each
    // compaction writes the other tables from the memo of the catalog that encoded them
    CheckpointMemo memo = writer.log().checkpointMemo();
    long hits = memo.hits();
    append(writer, 0, 1, 9);
    assertThat(memo.hits() - hits)
        .as("tables written from the memo by the writer that encoded them")
        .isGreaterThanOrEqualTo(3L * (TABLES - 1));

    // A catalog that decodes the checkpoint has its tables' bytes from the decode
    FileIOCatalog reader = mode.catalog(io, WAREHOUSE, "c", Map.of());
    append(reader, 1, 1, 3);
    assertThat(reader.log().checkpointMemo().hits())
        .as("tables written from the memo of the decode")
        .isGreaterThanOrEqualTo(TABLES - 1);

    CheckpointRoundTrip.assertEveryWrite(io);
    assertThat(writer.loadTable(table(0)).currentSnapshot().addedDataFiles(io))
        .extracting(DataFile::location)
        .containsExactly(file(0, 9).location());
  }

  @Test
  void theSwitchTurnsTheMemoOff() { // for ablation
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    FileIOCatalog catalog = LocalCatalogMode.COMPACT_ML.catalog(io, WAREHOUSE, "c",
        Map.of(LogCatalogProperties.CHECKPOINT_MEMOIZE, "false"));
    assertThat(catalog.log().checkpointMemo()).isNull();
    catalog.createNamespace(Namespace.of("db"));
    catalog.createTable(table(0), SCHEMA);
    append(catalog, 0, 0, 4);
    CheckpointRoundTrip.assertEveryWrite(io);
    assertThat(LocalCatalogMode.COMPACT_ML.<FileIOCatalog>catalog(io, WAREHOUSE, "c", Map.of())
            .log().checkpointMemo())
        .isNotNull();
  }
}
