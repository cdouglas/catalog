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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.invoke.SerializedLambda;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.io.BaseEncoding;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * An inline table shipped to a worker as Spark and Flink ship one: {@code
 * SerializableTable} copies a table's schema, specs and FileIO, and loads the rest from the table's
 * metadata file, which an inline table does not have. The copy must carry the table's metadata, so
 * that its snapshots, history and scans read as the table's, and so must a copy of each of its
 * metadata tables; by Java serialization (Flink, Spark's default) and by Kryo (Spark's other), at
 * format v2 and v3 (whose snapshots carry row lineage), and for a table with a metadata file too.
 */
class TestSerializableInlineTable {
  private static final TableIdentifier TABLE = TableIdentifier.of("ns", "t");
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "cat", Types.IntegerType.get()));
  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("cat").build();

  private int files = 0;

  private DataFile file() {
    return DataFiles.builder(SPEC)
        .withPath("mem:///data/f" + files + ".parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .withPartitionPath("cat=" + (files++ % 4))
        .build();
  }

  /** Serializes and deserializes a value. */
  @FunctionalInterface
  interface RoundTrip {
    <T> T of(T value) throws Exception;
  }

  static Stream<Arguments> modesAndSerializers() {
    return Stream.of("CAS", "CAS_TM", "CAS_ML", "COMPACT_ML", "MEMLOG_ML")
        .flatMap(mode -> Stream.of("java", "kryo")
            .flatMap(serializer -> Stream.of(2, 3)
                .map(version -> Arguments.of(LocalCatalogMode.valueOf(mode), serializer, version))));
  }

  @ParameterizedTest(name = "{0} {1} v{2}")
  @MethodSource("modesAndSerializers")
  void aSerializedTableReadsAsTheTable(LocalCatalogMode mode, String serializer, int formatVersion)
      throws Exception {
    RoundTrip roundTrip = serializer.equals("kryo")
        ? TestSerializableInlineTable::kryoRoundTrip
        : TestHelpers::roundTripSerialize;
    LogCatalog catalog = mode.catalog(new InMemoryAtomicFileIO(), "mem:///wh", "ser", Map.of());
    catalog.createNamespace(Namespace.of("ns"));
    Table table = catalog.buildTable(TABLE, SCHEMA)
        .withPartitionSpec(SPEC)
        .withProperty(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
        .create();
    DataFile first = file();
    table.newFastAppend().appendFile(first).appendFile(file()).commit();
    table.newAppend().appendFile(file()).commit();
    table.newDelete().deleteFile(first).commit();
    table.manageSnapshots().createTag("t1", table.currentSnapshot().snapshotId()).commit();
    table = catalog.loadTable(TABLE);
    assertThat(((HasTableOperations) table).operations().current().metadataFileLocation())
        .startsWith(mode.inlineTM ? "inline://" : "mem:///wh/");

    Table copy = roundTrip.of(org.apache.iceberg.SerializableTable.copyOf(table));
    TestHelpers.assertSerializedAndLoadedMetadata(table, copy);
    assertThat(copy.refs()).isEqualTo(table.refs());
    assertThat(read(copy)).isEqualTo(read(table)).hasSize(2);

    for (MetadataTableType type : MetadataTableType.values()) {
      Table metadataTable = MetadataTableUtils.createMetadataTableInstance(table, type);
      Table metadataCopy = roundTrip.of(org.apache.iceberg.SerializableTable.copyOf(metadataTable));
      TestHelpers.assertSerializedAndLoadedMetadata(metadataTable, metadataCopy);
      if (type != MetadataTableType.POSITION_DELETES) {
        assertThat(read(metadataCopy)).as("%s", type).isEqualTo(read(metadataTable));
      }
    }
  }

  /**
   * {@code value} serialized and deserialized by Kryo, set up as Iceberg's KryoHelpers sets it up;
   * the test store's FileIO goes as Java serializes it, as the store itself.
   */
  @SuppressWarnings("unchecked")
  static <T> T kryoRoundTrip(T value) {
    Kryo kryo = new Kryo();
    kryo.setInstantiatorStrategy(
        new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
    kryo.register(SerializedLambda.class);
    kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer());
    kryo.register(InMemoryAtomicFileIO.class, new JavaSerializer());
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (Output out = new Output(bytes)) {
      kryo.writeClassAndObject(out, value);
    }
    try (Input in = new Input(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (T) kryo.readClassAndObject(in);
    }
  }

  /** What a scan of {@code table} reads: its tasks' files, and a metadata table's rows. */
  private static List<String> read(Table table) throws Exception {
    List<String> read = new ArrayList<>();
    try (var tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        if (task instanceof DataTask) {
          try (var rows = ((DataTask) task).rows()) {
            rows.forEach(row -> read.add(render(row)));
          }
        } else {
          read.add(task.file().location());
        }
      }
    }
    read.sort(null);
    return read;
  }

  /** {@code value} as text, a struct's fields included (not every row type has a toString). */
  private static String render(Object value) {
    if (value instanceof StructLike) {
      StructLike struct = (StructLike) value;
      List<String> fields = new ArrayList<>();
      for (int i = 0; i < struct.size(); i++) {
        fields.add(render(struct.get(i, Object.class)));
      }
      return "(" + String.join(", ", fields) + ")";
    } else if (value instanceof ByteBuffer) {
      return BaseEncoding.base16().encode(ByteBuffers.toByteArray((ByteBuffer) value));
    } else if (value instanceof Map) {
      Map<String, String> entries = new TreeMap<>();
      ((Map<?, ?>) value).forEach((k, v) -> entries.put(render(k), render(v)));
      return entries.toString();
    } else if (value instanceof List) {
      return ((List<?>) value).stream()
          .map(TestSerializableInlineTable::render)
          .collect(Collectors.toList())
          .toString();
    }
    return String.valueOf(value);
  }
}
