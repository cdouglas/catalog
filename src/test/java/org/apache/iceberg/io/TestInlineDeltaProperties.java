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

import static org.apache.iceberg.types.Types.NestedField.required;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.statistics.Statistics;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.inmemory.InMemoryCatalog;

/**
 * The inline delta round trip: a commit that an inline catalog sends as a delta replays to the
 * metadata the writer committed. {@link InlineDeltaCodec#computeDelta} must either return null,
 * which sends the full metadata, or a delta that holds every change; a change it has no update
 * for and does not refuse is lost on replay (Bugs 37 and 39: a format-version upgrade and a
 * removed ref, each lost when the commit made another change too).
 *
 * <p>A generated schedule ({@link TableScript}) runs Iceberg operations, alone or several in a
 * table transaction, on a table in an {@link InMemoryCatalog}. Each commit takes the table from
 * {@code base} to {@code next}; its delta, encoded and replayed by {@link
 * ProtoCodec.UpdateTableInlineAction} on {@code base} as the inline codec stores it, must equal
 * {@code next} as the codec stores it (less the metadata log, which inline storage synthesizes).
 * Operations Iceberg rejects are skipped. Every committed version must also survive the inline
 * codec exactly, metadata log included.
 */
class TestInlineDeltaProperties {

  private static final ObjectMapper JSON = new ObjectMapper();

  @Property(tries = 400)
  void deltaReplaysToTheCommittedMetadata(
      @ForAll @IntRange(min = 2, max = 3) int formatVersion,
      @ForAll boolean partitioned,
      @ForAll boolean structural,
      @ForAll("schedules") List<TableScript.Cmd> schedule) {
    InlineMetadataCodec codec =
        structural ? InlineMetadataCodecs.STRUCTURAL : InlineMetadataCodecs.JSON_GZIP;
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("c", Map.of());
    catalog.createNamespace(Namespace.of("ns"));
    StringBuilder trace = new StringBuilder(String.format("v%d %s, %s%n", formatVersion,
        partitioned ? "partitioned" : "unpartitioned", codec.getClass().getSimpleName()));
    TableScript.Runner runner =
        new TableScript.Runner(catalog, formatVersion, partitioned, trace);
    for (List<TableScript.Cmd> step : TableScript.steps(schedule)) {
      TableOperations ops = ((HasTableOperations) runner.table).operations();
      TableMetadata base = ops.current();
      RuntimeException rejected = runner.run(step);
      Statistics.label("step")
          .collect(step.get(0).kind + (rejected == null ? "" : " rejected"));
      if (rejected == null) {
        check(codec, base, ops.refresh(), trace);
      }
    }
  }

  @Provide
  Arbitrary<List<TableScript.Cmd>> schedules() {
    return TableScript.schedules();
  }

  /**
   * Checks that {@code codec} stores {@code next} exactly, metadata log included, and that the
   * delta of the commit from {@code base} replays to it.
   */
  private static void check(
      InlineMetadataCodec codec, TableMetadata base, TableMetadata next, StringBuilder trace) {
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
    TableMetadata replayed = codec.decodeFull(builder.inlineMetadata(1), "inline://replayed");
    TableMetadata expected = codec.decodeFull(codec.encode(next), "inline://expected");
    String difference = difference(canonical(expected), canonical(replayed));
    if (difference != null) {
      throw new AssertionError(
          "the delta replays to other metadata: " + difference + "\ntrace:\n" + trace);
    }
  }

  /**
   * The metadata as JSON, less the metadata log, which inline storage synthesizes, and with its
   * statistics files by snapshot id: TableMetadata.Builder lists them in a HashMap's order, so a
   * replay that builds the metadata in fewer steps than its writer can list them otherwise.
   */
  private static JsonNode canonical(TableMetadata metadata) {
    ObjectNode node = (ObjectNode) json(metadata);
    node.remove("metadata-log");
    for (String field : List.of("statistics", "partition-statistics")) {
      if (node.get(field) != null) {
        List<JsonNode> files = new java.util.ArrayList<>();
        node.get(field).forEach(files::add);
        files.sort(java.util.Comparator.comparingLong(f -> f.get("snapshot-id").asLong()));
        node.set(field, JSON.valueToTree(files));
      }
    }
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
