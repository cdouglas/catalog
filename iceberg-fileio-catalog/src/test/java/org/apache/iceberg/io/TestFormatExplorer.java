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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Keeps catalog.proto, the codecs and the format explorer in step: the codecs' field numbers are
 * the ones catalog.proto declares, and the explorer's embedded schema and scenarios are the ones
 * {@link FormatExplorerFixtures} generates from the current code.
 */
public class TestFormatExplorer {
  private static final String REGENERATE =
      "the format explorer's fixtures are stale; regenerate them with "
          + "mvn test -Dtest=FormatExplorerFixtures -Dexplorer.regenerate=true";

  /** Each codec's constant prefixes, by the catalog.proto message or enum they number. */
  private static final Map<String, Map<String, String>> PREFIXES = new LinkedHashMap<>();

  static {
    Map<String, String> codec = new LinkedHashMap<>();
    codec.put("CHECKPOINT_", "Checkpoint");
    codec.put("NS_", "Namespace");
    codec.put("TBL_", "Table");
    codec.put("NSPROP_", "NamespaceProperty");
    codec.put("INLINE_TBL_", "InlineTable");
    codec.put("MF_", "ManifestFileEntry");
    codec.put("PFS_", "PartitionFieldSummaryEntry");
    codec.put("SMR_", "SnapshotManifestRefs");
    codec.put("INTENTION_", "Intention");
    codec.put("ACTION_", "Action");
    codec.put("CREATE_NS_", "CreateNamespace");
    codec.put("DROP_NS_", "DropNamespace");
    codec.put("SET_NSPROP_", "SetNamespaceProperty");
    codec.put("REMOVE_NSPROP_", "RemoveNamespaceProperty");
    codec.put("CREATE_TBL_", "CreateTable");
    codec.put("DROP_TBL_", "DropTable");
    codec.put("UPDATE_TBL_", "UpdateTableLocation");
    codec.put("READ_TBL_", "ReadTable");
    codec.put("UPDATE_INLINE_", "UpdateTableInline");
    codec.put("CREATE_INLINE_TBL_", "CreateTableInline");
    codec.put("RENAME_TBL_", "RenameTable");
    codec.put("SET_CONFLICT_ENCODING_", "SetConflictEncoding");
    PREFIXES.put("ProtoCodec", codec);

    Map<String, String> committed = new LinkedHashMap<>();
    committed.put("COMMITTED_TXNS_", "CommittedTransactions");
    PREFIXES.put("CommittedSetCodec", committed);

    Map<String, String> envelope = new LinkedHashMap<>();
    envelope.put("LOG_CHECKPOINT_", "LogCheckpoint");
    envelope.put("LOG_RECORD_", "LogRecord");
    envelope.put("ADVANCE_HORIZON_", "AdvanceHorizon");
    PREFIXES.put("LogEnvelope", envelope);

    Map<String, String> delta = new LinkedHashMap<>();
    delta.put("DELTA_", "TableMetadataDelta");
    delta.put("UPDATE_", "TableMetadataUpdate");
    delta.put("ADD_MF_", "AddManifestDelta");
    delta.put("REMOVE_MF_", "RemoveManifestDelta");
    delta.put("REMOVE_SCHEMA_", "RemoveSchemas");
    delta.put("REMOVE_SPEC_", "RemovePartitionSpecs");
    delta.put("SET_PROPS_", "SetTableProperties");
    delta.put("SET_LOC_", "SetTableLocation");
    delta.put("ADD_SCHEMA_", "AddSchema");
    delta.put("SET_SCHEMA_", "SetCurrentSchema");
    delta.put("ADD_SPEC_", "AddPartitionSpec");
    delta.put("SET_SPEC_", "SetDefaultPartitionSpec");
    delta.put("ADD_ORDER_", "AddSortOrder");
    delta.put("SF_", "SortField");
    delta.put("SET_ORDER_", "SetDefaultSortOrder");
    delta.put("REF_", "SetSnapshotRef");
    delta.put("REMOVE_SNAP_", "RemoveSnapshots");
    delta.put("SNAP_", "AddSnapshot");
    delta.put("CS_", "CompactSummary");
    delta.put("TK_", "TransformKind");
    delta.put("DIR_", "SortDirection");
    delta.put("NO_", "NullOrderKind");
    delta.put("MAP_ENTRY_", null); // a map field's entry: key = 1, value = 2
    PREFIXES.put("InlineDeltaCodec", delta);
  }

  /** Abbreviations in constant names, spelled out as catalog.proto spells them. */
  private static final Map<String, String> WORDS =
      Map.of(
          "NS", "NAMESPACE",
          "TBL", "TABLE",
          "TXN", "TRANSACTION",
          "TXNS", "TRANSACTIONS",
          "SEQ", "SEQUENCE",
          "NUM", "NUMBER",
          "COL", "COLUMN",
          "PART", "PARTITION",
          "SNAP", "SNAPSHOT");

  /** Constants whose name shares no word with their field's. */
  private static final Map<String, String> ALIASES = Map.of("SF_TRANSFORM_KIND", "kind");

  /**
   * Each codec constant numbers a field (or enum value) catalog.proto declares, with a name that
   * says the same, and each catalog.proto field has a constant: a field the codec writes but the
   * schema lacks, or one the schema declares but the codec never reads, fails here even if no
   * test writes it.
   */
  @Test
  public void codecConstantsMatchCatalogProto() throws IOException {
    CatalogProto.Schema schema = CatalogProto.schema();
    List<String> problems = new ArrayList<>();
    Map<String, Set<Integer>> numbered = new LinkedHashMap<>();
    for (Map.Entry<String, Map<String, String>> codec : PREFIXES.entrySet()) {
      String source = Files.readString(source(codec.getKey()));
      Matcher m =
          Pattern.compile("static\\s+final\\s+int\\s+([A-Z][A-Z0-9_]*)\\s*=\\s*(-?\\d+)\\s*;")
              .matcher(source);
      while (m.find()) {
        String name = m.group(1);
        int number = Integer.parseInt(m.group(2));
        if (name.startsWith("WIRE_") || name.equals("SUPPORTED_READER_VERSION")) {
          continue;
        }
        String prefix =
            codec.getValue().keySet().stream()
                .filter(name::startsWith)
                .max((x, y) -> Integer.compare(x.length(), y.length()))
                .orElse(null);
        if (prefix == null) {
          problems.add(codec.getKey() + "." + name + " has no prefix in TestFormatExplorer");
          continue;
        }
        String target = codec.getValue().get(prefix);
        if (target == null) {
          if (number != 1 && number != 2) {
            problems.add(codec.getKey() + "." + name + " = " + number + " is not a map entry field");
          }
          continue;
        }
        String suffix = name.substring(prefix.length());
        if (schema.enums.containsKey(target)) {
          Integer declared = schema.enums.get(target).values.get(suffix);
          if (declared == null || declared != number) {
            problems.add(String.format(
                "%s.%s = %d, but catalog.proto's %s has %s = %s",
                codec.getKey(), name, number, target, suffix, declared));
          }
          continue;
        }
        CatalogProto.Message message = schema.messages.get(target);
        if (message == null) {
          problems.add("catalog.proto has no message " + target + " for " + name);
          continue;
        }
        numbered.computeIfAbsent(target, k -> new HashSet<>()).add(number);
        CatalogProto.Field field = message.field(number);
        if (field == null) {
          problems.add(String.format(
              "%s.%s = %d, but %s has no field %d", codec.getKey(), name, number, target, number));
        } else if (!sameName(name, suffix, field.name, target)) {
          problems.add(String.format(
              "%s.%s = %d, but %s field %d is %s",
              codec.getKey(), name, number, target, number, field.name));
        }
      }
    }
    for (CatalogProto.Message message : schema.messages.values()) {
      Set<Integer> numbers = numbered.getOrDefault(message.name, Set.of());
      for (CatalogProto.Field field : message.fields) {
        if (!numbers.contains(field.number)) {
          problems.add(String.format(
              "no codec constant numbers %s.%s (%d)", message.name, field.name, field.number));
        }
      }
    }
    assertThat(problems).isEmpty();
  }

  /** The source of a codec: this module's, or intentlog-core's for the envelope's. */
  private static Path source(String codec) {
    return codec.equals("CommittedSetCodec") || codec.equals("LogEnvelope")
        ? Paths.get("../intentlog-core/src/main/java/org/intentlog/wire/" + codec + ".java")
        : Paths.get("src/main/java/org/apache/iceberg/io/" + codec + ".java");
  }

  /**
   * Whether a constant names its field: each word of {@code suffix}, abbreviations spelled out,
   * is a word of the field's name or of its message's (UPDATE_INLINE_TBL_ID is
   * UpdateTableInline.id).
   */
  private static boolean sameName(String constant, String suffix, String field, String message) {
    if (field.equals(ALIASES.get(constant))) {
      return true;
    }
    Set<String> words = new HashSet<>(Arrays.asList(field.toUpperCase().split("_")));
    words.addAll(
        Arrays.stream(message.split("(?=[A-Z])")).map(String::toUpperCase)
            .collect(Collectors.toList()));
    for (String word : suffix.split("_")) {
      if (!words.contains(WORDS.getOrDefault(word, word))) {
        return false;
      }
    }
    return true;
  }

  /**
   * The fixtures embedded in the explorer are the ones the current code generates. Gzip output
   * depends on the JDK's zlib, so the comparison runs only where the deflate probe matches.
   */
  @Test
  public void fixturesAreCurrent() throws IOException {
    String embedded = embedded();
    String probe = new ObjectMapper().readTree(embedded).path("deflateProbe").asText(null);
    assumeTrue(
        FormatExplorerFixtures.deflateProbe().equals(probe),
        "this JDK's deflate output differs from the one that generated the fixtures");
    assertThat(embedded).as(REGENERATE).isEqualTo(FormatExplorerFixtures.fixturesJson());
  }

  /** Every embedded scenario conforms to catalog.proto, whatever the JDK. */
  @Test
  public void embeddedScenariosConformToCatalogProto() throws IOException {
    JsonNode fixtures = new ObjectMapper().readTree(embedded());
    assertThat(fixtures.path("scenarios")).as(REGENERATE).isNotEmpty();
    for (JsonNode scenario : fixtures.path("scenarios")) {
      byte[] bytes = hex(scenario.path("hex").asText());
      assertThat(CatalogProto.violation(bytes))
          .as(scenario.path("name").asText() + ": " + REGENERATE)
          .isNull();
    }
  }

  /** The conformance check fails a file with a field the schema lacks. */
  @Test
  public void conformanceFindsAFieldTheSchemaLacks() throws IOException {
    String proto = Files.readString(CatalogProto.PROTO_FILE);
    String withoutRename = proto.replaceAll("(?m)^\\s*RenameTable\\s+rename_table\\s*=\\s*11;.*$", "");
    assertThat(withoutRename).isNotEqualTo(proto);
    CatalogProto.Schema schema =
        CatalogProto.parse(Files.readString(CatalogProto.LOG_PROTO_FILE), withoutRename);
    JsonNode fixtures = new ObjectMapper().readTree(embedded());
    for (JsonNode scenario : fixtures.path("scenarios")) {
      if (scenario.path("name").asText().equals("rename-table-across-namespaces")) {
        assertThat(CatalogProto.violation(schema, hex(scenario.path("hex").asText())))
            .contains("field #11")
            .contains("is not in Action");
        return;
      }
    }
    throw new AssertionError("no rename scenario: " + REGENERATE);
  }

  private static String embedded() throws IOException {
    String embedded =
        FormatExplorerFixtures.embeddedFixtures(Files.readString(FormatExplorerFixtures.HTML));
    assertThat(embedded).as("index.html has no fixtures block").isNotNull();
    return embedded;
  }

  private static byte[] hex(String hex) {
    byte[] bytes = new byte[hex.length() / 2];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
    }
    return bytes;
  }
}
