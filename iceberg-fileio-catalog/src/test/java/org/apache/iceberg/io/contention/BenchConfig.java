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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * One run of the contention driver: the store, the catalog, the tables, the writers and their
 * operations. A file holds one configuration, or a base and a {@code vary} block, every
 * combination of whose values is a run ({@link #expand}). Every key has a default ({@link
 * #DEFAULTS}); a run's configuration is the defaults with the file's values over them, and its
 * hash ({@link #hash}) names its results.
 */
final class BenchConfig {
  static final ObjectMapper MAPPER =
      new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

  /** Every key, with its default. */
  static final String DEFAULTS =
      "{"
          + "\"label\": \"run\","
          + "\"seed\": 1,"
          + "\"repetition\": 0,"
          // the store: an endive profile (StoreProfile), its time scaled (1: as measured)
          + "\"store\": \"s3x\","
          + "\"time_scale\": 1.0,"
          // the catalog
          + "\"encoding\": \"lamport\","
          + "\"mode\": \"ml\","
          + "\"max_append_count\": 100,"
          + "\"max_append_size\": 16777216,"
          // the tables: partitioned by identity on an int column
          + "\"tables\": 1,"
          + "\"table_dist\": \"uniform\","
          + "\"partitions\": 32,"
          + "\"format_version\": 2,"
          + "\"preload_commits\": 50,"
          // the load: closed (writers, each thinking between operations) or open (arrivals/s)
          + "\"writers\": 4,"
          + "\"think_ms\": 0,"
          + "\"arrival_rate\": 0,"
          + "\"max_inflight\": 64,"
          + "\"duration_s\": 60,"
          + "\"warmup_s\": 10,"
          + "\"drain_s\": 30,"
          // the operations: weights, partitions each touches, files per partition, job runtime
          + "\"mix\": {\"fast_append\": 1.0},"
          + "\"partitions_per_txn\": 1,"
          + "\"partition_dist\": \"uniform\","
          + "\"zipf_alpha\": 1.5,"
          + "\"files_per_partition\": 1,"
          // a job's runtime, lognormal about its median: by kind (runtime_ms), else the default
          + "\"runtime_median_ms\": 0,"
          + "\"runtime_sigma\": 0,"
          + "\"runtime_ms\": {},"
          // Iceberg's commit retry (TableProperties commit.retry.*)
          + "\"retry_num\": 4,"
          + "\"retry_min_wait_ms\": 100,"
          + "\"retry_max_wait_ms\": 60000,"
          // maintenance: expire all but the last `retain` snapshots every `expire_every_s`
          + "\"expire_every_s\": 5,"
          + "\"expire_retain\": 50,"
          // sampling of the catalog object
          + "\"sample_every_ms\": 1000"
          + "}";

  private final ObjectNode node;

  private BenchConfig(ObjectNode node) {
    this.node = node;
  }

  /** The runs a configuration file's JSON holds: its base, or each combination it varies. */
  static List<BenchConfig> expand(JsonNode file) {
    ObjectNode base = (ObjectNode) defaults();
    ObjectNode given = file.deepCopy();
    JsonNode vary = given.remove("vary");
    merge(base, given);
    List<ObjectNode> runs = new ArrayList<>();
    runs.add(base);
    if (vary != null) {
      Iterator<Map.Entry<String, JsonNode>> fields = vary.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> axis = fields.next();
        List<ObjectNode> next = new ArrayList<>();
        for (ObjectNode run : runs) {
          for (JsonNode value : (ArrayNode) axis.getValue()) {
            ObjectNode copy = run.deepCopy();
            if (value.isObject() && axis.getKey().startsWith("+")) {
              merge(copy, (ObjectNode) value); // "+name": [{...}, ...] sets several keys at once
            } else {
              copy.set(axis.getKey(), value);
            }
            next.add(copy);
          }
        }
        runs = next;
      }
    }
    List<BenchConfig> configs = new ArrayList<>();
    for (ObjectNode run : runs) {
      for (Iterator<String> it = run.fieldNames(); it.hasNext(); ) {
        String key = it.next();
        if (!defaults().has(key)) {
          throw new IllegalArgumentException("Unknown configuration key: " + key);
        }
      }
      configs.add(new BenchConfig(run));
    }
    return configs;
  }

  private static JsonNode defaults() {
    try {
      return MAPPER.readTree(DEFAULTS);
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }

  private static void merge(ObjectNode into, ObjectNode from) {
    from.fields().forEachRemaining(e -> into.set(e.getKey(), e.getValue()));
  }

  JsonNode node() {
    return node;
  }

  String string(String key) {
    return node.get(key).asText();
  }

  int integer(String key) {
    return node.get(key).asInt();
  }

  long longValue(String key) {
    return node.get(key).asLong();
  }

  double number(String key) {
    return node.get(key).asDouble();
  }

  boolean bool(String key) {
    return node.get(key).asBoolean();
  }

  /** The operation weights, by name, in a stable order. */
  Map<String, Double> mix() {
    Map<String, Double> mix = new TreeMap<>();
    node.get("mix").fields().forEachRemaining(e -> mix.put(e.getKey(), e.getValue().asDouble()));
    return mix;
  }

  /** The configuration as JSON with sorted keys. */
  String canonical() {
    try {
      return MAPPER.writeValueAsString(MAPPER.treeToValue(node, Object.class));
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }

  /** The first 12 hex digits of the SHA-256 of the configuration, less its label and repetition. */
  String hash() {
    ObjectNode copy = node.deepCopy();
    copy.remove("label");
    copy.remove("repetition");
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256")
              .digest(
                  MAPPER.writeValueAsString(MAPPER.treeToValue(copy, Object.class))
                      .getBytes(StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder();
      for (int i = 0; i < 6; i++) {
        hex.append(String.format("%02x", digest[i]));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException | java.io.IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /** The configuration as a map, for the run's manifest. */
  @SuppressWarnings("unchecked")
  Map<String, Object> asMap() {
    try {
      return MAPPER.treeToValue(node, LinkedHashMap.class);
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }
}
