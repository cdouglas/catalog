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
          // the tables: partitioned by identity on an int column, each created with
          // table_properties (as Iceberg's TableProperties: commit.manifest-merge.enabled,
          // commit.manifest.min-count-to-merge, ...); an operation's table is uniform or Zipf
          + "\"tables\": 1,"
          + "\"table_dist\": \"uniform\","
          + "\"table_zipf_alpha\": 1.5,"
          + "\"table_properties\": {},"
          + "\"partitions\": 32,"
          + "\"format_version\": 2,"
          + "\"preload_commits\": 50,"
          // the load: closed (writers, each thinking between operations) or open (arrivals/s)
          + "\"writers\": 4,"
          + "\"think_ms\": 0,"
          + "\"arrival_rate\": 0,"
          // exponential gaps of mean 1/rate, or lognormal of median 1/rate (endive's "exponential")
          + "\"arrival_dist\": \"exponential\","
          + "\"arrival_sigma\": 0.5,"
          + "\"max_inflight\": 64,"
          + "\"duration_s\": 60,"
          + "\"warmup_s\": 10,"
          + "\"drain_s\": 30,"
          // the operations: weights (fast_append, merge_append, wap, validated_overwrite,
          // compaction, property, schema, catalog_txn), partitions each touches in a table and
          // their Zipf exponent, files per partition, job runtime
          + "\"mix\": {\"fast_append\": 1.0},"
          + "\"partitions_per_txn\": 1,"
          + "\"partition_dist\": \"uniform\","
          + "\"zipf_alpha\": 1.5,"
          // a catalog transaction's tables (as table_dist chooses them): txn_tables, or with
          // txn_tables_dist exponential a draw of that mean, rounded down, from 1 to
          // txn_tables_max; each a fast append, or a merge append with txn_merge
          + "\"txn_tables\": 2,"
          + "\"txn_tables_dist\": \"fixed\","
          + "\"txn_tables_max\": 8,"
          + "\"txn_merge\": false,"
          + "\"files_per_partition\": 1,"
          // a job's runtime, lognormal about its median: by kind (runtime_ms), else the default
          + "\"runtime_median_ms\": 0,"
          + "\"runtime_sigma\": 0,"
          + "\"runtime_ms\": {},"
          // Iceberg's commit retry (TableProperties commit.retry.*)
          + "\"retry_num\": 4,"
          + "\"retry_min_wait_ms\": 100,"
          + "\"retry_max_wait_ms\": 60000,"
          // maintenance: every `expire_every_s`, expire the snapshots older than
          // `expire_older_than_s` (0: every one) but the last `expire_retain`; a validated
          // operation whose starting snapshot expires fails (Iceberg cannot tell its history)
          + "\"expire_every_s\": 5,"
          + "\"expire_retain\": 50,"
          + "\"expire_older_than_s\": 0,"
          // the catalog's backoff after a lost race (fileio.catalog.race.retry.min-wait-ms)
          + "\"race_min_wait_ms\": 100,"
          // sampling of the catalog object
          + "\"sample_every_ms\": 1000,"
          // the maintenance client's retries (-1: retry_num), so expiry can keep up with appends
          + "\"expire_retry_num\": -1,"
          // write every record the catalog's writes landed, in order, to log.bin (IngestBench)
          + "\"capture_log\": false,"
          // the format of the records a captured log holds (IngestBench sets the current one), so
          // a change of format captures anew: 1 before RemoveManifestDelta.parent_indices and
          // CompactSummary.inherits_parent_extra (2026-10-29), 2 before snapshot ids implied and
          // manifests named by UUID and number
          + "\"capture_format\": 1"
          + "}";

  private final ObjectNode node;

  private BenchConfig(ObjectNode node) {
    this.node = node;
  }

  /** The runs a configuration file's JSON holds: its base, or each combination it varies. */
  static List<BenchConfig> expand(JsonNode file) {
    return expand(file, DEFAULTS);
  }

  /** The runs of a file of another driver's configuration, whose keys are {@code defaults}'. */
  static List<BenchConfig> expand(JsonNode file, String defaults) {
    ObjectNode base = (ObjectNode) parse(defaults);
    JsonNode known = parse(defaults);
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
        if (!known.has(key)) {
          throw new IllegalArgumentException("Unknown configuration key: " + key);
        }
      }
      configs.add(new BenchConfig(run));
    }
    return configs;
  }

  static JsonNode parse(String json) {
    try {
      return MAPPER.readTree(json);
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

  /** The configuration under {@code key}, an object, as one with {@code defaults}' keys. */
  BenchConfig nested(String key, String defaults) {
    return expand(node.get(key), defaults).get(0);
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

  /** The object at {@code key} as strings by name. */
  Map<String, String> stringMap(String key) {
    Map<String, String> map = new TreeMap<>();
    node.get(key).fields().forEachRemaining(e -> map.put(e.getKey(), e.getValue().asText()));
    return map;
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

  /**
   * Keys added after the first campaign (p5, 2026-10-27), with the value the runs that recorded no
   * such key ran (race_min_wait_ms: p6's, the catalog's default then): one at that value is left
   * out of the hash, so a configuration keeps the hash its earlier runs recorded.
   */
  private static final Map<String, String> LATER_KEYS =
      Map.of("expire_older_than_s", "0", "race_min_wait_ms", "20", "expire_retry_num", "-1",
          "capture_log", "false", "capture_format", "1");

  /**
   * The first 12 hex digits of the SHA-256 of the configuration, less its label, its repetition and
   * the later keys at their earlier values.
   */
  String hash() {
    ObjectNode copy = node.deepCopy();
    copy.remove("label");
    copy.remove("repetition");
    LATER_KEYS.forEach((key, value) -> {
      if (copy.has(key) && copy.get(key).asText().equals(value)) {
        copy.remove(key);
      }
    });
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
