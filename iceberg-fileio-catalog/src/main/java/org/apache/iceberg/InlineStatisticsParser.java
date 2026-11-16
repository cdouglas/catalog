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
package org.apache.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.iceberg.util.JsonUtil;

/**
 * Statistics files as Iceberg's JSON of them, which table metadata files hold: for a delta's
 * statistics updates (catalog.proto's {@code SetStatistics} and {@code SetPartitionStatistics}).
 * Iceberg's parsers read JSON only within this package.
 */
public final class InlineStatisticsParser {
  private InlineStatisticsParser() {}

  public static String toJson(StatisticsFile file) {
    return StatisticsFileParser.toJson(file);
  }

  public static StatisticsFile statisticsFromJson(String json) {
    try {
      return StatisticsFileParser.fromJson(JsonUtil.mapper().readTree(json));
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot parse a statistics file: " + json, e);
    }
  }

  public static String toJson(PartitionStatisticsFile file) {
    return PartitionStatisticsFileParser.toJson(file);
  }

  public static PartitionStatisticsFile partitionStatisticsFromJson(String json) {
    try {
      return PartitionStatisticsFileParser.fromJson(JsonUtil.mapper().readTree(json));
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot parse a partition statistics file: " + json, e);
    }
  }
}
