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
package org.apache.iceberg.io.bench;

/**
 * Pathology presets for synthetic TableMetadata generation. Each scenario
 * emphasizes one production failure mode while keeping the rest realistic.
 *
 * <p>Used by {@link SyntheticTableMetadata#generate(SyntheticTableMetadata.Knobs)}
 * via {@link SyntheticTableMetadata.Knobs#forScenario}.
 */
public enum Scenario {
  /** Realistic baseline — modest snapshot count, single schema/spec. */
  NORMAL,
  /** Long-lived table, snapshot expiration off or lax. */
  SNAPSHOT_ACCUMULATION,
  /** Frequent ALTER TABLE / partition evolution; old schemas/specs retained. */
  SCHEMA_SPEC_CHURN,
  /** Hundreds-thousands of branches/tags (CI-per-PR catalog patterns). */
  MANY_REFS,
  /** Properties map abused as ETL config dump (multi-KB values). */
  VERBOSE_PROPERTIES,
  /** Statistics + partition statistics arrays per snapshot (Puffin-heavy). */
  STATS_PUFFIN_REFS,
  /** Pathological commit cadence (clustered / uniform / sparse). */
  COMMIT_FREQUENCY,
  /** Long previous-metadata-log via aggressive history retention. */
  METADATA_LOG_RETENTION,
  /** All seven dialed to moderate intensity simultaneously. */
  COMBINED_WORST_CASE
}
