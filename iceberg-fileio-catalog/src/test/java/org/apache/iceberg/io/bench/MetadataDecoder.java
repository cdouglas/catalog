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

import java.util.List;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;

/**
 * Encoder/decoder pair for inline TableMetadata, with three decode workloads
 * matching what real callers actually need from the catalog read path.
 *
 * <p>Each candidate in the bench implements {@link #encode(TableMetadata)} plus
 * the three {@code decode*} methods so the bench can compare not just bytes
 * saved, but also <em>which encoding's wire format makes lazy access cheap</em>
 * — the second axis on which a candidate can ship even if the bytes win is
 * marginal (Phase F path 2).
 */
public interface MetadataDecoder {

  /** Short label for report tables ("json-gzip", "json-zstd", "json-gzip+proto-snapshots"). */
  String name();

  /** Full encode. Deterministic given equal inputs. */
  byte[] encode(TableMetadata meta);

  /** Materialize the entire {@link TableMetadata} from {@code encoded} bytes. */
  TableMetadata decodeFull(byte[] encoded);

  /**
   * Return only the snapshot referenced by {@code current-snapshot-id}, or {@code null}
   * if the table has no current snapshot. The implementation should avoid
   * materializing other snapshots when the wire format permits — that's the lazy-access
   * win we measure.
   */
  Snapshot decodeCurrentSnapshotOnly(byte[] encoded);

  /**
   * Return the current snapshot followed by up to {@code k - 1} of its ancestors via
   * {@code parent-snapshot-id}. Order: current first, then most-recent parent, etc.
   * If the table has no current snapshot, returns an empty list.
   */
  List<Snapshot> decodeRecentSnapshots(byte[] encoded, int k);
}
