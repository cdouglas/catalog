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

import java.util.List;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;

/**
 * Strategy for encoding and decoding {@link TableMetadata} bytes when stored
 * inline in {@code ProtoCatalogFormat} (the {@code metadata} field of
 * {@code CreateTableInline}, the {@code full_metadata} oneof case of
 * {@code UpdateTableInline}, and the {@code metadata} field of
 * {@code InlineTable} in the checkpoint).
 *
 * <p>Two implementations are wired through the catalog:
 *
 * <ul>
 *   <li>{@link GzipInlineMetadataCodec} — Iceberg's {@code Codec.GZIP} JSON
 *       (gzip-wrapped). Roughly 5x smaller than raw JSON, byte-for-byte
 *       compatible with anything that reads {@code metadata.json.gz}.
 *   <li>{@link StructuralInlineMetadataCodec} — columnar layout that exploits
 *       Iceberg-specific redundancy (parent-offset, manifest-list path
 *       templating, summary-key typing, schema-id RLE, timestamp/sequence
 *       deltas, metadata-log templating). 40% smaller than gzip(JSON) at the
 *       corpus median and 4-7x faster on lazy-access decode workloads. See
 *       {@code docs/TM_ENCODING_BENCH_RESULTS.md}.
 * </ul>
 *
 * <p>Each encoded inline-TM record on disk carries a one-byte codec
 * discriminator (the {@link #tag()}) so a single catalog file may freely mix
 * codecs across tables. Decode is always driven by the per-record tag, never
 * by the catalog's currently-configured default.
 */
public interface InlineMetadataCodec {

  /** Numeric value matching the proto {@code InlineMetadataCodec} enum. */
  byte tag();

  /** Short name used in the {@code fileio.catalog.inline.tm.codec} config knob. */
  String shortName();

  /** Encode the given metadata. Deterministic given equal inputs. */
  byte[] encode(TableMetadata meta);

  /**
   * Materialise the entire {@link TableMetadata} from {@code encoded}, binding
   * the result to {@code metadataLocation} (the synthetic {@code inline://}
   * URI the catalog uses as a cache key, or any other opaque location string).
   */
  TableMetadata decodeFull(byte[] encoded, String metadataLocation);

  /**
   * Return only the snapshot referenced by {@code current-snapshot-id} in the
   * encoded blob, or {@code null} if the table has no current snapshot.
   * Implementations should avoid materialising other snapshots when the wire
   * format permits — that's the headroom the structural codec offers on lazy
   * read paths.
   */
  Snapshot decodeCurrentSnapshotOnly(byte[] encoded);

  /**
   * Return the current snapshot followed by up to {@code k - 1} of its
   * ancestors via {@code parent-snapshot-id}. Order: current first, then
   * most-recent parent, etc. Returns an empty list when the table has no
   * current snapshot.
   */
  List<Snapshot> decodeRecentSnapshots(byte[] encoded, int k);
}
