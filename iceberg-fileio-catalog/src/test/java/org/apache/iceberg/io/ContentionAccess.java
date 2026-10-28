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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.intentlog.Intention;
import org.intentlog.LogView;
import org.intentlog.StateMachine;
import org.intentlog.fileio.AtomicFileLog;
import org.intentlog.wire.ProtoWire;

/** What the contention driver (org.apache.iceberg.io.contention) reads of a catalog. */
public final class ContentionAccess {
  private ContentionAccess() {}

  /** How {@code catalog}'s commits ended since it opened its log. */
  public static CatalogLog.CommitCounts counts(LogCatalog catalog) {
    return catalog.log().counts();
  }

  /** The intention records after the checkpoint of the catalog object {@code bytes}. */
  public static int records(byte[] bytes) {
    LogView view = AtomicFileLog.decode(bytes);
    return view.records().size();
  }

  /** The catalog's state machine, for a driver that replays its records (IngestBench). */
  public static StateMachine<CatalogState, CatalogState.Builder, ? extends Intention<CatalogState.Builder>>
      machine() {
    return CatalogStateMachine.INSTANCE;
  }

  /**
   * Where the bytes of a catalog checkpoint's state (catalog.proto Checkpoint) go, counting each
   * field's value (not its tag and length): namespaces,
   * pointer-mode tables, namespace properties, and inline tables by field (metadata, manifest pool,
   * manifest lists, conflict state, the rest), with the pool's entries and the lists' count.
   */
  public static Map<String, Long> checkpointSizes(byte[] state) {
    Map<String, Long> sizes = new LinkedHashMap<>();
    for (String key : List.of("namespaces", "pointer_tables", "namespace_properties",
        "inline_metadata", "inline_pool", "inline_lists", "inline_conflict", "inline_other",
        "other", "inline_tables", "pool_entries", "lists")) {
      sizes.put(key, 0L);
    }
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(state);
      while (in.available() > 0) {
        int tag = ProtoWire.readVarint(in);
        int field = tag >>> 3;
        if ((tag & 0x7) != 2) {
          int before = in.available();
          ProtoWire.skipField(in, tag & 0x7);
          sizes.merge("other", (long) (before - in.available()), Long::sum);
          continue;
        }
        byte[] value = ProtoWire.readLengthDelimitedBytes(in);
        if (field == ProtoCodec.CHECKPOINT_NAMESPACES) {
          sizes.merge("namespaces", (long) value.length, Long::sum);
        } else if (field == ProtoCodec.CHECKPOINT_TABLES) {
          sizes.merge("pointer_tables", (long) value.length, Long::sum);
        } else if (field == ProtoCodec.CHECKPOINT_NAMESPACE_PROPERTIES) {
          sizes.merge("namespace_properties", (long) value.length, Long::sum);
        } else if (field == ProtoCodec.CHECKPOINT_INLINE_TABLES) {
          sizes.merge("inline_tables", 1L, Long::sum);
          inlineTableSizes(value, sizes);
        } else {
          sizes.merge("other", (long) value.length, Long::sum);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return sizes;
  }

  private static void inlineTableSizes(byte[] table, Map<String, Long> sizes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(table);
    while (in.available() > 0) {
      int tag = ProtoWire.readVarint(in);
      int field = tag >>> 3;
      int before = in.available();
      if ((tag & 0x7) != 2) {
        ProtoWire.skipField(in, tag & 0x7);
        sizes.merge("inline_other", (long) (before - in.available()), Long::sum);
        continue;
      }
      long length = ProtoWire.readLengthDelimitedBytes(in).length;
      if (field == ProtoCodec.INLINE_TBL_METADATA) {
        sizes.merge("inline_metadata", length, Long::sum);
      } else if (field == ProtoCodec.INLINE_TBL_MANIFEST_POOL) {
        sizes.merge("inline_pool", length, Long::sum);
        sizes.merge("pool_entries", 1L, Long::sum);
      } else if (field == ProtoCodec.INLINE_TBL_SNAPSHOT_REFS) {
        sizes.merge("inline_lists", length, Long::sum);
        sizes.merge("lists", 1L, Long::sum);
      } else if (field == ProtoCodec.INLINE_TBL_CONFLICT_STATE) {
        sizes.merge("inline_conflict", length, Long::sum);
      } else {
        sizes.merge("inline_other", length, Long::sum);
      }
    }
  }

  /** The bytes of the checkpoint of the catalog object {@code bytes}. */
  public static int checkpointBytes(byte[] bytes) {
    return AtomicFileLog.decode(bytes).checkpoint().map(c -> c.length).orElse(0);
  }
}
