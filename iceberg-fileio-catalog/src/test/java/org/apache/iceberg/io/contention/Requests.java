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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The requests one thread made of the store, by kind, with the bytes they moved and the latency
 * they waited: each writer's own (a thread's), from which an operation's are the difference
 * between two snapshots ({@link #minus}).
 */
final class Requests {
  long heads;
  long gets;
  long puts;
  long deletes;
  long casLanded;
  long casLost;
  long appendsLanded;
  long appendsLost;
  long bytesRead;
  long bytesWritten;
  /** Bytes of the catalog object read (its GETs). */
  long catalogBytesRead;
  long waitedNanos;

  // Every thread's, for a run's totals: Iceberg's worker pool writes manifests on threads of its own
  private static final java.util.Set<Requests> ALL =
      java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
  private static final ThreadLocal<Requests> MINE =
      ThreadLocal.withInitial(
          () -> {
            Requests requests = new Requests();
            ALL.add(requests);
            return requests;
          });

  /** The calling thread's requests so far. */
  static Requests mine() {
    return MINE.get();
  }

  /** Every thread's requests so far, summed (racy reads of other threads' counts, for totals). */
  static Requests total() {
    Requests t = new Requests();
    for (Requests r : ALL) {
      t.heads += r.heads;
      t.gets += r.gets;
      t.puts += r.puts;
      t.deletes += r.deletes;
      t.casLanded += r.casLanded;
      t.casLost += r.casLost;
      t.appendsLanded += r.appendsLanded;
      t.appendsLost += r.appendsLost;
      t.bytesRead += r.bytesRead;
      t.bytesWritten += r.bytesWritten;
      t.catalogBytesRead += r.catalogBytesRead;
      t.waitedNanos += r.waitedNanos;
    }
    return t;
  }

  Requests copy() {
    Requests c = new Requests();
    c.heads = heads;
    c.gets = gets;
    c.puts = puts;
    c.deletes = deletes;
    c.casLanded = casLanded;
    c.casLost = casLost;
    c.appendsLanded = appendsLanded;
    c.appendsLost = appendsLost;
    c.bytesRead = bytesRead;
    c.bytesWritten = bytesWritten;
    c.catalogBytesRead = catalogBytesRead;
    c.waitedNanos = waitedNanos;
    return c;
  }

  /** These requests less {@code earlier}'s, a snapshot of the same thread's. */
  Requests minus(Requests earlier) {
    Requests d = new Requests();
    d.heads = heads - earlier.heads;
    d.gets = gets - earlier.gets;
    d.puts = puts - earlier.puts;
    d.deletes = deletes - earlier.deletes;
    d.casLanded = casLanded - earlier.casLanded;
    d.casLost = casLost - earlier.casLost;
    d.appendsLanded = appendsLanded - earlier.appendsLanded;
    d.appendsLost = appendsLost - earlier.appendsLost;
    d.bytesRead = bytesRead - earlier.bytesRead;
    d.bytesWritten = bytesWritten - earlier.bytesWritten;
    d.catalogBytesRead = catalogBytesRead - earlier.catalogBytesRead;
    d.waitedNanos = waitedNanos - earlier.waitedNanos;
    return d;
  }

  /** The counts as fields of an operation's record. */
  Map<String, Object> fields() {
    Map<String, Object> f = new LinkedHashMap<>();
    f.put("heads", heads);
    f.put("gets", gets);
    f.put("puts", puts);
    f.put("deletes", deletes);
    f.put("cas_landed", casLanded);
    f.put("cas_lost", casLost);
    f.put("appends_landed", appendsLanded);
    f.put("appends_lost", appendsLost);
    f.put("bytes_read", bytesRead);
    f.put("bytes_written", bytesWritten);
    f.put("catalog_bytes_read", catalogBytesRead);
    f.put("store_wait_ms", waitedNanos / 1e6);
    return f;
  }
}
