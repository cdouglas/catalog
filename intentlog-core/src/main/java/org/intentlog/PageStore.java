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
package org.intentlog;

import java.util.List;
import java.util.Optional;

/**
 * The pages of a log's committed set: immutable objects, each named by its block's node and
 * digest (docs/COMMITTED_PAGING.md "Page objects"). The provider places a name (a file log beside
 * its object, a queue in its archiver's store), so a log that moves takes its pages along.
 *
 * <p>A name is a content address (a committed-set page's node and digest; a state machine's page
 * names its own, as snapshot pages name their table, version, block and digest), so every writer
 * that puts a name puts the same bytes: a put may be repeated, by this writer or another, and a put
 * that landed with its reply lost is no different from one that landed. Replay never reads a page:
 * the runtime gets one only to learn whether an id in (H, F] committed, and deletes one only once a
 * GC record dropped its reference, or as an orphan once the floor passed its node unreferenced.
 *
 * <p>A {@link #get} must find a page whose {@link #put} completed, by any writer, unless it was
 * deleted since: an empty get is taken for a page that was collected. A lookup of a committed id
 * then reads the log again, and a load of a table's sealed snapshots reads the table as it is
 * now if the catalog no longer names the page; either fails if a fresh read still names it. S3
 * (general-purpose and directory buckets), GCS and ADLS give a get read-after-write, as do the
 * in-memory stores. {@link #list} may lag puts and deletes: only a sweep lists, and it deletes
 * only pages that are garbage under the replica it sweeps from, so a page a lagging list misses
 * waits for a later sweep.
 */
public interface PageStore {

  /**
   * Stores {@code page} under {@code name}. Atomic: a reader finds the name's whole bytes or no
   * page, never part of them, though another writer puts the name at the same time, or crashes
   * putting it; so a put of a name that exists must not write it in place (Bug 116 (zonal page
   * re-put not atomic): on a GCS zonal bucket a plain write is visible before it is finalized).
   * Idempotent: a name's bytes never change, and a put of a name that exists may leave it as it is.
   */
  void put(String name, byte[] page);

  /**
   * The page stored under {@code name}; empty if there is none (it was never put, or was
   * deleted). Any exception means the page could not be read, never that it does not exist.
   */
  Optional<byte[]> get(String name);

  /** Deletes the page {@code name}, if it exists. */
  void delete(String name);

  /** The names of every page stored. */
  List<String> list();

  /**
   * Where the page {@code name} is, for a reader that reads it as a file (a copy of a table on a
   * worker, through the table's FileIO); null if the store's pages are not files.
   */
  default String location(String name) {
    return null;
  }
}
