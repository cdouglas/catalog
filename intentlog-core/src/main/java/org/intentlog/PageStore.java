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
 * <p>Every writer that puts a name puts the same bytes, so a put may be repeated, and a put that
 * landed with its reply lost is no different from one that landed. Replay never reads a page: the
 * runtime gets one only to learn whether an id in (H, F] committed, and deletes one only once a GC
 * record dropped its reference, or as an orphan once the floor passed its node unreferenced.
 */
public interface PageStore {

  /** Stores {@code page} under {@code name}. Idempotent: a name's bytes never change. */
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
}
