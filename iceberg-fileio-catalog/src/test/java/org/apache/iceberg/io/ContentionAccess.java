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

import org.intentlog.LogView;
import org.intentlog.fileio.AtomicFileLog;

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

  /** The bytes of the checkpoint of the catalog object {@code bytes}. */
  public static int checkpointBytes(byte[] bytes) {
    return AtomicFileLog.decode(bytes).checkpoint().map(c -> c.length).orElse(0);
  }
}
