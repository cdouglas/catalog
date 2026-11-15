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
package org.intentlog.fileio;

import org.apache.iceberg.io.InMemoryAtomicFileIO;
import org.intentlog.SharedLog;
import org.intentlog.memory.InMemorySegmentStore;
import org.intentlog.testkit.SharedLogContract;
import org.junit.jupiter.api.Nested;

/** The SharedLog contract on {@link SegmentedLog}: segments of three records, and of the store's. */
class TestSegmentedLogContract {

  static SegmentedLog log(long limit) {
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    AtomicFileLog catalog =
        AtomicFileLog.segmented(io, "mem:///warehouse/catalog", () -> 42L, false);
    return new SegmentedLog(catalog, new InMemorySegmentStore(64, 1 << 16, 1 << 22), "log",
        SegmentedLog.Options.defaults().withLimit(limit).withRetireTtlMs(0));
  }

  @Nested
  class SmallSegments extends SharedLogContract {
    @Override
    protected SharedLog newLog() {
      return log(3);
    }
  }

  @Nested
  class StoreLimit extends SharedLogContract {
    @Override
    protected SharedLog newLog() {
      return log(0);
    }
  }
}
