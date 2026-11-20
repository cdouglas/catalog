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
import org.intentlog.testkit.SharedLogContract;
import org.junit.jupiter.api.Nested;

/** The SharedLog contract on one object of a store, with offset appends and CAS only. */
class TestAtomicFileLogContract {

  @Nested
  class WithAppend extends SharedLogContract {
    private InMemoryAtomicFileIO io;

    @Override
    protected SharedLog newLog() {
      io = new InMemoryAtomicFileIO();
      return new AtomicFileLog(io, "mem:///log");
    }

    @Override
    protected long pageWrites() {
      return io.writesLanded(); // only page puts write while a page case runs
    }

    /**
     * Full: the object holds as many writes as it has (the store's cap, as S3 Express's parts).
     * Refused: the store's client error ({@code StorageRefusedException}).
     */
    @Override
    protected boolean refuseNextWrite(Refusal refusal) {
      switch (refusal) {
        case FULL:
          io.limitSegments(io.segments("mem:///log"));
          return true;
        case REFUSED:
          boolean[] once = {true};
          io.injectFaults((location, strategy) -> {
            if (once[0]) {
              once[0] = false;
              return InMemoryAtomicFileIO.Fault.REFUSED;
            }
            return InMemoryAtomicFileIO.Fault.NONE;
          });
          return true;
        default:
          return false;
      }
    }
  }

  @Nested
  class CasOnly extends SharedLogContract {
    private InMemoryAtomicFileIO io;

    @Override
    protected SharedLog newLog() {
      io = new InMemoryAtomicFileIO(false);
      return new AtomicFileLog(io, "mem:///log");
    }

    @Override
    protected long pageWrites() {
      return io.writesLanded(); // only page puts write while a page case runs
    }
  }
}
