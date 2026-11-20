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
package org.intentlog.memory;

import org.intentlog.SharedLog;
import org.intentlog.testkit.SharedLogContract;
import org.junit.jupiter.api.Nested;

/** The SharedLog contract on the in-memory log, with appends unconditional and conditional. */
class TestInMemorySharedLogContract {

  @Nested
  class Unconditional extends SharedLogContract {
    @Override
    protected SharedLog newLog() {
      return new InMemorySharedLog("log");
    }

    @Override
    protected boolean refuseNextWrite(Refusal refusal) {
      return notSentNext((InMemorySharedLog) log, refusal);
    }
  }

  @Nested
  class Conditional extends SharedLogContract {
    @Override
    protected SharedLog newLog() {
      return new InMemorySharedLog("log", true);
    }

    @Override
    protected boolean appendsUnconditionally() {
      return false;
    }

    @Override
    protected boolean refuseNextWrite(Refusal refusal) {
      return notSentNext((InMemorySharedLog) log, refusal);
    }
  }

  /** The log's next append is not sent; it has no way to be full or refused. */
  private static boolean notSentNext(InMemorySharedLog log, SharedLogContract.Refusal refusal) {
    if (refusal != SharedLogContract.Refusal.NOT_SENT) {
      return false;
    }
    boolean[] once = {true};
    log.intercept(new InMemorySharedLog.Interceptor() {
      @Override
      public InMemorySharedLog.WriteFault beforeWrite(
          InMemorySharedLog.Request request, byte[] payload) {
        if (request == InMemorySharedLog.Request.APPEND && once[0]) {
          once[0] = false;
          return InMemorySharedLog.WriteFault.NOT_SENT;
        }
        return InMemorySharedLog.WriteFault.NONE;
      }
    });
    return true;
  }
}
