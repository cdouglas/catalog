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
package org.intentlog.azure;

import org.intentlog.SharedLog;
import org.intentlog.testkit.SharedLogContract;
import org.junit.jupiter.api.Nested;

/**
 * The SharedLog contract on the Azure log over the in-memory store, with blind and conditional
 * appends, in segments large enough never to move on and small enough to move on every few
 * records.
 */
class TestAppendBlobLogContract {

  private static AppendBlobLog log(boolean conditional, long segmentLimit) {
    return new AppendBlobLog(new InMemoryAppendBlobStore(), "catalog.log",
        AppendBlobLog.Options.defaults().withConditional(conditional)
            .withSegmentLimit(segmentLimit).withTrimGraceMs(0));
  }

  @Nested
  class Blind extends SharedLogContract {
    @Override
    protected SharedLog newLog() {
      return log(false, 1 << 20);
    }
  }

  @Nested
  class BlindSmallSegments extends SharedLogContract {
    @Override
    protected SharedLog newLog() {
      return log(false, 200);
    }
  }

  @Nested
  class Conditional extends SharedLogContract {
    @Override
    protected SharedLog newLog() {
      return log(true, 1 << 20);
    }

    @Override
    protected boolean appendsUnconditionally() {
      return false;
    }
  }
}
