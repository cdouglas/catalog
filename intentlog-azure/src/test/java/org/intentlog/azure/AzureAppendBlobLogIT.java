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

import java.util.UUID;
import org.intentlog.SharedLog;
import org.intentlog.testkit.SharedLogContract;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * The SharedLog contract on a real account ({@link CloudAccount}): blind appends in segments that
 * never close and in segments that close every few records (real pads), and conditional appends.
 * Run explicitly: {@code mvn -o -pl intentlog-azure test -Dtest='AzureAppendBlobLogIT*'}.
 */
@EnabledIfEnvironmentVariable(named = "AZURE_LOG_IT_CONTAINER_URL", matches = ".+")
class AzureAppendBlobLogIT {

  private static final String PREFIX = CloudAccount.RUN + "/contract";

  private static AppendBlobLog log(boolean conditional, long segmentLimit) {
    return new AppendBlobLog(CloudAccount.store(), PREFIX + "/" + UUID.randomUUID(),
        AppendBlobLog.Options.defaults().withConditional(conditional)
            .withSegmentLimit(segmentLimit).withTrimGraceMs(0));
  }

  @AfterAll
  static void deleteWhatItWrote() {
    if (CloudAccount.configured()) {
      System.out.println("deleted " + CloudAccount.deleteAll(PREFIX) + " blobs under " + PREFIX);
    }
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
