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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.intentlog.CommittedSet;
import org.intentlog.Replica;
import org.intentlog.wire.UuidV7;
import org.junit.jupiter.api.Test;

/**
 * The committed set through a catalog file's checkpoint. The set's own round-trip and
 * defensive-decode tests are intentlog-core's.
 */
class TestCommittedTransactions {

  @Test
  void fullCheckpointRoundTripWithCommittedSet() {
    // End-to-end: a catalog file whose checkpoint holds committed txns, encoded through the
    // log's envelope with the catalog's state (not just the inner sub-message), decoded, and the
    // sets compared. This exercises the wire-level integration.
    Random rand = new Random(0xFEEDFACE);
    long base = System.currentTimeMillis();
    Set<UUID> txns = new LinkedHashSet<>();
    CommittedSet.Builder committed = CommittedSet.builder();
    for (int i = 0; i < 25; i++) {
      UUID id = UuidV7.newUuidV7(base + i * 31, rand);
      txns.add(id);
      committed.add(id);
    }
    UUID logId = UuidV7.newUuidV7(base, rand);
    byte[] file =
        CatalogTestSupport.encodeFullFile(logId, committed.build(), CatalogState.empty());

    Replica<CatalogState> decoded = CatalogTestSupport.replay(file);
    assertThat(decoded.logId()).isEqualTo(logId);
    assertThat(new HashSet<>(decoded.committed().ids())).isEqualTo(txns);
  }
}
