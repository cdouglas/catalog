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

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.intentlog.wire.UuidV7;
import org.junit.jupiter.api.Test;

/**
 * The committed set through the checkpoint codec. The set's own round-trip and
 * defensive-decode tests are intentlog-core's.
 */
class TestCommittedTransactions {

  private static final InputFile LOC = new InputFile() {
    @Override public long getLength() { return 0; }
    @Override public SeekableInputStream newStream() { throw new UnsupportedOperationException(); }
    @Override public String location() { return "test://committed-txns"; }
    @Override public boolean exists() { return false; }
  };

  @Test
  void fullCheckpointRoundTripWithCommittedSet() throws IOException {
    // End-to-end: build a full CatalogState with committed txns, encode
    // through the checkpoint codec (not just the inner sub-message), decode,
    // verify set equality. This exercises the wire-level integration.
    Random rand = new Random(0xFEEDFACE);
    long base = System.currentTimeMillis();
    Set<UUID> txns = new LinkedHashSet<>();
    for (int i = 0; i < 25; i++) {
      txns.add(UuidV7.newUuidV7(base + i * 31, rand));
    }
    CatalogState.Builder b = CatalogState.builder(LOC)
        .setCatalogUuid(UuidV7.newUuidV7(base, rand))
        .setNextNamespaceId(1)
        .setNextTableId(1);
    for (UUID u : txns) {
      b.addCommittedTransaction(u);
    }
    CatalogState original = b.rebuildLookups().build();

    ProtoCatalogFormat.ProtoIdManager idManager = new ProtoCatalogFormat.ProtoIdManager();
    idManager.setGlobals(original.nextNamespaceId(), original.nextTableId());
    ProtoCatalogFormat.Mut mut = new ProtoCatalogFormat.Mut(original);
    byte[] checkpointBytes = ProtoCodec.encodeCheckpoint(original, mut, idManager);

    CatalogState.Builder decodedBuilder = CatalogState.builder(LOC);
    ProtoCodec.decodeCheckpoint(checkpointBytes, decodedBuilder);
    CatalogState decoded = decodedBuilder.build();

    assertThat(new HashSet<>(decoded.committedTransactions())).isEqualTo(txns);
  }
}
