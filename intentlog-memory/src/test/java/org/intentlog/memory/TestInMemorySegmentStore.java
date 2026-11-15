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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.UncheckedIOException;
import org.intentlog.memory.InMemorySegmentStore.Fault;
import org.intentlog.memory.InMemorySegmentStore.Op;
import org.intentlog.segments.SegmentStore;
import org.junit.jupiter.api.Test;

/** {@link InMemorySegmentStore}: blind appends, its limits, and its faults. */
class TestInMemorySegmentStore {

  @Test
  void appendsReturnTheirOffsetAndIndex() {
    InMemorySegmentStore store = new InMemorySegmentStore(3, 100, 1_000);
    assertThat(store.create("s")).isTrue();
    assertThat(store.create("s")).isFalse();
    assertThat(store.append("s", new byte[4])).isEqualTo(new SegmentStore.Appended(0, 1));
    assertThat(store.append("s", new byte[6])).isEqualTo(new SegmentStore.Appended(4, 2));
    SegmentStore.Segment read = store.read("s", 4);
    assertThat(read.bytes()).hasSize(6);
    assertThat(read.length()).isEqualTo(10);
    assertThat(read.blocks()).isEqualTo(2);
    assertThat(store.read("s", 10).bytes()).isEmpty();
    assertThat(store.read("t", 0)).isNull();
  }

  @Test
  void theBlockAndSizeLimitsRefuseAppends() {
    InMemorySegmentStore store = new InMemorySegmentStore(2, 100, 12);
    store.create("s");
    store.append("s", new byte[5]);
    assertThatThrownBy(() -> store.append("s", new byte[8])).isInstanceOf(SegmentStore.Full.class);
    store.append("s", new byte[7]);
    assertThatThrownBy(() -> store.append("s", new byte[1])).isInstanceOf(SegmentStore.Full.class);
    assertThatThrownBy(() -> store.append("t", new byte[1]))
        .isInstanceOf(SegmentStore.NotFound.class);
    assertThatThrownBy(() -> new InMemorySegmentStore(2, 4, 100).append("s", new byte[5]))
        .isInstanceOf(SegmentStore.NotFound.class);
  }

  @Test
  void faultsLandOrNotAsTheySay() {
    InMemorySegmentStore store = new InMemorySegmentStore(10, 100, 1_000);
    store.create("s");
    Fault[] next = {Fault.LOST_REPLY};
    store.intercept((op, name) -> op == Op.APPEND ? next[0] : Fault.NONE);
    assertThatThrownBy(() -> store.append("s", new byte[1]))
        .isInstanceOf(UncheckedIOException.class);
    next[0] = Fault.LANDED_THROTTLED;
    assertThatThrownBy(() -> store.append("s", new byte[1]))
        .isInstanceOf(SegmentStore.Throttled.class);
    next[0] = Fault.THROTTLED;
    assertThatThrownBy(() -> store.append("s", new byte[1]))
        .isInstanceOf(SegmentStore.Throttled.class);
    next[0] = Fault.REFUSED;
    assertThatThrownBy(() -> store.append("s", new byte[1]))
        .isInstanceOf(SegmentStore.Refused.class);
    next[0] = Fault.DELAYED;
    assertThatThrownBy(() -> store.append("s", new byte[1]))
        .isInstanceOf(UncheckedIOException.class);
    assertThat(store.appends()).hasSize(2);
    assertThat(store.delayedCount()).isEqualTo(1);
    assertThat(store.deliverDelayed()).isEqualTo(1);
    assertThat(store.appends()).hasSize(3);
  }

  @Test
  void aShortReadLeavesOutTheLastBlock() {
    InMemorySegmentStore store = new InMemorySegmentStore(10, 100, 1_000);
    store.create("s");
    store.append("s", new byte[3]);
    store.append("s", new byte[4]);
    store.intercept((op, name) -> op == Op.READ ? Fault.SHORT_READ : Fault.NONE);
    SegmentStore.Segment read = store.read("s", 0);
    assertThat(read.length()).isEqualTo(3);
    assertThat(read.blocks()).isEqualTo(1);
    store.intercept((op, name) -> op == Op.READ ? Fault.MISSING : Fault.NONE);
    assertThat(store.read("s", 0)).isNull();
  }
}
