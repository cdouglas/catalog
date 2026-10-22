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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.InMemoryAtomicFileIO;
import org.intentlog.LogConflictException;
import org.intentlog.LogFullException;
import org.intentlog.LogTail;
import org.intentlog.LogThrottledException;
import org.intentlog.LogView;
import org.intentlog.Receipt;
import org.junit.jupiter.api.Test;

/** The atomic-file log: its layout, and the store's signals mapped onto the log's. */
class TestAtomicFileLog {

  private static final String LOCATION = "mem:///warehouse/catalog";

  private final InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
  private final AtomicFileLog log = new AtomicFileLog(io, LOCATION);

  private static byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static String string(byte[] b) {
    return new String(b, StandardCharsets.UTF_8);
  }

  /** The tail of the object before it exists: what a replace that creates it names. */
  private LogTail absent() {
    LogTail tail = AtomicFileLog.tailOf(io.newInputFile(LOCATION));
    assertThat(tail.exists()).isFalse();
    return tail;
  }

  private LogView create() {
    Receipt r = log.replace(absent(), bytes("chk"), List.of(bytes("r1")));
    return log.readThrough(r, Duration.ZERO);
  }

  @Test
  void aReplaceCreatesTheObjectAndAppendsFollowIt() {
    LogView created = create();
    assertThat(string(created.checkpoint().orElseThrow())).isEqualTo("chk");
    assertThat(created.records()).extracting(TestAtomicFileLog::string).containsExactly("r1");

    LogView appended = log.readThrough(log.append(bytes("r2"), created.tail()), Duration.ZERO);
    assertThat(appended.records()).extracting(TestAtomicFileLog::string)
        .containsExactly("r1", "r2");
    assertThat(appended.tail().continues(created.tail())).isTrue();
    assertThat(log.read().records()).hasSize(2);
    assertThat(io.conditionalWrites().get(LOCATION))
        .containsExactly(AtomicOutputFile.Strategy.CAS, AtomicOutputFile.Strategy.APPEND);
  }

  @Test
  void aReplaceStartsANewLineage() {
    LogView created = create();
    LogView replaced =
        log.readThrough(log.replace(created.tail(), bytes("c2"), List.of()), Duration.ZERO);
    assertThat(replaced.records()).isEmpty();
    assertThat(log.probe().continues(created.tail())).isFalse();
  }

  @Test
  void writesConditionedOnAStaleTailConflict() {
    LogView created = create();
    log.append(bytes("r2"), created.tail());
    assertThatThrownBy(() -> log.append(bytes("r3"), created.tail()))
        .isInstanceOf(LogConflictException.class);
    assertThatThrownBy(() -> log.replace(created.tail(), bytes("c2"), List.of()))
        .isInstanceOf(LogConflictException.class);
  }

  @Test
  void aCreateOfAnObjectThatExistsConflicts() {
    LogTail before = absent();
    create();
    assertThatThrownBy(() -> log.replace(before, bytes("c"), List.of()))
        .isInstanceOf(LogConflictException.class);
  }

  @Test
  void aThrottleIsAThrottleNotAConflict() {
    LogView created = create();
    io.injectFaults((location, strategy) -> InMemoryAtomicFileIO.Fault.THROTTLED);
    assertThatThrownBy(() -> log.append(bytes("r2"), created.tail()))
        .isInstanceOf(LogThrottledException.class);
  }

  @Test
  void anObjectAtItsSegmentLimitIsFull() {
    io.limitSegments(2);
    LogView created = create();
    LogView appended = log.readThrough(log.append(bytes("r2"), created.tail()), Duration.ZERO);
    assertThatThrownBy(() -> log.append(bytes("r3"), appended.tail()))
        .isInstanceOf(LogFullException.class);
  }

  @Test
  void aStoreWithoutAppendSaysSo() {
    assertThat(new AtomicFileLog(new InMemoryAtomicFileIO(false), LOCATION).capabilities().append())
        .isFalse();
    assertThat(log.capabilities().append()).isTrue();
    assertThat(log.capabilities().fusedReplace()).isTrue();
  }
}
