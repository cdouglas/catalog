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
package org.intentlog.testkit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.intentlog.Capabilities;
import org.intentlog.LogConflictException;
import org.intentlog.LogTail;
import org.intentlog.LogView;
import org.intentlog.Receipt;
import org.intentlog.SharedLog;
import org.intentlog.TrimmedException;
import org.intentlog.wire.LogEnvelope;
import org.intentlog.wire.UuidV7;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * What every {@link SharedLog} provider must do (P1-P6, I11), through the SPI alone. A provider's
 * tests extend it with {@link #newLog}; the tests of what a provider cannot do (by its {@link
 * Capabilities}) are skipped.
 */
public abstract class SharedLogContract {

  private static final Duration WAIT = Duration.ofSeconds(1);
  private static final UUID LOG_ID = UuidV7.newUuidV7();

  protected SharedLog log;
  private int records = 0;

  /** A log that does not exist yet. */
  protected abstract SharedLog newLog();

  @BeforeEach
  void setUp() {
    log = newLog();
  }

  /** A checkpoint (opaque to the log); {@code n} tells checkpoints apart. */
  protected static byte[] checkpoint(int n) {
    return LogEnvelope.encodeCheckpoint(LOG_ID, new byte[0],
        ("state-" + n).getBytes(StandardCharsets.UTF_8));
  }

  /** A new intention record. */
  protected byte[] record() {
    return LogEnvelope.encodeRecord(UuidV7.newUuidV7(),
        ("intention-" + records++).getBytes(StandardCharsets.UTF_8));
  }

  /** Creates the log with {@code checkpoint}, as the runtime does by what the log can do. */
  protected void create(byte[] checkpoint) {
    LogTail absent = log.probe();
    if (log.capabilities().fusedReplace()) {
      log.replace(absent, checkpoint, List.of());
    } else {
      log.putCheckpoint(absent, checkpoint);
    }
  }

  /** Appends {@code record}, at the tail a probe finds if the log appends only conditionally. */
  protected Receipt append(byte[] record) {
    Capabilities caps = log.capabilities();
    return log.append(record, caps.conditionalAppend() && !appendsUnconditionally()
        ? log.probe() : null);
  }

  /** Whether the log takes an append without an expected tail. */
  protected boolean appendsUnconditionally() {
    return !log.capabilities().fusedReplace();
  }

  private void assumeAppends() {
    assumeThat(log.capabilities().append()).as("the log appends").isTrue();
  }

  private void assumeSeparateCheckpoints() {
    assumeThat(log.capabilities().separateCheckpoints())
        .as("the log keeps checkpoints apart").isTrue();
  }

  @Test
  void aNewLogDoesNotExistUntilCreated() {
    assertThat(log.probe().exists()).isFalse();
    byte[] first = checkpoint(0);
    create(first);
    LogTail tail = log.probe();
    assertThat(tail.exists()).isTrue();
    LogView view = log.read();
    assertThat(view.checkpoint()).hasValueSatisfying(c -> assertThat(c).isEqualTo(first));
    assertThat(view.records()).isEmpty();
    assertThat(view.tail().exists()).isTrue();
  }

  @Test
  void aSecondCreateWithAnotherCheckpointFails() {
    LogTail absent = log.probe();
    create(checkpoint(0));
    assertThatThrownBy(() -> {
      if (log.capabilities().fusedReplace()) {
        log.replace(absent, checkpoint(1), List.of());
      } else {
        log.putCheckpoint(absent, checkpoint(1));
      }
    }).isInstanceOf(LogConflictException.class);
    assertThat(log.read().checkpoint().orElseThrow()).isEqualTo(checkpoint(0));
  }

  @Test
  void readsReturnACheckpointAndTheRecordsAfterItInOrder() { // P1
    assumeAppends();
    create(checkpoint(0));
    List<byte[]> appended = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      byte[] record = record();
      append(record);
      appended.add(record);
    }
    assertThat(log.read().records()).containsExactlyElementsOf(appended);
  }

  @Test
  void aReadThroughAWriteHasItsRecordAmongItsRecords() { // P3
    assumeAppends();
    create(checkpoint(0));
    byte[] first = record();
    byte[] second = record();
    append(first);
    Receipt receipt = append(second);
    append(record());
    List<byte[]> records = log.readThrough(receipt, WAIT).records();
    assertThat(records).startsWith(first, second);
  }

  @Test
  void ofTwoConditionalAppendsOnOneTailOneLands() { // P4
    assumeAppends();
    assumeThat(log.capabilities().conditionalAppend()).as("conditional appends").isTrue();
    create(checkpoint(0));
    LogTail tail = log.probe();
    byte[] first = record();
    log.append(first, tail);
    assertThatThrownBy(() -> log.append(record(), tail))
        .isInstanceOf(LogConflictException.class);
    assertThat(log.read().records()).containsExactly(first);
  }

  @Test
  void ofTwoReplacesOfOneTailOneLands() { // P4
    assumeThat(log.capabilities().fusedReplace()).as("fused replace").isTrue();
    create(checkpoint(0));
    LogTail tail = log.probe();
    log.replace(tail, checkpoint(1), List.of());
    assertThatThrownBy(() -> log.replace(tail, checkpoint(2), List.of()))
        .isInstanceOf(LogConflictException.class);
    assertThat(log.read().checkpoint().orElseThrow()).isEqualTo(checkpoint(1));
  }

  @Test
  void aLaterTailContinuesAnEarlierOne() {
    assumeAppends();
    create(checkpoint(0));
    LogTail before = log.probe();
    append(record());
    LogTail after = log.probe();
    assertThat(after.continues(before)).isTrue();
    assertThat(after.bytes()).isGreaterThan(before.bytes());
  }

  @Test
  void aReadStartsFromACheckpointThatCoversTheTrim() { // P5
    assumeSeparateCheckpoints();
    create(checkpoint(0));
    byte[] first = record();
    Receipt firstReceipt = append(first);
    append(record());
    LogTail through = log.read().tail();
    log.putCheckpoint(through, checkpoint(1));
    log.trim(through);
    byte[] third = record();
    Receipt thirdReceipt = append(third);
    LogView view = log.read();
    assertThat(view.checkpoint().orElseThrow()).isEqualTo(checkpoint(1));
    assertThat(view.records()).containsExactly(third);
    assertThat(log.readThrough(thirdReceipt, WAIT).records()).containsExactly(third);
    // Every checkpoint left holds the first record: a read through it cannot include it.
    assertThatThrownBy(() -> log.readThrough(firstReceipt, WAIT))
        .isInstanceOf(TrimmedException.class);
  }

  @Test
  void aTrimNoCheckpointCoversIsRefused() { // P5
    assumeSeparateCheckpoints();
    create(checkpoint(0));
    byte[] first = record();
    append(first);
    LogTail through = log.read().tail();
    assertThatThrownBy(() -> log.trim(through)).isInstanceOf(IllegalStateException.class);
    assertThat(log.read().records()).containsExactly(first);
  }

  @Test
  void checkpointsAtOnePositionAreTheSame() { // I11
    assumeSeparateCheckpoints();
    create(checkpoint(0));
    append(record());
    LogTail through = log.read().tail();
    log.putCheckpoint(through, checkpoint(1));
    log.putCheckpoint(through, checkpoint(1)); // idempotent
    assertThatThrownBy(() -> log.putCheckpoint(through, checkpoint(2)))
        .isInstanceOf(IllegalStateException.class);
    assertThat(log.read().checkpoint().orElseThrow()).isEqualTo(checkpoint(1));
  }

  @Test
  void aReadAfterATailHasTheRecordsSince() {
    assumeAppends();
    create(checkpoint(0));
    append(record());
    LogTail since = log.read().tail();
    byte[] later = record();
    append(later);
    LogView after = log.readAfter(since);
    if (after.checkpoint().isPresent()) {
      assertThat(after.records()).endsWith(later); // a whole view, to replay afresh
    } else {
      assertThat(after.records()).containsExactly(later);
    }
  }

  @Test
  void aReadAfterATrimmedTailIsTrimmedOrWhole() {
    assumeSeparateCheckpoints();
    create(checkpoint(0));
    LogTail since = log.read().tail();
    append(record());
    LogTail through = log.read().tail();
    log.putCheckpoint(through, checkpoint(1));
    log.trim(through);
    try {
      assertThat(log.readAfter(since).checkpoint()).isPresent();
    } catch (TrimmedException expected) {
      // the records after since are gone
    }
  }
}
