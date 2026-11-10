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

import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.InMemoryAtomicFileIO;
import org.apache.iceberg.io.SupportsAtomicOperations;
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
  void theLargestRecordIsTheStoresAppendLimitLessItsLengthPrefix() { // (Bug 71)
    assertThat(log.capabilities().maxRecordBytes()).isZero();
    io.limitAppendBytes(300);
    AtomicFileLog limited = new AtomicFileLog(io, LOCATION);
    assertThat(limited.capabilities().maxRecordBytes()).isEqualTo(298); // a 2-byte varint
    LogView created = create();

    LogView appended =
        limited.readThrough(limited.append(new byte[298], created.tail()), Duration.ZERO);
    assertThat(appended.records()).hasSize(2);
    assertThatThrownBy(() -> limited.append(new byte[299], appended.tail()))
        .isInstanceOf(UncheckedIOException.class);
  }

  @Test
  void aProbeOfAMissingObjectIsATailThatCreatesIt() {
    LogTail missing = log.probe();
    assertThat(missing.exists()).isFalse();
    log.replace(missing, bytes("chk"), List.of());
    assertThat(log.probe().exists()).isTrue();
    assertThatThrownBy(() -> log.replace(missing, bytes("c2"), List.of()))
        .isInstanceOf(LogConflictException.class);
  }

  @Test
  void aProbeThatCannotReadAnObjectThatExistsFails() {
    create();
    io.injectReadFaults(location -> InMemoryAtomicFileIO.ReadFault.FAIL);
    assertThatThrownBy(log::probe).isInstanceOf(UncheckedIOException.class);
  }

  /**
   * A read whose GET fails as stale (a write landed between the lookup that chose the version and
   * the GET) reads again, and finds the version that write made.
   */
  @Test
  void aReadThatFailsAsStaleReadsAgain() { // T6
    LogView created = create();
    log.append(bytes("r2"), created.tail());
    AtomicInteger stale = new AtomicInteger(2);
    io.injectReadFaults(location -> stale.getAndDecrement() > 0
        ? InMemoryAtomicFileIO.ReadFault.STALE
        : InMemoryAtomicFileIO.ReadFault.NONE);

    assertThat(log.read().records()).extracting(TestAtomicFileLog::string)
        .containsExactly("r1", "r2");
    assertThat(io.injected(InMemoryAtomicFileIO.ReadFault.STALE)).isEqualTo(2);
  }

  /** A store that never serves the version it reports fails the read, after a bounded number. */
  @Test
  void aReadThatIsAlwaysStaleFails() { // T6
    create();
    io.injectReadFaults(location -> InMemoryAtomicFileIO.ReadFault.STALE);

    assertThatThrownBy(log::read).isInstanceOf(SupportsAtomicOperations.StaleReadException.class);
    assertThat(io.injected(InMemoryAtomicFileIO.ReadFault.STALE))
        .isEqualTo(AtomicFileLog.READ_ATTEMPTS);
  }

  /**
   * A read through an append from a probed tail reads the version the append wrote; if a later
   * write superseded it, on a store that serves only the live version, it reads the live object,
   * whose records hold the append's.
   */
  @Test
  void aReadThroughAWriteALaterWriteSupersededReadsTheObject() { // T6, D20
    io.failSupersededReads(true);
    create();
    Receipt receipt = log.append(bytes("r2"), log.probe());
    log.append(bytes("r3"), log.read().tail());

    LogView through = log.readThrough(receipt, Duration.ZERO);
    assertThat(through.records()).extracting(TestAtomicFileLog::string)
        .containsExactly("r1", "r2", "r3");
    assertSameView(through, log.read());
  }

  @Test
  void theLayoutDecodesAsAReadFindsIt() {
    LogView created = create();
    log.append(bytes("r2"), created.tail());
    byte[] object = io.history().get(1).after();
    LogView decoded = AtomicFileLog.decode(object);
    assertThat(string(decoded.checkpoint().orElseThrow())).isEqualTo("chk");
    assertThat(decoded.records()).extracting(TestAtomicFileLog::string).containsExactly("r1", "r2");
    assertThat(decoded.tail().bytes()).isEqualTo(object.length);
    assertThat(AtomicFileLog.encode(bytes("chk"), List.of(bytes("r1"))))
        .isEqualTo(io.history().get(0).after());
  }

  /** Counts the read requests that reach the store from now on. */
  private AtomicInteger countReads() {
    AtomicInteger reads = new AtomicInteger();
    io.injectReadFaults(location -> {
      reads.incrementAndGet();
      return InMemoryAtomicFileIO.ReadFault.NONE;
    });
    return reads;
  }

  private static void assertSameView(LogView actual, LogView expected) {
    assertThat(actual.checkpoint().orElseThrow()).isEqualTo(expected.checkpoint().orElseThrow());
    assertThat(actual.records()).containsExactlyElementsOf(expected.records());
    assertThat(actual.tail().bytes()).isEqualTo(expected.tail().bytes());
  }

  @Test
  void aReadThroughAWriteThatSaysWhatTheObjectHoldsReadsNothing() { // D20
    LogTail absent = absent(); // a lookup
    AtomicInteger reads = countReads();
    LogView created =
        log.readThrough(log.replace(absent, bytes("chk"), List.of(bytes("r1"))), Duration.ZERO);
    LogView appended = log.readThrough(log.append(bytes("r2"), created.tail()), Duration.ZERO);
    LogView again = log.readThrough(log.append(bytes("r3"), appended.tail()), Duration.ZERO);
    LogView replaced =
        log.readThrough(log.replace(again.tail(), bytes("c2"), List.of(bytes("r4"))), Duration.ZERO);
    assertThat(reads).hasValue(0);

    assertThat(again.records()).extracting(TestAtomicFileLog::string)
        .containsExactly("r1", "r2", "r3");
    assertThat(string(replaced.checkpoint().orElseThrow())).isEqualTo("c2");
    // Each view is the object its write left, and its tail conditions the next write on it
    LogView fresh = log.read();
    assertSameView(replaced, fresh);
    log.append(bytes("r5"), replaced.tail());
    assertThat(log.read().records()).extracting(TestAtomicFileLog::string)
        .containsExactly("r4", "r5");
  }

  @Test
  void aReadThroughAnAppendAtAProbedTailReadsTheObject() { // D20
    LogView created = create();
    log.append(bytes("r2"), created.tail());
    AtomicInteger reads = countReads();
    LogTail probed = log.probe();
    LogView appended = log.readThrough(log.append(bytes("r3"), probed), Duration.ZERO);
    assertThat(reads.get()).isGreaterThan(1);
    assertThat(appended.records()).extracting(TestAtomicFileLog::string)
        .containsExactly("r1", "r2", "r3");
    assertSameView(appended, log.read());
  }

  @Test
  void aTailAWriteMadeExistsAfterALaterWrite() { // D20
    // A store that looks a generation up (GCS) finds none once a later write replaced it; the
    // tail saw the object, and a write from it conflicts rather than creating the log
    io.failSupersededReads(true);
    LogView created = create();
    LogView appended = log.readThrough(log.append(bytes("r2"), created.tail()), Duration.ZERO);
    LogView read = log.read();
    log.replace(log.read().tail(), bytes("c2"), List.of());
    assertThat(created.tail().exists()).isTrue();
    assertThat(appended.tail().exists()).isTrue();
    assertThat(read.tail().exists()).isTrue();
    assertThatThrownBy(() -> log.append(bytes("r3"), appended.tail()))
        .isInstanceOf(LogConflictException.class);
  }

  @Test
  void anAppendFromAReadThatLostTheRaceKnowsNothing() { // D20
    LogView created = create();
    LogView read = log.read();
    log.append(bytes("r2"), created.tail());
    assertThatThrownBy(() -> log.append(bytes("r3"), read.tail()))
        .isInstanceOf(LogConflictException.class);
    assertThat(log.read().records()).extracting(TestAtomicFileLog::string)
        .containsExactly("r1", "r2");
  }

  @Test
  void aStoreWithoutAppendSaysSo() {
    assertThat(new AtomicFileLog(new InMemoryAtomicFileIO(false), LOCATION).capabilities().append())
        .isFalse();
    assertThat(log.capabilities().append()).isTrue();
    assertThat(log.capabilities().fusedReplace()).isTrue();
  }
}
