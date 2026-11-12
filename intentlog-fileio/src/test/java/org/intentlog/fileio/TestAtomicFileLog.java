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

import java.io.ByteArrayOutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32C;
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

  private static final long INCARNATION = 0x0123456789ABCDEFL;

  private final InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
  private final AtomicFileLog log = new AtomicFileLog(io, LOCATION, () -> INCARNATION);

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
  void theLargestRecordIsTheStoresAppendLimitLessItsLengthPrefixAndTrailer() { // (Bug 71)
    assertThat(log.capabilities().maxRecordBytes()).isZero();
    io.limitAppendBytes(300);
    AtomicFileLog limited = new AtomicFileLog(io, LOCATION);
    assertThat(limited.capabilities().maxRecordBytes()).isEqualTo(278); // a 2-byte varint, 20
    LogView created = create();

    LogView appended =
        limited.readThrough(limited.append(new byte[278], created.tail()), Duration.ZERO);
    assertThat(appended.records()).hasSize(2);
    assertThatThrownBy(() -> limited.append(new byte[279], appended.tail()))
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
    byte[] replaced = AtomicFileLog.encode(INCARNATION, bytes("chk"), List.of(bytes("r1")));
    assertThat(replaced).isEqualTo(io.history().get(0).after());
    assertThat(AtomicFileLog.extend(replaced, bytes("r2"))).isEqualTo(object);
  }

  /** Format 3, byte for byte: each frame's length, body and trailer. */
  @Test
  void eachFrameEndsInATrailerNamingItsObjectAndOffset() {
    byte[] object = AtomicFileLog.encode(INCARNATION, bytes("chk"), List.of(bytes("r1")));
    ByteBuffer in = ByteBuffer.wrap(object).order(ByteOrder.LITTLE_ENDIAN);
    assertThat(Arrays.copyOf(object, 4)).isEqualTo(AtomicFileLog.MAGIC);
    assertThat(ByteBuffer.wrap(object, 4, 4).getInt()).isEqualTo(3);
    // the checkpoint's frame at 8: length 3, "chk", then its trailer
    assertThat(object[8]).isEqualTo((byte) 3);
    assertThat(string(Arrays.copyOfRange(object, 9, 12))).isEqualTo("chk");
    assertThat(in.getLong(12)).isEqualTo(INCARNATION);
    assertThat(in.getLong(20)).isEqualTo(8);
    CRC32C crc = new CRC32C();
    crc.update(object, 8, 20);
    assertThat(in.getInt(28)).isEqualTo((int) crc.getValue());
    // the record's frame at 32
    assertThat(object[32]).isEqualTo((byte) 2);
    assertThat(in.getLong(35)).isEqualTo(INCARNATION);
    assertThat(in.getLong(43)).isEqualTo(32);
    assertThat(object).hasSize(55);
  }

  /** Two objects of one log: a replace's, then another replace's of a different incarnation. */
  private static byte[][] twoIncarnations() {
    byte[] x = AtomicFileLog.encode(1L, bytes("chk"), List.of(bytes("r1"), bytes("r2")));
    byte[] y =
        AtomicFileLog.encode(2L, bytes("c2!"), List.of(bytes("r3"), bytes("r4"), bytes("r5")));
    return new byte[][] {x, y};
  }

  /** {@code a}'s first {@code at} bytes, then {@code b}'s from {@code at}, to {@code length}. */
  private static byte[] splice(byte[] a, byte[] b, int at, int length) {
    byte[] spliced = Arrays.copyOf(a, length);
    System.arraycopy(b, at, spliced, at, length - at);
    return spliced;
  }

  @Test
  void aSpliceAtAFrameBoundaryFailsTheIncarnationCheck() { // S5
    byte[][] objects = twoIncarnations();
    int boundary = 32; // the checkpoints' frames are the same length: the first records start here
    byte[] spliced = splice(objects[0], objects[1], boundary, objects[1].length);
    assertThatThrownBy(() -> AtomicFileLog.decode(spliced))
        .isInstanceOf(AtomicFileLog.FrameCheckException.class)
        .hasMessageContaining("incarnation");
    // Format 2's frames named nothing: the same splice decoded
    assertThat(Arrays.copyOfRange(objects[0], 0, boundary))
        .isNotEqualTo(Arrays.copyOfRange(objects[1], 0, boundary));
  }

  /**
   * Whatever a splice of two objects at any byte decodes to is a state the log held: a prefix of
   * one of them, ending at one of its frames' ends. Every other splice fails a check.
   */
  @Test
  void aSpliceAnywhereDecodesOnlyToAPrefixOfOneObject() { // S5
    byte[][] objects = twoIncarnations();
    int checked = 0;
    for (int at = AtomicFileLog.HEADER_SIZE; at < objects[0].length; at++) {
      for (int length = at + 1; length <= objects[1].length; length++) {
        byte[] spliced = splice(objects[0], objects[1], at, length);
        LogView view;
        try {
          view = AtomicFileLog.decode(spliced);
        } catch (AtomicFileLog.FrameCheckException e) {
          checked++;
          continue;
        }
        assertThat(isPrefixOf(view, objects[0]) || isPrefixOf(view, objects[1]))
            .as("splice at %d, %d bytes", at, length).isTrue();
      }
    }
    assertThat(checked).isPositive();
  }

  private static boolean isPrefixOf(LogView view, byte[] object) {
    LogView whole = AtomicFileLog.decode(object);
    if (!Arrays.equals(view.checkpoint().orElseThrow(), whole.checkpoint().orElseThrow())
        || view.records().size() > whole.records().size()) {
      return false;
    }
    for (int i = 0; i < view.records().size(); i++) {
      if (!Arrays.equals(view.records().get(i), whole.records().get(i))) {
        return false;
      }
    }
    return true;
  }

  @Test
  void aTruncatedObjectFailsTheChecks() {
    byte[] object = twoIncarnations()[0];
    for (int length = AtomicFileLog.HEADER_SIZE; length < object.length; length++) {
      byte[] truncated = Arrays.copyOf(object, length);
      boolean boundary = length == 32 || length == 55;
      if (boundary) {
        assertThat(AtomicFileLog.decode(truncated).records()).hasSize(length == 32 ? 0 : 1);
      } else {
        assertThatThrownBy(() -> AtomicFileLog.decode(truncated))
            .as("truncated to %d", length)
            .isInstanceOf(AtomicFileLog.FrameCheckException.class);
      }
    }
  }

  @Test
  void aFrameMovedFromWhereItWasWrittenFailsTheOffsetCheck() {
    // The same object's first record, framed for offset 32, copied in again at its end
    byte[] object = twoIncarnations()[0];
    byte[] moved = Arrays.copyOf(object, object.length + 23);
    System.arraycopy(object, 32, moved, object.length, 23);
    assertThatThrownBy(() -> AtomicFileLog.decode(moved))
        .isInstanceOf(AtomicFileLog.FrameCheckException.class)
        .hasMessageContaining("says it starts at 32");
  }

  @Test
  void formatTwoStillDecodesForTools() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.writeBytes(AtomicFileLog.MAGIC);
    out.writeBytes(new byte[] {0, 0, 0, 2, 3, 'c', 'h', 'k', 2, 'r', '1'});
    LogView decoded = AtomicFileLog.decode(out.toByteArray());
    assertThat(string(decoded.checkpoint().orElseThrow())).isEqualTo("chk");
    assertThat(decoded.records()).extracting(TestAtomicFileLog::string).containsExactly("r1");
  }

  /**
   * A read torn by a write that landed between its lookup and its GET, on a store that does not pin
   * reads, fails the frames' checks; the log reads again and finds the write's object.
   */
  @Test
  void aReadThatFailsTheChecksReadsAgain() { // T6, S5
    for (InMemoryAtomicFileIO.ReadMode mode : List.of(
        InMemoryAtomicFileIO.ReadMode.LIVE, InMemoryAtomicFileIO.ReadMode.SPLICE)) {
      InMemoryAtomicFileIO store = new InMemoryAtomicFileIO();
      AtomicInteger next = new AtomicInteger();
      AtomicFileLog writer = new AtomicFileLog(store, LOCATION, () -> next.incrementAndGet());
      LogView created = writer.readThrough(writer.replace(AtomicFileLog.tailOf(
          store.newInputFile(LOCATION)), bytes("checkpoint"), List.of(bytes("r1"), bytes("r2"))),
          Duration.ZERO);
      store.readMode(mode);
      store.spliceAt((bound, live) -> 20); // inside the checkpoint's frame
      // Each read's GET (its second request) first replaces the object with a shorter checkpoint
      // and more records, as long, until two have
      AtomicInteger requests = new AtomicInteger();
      AtomicInteger replaces = new AtomicInteger(2);
      LogView[] last = {created};
      store.injectReadFaults(location -> {
        if (requests.incrementAndGet() % 2 == 0 && replaces.getAndDecrement() > 0) {
          store.injectReadFaults(l -> InMemoryAtomicFileIO.ReadFault.NONE);
          last[0] = writer.readThrough(writer.replace(last[0].tail(), bytes("cp"),
              List.of(bytes("r1"), bytes("r2"), bytes("r3x"))), Duration.ZERO);
          store.injectReadFaults(this::noFault);
        }
        return InMemoryAtomicFileIO.ReadFault.NONE;
      });
      LogView read = writer.read();
      assertThat(store.served(mode)).as(mode.name()).isPositive();
      assertSameView(read, last[0]);
    }
  }

  private InMemoryAtomicFileIO.ReadFault noFault(String location) {
    return InMemoryAtomicFileIO.ReadFault.NONE;
  }

  /** A store that serves every read torn fails the read, after a bounded number. */
  @Test
  void aReadThatAlwaysFailsTheChecksFails() { // T6, S5
    create();
    io.readMode(InMemoryAtomicFileIO.ReadMode.SPLICE);
    io.spliceAt((bound, live) -> 20); // in the checkpoint's trailer
    AtomicInteger requests = new AtomicInteger();
    io.injectReadFaults(location -> {
      if (requests.incrementAndGet() % 2 == 0) {
        // between the lookup and the GET, a replace by another writer, of the same length
        io.seed(LOCATION, AtomicFileLog.encode(requests.get(), bytes("chk"), List.of(bytes("r9"))));
      }
      return InMemoryAtomicFileIO.ReadFault.NONE;
    });

    assertThatThrownBy(log::read).isInstanceOf(AtomicFileLog.FrameCheckException.class);
    assertThat(requests.get()).isEqualTo(2 * AtomicFileLog.READ_ATTEMPTS);
  }

  /**
   * An append from a probed tail frames its record as part of the version the probe found, whose
   * incarnation its last trailer gives: a replace since the log's own read made that another.
   */
  @Test
  void anAppendFromAProbedTailNamesTheVersionItExtends() {
    create();
    AtomicFileLog other = new AtomicFileLog(io, LOCATION, () -> 7L);
    other.replace(log.read().tail(), bytes("c2"), List.of(bytes("r2"), bytes("r3")));

    log.append(bytes("r4"), log.probe());
    byte[] object = io.bytes(LOCATION);
    assertThat(AtomicFileLog.decode(object).records()).extracting(TestAtomicFileLog::string)
        .containsExactly("r2", "r3", "r4");
    assertThat(ByteBuffer.wrap(object, object.length - 20, 8).order(ByteOrder.LITTLE_ENDIAN)
        .getLong()).isEqualTo(7L);
  }

  /** A probed version superseded before the append reads its trailer cannot take it: unsent. */
  @Test
  void anAppendFromAProbedTailThatIsGoneLosesTheRaceUnsent() {
    io.failSupersededReads(true);
    LogView created = create();
    LogTail probed = log.probe();
    log.append(bytes("r2"), created.tail());
    int writes = io.history().size();

    assertThatThrownBy(() -> log.append(bytes("r3"), probed))
        .isInstanceOf(LogConflictException.class);
    assertThat(io.history()).hasSize(writes);
  }

  /** Counts the read requests that reach the store from now on. */
  private AtomicInteger countReads() {
    return countReads(io);
  }

  private static AtomicInteger countReads(InMemoryAtomicFileIO store) {
    AtomicInteger reads = new AtomicInteger();
    store.injectReadFaults(location -> {
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

  // ============================================================
  // Tail reads (docs/TAIL_READS.md "Reads")
  // ============================================================

  /** Another writer's log over the same store, its replaces of incarnation {@code incarnation}. */
  private AtomicFileLog other(long incarnation) {
    return new AtomicFileLog(io, LOCATION, () -> incarnation);
  }

  @Test
  void aTailReadReadsTheRecordsAfterTheReplicaAndKeepsItsArrays() {
    LogView held = create();
    other(INCARNATION).append(bytes("r2"), log.read().tail());
    other(INCARNATION).append(bytes("r3"), log.read().tail());

    AtomicInteger reads = countReads();
    LogView after = log.readAfter(held.tail());
    assertThat(after.records()).extracting(TestAtomicFileLog::string)
        .containsExactly("r1", "r2", "r3");
    assertThat(after.checkpoint().orElseThrow()).isSameAs(held.checkpoint().orElseThrow());
    assertThat(after.records().get(0)).isSameAs(held.records().get(0));
    assertSameView(after, log.read());
    assertThat(after.tail().continues(held.tail())).isTrue();
    // the view's tail conditions an append, which needs no read back
    LogView appended = log.readThrough(log.append(bytes("r4"), after.tail()), Duration.ZERO);
    assertSameView(appended, log.read());
    assertThat(reads.get()).as("lookups and GETs, the checks' whole reads aside").isPositive();
  }

  @Test
  void aTailReadWithNothingNewIsTheReplica() {
    LogView held = create();
    LogView after = log.readAfter(held.tail());
    assertThat(after.records()).extracting(TestAtomicFileLog::string).containsExactly("r1");
    assertThat(after.tail().bytes()).isEqualTo(held.tail().bytes());
  }

  /**
   * A replace that made the object longer than the replica's tail, with a frame boundary at the
   * tail or not, is not the replica's object: the 20 bytes before the tail are not its trailer,
   * and the tail read reads the new object whole.
   */
  @Test
  void aReplaceLongerThanTheTailIsReadWhole() {
    LogView held = create();
    other(7L).replace(log.read().tail(), bytes("chk"),
        List.of(bytes("r1"), bytes("r2"), bytes("r3")));

    LogView after = log.readAfter(held.tail());
    assertSameView(after, log.read());
    assertThat(after.records()).extracting(TestAtomicFileLog::string)
        .containsExactly("r1", "r2", "r3");
    assertThat(after.tail().continues(held.tail())).as("another incarnation").isFalse();
  }

  /**
   * On a store without appends (S3 Standard, GCS) every change is a replace, so an object longer
   * than the replica's tail is read whole at once: a lookup and a GET, as a whole read. On a store
   * with appends the tail read first GETs the bytes from 20 before the tail, which a replace fails:
   * a GET more (each count here also holds the surefire check's read, intentlog.fileio.tail.check).
   */
  @Test
  void aLongerObjectOnAStoreWithoutAppendsIsReadWholeAtOnce() {
    Map<Boolean, Integer> requests = new HashMap<>();
    for (boolean appends : new boolean[] {true, false}) {
      InMemoryAtomicFileIO store = new InMemoryAtomicFileIO(appends);
      store.readMode(InMemoryAtomicFileIO.ReadMode.PINNED); // a lookup, then a GET, as S3's
      AtomicFileLog mine = new AtomicFileLog(store, LOCATION, () -> INCARNATION);
      LogTail absent = AtomicFileLog.tailOf(store.newInputFile(LOCATION));
      LogView held = mine.readThrough(
          mine.replace(absent, bytes("chk"), List.of(bytes("r1"))), Duration.ZERO);
      new AtomicFileLog(store, LOCATION, () -> 7L).replace(mine.read().tail(), bytes("chk"),
          List.of(bytes("r1"), bytes("r2"), bytes("r3")));

      AtomicInteger reads = countReads(store);
      LogView after = mine.readAfter(held.tail());
      requests.put(appends, reads.get());
      assertSameView(after, mine.read());
    }
    assertThat(requests.get(false)).as("read requests without appends")
        .isEqualTo(requests.get(true) - 1);
  }

  /**
   * Where the store's reads record the version they find (S3, ADLS), a read is one GET with no
   * lookup before it (TAIL_READS.md step 7): a whole read, and a tail read of the object extended;
   * a tail read of an object replaced by a longer one reads the rest of the version it found by a
   * second GET, and one replaced by an object shorter than the tail less 20 reads it whole after a
   * GET that found nothing. Elsewhere (GCS) a read looks the object up first. Without the suites'
   * check of each tail read against a whole read, whose read would count.
   */
  @Test
  void aReadIsOneRequestWhereTheStoresReadsRecordTheirVersion() {
    withoutTailReadCheck(() -> {
      for (boolean recording : new boolean[] {true, false}) {
        InMemoryAtomicFileIO store = new InMemoryAtomicFileIO();
        store.setReadsRecordVersion(recording);
        store.readMode(InMemoryAtomicFileIO.ReadMode.PINNED); // a stream's GET at its first read
        AtomicFileLog mine = new AtomicFileLog(store, LOCATION, () -> INCARNATION);
        AtomicFileLog other = new AtomicFileLog(store, LOCATION, () -> 7L);
        LogTail absent = AtomicFileLog.tailOf(store.newInputFile(LOCATION));
        LogView held = mine.readThrough(
            mine.replace(absent, bytes("chk"), List.of(bytes("r1"))), Duration.ZERO);
        other.append(bytes("r2"), mine.read().tail());
        String as = recording ? "reads recording their version" : "a lookup first";

        AtomicInteger reads = countReads(store);
        LogView whole = mine.read();
        assertThat(reads.getAndSet(0)).as("a whole read, " + as).isEqualTo(recording ? 1 : 2);
        LogView extended = mine.readAfter(held.tail());
        assertThat(reads.getAndSet(0)).as("a tail read, " + as).isEqualTo(recording ? 1 : 2);
        assertSameView(extended, whole);
        // a CAS from the view the tail read returned replaces the version it read
        LogView compacted = mine.readThrough(
            mine.replace(extended.tail(), bytes("chk2"), List.of()), Duration.ZERO);
        assertSameView(compacted, mine.read());

        LogView before = mine.read();
        other.replace(before.tail(), bytes("a longer checkpoint"),
            List.of(bytes("r3"), bytes("r4")));
        reads.set(0);
        LogView longer = mine.readAfter(before.tail());
        assertThat(reads.getAndSet(0)).as("a replace longer, " + as).isEqualTo(recording ? 2 : 3);
        assertSameView(longer, mine.read());
        assertThat(longer.records()).extracting(TestAtomicFileLog::string)
            .containsExactly("r3", "r4");

        LogView full = mine.read();
        other.replace(full.tail(), bytes("c"), List.of());
        reads.set(0);
        LogView shorter = mine.readAfter(full.tail());
        assertThat(reads.getAndSet(0)).as("a replace shorter, " + as).isEqualTo(2);
        assertThat(string(shorter.checkpoint().orElseThrow())).isEqualTo("c");
        assertThat(shorter.records()).isEmpty();
      }
    });
  }

  /** A page is written once: its read is one GET on any store, unless lookups are asked for. */
  @Test
  void aPageReadIsOneGet() {
    for (boolean lookupFirst : new boolean[] {false, true}) {
      InMemoryAtomicFileIO store = new InMemoryAtomicFileIO();
      store.readMode(InMemoryAtomicFileIO.ReadMode.PINNED);
      AtomicFileLog mine = new AtomicFileLog(store, LOCATION, lookupFirst);
      mine.pages().put("p0", bytes("page"));
      AtomicInteger reads = countReads(store);
      assertThat(mine.pages().get("p0")).hasValueSatisfying(
          page -> assertThat(string(page)).isEqualTo("page"));
      assertThat(reads.get()).as("lookup first: " + lookupFirst).isEqualTo(lookupFirst ? 2 : 1);
      assertThat(mine.pages().get("p1")).isEmpty();
    }
  }

  /** Runs {@code run} without the suites' check of each tail read against a whole read. */
  private static void withoutTailReadCheck(Runnable run) {
    String check = System.getProperty(AtomicFileLog.CHECK_TAIL_READS);
    System.setProperty(AtomicFileLog.CHECK_TAIL_READS, "false");
    try {
      run.run();
    } finally {
      if (check == null) {
        System.clearProperty(AtomicFileLog.CHECK_TAIL_READS);
      } else {
        System.setProperty(AtomicFileLog.CHECK_TAIL_READS, check);
      }
    }
  }

  @Test
  void aReplaceOfTheSameLengthIsReadWhole() {
    LogView held = create();
    other(7L).replace(log.read().tail(), bytes("chk"), List.of(bytes("r9")));
    assertThat(io.bytes(LOCATION)).hasSize((int) held.tail().bytes());

    LogView after = log.readAfter(held.tail());
    assertThat(after.records()).extracting(TestAtomicFileLog::string).containsExactly("r9");
  }

  @Test
  void aReplaceShorterThanTheTailIsReadWhole() {
    LogView held = create();
    other(INCARNATION).append(bytes("r2"), log.read().tail());
    LogView longer = log.read();
    other(7L).replace(longer.tail(), bytes("c"), List.of());

    LogView after = log.readAfter(longer.tail());
    assertThat(string(after.checkpoint().orElseThrow())).isEqualTo("c");
    assertThat(after.records()).isEmpty();
  }

  /** A tail a probe found holds no bytes this log read: a read after it reads whole. */
  @Test
  void aReadAfterAProbedTailIsWhole() {
    create();
    LogTail probed = log.probe();
    other(INCARNATION).append(bytes("r2"), log.read().tail());
    assertSameView(log.readAfter(probed), log.read());
  }

  /**
   * A tail read torn by a replace that landed between its lookup and its GET, on a store that
   * does not pin reads: spliced inside the replica's last frame or after it, or served short of
   * the length its lookup found. Each fails a check, and the read is read again, whole.
   */
  @Test
  void aTornTailReadReadsAgain() {
    for (InMemoryAtomicFileIO.ReadMode mode : List.of(InMemoryAtomicFileIO.ReadMode.LIVE,
        InMemoryAtomicFileIO.ReadMode.SPLICE)) {
      for (int shorter = 0; shorter <= 1; shorter++) {
        InMemoryAtomicFileIO store = new InMemoryAtomicFileIO();
        AtomicFileLog reader = new AtomicFileLog(store, LOCATION, () -> 1L);
        AtomicFileLog writer = new AtomicFileLog(store, LOCATION, () -> 2L);
        LogView held = reader.readThrough(reader.replace(AtomicFileLog.tailOf(
            store.newInputFile(LOCATION)), bytes("checkpoint"), List.of(bytes("r1"))),
            Duration.ZERO);
        writer.append(bytes("r2"), writer.read().tail());
        store.readMode(mode);
        long at = held.tail().bytes() - 10; // inside the replica's last trailer
        store.spliceAt((bound, live) -> (int) at);
        List<byte[]> replacement = shorter == 1 ? List.of() : List.of(bytes("x1"), bytes("x2"));
        AtomicInteger requests = new AtomicInteger();
        store.injectReadFaults(location -> {
          if (requests.incrementAndGet() == 2) { // the first tail read's GET
            store.injectReadFaults(l -> InMemoryAtomicFileIO.ReadFault.NONE);
            writer.replace(writer.read().tail(), bytes("cp"), replacement);
          }
          return InMemoryAtomicFileIO.ReadFault.NONE;
        });
        LogView after = reader.readAfter(held.tail());
        assertThat(string(after.checkpoint().orElseThrow())).as("%s, shorter %d", mode, shorter)
            .isEqualTo("cp");
        assertThat(after.records()).hasSize(replacement.size());
      }
    }
  }
}
