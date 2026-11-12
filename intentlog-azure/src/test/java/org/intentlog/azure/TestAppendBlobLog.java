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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.intentlog.LogTail;
import org.intentlog.LogView;
import org.intentlog.Receipt;
import org.intentlog.azure.AzureLogHead.Segment;
import org.intentlog.azure.InMemoryAppendBlobStore.Fault;
import org.intentlog.azure.InMemoryAppendBlobStore.Op;
import org.intentlog.wire.LogEnvelope;
import org.intentlog.wire.UuidV7;
import org.junit.jupiter.api.Test;

/**
 * What the Azure log does that the SPI's contract doesn't reach: segments closed by a pad and by
 * the block limit, appends that arrive after a close, a trim that follows a rotation, and a writer
 * whose head is stale (AZURE_LOG.md "Rotation", "Compaction, trim and sweep"; AZ1, AZ4, P5).
 */
class TestAppendBlobLog {

  private static final String ROOT = "catalog.log";
  private static final Duration WAIT = Duration.ofSeconds(1);
  private static final UUID LOG_ID = UuidV7.newUuidV7();

  private final InMemoryAppendBlobStore store = new InMemoryAppendBlobStore(50_000, () -> 0L);
  private int n = 0;

  private AppendBlobLog log(long limit) {
    return new AppendBlobLog(store, ROOT, AppendBlobLog.Options.defaults().withSegmentLimit(limit)
        .withTrimGraceMs(0).withClock(() -> 0L));
  }

  /** What a store around {@link #store} does with a request: calls {@code through} or not. */
  private interface Around {
    Object call(Method method, Object[] args, Callable<Object> through) throws Exception;
  }

  /** {@link #store} with {@code around} deciding each request, under a log of segment limit L. */
  private AppendBlobLog around(long limit, Around around) {
    AppendBlobStore wrapped = (AppendBlobStore) Proxy.newProxyInstance(
        AppendBlobStore.class.getClassLoader(), new Class<?>[] {AppendBlobStore.class},
        (proxy, method, args) -> around.call(method, args, () -> {
          try {
            return method.invoke(store, args);
          } catch (InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException r) {
              throw r;
            }
            throw new AssertionError(e);
          }
        }));
    return new AppendBlobLog(wrapped, ROOT, AppendBlobLog.Options.defaults()
        .withSegmentLimit(limit).withTrimGraceMs(0).withClock(() -> 0L));
  }

  private static byte[] checkpoint(int i, LogTail through) {
    return LogEnvelope.encodeCheckpoint(LOG_ID, new byte[0],
        ("state-" + i).getBytes(StandardCharsets.UTF_8), through.position());
  }

  private byte[] record() {
    return LogEnvelope.encodeRecord(UuidV7.newUuidV7(),
        ("intention-" + n++).getBytes(StandardCharsets.UTF_8));
  }

  private AzureLogHead head() {
    return AzureLogHead.decode(store.peek(ROOT + "/head"));
  }

  private static void create(AppendBlobLog log) {
    LogTail absent = log.probe();
    log.putCheckpoint(absent, LogEnvelope.encodeCheckpoint(LOG_ID, new byte[0],
        "state-0".getBytes(StandardCharsets.UTF_8), absent.position()));
  }

  /** Appends until the head names {@code segments} segments; returns the records appended. */
  private List<byte[]> appendUntil(AppendBlobLog log, int segments) {
    List<byte[]> appended = new ArrayList<>();
    while (head().segments().size() < segments) {
      byte[] r = record();
      log.append(r, null);
      appended.add(r);
    }
    return appended;
  }

  @Test
  void aFullSegmentClosesAtItsLimitAndTheHeadRecordsWhere() { // AZ1
    AppendBlobLog log = log(300);
    create(log);
    List<byte[]> appended = appendUntil(log, 4);
    AzureLogHead head = head();
    for (Segment s : head.segments().subList(0, 3)) {
      byte[] bytes = store.peek(s.name());
      assertThat(s.closedLength()).isEqualTo(s.limit()).isEqualTo(bytes.length);
      List<Frames.Frame> frames = Frames.parse(bytes, bytes.length, 0, s.ordinal());
      assertThat(frames.get(frames.size() - 1).pad()).as("closed by a pad").isTrue();
    }
    for (int i = 1; i < head.segments().size(); i++) {
      Segment before = head.segments().get(i - 1);
      assertThat(head.segments().get(i).base()).isEqualTo(before.base() + before.closedLength());
    }
    assertThat(log.read().records()).containsExactlyElementsOf(appended);
  }

  @Test
  void theBlockLimitClosesASegmentWithoutAPad() { // AZ1
    InMemoryAppendBlobStore few = new InMemoryAppendBlobStore(3, () -> 0L);
    AppendBlobLog log = new AppendBlobLog(few, ROOT, AppendBlobLog.Options.defaults()
        .withTrimGraceMs(0).withClock(() -> 0L));
    create(log);
    List<byte[]> appended = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      byte[] r = record();
      log.append(r, null);
      appended.add(r);
    }
    AzureLogHead head = AzureLogHead.decode(few.peek(ROOT + "/head"));
    assertThat(head.segments()).hasSize(3);
    Segment first = head.first();
    assertThat(first.closedLength()).isEqualTo(few.peek(first.name()).length)
        .isLessThan(first.limit());
    assertThat(log.read().records()).containsExactlyElementsOf(appended);
  }

  @Test
  void anAppendThatArrivesAfterTheCloseLandsNowhere() { // AZ1, AZ3
    AppendBlobLog log = log(300);
    create(log);
    log.append(record(), null);
    store.intercept((op, name) -> op == Op.APPEND ? Fault.DELAYED : Fault.NONE);
    byte[] late = record();
    assertThatThrownBy(() -> log.append(late, null)).isInstanceOf(UncheckedIOException.class);
    store.intercept(null);
    List<byte[]> appended = appendUntil(log, 2);
    assertThat(store.deliverDelayed()).as("the closed segment refused it").isZero();
    assertThat(log.read().records()).doesNotContain(late).endsWith(
        appended.toArray(new byte[0][]));
  }

  @Test
  void aTrimAfterTheLogMovedOnKeepsTheCheckpointsSegment() { // P5, AZ5
    AppendBlobLog log = log(300);
    create(log);
    log.append(record(), null);
    LogView read = log.read();
    // a compaction read the log; the log moves on before it trims
    log.putCheckpoint(read.tail(), checkpoint(1, read.tail()));
    List<byte[]> after = appendUntil(log, 3);
    log.trim(read.tail());
    LogView view = log.read();
    assertThat(view.checkpoint()).hasValue(checkpoint(1, read.tail()));
    assertThat(view.records()).containsExactlyElementsOf(after);
  }

  /**
   * A reader whose tail is the end of a segment a pad closed, read before the head named a
   * successor, reads the records the successor took once one does: the segment is closed at its
   * limit, which no writer's block reaches, though the read from its end sees no pad.
   */
  @Test
  void aReadFromTheEndOfAPaddedSegmentFollowsTheLogOn() { // AZ2
    AppendBlobLog writer = log(300);
    AppendBlobLog reader = log(300);
    create(writer);
    writer.append(record(), null);
    // a writer closed segment 0 with a pad and stopped before it named a successor
    Segment first = head().first();
    long length = store.properties(first.name()).length();
    store.appendBlock(first.name(), Frames.pad(first.limit() - length, first.ordinal()),
        first.limit(), length);
    LogView read = reader.read();
    assertThat(head().segments()).as("no successor yet").hasSize(1);
    byte[] next = record();
    writer.append(next, null);
    assertThat(head().segments()).as("the writer moved the log on").hasSize(2);
    assertThat(reader.readAfter(read.tail()).records()).containsExactly(next);
  }

  /**
   * A writer whose CAS of the head landed though its reply said it lost (a transport's resend,
   * failing its If-Match) finds its own candidate named, and keeps it: deleting it as another
   * writer's loser would leave the head naming a segment that does not exist (AZ5).
   */
  @Test
  void aRotationWhoseCasLandedUnseenKeepsItsSegment() { // AZ4, AZ5
    AtomicBoolean lose = new AtomicBoolean();
    AppendBlobLog log = around(300, (method, args, through) -> {
      Object result = through.call();
      boolean lost = method.getName().equals("putIfMatch") && result != null
          && lose.getAndSet(false);
      return lost ? null : result;
    });
    create(log);
    lose.set(true);
    List<byte[]> appended = appendUntil(log, 2);
    assertThat(lose).as("the rotation's CAS was reported lost").isFalse();
    assertThat(store.names()).contains(head().last().name());
    appended.addAll(appendUntil(log, 3));
    assertThat(log.read().records()).containsExactlyElementsOf(appended);
  }

  @Test
  void aWriterWhoseSegmentWasSweptNeverCreatesItAgain() { // AZ4
    AppendBlobLog stale = log(300);
    AppendBlobLog other = log(300);
    create(stale);
    stale.append(record(), null); // its cached head names segment 0
    String first = head().first().name();
    appendUntil(other, 3);
    LogView read = other.read();
    other.putCheckpoint(read.tail(), checkpoint(1, read.tail()));
    other.trim(read.tail());
    assertThat(store.names()).doesNotContain(first);
    byte[] r = record();
    Receipt receipt = stale.append(r, null);
    assertThat(store.names()).as("never created again").doesNotContain(first);
    assertThat(stale.readThrough(receipt, WAIT).records()).contains(r);
  }

  /**
   * A read through a write reads nothing when the reply put the record at the tail of the view
   * the log kept, and reads what landed between when something did (AZURE_LOG.md "Reading
   * through a write").
   */
  @Test
  void aReadThroughAWriteReadsOnlyWhatLandedBetween() {
    AppendBlobLog log = log(1 << 20);
    AppendBlobLog other = log(1 << 20);
    create(log);
    log.read();
    List<Op> requests = new ArrayList<>();
    store.intercept((op, name) -> {
      requests.add(op);
      return Fault.NONE;
    });
    byte[] mine = record();
    LogView view = log.readThrough(log.append(mine, null), WAIT);
    assertThat(requests).as("an append, and no read").containsExactly(Op.APPEND);
    assertThat(view.records()).containsExactly(mine);

    byte[] theirs = record();
    other.append(theirs, null);
    requests.clear();
    byte[] next = record();
    view = log.readThrough(log.append(next, null), WAIT);
    assertThat(requests).as("an append, and a read of what landed before it")
        .containsExactly(Op.APPEND, Op.READ);
    assertThat(view.records()).containsExactly(mine, theirs, next);
  }
}
