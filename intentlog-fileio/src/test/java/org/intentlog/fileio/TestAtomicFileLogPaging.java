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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InMemoryAtomicFileIO;
import org.intentlog.CompactionPolicy;
import org.intentlog.DedupHorizonExceededException;
import org.intentlog.LogRuntime;
import org.intentlog.LogView;
import org.intentlog.Replica;
import org.intentlog.RuntimeConfig;
import org.intentlog.testkit.EntityVersionsMachine;
import org.intentlog.testkit.EntityVersionsMachine.State;
import org.intentlog.testkit.EntityVersionsMachine.Txn;
import org.intentlog.wire.CommittedSetCodec;
import org.intentlog.wire.LogEnvelope;
import org.intentlog.wire.PageRef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Assertions;

/**
 * Paging the committed set on one object (docs/COMMITTED_PAGING.md): a replace writes {@code
 * [checkpoint][gc][page-outs][record]}, its pages put beside the object first; the checkpoint is
 * the replay of the version it replaces, so the next replace's omits the paged blocks; a replace
 * whose GC drops pages deletes them once it lands. On a store without appends (every commit a
 * replace) and on one with them (a replace when the policy says). Blocks hold at most 16 ids, as
 * surefire sets them.
 */
class TestAtomicFileLogPaging {

  private static final String LOCATION = "mem:///warehouse/catalog";
  private static final String PAGES = LOCATION + ".committed/";
  private static final EntityVersionsMachine MACHINE = EntityVersionsMachine.INSTANCE;
  private static final long HOUR = 3_600_000L;
  private static final long A = 1_000L;
  private static final long T0 = 1_800_000_000_000L;

  private final AtomicLong clock = new AtomicLong(T0 - 10_000L);
  private InMemoryAtomicFileIO io;
  private AtomicFileLog log;
  private LogRuntime<State, EntityVersionsMachine.Builder, Txn> writer;

  private void open(boolean appends) {
    io = new InMemoryAtomicFileIO(appends);
    log = new AtomicFileLog(io, LOCATION);
    // With appends, a compaction after the commit that takes the log to 8 records (the point a
    // writer draws in [1, 2) of the limits fixed at 1: drawn, the GC below may not come in time)
    CompactionPolicy policy = CompactionPolicy.limits(8, Long.MAX_VALUE).withDraws(() -> 0.0);
    writer = new LogRuntime<>(log, MACHINE,
        new RuntimeConfig(4, 3, 0L, 0L, HOUR, policy, Duration.ZERO, clock::get, A, 0L));
    writer.bootstrap(new Txn("init", Map.of(), Set.of()));
  }

  @BeforeEach
  void blocksOf16() {
    assumeThat(CommittedSetCodec.BLOCK_IDS).as("blocks of 16 ids (surefire)").isEqualTo(16);
  }

  private Replica<State> commitAt(long ts, String tag) {
    clock.set(ts);
    Replica<State> on = writer.read();
    return writer.commit(on, Txn.on(on.state(), tag, Set.of(), Set.of(tag)));
  }

  private List<String> pageObjects() {
    List<String> names = new ArrayList<>();
    for (FileInfo file : io.listPrefix(PAGES)) {
      names.add(file.location().substring(PAGES.length()));
    }
    return names;
  }

  private List<LogEnvelope.Record> records() {
    List<LogEnvelope.Record> records = new ArrayList<>();
    for (byte[] record : log.read().records()) {
      records.add(LogEnvelope.decodeRecord(record));
    }
    return records;
  }

  /** 40 commits from T0, then commits an admission window later until a replace pages. */
  private Replica<State> paged() {
    for (int i = 0; i < 40; i++) {
      commitAt(T0 + 10L * i, "t" + i);
    }
    Replica<State> r = null;
    for (int i = 0; i < 10 && (r == null || r.committed().pages().isEmpty()); i++) {
      r = commitAt(T0 + 10_000L + i, "u" + i);
    }
    return r;
  }

  @ParameterizedTest(name = "appends={0}")
  @ValueSource(booleans = {false, true})
  void aReplacePagesTheOldestBlocksBeforeItsOwnRecord(boolean appends) {
    open(appends);
    Replica<State> r = paged();
    assertThat(r.committed().pages()).isNotEmpty();
    // The replace that paged: [checkpoint][gc][page-outs][record], or later appends after it
    LogView view = log.read();
    List<LogEnvelope.Record> records = records();
    int pageOuts = (int) records.stream().filter(LogEnvelope.Record::isPageOut).count();
    if (pageOuts > 0) {
      assertThat(records.get(0).isAdvanceHorizon()).isTrue();
      for (int i = 1; i <= pageOuts; i++) {
        assertThat(records.get(i).isPageOut()).as("record %d", i).isTrue();
      }
      assertThat(records.get(pageOuts + 1).isIntention()).isTrue();
      // The checkpoint is the replay of what it replaced: it still holds the blocks this write
      // pages, and names only the pages sealed before it
      CommittedSetCodec.Decoded checkpointed = CommittedSetCodec.decodeBody(
          LogEnvelope.decodeCheckpoint(view.checkpoint().orElseThrow()).committed());
      assertThat(checkpointed.pages).hasSize(r.committed().pages().size() - pageOuts);
    }
    // Each page exists beside the object, and the floor stays below every record after it
    assertThat(pageObjects()).containsExactlyInAnyOrderElementsOf(
        r.committed().pages().stream().map(PageRef::name).toList());
    for (int i = 0; i < 40; i++) {
      assertThat(writer.committed(r, idOf("t" + i))).as("t%d", i).isTrue();
    }
    // The next replace's checkpoint omits the paged blocks: it holds their references
    Replica<State> after = r;
    for (int i = 0; i < 12; i++) {
      after = commitAt(T0 + 11_000L + i, "v" + i);
    }
    CommittedSetCodec.Decoded later = CommittedSetCodec.decodeBody(
        LogEnvelope.decodeCheckpoint(log.read().checkpoint().orElseThrow()).committed());
    assertThat(later.pages).isNotEmpty();
    assertThat(after.state().history()).contains("t0", "t39", "v11");
  }

  @ParameterizedTest(name = "appends={0}")
  @ValueSource(booleans = {false, true})
  void aReplaceWhoseGcDropsPagesDeletesThem(boolean appends) {
    open(appends);
    Replica<State> r = paged();
    assertThat(pageObjects()).isNotEmpty();
    // An hour on: the next replace's GC drops every page
    Replica<State> gc = null;
    for (int i = 0; i < 10 && (gc == null || !gc.committed().pages().isEmpty()); i++) {
      gc = commitAt(T0 + 20_000L + HOUR + i, "late" + i);
    }
    assertThat(gc.committed().pages()).isEmpty();
    assertThat(pageObjects()).isEmpty();
    // The oldest id is in the stale replica's first page, which GC dropped and deleted: the
    // lookup reads afresh, and finds it at or below the horizon
    Replica<State> stale = r;
    UUID init = idOf("init");
    assertThat(org.intentlog.wire.UuidV7.timestampMs(init))
        .isLessThanOrEqualTo(stale.committed().pages().get(0).top());
    Assertions.assertThrows(DedupHorizonExceededException.class,
        () -> writer.committed(stale, init));
  }

  /**
   * A replace whose clock is far ahead of its own id's stamp (a slow commit, or a clock that
   * jumped) seals only blocks below its own record: else its PageOut raises the floor past the
   * record that follows it, which replay then rejects (TLA+: pageown).
   */
  @org.junit.jupiter.api.Test
  void aReplaceKeepsItsPageOutsBelowItsOwnRecord() {
    open(false);
    for (int i = 0; i < 20; i++) {
      commitAt(T0 + 10L * i, "t" + i);
    }
    clock.set(T0 + 5_000L);
    Replica<State> base = writer.read();
    // Others commit after the writer read, in blocks around and above its stamp, with clocks too
    // close to seal them
    for (int i = 0; i < 40; i++) {
      commitAt(T0 + 5_001L + 10L * i, "u" + i);
    }
    // The writer mints at T0 + 5,000; by the time it writes, its clock reads an hour later
    long[] reads = {0};
    clock.set(T0 + 5_000L);
    LogRuntime<State, EntityVersionsMachine.Builder, Txn> slow = new LogRuntime<>(log, MACHINE,
        new RuntimeConfig(4, 3, 0L, 0L, 2 * HOUR,
            CompactionPolicy.limits(8, Long.MAX_VALUE).withDraws(() -> 0.0),
            Duration.ZERO, () -> reads[0]++ == 0 ? T0 + 5_000L : T0 + HOUR, A, 0L));
    Replica<State> landed =
        slow.commit(base, Txn.on(base.state(), "slow", Set.of(), Set.of("slow")));
    assertThat(landed.state().history()).contains("slow");
    assertThat(landed.committed().floor()).isLessThan(T0 + 5_000L);
  }

  private UUID idOf(String tag) {
    // Records leave the object as it compacts: find the id in the store's history
    for (InMemoryAtomicFileIO.Write write : io.history()) {
      if (!write.location().equals(LOCATION)) {
        continue;
      }
      for (byte[] record : AtomicFileLog.decode(write.after()).records()) {
        LogEnvelope.Record decoded = LogEnvelope.decodeRecord(record);
        if (decoded.isIntention()
            && MACHINE.decodeIntention(decoded.intention()).tag().equals(tag)) {
          return decoded.id();
        }
      }
    }
    throw new AssertionError("no record of " + tag);
  }
}
