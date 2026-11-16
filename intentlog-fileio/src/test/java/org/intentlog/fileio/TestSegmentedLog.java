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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.iceberg.io.InMemoryAtomicFileIO;
import org.intentlog.LogConflictException;
import org.intentlog.LogTail;
import org.intentlog.LogView;
import org.intentlog.Receipt;
import org.intentlog.TrimmedException;
import org.intentlog.memory.InMemorySegmentStore;
import org.intentlog.segments.SegmentFrames;
import org.intentlog.segments.SegmentPosition;
import org.intentlog.segments.SegmentStore;
import org.intentlog.wire.LogEnvelope;
import org.intentlog.wire.UuidV7;
import org.junit.jupiter.api.Test;

/** {@link SegmentedLog}'s regressions: the cases docs/LOG_SEGMENTS.md names, one each. */
class TestSegmentedLog {

  private static final String LOCATION = "mem:///warehouse/catalog";
  private static final UUID LOG_ID = UuidV7.newUuidV7();

  private final InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
  private final InMemorySegmentStore store = new InMemorySegmentStore(8, 1 << 16, 1 << 22);
  private int records = 0;

  private SegmentedLog log(long limit) {
    return log(limit, SegmentedLog.Options.defaults());
  }

  private SegmentedLog log(long limit, SegmentedLog.Options options) {
    return new SegmentedLog(AtomicFileLog.segmented(io, LOCATION, () -> 7L, false), store,
        "segs", options.withLimit(limit).withRetireTtlMs(0).withClock(() -> 0L));
  }

  private static byte[] checkpoint(int n, LogTail through) {
    return LogEnvelope.encodeCheckpoint(LOG_ID, new byte[0],
        ("state-" + n).getBytes(StandardCharsets.UTF_8), through.position());
  }

  private byte[] record() {
    return LogEnvelope.encodeRecord(UuidV7.newUuidV7(),
        ("r" + records++).getBytes(StandardCharsets.UTF_8));
  }

  private SegmentedLog created(long limit) {
    SegmentedLog log = log(limit);
    LogTail absent = log.probe();
    log.putCheckpoint(absent, checkpoint(0, absent));
    return log;
  }

  private String segment(long ordinal) {
    return store.names().stream()
        .filter(n -> SegmentedLog.ordinalOf(n.substring("segs/".length())) == ordinal)
        .findFirst().orElseThrow();
  }

  @Test
  void aSegmentEndsAtItsLimitAndTheNextTakesOver() {
    SegmentedLog log = created(2);
    byte[] a = record();
    byte[] b = record();
    byte[] c = record();
    for (byte[] r : List.of(a, b, c)) {
      log.readThrough(log.append(r, null), Duration.ZERO);
    }
    LogView view = log(2).read();
    assertThat(view.records()).containsExactly(a, b, c);
    SegmentPosition end = SegmentPosition.decode(view.tail().position());
    assertThat(end.ordinal()).isEqualTo(1);
    assertThat(end.records()).isEqualTo(1);
    assertThat(view.start()).isEqualTo(SegmentPosition.START.encode());
  }

  @Test
  void anAppendPastTheLimitIsVoidByItsReply() {
    SegmentedLog writer = created(1);
    SegmentedLog stale = log(1);
    stale.read(); // knows segment 0 alone, open
    byte[] a = record();
    writer.readThrough(writer.append(a, null), Duration.ZERO); // segment 0 ends at it
    byte[] b = record();
    stale.readThrough(stale.append(b, null), Duration.ZERO); // void in 0, then in 1
    assertThat(store.appends()).hasSize(3);
    assertThat(log(1).read().records()).containsExactly(a, b);
  }

  /** D1: a frame after a seal frame is void, so no reader that read before the seal loses one. */
  @Test
  void aFrameAfterASealIsVoidAndItsAppendALostRace() {
    SegmentedLog log = created(3);
    byte[] a = record();
    log.readThrough(log.append(a, null), Duration.ZERO);
    LogView before = log(3).read();
    store.append(segment(0), SegmentFrames.seal(0)); // another writer ends segment 0
    byte[] b = record();
    Receipt landed = log.append(b, null); // index 3, within the limit, but after the seal
    assertThatThrownBy(() -> log.readThrough(landed, Duration.ZERO))
        .isInstanceOf(LogConflictException.class);
    assertThat(before.records()).containsExactly(a);
    assertThat(log(3).read().records()).containsExactly(a);
  }

  /** A record in a later segment needs the earlier ones ended: the read through it seals them. */
  @Test
  void aReadThroughARecordInALaterSegmentSealsTheEarlierOne() {
    SegmentedLog log = created(3);
    byte[] a = record();
    log.readThrough(log.append(a, null), Duration.ZERO);
    SegmentedLog ahead = log(3, SegmentedLog.Options.defaults().withAheadMargin(3));
    byte[] b = record();
    ahead.readThrough(ahead.append(b, null), Duration.ZERO); // segment 0; installs 1 ahead
    String one = segment(1);
    byte[] c = record();
    byte[] frame = SegmentFrames.record(c, 1);
    SegmentStore.Appended at = store.append(one, frame);
    Receipt later = new SegmentedLog.SegReceipt(LOG_ID, List.of(new SegmentedLog.Copy(1,
        one.substring("segs/".length()), at.offset(), at.blocks(), frame.length,
        SegmentFrames.crcOf(frame))), c);
    assertThat(log(3).read().records()).containsExactly(a, b); // segment 1 waits
    assertThat(log.readThrough(later, Duration.ZERO).records()).containsExactly(a, b, c);
    assertThat(SegmentFrames.parse(store.peek(segment(0)), store.peek(segment(0)).length, 0, 0))
        .extracting(SegmentFrames.Frame::seal).containsExactly(false, false, true);
    assertThat(log(3).read().records()).containsExactly(a, b, c);
  }

  /** A copy in the next segment: the first copy the log holds is the record, once. */
  @Test
  void copiesNearTheEndAreReadThroughAtTheFirst() {
    SegmentedLog log = log(2, SegmentedLog.Options.defaults().withCopies(true));
    LogTail absent = log.probe();
    log.putCheckpoint(absent, checkpoint(0, absent));
    byte[] a = record();
    LogView through = log.readThrough(log.append(a, null), Duration.ZERO);
    assertThat(through.records()).containsExactly(a);
    byte[] b = record();
    log.readThrough(log.append(b, null), Duration.ZERO); // index 2: a copy goes to segment 1
    assertThat(log(2).read().records()).containsExactly(a, b, b); // replay skips the copy by id
  }

  @Test
  void aCheckpointRetiresTheSegmentsBeforeItsOwnAndTheSweepDeletesThem() {
    SegmentedLog log = created(1);
    byte[] a = record();
    Receipt first = log.append(a, null);
    log.readThrough(first, Duration.ZERO);
    byte[] b = record();
    log.readThrough(log.append(b, null), Duration.ZERO);
    byte[] c = record();
    log.readThrough(log.append(c, null), Duration.ZERO);
    LogTail through = log.read().tail(); // in segment 2
    log.putCheckpoint(through, checkpoint(1, through));
    assertThat(store.names()).hasSize(4); // 0-2, and 3 installed ahead
    log.trim(through);
    assertThat(store.names()).hasSize(2);
    assertThat(log(1).read().records()).isEmpty();
    assertThatThrownBy(() -> log(1).readThrough(first, Duration.ZERO))
        .isInstanceOf(TrimmedException.class);
    assertThatThrownBy(() -> log.putCheckpoint(through, checkpoint(2, through)))
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("I11");
  }

  /**
   * A retired segment is deleted by the name its Retired entry holds, and a checkpoint keeps the
   * entry until its segment is gone: a store that cannot list (a SAS without container scope)
   * still reclaims every segment a checkpoint retires, and a delete that failed is sent again.
   */
  @Test
  void retiredSegmentsAreDeletedByNameWithoutAListing() {
    AtomicLong now = new AtomicLong();
    Set<String> failing = new HashSet<>();
    store.intercept((op, name) -> op == InMemorySegmentStore.Op.LIST
        || op == InMemorySegmentStore.Op.DELETE && failing.contains(name)
        ? InMemorySegmentStore.Fault.FAIL_BEFORE : InMemorySegmentStore.Fault.NONE);
    SegmentedLog log = new SegmentedLog(AtomicFileLog.segmented(io, LOCATION, () -> 7L, false),
        store, "segs", SegmentedLog.Options.defaults().withLimit(1).withRetireTtlMs(1_000)
            .withClock(now::get));
    LogTail absent = log.probe();
    log.putCheckpoint(absent, checkpoint(0, absent));
    for (int i = 0; i < 3; i++) {
      log.readThrough(log.append(record(), null), Duration.ZERO);
    }
    String zero = segment(0);
    String one = segment(1);
    LogTail first = log.read().tail(); // in segment 2
    log.putCheckpoint(first, checkpoint(1, first)); // retires 0 and 1
    log.trim(first); // within their TTL; the listing fails
    assertThat(store.names()).contains(zero, one);

    now.set(2_000); // past their TTL
    failing.add(zero);
    for (int i = 0; i < 2; i++) {
      log.readThrough(log.append(record(), null), Duration.ZERO);
    }
    LogTail second = log.read().tail(); // in segment 4
    log.putCheckpoint(second, checkpoint(2, second)); // deletes 1; 0's delete fails; retires 2, 3
    assertThat(store.names()).contains(zero).doesNotContain(one);
    assertThat(retired()).containsExactlyInAnyOrder(0L, 2L, 3L);

    failing.clear();
    log.trim(second); // the sweep deletes 0 by its name
    assertThat(store.names()).doesNotContain(zero);

    now.set(4_000);
    log.readThrough(log.append(record(), null), Duration.ZERO);
    LogTail third = log.read().tail(); // in segment 5
    log.putCheckpoint(third, checkpoint(3, third)); // drops 0; deletes 2 and 3; retires 4
    assertThat(retired()).containsExactly(4L);
    SegmentedLog.Cat cat = SegmentedLog.parse(AtomicFileLog.decode(io.bytes(LOCATION)));
    List<String> named = new ArrayList<>(
        cat.segs().stream().map(seg -> "segs/" + seg.name()).toList());
    named.add(segment(4));
    assertThat(store.names()).containsExactlyInAnyOrderElementsOf(named);
  }

  /** The ordinals of the catalog object's Retired entries. */
  private List<Long> retired() {
    return SegmentedLog.parse(AtomicFileLog.decode(io.bytes(LOCATION))).retired().stream()
        .map(r -> r.ordinal()).toList();
  }

  @Test
  void aSweepKeepsANamedSegmentAndABlobOfAnOrdinalStillToCome() {
    SegmentedLog log = created(4);
    store.create("segs/seg-00000000000000000000-ffffffffffffffff"); // a creation's leftover
    store.create("segs/seg-00000000000000000001-ffffffffffffffff"); // an Install in flight
    log.sweep();
    assertThat(store.names()).containsExactlyInAnyOrder(segment(0),
        "segs/seg-00000000000000000001-ffffffffffffffff");
  }

  /**
   * An installer whose Install lost to another's of the same ordinal deletes the blob it created:
   * no Install can name it, and a sweep that cannot list would leave it.
   */
  @Test
  void anInstallThatLostDeletesItsBlob() {
    SegmentedLog writer = created(1);
    SegmentedLog stale = log(1);
    stale.read(); // knows segment 0 alone
    writer.readThrough(writer.append(record(), null), Duration.ZERO); // ends 0, installs 1 ahead
    stale.readThrough(stale.append(record(), null), Duration.ZERO); // void in 0; loses Install 1
    LogView view = log(1).read();
    SegmentedLog.Cat cat = SegmentedLog.parse(AtomicFileLog.decode(io.bytes(LOCATION)));
    List<String> named = cat.segs().stream().map(seg -> "segs/" + seg.name()).toList();
    assertThat(store.names()).containsExactlyInAnyOrderElementsOf(named);
    assertThat(view.records()).hasSize(2);
  }

  /**
   * A segment whose entry names a location is read, appended to, sealed and deleted there, not
   * under the log's prefix: a log moved to a new prefix keeps the segments its catalog object
   * locates at the old one, installs new ones under the new, and deletes the old ones by the
   * locations their Retired entries keep, which the sweep's listing of the new prefix never sees.
   */
  @Test
  void aSegmentIsWhereItsEntryLocatesIt() {
    SegmentedLog old = log("old", 2, 2); // installs the next ahead at every append
    LogTail absent = old.probe();
    old.putCheckpoint(absent, checkpoint(0, absent));
    byte[] a = record();
    byte[] b = record();
    byte[] c = record();
    for (byte[] r : List.of(a, b, c)) {
      old.readThrough(old.append(r, null), Duration.ZERO); // 0: a, b; 1: c; 2 installed
    }
    relocate(seg -> store.location("old/" + seg.name())); // the move to prefix segs

    SegmentedLog moved = log("segs", 2, 2);
    assertThat(moved.read().records()).containsExactly(a, b, c);
    byte[] d = record();
    moved.readThrough(moved.append(d, null), Duration.ZERO); // ends 1, at old
    byte[] e = record();
    moved.readThrough(moved.append(e, null), Duration.ZERO); // 2, at old; installs 3 at segs
    assertThat(store.names()).filteredOn(n -> n.startsWith("segs/")).hasSize(1);
    String three = segmentIn("segs", 3);
    byte[] f = record();
    byte[] frame = SegmentFrames.record(f, 3);
    SegmentStore.Appended at = store.append(three, frame);
    Receipt later = new SegmentedLog.SegReceipt(LOG_ID, List.of(new SegmentedLog.Copy(3,
        three.substring("segs/".length()), at.offset(), at.blocks(), frame.length,
        SegmentFrames.crcOf(frame))), f);
    assertThat(moved.readThrough(later, Duration.ZERO).records())
        .containsExactly(a, b, c, d, e, f); // seals 2, at old
    byte[] two = store.peek(segmentIn("old", 2));
    assertThat(SegmentFrames.parse(two, two.length, 0, 2))
        .extracting(SegmentFrames.Frame::seal).containsExactly(false, true);
    assertThat(log("segs", 2, 0).read().records()).containsExactly(a, b, c, d, e, f);

    LogTail through = moved.read().tail(); // in 3
    moved.putCheckpoint(through, checkpoint(1, through)); // retires 0-2, keeping their locations
    assertThat(SegmentedLog.parse(AtomicFileLog.decode(io.bytes(LOCATION))).retired())
        .extracting(r -> r.location())
        .allSatisfy(location -> assertThat(location).startsWith(store.location("old/")));
    moved.trim(through); // past their TTL (0): deleted where they are
    assertThat(store.names()).noneMatch(n -> n.startsWith("old/"));
    assertThat(log("segs", 2, 0).read().records()).isEmpty();
  }

  /** A location the store cannot reach fails the read: it is never read under the prefix. */
  @Test
  void aSegmentAtALocationTheStoreCannotReachFailsTheRead() {
    SegmentedLog log = created(2);
    log.readThrough(log.append(record(), null), Duration.ZERO);
    relocate(seg -> "https://elsewhere.example/logs/" + seg.name());
    assertThatThrownBy(() -> log(2).read()).hasStackTraceContaining("cannot reach");
  }

  /** Replaces the catalog object's Installs with ones naming the locations {@code where} gives. */
  private void relocate(Function<SegmentedLog.Seg, String> where) {
    AtomicFileLog catalog = AtomicFileLog.segmented(io, LOCATION, () -> 7L, false);
    LogView v = catalog.read();
    SegmentedLog.Cat cat = SegmentedLog.parse(v);
    List<byte[]> records = new ArrayList<>();
    records.add(LogEnvelope.encodeCheckpointThrough(cat.ck().encode()));
    for (SegmentedLog.Seg seg : cat.segs()) {
      records.add(new SegmentedLog.Seg(seg.ordinal(), seg.name(), where.apply(seg), seg.limit())
          .install());
    }
    catalog.replace(v.tail(), cat.checkpoint(), records);
  }

  private SegmentedLog log(String prefix, long limit, long aheadMargin) {
    return new SegmentedLog(AtomicFileLog.segmented(io, LOCATION, () -> 7L, false), store, prefix,
        SegmentedLog.Options.defaults().withLimit(limit).withAheadMargin(aheadMargin)
            .withRetireTtlMs(0).withClock(() -> 0L));
  }

  private String segmentIn(String prefix, long ordinal) {
    return store.names().stream()
        .filter(n -> n.startsWith(prefix + "/"))
        .filter(n -> SegmentedLog.ordinalOf(n.substring(prefix.length() + 1)) == ordinal)
        .findFirst().orElseThrow();
  }

  @Test
  void aLimitPastTheStoresBlocksIsRefused() {
    assertThatThrownBy(() -> log(9)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("past the store");
  }
}
