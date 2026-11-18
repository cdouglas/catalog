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
import org.intentlog.LogNotSentException;
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
    return checkpoint(LOG_ID, n, through);
  }

  private static byte[] checkpoint(UUID logId, int n, LogTail through) {
    return LogEnvelope.encodeCheckpoint(logId, new byte[0],
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
    return segmentIn("segs", ordinal);
  }

  /** A segment's name as an Install holds it: its blob's, without the prefix and the log's id. */
  private static String nameOf(String blob) {
    return blob.substring(blob.lastIndexOf('/') + 1);
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
        nameOf(one), at.offset(), at.blocks(), frame.length, SegmentFrames.crcOf(frame))), c);
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
        cat.segs().stream().map(seg -> SegmentedLog.path("segs", LOG_ID, seg.name())).toList());
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
    String leftover =
        SegmentedLog.path("segs", LOG_ID, "seg-00000000000000000000-ffffffffffffffff");
    String inFlight =
        SegmentedLog.path("segs", LOG_ID, "seg-00000000000000000001-ffffffffffffffff");
    store.create(leftover); // an Install's that lost and failed to delete it
    store.create(inFlight); // an Install in flight
    log.sweep();
    assertThat(store.names()).containsExactlyInAnyOrder(segment(0), inFlight);
  }

  /**
   * The sweep lists only its own log's segments: another log's unnamed blob of an ordinal at or
   * below this log's newest (an orphan of that log, or its live segment) is that log's to judge,
   * and so is anything at the prefix but this log's segments' names.
   */
  @Test
  void aSweepListsOnlyItsOwnLogsSegments() {
    SegmentedLog log = created(4);
    UUID otherId = UuidV7.newUuidV7();
    String others = SegmentedLog.path("segs", otherId, "seg-00000000000000000000-ffffffffffffffff");
    String flat = "segs/seg-00000000000000000000-ffffffffffffffff"; // a name with no log's id
    String odd = SegmentedLog.path("segs", LOG_ID, "seg-0-ff"); // not a segment's name
    for (String blob : List.of(others, flat, odd)) {
      store.create(blob);
    }
    log.sweep();
    assertThat(store.names()).containsExactlyInAnyOrder(segment(0), others, flat, odd);
    assertThat(SegmentedLog.ordinalAt("segs", LOG_ID, others)).isEqualTo(-1);
    assertThat(SegmentedLog.ordinalAt("segs", otherId, others)).isEqualTo(0);
    assertThat(SegmentedLog.ordinalAt("segs", LOG_ID, flat)).isEqualTo(-1);
    assertThat(SegmentedLog.ordinalAt("segs", LOG_ID, odd)).isEqualTo(-1);
  }

  /**
   * A creation that finds another writer's log deletes its segment 0: no Install can name it, and
   * no sweep lists it, its log's id being in no catalog object.
   */
  @Test
  void aCreationThatLostDeletesItsSegment() {
    SegmentedLog first = log(2);
    SegmentedLog second = log(2);
    LogTail absent = first.probe();
    AtomicLong creates = new AtomicLong();
    store.intercept((op, name) -> {
      if (op == InMemorySegmentStore.Op.CREATE && creates.getAndIncrement() == 0) {
        // the second creation creates its segment 0; the first creates the log meanwhile
        first.putCheckpoint(absent, checkpoint(0, absent));
      }
      return InMemorySegmentStore.Fault.NONE;
    });
    UUID otherId = UuidV7.newUuidV7();
    assertThatThrownBy(() -> second.putCheckpoint(absent, checkpoint(otherId, 0, absent)))
        .isInstanceOf(LogConflictException.class);
    assertThat(creates.get()).isEqualTo(2);
    assertThat(store.names()).containsExactly(segment(0));
    assertThat(log(2).read().checkpoint().orElseThrow()).isEqualTo(checkpoint(0, absent));
  }

  /**
   * Bug 100 (sweep deleted another log's segments): two catalogs configured with one segment
   * location keep two logs whose segments share a prefix of one store. A compaction of one, and
   * its sweep, must leave every segment of the other: its catalog object names them, and its
   * records are in them.
   */
  @Test
  void aSweepLeavesTheSegmentsOfAnotherLogAtItsPrefix() {
    String otherLocation = "mem:///other-warehouse/catalog";
    UUID otherId = UuidV7.newUuidV7();
    SegmentedLog other = new SegmentedLog(AtomicFileLog.segmented(io, otherLocation, () -> 7L,
        false), store, "segs", SegmentedLog.Options.defaults().withLimit(2).withRetireTtlMs(0)
            .withClock(() -> 0L));
    LogTail absent = other.probe();
    other.putCheckpoint(absent, checkpoint(otherId, 0, absent));
    byte[] b1 = record();
    byte[] b2 = record();
    other.readThrough(other.append(b1, null), Duration.ZERO);
    other.readThrough(other.append(b2, null), Duration.ZERO); // ends its 0; installs its 1

    SegmentedLog log = created(2);
    byte[] a1 = record();
    log.readThrough(log.append(a1, null), Duration.ZERO);
    LogTail through = log.read().tail();
    log.putCheckpoint(through, checkpoint(1, through));
    log.trim(through); // the sweep

    SegmentedLog reader = new SegmentedLog(AtomicFileLog.segmented(io, otherLocation, () -> 7L,
        false), store, "segs", SegmentedLog.Options.defaults().withLimit(2));
    assertThat(reader.read().records()).containsExactly(b1, b2);
    byte[] b3 = record();
    other.readThrough(other.append(b3, null), Duration.ZERO);
    assertThat(log(2).read().records()).isEmpty();
    assertThat(new SegmentedLog(AtomicFileLog.segmented(io, otherLocation, () -> 7L, false),
        store, "segs", SegmentedLog.Options.defaults().withLimit(2)).read().records())
        .containsExactly(b1, b2, b3);
  }

  // D13: the segments' prefix is the catalog object's

  /** A log at {@link #LOCATION} given {@code prefix} and {@code defaultPrefix} (each may be null). */
  private SegmentedLog given(String prefix, String defaultPrefix, long limit) {
    return given(LOCATION, prefix, defaultPrefix, limit);
  }

  private SegmentedLog given(String location, String prefix, String defaultPrefix, long limit) {
    return new SegmentedLog(AtomicFileLog.segmented(io, location, () -> 7L, false), store, prefix,
        defaultPrefix, SegmentedLog.Options.defaults().withLimit(limit).withRetireTtlMs(0)
            .withClock(() -> 0L));
  }

  private void create(SegmentedLog log) {
    LogTail absent = log.probe();
    log.putCheckpoint(absent, checkpoint(0, absent));
  }

  /** Every version the catalog object at {@code location} had, as a log without a default reads it. */
  private List<SegmentedLog.Cat> versions(String location) {
    return io.history().stream().filter(w -> w.location().equals(location) && w.after() != null)
        .map(w -> SegmentedLog.parse(AtomicFileLog.decode(w.after()))).toList();
  }

  /**
   * D13: clients given another prefix, or none, keep a log's segments where its catalog object
   * says. They read what its creator wrote, append to its segments, install the next ones under
   * its prefix and compact it, and nothing lands under their own. Each client was configured with
   * the prefix before, and one given another found no segment where it looked: its reads failed.
   */
  @Test
  void clientsGivenAnotherPrefixOrNoneKeepTheSegmentsWhereTheCatalogObjectSays() {
    SegmentedLog first = created(2); // given "segs", and no default: the object records it
    SegmentedLog other = given("elsewhere", null, 2);
    SegmentedLog none = given(null, null, 2);
    List<String> warnings = new ArrayList<>();
    java.util.logging.Logger jul = java.util.logging.Logger.getLogger(SegmentedLog.class.getName());
    java.util.logging.Handler handler = new java.util.logging.Handler() {
      @Override
      public void publish(java.util.logging.LogRecord r) {
        warnings.add(r.getMessage());
      }

      @Override
      public void flush() {}

      @Override
      public void close() {}
    };
    java.util.logging.Level level = jul.getLevel(); // another suite may have quieted it
    jul.setLevel(java.util.logging.Level.WARNING);
    jul.addHandler(handler);
    List<byte[]> sent = new ArrayList<>();
    try {
      for (SegmentedLog log : List.of(first, other, none, other, none, first, none)) {
        byte[] r = record();
        log.readThrough(log.append(r, null), Duration.ZERO); // segments 0-3, Installs by each
        sent.add(r);
      }
    } finally {
      jul.removeHandler(handler);
      jul.setLevel(level);
    }
    // the client given another prefix says, once, that it is ignored
    assertThat(warnings).singleElement().asString().contains("elsewhere", "is ignored");
    for (SegmentedLog log : List.of(first, other, none, given("elsewhere", "segs-default", 2))) {
      assertThat(log.read().records()).containsExactlyElementsOf(sent);
      assertThat(log.segmentPrefix()).isEqualTo("segs");
    }
    LogTail through = none.read().tail();
    none.putCheckpoint(through, checkpoint(1, through)); // restates the creator's prefix
    byte[] after = record();
    other.readThrough(other.append(after, null), Duration.ZERO);
    assertThat(given(null, null, 2).read().records()).containsExactly(after);
    assertThat(versions(LOCATION)).extracting(SegmentedLog.Cat::recorded).containsOnly("segs");
    assertThat(store.names()).isNotEmpty()
        .allSatisfy(n -> assertThat(n).startsWith("segs/" + LOG_ID + "/"));
  }

  /**
   * D13: a log created at its default prefix records none, so its catalog object holds no byte
   * more than before: a creator given the default records none either, and one given another
   * prefix records it in one frame of {@code 23 + n} bytes (its length's varint, the record's tag
   * and length, the prefix's {@code n} bytes, the 20-byte trailer; for {@code n} up to 125).
   * Checkpoints keep it so, whoever writes them.
   */
  @Test
  void aLogCreatedAtItsDefaultRecordsNoPrefix() {
    create(given(null, "segs", 2));
    byte[] atDefault = io.bytes(LOCATION);
    SegmentedLog.Cat cat = SegmentedLog.parse(AtomicFileLog.decode(atDefault), "segs");
    assertThat(cat.recorded()).isNull();
    assertThat(cat.prefix()).isEqualTo("segs");
    assertThat(cat.view().records()).hasSize(2); // the checkpoint's position, Install 0

    create(given("mem:///given-the-default/catalog", "segs", "segs", 2));
    assertThat(io.bytes("mem:///given-the-default/catalog")).hasSize(atDefault.length);
    create(given("mem:///given-another/catalog", "segs", null, 2));
    assertThat(io.bytes("mem:///given-another/catalog")).hasSize(atDefault.length + 23 + 4);

    SegmentedLog writer = given("elsewhere", "segs", 2); // the catalog object's wins
    for (int i = 0; i < 3; i++) {
      writer.readThrough(writer.append(record(), null), Duration.ZERO);
    }
    LogTail through = writer.read().tail();
    writer.putCheckpoint(through, checkpoint(1, through));
    assertThat(versions(LOCATION)).hasSizeGreaterThan(2)
        .allSatisfy(v -> assertThat(v.recorded()).isNull());
    assertThat(store.names()).noneMatch(n -> n.startsWith("elsewhere/"));
  }

  /**
   * D13: a prefix that is not the default is recorded at the log's creation and restated by every
   * checkpoint, whichever client writes it, so a client given none and no default reads the log.
   */
  @Test
  void anExplicitPrefixRoundTripsThroughCheckpoints() {
    SegmentedLog creator = given("segs", "segs-default", 1);
    create(creator);
    List<SegmentedLog> writers = List.of(creator, given(null, "segs-default", 1),
        given("elsewhere", "segs-default", 1));
    for (SegmentedLog writer : writers) {
      for (int i = 0; i < 2; i++) {
        writer.readThrough(writer.append(record(), null), Duration.ZERO);
      }
      LogTail through = writer.read().tail();
      writer.putCheckpoint(through, checkpoint(writers.indexOf(writer) + 1, through));
    }
    byte[] r = record();
    creator.readThrough(creator.append(r, null), Duration.ZERO);
    assertThat(versions(LOCATION)).hasSizeGreaterThan(writers.size())
        .allSatisfy(v -> assertThat(v.recorded()).isEqualTo("segs"));
    SegmentedLog none = given(null, null, 1);
    assertThat(none.read().records()).containsExactly(r);
    assertThat(none.segmentPrefix()).isEqualTo("segs");
    assertThat(store.names()).isNotEmpty().allSatisfy(n -> assertThat(n).startsWith("segs/"));
  }

  /**
   * D13: a client given no prefix and no default opens an existing log from its catalog object,
   * but cannot create one: it fails before it writes anything.
   */
  @Test
  void aClientWithNoPrefixCannotCreateALog() {
    SegmentedLog none = given(null, null, 2);
    LogTail absent = none.probe();
    assertThatThrownBy(() -> none.putCheckpoint(absent, checkpoint(0, absent)))
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("cannot create");
    assertThat(store.names()).isEmpty();
    assertThat(io.bytes(LOCATION)).isNull();
  }

  /**
   * D13: a catalog object that records no prefix keeps its segments at the default alone. A client
   * that derives none fails, rather than look under the prefix it was given.
   */
  @Test
  void aCatalogObjectWithoutAPrefixIsReadAtTheDefaultAlone() {
    SegmentedLog atDefault = given(null, "segs", 2);
    create(atDefault);
    byte[] a = record();
    atDefault.readThrough(atDefault.append(a, null), Duration.ZERO);
    assertThatThrownBy(() -> given("segs", null, 2).read())
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("records no prefix");
    assertThat(given("elsewhere", "segs", 2).read().records()).containsExactly(a);
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
    List<String> named =
        cat.segs().stream().map(seg -> SegmentedLog.path("segs", LOG_ID, seg.name())).toList();
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
    // the migration moves the prefix, and locates the segments it leaves at the old one
    relocate("segs", seg -> store.location(SegmentedLog.path("old", seg.logId(), seg.name())));

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
        nameOf(three), at.offset(), at.blocks(), frame.length, SegmentFrames.crcOf(frame))), f);
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
    relocate("segs", seg -> "https://elsewhere.example/logs/" + seg.name());
    assertThatThrownBy(() -> log(2).read()).hasStackTraceContaining("cannot reach");
  }

  /**
   * Replaces the catalog object's prefix with {@code prefix}, and its Installs with ones naming
   * the locations {@code where} gives.
   */
  private void relocate(String prefix, Function<SegmentedLog.Seg, String> where) {
    AtomicFileLog catalog = AtomicFileLog.segmented(io, LOCATION, () -> 7L, false);
    LogView v = catalog.read();
    SegmentedLog.Cat cat = SegmentedLog.parse(v);
    List<byte[]> records = new ArrayList<>();
    records.add(LogEnvelope.encodeCheckpointThrough(cat.ck().encode()));
    records.add(LogEnvelope.encodeSegmentPrefix(prefix));
    for (SegmentedLog.Seg seg : cat.segs()) {
      records.add(new SegmentedLog.Seg(seg.logId(), prefix, seg.ordinal(), seg.name(),
          where.apply(seg), seg.limit()).install());
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
        .filter(n -> SegmentedLog.ordinalAt(prefix, LOG_ID, n) == ordinal)
        .findFirst().orElseThrow();
  }

  @Test
  void aLimitPastTheStoresBlocksIsRefused() {
    assertThatThrownBy(() -> log(9)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("past the store");
  }

  // Bug 115 (segment that cannot end): segments of 8 blocks and 1,000 bytes, blocks of 1,000

  private static final int SMALL_SEGMENT = 1000;

  private final InMemorySegmentStore small = new InMemorySegmentStore(8, SMALL_SEGMENT,
      SMALL_SEGMENT);

  private SegmentedLog inSmall(long limit) {
    return new SegmentedLog(AtomicFileLog.segmented(io, LOCATION, () -> 7L, false), small, "segs",
        SegmentedLog.Options.defaults().withLimit(limit).withRetireTtlMs(0).withClock(() -> 0L));
  }

  /** A record whose frame is exactly {@code frameBytes}. */
  private static byte[] recordWithFrame(int frameBytes) {
    for (int payload = frameBytes; payload > 0; payload--) {
      byte[] r = LogEnvelope.encodeRecord(UuidV7.newUuidV7(), new byte[payload]);
      if (SegmentFrames.record(r, 0).length == frameBytes) {
        return r;
      }
    }
    throw new IllegalStateException("no record frames to " + frameBytes);
  }

  /**
   * Bug 115 (segment that cannot end): a segment's limit is its installer's K, but a writer sized
   * its records by its own K. A writer with K = 2 sent two records of 500-byte frames into a
   * segment whose installer set K = 4: the segment's 1,000 bytes held two records and no room for
   * a seal frame, so it never ended, and every record after it was lost to readers (SG8). The bound
   * now follows the store's block limit, which bounds every K, so writers of any K share it, and
   * such a record is refused before it is sent; whatever lands, the log reads on.
   */
  @Test
  void aSmallLimitsRecordsCannotFillALargeLimitsSegment() {
    SegmentedLog large = inSmall(4); // installs segment 0, K = 4
    LogTail absent = large.probe();
    large.putCheckpoint(absent, checkpoint(0, absent));
    SegmentedLog smallK = inSmall(2);
    List<byte[]> sent = new ArrayList<>();
    for (byte[] r : List.of(recordWithFrame(SMALL_SEGMENT / 2), recordWithFrame(SMALL_SEGMENT / 2),
        record(), record())) {
      try {
        smallK.readThrough(smallK.append(r, null), Duration.ZERO);
        sent.add(r);
      } catch (LogNotSentException e) {
        assertThat(r.length).isGreaterThan((int) smallK.capabilities().maxRecordBytes());
      }
    }
    assertThat(sent).hasSizeGreaterThanOrEqualTo(2);
    assertThat(inSmall(4).read().records()).containsExactlyElementsOf(sent);
    assertThat(smallK.capabilities().maxRecordBytes())
        .isEqualTo(large.capabilities().maxRecordBytes())
        .isEqualTo(inSmall(8).capabilities().maxRecordBytes());
  }

  /**
   * The bound's worst case: a segment whose installer set K to the store's block limit, filled by
   * a writer of any K with K - 1 of the largest frames its bound allows. The seal frame still fits:
   * a record that finds the segment full goes to the next, whose read through ends this one with
   * it, and the log reads on (Bug 115).
   */
  @Test
  void theLargestRecordsLeaveRoomForASealAtTheStoresLimit() {
    SegmentedLog full = inSmall(8);
    LogTail absent = full.probe();
    full.putCheckpoint(absent, checkpoint(0, absent));
    SegmentedLog smallK = inSmall(2);
    long bound = smallK.capabilities().maxRecordBytes();
    List<byte[]> sent = new ArrayList<>();
    for (int i = 0; i < 9; i++) {
      byte[] r = null;
      for (int payload = (int) bound; r == null || r.length > bound; payload--) {
        r = LogEnvelope.encodeRecord(UuidV7.newUuidV7(), new byte[payload]);
      }
      assertThat(bound - r.length).isLessThanOrEqualTo(1);
      smallK.readThrough(smallK.append(r, null), Duration.ZERO);
      sent.add(r);
    }
    LogView view = inSmall(8).read();
    assertThat(view.records()).containsExactlyElementsOf(sent);
    assertThat(SegmentPosition.decode(view.tail().position()).ordinal()).isEqualTo(1);
  }
}
