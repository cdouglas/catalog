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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import org.intentlog.Capabilities;
import org.intentlog.LogConflictException;
import org.intentlog.LogLagException;
import org.intentlog.LogNotSentException;
import org.intentlog.LogTail;
import org.intentlog.LogThrottledException;
import org.intentlog.LogView;
import org.intentlog.PageStore;
import org.intentlog.Receipt;
import org.intentlog.SharedLog;
import org.intentlog.TrimmedException;

/**
 * A shared log in memory, as a CORFU-like log or a queue with an archiver keeps one: one total
 * order of records, each at a position (1, 2, ...); checkpoints stored apart from the records, each
 * the state through a position; and a trim point, through which records are gone. The log exists
 * once its first checkpoint, at position 0, is stored ({@link #putCheckpoint} at an absent tail).
 *
 * <p>Appends are unconditional, or conditioned on the tail they expect if the log is built with
 * conditional appends. A read starts from the newest checkpoint that covers the trim point and
 * returns the records after it. {@link #readThrough} starts from one before the receipt's record,
 * so the record is among the records it returns (P3), and throws {@link TrimmedException} if every
 * checkpoint the log keeps is past it. A trim needs a checkpoint at or past it (P5), and two
 * checkpoints at one position must be the same bytes (I11): one that differs is refused and
 * recorded ({@link #violations}).
 *
 * <p>Test hooks ({@link Interceptor}) see every request before it is served: they can fail a
 * write before or after it lands, throttle it, delay an append (it lands later, at {@link
 * #deliverDelayed}), fail a read, serve a read from an older checkpoint, or lag one behind the
 * tail as a replica would. The log keeps every record ever appended, trimmed ones included, for
 * oracles to replay ({@link #records}).
 *
 * <p>The log keeps the pages of its committed set in a map ({@link #pages}), unless built without
 * ({@link #withoutPages}). Two puts of one name must be the same bytes (a page's name is its
 * digest): one that differs is refused and recorded ({@link #violations}). Hooks see each page
 * request too ({@link Interceptor#beforePage}): a put or delete can fail without effect, or land
 * with its reply lost; a get can fail, or find no page.
 */
public final class InMemorySharedLog implements SharedLog {

  /** What happens to a write. Faults that do not apply to a request act as {@link #FAIL_BEFORE}. */
  public enum WriteFault {
    /** The write lands. */
    NONE,
    /** The request fails without landing. */
    FAIL_BEFORE,
    /** Preparing the write fails; nothing is sent ({@link LogNotSentException}). */
    NOT_SENT,
    /** The write lands; its reply is lost. */
    LOST_REPLY,
    /** The log throttles the write; nothing lands. */
    THROTTLED,
    /** The write lands; the log then reports a throttle. */
    LANDED_THROTTLED,
    /** An append times out, still in flight: it lands later, at {@link #deliverDelayed}. */
    DELAYED,
    /**
     * A conditional append lands; a transport retry of it fails its condition. An unconditional
     * append meets {@link #LOST_REPLY} instead.
     */
    LANDED_AS_CONFLICT
  }

  /** A request, as the hooks see it. */
  public enum Request {
    APPEND,
    PUT_CHECKPOINT,
    TRIM,
    READ,
    READ_THROUGH,
    READ_AFTER,
    PROBE,
    PUT_PAGE,
    GET_PAGE,
    DELETE_PAGE,
    LIST_PAGES
  }

  /** What happens to a page request. */
  public enum PageFault {
    /** It is served. */
    NONE,
    /** It fails without effect: a put or delete does not land, a get or list reads nothing. */
    FAIL,
    /** A put or delete lands, and its reply is lost; a get or list fails. */
    LOST_REPLY,
    /** A get finds no page, as if it had been deleted; another request fails. */
    MISSING
  }

  /**
   * How a read is served: failed, or from a checkpoint older than the newest it could use (by
   * {@code olderCheckpoints}), ending {@code lag} records before the tail (but never before its
   * checkpoint, nor, for {@link #readThrough}, before the record it reads through). {@link
   * #PAST_RECORD} breaks P3: a read through a write starts from the newest checkpoint, which may
   * hold the record already. A {@link #gap} leaves out the first records after the checkpoint (or
   * the tail a {@link #readAfter} follows), as a log that trimmed past its newest checkpoint or
   * lost records would serve them; the view says where its records start ({@link LogView#start}),
   * and the read fails (S4).
   */
  public static final class ReadShape {
    public static final ReadShape NORMAL = new ReadShape(false, 0, 0, false, 0);
    public static final ReadShape FAIL = new ReadShape(true, 0, 0, false, 0);
    public static final ReadShape PAST_RECORD = new ReadShape(false, 0, 0, true, 0);

    private final boolean fail;
    private final int olderCheckpoints;
    private final int lag;
    private final boolean pastRecord;
    private final int gap;

    private ReadShape(boolean fail, int olderCheckpoints, int lag, boolean pastRecord, int gap) {
      this.fail = fail;
      this.olderCheckpoints = olderCheckpoints;
      this.lag = lag;
      this.pastRecord = pastRecord;
      this.gap = gap;
    }

    public static ReadShape of(int olderCheckpoints, int lag) {
      return new ReadShape(false, Math.max(0, olderCheckpoints), Math.max(0, lag), false, 0);
    }

    /** Served without the first {@code records} records after the checkpoint or tail. */
    public static ReadShape gap(int records) {
      return new ReadShape(false, 0, 0, false, Math.max(1, records));
    }

    @Override
    public String toString() {
      return fail ? "FAIL"
          : pastRecord ? "PAST_RECORD"
          : gap > 0 ? "gap " + gap
          : "older " + olderCheckpoints + ", lag " + lag;
    }
  }

  /**
   * Test hooks, called on the thread making the request, before the log serves it. A hook may
   * make requests of its own, as another writer would (they are served first).
   */
  public interface Interceptor {
    /** The fault the write meets. {@code payload} is the record or checkpoint (null for a trim). */
    default WriteFault beforeWrite(Request request, byte[] payload) {
      return WriteFault.NONE;
    }

    /** A write landed, before its reply. */
    default void afterLanding(Request request) {}

    /** How the read is served. */
    default ReadShape beforeRead(Request request) {
      return ReadShape.NORMAL;
    }

    /** What happens to a page request: a put, get or delete of {@code name}, or a list (null). */
    default PageFault beforePage(Request request, String name) {
      return PageFault.NONE;
    }
  }

  /** A write that landed, in the order they landed. */
  public static final class Write {
    private final Request request;
    private final long position;
    private final byte[] payload;

    Write(Request request, long position, byte[] payload) {
      this.request = request;
      this.position = position;
      this.payload = payload;
    }

    public Request request() {
      return request;
    }

    /**
     * The record's position (an append), the position checkpointed or trimmed through, or the
     * log's length when a page was put or deleted.
     */
    public long position() {
      return position;
    }

    /** The record, checkpoint or page written; null for a trim; a page's name for a delete. */
    public byte[] payload() {
      return payload;
    }

    @Override
    public String toString() {
      return request + "@" + position;
    }
  }

  private final String name;
  private final boolean conditionalAppend;
  private final List<byte[]> records = new ArrayList<>();
  private final NavigableMap<Long, byte[]> checkpoints = new TreeMap<>();
  private final List<byte[]> delayed = new ArrayList<>();
  private final List<Write> history = new ArrayList<>();
  private final List<String> violations = new ArrayList<>();
  private long trimmed = 0;
  private volatile long recordLimit = 0;
  private Interceptor interceptor = new Interceptor() {};
  private final NavigableMap<String, byte[]> pageObjects = new TreeMap<>();
  private final PageStore pageStore = new MemoryPages();
  private volatile boolean storesPages = true;

  /**
   * A log with unconditional appends. Its reads see every append acknowledged, unless a hook lags
   * them.
   */
  public InMemorySharedLog(String name) {
    this(name, false);
  }

  /** @param conditionalAppend whether an append may name the tail it expects */
  public InMemorySharedLog(String name, boolean conditionalAppend) {
    this.name = name;
    this.conditionalAppend = conditionalAppend;
  }

  /**
   * Stores records of at most {@code limit} bytes, as a queue caps a message (0: no limit): its
   * capabilities say so, and a larger append is refused before it is sent ({@link
   * LogNotSentException}).
   */
  public synchronized InMemorySharedLog limitRecordBytes(long limit) {
    this.recordLimit = limit;
    return this;
  }

  /** Keeps no pages: {@link #pages} is null, so the runtime never pages this log. */
  public synchronized InMemorySharedLog withoutPages() {
    this.storesPages = false;
    return this;
  }

  /** Replaces the test hooks. */
  public synchronized InMemorySharedLog intercept(Interceptor hooks) {
    this.interceptor = hooks != null ? hooks : new Interceptor() {};
    return this;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public Capabilities capabilities() {
    return new Capabilities(true, conditionalAppend, false, true, recordLimit);
  }

  // ============================================================
  // Writes
  // ============================================================

  @Override
  public synchronized Receipt append(byte[] record, LogTail expected) {
    if (expected != null && !conditionalAppend) {
      throw new IllegalArgumentException(name + " appends unconditionally");
    }
    WriteFault fault = interceptor.beforeWrite(Request.APPEND, record);
    checkExists();
    if (recordLimit > 0 && record.length > recordLimit) {
      throw new LogNotSentException(
          "A record of " + record.length + " bytes is larger than " + name + " stores", null);
    }
    failBefore(fault, Request.APPEND);
    if (fault == WriteFault.DELAYED) {
      delayed.add(record);
      throw new UncheckedIOException(new IOException("An append to " + name + " timed out"));
    }
    if (expected != null && position(expected) != records.size()) {
      throw new LogConflictException(
          "An append to " + name + " expected position " + position(expected) + ", found "
              + records.size(), null);
    }
    long position = land(record);
    interceptor.afterLanding(Request.APPEND);
    failAfter(expected == null && fault == WriteFault.LANDED_AS_CONFLICT
        ? WriteFault.LOST_REPLY : fault, Request.APPEND);
    return new Position(position);
  }

  /**
   * Lands every append delayed so far, in the order they were sent, as a timed-out request does
   * when it reaches the log after all. Returns how many landed.
   */
  public synchronized int deliverDelayed() {
    List<byte[]> pending = new ArrayList<>(delayed);
    delayed.clear();
    pending.forEach(this::land);
    return pending.size();
  }

  /** Appends delayed and not yet delivered. */
  public synchronized int delayedCount() {
    return delayed.size();
  }

  private long land(byte[] record) {
    records.add(record);
    history.add(new Write(Request.APPEND, records.size(), record));
    return records.size();
  }

  @Override
  public synchronized void putCheckpoint(LogTail through, byte[] checkpoint) {
    WriteFault fault = interceptor.beforeWrite(Request.PUT_CHECKPOINT, checkpoint);
    failBefore(fault, Request.PUT_CHECKPOINT);
    if (!through.exists()) {
      byte[] first = checkpoints.get(0L);
      if (first == null) {
        checkpoints.put(0L, checkpoint);
        history.add(new Write(Request.PUT_CHECKPOINT, 0L, checkpoint));
      } else if (!Arrays.equals(first, checkpoint)) {
        throw new LogConflictException(name + " was created by another writer", null);
      }
    } else {
      long position = position(through);
      if (position > records.size()) {
        throw new IllegalArgumentException(
            "No position " + position + " in " + name + " of " + records.size() + " records");
      }
      byte[] existing = checkpoints.get(position);
      if (existing == null) {
        checkpoints.put(position, checkpoint);
        history.add(new Write(Request.PUT_CHECKPOINT, position, checkpoint));
      } else if (!Arrays.equals(existing, checkpoint)) {
        String violation = "two checkpoints at position " + position + " of " + name + " differ";
        violations.add(violation);
        throw new IllegalStateException("I11: " + violation);
      }
    }
    interceptor.afterLanding(Request.PUT_CHECKPOINT);
    failAfter(fault, Request.PUT_CHECKPOINT);
  }

  @Override
  public synchronized void trim(LogTail through) {
    WriteFault fault = interceptor.beforeWrite(Request.TRIM, null);
    checkExists();
    failBefore(fault, Request.TRIM);
    long position = position(through);
    if (checkpoints.ceilingKey(position) == null) {
      throw new IllegalStateException(
          "P5: no checkpoint of " + name + " covers position " + position);
    }
    if (position > trimmed) {
      trimmed = position;
      history.add(new Write(Request.TRIM, position, null));
    }
    interceptor.afterLanding(Request.TRIM);
    failAfter(fault, Request.TRIM);
  }

  private void failBefore(WriteFault fault, Request request) {
    switch (fault) {
      case NOT_SENT:
        throw new LogNotSentException("A " + request + " to " + name + " was not sent", null);
      case THROTTLED:
        throw new LogThrottledException(name + " throttled a " + request, null);
      case FAIL_BEFORE:
        throw new UncheckedIOException(new IOException("A " + request + " to " + name + " failed"));
      case DELAYED:
        if (request != Request.APPEND) {
          throw new UncheckedIOException(
              new IOException("A " + request + " to " + name + " failed"));
        }
        return;
      default:
        return;
    }
  }

  private void failAfter(WriteFault fault, Request request) {
    switch (fault) {
      case LOST_REPLY:
        throw new UncheckedIOException(
            new IOException("The reply to a " + request + " to " + name + " was lost"));
      case LANDED_THROTTLED:
        throw new LogThrottledException(name + " throttled a " + request + " that landed", null);
      case LANDED_AS_CONFLICT:
        throw new LogConflictException(
            "A retry of a " + request + " to " + name + " that landed lost a race", null);
      default:
        return;
    }
  }

  // ============================================================
  // Reads
  // ============================================================

  @Override
  public synchronized LogView read() {
    ReadShape shape = beforeRead(Request.READ);
    checkExists();
    long checkpoint = choose(usable(records.size()), shape);
    return view(checkpoint, Math.max(checkpoint, records.size() - shape.lag), true, shape.gap);
  }

  @Override
  public synchronized LogView readThrough(Receipt receipt, Duration wait) {
    ReadShape shape = interceptor.beforeRead(Request.READ_THROUGH);
    if (shape.fail) {
      throw new LogLagException("A read through a write to " + name + " timed out");
    }
    long position = ((Position) receipt).position;
    List<Long> before = usable(shape.pastRecord ? records.size() : position - 1);
    if (before.isEmpty()) {
      throw new TrimmedException(name + " trimmed the record at position " + position);
    }
    long checkpoint = choose(before, shape);
    return view(checkpoint, Math.max(position, records.size() - shape.lag), true, shape.gap);
  }

  @Override
  public synchronized LogView readAfter(LogTail since) {
    ReadShape shape = beforeRead(Request.READ_AFTER);
    checkExists();
    long position = position(since);
    if (position < trimmed) {
      throw new TrimmedException(name + " trimmed the records after position " + position);
    }
    return view(position, Math.max(position, records.size() - shape.lag), false, shape.gap);
  }

  @Override
  public synchronized LogTail probe() {
    beforeRead(Request.PROBE);
    return checkpoints.isEmpty()
        ? new Tail(0, false, 0)
        : new Tail(records.size(), true, bytes(checkpoints.lastKey(), records.size()));
  }

  private ReadShape beforeRead(Request request) {
    ReadShape shape = interceptor.beforeRead(request);
    if (shape.fail) {
      throw new UncheckedIOException(new IOException("A " + request + " of " + name + " failed"));
    }
    return shape;
  }

  /** The positions of the checkpoints a read may start from, at or before {@code atMost}. */
  private List<Long> usable(long atMost) {
    return atMost < trimmed
        ? List.of()
        : new ArrayList<>(checkpoints.subMap(trimmed, true, atMost, true).keySet());
  }

  private static long choose(List<Long> usable, ReadShape shape) {
    return usable.get(Math.max(0, usable.size() - 1 - shape.olderCheckpoints));
  }

  /**
   * The view of the checkpoint at {@code from} (if {@code withCheckpoint}) and the records after
   * it through {@code to}, less the first {@code gap}: its records start after position {@code
   * from + gap}, or at the tail if that is past it.
   */
  private LogView view(long from, long to, boolean withCheckpoint, int gap) {
    long start = Math.min(from + gap, to);
    List<byte[]> after = List.copyOf(records.subList((int) start, (int) to));
    Optional<byte[]> checkpoint =
        withCheckpoint ? Optional.of(checkpoints.get(from)) : Optional.empty();
    Tail tail = new Tail(to, true, bytes(checkpoints.floorKey(to), to));
    byte[] follows = encodePosition(start);
    return new LogView() {
      @Override
      public LogTail tail() {
        return tail;
      }

      @Override
      public Optional<byte[]> checkpoint() {
        return checkpoint;
      }

      @Override
      public List<byte[]> records() {
        return after;
      }

      @Override
      public byte[] start() {
        return follows.clone();
      }
    };
  }

  /**
   * A position as this log encodes it ({@link LogTail#position}, {@code LogCheckpoint.through}):
   * the number of records through it, a big-endian int64.
   */
  public static byte[] encodePosition(long position) {
    return java.nio.ByteBuffer.allocate(8).putLong(position).array();
  }

  /** The size of a read from the checkpoint at {@code from} through position {@code to}. */
  private long bytes(long from, long to) {
    long bytes = checkpoints.get(from).length;
    for (int i = (int) from; i < to; i++) {
      bytes += records.get(i).length;
    }
    return bytes;
  }

  private void checkExists() {
    if (checkpoints.isEmpty()) {
      throw new IllegalStateException(name + " does not exist");
    }
  }

  private long position(LogTail tail) {
    if (!(tail instanceof Tail) || ((Tail) tail).log() != this) {
      throw new IllegalArgumentException("Not a tail of " + name + ": " + tail);
    }
    return ((Tail) tail).position;
  }

  // ============================================================
  // Pages
  // ============================================================

  @Override
  public PageStore pages() {
    return storesPages ? pageStore : null;
  }

  /** The pages of the committed set, in a map, as hooks let them be. */
  private final class MemoryPages implements PageStore {
    @Override
    public void put(String name, byte[] page) {
      synchronized (InMemorySharedLog.this) {
        PageFault fault = interceptor.beforePage(Request.PUT_PAGE, name);
        failPage(fault, Request.PUT_PAGE, name, false);
        byte[] existing = pageObjects.get(name);
        if (existing != null && !Arrays.equals(existing, page)) {
          String violation = "two puts of page " + name + " of " + InMemorySharedLog.this.name
              + " differ";
          violations.add(violation);
          throw new IllegalStateException(violation);
        }
        pageObjects.put(name, page.clone());
        history.add(new Write(Request.PUT_PAGE, records.size(), page.clone()));
        failPage(fault, Request.PUT_PAGE, name, true);
      }
    }

    @Override
    public Optional<byte[]> get(String name) {
      synchronized (InMemorySharedLog.this) {
        PageFault fault = interceptor.beforePage(Request.GET_PAGE, name);
        if (fault == PageFault.MISSING) {
          return Optional.empty();
        }
        failPage(fault == PageFault.LOST_REPLY ? PageFault.FAIL : fault, Request.GET_PAGE, name,
            false);
        byte[] page = pageObjects.get(name);
        return page == null ? Optional.empty() : Optional.of(page.clone());
      }
    }

    @Override
    public void delete(String name) {
      synchronized (InMemorySharedLog.this) {
        PageFault fault = interceptor.beforePage(Request.DELETE_PAGE, name);
        failPage(fault, Request.DELETE_PAGE, name, false);
        if (pageObjects.remove(name) != null) {
          history.add(new Write(Request.DELETE_PAGE, records.size(),
              name.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        }
        failPage(fault, Request.DELETE_PAGE, name, true);
      }
    }

    @Override
    public List<String> list() {
      synchronized (InMemorySharedLog.this) {
        PageFault fault = interceptor.beforePage(Request.LIST_PAGES, null);
        failPage(fault == PageFault.NONE ? fault : PageFault.FAIL, Request.LIST_PAGES, null,
            false);
        return new ArrayList<>(pageObjects.keySet());
      }
    }
  }

  /**
   * Throws as {@code fault} says, {@code landed} or not yet: FAIL (and MISSING, for a request other
   * than a get) before, LOST_REPLY after.
   */
  private void failPage(PageFault fault, Request request, String page, boolean landed) {
    boolean before = fault == PageFault.FAIL || fault == PageFault.MISSING;
    if (landed ? fault == PageFault.LOST_REPLY : before) {
      throw new UncheckedIOException(new IOException(
          (landed ? "The reply to a " : "A ") + request + (page != null ? " of " + page : "")
              + " of " + name + (landed ? " was lost" : " failed")));
    }
  }

  // ============================================================
  // What oracles see
  // ============================================================

  /** The pages stored now, by name. */
  public synchronized Map<String, byte[]> storedPages() {
    return Collections.unmodifiableMap(new TreeMap<>(pageObjects));
  }

  /** Every record ever appended, trimmed ones included, in log order. */
  public synchronized List<byte[]> records() {
    return List.copyOf(records);
  }

  /** Every checkpoint stored, by position, those a trim passed included. */
  public synchronized Map<Long, byte[]> checkpoints() {
    return Collections.unmodifiableMap(new TreeMap<>(checkpoints));
  }

  /** The log is trimmed through this position (0: nothing trimmed). */
  public synchronized long trimmedThrough() {
    return trimmed;
  }

  /** Every write that landed, in order. */
  public synchronized List<Write> history() {
    return List.copyOf(history);
  }

  /** The violations refused: two different checkpoints at one position (I11), or pages of one name. */
  public synchronized List<String> violations() {
    return List.copyOf(violations);
  }

  /** The position of {@code tail}, a tail of this log. */
  public synchronized long positionOf(LogTail tail) {
    return position(tail);
  }

  /** The position of the record {@code receipt} acknowledged. */
  public static long positionOf(Receipt receipt) {
    return ((Position) receipt).position;
  }

  /**
   * The end of a read: a position. A later tail continues an earlier one, since the log is never
   * replaced: a tail that does not continue another is behind it.
   */
  private final class Tail implements LogTail {
    private final long position;
    private final boolean exists;
    private final long bytes;

    Tail(long position, boolean exists, long bytes) {
      this.position = position;
      this.exists = exists;
      this.bytes = bytes;
    }

    InMemorySharedLog log() {
      return InMemorySharedLog.this;
    }

    @Override
    public boolean exists() {
      return exists;
    }

    @Override
    public long bytes() {
      return bytes;
    }

    @Override
    public boolean continues(LogTail earlier) {
      return earlier instanceof Tail
          && ((Tail) earlier).log() == InMemorySharedLog.this
          && position >= ((Tail) earlier).position;
    }

    @Override
    public byte[] position() {
      return encodePosition(position);
    }

    @Override
    public String toString() {
      return name + "@" + position;
    }
  }

  private static final class Position implements Receipt {
    private final long position;

    Position(long position) {
      this.position = position;
    }

    @Override
    public String toString() {
      return "@" + position;
    }
  }
}
