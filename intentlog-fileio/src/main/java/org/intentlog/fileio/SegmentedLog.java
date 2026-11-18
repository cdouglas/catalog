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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.exceptions.NotFoundException;
import org.intentlog.Capabilities;
import org.intentlog.LogConflictException;
import org.intentlog.LogFullException;
import org.intentlog.LogLagException;
import org.intentlog.LogNotSentException;
import org.intentlog.LogTail;
import org.intentlog.LogThrottledException;
import org.intentlog.LogView;
import org.intentlog.LogVoidException;
import org.intentlog.PageStore;
import org.intentlog.Receipt;
import org.intentlog.SharedLog;
import org.intentlog.TrimmedException;
import org.intentlog.segments.SegmentFrames;
import org.intentlog.segments.SegmentFrames.Frame;
import org.intentlog.segments.SegmentFrames.FrameCheckException;
import org.intentlog.segments.SegmentPosition;
import org.intentlog.segments.SegmentStore;
import org.intentlog.wire.InstallSegment;
import org.intentlog.wire.LogEnvelope;
import org.intentlog.wire.RetiredSegment;

/**
 * docs/LOG_SEGMENTS.md's log: a catalog object, which holds the checkpoint and installs segments,
 * and segments in a {@link SegmentStore}, which writers append to blind.
 *
 * <ul>
 *   <li>The catalog object is an {@link AtomicFileLog} in format 4 ({@link
 *       AtomicFileLog#segmented}): the checkpoint, the position it covers (a {@link
 *       SegmentPosition}: a checkpoint is opaque to its log), then {@code InstallSegment}
 *       records, each of the next ordinal and each
 *       setting its segment's record limit, then {@code RetiredSegment} entries. An Install is a
 *       conditional append; a checkpoint is a CAS replace that restates the Installs from its
 *       segment on and retires those before.
 *   <li>A segment ends at its limit-th record, or earlier at its first seal frame: both in the
 *       segment's own order, so every reader of it reads the same records (D1). Its records count
 *       only after every earlier segment has ended. Several segments may be open at once (D11).
 *   <li>An append goes to the first segment this log knows open, blind. The reply's block count
 *       is the frame's index: past the limit, the append is void with no read, and it goes to the
 *       next segment, installing one if there is none. Near the limit it installs the next ahead,
 *       and (with {@link Options#copies}) sends a copy there too.
 *   <li>A read through an append takes its copies in order: each needs the segments before its own
 *       ended, and seals one that hasn't; the first copy the log holds is the record, and if none
 *       does, the append is void, a {@link LogConflictException} (D4).
 *   <li>A segment's blob is {@code <prefix>/<log id>/<name>}: the log's id is its checkpoint's, so
 *       no entry carries it, and the logs that share a prefix keep their segments apart. The prefix
 *       is the catalog object's: its creator's, recorded after the checkpoint's position unless it
 *       is the default every client derives from the catalog object's location (D13), so a client
 *       configured with another, or none, keeps the log's segments where they are.
 *   <li>A retired segment is deleted by the name its Retired entry holds once its TTL has passed,
 *       by a checkpoint, which drops the entry only once the segment is gone, and by the sweep
 *       ({@link #trim}). The sweep also lists this log's segments for unnamed blobs, and deletes
 *       those whose ordinal is at most the newest Install's: no Install of it can land any more.
 *       It never lists another log's.
 * </ul>
 *
 * <p>To the runtime this is a log of blind appends and separate checkpoints. The store sends each
 * request once; this log retries reads itself, and leaves the retries of writes to the runtime.
 */
public final class SegmentedLog implements SharedLog {

  /**
   * How the log behaves.
   *
   * @param limit K, the record limit each Install sets (0: the store's block limit)
   * @param readAttempts how many times a read starts again before the log could not be read
   * @param sendAttempts how many segments one append, Install or checkpoint tries
   * @param retireTtlMs how long a retired segment stays before it is deleted
   * @param clockMs the clock a retirement is stamped with, and the TTL measured on
   * @param copies whether an append near its segment's limit also sends a copy to the next
   * @param aheadMargin how near the limit an append installs the next segment ahead (0: 1% of K)
   */
  public record Options(
      long limit,
      int readAttempts,
      int sendAttempts,
      long retireTtlMs,
      LongSupplier clockMs,
      boolean copies,
      long aheadMargin) {

    public static Options defaults() {
      return new Options(0, 8, 8, 60_000L, System::currentTimeMillis, false, 0);
    }

    public Options withLimit(long k) {
      return new Options(k, readAttempts, sendAttempts, retireTtlMs, clockMs, copies, aheadMargin);
    }

    public Options withRetireTtlMs(long ttl) {
      return new Options(limit, readAttempts, sendAttempts, ttl, clockMs, copies, aheadMargin);
    }

    public Options withClock(LongSupplier c) {
      return new Options(limit, readAttempts, sendAttempts, retireTtlMs, c, copies, aheadMargin);
    }

    public Options withCopies(boolean c) {
      return new Options(limit, readAttempts, sendAttempts, retireTtlMs, clockMs, c, aheadMargin);
    }

    public Options withAheadMargin(long m) {
      return new Options(limit, readAttempts, sendAttempts, retireTtlMs, clockMs, copies, m);
    }
  }

  private static final System.Logger LOG = System.getLogger(SegmentedLog.class.getName());

  private final AtomicFileLog catalog;
  private final SegmentStore store;
  /** Where a log this creates keeps its segments; null: the default. */
  private final String configured;
  /**
   * Where the segments of a log whose catalog object records no prefix are, which every client of
   * the catalog object derives alike from its location (D13); null if none is derived.
   */
  private final String defaultPrefix;
  private final Options options;
  private final long limit;
  private final long maxRecordBytes;

  /** The newest catalog object this log read or wrote; null until it reads one. */
  private volatile Cat cached = null;

  /** The newest whole view this log built, extended by the next read through an append. */
  private volatile View newest = null;

  /** Where segments this log read to their ends ended, by name. */
  private final Map<String, SegmentPosition> ends = new ConcurrentHashMap<>();

  /** Segments this log sends no more appends to (ended, or full), by name. */
  private final Set<String> skip = ConcurrentHashMap.newKeySet();

  /** Whether a read through an append of this log found it void: the log is behind. */
  private volatile boolean behind = false;

  /** Whether this log warned that its catalog object's prefix is not the one it was given. */
  private volatile boolean warned = false;

  /**
   * A segmented log whose catalog object is {@code catalog} (format 4) and whose segments are
   * {@code store}'s, named under {@code <prefix>/<log id>/}: a log this creates keeps them under
   * {@code prefix}, which its catalog object records; a log that exists keeps them where its
   * catalog object says.
   */
  public SegmentedLog(AtomicFileLog catalog, SegmentStore store, String prefix, Options options) {
    this(catalog, store, prefix, null, options);
  }

  /**
   * A segmented log whose catalog object is {@code catalog} (format 4) and whose segments are
   * {@code store}'s, named under {@code <prefix>/<log id>/}, the prefix the catalog object records
   * (D13). A log this creates keeps them under {@code prefix}, or {@code defaultPrefix} if that is
   * null, and records the prefix unless it is the default. A catalog object that records none
   * keeps them under {@code defaultPrefix}, which every client of the catalog object must derive
   * alike from its location.
   *
   * @param prefix where a log this creates keeps its segments; null: the default
   * @param defaultPrefix where a log whose catalog object records no prefix keeps them; null if
   *     there is no default for this catalog object, which then must record its prefix
   */
  public SegmentedLog(AtomicFileLog catalog, SegmentStore store, String prefix,
      String defaultPrefix, Options options) {
    this.catalog = catalog;
    this.store = store;
    this.configured = trimSlash(prefix);
    this.defaultPrefix = trimSlash(defaultPrefix);
    this.options = options;
    this.limit = options.limit() > 0 ? options.limit() : store.maxBlocks();
    if (limit > store.maxBlocks()) {
      // SG8: a segment must have room for its seal frame until it ends
      throw new IllegalArgumentException("A record limit of " + limit + " is past the store's "
          + store.maxBlocks() + " blocks a segment");
    }
    // SG8: before its end a segment holds at most K - 1 records, and its K is its installer's,
    // which may be any writer's, up to the store's block limit B. So the largest frame is sized for
    // K = B, whatever this log's K: B - 1 of them and a seal fit a segment, so K - 1 do for every K.
    // Sized by this log's own K, a writer with a small K sent records that filled a segment of a
    // larger K before its end, with no room for a seal: it never ended (Bug 115 (segment that
    // cannot end)).
    long blocks = store.maxBlocks();
    long room = store.maxSegmentBytes() - SegmentFrames.SEAL;
    long frame = Math.min(store.maxBlockBytes(), blocks > 1 ? room / (blocks - 1) : room);
    this.maxRecordBytes = frame - SegmentFrames.MAX_OVERHEAD;
    if (maxRecordBytes < 1) {
      throw new IllegalArgumentException("Segments of " + store.maxSegmentBytes() + " bytes hold "
          + blocks + " records of no size");
    }
  }

  private static String trimSlash(String prefix) {
    return prefix != null && prefix.endsWith("/") ? prefix.substring(0, prefix.length() - 1)
        : prefix;
  }

  @Override
  public String toString() {
    return "segmented log " + catalog + " (segments at " + segmentPrefix() + ")";
  }

  /**
   * Where this log's segments are: under the prefix its catalog object records (or the default),
   * once this log has read it; before, where a log it creates would keep them (null if nowhere).
   */
  public String segmentPrefix() {
    Cat c = cached;
    return c != null ? c.prefix() : configured != null ? configured : defaultPrefix;
  }

  @Override
  public Capabilities capabilities() {
    return new Capabilities(true, false, false, true, maxRecordBytes);
  }

  @Override
  public PageStore pages() {
    return catalog.pages();
  }

  /** K: the record limit this log's Installs set. */
  public long limit() {
    return limit;
  }

  /**
   * This log as another process holding what it holds: its catalog object and the view it kept,
   * for a test's oracle to read with while this log's caches stay as they are.
   */
  SegmentedLog copy() {
    SegmentedLog c = new SegmentedLog(catalog, store, configured, defaultPrefix, options);
    c.cached = cached;
    c.newest = newest;
    return c;
  }

  // ============================================================
  // Names
  // ============================================================

  /**
   * Where the segment {@code name} of the log {@code logId} is in its store under {@code prefix}:
   * {@code <prefix>/<log id>/<name>}. The id is the log's checkpoint's ({@code
   * LogCheckpoint.log_id}), which every catalog object of the log holds, so an entry names its
   * segment without it; and the logs whose segments share a prefix keep them apart, each sweep
   * listing only its own.
   */
  static String path(String prefix, UUID logId, String name) {
    return prefix + "/" + logId + "/" + name;
  }

  /** Where an Install's segment is: at the location it names, else under the log's prefix. */
  private String path(Seg seg) {
    return path(seg.prefix(), seg.logId(), seg.name(), seg.location());
  }

  /**
   * Where segment {@code name} of the log {@code logId} is: at {@code location}, a URL in the
   * store, if one is given (a segment a migration left at another prefix keeps its place), else
   * under the log's prefix.
   */
  private String path(String prefix, UUID logId, String name, String location) {
    if (location.isEmpty()) {
      return path(prefix, logId, name);
    }
    try {
      return store.nameAt(location);
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(this + "'s catalog object names segment " + name + " at "
          + location + ", which its store cannot reach", e);
    }
  }

  /** A segment's name: {@code seg-<ordinal %020d>-<nonce %016x>}. */
  private static final Pattern NAME = Pattern.compile("seg-([0-9]{20})-[0-9a-f]{16}");

  private static String segmentName(long ordinal) {
    return String.format("seg-%020d-%016x", ordinal, ThreadLocalRandom.current().nextLong());
  }

  /** The ordinal a segment's name holds; -1 if it is no segment's name. */
  static long ordinalOf(String name) {
    Matcher m = NAME.matcher(name);
    if (!m.matches()) {
      return -1;
    }
    try {
      return Long.parseLong(m.group(1));
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  /**
   * The ordinal of the segment at {@code path}, a name in the store, if it is one of the log
   * {@code logId}'s under {@code prefix}; -1 if it is not: another log's, or no segment's.
   */
  static long ordinalAt(String prefix, UUID logId, String path) {
    String own = path(prefix, logId, "");
    return path.startsWith(own) ? ordinalOf(path.substring(own.length())) : -1;
  }

  // ============================================================
  // The catalog object
  // ============================================================

  /**
   * A segment an Install of the log {@code logId} names. The Install holds all but the log's id,
   * which its catalog object's checkpoint holds, and the prefix, which the catalog object records
   * once (or the default).
   */
  record Seg(UUID logId, String prefix, long ordinal, String name, String location, long limit) {
    byte[] install() {
      return LogEnvelope.encodeInstallSegment(new InstallSegment(ordinal, name, location, limit));
    }
  }

  /**
   * The catalog object as read: its view, the log's id, the checkpoint and its position, the
   * prefix it records (null: none) and the segments' prefix (it, or the default; null if neither),
   * the segments installed from the checkpoint's on (contiguous), and the retired ones.
   */
  record Cat(LogView view, UUID logId, byte[] checkpoint, SegmentPosition ck, String recorded,
      String prefix, List<Seg> segs, List<RetiredSegment> retired) {

    Seg seg(long ordinal) {
      long i = ordinal - segs.get(0).ordinal();
      return i >= 0 && i < segs.size() ? segs.get((int) i) : null;
    }

    Seg last() {
      return segs.get(segs.size() - 1);
    }

    RetiredSegment retiredAs(long ordinal, String name) {
      for (RetiredSegment r : retired) {
        if (r.ordinal() == ordinal && r.name().equals(name)) {
          return r;
        }
      }
      return null;
    }
  }

  /** The catalog object's replay, its segments under the prefix it records alone. */
  static Cat parse(LogView v) {
    return parse(v, null);
  }

  /**
   * The catalog object's replay: the first record is the position the checkpoint covers; the
   * next, if it is one, the prefix the segments are named under (else they are under {@code
   * defaultPrefix}: D13); an Install counts only if it is of the next ordinal (the first one, of
   * the checkpoint's); any other record frame but a Retired entry is void.
   */
  static Cat parse(LogView v, String defaultPrefix) {
    byte[] checkpoint = v.checkpoint().orElseThrow(
        () -> new IllegalStateException("A catalog object without a checkpoint"));
    LogEnvelope.Checkpoint decoded = LogEnvelope.decodeCheckpoint(checkpoint);
    List<byte[]> frames = v.records();
    byte[] through = frames.isEmpty() ? null
        : LogEnvelope.decodeRecord(frames.get(0)).checkpointThrough();
    if (through == null) {
      throw new IllegalStateException("A catalog object names no position for its checkpoint");
    }
    SegmentPosition ck = SegmentPosition.decode(through);
    String recorded = null;
    List<Seg> segs = new ArrayList<>();
    List<RetiredSegment> retired = new ArrayList<>();
    for (int f = 1; f < frames.size(); f++) {
      LogEnvelope.Record r = LogEnvelope.decodeRecord(frames.get(f));
      if (f == 1 && r.segmentPrefix() != null) {
        recorded = trimSlash(r.segmentPrefix());
      } else if (r.installSegment() != null) {
        InstallSegment i = r.installSegment();
        long next = segs.isEmpty() ? ck.ordinal() : segs.get(segs.size() - 1).ordinal() + 1;
        if (i.ordinal() == next) {
          segs.add(new Seg(decoded.logId(), recorded != null ? recorded : defaultPrefix,
              i.ordinal(), i.name(), i.location(), i.limit()));
        }
      } else if (r.retiredSegment() != null) {
        retired.add(r.retiredSegment());
      }
    }
    if (segs.isEmpty()) {
      throw new IllegalStateException("A catalog object installs no segment at its checkpoint's");
    }
    return new Cat(v, decoded.logId(), checkpoint, ck, recorded,
        recorded != null ? recorded : defaultPrefix, List.copyOf(segs), List.copyOf(retired));
  }

  /**
   * The catalog object {@code v} as this log reads it: its segments under the prefix it records,
   * else this log's default. Fails if it records none and this log has no default: the log's
   * creator derived one this client does not. Warns once if the prefix this log was given is
   * another: the catalog object's wins.
   */
  private Cat parseCat(LogView v) {
    Cat c = parse(v, defaultPrefix);
    if (c.prefix() == null) {
      throw new IllegalStateException("The catalog object " + catalog + " records no prefix for "
          + "its segments, and this client derives no default from its location: open the "
          + "catalog at the location its creator did");
    }
    if (configured != null && !configured.equals(c.prefix()) && !warned) {
      warned = true;
      LOG.log(System.Logger.Level.WARNING, "The catalog object " + catalog + " keeps its "
          + "segments under " + c.prefix() + "; the prefix this client was given, " + configured
          + ", is ignored");
    }
    return c;
  }

  /** Keeps {@code c} if it is not older than the catalog object held; returns the newer. */
  private synchronized Cat adopt(Cat c) {
    Cat was = cached;
    if (was == null || !was.logId().equals(c.logId()) || c.ck().compareTo(was.ck()) > 0
        || (c.ck().compareTo(was.ck()) == 0
            && c.last().ordinal() >= was.last().ordinal())) {
      cached = c;
      return c;
    }
    return was.ck().compareTo(c.ck()) == 0 ? was : c;
  }

  /** A fresh read of the catalog object (a tail read after the one held); null if none exists. */
  private Cat readCat() {
    Cat held = cached;
    LogView v;
    try {
      v = held == null ? catalog.read() : catalog.readAfter(held.view().tail());
    } catch (NotFoundException e) {
      return null;
    }
    Cat c = parseCat(v);
    adopt(c);
    return c;
  }

  private Cat existing() {
    Cat c = readCat();
    if (c == null) {
      throw new IllegalStateException(this + " does not exist");
    }
    return c;
  }

  private Cat held() {
    Cat c = cached;
    return c != null ? c : existing();
  }

  // ============================================================
  // Reads
  // ============================================================

  /** What a walk over the segments found. */
  private static final class Walk {
    final List<byte[]> records = new ArrayList<>();
    final List<Long> ordinals = new ArrayList<>();
    final List<Long> offsets = new ArrayList<>();
    SegmentPosition end;
    long bytes = 0;
    Cat cat;
    // the segment the walk stopped in, and whether it stopped because that segment is open (else
    // it ended, and no successor is installed)
    long at;
    boolean open;

    int indexOf(long ordinal, long offset) {
      for (int i = ordinals.size() - 1; i >= 0; i--) {
        if (ordinals.get(i) == ordinal && offsets.get(i) == offset) {
          return i;
        }
      }
      return -1;
    }
  }

  /**
   * The records from {@code from} on: each segment to its end (its limit-th record, or its first
   * seal frame), then the next, through the first that hasn't ended. Reads the catalog object
   * again for a segment {@code c} doesn't name. Null if a segment is gone, or read short of where
   * the walk starts in it: the read starts again.
   */
  private Walk walk(Cat c, SegmentPosition from) {
    Walk w = new Walk();
    w.end = from;
    w.cat = c;
    long o = from.ordinal();
    long count = from.records();
    long offset = from.offset();
    while (true) {
      Seg seg = c.seg(o);
      if (seg == null) {
        Cat fresh = existing();
        if (!fresh.logId().equals(c.logId())) {
          return null;
        }
        seg = fresh.seg(o);
        if (seg == null) {
          if (o > fresh.last().ordinal() && o > from.ordinal()) {
            w.cat = fresh;
            w.at = o - 1;
            w.open = false; // it ended, and no successor is installed yet
            return w;
          }
          return null; // retired under the walk
        }
        c = fresh;
        w.cat = fresh;
      }
      boolean ended = count >= seg.limit();
      SegmentPosition segEnd = new SegmentPosition(o, count, offset);
      if (!ended) {
        SegmentStore.Segment s = store.read(path(seg), offset);
        if (s == null || s.length() < offset) {
          return null;
        }
        List<Frame> frames = SegmentFrames.parse(s.bytes(), s.bytes().length, offset, o);
        for (Frame f : frames) {
          if (count >= seg.limit() || f.seal()) {
            ended = true;
            break;
          }
          w.records.add(f.body());
          w.ordinals.add(o);
          w.offsets.add(f.offset());
          w.bytes += f.body().length;
          count++;
          segEnd = new SegmentPosition(o, count, f.end());
          w.end = segEnd;
        }
        ended |= count >= seg.limit();
        if (!ended && s.blocks() >= store.maxBlocks()) {
          throw new IllegalStateException("SG8: " + path(seg) + " holds " + s.blocks()
              + " blocks, " + count + " of them records, and has not ended");
        }
      }
      if (!ended) {
        w.at = o;
        w.open = true;
        return w;
      }
      ends.put(seg.name(), segEnd);
      skip.add(seg.name());
      o++;
      count = 0;
      offset = 0;
    }
  }

  @Override
  public LogView read() {
    RuntimeException last = null;
    for (int attempt = 0; attempt < options.readAttempts(); attempt++) {
      try {
        Cat c = existing();
        Walk w = walk(c, c.ck());
        if (w != null) {
          View v = whole(c).extend(viewOf(w, c.logId(), c.ck(), c.checkpoint().length));
          keep(v);
          return v;
        }
      } catch (FrameCheckException e) {
        last = e;
      }
    }
    throw unreadable(last);
  }

  @Override
  public LogView readAfter(LogTail since) {
    SegTail t = tailOf(since);
    if (!t.exists) {
      return read();
    }
    RuntimeException last = null;
    for (int attempt = 0; attempt < options.readAttempts(); attempt++) {
      try {
        Cat c = held();
        if (!c.logId().equals(t.logId)) {
          c = existing();
          if (!c.logId().equals(t.logId)) {
            throw new TrimmedException(this + " is another log than the one read at " + t);
          }
        }
        if (c.ck().compareTo(t.position) > 0) {
          return read(); // a checkpoint covers the tail
        }
        if (c.seg(t.position.ordinal()) == null) {
          c = existing();
          if (c.seg(t.position.ordinal()) == null || c.ck().compareTo(t.position) > 0) {
            return read();
          }
        }
        Walk w = walk(c, t.position);
        if (w == null) {
          Cat fresh = existing();
          if (fresh.seg(t.position.ordinal()) == null) {
            throw new TrimmedException(this + " trimmed the records after " + t);
          }
          continue;
        }
        return viewOf(w, t.logId, t.position, t.bytes);
      } catch (FrameCheckException e) {
        last = e;
      }
    }
    throw unreadable(last);
  }

  /** A view of the checkpoint of {@code c} and no records. */
  private View whole(Cat c) {
    SegTail tail = new SegTail(where(), c.logId(), true, c.ck(), c.checkpoint().length);
    return new View(tail, c.checkpoint(), c.ck(), List.of(), new long[0], new long[0]);
  }

  /** A view of the records {@code w} read after {@code from}, a tail of {@code bytes}. */
  private View viewOf(Walk w, UUID logId, SegmentPosition from, long bytes) {
    long[] ordinals = new long[w.ordinals.size()];
    long[] offsets = new long[ordinals.length];
    for (int i = 0; i < ordinals.length; i++) {
      ordinals[i] = w.ordinals.get(i);
      offsets[i] = w.offsets.get(i);
    }
    SegTail tail = new SegTail(where(), logId, true, w.end, bytes + w.bytes);
    return new View(tail, null, from, List.copyOf(w.records), ordinals, offsets);
  }

  private void keep(View v) {
    View was = newest;
    if (v.checkpoint != null && (was == null || !was.tail.logId.equals(v.tail.logId)
        || was.start.compareTo(v.start) < 0
        || (was.start.compareTo(v.start) == 0
            && was.tail.position.compareTo(v.tail.position) <= 0))) {
      newest = v;
    }
  }

  private UncheckedIOException unreadable(RuntimeException cause) {
    return new UncheckedIOException(new IOException(this + " could not be read in "
        + options.readAttempts() + " attempts", cause));
  }

  // ============================================================
  // Appends
  // ============================================================

  private long margin() {
    return options.aheadMargin() > 0 ? options.aheadMargin() : Math.max(1, limit / 100);
  }

  /** The first segment from where this log's newest view ends that it doesn't know ended. */
  private long firstOpen(Cat c) {
    View v = newest;
    long o = v != null && v.tail.logId.equals(c.logId()) && c.seg(v.tail.position.ordinal()) != null
        ? v.tail.position.ordinal() : c.ck().ordinal();
    while (c.seg(o) != null && skip.contains(c.seg(o).name())) {
      o++;
    }
    return o;
  }

  @Override
  public Receipt append(byte[] record, LogTail expected) {
    if (expected != null) {
      throw new IllegalArgumentException(this + " appends blind");
    }
    if (record.length > maxRecordBytes) {
      throw new LogNotSentException("A record of " + record.length + " bytes is larger than "
          + this + " stores", null);
    }
    Cat c = held();
    long o = firstOpen(c);
    int ended = behind ? 2 : 0;
    behind = false;
    for (int attempt = 0; attempt < options.sendAttempts(); attempt++) {
      if (ended >= 2) {
        // this log is behind: past the segments it knew, go to the newest a fresh read installs
        c = existing();
        o = Math.max(o, c.last().ordinal());
        ended = 0;
      }
      Seg seg = c.seg(o);
      if (seg == null) {
        if (o < c.segs().get(0).ordinal()) {
          o = firstOpen(c);
          continue;
        }
        c = install(c, o);
        if (c.seg(o) == null) {
          o = firstOpen(c);
        }
        continue;
      }
      if (skip.contains(seg.name())) {
        o++;
        continue;
      }
      byte[] frame = SegmentFrames.record(record, o);
      SegmentStore.Appended a;
      try {
        a = store.append(path(seg), frame);
      } catch (SegmentStore.Full e) {
        skip.add(seg.name()); // nothing landed; the segment ends without this record
        o++;
        ended++;
        continue;
      } catch (SegmentStore.NotFound e) {
        // a stale catalog object: the segment was retired and swept. Never create it (SG4).
        Cat fresh = existing();
        Seg again = fresh.seg(o);
        if (again != null && again.name().equals(seg.name())) {
          throw new IllegalStateException("SG4: the catalog object names " + path(seg)
              + ", which does not exist", e);
        }
        c = fresh;
        o = Math.max(o, firstOpen(fresh));
        continue;
      } catch (SegmentStore.Throttled e) {
        throw new LogThrottledException(this + " throttled an append", e);
      } catch (SegmentStore.Refused e) {
        throw new LogNotSentException("The store refused an append to " + this, e);
      }
      if (a.blocks() > seg.limit()) {
        skip.add(seg.name()); // void by its reply: past the segment's limit
        o++;
        ended++;
        continue;
      }
      List<Copy> copies = new ArrayList<>();
      copies.add(new Copy(o, seg.name(), a.offset(), a.blocks(), frame.length,
          SegmentFrames.crcOf(frame)));
      if (a.blocks() + margin() > seg.limit()) {
        Cat next = c;
        if (c.seg(o + 1) == null) {
          try {
            next = install(c, o + 1); // ahead of need, so a rotation doesn't wait for it
          } catch (RuntimeException e) {
            // the next append that needs it installs it
          }
        }
        Seg after = next.seg(o + 1);
        if (options.copies() && after != null && !skip.contains(after.name())) {
          Copy copy = sendCopy(after, record);
          if (copy != null) {
            copies.add(copy);
          }
        }
      }
      return new SegReceipt(c.logId(), List.copyOf(copies), record);
    }
    throw new LogNotSentException(this + " found no segment to take a record in "
        + options.sendAttempts() + " sends", null);
  }

  /**
   * A copy of {@code record} in {@code seg}: its receipt, or null if nothing landed, or it is void
   * by its reply. A copy whose fate the store can't say makes the append's outcome unknown: it may
   * be the record, where the read through it can't see.
   */
  private Copy sendCopy(Seg seg, byte[] record) {
    byte[] frame = SegmentFrames.record(record, seg.ordinal());
    try {
      SegmentStore.Appended a = store.append(path(seg), frame);
      return a.blocks() > seg.limit() ? null : new Copy(seg.ordinal(), seg.name(), a.offset(),
          a.blocks(), frame.length, SegmentFrames.crcOf(frame));
    } catch (SegmentStore.Full | SegmentStore.NotFound | SegmentStore.Refused e) {
      return null;
    } catch (RuntimeException e) {
      throw new UncheckedIOException(new IOException("A copy's outcome in " + this
          + " is unknown", e));
    }
  }

  /**
   * Installs segment {@code o} after {@code c}'s last: creates it under a fresh name, then appends
   * its Install to the catalog object from {@code c}'s version. Returns a catalog object that
   * names {@code o}, this log's or another's, or a fresher one if the log moved on.
   */
  private Cat install(Cat c, long o) {
    if (c.last().ordinal() + 1 != o) {
      Cat fresh = existing();
      if (fresh.seg(o) != null || fresh.last().ordinal() + 1 != o) {
        return fresh;
      }
      c = fresh;
    }
    UUID logId = c.logId();
    String prefix = c.prefix();
    String name = segmentName(o);
    try {
      while (!store.create(path(prefix, logId, name))) {
        name = segmentName(o);
      }
    } catch (SegmentStore.Throttled e) {
      throw new LogThrottledException(this + " throttled the creation of a segment", e);
    } catch (RuntimeException e) {
      throw new LogNotSentException("A segment of " + this + " could not be created", e);
    }
    byte[] install = new Seg(logId, prefix, o, name, "", limit).install();
    for (int attempt = 0; attempt < options.sendAttempts(); attempt++) {
      try {
        Receipt r = catalog.append(install, c.view().tail());
        return adopt(parseCat(catalog.readThrough(r, Duration.ZERO)));
      } catch (LogThrottledException | LogNotSentException e) {
        throw e;
      } catch (LogFullException e) {
        throw new LogNotSentException(this + "'s catalog object takes no more appends", e);
      } catch (RuntimeException e) {
        // lost (a conflict), or of unknown outcome: the catalog object says
        Cat fresh = existing();
        Seg won = fresh.seg(o);
        boolean another = !fresh.logId().equals(logId);
        if (another || (won != null && !won.name().equals(name))) {
          // another's Install of o landed, or the object is another log's (whose segments are
          // under its own id), so no Install can name this blob: delete it, rather than leave it
          // to a sweep (one that cannot list leaves it for good)
          try {
            store.delete(path(prefix, logId, name));
          } catch (RuntimeException deleteFailed) {
            // the log's sweep deletes it, once it can (if the object is another log's, none does)
          }
        }
        if (another || won != null || fresh.last().ordinal() + 1 != o) {
          return fresh;
        }
        c = fresh; // a checkpoint changed the object: append again from it
      }
    }
    throw new LogNotSentException(this + " could not install segment " + o, null);
  }

  /** Appends a seal frame to {@code seg}: it ends there, if it hasn't. */
  private void seal(Seg seg) {
    try {
      store.append(path(seg), SegmentFrames.seal(seg.ordinal()));
    } catch (RuntimeException e) {
      // full (ended), gone (retired), or of unknown outcome: the next walk says
    }
  }

  // ============================================================
  // A commit's outcome
  // ============================================================

  @Override
  public LogView readThrough(Receipt receipt, Duration wait) {
    if (!(receipt instanceof SegReceipt r)) {
      throw new IllegalArgumentException("Not a receipt of " + this + ": " + receipt);
    }
    long deadline = System.nanoTime() + wait.toNanos();
    RuntimeException last = null;
    for (int attempt = 0; ; attempt++) {
      try {
        View v = through(r, attempt > 0);
        if (v != null) {
          keep(v);
          return v;
        }
      } catch (FrameCheckException e) {
        last = e;
      }
      if (attempt + 1 >= options.readAttempts() && System.nanoTime() > deadline) {
        throw new LogLagException("A read through " + r + " of " + this + " did not reach it"
            + (last != null ? ": " + last.getMessage() : ""));
      }
    }
  }

  /**
   * The view through the first copy of {@code r} the log holds. A copy the checkpoint covers is
   * void if its segment's retired end lies before it, else trimmed ({@link TrimmedException}).
   * Every copy void: {@link LogConflictException}. Null if the read must start again.
   */
  private View through(SegReceipt r, boolean fresh) {
    Cat c = fresh ? existing() : held();
    if (!c.logId().equals(r.logId)) {
      c = existing();
      if (!c.logId().equals(r.logId)) {
        throw new TrimmedException(this + " is another log than the one written at " + r);
      }
    }
    for (Copy cp : r.copies) {
      if (cp.ordinal < c.ck().ordinal()
          || (cp.ordinal == c.ck().ordinal() && cp.offset < c.ck().offset())) {
        RetiredSegment ret = c.retiredAs(cp.ordinal, cp.name);
        if (ret != null && ret.end().length == SegmentPosition.BYTES
            && cp.offset >= SegmentPosition.decode(ret.end()).offset()) {
          continue; // void: past its segment's end
        }
        throw new TrimmedException("The checkpoint of " + this + " covers " + cp);
      }
      View v = viewThrough(c, cp, r.record);
      if (v == null) {
        return null;
      }
      if (v != VOID) {
        return v;
      }
    }
    behind = true;
    throw new LogVoidException("Every copy of an append to " + this
        + " landed past its segment's end", null);
  }

  private static final View VOID = new View(null, null, null, List.of(), new long[0], new long[0]);

  /**
   * The view through {@code cp}, from {@code c}'s checkpoint; {@link #VOID} if its segment ended
   * before it; null if the read must start again.
   */
  private View viewThrough(Cat c, Copy cp, byte[] record) {
    View kept = newest;
    SegmentPosition k = kept == null ? null : kept.tail.position;
    boolean usable = kept != null && kept.tail.logId.equals(c.logId())
        && kept.start.equals(c.ck())
        && (k.ordinal() < cp.ordinal || (k.ordinal() == cp.ordinal && k.offset() <= cp.offset));
    if (usable && record != null && cp.index > 0) {
      SegmentPosition t = kept.tail.position;
      Seg seg = c.seg(cp.ordinal);
      if (seg != null && t.ordinal() == cp.ordinal && t.offset() == cp.offset
          && t.records() + 1 == cp.index && cp.index <= seg.limit()) {
        // The reply put the copy at the end of the view this log kept: nothing landed between,
        // and the segment hadn't ended, so the view through it is that view and the record
        SegTail tail = new SegTail(where(), c.logId(), true,
            new SegmentPosition(cp.ordinal, cp.index, cp.offset + cp.length),
            kept.tail.bytes + record.length);
        return kept.extend(new View(tail, null, t, List.of(record), new long[] {cp.ordinal},
            new long[] {cp.offset}));
      }
    }
    View acc = usable ? kept : whole(c);
    SegmentPosition from = acc.tail.position;
    for (int attempt = 0; attempt < options.readAttempts(); attempt++) {
      Walk w = walk(c, from);
      if (w == null) {
        return null;
      }
      c = w.cat;
      acc = acc.extend(viewOf(w, c.logId(), from, acc.tail.bytes));
      from = w.end;
      int i = w.indexOf(cp.ordinal, cp.offset);
      if (i >= 0) {
        byte[] frame = SegmentFrames.record(w.records.get(i), cp.ordinal);
        if (frame.length != cp.length || SegmentFrames.crcOf(frame) != cp.crc) {
          throw new IllegalStateException("P3: another record is at " + cp);
        }
        return acc;
      }
      if (w.at > cp.ordinal || (w.at == cp.ordinal && !w.open)) {
        return VOID; // its segment ended before it
      }
      if (w.at == cp.ordinal) {
        return null; // read short of the copy: read again
      }
      Seg open = c.seg(w.at);
      if (w.open && open != null) {
        seal(open); // an earlier segment hasn't ended: end it
      } else {
        c = existing(); // the copy's segment is installed past the catalog object held
      }
    }
    return null;
  }

  // ============================================================
  // Checkpoints, retirement, the sweep
  // ============================================================

  @Override
  public LogTail probe() {
    if (!catalog.probe().exists()) {
      return new SegTail(where(), null, false, SegmentPosition.START, 0);
    }
    View v = newest;
    if (v != null) {
      try {
        LogView after = readAfter(v.tail);
        return after.tail();
      } catch (TrimmedException e) {
        // read whole
      }
    }
    return read().tail();
  }

  @Override
  public void putCheckpoint(LogTail through, byte[] checkpoint) {
    SegTail t = tailOf(through);
    if (!t.exists) {
      create(checkpoint);
      return;
    }
    Set<String> deleted = new HashSet<>();
    for (int attempt = 0; attempt < options.sendAttempts(); attempt++) {
      Cat c = existing();
      if (c.logId().equals(t.logId) && t.position.equals(c.ck())) {
        if (!Arrays.equals(c.checkpoint(), checkpoint)) {
          throw new IllegalStateException("I11: two checkpoints at " + t.position + " of " + this
              + " differ");
        }
        return; // this checkpoint, again
      }
      if (!c.logId().equals(t.logId) || t.position.compareTo(c.ck()) <= 0
          || c.seg(t.position.ordinal()) == null) {
        return; // another log, or a newer checkpoint
      }
      long now = options.clockMs().getAsLong();
      List<byte[]> records = new ArrayList<>();
      records.add(LogEnvelope.encodeCheckpointThrough(t.position.encode()));
      if (c.recorded() != null) {
        records.add(LogEnvelope.encodeSegmentPrefix(c.recorded())); // the creator's, restated
      }
      for (Seg s : c.segs()) {
        if (s.ordinal() >= t.position.ordinal()) {
          records.add(s.install());
        }
      }
      for (RetiredSegment old : c.retired()) {
        // an entry is the only reference to its segment: drop it only once the segment is gone
        if (!expired(old, now) || !deleteRetired(c, old, deleted)) {
          records.add(LogEnvelope.encodeRetiredSegment(old));
        }
      }
      for (Seg s : c.segs()) {
        if (s.ordinal() < t.position.ordinal()) {
          SegmentPosition end = endOf(c, s);
          records.add(LogEnvelope.encodeRetiredSegment(new RetiredSegment(s.ordinal(), s.name(),
              s.location(), end == null ? null : end.encode(), now)));
        }
      }
      try {
        Receipt r = catalog.replace(c.view().tail(), checkpoint, records);
        adopt(parseCat(catalog.readThrough(r, Duration.ZERO)));
        return;
      } catch (LogConflictException e) {
        // an Install or another checkpoint landed: read again
      }
    }
    throw new LogNotSentException(this + " could not store a checkpoint in "
        + options.sendAttempts() + " attempts", null);
  }

  /** Where {@code s}, which has ended, ended: as read before, or by reading it; null if unknown. */
  private SegmentPosition endOf(Cat c, Seg s) {
    SegmentPosition end = ends.get(s.name());
    if (end != null) {
      return end;
    }
    try {
      Walk w = walk(c, new SegmentPosition(s.ordinal(), 0, 0));
      return w == null ? null : ends.get(s.name());
    } catch (RuntimeException e) {
      return null;
    }
  }

  /**
   * Creates the log (docs/LOG_SEGMENTS.md "Creating the log"): segment 0, under the id of the log
   * its checkpoint begins and the prefix this log was given (or the default), then the catalog
   * object if absent, with the first checkpoint, the prefix unless it is the default (D13), and
   * segment 0's Install. One of the same checkpoint is this creation, retried; another is another
   * writer's log, and this creation deletes its segment 0, which no sweep lists.
   *
   * @throws IllegalStateException if the log is absent and this log has no prefix to create it at
   */
  private void create(byte[] checkpoint) {
    LogTail absent = catalog.probe();
    if (absent.exists()) {
      created(checkpoint, null);
      return;
    }
    String prefix = configured != null ? configured : defaultPrefix;
    if (prefix == null) {
      throw new IllegalStateException("The catalog object " + catalog + " does not exist, and "
          + "this client cannot create its log: it was given no prefix for its segments, and "
          + "derives no default from the catalog object's location");
    }
    UUID logId = LogEnvelope.decodeCheckpoint(checkpoint).logId();
    String name = segmentName(0);
    try {
      while (!store.create(path(prefix, logId, name))) {
        name = segmentName(0);
      }
    } catch (SegmentStore.Throttled e) {
      throw new LogThrottledException(this + " throttled its creation", e);
    } catch (SegmentStore.Refused e) {
      throw new LogNotSentException("The store refused the creation of " + this, e);
    }
    List<byte[]> records = new ArrayList<>();
    records.add(LogEnvelope.encodeCheckpointThrough(SegmentPosition.START.encode()));
    if (!prefix.equals(defaultPrefix)) {
      records.add(LogEnvelope.encodeSegmentPrefix(prefix));
    }
    records.add(new Seg(logId, prefix, 0, name, "", limit).install());
    try {
      Receipt r = catalog.replace(absent, checkpoint, records);
      adopt(parseCat(catalog.readThrough(r, Duration.ZERO)));
    } catch (LogConflictException e) {
      abandon(prefix, logId, name);
      created(checkpoint, e);
    } catch (RuntimeException e) {
      abandon(prefix, logId, name); // of unknown outcome: the runtime reads the log to settle it
      throw e;
    }
  }

  /**
   * Deletes segment {@code name} of a creation of the log {@code logId} under {@code prefix} if
   * the catalog object is another log's: then no Install can name it (the creation's replace was
   * conditioned on the object's absence), and no sweep lists it (each lists its own log's). Best
   * effort: one whose delete or read fails stays, as does one whose catalog object is absent (the
   * replace may land yet).
   */
  private void abandon(String prefix, UUID logId, String name) {
    try {
      Cat c = readCat();
      if (c != null && !c.logId().equals(logId)) {
        store.delete(path(prefix, logId, name));
      }
    } catch (RuntimeException e) {
      // it stays (docs/AZURE_LOG_FORMAT.md OI-10)
    }
  }

  private void created(byte[] checkpoint, RuntimeException cause) {
    UUID logId = LogEnvelope.decodeCheckpoint(checkpoint).logId();
    Cat c = existing();
    if (!c.logId().equals(logId) || c.ck().compareTo(SegmentPosition.START) != 0
        || !Arrays.equals(c.checkpoint(), checkpoint)) {
      throw new LogConflictException(this + " was created by another writer", cause);
    }
  }

  /** Whether {@code r}'s TTL has passed at {@code now}. */
  private boolean expired(RetiredSegment r, long now) {
    return r.retiredAtMs() + options.retireTtlMs() <= now;
  }

  /**
   * Deletes the segment of {@code r}, a Retired entry of the catalog object {@code c}, unless
   * {@code deleted} holds it already; whether it is gone. A delete that fails leaves it for the
   * sweep or the next checkpoint.
   */
  private boolean deleteRetired(Cat c, RetiredSegment r, Set<String> deleted) {
    if (deleted.contains(r.name())) {
      return true;
    }
    try {
      store.delete(path(c.prefix(), c.logId(), r.name(), r.location()));
    } catch (RuntimeException e) {
      return false;
    }
    deleted.add(r.name());
    return true;
  }

  /**
   * The sweep (docs/LOG_SEGMENTS.md "Compaction, retirement and the sweep"): deletes each retired
   * segment whose TTL has passed by its entry's name, then lists this log's segments ({@code
   * <prefix>/<log id>/}) and deletes each unnamed blob whose ordinal is at most the newest
   * Install's. Another log's segments at the prefix are its own sweep's. {@code through}'s
   * checkpoint is the catalog object's or older.
   */
  @Override
  public void trim(LogTail through) {
    SegTail t = tailOf(through);
    Cat c = existing();
    if (c.logId().equals(t.logId) && c.ck().compareTo(t.position) < 0) {
      throw new IllegalStateException("P5: no checkpoint of " + this + " covers " + t);
    }
    try {
      sweep();
    } catch (RuntimeException e) {
      // best effort: the next trim sweeps again
    }
  }

  void sweep() {
    Cat c = existing();
    long now = options.clockMs().getAsLong();
    Set<String> named = new HashSet<>();
    c.segs().forEach(s -> named.add(s.name()));
    Set<String> deleted = new HashSet<>();
    for (RetiredSegment r : c.retired()) {
      named.add(r.name());
      if (expired(r, now)) {
        deleteRetired(c, r, deleted); // by name: no listing needed
      }
    }
    long newest = c.last().ordinal();
    for (String full : store.list(path(c.prefix(), c.logId(), "seg-"))) {
      long ordinal = ordinalAt(c.prefix(), c.logId(), full); // -1: none of this log's segments
      String name = full.substring(full.lastIndexOf('/') + 1);
      if (ordinal >= 0 && ordinal <= newest && !named.contains(name)) {
        try {
          store.delete(full);
        } catch (RuntimeException e) {
          // the sweep finds it again
        }
      }
    }
  }

  private SegTail tailOf(LogTail tail) {
    if (!(tail instanceof SegTail t) || !t.log.equals(where())) {
      throw new IllegalArgumentException("Not a tail of " + this + ": " + tail);
    }
    return t;
  }

  /** The log a tail is of: its catalog object's location. */
  private String where() {
    return catalog.toString();
  }

  // ============================================================
  // Tails, receipts, views
  // ============================================================

  /**
   * Where a read ended: the catalog object's location, the log's id, a position, and the bytes
   * read since the checkpoint.
   */
  static final class SegTail implements LogTail {
    private final String log;
    private final UUID logId;
    private final boolean exists;
    private final SegmentPosition position;
    private final long bytes;

    SegTail(String log, UUID logId, boolean exists, SegmentPosition position, long bytes) {
      this.log = log;
      this.logId = logId;
      this.exists = exists;
      this.position = position;
      this.bytes = bytes;
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
      return earlier instanceof SegTail e && e.log.equals(log) && exists && e.exists
          && e.logId.equals(logId) && position.compareTo(e.position) >= 0;
    }

    @Override
    public byte[] position() {
      return position.encode();
    }

    SegmentPosition at() {
      return position;
    }

    @Override
    public String toString() {
      return log + "@" + position + (exists ? "" : " (absent)");
    }
  }

  /**
   * A copy of an append: its segment, where the reply put it, its index (0 or less: the store
   * can't say), length and check.
   */
  record Copy(long ordinal, String name, long offset, long index, int length, int crc) {}

  /** An append's receipt: its copies, in ordinal order, and its record. */
  static final class SegReceipt implements Receipt {
    final UUID logId;
    final List<Copy> copies;
    final byte[] record;

    SegReceipt(UUID logId, List<Copy> copies, byte[] record) {
      this.logId = logId;
      this.copies = copies;
      this.record = record;
    }

    @Override
    public String toString() {
      return "SegReceipt[logId=" + logId + ", copies=" + copies + "]";
    }
  }

  /** A view: a checkpoint (or none: records after a tail), its records and where each starts. */
  private static final class View implements LogView {
    private final SegTail tail;
    private final byte[] checkpoint;
    private final SegmentPosition start;
    private final List<byte[]> records;
    private final long[] ordinals;
    private final long[] offsets;

    View(SegTail tail, byte[] checkpoint, SegmentPosition start, List<byte[]> records,
        long[] ordinals, long[] offsets) {
      this.tail = tail;
      this.checkpoint = checkpoint;
      this.start = start;
      this.records = records;
      this.ordinals = ordinals;
      this.offsets = offsets;
    }

    /** This view with the records of {@code more}, a view of the records after its tail. */
    View extend(View more) {
      if (more.records.isEmpty() && more.tail.position.equals(tail.position)) {
        return this;
      }
      List<byte[]> r = new ArrayList<>(records.size() + more.records.size());
      r.addAll(records);
      r.addAll(more.records);
      long[] o = Arrays.copyOf(ordinals, ordinals.length + more.ordinals.length);
      System.arraycopy(more.ordinals, 0, o, ordinals.length, more.ordinals.length);
      long[] f = Arrays.copyOf(offsets, offsets.length + more.offsets.length);
      System.arraycopy(more.offsets, 0, f, offsets.length, more.offsets.length);
      return new View(more.tail, checkpoint, start, List.copyOf(r), o, f);
    }

    @Override
    public LogTail tail() {
      return tail;
    }

    @Override
    public Optional<byte[]> checkpoint() {
      return Optional.ofNullable(checkpoint);
    }

    @Override
    public List<byte[]> records() {
      return records;
    }

    @Override
    public byte[] start() {
      return start.encode();
    }
  }
}
