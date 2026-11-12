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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;
import org.intentlog.Capabilities;
import org.intentlog.LogConflictException;
import org.intentlog.LogFullException;
import org.intentlog.LogLagException;
import org.intentlog.LogNotSentException;
import org.intentlog.LogTail;
import org.intentlog.LogThrottledException;
import org.intentlog.LogView;
import org.intentlog.PageStore;
import org.intentlog.Receipt;
import org.intentlog.SharedLog;
import org.intentlog.TrimmedException;
import org.intentlog.azure.AzureLogHead.CheckpointRef;
import org.intentlog.azure.AzureLogHead.Segment;
import org.intentlog.azure.Frames.Frame;
import org.intentlog.azure.Frames.FrameCheckException;
import org.intentlog.wire.LogEnvelope;

/**
 * A {@link SharedLog} on Azure Append Blobs (docs/AZURE_LOG.md; model: spec/tla/AzureLog.tla).
 *
 * <p>Under {@code root}:
 *
 * <ul>
 *   <li>{@code head}: a block blob, changed only by CAS on its ETag ({@link AzureLogHead}): the
 *       log's id, its live segments, and its newest checkpoint;
 *   <li>{@code seg/<ordinal>-<nonce>}: Append Blobs, the records. Writers append without a
 *       position condition, each append carrying the segment's writer limit as its size
 *       condition, to the last segment; a writer that finds it full closes it with a pad that
 *       fills it to its limit, creates a candidate successor, and CASes the head to name it;
 *   <li>{@code ckpt/<position>-<log id>}: block blobs, one checkpoint each, created once;
 *   <li>{@code pages/<name>}: the committed set's pages.
 * </ul>
 *
 * <p>A position is a segment's base plus an offset in it, 8 bytes big-endian: the end of a closed
 * segment and the start of the next are one position. The store sends each request once; this
 * log retries reads itself, and leaves the retries of writes to the runtime.
 */
public final class AppendBlobLog implements SharedLog {

  /**
   * How the log behaves.
   *
   * @param conditional appends conditioned on the tail (the ablation: {@link
   *     Capabilities#conditionalAppend}), not blind
   * @param segmentLimit L, a new segment's limit in bytes
   * @param maxRotations how many times one append may move the log on before it is full
   * @param readAttempts how many times a read starts again (a segment trimmed under it, a frame
   *     that failed its check) before the log could not be read
   * @param trimGraceMs how long a segment out of the head stays before the sweep deletes it
   * @param checkpointKeep how many checkpoints older than the head's the sweep keeps
   * @param clockMs the clock the sweep's grace is measured on
   */
  public record Options(
      boolean conditional,
      long segmentLimit,
      int maxRotations,
      int readAttempts,
      long trimGraceMs,
      int checkpointKeep,
      LongSupplier clockMs) {

    public static Options defaults() {
      return new Options(false, 64L << 20, 4, 8, 60_000L, 2, System::currentTimeMillis);
    }

    public Options withConditional(boolean c) {
      return new Options(c, segmentLimit, maxRotations, readAttempts, trimGraceMs, checkpointKeep,
          clockMs);
    }

    public Options withSegmentLimit(long l) {
      return new Options(conditional, l, maxRotations, readAttempts, trimGraceMs, checkpointKeep,
          clockMs);
    }

    public Options withTrimGraceMs(long g) {
      return new Options(conditional, segmentLimit, maxRotations, readAttempts, g, checkpointKeep,
          clockMs);
    }

    public Options withCheckpointKeep(int k) {
      return new Options(conditional, segmentLimit, maxRotations, readAttempts, trimGraceMs, k,
          clockMs);
    }

    public Options withClock(LongSupplier c) {
      return new Options(conditional, segmentLimit, maxRotations, readAttempts, trimGraceMs,
          checkpointKeep, c);
    }
  }

  static final String META_LOG_ID = "intentlog_log_id";
  static final String META_SEGMENT = "intentlog_segment";
  static final String META_LIMIT = "intentlog_limit";
  static final String META_FORMAT = "intentlog_format";

  private final AppendBlobStore store;
  private final String root;
  private final Options options;
  private final PageStore pages;

  /** The newest head this log read or wrote, with its ETag; null until it reads one. */
  private volatile Head cached = null;

  /** The newest view this log built, extended by the next read through a write. */
  private volatile View newest = null;

  private record Head(AzureLogHead head, String etag) {}

  public AppendBlobLog(AppendBlobStore store, String root, Options options) {
    if (options.segmentLimit() < Frames.RESERVE + Frames.MAX_OVERHEAD + 1) {
      throw new IllegalArgumentException("A segment limit of " + options.segmentLimit()
          + " bytes holds no record");
    }
    this.store = store;
    this.root = root.endsWith("/") ? root.substring(0, root.length() - 1) : root;
    this.options = options;
    this.pages = new BlobPages();
  }

  @Override
  public String toString() {
    return "azure log " + root;
  }

  /**
   * This log as another process holding what it holds: its cached head and the view it kept, for
   * a test's oracle to read with while this log's caches stay as they are.
   */
  AppendBlobLog copy() {
    AppendBlobLog c = new AppendBlobLog(store, root, options);
    c.cached = cached;
    c.newest = newest;
    return c;
  }

  @Override
  public Capabilities capabilities() {
    long writer = options.segmentLimit() - Frames.RESERVE;
    long largest = Math.min(store.maxBlockBytes() - Frames.RESERVE, writer) - Frames.MAX_OVERHEAD;
    return new Capabilities(true, options.conditional(), false, true, largest);
  }

  // ============================================================
  // Names
  // ============================================================

  private String headName() {
    return root + "/head";
  }

  private String segmentName(long ordinal) {
    return String.format("%s/seg/%020d-%016x", root, ordinal,
        ThreadLocalRandom.current().nextLong());
  }

  private String checkpointName(long position, UUID logId) {
    return String.format("%s/ckpt/%020d-%s", root, position, hex(logId));
  }

  private static String hex(UUID id) {
    return String.format("%016x%016x", id.getMostSignificantBits(), id.getLeastSignificantBits());
  }

  private Map<String, String> metadata(UUID logId, long ordinal, long limit) {
    return Map.of(META_LOG_ID, hex(logId), META_SEGMENT, Long.toString(ordinal), META_LIMIT,
        Long.toString(limit), META_FORMAT, "1");
  }

  // ============================================================
  // The head
  // ============================================================

  /** A fresh read of the head; null if the log does not exist. */
  private Head readHead() {
    AppendBlobStore.Blob b = store.read(headName(), 0);
    if (b == null) {
      return null;
    }
    Head h = new Head(AzureLogHead.decode(b.bytes()), b.etag());
    adopt(h);
    return h;
  }

  private Head headOrRead() {
    Head h = cached;
    return h != null ? h : readHead();
  }

  /** Keeps {@code h} if it is newer than the head held. */
  private synchronized void adopt(Head h) {
    Head was = cached;
    if (was == null || !was.head().logId().equals(h.head().logId())
        || was.head().generation() < h.head().generation()) {
      cached = h;
    }
  }

  private Head existing() {
    Head h = readHead();
    if (h == null) {
      throw new IllegalStateException(this + " does not exist");
    }
    return h;
  }

  // ============================================================
  // Appends
  // ============================================================

  @Override
  public Receipt append(byte[] record, LogTail expected) {
    if (expected != null && !options.conditional()) {
      throw new IllegalArgumentException(this + " appends unconditionally");
    }
    if (record.length > capabilities().maxRecordBytes()) {
      throw new LogNotSentException("A record of " + record.length + " bytes is larger than "
          + this + " stores", null);
    }
    Head h = headOrRead();
    if (h == null) {
      throw new IllegalStateException(this + " does not exist");
    }
    Tail at = expected == null ? null : tailOf(expected);
    for (int rotations = 0; ; ) {
      Segment seg = h.head().last();
      if (at != null && (at.ordinal != seg.ordinal() || !at.logId.equals(h.head().logId()))) {
        throw new LogConflictException("An append to " + this + " expected " + at
            + ", and the log moved on", null);
      }
      byte[] frame = Frames.record(record, seg.ordinal());
      try {
        long offset = store.appendBlock(seg.name(), frame, seg.writerLimit(),
            at == null ? -1 : at.position - seg.base());
        return new AzReceipt(h.head().logId(), seg.ordinal(), seg.name(), seg.base(), offset,
            frame.length, Frames.crcOf(frame), record);
      } catch (AppendBlobStore.Full e) {
        // nothing landed (P6): the segment is full, or closed
        Head fresh = existing();
        if (fresh.head().last().ordinal() == seg.ordinal()) {
          if (rotations++ >= options.maxRotations()) {
            throw new LogFullException(this + " stayed full through " + options.maxRotations()
                + " moves", e);
          }
          try {
            fresh = rotate(fresh, seg);
          } catch (AppendBlobStore.Throttled | UncheckedIOException e2) {
            // nothing of this record landed; moving the log on failed partway, and the next
            // writer to find the segment full repairs it (AZURE_LOG.md "Rotation")
            throw new LogThrottledException(this + " failed to move on from " + seg.name(), e2);
          }
        }
        if (at != null) {
          throw new LogConflictException("An append to " + this + " found its segment full", e);
        }
        h = fresh;
      } catch (AppendBlobStore.NotFound e) {
        // a stale head: the segment was trimmed and swept. Never create it (AZ4).
        Head fresh = existing();
        if (fresh.head().byOrdinal(seg.ordinal()) != null
            && fresh.head().byOrdinal(seg.ordinal()).name().equals(seg.name())) {
          throw new IllegalStateException("AZ5: the head names " + seg.name()
              + ", which does not exist", e);
        }
        if (at != null) {
          throw new LogConflictException("An append to " + this + " found its segment gone", e);
        }
        h = fresh;
      } catch (AppendBlobStore.PositionMismatch e) {
        throw new LogConflictException("An append to " + this + " expected position "
            + at.position + ": " + e.getMessage(), e);
      } catch (AppendBlobStore.Throttled e) {
        throw new LogThrottledException(this + " throttled an append", e);
      } catch (AppendBlobStore.Refused e) {
        throw new LogNotSentException("The store refused an append to " + this, e);
      }
    }
  }

  /**
   * Moves the log on from {@code full}, the last segment of {@code h} (AZURE_LOG.md "Rotation"):
   * closes it, creates a candidate successor, and CASes the head to name it. Returns the head
   * that names a successor, this writer's or another's.
   */
  private Head rotate(Head h, Segment full) {
    long closed = close(full);
    if (closed < 0) {
      return existing(); // gone: the head moved on, and trimmed it
    }
    Segment next = new Segment(full.ordinal() + 1, segmentName(full.ordinal() + 1), -1,
        options.segmentLimit(), full.base() + closed);
    if (!store.createAppendBlob(next.name(),
        metadata(h.head().logId(), next.ordinal(), next.limit()))) {
      throw new IllegalStateException("A fresh segment name was taken: " + next.name());
    }
    while (true) {
      AzureLogHead rotated = h.head().rotated(full, closed, next);
      String etag = store.putIfMatch(headName(), rotated.encode(), h.etag());
      if (etag != null) {
        Head won = new Head(rotated, etag);
        adopt(won);
        return won;
      }
      h = existing();
      Segment last = h.head().last();
      if (last.name().equals(next.name())) {
        return h; // this CAS landed, though its reply said it lost (a resend that failed its If-Match)
      }
      if (last.ordinal() != full.ordinal()) {
        deleteQuietly(next.name()); // another writer moved the log on; the sweep would too
        return h;
      }
    }
  }

  /**
   * Closes {@code seg} so that no append lands in it again: finds it closed (at its limit, or at
   * the block limit), or appends a pad that fills it to its limit. Returns its final length; -1
   * if it no longer exists. A throttled pad, which may have landed, is the runtime's to send again
   * after its backoff ({@link AppendBlobStore.Throttled}): its next append finds the segment full
   * and closes it from a fresh look.
   */
  private long close(Segment seg) {
    while (true) {
      AppendBlobStore.Props p = store.properties(seg.name());
      if (p == null) {
        return -1;
      }
      if (p.length() >= seg.limit() || p.blocks() >= store.maxBlocks()) {
        return p.length();
      }
      try {
        store.appendBlock(seg.name(), Frames.pad(seg.limit() - p.length(), seg.ordinal()),
            seg.limit(), p.length());
        return seg.limit();
      } catch (AppendBlobStore.PositionMismatch | AppendBlobStore.Full e) {
        // the segment grew, or met the block limit: look again
      } catch (AppendBlobStore.NotFound e) {
        return -1;
      }
    }
  }

  // ============================================================
  // Checkpoints, trims, the sweep
  // ============================================================

  @Override
  public void putCheckpoint(LogTail through, byte[] checkpoint) {
    if (!through.exists()) {
      create(checkpoint);
      return;
    }
    Tail t = tailOf(through);
    String name = checkpointName(t.position, t.logId);
    try {
      if (!store.putIfAbsent(name, checkpoint)) {
        AppendBlobStore.Blob existing = store.read(name, 0);
        if (existing != null && !Arrays.equals(existing.bytes(), checkpoint)) {
          throw new IllegalStateException("I11: two checkpoints at " + t.position + " of " + this
              + " differ");
        }
      }
      while (true) {
        Head h = existing();
        AzureLogHead head = h.head();
        if (!head.logId().equals(t.logId) || head.checkpoint().through() >= t.position
            || head.at(t.position) == null) {
          return; // a newer checkpoint is named, or the log trimmed past this one
        }
        AzureLogHead named = head.withCheckpoint(new CheckpointRef(t.position, name));
        String etag = store.putIfMatch(headName(), named.encode(), h.etag());
        if (etag != null) {
          adopt(new Head(named, etag));
          return;
        }
      }
    } catch (AppendBlobStore.Throttled e) {
      throw new LogThrottledException(this + " throttled a checkpoint", e);
    } catch (AppendBlobStore.Refused e) {
      throw new LogNotSentException("The store refused a checkpoint of " + this, e);
    }
  }

  /**
   * Creates the log (AZURE_LOG.md "Creating the log"): its first checkpoint at position 0, an
   * empty segment 0, then the head, if absent. A head of the same log id is this creation,
   * retried; one of another is another writer's log.
   */
  private void create(byte[] checkpoint) {
    UUID logId = LogEnvelope.decodeCheckpoint(checkpoint).logId();
    String ck = checkpointName(0, logId);
    try {
      Head was = readHead();
      if (was != null) {
        AppendBlobStore.Blob first = store.read(was.head().checkpoint().name(), 0);
        if (!was.head().logId().equals(logId) || was.head().checkpoint().through() != 0
            || first == null || !Arrays.equals(first.bytes(), checkpoint)) {
          throw new LogConflictException(this + " was created by another writer", null);
        }
        return; // this creation, retried
      }
      if (!store.putIfAbsent(ck, checkpoint)) {
        AppendBlobStore.Blob existing = store.read(ck, 0);
        if (existing != null && !Arrays.equals(existing.bytes(), checkpoint)) {
          throw new LogConflictException(this + " was created by another writer", null);
        }
      }
      Segment first = new Segment(0, segmentName(0), -1, options.segmentLimit(), 0);
      store.createAppendBlob(first.name(), metadata(logId, 0, first.limit()));
      AzureLogHead head = new AzureLogHead(logId, 1, List.of(first), new CheckpointRef(0, ck));
      if (!store.putIfAbsent(headName(), head.encode())) {
        Head h = existing();
        if (!h.head().logId().equals(logId)) {
          throw new LogConflictException(this + " was created by another writer", null);
        }
      }
    } catch (AppendBlobStore.Throttled e) {
      throw new LogThrottledException(this + " throttled its creation", e);
    } catch (AppendBlobStore.Refused e) {
      throw new LogNotSentException("The store refused the creation of " + this, e);
    }
  }

  /**
   * Drops from the head the segments before the one {@code through} lies in, then sweeps. P5: the
   * head's checkpoint must cover {@code through}. Readers start from the head's checkpoint, so the
   * records before it are gone to them already ({@link #readAfter}, {@link #readThrough}); the trim
   * deletes whole segments.
   */
  @Override
  public void trim(LogTail through) {
    Tail t = tailOf(through);
    AzureLogHead trimmed;
    while (true) {
      Head h = existing();
      AzureLogHead head = h.head();
      if (!head.logId().equals(t.logId)) {
        return;
      }
      if (head.checkpoint().through() < t.position) {
        throw new IllegalStateException("P5: no checkpoint of " + this + " covers " + t);
      }
      Segment keep = head.at(t.position);
      if (keep == null || keep.ordinal() <= head.first().ordinal()) {
        trimmed = head;
        break;
      }
      AzureLogHead cut = head.trimmedBefore(keep.ordinal());
      String etag = store.putIfMatch(headName(), cut.encode(), h.etag());
      if (etag != null) {
        adopt(new Head(cut, etag));
        trimmed = cut;
        break;
      }
    }
    try {
      sweep(trimmed);
    } catch (RuntimeException e) {
      // best effort: the next trim sweeps again
    }
  }

  /**
   * Deletes what no head will name again (AZURE_LOG.md "Compaction, trim and sweep", AZ5): the
   * segments {@code head} doesn't name at or below its last ordinal (trimmed, or candidates that
   * lost), checkpoints older than its own but the newest {@code checkpointKeep}, and checkpoints
   * of other log ids; each only once it is older than the grace. A candidate past the head's last
   * ordinal may still win its CAS, and is kept.
   */
  void sweep(AzureLogHead head) {
    Instant cutoff = Instant.ofEpochMilli(options.clockMs().getAsLong() - options.trimGraceMs());
    Set<String> named = new HashSet<>();
    head.segments().forEach(s -> named.add(s.name()));
    for (AppendBlobStore.Entry e : store.list(root + "/seg/")) {
      long ordinal = ordinalOf(e.name());
      if (ordinal >= 0 && !named.contains(e.name()) && ordinal <= head.last().ordinal()
          && !e.lastModified().isAfter(cutoff)) {
        deleteQuietly(e.name());
      }
    }
    String own = hex(head.logId());
    TreeMap<Long, String> older = new TreeMap<>();
    for (AppendBlobStore.Entry e : store.list(root + "/ckpt/")) {
      String tail = e.name().substring(e.name().lastIndexOf('/') + 1);
      int dash = tail.indexOf('-');
      if (dash < 0) {
        continue;
      }
      long position = Long.parseLong(tail.substring(0, dash));
      boolean ours = tail.substring(dash + 1).equals(own);
      if (!ours && !e.lastModified().isAfter(cutoff)) {
        deleteQuietly(e.name());
      } else if (ours && position < head.checkpoint().through()) {
        older.put(position, e.name());
      }
    }
    while (older.size() > options.checkpointKeep()) {
      deleteQuietly(older.pollFirstEntry().getValue());
    }
  }

  private static long ordinalOf(String name) {
    String tail = name.substring(name.lastIndexOf('/') + 1);
    int dash = tail.indexOf('-');
    try {
      return dash < 0 ? -1 : Long.parseLong(tail.substring(0, dash));
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  private void deleteQuietly(String name) {
    try {
      store.delete(name);
    } catch (RuntimeException e) {
      // the sweep finds it again
    }
  }

  // ============================================================
  // Reads
  // ============================================================

  @Override
  public LogView read() {
    RuntimeException last = null;
    for (int attempt = 0; attempt < options.readAttempts(); attempt++) {
      try {
        View v = readWhole(existing());
        if (v != null) {
          keep(v);
          return v;
        }
      } catch (FrameCheckException e) {
        last = e;
      }
    }
    throw unreadable(last);
  }

  /** The head's checkpoint and the records after it; null if the read must start again. */
  private View readWhole(Head h) {
    AzureLogHead head = h.head();
    CheckpointRef ref = head.checkpoint();
    AppendBlobStore.Blob ck = store.read(ref.name(), 0);
    if (ck == null) {
      return null;
    }
    Segment from = head.at(ref.through());
    if (from == null) {
      return null;
    }
    Walk w = walk(head, from, ref.through() - from.base());
    if (w == null) {
      return null;
    }
    Tail tail = new Tail(root, head.logId(), true, w.end, ck.bytes().length + w.bytes,
        w.ordinal);
    return new View(tail, ck.bytes(), ref.through(), w.records, w.positions, ref.through());
  }

  @Override
  public LogView readAfter(LogTail since) {
    Tail t = tailOf(since);
    RuntimeException last = null;
    for (int attempt = 0; attempt < options.readAttempts(); attempt++) {
      try {
        Head h = headOrRead();
        if (h == null || !h.head().logId().equals(t.logId)) {
          h = existing();
        }
        AzureLogHead head = h.head();
        if (!head.logId().equals(t.logId)) {
          throw new TrimmedException(this + " is another log than the one read at " + t);
        }
        if (head.checkpoint().through() > t.position) {
          return read(); // a checkpoint covers the tail: the records after it are trimmed
        }
        Segment seg = head.byOrdinal(t.ordinal);
        if (seg == null) {
          head = existing().head();
          seg = head.byOrdinal(t.ordinal);
          if (seg == null || head.checkpoint().through() > t.position) {
            return read();
          }
        }
        Walk w = walk(head, seg, t.position - seg.base());
        if (w == null) {
          if (existing().head().byOrdinal(t.ordinal) == null) {
            throw new TrimmedException(this + " trimmed the records after " + t);
          }
          continue;
        }
        Tail tail = new Tail(root, t.logId, true, w.end, t.bytes + w.bytes, w.ordinal);
        View v = new View(tail, null, -1, w.records, w.positions, t.position);
        return v;
      } catch (FrameCheckException e) {
        last = e;
      }
    }
    throw unreadable(last);
  }

  @Override
  public LogView readThrough(Receipt receipt, Duration wait) {
    if (!(receipt instanceof AzReceipt r)) {
      throw new IllegalArgumentException("Not a receipt of " + this + ": " + receipt);
    }
    long deadline = System.nanoTime() + wait.toNanos();
    RuntimeException last = null;
    for (int attempt = 0; ; attempt++) {
      try {
        View v = through(r, attempt > 0);
        if (v != null) {
          int i = Arrays.binarySearch(v.positions, r.position());
          if (i >= 0) {
            byte[] frame = Frames.record(v.records.get(i), r.ordinal);
            if (frame.length != r.length || Frames.crcOf(frame) != r.crc) {
              throw new IllegalStateException("AZ3: another record is at " + r);
            }
            keep(v);
            return v;
          }
          if (v.tail.position >= r.position() + r.length) {
            throw new IllegalStateException("AZ3: no record starts at " + r);
          }
        }
      } catch (FrameCheckException e) {
        last = e;
      }
      if (System.nanoTime() > deadline && attempt + 1 >= options.readAttempts()) {
        throw new LogLagException("A read through " + r + " of " + this + " did not reach it"
            + (last != null ? ": " + last.getMessage() : ""));
      }
    }
  }

  /**
   * A view through the receipt's record from a checkpoint before it (P3). The head's checkpoint
   * covering the record means the log trimmed it ({@link TrimmedException}). Else the newest view
   * this log kept, if it starts from that checkpoint, extended by the records after its tail (one
   * ranged GET, while the segment doesn't change); else a whole read. Null if the read must start
   * again.
   */
  private View through(AzReceipt r, boolean fresh) {
    Head h = fresh ? existing() : headOrRead();
    if (h == null || !h.head().logId().equals(r.logId)) {
      h = existing();
      if (!h.head().logId().equals(r.logId)) {
        throw new TrimmedException(this + " is another log than the one written at " + r);
      }
    }
    if (h.head().checkpoint().through() > r.position()) {
      h = existing();
      if (h.head().checkpoint().through() > r.position()) {
        throw new TrimmedException("The checkpoint of " + this + " covers " + r);
      }
    }
    View kept = newest;
    if (kept != null && kept.checkpoint != null && kept.tail.logId.equals(r.logId)
        && kept.through == h.head().checkpoint().through()) {
      if (!fresh && r.record != null && kept.tail.position == r.position()) {
        // The reply put the record at the tail of the view this log kept: nothing landed between,
        // so the view through it is that view and the record, with no read (as a conditional
        // append that won its race; AZURE_LOG.md "Reading through a write")
        View own = new View(
            new Tail(root, r.logId, true, r.position() + r.length, kept.tail.bytes + r.length,
                r.ordinal),
            null, -1, List.of(r.record), new long[] {r.position()}, kept.tail.position);
        return kept.extend(own);
      }
      LogView more = readAfter(kept.tail);
      if (more instanceof View m && m.checkpoint == null) {
        return kept.extend(m);
      }
      return (View) more;
    }
    return readWhole(h);
  }

  private void keep(View v) {
    View was = newest;
    if (v.checkpoint != null
        && (was == null || !was.tail.logId.equals(v.tail.logId)
            || was.tail.position <= v.tail.position)) {
      newest = v;
    }
  }

  @Override
  public LogTail probe() {
    Head h = readHead();
    if (h == null) {
      return new Tail(root, null, false, 0, 0, 0);
    }
    Segment last = h.head().last();
    AppendBlobStore.Props p = store.properties(last.name());
    long length = p == null ? 0 : p.length();
    long position = last.base() + length;
    return new Tail(root, h.head().logId(), true, position,
        position - h.head().checkpoint().through(), last.ordinal());
  }

  /** What a walk over the segments found. */
  private record Walk(List<byte[]> records, long[] positions, long end, long ordinal, long bytes) {}

  /**
   * The records from {@code offset} in {@code seg} on, through the segments after it as long as
   * each is closed (reading the head for its successor when {@code head} names none): the frames
   * of each closed segment exactly to its closed length, of the last to the length the read
   * returned. Null if a segment is gone, or a closed one read short: the read starts again.
   */
  private Walk walk(AzureLogHead head, Segment seg, long offset) {
    List<byte[]> records = new ArrayList<>();
    List<Long> positions = new ArrayList<>();
    long bytes = 0;
    while (true) {
      AppendBlobStore.Blob b = store.read(seg.name(), offset);
      if (b == null || !matches(b.metadata(), head.logId(), seg) || b.length() < offset) {
        return null; // gone, another's, or read short of where this read starts: read again
      }
      if (seg.closed() && b.length() != seg.closedLength()) {
        return null;
      }
      List<Frame> frames = Frames.parse(b.bytes(), b.bytes().length, offset, seg.ordinal());
      boolean padded = false;
      for (Frame f : frames) {
        if (f.pad()) {
          padded = true;
        } else {
          records.add(f.body());
          positions.add(seg.base() + f.offset());
          bytes += f.body().length;
        }
      }
      long end = b.length();
      // a segment at its limit is closed though a read from its end sees no pad: only a pad
      // reaches the limit, since every writer's block stops at the writer limit
      boolean closed = seg.closed() || padded || end >= seg.limit()
          || b.blocks() >= store.maxBlocks();
      if (!closed) {
        return walked(records, positions, seg.base() + end, seg.ordinal(), bytes);
      }
      Segment next = head.byOrdinal(seg.ordinal() + 1);
      if (next == null) {
        AzureLogHead fresh = existing().head();
        Segment again = fresh.byOrdinal(seg.ordinal());
        if (again == null) {
          return null; // trimmed under the read
        }
        if (again.closed() && again.closedLength() != end) {
          return null; // AZ1: a closed length is the length a reader reads
        }
        next = fresh.byOrdinal(seg.ordinal() + 1);
        head = fresh;
        if (next == null) {
          return walked(records, positions, seg.base() + end, seg.ordinal(), bytes);
        }
      }
      if (next.base() != seg.base() + end) {
        return null;
      }
      seg = next;
      offset = 0;
    }
  }

  private static Walk walked(List<byte[]> records, List<Long> positions, long end, long ordinal,
      long bytes) {
    long[] p = new long[positions.size()];
    for (int i = 0; i < p.length; i++) {
      p[i] = positions.get(i);
    }
    return new Walk(List.copyOf(records), p, end, ordinal, bytes);
  }

  private static boolean matches(Map<String, String> metadata, UUID logId, Segment seg) {
    return hex(logId).equals(get(metadata, META_LOG_ID))
        && Long.toString(seg.ordinal()).equals(get(metadata, META_SEGMENT));
  }

  private static String get(Map<String, String> metadata, String key) {
    for (Map.Entry<String, String> e : metadata.entrySet()) {
      if (e.getKey().equalsIgnoreCase(key)) {
        return e.getValue();
      }
    }
    return null;
  }

  private UncheckedIOException unreadable(RuntimeException cause) {
    return new UncheckedIOException(new IOException(this + " could not be read in "
        + options.readAttempts() + " attempts", cause));
  }

  private Tail tailOf(LogTail tail) {
    if (!(tail instanceof Tail t) || !t.root.equals(root)) {
      throw new IllegalArgumentException("Not a tail of " + this + ": " + tail);
    }
    return t;
  }

  @Override
  public PageStore pages() {
    return pages;
  }

  // ============================================================
  // Tails, receipts, views
  // ============================================================

  /** Where a read ended: the log's id, a position, the segment it is in, and the bytes read. */
  static final class Tail implements LogTail {
    private final String root;
    private final UUID logId;
    private final boolean exists;
    private final long position;
    private final long bytes;
    private final long ordinal;

    Tail(String root, UUID logId, boolean exists, long position, long bytes, long ordinal) {
      this.root = root;
      this.logId = logId;
      this.exists = exists;
      this.position = position;
      this.bytes = bytes;
      this.ordinal = ordinal;
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
      return earlier instanceof Tail e && e.root.equals(root) && exists && e.exists
          && e.logId.equals(logId) && position >= e.position;
    }

    @Override
    public byte[] position() {
      return AzureLogHead.position(position);
    }

    long at() {
      return position;
    }

    @Override
    public String toString() {
      return root + "@" + position + (exists ? "" : " (absent)");
    }
  }

  /**
   * An append's receipt: its segment, the offset the reply named, its frame's length and check,
   * and its record (for a read through it that finds nothing landed between).
   */
  record AzReceipt(UUID logId, long ordinal, String segment, long base, long offset, int length,
      int crc, byte[] record) implements Receipt {
    long position() {
      return base + offset;
    }

    @Override
    public String toString() {
      return "AzReceipt[logId=" + logId + ", ordinal=" + ordinal + ", segment=" + segment
          + ", base=" + base + ", offset=" + offset + ", length=" + length + ", crc=" + crc + "]";
    }
  }

  /** A view: a checkpoint (or none: records after a tail), its records and their positions. */
  private static final class View implements LogView {
    private final Tail tail;
    private final byte[] checkpoint;
    private final long through;
    private final List<byte[]> records;
    private final long[] positions;
    private final long start;

    View(Tail tail, byte[] checkpoint, long through, List<byte[]> records, long[] positions,
        long start) {
      this.tail = tail;
      this.checkpoint = checkpoint;
      this.through = through;
      this.records = records;
      this.positions = positions;
      this.start = start;
    }

    /** This view with the records of {@code more}, a view of the records after its tail. */
    View extend(View more) {
      List<byte[]> r = new ArrayList<>(records);
      r.addAll(more.records);
      long[] p = Arrays.copyOf(positions, positions.length + more.positions.length);
      System.arraycopy(more.positions, 0, p, positions.length, more.positions.length);
      Tail t = new Tail(tail.root, tail.logId, true, more.tail.position,
          tail.bytes + (more.tail.bytes - tail.bytes), more.tail.ordinal);
      return new View(t, checkpoint, through, List.copyOf(r), p, start);
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
      return AzureLogHead.position(start);
    }
  }

  // ============================================================
  // Pages
  // ============================================================

  private final class BlobPages implements PageStore {
    private String name(String page) {
      return root + "/pages/" + page;
    }

    @Override
    public void put(String page, byte[] bytes) {
      store.put(name(page), bytes);
    }

    @Override
    public Optional<byte[]> get(String page) {
      AppendBlobStore.Blob b = store.read(name(page), 0);
      return b == null ? Optional.empty() : Optional.of(b.bytes());
    }

    @Override
    public void delete(String page) {
      store.delete(name(page));
    }

    @Override
    public List<String> list() {
      String prefix = root + "/pages/";
      List<String> names = new ArrayList<>();
      for (AppendBlobStore.Entry e : store.list(prefix)) {
        names.add(e.name().substring(prefix.length()));
      }
      return names;
    }

    @Override
    public String location(String page) {
      return store.location(name(page));
    }
  }
}
