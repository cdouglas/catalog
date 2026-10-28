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
package org.intentlog;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.intentlog.wire.CommittedSetCodec;
import org.intentlog.wire.UuidV7;

/**
 * The ids of the committed transactions a log's replay has applied, less those GC dropped, and
 * the GC horizon: the newest timestamp it dropped. Replay applies a record once by its id, and a
 * writer learns from the set whether its commit landed. See docs/GC.md.
 *
 * <p>The set is kept as the {@code CommittedTransactions} packed entries (newest first, each a
 * varint gap from the one before and 10 random bytes), in runs: slices of entries, newest run
 * first, every entry of a run older than every entry of the run before it. Queries scan the runs
 * in place. Adding ids that are newer than every entry, as replay's almost always are, makes them
 * a run of their own, and runs merge by concatenation up to {@link #RUN_BYTES}; ids older than
 * the newest entry merge only with the entries at or above them. A GC drops the oldest runs and
 * cuts the one it ends in. So building a set costs the ids added, not the set. {@link #bytes()}
 * concatenates the runs into the {@code CommittedTransactions} body, which depends only on the
 * ids and the horizon, however the set was built.
 */
public final class CommittedSet {

  /** The bytes of packed entries runs reach by merging, and the chunks a GC cuts a run into. */
  static final int RUN_BYTES = 1 << 20;

  private static final int TAG_MAX_TIMESTAMP = 0x09; // field 1, fixed64
  private static final int TAG_PACKED_ENTRIES = 0x12; // field 2, length-delimited
  private static final int TAG_HORIZON = 0x19; // field 3, fixed64
  private static final int RANDOM_BYTES = 10;

  public static final CommittedSet EMPTY = new CommittedSet(new Run[0], 0L, new byte[0]);

  private final Run[] runs;
  private final long horizon;
  // The body this set was decoded from, while it is unchanged; else null
  private final byte[] encoded;

  private CommittedSet(Run[] runs, long horizon, byte[] encoded) {
    this.runs = runs;
    this.horizon = horizon;
    this.encoded = encoded;
  }

  /** The set encoded by a {@code CommittedTransactions} body; empty for null or no bytes. */
  public static CommittedSet of(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return EMPTY;
    }
    long maxTs = 0L;
    long horizon = 0L;
    int packedStart = -1;
    int packedEnd = -1;
    int[] at = {0};
    while (at[0] < bytes.length) {
      int tag = (int) readVarint(bytes, at, bytes.length);
      switch (tag) {
        case TAG_MAX_TIMESTAMP:
          maxTs = readFixed64(bytes, at);
          break;
        case TAG_PACKED_ENTRIES:
          int len = (int) readVarint(bytes, at, bytes.length);
          packedStart = at[0];
          packedEnd = packedStart + len;
          if (packedEnd > bytes.length) {
            throw malformed("truncated packed entries");
          }
          at[0] = packedEnd;
          break;
        case TAG_HORIZON:
          horizon = readFixed64(bytes, at);
          break;
        default:
          skipField(bytes, at, tag);
      }
    }
    Run[] runs = packedStart < packedEnd
        ? new Run[] {Run.slice(bytes, packedStart, packedEnd, maxTs)}
        : new Run[0];
    return new CommittedSet(runs, horizon, bytes);
  }

  /** The {@code CommittedTransactions} body; empty iff the set holds no id and no horizon. */
  public byte[] bytes() {
    if (encoded != null) {
      return encoded;
    }
    int packed = packedSize();
    if (runs.length == 0 && horizon == 0L) {
      return new byte[0];
    }
    byte[] out = new byte[encodedSize(packed)];
    int at = 0;
    out[at++] = TAG_MAX_TIMESTAMP;
    at = writeFixed64(out, at, runs.length == 0 ? 0L : runs[0].maxTs);
    out[at++] = TAG_PACKED_ENTRIES;
    at = writeVarint(out, at, packed);
    for (int i = 0; i < runs.length; i++) {
      Run run = runs[i];
      at = writeVarint(out, at, i == 0 ? 0L : runs[i - 1].minTs() - run.maxTs);
      System.arraycopy(run.buf, run.bodyOff, out, at, run.end - run.bodyOff);
      at += run.end - run.bodyOff;
    }
    if (horizon != 0L) {
      out[at++] = TAG_HORIZON;
      writeFixed64(out, at, horizon);
    }
    return out;
  }

  /** The length of {@link #bytes()}, without encoding them. */
  public long encodedSize() {
    if (encoded != null) {
      return encoded.length;
    }
    if (runs.length == 0 && horizon == 0L) {
      return 0;
    }
    return encodedSize(packedSize());
  }

  private int encodedSize(int packed) {
    return 1 + 8 + 1 + varintSize(packed) + packed + (horizon != 0L ? 1 + 8 : 0);
  }

  private int packedSize() {
    long size = 0;
    for (int i = 0; i < runs.length; i++) {
      Run run = runs[i];
      size += varintSize(i == 0 ? 0L : runs[i - 1].minTs() - run.maxTs) + run.end - run.bodyOff;
    }
    if (size > Integer.MAX_VALUE - 32) {
      throw new IllegalStateException("A committed set of " + size + " bytes is too large");
    }
    return (int) size;
  }

  /** The newest timestamp GC dropped (0 before any GC). */
  public long horizon() {
    return horizon;
  }

  /**
   * Whether {@code id} committed. True if the set holds it; false if it does not and the id is
   * newer than the horizon, or not a UUIDv7; and if it does not and the id is at or below the
   * horizon, the set cannot tell an aged-out commit from one that never landed, and this throws
   * {@link DedupHorizonExceededException}.
   */
  public boolean contains(UUID id) {
    if (holds(id)) {
      return true;
    }
    checkAboveHorizon(id, horizon);
    return false;
  }

  /**
   * Whether replay may apply the record of {@code id}: the set neither holds the id (the record
   * is a retry of one already applied) nor dropped ids at or after its time. A record at or below
   * the horizon may be a retry of a commit whose id aged out, so replay rejects it (Bug 64).
   */
  public boolean mayApply(UUID id) {
    return !holds(id) && aboveHorizon(id, horizon);
  }

  private boolean holds(UUID id) {
    if (runs.length == 0 || !UuidV7.isV7(id)) {
      return false;
    }
    long ts = UuidV7.timestampMs(id);
    byte[] rand = UuidV7.packRandomBits(id);
    for (Run run : runs) {
      if (ts > run.maxTs) {
        // Newer than this run and older than the one before it
        return false;
      }
      int found = run.find(ts, rand);
      if (found != 0) {
        return found > 0;
      }
    }
    return false;
  }

  /** Decodes every id in the set, newest first. */
  public Set<UUID> ids() {
    if (runs.length == 0) {
      return Collections.emptySet();
    }
    try {
      return Collections.unmodifiableSet(CommittedSetCodec.decode(bytes()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** A builder that adds ids to this set. */
  public Builder toBuilder() {
    return new Builder(this);
  }

  public static Builder builder() {
    return new Builder(EMPTY);
  }

  /** The number of runs; for tests. */
  int runCount() {
    return runs.length;
  }

  /** This set with {@code added} too: the ids newer than every entry as a run of their own. */
  private CommittedSet with(Collection<UUID> added) {
    int n = added.size();
    long[] ts = new long[n];
    byte[][] rand = new byte[n][];
    sortNewestFirst(added, ts, rand);
    long oldest = ts[n - 1];

    List<Run> next = new ArrayList<>(runs.length + 2);
    int rest = 0;
    if (runs.length == 0 || oldest > runs[0].maxTs) {
      next.add(Run.of(ts, rand, 0, n));
    } else {
      // Merge the ids with the entries at or above the oldest of them: whole runs, then the part
      // of the run the oldest falls in, whose entries below it stay a run
      List<Run> above = new ArrayList<>();
      Run below = null;
      while (rest < runs.length && runs[rest].maxTs >= oldest) {
        Run[] split = runs[rest++].splitBelow(oldest);
        above.add(split[0]);
        if (split[1] != null) {
          below = split[1];
          break;
        }
      }
      byte[] merged = CommittedSetCodec.rewrite(
          new CommittedSet(above.toArray(new Run[0]), 0L, null).bytes(), added, 0L);
      next.add(of(merged).runs[0]);
      if (below != null) {
        next.add(below);
      }
    }
    next.addAll(Arrays.asList(runs).subList(rest, runs.length));
    return new CommittedSet(mergeHead(next), horizon, null);
  }

  /**
   * Merges the newest runs while the newest is at least as large as the next and both fit in a
   * run: the runs then grow like a binary counter, so an entry is copied a few times at most.
   */
  private static Run[] mergeHead(List<Run> runs) {
    while (runs.size() >= 2) {
      Run first = runs.get(0);
      Run second = runs.get(1);
      int firstBytes = first.end - first.off;
      int secondBytes = second.end - second.off;
      if (firstBytes < secondBytes || firstBytes + secondBytes > RUN_BYTES) {
        break;
      }
      runs.remove(0);
      runs.set(0, Run.concat(first, second));
    }
    return runs.toArray(new Run[0]);
  }

  /** This set less every id at or below {@code watermarkMs}, the horizon raised to the newest. */
  private CommittedSet dropThrough(long watermarkMs) {
    long newHorizon = horizon;
    int keep = runs.length;
    Run[] cut = null;
    while (keep > 0) {
      Run run = runs[keep - 1];
      if (run.maxTs <= watermarkMs) {
        // Its newest entry is its first
        newHorizon = Math.max(newHorizon, run.maxTs);
        keep--;
        continue;
      }
      Run.Cut kept = run.cutThrough(watermarkMs);
      if (kept != null) {
        newHorizon = Math.max(newHorizon, kept.newestDropped);
        cut = kept.chunks;
      }
      break;
    }
    if (keep == runs.length && cut == null) {
      return this;
    }
    List<Run> next = new ArrayList<>(Arrays.asList(runs).subList(0, keep));
    if (cut != null) {
      next.remove(keep - 1);
      next.addAll(Arrays.asList(cut));
    }
    return new CommittedSet(next.toArray(new Run[0]), newHorizon, null);
  }

  private static void sortNewestFirst(Collection<UUID> ids, long[] ts, byte[][] rand) {
    UUID[] sorted = ids.toArray(new UUID[0]);
    // UUID.compareTo is signed per half, so compare as v7 orders them: timestamp, then the random
    // bits unsigned (UuidV7.packRandomBits keeps their order)
    long[] keyTs = new long[sorted.length];
    byte[][] keyRand = new byte[sorted.length][];
    Integer[] order = new Integer[sorted.length];
    for (int i = 0; i < sorted.length; i++) {
      keyTs[i] = UuidV7.timestampMs(sorted[i]);
      keyRand[i] = UuidV7.packRandomBits(sorted[i]);
      order[i] = i;
    }
    Arrays.sort(order, (a, b) -> {
      int c = Long.compare(keyTs[b], keyTs[a]);
      return c != 0 ? c : Arrays.compareUnsigned(keyRand[b], keyRand[a]);
    });
    for (int i = 0; i < sorted.length; i++) {
      ts[i] = keyTs[order[i]];
      rand[i] = keyRand[order[i]];
    }
  }

  private static boolean aboveHorizon(UUID id, long horizon) {
    return !UuidV7.isV7(id) || horizon == 0L || UuidV7.timestampMs(id) > horizon;
  }

  private static void checkAboveHorizon(UUID id, long horizon) {
    if (!aboveHorizon(id, horizon)) {
      throw new DedupHorizonExceededException(id, horizon);
    }
  }

  /**
   * Packed entries {@code [off, end)} of a buffer, never changed once made. The first entry's
   * timestamp is {@code maxTs}; its gap varint (at {@code off}) is from an entry before the run,
   * if any, and is skipped: {@code bodyOff} is its random bytes.
   */
  private static final class Run {
    final byte[] buf;
    final int off;
    final int bodyOff;
    final int end;
    final long maxTs;
    // The oldest entry's timestamp, scanned on first use
    private volatile long minTs;
    private volatile boolean scanned;

    private Run(byte[] buf, int off, int end, long maxTs, long minTs, boolean scanned) {
      this.buf = buf;
      this.off = off;
      this.end = end;
      this.maxTs = maxTs;
      int[] at = {off};
      readVarint(buf, at, end);
      this.bodyOff = at[0];
      if (bodyOff + RANDOM_BYTES > end) {
        throw malformed("truncated entry");
      }
      this.minTs = minTs;
      this.scanned = scanned;
    }

    static Run slice(byte[] buf, int off, int end, long maxTs) {
      return new Run(buf, off, end, maxTs, 0L, false);
    }

    /** The entries {@code [from, to)} of sorted arrays, as a run of their own. */
    static Run of(long[] ts, byte[][] rand, int from, int to) {
      int size = 0;
      for (int i = from; i < to; i++) {
        size += varintSize(i == from ? 0L : ts[i - 1] - ts[i]) + RANDOM_BYTES;
      }
      byte[] buf = new byte[size];
      int at = 0;
      for (int i = from; i < to; i++) {
        at = writeVarint(buf, at, i == from ? 0L : ts[i - 1] - ts[i]);
        System.arraycopy(rand[i], 0, buf, at, RANDOM_BYTES);
        at += RANDOM_BYTES;
      }
      return new Run(buf, 0, size, ts[from], ts[to - 1], true);
    }

    /** {@code newer} then {@code older}, whose entries are all older than {@code newer}'s. */
    static Run concat(Run newer, Run older) {
      long gap = newer.minTs() - older.maxTs;
      int olderBody = older.end - older.bodyOff;
      byte[] buf = new byte[newer.end - newer.off + varintSize(gap) + olderBody];
      System.arraycopy(newer.buf, newer.off, buf, 0, newer.end - newer.off);
      int at = writeVarint(buf, newer.end - newer.off, gap);
      System.arraycopy(older.buf, older.bodyOff, buf, at, olderBody);
      return new Run(buf, 0, buf.length, newer.maxTs, older.minTs(), true);
    }

    long minTs() {
      if (!scanned) {
        long ts = maxTs;
        int[] at = {bodyOff + RANDOM_BYTES};
        while (at[0] < end) {
          ts -= readVarint(buf, at, end);
          at[0] += RANDOM_BYTES;
          if (at[0] > end) {
            throw malformed("truncated entry");
          }
        }
        minTs = ts;
        scanned = true;
      }
      return minTs;
    }

    /**
     * Scans for an entry: 1 if the run holds it, -1 if the run passed below its timestamp (no
     * older entry anywhere holds it), 0 if every entry is at or above it.
     */
    int find(long ts, byte[] rand) {
      long entryTs = maxTs;
      int at = bodyOff;
      while (true) {
        if (entryTs == ts && matches(at, rand)) {
          return 1;
        }
        if (entryTs < ts) {
          return -1;
        }
        at += RANDOM_BYTES;
        if (at >= end) {
          return 0;
        }
        long gap = 0;
        int shift = 0;
        while (true) {
          int b = buf[at++] & 0xFF;
          gap |= ((long) (b & 0x7F)) << shift;
          if ((b & 0x80) == 0) {
            break;
          }
          shift += 7;
        }
        entryTs -= gap;
        if (at + RANDOM_BYTES > end) {
          throw malformed("truncated entry");
        }
      }
    }

    private boolean matches(int at, byte[] rand) {
      for (int i = 0; i < RANDOM_BYTES; i++) {
        if (buf[at + i] != rand[i]) {
          return false;
        }
      }
      return true;
    }

    /**
     * This run split at its first entry older than {@code ts}: the entries at or above it, and
     * the rest as a run (null if none).
     */
    Run[] splitBelow(long ts) {
      if (scanned && minTs >= ts) {
        return new Run[] {this, null};
      }
      long entryTs = maxTs;
      int[] at = {bodyOff + RANDOM_BYTES};
      while (at[0] < end) {
        int entry = at[0];
        long next = entryTs - readVarint(buf, at, end);
        if (next < ts) {
          return new Run[] {
            new Run(buf, off, entry, maxTs, entryTs, true),
            new Run(buf, entry, end, next, minTs, scanned)
          };
        }
        entryTs = next;
        at[0] += RANDOM_BYTES;
      }
      return new Run[] {this, null};
    }

    /** What a GC keeps of a run that holds entries above its watermark. */
    static final class Cut {
      final Run[] chunks;
      final long newestDropped;

      Cut(Run[] chunks, long newestDropped) {
        this.chunks = chunks;
        this.newestDropped = newestDropped;
      }
    }

    /**
     * The entries above {@code watermarkMs}, in chunks of about {@link #RUN_BYTES}, and the newest
     * dropped; null if the run drops nothing. A run's first entry is above the watermark.
     */
    Cut cutThrough(long watermarkMs) {
      if (scanned && minTs > watermarkMs && end - off <= RUN_BYTES) {
        return null;
      }
      List<Run> chunks = new ArrayList<>();
      int chunkStart = off;
      long chunkMax = maxTs;
      long entryTs = maxTs;
      int[] at = {bodyOff + RANDOM_BYTES};
      while (at[0] < end) {
        int entry = at[0];
        long next = entryTs - readVarint(buf, at, end);
        if (next <= watermarkMs) {
          chunks.add(new Run(buf, chunkStart, entry, chunkMax, entryTs, true));
          return new Cut(chunks.toArray(new Run[0]), next);
        }
        if (entry - chunkStart >= RUN_BYTES) {
          chunks.add(new Run(buf, chunkStart, entry, chunkMax, entryTs, true));
          chunkStart = entry;
          chunkMax = next;
        }
        entryTs = next;
        at[0] += RANDOM_BYTES;
      }
      if (chunks.isEmpty()) {
        // Nothing dropped, and the run fits in one chunk: it stays as it is, now scanned
        minTs = entryTs;
        scanned = true;
        return null;
      }
      chunks.add(new Run(buf, chunkStart, end, chunkMax, entryTs, true));
      // Nothing dropped, but the run is now in chunks; the horizon stays where it was
      return new Cut(chunks.toArray(new Run[0]), Long.MIN_VALUE);
    }
  }

  /**
   * An inherited set plus the ids replay adds to it. The additions merge into the set when it is
   * built, costing the additions (and the entries above the oldest of them), not the set.
   */
  public static final class Builder {
    private CommittedSet base;
    private final Set<UUID> added = new HashSet<>();

    private Builder(CommittedSet base) {
      this.base = base;
    }

    /** Replaces the inherited set (a decoded checkpoint's); additions are kept. */
    public Builder inherit(byte[] bytes) {
      this.base = of(bytes);
      return this;
    }

    public Builder add(UUID id) {
      if (!UuidV7.isV7(id)) {
        throw new IllegalArgumentException("Committed transaction IDs must be UUIDv7; got " + id);
      }
      added.add(id);
      return this;
    }

    /** {@link CommittedSet#contains}, counting the ids added so far; the horizon is inherited. */
    public boolean contains(UUID id) {
      if (added.contains(id) || base.holds(id)) {
        return true;
      }
      checkAboveHorizon(id, base.horizon);
      return false;
    }

    /** {@link CommittedSet#mayApply}, counting the ids added so far. */
    public boolean mayApply(UUID id) {
      return !added.contains(id) && !base.holds(id) && aboveHorizon(id, base.horizon);
    }

    public long horizon() {
      return base.horizon;
    }

    /**
     * Applies a GC record ({@code AdvanceHorizon}): drops every id at or below {@code
     * watermarkMs}, inherited or added, and raises the horizon to the newest one dropped. A
     * watermark at or below the horizon drops nothing new.
     */
    public Builder advanceHorizon(long watermarkMs) {
      if (watermarkMs < 0L) {
        throw new IllegalArgumentException("watermarkMs must be >= 0, got " + watermarkMs);
      }
      if (watermarkMs > 0L) {
        this.base = build().dropThrough(watermarkMs);
        added.clear();
      }
      return this;
    }

    public CommittedSet build() {
      if (added.isEmpty()) {
        return base;
      }
      return base.with(added);
    }
  }

  private static UncheckedIOException malformed(String what) {
    return new UncheckedIOException(new IOException("Malformed CommittedTransactions: " + what));
  }

  private static long readVarint(byte[] buf, int[] at, int end) {
    long value = 0;
    int shift = 0;
    while (true) {
      if (at[0] >= end) {
        throw malformed("truncated varint");
      }
      int b = buf[at[0]++] & 0xFF;
      value |= ((long) (b & 0x7F)) << shift;
      if ((b & 0x80) == 0) {
        return value;
      }
      shift += 7;
    }
  }

  private static long readFixed64(byte[] buf, int[] at) {
    if (at[0] + 8 > buf.length) {
      throw malformed("truncated fixed64");
    }
    long value = 0;
    for (int i = 7; i >= 0; i--) {
      value = (value << 8) | (buf[at[0] + i] & 0xFF);
    }
    at[0] += 8;
    return value;
  }

  private static void skipField(byte[] buf, int[] at, int tag) {
    switch (tag & 0x7) {
      case 0:
        readVarint(buf, at, buf.length);
        break;
      case 1:
        at[0] += 8;
        break;
      case 2:
        at[0] += (int) readVarint(buf, at, buf.length);
        break;
      case 5:
        at[0] += 4;
        break;
      default:
        throw malformed("wire type " + (tag & 0x7));
    }
    if (at[0] > buf.length) {
      throw malformed("truncated field");
    }
  }

  private static int writeVarint(byte[] buf, int at, long value) {
    while ((value & ~0x7FL) != 0) {
      buf[at++] = (byte) ((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    buf[at++] = (byte) value;
    return at;
  }

  private static int writeFixed64(byte[] buf, int at, long value) {
    for (int i = 0; i < 8; i++) {
      buf[at++] = (byte) (value & 0xFF);
      value >>>= 8;
    }
    return at;
  }

  private static int varintSize(long value) {
    int size = 1;
    while ((value & ~0x7FL) != 0) {
      size++;
      value >>>= 7;
    }
    return size;
  }
}
