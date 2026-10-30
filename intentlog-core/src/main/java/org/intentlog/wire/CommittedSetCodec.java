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
package org.intentlog.wire;

import static org.intentlog.wire.ProtoWire.readFixed64;
import static org.intentlog.wire.ProtoWire.readLengthDelimitedBytes;
import static org.intentlog.wire.ProtoWire.readVarint;
import static org.intentlog.wire.ProtoWire.skipField;
import static org.intentlog.wire.ProtoWire.writeBytes;
import static org.intentlog.wire.ProtoWire.writeFixed64;
import static org.intentlog.wire.ProtoWire.writeRawVarint64;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.UUID;

/**
 * The {@code CommittedTransactions} message: a set of UUIDv7 transaction ids, sorted descending,
 * stored as a {@code max_timestamp_ms} and packed entries of (varint timestamp delta, 10 random
 * bytes), with the GC horizon {@code highest_dropped_timestamp_ms}. Queries scan the bytes in
 * place. See docs/LOG.md "The committed set and GC".
 */
public final class CommittedSetCodec {

  public static final int COMMITTED_TXNS_MAX_TIMESTAMP_MS = 1;
  public static final int COMMITTED_TXNS_PACKED_ENTRIES = 2;
  public static final int COMMITTED_TXNS_HIGHEST_DROPPED_TIMESTAMP_MS = 3;

  private CommittedSetCodec() {}

  /**
   * Encodes a UUIDv7 set as a {@code CommittedTransactions} sub-message body
   * with no GC horizon. See {@link #encode(Collection, long)}
   * for the GC-aware variant.
   */
  public static byte[] encode(Collection<UUID> txns)
      throws IOException {
    return encode(txns, 0L);
  }

  /**
   * Encodes a UUIDv7 set as a {@code CommittedTransactions} sub-message body.
   * Sorts entries descending by UUID (= descending by timestamp for v7),
   * emits {@code max_timestamp_ms}, then a single packed bytes field whose
   * payload is the interleaved (varint delta + 10 random bytes) per entry,
   * and optionally a {@code highest_dropped_timestamp_ms} field when the
   * caller has performed a GC pass.
   *
   * <p>Caller is responsible for omitting the outer field when the set is empty
   * AND the horizon is zero (i.e., no observable state to record).
   */
  public static byte[] encode(Collection<UUID> txns,
                                             long highestDroppedTimestampMs)
      throws IOException {
    UUID[] sorted = txns.toArray(new UUID[0]);
    Arrays.sort(sorted, (a, b) -> b.compareTo(a));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    long maxTs = sorted.length == 0 ? 0L : UuidV7.timestampMs(sorted[0]);
    writeFixed64(out, COMMITTED_TXNS_MAX_TIMESTAMP_MS, maxTs);

    ByteArrayOutputStream packed = new ByteArrayOutputStream(13 * sorted.length);
    long prev = maxTs;
    for (UUID u : sorted) {
      long ts = UuidV7.timestampMs(u);
      writeRawVarint64(packed, prev - ts);
      prev = ts;
      packed.write(UuidV7.packRandomBits(u));
    }
    writeBytes(out, COMMITTED_TXNS_PACKED_ENTRIES, packed.toByteArray());
    if (highestDroppedTimestampMs != 0L) {
      writeFixed64(out, COMMITTED_TXNS_HIGHEST_DROPPED_TIMESTAMP_MS, highestDroppedTimestampMs);
    }
    return out.toByteArray();
  }

  /**
   * Single-pass rewrite of an inherited {@code CommittedTransactions} body.
   * Streams the inherited entries in descending order, drops any whose
   * {@code timestamp_ms <= dropBeforeTimestampMs} (updating
   * {@code highest_dropped_timestamp_ms}), and merges in the supplied
   * additions in descending order. Additions are emitted unconditionally —
   * the caller's invariant is that they correspond to just-applied log
   * transactions which must remain queryable regardless of the watermark.
   *
   * <p>An exact-duplicate addition (same UUID as an inherited entry) is
   * deduped: the merged output emits a single copy.
   *
   * <p>Returns an empty byte array iff there are no entries to emit AND the
   * resulting {@code highest_dropped_timestamp_ms} is zero (no observable
   * state). Otherwise returns the encoded body — including a horizon-only
   * body when every entry was dropped but the horizon is non-zero, so that
   * monotonicity is preserved across subsequent compactions.
   */
  public static byte[] rewrite(
      byte[] inheritedBytes,
      Collection<UUID> addUnconditionally,
      long dropBeforeTimestampMs) {
    UUID[] adds = addUnconditionally.toArray(new UUID[0]);
    long[] addTs = new long[adds.length];
    byte[][] addRand = new byte[adds.length][];
    for (int i = 0; i < adds.length; i++) {
      if (!UuidV7.isV7(adds[i])) {
        throw new IllegalArgumentException(
            "Committed transaction IDs must be UUIDv7; got " + adds[i]);
      }
      addTs[i] = UuidV7.timestampMs(adds[i]);
      addRand[i] = UuidV7.packRandomBits(adds[i]);
    }
    // Sort additions descending: by ts desc, then by packed-random desc
    // (lexicographic unsigned matches UUID.compareTo for v7 — see UuidV7).
    Integer[] addIdx = new Integer[adds.length];
    for (int i = 0; i < adds.length; i++) {
      addIdx[i] = i;
    }
    Arrays.sort(addIdx, (a, b) -> {
      int c = Long.compare(addTs[b], addTs[a]);
      if (c != 0) return c;
      for (int k = 0; k < 10; k++) {
        int d = (addRand[b][k] & 0xFF) - (addRand[a][k] & 0xFF);
        if (d != 0) return d;
      }
      return 0;
    });
    long[] sortedAddTs = new long[adds.length];
    byte[][] sortedAddRand = new byte[adds.length][];
    for (int i = 0; i < adds.length; i++) {
      sortedAddTs[i] = addTs[addIdx[i]];
      sortedAddRand[i] = addRand[addIdx[i]];
    }

    // Parse the inherited body's top-level fields.
    long prevMaxTs = 0L;
    int packedStart = -1;
    int packedEnd = -1;
    long prevHorizon = 0L;
    if (inheritedBytes.length > 0) {
      try {
        ByteArrayInputStream in = new ByteArrayInputStream(inheritedBytes);
        while (in.available() > 0) {
          int tag = readVarint(in);
          int fieldNumber = tag >>> 3;
          int wireType = tag & 0x7;
          switch (fieldNumber) {
            case COMMITTED_TXNS_MAX_TIMESTAMP_MS:
              prevMaxTs = readFixed64(in);
              break;
            case COMMITTED_TXNS_PACKED_ENTRIES:
              int len = readVarint(in);
              packedStart = inheritedBytes.length - in.available();
              packedEnd = packedStart + len;
              long skipped = in.skip(len);
              if (skipped != len) {
                throw new IOException(
                    "Truncated CommittedTransactions: expected " + len
                        + " packed bytes, got " + skipped);
              }
              break;
            case COMMITTED_TXNS_HIGHEST_DROPPED_TIMESTAMP_MS:
              prevHorizon = readFixed64(in);
              break;
            default:
              skipField(in, wireType);
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    // Two-pointer merge of inherited entries (in descending order) against
    // the sorted additions. Output is buffered in (ts, rand) parallel arrays
    // because we don't know max_timestamp_ms until we've seen the first
    // emitted entry.
    int inheritedUpper = packedStart < 0 ? 0 : ((packedEnd - packedStart) / 11) + 1;
    int capacity = inheritedUpper + adds.length;
    long[] outTs = new long[capacity];
    byte[][] outRand = new byte[capacity][];
    int outCount = 0;
    long newHorizon = prevHorizon;

    long ts = prevMaxTs;
    int off = packedStart < 0 ? 0 : packedStart;
    int end = packedStart < 0 ? 0 : packedEnd;
    int aPtr = 0;

    while (off < end) {
      // Decode next inherited entry: varint delta + 10 random bytes.
      long delta = 0L;
      int shift = 0;
      while (true) {
        if (off >= end) {
          throw new UncheckedIOException(new IOException(
              "Malformed CommittedTransactions: truncated varint"));
        }
        int b = inheritedBytes[off++] & 0xFF;
        delta |= ((long) (b & 0x7F)) << shift;
        if ((b & 0x80) == 0) break;
        shift += 7;
      }
      ts -= delta;
      if (off + 10 > end) {
        throw new UncheckedIOException(new IOException(
            "Malformed CommittedTransactions: truncated random bytes"));
      }
      int randOff = off;
      off += 10;

      // Emit any additions strictly greater than the inherited entry first.
      while (aPtr < sortedAddTs.length
          && compareTsRand(sortedAddTs[aPtr], sortedAddRand[aPtr],
              ts, inheritedBytes, randOff) > 0) {
        outTs[outCount] = sortedAddTs[aPtr];
        outRand[outCount] = sortedAddRand[aPtr];
        outCount++;
        aPtr++;
      }

      if (ts <= dropBeforeTimestampMs) {
        // Drop this inherited entry; the addition (if any) at the same key
        // still stands and will be emitted from the addition stream.
        if (ts > newHorizon) {
          newHorizon = ts;
        }
      } else {
        // Dedup: an addition equal to this inherited entry collapses to one.
        if (aPtr < sortedAddTs.length
            && compareTsRand(sortedAddTs[aPtr], sortedAddRand[aPtr],
                ts, inheritedBytes, randOff) == 0) {
          aPtr++;
        }
        outTs[outCount] = ts;
        byte[] copy = new byte[10];
        System.arraycopy(inheritedBytes, randOff, copy, 0, 10);
        outRand[outCount] = copy;
        outCount++;
      }
    }

    // Drain remaining additions (older than the smallest inherited entry, or
    // older-than-watermark inherited that got dropped left holes in the
    // descending stream — additions still survive unconditionally).
    while (aPtr < sortedAddTs.length) {
      outTs[outCount] = sortedAddTs[aPtr];
      outRand[outCount] = sortedAddRand[aPtr];
      outCount++;
      aPtr++;
    }

    if (outCount == 0 && newHorizon == 0L) {
      return new byte[0];
    }

    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      long maxTs = outCount > 0 ? outTs[0] : 0L;
      writeFixed64(out, COMMITTED_TXNS_MAX_TIMESTAMP_MS, maxTs);

      ByteArrayOutputStream packed = new ByteArrayOutputStream(13 * outCount);
      long prev = maxTs;
      for (int i = 0; i < outCount; i++) {
        long entryTs = outTs[i];
        writeRawVarint64(packed, prev - entryTs);
        prev = entryTs;
        packed.write(outRand[i]);
      }
      writeBytes(out, COMMITTED_TXNS_PACKED_ENTRIES, packed.toByteArray());

      if (newHorizon != 0L) {
        writeFixed64(out, COMMITTED_TXNS_HIGHEST_DROPPED_TIMESTAMP_MS, newHorizon);
      }
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Compares an addition entry {@code (aTs, aRand)} against an inherited entry
   * stored in {@code body[bOff..bOff+10)} with timestamp {@code bTs}. Negative
   * iff a < b, zero iff equal, positive iff a > b.
   */
  private static int compareTsRand(long aTs, byte[] aRand, long bTs, byte[] body, int bOff) {
    if (aTs != bTs) {
      return Long.compare(aTs, bTs);
    }
    for (int i = 0; i < 10; i++) {
      int cmp = (aRand[i] & 0xFF) - (body[bOff + i] & 0xFF);
      if (cmp != 0) return cmp;
    }
    return 0;
  }

  /**
   * Scans the inner {@code CommittedTransactions} body for a v7 UUID. Returns
   * true iff the set contains the query. Walks the wire bytes linearly with
   * no per-call allocation: tracks the running timestamp by summing varint
   * deltas, compares the random-bits region of each entry against the
   * query's packed bits in place, and exits early when the running timestamp
   * passes the query's timestamp (descending order).
   *
   * <p>Returns false for non-v7 queries (the dedup set only stores v7 IDs).
   */
  public static boolean contains(byte[] body, UUID query) {
    if (body.length == 0 || !UuidV7.isV7(query)) {
      return false;
    }
    long queryTs = UuidV7.timestampMs(query);
    byte[] queryRand = UuidV7.packRandomBits(query);
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(body);
      long maxTs = 0L;
      int packedStart = -1;
      int packedEnd = -1;
      while (in.available() > 0) {
        int tag = readVarint(in);
        int fieldNumber = tag >>> 3;
        int wireType = tag & 0x7;
        switch (fieldNumber) {
          case COMMITTED_TXNS_MAX_TIMESTAMP_MS:
            maxTs = readFixed64(in);
            break;
          case COMMITTED_TXNS_PACKED_ENTRIES:
            int len = readVarint(in);
            packedStart = body.length - in.available();
            packedEnd = packedStart + len;
            in.skip(len);
            break;
          // COMMITTED_TXNS_HIGHEST_DROPPED_TIMESTAMP_MS: ignored here; the
          // membership predicate doesn't consult the horizon. The horizon-aware
          // tri-state result lives in CommittedSet#contains,
          // which calls horizon on a miss.
          default:
            skipField(in, wireType);
        }
      }
      if (packedStart < 0 || queryTs > maxTs) {
        return false;
      }
      long ts = maxTs;
      int off = packedStart;
      while (off < packedEnd) {
        // Inline varint read to avoid wrapping with InputStream.
        long delta = 0;
        int shift = 0;
        while (true) {
          int b = body[off++] & 0xFF;
          delta |= ((long) (b & 0x7F)) << shift;
          if ((b & 0x80) == 0) break;
          shift += 7;
        }
        ts -= delta;
        if (off + 10 > packedEnd) {
          return false; // malformed; treat as not-contained
        }
        if (ts == queryTs && randomMatches(body, off, queryRand)) {
          return true;
        }
        if (ts < queryTs) {
          return false; // descending order: any further entries are older
        }
        off += 10;
      }
      return false;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Reads the {@code highest_dropped_timestamp_ms} field from a committed-set
   * body. Returns 0 when the body is empty, the field is absent, or no GC has
   * ever fired on this set. Walks only the top-level fields and skips the
   * (potentially large) {@code packed_entries} payload, so this is cheap.
   */
  public static long horizon(byte[] body) {
    if (body.length == 0) {
      return 0L;
    }
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(body);
      long horizon = 0L;
      while (in.available() > 0) {
        int tag = readVarint(in);
        int fieldNumber = tag >>> 3;
        int wireType = tag & 0x7;
        if (fieldNumber == COMMITTED_TXNS_HIGHEST_DROPPED_TIMESTAMP_MS) {
          horizon = readFixed64(in);
        } else {
          skipField(in, wireType);
        }
      }
      return horizon;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static boolean randomMatches(byte[] body, int off, byte[] query) {
    for (int i = 0; i < 10; i++) {
      if (body[off + i] != query[i]) return false;
    }
    return true;
  }

  /**
   * Decodes a {@code CommittedTransactions} body into the canonical {@code Set<UUID>}.
   * Used by the {@code CommittedSet.ids()} bulk getter; not on the query hot path.
   */
  public static LinkedHashSet<UUID> decode(byte[] body)
      throws IOException {
    LinkedHashSet<UUID> out = new LinkedHashSet<>();
    if (body.length == 0) {
      return out;
    }
    ByteArrayInputStream in = new ByteArrayInputStream(body);
    long maxTs = 0L;
    byte[] packed = new byte[0];
    while (in.available() > 0) {
      int tag = readVarint(in);
      int fieldNumber = tag >>> 3;
      int wireType = tag & 0x7;
      switch (fieldNumber) {
        case COMMITTED_TXNS_MAX_TIMESTAMP_MS:
          maxTs = readFixed64(in);
          break;
        case COMMITTED_TXNS_PACKED_ENTRIES:
          packed = readLengthDelimitedBytes(in);
          break;
        // COMMITTED_TXNS_HIGHEST_DROPPED_TIMESTAMP_MS: skipped — this bulk
        // entrypoint surfaces only the entries, not the GC horizon. Callers
        // that need the horizon use horizon (cheap) or peek at
        // it during a single-pass merge in rewrite.
        default:
          skipField(in, wireType);
      }
    }
    long ts = maxTs;
    int off = 0;
    while (off < packed.length) {
      int start = off;
      long delta = 0;
      int shift = 0;
      while (true) {
        if (off >= packed.length) {
          throw new IOException("Malformed CommittedTransactions: truncated varint at " + start);
        }
        int b = packed[off++] & 0xFF;
        delta |= ((long) (b & 0x7F)) << shift;
        if ((b & 0x80) == 0) break;
        shift += 7;
      }
      ts -= delta;
      if (off + 10 > packed.length) {
        throw new IOException(
            "Malformed CommittedTransactions: entry at offset " + start
                + " missing 10 random bytes (have " + (packed.length - off) + ")");
      }
      out.add(UuidV7.fromTimestampAndRandom(ts, packed, off));
      off += 10;
    }
    return out;
  }
}
