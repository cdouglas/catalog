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
package org.apache.iceberg.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import org.intentlog.wire.ProtoWire;

/**
 * An encoding's state of a table as stamps: for each of a fixed set of the table's concerns, and
 * for each of a fixed number of ref buckets (a ref's name hashed), the version of the table at which
 * it last changed. A ref bucket has two stamps: one an advance of a ref in it moves (a commit to
 * the branch), one a reset moves (any other change: a rollback, a fast-forward, a ref created,
 * removed, or with new retention). Stamps only grow, so a witness that a stamp is at or below the
 * version its writer read, once false, stays false (S2). 0 is never changed since the table was
 * created, before any version a writer can read.
 */
final class ConcernStamps implements EncodingState {

  private final int[] concerns;
  private final int[] advances;
  private final int[] resets;

  ConcernStamps(int concerns, int buckets) {
    this(new int[concerns], new int[buckets], new int[buckets]);
  }

  private ConcernStamps(int[] concerns, int[] advances, int[] resets) {
    this.concerns = concerns;
    this.advances = advances;
    this.resets = resets;
  }

  int concerns() {
    return concerns.length;
  }

  int buckets() {
    return advances.length;
  }

  /** The version at which concern {@code concern} last changed. */
  int concern(int concern) {
    return concerns[concern];
  }

  /** The version at which a ref in {@code bucket} last advanced. */
  int advance(int bucket) {
    return advances[bucket];
  }

  /** The version at which a ref in {@code bucket} was last reset. */
  int reset(int bucket) {
    return resets[bucket];
  }

  /** The bucket of the ref named {@code ref}. */
  int bucket(String ref) {
    return Math.floorMod(ref.hashCode(), advances.length);
  }

  void stampConcern(int concern, int version) {
    concerns[concern] = Math.max(concerns[concern], version);
  }

  void stampAdvance(String ref, int version) {
    int bucket = bucket(ref);
    advances[bucket] = Math.max(advances[bucket], version);
  }

  void stampReset(String ref, int version) {
    int bucket = bucket(ref);
    resets[bucket] = Math.max(resets[bucket], version);
  }

  /** Stamps every concern and every ref bucket: a change the encoding cannot see into. */
  void stampAll(int version) {
    for (int i = 0; i < concerns.length; i++) {
      stampConcern(i, version);
    }
    for (int i = 0; i < advances.length; i++) {
      advances[i] = Math.max(advances[i], version);
      resets[i] = Math.max(resets[i], version);
    }
  }

  @Override
  public EncodingState copy() {
    return new ConcernStamps(concerns.clone(), advances.clone(), resets.clone());
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ConcernStamps)) {
      return false;
    }
    ConcernStamps that = (ConcernStamps) other;
    return Arrays.equals(concerns, that.concerns)
        && Arrays.equals(advances, that.advances)
        && Arrays.equals(resets, that.resets);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(concerns) * 31 + Arrays.hashCode(advances) * 7
        + Arrays.hashCode(resets);
  }

  @Override
  public String toString() {
    return "concerns " + Arrays.toString(concerns) + ", advances " + Arrays.toString(advances)
        + ", resets " + Arrays.toString(resets);
  }

  /**
   * The stamps' codec: the concerns', then each bucket's advance and reset, as varints (field 1,
   * repeated, unpacked); none for a table never observed.
   */
  static final class Codec implements EncodingState.Codec {
    private static final int STAMP = 1;
    private final int concerns;
    private final int buckets;

    Codec(int concerns, int buckets) {
      this.concerns = concerns;
      this.buckets = buckets;
    }

    @Override
    public EncodingState empty() {
      return new ConcernStamps(concerns, buckets);
    }

    @Override
    public byte[] encode(EncodingState state) {
      ConcernStamps stamps = (ConcernStamps) state;
      if (stamps.equals(empty())) {
        return new byte[0];
      }
      try {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int stamp : stamps.concerns) {
          ProtoWire.writeVarint(out, STAMP, stamp);
        }
        for (int i = 0; i < stamps.advances.length; i++) {
          ProtoWire.writeVarint(out, STAMP, stamps.advances[i]);
          ProtoWire.writeVarint(out, STAMP, stamps.resets[i]);
        }
        return out.toByteArray();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public EncodingState decode(byte[] bytes) {
      int[] values = new int[concerns + 2 * buckets];
      int n = 0;
      try {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        while (in.available() > 0) {
          int tag = ProtoWire.readVarint(in);
          if (tag >>> 3 == STAMP && n < values.length) {
            values[n++] = ProtoWire.readVarint(in);
          } else {
            ProtoWire.skipField(in, tag & 0x7);
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      int[] advances = new int[buckets];
      int[] resets = new int[buckets];
      for (int i = 0; i < buckets; i++) {
        advances[i] = values[concerns + 2 * i];
        resets[i] = values[concerns + 2 * i + 1];
      }
      return new ConcernStamps(Arrays.copyOf(values, concerns), advances, resets);
    }
  }
}
