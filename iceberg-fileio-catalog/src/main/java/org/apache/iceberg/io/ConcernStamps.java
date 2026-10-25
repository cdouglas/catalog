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
 * An encoding's state of a table as stamps, each the version of the table at which something last
 * changed: each of a fixed set of the table's concerns; each kind of change ({@link Ref}) to the
 * refs of each of a fixed number of ref buckets (a ref's name hashed); and each kind of change
 * ({@link Partition}) to the files of each of a fixed number of partition buckets. Stamps only
 * grow, so a witness that a stamp is at or below the version its writer read, once false, stays
 * false (S2). 0 is never changed since the table was created, before any version a writer can read.
 */
final class ConcernStamps implements EncodingState {

  /** Changes to a ref. */
  enum Ref {
    /** A commit to the branch: a new snapshot on its head. */
    ADVANCE,
    /** Any other change of its snapshot: a rollback, a fast-forward, a ref created or removed. */
    RESET,
    /** An advance whose snapshot drops manifests from its parent's list (a delete, a rewrite). */
    DROP,
    /** New retention, on the snapshot it had. */
    RETENTION
  }

  /** Changes to the files of a partition. */
  enum Partition {
    /** Delete files added. */
    DELETES_ADDED,
    /** Data files added. */
    DATA_ADDED,
    /** Data files removed. */
    DATA_REMOVED
  }

  private final int[] concerns;
  private final int[][] refs;
  private final int[][] partitions;

  ConcernStamps(int concerns, int refKinds, int refBuckets, int partitionKinds,
      int partitionBuckets) {
    this(new int[concerns], new int[refKinds][refBuckets],
        new int[partitionKinds][partitionBuckets]);
  }

  private ConcernStamps(int[] concerns, int[][] refs, int[][] partitions) {
    this.concerns = concerns;
    this.refs = refs;
    this.partitions = partitions;
  }

  /** The version at which concern {@code concern} last changed. */
  int concern(int concern) {
    return concerns[concern];
  }

  /** The version at which a ref in {@code bucket} last changed by {@code kind}. */
  int ref(Ref kind, int bucket) {
    return refs[kind.ordinal()][bucket];
  }

  /** The version at which a partition in {@code bucket} last changed by {@code kind}. */
  int partition(Partition kind, int bucket) {
    return partitions[kind.ordinal()][bucket];
  }

  /** The bucket of the ref named {@code ref}. */
  int bucket(String ref) {
    return Math.floorMod(ref.hashCode(), refs[0].length);
  }

  int partitionBuckets() {
    return partitions.length > 0 ? partitions[0].length : 0;
  }

  void stampConcern(int concern, int version) {
    concerns[concern] = Math.max(concerns[concern], version);
  }

  void stampRef(Ref kind, String ref, int version) {
    int bucket = bucket(ref);
    refs[kind.ordinal()][bucket] = Math.max(refs[kind.ordinal()][bucket], version);
  }

  /** Stamps partition bucket {@code bucket}, or every one if it is {@link PartitionKey#ALL}. */
  void stampPartition(Partition kind, int bucket, int version) {
    int[] stamps = partitions[kind.ordinal()];
    if (bucket == PartitionKey.ALL) {
      Arrays.setAll(stamps, i -> Math.max(stamps[i], version));
    } else {
      stamps[bucket] = Math.max(stamps[bucket], version);
    }
  }

  /** Stamps everything: a change the encoding cannot see into. */
  void stampAll(int version) {
    for (int i = 0; i < concerns.length; i++) {
      stampConcern(i, version);
    }
    for (int[][] kinds : new int[][][] {refs, partitions}) {
      for (int[] stamps : kinds) {
        Arrays.setAll(stamps, i -> Math.max(stamps[i], version));
      }
    }
  }

  @Override
  public EncodingState copy() {
    return new ConcernStamps(concerns.clone(), deepClone(refs), deepClone(partitions));
  }

  private static int[][] deepClone(int[][] stamps) {
    int[][] copy = new int[stamps.length][];
    for (int i = 0; i < stamps.length; i++) {
      copy[i] = stamps[i].clone();
    }
    return copy;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ConcernStamps)) {
      return false;
    }
    ConcernStamps that = (ConcernStamps) other;
    return Arrays.equals(concerns, that.concerns)
        && Arrays.deepEquals(refs, that.refs)
        && Arrays.deepEquals(partitions, that.partitions);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(concerns) * 31 + Arrays.deepHashCode(refs) * 7
        + Arrays.deepHashCode(partitions);
  }

  @Override
  public String toString() {
    return "concerns " + Arrays.toString(concerns) + ", refs " + Arrays.deepToString(refs)
        + (partitions.length > 0 ? ", partitions " + Arrays.deepToString(partitions) : "");
  }

  /**
   * The stamps' codec: the concerns', then each ref kind's buckets, then each partition kind's
   * buckets, as varints (field 1, repeated, unpacked); none for a table never observed.
   */
  static final class Codec implements EncodingState.Codec {
    private static final int STAMP = 1;
    private final int concerns;
    private final int refKinds;
    private final int refBuckets;
    private final int partitionKinds;
    private final int partitionBuckets;

    Codec(int concerns, int refKinds, int refBuckets, int partitionKinds, int partitionBuckets) {
      this.concerns = concerns;
      this.refKinds = refKinds;
      this.refBuckets = refBuckets;
      this.partitionKinds = partitionKinds;
      this.partitionBuckets = partitionBuckets;
    }

    @Override
    public EncodingState empty() {
      return new ConcernStamps(concerns, refKinds, refBuckets, partitionKinds, partitionBuckets);
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
        for (int[][] kinds : new int[][][] {stamps.refs, stamps.partitions}) {
          for (int[] kind : kinds) {
            for (int stamp : kind) {
              ProtoWire.writeVarint(out, STAMP, stamp);
            }
          }
        }
        return out.toByteArray();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public EncodingState decode(byte[] bytes) {
      int[] values =
          new int[concerns + refKinds * refBuckets + partitionKinds * partitionBuckets];
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
      int at = concerns;
      int[][] refs = new int[refKinds][];
      for (int k = 0; k < refKinds; k++, at += refBuckets) {
        refs[k] = Arrays.copyOfRange(values, at, at + refBuckets);
      }
      int[][] partitions = new int[partitionKinds][];
      for (int k = 0; k < partitionKinds; k++, at += partitionBuckets) {
        partitions[k] = Arrays.copyOfRange(values, at, at + partitionBuckets);
      }
      return new ConcernStamps(Arrays.copyOf(values, concerns), refs, partitions);
    }
  }
}
