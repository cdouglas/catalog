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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.intentlog.segments.SegmentFrames;
import org.intentlog.segments.SegmentPosition;

/**
 * A segmented log as its stores held it, for tests' checks: every version its catalog object had,
 * and every append that landed in a segment, deleted ones included. The log is the segments the
 * versions installed, in ordinal order, each up to its end (its limit-th record, or its first seal
 * frame), through the first that hasn't ended; its checkpoints are the versions', by position.
 */
public final class SegmentedLogHistory {

  /** An append that landed in a segment: the segment's name in its store, where, and the block. */
  public record Append(String name, long offset, byte[] block) {}

  /** A record in the log: its position (the end of its frame) and its bytes. */
  public record Landed(SegmentPosition position, byte[] record) {}

  private final List<Landed> records = new ArrayList<>();
  private final Map<SegmentPosition, byte[]> checkpoints = new TreeMap<>(
      (a, b) -> a.compareTo(b) != 0 ? a.compareTo(b) : Long.compare(a.offset(), b.offset()));
  private final List<String> violations = new ArrayList<>();
  private final java.util.Set<String> names = new java.util.TreeSet<>();
  private final Map<String, byte[]> pages;

  /**
   * The log whose catalog object had {@code versions} (oldest first, each the object's bytes) and
   * whose segments, named under {@code <prefix>/<log id>/} in their store (the prefix the versions
   * record, else {@code prefix}), took {@code appends} (in order); {@code pages} the committed
   * set's pages stored.
   */
  public SegmentedLogHistory(List<byte[]> versions, List<Append> appends, String prefix,
      Map<String, byte[]> pages) {
    this.pages = pages;
    Map<Long, SegmentedLog.Seg> installed = new TreeMap<>();
    for (byte[] version : versions) {
      SegmentedLog.Cat c = SegmentedLog.parse(AtomicFileLog.decode(version), prefix);
      byte[] was = checkpoints.putIfAbsent(c.ck(), c.checkpoint());
      if (was != null && !Arrays.equals(was, c.checkpoint())) {
        violations.add("two checkpoints at " + c.ck() + " differ (I11)");
      }
      c.retired().forEach(r -> names.add(c.logId() + "/" + r.name()));
      for (SegmentedLog.Seg s : c.segs()) {
        names.add(s.logId() + "/" + s.name());
        SegmentedLog.Seg other = installed.putIfAbsent(s.ordinal(), s);
        if (other != null && !other.name().equals(s.name())) {
          violations.add("two Installs of segment " + s.ordinal() + " (SG4)");
        }
      }
    }
    Map<String, List<Append>> bySegment = new LinkedHashMap<>();
    for (Append a : appends) {
      bySegment.computeIfAbsent(a.name(), k -> new ArrayList<>()).add(a);
    }
    for (long o = 0; installed.containsKey(o); o++) {
      SegmentedLog.Seg seg = installed.get(o);
      long count = 0;
      boolean ended = false;
      String name = SegmentedLog.path(seg.prefix(), seg.logId(), seg.name());
      for (Append a : bySegment.getOrDefault(name, List.of())) {
        if (count >= seg.limit()) {
          ended = true;
          break;
        }
        SegmentFrames.Frame f =
            SegmentFrames.parse(a.block(), a.block().length, a.offset(), o).get(0);
        if (f.seal()) {
          ended = true;
          break;
        }
        count++;
        records.add(new Landed(new SegmentPosition(o, count, f.end()), f.body()));
      }
      if (!ended && count < seg.limit()) {
        break;
      }
    }
  }

  /** Every record in the log, in order. */
  public List<Landed> records() {
    return records;
  }

  /** Every checkpoint the catalog object held, by position. */
  public Map<SegmentPosition, byte[]> checkpoints() {
    return checkpoints;
  }

  /** The records before {@code position}: those a checkpoint there covers. */
  public int recordsBefore(SegmentPosition position) {
    int n = 0;
    for (Landed l : records) {
      if (l.position().compareTo(position) <= 0) {
        n++;
      }
    }
    return n;
  }

  public Map<String, byte[]> pages() {
    return pages;
  }

  /**
   * Where every segment a version installed or retired is, under the prefix: {@code <log
   * id>/<name>}.
   */
  public java.util.Set<String> names() {
    return names;
  }

  /** What the history itself breaks: two checkpoints at a position, two Installs of a segment. */
  public List<String> violations() {
    return violations;
  }
}
