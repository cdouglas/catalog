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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.intentlog.azure.AzureLogHead.Segment;
import org.intentlog.wire.LogEnvelope;

/**
 * What an {@link InMemoryAppendBlobStore} that kept its history ({@link
 * InMemoryAppendBlobStore#recordHistory}) holds of the log at a root, for oracles: every record
 * frame that ever landed, in log order at its global position (rejected records and copies
 * included, pads not), every checkpoint ever put, by the position it covers, and the pages the
 * store holds now. As {@code InMemorySharedLog}'s records, checkpoints and stored pages are for
 * a shared log in memory.
 */
public final class AzureLogHistory {

  /** A record frame that landed: its global position and its record. */
  public record Landed(long position, byte[] record) {}

  private final List<Landed> records;
  private final Map<Long, byte[]> checkpoints;
  private final Map<String, byte[]> pages;
  private final List<String> violations;

  private AzureLogHistory(List<Landed> records, Map<Long, byte[]> checkpoints,
      Map<String, byte[]> pages, List<String> violations) {
    this.records = records;
    this.checkpoints = checkpoints;
    this.pages = pages;
    this.violations = violations;
  }

  /** The history of the log at {@code root} of {@code store}; empty if it has no head. */
  public static AzureLogHistory of(InMemoryAppendBlobStore store, String root) {
    List<String> violations = new ArrayList<>();
    Map<String, byte[]> pages = new TreeMap<>();
    String pagePrefix = root + "/pages/";
    for (String name : store.names()) {
      if (name.startsWith(pagePrefix)) {
        pages.put(name.substring(pagePrefix.length()), store.peek(name));
      }
    }
    byte[] headNow = store.peek(root + "/head");
    if (headNow == null) {
      return new AzureLogHistory(List.of(), Map.of(), pages, violations);
    }
    Map<String, List<byte[]>> versions = store.versions();

    // Each segment's base, from every head ever put
    Map<String, Long> bases = new HashMap<>();
    for (byte[] version : versions.getOrDefault(root + "/head", List.of())) {
      if (version.length == 0) {
        continue;
      }
      for (Segment s : AzureLogHead.decode(version).segments()) {
        Long was = bases.putIfAbsent(s.name(), s.base());
        if (was != null && was != s.base()) {
          violations.add("two heads put " + s.name() + " at two bases");
        }
      }
    }

    List<Landed> records = new ArrayList<>();
    String segPrefix = root + "/seg/";
    for (InMemoryAppendBlobStore.Appended a : store.appends()) {
      if (!a.name().startsWith(segPrefix) || a.block().length == 0 || a.block()[0] == 0) {
        continue; // another log's, or a pad
      }
      Long base = bases.get(a.name());
      if (base == null) {
        violations.add("a record landed in " + a.name() + ", which no head named");
        continue;
      }
      long ordinal = Long.parseLong(
          a.name().substring(segPrefix.length()).split("-")[0]);
      for (Frames.Frame f : Frames.parse(a.block(), a.block().length, a.offset(), ordinal)) {
        records.add(new Landed(base + f.offset(), f.body()));
      }
    }
    records.sort((x, y) -> Long.compare(x.position(), y.position()));

    Map<Long, byte[]> checkpoints = new TreeMap<>();
    String id = AzureLogHead.decode(headNow).logId().toString().replace("-", "");
    String ckptPrefix = root + "/ckpt/";
    versions.forEach((name, put) -> {
      if (name.startsWith(ckptPrefix) && name.endsWith("-" + id)) {
        for (byte[] v : put) {
          long p = AzureLogHead.position(LogEnvelope.checkpointThrough(v));
          byte[] was = checkpoints.putIfAbsent(p, v);
          if (was != null && !Arrays.equals(was, v)) {
            violations.add("two checkpoints at position " + p + " differ (I11)");
          }
        }
      }
    });
    return new AzureLogHistory(List.copyOf(records), checkpoints, pages, violations);
  }

  /** Every record frame that landed, in log order. */
  public List<Landed> records() {
    return records;
  }

  /** Every checkpoint ever put, by the position it covers: the log's first at 0. */
  public Map<Long, byte[]> checkpoints() {
    return checkpoints;
  }

  /** The number of records that landed before {@code position}. */
  public int recordsBefore(long position) {
    int n = 0;
    while (n < records.size() && records.get(n).position() < position) {
      n++;
    }
    return n;
  }

  /** The pages the store holds now, by name under the log's {@code pages/}. */
  public Map<String, byte[]> pages() {
    return pages;
  }

  /** What the history itself breaks: two checkpoints at a position, a record in no segment. */
  public List<String> violations() {
    return violations;
  }
}
