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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.intentlog.CommittedSet;
import org.intentlog.Intention;
import org.intentlog.LogView;
import org.intentlog.Replay;
import org.intentlog.SharedLog;
import org.intentlog.StateMachine;
import org.intentlog.azure.AzureLogHead.Segment;
import org.intentlog.azure.InMemoryAppendBlobStore.Fault;
import org.intentlog.azure.InMemoryAppendBlobStore.Op;
import org.intentlog.testkit.LogHarness;
import org.intentlog.wire.LogEnvelope;

/**
 * The Azure log over the in-memory store, dressed for fuzzing. Every request the log makes of the
 * store meets the fuzzer's faults: a write may fail, be refused, be throttled before or after it
 * lands, lose its reply, or (an append) land later; a read may fail, or (a segment's) be served
 * short of its last block. The oracles see the log the store holds: every record frame that landed,
 * at its segment's base (from every head ever written) plus its offset.
 */
final class AzureLogHarness implements LogHarness {

  private final ObservedStore store;
  private final SharedLog log;
  private final String root;
  private final StateMachine<?, ?, ?> machine;
  private Hooks hooks = null;

  /**
   * A harness over {@code logs}, logs of one root on {@code store}: each request goes to the next
   * in turn, as requests from several processes would, each with its own cached head and views.
   */
  AzureLogHarness(ObservedStore store, List<AppendBlobLog> logs, String root,
      StateMachine<?, ?, ?> machine) {
    this.store = store;
    this.log = new RoundRobin(logs);
    this.root = root;
    this.machine = machine;
    store.intercept(this::fault);
  }

  private Fault fault(Op op, String name) {
    Hooks h = hooks;
    if (h == null) {
      return Fault.NONE;
    }
    boolean page = name != null && name.startsWith(root + "/pages/");
    if (page) {
      PageRequest request = switch (op) {
        case PUT, PUT_IF_ABSENT -> PageRequest.PUT;
        case DELETE -> PageRequest.DELETE;
        case LIST -> PageRequest.LIST;
        default -> PageRequest.GET;
      };
      return switch (h.beforePage(request, name.substring((root + "/pages/").length()))) {
        case NONE -> Fault.NONE;
        case FAIL -> Fault.FAIL_BEFORE;
        case LOST_REPLY -> request == PageRequest.GET ? Fault.FAIL_BEFORE : Fault.LOST_REPLY;
        case MISSING -> request == PageRequest.GET ? Fault.MISSING : Fault.FAIL_BEFORE;
      };
    }
    switch (op) {
      case READ, PROPERTIES, LIST: {
        ReadFault r = h.beforeRead();
        if (r == ReadFault.FAIL) {
          return Fault.FAIL_BEFORE;
        }
        if (r == ReadFault.STALE && op == Op.READ && name != null && name.contains("/seg/")) {
          return Fault.SHORT_READ;
        }
        return Fault.NONE;
      }
      default: {
        WriteFault w = h.beforeWrite();
        return switch (w) {
          case NONE -> Fault.NONE;
          case FAIL_BEFORE -> Fault.FAIL_BEFORE;
          case NOT_SENT -> Fault.REFUSED;
          case LOST_REPLY, LANDED_AS_CONFLICT -> Fault.LOST_REPLY;
          case THROTTLED -> Fault.THROTTLED;
          case LANDED_THROTTLED -> Fault.LANDED_THROTTLED;
          case DELAYED -> op == Op.APPEND ? Fault.DELAYED : Fault.FAIL_BEFORE;
        };
      }
    }
  }

  @Override
  public SharedLog log() {
    return log;
  }

  /** One log of several instances, each request to the next. */
  private static final class RoundRobin implements SharedLog {
    private final List<AppendBlobLog> logs;
    private int next = 0;

    RoundRobin(List<AppendBlobLog> logs) {
      this.logs = List.copyOf(logs);
    }

    private AppendBlobLog pick() {
      return logs.get(next++ % logs.size());
    }

    @Override
    public org.intentlog.Capabilities capabilities() {
      return logs.get(0).capabilities();
    }

    @Override
    public LogView read() {
      return pick().read();
    }

    @Override
    public LogView readThrough(org.intentlog.Receipt receipt, java.time.Duration wait) {
      return pick().readThrough(receipt, wait);
    }

    @Override
    public org.intentlog.LogTail probe() {
      return pick().probe();
    }

    @Override
    public org.intentlog.Receipt append(byte[] record, org.intentlog.LogTail expected) {
      return pick().append(record, expected);
    }

    @Override
    public void putCheckpoint(org.intentlog.LogTail through, byte[] checkpoint) {
      pick().putCheckpoint(through, checkpoint);
    }

    @Override
    public void trim(org.intentlog.LogTail through) {
      pick().trim(through);
    }

    @Override
    public LogView readAfter(org.intentlog.LogTail since) {
      return pick().readAfter(since);
    }

    @Override
    public org.intentlog.PageStore pages() {
      return logs.get(0).pages();
    }

    @Override
    public String toString() {
      return logs.get(0).toString();
    }
  }

  @Override
  public void hook(Hooks newHooks) {
    this.hooks = newHooks;
  }

  /** A record frame landed (not a pad), as the store holds it: its position and its record. */
  private record Landed(long position, byte[] record) {}

  /** Every record frame that landed, in log order, at its global position. */
  private List<Landed> landed() {
    Map<String, Long> bases = new LinkedHashMap<>();
    for (byte[] version : store.versions().getOrDefault(root + "/head", List.of())) {
      if (version.length == 0) {
        continue;
      }
      for (Segment s : AzureLogHead.decode(version).segments()) {
        Long was = bases.putIfAbsent(s.name(), s.base());
        if (was != null && was != s.base()) {
          throw new AssertionError("Two heads put " + s.name() + " at two bases");
        }
      }
    }
    List<Landed> out = new ArrayList<>();
    for (InMemoryAppendBlobStore.Appended a : store.appends()) {
      if (a.block().length == 0 || a.block()[0] == 0) {
        continue; // a pad
      }
      Long base = bases.get(a.name());
      if (base == null) {
        throw new AssertionError("A record landed in " + a.name() + ", which no head named");
      }
      long ordinal = Long.parseLong(
          a.name().substring(a.name().lastIndexOf('/') + 1).split("-")[0]);
      for (Frames.Frame f : Frames.parse(a.block(), a.block().length, a.offset(), ordinal)) {
        out.add(new Landed(base + f.offset(), f.body()));
      }
    }
    out.sort((x, y) -> Long.compare(x.position(), y.position()));
    return out;
  }

  @Override
  public long landedWrites() {
    return store.appends().stream()
        .filter(a -> a.block().length > 0 && a.block()[0] != 0).count();
  }

  @Override
  public List<byte[]> landedRecords() {
    Set<ByteBuffer> seen = new LinkedHashSet<>();
    for (Landed l : landed()) {
      seen.add(ByteBuffer.wrap(l.record()));
    }
    List<byte[]> records = new ArrayList<>();
    seen.forEach(b -> records.add(b.array()));
    return records;
  }

  @Override
  public void appendCopy(byte[] record) {
    Hooks was = hooks;
    hooks = null;
    try {
      log.append(record, null);
    } finally {
      hooks = was;
    }
  }

  @Override
  public Map<String, byte[]> storedPages() {
    String prefix = root + "/pages/";
    Map<String, byte[]> pages = new TreeMap<>();
    for (String name : store.names()) {
      if (name.startsWith(prefix)) {
        pages.put(name.substring(prefix.length()), store.peek(name));
      }
    }
    return pages;
  }

  @Override
  public int deliverDelayed() {
    return store.deliverDelayed();
  }

  /** Every checkpoint ever put, by position, of the log the head names. */
  private Map<Long, byte[]> checkpoints() {
    Map<Long, byte[]> out = new TreeMap<>();
    byte[] head = store.peek(root + "/head");
    if (head == null) {
      return out;
    }
    String id = AzureLogHead.decode(head).logId().toString().replace("-", "");
    store.versions().forEach((name, versions) -> {
      if (name.startsWith(root + "/ckpt/") && name.endsWith("-" + id)) {
        for (byte[] v : versions) {
          long p = AzureLogHead.position(LogEnvelope.checkpointThrough(v));
          byte[] was = out.putIfAbsent(p, v);
          if (was != null && !Arrays.equals(was, v)) {
            out.put(-1L - p, v); // two checkpoints at one position: reported below
          }
        }
      }
    });
    return out;
  }

  @Override
  public List<String> checkpointViolations() {
    return violations(machine);
  }

  private <S, B extends StateMachine.Builder<S>, I extends Intention<B>> List<String> violations(
      StateMachine<S, B, I> sm) {
    List<String> violations = new ArrayList<>();
    Map<Long, byte[]> checkpoints = checkpoints();
    if (checkpoints.isEmpty()) {
      return violations;
    }
    checkpoints.keySet().stream().filter(p -> p < 0).forEach(p ->
        violations.add("two checkpoints at position " + (-1L - p) + " differ (I11)"));
    List<Landed> landed = landed();
    byte[] first = checkpoints.get(0L);
    if (first == null) {
      violations.add("no checkpoint at position 0");
      return violations;
    }
    List<byte[]> all = new ArrayList<>();
    landed.forEach(l -> all.add(l.record()));
    byte[] whole = checkpoint(sm, Replay.replay(sm, first, all, null), null);
    for (Map.Entry<Long, byte[]> c : checkpoints.entrySet()) {
      long p = c.getKey();
      if (p < 0) {
        continue;
      }
      List<byte[]> before = new ArrayList<>();
      List<byte[]> after = new ArrayList<>();
      for (Landed l : landed) {
        (l.position() < p ? before : after).add(l.record());
      }
      byte[] replayed = checkpoint(sm, Replay.replay(sm, first, before, null), p);
      if (!Arrays.equals(replayed, c.getValue())) {
        violations.add("the checkpoint at position " + p + " is not the replay through it");
      }
      byte[] from = checkpoint(sm, Replay.replay(sm, c.getValue(), after, null), null);
      if (!Arrays.equals(from, whole)) {
        violations.add("a read from the checkpoint at " + p + " disagrees");
      }
    }
    return violations;
  }

  private static <S, B extends StateMachine.Builder<S>, I extends Intention<B>> byte[] checkpoint(
      StateMachine<S, B, I> sm, Replay.Result<B> result, Long position) {
    CommittedSet committed = result.committed().build();
    return position == null
        ? LogEnvelope.encodeCheckpoint(result.logId(), committed.bytes(),
            sm.encodeCheckpoint(result.builder().build()))
        : LogEnvelope.encodeCheckpoint(result.logId(), committed.bytes(),
            sm.encodeCheckpoint(result.builder().build()), AzureLogHead.position(position));
  }

  /**
   * A view is a state the log held if its checkpoint is one put at the position it names, and its
   * records are the log's after that position, in order, none missing.
   */
  @Override
  public boolean held(LogView view) {
    byte[] checkpoint = view.checkpoint().orElse(null);
    if (checkpoint == null) {
      return true;
    }
    byte[] through = LogEnvelope.checkpointThrough(checkpoint);
    if (through == null) {
      return false;
    }
    long p = AzureLogHead.position(through);
    if (!Arrays.equals(checkpoint, checkpoints().get(p))) {
      return false;
    }
    List<byte[]> records = new ArrayList<>();
    for (Landed l : landed()) {
      if (l.position() >= p) {
        records.add(l.record());
      }
    }
    List<byte[]> seen = view.records();
    if (seen.size() > records.size()) {
      return false;
    }
    for (int i = 0; i < seen.size(); i++) {
      if (!Arrays.equals(seen.get(i), records.get(i))) {
        return false;
      }
    }
    return true;
  }
}
