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

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InMemoryAtomicFileIO;
import org.intentlog.Capabilities;
import org.intentlog.CommittedSet;
import org.intentlog.Intention;
import org.intentlog.LogTail;
import org.intentlog.LogView;
import org.intentlog.PageStore;
import org.intentlog.Receipt;
import org.intentlog.Replay;
import org.intentlog.SharedLog;
import org.intentlog.StateMachine;
import org.intentlog.TrimmedException;
import org.intentlog.memory.InMemorySegmentStore;
import org.intentlog.segments.SegmentFrames;
import org.intentlog.segments.SegmentPosition;
import org.intentlog.testkit.LogHarness;
import org.intentlog.wire.LogEnvelope;

/**
 * A {@link SegmentedLog} over an {@link InMemoryAtomicFileIO} (its catalog object and pages) and
 * an {@link InMemorySegmentStore} (its segments), dressed for fuzzing. Every request of either
 * store meets the fuzzer's faults: a write may fail, be refused, be throttled before or after it
 * lands, lose its reply, or (a segment append) land later; a read may fail, or (a segment's) be
 * served short of its last block. One to three instances of the log, each with its own catalog
 * object and views, take requests in turn.
 *
 * <p>The oracles see the log the stores hold: the segments every catalog object ever installed,
 * in ordinal order, each up to its end (its limit-th record or its first seal frame), through the
 * first that hasn't ended, built from every append that landed. Every checkpoint any catalog
 * object held must be the replay of that log through its position (I11); every view a read
 * returned a state it held; and at the drain every read must read on from its tail to the log's
 * end.
 */
final class SegmentedLogHarness implements LogHarness {

  private final InMemoryAtomicFileIO io;
  private final InMemorySegmentStore store;
  private final String location;
  private final String prefix;
  private final String pages;
  private final StateMachine<?, ?, ?> machine;
  private final RoundRobin log;
  private final List<String> readViolations = new ArrayList<>();
  private Hooks hooks = null;

  SegmentedLogHarness(InMemoryAtomicFileIO io, InMemorySegmentStore store, String location,
      String prefix, List<SegmentedLog> logs, StateMachine<?, ?, ?> machine) {
    this.io = io;
    this.store = store;
    this.location = location;
    this.prefix = prefix;
    this.pages = location + ".committed/";
    this.machine = machine;
    this.log = new RoundRobin(logs);
    io.injectFaults((loc, strategy) -> hooks == null ? InMemoryAtomicFileIO.Fault.NONE
        : ioFault(hooks.beforeWrite()));
    io.injectReadFaults(loc -> {
      if (hooks == null) {
        return InMemoryAtomicFileIO.ReadFault.NONE;
      }
      boolean fails = loc.startsWith(pages)
          ? hooks.beforePage(PageRequest.GET, loc.substring(pages.length())) != PageFault.NONE
          : hooks.beforeRead() == ReadFault.FAIL;
      return fails ? InMemoryAtomicFileIO.ReadFault.FAIL : InMemoryAtomicFileIO.ReadFault.NONE;
    });
    io.injectPlainFaults((request, loc) -> {
      if (hooks == null || !loc.startsWith(pages)) {
        return InMemoryAtomicFileIO.Fault.NONE;
      }
      PageRequest page = request == InMemoryAtomicFileIO.PlainRequest.PUT ? PageRequest.PUT
          : request == InMemoryAtomicFileIO.PlainRequest.DELETE ? PageRequest.DELETE
          : PageRequest.LIST;
      switch (hooks.beforePage(page, page == PageRequest.LIST ? null
          : loc.substring(pages.length()))) {
        case NONE:
          return InMemoryAtomicFileIO.Fault.NONE;
        case LOST_REPLY:
          return InMemoryAtomicFileIO.Fault.LOST_REPLY;
        default:
          return InMemoryAtomicFileIO.Fault.FAIL_BEFORE;
      }
    });
    store.intercept(this::segmentFault);
  }

  private static InMemoryAtomicFileIO.Fault ioFault(WriteFault fault) {
    switch (fault) {
      case LOST_REPLY:
        return InMemoryAtomicFileIO.Fault.LOST_REPLY;
      case THROTTLED:
        return InMemoryAtomicFileIO.Fault.THROTTLED;
      case LANDED_THROTTLED:
        return InMemoryAtomicFileIO.Fault.LANDED_THROTTLED;
      case LANDED_AS_CONFLICT:
        return InMemoryAtomicFileIO.Fault.LANDED_AS_CONFLICT;
      case FAIL_BEFORE:
      case NOT_SENT:
      case DELAYED:
        return InMemoryAtomicFileIO.Fault.FAIL_BEFORE;
      default:
        return InMemoryAtomicFileIO.Fault.NONE;
    }
  }

  private InMemorySegmentStore.Fault segmentFault(InMemorySegmentStore.Op op, String name) {
    Hooks h = hooks;
    if (h == null) {
      return InMemorySegmentStore.Fault.NONE;
    }
    switch (op) {
      case READ:
      case LIST: {
        ReadFault r = h.beforeRead();
        if (r == ReadFault.FAIL) {
          return InMemorySegmentStore.Fault.FAIL_BEFORE;
        }
        if (r == ReadFault.STALE && op == InMemorySegmentStore.Op.READ) {
          return InMemorySegmentStore.Fault.SHORT_READ;
        }
        return InMemorySegmentStore.Fault.NONE;
      }
      default:
        return switch (h.beforeWrite()) {
          case NONE -> InMemorySegmentStore.Fault.NONE;
          case FAIL_BEFORE -> InMemorySegmentStore.Fault.FAIL_BEFORE;
          case NOT_SENT -> InMemorySegmentStore.Fault.REFUSED;
          case LOST_REPLY, LANDED_AS_CONFLICT -> InMemorySegmentStore.Fault.LOST_REPLY;
          case THROTTLED -> InMemorySegmentStore.Fault.THROTTLED;
          case LANDED_THROTTLED -> InMemorySegmentStore.Fault.LANDED_THROTTLED;
          case DELAYED -> op == InMemorySegmentStore.Op.APPEND
              ? InMemorySegmentStore.Fault.DELAYED : InMemorySegmentStore.Fault.FAIL_BEFORE;
        };
    }
  }

  @Override
  public SharedLog log() {
    return log;
  }

  @Override
  public void hook(Hooks newHooks) {
    this.hooks = newHooks;
  }

  /** One log of several instances, each request to the next, each read checked. */
  private final class RoundRobin implements SharedLog {
    private final List<SegmentedLog> logs;
    private int next = 0;

    /** Each read served: a copy of its instance as the read left it, and the tail it returned. */
    private final List<Map.Entry<SegmentedLog, LogTail>> reads = new ArrayList<>();

    RoundRobin(List<SegmentedLog> logs) {
      this.logs = List.copyOf(logs);
    }

    private SegmentedLog pick() {
      return logs.get(next++ % logs.size());
    }

    private LogView seen(SegmentedLog by, LogView view, String how) {
      reads.add(Map.entry(by.copy(), view.tail()));
      if (view.checkpoint().isPresent()) {
        Hooks was = hooks;
        hooks = null;
        try {
          if (!held(view)) {
            readViolations.add(how + " returned a state the log never held: "
                + view.records().size() + " records after "
                + SegmentPosition.decode(view.start()));
          }
        } finally {
          hooks = was;
        }
      }
      return view;
    }

    @Override
    public Capabilities capabilities() {
      return logs.get(0).capabilities();
    }

    @Override
    public LogView read() {
      SegmentedLog by = pick();
      return seen(by, by.read(), "a read");
    }

    @Override
    public LogView readThrough(Receipt receipt, Duration wait) {
      SegmentedLog by = pick();
      return seen(by, by.readThrough(receipt, wait), "a read through a write");
    }

    @Override
    public LogTail probe() {
      return pick().probe();
    }

    @Override
    public Receipt append(byte[] record, LogTail expected) {
      return pick().append(record, expected);
    }

    @Override
    public void putCheckpoint(LogTail through, byte[] checkpoint) {
      pick().putCheckpoint(through, checkpoint);
    }

    @Override
    public void trim(LogTail through) {
      pick().trim(through);
    }

    @Override
    public LogView readAfter(LogTail since) {
      SegmentedLog by = pick();
      return seen(by, by.readAfter(since), "a read after a tail");
    }

    @Override
    public PageStore pages() {
      return logs.get(0).pages();
    }

    @Override
    public String toString() {
      return logs.get(0).toString();
    }
  }

  // ============================================================
  // The log the stores hold
  // ============================================================

  /** Every version of the catalog object, oldest first. */
  private List<SegmentedLog.Cat> catalogs() {
    List<SegmentedLog.Cat> out = new ArrayList<>();
    for (InMemoryAtomicFileIO.Write write : io.history()) {
      if (write.location().equals(location)) {
        out.add(SegmentedLog.parse(AtomicFileLog.decode(write.after())));
      }
    }
    return out;
  }

  /** A record in the log: its segment, its index there (from 1), where it ends, and its bytes. */
  private record Landed(long ordinal, long index, long end, byte[] record) {
    SegmentPosition position() {
      return new SegmentPosition(ordinal, index, end);
    }
  }

  /**
   * The records of the log the stores hold: every segment any catalog object installed, in
   * ordinal order, each up to its end, through the first that hasn't ended.
   */
  private List<Landed> landed() {
    Map<Long, SegmentedLog.Seg> installed = new TreeMap<>();
    for (SegmentedLog.Cat c : catalogs()) {
      for (SegmentedLog.Seg s : c.segs()) {
        SegmentedLog.Seg was = installed.putIfAbsent(s.ordinal(), s);
        if (was != null && !was.name().equals(s.name())) {
          throw new AssertionError("SG4: two Installs of segment " + s.ordinal() + ": "
              + was.name() + " and " + s.name());
        }
      }
    }
    List<Landed> out = new ArrayList<>();
    List<InMemorySegmentStore.Landed> appends = store.appends();
    for (long o = 0; installed.containsKey(o); o++) {
      SegmentedLog.Seg seg = installed.get(o);
      String name = prefix + "/" + seg.name();
      long count = 0;
      boolean ended = false;
      for (InMemorySegmentStore.Landed a : appends) {
        if (!a.name().equals(name)) {
          continue;
        }
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
        out.add(new Landed(o, count, f.end(), f.body()));
      }
      if (!ended && count < seg.limit()) {
        break;
      }
    }
    return out;
  }

  @Override
  public long landedWrites() {
    long n = 0;
    for (InMemorySegmentStore.Landed a : store.appends()) {
      if (a.block().length > 0 && a.block()[0] != 0) {
        n++;
      }
    }
    return n;
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
    Hooks was = hooks;
    hooks = null;
    try {
      Map<String, byte[]> stored = new TreeMap<>();
      for (FileInfo file : io.listPrefix(pages)) {
        byte[] page = io.bytes(file.location());
        if (page != null) {
          stored.put(file.location().substring(pages.length()), page);
        }
      }
      return stored;
    } finally {
      hooks = was;
    }
  }

  @Override
  public int deliverDelayed() {
    return store.deliverDelayed();
  }

  // ============================================================
  // Oracles
  // ============================================================

  /** Every checkpoint any catalog object held, by its position. */
  private Map<SegmentPosition, byte[]> checkpoints(List<String> violations) {
    Map<SegmentPosition, byte[]> out = new TreeMap<>((a, b) -> {
      int c = a.compareTo(b);
      return c != 0 ? c : Long.compare(a.offset(), b.offset());
    });
    for (SegmentedLog.Cat c : catalogs()) {
      byte[] was = out.putIfAbsent(c.ck(), c.checkpoint());
      if (was != null && !Arrays.equals(was, c.checkpoint())) {
        violations.add("two checkpoints at " + c.ck() + " differ (I11)");
      }
    }
    return out;
  }

  @Override
  public List<String> checkpointViolations() {
    List<String> violations = new ArrayList<>(readViolations);
    violations.addAll(violations(machine));
    violations.addAll(staleReaders());
    return violations;
  }

  private <S, B extends StateMachine.Builder<S>, I extends Intention<B>> List<String> violations(
      StateMachine<S, B, I> sm) {
    List<String> violations = new ArrayList<>();
    Map<SegmentPosition, byte[]> checkpoints = checkpoints(violations);
    if (checkpoints.isEmpty()) {
      return violations;
    }
    byte[] first = checkpoints.get(SegmentPosition.START);
    if (first == null) {
      violations.add("no checkpoint at the log's start");
      return violations;
    }
    List<Landed> landed = landed();
    List<byte[]> all = new ArrayList<>();
    landed.forEach(l -> all.add(l.record()));
    byte[] whole = checkpoint(sm, Replay.replay(sm, first, all, null), null);
    for (Map.Entry<SegmentPosition, byte[]> c : checkpoints.entrySet()) {
      SegmentPosition p = c.getKey();
      List<byte[]> before = new ArrayList<>();
      List<byte[]> after = new ArrayList<>();
      for (Landed l : landed) {
        (l.position().compareTo(p) <= 0 ? before : after).add(l.record());
      }
      if (!p.equals(SegmentPosition.START)) {
        byte[] replayed = checkpoint(sm, Replay.replay(sm, first, before, null), p);
        if (!Arrays.equals(replayed, c.getValue())) {
          violations.add("the checkpoint at " + p + " is not the replay through it");
        }
      }
      byte[] from = checkpoint(sm, Replay.replay(sm, c.getValue(), after, null), null);
      if (!Arrays.equals(from, whole)) {
        violations.add("a read from the checkpoint at " + p + " disagrees");
      }
    }
    return violations;
  }

  private static <S, B extends StateMachine.Builder<S>, I extends Intention<B>> byte[] checkpoint(
      StateMachine<S, B, I> sm, Replay.Result<B> result, SegmentPosition position) {
    CommittedSet committed = result.committed().build();
    return position == null
        ? LogEnvelope.encodeCheckpoint(result.logId(), committed.bytes(),
            sm.encodeCheckpoint(result.builder().build()))
        : LogEnvelope.encodeCheckpoint(result.logId(), committed.bytes(),
            sm.encodeCheckpoint(result.builder().build()), position.encode());
  }

  /**
   * Reads that, from their tail and the catalog object their instance held after them, read on
   * short of the end of the log the stores hold: a process that read there and has read nothing
   * since would stay behind. Each reads as a copy of its instance, without faults.
   */
  private List<String> staleReaders() {
    List<String> stale = new ArrayList<>();
    if (io.bytes(location) == null) {
      return stale;
    }
    Hooks was = hooks;
    hooks = null;
    try {
      List<Landed> landed = landed();
      for (Map.Entry<SegmentedLog, LogTail> e : log.reads) {
        LogView view;
        try {
          view = e.getKey().copy().readAfter(e.getValue());
        } catch (TrimmedException trimmed) {
          view = e.getKey().copy().read();
        }
        SegmentPosition reached = SegmentPosition.decode(view.tail().position());
        SegmentPosition end = landed.isEmpty()
            ? reached.compareTo(SegmentPosition.START) == 0 ? reached : SegmentPosition.START
            : landed.get(landed.size() - 1).position();
        if (!landed.isEmpty() && !(reached.equals(end))) {
          stale.add("a read after " + e.getValue() + " reached " + reached
              + ", not the log's end at " + end);
        }
      }
    } finally {
      hooks = was;
    }
    return stale;
  }

  /**
   * A view is a state the log held if its checkpoint is one a catalog object held at the position
   * its records start from, and its records are the log's after that position, in order.
   */
  @Override
  public boolean held(LogView view) {
    byte[] checkpoint = view.checkpoint().orElse(null);
    if (checkpoint == null) {
      return true;
    }
    SegmentPosition p = SegmentPosition.decode(view.start());
    if (!Arrays.equals(checkpoint, checkpoints(new ArrayList<>()).get(p))) {
      return false;
    }
    List<byte[]> records = new ArrayList<>();
    for (Landed l : landed()) {
      if (l.position().compareTo(p) > 0) {
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
