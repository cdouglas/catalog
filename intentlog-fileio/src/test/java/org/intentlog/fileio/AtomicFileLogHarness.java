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
import java.util.SplittableRandom;
import java.util.TreeMap;
import org.apache.iceberg.io.AtomicOutputFile;
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
import org.intentlog.testkit.LogHarness;
import org.intentlog.wire.LogEnvelope;

/**
 * An {@link AtomicFileLog} over an {@link InMemoryAtomicFileIO}, dressed for fuzzing. The store
 * cannot delay a write or serve a stale read, so those faults act as a failure before the write and
 * as none. It reads in a mode drawn per run ({@link InMemoryAtomicFileIO.ReadMode}): in all but
 * {@code OPEN} a write can land between a read's lookup and its GET, which a pinned store fails as
 * stale and an unpinned one serves live or spliced (at a frame's end half the time), as S3 before
 * step 0 could (TAIL_READS.md, errata T6). Every view a read returns must be a state the log held:
 * a version's checkpoint and a prefix of its records ({@link #checkpointViolations}). A copy of a record is appended at the object's offset, or, on a store without append,
 * by a CAS of the object with the copy after its records (another writer's write either way).
 * Every other CAS must write the replay of the version it replaced (I11), checked with {@code
 * machine}. The pages of the committed set are the store's objects under {@code <location>
 * .committed/}: their requests meet page faults, a page that is missing acting as a failure.
 */
public final class AtomicFileLogHarness implements LogHarness {

  private final InMemoryAtomicFileIO io;
  private final AtomicFileLog log;
  private final String pages;
  private final StateMachine<?, ?, ?> machine;
  private final HeldReads reads;
  private final List<String> readViolations = new ArrayList<>();
  private final Map<byte[], LogView> decoded = new java.util.IdentityHashMap<>();
  private Hooks hooks = null;

  public AtomicFileLogHarness(InMemoryAtomicFileIO io, String location,
      StateMachine<?, ?, ?> machine) {
    this(io, location, machine, InMemoryAtomicFileIO.ReadMode.OPEN, 0L);
  }

  /** A harness whose store reads in {@code mode}, splicing where {@code seed} draws. */
  public AtomicFileLogHarness(InMemoryAtomicFileIO io, String location,
      StateMachine<?, ?, ?> machine, InMemoryAtomicFileIO.ReadMode mode, long seed) {
    this.io = io;
    SplittableRandom random = new SplittableRandom(seed);
    this.log = new AtomicFileLog(io, location, random.split()::nextLong);
    this.reads = new HeldReads(log, location);
    this.pages = location + ".committed/";
    this.machine = machine;
    io.readMode(mode);
    SplittableRandom splices = random.split();
    io.spliceAt((bound, live) -> {
      List<Integer> ends = frameEnds(bound);
      return splices.nextBoolean() && !ends.isEmpty()
          ? ends.get(splices.nextInt(ends.size()))
          : splices.nextInt(bound.length + 1);
    });
    io.injectFaults((loc, strategy) -> hooks == null ? InMemoryAtomicFileIO.Fault.NONE
        : fault(hooks.beforeWrite()));
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
  }

  private static InMemoryAtomicFileIO.Fault fault(WriteFault fault) {
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

  public InMemoryAtomicFileIO io() {
    return io;
  }

  @Override
  public SharedLog log() {
    return reads;
  }

  @Override
  public void hook(Hooks newHooks) {
    this.hooks = newHooks;
  }

  @Override
  public long landedWrites() {
    return io.history().size();
  }

  @Override
  public List<byte[]> landedRecords() {
    Set<ByteBuffer> seen = new LinkedHashSet<>();
    for (InMemoryAtomicFileIO.Write write : io.history()) {
      for (byte[] record : AtomicFileLog.decode(write.after()).records()) {
        seen.add(ByteBuffer.wrap(record));
      }
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
      if (io.supportsAppend()) {
        log.append(record, log.probe());
      } else {
        LogView view = log.read();
        List<byte[]> records = new ArrayList<>(view.records());
        records.add(record);
        log.replace(view.tail(), view.checkpoint().orElseThrow(), records);
      }
    } finally {
      hooks = was;
    }
  }

  @Override
  public int deliverDelayed() {
    return 0;
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
  public List<String> checkpointViolations() {
    List<String> violations = new ArrayList<>(readViolations);
    violations.addAll(violations(machine));
    return violations;
  }

  /**
   * The ends of the frames of a format-3 object, after its header: where a splice that format 2
   * would have decoded falls. Empty for bytes that are not one.
   */
  static List<Integer> frameEnds(byte[] object) {
    List<Integer> ends = new ArrayList<>();
    int pos = AtomicFileLog.HEADER_SIZE;
    while (pos < object.length) {
      long length = 0;
      int at = pos;
      for (int shift = 0; ; shift += 7) {
        if (at >= object.length || shift > 28) {
          return ends;
        }
        byte b = object[at++];
        length |= (long) (b & 0x7F) << shift;
        if (b >= 0) {
          break;
        }
      }
      long end = at + length + AtomicFileLog.TRAILER_SIZE;
      if (end > object.length) {
        return ends;
      }
      pos = (int) end;
      ends.add(pos);
    }
    return ends;
  }

  /**
   * Whether {@code view} is a state the log held: some version's checkpoint, and a prefix of its
   * records (a read may return any prefix, P1). Every version the store held is in its history.
   */
  private boolean held(LogView view, String location) {
    List<byte[]> versions = new ArrayList<>();
    for (InMemoryAtomicFileIO.Write write : io.history()) {
      if (write.location().equals(location)) {
        if (write.before() != null) {
          versions.add(write.before());
        }
        versions.add(write.after());
      }
    }
    byte[] checkpoint = view.checkpoint().orElseThrow();
    for (byte[] version : versions) {
      LogView whole = decoded.computeIfAbsent(version, AtomicFileLog::decode);
      if (Arrays.equals(checkpoint, whole.checkpoint().orElseThrow())
          && startsWith(whole.records(), view.records())) {
        return true;
      }
    }
    return false;
  }

  /** The log, each view its reads return checked against what the store held. */
  private final class HeldReads implements SharedLog {
    private final AtomicFileLog log;
    private final String location;

    HeldReads(AtomicFileLog log, String location) {
      this.log = log;
      this.location = location;
    }

    private LogView checked(LogView view, String how) {
      Hooks was = hooks;
      hooks = null;
      try {
        if (!held(view, location)) {
          readViolations.add(how + " returned a state the log never held: "
              + view.records().size() + " records after a checkpoint of "
              + view.checkpoint().orElseThrow().length + " bytes");
        }
      } finally {
        hooks = was;
      }
      return view;
    }

    @Override
    public Capabilities capabilities() {
      return log.capabilities();
    }

    @Override
    public LogView read() {
      return checked(log.read(), "a read");
    }

    @Override
    public LogView readThrough(Receipt receipt, Duration wait) {
      return checked(log.readThrough(receipt, wait), "a read through a write");
    }

    @Override
    public LogTail probe() {
      return log.probe();
    }

    @Override
    public Receipt append(byte[] record, LogTail expected) {
      return log.append(record, expected);
    }

    @Override
    public Receipt replace(LogTail expected, byte[] checkpoint, List<byte[]> records) {
      return log.replace(expected, checkpoint, records);
    }

    @Override
    public LogView readAfter(LogTail since) {
      LogView view = log.readAfter(since);
      return view.checkpoint().isPresent() ? checked(view, "a read after a tail") : view;
    }

    @Override
    public PageStore pages() {
      return log.pages();
    }

    @Override
    public String toString() {
      return log.toString();
    }
  }

  private <S, B extends StateMachine.Builder<S>, I extends Intention<B>> List<String> violations(
      StateMachine<S, B, I> sm) {
    List<String> violations = new ArrayList<>();
    List<InMemoryAtomicFileIO.Write> history = io.history();
    for (int i = 0; i < history.size(); i++) {
      InMemoryAtomicFileIO.Write write = history.get(i);
      if (write.strategy() != AtomicOutputFile.Strategy.CAS || write.before() == null) {
        continue;
      }
      LogView before = AtomicFileLog.decode(write.before());
      LogView after = AtomicFileLog.decode(write.after());
      if (Arrays.equals(before.checkpoint().orElseThrow(), after.checkpoint().orElseThrow())
          && startsWith(after.records(), before.records())) {
        continue; // an append by CAS (a copy appended on a store without append): no checkpoint
      }
      Replay.Result<B> replayed =
          Replay.replay(sm, before.checkpoint().orElse(null), before.records(), null);
      CommittedSet committed = replayed.committed().build();
      byte[] expected = LogEnvelope.encodeCheckpoint(replayed.logId(), committed.bytes(),
          sm.encodeCheckpoint(replayed.builder().build()));
      byte[] written = after.checkpoint().orElseThrow();
      if (!Arrays.equals(expected, written)) {
        violations.add("CAS #" + i + " did not checkpoint the replay of the version it replaced");
      }
    }
    return violations;
  }

  private static boolean startsWith(List<byte[]> records, List<byte[]> prefix) {
    if (records.size() < prefix.size()) {
      return false;
    }
    for (int i = 0; i < prefix.size(); i++) {
      if (!Arrays.equals(records.get(i), prefix.get(i))) {
        return false;
      }
    }
    return true;
  }
}
