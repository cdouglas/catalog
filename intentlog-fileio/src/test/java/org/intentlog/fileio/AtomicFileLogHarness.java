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
 * stale and an unpinned one serves live or spliced (a third of the time at a frame's end, a third
 * inside the checkpoint's frame), as S3 before step 0 could (TAIL_READS.md, errata T6). Every view a read returns must be a state the log held:
 * a version's checkpoint and a prefix of its records ({@link #checkpointViolations}). A copy of a record is appended at the object's offset, or, on a store without append,
 * by a CAS of the object with the copy after its records (another writer's write either way).
 * Every other CAS must write the replay of the version it replaced (I11), checked with {@code
 * machine}. The pages of the committed set are the store's objects under {@code <location>
 * .committed/}: their requests meet page faults, a page that is missing acting as a failure.
 */
public final class AtomicFileLogHarness implements LogHarness {

  private final InMemoryAtomicFileIO io;
  private final AtomicFileLog log;
  private final String location;
  private final String pages;
  private final StateMachine<?, ?, ?> machine;
  private final HeldReads reads;
  private final List<String> readViolations = new ArrayList<>();
  private final Map<byte[], LogView> decoded = new java.util.IdentityHashMap<>();
  // In OPEN and PINNED modes no write lands between a read's lookup and its GET, or one that does
  // fails the GET: a fresh read of the one object is a whole version, no older than the object
  // was when the read began (the runtime's settles() rests on it; the 2026-11-17 review's B8). A
  // writer that preempts inside the read may read a later one first: its read began later
  private final boolean whole;
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
    this.location = location;
    this.pages = location + ".committed/";
    this.machine = machine;
    this.whole = mode == InMemoryAtomicFileIO.ReadMode.OPEN
        || mode == InMemoryAtomicFileIO.ReadMode.PINNED;
    io.readMode(mode);
    SplittableRandom splices = random.split();
    // A third at a frame's end (format 2 would decode it), a third inside the checkpoint's frame
    // (only the checksum sees one whose new object has a checkpoint as long), a third anywhere
    io.spliceAt((bound, live) -> {
      List<Integer> ends = frameEnds(bound);
      int pick = splices.nextInt(3);
      if (pick == 0 && !ends.isEmpty()) {
        return ends.get(splices.nextInt(ends.size()));
      }
      if (pick == 1 && !ends.isEmpty()) {
        return AtomicFileLog.HEADER_SIZE + splices.nextInt(ends.get(0) - AtomicFileLog.HEADER_SIZE);
      }
      return splices.nextInt(bound.length + 1);
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
      if (io.supportsAppend(location)) {
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

  @Override
  public boolean held(LogView view) {
    Hooks was = hooks;
    hooks = null;
    try {
      return held(view, reads.location);
    } finally {
      hooks = was;
    }
  }

  @Override
  public boolean tracksTails() {
    return true;
  }

  /**
   * The version the store held whose frames, through the tail's length, end in a trailer of the
   * tail's incarnation (its position: the incarnation and the length, little-endian), cut there.
   * Appends only extend a version, so every version of one incarnation that long agrees there.
   */
  @Override
  public LogView viewAt(LogTail tail) {
    byte[] at = tail.position();
    if (at == null || at.length != 2 * Long.BYTES) {
      return null;
    }
    long incarnation = littleEndian(at, 0);
    long length = littleEndian(at, Long.BYTES);
    for (InMemoryAtomicFileIO.Write write : io.history()) {
      if (!write.location().equals(reads.location)) {
        continue;
      }
      for (byte[] version : Arrays.asList(write.before(), write.after())) {
        if (version != null && length <= version.length
            && frameEnds(version).contains((int) length)
            && littleEndian(version, (int) length - AtomicFileLog.TRAILER_SIZE) == incarnation) {
          return AtomicFileLog.decode(Arrays.copyOf(version, (int) length));
        }
      }
    }
    return null;
  }

  private static long littleEndian(byte[] bytes, int at) {
    long value = 0;
    for (int i = Long.BYTES - 1; i >= 0; i--) {
      value = (value << 8) | (bytes[at + i] & 0xFFL);
    }
    return value;
  }

  /**
   * Whether {@code view} is a state the log held: some version's checkpoint, and a prefix of its
   * records (a read may return any prefix, P1). Every version the store held is in its history.
   */
  private boolean held(LogView view, String location) {
    return versionOf(view, location, false) >= 0;
  }

  /** Every version of the object at {@code location}, in the order the store held them. */
  private List<byte[]> versions(String location) {
    List<byte[]> versions = new ArrayList<>();
    for (InMemoryAtomicFileIO.Write write : io.history()) {
      if (write.location().equals(location)) {
        if (write.before() != null) {
          versions.add(write.before());
        }
        versions.add(write.after());
      }
    }
    return versions;
  }

  /**
   * The latest of {@link #versions} that holds {@code view}: its checkpoint, and its records
   * exactly if {@code exactly}, else a prefix of them; -1 if none does.
   */
  private int versionOf(LogView view, String location, boolean exactly) {
    List<byte[]> versions = versions(location);
    byte[] checkpoint = view.checkpoint().orElseThrow();
    for (int i = versions.size() - 1; i >= 0; i--) {
      LogView version = decoded.computeIfAbsent(versions.get(i), AtomicFileLog::decode);
      if (Arrays.equals(checkpoint, version.checkpoint().orElseThrow())
          && (exactly ? sameRecords(version.records(), view.records())
              : startsWith(version.records(), view.records()))) {
        return i;
      }
    }
    return -1;
  }

  private static boolean sameRecords(List<byte[]> a, List<byte[]> b) {
    return a.size() == b.size() && startsWith(a, b);
  }

  /**
   * Whether {@code records}, which a read after {@code since} returned alone, are what a version
   * the store held has after it: a version of its incarnation that continues the one it names,
   * with these records after it (exactly in OPEN and PINNED modes; else a prefix of them).
   */
  private boolean followsIn(LogTail since, List<byte[]> records, String location) {
    byte[] at = since.position();
    if (at == null || at.length != 2 * Long.BYTES) {
      return true; // a tail that names no position: nothing to check against
    }
    long length = littleEndian(at, Long.BYTES);
    LogView before = viewAt(since);
    if (before == null) {
      return false;
    }
    int n = before.records().size();
    for (byte[] version : versions(location)) {
      if (version.length < length || littleEndian(version, (int) length - AtomicFileLog.TRAILER_SIZE)
          != littleEndian(at, 0) || !frameEnds(version).contains((int) length)) {
        continue;
      }
      List<byte[]> all = decoded.computeIfAbsent(version, AtomicFileLog::decode).records();
      if (all.size() < n) {
        continue;
      }
      List<byte[]> after = all.subList(n, all.size());
      if (whole ? sameRecords(after, records) : startsWith(after, records)) {
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
      return checked(view, how, -2);
    }

    /** The newest version of the object now, as {@link #versions} counts them; -1 if none. */
    private int newest() {
      return versions(location).size() - 1;
    }

    /**
     * {@code view}, checked: a state the log held; and, for a fresh read (a read of the object as
     * it is, not one through a write, which may answer from it: {@code began} the newest version
     * when it began) in OPEN and PINNED modes, a whole version no older than that.
     */
    private LogView checked(LogView view, String how, int began) {
      Hooks was = hooks;
      hooks = null;
      try {
        if (began > -2 && whole) {
          int version = versionOf(view, location, true);
          if (version < 0) {
            readViolations.add(how + " returned no whole version the store held: "
                + view.records().size() + " records after a checkpoint of "
                + view.checkpoint().orElseThrow().length + " bytes");
          } else if (version < began) {
            readViolations.add(how + " returned version " + version
                + ", older than version " + began + ", the object when the read began");
          }
        } else if (!held(view, location)) {
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
      int began = newest();
      return checked(log.read(), "a read", began);
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
      int began = newest();
      LogView view = log.readAfter(since);
      if (view.checkpoint().isPresent()) {
        return checked(view, "a read after a tail", began);
      }
      Hooks was = hooks;
      hooks = null;
      try {
        if (!followsIn(since, view.records(), location)) {
          readViolations.add("a read after a tail returned " + view.records().size()
              + " records no version the store held has after it");
        }
      } finally {
        hooks = was;
      }
      return view;
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
      if (!replaysAPrefix(sm, before, after)) {
        violations.add("CAS #" + i + " did not checkpoint the replay of the version it replaced");
      }
    }
    return violations;
  }

  /**
   * Whether {@code after}'s checkpoint is the replay of a prefix of the version it replaced, and its
   * records begin with the rest of that version's: the whole version, for every replace but a
   * compaction sent again with the records that beat it (TAIL_READS.md "Compaction after the
   * commit"; CatalogCommit.tla's CheckpointIsReplay).
   */
  private static <S, B extends StateMachine.Builder<S>, I extends Intention<B>> boolean
      replaysAPrefix(StateMachine<S, B, I> sm, LogView before, LogView after) {
    byte[] written = after.checkpoint().orElseThrow();
    List<byte[]> records = before.records();
    for (int k = records.size(); k >= 0; k--) {
      if (!startsWith(after.records(), records.subList(k, records.size()))) {
        continue;
      }
      Replay.Result<B> replayed =
          Replay.replay(sm, before.checkpoint().orElse(null), records.subList(0, k), null);
      CommittedSet committed = replayed.committed().build();
      byte[] expected = LogEnvelope.encodeCheckpoint(replayed.logId(), committed.bytes(),
          sm.encodeCheckpoint(replayed.builder().build()));
      if (Arrays.equals(expected, written)) {
        return true;
      }
    }
    return false;
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
