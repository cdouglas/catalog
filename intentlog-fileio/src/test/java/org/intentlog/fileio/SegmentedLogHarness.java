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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.function.LongSupplier;
import net.jqwik.api.statistics.Statistics;
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
import org.intentlog.azure.AppendBlobSegments;
import org.intentlog.azure.AppendBlobStore;
import org.intentlog.azure.InMemoryAppendBlobStore;
import org.intentlog.memory.InMemorySegmentStore;
import org.intentlog.segments.SegmentStore;
import org.intentlog.segments.SegmentFrames;
import org.intentlog.segments.SegmentPosition;
import org.intentlog.testkit.LogHarness;
import org.intentlog.wire.LogEnvelope;
import org.intentlog.wire.UuidV7;

/**
 * A {@link SegmentedLog} over an {@link InMemoryAtomicFileIO} (its catalog object and pages) and
 * an {@link InMemorySegmentStore} (its segments), dressed for fuzzing. Every request of either
 * store may meet the fuzzer's faults, as the commit or step of maintenance running draws them: a
 * write may fail, be refused, be throttled before or after it lands, lose its reply, or (a segment
 * append) land later; a read may fail, or (a segment's) be served short of its last block. One to
 * three instances of the log, each with its own catalog object and views, take requests in
 * stretches drawn from the seed, as processes would, one sleeping through several checkpoints,
 * retirements and sweeps; an append's read-through goes to the instance that sent it, as a
 * process reads through its own appends. The oracle reads its own instance, and the instances
 * retire segments by the schedule's clock.
 *
 * <p>The oracles see the log the stores hold: the segments every catalog object ever installed,
 * in ordinal order, each up to its end (its limit-th record or its first seal frame), through the
 * first that hasn't ended, built from every append that landed. Every checkpoint any catalog
 * object held must be the replay of that log through its position (I11); every view a read
 * returned a state it held; and at the drain every read must read on from its tail to the log's
 * end.
 *
 * <p>Another log, of another catalog object, keeps its segments at the same prefix of the same
 * store, as two catalogs configured with one segment location do: it takes two records before the
 * run, and at the drain it must read them still. No request of the log under test may touch its
 * segments: Bug 100 (sweep deleted another log's segments).
 */
final class SegmentedLogHarness implements LogHarness {

  /** The segments' store as the harness sees it: its faults, and every append that landed. */
  interface Segments {
    enum Request { CREATE, APPEND, READ, DELETE, LIST, OTHER }

    enum Fault { NONE, FAIL_BEFORE, REFUSED, THROTTLED, LANDED_THROTTLED, LOST_REPLY, DELAYED,
      SHORT_READ }

    record Landed(String name, long offset, byte[] block) {}

    interface Hook {
      Fault before(Request request, String name);
    }

    SegmentStore store();

    void intercept(Hook hook);

    List<Landed> appends();

    int deliverDelayed();

    static Segments of(InMemorySegmentStore s) {
      return new Segments() {
        @Override
        public SegmentStore store() {
          return s;
        }

        @Override
        public void intercept(Hook hook) {
          s.intercept((op, name) -> InMemorySegmentStore.Fault.valueOf(
              hook.before(Request.valueOf(op.name()), name).name()));
        }

        @Override
        public List<Landed> appends() {
          List<Landed> out = new ArrayList<>();
          s.appends().forEach(a -> out.add(new Landed(a.name(), a.offset(), a.block())));
          return out;
        }

        @Override
        public int deliverDelayed() {
          return s.deliverDelayed();
        }
      };
    }

    /**
     * Append Blobs, through {@link AppendBlobSegments}; with {@code counted} false, through a
     * store whose appends don't say their block count (as one that wraps a store's requests may
     * not), so a read says where each landed.
     */
    static Segments of(InMemoryAppendBlobStore s, boolean counted) {
      AppendBlobStore through = counted ? s : uncounted(s);
      AppendBlobSegments segments = new AppendBlobSegments(through);
      return new Segments() {
        @Override
        public SegmentStore store() {
          return segments;
        }

        @Override
        public void intercept(Hook hook) {
          s.intercept((op, name) -> {
            Request r = switch (op) {
              case CREATE_APPEND -> Request.CREATE;
              case APPEND -> Request.APPEND;
              case READ, PROPERTIES -> Request.READ;
              case DELETE -> Request.DELETE;
              case LIST -> Request.LIST;
              default -> Request.OTHER;
            };
            return r == Request.OTHER ? InMemoryAppendBlobStore.Fault.NONE
                : InMemoryAppendBlobStore.Fault.valueOf(hook.before(r, name).name());
          });
        }

        @Override
        public List<Landed> appends() {
          List<Landed> out = new ArrayList<>();
          s.appends().forEach(a -> out.add(new Landed(a.name(), a.offset(), a.block())));
          return out;
        }

        @Override
        public int deliverDelayed() {
          return s.deliverDelayed();
        }
      };
    }

    private static AppendBlobStore uncounted(AppendBlobStore s) {
      return new AppendBlobStore() {
        @Override
        public boolean createAppendBlob(String name, Map<String, String> metadata) {
          return s.createAppendBlob(name, metadata);
        }

        @Override
        public long appendBlock(String name, byte[] block, long maxSize, long appendPosition) {
          return s.appendBlock(name, block, maxSize, appendPosition);
        }

        @Override
        public Blob read(String name, long from) {
          return s.read(name, from);
        }

        @Override
        public Props properties(String name) {
          return s.properties(name);
        }

        @Override
        public boolean putIfAbsent(String name, byte[] bytes) {
          return s.putIfAbsent(name, bytes);
        }

        @Override
        public void put(String name, byte[] bytes) {
          s.put(name, bytes);
        }

        @Override
        public String putIfMatch(String name, byte[] bytes, String etag) {
          return s.putIfMatch(name, bytes, etag);
        }

        @Override
        public void delete(String name) {
          s.delete(name);
        }

        @Override
        public List<Entry> list(String prefix) {
          return s.list(prefix);
        }

        @Override
        public int maxBlocks() {
          return s.maxBlocks();
        }
      };
    }
  }

  private final InMemoryAtomicFileIO io;
  private final Segments store;
  private final String location;
  private final String prefix;
  /** The prefix of a catalog object that records none (D13); null if the logs have no default. */
  private final String defaultPrefix;
  private final String pages;
  private final StateMachine<?, ?, ?> machine;
  private final RoundRobin log;
  // The oracle's instance, apart from the writers'
  private final RoundRobin oracle;
  private final Clock clock;
  // The instances' retirement TTL; and whether the store lists (one whose listings the service
  // refuses, as for a SAS without list permission, leaves the sweep only the retired segments'
  // deletes by name)
  private final long ttl;
  private final boolean listable;
  // What the catalog object's versions say of segments, read on as it is written: when each
  // retired segment was retired, every segment ever installed, and those the newest installs and
  // retires
  private int versionsSeen = 0;
  private final Map<String, Long> retiredAt = new HashMap<>();
  private final Set<String> everInstalled = new HashSet<>();
  private Set<String> installedNow = Set.of();
  private Set<String> retiredNow = Set.of();
  private final List<String> deleteViolations = new ArrayList<>();
  private final List<String> readViolations = new ArrayList<>();
  private Hooks hooks = null;

  /** The other log at the prefix: its catalog object's location, options and records. */
  private final String neighbour;
  private final SegmentedLog.Options neighbourOptions;
  private final List<byte[]> neighbourRecords = new ArrayList<>();

  /**
   * The harness of {@code logs}, instances of the log whose catalog object is at {@code location}
   * in {@code io} and whose segments are in {@code store}, under the prefix the catalog object
   * records or else {@code defaultPrefix}; the other log keeps its segments under {@code prefix}.
   */
  SegmentedLogHarness(InMemoryAtomicFileIO io, Segments store, String location,
      String prefix, String defaultPrefix, List<SegmentedLog> logs, SegmentedLog oracle,
      Clock clock, long ttl, boolean listable, long seed, StateMachine<?, ?, ?> machine) {
    this.io = io;
    this.store = store;
    this.location = location;
    this.prefix = prefix;
    this.defaultPrefix = defaultPrefix;
    this.pages = location + ".committed/";
    this.machine = machine;
    this.log = new RoundRobin(logs, seed);
    this.oracle = new RoundRobin(List.of(oracle), seed);
    this.clock = clock;
    this.ttl = ttl;
    this.listable = listable;
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
    this.neighbour = location + "-neighbour";
    // K the store's block limit: none of the log under test's options, which a mutant may break
    this.neighbourOptions = SegmentedLog.Options.defaults().withLimit(0).withRetireTtlMs(0)
        .withClock(() -> 0L);
    SegmentedLog other = neighbour();
    LogTail absent = other.probe();
    other.putCheckpoint(absent, LogEnvelope.encodeCheckpoint(UuidV7.newUuidV7(), new byte[0],
        new byte[0], absent.position()));
    for (int i = 0; i < 2; i++) {
      byte[] record = LogEnvelope.encodeRecord(UuidV7.newUuidV7(), new byte[] {(byte) i});
      other.readThrough(other.append(record, null), Duration.ZERO);
      neighbourRecords.add(record);
    }
  }

  /** The other log at the prefix, as a fresh process opens it. */
  private SegmentedLog neighbour() {
    return new SegmentedLog(AtomicFileLog.segmented(io, neighbour, () -> 7L, false),
        store.store(), prefix, neighbourOptions);
  }

  /** The other log at the prefix must hold its records still, whatever this log swept. */
  private List<String> neighbourViolations() {
    Hooks was = hooks;
    hooks = null;
    try {
      List<byte[]> read = neighbour().read().records();
      boolean same = read.size() == neighbourRecords.size();
      for (int i = 0; same && i < read.size(); i++) {
        same = Arrays.equals(read.get(i), neighbourRecords.get(i));
      }
      return same ? List.of() : List.of("another log at the prefix reads " + read.size()
          + " records, not its " + neighbourRecords.size());
    } catch (RuntimeException e) {
      return List.of("another log at the prefix cannot be read (its segments were swept?): " + e);
    } finally {
      hooks = was;
    }
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

  private Segments.Fault segmentFault(Segments.Request op, String name) {
    Hooks h = hooks;
    if (h == null) {
      return Segments.Fault.NONE;
    }
    if (op == Segments.Request.DELETE) {
      checkDelete(name);
    }
    if (op == Segments.Request.LIST && !listable) {
      Statistics.label("listings").collect("refused");
      return Segments.Fault.FAIL_BEFORE;
    }
    switch (op) {
      case READ:
      case LIST: {
        ReadFault r = h.beforeRead();
        if (r == ReadFault.FAIL) {
          return Segments.Fault.FAIL_BEFORE;
        }
        if (r == ReadFault.STALE && op == Segments.Request.READ) {
          return Segments.Fault.SHORT_READ;
        }
        return Segments.Fault.NONE;
      }
      default:
        WriteFault fault = h.beforeWrite();
        if (op == Segments.Request.DELETE) {
          Statistics.label("segment deletes").collect(fault == WriteFault.NONE ? "ran" : "failed");
        }
        return switch (fault) {
          case NONE -> Segments.Fault.NONE;
          case FAIL_BEFORE -> Segments.Fault.FAIL_BEFORE;
          case NOT_SENT -> Segments.Fault.REFUSED;
          case LOST_REPLY, LANDED_AS_CONFLICT -> Segments.Fault.LOST_REPLY;
          case THROTTLED -> Segments.Fault.THROTTLED;
          case LANDED_THROTTLED -> Segments.Fault.LANDED_THROTTLED;
          case DELAYED -> op == Segments.Request.APPEND
              ? Segments.Fault.DELAYED : Segments.Fault.FAIL_BEFORE;
        };
    }
  }

  @Override
  public SharedLog log() {
    return log;
  }

  @Override
  public SharedLog oracleLog() {
    return oracle;
  }

  @Override
  public boolean appendsMayVoid() {
    return true;
  }

  @Override
  public void clock(LongSupplier now) {
    clock.source = now;
  }

  /**
   * The clock the instances retire segments by: the schedule's, once the fuzzer gives it ({@link
   * #clock(LongSupplier)}); 0 before.
   */
  static final class Clock implements LongSupplier {
    private volatile LongSupplier source = () -> 0L;

    @Override
    public long getAsLong() {
      return source.getAsLong();
    }
  }

  @Override
  public void hook(Hooks newHooks) {
    this.hooks = newHooks;
  }

  /** One log of several instances, each request to the next, each read checked. */
  private final class RoundRobin implements SharedLog {
    private final List<SegmentedLog> logs;
    // The instance taking requests, and how many more it takes: stretches drawn from the seed,
    // so an instance may sleep through several checkpoints, retirements and sweeps
    private final SplittableRandom picks;
    private SegmentedLog current;
    private int stretch = 0;

    /** Each read served: a copy of its instance as the read left it, and the tail it returned. */
    private final List<Map.Entry<SegmentedLog, LogTail>> reads = new ArrayList<>();

    /** The instance each receipt came from: a process reads through its own appends. */
    private final Map<Receipt, SegmentedLog> appended = new java.util.IdentityHashMap<>();

    RoundRobin(List<SegmentedLog> logs, long seed) {
      this.logs = List.copyOf(logs);
      this.picks = new SplittableRandom(seed);
    }

    private SegmentedLog pick() {
      if (stretch-- <= 0) {
        current = logs.get(picks.nextInt(logs.size()));
        stretch = picks.nextInt(32);
      }
      return current;
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
      SegmentedLog by = appended.getOrDefault(receipt, logs.get(0));
      return seen(by, by.readThrough(receipt, wait), "a read through a write");
    }

    @Override
    public LogTail probe() {
      return pick().probe();
    }

    @Override
    public Receipt append(byte[] record, LogTail expected) {
      SegmentedLog by = pick();
      Receipt receipt = by.append(record, expected);
      appended.put(receipt, by);
      return receipt;
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
        out.add(SegmentedLog.parse(AtomicFileLog.decode(write.after()), defaultPrefix));
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
    List<Segments.Landed> appends = store.appends();
    for (long o = 0; installed.containsKey(o); o++) {
      SegmentedLog.Seg seg = installed.get(o);
      String name = SegmentedLog.path(seg.prefix(), seg.logId(), seg.name());
      long count = 0;
      boolean ended = false;
      for (Segments.Landed a : appends) {
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
    for (Segments.Landed a : store.appends()) {
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

  /**
   * Appends {@code record} unfaulted, and reads through it as its writer would, so that it is in
   * the log: sealing an earlier segment that hasn't ended, and sending it again if it landed void.
   */
  @Override
  public void appendCopy(byte[] record) {
    Hooks was = hooks;
    hooks = null;
    try {
      for (int attempt = 0; ; attempt++) {
        Receipt receipt = log.append(record, null);
        try {
          log.readThrough(receipt, Duration.ZERO);
          return;
        } catch (org.intentlog.LogVoidException e) {
          if (attempt >= 16) {
            throw e;
          }
        }
      }
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
    String prefix = null;
    for (SegmentedLog.Cat c : catalogs()) {
      byte[] was = out.putIfAbsent(c.ck(), c.checkpoint());
      if (was != null && !Arrays.equals(was, c.checkpoint())) {
        violations.add("two checkpoints at " + c.ck() + " differ (I11)");
      }
      if (prefix == null) {
        prefix = c.prefix();
      } else if (!prefix.equals(c.prefix())) {
        violations.add("the segments' prefix moved from " + prefix + " to " + c.prefix()
            + " at " + c.ck() + " (D13)");
      }
    }
    return out;
  }

  /** Reads on the catalog object's versions written since the last call. */
  private void catchUp() {
    List<InMemoryAtomicFileIO.Write> history = io.history();
    for (; versionsSeen < history.size(); versionsSeen++) {
      InMemoryAtomicFileIO.Write write = history.get(versionsSeen);
      if (!write.location().equals(location)) {
        continue;
      }
      SegmentedLog.Cat c = SegmentedLog.parse(AtomicFileLog.decode(write.after()), defaultPrefix);
      Set<String> installed = new HashSet<>();
      c.segs().forEach(seg -> installed.add(seg.name()));
      Set<String> retired = new HashSet<>();
      c.retired().forEach(r -> {
        retired.add(r.name());
        retiredAt.putIfAbsent(r.name(), r.retiredAtMs());
      });
      everInstalled.addAll(installed);
      installedNow = installed;
      retiredNow = retired;
    }
  }

  /**
   * SG6: a segment is deleted only once retired, and its TTL passed (the half of SG6 the model
   * does not hold): never one the newest catalog object installs, nor a retired one sooner.
   */
  private void checkDelete(String full) {
    catchUp();
    String name = full.substring(full.lastIndexOf('/') + 1);
    Long at = retiredAt.get(name);
    long now = clock.getAsLong();
    if (installedNow.contains(name)) {
      deleteViolations.add("a delete of segment " + name + ", which the catalog object installs");
    } else if (at != null && at + ttl > now) {
      deleteViolations.add("a delete of segment " + name + ", retired at " + at + ", before its"
          + " TTL of " + ttl + " ms passed (now " + now + ")");
    }
  }

  /**
   * A segment once installed that the store still holds must be installed or retired by the
   * newest catalog object: its entries are the only references to it, which a store that cannot
   * list leaves no sweep to find.
   */
  private List<String> leaks() {
    Hooks was = hooks;
    hooks = null;
    try {
      catchUp();
      List<SegmentedLog.Cat> versions = catalogs();
      if (versions.isEmpty()) {
        return List.of();
      }
      SegmentedLog.Cat newest = versions.get(versions.size() - 1);
      List<String> leaks = new ArrayList<>();
      for (String full : store.store().list(SegmentedLog.path(newest.prefix(), newest.logId(), ""))) {
        String name = full.substring(full.lastIndexOf('/') + 1);
        if (everInstalled.contains(name) && !installedNow.contains(name)
            && !retiredNow.contains(name)) {
          leaks.add("segment " + name + ", once installed, is neither installed nor retired by"
              + " the catalog object: no reference to it is left");
        }
      }
      return leaks;
    } finally {
      hooks = was;
    }
  }

  @Override
  public List<String> checkpointViolations() {
    List<String> violations = new ArrayList<>(readViolations);
    violations.addAll(deleteViolations);
    violations.addAll(leaks());
    violations.addAll(violations(machine));
    violations.addAll(staleReaders());
    violations.addAll(neighbourViolations());
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
