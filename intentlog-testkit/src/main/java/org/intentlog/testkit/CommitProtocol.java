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
package org.intentlog.testkit;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.RandomDistribution;
import net.jqwik.api.Tuple;
import net.jqwik.api.statistics.Statistics;
import org.intentlog.CommitOutcomeUnknownException;
import org.intentlog.CommitRejectedException;
import org.intentlog.CommitTrace;
import org.intentlog.CommittedSet;
import org.intentlog.CompactionPolicy;
import org.intentlog.DedupHorizonExceededException;
import org.intentlog.LogRuntime;
import org.intentlog.LogView;
import org.intentlog.PageStore;
import org.intentlog.PagedOutException;
import org.intentlog.Replica;
import org.intentlog.RuntimeConfig;
import org.intentlog.testkit.EntityVersionsMachine.Builder;
import org.intentlog.testkit.EntityVersionsMachine.State;
import org.intentlog.testkit.EntityVersionsMachine.Txn;
import org.intentlog.testkit.LogHarness.PageFault;
import org.intentlog.testkit.LogHarness.PageRequest;
import org.intentlog.testkit.LogHarness.ReadFault;
import org.intentlog.testkit.LogHarness.WriteFault;
import org.intentlog.wire.CommittedSetCodec;
import org.intentlog.wire.LogEnvelope;
import org.intentlog.wire.PageOut;
import org.intentlog.wire.PageRef;
import org.intentlog.wire.UuidV7;


/**
 * A fuzzer of the commit protocol on any log ({@link LogHarness}), over {@link
 * EntityVersionsMachine}: the TLA+ models' schedules, run on the Java (spec/tla/SharedLogCommit.tla
 * and CatalogCommit.tla).
 *
 * <p>A schedule interleaves, on one thread, writers that read the log and stage a transaction
 * ({@link Step#BEGIN}) and commit it ({@link Step#COMMIT}); a commit's write, read and page
 * requests may meet faults, and another writer's commit may run inside it, just before one of its
 * requests. Between commits, a compactor with its own clock and retention compacts a log that
 * keeps checkpoints apart ({@link Step#COMPACT}); a copy of any intention record or PageOut that
 * ever landed, trimmed or replaced ones included, is appended again ({@link Step#DUPLICATE}); and
 * appends delayed in flight land ({@link Step#DELIVER}).
 *
 * <p>Time is the schedule's: each step advances a clock by an amount drawn, and a commit mints its
 * id from a stamp drawn against the marks of the log as it starts ({@link Region}; the regions of
 * docs/COMMITTED_PAGING.md "Testing"): at the clock, inside the inline blocks, inside a block a
 * compaction in flight is sealing, at or below the floor or the horizon, or far ahead. A log may be
 * prefilled with records stamped over the recent past, so that its committed set spans blocks. In
 * runs that page the committed set ({@link Settings#paging}: a threshold of 0, and an admission
 * window A of 0 or 100 ms), compactors and writers page every block but the newest older than A; a
 * compaction is also split around
 * its PageOuts, its pages put at one step ({@link Step#SPLIT}) and its PageOuts landed at a later
 * one, or never: it crashed after the puts ({@link Step#LAND}); orphaned pages are swept at any
 * step, from a writer's replica, however stale, or a fresh one ({@link Step#SWEEP}); and the id of
 * any record that landed is looked up through such a replica, reading its page if it is paged
 * ({@link Step#LOOKUP}).
 *
 * <p>Each transaction carries its commit's tag, and the state keeps the tags it applied in order,
 * so the oracles read what applied from the state alone:
 *
 * <ul>
 *   <li>As each commit ends: a commit reported committed applied, once; one reported rejected (or
 *       any other error) did not; an error follows an injected fault; an unknown outcome follows
 *       a fault or GC; a commit nothing raced, on a log that did not fail, commits (the solo rule,
 *       which a writer minting at or below the floor breaks); and a commit is rejected beyond the
 *       floor only if its stamp is A older than some clock that compacted.
 *   <li>A copy of an intention record never applies: the state, committed set and horizon after a
 *       duplicate equal those before it, whether the original is a record, in a checkpoint, paged
 *       or past the GC horizon, and whether it applied or was rejected.
 *   <li>A PageOut (a split's, or a copy) seals its block if the block is still the oldest and not
 *       the newest, and changes nothing else; otherwise it changes nothing (oracle 1).
 *   <li>After every step, the committed set is the history's (oracle 2): the horizon is at or below
 *       the floor; the inline ids are exactly the ids of the transactions applied above the floor;
 *       the pages referenced end in order in (H, F]; each exists, holds the block its reference
 *       names, and holds exactly the ids applied above the floor it was sealed at, through its top;
 *       and every id applied in (H, F] is in a page; and the set's bytes are those the whole-set
 *       reference ({@link CommittedSetCodec}) encodes of its ids, marks and pages, so its blocks
 *       are the rule's.
 *   <li>A sweep deletes exactly the pages that the replica it acted on does not reference and whose
 *       nodes end at or below its floor (oracle 3); a compaction deletes the pages its GC record
 *       dropped.
 *   <li>A lookup of an id through a replica finds it only if its transaction applied, and misses it
 *       only if it had not applied there, nor ever will if the id is at or below the replica's
 *       floor; it reports the horizon exceeded only at or below the horizon, and fails only after a
 *       fault of a page read.
 *   <li>Once the run is over and every delayed append and PageOut has landed (the TLA+ "drain"
 *       oracle): every outcome again, against the final state (a failed commit whose copy landed
 *       late has not applied); no commit applied twice; the state is the serial execution of the
 *       transactions applied, each one's reads holding where it applied; every checkpoint is the
 *       replay of the log through it (I11), so readers from any of them agree; and a GC record at
 *       the newest id, then a sweep, leave no page at all (oracle 4).
 * </ul>
 *
 * <p>Judging each commit only as it ends misses copies that land after it, and checkpoints no
 * writer happens to read from (SharedLogCommit.tla's oracle matrix); the drain catches them.
 */
public final class CommitProtocol {

  private CommitProtocol() {}

  static final List<String> ENTITIES = List.of("a", "b", "c");

  /** No GC: the committed set keeps every id. */
  public static final long NO_GC = Long.MAX_VALUE;

  /** Where a run's clock starts: time is the schedule's, so stamps do not depend on when it runs. */
  static final long EPOCH_MS = 1_750_000_000_000L;

  /** How far ahead of the clock a stamp drawn far ahead lies (errata D21). */
  static final long AHEAD_MS = 100_000L;

  /** How far apart, on average, a prefilled log's records are stamped. */
  static final long PREFILL_SPACING_MS = 4L;

  public enum Step {
    BEGIN,
    COMMIT,
    COMPACT,
    DUPLICATE,
    DELIVER,
    SPLIT,
    LAND,
    SWEEP,
    LOOKUP
  }

  /**
   * Where a commit's stamp is drawn, against the marks of the log as the commit starts. The stamp
   * is its writer's clock, from which the runtime mints the commit's id just above the floor of
   * the replica the writer read if the clock is not past it: so a stamp at or below the floor lands
   * below the floor (or the horizon) only if the mark rose since the writer read.
   */
  enum Region {
    /** At the clock: above the newest id, the usual case. */
    NOW,
    /** In the same ms as an inline id. */
    SAME_MS,
    /** Between two inline ids, or the floor and the oldest: inside an inline block. */
    BETWEEN,
    /**
     * Between the floor and the oldest inline id: inside the oldest block. Drawn only for a commit
     * whose read back a compaction preempts.
     */
    OLDEST,
    /** Inside a block a split compaction in flight is sealing, above the floor. */
    SEALING,
    /** At the floor. */
    FLOOR,
    /** Between the horizon and the floor, where pages hold the ids. */
    PAGED,
    /** At the horizon. */
    HORIZON,
    /** Below the horizon: a clock behind. */
    BELOW,
    /** Far ahead of every clock. */
    AHEAD;

    /** NOW 6 times in 16, BETWEEN and SEALING twice, the others once. */
    static Region of(int bits) {
      switch (bits & 15) {
        case 6:
          return SAME_MS;
        case 7:
        case 8:
          return BETWEEN;
        case 9:
        case 10:
          return SEALING;
        case 11:
          return FLOOR;
        case 12:
          return PAGED;
        case 13:
          return HORIZON;
        case 14:
          return BELOW;
        case 15:
          return AHEAD;
        default:
          return NOW;
      }
    }
  }

  /**
   * One generated command. Its operands pick, when it runs: for {@link Step#BEGIN}, what the
   * transaction reads ({@code a}) and writes ({@code b}), as sets of entities; for {@link
   * Step#COMMIT}, the faults of its first two writes ({@code a}, {@code b}) and of one of its reads
   * ({@code c}), the request that another writer's commit or a compactor preempts ({@code d}), and
   * from {@code e} its stamp's region (bits 0-3), its page faults (bits 4-7) and a pick within the
   * region (bits 8-11); for {@link Step#COMPACT}, whether the compactor stops after its GC record
   * ({@code a}) and its page faults ({@code e}, bits 4-7); for {@link Step#DUPLICATE}, the record
   * copied ({@code e}); for {@link Step#SPLIT}, its page faults ({@code e}, bits 4-7); for {@link
   * Step#LAND}, whether the compactor crashes ({@code a}) after how many of its PageOuts ({@code
   * b}); for {@link Step#SWEEP}, the writer whose replica it sweeps from, if it has one; for {@link
   * Step#LOOKUP}, the writer whose replica it looks through, if it has one, the record whose id it
   * looks up ({@code a} and {@code e}, bits 8-11), and its page faults ({@code e}, bits 4-7). Each
   * step first advances the clock by {@code 1 + t} ms. Commands that do not apply are skipped.
   */
  public static final class Cmd {
    final Step step;
    final int writer;
    final int a;
    final int b;
    final int c;
    final int d;
    final int e;
    final int t;

    public Cmd(Step step, int writer, int a, int b, int c, int d, int e, int t) {
      this.step = step;
      this.writer = writer;
      this.a = a;
      this.b = b;
      this.c = c;
      this.d = d;
      this.e = e;
      this.t = t;
    }

    @Override
    public String toString() {
      String time = t > 0 ? "+" + t : "";
      switch (step) {
        case BEGIN:
          return String.format("w%d:BEGIN(%d,%d)%s", writer, a, b, time);
        case COMMIT:
          return String.format("w%d:COMMIT(%d,%d,%d,%d,%d)%s", writer, a, b, c, d, e, time);
        case COMPACT:
          return String.format("COMPACT(%d,%d)%s", a, e, time);
        case DUPLICATE:
          return String.format("DUPLICATE(%d)%s", e, time);
        case SPLIT:
          return String.format("SPLIT(%d)%s", e, time);
        case LAND:
          return String.format("LAND(%d,%d)%s", a, b, time);
        case SWEEP:
          return String.format("w%d:SWEEP%s", writer, time);
        case LOOKUP:
          return String.format("w%d:LOOKUP(%d,%d)%s", writer, a, e, time);
        default:
          return step + time;
      }
    }
  }

  /** Schedules of up to 50 commands. */
  public static Arbitrary<List<Cmd>> schedules() {
    Arbitrary<Step> steps = Arbitraries.frequency(
        Tuple.of(4, Step.BEGIN), Tuple.of(4, Step.COMMIT), Tuple.of(1, Step.COMPACT),
        Tuple.of(2, Step.DUPLICATE), Tuple.of(1, Step.DELIVER), Tuple.of(1, Step.SPLIT),
        Tuple.of(1, Step.LAND), Tuple.of(1, Step.SWEEP), Tuple.of(1, Step.LOOKUP));
    Arbitrary<Integer> operand = Arbitraries.integers().between(0, 15);
    Arbitrary<Integer> wide = Arbitraries.integers().between(0, 4095);
    // Most steps take a ms; some jump past a 50 ms retention, or a 200 ms one
    Arbitrary<Integer> advance = Arbitraries.frequency(
        Tuple.of(10, 0), Tuple.of(4, 2), Tuple.of(1, 40), Tuple.of(1, 150));
    return Combinators.combine(steps, Arbitraries.integers().between(0, 2), operand, operand,
            operand, operand, wide, advance)
        .as(Cmd::new)
        .list()
        .ofMinSize(1)
        .ofMaxSize(50)
        .withSizeDistribution(RandomDistribution.uniform());
  }

  /** What a run draws besides its schedule. */
  public static final class Settings {
    final int writers;
    final boolean faults;
    final long retentionMs;
    final long compactorAheadMs;
    final CompactionPolicy policy;
    final String policyName;
    /** Whether compactions page the committed set, at a threshold of 0. */
    final boolean paging;
    /** The admission window A, in ms, if they do. */
    final long admissionMs;
    /** How many records the log is prefilled with, stamped over the recent past. */
    final int prefill;
    final long seed;
    /**
     * Whether the runtimes read the log's tail ({@link RuntimeConfig#tailReads}), or read it whole
     * and probe after a lost append race: drawn from the seed's high bit, which the prefill's
     * random never sees alone.
     */
    final boolean tailReads;
    /**
     * Whether a log that replaces itself is compacted after a commit ({@link
     * RuntimeConfig#compactAfterCommit}), at points drawn from the seed, or by the commit past the
     * limits: drawn from the seed's next bit.
     */
    final boolean compactAfter;

    /** {@code admissionMs} is the admission window of a run that pages; -1 if it does not. */
    public Settings(int writers, boolean faults, long retentionMs, long compactorAheadMs,
        int maxRecords, long admissionMs, int prefill, long seed) {
      this.writers = writers;
      this.faults = faults;
      this.retentionMs = retentionMs;
      this.compactorAheadMs = compactorAheadMs;
      this.policy = maxRecords < 0
          ? log -> false
          : CompactionPolicy.limits(maxRecords, 1 << 20);
      this.policyName = maxRecords < 0 ? "never" : "every " + maxRecords + " records";
      this.paging = admissionMs >= 0;
      this.admissionMs = Math.max(0L, admissionMs);
      this.prefill = prefill;
      this.seed = seed;
      this.tailReads = seed >= 0;
      this.compactAfter = (seed & (1L << 62)) == 0;
    }

    /** A runtime's configuration in this run, at {@code retentionMs}, on {@code clock}. */
    RuntimeConfig config(long retention, LongSupplier clock) {
      Random draws = new Random(seed ^ 0x2545F4914F6CDD1DL);
      RuntimeConfig config =
          new RuntimeConfig(4, 3, 0L, 0L, retention, policy.withDraws(draws::nextDouble),
              Duration.ZERO, clock)
              .withTailReads(tailReads)
              .withCompactionAfterCommit(compactAfter);
      return paging ? config.withPaging(admissionMs, 0L) : config;
    }

    @Override
    public String toString() {
      return String.format(
          "writers=%d faults=%s retention=%s compactor ahead %d ms, compact %s %s%s, %s%s",
          writers, faults, retentionMs == NO_GC ? "none" : retentionMs + " ms", compactorAheadMs,
          policyName, compactAfter ? "after commits" : "with them",
          paging ? ", paging (A = " + admissionMs + " ms, T = 0)" : "",
          tailReads ? "tail reads" : "whole reads",
          prefill > 0 ? ", prefilled with " + prefill + " records (seed " + seed + ")" : "");
    }
  }

  /**
   * Settings drawn: 2 or 3 writers, GC off or at retention 0, 50 or 200 ms, compaction policies,
   * paging or not (at an admission window of 0, or of 100 ms), a log prefilled or not, and tail
   * reads or whole ones.
   */
  public static Arbitrary<Settings> settings() {
    return Combinators.combine(
            Arbitraries.integers().between(2, 3),
            Arbitraries.of(true, false),
            Arbitraries.of(NO_GC, NO_GC, 0L, 50L, 200L),
            Arbitraries.of(0L, 0L, 1_000L),
            Arbitraries.of(-1, 0, 2, 5),
            Arbitraries.of(-1L, -1L, 0L, 0L, 100L),
            Arbitraries.of(0, 24, 48, 48),
            Arbitraries.longs())
        .as(Settings::new);
  }

  // A commit's first write fails a third of the time, its second a fifth; later writes succeed.
  private static final List<WriteFault> FIRST_WRITE_FAULTS = List.of(
      WriteFault.NONE, WriteFault.NONE, WriteFault.NONE, WriteFault.NONE, WriteFault.NONE,
      WriteFault.NONE, WriteFault.NONE, WriteFault.NONE, WriteFault.NONE, WriteFault.NONE,
      WriteFault.FAIL_BEFORE, WriteFault.NOT_SENT, WriteFault.LOST_REPLY, WriteFault.THROTTLED,
      WriteFault.LANDED_THROTTLED, WriteFault.DELAYED);
  private static final List<WriteFault> SECOND_WRITE_FAULTS = List.of(
      WriteFault.NONE, WriteFault.NONE, WriteFault.NONE, WriteFault.NONE, WriteFault.NONE,
      WriteFault.NONE, WriteFault.NONE, WriteFault.NONE, WriteFault.NONE, WriteFault.NONE,
      WriteFault.NONE, WriteFault.NONE, WriteFault.LOST_REPLY, WriteFault.LANDED_THROTTLED,
      WriteFault.DELAYED, WriteFault.LANDED_AS_CONFLICT);

  /** Runs {@code schedule} on {@code harness}; a failed oracle throws {@link AssertionError}. */
  public static void run(LogHarness harness, Settings settings, List<Cmd> schedule) {
    new Run(harness, settings).execute(schedule);
  }

  enum Outcome {
    COMMITTED,
    REJECTED,
    UNKNOWN,
    ERROR
  }

  /** The page faults of a commit or a compaction: one kind of request fails, once or every time. */
  private static final class PagePlan {
    static final PagePlan NONE = new PagePlan(null, PageFault.NONE, false);

    final PageRequest request;
    final PageFault fault;
    final boolean every;
    boolean spent = false;

    PagePlan(PageRequest request, PageFault fault, boolean every) {
      this.request = request;
      this.fault = fault;
      this.every = every;
    }

    /**
     * None (0-9); a put fails (10), or lands with its reply lost (11); a get fails (12), finds no
     * page (13), or every get finds none (14); every delete and listing fails (15).
     */
    static PagePlan of(int bits) {
      switch (bits & 15) {
        case 10:
          return new PagePlan(PageRequest.PUT, PageFault.FAIL, false);
        case 11:
          return new PagePlan(PageRequest.PUT, PageFault.LOST_REPLY, false);
        case 12:
          return new PagePlan(PageRequest.GET, PageFault.FAIL, false);
        case 13:
          return new PagePlan(PageRequest.GET, PageFault.MISSING, false);
        case 14:
          return new PagePlan(PageRequest.GET, PageFault.MISSING, true);
        case 15:
          return new PagePlan(PageRequest.DELETE, PageFault.FAIL, true);
        default:
          return NONE;
      }
    }

    /**
     * The page faults of a commit whose read back a compaction preempts, which may read its page:
     * none (0-7); its first page read fails (8-11), finds no page (12-13), or every one finds none
     * (14-15).
     */
    static PagePlan readBack(int bits) {
      switch (bits & 15) {
        case 8:
        case 9:
        case 10:
        case 11:
          return new PagePlan(PageRequest.GET, PageFault.FAIL, false);
        case 12:
        case 13:
          return new PagePlan(PageRequest.GET, PageFault.MISSING, false);
        case 14:
        case 15:
          return new PagePlan(PageRequest.GET, PageFault.MISSING, true);
        default:
          return NONE;
      }
    }

    PageFault next(PageRequest seen) {
      boolean matches = seen == request
          || (request == PageRequest.DELETE && seen == PageRequest.LIST);
      if (spent || !matches) {
        return PageFault.NONE;
      }
      spent = !every;
      return fault;
    }
  }

  private static final class Writer {
    final int id;
    int commits = 0;
    Replica<State> snapshot; // null when idle
    long snapshotWrites;
    Txn txn;

    Writer(int id) {
      this.id = id;
    }
  }

  /** A commit running: its faults, the commit that may preempt it, and what happened. */
  private static final class Context {
    final List<WriteFault> plan;
    final int failingRead;
    final ReadFault readFault;
    final boolean everyRead;
    final PagePlan pages;
    // Preempt at request preemptAt, or, if afterWrite, at the first request after the first write
    final int preemptAt;
    final boolean afterWrite;
    Writer pending;
    // Instead of a writer, a compactor preempts: a whole compaction, or its GC record alone.
    Boolean compaction;
    int writes = 0;
    int reads = 0;
    int requests = 0;
    boolean faulted = false;
    boolean preempted = false;
    boolean active = false;

    Context(List<WriteFault> plan, int failingRead, ReadFault readFault, boolean everyRead,
        PagePlan pages, Writer pending, Boolean compaction, int preemptAt, boolean afterWrite) {
      this.plan = plan;
      this.failingRead = failingRead;
      this.readFault = readFault;
      this.everyRead = everyRead;
      this.pages = pages;
      this.pending = pending;
      this.compaction = compaction;
      this.preemptAt = preemptAt;
      this.afterWrite = afterWrite;
    }

    /** A commit without faults or preemption. */
    static Context plain() {
      return new Context(List.of(), -1, ReadFault.NONE, false, PagePlan.NONE, null, null, -1,
          false);
    }
  }

  /** A compaction split around its PageOuts: its pages put, its PageOuts not yet landed. */
  private static final class Split {
    final int id;
    final List<PageOut> pageOuts;

    Split(int id, List<PageOut> pageOuts) {
      this.id = id;
      this.pageOuts = pageOuts;
    }
  }

  private static final class Run implements LogHarness.Hooks {
    private final LogHarness harness;
    private final Settings settings;
    private final LogRuntime<State, Builder, Txn> runtime;
    private final LogRuntime<State, Builder, Txn> compactor;
    // The runtime of a commit that preempts another, as another process: its compaction after the
    // commit is drawn apart from the writers' (whose drawn point the preempted one has passed), so
    // the preempted commit's compaction may lose to its append alone and be sent again
    private final LogRuntime<State, Builder, Txn> racer;
    private final Writer[] writers;
    private final Deque<Context> commits = new ArrayDeque<>();
    private final Map<String, Txn> txns = new HashMap<>();
    private final Map<String, Outcome> outcomes = new LinkedHashMap<>();
    // The id of each transaction's record, by tag, as landed records show them
    private final Map<String, UUID> ids = new HashMap<>();
    private final List<Split> splits = new ArrayList<>();
    private final StringBuilder trace = new StringBuilder();
    private long now = EPOCH_MS;
    // The clock of the writer committing, its stamp: the writers' runtime mints from it
    private long writerClock = EPOCH_MS;
    // The page faults of the compaction, split or lookup running outside a commit
    private PagePlan pagePlan = PagePlan.NONE;
    // Whether a page read outside a commit met a fault
    private boolean pageReadFaulted = false;
    private int splitCount = 0;
    // The latest clock any compaction, split or commit ran on: no block above it less A is sealed
    private long sealingClock = Long.MIN_VALUE;

    Run(LogHarness harness, Settings settings) {
      this.harness = harness;
      this.settings = settings;
      this.runtime = new LogRuntime<>(harness.log(), EntityVersionsMachine.INSTANCE,
          settings.config(settings.retentionMs, () -> writerClock));
      this.compactor = new LogRuntime<>(harness.log(), EntityVersionsMachine.INSTANCE,
          settings.config(settings.retentionMs, () -> now + settings.compactorAheadMs));
      this.racer = new LogRuntime<>(harness.log(), EntityVersionsMachine.INSTANCE,
          settings.config(settings.retentionMs, () -> writerClock));
      this.writers = new Writer[settings.writers];
      for (int i = 0; i < writers.length; i++) {
        writers[i] = new Writer(i);
      }
      trace.append(settings).append('\n');
      Txn init = new Txn("init", Map.of(), Set.of());
      txns.put("init", init);
      long span = PREFILL_SPACING_MS * settings.prefill;
      writerClock = EPOCH_MS - span - 1;
      runtime.bootstrap(init);
      prefill(span);
      harness.hook(this);
      Statistics.label("settings").collect(
          (settings.retentionMs == NO_GC ? "no gc" : "gc") + (settings.paging ? ", paging" : ""));
    }

    /**
     * Appends the prefill: blind writes, each stamped in the {@code span} ms before the clock, in
     * a random order.
     */
    private void prefill(long span) {
      Random random = new Random(settings.seed);
      for (int i = 0; i < settings.prefill; i++) {
        String tag = "p" + i;
        Txn txn = new Txn(tag, Map.of(), Set.of(ENTITIES.get(i % ENTITIES.size())));
        txns.put(tag, txn);
        long stamp = EPOCH_MS - 1 - random.nextInt((int) span);
        harness.appendCopy(LogEnvelope.encodeRecord(UuidV7.newUuidV7(stamp, random),
            EntityVersionsMachine.INSTANCE.encodeIntention(txn)));
      }
      if (settings.prefill > 0) {
        trace.append("prefilled ").append(settings.prefill).append(" records over ")
            .append(span).append(" ms\n");
      }
    }

    // ============================================================
    // Hooks: faults and preemption of the commit running
    // ============================================================

    @Override
    public WriteFault beforeWrite() {
      Context c = commits.peek();
      if (c == null || !c.active) {
        return WriteFault.NONE;
      }
      request(c);
      WriteFault fault = c.writes < c.plan.size() ? c.plan.get(c.writes) : WriteFault.NONE;
      c.writes++;
      if (fault != WriteFault.NONE) {
        c.faulted = true;
        trace.append("    write #").append(c.writes - 1).append(": ").append(fault).append('\n');
      }
      return fault;
    }

    @Override
    public ReadFault beforeRead() {
      Context c = commits.peek();
      if (c == null || !c.active) {
        return ReadFault.NONE;
      }
      request(c);
      int read = c.reads++;
      if (c.readFault == ReadFault.NONE || (read != c.failingRead && !c.everyRead)) {
        return ReadFault.NONE;
      }
      c.faulted = true;
      trace.append("    read #").append(read).append(": ").append(c.readFault).append('\n');
      return c.readFault;
    }

    /**
     * A page request fails as the commit running plans, or the compaction or split outside one. A
     * failed get is a fault of the commit, which may change its outcome; a put, delete or listing
     * that fails must not.
     */
    @Override
    public PageFault beforePage(PageRequest request, String name) {
      Context c = commits.peek();
      boolean inCommit = c != null && c.active;
      PageFault fault = (inCommit ? c.pages : pagePlan).next(request);
      if (request == PageRequest.GET) {
        Statistics.label("page reads").collect(inCommit ? "a commit's" : "another's");
      }
      if (fault != PageFault.NONE) {
        if (request == PageRequest.GET && inCommit) {
          c.faulted = true;
        } else if (request == PageRequest.GET) {
          pageReadFaulted = true;
        }
        trace.append("    page ").append(request).append(name != null ? " of " + name : "")
            .append(": ").append(fault).append('\n');
        Statistics.label("page faults").collect(request + " " + fault);
      }
      return fault;
    }

    /** Counts a request of the commit {@code c}; runs the preempting commit or compactor first. */
    private void request(Context c) {
      int request = c.requests++;
      boolean due = c.afterWrite ? c.writes > 0 : request == c.preemptAt;
      if ((c.pending == null && c.compaction == null) || !due) {
        return;
      }
      Writer other = c.pending;
      Boolean compaction = c.compaction;
      c.pending = null;
      c.compaction = null;
      c.preempted = true;
      c.active = false;
      trace.append("  preempted at request #").append(request).append(" by\n");
      if (other != null) {
        commit(other, Region.NOW, 0, Context.plain());
        Statistics.label("preempted by").collect("a writer");
      } else if (compact(compaction)) {
        Statistics.label("preempted by").collect(compaction ? "a compaction" : "a GC record");
      }
      c.active = true;
    }

    /**
     * A compactor's turn, on a log that keeps checkpoints apart: a whole compaction, or its GC
     * record alone (a compactor that stops there, or has not gone on yet). Returns whether it did
     * anything (a log that replaces itself compacts as it commits).
     */
    private boolean compact(boolean whole) {
      if (!harness.log().capabilities().separateCheckpoints()) {
        return false;
      }
      if (whole) {
        trace.append("  compact\n");
        sealingClock = Math.max(sealingClock, now + settings.compactorAheadMs);
        compactor.compact();
        Statistics.label("steps").collect("compact");
        return true;
      }
      long clock = now + settings.compactorAheadMs;
      if (settings.retentionMs >= clock) {
        return false;
      }
      trace.append("  GC record at ").append(clock - settings.retentionMs).append('\n');
      harness.log().append(LogEnvelope.encodeAdvanceHorizon(clock - settings.retentionMs), null);
      Statistics.label("steps").collect("GC record");
      return true;
    }

    // ============================================================
    // The schedule
    // ============================================================

    void execute(List<Cmd> schedule) {
      for (Cmd cmd : schedule) {
        now += 1 + cmd.t;
        Writer w = writers[cmd.writer % writers.length];
        switch (cmd.step) {
          case BEGIN:
            if (w.snapshot == null) {
              begin(w, cmd);
            }
            break;
          case COMMIT:
            if (w.snapshot != null) {
              commit(w, cmd);
            }
            break;
          case COMPACT:
            compactStep(cmd);
            break;
          case DUPLICATE:
            duplicate(cmd.e);
            break;
          case DELIVER:
            int delivered = harness.deliverDelayed();
            if (delivered > 0) {
              trace.append("delivered ").append(delivered).append(" delayed\n");
              Statistics.label("steps").collect("deliver");
            }
            break;
          case SPLIT:
            split(cmd);
            break;
          case LAND:
            land(cmd);
            break;
          case SWEEP:
            sweep(w);
            break;
          case LOOKUP:
            lookup(w, cmd);
            break;
          default:
            throw new IllegalStateException("Unknown step " + cmd.step);
        }
        checkSet("after " + cmd);
      }
      for (Writer w : writers) {
        if (w.snapshot != null) {
          commit(w, Region.NOW, 0, Context.plain());
        }
      }
      drain();
    }

    private void begin(Writer w, Cmd cmd) {
      w.snapshot = runtime.read();
      checkHeld(w.snapshot, "a read");
      w.snapshotWrites = harness.landedWrites();
      String tag = "w" + w.id + "." + w.commits++;
      w.txn = Txn.on(w.snapshot.state(), tag, subset(cmd.a), subset(cmd.b));
      txns.put(tag, w.txn);
      trace.append(String.format("w%d begin %s on %s%n", w.id, w.txn, w.snapshot.state()));
    }

    private static Set<String> subset(int bits) {
      Set<String> entities = new TreeSet<>();
      for (int i = 0; i < ENTITIES.size(); i++) {
        if ((bits & (1 << i)) != 0) {
          entities.add(ENTITIES.get(i));
        }
      }
      return entities;
    }

    private void commit(Writer w, Cmd cmd) {
      // d: no preemption (0-7); just before request (d - 8) % 4, the next writer with a
      // transaction open commits it (8-11), or a compactor runs (12-15): a whole compaction, or
      // only its GC record, as a + b is odd or even. In a run that pages, d of 0-3 preempts the
      // first request after the commit's first write, its read back, with a whole compaction, or on
      // a log that compacts as it commits the next writer's commit: so a commit's id may be paged
      // after it applied and before it learns so, and its lookup reads the page. So too in a run
      // whose writers compact after their commits: on one object, that request is the compaction's
      // replace, which the next writer's append makes lose (and be sent again with its record).
      boolean afterWrite = (settings.paging || settings.compactAfter) && cmd.d < 4;
      int preemptAt = afterWrite ? -1 : cmd.d < 8 ? -1 : (cmd.d - 8) % 4;
      boolean compacts = harness.log().capabilities().separateCheckpoints();
      Writer other = null;
      Boolean compaction = null;
      if (afterWrite && compacts) {
        compaction = true;
      } else if (preemptAt >= 0 && cmd.d >= 12) {
        compaction = (cmd.a + cmd.b) % 2 == 1;
      }
      boolean byWriter = (afterWrite && !compacts) || (preemptAt >= 0 && cmd.d < 12);
      for (int k = 1; byWriter && other == null && k < writers.length; k++) {
        Writer candidate = writers[(w.id + k) % writers.length];
        other = candidate.snapshot != null ? candidate : null;
      }
      // A commit whose read back a compaction preempts is stamped in the oldest block, not at the
      // clock: in the newest block it would never be paged
      Region region = afterWrite && settings.paging && Region.of(cmd.e) == Region.NOW
          ? Region.OLDEST : Region.of(cmd.e);
      int pick = cmd.e >> 8;
      if (settings.faults) {
        // c: no read fault (0-4); fail, stale, break P3 or serve a gap on one of the first three
        // reads (5-13); stale every read (14-15).
        ReadFault readFault = cmd.c < 5 ? ReadFault.NONE
            : cmd.c >= 14 ? ReadFault.STALE
            : List.of(ReadFault.FAIL, ReadFault.STALE, ReadFault.PAST_RECORD, ReadFault.GAP)
                .get(cmd.c % 4);
        commit(w, region, pick, new Context(
            List.of(FIRST_WRITE_FAULTS.get(cmd.a), SECOND_WRITE_FAULTS.get(cmd.b)),
            (cmd.c - 5) / 3, readFault, cmd.c >= 14,
            afterWrite ? PagePlan.readBack(cmd.e >> 4) : PagePlan.of(cmd.e >> 4), other,
            compaction, preemptAt, afterWrite));
      } else {
        commit(w, region, pick, new Context(List.of(), -1, ReadFault.NONE, false, PagePlan.NONE,
            other, compaction, preemptAt, afterWrite));
      }
    }

    private void commit(Writer w, Region region, int pick, Context ctx) {
      Txn txn = w.txn;
      boolean raced = harness.landedWrites() != w.snapshotWrites;
      long stamp = stamp(region, pick);
      long clockBefore = writerClock;
      writerClock = stamp;
      sealingClock = Math.max(sealingClock, stamp);
      trace.append(String.format("w%d commit %s, stamp %s %d%n", w.id, txn.tag(), region, stamp));
      boolean top = commits.isEmpty();
      if (top) {
        CommitTrace.open(); // the runtime's events, for the compactions' statistics
      }
      commits.push(ctx);
      Outcome outcome;
      String reason = "";
      boolean beyondFloor = false;
      try {
        ctx.active = true;
        checkHeld((top ? runtime : racer).commit(w.snapshot, txn), "a commit");
        outcome = Outcome.COMMITTED;
      } catch (CommitRejectedException e) {
        outcome = Outcome.REJECTED;
        reason = " (" + e.reason() + ")";
        beyondFloor = e.reason() == CommitRejectedException.Reason.BEYOND_FLOOR;
      } catch (CommitOutcomeUnknownException e) {
        outcome = Outcome.UNKNOWN;
        reason = " (" + e.getCause() + ")";
      } catch (RuntimeException e) {
        if (!ctx.faulted) {
          throw new AssertionError("commit failed without an injected fault\ntrace:\n" + trace, e);
        }
        outcome = Outcome.ERROR;
        reason = " (" + e + ")";
      } finally {
        ctx.active = false;
        commits.pop();
        writerClock = clockBefore;
      }
      w.snapshot = null;
      w.txn = null;
      outcomes.put(txn.tag(), outcome);
      trace.append(String.format("w%d %s -> %s%s%n", w.id, txn.tag(), outcome, reason));
      Statistics.label("outcome").collect(outcome + (ctx.faulted ? " after a fault" : ""));
      Statistics.label("stamps").collect(region + " " + outcome
          + (outcome == Outcome.REJECTED ? reason : ""));

      Replica<State> live = runtime.read();
      List<String> applied = live.state().history();
      long times = applied.stream().filter(txn.tag()::equals).count();
      if (top) {
        // The compactions after this commit, and after the racers' run inside it: each sent,
        // sent again with the records that beat it, and landed
        boolean compaction = false;
        int resend = 0;
        for (CommitTrace.Event e : CommitTrace.drain()) {
          if (e.kind() == CommitTrace.Kind.SEND) {
            compaction = e.a() == 2;
            resend = (int) e.b();
            if (compaction) {
              Statistics.label("compaction after a commit")
                  .collect(resend == 0 ? "sent" : "sent again");
            }
          } else if (e.kind() == CommitTrace.Kind.LANDED && compaction) {
            Statistics.label("compaction after a commit")
                .collect(resend == 0 ? "landed" : "landed when sent again");
          } else if (e.kind() == CommitTrace.Kind.LOST && compaction) {
            Statistics.label("compaction after a commit")
                .collect(resend == 0 ? "lost" : "lost when sent again");
          }
        }
        CommitTrace.close();
      }
      switch (outcome) {
        case COMMITTED:
          check(times == 1, "%s was reported committed and applied %d times", txn.tag(), times);
          break;
        case REJECTED:
        case ERROR:
          check(times == 0, "%s was reported %s%s but applied", txn.tag(), outcome, reason);
          break;
        default:
          check(times <= 1, "%s applied %d times", txn.tag(), times);
          break;
      }
      check(outcome != Outcome.ERROR || ctx.faulted, "%s failed without a fault", txn.tag());
      check(outcome != Outcome.UNKNOWN || ctx.faulted || settings.retentionMs != NO_GC,
          "%s's outcome is unknown without a fault, and without GC%s", txn.tag(), reason);
      // A commit nothing raced, on a log that did not fail, commits.
      check(outcome == Outcome.COMMITTED || raced || ctx.preempted || ctx.faulted,
          "%s, which nothing raced, was %s%s", txn.tag(), outcome, reason);
      // The floor passed its id: a block holding it was sealed, which no compaction does within
      // A of its clock, and its id is at or above its stamp
      check(!beyondFloor || stamp <= sealingClock - settings.admissionMs,
          "%s, stamped %d, was rejected beyond the floor, though no clock that compacted was A ="
              + " %d ms past it (the latest, %d)", txn.tag(), stamp, settings.admissionMs,
          sealingClock);
    }

    /** A stamp in {@code region} of the log as it is now; {@code pick} chooses within it. */
    private long stamp(Region region, int pick) {
      if (region == Region.NOW) {
        return now;
      }
      if (region == Region.AHEAD) {
        return now + AHEAD_MS;
      }
      CommittedSet set = runtime.read().committed();
      long floor = set.floor();
      long horizon = set.horizon();
      List<Long> stamps = set.ids().stream().map(UuidV7::timestampMs).sorted()
          .collect(Collectors.toList());
      switch (region) {
        case SAME_MS:
          return stamps.isEmpty() ? now : stamps.get(pick % stamps.size());
        case BETWEEN:
        case OLDEST: {
          if (stamps.isEmpty()) {
            return now;
          }
          int i = region == Region.OLDEST ? 0 : pick % stamps.size();
          long lo = i == 0 ? floor : stamps.get(i - 1);
          long hi = stamps.get(i);
          return hi - lo > 1 ? lo + 1 + pick % (hi - lo - 1) : hi;
        }
        case SEALING:
          for (Split split : splits) {
            PageOut first = split.pageOuts.get(0);
            long from = Math.max(first.index() << first.level(), floor + 1);
            if (from <= first.top()) {
              return from + pick % (first.top() - from + 1);
            }
          }
          return now;
        case FLOOR:
          return floor > 0 ? floor : now;
        case PAGED:
          return floor - horizon > 1 ? horizon + 1 + pick % (floor - horizon - 1)
              : floor > 0 ? floor : now;
        case HORIZON:
          return horizon > 0 ? horizon : now;
        case BELOW:
          return (horizon > 0 ? horizon : now) - 1 - pick % 50;
        default:
          return now;
      }
    }

    /**
     * A compaction ({@link #compact}), its page requests faulted as {@code e} says; a whole one
     * deletes the pages its GC record dropped, unless deletes fail.
     */
    private void compactStep(Cmd cmd) {
      boolean whole = cmd.a % 3 != 0;
      Replica<State> before = whole && harness.log().pages() != null ? runtime.read() : null;
      pagePlan = settings.faults ? PagePlan.of(cmd.e >> 4) : PagePlan.NONE;
      boolean deletes = pagePlan.request != PageRequest.DELETE;
      try {
        if (!compact(whole)) {
          return;
        }
      } finally {
        pagePlan = PagePlan.NONE;
      }
      if (before != null && deletes) {
        Set<String> kept = names(runtime.read().committed().pages());
        Map<String, byte[]> stored = harness.storedPages();
        for (PageRef ref : before.committed().pages()) {
          check(kept.contains(ref.name()) || !stored.containsKey(ref.name()),
              "page %s, which the compaction's GC record dropped, was not deleted", ref.name());
        }
      }
    }

    /**
     * A compaction's first half, at the log as it reads now: puts the page of each block it would
     * seal (every block but the newest, with tops at or below its clock less A) and keeps their
     * PageOuts in flight, for {@link #land}. A put that fails ends it, as in the runtime.
     */
    private void split(Cmd cmd) {
      PageStore store = harness.log().pages();
      if (!settings.paging || store == null) {
        return;
      }
      Replica<State> at = runtime.read();
      sealingClock = Math.max(sealingClock, now + settings.compactorAheadMs);
      long ceiling = now + settings.compactorAheadMs - settings.admissionMs;
      CommittedSet set = at.committed();
      if (set.sealable(ceiling) == null) {
        return;
      }
      int id = splitCount++;
      trace.append("split #").append(id).append(" at floor ").append(set.floor()).append('\n');
      List<PageOut> pageOuts = new ArrayList<>();
      pagePlan = settings.faults ? PagePlan.of(cmd.e >> 4) : PagePlan.NONE;
      try {
        for (CommittedSet.Seal seal = set.sealable(ceiling); seal != null;
            seal = set.sealable(ceiling)) {
          try {
            store.put(seal.name(), seal.page(at.logId()));
          } catch (RuntimeException e) {
            trace.append("  put of ").append(seal.name()).append(" failed: ").append(e)
                .append('\n');
            break;
          }
          trace.append("  put ").append(seal.name()).append('\n');
          pageOuts.add(seal.record());
          set = set.pagedOut(seal.record());
        }
      } finally {
        pagePlan = PagePlan.NONE;
      }
      if (!pageOuts.isEmpty()) {
        splits.add(new Split(id, pageOuts));
      }
      Statistics.label("steps").collect("split");
    }

    /**
     * Lands the PageOuts of the oldest split in flight, or crashes its compactor after landing
     * some of them (none, possibly), as {@code a} and {@code b} say.
     */
    private void land(Cmd cmd) {
      if (splits.isEmpty()) {
        return;
      }
      Split split = splits.remove(0);
      int count = split.pageOuts.size();
      int landing = cmd.a < 12 ? count : cmd.b % count;
      trace.append("split #").append(split.id).append(landing == count
          ? " lands its PageOuts" : " crashes after " + landing + " of its PageOuts").append('\n');
      for (int i = 0; i < landing; i++) {
        landPageOut(split.pageOuts.get(i), "split");
      }
      Statistics.label("steps").collect(landing == count ? "split landed" : "split crashed");
    }

    /**
     * Appends a PageOut, which seals its block, and changes nothing else, if the block is still
     * the oldest and not the newest, as {@link CommittedSet#sealable} finds it; otherwise changes
     * nothing at all (oracle 1).
     */
    private void landPageOut(PageOut pageOut, String what) {
      Replica<State> before = runtime.read();
      CommittedSet.Seal oldest = before.committed().sealable(Long.MAX_VALUE);
      boolean seals = oldest != null && oldest.record().equals(pageOut);
      trace.append("  PageOut ").append(pageOut.name())
          .append(seals ? ": its block is the oldest" : ": its block changed").append('\n');
      harness.appendCopy(LogEnvelope.encodePageOut(pageOut));
      Replica<State> after = runtime.read();
      check(after.state().equals(before.state()), "a PageOut changed the state:%n  before: %s%n"
          + "  after:  %s", before.state(), after.state());
      CommittedSet was = before.committed();
      CommittedSet is = after.committed();
      if (seals) {
        List<PageRef> pages = new ArrayList<>(was.pages());
        pages.add(oldest.ref());
        check(is.floor() == pageOut.top() && is.horizon() == was.horizon()
                && is.pages().equals(pages),
            "a PageOut of the oldest block left floor %d (not %d), horizon %d (was %d), pages %s",
            is.floor(), pageOut.top(), is.horizon(), was.horizon(), is.pages());
      } else {
        check(Arrays.equals(is.bytes(), was.bytes()),
            "a PageOut whose block changed changed the committed set:%n  before: floor %d, %s%n"
                + "  after:  floor %d, %s", was.floor(), was.pages(), is.floor(), is.pages());
      }
      Statistics.label("PageOuts").collect(what + (seals ? ", sealed" : ", changed nothing"));
    }

    /**
     * Sweeps from the replica of writer {@code w}, however stale, or a fresh one if it has none:
     * the sweep deletes exactly the pages the replica does not reference whose nodes end at or
     * below its floor (oracle 3).
     */
    private void sweep(Writer w) {
      if (harness.log().pages() == null) {
        return;
      }
      boolean stale = w.snapshot != null;
      Replica<State> on = stale ? w.snapshot : runtime.read();
      CommittedSet set = on.committed();
      Set<String> referenced = names(set.pages());
      Map<String, byte[]> before = harness.storedPages();
      Set<String> expected = new TreeSet<>();
      for (String name : before.keySet()) {
        if (referenced.contains(name) || top(name) > set.floor()) {
          expected.add(name);
        }
      }
      runtime.sweep(on);
      Set<String> after = new TreeSet<>(harness.storedPages().keySet());
      check(after.equals(expected), "a sweep from %s replica (floor %d) left %s, not %s",
          stale ? "a writer's" : "a fresh", set.floor(), after, expected);
      int deleted = before.size() - after.size();
      trace.append(stale ? "w" + w.id + " sweeps" : "sweep").append(", floor ")
          .append(set.floor()).append(": deleted ").append(deleted).append('\n');
      Statistics.label("sweeps").collect(
          (stale ? "stale" : "fresh") + (deleted > 0 ? ", deleted" : ", nothing to delete"));
    }

    /**
     * Looks up the id of a record that landed, applied or not, through the replica of writer
     * {@code w}, however stale, or a fresh one if it has none ({@link LogRuntime#committed}): the
     * lookup reads the id's page if the replica has paged it, and a fresh read if that page is gone.
     */
    private void lookup(Writer w, Cmd cmd) {
      List<LogEnvelope.Record> landed = new ArrayList<>();
      for (byte[] bytes : harness.landedRecords()) {
        LogEnvelope.Record record = LogEnvelope.decodeRecord(bytes);
        if (record.isIntention()) {
          landed.add(record);
        }
      }
      LogEnvelope.Record record = landed.get(((cmd.a << 4) | (cmd.e >> 8)) % landed.size());
      UUID id = record.id();
      String tag = EntityVersionsMachine.INSTANCE.decodeIntention(record.intention()).tag();
      boolean stale = w.snapshot != null;
      String replica = stale ? "a writer's" : "a fresh";
      Replica<State> on = stale ? w.snapshot : runtime.read();
      CommittedSet set = on.committed();
      long ts = UuidV7.timestampMs(id);
      String where = ts <= set.horizon() ? "at or below the horizon"
          : ts > set.floor() ? "inline" : "in (H, F]";
      if (where.equals("in (H, F]")) {
        try {
          set.contains(id);
          where = "in (H, F], no page";
        } catch (PagedOutException e) {
          where = "in (H, F], paged";
        }
      }
      boolean applied = on.state().history().contains(tag);
      pagePlan = settings.faults ? PagePlan.of(cmd.e >> 4) : PagePlan.NONE;
      pageReadFaulted = false;
      String result;
      try {
        boolean found = runtime.committed(on, id);
        List<String> history = runtime.read().state().history();
        check(!found || history.contains(tag),
            "a lookup through %s replica found %s (%s), which did not apply", replica, tag, where);
        check(found || !applied, "a lookup through %s replica that applied %s (%s) did not find"
            + " it", replica, tag, where);
        check(found || ts > set.floor() || !history.contains(tag), "a lookup of %s, at or below"
            + " the floor of %s replica, did not find it, and it applied", tag, replica);
        result = found ? "found" : "not found";
      } catch (DedupHorizonExceededException e) {
        long horizon = runtime.read().committed().horizon();
        check(ts <= horizon, "a lookup of %s, above the horizon %d, exceeded it", tag, horizon);
        result = "horizon exceeded";
      } catch (RuntimeException e) {
        if (!pageReadFaulted) {
          throw new AssertionError("a lookup of " + tag + " (" + where + ") through " + replica
              + " replica failed without a fault\ntrace:\n" + trace, e);
        }
        result = "failed after a fault";
      } finally {
        pagePlan = PagePlan.NONE;
      }
      trace.append(stale ? "w" + w.id + " looks up " : "look up ").append(tag).append(" (")
          .append(where).append("): ").append(result).append('\n');
      Statistics.label("lookups").collect(where + ", " + result);
    }

    private static Set<String> names(List<PageRef> pages) {
      return pages.stream().map(PageRef::name).collect(Collectors.toSet());
    }

    /**
     * The top of the node a page's name ({@code <level>-<index>-<digest>}) names; for a name that
     * names no page, above any floor.
     */
    private static long top(String name) {
      String[] parts = name.split("-");
      try {
        return parts.length == 3
            ? ((Long.parseLong(parts[1]) + 1) << Integer.parseInt(parts[0])) - 1
            : Long.MAX_VALUE;
      } catch (NumberFormatException e) {
        return Long.MAX_VALUE;
      }
    }

    /** Appends a copy of a record that landed; it must change nothing. */
    private void duplicate(int pick) {
      List<byte[]> landed = new ArrayList<>();
      for (byte[] bytes : harness.landedRecords()) {
        LogEnvelope.Record record = LogEnvelope.decodeRecord(bytes);
        if (record.isIntention() || record.isPageOut()) {
          landed.add(bytes);
        }
      }
      if (landed.isEmpty()) {
        return;
      }
      byte[] copy = landed.get(pick % landed.size());
      LogEnvelope.Record record = LogEnvelope.decodeRecord(copy);
      Statistics.label("steps").collect("duplicate");
      if (record.isPageOut()) {
        trace.append("duplicate record #").append(pick % landed.size()).append(" (a PageOut)\n");
        landPageOut(record.pageOut(), "copy");
        return;
      }
      Replica<State> before = runtime.read();
      String original = original(copy, before);
      trace.append("duplicate record #").append(pick % landed.size()).append(" (")
          .append(original).append(")\n");
      harness.appendCopy(copy);
      Replica<State> after = runtime.read();
      check(after.state().equals(before.state()),
          "a copy of a record applied:%n  before: %s%n  after:  %s", before.state(), after.state());
      check(Arrays.equals(after.committed().bytes(), before.committed().bytes()),
          "a copy of a record changed the committed set or its horizon");
      Statistics.label("duplicate of").collect(original);
    }

    /** What became of the original of {@code copy}, as a read finds the log. */
    private String original(byte[] copy, Replica<State> live) {
      UUID id = LogEnvelope.decodeRecord(copy).id();
      boolean record =
          harness.log().read().records().stream().anyMatch(r -> Arrays.equals(r, copy));
      String where = record ? "a record" : "compacted out";
      CommittedSet set = live.committed();
      if (set.ids().contains(id)) {
        return "applied, " + where + ", id inline";
      }
      long ts = UuidV7.timestampMs(id);
      if (ts <= set.horizon()) {
        return "applied or not, " + where + ", id past the horizon";
      }
      if (ts <= set.floor()) {
        return "applied or not, " + where + ", id below the floor";
      }
      return "rejected, " + where;
    }

    // ============================================================
    // The committed set against the history
    // ============================================================

    /**
     * The committed set of a fresh read against the transactions it applied (oracle 2, and the
     * set's shape): the horizon at or below the floor; the inline ids exactly the ids applied above
     * the floor; the pages referenced, in order, ending in (H, F], each existing, holding the block
     * its reference names, and holding exactly the ids applied above the floor it was sealed at
     * (the page before it's top, or the horizon) through its top (spec/tla/RSM.tla's PagesExact);
     * and every id applied in (H, F] in a page. Replay never reads a page, so only this sees one
     * missing or wrong.
     */
    private void checkSet(String when) {
      Replica<State> live = runtime.read();
      CommittedSet set = live.committed();
      long horizon = set.horizon();
      long floor = set.floor();
      Set<UUID> applied = appliedIds(live.state().history());
      check(horizon <= floor, "%s: the horizon %d is above the floor %d", when, horizon, floor);
      Set<UUID> above = select(applied, Long.MAX_VALUE, floor + 1, Long.MAX_VALUE);
      Set<UUID> inline = set.ids();
      check(inline.equals(above), "%s: the inline ids are not those applied above the floor %d:"
              + "%n  not applied: %s%n  missing: %s", when, floor, minus(inline, above),
          minus(above, inline));
      Map<String, byte[]> stored = set.pages().isEmpty() ? Map.of() : harness.storedPages();
      Set<UUID> paged = new HashSet<>();
      // A page holds the ids above the floor it was sealed at, which is the top of the page before
      // it, or the horizon: its node may reach below, into pages before it
      long below = horizon;
      for (PageRef ref : set.pages()) {
        String name = ref.name();
        check(ref.top() > below && ref.top() <= floor, "%s: page %s ends outside (%d, F] ="
            + " (%d, %d]: the pages are not in order in (H, F]", when, name, below, below, floor);
        byte[] body = stored.get(name);
        check(body != null, "%s: page %s, which the committed set references, does not exist",
            when, name);
        LogEnvelope.Page page = LogEnvelope.decodePage(body);
        check(page.logId().equals(live.logId()) && page.level() == ref.level()
                && page.index() == ref.index() && ref.hasDigest(page.digest()),
            "%s: page %s does not hold the block its reference names", when, name);
        List<UUID> entries =
            CommittedSetCodec.decodeEntries(ref.level(), ref.index(), page.packedEntries());
        Set<UUID> holds = new HashSet<>(entries);
        Set<UUID> expected = select(applied, below, below + 1, ref.top());
        check(entries.size() == ref.count() && holds.equals(expected), "%s: page %s holds %d ids,"
                + " not those applied in (%d, %d]:%n  not applied: %s%n  missing: %s", when, name,
            entries.size(), below, ref.top(), minus(holds, expected), minus(expected, holds));
        paged.addAll(holds);
        below = ref.top();
      }
      Set<UUID> between = select(applied, horizon, horizon + 1, floor);
      check(paged.containsAll(between), "%s: ids applied in (H, F] = (%d, %d] are in no page: %s",
          when, horizon, floor, minus(between, paged));
      // Its blocks are the rule's: the whole-set reference encodes the same ids, marks and pages
      // to the same bytes, whatever history of adds, splits, PageOuts and merges made them
      check(Arrays.equals(set.bytes(), CommittedSetCodec.encode(
              inline, horizon, floor, set.pages(), CommittedSetCodec.BLOCK_IDS)),
          "%s: the committed set's blocks are not those the rule makes of its ids", when);
      Statistics.label("pages referenced").collect(
          set.pages().isEmpty() ? "none" : set.pages().size() == 1 ? "1" : "2 or more");
    }

    /** The ids in {@code ids} above {@code horizon}, stamped in {@code [from, to]}. */
    private static Set<UUID> select(Set<UUID> ids, long horizon, long from, long to) {
      Set<UUID> selected = new HashSet<>();
      for (UUID id : ids) {
        long ts = UuidV7.timestampMs(id);
        if ((horizon == Long.MAX_VALUE || ts > horizon) && ts >= from && ts <= to) {
          selected.add(id);
        }
      }
      return selected;
    }

    private static Set<UUID> minus(Set<UUID> a, Set<UUID> b) {
      Set<UUID> rest = new TreeSet<>(a);
      rest.removeAll(b);
      return rest;
    }

    /** The ids of the records of the transactions {@code history} names. */
    private Set<UUID> appliedIds(List<String> history) {
      Set<UUID> applied = new HashSet<>();
      for (String tag : history) {
        UUID id = ids.get(tag);
        if (id == null) {
          learnIds();
          id = ids.get(tag);
        }
        check(id != null, "%s applied, and no record of it landed", tag);
        applied.add(id);
      }
      return applied;
    }

    /** Learns the id of each transaction a record that landed carries; one id per transaction. */
    private void learnIds() {
      for (byte[] bytes : harness.landedRecords()) {
        LogEnvelope.Record record = LogEnvelope.decodeRecord(bytes);
        if (record.isIntention()) {
          String tag = EntityVersionsMachine.INSTANCE.decodeIntention(record.intention()).tag();
          UUID known = ids.putIfAbsent(tag, record.id());
          check(known == null || known.equals(record.id()), "records of %s carry ids %s and %s",
              tag, known, record.id());
        }
      }
    }

    // ============================================================
    // The drain
    // ============================================================

    /** Lands what was delayed, and judges the whole run against the final state. */
    private void drain() {
      int delivered = harness.deliverDelayed();
      trace.append("drain: delivered ").append(delivered).append(" delayed\n");
      while (!splits.isEmpty()) {
        Split split = splits.remove(0);
        trace.append("drain: split #").append(split.id).append(" lands its PageOuts\n");
        for (PageOut pageOut : split.pageOuts) {
          landPageOut(pageOut, "split");
        }
      }
      State last = runtime.read().state();
      List<String> applied = last.history();
      check(new TreeSet<>(applied).size() == applied.size(), "a commit applied twice: %s",
          applied);
      outcomes.forEach((tag, outcome) -> {
        boolean in = applied.contains(tag);
        check(outcome != Outcome.COMMITTED || in, "%s was committed and is not applied", tag);
        check((outcome != Outcome.REJECTED && outcome != Outcome.ERROR) || !in,
            "%s was reported %s and applied after all", tag, outcome);
      });
      // The serial execution of what applied, in order: each holds where it applies.
      State serial = State.empty();
      for (String tag : applied) {
        Txn txn = txns.get(tag);
        check(txn != null, "an unknown transaction %s applied", tag);
        check(txn.holdsOn(serial), "%s applied where its reads do not hold: %s", txn, serial);
        serial = serial.apply(txn);
      }
      check(serial.equals(last), "the state %s is not the serial execution %s", last, serial);
      List<String> violations = harness.checkpointViolations();
      check(violations.isEmpty(), "checkpoints: %s", violations);
      checkSet("at the drain");
      collectOrphans(applied);
    }

    /**
     * Oracle 4: a GC at the newest id that applied raises the horizon and the floor past every
     * page, the orphans' too, and a sweep then leaves none. On a log that keeps checkpoints apart a
     * compactor at retention 0 writes the GC record; on one that replaces itself it is appended.
     */
    private void collectOrphans(List<String> applied) {
      if (harness.log().pages() == null) {
        return;
      }
      long newest = appliedIds(applied).stream().mapToLong(UuidV7::timestampMs).max()
          .orElseThrow();
      if (harness.log().capabilities().separateCheckpoints()) {
        trace.append("drain: a compactor at retention 0, at ").append(newest).append('\n');
        new LogRuntime<>(harness.log(), EntityVersionsMachine.INSTANCE,
            settings.config(0L, () -> newest)).compact();
      } else {
        trace.append("drain: a GC record at ").append(newest).append('\n');
        harness.appendCopy(LogEnvelope.encodeAdvanceHorizon(newest));
      }
      Replica<State> last = runtime.read();
      check(last.committed().floor() >= newest && last.committed().pages().isEmpty(),
          "a GC record at %d left the floor at %d, and pages %s", newest,
          last.committed().floor(), last.committed().pages());
      runtime.sweep(last);
      Map<String, byte[]> left = harness.storedPages();
      check(left.isEmpty(), "pages outlived a GC record at the newest id and a sweep: %s",
          left.keySet());
      checkSet("after the GC at the newest id");
    }

    /**
     * The oracle that no replica holds a state the log never held (docs/TAIL_READS.md "Testing"):
     * the view {@code replica} was replayed from, if it keeps one, is a checkpoint the log stored
     * and the records after its position, none missing and none another log's.
     */
    private void checkHeld(Replica<State> replica, String what) {
      LogView view = replica.view();
      check(view == null || harness.held(view), "%s returned a replica of a state the log never"
          + " held: %d records after a checkpoint", what, view == null ? 0 : view.records().size());
    }

    private void check(boolean condition, String format, Object... args) {
      if (!condition) {
        throw new AssertionError(String.format(format, args) + "\ntrace:\n" + trace);
      }
    }
  }
}
