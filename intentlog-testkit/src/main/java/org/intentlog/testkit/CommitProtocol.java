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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.RandomDistribution;
import net.jqwik.api.Tuple;
import net.jqwik.api.statistics.Statistics;
import org.intentlog.CommitOutcomeUnknownException;
import org.intentlog.CommitRejectedException;
import org.intentlog.CompactionPolicy;
import org.intentlog.LogRuntime;
import org.intentlog.Replica;
import org.intentlog.RuntimeConfig;
import org.intentlog.testkit.EntityVersionsMachine.Builder;
import org.intentlog.testkit.EntityVersionsMachine.State;
import org.intentlog.testkit.EntityVersionsMachine.Txn;
import org.intentlog.testkit.LogHarness.ReadFault;
import org.intentlog.testkit.LogHarness.WriteFault;
import org.intentlog.wire.LogEnvelope;
import org.intentlog.wire.UuidV7;

/**
 * A fuzzer of the commit protocol on any log ({@link LogHarness}), over {@link
 * EntityVersionsMachine}: the TLA+ models' schedules, run on the Java (spec/tla/SharedLogCommit.tla
 * and CatalogCommit.tla).
 *
 * <p>A schedule interleaves, on one thread, writers that read the log and stage a transaction
 * ({@link Step#BEGIN}) and commit it ({@link Step#COMMIT}); a commit's write and read requests may
 * meet faults, and another writer's commit may run inside it, just before one of its requests.
 * Between commits, a compactor with its own clock and retention compacts a log that keeps
 * checkpoints apart ({@link Step#COMPACT}); a copy of any intention record that ever landed,
 * trimmed or replaced ones included, is appended again ({@link Step#DUPLICATE}); and appends
 * delayed in flight land ({@link Step#DELIVER}).
 *
 * <p>Each transaction carries its commit's tag, and the state keeps the tags it applied in order,
 * so the oracles read what applied from the state alone:
 *
 * <ul>
 *   <li>As each commit ends: a commit reported committed applied, once; one reported rejected (or
 *       any other error) did not; an error follows an injected fault; an unknown outcome follows
 *       a fault or GC; a commit nothing raced, on a log that did not fail, commits.
 *   <li>A copy of a record never applies: the state, committed set and horizon after a duplicate
 *       equal those before it, whether the original is a record, in a checkpoint, or past the GC
 *       horizon, and whether it applied or was rejected.
 *   <li>Once the run is over and every delayed append has landed (the TLA+ "drain" oracle): every
 *       outcome again, against the final state (a failed commit whose copy landed late has not
 *       applied); no commit applied twice; the state is the serial execution of the transactions
 *       applied, each one's reads holding where it applied; and every checkpoint is the replay of
 *       the log through it (I11), so readers from any of them agree.
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

  public enum Step {
    BEGIN,
    COMMIT,
    COMPACT,
    DUPLICATE,
    DELIVER
  }

  /**
   * One generated command. Its operands pick, when it runs: for {@link Step#BEGIN}, what the
   * transaction reads ({@code a}) and writes ({@code b}), as sets of entities; for {@link
   * Step#COMMIT}, the faults of its first two writes ({@code a}, {@code b}) and of one of its reads
   * ({@code c}), and the request that another writer's commit or a compactor preempts ({@code d});
   * for {@link Step#COMPACT}, whether the compactor stops after its GC record ({@code a}); for
   * {@link Step#DUPLICATE}, the record copied ({@code a}). Commands that do not apply are skipped.
   */
  public static final class Cmd {
    final Step step;
    final int writer;
    final int a;
    final int b;
    final int c;
    final int d;

    public Cmd(Step step, int writer, int a, int b, int c, int d) {
      this.step = step;
      this.writer = writer;
      this.a = a;
      this.b = b;
      this.c = c;
      this.d = d;
    }

    @Override
    public String toString() {
      switch (step) {
        case BEGIN:
          return String.format("w%d:BEGIN(%d,%d)", writer, a, b);
        case COMMIT:
          return String.format("w%d:COMMIT(%d,%d,%d,%d)", writer, a, b, c, d);
        case DUPLICATE:
          return String.format("DUPLICATE(%d)", a);
        default:
          return step.toString();
      }
    }
  }

  /** Schedules of up to 40 commands. */
  public static Arbitrary<List<Cmd>> schedules() {
    Arbitrary<Step> steps = Arbitraries.frequency(
        Tuple.of(4, Step.BEGIN), Tuple.of(4, Step.COMMIT), Tuple.of(1, Step.COMPACT),
        Tuple.of(2, Step.DUPLICATE), Tuple.of(1, Step.DELIVER));
    Arbitrary<Integer> operand = Arbitraries.integers().between(0, 15);
    return Combinators.combine(steps, Arbitraries.integers().between(0, 2), operand, operand,
            operand, operand)
        .as(Cmd::new)
        .list()
        .ofMinSize(1)
        .ofMaxSize(40)
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

    public Settings(int writers, boolean faults, long retentionMs, long compactorAheadMs,
        int maxRecords) {
      this.writers = writers;
      this.faults = faults;
      this.retentionMs = retentionMs;
      this.compactorAheadMs = compactorAheadMs;
      this.policy = maxRecords < 0
          ? (records, bytes, recordBytes) -> false
          : CompactionPolicy.limits(maxRecords, 1 << 20);
      this.policyName = maxRecords < 0 ? "never" : "every " + maxRecords + " records";
    }

    @Override
    public String toString() {
      return String.format("writers=%d faults=%s retention=%s compactor ahead %d ms, compact %s",
          writers, faults, retentionMs == NO_GC ? "none" : retentionMs + " ms", compactorAheadMs,
          policyName);
    }
  }

  /** Settings drawn: 2 or 3 writers, GC off or at retention 0 or 50 ms, compaction policies. */
  public static Arbitrary<Settings> settings() {
    return Combinators.combine(
            Arbitraries.integers().between(2, 3),
            Arbitraries.of(true, false),
            Arbitraries.of(NO_GC, NO_GC, 0L, 50L),
            Arbitraries.of(0L, 0L, 1_000L),
            Arbitraries.of(-1, 0, 2, 5))
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
    final int preemptAt;
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
        Writer pending, Boolean compaction, int preemptAt) {
      this.plan = plan;
      this.failingRead = failingRead;
      this.readFault = readFault;
      this.everyRead = everyRead;
      this.pending = pending;
      this.compaction = compaction;
      this.preemptAt = preemptAt;
    }
  }

  private static final class Run implements LogHarness.Hooks {
    private final LogHarness harness;
    private final Settings settings;
    private final LogRuntime<State, Builder, Txn> runtime;
    private final LogRuntime<State, Builder, Txn> compactor;
    private final Writer[] writers;
    private final Deque<Context> commits = new ArrayDeque<>();
    private final Map<String, Txn> txns = new HashMap<>();
    private final Map<String, Outcome> outcomes = new LinkedHashMap<>();
    private final StringBuilder trace = new StringBuilder();

    Run(LogHarness harness, Settings settings) {
      this.harness = harness;
      this.settings = settings;
      this.runtime = new LogRuntime<>(harness.log(), EntityVersionsMachine.INSTANCE,
          new RuntimeConfig(4, 3, 0L, 0L, settings.retentionMs, settings.policy, Duration.ZERO,
              System::currentTimeMillis));
      this.compactor = new LogRuntime<>(harness.log(), EntityVersionsMachine.INSTANCE,
          new RuntimeConfig(4, 3, 0L, 0L, settings.retentionMs, settings.policy, Duration.ZERO,
              () -> System.currentTimeMillis() + settings.compactorAheadMs));
      this.writers = new Writer[settings.writers];
      for (int i = 0; i < writers.length; i++) {
        writers[i] = new Writer(i);
      }
      trace.append(settings).append('\n');
      Txn init = new Txn("init", Map.of(), Set.of());
      txns.put("init", init);
      runtime.bootstrap(init);
      harness.hook(this);
      Statistics.label("settings").collect(settings.retentionMs == NO_GC ? "no gc" : "gc");
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

    /** Counts a request of the commit {@code c}; runs the preempting commit or compactor first. */
    private void request(Context c) {
      int request = c.requests++;
      if ((c.pending == null && c.compaction == null) || request != c.preemptAt) {
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
        commit(other, List.of(), -1, ReadFault.NONE, false, null, null, -1);
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
        compactor.compact();
        Statistics.label("steps").collect("compact");
        return true;
      }
      long now = System.currentTimeMillis() + settings.compactorAheadMs;
      if (settings.retentionMs >= now) {
        return false;
      }
      trace.append("  GC record at ").append(now - settings.retentionMs).append('\n');
      harness.log().append(LogEnvelope.encodeAdvanceHorizon(now - settings.retentionMs), null);
      Statistics.label("steps").collect("GC record");
      return true;
    }

    // ============================================================
    // The schedule
    // ============================================================

    void execute(List<Cmd> schedule) {
      for (Cmd cmd : schedule) {
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
            compact(cmd.a % 3 != 0);
            break;
          case DUPLICATE:
            duplicate(cmd.a);
            break;
          case DELIVER:
            int delivered = harness.deliverDelayed();
            if (delivered > 0) {
              trace.append("delivered ").append(delivered).append(" delayed\n");
              Statistics.label("steps").collect("deliver");
            }
            break;
          default:
            throw new IllegalStateException("Unknown step " + cmd.step);
        }
      }
      for (Writer w : writers) {
        if (w.snapshot != null) {
          commit(w, List.of(), -1, ReadFault.NONE, false, null, null, -1);
        }
      }
      drain();
    }

    private void begin(Writer w, Cmd cmd) {
      w.snapshot = runtime.read();
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
      // only its GC record, as a + b is odd or even.
      int preemptAt = cmd.d < 8 ? -1 : (cmd.d - 8) % 4;
      Writer other = null;
      Boolean compaction = null;
      if (preemptAt >= 0 && cmd.d >= 12) {
        compaction = (cmd.a + cmd.b) % 2 == 1;
      }
      for (int k = 1; preemptAt >= 0 && cmd.d < 12 && other == null && k < writers.length; k++) {
        Writer candidate = writers[(w.id + k) % writers.length];
        other = candidate.snapshot != null ? candidate : null;
      }
      if (settings.faults) {
        // c: no read fault (0-4); fail, stale or break P3 on one of the first three reads
        // (5-13); stale every read (14-15).
        ReadFault readFault = cmd.c < 5 ? ReadFault.NONE
            : cmd.c >= 14 ? ReadFault.STALE
            : List.of(ReadFault.FAIL, ReadFault.STALE, ReadFault.PAST_RECORD).get(cmd.c % 3);
        commit(w, List.of(FIRST_WRITE_FAULTS.get(cmd.a), SECOND_WRITE_FAULTS.get(cmd.b)),
            (cmd.c - 5) / 3, readFault, cmd.c >= 14, other, compaction, preemptAt);
      } else {
        commit(w, List.of(), -1, ReadFault.NONE, false, other, compaction, preemptAt);
      }
    }

    private void commit(Writer w, List<WriteFault> plan, int failingRead, ReadFault readFault,
        boolean everyRead, Writer preempting, Boolean compaction, int preemptAt) {
      Txn txn = w.txn;
      boolean raced = harness.landedWrites() != w.snapshotWrites;
      Context ctx = new Context(plan, failingRead, readFault, everyRead, preempting, compaction,
          preemptAt);
      trace.append(String.format("w%d commit %s%n", w.id, txn.tag()));
      commits.push(ctx);
      Outcome outcome;
      String reason = "";
      try {
        ctx.active = true;
        runtime.commit(w.snapshot, txn);
        outcome = Outcome.COMMITTED;
      } catch (CommitRejectedException e) {
        outcome = Outcome.REJECTED;
        reason = " (" + e.reason() + ")";
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
      }
      w.snapshot = null;
      w.txn = null;
      outcomes.put(txn.tag(), outcome);
      trace.append(String.format("w%d %s -> %s%s%n", w.id, txn.tag(), outcome, reason));
      Statistics.label("outcome").collect(outcome + (ctx.faulted ? " after a fault" : ""));

      List<String> applied = runtime.read().state().history();
      long times = applied.stream().filter(txn.tag()::equals).count();
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
    }

    /** Appends a copy of a record that landed; it must change nothing. */
    private void duplicate(int pick) {
      List<byte[]> landed = harness.landedRecords();
      if (landed.isEmpty()) {
        return;
      }
      byte[] copy = landed.get(pick % landed.size());
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
      Statistics.label("steps").collect("duplicate");
      Statistics.label("duplicate of").collect(original);
    }

    /** What became of the original of {@code copy}, as a read finds the log. */
    private String original(byte[] copy, Replica<State> live) {
      java.util.UUID id = LogEnvelope.decodeRecord(copy).id();
      boolean record =
          harness.log().read().records().stream().anyMatch(r -> Arrays.equals(r, copy));
      String where = record ? "a record" : "compacted out";
      if (live.committed().ids().contains(id)) {
        return "applied, " + where + ", id kept";
      }
      if (UuidV7.timestampMs(id) <= live.committed().horizon()) {
        return "applied or not, " + where + ", id past the horizon";
      }
      return "rejected, " + where;
    }

    /** Lands what was delayed, and judges the whole run against the final state. */
    private void drain() {
      int delivered = harness.deliverDelayed();
      trace.append("drain: delivered ").append(delivered).append(" delayed\n");
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
    }

    private void check(boolean condition, String format, Object... args) {
      if (!condition) {
        throw new AssertionError(String.format(format, args) + "\ntrace:\n" + trace);
      }
    }
  }
}
