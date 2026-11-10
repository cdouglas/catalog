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
package org.intentlog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.intentlog.wire.CommittedSetCodec;
import org.intentlog.wire.LogEnvelope;
import org.intentlog.wire.PageRef;
import org.intentlog.wire.UuidV7;

/**
 * Replicates a state machine through a {@link SharedLog}: reads it (a checkpoint and the records
 * after it, replayed), and commits intentions to it.
 *
 * <p>A commit reports one of three outcomes, each of which a caller can rely on: it returns (the
 * intention took effect, once), it throws {@link CommitRejectedException} (it did not and will
 * not), or {@link CommitOutcomeUnknownException} (either). A failure to read the log while none
 * of the commit's writes may have landed is rethrown as it is. A commit is reported rejected only
 * before any write, or after a read of the log that shows its record rejected or absent with no
 * write of it still able to land (I10).
 *
 * <p>A log with a {@link PageStore} pages its committed set (docs/COMMITTED_PAGING.md): a
 * compaction seals the oldest blocks older than the admission window into pages, while the inline
 * set is larger than the threshold ({@link RuntimeConfig#admissionWindowMs}, {@link
 * RuntimeConfig#pageThresholdBytes}). It puts each page before the {@code PageOut} record that
 * names it, deletes the pages its GC record dropped once the record has landed, and sweeps the
 * orphans, pages no PageOut references whose node the floor has passed. A lookup of an id between
 * the horizon and the floor reads its page.
 */
public final class LogRuntime<S, B extends StateMachine.Builder<S>, I extends Intention<B>> {

  /**
   * With {@code -Dintentlog.replay.check=true} (the test suites set it), each replay that folds
   * records onto a replica ({@link #replay(LogView, UUID, UUID, Replica)}) is checked against a
   * replay of the whole view: the same checkpoint of the state, committed set, record count and
   * outcome.
   */
  static final boolean CHECK = Boolean.getBoolean("intentlog.replay.check");

  /** How many times a lookup reads the log again when a page it names is gone. */
  private static final int MAX_PAGE_REREADS = 2;

  /** The pages a runtime keeps (their verified entries): they never change. */
  private static final int PAGE_CACHE = 4;

  private final SharedLog log;
  private final StateMachine<S, B, I> machine;
  private final RuntimeConfig config;
  // Pages read, by name: their entries, checked against the reference that named them
  private final Map<String, byte[]> pageCache =
      Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, byte[]> eldest) {
          return size() > PAGE_CACHE;
        }
      });
  // Pages this runtime put. A name's bytes never change, and a name is deleted only once the
  // floor has passed its node, after which no compactor proposes it again: so a put once is put.
  private final Set<String> pagesPut =
      Collections.newSetFromMap(Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
          return size() > 64;
        }
      }));

  // Compaction after a commit, on a log that replaces itself: the policy's progress at which this
  // runtime's writer compacts next (NaN: not drawn yet), the progress it saw last, and how many of
  // its compactions in a row lost their race since the log was last compacted. Guarded by `this`.
  private double compactAt = Double.NaN;
  private double progressSeen = 0.0;
  private int compactionsLost = 0;

  public LogRuntime(SharedLog log, StateMachine<S, B, I> machine, RuntimeConfig config) {
    this.log = log;
    this.machine = machine;
    this.config = config;
  }

  /** A replica of the log as a fresh read finds it. A read failure is thrown as it is. */
  public Replica<S> read() {
    CommitTrace.event(CommitTrace.Kind.READ, 0, 0);
    Replica<S> read = replay(viewed(log.read()), null).replica();
    CommitTrace.event(CommitTrace.Kind.BUILT);
    return read;
  }

  /**
   * A fresh read, or {@code floor} if the read lags behind it: a replica this reader read or
   * committed in, so a read of the log too (P1 lets a read return any prefix). A reader that
   * passes the newest replica it has seen never sees the log go back, past its own commit
   * included (Bug 72). A null floor is no floor. A read that continues the one {@code floor} was
   * replayed from replays only the records after it. With tail reads ({@link
   * RuntimeConfig#tailReads}) it reads only what follows {@code floor}'s tail ({@link
   * SharedLog#readAfter}), when the log still continues it.
   */
  public Replica<S> read(Replica<S> floor) {
    boolean after = floor != null && config.tailReads();
    CommitTrace.event(CommitTrace.Kind.READ, after ? 1 : 0, 0);
    Replica<S> read = after
        ? readAfter(floor, null, floor.logId()).replica()
        : replay(viewed(log.read()), null, null, floor).replica();
    CommitTrace.event(CommitTrace.Kind.BUILT);
    return newer(floor, read);
  }

  /**
   * A read of what follows {@code prior}'s tail (docs/TAIL_READS.md): a view that continues
   * prior's, whose new records are folded onto it; a view of records alone, folded onto prior if
   * they follow its tail ({@link LogGapException} if not); or a whole view, replayed. A whole read
   * if the log trimmed the records since, or if a fold could not say what replay did with the
   * record of {@code watch}, which prior's view does not show.
   */
  private Replayed readAfter(Replica<S> prior, UUID watch, UUID logId) {
    LogView view;
    try {
      view = viewed(log.readAfter(prior.tail()));
    } catch (TrimmedException e) {
      return replay(viewed(log.read()), watch, logId, prior);
    }
    return replayAfter(view, prior, watch, logId);
  }

  /** {@code view}, a read after {@code prior}'s tail, replayed onto prior (as {@link #readAfter}). */
  private Replayed replayAfter(LogView view, Replica<S> prior, UUID watch, UUID logId) {
    Replayed folded = foldAfter(view, prior, watch, logId);
    return folded != null ? folded : replay(viewed(log.read()), watch, logId, prior);
  }

  /** {@code view}, reported to the thread's trace as it arrives ({@link CommitTrace}). */
  private static LogView viewed(LogView view) {
    CommitTrace.event(CommitTrace.Kind.VIEW, view.checkpoint().isPresent() ? 1 : 0,
        view.records().size());
    return view;
  }

  /**
   * {@code view}, a read after {@code prior}'s tail, replayed onto prior; or null if it is records
   * alone that a fold could not judge {@code watch}'s record in (prior's view does not show it):
   * the caller reads the log whole, as its reads must (a commit's, up to its floor).
   */
  private Replayed foldAfter(LogView view, Replica<S> prior, UUID watch, UUID logId) {
    if (view.checkpoint().isPresent()) {
      return replay(view, watch, logId, prior);
    }
    checkFollows(view, prior.tail());
    if (watch != null && (prior.view() == null || holds(prior.view(), watch))) {
      return null;
    }
    Replay.Result<B> result =
        Replay.replayOnto(machine, machine.toBuilder(prior.state()), prior.committed().toBuilder(),
            prior.logId(), prior.intentionRecords(), view.records(), watch);
    CommitTrace.event(CommitTrace.Kind.REPLAYED, 1, view.records().size());
    return new Replayed(null, view.tail(), prior.checkpointBytes(), result,
        logId != null ? logId : prior.logId());
  }

  /** Whether {@code view} holds a record of {@code watch}. */
  private static boolean holds(LogView view, UUID watch) {
    for (byte[] record : view.records()) {
      LogEnvelope.Record decoded = LogEnvelope.decodeRecord(record);
      if (decoded.isIntention() && decoded.id().equals(watch)) {
        return true;
      }
    }
    return false;
  }

  /**
   * {@code later}, unless it lags behind {@code earlier} (a replica read or committed in before
   * it was read): then {@code earlier}. Only a log that cannot replace itself has reads that lag,
   * as the commit loop's floor assumes; on one that replaces itself, a tail that does not continue
   * an earlier one is a replace since, which is newer.
   */
  public Replica<S> newer(Replica<S> earlier, Replica<S> later) {
    if (earlier == null || log.capabilities().fusedReplace()
        || later.tail().continues(earlier.tail())) {
      return later;
    }
    return earlier;
  }

  /**
   * {@code prior} brought up to date: the records appended since folded onto it, if the log reads
   * them apart ({@link SharedLog#readAfter}); else, or if it trimmed them, a fresh read. Either
   * way the same replica, since replay is deterministic and every checkpoint is the replay of the
   * log through it (I11). Never one behind {@code prior}, which a fresh read on a log whose reads
   * lag could be.
   */
  public Replica<S> refresh(Replica<S> prior) {
    LogView view;
    try {
      view = log.readAfter(prior.tail());
    } catch (TrimmedException e) {
      return read(prior);
    }
    if (view.checkpoint().isPresent()) {
      return newer(prior, replay(view, null, prior.logId(), prior).replica());
    }
    // Records alone: fold them only onto the tail they follow
    checkFollows(view, prior.tail());
    Replay.Result<B> result =
        Replay.replayOnto(machine, machine.toBuilder(prior.state()), prior.committed().toBuilder(),
            prior.logId(), prior.intentionRecords(), view.records(), null);
    // The view holds only the records after prior's, so the replica is the replay of no view;
    // they follow prior's checkpoint
    return new Replayed(null, view.tail(), prior.checkpointBytes(), result, prior.logId())
        .replica();
  }

  /**
   * Compacts a log that keeps its checkpoints apart from its records ({@link
   * Capabilities#separateCheckpoints}): appends a GC record at the clock less the retention (none
   * when GC is off), reads through it; pages the oldest blocks of the committed set there, as the
   * policy says, by putting each page and appending its PageOut, and reads through the last; stores
   * the replay there as a checkpoint, and trims the records it holds. Then it deletes the pages a
   * GC record dropped, and sweeps the orphans. Any compactor may run it at any time, and each step
   * is safe to repeat or to abandon: a GC record that lands twice drops nothing new, a PageOut that
   * lands twice (or after its block changed) changes nothing, a checkpoint is the replay of the log
   * through its position whoever writes it (I11), and the trim follows a durable checkpoint (P5).
   * Returns the replica it checkpointed.
   */
  public Replica<S> compact() {
    if (!log.capabilities().separateCheckpoints()) {
      throw new UnsupportedOperationException(
          "A log that replaces itself compacts as it commits: " + log);
    }
    long now = config.clockMs().getAsLong();
    long watermark = watermark(now, Long.MAX_VALUE);
    LogView view;
    if (watermark > 0L) {
      byte[] gc = LogEnvelope.encodeAdvanceHorizon(watermark);
      view = log.readThrough(sendThrottled(() -> log.append(gc, null)), config.visibilityWait());
    } else {
      view = log.read();
    }
    // The pages the GC records this compactor folds drop, its own among them: whether the
    // checkpoint names them or a PageOut after it does, they leave the set there for good
    Set<PageRef> dropped = new LinkedHashSet<>();
    Replayed replayed = replay(view, null);
    dropped.addAll(replayed.result.committed().droppedPages());
    Replica<S> through = replayed.replica();
    List<CommittedSet.Seal> seals =
        page(through.committed(), pageCeiling(now, Long.MAX_VALUE), through.logId());
    if (!seals.isEmpty()) {
      Receipt last = null;
      for (CommittedSet.Seal seal : seals) {
        byte[] pageOut = LogEnvelope.encodePageOut(seal.record());
        last = sendThrottled(() -> log.append(pageOut, null));
      }
      view = log.readThrough(last, config.visibilityWait());
      replayed = replay(view, null, null, through);
      dropped.addAll(replayed.result.committed().droppedPages());
      through = replayed.replica();
    }
    log.putCheckpoint(view.tail(), checkpoint(through, view.tail()));
    log.trim(view.tail());
    tidyPages(new ArrayList<>(dropped), !seals.isEmpty(), through.committed());
    deleteDroppedStatePages(through.state());
    return through;
  }

  /**
   * The GC watermark a compaction writes at {@code now}: the clock less the retention, at most
   * {@code ceiling}; 0 (no GC record) when the retention reaches back past the epoch ({@code
   * Long.MAX_VALUE} disables GC).
   */
  private long watermark(long now, long ceiling) {
    long retention = config.committedRetentionMs();
    long watermark = retention >= now ? 0L : now - retention;
    return Math.max(0L, Math.min(watermark, ceiling));
  }

  /**
   * The newest top a compaction at {@code now} may seal: the clock less the admission window, at
   * most {@code ceiling}; -1 (nothing) when the window reaches back past the epoch.
   */
  private long pageCeiling(long now, long ceiling) {
    long window = config.admissionWindowMs();
    return Math.min(window >= now ? -1L : now - window, ceiling);
  }

  // ============================================================
  // Pages
  // ============================================================

  /**
   * The pages a compaction seals of {@code set}, its view of the committed set after its own GC
   * (so that it pages no id about to be dropped), each put before it is returned: while the inline
   * blocks are larger than the threshold, the oldest block if its top is at or below {@code
   * ceilingMs} and it is not the newest. Each is computed on the set after the ones before it.
   * Stops at a put that fails: its PageOut is not sent, nor any after it. None if the log stores no
   * pages.
   */
  private List<CommittedSet.Seal> page(CommittedSet set, long ceilingMs, UUID logId) {
    PageStore store = log.pages();
    if (store == null || ceilingMs < 0L) {
      return List.of();
    }
    List<CommittedSet.Seal> seals = new ArrayList<>();
    while (set.inlineBytes() > config.pageThresholdBytes()) {
      CommittedSet.Seal seal = set.sealable(ceilingMs);
      if (seal == null) {
        break;
      }
      if (!pagesPut.contains(seal.name())) {
        try {
          store.put(seal.name(), seal.page(logId));
        } catch (RuntimeException e) {
          LOG.log(System.Logger.Level.WARNING,
              "Putting page " + seal.name() + " of " + log + " failed; not paging it", e);
          break;
        }
        pagesPut.add(seal.name());
      }
      seals.add(seal);
      set = set.pagedOut(seal.record());
    }
    return seals;
  }

  /** The pages {@code before} names (oldest first) that {@code after}, a later set, does not. */
  private static List<PageRef> dropped(List<PageRef> before, CommittedSet after) {
    if (before.isEmpty()) {
      return List.of();
    }
    Set<PageRef> kept = new HashSet<>(after.pages());
    List<PageRef> dropped = new ArrayList<>();
    for (PageRef page : before) {
      if (!kept.contains(page)) {
        dropped.add(page);
      }
    }
    return dropped;
  }

  /**
   * After a write landed: deletes {@code dropped}, pages a GC record that landed dropped, and, if
   * the write dropped or sealed any page, sweeps the orphans against {@code landed}, the set it
   * landed in. Best effort: a page left behind costs storage, and a later sweep finds it.
   */
  private void tidyPages(List<PageRef> dropped, boolean sealed, CommittedSet landed) {
    PageStore store = log.pages();
    if (store == null || (dropped.isEmpty() && !sealed)) {
      return;
    }
    try {
      for (PageRef page : dropped) {
        store.delete(page.name());
      }
      sweep(store, landed);
    } catch (RuntimeException e) {
      LOG.log(System.Logger.Level.WARNING, "Deleting dropped or orphaned pages of " + log
          + " failed", e);
    }
  }

  /**
   * Deletes every page {@code set} does not reference whose node ends at or below its floor
   * (docs/COMMITTED_PAGING.md "Orphans"). Such a page can never become referenced, since a PageOut
   * names the oldest block above the floor and the floor only rises; a page whose PageOut may still
   * land is above it. A store that cannot list its pages is not swept.
   */
  private static void sweep(PageStore store, CommittedSet set) {
    List<String> names;
    try {
      names = store.list();
    } catch (UnsupportedOperationException e) {
      return;
    }
    Set<String> referenced = new HashSet<>();
    for (PageRef page : set.pages()) {
      referenced.add(page.name());
    }
    for (String name : names) {
      long top = CommittedSetCodec.pageTop(name);
      if (top >= 0L && top <= set.floor() && !referenced.contains(name)) {
        store.delete(name);
      }
    }
  }

  /** A page a reference names that the page store does not hold: GC dropped it since. */
  private static final class MissingPage extends RuntimeException {
    MissingPage(PageRef page) {
      super("Page " + page.name() + " does not exist", null, false, false);
    }
  }

  /**
   * Whether the page {@code ref} names holds {@code id}. Reads the page through the log's page
   * store, once (pages never change), and checks it is the one the reference names: the log's, its
   * node, its digest; a page that is not fails the lookup.
   *
   * @throws MissingPage if the store holds no such page: GC dropped it, and its compactor deleted
   *     it, since the replica that names it was read
   * @throws RuntimeException if the page could not be read: never read as "absent"
   */
  private boolean pageHolds(PageRef ref, UUID logId, UUID id) {
    byte[] packed = pageCache.get(ref.name());
    if (packed == null) {
      PageStore store = log.pages();
      if (store == null) {
        throw new IllegalStateException(
            log + " names page " + ref.name() + " but stores no pages");
      }
      Optional<byte[]> body = store.get(ref.name());
      if (body.isEmpty()) {
        throw new MissingPage(ref);
      }
      LogEnvelope.Page page = LogEnvelope.decodePage(body.get());
      if (logId != null && !logId.equals(page.logId())) {
        throw new IllegalStateException(
            "Page " + ref.name() + " belongs to log " + page.logId() + ", not " + logId);
      }
      if (page.level() != ref.level() || page.index() != ref.index()
          || !ref.hasDigest(page.digest())) {
        throw new IllegalStateException(
            "Page " + ref.name() + " does not hold the block its reference names");
      }
      packed = page.packedEntries();
      pageCache.put(ref.name(), packed);
    }
    return CommittedSetCodec.holds(ref.level(), ref.index(), packed, id);
  }

  /**
   * Whether the record of {@code id} applied, in the log as {@code replica} holds it (docs/LOG.md,
   * the tri-state lookup): from the inline set above the floor; from the page that holds it in
   * (H, F], read through the log's page store (a page GC has dropped since the replica was read
   * sends the lookup to a fresh read, as at most {@link #MAX_PAGE_REREADS} times); false where no
   * page can hold it.
   *
   * @throws DedupHorizonExceededException if the id is at or below the horizon
   * @throws RuntimeException if a page could not be read, or a read failed
   */
  public boolean committed(Replica<S> replica, UUID id) {
    for (int rereads = 0; ; rereads++) {
      try {
        return replica.committed().contains(id);
      } catch (PagedOutException e) {
        try {
          return pageHolds(e.page(), replica.logId(), id);
        } catch (MissingPage missing) {
          if (rereads >= MAX_PAGE_REREADS) {
            throw new IllegalStateException(
                "Page " + e.page().name() + " of " + log + " is gone on every read", missing);
          }
          replica = read(replica);
        }
      }
    }
  }

  /**
   * Deletes the orphaned pages of the log as {@code replica} holds it: each page in the log's page
   * store that its committed set does not reference and whose node ends at or below its floor
   * (docs/COMMITTED_PAGING.md "Orphans"). Safe from any replica, however stale: a PageOut that
   * lands later seals a block above the floor where it lands, so its page ends above the floor of
   * every replica before it. The runtime sweeps after each write that sealed or dropped a page;
   * this is for a maintenance job, or a test. Does nothing if the log stores no pages, or its
   * store cannot list them.
   *
   * @throws RuntimeException if listing or deleting a page failed
   */
  public void sweep(Replica<S> replica) {
    PageStore store = log.pages();
    if (store != null) {
      sweep(store, replica.committed());
      List<String> names;
      try {
        names = store.list();
      } catch (UnsupportedOperationException e) {
        return;
      }
      for (String name : machine.garbagePages(replica.state(), names)) {
        store.delete(name);
      }
    }
  }

  /**
   * After a checkpoint of {@code state} landed: deletes the pages its records dropped
   * ({@link StateMachine#droppedPages}), garbage under it and every checkpoint after. Best effort:
   * a page left behind costs storage, and a sweep finds it.
   */
  private void deleteDroppedStatePages(S state) {
    java.util.Collection<String> dropped = machine.droppedPages(state);
    PageStore store = log.pages();
    if (dropped.isEmpty() || store == null) {
      return;
    }
    try {
      for (String name : dropped) {
        store.delete(name);
      }
      machine.pagesDeleted(state, dropped);
    } catch (RuntimeException e) {
      LOG.log(System.Logger.Level.WARNING, "Deleting dropped pages of " + log + " failed", e);
    }
  }

  /**
   * The checkpoint of {@code replica}: exactly its log id, committed set and state. The pages the
   * state names that its records made are put first ({@link StateMachine#pages}): a checkpoint is
   * never written before a page it names. A put that fails fails the checkpoint, before anything
   * is sent.
   */
  private byte[] checkpoint(Replica<S> replica) {
    putStatePages(replica.state());
    return LogEnvelope.encodeCheckpoint(
        replica.logId(), replica.committed().bytes(), machine.encodeCheckpoint(replica.state()));
  }

  /**
   * A checkpoint of {@code replica} to store apart from the records, naming the position it covers,
   * {@code through}'s: a read checks that the records after it start there (S4).
   */
  private byte[] checkpoint(Replica<S> replica, LogTail through) {
    putStatePages(replica.state());
    return LogEnvelope.encodeCheckpoint(replica.logId(), replica.committed().bytes(),
        machine.encodeCheckpoint(replica.state()), through.position());
  }

  /**
   * Fails unless {@code view}'s records start where its checkpoint ends: on a log that stores its
   * checkpoints apart from its records, the position the checkpoint covers, if it names one, is
   * the one the provider says the records follow (P1, S4). A log that writes a checkpoint with its
   * records in one write checks the joint itself (one object: the trailers), whatever position a
   * checkpoint copied from another log names.
   */
  private void checkJoint(LogView view) {
    if (!log.capabilities().separateCheckpoints()) {
      return;
    }
    byte[] checkpoint = view.checkpoint().orElse(null);
    byte[] through = checkpoint == null ? null : LogEnvelope.checkpointThrough(checkpoint);
    if (through == null) {
      return;
    }
    byte[] start = view.start();
    if (start == null) {
      throw new LogGapException("A view of a checkpoint that covers a position says nothing of"
          + " where its records start");
    }
    if (!Arrays.equals(through, start)) {
      throw new LogGapException("A checkpoint covers " + hex(through) + ", and the records after"
          + " it follow " + hex(start));
    }
  }

  /** Fails unless the records of {@code view}, a view of records alone, follow {@code after}. */
  private static void checkFollows(LogView view, LogTail after) {
    byte[] expected = after.position();
    byte[] start = view.start();
    if (expected == null || start == null || !Arrays.equals(expected, start)) {
      throw new LogGapException("Records read after " + after + " follow "
          + (start == null ? "an unknown position" : hex(start)) + ", not "
          + (expected == null ? "an unknown position" : hex(expected)));
    }
  }

  private static String hex(byte[] bytes) {
    StringBuilder s = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      s.append(String.format("%02x", b));
    }
    return s.toString();
  }

  private void putStatePages(S state) {
    Map<String, byte[]> pages = machine.pages(state);
    if (pages.isEmpty()) {
      return;
    }
    PageStore store = log.pages();
    if (store == null) {
      throw new IllegalStateException(
          log + " stores no pages, but its state names " + pages.keySet());
    }
    // The machine offers a page until it is stored, so each is put once
    for (Map.Entry<String, byte[]> page : pages.entrySet()) {
      store.put(page.getKey(), page.getValue());
    }
    machine.pagesPut(state, pages.keySet());
  }

  /** Sends a write again while the log throttles it, with the commit loop's backoff. */
  private Receipt sendThrottled(Supplier<Receipt> send) {
    long backoffMs = config.throttleMinWaitMs();
    for (int attempt = 1; ; attempt++) {
      try {
        return send.get();
      } catch (LogThrottledException e) {
        if (attempt >= config.maxThrottleAttempts()) {
          throw e;
        }
        sleep(backoffMs + ThreadLocalRandom.current().nextLong(backoffMs / 2 + 1), e);
        backoffMs = Math.min(backoffMs * 2, Math.max(5_000L, config.throttleMinWaitMs()));
      }
    }
  }

  /** Sleeps; an interrupt stops the caller, with {@code cause} (its outcome unknown) thrown. */
  private static void sleep(long ms, RuntimeException cause) {
    if (ms <= 0L) {
      return;
    }
    try {
      Thread.sleep(ms);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      cause.addSuppressed(ie);
      throw new CommitOutcomeUnknownException(cause);
    }
  }

  /**
   * A replica of the log before it exists: the empty state, with a new log id, at the tail of the
   * log not yet created. A commit on it creates the log, and is rejected ({@link
   * CommitRejectedException.Reason#CREATE_LOST}) if another writer created it first.
   *
   * @throws IllegalStateException if the log exists
   */
  public Replica<S> empty() {
    LogTail tail = log.probe();
    if (tail.exists()) {
      throw new IllegalStateException("The log exists");
    }
    return before(tail);
  }

  /**
   * Creates the log, with a record of {@code initial}, unless it exists. Writers may bootstrap the
   * same log at once: one creates it, and the others find it created. A create that fails or
   * whose outcome is unknown is no failure if the log exists afterwards: another writer created
   * it, or this create landed.
   */
  public void bootstrap(I initial) {
    LogTail tail = log.probe();
    if (tail.exists()) {
      return;
    }
    try {
      commit(before(tail), initial);
    } catch (CommitRejectedException | CommitOutcomeUnknownException e) {
      if (!log.probe().exists()) {
        throw e;
      }
    }
  }

  private Replica<S> before(LogTail absent) {
    return new Replica<>(machine.empty().build(), CommittedSet.EMPTY, absent, 0, UuidV7.newUuidV7());
  }

  /**
   * Commits {@code intention}, built from {@code base}, and returns the replica of the log its
   * record landed in. Then, as the policy says of that replica, its writer may compact the log
   * ({@link #compact} on a log that keeps its checkpoints apart, {@code compactAfter} on one that
   * replaces itself); the commit stands whatever becomes of that.
   *
   * @throws CommitRejectedException if it did not take effect, and never will
   * @throws CommitOutcomeUnknownException if it may or may not have
   */
  public Replica<S> commit(Replica<S> base, I intention) {
    Replica<S> landed = new Commit(base, intention).run();
    Capabilities caps = log.capabilities();
    if (caps.separateCheckpoints() && !caps.fusedReplace()) {
      // The writer whose commit landed compacts, as the policy says of the log it read; the commit
      // stands whatever becomes of the compaction.
      try {
        if (config.compaction().due(new CompactionPolicy.Input(landed.intentionRecords(),
            landed.checkpointBytes(), landed.tail().bytes(), 0))) {
          compact();
        }
      } catch (RuntimeException e) {
        LOG.log(System.Logger.Level.WARNING, "Compaction after a commit failed: " + log, e);
      }
    } else if (caps.fusedReplace() && caps.append() && config.compactAfterCommit()) {
      compactAfter(landed);
    }
    return landed;
  }

  /**
   * Compacts a log that replaces itself (one object) after a commit landed in it, as {@code
   * landed}: if the policy says the log is due, and its progress past the limits has reached the
   * point this writer drew ({@link #drawn}), replaces the version the commit landed in with a
   * checkpoint of it, a GC record at the clock less the retention and the PageOuts the policy calls
   * for, and no record. Conditioned on that version, the replace lands only if no write landed
   * since, and then holds the state the log held (I11); one that loses its race, fails or stays
   * throttled is dropped, with what it wrote if it landed after all, since a replace that lands
   * late lands on the same version. Whatever becomes of it, the commit stands. After it lands, it
   * deletes the pages its GC record dropped and sweeps. Docs: LOG.md "Compaction".
   */
  private void compactAfter(Replica<S> landed) {
    CompactionPolicy policy = config.compaction();
    CompactionPolicy.Input input;
    try {
      input = new CompactionPolicy.Input(landed.intentionRecords(), landed.checkpointBytes(),
          landed.tail().bytes(), 0);
    } catch (RuntimeException e) {
      return; // the log's size is unknown: the next commit decides
    }
    double progress = policy.progress(input);
    if (!policy.due(input) || replacesEveryCommit(policy, landed) || !drawn(progress, policy)) {
      return;
    }
    CommitTrace.event(CommitTrace.Kind.COMPACT, landed.intentionRecords(), input.logBytes());
    boolean lands = false;
    try {
      Prepared replace = prepare(landed, null, Long.MAX_VALUE);
      CommitTrace.event(CommitTrace.Kind.PREPARED, replace.checkpoint.length, 0);
      CommitTrace.event(CommitTrace.Kind.SEND, 2, 0);
      sendThrottled(() -> log.replace(landed.tail(), replace.checkpoint, replace.records));
      CommitTrace.event(CommitTrace.Kind.LANDED);
      lands = true;
      // The set the replace landed in: the GC record and PageOuts it wrote, after the checkpoint
      Replay.Result<B> after = Replay.replayOnto(machine, machine.toBuilder(landed.state()),
          landed.committed().toBuilder(), landed.logId(), 0, replace.records, null);
      tidyPages(replace.dropped, replace.sealed, after.committed().build());
      @SuppressWarnings("unchecked")
      S checkpointed = (S) replace.state;
      deleteDroppedStatePages(checkpointed);
    } catch (LogConflictException e) {
      CommitTrace.event(CommitTrace.Kind.LOST, 0, 0);
    } catch (CommitOutcomeUnknownException e) {
      // interrupted in a backoff: the commit stands, and the interrupt is kept
      CommitTrace.event(CommitTrace.Kind.AMBIGUOUS);
    } catch (RuntimeException e) {
      CommitTrace.event(CommitTrace.Kind.AMBIGUOUS);
      LOG.log(System.Logger.Level.WARNING, "Compaction after a commit failed: " + log, e);
    } finally {
      tried(progress, lands, policy);
    }
  }

  /**
   * Whether {@code policy} has every commit to the log at {@code replica} replace it: it is due on
   * the log with no records after its checkpoint and one more of a byte (CAS only, or a size limit
   * of 0). Such a commit wrote a checkpoint, and needs no compaction after it.
   */
  private static boolean replacesEveryCommit(CompactionPolicy policy, Replica<?> replica) {
    return policy.due(new CompactionPolicy.Input(
        0, replica.checkpointBytes(), replica.checkpointBytes(), 1));
  }

  /**
   * Whether this runtime's writer compacts a log at {@code progress} toward compaction (1 or more:
   * due): once it reaches a point this writer drew, in [1, 2) after the log was last compacted
   * (a progress below the last one seen says it was), and after each compaction it tried, as
   * {@link #tried} moved it. Writers draw apart, so that past the limits they seldom compact at
   * once.
   */
  private synchronized boolean drawn(double progress, CompactionPolicy policy) {
    if (Double.isNaN(compactAt) || progress < progressSeen) {
      compactAt = 1.0 + policy.draw();
      compactionsLost = 0;
    }
    progressSeen = progress;
    return progress >= compactAt;
  }

  /**
   * After this runtime's writer tried a compaction at {@code progress}: its next point is the
   * progress then plus a draw in [0, 1), doubled for each compaction in a row that did not land
   * (up to 2^10). A compaction lands only if no append lands while it is encoded and sent; one that
   * loses says the log is busier than that, and its writer waits for the log to grow by twice as
   * much before it tries again, so that writers past the limits of a log they keep busy try ever
   * more seldom (and a store's cap on writes to one object makes the commit that meets it replace
   * the log with its record).
   */
  private synchronized void tried(double progress, boolean landed, CompactionPolicy policy) {
    compactionsLost = landed ? 0 : compactionsLost + 1;
    compactAt = progress + policy.draw() * (1 << Math.min(compactionsLost, 10));
  }

  private static final System.Logger LOG = System.getLogger(LogRuntime.class.getName());

  // ============================================================
  // Replay
  // ============================================================

  /** A replay of a view: the builder before it is built, so a commit can verify on it. */
  private final class Replayed {
    // The whole view replayed, or null if the records replayed followed a replica's
    private final LogView view;
    private final LogTail tail;
    private final Replay.Result<B> result;
    private final UUID logId;
    // The size of the checkpoint the replayed records follow
    private final long checkpointBytes;
    private Replica<S> replica;

    Replayed(LogView view, Replay.Result<B> result, UUID logId) {
      this(view, view.tail(), view.checkpoint().map(c -> (long) c.length).orElse(0L), result,
          logId);
    }

    Replayed(
        LogView view, LogTail tail, long checkpointBytes, Replay.Result<B> result, UUID logId) {
      this.view = view;
      this.tail = tail;
      this.checkpointBytes = checkpointBytes;
      this.result = result;
      this.logId = logId;
    }

    B builder() {
      return result.builder();
    }

    /** The log's id, from the replayed checkpoint, else the one given. */
    UUID logId() {
      return result.logId() != null ? result.logId() : logId;
    }

    Replica<S> replica() {
      if (replica == null) {
        replica =
            new Replica<>(
                result.builder().build(),
                result.committed().build(),
                tail,
                result.intentionRecords(),
                result.logId() != null ? result.logId() : logId,
                checkpointBytes,
                view);
      }
      return replica;
    }
  }

  private Replayed replay(LogView view, UUID watch) {
    return replay(view, watch, null);
  }

  private Replayed replay(LogView view, UUID watch, UUID logId) {
    Replayed whole = replayWhole(view, watch, logId);
    CommitTrace.event(CommitTrace.Kind.REPLAYED, 0, view.records().size());
    return whole;
  }

  private Replayed replayWhole(LogView view, UUID watch, UUID logId) {
    checkJoint(view);
    return new Replayed(
        view, Replay.replay(machine, view.checkpoint().orElse(null), view.records(), watch), logId);
  }

  /**
   * The replay of the whole view {@code view}: if it continues, byte for byte, the view {@code
   * prior} is the replay of (the same checkpoint, then the same records and more), the records
   * after those folded onto {@code prior}; else replayed from its checkpoint. The same result
   * either way, since replay is a deterministic fold of the bytes; but a read that finds a few
   * records appended to the log it last read replays only those, not the checkpoint and every
   * record after it again. A record of {@code watch} among those {@code prior} replayed would
   * leave the fold unable to say what replay did with it, so then the view is replayed whole.
   */
  private Replayed replay(LogView view, UUID watch, UUID logId, Replica<S> prior) {
    checkJoint(view);
    int from = prior == null ? -1 : continued(prior.view(), view, watch);
    if (from < 0) {
      return replay(view, watch, logId);
    }
    List<byte[]> records = view.records();
    Replay.Result<B> folded =
        Replay.replayOnto(machine, machine.toBuilder(prior.state()), prior.committed().toBuilder(),
            prior.logId(), prior.intentionRecords(), records.subList(from, records.size()), watch);
    Replayed result = new Replayed(view, folded, logId);
    CommitTrace.event(CommitTrace.Kind.REPLAYED, 1, records.size() - from);
    if (CHECK) {
      check(result, replayWhole(view, watch, logId));
    }
    return result;
  }

  /**
   * The number of records of {@code view} that {@code before} holds, if {@code view} continues it
   * byte for byte and none of them is a record of {@code watch}; else -1.
   */
  private static int continued(LogView before, LogView view, UUID watch) {
    if (before == null) {
      return -1;
    }
    byte[] checkpoint = before.checkpoint().orElse(null);
    if (!Arrays.equals(checkpoint, view.checkpoint().orElse(null))) {
      return -1;
    }
    List<byte[]> held = before.records();
    List<byte[]> records = view.records();
    if (records.size() < held.size()) {
      return -1;
    }
    for (int i = 0; i < held.size(); i++) {
      byte[] record = held.get(i);
      if (!Arrays.equals(record, records.get(i))) {
        return -1;
      }
      if (watch != null) {
        LogEnvelope.Record decoded = LogEnvelope.decodeRecord(record);
        if (decoded.isIntention() && decoded.id().equals(watch)) {
          return -1;
        }
      }
    }
    return held.size();
  }

  /** Fails unless a fold replayed what the replay of the whole view did. */
  private void check(Replayed folded, Replayed whole) {
    Replica<S> a = folded.replica();
    Replica<S> b = whole.replica();
    List<String> differ = new ArrayList<>();
    if (!Arrays.equals(machine.encodeCheckpoint(a.state()), machine.encodeCheckpoint(b.state()))) {
      differ.add("state");
    }
    if (!Arrays.equals(a.committed().bytes(), b.committed().bytes())) {
      differ.add("committed set");
    }
    if (a.intentionRecords() != b.intentionRecords()) {
      differ.add("records");
    }
    if (folded.result.outcome() != whole.result.outcome()
        || folded.result.seen() != whole.result.seen()) {
      differ.add("outcome");
    }
    if (!Objects.equals(a.logId(), b.logId())) {
      differ.add("log id");
    }
    if (!differ.isEmpty()) {
      throw new IllegalStateException(
          "A fold onto a replica of " + log + " differs from a replay of the whole view: "
              + String.join(", ", differ));
    }
  }

  /** A write whose outcome is unknown: its reply was lost, or reading back what it wrote failed. */
  private static final class AmbiguousWrite extends RuntimeException {
    AmbiguousWrite(Throwable cause) {
      super(cause);
    }
  }

  /** A read of the log that failed; {@link #failure} is what it threw. */
  private static final class UnreadableLog extends RuntimeException {
    UnreadableLog(RuntimeException cause) {
      super(cause);
    }

    RuntimeException failure() {
      return (RuntimeException) getCause();
    }
  }

  /**
   * A replace's payload: the checkpoint and the records after it, the pages its GC record drops,
   * and whether its PageOuts seal any.
   */
  private static final class Prepared {
    final byte[] checkpoint;
    final List<byte[]> records;
    final List<PageRef> dropped;
    final boolean sealed;
    // The state the checkpoint holds: once it lands, the pages its records dropped are garbage
    final Object state;

    Prepared(
        byte[] checkpoint, List<byte[]> records, List<PageRef> dropped, boolean sealed,
        Object state) {
      this.checkpoint = checkpoint;
      this.records = records;
      this.dropped = dropped;
      this.sealed = sealed;
      this.state = state;
    }
  }

  /**
   * What a replace of the log at {@code on} writes: the checkpoint of {@code on}, then a GC record,
   * the PageOuts, and {@code record}, a commit's (none for a compaction after a commit). GC of the
   * committed set is a record, never a filter on the checkpoint, so every checkpoint is a replay of
   * the log before it (I11). The watermark is now less the retention, clamped to 0 (a retention of
   * {@code Long.MAX_VALUE} disables GC, and no record is written), and at most {@code ownMs}, below
   * the commit's own time: the horizon must not reach the record that follows, or replay rejects it
   * (Bug 64). A short retention would otherwise drop the id of a commit this one rebased on, minted
   * after it. The PageOuts seal the oldest blocks of the set after that GC, as the policy says
   * ({@link #page}), with tops at most {@code ownMs} too, so the floor stays below the record; their
   * pages are put here. A compaction, with no record after it, passes {@code Long.MAX_VALUE}.
   */
  private Prepared prepare(Replica<S> on, byte[] record, long ownMs) {
    long now = config.clockMs().getAsLong();
    long watermark = watermark(now, ownMs);
    List<byte[]> records = new ArrayList<>(3);
    CommittedSet set = on.committed();
    if (watermark > 0L) {
      records.add(LogEnvelope.encodeAdvanceHorizon(watermark));
    }
    List<PageRef> dropped = List.of();
    List<CommittedSet.Seal> seals = List.of();
    if (log.pages() != null) {
      if (watermark > 0L) {
        set = set.toBuilder().advanceHorizon(watermark).build();
      }
      dropped = dropped(on.committed().pages(), set);
      seals = page(set, pageCeiling(now, ownMs), on.logId());
      for (CommittedSet.Seal seal : seals) {
        records.add(LogEnvelope.encodePageOut(seal.record()));
      }
    }
    if (record != null) {
      records.add(record);
    }
    return new Prepared(checkpoint(on), records, dropped, !seals.isEmpty(), on.state());
  }

  private final class Commit {
    private final Replica<S> base;
    private final I intention;
    private final UUID id;
    private final byte[] record;
    private final Capabilities caps = log.capabilities();

    // The replica of `tail`, unless stale: after a lost append race, the tail is refreshed
    // without reading the log.
    private Replica<S> current;
    private LogTail tail;
    private boolean stale = false;
    // The last write whose outcome is unknown, if any: its reply was lost, or reading back what
    // it wrote failed. It may still be in flight, so once one has happened, running out of
    // attempts no longer means the commit failed.
    private AmbiguousWrite ambiguous = null;
    // The last write that failed its condition, until a read of the log rules out that it landed.
    // A transport retry of a write that landed fails its condition, so the failure is no proof;
    // but the write landed before the reply, so a read without it settles it.
    private RuntimeException unresolved = null;
    // Whether storage refused an append because the log holds as many writes as it allows (S3
    // Express: 10,000 parts; an Azure Append Blob: 50,000 blocks): only a replace can commit
    // then, whatever the policy says.
    private boolean full = false;
    // The races this commit lost (writes that failed their condition): each doubles the bound of
    // the wait before it reads or probes the log again (raceBackoff).
    private int lostRaces = 0;
    // Whether the record is larger than one append stores (an Azure AppendBlock: 4 MiB before
    // service version 2022-11-02, 100 MiB since): only a replace can commit it. An append of it
    // fails without landing, which the commit took for ambiguous on every attempt (Bug 71).
    private final boolean oversized;
    // A send that the log throttled before the one it acknowledged, if the send was unconditional:
    // it may have landed too, so the record may be in the log twice (on one object, the second
    // send fails its condition instead, P4).
    private Throwable duplicate = null;
    // On a log that cannot replace itself, the tail no read of this commit may fall behind: the
    // read it staged on, or a later one it adopted. On a read that lags behind it, the intention
    // may not hold yet while its record has applied.
    private LogTail floor;

    Commit(Replica<S> base, I intention) {
      this.base = base;
      this.intention = intention;
      // After the floor of the log this writer read, at or below which replay rejects a record
      // (Bug 64), on the runtime's clock (a test sets it to draw stamps).
      this.id = UuidV7.newUuidV7After(base.committed().floor(), config.clockMs().getAsLong());
      this.record = LogEnvelope.encodeRecord(id, machine.encodeIntention(intention));
      this.oversized = caps.maxRecordBytes() > 0 && record.length > caps.maxRecordBytes();
      this.current = base;
      this.tail = base.tail();
      this.floor = !caps.fusedReplace() && tail.exists() ? tail : null;
    }

    Replica<S> run() {
      CommitTrace.event(CommitTrace.Kind.COMMIT, base.intentionRecords(), record.length);
      try {
        Replica<S> landed = attempts();
        CommitTrace.event(CommitTrace.Kind.END, 0, lostRaces);
        return landed;
      } catch (CommitRejectedException e) {
        CommitTrace.event(CommitTrace.Kind.END,
            e.reason() == CommitRejectedException.Reason.EXHAUSTED ? 3 : 1, lostRaces);
        throw e;
      } catch (CommitOutcomeUnknownException e) {
        CommitTrace.event(CommitTrace.Kind.END, 2, lostRaces);
        throw e;
      } catch (RuntimeException e) {
        CommitTrace.event(CommitTrace.Kind.END, 4, lostRaces);
        throw e;
      }
    }

    private Replica<S> attempts() {
      try {
        return loop();
      } catch (LogNotSentException e) {
        // A write failed before anything was sent. The commit failed, unless an earlier write of
        // it may have landed.
        Throwable earlier = earlier();
        if (earlier != null) {
          earlier.addSuppressed(e);
          throw new CommitOutcomeUnknownException(earlier);
        }
        throw new CommitRejectedException(
            CommitRejectedException.Reason.NOT_SENT, null,
            "Cannot commit: a write failed before it was sent: " + e.getMessage(), e);
      }
    }

    private Replica<S> loop() {
      if (oversized && !caps.fusedReplace()) {
        throw new CommitRejectedException(
            CommitRejectedException.Reason.TOO_LARGE, null,
            String.format("Cannot commit: a record of %d bytes is larger than %s stores (%d)",
                record.length, log, caps.maxRecordBytes()),
            null);
      }

      // Case 0: the log does not exist yet. A log that replaces itself is created by a replace of
      // the absent log; one that keeps checkpoints apart, by its first checkpoint.
      if (!tail.exists() && !caps.fusedReplace()) {
        create();
      } else if (!tail.exists()) {
        Prepared creation = prepare(base);
        try {
          return validate(
              write(() -> log.replace(tail, creation.checkpoint, creation.records), false));
        } catch (AmbiguousWrite | LogConflictException e) {
          // The creation may have landed: its reply was lost, reading it back failed, or it failed
          // its condition, as a transport retry of a creation that landed does.
          Throwable failure = e instanceof AmbiguousWrite ? e.getCause() : e;
          Replayed fresh = resolve(failure);
          if (fresh != null && landed(fresh, failure)) {
            return fresh.replica(); // the creation landed
          }
          if (fresh != null && !(e instanceof AmbiguousWrite)) {
            throw new CommitRejectedException(
                CommitRejectedException.Reason.CREATE_LOST, fresh.replica(),
                "Cannot commit: another writer created the log first", e);
          }
          throw new CommitOutcomeUnknownException(failure);
        }
      }

      for (int attempt = 0; attempt < config.maxAttempts(); attempt++) {
        try {
          // Replace (compact) or append, by what the log can do and what the policy says of its
          // observable state. A log without append replaces on every commit, and a record larger
          // than an append stores goes in a replace. Past the policy's limits a commit replaces
          // only if compaction is fused with it; else it appends, and its writer may compact the
          // log after (compactAfter), unless the policy is due even on a log with no records (CAS
          // only, or a record larger than the size limit).
          boolean mustReplace =
              caps.fusedReplace()
                  && (full
                      || oversized
                      || !caps.append()
                      || config.compaction().due(config.compactAfterCommit()
                          ? new CompactionPolicy.Input(0, current.checkpointBytes(),
                              current.checkpointBytes(), record.length)
                          : new CompactionPolicy.Input(current.intentionRecords(),
                              current.checkpointBytes(), bytes(tail), record.length)));
          CommitTrace.event(CommitTrace.Kind.ATTEMPT, attempt, mustReplace ? 1 : 0);

          if (mustReplace) {
            if (stale) {
              // The replace supersedes the log at its tail, so its checkpoint must hold the state
              // there: a checkpoint of an older read would erase every record committed since.
              // Our record follows the checkpoint and replay verifies it against that state, as it
              // would an append. Read afresh: storage fails a read pinned to a version another
              // writer has since replaced.
              Replayed fresh = read();
              if (landed(fresh, earlier())) {
                return fresh.replica(); // an append reported as a lost race landed
              }
              unresolved = null;
              adopt(fresh);
            }
            full = false;
            // The payload, its pages put: before the write, whose failures leave it ambiguous
            Prepared replace = prepare(current);
            CommitTrace.event(CommitTrace.Kind.PREPARED, replace.checkpoint.length, 0);
            try {
              LogTail at = tail;
              CommitTrace.event(CommitTrace.Kind.SEND, 1, 0);
              Replayed written =
                  write(() -> log.replace(at, replace.checkpoint, replace.records), false);
              tidyPages(replace.dropped, replace.sealed, written.result.committed().build());
              @SuppressWarnings("unchecked")
              S checkpointed = (S) replace.state;
              deleteDroppedStatePages(checkpointed);
              return validate(written);
            } catch (LogConflictException e) {
              CommitTrace.event(CommitTrace.Kind.LOST, 0, 0);
              unresolved = e;
              raceBackoff(e);
            }
            // Replace conflict: another writer changed the log since we read it. Rebase onto the
            // new state. Replay checks this intention's preconditions against the state its
            // record lands on, so if they hold there, retry on top of it -- by append or replace,
            // as that state calls for -- and the commit is accepted exactly when a serial commit
            // would be. If not, fail without writing; the client refreshes and re-applies.
            Replayed fresh = read();
            if (landed(fresh, earlier())) {
              return fresh.replica(); // an earlier attempt landed
            }
            unresolved = null;
            boolean holds = intention.verify(fresh.builder());
            String why = holds ? null : Replay.explain(intention, fresh.builder());
            adopt(fresh);
            if (!holds) {
              throw conflict(fresh, "Cannot commit: the log changed concurrently", why);
            }
          } else {
            try {
              LogTail at = tail;
              boolean conditional = caps.conditionalAppend();
              CommitTrace.event(CommitTrace.Kind.SEND, 0, 0);
              return validate(
                  write(() -> log.append(record, conditional ? at : null), !conditional));
            } catch (LogFullException e) {
              CommitTrace.event(CommitTrace.Kind.LOST, 1, 0);
              // Nothing landed, and no append to this log can: replace it, on a fresh read.
              // Without this the commit took the append for ambiguous, and with a count above the
              // store's limit no writer ever compacted (Bug 66).
              full = true;
              stale = true;
              continue;
            } catch (LogConflictException e) {
              CommitTrace.event(CommitTrace.Kind.LOST, 0, 0);
              unresolved = e;
              raceBackoff(e);
            }
            // Another writer appended first. With tail reads, on a log that replaces itself (one
            // object), read what followed the replica this commit holds (the records that beat
            // it, or the whole log if it was replaced since), and append again at that tail,
            // knowing what the log holds there: a read through the append needs no read
            // (docs/TAIL_READS.md). Such a read is of the object as it is, so a record of this
            // commit's that landed is in it. A log whose reads may lag its appends could hide
            // one: there the resend's read through it settles it (P3), as below.
            if (config.tailReads() && caps.fusedReplace()) {
              Replayed fresh = read();
              if (landed(fresh, earlier())) {
                return fresh.replica(); // an append reported as a lost race landed
              }
              unresolved = null;
              adopt(fresh);
              continue;
            }
            // Without, the record is the same and replay applies it once, by its id: the
            // committed set holds it, or it is at or below the floor, where replay admits
            // nothing. A replace that made the log longer reads as growth, and it may have taken
            // the id out of the set (GC, or paging: docs/COMMITTED_PAGING.md), so the floor is
            // what rejects such a copy. Append it again at the new tail without reading the log:
            // only the tail is needed.
            CommitTrace.event(CommitTrace.Kind.PROBE);
            LogTail probed = probe();
            boolean continues = probed.continues(tail);
            CommitTrace.event(CommitTrace.Kind.PROBED, continues ? 1 : 0, 0);
            if (!continues) {
              // The log was replaced (compacted). Read it, so that the next decision sees its
              // record count; afresh, as above, since the version may be replaced by now.
              Replayed fresh = read();
              if (landed(fresh, earlier())) {
                return fresh.replica(); // an append reported as a lost race landed
              }
              unresolved = null;
              adopt(fresh);
            } else {
              // The log grew (a concurrent append). Resend the same record at the new tail; the
              // record count is now a lower bound, which is fine for a soft limit. No read is
              // needed unless the next attempt replaces.
              tail = probed;
              stale = true;
            }
          }
        } catch (AmbiguousWrite e) {
          CommitTrace.event(CommitTrace.Kind.AMBIGUOUS);
          // The write may have landed: its reply was lost, or reading back what it wrote failed.
          // Its record carries this intention's id, so a fresh read says which. If it has not
          // landed, retry from the fresh state: a write still in flight carries the same record
          // (and, if conditional, a condition on the tail it saw), so replay applies the intention
          // once. A rejection stays a rejection: replay verifies a record against the state it
          // lands on, and once it fails there it fails at every later point (S2), so a late
          // landing of an intention that does not hold changes nothing.
          ambiguous = e;
          Replayed fresh = resolve(e.getCause());
          if (fresh == null) {
            throw new CommitOutcomeUnknownException(e.getCause());
          }
          if (landed(fresh, e.getCause())) {
            return fresh.replica(); // the write landed
          }
          unresolved = null;
          boolean holds = intention.verify(fresh.builder());
          String why = holds ? null : Replay.explain(intention, fresh.builder());
          adopt(fresh);
          if (!holds) {
            throw conflict(fresh, "Cannot commit: the log changed concurrently", why);
          }
        } catch (UnreadableLog e) {
          // A read failed. The commit failed too, unless one of its writes may have landed.
          Throwable write = earlier();
          if (write == null) {
            throw e.failure();
          }
          write.addSuppressed(e.failure());
          throw new CommitOutcomeUnknownException(write);
        }
      }

      if (ambiguous != null) {
        throw new CommitOutcomeUnknownException(ambiguous.getCause());
      }
      if (unresolved != null) {
        // The last attempt lost an append race, which its own landing would also report.
        Replayed fresh = resolve(unresolved);
        if (fresh == null) {
          throw new CommitOutcomeUnknownException(unresolved);
        }
        if (landed(fresh, unresolved)) {
          return fresh.replica();
        }
      }
      throw new CommitRejectedException(
          CommitRejectedException.Reason.EXHAUSTED, null, "Exceeded maximum commit attempts", null);
    }

    /**
     * Waits after a lost race, before the commit reads or probes the log again: a time drawn
     * uniformly below a bound that starts at {@code raceMinWaitMs} and doubles with each race the
     * commit lost, up to the larger of 1 s and that (full jitter). The writers who lost to one
     * winner would otherwise all read and resend at once, and on a log that only replaces itself
     * most of them lose again: each loss costs a read of the whole log. An interrupt stops the
     * commit, its outcome unknown (the write that lost may have landed).
     */
    private void raceBackoff(RuntimeException lost) {
      lostRaces += 1;
      long base = config.raceMinWaitMs();
      if (base <= 0L) {
        return;
      }
      long bound = Math.min(base << Math.min(lostRaces - 1, 20), Math.max(1_000L, base));
      long wait = ThreadLocalRandom.current().nextLong(bound + 1);
      CommitTrace.event(CommitTrace.Kind.BACKOFF, wait, 0);
      sleep(wait, lost);
    }

    private void adopt(Replayed fresh) {
      current = fresh.replica();
      CommitTrace.event(CommitTrace.Kind.ADOPTED);
      tail = current.tail();
      stale = false;
      if (!caps.fusedReplace()) {
        floor = tail;
      }
    }

    /** The last write of this commit, but one acknowledged, that may have landed, or null. */
    private Throwable earlier() {
      return ambiguous != null ? ambiguous.getCause() : unresolved != null ? unresolved : duplicate;
    }

    /**
     * Creates a log that keeps its checkpoints apart: stores the base's checkpoint (the empty
     * state, with the new log's id) as the log's first, and adopts a read of the log created.
     * Nothing of the intention has been sent, so a create that another writer beat, or whose
     * outcome no read settles, rejects the commit.
     */
    private void create() {
      RuntimeException failure = null;
      try {
        log.putCheckpoint(tail, checkpoint(base, tail));
      } catch (RuntimeException e) {
        failure = e; // another writer's create, or one of ours that may have landed
      }
      Replayed fresh;
      try {
        fresh = replay(log.read(), id, base.logId());
      } catch (RuntimeException e) {
        if (failure != null) {
          e.addSuppressed(failure);
        }
        throw new CommitRejectedException(CommitRejectedException.Reason.NOT_SENT, null,
            "Cannot commit: creating the log failed, and reading it failed too: " + e, e);
      }
      if (!fresh.replica().logId().equals(base.logId())) {
        throw new CommitRejectedException(CommitRejectedException.Reason.CREATE_LOST,
            fresh.replica(), "Cannot commit: another writer created the log first", failure);
      }
      adopt(fresh);
    }

    /** What a replace of the log at {@code on} with this commit's record writes ({@link #prepare}). */
    private Prepared prepare(Replica<S> on) {
      return LogRuntime.this.prepare(on, record, UuidV7.timestampMs(id) - 1);
    }

    /**
     * Sends a write and reads back what it wrote, again after a backoff while storage applies
     * backpressure ({@link LogThrottledException}). Each retry carries the same condition, so at
     * most one attempt lands. A throttled write may have landed even so: a 5xx can follow a write
     * the service applied, and an SDK's own retries report only the last attempt. A retry of a
     * write that landed fails its condition, which the commit loop resolves by reading the log;
     * running out of retries leaves the outcome unknown in the same way, so it is an {@link
     * AmbiguousWrite}. An interrupt during the backoff stops the commit with its outcome unknown.
     * The protocol's conflict and full signals propagate; any other failure of the write, or of
     * reading back what it wrote, is an {@link AmbiguousWrite}.
     */
    private Replayed write(Supplier<Receipt> send, boolean unconditional) {
      long backoffMs = config.throttleMinWaitMs();
      LogThrottledException lastThrottle = null;
      for (int attempt = 0; attempt < config.maxThrottleAttempts(); attempt++) {
        Receipt receipt;
        try {
          receipt = send.get();
        } catch (LogThrottledException e) {
          lastThrottle = e;
          sleep(backoffMs + ThreadLocalRandom.current().nextLong(backoffMs / 2 + 1), e);
          backoffMs = Math.min(backoffMs * 2, Math.max(5_000L, config.throttleMinWaitMs()));
          continue;
        } catch (LogNotSentException e) {
          // This attempt failed before writing; an earlier, throttled one may have landed.
          if (lastThrottle == null) {
            throw e;
          }
          e.addSuppressed(lastThrottle);
          throw new AmbiguousWrite(e);
        } catch (LogConflictException | LogFullException e) {
          throw e;
        } catch (RuntimeException e) {
          throw new AmbiguousWrite(e);
        }
        CommitTrace.event(CommitTrace.Kind.LANDED);
        if (unconditional && lastThrottle != null) {
          // The throttled send may have landed beside this one, and a view through this one may
          // start from a checkpoint that holds the other (dup in SharedLogCommit.tla).
          duplicate = lastThrottle;
        }
        Replayed written;
        try {
          written =
              replay(log.readThrough(receipt, config.visibilityWait()), id, base.logId(), current);
        } catch (RuntimeException e) {
          throw new AmbiguousWrite(e);
        }
        if (!written.result.seen()) {
          // Only a view whose records include this one can settle it (P3): a checkpoint that holds
          // it cannot say whether it applied once GC drops its id.
          throw new AmbiguousWrite(new IllegalStateException(
              "A read through a write to " + log + " does not include its record"));
        }
        return written;
      }
      throw new AmbiguousWrite(
          new IOException(
              String.format("Storage throttled %d attempts", config.maxThrottleAttempts()),
              lastThrottle));
    }

    /**
     * A fresh read; any failure becomes {@link UnreadableLog}. With tail reads, a read of what
     * follows the replica the commit holds ({@link #current}), which replays only the records
     * after it, or the whole log if it no longer continues it (docs/TAIL_READS.md). On a log that
     * cannot replace itself, a read that lags behind {@link #floor} is read again, waiting up to
     * the visibility wait in all, and then fails ({@link LogLagException}).
     */
    private Replayed read() {
      try {
        boolean after = config.tailReads() && current != null;
        CommitTrace.event(CommitTrace.Kind.READ, after ? 1 : 0, 1);
        LogView view = viewed(upToFloor(after ? this::fetch : log::read));
        if (after) {
          Replayed folded = foldAfter(view, current, id, base.logId());
          if (folded != null) {
            return folded;
          }
          // records alone it cannot fold: read whole, as below
          view = viewed(upToFloor(log::read));
        }
        return replay(view, id, base.logId(), current);
      } catch (CommitOutcomeUnknownException e) {
        throw new UnreadableLog((RuntimeException) e.getCause()); // interrupted waiting
      } catch (RuntimeException e) {
        throw new UnreadableLog(e);
      }
    }

    /**
     * A view {@code reader} reads, read again while it lags behind {@link #floor} (a log that
     * cannot replace itself), waiting up to the visibility wait in all ({@link LogLagException}
     * after): a read behind the replica the commit staged on could find the intention not holding
     * yet, and reject a commit whose copy has landed ([lagread]).
     */
    private LogView upToFloor(Supplier<LogView> reader) {
      LogView view = reader.get();
      for (int attempt = 1; floor != null && !view.tail().continues(floor); attempt++) {
        if (attempt >= config.maxThrottleAttempts()) {
          throw new LogLagException("A read of " + log + " lags behind one this commit made");
        }
        sleep(config.visibilityWait().toMillis() / config.maxThrottleAttempts(),
            new LogLagException("Interrupted waiting for " + log));
        view = reader.get();
      }
      return view;
    }

    /** A read of the log: of what follows {@link #current}, with tail reads; else whole. */
    private LogView fetch() {
      if (config.tailReads() && current != null) {
        try {
          return log.readAfter(current.tail());
        } catch (TrimmedException e) {
          return log.read();
        }
      }
      return log.read();
    }

    /** The log's tail; a failure to probe it becomes {@link UnreadableLog}. */
    private LogTail probe() {
      try {
        return log.probe();
      } catch (RuntimeException e) {
        throw new UnreadableLog(e);
      }
    }

    /** The size of the log at {@code at}; a failure to learn it becomes {@link UnreadableLog}. */
    private long bytes(LogTail at) {
      try {
        return at.bytes();
      } catch (RuntimeException e) {
        throw new UnreadableLog(e);
      }
    }

    /**
     * Reads the log after a write that may have landed, or returns null if that fails too (the
     * read failure is attached to {@code writeFailure}).
     */
    private Replayed resolve(Throwable writeFailure) {
      try {
        return read();
      } catch (UnreadableLog readFailure) {
        writeFailure.addSuppressed(readFailure.failure());
        return null;
      }
    }

    /**
     * Whether {@code fresh} holds this commit's record. Replay's judgment of the record where it
     * landed comes first: on a log with checkpoints apart from its records, another writer's GC
     * record can land between this record and the read, and drop the id of a record that applied.
     * Else the committed set. If the id is at or below its floor, replay rejects the record from
     * now on, so the answer is final: the page that holds the ids there says whether it applied
     * before the floor passed it (COMMITTED, or the commit is rejected, {@code BEYOND_FLOOR}). If
     * it is at or below the horizon, the set cannot say (Bug 64): the commit failed, unless {@code
     * earlier}, a write of it that may have landed, did, before its id aged out.
     *
     * <p>A page that cannot be read is a failed read, never "absent": the outcome is unknown if
     * {@code earlier} may have landed, else this throws {@link UnreadableLog}. A page GC has
     * dropped since {@code fresh} was read sends the lookup to a fresh read.
     */
    private boolean landed(Replayed fresh, Throwable earlier) {
      return landed(fresh, earlier, 0);
    }

    private boolean landed(Replayed fresh, Throwable earlier, int rereads) {
      switch (fresh.result.outcome()) {
        case APPLIED:
          return true;
        case REJECTED:
          return false;
        default:
          break;
      }
      CommittedSet.Builder set = fresh.result.committed();
      try {
        if (set.contains(id)) {
          return true;
        }
      } catch (PagedOutException e) {
        return paged(fresh, e.page(), earlier, rereads);
      } catch (DedupHorizonExceededException e) {
        if (earlier != null) {
          earlier.addSuppressed(e);
          throw new CommitOutcomeUnknownException(earlier);
        }
        throw new CommitRejectedException(
            CommitRejectedException.Reason.BEYOND_HORIZON, null,
            String.format(
                "Cannot commit: transaction %s is at or below the log's committed-set horizon",
                id),
            e);
      }
      if (UuidV7.timestampMs(id) <= set.floor()) {
        // In (H, F] where no page can hold it: nothing applied there, and nothing can
        throw beyondFloor();
      }
      return false;
    }

    /** {@link #landed} for an id that the page {@code page} holds if its record applied. */
    private boolean paged(Replayed fresh, PageRef page, Throwable earlier, int rereads) {
      boolean holds;
      try {
        holds = pageHolds(page, fresh.logId(), id);
      } catch (MissingPage missing) {
        // GC dropped the page and its compactor deleted it since fresh was read: read again. The
        // id is then at or below the horizon, or where no page can hold it (TLA+: missingfail)
        if (rereads >= MAX_PAGE_REREADS) {
          return unreadablePage(missing, earlier);
        }
        Replayed again;
        try {
          again = read();
        } catch (UnreadableLog e) {
          if (earlier != null) {
            earlier.addSuppressed(e.failure());
            throw new CommitOutcomeUnknownException(earlier);
          }
          throw e;
        }
        return landed(again, earlier, rereads + 1);
      } catch (RuntimeException e) {
        return unreadablePage(e, earlier);
      }
      if (holds) {
        return true; // applied before its id was paged: this is a copy, or a late lookup
      }
      throw beyondFloor();
    }

    /** A page that could not be read: unknown if {@code earlier} may have landed, else a failed read. */
    private boolean unreadablePage(RuntimeException failure, Throwable earlier) {
      if (earlier != null) {
        earlier.addSuppressed(failure);
        throw new CommitOutcomeUnknownException(earlier);
      }
      throw new UnreadableLog(failure);
    }

    private CommitRejectedException beyondFloor() {
      return new CommitRejectedException(
          CommitRejectedException.Reason.BEYOND_FLOOR, null,
          String.format("Cannot commit: transaction %s is at or below the log's committed-set"
              + " floor, and its record did not apply before the floor passed it", id),
          null);
    }

    /**
     * The replica of {@code result}, a read through a write of this commit, if it holds this
     * commit's record; else a conflict. A page the lookup needs that cannot be read leaves the
     * write ambiguous.
     */
    private Replica<S> validate(Replayed result) {
      boolean landed;
      try {
        landed = landed(result, earlier());
      } catch (UnreadableLog e) {
        throw new AmbiguousWrite(e.failure());
      }
      if (!landed) {
        // The record landed, but replay rejected it: a concurrent commit changed what it depends on.
        throw conflict(result, "Cannot commit: a concurrent commit invalidated this one",
            result.result.rejection());
      }
      return result.replica();
    }

    private CommitRejectedException conflict(Replayed on, String message, String rejection) {
      return new CommitRejectedException(
          CommitRejectedException.Reason.CONFLICT, on.replica(), message, rejection, null);
    }
  }
}
