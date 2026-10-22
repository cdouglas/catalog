--------------------------- MODULE CatalogCommit ---------------------------
(***************************************************************************)
(* The FileIOCatalog commit protocol over one catalog object in storage    *)
(* with conditional writes: LogRuntime's commit loop over an AtomicFileLog *)
(* (ProtoCatalogFormat.Mut.commit as of fileio-catalog 4a44e16). The       *)
(* catalog's state and replay are RSM's: entity versions (namespaces and   *)
(* tables alike) and the entities that creates add.                        *)
(*                                                                         *)
(* Each writer action is one storage request of the commit loop, so other  *)
(* writers interleave between any two; the comments name the code branch.  *)
(* The properties below are stated over RSM's ghost history, `hist`.       *)
(*                                                                         *)
(* Bug switches put back protocol bugs the fuzzers and reviews found       *)
(* (Bug N in fileio-catalog's commit subjects), to calibrate the           *)
(* properties and the oracles (CatalogOracles.tla). Sched restricts the    *)
(* interleavings to those a fuzzer's schedules produce, for the oracles.   *)
(***************************************************************************)
EXTENDS RSM, TLC

CONSTANTS
  Writers,          \* writer ids
  Shapes,           \* Writers -> Seq of [rd, wr: SUBSET Entities, cr: Entities \cup {None},
                    \*   retry: BOOLEAN]: an operation (retry: FileIOCatalog restages it
                    \*   on a fresh read after CommitFailedException) or a transaction
  AppendSupported,  \* FileIO.supportsAppend() (FALSE: GCS)
  MaxCount,         \* fileio.catalog.max.append.count (0: CAS only)
  MaxSize,          \* fileio.catalog.max.append.size, in records (the checkpoint counts 1)
  SegLimit,         \* the store's cap on writes to one object (0: none)
  MaxAttempts,      \* commit loop attempts (code: 10)
  MaxThrottle,      \* attempts of a write storage throttles (code: 8)
  Retention,        \* fileio.catalog.committed.retention.ms, in ticks (-1: GC off)
  MaxTime,          \* bound on the clock
  Skew,             \* Writers -> Nat: each writer's clock is now + Skew[w]
  MaxFaults,        \* how many faults storage may inject in a run
  FaultKinds,       \* the faults it may inject (see Write and the reads)
  ClientRetries,    \* how many times an operation is restaged (Iceberg: 4)
  Strict,           \* a read pinned to a replaced version fails (GCS, S3/ADLS If-Match)
  Sched             \* "free": any interleaving. A fuzzer's schedules, where operations
                    \* run one at a time and others may run to completion inside one:
                    \* "mw" (TestMultiWriterProperties: one, before any request, on a
                    \* snapshot taken earlier), "fault" (TestCatalogFaultProperties:
                    \* one, before the first write), "race" (TestCatalogRaceProperties:
                    \* writer 1 runs A, and each other writer runs fresh just before
                    \* one of A's writes, in turn, or after A)
                    \* Bugs (RSM): protocol bugs put back, by number

WriteFaults == {"failBefore", "lostReply", "landedConflict", "throttled",
                "landedThrottled", "inflight"}
ReadFaults == {"readFail"}

ASSUME /\ FaultKinds \subseteq WriteFaults \cup ReadFaults
       /\ MaxAttempts >= 1 /\ MaxThrottle >= 1
       /\ Sched \in {"free", "mw", "fault", "race"}
       /\ Bugs \subseteq {"10", "12", "16", "17", "18", "25", "26",
                          "64a", "64b", "64c", "64d", "66", "ckgc"}

VARIABLES
  file,      \* the catalog object: [gen, chk: [st, C, H], log, segs]
  inflight,  \* writes sent, not landed, not yet failed: a timed-out request
  now,       \* the clock
  nf,        \* faults injected so far
  ws,        \* Writers -> the writer's commit (see Idle)
  ended,     \* the outcomes reported, per commit: [id, out, cf, cx, ci]
  res,       \* Writers -> the outcome of the writer's last operation, or None
  ook,       \* every oracle check so far passed (CatalogOracles)
  sched      \* [active, nest: the operations running; pre: how many ran inside the
             \*  active one; racers: who ran inside one]

vars == <<file, inflight, now, nf, ws, ended, res, ook, sched>>

-----------------------------------------------------------------------------
(* The catalog object, and reads of it *)

InitFile == [gen |-> 1, chk |-> [st |-> InitSt, C |-> {}, H |-> 0],
             log |-> << >>, segs |-> 1]

\* A read: the replayed state, what replay did with the record of `watch`,
\* the version read and its length (in records, the checkpoint counting
\* one), and the intention-record count (intentionRecords).
ReplayOf(f, watch) ==
  LET a == Fold(Acc(f.chk.st, f.chk.C, f.chk.H, watch), f.log)
  IN [st |-> a.st, C |-> a.C, H |-> a.H, err |-> a.err, out |-> a.out,
      cnt |-> Len(Txns(f.log)), gen |-> f.gen, len |-> Len(f.log) + 1]

Replay(f) == ReplayOf(f, None)

FileLen(f) == Len(f.log) + 1

LiveR == Replay(file)

\* A commit's read of the live catalog, watching its own record.
LiveOf(s) == ReplayOf(file, s.txn.id)

-----------------------------------------------------------------------------
(* Writes *)

Clock(w) == 1 + now + Skew[w]

\* The file a CAS writes, but its generation: buildFullFile, a checkpoint of
\* `b`, a GC record at the watermark (none when GC is off), then the record.
\* The checkpoint is the replay of the version the CAS replaces, whatever the
\* clock says (I11). With Bug 64b the watermark is not capped below the
\* record's own time. With "ckgc" the checkpoint itself is filtered at the
\* watermark, as it was before GC records: a reader starting from another
\* checkpoint of the same log would not see the same horizon.
CasPayload(w, b, t) ==
  LET c    == Clock(w)
      wm0  == IF Retention < 0 \/ Retention >= c THEN 0 ELSE c - Retention
      wm   == IF Has("64b") THEN wm0 ELSE Max(0, Min(wm0, t.id.ts - 1))
      ckgc == Gc([C |-> b.C, H |-> b.H], wm)
  IN IF Has("ckgc")
       THEN [chk |-> [st |-> b.st, C |-> ckgc.C, H |-> ckgc.H], log |-> << t >>]
       ELSE [chk |-> [st |-> b.st, C |-> b.C, H |-> b.H],
             log |-> IF wm = 0 THEN << t >> ELSE << [wm |-> wm], t >>]

\* Storage applies a write x = [strat, cond, payload] to f.
Land(f, x) ==
  IF x.strat = "CAS"
    THEN [gen |-> f.gen + 1, chk |-> x.payload.chk, log |-> x.payload.log, segs |-> 1]
    ELSE [f EXCEPT !.gen = @ + 1, !.log = Append(@, x.payload), !.segs = @ + 1]

\* The store accepts another write to the object (S3 Express: 10,000 parts;
\* an Azure Append Blob: 50,000 blocks). A CAS replaces it.
SegOK(f, strat) == strat = "CAS" \/ SegLimit = 0 \/ f.segs + 1 <= SegLimit

-----------------------------------------------------------------------------
(* Writers *)

\* An idle writer, about to stage its k-th operation (ids count with n).
Idle(k, n) ==
  [pc |-> "idle", k |-> k, n |-> n, txn |-> None, snap |-> None, base |-> None,
   cur |-> None, stale |-> FALSE, amb |-> FALSE, unres |-> FALSE, full |-> FALSE,
   att |-> 0, thr |-> 0, pend |-> None, written |-> None, rt |-> 0, nw |-> 0,
   cf |-> FALSE, cx |-> FALSE, ci |-> FALSE, cw |-> << >>, first |-> None, ocf |-> FALSE]
\* pc: idle, staged, loop, cas, throttled, readback, rebase, refresh, shrink,
\*   resolve, restage.
\* rt: the times the operation was restaged; nw: its write requests.
\* base: the parsed state of cur (baseCatalog), unless stale (!baseIsCurrent).
\* cur: the version the next write is conditioned on [gen, len] (current).
\* amb: a write's outcome was unknown (ambiguous); unres: a write failed its
\*   precondition and no read has ruled out that it landed (unresolved);
\*   full: storage refused an append past its cap (full).
\* Ghosts: cf, a fault was injected into this commit; cx, a read of it had
\*   its id at or below the horizon; ci, another write landed since its
\*   snapshot; cw, its writes that landed, [before, after]; first, the
\*   catalog before the operation's first write; ocf, a fault was injected
\*   into the operation.

\* The other writers in a commit see the catalog change under them.
Upd(w, s2, landed) ==
  [v \in Writers |-> IF v = w THEN s2
                     ELSE IF landed /\ ws[v].pc # "idle" THEN [ws[v] EXCEPT !.ci = TRUE]
                     ELSE ws[v]]

\* Whether w may take a step: the operation running, or the one running
\* inside it.
CanStep(w) == Sched = "free" \/ w = sched.nest \/ (w = sched.active /\ sched.nest = None)

\* Faults fall on the operation running, not on one inside it (as in the
\* fuzzers).
CanFault(w) == nf < MaxFaults /\ (Sched = "free" \/ w # sched.nest)
WFaultsOf(w) == {"none"} \cup (IF CanFault(w) THEN FaultKinds \cap WriteFaults ELSE {})
RFaultsOf(w) == {"none"} \cup (IF CanFault(w) THEN FaultKinds \cap ReadFaults ELSE {})
UseFault(fk) == nf' = IF fk = "none" THEN nf ELSE nf + 1

Crossed(R, t) == R.H > 0 /\ t.id.ts <= R.H

\* s after a request fk: a fault, if fk is one.
Faulted(s, fk) == [s EXCEPT !.cf = @ \/ fk # "none", !.ocf = @ \/ fk # "none"]

\* A write of this commit may have landed (earlier(ambiguous, unresolved)).
Earlier(s) == s.amb \/ s.unres

\* The commit continues as s2.
Go(w, s2, landed) ==
  /\ ws' = Upd(w, s2, landed)
  /\ UNCHANGED <<ended, res, ook, sched>>

\* The oracles' verdict on w's commit s as it ends with outcome out (final:
\* its operation ends too). CatalogOracles defines them; a configuration
\* substitutes one (OracleAtEnd <- OracleMW). It may refer to file', which
\* every action sets before it ends a commit.
OracleAtEnd(w, s, out, final) == TRUE

\* The commit ends with outcome out: committed (returned), failed
\* (CommitFailedException: a conflict), error (another failure, not
\* CommitStateUnknownException) or unknown (CommitStateUnknownException).
\* An operation restages a failed commit on a fresh read.
End(w, s, out, landed) ==
  LET final == ~(out = "failed" /\ Shapes[w][s.k].retry /\ s.rt < ClientRetries)
      again == [Idle(s.k, s.n + 1) EXCEPT !.pc = "restage", !.rt = s.rt + 1, !.nw = s.nw,
                                          !.first = s.first, !.ocf = s.ocf]
  IN
  /\ ended' = ended \cup {[id |-> s.txn.id, out |-> out, cf |-> s.cf, cx |-> s.cx,
                           ci |-> s.ci]}
  /\ ws' = Upd(w, IF final THEN Idle(s.k + 1, s.n + 1) ELSE again, landed)
  /\ res' = IF final THEN [res EXCEPT ![w] = out] ELSE res
  /\ ook' = (ook /\ OracleAtEnd(w, s, out, final))
  /\ sched' = IF Sched = "free" \/ ~final THEN sched
              ELSE IF w = sched.nest THEN [sched EXCEPT !.nest = None]
              ELSE [sched EXCEPT !.active = None, !.nest = None, !.pre = 0]

\* A read the commit cannot use: UnreadableCatalog. Unknown if a write of
\* the commit may have landed, else the read's failure.
Unreadable(w, s) == End(w, s, IF Earlier(s) THEN "unknown" ELSE "error", FALSE)

\* landed(): what a read R (watching the commit's record) says of the
\* commit's own record. Committed if replay applied it, or if R holds its
\* id; at or below R's horizon, unknown if a write may have landed, else
\* CommitFailedException (with Bug 64d the exception escaped raw);
\* otherwise `then`. On one object, a GC record precedes every record of
\* its version, so replay's judgment and the committed set agree.
Judge(w, s, R, then) ==
  LET c == Landed(R, s.txn.id) IN
  CASE c = "yes" -> End(w, s, "committed", FALSE)
    [] c = "unknown" -> End(w, s, IF Has("64d") THEN "error"
                                 ELSE IF Earlier(s) THEN "unknown" ELSE "failed", FALSE)
    [] OTHER -> then

\* Rebase on a read R: base := R, current := its version.
Rebased(s, R) ==
  [s EXCEPT !.unres = FALSE, !.base = R, !.cur = [gen |-> R.gen, len |-> R.len],
            !.stale = FALSE]

\* catch (AmbiguousWrite): resolve by reading. With Bug 17 the commit
\* reported a failure.
Ambig(w, s, landed) ==
  IF Has("17") THEN End(w, s, "failed", landed)
  ELSE Go(w, [s EXCEPT !.amb = TRUE, !.pc = "resolve"], landed)

\* A precondition failure (CASException, AppendException). A transport retry
\* of a write that landed fails it too, so it stays unresolved; with Bug 18
\* it was taken as proof that the write had not landed.
Conflict(w, s, strat, landed) ==
  Go(w, [s EXCEPT !.unres = IF Has("18") THEN @ ELSE TRUE,
                  !.pc = IF strat = "CAS" THEN "rebase" ELSE "refresh"], landed)

MustCAS(s) ==
  s.full \/ ~AppendSupported \/ s.base.cnt >= MaxCount \/ s.cur.len + 1 > MaxSize

\* The write w is about to send, or None: a throttled one again; the CAS
\* after a fresh read; else as the loop decides (mustCAS).
NextWrite(w) ==
  LET s == ws[w]
      cas == [strat |-> "CAS", cond |-> s.cur.gen,
              payload |-> CasPayload(w, IF Has("12") THEN s.snap ELSE s.base, s.txn)]
  IN CASE s.pc = "throttled" -> s.pend
       [] s.pc = "cas" -> cas
       [] s.pc = "loop" /\ s.att < MaxAttempts /\ MustCAS(s) /\ (~s.stale \/ Has("12")) -> cas
       [] s.pc = "loop" /\ s.att < MaxAttempts /\ ~MustCAS(s) ->
            [strat |-> "APPEND", cond |-> s.cur.gen, payload |-> s.txn]
       [] OTHER -> None

\* The transaction staged for operation sh on a read R of the catalog
\* (CatalogFile.Mut), as w's n-th commit. Its id is minted after the horizon
\* of R (UuidV7.newUuidV7After; not with Bug 64c). With Bug 16 the record
\* carries the preconditions of the entities it writes, not of those it only
\* reads.
Stage(w, sh, R, n) ==
  LET ts  == IF Has("64c") THEN Clock(w) ELSE Max(Clock(w), R.H + 1)
      ird == [e \in sh.rd |-> R.st.ver[e]]
  IN [id |-> [w |-> w, n |-> n, ts |-> ts],
      rd |-> IF Has("16") THEN [e \in sh.wr |-> R.st.ver[e]] ELSE ird,
      ird |-> ird, wr |-> sh.wr, cr |-> sh.cr,
      cid |-> IF Has("10") /\ sh.cr # None THEN R.st.next ELSE 0]

Staged(s, R, r) ==
  [s EXCEPT !.txn = r, !.snap = R, !.base = R, !.cur = [gen |-> R.gen, len |-> R.len]]

NRacers == Cardinality(Writers) - 1

\* The next step of the active operation is a write request (a race point).
AboutToWrite(w) == ws[w].pc \in {"loop", "cas", "throttled"} /\ NextWrite(w) # None

\* Whether another operation may now run inside the active one.
MayPreempt ==
  /\ sched.active # None /\ sched.nest = None
  /\ CASE Sched = "mw" -> sched.pre = 0
       [] Sched = "fault" -> sched.pre = 0 /\ ws[sched.active].nw = 0
       [] Sched = "race" -> sched.active = 1 /\ sched.pre = ws[1].nw /\ sched.pre < NRacers
                            /\ AboutToWrite(1)
       [] OTHER -> FALSE

\* Stage the next operation on a read of the catalog. In the fuzzers'
\* schedules, between operations (race: A first, the rest after it).
Begin(w) ==
  LET s == ws[w] IN
  /\ s.pc = "idle" /\ s.k <= Len(Shapes[w])
  /\ Sched = "free" \/ sched.active = None
  /\ Sched = "race" => (w = 1 \/ ws[1].k > Len(Shapes[1]))
  /\ ~LiveR.err
  /\ ws' = [ws EXCEPT ![w] = [Staged(s, LiveR, Stage(w, Shapes[w][s.k], LiveR, s.n))
                                 EXCEPT !.pc = "staged"]]
  /\ UNCHANGED <<file, inflight, now, nf, ended, res, ook, sched>>

\* commit() starts: alone, or (mw, fault) on an earlier snapshot inside the
\* active operation.
Start(w) ==
  /\ ws[w].pc = "staged"
  /\ IF Sched = "free" THEN UNCHANGED sched
     ELSE \/ /\ sched.active = None
             /\ sched' = [sched EXCEPT !.active = w, !.nest = None, !.pre = 0]
          \/ /\ Sched \in {"mw", "fault"} /\ MayPreempt /\ w # sched.active
             /\ sched' = [sched EXCEPT !.nest = w, !.pre = @ + 1, !.racers = @ \cup {w}]
  /\ ws' = [ws EXCEPT ![w].pc = "loop"]
  /\ UNCHANGED <<file, inflight, now, nf, ended, res, ook>>

\* fault, race: an operation reads, stages and commits inside the active one.
BeginNested(w) ==
  LET s == ws[w] IN
  /\ Sched \in {"fault", "race"} /\ MayPreempt
  /\ s.pc = "idle" /\ s.k <= Len(Shapes[w]) /\ w # sched.active
  /\ ~LiveR.err
  /\ ws' = [ws EXCEPT ![w] = [Staged(s, LiveR, Stage(w, Shapes[w][s.k], LiveR, s.n))
                                 EXCEPT !.pc = "loop"]]
  /\ sched' = [sched EXCEPT !.nest = w, !.pre = @ + 1, !.racers = @ \cup {w}]
  /\ UNCHANGED <<file, inflight, now, nf, ended, res, ook>>

\* FileIOCatalog restages an operation whose commit failed on a fresh read
\* (commitOnAFreshRead; Iceberg's table commit retries likewise).
Restage(w, fk) ==
  LET s == ws[w] IN
  /\ CanStep(w) /\ s.pc = "restage"
  /\ fk \in RFaultsOf(w)
  /\ UseFault(fk)
  /\ UNCHANGED <<file, inflight, now>>
  /\ IF fk = "readFail" \/ LiveR.err
       THEN End(w, [s EXCEPT !.txn = [id |-> None], !.cf = TRUE, !.ocf = TRUE], "error", FALSE)
     ELSE Go(w, [Staged(s, LiveR, Stage(w, Shapes[w][s.k], LiveR, s.n)) EXCEPT !.pc = "loop"],
             FALSE)

\* tryCAS / tryAppend through retryThrottled and writeAndRead: one request.
\* Faults: failBefore (an IOException before landing), lostReply (landed,
\* reply lost), landedConflict (landed; a transport retry of it failed its
\* precondition), throttled (not landed), landedThrottled (landed, then
\* throttled), inflight (timed out, still in flight: it may land later, see
\* LandInflight). A write past the store's cap fails (SegmentLimitException);
\* with Bug 66 it was taken as ambiguous.
Write(w, fk) ==
  LET s  == ws[w]
      x  == NextWrite(w)
      ok == file.gen = x.cond
      seg == SegOK(file, x.strat)
      lands == ok /\ seg /\ fk \in {"none", "lostReply", "landedConflict", "landedThrottled"}
      f2 == IF lands THEN Land(file, x) ELSE file
      thr0 == IF s.pc = "throttled" THEN s.thr ELSE 0
      s1 == [s EXCEPT !.full = IF x.strat = "CAS" THEN FALSE ELSE @,
                      !.cf = @ \/ fk # "none", !.ocf = @ \/ fk # "none",
                      !.cw = IF lands THEN Append(@, [before |-> file, after |-> f2]) ELSE @,
                      !.first = IF @ = None THEN file ELSE @, !.nw = @ + 1,
                      !.pend = None, !.thr = 0]
  IN /\ CanStep(w) /\ x # None
     \* race: racer k runs just before A's write k
     /\ Sched = "race" /\ w = sched.active => sched.pre > s.nw \/ s.nw >= NRacers
     /\ fk \in WFaultsOf(w)
     /\ fk \in {"lostReply", "landedConflict", "landedThrottled", "inflight"} => ok /\ seg
     /\ file' = f2
     /\ inflight' = IF lands THEN {}
                    ELSE IF fk = "inflight"
                      THEN inflight \cup {[w |-> w, id |-> s.txn.id, x |-> x]}
                    ELSE inflight
     /\ UseFault(fk)
     /\ UNCHANGED now
     /\ CASE fk = "none" /\ ok /\ seg ->
               Go(w, [s1 EXCEPT !.pc = "readback", !.written = f2], TRUE)
          [] fk = "none" /\ ok ->       \* SegmentLimitException: compact on a fresh read
               IF Has("66") THEN Ambig(w, s1, FALSE)
               ELSE Go(w, [s1 EXCEPT !.full = TRUE, !.stale = TRUE, !.att = @ + 1,
                                     !.pc = "loop"], FALSE)
          [] fk \in {"none", "landedConflict"} -> Conflict(w, s1, x.strat, lands)
          [] fk \in {"failBefore", "lostReply", "inflight"} -> Ambig(w, s1, lands)
          [] OTHER ->                   \* throttled, landedThrottled
               IF thr0 + 1 < MaxThrottle
                 THEN Go(w, [s1 EXCEPT !.pc = "throttled", !.pend = x, !.thr = thr0 + 1], lands)
               ELSE IF Has("25") THEN End(w, s1, "failed", lands)
               ELSE Ambig(w, s1, lands)

\* writeAndRead reads back the version it wrote; validateCommit judges it. A
\* failed read, or (strict) one of a version replaced since, is ambiguous.
ReadBack(w, fk) ==
  LET s == ws[w]
      R == ReplayOf(s.written, s.txn.id)
      s1 == [Faulted(s, fk) EXCEPT !.written = None]
  IN /\ CanStep(w) /\ s.pc = "readback"
     /\ fk \in RFaultsOf(w)
     /\ UseFault(fk)
     /\ UNCHANGED <<file, inflight, now>>
     /\ IF fk = "readFail" \/ (Strict /\ file.gen # s.written.gen) \/ R.err
          THEN Ambig(w, s1, FALSE)
          ELSE Judge(w, [s1 EXCEPT !.cx = @ \/ Crossed(R, s.txn)], R,
                     End(w, s1, "failed", FALSE))   \* replay rejected the record: conflict

\* A read of the live catalog by a commit, then `then(R)`; a failure is
\* Unreadable.
ReadLive(w, fk, s, pcs) ==
  /\ CanStep(w) /\ s.pc \in pcs
  /\ fk \in RFaultsOf(w)
  /\ UseFault(fk)
  /\ UNCHANGED <<file, inflight, now>>

\* After a CAS conflict: rebase on a fresh read if the transaction still
\* holds on it, else fail without writing (conflict()).
Rebase(w, fk) ==
  LET s == ws[w]
      s1 == [Faulted(s, fk) EXCEPT !.cx = @ \/ Crossed(LiveR, s.txn)]
  IN /\ ReadLive(w, fk, s, {"rebase"})
     /\ IF fk = "readFail" \/ LiveR.err THEN Unreadable(w, Faulted(s, fk))
        ELSE Judge(w, s1, LiveOf(s),
               IF Verify(LiveR.st, s.txn)
                 THEN Go(w, [Rebased(s1, LiveR) EXCEPT !.att = @ + 1, !.pc = "loop"], FALSE)
                 ELSE End(w, [s1 EXCEPT !.unres = FALSE], "failed", FALSE))

\* After an append lost its race: a metadata request for the new length.
\* Shrank (compacted): re-read. Grew: retry the same record at the new
\* version without reading it (base goes stale).
Refresh(w, fk) ==
  LET s == ws[w]
      s1 == Faulted(s, fk)
      cur2 == [gen |-> file.gen, len |-> FileLen(file)]
  IN /\ ReadLive(w, fk, s, {"refresh"})
     /\ IF fk = "readFail" THEN Unreadable(w, s1)
        ELSE IF FileLen(file) < s.cur.len
          THEN Go(w, [s1 EXCEPT !.cur = cur2, !.pc = "shrink"], FALSE)
        ELSE Go(w, [s1 EXCEPT !.cur = cur2, !.stale = TRUE, !.att = @ + 1, !.pc = "loop"], FALSE)

\* The file shrank: read it afresh.
Shrink(w, fk) ==
  LET s == ws[w]
      s1 == [Faulted(s, fk) EXCEPT !.cx = @ \/ Crossed(LiveR, s.txn)]
  IN /\ ReadLive(w, fk, s, {"shrink"})
     /\ IF fk = "readFail" \/ LiveR.err THEN Unreadable(w, Faulted(s, fk))
        ELSE Judge(w, s1, LiveOf(s),
               Go(w, [Rebased(s1, LiveR) EXCEPT !.att = @ + 1, !.pc = "loop"], FALSE))

\* The loop must CAS but base is stale: read the live version afresh, then
\* CAS on it. With Bug 26 it read the version `current` was bound to, which
\* strict storage refuses once replaced. With Bug 12 there was no read (see
\* NextWrite).
FreshForCAS(w, fk) ==
  LET s == ws[w]
      s1 == [Faulted(s, fk) EXCEPT !.cx = @ \/ Crossed(LiveR, s.txn)]
  IN /\ s.pc = "loop" /\ s.att < MaxAttempts /\ MustCAS(s) /\ s.stale /\ ~Has("12")
     /\ ReadLive(w, fk, s, {"loop"})
     /\ IF fk = "readFail" \/ LiveR.err \/ (Has("26") /\ Strict /\ file.gen # s.cur.gen)
          THEN Unreadable(w, Faulted(s, fk))
        ELSE Judge(w, s1, LiveOf(s), Go(w, [Rebased(s1, LiveR) EXCEPT !.pc = "cas"], FALSE))

\* catch (AmbiguousWrite): a fresh read decides. If the record is not there,
\* retry on that read if the transaction holds on it, else fail. A failed
\* read leaves the outcome unknown.
Resolve(w, fk) ==
  LET s == ws[w]
      s1 == [Faulted(s, fk) EXCEPT !.cx = @ \/ Crossed(LiveR, s.txn)]
  IN /\ ReadLive(w, fk, s, {"resolve"})
     /\ IF fk = "readFail" \/ LiveR.err THEN Unreadable(w, Faulted(s, fk))
        ELSE Judge(w, s1, LiveOf(s),
               IF Verify(LiveR.st, s.txn)
                 THEN Go(w, [Rebased(s1, LiveR) EXCEPT !.att = @ + 1, !.pc = "loop"], FALSE)
                 ELSE End(w, [s1 EXCEPT !.unres = FALSE], "failed", FALSE))

\* Out of attempts: unknown after an ambiguous write; failed, unless the last
\* append lost a race to its own landing, which a final read decides.
Exhaust(w) ==
  LET s == ws[w] IN
  /\ CanStep(w) /\ s.pc = "loop" /\ s.att >= MaxAttempts
  /\ s.amb \/ ~s.unres
  /\ UNCHANGED <<file, inflight, now, nf>>
  /\ End(w, s, IF s.amb THEN "unknown" ELSE "failed", FALSE)

FinalRead(w, fk) ==
  LET s == ws[w]
      s1 == [Faulted(s, fk) EXCEPT !.cx = @ \/ Crossed(LiveR, s.txn)]
  IN /\ s.pc = "loop" /\ s.att >= MaxAttempts /\ ~s.amb /\ s.unres
     /\ ReadLive(w, fk, s, {"loop"})
     /\ IF fk = "readFail" \/ LiveR.err THEN Unreadable(w, Faulted(s, fk))
        ELSE Judge(w, s1, LiveOf(s), End(w, s1, "failed", FALSE))

\* A timed-out write lands after all, while its precondition holds.
LandInflight(i) ==
  /\ i \in inflight
  /\ file.gen = i.x.cond /\ SegOK(file, i.x.strat)
  /\ file' = Land(file, i.x)
  /\ inflight' = {}
  /\ ws' = [v \in Writers |->
              IF ws[v].pc # "idle" /\ (v # i.w \/ ws[v].txn.id # i.id)
                THEN [ws[v] EXCEPT !.ci = TRUE] ELSE ws[v]]
  /\ UNCHANGED <<now, nf, ended, res, ook, sched>>

Tick ==
  /\ Retention >= 0 /\ now < MaxTime
  /\ now' = now + 1
  /\ UNCHANGED <<file, inflight, nf, ws, ended, res, ook, sched>>

Done ==
  /\ \A w \in Writers : ws[w].pc = "idle" /\ ws[w].k > Len(Shapes[w])
  /\ UNCHANGED vars

Init ==
  /\ file = InitFile
  /\ inflight = {}
  /\ now = 0
  /\ nf = 0
  /\ ws = [w \in Writers |-> Idle(1, 1)]
  /\ ended = {}
  /\ res = [w \in Writers |-> None]
  /\ ook = TRUE
  /\ sched = [active |-> None, nest |-> None, pre |-> 0, racers |-> {}]

WriterNext(w) ==
  \/ Begin(w)
  \/ Start(w)
  \/ BeginNested(w)
  \/ \E fk \in WriteFaults \cup {"none"} : Write(w, fk)
  \/ \E fk \in ReadFaults \cup {"none"} :
       ReadBack(w, fk) \/ Rebase(w, fk) \/ Refresh(w, fk) \/ Shrink(w, fk)
         \/ FreshForCAS(w, fk) \/ Resolve(w, fk) \/ FinalRead(w, fk) \/ Restage(w, fk)
  \/ Exhaust(w)

Next ==
  \/ \E w \in Writers : WriterNext(w)
  \/ \E i \in inflight : LandInflight(i)
  \/ Tick
  \/ Done

Spec == Init /\ [][Next]_vars

\* Writers keep taking steps; storage stops failing (the fault budget).
FairSpec == Spec /\ \A w \in Writers : WF_vars(WriterNext(w))

-----------------------------------------------------------------------------
(* Properties *)

Hist == LiveR.st.hist

\* Every version of the catalog replays (Bug 64: one record at the horizon
\* failed every read).
Readable == ~LiveR.err

\* No transaction applies twice.
ExactlyOnce == \A i, j \in 1..Len(Hist) : i # j => Hist[i].id # Hist[j].id

\* The catalog is the serial execution of the transactions applied
\* (RSM's SerialFold).
Serializable == SerialFold(InitSt, Hist) = [ok |-> TRUE, st |-> LiveR.st]

\* No id names two entities (S4 in FileIOCatalog.tla; Bug 10).
UniqueIds == \A m1, m2 \in LiveR.st.made : m1.id = m2.id => m1 = m2

\* Honest outcomes: a commit reported committed applied, and stays applied
\* (through compaction and GC); one reported failed (or any error but
\* unknown) is not applied, now or later (a late landing included); one
\* reported unknown follows a fault or a read that put its id at or below
\* the horizon.
HonestCommitted == \A e \in ended : e.out = "committed" => e.id \in IdsOf(Hist)
HonestFailed == \A e \in ended : e.out \in {"failed", "error"} => e.id \notin IdsOf(Hist)
UnknownExplained == \A e \in ended : e.out = "unknown" => e.cf \/ e.cx

\* A commit no other write raced, on storage that did not fail, commits.
SoloCommits == \A e \in ended : (~e.cf /\ ~e.ci) => e.out = "committed"

\* What history the catalog holds only grows, by one transaction at a time.
HistGrows ==
  [][LET h2 == Replay(file').st.hist IN
       /\ Len(h2) >= Len(Hist) /\ SubSeq(h2, 1, Len(Hist)) = Hist
       /\ Len(h2) <= Len(Hist) + 1]_file

\* A CAS applies its own record when it holds on the state checkpointed
\* (Bug 64b: the GC horizon reached it).
CasAppliesItsRecord ==
  [][\A w \in Writers :
       LET s == ws[w] IN
       (/\ file' # file /\ s.pc \in {"loop", "cas", "throttled"}
        /\ NextWrite(w) # None /\ NextWrite(w).strat = "CAS"
        /\ file' = Land(file, NextWrite(w))
        /\ Verify(NextWrite(w).payload.chk.st, s.txn))
         => s.txn.id \in IdsOf(Replay(file').st.hist)]_vars

\* A CAS's checkpoint is the replay of the version it replaces: a function of
\* the log, not of the writer's clock (I11). A log with checkpoints apart from
\* its records needs this, so that every reader, from whichever checkpoint,
\* applies the same records.
CheckpointIsReplay ==
  [][(file'.gen # file.gen /\ file'.segs = 1) =>
       LET R == Replay(file) IN file'.chk = [st |-> R.st, C |-> R.C, H |-> R.H]]_file

Safety ==
  Readable /\ ExactlyOnce /\ Serializable /\ UniqueIds
    /\ HonestCommitted /\ HonestFailed /\ UnknownExplained

\* Every writer finishes.
Termination == <>[](\A w \in Writers : ws[w].pc = "idle" /\ ws[w].k > Len(Shapes[w]))

=============================================================================
