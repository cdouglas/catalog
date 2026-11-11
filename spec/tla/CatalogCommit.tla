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
(* With Paging, a CAS also pages the committed set as                      *)
(* docs/COMMITTED_PAGING.md designs it: its write is [checkpoint][gc]      *)
(* [page-outs][record], and it puts the pages first; sweeps delete pages   *)
(* no PageOut can name.                                                    *)
(*                                                                         *)
(* The object's frames name it (format 3, docs/TAIL_READS.md): each CAS    *)
(* draws an incarnation for the object it writes, and an append frames its *)
(* record with the incarnation of the version its condition names, from   *)
(* the read it was staged on or, after a probe, from the probed version's *)
(* last trailer. After an append loses its race, the loop tail-reads from  *)
(* the replica it holds (TailReads): the records after it if the object is *)
(* still its incarnation and no shorter, else the whole object.           *)
(*                                                                         *)
(* With CompactAfter, the count and size limits no longer make a commit    *)
(* replace the object: it appends, and once its record has landed its      *)
(* writer may compact (Compact): a replace of the version it holds with a  *)
(* checkpoint of it, a GC record and PageOuts, and no record of its own.   *)
(* Whatever becomes of that write, the commit stands. A compaction that    *)
(* lost to appends may be sent again, conditioned on the object as it      *)
(* grew, with the records that beat it copied in front of its GC record    *)
(* and PageOuts (CompactRead). A commit still replaces when no append can  *)
(* carry it (the store's cap, no append, CAS only).                        *)
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
  Paging,           \* a CAS pages the committed set's oldest blocks (COMMITTED_PAGING.md)
  Admission,        \* A, in ticks: a CAS pages blocks whose top is A old
  MaxSweep,         \* how many sweeps of the page store run
  TailReads,        \* fileio.catalog.read.tail: after a lost append race, a tail read
                    \* (TRUE), or a probe and a resend framed from the probed version's
                    \* last trailer (FALSE)
  CompactAfter,     \* compaction after the commit (TRUE): past the limits a commit appends,
                    \* and its writer may then compact; or fused with it (FALSE): past them
                    \* the commit replaces the object with a checkpoint and its record
  CompactResends,   \* how many times a compaction that lost to appends may be sent again
  Sched             \* "free": any interleaving. A fuzzer's schedules, where operations
                    \* run one at a time and others may run to completion inside one:
                    \* "mw" (TestMultiWriterProperties: one, before any request, on a
                    \* snapshot taken earlier), "fault" (TestCatalogFaultProperties:
                    \* one, before the first write), "race" (TestCatalogRaceProperties:
                    \* writer 1 runs A, and each other writer runs fresh just before
                    \* one of A's writes, in turn, or after A)
                    \* Bugs (RSM): protocol bugs put back, by number
                    \* BlockIds, GridBits (RSM): the committed set's blocks

WriteFaults == {"failBefore", "lostReply", "landedConflict", "throttled",
                "landedThrottled", "inflight"}
ReadFaults == {"readFail", "pageFail"}
PutFaults == {"putFail", "putLost"}

ASSUME /\ FaultKinds \subseteq WriteFaults \cup ReadFaults \cup PutFaults
       /\ MaxAttempts >= 1 /\ MaxThrottle >= 1
       /\ Sched \in {"free", "mw", "fault", "race"}
       /\ Bugs \subseteq {"10", "12", "16", "17", "18", "25", "26",
                          "64a", "64b", "64c", "64d", "66", "ckgc", "basever",
                          "floorgap", "nodigest", "absentpage", "sweepabove", "mintH",
                          "pageown", "rootfirst", "sweepstale",
                          "lengthonly", "notrailer", "probeinc",
                          "compactbase", "compactresend", "compactfail",
                          "compactcopyafter", "compactreplaced"}
       /\ TailReads \in BOOLEAN
       /\ CompactAfter \in BOOLEAN
       /\ CompactResends \in Nat

VARIABLES
  file,      \* the catalog object: [gen, chk: [st, C, H], log, segs]
  inflight,  \* writes sent, not landed, not yet failed: a timed-out request
  now,       \* the clock
  nf,        \* faults injected so far
  ws,        \* Writers -> the writer's commit (see Idle)
  ended,     \* the outcomes reported, per commit: [id, out, cf, cx, ci]
  res,       \* Writers -> the outcome of the writer's last operation, or None
  ook,       \* every oracle check so far passed (CatalogOracles)
  sched,     \* [active, nest: the operations running; pre: how many ran inside the
             \*  active one; racers: who ran inside one]
  store,     \* the page store: the names of the pages in it
  nsw,       \* sweeps so far
  spages     \* the state's pages stored (SNAPSHOT_PAGING.md): [e, v] names

vars == <<file, inflight, now, nf, ws, ended, res, ook, sched, store, nsw, spages>>
PageVars == <<store, nsw, spages>>

-----------------------------------------------------------------------------
(* The catalog object, and reads of it *)

\* inc: the object's incarnation; fi: the incarnation each record's frame
\* names (the checkpoint's is inc, as the CAS that wrote it drew); prev: with
\* "notrailer", the version before the last write, which a torn read splices.
InitFile == [gen |-> 1, chk |-> [st |-> InitSt, C |-> {}, H |-> 0, F |-> 0, P |-> {}],
             log |-> << >>, segs |-> 1, inc |-> <<0, 0, 0>>, fi |-> << >>, prev |-> None]

\* A read: the replayed state, what replay did with the record of `watch`,
\* the version read and its length (in records, the checkpoint counting
\* one), the intention-record count (intentionRecords), the incarnation, and
\* whether the view is a state the object held (held: the oracle that no
\* replica holds a state the log never held).
ReplayOf(f, watch) ==
  LET a == Fold(Acc(f.chk.st, f.chk, watch), f.log)
  IN [st |-> a.st, C |-> a.C, H |-> a.H, F |-> a.F, P |-> a.P, err |-> a.err, out |-> a.out,
      cnt |-> Len(Txns(f.log)), gen |-> f.gen, len |-> Len(f.log) + 1, rf |-> a.rf,
      inc |-> f.inc, held |-> TRUE]

Replay(f) == ReplayOf(f, None)

FileLen(f) == Len(f.log) + 1

LiveR == Replay(file)

\* A commit's read of the live catalog, watching its own record.
LiveOf(s) == ReplayOf(file, s.txn.id)

\* Every frame names the object it is in (TAIL_READS.md "Why a tail read is
\* sound"): a checked read of an object with a frame of another fails.
FramesTrue == \A i \in DOMAIN file.fi : file.fi[i] = file.inc

\* A read torn by a write that landed between its lookup and its GET, on a
\* store that does not pin reads: the bound version's checkpoint and first k
\* records (k short of all of them) and the live version's after them, to the
\* bound version's length. Format 3's
\* checks reject one of two incarnations; with "notrailer" it is replayed.
Spliced(k, watch) ==
  LET b == file.prev
      lg == SubSeq(b.log, 1, k) \o SubSeq(file.log, k + 1, Len(b.log))
      R == ReplayOf([gen |-> b.gen, chk |-> b.chk, log |-> lg, inc |-> b.inc], watch)
  IN [R EXCEPT !.held = (b.inc = file.inc)]

\* The reads a whole read may return: the live object, and with "notrailer"
\* a splice of it and the version before (the checks are off).
WholeReads(watch) ==
  {ReplayOf(file, watch)}
    \cup (IF Has("notrailer") /\ file.prev # None /\ file.prev.inc # file.inc
           /\ Len(file.prev.log) <= Len(file.log)
            THEN {Spliced(k, watch) : k \in 0..(Len(file.prev.log) - 1)} ELSE {})

\* A tail read from a replica R at version cur (readAfter): if the object is
\* still cur's incarnation and no shorter, R with the records after cur folded
\* onto it; else the whole object (WholeReads). With "lengthonly" it compares
\* lengths only, as FileTail.continues did: a longer replace reads as growth.
Continues(cur) == (Has("lengthonly") \/ file.inc = cur.inc) /\ FileLen(file) >= cur.len
Folded(R, cur, watch) ==
  LET rest == SubSeq(file.log, cur.len, Len(file.log))
      a == Fold([Acc(R.st, R, watch) EXCEPT !.out = R.out], rest)
  IN [st |-> a.st, C |-> a.C, H |-> a.H, F |-> a.F, P |-> a.P, err |-> a.err, out |-> a.out,
      cnt |-> R.cnt + Len(Txns(rest)), gen |-> file.gen, len |-> FileLen(file), rf |-> a.rf,
      inc |-> R.inc, held |-> R.held /\ file.inc = R.inc]
TailViews(R, cur, watch) ==
  IF Continues(cur) THEN {Folded(R, cur, watch)} ELSE WholeReads(watch)

-----------------------------------------------------------------------------
(* Writes *)

Clock(w) == 1 + now + Skew[w]

\* The PageOuts a compactor writes on view a: the oldest blocks, while one
\* is sealable and its top at or below the ceiling (COMMITTED_PAGING.md
\* "Compaction", with T = 0).
RECURSIVE PageOuts(_, _)
PageOuts(a, ceil) ==
  IF Sealable(a.C) /\ Top(Oldest(a.C)) <= ceil
    THEN << PoOf(a.C) >> \o PageOuts(PageOut(a, PoOf(a.C)), ceil)
  ELSE << >>

\* The file a CAS writes, but its generation (LogRuntime's replace and
\* `records`): a checkpoint of `b`, a GC record at the watermark (none when GC
\* is off), with Paging its PageOuts, then the record.
\* The checkpoint is the replay of the version the CAS replaces, whatever the
\* clock says (I11). With Bug 64b the watermark is not capped below the
\* record's own time. With "ckgc" the checkpoint itself is filtered at the
\* watermark, as it was before GC records: a reader starting from another
\* checkpoint of the same log would not see the same horizon.
\* The PageOuts page the view after the CAS's own GC, up to a ceiling: the
\* writer's clock less the admission window, and below its own record's
\* stamp, as its GC record is (with "pageown", not below it).
CasPayload(w, b, t) ==
  LET c    == Clock(w)
      wm0  == IF Retention < 0 \/ Retention >= c THEN 0 ELSE c - Retention
      wm   == IF Has("64b") THEN wm0 ELSE Max(0, Min(wm0, t.id.ts - 1))
      ceil == IF Has("pageown") THEN c - Admission ELSE Min(c - Admission, t.id.ts - 1)
      pos  == IF Paging THEN PageOuts(Gc(b, wm), ceil) ELSE << >>
  IN IF Has("ckgc")
       THEN [chk |-> Ck(Gc(b, wm)), log |-> << t >>]
       ELSE [chk |-> Ck(b),
             log |-> (IF wm = 0 THEN << >> ELSE << [wm |-> wm] >>) \o pos \o << t >>]

\* The file a compaction after a commit writes on the replica R its record
\* landed in (LogRuntime's compaction of a log that replaces itself): a
\* checkpoint of R, a GC record at the watermark and the PageOuts of the view
\* after it, and no record: nothing of its own follows, so neither the
\* watermark nor the ceiling is kept below a record. With "compactbase", a
\* checkpoint of the replica the commit staged or rebased on, before its
\* record.
CompactPayload(w, s) ==
  LET R    == IF Has("compactbase") THEN s.base ELSE s.lr
      c    == Clock(w)
      wm   == IF Retention < 0 \/ Retention >= c THEN 0 ELSE c - Retention
      pos  == IF Paging THEN PageOuts(Gc(R, wm), c - Admission) ELSE << >>
  IN [chk |-> Ck(R), log |-> (IF wm = 0 THEN << >> ELSE << [wm |-> wm] >>) \o pos]

\* The pages a CAS's PageOuts seal.
CasPages(pl) == {PageName(pl.log[i]) : i \in {j \in 1..Len(pl.log) : IsPo(pl.log[j])}}

\* Storage applies a write x = [strat, cond, payload] to f.
\* A CAS writes an object of the incarnation it drew, every frame naming it;
\* an append's frame names the incarnation its writer took for the version
\* its condition names (x.inc).
Land(f, x) ==
  LET pv == IF Has("notrailer") THEN [gen |-> f.gen, chk |-> f.chk, log |-> f.log, inc |-> f.inc]
            ELSE None
  IN IF x.strat = "CAS"
       THEN [gen |-> f.gen + 1, chk |-> x.payload.chk, log |-> x.payload.log, segs |-> 1,
             inc |-> x.inc, fi |-> [i \in 1..Len(x.payload.log) |-> x.inc], prev |-> pv]
     ELSE [f EXCEPT !.gen = @ + 1, !.log = Append(@, x.payload), !.segs = @ + 1,
                    !.fi = Append(@, x.inc), !.prev = pv]

\* The store accepts another write to the object (S3 Express: 10,000 parts;
\* an Azure Append Blob: 50,000 blocks). A CAS replaces it.
SegOK(f, strat) == strat = "CAS" \/ SegLimit = 0 \/ f.segs + 1 <= SegLimit

-----------------------------------------------------------------------------
(* Writers *)

\* An idle writer, about to stage its k-th operation (ids count with n).
Idle(k, n) ==
  [pc |-> "idle", k |-> k, n |-> n, txn |-> None, snap |-> None, base |-> None,
   cur |-> None, stale |-> FALSE, amb |-> FALSE, unres |-> FALSE, full |-> FALSE,
   att |-> 0, thr |-> 0, pend |-> None, written |-> None, rt |-> 0, nw |-> 0, pay |-> None,
   cf |-> FALSE, cx |-> FALSE, ci |-> FALSE, cw |-> << >>, first |-> None, ocf |-> FALSE,
   lr |-> None, cre |-> FALSE, cv |-> None, cp |-> << >>, crs |-> 0, cc |-> << >>]
\* pc: idle, staged, loop, cas, throttled, readback, rebase, refresh, shrink,
\*   resolve, restage, compact, compactread.
\* rt: the times the operation was restaged; nw: its write requests; pay:
\*   with Paging, the CAS payload fixed when its pages were put (PrepCas).
\* lr: the replica the commit's record landed in, which its compaction
\*   replaces (compact); cre: with "compactresend", the compaction was resent.
\*   cv: the version [gen, len, inc] a resent compaction is conditioned on
\*   (None: lr's); cp: the records it copies in front of its GC record and
\*   PageOuts; crs: how many times it was sent again (compactread).
\*   Ghost: cc, its compaction's writes that landed, [before, after].
\* base: the parsed state of cur (current), unless stale (stale).
\* cur: the version the next write is conditioned on [gen, len, inc]
\*   (current): inc is the incarnation an append from it frames its record
\*   with.
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

\* A read the commit cannot use: UnreadableLog. Unknown if a write of
\* the commit may have landed, else the read's failure.
Unreadable(w, s) == End(w, s, IF Earlier(s) THEN "unknown" ELSE "error", FALSE)

\* catch (AmbiguousWrite): resolve by reading. With Bug 17 the commit
\* reported a failure.
Ambig(w, s, landed) ==
  IF Has("17") THEN End(w, s, "failed", landed)
  ELSE Go(w, [s EXCEPT !.amb = TRUE, !.pc = "resolve"], landed)

\* The policy is due on a log with no records after its checkpoint, and one
\* more: CAS only (max.append.count 0), or a size limit no record fits under.
\* Every commit replaces the object then, and none is compacted after.
DueEmpty == MaxCount = 0 \/ 1 + 1 > MaxSize

\* With CompactAfter: whether the writer of a commit whose record landed in
\* R may compact it, as the policy says of R (the limits past which a fused
\* commit replaces). It may also not: the policy draws (jitter), and the
\* model takes both.
CompactDue(R) ==
  /\ CompactAfter /\ AppendSupported /\ ~DueEmpty
  /\ R.cnt >= MaxCount \/ R.len + 1 > MaxSize

\* The commit's record landed in R: it is committed, after a compaction of R
\* if its writer compacts.
Committed(w, s, R) ==
  \/ End(w, s, "committed", FALSE)
  \/ /\ CompactDue(R)
     /\ Go(w, [s EXCEPT !.pc = "compact", !.lr = R, !.pay = None], FALSE)

\* landed(): what a read R (watching the commit's record) says of the
\* commit's own record. Committed if replay applied it, or if R holds its
\* id; at or below R's horizon, unknown if a write may have landed, else
\* CommitFailedException (with Bug 64d the exception escaped raw);
\* otherwise `then`. On one object, a GC record precedes every record of
\* its version, so replay's judgment and the committed set agree. A paged
\* id is looked up in its page (pf: the get fails): one its page lacks can
\* never apply (BEYOND_FLOOR: CommitFailedException); a get that fails is a
\* failed read, in a read back (rb) an ambiguous write; a page deleted since
\* the read: the runtime reads again, so this read decides nothing.
Judge(w, s, R, pf, rb, then) ==
  LET c == LandedP(R, s.txn.id, store, pf) IN
  CASE c = "yes" -> Committed(w, s, R)
    [] c = "unknown" -> End(w, s, IF Has("64d") THEN "error"
                                 ELSE IF Earlier(s) THEN "unknown" ELSE "failed", FALSE)
    [] c = "absent" -> End(w, s, "failed", FALSE)
    [] c = "fail" -> IF rb THEN Ambig(w, s, FALSE) ELSE Unreadable(w, s)
    [] c = "missing" -> FALSE
    [] OTHER -> then

\* Rebase on a read R: base := R, current := its version.
Rebased(s, R) ==
  [s EXCEPT !.unres = FALSE, !.base = R, !.cur = [gen |-> R.gen, len |-> R.len, inc |-> R.inc],
            !.stale = FALSE, !.pay = None]

\* A precondition failure (CASException, AppendException). A transport retry
\* of a write that landed fails it too, so it stays unresolved; with Bug 18
\* it was taken as proof that the write had not landed.
Conflict(w, s, strat, landed) ==
  Go(w, [s EXCEPT !.unres = IF Has("18") THEN @ ELSE TRUE,
                  !.pc = IF strat = "CAS" THEN "rebase"
                         ELSE IF TailReads THEN "tailread" ELSE "refresh"], landed)

\* Whether the commit replaces the object (mustReplace): the store refused an
\* append, it has none, or CAS only; and, without CompactAfter, past the
\* limits.
MustCAS(s) ==
  \/ s.full \/ ~AppendSupported
  \/ IF CompactAfter THEN DueEmpty
     ELSE s.base.cnt >= MaxCount \/ s.cur.len + 1 > MaxSize

\* The write w is about to send, or None: a throttled one again; the CAS
\* after a fresh read; else as the loop decides (mustCAS). With Paging, a
\* CAS waits for its payload, fixed when its pages were put (PrepCas).
CasDue(s) ==
  \/ s.pc = "cas"
  \/ s.pc = "loop" /\ s.att < MaxAttempts /\ MustCAS(s) /\ (~s.stale \/ Has("12"))
NextWrite(w) ==
  LET s == ws[w]
      cas == [strat |-> "CAS", cond |-> s.cur.gen, inc |-> <<w, s.n, s.nw + 1>>,
              payload |-> IF Paging THEN s.pay
                          ELSE CasPayload(w, IF Has("12") THEN s.snap ELSE s.base, s.txn)]
  IN CASE s.pc = "throttled" -> s.pend
       [] CasDue(s) /\ (~Paging \/ s.pay # None) -> cas
       [] s.pc = "loop" /\ s.att < MaxAttempts /\ ~MustCAS(s) ->
            [strat |-> "APPEND", cond |-> s.cur.gen, inc |-> s.cur.inc, payload |-> s.txn]
       [] OTHER -> None

\* The transaction staged for operation sh on a read R of the catalog
\* (CatalogMutation), as w's n-th commit. Its id is minted after the floor
\* of R (UuidV7.newUuidV7After; not with Bug 64c; with "mintH", after its
\* horizon, as before paging). With Bug 16 the record carries the
\* preconditions of the entities it writes, not of those it only reads.
Stage(w, sh, R, n) ==
  LET ts  == IF Has("64c") THEN Clock(w)
             ELSE Max(Clock(w), (IF Has("mintH") THEN R.H ELSE R.F) + 1)
      ird == [e \in sh.rd |-> R.st.ver[e]]
  IN [id |-> [w |-> w, n |-> n, ts |-> ts],
      rd |-> IF Has("16") THEN [e \in sh.wr |-> R.st.ver[e]] ELSE ird,
      ird |-> ird, wr |-> sh.wr, cr |-> sh.cr, wv |-> [e \in sh.wr |-> R.st.ver[e]],
      cid |-> IF Has("10") /\ sh.cr # None THEN R.st.next ELSE 0]

Staged(s, R, r) ==
  [s EXCEPT !.txn = r, !.snap = R, !.base = R,
            !.cur = [gen |-> R.gen, len |-> R.len, inc |-> R.inc]]

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
  /\ \E R \in WholeReads(None) :
       /\ ~R.err
       /\ ws' = [ws EXCEPT ![w] = [Staged(s, R, Stage(w, Shapes[w][s.k], R, s.n))
                                      EXCEPT !.pc = "staged"]]
  /\ UNCHANGED <<file, inflight, now, nf, ended, res, ook, sched>>
  /\ UNCHANGED PageVars

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
  /\ UNCHANGED PageVars

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
  /\ UNCHANGED PageVars

\* FileIOCatalog restages an operation whose commit failed on a fresh read
\* (commitOnAFreshRead; Iceberg's table commit retries likewise).
Restage(w, fk) ==
  LET s == ws[w] IN
  /\ CanStep(w) /\ s.pc = "restage"
  /\ fk \in RFaultsOf(w) \ {"pageFail"}
  /\ UseFault(fk)
  /\ UNCHANGED <<file, inflight, now>>
  /\ UNCHANGED PageVars
  /\ IF fk = "readFail" \/ LiveR.err
       THEN End(w, [s EXCEPT !.txn = [id |-> None], !.cf = TRUE, !.ocf = TRUE], "error", FALSE)
     ELSE Go(w, [Staged(s, LiveR, Stage(w, Shapes[w][s.k], LiveR, s.n)) EXCEPT !.pc = "loop"],
             FALSE)

\* A replace or an append through LogRuntime's write: one request.
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
                      !.pend = None, !.thr = 0, !.pay = None]
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
     /\ UNCHANGED PageVars
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

\* write reads back the version it wrote (readThrough); validate judges it. A
\* failed read, or (strict) one of a version replaced since, is ambiguous.
ReadBack(w, fk) ==
  LET s == ws[w]
      R == ReplayOf(s.written, s.txn.id)
      s1 == [Faulted(s, fk) EXCEPT !.written = None]
  IN /\ CanStep(w) /\ s.pc = "readback"
     /\ fk \in RFaultsOf(w)
     /\ fk = "pageFail" => NeedsPage(R, s.txn.id)
     /\ UseFault(fk)
     /\ UNCHANGED <<file, inflight, now>>
     /\ UNCHANGED PageVars
     /\ IF fk = "readFail" \/ (Strict /\ file.gen # s.written.gen) \/ R.err
          THEN Ambig(w, s1, FALSE)
          ELSE Judge(w, [s1 EXCEPT !.cx = @ \/ Crossed(R, s.txn)], R, fk = "pageFail", TRUE,
                     End(w, s1, "failed", FALSE))   \* replay rejected the record: conflict

\* A read of the live catalog by a commit, then `then(R)`; a failure is
\* Unreadable. pageFail: the read succeeds, and the get of the page
\* landed() asks fails.
ReadLive(w, fk, s, pcs) ==
  /\ CanStep(w) /\ s.pc \in pcs
  /\ fk \in RFaultsOf(w)
  /\ fk = "pageFail" => s.pc # "refresh" /\ NeedsPage(LiveOf(s), s.txn.id)
  /\ UseFault(fk)
  /\ UNCHANGED <<file, inflight, now>>
  /\ UNCHANGED PageVars

\* After a CAS conflict: rebase on a fresh read if the transaction still
\* holds on it, else fail without writing (conflict()).
Rebase(w, fk) ==
  LET s == ws[w]
      s1 == [Faulted(s, fk) EXCEPT !.cx = @ \/ Crossed(LiveR, s.txn)]
  IN /\ ReadLive(w, fk, s, {"rebase"})
     /\ IF fk = "readFail" \/ LiveR.err THEN Unreadable(w, Faulted(s, fk))
        ELSE Judge(w, s1, LiveOf(s), fk = "pageFail", FALSE,
               IF Verify(LiveR.st, s.txn)
                 THEN Go(w, [Rebased(s1, LiveR) EXCEPT !.att = @ + 1, !.pc = "loop"], FALSE)
                 ELSE End(w, [s1 EXCEPT !.unres = FALSE], "failed", FALSE))

\* After an append lost its race, without tail reads: a metadata request for
\* the new length. Shrank (compacted): re-read. Grew: retry the same record
\* at the new version without reading it (base goes stale), framed with the
\* incarnation of the probed version's last trailer, read pinned to it (with
\* "probeinc", with the incarnation the writer last read: A4 broken).
Refresh(w, fk) ==
  LET s == ws[w]
      s1 == Faulted(s, fk)
      cur2 == [gen |-> file.gen, len |-> FileLen(file),
               inc |-> IF Has("probeinc") THEN s.cur.inc ELSE file.inc]
  IN /\ ReadLive(w, fk, s, {"refresh"})
     /\ IF fk = "readFail" THEN Unreadable(w, s1)
        ELSE IF FileLen(file) < s.cur.len
          THEN Go(w, [s1 EXCEPT !.cur = cur2, !.pc = "shrink"], FALSE)
        ELSE Go(w, [s1 EXCEPT !.cur = cur2, !.stale = TRUE, !.att = @ + 1, !.pc = "loop"], FALSE)

\* After an append lost its race, with tail reads (LogRuntime's commit loop,
\* readAfter from the replica it holds): the records after its version if the
\* object continues it, else the whole object; then the loop, on a fresh base.
\* A record of the commit's that landed after all decides it, as a fresh
\* read's would.
TailRead(w, fk) ==
  LET s == ws[w] IN
  /\ ReadLive(w, fk, s, {"tailread"})
  /\ IF fk = "readFail" THEN Unreadable(w, Faulted(s, fk))
     ELSE \E R \in TailViews(s.base, s.cur, s.txn.id) :
            LET s1 == [Faulted(s, fk) EXCEPT !.cx = @ \/ Crossed(R, s.txn)] IN
            IF R.err THEN Unreadable(w, Faulted(s, fk))
            ELSE Judge(w, s1, R, fk = "pageFail", FALSE,
                   Go(w, [Rebased(s1, R) EXCEPT !.att = @ + 1, !.pc = "loop"], FALSE))

\* The file shrank: read it afresh.
Shrink(w, fk) ==
  LET s == ws[w]
      s1 == [Faulted(s, fk) EXCEPT !.cx = @ \/ Crossed(LiveR, s.txn)]
  IN /\ ReadLive(w, fk, s, {"shrink"})
     /\ IF fk = "readFail" \/ LiveR.err THEN Unreadable(w, Faulted(s, fk))
        ELSE Judge(w, s1, LiveOf(s), fk = "pageFail", FALSE,
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
        ELSE Judge(w, s1, LiveOf(s), fk = "pageFail", FALSE,
                   Go(w, [Rebased(s1, LiveR) EXCEPT !.pc = "cas"], FALSE))

\* catch (AmbiguousWrite): a fresh read decides. If the record is not there,
\* retry on that read if the transaction holds on it, else fail. A failed
\* read leaves the outcome unknown.
Resolve(w, fk) ==
  LET s == ws[w]
      s1 == [Faulted(s, fk) EXCEPT !.cx = @ \/ Crossed(LiveR, s.txn)]
  IN /\ ReadLive(w, fk, s, {"resolve"})
     /\ IF fk = "readFail" \/ LiveR.err THEN Unreadable(w, Faulted(s, fk))
        ELSE Judge(w, s1, LiveOf(s), fk = "pageFail", FALSE,
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
  /\ UNCHANGED PageVars
  /\ End(w, s, IF s.amb THEN "unknown" ELSE "failed", FALSE)

FinalRead(w, fk) ==
  LET s == ws[w]
      s1 == [Faulted(s, fk) EXCEPT !.cx = @ \/ Crossed(LiveR, s.txn)]
  IN /\ s.pc = "loop" /\ s.att >= MaxAttempts /\ ~s.amb /\ s.unres
     /\ ReadLive(w, fk, s, {"loop"})
     /\ IF fk = "readFail" \/ LiveR.err THEN Unreadable(w, Faulted(s, fk))
        ELSE Judge(w, s1, LiveOf(s), fk = "pageFail", FALSE, End(w, s1, "failed", FALSE))

\* With CompactAfter, the compaction of the replica the commit's record
\* landed in: one conditional replace of that version (with Paging, its pages
\* put first, PrepCas). A replace that fails its condition, fails, or is
\* throttled is dropped (one in flight may still land, LandInflight); either
\* way the commit is committed. One that lost its race may be sent again
\* (CompactRead), conditioned on the version the writer read then (cv), with
\* the records that beat it (cp) in front of its GC record and PageOuts, and
\* a fresh incarnation. With "compactcopyafter", the records go after them.
\* With "compactresend", a compaction that lost its race is sent again,
\* once, on the version the object is now, as if the records that beat it
\* were in its checkpoint. With "compactfail", one that lost reports the
\* commit failed. A compaction is not one of the commit's writes (cw, nw),
\* and no racer runs before it (Sched "race").
CompactCond(s) ==
  IF s.cv = None THEN [gen |-> s.lr.gen, len |-> s.lr.len, inc |-> s.lr.inc] ELSE s.cv
Compact(w, fk) ==
  LET s  == ws[w]
      cv == CompactCond(s)
      pl == IF s.pay # None THEN s.pay ELSE CompactPayload(w, s)
      x  == [strat |-> "CAS", cond |-> cv.gen,
             inc |-> IF s.crs = 0 THEN <<w, s.n, 0>> ELSE <<w, s.n, 0, s.crs>>,
             payload |-> [pl EXCEPT !.log = IF Has("compactcopyafter") THEN @ \o s.cp
                                            ELSE s.cp \o @]]
      ok == file.gen = x.cond
      lands == ok /\ fk \in {"none", "lostReply", "landedConflict", "landedThrottled"}
      lost == fk = "none" /\ ~ok
      s1 == [Faulted(s, fk) EXCEPT !.pay = None,
               !.cc = IF lands THEN Append(@, [before |-> file, after |-> Land(file, x)]) ELSE @]
  IN /\ CanStep(w) /\ s.pc = "compact" /\ (~Paging \/ s.pay # None)
     /\ fk \in WFaultsOf(w)
     /\ fk \in {"lostReply", "landedConflict", "landedThrottled", "inflight"} => ok
     /\ file' = IF lands THEN Land(file, x) ELSE file
     /\ inflight' = IF lands THEN {}
                    ELSE IF fk = "inflight" THEN inflight \cup {[w |-> w, id |-> None, x |-> x]}
                    ELSE inflight
     /\ UseFault(fk)
     /\ UNCHANGED now
     /\ UNCHANGED PageVars
     /\ IF lost /\ Has("compactresend") /\ ~s.cre
          THEN Go(w, [s1 EXCEPT !.lr = [@ EXCEPT !.gen = file.gen], !.cre = TRUE,
                                !.pay = s.pay], FALSE)
        ELSE IF lost /\ Has("compactfail") THEN End(w, s1, "failed", FALSE)
        ELSE IF lost /\ s.crs < CompactResends
          THEN Go(w, [s1 EXCEPT !.pc = "compactread", !.pay = pl, !.cv = cv], FALSE)
        ELSE End(w, s1, "committed", lands)

\* After its compaction lost its race, the writer reads the object after the
\* version the compaction was conditioned on (a tail read): if it is still
\* that incarnation and no shorter, appends beat the compaction, and the
\* writer may send it again (or not: the runtime's rule, which the model
\* takes both ways), conditioned on the version it read, with the records
\* that beat it copied in front of its GC record and PageOuts. They replay on
\* its checkpoint as they did on the version it checkpointed, and the GC and
\* PageOuts follow them (a PageOut whose block they changed is refused, its
\* page an orphan). If the object was replaced, or the read fails, the
\* compaction is dropped and the commit is committed. With
\* "compactreplaced", the read does not check the incarnation: a replace as
\* long reads as growth.
CompactRead(w, fk) ==
  LET s == ws[w]
      grown == (Has("compactreplaced") \/ file.inc = s.cv.inc) /\ FileLen(file) >= s.cv.len
      rest == SubSeq(file.log, s.cv.len, Len(file.log))
      s1 == [Faulted(s, fk) EXCEPT !.pay = None]
  IN /\ CanStep(w) /\ s.pc = "compactread"
     /\ fk \in {"none"} \cup (IF CanFault(w) THEN FaultKinds \cap {"readFail"} ELSE {})
     /\ UseFault(fk)
     /\ UNCHANGED <<file, inflight, now>>
     /\ UNCHANGED PageVars
     /\ IF fk = "none" /\ grown
          THEN \/ Go(w, [s EXCEPT !.pc = "compact", !.crs = @ + 1, !.cp = @ \o rest,
                                  !.cv = [gen |-> file.gen, len |-> FileLen(file),
                                          inc |-> file.inc]], FALSE)
               \/ End(w, s1, "committed", FALSE)
        ELSE End(w, s1, "committed", FALSE)

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
  /\ UNCHANGED PageVars

\* With Paging, before a CAS or a compaction: its payload is fixed (the GC
\* watermark and PageOuts, at the writer's clock), and the pages its
\* PageOuts seal are put, before the CAS is sent. A put that fails, or lands
\* with its reply lost: the CAS goes without its PageOuts, and a page that
\* landed is an orphan.
PrepCas(w, fk) ==
  LET s   == ws[w]
      pl  == IF s.pc = "compact" THEN CompactPayload(w, s)
             ELSE CasPayload(w, IF Has("12") THEN s.snap ELSE s.base, s.txn)
      pgs == CasPages(pl)
  IN /\ Paging /\ CanStep(w) /\ (CasDue(s) \/ s.pc = "compact") /\ s.pay = None
     /\ fk \in {"none"} \cup (IF CanFault(w) /\ pgs # {} THEN FaultKinds \cap PutFaults ELSE {})
     /\ UseFault(fk)
     /\ store' = IF fk = "putFail" THEN store ELSE store \cup pgs
     \* The state's pages the checkpoint names are put first (LogRuntime.checkpoint);
     \* with "rootfirst", a root goes without them
     /\ spages' = IF Has("rootfirst") THEN spages ELSE spages \cup pl.chk.st.sealed
     /\ ws' = [ws EXCEPT ![w] =
                 [Faulted(s, fk) EXCEPT
                    !.pay = IF fk = "none" THEN pl
                            ELSE [pl EXCEPT !.log = SelectSeq(@, LAMBDA r : ~IsPo(r))]]]
     /\ UNCHANGED <<file, inflight, now, ended, res, ook, sched, nsw>>

\* The state's pages garbage under view a (CatalogStateMachine.garbagePages): sealed
\* at a version a has reached and not named by it. A page sealed after a, or
\* named by it, stays. With "sweepstale", every page a does not name.
StateGarbage(a) ==
  {p \in spages : p \notin a.st.sealed /\ (Has("sweepstale") \/ p.v <= a.st.ver[p.e])}

\* A sweep of the state's pages from a writer's view, however stale (its
\* snapshot): LogRuntime.sweep(replica).
SweepState(w) ==
  LET a == ws[w].snap
      del == IF a = None THEN {} ELSE StateGarbage(a)
  IN /\ SealEvery > 0 /\ nsw < MaxSweep /\ del # {}
     /\ spages' = spages \ del
     /\ nsw' = nsw + 1
     /\ UNCHANGED <<file, inflight, now, nf, ws, ended, res, ook, sched, store>>

\* A sweep (any compactor) lists the page store and deletes every page the
\* live catalog does not reference whose node ends at or below its floor:
\* no PageOut can name it again. With "sweepabove", every page it does not
\* reference. Deleting the pages a CAS's GC dropped, once it has landed, is
\* such a sweep.
Sweep ==
  LET del == {pg \in store : (\A p \in LiveR.P : PageName(p) # pg)
                             /\ (Has("sweepabove") \/ Top(pg) <= LiveR.F)}
  IN /\ Paging /\ nsw < MaxSweep /\ del # {}
     /\ store' = store \ del
     /\ nsw' = nsw + 1
     /\ UNCHANGED <<file, inflight, now, nf, ws, ended, res, ook, sched, spages>>

Tick ==
  /\ (Retention >= 0 \/ Paging) /\ now < MaxTime
  /\ now' = now + 1
  /\ UNCHANGED <<file, inflight, nf, ws, ended, res, ook, sched>>
  /\ UNCHANGED PageVars

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
  /\ store = {}
  /\ nsw = 0
  /\ spages = {}

WriterNext(w) ==
  \/ Begin(w)
  \/ Start(w)
  \/ BeginNested(w)
  \/ \E fk \in WriteFaults \cup {"none"} : Write(w, fk)
  \/ \E fk \in ReadFaults \cup {"none"} :
       ReadBack(w, fk) \/ Rebase(w, fk) \/ Refresh(w, fk) \/ Shrink(w, fk) \/ TailRead(w, fk)
         \/ FreshForCAS(w, fk) \/ Resolve(w, fk) \/ FinalRead(w, fk) \/ Restage(w, fk)
  \/ Exhaust(w)
  \/ \E fk \in WriteFaults \cup {"none"} : Compact(w, fk)
  \/ \E fk \in ReadFaults \cup {"none"} : CompactRead(w, fk)
  \/ \E fk \in PutFaults \cup {"none"} : PrepCas(w, fk)
  \/ SweepState(w)

Next ==
  \/ \E w \in Writers : WriterNext(w)
  \/ \E i \in inflight : LandInflight(i)
  \/ Sweep
  \/ Tick
  \/ Done

Spec == Init /\ [][Next]_vars

\* Writers keep taking steps; storage stops failing (the fault budget).
FairSpec == Spec /\ \A w \in Writers : WF_vars(WriterNext(w))

-----------------------------------------------------------------------------
(* Properties *)

Hist == LiveR.st.hist

\* Every version of the catalog replays (Bug 64: one record at the horizon
\* failed every read), and its frames pass a read's checks: each names the
\* object (format 3; "probeinc" and "lengthonly" frame one with another's).
Readable == ~LiveR.err /\ FramesTrue

\* No writer holds a replica of a state the object never held: a torn read
\* ("notrailer") or a fold of another incarnation's records ("lengthonly").
NoUnheldState == \A w \in Writers : ws[w].base # None => ws[w].base.held

\* No transaction applies twice.
ExactlyOnce == \A i, j \in 1..Len(Hist) : i # j => Hist[i].id # Hist[j].id

\* The catalog is the serial execution of the transactions applied
\* (RSM's SerialFold).
Serializable == SerialFold(InitSt, Hist) = [ok |-> TRUE, st |-> LiveR.st]

\* S3, of the state replay reaches.
StampsCount == CountsChanges(LiveR.st)

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
\* applies the same records. A compaction sent again checkpoints a prefix of
\* that version (the one it lost on), and its log begins with the rest.
CheckpointIsReplay ==
  [][(file'.gen # file.gen /\ file'.segs = 1) =>
       \E k \in 0..Len(file.log) :
         /\ file'.chk = Ck(Replay([file EXCEPT !.log = SubSeq(@, 1, k)]))
         /\ Len(file'.log) >= Len(file.log) - k
         /\ SubSeq(file'.log, 1, Len(file.log) - k) = SubSeq(file.log, k + 1, Len(file.log))]_file

\* Pages (COMMITTED_PAGING.md, oracle 2): every page the catalog references
\* is in the store, and holds exactly the ids that applied in its range.
\* The set keeps its shape; every stamp lies on the grid.
PagesExist == PagesExistIn(LiveR, store)

\* The state's pages (SNAPSHOT_PAGING.md "Testing", oracle 1): every page the
\* catalog object's root names, and the catalog still names, is in the store: a
\* compactor puts a root's pages before writing it, and a sweep deletes none a
\* reader of the object may need. A page a record after the root dropped (an
\* expiry that emptied its block) is garbage once that record is in the log: a
\* reader replays it before it loads; one that loaded before fails, as Iceberg's
\* readers do once an expiry deletes a manifest list. A page the catalog names
\* that the root does not is pending: the next root's compactor puts it.
StatePagesExist == \A p \in file.chk.st.sealed \cap LiveR.st.sealed : p \in spages
PagesExact == PagesExactIn(LiveR, Hist)
Shape == SetShape(LiveR)
OnGrid == OnGridIn(file.log)

Safety ==
  Readable /\ NoUnheldState /\ ExactlyOnce /\ Serializable /\ UniqueIds
    /\ HonestCommitted /\ HonestFailed /\ UnknownExplained /\ PagesExist /\ PagesExact
    /\ StatePagesExist

\* Every writer finishes.
Termination == <>[](\A w \in Writers : ws[w].pc = "idle" /\ ws[w].k > Len(Shapes[w]))

=============================================================================
