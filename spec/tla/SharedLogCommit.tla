-------------------------- MODULE SharedLogCommit --------------------------
(***************************************************************************)
(* The commit protocol over a shared log (intentlog-core's SharedLog), as  *)
(* a provider without fused replace has it: Kafka, SQS FIFO with an        *)
(* archiver, a ledger, InMemorySharedLog. Writers append intention records *)
(* unconditionally and read through them (LogRuntime's commit loop, on its *)
(* append branch); compactors append GC records, store checkpoints apart   *)
(* from the log, and trim it (LogRuntime.compact). The state machine and   *)
(* replay are RSM's.                                                       *)
(*                                                                         *)
(* The log is one total order of records (P1). `log` is all of it, the     *)
(* trimmed prefix included, as a ghost; a reader sees only what lies after *)
(* the checkpoint it starts from, and may start from any checkpoint that   *)
(* covers the trimmed prefix (a usable one). With Lag, a read returns a    *)
(* prefix of the log after its checkpoint (a replica that lags).           *)
(*                                                                         *)
(* Each writer or compactor action is one request to the log, so the       *)
(* others interleave between any two. Bug switches put back protocol bugs  *)
(* this kind of log admits, to calibrate the properties.                   *)
(***************************************************************************)
EXTENDS RSM, TLC

CONSTANTS
  Writers,      \* writer ids
  Shapes,       \* Writers -> Seq of [rd, wr: SUBSET Entities, cr: Entities \cup {None}]
  MaxAttempts,  \* commit loop attempts (code: 10)
  MaxThrottle,  \* attempts of a write the log throttles (code: 8)
  Retention,    \* committed retention, in ticks (-1: no GC records)
  MaxTime,      \* bound on the clock
  Skew,         \* Writers -> Nat: each writer's clock is now + Skew[w]
  CompSkews,    \* the compactors' clock skews: a compactor's clock is now + one of them
  MaxGc,        \* how many GC records compactors append in a run
  MaxCkpt,      \* how many checkpoints they store
  MaxTrim,      \* how many times they trim
  MaxFaults,    \* how many faults the log may inject in a run
  FaultKinds,   \* the faults it may inject (see Send and the reads)
  Lag           \* a read may return any prefix of the log after its checkpoint
                \* Bugs (RSM): protocol bugs put back, by name

WriteFaults == {"failBefore", "notSent", "lostReply", "throttled", "landedThrottled",
                "inflight"}
ReadFaults == {"readFail"}

ASSUME /\ FaultKinds \subseteq WriteFaults \cup ReadFaults
       /\ MaxAttempts >= 1 /\ MaxThrottle >= 1
       /\ Bugs \subseteq {"ckclock", "setonly", "trimearly", "noverify", "freshid", "p3",
                          "dup", "lagread", "basever"}

VARIABLES
  log,       \* every record appended, in order (the trimmed ones too)
  ckpts,     \* the checkpoints stored: [pos, st, C, H], the state through log[pos]
  trimmed,   \* the log is trimmed through this position
  inflight,  \* appends sent, not landed, not yet failed: a timed-out request
  now,       \* the clock
  nf,        \* faults injected so far
  comp,      \* what compactors did so far: [gc, ck, tr]
  ws,        \* Writers -> the writer's commit (see Idle)
  ended,     \* the outcomes reported, per commit: [c, id, out, cf, cx, ci]
  ook        \* every oracle check so far passed (SharedLogOracles)

vars == <<log, ckpts, trimmed, inflight, now, nf, comp, ws, ended, ook>>

-----------------------------------------------------------------------------
(* Reads *)

InitCk == [pos |-> 0, st |-> InitSt, C |-> {}, H |-> 0]

\* The checkpoints a reader can start from: those that cover the trimmed
\* prefix (P5 keeps one).
Usable == {c \in ckpts : c.pos >= trimmed}

\* A read from checkpoint c through position q, watching the record of
\* `watch`: replay of c and log[c.pos + 1 .. q].
ReadFrom(c, q, watch) ==
  LET a == Fold(Acc(c.st, c.C, c.H, watch), SubSeq(log, c.pos + 1, q))
  IN [st |-> a.st, C |-> a.C, H |-> a.H, out |-> a.out, q |-> q]

\* Where a read from c may end: the tail, or with Lag any position after c.
Ends(c) == IF Lag THEN c.pos..Len(log) ELSE {Len(log)}

\* The whole log replayed from the start (a ghost: no reader can after a
\* trim). The properties are stated over it.
Full == Fold(Acc(InitSt, {}, 0, None), log)

-----------------------------------------------------------------------------
(* Writers *)

Clock(w) == 1 + now + Skew[w]

\* An idle writer, about to stage its k-th transaction as its n-th commit.
Idle(k, n) ==
  [pc |-> "idle", k |-> k, n |-> n, txn |-> None, amb |-> FALSE, dup |-> FALSE, att |-> 0,
   thr |-> 0, base |-> 0, pos |-> 0, nw |-> 0,
   cf |-> FALSE, cx |-> FALSE, ci |-> FALSE, lg |-> FALSE]
\* pc: idle, send, throttled, readback, resolve.
\* amb: a write's outcome was unknown (ambiguous); dup: a send the log
\*   throttled may have landed, so the record may be in the log twice; att:
\*   the loop's attempts; base: where the read the commit staged on ended;
\*   thr: sends of the current write the log throttled; pos: where the last
\*   acknowledged append landed (its receipt); nw: the appends sent.
\* Ghosts: cf, a fault was injected into this commit; cx, a read of it had
\*   its id at or below the horizon; ci, a record it did not see landed
\*   before it ended (a race); lg, a read of it lagged the log.

\* The other writers see the log change under them.
Upd(w, s2, landed) ==
  [v \in Writers |-> IF v = w THEN s2
                     ELSE IF landed /\ ws[v].pc # "idle" THEN [ws[v] EXCEPT !.ci = TRUE]
                     ELSE ws[v]]

CanFault == nf < MaxFaults
WFaults == {"none"} \cup (IF CanFault THEN FaultKinds \cap WriteFaults ELSE {})
RFaults == {"none"} \cup (IF CanFault THEN FaultKinds \cap ReadFaults ELSE {})
UseFault(fk) == nf' = IF fk = "none" THEN nf ELSE nf + 1

Faulted(s, fk) == [s EXCEPT !.cf = @ \/ fk # "none"]
Crossed(R, t) == R.H > 0 /\ t.id.ts <= R.H

\* The commit continues as s2.
Go(w, s2, landed) ==
  /\ ws' = Upd(w, s2, landed)
  /\ UNCHANGED <<ended, ook>>

\* The oracle's verdict on w's commit s as it ends with outcome out.
\* SharedLogOracles defines them; a configuration substitutes one
\* (OracleAtEnd <- OracleNow).
OracleAtEnd(w, s, out) == TRUE

\* The commit ends with outcome out: committed (returned), failed
\* (CommitRejectedException), error (another failure, not
\* CommitOutcomeUnknownException) or unknown (CommitOutcomeUnknownException).
End(w, s, out) ==
  /\ ended' = ended \cup {[c |-> <<w, s.n>>, id |-> s.txn.id, out |-> out, cf |-> s.cf,
                           cx |-> s.cx, ci |-> s.ci]}
  /\ ws' = Upd(w, Idle(s.k + 1, s.n + 1), FALSE)
  /\ ook' = (ook /\ OracleAtEnd(w, s, out))

\* catch (AmbiguousWrite): resolve by reading.
Ambig(w, s, landed) == Go(w, [s EXCEPT !.amb = TRUE, !.pc = "resolve"], landed)

\* A write of this commit other than the one acknowledged may have landed:
\* one whose outcome was unknown, or a send the log throttled. An
\* unconditional append's retry can land beside it (on one object, a retry
\* fails its condition instead, P4), and a view through the acknowledged
\* one may start from a checkpoint that holds the other.
Earlier(s) == s.amb \/ (s.dup /\ ~Has("dup"))

\* landed(): what a read R (watching the commit's record) says of it. With
\* "setonly", the committed set alone decides, as on one object.
Judge(w, s, R, then) ==
  LET c == IF Has("setonly") THEN Contains(R.C, R.H, s.txn.id) ELSE Landed(R, s.txn.id) IN
  CASE c = "yes" -> End(w, s, "committed")
    [] c = "unknown" -> End(w, s, IF Earlier(s) THEN "unknown" ELSE "failed")
    [] OTHER -> then

\* The transaction staged for shape sh on a read R, as w's n-th commit. Its
\* id is minted after the horizon of R (Bug 64 rule 3); k counts the ids a
\* commit took ("freshid").
Stage(w, sh, R, n) ==
  LET rd == [e \in sh.rd |-> R.st.ver[e]] IN
  [id |-> [w |-> w, n |-> n, ts |-> Max(Clock(w), R.H + 1), k |-> 0],
   rd |-> rd, ird |-> rd, wr |-> sh.wr, cr |-> sh.cr, cid |-> 0,
   wv |-> [e \in sh.wr |-> R.st.ver[e]]]

\* Stage the next transaction on a read of the log, from any usable
\* checkpoint. A read that lags misses records that have landed: a race.
Begin(w) ==
  LET s == ws[w] IN
  /\ s.pc = "idle" /\ s.k <= Len(Shapes[w])
  /\ \E c \in Usable : \E q \in Ends(c) :
       LET R == ReadFrom(c, q, None) IN
       ws' = [ws EXCEPT ![w] = [s EXCEPT !.pc = "send", !.txn = Stage(w, Shapes[w][s.k], R, s.n),
                                         !.base = q, !.ci = q < Len(log), !.lg = q < Len(log)]]
  /\ UNCHANGED <<log, ckpts, trimmed, inflight, now, nf, comp, ended, ook>>

\* append(record, null), through write()'s throttle retries: one request.
\* Faults: failBefore (an exception before landing), notSent
\* (LogNotSentException: nothing landed), lostReply (landed, reply lost),
\* throttled (not landed), landedThrottled (landed, then throttled),
\* inflight (timed out, still in flight: it may land later, LandInflight).
Send(w, fk) ==
  LET s == ws[w]
      lands == fk \in {"none", "lostReply", "landedThrottled"}
      log2 == IF lands THEN Append(log, s.txn) ELSE log
      thr0 == IF s.pc = "throttled" THEN s.thr ELSE 0
      s1 == [Faulted(s, fk) EXCEPT !.att = IF s.pc = "send" THEN @ + 1 ELSE @,
                                   !.nw = @ + 1, !.thr = 0]
  IN /\ s.pc \in {"send", "throttled"}
     /\ s.pc = "send" => s.att < MaxAttempts
     /\ fk \in WFaults
     /\ UseFault(fk)
     /\ log' = log2
     /\ inflight' = IF fk = "inflight"
                      THEN inflight \cup {[w |-> w, rec |-> s.txn, seq |-> s.nw]}
                    ELSE inflight
     /\ UNCHANGED <<ckpts, trimmed, now, comp>>
     /\ CASE fk = "none" -> Go(w, [s1 EXCEPT !.pc = "readback", !.pos = Len(log2)], TRUE)
          [] fk = "notSent" ->
               \* nothing landed: rejected (NOT_SENT), unless an earlier write may
               \* have, or an earlier send of this one was throttled
               IF s.pc = "throttled" THEN Ambig(w, s1, FALSE)
               ELSE IF s.amb THEN End(w, s1, "unknown") ELSE End(w, s1, "error")
          [] fk \in {"failBefore", "lostReply", "inflight"} -> Ambig(w, s1, lands)
          [] OTHER ->                     \* throttled, landedThrottled
               IF thr0 + 1 < MaxThrottle
                 THEN Go(w, [s1 EXCEPT !.pc = "throttled", !.thr = thr0 + 1, !.dup = TRUE],
                         lands)
               ELSE Ambig(w, s1, lands)

\* readThrough(receipt): a view whose records include the one appended (P3):
\* from a usable checkpoint before it. None if the log was trimmed past it
\* (TrimmedException), and a failed read: the write is ambiguous. With
\* "p3", the view may start from a checkpoint that already covers the record.
ReadBack(w, fk) ==
  LET s == ws[w]
      cs == {c \in Usable : Has("p3") \/ c.pos < s.pos}
      s1 == Faulted(s, fk)
  IN /\ s.pc = "readback"
     /\ fk \in RFaults
     /\ UseFault(fk)
     /\ UNCHANGED <<log, ckpts, trimmed, inflight, now, comp>>
     /\ IF fk = "readFail" \/ cs = {} THEN Ambig(w, s1, FALSE)
        ELSE \E c \in cs, q \in s.pos..Len(log) :
               LET R == ReadFrom(c, Max(q, c.pos), s.txn.id)
                   s2 == [s1 EXCEPT !.cx = @ \/ Crossed(R, s.txn), !.lg = @ \/ q < Len(log)]
               IN /\ Lag \/ q = Len(log)
                  \* replay rejected the record: a concurrent commit invalidated it
                  /\ Judge(w, s2, R, End(w, s2, "failed"))

\* After an ambiguous write: a fresh read decides. If the record is not
\* there, send it again if the transaction holds on the read (a copy still
\* in flight carries the same id, and replay applies one of them), else
\* fail: replay rejects every copy from here on (S2). A failed read leaves
\* the outcome unknown. The read must reach the one the commit staged on:
\* S2 holds from there, and every copy lands after it; on a read that lags
\* behind it ("lagread"), the transaction may not hold yet. With
\* "noverify", the commit fails without asking whether the transaction
\* holds; with "freshid", the retry takes a new id.
Resolve(w, fk) ==
  LET s == ws[w] IN
  /\ s.pc = "resolve"
  /\ fk \in RFaults
  /\ UseFault(fk)
  /\ UNCHANGED <<log, ckpts, trimmed, inflight, now, comp>>
  /\ IF fk = "readFail" \/ Usable = {} THEN End(w, Faulted(s, fk), "unknown")
     ELSE \E c \in Usable : \E q \in Ends(c) :
            (q >= s.base \/ Has("lagread")) /\
            LET R == ReadFrom(c, q, s.txn.id)
                s1 == [Faulted(s, fk) EXCEPT !.cx = @ \/ Crossed(R, s.txn),
                                             !.lg = @ \/ q < Len(log)]
                id2 == [s.txn.id EXCEPT !.ts = Max(Clock(w), R.H + 1), !.k = @ + 1]
                again == [s1 EXCEPT !.pc = "send",
                                    !.txn = IF Has("freshid") THEN [@ EXCEPT !.id = id2] ELSE @]
            IN Judge(w, s1, R,
                 IF ~Has("noverify") /\ Verify(R.st, s.txn) THEN Go(w, again, FALSE)
                 ELSE End(w, s1, "failed"))

\* Out of attempts, after an ambiguous write (the loop only goes round
\* again after one): unknown.
Exhaust(w) ==
  LET s == ws[w] IN
  /\ s.pc = "send" /\ s.att >= MaxAttempts
  /\ UNCHANGED <<log, ckpts, trimmed, inflight, now, nf, comp>>
  /\ End(w, s, IF s.amb THEN "unknown" ELSE "failed")

\* A timed-out append lands after all, at the tail.
LandInflight(i) ==
  /\ i \in inflight
  /\ log' = Append(log, i.rec)
  /\ inflight' = inflight \ {i}
  /\ ws' = [v \in Writers |->
              IF ws[v].pc # "idle" /\ (v # i.w \/ ws[v].txn.id # i.rec.id)
                THEN [ws[v] EXCEPT !.ci = TRUE] ELSE ws[v]]
  /\ UNCHANGED <<ckpts, trimmed, now, nf, comp, ended, ook>>

-----------------------------------------------------------------------------
(* Compactors: LogRuntime.compact, a step at a time, in any order *)

\* A compactor appends a GC record at its clock less the retention.
AppendGc(sk) ==
  LET wm == 1 + now + sk - Retention IN
  /\ Retention >= 0 /\ comp.gc < MaxGc /\ wm > 0
  /\ log' = Append(log, [wm |-> wm])
  /\ comp' = [comp EXCEPT !.gc = @ + 1]
  /\ ws' = [v \in Writers |-> IF ws[v].pc # "idle" THEN [ws[v] EXCEPT !.ci = TRUE] ELSE ws[v]]
  /\ UNCHANGED <<ckpts, trimmed, inflight, now, nf, ended, ook>>

\* putCheckpoint(p): the replay of the log through p, read from a usable
\* checkpoint before it. With "ckclock", the checkpoint drops the ids its
\* compactor's clock says are past the retention, as the file's checkpoint
\* did before GC records: no record says so.
PutCheckpoint(p, c, sk) ==
  LET R == ReadFrom(c, p, None)
      g == IF Has("ckclock") /\ Retention >= 0 THEN Gc(R, Max(0, 1 + now + sk - Retention))
           ELSE R
  IN /\ comp.ck < MaxCkpt
     /\ c \in Usable /\ c.pos < p /\ p <= Len(log)
     /\ \A d \in ckpts : d.pos # p
     /\ ckpts' = ckpts \cup {[pos |-> p, st |-> g.st, C |-> g.C, H |-> g.H]}
     /\ comp' = [comp EXCEPT !.ck = @ + 1]
     /\ UNCHANGED <<log, trimmed, inflight, now, nf, ws, ended, ook>>

\* trim(p): only through records a stored checkpoint covers (P5); with
\* "trimearly", before one does.
Trim(p) ==
  /\ comp.tr < MaxTrim
  /\ trimmed < p /\ p <= Len(log)
  /\ Has("trimearly") \/ \E c \in ckpts : c.pos >= p
  /\ trimmed' = p
  /\ comp' = [comp EXCEPT !.tr = @ + 1]
  /\ UNCHANGED <<log, ckpts, inflight, now, nf, ws, ended, ook>>

Tick ==
  /\ now < MaxTime
  /\ now' = now + 1
  /\ UNCHANGED <<log, ckpts, trimmed, inflight, nf, comp, ws, ended, ook>>

Done ==
  /\ \A w \in Writers : ws[w].pc = "idle" /\ ws[w].k > Len(Shapes[w])
  /\ UNCHANGED vars

Init ==
  /\ log = << >>
  /\ ckpts = {InitCk}
  /\ trimmed = 0
  /\ inflight = {}
  /\ now = 0
  /\ nf = 0
  /\ comp = [gc |-> 0, ck |-> 0, tr |-> 0]
  /\ ws = [w \in Writers |-> Idle(1, 1)]
  /\ ended = {}
  /\ ook = TRUE

WriterNext(w) ==
  \/ Begin(w)
  \/ \E fk \in WriteFaults \cup {"none"} : Send(w, fk)
  \/ \E fk \in ReadFaults \cup {"none"} : ReadBack(w, fk) \/ Resolve(w, fk)
  \/ Exhaust(w)

CompactorNext ==
  \/ \E sk \in CompSkews : AppendGc(sk)
  \/ \E p \in 1..Len(log), c \in ckpts, sk \in CompSkews : PutCheckpoint(p, c, sk)
  \/ \E p \in 1..Len(log) : Trim(p)

Next ==
  \/ \E w \in Writers : WriterNext(w)
  \/ CompactorNext
  \/ \E i \in inflight : LandInflight(i)
  \/ Tick
  \/ Done

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

Hist == Full.st.hist

\* The commits a history applied: a commit is a writer's n-th, whatever ids
\* it took.
CommitOf(r) == <<r.id.w, r.id.n>>
CommitsOf(h) == {CommitOf(h[i]) : i \in 1..Len(h)}

\* Some checkpoint covers the trimmed prefix, so the log can be read.
Readable == Usable # {}

\* Every reader, from whichever usable checkpoint, replays the log to the
\* same state, committed set and horizon.
Deterministic ==
  \A c1, c2 \in Usable :
    LET R1 == ReadFrom(c1, Len(log), None)
        R2 == ReadFrom(c2, Len(log), None)
    IN R1.st = R2.st /\ R1.C = R2.C /\ R1.H = R2.H

\* A checkpoint at p is the replay of the log through p (I11).
CheckpointIsReplay ==
  \A c \in ckpts :
    LET R == ReadFrom(InitCk, c.pos, None)
    IN [st |-> R.st, C |-> R.C, H |-> R.H] = [st |-> c.st, C |-> c.C, H |-> c.H]

\* No commit applies twice, whatever ids it took.
ExactlyOnce == \A i, j \in 1..Len(Hist) : i # j => CommitOf(Hist[i]) # CommitOf(Hist[j])

\* The state is the serial execution of the transactions applied.
Serializable == SerialFold(InitSt, Hist) = [ok |-> TRUE, st |-> Full.st]

\* S3, of the state replay reaches.
StampsCount == CountsChanges(Full.st)

\* No id names two entities.
UniqueIds == \A m1, m2 \in Full.st.made : m1.id = m2.id => m1 = m2

\* Honest outcomes (I10): a commit reported committed applied, and stays
\* applied (through GC, checkpoints and trims); one reported failed (or any
\* error but unknown) is not applied, now or later (a late landing
\* included); one reported unknown follows a fault or a read that put its
\* id at or below the horizon.
HonestCommitted == \A e \in ended : e.out = "committed" => e.c \in CommitsOf(Hist)
HonestFailed == \A e \in ended : e.out \in {"failed", "error"} => e.c \notin CommitsOf(Hist)
UnknownExplained == \A e \in ended : e.out = "unknown" => e.cf \/ e.cx

\* A commit no other record raced, on a log that did not fail, commits.
\* Checkpoints and trims are no race; a read that lags is.
SoloCommits == \A e \in ended : (~e.cf /\ ~e.ci) => e.out = "committed"

\* What history the log holds only grows, by one transaction at a time.
HistGrows ==
  [][LET h2 == Fold(Acc(InitSt, {}, 0, None), log').st.hist IN
       /\ Len(h2) >= Len(Hist) /\ SubSeq(h2, 1, Len(Hist)) = Hist
       /\ Len(h2) <= Len(Hist) + 1]_log

Safety ==
  Readable /\ Deterministic /\ CheckpointIsReplay /\ ExactlyOnce /\ Serializable
    /\ UniqueIds /\ HonestCommitted /\ HonestFailed /\ UnknownExplained

=============================================================================
