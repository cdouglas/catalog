-------------------------- MODULE SharedLogCommit --------------------------
(***************************************************************************)
(* The commit protocol over a shared log (intentlog-core's SharedLog), as  *)
(* a provider without fused replace has it: Kafka, SQS FIFO with an        *)
(* archiver, a ledger, InMemorySharedLog. Writers append intention records *)
(* unconditionally and read through them (LogRuntime's commit loop, on its *)
(* append branch); compactors append GC records, store checkpoints apart   *)
(* from the log, and trim it (LogRuntime.compact). The state machine and   *)
(* replay are RSM's. Compactors also page the committed set, as            *)
(* docs/COMMITTED_PAGING.md designs it: they put a page, then append the   *)
(* PageOut that seals its block; sweeps delete pages no PageOut can name.  *)
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
(*                                                                         *)
(* With Cond, a writer's appends are conditional on the tail it holds (a   *)
(* log that cannot replace itself, P4): a lost one is unresolved until a   *)
(* read past the tail it named settles it (Bug 90). An append may also     *)
(* land void, past the end of its segment (docs/LOG_SEGMENTS.md D4):       *)
(* nothing landed, and the writer knows it. Writers' catalogs also read    *)
(* the log outside a commit (Look), never going back past what they read   *)
(* or committed in (Bug 72).                                               *)
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
  FaultKinds,   \* the faults it may inject (see Send, PutPage and the reads)
  Lag,          \* a read may return any prefix of the log after its checkpoint
  Admission,    \* A, in ticks: a compactor pages blocks whose top is A old
  MaxPage,      \* how many pages compactors put in a run (0: no paging)
  MaxSweep,     \* how many sweeps of the page store run
  Cond,         \* appends are conditional on the tail the writer holds
  MaxLooks      \* how many reads each writer's catalog makes outside a commit
                \* Bugs (RSM): protocol bugs put back, by name
                \* BlockIds, GridBits (RSM): the committed set's blocks

WriteFaults == {"failBefore", "notSent", "lostReply", "throttled", "landedThrottled",
                "inflight", "void", "condLanded"}
ReadFaults == {"readFail", "pageFail"}
PutFaults == {"putFail", "putLost"}

ASSUME /\ FaultKinds \subseteq WriteFaults \cup ReadFaults \cup PutFaults
       /\ MaxAttempts >= 1 /\ MaxThrottle >= 1
       /\ Cond \in BOOLEAN /\ MaxLooks \in Nat
       /\ "condLanded" \in FaultKinds => Cond
       /\ Bugs \subseteq {"ckclock", "setonly", "trimearly", "noverify", "freshid", "p3",
                          "dup", "lagread", "basever",
                          "floorgap", "nodigest", "absentpage", "sweepabove", "mintH",
                          "missingfail", "nojoint",
                          "viewnotable", "tablenoview", "viewdropnoremovals", "viewnobump", "viewstale",
                          "absentonly", "noabsence", "dropnobump", "renamenobump",
                          "voidunres", "voidlands", "lagsettle", "readsgoback"}

VARIABLES
  log,       \* every record appended, in order (the trimmed ones too)
  ckpts,     \* the checkpoints stored: [pos, st, C, H, F, P], the state, committed set,
             \* horizon, floor and page references through log[pos]
  trimmed,   \* the log is trimmed through this position
  inflight,  \* appends sent, not landed, not yet failed: a timed-out request
  now,       \* the clock
  nf,        \* faults injected so far
  comp,      \* what compactors did so far: [gc, ck, tr, pg, sw], the GC records,
             \* checkpoints, trims, page puts and sweeps
  ws,        \* Writers -> the writer's commit (see Idle)
  ended,     \* the outcomes reported, per commit: [c, id, out, cf, cx, ci]
  ook,       \* every oracle check so far passed (SharedLogOracles)
  store,     \* the page store: the names of the pages in it
  pend,      \* PageOuts whose pages were put, not yet sent (one never sent: a crash)
  looks      \* Writers -> what the writer's catalog read outside a commit (see Look)

vars == <<log, ckpts, trimmed, inflight, now, nf, comp, ws, ended, ook, store, pend, looks>>
PageVars == <<store, pend>>

-----------------------------------------------------------------------------
(* Reads *)

InitCk == [pos |-> 0, st |-> InitSt, C |-> {}, H |-> 0, F |-> 0, P |-> {}]

\* The checkpoints a reader can start from: those that cover the trimmed
\* prefix (P5 keeps one).
Usable == {c \in ckpts : c.pos >= trimmed}

\* A read from checkpoint c through position q, watching the record of
\* `watch`: replay of c and log[c.pos + 1 .. q].
ReadFrom(c, q, watch) ==
  LET a == Fold(Acc(c.st, c, watch), SubSeq(log, c.pos + 1, q))
  IN [st |-> a.st, C |-> a.C, H |-> a.H, F |-> a.F, P |-> a.P, out |-> a.out, q |-> q]

\* A read from c served with a gap of g records after it (a log that trimmed
\* past its checkpoint, or lost records). The view says where its records
\* start (LogView.start), c.pos + g, and the checkpoint names c.pos
\* (LogCheckpoint.through): the runtime fails the read unless g = 0 (S4). With
\* "nojoint" it replays it.
ReadFromGap(c, q, watch, g) ==
  LET a == Fold(Acc(c.st, c, watch), SubSeq(log, c.pos + 1 + g, q))
  IN [st |-> a.st, C |-> a.C, H |-> a.H, F |-> a.F, P |-> a.P, out |-> a.out, q |-> q]
\* The gaps a read from c through q may be served with and still be used.
Gaps(c, q) == IF Has("nojoint") /\ c.pos + 1 < q THEN {0, 1} ELSE {0}

\* Where a read from c may end: the tail, or with Lag any position after c.
Ends(c) == IF Lag THEN c.pos..Len(log) ELSE {Len(log)}

\* The whole log replayed from the start (a ghost: no reader can after a
\* trim). The properties are stated over it.
Full == Fold(Acc(InitSt, InitSet, None), log)

\* How many transactions the log through position q applied (a ghost).
HistAt(q) == Len(ReadFrom(InitCk, q, None).st.hist)

-----------------------------------------------------------------------------
(* Writers *)

Clock(w) == 1 + now + Skew[w]

\* An idle writer, about to stage its k-th transaction as its n-th commit.
Idle(k, n) ==
  [pc |-> "idle", k |-> k, n |-> n, txn |-> None, amb |-> FALSE, dup |-> FALSE, att |-> 0,
   thr |-> 0, base |-> 0, pos |-> 0, nw |-> 0, at |-> 0, ur |-> FALSE, la |-> -1,
   cf |-> FALSE, cx |-> FALSE, ci |-> FALSE, lg |-> FALSE, cl |-> FALSE, ul |-> FALSE,
   vd |-> FALSE]
\* pc: idle, send, throttled, readback, resolve.
\* amb: a write's outcome was unknown (ambiguous); dup: a send the log
\*   throttled may have landed, so the record may be in the log twice; att:
\*   the loop's attempts; base: where the read the commit staged on ended;
\*   thr: sends of the current write the log throttled; pos: where the last
\*   acknowledged append landed (its receipt); nw: the appends sent; at: the
\*   tail its next send names (with Cond); ur: a conditional send lost, and
\*   no read has settled whether it landed (unresolved); la: the furthest
\*   tail such a send named (lostAt), -1 for none.
\* Ghosts: cf, a fault was injected into this commit; cx, a read of it had
\*   its id at or below the horizon; ci, a record it did not see landed
\*   before it ended (a race); lg, a read of it lagged the log; cl, a
\*   conditional send of it lost a race; ul, its last read lagged behind a
\*   lost send's tail after one did; vd, a send of it landed void.

\* The other writers see the log change under them.
Upd(w, s2, landed) ==
  [v \in Writers |-> IF v = w THEN s2
                     ELSE IF landed /\ ws[v].pc # "idle" THEN [ws[v] EXCEPT !.ci = TRUE]
                     ELSE ws[v]]

CanFault == nf < MaxFaults
WFaults == {"none"} \cup (IF CanFault THEN FaultKinds \cap WriteFaults ELSE {})
RFaults == {"none"} \cup (IF CanFault THEN FaultKinds \cap ReadFaults ELSE {})
UseFault(fk) == nf' = IF fk = "none" THEN nf ELSE nf + 1

\* A void append is a lost race, not a fault: nothing landed, and the writer
\* knows it.
Faulted(s, fk) == [s EXCEPT !.cf = @ \/ fk \notin {"none", "void"}]
\* Whether a writer keeps the tail its sends name: with conditional appends,
\* or where a void send is recorded as a conditional loss ("voidunres").
TrackAt == Cond \/ Has("voidunres")
Crossed(R, t) == R.H > 0 /\ t.id.ts <= R.H

\* The commit continues as s2.
Go(w, s2, landed) ==
  /\ ws' = Upd(w, s2, landed)
  /\ UNCHANGED <<ended, ook, looks>>

\* The oracle's verdict on w's commit s as it ends with outcome out.
\* SharedLogOracles defines them; a configuration substitutes one
\* (OracleAtEnd <- OracleNow).
OracleAtEnd(w, s, out) == TRUE

\* The commit ends with outcome out: committed (returned), failed
\* (CommitRejectedException), error (another failure, not
\* CommitOutcomeUnknownException) or unknown (CommitOutcomeUnknownException).
End(w, s, out) ==
  /\ ended' = ended \cup {[c |-> <<w, s.n>>, id |-> s.txn.id, out |-> out, cf |-> s.cf,
                           cx |-> s.cx, ci |-> s.ci, ul |-> s.ul, vd |-> s.vd]}
  /\ ws' = Upd(w, Idle(s.k + 1, s.n + 1), FALSE)
  /\ ook' = (ook /\ OracleAtEnd(w, s, out))
  \* A catalog keeps the replica its commit returned (pos: its tail), and
  \* never reads behind it after (CatalogLog, Bug 72)
  /\ looks' = IF out = "committed" /\ MaxLooks > 0
             THEN [looks EXCEPT ![w] = [@ EXCEPT !.sn = Max(@, s.pos),
                                              !.hs = Max(@, HistAt(s.pos))]]
             ELSE looks

\* catch (AmbiguousWrite): resolve by reading.
Ambig(w, s, landed) == Go(w, [s EXCEPT !.amb = TRUE, !.pc = "resolve"], landed)

\* A write of this commit other than the one acknowledged may have landed:
\* one whose outcome was unknown, or a send the log throttled. An
\* unconditional append's retry can land beside it (on one object, a retry
\* fails its condition instead, P4), and a view through the acknowledged
\* one may start from a checkpoint that holds the other.
Earlier(s) == s.amb \/ s.ur \/ (s.dup /\ ~Has("dup"))

\* landed(): what a read R (watching the commit's record) says of it,
\* through the page store when its id is paged (pf: the get fails). With
\* "setonly", the committed set alone decides. An id its
\* page lacks can never apply: the commit fails (BEYOND_FLOOR). A get that
\* fails is a failed read: in a read back (rb) the write is ambiguous, else
\* the outcome unknown. A page deleted since the read (GC dropped it): the
\* runtime reads again, so this read decides nothing.
Judge(w, s, R, pf, rb, then) ==
  LET c0 == IF Has("setonly") THEN Contains(R, s.txn.id) ELSE Landed(R, s.txn.id)
      c  == IF c0 = "paged" THEN Lookup(R, s.txn.id, store, pf) ELSE c0
  IN CASE c = "yes" -> End(w, [s EXCEPT !.pos = R.q], "committed")
       [] c = "unknown" -> End(w, s, IF Earlier(s) THEN "unknown" ELSE "failed")
       [] c = "absent" -> End(w, s, "failed")
       [] c = "fail" -> IF rb THEN Ambig(w, s, FALSE) ELSE End(w, s, "unknown")
       [] c = "missing" ->
            \* with "missingfail", the lookup fails instead, as a failed get does
            Has("missingfail") /\ (IF rb THEN Ambig(w, s, FALSE) ELSE End(w, s, "unknown"))
       [] OTHER -> then

\* The transaction staged for shape sh on a read R, as w's n-th commit. Its
\* id is minted after the floor of R (Bug 64 rule 3; with "mintH", after
\* its horizon, as before paging); k counts the ids a commit took
\* ("freshid").
MintAfter(R) == IF Has("mintH") THEN R.H ELSE R.F
Stage(w, sh, R, n) ==
  LET rd == [e \in sh.rd |-> R.st.ver[e]] IN
  [id |-> [w |-> w, n |-> n, ts |-> Max(Clock(w), MintAfter(R) + 1), k |-> 0],
   rd |-> rd, ird |-> rd, wr |-> sh.wr, cr |-> sh.cr, cid |-> 0,
   wv |-> [e \in sh.wr |-> R.st.ver[e]], nm |-> NamedOn(sh, R.st)]

\* Stage the next transaction on a read of the log, from any usable
\* checkpoint. A read that lags misses records that have landed: a race. A
\* named action the read refuses (RSM's Refused) ends with no commit.
Begin(w) ==
  LET s == ws[w] IN
  /\ s.pc = "idle" /\ s.k <= Len(Shapes[w])
  /\ \E c \in Usable : \E q \in Ends(c) :
       LET R == ReadFrom(c, q, None) IN
       ws' = [ws EXCEPT ![w] =
                IF Refused(Shapes[w][s.k], R.st) THEN Idle(s.k + 1, s.n)
                ELSE [s EXCEPT !.pc = "send", !.txn = Stage(w, Shapes[w][s.k], R, s.n),
                               !.base = q, !.ci = q < Len(log), !.lg = q < Len(log),
                               !.at = IF TrackAt THEN q ELSE 0]]
  /\ UNCHANGED <<log, ckpts, trimmed, inflight, now, nf, comp, ended, ook, looks>>
  /\ UNCHANGED PageVars

\* append(record, null), or with Cond append(record, at), through write()'s
\* throttle retries: one request. A conditional append lands only if the
\* log's tail is the one it names (P4). Faults: failBefore (an exception
\* before landing), notSent (LogNotSentException: nothing landed), lostReply
\* (landed, reply lost), throttled (not landed), landedThrottled (landed,
\* then throttled), inflight (timed out, still in flight: it may land later,
\* LandInflight), void (an append past its segment's end: nothing landed,
\* and the reply says so), condLanded (a conditional append that landed,
\* its reply a failed condition, as a transport retry of it reports).
\*
\* A conditional append that lost (its condition failed) may have landed
\* all the same, so the loss is unresolved, at the furthest tail a lost send
\* named; the writer probes the log's tail and sends again there, without
\* reading (LogRuntime's loop, on a log that cannot replace itself). A void
\* append is a lost race that leaves nothing to resolve (D4): the writer
\* probes and sends again, unless a send of this write the log throttled may
\* have landed. With "voidunres" it is left unresolved, as a conditional
\* loss is; with "voidlands" it landed after all.
Lost(s1, at, probed, real) ==
  [s1 EXCEPT !.pc = "send", !.ur = TRUE, !.la = Max(@, at), !.at = probed, !.cl = @ \/ real]
Send(w, fk) ==
  LET s == ws[w]
      holds == ~Cond \/ Len(log) = s.at
      lands == holds /\ (fk \in {"none", "lostReply", "landedThrottled", "condLanded"}
                         \/ (fk = "void" /\ Has("voidlands")))
      log2 == IF lands THEN Append(log, s.txn) ELSE log
      probed == IF TrackAt THEN Len(log2) ELSE 0
      thr0 == IF s.pc = "throttled" THEN s.thr ELSE 0
      s1 == [Faulted(s, fk) EXCEPT !.att = IF s.pc = "send" THEN @ + 1 ELSE @,
                                   !.nw = @ + 1, !.thr = 0]
      sv == [s1 EXCEPT !.ci = TRUE, !.vd = TRUE]
  IN /\ s.pc \in {"send", "throttled"}
     /\ s.pc = "send" => s.att < MaxAttempts
     /\ fk \in WFaults
     /\ fk = "condLanded" => holds
     /\ UseFault(fk)
     /\ log' = log2
     /\ inflight' = IF fk = "inflight"
                      THEN inflight \cup {[w |-> w, rec |-> s.txn, seq |-> s.nw,
                                            at |-> IF Cond THEN s.at ELSE -1]}
                    ELSE inflight
     /\ UNCHANGED <<ckpts, trimmed, now, comp>>
     /\ UNCHANGED PageVars
     /\ CASE fk = "none" /\ holds ->
               Go(w, [s1 EXCEPT !.pc = "readback", !.pos = Len(log2)], TRUE)
          [] fk = "none" ->               \* a conditional append lost its race
               Go(w, Lost(s1, s.at, probed, TRUE), FALSE)
          [] fk = "condLanded" ->
               Go(w, Lost(s1, s.at, probed, FALSE), TRUE)
          [] fk = "void" ->
               IF s.pc = "throttled" THEN Ambig(w, sv, lands)
               ELSE IF Has("voidunres") THEN Go(w, Lost(sv, s.at, probed, FALSE), lands)
               ELSE Go(w, [sv EXCEPT !.pc = "send", !.at = probed], lands)
          [] fk = "notSent" ->
               \* nothing landed: rejected (NOT_SENT), unless an earlier write may
               \* have (attempts(): earlier(), a lost conditional send's included),
               \* or an earlier send of this one was throttled
               IF s.pc = "throttled" THEN Ambig(w, s1, FALSE)
               ELSE IF Earlier(s) THEN End(w, s1, "unknown") ELSE End(w, s1, "error")
          [] fk \in {"failBefore", "lostReply", "inflight"} -> Ambig(w, s1, lands)
          [] OTHER ->                     \* throttled, landedThrottled
               \* an unconditional retry can land beside it; a conditional one cannot
               IF thr0 + 1 < MaxThrottle
                 THEN Go(w, [s1 EXCEPT !.pc = "throttled", !.thr = thr0 + 1, !.dup = ~Cond],
                         lands)
               ELSE Ambig(w, s1, lands)

\* readThrough(receipt): a view whose records include the one appended (P3):
\* from a usable checkpoint before it. None if the log was trimmed past it
\* (TrimmedException), and a failed read: the write is ambiguous. With
\* "p3", the view may start from a checkpoint that already covers the record.
\* pageFail: the read succeeds, and the get of the page landed() asks fails.
ReadBack(w, fk) ==
  LET s == ws[w]
      cs == {c \in Usable : Has("p3") \/ c.pos < s.pos}
      s1 == Faulted(s, fk)
  IN /\ s.pc = "readback"
     /\ fk \in RFaults
     /\ UseFault(fk)
     /\ UNCHANGED <<log, ckpts, trimmed, inflight, now, comp>>
     /\ UNCHANGED PageVars
     /\ IF fk = "readFail" \/ cs = {} THEN fk # "pageFail" /\ Ambig(w, s1, FALSE)
        ELSE \E c \in cs, q \in s.pos..Len(log) :
               LET R == ReadFrom(c, Max(q, c.pos), s.txn.id)
                   s2 == [s1 EXCEPT !.cx = @ \/ Crossed(R, s.txn), !.lg = @ \/ q < Len(log)]
               IN /\ Lag \/ q = Len(log)
                  /\ fk = "pageFail" => NeedsPage(R, s.txn.id)
                  \* replay rejected the record: a concurrent commit invalidated it
                  /\ Judge(w, s2, R, fk = "pageFail", TRUE, End(w, s2, "failed"))

\* After an ambiguous write: a fresh read decides. If the record is not
\* there, send it again if the transaction holds on the read (a copy still
\* in flight carries the same id, and replay applies one of them), else
\* fail: replay rejects every copy from here on (S2). A failed read leaves
\* the outcome unknown. The read must reach the one the commit staged on:
\* S2 holds from there, and every copy lands after it; on a read that lags
\* behind it ("lagread"), the transaction may not hold yet. With
\* "noverify", the commit fails without asking whether the transaction
\* holds; with "freshid", the retry takes a new id. A read past the tail a
\* lost conditional send named settles that loss (settle), and the retry
\* names the read's tail (adopt).
Settles(s, q) == Has("lagsettle") \/ q > s.la
Resolve(w, fk) ==
  LET s == ws[w] IN
  /\ s.pc = "resolve"
  /\ fk \in RFaults
  /\ UseFault(fk)
  /\ UNCHANGED <<log, ckpts, trimmed, inflight, now, comp>>
  /\ UNCHANGED PageVars
  /\ IF fk = "readFail" \/ Usable = {} THEN fk # "pageFail" /\ End(w, Faulted(s, fk), "unknown")
     ELSE \E c \in Usable : \E q \in Ends(c) :
            (q >= s.base \/ Has("lagread")) /\
            LET R == ReadFrom(c, q, s.txn.id)
                s1 == [Faulted(s, fk) EXCEPT !.cx = @ \/ Crossed(R, s.txn),
                                             !.lg = @ \/ q < Len(log)]
                id2 == [s.txn.id EXCEPT !.ts = Max(Clock(w), MintAfter(R) + 1), !.k = @ + 1]
                settled == Settles(s, q)
                again == [s1 EXCEPT !.pc = "send",
                                    !.txn = IF Has("freshid") THEN [@ EXCEPT !.id = id2] ELSE @,
                                    !.at = IF TrackAt THEN q ELSE 0,
                                    !.ur = @ /\ ~settled,
                                    !.la = IF settled THEN -1 ELSE @]
            IN /\ fk = "pageFail" => NeedsPage(R, s.txn.id)
               /\ Judge(w, s1, R, fk = "pageFail", FALSE,
                    IF ~Has("noverify") /\ Verify(R.st, s.txn) THEN Go(w, again, FALSE)
                    ELSE End(w, s1, "failed"))

\* Out of attempts: unknown after an ambiguous write; failed after lost
\* races, unless a conditional send lost unresolved: the last attempt's loss
\* is what its own landing would also report, so a fresh read says whether
\* it landed (committed), and if not, settles it only if the read is past
\* the furthest tail a lost send named; a read that lags behind it cannot
\* say the send did not land: unknown (Bug 90; with "lagsettle", any read
\* settles it).
Exhaust(w, fk) ==
  LET s == ws[w] IN
  /\ s.pc = "send" /\ s.att >= MaxAttempts
  /\ UNCHANGED <<log, ckpts, trimmed, inflight, now, comp>>
  /\ UNCHANGED PageVars
  /\ IF s.amb \/ ~s.ur
       THEN fk = "none" /\ UNCHANGED nf /\ End(w, s, IF s.amb THEN "unknown" ELSE "failed")
     ELSE
       /\ fk \in RFaults
       /\ UseFault(fk)
       /\ IF fk = "readFail" \/ Usable = {}
            THEN fk # "pageFail" /\ End(w, Faulted(s, fk), "unknown")
          ELSE \E c \in Usable : \E q \in Ends(c) :
                 (q >= s.base \/ Has("lagread")) /\
                 LET R == ReadFrom(c, q, s.txn.id)
                     s1 == [Faulted(s, fk) EXCEPT !.cx = @ \/ Crossed(R, s.txn),
                                                  !.lg = @ \/ q < Len(log),
                                                  !.ul = s.cl /\ q < Len(log)]
                 IN /\ fk = "pageFail" => NeedsPage(R, s.txn.id)
                    /\ Judge(w, s1, R, fk = "pageFail", FALSE,
                         End(w, s1, IF Settles(s, q) THEN "failed" ELSE "unknown"))

\* A timed-out append lands after all, at the tail; a conditional one only
\* if the tail is still the one it names, else nowhere.
LandInflight(i) ==
  /\ i \in inflight
  /\ inflight' = inflight \ {i}
  /\ IF i.at < 0 \/ Len(log) = i.at
       THEN /\ log' = Append(log, i.rec)
            /\ ws' = [v \in Writers |->
                        IF ws[v].pc # "idle" /\ (v # i.w \/ ws[v].txn.id # i.rec.id)
                          THEN [ws[v] EXCEPT !.ci = TRUE] ELSE ws[v]]
       ELSE UNCHANGED <<log, ws>>
  /\ UNCHANGED <<ckpts, trimmed, now, nf, comp, ended, ook, looks>>
  /\ UNCHANGED PageVars

-----------------------------------------------------------------------------
(* Compactors: LogRuntime.compact, a step at a time, in any order *)

\* A compactor appends a GC record at its clock less the retention.
AppendGc(sk) ==
  LET wm == 1 + now + sk - Retention IN
  /\ Retention >= 0 /\ comp.gc < MaxGc /\ wm > 0
  /\ log' = Append(log, [wm |-> wm])
  /\ comp' = [comp EXCEPT !.gc = @ + 1]
  /\ ws' = [v \in Writers |-> IF ws[v].pc # "idle" THEN [ws[v] EXCEPT !.ci = TRUE] ELSE ws[v]]
  /\ UNCHANGED <<ckpts, trimmed, inflight, now, nf, ended, ook, looks>>
  /\ UNCHANGED PageVars

\* putCheckpoint(p): the replay of the log through p, read from a usable
\* checkpoint before it (a read with a gap fails, and the compactor reads
\* again; with "nojoint" it checkpoints the gapped replay). With "ckclock", the
\* checkpoint drops the ids its compactor's clock says are past the retention,
\* as the file's checkpoint did before GC records: no record says so.
PutCheckpoint(p, c, sk, gap) ==
  LET R == ReadFromGap(c, p, None, gap)
      g == IF Has("ckclock") /\ Retention >= 0 THEN Gc(R, Max(0, 1 + now + sk - Retention))
           ELSE R
  IN /\ comp.ck < MaxCkpt
     /\ c \in Usable /\ c.pos < p /\ p <= Len(log)
     /\ gap \in Gaps(c, p)
     /\ \A d \in ckpts : d.pos # p
     /\ ckpts' = ckpts \cup {[pos |-> p, st |-> g.st, C |-> g.C, H |-> g.H, F |-> g.F,
                             P |-> g.P]}
     /\ comp' = [comp EXCEPT !.ck = @ + 1]
     /\ UNCHANGED <<log, trimmed, inflight, now, nf, ws, ended, ook, looks>>
     /\ UNCHANGED PageVars

\* trim(p): only through records a stored checkpoint covers (P5); with
\* "trimearly", before one does.
Trim(p) ==
  /\ comp.tr < MaxTrim
  /\ trimmed < p /\ p <= Len(log)
  /\ Has("trimearly") \/ \E c \in ckpts : c.pos >= p
  /\ trimmed' = p
  /\ comp' = [comp EXCEPT !.tr = @ + 1]
  /\ UNCHANGED <<log, ckpts, inflight, now, nf, ws, ended, ook, looks>>
  /\ UNCHANGED PageVars

\* A compactor pages the oldest block of its read (from any usable
\* checkpoint; with Lag, through any position): it puts the page, then holds
\* the PageOut to send. Its ceiling is its clock less the admission window
\* (COMMITTED_PAGING.md "Compaction", with T = 0: it pages what the ceiling
\* allows). A put that fails, or lands with its reply lost, leaves no
\* PageOut to send: the page, if it landed, is an orphan.
PutPage(c, q, sk, fk) ==
  LET R == ReadFrom(c, q, None) IN
  /\ comp.pg < MaxPage
  /\ fk \in {"none"} \cup (IF CanFault THEN FaultKinds \cap PutFaults ELSE {})
  /\ Sealable(R.C) /\ Top(Oldest(R.C)) <= 1 + now + sk - Admission
  /\ UseFault(fk)
  /\ store' = IF fk = "putFail" THEN store ELSE store \cup {PoOf(R.C)}
  /\ pend' = IF fk = "none" THEN pend \cup {PoOf(R.C)} ELSE pend
  /\ comp' = [comp EXCEPT !.pg = @ + 1]
  /\ UNCHANGED <<log, ckpts, trimmed, inflight, now, ws, ended, ook, looks>>

\* The PageOut lands, after its page was put. Replay applies it only if its
\* block is unchanged where it lands. A compactor that crashes after the put
\* never sends it.
SendPageOut(po) ==
  /\ po \in pend
  /\ log' = Append(log, po)
  /\ pend' = pend \ {po}
  /\ ws' = [v \in Writers |-> IF ws[v].pc # "idle" THEN [ws[v] EXCEPT !.ci = TRUE] ELSE ws[v]]
  /\ UNCHANGED <<ckpts, trimmed, inflight, now, nf, comp, ended, ook, store, looks>>

\* A sweep lists the page store and deletes every page its read does not
\* reference whose node ends at or below its floor: no PageOut can name it
\* again. With "sweepabove", every page its read does not reference.
\* Deleting the pages a GC record dropped, once it has landed, is a sweep on
\* a read through it.
Sweep(c, q) ==
  LET R == ReadFrom(c, q, None)
      del == {pg \in store : (\A p \in R.P : PageName(p) # pg)
                             /\ (Has("sweepabove") \/ Top(pg) <= R.F)}
  IN /\ comp.sw < MaxSweep /\ del # {}
     /\ store' = store \ del
     /\ comp' = [comp EXCEPT !.sw = @ + 1]
     /\ UNCHANGED <<log, ckpts, trimmed, inflight, now, nf, ws, ended, ook, pend, looks>>

-----------------------------------------------------------------------------
(* Reads outside a commit: LogRuntime.read(floor) and refresh *)

\* A writer's catalog loads the log (a table load, Iceberg's refresh after a
\* commit), at most MaxLooks times. It keeps the newest replica it has read
\* or committed in (CatalogLog), and a read that lags behind it serves that
\* replica instead (Bug 72); with "readsgoback", the read's own. looks[w]: sn,
\* the position of the replica it holds; n, the reads so far; ghosts: hs,
\* the most transactions a replica it held had applied; ok, no read held
\* fewer; fl, a read lagged behind the replica held.
Look(w) ==
  /\ looks[w].n < MaxLooks
  /\ \E c \in Usable : \E q \in Ends(c) :
       LET q2 == IF q < looks[w].sn /\ ~Has("readsgoback") THEN looks[w].sn ELSE q
           h == HistAt(q2)
       IN looks' = [looks EXCEPT ![w] = [sn |-> q2, hs |-> Max(@.hs, h),
                                         ok |-> @.ok /\ h >= @.hs, fl |-> @.fl \/ q < @.sn,
                                         n |-> @.n + 1]]
  /\ UNCHANGED <<log, ckpts, trimmed, inflight, now, nf, comp, ws, ended, ook>>
  /\ UNCHANGED PageVars

Tick ==
  /\ now < MaxTime
  /\ now' = now + 1
  /\ UNCHANGED <<log, ckpts, trimmed, inflight, nf, comp, ws, ended, ook, looks>>
  /\ UNCHANGED PageVars

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
  /\ comp = [gc |-> 0, ck |-> 0, tr |-> 0, pg |-> 0, sw |-> 0]
  /\ ws = [w \in Writers |-> Idle(1, 1)]
  /\ ended = {}
  /\ ook = TRUE
  /\ store = {}
  /\ pend = {}
  /\ looks = [w \in Writers |-> [sn |-> 0, hs |-> 0, ok |-> TRUE, fl |-> FALSE, n |-> 0]]

WriterNext(w) ==
  \/ Begin(w)
  \/ \E fk \in WriteFaults \cup {"none"} : Send(w, fk)
  \/ \E fk \in ReadFaults \cup {"none"} : ReadBack(w, fk) \/ Resolve(w, fk) \/ Exhaust(w, fk)
  \/ Look(w)

CompactorNext ==
  \/ \E sk \in CompSkews : AppendGc(sk)
  \/ \E p \in 1..Len(log), c \in ckpts, sk \in CompSkews, gap \in {0, 1} :
       PutCheckpoint(p, c, sk, gap)
  \/ \E p \in 1..Len(log) : Trim(p)
  \/ \E c \in Usable, sk \in CompSkews, fk \in PutFaults \cup {"none"} :
       \E q \in Ends(c) : PutPage(c, q, sk, fk)
  \/ \E po \in pend : SendPageOut(po)
  \/ \E c \in Usable : \E q \in Ends(c) : Sweep(c, q)

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
\* same state and committed set (its ids, pages, floor and horizon).
Deterministic ==
  \A c1, c2 \in Usable : Ck(ReadFrom(c1, Len(log), None)) = Ck(ReadFrom(c2, Len(log), None))

\* A checkpoint at p is the replay of the log through p (I11).
CheckpointIsReplay ==
  \A c \in ckpts : Ck(ReadFrom(InitCk, c.pos, None)) = Ck(c)

\* No commit applies twice, whatever ids it took.
ExactlyOnce == \A i, j \in 1..Len(Hist) : i # j => CommitOf(Hist[i]) # CommitOf(Hist[j])

\* The state is the serial execution of the transactions applied.
Serializable == SerialFold(InitSt, Hist) = [ok |-> TRUE, st |-> Full.st]

\* S3, of the state replay reaches.
StampsCount == CountsChanges(Full.st)

\* No id names two entities.
UniqueIds == \A m1, m2 \in Full.st.made : m1.id = m2.id => m1 = m2

\* I13: a namespace's tables and views hold distinct names, none in a
\* namespace dropped (RSM's WellNamedSt).
WellNamed == WellNamedSt(Full.st)

\* Honest outcomes (I10): a commit reported committed applied, and stays
\* applied (through GC, checkpoints and trims); one reported failed (or any
\* error but unknown) is not applied, now or later (a late landing
\* included); one reported unknown follows a fault or a read that put its
\* id at or below the horizon.
HonestCommitted == \A e \in ended : e.out = "committed" => e.c \in CommitsOf(Hist)
HonestFailed == \A e \in ended : e.out \in {"failed", "error"} => e.c \notin CommitsOf(Hist)
\* With conditional appends, also after a lost race whose last read lagged
\* behind the tail a lost send named (ul): a ghost of the race, which a void
\* append (no race to resolve) does not set.
UnknownExplained == \A e \in ended : e.out = "unknown" => e.cf \/ e.cx \/ e.ul

\* A catalog's reads outside a commit never go back: none holds fewer
\* transactions than one it read or committed in before (Bug 72).
MonotoneReads == \A w \in Writers : looks[w].ok

\* A commit no other record raced, on a log that did not fail, commits.
\* Checkpoints and trims are no race; a read that lags is.
SoloCommits == \A e \in ended : (~e.cf /\ ~e.ci) => e.out = "committed"

\* Pages (COMMITTED_PAGING.md, oracle 2): every page the log references is
\* in the store, and holds exactly the ids that applied in its range. The
\* set keeps its shape; every stamp lies on the grid.
PagesExist == PagesExistIn(Full, store)
PagesExact == PagesExactIn(Full, Hist)
Shape == SetShape(Full)
OnGrid == OnGridIn(log)

\* What history the log holds only grows, by one transaction at a time.
HistGrows ==
  [][LET h2 == Fold(Acc(InitSt, InitSet, None), log').st.hist IN
       /\ Len(h2) >= Len(Hist) /\ SubSeq(h2, 1, Len(Hist)) = Hist
       /\ Len(h2) <= Len(Hist) + 1]_log

Safety ==
  Readable /\ Deterministic /\ CheckpointIsReplay /\ ExactlyOnce /\ Serializable
    /\ UniqueIds /\ HonestCommitted /\ HonestFailed /\ UnknownExplained
    /\ PagesExist /\ PagesExact

=============================================================================
