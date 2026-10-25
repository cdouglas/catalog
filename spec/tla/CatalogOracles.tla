-------------------------- MODULE CatalogOracles --------------------------
(***************************************************************************)
(* The jqwik oracles as predicates over the model, on what each test       *)
(* observes, so the model can check that an oracle passing implies the     *)
(* properties of CatalogCommit (Adequate), and which bugs each catches.    *)
(* Each runs on the schedules of its fuzzer (Sched).                       *)
(***************************************************************************)
EXTENDS CatalogCommit

\* What a test sees of the catalog: entity versions stand for the values the
\* fuzzers write (each write a fresh one), and creates count per parent (a
\* test names entities, not ids). No committed set, horizon or history.
ViewOf(st) ==
  [ver |-> st.ver, kids |-> [p \in Entities |-> Cardinality({m \in st.made : m.p = p})]]
View(f) == ViewOf(Replay(f).st)

\* Operation sh staged afresh on st: what a serial run commits.
Fresh(sh, st) ==
  LET rd == [e \in sh.rd |-> st.ver[e]] IN
  [id |-> None, rd |-> rd, ird |-> rd, wr |-> sh.wr, cr |-> sh.cr, cid |-> 0,
   wv |-> [e \in sh.wr |-> st.ver[e]]]

AllDone == \A w \in Writers : ws[w].pc = "idle" /\ ws[w].k > Len(Shapes[w])

-----------------------------------------------------------------------------
(* TestMultiWriterProperties.Run.commit: each commit, on its own writes that
   landed (`writes`), those that changed the state (`effective`), its outcome,
   and whether a fault was injected into it (ctx.faulted()). *)

Effective(cw) == SelectSeq(cw, LAMBDA x : View(x.after) # View(x.before))

OracleMW(w, s, out, final) ==
  LET eff == Effective(s.cw)
      W   == IF eff # << >> THEN Head(eff) ELSE Head(s.cw)
      pre == Replay(W.before).st
  IN \* "one commit changed the state twice"
     /\ Len(eff) <= 1
     \* checkTransition: valid on the state it replaced (reexecutionViolation),
     \* serial re-execution reaches the state it wrote, and so do its effects
     /\ (out = "committed" \/ (out = "unknown" /\ eff # << >>)) =>
          /\ s.cw # << >>                         \* "acknowledged commit changed nothing"
          /\ ValidOn(pre, s.txn)
          /\ View(W.after) = ViewOf(SerialApply(pre, s.txn))
     \* "commit reported REJECTED/FAILED but took effect"
     /\ out \in {"failed", "error"} => eff = << >>
     \* "commit failed without an injected fault"
     /\ out = "error" => s.cf
     \* "commit outcome unknown without an injected fault", unless gc
     /\ out = "unknown" => s.cf \/ Retention >= 0
     \* "one commit made %d conditional writes", on storage that does not fail
     /\ MaxFaults = 0 => Len(s.cw) <= 1

-----------------------------------------------------------------------------
(* TestCatalogFaultProperties.Run.resolve: after each operation, the catalog
   read back against the test's model, with and without the operation. The
   model is the catalog before the operation's first write: in the fault
   schedules nothing else writes from then until it returns. *)

OracleFault(w, s, out, final) ==
  ~final \/
  LET sh   == Shapes[w][s.k]
      M    == IF s.first = None THEN LiveR.st ELSE Replay(s.first).st
      \* op.effect.apply: an operation applies afresh; a catalog transaction
      \* only if what it read is unchanged (IsolationViolation)
      with == IF sh.retry THEN [ok |-> TRUE, st |-> SerialApply(M, Fresh(sh, M))]
              ELSE IF ValidOn(M, s.txn) THEN [ok |-> TRUE, st |-> SerialApply(M, s.txn)]
              ELSE [ok |-> FALSE, st |-> M]
      act  == ViewOf(Replay(file').st)
  IN /\ out = "committed" => with.ok /\ act = ViewOf(with.st)
     /\ out = "unknown" => (with.ok /\ act = ViewOf(with.st)) \/ act = ViewOf(M)
     /\ out \in {"failed", "error"} => act = ViewOf(M)
     \* a failure but CommitFailed/Unknown needs a fault ("unexpected")
     /\ out = "error" => s.ocf

-----------------------------------------------------------------------------
(* TestCatalogRaceProperties.aRaceEndsAsASerialRun: A is writer 1's
   operation; the racers each run alone, just before one of A's writes or
   after A. At the end, results and state against a serial run: the racers
   before A's writes, A, the rest. A version change commutes, and a test sees
   creates by count, so the order of the rest does not change the view. *)

RECURSIVE SerialOf(_, _)
SerialOf(st, W) ==
  IF W = {} THEN st
  ELSE LET v == CHOOSE v \in W : TRUE
       IN SerialOf(SerialApply(st, Fresh(Shapes[v][1], st)), W \ {v})

RacePred ==
  LET a       == Shapes[1][1]
      before  == sched.racers
      raceA   == res[1]
      racers  == Writers \ {1}
      withA   == ViewOf(SerialOf(InitSt, Writers))
      withoutA == ViewOf(SerialOf(InitSt, racers))
      act     == View(file)
      \* a transaction whose reads a racer changed must fail (D12: it may
      \* fail on any change to a table it pins)
      txnFailed == ~a.retry /\ before # {} /\ raceA # "committed"
      changed == \E b \in before : Shapes[b][1].wr \cap a.rd # {}
      \* gc: A may lose its outcome to the horizon (Bug 64)
      lost    == Retention >= 0 /\ raceA \in {"failed", "unknown"}
  IN /\ (~a.retry /\ changed) => txnFailed
     /\ \A b \in racers : res[b] = "committed"
     /\ IF txnFailed \/ lost
          THEN act = withoutA \/ (lost /\ act = withA)
          ELSE raceA = "committed" /\ act = withA

OracleRace == AllDone => RacePred

-----------------------------------------------------------------------------
(* Adequacy: where the fuzzer's oracle passes on a whole schedule, the
   catalog's properties hold. *)

OracleOK == IF Sched = "race" THEN RacePred ELSE ook

\* What the fuzzer asserts: its oracle as the run ends, and that every
\* version of the catalog reads (each fuzzer parses the versions written).
OracleHolds == Readable /\ (AllDone => OracleOK)

\* Where the fuzzer's assertions pass on a whole run, the properties hold.
Adequate == (AllDone /\ OracleHolds) => Safety
-----------------------------------------------------------------------------
(* Tightened oracles: the changes the model motivates (README, "Oracle
   changes"). Each must pass on the correct model, like the original. *)

\* A commit that nothing raced (no commit since its snapshot, none inside
\* it) on storage that did not fail is acknowledged. The multi-writer test
\* can tell: liveBytes() at the commit equals the writer's snapshotBytes,
\* ctx.preemptedAt < 0 and !ctx.faulted().
Solo(s, out) == (~s.cf /\ ~s.ci) => out = "committed"

OracleMW2(w, s, out, final) == OracleMW(w, s, out, final) /\ Solo(s, out)

\* TestCatalogFaultProperties as the multi-writer test: no outcome unknown
\* without a fault (or gc), and an operation nothing raced commits.
OracleFault2(w, s, out, final) ==
  /\ OracleFault(w, s, out, final)
  /\ final => (out = "unknown" => s.ocf \/ Retention >= 0)
  /\ final => ((~s.ocf /\ ~s.ci) => out = "committed")

\* The race test: A may lose its outcome to gc only to a racer that ran
\* before one of its writes, and if it took effect, its outcome is unknown,
\* not CommitFailedException.
RacePred2 ==
  /\ RacePred
  /\ (Retention >= 0 /\ res[1] \in {"failed", "unknown"}) =>
       /\ sched.racers # {}
       /\ View(file) = ViewOf(SerialOf(InitSt, Writers)) => res[1] = "unknown"

OracleOK2 == IF Sched = "race" THEN RacePred2 ELSE ook
OracleHolds2 == Readable /\ (AllDone => OracleOK2)
=============================================================================
