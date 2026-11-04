------------------------- MODULE SharedLogOracles -------------------------
(***************************************************************************)
(* Oracles a fuzzer of the shared-log protocol can assert (stage 6:        *)
(* CommitProtocolProperties over InMemorySharedLog, and                    *)
(* TestMultiWriterProperties ported to it), as predicates over the model,  *)
(* so the model can check that an oracle passing implies the properties of *)
(* SharedLogCommit (Adequate), and which bugs each catches.                *)
(*                                                                         *)
(* The harness owns the log, so it can replay all of it, trimmed records   *)
(* included, and it knows the faults it injected and the page store's      *)
(* contents. It cannot see the runtime's reads.                            *)
(***************************************************************************)
EXTENDS SharedLogCommit

AllDone == \A w \in Writers : ws[w].pc = "idle" /\ ws[w].k > Len(Shapes[w])

\* The run is over and every append in flight has landed: the harness
\* delivers what it delayed before it judges the run.
Quiet == AllDone /\ inflight = {}

\* Whether s's commit has applied, in a replay of the whole log now.
AppliedNow(s) == CommitOf(s.txn) \in CommitsOf(Hist)

-----------------------------------------------------------------------------
(* "now": each commit judged as it ends, against the log at that moment,   *)
(* as TestMultiWriterProperties judges a commit on the catalog file.       *)

OracleNow(w, s, out) ==
  /\ out = "committed" => AppliedNow(s)
  \* "commit reported REJECTED/FAILED but took effect"
  /\ out \in {"failed", "error"} => ~AppliedNow(s)
  \* "commit failed without an injected fault"
  /\ out = "error" => s.cf
  \* "commit outcome unknown without an injected fault", unless gc
  /\ out = "unknown" => s.cf \/ Retention >= 0

-----------------------------------------------------------------------------
(* "drain": "now", and when the run is quiet, every outcome against the    *)
(* whole log, each commit applied at most once (the harness tags each      *)
(* intention with its commit), every checkpoint stored against a replay of *)
(* the log through it (InMemorySharedLog.putCheckpoint), and the state     *)
(* against the serial execution of what applied (EntityVersionsMachine).   *)

\* Oracle 4 of COMMITTED_PAGING.md: orphans are collected. A final GC at
\* the newest stamp (retention 0) raises the floor past every page in the
\* store, so a sweep then empties it.
NewestStamp == SetMax({Txns(log)[i].id.ts : i \in 1..Len(Txns(log))} \cup {0})
OrphansCollected == \A pg \in store : Top(pg) <= Gc(Full, NewestStamp).F

DrainPred ==
  /\ \A e \in ended : e.out = "committed" => e.c \in CommitsOf(Hist)
  /\ \A e \in ended : e.out \in {"failed", "error"} => e.c \notin CommitsOf(Hist)
  /\ ExactlyOnce
  /\ CheckpointIsReplay
  /\ Serializable
  /\ OrphansCollected

-----------------------------------------------------------------------------
(* What each fuzzer asserts: every read of the log succeeds, its oracle,   *)
(* and after every step oracle 2 of COMMITTED_PAGING.md: each page the log *)
(* references is in the store and holds exactly the ids that applied in    *)
(* its range (the harness replays the whole log and reads the store).      *)
(* Where it passes on a whole run, the properties hold (Adequate).         *)

PagesOK == PagesExist /\ PagesExact
OracleHoldsNow == Readable /\ PagesOK /\ (AllDone => ook)
OracleHoldsDrain == OracleHoldsNow /\ (Quiet => DrainPred)

AdequateNow == (Quiet /\ OracleHoldsNow) => Safety
AdequateDrain == (Quiet /\ OracleHoldsDrain) => Safety
=============================================================================
