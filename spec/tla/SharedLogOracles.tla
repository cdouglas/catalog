------------------------- MODULE SharedLogOracles -------------------------
(***************************************************************************)
(* Oracles a fuzzer of the shared-log protocol can assert (stage 6:        *)
(* CommitProtocolProperties over InMemorySharedLog, and                    *)
(* TestMultiWriterProperties ported to it), as predicates over the model,  *)
(* so the model can check that an oracle passing implies the properties of *)
(* SharedLogCommit (Adequate), and which bugs each catches.                *)
(*                                                                         *)
(* The harness owns the log, so it can replay all of it, trimmed records   *)
(* included, and it knows the faults it injected. It cannot see the        *)
(* runtime's reads.                                                        *)
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

DrainPred ==
  /\ \A e \in ended : e.out = "committed" => e.c \in CommitsOf(Hist)
  /\ \A e \in ended : e.out \in {"failed", "error"} => e.c \notin CommitsOf(Hist)
  /\ ExactlyOnce
  /\ CheckpointIsReplay
  /\ Serializable

-----------------------------------------------------------------------------
(* What each fuzzer asserts: every read of the log succeeds, and its       *)
(* oracle. Where it passes on a whole run, the properties hold (Adequate). *)

OracleHoldsNow == Readable /\ (AllDone => ook)
OracleHoldsDrain == OracleHoldsNow /\ (Quiet => DrainPred)

AdequateNow == (Quiet /\ OracleHoldsNow) => Safety
AdequateDrain == (Quiet /\ OracleHoldsDrain) => Safety
=============================================================================
