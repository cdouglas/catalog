---- MODULE FileIOCatalog ----
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS Writers, Tables, MaxLog, MaxRetries

(* --algorithm FileIOCatalog

variables
  \* Storage: single-object cloud catalog file.
  storage = [etag |-> 0, length |-> 0, log |-> << >>, committed |-> {}];
  \* Monotonic UUID counter — each writer draws unique txn UUIDs from it.
  nextUuid = 1;
  \* Per-writer terminal result: "unknown" until the writer reaches a
  \* terminal state, then "committed" or "exhausted".
  terminalResult = [w \in Writers |-> "unknown"];

define
  \* MustCAS condition (SPEC.md §Commit Protocol), count limit only: the
  \* size limit and backend capability are not modelled (newBytes is unused).
  MustCAS(view, newBytes) ==
    Len(view.log) >= MaxLog
  \* Compaction result: committed set grows by the new UUID, log is reset to the new txn.
  Compact(view, uuid) ==
    [etag |-> view.etag + 1,
     length |-> 1,
     log |-> << uuid >>,
     committed |-> view.committed \cup {uuid}]
end define;

process Writer \in Writers
variables
  attempt = 0,
  myView = storage,
  myUuid = 0;
begin
  Begin:
    while attempt < MaxRetries do
      Read:
        myView := storage;
        if myUuid = 0 then
          myUuid := nextUuid;
          nextUuid := nextUuid + 1;
        end if;
      Decide:
        if MustCAS(myView, myUuid) then
          goto DoCAS;
        else
          goto DoAppend;
        end if;
      DoCAS:
        if myUuid \in storage.committed then
          terminalResult[self] := "committed";
          goto Finish;  \* Already committed by a previous attempt.
        elsif storage.etag = myView.etag then
          storage := Compact(myView, myUuid);
          terminalResult[self] := "committed";
          goto Finish;
        else
          attempt := attempt + 1;
          goto Begin;
        end if;
      DoAppend:
        if myUuid \in storage.committed then
          terminalResult[self] := "committed";
          goto Finish;  \* Already committed by a previous attempt.
        elsif storage.length = myView.length then
          storage := [storage EXCEPT
                      !.length = @ + 1,
                      !.log = Append(@, myUuid),
                      !.committed = @ \cup {myUuid}];
          terminalResult[self] := "committed";
          goto Finish;
        else
          attempt := attempt + 1;
          goto Begin;
        end if;
    end while;
  Exhaust:
    terminalResult[self] := "exhausted";
  Finish:
    skip;
end process;

end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "c9a8835e" /\ chksum(tla) = "daa6bf27")
VARIABLES pc, storage, nextUuid, terminalResult

(* define statement *)
MustCAS(view, newBytes) ==
  Len(view.log) >= MaxLog

Compact(view, uuid) ==
  [etag |-> view.etag + 1,
   length |-> 1,
   log |-> << uuid >>,
   committed |-> view.committed \cup {uuid}]

VARIABLES attempt, myView, myUuid

vars == << pc, storage, nextUuid, terminalResult, attempt, myView, myUuid >>

ProcSet == (Writers)

Init == (* Global variables *)
        /\ storage = [etag |-> 0, length |-> 0, log |-> << >>, committed |-> {}]
        /\ nextUuid = 1
        /\ terminalResult = [w \in Writers |-> "unknown"]
        (* Process Writer *)
        /\ attempt = [self \in Writers |-> 0]
        /\ myView = [self \in Writers |-> storage]
        /\ myUuid = [self \in Writers |-> 0]
        /\ pc = [self \in ProcSet |-> "Begin"]

Begin(self) == /\ pc[self] = "Begin"
               /\ IF attempt[self] < MaxRetries
                     THEN /\ pc' = [pc EXCEPT ![self] = "Read"]
                     ELSE /\ pc' = [pc EXCEPT ![self] = "Exhaust"]
               /\ UNCHANGED << storage, nextUuid, terminalResult, attempt, 
                               myView, myUuid >>

Read(self) == /\ pc[self] = "Read"
              /\ myView' = [myView EXCEPT ![self] = storage]
              /\ IF myUuid[self] = 0
                    THEN /\ myUuid' = [myUuid EXCEPT ![self] = nextUuid]
                         /\ nextUuid' = nextUuid + 1
                    ELSE /\ TRUE
                         /\ UNCHANGED << nextUuid, myUuid >>
              /\ pc' = [pc EXCEPT ![self] = "Decide"]
              /\ UNCHANGED << storage, terminalResult, attempt >>

Decide(self) == /\ pc[self] = "Decide"
                /\ IF MustCAS(myView[self], myUuid[self])
                      THEN /\ pc' = [pc EXCEPT ![self] = "DoCAS"]
                      ELSE /\ pc' = [pc EXCEPT ![self] = "DoAppend"]
                /\ UNCHANGED << storage, nextUuid, terminalResult, attempt, 
                                myView, myUuid >>

DoCAS(self) == /\ pc[self] = "DoCAS"
               /\ IF myUuid[self] \in storage.committed
                     THEN /\ terminalResult' = [terminalResult EXCEPT ![self] = "committed"]
                          /\ pc' = [pc EXCEPT ![self] = "Finish"]
                          /\ UNCHANGED << storage, attempt >>
                     ELSE /\ IF storage.etag = myView[self].etag
                                THEN /\ storage' = Compact(myView[self], myUuid[self])
                                     /\ terminalResult' = [terminalResult EXCEPT ![self] = "committed"]
                                     /\ pc' = [pc EXCEPT ![self] = "Finish"]
                                     /\ UNCHANGED attempt
                                ELSE /\ attempt' = [attempt EXCEPT ![self] = attempt[self] + 1]
                                     /\ pc' = [pc EXCEPT ![self] = "Begin"]
                                     /\ UNCHANGED << storage, terminalResult >>
               /\ UNCHANGED << nextUuid, myView, myUuid >>

DoAppend(self) == /\ pc[self] = "DoAppend"
                  /\ IF myUuid[self] \in storage.committed
                        THEN /\ terminalResult' = [terminalResult EXCEPT ![self] = "committed"]
                             /\ pc' = [pc EXCEPT ![self] = "Finish"]
                             /\ UNCHANGED << storage, attempt >>
                        ELSE /\ IF storage.length = myView[self].length
                                   THEN /\ storage' = [storage EXCEPT
                                                       !.length = @ + 1,
                                                       !.log = Append(@, myUuid[self]),
                                                       !.committed = @ \cup {myUuid[self]}]
                                        /\ terminalResult' = [terminalResult EXCEPT ![self] = "committed"]
                                        /\ pc' = [pc EXCEPT ![self] = "Finish"]
                                        /\ UNCHANGED attempt
                                   ELSE /\ attempt' = [attempt EXCEPT ![self] = attempt[self] + 1]
                                        /\ pc' = [pc EXCEPT ![self] = "Begin"]
                                        /\ UNCHANGED << storage, 
                                                        terminalResult >>
                  /\ UNCHANGED << nextUuid, myView, myUuid >>

Exhaust(self) == /\ pc[self] = "Exhaust"
                 /\ terminalResult' = [terminalResult EXCEPT ![self] = "exhausted"]
                 /\ pc' = [pc EXCEPT ![self] = "Finish"]
                 /\ UNCHANGED << storage, nextUuid, attempt, myView, myUuid >>

Finish(self) == /\ pc[self] = "Finish"
                /\ TRUE
                /\ pc' = [pc EXCEPT ![self] = "Done"]
                /\ UNCHANGED << storage, nextUuid, terminalResult, attempt, 
                                myView, myUuid >>

Writer(self) == Begin(self) \/ Read(self) \/ Decide(self) \/ DoCAS(self)
                   \/ DoAppend(self) \/ Exhaust(self) \/ Finish(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == (\E self \in Writers: Writer(self))
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 

TypeOK ==
  /\ storage \in [etag: Nat, length: Nat, log: Seq(Nat), committed: SUBSET Nat]
  /\ nextUuid \in Nat
  /\ terminalResult \in [Writers -> {"unknown", "committed", "exhausted"}]

\* S1: every log entry is a UUID that is in `committed`. Every append adds to
\* both, so this holds by construction; it is not a linearizability check.
S1_LogEntriesCommitted ==
  \A i \in 1..Len(storage.log):
    storage.log[i] \in storage.committed

\* S2: the committed set is monotone — once a UUID appears, it never
\* disappears. GC of the committed set is not modelled. This is the property
\* that catches a CAS without the etag check; it does not catch an append
\* without the offset check (see README, Known gaps).
S2_NoLostCommit == [][storage.committed \subseteq storage'.committed]_<<storage>>

\* S3: each UUID appears at most once in `log`. Vacuous as modelled: no writer
\* retries after its write lands (lost replies are not modelled), so the
\* "already committed" branches of DoCAS / DoAppend are never taken.
S3_IdempotentReplay ==
  \A i, j \in 1..Len(storage.log):
    (storage.log[i] = storage.log[j]) => (i = j)

\* Weak fairness on each writer: once a writer step is enabled, it is
\* eventually taken. Per-process WF on `Writer(self)` over the full `vars`
\* tuple is the standard idiom for PlusCal translations.
FairSpec == Spec /\ \A w \in Writers: WF_vars(Writer(w))

\* L1: every writer eventually terminates, committed or exhausted (pc = "Done",
\* matching the generated `Termination` operator). Follows from the bounded
\* retry loop.
L1_Termination ==
  \A w \in Writers: <>(pc[w] = "Done")

\* L2: every writer eventually commits or exhausts retries. `Exhaust` is
\* unreachable in the shipped configs, so in practice every writer commits.
L2_NoStarvation ==
  \A w \in Writers: <>(terminalResult[w] \in {"committed", "exhausted"})

\* L3: bounded retries — every writer's `attempt` counter stays within
\* MaxRetries. This is an invariant (safety), implied by the loop guard
\* `while attempt < MaxRetries`.
L3_BoundedRetries ==
  \A w \in Writers: attempt[w] <= MaxRetries

====
