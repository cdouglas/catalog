---- MODULE FileIOCatalog ----
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS Writers, Tables, MaxLog, MaxRetries

(* --algorithm FileIOCatalog

variables
  \* Storage: single-object cloud catalog file. `log` and `committed` carry
  \* transaction records [uuid, created], not bare UUIDs.
  storage = [etag |-> 0, length |-> 0, log |-> << >>, committed |-> {}];
  \* Monotonic UUID counter — each writer draws unique txn UUIDs from it.
  nextUuid = 1;
  \* Per-writer terminal result: "unknown" until the writer reaches a
  \* terminal state, then "committed" or "exhausted".
  terminalResult = [w \in Writers |-> "unknown"];

define
  \* MustCAS condition (SPEC.md §Commit Protocol), count limit only: the
  \* size limit and backend capability are not modelled.
  MustCAS(view) ==
    Len(view.log) >= MaxLog
  \* Union of all entity ids allocated by committed transactions in `view`.
  AllocatedIds(view) ==
    UNION { t.created : t \in view.committed }
  \* Writer-local id allocation from a snapshot: pick max(allocated)+1, or 1
  \* if nothing has committed yet. This mirrors the pre-808bca7 buggy path
  \* where two writers reading the same snapshot collide on the same id.
  NextIdFrom(view) ==
    IF AllocatedIds(view) = {} THEN 1
    ELSE (CHOOSE n \in AllocatedIds(view): \A m \in AllocatedIds(view): m <= n) + 1
  \* UUIDs projected from a set of transaction records.
  UuidsOf(S) == { t.uuid : t \in S }
  \* Compaction result: committed set grows by the new txn record, log is
  \* reset to that txn.
  Compact(view, txn) ==
    [etag |-> view.etag + 1,
     length |-> 1,
     log |-> << txn >>,
     committed |-> view.committed \cup {txn}]
end define;

process Writer \in Writers
variables
  attempt = 0,
  myView = storage,
  myUuid = 0,
  myTxn = [uuid |-> 0, created |-> {}];
begin
  Begin:
    while attempt < MaxRetries do
      Read:
        myView := storage;
        if myUuid = 0 then
          myUuid := nextUuid;
          nextUuid := nextUuid + 1;
        end if;
        \* Eager snapshot-local id allocation (the buggy behaviour).
        myTxn := [uuid |-> myUuid, created |-> {NextIdFrom(myView)}];
      Decide:
        if MustCAS(myView) then
          goto DoCAS;
        else
          goto DoAppend;
        end if;
      DoCAS:
        if myUuid \in UuidsOf(storage.committed) then
          terminalResult[self] := "committed";
          goto Finish;  \* Already committed by a previous attempt.
        elsif storage.etag = myView.etag then
          storage := Compact(myView, myTxn);
          terminalResult[self] := "committed";
          goto Finish;
        else
          attempt := attempt + 1;
          goto Begin;
        end if;
      DoAppend:
        if myUuid \in UuidsOf(storage.committed) then
          terminalResult[self] := "committed";
          goto Finish;  \* Already committed by a previous attempt.
        else
          \* Append always lands: content-addressed storage returns the
          \* actual landing offset, so a stale view does not fail the write.
          \* Replay/verify is what rejects conflicting records (not modelled
          \* here; see MC_ids_bug for the id-collision bug this exposes).
          storage := [storage EXCEPT
                      !.length = @ + 1,
                      !.log = Append(@, myTxn),
                      !.committed = @ \cup {myTxn}];
          terminalResult[self] := "committed";
          goto Finish;
        end if;
    end while;
  Exhaust:
    terminalResult[self] := "exhausted";
  Finish:
    skip;
end process;

end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "c4360955" /\ chksum(tla) = "c85d0e82")
VARIABLES pc, storage, nextUuid, terminalResult

(* define statement *)
MustCAS(view) ==
  Len(view.log) >= MaxLog

AllocatedIds(view) ==
  UNION { t.created : t \in view.committed }



NextIdFrom(view) ==
  IF AllocatedIds(view) = {} THEN 1
  ELSE (CHOOSE n \in AllocatedIds(view): \A m \in AllocatedIds(view): m <= n) + 1

UuidsOf(S) == { t.uuid : t \in S }


Compact(view, txn) ==
  [etag |-> view.etag + 1,
   length |-> 1,
   log |-> << txn >>,
   committed |-> view.committed \cup {txn}]

VARIABLES attempt, myView, myUuid, myTxn

vars == << pc, storage, nextUuid, terminalResult, attempt, myView, myUuid, 
           myTxn >>

ProcSet == (Writers)

Init == (* Global variables *)
        /\ storage = [etag |-> 0, length |-> 0, log |-> << >>, committed |-> {}]
        /\ nextUuid = 1
        /\ terminalResult = [w \in Writers |-> "unknown"]
        (* Process Writer *)
        /\ attempt = [self \in Writers |-> 0]
        /\ myView = [self \in Writers |-> storage]
        /\ myUuid = [self \in Writers |-> 0]
        /\ myTxn = [self \in Writers |-> [uuid |-> 0, created |-> {}]]
        /\ pc = [self \in ProcSet |-> "Begin"]

Begin(self) == /\ pc[self] = "Begin"
               /\ IF attempt[self] < MaxRetries
                     THEN /\ pc' = [pc EXCEPT ![self] = "Read"]
                     ELSE /\ pc' = [pc EXCEPT ![self] = "Exhaust"]
               /\ UNCHANGED << storage, nextUuid, terminalResult, attempt, 
                               myView, myUuid, myTxn >>

Read(self) == /\ pc[self] = "Read"
              /\ myView' = [myView EXCEPT ![self] = storage]
              /\ IF myUuid[self] = 0
                    THEN /\ myUuid' = [myUuid EXCEPT ![self] = nextUuid]
                         /\ nextUuid' = nextUuid + 1
                    ELSE /\ TRUE
                         /\ UNCHANGED << nextUuid, myUuid >>
              /\ myTxn' = [myTxn EXCEPT ![self] = [uuid |-> myUuid'[self], created |-> {NextIdFrom(myView'[self])}]]
              /\ pc' = [pc EXCEPT ![self] = "Decide"]
              /\ UNCHANGED << storage, terminalResult, attempt >>

Decide(self) == /\ pc[self] = "Decide"
                /\ IF MustCAS(myView[self])
                      THEN /\ pc' = [pc EXCEPT ![self] = "DoCAS"]
                      ELSE /\ pc' = [pc EXCEPT ![self] = "DoAppend"]
                /\ UNCHANGED << storage, nextUuid, terminalResult, attempt, 
                                myView, myUuid, myTxn >>

DoCAS(self) == /\ pc[self] = "DoCAS"
               /\ IF myUuid[self] \in UuidsOf(storage.committed)
                     THEN /\ terminalResult' = [terminalResult EXCEPT ![self] = "committed"]
                          /\ pc' = [pc EXCEPT ![self] = "Finish"]
                          /\ UNCHANGED << storage, attempt >>
                     ELSE /\ IF storage.etag = myView[self].etag
                                THEN /\ storage' = Compact(myView[self], myTxn[self])
                                     /\ terminalResult' = [terminalResult EXCEPT ![self] = "committed"]
                                     /\ pc' = [pc EXCEPT ![self] = "Finish"]
                                     /\ UNCHANGED attempt
                                ELSE /\ attempt' = [attempt EXCEPT ![self] = attempt[self] + 1]
                                     /\ pc' = [pc EXCEPT ![self] = "Begin"]
                                     /\ UNCHANGED << storage, terminalResult >>
               /\ UNCHANGED << nextUuid, myView, myUuid, myTxn >>

DoAppend(self) == /\ pc[self] = "DoAppend"
                  /\ IF myUuid[self] \in UuidsOf(storage.committed)
                        THEN /\ terminalResult' = [terminalResult EXCEPT ![self] = "committed"]
                             /\ pc' = [pc EXCEPT ![self] = "Finish"]
                             /\ UNCHANGED storage
                        ELSE /\ storage' = [storage EXCEPT
                                            !.length = @ + 1,
                                            !.log = Append(@, myTxn[self]),
                                            !.committed = @ \cup {myTxn[self]}]
                             /\ terminalResult' = [terminalResult EXCEPT ![self] = "committed"]
                             /\ pc' = [pc EXCEPT ![self] = "Finish"]
                  /\ UNCHANGED << nextUuid, attempt, myView, myUuid, myTxn >>

Exhaust(self) == /\ pc[self] = "Exhaust"
                 /\ terminalResult' = [terminalResult EXCEPT ![self] = "exhausted"]
                 /\ pc' = [pc EXCEPT ![self] = "Finish"]
                 /\ UNCHANGED << storage, nextUuid, attempt, myView, myUuid, 
                                 myTxn >>

Finish(self) == /\ pc[self] = "Finish"
                /\ TRUE
                /\ pc' = [pc EXCEPT ![self] = "Done"]
                /\ UNCHANGED << storage, nextUuid, terminalResult, attempt, 
                                myView, myUuid, myTxn >>

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
  /\ storage \in [etag: Nat, length: Nat,
                  log: Seq([uuid: Nat, created: SUBSET Nat]),
                  committed: SUBSET [uuid: Nat, created: SUBSET Nat]]
  /\ nextUuid \in Nat
  /\ terminalResult \in [Writers -> {"unknown", "committed", "exhausted"}]

\* S1: every log entry is a txn record whose uuid is in `committed`. Every
\* append adds to both, so this holds by construction; it is not a
\* linearizability check.
S1_LogEntriesCommitted ==
  \A i \in 1..Len(storage.log):
    storage.log[i].uuid \in UuidsOf(storage.committed)

\* S2: the set of committed UUIDs is monotone — once a UUID appears, it never
\* disappears. GC of the committed set is not modelled. This is the property
\* that catches a CAS without the etag check; it does not catch an append
\* without the offset check (see README, Known gaps).
S2_NoLostCommit ==
  [][UuidsOf(storage.committed) \subseteq UuidsOf(storage'.committed)]_<<storage>>

\* S3: each UUID appears at most once in `log`. Vacuous as modelled: no writer
\* retries after its write lands (lost replies are not modelled), so the
\* "already committed" branches of DoCAS / DoAppend are never taken.
S3_IdempotentReplay ==
  \A i, j \in 1..Len(storage.log):
    (storage.log[i].uuid = storage.log[j].uuid) => (i = j)

\* S4: no entity id is allocated by two distinct committed transactions.
\* Expected to FAIL under eager snapshot-local id allocation: two writers
\* that read the same snapshot pick the same `nextId` and both append.
\* Fixed in production code 808bca7 by late binding at replay.
S4_UniqueEntityIds ==
  \A t1, t2 \in storage.committed:
    (t1.uuid # t2.uuid) => (t1.created \cap t2.created = {})

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
