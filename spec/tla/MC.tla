--------------------------------- MODULE MC ---------------------------------
(* Scenarios for CatalogCommit: what each writer commits, in order. *)
EXTENDS CatalogOracles

Ents == {"a", "b"}

\* A transaction (CatalogFile.Mut, a catalog transaction), and an operation
\* FileIOCatalog restages on a fresh read when it conflicts.
T(rd, wr) == [rd |-> rd, wr |-> wr, cr |-> None, retry |-> FALSE]
Op(rd, wr) == [rd |-> rd, wr |-> wr, cr |-> None, retry |-> TRUE]
Create(p) == [rd |-> {p}, wr |-> {p}, cr |-> p, retry |-> FALSE]

\* Writer 2 reads what writer 1 writes.
Contend == [w \in {1, 2} |->
              IF w = 1 THEN << T({"a"}, {"a"}) >> ELSE << T({"a", "b"}, {"b"}) >>]

\* Two transactions each, so compaction races appends.
TwoEach == [w \in {1, 2} |->
              IF w = 1 THEN << T({"a"}, {"a"}), T({"b"}, {"b"}) >>
              ELSE << T({"a", "b"}, {"b"}), T({"a"}, {"a"}) >>]

\* Writer 1 fills the log and compacts it while writer 2's append retries
\* (the shrink path).
Long == [w \in {1, 2} |->
           IF w = 1 THEN << T({"a"}, {"a"}), T({"a"}, {"a"}), T({"a"}, {"a"}) >>
           ELSE << T({"b"}, {"b"}) >>]

\* Creates in different parents (Bug 10).
Creates == [w \in {1, 2} |-> << Create(IF w = 1 THEN "a" ELSE "b") >>]

\* A transaction that only reads.
ReadOnly == [w \in {1, 2} |->
               IF w = 1 THEN << T({"a"}, {"a"}) >> ELSE << T({"a"}, {}) >>]

\* Three writers, one transaction each.
Three == [w \in {1, 2, 3} |->
            CASE w = 1 -> << T({"a"}, {"a"}) >>
              [] w = 2 -> << T({"a", "b"}, {"b"}) >>
              [] OTHER -> << T({"b"}, {"b"}) >>]

\* Operations FileIOCatalog restages, and a catalog transaction
\* (TestCatalogFaultProperties).
OpsAndTxn == [w \in {1, 2} |->
                IF w = 1 THEN << Op({"a"}, {"a"}), T({"a", "b"}, {"b"}) >>
                ELSE << Op({"a"}, {"a"}) >>]

\* TestCatalogRaceProperties: A (writer 1) and two racers. RaceOp: A is an
\* operation that conflicts with the first racer; RaceTxn: A is a catalog
\* transaction that reads what the first racer writes.
RaceOp == [w \in {1, 2, 3} |->
             CASE w = 1 -> << Op({"a"}, {"a"}) >>
               [] w = 2 -> << Op({"a"}, {"a"}) >>
               [] OTHER -> << Op({"b"}, {"b"}) >>]
RaceTxn == [w \in {1, 2, 3} |->
              CASE w = 1 -> << T({"a", "b"}, {"b"}) >>
                [] w = 2 -> << Op({"a"}, {"a"}) >>
                [] OTHER -> << Op({"b"}, {"b"}) >>]
\* One racer: A, a catalog transaction, reads what it writes.
RaceTxn1 == [w \in {1, 2} |->
               IF w = 1 THEN << T({"a", "b"}, {"b"}) >> ELSE << Op({"a"}, {"a"}) >>]
RaceCreates1 == [w \in {1, 2} |->
                   << [Create(IF w = 1 THEN "a" ELSE "b") EXCEPT !.retry = TRUE] >>]
RaceCreates == [w \in {1, 2, 3} |->
                  CASE w = 1 -> << [Create("a") EXCEPT !.retry = TRUE] >>
                    [] w = 2 -> << [Create("b") EXCEPT !.retry = TRUE] >>
                    [] OTHER -> << Op({"b"}, {"b"}) >>]

NoSkew2 == [w \in {1, 2} |-> 0]
NoSkew3 == [w \in {1, 2, 3} |-> 0]
Behind2 == [w \in {1, 2} |-> IF w = 1 THEN 0 ELSE 2]
Behind3 == [w \in {1, 2, 3} |-> IF w = 1 THEN 0 ELSE 2]

\* Retention: no GC.
GcOff == -1
-----------------------------------------------------------------------------
(* Witnesses: each Not_ invariant must be violated by some configuration
   (check.py), so the passing configurations are known to reach the branch. *)

Not_Shrink == \A w \in Writers : ws[w].pc # "shrink"           \* append race, file compacted
Not_CasAfterFreshRead == \A w \in Writers : ws[w].pc # "cas"   \* FreshForCAS
Not_Rebase == \A w \in Writers : ws[w].pc # "rebase"           \* CAS conflict
Not_StaleAppend == \A w \in Writers : ~(ws[w].pc = "loop" /\ ws[w].stale)
Not_Throttled == \A w \in Writers : ws[w].pc # "throttled"
Not_Full == \A w \in Writers : ~ws[w].full                     \* SegmentLimitException
Not_FinalRead == \A w \in Writers :
  ~(ws[w].pc = "loop" /\ ws[w].att >= MaxAttempts /\ ~ws[w].amb /\ ws[w].unres)
Not_LateLanding == \A i \in inflight : file.gen # i.x.cond
\* A record twice in the log: exactly-once is not vacuous.
Not_DuplicateRecord ==
  \A i, j \in 1..Len(file.log) : i # j => file.log[i].id # file.log[j].id
\* A record in the log that replay rejected, of a commit that failed.
Not_FailedButLanded ==
  \A e \in ended : e.out = "failed" => \A i \in 1..Len(file.log) : file.log[i].id # e.id
Not_Horizon == file.chk.H = 0
Not_UnknownWithoutFault == \A e \in ended : e.out = "unknown" => e.cf
Not_Unknown == \A e \in ended : e.out # "unknown"
Not_CommittedAfterFault == \A e \in ended : e.cf => e.out # "committed"
\* The log runs past max.append.count (a soft limit).
Not_Overshoot == Len(file.log) <= MaxCount \/ MaxCount = 0
\* The fuzzers' schedules: one operation ran inside another; two did (race);
\* an operation was restaged; A, a transaction, failed by a race.
Not_Preempted == sched.racers = {}
Not_TwoRacers == Cardinality(sched.racers) < 2
Not_Restaged == \A w \in Writers : ws[w].pc # "restage"
Not_RaceFailedA == res[1] # "failed"
\* Interleavings the fuzzers do not produce: a timed-out write that can still
\* land after its commit reported; a writer whose clock is at or behind the
\* horizon; a writer resolving a lost reply after GC dropped its id.
Not_LateLandingAfterEnd ==
  \A i \in inflight : file.gen = i.x.cond => ws[i.w].txn # None /\ ws[i.w].txn.id = i.id
Not_ClockBehindHorizon == \A w \in Writers : Clock(w) > file.chk.H
Not_ResolveAfterGc ==
  \A w \in Writers :
    ~(ws[w].pc = "resolve" /\ ws[w].txn.id \in IdsOf(Hist) /\ ws[w].txn.id \notin LiveR.C)
Not_Exhausted == \A w \in Writers : ~(ws[w].pc = "loop" /\ ws[w].att >= MaxAttempts)
=============================================================================
