------------------------------ MODULE MCShared ------------------------------
(* Scenarios for SharedLogCommit: what each writer commits, in order. *)
EXTENDS SharedLogOracles

Ents == {"a", "b"}

T(rd, wr) == [rd |-> rd, wr |-> wr, cr |-> None]
Create(p) == [rd |-> {p}, wr |-> {p}, cr |-> p]

\* Tables and views (docs/VIEWS.md; RSM's named actions), as MC's: a writer
\* whose read refuses one ends it with no commit.
Nm(op, k, p, n, by, tp, tn) ==
  [rd |-> {}, wr |-> {}, cr |-> None,
   nm |-> [op |-> op, k |-> k, p |-> p, n |-> n, by |-> by, tp |-> tp, tn |-> tn]]
Make(k, p, n) == Nm("create", k, p, n, "name", None, None)
MakeByVersion(k, p, n) == Nm("create", k, p, n, "version", None, None)
Drop(k, p, n) == Nm("drop", k, p, n, None, None, None)
Move(k, p, n, tp, tn) == Nm("rename", k, p, n, "name", tp, tn)

\* The ABA of a create by name (design.md I13): writer 1's create of a.x is
\* rejected while writer 2's entity of the other kind holds the name, which
\* writer 2 then frees; a copy of writer 1's record still in flight
\* (LandInflight) must stay rejected, by the namespace's removals.
ViewFreed == [w \in {1, 2} |->
                IF w = 1 THEN << Make("table", "a", "x") >>
                ELSE << Make("view", "a", "x"), Drop("view", "a", "x") >>]
TableFreed == [w \in {1, 2} |->
                 IF w = 1 THEN << Make("view", "a", "x") >>
                 ELSE << Make("table", "a", "x"), Drop("table", "a", "x") >>]
TableMoved == [w \in {1, 2} |->
                 IF w = 1 THEN << Make("view", "a", "x") >>
                 ELSE << Make("table", "a", "x"), Move("table", "a", "x", "a", "y") >>]
\* Writer 1 binds by the namespace's version, which writer 2's create moves.
ViewFreedByVersion == [w \in {1, 2} |->
                         IF w = 1 THEN << MakeByVersion("table", "a", "x") >>
                         ELSE << Make("view", "a", "x"), Drop("view", "a", "x") >>]

\* Writer 2 reads what writer 1 writes.
Contend == [w \in {1, 2} |->
              IF w = 1 THEN << T({"a"}, {"a"}) >> ELSE << T({"a", "b"}, {"b"}) >>]

\* Two transactions each.
TwoEach == [w \in {1, 2} |->
              IF w = 1 THEN << T({"a"}, {"a"}), T({"b"}, {"b"}) >>
              ELSE << T({"a", "b"}, {"b"}), T({"a"}, {"a"}) >>]

\* Each writes what the other reads, and not what it reads itself: a copy
\* of a record that applied would apply again but for its id (write skew,
\* too).
Blind == [w \in {1, 2} |->
            IF w = 1 THEN << T({"a"}, {"b"}) >> ELSE << T({"b"}, {"a"}) >>]

\* Creates in different parents.
Creates == [w \in {1, 2} |-> << Create(IF w = 1 THEN "a" ELSE "b") >>]

\* Three writers, one transaction each.
Three == [w \in {1, 2, 3} |->
            CASE w = 1 -> << T({"a"}, {"a"}) >>
              [] w = 2 -> << T({"a", "b"}, {"b"}) >>
              [] OTHER -> << T({"b"}, {"a"}) >>]

\* Conflict encodings: a table's concerns are entities, and its version, v,
\* one more that every change of the table writes. A record reads the
\* concerns it depends on: a property change (writer 1) and an append to
\* main (writer 2) read only their own concern, so either applies after the
\* other (concern-vector); a change the encoding cannot see into, full
\* metadata or a schema change (writer 3), reads v, the Lamport witness.
CEnts == {"v", "props", "main"}
Concerns == [w \in {1, 2, 3} |->
               CASE w = 1 -> << T({"props"}, {"props", "v"}) >>
                 [] w = 2 -> << T({"main"}, {"main", "v"}) >>
                 [] OTHER -> << T({"v"}, {"v", "props", "main"}) >>]
\* Two of each: compaction and copies race the concern records.
Concerns2 == [w \in {1, 2} |->
                IF w = 1 THEN << T({"props"}, {"props", "v"}), T({"v"}, {"v", "props", "main"}) >>
                ELSE << T({"main"}, {"main", "v"}), T({"props"}, {"props", "v"}) >>]
\* Blind appends (blind-append): each reads nothing, so both apply in either
\* order; a copy of one would apply again but for its id.
BlindAppends == [w \in {1, 2} |-> << T({}, {"main", "v"}) >>]
\* Validated commits (partition-vector, manifest-window): a commit to main that
\* checks a partition reads it and writes it. Commits checking and writing
\* partitions p1 and p2 (writers 1 and 2) apply in either order; a third on p1
\* conflicts with the first, whichever lands second.
PEnts == {"v", "main", "p1", "p2"}
Validated == [w \in {1, 2, 3} |->
                CASE w = 1 -> << T({"p1"}, {"p1", "main", "v"}) >>
                  [] w = 2 -> << T({"p2"}, {"p2", "main", "v"}) >>
                  [] OTHER -> << T({"p1"}, {"p1", "main", "v"}) >>]

NoSkew2 == [w \in {1, 2} |-> 0]
NoSkew3 == [w \in {1, 2, 3} |-> 0]
Behind2 == [w \in {1, 2} |-> IF w = 1 THEN 0 ELSE 2]

\* Retention: no GC records.
GcOff == -1
-----------------------------------------------------------------------------
(* Witnesses: each Not_ invariant must be violated by some configuration
   (check.py), so the passing configurations are known to reach the branch. *)

\* A reader can start from a checkpoint other than the first, and two can
\* (Deterministic is not vacuous); the log was trimmed.
Not_LaterCheckpoint == \A c \in Usable : c.pos = 0
Not_TwoUsable == Cardinality(Usable) < 2
Not_Trimmed == trimmed = 0
\* A read back finds the log trimmed past the record (TrimmedException).
Not_ReadBackTrimmed == \A w \in Writers : ~(ws[w].pc = "readback" /\ trimmed >= ws[w].pos)
\* A GC record dropped the id of a record that applied, before its writer
\* read it back: the case judging by the committed set gets wrong.
Not_GcBeforeReadBack ==
  \A w \in Writers :
    ~(ws[w].pc = "readback" /\ CommitOf(ws[w].txn) \in CommitsOf(Hist)
      /\ ws[w].txn.id \notin Full.C)
\* A record skipped because the horizon passed its id before it landed.
Not_RecordBelowHorizon == \A e \in ended : ~(e.out = "failed" /\ e.cx)
\* A record twice in the log (exactly-once is not vacuous); a copy landing
\* after its commit reported.
Not_DuplicateRecord ==
  \A i, j \in 1..Len(Txns(log)) : i # j => Txns(log)[i].id # Txns(log)[j].id
Not_LateLandingAfterEnd == \A i \in inflight : ws[i.w].txn # None /\ ws[i.w].txn.id = i.rec.id
\* A failed commit whose record is in the log.
Not_FailedButLanded ==
  \A e \in ended : e.out = "failed" => \A i \in 1..Len(Txns(log)) : Txns(log)[i].id # e.id
Not_Horizon == Full.H = 0
Not_Unknown == \A e \in ended : e.out # "unknown"
Not_UnknownWithoutFault == \A e \in ended : e.out = "unknown" => e.cf
Not_Error == \A e \in ended : e.out # "error"
Not_Throttled == \A w \in Writers : ws[w].pc # "throttled"
Not_Exhausted == \A w \in Writers : ~(ws[w].pc = "send" /\ ws[w].att >= MaxAttempts)
\* A commit resolving an ambiguous write after a read of it lagged the log.
Not_LaggingRead == \A w \in Writers : ~(ws[w].lg /\ ws[w].pc = "resolve")
\* Tables and views: a copy of a create its writer reported failed landed
\* after a drop or a rename that applied freed the name it binds (the ABA).
Not_LateCopyAfterFree ==
  ~\E e \in ended : \E i, j \in 1..Len(log) :
     /\ j < i /\ IsTxn(log[i]) /\ IsTxn(log[j])
     /\ e.out = "failed" /\ log[i].id = e.id
     /\ log[i].nm # None /\ log[i].nm.op = "create"
     /\ log[j].nm # None /\ log[j].nm.op \in {"drop", "rename"} /\ log[j].id \in IdsOf(Hist)
     /\ log[j].nm.p = log[i].nm.p /\ log[j].nm.n = log[i].nm.n

\* Paging (COMMITTED_PAGING.md). A PageOut applied: the floor above the
\* horizon. A PageOut that changed nothing (its block changed, or was sealed
\* by another).
Not_Floor == Full.F = Full.H
Not_PageOutRefused == Full.rf = 0
\* The same PageOut twice in the log.
Not_DuplicatePageOut ==
  \A i, j \in 1..Len(log) : (i # j /\ IsPo(log[i])) => log[i] # log[j]
\* A commit about to judge its record whose id is paged.
Not_PagedOwnId ==
  \A w \in Writers : ~(ws[w].pc \in {"readback", "resolve"}
                       /\ Full.H < ws[w].txn.id.ts /\ ws[w].txn.id.ts <= Full.F)
\* A record that landed after a PageOut sealed its stamp, of a commit that
\* failed; a copy of a record whose block a PageOut sealed between them.
Not_FailedBelowFloor ==
  \A e \in ended :
    ~(e.out = "failed" /\ \E i, j \in 1..Len(log) :
         i < j /\ IsPo(log[i]) /\ IsTxn(log[j]) /\ log[j].id = e.id
         /\ e.id.ts <= Top(log[i]) /\ Full.H < e.id.ts)
Not_CopyAfterPageOut ==
  \A i, j, k \in 1..Len(log) :
    ~(i < j /\ j < k /\ IsTxn(log[i]) /\ IsPo(log[j]) /\ IsTxn(log[k])
      /\ log[k].id = log[i].id /\ log[i].id \in log[j].dg)
\* An orphan page: in the store, referenced by no PageOut that applied.
Not_Orphan == \A pg \in store : \E p \in Full.P : PageName(p) = pg
\* A sweep deleted a page; a page that GC dropped.
Not_Swept == comp.sw = 0
PoApplied(i) ==
  LET a == Fold(Acc(InitSt, InitSet, None), SubSeq(log, 1, i - 1)) IN PageOut(a, log[i]) # a
Not_PageDropped ==
  \A i \in 1..Len(log) : (IsPo(log[i]) /\ PoApplied(i)) => \E p \in Full.P : PageName(p) = log[i]
\* Blocks: two inline blocks; a block of more than one id.
Not_TwoBlocks == Cardinality(Blocks(Full.C)) < 2
Not_WideBlock == \A b \in Blocks(Full.C) : b.lv = 0

\* Conditional appends (Cond): a send that lost its race; a commit whose
\* lost send its last read could not settle (unknown); a read settled a
\* loss (the commit goes on); a send of a commit that landed, reported as
\* a lost race, and the commit committed after all.
Not_CondLost == \A w \in Writers : ~ws[w].cl
Not_UnknownUnsettled == \A e \in ended : ~(e.out = "unknown" /\ e.ul)
Not_Settled == \A w \in Writers : ~(ws[w].pc # "idle" /\ ws[w].cl /\ ~ws[w].ur)
Not_LostButCommitted ==
  \A e \in ended : ~(e.out = "committed" /\ e.cf
                     /\ \E i, j \in 1..Len(Txns(log)) :
                          i < j /\ Txns(log)[i].id = e.id /\ Txns(log)[j].id = e.id)
\* A void send; a commit that sent again after one and committed.
Not_Void == \A e \in ended : ~e.vd
Not_VoidCommitted == \A e \in ended : ~(e.vd /\ e.out = "committed")
\* A catalog's read outside a commit lagged behind the replica it held, so
\* it served that replica (Bug 72).
Not_Floored == \A w \in Writers : ~looks[w].fl
=============================================================================
