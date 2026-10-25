------------------------------ MODULE MCShared ------------------------------
(* Scenarios for SharedLogCommit: what each writer commits, in order. *)
EXTENDS SharedLogOracles

Ents == {"a", "b"}

T(rd, wr) == [rd |-> rd, wr |-> wr, cr |-> None]
Create(p) == [rd |-> {p}, wr |-> {p}, cr |-> p]

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
=============================================================================
