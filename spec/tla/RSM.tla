------------------------------- MODULE RSM -------------------------------
(***************************************************************************)
(* The replicated state machine both commit models share: the catalog's    *)
(* state, abstracted to entity versions, and replay of a log of intention  *)
(* and GC records onto a checkpoint (intentlog-core's Replay, as of        *)
(* fileio-catalog 1ee25b4). A model of the commit protocol over some log   *)
(* extends it: CatalogCommit (one object, conditional writes) and          *)
(* SharedLogCommit (a shared log, checkpoints apart from it).              *)
(*                                                                         *)
(* A transaction reads some entities at the versions its snapshot had,     *)
(* writes some of them (bumping their versions), and may create an entity  *)
(* under one it writes. Replay applies a record only if the committed set  *)
(* and its horizon admit its id (mayApply) and its reads still hold        *)
(* (Intention.verify). `hist`, inside the state, is a ghost: the records   *)
(* replay applied, in order, carried through checkpoints.                  *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
  Entities,  \* the state's entities, by name
  None,      \* no value
  Bugs       \* bugs put back, by name (strings); each model says which it has

Has(b) == b \in Bugs
Max(a, b) == IF a > b THEN a ELSE b
Min(a, b) == IF a < b THEN a ELSE b
SetMax(S) == CHOOSE x \in S : \A y \in S : y <= x

\* The state a checkpoint holds. `made` holds the entities creates added:
\* [id, p] for an entity with id `id` under `p`. `next` is the next id
\* replay binds (Checkpoint.next_table_id). `hist` is a ghost.
InitSt == [ver |-> [e \in Entities |-> 1], made |-> {}, next |-> 1, hist |-> << >>]

\* Intention.verify: the record's reads hold.
Verify(st, r) == \A e \in DOMAIN r.rd : st.ver[e] = r.rd[e]

\* The id a create gets: bound at replay (ProtoCodec.IdBinding), or, with
\* Bug 10, the one the writer took from its snapshot.
BindId(st, r) == IF r.cid > 0 THEN r.cid ELSE st.next

\* Intention.apply. A create of an id already in use replaces that entity,
\* as replay's tableById did (Bug 10). Each entity written moves one past
\* its version where the record lands; with "basever", one past its version
\* where the record was staged (wv), as a table version set to the record's
\* base + 1 does, which a conflict encoding that admits a record on a later
\* version makes go back.
Apply(st, r) ==
  LET id == BindId(st, r) IN
  [ver  |-> [e \in Entities |->
               IF e \in r.wr THEN (IF Has("basever") THEN r.wv[e] ELSE st.ver[e]) + 1
               ELSE st.ver[e]],
   made |-> IF r.cr = None THEN st.made
            ELSE {m \in st.made : m.id # id} \cup {[id |-> id, p |-> r.cr]},
   next |-> IF r.cr = None THEN st.next ELSE Max(st.next, id + 1),
   hist |-> Append(st.hist, r)]

\* CommittedSet.contains: committed, absent, or unknown (at or below the
\* horizon: DedupHorizonExceededException).
Contains(C, H, id) ==
  IF id \in C THEN "yes" ELSE IF H > 0 /\ id.ts <= H THEN "unknown" ELSE "no"

\* CommittedSet.Builder.mayApply.
MayApply(C, H, id) == id \notin C /\ (H = 0 \/ id.ts > H)

\* A log holds intention records and GC records, [wm]: AdvanceHorizon(wm),
\* which drops the committed ids at or below wm and raises the horizon to the
\* newest it dropped. Only GC records move the horizon.
IsGc(r) == "wm" \in DOMAIN r
Txns(log) == SelectSeq(log, LAMBDA r : ~IsGc(r))

Gc(a, wm) ==
  LET drop == {i \in a.C : i.ts <= wm}
  IN IF drop = {} THEN a
     ELSE [a EXCEPT !.C = @ \ drop, !.H = Max(@, SetMax({i.ts : i \in drop}))]

\* What replay folds a log into: the state, the committed set and horizon,
\* whether a record failed the read (Bug 64a), and what replay did with the
\* first record of the id `watch` it judged (Replay.Outcome): "unseen",
\* "applied" or "rejected".
Acc(st, C, H, watch) == [st |-> st, C |-> C, H |-> H, err |-> FALSE, watch |-> watch,
                         out |-> "unseen"]

\* Replay.replay over a log. With Bug 64a, replay asks the set (contains)
\* about each record, which throws at the horizon.
RECURSIVE Fold(_, _)
Fold(a, log) ==
  IF log = << >> \/ a.err THEN a
  ELSE LET r == Head(log)
           seen == r.id = a.watch /\ a.out = "unseen"
           b == IF IsGc(r) THEN Gc(a, r.wm)
                ELSE IF Has("64a") /\ Contains(a.C, a.H, r.id) = "unknown"
                  THEN [a EXCEPT !.err = TRUE]
                ELSE IF ~MayApply(a.C, a.H, r.id) THEN a
                ELSE IF Verify(a.st, r)
                  THEN [a EXCEPT !.st = Apply(@, r), !.C = @ \cup {r.id},
                                 !.out = IF seen THEN "applied" ELSE @]
                ELSE [a EXCEPT !.out = IF seen THEN "rejected" ELSE @]
       IN Fold(b, Tail(log))

\* LogRuntime's landed(): what a replay R says of the record of `id`. The
\* replay's judgment of the record where it landed comes first; else the
\* committed set, which cannot say at or below its horizon. "yes": it
\* applied; "no": it did not; "unknown": the set cannot tell.
Landed(R, id) ==
  CASE R.out = "applied" -> "yes"
    [] R.out = "rejected" -> "no"
    [] OTHER -> Contains(R.C, R.H, id)

\* S3: an entity's version counts the records that changed it. Versions are
\* the stamps a conflict encoding judges a record by: one that went back
\* would let a record that lost a race verify later (S2).
CountsChanges(st) ==
  \A e \in Entities : st.ver[e] = 1 + Len(SelectSeq(st.hist, LAMBDA r : e \in r.wr))

-----------------------------------------------------------------------------
(* Serial execution: what the properties compare a history with *)

IdsOf(h) == {h[i].id : i \in 1..Len(h)}

\* The serial execution of the applied transactions, in order: each one's
\* reads (as its snapshot had them, `ird`) hold where it applies, and
\* creates bind fresh ids. ok is FALSE if some transaction's reads do not
\* hold.
ValidOn(st, r) == \A e \in DOMAIN r.ird : st.ver[e] = r.ird[e]
SerialApply(st, r) == [Apply(st, [r EXCEPT !.cid = 0]) EXCEPT !.hist = Append(st.hist, r)]
RECURSIVE SerialFold(_, _)
SerialFold(st, h) ==
  IF h = << >> THEN [ok |-> TRUE, st |-> st]
  ELSE IF ~ValidOn(st, Head(h)) THEN [ok |-> FALSE, st |-> st]
  ELSE SerialFold(SerialApply(st, Head(h)), Tail(h))

=============================================================================
