------------------------------- MODULE RSM -------------------------------
(***************************************************************************)
(* The replicated state machine both commit models share: the catalog's    *)
(* state, abstracted to entity versions, and replay of a log of intention, *)
(* GC and PageOut records onto a checkpoint (intentlog-core's Replay, as   *)
(* of fileio-catalog 1ee25b4; the committed set's pages as                 *)
(* docs/COMMITTED_PAGING.md designs them, ahead of their Java). A model of *)
(* the commit protocol over some log extends it: CatalogCommit (one        *)
(* object, conditional writes) and SharedLogCommit (a shared log,          *)
(* checkpoints apart from it).                                             *)
(*                                                                         *)
(* A transaction reads some entities at the versions its snapshot had,     *)
(* writes some of them (bumping their versions), and may create an entity  *)
(* under one it writes. Replay applies a record only if the committed set  *)
(* admits its id (mayApply) and its reads still hold (Intention.verify).   *)
(* `hist`, inside the state, is a ghost: the records replay applied, in    *)
(* order, carried through checkpoints.                                     *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
  Entities,  \* the state's entities, by name
  None,      \* no value
  Bugs,      \* bugs put back, by name (strings); each model says which it has
  BlockIds,  \* N: the most inline ids a block holds (a level-0 block may hold more)
  GridBits   \* the grid's height: stamps lie in 0 .. 2^GridBits - 1

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

-----------------------------------------------------------------------------
(* The committed set (CommittedSet), paged as docs/COMMITTED_PAGING.md
   designs it: the inline ids C, above the floor F; references P to pages,
   which hold the ids in (H, F]; and the horizon H, at or below which
   nothing is known. Without PageOut records, F = H and P = {}: the set
   before paging. *)

InitSet == [C |-> {}, H |-> 0, F |-> 0, P |-> {}]

\* What a checkpoint stores of a replay: the state and the set.
Ck(a) == [st |-> a.st, C |-> a.C, H |-> a.H, F |-> a.F, P |-> a.P]

\* The grid: a node [lv, ix] is the span [ix * 2^lv, (ix + 1) * 2^lv).
Top(b) == (b.ix + 1) * 2^b.lv - 1
Low(b) == b.ix * 2^b.lv
IdsIn(C, b) == {i \in C : i.ts \div 2^b.lv = b.ix}
CountIn(C, k, ts) == Cardinality({i \in C : i.ts \div 2^k = ts \div 2^k})

\* The block rule: an id's block is the largest node that holds it and at
\* most N of the inline ids (its level-0 node if none does). The blocks are
\* a function of the inline ids alone: no history and no clock.
LevelAt(C, ts) ==
  LET ks == {k \in 0..GridBits : CountIn(C, k, ts) <= BlockIds}
  IN IF ks = {} THEN 0 ELSE SetMax(ks)
BlockAt(C, ts) == LET k == LevelAt(C, ts) IN [lv |-> k, ix |-> ts \div 2^k]
Blocks(C) == {BlockAt(C, i.ts) : i \in C}
NewestTs(C) == SetMax({i.ts : i \in C})
OldestTs(C) == CHOOSE t \in {i.ts : i \in C} : \A i \in C : t <= i.ts

\* The oldest block: the oldest id's (blocks are disjoint aligned nodes, so
\* any block below it would hold an older id).
Oldest(C) == BlockAt(C, OldestTs(C))

\* The oldest block may be sealed: it is not the newest.
Sealable(C) == C # {} /\ Top(Oldest(C)) < NewestTs(C)

\* A log holds intention records; GC records, [wm] (AdvanceHorizon(wm));
\* and PageOut records, [lv, ix, dg]: a node and the digest of the ids its
\* writer paged (the model's digest is the set of ids itself).
IsGc(r) == "wm" \in DOMAIN r
IsPo(r) == "dg" \in DOMAIN r
IsTxn(r) == ~IsGc(r) /\ ~IsPo(r)
Txns(log) == SelectSeq(log, IsTxn)

\* The PageOut a compactor proposes for the oldest block of C.
PoOf(C) == LET b == Oldest(C) IN [lv |-> b.lv, ix |-> b.ix, dg |-> IdsIn(C, b)]

\* A page's name: its node and digest (and so its body).
PageName(p) == [lv |-> p.lv, ix |-> p.ix, dg |-> p.dg]

\* Replay of PageOut: it seals the oldest block if that is still the block
\* its writer paged (the node, then the digest; with "nodigest", the node
\* alone) and is not the newest, and raises the floor to the block's top.
\* Otherwise it changes nothing. A reference keeps the node, the record's
\* digest, the top and the block's newest stamp (mx: the horizon when GC
\* drops it); lo, the floor below it, is a ghost.
PageOut(a, po) ==
  IF a.C = {} THEN a
  ELSE LET b == Oldest(a.C)
           ids == IdsIn(a.C, b)
       IN IF b.lv # po.lv \/ b.ix # po.ix \/ Top(b) >= NewestTs(a.C) THEN a
          ELSE IF ~Has("nodigest") /\ ids # po.dg THEN a
          ELSE [a EXCEPT !.C = @ \ ids, !.F = Top(b),
                         !.P = @ \cup {[lv |-> b.lv, ix |-> b.ix, dg |-> po.dg, top |-> Top(b),
                                        mx |-> SetMax({i.ts : i \in ids}), lo |-> a.F]}]

\* AdvanceHorizon(wm): GC drops the pages whose top is at or below wm (a
\* page that straddles it stays whole) and the inline ids at or below it,
\* raises the horizon to the newest stamp it dropped (a page's mx), and the
\* floor to the horizon. Only GC records move the horizon.
Gc(a, wm) ==
  LET dp == {p \in a.P : p.top <= wm}
      di == {i \in a.C : i.ts <= wm}
      h  == Max(a.H, SetMax({p.mx : p \in dp} \cup {i.ts : i \in di} \cup {0}))
  IN IF dp = {} /\ di = {} THEN a
     ELSE [a EXCEPT !.C = @ \ di, !.P = @ \ dp, !.H = h, !.F = Max(@, h)]

\* CommittedSet.contains: committed, absent, paged (in (H, F]: the runtime
\* asks the page, Lookup) or unknown (at or below the horizon:
\* DedupHorizonExceededException).
Contains(a, id) ==
  IF id \in a.C THEN "yes"
  ELSE IF a.H > 0 /\ id.ts <= a.H THEN "unknown"
  ELSE IF id.ts <= a.F THEN "paged"
  ELSE "no"

\* CommittedSet.Builder.mayApply: closed admission, above the floor only.
\* With "floorgap", above the horizon, as before paging.
MayApply(a, id) == id \notin a.C /\ id.ts > (IF Has("floorgap") THEN a.H ELSE a.F)

\* What replay folds a log into: the state, the committed set (C, H, F, P),
\* whether a record failed the read (Bug 64a), what replay did with the
\* first record of the id `watch` it judged (Replay.Outcome): "unseen",
\* "applied" or "rejected", and (a ghost) how many PageOut records changed
\* nothing.
Acc(st, set, watch) == [st |-> st, C |-> set.C, H |-> set.H, F |-> set.F, P |-> set.P,
                        err |-> FALSE, watch |-> watch, out |-> "unseen", rf |-> 0]

\* Replay.replay over a log. With Bug 64a, replay asks the set (contains)
\* about each record, which throws at the horizon.
RECURSIVE Fold(_, _)
Fold(a, log) ==
  IF log = << >> \/ a.err THEN a
  ELSE LET r == Head(log)
           seen == r.id = a.watch /\ a.out = "unseen"
           b == IF IsGc(r) THEN Gc(a, r.wm)
                ELSE IF IsPo(r)
                  THEN LET p == PageOut(a, r) IN IF p = a THEN [a EXCEPT !.rf = @ + 1] ELSE p
                ELSE IF Has("64a") /\ Contains(a, r.id) = "unknown"
                  THEN [a EXCEPT !.err = TRUE]
                ELSE IF ~MayApply(a, r.id) THEN a
                ELSE IF Verify(a.st, r)
                  THEN [a EXCEPT !.st = Apply(@, r), !.C = @ \cup {r.id},
                                 !.out = IF seen THEN "applied" ELSE @]
                ELSE [a EXCEPT !.out = IF seen THEN "rejected" ELSE @]
       IN Fold(b, Tail(log))

\* LogRuntime's landed(): what a replay R says of the record of `id`. The
\* replay's judgment of the record where it landed comes first; else the
\* committed set, which cannot say at or below its horizon. "yes": it
\* applied; "no": it did not; "unknown": the set cannot tell; "paged": the
\* page that holds it says (LandedP).
Landed(R, id) ==
  CASE R.out = "applied" -> "yes"
    [] R.out = "rejected" -> "no"
    [] OTHER -> Contains(R, id)

\* The references of R whose pages may hold stamp ts; the page holding it is
\* the oldest of them. There is none when ts lies above the newest id of a
\* page GC dropped and at or below its top (H is that id's stamp): nothing
\* applied there.
Covering(R, ts) == {p \in R.P : p.top >= ts}
Holding(R, ts) == CHOOSE p \in Covering(R, ts) : \A p2 \in Covering(R, ts) : p.top <= p2.top

\* landed() on a paged id: the page holding it, fetched from the page store
\* (`store`, a set of page names). "yes", or "absent", which is final
\* (replay rejects the id from here on); "fail" when the get failed (an
\* injected fault) and "missing" when there is no such page (deleted since
\* the read). Neither is ever "absent", but with "absentpage".
Lookup(R, id, store, fail) ==
  IF Covering(R, id.ts) = {} THEN "absent"
  ELSE LET p == Holding(R, id.ts) IN
       IF fail \/ PageName(p) \notin store
         THEN IF Has("absentpage") THEN "absent" ELSE IF fail THEN "fail" ELSE "missing"
       ELSE IF id \in p.dg THEN "yes" ELSE "absent"

\* landed()'s verdict, through the page store when the id is paged.
LandedP(R, id, store, fail) ==
  IF Landed(R, id) = "paged" THEN Lookup(R, id, store, fail) ELSE Landed(R, id)

\* Whether landed() fetches a page (so a get can fail).
NeedsPage(R, id) == Landed(R, id) = "paged" /\ Covering(R, id.ts) # {}

\* S3: an entity's version counts the records that changed it. Versions are
\* the stamps a conflict encoding judges a record by: one that went back
\* would let a record that lost a race verify later (S2).
CountsChanges(st) ==
  \A e \in Entities : st.ver[e] = 1 + Len(SelectSeq(st.hist, LAMBDA r : e \in r.wr))

-----------------------------------------------------------------------------
(* Serial execution, and the pages: what the properties compare with *)

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

\* Every reference names a page in the store: a landed PageOut's page
\* exists until a GC record drops it (oracle 2 of COMMITTED_PAGING.md).
PagesExistIn(a, store) == \A p \in a.P : PageName(p) \in store

\* Every page holds exactly the ids that applied in its range, (lo, top].
PagesExactIn(a, h) ==
  \A p \in a.P : p.dg = {i \in IdsOf(h) : p.lo < i.ts /\ i.ts <= p.top}

\* The set's shape: the horizon at or below the floor, the inline ids above
\* it, the pages' tops in (H, F].
SetShape(a) ==
  /\ a.H <= a.F
  /\ \A i \in a.C : i.ts > a.F
  /\ \A p \in a.P : a.H < p.top /\ p.top <= a.F

\* Every stamp lies on the grid.
OnGridIn(log) == \A i \in 1..Len(log) : IsTxn(log[i]) => log[i].id.ts < 2^GridBits

=============================================================================
