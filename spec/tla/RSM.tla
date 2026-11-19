------------------------------- MODULE RSM -------------------------------
(***************************************************************************)
(* The replicated state machine both commit models share: the catalog's    *)
(* state, abstracted to entity versions, and replay of a log of intention, *)
(* GC and PageOut records onto a checkpoint (intentlog-core's Replay; the  *)
(* committed set's pages as docs/COMMITTED_PAGING.md designs them and      *)
(* CommittedSet builds them). A model of the commit protocol over some log *)
(* extends it: CatalogCommit (one object, conditional writes) and          *)
(* SharedLogCommit (a shared log, checkpoints apart from it).              *)
(*                                                                         *)
(* A transaction reads some entities at the versions its snapshot had,     *)
(* writes some of them (bumping their versions), and may create an entity  *)
(* under one it writes. Replay applies a record only if the committed set  *)
(* admits its id (mayApply) and its reads still hold (Intention.verify).   *)
(* `hist`, inside the state, is a ghost: the records replay applied, in    *)
(* order, carried through checkpoints.                                     *)
(*                                                                         *)
(* A record may instead carry a named action on a table or a view          *)
(* (docs/VIEWS.md): a create of a name in a namespace, bound by name (the  *)
(* namespace's removals as read, and the name free) or by the namespace's  *)
(* version; a view's update, a drop or a rename at the version read; or a  *)
(* namespace's drop. A namespace's tables and views share its names (V7).  *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
  Entities,  \* the state's entities, by name
  None,      \* no value
  Bugs,      \* bugs put back, by name (strings); each model says which it has
  BlockIds,  \* N: the most inline ids a block holds (a level-0 block may hold more)
  GridBits,  \* the grid's height: stamps lie in 0 .. 2^GridBits - 1
  SealEvery  \* the state's pages (SNAPSHOT_PAGING.md): an entity's history seals into a page at
             \* each multiple of SealEvery of its version; 0 pages nothing

Has(b) == b \in Bugs
Max(a, b) == IF a > b THEN a ELSE b
Min(a, b) == IF a < b THEN a ELSE b
SetMax(S) == CHOOSE x \in S : \A y \in S : y <= x

\* The state a checkpoint holds. `made` holds the entities creates added:
\* [id, p, n, k, v] for an entity with id `id` under `p`, named n (None: a
\* create that names nothing) of kind k ("table" or "view") at version v.
\* `next` is the next id replay binds (Checkpoint.next_table_id). `hist` is a
\* ghost. `sealed` names the state's pages ([e, v]: entity e's history sealed
\* at its version v), as a root names its sealed snapshot blocks. `rm` counts
\* each namespace's removals (Namespace.removals: the names it freed), and
\* `gone` holds the namespaces dropped.
InitSt == [ver |-> [e \in Entities |-> 1], made |-> {}, next |-> 1, hist |-> << >>,
           sealed |-> {}, rm |-> [e \in Entities |-> 0], gone |-> {}]

\* The pages a record that moves the entities `wr` to versions `nv` seals, and
\* those it drops (an expiry that empties an older block: the one sealed
\* SealEvery before, at each multiple of 2 * SealEvery). A function of the
\* log alone, so every replica names the same pages (SnapshotSealing).
Sealed(old, wr, nv) ==
  IF SealEvery = 0 THEN old
  ELSE (old \ {[e |-> e, v |-> nv[e] - SealEvery] : e \in {x \in wr : nv[x] % (2 * SealEvery) = 0}})
       \cup {[e |-> e, v |-> nv[e]] : e \in {x \in wr : nv[x] % SealEvery = 0}}

-----------------------------------------------------------------------------
(* Named actions: tables and views beside each other (docs/VIEWS.md, design.md
   I13). A shape's `nm`, when it has one, is [op, k, p, n, by, tp, tn]: op
   "create" (of kind k, name n in namespace p, bound `by` "name" or
   "version"), "update" (a view's), "drop", "rename" (to name tn in tp, bound
   `by`) of the entity of kind k named n in p, or "dropns" (of namespace p).
   The record's is staged on a read (NamedOn): [op, k, p, n, by, tp, tn, id, v,
   rmv, pv, ok], with the entity's id and version read, the binding
   namespace's removals and version read, and whether the writer's own check
   passed (CatalogMutation: a name taken, an entity absent, a namespace gone or
   not empty fail before any record; such a writer stages none, Refused, and a
   record staged anyway never applies). *)

NmOf(sh) == IF "nm" \in DOMAIN sh THEN sh.nm ELSE None

\* The entity of kind k named n in namespace p, or None.
Named(st, k, p, n) ==
  IF \E m \in st.made : m.k = k /\ m.p = p /\ m.n = n
    THEN CHOOSE m \in st.made : m.k = k /\ m.p = p /\ m.n = n
  ELSE None

\* Whether name n in namespace p is held, by an entity of either kind.
Held(st, p, n) == \E m \in st.made : m.p = p /\ m.n = n
Empty(st, p) == ~\E m \in st.made : m.p = p

\* What a writer checks on its read before it stages a named action.
Refused(sh, st) ==
  LET a == NmOf(sh) IN
  /\ a # None
  /\ CASE a.op = "create" -> a.p \in st.gone \/ Held(st, a.p, a.n)
       [] a.op = "rename" -> Named(st, a.k, a.p, a.n) = None \/ a.tp \in st.gone
                             \/ Held(st, a.tp, a.tn)
       [] a.op = "dropns" -> a.p \in st.gone \/ ~Empty(st, a.p)
       [] OTHER -> Named(st, a.k, a.p, a.n) = None

\* The named action of shape sh staged on st (None for a shape without one).
NamedOn(sh, st) ==
  LET a == NmOf(sh)
      e == IF a = None \/ a.op \in {"create", "dropns"} THEN None ELSE Named(st, a.k, a.p, a.n)
      bp == IF a = None THEN None ELSE IF a.op = "rename" THEN a.tp ELSE a.p
  IN IF a = None THEN None
     ELSE [op |-> a.op, k |-> a.k, p |-> a.p, n |-> a.n, by |-> a.by, tp |-> a.tp, tn |-> a.tn,
           id |-> IF e = None THEN 0 ELSE e.id, v |-> IF e = None THEN 0 ELSE e.v,
           rmv |-> st.rm[bp], pv |-> st.ver[bp], ok |-> ~Refused(sh, st)]

\* Whether a binding of name n for kind k counts an entity of kind `held`
\* (ProtoCodec.nameTaken): both kinds share the names; with "viewnotable" a
\* view's binding ignores tables, with "tablenoview" a table's ignores views.
Counts(held, k) ==
  \/ held = k
  \/ k = "view" /\ ~Has("viewnotable")
  \/ k = "table" /\ ~Has("tablenoview")
Taken(st, p, n, k) == \E m \in st.made : m.p = p /\ m.n = n /\ Counts(m.k, k)

\* ProtoCodec.bindingProblem: the binding of name n in namespace p holds. By
\* the namespace's version, it is unchanged; by name, its removals are
\* (with "absentonly", ignored) and the name is free (with "noabsence",
\* ignored).
Binds(st, a, p, n) ==
  /\ p \notin st.gone
  /\ IF a.by = "version" THEN st.ver[p] = a.pv
     ELSE /\ Has("absentonly") \/ st.rm[p] = a.rmv
          /\ Has("noabsence") \/ ~Taken(st, p, n, a.k)

\* The entity a acts on is at the version read; with "viewstale" a view's
\* update needs only the view.
At(st, a) == \E m \in st.made : m.id = a.id /\ m.k = a.k /\ m.v = a.v
Exists(st, a) == \E m \in st.made : m.id = a.id /\ m.k = a.k

\* The named action's verify (CreateViewAction, UpdateViewAction, ... and the
\* table's).
NamedVerify(st, a) ==
  \/ a = None
  \/ /\ a.ok
     /\ CASE a.op = "create" -> Binds(st, a, a.p, a.n)
          [] a.op = "update" -> At(st, a) \/ (a.k = "view" /\ Has("viewstale") /\ Exists(st, a))
          [] a.op = "drop" -> At(st, a)
          [] a.op = "rename" -> At(st, a) /\ Binds(st, a, a.tp, a.tn)
          [] OTHER -> a.p \notin st.gone /\ st.ver[a.p] = a.pv

\* The namespaces whose versions a named action moves: a create's (with
\* "viewnobump", not a view's), a drop's, both of a rename's, the namespace
\* dropped. Each is one past its version where the record lands.
NamedWr(a) ==
  IF a = None THEN {}
  ELSE CASE a.op = "create" -> IF a.k = "view" /\ Has("viewnobump") THEN {} ELSE {a.p}
         [] a.op = "update" -> {}
         [] a.op = "rename" -> {a.p, a.tp}
         [] OTHER -> {a.p}

\* What a named action, given id `id` if it creates, makes of the entities: an
\* update and a rename set the version to the one read, plus one.
NamedMade(st, a, id) ==
  CASE a.op = "create" ->
         {m \in st.made : m.id # id} \cup {[id |-> id, p |-> a.p, n |-> a.n, k |-> a.k, v |-> 1]}
    [] a.op = "update" -> {IF m.id = a.id THEN [m EXCEPT !.v = a.v + 1] ELSE m : m \in st.made}
    [] a.op = "drop" -> {m \in st.made : m.id # a.id}
    [] a.op = "rename" ->
         {IF m.id = a.id THEN [m EXCEPT !.p = a.tp, !.n = a.tn, !.v = a.v + 1] ELSE m
            : m \in st.made}
    [] OTHER -> st.made

\* A drop and a rename count the name they free in its namespace's removals;
\* with "viewdropnoremovals" a view's drop does not, with "dropnobump" a
\* table's, with "renamenobump" a table's rename.
NamedRm(st, a) ==
  IF \/ a.op \notin {"drop", "rename"}
     \/ a.op = "drop" /\ a.k = "view" /\ Has("viewdropnoremovals")
     \/ a.op = "drop" /\ a.k = "table" /\ Has("dropnobump")
     \/ a.op = "rename" /\ a.k = "table" /\ Has("renamenobump")
    THEN st.rm
  ELSE [st.rm EXCEPT ![a.p] = @ + 1]

\* What a named action, applied, asked of the state as its snapshot had it,
\* whatever its record checks: a create's or a rename's name free in a live
\* namespace, the entity at the version read, the namespace dropped empty.
NamedValid(st, a) ==
  \/ a = None
  \/ CASE a.op = "create" -> a.p \notin st.gone /\ ~Held(st, a.p, a.n)
       [] a.op = "rename" -> At(st, a) /\ a.tp \notin st.gone /\ ~Held(st, a.tp, a.tn)
       [] a.op = "dropns" -> a.p \notin st.gone /\ Empty(st, a.p)
       [] OTHER -> At(st, a)

\* I13: no namespace holds two entities of one name, or any once dropped.
WellNamedSt(st) ==
  /\ \A m1, m2 \in st.made : m1.n # None /\ m1.p = m2.p /\ m1.n = m2.n => m1 = m2
  /\ \A m \in st.made : m.p \notin st.gone

-----------------------------------------------------------------------------

\* Intention.verify: the record's reads hold, and its named action's.
Verify(st, r) == (\A e \in DOMAIN r.rd : st.ver[e] = r.rd[e]) /\ NamedVerify(st, r.nm)

\* The entities a record moves.
Wr(r) == r.wr \cup NamedWr(r.nm)

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
  LET id == BindId(st, r)
      a  == r.nm
      creates == r.cr # None \/ (a # None /\ a.op = "create")
      nv == [e \in Entities |->
               IF e \in r.wr THEN (IF Has("basever") THEN r.wv[e] ELSE st.ver[e]) + 1
               ELSE IF e \in NamedWr(a) THEN st.ver[e] + 1
               ELSE st.ver[e]]
  IN
  [ver  |-> nv,
   made |-> IF a # None THEN NamedMade(st, a, id)
            ELSE IF r.cr = None THEN st.made
            ELSE {m \in st.made : m.id # id}
                   \cup {[id |-> id, p |-> r.cr, n |-> None, k |-> "table", v |-> 1]},
   next |-> IF creates THEN Max(st.next, id + 1) ELSE st.next,
   hist |-> Append(st.hist, r),
   sealed |-> Sealed(st.sealed, Wr(r), nv),
   rm   |-> IF a = None THEN st.rm ELSE NamedRm(st, a),
   gone |-> IF a # None /\ a.op = "dropns" THEN st.gone \cup {a.p} ELSE st.gone]

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
  \A e \in Entities : st.ver[e] = 1 + Len(SelectSeq(st.hist, LAMBDA r : e \in Wr(r)))

-----------------------------------------------------------------------------
(* Serial execution, and the pages: what the properties compare with *)

IdsOf(h) == {h[i].id : i \in 1..Len(h)}

\* The serial execution of the applied transactions, in order: each one's
\* reads (as its snapshot had them, `ird`) hold where it applies, and so does
\* what its named action asked of the state (NamedValid), and creates bind
\* fresh ids. ok is FALSE if some transaction's reads do not hold.
ValidOn(st, r) == (\A e \in DOMAIN r.ird : st.ver[e] = r.ird[e]) /\ NamedValid(st, r.nm)
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
