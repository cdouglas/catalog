---------------------------- MODULE SegmentedLog ----------------------------
(***************************************************************************)
(* docs/LOG_SEGMENTS.md's log, as a SharedLog provider: the catalog object *)
(* and its segments below the SPI that SharedLogCommit.tla takes as one    *)
(* log. Written before the Java, as AzureLog.tla was.                      *)
(*                                                                         *)
(* The catalog object holds the checkpoint and the Installs of segments,   *)
(* each with its record limit, from the checkpoint's segment on. It        *)
(* changes only conditionally: an Install is format 3's conditional        *)
(* append and a checkpoint a CAS replace, both from the version read, so   *)
(* each is one step that lands only if nothing changed the object since.   *)
(* Segments are Append Blobs, appended blind with no condition. A segment  *)
(* ends at its limit-th record, or earlier at its first seal frame. Its    *)
(* records count only once every earlier segment has ended. Several        *)
(* segments may be open at once: a writer sends a record to any segment it *)
(* knows, or copies of it to several, and reads through the first copy     *)
(* the log holds, sealing earlier segments that haven't ended.             *)
(*                                                                         *)
(* Every frame is one block. Each action is one request, so the others     *)
(* interleave between any two. A crash is a writer that takes no more      *)
(* steps, which every behaviour already includes.                          *)
(*                                                                         *)
(* The log the provider presents (Abstract, a ghost) is the records of the *)
(* installed segments, in ordinal order, each up to its end, through the   *)
(* first that hasn't ended. The properties say it is one log as            *)
(* SharedLogCommit's `log` is: it only grows (P1), every acknowledged      *)
(* record is in it where its read-through found it (P3), every read is a   *)
(* slice of it from a checkpoint, checkpoints keep their place, every      *)
(* segment can end (SG8), a copy its writer judged void is never in it     *)
(* (D4: a void append leaves nothing to resolve), and a record has no copy *)
(* in it that its writer doesn't know may have landed (the shared-log      *)
(* model's `dup`). Another log keeps a segment at the same prefix, which   *)
(* its own catalog object names: it must outlive this log's sweeps, Bug    *)
(* 100 (sweep deleted another log's segments). Bug switches put back       *)
(* LOG_SEGMENTS.md's mistakes.                                             *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
  Writers,     \* writer ids (1, 2, ...); 0 is the log's creator
  Recs,        \* Writers -> Seq of record ids: what each writer appends, in order
  Limit,       \* K: the record limit each Install sets
  MaxBlocks,   \* the store's block limit per segment (50,000 on Azure)
  MaxOrd,      \* the highest segment ordinal of a run
  MaxSends,    \* sends of one record, copies included
  MaxCopies,   \* copies a writer sends of a record before it reads through
  AnySeg,      \* TRUE: a writer sends to any segment it knows; FALSE: the first it may
  Ahead,       \* TRUE: a writer may install a segment whenever it sends; FALSE: only when
               \*   no segment it knows takes the record
  MaxFaults,   \* how many faults a run may inject
  FaultKinds,  \* lostReply, notSent, inflight (appends); shortRead (reads)
  MaxCkpt,     \* checkpoints compactors put
  MaxSweep,    \* blobs the sweep deletes
  Readers,     \* reader ids
  MaxReads,    \* reads per reader
  Bugs,        \* the mistakes put back, by name
  Seal,        \* a seal frame's record (a model value)
  None         \* no value (a model value)

ASSUME /\ Limit >= 1 /\ MaxBlocks >= 1 /\ MaxOrd >= 0 /\ MaxSends >= 1 /\ MaxCopies >= 1
       /\ AnySeg \in BOOLEAN /\ Ahead \in BOOLEAN
       /\ FaultKinds \subseteq {"lostReply", "notSent", "inflight", "shortRead"}
       /\ Bugs \subseteq {"catalogseal", "limitatnext", "sealcount", "readahead", "voidack",
                          "createonmissing", "sweepearly", "sweepinflight", "sweepothers",
                          "sdkretry", "overlimit", "shortvoid"}
       \* A log refuses a limit its store can't hold; "overlimit" installs one anyway.
       /\ Limit <= MaxBlocks \/ "overlimit" \in Bugs

Has(b) == b \in Bugs
Min2(a, b) == IF a <= b THEN a ELSE b

\* The limit an Install sets for its own segment. With "limitatnext" it sets
\* none, and the next Install sets it from a count read before.
InstallLim == IF Has("limitatnext") THEN -1 ELSE Limit

\* Segment names: <<log, ordinal, nonce>>, the nonce its creator's id. A
\* segment's blob is under its log's id, so the log is part of its name. This
\* log's are "this"; Other is another log's segment 0 at the same prefix,
\* which that log's catalog object (not modelled) names.
Other == <<"other", 0, 0>>
Names == {<<"this", o, n>> : o \in 0..MaxOrd, n \in Writers \cup {0}} \cup {Other}
LogOf(nm) == nm[1]
OrdOf(nm) == nm[2]
AllRecs == UNION {{Recs[w][i] : i \in 1..Len(Recs[w])} : w \in Writers}

VARIABLES
  blob,      \* Names -> [ex, fr]: whether the blob exists, and its frames [r, n]
             \*   (a deleted blob keeps its frames, as a ghost)
  cat,       \* the catalog object: [ver, ck: [ord, rec, k], segs: Seq of [ord, name, lim, cl],
             \*   ret: names retired]; lim -1: none; cl -1: none ("catalogseal")
  chain,     \* ghost: the names Installs have named, by ordinal (chain[i] has ordinal i - 1)
  lims,      \* ghost: ordinal -> the segment's limit (-1: none yet)
  closedAt,  \* ghost: ordinal -> the end a catalog record set ("catalogseal"), -1 before
  ckpts,     \* the checkpoints put: [ord, rec, k], k its place in Abstract
  pendCk,    \* a checkpoint put whose CAS hasn't run, or None
  inflight,  \* appends timed out and still in flight: [r, name]
  ws,        \* Writers -> the writer's state (WInit)
  rs,        \* Readers -> the reader's state (RInit)
  acks,      \* ghost: the copies read-throughs accepted: [r, ord, idx]
  sends,     \* ghost: per record, the sends its writer knows may have landed
  vok,       \* ghost: every read so far was a slice of Abstract when it ended
  nf,        \* faults injected so far
  comp,      \* compactors' budgets used: [ck, sw]
  seen,      \* ghost, for the witnesses
  voids      \* ghost: the copies writers judged void, [r, ord, idx]

vars == <<blob, cat, chain, lims, closedAt, ckpts, pendCk, inflight, ws, rs, acks, sends, vok,
          nf, comp, seen, voids>>

-----------------------------------------------------------------------------
(* Segments *)

SealsIn(fr) == {j \in 1..Len(fr) : fr[j].r = Seal}
HasSeal(fr) == SealsIn(fr) # {}
FirstSeal(fr) == CHOOSE j \in SealsIn(fr) : \A i \in SealsIn(fr) : j <= i

\* The frames before the first seal frame. Before its end a segment holds
\* only records, so these are the records it may hold.
Lead(fr) == IF HasSeal(fr) THEN FirstSeal(fr) - 1 ELSE Len(fr)

\* How many of a segment's first frames are in the log: up to its end, or all
\* of them while it is open. With "sealcount" a seal frame ends the segment
\* at the count it carries, read before it was sent; with "catalogseal" a
\* catalog record's count (cl) ends it.
Valid(fr, lim, cl) ==
  LET a == Lead(fr)
      b == IF lim >= 0 THEN Min2(a, lim) ELSE a
      c == IF Has("sealcount") /\ HasSeal(fr) THEN Min2(b, fr[FirstSeal(fr)].n) ELSE b
  IN IF cl >= 0 THEN Min2(c, cl) ELSE c

Ended(fr, lim, cl) == HasSeal(fr) \/ (lim >= 0 /\ Lead(fr) >= lim) \/ cl >= 0

Named(c, o) == \E j \in 1..Len(c.segs) : c.segs[j].ord = o
SegOf(c, o) == c.segs[CHOOSE j \in 1..Len(c.segs) : c.segs[j].ord = o]
IdxOf(c, o) == CHOOSE j \in 1..Len(c.segs) : c.segs[j].ord = o
NamesIn(c) == {c.segs[j].name : j \in 1..Len(c.segs)}
LastOrd(c) == c.segs[Len(c.segs)].ord

-----------------------------------------------------------------------------
(* The log the provider presents *)

SegFr(i) == blob[chain[i]].fr
Open(i) == ~Ended(SegFr(i), lims[i - 1], closedAt[i - 1])
SegRecs(i) == LET fr == SegFr(i) IN [j \in 1..Valid(fr, lims[i - 1], closedAt[i - 1]) |-> fr[j].r]

\* The segments in the log: through the first one that is open.
Reach ==
  IF \E i \in 1..Len(chain) : Open(i)
    THEN CHOOSE i \in 1..Len(chain) : Open(i) /\ \A h \in 1..i - 1 : ~Open(h)
    ELSE Len(chain)

RECURSIVE CatTo(_)
CatTo(i) == IF i = 0 THEN << >> ELSE CatTo(i - 1) \o SegRecs(i)
Abstract == CatTo(Reach)

\* A position (o, rec): rec records into segment o. It is in the log when o is
\* in reach and rec within its records; its place is the records before it.
InLog(o, rec) == o + 1 <= Reach /\ rec <= Len(SegRecs(o + 1))
Index(o, rec) == Len(CatTo(o)) + rec

IsSlice(k, v) == k + Len(v) <= Len(Abstract) /\ v = SubSeq(Abstract, k + 1, k + Len(v))

After(o1, r1, o2, r2) == o1 > o2 \/ (o1 = o2 /\ r1 > r2)

-----------------------------------------------------------------------------
(* Writers: the provider's append and read-through *)

WInit == [pc |-> "start", i |-> 1, wc |-> None, n |-> 0, cps |-> << >>, ci |-> 1, lo |-> 0,
          cand |-> None, c |-> 0, tgt |-> None, kn |-> {}]
\* pc: start (read the catalog object), send, rt (read through), seal, lread, create, cas,
\* done. wc: the catalog object the writer holds; n: sends of the current record; cps: its
\* copies with receipts [ord, name, idx], in ordinal order; ci: the copy the read-through is
\* on; lo: the lowest ordinal it may send to; cand: its candidate segment; c: a record count
\* it read; tgt: the segment it is sealing; kn: ordinals it has seen end.

UNCH_W0 == UNCHANGED <<cat, chain, lims, closedAt, ckpts, pendCk, rs, vok, comp>>
UNCH_W == UNCH_W0 /\ UNCHANGED voids

SetW(w, s2) == ws' = [ws EXCEPT ![w] = s2]

NextRec(w, s) ==
  [s EXCEPT !.i = @ + 1, !.n = 0, !.cps = << >>, !.ci = 1, !.lo = 0,
            !.pc = IF s.i + 1 <= Len(Recs[w]) THEN "send" ELSE "done"]

WStart(w) ==
  LET s == ws[w] IN
  /\ s.pc = "start"
  /\ SetW(w, [s EXCEPT !.wc = cat, !.pc = IF s.i <= Len(Recs[w]) THEN "send" ELSE "done"])
  /\ UNCH_W /\ UNCHANGED <<blob, inflight, acks, sends, nf, seen>>

LastCopyOrd(s) == IF s.cps = << >> THEN -1 ELSE s.cps[Len(s.cps)].ord

\* The segments a writer may send the record to: any its catalog object
\* names, at or past lo, after its last copy (with AnySeg FALSE, the first).
Allowed(s) ==
  {j \in 1..Len(s.wc.segs) : s.wc.segs[j].ord >= s.lo /\ s.wc.segs[j].ord > LastCopyOrd(s)}
Eligible(s) ==
  IF AnySeg \/ Allowed(s) = {} THEN Allowed(s)
  ELSE {CHOOSE j \in Allowed(s) : \A h \in Allowed(s) : j <= h}

CanSend(w) == ws[w].pc = "send" /\ ws[w].n < MaxSends

\* 404: the segment was swept; nothing landed. The writer reads the catalog
\* object again; with "createonmissing", it creates the segment.
SendGone(w, j) ==
  LET s == ws[w]
      nm == s.wc.segs[j].name
  IN /\ CanSend(w) /\ j \in Eligible(s) /\ ~blob[nm].ex
     /\ IF Has("createonmissing")
          THEN /\ blob' = [blob EXCEPT ![nm] = [ex |-> TRUE, fr |-> << >>]]
               /\ UNCHANGED ws
          ELSE /\ SetW(w, [s EXCEPT !.pc = "start"])
               /\ UNCHANGED blob
     /\ seen' = [seen EXCEPT !.w404 = TRUE]
     /\ UNCH_W /\ UNCHANGED <<inflight, acks, sends, nf>>

\* The block limit refused it: the segment has ended (its limit is at most
\* the block limit). The writer sends to a later one.
SendFull(w, j) ==
  LET s == ws[w]
      seg == s.wc.segs[j]
  IN /\ CanSend(w) /\ j \in Eligible(s)
     /\ blob[seg.name].ex /\ Len(blob[seg.name].fr) >= MaxBlocks
     /\ SetW(w, [s EXCEPT !.lo = seg.ord + 1, !.kn = @ \cup {seg.ord}])
     /\ seen' = [seen EXCEPT !.full = TRUE]
     /\ UNCH_W /\ UNCHANGED <<blob, inflight, acks, sends, nf>>

Land(b, nm, f) == [b EXCEPT ![nm].fr = Append(@, f)]

\* Append Block lands; its reply gives the block count, the frame's index. An
\* index past the limit is void with no read: the writer sends to a later
\* segment. Else it reads through (or first sends a copy to a later segment).
\* With "sdkretry", the SDK sent it twice and the writer hears of the second.
SendOk(w, j) ==
  LET s == ws[w]
      seg == s.wc.segs[j]
      nm == seg.name
      r == Recs[w][s.i]
      twice == Has("sdkretry") /\ nf < MaxFaults /\ Len(blob[nm].fr) + 1 < MaxBlocks
      b1 == IF twice THEN Land(blob, nm, [r |-> r, n |-> 0]) ELSE blob
      idx == Len(b1[nm].fr) + 1
      void == seg.lim >= 0 /\ idx > seg.lim
  IN /\ CanSend(w) /\ j \in Eligible(s)
     /\ blob[nm].ex /\ Len(blob[nm].fr) < MaxBlocks
     /\ blob' = Land(b1, nm, [r |-> r, n |-> 0])
     /\ nf' = IF twice THEN nf + 1 ELSE nf
     /\ sends' = [sends EXCEPT ![r] = @ + 1]
     /\ IF void
          THEN /\ SetW(w, [s EXCEPT !.n = @ + 1, !.lo = seg.ord + 1, !.kn = @ \cup {seg.ord}])
               /\ voids' = voids \cup {[r |-> r, ord |-> seg.ord, idx |-> idx]}
          ELSE /\ SetW(w, [s EXCEPT !.n = @ + 1, !.pc = "rt",
                                    !.cps = Append(@, [ord |-> seg.ord, name |-> nm, idx |-> idx])])
               /\ UNCHANGED voids
     /\ seen' = [seen EXCEPT !.voidreply = @ \/ void]
     /\ UNCH_W0 /\ UNCHANGED <<inflight, acks>>

CanFault(fk) == fk \in FaultKinds /\ nf < MaxFaults

\* A fault: the append lands and its reply is lost (lostReply), nothing lands
\* (notSent), or it times out still in flight and may land later (inflight).
\* The writer sends again (again), or gives the record up: its outcome is
\* unknown.
SendFault(w, j, fk, again) ==
  LET s == ws[w]
      nm == s.wc.segs[j].name
      r == Recs[w][s.i]
      s1 == [s EXCEPT !.n = @ + 1]
  IN /\ CanSend(w) /\ j \in Eligible(s)
     /\ blob[nm].ex /\ Len(blob[nm].fr) < MaxBlocks
     /\ fk \in {"lostReply", "notSent", "inflight"} /\ CanFault(fk)
     /\ nf' = nf + 1
     /\ blob' = IF fk = "lostReply" THEN Land(blob, nm, [r |-> r, n |-> 0]) ELSE blob
     /\ inflight' = IF fk = "inflight" THEN inflight \cup {[r |-> r, name |-> nm]} ELSE inflight
     /\ sends' = IF fk = "notSent" THEN sends ELSE [sends EXCEPT ![r] = @ + 1]
     /\ SetW(w, IF again THEN s1 ELSE NextRec(w, s1))
     /\ UNCH_W /\ UNCHANGED <<acks, seen>>

\* Out of sends for this record: the runtime gives it up.
GiveUp(w) ==
  LET s == ws[w] IN
  /\ s.pc = "send" /\ s.n >= MaxSends
  /\ SetW(w, NextRec(w, s))
  /\ UNCH_W /\ UNCHANGED <<blob, inflight, acks, sends, nf, seen>>

\* A timed-out append lands after all.
LandInflight(a) ==
  /\ a \in inflight
  /\ inflight' = inflight \ {a}
  /\ IF blob[a.name].ex /\ Len(blob[a.name].fr) < MaxBlocks
       THEN /\ blob' = Land(blob, a.name, [r |-> a.r, n |-> 0])
            /\ seen' = [seen EXCEPT !.late = TRUE]
       ELSE UNCHANGED <<blob, seen>>
  /\ UNCHANGED <<cat, chain, lims, closedAt, ckpts, pendCk, ws, rs, acks, sends, vok, nf, comp,
                 voids>>

-----------------------------------------------------------------------------
(* Installing a segment: create it under a fresh name, then append its     *)
(* Install to the catalog object from the version held. A writer may do it *)
(* whenever it is sending, ahead of need or because no segment it knows    *)
(* takes the record. Past MaxOrd the run stops the writer.                 *)

StartInstall(w) ==
  LET s == ws[w] IN
  /\ s.pc = "send"
  /\ Ahead \/ Eligible(s) = {}
  /\ IF LastOrd(s.wc) + 1 > MaxOrd
       THEN /\ Eligible(s) = {}
            /\ SetW(w, [s EXCEPT !.pc = "done"])
       ELSE SetW(w, [s EXCEPT !.pc = IF Has("limitatnext") THEN "lread" ELSE "create"])
  /\ UNCH_W /\ UNCHANGED <<blob, inflight, acks, sends, nf, seen>>

\* "limitatnext": the installer reads the last segment's records, the limit
\* its Install will set for it.
LRead(w) ==
  LET s == ws[w]
      nm == SegOf(s.wc, LastOrd(s.wc)).name
  IN /\ s.pc = "lread"
     /\ SetW(w, [s EXCEPT !.c = Lead(blob[nm].fr), !.pc = "create"])
     /\ UNCH_W /\ UNCHANGED <<blob, inflight, acks, sends, nf, seen>>

Create(w) ==
  LET s == ws[w]
      cand == <<"this", LastOrd(s.wc) + 1, w>>
  IN /\ s.pc = "create"
     /\ blob' = IF blob[cand].ex THEN blob ELSE [blob EXCEPT ![cand] = [ex |-> TRUE, fr |-> << >>]]
     /\ SetW(w, [s EXCEPT !.cand = cand, !.pc = "cas"])
     /\ UNCH_W /\ UNCHANGED <<inflight, acks, sends, nf, seen>>

\* The Install lands if the catalog object is the version held. Lost: if an
\* Install of the ordinal landed, the writer uses it and leaves its own blob
\* to the sweep; else (a checkpoint changed the object) it tries again from
\* the version it read.
Cas(w) ==
  LET s == ws[w]
      o == OrdOf(s.cand)
      segs1 == IF Has("limitatnext")
                 THEN [cat.segs EXCEPT ![Len(cat.segs)].lim = s.c] ELSE cat.segs
  IN /\ s.pc = "cas"
     /\ IF cat.ver = s.wc.ver
          THEN LET c2 == [cat EXCEPT !.ver = @ + 1,
                                     !.segs = segs1 \o << [ord |-> o, name |-> s.cand,
                                                           lim |-> InstallLim, cl |-> -1] >>]
               IN /\ cat' = c2
                  /\ chain' = Append(chain, s.cand)
                  /\ lims' = [lims EXCEPT ![o] = InstallLim,
                                          ![o - 1] = IF Has("limitatnext") THEN s.c ELSE @]
                  /\ SetW(w, [s EXCEPT !.wc = c2, !.pc = "send"])
                  /\ UNCHANGED seen
          ELSE /\ UNCHANGED <<cat, chain, lims>>
               /\ seen' = [seen EXCEPT !.caslost = TRUE]
               /\ SetW(w, [s EXCEPT !.wc = cat,
                                    !.pc = IF ~Named(cat, o) /\ LastOrd(cat) = o - 1
                                             THEN (IF Has("limitatnext") THEN "lread" ELSE "cas")
                                             ELSE "send"])
     /\ UNCHANGED <<blob, closedAt, ckpts, pendCk, rs, vok, comp, inflight, acks, sends, nf,
                    voids>>

-----------------------------------------------------------------------------
(* The read-through: the receipt's copies in ordinal order. Each needs the *)
(* segments before its own ended: the writer reads each it hasn't seen     *)
(* end, and seals one that hasn't. Then the copy is in the log if its      *)
(* segment hadn't ended before it; else it is void and the writer tries    *)
(* the next copy, or sends again.                                          *)

Cur(s) == s.cps[s.ci]
Pending(s) ==
  {j \in 1..Len(s.wc.segs) : s.wc.segs[j].ord < Cur(s).ord /\ s.wc.segs[j].ord \notin s.kn}

\* Read an earlier segment: gone (retired, so ended) or ended, or seal it.
\* A short read (a fault) returns a prefix of its frames: one that shows no
\* end may hide one, so the writer seals the segment, which is harmless.
RtCheck(w, j) ==
  LET s == ws[w]
      seg == s.wc.segs[j]
      b == blob[seg.name]
  IN /\ s.pc = "rt" /\ s.ci <= Len(s.cps) /\ j \in Pending(s)
     /\ \E e \in (IF b.ex THEN 0..Len(b.fr) ELSE {0}) :
          LET short == b.ex /\ e < Len(b.fr)
              fr == SubSeq(b.fr, 1, e)
          IN /\ short => CanFault("shortRead")
             /\ nf' = IF short THEN nf + 1 ELSE nf
             /\ IF ~b.ex \/ Ended(fr, seg.lim, seg.cl)
                  THEN SetW(w, [s EXCEPT !.kn = @ \cup {seg.ord}])
                  ELSE SetW(w, [s EXCEPT !.pc = "seal", !.tgt = seg, !.c = Lead(fr)])
     /\ UNCH_W /\ UNCHANGED <<blob, inflight, acks, sends, seen>>

\* A seal frame, appended blind. It carries the count read before it, which
\* only "sealcount" reads. With "catalogseal" the writer instead records that
\* count as the segment's end in the catalog object, from the version held.
SealStep(w) ==
  LET s == ws[w]
      nm == s.tgt.name
      o == s.tgt.ord
  IN /\ s.pc = "seal"
     /\ IF Has("catalogseal")
          THEN IF cat.ver = s.wc.ver /\ Named(cat, o)
                 THEN LET c2 == [cat EXCEPT !.ver = @ + 1, !.segs[IdxOf(cat, o)].cl = s.c] IN
                      /\ cat' = c2
                      /\ closedAt' = [closedAt EXCEPT ![o] = s.c]
                      /\ SetW(w, [s EXCEPT !.wc = c2, !.pc = "rt", !.kn = @ \cup {o}])
                 ELSE /\ SetW(w, [s EXCEPT !.wc = cat, !.pc = "rt"])
                      /\ UNCHANGED <<cat, closedAt>>
          ELSE /\ SetW(w, [s EXCEPT !.pc = "rt"])
               /\ UNCHANGED <<cat, closedAt>>
     /\ blob' = IF ~Has("catalogseal") /\ blob[nm].ex /\ Len(blob[nm].fr) < MaxBlocks
                  THEN Land(blob, nm, [r |-> Seal, n |-> s.c]) ELSE blob
     /\ seen' = [seen EXCEPT !.seal = @ \/ ~Has("catalogseal")]
     /\ UNCHANGED <<chain, lims, ckpts, pendCk, rs, vok, comp, inflight, acks, sends, nf, voids>>

\* Before reading through, the writer may send a copy to a later segment.
CopyMore(w) ==
  LET s == ws[w] IN
  /\ s.pc = "rt" /\ s.ci = 1 /\ Len(s.cps) < MaxCopies /\ s.n < MaxSends
  /\ SetW(w, [s EXCEPT !.pc = "send", !.lo = LastCopyOrd(s) + 1])
  /\ UNCH_W /\ UNCHANGED <<blob, inflight, acks, sends, nf, seen>>

\* Read the copy's segment. Gone: trimmed, the outcome unknown; the record is
\* given up. In: acknowledged. Void (it ended before the copy): the next
\* copy, or send again past it. "voidack" accepts the frame regardless. A
\* short read (a fault) returns a prefix of the frames: one that reaches the
\* copy, or shows the segment ended before it, decides as a whole read does;
\* one that does neither says nothing, and the writer reads again (the Java
\* tells "ended before the copy" from "read short of it"). With "shortvoid"
\* it takes such a prefix for void.
RtRead(w) ==
  LET s == ws[w]
      cp == Cur(s)
      seg == SegOf(s.wc, cp.ord)
      b == blob[cp.name]
      r == Recs[w][s.i]
  IN /\ s.pc = "rt" /\ s.ci <= Len(s.cps) /\ Pending(s) = {}
     /\ IF ~b.ex
          THEN /\ SetW(w, NextRec(w, s))
               /\ UNCHANGED <<acks, seen, nf, voids>>
          ELSE \E e \in 0..Len(b.fr) :
                 LET short == e < Len(b.fr)
                     fr == SubSeq(b.fr, 1, e)
                     in == Has("voidack") \/ cp.idx <= Valid(fr, seg.lim, seg.cl)
                 IN /\ short => CanFault("shortRead")
                    /\ nf' = IF short THEN nf + 1 ELSE nf
                    /\ CASE in ->
                              /\ acks' = acks \cup {[r |-> r, ord |-> cp.ord, idx |-> cp.idx]}
                              /\ SetW(w, NextRec(w, s))
                              /\ seen' = [seen EXCEPT !.second = @ \/ s.ci > 1]
                              /\ UNCHANGED voids
                         [] Ended(fr, seg.lim, seg.cl) \/ Has("shortvoid") ->
                              /\ SetW(w, [s EXCEPT !.ci = @ + 1, !.kn = @ \cup {cp.ord},
                                                   !.pc = IF s.ci + 1 <= Len(s.cps) THEN "rt"
                                                          ELSE "send",
                                                   !.lo = cp.ord + 1])
                              /\ voids' = voids \cup {[r |-> r, ord |-> cp.ord, idx |-> cp.idx]}
                              /\ seen' = [seen EXCEPT !.void = TRUE]
                              /\ UNCHANGED acks
                         [] OTHER ->
                              /\ seen' = [seen EXCEPT !.shortrt = TRUE]
                              /\ UNCHANGED <<ws, acks, voids>>
     /\ UNCH_W0 /\ UNCHANGED <<blob, inflight, sends>>

-----------------------------------------------------------------------------
(* Compactors: putCheckpoint (a CAS replace) and the sweep *)

UNCH_C == UNCHANGED <<chain, lims, closedAt, inflight, ws, rs, acks, sends, vok, nf, voids>>

\* Put a checkpoint through a place in the log past the catalog object's.
PutCk ==
  /\ comp.ck < MaxCkpt /\ pendCk = None
  /\ \E i \in 1..Reach, rec \in 0..MaxBlocks :
       LET o == i - 1 IN
       /\ rec <= Len(SegRecs(i))
       /\ Named(cat, o)
       /\ After(o, rec, cat.ck.ord, cat.ck.rec)
       /\ LET c == [ord |-> o, rec |-> rec, k |-> Index(o, rec)] IN
          /\ ckpts' = ckpts \cup {c}
          /\ pendCk' = c
  /\ comp' = [comp EXCEPT !.ck = @ + 1]
  /\ UNCHANGED <<blob, cat, seen>> /\ UNCH_C

\* The replace: the checkpoint, the Installs from its segment on, and the
\* segments before it retired. Done already if the object's checkpoint is at
\* or past it.
CasCk ==
  /\ pendCk # None
  /\ pendCk' = None
  /\ IF After(pendCk.ord, pendCk.rec, cat.ck.ord, cat.ck.rec) /\ Named(cat, pendCk.ord)
       THEN LET keep == SelectSeq(cat.segs, LAMBDA g : g.ord >= pendCk.ord)
                gone == {cat.segs[j].name : j \in {j \in 1..Len(cat.segs) :
                                                     cat.segs[j].ord < pendCk.ord}}
            IN /\ cat' = [cat EXCEPT !.ver = @ + 1, !.ck = pendCk, !.segs = keep,
                                     !.ret = @ \cup gone]
               /\ seen' = [seen EXCEPT !.retired = @ \/ gone # {}]
       ELSE UNCHANGED <<cat, seen>>
  /\ UNCHANGED <<blob, ckpts, comp>> /\ UNCH_C

\* The sweep (at TTL 0) deletes a retired segment, or an unnamed blob at or
\* below the newest Install's ordinal: no Install of it can land any more. It
\* lists only this log's blobs. "sweepinflight": an unnamed blob of any
\* ordinal. "sweepearly": a named one. "sweepothers": it lists every log's.
Sweep ==
  /\ comp.sw < MaxSweep
  /\ \E nm \in Names :
       /\ blob[nm].ex
       /\ LogOf(nm) = "this" \/ Has("sweepothers")
       /\ \/ nm \in cat.ret
          \/ nm \notin NamesIn(cat) /\ (OrdOf(nm) <= LastOrd(cat) \/ Has("sweepinflight"))
          \/ Has("sweepearly") /\ nm \in NamesIn(cat)
       /\ blob' = [blob EXCEPT ![nm].ex = FALSE]
  /\ comp' = [comp EXCEPT !.sw = @ + 1]
  /\ seen' = [seen EXCEPT !.swept = TRUE]
  /\ UNCHANGED <<cat, ckpts, pendCk>> /\ UNCH_C

-----------------------------------------------------------------------------
(* Readers: read() from the catalog object's checkpoint, and readAfter's   *)
(* step past an ended segment                                              *)

RInit == [pc |-> "idle", rc |-> None, j |-> 0, rec |-> 0, k |-> 0, v |-> << >>, n |-> 0]
\* pc: idle, seg (GET rc.segs[j] from record rec), next (read the catalog object past an
\* ended segment). k: the checkpoint's place in Abstract; v: the records read.

UNCH_R == UNCHANGED <<blob, cat, chain, lims, closedAt, ckpts, pendCk, inflight, ws, acks,
                      sends, comp, voids>>

SetR(r, q2) == rs' = [rs EXCEPT ![r] = q2]
Retry(r, q) == SetR(r, [q EXCEPT !.pc = "idle", !.n = @ + 1])

\* The view ends: it must be a slice of the log from the checkpoint.
Finish(r, q, v) ==
  /\ vok' = (vok /\ IsSlice(q.k, v))
  /\ Retry(r, q)

RStart(r) ==
  LET q == rs[r] IN
  /\ q.pc = "idle" /\ q.n < MaxReads /\ Named(cat, cat.ck.ord)
  /\ SetR(r, [q EXCEPT !.pc = "seg", !.rc = cat, !.j = IdxOf(cat, cat.ck.ord),
                       !.rec = cat.ck.rec, !.k = cat.ck.k, !.v = << >>])
  /\ UNCH_R /\ UNCHANGED <<vok, nf, seen>>

RecsOf(fr, from, to) == [j \in 1..(IF to > from THEN to - from ELSE 0) |-> fr[from + j].r]

\* GET the segment: its frames, or with a short read (a fault) fewer. 404: a
\* sweep raced the read; read again. Ended: on to the next segment, reading
\* the catalog object if the one held names none. Open: the view ends there
\* ("readahead": it goes on to the next segment anyway).
RSeg(r) ==
  LET q == rs[r]
      seg == q.rc.segs[q.j]
      b == blob[seg.name]
  IN /\ q.pc = "seg"
     /\ IF ~b.ex \/ Len(b.fr) < q.rec
          THEN /\ Retry(r, q)
               /\ seen' = [seen EXCEPT !.r404 = TRUE]
               /\ UNCHANGED <<vok, nf>>
          ELSE \E e \in q.rec..Len(b.fr) :
                 LET short == e # Len(b.fr)
                     fr == SubSeq(b.fr, 1, e)
                     to == Valid(fr, seg.lim, seg.cl)
                     v2 == q.v \o RecsOf(fr, q.rec, to)
                     more == q.j < Len(q.rc.segs)
                 IN /\ short => CanFault("shortRead")
                    /\ nf' = IF short THEN nf + 1 ELSE nf
                    /\ UNCHANGED seen
                    /\ CASE Ended(fr, seg.lim, seg.cl) /\ more ->
                              SetR(r, [q EXCEPT !.v = v2, !.j = @ + 1, !.rec = 0])
                              /\ UNCHANGED vok
                         [] Ended(fr, seg.lim, seg.cl) ->
                              SetR(r, [q EXCEPT !.v = v2, !.pc = "next"])
                              /\ UNCHANGED vok
                         [] Has("readahead") /\ more ->
                              SetR(r, [q EXCEPT !.v = v2, !.j = @ + 1, !.rec = 0])
                              /\ UNCHANGED vok
                         [] OTHER -> Finish(r, q, v2)
     /\ UNCH_R

\* Past an ended last segment: the catalog object names its successor
\* (continue there), still names it last (the view ends), or no longer names
\* it (retired: read whole again).
RNext(r) ==
  LET q == rs[r]
      o == q.rc.segs[q.j].ord
  IN /\ q.pc = "next"
     /\ seen' = [seen EXCEPT !.rnext = TRUE]
     /\ UNCHANGED nf
     /\ CASE Named(cat, o + 1) ->
               SetR(r, [q EXCEPT !.rc = cat, !.j = IdxOf(cat, o + 1), !.rec = 0, !.pc = "seg"])
               /\ UNCHANGED vok
          [] Named(cat, o) -> Finish(r, q, q.v)
          [] OTHER -> Retry(r, q) /\ UNCHANGED vok
     /\ UNCH_R

-----------------------------------------------------------------------------

Init ==
  /\ blob = [nm \in Names |-> [ex |-> nm \in {<<"this", 0, 0>>, Other}, fr |-> << >>]]
  /\ cat = [ver |-> 1, ck |-> [ord |-> 0, rec |-> 0, k |-> 0],
            segs |-> << [ord |-> 0, name |-> <<"this", 0, 0>>, lim |-> InstallLim, cl |-> -1] >>,
            ret |-> {}]
  /\ chain = << <<"this", 0, 0>> >>
  /\ lims = [o \in 0..MaxOrd |-> IF o = 0 THEN InstallLim ELSE -1]
  /\ closedAt = [o \in 0..MaxOrd |-> -1]
  /\ ckpts = {[ord |-> 0, rec |-> 0, k |-> 0]}
  /\ pendCk = None
  /\ inflight = {}
  /\ ws = [w \in Writers |-> WInit]
  /\ rs = [r \in Readers |-> RInit]
  /\ acks = {}
  /\ sends = [r \in AllRecs |-> 0]
  /\ vok = TRUE
  /\ nf = 0
  /\ comp = [ck |-> 0, sw |-> 0]
  /\ seen = [full |-> FALSE, voidreply |-> FALSE, caslost |-> FALSE, w404 |-> FALSE,
             r404 |-> FALSE, rnext |-> FALSE, late |-> FALSE, seal |-> FALSE, void |-> FALSE,
             second |-> FALSE, retired |-> FALSE, swept |-> FALSE, shortrt |-> FALSE]
  /\ voids = {}

WriterNext(w) ==
  \/ WStart(w) \/ GiveUp(w) \/ StartInstall(w) \/ LRead(w) \/ Create(w) \/ Cas(w)
  \/ \E j \in 1..Len(ws[w].wc.segs) :
       \/ SendGone(w, j) \/ SendFull(w, j) \/ SendOk(w, j)
       \/ \E fk \in {"lostReply", "notSent", "inflight"}, again \in BOOLEAN :
            SendFault(w, j, fk, again)
       \/ RtCheck(w, j)
  \/ SealStep(w) \/ CopyMore(w) \/ RtRead(w)

Done ==
  /\ \A w \in Writers : ws[w].pc = "done"
  /\ \A r \in Readers : rs[r].pc = "idle" /\ rs[r].n >= MaxReads
  /\ UNCHANGED vars

Next ==
  \/ \E w \in Writers : ws[w].pc # "start" /\ ws[w].pc # "done" /\ WriterNext(w)
  \/ \E w \in Writers : WStart(w)
  \/ \E a \in inflight : LandInflight(a)
  \/ PutCk \/ CasCk \/ Sweep
  \/ \E r \in Readers : RStart(r) \/ RSeg(r) \/ RNext(r)
  \/ Done

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

\* P1: the log only grows. Every record in it keeps its place (SG3).
AbstractGrows ==
  [][Len(Abstract) <= Len(Abstract') /\ SubSeq(Abstract', 1, Len(Abstract)) = Abstract]_vars

\* SG1, SG2: every read was a slice of the log from its checkpoint.
ReadsAreSlices == vok

\* P3: every copy a read-through accepted is in the log, where it was read.
AckedInLog ==
  \A a \in acks :
    /\ InLog(a.ord, a.idx)
    /\ SegFr(a.ord + 1)[a.idx].r = a.r

\* D4: a copy its writer judged void (by the reply's block count, or by a
\* read that showed its segment ended before it) is never in the log: the
\* runtime takes a void append for a lost race with nothing to resolve.
VoidNotInLog ==
  \A v \in voids : ~(InLog(v.ord, v.idx) /\ SegFr(v.ord + 1)[v.idx].r = v.r)

\* Every segment the catalog object names exists (SG4, SG6).
NamedExist == \A j \in 1..Len(cat.segs) : blob[cat.segs[j].name].ex

\* Another log's segment at the prefix, which its catalog object names, exists.
OthersKept == blob[Other].ex

\* P5: the checkpoint lies in a named segment, so a reader can start.
Readable == Named(cat, cat.ck.ord) /\ cat.ck \in ckpts

\* A checkpoint keeps its place in the log.
CheckpointsStable == \A c \in ckpts : InLog(c.ord, c.rec) /\ Index(c.ord, c.rec) = c.k

\* SG8: every installed segment can end: one that hasn't has room for a seal.
CanEnd == \A i \in 1..Len(chain) : Open(i) => Len(SegFr(i)) < MaxBlocks

\* No copy of a record is in the log that its writer doesn't know may have
\* landed (SharedLogCommit's "dup": the runtime counts every such write).
CopiesKnown ==
  \A r \in AllRecs : Cardinality({j \in 1..Len(Abstract) : Abstract[j] = r}) <= sends[r]

Safety ==
  /\ ReadsAreSlices /\ AckedInLog /\ NamedExist /\ Readable /\ CheckpointsStable /\ CanEnd
  /\ CopiesKnown /\ OthersKept /\ VoidNotInLog
=============================================================================
