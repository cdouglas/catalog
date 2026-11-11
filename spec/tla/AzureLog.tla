------------------------------ MODULE AzureLog ------------------------------
(***************************************************************************)
(* docs/AZURE_LOG.md's log on Azure Append Blobs, as a SharedLog provider: *)
(* the objects and requests below the SPI that SharedLogCommit.tla takes  *)
(* as one log. Written before the Java, as SharedLogCommit.tla was.        *)
(*                                                                         *)
(* Records go into segments (Append Blobs), appended blind, each append    *)
(* carrying the writer limit W as its size condition. A writer that finds  *)
(* the last segment full closes it with a pad that fills it to its limit   *)
(* L = W + 1 (or finds it closed: by a pad, or by the block limit),        *)
(* creates a candidate successor and CASes the head to name it. The head   *)
(* (one object, changed only by CAS) names the live segments, each closed  *)
(* one with its final length, and the newest checkpoint. Compactors put    *)
(* checkpoints and CAS the head to name them, trim (a CAS that drops       *)
(* segments before the checkpoint's) and sweep (delete segments the head   *)
(* doesn't name). Readers read from a head they hold, however stale.       *)
(*                                                                         *)
(* Sizes are in units: a record's frame is 1, a pad whatever reaches L.    *)
(* Each action is one request, so the others interleave between any two.   *)
(* A crash is a writer that takes no more steps, which every behaviour     *)
(* already includes.                                                       *)
(*                                                                         *)
(* The log the provider presents (Abstract, a ghost) is the records of the *)
(* segments every head has named, in ordinal order, each up to its closed *)
(* length (the last up to its size), pads left out. The properties say it  *)
(* is one log as SharedLogCommit's `log` is: it only grows (P1), every     *)
(* acknowledged append is in it at its receipt (P3), every read is a       *)
(* slice of it from a checkpoint, checkpoints keep their place, and a      *)
(* record has no copy in it that its writer doesn't know may have landed   *)
(* (the shared-log model's `dup`). Bug switches put back the mistakes of   *)
(* AZURE_LOG.md "Mistakes the model must catch".                           *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
  Writers,     \* writer ids (1, 2, ...); 0 is the log's creator
  Recs,        \* Writers -> Seq of record ids: what each writer appends, in order
  WLimit,      \* W: every append carries maxsize W, so a writer's frame ends at or before W
  MaxBlocks,   \* the service's block limit per blob (50,000 on Azure)
  MaxOrd,      \* the highest segment ordinal of a run
  MaxSends,    \* sends of one record (the runtime sends again after an ambiguous write)
  MaxFaults,   \* how many faults a run may inject
  FaultKinds,  \* lostReply, notSent, inflight (writes); shortRead (reads)
  MaxCkpt,     \* checkpoints compactors put
  MaxTrim,     \* trims
  MaxSweep,    \* blobs the sweep deletes
  Readers,     \* reader ids
  MaxReads,    \* reads per reader
  Bugs,        \* the mistakes put back, by name
  Pad,         \* the pad's record (a model value)
  None         \* no value (a model value)

ASSUME /\ WLimit >= 1 /\ MaxBlocks >= 1 /\ MaxOrd >= 0 /\ MaxSends >= 1
       /\ FaultKinds \subseteq {"lostReply", "notSent", "inflight", "shortRead"}
       /\ Bugs \subseteq {"casbeforeclose", "nolimit", "createonmissing", "noclosedlength",
                          "sweepnamed", "trimearly", "sdkretry", "offsetfromlength"}

Has(b) == b \in Bugs

\* L, the segment's limit: a pad fills a segment to it. Every append but a
\* pad carries the writer limit W. With "nolimit", writer 1's carry none, as
\* a code path that forgot it would send them.
Limit == WLimit + 1
Lim(w) == IF Has("nolimit") /\ w = 1 THEN -1 ELSE WLimit

\* Segment names: <<ordinal, nonce>>, the nonce its creator's id.
Names == {<<o, n>> : o \in 0..MaxOrd, n \in Writers \cup {0}}
OrdOf(nm) == nm[1]
AllRecs == UNION {{Recs[w][i] : i \in 1..Len(Recs[w])} : w \in Writers}

VARIABLES
  blob,      \* Names -> [ex, fr]: whether the blob exists, and its frames [r, off, sz]
             \*   (a deleted blob keeps its frames, as a ghost)
  head,      \* [ver, segs: Seq of [ord, name, cl], ck: [ord, off, k]]; cl = -1: open
  chain,     \* ghost: the names heads have named, by ordinal (chain[i] has ordinal i - 1)
  closedAt,  \* ghost: ordinal -> the closed length the head recorded, -1 before
  ckpts,     \* the checkpoints put: [ord, off, k], k its place in Abstract
  pendCk,    \* a checkpoint put whose CAS hasn't run, or None
  inflight,  \* appends timed out and still in flight: [r, name, lim]
  ws,        \* Writers -> the writer's state (WInit)
  rs,        \* Readers -> the reader's state (RInit)
  acks,      \* ghost: the receipts appends returned: [r, ord, off]
  sends,     \* ghost: per record, the sends its writer knows may have landed
  vok,       \* ghost: every read so far was a slice of Abstract when it ended
  nf,        \* faults injected so far
  comp,      \* compactors' budgets used: [ck, tr, sw]
  seen       \* ghost, for the witnesses: [pad, blocks, caslost, w404, r404, rnext, late]

vars == <<blob, head, chain, closedAt, ckpts, pendCk, inflight, ws, rs, acks, sends, vok, nf,
          comp, seen>>

-----------------------------------------------------------------------------
(* Blobs *)

Size(fr) == IF fr = << >> THEN 0 ELSE fr[Len(fr)].off + fr[Len(fr)].sz
Last(h) == h.segs[Len(h.segs)]
Named(h, o) == \E j \in 1..Len(h.segs) : h.segs[j].ord = o
IdxOf(h, o) == CHOOSE j \in 1..Len(h.segs) : h.segs[j].ord = o
NamesIn(h) == {h.segs[j].name : j \in 1..Len(h.segs)}

\* Append Block of a frame of size sz: it lands only if the blob exists, has
\* room for a block, stays within maxsize lim (-1: none) and is at appendpos
\* ap (-1: none).
Fits(nm, sz, lim, ap) ==
  /\ blob[nm].ex
  /\ Len(blob[nm].fr) < MaxBlocks
  /\ lim = -1 \/ Size(blob[nm].fr) + sz <= lim
  /\ ap = -1 \/ Size(blob[nm].fr) = ap
Land(b, nm, r, sz) == [b EXCEPT ![nm].fr = Append(@, [r |-> r, off |-> Size(@), sz |-> sz])]

\* A segment is closed when no append can land in it: it reached L (a pad),
\* or the block limit.
IsClosed(fr) == Size(fr) >= Limit \/ Len(fr) >= MaxBlocks

-----------------------------------------------------------------------------
(* The log the provider presents *)

\* The length of chain[i] in the log: its closed length once a head recorded
\* one, else its size.
Bound(i) == IF closedAt[i - 1] >= 0 THEN closedAt[i - 1] ELSE Size(blob[chain[i]].fr)

\* The records of the frames of fr that lie in [from, to), pads left out.
RecsIn(fr, from, to) ==
  LET sel == SelectSeq(fr, LAMBDA f : f.off >= from /\ f.off + f.sz <= to /\ f.r # Pad)
  IN [j \in 1..Len(sel) |-> sel[j].r]

RECURSIVE Cat(_)
Cat(i) == IF i = 0 THEN << >> ELSE Cat(i - 1) \o RecsIn(blob[chain[i]].fr, 0, Bound(i))
Abstract == Cat(Len(chain))

\* A position's place in Abstract: the records before it.
Index(o, off) == Len(Cat(o)) + Len(RecsIn(blob[chain[o + 1]].fr, 0, off))

IsSlice(k, v) == k + Len(v) <= Len(Abstract) /\ v = SubSeq(Abstract, k + 1, k + Len(v))

-----------------------------------------------------------------------------
(* Writers: the provider's append, with rotation inside it *)

WInit == [pc |-> "start", i |-> 1, wh |-> None, n |-> 0, full |-> None, x |-> 0, cl |-> 0,
          cand |-> None, pre |-> -1, late |-> FALSE]
\* pc: start (read the head), send, rhead, props, pad, create, cas, pad2, done.
\* wh: the head the writer holds; n: sends of the current record; full: the
\* segment it found full; x: the length it pads from; cl: the closed length
\* it learned; cand: its candidate; pre: a length read before the append
\* ("offsetfromlength"); late: a pad still to send ("casbeforeclose").

UNCH_W == UNCHANGED <<head, chain, closedAt, ckpts, pendCk, rs, vok, comp>>

SetW(w, s2) == ws' = [ws EXCEPT ![w] = s2]

\* The record after this one, or done.
NextRec(w, s) ==
  [s EXCEPT !.i = @ + 1, !.n = 0, !.pre = -1,
            !.pc = IF s.i + 1 <= Len(Recs[w]) THEN "send" ELSE "done"]

WStart(w) ==
  LET s == ws[w] IN
  /\ s.pc = "start"
  /\ SetW(w, [s EXCEPT !.wh = head, !.pc = IF s.i <= Len(Recs[w]) THEN "send" ELSE "done"])
  /\ UNCH_W /\ UNCHANGED <<blob, inflight, acks, sends, nf, seen>>

Seg(s) == Last(s.wh)

\* "offsetfromlength": the writer reads the length before it appends.
Probe(w) ==
  LET s == ws[w] IN
  /\ Has("offsetfromlength") /\ s.pc = "send" /\ s.pre = -1 /\ blob[Seg(s).name].ex
  /\ SetW(w, [s EXCEPT !.pre = Size(blob[Seg(s).name].fr)])
  /\ UNCH_W /\ UNCHANGED <<blob, inflight, acks, sends, nf, seen>>

CanSend(w) == ws[w].pc = "send" /\ ws[w].n < MaxSends

\* 404: the head was stale (the segment was trimmed and swept); nothing
\* landed. The writer reads the head again; with "createonmissing", it
\* creates the segment, and sends to it next.
SendGone(w) ==
  LET s == ws[w] IN
  /\ CanSend(w) /\ ~blob[Seg(s).name].ex
  /\ IF Has("createonmissing")
       THEN /\ blob' = [blob EXCEPT ![Seg(s).name] = [ex |-> TRUE, fr |-> << >>]]
            /\ UNCHANGED ws
       ELSE /\ SetW(w, [s EXCEPT !.pc = "start", !.pre = -1])
            /\ UNCHANGED blob
  /\ seen' = [seen EXCEPT !.w404 = TRUE]
  /\ UNCH_W /\ UNCHANGED <<inflight, acks, sends, nf>>

\* Full (412 MaxBlobSizeConditionNotMet, 409 BlockCountExceedsLimit): nothing
\* landed. The writer reads the head, and moves the log on if it must.
SendFull(w) ==
  LET s == ws[w] IN
  /\ CanSend(w) /\ blob[Seg(s).name].ex /\ ~Fits(Seg(s).name, 1, Lim(w), -1)
  /\ SetW(w, [s EXCEPT !.pc = "rhead", !.full = Seg(s).name, !.pre = -1])
  /\ UNCH_W /\ UNCHANGED <<blob, inflight, acks, sends, nf, seen>>

\* The append lands and its reply names where (x-ms-blob-append-offset); with
\* "offsetfromlength", the receipt takes the length read before instead.
SendOk(w) ==
  LET s == ws[w]
      nm == Seg(s).name
      r == Recs[w][s.i]
  IN /\ CanSend(w) /\ Fits(nm, 1, Lim(w), -1)
     /\ Has("offsetfromlength") => s.pre >= 0
     /\ blob' = Land(blob, nm, r, 1)
     /\ acks' = acks \cup {[r |-> r, ord |-> Seg(s).ord,
                             off |-> IF Has("offsetfromlength") THEN s.pre
                                     ELSE Size(blob[nm].fr)]}
     /\ sends' = [sends EXCEPT ![r] = @ + 1]
     /\ SetW(w, NextRec(w, s))
     /\ UNCH_W /\ UNCHANGED <<inflight, nf, seen>>

CanFault(fk) == fk \in FaultKinds /\ nf < MaxFaults

\* A fault: the append lands and its reply is lost (lostReply), nothing
\* lands (notSent, LogNotSentException), or it times out still in flight and
\* may land later (inflight). The first and last are ambiguous: the runtime
\* counts them as writes that may have landed, and may send again (again)
\* or give the record up.
SendFault(w, fk, again) ==
  LET s == ws[w]
      nm == Seg(s).name
      r == Recs[w][s.i]
      s1 == [s EXCEPT !.n = @ + 1, !.pre = -1]
  IN /\ CanSend(w) /\ Fits(nm, 1, Lim(w), -1)
     /\ fk \in {"lostReply", "notSent", "inflight"} /\ CanFault(fk)
     /\ nf' = nf + 1
     /\ blob' = IF fk = "lostReply" THEN Land(blob, nm, r, 1) ELSE blob
     /\ inflight' = IF fk = "inflight" THEN inflight \cup {[r |-> r, name |-> nm, lim |-> Lim(w)]}
                    ELSE inflight
     /\ sends' = IF fk = "notSent" THEN sends ELSE [sends EXCEPT ![r] = @ + 1]
     /\ SetW(w, IF again THEN s1 ELSE NextRec(w, s1))
     /\ UNCH_W /\ UNCHANGED <<acks, seen>>

\* "sdkretry": the SDK sends a timed-out append again by itself. The first
\* copy landed; the runtime hears only of the second: a receipt, or full.
SendSdk(w) ==
  LET s == ws[w]
      nm == Seg(s).name
      r == Recs[w][s.i]
      b1 == Land(blob, nm, r, 1)
      fits2 == /\ Len(b1[nm].fr) < MaxBlocks
               /\ Lim(w) = -1 \/ Size(b1[nm].fr) + 1 <= Lim(w)
  IN /\ Has("sdkretry") /\ nf < MaxFaults
     /\ CanSend(w) /\ Fits(nm, 1, Lim(w), -1)
     /\ nf' = nf + 1
     /\ IF fits2
          THEN /\ blob' = Land(b1, nm, r, 1)
               /\ acks' = acks \cup {[r |-> r, ord |-> Seg(s).ord, off |-> Size(b1[nm].fr)]}
               /\ sends' = [sends EXCEPT ![r] = @ + 1]
               /\ SetW(w, NextRec(w, s))
          ELSE /\ blob' = b1
               /\ UNCHANGED <<acks, sends>>
               /\ SetW(w, [s EXCEPT !.pc = "rhead", !.full = nm, !.pre = -1])
     /\ UNCH_W /\ UNCHANGED <<inflight, seen>>

\* Out of sends for this record: the runtime gives it up.
GiveUp(w) ==
  LET s == ws[w] IN
  /\ s.pc = "send" /\ s.n >= MaxSends
  /\ SetW(w, NextRec(w, s))
  /\ UNCH_W /\ UNCHANGED <<blob, inflight, acks, sends, nf, seen>>

\* A timed-out append lands after all, under the conditions it was sent with.
LandInflight(a) ==
  /\ a \in inflight
  /\ inflight' = inflight \ {a}
  /\ IF Fits(a.name, 1, a.lim, -1)
       THEN /\ blob' = Land(blob, a.name, a.r, 1)
            /\ seen' = [seen EXCEPT !.late = TRUE]
       ELSE UNCHANGED <<blob, seen>>
  /\ UNCHANGED <<head, chain, closedAt, ckpts, pendCk, ws, rs, acks, sends, vok, nf, comp>>

-----------------------------------------------------------------------------
(* Rotation (AZURE_LOG.md "Rotation") *)

\* Read the head. If it names a segment after the full one, send there.
RHead(w) ==
  LET s == ws[w] IN
  /\ s.pc = "rhead"
  /\ SetW(w, [s EXCEPT !.wh = head, !.pc = IF Last(head).name = s.full THEN "props" ELSE "send"])
  /\ UNCH_W /\ UNCHANGED <<blob, inflight, acks, sends, nf, seen>>

\* 1. Close it: Get Blob Properties. Closed already (L, or the block limit):
\* its final length. Else pad from the length read. With "casbeforeclose",
\* the writer takes the length read as final and pads after its CAS.
Props(w) ==
  LET s == ws[w]
      b == blob[s.full]
  IN /\ s.pc = "props"
     /\ CASE ~b.ex -> SetW(w, [s EXCEPT !.pc = "start"])
          [] IsClosed(b.fr) -> SetW(w, [s EXCEPT !.cl = Size(b.fr), !.pc = "create"])
          [] Has("casbeforeclose") ->
               SetW(w, [s EXCEPT !.cl = Size(b.fr), !.x = Size(b.fr), !.late = TRUE,
                                 !.pc = "create"])
          [] OTHER -> SetW(w, [s EXCEPT !.x = Size(b.fr), !.pc = "pad"])
     /\ seen' = [seen EXCEPT !.blocks = @ \/ (b.ex /\ Len(b.fr) >= MaxBlocks /\ Size(b.fr) < Limit)]
     /\ UNCH_W /\ UNCHANGED <<blob, inflight, acks, sends, nf>>

\* The pad: L - x at appendpos x, maxsize L. Lost (the blob grew, or met the
\* block limit): back to Get Blob Properties.
PadStep(w) ==
  LET s == ws[w] IN
  /\ s.pc = "pad"
  /\ IF Fits(s.full, Limit - s.x, Limit, s.x)
       THEN /\ blob' = Land(blob, s.full, Pad, Limit - s.x)
            /\ SetW(w, [s EXCEPT !.cl = Limit, !.pc = "create"])
            /\ seen' = [seen EXCEPT !.pad = TRUE]
       ELSE /\ SetW(w, [s EXCEPT !.pc = "props"])
            /\ UNCHANGED <<blob, seen>>
  /\ UNCH_W /\ UNCHANGED <<inflight, acks, sends, nf>>

\* 2. Create a candidate (Put Blob, If-None-Match: *). Past MaxOrd the run
\* stops this writer.
Create(w) ==
  LET s == ws[w]
      o == OrdOf(s.full) + 1
      cand == <<o, w>>
  IN /\ s.pc = "create"
     /\ IF o > MaxOrd
          THEN /\ SetW(w, [s EXCEPT !.pc = "done"])
               /\ UNCHANGED blob
          ELSE /\ blob' = IF blob[cand].ex THEN blob
                          ELSE [blob EXCEPT ![cand] = [ex |-> TRUE, fr |-> << >>]]
               /\ SetW(w, [s EXCEPT !.cand = cand, !.pc = "cas"])
     /\ UNCH_W /\ UNCHANGED <<inflight, acks, sends, nf, seen>>

\* 3. CAS the head from the version held: record the closed length, add the
\* candidate. 4. Lost: if the head names a successor, drop the candidate and
\* send there; if it still names the full segment last (a checkpoint or a
\* trim changed it), CAS again from it.
Cas(w) ==
  LET s == ws[w]
      o == OrdOf(s.full) + 1
  IN /\ s.pc = "cas"
     /\ IF head.ver = s.wh.ver
          THEN LET h2 == [head EXCEPT
                            !.ver = @ + 1,
                            !.segs = [@ EXCEPT ![Len(@)].cl = s.cl]
                                       \o << [ord |-> o, name |-> s.cand, cl |-> -1] >>]
               IN /\ head' = h2
                  /\ chain' = Append(chain, s.cand)
                  /\ closedAt' = [closedAt EXCEPT ![o - 1] = s.cl]
                  /\ SetW(w, [s EXCEPT !.wh = h2, !.pc = IF s.late THEN "pad2" ELSE "send"])
                  /\ UNCHANGED <<blob, seen>>
          ELSE /\ UNCHANGED <<head, chain, closedAt>>
               /\ seen' = [seen EXCEPT !.caslost = TRUE]
               /\ CASE Named(head, o) ->
                         /\ blob' = [blob EXCEPT ![s.cand].ex = FALSE]
                         /\ SetW(w, [s EXCEPT !.wh = head, !.pc = "send", !.late = FALSE])
                    [] Last(head).name = s.full ->
                         /\ SetW(w, [s EXCEPT !.wh = head])
                         /\ UNCHANGED blob
                    [] OTHER ->
                         /\ SetW(w, [s EXCEPT !.wh = head, !.pc = "send", !.late = FALSE])
                         /\ UNCHANGED blob
     /\ UNCHANGED <<ckpts, pendCk, rs, vok, comp, inflight, acks, sends, nf>>

\* "casbeforeclose": the pad, after the CAS.
Pad2(w) ==
  LET s == ws[w] IN
  /\ s.pc = "pad2"
  /\ blob' = IF Fits(s.full, Limit - s.x, Limit, s.x)
               THEN Land(blob, s.full, Pad, Limit - s.x) ELSE blob
  /\ SetW(w, [s EXCEPT !.pc = "send", !.late = FALSE])
  /\ UNCH_W /\ UNCHANGED <<inflight, acks, sends, nf, seen>>

-----------------------------------------------------------------------------
(* Compactors: putCheckpoint, trim, the sweep *)

UNCH_C == UNCHANGED <<chain, closedAt, inflight, ws, rs, acks, sends, vok, nf, seen>>

\* The places in a live segment a checkpoint may cover through: its start,
\* and the end of each record within its length.
Ends(seg) ==
  LET fr == blob[seg.name].fr
      to == IF seg.cl >= 0 THEN seg.cl ELSE Size(fr)
  IN {0} \cup {fr[j].off + fr[j].sz :
                 j \in {j \in 1..Len(fr) : fr[j].off + fr[j].sz <= to /\ fr[j].r # Pad}}

After(o1, f1, o2, f2) == o1 > o2 \/ (o1 = o2 /\ f1 > f2)

\* Put a checkpoint (created once, its name its position) through a place in
\* the log past the head's.
PutCk ==
  /\ comp.ck < MaxCkpt /\ pendCk = None
  /\ \E j \in 1..Len(head.segs) :
       LET seg == head.segs[j] IN
       /\ blob[seg.name].ex
       /\ \E off \in Ends(seg) :
            /\ After(seg.ord, off, head.ck.ord, head.ck.off)
            /\ LET c == [ord |-> seg.ord, off |-> off, k |-> Index(seg.ord, off)] IN
               /\ ckpts' = ckpts \cup {c}
               /\ pendCk' = c
  /\ comp' = [comp EXCEPT !.ck = @ + 1]
  /\ UNCHANGED <<blob, head>> /\ UNCH_C

\* CAS the head to name it, if it is newer than the head's and in a live
\* segment; else it has nothing to do.
CasCk ==
  /\ pendCk # None
  /\ pendCk' = None
  /\ head' = IF After(pendCk.ord, pendCk.off, head.ck.ord, head.ck.off) /\ Named(head, pendCk.ord)
               THEN [head EXCEPT !.ver = @ + 1, !.ck = pendCk] ELSE head
  /\ UNCHANGED <<blob, ckpts, comp>> /\ UNCH_C

\* trim: CAS the head to drop the segments before the checkpoint's (P5).
\* With "trimearly", the checkpoint's own too.
Trim ==
  LET keep == IF Has("trimearly") THEN head.ck.ord + 1 ELSE head.ck.ord
      n == Cardinality({j \in 1..Len(head.segs) : head.segs[j].ord >= keep})
  IN /\ comp.tr < MaxTrim
     /\ head.segs[1].ord < keep /\ n > 0
     /\ head' = [head EXCEPT !.ver = @ + 1, !.segs = SubSeq(@, Len(@) - n + 1, Len(@))]
     /\ comp' = [comp EXCEPT !.tr = @ + 1]
     /\ UNCHANGED <<blob, ckpts, pendCk>> /\ UNCH_C

\* The sweep deletes a segment the head doesn't name, at or below its last
\* ordinal: trimmed, or a losing candidate. A candidate past the last may
\* still win its CAS. With "sweepnamed", any segment.
Sweep ==
  /\ comp.sw < MaxSweep
  /\ \E nm \in Names :
       /\ blob[nm].ex
       /\ Has("sweepnamed") \/ (nm \notin NamesIn(head) /\ OrdOf(nm) <= Last(head).ord)
       /\ blob' = [blob EXCEPT ![nm].ex = FALSE]
  /\ comp' = [comp EXCEPT !.sw = @ + 1]
  /\ UNCHANGED <<head, ckpts, pendCk>> /\ UNCH_C

-----------------------------------------------------------------------------
(* Readers: read() from the head's checkpoint, and readAfter's step past a *)
(* closed last segment *)

RInit == [pc |-> "idle", rh |-> None, i |-> 0, off |-> 0, k |-> 0, v |-> << >>, n |-> 0]
\* pc: idle, seg (GET rh.segs[i] from off), next (read the head past a
\* closed segment). k: the checkpoint's place in Abstract; v: the records read.

UNCH_R == UNCHANGED <<blob, head, chain, closedAt, ckpts, pendCk, inflight, ws, acks, sends,
                      comp>>

SetR(r, q2) == rs' = [rs EXCEPT ![r] = q2]
Retry(r, q) == SetR(r, [q EXCEPT !.pc = "idle", !.n = @ + 1])

\* The view ends: it must be a slice of the log from the checkpoint.
Finish(r, q, v) ==
  /\ vok' = (vok /\ IsSlice(q.k, v))
  /\ Retry(r, q)

RStart(r) ==
  LET q == rs[r] IN
  /\ q.pc = "idle" /\ q.n < MaxReads /\ Named(head, head.ck.ord)
  /\ SetR(r, [q EXCEPT !.pc = "seg", !.rh = head, !.i = IdxOf(head, head.ck.ord),
                       !.off = head.ck.off, !.k = head.ck.k, !.v = << >>])
  /\ UNCH_R /\ UNCHANGED <<vok, nf, seen>>

\* The frame boundaries a GET of nm from `from` may end at: its length, or
\* with a short read (a fault), any boundary before it.
GetEnds(nm, from) ==
  LET fr == blob[nm].fr IN
  {from} \cup {fr[j].off + fr[j].sz : j \in {j \in 1..Len(fr) : fr[j].off >= from}}

\* GET the segment from off. 404: a trim raced the read; read again. A
\* segment the held head records closed must read exactly to its closed
\* length ("noclosedlength": whatever the GET returned). The last segment
\* ends the view, unless the GET shows it closed: then read the head.
RSeg(r) ==
  LET q == rs[r]
      seg == q.rh.segs[q.i]
      b == blob[seg.name]
  IN /\ q.pc = "seg"
     /\ IF ~b.ex \/ q.off > Size(b.fr)
          THEN /\ Retry(r, q)
               /\ seen' = [seen EXCEPT !.r404 = TRUE]
               /\ UNCHANGED <<vok, nf>>
          ELSE \E e \in GetEnds(seg.name, q.off) :
                 LET short == e # Size(b.fr)
                     v2 == q.v \o RecsIn(b.fr, q.off, e)
                     shows == ~short /\ IsClosed(b.fr)
                 IN /\ short => CanFault("shortRead")
                    /\ nf' = IF short THEN nf + 1 ELSE nf
                    /\ UNCHANGED seen
                    /\ CASE seg.cl >= 0 /\ e # seg.cl /\ ~Has("noclosedlength") ->
                              Retry(r, q) /\ UNCHANGED vok
                         [] seg.cl >= 0 ->
                              SetR(r, [q EXCEPT !.v = v2, !.i = @ + 1, !.off = 0])
                              /\ UNCHANGED vok
                         [] shows ->
                              SetR(r, [q EXCEPT !.v = v2, !.pc = "next", !.off = e])
                              /\ UNCHANGED vok
                         [] OTHER -> Finish(r, q, v2)
     /\ UNCH_R

\* Past a closed last segment: the head names its successor (continue
\* there), or still names it last (the view ends), or no longer names it
\* (trimmed: read whole again).
RNext(r) ==
  LET q == rs[r]
      o == q.rh.segs[q.i].ord
  IN /\ q.pc = "next"
     /\ seen' = [seen EXCEPT !.rnext = TRUE]
     /\ UNCHANGED nf
     /\ CASE Named(head, o + 1) ->
               SetR(r, [q EXCEPT !.rh = head, !.i = IdxOf(head, o + 1), !.off = 0, !.pc = "seg"])
               /\ UNCHANGED vok
          [] Named(head, o) -> Finish(r, q, q.v)
          [] OTHER -> Retry(r, q) /\ UNCHANGED vok
     /\ UNCH_R

-----------------------------------------------------------------------------

Init ==
  /\ blob = [nm \in Names |-> [ex |-> nm = <<0, 0>>, fr |-> << >>]]
  /\ head = [ver |-> 1, segs |-> << [ord |-> 0, name |-> <<0, 0>>, cl |-> -1] >>,
             ck |-> [ord |-> 0, off |-> 0, k |-> 0]]
  /\ chain = << <<0, 0>> >>
  /\ closedAt = [o \in 0..MaxOrd |-> -1]
  /\ ckpts = {[ord |-> 0, off |-> 0, k |-> 0]}
  /\ pendCk = None
  /\ inflight = {}
  /\ ws = [w \in Writers |-> WInit]
  /\ rs = [r \in Readers |-> RInit]
  /\ acks = {}
  /\ sends = [r \in AllRecs |-> 0]
  /\ vok = TRUE
  /\ nf = 0
  /\ comp = [ck |-> 0, tr |-> 0, sw |-> 0]
  /\ seen = [pad |-> FALSE, blocks |-> FALSE, caslost |-> FALSE, w404 |-> FALSE,
             r404 |-> FALSE, rnext |-> FALSE, late |-> FALSE]

WriterNext(w) ==
  \/ WStart(w) \/ Probe(w) \/ SendGone(w) \/ SendFull(w) \/ SendOk(w) \/ SendSdk(w)
  \/ \E fk \in {"lostReply", "notSent", "inflight"}, again \in BOOLEAN : SendFault(w, fk, again)
  \/ GiveUp(w)
  \/ RHead(w) \/ Props(w) \/ PadStep(w) \/ Create(w) \/ Cas(w) \/ Pad2(w)

Done ==
  /\ \A w \in Writers : ws[w].pc = "done"
  /\ \A r \in Readers : rs[r].pc = "idle" /\ rs[r].n >= MaxReads
  /\ UNCHANGED vars

Next ==
  \/ \E w \in Writers : WriterNext(w)
  \/ \E a \in inflight : LandInflight(a)
  \/ PutCk \/ CasCk \/ Trim \/ Sweep
  \/ \E r \in Readers : RStart(r) \/ RSeg(r) \/ RNext(r)
  \/ Done

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

\* P1: the log only grows. Every record in it keeps its place.
AbstractGrows ==
  [][Len(Abstract) <= Len(Abstract') /\ SubSeq(Abstract', 1, Len(Abstract)) = Abstract]_vars

\* AZ1: every segment the head names but the last is closed, at the length
\* the head records.
ClosedSegments ==
  \A j \in 1..Len(head.segs) - 1 :
    LET seg == head.segs[j] IN
    /\ seg.cl >= 0
    /\ blob[seg.name].ex => Size(blob[seg.name].fr) = seg.cl

\* AZ1, on the ghost: nothing lies past a closed length, so whatever landed
\* in a segment of the log is in the log.
NothingPastClose ==
  \A i \in 1..Len(chain) : closedAt[i - 1] >= 0 => Size(blob[chain[i]].fr) = closedAt[i - 1]

\* AZ2: every read was a slice of the log from its checkpoint.
ReadsAreSlices == vok

\* AZ3: every acknowledged append is in the log, at its receipt.
AckedAtReceipt ==
  \A a \in acks :
    LET i == a.ord + 1 IN
    /\ i <= Len(chain)
    /\ \E j \in 1..Len(blob[chain[i]].fr) :
         LET f == blob[chain[i]].fr[j] IN
         f.off = a.off /\ f.r = a.r /\ f.off + f.sz <= Bound(i)

\* AZ5: every segment the head names exists.
NamedExist == \A j \in 1..Len(head.segs) : blob[head.segs[j].name].ex

\* P5: the head's checkpoint lies in a live segment, so a reader can start.
Readable == Named(head, head.ck.ord) /\ head.ck \in ckpts

\* A checkpoint keeps its place in the log.
CheckpointsStable == \A c \in ckpts : c.ord + 1 <= Len(chain) /\ Index(c.ord, c.off) = c.k

\* No copy of a record is in the log that its writer doesn't know may have
\* landed (SharedLogCommit's "dup": the runtime counts every such write).
CopiesKnown ==
  \A r \in AllRecs : Cardinality({j \in 1..Len(Abstract) : Abstract[j] = r}) <= sends[r]

Safety ==
  /\ ClosedSegments /\ NothingPastClose /\ ReadsAreSlices /\ AckedAtReceipt /\ NamedExist
  /\ Readable /\ CheckpointsStable /\ CopiesKnown
=============================================================================
