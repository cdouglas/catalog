------------------------------ MODULE MCAzure ------------------------------
(* Scenarios for AzureLog: what each writer appends, in order. *)
EXTENDS AzureLog

OneEach == [w \in {1, 2} |-> IF w = 1 THEN << "a" >> ELSE << "b" >>]
TwoEach == [w \in {1, 2} |-> IF w = 1 THEN << "a", "c" >> ELSE << "b", "d" >>]
\* Writer 1 appends again after writer 2 may have moved the log on.
TwoOne == [w \in {1, 2} |-> IF w = 1 THEN << "a", "c" >> ELSE << "b" >>]
Three == [w \in {1, 2, 3} |-> CASE w = 1 -> << "a" >> [] w = 2 -> << "b" >> [] OTHER -> << "c" >>]

-----------------------------------------------------------------------------
(* Witnesses: each Not_ invariant must be violated by some configuration
   (check.py --witnesses), so the passing configurations reach the branch. *)

Not_Rotated == Len(chain) < 2
Not_TwoRotations == Len(chain) < 3
Not_Pad == ~seen.pad
Not_BlockClosed == ~seen.blocks
Not_CasLost == ~seen.caslost
Not_CheckpointInHead == head.ck.ord = 0 /\ head.ck.off = 0
Not_Trimmed == head.segs[1].ord = 0
Not_Swept == comp.sw = 0
Not_Writer404 == ~seen.w404
Not_Reader404 == ~seen.r404
Not_ReadNext == ~seen.rnext
Not_LateLanding == ~seen.late
\* A record twice in the log (a copy the runtime knew of).
Not_Copy == \A i, j \in 1..Len(Abstract) : i # j => Abstract[i] # Abstract[j]
=============================================================================
