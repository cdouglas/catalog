# Catalog log segments (design)

Status: proposed 2026-11-14, from the review of the Azure log's format
(2026-11-13 to 11-14), for review. When built, it replaces the Azure log's head and
checkpoint blobs ([AZURE_LOG.md](AZURE_LOG.md), [AZURE_LOG_FORMAT.md](AZURE_LOG_FORMAT.md)).
It makes one correction to the reviewed design. A segment's end is fixed in the
segment's own append order, not by a record in the catalog object. The reason is under
[Decisions beyond the review](#decisions-beyond-the-review), D1.

## Summary

The catalog stays one object at `<warehouse>/catalog`, read and written through
atomicio, in format 3's framing ([ATOMIC_FILE_LOG.md](ATOMIC_FILE_LOG.md)). Its records
can move out to segments. A segment is an append-only object of a segment store (Azure
Append Blobs first), which writers append to blind.
- **Installs.** Records in the catalog object install segments in a total order. Each
  Install sets its segment's record limit K. Installs may run ahead, so several
  segments can be open at once. An Install is never valid in a segment.
- **Ends.** A segment ends at its Kth record, or earlier at a seal frame inside it.
  Both are in the segment's own order:
  [Why a segment's end is in the segment](#why-a-segments-end-is-in-the-segment). A
  segment's records follow the previous segment's, so they count only once it has ended.
- **Appends.** A writer appends a record to any open segment, or copies of it to
  several. The first copy in the log decides its outcome.
- **Compaction.** One CAS replace of the catalog object writes a checkpoint whose
  position says where in the segments replay resumes.

The head blob, the separate checkpoint blobs and the pad go away. So do OI-1 (nothing
records which log), OI-3 (head unvalidated), OI-6 (unbounded append loop) and the head's
and checkpoints' missing checksums. For a named segment the sweep's grace counts from
its retirement, which answers half of OI-5 (grace from last-modified).

To the runtime this log looks like `AppendBlobLog` today: blind appends and separate
checkpoints (`Capabilities(true, false, false, true, max)`). So the commit loop,
compaction, GC and paging run unchanged. The one runtime change is that a void append
counts as a lost race (D4).

## Objects

| Object | Where | Format | Written by |
|---|---|---|---|
| Catalog object | `<warehouse>/catalog` | format 4: format 3's header, checkpoint frame and record frames, with segment records | atomicio: conditional append, or CAS replace |
| Segment | `<prefix>/<name>`, or the location its Install names | segment frames (below) | blind appends through the segment store |
| Page | `<warehouse>/catalog.committed/` | unchanged | unchanged |

`<prefix>` is `fileio.catalog.log.location`. A segment's name is
`seg-<ordinal>-<nonce>`, with a fresh nonce, as in v1 (AZ4). Only an installer creates a segment, and only before it sends the
Install. A writer that finds a segment missing never creates it. A catalog object
holds segment records only when the log keeps segments. A log without a segment store
writes format 3, as today.

## Records

Two new bodies in `LogRecord`'s oneof (log.proto). Both are provider records:
`SegmentedLog` reads them from the catalog object and leaves them out of every view, so
the runtime's replay never sees one.

```proto
// Installs segment `ordinal` after the last installed one. Replay of the catalog object takes
// it only if `ordinal` is the last installed one plus 1, or, as the first Install
// after a checkpoint, the ordinal of the checkpoint's position.
message InstallSegment {
  uint64 ordinal = 1;
  string name = 2;      // under the log's prefix, with its nonce
  string location = 3;  // optional: a URL in place of prefix + name
  uint64 limit = 4;     // the segment ends at its limit-th record, or earlier at a seal (D2)
}

// A segment a checkpoint covers wholly, kept until the sweep deletes it.
message RetiredSegment {
  uint64 ordinal = 1;
  string name = 2;
  string location = 3;
  bytes end = 4;              // the position after its last record
  fixed64 retired_at_ms = 5;  // the compactor's clock when it retired it
}
```

`LogRecord.body` gains `InstallSegment install_segment = 5` and
`RetiredSegment retired_segment = 6`. Neither carries an id.

**Segment frames** are v1's frames with the pad replaced by a seal frame:

```abnf
segment = *frame
frame   = record / seal
record  = varint body trailer   ; varint length n > 0, then a LogRecord's n octets
seal    = %x00 trailer          ; ends the segment, taking the rest of its limit
trailer = ordinal crc           ; the segment's ordinal (u64le), CRC32C (u32le) of all before
```

A seal frame is 13 bytes. Every frame after a segment's end is void. Appends carry no
condition: there is no byte bound (D2). Each frame is one block of the store, and
before its end a segment holds fewer than K frames, all records. So a seal frame
always fits if K is at most the store's block limit (50,000 on Azure), and if K − 1 of
the largest frame and a seal frame fit the store's largest object. A log checks both
when it opens, and installs no segment that breaks them (SG8).

**Positions** are the end of the last record read, or a checkpoint's position when no
record follows it:

```abnf
position = ordinal records offset ; ordinal u64be, records u64be, then the store's bytes
```

`records` counts the segment's records before this point, which is what the limit
counts. `offset` is the store's
encoding: a big-endian u64 byte offset on Azure. Only the provider reads a position.
The runtime compares positions for equality, and the provider orders them by
`(ordinal, records)`. `LogCheckpoint.through` carries the checkpoint's position (the
review's "cursor"), so `LogCheckpoint` needs no new field. A position is never
normalized to the start of the next segment. Two tails at one place therefore have
equal bytes, as TAIL_READS.md "Positions" requires.

## Reading

`read()`:

1. GET the catalog object (AtomicFileLog's tail read, so a refreshed instance reads
   only new bytes). The checkpoint gives position `c` in segment `m0`. The records
   after it give Installs `m0..t`, each with its limit, and the Retired list.
2. Read each segment from `m0` (from `c`'s offset) to `t`. For each one:
   - check every frame's trailer: its ordinal and CRC;
   - fail the read on a record whose body is an Install or a Retired entry, which is a
     format error: an Install is never valid in a segment;
   - stop at the segment's end: its Kth record or its first seal frame;
   - if it has not ended, stop there: no later segment can be ordered yet, though
     later segments may already hold records.
3. Return the checkpoint and the records read, with `start()` = `c` and the tail at the
   last record's position.

`readAfter(tail)` reads the tail's segment from the tail's offset, and on into the
later segments this instance knows while each one has ended. It reads the catalog
object only when it runs out of segments it knows, to find the next Install. A steady-state
refresh is one GET. A whole read is two (v1 needed three: head, checkpoint, segment).
A segment that is gone (swept) is `TrimmedException`, and the runtime reads afresh.

Replay of the catalog object discards any record frame other than an Install or a
Retired after the first Install. That is the review's rule, "records in the catalog
object while a segment is open are void". Letting records live in the catalog object
again is [Later](#later) (D9).

## Writing

A segment is **open** to a writer when the catalog object it read installs it and the
writer hasn't seen it end. `append(record)` sends the record to the first open segment
this instance knows, blind and with no condition. The frame names that segment's
ordinal, so a copy for another segment is framed again. On success the receipt is the
segment's ordinal and name, the offset from the reply, the frame's length and CRC, and
its index: the reply's block count (Azure's `x-ms-blob-committed-block-count`).

- **Past the end.** An index above K, or a store that refuses the append at its block
  limit, means the copy is void, with no read. The writer sends the record to the
  next open segment, installing one if there is none.
- **Copies.** A writer may also send copies to later open segments, for example near
  the first one's limit, so that a void copy costs no extra round trip. The receipt
  lists every copy. Copies are safe by the log's rules for unconditional appends
  (LOG.md "The commit loop"): replay applies a record once by its id, and a record
  that lost a race never applies later (S2). So only the first copy in the log can
  apply.
- **NotFound** (the segment was swept) re-reads the catalog object and sends to its
  first open segment.
- The loop is bounded by attempts. Past the bound it fails with `LogNotSentException`,
  since nothing landed. That fixes v1's OI-6.

**Installing.** To install segment `m+1` (with `m` the last installed), a writer
creates it under a fresh name, then appends `InstallSegment(m+1, K)` to the catalog
object with format 3's condition (its version and offset as read; on a store without
appends, a CAS replace). A loser reads again. If an `m+1` landed, it uses that one and
leaves its own blob to the sweep. Replay rejects any ordinal but the next, so each
Install adds exactly one segment. A checkpoint or another Install landing in between
also makes an Install retry; that costs one read. Installing several in one write is
[Later](#later).

A writer installs the next segment ahead when its copy's index is within a margin of
K (provisional: the last 1%), so that a rotation doesn't wait for an Install. Installs
needn't be scarce: a segment opened in excess is ended by a seal frame, below.

**Ending a segment early.** A seal frame takes the rest of a segment's limit: the
segment ends there, as if its remaining records had landed. A writer whose record is in
a segment after `m` needs `m` ended to learn its outcome, so it appends a seal frame to
`m` if `m` hasn't ended. That covers a crash after an Install, segments opened in
excess, a limit that traffic hasn't reached, and writers that chose a later segment,
with no owner: any writer that needs `m` ended ends it. A seal frame after the end is
void and harmless. Since a record in a later segment costs the earlier ones their
remaining limits, writers send to the first open segment, and to later ones only as
copies or when it is past its end.

## A commit's outcome

`readThrough(receipt)` takes the receipt's copies in log order (by ordinal). For each
copy, in segment `m`:

1. Read through `m`, ending (with a seal frame) any earlier segment that has not
   ended.
2. Find the copy's frame at its offset, and check its length and CRC.
3. If `m` ended before that offset, the copy is void: it is not in the log, and no
   later write can put it there. Go on to the next copy.
4. Otherwise return the view through this copy. Its records include the record (P3),
   and replay says whether it applied. Later copies change nothing.

If every copy is void, throw `LogConflictException`. A copy in a retired segment is
void if it lies past the Retired entry's `end`. Otherwise it is `TrimmedException`, as
today.

Once a reader has read a record, no later write voids it. A seal frame lands after it
in the store's order, and a limit is fixed before the segment takes any record. That is
the property D1 restores.

## Why a segment's end is in the segment

The catalog object is totally ordered, and so is each segment, but nothing orders one
against the other. A write to the catalog object is conditioned on the catalog object's version
alone, and an append to a segment doesn't change that version. Readers apply an open
segment's records as they find them, since that is what makes a commit one blind
append and a read. So an end that a catalog record sets, after the segment has taken
records, can cut off records that readers have already applied:

| Step | Catalog object | Segment `m` | What happens |
|---|---|---|---|
| 1 | `Install(m)` | r1 r2 r3 | |
| 2 | | | Sealer S reads `m` to r3 and will write `Seal(m, 3)` |
| 3 | | r4 | Writer W appends r4 blind. Nothing stops it: an account with a hierarchical namespace has no Seal Blob, and no limit was set |
| 4 | | | Reader R reads the catalog object (`m` open, no seal), then `m` (r1–r4), and applies r4. W reads through its write and reports its commit |
| 5 | `Seal(m, 3)`, `Install(m+1)` | | S's write lands: the catalog object hasn't changed since S read it |

The log is now r1–r3, then `m+1`. r4 isn't in it. W reported a commit that the log
doesn't hold, and R's replica holds a record that no later reader sees (P1). S can't
avoid this. No read of `m` bounds what other readers have read, and no store makes the
catalog object's write conditional on `m`. A limit set by `Install(m+1)` has the same
trace (D2).

With the end in the segment, step 5's seal is a seal frame appended to `m`, and `m`'s
own order puts it before r4 or after it:
- **r1 r2 r3 seal r4.** Every reader stops at the seal, so R never applies r4. W's
  read-through finds r4 past the end, a lost race, and W sends it again to `m+1`.
- **r1 r2 r3 r4 seal.** r4 is in the log for every reader.

A limit set by `m`'s own Install is safe for the same reason: it is fixed before `m`
takes a record. A limit set later, by a catalog record or by the next Install, is not.
A catalog record of an end already fixed in `m` would be safe, but it would only be a
cache. Readers need `m`'s records, so they read `m` to its end anyway. Opening `m+1`
early is safe for the same reason too: its records wait for `m`'s end, wherever that
falls.

The other way to close the gap is for readers not to apply an open segment's records
until a catalog record ends the segment. Then a catalog seal could set the end, but
every commit would wait for a write to the catalog object. Commits would become group
commits through the very object that segments take appends off. This design doesn't do
that.

## Compaction, retirement and the sweep

The runtime's separate-checkpoint compaction (`LogRuntime.compact`) runs unchanged:
GC record, read through it, PageOuts, `putCheckpoint`, `trim`.

- **`putCheckpoint(through, checkpoint)`** reads the catalog object. If its
  checkpoint's position is at or after `through`, it is done: every checkpoint at one
  position is the same (I11). Otherwise it CAS-replaces the object with:
  - the header (format 4);
  - the checkpoint frame;
  - an `InstallSegment` for each segment from `through`'s ordinal on, as the object
    names them, each with its limit (open segments installed ahead among them);
  - a `RetiredSegment` for each segment before `through`'s ordinal that the object
    named: those newly retired, with now's time and their end, and the older entries
    whose TTL has not passed.

  A CAS that loses (an Install landed) reads again and retries. The replace never
  carries records, so appends to segments never race a compaction.
- **`trim(through)`** is the sweep. It deletes each retired segment whose TTL
  (`fileio.catalog.log.retire.ttl.ms`) has passed since `retired_at_ms`. It also lists
  the prefix and deletes unnamed blobs (losers' segments, creators' leftovers) whose
  ordinal is at most the newest Install's. No Install of that ordinal can land any
  more, since replay takes only the next ordinal, so no Install in flight names one.
  The rule needs no clock, and so works with the catalog object and the segments on
  different stores. Retired entries keep overridden locations, which a listing can't
  find. An unnamed blob at an overridden location is leaked; only a migration makes
  one.
- A reader holding an older catalog object that finds a segment gone reads afresh. A
  writer holding one appends to an ended segment (void) or a gone one (NotFound), and
  goes on from a fresh read. A clock ahead of the compactor's can only make the sweep
  early, which costs a reader a restart, never a record.

**Creating the log** creates segment 0, then puts the catalog object if absent, with
the first checkpoint (position `(0, 0, 0)`) and `InstallSegment(0, K)`. A creator that
loses finds the log exists (`LogConflictException`); its segment is the sweep's.

**Finding the log.** A client finds the catalog at `<warehouse>/catalog`, as with
format 3. A client opened without a segment store that finds a format 4 object refuses
it: "this catalog keeps segments". It never appends to it. That closes OI-1.

## Decisions beyond the review

| | Decision | Why |
|---|---|---|
| D1 | A segment's end is fixed in its own order: its Kth record or its first seal frame. There is no `SealSegment` record in the catalog object. The review's "seal frame as an optional hint" becomes the authority. | A catalog record of an end written after a read can void a record that a reader has already applied, and a writer may already have reported it as committed: [the example](#why-a-segments-end-is-in-the-segment). The store's order is the only one every reader of a segment sees alike. |
| D2 | Every Install sets its own segment's record limit K, never the next one's. There is no byte bound: appends carry no condition, and K is bounded so that a seal frame always fits (SG8). | For the next Install's limit, the same example, with K = 3 set by `InstallSegment(m+1)`. A byte bound can't end a segment, since a smaller record fits after a refused one (v1 needed a pad), and some stores can't enforce one. A record count works on every store. |
| D3 | A segment ends early at one seal frame, which takes the rest of its limit, not at no-op records. | One blind append instead of up to K. No record type is added. |
| D4 | The log presents blind appends and separate checkpoints to the runtime, as `AppendBlobLog` does. `putCheckpoint` is the CAS replace; `trim` is the sweep. A void append is a `LogConflictException` from `readThrough`, which `LogRuntime.write` lets through as a lost race. | The runtime's separate-checkpoint path is built and fuzzed. Today `write` wraps every read-through failure as ambiguous; that path is still correct, but it costs a read and reports the wrong kind. |
| D5 | Positions are `(ordinal, records, store offset)`, the end of the last record read, carried in `LogCheckpoint.through`. | The record count is what the limit counts, from a reader that starts mid-segment. Never normalizing positions keeps one encoding for one place. |
| D6 | The checkpoint's segment table is records after the checkpoint frame, in the same replace: Installs from the position's segment on, then Retired entries. | The runtime's `LogCheckpoint` is unchanged, and replay of the catalog object has one rule for Installs. |
| D7 | The code is a new `SharedLog`, `SegmentedLog` (intentlog-fileio), over an `AtomicFileLog` for the catalog object and a `SegmentStore` SPI (intentlog-core). intentlog-azure becomes a segment store. | The catalog object is format 3's object, so its code is reused, not copied. |
| D8 | Segment names carry a nonce, and only an installer creates a segment, before its Install. | v1's AZ4. A stale writer can't bring back a swept segment, and a reader of an older catalog object can't read a newer blob under an old name. |
| D9 | Records in the catalog object again (the review's "after the tail is sealed and no newer one is installed") are a segment too: an Install that names the catalog object. Built later. | It keeps ordinals contiguous and positions uniform, and the run ends in the object's own order (the next Install). Positions in it are the record count alone, since a compaction moves its frames. |
| D10 | Format 4 is a catalog object with segment records. A log without segments writes format 3. | The review asked for a distinct number, so fixtures and tests name their layout. |
| D11 | Several segments may be open at once. A writer may send a record to any open segment, or copies of it to several; the first copy in the log decides. | Installs can run ahead, so a rotation never waits for one. A copy in the next segment makes a commit at a segment's end one round trip. Copies are safe by the committed set and S2. |

## Code

| Module | Change |
|---|---|
| intentlog-core | `SegmentStore` SPI: create a segment under a name if absent; append a frame blind (returns the offset and the block count); read from an offset (bytes, length, blocks); delete; list; the limits (blocks, the largest object, the largest frame). Segment frames move here from intentlog-azure's `Frames`, with the seal frame in place of the pad. log.proto and `LogEnvelope` gain `InstallSegment` and `RetiredSegment`. `LogRuntime.write` lets a read-through's `LogConflictException` through as a lost race. |
| intentlog-fileio | `SegmentedLog`: the catalog object as an `AtomicFileLog` writing format 4, the merged view, positions and tails, appends and rotation, `readThrough`, creation, `putCheckpoint`, `trim`. `AtomicFileLog` reads format 4 when it has a segment store, and refuses it otherwise. |
| intentlog-memory | `InMemorySegmentStore`: limits in blocks and bytes, and the fault hooks of `InMemorySharedLog`. |
| intentlog-azure | `AppendBlobSegments`: a `SegmentStore` on `AppendBlobStore` (Append Block with no condition, nonce names). `AppendBlobLog`, `AzureLogHead` and the checkpoint blobs are removed. So are `AppendBlobStore`'s block-blob requests, if nothing else uses them. `AzureLogHistory` becomes a check of each checkpoint against a replay of the segments' history. |
| iceberg-fileio-catalog | `AzureLogs` opens a `SegmentedLog`: the catalog object on the catalog's FileIO, segments at `fileio.catalog.log.location`. `LocalCatalogMode`'s `AZLOG*` modes run it over `InMemoryAtomicFileIO` and the in-memory Azure store. |
| spec/tla | `SegmentedLog.tla` and its configurations in `check.py`, in place of `AzureLog.tla`. |
| fuzz | `fuzz/mutants/segments.sh`. ce-fuzz's `azure` job runs the new fuzzer. |
| docs | AZURE_LOG.md and AZURE_LOG_FORMAT.md describe this log. ATOMIC_FILE_LOG.md gains format 4. LOG.md's providers, CLAUDE.md, and the format explorer (format 4 objects and segment frames) follow. |

Unchanged: blind appends and the measurement behind them (AZURE_LOG.md "Why"), the
store's failure classes and its SDK with retries off, the compaction policy's knobs,
pages, and the catalog above the log.

## Invariants

| | Invariant | Why it holds |
|---|---|---|
| SG1 | The log is the catalog object's checkpoint, then the records of its segments in ordinal order, each up to its end. Every reader of a segment reads the same frames, in the same order, to the same end. | Append Block is atomic and reads are strongly consistent; ends are limits and seal frames, both in the store's order (D1). |
| SG2 | A reader reads segment `m+1` only after reading `m` to its end, so what it returns is a prefix of the log (P1). | Reading, step 2. |
| SG3 | No write voids a record a read has returned. | A seal frame lands after it; a limit is fixed before the segment takes records (D1, D2). |
| SG4 | Ordinals are contiguous from the checkpoint's. Each Install names a segment its installer created under a fresh name before sending it. | The catalog object's condition, replay's ordinal rule, nonce names (D8). |
| SG5 | After the first Install, no record frame in the catalog object is in the log. | Replay of the catalog object. |
| SG6 | The sweep deletes a named segment only after a checkpoint covers it wholly and its TTL has passed since it was retired. It deletes an unnamed blob only once its ordinal is at most the newest Install's, so no Install in flight can name it. | Retirement happens only in `putCheckpoint`, for segments before the position's; replay takes an Install only of the next ordinal. |
| SG7 | P1–P6 as the log meets them. P4 is not claimed: appends are blind. | As stated. |
| SG8 | Every segment can end. Before its end it holds fewer than K frames, so a seal frame fits. | K is at most the store's block limit, and K − 1 of the largest frame and a seal frame fit its largest object, checked when the log opens. |
| SG9 | A record applies at most once, at its first copy in the log. | The committed set skips a copy of a record that applied; S2 keeps a rejected one rejected. |

The model's mistakes, each a switch that must break a property:

| Switch | The mistake | Breaks |
|---|---|---|
| `catalogseal` | a catalog record states a segment's end from an earlier read (the review's `SealSegment`) | SG3, P1 |
| `limitatnext` | a segment's limit set by the next segment's Install | SG3 |
| `readahead` | a reader applies `m+1` before `m` has ended | SG1, SG2 |
| `voidack` | `readThrough` accepts a frame past its segment's end | SG3, an honest outcome |
| `overlimit` | an Install sets a limit above the store's block limit | SG8: a segment fills before its end, takes no seal frame, and the log stops |
| `lastcopy` | `readThrough` decides by a later copy while an earlier one is in the log | an honest outcome, when a compaction drops the id between them (LOG.md's `dup`) |
| `createonmissing` | a writer that finds a segment missing creates it | SG4, then SG3 |
| `ordinalgap` | replay takes an Install of any ordinal | SG1 |
| `rootrecords` | replay applies record frames in the catalog object after an Install | SG5, P1 |
| `nested` | replay of a segment takes an Install or a Retired entry in it | SG1, SG4 |
| `sweepearly` | the sweep deletes a segment the checkpoint doesn't cover | P5 (`Readable`) |
| `sweepinflight` | the sweep deletes an unnamed blob of the next ordinal | SG4: a delayed Install names a deleted segment, and the log stops |
| `offsetfromlength` | the receipt's offset is the length before the append, not the reply's | an honest outcome, as in v1 |
| `sdkretry` | a timed-out append is resent below the runtime | the shared-log model's `dup` |

## Testing

As LOG.md "Testing a provider", per stage:

- **The model first**, as with `SharedLogCommit.tla` and `AzureLog.tla`. The catalog
  object has conditional appends and CAS replace. Segments have blind appends, a block
  limit, record limits and seal frames. The model also has Installs ahead, the Install
  race, copies, appends delayed past an end, readers holding older catalog objects,
  compaction with positions, retirement with a TTL (as reader epochs), and the sweep. `check.py` holds every switch above to its verdict. `catalogseal`'s
  trace is D1's example.
- **`SharedLogContract`** on `SegmentedLog` (`TestSegmentedLogContract`), over both
  in-memory stores.
- **`TestSegmentedCommitProtocol`** (intentlog-fileio): the testkit's `CommitProtocol`
  fuzzer on `SegmentedLog` over `InMemoryAtomicFileIO` and `InMemorySegmentStore`. It
  draws:
  - faults on every request of both stores;
  - small limits and block limits, Installs ahead, early seals, and copies;
  - appends delayed until after an end;
  - one to three instances with stale catalog objects;
  - sweeps at TTL 0.

  At the drain, every replica a read returned must read on to the end (as
  `staleReaders` does today). `TestAzureCommitProtocol` runs the same fuzzer over
  `AppendBlobSegments` on `InMemoryAppendBlobStore`.
- **Mutants** (`fuzz/mutants/segments.sh`): each switch above, put in the code, fails
  the fuzzer.
- **Catalog suites**: `TestLocalCatalog*` in the `AZLOG*` modes. Also
  `TestCatalogFaultProperties` and `TestCatalogRaceProperties`, whose Append Blob draws
  open the new log.
- **Real account**: `AzureLogCatalogIT`, with the warehouse (the catalog object) on
  the account through the ADLS FileIO.

## Plan

Each stage is one or more commits on branch `log-segments`, each fix with its test.

| Stage | Delivers | Gate before the next |
|---|---|---|
| 0. Model | `SegmentedLog.tla` and its configurations | `check.py`: every configuration at its expected verdict; `catalogseal` and `limitatnext` fail SG3 |
| 1. Wire and SPI | log.proto, `LogEnvelope` codecs, `SegmentStore`, segment frames with the seal frame, `InMemorySegmentStore`, the format explorer's decoder and fixtures | codec round trips, `TestFormatExplorer`; `mvn install` green with nothing else changed |
| 2. `SegmentedLog` | reads, appends with limits, rotation past the end, the Install race, seal frames, `readThrough` with void appends, creation, `putCheckpoint`, the sweep; the runtime's lost-race change (its own commit and test) | contract, regressions, `TestSegmentedCommitProtocol` at 2,000 tries, every mutant caught |
| 3. Open segments and copies | Installs ahead, sends to later segments, copies and their receipts; the fuzzer draws them | as stage 2 |
| 4. Azure and the catalog | `AppendBlobSegments`, `TestAzureCommitProtocol`, `AzureLogs`, format 4 refusal, the `AZLOG*` modes, catalog fuzzers' draws; v1's head, checkpoint blobs and pad removed; docs describe the new log | `mvn install` (every suite), fault and race properties locally, mutants; then merge to main |
| 5. Cloud and havoc | `AzureLogCatalogIT` on the real account; a havoc round (`azure` at 1,000,000 tries; fault and race) | the user's go for each; failures fixed forward |

## Configuration

| Property | Default | Meaning |
|---|---|---|
| `fileio.catalog.log` | `atomic-file` | `azure-append`: a segmented log on Azure Append Blobs |
| `fileio.catalog.log.location` | none: required | the segments' prefix, a Blob URL |
| `fileio.catalog.log.segment.records` | the store's block limit (provisional) | K, the limit each Install sets. At most the store's block limit, and small enough that K − 1 of the largest frame fit an object (SG8) |
| `fileio.catalog.log.checkpoint.share`, `.min.bytes`, `.records` | as today | when to compact |
| `fileio.catalog.log.retire.ttl.ms` | 60,000 | how long a retired segment stays before the sweep deletes it. It replaces `trim.grace.ms` |

`fileio.catalog.log.checkpoint.keep` goes: checkpoints live in the catalog object.
`fileio.catalog.log.append` `conditional` (the ablation) is [Later](#later).

## Later

- Records in the catalog object again, as a segment there (D9).
- Several Installs in one write.
- Segment stores for S3 Express (positioned appends, 10,000 per object; a blind append
  is a size read and an append at it), Kafka and SQS FIFO (one segment, never ended; the
  provider's retention must outlast the checkpoint interval, or replay finds a gap).
- The conditional ablation on segments (the position condition at the tail's offset).
- A migration between stores: compact, then install a segment elsewhere.
- Measuring a read (two GETs against v1's three) and a rotation on the real account.
- Sealing a segment early by its bytes or age, if retention needs it.
- Copies by default near a segment's end, if measurement favors them.

## Open questions

1. K's default. The store's block limit makes the fewest rotations. A smaller K bounds
   how much a segment holds until a checkpoint passes its end. Should K come from
   recent commit rates or a service instead?
2. `AzureLogHistory` checks each checkpoint against a replay of the log's history.
   Does its replacement keep every segment's frames in memory in the `AZLOG*` modes,
   as now, or sample them?
