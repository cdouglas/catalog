# Catalog log segments (design)

Status: designed 2026-11-14 from the review of the log v1's format (2026-11-13 to 11-14),
and built 2026-11-15 ([Progress](#progress)): the catalog's log on Azure
([AZURE_LOG.md](AZURE_LOG.md); the format, for review, [AZURE_LOG_FORMAT.md](AZURE_LOG_FORMAT.md)).
It made one correction to the reviewed design. A segment's end is fixed in the segment's own
append order, not by a record in the catalog object. The reason is under
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

There is no head blob, no checkpoint blob and no pad. The catalog object says which log
a catalog keeps (format 4, which a client without a segment store refuses) and where its
segments are (D13), so a client needs only credentials for them; every frame,
the catalog object's and the segments', is checked; an append tries a bounded number of
segments; and a named segment's TTL counts from its retirement.

To the runtime this log looks like the log v1 did: blind appends and separate
checkpoints (`Capabilities(true, false, false, true, max)`). So the commit loop,
compaction, GC and paging run unchanged. The one runtime change is that a void append
counts as a lost race (D4).

## Objects

| Object | Where | Format | Written by |
|---|---|---|---|
| Catalog object | `<warehouse>/catalog` | format 4: format 3's header, checkpoint frame and record frames, with segment records | atomicio: conditional append, or CAS replace |
| Segment | `<prefix>/<log id>/<name>`, under the catalog object's prefix (D13), or the location its Install names | segment frames (below) | blind appends through the segment store |
| Page | `<warehouse>/catalog.committed/` | unchanged | unchanged |

`<prefix>` is the catalog object's (D13). The log's creator writes the prefix it was given
(`fileio.catalog.log.location`) after the checkpoint's position, and every checkpoint restates
it, unless it is the default every client derives from the catalog object's own location: on
Azure, the object's Blob URL with `.segments` appended. Then the object records nothing, and a
client reads the segments at the default. A client given another prefix uses the object's, and
warns that its own is ignored; one given none needs no prefix to open a log, only to create one
where no default is derived. An entry that names a location (a URL in the
segment store, `SegmentStore.nameAt`) finds its segment there, not under the prefix: a log never
writes one, but a migration that moves the prefix writes them for the segments it leaves at the
old one, so they are read, appended to, sealed and deleted where they are. A location the store
can't reach (an account the client has no credentials for) fails the read rather than read under
the prefix. A
segment's name is `seg-<ordinal>-<nonce>`, with a fresh nonce, as in v1 (AZ4), and its object
is under the log's id, `<prefix>/<log id>/<name>` (D12): every catalog object of the log holds
the id in its checkpoint, so no entry carries it, and logs whose catalogs were given one prefix
keep their segments apart. Only an installer creates a segment, and only before it sends the
Install. A writer that finds a segment missing never creates it. A catalog object
holds segment records only when the log keeps segments. A log without a segment store
writes format 3, as today.

## Records

Four new bodies in `LogRecord`'s oneof (log.proto). All are provider records:
`SegmentedLog` reads them from the catalog object and leaves them out of every view, and
replay refuses one in a view.

```proto
// Installs segment `ordinal` after the last installed one. Replay of the catalog object takes
// it only if `ordinal` is the last installed one plus 1, or, as the first Install
// after a checkpoint, the ordinal of the checkpoint's position.
message InstallSegment {
  uint64 ordinal = 1;
  string name = 2;      // seg-<ordinal>-<nonce>, under <prefix>/<log id>/ (D12)
  string location = 3;  // optional: a URL in place of prefix + name
  uint64 limit = 4;     // the segment ends at its limit-th record, or earlier at a seal (D2)
}

// A segment a checkpoint covers wholly, kept until its segment is deleted.
message RetiredSegment {
  uint64 ordinal = 1;
  string name = 2;
  string location = 3;  // its Install's: the only reference to it once the Install is gone
  bytes end = 4;              // the position after its last record
  fixed64 retired_at_ms = 5;  // the compactor's clock when it retired it
}
```

A Retired entry keeps its Install's name and location because a checkpoint drops the
Install: from then on the entry is the only thing that says where the segment is, and the one
reference a deletion can follow without listing, while a listing of the prefix can't find a
segment at another location at all.

`LogRecord.body` gains `InstallSegment install_segment = 5`,
`RetiredSegment retired_segment = 6`, `bytes checkpoint_through = 7`: the position
the catalog object's checkpoint covers, the frame right after the checkpoint, and `string
segment_prefix = 8`: where the segments are, the frame after that, absent at the default
(D13). None
carries an id, nor the log's id, which the checkpoint holds. A checkpoint is opaque to its
log (the SharedLog contract writes them without a `through`), so the catalog object says
where its checkpoint is, as v1's head did; the runtime checks the `through` it wrote
against it. A catalog object is the
header, the checkpoint frame, `checkpoint_through`, `segment_prefix` unless it is the
default, the Installs from the checkpoint's segment on, then Retired entries. A
`segment_prefix` anywhere else is void.

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
always fits if K is at most the store's block limit B (50,000 on Azure), and if K − 1 of
the largest frame and a seal frame fit the store's largest object. A segment's K is its
installer's, and a writer's records land in segments others installed, so the largest
frame must leave room for a seal at any K a writer may set: it is the smaller of the
store's largest block and a (B − 1)th of its largest object less a seal frame, whatever
K the writer sets itself. A log checks K against B when it opens, and installs no
segment past B (SG8). Sized by its own K instead, a writer with a small K sent records
that filled, before its Kth record, a segment a writer with a larger K installed, with
no room left for a seal frame; the segment never ended and the log stopped there
(Bug 115 (segment that cannot end)).

**Positions** are the end of the last record read, or a checkpoint's position when no
record follows it:

```abnf
position = ordinal records offset ; ordinal u64be, records u64be, then the store's bytes
```

`records` counts the segment's records before this point, which is what the limit
counts. `offset` is the store's
encoding: a big-endian u64 byte offset on Azure. Only the provider reads a position.
The runtime compares positions for equality, and the provider orders them by
`(ordinal, records)`. The catalog object's `checkpoint_through` carries the checkpoint's
position (the review's "cursor"), and the runtime's checkpoints name it in
`LogCheckpoint.through` too, so `LogCheckpoint` needs no new field. A position is never
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
  since nothing landed.

**Installing.** To install segment `m+1` (with `m` the last installed), a writer
creates it under a fresh name, then appends `InstallSegment(m+1, K)` to the catalog
object with format 3's condition (its version and offset as read; on a store without
appends, a CAS replace). A loser reads again. If an `m+1` landed, or the object it reads is
another log's, it deletes its own blob, which no Install can name, and goes on with the
object it read; one whose delete fails leaves it to its log's sweep (to none, if the object
is another log's). Replay rejects any
ordinal but the next, so each Install adds exactly one segment. A checkpoint or another Install landing in between
also makes an Install retry; that costs one read. Installing several in one write is
[Later](#later).

A writer installs the next segment ahead when its copy's index is within a margin of
K (provisional: the last 1%), so that a rotation doesn't wait for an Install. Installs
needn't be scarce: a segment opened in excess is ended by a seal frame, below.

**Ending a segment early.** The limit is an implicit seal at the Kth record. A seal
frame closes a segment earlier, so that the next segment's records can count. It
carries no count: it ends the segment where it lands, taking the rest of the limit. A
seal that named a count from an earlier read could land after more records than that
and void them, which is D1's problem inside the segment. Safety needs only the limit.
Progress needs a seal frame whenever traffic leaves a segment short of K. A writer
whose record is in a segment after `m` needs `m` ended to learn its outcome, so it
appends a seal frame to `m` if `m` hasn't ended. That covers a crash after an Install, segments opened in
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
  - the object's `segment_prefix`, if it records one: its creator's, whatever prefix the
    compactor was given (D13);
  - an `InstallSegment` for each segment from `through`'s ordinal on, as the object
    names them, each with its limit (open segments installed ahead among them);
  - a `RetiredSegment` for each segment before `through`'s ordinal that the object
    named: those newly retired, with now's time and their end, and the older entries
    but those it deleted. It deletes the segment of each older entry whose TTL has
    passed, where the entry locates it, before the replace drops the entry, and keeps
    the entry of one whose delete failed: an entry is its segment's only reference that
    needs no listing to follow.

  A CAS that loses (an Install landed) reads again and retries. The replace never
  carries records, so appends to segments never race a compaction.
- **`trim(through)`** is the sweep. It deletes each retired segment whose TTL
  (`fileio.catalog.log.retire.ttl.ms`) has passed since `retired_at_ms`, where its
  entry locates it. Then it lists its own log's segments, `<prefix>/<log id>/`, and
  deletes unnamed blobs (those of installers that lost and failed to delete them) whose
  ordinal is at most the newest Install's. No Install of that ordinal can land any more,
  since replay takes only the next ordinal, so no Install in flight names one. The rule
  needs no clock, and so works with the catalog object and the segments on different
  stores. It never lists another log's segments: another log at the prefix has segments
  of the same ordinals that only its own catalog object names (D12).
  Only unnamed blobs need the listing, and they are empty (no writer appends to a
  segment no Install names): a credential that can't list (a SAS without container
  scope) leaks those alone. An unnamed blob at an overridden location is leaked; only a
  migration makes one.
- A reader holding an older catalog object that finds a segment gone reads afresh. A
  writer holding one appends to an ended segment (void) or a gone one (NotFound), and
  goes on from a fresh read. A clock ahead of the compactor's can only make a deletion
  early, which costs a reader a restart, never a record.

**Creating the log** creates segment 0, under the id of the log the first checkpoint
begins and the prefix the creator was given (or the default), then puts the catalog object if
absent, with that checkpoint (position `(0, 0, 0)`), the prefix unless it is the default, and
`InstallSegment(0, K)`. A creator given no prefix that derives no default fails before it writes
anything. A creator that loses finds the log exists
(`LogConflictException`) and deletes its segment 0: it is under a log id no catalog
object holds, so no sweep lists it. One whose delete fails, or whose creation failed
with the catalog object still absent (its put may land yet), leaves it
([AZURE_LOG_FORMAT.md](AZURE_LOG_FORMAT.md) OI-10).

**Finding the log.** A client finds the catalog at `<warehouse>/catalog`, as with
format 3, and the segments where its catalog object says (D13). A client opened without a
segment store that finds a format 4 object refuses
it: "this catalog keeps segments". It never appends to it. A client that finds an object
recording no prefix and derives no default from its location (it was opened at another form of
the location than the creator's, one the default rule doesn't know) fails rather than look
anywhere else.

## Decisions beyond the review

| | Decision | Why |
|---|---|---|
| D1 | A segment's end is fixed in its own order: its Kth record or its first seal frame. There is no `SealSegment` record in the catalog object. The review's "seal frame as an optional hint" becomes the authority. | A catalog record of an end written after a read can void a record that a reader has already applied, and a writer may already have reported it as committed: [the example](#why-a-segments-end-is-in-the-segment). The store's order is the only one every reader of a segment sees alike. |
| D2 | Every Install sets its own segment's record limit K, never the next one's. There is no byte bound: appends carry no condition, and K is bounded so that a seal frame always fits (SG8). | For the next Install's limit, the same example, with K = 3 set by `InstallSegment(m+1)`. A byte bound can't end a segment, since a smaller record fits after a refused one (v1 needed a pad), and some stores can't enforce one. A record count works on every store. |
| D3 | A segment ends early at one seal frame, which takes the rest of its limit, not at no-op records. | One blind append instead of up to K. No record type is added. |
| D4 | The log presents blind appends and separate checkpoints to the runtime, as `AppendBlobLog` does. `putCheckpoint` is the CAS replace; `trim` is the sweep. A void append is a `LogVoidException` (a `LogConflictException`) from `readThrough`, which `LogRuntime.write` lets through as a lost race that leaves nothing to resolve, unless a throttled send of the same write may have landed; a compaction sends a void GC record or PageOut again. | The runtime's separate-checkpoint path is built and fuzzed. Wrapping the read-through's failure as ambiguous is correct, but costs a read and reports the wrong kind; and a void append recorded as a possibly landed race left a commit whose sends all landed void unknown (the fuzzer found both). |
| D5 | Positions are `(ordinal, records, store offset)`, the end of the last record read, carried in the catalog object's `checkpoint_through`. | The record count is what the limit counts, from a reader that starts mid-segment. Never normalizing positions keeps one encoding for one place. |
| D6 | The checkpoint's segment table is records after the checkpoint frame, in the same replace: Installs from the position's segment on, then Retired entries. | The runtime's `LogCheckpoint` is unchanged, and replay of the catalog object has one rule for Installs. |
| D7 | The code is a new `SharedLog`, `SegmentedLog` (intentlog-fileio), over an `AtomicFileLog` for the catalog object and a `SegmentStore` SPI (intentlog-core). intentlog-azure becomes a segment store. | The catalog object is format 3's object, so its code is reused, not copied. |
| D8 | Segment names carry a nonce, and only an installer creates a segment, before its Install. | v1's AZ4. A stale writer can't bring back a swept segment, and a reader of an older catalog object can't read a newer blob under an old name. |
| D9 | Records in the catalog object again (the review's "after the tail is sealed and no newer one is installed") are a segment too: an Install that names the catalog object. Built later. | It keeps ordinals contiguous and positions uniform, and the run ends in the object's own order (the next Install). Positions in it are the record count alone, since a compaction moves its frames. |
| D10 | Format 4 is a catalog object with segment records. A log without segments writes format 3. | The review asked for a distinct number, so fixtures and tests name their layout. |
| D11 | Several segments may be open at once. A writer may send a record to any open segment, or copies of it to several; the first copy in the log decides. | Installs can run ahead, so a rotation never waits for one. A copy in the next segment makes a commit at a segment's end one round trip. Copies are safe by the committed set and S2. |
| D12 | A segment's object is `<prefix>/<log id>/<name>`, the log's id in its UUID text form. No entry carries the id: every catalog object of the log holds it in its checkpoint, so an entry's name stays `seg-<ordinal>-<nonce>` (41 bytes). The sweep lists only `<prefix>/<log id>/`, and a creator that loses deletes its own segment 0. | Nothing stops the creators of two catalogs from being given one prefix. A sweep of the whole prefix took another log's live segments for its own unnamed blobs (any ordinal at or below its newest Install's), and that log lost every record after its checkpoint and could no longer be read: Bug 100 (sweep deleted another log's segments). The object's name is derived from what the catalog object already holds, as a page's is from its reference's node and digest, so it costs a record nothing. |
| D13 | The catalog object records the segments' prefix, `segment_prefix` after `checkpoint_through`, as the log's creator was given it, and every checkpoint restates it; when it equals the default, it is absent. The default is derived from the catalog object's own location: on Azure (`abfs[s]://<container>@<account>.dfs.<suffix>/<path>`, or `wasb[s]://` and `.blob.`) the Blob URL `https://<account>.blob.<suffix>/<container>/<path>.segments`, the same for every form of the location; elsewhere there is none, and the prefix is recorded in full. `fileio.catalog.log.location` is used only to create a log, and ignored, with a warning, when the object records another. A client that reads an object recording no prefix and derives no default fails. | The prefix was client configuration that every client had to repeat; one given another found no segment where it looked, and failed closed. Recorded, it needs no client to agree, and a client needs only credentials. The default keeps the common case to no byte more than before: a catalog object on Azure keeps its segments beside it, `.segments` a sibling of the object as `.committed/` is, never a path under it (on a hierarchical namespace the object is a file), and named so that no table location, `<warehouse>/<namespace>/<table>`, holds `seg-` names under a log's id. Explicit, it is one frame of 23 + n bytes for a prefix of n ≤ 125 bytes: its length, the record's tag and length, the prefix, the 20-byte trailer. A field beside `checkpoint_through` in its record would save the frame's 21 bytes but give `LogRecord` a field outside its oneof for one body. Since the default is derived, the rule is part of the format: a client that derived another from the same object would look elsewhere, so it maps every form of a location to one prefix, and derives none from a form it doesn't know. The model (`SegmentedLog.tla`) names segments by log, ordinal and nonce under one prefix every client shares; D13 makes that the format's guarantee rather than the configuration's, and the model is unchanged. |

## Code

| Module | Change |
|---|---|
| intentlog-core | `SegmentStore` SPI: create a segment under a name if absent; append a frame blind (returns the offset and the block count); read from an offset (bytes, length, blocks); delete; list; the limits (blocks, the largest object, the largest frame). Segment frames move here from intentlog-azure's `Frames`, with the seal frame in place of the pad. log.proto and `LogEnvelope` gain `InstallSegment` and `RetiredSegment`. `LogRuntime.write` lets a read-through's `LogConflictException` through as a lost race. |
| intentlog-fileio | `SegmentedLog`: the catalog object as an `AtomicFileLog` writing format 4, the merged view, positions and tails, appends and rotation, `readThrough`, creation, `putCheckpoint`, `trim`. `AtomicFileLog` reads format 4 when it has a segment store, and refuses it otherwise. |
| intentlog-memory | `InMemorySegmentStore`: limits in blocks and bytes, and the fault hooks of `InMemorySharedLog`. |
| intentlog-azure | `AppendBlobSegments`: a `SegmentStore` on `AppendBlobStore` (Append Block with no condition; `appendBlockCounted`, the reply's block count). `BlobUrlSegments`: segments named by their Blob URLs, each in the container its URL names, through a store opened for that container (D13). The log v1 (`AppendBlobLog`, `AzureLogHead`, its checkpoint blobs and pad) is removed; `AppendBlobStore` keeps its block-blob requests, which catalog-bench's timed store implements. intentlog-fileio's `SegmentedLogHistory` checks each checkpoint against a replay of the log's history. |
| iceberg-fileio-catalog | `AzureLogs` opens a `SegmentedLog`: the catalog object on the catalog's FileIO, segments where it says (D13), through `BlobUrlSegments` with each container signed by its account's key or signature; a log it creates at `fileio.catalog.log.location`, or the default `AzureLogs.defaultPrefix` derives. `LocalCatalogMode`'s `AZLOG*` modes run it over `InMemoryAtomicFileIO` and the in-memory Azure store. |
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
| SG6 | A named segment is deleted (by a checkpoint or the sweep) only after a checkpoint covers it wholly and its TTL has passed since it was retired, and its Retired entry is dropped only once it is deleted. The sweep deletes an unnamed blob only if it is its own log's and its ordinal is at most the newest Install's, so no Install in flight can name it, and no other log's catalog object does. | Retirement happens only in `putCheckpoint`, for segments before the position's; replay takes an Install only of the next ordinal; the sweep lists only its log's id (D12). |
| SG7 | P1–P6 as the log meets them. P4 is not claimed: appends are blind. | As stated. |
| SG8 | Every segment can end. Before its end it holds fewer than K frames, so a seal frame fits. | K is at most the store's block limit B, checked when the log opens, and the largest frame is at most a (B − 1)th of the store's largest object less a seal frame, so K − 1 of them and a seal fit whatever K a segment's installer set (Bug 115 (segment that cannot end)). |
| SG9 | A record applies at most once, at its first copy in the log. | The committed set skips a copy of a record that applied; S2 keeps a rejected one rejected. |
| SG10 | Every client of a log names its segments under one prefix, which never changes: the one its catalog object records, or the default. | The creator records its prefix unless it is the default; a checkpoint restates the object's; replay takes it only right after `checkpoint_through`; a client reads it before any request of a segment, and one that derives no default for an object recording none fails (D13). |

The mistakes, each a switch that must break a property. The model (`SegmentedLog.tla`) has
the first eleven, and the Java has each as a mutant:

| Switch | The mistake | Breaks |
|---|---|---|
| `catalogseal` | a catalog record states a segment's end from an earlier read (the review's `SealSegment`) | SG3, P1 |
| `limitatnext` | a segment's limit set by the next segment's Install | SG3 |
| `sealcount` | a seal frame ends its segment at a count from an earlier read, not where it lands | SG3 |
| `readahead` | a reader applies `m+1` before `m` has ended | SG1, SG2 |
| `voidack` | `readThrough` accepts a frame past its segment's end | SG3, an honest outcome |
| `createonmissing` | a writer that finds a segment missing creates it | SG3 |
| `sweepearly` | the sweep deletes a segment the catalog object names | SG4, P5 |
| `sweepinflight` | the sweep deletes an unnamed blob of the next ordinal | SG4: a delayed Install names a deleted segment, and the log stops |
| `sweepothers` | the sweep lists every log's segments at the prefix: Bug 100 (sweep deleted another log's segments) | SG6, and P1 and P5 for the other log: its named segments are deleted |
| `sdkretry` | a timed-out append is resent below the runtime | the shared-log model's `dup` |
| `overlimit` | an Install sets a limit above the store's block limit | SG8: a segment fills before its end, takes no seal frame, and the log stops |
| `nested` | replay of a segment takes an Install or a Retired entry in it | SG1, SG4 |
| `lastcopy` | `readThrough` decides by a later copy while an earlier one is in the log | an honest outcome, when a compaction drops the id between them (LOG.md's `dup`) |
| `kbound` | a writer sizes its largest record by its own K, not the store's block limit: Bug 115 (segment that cannot end) | SG8: a writer with a small K fills a segment another installed with a larger K before its end, with no room for a seal, and the log stops |

The last three are below the model's level: it has no frame bodies or sizes, and no
committed set.
Two mistakes can't happen here, and so have no switch. Replay taking an
Install of any ordinal (`ordinalgap`) needs an Install that lands on a catalog object it
didn't read, which the catalog object's condition refuses. Replay applying record frames in
the catalog object (`rootrecords`) needs a writer that appends one, and no writer does:
a client without a segment store refuses a format 4 object. Replay keeps both rules, each
with a unit test. A receipt's offset taken from a length read before the append
(`offsetfromlength`, v1's) is caught by the read-through's length and CRC check, as in v1.

## Testing

As LOG.md "Testing a provider", per stage:

- **The model first**, as with `SharedLogCommit.tla` and `AzureLog.tla`. The catalog
  object has conditional appends and CAS replace. Segments have blind appends, a block
  limit, record limits and seal frames. The model also has Installs ahead, the Install
  race, copies, appends delayed past an end, readers holding older catalog objects,
  compaction with positions, retirement with a TTL (as reader epochs), and the sweep. `check.py` holds each of the model's switches to its verdict. `catalogseal`'s
  trace is D1's example.
- **`SharedLogContract`** on `SegmentedLog` (`TestSegmentedLogContract`), over both
  in-memory stores.
- **`TestSegmentedCommitProtocol`** (intentlog-fileio): the testkit's `CommitProtocol`
  fuzzer on `SegmentedLog` over `InMemoryAtomicFileIO` and `InMemorySegmentStore`. It
  draws:
  - faults on every request of both stores;
  - small limits and block limits, Installs ahead, early seals, and copies;
  - appends delayed until after an end;
  - one to three instances with stale catalog objects, each given the prefix, another, or none,
    with or without a default (D13: the catalog object's prefix must never move);
  - sweeps at TTL 0;
  - another log at the same prefix of the same store, which must read its records at the
    drain.

  At the drain, every replica a read returned must read on to the end (as
  `staleReaders` did for the log v1). It draws the segments' store: in memory, or
  `AppendBlobSegments` on `InMemoryAppendBlobStore`, whose replies say the block count or
  don't.
- **Mutants** (`fuzz/mutants/segments.sh`): each switch above, put in the code, fails
  the fuzzer; a checkpoint that drops an expired Retired entry without deleting its
  segment (`dropretired`, a leak), a log that ignores an entry's location
  (`nolocation`) and a creator that loses and keeps its segment 0 (`creationleak`, a leak)
  fail `TestSegmentedLog`.
- **Catalog suites**: `TestLocalCatalog*` in the `AZLOG*` modes. Also
  `TestCatalogFaultProperties` and `TestCatalogRaceProperties`, whose Append Blob draws
  open the new log, and `TestAzureLogs`: catalogs configured with another location, or none,
  share their creator's log; one of a catalog object on Azure records no prefix (D13).
- **Real account**: `AzureLogCatalogIT`, with the warehouse (the catalog object) on
  the account through the ADLS FileIO.

## Plan

Each stage is one or more commits on branch `log-segments`, each fix with its test.

| Stage | Delivers | Gate before the next |
|---|---|---|
| 0. Model | `SegmentedLog.tla` and its configurations | `check.py`: every configuration at its expected verdict; `catalogseal` and `limitatnext` fail SG3 |
| 1. Wire and SPI | log.proto, `LogEnvelope` codecs, `SegmentStore`, segment frames with the seal frame, `InMemorySegmentStore`, the format explorer's decoder and fixtures | codec round trips, `TestFormatExplorer`; `mvn install` green with nothing else changed |
| 2. `SegmentedLog` | reads, appends with limits, rotation past the end, the Install race, seal frames, `readThrough` with void appends, creation, `putCheckpoint`, the sweep; the runtime's lost-race change (its own commit and test); and stage 3's Installs ahead and copies, which it needed | contract, regressions, `TestSegmentedCommitProtocol` at 2,000 tries, every mutant caught |
| 4. Azure and the catalog | `AppendBlobSegments` (the fuzzer draws Append Blobs, with and without block counts in replies), `AzureLogs`, format 4 refusal, the `AZLOG*` modes, catalog fuzzers' draws; v1's head, checkpoint blobs and pad removed; docs describe the new log | `mvn install` (every suite), fault and race properties locally, mutants; then merge to main |
| 5. Cloud and havoc | `AzureLogCatalogIT` on the real account; a havoc round (`azure` at 1,000,000 tries; fault and race) | the user's go for each; failures fixed forward |

## Configuration

| Property | Default | Meaning |
|---|---|---|
| `fileio.catalog.log` | `atomic-file` | `azure-append`: a segmented log on Azure Append Blobs |
| `fileio.catalog.log.location` | the default (D13) | where a log the catalog creates keeps its segments, a Blob URL; each log's are under `<prefix>/<log id>/` (D12). The catalog object records it unless it is the default, beside the object; a client given another uses the object's. A catalog object elsewhere than Azure has no default, and needs it to be created |
| `fileio.catalog.log.segment.records` | the store's block limit (provisional) | K, the limit each Install sets. At most the store's block limit (SG8); the largest record does not depend on it |
| `fileio.catalog.log.copies` | false | whether an append near its segment's end also sends a copy to the next (D11) |
| `fileio.catalog.log.checkpoint.share`, `.min.bytes`, `.records` | 1.0, 1 MiB, 10,000 | when to compact |
| `fileio.catalog.log.retire.ttl.ms` | 60,000 | how long a retired segment stays before it is deleted |

The conditional ablation on segments is [Later](#later).

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

## Progress

Built on branch `log-segments` (2026-11-15): stages 0 to 4, stage 3 within stage 2. D13 (the
prefix in the catalog object) on branch `segment-location` (2026-11-17).
catalog-bench's `sut-fileio` opens the segmented log too (its branch `log-segments`). Locally,
the fuzzer passes 2,000 tries on four seeds; of `fuzz/mutants/segments.sh` twelve mutants are
caught and `fastvoid` is equivalent (2026-11-17, with D12's `sweepothers` and `creationleak`); the catalog's suites (the `AZLOG` modes among them) and the
fault and race properties pass. `AzureLogCatalogIT` passed on the real account with a SAS that
cannot list (450 tests). A havoc round is next.

## Open questions

1. K's default. The store's block limit makes the fewest rotations. A smaller K bounds
   how much a segment holds until a checkpoint passes its end. Should K come from
   recent commit rates or a service instead?
2. `SegmentedLogHistory` checks each checkpoint against a replay of the log's history,
   every segment's frames in memory, as the `AZLOG*` modes did before. Sample them
   instead, for long runs?
