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
- **Installs.** Records in the catalog object install segments, one at a time, in a
  total order.
- **Ends.** A segment ends at a seal frame inside it, at a record count its Install
  set, or at the store's append limit.
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
// Installs segment `ordinal` as the log's newest. Replay of the catalog object takes
// it only if `ordinal` is the last installed one plus 1, or, as the first Install
// after a checkpoint, the ordinal of the checkpoint's position.
message InstallSegment {
  uint64 ordinal = 1;
  string name = 2;      // under the log's prefix, with its nonce
  string location = 3;  // optional: a URL in place of prefix + name
  uint64 seal_at = 4;   // optional: the segment ends after this many records (D2)
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
seal    = %x00 trailer          ; ends the segment: every frame after it is void
trailer = ordinal crc           ; the segment's ordinal (u64le), CRC32C (u32le) of all before
```

A seal frame is 13 bytes. Writers append records with v1's size bound: the limit L
less 32 bytes. Seal frames are appended with the bound at L, so one always fits until
the block limit, which is itself an end.

**Positions** are the end of the last record read, or a checkpoint's position when no
record follows it:

```abnf
position = ordinal records offset ; ordinal u64be, records u64be, then the store's bytes
```

`records` counts the segment's records before this point. `offset` is the store's
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
   after it give Installs `m0..t` and the Retired list.
2. Read each segment from `m0` (from `c`'s offset) to `t`. For each one:
   - check every frame's trailer: its ordinal and CRC;
   - stop at the segment's end: its first seal frame, its `seal_at`-th record, or the
     store's block limit (50,000 on Azure). The byte bound never ends a segment:
     records stop 32 bytes short of L, which a Full turns into a rotation and a seal
     frame;
   - if it has not ended, stop there: no later segment can be ordered yet.
3. Return the checkpoint and the records read, with `start()` = `c` and the tail at the
   last record's position.

`readAfter(tail)` reads the tail's segment from the tail's offset. It reads the
catalog object only if that segment has ended, to find the next Install. A steady-state
refresh is one GET. A whole read is two (v1 needed three: head, checkpoint, segment).
A segment that is gone (swept) is `TrimmedException`, and the runtime reads afresh.

Replay of the catalog object discards any record frame other than an Install or a
Retired after the first Install. That is the review's rule, "records in the catalog
object while a segment is open are void". Letting records live in the catalog object
again is [Later](#later) (D9).

## Writing

`append(record)` frames the record for the newest segment this instance knows, then
appends it blind under the size bound L − 32 (Append Block with
`x-ms-blob-condition-maxsize` on Azure). On success the receipt is the segment's
ordinal and name, the offset from the reply, and the frame's length and CRC, as in
v1.

- **Full** (the size bound, or the block limit) rotates, then sends to the new segment:
  1. Read the catalog object. If a newer Install landed, use that segment.
  2. Otherwise create segment `m+1` under a fresh name, then append
     `InstallSegment(m+1)` to the catalog object with format 3's condition (its version
     and offset as read; on a store without appends, a CAS replace). A loser reads
     again, uses the winner's `m+1`, and leaves its own blob to the sweep.
  3. Append a seal frame to `m`, unless `m` has already ended.
- **NotFound** (the segment was swept) re-reads the catalog object and sends to its
  newest segment.
- The loop is bounded by attempts. Past the bound it fails with `LogNotSentException`,
  since nothing landed. That fixes v1's OI-6.

Concurrent Installs resolve by the catalog object's condition. Its replay also rejects
any ordinal but the next, so each Install adds exactly one segment. Installing several
in one write is [Later](#later). A checkpoint or another Install landing in between
also makes an Install retry; that costs one read.

**Ending a segment.** Two policies, chosen by `fileio.catalog.log.seal`:

- **`eager`** (default): rotation is triggered by Full, and the writer that installs
  `m+1` seals `m` with a seal frame (step 3 above).
- **`count`**: each Install sets `seal_at` = N for its own segment. A segment ends at
  its Nth record, with no write. A writer that sees `m` at N records, or whose append
  lands past it (void), installs `m+1`. Choosing N (from recent transaction timestamps,
  or by a service) is open: errata D26 (seal-at-count tuning).

Under both policies, a writer whose record is in `m+1` and finds `m` not yet ended
appends a seal frame to `m`. That covers a crash between Install and seal, and a count
that traffic hasn't reached, with no owner: any writer that needs `m` ended ends it. A
seal frame after the first is void and harmless.

## A commit's outcome

`readThrough(receipt)` for a record in segment `m`:

1. Read through `m`, ending (with a seal frame) any earlier segment that has not
   ended.
2. Find the receipt's frame at its offset, and check its length and CRC.
3. If `m` ended before that offset, the append is void. It is not in the log, and no
   later write can put it there. Throw `LogConflictException`.
4. Otherwise return the view through the frame. Its records include the receipt's
   record (P3), and replay says whether it applied.

A receipt in a retired segment is void if it lies past the Retired entry's `end`.
Otherwise it is `TrimmedException`, as today.

Once a reader has read a record, no later write voids it. A seal frame lands after it
in the store's order, and a count is fixed before the segment takes any record. That is
the property D1 restores.

## Compaction, retirement and the sweep

The runtime's separate-checkpoint compaction (`LogRuntime.compact`) runs unchanged:
GC record, read through it, PageOuts, `putCheckpoint`, `trim`.

- **`putCheckpoint(through, checkpoint)`** reads the catalog object. If its
  checkpoint's position is at or after `through`, it is done: every checkpoint at one
  position is the same (I11). Otherwise it CAS-replaces the object with:
  - the header (format 4);
  - the checkpoint frame;
  - an `InstallSegment` for each segment from `through`'s ordinal on, as the object
    names them (with each one's `seal_at`);
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
the first checkpoint (position `(0, 0, 0)`) and `InstallSegment(0)`. A creator that
loses finds the log exists (`LogConflictException`); its segment is the sweep's.

**Finding the log.** A client finds the catalog at `<warehouse>/catalog`, as with
format 3. A client opened without a segment store that finds a format 4 object refuses
it: "this catalog keeps segments". It never appends to it. That closes OI-1.

## Decisions beyond the review

| | Decision | Why |
|---|---|---|
| D1 | A segment's end is fixed in its own order: its first seal frame, its `seal_at`-th record, or the store's limit. There is no `SealSegment` record in the catalog object. The review's "seal frame as an optional hint" becomes the authority. | A catalog record of an end written after a read can void a record a reader has already applied. Example: reader R reads frames 1–5 of `m` and applies them. Writer W read `m` at 3 frames and appends `SealSegment(m, end of 3)`. Frames 4–5 are now void. R disagrees with every later reader (P1), and frame 4's writer may have reported success. Nothing a writer can read bounds what other readers have seen, so the end must come from the store's order, which every reader sees alike. |
| D2 | `seal_at` is set by a segment's own Install, not by the next one's. | Same example, with N = 3 set by `InstallSegment(m+1)`. |
| D3 | A shortfall under `count` is closed by one seal frame, not by no-op records. | One blind append instead of up to N. No record type is added. |
| D4 | The log presents blind appends and separate checkpoints to the runtime, as `AppendBlobLog` does. `putCheckpoint` is the CAS replace; `trim` is the sweep. A void append is a `LogConflictException` from `readThrough`, which `LogRuntime.write` lets through as a lost race. | The runtime's separate-checkpoint path is built and fuzzed. Today `write` wraps every read-through failure as ambiguous; that path is still correct, but it costs a read and reports the wrong kind. |
| D5 | Positions are `(ordinal, records, store offset)`, the end of the last record read, carried in `LogCheckpoint.through`. | The record count is what `seal_at` counts, from a reader that starts mid-segment. Never normalizing positions keeps one encoding for one place. |
| D6 | The checkpoint's segment table is records after the checkpoint frame, in the same replace: Installs from the position's segment on, then Retired entries. | The runtime's `LogCheckpoint` is unchanged, and replay of the catalog object has one rule for Installs. |
| D7 | The code is a new `SharedLog`, `SegmentedLog` (intentlog-fileio), over an `AtomicFileLog` for the catalog object and a `SegmentStore` SPI (intentlog-core). intentlog-azure becomes a segment store. | The catalog object is format 3's object, so its code is reused, not copied. |
| D8 | Segment names carry a nonce, and only an installer creates a segment, before its Install. | v1's AZ4. A stale writer can't bring back a swept segment, and a reader of an older catalog object can't read a newer blob under an old name. |
| D9 | Records in the catalog object again (the review's "after the tail is sealed and no newer one is installed") are a segment too: an Install that names the catalog object. Built later. | It keeps ordinals contiguous and positions uniform, and the run ends in the object's own order (the next Install). Positions in it are the record count alone, since a compaction moves its frames. |
| D10 | Format 4 is a catalog object with segment records. A log without segments writes format 3. | The review asked for a distinct number, so fixtures and tests name their layout. |

## Code

| Module | Change |
|---|---|
| intentlog-core | `SegmentStore` SPI: create a segment under a name if absent; append a frame blind under a size bound (returns the offset); read from an offset (bytes, length, blocks); delete; list; the limits (blocks, the largest frame). Segment frames move here from intentlog-azure's `Frames`, with the seal frame in place of the pad. log.proto and `LogEnvelope` gain `InstallSegment` and `RetiredSegment`. `LogRuntime.write` lets a read-through's `LogConflictException` through as a lost race. |
| intentlog-fileio | `SegmentedLog`: the catalog object as an `AtomicFileLog` writing format 4, the merged view, positions and tails, appends and rotation, `readThrough`, creation, `putCheckpoint`, `trim`. `AtomicFileLog` reads format 4 when it has a segment store, and refuses it otherwise. |
| intentlog-memory | `InMemorySegmentStore`: limits in bytes, blocks and records, and the fault hooks of `InMemorySharedLog`. |
| intentlog-azure | `AppendBlobSegments`: a `SegmentStore` on `AppendBlobStore` (Append Block with the size bound, nonce names). `AppendBlobLog`, `AzureLogHead` and the checkpoint blobs are removed. So are `AppendBlobStore`'s block-blob requests, if nothing else uses them. `AzureLogHistory` becomes a check of each checkpoint against a replay of the segments' history. |
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
| SG1 | The log is the catalog object's checkpoint, then the records of its segments in ordinal order, each up to its end. Every reader of a segment reads the same frames, in the same order, to the same end. | Append Block is atomic and reads are strongly consistent; ends are seal frames, counts or limits, all in the store's order (D1). |
| SG2 | A reader reads segment `m+1` only after reading `m` to its end, so what it returns is a prefix of the log (P1). | Reading, step 2. |
| SG3 | No write voids a record a read has returned. | A seal frame lands after it; `seal_at` is fixed before the segment takes records (D1, D2). |
| SG4 | Ordinals are contiguous from the checkpoint's. Each Install names a segment its installer created under a fresh name before sending it. | The catalog object's condition, replay's ordinal rule, nonce names (D8). |
| SG5 | After the first Install, no record frame in the catalog object is in the log. | Replay of the catalog object. |
| SG6 | The sweep deletes a named segment only after a checkpoint covers it wholly and its TTL has passed since it was retired. It deletes an unnamed blob only once its ordinal is at most the newest Install's, so no Install in flight can name it. | Retirement happens only in `putCheckpoint`, for segments before the position's; replay takes an Install only of the next ordinal. |
| SG7 | P1–P6 as the log meets them. P4 is not claimed: appends are blind. | As stated. |

The model's mistakes, each a switch that must break a property:

| Switch | The mistake | Breaks |
|---|---|---|
| `catalogseal` | a catalog record states a segment's end from an earlier read (the review's `SealSegment`) | SG3, P1 |
| `sealatnext` | `seal_at` set by the next segment's Install | SG3 |
| `readahead` | a reader applies `m+1` before `m` has ended | SG1, SG2 |
| `voidack` | `readThrough` accepts a frame past its segment's end | SG3, an honest outcome |
| `createonmissing` | a writer that finds a segment missing creates it | SG4, then SG3 |
| `ordinalgap` | replay takes an Install of any ordinal | SG1 |
| `rootrecords` | replay applies record frames in the catalog object after an Install | SG5, P1 |
| `sweepearly` | the sweep deletes a segment the checkpoint doesn't cover | P5 (`Readable`) |
| `sweepinflight` | the sweep deletes an unnamed blob of the next ordinal | SG4: a delayed Install names a deleted segment, and the log stops |
| `offsetfromlength` | the receipt's offset is the length before the append, not the reply's | an honest outcome, as in v1 |
| `sdkretry` | a timed-out append is resent below the runtime | the shared-log model's `dup` |

## Testing

As LOG.md "Testing a provider", per stage:

- **The model first**, as with `SharedLogCommit.tla` and `AzureLog.tla`. The catalog
  object has conditional appends and CAS replace. Segments have blind appends, a byte
  bound with its reserve, a block limit, seal frames and counts. The model also has
  both policies, the Install race, appends delayed past an end, readers holding older
  catalog objects, compaction with positions, retirement with a TTL (as reader epochs),
  and the sweep. `check.py` holds every switch above to its verdict. `catalogseal`'s
  trace is D1's example.
- **`SharedLogContract`** on `SegmentedLog` (`TestSegmentedLogContract`), over both
  in-memory stores.
- **`TestSegmentedCommitProtocol`** (intentlog-fileio): the testkit's `CommitProtocol`
  fuzzer on `SegmentedLog` over `InMemoryAtomicFileIO` and `InMemorySegmentStore`. It
  draws:
  - faults on every request of both stores;
  - small segments (bytes, blocks, counts) and both policies;
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
| 0. Model | `SegmentedLog.tla` and its configurations | `check.py`: every configuration at its expected verdict; `catalogseal` and `sealatnext` fail SG3 |
| 1. Wire and SPI | log.proto, `LogEnvelope` codecs, `SegmentStore`, segment frames with the seal frame, `InMemorySegmentStore`, the format explorer's decoder and fixtures | codec round trips, `TestFormatExplorer`; `mvn install` green with nothing else changed |
| 2. `SegmentedLog`, eager | reads, appends, rotation, the Install race, `readThrough` with void appends, creation, `putCheckpoint`, the sweep; the runtime's lost-race change (its own commit and test) | contract, regressions, `TestSegmentedCommitProtocol` at 2,000 tries, every mutant caught |
| 3. Seal at a count | `seal_at`, rotation by count, shortfall seal frames; the fuzzer draws the policy and N; errata D26 (seal-at-count tuning) | as stage 2 |
| 4. Azure and the catalog | `AppendBlobSegments`, `TestAzureCommitProtocol`, `AzureLogs`, format 4 refusal, the `AZLOG*` modes, catalog fuzzers' draws; v1's head, checkpoint blobs and pad removed; docs describe the new log | `mvn install` (every suite), fault and race properties locally, mutants; then merge to main |
| 5. Cloud and havoc | `AzureLogCatalogIT` on the real account; a havoc round (`azure` at 1,000,000 tries; fault and race) | the user's go for each; failures fixed forward |

## Configuration

| Property | Default | Meaning |
|---|---|---|
| `fileio.catalog.log` | `atomic-file` | `azure-append`: a segmented log on Azure Append Blobs |
| `fileio.catalog.log.location` | none: required | the segments' prefix, a Blob URL |
| `fileio.catalog.log.segment.max.bytes` | 64 MiB (provisional) | L. It must exceed the largest record's frame plus 32 bytes |
| `fileio.catalog.log.seal` | `eager` | `eager` or `count` |
| `fileio.catalog.log.seal.count` | provisional (D26) | N under `count` |
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

## Open questions

1. D1 and D2 change the review's sealing: the authority moves into the segment, and
   `seal_at` moves to a segment's own Install. Agreed?
2. Should the eager policy rotate only on Full, or also on a record count or an age,
   to bound how much one segment holds?
3. `AzureLogHistory` checks each checkpoint against a replay of the log's history.
   Does its replacement keep every segment's frames in memory in the `AZLOG*` modes,
   as now, or sample them?
