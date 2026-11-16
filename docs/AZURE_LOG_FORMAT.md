# The Azure log: format and protocol

The segmented log on Azure Append Blobs, specified from the code for review: what it stores, in
what bytes, and the requests each operation makes. [LOG_SEGMENTS.md](LOG_SEGMENTS.md) is its
design (the decisions D1–D11 and invariants SG1–SG9 cited here are its), and
[AZURE_LOG.md](AZURE_LOG.md) is why it is built on Azure this way.

## 1. Scope

- **Code:** `SegmentedLog` (intentlog-fileio), over `AtomicFileLog.segmented` for the catalog
  object; `org.intentlog.segments` (intentlog-core: `SegmentStore`, `SegmentFrames`,
  `SegmentPosition`); `AppendBlobSegments`, `AzureAppendBlobStore` and `InMemoryAppendBlobStore`
  (intentlog-azure); `AzureLogs` (iceberg-fileio-catalog).
- **Schema of record:** intentlog-core's `log.proto` (`LogCheckpoint`, `LogRecord` with
  `InstallSegment`, `RetiredSegment` and `checkpoint_through`).
- **Not here:** the runtime above the SPI (LOG.md), and the catalog's payloads (SPEC.md).

## 2. Objects

| Object | Where | Written by |
|---|---|---|
| Catalog object | the catalog's FileIO, `<warehouse>/catalog` | an Install: a conditional append; a checkpoint: a CAS replace |
| Segment | `<prefix>/seg-<ordinal %020d>-<nonce %016x>`, an Append Blob, or the location (a blob URL in the container) its entry names | Append Block with no condition |
| Page | the FileIO, `<warehouse>/catalog.committed/` | as the one-object log's (COMMITTED_PAGING.md) |

`<prefix>` is the path of `fileio.catalog.log.location` in its container. A segment's name
carries a fresh nonce, drawn until a create-if-absent takes it, so a stale writer never
reaches a newer blob under an old name (D8). The log writes no location; a migration that moves
the prefix writes one into each entry of a segment it leaves at the old one, and every request
for that segment, its deletion among them, goes there (`AppendBlobStore.nameAt`, the URL's path
in the container, decoded).

## 3. Wire formats

### 3.1 The catalog object (format 4)

Format 3's object ([ATOMIC_FILE_LOG.md](ATOMIC_FILE_LOG.md)) under format number 4: the magic
`LCAT`, the version 4 (u32be), then frames, each a varint length, a body, and the 20-byte trailer
(the object's incarnation, the frame's offset, CRC32C). The frames are, in order:

1. the checkpoint: a `LogCheckpoint`, opaque to the log (the runtime's carry `through`, the
   position below, which it checks against the next frame's);
2. a `LogRecord` with `checkpoint_through`: the checkpoint's position (3.3);
3. `LogRecord`s with `install_segment`: segments `c, c+1, …, t` from the checkpoint's segment
   `c` on, each with its name, its location (if any) and its record limit K;
4. `LogRecord`s with `retired_segment`: segments a checkpoint covers wholly, each with its
   Install's name and location, its end position and when it was retired.

Installs appended later follow. Replay of the object takes an Install only of the next ordinal,
and ignores every other record frame but a Retired entry. A log opened without a segment store
refuses a format 4 object ("this catalog keeps segments"), and a segmented log refuses format 3.

### 3.2 Segment frames

```abnf
segment = *frame
frame   = record / seal
record  = varint body trailer   ; varint length n > 0, then a LogRecord's n octets
seal    = %x00 trailer          ; 13 bytes: ends the segment, taking the rest of its limit
trailer = ordinal crc           ; the segment's ordinal (u64le), CRC32C (u32le) of all before
```

Each frame is one block. A segment ends at its Kth record or at its first seal frame, whichever
comes first; every frame after the end is void. A record whose body is a provider record (an
Install, a Retired entry, a checkpoint position) is malformed: replay refuses it.

### 3.3 Positions

```abnf
position = ordinal records offset ; each a u64be: 24 bytes
```

The end of the last record read: its segment, the segment's records before that point, and the
byte offset after the record's frame. A checkpoint's position is where it was taken. Positions
order by (ordinal, records), and are never moved to the start of the next segment, so two tails
at one place have equal bytes.

### 3.4 Receipts and views

A receipt lists the append's copies in ordinal order, each `(ordinal, name, offset, index,
length, crc)`, and the record. The index is the reply's committed block count (0 or less: the
store can't say). A view is a checkpoint (or none: records after a tail), its records, and where
each starts; `start()` is the checkpoint's position or the tail the records follow.

## 4. The store's requests

| Operation | Requests |
|---|---|
| `read()` | the catalog object (a GET, after the one held: a tail read); each segment from the checkpoint's position (ranged GETs) |
| `readAfter(tail)` | the tail's segment from its offset; the catalog object only for an Install the instance doesn't know |
| `append` | Append Block; on a void reply or a full segment, the next segment's; an Install when none is next |
| install | Put Blob (Append Blob) if absent; the catalog object's conditional append |
| `readThrough` | none, if the reply put the copy at the kept view's end; else ranged GETs; a seal frame (Append Block) for each earlier segment that hasn't ended |
| `putCheckpoint` | the catalog object (a GET); Delete Blob for each Retired entry past its TTL; its CAS replace |
| `trim` (the sweep) | the catalog object (a GET); Delete Blob for each Retired entry past its TTL; List Blobs under the prefix; Delete Blob for each unnamed blob at or below the newest Install's ordinal |

## 5. Algorithms

LOG_SEGMENTS.md "Reading", "Writing", "A commit's outcome" and "Compaction, retirement and the
sweep" are the algorithms; the code follows them, with these particulars:
- **An append's target** is the first segment this instance knows open: from the newest view's
  tail on, past segments it saw end. After two ended segments (void replies, full), or after a
  read-through found an append void, it reads the catalog object and goes to the newest. It
  tries `sendAttempts` (8) segments, then fails unsent (`LogNotSentException`).
- **Installs ahead:** an append whose index is within `max(1, K/100)` of K installs the next
  segment if none is installed, ignoring a failure; with `copies`, it also sends a copy there,
  and a copy whose outcome the store can't say makes the append's outcome unknown.
- **A read-through** takes the receipt's copies in order. A copy the checkpoint covers is void
  if its retired segment's end lies at or before it, else trimmed. Each other copy is read
  through: an earlier segment that hasn't ended is sealed and read again; the copy is the record
  if its segment hadn't ended before it (its length and CRC checked), else void. Every copy void
  is `LogVoidException`.
- **A checkpoint** restates the Installs from its position's segment on and retires those
  before, each with its end (as read, or read now). It keeps the older Retired entries, but
  those past their TTL whose segments it deleted (where the entry locates them): an entry is
  dropped only once its segment is gone. A checkpoint at the object's own position with other
  bytes is refused (I11).
- **A trim** first checks that the catalog object's checkpoint covers it (P5), then sweeps.
- **Creation** creates segment 0, then the catalog object if absent: the checkpoint, position
  `(0, 0, 0)`, Install 0. A catalog object with the same checkpoint is this creation, retried;
  another is another writer's log (`LogConflictException`).

## 6. Invariants

LOG_SEGMENTS.md "Invariants" (SG1–SG9), and P1–P6 of LOG.md but P4 (appends are blind). The
code checks some at run time: a segment that holds the store's block limit without having
ended fails a read (SG8), a catalog object that names a segment the store lacks fails an append
(SG4), and replay refuses a provider record in a view.

## 7. Failure mapping

AZURE_LOG.md "Failures".

## 8. Versioning and limits

- **Formats:** the catalog object's format number 4; a segment frame has no version of its own
  (a change to it is a new format number for the catalog object).
- **K** is at most the store's block limit (50,000), and K − 1 of the largest frame and a seal
  frame fit a blob (50,000 × 4 MiB), so a segment can always end (SG8). The largest record is
  therefore at most the store's block and about 4 MiB at K = 50,000; a smaller K allows larger
  records.

## 9. Open issues

| | Issue | Where |
|---|---|---|
| OI-2 | **A reader trusts that a ranged GET's body runs to the reported length.** The walk parses the bytes returned and takes the segment's length from the reply; nothing checks `from + bytes == length`. If the service returned fewer bytes with the full total, the reader would end its view early (a prefix, so P1 holds), not skip frames. AZURE_LOG.md's Q2 asks this of Azure. | `SegmentedLog.walk`, `AzureAppendBlobStore.read` |
| OI-5 | **The sweep runs only inside a trim.** An idle log keeps unnamed and retired segments until its next compaction. | `SegmentedLog.trim` |
| OI-7 | **`readThrough` retries without a pause** until `wait` has passed and `readAttempts` were made. | `SegmentedLog.readThrough` |
| OI-10 | **A blob of a higher ordinal than the newest Install's is never swept**: a creation abandoned before its Install, until that ordinal is installed, or another log's at the same prefix. | `SegmentedLog.sweep` |
| OI-13 | **Only account-key credentials** reach the segments from a catalog. | `AzureLogs.open` |
| OI-15 | **A catalog object that takes no more appends** (its store's append limit) takes no Install: once the newest segment has ended, appends fail unsent until a checkpoint replaces the object, which needs its GC record to land. Real stores' limits (ADLS: 50,000 appends) are far above an interval's Installs. | `SegmentedLog.install` |
| OI-16 | **An instance's memory of segments grows**: the names it saw end, and their ends, are kept for the instance's life. | `SegmentedLog.skip`, `ends` |
| OI-17 | **Unnamed blobs need a listing.** The sweep lists the prefix on every trim to find the blobs no catalog object names: an installer's that lost and failed to delete it, an abandoned creation's. They are empty. A SAS without container scope cannot list, and they stay; retired segments are deleted by name regardless. | `SegmentedLog.sweep` |
| OI-18 | **A location must be in the log's container.** An `AppendBlobStore` is bound to one container, so a migration to another container or account would need a store per location; an entry locating a segment elsewhere fails the read. | `AzureAppendBlobStore.nameAt` |
