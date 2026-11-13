# The Azure append-blob log

**Status:** a specification, written on 2026-11-10, and its implementation:
- **The fence.** The same day, probes found that Append Blob Seal doesn't exist on an account
  with a hierarchical namespace, and that a lease doesn't stop appends there. So a segment is
  closed by filling it to its size limit instead
  ([Closing a segment](#closing-a-segment-seal-and-leases-dont-work-filling-does)).
- **Done** ([Testing](#testing), steps 1–3 and 5):
  - the model, `spec/tla/AzureLog.tla`;
  - the module `intentlog-azure`: `AppendBlobLog`, the in-memory store and `AzureAppendBlobStore`;
  - the contract;
  - the commit-protocol fuzzer, calibrated by mutants;
  - both again on the spike's account.
- **Step 4, in part** (2026-11-10): `fileio.catalog.log` and the log's properties
  (`LogCatalogProperties`, `AzureLogs`; `FileIOCatalog` opens it on a real account with an
  account key), and the `AZLOG` modes of `TestLocalCatalog` and `TestLocalCatalogTransaction`:
  Iceberg's catalog suites on this log, every checkpoint checked against a replay of the log's
  history (`AzureLogHistory`).
- **Not done:**
  - step 4's fuzzers: `TestCatalogFaultProperties` and `TestCatalogRaceProperties` drawing
    this log (the compaction rule is built: [When to compact](#when-to-compact));
  - the measurements (catalog-bench docs/DESIGN.md, "The paper's configurations").

`AppendBlobLog` is a `SharedLog` ([LOG.md](LOG.md)) kept in Azure Blob Storage, and its
checkpoints are stored apart from its records. Writers append their records to an Append
Blob **without a position condition** (blind appends). The service picks where each record
lands and says so in its reply. A small head object, changed only by compare-and-swap (CAS),
names the log's segments and its newest checkpoint.

So no writer ever loses a race to append. A commit is rejected only when replay finds that
its intention no longer holds where its record landed. That is the case the conflict
encodings exist for ([SPEC.md](SPEC.md) "Conflict Encodings").

The runtime already has everything this log needs:
- the commit loop's branch for logs without conditional appends;
- `compact()` for logs whose checkpoints are stored apart;
- the three rules for unconditional appends (LOG.md "Unconditional appends");
- the shared-log model that found them (`SharedLogCommit.tla`, configurations `log-*`);
- `InMemorySharedLog` in its unconditional mode, under the commit-protocol fuzzer.

What this document adds is a provider on a real store: its objects, the format of each, how
each SPI operation maps onto Azure's requests, how the log moves from one blob to the next,
and the invariants a new model and the fuzzers must check.

## Why: the measurement

For a year the catalog took it as given that several writers can share one appendable
object only by conditioning each append on the length they last saw. That is true of the
one-object log ([ATOMIC_FILE_LOG.md](ATOMIC_FILE_LOG.md)), because a CAS compaction replaces
the object, and of S3 Express, whose append must name the object's current size
(`x-amz-write-offset-bytes`; any other value fails with `InvalidWriteOffset`). It is not true
of an Azure Append Blob. Append Block's position condition (`x-ms-blob-condition-appendpos`)
is optional, and its reply names the offset where the block landed
(`x-ms-blob-append-offset`).

`AzureAppendSpike` (catalog-bench's `spikes` module, docs/AZURE_APPENDS.md; written in rmow-bench,
whose VM it ran on) measured both kinds of append on 2026-11-10:
- **Setup:** one D8as_v6 VM in westus, beside an account with a hierarchical namespace;
  4 KiB blocks; a new blob for each level; each level 2 s of warm-up and then 20 s measured.
- **Retries and connections:** the SDK's retries were off and every writer had its own
  connection.
- **The check:** after each level the blob was read back. Every acknowledged block had to be
  intact (CRC32C), appear once, and sit at the offset its reply named.
- **Where:** the run is catalog-bench's `results/spike/azure-appends-20261110T192545Z` (moved
  from rmow-bench on 2026-11-11), built
  at ee7f046.

| Writers | Blind appends landed/s | Conditional appends landed/s | Conditional appends that lost |
|---:|---:|---:|---:|
| 1 | 94 | 96 | 0% |
| 2 | 192 | 100 | 38% |
| 4 | 341 | 101 | 65% |
| 8 | 640 | 98 | 81% |
| 16 | 1,178 | 98 | 90% |
| 32 | 2,043 | 99 | 95% |
| 64 | 3,113 | 95 | 97% |

The 64-writer blind level filled its blob: it reached the 50,000-block limit 14.1 s into
its 20 s window. Its rate here is over the 14.1 s it ran. The spike's own `levels.jsonl`
says 2,199, because it divided by the full 20 s; that's fixed for later runs. The table is
computed from the run's requests in catalog-bench's lab (`append_requests`).

What the run showed:
- **Conditional appends** land at about one writer's rate (a 10 ms median append), however
  many writers there are. A lost race costs a lost append (8–11 ms median) and a HEAD
  (7 ms), not the 2 s of catalog-bench's `azure` profile.
- **Blind appends** scale with the writers up to 64, with no throttling, no errors and no
  lost appends. Their median latency rose from 10 ms to 16 ms, and their p99 reached 99 ms
  at 64 writers. The client's CPU share stayed at or below 7.4%, so the VM was not the
  limit.
- **The check passed at every level.**
- **One blob is not enough:** at 64 writers a blob fills in about 16 s, which is the case
  for moving to a new segment.

The limits of that run, which the spikes in [Open questions](#open-questions) address:
- one run of 20-second levels, the last cut short by its full blob;
- every writer on one VM;
- 4 KiB blocks only;
- one account, which has a hierarchical namespace;
- no reads while appends landed;
- no moving from one blob to the next. Closing a blob was probed separately (next section).

### Closing a segment: Seal and leases don't work, filling does

A log of several blobs must be able to close one: once the head names the next segment, no
append may land in the one before it. `AzureSealProbe` (catalog-bench's `spikes` module) tried
three ways on 2026-11-10. It ran from the laptop against the same account, and against Azurite
3.37.0 (catalog-bench's `results/spike/azure-seal-20261110T224335Z`, `azure-lease-20261110T224617Z`,
`azure-fill-20261110T224935Z` and `azure-fill-paced-20261110T225107Z`):

| Fence | Azurite 3.37.0 | The account (hierarchical namespace) |
|---|---|---|
| Append Blob Seal | works as documented: appends after it fail with `409 BlobIsSealed` | doesn't exist. Seal fails with `400 InvalidQueryParameterValue` (`comp=seal`) at service versions 2019-12-12, 2021-12-02 and 2025-05-05, and appends after it land |
| An infinite lease, whose id no appender sends | appends without the id fail with `412 LeaseIdMissing` | appends without the id land (201), on an empty blob too. Overwrite and Delete without the id do fail with `412 LeaseIdMissing`: Append Block alone ignores the lease |
| Filling the blob to its size limit | holds | holds. Each writer appends with `x-ms-blob-condition-maxsize` set below the blob's limit, and a pad that reaches the limit closes the blob |

The fill fence ran 12 times on the account, on a 256 KiB blob with 1 KiB blocks:
- **Six runs with 16 writers.** The pad lost its position race 3–4 times in the runs where
  it started at half full, and then landed once the writers had filled the blob. In every
  run, each writer's next append failed with `412 MaxBlobSizeConditionNotMet`.
- **Six runs with 2 writers, paced so the pad could win early.** In three of them the pad
  landed at 134, 142 and 194 KiB while both writers were still appending. The writers kept
  sending for a second after each pad, 37–45 appends a run, and none landed.
- **Every run** ended at exactly the limit, holding the acknowledged blocks and then the pad.

The size condition is checked by Append Block itself, as the position condition is, which
the spike's conditional levels showed to be exact at 64 writers. So this log closes a
segment by filling it ([Rotation](#rotation)). The fence works on an account with or
without a hierarchical namespace, and on Azurite, so the log uses this one mechanism
everywhere.

## The design in brief

```
<root>/head                               block blob, changed only by CAS (If-Match)
<root>/seg/<ordinal>-<nonce>              Append Blobs, the records, blind appends
<root>/ckpt/<position>-<log id>           block blobs, one checkpoint each, created once
<root>/pages/<name>                       block blobs, the committed set's pages (PageStore)
```

**The records** go into a chain of Append Blobs called *segments*. Only the newest segment
takes appends. Every older segment is *closed*: no append can land in it again.
- **The limits.** Each segment has a limit *L*. Every writer's append carries
  `x-ms-blob-condition-maxsize` set to the writer limit *W* = *L* − 32, so no writer's block
  takes the segment past *W*.
- **Closing a segment.** A writer that finds the segment full (`412
  MaxBlobSizeConditionNotMet`) closes it with a *pad*: one block that fills it to exactly
  *L*. After that, any append that respects *W* fails.
- **The block limit.** Azure's limit of 50,000 blocks per blob (`409
  BlockCountExceedsLimit`) closes a segment too.
- **Then** the writer moves the log on to the next segment ([Rotation](#rotation)).

**The head** is the only object anyone overwrites, always by CAS on its ETag. It names the
log's id, its live segments in order (each with its limit, and each closed segment with its
final length), and its newest checkpoint. It changes only when the log moves to a new segment, when a checkpoint
supersedes an older one, and at a trim. That is a few times a minute at the rates measured
above, against thousands of appends a second, so the CAS has almost no contention.

**Checkpoints, pages and segments are created once and never changed.** A segment's name
carries a random nonce, and only the writer whose CAS adds it to the head makes it part of
the log. So no stale writer can bring a deleted segment back (see
[Invariants](#invariants), AZ4).

## Objects

`<root>` is the log's location: a container and a prefix, such as
`abfss://catalog@account.dfs.core.windows.net/warehouse/catalog.log`. The log talks to the
account's Blob endpoint, since Append Blobs are a Blob API, whether or not the account has
a hierarchical namespace.

### The head

A block blob holding one protobuf message, encoded as log.proto's are (`org.intentlog.wire`).
Its schema of record is `intentlog-azure/src/main/proto/azure_log.proto`:

```protobuf
message AzureLogHead {
  int32  min_reader_version = 1;   // 1; required, first
  bytes  log_id             = 2;   // the LogCheckpoint's log_id: 16 bytes, UUIDv7; required
  uint64 generation         = 3;   // +1 at every CAS: for traces and tests only
  repeated Segment segments = 4;   // the live segments, oldest first, ordinals consecutive
  CheckpointRef checkpoint  = 5;   // the newest checkpoint: at or after segments[0]'s start
}

message Segment {
  uint64 ordinal       = 1;        // 0, 1, 2, ...: its place in the log
  string name          = 2;        // "seg/<ordinal, 20 digits>-<nonce, 16 hex>"
  uint64 closed_length = 3;        // its final length; absent on the last segment only
  uint64 limit         = 4;        // L: a pad fills it to this; writers stop at L - 32
  uint64 base          = 5;        // the position its offset 0 is: the one before's base + length
}

message CheckpointRef {
  bytes  through = 1;              // the position it covers (Positions, below)
  string name    = 2;              // "ckpt/<position, 20 digits>-<log id hex>"
}
```

The head's rules:
- **Every segment except the last is closed**, and its `closed_length` is its final length:
  its `limit` when a pad closed it, or its length at 50,000 blocks.
- **The checkpoint covers a position in one of the live segments.** Segments before the
  first live one have been trimmed.
- **The head goes only forward.** A CAS that would move the checkpoint back, or drop a
  segment that is still live, is not written; the writer reads the head again instead.

### Segments

An Append Blob, created empty by Put Blob with `If-None-Match: *` and with metadata:

| Metadata | Value |
|---|---|
| `intentlog_log_id` | the log id, 32 hex digits |
| `intentlog_segment` | the ordinal, in decimal |
| `intentlog_limit` | *L*, in decimal |
| `intentlog_format` | `1` |

The metadata is set in the same request that creates the blob, so a segment is never
without it. A reader checks it against the head's entry on every segment it reads. A
segment whose metadata disagrees with the head, or that the head doesn't name, is not part
of the log.

Each block is exactly one frame. A frame is either one log.proto `LogRecord` (an intention,
a GC record, or a `PageOut`) or, as a closed segment's last block, a pad.

```abnf
segment  = *frame [pad]
frame    = varint body trailer   ; varint length n > 0, then the n octets of a LogRecord
pad      = %x00 padlen filler trailer
padlen   = u32le                 ; the filler's length, so a pad can be any size from 17 up
filler   = *%x00
trailer  = ordinal crc
ordinal  = u64le                 ; the segment's ordinal, written by the appender
crc      = u32le                 ; CRC32C of everything in the frame before it
```

A `LogRecord` is never empty, so a length of 0 marks a pad. A pad is 17 bytes plus its
filler, and the writer limit leaves at least 32 bytes for it. Readers skip the pad. It is
the last frame of a segment that a pad closed, and it ends at the segment's `limit`.

A blind appender can't know its record's offset before the record lands, so unlike
[ATOMIC_FILE_LOG.md](ATOMIC_FILE_LOG.md)'s format 3 the trailer names no offset and no
incarnation. Positions come from the service instead: the reply's
`x-ms-blob-append-offset`, and a reader's own parse from a known boundary (the segment's
start, or a checkpoint's offset). The ordinal ties a frame to its segment, and the checksum
catches a parse that starts off a boundary. A segment's frames must end exactly at the
length the read returned. One that ends in a partial frame was read badly, and the reader
reads again. Append Block is atomic, so a partial block cannot be stored.

### Checkpoints

A checkpoint is a block blob holding one log.proto `LogCheckpoint`, whose `through` is the
position it covers. It is created by Put Blob with `If-None-Match: *` under
`ckpt/<position>-<log id hex>`, the position in 20 decimal digits.

Two compactors at one position write the same bytes (I11). A put that finds the name taken
reads the existing checkpoint and compares the bytes. Equal bytes mean the put already
happened. Different bytes break I11, and the put throws `IllegalStateException` naming both.
The log id is in the name so that two writers racing to create two logs at one root never
collide on a checkpoint (see [Creating the log](#creating-the-log)).

### Pages

`pages()` returns a `PageStore` under `<root>/pages/`. Each page is a block blob put without
a condition, since every writer puts the same bytes under a name (PageStore's contract).
`location(name)` returns the blob's URL, so a worker can read a page as a file.

## Positions, tails and receipts

A position is a segment's base plus an offset in it: 8 bytes, a big-endian u64. The head
records each segment's base, the base before it plus that segment's closed length. So the end
of a closed segment and the start of the next are one position, as the runtime needs: it
compares positions only for equality, and two tails at one place in the log must be equal
(TAIL_READS.md "Positions"). The provider orders positions numerically.

- **A tail** is the head's log id, the last segment read, and its position.
  - `exists()` is whether the head exists.
  - `bytes()` is the bytes of records after the head's checkpoint, which the compaction
    policy's size threshold compares.
  - `continues(earlier)` holds when the log ids are equal and this tail's position is at or
    after `earlier`'s. A log with separate checkpoints is never replaced.
- **A receipt** is an append's segment (ordinal, name and base), its offset from the reply,
  and the frame's length and CRC. A read through it checks that the frame at that offset is
  this one.
- **Trimmed, to a reader,** is everything before the head's checkpoint, which is where every
  read starts. A read after an earlier position returns a whole view. A read through a
  record before it is `TrimmedException`, as P3 requires. The trim itself only deletes whole
  segments.
- **A view's `start()`** is the position its records follow: its checkpoint's `through`, or
  for records alone (`readAfter`), the tail they continue.

## Operations

| `SharedLog` | On Azure |
|---|---|
| `capabilities()` | `append` true; `conditionalAppend` false (true in the conditional ablation, below); `fusedReplace` false; `separateCheckpoints` true; `maxRecordBytes` the smaller of the service version's Append Block limit (4 MiB before 2022-11-02, 100 MiB since) and the writer limit of an empty segment, less the frame's overhead |
| `read()` | GET the head; GET its checkpoint; GET each live segment from the checkpoint's position on (a ranged GET of the first, from its offset). Each closed segment must read exactly to its `closed_length`, and the last segment to the length its GET returned. Any 404 means a trim raced the read: read again from a fresh head, up to `maxThrottleAttempts` times, after which the log could not be read |
| `readAfter(since)` | A ranged GET of `since`'s segment from its offset (from the byte before it, dropped, so that a read at the segment's end is answered by a 206 rather than a 416 and a Get Blob Properties). If the GET shows the segment open (its last frame isn't a pad, it is short of its limit, and it has fewer than 50,000 blocks, from `x-ms-blob-committed-block-count`), the view is those frames: one request. A segment at its limit is closed though a read from its end sees no pad: only a pad reaches the limit. If it is closed, GET the head and continue with the segments after it. If the head no longer names it, it was trimmed: `TrimmedException`, and the runtime reads whole |
| `readThrough(r, wait)` | If the reply put `r` at the tail of the newest view this log holds, that view and `r`'s record, with no request ([Reading through a write](#reading-through-a-write)). Else the newest view this log has returned or built that ends at or before `r`, extended by a ranged GET from its tail through `r`'s frame (and across segments as in `readAfter`). Without such a view, a whole `read()`. The view must hold `r`'s frame at `r`'s offset (P3). A GET that ends before it is read again within `wait`, then `LogLagException`. Azure's reads are strongly consistent, so that is not expected (an open question below) |
| `probe()` | GET the head and Get Blob Properties of its last segment: the tail without its records. Used only in the conditional ablation |
| `append(record, null)` | Append Block to the head's last segment, with no position condition and with `x-ms-blob-condition-maxsize` at the segment's writer limit. If the segment is full or closed (P6: nothing landed), read the head: if it names a later segment, send there; if not, move the log on ([Rotation](#rotation)) and send to the new last segment. After `maxRotations` (4) moves, `LogFullException`. A 404 (a stale head: the segment was trimmed) re-reads the head and sends again. The receipt is the segment, the reply's `x-ms-blob-append-offset`, and the frame's length |
| `append(record, tail)` | The conditional ablation only: conditioned on `tail`'s position, not its segment (Bug 89 (moved on, same position)). Append Block with the writer limit and `x-ms-blob-condition-appendpos` set to the position's offset in the head's last segment: `tail`'s segment, or a later one that starts at the position (a close that needed no pad, or a trim). A head older than `tail` is read again first; a full segment is moved on from as by a blind append. A last segment that starts past the position, a 412, or another log fails as a conflict |
| `putCheckpoint(through, bytes)` | Put the checkpoint blob ([Checkpoints](#checkpoints)), then CAS the head to name it if it is newer than the head's. At a tail that does not exist, create the log ([Creating the log](#creating-the-log)) |
| `trim(through)` | CAS the head to drop the segments before `through`'s ordinal. P5: only if the head's checkpoint, or the one just put, is at or after `through`. The blobs are deleted later, by the sweep |
| `pages()` | [Pages](#pages) |

### Reading through a write

A writer learns whether its commit applied by replaying the records that landed between the
state it read and its own record. On any log that is the read through its write; what differs
is when it costs a request:

- **Nothing landed between.** The reply's offset puts the record at the tail of the view the
  log holds, so the view through it is that view and the record, with no request. This is the
  case of a conditional append that won its race, which `AtomicFileLog` reads nothing for
  either (ATOMIC_FILE_LOG.md).
- **Something landed between.** One ranged GET from the view's tail through the record. A
  conditional append in the same case lost its race: it reads the tail, sends its record
  again, and may lose again.

So blind appends add no reads: they remove the lost appends and the resends. The receipt
carries its record for the first case (`AzReceipt.record`), and `TestAppendBlobLog` checks
both (`aReadThroughAWriteReadsOnlyWhatLandedBetween`). With tail reads on
(`fileio.catalog.read.tail`), a catalog's refresh is also one ranged GET of the last segment
while it doesn't change.

A commit's cost, then, is one Append Block (about 10 ms on the spike's account), one ranged
GET when other records landed while it was in flight, and a move to a new segment now and
then. It never pays for a lost race.

Rejected records are no different from the one-object log's: a record whose writer's read
was stale lands, replay rejects it, and it stays in the log until a compaction drops it.

**Every read goes to the primary endpoint.** Reads there are strongly consistent, and
AZ2 and `readThrough` depend on that. On an account that can read its geo-secondary
(RA-GRS or RA-GZRS), the secondary endpoint (`<account>-secondary.blob.core.windows.net`)
is copied asynchronously and can lag the primary by any amount. A read there can return
an older head, or a segment shorter than appends already acknowledged. The provider
builds its clients on the primary endpoint and refuses a location that names the
secondary. It never sets the SDK's `secondaryHost` (`RequestRetryOptions`), which would
send a failed read's retries to the secondary.

### Creating the log

`putCheckpoint` at a tail that does not exist creates the log, as `LogRuntime`'s `CREATE`
expects:
1. Put the first checkpoint, at position (0, 0), under its own log id's name.
2. Create segment 0 under a fresh nonce.
3. Create the head with `If-None-Match: *`, naming both.

If the head already exists, read it. With the same log id, it is this creation, retried.
With another log id, another writer created the log first, and the put fails
(`LogConflictException`; the runtime then rejects the commit with `CREATE_LOST`). The
loser's checkpoint and segment are named by its own log id and nonce, which no head names,
so the sweep deletes them.

## Rotation

A writer whose append finds the last segment *n* full (`412 MaxBlobSizeConditionNotMet`,
`409 BlockCountExceedsLimit`), and whose fresh read of the head still names *n* last, moves
the log on:

1. **Close *n*.** Get Blob Properties gives its length *x* and its block count.
   - If *x* is its limit *L*, or it has 50,000 blocks, it is already closed, at *x*.
   - Otherwise, append a pad of *L* − *x* bytes with `x-ms-blob-condition-appendpos` *x* and
     `x-ms-blob-condition-maxsize` *L*. On 201, *n* is closed at *L*.
   - If the pad loses its position race (`412 AppendPositionConditionNotMet`: other
     writers' blocks still fit), go back to Get Blob Properties. If it meets the block
     limit (`409 BlockCountExceedsLimit`), *n* is closed at the length that Get Blob
     Properties then returns.

   The pad can land only at *x*: any other length would take the blob past *L*. Every
   lost race means the blob grew, and it can't grow past *W*, so the loop ends. A writer
   rotates only once its own block didn't fit, so the pad is at most that block's size
   plus 32 bytes: one Append Block. After the close, no append lands in *n*.
2. **Create a candidate *n*+1** under a fresh nonce (Put Blob, `If-None-Match: *`, with
   its metadata).
3. **CAS the head**, conditioned on the version it read (which named *n* last), to record
   *n*'s `closed_length` and add the candidate, with its limit, as the last segment.
4. **If the CAS fails, read the head.** If it names an *n*+1, another writer moved the log
   on: delete this writer's candidate, which is best effort since the sweep also finds it,
   and append there. If the head changed for another reason (a checkpoint or a trim), go
   back to step 3 with the new version.

Several writers that find *n* full together all try to close it, and one pad lands at
most. They each create a candidate, and exactly one CAS wins. A writer that crashes at any
step leaves one of two things:
- a closed *n* with no successor, which the next writer to find *n* full repairs by the same
  steps;
- an orphan candidate, which the sweep deletes.

Appends that fail during a move landed nothing (P6), and their writers append again to the
new segment.

The order matters, and the model must break if it is changed:
- **Close before the CAS.** Then a reader that sees a segment other than the last knows it
  can't grow, and that its length is the `closed_length` the head records. Moving the log
  on before closing would let an append land in *n* after a reader had read *n* in full as
  the head's closed segment: a record the reader skipped, and a splice.
- **Every append carries the writer limit.** An append without it could land after the
  pad, past the segment's `closed_length`: acknowledged, but outside the log.
- **Create before the CAS.** Then every segment the head names exists until it is trimmed,
  and nobody ever creates a segment because it was missing. A writer that found the next
  segment missing and created it could bring back a segment that a trim had deleted (AZ4).

Rotations are rare: 50,000 blocks at the spike's 3,100 appends a second is one rotation
every 16 s. A rotation takes four or five requests (the head's read, Get Blob Properties,
the pad, the create and the CAS), so appends stall for about 50 ms each time. Putting
several records in one block would make rotations rarer still; see [Later](#later).

## Compaction, trim and sweep

`LogRuntime.compact()` runs on this log as it does on any log with separate checkpoints
(LOG.md "Compaction"):
1. append a GC record;
2. read through it;
3. page the committed set's blocks, if any are due;
4. `putCheckpoint` at the view's tail;
5. `trim` through that tail;
6. delete the pages it dropped.

The provider adds three things:

- **`putCheckpoint` CASes the head to name the new checkpoint**, so that readers start from
  it. A checkpoint that loses its CAS to a newer one has nothing to do, since the head goes
  only forward.
- **`trim` only changes the head.** Segments the head no longer names are deleted later, by
  the sweep, not by the trim itself. A reader holding an older head can keep reading them
  for a while, and one that finds a segment gone reads again from a fresh head. Deletion is
  safe at any time (AZ5); the grace period only saves those reads.
- **The sweep runs after a trim, and from `LogRuntime.sweep`.** It deletes three kinds of
  blob:
  - segments the head doesn't name whose ordinal is at or below the head's last ordinal,
    last modified more than `trim.grace` ago. This covers trimmed segments and losing
    candidates. A candidate for an ordinal past the head's last may still win its CAS, so it
    is never swept;
  - checkpoints older than the head's, other than the newest `checkpoint.keep`;
  - checkpoints and segments of other log ids.

A committer compacts, best effort, when the policy says the log is due, as on any log with
checkpoints apart. At thousands of commits a second, compacting every 1,000 records (the
default `max.append.count`, which was measured for the one-object log on S3 Express) would
put several checkpoints a second. This log therefore has its own rule ([When to
compact](#when-to-compact)), in `max.append.count`'s place (`LogCatalogProperties.runtimeConfig`).
A dedicated compactor (`CatalogLog.compact()`
on a timer) is optional. Correctness never depends on who compacts: a checkpoint at a
position is the same whoever writes it (I11).

### When to compact

Nothing about the store bounds the records after the checkpoint here: segments move on at
their limit, and a checkpoint never races an append. What the threshold trades is a cold
reader's cost (the checkpoint and every record after it) against the checkpoints' cost (each
encodes the whole catalog). A count does that badly across modes: pointer records are about a
KB and inline ML's tens of KB, and the checkpoint grows with the catalog. So this log compacts
by a share of its checkpoint (`CompactionPolicy.share`, since 2026-11-10):

- **When the records reach a share of the checkpoint.** Once the bytes of the records since
  the checkpoint reach `checkpoint.share` (1.0) of the checkpoint's, but not before
  `checkpoint.min.bytes` (1 MiB) of them; and at `max.append.size` (16 MiB) or
  `checkpoint.records` (10,000) records whatever the checkpoint's size. A cold read then costs
  at most (1 + share) checkpoints (or a checkpoint and the floor), and a checkpoint's bytes per
  byte of records are at most 1 / share.
- **Counted from the last GC record.** A writer that reads only what follows its own tail
  never sees another writer's checkpoint, so counting from the checkpoint it last read made
  every writer compact at every threshold. But every compaction begins with a GC record, which
  every writer replays, so replay also counts the records and bytes since the last GC record
  (`Replay.Result.recordsSinceGc`, `Replica.recordsSinceGc`), and the policy counts those.
- **At a point each writer draws.** Past the threshold, a writer compacts once the log's
  progress reaches a point it drew in [1, 2) times the threshold (`LogRuntime.compactApart`,
  as the one-object log's compaction after the commit draws), and after a compaction that
  failed, twice as far on. The first writer to reach its point compacts, and the rest replay
  its GC record before they reach theirs.

Before, in a scratch run in catalog-bench (pointer mode, 50 tables, 16 writers, 25 commits a
second on `azure-2026-11`, a checkpoint every 200 records), all 16 writers compacted at each of
the four thresholds crossed with blind appends, and 3 or 4 with conditional ones, whose writers
land one at a time. `TestAzureCompaction` has 8 writers take turns for 1,600 commits past a
threshold of 100 records: they must compact 7 to 17 times between them; counting from their own
checkpoints, they compacted 77 times.

## Failures

Azure's responses, mapped onto the SPI's signals (LOG.md "Failures"):

| Response | From | Signal |
|---|---|---|
| 201 | Append Block | a receipt |
| 409 `BlockCountExceedsLimit`, 412 `MaxBlobSizeConditionNotMet` | Append Block | nothing landed: read the head, rotate if it still names this segment last, and send again (within `append`); after `maxRotations`, `LogFullException` |
| 404 `BlobNotFound` | Append Block | nothing landed: the head was stale. Read it and send again (within `append`) |
| 412 `AppendPositionConditionNotMet` | Append Block, conditional ablation | `LogConflictException` |
| 503 `ServerBusy`, 500 `OperationTimedOut`, 500 `InternalError` | any write | `LogThrottledException`: it may have landed, and sending it again is safe |
| 412 on the head's `If-Match` | a CAS | read the head and act again (inside the provider) |
| another 4xx (403, 400) | any write | `LogNotSentException`: the service refused it, and nothing landed |
| a timeout, a reset connection, any other exception | a write | unknown outcome (the exception itself) |
| any failure | a read | the log could not be read |

**The Azure SDK's own retries are off**: `RequestRetryOptions` with `maxTries` 1, as in the
spike, for every request (`AzureAppendBlobStore.create`; a client given to its constructor
must be built the same way, which it cannot check). An SDK that sends a timed-out Append Block
again can land the record twice and report only the second copy. The runtime would never know
about the first, which is exactly the bug that `SharedLogCommit.tla`'s `dup` switch puts back:
a throttled send that isn't counted as a write that may have landed. The runtime's `WRITE`
does the retries and keeps count. A pad the store throttles fails the append that found the
segment full as throttled (`LogThrottledException`), and the runtime's next send closes the
segment from a fresh look at its length. Conditional writes resent below the provider would
be safe only in part: a resent CAS of the head that already landed fails its condition, and a
rotation that then reads the head and finds its own candidate named keeps it; but a resent
create of a candidate finds its name taken and fails the rotation.

## Invariants

The provider's invariants, which the new model ([Testing](#testing)) states and the
fuzzers check. AZ1–AZ5 are about the log's structure; AZ6 is the SPI's P1 to P6 as this
log meets them.

| | Invariant | Why it holds |
|---|---|---|
| AZ1 | Every segment the head names, except the last, is closed, and its length is its `closed_length` | Rotation closes before its CAS, and records the length the close ended at; every append carries the writer limit, so none lands in a closed segment |
| AZ2 | The log is the head's checkpoint, then the frames of its live segments in ordinal order, each segment from its start (the first from the checkpoint's offset) to its length, pads left out; every reader that starts from that head reads the same sequence, up to where it stops in the last segment | AZ1; closed segments can't change; Append Block is atomic; reads go to the primary, where they are strongly consistent; and a reader parses frames only from known boundaries |
| AZ3 | An acknowledged append's frame is at its receipt's offset in the segment its receipt names, before that segment's `closed_length` once it has one, and that segment is in the log from the head that named it until a checkpoint covers it | The service's offset (the spike's check); the writer limit (an acknowledged block ends at or before *W*, and a closed segment's length is at least the end of every block in it); a segment leaves the head only by a trim, which needs a checkpoint past it (P5) |
| AZ4 | A segment's name is in some head only if its creator's CAS put it there, and a blob under that name is that creator's | Names carry a fresh nonce, and only the CAS winner's candidate is named. Nobody creates a named segment after the fact, so a stale writer can't bring back a deleted one. It gets a 404, or finds the segment full, and reads the head |
| AZ5 | The sweep never deletes a segment the current head names, so what it deletes is either covered by the head's checkpoint or was never part of the log | It deletes only segments the current head doesn't name, at or below the head's last ordinal. A reader holding an older head that finds a segment gone reads again from a fresh head |
| AZ6 | P1–P6: one order (AZ2), with `start()` truthful; acknowledged writes durable (the service's write acknowledgement); `readThrough` holds the record (checked against the receipt); P4 in the conditional ablation (the service's position condition); trims only behind a checkpoint; full and not-sent are failures where nothing landed (409/412/4xx) | As stated |

What the runtime already guarantees on top, by the shared-log model: exactly-once
application by record id, honest outcomes under duplicates and lagging reads (the three
rules), and replay's determinism. None of that changes.

## Mistakes the model must catch

Each mistake below is a switch for the new model (and later for the fuzzers' mutants), in
the style of `SharedLogCommit.tla`'s. Each must break a property:

| Switch | The mistake | Breaks |
|---|---|---|
| `casbeforeclose` | rotation CASes the head before closing *n* | AZ2: an append lands in *n* after a reader read it whole: a splice |
| `nolimit` | an append is sent without the writer limit | AZ3: a block lands after the pad, past `closed_length`, acknowledged but outside the log |
| `createonmissing` | a writer that finds the head's last segment missing creates it | AZ4, then AZ2: a stale writer brings back a trimmed segment, and a reader of an older head reads its record |
| `noclosedlength` | readers trust whatever length a closed segment's GET returns | AZ2 under a short read |
| `sweepnamed` | the sweep deletes segments the head still names | AZ3: an acknowledged record is gone, `Readable` |
| `trimearly` | a trim drops segments past the head's checkpoint | P5 (`Readable`), as in `SharedLogCommit.tla` |
| `sdkretry` | a timed-out append is resent below the runtime | the shared-log model's `dup`: an honest outcome turns false |
| `offsetfromlength` | the receipt's offset is the length before the append (from a HEAD) rather than the reply's | AZ3: with other writers, the read back judges another record |

## Configuration

The catalog opens this log when `fileio.catalog.log` is `azure-append`. The default,
`atomic-file`, is today's `FileIOCatalog`. The properties:

| Property | Default | Meaning |
|---|---|---|
| `fileio.catalog.log` | `atomic-file` | which log the catalog keeps |
| `fileio.catalog.log.location` | none: required | the log's root, a Blob URL: `https://<account>.blob.core.windows.net/<container>/<path>` |
| `fileio.catalog.log.append` | `blind` | `blind`, or `conditional` (the ablation: the same objects and frames, appends conditioned on the tail; `conditionalAppend` true) |
| `fileio.catalog.log.segment.max.bytes` | 64 MiB (provisional) | a new segment's limit *L*. It must exceed the largest record by the frame's overhead plus 32 bytes. Small values make rotations frequent, for tests |
| `fileio.catalog.log.checkpoint.share` | 1.0 | compact once the records since the checkpoint reach this share of its bytes ([When to compact](#when-to-compact)) |
| `fileio.catalog.log.checkpoint.min.bytes` | 1 MiB | the records' bytes below which the log never compacts by size |
| `fileio.catalog.log.checkpoint.records` | 10,000 (provisional) | the records at which it compacts whatever their bytes (`max.append.size` bounds their bytes); in place of `max.append.count` |
| `fileio.catalog.log.checkpoint.keep` | 2 | checkpoints kept besides the head's |
| `fileio.catalog.log.trim.grace.ms` | 60,000 | how long a segment out of the head stays before the sweep deletes it |

The credentials are an account key, given as the fork's ADLS FileIO names it
(`adls.auth.shared-key.account.name` and `.key`, `AzureProperties`), built into a Blob
client on the primary endpoint (`AzureLogs.open`), with the SDK's retries off and no
secondary host. The FileIO's other credentials (a SAS, a connection string, the default
credential) are [Later](#later): `AzureAppendBlobStore.create` already takes an Entra ID
`TokenCredential`.

## Modules

- **`intentlog-azure`** (new): `AppendBlobLog`, over a narrow `AppendBlobStore` interface
  with two implementations:
  - `AzureAppendBlobStore`, on `azure-storage-blob`;
  - `InMemoryAppendBlobStore`, with fault hooks as `InMemorySharedLog`'s.

  The interface is the seven requests the log uses:
  - create an Append Blob with metadata if absent;
  - append a block with a size limit, blind or at a position;
  - get a range, with the length, block count and metadata;
  - get properties (length, block count, metadata);
  - put a block blob if absent, or unconditionally;
  - put a block blob if it matches an ETag;
  - delete, and list.

  The in-memory store models the account, not Azurite: it has no Seal, and its leases
  (which the log doesn't use) wouldn't stop appends.

  The module depends on `intentlog-core` and the Azure SDK only, not on Iceberg.
- **`iceberg-fileio-catalog`**: opening the log from the catalog's properties (`AzureLogs`,
  which `FileIOCatalog` calls for `fileio.catalog.log` `azure-append`; the module depends on
  `intentlog-azure` optionally).

## Testing

As every provider is tested (LOG.md "Testing a provider"), plus a model:

1. **The model, first.** Write `AzureLog.tla` before the Java, as `SharedLogCommit.tla` was:
   - the head and its CAS, segments with limits, the close (its pad racing appends),
     rotation's steps with a crash between any two, the sweep, and readers holding older
     heads;
   - writers whose appends land, fail, or land late, before or after a close.

   Its properties: AZ1–AZ5, and that the sequence every reader reads is a prefix of one log.
   It should refine `SharedLogCommit.tla`'s log, a single sequence with checkpoints and a
   trim point, so the commit protocol's proofs carry over unchanged. Each switch above must
   break a property (`check.py`).
2. **The contract.** `SharedLogContract` on `InMemoryAppendBlobStore`, and on Azurite
   through `AzureAppendBlobStore` (it supports the size condition), in both append modes.
3. **The fuzzer.** `CommitProtocol` on `AppendBlobLog` over the in-memory store
   (`TestAzureCommitProtocol`, 5,000 tries):
   - every fault the memory log has;
   - rotations forced by a tiny `segment.max.bytes`, with a crash at each step;
   - sweeps from stale heads;
   - appends that land late into a segment closed since (they must fail);
   - readers whose heads are stale.

   Calibrate it with the switches above as mutants (`fuzz/mutants/azurelog.sh`).
4. **The catalog.** `TestLocalCatalog` and `TestLocalCatalogTransaction` in an `AZLOG` mode
   over the in-memory store; `TestCatalogFaultProperties` and `TestCatalogRaceProperties`
   drawing it as one more log.
5. **The cloud.** An integration test on a real account (`AzureAppendBlobLogIT`): the
   contract, and a run of the fuzzer's schedules with real requests.

**Where it stands (2026-11-13): all five steps are done.** The format and protocol are
specified, for review, in [AZURE_LOG_FORMAT.md](AZURE_LOG_FORMAT.md).

1. **The model** (`spec/tla/README.md` "The Azure log") holds in eight configurations, the
   largest at 335,000 states:
   - two and three writers;
   - segments closed by a pad and by the block limit;
   - checkpoints, trims and sweeps;
   - write faults, late landings and short reads.

   Each of the eight switches breaks a property. Fourteen witnesses show the passing runs
   reach the branches: a rotation, a lost CAS, a pad, a block-limit close, a 404 for a stale
   writer and for a reader, a late landing, and the rest.
2. **The contract** passes in three variants (`TestAppendBlobLogContract`): blind appends,
   blind appends in segments that close every few records, and conditional appends. The SDK
   store passes its own tests on Azurite (`TestAzureAppendBlobStore`, with
   `AZURITE_BLOB_ENDPOINT`), but the contract does not yet run on Azurite.
3. **The fuzzer** (`TestAzureCommitProtocol`, 2,000 tries):
   - segment limits of 300 bytes to 1 MiB and blobs of 2, 4 or 50,000 blocks;
   - every fault on every store request;
   - one to three instances of the log taking requests in turn, as processes with stale
     cached heads;
   - at the drain, every replica a read returned must read on to the log's end (stale
     readers).

   `TestAppendBlobLog` adds what the fuzzer can't reach: closes by a pad and by the block
   limit, an append that arrives after a close, a trim after the log moved on, and a writer
   whose segment was swept.

   **Calibration** (`fuzz/mutants/azurelog.sh`; on havoc 2026-11-12, `az-calib` at 9ab6c80,
   the fuzzer at 100,000 tries): every mutant fails an oracle, and the unmutated run passes.
   - `createonmissing`, `noclosedlength` and `sdkretry` fail the fuzzer's oracles.
   - `casbeforeclose`, `nolimit` and `sweepnamed` are caught first by the log's own checks:
     a closed segment of the wrong length, or a named segment missing, makes the log
     unreadable rather than wrong.
   - `trimearly`: `TestAppendBlobLog` catches it. The fuzzer never preempts a compaction, so
     it caught it only at the calibration's 100,000 tries, with the log unreadable.
   - `offsetfromlength` was equivalent in Java while `readThrough` always read: it checked the
     receipt's CRC, and the runtime judges a commit by its id. Since a read through a write
     trusts the reply's offset to skip its read ([Reading through a write](#reading-through-a-write)),
     a wrong offset returns a state the log never held, and the fuzzer and `TestAppendBlobLog`
     kill it (2026-11-10), as the model's `AckedAtReceipt` does.
   - Two mutants of the Java alone (2026-11-10): `skipread` (the read skipped when records
     landed before the write too), killed by `TestAppendBlobLog` and the fuzzer; and
     `compactbycheckpoint` (writers counting toward compaction from their own checkpoints),
     killed by `TestAzureCompaction` only, since it makes compactions many, not wrong.
   - `padunseen` (2026-11-11, from a review): a read from the end of a segment a pad closed,
     before the head named its successor, takes the segment for open. `TestAppendBlobLog`
     kills it, and the fuzzer's stale-readers oracle does at the calibration's 100,000 tries
     (not in 2,000 local ones).

4. **The catalog.** `TestLocalCatalog` and `TestLocalCatalogTransaction` pass in `AZLOG`,
   `AZLOG_TM` and `AZLOG_ML` (112 and 34 tests each, the same skips as every other mode): each
   catalog an `AppendBlobLog` of its own over one in-memory store whose segments hold 8
   blocks, so the log moves on every few commits, and a compaction every 3 records. After each
   test, every checkpoint ever put must be the replay of the records before it
   (`CheckpointRoundTrip.assertEveryCheckpoint` over `AzureLogHistory`).

   The fault and race fuzzers draw this log a quarter of the time, as they draw the shared log
   in memory (`LocalCatalogMode.Log`; 2026-11-13). After each run, every checkpoint must replay
   the log before it.
   - `TestCatalogFaultProperties`: every write of the log meets the command's planned faults (a
     record, the moves that make room for it, a compaction's record, checkpoint and head), and
     every read of the head, a checkpoint or a segment its planned read fault. A blind append
     has no conflict to report, so a write planned to land as a conflict lands with its reply
     lost. A preempting command runs before a read of the log or the first write, as on one
     object; a read after it sees its effects, since a walk goes on past a segment closed under
     it. Segments hold 3 or 7 blocks in 64 KiB, closing by the block limit, or 50,000 in 2 KiB,
     closing by a pad (about half of those runs land one; a record a segment can't hold is
     rejected before it is sent, `TOO_LARGE`).
   - `TestCatalogRaceProperties`: each racer runs just before one of A's commit's appends (a
     `SEND` in its commit trace), as before its writes on the other logs; a compaction's appends
     are not its writes.

5. **The cloud** (2026-11-11 UTC, from the laptop, on the spike's account: a hierarchical
   namespace, westus). All pass:
   - `AzureAppendBlobLogIT`: the contract in its three variants, 43 tests run (5 skipped for
     capabilities the log lacks).
   - `AzureCommitProtocolIT`: 25 tries of the fuzzer, twice, with real requests through
     `RecordingStore`. It injects the in-memory store's faults around real requests, and
     records what landed for the oracles. In the second run, 14 tries moved the log on (35
     real pads, up to 5 segments), with one to three instances of the log.
   - `AzureAppendBlobStoreIT` (2026-11-12): a GET from a blob's length is `416`
     `InvalidRange` there (Azurite: `206`, no bytes), from the byte before a `206` of one byte,
     and the store's read from a blob's end one request.
   - `AzureLogCatalogIT` (2026-11-13, from the laptop, on the rmow-bench account, westus):
     Iceberg's catalog and catalog-transaction suites through a `FileIOCatalog` that opens its
     log from its properties (`azure-append`, blind appends, an account key), in the three table
     metadata storages, the tables' files in memory. 438 tests run (40 skipped, as in memory).
     With segments of 1 KiB (2 KiB for inline tables) and a trim grace of 0, 46 of the 444 logs
     moved on, 57 times, and sweeps deleted segments as they left the head.

   Each run writes under `intentlog-it/<UTC>-<random>` (`AzureLogCatalogIT`:
   `catalog-it/<UTC>-<random>`) and deletes it afterwards. They run only when asked
   (`-Dtest='AzureAppendBlobLogIT*,AzureCommitProtocolIT,AzureAppendBlobStoreIT'`;
   `-pl iceberg-fileio-catalog -Dtest='AzureLogCatalogIT*'`), with `AZURE_LOG_IT_CONTAINER_URL` and
   `AZURE_STORAGE_KEY` set.

## Measurements

These run after the code exists, each with its predictions written down first:
- the contention driver and catalog-bench on this log against `AtomicFileLog` on the same
  account, by writers and by conflict encoding;
- the commit rate that applies, the rejections by reason, and commit latency;
- the conditional ablation on the same log, which isolates what blind appends buy from
  what the segments and the head cost.

catalog-bench's `azure` store profile (endive's, an 87 ms append and a 2 s lost race) isn't
this account's. The spike's latencies should become a measured profile (`azure-2026-11`),
as the S3 Express spikes became `s3x-2026-11`.

## Open questions

Each needs a spike before the code depends on it. The cloud ones can run together, as
`AzureAppendSpike` modes run by catalog-bench's `bin/azure-spike.sh`, on a VM of the rmow-bench
deploy:

| | Question | How to answer it |
|---|---|---|
| Q1 | Does Append Blob Seal work on an account with a hierarchical namespace? **Answered 2026-11-10: no**, and a lease doesn't stop appends there either; filling to the size limit does ([Closing a segment](#closing-a-segment-seal-and-leases-dont-work-filling-does)) | rmow-bench's `AzureSealProbe` |
| Q2 | While appends land, does a ranged GET return only whole blocks, with its length and block count consistent with its body? | readers parsing frames during the spike's blind levels |
| Q3 | How long do appends stall during a rotation at 32–64 writers, and does the fill fence hold there? It was probed from the laptop, with 16 writers and 1 KiB blocks | the spike with a small segment limit and the rotation protocol, from the VM |
| Q4 | Where does a blob's rate stop growing? Blind appends had not stopped at 64 writers (3,113 landed/s, the client's CPU share 7.4%) | the spike at 128 and 256 writers, from two VMs, with rotation (Q3) so that no level fills its blob |
| Q5 | Do the rates hold at 64 KiB and across repeated runs, and on an account without a hierarchical namespace? | `--bytes 4096,65536 --reps 3`; a second account |
| Q6 | Does Azurite support Seal and `x-ms-blob-condition-maxsize`? **Answered 2026-11-10: yes, both**, but it is no model of an account with a hierarchical namespace: it seals, and its leases stop appends. It also answers a read that starts exactly at a blob's length with `206` and no bytes, where Azure answers `416` `InvalidRange` (`AzureAppendBlobStore` handles both, and reads from the byte before, so a read from the end is one request on either) | `AzureSealProbe` on Azurite 3.37.0; `TestAzureAppendBlobStore`; on the account, `AzureAppendBlobStoreIT` (2026-11-12) |

## Later

- **Several records in one block** (group commit): one writer's batch, or a short wait in
  a client shared by many threads. That means fewer blocks, so rarer rotations, at the cost
  of latency. Each record keeps its own frame and id, so replay doesn't change.
- **One elected compactor**, by a lease on the head, so committers stop duplicating
  checkpoints at high rates. A lease would put a lease id on the head's CAS. Only if the
  measurements show duplicate checkpoints costing something.
- **The FileIO's other credentials** for the log (a SAS, a connection string, the default
  credential), beside the account key: the store takes a `TokenCredential` already.
- **Other stores.** S3 Express has no append without an offset, and GCS's appendable objects
  admit one writer at a time. On those stores the catalog keeps conditional appends
  (`AtomicFileLog`) or a log in another service. The catalog keeps one SPI, with a provider
  for each store.

## Related work

AirDB ("Eliminating Servers in State Machine Replication", arXiv 2611.01363, Nov 2026) also
keeps a command log in Azure Append Blob segments, created if absent and listed in a meta
segment, and its writers validate after appending. Cite it as concurrent work. What this
log adds:
- a head changed by CAS that makes rotation and trimming safe against stale writers (AZ4);
- positions that every read checks;
- the runtime's exactly-once commit with its proofs;
- conflict rules carried in the records, which judge each record where it lands.
