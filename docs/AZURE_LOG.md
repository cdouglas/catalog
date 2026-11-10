# The Azure append-blob log

**Status:** this is a specification. No code exists yet. It was written on 2026-11-10, on
branch `azure-log` (from `tail-reads` at 3b01e2a).

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

rmow-bench's `AzureAppendSpike` measured both kinds of append on 2026-11-10:
- **Setup:** one D8as_v6 VM in westus, beside an account with a hierarchical namespace;
  4 KiB blocks; a new blob for each level; each level 2 s of warm-up and then 20 s measured.
- **Retries and connections:** the SDK's retries were off and every writer had its own
  connection.
- **The check:** after each level the blob was read back. Every acknowledged block had to be
  intact (CRC32C), appear once, and sit at the offset its reply named.
- **Where:** the run is rmow-bench's `results/spike/azure-appends-20261110T192545Z`, built
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
computed from the run's requests in rmow-bench's lab (`append_requests`).

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
- no sealing and no moving from one blob to the next.

## The design in brief

```
<root>/head                               block blob, changed only by CAS (If-Match)
<root>/seg/<ordinal>-<nonce>              Append Blobs, the records, blind appends
<root>/ckpt/<ordinal>-<offset>-<log id>   block blobs, one checkpoint each, created once
<root>/pages/<name>                       block blobs, the committed set's pages (PageStore)
```

**The records** go into a chain of Append Blobs called *segments*. Only the newest segment
takes appends. Every older segment is sealed, which Azure's Append Blob Seal makes final:
an append to a sealed blob fails with `409 BlobIsSealed`. A segment is full when Azure
refuses another block (50,000 blocks, `409 BlockCountExceedsLimit`) or when it reaches a
configured size (`x-ms-blob-condition-maxsize`, `412 MaxBlobSizeConditionNotMet`). A writer
that finds the newest segment full or sealed moves the log on to the next segment (see
[Rotation](#rotation)).

**The head** is the only object anyone overwrites, always by CAS on its ETag. It names the
log's id, its live segments in order (with each sealed segment's final length), and its
newest checkpoint. It changes only when the log moves to a new segment, when a checkpoint
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
  uint64 sealed_length = 3;        // its final length; absent on the last segment only
}

message CheckpointRef {
  bytes  through = 1;              // the position it covers (Positions, below)
  string name    = 2;              // "ckpt/<ordinal>-<offset>-<log id hex>"
}
```

The head's rules:
- **Every segment except the last is sealed**, and its `sealed_length` is the length it was
  sealed at.
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
| `intentlog_format` | `1` |

The metadata is set in the same request that creates the blob, so a segment is never
without it. A reader checks it against the head's entry on every segment it reads. A
segment whose metadata disagrees with the head, or that the head doesn't name, is not part
of the log.

Each block is exactly one frame, and each frame is one log.proto `LogRecord`: an intention,
a GC record, or a `PageOut`.

```abnf
segment  = *frame
frame    = varint body trailer   ; varint length L, then the L octets of a LogRecord
trailer  = ordinal crc
ordinal  = u64le                 ; the segment's ordinal, written by the appender
crc      = u32le                 ; CRC32C of the varint, the body and the ordinal
```

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
`ckpt/<ordinal>-<offset>-<log id hex>`, where ordinal and offset are `through`'s, in
decimal.

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

A position is a segment's ordinal and an offset in it: 16 bytes, a big-endian u64 for
each. The runtime compares positions only for equality. The provider orders them, by
ordinal and then by offset.

- **A tail** is the head's log id and generation, the last segment read and the length
  read in it, and its position.
  - `exists()` is whether the head exists.
  - `bytes()` is the bytes of records after the head's checkpoint, which the compaction
    policy's size threshold compares.
  - `continues(earlier)` holds when the log ids are equal and this tail's position is at or
    after `earlier`'s. A log with separate checkpoints is never replaced.
- **A receipt** is an append's segment (ordinal and name), its offset from the reply, and
  the frame's length.
- **A view's `start()`** is the position its records follow: its checkpoint's `through`, or
  for records alone (`readAfter`), the tail they continue.

## Operations

| `SharedLog` | On Azure |
|---|---|
| `capabilities()` | `append` true; `conditionalAppend` false (true in the conditional ablation, below); `fusedReplace` false; `separateCheckpoints` true; `maxRecordBytes` the service version's Append Block limit (4 MiB before 2022-11-02, 100 MiB since) less the frame's overhead |
| `read()` | GET the head; GET its checkpoint; GET each live segment from the checkpoint's position on (a ranged GET of the first, from its offset). Each sealed segment must read exactly to its `sealed_length`, and the last segment to the length its GET returned. Any 404 means a trim raced the read: read again from a fresh head, up to `maxThrottleAttempts` times, after which the log could not be read |
| `readAfter(since)` | A ranged GET of `since`'s segment from its offset. If that segment is still the head's last (the GET shows it unsealed), the view is those frames: one request. If it is sealed, GET the head and continue with the segments after it. If the head no longer names it, it was trimmed: `TrimmedException`, and the runtime reads whole |
| `readThrough(r, wait)` | The newest view this log has returned or built that ends at or before `r`, extended by a ranged GET from its tail through `r`'s frame (and across segments as in `readAfter`). Without such a view, a whole `read()`. The view must hold `r`'s frame at `r`'s offset (P3). A GET that ends before it is read again within `wait`, then `LogLagException`. Azure's reads are strongly consistent, so that is not expected (an open question below) |
| `probe()` | GET the head and Get Blob Properties of its last segment: the tail without its records. Used only in the conditional ablation |
| `append(record, null)` | Append Block to the head's last segment, with no position condition. If the segment is full or sealed (P6: nothing landed), move the log on ([Rotation](#rotation)) and send to the new last segment, up to `maxRotations` (4) times, then `LogFullException`. A 404 (a stale head: the segment was trimmed) re-reads the head and sends again. The receipt is the segment, the reply's `x-ms-blob-append-offset`, and the frame's length |
| `append(record, tail)` | The conditional ablation only: Append Block with `x-ms-blob-condition-appendpos` set to `tail`'s length, to `tail`'s segment. A segment other than the head's last fails as a conflict |
| `putCheckpoint(through, bytes)` | Put the checkpoint blob ([Checkpoints](#checkpoints)), then CAS the head to name it if it is newer than the head's. At a tail that does not exist, create the log ([Creating the log](#creating-the-log)) |
| `trim(through)` | CAS the head to drop the segments before `through`'s ordinal. P5: only if the head's checkpoint, or the one just put, is at or after `through`. The blobs are deleted later, by the sweep |
| `pages()` | [Pages](#pages) |

The provider keeps the newest view it has returned, so a commit's read back usually costs
one ranged GET. `AtomicFileLog` does the same for appends from a view it holds (LOG.md
"Providers"). With tail reads on (`fileio.catalog.read.tail`), a catalog's refresh is also
one ranged GET of the last segment while it doesn't change.

A commit's cost, then, is one Append Block and one ranged GET (about 10 ms each on the
spike's account), plus a move to a new segment now and then. It never pays for a lost
race.

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

A writer whose append finds the last segment *n* full (`BlockCountExceedsLimit`,
`MaxBlobSizeConditionNotMet`) or sealed (`BlobIsSealed`) moves the log on:

1. **Seal *n*** (Append Blob Seal). Sealing is idempotent: a second seal returns 200.
   After it, no append lands in *n*.
2. **Learn *n*'s final length** with Get Blob Properties. After the seal it can't change.
3. **Create a candidate *n*+1** under a fresh nonce (Put Blob, `If-None-Match: *`, with
   its metadata).
4. **CAS the head**, conditioned on the version it read (which named *n* last), to record
   *n*'s `sealed_length` and add the candidate as the last segment.
5. **If the CAS fails, read the head.** If it names an *n*+1, another writer moved the log
   on: delete this writer's candidate, which is best effort since the sweep also finds it,
   and append there. If the head changed for another reason (a checkpoint or a trim), go
   back to step 4 with the new version.

Several writers that find *n* full together all seal it (harmlessly), each create a
candidate, and exactly one CAS wins. A writer that crashes at any step leaves either a
sealed *n* with no successor, which the next writer to find *n* sealed repairs by the same
steps, or an orphan candidate for the sweep. Appends that fail during a move landed nothing
(P6), and their writers append again to the new segment.

The order matters, and the model must break if it is changed:
- **Seal before the CAS.** Then a reader that sees a segment other than the last knows it
  can't grow, and that its length is the `sealed_length` the head records. Moving the log
  on before sealing would let an append land in *n* after a reader had read *n* in full as
  the head's sealed segment: a record the reader skipped, and a splice.
- **Create before the CAS.** Then every segment the head names exists until it is trimmed,
  and nobody ever creates a segment because it was missing. A writer that found the next
  segment missing and created it could bring back a segment that a trim had deleted (AZ4).

Rotations are rare: 50,000 blocks at the spike's 3,100 appends a second is one rotation
every 16 s. A rotation takes four requests, so appends stall for about 50 ms each time.
Putting several records in one block would make rotations rarer still; see
[Later](#later).

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
checkpoints apart. At thousands of commits a second, compacting every 100 records (the
default `max.append.count`, which was measured for the one-object log on S3 Express) would
put several checkpoints a second. This log therefore has its own threshold
(`fileio.catalog.log.checkpoint.records`, below). A dedicated compactor (`CatalogLog.compact()`
on a timer) is optional. Correctness never depends on who compacts: a checkpoint at a
position is the same whoever writes it (I11).

## Failures

Azure's responses, mapped onto the SPI's signals (LOG.md "Failures"):

| Response | From | Signal |
|---|---|---|
| 201 | Append Block | a receipt |
| 409 `BlockCountExceedsLimit`, 412 `MaxBlobSizeConditionNotMet`, 409 `BlobIsSealed` | Append Block | nothing landed: rotate and send again (within `append`); after `maxRotations`, `LogFullException` |
| 404 `BlobNotFound` | Append Block | nothing landed: the head was stale. Read it and send again (within `append`) |
| 412 `AppendPositionConditionNotMet` | Append Block, conditional ablation | `LogConflictException` |
| 503 `ServerBusy`, 500 `OperationTimedOut`, 500 `InternalError` | any write | `LogThrottledException`: it may have landed, and sending it again is safe |
| 412 on the head's `If-Match` | a CAS | read the head and act again (inside the provider) |
| another 4xx (403, 400) | any write | `LogNotSentException`: the service refused it, and nothing landed |
| a timeout, a reset connection, any other exception | a write | unknown outcome (the exception itself) |
| any failure | a read | the log could not be read |

**The Azure SDK's own retries must be off** for appends: `RequestRetryOptions` with
`maxTries` 1, as in the spike. An SDK that sends a timed-out Append Block again can land the
record twice and report only the second copy. The runtime would never know about the first,
which is exactly the bug that `SharedLogCommit.tla`'s `dup` switch puts back: a throttled
send that isn't counted as a write that may have landed. The runtime's `WRITE` does the
retries and keeps count. Reads, Seal, Put Blob with `If-None-Match` and the head's CAS may
use the SDK's retries: a resent read is harmless, a resent seal is idempotent, and a resent
conditional create or CAS that already landed fails its condition, which the provider
resolves by reading.

## Invariants

The provider's invariants, which the new model ([Testing](#testing)) states and the
fuzzers check. AZ1–AZ5 are about the log's structure; AZ6 is the SPI's P1 to P6 as this
log meets them.

| | Invariant | Why it holds |
|---|---|---|
| AZ1 | Every segment the head names, except the last, is sealed, and its length is its `sealed_length` | Rotation seals before its CAS, and records the length it learned after the seal |
| AZ2 | The log is the head's checkpoint, then the frames of its live segments in ordinal order, each segment from its start (the first from the checkpoint's offset) to its length; every reader that starts from that head reads the same sequence, up to where it stops in the last segment | AZ1; sealed segments can't change; Append Block is atomic; reads go to the primary, where they are strongly consistent; and a reader parses frames only from known boundaries |
| AZ3 | An acknowledged append's frame is at its receipt's offset in the segment its receipt names, and that segment is in the log from the head that named it until a checkpoint covers it | The service's offset (the spike's check, for unsealed blobs); a segment leaves the head only by a trim, which needs a checkpoint past it (P5) |
| AZ4 | A segment's name is in some head only if its creator's CAS put it there, and a blob under that name is that creator's | Names carry a fresh nonce, and only the CAS winner's candidate is named. Nobody creates a named segment after the fact, so a stale writer can't bring back a deleted one. It gets a 404 or `BlobIsSealed`, and reads the head |
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
| `casbeforeseal` | rotation CASes the head before sealing *n* | AZ2: an append lands in *n* after a reader read it whole: a splice |
| `createonmissing` | a writer that finds the head's last segment missing creates it | AZ4, then AZ2: a stale writer brings back a trimmed segment, and a reader of an older head reads its record |
| `nosealedlength` | readers trust whatever length a sealed segment's GET returns | AZ2 under a short read |
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
| `fileio.catalog.log.location` | `<warehouse>/catalog.log` | the log's root |
| `fileio.catalog.log.append` | `blind` | `blind`, or `conditional` (the ablation: the same objects and frames, appends conditioned on the tail; `conditionalAppend` true) |
| `fileio.catalog.log.segment.max.bytes` | 0 (none) | a segment's size limit (`x-ms-blob-condition-maxsize`); small values make rotations frequent, for tests |
| `fileio.catalog.log.checkpoint.records` | 10,000 (provisional) | records after the checkpoint before a committer compacts; to be measured, as `max.append.count` was |
| `fileio.catalog.log.checkpoint.keep` | 2 | checkpoints kept besides the head's |
| `fileio.catalog.log.trim.grace.ms` | 60,000 | how long a segment out of the head stays before the sweep deletes it |

The credentials and the client are the fork's ADLS FileIO's (`AzureProperties`: a SAS, a
connection string, or the default credential), built into a Blob client on the primary
endpoint, with the SDK's retries off for appends and no secondary host for reads.

## Modules

- **`intentlog-azure`** (new): `AppendBlobLog`, over a narrow `AppendBlobStore` interface
  with two implementations:
  - `AzureAppendBlobStore`, on `azure-storage-blob`;
  - `InMemoryAppendBlobStore`, with fault hooks as `InMemorySharedLog`'s.

  The interface is the eight requests the log uses:
  - create an Append Blob with metadata if absent;
  - append a block blind, or at a position;
  - seal;
  - get a range, with the sealed flag, metadata and length;
  - get properties;
  - put a block blob if absent, or unconditionally;
  - put a block blob if it matches an ETag;
  - delete, and list.

  The module depends on `intentlog-core` and the Azure SDK only, not on Iceberg.
- **`iceberg-fileio-catalog`**: opening the log from the catalog's properties
  (`LogCatalog.openLog`), with the client built from `AzureProperties`.

## Testing

As every provider is tested (LOG.md "Testing a provider"), plus a model:

1. **The model, first.** Write `AzureLog.tla` before the Java, as `SharedLogCommit.tla` was:
   - the head and its CAS, segments with seal, rotation's steps with a crash between any
     two, the sweep, and readers holding older heads;
   - writers whose appends land, fail, or land late, before or after a seal.

   Its properties: AZ1–AZ5, and that the sequence every reader reads is a prefix of one log.
   It should refine `SharedLogCommit.tla`'s log, a single sequence with checkpoints and a
   trim point, so the commit protocol's proofs carry over unchanged. Each switch above must
   break a property (`check.py`).
2. **The contract.** `SharedLogContract` on `InMemoryAppendBlobStore`, and on Azurite if it
   supports Seal (an open question below), in both append modes.
3. **The fuzzer.** `CommitProtocol` on `AppendBlobLog` over the in-memory store
   (`TestAzureCommitProtocol`, 5,000 tries):
   - every fault the memory log has;
   - rotations forced by a tiny `segment.max.bytes`, with a crash at each step;
   - sweeps from stale heads;
   - appends that land late into a segment sealed since (they must fail);
   - readers whose heads are stale.

   Calibrate it with the switches above as mutants (`fuzz/mutants/azurelog.sh`).
4. **The catalog.** `TestLocalCatalog` and `TestLocalCatalogTransaction` in an `AZLOG` mode
   over the in-memory store; `TestCatalogFaultProperties` and `TestCatalogRaceProperties`
   drawing it as one more log.
5. **The cloud.** An integration test on a real account (`AzureAppendBlobLogIT`): the
   contract, and a run of the fuzzer's schedules with real requests.

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
`AzureAppendSpike` modes on the rmow-bench deploy:

| | Question | How to answer it |
|---|---|---|
| Q1 | Does Append Blob Seal work on an account with a hierarchical namespace? Do Get Blob and Get Blob Properties return `x-ms-blob-sealed`, and does an append after a seal fail with `409 BlobIsSealed`? The REST reference documents the code, but its account table doesn't list HNS accounts | seal a blob on the spike's account; append; read |
| Q2 | While appends land, does a ranged GET return only whole blocks, with its length and sealed flag consistent with its body? | readers parsing frames during the spike's blind levels |
| Q3 | How long do appends stall during a rotation at 32–64 writers? | the spike with a small `x-ms-blob-condition-maxsize` and the rotation protocol |
| Q4 | Where does a blob's rate stop growing? Blind appends had not stopped at 64 writers (3,113 landed/s, the client's CPU share 7.4%) | the spike at 128 and 256 writers, from two VMs, with rotation (Q3) so that no level fills its blob |
| Q5 | Do the rates hold at 64 KiB and across repeated runs, and on an account without a hierarchical namespace? | `--bytes 4096,65536 --reps 3`; a second account |
| Q6 | Does Azurite support Seal and `x-ms-blob-condition-maxsize`? | the contract on Azurite; if not, the in-memory store and the cloud test cover it |

## Later

- **Several records in one block** (group commit): one writer's batch, or a short wait in
  a client shared by many threads. That means fewer blocks, so rarer rotations, at the cost
  of latency. Each record keeps its own frame and id, so replay doesn't change.
- **One elected compactor**, by a lease on the head, so committers stop duplicating
  checkpoints at high rates. A lease would put a lease id on the head's CAS. Only if the
  measurements show duplicate checkpoints costing something.
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
