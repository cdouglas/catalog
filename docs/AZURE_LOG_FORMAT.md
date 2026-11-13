# The Azure append-blob log: format and protocol

This is the specification of `AppendBlobLog` (module `intentlog-azure`). It covers the objects
the log keeps in an Azure Blob Storage container, their bytes, and the requests that read and
write them. It is written from the code at fileio-catalog `5304227`, for review. Where the code
and [AZURE_LOG.md](AZURE_LOG.md) or `spec/tla/AzureLog.tla` disagree, the code is described
here and the difference is listed under [Discrepancies](#14-discrepancies).

## 1. Scope

**What this document fixes:**
- the names, types and lifetimes of the log's blobs ([§3](#3-objects));
- their bytes ([§4](#4-wire-formats));
- the store requests the log makes ([§5](#5-the-store-requests));
- the reader and writer algorithms ([§6](#6-reader-algorithms), [§7](#7-writer-algorithms));
- how a catalog opens the log ([§8](#8-how-a-catalog-finds-and-opens-the-log));
- the invariants, the failure mapping, concurrency, versioning and limits.

**What it relies on:**
- [LOG.md](LOG.md) defines the `SharedLog` SPI and its obligations P1-P6, the envelope (log.proto),
  replay and the commit loop. This log is one provider of that SPI. Its capabilities are
  `append`, `separateCheckpoints`, no `fusedReplace`, and `conditionalAppend` only in the
  ablation ([§7.2](#72-conditional-append-the-ablation)).
- [AZURE_LOG.md](AZURE_LOG.md) keeps the rationale (the measurements, why Seal and leases don't
  work, when to compact), the bug switches, the testing plan and its status. This document does
  not repeat the reasons.

**Conventions.**
- MUST and MUST NOT mark rules that correctness depends on. SHOULD marks rules the code follows
  for cost or for tidiness.
- "Inferred" marks a statement read from documentation or reasoning, not from code.
- Code is cited as `File.java:line` at `5304227`. The files are:

| Short name | Path |
|---|---|
| `AppendBlobLog.java` | `intentlog-azure/src/main/java/org/intentlog/azure/AppendBlobLog.java` |
| `AzureLogHead.java` | `intentlog-azure/src/main/java/org/intentlog/azure/AzureLogHead.java` |
| `Frames.java` | `intentlog-azure/src/main/java/org/intentlog/azure/Frames.java` |
| `AppendBlobStore.java` | `intentlog-azure/src/main/java/org/intentlog/azure/AppendBlobStore.java` |
| `AzureAppendBlobStore.java` | `intentlog-azure/src/main/java/org/intentlog/azure/AzureAppendBlobStore.java` |
| `InMemoryAppendBlobStore.java` | `intentlog-azure/src/main/java/org/intentlog/azure/InMemoryAppendBlobStore.java` |
| `LogRuntime.java` | `intentlog-core/src/main/java/org/intentlog/LogRuntime.java` |
| `AzureLogs.java`, `LogCatalogProperties.java`, `FileIOCatalog.java` | `iceberg-fileio-catalog/src/main/java/org/apache/iceberg/io/` |

## 2. Terms

| Term | Meaning |
|---|---|
| root | The log's location: a blob path prefix inside one container, without a trailing slash (`AppendBlobLog.java:152`). |
| head | The blob `<root>/head`. It names the log's live segments and its newest checkpoint. |
| segment | An Append Blob holding records. Segments have ordinals 0, 1, 2, ... |
| live segments | The segments the current head names. They have consecutive ordinals. |
| base | A segment's position in the log: the base of the segment before it plus that segment's closed length. Segment 0's base is 0. |
| position | A place in the log: a segment's base plus an offset in that segment. |
| L, the limit | A segment's size limit in bytes. A pad fills a closed segment to exactly L. |
| W, the writer limit | L − 32. Every record append carries W as its size condition. |
| frame | One Append Block's bytes: a record or a pad, with a trailer. |
| closed | A segment no append can land in again: at L, or at the block limit. |
| checkpoint | A block blob holding one log.proto `LogCheckpoint`, the state through a position. |
| log id | A 16-byte UUID that names one log. It is in the head, the checkpoint names, and segment metadata. |
| instance | One `AppendBlobLog` object. It caches the newest head it saw and the newest view it built. Several instances (processes) share one log. |

## 3. Objects

All names below are blob paths in the log's container. Nothing else under `<root>` belongs to
the log, except what a listing under `<root>/seg/`, `<root>/ckpt/` and `<root>/pages/` finds.

| Object | Name | Blob type | Created by | Changes after creation | Deleted by |
|---|---|---|---|---|---|
| head | `<root>/head` | block blob | log creation: Put Blob, `If-None-Match: *` (`AppendBlobLog.java:463`) | yes: Put Blob with `If-Match: <ETag read>` only (a rotation, a checkpoint, a trim) | never |
| segment | `<root>/seg/<ordinal, 20 decimal digits>-<nonce, 16 hex digits>` (`AppendBlobLog.java:188-191`) | Append Blob | Put Blob (Append Blob), `If-None-Match: *`, with its metadata in the same request (`AzureAppendBlobStore.java:164-178`) | Append Block only, at its end | the sweep, once no head can name it again ([§7.8](#78-sweep)) |
| checkpoint | `<root>/ckpt/<position, 20 decimal digits>-<log id, 32 hex digits>` (`AppendBlobLog.java:193-199`) | block blob | Put Blob, `If-None-Match: *` (`AppendBlobLog.java:408`, `:454`) | never | the sweep |
| page | `<root>/pages/<page name>` | block blob | Put Blob, unconditional (`AppendBlobLog.java:991-993`) | rewritten only with the same bytes (PageStore's contract: one name, one content) | the runtime (dropped pages, orphans), not this log's sweep |

Rules:
- **O1.** The head MUST change only by a Put Blob conditioned on the ETag of the version the
  writer read. No code path deletes it.
- **O2.** A segment MUST be created empty, with its metadata, under a name with a fresh nonce.
  No writer may create a segment under a name some head named, or recreate a missing one
  (AZ4).
- **O3.** A checkpoint MUST be created once. Two compactors at one position MUST put the same
  bytes (I11). A put that finds the name taken reads the blob and compares
  (`AppendBlobLog.java:408-413`).
- **O4.** The head stores each segment's and the checkpoint's **full blob path, root included**
  (`segmentName` and `checkpointName` prefix `root`). A log copied under another root still
  names the old root's blobs. See [OI-8](#13-open-issues-and-review-checklist).

## 4. Wire formats

### 4.1 The head

The head is one protobuf message, encoded with intentlog's hand-written proto codec
(`org.intentlog.wire.ProtoWire`). There is no `.proto` file. The schema below is the one
`AzureLogHead.java:36-49` gives, and `encode`/`decode` implement (`AzureLogHead.java:174-274`).

```protobuf
message AzureLogHead {
  int32  min_reader_version = 1;   // written as 1, first; required
  bytes  log_id             = 2;   // 16 bytes (UUID, most significant half first); required
  uint64 generation         = 3;   // 1 at creation, +1 at every CAS
  repeated Segment segments = 4;   // the live segments, oldest first; at least one
  CheckpointRef checkpoint  = 5;   // the newest checkpoint; required
}

message Segment {
  uint64 ordinal       = 1;        // its place: 0, 1, 2, ...
  string name          = 2;        // the blob path, root included; required
  uint64 closed_length = 3;        // its final length; absent while it is the open last segment
  uint64 limit         = 4;        // L; required, > 0
  uint64 base          = 5;        // its position: the base before it + that segment's closed_length
}

message CheckpointRef {
  bytes  through = 1;              // a position: 8 bytes, big-endian (§4.6); required
  string name    = 2;              // the checkpoint's blob path, root included; required
}
```

Field rules. The decoder checks only some of them; the column says which.

| Rule | Checked on decode |
|---|---|
| H1. `min_reader_version` is present and at most 1. A reader MUST refuse a larger one. | yes: an `IllegalStateException` (`AzureLogHead.java:221-226`). It is accepted at any field position, not only first. |
| H2. `log_id`, at least one segment, and the checkpoint are present. | yes (`AzureLogHead.java:225-227`) |
| H3. Each segment has a name and a limit above 0. | yes (`AzureLogHead.java:252-254`) |
| H4. Ordinals are consecutive, oldest first. | no. `byOrdinal` assumes it (`AzureLogHead.java:119-122`). |
| H5. Every segment but the last has `closed_length`; the last has none. | no |
| H6. Each segment's base is the previous base plus the previous closed length. | no. A reader checks the chain while it walks ([§6.1](#61-the-segment-walk)). |
| H7. The checkpoint's position lies in a live segment (`at(through)` is not null). | no. A reader that finds otherwise reads again. |
| H8. Unknown fields are skipped. | yes (`AzureLogHead.java:219`, `:249`, `:267`) |

`generation` orders heads within one instance's cache: an instance keeps a head it reads only
if its generation is higher than the cached one's, or the log id differs
(`AppendBlobLog.java:227-233`). It is not used for any condition on the store.

### 4.2 Segment metadata

Set by the Put Blob that creates the segment (`AppendBlobLog.java:201-204`):

| Key | Value |
|---|---|
| `intentlog_log_id` | the log id, 32 lowercase hex digits |
| `intentlog_segment` | the ordinal, in decimal |
| `intentlog_limit` | L, in decimal |
| `intentlog_format` | `1` |

A reader compares `intentlog_log_id` and `intentlog_segment` with the head's entry on every
segment GET, ignoring the keys' case (`AppendBlobLog.java:771`, `:829-841`). It does not read
`intentlog_limit` or `intentlog_format`. It takes L from the head.

### 4.3 Segment body: frames

Each Append Block writes exactly one frame. Append Block is atomic, so a segment's body is a
sequence of whole frames (`Frames.java:27-35`):

```abnf
segment  = *record-frame [pad]
record-frame = length body trailer
length   = varint            ; unsigned LEB128 of n, 1 to 5 bytes, n > 0 and < 2^31
body     = n OCTET           ; one log.proto LogRecord, opaque to this log
pad      = %x00 padlen filler trailer
padlen   = 4OCTET            ; u32, little-endian: the filler's length
filler   = padlen %x00
trailer  = ordinal crc
ordinal  = 8OCTET            ; u64, little-endian: the segment's ordinal
crc      = 4OCTET            ; u32, little-endian: CRC32C of every byte of the frame before it,
                             ; the ordinal included
```

| Constant | Value | Source |
|---|---|---|
| trailer | 12 bytes | `Frames.java:39` |
| smallest pad (`PAD_MIN`) | 1 + 4 + 12 = 17 bytes | `Frames.java:42` |
| reserve (L − W) | 32 bytes | `Frames.java:45` |
| largest record overhead (`MAX_OVERHEAD`) | 5 + 12 = 17 bytes | `Frames.java:48` |
| a record's frame length | `varintSize(n) + n + 12` | `Frames.java:84-86` |
| smallest L | 32 + 17 + 1 = 50 bytes | `AppendBlobLog.java:147-150` |

Rules:
- **F1.** A record is never empty, so a length of 0 marks a pad (`Frames.java:61-63`, `:115`).
- **F2.** A pad is at least 17 bytes. The writer limit leaves at least 32 bytes below L, so a
  pad always fits ([§7.3](#73-close)).
- **F3.** A pad is the last frame of a segment a pad closed, and ends at exactly L. The parser
  does not enforce that a pad is last ([OI-3](#13-open-issues-and-review-checklist)); the
  writer limit makes it so.
- **F4.** A reader parses only from a known frame boundary: a segment's start, the checkpoint's
  position, or a tail it read. Each frame's CRC and ordinal MUST match, and the frames MUST end
  exactly at the bytes read. Any failure is a `FrameCheckException`, and the read starts again
  (`Frames.java:107-148`).
- **F5.** The trailer carries no offset and no incarnation (unlike `AtomicFileLog`'s format 3).
  A blind appender can't know its offset in advance. The position comes from the service's reply
  (`x-ms-blob-append-offset`) or from the reader's own parse.

Worked numbers, at the default L = 64 MiB = 67,108,864: W = 67,108,832. A record of 1,000 bytes
has a 2-byte length, so its frame is 1,014 bytes.

### 4.4 Checkpoint blob

The whole blob is one log.proto `LogCheckpoint` as the runtime encodes it. The log never decodes
it, except at creation, to learn the log id (`AppendBlobLog.java:442`). Its `through` field MUST
equal the position in its name and in the head's `CheckpointRef.through`. The runtime checks
`through` against the view's start (`LogRuntime.java:566-584`), so a mismatch fails the read
with `LogGapException`.

### 4.5 Page blobs

Each page is one blob whose bytes are what the runtime or the catalog put (a log.proto
`CommittedPage`, or a catalog snapshot page). The log treats them as opaque. `location(name)`
gives the blob's URL so that a worker can read it as a file (`AzureAppendBlobStore.java:414-417`).

### 4.6 Positions, tails, receipts and views

**Positions** are 8 bytes, a big-endian signed 64-bit integer, never negative
(`AzureLogHead.java:277-286`). A position is a base plus an offset, so the end of a closed
segment and the start of the next are one position.

The other three exist only in memory. The runtime sees only `LogTail.position()` and
`LogView.start()` as bytes.

| Object | Fields | Rules |
|---|---|---|
| `Tail` (`AppendBlobLog.java:865-911`) | root, log id, exists, position, ordinal (the segment the position was read in), bytes | `continues(e)` iff same root, both exist, same log id, and `position ≥ e.position` (`:893-896`). A tail of a log that does not exist has no log id and position 0. |
| `AzReceipt` (`:917-928`) | log id, ordinal, segment name, base, offset (from the reply), frame length, CRC, the record | position = base + offset |
| `View` (`:931-979`) | tail, checkpoint bytes (or none), the checkpoint's position, records, each record's position, start | start = the checkpoint's position for a whole read; the tail it continues for a read of records alone |

`Tail.bytes` is computed differently by path. See [M9](#14-discrepancies).

## 5. The store requests

`AppendBlobLog` talks to the container only through `AppendBlobStore` (`AppendBlobStore.java`).
Its two implementations are `AzureAppendBlobStore` (the Azure SDK) and `InMemoryAppendBlobStore`
(tests and benchmarks; the account's semantics, with fault hooks).

| Store method | Azure request | Conditions | Result | Failures it maps |
|---|---|---|---|---|
| `createAppendBlob(name, metadata)` | Put Blob (`x-ms-blob-type: AppendBlob`) | `If-None-Match: *` | true; false if the name exists | |
| `appendBlock(name, block, maxSize, pos)` | Append Block | `x-ms-blob-condition-maxsize` if `maxSize > 0`; `x-ms-blob-condition-appendpos` if `pos ≥ 0` | the reply's `x-ms-blob-append-offset` | 412 `MaxBlobSizeConditionNotMet` and 409 `BlockCountExceedsLimit`: `Full`; 412 `AppendPositionConditionNotMet`: `PositionMismatch`; 404: `NotFound` |
| `read(name, from)` | Get Blob with `Range: bytes=<from−1>-` (or no range from 0), the first byte dropped | none | the bytes from `from` to the end, the blob's length (`Content-Range` total), committed block count, metadata, ETag; null on 404 | 416: Get Blob Properties, then no bytes with the length if the blob ends at or before `from`, else read again (up to 3 times) |
| `properties(name)` | Get Blob Properties | none | length, committed block count, metadata, last modified; null on 404 | |
| `putIfAbsent(name, bytes)` | Put Blob (block blob, one request) | `If-None-Match: *` | true; false if the name exists (409 `BlobAlreadyExists` or 412) | |
| `put(name, bytes)` | Put Blob | none | | |
| `putIfMatch(name, bytes, etag)` | Put Blob | `If-Match: <etag>` | the new ETag; null on 412 or 404 | |
| `delete(name)` | Delete Blob | none | 404 ignored | |
| `list(prefix)` | List Blobs, flat, with metadata | | names and last-modified times; directory placeholders (`hdi_isfolder`) skipped | |

Rules for any store:
- **ST1. One try per request.** A store MUST NOT resend a request itself. An SDK resend of a
  timed-out Append Block can land a record twice and report only the second copy
  (AZURE_LOG.md "Failures"; model switch `sdkretry`). `AzureAppendBlobStore` builds its client
  with `maxTries` 1 and download retries off (`AzureAppendBlobStore.java:86-87`, `:131-142`).
  A client passed to its constructor must be built the same way; the class cannot check it.
- **ST2. The primary endpoint only.** A store MUST NOT read from a geo-secondary endpoint,
  whose reads lag. `refuseSecondary` refuses a `-secondary` host or emulator account
  (`AzureAppendBlobStore.java:150-161`). Reads on the primary are strongly consistent (an Azure
  guarantee, inferred from its documentation).
- **ST3. A read returns one snapshot of the blob.** The bytes MUST run from `from` to the length
  reported. The code relies on this and does not check it ([OI-2](#13-open-issues-and-review-checklist)).
- **ST4. Failure classes** (`AppendBlobStore.java:30-35`, `AzureAppendBlobStore.java:449-465`):
  - `Full`, `PositionMismatch`, `NotFound`, `Refused` (other 4xx): nothing landed.
  - `Throttled` (503, 500 `OperationTimedOut`, 500 `InternalError`): a write may have landed and
    may be sent again; a read failed.
  - Anything else: a write's outcome is unknown; a read failed.
- The block limit is `maxBlocks()` (50,000 on Azure). The largest block is `maxBlockBytes()`:
  4 MiB for a client pinned to a service version before 2022-11-02, else 100 MiB
  (`AzureAppendBlobStore.java:419-426`).

## 6. Reader algorithms

A reader holds a head: its instance's cached one, or a fresh GET. "GET head" below is always a
fresh read, and a newer one replaces the cache (`readHead`, `AppendBlobLog.java:211-219`). A
head that does not exist makes every read but `probe` fail with `IllegalStateException`
(`AppendBlobLog.java:235-241`).

`readAttempts` is 8 by default (`AppendBlobLog.java:98-100`). "Restart" means the current
attempt ends and the next one begins. Store exceptions other than those named are not retried:
they fail the read, and the runtime treats the log as unreadable.

### 6.1 The segment walk

`walk(H, seg, offset)` reads from `offset` in `seg` on, through the segments after it while each
is closed (`AppendBlobLog.java:765-818`):

```
walk(H, seg, off):
  records = []; positions = []; bytes = 0
  loop:
    b = read(seg.name, off)
    if b is null                                          -> restart   # gone: trimmed and swept
    if b.metadata's log id != H.log_id or its ordinal != seg.ordinal -> restart
    if b.length < off                                     -> restart   # read short of the start
    if seg.closed_length is set and b.length != seg.closed_length -> restart   # AZ1
    for f in parse(b.bytes, from off, ordinal seg.ordinal):         # FrameCheckException -> restart
      if f is a pad: padded = true
      else: records += f.body; positions += seg.base + f.offset; bytes += |f.body|
    end = b.length
    closed = seg.closed_length is set or padded or end >= seg.limit or b.blocks >= maxBlocks
    if not closed: return (records, position seg.base + end, ordinal seg.ordinal)
    next = H.segment(seg.ordinal + 1)
    if next is none:
      H = GET head
      again = H.segment(seg.ordinal)
      if again is none                                    -> restart   # trimmed under the read
      if again.closed_length is set and != end            -> restart
      next = H.segment(seg.ordinal + 1)
      if next is none: return (records, position seg.base + end, ordinal seg.ordinal)
    if next.base != seg.base + end                        -> restart   # the chain must join
    seg = next; off = 0
```

Notes:
- **R1.** A segment at its limit is closed even when a read from its end sees no pad. Only a pad
  reaches L, since every record append stops at W (`AppendBlobLog.java:789-792`; the
  `padunseen` mutant).
- **R2.** The walk ends a view in the last segment at whatever length the GET returned. Any
  prefix is a legal read (P1).
- **R3.** The segment's end is `b.length`, the reported total, not `off + |b.bytes|`. See
  [OI-2](#13-open-issues-and-review-checklist).

### 6.2 `read()`

```
read():
  repeat readAttempts times:
    H = GET head
    ck = read(H.checkpoint.name, 0);   if null      -> next attempt
    seg = H.at(H.checkpoint.through);  if null      -> next attempt
    w = walk(H, seg, H.checkpoint.through - seg.base);  restart -> next attempt
    v = View(checkpoint ck, records w, start H.checkpoint.through,
             tail(position w.end, ordinal w.ordinal, bytes |ck| + w.bytes))
    keep v as this instance's newest view if it is at or past the one kept
    return v
  fail: UncheckedIOException ("could not be read in 8 attempts")
```

`H.at(p)` (`AzureLogHead.java:129-141`) is the live segment a read from `p` starts in: the one
`p` lies in, or at the end of a closed segment, the next (or that segment, if it is last). It is
null if `p` is before the first live segment or past a closed last segment's end.

### 6.3 `readAfter(since)`

The records after a tail this instance or another returned (`AppendBlobLog.java:615-654`):

```
readAfter(t):
  repeat readAttempts times:
    H = the cached head; GET head if none is cached or its log id differs from t's
    if H.log_id != t.log_id: throw TrimmedException
    if H.checkpoint.through > t.position: return read()         # a newer checkpoint covers t
    seg = H.segment(t.ordinal)
    if seg is none:
      H = GET head; seg = H.segment(t.ordinal)
      if seg is none or H.checkpoint.through > t.position: return read()
    w = walk(H, seg, t.position - seg.base)
    if w restarted:
      if GET head names no segment t.ordinal: throw TrimmedException
      next attempt
    return View(no checkpoint, records w, start t.position,
                tail(position w.end, ordinal w.ordinal, bytes t.bytes + w.bytes))
  fail: unreadable
```

When nothing changed, this is one ranged GET of the tail's segment. The runtime answers a
`TrimmedException` with a whole `read()` (`LogRuntime.java:1726-1734`).

### 6.4 `readThrough(receipt, wait)`

A view whose records include the receipt's record at its position (P3;
`AppendBlobLog.java:657-731`):

```
readThrough(r, wait):
  deadline = now + wait
  for attempt = 0, 1, ...:
    v = through(r, fresh = attempt > 0)            # FrameCheckException: remember it, go on
    if v is not null:
      if v holds a record at position r.position:
        if frame(that record, r.ordinal) differs from r in length or CRC:
          throw IllegalStateException ("AZ3: another record is at r")
        keep v; return v
      if v.tail.position >= r.position + r.length:
        throw IllegalStateException ("AZ3: no record starts at r")
    if now > deadline and attempt + 1 >= readAttempts: throw LogLagException

through(r, fresh):
  H = fresh ? GET head : the cached head (GET head if none)
  if H.log_id != r.log_id: H = GET head; if still different: throw TrimmedException
  if H.checkpoint.through > r.position:
    H = GET head; if still: throw TrimmedException   # every checkpoint kept holds the record
  K = this instance's newest kept view
  if K is from the same log and K's checkpoint position == H.checkpoint.through:
    if not fresh and r carries its record and K.tail.position == r.position:
      return K + [r.record at r.position], tail r.position + r.length      # no request
    m = readAfter(K.tail)
    return m has no checkpoint ? K extended by m : m
  return the whole read of H (null: the caller tries again)
```

- **RT1.** The no-request case is sound because positions are unique in a log. If the reply put
  the record exactly at the end of a view this instance holds, nothing landed between them.
- **RT2.** The loop does not pause between attempts ([OI-7](#13-open-issues-and-review-checklist)).
  On Azure, reads are consistent, so the first attempt normally finds the record.
- **RT3.** A `TrimmedException` from `through` or from `readAfter` propagates. The runtime then
  treats the write as ambiguous and settles it with a fresh read (LOG.md "Failures").

### 6.5 `probe()`

The tail without its records (`AppendBlobLog.java:743-754`). The runtime uses it to find
whether the log exists, and the conditional ablation uses it after a lost race:

```
probe():
  H = GET head;  if none: return Tail(exists false, position 0)
  p = properties(H.last.name);  length = p ? p.length : 0
  return Tail(position H.last.base + length, ordinal H.last.ordinal,
              bytes position - H.checkpoint.through)
```

## 7. Writer algorithms

### 7.1 Blind append

`append(record, null)` (`AppendBlobLog.java:247-316`):

```
append(record):
  if |record| > maxRecordBytes: throw LogNotSentException
  H = the cached head (GET head if none; IllegalStateException if the log does not exist)
  rotations = 0
  loop:
    seg = H.last
    frame = frame(record, seg.ordinal)
    try:
      off = appendBlock(seg.name, frame, maxSize = seg.limit - 32)        # no position condition
      return AzReceipt(H.log_id, seg.ordinal, seg.name, seg.base, off, |frame|, crc(frame), record)
    on Full:
      F = GET head
      if F.last.ordinal == seg.ordinal:
        if rotations++ >= maxRotations (4): throw LogFullException
        F = rotate(F, seg)       # Throttled or UncheckedIOException inside: LogThrottledException
      H = F
    on NotFound:                 # a stale head: the segment was trimmed and swept
      F = GET head
      if F names seg (same ordinal and same name): throw IllegalStateException ("AZ5")
      H = F
    on Throttled: throw LogThrottledException
    on Refused:   throw LogNotSentException
    anything else: propagates (the outcome is unknown)
```

Rules:
- **W1.** Every record append MUST carry `maxSize = W` of the segment it targets, as recorded in
  the head. This is the fence that closes a segment (model switch `nolimit`).
- **W2.** The receipt's offset MUST be the reply's `x-ms-blob-append-offset`, never a length read
  before the append (model switch `offsetfromlength`).
- **W3.** A writer that finds a segment missing MUST read the head and MUST NOT create the
  segment (AZ4; model switch `createonmissing`).
- **W4.** An append that failed with `Full` or `NotFound` landed nothing (P6), so it may go to
  another segment.

`maxRecordBytes` is `min(maxBlockBytes − 32, L − 32) − 17` (`AppendBlobLog.java:174-178`). The
first term keeps a pad within one Append Block: a pad is at most the failed frame's size plus
31 bytes ([§7.3](#73-close)), so a frame of at most `maxBlockBytes − 32` bytes leaves room. The
second term makes every record fit an empty segment.

### 7.2 Conditional append (the ablation)

`append(record, tail)`, only when the log was built with `conditional`
(`fileio.catalog.log.append=conditional`; `AppendBlobLog.java:249-251`, `:260-274`). The same
objects and frames as blind appends. The differences:

```
  at = tail;  reread = false
  loop:
    seg = H.last
    if at.log_id == H.log_id and at.ordinal > seg.ordinal and not reread:
      H = GET head; reread = true; continue        # the tail was read through a newer head
    if not holdsPosition(at, H, seg): throw LogConflictException   # the log moved on
    appendBlock(seg.name, frame, maxSize = W, appendPosition = at.position - seg.base)
    on PositionMismatch: throw LogConflictException
    on Full: as the blind append, then go round (the check above runs again)

holdsPosition(at, H, seg) =        # Bug 89 (moved on, same position)
  at.log_id == H.log_id and
  (at.ordinal == seg.ordinal or (at.ordinal < seg.ordinal and seg.base == at.position))
```

- **CA1.** The condition is on the tail's **position**, not its segment. A conditional append may
  land in a later segment that starts exactly where the tail was: one closed at the block limit
  with no pad, or one reached after a trim.
- **CA2.** After a rotation that needed a pad, the next segment starts past the tail's position,
  so the append fails as a conflict and the runtime reads again
  ([OI-12](#13-open-issues-and-review-checklist)).
- **CA3.** P4 holds: two conditional appends at one tail target one position. Only one segment can
  start there, and the service's position condition admits at most one append at it.

### 7.3 Close

`close(seg)` makes sure no append lands in `seg` again, and returns its final length
(`AppendBlobLog.java:374-393`):

```
close(seg):
  loop:
    p = properties(seg.name);  if null: return -1
    if p.length >= seg.limit or p.blocks >= maxBlocks: return p.length      # already closed
    try:
      appendBlock(seg.name, pad(seg.limit - p.length, seg.ordinal),
                  maxSize = seg.limit, appendPosition = p.length)
      return seg.limit
    on PositionMismatch or Full: continue        # the segment grew, or met the block limit
    on NotFound: return -1
```

- **CL1.** The pad MUST carry both conditions: the position it was sized for, and L. It can land
  only at that length, and then the segment is exactly L long.
- **CL2.** The loop ends. Each lost pad means the segment grew. Record appends cannot take it
  past W, and blocks are bounded. Once it has no room, a pad lands or the block limit closes it.
- **CL3.** A pad is at least 32 bytes. A writer closes only after its own append failed `Full`,
  so the length is at most W = L − 32. Every pad is at least 32 ≥ 17 bytes, and at most the
  failed frame's size plus 31 bytes.
- **CL4.** A throttled pad may have landed. `close` lets `Throttled` propagate. The append fails
  as `LogThrottledException`, and the runtime's next send finds the segment full and closes it
  from a fresh look.

### 7.4 Rotation

`rotate(H, full)`, where `full` is the head's last segment (`AppendBlobLog.java:336-365`):

```
rotate(H, full):
  x = close(full);  if x < 0: return GET head
  next = Segment(ordinal full.ordinal + 1, a fresh name, open,
                 limit = this instance's configured L, base = full.base + x)
  if not createAppendBlob(next.name, metadata(H.log_id, next.ordinal, next.limit)):
    throw IllegalStateException                         # a fresh name was taken
  loop:
    H2 = H with full closed at x and next appended, generation + 1
    if putIfMatch(head, H2, H.etag) succeeded: cache H2; return H2
    H = GET head
    if H.last.name == next.name: return H               # our CAS landed; its reply said not
    if H.last.ordinal != full.ordinal: delete next (best effort); return H
    # the head changed for another reason (a checkpoint, a trim): CAS again on it
```

Order rules (each has a model switch; AZURE_LOG.md "Rotation"):
- **RO1. Close before the CAS** (`casbeforeclose`). A reader that sees a segment other than
  the last then knows it can't grow.
- **RO2. Create before the CAS.** Every segment a head names exists until a trim drops it. No
  one creates a segment because it was missing (W3).
- **RO3.** A new segment's limit is the rotating writer's configured L, not the old segment's.
  Writers with different L settings produce segments of different limits. Readers and writers
  MUST take L from the head.

### 7.5 Creating the log

`putCheckpoint(t, bytes)` with `t.exists()` false creates the log (`AppendBlobLog.java:441-474`).
The runtime calls it with the empty state's checkpoint, which carries a fresh log id
(`LogRuntime.java:1584-1608`):

```
create(checkpoint):
  id = the checkpoint's log id
  W = GET head
  if W exists:
    C = read(W.checkpoint.name)
    if W.log_id != id or W.checkpoint.through != 0 or C is null or C != checkpoint:
      throw LogConflictException                           # another writer's log
    return                                                 # this creation, retried
  if not putIfAbsent(<root>/ckpt/00000000000000000000-<id>, checkpoint):
    if the existing blob differs: throw LogConflictException
  createAppendBlob(segment 0 under a fresh name, metadata)   # its result is not checked
  if not putIfAbsent(<root>/head, head{id, generation 1, [segment 0, base 0, open],
                                       checkpoint (0, its name)}):
    if GET head's log id != id: throw LogConflictException
  Throttled: LogThrottledException;  Refused: LogNotSentException
```

The runtime then reads the log. If the log id it reads is not its own, the commit is rejected
`CREATE_LOST`.

### 7.6 `putCheckpoint`

For a log that exists (`AppendBlobLog.java:400-434`):

```
putCheckpoint(t, bytes):
  name = <root>/ckpt/<t.position, 20 digits>-<t.log_id>
  if not putIfAbsent(name, bytes):
    E = read(name); if E exists and E != bytes: throw IllegalStateException ("I11")
  loop:
    H = GET head
    if H.log_id != t.log_id or H.checkpoint.through >= t.position or H.at(t.position) is null:
      return                                     # a newer checkpoint is named, or t was trimmed
    if putIfMatch(head, H with checkpoint (t.position, name), H.etag): cache it; return
  Throttled: LogThrottledException;  Refused: LogNotSentException
```

- **CK1.** The head's checkpoint only moves forward.
- **CK2.** A checkpoint MUST lie at a frame boundary. Tails are frame ends, and the runtime puts
  checkpoints at a view's tail.
- **CK3.** A put that finds its name taken, then finds the blob gone, goes on to the head. It
  cannot name a missing blob. The sweep deletes this log's checkpoints only below the head's,
  so the head's checkpoint is past `t` and the loop returns. This argument is worth a
  reviewer's check ([§13](#13-open-issues-and-review-checklist)).

### 7.7 `trim`

`trim(t)` drops the segments before the one `t`'s position lies in (`AppendBlobLog.java:483-513`):

```
trim(t):
  loop:
    H = GET head
    if H.log_id != t.log_id: return                        # no sweep either
    if H.checkpoint.through < t.position: throw IllegalStateException ("P5")
    keep = H.at(t.position)
    if keep is null or keep.ordinal <= H.first.ordinal: T = H; break
    if putIfMatch(head, H without segments before keep.ordinal, H.etag): T = that head; break
  sweep(T), ignoring any failure
```

- **TR1 (P5).** A trim MUST NOT drop a segment unless the head's checkpoint lies at or after
  the trim position. The trim never drops the checkpoint's own segment, or the last segment.
- **TR2.** The trim deletes no blob. Readers holding older heads can read trimmed segments
  until the sweep deletes them.

### 7.8 Sweep

`sweep(H)` runs only at the end of `trim`, with the head the trim ended on
(`AppendBlobLog.java:509`, `:522-552`). `LogRuntime.sweep` deletes orphaned pages only, and does
not reach this method:

```
sweep(H):
  cutoff = now - trimGraceMs                                  # default 60,000 ms
  for each blob listed under <root>/seg/:
    o = the ordinal in its name (-1 if it does not parse)
    if o >= 0 and H does not name it and o <= H.last.ordinal and last_modified <= cutoff:
      delete it (errors ignored)
  for each blob listed under <root>/ckpt/:
    (position, id) = parse its name
    if id != H.log_id and last_modified <= cutoff: delete it
    else if id == H.log_id and position < H.checkpoint.through: candidate
  delete every candidate but the checkpointKeep (default 2) with the highest positions  # no grace
```

- **SW1 (AZ5).** The sweep MUST NOT delete a segment any current or later head names. A segment
  with an ordinal past `H.last` may be a candidate that still wins its CAS, so it is kept.
- **SW2.** The grace is measured from the blob's last-modified time, not from when it left the
  head. A segment closed long before its trim is deleted at the first sweep after the trim
  ([M6](#14-discrepancies)). Deletion is safe at any time (AZ5); the grace only saves readers
  a restart.
- **SW3.** Checkpoints older than the head's are deleted without the grace. A reader that finds
  its head's checkpoint gone reads again ([§6.2](#62-read)).
- **SW4.** Segments are not matched by log id. A foreign segment (another log's, at this root)
  is deleted only if its ordinal is at or below `H.last`.

### 7.9 Compaction, as the runtime drives it

`LogRuntime.compact()` (`LogRuntime.java:257-295`; LOG.md "Compaction"):

1. Append a GC record blind, sent again while it is throttled. Then `readThrough` it and replay.
2. If pages are due, put each page, append its `PageOut`, and `readThrough` the last.
3. `putCheckpoint(view.tail, checkpoint of the replay)` ([§7.6](#76-putcheckpoint)).
4. `trim(view.tail)`, which sweeps ([§7.7](#77-trim)).
5. Delete the pages the GC records dropped, and sweep orphaned pages.

A writer runs it after a commit landed, when the policy says the log is due and the writer's
drawn point is reached (`compactApart`, `LogRuntime.java:724-742`). For this log the policy is
`CompactionPolicy.share` (`LogCatalogProperties.java:355-361`): compact when the bytes since the
checkpoint (or since the last GC record) reach `checkpoint.share` (1.0) of the checkpoint's
size, but not below `checkpoint.min.bytes` (1 MiB), and at `max.append.size` bytes or
`checkpoint.records` (10,000) records regardless. A failed compaction is logged; the commit
stands.

### 7.10 A commit, as the runtime drives it

For orientation; LOG.md "The commit loop" is the specification:
- The runtime appends the commit's record blind, then reads through it
  (`LogRuntime.java:1393-1398`, `:1650-1667`). Replay decides whether the record applied where
  it landed. There is no lost race.
- A throttled send is sent again. A throttled send that may also have landed is remembered as
  a possible duplicate. Replay applies a record id at most once.
- `LogFullException` (after 4 rotations) makes the runtime read afresh and append again. It
  cannot replace this log (`LogRuntime.java:1332-1335`, `:1400-1407`).
- Any other write failure leaves the write ambiguous, and a read settles it.

## 8. How a catalog finds and opens the log

A catalog finds the log **from its configuration alone**. `FileIOCatalog.openLog` opens this log
when `fileio.catalog.log` is `azure-append` (`FileIOCatalog.java:89-93`). There is no discovery
step. Nothing in the warehouse points to the log, and the log does not point to the warehouse.

| Property | Default | Meaning |
|---|---|---|
| `fileio.catalog.log` | `atomic-file` | `azure-append` selects this log |
| `fileio.catalog.log.location` | none: required (`AzureLogs.java:96-100`) | `https://<account>.blob.core.windows.net/<container>/<path>`. The container URL is the store; `<path>` is the root |
| `fileio.catalog.log.append` | `blind` | `blind` or `conditional` (the ablation) |
| `fileio.catalog.log.segment.max.bytes` | 64 MiB | L for segments this writer creates |
| `fileio.catalog.log.checkpoint.share` | 1.0 | [§7.9](#79-compaction-as-the-runtime-drives-it) |
| `fileio.catalog.log.checkpoint.min.bytes` | 1 MiB | [§7.9](#79-compaction-as-the-runtime-drives-it) |
| `fileio.catalog.log.checkpoint.records` | 10,000 | replaces `max.append.count` for this log |
| `fileio.catalog.log.checkpoint.keep` | 2 | checkpoints the sweep keeps besides the head's |
| `fileio.catalog.log.trim.grace.ms` | 60,000 | the sweep's grace ([SW2](#78-sweep)) |
| `adls.auth.shared-key.account.name`, `.key` | none: required | the account key (`AzureLogs.java:40`, `:106-110`) |

Not configurable from the catalog: `maxRotations` (4), `readAttempts` (8). The clock is the
system's.

Other facts a reviewer should know:
- **Credentials.** Only an account key. The store also takes a `TokenCredential`, but the
  catalog does not wire one (`AzureAppendBlobStore.java:126-129`).
- **The tables' files** stay in the catalog's FileIO. `FileIOCatalog` still requires
  `warehouse` and loads a `SupportsAtomicOperations` FileIO (default `GCSFileIO`), even though
  the log does not use it (`FileIOCatalog.java:73-88`).
- **Creation.** The catalog's bootstrap creates the log with its first record if the head does
  not exist (`LogCatalog.java:205`, `LogRuntime.bootstrap`).
- **Which log a catalog keeps is not recorded anywhere** ([OI-1](#13-open-issues-and-review-checklist)).

## 9. Invariants

AZ1-AZ5 are about the log's structure. AZ6 is the SPI's P1-P6 as this log meets them. Each
argument below rests on the algorithms above.

**AZ1. Every segment the head names except the last is closed, and its length is its
`closed_length`.**
- A head gains a closed segment only through a rotation's CAS, which records the length
  `close` returned ([§7.4](#74-rotation)). Trims drop segments from the front only. Creation
  names one open segment.
- `close` returns a length only after (a) its pad landed, leaving exactly L bytes, or (b) Get
  Blob Properties showed length ≥ L or the block limit. In case (b), length ≥ L can only be a
  pad's doing, because record appends stop at W.
- After either, no append lands. A record append needs length + frame ≤ W < L. A pad needs its
  exact position and would pass L. At the block limit, nothing lands. This holds for appends
  still in flight too: they land, if at all, under the conditions they were sent with (W1).
- So the length the CAS records is final, and the close happened before the CAS (RO1).

**AZ2. The log is the head's checkpoint, then the frames of its live segments in ordinal
order, each segment from its start (the first from the checkpoint's offset) to its length, pads
left out. Every reader that starts from that head reads the same sequence, up to where it stops
in the last segment.**
- Closed segments never change (AZ1). The last segment grows only at its end, by atomic Append
  Blocks.
- A reader checks on every segment:
  - the metadata: log id and ordinal;
  - a closed segment's length;
  - the chain of bases;
  - each frame's CRC and ordinal;
  - that frames end at the bytes read.
- Reads go to the primary endpoint, where they are consistent (ST2).
- The reader trusts that a GET's body runs to the length it reports (ST3).

**AZ3. An acknowledged append's frame is at its receipt's offset in the segment its receipt
names, before that segment's `closed_length` once it has one. That segment stays in the log
until a checkpoint covers it.**
- The offset is the service's (W2).
- An acknowledged block ends at or before W. A pad starts after it, and a block-limit close
  happens at a length after it. So the block lies before the closed length.
- A segment leaves the head only by a trim, which needs the head's checkpoint at or past the
  trim position. The trim keeps the checkpoint's segment (TR1).
- `readThrough` checks the frame's length and CRC at the receipt's position ([§6.4](#64-readthroughreceipt-wait)).

**AZ4. A segment's name is in some head only if its creator's CAS put it there, and the blob
under that name is that creator's.**
- Names carry a fresh 64-bit nonce. A rotation fails if its name is taken.
- Only a rotation, which uses a fresh name, and creation create segments. A writer that finds a
  segment missing re-reads the head (W3).
- This holds with high probability, not certainty: the nonce is from `ThreadLocalRandom`, and
  creation does not check that segment 0's name was free
  ([OI-11](#13-open-issues-and-review-checklist)).

**AZ5. The sweep never deletes a segment the current head names. What it deletes was either
covered by the head's checkpoint or never part of the log.**
- Heads add segments only at the end: a CAS adds ordinal k + 1 only on a head whose last is k.
  Heads remove segments only from the front.
- So the sweep's head H is a prefix-trimmed ancestor of every later head. A later head names an
  ordinal ≤ `H.last` only under the name H gives it.
- The sweep deletes only unnamed blobs at or below `H.last`. These are trimmed segments
  (covered by a checkpoint) and losing candidates (never named).
- It deletes this log's checkpoints only below H's checkpoint. Later heads name a checkpoint at
  or after H's (CK1).
- The append path asserts AZ5: a 404 on a segment the fresh head still names throws
  (`AppendBlobLog.java:300-305`).
- The argument assumes the head is never deleted or replaced by another log at the same root.

**AZ6. P1-P6.**

| | How this log meets it |
|---|---|
| P1 | One order (AZ2). A view's `start()` is the checkpoint's position for a whole read, and the tail it continues for a read of records alone. The runtime checks both (`LogRuntime.java:566-595`). |
| P2 | An acknowledged write is durable (Azure's guarantee, inferred from its documentation). |
| P3 | `readThrough` returns a view holding the record at the receipt's position, checked by length and CRC. Otherwise it throws `TrimmedException`, `LogLagException` or `IllegalStateException`. |
| P4 | Conditional ablation only: CA3. |
| P5 | TR1. |
| P6 | `LogFullException` is thrown only after appends that failed `Full`. `LogNotSentException` is thrown for an oversized record or a refused request. Neither landed. |

## 10. Failure mapping

| Response | Request | Store exception | `AppendBlobLog` signal | Runtime |
|---|---|---|---|---|
| 201, with `x-ms-blob-append-offset` | Append Block | none | a receipt | read through it |
| 412 `MaxBlobSizeConditionNotMet`, 409 `BlockCountExceedsLimit` | Append Block | `Full` | none: read the head, rotate if it still names the segment last, send again. After 4 rotations: `LogFullException` | read afresh, append again |
| 404 | Append Block | `NotFound` | none: read the head, send again. If the head still names the segment: `IllegalStateException` (AZ5) | unknown outcome |
| 412 `AppendPositionConditionNotMet` | Append Block, conditional | `PositionMismatch` | `LogConflictException` | lost race: read, retry |
| 503, 500 `OperationTimedOut`, 500 `InternalError` | a record append | `Throttled` | `LogThrottledException` | back off, send again (up to 8 sends) |
| `Throttled` or a transport failure during a rotation | rotation's requests | `Throttled`, `UncheckedIOException` | `LogThrottledException` (`AppendBlobLog.java:291-295`) | back off, send again |
| other failures during a rotation (`Refused`, 5xx, `IllegalStateException`) | rotation's requests | as thrown | propagate unchanged | unknown outcome; the record did not land |
| other 4xx | any write | `Refused` | `LogNotSentException` | reject `NOT_SENT`, unless an earlier send may have landed |
| 412 on `If-Match` | head CAS | null from `putIfMatch` | none: re-read the head, act again | |
| 409 / 412 on `If-None-Match: *` | create, checkpoint put | false | compare with what is there ([§7.5](#75-creating-the-log), [§7.6](#76-putcheckpoint)) | |
| timeout, reset, other exception | a write | as thrown | propagates | unknown outcome; a read settles it |
| 404 on a GET | a read | null | restart the read, or `TrimmedException` | `readAfter`: a whole read; `readThrough`: ambiguous, settled by a read |
| frame check failure | a read | `FrameCheckException` | restart the read | |
| 8 failed attempts | a read | | `UncheckedIOException` | the log could not be read |
| any failure | `putCheckpoint`, `trim` in a compaction | as mapped | as mapped | the compaction is abandoned; the commit stands |

## 11. Concurrency walk-throughs

**Racing rotations.** Writers A and B both get `Full` on segment n and read a head naming n last.
1. Both run `close(n)`. One pad lands at length x. The other pad fails `PositionMismatch`, its
   writer looks again, sees length L, and returns L. Both now hold x' = L.
2. Each creates its own candidate n + 1, with a distinct nonce.
3. Both CAS from the same ETag. One wins, say A.
4. B re-reads the head. Its last ordinal is n + 1, not n, so B deletes its candidate and appends
   to A's segment.

If B crashes before deleting its candidate, the sweep deletes it later: its ordinal n + 1 is at
or below the head's last, and the head does not name it.

**A pad racing appends.** Writer A's frame does not fit (length + frame > W), but smaller frames
from others still fit.
1. A reads length x and sends a pad of L − x at position x.
2. B's small append lands first, so A's pad fails `PositionMismatch`.
3. A reads the length again and sends a smaller pad.

This repeats until a pad lands or the block limit closes the segment. Records that landed
before the pad are in the log. Records sent after it fail `Full` and go to the next segment.

**A stale head, writer side.** Writer A caches a head naming segment n last, but the log has
moved to n + 2 and n was trimmed and swept.
1. A's append to n gets 404.
2. A reads the head, which no longer names n, and appends to n + 2.

If n is still there but closed, A gets `Full`. A then reads the head, which names a later last
segment, and appends there without rotating.

**A stale head, reader side.** Reader R holds a head naming segments k..n.
- If k was swept, R's GET gets 404 and the read restarts from a fresh head.
- If R walks past the end of n, which it finds closed, and its head names no n + 1, R reads the
  head. It then continues into n + 1, checking that n + 1's base equals n's base plus the length
  read.
- If n's closed length in the fresh head differs from what R read, R restarts.

**A create race.** Writers A and B find no head.
1. Each puts a checkpoint at position 0 under its own log id. The names differ, so both succeed.
2. Each creates a segment 0 under its own nonce.
3. Both put the head with `If-None-Match: *`. One wins, say A.
4. B reads A's head, sees a different log id, and throws `LogConflictException`. B's runtime
   reads the log, finds A's log id, and rejects B's commit `CREATE_LOST`.

B's checkpoint is another log's, and is swept after the grace. B's segment 0 is unnamed at
ordinal 0, and is swept after the grace. Both sweeps happen only at the first trim.

**A sweep racing a reader.** A compactor trims segments k..m − 1 and sweeps them. A reader
holding the older head is in the middle of reading k.
- If the reader's GET completed, it has a consistent snapshot of a closed segment. Its view is
  still a slice of the log (AZ2). The runtime folds it, and the next read starts from the new
  checkpoint.
- If the GET comes after the delete, it gets 404, and the read restarts.

The same holds for the reader's checkpoint blob: older checkpoints beyond `keep` are deleted
without a grace, so a reader may restart on them.

**A late append.** An append to n times out in flight, and its writer moves on. If n is closed
before the append lands, the append fails W1's size condition and nothing lands. If it lands
first, it is before the pad, inside the log.
- The runtime counted the timed-out send as possibly landed, so a copy in the log is known.
- Replay applies the record id once.

**Two compactors.** Two compactors checkpoint at the same position. Both put the same bytes
(I11), and the second finds the name taken with equal bytes. One CAS names the checkpoint; the
other sees the head's checkpoint at or past its own and stops. At different positions, the
head moves to the later one. A compactor whose CAS finds a newer checkpoint does nothing.

## 12. Versioning and limits

**Versioning.**

| Field | Written | Read |
|---|---|---|
| head `min_reader_version` | 1 | refused if above 1 (H1) |
| segment `intentlog_format` | `1` | **never checked** ([OI-4](#13-open-issues-and-review-checklist)) |
| frames | no version byte | |
| checkpoint, records, pages | log.proto's own `min_reader_version` fields, checked by the runtime | |

A change to the frame layout or segment semantics has, today, no field a reader checks.
Bumping the head's `min_reader_version` is the only way to stop old readers.

**Limits.**

| Limit | Value | Where |
|---|---|---|
| Blocks per Append Blob | 50,000 | `AppendBlobStore.java:97-100`; a segment at it is closed |
| Bytes per Append Block | 4 MiB before service version 2022-11-02, 100 MiB since | `AzureAppendBlobStore.java:419-426` |
| L (segment limit) | default 64 MiB; at least 50 bytes | `AppendBlobLog.java:98-100`, `:147-150` |
| W (writer limit) | L − 32 | `AzureLogHead.java:67-69` |
| Largest record | `min(maxBlockBytes − 32, L − 32) − 17`: 67,108,815 bytes at the defaults | `AppendBlobLog.java:174-178` |
| Rotations per append | 4 of this writer's own | `AppendBlobLog.java:285` |
| Read attempts | 8 | `AppendBlobLog.java:99` |
| A checkpoint's size | one Put Blob (inferred: 5,000 MiB on current service versions) | `AzureAppendBlobStore.java:357-364` |
| A request's time | 60 s, one try | `AzureAppendBlobStore.java:82` |

At the defaults a segment holds 50,000 records of up to about 1.3 KiB each before the block
limit closes it, or fewer, larger records before L does.

## 13. Open issues and review checklist

### Open issues

| | Issue | Evidence |
|---|---|---|
| OI-1 | **Nothing records which log a catalog keeps.** A client that opens the same warehouse with the default `atomic-file` finds no `<warehouse>/catalog`, sees an empty catalog, and its first commit creates a second, independent catalog. The tables' metadata files would be shared between the two. Equally, two clients with different `log.location`s split the catalog. The design has no marker in the warehouse, and the log does not name its warehouse. For review: should the warehouse hold a marker naming the log, which every client checks? | `FileIOCatalog.java:73-95`, `AzureLogs.java:96-111` |
| OI-2 | **A reader trusts that a ranged GET's body runs to the reported length.** The walk takes the segment's end from `b.length`, the `Content-Range` total, and parses only the bytes returned. Nothing checks `from + bytes == length`. If the service ever returned fewer bytes with the full total, the reader would skip frames silently. The in-memory store's short read keeps the two consistent, so the fuzzer cannot produce the case. AZURE_LOG.md's Q2 asks this of Azure. A one-line check would turn it into a restart. | `AppendBlobLog.java:777-788`, `AzureAppendBlobStore.java:267-285`, `InMemoryAppendBlobStore.java:328-334` |
| OI-3 | **The head's structure is not validated on decode** (H4-H7), and the parser does not enforce that a pad is last (F3). A head written by a faulty writer would be trusted. Readers' checks catch some cases (the base chain, closed lengths). | `AzureLogHead.java:201-232`, `Frames.java:107-142` |
| OI-4 | **`intentlog_format` and `intentlog_limit` are never read.** The format metadata cannot stop an old reader. | `AppendBlobLog.java:829-832` |
| OI-5 | **The sweep runs only inside a trim.** An idle log keeps unnamed segments and others' checkpoints until its next compaction. The grace counts from last-modified time (SW2). | `AppendBlobLog.java:509`, `:528-529` |
| OI-6 | **The append loop is bounded only in this writer's own rotations.** A writer that keeps finding segments full because others rotate goes round without limit (inferred: practically bounded by load). | `AppendBlobLog.java:262-297` |
| OI-7 | **`readThrough` retries without a pause** until `wait` has passed and 8 attempts were made. | `AppendBlobLog.java:663-687` |
| OI-8 | **The head stores root-prefixed names** (O4). A log cannot be moved or copied to another root without rewriting the head. | `AppendBlobLog.java:188-195` |
| OI-9 | **A checkpoint is one Put Blob**, so its size is bounded by the service's single-request limit, and by memory. Inferred, not tested. | `AzureAppendBlobStore.java:357-364` |
| OI-10 | **Foreign segments past the head's last ordinal are never swept** (SW4). This arises only if another log once lived at this root. | `AppendBlobLog.java:526-532` |
| OI-11 | **Nonces come from `ThreadLocalRandom`, and creation ignores a taken segment-0 name.** A collision is improbable, but AZ4 rests on it. | `AppendBlobLog.java:188-191`, `:461` |
| OI-12 | **The conditional ablation pays a conflict after every padded rotation** (CA2). This matters when comparing it with blind appends. | `AppendBlobLog.java:271-274`, `:325-329` |
| OI-13 | **Only account-key credentials** reach the store from a catalog. | `AzureLogs.java:96-111` |
| OI-14 | **`LogFullException` cannot be answered with a replace** on this log. The runtime appends again, and LOG.md's failure table says "replaces". | `LogRuntime.java:1332-1335`, `:1400-1407`; LOG.md "Failures" |

### Review checklist

1. Are AZ1's two closing cases exhaustive? Can any request other than a pad reach L, for example
   a block appended without `maxsize` by some other tool?
2. Is the base chain check (`next.base == seg.base + end`) enough to rule out a splice when a
   reader joins a head read after the segment?
3. CK3: can `putCheckpoint` ever CAS the head to name a checkpoint blob that is gone?
4. AZ5's argument assumes heads only append and trim. Does any path build a head otherwise:
   `rotated` when `last()` differs, or a resent CAS?
5. Is `holdsPosition` (Bug 89 (moved on, same position)) right for a tail read in a segment the
   head has since trimmed?
6. Is the no-request `readThrough` (RT1) sound when several threads share one instance's kept
   view?
7. Does any Azure behaviour break ST3 (OI-2): a ranged GET shorter than its `Content-Range`
   total, or a block count inconsistent with the body?
8. Is a sweep with no grace on old checkpoints (SW3) acceptable for long reads of a large
   checkpoint?
9. OI-1: what should a catalog do when the warehouse says one log and its configuration says
   another?
10. Are the limits right for the newest service version: block size, blob size, and the Put Blob
    size for checkpoints?

## 14. Discrepancies

Places where the code differs from [AZURE_LOG.md](AZURE_LOG.md), `spec/tla/AzureLog.tla`, the
model's README, or the code's own comments. Nothing here was changed.

| | Says | Code | Evidence |
|---|---|---|---|
| M1 | AZURE_LOG.md: the head's schema of record is `intentlog-azure/src/main/proto/azure_log.proto`. | No such file. The schema is a Javadoc comment, and the codec is hand-written. | AZURE_LOG.md:174; `AzureLogHead.java:32-49` |
| M2 | AZURE_LOG.md: `min_reader_version` is "required, first". | Required, but accepted at any position. | AZURE_LOG.md:178; `AzureLogHead.java:221-226` |
| M3 | AZURE_LOG.md: `generation` is "for traces and tests only". | It orders heads in an instance's cache (`adopt`). | AZURE_LOG.md:180; `AppendBlobLog.java:227-233` |
| M4 | AZURE_LOG.md: segment and checkpoint names are `seg/...` and `ckpt/...`. | The head stores full blob paths, root included. | AZURE_LOG.md:187, :195; `AppendBlobLog.java:188-195` |
| M5 | AZURE_LOG.md: "A reader checks it [the metadata] against the head's entry on every segment it reads." | Only the log id and ordinal are checked; limit and format are not. | AZURE_LOG.md:218-221; `AppendBlobLog.java:829-832` |
| M6 | AZURE_LOG.md and `LogCatalogProperties`: `trim.grace.ms` is "how long a segment out of the head stays before the sweep deletes it". | The grace counts from the blob's last-modified time. A segment last written long before its trim gets no grace. | AZURE_LOG.md:561; `LogCatalogProperties.java:316-317`; `AppendBlobLog.java:523`, `:528-529` |
| M7 | AZURE_LOG.md: "The sweep runs after a trim, and from `LogRuntime.sweep`." | `AppendBlobLog.sweep` is package-private and called only from `trim`. `LogRuntime.sweep` deletes orphaned pages only. | AZURE_LOG.md:431; `AppendBlobLog.java:509`, `:522`; `LogRuntime.java:500-510` |
| M8 | `sweep`'s Javadoc: every deletion waits for the grace. AZURE_LOG.md: "checkpoints and segments of other log ids" are deleted. | Older checkpoints of this log are deleted with no grace. Segments are never matched by log id. | `AppendBlobLog.java:516-521` vs `:543-551`, `:526-532`; AZURE_LOG.md:437-438 |
| M9 | AZURE_LOG.md: a tail's `bytes()` is "the bytes of records after the head's checkpoint". | It depends on the path. `read()`: the checkpoint's size plus record bodies (as the SPI defines it, "checkpoint included"). `readAfter`: the earlier tail's bytes plus bodies. The no-request `readThrough`: plus the frame length. `probe()`: position minus checkpoint position (frames and pads, no checkpoint). No path this log takes feeds it to the compaction policy (inferred: `compactApart` uses replica counts). | AZURE_LOG.md:278-279; `AppendBlobLog.java:609-610`, `:646`, `:719-720`, `:752-753`; `LogTail.java` `bytes()`; `LogRuntime.java:726-727` |
| M10 | AZURE_LOG.md: `maxRecordBytes` is "the smaller of the service version's Append Block limit and the writer limit of an empty segment, less the frame's overhead". | 32 is also taken off the block limit, so that a pad fits one block. | AZURE_LOG.md:296; `AppendBlobLog.java:174-178` |
| M11 | AZURE_LOG.md: `<root>` example `abfss://catalog@account.dfs.core.windows.net/...`. | The location must be an `https` Blob URL (`BlobUrlParts`). The doc's own Configuration table says so. | AZURE_LOG.md:166-167 vs :554; `AzureLogs.java:96-104` |
| M12 | `LogCatalogProperties.LOG_LOCATION` Javadoc: "default: the warehouse's `catalog.log`". | No default: a missing location is an `IllegalArgumentException`. | `LogCatalogProperties.java:271-275`; `AzureLogs.java:97-100` |
| M13 | AZURE_LOG.md: `read()` retries "up to `maxThrottleAttempts` times". | `Options.readAttempts` (8). A missing checkpoint and a frame check failure also count. | AZURE_LOG.md:297; `AppendBlobLog.java:98-100`, `:577-591` |
| M14 | AZURE_LOG.md `readAfter`: if the head no longer names the segment, `TrimmedException`, and the runtime reads whole. | `readAfter` itself returns a whole `read()` when the head's checkpoint is past the tail or the segment is not named. It throws `TrimmedException` only when a walk failed and a fresh head no longer names the segment. | AZURE_LOG.md:298; `AppendBlobLog.java:628-643` |
| M15 | AZURE_LOG.md `readThrough`: extend "the newest view ... that ends at or before r ... through r's frame". | The kept view must start from the head's current checkpoint, whatever its end. It is extended by a `readAfter` that reads to the segment's end. | AZURE_LOG.md:299; `AppendBlobLog.java:711-731` |
| M16 | AZURE_LOG.md `trim`: drop "the segments before `through`'s ordinal". | Before the segment `at(through.position)` returns. At a closed segment's end, that is the next segment, so the closed one is dropped too. | AZURE_LOG.md:304; `AppendBlobLog.java:495-500` |
| M17 | AZURE_LOG.md creation: "position (0, 0)"; "With the same log id, it is this creation, retried." | Position 0 (one number). A retry also needs the head's checkpoint still at 0 with equal bytes, else it reports a conflict. The runtime then still adopts its own log by id, so the outcome is right. | AZURE_LOG.md:347-352; `AppendBlobLog.java:443-452`; `LogRuntime.java:1597-1607` |
| M18 | AZURE_LOG.md: an I11 violation "throws `IllegalStateException` naming both". | The message names the position only. | AZURE_LOG.md:257-258; `AppendBlobLog.java:411-412` |
| M19 | AZURE_LOG.md failures table: a 404 on Append Block re-reads the head and sends again. | Also an `IllegalStateException` (AZ5) if the fresh head still names the segment. | AZURE_LOG.md:490; `AppendBlobLog.java:300-305` |
| M20 | AZURE_LOG.md: after `maxRotations` moves, `LogFullException`. | Only this writer's own rotations count. Following others' rotations is unbounded (OI-6). | AZURE_LOG.md:301, :489; `AppendBlobLog.java:285` |
| M21 | AZURE_LOG.md conditional row: "a full segment is moved on from as by a blind append". | It moves on, then conflicts unless the close needed no pad (CA2). | AZURE_LOG.md:302; `AppendBlobLog.java:271-274` |
| M22 | `AzureLog.tla` models no log creation (Init creates it), no conditional appends, no head-CAS or checkpoint-put faults, no checkpoint deletion, no grace, no sweep of other log ids, no `readThrough` without a request, and no bases. Its sweep deletes segments only. | The code has all of these. Coverage of them rests on the Java fuzzers and tests. | `spec/tla/AzureLog.tla` (Init; `Fits` used with `ap = -1` by writers; `Sweep`; `RSeg`/`RNext`) |
| M23 | `AzureLog.tla`: a lost rotation CAS deletes its candidate only when the head names ordinal o. | Java also deletes when the head's last is any ordinal other than the full one's, and treats a head whose last is its own candidate as its CAS having landed. | `AzureLog.tla` `Cas`; `AppendBlobLog.java:355-363` |
| M24 | spec/tla/README.md "The Azure log": `offsetfromlength` "is equivalent there". | AZURE_LOG.md says it has been killed since 2026-11-10, when `readThrough` began trusting the reply's offset. The two docs disagree; the code matches AZURE_LOG.md (RT1). | spec/tla/README.md:1120-1121; AZURE_LOG.md:661-665 |
| M25 | LOG.md "Providers": the table lists no Azure log. | `AppendBlobLog` is a provider. | LOG.md:959-966 |
