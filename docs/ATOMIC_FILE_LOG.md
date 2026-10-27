# The atomic-file log

`AtomicFileLog` (intentlog-fileio) keeps an intention log ([LOG.md](LOG.md))
in one object of a store with conditional writes, through the Iceberg fork's
`SupportsAtomicOperations`: S3 (by CAS on general-purpose buckets, and by
append too on Express One Zone), GCS (by CAS) and ADLS (both).
`FileIOCatalog` keeps its catalog this way, in the object
`<warehouse>/catalog`: the catalog file.

This document specifies the object's layout, and how the log's operations
and the commit loop map onto the store's conditional writes. The envelope of
the checkpoint and of each record is the log's
([LOG.md](LOG.md) §"The envelope"); what the catalog puts inside them is in
[SPEC.md](SPEC.md). For a byte-level walkthrough of real files, open
[format-explorer/index.html](format-explorer/index.html) in a browser.

## Layout

A fixed 8-byte header, a length-prefixed checkpoint, then the length-prefixed
records appended after it:

<!-- RFC frame generated with luismartingarcia/protocol:
protocol "Magic = LCAT:32,Format Version = 2:32,Checkpoint Length (varint):16,LogCheckpoint (protobuf - variable):48,Record Length (varint):16,LogRecord (protobuf - variable):48" -->
```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Magic = LCAT                         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Format Version = 2                      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|   Checkpoint Length (varint)  |                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|              LogCheckpoint (protobuf - variable)              |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     Record Length (varint)    |                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|                LogRecord (protobuf - variable)                |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

As a grammar (`framed` = a varint byte length followed by the bytes of that
protobuf message):

```abnf
log-file   = header checkpoint *record
header     = magic version
magic      = %x4C.43.41.54        ; "LCAT"
version    = u32                  ; big-endian, currently 2
checkpoint = framed               ; protobuf LogCheckpoint (log.proto)
record     = framed               ; protobuf LogRecord (log.proto): an intention or a GC record
framed     = varint *OCTET        ; varint length L, then L octets
```

```
Offset  Size   Field             Description
------  ----   -----             -----------
  0       4    magic             "LCAT" (0x4C 0x43 0x41 0x54)
  4       4    formatVersion     Big-endian int32, must be 2 (AtomicFileLog.FORMAT_VERSION)
                          Total: 8 bytes
```

Everything after the header is variable-length. The bit-grid widths of the
varint and protobuf regions above are schematic: a varint is 1 or more bytes,
and the protobuf bodies are tag-keyed, with no fixed offsets (the same caveat
applies to the frames in [SPEC.md](SPEC.md) §"Worked examples"). Each object
has exactly one checkpoint, and every record in it follows that checkpoint:
a record is never written without the checkpoint it replays onto.

A read (`AtomicFileLog.read`) reads the whole object, checks the magic and
the version, and splits the rest into the checkpoint and the records; it fails
on a file shorter than the header, another magic, another version, or a
truncated frame. Replay is the log's ([LOG.md](LOG.md) §"Replay").
`AtomicFileLog.encode` and `decode` turn a checkpoint and records into an
object's bytes and back, for tests and tools.

## Operations

| `SharedLog` | On the object |
|---|---|
| `capabilities()` | appends if the FileIO does (`supportsAppend()`), conditionally; replaces (fused); no separate checkpoints; reads its own writes |
| `read()` | the whole object, bound to the version it found |
| `probe()` | the object's version and length, without reading it; a tail that does not exist if the object does not |
| `append(record, tail)` | appends `[varint length][record]` at the tail's length, conditioned on its version (`Strategy.APPEND`). An unconditional append is refused |
| `replace(tail, checkpoint, records)` | writes `[header][checkpoint][records]` as the whole object, conditioned on the tail's version, or on the object's absence if the tail does not exist (`Strategy.CAS`) |
| `readThrough(receipt)` | reads the version the write wrote, so its record is among the records (P3) |
| `putCheckpoint`, `trim`, `readAfter` | not supported (a replace checkpoints), or the default (a fresh read) |

**Tails.** A tail is the object's version (an ETag or generation) and length.
The next conditional write names that version: an append at that offset (S3
Express's write offset, an Azure Append Blob's position), a replace of that
version (if-match, a generation precondition). A tail has always looked the
object up: `read()` and `probe()` take the length through the snapshot the next
write names (`getLength()`), which pins the version it saw on every store. The
fork's atomic writes take a snapshot that has seen no version to name an absent
object (a CAS from it creates the object only if none exists, with no lookup),
so a tail that had not looked would turn a replace into a failed create. A tail
continues an earlier one
if the object did not shrink: an append makes it longer, while a replace
usually makes it shorter. A replace that makes it longer reads as an append,
which costs a resend at most: the record is the same, and replay applies it
once.

**Signals.** Each write is prepared (`AtomicOutputFile.prepare`, which
computes the checksum the store checks) and then sent (`writeAtomic`). The
fork's exceptions map onto the log's ([LOG.md](LOG.md) §"Failures"):

| Store | Log |
|---|---|
| a failure in `prepare` | `LogNotSentException`: nothing was sent |
| `StorageThrottleException` (a 429, a 5xx, GCS's per-object update rate) | `LogThrottledException`. Mapped before `CASException`, its superclass |
| `SegmentLimitException` (S3 Express: 10,000 parts; an Azure Append Blob: 50,000 blocks) | `LogFullException` |
| `CASException`, `AppendException` | `LogConflictException` |
| any other failure | the write's outcome is unknown |

## Commits on one object

The commit loop ([LOG.md](LOG.md) §"The commit loop") appends a record when
it can, and replaces the object when it must:

- the FileIO cannot append (`supportsAppend()` is false: `GCSFileIO`);
- an append was refused as full (`SegmentLimitException`): the object holds as
  many writes as the store allows, whatever the thresholds say (Bug 66);
- the record is larger than one append to the store writes
  (`maxAppendBytes`, less the record's length prefix; ADLS's `AppendBlock`
  limit), whatever the thresholds say (Bug 71);
- or the compaction policy says the object is due: the read it staged on holds
  at least `max.append.count` intention records, or its record would take the
  object past `max.append.size` bytes.

A replace is a compaction. It writes `[header][checkpoint][gc][record]`: the
checkpoint of the state the writer read (after a lost race, one it read
again), a GC record at its watermark (none when GC is off), and its own
record, which replay verifies against the checkpoint and applies as it would
an append ([LOG.md](LOG.md) §"The committed set and GC"). Afterwards the
object holds one intention record.

| Property | Default | Purpose |
|---|---|---|
| `fileio.catalog.max.append.count` | 100 | Soft limit on intention records (GC records not counted): a writer that reads at least this many replaces instead of appending. Writers retrying a lost append race do not read the count again, so the log can run a few records past it. The stores bound it (S3 Express 10,000 parts, Azure Append Blob 50,000 blocks): an append past one is refused, and the writer compacts instead, so a count above the store's limit costs a refused append, not a stuck catalog. Measured best of 0, 100 and 1000 on S3 Express (design.md §"Choosing append vs CAS"). **0 replaces on every commit**, which an S3 general-purpose bucket needs |
| `fileio.catalog.max.append.size` | 16777216 | Soft target for the object's size. Larger objects take longer to read |
| `fileio.catalog.throttle.retry.min-wait-ms` | 200 | First wait before sending again a write that storage throttled ([LOG.md](LOG.md) §"The commit loop") |
| `fileio.catalog.race.retry.min-wait-ms` | 20 | Bound of the random wait after a lost race, before the writer reads or probes the object again ([LOG.md](LOG.md) §"The commit loop") |
| `fileio.catalog.committed.retention.ms` | 21600000 (6 h) | How long the committed set keeps an id ([LOG.md](LOG.md) §"The committed set and GC") |

**CAS-only mode.** A store that replaces conditionally but cannot append
needs every commit to replace. On GCS (a generation precondition) the log
forces it: `GCSFileIO.supportsAppend()` is false. On an S3 general-purpose
bucket (if-match) the setting `max.append.count=0` is required: `S3FileIO`
reports `supportsAppend()`, but appends at a write offset, which only S3
Express directory buckets accept. A replace that loses a race rebases on a
fresh read, so concurrent commits whose preconditions do not overlap all
succeed, as they would by append.

**Races.** No hint in the object says when to compact: either writer in a
race decides from the length of the live object and the record count of the
version it last read. After a lost append race the loop probes the object's
length without reading it. If the object grew (another append), it resends
the same record at the new length, and its record count is a lower bound,
which is fine for a soft limit. If it shrank (a replace), it reads the new
version. Writers configured with different thresholds disagree on when to
compact, but cannot corrupt the catalog: every write is conditional (P4).

**Creating the object.** `FileIOCatalog.initialize` calls
`CatalogLog.bootstrap`, which commits an intention setting the catalog's
conflict encoding (SPEC.md §"Conflict Encodings") to a log that does not
exist: a replace conditioned on the object's absence, writing the
checkpoint of the empty catalog, a GC record, and the bootstrap record,
whose id joins the committed set like any other. Writers may
bootstrap at once: one creates the object, and the others find it created.

## Schema evolution

The header's `formatVersion` must equal `AtomicFileLog.FORMAT_VERSION` (2).
Version 1 was the layout before the envelope, with the catalog's checkpoint
and transactions framed directly; a version-2 reader refuses it. Changes
inside the envelope are gated by `LogCheckpoint.min_reader_version`
([LOG.md](LOG.md) §"The envelope"), and changes to the catalog's messages by
`Checkpoint.min_reader_version` ([SPEC.md](SPEC.md) §"Schema Evolution").

## Key files

| File | Purpose |
|------|---------|
| `intentlog-fileio/src/main/java/org/intentlog/fileio/AtomicFileLog.java` | The layout; reads, appends and replaces through `SupportsAtomicOperations` |
| `intentlog-fileio/src/test/java/org/apache/iceberg/io/InMemoryAtomicFileIO.java` | An in-memory store that enforces conditional appends and replaces, with faults, segment limits and append size limits, for tests |
| `intentlog-core/src/main/java/org/intentlog/LogRuntime.java` | The commit loop |
| `intentlog-core/src/main/proto/log.proto` | The envelope |
