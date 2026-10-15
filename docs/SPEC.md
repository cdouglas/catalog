# ProtoCatalogFormat Specification

This document specifies the protobuf-based catalog format implemented by
`ProtoCatalogFormat`, the only catalog format used by `FileIOCatalog`. It uses
a checkpoint + append-log architecture encoded as protobuf wire format.

For inline-table extensions, see:
- [SPEC_TM.md](SPEC_TM.md) — inline table metadata (field 13 of `Checkpoint`,
  `CreateTableInline` / `UpdateTableInline` actions, `TableMetadataDelta`).
- [SPEC_ML.md](SPEC_ML.md) — inline manifest lists (fields 7–8 of
  `InlineTable`, `AddManifestDelta` / `RemoveManifestDelta` updates).

For an interactive byte-level walkthrough of these structures, open
[format-explorer/index.html](format-explorer/index.html) in a browser.

## Overview

A catalog file is a single object stored in cloud storage. It encodes the full
state of the catalog — namespaces, tables, and their properties — using
protobuf messages. It supports two mutation strategies:

- **Append**: append a length-prefixed transaction record (offset-validated).
- **CAS** (compare-and-swap): atomically replace the entire file
  (ETag-validated).

Both strategies rely on cloud-storage conditional writes
(`SupportsAtomicOperations`).

The codec (`ProtoCodec`) implements protobuf wire format manually — it does
not use `protoc`-generated classes. The encoding follows standard protobuf
conventions (varints, field tags, length-delimited fields) and is wire-
compatible with `catalog.proto`. Any protobuf tool given the `.proto` file
can decode the output.

## File Layout

A fixed 8-byte header, a length-prefixed checkpoint, then a series of
length-prefixed transaction records appended on top of it:

<!-- RFC frame generated with luismartingarcia/protocol:
protocol "Magic = LCAT:32,Format Version = 1:32,Checkpoint Length (varint):16,Checkpoint (protobuf - variable):48,Record Length (varint):16,Transaction (protobuf - variable):48" -->
```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Magic = LCAT                         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Format Version = 1                      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|   Checkpoint Length (varint)  |                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|                Checkpoint (protobuf - variable)               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     Record Length (varint)    |                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|               Transaction (protobuf - variable)               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

As a grammar (`framed` = a varint byte-length followed by the bytes of that
protobuf message):

```abnf
catalog-file = header checkpoint *record
header       = magic version
magic        = %x4C.43.41.54        ; "LCAT"
version      = u32                  ; big-endian, currently 1
checkpoint   = framed               ; protobuf Checkpoint
record       = framed               ; protobuf Transaction
framed       = varint *OCTET        ; varint length L, then L octets
```

The header is 8 bytes fixed. Everything after the header is variable-length,
using varint length prefixes for each record. There is no separate
committed-transaction-ID block; the checkpoint message carries that set
internally. The bit-grid widths for the varint and protobuf regions above are
schematic: a varint is 1+ bytes, and the protobuf bodies are tag-keyed with no
fixed offsets (the same caveat applies to the worked-example frames below).

### Header

```
Offset  Size   Field             Description
------  ----   -----             -----------
  0       4    magic             "LCAT" (0x4C 0x43 0x41 0x54)
  4       4    formatVersion     Big-endian int32, must be 1
                          Total: 8 bytes
```

## Protobuf Schema

Defined in `src/main/proto/catalog.proto`, package `iceberg.catalog`.

### Checkpoint

Written on compaction (CAS). Contains the full materialized catalog state.

```protobuf
message Checkpoint {
  bytes  catalog_uuid              = 1;   // 16 bytes, UUIDv7
  int32  next_namespace_id         = 2;
  int32  next_table_id             = 3;
  repeated Namespace          namespaces             = 10;
  repeated Table              tables                 = 11;  // pointer-mode tables
  repeated NamespaceProperty  namespace_properties   = 12;
  repeated InlineTable        inline_tables          = 13;  // inline-mode tables
  CommittedTransactions       committed_transactions = 20;  // compressed UUIDv7 set
}

message CommittedTransactions {
  fixed64 max_timestamp_ms = 1;   // largest timestamp in the set
  // Per entry, descending order, back-to-back inside one packed bytes
  // field: varint delta_ms (gap from previous) || 10 random bytes
  // (rand_a + rand_b + 6-bit pad).
  bytes   packed_entries   = 2;
}
```

A table ID appears in either `tables` (pointer mode) or `inline_tables`
(inline mode), never both.

The `committed_transactions` sub-message exploits the structure of UUIDv7
to compress the dedup set. Entries are sorted descending by UUID (= by
timestamp), the largest timestamp is stored once as `max_timestamp_ms`,
and each entry's varint gap-from-previous and 10 bytes of random
material (the 12-bit `rand_a` plus 62-bit `rand_b`, with the fixed
4-bit version and 2-bit variant nibbles dropped on encode and
reconstructed on decode) are interleaved back-to-back inside a single
`packed_entries` bytes blob. For typical clustered commit traffic this
saves ~30% per entry vs the older `repeated bytes` form.

The interleaved layout lets `ProtoCatalogFile.containsTransaction` walk
the wire bytes in a single linear pass with no per-call heap
allocation: the catalog file holds the inner-message bytes directly
(no materialized `HashSet<UUID>`), and re-encoding to a fresh checkpoint
is a byte copy when no commits have been added since decode.

All entries must be UUIDv7; non-v7 IDs are rejected at the dedup-set
entry point (`ProtoCatalogFile.Builder.addCommittedTransaction`) and
skipped at read-time replay (`Transaction.verify` returns false). See
[UUID_V7_BENCH_RESULTS.md](UUID_V7_BENCH_RESULTS.md) for the encoding
study that informed this choice.

#### Committed-set retention (GC)

The dedup set in `Checkpoint.committed_transactions` grows monotonically
across compactions. Without a retention policy, even with v7 compression
the section's size scales linearly with the catalog's total commit
history. The retention policy below trims old entries at compaction
time while preserving the dedup guarantee inside a configurable window.
See [GC.md](GC.md) for the implementation rationale.

**Premise: UUIDv7 timestamps approximate commit time.** Transaction IDs
are minted in `ProtoCodec.Transaction.from()` (called from `Mut.commit`)
using `UuidV7.newUuidV7()`, which embeds `System.currentTimeMillis()`
in the high 48 bits. Because the mint happens at commit-submission
time — not at transaction-begin time — a long-running transaction
does not get a stale timestamp; a transaction that ran for a week and
then commits has a UUID timestamp from the moment of commit, not from
a week prior. Retention can therefore be expressed in wall-clock terms
without disadvantaging slow workloads.

**Wire format addition.** The `CommittedTransactions` message gains a
new field:

```protobuf
message CommittedTransactions {
  fixed64 max_timestamp_ms              = 1;
  bytes   packed_entries                = 2;
  // Highest UUIDv7 timestamp that was dropped during the most recent
  // compaction. Implicitly 0 (no GC has occurred yet) when absent.
  // Monotonically non-decreasing across compactions.
  fixed64 highest_dropped_timestamp_ms  = 3;
}
```

**GC algorithm at compaction.** At every compaction (any CAS-mode
commit, including the always-CAS path when `maxAppendCount=0`):

```
function compactCheckpoint(oldCheckpoint, logTxns, retentionMs):
    nowMs        = System.currentTimeMillis()
    watermarkMs  = nowMs - retentionMs

    // 1. Inherited committed-set: keep only entries above the watermark.
    keptInherited = []
    droppedHighest = oldCheckpoint.highest_dropped_timestamp_ms
    for entry in decode(oldCheckpoint.packed_entries):
        if uuidv7_timestamp(entry) > watermarkMs:
            keptInherited.append(entry)
        else:
            droppedHighest = max(droppedHighest, uuidv7_timestamp(entry))

    // 2. Log transactions are NEVER dropped — they all become part of
    //    the new compacted checkpoint, regardless of timestamp. A log
    //    entry whose UUIDv7 timestamp is older than the watermark
    //    (clock skew, late retry, etc.) survives this compaction; it
    //    only becomes eligible for GC at the *next* compaction.
    newCommitted = mergeSortDescending(keptInherited, logTxns.map(txn => txn.id))

    return CommittedTransactions(
        max_timestamp_ms             = newCommitted[0].timestamp or 0,
        packed_entries               = encode(newCommitted),
        highest_dropped_timestamp_ms = droppedHighest)
```

The merge step preserves the streaming-friendly property: both
`keptInherited` and `logTxns` are already sorted descending by
UUID, so a single linear merge produces the new packed_entries body
without random-access materialization. The single-pass rewrite that
fuses decode + filter + merge into one walk is implemented in
`ProtoCodec.rewriteCommittedBytes`.

**Edge case: highest dropped can exceed max retained.** It is possible
(though rare) for `highest_dropped_timestamp_ms` to be *more recent*
than `max_timestamp_ms` of the same checkpoint. This happens when the
inherited committed-set had its newest entries below the watermark
(all dropped) and the log contributed only entries whose timestamps
are even older — typically a clock-skewed writer or a heavily-retried
old commit. The two timestamps must be tracked independently.

**Query semantics: tri-state.** With GC in place, `containsTransaction`
is no longer a clean boolean. A query for UUID `q` against
`CommittedTransactions(packed_entries=P, highest_dropped=H)` resolves
to one of:

| Outcome | Condition | Meaning |
|---|---|---|
| `COMMITTED` | `q ∈ P` | the transaction landed |
| `ABSENT` | `q ∉ P` and `timestamp(q) > H` | the transaction was never committed |
| `UNKNOWN` | `q ∉ P` and `timestamp(q) <= H` | may have been GC'd; cannot distinguish from never-committed |

For the dedup-replay path (`readInternal`'s idempotency check), the
querying transaction was just appended within the current append
window, so its timestamp is always above any reasonable watermark and
the result is always definitive. The tri-state distinction matters for
the writer-confirmation use case: a writer that comes back to confirm
its own commit after the retention window must be prepared to receive
`UNKNOWN` and decide its own policy (assume committed, retry, escalate).

The recommended public API:

- `boolean containsTransaction(UUID)` — `true` iff `COMMITTED`. Existing
  callers (replay) continue to use this.
- `TxnDedupStatus dedupStatus(UUID)` — returns `COMMITTED` / `ABSENT`
  / `UNKNOWN`. New API for writers that care about the GC ambiguity.
- `long highestDroppedTimestampMs()` — exposed for callers building
  bespoke logic.

**Configuration knob.**

| Property | Default | Purpose |
|---|---|---|
| `fileio.catalog.committed.retention.ms` | `21_600_000` (6h) | Minimum age before a committed-txn UUID becomes eligible for GC. Should comfortably exceed the longest plausible retry window for a writer learning its outcome. Setting to `Long.MAX_VALUE` disables GC. Setting to `0` would GC aggressively but is not recommended — would routinely produce `UNKNOWN` for any post-CAS query. |

A 6-hour default is pessimistic by design: it bounds the `CommittedTransactions`
section size to `retention × commit-rate` even for CAS-only catalogs
that compact on every commit, while leaving a generous window for
writers to confirm. For a 1-commit/sec workload with 6h retention the
section caps at ~21,600 entries (~250 KB compressed); for 100/sec it
caps at ~2.16M entries (~25 MB compressed) — still bounded, predictable,
and recoverable from the writer side via the `UNKNOWN` signal.

**Clock-skew note.** Watermark comparisons use the compactor's local
clock against UUIDv7 timestamps minted by other writers. Wall-clock
skew between writers translates directly into GC-window jitter: a
writer with a clock 10 minutes behind has its UUID timestamps appear
10 minutes "older," shrinking its effective retention by 10 minutes.
Keep writer clocks NTP-synced. This is a deployment requirement, not a
correctness invariant — a wildly skewed clock can cause spurious
`UNKNOWN` results but cannot violate idempotency for in-window writers.

### Catalog Entities

```protobuf
message Namespace {
  int32  id        = 1;
  int32  version   = 2;
  int32  parent_id = 3;       // 0 = root
  string name      = 4;       // single-level name, not full path
}

message Table {
  int32  id                = 1;
  int32  version           = 2;
  int32  namespace_id      = 3;
  string name              = 4;
  string metadata_location = 5;
}

message NamespaceProperty {
  int32  namespace_id = 1;
  string key          = 2;
  string value        = 3;
}
```

For `InlineTable`, see [SPEC_TM.md](SPEC_TM.md).

### Transaction

Appended atomically to the log region.

```protobuf
message Transaction {
  bytes           transaction_id = 1;    // UUIDv7, 16 bytes
  repeated Action actions        = 3;
}

message Action {
  oneof action {
    CreateNamespace         create_namespace          = 1;
    DropNamespace           drop_namespace            = 2;
    SetNamespaceProperty    set_namespace_property    = 3;
    RemoveNamespaceProperty remove_namespace_property = 4;
    CreateTable             create_table              = 5;
    DropTable               drop_table                = 6;
    UpdateTableLocation     update_table_location     = 7;
    ReadTable               read_table                = 8;
    UpdateTableInline       update_table_inline       = 9;   // see SPEC_TM
    CreateTableInline       create_table_inline       = 10;  // see SPEC_TM
    RenameTable             rename_table              = 11;
  }
}
```

### Pointer-Mode Action Messages

Each action carries version fields for optimistic conflict detection.

```protobuf
message CreateNamespace {
  int32  id             = 1;    // assigned ID, or negative for late-bind
  int32  version        = 2;    // -1 for late-bind
  int32  parent_id      = 3;
  int32  parent_version = 4;    // -1 if parent is late-bound
  string name           = 5;
}

message DropNamespace {
  int32 id      = 1;
  int32 version = 2;            // must match for commit
}

message SetNamespaceProperty {
  int32  namespace_id      = 1;
  int32  namespace_version = 2; // -1 if created in same txn
  string key               = 3;
  string value             = 4;
}

message RemoveNamespaceProperty {
  int32  namespace_id      = 1;
  int32  namespace_version = 2;
  string key               = 3;
}

message CreateTable {
  int32  id                = 1; // -1 for late-bind
  int32  version           = 2;
  int32  namespace_id      = 3;
  int32  namespace_version = 4; // -1 if namespace is late-bound
  string name              = 5;
  string metadata_location = 6;
}

message DropTable {
  int32 id      = 1;
  int32 version = 2;
}

message UpdateTableLocation {
  int32  id                = 1;
  int32  version           = 2; // must match current
  string metadata_location = 3;
}

message ReadTable {
  int32 id      = 1;
  int32 version = 2;            // version that was read
}

// Renames a table in place — the int32 table id is stable, only
// (namespace, name) on TblEntry changes. Applies to both pointer-mode and
// inline tables (the id-keyed manifest pool / snapshot refs follow by
// construction). See SPEC_TM.md "Rename" and design.md "Operation × Operation
// Conflict Matrix" entry RT'.
message RenameTable {
  int32  id                    = 1;
  int32  version               = 2; // source tbl_v; must match current
  int32  new_namespace_id      = 3;
  int32  new_namespace_version = 4; // -1 if dest ns is created in same txn
  string new_name              = 5;
}
```

Inline action messages (`CreateTableInline`, `UpdateTableInline`) and the
delta hierarchy under `TableMetadataDelta` live in [SPEC_TM.md](SPEC_TM.md);
manifest-list delta updates live in [SPEC_ML.md](SPEC_ML.md).

## Worked examples

Two end-to-end layouts showing a checkpoint plus appended records. The RFC frames
are schematic (see the caveat under [File Layout](#file-layout)); a protobuf
body's internal fields are listed in the grammar rather than placed at fixed
offsets.

### Create a namespace and a table

`createNamespace("db")` then `createTable("db.t")`, shown *before* compaction so
the two appended records are visible:

```abnf
file       = header checkpoint record-1 record-2
checkpoint = framed<Checkpoint{ next_namespace_id=1, next_table_id=1 }>   ; empty
record-1   = framed<Transaction{ CreateNamespace(id=1, version=1,
                                                  parent_id=0, name="db") }>
record-2   = framed<Transaction{ CreateTable(id=1, version=1,
                                  namespace_id=1, namespace_version=1,
                                  name="t", metadata_location="…/meta-A.json") }>
```

<!-- protocol "Magic = LCAT:32,Format Version = 1:32,CP Length (varint):16,Checkpoint - empty (next_ns_id=1 next_tbl_id=1):48,Rec1 Length (varint):16,Record 1 - CreateNamespace (id=1 v=1 db):48,Rec2 Length (varint):16,Record 2 - CreateTable (id=1 v=1 ns=1 name=t):48" -->
```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Magic = LCAT                         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Format Version = 1                      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|       CP Length (varint)      |                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|        Checkpoint - empty (next_ns_id=1 next_tbl_id=1)        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|      Rec1 Length (varint)     |                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|            Record 1 - CreateNamespace (id=1 v=1 db)           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|      Rec2 Length (varint)     |                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|         Record 2 - CreateTable (id=1 v=1 ns=1 name=t)         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

After a CAS compaction the two records fold into the checkpoint, leaving one
carry-over record (the compacting commit's own transaction id, kept for dedup):

```
checkpoint: next_ns_id=2  next_tbl_id=2
            Namespace{ id=1  v=1  "db" }
            Table{ id=1  v=1  ns=1  "t" -> …/meta-A.json }
record 1  : carry-over (last txn id; dedup only)
```

### Multi-table transaction

Four tables already in the checkpoint; one atomic `CatalogTransaction` appends a
single record whose four `UpdateTableLocation` actions each match the table's
current version and advance it by one:

```abnf
file       = header checkpoint record-1
checkpoint = framed<Checkpoint{ next_table_id=5,
               Table{id=1,version=3,name="a"}, Table{id=2,version=5,name="b"},
               Table{id=3,version=2,name="c"}, Table{id=4,version=7,name="d"} }>
record-1   = framed<Transaction{
               UpdateTableLocation(id=1, version=3, metadata_location="…/a-4.json"),
               UpdateTableLocation(id=2, version=5, metadata_location="…/b-6.json"),
               UpdateTableLocation(id=3, version=2, metadata_location="…/c-3.json"),
               UpdateTableLocation(id=4, version=7, metadata_location="…/d-8.json") }>
```

<!-- protocol "Magic = LCAT:32,Format Version = 1:32,CP Length (varint):16,Checkpoint - Tables a(v3) b(v5) c(v2) d(v7):48,Record Length (varint):16,Record - 4x UpdateTableLocation bumps a-d:48" -->
```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Magic = LCAT                         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Format Version = 1                      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|       CP Length (varint)      |                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|          Checkpoint - Tables a(v3) b(v5) c(v2) d(v7)          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     Record Length (varint)    |                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|           Record - 4x UpdateTableLocation bumps a-d           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

Applying the record advances all four versions together (optimistic concurrency —
each action's `version` must match the current table version, else the commit is
rejected):

```
              before        after
  table a       v3    ->      v4
  table b       v5    ->      v6
  table c       v2    ->      v3
  table d       v7    ->      v8
```

## Wire Format Details

The codec uses standard protobuf encoding:

- **Field tag** = `(field_number << 3) | wire_type`
- **Wire type 0** (varint): `int32`, `bool`
- **Wire type 2** (length-delimited): `string`, `bytes`, nested messages
- **Wire type 1/5** (fixed64/fixed32): used for snapshot IDs

Varints use the standard protobuf unsigned encoding (7 bits per byte, MSB
continuation). Signed integers are not zigzag-encoded: a negative `int32`
(a late-bound ID) is written as the 5-byte varint of its 32-bit two's
complement.

Default-valued fields (zero for ints, false for bools, empty for
strings/bytes) are omitted per proto3 conventions.

## Read Protocol

```
function READ(fileIO, location):
    bytes  = fileIO.read(location)
    stream = InputStream(bytes)

    # 1. Read and validate header
    header = stream.read(8)
    assert header[0..4] == "LCAT"
    assert readInt(header, 4) == 1          # format version

    # 2. Read checkpoint
    chkLen   = readVarint(stream)
    chkBytes = stream.read(chkLen)
    state    = decodeCheckpoint(chkBytes)   # populates UUID, IDs, entities,
                                            # committed txns, inline tables,
                                            # manifest pools

    # 3. Read and apply transaction log
    while stream.hasRemaining():
        txnLen   = readVarint(stream)
        txnBytes = stream.read(txnLen)
        txn      = decodeTransaction(txnBytes)

        if state.committedTxn.contains(txn.id):
            continue                         # already applied (idempotent)
        if txn.verify(state):
            txn.apply(state)
            state.committedTxn.add(txn.id)

    return freeze(state)
```

A transaction that fails verification during read is silently skipped (not
applied, no error). This is correct behaviour: concurrent writers may append
transactions that conflict with each other, and during log replay the losing
transaction has no effect.

Inline-mode `UpdateTableInline` actions in delta mode fold their delta
into the in-memory checkpoint via
`InlineDeltaCodec.applyDeltaWithManifests` during this read-time apply
step. The replay must be byte-stable so the resulting `tblInlineMetadata`
hash (used as the synthetic `metadataFileLocation`) is the same across
two reads of the same catalog state — see [SPEC_TM.md](SPEC_TM.md)
§"Replay determinism" for the two pinning rules.

## Commit Protocol

Two thresholds control whether a commit takes the append path or the CAS
(compaction) path:

| Property                          | Default     | Purpose |
|-----------------------------------|-------------|---------|
| `fileio.catalog.max.append.count` | 10000       | Soft limit on log records: a writer that reads at least this many CASes instead of appending. Writers retrying a lost append race do not re-read the count, so the log can run a few records past it. Sized to provider limits (S3 Express 10k parts, Azure AppendBlob 50k blocks). **Setting to 0 forces CAS on every commit**, for providers without conditional append (S3 standard, GCS). |
| `fileio.catalog.max.append.size`  | 16777216    | Soft performance target for total file size. Larger catalogs take longer to read. |
| `fileio.catalog.throttle.retry.min-wait-ms` | 200 | First wait before retrying a write that storage throttled; later waits double, up to 5 s, for 8 attempts in all. |

`ProtoCatalogFile` tracks `appendCount` (the number of transaction records in
the log portion), computed during `readInternal`.

```
function COMMIT(fileIO, original, mutations):
    txn      = buildTransaction(original, mutations)
    txnBytes = encodeTransaction(txn)
    current  = original.location
    base     = original          # parsed state of current, unless stale
    stale    = false

    # Case 0: catalog does not exist yet
    if not current.exists():
        try:
            return tryCAS(original, current, txnBytes, fileIO)
        catch Ambiguous or Conflict as e:
            # The creation may have landed: its reply was lost, reading it back
            # failed, or it failed its precondition, as a transport retry of a
            # creation that landed does.
            fresh = READ(fileIO, current.location)   # failure: throw CommitStateUnknown
            if txn.id in fresh.committed:
                return fresh                 # the creation landed
            if e is Conflict:
                throw CommitFailed           # another writer created the catalog
            throw CommitStateUnknown

    ambiguous  = false   # a write's outcome is unknown; it may still be in flight
    unresolved = false   # a write failed its precondition; no read has ruled it out
    for attempt in 0..9:
      try:
        # Must CAS when either limit is at/beyond threshold based on the
        # observable file state. (maxAppendCount=0 forces CAS on every
        # commit — useful for backends without conditional append.)
        mustCAS = not fileIO.supportsAppend()
            or base.appendCount >= maxAppendCount
            or current.length + len(txnBytes) > maxAppendSize

        if mustCAS:
            # The CAS replaces current, so it must checkpoint the state current
            # names. Checkpointing a stale base would erase every record
            # committed since it was read.
            if stale:
                # Afresh: a read pinned to a version another writer has replaced
                # since current was bound fails on real storage.
                current = fileIO.newInputFile(current.location)
                base    = READ(fileIO, current)
                stale   = false
                if txn.id in base.committed:
                    return base              # an append reported as a lost race landed
                unresolved = false
            try:
                return validateCommit(tryCAS(base, current, txnBytes, fileIO), txn)
            catch Conflict:
                unresolved = true
            # CAS conflict: rebase. Replay checks txn's preconditions against
            # the state its record lands on, so if they hold on the new state,
            # retry on top of it (append or CAS, as that state calls for): the
            # commit is accepted exactly when a serial commit would be.
            # Otherwise fail without writing: AlreadyExists if a pending create
            # now collides, CommitFailed otherwise. The client retries.
            current = fileIO.newInputFile(current.location)
            base    = READ(fileIO, current)
            if txn.id in base.committed:
                return base                  # an earlier attempt landed
            unresolved = false
            if not txn.verify(base):
                throw conflict(base)

        else:
            try:
                return validateCommit(tryAppend(current, txnBytes, fileIO), txn)
            catch Conflict:
                unresolved = true

            # Append failed (offset mismatch from concurrent writer). The
            # transaction bytes are identical and idempotent (UUID-deduped),
            # so retry at the new offset without re-reading.
            oldLength = current.length
            current   = fileIO.newInputFile(current.location)
            if current.length < oldLength:
                # File shrank (compacted). Re-read so the next mustCAS
                # decision sees the current appendCount; afresh, as above.
                current = fileIO.newInputFile(current.location)
                base    = READ(fileIO, current)
                if txn.id in base.committed:
                    return base              # an append reported as a lost race landed
                unresolved = false
            else:
                # File grew (concurrent append). Retry same bytes; base's
                # appendCount is now a lower bound.
                stale = true

      catch Ambiguous:
        # The write may have landed: its reply was lost, or reading back what
        # it wrote failed. Its record carries txn.id, so a fresh read decides.
        # If it has not landed, retry: a write still in flight carries the same
        # transaction and a precondition on the version it saw, so at most one
        # of the two lands, and replay applies txn once. A rejection stays
        # definite: replay verifies a record against the state it lands on, so
        # a late landing of a transaction that does not hold changes nothing.
        ambiguous = true
        current = fileIO.newInputFile(current.location)
        base    = READ(fileIO, current)  # failure here: throw CommitStateUnknown
        stale   = false
        if txn.id in base.committed:
            return base                  # the write landed
        unresolved = false
        if not txn.verify(base):
            throw conflict(base)

      catch ReadFailed as e:
        # Reading the catalog failed. The commit failed too, unless one of its
        # writes may have landed.
        throw (ambiguous or unresolved) ? CommitStateUnknown : e

    # After an ambiguous write, running out of attempts is not a failure: a
    # write might yet have landed.
    if ambiguous:
        throw CommitStateUnknown
    if unresolved:
        # The last append lost a race, which its own landing also reports.
        base = READ(fileIO, current.location)  # failure here: throw CommitStateUnknown
        if txn.id in base.committed:
            return base
    throw CommitFailed("exceeded retry limit")
```

A commit reports one of three outcomes, and a caller can rely on each:
**returned** (the transaction took effect, once), **CommitFailed /
AlreadyExists** or any other failure (it did not and will not),
**CommitStateUnknown** (either). Iceberg deletes a commit's new files on a
`CleanableFailure` such as `CommitFailedException` (and, for table operations
that do not require strict cleanup, on any failure but
`CommitStateUnknownException`), so reporting a landed commit any other way
loses the files its snapshot references. A write fails ambiguously when
`writeAtomic` throws anything but the protocol's conflict and throttling
signals (`CASException`, `AppendException`, `StorageThrottleException`), when
reading back what it wrote fails, or when storage throttles it until the
retries run out (a 5xx can follow a write the service applied, and an SDK's
own retries report only the last attempt); `tryCAS` and `tryAppend` raise
`Ambiguous` then. A write that fails its precondition (`Conflict`) may have
landed too: a transport retry of a write that landed fails it. But that write
landed before the reply, so a read of the catalog that does not show it rules
it out; until one does, a failed read is `CommitStateUnknown`.

### CAS-only Mode

Setting `fileio.catalog.max.append.count=0` makes every commit go through
`tryCAS`. A CAS that loses a race rebases as above, so concurrent commits whose
preconditions do not overlap all succeed, as they would on the append path. This is required for storage providers that support conditional
full-object replacement but not conditional append (S3 standard via if-match
ETag, GCS via generation number). The commit loop never enters the append
branch because `appendCount >= 0` is always true.

### tryCAS

Builds a full file (header + checkpoint + transaction) and writes it
atomically, conditioned on the ETag / generation number of the existing file.

```
function tryCAS(base, current, txnBytes, fileIO):
    return WRITE(current, buildFullFile(base, txnBytes), Strategy.CAS, fileIO)
```

`buildFullFile` creates:
`[MAGIC][VERSION][varint(chkLen)][checkpoint][varint(txnLen)][txn]`. The
checkpoint is encoded from `base` alone, the replayed state of the file being
replaced; the pending mutations are only in the transaction record, which
replay verifies against `base` and applies, exactly as for an append. `base`
is usually the writer's `original`. After a lost append race it is the state
the writer re-read, and a transaction whose preconditions no longer hold is
rejected at replay (`validateCommit` throws).

### tryAppend

Appends only the transaction record (`[varint(txnLen)][txn]`) to the end of
the file.

```
function tryAppend(current, txnBytes, fileIO):
    return WRITE(current, [varint(len(txnBytes))] + txnBytes, Strategy.APPEND, fileIO)
```

Both write through `WRITE`, which retries a write that storage throttles
(`StorageThrottleException`: a 429, a 5xx, the GCS per-object update rate)
after a backoff that starts at `fileio.catalog.throttle.retry.min-wait-ms` and
doubles up to 5 s. Every attempt carries the same precondition, so at most one
lands; a retry of one that landed fails its precondition, which `COMMIT`
resolves by reading.

```
function WRITE(current, bytes, strategy, fileIO):
    for attempt in 0..7:
        outputFile = fileIO.newAtomicOutputFile(current)
        token      = outputFile.prepare(bytes, strategy)
        try:
            written = outputFile.writeAtomic(token, bytes)
        catch StorageThrottleException:
            backoff()                   # interrupted: throw CommitStateUnknown
            continue
        catch CASException or AppendException:
            throw Conflict              # may have landed: see COMMIT
        catch other:
            throw Ambiguous(other)      # may have landed
        return READ(written)            # a failure here: Ambiguous too
    throw Ambiguous                     # a throttled attempt may have landed
```

## Compaction

Compaction is triggered by observable file state — no per-transaction
hint flag is required, because correctness rests on atomic append + atomic
CAS at the storage layer:

1. The next writer reads the file. If `appendCount >= maxAppendCount` or
   `current.length + len(txnBytes) > maxAppendSize`, it enters the CAS
   branch.
2. The CAS builds a fresh checkpoint from the fully-replayed state and
   writes `[header][checkpoint][new-txn]`, replacing the entire file.
3. After CAS, the file has a single record in the log portion (the
   committing transaction), and `appendCount = 1`.

Two writers with mismatched `maxAppendCount` / `maxAppendSize` config can
disagree on when to compact, but they cannot corrupt the catalog: the
worst case is one writer continues appending past another writer's
preferred threshold. Both atomic primitives still enforce linearizable
ordering. The committed-transaction-ID set is carried forward in the
checkpoint for idempotency.

## Late-Binding

Writers never allocate entity IDs. A create action carries a negative
late-bound ID, and replay binds it to a real ID:

- `buildActions()` sorts new namespaces by depth (parents before
  children) and numbers new namespaces and tables `-1, -2, ...`
  (separate sequences). Actions later in the same transaction reference
  a new entity by its late-bound ID.
- Version fields are set to `-1` when the referenced entity was created
  in the same txn.
- On replay, `Transaction.apply` binds each late-bound ID, at its
  defining create, to the replaying state's next namespace / table ID
  (`ProtoCodec.IdBinding`), and resolves later references to it. Every
  reader replays the log in the same order, so every reader assigns the
  same IDs.
- `Transaction.verify` rejects a record that references a late-bound ID
  before (or without) the create that defines it.

Binding at replay is what keeps concurrent creates apart. When writers
allocated IDs from their own snapshot, two writers that read the same
snapshot and created unrelated entities -- e.g. tables in different
namespaces -- wrote records with the same ID. Both records verified, and replay let the second overwrite the
first.

## State Model

The catalog state is held in `ProtoCatalogFile` (immutable snapshot):

| Field                | Type                             | Description                          |
|----------------------|----------------------------------|--------------------------------------|
| `uuid`               | `UUID`                           | Catalog identity (UUIDv7)            |
| `nextNamespaceId`    | `int`                            | Next namespace ID to allocate        |
| `nextTableId`        | `int`                            | Next table ID to allocate            |
| `appendCount`        | `int`                            | Transaction records in the log       |
| `namespaceById`      | `Map<Integer, NsEntry>`          | Namespace ID → entry                 |
| `namespaceLookup`    | `Map<Namespace, Integer>`        | Namespace path → ID (derived)        |
| `nsProperties`       | `Map<Integer, Map<Str, Str>>`    | Namespace ID → properties            |
| `tableById`          | `Map<Integer, TblEntry>`         | Table ID → entry                     |
| `tableLookup`        | `Map<TableIdentifier, Integer>`  | Table name → ID (derived)            |
| `tblInlineMetadata`  | `Map<Integer, byte[]>`           | Table ID → TableMetadata JSON bytes  |
| `tblManifestPrefix`  | `Map<Integer, String>`           | Table ID → manifest-list prefix      |
| `manifestPool`       | `Map<Integer, Map<String, ManifestFile>>` | Per-table manifest pool     |
| `snapshotManifests`  | `Map<Integer, Map<Long, List<String>>>`   | Per-snapshot manifest paths |
| `committedTxns`      | `Set<UUID>`                      | Applied transaction IDs              |

Inline-only fields (`tblInlineMetadata`, `tblManifestPrefix`, `manifestPool`,
`snapshotManifests`) are described in [SPEC_TM.md](SPEC_TM.md) and
[SPEC_ML.md](SPEC_ML.md).

**The root namespace** is an ordinary `NsEntry` row with id 0
(`parentId = 0`, `name = ""`), present in every catalog and written to
the checkpoint with the other namespaces. `parentId = 0` means "child of
the root". Because it is a row, the root is versioned like any other
parent: creating or dropping a top-level namespace bumps its version, so
top-level creates serialize as sibling creates do under any other
parent. The root is otherwise reserved:

- it is not addressable by name: `namespaceLookup` omits it, so
  `Namespace.empty()` does not exist as far as callers are concerned;
- no action may create or drop it (`CreateNamespace` / `DropNamespace`
  with id 0 fail verification);
- tables and namespace properties cannot live on it: `FileIOCatalog`
  rejects table identifiers with an empty namespace
  (`supportsEmptyNamespace() = false` in the Iceberg catalog contract).

**Entry classes:**

- `NsEntry(int parentId, String name, int version)` — namespace metadata.
  The `namespaceLookup` map is rebuilt from `namespaceById` by walking
  parent chains.
- `TblEntry(int namespaceId, String name, int version, String metadataLocation)`
  — table metadata. For inline tables `metadataLocation` is null; the actual
  metadata lives in `tblInlineMetadata`.

A table ID appears in either `tableById` with a non-null `metadataLocation`
(pointer mode) or in `tblInlineMetadata` (inline mode), never both. Inline
tables also appear in `tableById` with `metadataLocation = null` so
`tableLookup` resolves them uniformly.

## Action Verification Rules

Each action's `verify()` checks version-based preconditions:

| Action                   | Precondition                                              |
|--------------------------|-----------------------------------------------------------|
| CreateNamespace          | not the root (id 0); parent version matches (skip if late-bound) |
| DropNamespace            | not the root (id 0); namespace version matches            |
| SetNamespaceProperty     | namespace version matches (skip if late-bound)            |
| RemoveNamespaceProperty  | namespace version matches (skip if late-bound)            |
| CreateTable              | namespace version matches (skip if late-bound)            |
| DropTable                | table version matches                                     |
| UpdateTableLocation      | table version matches                                     |
| ReadTable                | table version matches                                     |
| CreateTableInline        | namespace version matches (skip if late-bound)            |
| UpdateTableInline        | table version matches                                     |

Each `apply()` bumps the version of any entity whose state it mutates. See
[design.md](design.md) for the operation × operation conflict matrix that
falls out of these rules.

## Schema Evolution

Protobuf's forward and backward compatibility applies: new field numbers
can be added without breaking old readers, and removed fields leave their
numbers reserved. Unknown fields are silently skipped during decode (see
`ProtoCodec.skipField`).

## Key Files

| File | Purpose |
|------|---------|
| `src/main/proto/catalog.proto` | Protobuf schema (catalog + inline TM + ML) |
| `src/main/java/.../ProtoCatalogFormat.java` | Format implementation (read, commit, CAS/append) |
| `src/main/java/.../ProtoCatalogFile.java` | Immutable catalog snapshot + builder |
| `src/main/java/.../ProtoCodec.java` | Wire encoding/decoding, action types |
| `src/main/java/.../InlineDeltaCodec.java` | Inline TM/ML delta encode/decode/apply |
| `src/main/java/.../FileIOCatalog.java` | Catalog + `FileIOTableOperations` |
| `src/main/java/.../CatalogFormat.java` | Format strategy interface |
| `src/main/java/.../CatalogFile.java` | Abstract base + inline-aware `Mut` API |
