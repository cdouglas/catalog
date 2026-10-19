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

The codec (`ProtoCodec`, with `InlineDeltaCodec` for table-metadata deltas)
implements protobuf wire format manually — it does not use `protoc`-generated
classes. The encoding follows standard protobuf conventions (varints, field
tags, length-delimited fields), and `catalog.proto` declares every field the
codecs write, with a type of the wire type it is written with. Tests keep the
two in step: `CatalogProto` walks every catalog file the suites write against
the schema, and `TestFormatExplorer` checks the codecs' field-number constants
against it. A protobuf tool given the `.proto` file can decode the output.
Where the bytes differ from `protoc`'s, readers `protoc` generates accept them:
a negative `int32` is a 5-byte varint, not proto3's 10-byte sign extension;
zero-valued fields are written; and the repeated scalar fields are unpacked,
as the `.proto` declares (`[packed = false]`). Interop does not hold the other
way: `ProtoCodec`'s 32-bit varint reader decodes `protoc`'s 10-byte form only
for -1 to -8 (a ninth late-bound id, -9, reads as -1), and `protoc` omits
zero-valued fields that the decoders require, such as a `CODEC_JSON_GZIP` (0)
codec tag (see [Wire Format Details](#wire-format-details)).

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
  // Replay binds a transaction's late-bound ids to these, in log order.
  int32  next_namespace_id         = 2;
  int32  next_table_id             = 3;
  // The reader version this checkpoint needs (ProtoCodec
  // SUPPORTED_READER_VERSION, now 1). Written first; required.
  int32  min_reader_version        = 4;
  repeated Namespace          namespaces             = 10;
  repeated Table              tables                 = 11;  // pointer-mode tables
  repeated NamespaceProperty  namespace_properties   = 12;
  repeated InlineTable        inline_tables          = 13;  // inline-mode tables
  CommittedTransactions       committed_transactions = 20;  // compressed UUIDv7 set
}

message CommittedTransactions {
  // Largest timestamp in the set; 0 when the set is empty. Always written.
  fixed64 max_timestamp_ms             = 1;
  // Per entry, descending order, back-to-back inside one packed bytes
  // field: varint delta_ms (gap from previous; the first is 0) || 10 random
  // bytes (rand_a + rand_b + 6-bit zero pad).
  bytes   packed_entries               = 2;
  // GC horizon: the largest timestamp of any id a retention pass dropped.
  // 0 (absent) until the first drop; ids at or below it are UNKNOWN.
  fixed64 highest_dropped_timestamp_ms = 3;
}
```

The encoder writes `min_reader_version` before every other field, so a reader
that cannot read the checkpoint fails before decoding the rest; the decoder
fails if it is absent or exceeds `ProtoCodec.SUPPORTED_READER_VERSION`.
`committed_transactions` is omitted when the set is empty and has no horizon.

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
the wire bytes in a single linear pass without materializing the set:
the catalog file holds the inner-message bytes directly (no
`HashSet<UUID>`). A read that applies no log records keeps the
checkpoint's bytes as they are; one that applies records merges their
ids in (`ProtoCodec.rewriteCommittedBytes`). A CAS copies the bytes into
its checkpoint unchanged only when GC is off; otherwise it rewrites them
on every CAS (see below).

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
using `UuidV7.newUuidV7After(original.highestDroppedTimestampMs())`,
which embeds `System.currentTimeMillis()` in the high 48 bits, or the
horizon of the catalog the writer read plus 1 ms if the clock has not
passed it (see the clock-skew note below). Because the mint happens at
commit-submission time — not at transaction-begin time — a long-running
transaction does not get a stale timestamp; a transaction that ran for a
week and then commits has a UUID timestamp from the moment of commit, not
from a week prior. Retention can therefore be expressed in wall-clock
terms without disadvantaging slow workloads.

**Horizon.** `CommittedTransactions.highest_dropped_timestamp_ms` (field
3) is the set's GC horizon: the largest timestamp of any id a retention
pass has dropped. It is absent (0) until the first drop and never
decreases: each pass keeps the larger of the inherited horizon and what
it drops.

**GC algorithm.** The set changes in two places. A read merges the id of
each record replay applies into the set, dropping nothing
(`ProtoCatalogFile.Builder.build()`). A CAS (any CAS-mode commit,
including the always-CAS path when `maxAppendCount=0`) then filters the
set of the state it checkpoints (`ProtoCatalogFormat` `buildFullFile`,
through `ProtoCodec.encodeCheckpoint`):

```
function compactCommitted(base, txn, retentionMs):
    # base: the replayed state of the file being replaced. Its set holds the
    # checkpoint's ids and those of every log record replay applied.
    nowMs       = System.currentTimeMillis()
    watermarkMs = retentionMs >= nowMs ? 0 : nowMs - retentionMs
    # Below this commit's own id: its record follows the checkpoint, and
    # replay rejects a record at or below the horizon.
    watermarkMs = max(0, min(watermarkMs, timestamp(txn.id) - 1))
    if watermarkMs == 0:
        return base.committed                    # GC off: copied unchanged

    kept    = []
    horizon = base.committed.highest_dropped_timestamp_ms
    for entry in decode(base.committed.packed_entries):   # descending
        if timestamp(entry) > watermarkMs:
            kept.append(entry)
        else:
            horizon = max(horizon, timestamp(entry))

    if kept is empty and horizon == 0:
        return absent                             # field omitted
    return CommittedTransactions(
        max_timestamp_ms             = kept[0].timestamp or 0,
        packed_entries               = encode(kept),
        highest_dropped_timestamp_ms = horizon)   # omitted when 0
```

The log records of the file being replaced are in `base`'s set by then,
so they are as eligible for GC as the checkpoint's own entries. Only the
compacting commit's record, which follows the new checkpoint, is outside
the pass; its id joins the set when a read replays it. Both passes are
`ProtoCodec.rewriteCommittedBytes`, one linear walk of the descending
entries: the merge at read with no watermark, the filter at CAS with no
additions.

**Edge case: the horizon can exceed `max_timestamp_ms`.** A pass that
drops every entry writes a horizon-only body (`max_timestamp_ms = 0`,
`highest_dropped_timestamp_ms` non-zero), so that the horizon survives
later compactions. The two timestamps are tracked independently.

**Query semantics: tri-state.** `containsTransaction(UUID)` (on
`ProtoCatalogFile` and its `Builder`) resolves a query for UUID `q`
against `CommittedTransactions(packed_entries=P, highest_dropped=H)` to
one of:

| Outcome | Condition | Result | Meaning |
|---|---|---|---|
| `COMMITTED` | `q ∈ P` | `true` | the transaction landed |
| `ABSENT` | `q ∉ P` and (`H = 0` or `timestamp(q) > H`) | `false` | the transaction was never committed |
| `UNKNOWN` | `q ∉ P` and `0 < H` and `timestamp(q) <= H` | throws `DedupHorizonExceededException` | may have been GC'd; cannot distinguish from never-committed |

A non-v7 `q` returns `false` without consulting the horizon.
`highestDroppedTimestampMs()` returns `H`.

**Replay rejects a record at or below the horizon.** Replay does not ask
`containsTransaction`, which would throw for such a record and so fail
every read of the catalog (Bug 64). It asks
`ProtoCatalogFile.Builder.mayApply`, which admits a record only if the
set does not hold its id (else it is a retry of a record already
applied) and its timestamp is above the horizon (else it may be a retry
of a commit whose id aged out). Such records are reachable: a writer
whose clock is behind another's by more than the retention, one that
stalls that long between minting its id and writing, or a race under a
short retention. Replay skips them as it skips a record that fails
verification.

**Writers.** A writer asks `containsTransaction` of its own id on each
read that decides its commit (`ProtoCatalogFormat.Mut.landed`). Its own
CAS keeps the horizon below its record (the watermark cap above), and it
mints its id after the horizon of the catalog it read. Another writer's
compaction can still raise the horizon past an id in flight: a commit
whose id is at or below the horizon of a read fails
(`CommitFailedException`), or is unknown (`CommitStateUnknownException`)
if a write of it may have landed before its id aged out. A caller that
must confirm such a commit later needs another oracle (see GC.md "Two
oracles").

**Configuration knob.**

| Property | Default | Purpose |
|---|---|---|
| `fileio.catalog.committed.retention.ms` | `21_600_000` (6h) | Minimum age before a committed-txn UUID becomes eligible for GC. Should comfortably exceed the longest plausible retry window for a writer learning its outcome. Setting to `Long.MAX_VALUE` disables GC. A retention shorter than a commit takes, `0` included, is legal but lossy (below). |

**A short retention is lossy.** A concurrent CAS can drop a committed id
newer than an in-flight writer's transaction id, or minted in the same
millisecond (with a retention of 0, each CAS drops every id older than its
own). The
horizon then covers the writer's id, replay rejects its record, and the writer
fails (`CommitFailedException`) or, if an earlier write of it may have landed,
cannot learn its outcome (`CommitStateUnknownException`). The catalog is never
made incorrect or unreadable. The default 6 h exceeds any commit's retries.

A 6-hour default is pessimistic by design: it bounds the `CommittedTransactions`
section size to `retention × commit-rate` even for CAS-only catalogs
that compact on every commit, while leaving a generous window for
writers to confirm. For a 1-commit/sec workload with 6h retention the
section caps at ~21,600 entries (~250 KB compressed); for 100/sec it
caps at ~2.16M entries (~25 MB compressed) — still bounded and
predictable.

**Clock-skew note.** Watermark comparisons use the compactor's local
clock against UUIDv7 timestamps minted by other writers. Wall-clock
skew between writers translates directly into GC-window jitter: a
writer with a clock 10 minutes behind has its UUID timestamps appear
10 minutes "older," shrinking its effective retention by 10 minutes.
Keep writer clocks NTP-synced. This is a deployment requirement, not a
correctness invariant: a writer behind by more than the retention still
mints its id above the horizon it read, and a record that lands at or
below the horizon is rejected, never applied twice or left to fail
reads. Skew costs commits that fail, or report
`CommitStateUnknownException`, spuriously.

### Catalog Entities

```protobuf
// Every checkpoint holds the root namespace as a row: id 0, parent 0, empty
// name. Its version orders top-level creates and drops like any parent's.
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
  reserved 2;                            // was `sealed`
  bytes           transaction_id = 1;    // UUIDv7, 16 bytes; required
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
    ReadTable               read_table                = 8;   // pins a version read
    UpdateTableInline       update_table_inline       = 9;   // see SPEC_TM
    CreateTableInline       create_table_inline       = 10;  // see SPEC_TM
    RenameTable             rename_table              = 11;
  }
}
```

### Pointer-Mode Action Messages

Each action carries version fields for optimistic conflict detection. A
create names its new entity by a late-bound (negative) id, `-1, -2, ...` for
namespaces and, separately, for tables, in the order of their creates; later
actions of the same transaction may name it (see [Late-Binding](#late-binding)).
A `*_version` of -1 means the entity it names is late-bound, so there is no
version to check.

```protobuf
message CreateNamespace {
  int32  id             = 1;    // late-bound (negative); 0, the root, is rejected
  int32  version        = 2;    // the new namespace's version (1)
  int32  parent_id      = 3;    // 0 = root; negative if created in this transaction
  int32  parent_version = 4;    // must match; -1 if the parent is late-bound
  string name           = 5;
}

message DropNamespace {
  int32 id      = 1;
  int32 version = 2;            // must match for commit
}

message SetNamespaceProperty {
  int32  namespace_id      = 1;
  int32  namespace_version = 2; // must match; -1 if created in same txn
  string key               = 3;
  string value             = 4;
}

message RemoveNamespaceProperty {
  int32  namespace_id      = 1;
  int32  namespace_version = 2; // must match
  string key               = 3;
}

message CreateTable {
  int32  id                = 1; // late-bound (negative)
  int32  version           = 2; // the new table's version (1)
  int32  namespace_id      = 3; // negative if created in this transaction
  int32  namespace_version = 4; // must match; -1 if the namespace is late-bound
  string name              = 5;
  string metadata_location = 6;
}

message DropTable {
  int32 id      = 1;
  int32 version = 2; // must match current; FileIOCatalog.dropTable retries on a
                     // fresh read when another commit changed the table, and a
                     // purge deletes the dropped version's files once it lands
}

message UpdateTableLocation {
  int32  id                = 1;
  int32  version           = 2; // must match current
  string metadata_location = 3; // on apply, clears any inline state (SPEC_TM)
}

message ReadTable {
  int32 id      = 1;
  int32 version = 2;            // version that was read
}

// Renames a table in place — the int32 table id is stable, only
// (namespace, name) on TblEntry changes. Applies to both pointer-mode and
// inline tables (the id-keyed manifest pool / snapshot refs follow by
// construction). Apply bumps the version of the table and of both
// namespaces. See SPEC_TM.md "Rename" and design.md "Operation × Operation
// Conflict Matrix" entry RT'.
message RenameTable {
  int32  id                    = 1;
  int32  version               = 2; // source tbl_v; must match current
  int32  new_namespace_id      = 3; // negative if created in this transaction
  int32  new_namespace_version = 4; // must match; -1 if late-bound
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

The file `FileIOCatalog.initialize` creates, then `createNamespace("db")` and
`createTable("db.t")`, shown *before* compaction so the appended records are
visible (the format explorer's `empty-bootstrap` and
`txn-multi-action-with-late-bind` scenarios walk the bytes the code writes):

```abnf
file       = header checkpoint record-1 record-2 record-3
checkpoint = framed<Checkpoint{ min_reader_version=1, catalog_uuid=…,
               next_namespace_id=1, next_table_id=1,
               Namespace{id=0, version=0, parent_id=0, name=""} }>   ; empty
record-1   = framed<Transaction{ transaction_id=… }>                 ; no actions
record-2   = framed<Transaction{ CreateNamespace(id=-1, version=1,
                                  parent_id=0, parent_version=0, name="db") }>
record-3   = framed<Transaction{ CreateTable(id=-1, version=1,
                                  namespace_id=1, namespace_version=1,
                                  name="t", metadata_location="…/meta-A.json") }>
```

`FileIOCatalog.initialize` creates the file with a CAS of the empty catalog
(Case 0 of [COMMIT](#commit-protocol)): a checkpoint holding only the root row,
then the empty transaction's record, whose id joins the committed set like any
other. Replay binds record 2's namespace -1 to `next_namespace_id`, 1, and bumps
the root to version 1; `parent_version` is the root's version when the writer
read it. Record 3's writer read `db` as id 1 at version 1; replay binds the table
-1 to id 1 and bumps `db` to version 2. Created in one transaction, the two
actions would share one record, the `CreateTable` naming the namespace by its
late-bound id (`namespace_id=-1, namespace_version=-1`).

<!-- protocol "Magic = LCAT:32,Format Version = 1:32,CP Length (varint):16,Checkpoint - empty (root row; next ids 1):48,Rec1 Length (varint):16,Record 1 - empty txn (initialize):48,Rec2 Length (varint):16,Record 2 - CreateNamespace (id=-1 v=1 db):48,Rec3 Length (varint):16,Record 3 - CreateTable (id=-1 v=1 ns=1 t):48" -->
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
|           Checkpoint - empty (root row; next ids 1)           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|      Rec1 Length (varint)     |                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|               Record 1 - empty txn (initialize)               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|      Rec2 Length (varint)     |                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|           Record 2 - CreateNamespace (id=-1 v=1 db)           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|      Rec3 Length (varint)     |                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|           Record 3 - CreateTable (id=-1 v=1 ns=1 t)           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

A commit that CASes next, say an `UpdateTableLocation` of `t`, writes a
checkpoint of the replayed state and then its own record, in full:

```
checkpoint: min_reader_version=1  next_ns_id=2  next_tbl_id=2
            Namespace{ id=0  v=1  parent=0  "" }     ; root, bumped by the create of db
            Namespace{ id=1  v=2  parent=0  "db" }   ; bumped by the create of t
            Table{ id=1  v=1  ns=1  "t" -> …/meta-A.json }
            committed_transactions: the ids of records 1-3, less any at or
                                    below this CAS's GC watermark
record 1  : Transaction{ UpdateTableLocation(id=1, version=1,
                                             metadata_location="…/meta-B.json") }
```

Replay verifies and applies record 1 on top of the checkpoint, as it would an
append, leaving `t` at version 2.

### Multi-table transaction

Four tables already in the checkpoint; one atomic `CatalogTransaction` appends a
single record whose four `UpdateTableLocation` actions each match the table's
current version and advance it by one:

```abnf
file       = header checkpoint record-1
checkpoint = framed<Checkpoint{ min_reader_version=1, …, next_table_id=5,
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
- **Wire type 0** (varint): `int32`, `int64`, `bool`, enums, and `sint64`
- **Wire type 1** (fixed64): snapshot IDs and the committed set's
  timestamps (`max_timestamp_ms`, `highest_dropped_timestamp_ms`)
- **Wire type 2** (length-delimited): `string`, `bytes`, nested messages
- **Wire type 5** (fixed32): never written; skipped on read

Varints use the standard protobuf unsigned encoding (7 bits per byte, MSB
continuation). `int32` and `int64` are not zigzag-encoded: a negative `int32`
(a late-bound ID, or a version of -1) is written as the 5-byte varint of its
32-bit two's complement. The one `sint64`, `AddSnapshot.timestamp_delta_ms`
(SPEC_TM), is zigzag-encoded.

Unlike proto3, `ProtoCodec` writes the fields of the catalog entities and
actions unconditionally, zero and empty values included (an id or version of 0,
the root's `parent_id = 0`, an empty string such as the root's name). Optional
fields are written only when present: `committed_transactions` (omitted when
the set is empty with no horizon), `highest_dropped_timestamp_ms` (omitted when
0), the one case of `UpdateTableInline`'s payload, and the optional inline
fields SPEC_TM and SPEC_ML describe. `ManifestFileEntry` omits zero or null
counts, a zero spec id or sequence number, and the content of a data manifest. A decoder takes an
absent scalar as zero or empty, but some fields are required, and their absence
fails the read: `Checkpoint.min_reader_version`, `Transaction.transaction_id`,
every inline-metadata codec tag (`InlineTable.metadata_codec`,
`CreateTableInline.metadata_codec`, and `UpdateTableInline.full_metadata_codec`
with `full_metadata`; 0 included), and `TableMetadataDelta.last_updated_ms`.

## Read Protocol

```
function READ(fileIO, location):
    bytes  = fileIO.read(location)
    stream = InputStream(bytes)

    # 1. Read and validate header
    header = stream.read(8)
    assert header[0..4] == "LCAT"
    assert readInt(header, 4) == 1          # format version: must equal 1

    # 2. Read checkpoint
    chkLen   = readVarint(stream)
    chkBytes = stream.read(chkLen)
    state    = decodeCheckpoint(chkBytes)   # populates UUID, IDs, entities,
                                            # committed txns, inline tables,
                                            # manifest pools; fails if
                                            # min_reader_version is absent or
                                            # > SUPPORTED_READER_VERSION (1)

    # 3. Read and apply transaction log
    while stream.hasRemaining():
        txnLen   = readVarint(stream)
        txnBytes = stream.read(txnLen)
        txn      = decodeTransaction(txnBytes)

        if not state.mayApply(txn.id):
            continue                         # in the committed set (a retry), or
                                             # at or below its GC horizon
        if txn.verify(state):
            txn.apply(state)
            state.committedTxn.add(txn.id)

    return freeze(state)                     # merges the added ids into the set
```

A transaction that fails verification during read, or that `mayApply` rejects
(see [Committed-set retention](#committed-set-retention-gc)), is silently
skipped (not applied, no error). This is correct behaviour: concurrent writers
may append transactions that conflict with each other, and during log replay
the losing transaction has no effect.

Inline-mode `UpdateTableInline` actions in delta mode fold their delta
into the in-memory checkpoint via
`InlineDeltaCodec.applyDeltaWithManifests` during this read-time apply
step. The replay must be byte-stable so the resulting `tblInlineMetadata`
hash (used as the synthetic `metadataFileLocation`) is the same across
two reads of the same catalog state — see [SPEC_TM.md](SPEC_TM.md)
§"Replay determinism" for the two pinning rules.

## Commit Protocol

Two thresholds, and whether the FileIO can append, control whether a commit
takes the append path or the CAS (compaction) path:

| Property                          | Default     | Purpose |
|-----------------------------------|-------------|---------|
| `fileio.catalog.max.append.count` | 10000       | Soft limit on log records: a writer that reads at least this many CASes instead of appending. Writers retrying a lost append race do not re-read the count, so the log can run a few records past it. Sized to provider limits (S3 Express 10k parts, Azure AppendBlob 50k blocks); an append past one fails with `SegmentLimitException`, and the writer compacts instead, so a count above the store's limit costs a refused append, not a stuck catalog. **Setting to 0 forces CAS on every commit**, which an S3 standard bucket needs: `S3FileIO` reports `supportsAppend()`, but only S3 Express accepts its offset appends. A FileIO whose `supportsAppend()` is false (`GCSFileIO`) is CAS-only whatever the setting. |
| `fileio.catalog.max.append.size`  | 16777216    | Soft performance target for total file size. Larger catalogs take longer to read. |
| `fileio.catalog.throttle.retry.min-wait-ms` | 200 | First wait before retrying a write that storage throttled; later waits double, up to the larger of 5 s and this value, each plus up to 50% random jitter, for 8 attempts in all. |

`ProtoCatalogFile` tracks `appendCount` (the number of transaction records in
the log portion), computed during `readInternal`.

```
# landed(state): whether state's committed set holds txn.id
# (containsTransaction). If txn.id is at or below state's GC horizon, the set
# cannot say: throw CommitStateUnknown if a write of txn may have landed (the
# write this read resolves, or an earlier one that ambiguous or unresolved
# tracks), else CommitFailed (ProtoCatalogFormat.Mut.landed).
# validateCommit(state, txn) returns state if landed(state), else throws
# conflict(state).

function COMMIT(fileIO, original, mutations):
    txn      = buildTransaction(original, mutations)   # id minted after original's horizon
    txnBytes = encodeTransaction(txn)
    current  = original.location
    base     = original          # parsed state of current, unless stale
    stale    = false

    # Case 0: catalog does not exist yet
    if not current.exists():
        try:
            return tryCAS(original, current, txn, txnBytes, fileIO)
        catch Ambiguous or Conflict as e:
            # The creation may have landed: its reply was lost, reading it back
            # failed, or it failed its precondition, as a transport retry of a
            # creation that landed does.
            fresh = READ(fileIO, current.location)   # failure: throw CommitStateUnknown
            if landed(fresh):
                return fresh                 # the creation landed
            if e is Conflict:
                throw CommitFailed           # another writer created the catalog
            throw CommitStateUnknown

    ambiguous  = false   # a write's outcome is unknown; it may still be in flight
    unresolved = false   # a write failed its precondition; no read has ruled it out
    full       = false   # storage refused an append: the object holds all the writes it can
    for attempt in 0..9:
      try:
        # Must CAS when the FileIO cannot append, the object is full, or
        # either limit is at/beyond threshold based on the observable file state.
        # (maxAppendCount=0 forces CAS on every commit, for a backend that
        # reports supportsAppend() but cannot append, as an S3 standard bucket.)
        mustCAS = full
            or not fileIO.supportsAppend()
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
                if landed(base):
                    return base              # an append reported as a lost race landed
                unresolved = false
            full = false
            try:
                return validateCommit(tryCAS(base, current, txn, txnBytes, fileIO), txn)
            catch Conflict:
                unresolved = true
            # CAS conflict: rebase. Replay checks txn's preconditions against
            # the state its record lands on, so if they hold on the new state,
            # retry on top of it (append or CAS, as that state calls for): the
            # commit is accepted exactly when a serial commit would be.
            # Otherwise fail without writing: AlreadyExists if a pending create
            # or rename destination now collides, NoSuchTable if a rename's
            # source is gone (checked first, as renameTable does),
            # CommitFailed otherwise. The client retries.
            current = fileIO.newInputFile(current.location)
            base    = READ(fileIO, current)
            if landed(base):
                return base                  # an earlier attempt landed
            unresolved = false
            if not txn.verify(base):
                throw conflict(base)

        else:
            try:
                return validateCommit(tryAppend(current, txnBytes, fileIO), txn)
            catch SegmentLimit:
                # The object holds as many writes as the store allows (S3 Express:
                # 10,000 parts; an Azure Append Blob: 50,000 blocks). Nothing landed,
                # and no append can: compact by CAS on a fresh read (Bug 66).
                full  = true
                stale = true
                continue
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
                if landed(base):
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
        if landed(base):
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
        if landed(base):
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

`COMMIT` fails a transaction that no longer holds on the state it lost a race
to; restaging it on a fresh read is the caller's choice. `FileIOCatalog`
restages namespace operations, renames and drops on a fresh read and retries
their `CommitFailedException`, up to Iceberg's default number of attempts
(`commitOnAFreshRead`, `dropTable`; a table create likewise, in
`FileIOTableOperations.commitCreate`). `commitTransaction` does not retry: a
catalog transaction conflicts on the whole version of each table it pins.

### CAS-only Mode

Setting `fileio.catalog.max.append.count=0` makes every commit go through
`tryCAS`. A CAS that loses a race rebases as above, so concurrent commits whose
preconditions do not overlap all succeed, as they would on the append path. The
commit loop never enters the append branch because `appendCount >= 0` is always
true. A storage provider that supports conditional full-object replacement but
not conditional append needs CAS-only commits. On GCS (CAS via generation
number) the format forces them: `GCSFileIO.supportsAppend()` is false. On an S3
standard bucket (CAS via if-match ETag) the setting is required: `S3FileIO`
reports `supportsAppend()`, but appends with a write offset, which only S3
Express directory buckets accept.

### tryCAS

Builds a full file (header + checkpoint + transaction) and writes it
atomically, conditioned on the ETag / generation number of the existing file.

```
function tryCAS(base, current, txn, txnBytes, fileIO):
    return WRITE(current, buildFullFile(base, txn, txnBytes), Strategy.CAS, fileIO)
```

`buildFullFile` creates:
`[MAGIC][VERSION][varint(chkLen)][checkpoint][varint(txnLen)][txn]`. The
checkpoint is encoded from `base` alone, the replayed state of the file being
replaced, its committed set filtered at a GC watermark capped below `txn`'s own
id (see [Committed-set retention](#committed-set-retention-gc)); the pending
mutations are only in the transaction record, which replay verifies against
`base` and applies, exactly as for an append. `base`
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
doubles up to the larger of 5 s and that value, each wait plus up to 50% random
jitter (`ProtoCatalogFormat.Mut.retryThrottled`). Every attempt carries the
same precondition, so at most one lands; a retry of one that landed fails its
precondition, which `COMMIT` resolves by reading.

```
function WRITE(current, bytes, strategy, fileIO):
    for attempt in 0..7:
        outputFile = fileIO.newOutputFile(current)
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

1. The next writer reads the file. If its FileIO cannot append
   (`supportsAppend()` is false), `appendCount >= maxAppendCount`, or
   `current.length + len(txnBytes) > maxAppendSize`, it enters the CAS
   branch. So does a writer whose append storage refused with
   `SegmentLimitException`: the object holds as many writes as the store
   allows, whatever the knobs say.
2. The CAS builds a fresh checkpoint from the fully-replayed state,
   dropping committed ids at or below its GC watermark, and writes
   `[header][checkpoint][new-txn]`, replacing the entire file.
3. After CAS, the file has a single record in the log portion (the
   committing transaction), and `appendCount = 1`.

Two writers with mismatched `maxAppendCount` / `maxAppendSize` config can
disagree on when to compact, but they cannot corrupt the catalog: the
worst case is one writer continues appending past another writer's
preferred threshold. Both atomic primitives still enforce linearizable
ordering. The committed-transaction-ID set is carried forward in the
checkpoint for idempotency, less the ids its GC pass drops.

## Late-Binding

Writers never allocate entity IDs. A create action carries a negative
late-bound ID, and replay binds it to a real ID:

- `buildActions()` sorts new namespaces by depth (parents before
  children) and numbers new namespaces and tables `-1, -2, ...`
  (separate sequences). Actions later in the same transaction reference
  a new entity by its late-bound ID.
- A create's own `version` is 1, the version replay gives the new
  entity. A version field that names another entity (`parent_version`,
  `namespace_version`, `new_namespace_version`) is `-1` when that entity
  is created in the same txn.
- On replay, `Transaction.apply` binds each late-bound ID, at its
  defining create, to the replaying state's next namespace / table ID
  (`ProtoCodec.IdBinding`), and resolves later references to it. Every
  reader replays the log in the same order, so every reader assigns the
  same IDs.
- `Transaction.verify` rejects a record that references a late-bound ID
  before (or without) the create that defines it, or that defines one
  twice.

Binding at replay is what keeps concurrent creates apart. When writers
allocated IDs from their own snapshot, two writers that read the same
snapshot and created unrelated entities -- e.g. tables in different
namespaces -- wrote records with the same ID. Both records verified,
and replay let the second overwrite the first.

## State Model

The catalog state is held in `ProtoCatalogFile` (immutable snapshot):

| Field                | Type                             | Description                          |
|----------------------|----------------------------------|--------------------------------------|
| `uuid`               | `UUID`                           | Catalog identity (UUIDv7)            |
| `nextNamespaceId`    | `int`                            | Next namespace ID replay binds       |
| `nextTableId`        | `int`                            | Next table ID replay binds           |
| `appendCount`        | `int`                            | Transaction records in the log       |
| `namespaceById`      | `Map<Integer, NsEntry>`          | Namespace ID → entry                 |
| `namespaceLookup`    | `Map<Namespace, Integer>`        | Namespace path → ID (derived)        |
| `nsProperties`       | `Map<Integer, Map<Str, Str>>`    | Namespace ID → properties            |
| `tableById`          | `Map<Integer, TblEntry>`         | Table ID → entry                     |
| `tableLookup`        | `Map<TableIdentifier, Integer>`  | Table name → ID (derived)            |
| `tblInlineMetadata`  | `Map<Integer, byte[]>`           | Table ID → encoded TableMetadata bytes |
| `tblInlineMetadataCodec` | `Map<Integer, Byte>`         | Table ID → codec tag of those bytes  |
| `tblManifestPrefix`  | `Map<Integer, String>`           | Table ID → manifest-list prefix      |
| `manifestPool`       | `Map<Integer, Map<String, ManifestFile>>` | Per-table manifest pool     |
| `snapshotManifests`  | `Map<Integer, Map<Long, List<String>>>`   | Per-snapshot manifest paths |
| `committedTransactionsBytes` | `byte[]`                 | Applied transaction IDs: the inner `CommittedTransactions` bytes (compressed) |

Inline-only fields (`tblInlineMetadata`, `tblInlineMetadataCodec`,
`tblManifestPrefix`, `manifestPool`, `snapshotManifests`) are described in
[SPEC_TM.md](SPEC_TM.md) and [SPEC_ML.md](SPEC_ML.md). The committed set is
queried in place (`containsTransaction`); `committedTransactions()` decodes a
`Set<UUID>` on demand, for tests and tooling.

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
- tables and namespace properties cannot live on it. Writers enforce
  this, not replay: `CatalogFile.Mut` treats the empty namespace as one
  that does not exist, and `FileIOCatalog.isValidIdentifier` rejects a
  table identifier with an empty namespace.

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
`tableLookup` resolves them uniformly. Applying `UpdateTableLocation`
clears the table's inline state (metadata, manifest pool, snapshot refs);
applying a full `UpdateTableInline` clears the location. A checkpoint
encodes an inline table without a location, so a table holding both
would lose its metadata file there.

## Action Verification Rules

Replay applies a record only if `ProtoCatalogFile.Builder.mayApply`
admits its id (not in the committed set, and above the set's GC horizon;
see [Committed-set retention](#committed-set-retention-gc)) and
`Transaction.verify` accepts it. `Transaction.verify` rejects a record
whose:

- transaction id is not a UUIDv7;
- late-bound id is referenced before (or without) its defining create,
  or defined twice (`ProtoCodec.IdBinding`);
- action references a namespace or table that another of its actions
  drops (`Transaction.selfConsistent`, Bug 14): every action verifies
  against the state before the transaction, so such a record would pass
  and apply to a state no read could build;
- action fails its own `verify()`, checked against the state before the
  transaction, not after the record's earlier actions:

| Action                   | Precondition                                              |
|--------------------------|-----------------------------------------------------------|
| CreateNamespace          | not the root (id 0); parent version matches (skipped if -1: late-bound) |
| DropNamespace            | not the root (id 0); namespace version matches            |
| SetNamespaceProperty     | namespace version matches (skipped if -1)                 |
| RemoveNamespaceProperty  | namespace version matches (skipped if -1)                 |
| CreateTable              | namespace version matches (skipped if -1)                 |
| DropTable                | table version matches                                     |
| UpdateTableLocation      | table version matches                                     |
| ReadTable                | table version matches                                     |
| CreateTableInline        | namespace version matches (skipped if -1)                 |
| UpdateTableInline        | table version matches                                     |
| RenameTable              | table version matches; destination namespace version matches (skipped if -1) |
| unknown action type      | always passes (`UnknownAction`)                           |

Each `apply()` bumps the version of any entity whose state it mutates: a
table update or rename bumps the table, and a create starts its entity at
the record's `version` (1). A namespace's version covers its children and
properties: creating, dropping or renaming a child bumps the parent
namespace (a rename bumps both, when they differ), and setting or removing
a property bumps that namespace. `ReadTable` and unknown actions change
nothing. See [design.md](design.md) for the operation × operation conflict
matrix that falls out of these rules.

## Schema Evolution

Protobuf's forward and backward compatibility applies: new field numbers
can be added without breaking old readers, and removed fields leave their
numbers reserved (`Transaction` reserves 2, the former `sealed`).

- Unknown fields are silently skipped during decode (`ProtoCodec.skipField`,
  for wire types 0, 1, 2 and 5); an unknown wire type throws, failing the
  read.
- An unknown action type decodes as `UnknownAction`, which always
  verifies and applies nothing; the record's other actions apply.
- The header's `formatVersion` must equal `ProtoCatalogFormat.FORMAT_VERSION`
  (1).
- `Checkpoint.min_reader_version` gates changes a reader cannot skip safely:
  every checkpoint carries `ProtoCodec.SUPPORTED_READER_VERSION` (1), and a
  reader refuses one that lacks the field or carries more than it supports. It
  gates checkpoints only: a record appended to a checkpoint is read by any
  reader that accepts the checkpoint.

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
