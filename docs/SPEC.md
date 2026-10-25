# Catalog State Machine Specification

This document specifies the catalog as a state machine (`CatalogStateMachine`):
its state, the checkpoint that holds it, and the intentions (a transaction's
actions) that change it, with the rules replay verifies and applies them by.
The intention log replicates it ([LOG.md](LOG.md): the envelope, replay, the
committed set and its GC, the commit loop), on any provider: `FileIOCatalog`
keeps it in one object ([ATOMIC_FILE_LOG.md](ATOMIC_FILE_LOG.md): the catalog
file's layout and commits), and `LogCatalog` runs on any `SharedLog`.

For inline-table extensions, see:
- [SPEC_TM.md](SPEC_TM.md) — inline table metadata (field 13 of `Checkpoint`,
  `CreateTableInline` / `UpdateTableInline` actions, `TableMetadataDelta`).
- [SPEC_ML.md](SPEC_ML.md) — inline manifest lists (fields 7–8 of
  `InlineTable`, `AddManifestDelta` / `RemoveManifestDelta` updates).

For an interactive byte-level walkthrough of these structures, open
[format-explorer/index.html](format-explorer/index.html) in a browser.

## Overview

The catalog's state — namespaces, their properties, and tables with their
metadata — is a `CatalogState`. A checkpoint encodes all of it; an intention
encodes one transaction's actions, each naming the versions it depends on.
Replay applies an intention where its record lands if every action's
preconditions hold there, binding the transaction's new entities to ids as it
goes, and skips it otherwise.

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

## Protobuf Schema

Two schemas, one inside the other. The envelope of the checkpoint and of
each record is the intention log's, `log.proto` (package `intentlog`,
[LOG.md](LOG.md) §"The envelope"): a `LogCheckpoint` carries the log's
identity (the catalog's UUID), the committed set and, in `state`, the
catalog's `Checkpoint`; a `LogRecord` carries the transaction's id and, in
`intention`, the catalog's `Intention`, or else a GC record. The catalog's
messages are defined in `iceberg-fileio-catalog/src/main/proto/catalog.proto`,
package `iceberg.catalog`.

### Checkpoint

The state of a `LogCheckpoint`: the full materialized catalog. Every
compaction writes one: a replace of the catalog file, or a checkpoint a shared
log stores apart from its records.

```protobuf
message Checkpoint {
  reserved 1, 20;                         // were catalog_uuid and committed_transactions
  // Replay binds a transaction's late-bound ids to these, in log order.
  int32  next_namespace_id         = 2;
  int32  next_table_id             = 3;
  // The reader version this checkpoint needs (ProtoCodec
  // SUPPORTED_READER_VERSION, now 1). Written first; required.
  int32  min_reader_version        = 4;
  // The catalog's conflict encoding; absent until a record sets one
  // (SetConflictEncoding), and then replay judges by "lamport".
  string conflict_encoding         = 5;
  repeated Namespace          namespaces             = 10;
  repeated Table              tables                 = 11;  // pointer-mode tables
  repeated NamespaceProperty  namespace_properties   = 12;
  repeated InlineTable        inline_tables          = 13;  // inline-mode tables
}
```

The encoder writes `min_reader_version` before every other field, so a reader
that cannot read the checkpoint fails before decoding the rest; the decoder
fails if it is absent or exceeds `ProtoCodec.SUPPORTED_READER_VERSION`.

A table ID appears in either `tables` (pointer mode) or `inline_tables`
(inline mode), never both.

`conflict_encoding` names how replay decides whether a table update
conflicts with what landed since its writer read the table ([Conflict
Encodings](#conflict-encodings)). A reader that does not know the encoding
fails the read: its replay could differ.

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
  bytes  conflict_state    = 6;   // the conflict encoding's state of the
                                  //   table, in its own layout; absent: none
}

message NamespaceProperty {
  int32  namespace_id = 1;
  string key          = 2;
  string value        = 3;
}
```

For `InlineTable`, see [SPEC_TM.md](SPEC_TM.md).

### Intention

The intention of a `LogRecord`: a transaction's actions. A commit appends it
in one record, whose `id` is the transaction id.

```protobuf
message Intention {
  reserved 1, 2;                         // were transaction_id and `sealed`
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
    SetConflictEncoding     set_conflict_encoding     = 12;
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

// Sets the catalog's conflict encoding. The catalog's first record carries
// it; it applies only to a catalog with no encoding that has never had a
// table, so no record is judged under one encoding and then another.
message SetConflictEncoding {
  string name = 1;
}
```

Inline action messages (`CreateTableInline`, `UpdateTableInline`) and the
delta hierarchy under `TableMetadataDelta` live in [SPEC_TM.md](SPEC_TM.md);
manifest-list delta updates live in [SPEC_ML.md](SPEC_ML.md).

## Worked examples

Two end-to-end layouts of a catalog file ([ATOMIC_FILE_LOG.md](ATOMIC_FILE_LOG.md)),
a checkpoint and the records after it. The RFC frames are schematic (see the
caveat under its §"Layout"); a protobuf body's internal fields are listed in
the grammar rather than placed at fixed offsets. With the default retention,
every replace writes a GC record between its checkpoint and its own record;
here it drops nothing.

### Create a namespace and a table

The file `FileIOCatalog.initialize` creates, then `createNamespace("db")` and
`createTable("db.t")`, shown *before* compaction so the appended records are
visible (the format explorer's `empty-bootstrap` and
`txn-multi-action-with-late-bind` scenarios walk the bytes the code writes):

```abnf
file       = header checkpoint gc record-1 record-2 record-3
checkpoint = framed<LogCheckpoint{ min_reader_version=1, log_id=…,
               state=Checkpoint{ min_reader_version=1,
                 next_namespace_id=1, next_table_id=1,
                 Namespace{id=0, version=0, parent_id=0, name=""} } }>   ; empty
gc         = framed<LogRecord{ advance_horizon=AdvanceHorizon{
               watermark_ms=now - retention } }>                         ; drops nothing
record-1   = framed<LogRecord{ id=…, intention=Intention{
               SetConflictEncoding(name="lamport") } }>                  ; the encoding
record-2   = framed<LogRecord{ id=…, intention=Intention{
               CreateNamespace(id=-1, version=1,
                               parent_id=0, parent_version=0, name="db") } }>
record-3   = framed<LogRecord{ id=…, intention=Intention{
               CreateTable(id=-1, version=1,
                           namespace_id=1, namespace_version=1,
                           name="t", metadata_location="…/meta-A.json") } }>
```

`FileIOCatalog.initialize` creates the file with a replace of the absent
object ([ATOMIC_FILE_LOG.md](ATOMIC_FILE_LOG.md) §"Commits on one object"): a
checkpoint holding only the root row, a GC record, then the bootstrap
record, which sets the catalog's conflict encoding
(`fileio.catalog.conflict.encoding`, default `lamport`) and whose id joins
the committed set like any other. Replay
binds record 2's namespace -1 to `next_namespace_id`, 1, and bumps the root to
version 1; `parent_version` is the root's version when the writer read it.
Record 3's writer read `db` as id 1 at version 1; replay binds the table -1 to
id 1 and bumps `db` to version 2. Created in one transaction, the two actions
would share one record, the `CreateTable` naming the namespace by its
late-bound id (`namespace_id=-1, namespace_version=-1`).

<!-- protocol "Magic = LCAT:32,Format Version = 2:32,CP Length (varint):16,Checkpoint - empty (root row; next ids 1):48,GC Length (varint):16,GC record - AdvanceHorizon (drops nothing):48,Rec1 Length (varint):16,Record 1 - SetConflictEncoding (initialize):48,Rec2 Length (varint):16,Record 2 - CreateNamespace (id=-1 v=1 db):48,Rec3 Length (varint):16,Record 3 - CreateTable (id=-1 v=1 ns=1 t):48" -->
```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Magic = LCAT                         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Format Version = 2                      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|       CP Length (varint)      |                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|           Checkpoint - empty (root row; next ids 1)           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|       GC Length (varint)      |                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|           GC record - AdvanceHorizon (drops nothing)          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|      Rec1 Length (varint)     |                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|          Record 1 - SetConflictEncoding (initialize)          |
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

A commit that replaces the file next, say an `UpdateTableLocation` of `t`,
writes a checkpoint of the replayed state, a GC record, and its own record:

```
checkpoint: log_id   committed: the ids of records 1-3
            state: min_reader_version=1  next_ns_id=2  next_tbl_id=2
              Namespace{ id=0  v=1  parent=0  "" }     ; root, bumped by the create of db
              Namespace{ id=1  v=2  parent=0  "db" }   ; bumped by the create of t
              Table{ id=1  v=1  ns=1  "t" -> …/meta-A.json }
gc        : advance_horizon: watermark_ms = now - retention, below this record's id
record 1  : id   intention: UpdateTableLocation(id=1, version=1,
                                                metadata_location="…/meta-B.json")
```

The checkpoint holds the committed set exactly as replay reached it (I11);
the GC record after it drops the ids at or below its watermark. Replay
verifies and applies record 1 on top of the checkpoint, as it would an
append, leaving `t` at version 2.

### Multi-table transaction

Four tables already in the checkpoint; one atomic `CatalogTransaction` appends a
single record whose four `UpdateTableLocation` actions each match the table's
current version and advance it by one:

```abnf
file       = header checkpoint record-1
checkpoint = framed<LogCheckpoint{ …, state=Checkpoint{ …, next_table_id=5,
               Table{id=1,version=3,name="a"}, Table{id=2,version=5,name="b"},
               Table{id=3,version=2,name="c"}, Table{id=4,version=7,name="d"} } }>
record-1   = framed<LogRecord{ id=…, intention=Intention{
               UpdateTableLocation(id=1, version=3, metadata_location="…/a-4.json"),
               UpdateTableLocation(id=2, version=5, metadata_location="…/b-6.json"),
               UpdateTableLocation(id=3, version=2, metadata_location="…/c-3.json"),
               UpdateTableLocation(id=4, version=7, metadata_location="…/d-8.json") } }>
```

<!-- protocol "Magic = LCAT:32,Format Version = 2:32,CP Length (varint):16,Checkpoint - Tables a(v3) b(v5) c(v2) d(v7):48,Record Length (varint):16,Record - 4x UpdateTableLocation bumps a-d:48" -->
```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Magic = LCAT                         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Format Version = 2                      |
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

The codecs use standard protobuf encoding:

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
fields are written only when present: the envelope's `committed` (omitted by
`LogEnvelope` when the set is empty with no horizon),
`highest_dropped_timestamp_ms` (omitted when 0), the one case of `UpdateTableInline`'s payload, and the optional inline
fields SPEC_TM and SPEC_ML describe. `ManifestFileEntry` omits zero or null
counts, a zero spec id or sequence number, and the content of a data manifest. A decoder takes an
absent scalar as zero or empty, but some fields are required, and their absence
fails the read: `LogCheckpoint.min_reader_version`, `log_id` and `state`,
`Checkpoint.min_reader_version`, a `LogRecord`'s body and, with an intention,
its id,
every inline-metadata codec tag (`InlineTable.metadata_codec`,
`CreateTableInline.metadata_codec`, and `UpdateTableInline.full_metadata_codec`
with `full_metadata`; 0 included), and `TableMetadataDelta.last_updated_ms`.

## Replay

The log replays each record ([LOG.md](LOG.md) §"Replay"): it skips a record
whose id the committed set holds or that is at or below its GC horizon, and
one whose id is not a UUIDv7; it decodes the rest with `CatalogStateMachine`,
and applies an intention if `CatalogIntention.verify` accepts it on the state
the record lands on ([Action Verification Rules](#action-verification-rules)),
binding its late-bound ids ([Late-Binding](#late-binding)). A record that
fails verification is skipped silently, not an error: concurrent writers
append transactions that conflict, and the one that lands second has no
effect.

Inline-mode `UpdateTableInline` actions in delta mode fold their delta into
the replayed state with `InlineDeltaCodec.applyDeltaWithManifests`. The
replay must be byte-stable so that every reader of the same records rebuilds
the same metadata, whatever its clock — see [SPEC_TM.md](SPEC_TM.md)
§"Replay determinism" for the two pinning rules.

## Conflict Encodings

A table action's precondition is decided by the catalog's conflict
encoding (`ConflictEncoding`), which the bootstrap record sets
(`SetConflictEncoding`) and every checkpoint keeps (`conflict_encoding`).
An encoding takes a table action of a record (`TableAction`: the table,
the version its writer read, and its payload, decoded once), extracts a
conflict witness from it, and validates the witness against the table
where the record lands (`ReplayView`) and against what the encoding keeps
of the table (`EncodingState`, in `Table.conflict_state` /
`InlineTable.conflict_state`). `lamport`, the default, is the Lamport
clock: an action applies only on the version its writer read. Other
encodings separate a table's concerns, so that replay applies a commit
Iceberg's retry would have repaired, merged onto the commits that landed
first.

- `UpdateTableInline`, `UpdateTableLocation` and `ReadTable` verify through
  the encoding (`ConflictEncodings.admits`); their applies, and a rename's,
  let it observe the change. Namespace actions, creates, drops and renames
  keep their own version checks.
- Whatever the encoding, an action applies on a version of its table other
  than the one its writer read only if it is an unpinned inline delta
  whose late-bound fields can be finalized there ([SPEC_TM.md](SPEC_TM.md)
  §"Late-bound fields"). A metadata location, full metadata, a read pin
  and a pinned update (a table a catalog transaction writes, errata D12)
  apply only on that version.
- The table's version becomes its version where the record lands, plus
  one.
- An encoding is deterministic (S1), stable (S2: a rejection stays a
  rejection, guarded by evidence that only accumulates in its state), and
  total (it never throws); it coarsens to the Lamport witness where it
  cannot see more, and admits only what merges exactly. Each encoding's
  test extends `ConflictEncodingContract`, and `TestRebaseProperties`
  checks that a record it admits on a changed table is what Iceberg's retry
  of the operation commits there.

## Commits

A writer stages a transaction on a read (`CatalogMutation`, on the `Replica`
`CatalogLog.read` returns), and `CatalogMutation.intention()` turns its
pending changes into actions ([Late-Binding](#late-binding)).
`CatalogLog.commit` commits the intention with the log's commit loop
([LOG.md](LOG.md) §"The commit loop") and maps its outcomes onto Iceberg's:

| The log's outcome | The catalog throws |
|---|---|
| returned | nothing: the transaction took effect, once |
| rejected, `CONFLICT` | `CatalogMutation.explain` of the state it lost to: `AlreadyExistsException` if a pending create or rename destination now collides, `NoSuchTableException` if a rename's source is gone (checked first, as `renameTable` does), else `CommitFailedException` |
| rejected, any other reason | `CommitFailedException` |
| unknown | `CommitStateUnknownException` |

Iceberg deletes a commit's new files on a `CleanableFailure` such as
`CommitFailedException` (and, for table operations that do not require strict
cleanup, on any failure but `CommitStateUnknownException`), so reporting a
landed commit any other way loses the files its snapshot references (I10).
The catalog restages namespace operations, renames, drops and table creates
on a fresh read and retries their `CommitFailedException`, up to Iceberg's
default number of attempts (`LogCatalog.commitOnAFreshRead`, `dropTable`,
`FileIOTableOperations.commitCreate`). `commitTransaction` does not retry: a
catalog transaction conflicts on the whole version of each table it pins, a
table it writes from when it first read it ([errata.md](errata.md) §D12).

The catalog's log properties (`LogCatalogProperties`) configure the commit
loop: `fileio.catalog.max.append.count` and `max.append.size` set when it
compacts ([ATOMIC_FILE_LOG.md](ATOMIC_FILE_LOG.md) §"Commits on one object"),
`fileio.catalog.committed.retention.ms` how long the committed set keeps an id
([LOG.md](LOG.md) §"The committed set and GC"), and
`fileio.catalog.throttle.retry.min-wait-ms` the backoff after a throttled
write.

## Late-Binding

Writers never allocate entity IDs. A create action carries a negative
late-bound ID, and replay binds it to a real ID:

- `CatalogMutation.intention()` sorts new namespaces by depth (parents
  before children) and numbers new namespaces and tables `-1, -2, ...`
  (separate sequences). Actions later in the same transaction reference
  a new entity by its late-bound ID.
- A create's own `version` is 1, the version replay gives the new
  entity. A version field that names another entity (`parent_version`,
  `namespace_version`, `new_namespace_version`) is `-1` when that entity
  is created in the same txn.
- On replay, `CatalogIntention.apply` binds each late-bound ID, at its
  defining create, to the replaying state's next namespace / table ID
  (`ProtoCodec.IdBinding`), and resolves later references to it. Every
  reader replays the log in the same order, so every reader assigns the
  same IDs.
- `CatalogIntention.verify` rejects a record that references a late-bound
  ID before (or without) the create that defines it, that defines one
  twice, or whose create names a concrete ID, which could bring back an ID
  a drop retired.

Binding at replay is what keeps concurrent creates apart. When writers
allocated IDs from their own snapshot, two writers that read the same
snapshot and created unrelated entities -- e.g. tables in different
namespaces -- wrote records with the same ID. Both records verified,
and replay let the second overwrite the first.

## State Model

The catalog state is held in `CatalogState` (immutable snapshot). A read of
the log (`Replica`, [LOG.md](LOG.md) §"Replay") pairs it with the log's
identity (`logId`, a UUIDv7), its committed set (`committed`, the inner
`CommittedTransactions` bytes, queried in place), the tail the read ended at,
and the number of intention records after the checkpoint
(`intentionRecords`):

| Field                | Type                             | Description                          |
|----------------------|----------------------------------|--------------------------------------|
| `nextNamespaceId`    | `int`                            | Next namespace ID replay binds       |
| `nextTableId`        | `int`                            | Next table ID replay binds           |
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
| `conflictEncoding`   | `String`                         | The conflict encoding's name; null: unset (`lamport`) |
| `conflictStates`     | `Map<Integer, EncodingState>`    | Table ID → the encoding's state of it |

Inline-only fields (`tblInlineMetadata`, `tblInlineMetadataCodec`,
`tblManifestPrefix`, `manifestPool`, `snapshotManifests`) are described in
[SPEC_TM.md](SPEC_TM.md) and [SPEC_ML.md](SPEC_ML.md). The committed set is
queried in place (`CommittedSet.contains`); `ids()` decodes a `Set<UUID>` on
demand, for tests and tooling.

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
  this, not replay: `CatalogMutation` treats the empty namespace as one
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

Replay applies a record only if the committed set admits its id (not in the
set, and above its GC horizon; [LOG.md](LOG.md) §"The committed set and GC"),
the id is a UUIDv7, and `CatalogIntention.verify` accepts it.
`CatalogIntention.verify` rejects a record whose:

- late-bound id is referenced before (or without) its defining create,
  or defined twice, or whose create names a concrete id
  (`ProtoCodec.IdBinding`);
- action references a namespace or table that another of its actions
  drops (`CatalogIntention.selfConsistent`, Bug 14): every action verifies
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
| UpdateTableLocation      | the conflict encoding admits it: the table version matches |
| ReadTable                | the conflict encoding admits it: the table version matches |
| CreateTableInline        | namespace version matches (skipped if -1)                 |
| UpdateTableInline        | the conflict encoding admits it ([Conflict Encodings](#conflict-encodings)): under `lamport`, the table version matches |
| RenameTable              | table version matches; destination namespace version matches (skipped if -1) |
| SetConflictEncoding      | the catalog has no encoding and has never had a table     |
| unknown action type      | always passes (`UnknownAction`)                           |

Each `apply()` bumps the version of any entity whose state it mutates: a
table update or rename bumps the table (an update from its version where
the record lands), and a create starts its entity at the record's
`version` (1). A namespace's version covers its children and
properties: creating, dropping or renaming a child bumps the parent
namespace (a rename bumps both, when they differ), and setting or removing
a property bumps that namespace. `ReadTable` and unknown actions change
nothing. See [design.md](design.md) for the operation × operation conflict
matrix that falls out of these rules.

## Schema Evolution

Protobuf's forward and backward compatibility applies: new field numbers
can be added without breaking old readers, and removed fields leave their
numbers reserved (`Intention` reserves 1 and 2, the former transaction id
and `sealed`; `Checkpoint` reserves 1 and 20, the former catalog UUID and
committed set, now the envelope's).

- Unknown fields are silently skipped during decode (`ProtoCodec.skipField`,
  for wire types 0, 1, 2 and 5); an unknown wire type throws, failing the
  read.
- An unknown action type decodes as `UnknownAction`, which always
  verifies and applies nothing; the record's other actions apply.
- A reader refuses a checkpoint or record that names a conflict encoding it
  does not know (`ConflictEncodings.byName`).
- `Checkpoint.min_reader_version` gates changes a reader cannot skip safely:
  every checkpoint carries `ProtoCodec.SUPPORTED_READER_VERSION` (1), and a
  reader refuses one that lacks the field or carries more than it supports. It
  gates checkpoints only: a record appended to a checkpoint is read by any
  reader that accepts the checkpoint.
- The envelope has its own reader version, `LogCheckpoint.min_reader_version`
  ([LOG.md](LOG.md) §"The envelope"), and the catalog file's header its
  format version ([ATOMIC_FILE_LOG.md](ATOMIC_FILE_LOG.md) §"Schema
  evolution").

## Key Files

| File | Purpose |
|------|---------|
| `iceberg-fileio-catalog/src/main/proto/catalog.proto` | Protobuf schema (catalog + inline TM + ML) |
| `iceberg-fileio-catalog/src/main/java/.../CatalogStateMachine.java` | The catalog as a state machine: checkpoints and intentions |
| `iceberg-fileio-catalog/src/main/java/.../CatalogState.java` | Immutable catalog state + builder |
| `iceberg-fileio-catalog/src/main/java/.../ProtoCodec.java` | Wire encoding/decoding, `CatalogIntention`, action types |
| `iceberg-fileio-catalog/src/main/java/.../InlineDeltaCodec.java` | Inline TM/ML delta encode/decode/apply |
| `iceberg-fileio-catalog/src/main/java/.../CatalogMutation.java` | A transaction staged on a read, with its intention |
| `iceberg-fileio-catalog/src/main/java/.../CatalogLog.java` | The catalog's log: reads, commits, Iceberg's exceptions |
| `iceberg-fileio-catalog/src/main/java/.../LogCatalog.java` | Catalog + `FileIOTableOperations`, over any log |
| `iceberg-fileio-catalog/src/main/java/.../FileIOCatalog.java` | `LogCatalog` over an `AtomicFileLog` |
| `intentlog-core/src/main/proto/log.proto` | The envelope ([LOG.md](LOG.md)) |
| `intentlog-fileio/src/main/java/.../AtomicFileLog.java` | The catalog file's layout ([ATOMIC_FILE_LOG.md](ATOMIC_FILE_LOG.md)) |
