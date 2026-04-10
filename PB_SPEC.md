# ProtoCatalogFormat (PB) Specification

This document specifies the protobuf-based catalog format implemented by `ProtoCatalogFormat`.
It uses the same checkpoint + append log architecture as `LogCatalogFormat` but encodes all
records using protobuf-compatible wire format.

## Overview

A catalog file is a single object stored in cloud storage. It encodes the full state of the
catalog -- namespaces, tables, and their properties -- using protobuf messages. Like LCF, it
supports two mutation strategies:

- **Append**: append a length-prefixed transaction record (offset-validated)
- **CAS (compare-and-swap)**: atomically replace the entire file (ETag-validated)

Both strategies rely on cloud-storage conditional writes (`SupportsAtomicOperations`).

The codec (`ProtoCodec`) implements protobuf wire format manually -- it does not use generated
protobuf classes. The encoding follows standard protobuf conventions (varints, field tags,
length-delimited fields) and is wire-compatible with `catalog.proto`.

## File Layout

```
+------------------------------------------------------------------+
|                     Catalog File                                 |
+==================================================================+
| Magic              (4 bytes, "LCAT")                             |
| Format Version     (4 bytes, big-endian int32, currently 1)      |
+------------------------------------------------------------------+
| Checkpoint Length   (varint)                                     |
| Checkpoint          (protobuf Checkpoint message)                |
+------------------------------------------------------------------+
| Transaction 1 Length (varint)                                    |
| Transaction 1       (protobuf Transaction message)               |
+------------------------------------------------------------------+
| Transaction 2 Length (varint)                                    |
| Transaction 2       (protobuf Transaction message)               |
+------------------------------------------------------------------+
| ...                                                              |
+------------------------------------------------------------------+
```

The header is 8 bytes fixed. Everything after the header is variable-length, using varint
length prefixes for each record. There is no separate committed-transaction-ID block; the
checkpoint message carries that set internally.

## Header

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
  repeated Namespace              namespaces              = 10;
  repeated Table                  tables                  = 11;   // pointer-mode tables only
  repeated NamespaceProperty      namespace_properties    = 12;
  repeated InlineTable            inline_tables           = 13;   // inline-mode tables
  repeated bytes                  committed_transaction_ids = 20;  // 16-byte UUIDs
}
```

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

message InlineTable {
  int32  id                   = 1;
  int32  version              = 2;
  int32  namespace_id         = 3;
  string name                 = 4;
  bytes  metadata             = 5;  // TableMetadata as JSON bytes
  string manifest_list_prefix = 6;  // per-table dictionary: shared manifest path prefix
}
```

A table ID appears in either `tables` (pointer mode) or `inline_tables` (inline mode),
never both. Inline tables store the full `TableMetadata` as opaque JSON bytes in the
checkpoint and carry a `manifest_list_prefix` for efficient snapshot delta encoding.
Pointer-mode tables store only an external metadata file location.

### Transaction

Appended atomically to the log region.

```protobuf
message Transaction {
  bytes          transaction_id = 1;    // UUIDv7, 16 bytes
  bool           sealed         = 2;    // signals compaction needed
  repeated Action actions       = 3;
}

message Action {
  oneof action {
    CreateNamespace        create_namespace         = 1;
    DropNamespace          drop_namespace           = 2;
    SetNamespaceProperty   set_namespace_property   = 3;
    RemoveNamespaceProperty remove_namespace_property = 4;
    CreateTable            create_table             = 5;
    DropTable              drop_table               = 6;
    UpdateTableLocation    update_table_location    = 7;
    ReadTable              read_table               = 8;
    UpdateTableInline      update_table_inline      = 9;   // inline table update
    CreateTableInline      create_table_inline      = 10;  // inline table create
  }
}
```

### Action Messages

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
```

### Inline Table Action Messages

These actions operate on tables whose metadata is stored directly in the catalog
rather than in an external metadata.json file. See
[INLINE_INTENTION.md](INLINE_INTENTION.md) for the full design.

```protobuf
message CreateTableInline {
  int32  id                = 1;   // -1 for late-bind
  int32  version           = 2;
  int32  namespace_id      = 3;
  int32  namespace_version = 4;   // -1 if late-bound
  string name              = 5;
  bytes  metadata          = 6;   // full TableMetadata as JSON bytes
}

message UpdateTableInline {
  int32 id      = 1;
  int32 version = 2;              // must match current

  oneof payload {
    TableMetadataDelta delta             = 3;  // structured metadata changes
    bytes              full_metadata     = 4;  // full TableMetadata JSON
    string             metadata_location = 5;  // pointer fallback (eviction)
  }
}
```

**CreateTableInline** creates a table with its full `TableMetadata` inlined. The
verification rule is the same as `CreateTable` (namespace version check, late-bind
support).

**UpdateTableInline** updates an inline table in one of three modes:
- **Delta** (`delta`): applies structured metadata changes. The typical path for
  data commits -- encodes only what changed (~100 bytes for an AddSnapshot).
- **Full** (`full_metadata`): replaces the entire inline metadata. Used on
  initial creation, re-inlining, or when the delta exceeds the size budget.
- **Pointer** (`metadata_location`): evicts the table from inline to pointer mode.
  The inline metadata is removed and replaced with an external file location.

Version verification is the same as `UpdateTableLocation`.

### Table Metadata Delta Messages

The delta payload is a sequence of typed metadata updates, each representing
one logical change to the table's `TableMetadata`. See
[INLINE_INTENTION.md](INLINE_INTENTION.md) for the full dictionary design.

```protobuf
message TableMetadataDelta {
  repeated TableMetadataUpdate updates = 1;
}

message TableMetadataUpdate {
  oneof update {
    AddSnapshot             add_snapshot               = 1;
    SetSnapshotRef          set_snapshot_ref           = 2;
    RemoveSnapshots         remove_snapshots           = 3;
    AddSchema               add_schema                 = 4;
    SetCurrentSchema        set_current_schema         = 5;
    AddPartitionSpec        add_partition_spec         = 6;
    SetDefaultPartitionSpec set_default_partition_spec = 7;
    AddSortOrder            add_sort_order             = 8;
    SetDefaultSortOrder     set_default_sort_order     = 9;
    SetTableProperties      set_properties             = 10;
    SetTableLocation        set_table_location         = 11;
  }
}

message AddSnapshot {
  fixed64        snapshot_id          = 1;
  string         manifest_list_suffix = 2;   // appended to per-table prefix
  CompactSummary summary              = 3;   // typed summary (replaces map)
  sint64         timestamp_delta_ms   = 4;   // signed delta from lastUpdatedMs
  int32          schema_id            = 5;   // only if changed from current
  int64          added_rows           = 6;   // v3+
}

enum OperationType { APPEND = 0; OVERWRITE = 1; DELETE = 2; REPLACE = 3; }

message CompactSummary {
  OperationType      operation              = 1;
  int64              added_data_files       = 2;
  int64              added_records          = 3;
  int64              added_files_size       = 4;
  int64              total_data_files       = 5;
  int64              total_records          = 6;
  int64              total_files_size       = 7;
  int64              added_delete_files     = 8;
  int64              total_delete_files     = 9;
  int64              total_equality_deletes = 10;
  int64              total_position_deletes = 11;
  map<string,string> extra                  = 15;
}

message SetSnapshotRef {
  string ref_name              = 1;
  int64  snapshot_id           = 2;
  string ref_type              = 3;   // "branch" or "tag"
  int32  min_snapshots_to_keep = 4;
  int64  max_snapshot_age_ms   = 5;
  int64  max_ref_age_ms        = 6;
}

message RemoveSnapshots          { repeated int64 snapshot_ids = 1; }
message AddSchema                { int32 schema_id = 1; int32 last_column_id = 2; bytes schema_json = 3; }
message SetCurrentSchema         { int32 schema_id = 1; }
message AddPartitionSpec         { int32 spec_id = 1; int32 last_partition_id = 2; bytes spec_json = 3; }
message SetDefaultPartitionSpec  { int32 spec_id = 1; }
message AddSortOrder             { int32 order_id = 1; bytes order_json = 2; }
message SetDefaultSortOrder      { int32 order_id = 1; }
message SetTableProperties       { map<string,string> updated = 1; repeated string removed = 2; }
message SetTableLocation         { string location = 1; }
```

`AddSnapshot` uses dictionary-derived fields: the per-table `manifest_list_prefix`
is prepended to `manifest_list_suffix`, the timestamp is reconstructed from
`state.lastUpdatedMs + timestamp_delta_ms`, the sequence number is
`state.lastSequenceNumber + 1`, and the parent snapshot is `state.currentSnapshotId`.
A typical data commit delta is ~80-115 bytes.

## Wire Format Details

The codec uses standard protobuf encoding:

- **Field tag** = `(field_number << 3) | wire_type`
- **Wire type 0** (varint): `int32`, `bool`
- **Wire type 2** (length-delimited): `string`, `bytes`, nested messages

Varints use the standard protobuf unsigned encoding (7 bits per byte, MSB continuation).
Signed integers use protobuf's default zigzag-less encoding, so negative values (used for
late-binding sentinels) occupy 10 bytes on the wire.

Default-valued fields (zero for ints, false for bools, empty for strings/bytes) are omitted
per proto3 conventions.

## Read Protocol

```
function READ(fileIO, location):
    bytes = fileIO.read(location)
    stream = InputStream(bytes)

    # 1. Read and validate header
    header = stream.read(8)
    assert header[0..4] == "LCAT"
    assert readInt(header, 4) == 1          # format version

    # 2. Read checkpoint
    chkLen = readVarint(stream)
    chkBytes = stream.read(chkLen)
    state = decodeCheckpoint(chkBytes)       # populates UUID, IDs, entities, committed txns

    # 3. Read and apply transaction log
    while stream.hasRemaining():
        txnLen = readVarint(stream)
        txnBytes = stream.read(txnLen)
        txn = decodeTransaction(txnBytes)

        if state.committedTxn.contains(txn.id):
            continue                         # already applied (idempotent)
        if txn.verify(state):
            txn.apply(state)
            state.committedTxn.add(txn.id)
        if txn.sealed:
            state.sealed = true
            break                            # stop after sealed txn

    return freeze(state)
```

Note: unlike LCF, a transaction that fails verification during read is silently skipped
rather than treated as corruption. This is because the protobuf format expects that
concurrent writers may append transactions that conflict, and the correct behavior is to
ignore them.

## Commit Protocol

The commit protocol is structurally identical to LCF:

```
function COMMIT(fileIO, original, mutations):
    txn      = buildTransaction(original, mutations)
    txnBytes = encodeTransaction(txn)
    current  = original.location

    # Case 0: catalog does not exist yet
    if not current.exists():
        return tryCAS(current, txnBytes, fileIO)
            .orElseThrow(CommitFailed)

    for attempt in 0..9:

        # Case 1: sealed - must compact via CAS
        if original.sealed:
            unseal(txnBytes)
            result = tryCAS(current, txnBytes, fileIO)
            if result.present:
                return validateCommit(result, txn)

        # Case 2: normal append
        else:
            if current.length + len(txnBytes) > 16 MB:
                seal(txnBytes)
            result = tryAppend(current, txnBytes, fileIO)
            if result.present:
                return validateCommit(result, txn)

        # Refresh for retry
        oldLength = current.length
        current = fileIO.newInputFile(current.location)
        if current.length < oldLength:
            unseal(txnBytes)                 # file was compacted
        original = READ(fileIO, current)

    throw CommitFailed("exceeded retry limit")
```

### tryCAS (compare-and-swap)

Builds a full file (header + checkpoint + transaction) and writes it atomically, conditioned
on the ETag / generation number of the existing file.

```
function tryCAS(current, txnBytes, fileIO):
    outputFile = fileIO.newAtomicOutputFile(current)
    fullFile   = buildFullFile(original, txnBytes)
    token      = outputFile.prepare(fullFile, Strategy.CAS)
    try:
        written = outputFile.writeAtomic(token, fullFile)
        return Some(READ(written))
    catch CASException:
        return None
```

`buildFullFile` creates: `[MAGIC][VERSION][varint(chkLen)][checkpoint][varint(txnLen)][txn]`

The checkpoint is encoded from the merged state (original + pending mutations), so the
resulting file is fully compacted.

### tryAppend (offset-validated)

Appends only the transaction record (`[varint(txnLen)][txn]`) to the end of the file.

```
function tryAppend(current, txnBytes, fileIO):
    outputFile = fileIO.newAtomicOutputFile(current)
    record     = [varint(len(txnBytes))] + txnBytes
    token      = outputFile.prepare(record, Strategy.APPEND)
    try:
        written = outputFile.writeAtomic(token, record)
        return Some(READ(written))
    catch AppendException:
        return None
```

### Seal / Unseal

The sealed flag is a `bool` field (field number 2) in the `Transaction` message. The codec
provides `sealTransaction()` and `unsealTransaction()` methods that mutate the serialized
bytes in place by locating the field tag for field 2 and toggling the varint value.

## Compaction

Same trigger and mechanics as LCF:

1. Writer detects `file_size + txn_size > 16 MB` and sets `sealed = true`.
2. Next writer sees `sealed = true` and performs a full CAS.
3. CAS builds a fresh checkpoint from the fully-replayed state.
4. New file: header + checkpoint + transaction (minimal log).

The committed-transaction-ID set is carried forward in the checkpoint for idempotency.

## Late-Binding

When a transaction creates a namespace or table and references it within the same
transaction, the entity ID is not yet allocated. The format uses negative integers as
virtual IDs:

- `ProtoIdManager` maintains allocation counters (`nextNsid`, `nextTblid`) and a remap
  table (`virtualId -> realId`).
- During `buildActions()`, new namespaces are sorted by depth (parents before children)
  and assigned IDs from `idManager.allocateNsid()`.
- Version fields are set to `-1` when the referenced entity was created in the same txn.
- On replay, `resolve(id)` maps negative IDs to their allocated counterparts.

## State Model

The catalog state is held in `ProtoCatalogFile` (immutable snapshot), mirroring LCF's
`LogCatalogFile`:

| Field              | Type                            | Description                          |
|--------------------|---------------------------------|--------------------------------------|
| `uuid`             | `UUID`                          | Catalog identity (UUIDv7)            |
| `nextNamespaceId`  | `int`                           | Next namespace ID to allocate        |
| `nextTableId`      | `int`                           | Next table ID to allocate            |
| `sealed`           | `boolean`                       | Whether a sealed txn was encountered |
| `namespaceIds`     | `Map<Namespace, Integer>`       | Namespace -> internal ID             |
| `namespaceVersions`| `Map<Integer, Integer>`         | Namespace ID -> version counter      |
| `namespaceProps`   | `Map<Integer, Map<Str, Str>>`   | Namespace ID -> properties           |
| `tableIds`         | `Map<TableIdentifier, Integer>` | Table -> internal ID                 |
| `tableVersions`    | `Map<Integer, Integer>`         | Table ID -> version counter          |
| `tableLocations`   | `Map<Integer, String>`          | Table ID -> metadata file location (pointer tables only) |
| `tblInlineMetadata`| `Map<Integer, byte[]>`          | Table ID -> TableMetadata JSON bytes (inline tables only) |
| `tblManifestPrefix`| `Map<Integer, String>`          | Table ID -> manifest list path prefix (inline tables only) |
| `committedTxns`    | `Set<UUID>`                     | Applied transaction IDs              |

A table ID appears in either `tableLocations` (pointer mode) or `tblInlineMetadata`
(inline mode), never both. Every inline table also has a `tblManifestPrefix` entry.
Both pointer and inline tables share the same `tableIds` and `tableVersions` maps.

## Action Verification Rules

Identical to LCF. Each action's `verify()` checks version-based preconditions:

| Action                   | Precondition                                              |
|--------------------------|-----------------------------------------------------------|
| CreateNamespace          | parent version matches (skip if late-bound)               |
| DropNamespace            | namespace version matches                                 |
| SetNamespaceProperty     | namespace version matches (skip if late-bound)            |
| RemoveNamespaceProperty  | namespace version matches (skip if late-bound)            |
| CreateTable              | namespace version matches (skip if late-bound)            |
| DropTable                | table version matches                                     |
| UpdateTableLocation      | table version matches                                     |
| ReadTable                | table version matches                                     |
| CreateTableInline        | namespace version matches (skip if late-bound)            |
| UpdateTableInline        | table version matches                                     |

## Inline Table Metadata

The PB format supports **inline table metadata** -- storing `TableMetadata` directly
in the catalog rather than in external `metadata.json` files. This eliminates the
second write per table update and reduces per-commit payload from 2-700 KB (full
metadata rewrite) to ~80-170 bytes (delta mode for a typical data commit).

### Storage Model

A table is either **pointer** (external metadata file) or **inline** (metadata in
catalog). The `Checkpoint` message stores pointer tables in `tables` (field 11)
and inline tables in `inline_tables` (field 13). A table ID appears in exactly one.
Pointer and inline tables can coexist in the same catalog; individual tables can
transition between modes.

### Delivery Modes

When committing an update to an inline table, `FileIOTableOperations.doCommit()`
picks one of three modes based on the resulting transaction size:

- **Delta**: applies structured changes via `InlineDeltaCodec`. This is the common
  path for data commits -- an `AddSnapshot` + `SetSnapshotRef` typically encodes
  to ~80-115 bytes.
- **Full**: serializes the complete `TableMetadata` JSON. Used on initial creation
  or when the delta would exceed the append size budget.
- **Pointer**: writes `TableMetadata` to an external file and records only its
  location. Used as a fallback when even the full metadata exceeds the 4 MiB Azure
  `AppendBlock` limit.

`InlineDeltaCodec.selectMode(delta, newMeta, currentTxnSize)` picks the mode given
the current transaction size and a 4 MiB budget (`APPEND_LIMIT`).

### Delta Application

`InlineDeltaCodec.computeDelta(oldMeta, newMeta, manifestListPrefix)` diffs two
`TableMetadata` instances and produces a minimal list of `DeltaUpdate` objects
covering all 11 update types (new snapshots, removed snapshots, ref changes,
schema additions, partition specs, sort orders, property changes, location
changes).

On replay, `InlineDeltaCodec.applyDelta(baseMetadataJson, deltaBytes)` parses the
base metadata, applies each update via `TableMetadata.Builder`, and re-serializes.
`AddSnapshot` reconstructs derived fields (sequence number, parent snapshot,
timestamp) from the base state's corresponding values.

### Configuration

Set `fileio.catalog.inline=true` in catalog properties to enable inline mode.
When disabled (default), the catalog uses the traditional pointer path.

### Table Loading (No Base Class Changes)

`FileIOTableOperations.loadFromCatalogFile()` detects inline tables via
`CatalogFile.isInlineTable()` and calls
`BaseMetastoreTableOperations.refreshFromMetadataLocation(syntheticLoc, null, 0,
customLoader)` with a custom `Function<String, TableMetadata>` that parses from
the catalog's inline bytes. **No changes to `BaseMetastoreCatalog` or
`BaseMetastoreTableOperations` in the iceberg/ fork are required** -- the
existing protected API supports this integration cleanly.

### Commit Protocol (Inline Mode)

```
function doCommit(base, newMetadata):
    if isCreate:
        bytes = TableMetadataParser.toJson(newMetadata).getBytes(UTF_8)
        format.from(lastCatalogFile)
            .createTableInline(tableId, bytes)
            .commit(io)
        return

    # Compute delta using the per-table manifest list prefix
    prefix = lastCatalogFile.manifestListPrefix(tableId)
    delta  = InlineDeltaCodec.computeDelta(base, newMetadata, prefix)
    mode   = InlineDeltaCodec.selectMode(delta, newMetadata, 0)

    mut = format.from(lastCatalogFile)
    switch mode:
        case "delta":
            mut.updateTableInlineDelta(tableId, InlineDeltaCodec.encodeDelta(delta))
        case "full":
            mut.updateTableInline(tableId, newMetadata.toJson().getBytes(UTF_8))
        case "pointer":
            loc = writeMetadataFile(newMetadata)
            mut.updateTable(tableId, loc)
    mut.commit(io)
```

`commitTransaction()` (multi-table commits) uses the same delta/full/pointer
selection per table.

## Current Status

All 8 implementation stages are complete. See
[docs/INLINE_IMPL.md](docs/INLINE_IMPL.md) for the progress log.

- **Hand-rolled codec** -- `ProtoCodec` and `InlineDeltaCodec` implement protobuf
  wire format manually. This validates correctness against the `.proto` schema
  before committing to generated classes as a dependency.
- **Inline metadata** -- full integration with `FileIOCatalog`, using delta mode
  for data commits and falling back to full/pointer for oversized transactions.
- **Test coverage** -- 134 unit tests passing (`mvn test`). Cloud provider
  integration tests are separated via `maven-failsafe-plugin` and run with
  `mvn verify`.

## Key Files

| File | Purpose |
|------|---------|
| `src/main/proto/catalog.proto` | Protobuf schema definition (catalog + inline delta messages) |
| `src/main/java/.../ProtoCatalogFormat.java` | Format implementation (read, commit, CAS/append) |
| `src/main/java/.../ProtoCatalogFile.java` | Immutable catalog snapshot + builder (pointer + inline tables) |
| `src/main/java/.../ProtoCodec.java` | Protobuf wire encoding/decoding, catalog-level action types |
| `src/main/java/.../InlineDeltaCodec.java` | Delta encode/decode/apply, `computeDelta`, `selectMode` |
| `src/main/java/.../FileIOCatalog.java` | Catalog + `FileIOTableOperations` with inline integration |
| `src/main/java/.../CatalogFile.java` | Abstract base with inline-aware `Mut` API |
| `src/test/java/.../TestProtoCatalogFormat.java` | Codec and format tests |
| `src/test/java/.../TestProtoActions.java` | Systematic action tests (positive/negative/randomized/inline) |
| `src/test/java/.../TestInlineDelta.java` | Delta encode/decode/apply, `computeDelta`, `selectMode` tests |

---

## Comparison with LogCatalogFormat (LCF)

The two formats share the same high-level architecture (checkpoint + append log, CAS/append
commit strategies, 16 MB compaction threshold, version-based optimistic concurrency). The
differences are in encoding, file structure, and extensibility.

### Encoding

| Aspect | LCF | PB |
|--------|-----|-----|
| **Record format** | Custom opcode-based (`DataOutputStream`) | Protobuf wire format (field tags + varints) |
| **Opcodes** | 1-byte prefix per record (0x00--0x09) | Protobuf `oneof` field numbers (1--10) in `Action` |
| **Integer encoding** | Fixed 4-byte big-endian | Protobuf varint (1--5 bytes for positive values) |
| **String encoding** | Java modified UTF-8 (2-byte length prefix) | Protobuf length-delimited (varint prefix + UTF-8) |
| **UUID encoding** | Raw 16 bytes (MSB long + LSB long) | Protobuf `bytes` field (varint tag + varint length + 16 bytes) |
| **Boolean encoding** | 1 byte unsigned (0/1) | Protobuf varint (0 omitted per proto3 default) |
| **Negative ints** | 4-byte big-endian (compact) | Protobuf varint (10 bytes -- zigzag not used) |

PB is slightly more verbose for negative values (late-bind sentinels), but more compact for
small positive integers (common case for IDs and versions).

### File Structure

| Aspect | LCF | PB |
|--------|-----|-----|
| **Header** | 37-byte checkpoint header with embedded counters | 8-byte magic + version; counters inside checkpoint message |
| **Checkpoint** | Opcode stream (CREATE_NAMESPACE, ADD_NAMESPACE_PROPERTY, CREATE_TABLE) in deterministic order | Single protobuf `Checkpoint` message with repeated fields |
| **Committed txn IDs** | Separate block between checkpoint data and transaction log (4-byte count + 16-byte UUIDs) | Inside `Checkpoint` message as `repeated bytes` field |
| **Table embed region** | Reserved region (currently unused, 0 bytes) | Not present |
| **Transaction log** | TRANSACTION opcode records | Varint-prefixed protobuf `Transaction` messages |
| **Length prefixes** | Fixed 4-byte in header (`chkLen`, `committedTxnLen`) | Varint throughout |

PB has a simpler file structure: no reserved regions, no separate committed-txn block.
Everything is either header or varint-prefixed protobuf message.

### Schema Evolution and Forward Compatibility

| Aspect | LCF | PB |
|--------|-----|-----|
| **Unknown fields** | Must add explicit skip logic for new opcodes | Protobuf preserves unknown fields by default |
| **Adding fields** | Requires format version bump and custom parsing | Add new field number; old readers ignore it |
| **Removing fields** | Risky without versioned skip tables | Safe (field number reserved, old data still parseable) |
| **Tooling** | Custom -- no external tools can parse | Any protobuf tool can decode if given the `.proto` schema |

This is the primary motivation for the migration. LCF has no schema evolution story; every
change requires hand-written compatibility code. PB inherits protobuf's field-level forward
and backward compatibility.

### Behavioral Differences

| Aspect | LCF | PB |
|--------|-----|-----|
| **Failed verification on read** | Treated as corruption (error) | Silently skipped (conflict from concurrent writer) |
| **Seal/unseal** | Toggle byte at fixed offset 17 in transaction record | Locate protobuf field tag for field 2 and toggle varint |
| **Codec** | Inline in `LogCatalogFormat` (~1500 lines) | Separated: `ProtoCodec` (catalog) + `InlineDeltaCodec` (deltas) |
| **Inline metadata** | Not supported (pointer only) | Supported (delta/full/pointer mode selection) |
| **Generated code** | None | None yet (hand-rolled codec validates against `.proto`) |

### What Is Identical

- Commit protocol (CAS + append, 10-attempt retry)
- Compaction threshold (16 MB)
- Late-binding with negative virtual IDs
- Version-based optimistic concurrency (same verification rules)
- The 8 base action semantics (namespace/table CRUD) -- PB adds 2 inline actions
- State model for namespaces, pointer tables, and committed transactions
- Namespace hierarchy (parent pointers, root = ID 0)
- Idempotency via committed-transaction-ID tracking
