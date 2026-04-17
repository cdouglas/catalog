# ProtoCatalogFormat (PB) Specification

This document specifies the protobuf-based catalog format implemented by `ProtoCatalogFormat`,
the only catalog format used by `FileIOCatalog`. It uses a checkpoint + append log
architecture encoded via protobuf-compatible wire format.

## Overview

A catalog file is a single object stored in cloud storage. It encodes the full state of the
catalog -- namespaces, tables, and their properties -- using protobuf messages. It supports
two mutation strategies:

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
  string manifest_list_prefix = 6;  // per-table dictionary: shared manifest file path prefix
  repeated ManifestFileEntry     manifest_pool  = 7;  // unique manifests for this table
  repeated SnapshotManifestRefs  snapshot_refs  = 8;  // per-snapshot index lists into pool
}

message ManifestFileEntry {
  string manifest_path_suffix   = 1;   // relative to manifest_list_prefix
  int64  manifest_length        = 2;
  int32  partition_spec_id      = 3;
  int32  content                = 4;   // 0=data, 1=deletes
  int64  sequence_number        = 5;
  int64  min_sequence_number    = 6;
  int64  added_snapshot_id      = 7;   // fixed64 encoding
  int32  added_files_count      = 8;
  int32  existing_files_count   = 9;
  int32  deleted_files_count    = 10;
  int64  added_rows_count       = 11;
  int64  existing_rows_count    = 12;
  int64  deleted_rows_count     = 13;
  repeated PartitionFieldSummaryEntry partitions = 14;
  bytes  key_metadata           = 15;
  int64  first_row_id           = 16;
}

message PartitionFieldSummaryEntry {
  bool  contains_null = 1;
  bool  contains_nan  = 2;
  bytes lower_bound   = 3;
  bytes upper_bound   = 4;
}

message SnapshotManifestRefs {
  int64          snapshot_id  = 1;
  repeated int32 pool_indices = 2;  // positions in manifest_pool
}
```

A table ID appears in either `tables` (pointer mode) or `inline_tables` (inline mode),
never both. Inline tables store the full `TableMetadata` as opaque JSON bytes in the
checkpoint and carry a `manifest_list_prefix` for efficient snapshot delta encoding.
Pointer-mode tables store only an external metadata file location.

When inline manifest lists are enabled (`fileio.catalog.inline.manifests=true`), the
checkpoint also carries a pool of unique `ManifestFileEntry` records per table (field 7)
and per-snapshot index arrays (field 8) referencing pool positions. Each unique manifest
file is stored once in the pool; consecutive snapshots share most entries. Manifest file
paths are stored as suffixes relative to `manifest_list_prefix` (field 6).

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
    AddManifestDelta        add_manifest               = 12;
    RemoveManifestDelta     remove_manifest            = 13;
  }
}

message AddManifestDelta {
  int64              snapshot_id = 1;   // which snapshot this manifest belongs to
  ManifestFileEntry  manifest    = 2;   // full manifest entry (see ManifestFileEntry)
}

message RemoveManifestDelta {
  int64  snapshot_id           = 1;
  string manifest_path_suffix  = 2;   // identifies manifest to remove (relative to prefix)
}

message AddSnapshot {
  fixed64        snapshot_id          = 1;
  string         manifest_list_suffix = 2;   // appended to per-table prefix; empty for inline-ML snapshots
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

A transaction that fails verification during read is silently skipped (not applied, no
error). This is correct behavior: concurrent writers may append transactions that
conflict with each other, and during log replay the losing transaction simply has no
effect.

## Commit Protocol

Two configurable thresholds control when a commit takes the append path versus the
CAS (compaction) path:

| Property | Default | Purpose |
|----------|---------|---------|
| `fileio.catalog.max.append.count` | 10000 | Hard limit on log records. Must CAS when exceeded. Matches provider limits (S3 Express ~10k, Azure AppendBlob 50k). **Setting to 0 forces CAS on every commit**, for providers without conditional append (S3 standard, GCS). |
| `fileio.catalog.max.append.size`  | 16 MB  | Soft performance target for total file size. Larger catalogs take longer to read. |

`ProtoCatalogFile` tracks `appendCount` (the number of transaction records in the log
portion), computed during `readInternal`.

```
function COMMIT(fileIO, original, mutations):
    txn      = buildTransaction(original, mutations)
    txnBytes = encodeTransaction(txn)
    current  = original.location

    # Case 0: catalog does not exist yet
    if not current.exists():
        return tryCAS(current, txnBytes, fileIO).orElseThrow(CommitFailed)

    for attempt in 0..9:
        # Must CAS if already sealed, or append count at/beyond the hard limit.
        # (maxAppendCount=0 makes this always true, forcing CAS on every commit.)
        mustCAS = original.sealed
            or original.appendCount >= maxAppendCount

        if mustCAS:
            unseal(txnBytes)
            result = tryCAS(current, txnBytes, fileIO)
            if result.present:
                return validateCommit(result, txn)
            # CAS failed: must re-read to rebuild checkpoint
            current = fileIO.newInputFile(current.location)
            original = READ(fileIO, current)

        else:
            if original.appendCount + 1 >= maxAppendCount
               or current.length + len(txnBytes) > maxAppendSize:
                seal(txnBytes)
            result = tryAppend(current, txnBytes, fileIO)
            if result.present:
                return validateCommit(result, txn)

            # Append failed (offset mismatch from concurrent writer).
            # The transaction bytes are identical and idempotent (UUID-
            # deduplicated), so retry at the new offset without re-reading.
            oldLength = current.length
            current = fileIO.newInputFile(current.location)
            if current.length < oldLength:
                # File shrank (compacted). Must re-read to check sealed/count.
                unseal(txnBytes)
                original = READ(fileIO, current)
            # else: file grew (concurrent append). Retry same bytes.

    throw CommitFailed("exceeded retry limit")
```

### CAS-only Mode

Setting `fileio.catalog.max.append.count=0` makes every commit go through `tryCAS`.
This is required for storage providers that support conditional full-object
replacement but not conditional append (S3 standard via if-match ETag, GCS via
generation number). The commit loop never enters the append branch because
`appendCount >= 0` is always true.

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

Compaction is triggered when either the append count or the append size threshold
would be exceeded:

1. A writer detects that committing this transaction would push the log past
   `maxAppendCount` records or `maxAppendSize` bytes. It **seals** the transaction
   (flipping the `sealed` bit in the encoded bytes) so the next writer knows to compact.
2. Next writer reads the file, sees `sealed = true` (or `appendCount >= maxAppendCount`),
   and enters the CAS branch.
3. The CAS builds a fresh checkpoint from the fully-replayed state and writes
   `[header][checkpoint][new-txn]`, replacing the entire file.
4. After CAS, the file has a single record in the log portion (the committing
   transaction), and `appendCount = 1`.

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

The catalog state is held in `ProtoCatalogFile` (immutable snapshot):

| Field                | Type                             | Description                          |
|----------------------|----------------------------------|--------------------------------------|
| `uuid`               | `UUID`                           | Catalog identity (UUIDv7)            |
| `nextNamespaceId`    | `int`                            | Next namespace ID to allocate        |
| `nextTableId`        | `int`                            | Next table ID to allocate            |
| `sealed`             | `boolean`                        | Whether a sealed txn was encountered |
| `appendCount`        | `int`                            | Transaction records in the log portion (for compaction threshold) |
| `namespaceById`      | `Map<Integer, NsEntry>`          | Namespace ID -> entry (parentId, name, version)   |
| `namespaceLookup`    | `Map<Namespace, Integer>`        | Namespace path -> ID (derived from namespaceById) |
| `nsProperties`       | `Map<Integer, Map<Str, Str>>`    | Namespace ID -> properties           |
| `tableById`          | `Map<Integer, TblEntry>`         | Table ID -> entry (nsId, name, version, metadataLocation) |
| `tableLookup`        | `Map<TableIdentifier, Integer>`  | Table name -> ID (derived from tableById) |
| `tblInlineMetadata`  | `Map<Integer, byte[]>`           | Table ID -> TableMetadata JSON bytes (inline tables only) |
| `tblManifestPrefix`  | `Map<Integer, String>`           | Table ID -> manifest list path prefix (inline tables only) |
| `committedTxns`      | `Set<UUID>`                      | Applied transaction IDs              |

**Entry classes:**

- `NsEntry(int parentId, String name, int version)` -- namespace metadata. The
  `namespaceLookup` map is rebuilt from `namespaceById` by walking parent chains.
- `TblEntry(int namespaceId, String name, int version, String metadataLocation)` --
  table metadata. For inline tables, `metadataLocation` is null; the actual metadata
  is in `tblInlineMetadata`.

A table ID appears in either `tableById` with a non-null `metadataLocation` (pointer
mode) or in `tblInlineMetadata` (inline mode), never both. Inline tables appear in
`tableById` with `metadataLocation = null` for unified lookup via `tableLookup`.

**Location behavior:** `CatalogFile.location(table)` returns the `metadataLocation`
string from `TblEntry` -- this is null for inline tables. Callers use
`isInlineTable(table)` and `inlineMetadata(table)` to access inline metadata.

## Action Verification Rules

Each action's `verify()` checks version-based preconditions:

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
the current transaction size and a ~4 MiB budget (`APPEND_LIMIT = 4 MiB - 4096`,
leaving a safety margin for framing overhead).

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

All properties are set in the catalog properties map passed to `FileIOCatalog.initialize()`.

| Property | Default | Description |
|----------|---------|-------------|
| `fileio.catalog.inline` | `false` | Enable inline table metadata. When true, `doCommit()` uses delta/full/pointer mode selection instead of writing external metadata files. |
| `fileio.catalog.max.append.count` | `10000` | Hard limit on log records before CAS compaction (0 = CAS-only). |
| `fileio.catalog.max.append.size` | `16777216` (16 MB) | Soft file-size target before CAS compaction. |

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

- **Hand-rolled protobuf codec** -- `ProtoCodec` (catalog messages) and
  `InlineDeltaCodec` (delta messages) both produce standard protobuf wire
  format without using `protoc`-generated classes. The output is
  wire-compatible with `catalog.proto` and decodable by any protobuf tool.
- **Inline metadata** -- full integration with `FileIOCatalog`, using delta mode
  for data commits and falling back to full/pointer for oversized transactions.
- **Test coverage** -- 128 unit tests passing (`mvn test`). Cloud provider
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
| `src/main/java/.../CatalogFormat.java` | Format strategy interface (`empty`, `read`, `from`) |
| `src/main/java/.../CatalogFile.java` | Abstract base with inline-aware `Mut` API |
| `src/test/java/.../TestProtoCatalogFormat.java` | Codec and format tests |
| `src/test/java/.../TestProtoActions.java` | Systematic action tests (positive/negative/randomized/inline) |
| `src/test/java/.../TestProtoCommitKnobs.java` | max_appends / max_size commit-knob tests |
| `src/test/java/.../TestInlineDelta.java` | Delta encode/decode/apply, `computeDelta`, `selectMode` tests |

---

## Design Notes

### Hand-Rolled Wire Format

`ProtoCodec` and `InlineDeltaCodec` implement the protobuf wire format manually
rather than using generated classes. This choice lets the codebase validate
correctness against `catalog.proto` before committing to generated-code
dependencies; the wire format is still standard protobuf (field tags, varints,
length-delimited messages), so any protobuf tool can decode the output given
the `.proto` file.

The codec is split across two classes:
- `ProtoCodec` handles the catalog-level messages: `Checkpoint`, `Transaction`,
  `Action` oneof, and all catalog/namespace/table actions.
- `InlineDeltaCodec` handles the inline table delta messages: `TableMetadataDelta`,
  all 11 `TableMetadataUpdate` types, and `CompactSummary`.

### Schema Evolution

Protobuf's forward and backward compatibility applies: new field numbers can
be added without breaking old readers, and removed fields leave their numbers
reserved. Unknown fields are silently skipped during decode (see `ProtoCodec.skipField`).

### Seal/Unseal

The `sealed` flag is a `bool` field (field number 2) in the `Transaction` message.
`ProtoCodec.sealTransaction()` and `unsealTransaction()` locate the field tag in
the serialized bytes and toggle the varint value in place. To make this possible,
`encodeTransaction()` always writes the sealed field (even when false), overriding
proto3's default-value suppression -- otherwise in-place mutation would need to
insert bytes into the middle of the encoded message.
