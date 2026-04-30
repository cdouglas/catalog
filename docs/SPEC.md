# ProtoCatalogFormat Specification

This document specifies the protobuf-based catalog format implemented by
`ProtoCatalogFormat`, the only catalog format used by `FileIOCatalog`. It uses
a checkpoint + append-log architecture encoded as protobuf wire format.

For inline-table extensions, see:
- [SPEC_TM.md](SPEC_TM.md) — inline table metadata (field 13 of `Checkpoint`,
  `CreateTableInline` / `UpdateTableInline` actions, `TableMetadataDelta`).
- [SPEC_ML.md](SPEC_ML.md) — inline manifest lists (fields 7–8 of
  `InlineTable`, `AddManifestDelta` / `RemoveManifestDelta` updates).

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

The header is 8 bytes fixed. Everything after the header is variable-length,
using varint length prefixes for each record. There is no separate
committed-transaction-ID block; the checkpoint message carries that set
internally.

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
  repeated Namespace          namespaces                = 10;
  repeated Table              tables                    = 11;  // pointer-mode tables
  repeated NamespaceProperty  namespace_properties      = 12;
  repeated InlineTable        inline_tables             = 13;  // inline-mode tables
  repeated bytes              committed_transaction_ids = 20;  // 16-byte UUIDs
}
```

A table ID appears in either `tables` (pointer mode) or `inline_tables`
(inline mode), never both.

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
  bytes          transaction_id = 1;    // UUIDv7, 16 bytes
  bool           sealed         = 2;    // signals compaction needed
  repeated Action actions       = 3;
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
```

Inline action messages (`CreateTableInline`, `UpdateTableInline`) and the
delta hierarchy under `TableMetadataDelta` live in [SPEC_TM.md](SPEC_TM.md);
manifest-list delta updates live in [SPEC_ML.md](SPEC_ML.md).

## Wire Format Details

The codec uses standard protobuf encoding:

- **Field tag** = `(field_number << 3) | wire_type`
- **Wire type 0** (varint): `int32`, `bool`
- **Wire type 2** (length-delimited): `string`, `bytes`, nested messages
- **Wire type 1/5** (fixed64/fixed32): used for snapshot IDs

Varints use the standard protobuf unsigned encoding (7 bits per byte, MSB
continuation). Signed integers use protobuf's default zigzag-less encoding,
so negative values (used for late-binding sentinels) occupy 10 bytes on the
wire.

Default-valued fields (zero for ints, false for bools, empty for
strings/bytes) are omitted per proto3 conventions, with one exception: the
`sealed` flag on `Transaction` is always written so it can be toggled
in-place (see Seal / Unseal).

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
        if txn.sealed:
            state.sealed = true
            break                            # stop after sealed txn

    return freeze(state)
```

A transaction that fails verification during read is silently skipped (not
applied, no error). This is correct behaviour: concurrent writers may append
transactions that conflict with each other, and during log replay the losing
transaction has no effect.

## Commit Protocol

Two thresholds control whether a commit takes the append path or the CAS
(compaction) path:

| Property                          | Default     | Purpose |
|-----------------------------------|-------------|---------|
| `fileio.catalog.max.append.count` | 10000       | Hard limit on log records. Must CAS when exceeded. Matches provider limits (S3 Express ~10k, Azure AppendBlob 50k). **Setting to 0 forces CAS on every commit**, for providers without conditional append (S3 standard, GCS). |
| `fileio.catalog.max.append.size`  | 16777216    | Soft performance target for total file size. Larger catalogs take longer to read. |

`ProtoCatalogFile` tracks `appendCount` (the number of transaction records in
the log portion), computed during `readInternal`.

```
function COMMIT(fileIO, original, mutations):
    txn      = buildTransaction(original, mutations)
    txnBytes = encodeTransaction(txn)
    current  = original.location

    # Case 0: catalog does not exist yet
    if not current.exists():
        return tryCAS(current, txnBytes, fileIO).orElseThrow(CommitFailed)

    for attempt in 0..9:
        # Must CAS if already sealed, or append count at/beyond the hard
        # limit. (maxAppendCount=0 forces CAS on every commit.)
        mustCAS = original.sealed
            or original.appendCount >= maxAppendCount

        if mustCAS:
            unseal(txnBytes)
            result = tryCAS(current, txnBytes, fileIO)
            if result.present:
                return validateCommit(result, txn)
            current  = fileIO.newInputFile(current.location)
            original = READ(fileIO, current)

        else:
            if original.appendCount + 1 >= maxAppendCount
               or current.length + len(txnBytes) > maxAppendSize:
                seal(txnBytes)
            result = tryAppend(current, txnBytes, fileIO)
            if result.present:
                return validateCommit(result, txn)

            # Append failed (offset mismatch from concurrent writer). The
            # transaction bytes are identical and idempotent (UUID-deduped),
            # so retry at the new offset without re-reading.
            oldLength = current.length
            current   = fileIO.newInputFile(current.location)
            if current.length < oldLength:
                # File shrank (compacted). Must re-read to check sealed/count.
                unseal(txnBytes)
                original = READ(fileIO, current)
            # else: file grew (concurrent append). Retry same bytes.

    throw CommitFailed("exceeded retry limit")
```

### CAS-only Mode

Setting `fileio.catalog.max.append.count=0` makes every commit go through
`tryCAS`. This is required for storage providers that support conditional
full-object replacement but not conditional append (S3 standard via if-match
ETag, GCS via generation number). The commit loop never enters the append
branch because `appendCount >= 0` is always true.

### tryCAS

Builds a full file (header + checkpoint + transaction) and writes it
atomically, conditioned on the ETag / generation number of the existing file.

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

`buildFullFile` creates:
`[MAGIC][VERSION][varint(chkLen)][checkpoint][varint(txnLen)][txn]`. The
checkpoint is encoded from the merged state (original + pending mutations),
so the resulting file is fully compacted.

### tryAppend

Appends only the transaction record (`[varint(txnLen)][txn]`) to the end of
the file.

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

The `sealed` flag is a `bool` field (field 2) in `Transaction`.
`ProtoCodec.sealTransaction()` and `unsealTransaction()` locate the field
tag in the serialized bytes and toggle the varint value in place. To make
this possible, `encodeTransaction()` always writes the sealed field (even
when false), overriding proto3's default-value suppression — otherwise
in-place mutation would need to insert bytes into the middle of the encoded
message.

## Compaction

Compaction is triggered when either the append count or the append size
threshold would be exceeded:

1. A writer detects that committing this transaction would push the log past
   `maxAppendCount` records or `maxAppendSize` bytes. It **seals** the
   transaction (flipping the `sealed` bit in the encoded bytes) so the next
   writer knows to compact.
2. Next writer reads the file, sees `sealed = true` (or `appendCount >=
   maxAppendCount`), and enters the CAS branch.
3. The CAS builds a fresh checkpoint from the fully-replayed state and
   writes `[header][checkpoint][new-txn]`, replacing the entire file.
4. After CAS, the file has a single record in the log portion (the
   committing transaction), and `appendCount = 1`.

The committed-transaction-ID set is carried forward in the checkpoint for
idempotency.

## Late-Binding

When a transaction creates a namespace or table and references it within the
same transaction, the entity ID is not yet allocated. The format uses
negative integers as virtual IDs:

- `ProtoIdManager` maintains allocation counters (`nextNsid`, `nextTblid`)
  and a remap table (`virtualId -> realId`).
- During `buildActions()`, new namespaces are sorted by depth (parents
  before children) and assigned IDs from `idManager.allocateNsid()`.
- Version fields are set to `-1` when the referenced entity was created in
  the same txn.
- On replay, `resolve(id)` maps negative IDs to their allocated counterparts.

## State Model

The catalog state is held in `ProtoCatalogFile` (immutable snapshot):

| Field                | Type                             | Description                          |
|----------------------|----------------------------------|--------------------------------------|
| `uuid`               | `UUID`                           | Catalog identity (UUIDv7)            |
| `nextNamespaceId`    | `int`                            | Next namespace ID to allocate        |
| `nextTableId`        | `int`                            | Next table ID to allocate            |
| `sealed`             | `boolean`                        | Whether a sealed txn was encountered |
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
