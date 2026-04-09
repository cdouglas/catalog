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
  repeated Table                  tables                  = 11;
  repeated NamespaceProperty      namespace_properties    = 12;
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
```

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
| `tableLocations`   | `Map<Integer, String>`          | Table ID -> metadata file location   |
| `committedTxns`    | `Set<UUID>`                     | Applied transaction IDs              |

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

## Current Status

- **Implemented and tested** (`TestProtoCatalogFormat`, 684 lines).
- **Not yet wired into FileIOCatalog** -- the format selection logic in `FileIOCatalog`
  currently supports `"append"` (LogCatalogFormat) and `"cas"` (CASCatalogFormat) but
  does not yet offer a `"proto"` option.
- **Hand-rolled codec** -- `ProtoCodec` implements protobuf wire format manually. This
  validates correctness against the `.proto` schema before committing to generated classes
  as a dependency.

## Key Files

| File | Purpose |
|------|---------|
| `src/main/proto/catalog.proto` | Protobuf schema definition |
| `src/main/java/.../ProtoCatalogFormat.java` | Format implementation (read, commit, CAS/append) |
| `src/main/java/.../ProtoCatalogFile.java` | Immutable catalog snapshot + builder |
| `src/main/java/.../ProtoCodec.java` | Protobuf wire encoding/decoding, action types |
| `src/test/java/.../TestProtoCatalogFormat.java` | Test suite |

---

## Comparison with LogCatalogFormat (LCF)

The two formats share the same high-level architecture (checkpoint + append log, CAS/append
commit strategies, 16 MB compaction threshold, version-based optimistic concurrency). The
differences are in encoding, file structure, and extensibility.

### Encoding

| Aspect | LCF | PB |
|--------|-----|-----|
| **Record format** | Custom opcode-based (`DataOutputStream`) | Protobuf wire format (field tags + varints) |
| **Opcodes** | 1-byte prefix per record (0x00--0x09) | Protobuf `oneof` field numbers (1--8) in `Action` |
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
| **Codec** | Inline in `LogCatalogFormat` (~1500 lines) | Separated into `ProtoCodec` (~1176 lines) |
| **Generated code** | None | None yet (hand-rolled codec validates against `.proto`) |

### What Is Identical

- Commit protocol (CAS + append, 10-attempt retry)
- Compaction threshold (16 MB)
- Late-binding with negative virtual IDs
- Version-based optimistic concurrency (same verification rules)
- Action semantics (same 8 action types)
- State model (same fields in snapshot)
- Namespace hierarchy (parent pointers, root = ID 0)
- Idempotency via committed-transaction-ID tracking
