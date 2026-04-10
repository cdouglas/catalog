# Inline Table Metadata Intention Records

This document specifies how catalog intention records (transaction actions) carry
table metadata changes inline, eliminating the separate metadata.json write. It
covers both the LCF and PB catalog formats.

## Motivation

Today, every table update is a two-write operation: write a full `v<N>.metadata.json`
to storage, then atomically commit an `UPDATE_TABLE` action pointing to that file.
As analyzed in [INLINE_TBL_UPDATE.md](./INLINE_TBL_UPDATE.md), a typical data commit
adds ~700 bytes of new information (one snapshot, updated scalars), but rewrites
the full metadata file every time. After 10 commits, 92% of every write is redundant.

By inlining table metadata changes into the catalog intention record, we:

- Reduce to a single atomic write per table update
- Eliminate orphaned metadata files on commit failure
- Reduce per-commit write amplification by 1--3 orders of magnitude
- Enable the catalog to serve table metadata directly (no external read)

The catalog absorbs the first level of Iceberg table state. Manifest lists, manifest
files, and data files remain external.

---

## Design Overview

### Delivery Modes

Each table update action carries its payload in one of three **delivery modes**:

| Mode        | Payload                         | When used                                   |
|-------------|---------------------------------|---------------------------------------------|
| **Delta**   | Structured metadata changes     | Normal data commits and DDL operations      |
| **Full**    | Complete `TableMetadata` (JSON) | Table creation; re-inlining from pointer    |
| **Pointer** | Metadata file location (string) | Fallback when inline exceeds size limits    |

A single transaction may contain actions in different modes for different tables.

### The Catalog as Metadata Store

For **inline** tables (delta or full mode), the catalog holds the authoritative table
metadata. The checkpoint serializes the full reconstructed `TableMetadata` for each
inline table, and transaction-log deltas are replayed on top.

For **pointer** tables, behavior is unchanged: the catalog stores a metadata file
location string.

Tables may transition between modes:

- **Inline -> Pointer**: when an update would push the transaction beyond the
  append size limit (see [Size Budget](#size-budget-and-mode-selection))
- **Pointer -> Inline**: on compaction, the compactor reads external metadata and
  inlines it; or on the next update, the writer inlines the new metadata directly

### Metadata-Log and Snapshot-Log

For inline tables, no separate metadata.json files are written, so `metadata-log`
entries are not generated. The catalog's own transaction history serves as the
metadata audit trail.

`snapshot-log` entries are derived during `TableMetadata` reconstruction: each
`AddSnapshot` delta produces a `{timestamp-ms, snapshot-id}` entry.

When a `TableMetadata` is materialized for API consumers (e.g., REST catalog),
the snapshot-log is populated from the inline history and the metadata-log is
left empty (or contains only entries from before the table was inlined).

### Checkpoint Dictionary

The checkpoint defines shared state that intention records reference to avoid
repeating common values. This serves as a **dictionary** -- the checkpoint
establishes vocabulary and base values, and deltas encode only what differs.

Three layers of dictionary apply:

**1. Per-table dictionary** (stored in each inline table's checkpoint entry):

| Entry                    | Stored?  | Derivation / purpose                     | Savings per AddSnapshot |
|--------------------------|----------|------------------------------------------|-------------------------|
| `manifest_list_prefix`   | explicit | Shared path prefix for manifest lists    | ~70--80 bytes           |
| `current_snapshot_id`    | implicit | Eliminates `parent-snapshot-id`          | 8 bytes                 |
| `last_sequence_number`   | implicit | Eliminates `sequence-number` (always +1) | 8 bytes                 |
| `last_updated_ms`        | implicit | Enables delta-encoding of `timestamp-ms` | 4--5 bytes              |
| `current_schema_id`      | implicit | Eliminates `schema-id` when unchanged    | ~4 bytes                |
| `next_row_id`            | implicit | Eliminates `first-row-id` (v3)           | 8 bytes                 |

The "implicit" entries are values already present in the checkpoint's table
metadata; no additional storage is needed. After each `AddSnapshot` is applied
during log replay, these base values are updated, so the *next* delta in the log
is decoded relative to the state after the previous delta -- exactly like a
forward-differenced chain.

The one **explicit** entry is `manifest_list_prefix`. The manifest-list path for
every snapshot of a table follows the pattern
`<table-location>/metadata/snap-<snapshotId>-<attempt>-<uuid>.avro`.
The prefix up to and including `snap-` is identical for all snapshots. The
checkpoint stores this prefix per-table, and each `AddSnapshot` carries only
the variable suffix (typically ~50 bytes instead of ~120 bytes).

**2. Compact snapshot summary** (schema-level dictionary):

The snapshot summary `map<string, string>` carries standard keys (`operation`,
`added-data-files`, `added-records`, ...) as full strings with string-encoded
integer values. A **CompactSummary** type replaces this with:

- An enum for `operation` (1 byte vs 6--9 byte string)
- Typed integer fields for standard counters (varint vs string digits)
- An overflow map for non-standard summary entries

Field numbers in the schema serve as an implicit dictionary: they replace ~120
bytes of repeated key strings. Integer types replace string-encoded numbers,
giving an additional ~50--80 bytes savings per snapshot.

**3. Catalog-wide string dictionary** (optional, stored in checkpoint):

An indexed table of strings shared across all inline tables:

| Index range | Contents                           |
|-------------|------------------------------------|
| 0--63       | Reserved for future standard use   |
| 64+         | Catalog-defined strings            |

The primary use case is a shared warehouse or bucket prefix that per-table
`manifest_list_prefix` values can reference, reducing checkpoint size when
many tables share a common storage root. This dictionary is resolved at
checkpoint load time -- intention records do not reference it directly.

If no custom entries are needed, the dictionary is omitted.

#### Combined Savings

| Dictionary layer                | Savings per AddSnapshot | Mechanism                 |
|---------------------------------|-------------------------|---------------------------|
| Manifest-list prefix            | ~70--80 bytes           | Per-table string prefix   |
| Implicit parent-snapshot-id     | ~8 bytes                | Derived from state        |
| Implicit sequence-number        | ~8 bytes                | Derived (always +1)       |
| Implicit first-row-id (v3)      | ~8 bytes                | Derived from state        |
| Delta-encoded timestamp         | ~4--5 bytes             | Signed delta vs absolute  |
| Schema-id (omit if unchanged)   | ~4 bytes                | Flag bit                  |
| CompactSummary keys             | ~120 bytes              | Field numbers ≡ keys      |
| CompactSummary value encoding   | ~50--80 bytes           | Varint vs string digits   |
| **Total**                       | **~270--375 bytes**     |                           |

This brings an `AddSnapshot` delta from ~500--600 bytes (JSON) or ~230--335
bytes (naive protobuf) down to **~65--95 bytes** (PB with dictionary) or
**~100--140 bytes** (LCF with dictionary), matching the Option D compressed
estimate from [INLINE_TBL_UPDATE.md](./INLINE_TBL_UPDATE.md).

---

## Table Metadata Delta

A delta is an ordered sequence of **metadata updates**, each describing one logical
change to the table metadata. These correspond to Iceberg's `MetadataUpdate` types.

### AddSnapshot

The overwhelmingly common case (~99% of table updates). Uses the per-table
dictionary to eliminate redundant fields (see [Checkpoint Dictionary](#checkpoint-dictionary)).

**Fields carried in the delta:**

| Field                   | Type           | Description                                            |
|-------------------------|----------------|--------------------------------------------------------|
| `snapshot-id`           | long           | Unique snapshot identifier (random, 8 bytes verbatim)  |
| `manifest-list-suffix`  | string         | Suffix appended to per-table `manifest_list_prefix`    |
| `summary`               | CompactSummary | Operation type and statistics (see below)              |
| `timestamp-delta-ms`    | long (signed)  | Delta from state's `last-updated-ms`                   |
| `schema-id`             | int            | Only encoded if different from state's `current-schema-id`; omitted otherwise |
| `added-rows`            | long           | V3+: rows added (0 if not v3)                          |

**Fields derived from catalog state (not transmitted):**

| Field               | Derivation                                          |
|---------------------|-----------------------------------------------------|
| `sequence-number`   | `state.lastSequenceNumber + 1`                      |
| `parent-snapshot-id`| `state.currentSnapshotId`                           |
| `timestamp-ms`      | `state.lastUpdatedMs + timestamp-delta-ms`          |
| `first-row-id`      | `state.nextRowId` (v3+)                             |
| `manifest-list`     | `table.manifestListPrefix + manifest-list-suffix`   |

**Implied scalar updates on apply:**
- `last-sequence-number` = derived `sequence-number`
- `last-updated-ms` = derived `timestamp-ms`
- `current-snapshot-id` = `snapshot-id`
- `next-row-id` += `added-rows` (v3+; validated: derived `first-row-id >= state.nextRowId`)

A data commit also implies a `SetSnapshotRef` for the `main` branch, which is
recorded as a separate update in the delta (see below).

### CompactSummary

Replaces the `map<string, string>` snapshot summary with typed fields. Field
numbers serve as an implicit key dictionary; integer types replace string-encoded
values.

| Field                    | Type  | Map key equivalent            |
|--------------------------|-------|-------------------------------|
| `operation`              | enum  | `"operation"`                 |
| `added-data-files`       | long  | `"added-data-files"`          |
| `added-records`          | long  | `"added-records"`             |
| `added-files-size`       | long  | `"added-files-size"`          |
| `total-data-files`       | long  | `"total-data-files"`          |
| `total-records`          | long  | `"total-records"`             |
| `total-files-size`       | long  | `"total-files-size"`          |
| `added-delete-files`     | long  | `"added-delete-files"`        |
| `total-delete-files`     | long  | `"total-delete-files"`        |
| `total-equality-deletes` | long  | `"total-equality-deletes"`    |
| `total-position-deletes` | long  | `"total-position-deletes"`    |
| `extra`                  | map   | Non-standard entries (rare)   |

**Operation enum values:** `APPEND` = 0, `OVERWRITE` = 1, `DELETE` = 2, `REPLACE` = 3.

Fields with value 0 are omitted (proto3 default semantics / zero-suppression).
The `extra` map captures non-standard summary entries that engines may add.

### SetSnapshotRef

| Field                    | Type    | Description                              |
|--------------------------|---------|------------------------------------------|
| `ref-name`               | string  | Reference name (e.g., `"main"`)          |
| `snapshot-id`            | long    | Target snapshot ID                       |
| `type`                   | string  | `"branch"` or `"tag"`                    |
| `min-snapshots-to-keep`  | int?    | Retention policy (0 = unset)             |
| `max-snapshot-age-ms`    | long?   | Retention policy (0 = unset)             |
| `max-ref-age-ms`         | long?   | Retention policy (0 = unset)             |

### RemoveSnapshots

| Field          | Type     | Description                       |
|----------------|----------|-----------------------------------|
| `snapshot-ids` | long[]   | Snapshot IDs to remove            |

Removes entries from `snapshots[]` and any corresponding `snapshot-log` entries.
Updates `current-snapshot-id` if the removed snapshot was current.

### AddSchema

| Field            | Type   | Description                              |
|------------------|--------|------------------------------------------|
| `schema-id`      | int    | ID for the new schema                    |
| `last-column-id` | int    | Highest field ID assigned in this schema |
| `schema`         | bytes  | Full schema (serialized via `SchemaParser.toJson`) |

**Implied:** `last-column-id` updated to `max(current, new)`.

### SetCurrentSchema

| Field       | Type | Description             |
|-------------|------|-------------------------|
| `schema-id` | int  | The active schema's ID |

### AddPartitionSpec

| Field               | Type   | Description                            |
|---------------------|--------|----------------------------------------|
| `spec-id`           | int    | ID for the new partition spec          |
| `last-partition-id` | int    | Highest partition field ID assigned    |
| `spec`              | bytes  | Full spec (`PartitionSpecParser.toJson`) |

### SetDefaultPartitionSpec

| Field     | Type | Description                    |
|-----------|------|--------------------------------|
| `spec-id` | int  | The active partition spec's ID |

### AddSortOrder

| Field      | Type  | Description                     |
|------------|-------|---------------------------------|
| `order-id` | int   | ID for the new sort order       |
| `order`    | bytes | Full order (`SortOrderParser.toJson`) |

### SetDefaultSortOrder

| Field      | Type | Description                   |
|------------|------|-------------------------------|
| `order-id` | int  | The active sort order's ID    |

### SetProperties

| Field      | Type            | Description                       |
|------------|-----------------|-----------------------------------|
| `updates`  | map<str, str>   | Properties to set (upsert)        |
| `removals` | string[]        | Property keys to remove           |

### SetTableLocation

| Field      | Type   | Description            |
|------------|--------|------------------------|
| `location` | string | New table root path    |

---

### Typical Data Commit Delta

A normal append or overwrite produces exactly two updates:

1. **AddSnapshot** -- compact snapshot delta using the per-table dictionary
2. **SetSnapshotRef** -- update `main` branch to the new snapshot ID

| Encoding           | AddSnapshot | SetSnapshotRef | Total        |
|--------------------|-------------|----------------|--------------|
| PB (with dict)     | ~65--95 B   | ~15--20 B      | ~80--115 B   |
| LCF (with dict)    | ~100--140 B | ~25--30 B      | ~125--170 B  |
| PB (without dict)  | ~230--335 B | ~20--30 B      | ~250--365 B  |
| LCF (without dict) | ~300--450 B | ~30--40 B      | ~330--490 B  |

The dictionary encoding reduces per-commit size by ~3x (PB) to ~2.5x (LCF),
bringing the total close to the ~100-byte Option D estimate from
[INLINE_TBL_UPDATE.md](./INLINE_TBL_UPDATE.md).

Compare to the current approach: rewriting the full metadata.json at 2--700 KB.

---

## LCF Encoding

### New Opcodes

```
Opcode  Name                      Payload
------  ----                      -------
0x0A    CREATE_TABLE_INLINE       name:utf  tblId:i32  tblVer:i32  nsid:i32  nsVer:i32
                                  metadataLen:i32  metadata:byte[metadataLen]
0x0B    UPDATE_TABLE_INLINE       tblId:i32  tblVer:i32  mode:u8
                                  (mode-dependent payload, see below)
```

Existing opcodes 0x01 (`CREATE_TABLE`) and 0x02 (`UPDATE_TABLE`) remain unchanged
for pointer-mode tables.

### CREATE_TABLE_INLINE (0x0A)

Creates a table with its full initial `TableMetadata` inlined. The metadata is the
JSON output of `TableMetadataParser.toJson()`, encoded as a length-prefixed byte
array.

```
Offset  Size   Field
------  ----   -----
  0       1    opcode           0x0A
  1     var    name             utf (table name)
var+1     4    tblId            i32 (-1 for late-bind)
var+5     4    tblVer           i32 (-1 for late-bind)
var+9     4    nsid             i32 (namespace ID, or negative for late-bind)
var+13    4    nsVer            i32 (namespace version, -1 for late-bind)
var+17    4    metadataLen      i32 (byte length of JSON)
var+21  var    metadata         byte[metadataLen] (TableMetadata JSON, UTF-8)
```

### UPDATE_TABLE_INLINE (0x0B)

Updates an inline table's metadata. Three sub-formats selected by a mode byte.

```
Offset  Size   Field
------  ----   -----
  0       1    opcode           0x0B
  1       4    tblId            i32
  5       4    tblVer           i32 (must match current version)
  9       1    mode             u8  (0x00 = delta, 0x01 = full, 0x02 = pointer)
 10     var    payload          (mode-dependent)
```

**Mode 0x00 -- Delta:**

```
 10       4    nUpdates         i32 (number of metadata updates)
 14     var    updates[]        sequence of sub-records:
                                  subOpcode: u8
                                  ... (type-specific, see below)
```

**Mode 0x01 -- Full:**

```
 10       4    metadataLen      i32
 14     var    metadata         byte[metadataLen] (TableMetadata JSON, UTF-8)
```

**Mode 0x02 -- Pointer:**

```
 10     var    location         utf (metadata file location)
```

### Delta Sub-opcodes

Type conventions follow the LCF base format: `i32` (4-byte big-endian signed int),
`i64` (8-byte big-endian signed long), `utf` (Java modified UTF-8 with 2-byte length
prefix), `u8` (unsigned byte).

```
Sub     Name                    Payload
---     ----                    -------
0x01    ADD_SNAPSHOT             snapId:i64
                                 manifestListSuffix:utf
                                 timestampDeltaMs:i64   (signed; state.lastUpdatedMs + delta)
                                 flags:u8               (bit 0: schema changed,
                                                         bit 1: has v3 row lineage)
                                 [schemaId:i32]         (only if flags bit 0 set)
                                 [addedRows:i64]        (only if flags bit 1 set)
                                 operation:u8           (enum: 0=append, 1=overwrite,
                                                         2=delete, 3=replace)
                                 addedDataFiles:i64
                                 addedRecords:i64
                                 addedFilesSize:i64
                                 totalDataFiles:i64
                                 totalRecords:i64
                                 totalFilesSize:i64
                                 addedDeleteFiles:i64
                                 totalDeleteFiles:i64
                                 totalEqualityDeletes:i64
                                 totalPositionDeletes:i64
                                 nExtra:i32             (non-standard summary entries)
                                 (key:utf val:utf)...
                                 -------
                                 Derived on read:
                                   sequenceNumber    = state.lastSequenceNumber + 1
                                   parentSnapshotId  = state.currentSnapshotId
                                   firstRowId        = state.nextRowId (v3)
                                   manifestList      = table.manifestListPrefix + suffix
                                   timestampMs       = state.lastUpdatedMs + timestampDeltaMs
                                   schemaId          = flags.bit0 ? schemaId : state.currentSchemaId

0x02    SET_SNAPSHOT_REF         name:utf  snapId:i64  type:u8 (0=branch, 1=tag)
                                 hasRetention:u8
                                 [minKeep:i32 maxAgeMs:i64 maxRefAgeMs:i64]
                                                                 (if hasRetention=1)
0x03    REMOVE_SNAPSHOTS         nIds:i32  snapId:i64...

0x04    ADD_SCHEMA               schemaId:i32  lastColId:i32
                                 jsonLen:i32  json:byte[jsonLen]

0x05    SET_CURRENT_SCHEMA       schemaId:i32

0x06    ADD_PARTITION_SPEC       specId:i32  lastPartId:i32
                                 jsonLen:i32  json:byte[jsonLen]

0x07    SET_DEFAULT_SPEC         specId:i32

0x08    ADD_SORT_ORDER           orderId:i32
                                 jsonLen:i32  json:byte[jsonLen]

0x09    SET_DEFAULT_SORT_ORDER   orderId:i32

0x0A    SET_PROPERTIES           nSet:i32  (key:utf val:utf)...
                                 nRemove:i32  key:utf...

0x0B    SET_TABLE_LOCATION       location:utf
```

Complex structural objects (schemas, partition specs, sort orders) are serialized as
length-prefixed JSON using Iceberg's existing parsers. These operations are rare
(schema/partition evolution), so the JSON overhead is acceptable.

### Checkpoint Extension: Table Embed Region

The LCF checkpoint header already reserves a **table embed region** between the
checkpoint data and committed transaction IDs (`tblEmbedEnd` at header offset 29,
currently always 0). For inline tables, this region contains:

```
Offset  Size   Field
------  ----   -----
  0       4    nInlineTables         i32 (number of inline table entries)
  4     var    entries[]:
                 tblId:               i32
                 tblVer:              i32
                 nsid:                i32
                 name:                utf
                 manifestListPrefix:  utf   (per-table dictionary: shared manifest path prefix)
                 metadataLen:         i32
                 metadata:            byte[metadataLen]  (TableMetadata JSON, UTF-8)
```

Each table's `manifestListPrefix` is typically `<table-location>/metadata/snap-`.
This prefix is prepended to the `manifestListSuffix` in each `ADD_SNAPSHOT`
sub-record during log replay to reconstruct the full manifest-list path.

`tblEmbedEnd` in the checkpoint header is set to the total byte length of this
region. Pointer-mode tables appear as `CREATE_TABLE` (0x01) records in the
checkpoint data region with a `location` string, as before. Inline tables appear
**only** in the table embed region, not in the checkpoint data.

### Updated File Layout (LCF)

```
+------------------------------------------------------------------+
|                     Catalog File                                 |
+==================================================================+
| Checkpoint Header          (37 bytes, fixed)                     |
+------------------------------------------------------------------+
| Checkpoint Data            (chkLen bytes)                        |
|   CREATE_NAMESPACE records                                       |
|   ADD_NAMESPACE_PROPERTY records                                 |
|   CREATE_TABLE records     (pointer-mode tables only)            |
+------------------------------------------------------------------+
| Table Embed Region         (tblEmbedEnd bytes)                   |
|   nInlineTables: i32                                             |
|   Per-table: tblId, tblVer, nsid, name, manifestListPrefix,     |
|              metadataLen, metadata                                |
+------------------------------------------------------------------+
| Committed Transaction IDs  (committedTxnLen bytes)               |
+------------------------------------------------------------------+
| Transaction Log                                                  |
+------------------------------------------------------------------+
```

### Compatibility

LCF has no unknown-opcode skip mechanism. Readers that do not recognize opcodes
0x0A or 0x0B will fail. Writers must not emit inline opcodes until all readers
are updated. Deployment requires upgrading readers before writers.

---

## PB Encoding

### New Messages

Add to `catalog.proto`:

```protobuf
// ============================================================
// Inline table metadata - checkpoint
// ============================================================

message InlineTable {
  int32  id                    = 1;
  int32  version               = 2;
  int32  namespace_id          = 3;
  string name                  = 4;
  bytes  metadata              = 5;   // TableMetadata as JSON bytes
  string manifest_list_prefix  = 6;   // per-table dictionary: shared manifest path prefix
}

// ============================================================
// Inline table metadata - actions
// ============================================================

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
    TableMetadataDelta delta             = 3;
    bytes              full_metadata     = 4;   // full TableMetadata JSON
    string             metadata_location = 5;   // pointer fallback
  }
}

// ---- Delta structure ----

message TableMetadataDelta {
  repeated TableMetadataUpdate updates = 1;
}

message TableMetadataUpdate {
  oneof update {
    AddSnapshot             add_snapshot              = 1;
    SetSnapshotRef          set_snapshot_ref          = 2;
    RemoveSnapshots         remove_snapshots          = 3;
    AddSchema               add_schema                = 4;
    SetCurrentSchema        set_current_schema        = 5;
    AddPartitionSpec        add_partition_spec         = 6;
    SetDefaultPartitionSpec set_default_partition_spec = 7;
    AddSortOrder            add_sort_order             = 8;
    SetDefaultSortOrder     set_default_sort_order     = 9;
    SetTableProperties      set_properties            = 10;
    SetTableLocation        set_table_location        = 11;
  }
}

// ---- Metadata update messages ----
// AddSnapshot uses the per-table dictionary: fields derivable from
// catalog state are omitted and reconstructed on read.

message AddSnapshot {
  fixed64        snapshot_id            = 1;  // random, 8 bytes verbatim
  string         manifest_list_suffix   = 2;  // appended to per-table prefix
  CompactSummary summary                = 3;  // typed summary (replaces map)
  sint64         timestamp_delta_ms     = 4;  // signed delta from state.lastUpdatedMs
  int32          schema_id              = 5;  // only set if changed from state.currentSchemaId
  int64          added_rows             = 6;  // v3+; 0 if not applicable

  // Derived on read (not transmitted):
  //   sequence_number    = state.lastSequenceNumber + 1
  //   parent_snapshot_id = state.currentSnapshotId
  //   first_row_id       = state.nextRowId  (v3+)
  //   timestamp_ms       = state.lastUpdatedMs + timestamp_delta_ms
  //   manifest_list      = table.manifestListPrefix + manifest_list_suffix
  //   schema_id (if 0)   = state.currentSchemaId
}

enum OperationType {
  APPEND    = 0;
  OVERWRITE = 1;
  DELETE    = 2;
  REPLACE   = 3;
}

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
  map<string,string> extra                  = 15;  // non-standard entries (rare)
}

message SetSnapshotRef {
  string ref_name               = 1;
  int64  snapshot_id            = 2;
  string ref_type               = 3;   // "branch" or "tag"
  int32  min_snapshots_to_keep  = 4;   // 0 = unset
  int64  max_snapshot_age_ms    = 5;   // 0 = unset
  int64  max_ref_age_ms         = 6;   // 0 = unset
}

message RemoveSnapshots {
  repeated int64 snapshot_ids = 1;
}

message AddSchema {
  int32 schema_id      = 1;
  int32 last_column_id = 2;
  bytes schema_json    = 3;            // SchemaParser.toJson()
}

message SetCurrentSchema {
  int32 schema_id = 1;
}

message AddPartitionSpec {
  int32 spec_id           = 1;
  int32 last_partition_id = 2;
  bytes spec_json         = 3;         // PartitionSpecParser.toJson()
}

message SetDefaultPartitionSpec {
  int32 spec_id = 1;
}

message AddSortOrder {
  int32 order_id   = 1;
  bytes order_json = 2;               // SortOrderParser.toJson()
}

message SetDefaultSortOrder {
  int32 order_id = 1;
}

message SetTableProperties {
  map<string,string> updated = 1;     // properties to set (upsert)
  repeated string    removed = 2;     // property keys to remove
}

message SetTableLocation {
  string location = 1;
}
```

### Checkpoint Extension

Add fields 13 and 14 to the existing `Checkpoint` message:

```protobuf
message Checkpoint {
  // ... existing fields (1-3, 10-12, 20) ...
  repeated InlineTable inline_tables  = 13;
  StringDictionary     dictionary     = 14;  // catalog-wide string dictionary (optional)
}

message StringDictionary {
  repeated string entries = 1;  // indexed 0, 1, 2, ...
}
```

Pointer-mode tables remain in `repeated Table tables = 11`. Inline-mode tables
appear in `repeated InlineTable inline_tables = 13`. A table ID must not appear
in both.

The `StringDictionary` is optional. When present, per-table fields like
`manifest_list_prefix` may reference dictionary entries by convention: a prefix
of `\x00` followed by a varint index indicates a dictionary lookup whose value
is prepended to the remainder of the string. If the prefix is absent, the string
is a literal. The primary use case is a shared warehouse root (e.g.,
`gs://bucket/warehouse/`) referenced by many tables' manifest-list prefixes.

### Action Extension

Add fields 9 and 10 to the existing `Action` message:

```protobuf
message Action {
  oneof action {
    // ... existing fields (1-8) ...
    UpdateTableInline  update_table_inline  = 9;
    CreateTableInline  create_table_inline  = 10;
  }
}
```

### Compatibility

Protobuf's forward-compatibility guarantees apply: old readers that do not
recognize fields 9, 10, or 13 will silently skip them. This means old readers
will see inline tables as missing (not in `tables`, not in `inline_tables`).
This is acceptable during a rollout where all readers are upgraded before writers
begin emitting inline actions. Alternatively, a writer could emit both an inline
action (field 9/10) and a pointer action (field 7) for the same table during
a transition period, though this adds complexity.

---

## State Model Changes

The catalog state model gains a per-table inline metadata field.

### Existing Fields (unchanged)

| Field              | Type                            | Description                          |
|--------------------|---------------------------------|--------------------------------------|
| `tblIds`           | `Map<TableIdentifier, Integer>` | Table -> internal ID                 |
| `tblVersion`       | `Map<Integer, Integer>`         | Table ID -> version counter          |

### Modified Fields

| Old Field          | New Field              | Type                                  | Description                        |
|--------------------|------------------------|---------------------------------------|------------------------------------|
| `tblLocations`     | `tblLocations`         | `Map<Integer, String>`                | Table ID -> metadata location      |
|                    |                        |                                       | (**pointer-mode tables only**)     |
| (new)              | `tblInlineMetadata`    | `Map<Integer, TableMetadata>`         | Table ID -> reconstructed metadata |
|                    |                        |                                       | (**inline tables only**)           |
| (new)              | `tblManifestPrefix`    | `Map<Integer, String>`                | Table ID -> manifest-list prefix   |
|                    |                        |                                       | (per-table dictionary entry)       |

A table's ID appears in exactly one of `tblLocations` or `tblInlineMetadata`, never
both. Every inline table has a corresponding `tblManifestPrefix` entry.

### Lookup Behavior

- **Inline table**: return `TableMetadata` from `tblInlineMetadata` directly. No
  external file read required.
- **Pointer table**: read `metadata_location` from `tblLocations`, fetch and parse
  the external metadata.json file.

### Version Semantics

The table version counter increments on every update, regardless of delivery mode.
The version guards catalog-level concurrency (optimistic locking). A delta that
passes version verification applies to the correct base metadata because no concurrent
writer could have modified the base without incrementing the version.

---

## Size Budget and Mode Selection

### Azure 4 MiB Append Limit

Azure ADLS conditional append (`AppendBlock`) has a hard limit of **4 MiB**
(4,194,304 bytes) per append operation. This is the binding constraint for
transaction record size.

GCS compose and S3 complete-multipart-upload have much larger limits (5 GB), so
Azure is the worst case. The format must ensure that any single transaction record
fits within 4 MiB.

### Size Budget per Transaction

```
Azure append limit:                     4,194,304 bytes
Safety margin (framing, varint, etc.):     -4,096 bytes
                                       ──────────────────
Available for transaction payload:      4,190,208 bytes (~4.0 MiB)
```

### Inline Delta Size Estimates

| Scenario                       | Delta (with dict) | Delta (no dict) | Fits in 4 MiB? |
|--------------------------------|--------------------|-----------------|-----------------|
| Single data commit (1 table)   | 80--170 B          | 330--490 B      | Yes             |
| 10-table multi-table txn       | 0.8--1.7 KB        | 3.3--4.9 KB     | Yes             |
| Schema evolution (wide table)  | 10--20 KB          | 10--20 KB       | Yes             |
| Full metadata (100 snapshots)  | ~70 KB             | ~70 KB          | Yes             |
| Full metadata (1000 snapshots) | ~700 KB            | ~700 KB         | Yes             |
| Full metadata (6000+ snapshots)| >4 MB              | >4 MB           | **No -> pointer**|

With the dictionary, a typical single-table data commit fits in **~100--170 bytes**
(PB: ~80--115, LCF: ~125--170). The dictionary has no effect on full-mode or
pointer-mode actions since those carry complete metadata or location strings.

For **delta** mode (the common case), the payload is well under 1 KB per table.

### Mode Selection Algorithm

```
function selectMode(table, delta, txnSizeEstimate):
    deltaSize = estimateWireSize(delta)

    if txnSizeEstimate + deltaSize <= APPEND_LIMIT:
        return DELTA

    # Delta itself too large (extremely rare) -- try full metadata
    fullSize = estimateWireSize(table.metadata.toJson())
    if txnSizeEstimate + fullSize <= APPEND_LIMIT:
        return FULL

    # Full metadata too large -- fall back to external pointer
    writeMetadataFile(table)
    return POINTER
```

where `APPEND_LIMIT` = 4,190,208 bytes (4 MiB minus safety margin).

In practice, the delta path handles >99.9% of updates.

### Compaction Considerations

During compaction (CAS), the checkpoint includes full metadata for all inline tables.
If the total checkpoint size approaches the compaction threshold (16 MB), the
compactor should **evict** the largest inline tables to pointer mode:

1. Compute the checkpoint size including all inline table metadata.
2. While total size exceeds threshold:
   a. Select the inline table with the largest serialized metadata.
   b. Write its metadata to an external `v<N>.metadata.json` file.
   c. Move the table from `inline_tables` to `tables` (pointer mode).
3. The evicted table can be re-inlined on its next update.

This ensures the catalog file stays within manageable size limits even with many
inline tables.

---

## Protocol Changes

### Read Protocol

The read protocol extends to reconstruct inline table metadata. Pseudocode additions
are marked with `# NEW`.

```
function READ(fileIO, location):
    bytes = fileIO.read(location)
    stream = InputStream(bytes)

    # 1. Read checkpoint (format-specific: LCF header or PB magic+checkpoint)
    state = readCheckpoint(stream)

    # 2. Load inline table metadata + dictionary from checkpoint  # NEW
    for entry in state.checkpoint.inlineTables:                  # NEW
        metadata = TableMetadataParser.fromJson(entry.metadata)  # NEW
        state.tblInlineMetadata[entry.tblId] = metadata          # NEW
        state.tblManifestPrefix[entry.tblId] = entry.manifestListPrefix  # NEW

    # 3. Replay transaction log
    while stream.hasRemaining():
        txn = readTransaction(stream)
        if state.committedTxn.contains(txn.id):
            continue
        if txn.verify(state):
            txn.apply(state)               # dispatches to applyAction
            state.committedTxn.add(txn.id)
        if txn.sealed:
            state.sealed = true
            break

    return freeze(state)
```

### Action Application

New action types are applied as follows:

```
function applyCreateTableInline(state, action):
    # Verify: namespace version matches (same as CREATE_TABLE)
    tblId = allocateOrResolve(action.id, state)
    metadata = TableMetadataParser.fromJson(action.metadata)
    state.tblIds[action.tableName] = tblId
    state.tblVersion[tblId] = 1
    state.tblInlineMetadata[tblId] = metadata

function applyUpdateTableInline(state, action):
    assert action.version == state.tblVersion[action.tblId]

    switch action.mode:
        case DELTA:
            base = state.tblInlineMetadata[action.tblId]
            prefix = state.tblManifestPrefix[action.tblId]
            updated = base
            for update in action.delta.updates:
                updated = applyMetadataUpdate(updated, update, prefix)
            state.tblInlineMetadata[action.tblId] = updated

        case FULL:
            metadata = TableMetadataParser.fromJson(action.fullMetadata)
            state.tblInlineMetadata[action.tblId] = metadata

        case POINTER:
            state.tblInlineMetadata.remove(action.tblId)
            state.tblLocations[action.tblId] = action.metadataLocation

    state.tblVersion[action.tblId] += 1
```

### applyMetadataUpdate

```
function applyMetadataUpdate(metadata, update, manifestListPrefix):
    switch update.type:
        case ADD_SNAPSHOT:
            // Reconstruct derived fields from catalog state (the dictionary)
            seqNum    = metadata.lastSequenceNumber + 1
            parentId  = metadata.currentSnapshotId
            timestamp = metadata.lastUpdatedMs + update.timestampDeltaMs
            schemaId  = update.schemaId if update.hasSchemaId
                        else metadata.currentSchemaId
            firstRow  = metadata.nextRowId       // v3
            manifest  = manifestListPrefix + update.manifestListSuffix

            snapshot = buildSnapshot(
                update.snapshotId, seqNum, parentId, timestamp,
                schemaId, manifest, update.summary.toMap(),
                firstRow, update.addedRows)

            metadata = metadata.builder()
                .addSnapshot(snapshot)
                .setLastSequenceNumber(seqNum)
                .setLastUpdatedMs(timestamp)
                .setCurrentSnapshotId(update.snapshotId)
            if update.addedRows > 0:
                metadata.setNextRowId(metadata.nextRowId + update.addedRows)
            return metadata.build()

        case SET_SNAPSHOT_REF:
            ref = buildRef(update)
            return metadata.builder()
                .setRef(update.refName, ref)
                .build()

        case REMOVE_SNAPSHOTS:
            builder = metadata.builder()
            for id in update.snapshotIds:
                builder.removeSnapshot(id)
            return builder.build()

        case ADD_SCHEMA:
            schema = SchemaParser.fromJson(update.schemaJson)
            return metadata.builder()
                .addSchema(schema)
                .setLastColumnId(max(metadata.lastColumnId, update.lastColumnId))
                .build()

        case SET_CURRENT_SCHEMA:
            return metadata.builder()
                .setCurrentSchemaId(update.schemaId)
                .build()

        case ADD_PARTITION_SPEC:
            spec = PartitionSpecParser.fromJson(update.specJson)
            return metadata.builder()
                .addPartitionSpec(spec)
                .setLastPartitionId(max(metadata.lastPartitionId, update.lastPartitionId))
                .build()

        case SET_DEFAULT_PARTITION_SPEC:
            return metadata.builder()
                .setDefaultSpecId(update.specId)
                .build()

        case ADD_SORT_ORDER:
            order = SortOrderParser.fromJson(update.orderJson)
            return metadata.builder()
                .addSortOrder(order)
                .build()

        case SET_DEFAULT_SORT_ORDER:
            return metadata.builder()
                .setDefaultSortOrderId(update.orderId)
                .build()

        case SET_PROPERTIES:
            builder = metadata.builder()
            for (k, v) in update.updated:
                builder.setProperty(k, v)
            for k in update.removed:
                builder.removeProperty(k)
            return builder.build()

        case SET_TABLE_LOCATION:
            return metadata.builder()
                .setLocation(update.location)
                .build()
```

### Commit Protocol

The commit protocol is unchanged at the transaction level. The change is in how
table update actions are built:

```
function buildTableUpdateAction(table, newMetadata):
    if table.isInline:
        delta = computeDelta(table.currentMetadata, newMetadata, table.manifestListPrefix)
        mode  = selectMode(table, delta, currentTxnSize)
        switch mode:
            case DELTA:
                return UpdateTableInline(table.id, table.version, delta=delta)
            case FULL:
                return UpdateTableInline(table.id, table.version,
                                         fullMetadata=newMetadata.toJson())
            case POINTER:
                location = writeMetadataFile(newMetadata)
                return UpdateTableInline(table.id, table.version,
                                         metadataLocation=location)
    else:
        # Existing behavior for pointer-mode tables
        location = writeMetadataFile(newMetadata)
        return UpdateTableLocation(table.id, table.version,
                                   metadataLocation=location)
```

### computeDelta

Compares old and new `TableMetadata` and produces a minimal delta. For
`AddSnapshot`, the writer encodes fields relative to the per-table dictionary
(the old metadata's state).

```
function computeDelta(oldMeta, newMeta, manifestListPrefix):
    updates = []

    # New snapshots (most common) -- encode relative to dictionary
    oldSnapIds = {s.id for s in oldMeta.snapshots}
    for snap in newMeta.snapshots:
        if snap.id not in oldSnapIds:
            updates.add(AddSnapshot(
                snapshotId         = snap.snapshotId,
                manifestListSuffix = snap.manifestList.removePrefix(manifestListPrefix),
                timestampDeltaMs   = snap.timestampMs - oldMeta.lastUpdatedMs,
                schemaId           = snap.schemaId if snap.schemaId != oldMeta.currentSchemaId
                                     else OMITTED,
                addedRows          = snap.addedRows,    // v3; 0 if not v3
                summary            = CompactSummary.from(snap.summary),
            ))

    # Removed snapshots (expire)
    newSnapIds = {s.id for s in newMeta.snapshots}
    removed = oldSnapIds - newSnapIds
    if removed:
        updates.add(RemoveSnapshots(removed))

    # Ref changes
    for (name, ref) in newMeta.refs:
        if ref != oldMeta.refs.get(name):
            updates.add(SetSnapshotRef(name, ref))
    # (Ref removals handled by RemoveSnapshots cascading)

    # Schema changes
    oldSchemaIds = {s.id for s in oldMeta.schemas}
    for schema in newMeta.schemas:
        if schema.id not in oldSchemaIds:
            updates.add(AddSchema(schema))
    if newMeta.currentSchemaId != oldMeta.currentSchemaId:
        updates.add(SetCurrentSchema(newMeta.currentSchemaId))

    # Partition spec changes
    oldSpecIds = {s.id for s in oldMeta.specs}
    for spec in newMeta.specs:
        if spec.id not in oldSpecIds:
            updates.add(AddPartitionSpec(spec))
    if newMeta.defaultSpecId != oldMeta.defaultSpecId:
        updates.add(SetDefaultPartitionSpec(newMeta.defaultSpecId))

    # Sort order changes
    oldOrderIds = {o.id for o in oldMeta.sortOrders}
    for order in newMeta.sortOrders:
        if order.id not in oldOrderIds:
            updates.add(AddSortOrder(order))
    if newMeta.defaultSortOrderId != oldMeta.defaultSortOrderId:
        updates.add(SetDefaultSortOrder(newMeta.defaultSortOrderId))

    # Property changes
    added   = {k:v for k,v in newMeta.properties if oldMeta.properties.get(k) != v}
    removed = {k for k in oldMeta.properties if k not in newMeta.properties}
    if added or removed:
        updates.add(SetProperties(added, removed))

    # Location change
    if newMeta.location != oldMeta.location:
        updates.add(SetTableLocation(newMeta.location))

    return TableMetadataDelta(updates)
```

---

## Inline <-> Pointer Transitions

### Inline -> Pointer (Eviction)

Triggered when:
1. A transaction's inline content would exceed the 4 MiB append limit.
2. Compaction checkpoint would exceed the 16 MB threshold.

On eviction:
1. Serialize the current `TableMetadata` to a `v<N>.metadata.json` in the table's
   metadata directory.
2. Record the file location in the action (pointer mode).
3. On apply, move the table from `tblInlineMetadata` to `tblLocations`.

### Pointer -> Inline (Re-inlining)

Occurs:
1. During compaction: the compactor reads external metadata.json files and includes
   the metadata in the checkpoint's table embed region / `inline_tables`.
2. On the next table update: the writer can choose to inline the new metadata via
   full mode, transitioning the table to inline.

### Mixed-Mode Catalogs

A catalog can contain both inline and pointer tables simultaneously. The state model
supports this natively (separate maps for locations and metadata). This enables:

- Gradual migration from pointer to inline mode.
- Large, rarely-updated tables in pointer mode; active tables inline.
- Automatic eviction and re-inlining based on size constraints.

---

## Verification Rules

Inline actions follow the same version-based optimistic concurrency as existing
actions:

| Action                   | Precondition                                              |
|--------------------------|-----------------------------------------------------------|
| `CreateTableInline`      | Namespace version matches (skip if late-bound)            |
| `UpdateTableInline`      | Table version matches                                     |

`UpdateTableInline` subsumes the existing `UpdateTableLocation` for inline tables.
A version mismatch means a concurrent writer modified the table; the transaction
is rejected and retried with a refreshed catalog state, including the concurrent
writer's metadata changes.

---

## Summary

| Aspect                    | Current (pointer)               | Inline (delta + dictionary)       |
|---------------------------|---------------------------------|-----------------------------------|
| Writes per table update   | 2 (metadata.json + catalog)     | 1 (catalog only)                  |
| Per-commit payload        | 2--700 KB (full metadata.json)  | ~80--170 B (PB: ~80--115 B)      |
| Redundancy                | 90--99.9%                       | ~0%                               |
| Failure modes             | Orphaned metadata files         | Single atomic write               |
| Table load (catalog read) | Parse catalog + read metadata   | Parse catalog (metadata inline)   |
| Fallback                  | N/A                             | Pointer mode (external file)      |
| Azure 4 MiB limit         | N/A                             | Delta always fits; pointer if needed |
| Dictionary overhead       | N/A                             | ~100 B per table in checkpoint    |
| LCF compatibility         | All readers                     | Requires reader upgrade           |
| PB compatibility          | All readers                     | Old readers skip inline tables    |
