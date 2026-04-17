# Inline Manifest List Analysis

This document analyzes the contents of Iceberg manifest lists, how they change
across different operation types, and the feasibility of inlining manifest list
changes into catalog intention records -- absorbing the second level of Iceberg
table state into the catalog.

## Current Architecture

Today, every data commit writes **three** objects:

1. **Manifest file(s)** -- Avro files containing data file entries (paths,
   column stats, partition values). One or more per commit.
2. **Manifest list** -- Avro file listing all manifest files for the snapshot.
   Rewritten in full on every commit.
3. **Table metadata** -- JSON file with the full table state, pointing to the
   manifest list.

Plus the catalog update (a fourth write) pointing to the table metadata.

With [INLINE_INTENTION.md](./INLINE_INTENTION.md), we eliminated write 3 by
inlining table metadata deltas into the catalog. This document investigates
eliminating write 2 by inlining manifest list changes as well.

If successful, a data commit reduces to:

1. Write manifest file(s) (the actual data file index -- stays external)
2. Write a single catalog intention record containing both the table metadata
   delta and the manifest list delta

---

## Manifest List Structure

A manifest list is an Avro file where each record is a `manifest_file` struct.
The schema has evolved across format versions (v1, v2, v3); we focus on v2/v3
as the current production formats.

### ManifestFile Fields (v2/v3)

Source: `ManifestFile.java`, `V2Metadata.java`, `V3Metadata.java`

| ID  | Field                  | Type     | v2       | v3       | Description                                    |
|-----|------------------------|----------|----------|----------|------------------------------------------------|
| 500 | `manifest_path`        | string   | required | required | Location URI (FS scheme + full path)           |
| 501 | `manifest_length`      | long     | required | required | File size in bytes                             |
| 502 | `partition_spec_id`    | int      | required | required | Partition spec used to write the manifest      |
| 517 | `content`              | int      | required | required | 0 = data manifest, 1 = delete manifest        |
| 515 | `sequence_number`      | long     | required | required | Table sequence number when manifest was added  |
| 516 | `min_sequence_number`  | long     | required | required | Lowest sequence number of entries in manifest  |
| 503 | `added_snapshot_id`    | long     | required | required | Snapshot ID that added the manifest            |
| 504 | `added_files_count`    | int      | required | required | Count of ADDED entries in manifest             |
| 505 | `existing_files_count` | int      | required | required | Count of EXISTING entries in manifest          |
| 506 | `deleted_files_count`  | int      | required | required | Count of DELETED entries in manifest           |
| 512 | `added_rows_count`     | long     | required | required | Rows in ADDED entries                          |
| 513 | `existing_rows_count`  | long     | required | required | Rows in EXISTING entries                       |
| 514 | `deleted_rows_count`   | long     | required | required | Rows in DELETED entries                        |
| 507 | `partitions`           | list     | optional | optional | Per-partition-field summary (see below)         |
| 519 | `key_metadata`         | binary   | optional | optional | Encryption key metadata blob                   |
| 520 | `first_row_id`         | long     | --       | optional | V3+: starting row ID for ADDED data files      |

### PartitionFieldSummary

Each element in the `partitions` list summarizes one partition field across all
files in the manifest:

| ID  | Field           | Type    | Description                                |
|-----|-----------------|---------|--------------------------------------------|
| 509 | `contains_null` | boolean | True if any file has null for this field    |
| 518 | `contains_nan`  | boolean | True if any file has NaN (v2+, optional)   |
| 510 | `lower_bound`   | binary  | Partition lower bound (serialized value)   |
| 511 | `upper_bound`   | binary  | Partition upper bound (serialized value)   |

### Manifest List File Metadata

The Avro file also carries key-value metadata in its header:

| Key                  | Description                              |
|----------------------|------------------------------------------|
| `snapshot-id`        | Snapshot that owns this manifest list    |
| `parent-snapshot-id` | Parent snapshot ID                       |
| `sequence-number`    | Table sequence number at commit time     |
| `format-version`     | Table format version (1, 2, 3, or 4)    |
| `first-row-id`       | V3+: starting row ID for this snapshot   |

### Manifest File Path Convention

Manifest files: `<metadata-dir>/<commitUUID>-m<N>.avro`

```
gs://bucket/warehouse/db/tbl/metadata/a1b2c3d4-e5f6-7890-abcd-ef1234567890-m0.avro
|___________________________________________|  |____________________________________|
              shared prefix (~80 B)                    variable suffix (~45 B)
```

Manifest list files: `<metadata-dir>/snap-<snapshotId>-<attempt>-<commitUUID>.avro`

Both share the `<metadata-dir>/` prefix with the table location.

---

## Size Estimates

### Per-Entry Size

| Component                       | Size           | Notes                         |
|---------------------------------|----------------|-------------------------------|
| Fixed scalar fields (13 fields) | 76 bytes       | 5 longs + 4 ints + 3 longs + 1 int |
| `manifest_path`                 | ~100--200 B    | Full URI with scheme          |
| `partitions` (unpartitioned)    | 0 bytes        | Omitted if no partition spec  |
| `partitions` (1 partition field)| ~20--40 B      | bool + optional NaN + bounds  |
| `partitions` (3 fields)         | ~60--150 B     | Depends on bound size         |
| `key_metadata`                  | 0 bytes        | Usually null                  |
| `first_row_id` (v3)             | 8 bytes        | When present                  |
| Avro per-record overhead        | ~5--15 B       | Field tags, block framing     |

**Typical entry sizes:**

| Table type                  | Entry size        |
|-----------------------------|-------------------|
| Unpartitioned               | ~190--290 bytes   |
| Partitioned (1 field, int)  | ~220--330 bytes   |
| Partitioned (3 fields)      | ~280--440 bytes   |
| Wide partitions (6+ fields) | ~400--700 bytes   |

**Estimate used below: ~300 bytes per entry** (typical partitioned table).

### Total Manifest List Size

```
Size(N manifests) = Avro_header (~200 B) + N * ~300 bytes

After 1 commit:    ~500 B    (1 manifest)
After 10 commits:  ~3.2 KB   (10 manifests)
After 50 commits:  ~15 KB    (50 manifests)
After 100 commits: ~30 KB    (100 manifests)
After 1000 commits: ~300 KB  (1000 manifests)
```

Note: snapshot expiration removes old manifests, so the manifest count typically
plateaus at the retention window size. But between expiration runs, the list
grows monotonically with each append.

---

## What Changes Per Operation Type

### FastAppend (the common case, ~80--90% of commits)

The simplest and most common operation. `FastAppend.apply()` builds the new
manifest list by:

1. Writing 1 new manifest containing the appended data files
2. Concatenating ALL existing manifests from the parent snapshot unchanged

```
Before: [M1, M2, ..., MN]
After:  [M_new, M1, M2, ..., MN]
```

**Delta: +1 entry. All existing entries unchanged.**

This is a pure append at the manifest-list level. No existing entries are
modified, rewritten, or removed.

### MergeAppend (append with manifest compaction)

Same as FastAppend, but after concatenation, the merge manager may combine
small manifests into larger ones:

```
Before: [M1, M2, M3, M4, M5]  (M3, M4, M5 are small)
After:  [M_new, M1, M2, M_merged]
```

**Delta: +2 entries (M_new, M_merged), -3 entries (M3, M4, M5).**

Manifest merging is triggered by size thresholds in `ManifestMergeManager`.
Small manifests are packed into bins by size; bins with multiple manifests
are merged. The merge rewrites manifest file entries (ADDED -> EXISTING
for older entries, DELETED entries from non-current snapshots are dropped).

### Overwrite / Delete (MergingSnapshotProducer)

Overwrites and deletes filter the existing manifest list through
`ManifestFilterManager`, which:

1. Scans existing manifests for files that match the delete predicate
2. Rewrites affected manifests with those entries marked as DELETED
3. Drops manifests that become empty (no remaining ADDED or EXISTING entries)

```
Before: [M1, M2, M3, M4]
After:  [M_new, M1', M3, M4]   (M2 dropped, M1 rewritten as M1')
```

**Delta: +2 entries (M_new, M1'), -2 entries (M1, M2).**

The rewritten manifest M1' has a new path, new snapshot ID, updated counts
(deleted entries now tracked), but the same partition summaries and spec ID.

### Rewrite / Compaction (BaseRewriteFiles)

Compaction replaces a set of data files with a new, optimized set:

```
Before: [M1, M2, M3, M4]       (M2, M3 contain files being compacted)
After:  [M_new, M1, M2', M3', M4]  (M2', M3' have deletions; M_new has replacements)
```

**Delta: +1 new entry, 2 replaced entries (new paths/counts).**

In practice, the merge manager often combines the rewritten manifests, so the
net effect may be fewer total entries.

### Row-Level Deletes (BaseRowDelta)

Adds delete manifests (equality or position deletes):

```
Before: data=[M1, M2]  deletes=[D1]
After:  data=[M1, M2]  deletes=[D_new, D1]
```

**Delta: +1 delete manifest entry. Data manifests unchanged.**

### Summary: Operations and Manifest List Deltas

| Operation       | New entries | Removed entries | Replaced entries | Net change |
|-----------------|-------------|-----------------|------------------|------------|
| FastAppend      | 1           | 0               | 0                | +1         |
| MergeAppend     | 1--2        | 0--N (merged)   | 0                | +1 to -N+2|
| Overwrite       | 1--2        | 0--M (empty)    | 0--K (filtered)  | varies     |
| Delete          | 0--1        | 0--M            | 0--K             | varies     |
| Rewrite         | 1           | 0               | 1--K             | ~0         |
| RowDelta        | 1 (delete)  | 0               | 0                | +1         |

For the dominant case (FastAppend), the manifest list delta is exactly
**one new ManifestFile entry appended to the front**.

---

## Redundancy Analysis

The manifest list exhibits the **same redundancy pattern** as table metadata:
the entire file is rewritten on every commit, but only a small fraction is new.

### FastAppend Redundancy (the 80--90% case)

| Commits | New data     | Total ML size | Redundancy |
|---------|--------------|---------------|------------|
| 1       | ~300 B       | ~500 B        | 0%         |
| 10      | ~300 B       | ~3.2 KB       | 91%        |
| 50      | ~300 B       | ~15 KB        | 98%        |
| 100     | ~300 B       | ~30 KB        | 99%        |
| 1000    | ~300 B       | ~300 KB       | 99.9%      |

After 10 commits, **91% of every manifest list write is data that was already
written in the previous version.** The pattern is nearly identical to the
metadata.json redundancy analyzed in [INLINE_TBL_UPDATE.md](./INLINE_TBL_UPDATE.md).

### Within-Entry Redundancy

Even for the 1 new entry per FastAppend, many fields are predictable:

| Field                  | Entropy     | Notes                                     |
|------------------------|-------------|-------------------------------------------|
| `manifest_path`        | ~45 B       | Prefix shared with all other entries       |
| `manifest_length`      | high        | Varies by manifest size                    |
| `partition_spec_id`    | ~0          | Same as all other entries (rarely changes) |
| `content`              | ~0          | 0 (DATA) for data commits                 |
| `sequence_number`      | 1 B delta   | = table sequence number = previous + 1    |
| `min_sequence_number`  | 1 B delta   | = sequence_number for new manifests       |
| `added_snapshot_id`    | 8 B         | = snapshot ID (random)                    |
| `added_files_count`    | low         | Small int, varint-friendly                |
| `existing_files_count` | ~0          | 0 for brand-new manifests                 |
| `deleted_files_count`  | ~0          | 0 for brand-new manifests                 |
| `added_rows_count`     | medium      | Varint-friendly                           |
| `existing_rows_count`  | ~0          | 0 for brand-new manifests                 |
| `deleted_rows_count`   | ~0          | 0 for brand-new manifests                 |
| `partitions`           | variable    | Must be transmitted (data-dependent)      |
| `first_row_id`         | ~0          | Derivable from table state (v3)           |

For a brand-new manifest from a FastAppend, 6 of 13 count fields are zero
(existing and deleted counts are all 0), `partition_spec_id` and `content` are
constants, and `sequence_number` / `min_sequence_number` are derivable. The
truly variable fields are: manifest path suffix, manifest length, file/row
counts for added entries, partition summaries, and the snapshot ID.

---

## Inline Manifest List Delta

### Delta Operations

Two operations cover all manifest list mutations:

```
ADD_MANIFEST    -- a new or rewritten ManifestFile entry
REMOVE_MANIFEST -- drop an entry by manifest path
```

Every commit type is expressible as a combination:

| Commit type     | Delta                                          |
|-----------------|------------------------------------------------|
| FastAppend      | 1 ADD_MANIFEST                                 |
| MergeAppend     | 1+ ADD_MANIFEST, 0+ REMOVE_MANIFEST            |
| Overwrite       | 1+ ADD_MANIFEST, 0+ REMOVE_MANIFEST            |
| Delete          | 0+ ADD_MANIFEST, 0+ REMOVE_MANIFEST            |
| Rewrite         | 1+ ADD_MANIFEST, 0+ REMOVE_MANIFEST            |
| RowDelta        | 1 ADD_MANIFEST (delete manifest)               |

### ADD_MANIFEST Entry Format

A new ManifestFile entry in the delta, using the same dictionary approach
as [INLINE_INTENTION.md](./INLINE_INTENTION.md):

**Fields carried in the delta:**

| Field                  | Encoding                | Size (PB)     | Size (LCF)    |
|------------------------|-------------------------|---------------|----------------|
| `manifest_path_suffix` | String (prefix shared)  | ~47 B         | ~49 B          |
| `manifest_length`      | varint / i64            | ~4 B          | 8 B            |
| `content`              | enum / u8               | 0--1 B        | 1 B            |
| `added_snapshot_id`    | omit if = commit snap   | 0 B           | 0 B            |
| `added_files_count`    | varint / i32            | ~2 B          | 4 B            |
| `added_rows_count`     | varint / i64            | ~3 B          | 8 B            |
| `existing_files_count` | omit if 0               | 0 B           | 0 B*           |
| `existing_rows_count`  | omit if 0               | 0 B           | 0 B*           |
| `deleted_files_count`  | omit if 0               | 0 B           | 0 B*           |
| `deleted_rows_count`   | omit if 0               | 0 B           | 0 B*           |
| `partitions`           | binary (same as Avro)   | ~20--150 B    | ~20--150 B     |
| **Total (new manifest)** |                       | **~76--207 B**| **~90--220 B** |

*LCF: use a flags byte to indicate which optional counts are present.

**Fields derived from catalog state (not transmitted):**

| Field                 | Derivation                                     |
|-----------------------|------------------------------------------------|
| `manifest_path`       | `table.manifestPathPrefix + suffix`            |
| `partition_spec_id`   | Omit if = table's default spec; else explicit  |
| `sequence_number`     | = snapshot's sequence number (from AddSnapshot)|
| `min_sequence_number` | = sequence_number for brand-new manifests; explicit otherwise |
| `added_snapshot_id`   | = snapshot ID being committed                  |
| `first_row_id`        | Assigned during manifest list write (derivable from running count) |

### REMOVE_MANIFEST Format

A removal identifies the manifest to drop:

| Field                  | Encoding             | Size (PB)  | Size (LCF) |
|------------------------|----------------------|------------|-------------|
| `manifest_path_suffix` | String (prefix)      | ~47 B      | ~49 B       |

Alternatively, manifests can be identified by index position in the current
list (varint, 1--3 bytes), but path-based removal is safer across concurrent
retries.

### Carried-Forward Manifests

Manifests that are neither added nor removed are **implicit** -- they are
already in the catalog's in-memory manifest list from the checkpoint or
previous deltas. No bytes are transmitted for carried-forward manifests.

This is the key insight: in the current architecture, carried-forward manifests
are rewritten into every manifest list Avro file. With inline deltas, they
cost zero bytes per commit.

---

## Size Comparison

### Per-Commit Write Cost

| Component               | Current          | Inline (delta)      | Savings   |
|-------------------------|------------------|---------------------|-----------|
| Manifest list file      | N * ~300 B       | ~80--210 B          | 90--99.9% |
| Table metadata file     | 2--700 KB        | ~80--170 B          | 90--99.9% |
| Catalog update          | ~50 B (pointer)  | (included above)    | --        |
| Manifest file(s)        | unchanged        | unchanged           | 0%        |
| **Total overhead**      | **2 KB -- 1 MB** | **~160--380 B**     | **97--99.9%** |

### Cumulative Write Amplification

For a table with 100 manifests (100 prior appends, no expiration yet):

| Write                    | Current size    | Inline delta    |
|--------------------------|-----------------|-----------------|
| Manifest list            | ~30 KB          | ~150 B (1 ADD)  |
| Table metadata           | ~70 KB          | ~100 B          |
| Catalog transaction      | ~50 B           | ~250 B (total)  |
| **Total per commit**     | **~100 KB**     | **~250 B**      |
| **Write amplification**  | **~400x**       | **~1x**         |

After 100 appends, the current approach has written ~5 MB of manifest list
files (100 + 99 + 98 + ... + 1 entries * 300 B), while the inline approach
has written ~15 KB (100 entries * 150 B).

---

## Checkpoint and State Model

### Manifest List in Catalog State

The catalog state for each inline table gains a manifest list:

| Field               | Type                     | Description                     |
|---------------------|--------------------------|---------------------------------|
| `dataManifests`     | `List<ManifestFile>`     | Current data manifests          |
| `deleteManifests`   | `List<ManifestFile>`     | Current delete manifests        |
| `manifestPathPrefix`| `String`                 | Shared path prefix for manifests|

These are populated from the checkpoint and updated by applying
`ADD_MANIFEST` / `REMOVE_MANIFEST` deltas during log replay.

### Checkpoint Format

The checkpoint already includes full `TableMetadata` JSON for each inline
table. The manifest list can be included alongside it:

**LCF table embed region** -- extend each entry:

```
Per-table entry:
  tblId:                i32
  tblVer:               i32
  nsid:                 i32
  name:                 utf
  manifestListPrefix:   utf     (shared prefix for manifest paths)
  manifestPathPrefix:   utf     (shared prefix for manifest file paths)
  metadataLen:          i32
  metadata:             byte[]  (TableMetadata JSON)
  nManifests:           i32     (number of ManifestFile entries)
  manifests:            byte[]  (ManifestFile entries, Avro or binary)
```

**PB checkpoint** -- extend `InlineTable`:

```protobuf
message InlineTable {
  // ... existing fields ...
  string manifest_path_prefix  = 7;   // shared prefix for manifest file paths
  repeated ManifestFileEntry manifests = 8;
}

message ManifestFileEntry {
  string manifest_path_suffix   = 1;
  int64  manifest_length        = 2;
  int32  partition_spec_id      = 3;
  int32  content                = 4;   // 0=data, 1=deletes
  int64  sequence_number        = 5;
  int64  min_sequence_number    = 6;
  int64  added_snapshot_id      = 7;
  int32  added_files_count      = 8;
  int32  existing_files_count   = 9;
  int32  deleted_files_count    = 10;
  int64  added_rows_count       = 11;
  int64  existing_rows_count    = 12;
  int64  deleted_rows_count     = 13;
  repeated PartitionFieldSummary partitions = 14;
  bytes  key_metadata           = 15;
  int64  first_row_id           = 16;
}

message PartitionFieldSummary {
  bool   contains_null = 1;
  bool   contains_nan  = 2;
  bytes  lower_bound   = 3;
  bytes  upper_bound   = 4;
}
```

### Checkpoint Size Impact

A manifest list in the checkpoint adds ~300 bytes per manifest. For a table
with 100 manifests, this is ~30 KB -- the same as the current manifest list
Avro file. The checkpoint grows, but this is offset by no longer writing
separate manifest list files.

---

## Integration with INLINE_INTENTION.md

### Extended AddSnapshot Delta

The `AddSnapshot` delta from INLINE_INTENTION.md is extended to include
manifest list changes:

```
AddSnapshot:
  snapshot_id            (8 bytes, random)
  manifest_list_suffix   -- REMOVED (no external manifest list file)
  summary                (CompactSummary)
  timestamp_delta_ms     (signed delta)
  schema_id              (if changed)
  added_rows             (v3+)
  manifest_deltas:       (NEW -- manifest list changes)
    repeated ADD_MANIFEST
    repeated REMOVE_MANIFEST
```

The `manifest_list` / `manifest_list_suffix` field is **no longer needed**
for inline tables -- there is no external manifest list file. Instead, the
manifest list is maintained inline and the `manifest_deltas` field carries
the changes.

For pointer-mode tables (fallback), the existing `manifest_list` field
continues to point to an external Avro file, and `manifest_deltas` is empty.

### PB Encoding

```protobuf
message AddSnapshot {
  fixed64            snapshot_id          = 1;
  CompactSummary     summary              = 3;
  sint64             timestamp_delta_ms   = 4;
  int32              schema_id            = 5;
  int64              added_rows           = 6;

  // Manifest list delta (replaces manifest_list_suffix)
  repeated AddManifest    add_manifests    = 10;
  repeated RemoveManifest remove_manifests = 11;

  // Fallback: external manifest list (pointer-mode only)
  string manifest_list_suffix              = 2;
}

message AddManifest {
  string manifest_path_suffix   = 1;   // relative to table's manifest path prefix
  int64  manifest_length        = 2;
  int32  partition_spec_id      = 3;   // 0 = omitted, use table default
  int32  content                = 4;   // 0=data, 1=deletes
  int32  added_files_count      = 5;
  int64  added_rows_count       = 6;
  int32  existing_files_count   = 7;   // 0 = omitted (new manifest)
  int64  existing_rows_count    = 8;   // 0 = omitted
  int32  deleted_files_count    = 9;   // 0 = omitted
  int64  deleted_rows_count     = 10;  // 0 = omitted
  repeated PartitionFieldSummary partitions = 11;
  bytes  key_metadata           = 12;
  int64  sequence_number        = 13;  // 0 = use snapshot's sequence number
  int64  min_sequence_number    = 14;  // 0 = use sequence_number
  int64  first_row_id           = 15;  // v3+
}

message RemoveManifest {
  string manifest_path_suffix = 1;     // identifies manifest to remove
}
```

### LCF Encoding

Add new delta sub-opcodes within `UPDATE_TABLE_INLINE`:

```
Sub     Name                    Payload
---     ----                    -------
0x0C    ADD_MANIFEST             manifestPathSuffix:utf
                                 manifestLength:i64
                                 flags:u8  (bit 0: has partition summaries,
                                            bit 1: has existing counts,
                                            bit 2: has deleted counts,
                                            bit 3: non-default spec,
                                            bit 4: delete manifest,
                                            bit 5: has explicit seq nums,
                                            bit 6: has key metadata,
                                            bit 7: has first_row_id)
                                 addedFilesCount:i32
                                 addedRowsCount:i64
                                 [partSpecId:i32]          (if flags bit 3)
                                 [existFilesCount:i32]     (if flags bit 1)
                                 [existRowsCount:i64]      (if flags bit 1)
                                 [delFilesCount:i32]       (if flags bit 2)
                                 [delRowsCount:i64]        (if flags bit 2)
                                 [seqNum:i64]              (if flags bit 5)
                                 [minSeqNum:i64]           (if flags bit 5)
                                 [nPartitions:i32  partitionData:byte[]] (if flags bit 0)
                                 [keyMetadataLen:i32  keyMetadata:byte[]] (if flags bit 6)
                                 [firstRowId:i64]          (if flags bit 7)

0x0D    REMOVE_MANIFEST          manifestPathSuffix:utf
```

The flags byte keeps the common case compact: a brand-new data manifest from
a FastAppend needs only bits 0 (partitions) and possibly bit 7 (first_row_id),
resulting in zero overhead for the six count fields that are all zero.

---

## What Remains External

With inline manifest lists, the storage hierarchy becomes:

```
Catalog file (single object):
  ├── Catalog state (namespaces, table registry)
  ├── Per-table: TableMetadata (schemas, specs, properties, snapshot history)
  └── Per-table: Manifest list (ManifestFile entries)

External storage:
  ├── Manifest files (.avro) -- data file entries with column stats
  └── Data files (.parquet/.orc) -- the actual data
```

The catalog absorbs **two levels** of table state:
1. Table metadata (was metadata.json)
2. Manifest list (was snap-*.avro)

Manifest files remain external because they contain per-data-file entries
(paths, column statistics, partition values, etc.) that are much larger and
less amenable to delta encoding.

---

## Considerations

### Manifest File References

Manifest files are still external Avro objects. The `manifest_path` in each
ManifestFile entry points to these external files. Query engines need to read
manifest files to plan scans. With inline manifest lists, the catalog can
serve the manifest list directly, so engines skip the manifest list file read
but still read individual manifest files.

### Partition Summaries

Partition summaries (`partitions` field) are the most variable-size component
of a ManifestFile entry. For tables with many partition fields or wide partition
value ranges (e.g., high-cardinality string partitions), summaries can be
100--300+ bytes per entry. These must be transmitted in each `ADD_MANIFEST`
since they depend on the data written.

For unpartitioned tables, partition summaries are omitted, and the per-entry
size drops to ~75--100 bytes in PB encoding.

### Size Budget (Azure 4 MiB)

A single `ADD_MANIFEST` is ~80--220 bytes. Even a complex multi-manifest
commit (e.g., merge that adds 5 manifests and removes 20) fits easily:

```
5 * 200 B (ADD) + 20 * 50 B (REMOVE) = 2 KB
```

Combined with the table metadata delta (~100 B), the total intention record
for even complex commits is well under 10 KB.

### Snapshot Expiration

Snapshot expiration removes old snapshots and their exclusively-referenced
manifests. With inline manifest lists, expiration produces `REMOVE_MANIFEST`
deltas for manifests that are no longer referenced by any retained snapshot.

The `RemoveSnapshots` table metadata update (already defined in
INLINE_INTENTION.md) is extended: when a snapshot is removed, the catalog
identifies manifests that were only referenced by that snapshot and emits
`REMOVE_MANIFEST` entries for them.

### Manifest Merging

Manifest merging (combining small manifests into larger ones) happens during
`MergingSnapshotProducer` commits. The engine writes the merged manifest file
externally, then the intention record carries:
- `ADD_MANIFEST` for the merged manifest
- `REMOVE_MANIFEST` for each constituent manifest

The catalog applies these atomically.

### Reading Manifest Files During Commits

Some operations (overwrite, delete, rewrite) need to **read** existing manifests
to filter entries. With inline manifest lists, the catalog provides the
`ManifestFile` metadata (path, counts, partitions), but the engine still reads
the actual manifest file contents from external storage. The inline manifest
list does not change this -- it only eliminates the manifest list Avro file,
not the manifest files themselves.

### Concurrent Writers

The existing optimistic concurrency model applies unchanged. The table version
check ensures that the manifest list delta is applied to the correct base state.
If a concurrent writer modified the manifest list (by committing a different
snapshot), the version check fails and the transaction retries with refreshed
state.

On retry, the engine re-reads the updated manifest list (now from the catalog
rather than an external file), recomputes the delta, and commits again.

---

## Implementation in FileIOCatalog

This section describes how to implement inline manifest lists in the existing
`FileIOCatalog`, building on the proven pattern used for inline table metadata.

### How Inline Table Metadata Is Hidden Today

The existing inline metadata implementation in `FileIOCatalog.java:309-329`
uses a **custom loader lambda** to bypass FileIO entirely:

```java
// FileIOCatalog.java:319-321
String syntheticLoc = "inline://" + tableId + "#v" + System.nanoTime();
refreshFromMetadataLocation(syntheticLoc, null, 0,
    loc -> TableMetadataParser.fromJson(loc, json));
```

The synthetic location never reaches `FileIO`. The lambda parses JSON bytes
already in memory. No wrapper needed. This works because `TableOperations`
provides a clean seam: `refreshFromMetadataLocation` accepts a custom loader.

### Why Manifest Lists Require a Different Approach

Manifest lists are loaded inside `BaseSnapshot.cacheManifests()`:

```java
// BaseSnapshot.java:183-185
if (allManifests == null) {
    this.allManifests = ManifestLists.read(fileIO.newInputFile(manifestListLocation));
}
```

`Snapshot.allManifests(FileIO)`, `dataManifests(FileIO)`, and
`deleteManifests(FileIO)` all funnel through `cacheManifests`, which calls
`fileIO.newInputFile(manifestListLocation)`. There is no lambda seam at this
level, but the write path *does* have a hookable seam if we extend
`SnapshotProducer` itself — which we can, since this is a fork.

### Implementation in Iceberg Core (Done)

The `vldb-1.10.1-ml` branch adds two extension points to Iceberg core that
the FileIO catalog can opt into.

**`ManifestListSink` interface** (`core/src/main/java/org/apache/iceberg/ManifestListSink.java`):

```java
public interface ManifestListSink {
  void stageManifestListDelta(
      long sequenceNumber,
      long snapshotId,
      Long parentSnapshotId,
      Long nextRowId,
      ManifestListDelta delta,          // added ManifestFiles + removed paths
      Long nextRowIdAfter);

  final class ManifestListDelta {
    public List<ManifestFile> added();
    public List<String> removedPaths();
  }
}
```

A `TableOperations` implementation that also implements `ManifestListSink`
signals that it wants to absorb manifest list state. When this is detected in
`SnapshotProducer.apply()`, the producer:

1. Computes the full finalized manifest list (enriched, sequence numbers and
   v3+ `first_row_id` assigned — identical to what `ManifestListWriter` would
   write to Avro).
2. Computes the **delta** between the parent snapshot's manifest list and the
   finalized list, comparing by `manifest_path`.
3. Calls `sink.stageManifestListDelta(...)` with only the delta (adds +
   removed paths). The catalog stores the delta in its intention record.
4. Returns an `InlineSnapshot` that holds the full in-memory list, so the same
   JVM session can continue to commit without reading the manifest list from
   storage.

**No `snap-*.avro` file is written** when the sink path is taken.

**`InlineSnapshot`** (`core/src/main/java/org/apache/iceberg/InlineSnapshot.java`)
implements `Snapshot` as a standalone class holding the full manifest list
in memory. `manifestListLocation()` returns `null`; `allManifests(io)` /
`dataManifests(io)` / `deleteManifests(io)` return the in-memory list without
touching FileIO. The file-diff methods (`addedDataFiles`, `removedDataFiles`,
etc.) still use FileIO — but only to read *manifest files* (external Avro),
not the manifest list itself.

### What Changes in the Default Path

Nothing. The `SnapshotProducer.apply()` refactor keeps the existing Avro-file
write path unchanged: when `ops` does not implement `ManifestListSink`, the
producer writes the full manifest list to `snap-*.avro` and returns a normal
`BaseSnapshot` with `manifestListLocation` set to the file path.

All 610+ existing manifest-list-exercising tests pass unmodified.

---

## FileIO Catalog Integration (To Implement)

The `vldb-1.10.1-ml` branch lands the Iceberg core hooks. The corresponding
FileIO catalog changes are additive — they build on the existing
`inlineEnabled` pathway without replacing it.

### State Model Extensions in `ProtoCatalogFile`

Add two per-table fields to the in-memory catalog state:

| Field               | Type                              | Role                                |
|---------------------|-----------------------------------|-------------------------------------|
| `manifestPool`      | `Map<Integer, List<ManifestFile>>`| Per-table pool of unique manifests  |
| `snapshotManifests` | `Map<Integer, Map<Long, int[]>>`  | Per-snapshot index lists into pool  |

Store each unique `ManifestFile` once per table in `manifestPool` (keyed by
`manifest_path`). Each snapshot's manifest list is a `int[]` of indices into
the pool. Consecutive snapshots share almost all their manifests (FastAppend
adds 1, keeps N-1), so the pool approach gets ~50× compression vs storing
each snapshot's full list.

Accessor on `ProtoCatalogFile`:

```java
public List<ManifestFile> inlineManifests(int tblId, long snapshotId) {
  int[] indices = snapshotManifests.get(tblId).get(snapshotId);
  List<ManifestFile> pool = manifestPool.get(tblId);
  return Arrays.stream(indices).mapToObj(pool::get).collect(toImmutableList());
}

public boolean hasInlineManifests(int tblId, long snapshotId) {
  return snapshotManifests.get(tblId) != null
      && snapshotManifests.get(tblId).containsKey(snapshotId);
}
```

### FileIOTableOperations Changes

Make `FileIOTableOperations` implement `ManifestListSink`. The sink method
stashes the delta on an instance field for `doCommit` to pick up after
`SnapshotProducer.commit()` returns:

```java
static class FileIOTableOperations extends BaseMetastoreTableOperations
    implements ManifestListSink {

  // Staged by stageManifestListDelta, consumed by doCommit
  private final Map<Long, ManifestListDelta> stagedDeltas = new LinkedHashMap<>();

  @Override
  public void stageManifestListDelta(
      long sequenceNumber, long snapshotId, Long parentSnapshotId,
      Long nextRowId, ManifestListDelta delta, Long nextRowIdAfter) {
    if (inlineManifestsEnabled) {
      stagedDeltas.put(snapshotId, delta);
    }
    // If inline manifests are disabled, do nothing — SnapshotProducer will
    // still have written the Avro file in this case (it only calls the sink
    // when ops implements ManifestListSink, so return false from that check).
  }
}
```

**Subtle point:** `SnapshotProducer` only checks `ops instanceof
ManifestListSink`. If you implement the interface unconditionally, you lose
the ability to toggle inline manifests on/off per commit. Options:

- **Feature flag via property:** Implement `ManifestListSink` conditionally
  based on a catalog property (`fileio.catalog.inline.manifests=true`) by
  wrapping `ops` with a forwarding class that adds or omits the interface.
- **Delegate-and-discard:** Always implement the interface, but in
  `stageManifestListDelta`, if inline manifests are disabled, request
  Iceberg to fall back to the Avro path. Since `SnapshotProducer` already
  committed to the sink path by the time it calls `stageManifestListDelta`,
  this requires a tweak: introduce a return value or a sentinel exception
  signalling "fall back."

The simplest MVP is the feature flag approach with two classes:

```java
// Default: no ManifestListSink; Avro path is used
class FileIOTableOperations extends BaseMetastoreTableOperations { ... }

// Inline-manifests-enabled: also implements ManifestListSink
class InlineManifestTableOperations extends FileIOTableOperations
    implements ManifestListSink { ... }
```

`FileIOCatalog.newTableOps(...)` picks the right subclass based on the
catalog-level property `fileio.catalog.inline.manifests`.

### loadFromCatalogFile Extension

When loading an inline table whose snapshots have inline manifest lists,
wrap each such snapshot with an `InlineSnapshot` in the custom-loader lambda:

```java
private synchronized void loadFromCatalogFile(CatalogFile catalogFile) {
  if (catalogFile.isInlineTable(tableId)) {
    byte[] inlineMeta = catalogFile.inlineMetadata(tableId);
    String json = new String(inlineMeta, StandardCharsets.UTF_8);
    String syntheticLoc = "inline://" + tableId + "#v" + System.nanoTime();

    refreshFromMetadataLocation(syntheticLoc, null, 0, loc -> {
      TableMetadata parsed = TableMetadataParser.fromJson(loc, json);
      if (!(catalogFile instanceof ProtoCatalogFile)) return parsed;
      ProtoCatalogFile proto = (ProtoCatalogFile) catalogFile;
      Integer tblId = proto.tableId(tableId);
      if (tblId == null) return parsed;

      // Replace each snapshot with InlineSnapshot if we have its inline ML
      TableMetadata.Builder builder = TableMetadata.buildFrom(parsed);
      for (Snapshot s : parsed.snapshots()) {
        if (proto.hasInlineManifests(tblId, s.snapshotId())) {
          List<ManifestFile> manifests =
              proto.inlineManifests(tblId, s.snapshotId());
          Snapshot wrapped = new InlineSnapshot(
              s.sequenceNumber(), s.snapshotId(), s.parentId(),
              s.timestampMillis(), s.operation(), s.summary(),
              s.schemaId(), s.firstRowId(), s.addedRows(), s.keyId(),
              manifests);
          builder.removeSnapshots(ImmutableList.of(s));
          builder.addSnapshot(wrapped);
        }
      }
      return builder.build();
    });
  } else {
    // ... existing pointer-mode logic ...
  }
}
```

`InlineSnapshot` is in package `org.apache.iceberg` (not `org.apache.iceberg.io`)
because it needs access to `ManifestFiles`, `ManifestGroup`, etc. to implement
`addedDataFiles`/`removedDataFiles`. It has a public-by-default constructor
that `fileio-catalog` can call from `org.apache.iceberg.io`.

**Wait — `InlineSnapshot` is package-private in core.** To call its
constructor from `org.apache.iceberg.io`, either:

- Make `InlineSnapshot` public (requires a small change to core). Preferred.
- Add a public factory method in `org.apache.iceberg` (e.g.,
  `BaseSnapshotFactory.inline(...)`) that constructs it.
- Use reflection (not recommended).

Making `InlineSnapshot` public is the cleanest option since we control the
Iceberg fork. Follow-up commit on `vldb-1.10.1-ml`.

### commitInline Extension

After the existing table metadata delta is computed, drain the staged
manifest list deltas and attach them to the intention record:

```java
private void commitInline(TableMetadata base, TableMetadata metadata, boolean isCreate) {
  if (isCreate) {
    // ... existing create logic ...
    // For create: stagedDeltas contains one entry for the initial snapshot,
    // which is the full initial manifest list (empty parent).
    return;
  }

  // Existing: compute table metadata delta
  List<InlineDeltaCodec.DeltaUpdate> delta =
      InlineDeltaCodec.computeDelta(base, metadata, manifestPrefix);

  // NEW: for each new snapshot, attach its manifest list delta
  for (Snapshot newSnap : metadata.snapshots()) {
    if (base.snapshot(newSnap.snapshotId()) != null) continue;
    ManifestListDelta mlDelta = stagedDeltas.remove(newSnap.snapshotId());
    if (mlDelta != null) {
      InlineDeltaCodec.attachManifestDelta(
          delta, newSnap.snapshotId(),
          mlDelta.added(), mlDelta.removedPaths());
    }
  }

  // Existing: select mode and commit
  String mode = InlineDeltaCodec.selectMode(delta, metadata, 0);
  // ... switch on mode: delta / full / pointer ...
}
```

### InlineDeltaCodec Extensions

Add two new `DeltaUpdate` subtypes:

```java
// In InlineDeltaCodec.java
class AddManifestUpdate extends DeltaUpdate {
  final long snapshotId;       // which snapshot this manifest belongs to
  final ManifestFile manifest; // the full ManifestFile entry to add to the pool
}

class RemoveManifestUpdate extends DeltaUpdate {
  final long snapshotId;
  final String manifestPath;   // path of the manifest to remove from snapshot refs
}
```

Update:

- `computeDelta` — no change (it doesn't compute ML deltas; those come from
  the sink).
- `encodeDelta` / `decodeDelta` — extend the protobuf/wire format with the
  two new update types. The encoding can use a single `ManifestFile` message
  matching the Iceberg Avro schema (reuse the existing protobuf
  `ManifestFileEntry` from `catalog.proto`).
- `applyDelta` — when reading the log, each `AddManifestUpdate` appends its
  `ManifestFile` to the table's pool (if not already present) and appends the
  pool index to the snapshot's reference list. Each `RemoveManifestUpdate`
  removes the path from the snapshot's reference list.
- `selectMode` — inline manifest deltas are small (~150-300 B per add); they
  don't affect mode selection materially.

A new `attachManifestDelta(delta, snapshotId, added, removedPaths)` helper
method adds the right set of `AddManifestUpdate`/`RemoveManifestUpdate`
entries to the existing `List<DeltaUpdate>`.

### Checkpoint Serialization

On checkpoint (CAS), the `ProtoCatalogFormat` writes the full inline state.
Extend `InlineTable` in `catalog.proto`:

```protobuf
message InlineTable {
  // ... existing fields (1-6) ...
  string manifest_path_prefix                 = 7;
  repeated ManifestFileEntry manifest_pool    = 8;
  repeated SnapshotManifestRefs snapshot_refs = 9;
}

message SnapshotManifestRefs {
  int64 snapshot_id          = 1;
  repeated int32 indices     = 2;  // positions in manifest_pool
}

message ManifestFileEntry {
  string manifest_path_suffix   = 1;   // after manifest_path_prefix
  int64  manifest_length        = 2;
  int32  partition_spec_id      = 3;
  int32  content                = 4;   // 0=data, 1=deletes
  int64  sequence_number        = 5;
  int64  min_sequence_number    = 6;
  int64  added_snapshot_id      = 7;
  int32  added_files_count      = 8;
  int32  existing_files_count   = 9;
  int32  deleted_files_count    = 10;
  int64  added_rows_count       = 11;
  int64  existing_rows_count    = 12;
  int64  deleted_rows_count     = 13;
  repeated PartitionFieldSummary partitions = 14;
  bytes  key_metadata           = 15;
  int64  first_row_id           = 16;
}
```

`ProtoCodec` converts between `ManifestFile` (Iceberg API) and
`ManifestFileEntry` (protobuf) in both directions.

### Feature Flag

Add to `FileIOCatalog` configuration:

```java
public static final String INLINE_MANIFESTS = "fileio.catalog.inline.manifests";
public static final boolean INLINE_MANIFESTS_DEFAULT = false;
```

When disabled, tables use inline metadata (if enabled) but manifest lists are
still written to Avro files — the `FileIOTableOperations` does not implement
`ManifestListSink`, so `SnapshotProducer` takes the default Avro path.

When enabled on a per-table basis, commits automatically start storing
manifest lists inline. Existing snapshots continue to reference their Avro
manifest list files (carried forward in `manifestListLocation`); only new
snapshots use the inline path. This allows gradual migration without a
one-time rewrite.

### Integration Checklist

1. **Make `InlineSnapshot` public** (in iceberg-core on `vldb-1.10.1-ml`)
   so fileio-catalog can construct it.

2. **`ProtoCatalogFile`**: add `manifestPool` and `snapshotManifests` maps
   plus `inlineManifests()` / `hasInlineManifests()` accessors.

3. **`catalog.proto`**: extend `InlineTable` with `manifest_pool` and
   `snapshot_refs`; add `ManifestFileEntry` and `SnapshotManifestRefs`
   messages. Regenerate.

4. **`ProtoCodec`**: add `ManifestFile ⇄ ManifestFileEntry` conversions.

5. **`InlineDeltaCodec`**: add `AddManifestUpdate` and `RemoveManifestUpdate`
   types; extend `encodeDelta` / `decodeDelta` / `applyDelta`; add
   `attachManifestDelta` helper.

6. **`FileIOTableOperations`**: make it (conditionally, via subclass or
   wrapper) implement `ManifestListSink`. Stash deltas in a map keyed by
   snapshot ID.

7. **`loadFromCatalogFile`**: when `ProtoCatalogFile.hasInlineManifests(tblId,
   snapId)`, replace the parsed `BaseSnapshot` with an `InlineSnapshot`
   carrying the resolved manifest list from the pool.

8. **`commitInline`**: drain `stagedDeltas` and call
   `InlineDeltaCodec.attachManifestDelta` for each new snapshot.

9. **`FileIOCatalog` configuration**: add `fileio.catalog.inline.manifests`
   property and wire it through to `FileIOTableOperations`.

10. **Tests**: extend `TestInlineDelta` and `TestProtoActions` with
    manifest list cases — FastAppend (1 add, 0 remove), delete (1 add + 1
    remove for the rewritten manifest), snapshot expiration (removes only),
    compaction/merge (multiple adds + multiple removes).

11. **End-to-end test**: commit through the inline manifest path, refresh the
    catalog, and verify `snapshot.allManifests(io)` returns the correct list
    without reading any `snap-*.avro` file from storage.

### Build Note

`fileio-catalog` consumes iceberg as a Maven `1.11.0-SNAPSHOT` artifact. To
use the `ManifestListSink` / `InlineSnapshot` additions, publish iceberg to
the local Maven repository first:

```bash
cd iceberg
./gradlew publishToMavenLocal -x test -x integrationTest -x generateGitProperties
cd ../fileio-catalog
mvn clean install
```

The `-x generateGitProperties` flag is required because `iceberg/` isn't a
standalone git repository in this workspace.

---

## Combined Savings: Three Levels

| Level      | Current write          | Inline write       | Writes saved |
|------------|------------------------|--------------------|--------------|
| Metadata   | metadata.json (2--700 KB) | ~100 B delta    | 1 file       |
| Manifest list | snap-*.avro (3--300 KB) | ~150 B delta   | 1 file       |
| Catalog    | ~50 B pointer update   | ~250 B total       | --           |
| Manifest files | unchanged           | unchanged          | 0            |
| Data files | unchanged              | unchanged          | 0            |
| **Total**  | **3 files + catalog**  | **1 catalog write** | **2 files**  |

A typical data commit (100-manifest table):

```
Current:   metadata.json (70 KB) + manifest list (30 KB) + catalog (50 B) = ~100 KB across 3 writes
Inline:    catalog intention record (250 B) = ~250 B in 1 write

Reduction: ~400x fewer bytes, 3 writes → 1 write
```

---

## Summary

The manifest list exhibits the same redundancy pattern as table metadata:
**every commit rewrites the entire file, but only 1 entry out of N is new.**
After 10 commits, 91% of the manifest list write is redundant; after 100
commits, 99%.

Inlining manifest list changes into catalog intention records is feasible
and follows naturally from the approach in INLINE_INTENTION.md:

- **Two delta operations** (`ADD_MANIFEST`, `REMOVE_MANIFEST`) cover all
  commit types
- The dominant case (FastAppend) is a single `ADD_MANIFEST` of ~80--220 bytes
- Carried-forward manifests cost **zero bytes** per commit (implicit in state)
- The checkpoint stores the full manifest list, same total size as the
  current Avro file
- Combined with table metadata inlining, a data commit reduces from
  ~100 KB across 3 writes to **~250 bytes in 1 write**

The catalog absorbs two levels of Iceberg state (metadata + manifest list),
leaving only manifest files and data files as external objects.
