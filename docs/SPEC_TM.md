# Inline Table Metadata (TM)

This document specifies the inline-table-metadata extension to
`ProtoCatalogFormat`: how tables can store their `TableMetadata` directly in
the catalog instead of in external `metadata.json` files. See
[SPEC.md](SPEC.md) for the base format and [SPEC_ML.md](SPEC_ML.md) for the
manifest-list extension that builds on this one.

## Why

Today, every table update is a two-write operation: write a full
`v<N>.metadata.json` to storage, then atomically commit an
`UPDATE_TABLE_LOCATION` action pointing to that file. A typical data commit
adds ~700 bytes of new information but rewrites the full metadata file
every time. After 10 commits, ~92% of every write is redundant.

Inlining table metadata changes into the catalog intention record:

- reduces a commit to a single atomic write;
- eliminates orphaned metadata files on commit failure;
- shrinks per-commit payload from 2–700 KB (full metadata rewrite) to
  ~80–170 bytes (delta mode for a typical data commit);
- lets the catalog serve table metadata directly (no external read).

## Storage Model

A table is either **pointer** (external metadata file) or **inline**
(metadata in catalog). The `Checkpoint` message stores pointer tables in
`tables` (field 11) and inline tables in `inline_tables` (field 13). A
table ID appears in exactly one. Pointer and inline tables can coexist in
the same catalog; individual tables can transition between modes.

### `InlineTable` Message

```protobuf
message InlineTable {
  int32  id                   = 1;
  int32  version              = 2;
  int32  namespace_id         = 3;
  string name                 = 4;
  bytes  metadata             = 5;  // codec-encoded TableMetadata; codec
                                    //   identified by `metadata_codec`
  string manifest_list_prefix = 6;  // shared manifest-list path prefix
  repeated ManifestFileEntry     manifest_pool  = 7;  // see SPEC_ML.md
  repeated SnapshotManifestRefs  snapshot_refs  = 8;  // see SPEC_ML.md
  int32  metadata_codec       = 9;  // codec tag (see "Inline-TM codecs")
}
```

`metadata` is opaque bytes produced by an `InlineMetadataCodec`
(`structural` by default, `gzip` for diagnostic / portable use; see
"Inline-TM codecs" below). The codec is identified on the wire by
`metadata_codec`, so different `InlineTable` records and different
`UpdateTableInline` actions can carry different codecs within the same
catalog file. Storing the bytes opaque keeps the catalog codec decoupled
from `TableMetadataParser`.

Fields 7–8 belong to the manifest-list extension (see SPEC_ML.md).
Field 6, `manifest_list_prefix`, is the per-table dictionary entry that
lets `AddSnapshot` deltas carry only the variable suffix of each
snapshot's manifest-list path.

### Inline-TM codecs

Inline `TableMetadata` bytes pass through one of two codecs. Decode is
always driven by the per-record codec tag on the wire — the writer's
configured codec only affects newly-written records.

| Codec | Tag | When used |
|-------|-----|-----------|
| `structural` | `0x02` | Default. Strips the snapshots and metadata-log arrays out of the canonical JSON envelope and re-encodes them as a binary-structural columnar block (RLE schema ids, templated manifest-list paths, per-snapshot dictionary summaries). 40% smaller than gzip-JSON at the corpus median; 4–7× faster on lazy-decode workloads. |
| `gzip`       | `0x01` | Iceberg-style `Codec.GZIP` JSON. Diagnostic / portable fallback. Compatible with any tool that knows how to read `metadata.json.gz`. |

The codec selection is recorded in `InlineTable.metadata_codec` and in
the codec field of each `CreateTableInline` / `UpdateTableInline` action.
Both codecs preserve the same `TableMetadata` semantics; the on-disk
shape differs only in how the snapshots and metadata-log are serialized.

**Sentinel handling for inline manifest lists.** Both codecs serialize a
snapshot's `manifest_list` path as written — if the snapshot's
`manifestListLocation()` is the catalog sentinel
`inline://<snapshotId>`, that string is what lands on the wire.
`InlineSnapshot` (whose `manifestListLocation()` returns `null`) must be
rewritten to a `BaseSnapshot` carrying the sentinel before encode —
see "Commit Protocol" below. This keeps both codecs codec-uniform on
the wire and matches what `wrapInlineManifests` expects on read.

## Delivery Modes

When committing an update to an inline table,
`FileIOTableOperations.doCommit()` picks one of three modes based on the
resulting transaction size:

| Mode        | Payload                            | When used                                   |
|-------------|------------------------------------|---------------------------------------------|
| **Delta**   | Structured `TableMetadataDelta`    | Common path for data commits and DDL        |
| **Full**    | Complete `TableMetadata` JSON      | Initial creation, re-inlining from pointer  |
| **Pointer** | Metadata file location string      | Fallback when inline exceeds size limits    |

A single transaction may contain actions in different modes for different
tables.

`InlineDeltaCodec.selectMode(delta, newMeta, currentTxnSize)` picks the
mode given the current transaction size and a ~4 MiB budget
(`APPEND_LIMIT = 4 MiB - 4096`, leaving a safety margin for framing).

### Action Messages

```protobuf
message CreateTableInline {
  int32  id                = 1;   // -1 for late-bind
  int32  version           = 2;
  int32  namespace_id      = 3;
  int32  namespace_version = 4;   // -1 if late-bound
  string name              = 5;
  bytes  metadata          = 6;   // codec-encoded full TableMetadata
  int32  metadata_codec    = 7;   // codec tag (omitted when 0x01 / gzip)
  bytes  delta             = 8;   // OPTIONAL: AddManifestDelta entries
                                  //   for inline-ML create-with-snapshots
                                  //   (see SPEC_ML.md)
}

message UpdateTableInline {
  int32 id                  = 1;
  int32 version             = 2;  // must match current
  int32 full_metadata_codec = 6;  // codec tag for `full_metadata` payload
                                  //   (omitted when 0x01 / gzip)

  oneof payload {
    TableMetadataDelta delta             = 3;  // structured changes
    bytes              full_metadata     = 4;  // codec-encoded TableMetadata
    string             metadata_location = 5;  // pointer fallback
  }
}
```

`CreateTableInline` verifies the same way as `CreateTable` (namespace
version check, late-bind support). `UpdateTableInline` verifies on the
table version, like `UpdateTableLocation`.

`CreateTableInline.delta` carries an encoded `TableMetadataDelta` whose
updates are exclusively `AddManifestDelta` (one entry per manifest in
each freshly-created snapshot's manifest list). It is set when the
create transaction is inline-ML *and* at least one snapshot lands as
part of the create (e.g. `createTransaction().newFastAppend()`); when
set, the action's `apply` populates the manifest pool and per-snapshot
ref list atomically with `addInlineTable`, using the same
`InlineDeltaCodec.applyDeltaWithManifests` hook the update path uses.
Absent for plain creates and for inline-TM-without-ML.

## Delta Structure

A delta is an ordered sequence of metadata updates, each describing one
logical change to `TableMetadata`. These correspond to Iceberg's
`MetadataUpdate` types.

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
    AddManifestDelta        add_manifest               = 12;  // SPEC_ML
    RemoveManifestDelta     remove_manifest            = 13;  // SPEC_ML
    RemoveSchemas           remove_schemas             = 14;
    RemovePartitionSpecs    remove_partition_specs     = 15;
  }
}
```

### AddSnapshot

The dominant case (~99% of table updates). Uses a per-table dictionary to
avoid repeating fields whose value is derivable from catalog state.

```protobuf
message AddSnapshot {
  fixed64        snapshot_id          = 1;
  string         manifest_list_suffix = 2;   // appended to per-table prefix;
                                             // empty for inline-ML snapshots
  CompactSummary summary              = 3;   // typed summary (replaces map)
  sint64         timestamp_delta_ms   = 4;   // signed delta from lastUpdatedMs
  int32          schema_id            = 5;   // only if changed
  int64          added_rows           = 6;   // v3+
  fixed64        parent_snapshot_id   = 7;   // optional: stage-only / branch
                                             // / cherry-pick. Absent => fall
                                             // back to base.currentSnapshot()
  int64          first_row_id         = 8;   // optional: v3+ row lineage
  string         key_id               = 9;   // optional: encrypted tables
}
```

**Fields derived from catalog state (not transmitted)** when the optional
fields are absent:

| Field                | Derivation                                        |
|----------------------|---------------------------------------------------|
| `sequence_number`    | `state.lastSequenceNumber + 1`                    |
| `parent_snapshot_id` | `state.currentSnapshotId` (if field 7 absent)     |
| `timestamp_ms`       | `state.lastUpdatedMs + timestamp_delta_ms`        |
| `manifest_list`      | `table.manifestListPrefix + manifest_list_suffix` |
| `schema_id`          | `state.currentSchemaId` (if field 5 absent)       |

A typical data commit `AddSnapshot` encodes to ~80–115 bytes. Fields 7–9
are presence-encoded: emit them when the producer can't legally derive the
value (stage-only commits, non-`main` branch commits, cherry-picks, v3+
row lineage, encrypted tables).

### CompactSummary

Replaces the snapshot summary `map<string, string>` with typed fields.
Field numbers serve as an implicit key dictionary.

```protobuf
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
```

Zero-valued fields are omitted (proto3 semantics). The `extra` map captures
non-standard summary entries that engines may add.

### Other Updates

```protobuf
message SetSnapshotRef {
  string ref_name              = 1;
  int64  snapshot_id           = 2;
  string ref_type              = 3;   // "branch" or "tag"
  int32  min_snapshots_to_keep = 4;
  int64  max_snapshot_age_ms   = 5;
  int64  max_ref_age_ms        = 6;
}

message RemoveSnapshots         { repeated int64 snapshot_ids = 1; }
message AddSchema               { int32 schema_id = 1; int32 last_column_id = 2; bytes schema_json = 3; }
message SetCurrentSchema        { int32 schema_id = 1; }
message AddPartitionSpec        { int32 spec_id = 1; int32 last_partition_id = 2; bytes spec_json = 3; }
message SetDefaultPartitionSpec { int32 spec_id = 1; }
message AddSortOrder            { int32 order_id = 1; repeated SortField fields = 2; }
message SortField {
  int32 source_id            = 1;
  TransformKind kind         = 2;   // IDENTITY | YEAR | MONTH | DAY | HOUR | BUCKET | TRUNCATE | VOID
  int32 transform_param      = 3;   // bucket N or truncate N; omitted for non-parameterized
  SortDirection direction    = 4;   // ASC | DESC
  NullOrderKind null_order   = 5;   // NULLS_FIRST | NULLS_LAST
}
// Each enum carries an explicit *_UNSPECIFIED = 0 sentinel so the decoder
// can distinguish "missing on wire" from "valid value". See catalog.proto
// for the full enum definitions.
message SetDefaultSortOrder     { int32 order_id = 1; }
message SetTableProperties      { map<string,string> updated = 1; repeated string removed = 2; }
message SetTableLocation        { string location = 1; }
message RemoveSchemas           { repeated int32 schema_ids = 1; }
message RemovePartitionSpecs    { repeated int32 spec_ids   = 1; }
```

Schemas and partition specs are serialized as gzip-wrapped JSON via
Iceberg's existing parsers (`SchemaParser`, `PartitionSpecParser`). The
`schema_json` and `spec_json` byte fields hold the gzip stream directly;
the decoder gunzips before handing the bytes to the parser. Gzip is
applied unconditionally — for some payloads it is roughly a wash, but
always compressing puts a ceiling on the worst case (a wide nested
schema) which would otherwise risk pushing the append log past
`max.append.size` and triggering compaction earlier than snapshot
volume alone warrants. Future revisits to a typed-protobuf encoding
for `Schema` and `PartitionSpec` are tracked in [errata.md §D8](errata.md).

Sort orders are encoded structurally. The `SortField` grammar is closed
(8 transform kinds: identity, year, month, day, hour, bucket[N],
truncate[N], void; two directions; two null orders); the encoder fails
loudly if a `SortField`'s `transform.toString()` doesn't match the
grammar, so silent data loss on grammar evolution is impossible. The
1-based wire values for `kind` / `direction` / `null_order` (rather
than proto3 zero-defaults) let the decoder distinguish "missing" from
"valid value" and reject incomplete messages. The apply path
synthesizes the canonical Iceberg JSON form and hands it to
`SortOrderParser` — equivalent to the legacy gzip(JSON) path, but
without an intermediate JSON blob on disk. A round-trip property test
exercises every transform kind to catch upstream Iceberg adding new
fields to `SortField`.

`RemoveSchemas` / `RemovePartitionSpecs` are emitted by `computeDelta`
when `expireSnapshots().cleanExpiredMetadata(true)` (or a replace
transaction) prunes ids that no retained snapshot references. Without
explicit removes, the replay would carry every old schema/spec id forward
through `Builder.buildFrom(base)` and the loaded table would still expose
them. The `applyTo` for both routes through
`MetadataUpdate.RemoveSchemas` / `MetadataUpdate.RemovePartitionSpecs`
because `TableMetadata.Builder.removeSchemas` / `removeSpecs` are
package-private.

**Update ordering inside a delta:** `Add → SetCurrent / SetDefault → Remove`
for both schemas and partition specs. The Builder rejects removing the
current schema id or default spec id, so a replace transaction that
swaps schemas or specs has to advance the `current` / `default` pointer
before pruning the prior one.

## Delta Application

`InlineDeltaCodec.applyDelta(baseMetadataJson, deltaBytes)` parses the base
`TableMetadata`, applies each update via `TableMetadata.Builder`, and
re-serializes. `AddSnapshot` reconstructs derived fields (sequence number,
parent snapshot when field 7 is absent, timestamp, manifest-list path)
from the base state.

`InlineDeltaCodec.computeDelta(oldMeta, newMeta, manifestListPrefix)` diffs
two `TableMetadata` instances and produces a minimal list of `DeltaUpdate`
objects covering 13 update types (new snapshots, removed snapshots, ref
changes, schema add/remove, partition spec add/remove, sort orders,
property changes, location changes).

### Replay determinism

The replay path (`applyDelta` / `applyDeltaWithManifests`) must produce
**byte-stable** output for repeated replays of the same inputs. Two
properties make this work:

1. **Synthetic base location.** `TableMetadataParser.fromJson(json)` (the
   no-location overload) leaves `metadataFileLocation = null`, which makes
   `Builder.buildFrom(base).discardChanges().build()` skip
   `addPreviousFile` and drop the would-be metadata-log entry for the
   prior version. Both replay entry points pass a deterministic synthetic
   location of the form `inline://#<hash-of-base-bytes>` so the new TM's
   `previousFiles()` history accumulates correctly. The exact format isn't
   load-bearing — metadata-log entries are opaque strings, never resolved
   as filesystem paths in inline mode — only determinism is.

2. **`lastUpdatedMillis` pin BEFORE `applyTo`.** `Builder.lastUpdatedMillis`
   starts null and `Builder.build()` defaults to
   `System.currentTimeMillis()` when unset. Several updates (notably
   `SetSnapshotRefUpdate` for the `main` branch, which calls
   `Builder.setRef`) read the field synchronously to stamp a new
   `SnapshotLogEntry`, so a null reads as wall-clock time. Both replay
   loops set `tmBuilder.setLastUpdatedMillis(current.lastUpdatedMillis())`
   before calling `update.applyTo(tmBuilder)`. `AddSnapshotUpdate` is
   exempt: its `applyTo` calls `addSnapshot(snapshot)` which itself pins
   `lastUpdatedMillis` to the snapshot's deterministic timestamp.

Without these two properties, replay produces different bytes on each
read, the synthetic `metadataFileLocation` (Arrays.hashCode of the bytes)
flips, and BaseTransaction's reference-equality refresh check
(`base != underlyingOps.refresh()`) trips on every `commitTransaction`.

### Concurrent-replace id collisions

Schema, partition-spec, and sort-order ids are usually unique per
content, but two racing replace transactions starting from the same base
can independently assign the same id (e.g. `sortOrderId=1`) to different
content. Delta encoding can't express an in-place id swap — Builder's
`addSortOrderInternal` short-circuits on existing ids, and there's no
public `removeSortOrders` / `replaceSortOrder`. `computeDelta` detects
this at the top of the diff and returns `null`, which `selectMode`
routes to **full mode**. Full mode serializes the new TM verbatim, so
the collision resolves naturally on the next read.

For an **inline-ML** table this routing has one extra constraint: if
the producer staged a non-empty manifest-list delta against a live
snapshot in the same commit (so the new ML data must be preserved),
`commitInline` / `commitTransaction` instead surface
`CommitFailedException`. Full mode would serialize `TableMetadata`
without visiting the per-snapshot manifest pool, and pointer-mode
eviction would clear it; both silently drop the staged ML data. The
exception triggers the producer's retry loop, which refreshes the
base and re-stages on top of the rebased state — the colliding id is
reassigned naturally on the next attempt (Builder picks the next
free id).

## Configuration

| Property                          | Default       | Description |
|-----------------------------------|---------------|-------------|
| `fileio.catalog.inline`           | `false`       | Enable inline TM. When true, `doCommit()` uses delta/full/pointer mode selection instead of writing external metadata files. |
| `fileio.catalog.inline.tm.codec`  | `structural`  | Codec for newly-written inline `TableMetadata` bytes. `structural` (binary, columnar) or `gzip` (gzipped JSON). Decode is always driven by the on-wire codec tag, so changing this setting affects only new writes. |

`fileio.catalog.max.append.count` and `fileio.catalog.max.append.size`
(see [SPEC.md](SPEC.md)) still apply.

## Commit Protocol

```
function doCommit(base, newMetadata):
    codec = configuredInlineCodec()                    // structural or gzip
    if isCreate:
        # Inline-ML create-with-snapshots: rewrite InlineSnapshots to
        # BaseSnapshot+sentinel and bundle the manifest pool with the
        # create action.
        delta = null
        toEncode = newMetadata
        if isInlineML() and hasStagedDeltas():
            staged   = drainStagedDeltas()
            delta    = encodeDelta(buildAddManifestUpdates(staged))
            toEncode = rewriteInlineSnapshotsToSentinels(newMetadata)
        bytes = codec.encode(toEncode)
        format.from(lastCatalogFile)
            .createTableInlineWithManifests(tableId, bytes, codec.tag(), delta)
            .commit(io)
        return

    prefix = lastCatalogFile.manifestListPrefix(tableId)
    delta  = InlineDeltaCodec.computeDelta(base, newMetadata, prefix)
    attachStagedManifestDeltas(delta, newMetadata.snapshots())   // SPEC_ML
    mode   = InlineDeltaCodec.selectMode(delta, newMetadata, 0, codec)

    mut = format.from(lastCatalogFile)
    switch mode:
        case "delta":
            mut.updateTableInlineDelta(tableId, encodeDelta(delta))
        case "full":
            mut.updateTableInline(tableId, codec.encode(newMetadata), codec.tag())
        case "pointer":
            loc = writeMetadataFile(newMetadata)
            mut.updateTable(tableId, loc)
    mut.commit(io)
```

The `delta` payload on `createTableInlineWithManifests` is `null` for
plain (non-ML) creates; the format reverts to the single-action
`createTableInline` shape (the `delta` field is simply omitted from the
encoded action — see "Action Messages" above). The
`rewriteInlineSnapshotsToSentinels` step uses
`TableMetadata.Builder.replaceSnapshots` — the same hook
`wrapInlineManifests` uses on the read side — to swap each
`InlineSnapshot` (whose `manifestListLocation()` is null) for a
`BaseSnapshot` carrying `inline://<snapshotId>`. This makes the encoded
TM uniform across both codecs: both the `gzip` and `structural` write
paths see a snapshot with a non-null sentinel `manifest-list` and emit
it verbatim, rather than falling into Iceberg's v1-embedded-manifests
branch (which would lose per-manifest `partitionSpecId` on decode).

## Table Loading

`FileIOTableOperations.loadFromCatalogFile()` detects inline tables via
`CatalogFile.isInlineTable()` and calls
`BaseMetastoreTableOperations.refreshFromMetadataLocation(syntheticLoc, null, 0, customLoader)`
with a `Function<String, TableMetadata>` that parses from the catalog's
inline bytes.

The synthetic location is `inline://<tableIdentifier>#<hash-of-bytes>`,
where the hash is `Integer.toHexString(Arrays.hashCode(inlineMeta))`.
The hash form is required for the
`BaseMetastoreTableOperations.refreshFromMetadataLocation` cache check:
when the inline bytes haven't changed, the synthetic location matches
`currentMetadataLocation`, refresh short-circuits, and the cached
`TableMetadata` instance survives. A wall-clock-based suffix would force
a reload on every refresh and break BaseTransaction's reference-equality
check (`base != underlyingOps.refresh()`). The synthetic location never
reaches `FileIO`. **No changes to `BaseMetastoreCatalog` or
`BaseMetastoreTableOperations` are required** — the existing protected
API supports this integration cleanly.

## Rename

`Catalog.renameTable(from, to)` is a single in-place update of the
`TblEntry`'s `(namespaceId, name)` fields. The `int` table id stays put,
so every id-keyed map (`tblInlineMetadata`, `tblManifestPrefix`,
`manifestPool`, `snapshotManifests`) follows by construction — no inline
state is copied across, and no migration can leak it.

Implementation: `FileIOCatalog.renameTable` calls
`format.from(catalogFile).renameTable(from, to).commit(io)`. Pointer-mode
and inline (TM, TM+ML) tables all take the same path. The wire action is
`RenameTable(id, version, new_namespace_id, new_namespace_version, new_name)`;
on apply it bumps the source and destination namespaces' children-set
versions, exactly like `DropTable` + `CreateTable` would have. Conflict
semantics fall out of the design.md operation × operation matrix without
new fields — see entry **RT'** there.

## Inline ↔ Pointer Transitions

### Inline → Pointer (Eviction)

Triggered when an update would push the transaction past `APPEND_LIMIT`,
or when compaction would push the checkpoint past
`fileio.catalog.max.append.size`. On eviction:

1. Serialize current `TableMetadata` to a `v<N>.metadata.json` in the
   table's metadata directory.
2. Record the file location in an `UpdateTableInline` with the `metadata_location`
   payload.
3. On apply, the table moves out of `inline_tables` and back into `tables`.

### Pointer → Inline (Re-inlining)

- During compaction: the compactor reads external metadata.json files and
  inlines them in the new checkpoint.
- On the next table update: the writer can choose to inline the new
  metadata via full mode.

### Mixed-Mode Catalogs

A catalog can contain both inline and pointer tables. The state model
supports this natively: separate maps for `tableById` (with metadata
location) and `tblInlineMetadata` (with parsed JSON). This enables gradual
migration and per-table size-based decisions.

## Size Budget

Azure ADLS conditional append (`AppendBlock`) has a hard limit of **4 MiB**
per append operation. GCS compose and S3 multipart-upload limits are much
larger (5 GB), so Azure is the binding constraint.

```
Azure append limit:                     4,194,304 bytes
Safety margin (framing, varint, etc.):     -4,096 bytes
                                       ──────────────────
APPEND_LIMIT (per transaction):         4,190,208 bytes (~4.0 MiB)
```

Approximate sizes per update:

| Scenario                       | Delta size     | Fits in 4 MiB?    |
|--------------------------------|----------------|-------------------|
| Single data commit (1 table)   | 80–170 B       | yes               |
| 10-table multi-table txn       | 0.8–1.7 KB     | yes               |
| Schema evolution (wide table)  | 10–20 KB       | yes               |
| Full metadata (1000 snapshots) | ~700 KB        | yes (full mode)   |
| Full metadata (6000+ snapshots)| >4 MB          | no → pointer mode |

The delta path handles >99.9% of updates.

## Compatibility

Protobuf's forward-compatibility guarantees apply: old readers that do not
recognize fields 9, 10, or 13 will silently skip them. This means an old
reader will see inline tables as missing (not in `tables`, not in
`inline_tables`). Deployment requires upgrading readers before writers.

## Verification Rules

| Action                | Precondition                                              |
|-----------------------|-----------------------------------------------------------|
| `CreateTableInline`   | Namespace version matches (skip if late-bound)            |
| `UpdateTableInline`   | Table version matches                                     |

`UpdateTableInline` subsumes `UpdateTableLocation` for inline tables: a
version mismatch means a concurrent writer modified the table; the
transaction is rejected and retried with a refreshed catalog state.
