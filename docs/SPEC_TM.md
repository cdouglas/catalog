# Inline Table Metadata (TM)

This document specifies the inline-table-metadata extension to the catalog
format: how tables can store their `TableMetadata` directly in
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
- shrinks per-commit payload from 2–700 KB (full metadata rewrite) to a
  few hundred bytes (delta mode: a data commit's log record measures
  ~0.35–0.39 KB in [sample/](sample/README.md));
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
  string manifest_list_prefix = 6;  // per-table path dictionary; always ""
  repeated ManifestFileEntry     manifest_pool  = 7;  // see SPEC_ML.md
  repeated SnapshotManifestRefs  snapshot_refs  = 8;  // see SPEC_ML.md
  InlineMetadataCodec metadata_codec = 9;  // required, 0 included (see
                                           //   "Inline-TM codecs")
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
Field 6, `manifest_list_prefix`, is a per-table dictionary entry
reserved for a path prefix that `AddSnapshot.manifest_list_suffix` and
`ManifestFileEntry.manifest_path_suffix` are relative to. Nothing sets
one: a create and a table that returns to inline storage get the empty
prefix, and a checkpoint copies it, so every suffix is a full path.

### Inline-TM codecs

Inline `TableMetadata` bytes pass through one of two codecs. Decode is
always driven by the per-record codec tag on the wire — the writer's
configured codec only affects newly-written records.

| Codec | Tag | When used |
|-------|-----|-----------|
| `structural` | `1` (`CODEC_STRUCTURAL`) | Default. Strips the snapshots and metadata-log arrays out of the canonical JSON envelope and re-encodes them as a binary-structural columnar block (RLE schema ids, templated manifest-list paths, per-snapshot dictionary summaries). 40% smaller than gzip-JSON at the corpus median; 4–7× faster on lazy-decode workloads. |
| `gzip`       | `0` (`CODEC_JSON_GZIP`) | Iceberg-style `Codec.GZIP` JSON. Diagnostic / portable fallback. Compatible with any tool that knows how to read `metadata.json.gz`. |

The codec selection is recorded in `InlineTable.metadata_codec` and in
the codec field of each `CreateTableInline` / full-mode
`UpdateTableInline` action. The tag is written on every such record,
gzip's 0 included, and the decoder rejects a record without it.
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

When an inline catalog commits a table update,
`FileIOTableOperations.stageUpdate()` takes the first of three modes
whose payload fits in `fileio.catalog.inline.max.update.size` (default
`InlineDeltaCodec.APPEND_LIMIT` = 4 MiB - 4096, leaving a safety margin
for framing):

| Mode        | Payload                            | When used                                   |
|-------------|------------------------------------|---------------------------------------------|
| **Delta**   | Structured `TableMetadataDelta`    | The table is inline (common path for data commits and DDL) |
| **Full**    | Complete encoded `TableMetadata`   | No delta: the table is a pointer table (re-inlining), the delta does not fit, or `computeDelta` returns none: it finds no update, or cannot express the change (a format-version upgrade, a removed ref, a change to the encryption keys, a statistics change, an id collision, a new schema or spec that replay would number otherwise, a new sort order or spec that does not bind to the commit's schema, a last column or partition id other than the larger of the base's and the highest field id in the commit's new schemas or specs, new snapshot-log entries other than replay's: none if main did not move, else one, last, for main's new snapshot at the snapshot's own timestamp if the commit added it and at last-updated-ms otherwise (a rollback), new snapshots whose sequence numbers or row ids do not follow the base's) |
| **Pointer** | Metadata file location (`UpdateTableLocation`) | Neither fits, or a new inline-ML snapshot restates a pooled manifest (see [SPEC_ML.md](SPEC_ML.md)): the table leaves inline storage (eviction) |

Manifest updates of new inline-ML snapshots count toward the payload
(see [SPEC_ML.md](SPEC_ML.md)). A create is full mode or pointer mode by
the same rules. A single transaction may contain actions in different
modes for different tables. A delta that fits keeps a table inline
however large its full metadata grows; only the update's size is
bounded. A delta keeps the table's codec: replay re-encodes with the
base record's, so a table changes codec only with a full-mode update.

### Action Messages

```protobuf
message CreateTableInline {
  int32  id                = 1;   // late-bound (negative)
  int32  version           = 2;   // the new table's version (1)
  int32  namespace_id      = 3;   // negative if created in this transaction
  int32  namespace_version = 4;   // must match; -1 if late-bound
  string name              = 5;
  bytes  metadata          = 6;   // codec-encoded full TableMetadata
  InlineMetadataCodec metadata_codec = 7;  // required, 0 (gzip) included
  TableMetadataDelta  delta          = 8;  // optional: manifest updates of
                                           //   the create's inline-ML
                                           //   snapshots (see below)
}

message UpdateTableInline {
  int32 id                  = 1;
  int32 version             = 2;  // must match current

  oneof payload {
    TableMetadataDelta delta             = 3;  // structured changes
    bytes              full_metadata     = 4;  // codec-encoded TableMetadata
    string             metadata_location = 5;  // pointer fallback (writers
                                                   //   evict with UpdateTableLocation)
  }
  InlineMetadataCodec full_metadata_codec = 6;  // required with full_metadata,
                                                //   0 (gzip) included; absent otherwise
  TableMetadataDelta  ml_delta            = 7;  // optional, with full_metadata
                                                //   only: manifest updates
                                                //   (see SPEC_ML.md)
}
```

`CreateTableInline` verifies the same way as `CreateTable` (namespace
version check, late-bind support). `UpdateTableInline` verifies on the
table version, like `UpdateTableLocation`.

`CreateTableInline.delta` carries the manifest updates of the inline-ML
snapshots a create transaction commits with the table (e.g.
`createTransaction().newFastAppend()`), built as a full-mode update's
are (`manifestUpdates(..., full=true)` in `FileIOTableOperations.stageCreate`):
the table's first snapshot sends its whole list, and a later snapshot a
delta against its parent's, so the payload may hold `RemoveManifestDelta`
updates too, or none (snapshots that add no manifests). The action's
`apply` lands it with `addInlineTable` through
`InlineDeltaCodec.applyManifestsOnly`, the hook a full-mode update uses,
which also registers each inline snapshot of the new metadata that has
no list yet. Absent for a create without inline-ML snapshots.

## Delta Structure

A delta is an ordered sequence of metadata updates, each describing one
logical change to `TableMetadata`. These correspond to Iceberg's
`MetadataUpdate` types.

```protobuf
message TableMetadataDelta {
  repeated TableMetadataUpdate updates = 1;
  int64 last_updated_ms = 2;   // required: the committed last-updated-ms,
                               //   which replay sets
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

The dominant case (~99% of table updates). Fields whose value replay
derives from the base metadata are not transmitted.

```protobuf
message AddSnapshot {
  fixed64        snapshot_id          = 1;
  string         manifest_list_suffix = 2;   // path less the per-table prefix
                                             // (always "": the full path);
                                             // empty for inline-ML snapshots
  CompactSummary summary              = 3;   // typed summary (replaces map)
  sint64         timestamp_delta_ms   = 4;   // signed delta from the base's
                                             // last-updated-ms
  int32          schema_id            = 5;   // present (0 included) unless it is
                                             // the base's current schema
  int64          added_rows           = 6;   // v3+: present, 0 included
  fixed64        parent_snapshot_id   = 7;   // the parent; absent: no parent
  int64          first_row_id         = 8;   // present whenever the snapshot
                                             // has one, 0 included
  string         key_id               = 9;   // optional: encrypted tables
}
```

**Fields derived from the base metadata (not transmitted)**, for the
delta's k-th new snapshot (k = 1, 2, ...):

| Field                | Derivation                                        |
|----------------------|---------------------------------------------------|
| `sequence_number`    | `base.lastSequenceNumber + k` (0 for v1)          |
| `timestamp_ms`       | `base.lastUpdatedMs + timestamp_delta_ms`         |
| `manifest_list`      | `table.manifestListPrefix + manifest_list_suffix`; `inline://<snapshotId>` if the suffix is empty |
| `schema_id`          | `base.currentSchemaId` (if field 5 absent)        |

`base` is the metadata the delta was computed against, the same for every
snapshot of the delta: adding a snapshot moves last-updated-ms, so
resolving a later snapshot's time against the metadata as the earlier
updates left it would get it wrong. A commit whose new snapshots are
numbered otherwise, or whose next row id is not the base's advanced by
their added rows, goes in full.

`parent_snapshot_id` is sent whenever the snapshot has a parent; absent
means it has none (a table's first snapshot, a replace's). Replay never
takes `base.currentSnapshot()`, which is wrong for staged, branch and
cherry-picked snapshots and for replaces. `added_rows` is present for
every v3+ snapshot and `first_row_id` whenever the snapshot has one, 0
included; `key_id` only for encrypted tables.

Most of a data commit's `AddSnapshot` is its summary (Iceberg puts
`iceberg-version` in every summary, and engines add their own entries,
all in `extra`) and, for a snapshot with a manifest list file, that
file's full path; the whole log record of a data commit measures
~0.35–0.39 KB in [sample/](sample/README.md).

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

A count is present whenever the summary has it, 0 included: an absent count
is one the summary lacks, which readers take as unknown. `operation` is
omitted for `APPEND`, the default. The `extra` map captures the other entries
engines add, and any entry the typed fields cannot hold exactly (an operation
not in `OperationType`, a count not written as a plain decimal number).

### Other Updates

```protobuf
message SetSnapshotRef {
  string  ref_name              = 1;
  fixed64 snapshot_id           = 2;
  string  ref_type              = 3;   // "branch" or "tag"
  int32   min_snapshots_to_keep = 4;   // retention fields: absent when unset
  int64   max_snapshot_age_ms   = 5;
  int64   max_ref_age_ms        = 6;
}

message RemoveSnapshots         { repeated int64 snapshot_ids = 1 [packed = false]; }
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
message RemoveSchemas           { repeated int32 schema_ids = 1 [packed = false]; }
message RemovePartitionSpecs    { repeated int32 spec_ids   = 1 [packed = false]; }
```

Replay ignores the ids `AddSchema`, `AddPartitionSpec` and `AddSortOrder`
carry (`schema_id`, `last_column_id`, `spec_id`, `last_partition_id`,
`order_id`): Iceberg's builder numbers each schema, spec and sort order
it adds (an equal one's id, else one past the highest) and raises the
last column and partition ids to the added fields'. So `computeDelta`
sends in full a commit whose new schema or spec ids, or last column or
partition ids, replay would derive otherwise (see "Delivery Modes"). The
builder also binds each sort order and spec it adds to the schema current
by then, the commit's, while Iceberg's metadata parser binds sort orders
other than the default unchecked: an order that names a column the commit
dropped goes in full too (Bug 65).

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
before pruning the prior one. Every manifest update (SPEC_ML) must follow
its snapshot's `AddSnapshot`: replay rejects one for a snapshot neither
in the base nor added earlier in the delta. Writers append the manifest
updates after all table metadata updates.

## Delta Application

An `UpdateTableInline` delta replays through
`InlineDeltaCodec.applyDeltaWithManifests(baseBytes, baseCodec, deltaBytes, catalogBuilder, tableId)`:
it decodes the base record with its codec, applies the table metadata
updates through `TableMetadata.Builder` and the manifest updates to the
table's manifest pool and snapshot lists (SPEC_ML), and re-encodes the
result with the base's codec. `AddSnapshot` reconstructs the derived
fields (sequence number, timestamp, manifest-list path, schema when field
5 is absent) from the base metadata.

`InlineDeltaCodec.computeDelta(oldMeta, newMeta, manifestListPrefix)` diffs
two `TableMetadata` instances and produces a minimal list of `DeltaUpdate`
objects covering 13 update types (new snapshots, removed snapshots, ref
changes, schema add/remove, partition spec add/remove, sort orders,
property changes, location changes). It returns null, and the commit
takes full mode, when it finds no update or cannot express the change
(see "Delivery Modes").

### Replay determinism

Replay must produce **byte-stable** output for repeated replays of the
same inputs, equal to the metadata the writer committed. Two properties
make this work:

1. **Synthetic base location.** `TableMetadataParser.fromJson(json)` (the
   no-location overload) leaves `metadataFileLocation = null`, which makes
   `Builder.buildFrom(base).discardChanges().build()` skip
   `addPreviousFile` and drop the would-be metadata-log entry for the
   prior version. Replay passes a deterministic synthetic location of
   the form `inline://#<hash-of-base-bytes>` so the new TM's
   `previousFiles()` history accumulates correctly. The exact format isn't
   load-bearing — metadata-log entries are opaque strings, never resolved
   as filesystem paths in inline mode — only determinism is.

2. **`lastUpdatedMillis` pin BEFORE `applyTo`.** `Builder.lastUpdatedMillis`
   starts null and `Builder.build()` defaults to
   `System.currentTimeMillis()` when unset, as does
   `SetSnapshotRefUpdate` for the `main` branch (`Builder.setRef`) when it
   stamps a new `SnapshotLogEntry` for a snapshot the builder did not add.
   Replay pins the writer's last-updated-ms, which the delta carries, on
   each builder it opens, before calling `update.applyTo(builder)`. The run
   of table metadata updates up to the delta's first manifest update, its
   `AddSnapshotUpdate`s included, goes to one builder: Iceberg logs
   main's move to a snapshot the same builder added at the snapshot's
   timestamp (Iceberg 1.12.0 no longer takes last-updated-ms from
   `addSnapshot`), so the snapshot and the ref that moves to it must be
   applied together, as the writer did.

Without the first, the replayed metadata log loses the prior version's
entry; without the second, each replay stamps the reader's clock, so
readers of one catalog file decode different metadata and none decodes
what the writer committed. The hash names only that metadata-log entry:
a loaded inline table's location is `inline://<table>#<id>.<version>`
(see "Table Loading").

### Concurrent-replace id collisions

Schema, partition-spec, and sort-order ids are usually unique per
content, but two racing replace transactions starting from the same base
can independently assign the same id (e.g. `sortOrderId=1`) to different
content. Delta encoding can't express an in-place id swap — Builder's
`addSortOrderInternal` short-circuits on existing ids, and there's no
public `removeSortOrders` / `replaceSortOrder`. `computeDelta` detects
this at the top of the diff and returns `null`, which the commit
routes to **full mode**. Full mode serializes the new TM verbatim, so
the collision resolves naturally on the next read.

For an **inline-ML** table, full mode carries the new snapshots' manifest-list
updates in its `ml_delta` payload, with the list of any inline snapshot
the table holds no inline list for, so the lists land atomically with
the TM replacement; pointer mode writes them to Avro files (see
[SPEC_ML.md](SPEC_ML.md) §"Writers in Different Modes").

## Configuration

| Property                          | Default       | Description |
|-----------------------------------|---------------|-------------|
| `fileio.catalog.inline`           | `false`       | Enable inline TM. When true, `doCommit()` uses delta/full/pointer mode selection instead of writing external metadata files. |
| `fileio.catalog.inline.tm.codec`  | `structural`  | Codec for newly-written inline `TableMetadata` bytes. `structural` (binary, columnar) or `gzip` (gzipped JSON). Decode is always driven by the on-wire codec tag, so changing this setting affects only new writes. |
| `fileio.catalog.inline.max.update.size` | 4,190,208 | Largest inline table update in bytes (a delta, or full metadata with its manifest updates). A larger update writes a metadata file. See "Delivery Modes". |

`fileio.catalog.max.append.count` and `fileio.catalog.max.append.size`
(see [SPEC.md](SPEC.md)) still apply.

## Commit Protocol

```
function stageCreate(mut, metadata):                   # base == null
    if inline:
        manifests = manifestUpdates(null, metadata, full=true)  # null: no inline-ML snapshot
        bytes = codec.encode(rewriteInlineSnapshotsForFullEncode(metadata))
        if size(bytes, manifests) <= maxUpdateSize
                and not restatesAManifest(null, metadata, full=true):
            mut.createTableInlineWithManifests(tableId, bytes, codec.tag(), manifests)
            return
    mut.createTable(tableId, writeUpdateMetadata(metadata))

function stageUpdate(mut, base, metadata):
    if inline:
        manifests = manifestUpdates(base, metadata, full=false)  # new snapshots' lists; SPEC_ML
        if manifests != null and restatesAManifest(base, metadata, full=false):
            mut.updateTable(tableId, writeUpdateMetadata(metadata))  # first snapshot after a v3 upgrade
            return
        if lastRead.state().isInlineTable(tableId):
            delta = computeDelta(base, metadata, prefix)   # null: inexpressible or empty
            if delta != null and size(delta + manifests) <= maxUpdateSize:
                mut.updateTableInlineDelta(tableId, encodeDelta(delta + manifests, metadata.lastUpdatedMs))
                return
        manifests = manifestUpdates(base, metadata, full=true)   # also lists the table lacks; SPEC_ML
        if manifests != null and restatesAManifest(base, metadata, full=true):
            mut.updateTable(tableId, writeUpdateMetadata(metadata))
            return
        full = codec.encode(rewriteInlineSnapshotsForFullEncode(metadata))
        if size(full, manifests) <= maxUpdateSize:
            mut.updateTableInline(tableId, full, codec.tag(), manifests)
            return
    # evict, or a writer that does not inline
    mut.updateTable(tableId, writeUpdateMetadata(metadata))

function writeUpdateMetadata(metadata):
    # A metadata file names manifest list files only (SPEC_ML)
    for each InlineSnapshot s in metadata:
        replace s with a BaseSnapshot naming writeManifestList(s.allManifests)
    return writeNewMetadataFile(metadata)
```

`doCommit` and the multi-table `commitTransaction` both stage through
these, then commit the catalog file once.

The `delta` payload on `createTableInlineWithManifests` is `null` for a
create without inline-ML snapshots; the format reverts to the
single-action `createTableInline` shape (the `delta` field is simply
omitted from the encoded action — see "Action Messages" above). The
`rewriteInlineSnapshotsForFullEncode` step uses
`TableMetadata.Builder.replaceSnapshots` — the same hook
`wrapInlineManifests` uses on the read side — to swap each
`InlineSnapshot` (whose `manifestListLocation()` is null) for a
`BaseSnapshot` carrying `inline://<snapshotId>`. This makes the encoded
TM uniform across both codecs: both the `gzip` and `structural` write
paths see a snapshot with a non-null sentinel `manifest-list` and emit
it verbatim, rather than falling into Iceberg's v1-embedded-manifests
branch (which would lose per-manifest `partitionSpecId` on decode).

## Table Loading

`FileIOTableOperations.loadFrom(CatalogState)` detects inline tables via
`CatalogState.isInlineTable()` and calls
`BaseMetastoreTableOperations.refreshFromMetadataLocation(syntheticLoc, null, 0, customLoader)`
with a `Function<String, TableMetadata>` that parses from the catalog's
inline bytes.

The synthetic location is `inline://<tableIdentifier>#<tableId>.<version>`
(`FileIOCatalog.metadataLocation`): the table version bumps on every
commit, and the table id tells apart tables that held the name in turn.
The `BaseMetastoreTableOperations.refreshFromMetadataLocation` cache
check relies on it: when the table hasn't changed, the synthetic
location matches `currentMetadataLocation`, refresh short-circuits, and
the cached `TableMetadata` instance survives. A wall-clock-based suffix would force
a reload on every refresh and break BaseTransaction's reference-equality
check (`base != underlyingOps.refresh()`). The synthetic location reaches
`FileIO` only from Iceberg's metadata tables (snapshots, history, refs,
partitions, manifests, ...), which take the length of the table's metadata
file: a table's FileIO (`InlineMetadataFileIO`) reads its current synthetic
location as a file of the stored metadata's length, which cannot be opened,
and is the catalog's FileIO otherwise. **No changes to `BaseMetastoreCatalog` or
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

Triggered when an update fits in `fileio.catalog.inline.max.update.size`
neither as a delta nor in full, or restates a pooled manifest (SPEC_ML).
On eviction:

1. Write each inline-ML snapshot's manifest list to an Avro file
   (`snap-<snapshotId>-<uuid>.avro`) and point the snapshot at it.
2. Serialize the resulting `TableMetadata` to a metadata file in the
   table's metadata directory.
3. Record the file location in an `UpdateTableLocation` action. On
   apply, the table's inline state (metadata, manifest pool, snapshot
   refs) is cleared: the table is a pointer table.

A pointer writer's commit to an inline table is the same record and
has the same effect. The table version check makes the step atomic
with the commit it carries.

### Pointer → Inline (Re-inlining)

An inline writer's next update to a pointer table is full mode if the
full metadata (with any manifest updates) fits; otherwise the table
stays a pointer table and the update writes another metadata file. The
snapshots that have manifest list files keep them. Compaction does not
re-inline tables.

### Mixed-Mode Catalogs

A catalog can contain both inline and pointer tables. The state model
supports this natively: separate maps for `tableById` (with metadata
location) and `tblInlineMetadata` (with the encoded metadata). This
enables gradual migration and per-table size-based decisions.

## Size Budget

Azure ADLS conditional append (`AppendBlock`) has a hard limit of **4 MiB**
per append operation. GCS compose and S3 multipart-upload limits are much
larger (5 GB), so Azure is the binding constraint.

```
Azure append limit:                     4,194,304 bytes
Safety margin (framing, varint, etc.):     -4,096 bytes
                                       ──────────────────
APPEND_LIMIT (per table update):        4,190,208 bytes (~4.0 MiB)
```

Approximate sizes per update:

| Scenario                       | Delta size     | Fits in 4 MiB?    |
|--------------------------------|----------------|-------------------|
| Single data commit (1 table)   | ~0.35–0.39 KB (log record, [sample/](sample/README.md)) | yes |
| 10-table multi-table txn       | ~3–4 KB (estimate) | yes           |
| Schema evolution (wide table)  | 10–20 KB       | yes               |
| Full metadata (1000 snapshots) | ~700 KB        | yes (full mode)   |
| Full metadata (6000+ snapshots)| >4 MB          | no → pointer mode |

The delta path handles >99.9% of updates.

## Compatibility

The format has no deployed users and no backward-compatibility goal.
`Checkpoint.min_reader_version` gates checkpoints: a reader that supports
less than the version a checkpoint names refuses it rather than skip
fields it does not know.

## Verification Rules

| Action                | Precondition                                              |
|-----------------------|-----------------------------------------------------------|
| `CreateTableInline`   | Namespace version matches (skip if late-bound)            |
| `UpdateTableInline`   | Table version matches                                     |

`UpdateTableInline` subsumes `UpdateTableLocation` for inline tables: a
version mismatch means a concurrent writer modified the table; the
transaction is rejected and retried with a refreshed catalog state.
