# FileIOCatalog Design Notes

High-level design, invariants, and best practices for working in this
codebase. For wire-format details see [SPEC.md](SPEC.md), [SPEC_TM.md](SPEC_TM.md),
[SPEC_ML.md](SPEC_ML.md). For known shortcuts and open issues see
[errata.md](errata.md).

## What This Is

`FileIOCatalog` is an Iceberg catalog whose entire state — namespaces,
tables, optionally table metadata, optionally manifest lists — lives in a
single object in cloud storage. There is no separate metastore. All
mutation goes through cloud-storage conditional writes (`if-match` ETag,
generation number, append offset), provided by the `SupportsAtomicOperations`
extension to `FileIO`.

The catalog absorbs successive levels of Iceberg state:

| Level                 | External today | Inlined when                        |
|-----------------------|----------------|-------------------------------------|
| Catalog (this project)| —              | always                              |
| `TableMetadata` (JSON)| metadata.json  | `fileio.catalog.inline=true`        |
| Manifest list (Avro)  | snap-*.avro    | `fileio.catalog.inline.manifests=true` |
| Manifest files (Avro) | external       | (not absorbed)                      |
| Data files            | external       | (not absorbed)                      |

When fully inlined, a typical data commit is one ~250-byte atomic write to
one object instead of three writes totalling ~100 KB.

## Architecture

### Snapshot + Mutation

A read of the catalog's log (`CatalogLog.read`) is an **immutable
snapshot**: a `Replica` of the catalog's state (`CatalogState`) at the log
position the read ended at, with the log's committed set. All mutation goes
through a transaction (`CatalogMutation`) that accumulates pending changes
on it and commits them atomically:

1. Read the catalog (`CatalogLog.read`).
2. Stage a `CatalogMutation` on the read.
3. Accumulate changes (`createTable`, `dropNamespace`, etc.).
4. `commit()` appends its intention (`CatalogMutation.intention`) to the
   log as one record.
5. The commit loop (intentlog-core's `LogRuntime.commit`, over the
   catalog file's `AtomicFileLog`) handles
   what storage reports. After a CAS conflict it replays the new file,
   checks the record's preconditions again, and rebases the same record
   onto it; after a lost append race it retries the same bytes at the new
   offset; it retries a throttled write after a backoff (`write`,
   `fileio.catalog.throttle.retry.min-wait-ms`); and it settles a write
   whose outcome it did not learn by reading the catalog for the
   transaction's id, or throws `CommitStateUnknownException` (I10). A
   record whose preconditions fail goes back to the caller: the catalog
   (`LogCatalog`) stages a catalog operation again on a fresh read
   (`commitOnAFreshRead`), and Iceberg re-applies a table commit; a
   catalog transaction fails ([errata.md](errata.md) §D12). See
   [SPEC.md](SPEC.md) §"Commit Protocol".

### Two Mutation Strategies

Every commit is either:

- **Append** — append a length-prefixed transaction record to the end of
  the file, conditional on the file's current length.
- **CAS** — atomically replace the entire file with `[header][checkpoint][txn]`,
  conditional on the file's ETag/generation.

Append is fast and incremental; CAS is full-rewrite but compacts the log.
The format alternates between them based on log-size thresholds. See
[SPEC.md](SPEC.md) §"Commit Protocol".

### Codec Layout

Three codecs cooperate at write/read time:

- `ProtoCodec` handles catalog-level messages: `Checkpoint`, `Transaction`,
  `Action` oneof, all catalog/namespace/table actions.
- `InlineDeltaCodec` handles inline TM delta messages:
  `TableMetadataDelta`, all 15 `TableMetadataUpdate` types,
  `CompactSummary`. The `ManifestFileEntry` an `AddManifestDelta`
  carries is encoded by `ProtoCodec`, which also writes it in the
  checkpoint's manifest pool.
- `InlineMetadataCodec` (with two implementations,
  `StructuralInlineMetadataCodec` and `GzipInlineMetadataCodec`)
  encodes full `TableMetadata` bytes inside `InlineTable`,
  `CreateTableInline`, and `UpdateTableInline.full_metadata`. The codec
  used is identified by a per-record tag on the wire (`structural` /
  `gzip`); see [SPEC_TM.md](SPEC_TM.md) §"Inline-TM codecs". The
  writer's `fileio.catalog.inline.tm.codec` property selects the codec
  for new writes; decode always follows the wire tag, so codecs can
  mix freely within a single catalog file.

`ProtoCodec` and `InlineDeltaCodec` write protobuf by hand; nothing is
generated from `catalog.proto`. Inline TM bytes are opaque `bytes`
fields in it, in the format their codec tag names. `catalog.proto`
declares every field the codecs write, with the wire type they use, and
two tests hold it to that (516e9c4): `CatalogProto` checks every catalog
file the suites and fuzzers write against it (through
`CheckpointRoundTrip`), and `TestFormatExplorer` checks the codecs'
field-number constants against it in both directions. So a protobuf
tool given `catalog.proto` decodes the checkpoint and each log record
(past the file's header and length prefixes). The one departure from
`protoc`'s output, a negative int32 as a 5-byte varint, is one `protoc`
readers accept (see the header of `catalog.proto`).

### Component Separation

This project consumes Iceberg as a Maven SNAPSHOT artifact. The
`SupportsAtomicOperations` interface, the `ManifestListSink` extension
point, and the cloud-provider `SupportsAtomicOperations` implementations
(`S3FileIO`, `GCSFileIO`, `ADLSFileIO`) all live in the iceberg fork
(branch `atomicio`, see the [README](../README.md)). The catalog format
and the FileIO catalog itself live here.

## Invariants

These hold in the current code and must not regress.

### I1. One intention record per commit

A single `doCommit` / `commitTransaction` produces exactly one
transaction-level record per affected table, encoded into one transaction
and written to storage in one atomic operation (append or CAS). Every
configuration is subject to this rule:

- **pointer**: one `UpdateTableLocation` / `CreateTable` action referencing
  a pre-written `v<N>.metadata.json`.
- **TM inline**: one `UpdateTableInline` / `CreateTableInline` action
  carrying delta or full metadata (codec-encoded).
- **TM+ML inline (update)**: one `UpdateTableInline` action whose delta
  carries the TM updates *and* the per-snapshot `AddManifestDelta` /
  `RemoveManifestDelta` entries in the same blob.
- **TM+ML inline (create-with-snapshots)**: one `CreateTableInline`
  action whose `metadata` carries the codec-encoded TM (with sentinel
  manifest-list locations for every freshly-created snapshot) and
  whose optional `delta` field carries the matching `AddManifestDelta`
  entries that populate the pool. Apply runs the create and the
  manifest-pool fill in the same `Action.apply()` call.

A compaction or crash mid-flight must not be able to observe a half-applied
commit. ML actions are never written separately from the TM action that
introduces the snapshot they belong to.

### I2. Actions are idempotent on replay

Each action's `verify` checks preconditions (table version, namespace
version, read-set versions); each action's `apply` mutates state so a
replay of the same record against the post-apply state fails `verify`.
This is the second line of defence alongside `committedTxn` deduplication
— both must hold.

`apply()` bumps the version of any entity whose state it mutates: the
table on a table update or rename, the namespace itself on
`SetNamespaceProperty` and `RemoveNamespaceProperty`, and the *parent
namespace* on actions that change its children set (`CreateNamespace`,
`DropNamespace`, `CreateTable`, `CreateTableInline`, `DropTable`).
`RenameTable` bumps both the source and the destination namespace.

### I3. Inline metadata rotation preserves the manifest pool

Replaying an inline TM update (delta, full, or pointer-eviction) must
never silently drop the catalog's per-table manifest pool unless the
eviction is explicitly to pointer mode. `updateInlineMetadata` preserves
the pool; `removeInlineMetadata` clears it. Callers pick by intent, not
convenience. Every `UpdateTableLocation` is such an eviction: a table is
inline or a pointer table, never both, and the metadata file it names
carries a manifest list file for every snapshot (its writer wrote the
inline ones out first).

### I3a. A committed snapshot's manifest list never changes

Whether a snapshot's list is inline or a file, and whichever writer
reads it, it lists the same manifests with the same entries (sequence
numbers, spec, adding snapshot, counts, first row id). A delta sends
the lists of only the snapshots it adds, taken from the snapshots
themselves. A full-metadata commit (a create, or an update in full
mode) also sends the list of every inline snapshot it names that the
table no longer holds a list for: its handle loaded the table inline,
and another writer has since evicted it (Bug 62: a replace transaction
commits the snapshots it loaded). Eviction writes inline lists to
files unchanged; a new inline snapshot on a parent whose list is a file
carries its whole list. `TestCatalogFaultProperties` checks this after
every operation, and in v3 tables that a data file's row ids never
change.

### I4. Inline replay is deterministic

Every reader that replays the same records rebuilds the same metadata
for every inline table, whatever its clock. I6 and I7 rest on this: a
compactor writes a checkpoint of the state it replayed, and a delta
must replay to the metadata its writer committed. The location a table
loads from does not depend on the bytes: it is
`inline://<table>#<id>.<version>` (`FileIOCatalog.metadataLocation`;
keyed by version since 416b2c9, and by table id since Bug 22), so
`BaseMetastoreTableOperations` reloads when the table's version changes
(see [SPEC_TM.md](SPEC_TM.md) §"Table Loading").

Two operational rules in `InlineDeltaCodec` keep replay deterministic:

1. The base parsed inside `applyDelta` / `applyDeltaWithManifests` is
   given a deterministic synthetic `metadataFileLocation`
   (`inline://#<hash-of-base-bytes>`), not null. Without it,
   `Builder.buildFrom(base).discardChanges().build()` skips
   `addPreviousFile` and the new TM's metadata-log loses the entry for
   the prior version.
2. Every builder replay opens is pinned to the writer's last-updated-ms,
   which the delta carries (`setLastUpdatedMillis`), **before** any update
   is applied. `Builder.lastUpdatedMillis` starts null, and both
   `Builder.setRef` for `main` and `Builder.build` then stamp
   `System.currentTimeMillis()`: setRef as the snapshot-log time of a move
   to a snapshot the builder did not add, build as the table's
   last-updated-ms. Since Iceberg 1.12.0 `Builder.addSnapshot` no longer
   sets the field, so a delta that only adds a snapshot needs the pin too
   (Bug 60).

A delta's new snapshots are added in the same builder as its refs and
other metadata updates, as a writer's commit adds its snapshot and moves
the branch in one builder: Iceberg logs main's move to a snapshot that
builder added at the snapshot's own timestamp, which replay reconstructs
as `base.lastUpdatedMillis() + timestampDeltaMs`, and a move to any
other snapshot (a rollback) at last-updated-ms (Bug 60).

### I5. Inline-ML snapshots encode as sentinels, never as embedded paths

Every `Snapshot` written into inline-mode `TableMetadata` bytes carries
a non-null `manifestListLocation()`. For snapshots whose manifest list
lives in the catalog's manifest pool (inline-ML), that location is the
sentinel `inline://<snapshotId>`. The `InlineSnapshot` type (which
returns `null` from `manifestListLocation()`) is a runtime in-memory
representation only — before any codec encode of `TableMetadata`
that contains `InlineSnapshot` instances, the snapshots must be
rewritten to `BaseSnapshot` with the sentinel set, using
`TableMetadata.Builder.replaceSnapshots`. The rewrite is the only path
that keeps both codecs (`gzip` and `structural`) producing the same
on-wire shape: a `manifest-list` field with the sentinel, no embedded
`manifests` array, no v1-style path list.

On the read side, `wrapInlineManifests` (FileIOCatalog) reverses the
transformation by swapping each sentinel-bearing `BaseSnapshot` for an
`InlineSnapshot` whose `manifests` come from the pool. Loading rejects
a sentinel without a matching pool entry (`IllegalStateException`,
"catalog state corrupt") — catalog state must always satisfy "sentinel
⇒ pool entry".

The delta-update path emits sentinels through `AddSnapshotUpdate`
(which encodes the manifest-list suffix; for inline-ML the suffix is
empty and the consumer reconstructs `inline://<snapshotId>`). The
full-mode and pointer-mode write paths rely on the I5 rewrite.

### I6. A checkpoint holds exactly the replayed state

Compaction writes a checkpoint of the state a writer replayed from the
checkpoint and log, so replaying the log and decoding a checkpoint of
the result must give the same state: every namespace, property and
table by id, with its metadata location, inline metadata, codec,
manifest list prefix, manifest pool and snapshot references, and the
name lookups, id counters and committed-transaction set. An apply step
may not leave state a checkpoint does not represent: a table with both
a metadata location and inline metadata (Bug 27, which the next
compaction reverted to the inline copy), inline state for a dropped
table, an inline table without a prefix. `CheckpointRoundTrip` checks
every catalog file written in `TestCatalogFaultProperties`,
`TestMultiWriterProperties` and the `TestLocalCatalog*` suites.

### I7. A delta replays to the metadata its writer committed

An inline delta must hold every change of its commit, or
`InlineDeltaCodec.computeDelta` must refuse it (return null, so the
commit sends the full metadata). A change the delta has no update for
and does not refuse is lost on replay: a format-version upgrade (Bug 37)
or a removed ref (Bug 39) made alongside another change, a schema id or
count of 0 written as absent (Bugs 33, 38, 40), a value derived on
replay that the commit did not derive the same way (Bugs 41, 42).
Replay must also build as the writer did: TableMetadata.Builder checks
the default spec and sort order against the current schema on every
build, so a run of metadata updates is applied to one builder and built
once (Bug 49: an intermediate state of a replace to a schema without
the partition column did not validate), with the commit's new snapshots
in it (Bug 60: Iceberg logs main's move to a snapshot at the snapshot's
time only if the same builder added it). And it must number new schemas
and specs as the writer did: the builder gives each the id of an equal
one, else one past the highest, and replay adds before it removes (Bug
52: a transaction that cleaned up unused specs, then went back to one).
Replay's last column and partition ids are the base's, up to the
highest in the schemas and specs it adds, so a commit whose last id is
above those goes in full (Bug 63: a transaction that added a partition
field, removed it, and cleaned up the spec that had it).
`TestInlineDeltaProperties` runs random Iceberg operations and checks
that each commit's delta, replayed on its base as the inline codec
stores it, gives the committed metadata, and that the codec stores
every version exactly (Bug 43).

### I8. An inline catalog reads like a pointer catalog

Storing a table inline -- its metadata, its manifest lists, or both --
changes how it is stored, not what a reader sees. The same operations
on a table in a pointer catalog and on one in an inline catalog must
commit or fail alike and read back the same: format version, sequence
numbers and next row id, schemas, specs, sort orders and properties,
snapshots (by position, with their summaries and row-id fields) and
their manifest lists entry by entry and in order, refs and the snapshot
log, and every ref's live data and delete files with their sequence
numbers and row ids. Snapshot ids, UUIDs, timestamps and the names of
manifest and metadata files differ. `TestInlineDifferentialProperties`
checks this after every step of generated schedules, in each inline
mode, with tables moving in and out of inline storage (Bugs 50, 51).

### I9. A catalog operation that races others ends as a serial run

A catalog operation -- a namespace create, drop or property change, a
table create, drop, rename or property change -- depends on what it
read only through its preconditions, which it checks again when its
write conflicts. So one whose writes others' operations preceded must
report what it would have, and leave the catalog as it would, had it
run after them: return the same value or fail the same way, with the
same namespaces, properties and tables. `TestCatalogRaceProperties`
runs one operation with another just before its first write and a
third just before its second, in mixed modes, against a serial run of
the same operations on another copy (Bugs 53, 54, 55).

### I10. A landed commit is never reported as failed

Iceberg deletes a commit's new files when it fails with
`CommitFailedException` (a `CleanableFailure`), and a caller may run a
failed commit again, so a commit that landed must not report failure.
The commit loop reports a definite failure only before any write, or
after a read of the catalog without its record. A write whose outcome
it did not learn -- a lost reply, a failed read-back, a throttled write
that may have landed -- it settles by reading the catalog for the
transaction's id; if that read fails, or the loop runs out of attempts
while such a write may still be in flight, the commit ends in
`CommitStateUnknownException`. So does a commit whose id the committed
set's GC horizon has passed, if one of its writes may have landed;
otherwise it fails, since replay rejects its record from then on (Bug
64). A retention shorter than a commit takes is legal but lossy: it
makes this case common, but never incorrect ([GC.md](GC.md)). Work
after the commit lands does not change its outcome: FileIOCatalog
deletes the metadata files that dropped out of a table's log through
`CatalogUtil.deleteFiles`, which logs a failure instead of throwing it
(Bug 61). `TestCatalogFaultProperties` and `TestMultiWriterProperties`
check that an operation reported failed any way but unknown did not
take effect.

## Operation × Operation Conflict Matrix

The matrix below falls out of I2 (action versions are bumped on apply)
and the version-checking rules in [SPEC.md](SPEC.md) §"Action Verification
Rules". It is the *enforced* rule, backed by `TestProtoActions$ConflictTests`.

Each row is the **first** action applied (A); each column is a **second**
action (B) whose preconditions were captured against the pre-A state and
is applied to the post-A state. Cells hold:

- **✓** — the pair composes; B.verify succeeds.
- a field name — B.verify fails because A bumped that field.
- **—** — the pair is not meaningful (target doesn't exist).

Assumptions: same namespace or same table unless otherwise noted. Ops on
*different* namespaces/tables always compose. `CN↓` = create child under
the same parent.

|          | CN↓  | DN   | SP   | RP   | CT   | CTI  | DT    | UT    | RT'   | RT    |
|----------|------|------|------|------|------|------|-------|-------|-------|-------|
| **CN↓**  | ns v | ns v | ns v | ns v | ns v | ns v | ✓     | ✓     | ns v  | ✓     |
| **DN**   | —    | ns v | ns v | ns v | ns v | ns v | —     | —     | —     | —     |
| **SP**   | ns v | ns v | ns v | ns v | ns v | ns v | ✓     | ✓     | ns v  | ✓     |
| **RP**   | ns v | ns v | ns v | ns v | ns v | ns v | ✓     | ✓     | ns v  | ✓     |
| **CT**   | ns v | ns v | ns v | ns v | ns v | ns v | ✓     | ✓     | ns v  | ✓     |
| **CTI**  | ns v | ns v | ns v | ns v | ns v | ns v | ✓     | ✓     | ns v  | ✓     |
| **DT**   | ✓    | —    | ✓    | ✓    | ✓    | ✓    | —     | tbl v | tbl v | tbl v |
| **UT**   | ✓    | —    | ✓    | ✓    | ✓    | ✓    | tbl v | tbl v | tbl v | tbl v |
| **RT'**  | ns v | —    | ns v | ns v | ns v | ns v | tbl v | tbl v | tbl v | tbl v |
| **RT**   | ✓    | ✓    | ✓    | ✓    | ✓    | ✓    | tbl v | tbl v | tbl v | ✓     |

Legend: **CN↓** `CreateNamespace` (child), **DN** `DropNamespace`,
**SP** `SetNamespaceProperty`, **RP** `RemoveNamespaceProperty`,
**CT** `CreateTable`, **CTI** `CreateTableInline`, **DT** `DropTable`,
**UT** `UpdateTableLocation` / `UpdateTableInline`,
**RT'** `RenameTable` (in-place; bumps both source and destination ns),
**RT** `ReadTable`. "ns v" = parent namespace's version conflicts;
"tbl v" = table version conflicts.

What the matrix says:

1. All ns-level mutations under the same parent serialize on `ns v`.
2. `DT` and `UT` on the *same* table serialize on `tbl v`. Neither
   conflicts with unrelated ns-level ops except through the parent's
   children-set rule.
3. `DT` and `UT` on *different* tables in the same ns compose. Bumping
   `ns v` on `CT` / `DT` protects ns-level ops; it doesn't pessimize
   unrelated table ops, because those carry table-level preconditions
   that are more precise.
4. `ReadTable` is the read side of optimistic concurrency — it conflicts
   with any update to the same table, nothing else.
5. The root is a parent like any other: an ordinary namespace row with
   id 0, so top-level creates and drops serialize on its version. It
   cannot be named, created, or dropped (SPEC.md §State Model).
6. A conflict fails the record, not the operation: Iceberg re-applies a
   table commit on a fresh read, and FileIOCatalog re-applies a catalog
   operation -- a namespace create, drop or property change, a table
   create, rename or drop -- which depends on nothing a concurrent change
   alters but existence, checked again on the fresh read (Bugs 47, 53,
   54). A catalog transaction is the exception: it conflicts on the
   whole version of each table it writes, as it first read it, or reads
   under serializable isolation, and one that conflicts fails, for its
   caller to run again ([errata.md](errata.md) §D12).

## Late-Binding

Writers never allocate entity IDs. A create action carries a negative
late-bound ID, and replay binds it to a real ID:

- `buildActions()` sorts new namespaces by depth (parents before
  children) and numbers new namespaces and tables `-1, -2, ...`
  (separate sequences). Actions later in the same transaction reference
  a new entity by its late-bound ID.
- Version fields are set to `-1` when the referenced entity was created
  in the same txn.
- On replay, `Transaction.apply` binds each late-bound ID, at its
  defining create, to the replaying state's next namespace / table ID
  (`ProtoCodec.IdBinding`), and resolves later references to it. Every
  reader replays the log in the same order, so every reader assigns the
  same IDs.
- `Transaction.verify` rejects a record that references a late-bound ID
  before (or without) the create that defines it.

Binding at replay is what keeps concurrent creates apart. When writers
allocated IDs from their own snapshot, two writers that read the same
snapshot and created unrelated entities -- e.g. tables in different
namespaces -- wrote records with the same ID. Both records verified, and replay let the second overwrite the
first.

## Compaction Trigger

Compaction happens at write time when the file's observable state
crosses either threshold:

- `appendCount >= maxAppendCount`, or
- `current.length + len(txnBytes) > maxAppendSize`.

No on-disk hint is needed. Atomic append + atomic CAS at the storage
layer are sufficient to linearize commits, and either writer in a race
will independently decide to compact based on the live file's length
and the append count of the last version it read. After a lost append
race the loop retries without reading the file again, so its
`appendCount` is a lower bound; the count limit is soft. Field 2 in
`Transaction` (formerly a `sealed` bool flag) is reserved.

## Best Practices

### Adding a new catalog action

1. Add the message to `catalog.proto`, a new variant in the
   `Action.action` oneof, and its field-number constants in `ProtoCodec`.
   Regenerate the format explorer's fixtures (`mvn test
   -Dtest=FormatExplorerFixtures -Dexplorer.regenerate=true`).
   `TestFormatExplorer` fails until the constants match `catalog.proto`
   and the fixtures are current.
2. Implement `ProtoCodec.Action` (an interface: `verify`, `apply`,
   `bind`) and add a branch for it to `encodeAction` and `decodeAction`.
3. An action that references a namespace or table id must implement
   `bind(IdBinding)`, defining the late-bound ids it creates and
   resolving the ones it references (808bca7). The default `bind`
   returns the action unchanged: its late-bound ids would reach `apply`
   unbound, and `Transaction.verify`, which rejects a record whose
   reference precedes its create when `bind` throws, would not see
   them. Add it to `Transaction.selfConsistent` too, so a record that
   also drops what it references is rejected (Bug 14).
4. `apply` must bump the version of every entity whose state it mutates,
   including the parent namespace if it changes the namespace's
   children set (I2).
5. Add an `apply()` row to the conflict matrix above and one test in
   `TestProtoActions$ConflictTests` per cell.
6. Add an idempotency test to `TestProtoActions$IdempotencyTests`:
   replaying the action with a fresh UUID against the post-apply state
   must be rejected.

### Adding a new inline TM delta type

1. Add the message to `catalog.proto` under `TableMetadataUpdate.update`
   and its field-number constants in `InlineDeltaCodec`, and regenerate
   the explorer's fixtures, as for a catalog action.
2. Implement `InlineDeltaCodec.DeltaUpdate` (an interface with
   `applyTo(TableMetadata.Builder)`), and add a branch for it to the
   static `encodeUpdate` and `decodeUpdate` dispatchers. An update that
   references inline state such as the manifest pool is routed in
   `applyDeltaWithManifests` instead of applied to the builder.
3. Update `computeDelta` to emit the new update when relevant; the diff
   should be minimal (only emit when the field differs). It must refuse
   (return null) any change it cannot express (I7);
   `TestInlineDeltaProperties` checks that every delta replays to the
   committed metadata.
4. **Update ordering** matters when a Builder method rejects the
   operation against the current state (e.g. `removeSchemas` rejects
   removing the current schema id). Emit `Add → SetCurrent / SetDefault →
   Remove` so the pointer advances before the prune.
5. **Determinism check.** If the update's `applyTo` reads
   `Builder.lastUpdatedMillis` to stamp a derived field (snapshot-log
   entry, etc.), make sure the rebuild loop in `applyDeltaWithManifests`
   pins that value before `applyTo` runs. See I4.
6. Add unit tests in `TestInlineDelta`: encode/decode round-trip,
   apply against a fresh `TableMetadata`, idempotency on replay, the
   same metadata from repeated replays of the same delta (I4).
7. If the new update type interacts with manifest-pool state, also extend
   the routing in `applyDeltaWithManifests` (see SPEC_TM.md and SPEC_ML.md).

### Choosing append vs CAS

The default `max.append.count = 10000` works for S3 Express and Azure
AppendBlob. GCS has no append, and the format commits every write there
by CAS whatever the setting (`GCSFileIO.supportsAppend()` is false). For
S3 standard buckets, which support conditional full-object replace but
not conditional append, set `max.append.count = 0` to force CAS-only
mode.

The default `max.append.size = 16 MiB` is a soft target. Larger catalogs
mean longer reads on every load; tune down for read-heavy workloads, up
for write-heavy.

### Inline TM only vs TM+ML

- `inline=false` works on every storage provider; baseline behaviour with
  external `metadata.json` and `snap-*.avro` files.
- `inline=true, inline.manifests=false` saves the metadata-file write
  but keeps Avro manifest lists. Replay needs the fork's
  `TableMetadata.Builder.setLastUpdatedMillis` (I4).
- `inline=true, inline.manifests=true` saves both writes; it also needs
  the fork's `ManifestListSink`, `InlineSnapshot` and
  `TableMetadata.Builder.replaceSnapshots`.

Every mode needs the Iceberg fork for `SupportsAtomicOperations`. The
fork's `core-fixes` branch also fixes the Iceberg core issues in
[ICEBERG_ISSUES.md](ICEBERG_ISSUES.md), which come back on stock
Iceberg. See the [README](../README.md) for fork dependencies.

### Choosing the inline-TM codec

`fileio.catalog.inline.tm.codec` selects between two encoders for
inline `TableMetadata` bytes; decode always follows the on-wire tag, so
catalogs can carry a mix produced by different writers.

- `structural` (default) — purpose-built binary-columnar encoder. 40%
  smaller at the corpus median, 4–7× faster on lazy-decode workloads
  (current-snapshot lookups, snapshot-history scans). Recommended for
  production.
- `gzip` — Iceberg-style `Codec.GZIP` JSON. Useful when an external
  tool needs to read inline TM bytes by gunzipping them as
  `metadata.json.gz`, or as a diagnostic fallback. Larger and slower
  than `structural`; otherwise functionally equivalent.

## Hand-Rolled Wire Format

`ProtoCodec` and `InlineDeltaCodec` implement protobuf wire format
manually rather than using `protoc`-generated classes: no runtime
`protobuf-java` dependency or build-time `protoc`, and latitude for
lazy scanning (see the header of `catalog.proto`). The wire format is
still standard protobuf (field tags, varints, length-delimited
messages), and `catalog.proto` declares every field the codecs write:
`CatalogProto` and `TestFormatExplorer` check that in every `mvn test`
(see §"Codec Layout").

The trade-off: when adding a new field number, both `encode` and `decode`
need explicit handling, and `decodeXxx` must include a default case that
calls `skipField(wireType)` to preserve forward compatibility. Generated
classes handle this for you; manual codecs require it as a discipline.

## Test Layout

| File | What it covers |
|------|----------------|
| `TestProtoCatalogFormat` | PB codec, replay of catalog files, reader-version gates |
| `TestProtoActions` | Per-action positive/negative tests, conflict matrix, idempotency, randomized tests, inline tables, manifest-pool round-trip |
| `TestProtoCommitKnobs` | `max.append.count` / `max.append.size` thresholds; `MockIO` |
| `TestInlineDelta` | Inline TM delta encode/decode/apply, `computeDelta`, ML delta types |
| `TestInlineManifestEndToEnd` | End-to-end commit→reload across BASELINE / TM_ONLY / TM_ML configs; per-`SnapshotProducer` operation coverage; codec-axis subclass parameterized over `gzip` / `structural` |
| `CatalogTests`, `CatalogTransactionTests` | Abstract suites consumed from `iceberg-core-tests.jar` (commit `35076d6`) and run by the local and cloud subclasses |
| `TestLocalCatalog`, `TestLocalCatalogTransaction` | Both abstract suites in every `LocalCatalogMode` (append / CAS / compact × pointer / inline TM / inline ML) over `InMemoryAtomicFileIO` |
| `TestCatalogFaultProperties` | jqwik: several writers run Iceberg operations through FileIOCatalog on faulty storage, in mixed modes; the catalog must match a model after every operation (I3a, I6, I10) |
| `TestCatalogRaceProperties` | jqwik: a catalog operation raced by others must end as a serial run (I9) |
| `TestInlineDeltaProperties` | jqwik: each commit's delta replays to the committed metadata, and each codec stores every version exactly (I7) |
| `TestInlineDifferentialProperties` | jqwik: the same schedule on a pointer catalog and an inline one commits alike and reads the same (I8) |
| `TestCatalogRegressions` | One nested group per catalog bug, with the smallest scenario that reproduces it |
| `TestCommittedSetGc`, `TestCommittedTransactions` | Committed-set encoding, GC merge and horizon, tri-state lookup ([GC.md](GC.md)) |
| `TestIcebergCoreIssues` | Canaries asserting each Iceberg core issue C1-C24 ([ICEBERG_ISSUES.md](ICEBERG_ISSUES.md)) is absent |
| `CheckpointRoundTrip` | Not a test class: checks a catalog file against `catalog.proto` and its replayed state against a checkpoint of it (I6); the local suites and the fault and multi-writer fuzzers run it on every write, and other suites use it too |
| `TestFormatExplorer` | Codec field-number constants against `catalog.proto` both ways; explorer fixtures current |
| `spec/` (`TestCodecProperties`, `TestInvariantProperties`, `TestCanonicalStateProperty`, `TestMultiWriterProperties`) | jqwik at the format layer: codec round trip and determinism, well-formedness, builder state through the codec, and a multi-writer fuzzer of the commit protocol |
| `GCSCatalogTest`, `TestS3Catalog`, `ADLSCatalogTest` | Cloud integration tests, pointer mode; append mode on S3 and ADLS, CAS on GCS (require credentials/emulators) |
| `*CAS`, `*InlineTM`, `*InlineML` subclasses | Cloud matrix subclasses overriding `maxAppendCount()` / `inlineTM()` / `inlineML()` -- see [COMPAT.md](COMPAT.md) for status |

The end-to-end suite uses an in-memory `MemoryFileIO`, and the local
suites and fuzzers `InMemoryAtomicFileIO`, which enforces conditional
CAS and append, so commit→reload cycles run without storage. Cloud tests
verify that the actual provider APIs honour `SupportsAtomicOperations`
semantics.
