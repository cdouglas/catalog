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

The catalog file is an **immutable snapshot** (`CatalogFile`,
`ProtoCatalogFile`). All mutation goes through a builder (`CatalogFile.Mut`,
`ProtoCatalogFile.Mut`) that accumulates pending changes and commits them
atomically:

1. Read the current `CatalogFile` snapshot.
2. Create a `Mut` builder from it.
3. Accumulate changes (`createTable`, `dropNamespace`, etc.).
4. `commit()` builds an intention record and writes it atomically.
5. On `CASException` or `AppendException`, retry from step 1 (or, for
   append failures with idempotent bytes, retry at the new offset).

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

The codec is split across two classes:

- `ProtoCodec` handles catalog-level messages: `Checkpoint`, `Transaction`,
  `Action` oneof, all catalog/namespace/table actions.
- `InlineDeltaCodec` handles inline TM delta messages:
  `TableMetadataDelta`, all 13 `TableMetadataUpdate` types,
  `CompactSummary`, `ManifestFileEntry`.

Both implement protobuf wire format manually; neither depends on
`protoc`-generated classes. The output is wire-compatible with
`catalog.proto` so any protobuf tool can decode it given the schema.

### Component Separation

This project consumes Iceberg as a Maven SNAPSHOT artifact. The
`SupportsAtomicOperations` interface, the `ManifestListSink` extension
point, and the cloud-provider `SupportsAtomicOperations` implementations
(`S3FileIO`, `GCSFileIO`, `ADLSFileIO`) all live in the iceberg fork at
`../iceberg/`. The catalog format and the FileIO catalog itself live here.

## Invariants

These hold in the current code and must not regress.

### I1. One intention record per commit

A single `doCommit` / `commitTransaction` produces exactly one
transaction-level record per affected table, encoded into one transaction
and written to storage in one atomic operation (append or CAS). Every
configuration is subject to this rule:

- **pointer**: one `UpdateTableLocation` / `CreateTable` action referencing
  a pre-written `v<N>.metadata.json`.
- **TM inline**: one `UpdateTableInline` action carrying delta or full
  metadata.
- **TM+ML inline**: one `UpdateTableInline` action whose delta carries the
  TM updates *and* the per-snapshot `AddManifestDelta` /
  `RemoveManifestDelta` entries in the same blob.

A compaction or crash mid-flight must not be able to observe a half-applied
commit. ML actions are never written separately from the TM action that
introduces the snapshot they belong to.

### I2. Actions are idempotent on replay

Each action's `verify` checks preconditions (table version, namespace
version, read-set versions); each action's `apply` mutates state so a
replay of the same record against the post-apply state fails `verify`.
This is the second line of defence alongside `committedTxn` deduplication
— both must hold.

`apply()` bumps the version of any entity whose state it mutates,
including the *parent namespace* on actions that change a namespace's
children set or properties (`CreateNamespace`, `DropNamespace`,
`SetNamespaceProperty`, `RemoveNamespaceProperty`, `CreateTable`,
`CreateTableInline`, `DropTable`).

### I3. Inline metadata rotation preserves the manifest pool

Replaying an inline TM update (delta, full, or pointer-eviction) must
never silently drop the catalog's per-table manifest pool unless the
eviction is explicitly to pointer mode. `updateInlineMetadata` preserves
the pool; `removeInlineMetadata` clears it. Callers pick by intent, not
convenience.

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

|         | CN↓  | DN   | SP   | RP   | CT   | CTI  | DT    | UT    | RT    |
|---------|------|------|------|------|------|------|-------|-------|-------|
| **CN↓** | ns v | ns v | ns v | ns v | ns v | ns v | ✓     | ✓     | ✓     |
| **DN**  | —    | ns v | ns v | ns v | ns v | ns v | —     | —     | —     |
| **SP**  | ns v | ns v | ns v | ns v | ns v | ns v | ✓     | ✓     | ✓     |
| **RP**  | ns v | ns v | ns v | ns v | ns v | ns v | ✓     | ✓     | ✓     |
| **CT**  | ns v | ns v | ns v | ns v | ns v | ns v | ✓     | ✓     | ✓     |
| **CTI** | ns v | ns v | ns v | ns v | ns v | ns v | ✓     | ✓     | ✓     |
| **DT**  | ✓    | —    | ✓    | ✓    | ✓    | ✓    | —     | tbl v | tbl v |
| **UT**  | ✓    | —    | ✓    | ✓    | ✓    | ✓    | tbl v | tbl v | tbl v |
| **RT**  | ✓    | ✓    | ✓    | ✓    | ✓    | ✓    | tbl v | tbl v | ✓     |

Legend: **CN↓** `CreateNamespace` (child), **DN** `DropNamespace`,
**SP** `SetNamespaceProperty`, **RP** `RemoveNamespaceProperty`,
**CT** `CreateTable`, **CTI** `CreateTableInline`, **DT** `DropTable`,
**UT** `UpdateTableLocation` / `UpdateTableInline`, **RT** `ReadTable`.
"ns v" = parent namespace's version conflicts; "tbl v" = table version
conflicts.

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

## Late-Binding

When a transaction creates a namespace or table and references it in the
same transaction, the entity ID is not yet allocated. The format uses
negative integers as virtual IDs:

- `ProtoIdManager` maintains allocation counters and a remap table
  (`virtualId -> realId`).
- `buildActions()` sorts new namespaces by depth (parents before
  children) and assigns real IDs.
- Version fields are set to `-1` when the referenced entity was created
  in the same txn.
- On replay, `resolve(id)` maps negative IDs to their allocated
  counterparts.

## Sealing

The transaction `sealed` flag is a `bool` (field 2 in `Transaction`). A
writer about to push the log past `maxAppendCount` or `maxAppendSize`
seals its transaction so the next writer knows to compact rather than
append. Sealing is bytewise in-place: `ProtoCodec.sealTransaction()` /
`unsealTransaction()` toggle the varint at the field-2 offset without
re-encoding. To make this work, `encodeTransaction()` always writes
field 2 even when false (overriding proto3's default-value suppression).

## Best Practices

### Adding a new catalog action

1. Add the message to `catalog.proto` and a new variant in the
   `Action.action` oneof.
2. Add an `Action` subclass in `ProtoCodec` (or its inner class hierarchy)
   with an `encode`, `decode`, `verify`, and `apply` method.
3. `apply` must bump the version of every entity whose state it mutates,
   including the parent namespace if it changes the namespace's
   children set or properties (I2).
4. Add an `apply()` row to the conflict matrix above and one test in
   `TestProtoActions$ConflictTests` per cell.
5. Add an idempotency test to `TestProtoActions$IdempotencyTests`:
   replaying the action with a fresh UUID against the post-apply state
   must be rejected.

### Adding a new inline TM delta type

1. Add the message to `catalog.proto` under `TableMetadataUpdate.update`.
2. Add a `DeltaUpdate` subclass in `InlineDeltaCodec` with `encodeUpdate`,
   `decodeUpdate`, and either `applyTo(TableMetadata.Builder)` (for
   metadata-only changes) or routing through the prefix-accepting
   overload (for changes that reference inline state like manifest pools).
3. Update `computeDelta` to emit the new update when relevant; the diff
   should be minimal (only emit when the field differs).
4. Add unit tests in `TestInlineDelta`: encode/decode round-trip,
   apply against a fresh `TableMetadata`, idempotency on replay.
5. If the new update type interacts with manifest-pool state, also extend
   the routing in `applyDeltaWithManifests` (see SPEC_TM.md and SPEC_ML.md).

### Choosing append vs CAS

The default `max.append.count = 10000` works for S3 Express and Azure
AppendBlob. For S3 standard or GCS — providers that support conditional
full-object replace but not conditional append — set `max.append.count = 0`
to force CAS-only mode.

The default `max.append.size = 16 MiB` is a soft target. Larger catalogs
mean longer reads on every load; tune down for read-heavy workloads, up
for write-heavy.

### Inline TM only vs TM+ML

- `inline=false` works on every storage provider; baseline behaviour with
  external `metadata.json` and `snap-*.avro` files.
- `inline=true, inline.manifests=false` saves the metadata-file write
  but keeps Avro manifest lists. Works with stock Iceberg.
- `inline=true, inline.manifests=true` saves both writes; **requires the
  Iceberg fork** for `ManifestListSink` and `InlineSnapshot`. See the
  [README](../README.md) for fork dependencies.

## Hand-Rolled Wire Format

`ProtoCodec` and `InlineDeltaCodec` implement protobuf wire format
manually rather than using `protoc`-generated classes. This choice lets
the codebase validate correctness against `catalog.proto` before
committing to generated-code dependencies; the wire format is still
standard protobuf (field tags, varints, length-delimited messages), so
any protobuf tool can decode the output given the `.proto` file.

The trade-off: when adding a new field number, both `encode` and `decode`
need explicit handling, and `decodeXxx` must include a default case that
calls `skipField(wireType)` to preserve forward compatibility. Generated
classes handle this for you; manual codecs require it as a discipline.

## Test Layout

| File | What it covers |
|------|----------------|
| `TestProtoCatalogFormat` | PB codec, format read/write, header validation |
| `TestProtoActions` | Per-action positive/negative tests, conflict matrix, idempotency, randomized tests, inline tables, manifest-pool round-trip |
| `TestProtoCommitKnobs` | `max.append.count` / `max.append.size` thresholds; `MockIO` |
| `TestInlineDelta` | Inline TM delta encode/decode/apply, `computeDelta`, `selectMode`, ML delta types |
| `TestInlineManifestEndToEnd` | End-to-end commit→reload across BASELINE / TM_ONLY / TM_ML configs; per-`SnapshotProducer` operation coverage |
| `CatalogTests`, `CatalogTransactionTests` | Abstract suites copied from Iceberg core, run by cloud-provider subclasses |
| `GCSCatalogTest`, `TestS3Catalog`, `ADLSCatalogTest` | Cloud integration tests (require credentials/emulators; pointer mode only today) |

The end-to-end suite uses an in-memory `MemoryFileIO` so commit→reload
cycles run without storage. Cloud tests verify that the actual provider
APIs honour `SupportsAtomicOperations` semantics.
