# Catalog Format Errata

Pending and deferred design decisions for the log-based catalog format.

## Deferred: Forward Compatibility for Columnar Regions

Protobuf's unknown field preservation works well for sparse/structural data (headers,
transaction records), but doesn't directly apply to columnar-encoded regions within
the checkpoint.

**Open questions:**
- How should readers handle unknown columns within a region?
- Should each column carry its own schema version/tag?
- Can we use a protobuf envelope around columnar data to get unknown field semantics?

**Potential approaches:**
1. Region-level versioning with fallback to raw bytes
2. Column descriptors with type tags (similar to Parquet schema)
3. Hybrid: protobuf message with `bytes` fields for columnar data

## Deferred: Inlined Table Metadata

Embedding JSON table metadata directly in the catalog file could reduce I/O for
common operations and enable cross-table deduplication.

**Optimization opportunities:**
- String dictionary for repeated schema field names, column names, types
- Location templates: `s3://bucket/warehouse/{ns}/{table}/metadata/{version}.metadata.json`
- Delta-encoded partition specs across similar tables
- Shared schema fragments (many tables share identical column definitions)

**Open questions:**
- Parse and re-encode JSON, or deduplicate raw strings?
- Granularity: full metadata vs. hot fields (schema, partition spec, current snapshot)?
- Invalidation: how to handle metadata that changes outside the catalog?
- Size budget: when does inlining hurt more than help?

## Deferred: Advanced Encodings

Potential columnar optimizations for future consideration:

- **RLE for version columns**: Many entries at version 1
- **Delta-RLE for sequential IDs**: nsid, tbl_id often sequential
- **Prefix compression for string dictionary**: Common path prefixes
- **UUIDv7 timestamp delta encoding**: Committed transactions from similar timeframes

These are deferred until the base format is stable and benchmarks indicate need.

## Pending: Cross-Project Test Synchronization

The abstract test classes `CatalogTests` and `CatalogTransactionTests` are duplicated between:
- `iceberg/core/src/test/java/org/apache/iceberg/catalog/`
- `fileio-catalog/src/test/java/org/apache/iceberg/catalog/`

**Current state:**
- fileio-catalog copies these from iceberg to avoid circular dependencies
- Changes in iceberg must be manually synced to fileio-catalog

**Potential solutions:**
1. Extract shared test infrastructure to a separate module
2. Use Git submodule or subtree for shared test code
3. Publish iceberg test-jar and depend on it (current approach, but fragile)

## Resolved: LogCatalogFormat and CASCatalogFormat Removed

LogCatalogFormat and CASCatalogFormat (including `LogCatalogFile`, `LogSerializer`,
`LogMutator`, `LogIdManager`) have been removed. ProtoCatalogFormat is now the only
catalog format. The known LCF serialization issues (naive DataOutputStream encoding,
no schema evolution, no per-record checksums) are addressed by protobuf's wire
format and the separated `ProtoCodec` / `InlineDeltaCodec` classes.

## Pending: Cloud Provider Integration Tests

Integration tests for AWS S3 and Azure ADLS require:
- **S3**: LocalStack or real AWS credentials
- **Azure**: Azurite emulator or real Azure credentials
- **GCS**: fake-gcs-server or real GCP credentials

Currently, tests are skipped without credentials. Consider:
1. Testcontainers setup for automated emulator lifecycle
2. CI/CD pipeline with secret management
3. Mock-based unit tests as fallback

## INLINE_ML.md Errata (2026-04-16)

Discrepancies between INLINE_ML.md and the actual codebase, discovered during
implementation planning for inline manifest lists.

### E-ML1. `InlineSnapshot` access modifier is already resolved

The spec (INLINE_ML.md lines 928-938) discusses that `InlineSnapshot` is
package-private and proposes making it public. Commit `72159117e` on
`vldb-1.10.1-ml` already made it public. The alternatives discussion
(factory method, reflection) is moot.

### E-ML2. `manifestListPrefix` vs `manifest_path_prefix` naming

The spec uses three names interchangeably:

- `manifest_list_prefix` -- field 6 in `InlineTable` (catalog.proto:75),
  returned by `ProtoCatalogFile.manifestListPrefix(tblId)`
- `manifestPathPrefix` -- proposed `InlineTable` extension (INLINE_ML.md:443)
- `manifest_path_prefix` -- proposed protobuf field (INLINE_ML.md:1019)

These are conceptually the same: a shared prefix for manifest file paths
within a table's metadata directory. The existing name `manifest_list_prefix`
is slightly misleading since it prefixes manifest *file* paths, not manifest
*list* paths.

**Decision:** Keep existing `manifest_list_prefix` / `manifestListPrefix`
to avoid a migration. Add a clarifying comment.

### E-ML3. `TableMetadata.Builder` snapshot replacement API

The spec (INLINE_ML.md lines 902-916) proposes:

```java
builder.removeSnapshots(ImmutableList.of(s));
builder.addSnapshot(wrapped);
```

But `removeSnapshots()` takes `List<Long>` (snapshot IDs), not
`List<Snapshot>`. The corrected call is:

```java
builder.removeSnapshots(ImmutableList.of(s.snapshotId()));
builder.addSnapshot(wrapped);
```

Even corrected, removing and re-adding snapshots may trigger builder side
effects (snapshot log updates, ref revalidation). Needs prototyping during
implementation to determine if a lower-level approach is required.

### E-ML4. `AddSnapshotUpdate.applyTo` needs ML-aware path

The existing `InlineDeltaCodec.AddSnapshotUpdate` (InlineDeltaCodec.java:572)
requires `manifestListSuffix` to construct the snapshot's
`manifestListLocation`. With inline ML, new snapshots have no manifest list
file. `applyTo` needs a code path where `manifestListSuffix` is null/empty,
producing an `InlineSnapshot` instead of a `BaseSnapshot`. This is a
functional change to the existing class, not just a new class.

### E-ML5. Pool index stability on snapshot deletion

The pool + index design (INLINE_ML.md lines 803-813) stores unique
ManifestFile entries by position. Expiring a snapshot can't simply delete
pool entries (indices would shift). Options:

1. Tombstone slots (null, skip during iteration)
2. Rebuild pool on checkpoint write
3. Path-keyed maps in memory, convert to index arrays only for serialization

**Decision:** Option 3. Use `Map<String, ManifestFile>` keyed by path in
memory. Convert to positional arrays only during checkpoint write. This
avoids the index-stability problem entirely and simplifies the Java API.

### E-ML6. No end-to-end inline test coverage via `FileIOCatalog`

The existing tests exercise inline at the proto-format level
(`TestProtoActions.InlineTableTests`, `TestInlineDelta`) but never set
`fileio.catalog.inline=true` on a `FileIOCatalog` instance. All cloud
integration tests (`GCSCatalogTest`, `TestS3Catalog`, `ADLSCatalogTest`)
use pointer mode exclusively. The `loadFromCatalogFile` inline path and
`commitInline` path have no end-to-end test coverage through the catalog API.

This is a pre-existing gap. The parameterized test strategy addresses it.

## INLINE_ML Implementation Review Fixes (2026-04-17)

Defects identified by ML_INLINE_IMPL_REVIEW.md and resolved:

### §3.1/§3.2 Resolved: computeDelta NPE on InlineSnapshot

`computeDelta` now handles null `manifestListLocation` (InlineSnapshot)
by using empty string as the suffix. Defensive null check added to
`writeString`. `AddSnapshotUpdate` uses `"inline://<snapshotId>"` as a
sentinel manifest-list location in the JSON, ensuring SnapshotParser
takes the v2 path rather than the v1 embedded-manifests branch.

### §3.3/§2.4 Resolved: Delta replay for ML updates

New `applyDeltaWithManifests()` routes `AddManifestUpdate`/
`RemoveManifestUpdate` to `ProtoCatalogFile.Builder` manifest pool, and
`AddSnapshotUpdate` through its prefix-accepting overload. Manifest
carry-forward copies parent snapshot's refs for new snapshots.
`updateInlineMetadata()` preserves pool during metadata rotation.

### §2.1 Resolved: InlineSnapshot equals/hashCode

`InlineSnapshot` now overrides `equals()` and `hashCode()` matching on
snapshotId, parentId, sequenceNumber, timestampMillis, schemaId.
`BaseSnapshot.equals()` loosened to accept any `Snapshot` (not just
`BaseSnapshot`).

### §2.2 Resolved: Full-mode guard for inline snapshots

When ML deltas are attached, `commitInline` forces delta mode. The
full/pointer modes lose ML payload because `SnapshotParser.toJson`
serializes InlineSnapshot via the v1 embedded-manifests branch.

### §2.6 Resolved: SnapshotProducer cleanup null-safety

Reversed comparison to `manifestList.equals(committedSnapshot.
manifestListLocation())` — the loop variable is never null, making the
comparison null-safe when the committed snapshot is an InlineSnapshot.

## ML_INLINE_REVIEW2.md Remediation (2026-04-17 → 2026-04-18)

### §1.3/§2.5 Resolved: AddSnapshotUpdate wire format extended

Added three optional wire fields to `AddSnapshot` (proto fields 7, 8, 9):
- `parent_snapshot_id` (fixed64): required for stage-only, branch
  commits, cherry-pick. Replaces the buggy `base.currentSnapshot()`
  fallback.
- `first_row_id` (int64): required for v3+ row-lineage tables.
- `key_id` (string): required for encrypted tables.

All optional for backward compatibility. `computeDelta` populates from
`Snapshot.parentId()`, `Snapshot.firstRowId()`, `Snapshot.keyId()`.
Sink now captures `nextRowId`/`nextRowIdAfter` into a
`StagedSnapshotData` record.

### §1.1 Resolved: Full-mode pool preservation

Two-layer defense:
1. Replay-side (ProtoCodec UpdateTableInlineAction full-mode branch):
   uses `updateInlineMetadata` instead of `removeInlineMetadata +
   addInlineTable`. `removeInlineMetadata` reserved for POINTER-mode
   eviction.
2. Commit-side (FileIOCatalog.commitInline): force delta mode when the
   catalog has an existing ML pool, regardless of whether the current
   commit stages new ML deltas. Prevents oversized full TM writes.

### §2.6 Resolved: RemoveSnapshotsUpdate cascade to pool

`ProtoCatalogFile.Builder.removeSnapshotManifests(tblId, snapshotId)`
drops the snapshot's ref list and GCs any pool entries no longer
referenced. `applyDeltaWithManifests` routes `RemoveSnapshotsUpdate`
through this helper. Prevents unbounded pool growth after snapshot
expiration.

### §1.2 Partial: commitTransaction ML integration

Per-snapshot ML delta is now extracted from staged snapshots in
`newMetadata` and attached to the `UpdateTableInlineDelta`. Works for
single-table transactions. **Caveat**: `BaseTransaction`'s
`TransactionTableOperations` wrapper doesn't forward `ManifestListSink`,
so `SnapshotProducer` writes a transient `snap-*.avro` during transaction
staging. The ML delta is still captured via `allManifests(io)` but the
Avro file is written. Eliminating that requires forwarding the sink
through the wrapper (iceberg core change; tracked as follow-up).

### §2.1 Resolved: Delta ordering invariant

`applyDeltaWithManifests` now documents and enforces: `AddSnapshotUpdate`
must precede `AddManifestUpdate` / `RemoveManifestUpdate` for the same
snapshot id. Fails with `IllegalStateException` on violation.

### §2.3 Resolved: inline:// sentinel leak check

`wrapInlineManifests` scans for `inline://` sentinel locations without
corresponding pool entries and throws `IllegalStateException`. Prevents
silent `File not found` errors when the pool is corrupted.

### §2.4 Resolved (docs): InlineSnapshot.equals semantics

Class javadoc now documents that `equals` compares only scalar identity
fields, NOT manifest list contents. Callers needing manifest-pool
divergence detection must compare `allManifests()` directly.

### §2.7 Partial: Mixed-mode detection

`wrapInlineManifests` logs a warning when a table has both inline and
pointer-mode snapshots. Strict-mode rejection deferred (requires
threading the catalog property through to the static inner class).

### §4.1 Resolved (partial): Per-operation test coverage

Added 5 new tests to `TmMlTests` covering MergeAppend, Overwrite,
ReplacePartitions, Delete, RewriteManifests. CherryPickOperation and
BaseRowDelta still untested (deferred — require more complex setup).
Writing the Overwrite test discovered a latent bug: `computeDelta` did
not ensure the "operation" key was present in the summary map, so
non-append operations round-tripped as "append" on reload. Fixed.

### Open follow-ups (tracked, not closed)

- **§2.2 core fork**: `TableMetadata.Builder.replaceSnapshots` change
  in iceberg-core is still load-bearing. Replacing `wrapInlineManifests`
  with a parser-level wrap strategy would remove the fork.
- **TransactionTableOperations sink forwarding**: required for
  commitTransaction to avoid writing transient `snap-*.avro` files.
- **Multi-table buildTable().create() bug**: consecutive creates in
  a single catalog session overwrite each other. Pre-existing,
  unrelated to ML.
- **CherryPickOperation / BaseRowDelta end-to-end tests**: deferred.
- **Strict mixed-mode rejection**: deferred.
