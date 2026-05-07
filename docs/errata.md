# Errata

Current shortcuts, deferred functionality, known bugs, and known unknowns
in `FileIOCatalog` and the inline manifest-list extension. Items are
grouped by what they are; within each group, ordering is suggestive, not
strict.

For invariants that must not regress, see [design.md](design.md). For the
wire format see [SPEC.md](SPEC.md).

## Current Shortcuts

### S1. `wrapInlineManifests` mutates a parsed `TableMetadata` via a builder fork

`FileIOCatalog.wrapInlineManifests` calls
`TableMetadata.Builder.replaceSnapshots(Map<Long, Snapshot>)` to swap
parsed `BaseSnapshot` instances for `InlineSnapshot` wrappers after
`TableMetadataParser.fromJson` returns. `replaceSnapshots` is a 32-line
addition to `TableMetadata.Builder` in the iceberg fork (with a
`snapshotsReplaced` flag wiring it into `hasChanges()`).

This works, but it is a builder semantics change in core that would have
to be upstreamed — or rewritten — before this catalog could ride a stock
Iceberg. The cleaner alternative is a parser-level hook: detect the
`inline://<snapshotId>` sentinel during snapshot deserialization, look up
pool entries, and construct `InlineSnapshot` directly. **Trigger to
revisit:** any plan to drop the iceberg fork or upstream the changes.

## Deferred Functionality

### D3. Pointer-mode eviction of an inline-ML table does not materialize Avro manifest lists

If an inline TM falls back to pointer mode (size eviction), any snapshot
with inline ML keeps its manifest data in the checkpoint. Pointer-mode
eviction writes `metadata.json`, not `snap-*.avro`. Snapshots that need
an Avro manifest list at read time would have to materialize one
on demand, which is not implemented today.

**Trigger to revisit:** if a real workload hits the path. Until then,
inline ML implies the table stays inline.

### D5. `renameTable` is layered as drop+create, not as an in-place update (resolved)

`renameTable` is now a first-class action: `RenameTable(id, version,
new_namespace_id, new_namespace_version, new_name)`, wire type 11. The
action mutates `TblEntry` in place — the `int` table id stays put, so
all id-keyed maps (`tblInlineMetadata`, `tblManifestPrefix`,
`manifestPool`, `snapshotManifests`) follow by construction. Inline-TM
and inline-ML rename are correct and atomic. Conflict semantics fall
out of the existing version-bumping rules in the
design.md operation × operation matrix (entry RT'). See
[SPEC.md](SPEC.md), [SPEC_TM.md](SPEC_TM.md) §"Rename", and
`TestProtoActions$RenameTableTests` /
`TestProtoActions$ConflictTests` for coverage.

### D4. `RewriteTablePathUtil` does not handle inline-ML tables

`core/.../RewriteTablePathUtil.java:252,280` calls `ManifestLists.write()`
unconditionally and dereferences `snapshot.manifestListLocation()` with
no null check. Running the path-rewrite migration utility on an
inline-ML table NPEs. Pre-existing in upstream; flagged here so users
who hit it mid-migration can recognize it.

## Test Coverage Gaps

### T2. Cloud integration tests don't run in inline-ML mode (mostly resolved)

S3 and GCS now run an `inline=true` matrix:
`TestS3CatalogCASInlineTM` / `TestS3CatalogCASInlineML` and the GCS
counterparts; the same shape exists for the transaction suites. Each is
a one-line subclass that overrides `inlineTM()` / `inlineML()` and
flips `maxAppendCount()` to 0. The full atomic-mode × inline-mode grid
status lives in [COMPAT.md](COMPAT.md). The non-inlined and CAS-inline-
TM cells are green on S3 and GCS; the inline-TM cells require the
`InlineDeltaCodec` determinism + RemoveSchemas/RemovePartitionSpecs
fixes from this session.

**Still open for ADLS:** `ADLSCatalogTest` and
`ADLSFileIOCatalogTransactionTests` now share the same hooks
(`maxAppendCount`, `inlineTM`, `inlineML`) as their S3/GCS counterparts
and have CAS-mode subclasses (`ADLSCatalogTestCAS`,
`ADLSFileIOCatalogTransactionTestsCAS`) but no `*InlineTM` /
`*InlineML` variants yet. Adding them is mechanical once the inline-CAS
matrix on S3/GCS proves the underlying behavior, which it now does.

### T4. Inline-CatalogTests reachability tests (mostly resolved)

Three upstream `CatalogTests` cases were originally `@Disabled` on the
inline-TM matrix because they assert through accessors that don't
translate cleanly to inline metadata:

* `testMetadataFileLocationsRemovalAfterCommit` calls
  `ReachableFileUtil.metadataFileLocations(table, false)`, which adds
  the URIs to a `Set<String>` and (with `recursive=false`) doesn't
  actually resolve them as files. The non-test problem was upstream
  `CatalogUtil.deleteRemovedMetadataFiles`, which forwards
  `inline://` URIs to `FileIO.deleteFiles` after retention bounds
  kick in — S3 / GCS / ADLS treat them as missing keys and surface a
  `BulkDeletionFailure`.
* `testRegisterTable` round-trips a metadata-location string through
  `catalog.dropTable` then `catalog.registerTable`. After drop, the
  inline metadata bytes are gone; without a register-from-bytes API,
  the round-trip is fundamentally inexpressible.
* `testRegisterExistingTable` round-trips the same metadata location
  but expects an `AlreadyExistsException`. Upstream
  `BaseMetastoreCatalog.registerTable` runs the `tableExists` check
  *before* any I/O on the URI, so the test passes against the
  inline:// pseudo-URI as-is.

**Resolved 2026-05-06:**

* `FileIOTableOperations.commit` overrides the upstream
  `BaseMetastoreTableOperations.commit` to call a local
  `deleteRemovedRealMetadataFiles` that filters `inline://` entries
  out of the previousFiles diff before invoking
  `FileIO.deleteFile(s)`. Real metadata files (pointer-mode
  predecessors) still get cleaned up; inline pseudo-URIs are
  no-ops. `BulkDeletionFailure` no longer fires.
* `TestS3CatalogCASInlineTM` / `GCSCatalogTestInlineTM` carry an
  inline-equivalent `testMetadataFileLocationsRemovalAfterCommit`
  that asserts on `metadata.previousFiles().size()` directly (one
  fewer than the upstream count, since the upstream assertion adds
  `metadataFileLocation()` to the set). The semantic — bounded
  history under `METADATA_PREVIOUS_VERSIONS_MAX` after enabling
  `METADATA_DELETE_AFTER_COMMIT_ENABLED` — is preserved verbatim;
  only the accessor changed. Confirmed passing on S3.
* `testRegisterExistingTable` re-enabled (the `@Disabled` override
  removed). The existence check fires before URI resolution, so the
  inline:// path never gets touched.

**Still disabled:** `testRegisterTable` only. The drop +
registerTable round-trip requires a register-from-bytes API on
`FileIOCatalog` — no equivalent exists today. Disabled with the gap
documented inline; revisit if a real workload needs catalog
re-registration of a previously-dropped inline table.

### T3. Cloud integration tests require manual emulator setup

S3, ADLS, and GCS integration tests are skipped without credentials.
We rely on developer-run emulators; CI coverage is best-effort.

**Fix path:** add Testcontainers lifecycle for LocalStack (S3), Azurite
(ADLS), and fake-gcs-server (GCS) under the `verify` Maven profile.
Gate container startup behind a profile flag so `mvn test` stays fast
and offline.

## Known Unknowns

These are pending measurement or validation. None block current work;
each has a stated trigger condition.

### U1. Per-commit byte counts in production-like workloads

The size estimates in [SPEC_TM.md](SPEC_TM.md) (~80–115 B for an
`AddSnapshot` delta) and [SPEC_ML.md](SPEC_ML.md) (~150 B per
`AddManifestDelta`) come from local synthetic commits and partition
specs we control. We have not measured against:

- partitioned tables with high-cardinality string partitions (long
  bounds → larger `PartitionFieldSummaryEntry`);
- snapshot summaries with many `extra` entries;
- v3+ row-lineage tables.

**Trigger:** before publishing comparative benchmarks, validate the
estimates with an instrumented run.

### U2. Checkpoint size scaling with manifest-pool growth

The pool stores each unique manifest once per table; consecutive
`FastAppend` snapshots add one entry. Long-lived tables with many
retained snapshots have not been measured. Compaction (CAS) writes the
full pool every time, so pool size directly drives checkpoint write
cost.

**Trigger:** measure pool size at 100 / 1000 / 10000 retained snapshots
on a representative table; decide whether to add a per-table
"checkpoint-too-big → evict to pointer" threshold.

### U3. Reader / writer skew during a rolling deploy

Protobuf forward-compatibility means an old reader silently skips new
fields, so an old reader looking at a catalog written with inline ML
would see *no* manifest list for inline snapshots — leading to a
different (worse) failure than a clear version mismatch. The
documentation says "upgrade readers before writers" but we have no
version negotiation, no minimum-reader-version field, and no test that
proves what an old reader does.

**Trigger:** before a multi-process deployment, decide whether to add a
`min_reader_version` field to `Checkpoint` and gate it.

### U4. Behaviour under storage provider edge cases

We exercise S3, GCS, and ADLS conditional writes in integration tests,
but only for the conditions we know to test (basic CAS, append). We
have not exercised:

- ETag changes that aren't object replacements (provider-side metadata
  rewrites — does Azure's HNS rename change ETag?);
- region-failover redirects mid-write;
- conditional appends that succeed at the storage layer but return a
  partial offset (does any provider do this?);
- AppendBlob block boundary effects on the 50k-block hard limit.

**Trigger:** the first time a real workload sees an unexpected commit
failure on a specific provider.

### U5. `InlineSnapshot.equals` semantics interact with retention

`InlineSnapshot.equals` compares only scalar identity, not the manifest
list. This is load-bearing for `Set<Snapshot>` membership in retention /
expiration code. The behaviour is correct for the cases we tested, but
we have not exhaustively walked all upstream Iceberg code that builds
sets of snapshots — there could be a path that legitimately wants
manifest-list equality.

**Trigger:** any retention or cleanup bug report that involves an
inline-ML table.
