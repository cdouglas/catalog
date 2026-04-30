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

### S2. `BaseTransaction.TransactionTableOperations` does not forward `ManifestListSink`

For multi-table catalog transactions, `BaseTransaction` wraps the
underlying `TableOperations` in `TransactionTableOperations`. That
wrapper does not implement (or forward) `ManifestListSink`, so
`SnapshotProducer.apply()` falls back to writing a transient
`snap-*.avro` *before* the catalog commit. The catalog then reads that
file back via `allManifests(io)` in `commitTransaction` to reconstruct
the ML delta into the single atomic intention record.

The end state on reload is correct (the intention record itself is
atomic), but:

- a crash between the Avro write and the catalog commit orphans the
  Avro file;
- a checkpoint that lands in that window captures no record of the
  Avro, making cleanup harder;
- it violates the spirit of I1 even though the intention record is a
  single atomic write.

**Fix path:** add sink-capability forwarding in
`BaseTransaction.TransactionTableOperations` (iceberg-core change), have
`SnapshotProducer`'s sink-detection walk through the wrapper, and
simplify `FileIOCatalog.commitTransaction` to drain the sink directly.
Extend `TestInlineManifestEndToEnd#commitTransactionWithML` to assert no
`snap-*.avro` is written.

### S3. The `Configurable` Hadoop interface lives on `FileIOCatalog`

`FileIOCatalog` implements Hadoop's `Configurable` (`Configuration conf`)
because some Iceberg client paths still call it. The field is marked
`// TODO: delete` in the source. It is dead weight today; remove once
the call sites are gone.

## Deferred Functionality

### D2. Strict mixed-mode rejection

`wrapInlineManifests` currently logs a warning when a table has both
inline and pointer-mode snapshots. Production deployments should be
able to opt into hard rejection.

**Fix path:** thread the existing `fileio.catalog.inline` property (or
add `fileio.catalog.inline.strict`) through `FileIOCatalog` to the
loader; on strict mode throw `ValidationException` rather than logging.
Add a test with a hand-constructed mixed-mode `CatalogFile` covering
both behaviours.

### D3. Pointer-mode eviction of an inline-ML table does not materialize Avro manifest lists

If an inline TM falls back to pointer mode (size eviction), any snapshot
with inline ML keeps its manifest data in the checkpoint. Pointer-mode
eviction writes `metadata.json`, not `snap-*.avro`. Snapshots that need
an Avro manifest list at read time would have to materialize one
on demand, which is not implemented today.

**Trigger to revisit:** if a real workload hits the path. Until then,
inline ML implies the table stays inline.

### D4. `RewriteTablePathUtil` does not handle inline-ML tables

`core/.../RewriteTablePathUtil.java:252,280` calls `ManifestLists.write()`
unconditionally and dereferences `snapshot.manifestListLocation()` with
no null check. Running the path-rewrite migration utility on an
inline-ML table NPEs. Pre-existing in upstream; flagged here so users
who hit it mid-migration can recognize it.

## Test Coverage Gaps

### T1. `CherryPickOperation` and `BaseRowDelta` are not exercised end-to-end

`TestInlineManifestEndToEnd.TmMlTests` covers `FastAppend`,
`MergeAppend`, `BaseOverwriteFiles`, `BaseReplacePartitions`,
`StreamingDelete`, `BaseRewriteManifests`, and `expireSnapshots`. The
remaining `SnapshotProducer` subclasses — `CherryPickOperation` and
`BaseRowDelta` — are exercised at the codec level only, not through a
real `SnapshotProducer.commit()`.

**Fix path:** add cherry-pick (stage append on side branch, cherry-pick
to main, reload, assert ML round-trips) and rowDelta (data + position
delete, reload, assert delete-manifest round-trips through the pool).

### T2. Cloud integration tests don't run in inline-ML mode

`TestS3Catalog`, `GCSCatalogTest`, and `ADLSCatalogTest` set neither
`fileio.catalog.inline=true` nor `fileio.catalog.inline.manifests=true`.
The cloud suite runs in pointer mode only. Provider-specific
conditional-write quirks could interact with inline-ML semantics in
ways not covered today.

**Fix path:** parameterize each cloud test by `InlineConfig` (BASELINE /
TM_ONLY / TM_ML), as already done for the in-memory end-to-end tests.

### T3. Cloud integration tests require manual emulator setup

S3, ADLS, and GCS integration tests are skipped without credentials.
We rely on developer-run emulators; CI coverage is best-effort.

**Fix path:** add Testcontainers lifecycle for LocalStack (S3), Azurite
(ADLS), and fake-gcs-server (GCS) under the `verify` Maven profile.
Gate container startup behind a profile flag so `mvn test` stays fast
and offline.

### T4. `CatalogTests` and `CatalogTransactionTests` are duplicated

The abstract suites are duplicated between
`iceberg/core/src/test/` and `fileio-catalog/src/test/`. The copies have
already diverged (verified 2026-04-22: this project's `CatalogTests`
lacks imports and assertions added upstream).

**Fix path (pick one):**
1. Publish iceberg's test-jar and consume it (current intended approach
   — needs the publish step wired into the Gradle task).
2. Extract `catalog-tests` into a standalone published module.
3. Add a CI check that diffs the two files and fails on drift.

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
