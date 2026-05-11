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

### D6. Concurrent-replace id collision on an inline-ML table can lose the new ML data — RESOLVED

Resolved 2026-05-10. When `computeDelta` returns null (idCollidesWith-
DifferentContent: concurrent-replace race assigning the same schema /
spec / sort-order id to different content) AND staged ML data is
non-empty for a live snapshot, `commitInline` and `commitTransaction`
now throw `CommitFailedException` instead of silently falling back to
full / pointer mode (both of which dropped the staged ML data). The
producer's retry loop refreshes the base and re-stages on top of the
rebased state, where the colliding id has been reassigned naturally.

Regression test: `TestInlineManifestEndToEnd$TmMlTests
.concurrentReplaceCollisionWithStagedMLThrowsCommitFailed` (verified to
fail without the fix). A companion test
(`concurrentReplaceCollisionWithEmptyStagedMLFallsThrough`) guards
against a D6 false positive on empty staging.

### D7. ML deltas on existing snapshots are silently dropped — RESOLVED

Resolved 2026-05-10. `commitInline` and `commitTransaction` now iterate
`mlDeltas.entrySet()` directly, attaching every staged delta whose
snapshot id is present in `metadata.snapshots()`. The old code filtered
to snapshot ids absent from `base.snapshots()` (the "new snapshots"
set), silently dropping manifest rewrites against existing snapshot
ids. The new filter also drops orphan stages (snapshot ids not present
in the target metadata, e.g. rolled-back producer runs) instead of
attaching pool entries with no referent.

Regression tests: `TestInlineManifestEndToEnd$TmMlTests
.stagedDeltaOnExistingSnapshotIdSurvivesCommit` (verified to fail
without the fix; the manifest list still pointed at the original
manifest path) and `.stagedDeltaForOrphanSnapshotIdIsDropped` (guards
against over-applying orphan stages).

### D3. Pointer-mode eviction of an inline-ML table does not materialize Avro manifest lists

If an inline TM falls back to pointer mode (size eviction), any snapshot
with inline ML keeps its manifest data in the checkpoint. Pointer-mode
eviction writes `metadata.json`, not `snap-*.avro`. Snapshots that need
an Avro manifest list at read time would have to materialize one
on demand, which is not implemented today.

**Trigger to revisit:** if a real workload hits the path. Until then,
inline ML implies the table stays inline.

### D8. Typed-protobuf encoding for Iceberg structural types (skip JSON serde)

The `bytes` fields in our wire format that carry upstream Iceberg objects
— full `TableMetadata`, `Schema`, `PartitionSpec`, `SortOrder` — are
JSON today because Iceberg's only public serialization for those types
is JSON. The catalog file is otherwise protobuf, with structured
encodings for snapshots (`AddSnapshotUpdate` / `CompactSummary`) that we
own.

Replacing JSON with typed protobuf would (a) skip JSON parsing on the
read path and (b) shrink the bytes we store. For a research prototype
where deployments handle reader-version compatibility, the upstream-
slow-evolution constraint is less binding than for a stock Iceberg
catalog. Constructor visibility audit:

| Type | Visibility | How we'd build one |
|---|---|---|
| `TableMetadata` | package-private | factory in our `org.apache.iceberg` package (same trick as `InlineDeltaSnapshots`) |
| `BaseSnapshot`, `SnapshotLogEntry`, `MetadataLogEntry` | package-private | same |
| `Schema` | public | direct `new Schema(...)` |
| `PartitionSpec` | **`private`** | public `Builder` API |
| `SortOrder` | **`private`** | public `Builder` API |

No additional Iceberg fork needed beyond the package-peer factory.
The hard part isn't constructors — it's the recursive `Type` system
(`StructType` of `ListType` of `MapType` of …, plus parameterized
primitives like `DecimalType(precision, scale)`). A protobuf grammar
mirroring `org.apache.iceberg.types.Types` is ~300-500 lines of
encoder + decoder.

#### Per-type cost / benefit

For each candidate, the benefit depends on **frequency** (how often the
encoded bytes get written) and the **JSON-vs-binary ratio**:

| Type | Where it lives | Write frequency | JSON size | Binary est. | Savings worth the encoder? |
|---|---|---|---|---|---|
| Snapshot list (inside TM) | every `inline_tables.metadata` | every CAS commit; long tail accumulates | ~500 B/snapshot × N | ~80-100 B/snapshot × N | **Yes.** Dominant term for active tables (1000 snapshots ≈ 500 KB JSON → 100 KB protobuf). Largely overlaps the `AddSnapshotUpdate` codec we already have — we'd be reusing it for the in-checkpoint storage. |
| `TableMetadata` wrapper (scalars + lists + refs + properties) | every `inline_tables.metadata` | every CAS commit | ~1-3 KB structural + history | ~500 B - 1.5 KB | **Yes.** Cuts checkpoint bytes ~50%; eliminates JSON parse on every `loadTable`. |
| `Schema` (with `Type` system) | inside TM JSON; `AddSchemaUpdate` deltas | every CAS commit (unchanged across most commits, but rewritten as part of TM) | ~400-800 B narrow / 8-15 KB wide | ~50-70% smaller | **Yes for wide tables, marginal for narrow.** The recursive `Type` encoder is the bulk of the work. |
| `PartitionSpec` | inside TM JSON; `AddPartitionSpecUpdate` deltas | every CAS commit (typically 1-2 specs/table) | ~100-300 B each | ~40-60% smaller | **Marginal.** Once Type system exists, encoder is small (~50 lines), but per-table savings are modest. |
| `SortOrder` | inside TM JSON; `AddSortOrderUpdate` deltas | every CAS commit (typically 1 order/table) | ~100-300 B | ~40-60% smaller | **Marginal.** Same shape as PartitionSpec. |

The user's intuition — "if a schema is written once per catalog file
it may not be worth it" — is accurate **for the delta-update path**:
`AddSchemaUpdate` / `AddPartitionSpecUpdate` / `AddSortOrderUpdate`
fire only when those types change, which is rare. Compressing the
delta JSON saves bytes on rare commits.

It is **not** accurate for the inline-TM storage path: the full
`TableMetadata` JSON in `inline_tables.metadata` includes the schema /
specs / sort-orders array unchanged from the previous version, and
that JSON is rewritten on every CAS commit (per `INLINE_TBL_UPDATE.md`
the structural overhead is 90%+ redundant after 10 commits). Compression
helps every commit, even though the schema itself didn't change.

#### Recommended slicing

If we ever do this work, the natural cuts are:

1. **TableMetadata wrapper + snapshot list** — biggest payoff per line of
   code. Reuses `AddSnapshotUpdate` encoding for snapshots in
   checkpoint storage. Schemas / specs / sort-orders stay JSON-as-bytes
   in this slice. Cuts the dominant cost; ~300 lines.
2. **`Type` system + `Schema`** — recursive, careful, but bounded. ~400
   lines. Only worth doing if step (1) lands and wide-table workloads
   matter.
3. **`PartitionSpec` / `SortOrder`** — fall out cheaply once (2) exists
   (~100 lines). Skip if (2) is skipped.

#### When to revisit

- Profiling shows `TableMetadataParser.fromJson` cost is non-negligible
  on `loadTable`-heavy workloads.
- Catalog-file size becomes a write-amplification concern (high-rate
  CAS catalogs, especially on GCS where every commit is a CAS replace).
- Inline-ML matrix is green and we're chasing the next compressibility
  bound from `docs/INLINE_TBL_UPDATE.md`.

Until any of those triggers, the JSON-as-bytes path is correct and the
encoder cost outweighs the benefit.

### D9. Inline-ML create-transaction loses per-manifest `partitionSpecId` — RESOLVED

Resolved 2026-05-10. The scope was larger than originally described:
pre-fix, inline-ML create-with-snapshots was broken under **both**
codecs.

- **gzip**: `TableMetadataParser.toJson(meta)` on an `InlineSnapshot`
  fell into the v1-embedded-manifests branch and wrote only the
  manifest paths as strings; on decode `GenericManifestFile(InputFile,
  /*specId=*/0, snapshotId)` hardcoded spec 0. Single-spec
  accidentally worked; multi-spec produced the originally-tracked
  `specId() == 0` symptom.
- **structural**: `planPaths` emits an `inline://<snapId>` sentinel for
  any snapshot with null `manifestListLocation`, but
  `CreateTableInlineAction.apply` never populated the manifest pool, so
  `wrapInlineManifests` §2.3 threw "catalog state corrupt" on every
  inline-ML create-with-snapshots reload. This breakage was latent in
  COMPAT.md's `OK*` cells for the structural column — the inheriting
  cloud test suites hadn't been re-run since the codec discriminator
  became default on 2026-05-08, and the local suites' inline-ML
  tests use a separate `create()` then `fastAppend()` (which goes
  through the update path, not create-with-snapshots).

Fix implemented:

1. `FileIOCatalog.commitInline isCreate=true` (FileIOCatalog.java:580)
   drains `InlineManifestTableOperations.stagedDeltas`, builds a
   delta of `AddManifestUpdate` entries via
   `InlineDeltaCodec.attachManifestDelta`, and rewrites InlineSnapshots
   → `BaseSnapshot` with `inline://<snapId>` sentinel via
   `TableMetadata.Builder.replaceSnapshots`. The encoded TM is now
   codec-agnostic — both codecs emit sentinel-shaped snapshots.
2. `CreateTableInlineAction` (ProtoCodec.java:2447) gained an optional
   `deltaBytes` payload (wire field 8). On apply, after
   `addInlineTable`, the action calls
   `InlineDeltaCodec.applyDeltaWithManifests` — the same hook the
   update path uses — to populate the per-table manifest pool and
   per-snapshot ref list atomically with the create.
3. New `Mut.createTableInlineWithManifests(table, bytes, codec, delta)`
   (CatalogFile.java) wires the staged delta from `FileIOCatalog`
   through `ProtoCatalogFormat.Mut.buildActions` to the action.

Regression tests:
`TestInlineManifestEndToEnd$CodecAxisTests.tmMlCreateTransactionSingleSpecRoundTrip`
and `.tmMlCreateTransactionMultiSpecRoundTrip` — each parameterized
across `gzip` and `structural`. All four cases verified to fail
without the fix.

### D4. `RewriteTablePathUtil` does not handle inline-ML tables

`core/.../RewriteTablePathUtil.java:252,280` calls `ManifestLists.write()`
unconditionally and dereferences `snapshot.manifestListLocation()` with
no null check. Running the path-rewrite migration utility on an
inline-ML table NPEs. Pre-existing in upstream; flagged here so users
who hit it mid-migration can recognize it.

### D10. Single-pass rewrite of `committedTransactionsBytes` — RESOLVED

Resolved by `ProtoCodec.rewriteCommittedBytes`, called from
`ProtoCatalogFile.Builder.materializeCommittedBytes(long)` and from
`ProtoCodec.encodeCheckpoint` on the CAS / compaction path. The merge
streams the inherited `packed_entries` once, two-pointer-merging the
sorted additions and emitting a merged descending stream — the same
loop applies the D11 GC drop. See `docs/GC.md` Phase B.

### D11. Committed-set GC (watermark-based retention) — RESOLVED

Implemented per `docs/GC.md`. The format property
`fileio.catalog.committed.retention.ms` (default 6 hours) sets the
window; on each CAS / compaction commit the writer GCs inherited entries
whose UUIDv7 timestamp is at or below `now - retentionMs` and persists
the largest dropped timestamp in
`CommittedTransactions.highest_dropped_timestamp_ms`.
`ProtoCatalogFile.containsTransaction` is now tri-state: `true`,
`false` (presumed abort, ts above horizon), or
`DedupHorizonExceededException` (UNKNOWN, ts at or below horizon).
`Long.MAX_VALUE` retention disables GC; `0` retention drops everything
older than the current commit.

## Test Coverage Gaps

### T2. ADLS inline matrix complete (2026-05-07)

ADLS now runs the full `inline=true` matrix matching S3/GCS:
`ADLSCatalogTest{,CAS}Inline{TM,ML}` and
`ADLSFileIOCatalogTransactionTests{,CAS}Inline{TM,ML}`. All cells
green except for the deferred D9 outlier
(`testCompleteCreateTransactionMultipleSchemas`).

### T3. Cloud integration tests require manual emulator setup

S3, ADLS, and GCS integration tests are skipped without credentials.
We rely on developer-run emulators; CI coverage is best-effort.

**Fix path:** add Testcontainers lifecycle for LocalStack (S3), Azurite
(ADLS), and fake-gcs-server (GCS) under the `verify` Maven profile.
Gate container startup behind a profile flag so `mvn test` stays fast
and offline.

### T4. `testRegisterTable` (inline) needs a register-from-bytes API

`TestS3CatalogCASInlineTM` and `GCSCatalogTestInlineTM` keep a
single `@Disabled` override on `testRegisterTable`. The upstream test
calls `catalog.dropTable(t)` then `catalog.registerTable(t, oldLoc)`
with the previously-loaded metadata location string. After drop, the
inline metadata bytes are gone, and there is no `registerTable(name,
bytes)` API on `FileIOCatalog` to re-inline them. Two options:

1. Add a `registerInlineTable(TableIdentifier, byte[])` overload
   surfaced through `FileIOCatalog`. The upstream `Catalog` interface
   only exposes the location-string variant, so callers would need to
   downcast — acceptable for inline-aware tools.
2. Stash a side copy of the dropped inline bytes in `FileIOCatalog`
   keyed by metadata location, so the register-by-location path can
   look them up. Smells like a leak waiting to happen; (1) is cleaner.

**Trigger to revisit:** when a real workload needs catalog
re-registration of a previously-dropped inline table.

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
