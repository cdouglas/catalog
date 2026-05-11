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

### D4. `RewriteTablePathUtil` does not handle inline-ML tables

`core/.../RewriteTablePathUtil.java:252,280` calls `ManifestLists.write()`
unconditionally and dereferences `snapshot.manifestListLocation()` with
no null check. Running the path-rewrite migration utility on an
inline-ML table NPEs. Pre-existing in upstream; flagged here so users
who hit it mid-migration can recognize it.

### D8. Typed-protobuf encoding for Iceberg structural types (skip JSON serde)

The `bytes` fields in our wire format that carry upstream Iceberg objects
— full `TableMetadata` (in `gzip` codec mode), `Schema`, `PartitionSpec`,
`SortOrder` — are JSON today because Iceberg's only public serialization
for those types is JSON. The catalog file is otherwise protobuf, with
structured encodings for snapshots (`AddSnapshotUpdate` / `CompactSummary`)
that we own, plus a binary-structural codec for `TableMetadata` itself
(see [SPEC_TM.md](SPEC_TM.md) §"Inline-TM codecs"). The remaining JSON
payloads live in the rare-update path: `AddSchemaUpdate`,
`AddPartitionSpecUpdate`, `AddSortOrderUpdate`.

Replacing JSON with typed protobuf in those updates would (a) skip JSON
parsing on the read path and (b) shrink the bytes we store. Constructor
visibility audit:

| Type | Visibility | How we'd build one |
|---|---|---|
| `TableMetadata` | package-private | factory in our `org.apache.iceberg` package (same trick as `InlineDeltaSnapshots`) |
| `BaseSnapshot`, `SnapshotLogEntry`, `MetadataLogEntry` | package-private | same |
| `Schema` | public | direct `new Schema(...)` |
| `PartitionSpec` | **`private`** | public `Builder` API |
| `SortOrder` | **`private`** | public `Builder` API |

No additional Iceberg fork needed beyond the package-peer factory. The
hard part isn't constructors — it's the recursive `Type` system
(`StructType` of `ListType` of `MapType` of …, plus parameterized
primitives like `DecimalType(precision, scale)`). A protobuf grammar
mirroring `org.apache.iceberg.types.Types` is ~300-500 lines of
encoder + decoder.

For the delta-update path the benefit is marginal — `AddSchemaUpdate` /
`AddPartitionSpecUpdate` / `AddSortOrderUpdate` fire only when those
types change, which is rare. The dominant cost (full `TableMetadata`
JSON in `inline_tables.metadata`) is already addressed by the
`structural` codec.

**Trigger to revisit:** wide-table workloads where individual
schema/spec change records dominate; profiling shows JSON parse on the
rare-update path is non-negligible.

## Test Coverage Gaps

### T3. Cloud integration tests rely on emulators for some behaviour

`mvn verify` runs the S3, ADLS, and GCS integration tests against
Testcontainers emulators by default (MinIO, Azurite, fake-gcs-server),
falling back to live cloud when `AWS_ACCESS_KEY_ID`,
`AZURE_SAS_CREDENTIALS_FILE`, or `GOOGLE_APPLICATION_CREDENTIALS` is
present. `CloudMode#assumeRealCloud` skips individual tests when the
configured emulator does not implement the feature being exercised, and
each skip is paired with a canary in `*EmulatorCanaries` that asserts
the gap is still present. When an emulator image bump closes a gap, the
matching canary fails and the upgrader knows to delete the workaround.

**Residual gaps guarded by canaries:**

- **MinIO**: no S3 directory-bucket semantics. APPEND-mode commits
  (`maxAppendCount > 0`) skip on MinIO; `MinIOEmulatorCanaries` asserts
  `writeOffsetBytes` is still rejected and that `If-Match` preconditions
  are still honoured (catches silent-ignore regressions).
- **Azurite**: `DataLakeFileClient.uploadWithResponse` returns HTTP 400
  on a non-existent path; real ADLS auto-creates the file. Every test in
  `ADLSCatalogTest` / `ADLSFileIOCatalogTransactionTests` writes the
  initial catalog file via that path, so on Azurite the whole class
  skips. `AzuriteEmulatorCanaries#azuriteRejectsUploadToNonExistentPath`
  asserts the gap; the positive
  `AzuriteEmulatorCanaries#azuriteEnforcesIfMatchPrecondition` guards
  against silent `If-Match` regressions.
- **fake-gcs-server**: standard-bucket CAS path works.
  `FakeGcsEmulatorCanaries` carries a positive canary for
  `ifGenerationMatch` enforcement. The GCS Rapid stage-and-move path
  (`blobAppendableUpload` + `moveBlob` with dual generations) is not
  exercised by `GCSCatalogTest`, so no negative canary is needed today.

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
