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

### D9. Statistics-file changes force full mode (perf, not correctness)

`InlineDeltaCodec.computeDelta` returns `null` whenever a stats file
changed (the `statisticsChanged` check around line 784). Any commit that
runs `setStatistics` in addition to a real change pays full-mode bytes,
defeating the delta-mode benefit on tables that maintain stats. Adding
`AddStatistics` / `RemoveStatistics` delta types is straightforward; the
JSON parsers are already in upstream Iceberg.

**Trigger to revisit:** when a stats-maintaining workload shows up where
the full-mode size penalty matters.

### D10. ADLS CAS does not work with vended SAS credentials

ADLS CAS renames a staged temp onto the catalog file, and under SAS
auth the rename must carry the SAS in `x-ms-rename-source`. The Azure
SDK adds it only for clients built with a SAS token (`adls.sas-token.*`
in `AzureProperties`); SAS added by `VendedAzureSasCredentialPolicy`
(REST-catalog credential vending) reaches the request URL but not the
rename source, so the rename fails with 403. Shared key and OAuth are
unaffected. Building the rename source from the vended token would fix
it.

**Trigger:** the first deployment that serves a `FileIOCatalog` on ADLS
through vended credentials.

### D8. Typed-protobuf encoding for Iceberg structural types (skip JSON serde)

`AddSortOrderUpdate` is encoded structurally (see SPEC_TM.md §"Other
Updates"); the apply path synthesizes JSON only as the boundary into
`SortOrderParser`. `AddSchemaUpdate` and `AddPartitionSpecUpdate` remain
gzip-wrapped JSON. The split is driven by the size of the grammar each
type carries — sort fields are a closed 8-kind transform vocabulary and
a pair of two-value enums; schemas drag the full recursive `Type` system.

A typed encoder for the remaining two would (a) skip the JSON parse +
gunzip on the read path and (b) shrink the bytes further for small
payloads where gzip has little to compress. Constructor visibility
audit:

| Type | Visibility | How we'd build one |
|---|---|---|
| `TableMetadata` | package-private | factory in our `org.apache.iceberg` package (same trick as `InlineDeltaSnapshots`) |
| `BaseSnapshot`, `SnapshotLogEntry`, `MetadataLogEntry` | package-private | same |
| `Schema` | public | direct `new Schema(...)` |
| `PartitionSpec` | **`private`** | public `Builder` API |

The hard part isn't constructors — it's the recursive `Type` system
(`StructType` of `ListType` of `MapType` of …, plus parameterized
primitives like `DecimalType(precision, scale)`). A protobuf grammar
mirroring `org.apache.iceberg.types.Types` is ~300-500 lines of
encoder + decoder, plus the ongoing burden of mirroring every new
upstream type (v3 row-lineage, future variant / geometry, etc.).
`PartitionSpec` is in the middle tier: its transform vocabulary
matches the one already implemented for sort order, so a typed encoder
is mostly mechanical — but it carries a `field_id` per partition field
and a `last_assigned_partition_id` that need careful handling.

For the delta-update path the marginal benefit over gzip(JSON) is small
— these updates fire only when those types change, which is rare. The
dominant cost (full `TableMetadata` JSON in `inline_tables.metadata`)
is already addressed by the `structural` codec.

**Trigger to revisit:** when we decide to upstream / drop the iceberg
fork — at that point the package-peer factory becomes load-bearing
elsewhere and a typed encoder removes it from the wire format
altogether. PartitionSpec is the cheaper of the two to lift next if a
workload motivates it independently.

## Test Coverage Gaps

### T3. Cloud integration tests rely on emulators for some behaviour

`mvn verify` runs the S3, ADLS, and GCS integration tests against
Testcontainers emulators by default (versitygw, Azurite, fake-gcs-server),
falling back to live cloud when the provider's env vars are set.
`mvn verify -Preal-cloud` requires live cloud and fails fast if any
required env var is missing. See [INTEGRATION_TESTING.md](INTEGRATION_TESTING.md)
for the env-var contract and the
[`IntegTestEnv`](../src/test/java/org/apache/iceberg/io/IntegTestEnv.java)
util for the constants.

`CloudMode#assumeRealCloud` skips individual tests when the configured
emulator does not implement the feature being exercised, and each skip
is paired with a canary in `*EmulatorCanaries` that asserts the gap is
still present. When an emulator image bump closes a gap, the matching
canary fails and the upgrader knows to delete the workaround.

**Residual gaps guarded by canaries:**

- **versitygw** (replaced MinIO, whose images stopped being published and
  were removed from Docker Hub in 2026-09): no S3 directory-bucket
  semantics; `writeOffsetBytes` is accepted and ignored (the object is
  overwritten). APPEND-mode commits (`maxAppendCount > 0`) skip on
  versitygw; `VersityGwEmulatorCanaries` asserts the gap, and positive
  canaries check that `If-Match` / `If-None-Match: *` are enforced and that
  concurrent conditional PUTs have exactly one winner. No S3 emulator
  implements S3 Express append (checked 2026-10: versitygw, SeaweedFS and
  LocalStack ignore or lack it; RustFS rejects it with 501).
- **Azurite** (3.37.0): no Data Lake endpoint. `ADLSFileIO` keeps the
  catalog file as an Append Blob: APPEND is one conditional
  `AppendBlock`, and CAS stages a temp Append Blob and renames it onto
  the catalog file through the Data Lake API, which Azurite answers with
  HTTP 400. Every test in `ADLSCatalogTest` /
  `ADLSFileIOCatalogTransactionTests` starts with a CAS (creating the
  catalog file), so on Azurite the whole class skips.
  `AzuriteEmulatorCanaries#azuriteRejectsDataLakeRename` asserts the gap;
  the positive `azuriteEnforcesAppendBlockConditions` checks that a stale
  `If-Match` and a wrong append position are rejected, the conditions the
  APPEND path relies on. The ADLS suites run against real ADLS only
  (`AZURE_STORAGE_ACCOUNT` etc., see INTEGRATION_TESTING.md).
- **fake-gcs-server** (1.56.1): standard-bucket CAS path works.
  `GCSFileIO` writes each CAS as one multipart upload (`storage.create`
  with `ifGenerationMatch`), which fake-gcs-server has always enforced.
  Before 1.55.0 it ignored `ifGenerationMatch` on resumable uploads
  (`BlobWriteSession`, `WriteChannel`), the path `GCSFileIO` used until
  iceberg `e0bc582f7`, so the six conflict tests in each `GCSCatalogTest*`
  class failed on the emulator. `FakeGcsEmulatorCanaries` checks
  `ifGenerationMatch` on both kinds of upload. Zonal (Rapid) buckets take
  a single XML API PUT instead; fake-gcs-server has no zonal semantics,
  so that path is covered by iceberg's `GcsRapidCanaries` and
  `GcsFileIOAtomicRapidTest` against a real Rapid bucket (`RAPID_BUCKET`).

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

### T5. `catalog.proto` is a schema-of-record with no drift-detection oracle

`src/main/proto/catalog.proto` documents the on-disk wire format in
protobuf IDL, but the build does not compile it. All encode/decode is
hand-rolled in `ProtoCodec.java` and `InlineDeltaCodec.java` and stays
byte-compatible with what `protoc`-generated code would produce by
convention only. Field numbers, wire types, oneof membership, and
message structure must agree between the `.proto` and the Java
constants; today nothing fails the build if they diverge. The header
comment in `catalog.proto` calls this out, but the comment is not a
test.

A proportionate fix is a test-scope dependency on `protobuf-java`
plus a CI-time validation oracle: feed a corpus of catalog files
through both encoders, assert they produce identical bytes, and have
the test framework parse `catalog.proto` so a missing field surfaces
at test time. Estimate: ~30 minutes to wire up, ~50 MB of
test-only dependency weight, no impact on runtime artifacts. The
runtime stays lean; only CI carries the toolchain.

**Trigger to revisit:** the first time we ship a wire-format change
that compiled-tool consumers (rather than the hand-rolled codec) need
to read, or the first time review misses a drift between `.proto` and
Java field numbers.

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

### U4. Behavior under storage provider edge cases

We exercise S3, GCS, and ADLS conditional writes in integration tests,
but only for the conditions we know to test (basic CAS, append). We
have not exercised:

- ETag changes that aren't object replacements (provider-side metadata
  rewrites). Answered for Azure's HNS rename, which ADLS CAS relies on:
  the renamed file keeps the source's ETag, and the rename response
  carries it (checked 2026-10-14), so a CAS learns the new ETag without
  a HEAD that could see a later writer;
- region-failover redirects mid-write;
- conditional appends that succeed at the storage layer but return a
  partial offset (does any provider do this?);
- AppendBlob block boundary effects on the 50k-block hard limit
  (`BlockCountExceedsLimit`; see iceberg `TOO_MANY_PARTS.md`). Every
  ADLS APPEND is one block, so a catalog file takes at most 50,000
  appends between compactions.

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
