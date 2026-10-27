# Errata

Current shortcuts, deferred functionality, known bugs, and known unknowns
in `FileIOCatalog` and the inline manifest-list extension. Items are
grouped by what they are; within each group, ordering is suggestive, not
strict.

For invariants that must not regress, see [design.md](design.md). For the
wire format see [SPEC.md](SPEC.md), [ATOMIC_FILE_LOG.md](ATOMIC_FILE_LOG.md)
and [LOG.md](LOG.md). Bugs in Apache Iceberg itself are in
[ICEBERG_ISSUES.md](ICEBERG_ISSUES.md). Upstream fixed C1 (1.11.0), and
the fork's `core-fixes` branch fixes C2-C25, so this project's tests
work around none of them (the workarounds for C2-C8 were dropped in
63dd25e, b82ceb9, dbac28b, 411f73c, e2c6b7b and 890fd8d); `TestIcebergCoreIssues`
asserts each is absent. On stock Iceberg 1.12.0 C2-C25 come back.

An entry's ID is not reused: a fixed entry is deleted or marked
resolved, and its ID retired. Retired: S3, D1-D3, D5-D7, D11, T1, T2,
U3 (`git log -p docs/errata.md` has each). Five IDs were reused before
this rule, so an older code comment or commit citing one may mean the
earlier entry:

- S2: `BaseTransaction.TransactionTableOperations` did not forward
  `ManifestListSink` (fixed 6f30fbb).
- D9: an inline-ML create transaction lost its manifests' partition
  spec ids (fixed 3c614c2: the create carries its manifest-pool delta).
- D10: single-pass rewrite of the committed set (fixed b1af354); then
  ADLS CAS with vended SAS credentials (fixed in the fork, entry dropped
  a8aa0e9).
- T4: the abstract suites were duplicated (fixed 35076d6); then inline
  `CatalogTests` reachability tests (14ac8d2).
- T5: `TestInlineDelta` lacked a reconstruction round trip for
  `addSnapshot` (fixed 07195d5).

## Current Shortcuts

### S1. Inline tables rely on fork changes to `TableMetadata.Builder` and `BaseSnapshot`

`TableMetadata.Builder.replaceSnapshots(Map<Long, Snapshot>)` swaps a
table's snapshots in place: `LogCatalog` uses it to give inline-ML
snapshots their sentinel before an encode (design.md I5) and their
manifest-list files when a table leaves inline storage. It is a 28-line
addition to `TableMetadata.Builder` in the iceberg fork (with a
`snapshotsReplaced` flag wiring it into `hasChanges()`). A load no longer
uses it: `wrapInlineManifests` builds the metadata with its
`InlineSnapshot`s directly (`InlineTableMetadata.create`, which calls
`TableMetadata`'s package-private constructor from this project's
`org.apache.iceberg` package, as `ManifestListFiles` does, with no fork
change). The fork makes two more changes to core for inline tables:

- `TableMetadata.Builder.setLastUpdatedMillis` (17 lines, 6085f96f4)
  lets replay pin the writer's last-updated-ms, which `buildFrom`
  resets and `setRef` and `build` otherwise take from the clock; since
  Iceberg 1.12.0 a delta that only adds a snapshot needs it too
  (design.md I4, Bug 60).
- `BaseSnapshot.equals` accepts any `Snapshot`, not only a
  `BaseSnapshot` (d0b03bc1b), so a `BaseSnapshot` and an
  `InlineSnapshot` of the same snapshot are equal both ways.

These work, but they are builder and equality changes in core that
would have to be upstreamed — or rewritten — before this catalog could
ride a stock Iceberg. For `replaceSnapshots`, the cleaner alternative is
a parser-level hook: detect the
`inline://<snapshotId>` sentinel during snapshot deserialization, look up
pool entries, and construct `InlineSnapshot` directly. **Trigger to
revisit:** any plan to drop the iceberg fork or upstream the changes.

### S2. Catalog transactions use a forked `SupportsCatalogTransactions`

`FileIOCatalog` commits multi-table transactions through the fork's
`SupportsCatalogTransactions` and `BaseCatalogTransaction`, a prototype
of a catalog-transaction API that Apache Iceberg does not have, and the
fork has since changed both:

- `commitTransaction(Map<BaseCatalogTransaction.TableRef, TableMetadata>
  reads, List<TableCommit> commits)` takes, for serializable isolation,
  the metadata of every branch the transaction read, as it read it. The
  catalog checks them again on the state its commit builds on
  (`BaseCatalogTransaction.unchangedSinceRead`) and pins those tables in
  the commit's record. It used to take only the table names, so a change
  that landed between `BaseCatalogTransaction`'s own check and the
  catalog's read went unseen (Bug 21). `TableRef` is public for this.
- `BaseCatalogTransaction` stages a table's updates on the version the
  transaction read, not a refresh of it, so a write to a table that
  changed after the transaction read it fails the commit's requirements
  at either isolation level (Bug 23).
- `BaseCatalogTransaction` knows a table by the identifier it was
  loaded by. It used to recover the identifier from the table's name:
  it removed the catalog's name wherever it occurred and split the rest
  at dots. So a transaction looked up the wrong identifier for a table
  whose namespace repeated the catalog's name, or had a dot in a level
  (Bug 59).

So this catalog is not a drop-in replacement in an existing Iceberg
deployment: anything that implements or calls `commitTransaction` must
use the new signature, and a catalog transaction run against a stock
`BaseCatalogTransaction` keeps all three bugs. `RESTCatalog` accepts the read
set but does not send it, so a REST server sees only the table commits'
requirements and keeps the race Bug 21 closes here.

**Trigger to revisit:** any plan to drop the iceberg fork or upstream
the changes. The read set could travel as requirement-only
`TableCommit`s, which the REST protocol already carries, at the cost of
checking a read branch's snapshot (`AssertRefSnapshotId`) rather than
the whole table.

## Deferred Functionality

### D4. `RewriteTablePathUtil` does not handle inline-ML tables

The path-rewrite migration utility writes a new manifest list file for
each snapshot, and an inline-ML snapshot has none to rewrite. Rewriting
the paths of an inline-ML table is unsupported: evict it to pointer mode
first (a commit through a catalog with `fileio.catalog.inline=false`
does), which writes its manifest lists to files. The fork's
`RewriteTablePathUtil` fails fast with `IllegalStateException`
(`checkSnapshotIsNotInline`, fork 904c23865) on a snapshot without a
manifest list location, in both `replacePaths` and
`rewriteManifestList`.

### D9. Statistics-file changes force full mode (perf, not correctness)

`InlineDeltaCodec.computeDelta` returns `null` whenever a stats file
changed (the `statisticsChanged` check around line 938). Any commit that
runs `setStatistics` in addition to a real change pays full-mode bytes,
defeating the delta-mode benefit on tables that maintain stats. Adding
`AddStatistics` / `RemoveStatistics` delta types is straightforward; the
JSON parsers are already in upstream Iceberg.

**Trigger to revisit:** when a stats-maintaining workload shows up where
the full-mode size penalty matters.

### D10. `SerializableTable` cannot load an inline table's metadata lazily

`SerializableTable` (what Spark and Flink ship to workers) copies a
table's schema, spec, properties, FileIO and location eagerly, but loads
the rest -- `schemas()`, `sortOrders()`, `snapshots()`,
`currentSnapshot()`, `history()`, statistics files, new scans -- by
reading the table's metadata file through `StaticTableOperations`. An
inline table's metadata location is synthetic (`inline://...`): a
table's FileIO gives it a length, for Iceberg's metadata tables (Bug
57), but no content, so those calls fail on a serialized inline table.
Serving the metadata as JSON would work where the table was loaded, but
not on a worker, whose FileIO is the catalog's.

**Trigger to revisit:** a worker-side code path that needs one of the
lazily loaded parts of an inline table.

### D12. A catalog transaction conflicts on each table's whole version

A catalog transaction's record pins the version of each table it writes
and, under serializable isolation, of each table it read. A commit that
lands first on one of those tables bumps its version, and the
transaction fails with `CommitFailedException` (design.md, conflict
matrix). Any change counts, a property change included. The version a
written table must still have is the one the transaction first read,
not the one current when it commits: `BaseCatalogTransaction` passes it
in the read set (with a null ref, under either isolation level), and
`commitTransaction` fails if the table changed since, after checking the
commit's requirements, which say more specifically what changed. Before
Bug 70, a change between the transaction's read and its commit that the
requirements don't name went through: a property change, a commit to
another branch, a tag on a snapshot the transaction expired. This is the
catalog's semantics, and it's stricter than Iceberg's:
- The requirements `UpdateRequirements` derives for a table commit name
  only the table's UUID, the heads of the branches it moves, and the
  schema, spec and sort order it changes.
- Iceberg's REST server retries a table commit whose requirements still
  hold (`CatalogHandlers.commit`), so it lets a concurrent property
  change through.
- Iceberg's spec doesn't say whether a commit depends on the table's
  properties.

**Superseded for single-table commits.** A table commit outside a
catalog transaction need not conflict on the table's whole version: under
the conflict encodings beyond `lamport` (SPEC.md §"Conflict Encodings";
design.md §"The same table, per conflict encoding", I12), replay merges
its record onto a changed table where Iceberg's retry would commit the
same. That is the split "Later" below proposes, finer (concerns, refs,
partitions, a branch's manifests), and it needs no restaging: the record
is an inline delta whose late-bound fields replay finalizes where it
lands (SPEC_TM.md §"Late-bound fields"). A catalog transaction pins every
table it writes (`UpdateTableInline.pinned`), so under every encoding it
conflicts as this section says; a finer witness for it would need the
transaction to declare what it read.

A commit to any other table doesn't fail the transaction: the format's
commit loop rebases a record whose actions still hold. Nothing above the
catalog retries. `BaseCatalogTransaction` doesn't (its "TODO: should
this be retryable?"), so a caller whose transaction failed runs it
again.

The fork's `RESTCatalog` ignores the read set, written tables included:
its server checks only the requirements.

`TestCatalogFaultProperties` checks that an acknowledged transaction's
written tables are the versions it first read; with the check in
`commitTransaction` disabled, it fails within about 1,200 tries.
`TestCatalogRaceProperties` checks that a transaction whose table a
racer changed fails (its racers run within the commit, so the
commit loop's pin catches them too), and counts the transactions that
fail although what Iceberg's isolation checks (the branches they read)
held. In a 200,000-try run, 24 of the 8,469 racing transactions (0.28%)
did.

**Later: depend on part of a table** (for catalog transactions; single
table commits have it, above). A table's version could be split
into its metadata (properties) and the rest, with the intention record
saying which of the two a transaction depends on. A transaction that
depends only on the rest would then survive a concurrent property
change. But the format's commit loop couldn't simply rebase its record.
The record carries the table's new metadata as built on the version it
read: a metadata file, or a full inline copy. Replaying it over the
change would undo the change. So `commitTransaction` would have to
re-apply the commits' updates on a fresh read, as `commitOnAFreshRead`
and `CatalogHandlers.commit` do. Each attempt would make every check
again on its read:

- **The read set:** `unchangedSinceRead` for each branch read.
- **Each commit's requirements:** derived from the version the
  transaction read (Bug 23).
- **`TableMetadata.Builder`:** it rejects a snapshot whose sequence
  number or first row id another commit has since passed.

The retry would need three cautions:
- **Stop on a failed check.** When a requirement or a read check fails,
  stop without retrying, as `CatalogHandlers` does
  (`ValidationFailureException`).
- **Retry only a definite failure.** `CommitStateUnknownException`
  must still propagate. The format's commit loop reports
  `CommitFailedException` only after a read without the record, so a
  landed record is never retried.
- **Stage each attempt on its own read:** build a new `Mut` and call
  `stageUpdate`, which stages inline-ML manifest lists, from scratch.

**Trigger to revisit:** a workload whose multi-table transactions race
property changes on their tables.

### D13. `partition-vector` retries a validated commit that `concern-vector` merges

`partition-vector` judges a validated record (an overwrite, a row delta,
a compaction) by its checks against the partition buckets, which stamp
the files every commit adds or removes on any branch. So a validated
commit on main after a commit to another branch in the same partition
conflicts there, and Iceberg retries it, although main did not move:
`concern-vector` and `blind-append`, which judge that record by the
branch it advances, merge it (design.md, "The same table, per conflict
encoding"; `TestConflictMatrix`). `manifest-window` reads the branch's
own history and merges it too. A validated record could be admitted if
either witness holds: both are stable, so their disjunction is.

**Trigger to revisit:** a workload of validated commits racing commits
to other branches (write-audit-publish) where `partition-vector` is the
chosen encoding.

### D14. `max.append.count` and the race backoff are fixed numbers, not per store

`fileio.catalog.max.append.count` defaults to 100 and
`fileio.catalog.race.retry.min-wait-ms` to 100 ms, both from one store's
latencies as the contention benchmark injects them (endive's S3 Express
profile: a conditional append takes about 21 ms to land and 23 ms to lose;
design.md §"Choosing append vs CAS"). The best values depend on the
store's latencies as a writer sees them, which differ by store and by
where the writer runs: from a cloud VM in the bucket's zone, S3 Express
took about 11 ms an append (90 a second), so 100 ms is about nine appends
there and five in the benchmark; an Azure append that loses its race takes
about 2 s to say so, so 100 ms is too short there; an S3 general-purpose
bucket only replaces. They also depend on the catalog: a reader replays
the records after the checkpoint and reads the whole object, so the tail's
cost grows with the records and the object, while a compaction's cost is
the checkpoint's size and its race with the others' appends.

A principled rule:

- **Backoff by observed latency.** The commit loop times each write it
  sends. After a lost race, wait a time drawn below a bound of a few times
  the latency of the write that lost (or a moving average of the loop's
  writes), doubled with each race the commit loses, with the property as a
  floor and a cap of a few hundred writes' worth. The writers who lost to
  one winner then spread over a few write slots, whatever the store and
  however far the writer is from it.
- **Compact by cost.** Compact when the records after the checkpoint cost a
  reader more than a compaction, amortized over the commits until the next
  one, costs a writer: bytes or replay time of the tail against the
  checkpoint's.

Replay folds a read onto the replica it continues (LOG.md "Replay"), so a
writer that holds a replica replays only the records appended since; a
compaction changes the checkpoint, and every writer then decodes the new
one. That shifts the trade toward compacting less often than the campaign's
100, which was chosen before the fold.

**Trigger to revisit:** a deployment on a store other than S3 Express, or
the contention benchmark's Azure and GCS profiles.

### D15. A read of the catalog object reads all of it

A writer that holds a replica replays only the records appended since it
read (LOG.md "Replay"), but `AtomicFileLog` still GETs the whole object for
every read, read through a write, and refresh. The contention benchmark does
not see this: endive's profiles price a GET by request, not by size. On a
real store a catalog object of several MB (inline manifest lists over
thousands of snapshots, docs/REPLAY_BENCH_RESULTS.md) costs its transfer on
every attempt.

`SharedLog.readAfter(since)` is the SPI for reading only what follows a tail
(`InMemorySharedLog` implements it). On one object, a ranged GET from
`since.bytes()` is sound only if the object is still the one `since` read,
extended by appends: `FileTail.continues` compares lengths, which a replace
that grows the object defeats, and a replace can write a record a reader
already holds (a commit's own record, resent in its replace) at any offset.
A sound design:

- each replace writes a random incarnation id in the object's header
  (format version 3), and each append frames its record with the id of the
  incarnation it extends;
- every append is conditioned on the exact version it extends, as it is
  today on S3 Express (`If-Match` and the write offset) and on Azure (the
  append position and `If-Match`), so a record can land only in the
  incarnation it names;
- a probe reads the header with a ranged GET instead of a HEAD, which gives
  the version, the length and the incarnation in one request, so a resend
  after a lost race names the incarnation it lands in;
- `readAfter(since)` GETs from the start of the last frame `since` holds: if
  those bytes match and the frames after them name `since`'s incarnation,
  it returns the frames after; else it reads the whole object.

**Trigger to revisit:** a deployment whose catalog object grows past a few
MB on a store that appends, or a benchmark profile that prices a GET by its
bytes.

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
| `TableMetadata` | package-private | factory in our `org.apache.iceberg` package: `InlineTableMetadata.create`, which replay's loads already use (same trick as `InlineDeltaSnapshots`) |
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
[`IntegTestEnv`](../iceberg-fileio-catalog/src/test/java/org/apache/iceberg/io/IntegTestEnv.java)
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
  iceberg `8f94c5100` (`e0bc582f7` before the 1.12.0 rebase, on tag
  `atomicio-1.10.1`), so the six conflict tests in each `GCSCatalogTest*`
  class failed on the emulator. `FakeGcsEmulatorCanaries` checks
  `ifGenerationMatch` on both kinds of upload. Zonal (Rapid) buckets take
  a single XML API PUT for CAS and an appendable upload for regular
  writes (manifests, data files), since they reject resumable uploads;
  fake-gcs-server has no zonal semantics, so those paths are covered by
  iceberg's `GcsRapidCanaries` and `GcsFileIOAtomicRapidTest` against a
  real Rapid bucket (`RAPID_BUCKET`), and by running these suites with
  `GCS_TEST_BUCKET` set to one.

### T4. `testRegisterTable` (inline) needs a register-from-bytes API

The inline-TM catalog suites (`TestS3CatalogInlineTM`,
`TestS3CatalogCASInlineTM`, `GCSCatalogTestInlineTM`,
`ADLSCatalogTestInlineTM`, `ADLSCatalogTestCASInlineTM`, and the
`*InlineML` suites that extend them) keep a single `@Disabled` override
on `testRegisterTable`, and `TestLocalCatalog` assumes it away in its
inline-TM modes. The upstream test
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

### T5. `catalog.proto` is a schema-of-record with no drift-detection oracle — RESOLVED

Resolved 2026-10-18 (516e9c4). The build still does not compile
`catalog.proto`, and the codecs stay hand-rolled, but two tests now fail
when the schema and the codecs diverge, with no `protobuf-java`
dependency:

- `CatalogProto` parses `catalog.proto` and checks that a catalog file
  conforms to it: every field declared in its message, with a type of
  the wire type it was written with, a declared enum value, a singular
  field or oneof member at most once, valid UTF-8 in strings.
  `CheckpointRoundTrip` applies it to every catalog file the suites and
  fuzzers write.
- `TestFormatExplorer` checks the codecs' field-number constants
  against `catalog.proto` in both directions, so a field the codecs
  write but the schema lacks, or one the schema declares but no codec
  reads, fails even if no test writes it; and it checks that the format
  explorer's embedded fixtures are the ones the current code generates.

The drift these found when they landed (missing fields and messages,
wrong wire types, packed declarations) is fixed in `catalog.proto`.

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
"checkpoint-too-big → evict to pointer" threshold. Eviction today is by
update size only (`fileio.catalog.inline.max.update.size`): a table whose
deltas fit stays inline however large its metadata and pool grow.

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
- the stores' hard limits on writes per object. Answered (Bug 66): an
  S3 Express object holds 10,000 parts and an ADLS Append Blob 50,000
  blocks, each counting the write that creates the object; the append
  past the limit fails with `SegmentLimitException`, on which a writer
  compacts. The fork's `appendPastTheSegmentLimitFails` checked both
  against the real services (2026-10-19 and 2026-10-21). It is a canary
  for a change in the stores, not a regular check (it makes 10,000 and
  50,000 appends); the catalog's reaction to the exception is tested on
  `InMemoryAtomicFileIO` with a small limit (Bug 66, and the fault and
  multi-writer fuzzers).

**Trigger:** the first time a real workload sees an unexpected commit
failure on a specific provider.

### U5. `InlineSnapshot.equals` semantics interact with retention

`InlineSnapshot.equals` compares only scalar identity, not the manifest
list. This is load-bearing for `Set<Snapshot>` membership in retention /
expiration code. The behaviour is correct for the cases we tested, but
we have not exhaustively walked all upstream Iceberg code that builds
sets of snapshots — there could be a path that legitimately wants
manifest-list equality.

The manifest side of this did hide a bug (Bug 44): expiry's
`ReachableFileCleanup` compares `ManifestFile` objects, and an inline
snapshot's manifests were the catalog's own class, equal only to
itself, so expiry deleted manifests retained snapshots still listed.
Inline snapshots now hold `GenericManifestFile` copies, which are equal
by path as Iceberg expects; `TestCatalogFaultProperties` expires
snapshots of tables with tags, branches and replaced histories, and
checks that every retained snapshot's manifests exist.

**Trigger:** any retention or cleanup bug report that involves an
inline-ML table.
