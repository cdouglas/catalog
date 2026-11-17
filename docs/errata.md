# Errata

Pending and deferred work in `FileIOCatalog` and the inline manifest-list
extension: shortcuts the implementation takes, functionality it defers, gaps
in its tests, and what is not yet measured. An entry leaves this file when its
work is done; the design docs describe what the work built. Within each group,
ordering is suggestive, not strict.

For invariants that must not regress, see [design.md](design.md). For the
wire format see [SPEC.md](SPEC.md), [ATOMIC_FILE_LOG.md](ATOMIC_FILE_LOG.md)
and [LOG.md](LOG.md). Bugs in Apache Iceberg itself are in
[ICEBERG_ISSUES.md](ICEBERG_ISSUES.md): upstream fixed C1 (1.11.0) and the
fork's `core-fixes` branch fixes C2-C29, so this project's tests work around
none of them. `TestIcebergCoreIssues` asserts each is absent; on stock Iceberg
1.12.0 C2-C29 come back.

An entry's ID is not reused: a finished entry is deleted and its ID retired.
Retired: S3-S5, D1-D3, D5-D7, D9, D10, D11, D15, D17, D19, D20, D23, D24, D26, T1, T2, T4, T5,
U2, U3
(`git log -p docs/errata.md` has each; the plans in TAIL_READS.md,
CHECKPOINT_PLAN.md and SNAPSHOT_PAGING.md cite them by ID and summary). Five
IDs (S2, D9, D10, T4, T5) were reused before this rule, so an older code
comment or commit citing one may mean an earlier entry.

## Current Shortcuts

### S1. Inline tables rely on fork changes to `TableMetadata.Builder` and `BaseSnapshot`

`TableMetadata.Builder.replaceSnapshots(Map<Long, Snapshot>)` swaps a
table's snapshots in place: `LogCatalog` uses it to give inline-ML
snapshots their sentinel before an encode (design.md I5) and their
manifest-list files when a table leaves inline storage. It is a 28-line
addition to `TableMetadata.Builder` in the iceberg fork (with a
`snapshotsReplaced` flag wiring it into `hasChanges()`). A load does not
use it: `wrapInlineManifests` builds the metadata with its
`InlineSnapshot`s directly (`InlineTableMetadata.create`, which calls
`TableMetadata`'s package-private constructor from this project's
`org.apache.iceberg` package, as `ManifestListFiles` does, with no fork
change). The fork makes three more changes to core for inline tables:

- `TableMetadata.Builder.setLastUpdatedMillis` (17 lines, 6085f96f4)
  lets replay pin the writer's last-updated-ms, which `buildFrom`
  resets and `setRef` and `build` otherwise take from the clock; a delta
  that only adds a snapshot needs it too (design.md I4, Bug 60).
- `BaseSnapshot.equals` accepts any `Snapshot`, not only a
  `BaseSnapshot` (d0b03bc1b), so a `BaseSnapshot` and an
  `InlineSnapshot` of the same snapshot are equal both ways.
- `SerializableTable` ships a table whose metadata location is not a
  `metadata.json` with its metadata as JSON and its `InlineSnapshot`s'
  manifests (0f44056ccf), which a worker rebuilds with
  `replaceSnapshots`. Stock Iceberg loads a shipped table's metadata
  from its file, which an inline table does not have, so on it a Spark or
  Flink worker cannot read one (`TestSerializableInlineTable`).

These work, but they are builder and equality changes in core that
would have to be upstreamed — or rewritten — before this catalog could
ride a stock Iceberg. For `replaceSnapshots`, the cleaner alternative is
a parser-level hook: detect the
`inline://<snapshotId>` sentinel during snapshot deserialization, look up
pool entries, and construct `InlineSnapshot` directly. **Trigger to
revisit:** any plan to drop the iceberg fork or upstream the changes.

### S2. Catalog transactions use an API Iceberg does not have

`FileIOCatalog` commits multi-table transactions through `SupportsCatalogTransactions`,
`CatalogTransaction` and `BaseCatalogTransaction`, a catalog-transaction API that Apache Iceberg
does not have, which `core-perf` (the Iceberg every catalog in catalog-bench runs) holds, so
`JdbcCatalog` and `RESTCatalog` implement it too (docs/CATALOG_TRANSACTIONS.md). It grew from
upstream's prototype (apache/iceberg#6948, closed unmerged) and differs from it:

- `commitTransaction(CatalogCommit)` takes the transaction's net effect on the catalog as it found
  it (namespace changes; table updates staged on the version first read, renames and drops; tables
  created) and, under serializable isolation, what it read. The prototype took only table commits
  and a set of table names: drops and renames went straight to the catalog, each its own commit,
  a table's `newTransaction()` committed directly, and a change landing between the client's check
  of a read and the catalog's commit went unseen (Bug 21).
- A serializable transaction reads what it scanned (a ref's rows under the scan's filter, as of
  the snapshot it named) and what it declares (`declareRead`), not every table it loaded; a
  read-only transaction's commit checks its reads.
- `BaseCatalogTransaction` stages a table's updates on the version the transaction read (Bug 23),
  knows a table by the identifier it was loaded by (Bug 59), and stages partition statistics with
  the rest (Bug 91).

So this catalog is not a drop-in replacement in an existing Iceberg deployment: anything that
implements or calls `commitTransaction` must use this API. `RESTCatalog` commits snapshot-isolation
transactions of table updates only, each checked by its requirements, and refuses serializable
ones and catalog changes: the REST protocol carries neither reads nor them.

**Trigger to revisit:** any plan to drop the iceberg fork or upstream the API. The read set could
travel as requirement-only `TableCommit`s, which the REST protocol already carries, at the cost of
checking a read branch's snapshot (`AssertRefSnapshotId`) rather than its rows.

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

### D12. A transaction's other updates conflict on the table's whole version

A catalog transaction's update of an inline table that adds snapshots and
moves branches to them goes as a delta from the version the transaction first
read, unpinned, and the catalog's conflict encoding judges it where the record
lands, as it does a table commit's: it commits where a retry of the update
would commit the same. An append to an inline-ML table merges onto a branch
another writer moved; an inline-TM table's manifest list is a file built on
the branch's old head, so there it merges only past changes that leave the
branch alone (`TestLocalCatalogTransaction.aRacedAppendCommitsWhereARetryWould`).
Iceberg's REST server fails such a transaction on the raced table's
requirements.

Every other update of a table a transaction writes is pinned and applies only
on the version the transaction first read: a pointer table's, one that goes as
full metadata (one past the inline size), and a
schema, partition-spec or ref change, which Iceberg commits only on the
version it read. Applying those to a changed version, checked only by the
requirements Iceberg derives, as its REST server does, loses what the change
did (Bug 70): another writer's property change that a schema update's name
mapping rewrote, or a tag on a snapshot the transaction expired. Under
serializable isolation the record names the branches the transaction scanned of
each table (a read witness, SPEC.md §"Commits"), which the encoding judges by
what changed of them; a branch read with no snapshot, and a declared read, hold
the table's version (a read pin). A scan read is judged by its ref, not yet by
its filter (below).

So there the catalog is stricter than Iceberg's REST semantics: a concurrent
property change, or a commit to another branch, fails a transaction whose
requirements still hold. The fork's `CatalogTransactionTests` names those
cases (`conflictsOnTableVersions`: a pointer table's; and
`conflictsOnTableVersionsForCommitOnceUpdates`: a branch the transaction
creates, in every mode). Its failure cases race real conflicts, which every
catalog rejects.

A transaction is not retried: `BaseCatalogTransaction` doesn't (its "TODO:
should this be retryable?"), and a rejected delta is a real conflict. A
caller whose transaction failed runs it again. The fork's `RESTCatalog`
ignores the read set: its server checks only the requirements.

Deferred:

- **Commit-once updates judged by what they change.** A ref change by the ref
  and the snapshot it names, a schema change by the schema, through the
  encodings' concern stamps (`TableChange`) rather than the whole version:
  what REST's requirements check, without Bug 70's losses (the properties
  concern catches the name mapping). The record would say an update is a
  transaction's, so a table commit's stays pinned as Iceberg commits it.
- **Scan reads judged by their filter.** A scan read is unchanged where nothing
  matching its filter was added to or removed from its ref; the encodings that
  judge validated writes by their filters (`partition-vector`,
  `manifest-window`) could judge it the same way, so an append to another
  partition would not fail it. The rebase oracle for reads judges that change
  as it judges the ref's: a serializable transaction's scan lands after commits
  it did not see, and if admitted must read the same rows under its filter
  where it lands (`TestRebaseProperties.anAdmittedReadSeesTheRowsItRead`). Its
  statistics count, per encoding, the reads rejected whose rows had not
  changed: the false positives judging by filter would remove.
  `fuzz/mutants/reads.sh` calibrates it, `readfilter` among its mutants: a
  filter judged by the data files added alone, which misses delete files,
  removals and resets.
- **The rebase oracle for transactions.** `TestRebaseProperties` lands table
  commits, and transactions' scan reads, after a window of commits they did not
  see; a transaction's deltas are checked only by the race fuzzer's serial run
  and the fault fuzzer's model, both of whose transactions append.

**Trigger to revisit:** a workload whose transactions change refs, schemas or
partition specs while other commits race them. For scan reads judged by their
filter: E5r (catalog-bench `configs/e5r-*.json`, not yet run), whose arm that
scans the last closed partition (`read_slot` 1) meets no real conflict: every
read its serializable FileIOCatalog runs reject is a false one, by an append to
the current partition (which judging by filter would commit) or by a compaction
of the scanned one (which it would commit only if it also judged a rewrite by
the rows it keeps).

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

### D14. `max.append.count` and compaction's draws are fixed numbers, not per store

`fileio.catalog.max.append.count` defaults to 1,000, provisional, chosen on one
store's latencies as the benchmarks inject them (S3 Express: endive's profile,
where a conditional append takes about 21 ms to land and 23 ms to lose, and
catalog-bench's measured `s3x-2026-11`; design.md §"Choosing append vs CAS").
The best value depends on the store and on the catalog. A reader that holds a
replica reads and folds only the records appended since (LOG.md "Replay"), and
a record is about 200 bytes, so the count bounds a cold reader's whole read and
how often every writer decodes a new checkpoint; a compaction's cost is the
checkpoint's size and its race with the others' appends. The wait after a lost
race follows the writes' latency as the writer observes it (LOG.md "The commit
loop"), so it needs no per-store setting.

A principled rule: **compact by cost.** Compact when the records after the
checkpoint cost a reader more than a compaction, amortized over the commits
until the next one, costs a writer: bytes or replay time of the tail against
the checkpoint's.

A writer compacts after its commit, at a point past the limits it draws,
backing off as its compactions lose (TAIL_READS.md "Compaction after the
commit"). The draws are not derived from the store either: a compaction lands
only if no append lands while it is encoded and sent, which a writer could
estimate from the rate its log grows and its last compaction's time, and skip
compactions that cannot land. A compaction that lost to appends is sent again
by that estimate (`CompactionPolicy.resend`), but the first one is not
skipped. Measured on S3 Express (campaigns `tr6k` and `tr6m`): 1,000 beat 100
at every point but one where one run an arm is noise. At E1's 1,000-table
knee, writers began 2,680 compactions in four runs and 2 landed; the store's
cap of 10,000 appends bounded the log.

A checkpoint holds the committed set's ids inside the admission window and
names pages for the rest (COMMITTED_PAGING.md): at 1,000 commits a second's
steady state, 3.4 MB of a 4.1 MB checkpoint at the median compaction (ingest
campaign `i11-paging`), while the ingest benchmark's captured catalog state is
under 1 MB. So a cost rule must count the set. The ingest benchmark
(`IngestBench`, campaign `i2-compaction`, lab page `ingest`) measures a
compaction's encode and decode as the catalog grows.

**Trigger to revisit:** a deployment on a store other than S3 Express, or
the contention benchmark's Azure and GCS profiles.

### D16. The committed set's retention and admission window are not derived from the store

The committed set holds every id replay applied within the retention
(`fileio.catalog.committed.retention.ms`, 30 minutes): the ids inside the
admission window A (`fileio.catalog.committed.admission.ms`, 5 minutes) in the
checkpoint, and the older ones in pages while the inline set exceeds the page
threshold T (1 MiB; COMMITTED_PAGING.md). All three are provisional. What the
set must cover is how late a copy of a record can land, which a store may bound
(SigV4 and Azure Shared Key reject a request signed about 15 minutes earlier;
to be checked per store), and how long a writer may ask whether its commit
landed, which the snapshot oracle (LOG.md) answers past the horizon. A commit
whose id the floor passes before it applies is rejected (`BEYOND_FLOOR`), so A
must outlast a commit's retries, which the runtime could derive from the commit
durations it observes (D14). Only A sizes the checkpoint, so the retention
could grow cheaply.

**Trigger to revisit:** a store whose late landings the retention does not
cover, or commits whose retries approach A. COMMITTED_PAGING.md "Open
questions" has the rest, and D21 the stamps the set leaves unbounded.

### D18. An inline-ML table's root grows with its unmerged manifests and its history

Snapshot paging ([SNAPSHOT_PAGING.md](SNAPSHOT_PAGING.md)) keeps an inline-ML
table's recent snapshots in the catalog's root and seals the older ones, with
their lists, into pages that replay never reads, and stores each list as a
change of its parent's. At E3's 7,200 snapshots of merge appends a table's
root holds 0.36 MB, and 1.07 MB lies in 24 pages (SNAPSHOT_PAGING.md
"Sizes"). What still grows:

- A fast-append table's head list names every manifest it has, which only a
  merge shrinks, and fast appends never merge.
- The root keeps about 18 B a sealed snapshot, so it grows with the history
  expiry leaves.
- Eviction is by update size only (`fileio.catalog.inline.max.update.size`),
  so a table whose deltas fit stays inline however large it grows. A per-table
  size threshold would evict it to pointer mode (SNAPSHOT_PAGING.md step 6,
  cold-table eviction, deferred).

**Trigger to revisit:** an inline-ML workload of mostly fast appends, or
tables whose history expiry does not bound.

### D21. A commit id's timestamp has no upper bound

A commit's id is a UUIDv7 stamped by its writer's clock, at least one past the
horizon the writer read (LOG.md, Bug 64). Replay admits a record above the
horizon however far ahead its stamp is, and a GC record's watermark is its
writer's clock less the retention. Exactly-once holds whatever the clocks say;
a clock far ahead costs retention and liveness:

- A record stamped ahead stays in the committed set until the clocks reach
  its stamp plus the retention.
- A compactor whose clock is ahead by more than the retention drops the ids of
  commits in flight: they are rejected, or unknown if a write may have landed.
- Once such a compactor drops a stamp that is itself ahead (its own earlier
  commit's), the horizon moves ahead of every clock. Every writer then mints
  after it, and no other compactor's watermark reaches their ids, so the set
  grows until the clocks catch up. The horizon cannot come back down, since a
  lower one would admit copies.

LOG.md ("Clock skew") makes synced clocks a deployment requirement. No test
runs a writer ahead by more than the retention
(`TestCatalogRegressions.CommittedSetHorizon` runs one behind).
Paging keeps the exposure (COMMITTED_PAGING.md): its floor follows the oldest
block, not a clock, but a compactor's ceiling (now less the admission window)
does, so one far ahead can seal every block but the newest.

Fixes, not built:

- **Bound a writer's clock by the store's.** Every store answers with its time
  (a response's `Date`, an object's `Last-Modified`). A writer whose clock is
  ahead of it by more than a bound refuses to commit or compact. This catches
  skew, the usual cause, but not a writer that stamps ahead on purpose or by a
  bug.
- **A bound replay enforces.** Rejecting a record stamped too far ahead needs a
  time replay can trust and the writer does not choose: a shared log's append
  time, recorded with the record. One object records none.
- **GC by log time.** Cap a watermark also at the newest stamp in the
  compactor's base less the retention, so a compactor whose clock alone is
  ahead drops nothing younger than the retention. It does not help once
  stamps ahead are in the base.

**Trigger to revisit:** writers whose clocks one operator does not control.

### D22. A format upgrade to v3 evicts an inline-ML table

A v3 table gives every data row an id (row lineage). The first commit after
an upgrade gives each data manifest it keeps, and that had none, a first row
id (`SnapshotProducer`), so the new snapshot lists that manifest's path with
an entry that differs from the one the table's pool holds. A pooled path's
entry never changes (SPEC_ML.md; CHECKPOINT_PLAN.md decision 3), so the
writer evicts the table instead (`LogCatalog.restatesAManifest`): it writes
a metadata file and a list file per snapshot. An inline-TM table is not
affected: its lists are files.

The eviction is not for good. The table's next commit through an inline
writer re-inlines it (SPEC_TM.md, "Pointer → Inline"): a pointer table has no
pool, so its new lists start one with the upgraded entries, and the old
snapshots keep the list files the eviction wrote; with snapshot paging, a
table of any depth comes back, its history in import pages
(SNAPSHOT_PAGING.md step 5). `TestSnapshotPaging.aTableUpgradedToV3ComesBackInline`
checks both. What the upgrade costs is the one eviction: a list file for
every inline-ML snapshot (read from its pages, if sealed) and a metadata
file.

Keeping the table inline would mean restating the pool's entries for the
kept manifests in one record, and either rewriting the old snapshots' lists
or letting an entry differ between a snapshot and its successors.
A paged table is evicted the same way.

**Trigger to revisit:** v3 upgrades of deep inline-ML tables in a workload
the paper measures, where the eviction's writes matter.

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

### D25. A fold copies each table-keyed map a record changes (perf)

A fold onto a replica copies only what its records change (a builder shares
the state's maps until it changes one, `CatalogState.SharedMap`), but a record
that changes a table copies each of the state's maps keyed by table that it
changes, so a fold still grows with the catalog's tables. `FoldProfile` (two
writers taking turns, 5,000 records after the checkpoint) folds in 0.19 ms at
300 pointer tables and 0.49 ms at 3,000, and in campaign `tr8y` (E1's 3,000
tables on `s3x-2026-11`) a fold took 1.0 ms, inside a resend's window of 15 to
20 ms. Persistent maps would make it logarithmic in the tables.

**Trigger to revisit:** catalogs of tens of thousands of tables, or a fold's
CPU again a large part of a resend's window.

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

- **versitygw**: no S3 directory-bucket semantics; `writeOffsetBytes` is
  accepted and ignored (the object is overwritten). APPEND-mode commits
  (`maxAppendCount > 0`) skip on versitygw; `VersityGwEmulatorCanaries`
  asserts the gap, and positive canaries check that `If-Match` /
  `If-None-Match: *` are enforced and that concurrent conditional PUTs have
  exactly one winner. No open-source S3 emulator implements S3 Express
  append (checked 2026-10: versitygw, SeaweedFS and LocalStack ignore or lack
  it; RustFS rejects it with 501); AWS's own does, below.
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
  with `ifGenerationMatch`), which fake-gcs-server enforces;
  `FakeGcsEmulatorCanaries` checks `ifGenerationMatch` on multipart and
  resumable uploads. Zonal (Rapid) buckets take
  a single XML API PUT for CAS and an appendable upload for regular
  writes (manifests, data files), since they reject resumable uploads;
  fake-gcs-server has no zonal semantics, so those paths are covered by
  iceberg's `GcsRapidCanaries` and `GcsFileIOAtomicRapidTest` against a
  real Rapid bucket (`RAPID_BUCKET`), and by running these suites with
  `GCS_TEST_BUCKET` set to one.

**The AWS Emulator, tried and not adopted.** AWS's own emulator
(github.com/aws/aws-emulator, v1.0.338, tried 2026-11-12) implements
directory buckets, S3 Express sessions and append (`writeOffsetBytes`,
checked with `If-Match`), and ranged GETs pinned by `If-Match`. The fork's
`S3ExpressFileIOAtomicTest` and `S3StandardFileIOAtomicTest` pass on it, and
so do all twelve `TestS3*` suites, the APPEND ones included (876 tests, no
failures; the 80 skips are the suites' own). It is not mature enough to
adopt:

- It is young (first released 2026-09, several releases a day), ships as a
  1 GB native package with no container image, sends telemetry unless
  started with `--telemetry off`, and makes a new TLS CA on every start, so a
  JVM needs a trust store made after each start.
- It caps what it holds (1,000 objects and 64 MiB a bucket, 128 MiB in all,
  256 open S3 Express sessions; its own open issue 20261111T165323Z), so each
  suite needs an emulator and bucket of its own, and the catalog suites, which
  make a client per FileIO, need session auth off.
- It answers writes one at a time, so it never returns 409
  `ConditionalRequestConflict`, and it never refuses an append past 10,000
  parts (`TooManyParts`): the S3 error paths the commit loop depends on stay
  covered only by `InMemoryAtomicFileIO` and real cloud. The catalog's APPEND
  logic already runs locally on `InMemoryAtomicFileIO` (`TestLocalCatalog`,
  the fuzzers), so the suites it unskips add the S3 Express wire path alone.

**Trigger to revisit:** a container image and its caps lifted; it could then
replace versitygw for the S3 suites, and pre-check the fork's S3 contract
before a real-cloud run.

### T6. No test reads the catalog through a store's accelerated read path

A read is of one version (ATOMIC_FILE_LOG.md "Reads"): `If-Match` on S3 and
ADLS, `if-generation-match` on GCS, and `StaleReadException` once a write
supersedes it. The fork's contract suite checks it on every store, and the
fuzzers' `InMemoryAtomicFileIO` read modes serve what an unpinned store could.
The stores' accelerated read paths (S3's analytics accelerator, GCS's
analytics core; both off by default) read whatever is live. With the
accelerator on, the fork's S3 reports `readsRecordVersion()` false, so
`AtomicFileLog` looks the object up before reading it, and format 3's frame
checks reject a torn read; but neither path has a contract case or a fuzz
mode, so a catalog's FileIO should leave them off, and nothing makes it.

**Trigger to revisit:** when a catalog's FileIO enables an accelerated read
path.

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

The ingest benchmark's capture (16 writers on 16 tables, mostly merge
appends with some fast appends, validated overwrites and compactions, over
the contention driver's synthetic data files; lab page `ingest`) averages
721 B a record. The estimates above are per delta; a record holds a
snapshot's delta, the manifests it adds and removes, and its producer's
footprint. The cases listed above remain unmeasured.

**Trigger:** before publishing comparative benchmarks, validate the
estimates with an instrumented run.

### U4. Behavior under storage provider edge cases

We exercise S3, GCS, and ADLS conditional writes in integration tests,
but only for the conditions we know to test (CAS, append, reads of one
version, the stores' append limits). We have not exercised:

- ETag changes that aren't object replacements (provider-side metadata
  rewrites). Azure's HNS rename, which ADLS CAS relies on, keeps the
  source's ETag and returns it (checked 2026-10-14), so a CAS learns the
  new ETag without a HEAD that could see a later writer;
- region-failover redirects mid-write;
- conditional appends that succeed at the storage layer but return a
  partial offset (does any provider do this?).

**Trigger:** the first time a real workload sees an unexpected commit
failure on a specific provider.

### U5. `InlineSnapshot.equals` semantics interact with retention

`InlineSnapshot.equals` compares only scalar identity, not the manifest
list. This is load-bearing for `Set<Snapshot>` membership in retention /
expiration code. The behaviour is correct for the cases we tested, but
we have not exhaustively walked all upstream Iceberg code that builds
sets of snapshots — there could be a path that legitimately wants
manifest-list equality.

The manifest side is settled: expiry's `ReachableFileCleanup` compares
`ManifestFile` objects, and inline snapshots hold `GenericManifestFile`
copies, equal by path as Iceberg expects (Bug 44).
`TestCatalogFaultProperties` expires snapshots of tables with tags,
branches and replaced histories, and checks that every retained
snapshot's manifests exist.

**Trigger:** any retention or cleanup bug report that involves an
inline-ML table.

### U6. Measurements still to take on the merged mains

Tail reads and one request a read (TAIL_READS.md steps 1-7) are merged and
measured at E1's and E3's points on `s3x-2026-11`. Still to take:

- **Step 7's real-cloud contract run.** Fork atomicio fbd1036bf9 (a GET's
  reply reports the version and length it read; a GET from an object's end is
  the stream's end) has passed the local suites, the fork's mock tests and its
  S3 contract on the AWS Emulator (T3), not the contract suite on the real
  stores.
- **`read_lookup` on and off** (`fileio.catalog.read.lookup`): the cost of the
  lookup step 7 saves, measured alone.
- **E1's 300 tables at 256 workers**, the driver pool's limit in every arm of
  `tr6x`, `p4r` and `e1-d20`, which did not tell the arms apart.
- **Reruns under today's latency model.** Campaigns run on code from before
  2026-11-12 had a latency model that charged no HEAD for a lookup after an
  open and saw a lookup at the end of its latency and a GET at its start, and a
  fold that rebuilt the whole catalog state; until 2026-11-13 both models
  priced a HEAD as a GET of nothing, which only `azure-2026-11` tells apart
  (5.85 ms against 9.84). Comparisons whose arms read alike stand;
  absolute numbers are a HEAD a read low; tail reads against probes, where the
  commit loop sets the limit, favour the probes; and FileIOCatalog's CPU per
  commit grows with the catalog's tables. catalog-bench's JDBC reruns on
  `core-perf` (`e1j-cp`, `e3j-cp`, `p4rj-cp`) predate the new model and run
  again.

- **The race backoff by latency against the fixed bound**
  (`fileio.catalog.race.retry.basis` `latency` and `fixed`): at E1's knee,
  and on `s3`, where every commit replaces the object and each lost race is
  another read of it (there `partition-vector` committed less than `lamport`
  under overload, lab page `c6-endive`, as `lamport`'s conflicts went back to
  Iceberg's retry, whose longer backoff throttled the writers).

**Trigger:** before the paper's cloud runs.
