# Errata

Current shortcuts, deferred functionality, known bugs, and known unknowns
in `FileIOCatalog` and the inline manifest-list extension. Items are
grouped by what they are; within each group, ordering is suggestive, not
strict.

For invariants that must not regress, see [design.md](design.md). For the
wire format see [SPEC.md](SPEC.md), [ATOMIC_FILE_LOG.md](ATOMIC_FILE_LOG.md)
and [LOG.md](LOG.md). Bugs in Apache Iceberg itself are in
[ICEBERG_ISSUES.md](ICEBERG_ISSUES.md). Upstream fixed C1 (1.11.0), and
the fork's `core-fixes` branch fixes C2-C27, so this project's tests
work around none of them (the workarounds for C2-C8 were dropped in
63dd25e, b82ceb9, dbac28b, 411f73c, e2c6b7b and 890fd8d); `TestIcebergCoreIssues`
asserts each is absent. On stock Iceberg 1.12.0 C2-C27 come back.

An entry's ID is not reused: a fixed entry is deleted or marked
resolved, and its ID retired. Retired: S3, D1-D3, D5-D7, D11, T1, T2,
T5, U2 (answered: D18), U3 (`git log -p docs/errata.md` has each). Five IDs were reused before
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

### S4. The runtime trusts a shared log's checkpoints to continue its records

On a log with separate checkpoints (`SharedLog.Capabilities#separateCheckpoints`),
a read is the newest checkpoint and the records after it, and the runtime
relies on the provider for their joint: one order of records, each read a
checkpoint and a prefix of the records after it (P1), and no trim before a
durable checkpoint covers what it trims (P5; LOG.md). A checkpoint does not
say which position it covers, so a provider that served a checkpoint and
records with a gap between them, or trimmed past its newest checkpoint,
would lose the records in the gap and the runtime would not notice.
`SharedLogContract` checks P1 and P5 on a provider, and the fuzzers' harnesses
(`MemoryLogHarness`, `AtomicFileLogHarness`) report checkpoint violations. A
checkpoint that named the last record it holds (its position, or its id)
would let a read check that the records after it start where it ends.

**Trigger to revisit:** a provider that `SharedLogContract` does not cover,
or a production shared log. S5 makes the format check the joint.

### S5. A read's continuity is the store's word, not the format's

The log's invariant is one total order of records with no gaps: a read is a
checkpoint and a prefix of the records after it (P1, LOG.md). The runtime
relies on the provider for it and checks nothing. No record says what it
follows, and no checkpoint says where it ends (S4). The providers give it
physically only while each read sees one version of the log. ADLS and GCS
read by version, and the in-memory providers do; the fork's S3 reads do not
(T6). An S3 read is a ranged GET with no `If-Match`, reopened mid-stream by
another, so a read that races a replace can return the start of one object
and the rest of another.

Commits are safe regardless. Every write is conditioned on the version its
read bound to (P4), so a commit staged on such a view fails. But a reader that
never writes (a load, a scan's planning) takes the view for the log. A splice
at a frame boundary decodes: an old checkpoint followed by records written
after a newer one, a state the log never held. Pinning S3's reads to the ETag
the probe saw restores the invariant for whole reads, but not for the reads
the format should allow:

- **A tail read** (D15). An append changes the object's ETag, so a read pinned
  to the ETag a reader holds fails after any append. One pinned to the new
  ETag shows that the tail comes from one object, not that the object extends
  the prefix the reader holds: a replace can grow past it.
- **A shared log's checkpoints** (S4), which reach a reader apart from the
  records after them, joined only by the provider.

So the format should carry the invariant, and detect a gap or a splice where
the log cannot rule one out:

- Each record names what it follows: the incarnation of the log it extends
  (D15's incarnation id) and its predecessor (its position, or a digest of the
  record before it). Replay checks each record against the one before, and a
  gap or a splice fails the read rather than applying.
- Each checkpoint names the last record it covers (S4), and a read checks
  that its records start where its checkpoint ends.
- What a record's writer cannot know before it lands (its position, and on a
  shared log that orders appends itself, its predecessor) the log
  implementation adds in the envelope at append, as part of the SPI's
  contract: an obligation beside P1-P6, checked by `SharedLogContract`. On one
  object the writer knows both, since its append is conditioned on the exact
  version and offset it extends.

The models should say the same. `RSM.tla`, `CatalogCommit.tla` and
`SharedLogCommit.tla` model a read as a prefix of the log. They need reads
that skip or splice (a store misbehaving within what S3 allows), an oracle
that no replica holds a state the log never held, and a bug switch per check
the format adds. In Java, a fuzzer mode of `InMemoryAtomicFileIO` whose bound
read returns the live generation or splices two (T6), and a shared-log harness
that serves a gap.

The cost is a few bytes a frame: an 8-byte incarnation id and a varint
position (or a truncated digest), about 3% of a 330-byte record.

**Trigger to revisit:** a dedicated task, before relying on S3 in production,
before tail reads (D15), or before a production shared log (S4). It takes over
the format half of D15 and the checks S4 and T6 ask for.

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
changed (its `statisticsChanged` check). Any commit that
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

A compaction writes the committed set whole (D16), and at the default
retention and a steady commit rate the set is most of the checkpoint: 25 MB
at 100 commits a second and 238 MB at 1,000, while the ingest benchmark's
captured catalog state is under 1 MB. So a cost rule must count the set, or
the set must leave the checkpoint (D16). The ingest benchmark (`IngestBench`,
campaign `i2-compaction`, lab page `ingest`) measures a compaction's encode
and decode as the catalog grows.

The backoff matters most where every commit replaces the object: on `s3`
under overload, `partition-vector` commits less than `lamport` (lab page
`c6-endive`), since each lost race is another read of the whole object, while
`lamport`'s conflicts go back to Iceberg's retry, whose longer backoff
throttles the writers.

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

The design needs no read pinned to a version, which S3 does not give (T6):
the incarnation id in each frame is what says whether the bytes after
`since` extend the object `since` read.

**Trigger to revisit:** a deployment whose catalog object grows past a few
MB on a store that appends (at the default retention, the committed set alone
reaches 2.5 MB at 10 commits a second, D16), or a benchmark profile that
prices a GET by its bytes.

### D16. Replay rewrites the whole committed set, and every checkpoint carries it

The committed set holds every id within the retention
(`fileio.catalog.committed.retention.ms`, 30 minutes by default since
2026-10-28, 6 hours before; GC.md), so at a steady rate it holds rate ×
retention ids. At 6 hours that was 2.16 million at 100 commits a second and
21.6 million at 1,000; at 30 minutes it is 180,000 and 1.8 million. It is one
packed blob (`CommittedTransactions`, LOG.md: newest first, per id a varint gap
in ms from the one before and its 10 random bytes) of about 11 B an id: 24.7 MB
and 238 MB at 6 hours, 2 MB and 20 MB at 30 minutes. A lookup scans it
in place, newest first, and a checkpoint copies it as it is
(`LogRuntime.checkpoint`). But a replay that applies any record builds a new
one: `CommittedSet.Builder.build` calls `CommittedSetCodec.rewrite`, which
unpacks every inherited entry (a 10-byte array each) to merge the new ids,
then packs them all again, and a GC record (`advanceHorizon`) rewrites it too.
Every writer's fold of a record pays it, and so does its read through its
own write.

The ingest benchmark (campaign `i1-ingest`, no compactions; lab page
`ingest`) prefills the set to its steady state, at the 6-hour retention for
the campaigns below. Building the state after
every record, one thread ingests 758 records a second from an empty set, 10
with 100 a second's set, and 1.2 with 1,000's: about 100 ms and 860 ms a
build, linear in the set's size, against about 70 µs to fold the record. In
batches of 64 records it ingests 567 a second with 100 a second's set. The
contention benchmark does not see this: its ten-minute runs start from an
empty set. Beyond the build, the checkpoint a compaction writes and a fresh
read GETs carries the whole set (D14, D15).

The 74 random bits of a UUIDv7 do not compress, so an exact set costs at
least about 9.25 B an id. A lossy one (a filter, truncated ids) could report
a record applied that was not, and drop it. The fixes, in order:

- **Build without unpacking (done, 2db98d7).** Entries are sorted newest first, and the ids
  a replay adds are almost all newer than every entry it inherits, so adding
  them is a prepend: pack the new ids and re-encode the gap of the first
  inherited entry. A GC drops a suffix: cut the blob at the first entry at or
  below the watermark. Only an id older than the newest entry (a commit that
  retried while others landed, a writer whose clock is behind) needs a merge,
  and only with the entries newer than it. A build then costs its ids and a
  copy of the bytes; a set kept as a list of immutable runs, merged as they
  accumulate, would not copy them at all. The wire format and I11 stay as
  they are. The set is now kept in runs this way (`CommittedSet`), and the
  ingest benchmark's steady state for 1,000 records/s ingests 23,907
  records/s in batches of 64 and 19,968 one record at a time (campaign
  `i5-fold`, lab page `ingest-ladder`), against 30,143 with the ids
  discarded. The checkpoint still carries the whole set.
- **Page out the old ids** (designed in
  [COMMITTED_PAGING.md](COMMITTED_PAGING.md), not built). The horizon splits in
  two: a floor below which replay admits no record, and the horizon below which
  lookups are unknown. The ids between them move to immutable pages, which the
  checkpoint names by URI. A `PageOut` record seals the oldest block of the set
  and raises the floor; it applies only if the block's digest matches what its
  writer paged. The blocks are the maximal nodes of a binary trie over the ids'
  timestamps holding at most 2^16 ids: deterministic, sized by density, never
  pivoting on an id. The checkpoint then carries rate × the admission window
  (5 minutes proposed), not rate × the retention. Replay never reads a page,
  and a writer's lookup in the paged range fetches it and is exact.
- **Derive the retention.** 6 hours was "longer than any commit's retries"
  (GC.md); the default is 30 minutes since 2026-10-28, which makes the set 12
  times smaller. What the set must cover is how late a copy of a record can
  land, which a store may bound (SigV4 and Azure Shared Key reject a request
  signed about 15 minutes earlier; to be checked per store), and how long a
  writer may ask whether its commit landed, which the snapshot oracle (GC.md)
  answers past the horizon. With paging, only the admission window sizes the
  checkpoint, and the retention could grow again cheaply.

**Trigger to revisit:** a commit rate past about 50 a second at the default
retention (the set passes 1 MB there, and grows linearly with the rate), or the
ingest and materialization service.

### D17. A snapshot expiry never merges, so it starves under contention

An expiry's record removes the snapshots it found unused on its base, and
replay merges no record that removes something (`Landing`: "it removes what
it found unused on its base"). So under every encoding an expiry fails if
anything changed its table since it read it. That is sound: the snapshots it
removes were unreferenced on its base, not necessarily where it lands (a
concurrent commit may have tagged, cherry-picked or rolled back to one). But
under contention an expiry rarely lands. In a contention-benchmark run (`s3x`,
`manifest-window`, 32 writers on fast appends), 9 of 10 expiries exhausted
their retries, and the ingest benchmark's capture raises the maintenance
client's retries to 100 (`expire_retry_num`) to keep its tables at their
last 100 snapshots. A table whose expiries starve keeps every snapshot, and
its checkpoint and loads grow with them (D18): in the 32-writer profile a
table reached about 1,400.

A merge rule would admit an expiry onto a table changed since its base only
by records that reference none of the snapshots it removes: no ref moved to
one, no cherry-pick or rollback of one. It must also keep its writer's file
cleanup safe. Iceberg picks the files to delete from the expiry's base and a
refresh after its commit, and its incremental cleanup assumes the base is
the version the commit replaced. Other commit-once records (schema and spec
changes, ref changes) starve the same way, but they are rare; an expiry
recurs, and bounds the table's growth.

**Trigger to revisit:** a table with sustained concurrent commits whose
expiries fail, or the ingest and materialization service.

### D18. An inline-ML table stores each snapshot's manifest list whole

An inline-ML table keeps each manifest once (its pool) but each retained
snapshot's manifest list whole, so its checkpoint grows with the snapshots
times the manifests each lists (this answers U2). ReplayBench
(docs/REPLAY_BENCH_RESULTS.md) measured it. Under fast appends, which never
merge manifests, the checkpoint grows with the square of the snapshots (12.8
MB at 3,000); under merge appends about linearly (927 KB at 3,000). A load
makes the table's metadata and each snapshot's `InlineSnapshot`. A later load
reuses the ones an earlier load made while their lists are unchanged
(`LogCatalog.InlineSnapshots`), so only a catalog's first load of a table
makes every list (0.66 s at 3,000 fast-append snapshots), and its first
after each compaction: a decoded checkpoint shares no list object with the
state before it, and reuse goes by the list's identity.

The options:

- **Delta-encode the lists on the wire**: a snapshot's list as its parent's,
  plus the manifests it adds and minus those it removes, so the checkpoint
  grows with the snapshots plus the manifests. Replay already shares
  unchanged lists in memory.
- **Make lists lazily** (fork): an `InlineSnapshot` that resolves its list
  on first use, so a load costs the snapshots, not their manifests.
- **Page out old snapshots**: keep a table's recent snapshots inline and the
  older ones, with their lists, in immutable objects the checkpoint points
  to, as D16 proposes for the committed set. A load fetches them only for
  history or time travel.
- **Evict by size**: eviction is by update size only
  (`fileio.catalog.inline.max.update.size`), so a table whose deltas fit
  stays inline however large its lists grow. A per-table checkpoint-size
  threshold would evict it to pointer mode.

**Trigger to revisit:** a table that keeps thousands of snapshots without
merging manifests, or whose expiries starve (D17).

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

### T6. The fuzzers pin reads, and S3 does not

`InMemoryAtomicFileIO` binds an `InputFile` to the generation it first
observes and keeps reading those bytes, as a read conditioned on the version
would. The fork's S3 reads are not conditioned: `S3InputStream` sends a
ranged GET with no `If-Match`, and on a read error reopens the stream at its
position with another. So an `AtomicFileLog` read on S3 (`getLength`, a
HEAD, then the bytes) can return the first bytes of a newer object than the
one its tail is bound to, or, after a reopen, the start of one object and
the rest of another. ADLS and GCS read by version.

That this is safe is argued, not tested. A read cut inside a frame fails to
decode, and the read fails. A frame-aligned prefix of a newer object is a
state the log held, and its tail is still bound to the version the HEAD
saw, so a write conditioned on it fails. A fold compares bytes, so it
replays such a view whole. And a read through a write must include the
write's record (`seen`), or the commit is ambiguous. A spliced view is the
case the argument covers least: its bytes come from two objects, and only
decoding stands between them and replay. D15's incarnation id in each frame
would reject one.

A fuzzer mode would test it: an `InMemoryAtomicFileIO` option whose bound
read returns the first bytes of the live generation, or splices two at a
random offset. `InMemoryAtomicFileIO`'s doc also says S3 reads with
`If-Match` (the `failSupersededReads` paragraph), which the fork's S3 reads
do not.

**Trigger to revisit:** before relying on S3 (general purpose or Express)
in production, or when D15's ranged reads are built. S5 is the fix: the format
detects a spliced or gapped read, whatever the store returns.

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
