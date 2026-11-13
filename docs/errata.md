# Errata

Current shortcuts, deferred functionality, known bugs, and known unknowns
in `FileIOCatalog` and the inline manifest-list extension. Items are
grouped by what they are; within each group, ordering is suggestive, not
strict.

For invariants that must not regress, see [design.md](design.md). For the
wire format see [SPEC.md](SPEC.md), [ATOMIC_FILE_LOG.md](ATOMIC_FILE_LOG.md)
and [LOG.md](LOG.md). Bugs in Apache Iceberg itself are in
[ICEBERG_ISSUES.md](ICEBERG_ISSUES.md). Upstream fixed C1 (1.11.0), and
the fork's `core-fixes` branch fixes C2-C29, so this project's tests
work around none of them (the workarounds for C2-C8 were dropped in
63dd25e, b82ceb9, dbac28b, 411f73c, e2c6b7b and 890fd8d); `TestIcebergCoreIssues`
asserts each is absent. On stock Iceberg 1.12.0 C2-C29 come back.

An entry's ID is not reused: a fixed entry is deleted or marked
resolved, and its ID retired. Retired: S3, D1-D3, D5-D7, D10, D11, T1, T2,
T5, U2 (answered: D18), U3 (`git log -p docs/errata.md` has each). Five IDs were reused before
this rule, so an older code comment or commit citing one may mean the
earlier entry:

- S2: `BaseTransaction.TransactionTableOperations` did not forward
  `ManifestListSink` (fixed 6f30fbb).
- D9: an inline-ML create transaction lost its manifests' partition
  spec ids (fixed 3c614c2: the create carries its manifest-pool delta).
- D10: single-pass rewrite of the committed set (fixed b1af354); then
  ADLS CAS with vended SAS credentials (fixed in the fork, entry dropped
  a8aa0e9); then `SerializableTable` could not load an inline table's
  metadata on a worker (fixed in the fork, 0f44056ccf, with
  `TestSerializableInlineTable`; ID retired).
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
change). The fork makes three more changes to core for inline tables:

- `TableMetadata.Builder.setLastUpdatedMillis` (17 lines, 6085f96f4)
  lets replay pin the writer's last-updated-ms, which `buildFrom`
  resets and `setRef` and `build` otherwise take from the clock; since
  Iceberg 1.12.0 a delta that only adds a snapshot needs it too
  (design.md I4, Bug 60).
- `BaseSnapshot.equals` accepts any `Snapshot`, not only a
  `BaseSnapshot` (d0b03bc1b), so a `BaseSnapshot` and an
  `InlineSnapshot` of the same snapshot are equal both ways.
- `SerializableTable` ships a table whose metadata location is not a
  `metadata.json` with its metadata as JSON and its `InlineSnapshot`s'
  manifests (0f44056ccf), which a worker rebuilds with
  `replaceSnapshots`: stock Iceberg loads a shipped table's metadata
  from its file, which an inline table does not have, so a Spark or
  Flink worker could not read one (`TestSerializableInlineTable`).

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

**Resolved 2026-11-10** ([TAIL_READS.md](TAIL_READS.md) step 3; LOG.md
"Positions"). A checkpoint stored apart names the position it covers
(`LogCheckpoint.through`, the provider's encoding of the tail the compaction
read), every view says where its records start (`LogView.start`), and the
runtime fails a view whose records do not start where its checkpoint ends, and
folds records read after a tail only onto that tail (`LogGapException`).
`InMemorySharedLog` encodes positions as record counts and can serve a gap;
the commit-protocol fuzzer draws gap reads and checks that no replica holds a
state the log never held (`LogHarness.held`), and a runtime that skips the
check fails it; `SharedLogContract` holds a provider to positions. What stays
the provider's word is that its positions are true: a provider whose records'
positions lie passes the runtime's check.

### S5. A read's continuity is the store's word, not the format's

The log's invariant is one total order of records with no gaps: a read is a
checkpoint and a prefix of the records after it (P1, LOG.md). The runtime
relies on the provider for it and checks nothing. No record says what it
follows, and no checkpoint says where it ends (S4). The providers give it
physically only while each read sees one version of the log. Since
2026-11-09 the fork's stores all do, on their word (T6): a read asks for the
version its lookup found (S3's and ADLS's `If-Match`, GCS's
`if-generation-match`) and fails as stale once a write supersedes it. Before,
S3 and GCS reads asked for none, so a read that raced a replace could return
the start of one object and the rest of another.

Commits were safe regardless. Every write is conditioned on the version its
read bound to (P4), so a commit staged on such a view fails. But a reader that
never writes (a load, a scan's planning) takes the view for the log. A splice
at a frame boundary decodes: an old checkpoint followed by records written
after a newer one, a state the log never held. Pinned reads restore the
invariant for whole reads, on the store's word; a store that breaks its read
contract still splices undetected. And pinning cannot give the invariant to
the reads the format should allow:

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

**Done on one object** (TAIL_READS.md step 2, 2026-11-10): format 3 frames
every checkpoint and record with a trailer naming the object's incarnation and
the frame's offset, with a CRC32C over both and the frame (ATOMIC_FILE_LOG.md
"Layout"). Every read checks every frame, and a read that fails a check reads
again: a splice at a frame boundary fails the incarnation or the offset, one
inside a frame the checksum. `InMemoryAtomicFileIO`'s read modes serve the
splices S3 could (T6), and the commit-protocol fuzzer's oracle checks that no
read returns a state the log never held; a mutant that skips the checks fails
it.

**Done on shared logs** (step 3, 2026-11-10): positions in the SPI (S4,
resolved).

**Done in the models** (step 4, 2026-11-10; spec/tla/README.md "Tail
reads"): incarnations, splices and tail reads in `CatalogCommit.tla` and gaps
in `SharedLogCommit.tla`, under the invariant `NoUnheldState`, with a switch
per check (`lengthonly`, `notrailer`, `probeinc`, `nojoint`), each breaking a
property.

**Resolved 2026-11-10.** What stays the store's or the provider's word: that a
shared log's positions are true (S4), and that a catalog's FileIO leaves the
stores' accelerated read paths off (T6).

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
same. That is finer than splitting a table's version into its properties
and the rest, as this entry once proposed (concerns, refs, partitions, a
branch's manifests), and it needs no restaging: the record
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

**Later: a transaction's own witness.** Under a merging encoding, a
transaction could conflict only where Iceberg's retry of it would fail,
as a single-table commit does:

- **The tables it writes:** its records go unpinned, as inline deltas
  with their producers' footprints, which the catalog's encoding judges
  where they land and `Landing` finalizes, as for a single-table commit.
  A pointer-mode or full-mode write stays pinned.
- **The tables it only read:** the record names what it read of each
  (the branches and their heads: `BaseCatalogTransaction`'s read set,
  which `unchangedSinceRead` checks), and replay checks that rather
  than the table's version.

That is a format change (the read set in the intention record), and the
conflict-encoding oracle would need to cover it: `TestRebaseProperties`
landing a transaction after a window of commits it did not see, against
a reference that runs the transaction again. The alternative without a
format change is to restage a failed transaction on a fresh read in
`commitTransaction`, as `commitOnAFreshRead` and `CatalogHandlers.commit`
do. Each attempt would check the read set, the commits' requirements
(derived from the version the transaction read, Bug 23) and
`TableMetadata.Builder`'s sequence-number and row-id checks again on its
read, stop on a failed check, retry only a definite failure
(`CommitStateUnknownException` propagates), and stage from scratch
(`stageUpdate`, inline-ML lists included).

**Trigger to revisit:** a workload whose multi-table transactions race
other commits to their tables.

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

`fileio.catalog.max.append.count` defaulted to 100 (1,000 since 2026-11-10,
below) and `fileio.catalog.race.retry.min-wait-ms` to 100 ms, both from one store's
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

Since 2026-11-10 (TAIL_READS.md "Compaction after the commit") the count's
default is 1,000, and a writer compacts after its commit at a point past the
limits it draws, backing off as its compactions lose, instead of the next
commit replacing the object: at E1's 1,000 tables, writers that compacted at
the count all at once lost to each other, and a compaction's PUT outlasts the
gap between appends many times over. 1,000 is provisional too: a reader
that folds pays for a record once, and a record is about 200 bytes, so the
count now bounds a cold reader's whole read and how often every writer
decodes a new checkpoint; the rule above still stands. The draws are not
derived from the store either: a compaction lands only if no append lands
while it is encoded and sent, which a writer could estimate from the rate its
log grows and its last compaction's time, and skip compactions that cannot
land. A compaction that lost to appends is sent again by that estimate
(`CompactionPolicy.resend`), but the first one is not skipped. Measured on
S3 Express (TAIL_READS.md "Compaction after the commit", campaigns `tr6k` and
`tr6m`): 1,000 beat 100 at every point but one where one run an arm is
noise. At E1's 1,000-table knee, writers began 2,680 compactions in four
runs and 2 landed; the store's cap of 10,000 appends bounded the log.

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
catalog-bench's `s3x-2026-11` profile (measured, 7 ms a MiB read) now prices
it, and a commit paid it four times (D20); twice since D20's fix (its load and
the refresh before it applies), which ranged reads would make once.
Plan: [TAIL_READS.md](TAIL_READS.md) (proposed 2026-11-06), with S5's format half.

**Resolved 2026-11-10** ([TAIL_READS.md](TAIL_READS.md) steps 2 and 5, branch
`tail-reads`). The design above, with the incarnation in a trailer that ends
every frame (format 3) rather than in a header, and checksums: a tail read is a
GET from 20 bytes before the end (after a lookup, until step 7), and a client's
loads, refreshes and commit-loop reads read only what follows its replica.
Measured (step 6, catalog-bench `tr6`, `s3x-2026-11`): a merge append's p50 fell
48-72% inline ML and 16-37% inline TM across E1 at 300 tables and E3 at 1,000
and 7,200 snapshots, and the catalog bytes it reads fell 8-32x. At E1's 1,000
tables, past the knee, 0.3-2.6% of merge appends exhausted their retries with
tail reads: writers that learned the exact record count compacted together and
lost to each other, which compaction after the commit (step 6b) answered. Also
measured on S3 Standard, where a longer object is now read whole at once
(0a00685; TAIL_READS.md "Measured on S3 Standard").

One request a read (step 7, merged 2026-11-12: fileio 3b6c71c, fork
fbd1036bf9): a read is one GET on S3 and ADLS, whose replies report the
version and length they read, and the model charges the lookup it saves
(D23). Still to run: its real-cloud contract run, and `read_lookup` on and
off (the lookup's own cost).

**Lost races at high load: resolved 2026-11-12** (TAIL_READS.md "Measured
at E1's largest pointer point"). Catalog-bench's campaign `p4r` (fileio main
after step 6b) committed fewer merge appends a second than `e1-d20` (fileio
e505a0f) at E1's 3,000 pointer tables and 128 workers on S3 Express (37.8
against 49.9), losing several times the append races a commit from the run's
first minute. Campaign `tr6x` (`read_tail` and `compact.after.commit` on and
off) put the loss on tail reads: 49.9 with them off, 43.4 with them on and
36.0 with compaction after the commit too. Commit traces (`tr6y`) put it on
the resend's window, from the store's answer to the read a writer resends
from to its append taking effect, which bounds how close together landings
come: 12.8 ms after a probe and 19.1 after a tail read, against 19.6 ms
between the commits offered. Two causes, neither in tail reads: the model's
reads (D24), which favoured probes by 6.7 ms of window, and a fold that
rebuilt the whole catalog state (D25), 6.7-8.2 ms of CPU a resend at 3,000
tables. Fixed in turn (campaigns `tr7y` and `tr8y`), tail reads went to 47.5
and then 49.8 merge appends a second, the offered rate, and beat the probes:
a p50 of 222 ms against 767 (p99 2,971 against 9,656), 2.0 lost appends a
commit against 5.1, 16 writers in the commit loop against 77, and 19 ms of
CPU a merge append against 37. The probes, slower than `e1-d20`, read a
trailer inside their window, which the catalog before format 3 did not.

Not measured again: E1's 300 tables at 256 workers, the driver pool's limit
in every arm of `tr6x`, `p4r` and `e1-d20`, which did not tell the arms
apart; it runs again on the merged mains.

### D16. Replay rewrites the whole committed set, and every checkpoint carries it

The committed set holds every id within the retention
(`fileio.catalog.committed.retention.ms`, 30 minutes by default since
2026-10-28, 6 hours before; LOG.md), so at a steady rate it holds rate ×
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
- **Page out the old ids** (done: [COMMITTED_PAGING.md](COMMITTED_PAGING.md)
  stages 1-4, built 2026-11-04, on main and calibrated on havoc 2026-11-05,
  admission window A 5 minutes and page threshold T 1 MiB by default). The horizon splits in
  two: a floor below which replay admits no record, and the horizon below which
  lookups are unknown. The ids between them move to immutable pages, which the
  checkpoint names by node and digest. A `PageOut` record seals the oldest block of the set
  and raises the floor; it applies only if the block's digest matches what its
  writer paged. The blocks are the maximal nodes of a binary trie over the ids'
  timestamps holding at most 2^16 ids: deterministic, sized by density, never
  pivoting on an id. The checkpoint then carries rate × the admission window
  (5 minutes proposed), not rate × the retention. Replay never reads a page,
  and a writer's lookup in the paged range fetches it and is exact.
- **Derive the retention.** 6 hours was chosen as "longer than any commit's
  retries"; the default is 30 minutes since 2026-10-28, which makes the set 12
  times smaller. What the set must cover is how late a copy of a record can
  land, which a store may bound (SigV4 and Azure Shared Key reject a request
  signed about 15 minutes earlier; to be checked per store), and how long a
  writer may ask whether its commit landed, which the snapshot oracle (LOG.md)
  answers past the horizon. With paging, only the admission window sizes the
  checkpoint, and the retention could grow again cheaply.

**Resolved 2026-11-05** by the first two fixes. At 1,000 records a second's
30-minute steady state (ingest campaign `i11-paging`, a compaction every 100
records), paging holds the inline set at the ids inside A: 3.4 MB at the
median compaction against 19.8 MB kept whole, and the checkpoint at 4.1 MB
against 20.4 MB. Still open: deriving the retention and A per store (the third
fix; COMMITTED_PAGING.md "Open questions"), and the stamps D21 leaves
unbounded.

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
- **Built** as [SNAPSHOT_PAGING.md](SNAPSHOT_PAGING.md) steps 1-5
  (2026-11-05/06, on main; on by default since step 5, block 256 and window
  1,024): replay applies snapshot updates and expiry natively on a head without
  history, lists by ordinal and as deltas, lazy lists (fork), and old
  snapshots sealed into pages by sequence number, which replay never reads.
- **Evict by size**: eviction is by update size only
  (`fileio.catalog.inline.max.update.size`), so a table whose deltas fit
  stays inline however large its lists grow. A per-table checkpoint-size
  threshold would evict it to pointer mode. Deferred (SNAPSHOT_PAGING.md
  step 6, cold-table eviction).

**Resolved 2026-11-06** by the first four options (SNAPSHOT_PAGING.md
"Sizes"). An ML table of merge appends at E3's 7,200 snapshots holds 0.36 MB
in the root, against 2.38 MB before, and 1.07 MB in 24 pages. On S3 Express a
merge append's p50 went from 904 to 109 ms (catalog-bench e3-d20 against
e3-sp4), and it reads no pages. Still open: a fast-append table's head list
names every manifest it has, which only a merge shrinks; and the paged root
keeps about 18 B a sealed snapshot.

### D19. Compaction is due by the whole object's size, so a large checkpoint compacts on every commit

`CompactionPolicy.limits` (intentlog-core) compacts when the log holds
`max.append.count` records or when `bytes + recordBytes > maxBytes`, and
`bytes` is the whole object: checkpoint and tail. `fileio.catalog.max.append.size`
(16 MB) so bounds the object, not the records after the checkpoint. Once the
checkpoint alone passes it, compaction is due on every commit: each commit
replaces the whole object by a CAS instead of appending its record, and the
writers race whole-object replaces, each lost race another read and encode of
the catalog.

catalog-bench measured it (2026-11-02; its DESIGN.md "P4 findings", lab page
`e3-depth`): 10 inline-ML tables aged 7,200 snapshots each hold 72,000
snapshots, each with its list whole (D18), a 23.9 MB object. On S3 Express
(`s3x-2026-11`), 8 writers offered 1.7 merge appends a second: every commit was
a CAS (0 appends, 1.0 landed and 5.1 lost CASes a commit), a merge append wrote
about 145 MB and read about 224 MB, and the runs committed 0.27 a second. At
1,000 snapshots the object is 3.3 MB and commits append. On a store without
append (S3 Standard, GCS) every commit replaces the object anyway, so there the
checkpoint's size is every commit's cost.

The knobs bound different things. The record count is a soft limit sized to
a provider's caps (the S3 Express part count; Bug 66's SegmentLimitException
handles the cap itself), and bounds what a reader replays after the
checkpoint; the size is a performance target. Neither should make a
checkpoint that is merely large compact on every commit. The direction, to
design with paging:

- **Bound the tail, not the object.** Compact when the records after the
  checkpoint cost a reader more than a compaction, amortized over the commits
  until the next, costs a writer (D14's "compact by cost"): the tail's bytes
  and replay time against the checkpoint's. A provider's hard limits (an
  object's or an append's size, Azure's 100 MiB append, the S3 Express part
  count, SegmentLimitException) stay as caps.
- **Bound the checkpoint by paging.** What a compaction writes is the
  checkpoint, so its size is a policy too: page out the committed set's old
  ids (D16) and a table's old snapshots with their lists (D18), so the
  checkpoint holds what replay needs (the admission window, recent
  snapshots) and names immutable pages for the rest. Compaction policy (when
  to rewrite the checkpoint) and paging policy (what the checkpoint holds)
  together bound both the write amplification and every read's cost (D15,
  D20).

**Stopgap, 2026-11-02 (docs/CHECKPOINT_PLAN.md R0.1):** the policy sees the
checkpoint's size apart from the log's (`CompactionPolicy.Input`, from the
replica's `checkpointBytes`), and `max.append.size` bounds the records after
the checkpoint (`fileio.catalog.max.append.size.basis` = `tail`, the default;
`object` is the behaviour above, kept for ablation). A large checkpoint now
compacts when its tail reaches the count or the size, not on every commit
(`TestProtoCommitKnobs`). Compaction itself got cheaper with it: a checkpoint
writes each inline table unchanged since a decode or an earlier encode as the
bytes it had (R0.2, `fileio.catalog.checkpoint.memoize`). The checkpoint is
still unbounded, and every compaction and fresh read still moves it whole:
paging (R2) is the fix.

**Resolved 2026-11-10.** The checkpoint is bounded by paging: the committed
set's (D16) and old snapshots' (D18). At E3's 7,200 snapshots a table's root
is 0.36 MB, against 2.38 MB before. A warm reader reads only the records after
its replica (D15, tail reads). A compaction runs after its writer's commit, at
a point it draws past the limits, instead of the next commit replacing the
object (TAIL_READS.md "Compaction after the commit"). What remains is D14's
cost-based policy: the limits are still fixed numbers.

### D20. A commit reads the whole catalog object four times (perf bug)

A commit through Iceberg's API reads the catalog object four times, each
time whole, in every mode. catalog-bench counted exactly four catalog GETs a
merge append, pointer, inline TM and inline ML alike, from 100 to 3,000
tables (2026-11-02; lab pages `f0-sizing`, `e1-knee`):

1. `loadTable`: `FileIOTableOperations.doRefresh` reads the log
   (`CatalogLog.read`).
2. `SnapshotProducer.apply` refreshes before building the snapshot
   (`SnapshotProducer.java:349`), milliseconds after 1.
3. The commit reads through its own write (`LogRuntime`:
   `log.readThrough(receipt)`, which `AtomicFileLog` answers with a parse of
   the whole object written) to learn whether replay applied its record.
4. `SnapshotProducer.commit` refreshes after committing
   (`SnapshotProducer.java:841`), to find the snapshot it made for its
   cleanup.

Each is a GET of the whole object (D15) and a parse that copies its
checkpoint and every record (`AtomicFileLog.decode`) before a replica folds
only what is new, so a commit's cost grows with the catalog, not with its
table. At 300 tables aged 100 snapshots on S3 Express, a merge append read
31.7 MB in ML (a 7.6 MB object, four times), 11.2 MB in TM and 1.7 MB in
pointer mode; its median took 317, 172 and 106 ms, against 67 ms for JDBC on
the same store, which reads its own table's row and metadata file. In E3 TM
reads 18.8 MB a commit at 7,200 snapshots. In the driver it is CPU and
garbage too: one client aging 10 inline-ML tables to 7,200 snapshots took 9
hours, about the square of the depth.

Only the first is needed:

- **3 after a conditional append.** An append conditional on the tail the
  writer read (S3 Express's write offset and `If-Match`, Azure's append
  position) lands right after the state on which the writer checked its
  intention's preconditions, so replay there applies it exactly as the
  writer did: the replica it lands in is the writer's plus its record,
  known without a read. The read through stays for an unconditional append,
  and for a write whose reply was lost (ambiguous, P3).
- **4 from the landed replica.** The commit has just produced the replica
  its record landed in; the table's operations can take it as their current
  state, so the refresh after a commit reads nothing.
- **2 from 1's replica.** A refresh milliseconds after a read can confirm the
  object is unchanged with a conditional GET (`If-None-Match` on the version
  it read) or read only the bytes after it (D15's `readAfter`).

A commit then reads once, and with D15 reads only the records it does not
hold, instead of four whole GETs. Iceberg's refresh contract stays: a
refresh served from a replica must still see what landed since (a
conditional or ranged read), and a stale base only costs a retry or a merge,
since replay judges every record where it lands.

**Fixed (3 and 4), 2026-11-02:** a merge append reads the catalog once, for
the refresh in `apply` (`TestCommitReads`, in every one-object mode).

- 3: `AtomicFileLog` answers a read through a write from the write when the
  write says what the object holds: a replace holds what it wrote, and an
  append conditioned on a tail whose bytes the log read or wrote holds those
  bytes and the record. The condition names the version the tail is bound
  to, and no version recurs (each write lands a record with an id of its
  own), so the append landed on exactly those bytes; on S3, whose reads are
  not pinned (T6), a read lands between the lookup that bound the version
  and the append, so it read that version. An append from a tail that only
  a probe found (a resend after a lost race) reads the object back, as
  before. `LogRuntime` is unchanged: P3 holds, the view being the log the
  instant the write landed.
- 4: `FileIOTableOperations.commit` takes the replica the commit landed in
  as the table's current state, as Iceberg's REST operations take the
  metadata a commit returns, instead of requesting a refresh; the fork's
  `SnapshotProducer.commit` looks the committed snapshot up in
  `ops.current()` and refreshes only if it is not there. Upstream refreshed
  there since #15511 (2026-03, the snapshot the catalog committed, not the
  one staged, decides the cleanup), which brought back the load #14709
  (2026-02) had removed from `MergingSnapshotProducer.updateEvent` for REST.
  `current()` after a commit is the table as the catalog committed it on
  every catalog: a REST commit's response, a metastore catalog's refresh
  (`requestRefresh`), FileIOCatalog's landed replica (with any merge replay
  made). A pointer table still reads the metadata file it just wrote, as a
  metastore catalog does.
- A commit can now run from the replica a write returned, whose tail names
  the version the write made. GCS answers `exists()` of such a file by
  looking that generation up, which a later write removes, so a raced commit
  took the log for absent and failed as a lost create ("Catalog creation
  failed"; the emulator's `CatalogTests` conflict tests caught it). A tail now
  exists if the log read or wrote its bytes, without asking the store
  (`LogTail.exists` is the tail's, not the object's now), and strict versions
  in `InMemoryAtomicFileIO` (the fuzzers draw them) look a replaced
  generation up as GCS does (`TestCommitReads`, `TestAtomicFileLog`).

Still open: 2 needs a read that sees what landed since 1 without reading it
whole (D15's ranged `readAfter`, or a conditional GET the FileIO API does
not have), and 1 reads the whole object even for a client that holds a
replica.

**Resolved 2026-11-10** (2 and a warm client's 1, by D15's tail reads). Every
load and refresh through `CatalogLog` reads only what follows the replica its
catalog holds (`LogRuntime.read(floor)`). So a merge append's two reads, its
load and the refresh in `apply`, fetch the records since, not the object. At
E1's 300 inline-ML tables on S3 Express (catalog-bench `tr6`) a merge append
read 1.55 MiB of the catalog, against 12.5 with whole reads, in 2.2 GETs. The
two reads stay: Iceberg's refresh contract asks for both. Each is one GET on
S3 and ADLS since step 7, a HEAD and a GET on GCS (D23).

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
COMMITTED_PAGING.md keeps the exposure: its floor follows the oldest block,
not a clock, but a compactor's ceiling (now less the admission window) does,
so one far ahead can seal every block but the newest.

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

**Trigger to revisit:** writers whose clocks one operator does not control, or
COMMITTED_PAGING.md built.

### D22. A format upgrade to v3 evicts an inline-ML table

A v3 table gives every data row an id (row lineage). The first commit after
an upgrade gives each data manifest it keeps, and that had none, a first row
id (`SnapshotProducer`), so the new snapshot lists that manifest's path with
an entry that differs from the one the table's pool holds. A pooled path's
entry never changes (SPEC_ML.md; CHECKPOINT_PLAN.md decision 3), so the
writer evicts the table instead (`LogCatalog.restatesAManifest`): it writes
a metadata file and a list file per snapshot. An inline-TM table is not
affected: its lists are files.

Corrected 2026-11-06: the eviction is not for good. The table's next commit
through an inline writer re-inlines it (SPEC_TM.md, "Pointer → Inline"): a
pointer table has no pool, so its new lists start one with the upgraded
entries, and the old snapshots keep the list files the eviction wrote; with
snapshot paging, a table of any depth comes back, its history in import pages
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

### D23. The latency model charged no HEAD for a lookup after an open

The benchmarks' latency model (`LatencyFileIO` for FileIOCatalog,
catalog-bench's `ModelStore` for every client) took a stream's open as
binding its file, so a lookup after the open (`getLength`) cost nothing. On
S3, ADLS and GCS a stream sends nothing until it reads, and a lookup before
its first read is a HEAD. So the model charged every FileIOCatalog read
(open, look up, read) one request less than a store does (on `s3x-2026-11`
about 6 ms, on S3 Standard about 27 ms; two or more reads a commit), and
every client's read of a file whose length Iceberg looks up after opening it
(an Avro manifest list: `AvroIterable` opens a stream, then asks the
length).

**Resolved 2026-11-11** (TAIL_READS.md step 7; fileio c2c1ea3, merged
2026-11-12): the model charges a lookup of a file nothing bound as a HEAD, and
opening a stream binds nothing; a GET binds its file only where the store's
reads record their version and the client keeps it (the fork's S3 and ADLS
FileIOs; not a stock client's, nor GCS's). And `AtomicFileLog` reads first where
reads record their version, one request a read. What stays: every campaign
before the merge ran under the old model. Comparisons within a campaign stand
(each arm's reads alike short), and absolute numbers are a HEAD a read low; the
paper's runs are taken under the new model. A stock client's manifest-list read
is a HEAD and a GET, as on S3, for JDBC, Polaris and FileIOCatalog's pointer and
inline-TM modes alike; a client fix (Avro's length asked lazily, and a file that
keeps the length its GET reported) would save it for every client, and is not
made (`core-perf` has none).

Both models price a lookup as a GET of no bytes (`readMs(0)`), not as the
profile's measured HEAD (`StoreProfile.headMs`, which only `azure-2026-11`
measures: 5.85 ms against a GET's 9.84 base), so on that profile every lookup
costs about 4 ms too much: FileIOCatalog's probes (tail reads off) and its
lookups under `fileio.catalog.read.lookup`, and every client's manifest-list
lookup above. Not fixed yet: `LatencyFileIO` and catalog-bench's `ModelStore`
should charge `headMs`.

### D24. The latency model's reads saw the store at their ends and starts

`LatencyFileIO` (FileIOCatalog's latency model) took a conditional write as
acting halfway through its latency (endive's split), but a lookup (a HEAD) as
seeing the object at the end of its latency, and a GET as serving the version
the store held at its start. A store answers a request at one point while it is
in flight, about halfway for all three. The difference decides races: what a
writer knows when it sends is as old as the time since the store answered the
read it learned it from. A writer resending after a probe knew the object as of
the probe's reply, half a request fresher than a store would have told it; one
resending after a tail read knew it as of the GET's start, half a request
staler. On `s3x-2026-11` that is about 6.7 ms per resend against tail reads
(half a HEAD and half a GET).

It decides throughput where landings come as close together as a resend's
window. Campaign tr6y (commit traces at E1's 3,000 tables and 128 workers on
`s3x-2026-11`, pointer, tail reads on and off; catalog-bench lab campaign
`tr6y`) measured each resend's window, from the instant the model's store last
answered the writer's read to its append taking effect: 12.8 ms after a probe
(a pinned read of the probed version's last trailer, 8 ms, then half an
append), 19.1 ms after a tail read (the GET, 7.4 ms; folding the records and
building the replica, 6.8 ms of CPU; half an append; catalog-bench's lab page
`commit-traces`). Landings came 17.8 ms
apart at the median with probes and 22.4 ms with tail reads, which sat at
capacity: 107 of 128 writers in the commit loop and 87% of resends lost, against
57 and 75%. With both reads answered halfway the windows are about 15.8 and
15.4 ms.

**Resolved 2026-11-12** (fileio 5a577d7): the model's store acts on a lookup, a
GET and a conditional write halfway through its latency (`TestLatencyFileIO`: a
write landing in a request's first half is seen, one in its second half is not).
With it (and step 7, which changed no request at this point), tail reads at E1's
3,000 tables went from 40.9 to 47.5 merge appends a second (campaign tr7y; D15).
What stays: every campaign before it ran under the old model. Its comparisons of
tail reads against probes and whole reads favour the probes wherever the commit
loop sets the limit (D15's at E1's 3,000 tables: p4r against e1-d20, tr6x,
tr6y); comparisons whose arms read alike stand. catalog-bench's `ModelStore`
answers a HEAD at its end too (its GET halfway); the objects it serves are
written once (metadata files, manifest lists), so no read can tell, and it is
left as it is.

### D25. A fold rebuilt the whole catalog state (perf bug)

Folding records onto a replica (a commit's read after a lost race, its read
through its own write, a client's refresh) rebuilt the catalog's whole state,
whatever the records changed. `CatalogState.toBuilder` copied each of the
state's maps keyed by table and namespace (about fifteen); `build` rebuilt the
namespace and table lookups from the entries and copied every map again; and
`LogRuntime.continued` decoded every record the replica held, to make sure none
was the commit's own. At E1's 3,000 pointer tables that was 6.7 ms of CPU per
resend in campaign tr6y (fold 3.4, build 3.3; tr7y 4.8 and 3.4 on a busier
host), in a commit loop whose window from knowing the log's tail to the append
taking effect is 15 to 20 ms: once the model's reads were answered halfway
(D24), most of what tail reads still cost against probes (tr7y: 47.5 merge
appends a second with tail reads, 49.8 without).

`FoldProfile` (test scope: two writers taking turns committing from a replica
the other just made stale, on 3,000 pointer tables with 5,000 records after the
checkpoint) reproduces it, and JFR places it: a fold's CPU was 70% `toBuilder`
and 29% `continued`; a build's 57% copying maps and 42% rebuilding lookups. A
build grew with tables (0.24 ms at 300, 0.82 at 1,000, 1.85 at 3,000).

**Resolved 2026-11-12** (fileio 2b9b5c6, 9a417d3, e0aceaf): a builder rebuilds
its lookups only after adding a namespace, whose path a checkpoint's arbitrary
order may not yet know (every other change keeps them; under the replay check
each build compares them with a rebuild); it shares the state's maps until it
changes them, and hands a state the maps it changed read only, copying them
again before a later change (`TestCatalogStateBuilder`); and a replica keeps the
ids of its view's intention records as replay decoded them, which `continued`
scans (`TestReplay`, `TestLogRuntime`; under the replay check each scan is
compared with a decode). At 3,000 tables and 5,000 records `FoldProfile`'s fold
now takes 0.49 ms (2.9), its build about nothing (1.85), and a whole commit 0.87
ms of CPU (7.3); at 300 tables, 0.19, 0.01 and 0.42. In the benchmark (campaign
tr8y, E1's 3,000 tables) a fold took 1.0 ms and a build 0.02 (tr7y 4.8 and 3.4),
and tail reads beat probes there (D15). What stays: a record still copies each
table-keyed map it changes, so a fold grows with tables, about a tenth as fast
(persistent maps would make it logarithmic); and every campaign before the fix
ran with the copies, so FileIOCatalog's CPU per commit grew with the catalog's
tables in them.

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

### T6. The fuzzers do not race a write between a read's lookup and its GET

Until 2026-11-09 the fork's S3 and GCS reads asked for no version.
`S3InputStream` sent each ranged GET with no `If-Match` and reopened the
stream mid-read with another; `GCSInputStream` read the live generation, not
the one the file's lookup found. So an `AtomicFileLog` read (a lookup, then
the bytes) could return the first bytes of a newer object than the one its
tail is bound to, or the start of one object and the rest of another, while
the fuzzers' `InMemoryAtomicFileIO` pinned every read. ADLS already asked for
one version.

Now every read asks for one version (fork 7b948d51ce): a file reads the version
it has seen, by `If-Match` (S3, ADLS) or `if-generation-match` (GCS), and on S3
and ADLS a file whose first request was a read has seen the version it found.
A read of a version a write superseded fails as
`SupportsAtomicOperations.StaleReadException`; `AtomicFileLog` reads again,
and a read through a superseded write reads the live object (ATOMIC_FILE_LOG.md
"Reads"). `SupportsAtomicOperationsContractTest` has three read cases: a file
that looked the object up reads that version after a replace (a longer one,
whose start would pass for the old version and appends) and after an append;
and a file whose first request was a read reads that version by a reopened
stream, a range read and a new stream, keeps its length, and fails a CAS
rather than replace a version it did not read (stores whose reads record
their version: not GCS, whose client library reports none). They passed on
every store on 2026-11-09 (fork 7a5d42de5e, `cloud-runs/2026-11-09-contract-b`:
S3 Express, S3 Standard, ADLS, GCS, GCS Rapid, each case its store
supports); S3 Express honours `If-Match` on a GET.

Since TAIL_READS.md step 2 (2026-11-10) the fuzzers race them too.
`InMemoryAtomicFileIO`'s read modes open a stream as S3's does, with no
request, and make its first read the GET, a request of its own that a fuzzer's
preemption can precede: the store then serves the bound version, or fails as
stale if a write superseded it (`PINNED`, as a GET with `If-Match`), the live
one (`LIVE`, as an unpinned GET), or the bound one's bytes up to an offset and
the live one's after (`SPLICE`, as a stream that reopened without pinning).
`OPEN`, the default, binds as the stream opens, as before.
`TestAtomicFileCommitProtocol` (a third of whose splices fall at a frame's end,
a third inside the checkpoint's frame), `TestMultiWriterProperties` and
`TestCatalogRaceProperties` draw the mode per try, and format 3's checks (S5)
reject what an unpinned store serves torn. `TestCatalogFaultProperties` draws
`OPEN` or `PINNED` only: its model runs a preempting operation first, which a
read served from the version before the preemption breaks without the catalog
being wrong (a read that lags by whole records is a prefix, P1); round
`tr5b-r0` failed its fault jobs so, until `PINNED` failed every read of a
superseded version and the fault fuzzer drew no other.

What remains:

- The accelerated read paths (S3's analytics accelerator, GCS's analytics
  core; both off by default) read whatever is live: a catalog's FileIO must
  leave them off. Format 3 would reject a torn read of either, but a tail read
  (D15) through one would also lose step 0's guarantee that its bytes are the
  version its HEAD found.

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
