# Iceberg Core Issues

Bugs in Apache Iceberg that this project's fuzzers found. None is in
`FileIOCatalog`: each one reproduces on Iceberg's own `InMemoryCatalog`.
This page records them for a rebase onto a newer Iceberg. Each issue also
has a document of its own in [iceberg-issues/](iceberg-issues/), for
reports or pull requests upstream. It covers the reproduction, the cause
with links to the 1.12.0 source, the fix, its tests, and what to settle
before a PR.

Upstream fixed C1. The fork fixes C2 to C10 on branch `core-fixes`, each
in an upstream-style commit with its tests, so the project's tests no
longer work around any of them.

`TestIcebergCoreIssues` has a canary for each issue: a test on
`InMemoryCatalog` that asserts the bug is absent, with the message
"Iceberg issue C*n* is back". Every canary passes against the fork. A
canary that fails means a rebase dropped the fork's fix, or a release
changed the behavior. The canaries assert only that the bug is absent,
not the fork's way of fixing it, so a different fix upstream passes too.
They matter most for C3 and C6, which the fuzzers don't reach reliably.
The tests use only iceberg-core, so they also run in Iceberg's own test
tree, where on stock 1.12.0 every canary but C1's fails. From the
fileio-catalog directory, with the clone in a temporary directory:

```bash
tmp=$(mktemp -d)
git clone --depth 1 --branch apache-iceberg-1.12.0 https://github.com/apache/iceberg.git "$tmp/iceberg"
mkdir -p "$tmp/iceberg/core/src/test/java/org/apache/iceberg/io"
cp src/test/java/org/apache/iceberg/io/TestIcebergCoreIssues.java \
   "$tmp/iceberg/core/src/test/java/org/apache/iceberg/io/"
cd "$tmp/iceberg" && ./gradlew :iceberg-core:test --tests org.apache.iceberg.io.TestIcebergCoreIssues
```

## Status

Checked on 2026-10-16 against Iceberg 1.12.0 and upstream main at
`524fe9e`. On 2026-10-17 the atomicio fork moved from the 1.10.1 release
to the 1.12.0 release. By 2026-10-18 its branch `core-fixes` had all six
fixes, C2 to C7 (see "Rebasing onto a newer Iceberg"). On 2026-10-18 the
per-issue documents were checked against main at `9bbee07`: the files
each fix changes are unchanged there since `524fe9e`, and each fix
cherry-picks onto it (C2's test file only after C7's). C8, found the same
day, was fixed on `core-fixes` on 2026-10-19, as were C9 and C10, found
that day; C9 was checked against main at `6890694bd`, C10 at
`ddbd99073`. The Fork column is
`atomicio`, which sits on `core-fixes`.

| Issue | Fork | 1.12.0 | main | Upstream |
|---|---|---|---|---|
| C1. A merging delete renumbers rows | fixed | fixed | fixed | fixed by [#16263](https://github.com/apache/iceberg/pull/16263), in 1.11.0 |
| C2. A retried commit renumbers rows | fixed, `core-fixes` 6c6b6fd | yes | yes | not reported |
| C3. A replace restores an expired snapshot | fixed, `core-fixes` 56527cb | yes | yes | [#16942](https://github.com/apache/iceberg/issues/16942), part of [#16232](https://github.com/apache/iceberg/issues/16232); fix in PR [#16943](https://github.com/apache/iceberg/pull/16943), open |
| C4. A stale handle fails on a newer spec | fixed, `core-fixes` bfc20da | yes | yes | not reported |
| C5. Rewriting manifests fails with no snapshot | fixed, `core-fixes` f723a52 | yes | yes | not reported |
| C6. A repeated cherry-pick leaves the table unplannable | fixed, `core-fixes` 724608a | yes | yes | not reported |
| C7. The all_* tables miss row ids | fixed, `core-fixes` 2779515 | yes | yes | not reported |
| C8. A DV older than its data file leaves the table unplannable | fixed, `core-fixes` 1d46241 | yes | yes | not reported |
| C9. A create-or-replace replaces a table created since it began | fixed, `core-fixes` b7a2abd | yes | yes | not reported; PR [#16943](https://github.com/apache/iceberg/pull/16943) would change it (see C9) |
| C10. An expiry that retries fails on what its first attempt removed | fixed, `core-fixes` 93e8831 | yes | yes | not reported |

"Not reported" means a search of apache/iceberg's issues and pull
requests found nothing for the issue.

## Rebasing onto a newer Iceberg

The fork fixes these issues itself, each in a commit of its own on branch
`core-fixes`, which sits on the release; `atomicio` sits on `core-fixes`.
Each fix carries an upstream-style test in iceberg-core, and the issue's
workarounds here are gone.

1. Rebase `core-fixes` onto the new release first. Drop each fix that
   upstream merged, and record the release here. A fix that upstream
   merged comes out empty or conflicts. Before dropping it, check that
   its tests and its canary in `TestIcebergCoreIssues` pass on the new
   release without it.
2. Rebase `atomicio` onto `core-fixes`.
3. Run the full `iceberg-core` tests on `core-fixes`. Then run
   fileio-catalog's suite on the published fork: its canaries
   (`TestIcebergCoreIssues`) must all pass. Then run the fuzzers long:
   `TestCatalogFaultProperties`, `TestCatalogRaceProperties`,
   `TestInlineDifferentialProperties` and `TestInlineDeltaProperties`,
   which drive Iceberg's APIs, and `spec/TestMultiWriterProperties`, at
   the catalog-format layer. Each sets its tries in
   `@Property(tries = …)`, which no system property overrides (jqwik's
   `jqwik.tries.default` applies only to properties without one), so
   raise them in the source for the run and put them back after. Run one
   with `mvn test -Dtest=TestCatalogFaultProperties`.

## Issues

### C1. A delete that merges manifests gives existing rows new ids

Details: [iceberg-issues/C1.md](iceberg-issues/C1.md).

On a v3 table, a delete whose commit merges manifests gives an existing
data file a new `first_row_id`. In the test, file f6 went from row id 1
to 6. The spec requires the inherited `first_row_id` to be written into
existing and deleted entries when a manifest is rewritten. Without the
merge, f6 keeps its id.

- **Upstream:** fixed by #16263, "Core: Fix row ID assignment for
  EXISTING entry during a manifest merge", released in 1.11.0.
- **Removed on the rebase onto 1.12.0:** the test,
  `aDeleteThatMergesManifestsRenumbersRows`, and the workaround,
  `TestCatalogFaultProperties.V3_PROPERTIES`, which turned off manifest
  merging on v3 tables. The fuzzer merges manifests on v3 tables again,
  and its row-lineage check stays.
- **Canary:** `aDeleteThatMergesManifestsKeepsRowIds`.

### C2. A retried commit gives a manifest a second row-id range

Details: [iceberg-issues/C2.md](iceberg-issues/C2.md).

Suppose a commit had to retry because another commit landed first, and
that other commit gave row ids to a manifest the retrying commit carries,
one written before a v3 upgrade. The retry then gave the manifest a
second range: its data files' row ids changed.

- **Cause:** four caches kept what a commit made on an earlier attempt,
  keyed by `ManifestFile` equality (the path), from the copy of the
  manifest that attempt saw, without `first_row_id`:
  - `SnapshotProducer`'s manifests with metadata (the manifests a commit
    carries: a fast append);
  - `ManifestFilterManager.filterManifest` (kept and rewritten manifests:
    a delete);
  - `ManifestMergeManager.createManifest` (merged bins, keyed by lists of
    manifests: a merge append);
  - `BaseRewriteManifests.requiresRewrite` (a manifest rewrite).
- **Fork:** fixed on `core-fixes` by 6c6b6fd, "Core: Keep the row ids of
  the manifests a retried commit carries". `SnapshotProducer` no longer
  caches manifests that need no metadata, and the other three redo their
  work for a manifest whose `first_row_id` changed. Its tests are the
  `*RetryAfterUpgrade` cases in `TestRowLineageAssignment`, one for each
  cache; each fails without its cache's part of the fix.
- **Removed with the fix:** the test,
  `aRetriedCommitGivesAManifestASecondRowIdRange`, and the workaround,
  `TestCatalogFaultProperties`'s `preUpgrade` map, which exempted the
  files a table had at its v3 upgrade from the check that a row id never
  changes.
- **Canary:** `aRetriedCommitKeepsAManifestsRowIds`.

### C3. A replace transaction brings back a snapshot expired since it began

Details: [iceberg-issues/C3.md](iceberg-issues/C3.md).

A replace transaction commits the table it built at its start, snapshots
included, over whatever changed since
(`BaseTransaction.commitReplaceTransaction`). A snapshot expired in
between therefore came back, though the expiry deleted its manifest
list, so reading that snapshot failed.

- **Upstream:**
  - #16232 (open) reports the general problem: a replace silently
    overwrites concurrent schema, property, data and expiry changes.
  - #16942 (open) reports a concurrent writer's snapshot dropped by a
    `createOrReplace`.
  - PR #16943 (open, by 2026-10-12) would fix both for
    `BaseMetastoreCatalog` catalogs, `FileIOCatalog` among them. When the
    table changed, it rebuilds the replacement on the refreshed metadata
    and replays the transaction's updates. It falls back to today's
    behavior when the rebuilt schema, spec, sort order or format version
    would differ, or an update can't be replayed.

  Iceberg means a replace to win over concurrent changes, but it should
  not restore what an expiry removed.
- **Fork:** fixed on `core-fixes` by 56527cb, "Core: Don't bring back
  snapshots expired during a replace transaction". When a retry finds the
  table changed, the replacement drops the snapshots the table had at the
  start but no longer has, and with them any ref that named one. A
  replace still wins over other concurrent changes (A1). The fix is
  smaller than PR #16943, and also applies where that PR falls back to
  today's behavior. Its test is
  `TestReplaceTransaction.testReplaceTransactionWithConcurrentExpiry`.
- **Removed with the fix:** the test,
  `aReplaceBringsBackASnapshotExpiredSinceItStarted`, and the workaround,
  `TestCatalogFaultProperties`'s check of manifest lists only for the
  snapshots a ref reaches: it checks every snapshot again. Its
  `REPLACE_TABLE` model now drops a ref of the replacement whose snapshot
  was expired since the replace began.
- **Canary:** `aReplaceDoesNotBringBackASnapshotExpiredSinceItStarted`.

### C4. A stale handle fails on a manifest in a newer partition spec

Details: [iceberg-issues/C4.md](iceberg-issues/C4.md).

A handle loaded before another writer added a partition spec couldn't run
some commits once the table had a manifest in the new spec. These failed
with a `NullPointerException`:
- a delete by path;
- an overwrite by row filter;
- a dynamic partition replace;
- a row delta that removes a delete file;
- a manifest rewrite.

- **Cause:** `MergingSnapshotProducer`'s filter managers and
  `BaseRewriteManifests` took the partition specs of the handle's metadata
  when they were created. Their commit then refreshed to metadata with a
  spec they didn't have.
- **Fork:** fixed on `core-fixes` by bfc20da, "Core: Use the partition
  specs of the metadata a commit applies to". The filter managers and the
  rewrite now look specs up in the metadata each commit attempt applies
  to, as the merge managers already did. Its tests are the
  `*WithConcurrentSpecChange` cases in `TestDeleteFiles`, `TestOverwrite`,
  `TestReplacePartitions`, `TestRowDelta` and `TestRewriteManifests`.
- **Removed with the fix:** the test,
  `aStaleHandleFailsOnAManifestInANewerSpec`, and the workarounds in
  `TestCatalogFaultProperties`: `staleSpecs`, which turned the exception
  into a rejection for merge appends, deletes and row deltas, and the
  `rewriteManifests` op's catch.
- **Canary:** `aStaleHandleCommitsOnAManifestInANewerSpec`, for each
  commit above but the row delta.

### C5. Rewriting manifests fails on a table without a current snapshot

Details: [iceberg-issues/C5.md](iceberg-issues/C5.md).

`rewriteManifests` threw a `NullPointerException` when the table had no
current snapshot, either because it never had one or because a replace
left it with none after the handle was loaded.
`BaseRewriteManifests.apply` dereferenced the current snapshot of the
metadata it commits on.

- **Fork:** fixed on `core-fixes` by f723a52, "Core: Rewrite the manifests
  of a table without a current snapshot". With no current snapshot, the
  rewrite has no manifests to rewrite. It commits a snapshot without
  manifests, as it commits a snapshot that keeps every manifest when it
  has nothing to rewrite, and as an empty append does. (Committing nothing
  would need a way for a `SnapshotProducer` to skip its commit, decided
  again on each retry.) Its tests are `testRewriteManifestsWithoutSnapshot`
  and `testRewriteManifestsWithConcurrentReplace` in
  `TestRewriteManifests`.
- **Removed with the fix:** the test,
  `aRewriteOfManifestsFailsOnATableWithoutASnapshot`, and the workarounds
  in `TestCatalogFaultProperties`: it didn't generate a rewrite for a
  table its model said had no snapshot, and its `rewriteManifests` op
  caught the exception from a concurrent replace.
- **Canary:** `aRewriteOfManifestsRunsOnATableWithoutASnapshot`, for both
  cases.

### C6. A repeated cherry-pick leaves a table it can't plan

Details: [iceberg-issues/C6.md](iceberg-issues/C6.md).

Cherry-picking a snapshot a second time, once its earlier pick had been
expired, added the snapshot's data files again at a new sequence number.
Once one of those files had a deletion vector, planning failed: "DV data
sequence number (5) must be greater than or equal to data file sequence
number (7)".

- **Cause:** `CherryPickOperation` finds an earlier pick through the
  `source-snapshot-id` (or published `wap.id`) of the current snapshot's
  ancestors, and the expiry removed the ancestor that carried it.
- **Fork:** fixed on `core-fixes` by 724608a, "Core: Refuse a cherry-pick
  repeated after its earlier pick expired". The pick refuses data files
  already in the table when an expiry may have removed an earlier pick:
  the oldest remaining ancestor is newer than the picked snapshot, or the
  table is v1. Its tests are in `TestWapWorkflow`.
- **Removed with the fix:** the test,
  `aCherryPickRepeatedAfterAnExpiryLeavesATableItCannotPlan`, with
  `TestIcebergCoreIssues` (the last of its tests; the class came back with
  the canaries), and the workaround,
  `TestInlineDifferentialProperties.files`'s comparison of a planning
  failure as the string "fails: ...".
- **Canary:** `aCherryPickRepeatedAfterAnExpiryIsRefused`.

### C7. The all_* metadata tables miss the row ids of an upgraded file

Details: [iceberg-issues/C7.md](iceberg-issues/C7.md).

The `all_*` metadata tables read every snapshot's manifest list and kept
one copy of each manifest, whichever they read first:
`BaseAllMetadataTableScan.reachableManifests` filled a `HashSet` from a
`ParallelIterable`. A manifest carried across a v3 upgrade has no
`first_row_id` in a list written before the upgrade. So `all_data_files`
could report no row id for a data file that has one, and which copy it
reported could vary between runs.

- **Fork:** fixed on `core-fixes` by 2779515, "Core: Keep the row ids of
  upgraded manifests in all_* metadata tables". The scan keeps, for each
  manifest path, the copy with an assigned `first_row_id`. Its test is
  `TestRowLineageAssignment.testAllDataFilesAfterUpgrade`, which plans on
  one thread so the lists are read oldest first.
- **Removed with the fix:** the test,
  `allDataFilesMissesTheRowIdsOfAFileCarriedAcrossAnUpgrade`, and the
  workaround, `TestInlineDifferentialProperties.metadataTable`'s mask of
  `first_row_id` in the `all_*` tables.
- **Canary:** `allDataFilesHasTheRowIdsOfAFileCarriedAcrossAnUpgrade`.

### C8. A deletion vector older than its data file leaves a table Iceberg can't plan

Details: [iceberg-issues/C8.md](iceberg-issues/C8.md).

A row delta that does not validate its referenced data files can commit a
deletion vector for a file that is not live where it commits: through a
stale handle, or for a file only staged. A later commit that adds that
file, a cherry-pick of the snapshot that has it, gives the file a newer
data sequence number than its deletion vector, and planning the branch
fails: "DV data sequence number (5) must be greater than or equal to data
file sequence number (6)". The symptom is C6's, by another path: the pick
is a first pick, so C6's fix does not apply.

- **Cause:** `DeleteFileIndex.findDV` throws for a deletion vector whose
  data sequence number is below its data file's. The spec says when a
  deletion vector must apply, not that this state is an error. Nothing
  stops the commits that make it.
- **Found by:** `TestInlineDifferentialProperties`, 2026-10-18, at try
  999 of a 6,000-try run on the fork (core-fixes included); planning
  failed in both catalogs alike. Reproduces on 1.12.0 and on main at
  `524fe9e` (the files involved are unchanged at `9bbee07`).
- **Fork:** fixed on `core-fixes` by 1d46241, "Core: Skip a DV older than
  its data file, and refuse to add a data file a DV names". Planning skips
  such a DV with a warning, as the spec's rule implies, and a commit that
  adds a data file whose path a live DV names is refused. Its tests are
  in `TestWapWorkflow`, `TestFastAppend` and the delete-index tests.
- **Removed with the fix:** the workaround,
  `TestInlineDifferentialProperties.files`'s comparison of a planning
  failure with this message as "fails (C8): ...".
- **Canary:** `aDataFileADeletionVectorNamesIsNotAddedAgain`.

### C9. A create-or-replace replaces a table created since it began with a different one

Details: [iceberg-issues/C9.md](iceberg-issues/C9.md).

A create-or-replace transaction that began without its table, on a
metastore catalog, committed the new table it built over one another
writer created before its commit refreshed the table. The table took the
transaction's UUID and lost the created table's history; neither order of
the two commits gives that. When the create landed after the refresh, the
commit failed with `AlreadyExistsException` instead (A2), as it always
does on REST.

- **Cause:** `BaseTransaction.commitReplaceTransaction` commits the
  metadata it built at the start over whatever the refresh found, and a
  transaction that began without a table has no UUID to check the refreshed
  table against.
- **Found by:** `TestCatalogFaultProperties`, 2026-10-19, in both its
  properties (tries 8,202 and 18,447 of 30,000-try runs), shrunk to a
  create-or-replace preempted by a create of the same table. Reproduces on
  `InMemoryCatalog`.
- **Fork:** fixed on `core-fixes` by b7a2abd, "Core: Fail a
  create-or-replace when its table was created since it began". When the
  refresh finds a table where the transaction began without one, the
  commit fails with `AlreadyExistsException`, as it does when the create
  lands later. Its tests are
  `TestReplaceTransaction.testCreateOrReplaceTransactionWithConcurrentCreate`
  and `CatalogTests.testCreateOrReplaceTransactionConcurrentCreate`, which
  expected this all along but ran only on REST catalogs; it now runs on
  every catalog, `FileIOCatalog`'s modes in `TestLocalCatalog` included.
- **No workaround** to remove: the fuzzer's model expected a serial
  outcome.
- **Canary:** `aCreateOrReplaceKeepsATableCreatedSinceItBegan`.

### C10. An expiry that retries after a conflict fails on what its first attempt removed

Details: [iceberg-issues/C10.md](iceberg-issues/C10.md).

`ExpireSnapshots` kept the snapshots its earlier attempts chose to remove
when it retried after a `CommitFailedException`. If another writer had
expired them in between, the retry failed with `IllegalArgumentException:
arraySize cannot be negative`; if another writer had tagged one, with
"Cannot expire ... Still referenced by refs".

- **Cause:** `RemoveSnapshots.internalApply` adds each attempt's choice to
  `idsToRemove`, the set of ids the caller named, and a retry starts from
  it.
- **Found by:** `TestCatalogFaultProperties`, 2026-10-19 (try 13,678 of a
  30,000-try run): an expiry with metadata cleanup, preempted by another
  writer's expiry of the same table. Reproduces on `InMemoryCatalog`.
- **Fork:** fixed on `core-fixes` by 93e8831, "Core: Work out what an expiry
  removes on the metadata each attempt commits on". Each attempt works out
  what to remove on its own metadata: the snapshots it has that the caller
  named or that it does not retain. Its tests are
  `TestRemoveSnapshots.testRetryAfterConcurrentExpiry` and
  `testRetryAfterConcurrentTag`.
- **No workaround** to remove: the fuzzer counted the exception as
  unexpected.
- **Canary:** `anExpiryRetriesAfterAConcurrentExpiry`.

## Accepted Iceberg Behavior

The oracles accept these behaviors as Iceberg's semantics, not as
issues. A release that changes one of them breaks the oracle that
accepts it, so a rebase finds out anyway.

### A1. A replace transaction wins over concurrent changes

The replace commits the metadata it built at its start, whatever
changed since (C3 is the case where that is clearly wrong). So
`TestCatalogRaceProperties` doesn't race replace transactions, and
`TestCatalogFaultProperties` models a replace as the table the
transaction saw at its start. PR #16943 (C3) would make a replace keep
concurrent changes where it can. A release with it breaks that model,
which should then expect the concurrent changes kept, and replace
transactions can join the race property.

### A2. A create-or-replace that races a create fails

A create-or-replace transaction that finds no table, while another
writer creates one before it commits, fails with
`AlreadyExistsException`. It doesn't replace the new table.
`TestCatalogFaultProperties` counts that failure as a rejection. Until
C9's fix, it did so only when the create landed after the commit's
refresh of the table.

### A3. A handle to a re-created table can't commit

A commit through a handle to a table that was dropped and created again
under its name fails with `IllegalStateException` ("Table UUID does not
match"), not `CommitFailedException`. `TestCatalogRaceProperties`
(`REPLACED`) and `TestCatalogFaultProperties` accept that failure.

### A4. A delete file is checked against the version its commit lands on

Since 1.12.0 (#16146, "Core: Validate v2 deletes against concurrent
format upgrade"), a row delta checks each delete file against the format
version of the metadata its commit lands on, not the handle's. A handle
loaded while the table was v2 that adds a position delete file after
another writer upgraded the table to v3 fails with
`IllegalArgumentException` ("Must use DVs for position deletes in V3").
Before, the commit landed a v2 position delete in a v3 table.
`TestCatalogFaultProperties` counts that failure as a rejection.
