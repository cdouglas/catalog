# Iceberg Core Issues

Bugs in Apache Iceberg that this project's fuzzers found. None is in
`FileIOCatalog`: each one reproduces on Iceberg's own `InMemoryCatalog`.
The project's tests work around them, so this page records them for a
rebase onto a newer Iceberg, and for reports or fixes upstream.

`TestIcebergCoreIssues` has a test for each open issue. Each test asserts
what Iceberg does now, and its Javadoc says what Iceberg should do instead.
So a test passes while its issue remains and fails once a release fixes
or changes it, with the message "Iceberg issue C*n* no longer
reproduces". The tests use only iceberg-core, so they also run in
Iceberg's own test tree:

```bash
git clone --depth 1 --branch apache-iceberg-1.12.0 https://github.com/apache/iceberg.git
mkdir -p iceberg/core/src/test/java/org/apache/iceberg/io
cp src/test/java/org/apache/iceberg/io/TestIcebergCoreIssues.java \
   iceberg/core/src/test/java/org/apache/iceberg/io/
cd iceberg && ./gradlew :iceberg-core:test --tests org.apache.iceberg.io.TestIcebergCoreIssues
```

## Status

Checked on 2026-10-16 against Iceberg 1.12.0 and upstream main at
`524fe9e`. On 2026-10-17 the atomicio fork moved from the 1.10.1 release
to the 1.12.0 release, and its branch `core-fixes` began fixing these
issues (see "Rebasing onto a newer Iceberg"). The Fork column is
`atomicio`, which sits on `core-fixes`.

| Issue | Fork | 1.12.0 | main | Upstream |
|---|---|---|---|---|
| C1. A merging delete renumbers rows | fixed | fixed | fixed | fixed by [#16263](https://github.com/apache/iceberg/pull/16263), in 1.11.0 |
| C2. A retried commit renumbers rows | fixed, `core-fixes` 6c6b6fd | yes | yes | not reported |
| C3. A replace restores an expired snapshot | yes | yes | yes | [#16942](https://github.com/apache/iceberg/issues/16942), part of [#16232](https://github.com/apache/iceberg/issues/16232); fix in PR [#16943](https://github.com/apache/iceberg/pull/16943), open |
| C4. A stale handle fails on a newer spec | fixed, `core-fixes` bfc20da | yes | yes | not reported |
| C5. Rewriting manifests fails with no snapshot | fixed, `core-fixes` f723a52 | yes | yes | not reported |
| C6. A repeated cherry-pick leaves the table unplannable | yes | yes | yes | not reported |
| C7. The all_* tables miss row ids | fixed, `core-fixes` 2779515 | yes | yes | not reported |

"Not reported" means a search of apache/iceberg's issues and pull
requests found nothing for the issue.

## Rebasing onto a newer Iceberg

The fork fixes some of these issues itself, each in a commit of its own
on branch `core-fixes`, which sits on the release; `atomicio` sits on
`core-fixes`. An issue the fork fixes has no test here: its fix carries
an upstream-style test in iceberg-core, and its workarounds are gone.

1. Rebase `core-fixes` onto the new release first. Drop each fix that
   upstream merged, and record the release here.
2. Rebase `atomicio` onto `core-fixes`.
3. Run `TestIcebergCoreIssues` on the new release.
4. For each test that fails, the release fixed or changed that issue:
   - Remove the issue's workarounds, listed under it below.
   - Run the fuzzers long (CLAUDE.md lists them), so they can confirm
     the fix.
   - Make the test assert the fixed behavior, or delete it.
   - Mark the issue fixed here.
5. For each test that still passes, keep the issue's workarounds.

## Issues

### C1. A delete that merges manifests gives existing rows new ids

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

### C2. A retried commit gives a manifest a second row-id range

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

### C3. A replace transaction brings back a snapshot expired since it began

A replace transaction commits the table it built at its start, snapshots
included, over whatever changed since
(`BaseTransaction.commitReplaceTransaction`). A snapshot expired in
between therefore comes back, though the expiry deleted its manifest
list, so reading that snapshot fails.

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
- **Test:** `aReplaceBringsBackASnapshotExpiredSinceItStarted`.
- **Workarounds:**
  - `TestCatalogFaultProperties` checks manifest lists only for
    snapshots a ref reaches (see "Only snapshots a ref reaches").
  - Its `REPLACE_TABLE` model has the replace win over everything since
    the transaction began. A1 below covers that.

### C4. A stale handle fails on a manifest in a newer partition spec

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

### C5. Rewriting manifests fails on a table without a current snapshot

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

### C6. A repeated cherry-pick leaves a table it can't plan

Cherry-picking a snapshot a second time, once its earlier pick has been
expired, adds the snapshot's data files again at a new sequence number.
Once one of those files has a deletion vector, planning fails: "DV data
sequence number (5) must be greater than or equal to data file sequence
number (7)".

- **Cause:** `CherryPickOperation` finds an earlier pick through the
  `source-snapshot-id` of the current snapshot's ancestors, and the
  expiry removed the ancestor that carried it.
- **Should:** refuse the second pick as a duplicate.
- **Test:** `aCherryPickRepeatedAfterAnExpiryLeavesATableItCannotPlan`.
- **Workaround:** `TestInlineDifferentialProperties` compares a planning
  failure as the string "fails: ...", which both catalogs must produce
  alike.

### C7. The all_* metadata tables miss the row ids of an upgraded file

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
`TestCatalogFaultProperties` counts that failure as a rejection.

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
