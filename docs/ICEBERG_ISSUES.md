# Iceberg Core Issues

Bugs in Apache Iceberg that this project's fuzzers found, and, from C15
on, a review of Iceberg's commit retry paths that the fuzzers' finds
prompted. None is in `FileIOCatalog`: each one reproduces on Iceberg's
own `InMemoryCatalog`, directly or, for C21 and C22, by applying a
client's updates through `CatalogHandlers` as a REST server does. C23
was accepted behavior (A1) until the review, on 2026-10-20, showed which
part of it upstream itself calls a bug.
This page records them for a rebase onto a newer Iceberg. Each issue also
has a document of its own in [iceberg-issues/](iceberg-issues/), for
reports or pull requests upstream. It covers the reproduction, the cause
with links to the 1.12.0 source, the fix, its tests, and what to settle
before a PR.

Upstream fixed C1. The fork fixes C2 to C27 on branch `core-fixes`, each
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
cp iceberg-fileio-catalog/src/test/java/org/apache/iceberg/io/TestIcebergCoreIssues.java \
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
`ddbd99073`. C11 to C14 were fixed on 2026-10-20 and checked against
main at `3738f4a97`. C15 to C22, from the review, were fixed the same
day and checked against main at `834441b96`, as were C23, fixed later
that day, and C24, which round 9 of the fuzzers found on 2026-10-21. On
2026-10-21 each issue was checked against a local copy of upstream's
record: every issue and PR (from the GitHub API), every PR's branch, and
the issues@ and dev@ lists, which carry each issue's and PR's discussion.
What that found is in each issue's document. C25, which the review of the
conflict-encoding plan found on 2026-10-24, was fixed that day and
checked against main at `e70a2efcd` and upstream's record. C26 and C27,
which the conflict-encoding fuzz rounds found on 2026-10-25, were fixed
that day; both reproduce on main at `97659351d4`, and both had been
reported upstream, C26 with a fix the stale bot closed, C27 with a fix,
in 2020, that missed the metastores' drop check. The Fork column is
`atomicio`, which sits on `core-fixes`.

| Issue | Fork | 1.12.0 | main | Upstream |
|---|---|---|---|---|
| C1. A merging delete renumbers rows | fixed | fixed | fixed | fixed by [#16263](https://github.com/apache/iceberg/pull/16263), in 1.11.0 |
| C2. A retried commit renumbers rows | fixed, `core-fixes` 6c6b6fd | yes | yes | not reported |
| C3. A replace restores an expired snapshot | fixed, `core-fixes` 56527cb | yes | yes | [#13651](https://github.com/apache/iceberg/issues/13651), closed by the stale bot after a maintainer called it a bug; [#16942](https://github.com/apache/iceberg/issues/16942), part of [#16232](https://github.com/apache/iceberg/issues/16232); fix in PR [#16943](https://github.com/apache/iceberg/pull/16943), open |
| C4. A stale handle fails on a newer spec | fixed, `core-fixes` bfc20da | yes | yes | not reported |
| C5. Rewriting manifests fails with no snapshot | fixed, `core-fixes` f723a52 | yes | yes | not reported; fix in PR [#16645](https://github.com/apache/iceberg/pull/16645), open |
| C6. A repeated cherry-pick leaves the table unplannable | fixed, `core-fixes` 724608a | yes | yes | not reported |
| C7. The all_* tables miss row ids | fixed, `core-fixes` 2779515 | yes | yes | not reported |
| C8. A DV older than its data file leaves the table unplannable | fixed, `core-fixes` 1d46241 | yes | yes | not reported; maintainers oppose fixing it in planning (see C8) |
| C9. A create-or-replace replaces a table created since it began | fixed, `core-fixes` b7a2abd | yes | yes | not reported; PR [#16943](https://github.com/apache/iceberg/pull/16943) would change it (see C9) |
| C10. An expiry that retries fails on what its first attempt removed | fixed, `core-fixes` 8900018 | yes | yes | not reported |
| C11. An append commits files of a spec another writer removed | fixed, `core-fixes` 69e83ef | yes | yes | not reported |
| C12. An expiry follows the GC and retention settings of when it was created | fixed, `core-fixes` 0824e0d | yes | yes | `gc.enabled`: PR [#17791](https://github.com/apache/iceberg/pull/17791), open; retention: not reported |
| C13. An expiry that committed reports a failure when its cleanup fails | fixed, `core-fixes` 7f3708a | yes | yes | not reported |
| C14. A cherry-pick of a snapshot expired since it was picked fails with an NPE | fixed, `core-fixes` b1f2e01 | yes | yes | not reported |
| C15. An expiry in a transaction deletes files before the transaction commits | fixed, `core-fixes` 44fce44 | yes | yes | not reported; a regression from [#13631](https://github.com/apache/iceberg/pull/13631), in 1.10.0 |
| C16. A rollback undoes another writer's rollback | fixed, `core-fixes` 14d869e | yes | yes | not reported |
| C17. Location and partition statistics updates fail after one concurrent commit | fixed, `core-fixes` b0669b6 | yes | yes | partition statistics: PR [#18204](https://github.com/apache/iceberg/pull/18204), open; location: not reported |
| C18. Replace, overwrite and merge paths fail with an NPE on a removed spec | fixed, `core-fixes` 4692c71 | yes | yes | not reported |
| C19. A sort order replaced after a rename fails with an NPE | fixed, `core-fixes` 7e4517f | yes | yes | not reported |
| C20. A row delta whose starting snapshot expired fails with IllegalArgumentException | fixed, `core-fixes` b967a0a | yes | yes | not reported |
| C21. Ref and spec removals applied to newer metadata lose a commit or a spec in use | fixed, `core-fixes` 517c1c4 | yes | yes | not reported |
| C22. A snapshot removal applied to newer metadata drops a ref set since | fixed, `core-fixes` a2a7847 | yes | yes | reported in a comment on [#6710](https://github.com/apache/iceberg/issues/6710) and on dev@ (2026-02), not fixed |
| C23. A replace transaction drops what other writers committed since it began | fixed, `core-fixes` 29566e9 | yes | yes | [#16232](https://github.com/apache/iceberg/issues/16232), open; PR [#16943](https://github.com/apache/iceberg/pull/16943), open, which this follows except where it falls back; three earlier attempts closed by the stale bot |
| C24. A commit that removes a data file keeps its deletion vector | fixed, `core-fixes` 7f88e98 | yes | yes | PR [#18197](https://github.com/apache/iceberg/pull/18197), open, whose first commit this takes (its second reverses [#11131](https://github.com/apache/iceberg/pull/11131)) |
| C25. A rewrite with an older data sequence number drops a delete it still needs | fixed, `core-fixes` 0e0e9bb | yes | yes | [#10312](https://github.com/apache/iceberg/issues/10312), closed by the stale bot; PR [#10962](https://github.com/apache/iceberg/pull/10962), open, whose age-off half this takes (maintainers opposed the other half) |
| C26. A row delta whose conflict filter misses a file commits a second DV for it | fixed, `core-fixes` 04a2388 | yes | yes | PR [#15966](https://github.com/apache/iceberg/pull/15966), closed by the stale bot unreviewed; [#17206](https://github.com/apache/iceberg/issues/17206), open, on the unreadable tables it leaves |
| C27. A handle to a dropped table fails with an NPE once it has reported the drop | fixed, `core-fixes` 628063f | yes | yes | [#1513](https://github.com/apache/iceberg/issues/1513), closed by [#1553](https://github.com/apache/iceberg/pull/1553) (0.10.0), which missed the metastores' check of the cleared location |

"Not reported" means a search of apache/iceberg's issues, pull requests
and PR branches, and of the issues@ and dev@ lists, found nothing for the
issue (2026-10-21).

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
  - #13651 reported C3 itself; the stale bot closed it, though
    RussellSpitzer had called it "a bug now". Its fix, PR #13764, was
    closed by the stale bot unreviewed.
  - #16232 (open) reports the general problem: a replace silently
    overwrites concurrent schema, property, data and expiry changes.
  - #16942 (open) reports a concurrent writer's snapshot dropped by a
    `createOrReplace`.
  - PR #16943 (open on 2026-10-21) would fix both for
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
  C23 supersedes it where the catalog's table builder lets the
  transaction rebuild its replacement; C3's change remains for the
  rest.
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

- **Upstream:** PR #16645 (open) makes the same change, as part of
  branch support for `RewriteManifests`.
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
- **Upstream:** not reported. Maintainers oppose skipping such a DV in
  planning, which this fix's first part does; the spec requires writers to
  remove a removed data file's DVs (see C8.md).
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
- **Fork:** fixed on `core-fixes` by 8900018, "Core: Work out what an expiry
  removes on the metadata each attempt commits on". Each attempt works out
  what to remove on its own metadata: the snapshots it has that the caller
  named or that it does not retain. Its tests are
  `TestRemoveSnapshots.testRetryAfterConcurrentExpiry` and
  `testRetryAfterConcurrentTag`.
- **No workaround** to remove: the fuzzer counted the exception as
  unexpected.
- **Canary:** `anExpiryRetriesAfterAConcurrentExpiry`.

### C11. An append commits files of a partition spec another writer removed

Details: [iceberg-issues/C11.md](iceberg-issues/C11.md).

An expiry that cleans expired metadata removes the specs no remaining
snapshot uses. A commit that had added files of such a spec failed with a
`NullPointerException` if the spec went before its attempt. If it went
between two attempts, the retry committed the first attempt's manifest:
a snapshot whose manifest names a spec the table does not have, which no
scan can read. An appended manifest of the spec was committed as it was.

- **Cause:** a file's spec is checked when it is added, against the
  handle's metadata. Each attempt looks it up again in the refreshed
  metadata without a check, and a reused or appended manifest's spec is
  not looked up at all.
- **Found by:** `TestCatalogFaultProperties`, 2026-10-20 (try 26,798 of a
  30,000-try run): a merge append through a handle loaded before another
  writer partitioned the table and expired with metadata cleanup.
  Reproduces on `InMemoryCatalog`.
- **Fork:** fixed on `core-fixes` by 69e83ef, "Core: Fail a commit of files
  whose partition spec was removed concurrently". The commit fails with
  `ValidationException` when its metadata lacks the spec of a file it
  writes (`SnapshotProducer.specForNewFiles`) or of a manifest of the new
  snapshot. Its tests are in `TestFastAppend`, `TestMergeAppend` and
  `TestRowDelta` (`*ConcurrentSpecRemoval`). On a REST catalog the window
  between the client's attempt and the server's commit stays open (see
  C11.md).
- **No workaround** to remove: the fuzzer counted the exception as
  unexpected.
- **Canary:** `anAppendNeverCommitsFilesOfARemovedSpec`.

### C12. An expiry follows the GC and retention settings its table had when it was created

Details: [iceberg-issues/C12.md](iceberg-issues/C12.md).

`ExpireSnapshots` checked `gc.enabled`, and read the table's retention
defaults (`history.expire.*`), when it was created. Each attempt follows
the retention of the refs in the metadata it commits on, but kept the
table settings from creation: an expiry created before another writer
disabled GC deleted files anyway, and one created before another writer
raised the table's retention expired snapshots the new retention keeps.

- **Cause:** the `RemoveSnapshots` constructor reads the settings from
  `ops.current()`; `internalApply` refreshes `base` but not them.
- **Found by:** reading `RemoveSnapshots` while adding retention settings
  to `TestCatalogFaultProperties`, 2026-10-20. The fuzzer's model does not
  follow snapshot history, so it cannot see this. Reproduces on
  `InMemoryCatalog`.
- **Upstream:** PR #17791 (open) checks `gc.enabled` on each attempt, and
  changes what it gates; the retention part is not reported.
- **Fork:** fixed on `core-fixes` by 0824e0d, "Core: Read an expiry's GC and
  retention settings on each attempt's metadata". Its tests are
  `TestRemoveSnapshots.testRetentionRaisedSinceCreated`,
  `testRefAgeSetSinceCreated` and `testGcDisabledSinceCreated`.
- **No workaround** to remove.
- **Canary:** `anExpiryFollowsSettingsChangedSinceItWasCreated`.

### C13. An expiry that committed reports a failure when its file cleanup fails

Details: [iceberg-issues/C13.md](iceberg-issues/C13.md).

`ExpireSnapshots.commit()` cleans up the expired files after it commits.
If reading the table back failed, or the reachable cleanup could not read
a manifest list, the exception escaped `commit()`: the caller was told an
expiry that committed had failed, and one that retried left the expired
files behind.

- **Cause:** `RemoveSnapshots.commit` runs `cleanExpiredSnapshots` after
  the commit loop, outside any handler; `SnapshotProducer` logs such
  failures instead.
- **Found by:** `TestCatalogFaultProperties`, 2026-10-20 (try 3,172 of a
  30,000-try run of `faultyStorageNeverMisreportsACommit`): an expiry
  whose commit landed failed when reading the catalog back failed. The
  fuzzer saw it once an expiry could remove a ref its model follows.
  Reproduces on `InMemoryCatalog`.
- **Fork:** fixed on `core-fixes` by 7f3708a, "Core: Let an expiry that
  committed stand when its file cleanup fails". Its tests are
  `TestRemoveSnapshots.testCommittedExpiryWhoseRefreshFails` and
  `testCommittedExpiryWhoseCleanupFails`.
- **No workaround** to remove.
- **Canary:** `anExpiryThatCommittedDoesNotFailOnItsCleanup`.

### C14. A cherry-pick of a snapshot expired since it was picked fails with a NullPointerException

Details: [iceberg-issues/C14.md](iceberg-issues/C14.md).

A cherry-pick checks that its snapshot exists when it is set up. If
another writer expired it before the pick committed, as an expiry does a
staged snapshot that no ref holds, the attempt failed with a
`NullPointerException`: in `WapUtil.validateWapPublish` for a pick that
adds the snapshot's files, or committing a null snapshot for a
fast-forward.

- **Cause:** `CherryPickOperation.apply()` refreshes the table but keeps
  the snapshot it looked up when the pick was set up.
- **Found by:** `TestCatalogFaultProperties`, 2026-10-20 (try 25,453 of a
  30,000-try run): a cherry-pick preempted by an expiry of the table.
  Reproduces on `InMemoryCatalog`.
- **Fork:** fixed on `core-fixes` by b1f2e01, "Core: Fail a cherry-pick of a
  snapshot expired since it was picked": `apply()` fails with
  `ValidationException` when the refreshed table lacks the snapshot. Its
  tests are `TestWapWorkflow.testCherrypickOfASnapshotExpiredSincePicked`
  and `testFastForwardToASnapshotExpiredSincePicked`.
- **No workaround** to remove.
- **Canary:** `aCherryPickOfASnapshotExpiredSincePickedFailsCleanly`.

### C15. An expiry in a transaction deletes files before the transaction commits

Details: [iceberg-issues/C15.md](iceberg-issues/C15.md).

An expiry in a transaction deleted the files of the snapshots it removed
when it was committed to the transaction. If the transaction failed,
those snapshots stayed without their manifest lists; if it retried after
a concurrent tag, the retry kept the tagged snapshot, whose files were
gone.

- **Cause:** `BaseTransaction.expireSnapshots()` lost its deferred
  `deleteWith` in #13631 (1.10.0), and the deferral it had did not
  handle a failed or retried transaction either.
- **Found by:** the review of Iceberg's commit retry paths, 2026-10-20.
  Reproduces on `TestTables`.
- **Fork:** fixed on `core-fixes` by 44fce44, "Core: Clean up a transaction's
  expired files once the transaction commits". Its tests are
  `TestRemoveSnapshots.testTransactionExpiryDeletesNothingWhenTheTransactionFails`
  and `testTransactionExpiryRetriedAfterConcurrentTag`.
- **No workaround** to remove.
- **Canary:** `aTransactionsExpiryDeletesFilesOnlyOnceItCommits`.

### C16. A rollback undoes another writer's rollback, or fails with an NPE when its target expires

Details: [iceberg-issues/C16.md](iceberg-issues/C16.md).

`rollbackTo` checked that its target was an ancestor of main only when
called. After another writer rolled main back further, the rollback
moved main forward again. A target expired in between failed
`rollbackTo` and `setCurrentSnapshot` with a `NullPointerException`.

- **Cause:** `SetSnapshotOperation.apply()` checks ancestry again only
  for `rollbackToTime`, and returns a null snapshot for an expired
  target.
- **Found by:** the review, 2026-10-20. Reproduces on `TestTables`.
- **Fork:** fixed on `core-fixes` by 14d869e, "Core: Check a rollback's target
  on each attempt". Its tests are in `TestSnapshotManager`
  (`*Concurrent*`).
- **No workaround** to remove.
- **Canary:** `aRollbackDoesNotUndoAConcurrentRollbackPastIt`.

### C17. Location and partition statistics updates fail after one concurrent commit

Details: [iceberg-issues/C17.md](iceberg-issues/C17.md).

`UpdateLocation`, for tables and views, refreshed once before its
retries, and `UpdatePartitionStatistics` committed once without a
refresh, so one concurrent commit failed them with
`CommitFailedException`.

- **Cause:** neither refreshes per attempt, as `SetStatistics` does since
  #15148.
- **Found by:** the review, 2026-10-20. PR #18204, open, fixes the
  partition statistics part.
- **Fork:** fixed on `core-fixes` by b0669b6, "Core: Refresh location and
  partition statistics updates on each attempt". Its tests are in
  `TestSetLocation`, `TestSetPartitionStatistics` and
  `TestInMemoryViewCatalog`.
- **No workaround** to remove.
- **Canary:** `aLocationUpdateRetriesOnARefreshedTable`.

### C18. A replace of partitions, an overwrite checking its filter, or a merge of appended manifests fails with an NPE on a removed spec

Details: [iceberg-issues/C18.md](iceberg-issues/C18.md).

Three paths looked up the spec of a commit's files before C11's checks,
and failed with a `NullPointerException` when another writer had
removed it: `ReplacePartitions`, `OverwriteFiles` with
`validateAddedFilesMatchOverwriteFilter`, and the merge of appended
manifests.

- **Cause:** `MergingSnapshotProducer.dataSpec()` and the manifest merge
  managers look the spec up without a check.
- **Found by:** the review, 2026-10-20, of the paths C11 left.
- **Fork:** fixed on `core-fixes` by 4692c71, "Core: Fail cleanly on a removed
  spec in replace, overwrite and merge paths", with C11's
  `specForNewFiles`. Its tests are in `TestReplacePartitions`,
  `TestOverwrite` and `TestMergeAppend` (`*ConcurrentSpecRemoval`).
- **No workaround** to remove.
- **Canary:** `aReplaceOfPartitionsOfARemovedSpecFailsCleanly`.

### C19. A sort order replaced after another writer renamed its column fails with an NPE

Details: [iceberg-issues/C19.md](iceberg-issues/C19.md).

`ReplaceSortOrder` binds its fields to the schema it is created with.
After a concurrent rename of a sort column, the metadata builder looked
the old name up in the new schema, and the commit failed with a
`NullPointerException`.

- **Cause:** `TableMetadata.freshSortOrder` maps fields by name from the
  order's schema to the current one.
- **Found by:** the review, 2026-10-20. Reproduces on `TestTables`.
- **Fork:** fixed on `core-fixes` by 7e4517f, "Core: Bind a replaced sort order
  to the schema each attempt commits on", by field id, as a REST server
  binds it. Its tests are in `TestSortOrder` (`*AfterConcurrent*`).
- **No workaround** to remove.
- **Canary:** `aSortOrderReplacedAfterARenameDoesNotFail`.

### C20. A row delta whose starting snapshot expired fails with IllegalArgumentException

Details: [iceberg-issues/C20.md](iceberg-issues/C20.md).

A row delta's check that its starting snapshot is an ancestor of the
branch runs on each attempt's metadata, but failed with
`IllegalArgumentException`, as for a bad argument, when another writer
had expired the snapshot or rolled the branch back past it.

- **Cause:** the check uses `Preconditions.checkArgument`.
- **Found by:** the review, 2026-10-20. Reproduces on `TestTables`.
- **Fork:** fixed on `core-fixes` by b967a0a, "Core: Fail a row delta whose
  starting snapshot is gone with ValidationException". Its test is
  `TestRowDelta.testValidateFromSnapshotExpiredConcurrently`.
- **No workaround** to remove. (A4's `IllegalArgumentException` is a
  different check, which upstream chose and tests.)
- **Canary:** `aRowDeltaFromAnExpiredSnapshotFailsValidation`.

### C21. Ref and spec removals applied to newer metadata lose a concurrent commit or remove a spec in use

Details: [iceberg-issues/C21.md](iceberg-issues/C21.md).

A REST server, and `FileIOCatalog.commitTransaction`, apply a commit's
updates to the table's current metadata, checked by the requirements
`UpdateRequirements` derives. A ref removal required nothing, so a
branch rename racing a commit to the branch lost the commit. A spec or
schema removal required every branch but main, so a commit of old-spec
files to main that landed first left the table unscannable.

- **Cause:** no requirement for `RemoveSnapshotRef`; `requireNoBranchesChanged`
  skips main (#10755).
- **Found by:** the review, 2026-10-20. Reproduces through
  `CatalogHandlers` on `InMemoryCatalog`.
- **Fork:** fixed on `core-fixes` by 517c1c4, "Core: Require the refs that ref
  and spec removals depend on". Its tests are in `TestUpdateRequirements`.
- **No workaround** to remove. Bug 69's check covers the opposite order
  in `FileIOCatalog`.
- **Canary:** `removalsAppliedToNewerMetadataRequireTheirRefs`.

### C22. A snapshot removal applied to newer metadata drops a ref set since

Details: [iceberg-issues/C22.md](iceberg-issues/C22.md).

Removing snapshots from metadata also removes the refs that point to
them. Applied to newer metadata, an expiry's removal dropped a tag
another writer had created on an expired snapshot, or main after a
rollback to one.

- **Cause:** `TableMetadata.Builder.removeSnapshots` drops dangling refs,
  and no requirement can say that no new ref points to a snapshot.
- **Found by:** the review, 2026-10-20. Reproduces through
  `CatalogHandlers` on `InMemoryCatalog`.
- **Upstream:** reported in a comment on #6710 (2025-11) and in a dev@
  thread (2026-02), with no fix or decision.
- **Fork:** fixed on `core-fixes` by a2a7847, "Core: Fail to apply the removal
  of a snapshot that a ref points to", with
  `RetryableValidationException`, which a REST client retries. Its test is
  `TestTableMetadata.testRemovingAReferencedSnapshotIsRetryable`.
- **No workaround** to remove.
- **Canary:** `aRemovalAppliedToNewerMetadataKeepsAConcurrentTag`.

### C23. A replace transaction drops what other writers committed since it began

Details: [iceberg-issues/C23.md](iceberg-issues/C23.md).

A replace transaction committed the replacement it built from the table
as it began over whatever the table had become, dropping other writers'
commits to main and to other branches, tags, properties (such as
`gc.enabled=false`) and format upgrades (downgrading the table), and
bringing back expired snapshots (C3). Upstream holds that a replace
wins without a conflict check (rdblue, on PR #16289), but that it must
keep the table's history, and that erasing a concurrent change "as
though the concurrent commit never happened" is a bug.

- **Cause:** `BaseTransaction.commitReplaceTransaction` commits the
  replacement as built; it can't build it again.
- **Found by:** the review of 2026-10-20; fileio-catalog had accepted it
  as A1.
- **Fork:** fixed on `core-fixes` by 29566e9, "Core: Rebuild a replace on
  a table changed since it began, or fail". As in PR #16943, the table
  builder passes a function that builds the replacement, and the
  transaction rebuilds it on the refreshed table and applies its updates
  again. Where it can't do so exactly (another schema, spec, sort order
  or format version; an update that commits once), it fails with
  `CommitFailedException`, where #16943 commits the replacement as it
  began. Two `CatalogTests` racing-replace cases expect the failure
  outside REST. Tests in `TestReplaceTransaction` and `CatalogTests`.
- **Workaround removed:** `TestCatalogFaultProperties` modeled a replace
  as the table the transaction saw when it began; it now models the
  table as the replace commits over it.
- **Canary:** `aReplaceKeepsWhatOtherWritersCommittedSinceItBegan`.

### C24. A commit that removes a data file keeps its deletion vector

Details: [iceberg-issues/C24.md](iceberg-issues/C24.md).

In a v3 table, a commit that removes a data file removes its deletion
vector too. The filter's check of which delete manifests to open looked
at the partitions of explicitly deleted files first, and only without
them at the removed data files, so it skipped the DV's manifest when it
lay outside those partitions: a row delta that also removes a delete
file of another partition, or a commit retried after another writer
merged the DV's manifest into a new one (the first attempt's dropped DV
counts as an explicit delete without a partition).

- **Cause:** `ManifestFilterManager.canContainDroppedFiles` returns the
  partition check before it reaches `removedDataFilePaths` (#13222,
  1.10.0).
- **Found by:** `TestCatalogFaultProperties`, 2026-10-21 (round 9, try
  8,582 of a 30,000-try run): a delete retried after a concurrent row
  delta merged delete manifests. Reproduces on `InMemoryCatalog`.
- **Fork:** fixed on `core-fixes` by 7f88e98, "Core: Remove a removed data
  file's deletion vector from any delete manifest", which takes PR
  #18197's first commit and its test. It leaves out the second, which
  also rewrites a delete manifest just to drop delete files older than
  every live data file: #11131 stopped that on purpose, as not needed for
  correctness, and the maintainers held to it on #10962 (decided
  2026-10-24). Its tests are `TestManifestFilterManager` and
  `TestRowDelta.test*TheFilesDV`.
- **No workaround** to remove.
- **Canary:** `aRemovedDataFilesDeletionVectorIsRemovedOnRetry`.

### C25. A rewrite with an older data sequence number drops a delete it still needs

Details: [iceberg-issues/C25.md](iceberg-issues/C25.md).

A commit drops the delete files older than the minimum data sequence
number of the live data files. It took that minimum over the data
manifests it kept, not over the files it adds. A `RewriteFiles` that
commits its files with an older data sequence number (Spark's
`rewrite_data_files` does by default) can need a delete newer than that.
When no kept data file is older than the delete, the delete is dropped
and the rows it deleted come back. A retry gets there more easily: a
concurrent commit that removes the last older data file is enough.

- **Cause:** `MergingSnapshotProducer.apply`'s age-off minimum leaves out
  the new data files (#1105, 0.9.0; the rewrite's data sequence number
  is from #3480, 0.13.0).
- **Found by:** the review of the conflict-encoding plan, 2026-10-24.
  Iceberg's retry is that work's test oracle, and this case makes the
  retry wrong. Reproduces on `InMemoryCatalog` with a single writer.
- **Fork:** fixed on `core-fixes` by 0e0e9bb, "Core: Keep the deletes a
  rewrite's older-sequence data files still need", which takes the
  age-off half of PR #10962. Its test is
  `TestRewriteFiles.testRewriteWithOldSequenceNumberKeepsTheDeletesAfterIt`.
- **No workaround** to remove: the fuzzers' rewrites never set a data
  sequence number.
- **Canary:** `aRewriteWithAnOlderSequenceNumberKeepsTheDeletesItNeeds`.

### C26. A row delta whose conflict filter misses a file commits a second deletion vector for it

Details: [iceberg-issues/C26.md](iceberg-issues/C26.md).

A row delta that adds a deletion vector for a data file checks that no
commit since it began added one for the same file: two DVs for one file
make every scan of the table fail. The check read only the delete
manifests and entries that the operation's conflict detection filter
matches, which bounds the rows it read and need not cover the files it
adds DVs for. A concurrent DV outside the filter got through, and the
commit added a second.

- **Cause:** `MergingSnapshotProducer.validateAddedDVs` filters by the
  conflict detection filter: its entries since #11495 (1.8.0), its
  manifests too since #15653 (1.11.0).
- **Found by:** `TestRebaseProperties`, 2026-10-25, whose merge row delta
  had a conflict filter for the wrong partition (the fuzzer's Bug 77):
  Iceberg's own retry committed the second DV.
- **Upstream:** PR #15966 reported it with a fix, and was closed by the
  stale bot unreviewed; #17206 (open) asks to read such tables, which
  maintainers oppose.
- **Fork:** fixed on `core-fixes` by 04a2388, "Core: Check for concurrent
  deletion vectors outside the conflict filter", which scopes the check by
  the partitions of the commit's own DVs. Its test is
  `TestRowDelta.testConcurrentDVOutsideTheConflictDetectionFilter`.
- **No workaround** to remove: the fuzzer's scans now include column stats.
- **Canary:** `aConcurrentDvOutsideTheConflictFilterIsCaught`.

### C27. A handle to a dropped table fails with an NPE once it has reported the drop

Details: [iceberg-issues/C27.md](iceberg-issues/C27.md).

After another process drops a table, a handle's refresh throws
NoSuchTableException and clears the handle's metadata location. The
metastores' operations (Hive, JDBC, Glue, Nessie, Dell ECS) report a
missing table only while they have one, so the handle's next refresh
takes the table for one not created yet and leaves it no metadata, and
its next update fails with a NullPointerException. Views alike.

- **Cause:** `BaseMetastoreTableOperations.refresh` (and
  `BaseViewOperations.refresh`) clear the location that `doRefresh`
  tells a drop by; #1553 (0.10.0) meant the next refresh to fail again.
- **Found by:** `TestCatalogFaultProperties`, 2026-10-25 (fileio-catalog's
  Bug 78): an expiry's file cleanup swallowed the first exception (C13's
  fix lets it), and the writer's next update hit the NPE.
- **Fork:** fixed on `core-fixes` by 628063f, "Core: Keep failing on a
  dropped table after the first refresh says so". Its tests are
  `TestJdbcCatalog.testHandleToADroppedTableKeepsFailing` and
  `TestJdbcViewCatalog.testHandleToADroppedViewKeepsFailing`.
- **No workaround** to remove: `LogCatalog`'s table operations fixed their
  own refresh (Bug 78), and keep it, so they work on stock Iceberg.
- **Canary:** `aHandleToADroppedTableKeepsFailingWithNoSuchTable`, on
  table operations of its own that report a drop as Hive's and JDBC's do.

## Accepted Iceberg Behavior

The oracles accept these behaviors as Iceberg's semantics, not as
issues. A release that changes one of them breaks the oracle that
accepts it, so a rebase finds out anyway.

### A1. A replace transaction wins over concurrent changes, keeping their history

A replace replaces the table's data, schema and partitioning, whatever
changed since it began, without a conflict check: two racing replaces
with the same definition end as the second built it. That much is
Iceberg's intended semantics (rdblue, on PR #16289), and the fuzzers
accept it. Until 2026-10-20 they also accepted that the replace dropped
everything else other writers committed since it began, which C23 now
fixes: `TestCatalogFaultProperties` models a replace as the table when
it commits, with main's data and the schema replaced, and counts a
replace that can't be rebuilt, and fails, as a rejection.
`TestCatalogRaceProperties` still doesn't race replace transactions.

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

### A5. A file from before a v3 upgrade gets its row ids once per lineage

A v3 commit gives row ids to every manifest it carries that has none,
as the manifests of a table's files from before its upgrade to v3 have
none. Each lineage does so on its own: on a branch from before the
upgrade, the same file gets ids that main's lineage didn't give it.
After main moves to such a branch (a replace that leaves main empty,
then a fast-forward to the branch; or a rollback to a snapshot from
before the upgrade), its next commit gives the file new ids, from the
table's next row id. The table's row ids stay disjoint, and stable along
each lineage. `TestCatalogFaultProperties` checks that a file's first
row id never changes along main's lineage (it keeps every snapshot's
parent, so the check holds across expiries); before round 7 of
2026-10-20 it checked across lineages and failed on this.
