# Iceberg Core Issues

Bugs in Apache Iceberg that this project's fuzzers found. None is in
`FileIOCatalog`: each one reproduces on Iceberg's own `InMemoryCatalog`.
The project's tests work around them, so this page records them for a
rebase onto a newer Iceberg, and for reports or fixes upstream.

`TestIcebergCoreIssues` has a test for each issue. Each test asserts what
Iceberg does now, and its Javadoc says what Iceberg should do instead.
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

Checked on 2026-10-16 against three versions:
- **Fork:** the atomicio fork, whose upstream base is `2114bf6` (2025-09-05).
- **1.12.0:** the Iceberg 1.12.0 release.
- **main:** upstream main at `524fe9e`, as of 2026-10-16.

| Issue | Fork | 1.12.0 | main | Upstream |
|---|---|---|---|---|
| C1. A merging delete renumbers rows | yes | fixed | fixed | fixed by [#16263](https://github.com/apache/iceberg/pull/16263), in 1.11.0 |
| C2. A retried commit renumbers rows | yes | yes | yes | not reported |
| C3. A replace restores an expired snapshot | yes | yes | yes | part of [#16232](https://github.com/apache/iceberg/issues/16232), open |
| C4. A stale handle fails on a newer spec | yes | yes | yes | not reported |
| C5. Rewriting manifests fails with no snapshot | yes | yes | yes | not reported |
| C6. A repeated cherry-pick leaves the table unplannable | yes | yes | yes | not reported |
| C7. The all_* tables miss row ids | yes | yes | yes | not reported |

"Not reported" means a search of apache/iceberg's issues and pull
requests found nothing for the issue.

## Rebasing onto a newer Iceberg

1. Run `TestIcebergCoreIssues` on the new release.
2. For each test that fails, the release fixed or changed that issue:
   - Remove the issue's workarounds, listed under it below.
   - Run the fuzzers long (CLAUDE.md lists them), so they can confirm
     the fix.
   - Make the test assert the fixed behavior, or delete it.
   - Mark the issue fixed here.
3. For each test that still passes, keep the issue's workarounds.

C1 is fixed in 1.11.0, so a rebase onto 1.11.0 or later can drop its
workaround now.

## Issues

### C1. A delete that merges manifests gives existing rows new ids

On a v3 table, a delete whose commit merges manifests gives an existing
data file a new `first_row_id`. In the test, file f6 goes from row id 1 to
6. The spec requires the inherited `first_row_id` to be written into
existing and deleted entries when a manifest is rewritten. Without the
merge, f6 keeps its id.

- **Test:** `aDeleteThatMergesManifestsRenumbersRows`.
- **Workaround:** `TestCatalogFaultProperties.V3_PROPERTIES` turns off
  manifest merging on v3 tables, both on creation and on upgrade. The
  class Javadoc mentions it.
- **Upstream:** fixed by #16263, "Core: Fix row ID assignment for
  EXISTING entry during a manifest merge", released in 1.11.0. After the
  rebase, drop `V3_PROPERTIES` and keep the fuzzer's row-lineage check.

### C2. A retried commit gives a manifest a second row-id range

Suppose a commit has to retry because another commit landed first, and
that other commit gave row ids to a manifest the retrying commit carries,
one written before a v3 upgrade. The retry then gives the manifest a
second range. In the test, f0 goes from row id 0 to 2.

- **Cause:** `SnapshotProducer` caches the manifests it carries, with
  their metadata, keyed by `ManifestFile` equality (the path). The retry
  reuses the copy it made before the other commit assigned the manifest's
  `first_row_id`.
- **Should:** a data file's row ids never change.
- **Test:** `aRetriedCommitGivesAManifestASecondRowIdRange`.
- **Workaround:** `TestCatalogFaultProperties` (the `preUpgrade` map)
  exempts files a table had at its v3 upgrade from the check that a row
  id never changes.

### C3. A replace transaction brings back a snapshot expired since it began

A replace transaction commits the table it built at its start, snapshots
included, over whatever changed since
(`BaseTransaction.commitReplaceTransaction`). A snapshot expired in
between therefore comes back, though the expiry deleted its manifest
list, so reading that snapshot fails.

- **Upstream:** #16232 (open) reports the general problem: a replace
  silently overwrites concurrent schema, property, data and expiry
  changes. Iceberg means a replace to win over concurrent changes, but it
  should not restore what an expiry removed.
- **Test:** `aReplaceBringsBackASnapshotExpiredSinceItStarted`.
- **Workarounds:**
  - `TestCatalogFaultProperties` checks manifest lists only for
    snapshots a ref reaches (see "Only snapshots a ref reaches").
  - Its `REPLACE_TABLE` model has the replace win over everything since
    the transaction began. A1 below covers that.

### C4. A stale handle fails on a manifest in a newer partition spec

A handle loaded before another writer added a partition spec can't run
some commits once the table has a manifest in the new spec. These fail
with a `NullPointerException`:
- a delete by path;
- an overwrite by row filter;
- a dynamic partition replace;
- a manifest rewrite.

A merge append, a fast append, a row delta and a delete of an old-spec
file through the same handle all commit.

- **Cause:** `MergingSnapshotProducer`'s filter managers and
  `BaseRewriteManifests` take the partition specs of the handle's metadata
  when they are created. Their commit then refreshes to metadata with a
  spec they don't have.
- **Should:** use the specs of the metadata they commit on.
- **Test:** `aStaleHandleFailsOnAManifestInANewerSpec`, one case per
  failing operation.
- **Workarounds** in `TestCatalogFaultProperties`, which both turn the
  `NullPointerException` into a rejection:
  - `staleSpecs` wraps merge appends, deletes and row deltas.
  - The `rewriteManifests` op catches it.

### C5. Rewriting manifests fails on a table without a current snapshot

`rewriteManifests` throws a `NullPointerException` when the table has no
current snapshot, either because it never had one or because a replace
left it with none after the handle was loaded.
`BaseRewriteManifests.apply` dereferences the current snapshot of the
metadata it commits on.

- **Should:** commit nothing.
- **Test:** `aRewriteOfManifestsFailsOnATableWithoutASnapshot`.
- **Workarounds:** `TestCatalogFaultProperties` doesn't generate a
  rewrite for a table its model says has no snapshot. Its
  `rewriteManifests` op catches the exception from a concurrent replace.

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

The `all_*` metadata tables read every snapshot's manifest list and keep
one copy of each manifest, whichever they read first:
`BaseAllMetadataTableScan.reachableManifests` fills a `HashSet` from a
`ParallelIterable`. A manifest carried across a v3 upgrade has no
`first_row_id` in a list written before the upgrade. So `all_data_files`
can report no row id for a data file that has one, and which copy it
reports can vary between runs.

- **Should:** report the file's row ids.
- **Test:** `allDataFilesMissesTheRowIdsOfAFileCarriedAcrossAnUpgrade`.
- **Workaround:** `TestInlineDifferentialProperties.metadataTable` masks
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
transaction saw at its start.

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
