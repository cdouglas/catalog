# Manifest List Inlining — Code Review (v3)

**Date:** 2026-04-17
**Scope:** `fileio-catalog` ML inline implementation after the review-2 fix
cycle (commits `bf55b46` through `03bad15`) and the `iceberg/vldb-1.10.1-ml`
hooks (`BaseSnapshot`, `InlineSnapshot`, `SnapshotProducer`, plus the
`TableMetadata.Builder` fix in `73d36fc4`).
**Prior review:** [ML_INLINE_IMPL_REVIEW.md](./ML_INLINE_IMPL_REVIEW.md).
**Design context:** [ML_INLINE_DESIGN_NOTES.md](./ML_INLINE_DESIGN_NOTES.md).
**Test status:** `mvn test` green, 163/163 tests pass.

## Executive summary

The review-2 defects are fixed. `FastAppend` now commits, replays, and
reloads correctly on the ML path (`TestInlineManifestEndToEnd.TmMlTests`
covers single and double `FastAppend` + fresh-catalog reload, with
manifest carry-forward). Delta replay routes `AddManifestUpdate` /
`RemoveManifestUpdate` into the `ProtoCatalogFile.Builder` manifest pool,
and `AddSnapshotUpdate` through its prefix-accepting overload. The five
hazards flagged in v2 are resolved to the extent that the
[fast path] works.

Beyond the fast path, the implementation has **three correctness risks
and four integration-debt hazards** that ship-gating the feature should
address:

- **Full-mode data loss on non-ML commits to an ML table** (§1.1).
- **Multi-table `commitTransaction` silently drops ML payload** (§1.2).
- **`parentSnapshotId` / `keyId` / `firstRowId` not carried through
  delta** (§1.3).

The Iceberg-side changes are now a nontrivial fork of upstream; the
dependency on a `replaceSnapshots`/`hasChanges` change in core is a
maintainability hazard (§3.1).

The test estate doesn't exercise anything beyond `FastAppend`. Every
other `SnapshotProducer` subclass (nine of them) is unverified end-to-end
(§4). The design-notes gaps (expiration, `next-row-id`, statistics) all
remain open.

Do not ship behind a non-preview flag.

---

## 1. Correctness risks in the current implementation

### 1.1 Full-mode commit on an ML-populated table drops the pool

**Severity: blocker (silent data loss).** **File:** `ProtoCodec.java:1883-1891`,
`FileIOCatalog.java:448-450`.

`commitInline` guards full-mode only when the current commit attaches new
ML deltas:

```java
// FileIOCatalog.java:448
if (hasMLDeltas && !"delta".equals(mode)) {
  mode = "delta";
}
```

`hasMLDeltas` is true only if this commit stages a new snapshot through
the sink. A pure schema-evolution, properties-change, or
`ExpireSnapshots` commit against a table that already has an ML pool has
`hasMLDeltas == false`. If `selectMode` picks `"full"` (delta too large
or `computeDelta` returned `null`), `UpdateTableInlineAction.apply` runs:

```java
// ProtoCodec.java:1884-1890
} else if (fullMetadata != null) {
  builder.removeInlineMetadata(id);   // <-- drops manifestPool + snapshotManifests
  builder.addInlineTable(id, ..., fullMetadata, manifestListPrefix);
}
```

`removeInlineMetadata` at `ProtoCatalogFile.java:553-559` clears both
`manifestPool` and `snapshotManifests` for the table. The new inline
bytes (full `TableMetadataParser.toJson(metadata)`) are written with
`InlineSnapshot` instances — which serialize through the v1
embedded-manifests branch of `SnapshotParser.toJson` (Hazard #2 from the
first review, still unfixed in `iceberg/core`). On reload,
`wrapInlineManifests` finds no pool entries, returns the `BaseSnapshot`
unmodified, and `snap.allManifests(io)` tries to read phantom files.

`selectMode` for a real 100-snapshot table with `ExpireSnapshots`
retiring a handful of snapshots can easily exceed `APPEND_LIMIT` (4 MiB
minus safety). The user's table is then dead.

**Fix:** either (a) gate full-mode on `builder.hasManifestPool(id) == false`,
or (b) preserve pool + `snapshotManifests` across a full-mode metadata
rotation. Option (b) is structurally right — the pool is independent of
the TM blob.

### 1.2 `commitTransaction` has no ML integration at all

**Severity: blocker for multi-table transactions.** **File:**
`FileIOCatalog.java:572-635`.

The inline branch inside `commitTransaction` (lines 597-624) computes a
TM-only delta and never drains any sink:

```java
List<DeltaUpdate> delta = InlineDeltaCodec.computeDelta(
    currentMetadata, newMetadata, manifestPrefix);
String mode = InlineDeltaCodec.selectMode(delta, newMetadata, 0);
switch (mode) {
  case "delta":
    newCatalog.updateTableInlineDelta(tableId, encodeDelta(delta));   // <-- no ML
    break;
  ...
}
```

If `newMetadata.snapshots()` contains `InlineSnapshot` instances (which
they are, because `SnapshotProducer.apply()` produced them into the
staged transaction), `computeDelta` produces `AddSnapshotUpdate` entries
with empty `manifestListSuffix` — but no `AddManifestUpdate` entries are
attached. On replay, `applyDeltaWithManifests` adds the snapshots but
never populates the pool. Every committed snapshot from the multi-table
transaction has an empty manifest list.

This isn't a ML regression; it's an ML-aware hole that was never filled.
The single-table `commitInline` path (lines 420-442) has the sink-drain
logic; the multi-table path does not.

**Fix:** thread the `InlineManifestTableOperations` instance for each
committed table through `commitTransaction` so its staged deltas can be
drained and attached by the same `attachManifestDelta` call used in
`commitInline`. The current `newTableOps(tableId, current)` at line 587
creates an ops instance, but its sink was never used by the
SnapshotProducer that built the TableCommit — the deltas are on the
*original* ops from the table handle passed to the transaction. There's
no clean way to recover them from here.

This is architectural: the `InlineManifestTableOperations` pattern
assumes the sink lives on the ops instance that `SnapshotProducer`
commits through, but catalog transactions buffer updates and commit
them later through a different path.

### 1.3 `AddSnapshotUpdate` discards `parentSnapshotId`, `firstRowId`, `keyId`

**Severity: high (correctness for non-linear histories, v3+, encrypted).**
**File:** `InlineDeltaCodec.java:330-334, 690-728`.

`AddSnapshotUpdate` carries: `snapshotId`, `manifestListSuffix`,
`summary`, `timestampDeltaMs`, `schemaId`, `addedRows`. It does not
carry:

- **`parentSnapshotId`.** `applyTo` reconstructs it as
  `base.currentSnapshot().snapshotId()` (line 692-693). This is wrong
  for:
  - **Stage-only snapshots** (`SnapshotProducer.stageOnly`), which add
    to `TableMetadata` without advancing a ref. Replay gives them the
    wrong parent.
  - **Branch commits**, which advance a non-`main` branch. The parent
    should be the last snapshot on that branch, not `currentSnapshot()`.
  - **Cherry-picks / rollbacks**, which explicitly set parent to a
    historical snapshot.

  Linear `main`-branch appends happen to work because the parent is
  coincidentally the current snapshot.

- **`firstRowId`.** For v3+ row lineage, `TableMetadata.next-row-id`
  must advance per commit. `ManifestListSink.stageManifestListDelta`
  receives `nextRowId` and `nextRowIdAfter`, but
  `InlineManifestTableOperations.stageManifestListDelta` at
  `FileIOCatalog.java:555-559` discards both. This reproduces Design
  Notes Gap 2.

- **`keyId`.** `InlineSnapshot` carries a `keyId` for encryption
  (`InlineSnapshot.java:62`), never passed through `AddSnapshotUpdate`.
  Encrypted tables committed through the ML path silently drop the
  snapshot's key ID on reload.

**Fix:** extend `AddSnapshotUpdate`'s wire format with optional
`parent_snapshot_id` (fixed64), `first_row_id` (varint64), and `key_id`
(string) fields. Populate from the staged `ManifestListDelta` context in
the sink and from the `Snapshot` in `computeDelta`.

---

## 2. Sharp edges in the fileio-catalog implementation

### 2.1 `applyDeltaWithManifests` assumes update ordering

**File:** `InlineDeltaCodec.java:183-242`.

Manifest carry-forward depends on `AddSnapshotUpdate` running *before*
the `AddManifestUpdate` / `RemoveManifestUpdate` entries for that
snapshot (so that `current.snapshot(add.snapshotId)` can find the new
snapshot and read its `parentId`). Today this ordering is produced by
`computeDelta` + `attachManifestDelta`, which always emits additions
before manifest updates. But nothing enforces it in the decoder, and no
test exercises a mis-ordered delta. A future encoder change that
reorders updates would silently break carry-forward.

**Fix:** either document the invariant explicitly on `DeltaUpdate` /
`applyDeltaWithManifests`, or make carry-forward robust to reordering
(two passes: first collect all add-snapshots into a parent-lookup map,
then process manifest updates).

### 2.2 `Builder.build()` short-circuit fix is a core fork

**File:** `iceberg/core/.../TableMetadata.java` (commit `73d36fc4`).

The fileio-catalog load path depends on a 32-line addition to
`TableMetadata.Builder.replaceSnapshots` in Iceberg core (`snapshotsReplaced`
flag + `hasChanges()` check). This is a functional change to Iceberg's
builder semantics, not an isolated hook. It affects any caller of
`replaceSnapshots` (which is currently zero in upstream, but any future
upstream use would inherit the new side effect).

The alternative — wrap snapshots during JSON parse (custom
`TableMetadataParser` path) instead of post-parse via
`TableMetadata.Builder` — avoids the fork. That's cleaner for long-term
maintenance.

### 2.3 `inline://` sentinel leak if wrapping is skipped

**File:** `FileIOCatalog.java:475-515`, `InlineDeltaCodec.java:696-700`.

`AddSnapshotUpdate.applyTo` writes the manifest-list location as
`inline://<snapshotId>` when suffix is empty. `wrapInlineManifests`
replaces those with `InlineSnapshot` only if the pool has entries for
that snapshot ID. If the pool is lost (§1.1), the replacement path
early-exits at line 493 (`hasAny == false`) and the `BaseSnapshot` with
`inline://` location is handed to callers. Any read of `allManifests()`
on that snapshot will fail with a file-not-found error against the
(non-existent) `inline://` path.

**Fix:** in `wrapInlineManifests`, unconditionally scan for snapshots
with a `manifestListLocation` starting with `inline://` and either
replace them (if pool has entries) or throw a clear error (if pool is
missing). Silent delegation to storage I/O for a synthetic path is the
worst failure mode.

### 2.4 `InlineSnapshot.equals` doesn't compare manifests

**File:** `InlineSnapshot.java:276-295`.

`equals` compares `{snapshotId, parentId, sequenceNumber,
timestampMillis, schemaId}` — deliberately *not* the manifest list, so
that an `InlineSnapshot` and a `BaseSnapshot` with the same scalar
identity compare equal. This is correct for snapshot-retention sets (the
original use case), but it means two `InlineSnapshot` instances with
different manifest lists and the same ID are also `equal()`. A test that
asserts `snap1.equals(snap2)` won't catch a manifest-pool divergence.

Not a bug — a design choice with a downside. Worth documenting on the
class javadoc. "Manifest content is not part of identity" is a load-
bearing invariant.

### 2.5 Unused `nextRowId` / `nextRowIdAfter` sink parameters

**File:** `FileIOCatalog.java:554-559`.

```java
public void stageManifestListDelta(
    long sequenceNumber, long snapshotId, Long parentSnapshotId,
    Long nextRowId, ManifestListDelta delta, Long nextRowIdAfter) {
  stagedDeltas.put(snapshotId, delta);
}
```

Five of six parameters are discarded. `parentSnapshotId` is the
remediation for §1.3 A; `nextRowId`/`nextRowIdAfter` for §1.3 B.
`sequenceNumber` is already carried through the TableMetadata. But the
current implementation has no way to know it's silently dropping these
— the compiler doesn't warn. Consider a sink implementation that fails
fast if v3+ metadata is passed with non-null `nextRowId`, to catch the
regression when someone enables ML on a v3 table.

### 2.6 `RemoveSnapshotsUpdate.applyTo` doesn't cascade to the pool

**File:** `InlineDeltaCodec.java:662-664`.

```java
@Override
public void applyTo(TableMetadata.Builder builder) {
  builder.removeSnapshots(snapshotIds);
}
```

This is the Design Notes Gap 1. `applyDeltaWithManifests` routes this
update through the default `applyTo(Builder)` branch at line 239 — which
updates `TableMetadata` but does not touch `snapshotManifests` or the
`manifestPool` in the catalog builder. Expired snapshots' ref lists and
pool entries are retained indefinitely.

Not user-visible until after a compaction: until then the pool is rebuilt
from the checkpoint each load. After CAS compaction, orphaned pool
entries are baked into the checkpoint. The pool grows linearly with
expired-snapshot count.

**Fix:** add a `RemoveSnapshotsUpdate` branch in `applyDeltaWithManifests`
that cascades via a new `ProtoCatalogFile.Builder.removeSnapshotManifests(
tblId, snapshotId)` + pool GC (drop pool entries whose last referrer was
just removed).

### 2.7 No guard against mixing pointer-mode and inline-mode snapshots

**File:** `FileIOCatalog.java:475-515`.

`wrapInlineManifests` replaces a `BaseSnapshot` with `InlineSnapshot` if
the pool has entries for that ID; otherwise leaves it as a
`BaseSnapshot`. This silently supports a "mixed" table where some
snapshots are pointer-mode (Avro manifest list) and some are inline.
Mixed tables aren't tested, and any code path that distinguishes the two
(e.g., cleanup, rewrite tools) is likely to mishandle at least one.

The design notes explicitly flag `TestInlineManifestMixed` as deferred.
If mixed mode is supported, `wrapInlineManifests` should log mixed-mode
tables on load (audit trail) or reject them. If mixed mode is not
supported, migration-from-pointer-to-inline should fail loudly.

---

## 3. Integration with Iceberg — assessment

### 3.1 Clean hooks

The core-side additions are minimal and surgical:

| Hook | File | Notes |
|------|------|-------|
| `ManifestListSink` interface | `api/.../ManifestListSink.java` | Clean |
| `SnapshotProducer.apply()` sink check | `SnapshotProducer.java` | Clean |
| `InlineSnapshot` class | `core/.../InlineSnapshot.java` | Clean; public now |
| `BaseSnapshot.equals` loosened | `BaseSnapshot.java:331-340` | Minor; reasonable |
| `InlineSnapshot.equals`/`hashCode` | `InlineSnapshot.java:276-295` | Clean |
| `SnapshotProducer` cleanup `.equals` reversal | `SnapshotProducer.java:665` | Defensive, clean |

These are the kind of changes you would submit upstream.

### 3.2 Less clean — the `Builder.build()` fork

`TableMetadata.Builder.replaceSnapshots` + `snapshotsReplaced` flag
(`TableMetadata.java` commit `73d36fc4`) is more invasive. It fixes a
real bug in the builder (mutate-without-change-tracking breaks
`hasChanges()` short-circuit), but is only load-bearing because of
fileio-catalog's architectural choice to mutate parsed `TableMetadata`
in `wrapInlineManifests`.

**Recommendation:** replace `wrapInlineManifests` with a custom
`TableMetadataParser`-compatible path that produces `InlineSnapshot`
directly from the stored JSON (detect the `inline://` sentinel during
snapshot deserialization, look up pool entries, construct
`InlineSnapshot`). This removes the need for the core fork and the
`replaceSnapshots` side effect.

### 3.3 Migration-tool regression

`RewriteTablePathUtil.java:252,280` (from the first review Hazard #5)
still calls `ManifestLists.write()` unconditionally and dereferences
`snapshot.manifestListLocation()` with no null check. Running the path-
rewrite utility on an inline-ML table NPEs. This is pre-existing but
worth flagging — users who discover it mid-migration will not enjoy it.

### 3.4 `ReachableFileCleanup` expiration

The first review's §4 concern about `ReachableFileCleanup.cleanFiles()`
is now addressed by `InlineSnapshot.equals`. `Set<Snapshot>` over mixed
`BaseSnapshot` and `InlineSnapshot` instances now compares on snapshot
ID + scalars, so pre-migration pointer-mode snapshots won't be spuriously
expired. Not tested, but no longer structurally broken.

---

## 4. Test coverage — systematic gaps

### 4.1 Per-operation coverage

The Design Notes enumerate nine snapshot-producing operations. The test
estate exercises **one**:

| Operation | Abstraction verified? | End-to-end (commit→reload)? |
|-----------|------------------------|------------------------------|
| `FastAppend` | yes | yes (`fastAppendReloadManifests`) |
| `MergeAppend` | no | no |
| `BaseOverwriteFiles` | no | no |
| `BaseRewriteFiles` | no | no |
| `BaseRowDelta` | no | no |
| `BaseReplacePartitions` | no | no |
| `StreamingDelete` | no | no |
| `CherryPickOperation` | no | no |
| `BaseRewriteManifests` | no | no |

`TestInlineDelta.DeltaReplayTests` exercises `AddSnapshot + AddManifest`
generically, but never drives it through the actual `SnapshotProducer`
subclasses for these operations. The delete-manifest (`ManifestContent.DELETES`)
path is tested at the codec level only.

**High-value gaps to close:**

- `MergeAppend` — exercises merging a new manifest with carried-forward
  parent manifests. The manifest-pool sharing logic should dedupe this
  correctly.
- `BaseOverwriteFiles` — the first operation that produces a non-empty
  `removedPaths` list. Currently untested.
- `BaseRewriteFiles` — compaction. Produces many removes + many adds;
  stresses pool GC (§2.6).
- `CherryPickOperation` — the only operation that may re-add a manifest
  already in the pool. `addManifestToPool` is idempotent by path, so
  should work, but is unverified.
- `BaseRewriteManifests` — rewrites the entire manifest list. Largest
  delta; most likely to hit `selectMode == "full"` and trigger §1.1.

### 4.2 Cross-cutting gaps

Nothing exercises any of:

- **Multi-table catalog transaction with ML** (§1.2). Would fail
  silently today.
- **`ExpireSnapshots`** against an inline-ML table. Pool GC is missing
  (§2.6).
- **Transactions with `newTransaction()`** staging multiple operations.
  Per Design Notes "worth noting": should work but untested.
- **Mixed mode** (pointer snapshots + inline snapshots in the same
  table). Explicitly deferred.
- **v3+ tables** (`firstRowId` / `next-row-id` lineage). Would silently
  corrupt.
- **Encrypted tables** (`keyId`). Would silently lose encryption
  metadata.
- **Full-mode commit on an ML-populated table** (§1.1). Would drop the
  pool.
- **Delta replay against a mis-ordered delta** (§2.1). Invariant
  unenforced.

### 4.3 Cloud provider ML tests

None of `TestS3Catalog`, `GCSCatalogTest`, `ADLSCatalogTest` set
`fileio.catalog.inline.manifests=true`. The cloud integration suite runs
in pointer mode only. If provider-specific conditional-write quirks
interact with ML append semantics, we'll find out in production.

### 4.4 Recommended additions

In rough priority order:

1. Parameterize `TmMlTests` over every `SnapshotProducer` subclass (write
   one test-method-per-op that does `commit → reload → assert manifests
   match`). Blocks feature ship.
2. `FullModePreservesPool` regression test — force `selectMode` to
   return `"full"` on an ML-populated table, assert the pool survives.
   Blocks §1.1 fix.
3. `MultiTableTransactionWithML` — two tables in one transaction, each
   getting a FastAppend. Blocks §1.2 fix.
4. `ExpireSnapshotsGcsPool` — commit N snapshots, expire N-1, assert
   pool has one entry left. Blocks §2.6 fix.
5. `CommitTransactionFailureCleanup` — if commit fails mid-transaction,
   does the staged sink get cleared? Currently `drainStagedDeltas` is
   called unconditionally in `commitInline`, but on retry the same
   snapshot ID is produced and re-staged, so it probably works. Verify.
6. `StageOnlyMLCommit` — stage a snapshot without advancing a branch,
   verify `parentSnapshotId` round-trips. Would fail today (§1.3 A).
7. Port a subset of the above to `GCSCatalogTest` / `TestS3Catalog` as
   an inline-ML integration profile.

---

## 5. Verdict and next steps

Each of v2's hazards was addressed correctly — the `FastAppend` golden
path works, unit tests cover the codec thoroughly, and the new
`DeltaReplayTests` / `TmMlTests` classes show the right instinct toward
end-to-end verification. But the verification is breadth-first over one
operation, not any-operation. The "works for FastAppend" green signal
doesn't generalize.

**Blocks for a non-preview ship:**

1. §1.1 (full-mode data loss on ML-populated tables) — correctness.
2. §1.2 (multi-table `commitTransaction` drops ML payload) — correctness
   for a supported API.
3. §1.3 (`parentSnapshotId` / `firstRowId` / `keyId` not round-tripped)
   — correctness for non-trivial histories and v3+ tables.
4. §2.6 (pool GC on snapshot expiration) — unbounded growth.
5. §4.1 coverage (per-operation end-to-end tests for the remaining eight
   `SnapshotProducer` subclasses) — process/CI.

**Should-close before general availability:**

6. §2.2 (core-fork `Builder.build()` change) — swap to a parser-level
   wrap strategy.
7. §2.3 (`inline://` sentinel leak if pool is missing) — fail loudly.
8. §2.7 (mixed-mode support explicit yes/no).

**Defer:**

9. §3.3 (`RewriteTablePathUtil` NPE) — out of scope, pre-existing.
10. Statistics-file delta types (Design Notes Gap 3) — orthogonal to ML.

Confidence: all claims in §1–§3 are verified against current code with
file:line references. `mvn test` green at HEAD. Per-operation coverage
claims in §4.1 are derived from a grep over `extends SnapshotProducer`
and `extends MergingSnapshotProducer` cross-referenced against the test
sources — negative results (no test found) were confirmed by searching
for each operation by name in the `TmMlTests` body and finding only
`newFastAppend`. §5 is judgment.

The feature is close. It is not done.
