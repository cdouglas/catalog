# Manifest List Inlining — Implementation Review (v2)

**Date:** 2026-04-17
**Scope:** Complete ML inlining implementation in `fileio-catalog`
(stages 1-7 on `main`) + Iceberg core hooks on `vldb-1.10.1-ml`.
**Reviewed commits (fileio-catalog):** `719d830` (stage 1) through
`194f8f0` (plan marked complete).
**Reviewed commits (iceberg):** `1d99c71b2`, `903462b7e`, `72159117e`.

## Executive summary

The implementation as-landed on `main` **does not work end-to-end.** A
single-commit reproduction (create namespace → create table → FastAppend)
fails with a `NullPointerException` at `InlineDeltaCodec.writeString`
when `fileio.catalog.inline.manifests=true`. The sink hook is wired
correctly through `SnapshotProducer`, but the codec on the fileio-catalog
side was written against the pointer-mode assumption that every snapshot
has a non-null `manifestListLocation`. In the sink path that assumption
is violated and the delta encoder fails.

Beyond the immediate commit-path crash, the log-replay side has an
orthogonal defect: `AddSnapshotUpdate.applyTo(TableMetadata.Builder)`
explicitly throws `UnsupportedOperationException`, so even if the encode
were fixed, reading the catalog back would crash on any delta
containing a snapshot addition. `AddManifestUpdate.applyTo` and
`RemoveManifestUpdate.applyTo` are silent no-ops referencing a method
(`applyManifestDelta`) that does not exist. The checkpoint pool is
populated on decode; the transaction log is not. No test exercises the
full `commit → persist → replay → read` cycle.

Of the six hazards flagged in the first review, five remain unfixed and
a further three new defects were uncovered. The ML path is not ready to
exercise against real tables.

---

## 1. How I tested this

I wrote a minimal end-to-end test (`TestInlineMLBug`) that does what
the Stage 7 suite does not: commit a single data file and then reload
the catalog:

```java
catalog.initialize(..., inline=true, inline.manifests=true);
catalog.createNamespace(Namespace.of("db"));
catalog.buildTable(..., schema).create();
Table tbl = catalog.loadTable(...);
tbl.newFastAppend().appendFile(f).commit();           // FAILS HERE
// Would also need: reload catalog, loadTable, allManifests
```

Result:

```
java.lang.NullPointerException: Cannot invoke "String.getBytes(...)"
  because "value" is null
    at InlineDeltaCodec.writeString(InlineDeltaCodec.java:1263)
    at InlineDeltaCodec.encodeUpdate(InlineDeltaCodec.java:814)
    at InlineDeltaCodec.encodeDelta(InlineDeltaCodec.java:179)
    at InlineDeltaCodec.selectMode(InlineDeltaCodec.java:374)
    at FileIOTableOperations.commitInline(FileIOCatalog.java:443)
    at FileIOTableOperations.doCommit(FileIOCatalog.java:388)
    at BaseMetastoreTableOperations.commit(BMTO.java:126)
    at SnapshotProducer.commit(SnapshotProducer.java:611)
    at TestInlineMLBug.inlineMLFastAppendAndReload
```

The existing Stage 7 `TmMlTests` in `TestInlineManifestEndToEnd.java`
only exercise `createNamespace` + `buildTable.create()` + `loadTable`;
none of them commit a snapshot. The `InlineManifestTableOpsTests` in
`TestInlineDelta.java` call `stageManifestListDelta` directly but
never drive it through a real `SnapshotProducer.commit()`. The
commit→replay cycle has zero coverage.

---

## 2. Hazards from the first review — current status

### 2.1 Hazard #1: `InlineSnapshot` has no `equals` / `hashCode`

**Status: unfixed.**

Verified by grep: `core/.../InlineSnapshot.java` still has no
`equals()` or `hashCode()` override. Two `InlineSnapshot` instances
with identical scalar fields and manifest list compare as unequal via
`Object` identity, and do not equal the corresponding `BaseSnapshot`.

**Impact:** Any code that builds a `Set<Snapshot>` and tests
membership (e.g., `ReachableFileCleanup.cleanFiles()` at
`core/.../ReachableFileCleanup.java:55-65`) will treat a `BaseSnapshot`
and an `InlineSnapshot` with the same snapshot ID as distinct
snapshots, marking the pre-migration version as expired.

**Recommendation:** Override both methods on `InlineSnapshot` to match
`BaseSnapshot`'s field-based comparison, and loosen
`BaseSnapshot.equals()` to accept any `Snapshot` with matching scalars.

### 2.2 Hazard #2: `SnapshotParser.toJson` loses manifest metadata for inline snapshots

**Status: unfixed, and actively reachable via the FULL-mode path.**

`SnapshotParser.toJson()` at `core/.../SnapshotParser.java:85-95`
still falls through to the v1 "embedded manifests" branch when
`manifestListLocation` is null, writing only the manifest path list
and discarding enriched metadata.

**New observation:** `commitInline`'s `"full"` mode (line 451-454 of
`FileIOCatalog.java`) calls `TableMetadataParser.toJson(metadata)`
where `metadata` contains `InlineSnapshot` instances with
`manifestListLocation == null`. The stored bytes carry only paths;
there is no manifest-pool entry to compensate because full-mode skips
the pool. On reload, `wrapInlineManifests` finds no pool entries for
that snapshot and leaves the `BaseSnapshot` as-is with
`v1ManifestLocations` populated — activating Hazard #3.

**Recommendation:** Either (a) write to the pool on full-mode
commits too, or (b) document that full-mode is only compatible with
pointer-mode snapshots and guard against it when any snapshot has
null `manifestListLocation`.

### 2.3 Hazard #3: `BaseSnapshot.cacheManifests` v1 fallback

**Status: unfixed, reachable through the Hazard #2 chain.**

`BaseSnapshot.java:174-181` still does:

```java
if (allManifests == null && v1ManifestLocations != null) {
  allManifests = Lists.transform(
      Arrays.asList(v1ManifestLocations),
      location -> new GenericManifestFile(
          fileIO.newInputFile(location), 0, this.snapshotId));
}
```

If the Hazard #2 chain produces a `BaseSnapshot` with populated
`v1ManifestLocations` on reload, any call to `allManifests(io)`
attempts storage reads of each manifest file to populate counts and
partition summaries — data that the catalog originally had enriched.
Correctness is preserved; throughput and cost are not.

**Recommendation:** In `wrapInlineManifests`, if a parsed snapshot has
`v1ManifestLocations != null` and the pool has no entry for it, log
a warning. Consider throwing on a v2+ table to fail fast.

### 2.4 Hazard #4: `AddSnapshotUpdate.applyTo` throws on replay

**Status: unfixed. In fact, worse than originally characterized.**

The implementation at `InlineDeltaCodec.java:644-651` now explicitly
throws:

```java
@Override
public void applyTo(TableMetadata.Builder builder) {
  throw new UnsupportedOperationException(
      "AddSnapshot requires manifest list prefix; use applyTo(TableMetadata, String)");
}
```

The prefix-accepting overload
(`applyTo(TableMetadata base, String manifestListPrefix)` at line
604-638) exists but has **zero callers** in the codebase. The log
replay path calls `applyDelta → applyUpdates → update.applyTo(builder)`
at line 169, which dispatches to the throwing version for any
`AddSnapshotUpdate` in the decoded delta.

**In practice this never fires today** because the commit-side crash
(§3.1 below) prevents a delta from ever being persisted in the first
place. As soon as the commit side is fixed, the replay side crashes.

**Recommendation:** Per the planning doc's own option (b) in
`catalog_errata.md` E-ML4, introduce an `applyDeltaToProto(
ProtoCatalogFile.Builder, int tblId, List<DeltaUpdate>)` that routes
`AddSnapshotUpdate` through the prefix-accepting path and routes
`AddManifestUpdate` / `RemoveManifestUpdate` to the pool mutations.
Update `UpdateTableInlineAction.apply` to call this instead of
the current `InlineDeltaCodec.applyDelta`.

### 2.5 Hazard #5: `RewriteTablePathUtil` bypasses the sink

**Status: unfixed, but scope-limited as before.**

`core/.../RewriteTablePathUtil.java:252, 280` still calls
`ManifestLists.write()` directly and reads
`snapshot.manifestListLocation()` without a null check. Running this
migration utility against an inline-ML table will NPE. Low severity
(not on the commit path).

### 2.6 Hazard #6: `SnapshotProducer.commit()` cleanup loop

**Status: unfixed; still safe by accident.**

The cleanup loop at `SnapshotProducer.java:594` still does
`committedSnapshot.manifestListLocation().equals(manifestList)`. The
inline path never adds to `manifestLists`, so the loop body doesn't
execute. Defensive order swap still recommended:

```java
manifestList.equals(committedSnapshot.manifestListLocation())
```

---

## 3. New defects uncovered in the actual implementation

### 3.1 `computeDelta` NPE on InlineSnapshot

**Severity: blocker.** **File:** `InlineDeltaCodec.java:232-240`.

```java
for (org.apache.iceberg.Snapshot snap : newMeta.snapshots()) {
  if (!oldSnapIds.contains(snap.snapshotId())) {
    String manifestList = snap.manifestListLocation();      // null for InlineSnapshot
    String suffix = manifestList;                           // null
    if (manifestListPrefix != null && !manifestListPrefix.isEmpty()
        && manifestList.startsWith(manifestListPrefix)) {   // NPE if prefix != ""
      suffix = manifestList.substring(manifestListPrefix.length());
    }
    // ...
    updates.add(new AddSnapshotUpdate(
        snap.snapshotId(), suffix /* possibly null */, ...));
  }
}
```

When the sink produces an `InlineSnapshot`,
`snap.manifestListLocation()` returns `null`. Two failure modes
depending on prefix:

- **Non-empty prefix:** immediate NPE at `manifestList.startsWith`.
- **Empty prefix (current default):** `suffix = null`, the
  `AddSnapshotUpdate` is constructed with null suffix, and the NPE
  deferred to `writeString` in the encoder (§3.2).

**Recommendation:** In `computeDelta`, detect
`snap.manifestListLocation() == null` (i.e., this is an inline
snapshot) and construct the `AddSnapshotUpdate` with an empty-string
or sentinel `manifestListSuffix` — or, better, redesign
`AddSnapshotUpdate` so it no longer carries a `manifestListSuffix`
when the caller is producing an inline snapshot. The manifest list
is carried in the separate `AddManifestUpdate` entries; the
`AddSnapshotUpdate` doesn't need to reference an (inline) Avro file
that doesn't exist.

### 3.2 `writeString` NPE on null `manifestListSuffix`

**Severity: blocker.** **File:** `InlineDeltaCodec.java:814, 1263`.

```java
// encodeUpdate:
writeString(inner, SNAP_MANIFEST_SUFFIX, u.manifestListSuffix);

// writeString:
byte[] bytes = value.getBytes(StandardCharsets.UTF_8);   // NPE
```

This is the observed failure point in my reproduction. Fixing §3.1
eliminates this, but `writeString` should also null-check
defensively — the encoder shouldn't be the layer that catches model
invariants.

### 3.3 `AddManifestUpdate.applyTo` and `RemoveManifestUpdate.applyTo` are no-ops pointing at a nonexistent method

**Severity: blocker.** **File:** `InlineDeltaCodec.java:669-692`.

```java
public static class AddManifestUpdate implements DeltaUpdate {
  // ...
  @Override
  public void applyTo(TableMetadata.Builder builder) {
    // ML deltas are applied to ProtoCatalogFile.Builder, not TableMetadata.Builder.
    // This is a no-op here; handled by applyManifestDelta.
  }
}
```

`applyManifestDelta` does not exist in the codebase (verified via
grep). The comment refers to a method that was designed but not
implemented. Consequence:

1. A delta containing `AddManifestUpdate`/`RemoveManifestUpdate` entries
   is encoded and committed into the catalog transaction log.
2. On catalog reload, `UpdateTableInlineAction.apply` at
   `ProtoCodec.java:1864-1877` calls `InlineDeltaCodec.applyDelta`,
   which iterates the decoded updates via `applyTo(builder)`.
3. `AddManifestUpdate.applyTo` is a no-op. The catalog's in-memory
   pool and `snapshotManifests` map never receive the delta's manifest
   entries.
4. The pool is only correctly populated on **checkpoint** decode
   (ProtoCodec.java:531-538), not on transaction-log decode.
5. `wrapInlineManifests` queries the pool for each snapshot; entries
   staged via deltas are missing.

Net effect: even if §3.1 and §3.2 are fixed, transaction-log deltas
silently lose their ML payload. After the next CAS compaction the
pool is reconstructed from the checkpoint's committed state, which
does not include the dropped deltas — so the ML data is gone.

**Recommendation:** Implement `applyManifestDelta` (or the
`applyDeltaToProto` variant from §2.4) and call it from
`UpdateTableInlineAction.apply`. The method must:
- Route `AddManifestUpdate` to
  `ProtoCatalogFile.Builder.addManifestToPool(tblId, manifest)` and
  append the path to the snapshot's ref list.
- Route `RemoveManifestUpdate` to remove the path from the snapshot's
  ref list (and optionally GC the pool entry if no remaining ref).
- Route `AddSnapshotUpdate` through the prefix-accepting overload.

### 3.4 No test exercises the full commit→replay cycle in TM_ML mode

**Severity: high (process).**

Looking at `TestInlineManifestEndToEnd`:

- `BaselineTests.createTableWritesMetadataFile` — no commit.
- `TmOnlyTests.createTableInline` — no commit.
- `TmMlTests.createTableInlineNoMetadataFile` — no commit.
- `TmMlTests.tableOperationsIsManifestListSink` — `instanceof` check.

Looking at `TestInlineDelta.InlineManifestTableOpsTests`:

- `sinkStagesAndDrains` — constructs the sink directly, calls
  `stageManifestListDelta` manually, drains. No `SnapshotProducer`
  involved, no delta encoded into an actual catalog, no reload.
- `isInstanceOfManifestListSink` — `instanceof` check.
- `configValidationRejectsManifestsWithoutInline` — config validation.

Looking at `TestInlineDelta.MultiUpdateTests.multipleUpdatesApplyInOrder`:
this one does call `applyUpdates`, but with only
`SetPropertiesUpdate` and `SetLocationUpdate`. `AddSnapshotUpdate` is
never passed through `applyUpdates` in any test (verified via grep).

The entire ML inline flow — sink stages → commitInline attaches →
encoder writes → catalog persists → replay decodes → applyDelta
reconstructs — is **not tested anywhere**. The unit tests cover each
stage in isolation with mocked inputs, but the stages don't compose
under the actual data that `SnapshotProducer` produces.

**Recommendation:** Write an end-to-end test in
`TestInlineManifestEndToEnd` that:

1. Creates a table (with `inline.manifests=true`).
2. Performs a `FastAppend` with one data file.
3. Performs a second `FastAppend` (exercises delta vs. pool carryover).
4. Constructs a second `FileIOCatalog` instance against the same
   in-memory `FileIO` (forces checkpoint+log replay).
5. Loads the table from the second catalog, calls
   `tbl.currentSnapshot().allManifests(io)`, and asserts the manifest
   list matches what was committed.
6. Asserts that `io.files` contains no `snap-*.avro` entries.

This is what the plan called for in Stage 7 and is what my
`TestInlineMLBug` started. It needs to live as a permanent test.

---

## 4. Snapshot retention / expiration correctness

Not tested. Not exercised. §2.1 (the `equals` gap) will manifest in the
expiration codepaths the moment anyone runs `expireSnapshots()` on an
inline-ML table:

- `ReachableFileCleanup.cleanFiles()` (`ReachableFileCleanup.java:55-65`)
  builds `Set<Snapshot>` over both `before` and `after` metadata and
  diffs them. If `before.snapshots()` contains a `BaseSnapshot` (from
  a pointer-mode snapshot written pre-migration) and `after.snapshots()`
  contains the same snapshot ID as an `InlineSnapshot` (because
  `wrapInlineManifests` replaced it after the inline commit), the diff
  treats the `BaseSnapshot` as expired and schedules its manifest list
  location for deletion — even though the snapshot is still live.

The mixed-mode (pointer-mode snapshots persisting across an
`inline.manifests=true` upgrade) scenario is also untested. Stage 7
explicitly deferred `TestInlineManifestMixed` from the plan.

---

## 5. Checkpoint-path pool encoding is correct

On a positive note: when the pool *does* get populated (directly via
`ProtoCatalogFile.Builder.addManifestToPool` in tests), the checkpoint
encode/decode roundtrip works. `TestProtoActions.InlineManifestListTests`
covers:

- 3-manifest, 3-snapshot pool with sharing → round-trip preserves
  pool contents and per-snapshot index arrays.
- Empty-pool backward compatibility.
- Multiple-table independent pools.

This is the right foundation; the bug is that no commit path actually
writes into it. Once the transaction-log application (§3.3) routes
deltas into the builder, this layer should Just Work.

---

## 6. Verdict and remediation order

The implementation stages are architecturally correct but were not
integration-tested. Each stage passed its unit tests against mocked
inputs, which masked the fact that they do not interoperate.

**Remediation, in order of dependency:**

1. **Fix §3.1 and §3.2.** `computeDelta` must not deref null, and
   `AddSnapshotUpdate` encoding must tolerate a null/empty suffix
   (or, better, the model should use separate types for inline vs.
   pointer snapshots so suffix is structurally absent on the inline
   side). This unblocks the commit path.

2. **Fix §3.3.** Implement the missing delta-application routing for
   `AddManifestUpdate`/`RemoveManifestUpdate` and route
   `AddSnapshotUpdate` through its prefix-accepting overload.
   `UpdateTableInlineAction.apply` needs to thread the
   `ProtoCatalogFile.Builder` through to the updates.

3. **Fix §2.4 (Hazard #4).** Same as above — the `applyTo(Builder)`
   throw is the same symptom as §3.3 for one particular update type.

4. **Fix §2.1 (Hazard #1).** Add `equals`/`hashCode` to
   `InlineSnapshot` before anything near expiration runs.

5. **Write the end-to-end test suite from §3.4.** Without it, every
   subsequent fix is one integration test away from discovering a
   new composition bug.

6. **Decide §2.2 (Hazard #2) full-mode policy.** Either store the
   manifest pool on full-mode commits, or forbid full-mode when any
   snapshot is inline.

7. **Defer §2.5 and §2.6** (migration tool and cleanup-loop defensive
   order). Both are pre-existing and out of scope for shipping ML
   inlining as a feature, but track as follow-ups.

### Confidence

My reproduction (`TestInlineMLBug`) was deleted after confirming the
bug; reinstating it (or an equivalent) as a permanent test is
recommended. All claims in §3 were verified against the code with
file:line references; §2.1-§2.6 status was verified by grep and by
re-reading the affected files. §4 is by inspection — I did not run
expiration tests.

**The headline finding of this review is a category mismatch between
the first review's assessment and reality:** I previously reported
that the Iceberg-core substrate was "sound" and the fileio-catalog ML
changes "not yet implemented." Both statements remain technically
true — the core sink hook is correct in isolation, and the
fileio-catalog ML implementation was only implemented at the encode
layer, not the decode/apply layer. What the planning doc marked as
"Stage 6/7 complete" is more accurately described as "Stage 6/7
scaffolding in place, never executed." The Stage 7 tests are
structurally incomplete (no actual data commit), which is why the
defects survived to this review.
