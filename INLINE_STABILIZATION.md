# Inline TM/ML stabilization plan

## Status (2026-05-05) — all 8 inline-TM bugs fixed

`TestS3CatalogCASInlineTM` is **102/102 passing** (13 skipped, all
pre-existing). The plan below was executed; the per-step landings are
summarized here so the rest of this doc reads as historical context.

| Step | Bug                                                | Resolution                                     | Commit    |
|------|----------------------------------------------------|------------------------------------------------|-----------|
| A    | `testUpdateTransaction` — `previousFiles().size() == 0` | Synthetic `inline://#<hash>` base location in `applyDelta`/`applyDeltaWithManifests` so `Builder.previousFileLocation` is non-null and `addPreviousFile` runs | `804465b` |
| B    | `testRemoveUnusedSpec/Schemas[1]` — refresh required   | `setLastUpdatedMillis(current.lastUpdatedMillis())` BEFORE `update.applyTo` (was after) so `setRef` sees a deterministic stamp | `45c88c5` |
| C    | `testRemoveUnusedSpec/Schemas[2]` — specs/schemas not removed | New `RemoveSchemasUpdate` / `RemovePartitionSpecsUpdate` delta types (wire fields 14/15); `computeDelta` emits them with `Add → SetCurrent/SetDefault → Remove` ordering | `b616131` |
| D    | `testRenameTable` — destination empty for inline tables | Inline-aware `renameTable`: `dropTable + createTableInline(to, oldInlineMeta)` in a single `Mut`. TM+ML rename still TODO. | `067df2c` |
| E    | `testReplaceTableKeepsSnapshotLog` — wall-clock timestamp | Same Step B fix (the `setRef`-before-pin bug also corrupted the snapshot log entry's timestamp on inherited snapshots) | `45c88c5` |
| F    | `testConcurrentReplaceTransactionSortOrderConflict` | `idCollidesWithDifferentContent` check in `computeDelta`: if any schema/spec/sort-order id appears in both old and new with different content, return null → full-mode write | `baac33b` |

The four `testReplaceTransaction`-family `@Disabled` overrides (Group A
in the original plan) were re-enabled in commit `d8e19e8`; Step A was
sufficient to fix them. **Updated 2026-05-06:** of the three remaining
disabled cases, two are now re-enabled and one keeps a narrowed
disable:

* `testMetadataFileLocationsRemovalAfterCommit` — re-enabled in both
  `TestS3CatalogCASInlineTM` and `GCSCatalogTestInlineTM` with an
  inline-equivalent body that asserts on `metadata.previousFiles().size()`
  directly (one less than the upstream `Set<String>` assertion, which
  also includes `metadataFileLocation()`). The upstream
  `CatalogUtil.deleteRemovedMetadataFiles` was forwarding `inline://`
  pseudo-URIs to `FileIO.deleteFiles` and surfacing
  `BulkDeletionFailure`; `FileIOTableOperations.commit` now overrides
  `BaseMetastoreTableOperations.commit` and routes the cleanup
  through a local helper that filters those entries.
* `testRegisterExistingTable` — re-enabled. The existence check in
  `BaseMetastoreCatalog.registerTable` fires before any I/O on the
  metadata-file URI, so the inline:// pseudo-URI never gets resolved
  and the test passes as-is.
* `testRegisterTable` — still `@Disabled`. The drop + register
  round-trip needs a register-from-bytes API on `FileIOCatalog` (no
  equivalent today). Disable message updated to point at errata T4.

The full atomic-mode × inline-mode matrix status (which cells are green,
WIP, or open) lives in [docs/COMPAT.md](docs/COMPAT.md). Replay-side
determinism is documented in [docs/SPEC_TM.md](docs/SPEC_TM.md)
§"Replay determinism" and as invariant I4 in
[docs/design.md](docs/design.md). Original plan and analysis below
preserved for context.

---

## Context

Extending the cloud test matrix from "non-inlined" to also cover `fileio.catalog.inline=true` (TM-only) and `fileio.catalog.inline=true + fileio.catalog.inline.manifests=true` (TM+ML), starting with CAS-only mode on S3 and GCS. The non-inlined matrix is green (S3-Express-append, S3-CAS, GCS-CAS, both `CatalogTests` and `CatalogTransactionTests`, 260 + 130 = 390/390 across six suites).

Pointing the upstream `CatalogTests` at `TestS3CatalogCASInlineTM` initially produced 14 failures. After two determinism fixes and seven `@Disabled` annotations for file-counting incompatibilities (already committed as `69649a3` and iceberg `ad714e683`), 8 failures remain. This document scopes the rest.

## What's already in

- **Test plumbing.** `inlineTM()` / `inlineML()` hooks on `TestS3Catalog`, `TestS3FileIOCatalogTransaction`, `GCSCatalogTest`, `GCSFileIOCatalogTransactionTests`. Eight inline subclasses for the 4-cell grid (S3-CAS-Inline-{TM,ML} × {Catalog, Transaction} and GCS-Inline-{TM,ML} × {Catalog, Transaction}). The `*InlineML` subclasses extend `*InlineTM` so disable lists flow through.
- **`@Disabled` overrides** in `TestS3CatalogCASInlineTM` and `GCSCatalogTestInlineTM` for: `testReplaceTransaction`, `testCompleteReplaceTransaction`, `testCompleteCreateOrReplaceTransactionReplace`, `testCreateOrReplaceReplaceTransactionReplace`, `testMetadataFileLocationsRemovalAfterCommit`, `testRegisterTable`, `testRegisterExistingTable`. These all assert on metadata.json files / paths that inline TM intentionally doesn't write or expose. They will require either a forked test suite or upstream patches to pass under inline mode and are out of scope here.
- **Determinism fix #1 — synthetic location.** `FileIOCatalog.loadFromCatalogFile` now uses `inline://<table>#<Arrays.hashCode(inlineMeta)>` instead of `System.nanoTime()`, so `BaseMetastoreTableOperations.refreshFromMetadataLocation` short-circuits when the catalog content is unchanged.
- **Determinism fix #2 — `last-updated-ms` on delta replay.** `InlineDeltaCodec.applyUpdates` pins `lastUpdatedMillis` via `Builder.setLastUpdatedMillis(base.lastUpdatedMillis())` before `discardChanges().build()`. The companion setter was added in iceberg `ad714e683`.

## Open design question (resolved 2026-05-06 — encoded in intention record)

The original concern: the inline-TM determinism fix relied on a determinism *hack* — pinning `Builder.lastUpdatedMillis` to `base.lastUpdatedMillis()` so the rebuilt JSON was byte-stable across replays. The base's timestamp is the wrong source of truth (the delta is a logically-new commit, not the same one), and the only reason it "worked" was that consistency was the only requirement. Two alternatives were on the table:

1. **Encode the writer's `last-updated-ms` in the delta bytes**, post-process JSON on read.
2. **Compute a deterministic synthetic value** from inputs.

**Resolution: option (1).** The wire format gained field 2 (`last_updated_ms`, varint64) on `TableMetadataDelta`, **required** on every encoded delta. `FileIOCatalog.commitInline` and the transaction-side equivalent pass `metadata.lastUpdatedMillis()` to `InlineDeltaCodec.encodeDelta(List, long)`; `applyDelta` / `applyDeltaWithManifests` decode it via `decodeDelta` (which returns `DecodedDelta`) and use it during JSON post-processing.

The writer's actual wall-clock value is the source of truth — every replay reproduces the writer's `last-updated-ms` exactly, including any new `snapshot-log` entries. No fallback / legacy path: a delta missing field 2 is malformed and `decodeDelta` raises. (This research prototype has never been deployed, so wire-level back-compat isn't a constraint.)

**Upstream setter dropped.** `InlineDeltaCodec.pinTimestamps(json, baseSnapshotLogSize, writerLastUpdatedMs)` substitutes the writer's value into the serialized JSON in two places:

1. Top-level `"last-updated-ms":N` (single occurrence, matched with `replaceFirst`).
2. Trailing `snapshot-log` entries beyond `baseSnapshotLogSize` — those are the entries `setRef` stamped during this builder pass; their `timestamp-ms` fields get rewritten to `writerLastUpdatedMs`.

The single-timestamp substitution covers both spots because, within any one builder pass, every new `snapshot-log` entry's `timestamp-ms` equals the final `metadata.lastUpdatedMillis()` (`setRef` reads `builder.lastUpdatedMillis` at stamp time; the only writes between stamps are `addSnapshot` (snapshot's own timestamp) or the wall-clock fallback; the last write before `build()` wins for the top-level field, and any earlier `setRef` saw the same value or a precursor that got overwritten). Implementation: regex on the JSON string with bracket-tracking to scope the snapshot-log scan; no JSON re-parse. The `Builder.setLastUpdatedMillis` we had added to `iceberg/core` has been reverted — the catalog rides a stock Iceberg `TableMetadata.Builder` again.

## Remaining failures (`TestS3CatalogCASInlineTM`, 8 errors)

| # | Test | Symptom | Diagnosis |
|---|---|---|---|
| A | `testUpdateTransaction` | `assertPreviousMetadataFileCount` failure at line 1906 | Disable-worthy. After the determinism fix unblocked the refresh-required failure, this one falls to the same disk-counting incompatibility as the seven already disabled. |
| B | `testRemoveUnusedSpec(boolean)[1]`, `testRemoveUnusedSchemas(boolean)[1]` | `CommitFailedException: Table metadata refresh is required` from `BaseTransaction.applyUpdates` → `UpdateSnapshotReferencesOperation.commit` (lines 1629 / 1685) | Real bug. `applyDeltaWithManifests` (the ML-aware sibling of `applyUpdates`) doesn't pin `lastUpdatedMillis`, so the second delta-replay path is still non-deterministic. Triggered when `manageSnapshots().createBranch().commit()` runs through it. |
| C | `testRemoveUnusedSpec(boolean)[2]`, `testRemoveUnusedSchemas(boolean)[2]` | After `expireSnapshots().cleanExpiredMetadata(true).commit()`, the loaded table still contains the unused spec/schema (lines 1661 / 1721) | Real bug. Either `RemoveSpec`/`RemoveSchema` updates aren't represented in our delta encoding (so the removal never propagates), or full-mode JSON serialization re-includes them. |
| D | `testRenameTable` | `tableExists(RENAMED_TABLE)` returns false right after `renameTable(TABLE, RENAMED_TABLE)` (line 982) | Real bug. `FileIOCatalog.renameTable` does `dropTable(from).createTable(to, oldLocation)`. For inline tables `oldLocation == null`, so the rename creates an entry pointing at nothing — the inline metadata bytes, manifest pool, and snapshot refs stay keyed under the old (now-dropped) id. |
| E | `testReplaceTableKeepsSnapshotLog` | After replace, `snapshotLog` size 1, expected 2 (line 2616) | Real bug. The pre-replace snapshot log entry isn't preserved through the inline replace path. Either `commitInline`'s replace branch isn't propagating `base.snapshotLog()` into the new `TableMetadata`, or the JSON round-trip is dropping it. |
| F | `testConcurrentReplaceTransactionSortOrderConflict` | "Table order should match the new order" (line 2980) | Real bug. Concurrent replace conflict not detected with inline. Likely downstream of (B), (C), or (E) — defer until those land. |

## Plan

Land in this order so each step's diff is reviewable and downstream noise collapses as we go.

### Step 1 — Disable `testUpdateTransaction` (A)

Add `@Test @Disabled(...)` overrides in `TestS3CatalogCASInlineTM` and `GCSCatalogTestInlineTM`. One-line each. Confirm the run drops to 7 errors / 18 skipped.

### Step 2 — Pin `lastUpdatedMillis` on `applyDeltaWithManifests` (B)

`InlineDeltaCodec.applyDeltaWithManifests` is the second delta-replay entry point. It builds and calls `discardChanges().build()` like `applyUpdates`, but doesn't pin `lastUpdatedMillis`. Apply the same fix:

```java
tmBuilder.setLastUpdatedMillis(base.lastUpdatedMillis());
return tmBuilder.discardChanges().build();
```

(Or, if we accept the design alternative discussed above, switch both `applyUpdates` and `applyDeltaWithManifests` to JSON post-processing in one shot and revert the upstream setter.)

Add a focused unit test under `src/test/java/org/apache/iceberg/io/TestInlineDelta.java` that calls `applyDeltaWithManifests` twice with the same inputs and asserts byte-equality of the result.

### Step 3 — Inline-aware rename (D) — resolved

Resolved by promoting `renameTable` to a first-class action
(`RenameTable`, wire type 11). The catalog state is keyed by `int` table
id throughout; the action mutates `TblEntry` in place, leaving all
id-keyed maps (`tblInlineMetadata`, `tblManifestPrefix`, `manifestPool`,
`snapshotManifests`) untouched. Inline-TM, inline-ML, and pointer-mode
rename all take the same path. See [docs/SPEC.md](docs/SPEC.md) and
[docs/SPEC_TM.md](docs/SPEC_TM.md) §"Rename"; coverage in
`TestProtoActions$RenameTableTests`,
`TestProtoActions$ConflictTests` (RT' row/column),
`TestProtoActions$IdempotencyTests`,
`TestProtoCatalogFormat#testRenameTableActionEncodeDecode`.

### Step 4 — Replace preserves snapshot log (E)

Reproduce locally with a focused unit test that mirrors the failing assertion: create an inline table with one snapshot, run `newReplaceTableTransaction(...).newAppend().appendFile(F).commitTransaction()`, then assert `loadTable(TABLE).snapshotLog().size() == 2`. That will localize whether the bug is in:
- `commitInline`'s replace branch (full mode), or
- the delta encoding for replace (does `computeDelta` know how to express "snapshot log gets a new entry"?), or
- `wrapInlineManifests` discarding history.

Most likely path: full-mode replace serializes `metadata` via `TableMetadataParser.toJson(metadata)`. If `metadata.snapshotLog()` is populated when handed to us, the JSON should include it; if it isn't populated, the upstream replace logic isn't appending to the log, in which case fix moves into `BaseTransaction.commitReplaceTransaction` invariants (probably not us).

### Step 5 — `cleanExpiredMetadata` removes specs/schemas (C)

Same approach: focused unit test that calls `expireSnapshots().cleanExpiredMetadata(true).commit()` on an inline table with multiple specs/schemas, then asserts only the reachable ones come back from `loadTable`.

If the bug is that `RemoveSpec` / `RemoveSchema` aren't in `InlineDeltaCodec.computeDelta`'s output, add them:
- New `RemoveSpecUpdate` / `RemoveSchemaUpdate` (or a single `MetadataUpdate` analogue) under `InlineDeltaCodec`'s `DeltaUpdate` family.
- `computeDelta` emits them when the new metadata's `specsById` / `schemasById` is a strict subset of base's.
- `applyTo` calls `builder.removeSpecs(...)` / `builder.removeSchemas(...)`.

If the bug is that full-mode round-trip re-includes them, the fix is upstream (or in our serialization wrapper) — investigate and report.

### Step 6 — Concurrent sort-order conflict (F)

Defer. Re-run the matrix after Steps 1–5 land; if (F) still fails, instrument `Mut.commit`'s conflict-detection path to compare against the actual reload.

## Verification

After each step:
```bash
cd /home/chris/work/catalog/iceberg && \
  ./gradlew :iceberg-core:publishToMavenLocal -x test -x integrationTest -x generateGitProperties
cd /home/chris/work/catalog/fileio-catalog && \
  mvn test -Dtest='TestS3CatalogCASInlineTM' 2>&1 | grep -E 'Tests run|ERROR.*test'
```

After Steps 1–5:
```bash
mvn test -Dtest='TestS3Catalog,TestS3CatalogCAS,TestS3CatalogCASInlineTM,TestS3CatalogCASInlineML,TestS3FileIOCatalogTransaction,TestS3FileIOCatalogTransactionCAS,TestS3FileIOCatalogTransactionCASInlineTM,TestS3FileIOCatalogTransactionCASInlineML,GCSCatalogTest,GCSCatalogTestInlineTM,GCSCatalogTestInlineML,GCSFileIOCatalogTransactionTests,GCSFileIOCatalogTransactionTestsInlineTM,GCSFileIOCatalogTransactionTestsInlineML'
```

Goal: zero failures across the 14 suites, with `*InlineTM/ML` reporting ~18 skipped for the file-counting incompatibilities.

## Out of scope

- ADLS suites (per user: faulty append implementation; revisit later).
- The 7 file-counting tests already disabled. Passing them would require either forking upstream `CatalogTests` or proposing changes to its assertions.
- Inline-ML-only failures beyond what InlineTM exposes — surface them with the matrix run after Step 5; address in a follow-up.

## Critical files

- `iceberg/core/src/main/java/org/apache/iceberg/TableMetadata.java` — `Builder.setLastUpdatedMillis` (committed `ad714e683`); revisit if we adopt the JSON-post-processing alternative.
- `src/main/java/org/apache/iceberg/io/InlineDeltaCodec.java` — Step 2 (and Step 5 if delta encoding gains `RemoveSpec`/`RemoveSchema`).
- `src/main/java/org/apache/iceberg/io/FileIOCatalog.java` — Step 4 may touch `commitInline`'s replace branch. (Step 3 landed: `renameTable` is a first-class action — see §Step 3.)
- `src/main/java/org/apache/iceberg/io/{CatalogFile,ProtoCatalogFile,ProtoCodec}.java` — Step 3 (resolved).
- `src/test/java/org/apache/iceberg/aws/s3/TestS3CatalogCASInlineTM.java`, `gcp/gcs/GCSCatalogTestInlineTM.java` — Step 1.
- New focused unit tests in `src/test/java/org/apache/iceberg/io/{TestInlineDelta,TestProtoCatalogFormat,TestInlineManifestEndToEnd}.java` so each cloud bug has a non-cloud regression.
