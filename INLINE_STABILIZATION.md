# Inline TM/ML stabilization — open items

Inline-TM is green across the matrix — see [docs/COMPAT.md](docs/COMPAT.md).
The replay path was simplified in 2026-05-07: synthetic location is
derived from `(tableId, version)` (no content hashing), the writer's
`last-updated-ms` flows through delta field 2 and is pinned on
`TableMetadata.Builder` via `setLastUpdatedMillis` (one-line addition
to the iceberg fork's `Builder`), and the `pinTimestamps` regex pass
over JSON is gone. Replay determinism is now a property of values,
not bytes.

This file tracks the inline-ML work still ahead and a couple of
TM-side gaps that surfaced during the cleanup. Resolved shortcuts and
deferred functionality live in [docs/errata.md](docs/errata.md).

## Open

### M1. Inline-ML matrix triage (GCS run 2026-05-07)

GCS inline-ML run: `GCSCatalogTestInlineML` 102 tests / 5 failures /
0 errors / 11 skipped; `GCSFileIOCatalogTransactionTestsInlineML`
28 tests / 2 failures / 6 errors. Total **13 failures across 3
distinct modes** plus one outlier. Inline-TM shielded us from these
because either `wrapInlineManifests` was a no-op (no pool) or the
ML-specific code paths weren't exercised.

Reproduce:
```
mvn test -Dtest='GCSCatalogTestInlineML,GCSFileIOCatalogTransactionTestsInlineML'
```

#### Mode A — Replace transaction adds an extra `metadata-log` entry (4 tests)

- `testReplaceTransaction`
- `testCompleteReplaceTransaction`
- `testCreateOrReplaceReplaceTransactionReplace`
- `testCompleteCreateOrReplaceTransactionReplace`

Assertion: `previousFiles().size() == 1`, observed 2. The two entries
have *different* synthetic-location forms:

```
[MetadataLogEntry{file=inline://#9d55ea46},
 MetadataLogEntry{file=inline://newdb.newtable#2}]
```

Two layers each generate a synthetic location: `InlineDeltaCodec.applyDelta`
emits `inline://#<hash-of-base-bytes>` as the parsed-base
`metadataFileLocation` (used by `Builder.buildFrom` to populate
`previousFiles`); `FileIOCatalog.loadFromCatalogFile` separately uses
`inline://<id>#<version>` as the BaseMetastoreTableOperations cache
key. Both forms end up in the rebuilt TM's `previousFiles()` —
metadata-log accumulates one entry per layer instead of one per
logical predecessor.

Inline-TM has the same architecture but doesn't trip the assertion
because `wrapInlineManifests` is a no-op there (no pool); only
inline-ML reconstructs the TM through `Builder.buildFrom` →
`replaceSnapshots` → `build`, which is where the second metadata-log
entry leaks in. **Fix path:** unify the two synthetic-location
generators on `inline://<id>#<version>`. Pass `(tableId, baseVersion)`
through to `applyDelta` / `applyDeltaWithManifests` (or precompute the
location at the FileIOCatalog layer and thread it down). Eliminates
the double-entry by construction.

#### Mode B — Sentinel-without-pool corruption (6 transaction tests)

- `catalogTxWithSingleOp`, `concurrentTx`, `concurrentTxOnBranch`
- `txAgainstDifferentBranchesWithSerializable`
- `txAgainstMultipleTables` (×2 isolation levels)

All hit the corruption check in `FileIOCatalog.wrapInlineManifests`:

```
IllegalStateException: Inline snapshot <id> has sentinel manifest-list
location 'inline://<id>' but no pool entry — catalog state corrupt
```

This is real catalog state corruption: `AddSnapshotUpdate` fired
(sentinel written) but no matching `AddManifestUpdate` reached the
pool. The trigger appears specific to the multi-table / cross-branch
transaction paths — single-table, single-branch inline-ML commits
work (the `*Catalog* ` suite has no Mode B failures). Likely
correlated with **errata D7** (ML deltas on *existing* snapshots
silently dropped by `commitInline`'s "new snapshots only" loop), but
the transaction layer may also be staging deltas that don't reach
`commitInline` at all when a transaction spans multiple tables or
branches. Needs targeted reproduction: add a unit test that mirrors
`txAgainstMultipleTables` against `TestInlineManifestEndToEnd`'s
fixtures, instrument `commitInline`'s `mlDeltas.entrySet()` vs
`metadata.snapshots()`, see what's missing.

#### Mode C — NPE on `metadataFileLocation` (2 transaction tests)

- `readTableAfterLoadTableInsideTx`
- `txAgainstMultipleTablesLastOneFails`

Stack:
```
java.lang.NullPointerException: Cannot invoke "String.equals(Object)"
  because the return value of "TableMetadata.metadataFileLocation()" is null
  at BaseCatalogTransaction.validateSerializableIsolation(BaseCatalogTransaction.java:171)
```

Root cause: `wrapInlineManifests` rebuilds the parsed `TableMetadata`
through `Builder.buildFrom(parsed) → replaceSnapshots → discardChanges →
build`. `Builder.buildFrom` carries `previousFileLocation` from base but
leaves the new TM's `metadataLocation` null (it's the *new* version's
location, normally set by the caller). Caller doesn't set it for
inline tables, so the rebuilt TM has `metadataFileLocation() == null`.
Then `BaseCatalogTransaction.validateSerializableIsolation` calls
`.equals(...)` on the null and NPEs. The test expected a
`ValidationException` (which uses the same string-compare path).
Inline-ML-specific because the `Builder.buildFrom → build` only runs
in `wrapInlineManifests`, which is a no-op without a manifest pool.

**Fix path:** preserve `metadataFileLocation` across the rebuild.
Cleanest is `builder.withMetadataLocation(parsed.metadataFileLocation())`
— the side effects (`lastUpdatedMillis = base.lastUpdatedMillis()`,
`previousFileLocation = null`) are correct for our case (no logical
change, just an in-memory swap of `BaseSnapshot` for `InlineSnapshot`).
Verify it doesn't conflict with the fork's `replaceSnapshots`/
`snapshotsReplaced` flag.

#### Outlier — `testCompleteCreateTransactionMultipleSchemas`

Failure: `[Spec ID should match] expected: 1 but was: 0`. Different
shape from Modes A–C; involves multi-schema/spec replay. Not yet
diagnosed. Defer until A/B/C are landed; may be downstream of one of
them.

#### Sequencing

A and C are localized fixes (~10-30 lines each). B is the structural
one — likely needs a reproducer and may surface design questions in
how the transaction layer stages ML deltas for cross-table commits.
Land A and C first; they may also collapse the outlier.

#### After GCS goes green

1. Run S3 inline-ML matrix (`TestS3CatalogCASInlineML`,
   `TestS3FileIOCatalogTransactionCASInlineML`) — same architectural
   path, same expected results.
2. Add `*InlineML` subclasses on ADLS — mechanical one-line
   subclasses once the underlying CAS · TM+ML cells are green.
3. Add the `append · TM+ML` cell on S3 (one-line subclass on
   `TestS3CatalogInlineTM`).

### M2. Statistics-file changes force full mode (perf, not correctness)

`InlineDeltaCodec.computeDelta` returns `null` whenever a stats file
changed (line 774–776). Any commit that runs `setStatistics` in addition
to a real change pays full-mode bytes, defeating the delta-mode benefit
on tables that maintain stats. Adding `AddStatistics` /
`RemoveStatistics` delta types is straightforward; the JSON parsers are
already in upstream Iceberg. Defer until inline-ML matrix is green.

