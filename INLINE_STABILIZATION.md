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

### M2. Statistics-file changes force full mode (perf, not correctness)

`InlineDeltaCodec.computeDelta` returns `null` whenever a stats file
changed (line 774–776). Any commit that runs `setStatistics` in addition
to a real change pays full-mode bytes, defeating the delta-mode benefit
on tables that maintain stats. Adding `AddStatistics` /
`RemoveStatistics` delta types is straightforward; the JSON parsers are
already in upstream Iceberg.

## Closed

### M1. Inline-ML matrix triage (GCS run 2026-05-07) — RESOLVED 2026-05-07

GCS inline-ML run was 13 failures across 3 modes plus one outlier;
matrix is now green except for the deferred outlier (errata D9).

#### Mode A — Replace transaction adds an extra `metadata-log` entry — fixed

Symptom (4 tests): `previousFiles().size() == 1`, observed 2.
`InlineDeltaCodec.applyDelta` and `wrapInlineManifests` each rebuilt
the `TableMetadata` and each contributed one entry to `previousFiles`.

Fix: in `wrapInlineManifests`, call
`builder.withMetadataLocation(parsed.metadataFileLocation())` before
`build()`. The fork's `withMetadataLocation` carries
`lastUpdatedMillis` from base and nulls `previousFileLocation` —
exactly the right semantics for an in-memory `BaseSnapshot` →
`InlineSnapshot` swap (no logical change, no new metadata-log entry).
Side effect: also fixes Mode C below by preserving
`metadataFileLocation` across the rebuild.

#### Mode B — Sentinel-without-pool corruption — fixed

Symptom (6 tests, multi-table / cross-branch transaction paths):
`Inline snapshot <id> has sentinel manifest-list location
'inline://<id>' but no pool entry — catalog state corrupt`.

Two distinct bugs surfaced through diagnostic instrumentation:

1. **Validation-pass clobber** (`txnSinkOps`).
   `BaseCatalogTransaction.validateSerializableIsolation` calls
   `origin.loadTable()` for every read table during its own
   `commitTransaction()`. Each call constructed a fresh
   `InlineManifestTableOperations` with empty `stagedDeltas` and
   overwrote the user-staged ops in `txnSinkOps`. By the time
   `FileIOCatalog#commitTransaction` drained the registered ops, the
   real staged deltas were gone, no `AddManifestUpdate` made it into
   the delta, and the AddSnapshot replay wrote the
   `inline://<snapId>` sentinel without a corresponding pool entry.

   Fix: peek-not-clobber in `FileIOCatalog#loadTable`. Skip the
   overwrite when the existing `txnSinkOps` entry has unflushed
   staged deltas (new `hasStagedDeltas()` peek method on
   `InlineManifestTableOperations`).

2. **Empty inline-ML snapshot** (delete-only on empty table).
   `InlineDeltaCodec.applyDeltaWithManifests`'s `AddSnapshotUpdate`
   branch wrote the `inline://<snap>` sentinel but only registered
   pool entries through the `AddManifestUpdate` path. A delete-only
   snapshot on an empty table has zero added manifests, so
   `attachManifestDelta` emitted no `AddManifestUpdate`s, and the
   pool ended up without an entry for the snapshot. `wrapInlineManifests`
   then flagged the sentinel as catalog-state corruption.

   Fix: in `applyDeltaWithManifests`, when an inline-ML
   `AddSnapshotUpdate` (empty `manifestListSuffix`) is applied,
   pre-register an empty `setSnapshotManifests(tblId, snapId, [])`
   so `hasInlineManifests` returns true even for snapshots with no
   manifests. The carry-forward inheritance in subsequent
   `AddManifestUpdate` calls is unaffected (it checks
   `refs.isEmpty()`).

#### Mode C — NPE on `metadataFileLocation` — fixed

Symptom (2 tests): `BaseCatalogTransaction.validateSerializableIsolation`
NPE'd on `currentTableMetadata.metadataFileLocation().equals(...)` because
the rebuilt TM in `wrapInlineManifests` had `metadataLocation == null`
(`Builder.buildFrom` carries it as `previousFileLocation`, not
`metadataLocation`).

Fix: same `withMetadataLocation` call as Mode A — preserves the
metadata file location across the in-memory rebuild.

#### Outlier — `testCompleteCreateTransactionMultipleSchemas` — deferred

Symptom: `[Spec ID should match] expected: 1 but was: 0`. An inline-ML
create-transaction that evolves the spec mid-flight loses per-manifest
`partitionSpecId` on read.

Root cause and fix path documented in [docs/errata.md](docs/errata.md)
D9. The inline-ML create path uses `TableMetadataParser.toJson`, which
serializes `InlineSnapshot`s through the v1-embedded-manifests branch
of `SnapshotParser.toJson` (no `manifestListLocation`). On read,
`GenericManifestFile`'s `InputFile` constructor hardcodes `specId=0`,
so files in non-zero spec manifests decode against the wrong spec.

Fix requires routing inline-ML create through the sentinel + pool
encoding (as the update path does), which means an extension to the
catalog Mut/codec to populate the manifest pool atomically with
`createTableInline`. Until then, inline-ML create-transactions are
limited to single-spec tables.

#### After GCS goes green

1. Run S3 inline-ML matrix (`TestS3CatalogCASInlineML`,
   `TestS3FileIOCatalogTransactionCASInlineML`) — same architectural
   path, same expected results.
2. Add `*InlineML` subclasses on ADLS — mechanical one-line
   subclasses once the underlying CAS · TM+ML cells are green.
3. Add the `append · TM+ML` cell on S3 (one-line subclass on
   `TestS3CatalogInlineTM`).
