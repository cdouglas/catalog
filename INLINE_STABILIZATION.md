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

### M1. Verify the inline-ML matrix end-to-end

The `*InlineML` cells in the compatibility matrix (S3 CAS, GCS CAS) are
still listed as `WIP`. The suites exist (`TestS3CatalogCASInlineML`,
`GCSCatalogTestInlineML`, plus the transaction equivalents) but were not
exercised in the last matrix run. The work:

1. Run all `*InlineML` suites and triage failures. Inline-TM shielded us
   from any bug specific to `applyDeltaWithManifests`'s pool / ref-list
   bookkeeping, manifest sentinel handling in `wrapInlineManifests`, and
   the `hasMLPool`-forces-delta path in `commitInline`.
2. Add `*InlineML` subclasses on ADLS once the S3/GCS cells land — both
   atomic modes are mechanical one-line subclasses.
3. Add the `append · TM+ML` cell on S3 (one-line subclass on
   `TestS3CatalogInlineTM`) once CAS · TM+ML goes green.

Triggered by user direction: "before we tackle inline-ML, we want a
solid base with minimal exception paths." Step 1 is the gating activity.

### M2. Statistics-file changes force full mode (perf, not correctness)

`InlineDeltaCodec.computeDelta` returns `null` whenever a stats file
changed (line 774–776). Any commit that runs `setStatistics` in addition
to a real change pays full-mode bytes, defeating the delta-mode benefit
on tables that maintain stats. Adding `AddStatistics` /
`RemoveStatistics` delta types is straightforward; the JSON parsers are
already in upstream Iceberg. Defer until inline-ML matrix is green.

