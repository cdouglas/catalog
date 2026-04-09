# Inline Table Metadata -- PB Implementation Plan

Tracks implementation of [INLINE_INTENTION.md](../INLINE_INTENTION.md) for
`ProtoCatalogFormat`. Each stage is independently testable; all tests pass
between stages.

## Architecture Note (E8)

`BaseMetastoreTableOperations` exposes a 4-argument `refreshFromMetadataLocation`
overload accepting a custom `Function<String, TableMetadata> metadataLoader`. For
inline tables, `FileIOTableOperations` supplies a loader that deserializes from
the catalog's inline bytes instead of fetching from storage. No changes to shared
base classes in the iceberg/ fork are needed. See `docs/INLINE_ERR.md` E8.

This means FileIOCatalog integration (Stage 4) can proceed immediately using
Full mode from Stage 3 -- delta optimization is deferred to Stages 5-8.

## Stages

### Stage 1: State model + checkpoint encoding for InlineTable

**Goal:** The catalog can store and round-trip inline tables in the checkpoint.

**Status:** Complete

---

### Stage 2: CreateTableInline action

**Goal:** Transactions can create inline tables via the log.

**Status:** Complete

---

### Stage 3: UpdateTableInline -- Full + Pointer modes

**Goal:** Inline tables can be updated (full metadata replacement) or evicted
to pointer mode. Delta mode is NOT yet implemented.

**Status:** Complete

---

### Stage 4: Mut-level inline API + FileIOCatalog integration

**Goal:** End-to-end: table creates and updates use inline metadata through
the FileIOCatalog commit path. Uses Full mode only (no deltas needed).

**Deliverables:**
- `CatalogFile.Mut`: add `createTableInline(TableIdentifier, byte[])` and
  `updateTableInline(TableIdentifier, byte[])` methods. Default implementations
  throw `UnsupportedOperationException`.
- `ProtoCatalogFormat.Mut`: override to accumulate inline mutations. Wire
  `buildActions()` to produce `CreateTableInlineAction` / `UpdateTableInlineAction`
  from inline maps.
- `FileIOTableOperations.doRefresh()`: when `location()` returns null, check for
  inline metadata. Use `refreshFromMetadataLocation(syntheticLoc, null, 0,
  customLoader)` to load metadata from inline bytes.
- `FileIOTableOperations.doCommit()`: when inline mode is active, serialize
  metadata to JSON bytes and use inline Mut methods instead of writing external
  file.
- Configuration: `fileio.catalog.inline` property (default false) to opt in.
- **Tests (Mut API):** `buildTransaction()` from inline Mut produces correct
  action types.
- **Tests (end-to-end):** create inline table, load it, update it, verify
  metadata survives catalog roundtrip.
- **Tests (negative):** version conflict on inline update.

**Status:** Complete

---

### Stage 5: Delta infrastructure + SetTableProperties/SetTableLocation

**Goal:** Delta-mode updates work for property and location changes.

**Deliverables:**
- `catalog.proto`: add `TableMetadataDelta`, `TableMetadataUpdate` (oneof),
  `SetTableProperties`, `SetTableLocation`.
- Delta application engine using `TableMetadataParser` + `TableMetadata.Builder`.
- `UpdateTableInlineAction`: DELTA mode support.
- **Tests:** set/remove properties via delta, change location via delta.

**Status:** Complete (merged with Stage 6)

---

### Stage 6: Schema, partition spec, sort order, and snapshot deltas

**Goal:** All metadata update types work via delta, including the core data-commit
path (AddSnapshot + SetSnapshotRef).

**Deliverables:**
- `catalog.proto`: add `AddSchema`, `SetCurrentSchema`, `AddPartitionSpec`,
  `SetDefaultPartitionSpec`, `AddSortOrder`, `SetDefaultSortOrder`,
  `CompactSummary`, `AddSnapshot`, `SetSnapshotRef`, `RemoveSnapshots`.
- AddSnapshot with dictionary-derived fields.
- **Tests:** per-type positive/negative, typical data commit roundtrip.

**Status:** Complete (merged with Stage 5)

---

### Stage 7: computeDelta + mode selection

**Goal:** The system can diff two TableMetadata instances and produce a minimal
delta. Mode selection picks delta/full/pointer based on size.

**Deliverables:**
- `InlineMetadataDelta.computeDelta(oldMeta, newMeta, manifestListPrefix)`.
- `InlineMetadataDelta.selectMode(table, delta, txnSize)` with 4 MiB limit.
- **Tests:** data commit diff, schema evolution diff, mode selection fallback.

**Status:** Complete

---

### Stage 8: Delta integration into FileIOCatalog

**Goal:** Upgrade FileIOCatalog commit path from Full-only to delta-when-possible.

**Deliverables:**
- Wire `computeDelta` + `selectMode` into `FileIOTableOperations.doCommit()`.
- Compaction: checkpoint includes inline tables, eviction when oversized.
- **Tests:** end-to-end data commit with delta mode, compaction, eviction.

**Status:** Not started

---

## Progress Log

| Date | Stage | Notes |
|------|-------|-------|
| 2026-04-08 | Stage 1 | InlineTable proto, state model, checkpoint encode/decode, 6 tests |
| 2026-04-08 | Stage 2 | CreateTableInline action + encode/decode, 5 action tests |
| 2026-04-08 | Stage 3 | UpdateTableInline (Full + Pointer), 4 tests (update, evict, mismatch, concurrent) |
| 2026-04-09 | Stage 4 | Mut-level inline API, FileIOCatalog integration, fix addNamespace flake. 3 Mut tests |
| 2026-04-09 | Stage 5+6 | InlineDeltaCodec: all 11 delta types, CompactSummary, DELTA mode in UpdateTableInlineAction. 14 tests |
| 2026-04-09 | Stage 7 | computeDelta + selectMode. 8 tests (property/location/schema diff, roundtrip, mode selection) |
