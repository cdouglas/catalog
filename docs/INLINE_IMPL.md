# Inline Table Metadata -- PB Implementation Plan

Tracks implementation of [INLINE_INTENTION.md](../INLINE_INTENTION.md) for
`ProtoCatalogFormat`. Each stage is independently testable; all tests pass
between stages.

## Stages

### Stage 1: State model + checkpoint encoding for InlineTable

**Goal:** The catalog can store and round-trip inline tables in the checkpoint.
No transaction-log actions yet -- just checkpoint-level persistence.

**Deliverables:**
- `catalog.proto`: add `InlineTable` message (id, version, namespace_id, name,
  metadata bytes, manifest_list_prefix). Add `repeated InlineTable inline_tables = 13`
  to `Checkpoint`.
- `ProtoCatalogFile`: add `Map<Integer, byte[]> tblInlineMetadata` (opaque JSON)
  and `Map<Integer, String> tblManifestPrefix`. A table ID appears in either
  `tableById` (pointer) or `tblInlineMetadata` (inline), never both.
- `ProtoCatalogFile.Builder`: `addInlineTable(id, nsId, name, version, metadata,
  manifestPrefix)`, `removeInlineTable(id)`, plus accessors.
- `ProtoCodec`: encode/decode `InlineTable` in checkpoint (new field number 13).
- `CatalogBuilder` (test): `.inlineTbl(id, nsId, name, version, metadata, prefix)`.
- **Tests (positive):** checkpoint roundtrip with inline tables, mixed inline +
  pointer tables coexist, inline table accessible via `location()` returns null
  (not a pointer), new accessor for inline metadata returns bytes.
- **Tests (negative):** table ID in both pointer and inline maps rejected at build.
- **Tests (randomized):** extend `generateRandom` to include inline tables.

**Status:** Complete

---

### Stage 2: CreateTableInline action

**Goal:** Transactions can create inline tables via the log.

**Deliverables:**
- `catalog.proto`: add `CreateTableInline` message. Add field 10 to `Action` oneof.
- `ProtoCodec`: `CreateTableInlineAction` class (verify, apply), encode/decode.
- **Tests (positive):** create inline table in existing namespace, late-bound
  namespace, inline table appears in state after apply.
- **Tests (negative):** namespace version mismatch rejects transaction, entire
  transaction rejected when one action fails.
- **Tests (integration):** create inline + create pointer in same transaction,
  checkpoint roundtrip after applying CreateTableInline.

**Status:** Not started

---

### Stage 3: UpdateTableInline -- Full + Pointer modes

**Goal:** Inline tables can be updated (full metadata replacement) or evicted
to pointer mode. Delta mode is NOT yet implemented.

**Deliverables:**
- `catalog.proto`: add `UpdateTableInline` message with `oneof payload`
  (full_metadata bytes, metadata_location string). Delta field declared but
  not yet decoded.
- `ProtoCodec`: `UpdateTableInlineAction` (verify, apply for FULL and POINTER).
- **Tests (positive):** full-mode update replaces metadata, pointer-mode evicts
  inline table to pointer, version increments.
- **Tests (negative):** version mismatch, update on pointer table (wrong action
  type), update on nonexistent table.
- **Tests (transition):** inline->pointer transition verified in state model,
  pointer table has location but no inline metadata.

**Status:** Not started

---

### Stage 4: Delta infrastructure + simple deltas

**Goal:** Delta-mode updates work for property and location changes. This
establishes the delta application framework without Iceberg TableMetadata
parsing.

**Deliverables:**
- `catalog.proto`: add `TableMetadataDelta`, `TableMetadataUpdate` (oneof),
  `SetTableProperties`, `SetTableLocation`.
- Delta application engine: dispatch on update type, apply to metadata bytes.
  Requires parsing TableMetadata JSON (via `TableMetadataParser`), applying
  the change, and re-serializing.
- `UpdateTableInlineAction`: DELTA mode support.
- **Tests (positive):** set/remove properties via delta, change location via
  delta, multiple updates in single delta.
- **Tests (negative):** delta on pointer table fails, version mismatch.

**Depends on:** Iceberg `TableMetadata`, `TableMetadataParser` (already in deps).

**Status:** Not started

---

### Stage 5: Schema, partition spec, and sort order deltas

**Goal:** Structural evolution operations work via delta.

**Deliverables:**
- `catalog.proto`: add `AddSchema`, `SetCurrentSchema`, `AddPartitionSpec`,
  `SetDefaultPartitionSpec`, `AddSortOrder`, `SetDefaultSortOrder`.
- Delta application for each type.
- **Tests:** one positive + one negative per update type. Schema evolution
  roundtrip (add schema, set current, verify metadata).

**Status:** Not started

---

### Stage 6: Snapshot deltas

**Goal:** The core data-commit path works: AddSnapshot with dictionary encoding,
SetSnapshotRef, RemoveSnapshots.

**Deliverables:**
- `catalog.proto`: add `CompactSummary`, `OperationType` enum, `AddSnapshot`,
  `SetSnapshotRef`, `RemoveSnapshots`.
- AddSnapshot delta application with dictionary-derived fields (sequence number,
  parent snapshot, timestamp, manifest list path, schema ID, first row ID).
- CompactSummary encode/decode (typed fields + extra map).
- **Tests (positive):** single AddSnapshot + SetSnapshotRef (typical data commit),
  multiple snapshots in sequence (forward-difference chain), RemoveSnapshots.
- **Tests (negative):** missing manifest prefix, stale state derivation.
- **Tests (dictionary):** manifest_list_suffix + prefix reconstruction,
  timestamp delta encoding, implicit field derivation.

**Status:** Not started

---

### Stage 7: computeDelta + mode selection

**Goal:** The system can diff two TableMetadata instances and produce a minimal
delta. Mode selection picks delta/full/pointer based on size.

**Deliverables:**
- `InlineMetadataDelta.computeDelta(oldMeta, newMeta, manifestListPrefix)`.
- `InlineMetadataDelta.selectMode(table, delta, txnSize)` with 4 MiB limit.
- **Tests:** diff produces correct delta for data commit (AddSnapshot +
  SetSnapshotRef), schema evolution, property changes, snapshot expiry.
  Mode selection returns POINTER when delta exceeds limit.

**Status:** Not started

---

### Stage 8: FileIOCatalog integration

**Goal:** End-to-end: table creates and updates use inline metadata through
the FileIOCatalog commit path.

**Deliverables:**
- Wire `ProtoCatalogFormat.Mut` to produce `CreateTableInline` /
  `UpdateTableInline` actions.
- Modify `FileIOCatalog.doCommit()` to use inline path when format supports it.
- Compaction: checkpoint includes inline tables, eviction when oversized.
- **Tests:** end-to-end create table, data commit, schema evolution, compaction
  with inline tables, mixed inline/pointer catalog.

**Status:** Not started

---

## Progress Log

| Date | Stage | Notes |
|------|-------|-------|
| 2026-04-08 | Stage 1 | InlineTable proto, state model, checkpoint encode/decode, 6 tests |
