# Manifest List Inline — Abstraction Sufficiency Design Notes

**Date:** 2026-04-17
**Scope:** Whether the `ManifestListSink` / `ManifestListDelta` /
`AddManifestUpdate` / `RemoveManifestUpdate` abstractions are sufficient
to correctly round-trip every Iceberg operation type — not just
`FastAppend` — through the inline-ML path.

The companion document [ML_INLINE_IMPL_REVIEW.md](./ML_INLINE_IMPL_REVIEW.md)
covers correctness bugs in the current implementation. This one is about
whether the *shape* of the abstraction is right — i.e., whether fixing
the implementation bugs would be enough to support operations beyond
`FastAppend`.

## Short answer

The **ML-specific** abstractions (sink + per-snapshot add/remove delta)
are structurally sufficient for every `SnapshotProducer` subclass. But
there are three adjacent gaps in the delta framework as a whole that
any non-FastAppend operation will hit on reload, even after the
FastAppend fixes land.

## What fits cleanly

Every operation creates a new snapshot via `SnapshotProducer.apply()`,
which now computes a full manifest list and hands a path-based
`(added, removedPaths)` delta to the sink. That delta composes through
everything:

| Operation               | What `apply()` produces               | Delta shape                                      |
|-------------------------|----------------------------------------|--------------------------------------------------|
| `FastAppend`            | new manifests carried over parent      | `+K new`, `−0`                                   |
| `MergeAppend`           | new + merged manifests                 | `+1..K + M_merged`, `−merged_originals`          |
| `BaseOverwriteFiles`    | new + filtered (rewritten) parent      | `+K new + rewritten`, `−filtered_originals`      |
| `BaseRewriteFiles`      | compaction replacements                | `+replacements`, `−compacted_originals`          |
| `BaseRowDelta`          | delete manifest(s) + data edits        | `+delete_manifests [+ rewritten]`, `−originals`  |
| `BaseReplacePartitions` | new partitions minus dropped           | `+new_partition_mfs`, `−dropped_partition_mfs`   |
| `StreamingDelete`       | delete manifest(s)                     | `+delete_manifests`, `−originals if rewritten`   |
| `CherryPickOperation`   | duplicate of staged snapshot's ML      | `+source_not_in_parent`, `−parent_not_in_source` |
| `BaseRewriteManifests`  | fully rewritten manifest list          | `+all new manifests`, `−all originals` (large)   |

All extend `SnapshotProducer` or `MergingSnapshotProducer`, and all go
through the single `apply()` → `stageManifestList()` path. The
`content()` field on `ManifestFile` distinguishes DATA vs. DELETES, so
delete manifests flow through `AddManifestUpdate` correctly. Key
metadata, partition summaries, and row-lineage `first_row_id` per
manifest are all carried on the `ManifestFile` itself, so the pool
stores the full finalized entry.

The `CherryPickOperation` case is the trickiest — it copies the source
snapshot's manifests onto the target branch. If the source snapshot is
already in the pool (because it was committed via the sink), the
`AddManifestUpdate` entries re-add by path, and `addManifestToPool` is
keyed by path and so is idempotent
(`ProtoCatalogFile.java:552-555`). That composes.

So the **per-snapshot abstraction shape is right**. Any composition of
ADDs and REMOVEs against a parent ref list expresses every operation.

## What doesn't fit — three gaps

### Gap 1: No snapshot-expiration cascade to the pool

`InlineDeltaCodec.RemoveSnapshotsUpdate.applyTo(TableMetadata.Builder)`
at `InlineDeltaCodec.java:576-578`:

```java
@Override
public void applyTo(TableMetadata.Builder builder) {
  builder.removeSnapshots(snapshotIds);
}
```

Removes the snapshots from `TableMetadata`, but the catalog's
`snapshotManifests` and `manifestPool` maps are untouched. Expiring
snapshots leaves their ref lists in `snapshotManifests` and their
exclusively-referenced entries in `manifestPool` forever. There's no
`removeFromPool` or `removeSnapshotManifests` method on
`ProtoCatalogFile.Builder`.

This isn't a gap in the ML delta *shape* — it's a missing cascade in
the expiration path. Once someone runs `ExpireSnapshots` on an
inline-ML table the pool grows without bound.

**Needs:** a new `ProtoCatalogFile.Builder.removeSnapshotManifests(
tblId, snapshotId)` that drops the ref list and GCs pool entries with
zero remaining references, invoked from `RemoveSnapshotsUpdate.applyTo`
(once that gets the `ProtoCatalogFile.Builder` routing discussed in the
implementation review).

### Gap 2: v3+ row-lineage `next-row-id` not carried in delta

`AddSnapshotUpdate` carries `addedRows` but not `firstRowId`
(`InlineDeltaCodec.java:587`). The JSON it builds in
`applyTo(TableMetadata, String)` at line 613+ has no `first-row-id`
field either.

In pointer mode this is fine: the full `TableMetadata` JSON carries
`next-row-id` and is rewritten on every commit, so the counter is
restored from the stored JSON.

In delta mode, the catalog stores only the delta bytes and reconstructs
by `applyDelta(baseJson, deltaBytes)`. The reconstructed snapshot has no
`first-row-id`, so `TableMetadata.Builder.addSnapshot()` can't update
`next-row-id`. The counter stays at whatever the base has — silently
breaking row lineage guarantees for v3+ tables on every commit.

The sink interface does pass `nextRowId` and `nextRowIdAfter` to
`stageManifestListDelta`, but `InlineManifestTableOperations.stageManifestListDelta`
discards them (`FileIOCatalog.java:551-556`).

**Needs:** either add `firstRowId` to `AddSnapshotUpdate`'s wire format,
or add a `SetNextRowIdUpdate` delta type. Either fix is small; nothing
currently carries the value through delta mode.

### Gap 3: Statistics-file updates don't have a delta type at all

Iceberg's `MetadataUpdate` hierarchy includes `SetStatistics`,
`RemoveStatistics`, `SetPartitionStatistics`, `RemovePartitionStatistics`.
`InlineDeltaCodec.computeDelta` doesn't handle any of them. `selectMode`
will still see an "empty" delta for a pure `ANALYZE TABLE` commit and
pick `"delta"`, but the delta captures no actual change. On reload, the
statistics are gone.

This is **not ML-specific** — it pre-exists the ML work and affects TM
delta mode too. But ML-enabled tables inherit it.

**Needs:** four new `DeltaUpdate` subclasses (`SetStatisticsUpdate`,
`RemoveStatisticsUpdate`, and the partition equivalents). Or a generic
"raw metadata update" fallback that falls through to `"full"` mode on
unhandled update types.

## Not gaps, but worth noting

- **Multi-snapshot transactions.** `TransactionImpl` can stage multiple
  snapshots and commit them together. The sink correctly keys staged
  deltas by snapshot id. `commitInline`'s loop at `FileIOCatalog.java:431-441`
  iterates new snapshots and attaches each delta. Should work, but is
  untested.
- **Rollback / branch-set / tag-create** don't go through
  `SnapshotProducer`. They're ref-map changes on `TableMetadata`.
  Covered by the existing `SetSnapshotRefUpdate`.
- **`InlineSnapshot.equals`/`hashCode`** still missing. Not an
  abstraction gap; an implementation gap (tracked in
  ML_INLINE_IMPL_REVIEW.md §2.1) that will bite expiration.

## Bottom line

The `(added ManifestFiles, removed paths)` shape correctly models every
snapshot-producing operation in Iceberg. The ML abstractions compose;
the **framework they sit in** has three adjacent gaps:

1. **Expiration doesn't cascade to the pool** — fix by extending
   `RemoveSnapshotsUpdate` with pool GC.
2. **`next-row-id` isn't round-tripped in delta mode** — fix by adding
   `firstRowId` to `AddSnapshotUpdate` (or a dedicated update type).
3. **Statistics files aren't in the delta universe** — fix with four
   new update types, or a fallback-to-full mode when unhandled updates
   appear in `computeDelta`.

Recommend gating `fileio.catalog.inline.manifests=true` on format
version < 3 until gap 2 is closed, and documenting that snapshot
expiration leaks pool memory until gap 1 is closed. Gate the feature
behind a "preview" flag until all three are resolved.
