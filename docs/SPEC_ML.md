# Inline Manifest Lists (ML)

This document specifies the inline-manifest-list extension to
`ProtoCatalogFormat`: how the catalog absorbs the *second* level of Iceberg
table state (the manifest list). It builds on inline table metadata
([SPEC_TM.md](SPEC_TM.md)) and the base catalog format ([SPEC.md](SPEC.md)).

## Why

Today, every data commit writes three files:

1. **Manifest file(s)** — Avro files containing data file entries.
2. **Manifest list** — Avro file listing the manifest files for a snapshot.
   Rewritten in full on every commit.
3. **Table metadata** — JSON file with full table state (eliminated by TM
   inlining).

Plus the catalog update.

The manifest list shows the same redundancy pattern as the metadata file:
the entire file is rewritten on every commit, but typically only one
`ManifestFile` entry is new. After 10 commits, ~91% of every write is
redundant; after 100, ~99%.

Inlining manifest lists into the catalog intention record:

- removes the per-commit `snap-*.avro` write entirely;
- combined with TM, reduces a typical data commit from ~100 KB across
  three writes to **~250 bytes in one write**;
- carries forward unchanged manifests at zero per-commit cost (they live
  in the catalog's manifest pool, not in every snapshot's list).

Manifest *files* and data files remain external. The catalog now absorbs
two levels of table state; everything below `ManifestFile` stays where it
is.

## Storage Model

Each inline table carries:

- A **manifest pool**: a deduplicated map of `manifest_path` →
  `ManifestFile` entry. Each unique manifest is stored once per table,
  regardless of how many snapshots reference it.
- **Per-snapshot index lists**: each snapshot's manifest list is an array
  of pool indices.

Consecutive snapshots of a `FastAppend` table share all but one entry, so
the pool gets ~50× compression versus storing each snapshot's full list
verbatim.

### In-Memory Form

```java
// On ProtoCatalogFile
Map<Integer, Map<String, ManifestFile>> manifestPool;     // tblId → path → entry
Map<Integer, Map<Long, List<String>>>    snapshotManifests; // tblId → snapId → ordered paths
```

In memory, manifest references are path-keyed (not index-keyed) so that
expiring snapshots and rewriting the pool don't invalidate other
snapshots' references.

### On-Wire Form

```protobuf
message InlineTable {
  // fields 1-6: see SPEC_TM.md
  repeated ManifestFileEntry     manifest_pool  = 7;
  repeated SnapshotManifestRefs  snapshot_refs  = 8;
}

message ManifestFileEntry {
  string manifest_path_suffix   = 1;   // relative to table.manifest_list_prefix
  int64  manifest_length        = 2;
  int32  partition_spec_id      = 3;
  int32  content                = 4;   // 0 = data, 1 = deletes
  int64  sequence_number        = 5;
  int64  min_sequence_number    = 6;
  int64  added_snapshot_id      = 7;   // fixed64 encoding
  int32  added_files_count      = 8;
  int32  existing_files_count   = 9;
  int32  deleted_files_count    = 10;
  int64  added_rows_count       = 11;
  int64  existing_rows_count    = 12;
  int64  deleted_rows_count     = 13;
  repeated PartitionFieldSummaryEntry partitions = 14;
  bytes  key_metadata           = 15;
  int64  first_row_id           = 16;  // present whenever set, 0 included
}

message PartitionFieldSummaryEntry {
  bool  contains_null = 1;
  bool  contains_nan  = 2;
  bytes lower_bound   = 3;
  bytes upper_bound   = 4;
}

message SnapshotManifestRefs {
  int64          snapshot_id  = 1;
  repeated int32 pool_indices = 2;  // positions in manifest_pool
}
```

Wire encoding uses positional indices into `manifest_pool` so the on-disk
representation is compact. The path-keyed map is reconstructed on decode.

## Delta Updates

Two `TableMetadataUpdate` types carry manifest list changes inside an
`UpdateTableInline` delta:

```protobuf
message AddManifestDelta {
  int64              snapshot_id = 1;
  ManifestFileEntry  manifest    = 2;   // full ManifestFile entry
}

message RemoveManifestDelta {
  int64  snapshot_id          = 1;
  string manifest_path_suffix = 2;      // identifies manifest to remove
}
```

These are independent updates, not embedded in `AddSnapshot`. Each carries
the snapshot ID it belongs to. On apply:

- `AddManifestDelta` adds `manifest` to the table's pool (idempotent by
  path) and appends the path to the snapshot's reference list.
- `RemoveManifestDelta` removes the path from the snapshot's reference
  list.

Both operations cover every `SnapshotProducer` subclass in Iceberg —
`FastAppend`, `MergeAppend`, `BaseOverwriteFiles`, `BaseRowDelta`,
`BaseRewriteFiles`, `BaseReplacePartitions`, `StreamingDelete`,
`CherryPickOperation`, `BaseRewriteManifests`. The
`(added ManifestFiles, removed paths)` shape composes through all of them.

| Commit type     | Delta                                         |
|-----------------|-----------------------------------------------|
| `FastAppend`    | 1 `AddManifestDelta`                          |
| `MergeAppend`   | 1+ `AddManifestDelta`, 0+ `RemoveManifestDelta` |
| `Overwrite`     | 1+ `AddManifestDelta`, 0+ `RemoveManifestDelta` |
| `RowDelta`      | 1 `AddManifestDelta` (delete manifest)        |
| `RewriteFiles`  | 1+ `AddManifestDelta`, 0+ `RemoveManifestDelta` |
| `RewriteManifests` | full replacement (large delta)             |

Carried-forward manifests cost **zero bytes** per commit. They live in the
pool already; the new snapshot's `pool_indices` array references them.

## Sink Integration

`SnapshotProducer.apply()` checks whether the bound `TableOperations`
implements `ManifestListSink`:

```java
public interface ManifestListSink {
  void stageManifestListDelta(
      long sequenceNumber,
      long snapshotId,
      Long parentSnapshotId,
      Long nextRowId,
      ManifestListDelta delta,        // added ManifestFiles + removed paths
      Long nextRowIdAfter);

  final class ManifestListDelta {
    public List<ManifestFile> added();
    public List<String> removedPaths();
  }
}
```

When the sink is present, the producer:

1. Computes the full finalized manifest list (sequence numbers, v3+
   `first_row_id` assigned — identical to what `ManifestListWriter`
   would write to Avro).
2. Computes the delta vs. the parent snapshot's manifest list, comparing
   by `manifest_path`.
3. Calls `sink.stageManifestListDelta(...)` with the delta, which
   `FileIOCatalog` does not keep (below).
4. Returns an `InlineSnapshot` (an Iceberg-core class with
   `manifestListLocation() == null`) holding the full in-memory list, so
   the same JVM session can continue without reading the manifest list
   from storage, and the commit can send it.

**No `snap-*.avro` file is written** when the sink path is taken.

When `ops` does not implement `ManifestListSink`, the producer writes the
manifest list to a `snap-*.avro` file as before and returns a normal
`BaseSnapshot`. The default path is unchanged.

## FileIOCatalog Integration

`FileIOCatalog.newTableOps()` instantiates `InlineManifestTableOperations`
(extends `FileIOTableOperations` and implements `ManifestListSink`) when
`fileio.catalog.inline.manifests=true`. Otherwise it uses the existing
`FileIOTableOperations`. This ensures `SnapshotProducer` only takes the
sink path when the catalog is prepared to store ML deltas.

A commit takes each new snapshot's list from the snapshot: the snapshots
in the new metadata that are `InlineSnapshot`s and not in the base, which
the commit's producers returned. It turns them into manifest updates in
the same intention record that carries the table update
(`FileIOTableOperations.manifestUpdates`):

```java
List<DeltaUpdate> manifests = null;                   // null: no new inline snapshot
for (Snapshot snap : metadata.snapshots()) {
  if (!(snap instanceof InlineSnapshot) || base.snapshot(snap.snapshotId()) != null) {
    continue;                                         // committed: its list is final
  }
  if (manifests == null) manifests = new ArrayList<>();
  List<ManifestFile> list = snap.allManifests(io);    // held by the snapshot
  List<ManifestFile> parent =                         // the list the replay builds on
      snap.parentId() == null ? List.of()
      : isNew(metadata.snapshot(snap.parentId())) ? metadata.snapshot(snap.parentId()).allManifests(io)
      : proto.hasInlineManifests(tblId, snap.parentId()) ? proto.inlineManifests(tblId, snap.parentId())
      : null;                                         // parent's list is a file
  if (parent == null) {
    attachManifestDelta(manifests, snap.snapshotId(), list, List.of(), prefix);
  } else {
    attachManifestDelta(manifests, snap.snapshotId(),
        added(parent, list), removedPaths(parent, list), prefix);   // by manifest path
  }
}
```

Only snapshots the commit adds send lists. A committed snapshot's list
never changes (design.md I3a), so a delta the sink was offered for one,
or for a snapshot that never reached the metadata (errata D7), changes
nothing. A new snapshot's list is sent as a delta against its parent's
list, and the replay builds it on the parent's *inline* list
(`NewSnapshotManifests`). A parent whose list is an Avro file has no
inline list, so a snapshot on such a parent carries its whole list.
Such parents come from writers that do not inline manifest lists and
from eviction (see "Writers in Different Modes" below).

The commit used to send the deltas the sink was offered, kept on the
table handle that received them until a commit landed. A catalog
transaction's commit has only its tables' new metadata, so it looked
the handle up in a per-thread registry of loaded tables, and took
another handle's leftover stage when one had it (a table transaction
abandoned, or whose commit failed before it landed): its own snapshot
then got its parent's list, and the acknowledged append was lost
(`TestCatalogRegressions.CatalogTransactionWithAnotherHandleStaged`).

The manifest updates ride in whichever form the table update takes
(see [SPEC_TM.md](SPEC_TM.md) §"Delivery Modes"): appended to the TM
delta, or bundled with full metadata as the `UpdateTableInline.ml_delta`
payload. A full update or a create sends the payload whenever the commit
adds an inline snapshot, even with no updates in it (a delete that
matched nothing changes no manifests); the replay registers every new
inline snapshot's list, so the invariant *sentinel ⇒ list* holds. A
metadata-file update (eviction) writes each inline snapshot's list to
an Avro file instead.

A multi-table `commitTransaction` follows the same path: each table's
new snapshots hold their lists, and its `stageUpdate` sends them.

### Create-with-snapshots

A `createTransaction()` that runs one or more `newFastAppend` calls
before `commitTransaction()` lands as a single inline create with
embedded snapshots. The `CreateTableInline` action carries an optional
`delta` field (`AddManifestDelta` entries only) that
`InlineDeltaCodec.applyManifestsOnly` consumes on apply, the way a
full-mode update does. The TM bytes inside the same
action carry sentinel manifest-list locations
(`inline://<snapshotId>`) for every freshly-created snapshot — see
[SPEC_TM.md](SPEC_TM.md) §"Commit Protocol" for the `InlineSnapshot →
BaseSnapshot+sentinel` rewrite that produces them. Without the rewrite,
the `gzip` codec falls into Iceberg's v1-embedded-manifests serialization
branch and decodes manifests with `partitionSpecId` hardcoded to 0;
multi-spec create transactions surface the wrong spec on read.

### ID collision under inline-ML

`InlineDeltaCodec.computeDelta` returns `null` when two racing replace
transactions have independently assigned the same schema / spec /
sort-order id to different content (see [SPEC_TM.md](SPEC_TM.md)
§"Concurrent-replace id collisions"). The commit then takes full mode,
which carries the manifest updates as its `ml_delta` payload, so
the new snapshots' lists land atomically with the TM replacement. (The
multi-table path used to throw `CommitFailedException` here instead;
Iceberg's replace-transaction retry reuses the producer's metadata, so
the collision recurred on every attempt.)

### Writers in Different Modes

Writers sharing a catalog need not agree on `fileio.catalog.inline*`:
each writer's properties decide only how *it* writes. A table is either
inline or a pointer table, never both: an `UpdateTable` record (a
metadata file location) clears the table's inline state, pool included.

- A **pointer writer** (no inline TM) committing to an inline-ML table,
  and an inline writer **evicting** one (an update that does not fit in
  `fileio.catalog.inline.max.update.size`), write a metadata file. A
  metadata file can name only manifest list files, so
  `writeUpdateMetadata` first writes each `InlineSnapshot`'s list to
  `<metadata dir>/snap-<snapshotId>-<uuid>.avro`
  (`ManifestListFiles.write`, the same writer `SnapshotProducer` uses;
  the manifests are already final, so they are copied unchanged). The
  snapshot then names that file. The lists are written before the
  catalog commit; a commit that fails leaves them behind, as it leaves
  its metadata file.
- An **inline-TM writer** committing a snapshot to an inline-ML table
  writes an Avro manifest list for it; the table stays inline, in mixed
  mode.
- An **inline-ML writer** committing on a parent whose list is a file
  sends the new snapshot's whole list (above). Committing to a pointer
  table, it brings the table back inline if the full metadata, with the
  manifest updates, fits; the older snapshots keep their files.

A committed snapshot's manifest list never changes, whether it is
inline or a file, so any writer reads the same manifests.

### Loading

`loadFromCatalogFile` detects inline tables and, for each snapshot whose
ID is in the table's `snapshotManifests` map, replaces the parsed
`BaseSnapshot` with an `InlineSnapshot` carrying the resolved manifest
list from the pool. Snapshots without inline ML (older pointer-mode
snapshots that survived a migration) remain as `BaseSnapshot` with their
original `manifestListLocation`.

## Configuration

| Property                          | Default | Description |
|-----------------------------------|---------|-------------|
| `fileio.catalog.inline`           | `false` | Inline TM (see SPEC_TM.md). |
| `fileio.catalog.inline.manifests` | `false` | Inline ML. Requires `inline=true`. |
| `fileio.catalog.inline.max.update.size` | 4,190,208 | Largest inline table update, manifest updates included (see SPEC_TM.md). |

Setting `inline.manifests=true` without `inline=true` raises
`IllegalArgumentException` at catalog initialization. ML requires TM
because manifest-list deltas are carried inside the TM intention record.

Valid combinations:

| `inline` | `inline.manifests` | Behaviour                          |
|----------|--------------------|------------------------------------|
| false    | false              | Baseline: pointer TM, Avro ML     |
| true     | false              | TM-only: inline TM, Avro ML       |
| true     | true               | TM+ML: inline TM + inline ML      |
| false    | true               | rejected                          |

## Gradual Migration

Existing snapshots keep their `manifestListLocation` (pointer to Avro).
Only new snapshots produced while `inline.manifests=true` get inline ML.
The loader handles mixed snapshots: `InlineSnapshot` for those with inline
data, `BaseSnapshot` for those with Avro pointers. There is no one-time
rewrite.

`InlineSnapshot.equals` compares only the snapshot's scalar identity, not
its manifest list, so a `BaseSnapshot` and an `InlineSnapshot` with the
same snapshot ID and scalar fields compare equal. This is load-bearing for
expiration and retention logic that builds `Set<Snapshot>` over a mix of
the two types.

## What Remains External

```
Catalog file (single object):
  ├── Catalog state (namespaces, table registry)
  ├── Per-table: TableMetadata (schemas, specs, properties, snapshot history)
  └── Per-table: Manifest list (ManifestFile entries in the pool)

External storage:
  ├── Manifest files (.avro) — data file entries with column statistics
  └── Data files (.parquet/.orc)
```

Query engines reading an inline-ML snapshot get the manifest list from the
catalog (no `snap-*.avro` read) but still read individual manifest files
from external storage.

## Iceberg-Core Hooks

The fork in `../iceberg/` adds these extension points (all on the
`atomicio` branch, alongside the atomic-operations hooks):

| Hook | File | Notes |
|------|------|-------|
| `ManifestListSink` interface       | `core/.../ManifestListSink.java`  | Sink interface |
| `SnapshotProducer.apply()` sink check | `core/.../SnapshotProducer.java` | Routes to sink when `ops` implements it |
| `InlineSnapshot` class             | `core/.../InlineSnapshot.java`    | Public; constructable from `org.apache.iceberg.io` |
| `BaseSnapshot.equals` loosened     | `core/.../BaseSnapshot.java`      | Accepts any `Snapshot` with matching scalars |
| `TableMetadata.Builder.replaceSnapshots` | `core/.../TableMetadata.java` | Used to swap parsed snapshots for `InlineSnapshot` |

The first three are clean upstream candidates. The
`BaseSnapshot.equals` change and the `replaceSnapshots` builder addition
are tracked as integration debt in [errata.md](errata.md) — both are
candidates for replacement with a parser-level wrap that avoids the core
fork.
