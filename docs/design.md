# FileIOCatalog Design Notes

High-level design, invariants, and best practices for working in this
codebase. For the intention log the catalog runs on see [LOG.md](LOG.md),
for the catalog file's layout [ATOMIC_FILE_LOG.md](ATOMIC_FILE_LOG.md), and
for the catalog's messages [SPEC.md](SPEC.md), [SPEC_TM.md](SPEC_TM.md) and
[SPEC_ML.md](SPEC_ML.md). For known shortcuts and open issues see
[errata.md](errata.md).

## What This Is

`FileIOCatalog` is an Iceberg catalog whose entire state — namespaces,
tables, optionally table metadata, optionally manifest lists — lives in a
single object in cloud storage. There is no separate metastore. All
mutation goes through cloud-storage conditional writes (`if-match` ETag,
generation number, append offset), provided by the `SupportsAtomicOperations`
extension to `FileIO`.

The catalog is a state machine replicated through an intention log
(intentlog-core, [LOG.md](LOG.md)), and the object is one provider of that
log (`AtomicFileLog`). `LogCatalog`, the catalog's code, runs on any provider:
`FileIOCatalog` is `LogCatalog` over an `AtomicFileLog`, and the test suites
also run it on a shared log whose checkpoints are stored apart from its
records (`InMemorySharedLog`).

The catalog absorbs successive levels of Iceberg state:

| Level                 | External today | Inlined when                        |
|-----------------------|----------------|-------------------------------------|
| Catalog (this project)| —              | always                              |
| `TableMetadata` (JSON)| metadata.json  | `fileio.catalog.inline=true`        |
| Manifest list (Avro)  | snap-*.avro    | `fileio.catalog.inline.manifests=true` |
| Manifest files (Avro) | external       | (not absorbed)                      |
| Data files            | external       | (not absorbed)                      |

When fully inlined, a typical data commit is one ~250-byte atomic write to
one object instead of three writes totalling ~100 KB.

## Architecture

### Snapshot + Mutation

A read of the catalog's log (`CatalogLog.read`) is an **immutable
snapshot**: a `Replica` of the catalog's state (`CatalogState`) at the log
position the read ended at, with the log's committed set. All mutation goes
through a transaction (`CatalogMutation`) that accumulates pending changes
on it and commits them atomically:

1. Read the catalog (`CatalogLog.read`).
2. Stage a `CatalogMutation` on the read.
3. Accumulate changes (`createTable`, `dropNamespace`, etc.).
4. `commit()` appends its intention (`CatalogMutation.intention`) to the
   log as one record.
5. The commit loop (intentlog-core's `LogRuntime.commit`) handles what
   the log reports. After a CAS conflict it replays the new log, checks
   the record's preconditions again, and rebases the same record onto it;
   after a lost append race it resends the same record at the new tail;
   it resends a throttled write after a backoff
   (`fileio.catalog.throttle.retry.min-wait-ms`); it waits a random,
   growing time after each lost race before it reads again
   (`fileio.catalog.race.retry.min-wait-ms`); and it settles a write
   whose outcome it did not learn by reading the log for the
   transaction's record, or throws `CommitStateUnknownException` (I10). A
   record whose preconditions fail goes back to the caller: the catalog
   (`LogCatalog`) stages a catalog operation again on a fresh read
   (`commitOnAFreshRead`), and Iceberg re-applies a table commit; a
   catalog transaction fails ([errata.md](errata.md) §D12). See
   [LOG.md](LOG.md) §"The commit loop".

### Two Mutation Strategies

On the catalog file, every commit is either:

- **Append** — append a length-prefixed intention record to the end of
  the file, conditional on the file's current length.
- **CAS** — atomically replace the entire file with
  `[header][checkpoint][gc][record]`, conditional on the file's
  ETag/generation.

Append is fast and incremental; CAS is full-rewrite but compacts the log.
The format alternates between them based on log-size thresholds. See
[ATOMIC_FILE_LOG.md](ATOMIC_FILE_LOG.md) §"Commits on one object". On a
shared log with checkpoints apart, every commit appends, and a writer
whose commit landed compacts by the same thresholds: a GC record, a
checkpoint through it, a trim ([LOG.md](LOG.md) §"Compaction").

### Codec Layout

Three codecs cooperate at write/read time:

- `ProtoCodec` handles catalog-level messages: `Checkpoint`, `Intention`,
  `Action` oneof, all catalog/namespace/table actions. The envelope around
  them (log.proto) is intentlog-core's `LogEnvelope`.
- `InlineDeltaCodec` handles inline TM delta messages:
  `TableMetadataDelta`, all 15 `TableMetadataUpdate` types,
  `CompactSummary`. The `ManifestFileEntry` an `AddManifestDelta`
  carries is encoded by `ProtoCodec`, which also writes it in the
  checkpoint's manifest pool.
- `InlineMetadataCodec` (with two implementations,
  `StructuralInlineMetadataCodec` and `GzipInlineMetadataCodec`)
  encodes full `TableMetadata` bytes inside `InlineTable`,
  `CreateTableInline`, and `UpdateTableInline.full_metadata`. The codec
  used is identified by a per-record tag on the wire (`structural` /
  `gzip`); see [SPEC_TM.md](SPEC_TM.md) §"Inline-TM codecs". The
  writer's `fileio.catalog.inline.tm.codec` property selects the codec
  for new writes; decode always follows the wire tag, so codecs can
  mix freely within a single catalog file.

`ProtoCodec` and `InlineDeltaCodec` write protobuf by hand; nothing is
generated from `catalog.proto`. Inline TM bytes are opaque `bytes`
fields in it, in the format their codec tag names. `catalog.proto`
declares every field the codecs write, with the wire type they use, and
two tests hold it to that (516e9c4): `CatalogProto` checks every catalog
file the suites and fuzzers write against it (through
`CheckpointRoundTrip`), and `TestFormatExplorer` checks the codecs'
field-number constants against it in both directions. So a protobuf
tool given `catalog.proto` decodes the checkpoint and each log record
(past the file's header and length prefixes). The one departure from
`protoc`'s output, a negative int32 as a 5-byte varint, is one `protoc`
readers accept (see the header of `catalog.proto`).

### Replay State

Replay keeps each inline table as fields, not as Iceberg objects.
`CatalogState` maps a table to an `InlineTable`, which holds the table's
encoded bytes, its `TableFields`, or both, and makes either from the
other when asked. `TableFields` holds `TableMetadata`'s fields; its
snapshots and snapshot log are `Chain`s, append-only lists that
successive states share, and its metadata log a `Window`, the newest
entries of such a list up to `write.metadata.previous-versions-max`, so
that a record extends each in place rather than copying it. Landing,
`TableChange` and the encodings read a table through `MetadataView`,
which `TableFields` implements, and which adapts a `TableMetadata` too.

- A delta of snapshots, refs and properties (`AddSnapshot`,
  `SetSnapshotRef`, `SetProperties`, then its manifest-list updates)
  applies to the fields (`NativeDelta`). It reproduces
  `TableMetadata.Builder` exactly: the sequence-number and row-id
  checks, the next row id, the snapshot log with intermediate snapshots
  suppressed, one metadata-log entry per record (trimmed to
  `write.metadata.previous-versions-max`) and last-updated-ms. Any other
  delta goes through the builder on the table's metadata
  (`InlineDeltaCodec.applyDelta`), and full metadata replaces the bytes.
- Iceberg's objects are made on demand, not per record: a
  `TableMetadata` when a table is loaded (built directly by
  `InlineTableMetadata.create` and kept per version), the encoded bytes
  when a checkpoint is written, a builder only on the slow path above.
  That the catalog can produce every object a v3 table would have stored
  (metadata, manifest lists) is the compatibility it keeps; producing
  them is not replay's job.
- `CatalogState` shares what a batch of records leaves alone: a table's
  manifest pool and lists are persistent maps (`PersistentMap`, a hash
  array mapped trie), so a change copies the few nodes on its path and
  building a state copies nothing; a table's conflict-encoding state is
  copied on its first change in a builder. The committed set is kept in
  runs that a build prepends to (LOG.md "The committed set and GC"). So a
  fold costs its records, not the catalog. A load reuses the
  `InlineSnapshot`s earlier loads made (`LogCatalog.InlineSnapshots`).
- In the test suites (surefire sets `-Dfileio.catalog.replay.check=true`),
  every native apply also runs through the builder and must give the
  same metadata, and every encode of an inline table must round-trip
  exactly (I11).

Replay went from about 200 records/s on a 300-snapshot table to over
100,000 ([REPLAY_BENCH_RESULTS.md](REPLAY_BENCH_RESULTS.md), the JMH
`ReplayBench`).

### Component Separation

This project consumes Iceberg as a Maven SNAPSHOT artifact. The
`SupportsAtomicOperations` interface, the `ManifestListSink` extension
point, and the cloud-provider `SupportsAtomicOperations` implementations
(`S3FileIO`, `GCSFileIO`, `ADLSFileIO`) all live in the iceberg fork
(branch `atomicio`, see the [README](../README.md)). The catalog format
and the FileIO catalog itself live here, in modules:

- `intentlog-core`: the intention log, with no Iceberg in it (the
  enforcer bans Iceberg, Hadoop and Guava): the SPIs, replay, the
  committed set, the commit loop, the envelope.
- `intentlog-fileio` and `intentlog-memory`: its providers,
  `AtomicFileLog` and `InMemorySharedLog`.
- `intentlog-testkit`: the provider contract and the commit-protocol
  fuzzer.
- `iceberg-fileio-catalog`: the catalog, its state machine and codecs.

## Invariants

These hold in the current code and must not regress. I1, I2, I6, I10 and
I11 are the intention log's, for any state machine on any provider; each
says what the catalog adds.

### I1. One intention record per commit

A commit (`LogRuntime.commit`) writes its intention as one record, in one
atomic write: an append, or the replace that compacts. Replay applies the
record whole or not at all, and every resend of it is the same bytes under
the same id. So a reader never sees part of a commit.

In the catalog, a single `doCommit` / `commitTransaction` stages one
intention, with one action per affected table. Every configuration is
subject to this rule:

- **pointer**: one `UpdateTableLocation` / `CreateTable` action referencing
  a pre-written `v<N>.metadata.json`.
- **TM inline**: one `UpdateTableInline` / `CreateTableInline` action
  carrying delta or full metadata (codec-encoded).
- **TM+ML inline (update)**: one `UpdateTableInline` action whose delta
  carries the TM updates *and* the per-snapshot `AddManifestDelta` /
  `RemoveManifestDelta` entries in the same blob.
- **TM+ML inline (create-with-snapshots)**: one `CreateTableInline`
  action whose `metadata` carries the codec-encoded TM (with sentinel
  manifest-list locations for every freshly-created snapshot) and
  whose optional `delta` field carries the matching `AddManifestDelta`
  entries that populate the pool. Apply runs the create and the
  manifest-pool fill in the same `Action.apply()` call.

A compaction or crash mid-flight must not be able to observe a half-applied
commit. ML actions are never written separately from the TM action that
introduces the snapshot they belong to.

### I2. A record applies at most once

Replay applies a record once, whatever copies of it the log holds: a resend
after a lost reply or a lost race, a send the log throttled and that landed
beside its retry, a request delayed in flight. The committed set skips a
copy whose id it holds, and its GC horizon one whose id GC may have dropped
([LOG.md](LOG.md) §"The committed set and GC"). This holds for any state
machine, blind writes included. The protocol fuzzers append a copy of any
record that ever landed, trimmed or compacted ones included, and require
that the state, the committed set and the horizon do not change.

The catalog adds a second line of defence: each action's `verify` checks
preconditions (table version, namespace version, read-set versions), and
each action's `apply` mutates state so a replay of the same record against
the post-apply state fails `verify` (S2, S3). Both must hold.

`apply()` bumps the version of any entity whose state it mutates: the
table on a table update or rename, the namespace itself on
`SetNamespaceProperty` and `RemoveNamespaceProperty`, and the *parent
namespace* on actions that change its children set (`CreateNamespace`,
`DropNamespace`, `CreateTable`, `CreateTableInline`, `DropTable`).
`RenameTable` bumps both the source and the destination namespace.

### I3. Inline metadata rotation preserves the manifest pool

Replaying an inline TM update (delta, full, or pointer-eviction) must
never silently drop the catalog's per-table manifest pool unless the
eviction is explicitly to pointer mode. `updateInlineMetadata` preserves
the pool; `removeInlineMetadata` clears it. Callers pick by intent, not
convenience. Every `UpdateTableLocation` is such an eviction: a table is
inline or a pointer table, never both, and the metadata file it names
carries a manifest list file for every snapshot (its writer wrote the
inline ones out first). An inline table's pool holds exactly the
manifests its lists name, each under its own path; each list names a
manifest once; and the table has a list for exactly each snapshot whose
manifest list is an `inline://` sentinel (a load skips a path the pool
lacks, so a lost entry would read as a manifest missing, not an error).
`CheckpointRoundTrip` checks this on every catalog file the suites and
fuzzers write (`malformedLists`).

### I3a. A committed snapshot's manifest list never changes

Whether a snapshot's list is inline or a file, and whichever writer
reads it, it lists the same manifests with the same entries (sequence
numbers, spec, adding snapshot, counts, first row id). A delta sends
the lists of only the snapshots it adds, taken from the snapshots
themselves. A full-metadata commit (a create, or an update in full
mode) also sends the list of every inline snapshot it names that the
table no longer holds a list for: its handle loaded the table inline,
and another writer has since evicted it (Bug 62: a replace transaction
commits the snapshots it loaded). Eviction writes inline lists to
files unchanged; a new inline snapshot on a parent whose list is a file
carries its whole list. `TestCatalogFaultProperties` checks this after
every operation, and in v3 tables that a data file's row ids never
change.

### I4. Inline replay is deterministic

Every reader that replays the same records rebuilds the same metadata
for every inline table, whatever its clock. I6 and I7 rest on this: a
compactor writes a checkpoint of the state it replayed, and a delta
must replay to the metadata its writer committed. The location a table
loads from does not depend on the bytes: it is
`inline://<table>#<id>.<version>` (`FileIOCatalog.metadataLocation`;
keyed by version since 416b2c9, and by table id since Bug 22), so
`BaseMetastoreTableOperations` reloads when the table's version changes
(see [SPEC_TM.md](SPEC_TM.md) §"Table Loading").

Two operational rules keep replay deterministic:

1. The base a delta applies to has its version's location,
   `inline://<table>#<id>.<version>`, the one a load of that version
   reports, as its `metadataFileLocation`, not null. Without it,
   `Builder.buildFrom(base).discardChanges().build()` skips
   `addPreviousFile` and the new TM's metadata-log loses the entry for
   the prior version. `NativeDelta` adds the same entry. (Before replay
   kept tables as fields, the location was a hash of the base's bytes.)
2. Every builder replay opens is pinned to the writer's last-updated-ms,
   which the delta carries (`setLastUpdatedMillis`), **before** any update
   is applied. `Builder.lastUpdatedMillis` starts null, and both
   `Builder.setRef` for `main` and `Builder.build` then stamp
   `System.currentTimeMillis()`: setRef as the snapshot-log time of a move
   to a snapshot the builder did not add, build as the table's
   last-updated-ms. Since Iceberg 1.12.0 `Builder.addSnapshot` no longer
   sets the field, so a delta that only adds a snapshot needs the pin too
   (Bug 60).

A delta's new snapshots are added in the same builder as its refs and
other metadata updates, as a writer's commit adds its snapshot and moves
the branch in one builder: Iceberg logs main's move to a snapshot that
builder added at the snapshot's own timestamp, which replay reconstructs
as the delta's last-updated-ms plus `timestampDeltaMs`, and a move to any
other snapshot (a rollback) at last-updated-ms (Bug 60).

### I5. Inline-ML snapshots encode as sentinels, never as embedded paths

Every `Snapshot` written into inline-mode `TableMetadata` bytes carries
a non-null `manifestListLocation()`. For snapshots whose manifest list
lives in the catalog's manifest pool (inline-ML), that location is the
sentinel `inline://<snapshotId>`. The `InlineSnapshot` type (which
returns `null` from `manifestListLocation()`) is a runtime in-memory
representation only — before any codec encode of `TableMetadata`
that contains `InlineSnapshot` instances, the snapshots must be
rewritten to `BaseSnapshot` with the sentinel set, using
`TableMetadata.Builder.replaceSnapshots`. The rewrite is the only path
that keeps both codecs (`gzip` and `structural`) producing the same
on-wire shape: a `manifest-list` field with the sentinel, no embedded
`manifests` array, no v1-style path list.

On the read side, `wrapInlineManifests` (FileIOCatalog) reverses the
transformation by swapping each sentinel-bearing `BaseSnapshot` for an
`InlineSnapshot` whose `manifests` come from the pool. Loading rejects
a sentinel without a matching pool entry (`IllegalStateException`,
"catalog state corrupt") — catalog state must always satisfy "sentinel
⇒ pool entry".

The delta-update path emits sentinels through `AddSnapshotUpdate`
(which encodes the manifest-list suffix; for inline-ML the suffix is
empty and the consumer reconstructs `inline://<snapshotId>`). The
full-mode and pointer-mode write paths rely on the I5 rewrite.

### I6. A checkpoint holds exactly the replayed state

Compaction writes a checkpoint of the state a writer replayed from the
checkpoint and log: the log's id, the committed set and its horizon
exactly, and the state machine's `encodeCheckpoint` of the state, which
must round-trip (S4). So replaying the log and decoding a checkpoint of
the result must give the same state. For the catalog: every namespace,
property and
table by id, with its metadata location, inline metadata, codec,
manifest list prefix, manifest pool and snapshot references, and the
name lookups, id counters and committed-transaction set. An apply step
may not leave state a checkpoint does not represent: a table with both
a metadata location and inline metadata (Bug 27, which the next
compaction reverted to the inline copy), inline state for a dropped
table, an inline table without a prefix. `CheckpointRoundTrip` checks
every catalog file written in `TestCatalogFaultProperties`,
`TestMultiWriterProperties` and the `TestLocalCatalog*` suites.

### I7. A delta replays to the metadata its writer committed

An inline delta must hold every change of its commit, or
`InlineDeltaCodec.computeDelta` must refuse it (return null, so the
commit sends the full metadata). A change the delta has no update for
and does not refuse is lost on replay: a format-version upgrade (Bug 37)
or a removed ref (Bug 39) made alongside another change, a schema id or
count of 0 written as absent (Bugs 33, 38, 40), a value derived on
replay that the commit did not derive the same way (Bugs 41, 42).
Replay must also build as the writer did: TableMetadata.Builder checks
the default spec and sort order against the current schema on every
build, so a run of metadata updates is applied to one builder and built
once (Bug 49: an intermediate state of a replace to a schema without
the partition column did not validate), with the commit's new snapshots
in it (Bug 60: Iceberg logs main's move to a snapshot at the snapshot's
time only if the same builder added it). And it must number new schemas
and specs as the writer did: the builder gives each the id of an equal
one, else one past the highest, and replay adds before it removes (Bug
52: a transaction that cleaned up unused specs, then went back to one).
Replay's last column and partition ids are the base's, up to the
highest in the schemas and specs it adds, so a commit whose last id is
above those goes in full (Bug 63: a transaction that added a partition
field, removed it, and cleaned up the spec that had it).
`TestInlineDeltaProperties` runs random Iceberg operations and checks
that each commit's delta, replayed on its base as the inline codec
stores it, gives the committed metadata, and that the codec stores
every version exactly (Bug 43).

### I8. An inline catalog reads like a pointer catalog

Storing a table inline -- its metadata, its manifest lists, or both --
changes how it is stored, not what a reader sees. The same operations
on a table in a pointer catalog and on one in an inline catalog must
commit or fail alike and read back the same: format version, sequence
numbers and next row id, schemas, specs, sort orders and properties,
snapshots (by position, with their summaries and row-id fields) and
their manifest lists entry by entry and in order, refs and the snapshot
log, and every ref's live data and delete files with their sequence
numbers and row ids. Snapshot ids, UUIDs, timestamps and the names of
manifest and metadata files differ. `TestInlineDifferentialProperties`
checks this after every step of generated schedules, in each inline
mode, with tables moving in and out of inline storage (Bugs 50, 51).

### I9. A catalog operation that races others ends as a serial run

A catalog operation -- a namespace create, drop or property change, a
table create, drop, rename or property change -- depends on what it
read only through its preconditions, which it checks again when its
write conflicts. So one whose writes others' operations preceded must
report what it would have, and leave the catalog as it would, had it
run after them: return the same value or fail the same way, with the
same namespaces, properties and tables. `TestCatalogRaceProperties`
runs one operation with another just before its first write and a
third just before its second, in mixed modes, against a serial run of
the same operations on another copy (Bugs 53, 54, 55).

### I10. A landed commit is never reported as failed

A commit's outcome is one of three, and a caller can rely on each: it
returned (the intention took effect, once), it was rejected
(`CommitRejectedException`: it did not and never will), or it is unknown.
The commit loop rejects a commit only before any write, or after a read
of the log that shows its record rejected or absent with no write of it
still able to land. A write whose outcome it did not learn -- a lost
reply, a failed read-back, a throttled write that may have landed -- it
settles by reading the log; if that read fails, or the loop runs out of
attempts while such a write may still be in flight, the commit is
unknown. So is a commit whose id the committed set's GC horizon has
passed, if one of its writes may have landed; otherwise it is rejected,
since replay rejects its record from then on (Bug 64). A retention
shorter than a commit takes is legal but lossy: it makes this case
common, but never incorrect ([LOG.md](LOG.md) "The committed set and
GC"). A writer judges its record by replay's judgment where it landed
before the committed set,
since another compactor's GC can drop the id of a record that applied;
and on a log without conditional appends, it counts a throttled send as
a write that may have landed, never lets a read fall behind the one it
staged on, and settles a write only on a read whose records include it
([LOG.md](LOG.md) §"The commit loop").

For the catalog the stakes are files: Iceberg deletes a commit's new
files when it fails with `CommitFailedException` (a `CleanableFailure`),
and a caller may run a failed commit again, so a commit that landed must
not report failure. `CatalogLog` maps a rejection to
`CommitFailedException` (or the conflict it explains) and an unknown
outcome to `CommitStateUnknownException`. Work after the commit lands
does not change its outcome: the catalog deletes the metadata files that
dropped out of a table's log through `CatalogUtil.deleteFiles`, which
logs a failure instead of throwing it (Bug 61). `TestCatalogFaultProperties`,
`TestMultiWriterProperties` and the `CommitProtocol` fuzzer check that an
operation reported failed any way but unknown did not take effect, and the
latter two check it again once every delayed write has landed.

### I11. A checkpoint is the replay of the log through its position

Every checkpoint holds exactly what replay of the log through its position
reaches, whoever wrote it and whatever their clock said. So every
checkpoint at one position is the same, and readers that start from
different checkpoints of one log apply the same records and agree. Only a
GC record moves the committed set's horizon; a checkpoint never filters
the set by its writer's clock (it did before GC records, which made
readers of a shared log disagree: `ckclock` in the TLA+ models). A
compaction on the catalog file writes the checkpoint of the version it
replaces, then its GC record, then its own record; a compaction on a
shared log appends its GC record first and checkpoints after it
([LOG.md](LOG.md) §"Compaction"). `CheckpointRoundTrip` checks every
replace in the local suites and the fuzzers (its checkpoint equals the
replay of the version replaced, committed set and horizon included);
`InMemorySharedLog` refuses a second checkpoint at a position that
differs from the first; and the `CommitProtocol` and multi-writer
fuzzers check every stored checkpoint against a replay of the log
through it.

For the catalog, a checkpoint writes each inline table's encoded bytes,
while replay may hold the table only as fields (§"Replay State"), so the
inline codec must round-trip exactly: decoding a table's encoding and
encoding the result again gives the same bytes, the order of every map
included. The structural codec writes snapshot summaries in its uniform
mode only when every summary has the same keys in the same order. The
checkpoint writes what the catalog's state holds in maps (namespaces,
tables, properties, an inline table's manifest pool and lists) in the
order of their keys, not in a map's iteration order, which depends on how
the map was built (Bug 80, found by the runtime's check of each fold of a
read onto a replica against a replay of the whole view). A
codec that normalized the order made a compaction's checkpoint differ
from the replay it replaced (`TestBlindAppendEncoding`'s I11 check found
it). In the suites, `InlineTable` checks the round trip on every encode.

### I12. A conflict encoding admits only what Iceberg's retry commits

Every conflict encoding (`ConflictEncoding`, [SPEC.md](SPEC.md)
§"Conflict Encodings") meets one contract, so the encoding decides how
many records conflict, never what a table holds:

1. **Deterministic** (S1): its witness and its judgment are functions of
   the record and the state replay reached, so every reader admits the
   same records (I4, I11). Its state is in the checkpoint
   (`conflict_state`).
2. **Stable** (S2): a record it rejects where it lands, it rejects at
   every later position, since a writer that read the log may already
   have reported the rejection (I10). A condition that could flip back
   (a ref rolled back, an id reused, a snapshot expired) is guarded by
   stamps that only grow.
3. **Total**: it never throws; what it cannot decode, it coarsens.
4. **Coarsens when blind**, to the Lamport witness, the version read: a
   pointer table, full metadata, a read pin, a pinned update (a table a
   catalog transaction writes, [errata.md](errata.md) §D12, or an update
   Iceberg commits once), a record without the footprint it needs.
5. **Merges exactly**: a record admitted on a changed table goes through
   `Landing`, which finalizes its late-bound fields as Iceberg's retry
   would ([SPEC_TM.md](SPEC_TM.md) §"Late-bound fields"), and an
   encoding admits a record only where that merge succeeds.
6. **No false negatives**: what it admits on a changed table is what
   Iceberg's retry of the operation commits there, logically the same
   table.

Each encoding's test extends `ConflictEncodingContract` (S1, S2, the
version a merged record moves the table to, totality).
`TestRebaseProperties` is the oracle of 5 and 6: an operation runs after
a window of commits it did not see, under the encoding and under a
reference that conflicts on the whole version and lets Iceberg retry,
and the two must end alike; it was calibrated against mutants of the
encodings (`fuzz/mutants`). `TestConflictMatrix` holds the matrix below.

## Operation × Operation Conflict Matrix

The matrix below falls out of I2 (action versions are bumped on apply)
and the version-checking rules in [SPEC.md](SPEC.md) §"Action Verification
Rules". It is the *enforced* rule, backed by `TestProtoActions$ConflictTests`.

Each row is the **first** action applied (A); each column is a **second**
action (B) whose preconditions were captured against the pre-A state and
is applied to the post-A state. Cells hold:

- **✓** — the pair composes; B.verify succeeds.
- a field name — B.verify fails because A bumped that field.
- **—** — the pair is not meaningful (target doesn't exist).

Assumptions: same namespace or same table unless otherwise noted. Ops on
*different* namespaces/tables always compose. `CN↓` = create child under
the same parent.

|          | CN↓  | DN   | SP   | RP   | CT   | CTI  | DT    | UT    | RT'   | RT    |
|----------|------|------|------|------|------|------|-------|-------|-------|-------|
| **CN↓**  | ns v | ns v | ns v | ns v | ns v | ns v | ✓     | ✓     | ns v  | ✓     |
| **DN**   | —    | ns v | ns v | ns v | ns v | ns v | —     | —     | —     | —     |
| **SP**   | ns v | ns v | ns v | ns v | ns v | ns v | ✓     | ✓     | ns v  | ✓     |
| **RP**   | ns v | ns v | ns v | ns v | ns v | ns v | ✓     | ✓     | ns v  | ✓     |
| **CT**   | ns v | ns v | ns v | ns v | ns v | ns v | ✓     | ✓     | ns v  | ✓     |
| **CTI**  | ns v | ns v | ns v | ns v | ns v | ns v | ✓     | ✓     | ns v  | ✓     |
| **DT**   | ✓    | —    | ✓    | ✓    | ✓    | ✓    | —     | tbl v | tbl v | tbl v |
| **UT**   | ✓    | —    | ✓    | ✓    | ✓    | ✓    | tbl v | tbl v | tbl v | tbl v |
| **RT'**  | ns v | —    | ns v | ns v | ns v | ns v | tbl v | tbl v | tbl v | tbl v |
| **RT**   | ✓    | ✓    | ✓    | ✓    | ✓    | ✓    | tbl v | tbl v | tbl v | ✓     |

Legend: **CN↓** `CreateNamespace` (child), **DN** `DropNamespace`,
**SP** `SetNamespaceProperty`, **RP** `RemoveNamespaceProperty`,
**CT** `CreateTable`, **CTI** `CreateTableInline`, **DT** `DropTable`,
**UT** `UpdateTableLocation` / `UpdateTableInline`,
**RT'** `RenameTable` (in-place; bumps both source and destination ns),
**RT** `ReadTable`. "ns v" = parent namespace's version conflicts;
"tbl v" = table version conflicts. A `UT` or `RT` cell of the same table
is the `lamport` encoding's; the others relax `UT` × `UT` (below).

What the matrix says:

1. All ns-level mutations under the same parent serialize on `ns v`.
2. `DT` and `UT` on the *same* table serialize on `tbl v`. Neither
   conflicts with unrelated ns-level ops except through the parent's
   children-set rule.
3. `DT` and `UT` on *different* tables in the same ns compose. Bumping
   `ns v` on `CT` / `DT` protects ns-level ops; it doesn't pessimize
   unrelated table ops, because those carry table-level preconditions
   that are more precise.
4. `ReadTable` is the read side of optimistic concurrency — it conflicts
   with any update to the same table, nothing else.
5. The root is a parent like any other: an ordinary namespace row with
   id 0, so top-level creates and drops serialize on its version. It
   cannot be named, created, or dropped (SPEC.md §State Model).
6. A conflict fails the record, not the operation: Iceberg re-applies a
   table commit on a fresh read, and FileIOCatalog re-applies a catalog
   operation -- a namespace create, drop or property change, a table
   create, rename or drop -- which depends on nothing a concurrent change
   alters but existence, checked again on the fresh read (Bugs 47, 53,
   54). A catalog transaction is the exception: it conflicts on the
   whole version of each table it writes, as it first read it, or reads
   under serializable isolation, and one that conflicts fails, for its
   caller to run again ([errata.md](errata.md) §D12).

### The same table, per conflict encoding

Two updates of one table (`UT` × `UT`) conflict under `lamport` whatever
they change, and Iceberg retries the second on a fresh read. The other
encodings merge the second onto the first where Iceberg's retry would
commit the same (I12). The rows are commits W and R, where R's writer
read the table just before W landed; `TestConflictMatrix` runs each row
under each encoding against Iceberg's retry, on a v2 table partitioned
by one column, inline TM+ML. "Overwrite" is a copy-on-write overwrite of
a partition under serializable isolation (Spark's), which validates that
no data or deletes were added in it.

| W, then R | `lamport` | `lamport-minus-properties` | `concern-vector` | `blind-append` | `partition-vector` | `manifest-window` |
|---|---|---|---|---|---|---|
| property change, append | ↻ | ✓ | ✓ | ✓ | ✓ | ✓ |
| append, property change | ↻ | ↻ | ✓ | ✓ | ✓ | ✓ |
| sort-order change, append | ↻ | ↻ | ✓ | ✓ | ✓ | ✓ |
| statistics, append | ↻ | ↻ | ✓ | ✓ | ✓ | ✓ |
| append to a branch, append to main | ↻ | ↻ | ✓ | ✓ | ✓ | ✓ |
| append, append (another partition or the same) | ↻ | ↻ | ↻ | ✓ | ✓ | ✓ |
| append, merge append | ↻ | ↻ | ↻ | ✓ | ✓ | ✓ |
| append, overwrite of another partition | ↻ | ↻ | ↻ | ↻ | ✓ | ✓ |
| append, delete by filter in another partition | ↻ | ↻ | ↻ | ↻ | ✓ | ✓ |
| append, compaction (another partition or the same) | ↻ | ↻ | ↻ | ↻ | ✓ | ✓ |
| append to a branch, overwrite on main of the same partition | ↻ | ↻ | ✓ | ✓ | ↻ | ✓ |
| schema change, append | ↻ | ↻ | ↻ | ✓ | ✓ | ✓ |
| schema change, overwrite | ↻ | ↻ | ↻ | ↻ | ↻ | ↻ |
| partition-spec change, append | ↻ | ↻ | ↻ | ↻ | ↻ | ↻ |
| snapshot expiry, append | ↻ | ↻ | ↻ | ↻ | ↻ | ↻ |
| rollback, append | ↻ | ↻ | ↻ | ↻ | ↻ | ↻ |
| append, overwrite of its partition | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |
| append, schema change | once | once | once | once | once | once |

Legend: **✓** R merges onto W, in one commit attempt; **↻** R's record
conflicts and Iceberg's retry commits it (a false positive, which costs
an attempt: a read, a staging and a write); **✗** R conflicts and the
retry fails its validation (a real conflict); **once** R conflicts and
fails, since Iceberg commits a schema, spec or ref update once, on the
version it read.

What the per-encoding matrix says:

1. No encoding admits what the retry would not commit (no ✓ where
   `lamport` has ✗ or once), and none turns a real conflict into a merge.
2. Each encoding, in the order listed, merges more of these rows than
   the one before it, but not always the same ones and more:
   `partition-vector` stamps the files of every branch, and judges a
   validated record by its partitions alone, so it misses what
   `concern-vector` sees: that main did not move. `manifest-window` reads
   the branch's own history and sees both.
3. The guards stay whole-table under every encoding: a spec change, a
   snapshot expiry, a rollback, and (for a record that validates) a
   schema change.
4. Buckets can collide: ref names hash into 16 buckets and partitions
   into 64, so two branches or partitions that share a bucket conflict
   (a false positive, never a false merge).
5. A catalog transaction pins every table it writes (D12), and conflicts
   under every encoding as under `lamport`.

## Late-Binding

Writers never allocate entity IDs. A create action carries a negative
late-bound ID, and replay binds it to a real ID:

- `CatalogMutation.intention()` sorts new namespaces by depth (parents
  before children) and numbers new namespaces and tables `-1, -2, ...`
  (separate sequences). Actions later in the same transaction reference
  a new entity by its late-bound ID.
- Version fields are set to `-1` when the referenced entity was created
  in the same txn.
- On replay, `CatalogIntention.apply` binds each late-bound ID, at its
  defining create, to the replaying state's next namespace / table ID
  (`ProtoCodec.IdBinding`), and resolves later references to it. Every
  reader replays the log in the same order, so every reader assigns the
  same IDs.
- `CatalogIntention.verify` rejects a record that references a late-bound
  ID before (or without) the create that defines it, or whose create
  names a concrete ID ([SPEC.md](SPEC.md) §"Late-Binding").

Binding at replay is what keeps concurrent creates apart. When writers
allocated IDs from their own snapshot, two writers that read the same
snapshot and created unrelated entities -- e.g. tables in different
namespaces -- wrote records with the same ID. Both records verified, and replay let the second overwrite the
first.

## Compaction Trigger

Compaction happens at write time when the log's observable state
crosses either threshold (`CompactionPolicy.limits`):

- the read the commit staged on holds at least `max.append.count`
  intention records after its checkpoint (GC records not counted), or
- the record would take the log past `max.append.size` bytes.

No on-disk hint is needed. Atomic append + atomic CAS at the storage
layer are sufficient to linearize commits, and either writer in a race
will independently decide to compact based on the live file's length
and the record count of the last version it read. After a lost append
race the loop resends without reading the file again, so its count is
a lower bound; the count limit is soft. Field 2 of `Intention` (formerly
a `sealed` bool flag) is reserved. On a shared log with checkpoints
apart, a writer whose commit landed checks the same thresholds against
the log it landed in, and compacts after the commit
([LOG.md](LOG.md) §"Compaction").

## Best Practices

### Adding a new catalog action

1. Add the message to `catalog.proto`, a new variant in the
   `Action.action` oneof, and its field-number constants in `ProtoCodec`.
   Regenerate the format explorer's fixtures (`mvn test
   -Dtest=FormatExplorerFixtures -Dexplorer.regenerate=true`).
   `TestFormatExplorer` fails until the constants match `catalog.proto`
   and the fixtures are current.
2. Implement `ProtoCodec.Action` (an interface: `verify`, `apply`,
   `bind`) and add a branch for it to `encodeAction` and `decodeAction`.
3. An action that references a namespace or table id must implement
   `bind(IdBinding)`, defining the late-bound ids it creates and
   resolving the ones it references (808bca7). The default `bind`
   returns the action unchanged: its late-bound ids would reach `apply`
   unbound, and `CatalogIntention.verify`, which rejects a record whose
   reference precedes its create when `bind` throws, would not see
   them. Add it to `CatalogIntention.selfConsistent` too, so a record that
   also drops what it references is rejected (Bug 14).
4. `apply` must bump the version of every entity whose state it mutates,
   including the parent namespace if it changes the namespace's
   children set (I2).
5. Add an `apply()` row to the conflict matrix above and one test in
   `TestProtoActions$ConflictTests` per cell.
6. Add an idempotency test to `TestProtoActions$IdempotencyTests`:
   replaying the action with a fresh UUID against the post-apply state
   must be rejected.

### Adding a new inline TM delta type

1. Add the message to `catalog.proto` under `TableMetadataUpdate.update`
   and its field-number constants in `InlineDeltaCodec`, and regenerate
   the explorer's fixtures, as for a catalog action.
2. Implement `InlineDeltaCodec.DeltaUpdate` (an interface with
   `applyTo(TableMetadata.Builder)`), and add a branch for it to the
   static `encodeUpdate` and `decodeUpdate` dispatchers. An update that
   references inline state such as the manifest pool is routed in
   `applyDeltaWithManifests` instead of applied to the builder.
3. Update `computeDelta` to emit the new update when relevant; the diff
   should be minimal (only emit when the field differs). It must refuse
   (return null) any change it cannot express (I7);
   `TestInlineDeltaProperties` checks that every delta replays to the
   committed metadata.
4. **Update ordering** matters when a Builder method rejects the
   operation against the current state (e.g. `removeSchemas` rejects
   removing the current schema id). Emit `Add → SetCurrent / SetDefault →
   Remove` so the pointer advances before the prune.
5. **Determinism check.** If the update's `applyTo` reads
   `Builder.lastUpdatedMillis` to stamp a derived field (snapshot-log
   entry, etc.), make sure the rebuild loop in `applyDeltaWithManifests`
   pins that value before `applyTo` runs. See I4.
6. Add unit tests in `TestInlineDelta`: encode/decode round-trip,
   apply against a fresh `TableMetadata`, idempotency on replay, the
   same metadata from repeated replays of the same delta (I4).
7. If the new update type interacts with manifest-pool state, also extend
   the routing in `applyDeltaWithManifests` (see SPEC_TM.md and SPEC_ML.md).
8. A delta holding the new update replays through the builder
   (`NativeDelta.applies` is false), which is correct but slower. To
   replay it on the table's fields, extend `NativeDelta` to reproduce
   what the builder does with it; the suites' replay check compares the
   two on every apply (§"Replay State").
9. Name the concern it changes in `TableChange.of`, which the conflict
   encodings read and stamp by; or set `commitOnce` for it, which gives
   its record the Lamport witness (I12). `TableChange.of` has no default
   branch: an update it does not name changes no concern, and an
   encoding would merge across it.

### Choosing append vs CAS

The default `max.append.count` is 100. A reader replays every record
after the checkpoint, and every commit reads, so a long tail costs every
writer; a compaction rewrites the checkpoint and races the others'
appends, so a short one costs compactions. In the contention benchmark
(fast appends on S3 Express, lab/ `c4-knobs`), 100 beat both a replace
on every commit and 1000, under `lamport` and `blind-append`. It was
10000, sized to the stores' append caps (S3 Express 10,000 parts, Azure
Append Blob 50,000 blocks), which only bound it: a count above a store's
cap costs a refused append, then a compaction (Bug 66). The best count
depends on the store's latencies and the catalog's size, so it should be
measured per store or set by a principled rule ([errata.md](errata.md)
§D14). GCS has no append, and the format commits every write there by
CAS whatever the setting (`GCSFileIO.supportsAppend()` is false). For S3
standard buckets, which support conditional full-object replace but not
conditional append, set `max.append.count = 0` to force CAS-only mode.

A writer that loses a race for the object (another writer's append or
replace landed first) waits before it reads again and resends: a time
drawn below a bound that starts at `fileio.catalog.race.retry.min-wait-ms`
(default 100 ms, the best of none, 20 and 100 ms in the contention benchmark's
`c7-backoff` under `manifest-window`) and doubles with each race the commit loses, up to 1 s.
Without it the writers who lost to one winner all read and resend at
once, and on a store that only replaces, most lose again, each loss a
read of the whole object: at 32 writers on an S3 general-purpose bucket,
a committed operation lost 13.7 races on average.

The default `max.append.size = 16 MiB` is a soft target. Larger catalogs
mean longer reads on every load; tune down for read-heavy workloads, up
for write-heavy.

### Inline TM only vs TM+ML

- `inline=false` works on every storage provider; baseline behaviour with
  external `metadata.json` and `snap-*.avro` files.
- `inline=true, inline.manifests=false` saves the metadata-file write
  but keeps Avro manifest lists. Replay needs the fork's
  `TableMetadata.Builder.setLastUpdatedMillis` (I4).
- `inline=true, inline.manifests=true` saves both writes; it also needs
  the fork's `ManifestListSink`, `InlineSnapshot` and
  `TableMetadata.Builder.replaceSnapshots`.

Every mode needs the Iceberg fork for `SupportsAtomicOperations`. The
fork's `core-fixes` branch also fixes the Iceberg core issues in
[ICEBERG_ISSUES.md](ICEBERG_ISSUES.md), which come back on stock
Iceberg. See the [README](../README.md) for fork dependencies.

### Choosing the inline-TM codec

`fileio.catalog.inline.tm.codec` selects between two encoders for
inline `TableMetadata` bytes; decode always follows the on-wire tag, so
catalogs can carry a mix produced by different writers.

- `structural` (default) — purpose-built binary-columnar encoder. 40%
  smaller at the corpus median, 4–7× faster on lazy-decode workloads
  (current-snapshot lookups, snapshot-history scans). Recommended for
  production.
- `gzip` — Iceberg-style `Codec.GZIP` JSON. Useful when an external
  tool needs to read inline TM bytes by gunzipping them as
  `metadata.json.gz`, or as a diagnostic fallback. Larger and slower
  than `structural`; otherwise functionally equivalent.

## Hand-Rolled Wire Format

`ProtoCodec` and `InlineDeltaCodec` implement protobuf wire format
manually rather than using `protoc`-generated classes: no runtime
`protobuf-java` dependency or build-time `protoc`, and latitude for
lazy scanning (see the header of `catalog.proto`). The wire format is
still standard protobuf (field tags, varints, length-delimited
messages), and `catalog.proto` declares every field the codecs write:
`CatalogProto` and `TestFormatExplorer` check that in every `mvn test`
(see §"Codec Layout").

The trade-off: when adding a new field number, both `encode` and `decode`
need explicit handling, and `decodeXxx` must include a default case that
calls `skipField(wireType)` to preserve forward compatibility. Generated
classes handle this for you; manual codecs require it as a discipline.

## Test Layout

| File | What it covers |
|------|----------------|
| `TestProtoCatalogFormat` | PB codec, replay of catalog files, reader-version gates |
| `TestProtoActions` | Per-action positive/negative tests, conflict matrix, idempotency, randomized tests, inline tables, manifest-pool round-trip |
| `TestProtoCommitKnobs` | `max.append.count` / `max.append.size` thresholds; `MockIO` |
| `TestInlineDelta` | Inline TM delta encode/decode/apply, `computeDelta`, ML delta types |
| `TestInlineManifestEndToEnd` | End-to-end commit→reload across BASELINE / TM_ONLY / TM_ML configs; per-`SnapshotProducer` operation coverage; codec-axis subclass parameterized over `gzip` / `structural` |
| `CatalogTests`, `CatalogTransactionTests` | Abstract suites consumed from `iceberg-core-tests.jar` (commit `35076d6`) and run by the local and cloud subclasses |
| `TestLocalCatalog`, `TestLocalCatalogTransaction` | Both abstract suites in every `LocalCatalogMode`: append / CAS / compact × pointer / inline TM / inline ML over `InMemoryAtomicFileIO`, and the `MEMLOG*` modes on an `InMemorySharedLog` |
| `TestCatalogFaultProperties` | jqwik: several writers run Iceberg operations through the catalog on faulty storage, in mixed modes, on one object or a shared log; the catalog must match a model after every operation (I3a, I6, I10, I11) |
| `TestCatalogRaceProperties` | jqwik: a catalog operation raced by others must end as a serial run (I9), on one object or a shared log |
| `TestInlineDeltaProperties` | jqwik: each commit's delta replays to the committed metadata, and each codec stores every version exactly (I7) |
| `TestInlineDifferentialProperties` | jqwik: the same schedule on a pointer catalog and an inline one commits alike and reads the same (I8) |
| `Test*Encoding` (`ConflictEncodingContract`), `TestRebaseProperties`, `TestConflictMatrix` | The conflict encodings: each one's contract, the oracle against Iceberg's retry, and the per-encoding matrix (I12) |
| `TestCatalogRegressions` | One nested group per catalog bug, with the smallest scenario that reproduces it |
| `TestCommittedSetGc`, `TestCommittedTransactions`, `TestUuidV7` (intentlog-core) | Committed-set encoding, GC merge and horizon, tri-state lookup ([LOG.md](LOG.md) "The committed set and GC"); UUIDv7 minting |
| `TestReplay`, `TestLogRuntime` (intentlog-core) | Replay, and the commit loop branch by branch on a scripted log laid out as one object, each test named for the TLA+ action it drives |
| `TestSharedLogRuntime` (intentlog-memory) | The commit loop on a shared log with checkpoints apart: the append branch, creation, compaction, refresh, and the schedules the shared-log model found (`dup`, `lagread`, `p3`), with copies of records that must never apply again |
| `TestAtomicFileLog`, `TestAtomicFileLogContract`, `TestInMemorySharedLogContract` | Each provider's layout and signals, and `SharedLogContract` (P1, P3-P5, I11) on each |
| `TestMemoryCommitProtocol`, `TestAtomicFileCommitProtocol` | jqwik: the testkit's `CommitProtocol` on each provider, over `EntityVersionsMachine`: faults, compactors and GC records inside commits, stale and P3-breaking reads, delayed appends, and copies of any record that ever landed (I2, I10, I11) |
| `TestIcebergCoreIssues` | Canaries asserting each Iceberg core issue C1-C27 ([ICEBERG_ISSUES.md](ICEBERG_ISSUES.md)) is absent |
| `TestSerializableInlineTable` | A table shipped to a worker as Spark and Flink ship one (`SerializableTable`, by Java serialization and Kryo), in pointer and inline modes: the copy, and a copy of each metadata table, reads as the table |
| `TestWindow`, `TestInlineSnapshotReuse` | Replay's shared lists: a `Window` against `TableMetadata.Builder`'s trim of the metadata log, appending to older lists and changing bounds; a load's reuse of the `InlineSnapshot`s earlier loads made |
| `CheckpointRoundTrip` | Not a test class: checks a catalog file against `catalog.proto` and its replayed state against a checkpoint of it (I6), and a compaction's checkpoint against a replay of what it replaced (I11); the local suites and the fault and multi-writer fuzzers run it on every write and every stored checkpoint, and other suites use it too |
| `TestFormatExplorer` | Codec field-number constants against `catalog.proto` both ways; explorer fixtures current |
| `spec/` (`TestCodecProperties`, `TestInvariantProperties`, `TestCanonicalStateProperty`, `TestMultiWriterProperties`) | jqwik at the format layer: codec round trip and determinism, well-formedness, builder state through the codec, and a multi-writer fuzzer of the commit protocol on one object and on a shared log, with copies of records and late landings |
| `GCSCatalogTest`, `TestS3Catalog`, `ADLSCatalogTest` | Cloud integration tests, pointer mode; append mode on S3 and ADLS, CAS on GCS (require credentials/emulators) |
| `*CAS`, `*InlineTM`, `*InlineML` subclasses | Cloud matrix subclasses overriding `maxAppendCount()` / `inlineTM()` / `inlineML()` -- see [COMPAT.md](COMPAT.md) for status |

The end-to-end suite uses an in-memory `MemoryFileIO`, and the local
suites and fuzzers `InMemoryAtomicFileIO`, which enforces conditional
CAS and append, or `InMemorySharedLog`, so commit→reload cycles run
without storage. Cloud tests
verify that the actual provider APIs honour `SupportsAtomicOperations`
semantics.
