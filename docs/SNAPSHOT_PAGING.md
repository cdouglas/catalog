# Paging a table's snapshots (design)

Status: proposed 2026-11-04, revised 2026-11-05 after review; steps 1-3
built (2026-11-05, measured in REPLAY_BENCH_RESULTS.md; step 3's worker copies
move to step 4); step 4 built and measured; step 5 built (2026-11-06), with
which paging is on by default (block 256, window 1,024); step 6 deferred
(2026-11-06).
Errata D18 (ML lists stored whole) tracks the problem; D17 (expiry starves)
and D19 (whole-object compaction trigger) are what it costs. It is R2.1 and
part of R2.2 in [CHECKPOINT_PLAN.md](CHECKPOINT_PLAN.md), under its
decisions: replay never reads a page (1), page GC (2), and the pool as a
dedup dictionary (3). The committed set pages apart
([COMMITTED_PAGING.md](COMMITTED_PAGING.md)), since its ids differ from
snapshots in lifetime and key.

## Context

An inline table lives whole in the checkpoint (`InlineTable`, catalog.proto):

- **Its metadata (TM).** The structural codec's bytes hold a JSON head (schemas,
  specs, sort orders, properties, refs, statistics and the snapshot log), every
  snapshot in one columnar block, and the metadata log, bounded by
  `write.metadata.previous-versions-max`. The snapshot block cannot be decoded
  one snapshot at a time.
- **Its manifest lists (ML).** The pool holds each manifest's entry once; each
  snapshot's list was written whole, as indices into the pool sorted by path,
  so one new manifest renumbered every list that named a later path (until
  step 2, below).

So the checkpoint grows with every snapshot a table keeps:

| Measured | Per snapshot | Source |
|---|---|---|
| inline TM, structural | about 65-73 B | FORMAT.md; TM_ENCODING_BENCH_RESULTS.md |
| lists, merge appends | about 137 B, plus 59 B per manifest in the pool | FORMAT.md (ingest capture) |
| lists, fast appends | grows with the snapshots: 12.8 MB at 3,000 | REPLAY_BENCH_RESULTS.md |
| an ML table, merge appends | about 330 B: 2.4 MB at 7,200 | catalog-bench E3 (D19) |

Every compaction writes all of it and every fresh read fetches it (D15, D19).
Replay leaves a table it does not touch as bytes, but a load decodes the
table's whole snapshot block and builds every snapshot and every snapshot's
list: 664 ms and 533 MB allocated at 3,000 fast-append snapshots, whose lists
hold 4.5 million entries (later loads reuse what the first made,
`LogCatalog.InlineSnapshots`).

Iceberg is eager in snapshots and lazy in lists. Reading a metadata file
parses every snapshot in it, on every commit and load, and on a worker too:
`SerializableTable` ships the file's location and parses the file on first
use, even for the current snapshot. Only REST's
`snapshot-loading-mode=refs` sends just the refs' snapshots and loads the
rest on demand ([Loads](#loads)). A snapshot's manifest list, though, is
read only when something asks for its manifests, one snapshot at a time.
The lists are where this catalog is eager beyond Iceberg:
`InlineTableMetadata.create` takes every snapshot, and each `InlineSnapshot`
holds its list resolved. The fork's `SerializableTable` is eager too: for an
inline table it ships the whole metadata as JSON and a copy of every
snapshot's manifests. None of this needs pages to fix ([Order of
work](#order-of-work), step 3): lists resolved when first asked for, as
Iceberg reads list files; snapshots through a supplier, decoded by block;
and a `SerializableTable` that ships the head.

Most of it is history that replay never needs. A commit lands on its branch's
head, and its conflict window reaches back over the commits on that branch it
did not see. Old snapshots matter to loads that ask for history or time
travel, to expiry, and to a ref moved back onto one. This design keeps a
table's recent snapshots inline and moves older ones, with their lists, into
immutable pages the checkpoint names. Replay never reads a page. A load reads
one only when Iceberg asks for history.

## What replay reads of a table's history

Each read replay makes of a table's history today, and where it comes from
once old snapshots are paged:

| Replay reads | For | Reach | After paging |
|---|---|---|---|
| every snapshot id | a new snapshot's id is unique (NativeDelta, Landing, the builder) | all | the root keeps the ids of sealed snapshots ([The root](#the-root-and-the-pages)) |
| a ref's target | it exists (rollback, set ref, tag, fast-forward cherry-pick) | any snapshot | a ref's target is always inline; a record that moves a ref onto a sealed snapshot carries it ([Bringing a sealed snapshot back](#bringing-a-sealed-snapshot-back)) |
| the parent's list | a new snapshot's list is its parent's, changed | the writer's parent | inline, within W or held by a ref (a branch's head); a sealed parent conflicts |
| the parent's summary | `inherits_parent_extra` | the writer's parent | as above |
| the head, its list and pool entries | a merge onto a changed table (Landing) | the branch's head | inline: a ref holds it |
| the window | manifest-window: the snapshots from the head back to the record's parent, at most 1,000 | the commits on the branch since the writer's base | inline within W; a window that reaches a sealed snapshot conflicts, as one that reaches a missing snapshot does today |
| the pool, by path | the dedup (decision 3) and `pool: restates a pooled manifest` | every pooled path | the root's pool covers the inline lists; a block keeps its own |
| the whole snapshot log | the log rebuilt when one delta moves main through several snapshots | all | the log stays inline, whole |
| everything | the builder path (updates other than snapshots, refs and properties), FULL mode, expiry | all | snapshot updates apply natively; the builder sees the head without snapshots; FULL carries the head; expiry is native ([Expiry](#expiry)) |

Scalars need no history: the last sequence number, the next row id, Landing's
shifts, partition-vector's buckets, and the concern encodings' stamps.

Three of these rows are changes to replay that do not depend on pages, and
come first ([Order of work](#order-of-work)):

- **Snapshot updates apply natively, always.** NativeDelta applies a delta of
  snapshots, refs and properties today; every other delta goes through
  Iceberg's builder on a `TableMetadata` of the whole table. Splitting each
  delta lets the builder see a head with no snapshots: the snapshot updates
  (add, remove, set and remove refs, the snapshot log, statistics keyed by
  snapshot) apply natively, and the rest (schemas, specs, sort orders,
  location, format version, encryption keys) go through the builder on the
  head. The builder must not see a partial snapshot list: `updateSnapshotLog`
  silently drops the log entries of snapshots it cannot see. The replay check
  (`fileio.catalog.replay.check`) compares the split with the builder on the
  whole table, as it does each native apply now.
- **Expiry applies natively** (decision 1).
- **FULL mode carries the head**, not the whole table.

## Blocks

**The grid.** A table's snapshots are cut into blocks by sequence number:
block k holds the snapshots with sequence numbers in [k·B, (k+1)·B).

**Sealing.** Block k is sealed once the table's last sequence number reaches
(k+1)·B + W. Replay checks after each record it applies, so a record whose
parent is in the block lands before the seal its own snapshot causes. The
block's content is the snapshots in its range that exist at that record and
that no ref targets.

- *A function of the state.* Every replica seals the same block at the same
  record, so every checkpoint is the replay of the log before it (I11). No
  record is needed and no clock is read. The committed set's floor needs a
  PageOut record because its blocks fill by time, which a compactor's clock
  judges. A table's sequence numbers are state.
- *Sealed blocks never change.* A new snapshot's sequence number is above the
  table's last, so a block gains nothing once its range is behind. A merge
  (Landing) shifts the new snapshot's number, never an old one's (I3a).
  Expiry removes ids from the root's live set; the page is never rewritten
  ([Expiry](#expiry)).
- *Ref targets stay inline.* A snapshot that a ref targets when its block
  seals is left out of the block and stays inline: a tag holds its snapshot
  in the root for as long as the tag lives. Iceberg also requires it: a
  `TableMetadata` whose snapshots load lazily must still hold every ref's
  target (`validateRefs`). A snapshot left inline this way stays inline after
  its ref moves on, until it expires. Such stragglers number at most the ref
  moves off an old snapshot, which are rare.
- *v1 tables do not page.* Their snapshots all have sequence number 0.

**B and W** are fixed by the catalog's first record, as the conflict encoding
is. A setting that writers chose could let two compactors at one position
write different roots. Defaults, both provisional and swept in experiments:

- **W = 1,024** sequence numbers inline behind the head. A commit whose
  window reaches a sealed snapshot conflicts and retries on a fresh base, so W
  should cover the windows manifest-window merges today: at most 1,000
  commits on the record's branch (`MAX_WINDOW`). The two count differently.
  Every commit to the table, on any branch, takes a sequence number; the
  window counts only commits on the record's own branch. On a table whose
  only active branch is main they agree. Where other branches commit in
  between (write-audit-publish, an audit branch), a writer fewer than 1,000
  commits behind on main can find its base sealed, and retries where today it
  would merge. Such a writer is already far behind, so the retry is accepted
  rather than counting W per branch. Smaller W pages more and turns more
  stale merges into retries.
- **B = 256** snapshots a block. A page is then tens of KB to a few hundred
  (snapshots, lists and the pool entries they first name), a GET dominated by
  latency. Each block costs the root a reference.

## The root and the pages

An inline table in the root keeps:

- **the head**: the JSON head (with the whole snapshot log), the inline
  snapshots, their lists, the pool entries they name, and the metadata log;
- **for each sealed block**: its index, the Merkle root of its snapshots
  ([below](#bringing-a-sealed-snapshot-back)), and its live ids: the sealed
  snapshots that have not expired, sorted, 8 B each;
- the row, prefix, codec and conflict state, as now.

A sealed snapshot costs the root its id (8 B) and its snapshot log entry
(about 10 B, if main ever pointed to it). The live ids serve replay's
uniqueness check exactly, and tell a load which of a page's snapshots still
exist.

The snapshot log stays whole because Iceberg cannot load it lazily. The
`TableMetadata` constructor checks the log's order, and
`snapshot-loading-mode=refs` in the REST protocol likewise sends the whole log
with only the refs' snapshots.

**A page** holds one sealed block:

```protobuf
message SnapshotPage {
  int32  min_reader_version = 1;
  bytes  log_id             = 2;
  int64  table_id           = 3;
  uint64 block              = 4;   // k: sequence numbers [k·B, (k+1)·B)
  bytes  snapshots          = 5;   // the structural codec's snapshot block, these snapshots only
  repeated ManifestFileEntry pool = 6;   // the entries the block's lists name, by ordinal
  repeated ListDelta lists  = 7;   // per snapshot, sequence order
}
```

- **Name.** `t<table id>/<block>-<digest hex>`, placed by the provider's page
  store (the `PageStore` SPI that COMMITTED_PAGING.md adds, under another
  prefix). Every compactor at one position seals the same blocks with the
  same bytes, so racing compactors' PUTs agree.
- **Written before the root that names it**, by the compactor that writes
  the root. A page put that fails fails the compaction before anything is
  sent.
- **Its pool** copies the entries its lists name from the table's pool.
  Decision 3 makes that safe: a pooled path's entry never changes, so a copy
  never goes stale.

## Lists: stable ordinals and deltas

Paging takes old lists out of the checkpoint. The lists left inline still
repeat themselves, and under fast appends each holds every manifest the table
has (D18). R2.1 changes how lists are encoded, with or without pages:

- **Ordinals, not positions.** A manifest's ordinal is assigned once, when
  the table first pools it, from a counter in the table's row. A list names
  ordinals, so a new manifest renumbers nothing.
- **A list is its parent's, changed.** A snapshot's list is written as its
  parent's list plus the manifests it adds, minus those it removes, by
  position in the parent's list (as `RemoveManifestDelta` already is on the
  wire). A list is whole where its parent is not in the same block or
  inline, and every 64 snapshots along a chain (provisional), so that
  decoding one list never walks far.

The checkpoint then grows with the snapshots plus the manifests, not their
product. That fixes fast appends' square growth without any page. The head's
own list still names every manifest a table without merges has, as its
manifest list file would.

## Bringing a sealed snapshot back

A ref moved onto a sealed snapshot needs that snapshot inline: its target
must be (above), and the next commit on that branch builds on its list.
Rollbacks, `setCurrentSnapshot`, tags and branches created on old snapshots,
and fast-forward cherry-picks of old staged snapshots all do this.

The record carries the snapshot, and replay checks it against the root
without reading the page:

- **A leaf** is a snapshot's canonical encoding, standalone, with its list
  resolved to manifest entries. A block's Merkle root covers the hashes of
  its leaves in sequence order (SHA-256, truncated to 16 bytes, as the
  committed set's digests are).
- **The writer** has the snapshot from the pages, which its load read when
  Iceberg checked the target (writers may read pages). Its record carries the
  leaf and the sibling hashes up to the block's root: 8 hashes at B = 256.
- **Replay** checks that the id is in the block's live ids and that the leaf
  hashes up to the block's Merkle root. It then moves the snapshot inline,
  removing its id from the block's live ids, and applies the ref change. A
  record whose proof fails, or whose snapshot has expired since its writer
  read it, does not apply (`sealed: ...`), and its writer retries on a fresh
  read.

Any other record that needs a sealed snapshot's content conflicts with the
same reason: a commit whose parent is sealed (a stale writer, a stale
branch), or a window that reaches one. This is stable (S2): blocks only
become sealed, so a record that conflicts where it lands would conflict
anywhere later.

## Expiry

Expiry is maintenance an operator runs, not a side effect of commits:
Iceberg expires nothing on its own. A job (Spark's `expire_snapshots`, a
scheduled service) commits a removal by the table's retention, by default
the snapshots older than 5 days (`history.expire.max-snapshot-age-ms`) past
the newest of each branch (`history.expire.min-snapshots-to-keep`, 1), then
deletes the files only the removed snapshots reached. It is a writer's
change, not a hint: once it lands, no reader may load those snapshots, and
their files may be gone. How the catalog keeps the removed metadata is its
own affair. Here a removed snapshot leaves the table at once (the root's
live ids), and its bytes stay in its page, which nothing reads for it, until
every snapshot of its block is gone and the page is collected. Expiry is by
snapshot; reclamation is by block.

Expiry applies natively (decision 1). A `RemoveSnapshots` record:

1. removes each id from the inline snapshots, or from its block's live ids;
2. drops every ref that targets a removed snapshot, as the fork's builder
   does;
3. rewrites the snapshot log as Iceberg's `updateSnapshotLog` does: an entry
   whose snapshot is gone clears the history before it. The log is inline,
   so this needs no page;
4. drops the statistics files of removed snapshots;
5. drops from the inline pool the manifests no inline list names
   (`removeSnapshotManifests`, over the inline lists only).

A block whose live ids empty drops from the root, and its page becomes
garbage. Pages are never rewritten. Expiry by age or count removes the oldest
snapshots, so it empties the oldest blocks whole. A block that keeps a few
live snapshots (a branch retaining its own ancestors, or an expiry's cutoff
inside the block) keeps its page, and the root keeps only those snapshots'
ids. Rewriting a block to drop its dead snapshots would save bytes at rest,
not in the root ([Alternatives](#alternatives-considered)).

Admission is unchanged: an expiry applies only on its base version (Landing),
which D17 is about. The writer still reads every page: Iceberg's expiry
reads all snapshots to choose what to remove and which files to delete.

## Loads

A block is the unit of decoding as well as of paging. The inline snapshots
are encoded by block too, on the same grid, so a reader decodes a fraction of
a table, the blocks it needs, whether they sit in the root or in pages.

- **Replay** decodes a table's head when a record first touches the table
  (inline tables stay bytes until then, as now), and a block only when a
  record reaches into it: almost always the newest. Once snapshot updates
  apply natively (step 1), what replay validates needs no more of a table's
  history than its snapshot ids, its refs' targets, the window behind a stale
  record, and scalars. A service that follows the log can keep its inner
  loop on the newest snapshots, and rebuild a table's metadata, its history
  included, in the background.
- **A load** builds the head's `TableMetadata` with the snapshots its refs
  target (Iceberg requires them: `validateRefs`), the whole snapshot log, and
  a snapshots supplier (`setSnapshotsSupplier`, upstream since #6811). The
  supplier decodes the table's other blocks, from the root's bytes or from
  pages, and keeps the snapshots the live ids name. Pages are cached by name,
  since they never change. `InlineSnapshot`s are made as their blocks are
  decoded, each resolving its list only when asked for its manifests, as
  Iceberg reads a list file. A reader that plans a scan of the current
  snapshot or of a ref decodes the head and the blocks its refs' targets are
  in, and reads no page.

What triggers the supplier, as Iceberg stands:

- `snapshot(id)` for an id not yet loaded, and `snapshots()`: history, time
  travel by id, the snapshots metadata table, expiry;
- **`TableMetadata.buildFrom(base)`**, which copies `base.snapshots()`. Every
  commit calls it (`SnapshotProducer`), as do transactions, and
  `SnapshotProducer`'s probe that a new snapshot id is unused loads them too;
- `TableMetadataParser.toJson`: pointer-mode writes, and the fork's
  `SerializableTable`, which ships an inline table's metadata as JSON to
  workers.

`currentSnapshot()`, `ref()` and `history()` do not trigger it.

Iceberg's supplier is all or nothing: the first call for a snapshot not yet
loaded decodes every block and reads every page of the table. A time-travel
query to one old snapshot pays for all of them. Loading one block would take
a supplier per snapshot id, a change to Iceberg's API that this design does
not make.

As Iceberg stands, then, every commit, and every job that ships a table to
workers, loads every snapshot. Two fork changes, of the same kind as errata
S1's:

- **Commits stay lazy.** Upstream PR #18073 (open since 2026-09-12, by a
  committer; marked stale) makes a commit under REST's
  `snapshot-loading-mode=refs` load no snapshot it does not need: the
  builder and the id probes no longer force a load, and `refresh()` keeps the
  supplier (#17830). The fork follows it. The catalog's replay still checks
  a new snapshot's id against every id, sealed ones included.
- **`SerializableTable`** ships the head and a supplier that a worker can
  run (it decodes blocks and reads pages through the table's FileIO), not
  the whole table.

Decided in review (2026-11-05): the fork takes both, behind one switch for
ablation, `fileio.catalog.load.lazy` (default true; false loads every
snapshot and list and ships the whole table, as today). Both sit in Iceberg
core and in the catalog's load path, above the shared log, so every
provider gains alike. #18073 changes core and helps any client that loads
snapshots lazily (REST's refs mode), so it goes on the baselines' `perf`
branch too (catalog-bench DESIGN.md, "Baseline clients").

## Page GC

Pages are one-way (decision 2 for the state's pages). A page a compactor
sealed is garbage once:

- its block is behind the sealing line ((k+1)·B + W at or below the last
  sequence number) and the root has no entry for it: every one of the
  block's snapshots expired; or

- its table is gone (dropped, or evicted to a pointer), its id below the
  catalog's table counter and absent from the root.

An import page ([Re-inlining](#re-inlining)) is garbage once the root's
version of its table has passed the version the page was staged on, and the
root does not name it.

None of these can be undone. Sequence numbers and table versions only rise,
snapshots never come back, and table ids are not reused. So a sweep needs no
clock and no grace period. Any compactor may sweep: list the page store and
delete the garbage the root shows. A page whose block is not yet sealed in
the root (one a compactor ahead wrote) is not garbage.

- On a shared log with checkpoints apart, the sweep counts every checkpoint
  the provider may still serve, not only the newest.
- A reader holding metadata from before a block emptied, or from before its
  table was evicted, reads the snapshots it held from the table as it is
  now, less those expired since. That is a REST catalog's lazy load: its
  supplier loads the table as it is when asked, and Iceberg's
  `TableMetadata` keeps what the supplier gives of the snapshots it
  inherited and drops those past its last sequence number. A pointer
  handle holds them all; matching it would take a grace period, a clock in
  the sweep, and REST's semantics are enough (settled 2026-11-06). As
  built: a load whose page is missing reads the
  catalog again. If the table has moved past the version it read and no
  longer names the page, the page was collected, and the load answers with
  its version's inline snapshots and those of its sealed blocks' live ids
  that a load of the table now has (a snapshot never changes, wherever the
  table keeps it). Only a dropped table's are gone
  (`LogCatalog.PageCollectedException`, a `NotFoundException`). A commit
  staged on such a read fails as stale (`CommitFailedException`), which
  Iceberg's retry cures with a refresh. A missing page the table still
  names is a loss, never taken for collected (`SnapshotPages.MissingPage`).
  Found by fault rounds at B = 2, W = 1: a stale handle's schema update
  staged on such metadata (step 5, which first failed the load as not
  found), then, in round sp5-r0, stale handles' histories, which Iceberg
  and its callers read outside the commit retry (an expiry's choice of
  snapshot, every command of the differential test's): not found there
  was a failure where a pointer catalog succeeds. The differential test's
  stale steps pick among the snapshots the table still has, where both
  catalogs agree.
- A copy shipped to a worker reads its version's pages through the table's
  FileIO and has no catalog to ask: a page collected since fails there, as
  a lost one does. That takes a job outliving its table's move past the
  version it shipped, and a compaction after (open).

A table outlives its blocks. Its head is never sealed (a ref's target stays
inline, [Blocks](#blocks)), and Iceberg's expiry never removes it: main is
always retained, each retained branch keeps its newest
`min-snapshots-to-keep` snapshots (at least 1), and a snapshot a retained
ref targets cannot be expired (`RemoveSnapshots`). A table whose old blocks
have all emptied keeps its head and its last snapshots inline. It ends only
by a drop, and leaves inline storage only by eviction ([Whole
tables](#whole-tables)).

## Whole tables

Decision 1 also evicts a table untouched for a while to a pointer (a
metadata file and manifest list files). Eviction is a record
(`UpdateTableLocation`), so a record staged on the inline table before it
does not apply; the policy is the writer's: which tables, after how long.
The evicting writer reads the table's pages to write the old snapshots' list
files. Paging makes eviction matter less: an idle inline table costs the root
its head, its last W snapshots and its sealed ids. The policy is deferred
([Order of work](#order-of-work), step 6).

Re-inlining works as SPEC_TM.md has it: an inline writer's next commit to a
pointer table re-inlines it in full mode when the whole metadata fits in a
record; otherwise the table stays a pointer table and the commit writes
another metadata file. The snapshots keep the manifest list files they
have. A deep table's metadata does not fit in a record; with pages it comes
back at any depth ([Re-inlining](#re-inlining)). Only a policy that
re-inlines on its own (a compaction, or a reader moving a cold pointer table
back) is out of scope, as today.

## Re-inlining

Paging bounds what an inline table costs the root, whatever its depth: its
head, its last W snapshots and about 18 B a sealed snapshot. So a pointer
table of any depth can come back inline, as long as its history goes to
pages rather than into the record.

**Import pages.** The re-inlining writer has the pointer table's metadata
(its commit loaded it). It cuts the snapshots behind the sealing line into
blocks on the table's grid and puts each as a page before it sends its
record, as a compactor puts a page before its root. An import page is a
`SnapshotPage` whose snapshots keep the manifest list files they have (a
snapshot's list location is part of the snapshot; the page has no pool or
lists). The record is FULL, on the table's version: the head, the inline
snapshots, and for each block its entry (`imported_blocks`: index, live
ids, Merkle root, digest, leaf count, version), from which the page's name
follows. That is about 18 B a snapshot and 50 B a block, 0.8 MB at
43,200 snapshots, within one record. Replay installs the blocks as sealed
and reads no page.

- *The writer's word.* Replay cannot check an import page against the ids
  and Merkle root its record carries, just as it cannot check a pointer
  table's metadata file against the location a record names. A compactor's
  page is a function of the replayed state; an import page is the writer's,
  named by its digest. A later record that brings one of its snapshots back
  is checked against the importer's Merkle root, so loads and bring-backs
  agree.
- *Garbage.* An import page is named, as a sealed page is, for the table
  version the record makes (one past the version it was staged on):
  `snap.<table>.<version>.<block>.<digest>`. The record applies only on the
  version it read, so once the table's version has reached the page's and
  the root does not name the page, nothing ever will ([Page GC](#page-gc)). A
  crashed import's pages wait for the table's next commit; no clock is
  read.
- *Policy.* As today, an inline writer re-inlines a pointer table on its
  next commit to it (SPEC_TM.md); with import pages this holds at any depth.
  A table a writer evicted as idle comes back when it is written again. A
  maintenance call on `FileIOCatalog` re-inlines a table without a data
  commit. A compaction never re-inlines: it writes no table records.
- *A table upgraded to v3.* Its first commit after the upgrade lists its kept
  manifests with row ids that the pool's entries lack, which evicts it
  (errata D22). It was thought evicted for good; step 5's test showed its
  next commit re-inlines it, with or without paging: a pointer table has no
  pool, so the lists start a new one with the upgraded entries, and the old
  snapshots keep their list files (in import pages, when paged).

## Sizes

A table's part of the root, measured after step 5 (2026-11-07):

- **E3** (catalog-bench): 10 tables of mix A (merge appends), as their
  catalog object's median size over a run's measured window, divided by
  10. Before step 2 is e3-d20 (fileio e505a0f), lists as deltas only is
  e3-sp4's ablation (paging off), and paged is e3-sp4 (fileio 4aca81c).
- **ReplayBench**: one table's checkpoint (REPLAY_BENCH_RESULTS.md).
  Before step 2 is the capture before it; the other two are step 4's off
  and on.

The fast-append row is still estimates past its first column, because the
TM capture of fast appends at 3,000 does not fit the dev box:

| Table | Before step 2 | Lists as deltas only | Paged, W = 1,024 | Its pages |
|---|---|---|---|---|
| ML, merge appends, 7,200 snapshots (E3) | 2.38 MB | 1.28 MB | 0.36 MB | 1.07 MB in 24 |
| ML, merge appends, 3,000 snapshots (ReplayBench) | 927 KB | 393 KB | 216 KB | |
| ML, fast appends, 3,000 snapshots | 12.8 MB | about 0.4 MB | about 0.3 MB | |
| TM only, 7,200 snapshots (E3) | 0.46 MB | the same | 0.23 MB | 0.32 MB in 24 |
| TM only, 3,000 snapshots (ReplayBench) | 176 KB | the same | 134 KB | |

The estimates for E3 were about 1.2 MB and 0.25-0.45 MB for an ML table,
and about 0.2 MB paged for TM.

The paged root still costs about 18 B a sealed snapshot (its id and log
entry), about 110 KB at 7,200, and a catalog of many such tables adds them
up ([Open questions](#open-questions)). A fast-append
table's head list names every manifest it has: paging cannot shrink that, and
only a merge can.

**What it buys a commit.** E3 at 7,200 snapshots, from catalog-bench
e3-d20 and e3-sp4 (medians of 3 runs). Each cell is a merge append's
commit latency (p50 / p99) and the catalog bytes it read:

| Store, mode | e3-d20 | Paging off | Paged |
|---|---|---|---|
| S3 Standard, ML | 9,179 / 99,786 ms, 158 MiB | 2,811 / 48,911 ms, 74 MiB | 485 / 2,099 ms, 8.1 MiB |
| S3 Standard, TM | 824 / 9,328 ms, 13.5 MiB | 838 / 6,046 ms, 12.9 MiB | 475 / 1,301 ms, 5.1 MiB |
| S3 Express, ML | 904 / 5,447 ms, 56 MiB | 346 / 4,642 ms, 28 MiB | 109 / 723 ms, 7.2 MiB |
| S3 Express, TM | 358 / 1,425 ms, 9.6 MiB | 199 / 1,655 ms, 9.4 MiB | 117 / 403 ms, 4.6 MiB |

- **ML on S3 Standard.** Every commit there is a CAS of the whole object.
  With paging off, the driver saturated: its CPU, its GC (24% of the run)
  and its writer pool. It finished 174 of the schedule's 500 merge appends.
  Paged, it ran all 500 with GC at 2.3%, against e3-d20's 44.5%.
- **Pages read.** A paged merge append reads none. Before a commit read
  only the snapshots it adds (e3-sp3, fileio ebb621e), it read about one
  (0.8-1.6 GETs), and its p50 was 547 ms on S3 Standard and 130 ms on S3
  Express.
- **Pointer tables** are unchanged: 778 and 431 ms p50 against e3-d20's 807
  and 441.
- **TM and pointer on S3 Standard** flag the driver's writer pool in every
  arm. Their hourly expiry reads every snapshot's list file, 7,211 GETs, and
  takes 260 s; the table's merge appends queue behind it. ML's lists are
  inline, so its expiry takes 0.9 s.
- **Live heap at the end of a run** (after a full GC) at 7,200 ML
  snapshots:
  - S3 Standard: 1.47 GB paged, 2.04 GB with paging off (which ran a third
    of the commits), 6.85 GB for e3-d20.
  - S3 Express: 1.52, 2.36 and 5.17 GB.
  - Setup alone leaves 0.85 GB: the aged tables' files in the in-memory
    store.
  - At 1,000 snapshots, where no block seals, the run ends at 0.33-0.35
    GB, against e3-d20's 0.68-1.07.
  - The rest at 7,200, about 75 MB a client, is each of the run's 8
    clients' replica: the catalog object, each table's inline window, and
    Iceberg's snapshot log, which a table's metadata holds whole (7,200
    entries a table).
  - Before a commit read only the snapshots it adds (e3-sp3), the paged
    runs ended at 2.67 and 2.90 GB, with 1.8 and 1.5 times the CPU. The
    difference was the tables' whole history, decoded once per client: 10
    tables of 7,200 snapshots in each of the 8 clients (2026-11-07,
    attributed locally: the S3 Express configuration run from havoc's pack
    ended at 2.91 GB, as on havoc).
    - Its live heap grew 2.0 GB from the warmup's start. That growth holds
      589,000 `InlineSnapshot`s, 559,000 `BaseSnapshot`s and 587,000
      summary maps: 8 × 72,000. That is about 3.7 KB a snapshot, or 250 MB a
      client.
    - Most of it is the summaries, a fresh map of strings per snapshot parsed
      from its JSON, and the manifest entries of the pages' lists.
    - Every merge append decoded it. `stageOn` asked for the snapshots of the
      commit's metadata, and that resolved the base's snapshot supplier. It
      did so in `manifestUpdates`, to find the snapshots the commit adds,
      and in `computeDelta`, which compared the two versions' ids. The
      supplier reads every sealed block's page.
    - Two holders kept the history. The catalog's page cache
      (`InlineSnapshots.pages`) is bounded at 256 pages, not by bytes, and
      E3 has 240, so every client kept every page, and with it the 85% of
      snapshots behind the line. The resolved supplier of each table
      version's metadata held an `InlineSnapshot` over each of them for as
      long as the client's replica held that version. The replica's
      chains hold the inline window.
    - One process per client, as in production, would hold one copy. The
      benchmark's driver hosts all 8 (catalog-bench DESIGN.md, caveats 7
      and 8).
    - Since 2026-11-08 a commit reads only the snapshots it adds (step 3,
      "Still made whole"), so it holds none of this; e3-sp4 measured the
      numbers above it. A load that asks for history, an expiry or a
      transaction still reads every page, and the page cache, bounded in
      pages, keeps them ([Open questions](#open-questions)).

## What changes in the code

- **Replay** (`ProtoCodec`, `NativeDelta`, `InlineDeltaCodec`): the split of
  every delta into native snapshot updates and the builder on a head without
  snapshots; native expiry; FULL carrying the head; sealing; a ref move with
  its leaf and proof; the `sealed: ...` rejections.
- **State** (`CatalogState`, `InlineTable`, `TableFields`): inline and sealed
  snapshots; per-block live ids and Merkle roots; ordinals for the pool.
- **Codec** (catalog.proto, the structural codec): the head apart from the
  snapshot block; `SnapshotPage`; lists by ordinal and as deltas; the
  envelope's reader version.
- **Compaction** (`LogRuntime`'s checkpoint step, R0.3): pages for blocks
  sealed since the last root, put before the root; the sweep.
- **Loads** (`LogCatalog`, `InlineTableMetadata`, `InlineSnapshots`): the
  head's metadata with a snapshots supplier, and a page cache.
- **Writers** (`LogCatalog.stageUpdate`): the leaf and proof when a ref moves
  onto a sealed snapshot.
- **The fork**: a lazy `Builder(base)` and `SerializableTable`, and an
  `InlineSnapshot` that resolves its list on first use.
- **Re-inlining** (`LogCatalog`'s full mode, `FileIOCatalog`): import pages
  put before a FULL record that names them, their GC, and a maintenance
  call.

## Testing

The cases that matter sit at the sealing line, as the committed set's sit at
its marks: records that touch a snapshot just inside the inline window, just
behind it, in a sealed block, pinned by a ref, brought back, or expired. The
fuzzers run with B = 4 and W = 8, so that tables seal blocks constantly.

**Targets drawn against the line.** A generator draws the snapshot a record
touches (its parent, its ref target, its cherry-pick source, the snapshots an
expiry removes) from regions of the table where it lands:

| Region | A commit on it (parent) | A ref moved to it | Expiry of it |
|---|---|---|---|
| the head | applies | applies | per Iceberg |
| inline, within W | merges or conflicts per the encoding | applies | applies |
| in the block the next record seals | applies; `sealed` once a later record has sealed it | applies; with a proof once sealed | applies |
| sealed, live | conflicts (`sealed`) | applies with a valid proof; `sealed` without, or with a wrong one | applies: its id leaves the live set |
| sealed, expired | conflicts | `sealed` (not live) | no-op, as Iceberg's |
| pinned by a ref (a straggler) | as inline | applies | applies |
| brought back by an earlier record | as inline | applies | applies |

**The reference.** Every property runs a paged catalog beside a catalog that
never pages (B infinite), on the same schedule:

- **Loads agree.** Each table reads the same through both: metadata, every
  snapshot, the snapshot log, refs, lists, files per ref, every metadata table
  (`TableViews.logical` and `physical`, as `TestInlineDifferentialProperties`
  compares pointer and inline catalogs).
- **Admission differs only where it may.** A record that applies in the
  reference and not in the paged catalog must be one that needed a sealed
  snapshot without carrying it (its rejection says `sealed`). Every other
  outcome is the same. The reverse never happens.
- **The replay check** compares the split of each delta with the builder on
  the whole table, in the reference, where every snapshot is at hand.

**The catalog fuzzers** (`TestCatalogFaultProperties`, `TableScript`'s
schedules, the differential and rebase properties) gain the small B and W,
and operations aimed at the line: rollbacks and tags on old snapshots,
fast-forward cherry-picks of old staged snapshots, branches that keep their
own ancestors past expiry (holes in blocks), expiries whose cutoff falls
inside a block, and stale writers whose base is behind the line. They
already move tables out of inline storage and back (a small inline update
limit); at the small B and W the tables they bring back are deep, so a
re-inline imports pages, and page faults reach its puts. A re-inlined table
must load as its pointer form did.

**Pages.** `CheckpointRoundTrip` checks that every page a root names exists,
decodes, hashes to its name, and holds what the reference's snapshots are; and
that replaying the log equals decoding the root (I11, with roots byte-equal).
Faults: a page missing, a get that fails, a put whose reply is lost, a sweep
at any step. Oracles:

1. A load or an expiry's read never takes a missing page for empty: it fails.
2. A sweep deletes only pages the root shows are garbage, and never one that
   a root a provider may serve names, nor an import page whose record may
   still apply.
3. At the drain, after every table is dropped or expires every snapshot, a
   sweep leaves no page.
4. A load that asks for no history reads no page (requests counted, as
   `TestCommitReads` counts reads).

**TLA+.** The models learn pages for the state as the review proposed (put,
get, delete, a root naming them), with bug switches for a root written before
its pages, a sweep of a page a servable root names, and a missing page read
as empty.

**Measurements.** ReplayBench at 3,000 and 7,200 snapshots (fast and merge),
the ingest captures' checkpoint breakdown, and catalog-bench E3 (depth
7,200), before and after each step below.

## Order of work

Each step stands alone and is measured before the next:

1. **Replay without whole-table history.** The split of deltas (native
   snapshot updates, the builder on a head without snapshots), native expiry,
   and FULL carrying the head. The first two change no format; the third
   does: a FULL record on an inline table carries the head, the snapshots
   its base lacks and the ids of those it drops. The replay check and the
   fuzzers cover all three. Built 2026-11-05 (7d3d0da): every delta applies
   to the fields in passes split at manifest updates (`NativeDelta`), the
   replay check now also requires the builder and the fields to reject the
   same records, and a full update of an inline table is `head_only`,
   merged lazily (`NativeDelta.full`, `InlineTable.ofHeadOnly`; a head-only
   update of a pointer table does not apply). Measured with `ReplayBench`'s
   `last`: a schema change replays at 93,000 a second whatever the depth (it
   was 6,100 at 3,000 snapshots), a statistics change's record is a third the
   size at 3,000 snapshots and replays 2.6-5.3 times faster, and an expiry
   costs what it did (REPLAY_BENCH_RESULTS.md).
2. **Lists by ordinal and as deltas** (R2.1). A format change, no pages. This
   alone fixes fast appends' square growth (D18). Built 2026-11-05: a pooled
   manifest's ordinal comes from the table's counter (`next_ordinal`) when
   the table first pools it, and the checkpoint writes the pool in ordinal
   order (`pool_ordinals`); a list goes whole (`ordinals`) or as a change of
   its parent's (`base`, the entry that many before it; `removed`; `added`),
   whichever is smaller, and whole every 64 along a chain (`ManifestLists`:
   a change keeps the longest run of the parent's manifests the list holds
   in the parent's order). Lists go in the table's snapshot order, so a
   parent's precedes its children's. A table whose metadata does not decode
   (tests' placeholders) has its lists written whole. Measured with
   `ReplayBench` on fast appends (each list holding every manifest): the
   checkpoint of a 1,000-snapshot table is 134 KB (1.27 MB before), and
   encoding, decoding, a read and a load are all faster
   (REPLAY_BENCH_RESULTS.md).
3. **Lazy loads.** Blocks as the unit of decoding, lists resolved on first
   use, loads through the supplier, and the fork following #18073 and
   changing `SerializableTable`, with tests in the fork; behind
   `fileio.catalog.load.lazy`. Readers that load only what they access gain
   from this without any page. Built 2026-11-05, with two clarifications
   below:
   - The fork: #18073 ported to 1.12.0 (atomicio af097dcda2, and the
     baselines' perf a18e9c3f50: 1.12.0's REST table operations carry the
     snapshot mode beside their read parameters), and an `InlineSnapshot`
     that makes its list on first use (96970062e3).
   - Lists on first use: a load's `InlineSnapshot`s make their lists when
     asked for (e7a9a23), and a checkpoint's decode keeps each list as a
     change of its parent's, applied on first use (0d0020a). An expiry's
     pool GC and a checkpoint's encode still make every list.
   - Snapshots on first use (672763e): the structural codec decodes a
     table's columns at once and makes each snapshot from them when first
     read, as the parser would (`Chain.lazy`); replay reads only what it
     needs (ids by the chain's index). This is "blocks as the unit of
     decoding" without blocks in the root: the work a read skipped was
     making snapshots, not reading their columns, so the root keeps its
     inline snapshots in one block as before, and the grid's blocks are
     written only as pages (step 4, by `encodeSnapshotsBlock` on a block's
     snapshots). Whether the root should also cut its inline snapshots by
     block (to reuse unchanged blocks' bytes in a checkpoint) is measured
     with step 4.
   - Loads through the supplier (c5f0ee5): a load holds the snapshots its
     refs target and a supplier of all of them (`LazySnapshots`), which
     Iceberg calls for history, time travel, expiry and a commit's delta.
   - The supplier holds only its version (Bug 83, fixed 2026-11-06). The
     metadata a lazy load makes is kept on the version's `InlineTable`,
     which later states share while the table is unchanged. Its supplier
     called the loading operations' methods and, from 7fba41f on, used the
     read's state, so it held both. The operations held their last read: a
     replica, which is the catalog object's bytes and its state. That
     state's other tables kept metadata holding older reads in turn.
     catalog-bench's e3-sp campaign found it. At 1,000 snapshots, live heap
     was 1.5-3x e3-d20's. Mid-run, 8 clients held 95 replicas where 8 were
     needed. At 7,200 snapshots unpaged, where a read is about 30 MB, a run
     ran out of memory in a 12 GB heap. The supplier is now made by a static
     method (`allSnapshots`) from what it needs. In a local run of e3's
     1,000-snapshot ML configuration on S3 Express (150 s measured), live
     heap at the end fell from 0.92 GB to 0.50 GB; e3-d20's 300 s runs on
     havoc ended at 0.73 GB. e3-sp is rerun with the fix.
   - Unused lists hold their state's pool only (Bug 84) and the checkpoint
     memo goes with its row (Bug 85, CHECKPOINT_PLAN.md R0.2), both fixed
     2026-11-07. e3-sp2, the rerun, still ended at 2.3-3.9 GB of live heap at
     1,000 ML snapshots on S3 Standard, where every commit is a CAS, against
     e3-d20's 1.1-1.3 GB; S3 Express, which appends, matched e3-d20. Run
     locally in havoc's 12 GB heap, the configuration ended at 3.21 GB. Two
     things kept old decodes. First, a catalog keeps the `InlineSnapshot`s
     its loads made, per table, for later loads to reuse. One made lazily
     held the pool of its load's state until its list was first used, which
     for an old snapshot is seldom. A lazy load wraps only its refs' targets,
     so nothing dropped the rest: in a CAS mode, where a read decodes anew,
     each held a whole decode's pool. A load of new lists now drops what was
     made from lists its state does not hold and moves the unused rest onto
     its state's pool. Second, and most of it, the memo's `WeakHashMap` kept
     each entry's table, pool and lists for an old-generation cycle after
     its row died. In a CAS mode every state has rows of its own, so every
     state's tables were kept between cycles: 1,885 entries for 80 live rows.
     With both fixed, the local run ended at 0.33 GB (from 3.21), its GC time
     2.3 s (from 5.1) and its CPU 97 s (from 131). e3-d20 had the memo too,
     so its live heap is high by some of the same. e3-sp3, the second rerun,
     ended at 0.33 GB on S3 Standard and 0.49 GB on S3 Express at 1,000 ML
     snapshots (e3-d20: 1.07 and 0.68 GB), with GC at 1.1% of the run
     (e3-d20: 5.5%); its bytes and requests are e3-sp2's ([Sizes](#sizes)).
   - `SerializableTable` moves to step 4: before pages, a worker's supplier
     could read only the whole catalog object, whose newer versions may
     have expired the shipped version's snapshots; pages are immutable and
     named. Until then a copy ships every snapshot, as before.
   - Still made whole: an expiry, a full update, a checkpoint's encode, and
     a copy for workers.
   - A commit's delta was made whole too until 2026-11-08. `computeDelta`
     compared the two versions' snapshots, and `manifestUpdates` looked
     through them for the new ones. Paged, that read every page, and at
     E3's 7,200 snapshots each client kept the whole history
     ([Sizes](#sizes)).
   - A delta now reads only the snapshots the commit adds (`SnapshotChange`).
     When the commit's metadata still holds its base's supplier, uncalled,
     those are the snapshots its changes past the base's add
     (`InlineTableMetadata.addedSnapshots`). A builder that removes a
     snapshot, or moves a ref onto one it has not made, makes them all
     first, so such metadata removes none. Each added snapshot is numbered
     past the base's last sequence number, so the base cannot hold it.
     Otherwise, as on format 1, the two versions are compared as before.
     `TestSnapshotPaging.aCommitMakesNoSnapshotItDoesNotAdd` checks
     merge and fast appends, an append to a branch, and a staged append.
   - Iceberg still makes every snapshot in three cases ([Open
     questions](#open-questions)): a transaction, a transaction that moves
     main twice, and a staged snapshot's producer after its commit.
   - Measured by catalog-bench e3-sp4 ([Sizes](#sizes)): at E3's 7,200
     snapshots a paged merge append reads no pages (about one before), and
     a run ends at 1.5 GB live (2.7-2.9 before), with 0.56-0.66 times the
     CPU.
4. **Sealing and pages.** The root's sealed blocks, the page store (shared
   with the committed set), refs moved back with proofs, and page GC; and
   `SerializableTable` shipping the head and a supplier that reads pages
   (from step 3). Built 2026-11-05/06, behind `fileio.catalog.snapshot.block`
   (256 since step 5, 0 before) and `.window` (1,024), which the catalog's first
   record fixes (`SetSnapshotPaging`, 28f8248). As built:
   - Replay seals (`SnapshotSealing`, d896226) after a delta or full update,
     the blocks the table's last sequence number passed, keeping refs'
     targets inline. A sealed block's entry (`SealedBlock`) holds its digest,
     Merkle root, leaf count, live ids and the table version of the record
     that sealed it; the page holds that version and the table's uuid (the
     log id the sketch above names is not known to replay), so a table that
     leaves inline storage and comes back seals under new names and a
     dropped page is garbage for good (a5c8aaf). Pages are named
     `snap.<table>.<version>.<block>.<digest>`.
   - Until a compactor stores a page, it exists only in the replicas that
     replayed its seal (`Pending`): a load reads those bytes first, then the
     page store. The runtime puts a state's pending pages before any
     checkpoint it writes (`StateMachine.pages`; a5c75ae), so no root precedes
     its pages.
   - Records that need a sealed snapshot without carrying it do not apply,
     with reason `sealed: ...`; a writer brings back each sealed snapshot its
     refs target with its leaf and proof (`BroughtBack`, 3b9d2d0), which
     replay checks against the root's Merkle root.
   - Expiry removes sealed ids from their blocks' live ids; replay records
     the pages its records drop, and the runtime deletes them once a root
     without them lands; `LogRuntime.sweep` deletes orphans
     (`StateMachine.garbagePages`; a5c8aaf).
   - A copy for workers (`SerializableTable`) of a table whose history is
     not loaded ships the snapshots its load holds and a supplier a worker
     calls (the fork's `ShippableSnapshots`, atomicio 29e0a930c2): the inline
     snapshots and their lists as a page, and where each sealed block's page
     is, read through the table's FileIO (`PagedSnapshots`); a block no
     compactor has stored goes with its page's bytes. A log whose pages are
     not files (`PageStore.location`) ships every snapshot, as before.
   - Testing: `TestSnapshotPages`, `TestSnapshotPaging`, the suites with
     `-Dfileio.catalog.test.snapshot.block=4` (window 8) in the test JVM, and
     rounds with `SNAPSHOT_BLOCK=4` (fuzz/ce-fuzz). Unset, the suites and
     rounds take the catalog's default, which seals nothing in their short
     schedules; `0` turns paging off. `CheckpointRoundTrip`
     checks every root's pages. The file model (`spec/tla`) has the state's
     pages: puts before the root, sweeps from any view, and two bug switches
     (a root before its pages; a sweep that ignores versions) that break
     `StatePagesExist`; the shared-log model does not have them yet.
5. **Re-inlining at any depth.** Import pages, the FULL record that names
   them, their GC, and a maintenance call that re-inlines a pointer table
   without a data commit. Built 2026-11-06:
   - A full update of a pointer table in a catalog that pages
     (`LogCatalog.stageUpdate`, `importBlocks`) cuts the snapshots behind
     the sealing line at the table's last sequence number into blocks,
     keeping inline its refs' targets and any snapshot whose list the update
     sends inline, and puts each block's page while staging, before the
     record is sent. The record carries the head and an entry per block
     (`imported_blocks`, field 12 of `UpdateTableInline`), which replay
     installs as sealed blocks (`ProtoCodec.UpdateTableInlineAction.imports`).
   - Replay checks what the record shows (reason `import: ...`): blocks of
     the version the update makes, ascending, nonempty, apart from each
     other and from the head, no ref on one, only on a full, not head-only,
     update of a pointer table. The rest is the writer's word.
   - `LogCatalog.reinline(TableIdentifier)` re-inlines a pointer table
     without a data commit (its current metadata, unchanged), retrying a
     conflict on a fresh read; it returns whether the table is inline,
     false when the head does not fit or the catalog does not inline.
   - Testing: `TestSnapshotPaging` (a 40-snapshot pointer table at B = 4,
     W = 8 comes back with 8 import blocks and 9 inline snapshots; loads,
     time travel and a rollback into an imported block; an import whose
     record does not land is rejected and its pages swept; the checks; a
     v3-upgraded table back after its eviction, errata D22; a stale handle
     whose pages were collected, read as the table is now: "Page GC"), and a
     `REINLINE` operation in
     `TestCatalogFaultProperties`, whose other re-inlining commits also
     import when the suite pages. At B = 4, W = 8 its 40-operation schedules
     reach no import (a probe counted none in 200 tries); at B = 2, W = 1,
     30 in 200, so rounds for this step run at B = 2, W = 1. The file model
     (`spec/tla`) does not have import pages: their order (pages, then the
     record) is the compactor's (pages, then the root), and their sweep is
     the same rule by version.
6. **Cold tables evicted** by a writer's policy, if the measurements still
   call for it. Deferred (2026-11-06): they do not yet. An idle table of
   3,000 snapshots costs the root 134 KB with inline table metadata and
   216 KB with inline manifest lists too (176 KB and 393 KB unpaged; step
   4's `ReplayBench`, B = 256, W = 1,024); one of 7,200 costs an estimated
   190-270 KB, so 100 such tables add 13-27 MB to every read of the
   catalog. Evicting one leaves a pointer entry, but costs, once, a
   metadata file of its whole history and, with inline lists, a list file
   per snapshot (3,000 writes at that depth); its next write brings it back
   (step 5). A smaller window for idle tables would leave the sealed
   residual, about 18 B a snapshot (130 KB at 7,200), so it barely helps
   the deepest. And Iceberg's default expiry (5 days, at least 1 snapshot
   kept) shrinks an idle table to its last snapshot within 5 days where
   maintenance runs: the cost stays only for deep tables nobody expires.
   Revisit when a catalog-bench workload has many idle tables.

## Alternatives considered

- **Seal by a record** (a PageOut per block, as the committed set does). It
  lets the policy change without a format change, but costs a write per
  block, refusals and orphans. A table's sequence numbers seal
  deterministically without a clock, so the record buys nothing here.
- **Rewrite blocks to drop expired snapshots** (an LSM's compaction). It
  saves bytes at rest. The root already drops their ids, and expiry empties
  old blocks whole. Worth it only if holes persist, which the fuzzers' branch
  retention will show.
- **A digest per snapshot in the root** instead of a Merkle root per block:
  16 B more per sealed snapshot in the root, against 8 hashes in the rare
  record that brings one back.
- **A filter alone instead of the sealed ids** (about 3 B an id; with proofs
  of absence it works, see [Open questions](#open-questions)). A false positive
  rejects a valid snapshot id, and `SnapshotProducer` keeps its id across
  retries, so the commit would fail every time it retried.
- **Lazy `InlineSnapshot`s only** (a D18 option). It cuts a load's cost, not
  the checkpoint's: step 3 takes it, and the checkpoint still needs pages.
- **Evict by size** (a D18 option). The table leaves inline storage whole and
  loses its single-write commits.
- **References in a tree.** Each block costs the root one reference (about
  50 B with its Merkle root, plus its live ids), so 7,200 snapshots need 28. A
  tree pays only for tables of millions of snapshots.

## Open questions

- **W and B**: their defaults, swept in experiments.
- **Commits that Iceberg makes whole.** The catalog's own commit path reads
  only the snapshots a commit adds (2026-11-08; step 3). Iceberg's still
  makes every snapshot of a lazily loaded table in three cases, each of
  which would read every page of a deep paged table:
  - **A transaction.** `BaseTransaction.commitSimpleTransaction` notes the
    ids of the snapshots it started with, to clean up after a failure.
    `manageSnapshots()` commits through one.
  - **A transaction that moves main twice.** The builder rewrites the
    snapshot log without the intermediate snapshot and checks it against
    every snapshot.
  - **A staged snapshot.** Its producer looks for it, after the commit, in
    the version the operations adopted, which holds only its refs' targets.

  Each is a fork change, upstreamable with the lazy loads it completes.
  None is on E3's path: its merge appends and compactions are single
  producers, and an expiry makes every snapshot anyway.
- **The page cache's bound** is in pages: 256 a catalog, all of E3's 240.
  A bound in bytes would hold for deep fast-append tables, whose pages are
  larger.
- **The sealed ids and log entries.** A sealed snapshot still costs the root
  about 18 B: its id (8 B) and its snapshot-log entry (about 10 B). A table's
  depth is the snapshots it keeps, its commit rate times its retention;
  Iceberg's default retention is 5 days (`history.expire.max-snapshot-age-ms`),
  applied when expiry runs:

  | Writer | Commits | Snapshots kept over 5 days | Sealed residual |
  |---|---|---|---|
  | daily batch | 1 a day | 5 | none (inline) |
  | hourly batch | 24 a day | 120 | none (inline) |
  | Kafka Connect | every 300 s | 1,440 | about 7 KB |
  | Flink | every 60 s | 7,200 | about 110 KB |
  | Flink | every 10 s | 43,200 | about 760 KB |

  A table that never expires keeps growing. The residual adds up per catalog:
  1,000 tables written by Flink every minute carry about 110 MB. Cutting it
  takes two changes, both deferred until measurements call for them: log
  entries that name their snapshot by position in the ids (about 10 B to 3),
  and the ids replaced by a filter in the root (8 B to under 2), with a writer
  whose new id the filter flags attaching a proof that no sealed block holds
  it (each block's ids in a Merkle tree sorted by id). A lazy snapshot log
  would remove the entries entirely, but Iceberg validates the log whole at
  construction, so it is a fork change.

  Nesting is the general form (review, 2026-11-05): the root's per-block
  entries (the live ids, and the log entries) would move into pages of their
  own, the root keeping one reference per group of blocks, nested again as
  tables deepen, as [References in a tree](#alternatives-considered) does for
  the blocks' references. It meets the same two obstacles: replay checks a
  new id against every sealed id, so the ids leave the root only with the
  filter and its proofs, and Iceberg needs the whole log, so the log entries
  leave it only with the fork change. It is deferred with them, until a
  workload calls for it. A table that keeps tens of thousands of snapshots
  may be misconfigured: Iceberg itself rewrites a metadata file holding every
  kept snapshot (hundreds of bytes of JSON each) on every commit.

- **A table upgraded to v3** gives row ids to the manifests it keeps, which
  evicts an inline-ML table once (errata D22); its next commit brings it
  back ([Re-inlining](#re-inlining)). Whether to avoid the eviction (restating
  the pool's entries in one record) is open, for deep tables.
