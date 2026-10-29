# The log format at a glance

This page maps the documents that specify the catalog's log and ties each part of it to what it
costs, as measured, so that a change to the format can be judged by what it saves. The documents
listed below are the specs of record; this page restates only enough to name the parts.

## Where the format is specified

| Layer | Documents | What they cover |
|---|---|---|
| The intention log, for any state machine on any provider | [LOG.md](LOG.md), [GC.md](GC.md), schema `intentlog-core/src/main/proto/log.proto` | The `SharedLog` interface and what a provider must guarantee (P1–P6), the checkpoint and record envelope, replay, the committed set and its GC, the commit loop, compaction, providers |
| The log in one object | [ATOMIC_FILE_LOG.md](ATOMIC_FILE_LOG.md) | The catalog file: header, checkpoint, appended records; commits on one object |
| The catalog as a state machine | [SPEC.md](SPEC.md), schema `iceberg-fileio-catalog/src/main/proto/catalog.proto` | The catalog's checkpoint and intentions, worked examples, wire format, replay, verification rules |
| Conflict encodings | [SPEC.md](SPEC.md) §"Conflict Encodings", [SPEC_TM.md](SPEC_TM.md) §"Late-bound fields" | Each encoding's witness, the state it keeps per table and what it admits beyond `lamport`; how a delta admitted on a newer version is finalized there |
| Inline table metadata | [SPEC_TM.md](SPEC_TM.md) | Codecs, when a commit sends a delta, full metadata or a pointer, the delta's structure, replay determinism |
| Inline manifest lists | [SPEC_ML.md](SPEC_ML.md) | The manifest pool and per-snapshot lists, and how writers in different modes interoperate |
| Invariants and conflict matrices | [design.md](design.md) | I1–I12 (I11: a checkpoint is the replay of the log; I12: an encoding admits only what Iceberg's retry commits); pairs of operations under each encoding |
| Bytes | [format-explorer/](format-explorer/index.html) | A decoder of catalog files in the browser, with scenarios generated from the codecs |
| Proposed | [COMMITTED_PAGING.md](COMMITTED_PAGING.md) | Paging the committed set out of checkpoints |
| Known costs and shortcuts | [errata.md](errata.md) D14–D18, [REPLAY_BENCH_RESULTS.md](REPLAY_BENCH_RESULTS.md), the lab's ingest pages | Compaction policy, whole-object reads, the committed set, expiry starvation, inline lists stored whole |

## The parts

A log is a checkpoint and the records after it. On one object (`AtomicFileLog`) both are in the
catalog file: an 8-byte header, the checkpoint, then each record, each framed by its length. On
a shared log the records are the log's entries and checkpoints are stored apart, each the state
through a position.

```
LogCheckpoint                                  log.proto
  log_id                                       the log's UUIDv7
  committed: CommittedTransactions             ids replay applied within the retention, ~11 B each
  state: Checkpoint                            catalog.proto
    namespaces, tables                         ids, versions, names, properties
    Table                                      a pointer table: its metadata file's location
    InlineTable                                an inline table:
      metadata                                   TableMetadata, structural codec (gzip at its fastest level)
      manifest_list_prefix, manifest_pool        every manifest its snapshots list, once each
      snapshot_refs                              per snapshot, its list as indices into the pool
      conflict_state                             what the catalog's encoding keeps of the table
    conflict_encoding

LogRecord                                      log.proto
  id                                           the commit's UUIDv7
  intention: Intention                         catalog.proto: the transaction's actions, e.g.
    UpdateTableInline                            a table commit:
      id, version, pinned                          the table, the version its writer read
      delta: TableMetadataDelta                    or full_metadata, or metadata_location
        AddSnapshot                                  id, parent, summary (CompactSummary), footprint
        SetSnapshotRef                               the branch it moves
        AddManifestDelta, RemoveManifestDelta        the snapshot's list against its parent's
        RemoveSnapshots, AddSchema, ...              other updates
  | advance_horizon: AdvanceHorizon            a GC of the committed set
```

Replay folds each record onto the state in log order: it decodes the record, the catalog's
conflict encoding judges each table action where it lands (verify), and an admitted action
applies to the table's fields (`NativeDelta`, or Iceberg's `TableMetadata.Builder` for what it
does not cover) and to the manifest pool and lists. A reader publishes the state after a batch of
records (builds it); a compaction encodes the state as a checkpoint.

## What a record holds

The ingest benchmark's baseline capture (`cap-i1`: 16 writers on 16 tables, 85% merge appends,
5% fast appends, 5% validated overwrites, 5% compactions, an expiry every 5 s keeping 100
snapshots), decoded by the format explorer (`format-explorer/anatomy.mjs`, every 50th record;
lab source `anatomy`, page i8-shapes). Since records name what they keep of their parents
(2026-10-29), a record is 333 B on average (p50 271 B, p90 628 B, p99 1.1 KB); before, it was
744 B (p50 470 B, p99 8.9 KB), the largest being merges that named every manifest they removed.

| Part | Bytes per record | Share | Before |
|---|---|---|---|
| The paths of the manifests it adds (`ManifestFileEntry.manifest_path_suffix`) | 88 | 26% | 87 |
| Snapshot ids, written again in each manifest update and ref (and the snapshot's own and parent's) | 50 | 15% | 72 |
| The other fields of the manifests it adds: counts, sequence numbers, partition bounds | 42 | 13% | 41 |
| Summary counts (`CompactSummary`'s typed fields) | 37 | 11% | 26 |
| The record's id and framing | 23 | 7% | 23 |
| Summary entries beyond the counts (`extra`), now only those that differ from the parent's | 3 | 1% | 214 |
| The manifests it removes from its parent's list (`RemoveManifestDelta`), now by index | 3 | 1% | 191 |
| Everything else: refs, footprints, expiry's snapshot ids, update framing | 87 | 26% | 91 |

Before, two parts named what the state already held: the summary's entries that are the same in
every snapshot (`iceberg-version` alone is ~110 B), and each removed manifest's path, which is in
the parent's list. A snapshot now keeps its parent's extra entries (`inherits_parent_extra`,
SPEC_TM.md "Summaries") and names what it removes by index in the parent's list
(`parent_indices`, SPEC_ML.md). Across i8-shapes' seven workloads records shrank by 41% (fast
appends only, 457 to 270 B) to 71% (one table, 1,226 to 351 B, where merges remove many
manifests). What remains is mostly a new manifest's path, text where Iceberg writes a UUID and a
counter, and the snapshot id repeated in each field that names it.

## What a checkpoint holds

The checkpoint at the end of the capture, from the ingest benchmark's breakdown
(`compactions.jsonl`; campaign `i7-retention`):

| Part | Bytes | Share of the state |
|---|---|---|
| Manifest pools (3,348 manifests, about 108 B each, mostly their paths) | 361 KB | 44% |
| Manifest lists (2,163 snapshots, about 134 B each: one index per manifest, each with its own tag, `pool_indices` being unpacked) | 291 KB | 36% |
| Inline table metadata (16 tables, about 9 KB each at about 135 snapshots each) | 145 KB | 18% |
| Conflict state, namespaces, pointer tables, the rest | 14 KB | 2% |
| **The state** | **812 KB** | |
| The committed set: rate × retention ids at about 11 B each | 2 MB at 100 records/s, 20 MB at 1,000 (30 minutes) | |

At 1,000 records/s the committed set is 96% of the checkpoint; without it, the pools and lists
are 80%.

## What replay costs

One thread, no storage (`IngestFoldBench`, JMH, the capture with its ids retimed to 1,000
records/s and the committed set at its steady state for 30 minutes):

| | Ids in order, evenly spaced | Ids as commits mint them (Poisson landings, lognormal lateness, clock skew) |
|---|---|---|
| Publishing after every record | 22.0k ± 1.1k records/s | 15.6k ± 1.1k |
| Publishing after 64 records | 27.1k ± 7.4k | 25.3k ± 11.9k |

Where the time goes, publishing after 64 records with modeled ids (async-profiler,
`IngestFoldBench.fold`, each sample counted once, in the first of these it falls in):

| Phase | Share | What it is |
|---|---|---|
| Apply: new snapshots' lists | 19% | `NewSnapshotManifests`: a snapshot's list built from its parent's, less what it removes, plus what it adds |
| Apply: the table's fields | 18% | `NativeDelta`: snapshots, refs and properties applied to the table's fields |
| Apply: expiry | 9% | `removeSnapshotManifests`: an expiry's removal of lists and of the pool entries no list names (2% of records) |
| Apply: other | 9% | Iceberg's builder for what `NativeDelta` does not cover, the action's bookkeeping |
| Verify: finalization | 9% | `Landing.unmergeable`: whether a delta on a changed table can be finalized there |
| Verify: the encoding | 8% | manifest-window's witness and its judge over the window of manifests |
| Verify: what the delta changes | 8% | `TableChange.of`, which the judge, the observation and the finalization each need |
| Decode | 12% | the delta (8%), the record's envelope and intention (3%) |
| The committed set | 7% | late ids merged with the entries newer than them |
| Publishing, the loop | 2% | building the state after a batch |

A compaction encodes the state and the committed set on the same thread: 29 ms with the 20 MB set
for 1,000 records/s, 21 ms with the 2 MB one for 100 records/s; a reader decodes the checkpoint in
10 ms and 6.6 ms. With a compaction every 100 records, one thread ingests 3,074 records/s (2,861
with modeled ids) at 1,000 records/s, 4,037 at 100 (campaign `i7-retention`).

Where an encode's time goes:

- **The committed set.** Encoding the state 20,000 records into the capture with the 20 MB set
  takes 15.5 ms (`IngestFoldBench.encode`), 6.3 ms without it. With the set, 58% of the encode is
  the set: 19% concatenating its runs into the `CommittedTransactions` body, and 39% copying that
  body into the checkpoint's envelope.
- **The state, its tables' metadata already encoded,** as a compaction finds the tables no record
  changed since the last one:
  - the manifest lists are 49%, a third of that looking up each manifest's index in the pool by
    its path;
  - the pool's entries are 14%;
  - the rest is varints and copies.
- **The tables changed since the last checkpoint** are encoded again in a compaction proper. With
  one every 100 records (campaign `i6-compaction`, cbe9410's profile), gzip of the inline metadata
  is about 30% of the encode, the structural snapshots block 14%, and the pools and lists 18%.

## How it grows

| Part | Grows with | Cost now |
|---|---|---|
| A record | the commit's changes, chiefly the paths of the manifests it adds | 333 B on average (744 B before records referred to their parents) |
| The committed set | rate × retention | about 11 B per id |
| A table's manifest pool | its live manifests | about 108 B per manifest |
| A table's lists | its retained snapshots × manifests per list | about 134 B per snapshot here |
| A table's inline metadata | its retained snapshots, schemas and specs | about 70 B per snapshot |
| A fold | the records folded | 40–65 µs per record |
| A fresh read | the checkpoint, plus the records after it | decode 6.6–10 ms here, plus the tail's fold |

## Where to improve it

In order of what the measurements say each would save:

- **Records** (the bytes every append and every read of the tail carries):
  - manifest paths as Iceberg makes them, a 16-byte UUID and a counter, not text (26%);
  - snapshot ids written once per record (15%);
  - done (2026-10-29): summary entries a snapshot shares with its parent, and removed manifests,
    by index in the parent's list (together 55% of the baseline's bytes, above). Replaying the
    new records costs 0-10% more per record (JMH IngestFoldBench on each of i8/i9's seven shapes,
    each build on its own capture, steady set, batches of 64: -10% to +5%, mostly resolving a
    snapshot's inherited entries); on records without parent references the new build is as fast
    as the old.
- **Checkpoints:**
  - the committed set paged out ([COMMITTED_PAGING.md](COMMITTED_PAGING.md)): 20 MB to 1-4 MB at
    1,000 records/s;
  - lists packed or delta-encoded against their parents (errata D18);
  - pool paths as UUIDs.
- **Replay** (the fold's profile above):
  - a snapshot's list built without copying its parent's (19%);
  - `TableChange.of` computed once per action, not by each of its users;
  - expiry's list cleanup by reference counts instead of a scan of every list (9% for 2% of the
    records).
- **Encoding a checkpoint:** lists kept as pool indices, so a checkpoint writes them without
  looking each manifest up by its path (a sixth of the state's encode); the committed set
  written into the envelope without a copy.
- **Late ids:** keeping the newest few seconds of the committed set unpacked, so that an id older
  than the newest lands without a merge (29% of the fold, publishing after every record).
- **Compaction off the ingest thread:** not a format change. Any replica's checkpoint at a
  position is the same (I11), so a worker can encode a published state while the fold goes on.
