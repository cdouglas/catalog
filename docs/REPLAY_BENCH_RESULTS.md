# Replay benchmarks

JMH results for replay alone: how fast a catalog reader tests and applies records, and what a
read and a table load cost. Measured on the dev box (AMD Ryzen 7 PRO 8840HS, 16 threads, WSL2,
JDK 17), 2026-10-26/27, while building the native replay state (`InlineTable`, `TableFields`,
`NativeDelta`). The "after" figures were taken while other work shared the machine; the
per-encoding ones on a quiet machine.

Source: `iceberg-fileio-catalog/src/test/java/org/apache/iceberg/io/ReplayBench.java`. One writer
commits fast appends (one file each, round robin over 32 partitions of one table) to a catalog
that replaces its object on every commit, and the record each commit wrote is kept, cached under
`target/replay-bench`. Then:

- `replay` decodes, tests and applies the records onto the checkpoint the log started from;
  `records` is records per second;
- `read` decodes a checkpoint taken `tail` (100) records before the end, replays the tail and
  builds the state, as each commit attempt does twice;
- `load` loads the table through a `FileIOCatalog` on that object, as a client's refresh that finds
  a new version does;
- `reread` reads the object after `fresh` more records landed, from its checkpoint, as a reader
  that reads afresh brings itself up to date; `fold` replays only those records onto the state
  the reader holds, as the runtime does when a read continues the one its replica came from
  (LOG.md "Replay").

Run via (after `mvn test-compile`; async-profiler from a JetBrains IDE's `lib/async-profiler`):

    java -Xmx1g -cp <test classpath> org.openjdk.jmh.Main ReplayBench -p mode=ml,tm -prof gc
    java ... org.openjdk.jmh.Main 'ReplayBench.replay$' -prof "async:libPath=<lib>;output=collapsed"

## Before: replay decoded and re-encoded the whole table per record

`CatalogState` held each inline table as encoded bytes. Replaying a delta decoded the table
(structural codec, JSON, `TableMetadataParser`), applied the delta through
`TableMetadata.Builder`, and encoded the table again (JSON, gzip). Every read replays from the
checkpoint, and a commit reads twice.

| | pointer | inline TM | inline TM+ML |
|---|---|---|---|
| replay, 100-commit log | 2.09M rec/s | 508 | 525 |
| replay, 300-commit log | 2.01M rec/s | 203 | 198 |
| allocation per record (300) | 1.4 KB | 3.8 MB | 3.7 MB |
| record decode alone (300) | 4.8M rec/s | 3.4M | 4.3M |

Profile of a record's replay into a 300-snapshot table (about 5 ms):

| Where | Share |
|---|---|
| structural encode | 55% |
| ├ snapshot blocks | 20% |
| ├ summary planning (a `NumberFormatException` per non-numeric value) | 19% |
| ├ gzip | 10% |
| └ `toJson` | 10% |
| decode | 40% |
| └ `TableMetadataParser.fromJson` | 20% |
| applying the delta | 3% |

The log runtime and the record format were not the cost: pointer tables replay at 2M records/s.

## After

Replay keeps each inline table as `TableFields` (an `InlineTable` holds the bytes and the fields,
each made from the other when asked for). `NativeDelta` applies a record's snapshots, refs and
properties to the fields directly, reproducing `TableMetadata.Builder` exactly (checked against
the builder on every apply in the test suites); other updates take the builder's path. A table's
manifest lists are shared between states, and a load wraps each manifest once.

Replay, records/s:

| | before | after |
|---|---|---|
| inline TM, 100-commit log | 508 | 139,634 |
| inline TM, 300-commit log | 203 | 135,224 |
| inline TM+ML, 100-commit log | 525 | 118,435 |
| inline TM+ML, 300-commit log | 198 | 107,914 |

Inline-TM replay no longer depends on the table's size; allocation per record fell from 3.8 MB to
about 13 KB (TM) and 22 KB (ML).

Read and load, 300 snapshots, a 100-record tail, ms per operation:

| | read before | read after | load before | load after |
|---|---|---|---|---|
| inline TM | 3.4 | 1.5 | 3.6 | 1.6 |
| inline TM+ML | 4.8 | 2.8 | 13.8 | 5.9 |

## By conflict encoding

Inline TM+ML, 300 commits, 2 forks of 5 iterations (± the 99.9% interval):

| encoding | replay, records/s | allocation per record | read |
|---|---|---|---|
| lamport | 93,300 ± 9,800 | 22.5 KB | 3.2 ms |
| lamport-minus-properties | 76,300 ± 2,300 | 25.7 KB | 3.4 ms |
| concern-vector | 75,800 ± 2,700 | 25.9 KB | 3.4 ms |
| blind-append | 80,800 ± 7,100 | 25.7 KB | 3.3 ms |
| partition-vector | 82,900 ± 6,500 | 25.5 KB | 3.1 ms |
| manifest-window | 73,700 ± 6,300 | 25.8 KB | 3.0 ms |

The encodings' bookkeeping (each record's change observed into stamps) costs 10–20% of replay;
a read is dominated by decoding its checkpoint either way.

## Snapshots, and folding a read onto a replica

Inline TM+ML under `manifest-window`, one table of 32 partitions, one file per commit, every
snapshot kept (2026-10-27; intentlog's test suites shared the machine for part of the run). `op`
is how the writer appends: `fast` (`newFastAppend`) never merges manifests, so a snapshot's list
holds every manifest before it and the lists hold about snapshots²/2 entries; `merge`
(`newAppend`, Iceberg's default) merges them once a list reaches 100, as Iceberg's defaults say.
The capture now appends to its object instead of replacing it on every commit, so 3,000 commits
take minutes. The reader read a checkpoint and the 100 records after it; `fresh` records landed
since.

The object (records are about 470 B each under `fast`, 550 B under `merge`):

| snapshots | checkpoint, `fast` | manifests in the current list | checkpoint, `merge` | manifests in the current list |
|---|---|---|---|---|
| 300 | 77 KB | 299 | 57 KB | 2 |
| 1,000 | 1.26 MB | 999 | 283 KB | 9 |
| 3,000 | 12.8 MB | 2,999 | 927 KB | 29 |

ms per operation, one fresh record (allocation per operation in parentheses):

| | `fast` 300 | 1,000 | 3,000 | `merge` 300 | 1,000 | 3,000 |
|---|---|---|---|---|---|---|
| read (checkpoint and 100 records) | 3.7 (5.4 MB) | 20.8 (32 MB) | 163 (248 MB) | 3.5 (4.4 MB) | 8.9 (12 MB) | 23.8 (32 MB) |
| reread (and the fresh record) | 3.6 | 18.5 | 143 | 3.5 | 8.7 | 23.9 |
| fold of the fresh record | 0.084 (0.11 MB) | 0.33 (0.33 MB) | 1.16 (0.92 MB) | 0.082 (0.10 MB) | 0.33 (0.29 MB) | 1.14 (0.79 MB) |
| fold of 16 fresh records | 0.32 | 0.65 | 1.89 | 0.71 | 0.88 | 1.32 |
| load (the read, then the table's metadata) | 6.0 (8.6 MB) | 51 (64 MB) | 664 (533 MB) | 4.4 (5.9 MB) | 14.0 (17 MB) | 39 (47 MB) |

- A reader that reads afresh pays for the checkpoint whatever landed: a reread of 1 or 16 new
  records costs what the read did.
- Folding them onto the replica it holds costs about 1/40 of a reread at 300 snapshots, and at
  3,000 about 1/20 (merge appends) to 1/120 (fast appends). A fold still grows with the table (84 µs to 1.16 ms for one record from 300 to 3,000
  snapshots): building on a replica copies the touched table's manifest pool, its map of
  snapshots to manifest lists and its encoding's state (copy-on-write per table), and building
  the state copies them again. Persistent maps would leave a fold the cost of its records.
- Under fast appends the checkpoint grows with the square of the snapshots (12.8 MB at 3,000)
  and a read with it (5.7 times from 300 to 1,000 snapshots, 7.8 times from 1,000 to 3,000);
  under merge appends both grow about linearly (927 KB and 24 ms at 3,000).
- A load at 3,000 fast-append snapshots takes 0.66 s and allocates 533 MB: making the table's
  metadata makes every snapshot's manifest list (4.5 million entries). It is now the larger
  cost for a client that loads a table its replica already holds.

3,000 snapshots bracket the costs: fast appends without merging are past where inline manifest
lists pay (the lists are quadratic, which Iceberg's merging appends avoid), and merge appends
and the fold grow linearly, so the slope extrapolates (errata D18, which retired U2's request for
10,000; a capture of that many commits takes about half an hour under merge appends).

## Records that built the whole table (SNAPSHOT_PAGING.md step 1)

Until step 1 (2026-11-05, 7d3d0da), replay applied a delta of snapshots, refs and properties to
the table's fields, but any other record through Iceberg's builder on the whole table: an expiry,
a schema, spec, sort order or location change, and a full update, whose record held the whole
table. Now every delta applies to the fields (the head updates through the builder on the table
without its snapshots), and a full update of an inline table carries its head and only the
snapshots its base lacks.

`fold` of one such record (`last`: an expiry of the oldest snapshot, a column added, a statistics
file set, which takes full mode), at 300 snapshots (fast appends, inline TM) and 3,000 (merge
appends, inline TM, and TM+ML), on main (d02adc1) and on 7d3d0da, ops/s, one fork each, the dev
box; the size of the record in parentheses where it changed:

| | expiry | schema change | statistics change (full) |
|---|---|---|---|
| TM, 300, before | 53,134 | 48,538 | 195,533 (16,436 B) |
| TM, 300, after | 45,516 | 93,534 | 382,033 (6,331 B) |
| TM, 3,000, before | 4,112 | 6,130 | 27,784 (149,808 B) |
| TM, 3,000, after | 3,966 | 93,224 | 72,909 (50,781 B) |
| TM+ML, 3,000, before | 3,485 | | 161 (129,673 B) |
| TM+ML, 3,000, after | 3,166 | | 848 (50,648 B) |

- A schema change no longer depends on the table's depth (93,000 a second at 300 and 3,000
  snapshots); it fell from 48,500 to 6,100 with depth before.
- A full update's record is a third of what it was at 3,000 snapshots, and replays 2.6 times
  (TM) to 5.3 times (TM+ML, which decoded the whole record to reconcile the lists) faster. What
  remains of it is the head, chiefly the whole snapshot log (3,000 entries), which stays inline
  (SNAPSHOT_PAGING.md, "The root").
- An expiry costs about what it did, 4-14% less throughput: it removes snapshots from the
  fields in O(snapshots) (a new indexed `Chain`), as the builder did. The "before" figures
  flatter the builder: `fold` folds onto the same replica each time, so the whole table's
  metadata, which replay built for the builder, was built once and kept; in replay each version
  is new.
- The TM fast-append capture of 3,000 snapshots does not fit in 6 GB (each snapshot's manifest
  list is a file in the in-memory store, holding every manifest), so 3,000 is merge appends.

## Lists by ordinal and as changes (SNAPSHOT_PAGING.md step 2)

Until step 2 (2026-11-05, b892245 and 0f5105e), a checkpoint wrote each snapshot's inline
manifest list whole, as indices into the pool sorted by path. Under fast appends each list holds
every manifest the table had when it was made, so the lists grew with snapshots x manifests
(errata D18). Now a pooled manifest keeps the ordinal it was given when first pooled, and a list
goes as a change of its parent's list (or whole, every 64 along a chain, or where that is
smaller).

Inline TM+ML, fast appends (each list holds every manifest), a 100-record tail; on 7d3d0da
(before) and 0f5105e (after), ops/s, one fork each, the dev box:

| | 300, before | 300, after | 1,000, before | 1,000, after |
|---|---|---|---|---|
| checkpoint | 72,624 B | 26,860 B | 1,268,395 B | 133,631 B |
| `encodeCheckpoint` | 1,438 | 2,110 | 167 | 278 |
| `decodeCheckpoint` | 1,489 | 1,809 | 156 | 238 |
| `read` | 674 | 733 | 107 | 164 |
| `load` | 259 | 275 | 37.9 | 47.4 |

- The checkpoint is 9.5 times smaller at 1,000 snapshots (2.7 times at 300), and now grows with
  the snapshots plus the manifests: what remains is each snapshot's structural metadata, the
  pool's entries, and the head's own list, which names every manifest as its file would.
- Encoding first ran 2.4 times slower (69 a second at 1,000 snapshots): diffing each list against
  its parent's hashed every path into a new map per snapshot. Diffing ordinals through a scratch
  array, and recognizing a fast append's list by its suffix, made it faster than before.
- A read at 1,000 snapshots takes 6.1 ms (9.4 before), a load 21 ms (26). A load's cost is now
  chiefly building the table's metadata and an `InlineSnapshot` per snapshot, which step 3 (lazy
  loads) takes on.

## What remains

A read's cost is now its checkpoint: decoding the table's structural metadata (snapshots go
through JSON); since step 2 the lists cost the snapshots plus the manifests, not their product. A
load still makes an `InlineSnapshot` list per snapshot. Candidates: decoding snapshots without
JSON, and loads that decode only what they use, with lists the fork's `InlineSnapshot` resolves
lazily (SNAPSHOT_PAGING.md step 3). Replay now folds a read onto the replica
it continues, which removes the checkpoint from a reader's refresh between compactions, but the
read still transfers the whole object (errata D15), and a fold copies the touched table's maps.
