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
and the fold grow linearly, so the slope extrapolates. errata U2 asks for 10,000; a capture of
that many commits takes about half an hour under merge appends.

## What remains

A read's cost is now its checkpoint: decoding the table's structural metadata (snapshots go
through JSON) and, with inline manifest lists, every snapshot's list of indices (O(snapshots x
manifests) on the wire). A load still makes an `InlineSnapshot` list per snapshot. Candidates:
decoding snapshots without JSON, delta-encoding snapshot lists on the wire, and lists the fork's
`InlineSnapshot` resolves lazily (a load's cost above). Replay now folds a read onto the replica
it continues, which removes the checkpoint from a reader's refresh between compactions, but the
read still transfers the whole object (errata D15), and a fold copies the touched table's maps.
