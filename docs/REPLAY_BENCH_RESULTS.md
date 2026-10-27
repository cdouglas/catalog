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
  a new version does.

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

## What remains

A read's cost is now its checkpoint: decoding the table's structural metadata (snapshots go
through JSON) and, with inline manifest lists, every snapshot's list of indices (O(snapshots x
manifests) on the wire). A load still makes an `InlineSnapshot` list per snapshot. Candidates:
decoding snapshots without JSON, delta-encoding snapshot lists on the wire, lists the fork's
`InlineSnapshot` resolves lazily, and refreshing a replica from the bytes appended since it was
read instead of from the checkpoint.
