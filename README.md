# fileio-catalog

A storage-only [Apache Iceberg](https://iceberg.apache.org/) catalog. The
entire catalog state — namespaces, tables, optionally table metadata,
optionally manifest lists — lives in a single object in cloud storage. No
separate metastore, no Hive, no Glue, no REST.

The catalog is a state machine replicated through an intention log
(`intentlog-*`, [docs/LOG.md](docs/LOG.md)): writers append intentions,
and every reader replays them in order from a checkpoint. The object in
cloud storage is one provider of that log. The same catalog also runs on
a shared log whose checkpoints are stored apart from its records, the
shape of Kafka, a queue with an archiver, or a ledger; an in-memory one
is implemented and tested, and the others are mapped on paper.

## Goals

- **One atomic write per commit.** Cloud-storage conditional writes
  (if-match ETag, generation number, append offset) are sufficient to
  serve as the catalog's transaction primitive. A multi-table transaction
  is one append or one CAS.
- **Absorb redundant writes.** A typical Iceberg data commit writes three
  files — manifest list (rewritten in full), table metadata (rewritten in
  full), and the catalog pointer. With both inline modes enabled, a
  commit becomes one ~380-byte atomic write to the catalog object
  ([docs/sample](docs/sample/README.md)).
- **No new servers.** Anything that supports `SupportsAtomicOperations`
  (S3 with if-match, GCS with generation number, ADLS with AppendBlock)
  works as a catalog backend.

## Status

A research prototype. The on-disk format can still change without a
migration path. Catalog transactions are serializable across tables and
namespaces, and stricter than Iceberg's: a transaction fails if any table
it writes changed after it first read it, a property change included
([docs/errata.md](docs/errata.md) D12). On real S3 (general-purpose and
Express One Zone buckets), GCS (standard and zonal buckets) and ADLS
Gen2, it passes Iceberg's `CatalogTests` and `CatalogTransactionTests` in
every commit and inline mode each store supports
([docs/COMPAT.md](docs/COMPAT.md)); [Testing](#testing) describes the
fuzzers and the TLA+ model. It does not run on stock Iceberg (see the
next section).

## What's in here

```
intentlog-core/                              the intention log: replicated state machine SPI,
                                             log SPI, replay, committed set, commit loop
                                             (LogRuntime); JDK only
intentlog-memory/                            a shared log in memory, with checkpoints apart
                                             (InMemorySharedLog, CORFU-like; fault hooks)
intentlog-fileio/                            the log in one object of a store with conditional
                                             writes (AtomicFileLog, over SupportsAtomicOperations)
intentlog-testkit/                           what providers' tests share: the SharedLog contract,
                                             a state machine of entity versions, and the
                                             commit-protocol fuzzer (CommitProtocol)
iceberg-fileio-catalog/                      the catalog module (Maven), under src/:
  main/java/.../LogCatalog.java              catalog over any shared log (Iceberg Catalog,
                                             namespaces, multi-table transactions)
  main/java/.../FileIOCatalog.java           LogCatalog over an AtomicFileLog
  main/java/.../CatalogLog.java              the catalog's log: reads, commits, Iceberg errors
  main/java/.../CatalogMutation.java         a transaction, staged on a read, as an intention
  main/java/.../CatalogStateMachine.java     the catalog as a state machine (CatalogState)
  main/java/.../ProtoCodec.java              wire encoding for catalog
  main/java/.../InlineDeltaCodec.java        wire encoding for TM/ML deltas
  main/proto/catalog.proto                   protobuf schema
docs/LOG.md                                  the intention log: SPIs, envelope, replay, GC,
                                             commit loop, compaction, providers
docs/ATOMIC_FILE_LOG.md                      the catalog file: AtomicFileLog's layout and commits
docs/SPEC.md                                 the catalog's state machine: checkpoint, actions
docs/SPEC_TM.md                              inline table metadata
docs/SPEC_ML.md                              inline manifest lists
docs/design.md                               invariants, conflict matrix, best practices
docs/errata.md                               current shortcuts, gaps, known unknowns
docs/GC.md                                   retention of the committed-transaction set
docs/COMPAT.md                               cloud-store x commit-mode x inline-mode results
docs/INTEGRATION_TESTING.md                  running the suites on emulators or real cloud
docs/ICEBERG_ISSUES.md                       Iceberg core bugs the fuzzers found, and their fixes
docs/iceberg-issues/                         one document per Iceberg issue, for upstream reports
docs/format-explorer/                        byte-level viewer of catalog files
docs/sample/                                 measured footprint of each inline mode
docs/REPLAY_BENCH_RESULTS.md                 replay throughput, read and load (JMH ReplayBench)
spec/tla/                                    TLA+ models of the commit loop and the fuzzers' oracles
bench/                                       the multi-writer contention driver's campaigns: configs,
                                             run.sh, shards on another host (havoc/), endive's
                                             predictions (endive/); results in bench/results/
lab/                                         labbook over the campaigns' results (DuckDB, MyST pages)
```

## Dependencies on the Iceberg fork

This project does not run against stock Iceberg. It consumes the fork at
[github.com/cdouglas/iceberg](https://github.com/cdouglas/iceberg), branch
`atomicio`, as a Maven `1.12.0-atomicio-SNAPSHOT` artifact: the fork is
based on the Iceberg 1.12.0 release and names it in its version. That
branch carries both the atomic-operations hooks used by the base catalog
and the manifest-list-inlining hooks used by `inline.manifests`.

The fork adds these extension points. Its other changes to stock Iceberg
include the fixes for core bugs C2 to C25 (branch `core-fixes`, which
`atomicio` sits on; see [docs/ICEBERG_ISSUES.md](docs/ICEBERG_ISSUES.md)),
`TableMetadata.Builder.setLastUpdatedMillis` for inline replay, a guard
that makes `RewriteTablePathUtil` reject inline-ML snapshots, catalog
transactions in `RESTCatalog`, and the fork's version
(`<release>-atomicio-SNAPSHOT`).

| Addition | File | What it provides |
|----------|------|------------------|
| `SupportsAtomicOperations` | `api/.../SupportsAtomicOperations.java` | FileIO extension with `AtomicOutputFile`, `CAS` / `APPEND` strategies, `CASException` (`StorageInvariantException`, `StorageThrottleException`), `AppendException`, `SegmentLimitException`; `supportsAppend()` and `maxAppendBytes(location)` |
| `AtomicOutputFile`         | `api/.../AtomicOutputFile.java` | `CAS prepare(Supplier<InputStream>, Strategy)` + `InputFile writeAtomic(CAS, Supplier<InputStream>)` |
| `FileChecksum`             | `api/.../FileChecksum.java` | Content checksum (`update`, `contentChecksumBytes`, `contentHeaderString`). The ETag / generation a write replaces is pinned by the `InputFile` passed to `newOutputFile(InputFile)` |
| `S3FileIO` atomic ops      | `aws/.../S3FileIO.java` | If-Match / If-None-Match conditional puts; APPEND at an offset on S3 Express |
| `GCSFileIO` atomic ops     | `gcp/.../GCSFileIO.java` | Generation-number conditional puts, one request per CAS (an XML API PUT on zonal buckets) |
| `ADLSFileIO` atomic ops    | `azure/.../ADLSFileIO.java` | APPEND: one conditional `AppendBlock`. CAS: stage an Append Blob, then rename it onto the target with If-Match |
| `SupportsCatalogTransactions`, `BaseCatalogTransaction` | `core/.../catalog/` | Multi-table transaction API. `commitTransaction` takes a read set: each table the transaction writes and, under serializable isolation, each branch it read, as it first read them |
| `ManifestListSink`         | `core/.../ManifestListSink.java` | Hook for `SnapshotProducer` to deliver finalized manifest list deltas instead of writing `snap-*.avro` |
| `InlineSnapshot`           | `core/.../InlineSnapshot.java` | `Snapshot` whose `manifestListLocation()` is null and whose manifests are held in memory |

`InlineSnapshot` integration also requires a small builder change
(`TableMetadata.Builder.replaceSnapshots`) and a loosened `BaseSnapshot.equals`
in core, both listed in [docs/SPEC_ML.md](docs/SPEC_ML.md) ("Iceberg-Core
Hooks") and tracked as integration debt in
[docs/errata.md](docs/errata.md) (S1). The catalog-transaction API is
errata S2.

## Building

The fork builds with JDK 17 or 21, and this project targets Java 17.
Publish the fork to the local Maven repository first:

```bash
git clone --branch atomicio https://github.com/cdouglas/iceberg.git
cd iceberg
./gradlew publishToMavenLocal -x test -x integrationTest
```

This also publishes the fork's `tests` jars, from which this project
runs Iceberg's abstract `CatalogTests` and `CatalogTransactionTests`.
Then, in this repository:

```bash
mvn clean install
```

The root `pom.xml` is a reactor over the modules; build and test from the
root. `mvn test -Dtest=TestProtoActions` runs one class in whichever module
holds it, and `mvn -pl iceberg-fileio-catalog ...` runs a goal in one module
(add `-am` to build the modules it depends on).

`mvn test` runs unit tests, the in-memory end-to-end suites and the
property tests. `mvn verify` adds the cloud integration suites (S3, GCS,
ADLS); these need credentials or emulators (versitygw, fake-gcs-server,
Azurite). On Azurite, which has no Data Lake rename, the ADLS catalog
suites skip; see [docs/INTEGRATION_TESTING.md](docs/INTEGRATION_TESTING.md).

## Testing

- **Iceberg's catalog suites.** `TestLocalCatalog` and
  `TestLocalCatalogTransaction` run `CatalogTests` and
  `CatalogTransactionTests` in every local mode (append, CAS and
  compaction, each with pointer, inline TM and inline ML tables) over
  `InMemoryAtomicFileIO`, which enforces the conditions of CAS and
  append writes, and on a shared log (`InMemorySharedLog`) in each table
  mode. The cloud suites run the same tests against each store
  ([docs/INTEGRATION_TESTING.md](docs/INTEGRATION_TESTING.md)).
- **Property tests** ([jqwik](https://jqwik.net/)), in `mvn test`:
  - `TestCatalogFaultProperties` runs concurrent writers, each in its own
    commit and inline mode, through Iceberg's table and catalog APIs on
    storage that fails, throttles, lands a write but loses its reply, or
    reports a landed write as a conflict. After each operation the
    catalog must match a model: an acknowledged operation took effect
    once, a failed one not at all, an unknown one entirely or not at all,
    and every table loads and keeps its row lineage. Its javadoc lists
    the operations and checks.
  - `TestCatalogRaceProperties` runs other operations just before an
    operation's catalog writes, and requires the result of a serial run.
    It and `TestCatalogFaultProperties` draw the catalog's log: one
    object, or a shared log.
  - `TestInlineDifferentialProperties` runs one schedule of table
    operations on a pointer catalog and an inline one, and requires the
    same results and reads; `TestInlineDeltaProperties` checks that every
    inline delta replays to the metadata it committed.
  - `spec/` checks the format itself: concurrent writers (on one object
    and on a shared log, with copies of records appended again, and
    sends that land late), invariants, codecs, and canonical state.
  - `CommitProtocol` (intentlog-testkit) fuzzes the commit protocol on
    each provider (`TestMemoryCommitProtocol`,
    `TestAtomicFileCommitProtocol`), over a state machine of entity
    versions: faults, compactors and GC records inside commits, stale
    reads, and copies of any record that ever landed, which must never
    apply again. `SharedLogContract` holds each provider to the SharedLog
    obligations.

  - The conflict encodings: each one's test extends
    `ConflictEncodingContract` (deterministic, stable, total), and
    `TestRebaseProperties` runs an operation after commits it did not
    see, under the encoding and under a reference that lets Iceberg
    retry: a record the encoding merges must be what the retry commits.
    `TestConflictMatrix` pins the per-encoding matrix of design.md.

  Each property's `tries` is in its `@Property` annotation; raise it for
  a longer run. Every bug the fuzzers or reviews found in this project
  has a regression test in `TestCatalogRegressions` (Bug *N* in commit
  subjects). Bugs they found in Iceberg itself are in
  [docs/ICEBERG_ISSUES.md](docs/ICEBERG_ISSUES.md), and
  `TestIcebergCoreIssues` checks that each is still fixed.
- **Format checks.** `CheckpointRoundTrip` checks every catalog file the
  suites and fuzzers write against `catalog.proto`, and that its replayed
  state equals a checkpoint of it. `TestFormatExplorer` checks the codecs'
  field numbers against `catalog.proto`.
- **Model checking.** [spec/tla/](spec/tla/README.md) models the commit
  loop in TLA+, on one catalog object and on a shared log with
  checkpoints apart from it, and states the fuzzers' oracles over the
  models. The commit-loop bugs the fuzzers and reviews found are switches
  in the model that put each bug back; each one must break a property,
  and the fuzzers' oracles must catch it.

## Benchmarks

- **Replay.** `ReplayBench` (JMH, in the catalog module's tests) measures
  how fast a reader tests and applies records, and what a read and a
  table load cost ([docs/REPLAY_BENCH_RESULTS.md](docs/REPLAY_BENCH_RESULTS.md)).
- **Contention.** The contention driver (`org.apache.iceberg.io.contention`,
  test scope) runs N writers, each with its own catalog, over an
  in-memory store that injects a provider's latencies (endive's
  profiles), under a workload of appends, overwrites, compactions and
  property changes, and records every operation. `bench/run.sh` runs a
  campaign of configurations locally; `bench/havoc/` packs one to run as
  shards in containers on another host. [lab/](lab/NOTES.md) ingests the
  results with labbook and compares them with the predictions of
  endive, a discrete-event simulator of Iceberg's optimistic commits
  across cloud stores (`bench/endive/` runs a checkout of it beside this
  one, or at `ENDIVE`).

## Configuration

| Property                          | Default       | Effect |
|-----------------------------------|---------------|--------|
| `fileio.catalog.inline`           | `false`       | Inline table metadata (no `metadata.json`) |
| `fileio.catalog.inline.manifests` | `false`       | Inline manifest lists (no `snap-*.avro`); requires `inline=true` |
| `fileio.catalog.inline.strict`    | `false`       | Reject loading a table with both inline and file manifest lists, instead of logging a warning |
| `fileio.catalog.inline.max.update.size` | `4190208` | Largest inline table update (a delta, or full metadata); a table whose update does not fit gets a `metadata.json` and leaves inline storage until an update fits in full |
| `fileio.catalog.inline.tm.codec`  | `structural`  | Codec for newly-written inline `TableMetadata` bytes: `structural` (binary, columnar; default) or `gzip` (Iceberg-style gzipped JSON). Decode follows the on-wire codec tag, so writers can change this setting freely |
| `fileio.catalog.max.append.count` | `100`         | Soft limit on log records before CAS compaction. `0` forces CAS-only mode (S3 standard, GCS) |
| `fileio.catalog.max.append.size`  | `16777216`    | Soft target for catalog-file size before compaction |
| `fileio.catalog.committed.retention.ms` | `21600000` (6h) | Minimum age before a committed-txn UUID becomes eligible for GC at compaction. `Long.MAX_VALUE` disables; `0` GCs aggressively (not recommended) |
| `fileio.catalog.throttle.retry.min-wait-ms` | `200` | First wait before retrying a write that storage throttled; each later wait doubles, up to 5 s |
| `fileio.catalog.race.retry.min-wait-ms` | `20` | Bound of the random wait after a commit loses a race for the catalog, before it reads again; each later loss doubles it, up to 1 s. `0` resends at once |
| `fileio.catalog.conflict.encoding` | `manifest-window` | How replay decides whether a table update conflicts with what landed since its writer read the table; fixed when the catalog is created ([docs/SPEC.md](docs/SPEC.md) "Conflict Encodings") |

See [docs/LOG.md](docs/LOG.md) for the commit protocol,
[docs/ATOMIC_FILE_LOG.md](docs/ATOMIC_FILE_LOG.md) for how the catalog file
commits, and [docs/design.md](docs/design.md) for guidance on choosing
append vs CAS and which inline mode and codec to enable.
