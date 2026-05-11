# fileio-catalog

A storage-only [Apache Iceberg](https://iceberg.apache.org/) catalog. The
entire catalog state — namespaces, tables, optionally table metadata,
optionally manifest lists — lives in a single object in cloud storage. No
separate metastore, no Hive, no Glue, no REST.

## Goals

- **One atomic write per commit.** Cloud-storage conditional writes
  (if-match ETag, generation number, append offset) are sufficient to
  serve as the catalog's transaction primitive. A multi-table transaction
  is one append or one CAS.
- **Absorb redundant writes.** A typical Iceberg data commit writes three
  files — manifest list (rewritten in full), table metadata (rewritten in
  full), and the catalog pointer. With both inline modes enabled, a
  commit becomes one ~250-byte atomic write to the catalog object.
- **No new servers.** Anything that supports `SupportsAtomicOperations`
  (S3 with if-match, GCS with generation number, ADLS with AppendBlock)
  works as a catalog backend.

## What's in here

```
src/main/java/.../FileIOCatalog.java         catalog implementation
src/main/java/.../ProtoCatalogFormat.java    on-disk format (PB)
src/main/java/.../ProtoCodec.java            wire encoding for catalog
src/main/java/.../InlineDeltaCodec.java      wire encoding for TM/ML deltas
src/main/proto/catalog.proto                 protobuf schema
docs/SPEC.md                                 base format spec
docs/SPEC_TM.md                              inline table metadata
docs/SPEC_ML.md                              inline manifest lists
docs/design.md                               invariants, conflict matrix, best practices
docs/errata.md                               current shortcuts, gaps, known unknowns
```

## Dependencies on the Iceberg fork

This project does not run against stock Iceberg. It consumes the fork at
[github.com/cdouglas/iceberg](https://github.com/cdouglas/iceberg) (locally
at `../iceberg`, branch `atomicio`) as a Maven `1.11.0-SNAPSHOT` artifact.
That branch carries both the atomic-operations hooks used by the base
catalog and the manifest-list-inlining hooks used by `inline.manifests`.

The fork adds these extension points; everything else is stock Iceberg.

| Addition | File | What it provides |
|----------|------|------------------|
| `SupportsAtomicOperations` | `api/.../SupportsAtomicOperations.java` | FileIO extension with `AtomicOutputFile`, `CAS` / `APPEND` strategies, `CASException` / `AppendException` |
| `AtomicOutputFile`         | `api/.../AtomicOutputFile.java` | `prepare(Strategy)` + `writeAtomic(token)` |
| `FileChecksum`             | `api/.../FileChecksum.java` | Provider-specific token (ETag / generation / offset) |
| `S3FileIO` atomic ops      | `aws/.../S3FileIO.java` | If-match conditional puts |
| `GCSFileIO` atomic ops     | `gcp/.../GCSFileIO.java` | Generation-number conditional puts |
| `ADLSFileIO` atomic ops    | `azure/.../ADLSFileIO.java` | `AppendBlock` + conditional ETag puts |
| `SupportsCatalogTransactions`, `BaseCatalogTransaction` | `core/.../catalog/` | Multi-table transaction API |
| `ManifestListSink` (ML branch) | `core/.../ManifestListSink.java` | Hook for `SnapshotProducer` to deliver finalized manifest list deltas instead of writing `snap-*.avro` |
| `InlineSnapshot` (ML branch) | `core/.../InlineSnapshot.java` | `Snapshot` whose `manifestListLocation()` is null and whose manifests are held in memory |

`InlineSnapshot` integration also requires a small builder change
(`TableMetadata.Builder.replaceSnapshots`) and a loosened `BaseSnapshot.equals`
in core; both are tracked as integration debt in
[docs/errata.md](docs/errata.md).

## Building

The fileio-catalog depends on Iceberg SNAPSHOT artifacts. Always build
iceberg first:

```bash
cd ../iceberg
./gradlew publishToMavenLocal -x test -x integrationTest -x generateGitProperties

cd ../fileio-catalog
mvn clean install
```

`mvn test` runs unit + in-memory end-to-end tests. `mvn verify` adds the
cloud integration suites (S3, GCS, ADLS); these need credentials or
emulators (LocalStack, fake-gcs-server, Azurite).

## Configuration

| Property                          | Default       | Effect |
|-----------------------------------|---------------|--------|
| `fileio.catalog.inline`           | `false`       | Inline table metadata (no `metadata.json`) |
| `fileio.catalog.inline.manifests` | `false`       | Inline manifest lists (no `snap-*.avro`); requires `inline=true` |
| `fileio.catalog.inline.tm.codec`  | `structural`  | Codec for newly-written inline `TableMetadata` bytes: `structural` (binary, columnar; default) or `gzip` (Iceberg-style gzipped JSON). Decode follows the on-wire codec tag, so writers can change this setting freely |
| `fileio.catalog.max.append.count` | `10000`       | Hard limit on log records before CAS compaction. `0` forces CAS-only mode (S3 standard, GCS) |
| `fileio.catalog.max.append.size`  | `16777216`    | Soft target for catalog-file size before compaction |
| `fileio.catalog.committed.retention.ms` | `21600000` (6h) | Minimum age before a committed-txn UUID becomes eligible for GC at compaction. `Long.MAX_VALUE` disables; `0` GCs aggressively (not recommended) |

See [docs/SPEC.md](docs/SPEC.md) for the commit protocol and
[docs/design.md](docs/design.md) for guidance on choosing append vs CAS
and which inline mode and codec to enable.
