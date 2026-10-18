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
  commit becomes one ~380-byte atomic write to the catalog object
  ([docs/sample](docs/sample/README.md)).
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
docs/ICEBERG_ISSUES.md                       Iceberg core bugs the fuzzers found, and their fixes
docs/format-explorer/                        byte-level viewer of catalog files
```

## Dependencies on the Iceberg fork

This project does not run against stock Iceberg. It consumes the fork at
[github.com/cdouglas/iceberg](https://github.com/cdouglas/iceberg) (locally
at `../iceberg`, branch `atomicio`) as a Maven `1.12.0-atomicio-SNAPSHOT`
artifact: the fork is based on the Iceberg 1.12.0 release and names it in its
version. That branch carries both the atomic-operations hooks used by the base
catalog and the manifest-list-inlining hooks used by `inline.manifests`.

The fork adds these extension points. Its other changes to stock Iceberg
include the fixes for core bugs C2 to C7 (branch `core-fixes`, see
[docs/ICEBERG_ISSUES.md](docs/ICEBERG_ISSUES.md)),
`TableMetadata.Builder.setLastUpdatedMillis` for inline replay, a guard
that makes `RewriteTablePathUtil` reject inline-ML snapshots, catalog
transactions in `RESTCatalog`, and the fork's version
(`<release>-atomicio-SNAPSHOT`).

| Addition | File | What it provides |
|----------|------|------------------|
| `SupportsAtomicOperations` | `api/.../SupportsAtomicOperations.java` | FileIO extension with `AtomicOutputFile`, `CAS` / `APPEND` strategies, `CASException` / `AppendException` |
| `AtomicOutputFile`         | `api/.../AtomicOutputFile.java` | `CAS prepare(Supplier<InputStream>, Strategy)` + `InputFile writeAtomic(CAS, Supplier<InputStream>)` |
| `FileChecksum`             | `api/.../FileChecksum.java` | Content checksum (`update`, `contentChecksumBytes`, `contentHeaderString`). The ETag / generation a write replaces is pinned by the `InputFile` passed to `newOutputFile(InputFile)` |
| `S3FileIO` atomic ops      | `aws/.../S3FileIO.java` | If-Match / If-None-Match conditional puts; APPEND at an offset on S3 Express |
| `GCSFileIO` atomic ops     | `gcp/.../GCSFileIO.java` | Generation-number conditional puts |
| `ADLSFileIO` atomic ops    | `azure/.../ADLSFileIO.java` | APPEND: one conditional `AppendBlock`. CAS: stage an Append Blob, then rename it onto the target with If-Match |
| `SupportsCatalogTransactions`, `BaseCatalogTransaction` | `core/.../catalog/` | Multi-table transaction API |
| `ManifestListSink`         | `core/.../ManifestListSink.java` | Hook for `SnapshotProducer` to deliver finalized manifest list deltas instead of writing `snap-*.avro` |
| `InlineSnapshot`           | `core/.../InlineSnapshot.java` | `Snapshot` whose `manifestListLocation()` is null and whose manifests are held in memory |

`InlineSnapshot` integration also requires a small builder change
(`TableMetadata.Builder.replaceSnapshots`) and a loosened `BaseSnapshot.equals`
in core, both listed in [docs/SPEC_ML.md](docs/SPEC_ML.md) ("Iceberg-Core
Hooks") and tracked as integration debt in
[docs/errata.md](docs/errata.md) (S1).

## Building

The fileio-catalog depends on Iceberg SNAPSHOT artifacts. Always build
iceberg first:

```bash
cd ../iceberg
./gradlew publishToMavenLocal -x test -x integrationTest

cd ../fileio-catalog
mvn clean install
```

`mvn test` runs unit + in-memory end-to-end tests. `mvn verify` adds the
cloud integration suites (S3, GCS, ADLS); these need credentials or
emulators (versitygw, fake-gcs-server, Azurite). On Azurite, which has no
Data Lake rename, the ADLS catalog suites skip; see
[docs/INTEGRATION_TESTING.md](docs/INTEGRATION_TESTING.md).

## Configuration

| Property                          | Default       | Effect |
|-----------------------------------|---------------|--------|
| `fileio.catalog.inline`           | `false`       | Inline table metadata (no `metadata.json`) |
| `fileio.catalog.inline.manifests` | `false`       | Inline manifest lists (no `snap-*.avro`); requires `inline=true` |
| `fileio.catalog.inline.strict`    | `false`       | Reject loading a table with both inline and file manifest lists, instead of logging a warning |
| `fileio.catalog.inline.max.update.size` | `4190208` | Largest inline table update (a delta, or full metadata); a table whose update does not fit gets a `metadata.json` and leaves inline storage until an update fits in full |
| `fileio.catalog.inline.tm.codec`  | `structural`  | Codec for newly-written inline `TableMetadata` bytes: `structural` (binary, columnar; default) or `gzip` (Iceberg-style gzipped JSON). Decode follows the on-wire codec tag, so writers can change this setting freely |
| `fileio.catalog.max.append.count` | `10000`       | Soft limit on log records before CAS compaction. `0` forces CAS-only mode (S3 standard, GCS) |
| `fileio.catalog.max.append.size`  | `16777216`    | Soft target for catalog-file size before compaction |
| `fileio.catalog.committed.retention.ms` | `21600000` (6h) | Minimum age before a committed-txn UUID becomes eligible for GC at compaction. `Long.MAX_VALUE` disables; `0` GCs aggressively (not recommended) |
| `fileio.catalog.throttle.retry.min-wait-ms` | `200` | First wait before retrying a write that storage throttled; each later wait doubles, up to 5 s |

See [docs/SPEC.md](docs/SPEC.md) for the commit protocol and
[docs/design.md](docs/design.md) for guidance on choosing append vs CAS
and which inline mode and codec to enable.
