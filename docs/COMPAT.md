---
last-verified: 2026-10-24 — fileio-catalog eb4298a on fork atomicio 9cebc695a. Every suite below on real cloud, five stores: 3,072 tests, 0 failures, 280 skipped. All runs wrote the default structural codec (see "How to update this doc").
---

# Cloud-store compatibility matrix

Status of `FileIOCatalog` cloud test suites across the
**cloud-store × commit-mode × inline-mode** grid:

- **Commit mode** — how the catalog (`LogRuntime` over `AtomicFileLog`) commits the catalog file:
  - `CAS` — every commit is a compare-and-swap replace. Requested via
    `fileio.catalog.max.append.count=0`. Supported by every
    `SupportsAtomicOperations` FileIO.
  - `append` — appends to the existing catalog file using offset-validated
    atomic writes; falls back to a CAS-mode checkpoint when the append log
    crosses `max.append.count` / `max.append.size`. Requires the FileIO to
    accept `Strategy.APPEND` (currently S3 Express, ADLS).
- **Inline mode** — how table metadata is stored:
  - `file` — catalog stores `metadata-location` pointers; `metadata.json`
    blobs live next to data (the original Iceberg layout).
  - `TM` — `fileio.catalog.inline=true`. Table metadata is embedded in the
    catalog file; no `metadata.json` is written.
  - `TM+ML` — `fileio.catalog.inline=true` plus
    `fileio.catalog.inline.manifests=true`. Manifest lists are also
    embedded; only data files and per-snapshot manifests round-trip through
    storage.
- **Inline-TM codec** — how full inline `TableMetadata` bytes are encoded.
  Selected via `fileio.catalog.inline.tm.codec` (default `structural`).
  Decode is always driven by the per-record codec discriminator on the wire,
  so the matrix run for one codec verifies that records *written* with that
  codec round-trip correctly. See
  [TM_ENCODING_BENCH_RESULTS.md](TM_ENCODING_BENCH_RESULTS.md).
  - `gzip` — Iceberg-style `Codec.GZIP` JSON. Portable / diagnostic
    fallback.
  - `structural` — purpose-built columnar layout. 40% smaller at the corpus
    median, 4–7× faster on lazy-decode workloads. Default.
  Cells in the matrix below are reported as `gzip / structural`. The cloud
  runs never selected `gzip`, so its column was not separately exercised on
  cloud storage. Locally, `TestInlineManifestEndToEnd`'s codec axis,
  `TestCatalogFaultProperties` and `TestInlineDifferentialProperties`
  write both codecs.

Cell legend:

| Code  | Meaning                                                      |
|-------|--------------------------------------------------------------|
| `OK`  | green — full `CatalogTests` + `CatalogTransactionTests` pass |
| `NR`  | not run: the suite exists, but this codec was not exercised  |
| `WIP` | partial — known failures tracked in `errata.md`              |
| `BAD` | broken                                                       |
| `--`  | no suite for this cell yet                                   |
| `NA`  | unsupported by the store                                     |

## Matrix

| Store | CAS · file | CAS · TM  | CAS · TM+ML | append · file | append · TM | append · TM+ML |
|-------|------------|-----------|-------------|---------------|-------------|----------------|
| GCS, standard bucket | OK | NR / OK | NR / OK | NA | NA | NA |
| GCS, zonal bucket    | OK | NR / OK | NR / OK | NA | NA | NA |
| S3, general-purpose bucket | OK | NR / OK | NR / OK | NA | NA | NA |
| S3 Express One Zone  | OK | NR / OK | NR / OK | OK | NR / OK | NR / OK |
| ADLS Gen2 (HNS)      | OK | NR / OK | NR / OK | OK | NR / OK | NR / OK |

The 2026-10-24 run, by store:

| Store | Suites | Tests | Failed | Skipped |
|---|---|---|---|---|
| GCS, standard bucket | `GCS*` | 438 | 0 | 40 |
| GCS, zonal bucket | `GCS*` | 438 | 0 | 40 |
| S3, general-purpose bucket | `TestS3*CAS*` | 438 | 0 | 40 |
| S3 Express One Zone | `TestS3*` | 876 | 0 | 80 |
| ADLS Gen2 (HNS) | `ADLS*` | 882 | 0 | 80 |

Each `CatalogTests` class skips 12 tests of features the suites don't
declare (`supportsEmptyNamespace`, `supportsVariant`,
`supportsServerSideRetry`); an inline one skips 2 more, which need a
`metadata.json` (`testLoadTableWithMissingMetadataFile`, and
`testRegisterTable`, errata T4). The `CatalogTransactionTests` classes
skip none.

The fork's `SupportsAtomicOperations` contract tests last passed on these
stores on 2026-10-21, at atomicio bee565b (the fork's README, "Testing on
real cloud storage"). The fork has since added only `maxAppendBytes`, which
the ADLS suites above call as each catalog opens its log.

Earlier runs: 2026-10-21 at 9f792e8, before the intention-log split, the
same counts; 2026-10-20 at 6031709, every suite green on the same
stores; 2026-05-12, 1961 tests, 0 failures, before the rebase onto
Iceberg 1.12.0, the ADLS Append Blob writes and the one-request GCS CAS.

## Notes

### GCS

- **Append is unsupported.** `GCSOutputFile.prepare` rejects
  `Strategy.APPEND` with a `Preconditions` check; the only honest
  implementation would already be a CAS replace. `GCSFileIO.supportsAppend()`
  returns false, and `AtomicFileLog` reports no append, so the catalog
  commits by CAS whatever `max.append.count` says.
- **Zonal buckets** take a different write path: each CAS is one XML API
  PUT, and regular writes (manifests, data files) an appendable upload.
  The same suites run on them with `GCS_TEST_BUCKET` set to a zonal
  bucket.
- **CAS · file** — `GCSCatalogTest`, `GCSFileIOCatalogTransactionTests`.
- **CAS · TM** — `GCSCatalogTestInlineTM`,
  `GCSFileIOCatalogTransactionTestsInlineTM`.
- **CAS · TM+ML** — `GCSCatalogTestInlineML`,
  `GCSFileIOCatalogTransactionTestsInlineML`.

### S3

- **append · file** — `TestS3Catalog`,
  `TestS3FileIOCatalogTransaction`. Runs against an S3 Express One Zone
  bucket (the only S3 surface that natively supports
  `WriteOffsetBytes`); a general-purpose bucket rejects it, so only the
  CAS suites run there.
- **CAS · file** — `TestS3CatalogCAS`,
  `TestS3FileIOCatalogTransactionCAS`.
- **CAS · TM** — `TestS3CatalogCASInlineTM`,
  `TestS3FileIOCatalogTransactionCASInlineTM`.
- **CAS · TM+ML** — `TestS3CatalogCASInlineML`,
  `TestS3FileIOCatalogTransactionCASInlineML`.
- **append · TM** — `TestS3CatalogInlineTM`,
  `TestS3FileIOCatalogTransactionInlineTM`.
- **append · TM+ML** — `TestS3CatalogInlineML`,
  `TestS3FileIOCatalogTransactionInlineML`.

### ADLS

- **append · file** — `ADLSCatalogTest`,
  `ADLSFileIOCatalogTransactionTests`. The default catalog
  configuration leaves `max.append.count` at 10 000. `ADLSFileIO` keeps
  the catalog file an Append Blob: `Strategy.APPEND` is one conditional
  `AppendBlock`, and CAS stages a temp Append Blob and renames it onto
  the catalog file.
- **CAS · file** — `ADLSCatalogTestCAS`,
  `ADLSFileIOCatalogTransactionTestsCAS`. One-line subclasses that flip
  `maxAppendCount()` to 0, mirroring the S3 CAS suites.
- **CAS · TM** — `ADLSCatalogTestCASInlineTM`,
  `ADLSFileIOCatalogTransactionTestsCASInlineTM`.
- **append · TM** — `ADLSCatalogTestInlineTM`,
  `ADLSFileIOCatalogTransactionTestsInlineTM`.
- **CAS · TM+ML** — `ADLSCatalogTestCASInlineML`,
  `ADLSFileIOCatalogTransactionTestsCASInlineML`.
- **append · TM+ML** — `ADLSCatalogTestInlineML`,
  `ADLSFileIOCatalogTransactionTestsInlineML`.

## How to update this doc

After running a meaningful slice of the cloud matrix, with the fork
published to the local Maven repository (see the
[README](../README.md#building)) and the env vars of
[INTEGRATION_TESTING.md](INTEGRATION_TESTING.md) set:

```bash
# The -Preal-cloud profile fails fast if any provider's env vars are missing.
mvn verify -Preal-cloud > target/compat.log 2>&1
```

or one provider or bucket at a time with `-Dit.test`, as
INTEGRATION_TESTING.md describes.

A `-Dfileio.catalog.inline.tm.codec=<codec>` flag on this command has
no effect: `FileIOCatalog` reads the codec only from its catalog
properties, and the cloud test bases do not pass the system property
through, so every cloud run writes the default `structural` codec. A
`gzip` column needs the bases to set `fileio.catalog.inline.tm.codec`
in their catalog properties.

Update the cell codes and bump `last-verified` in the frontmatter to
match the date of the run. Keep notes terse — link to `errata.md` for
details rather than duplicating them here.
