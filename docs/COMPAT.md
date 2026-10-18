---
last-verified: 2026-05-12 — full real-cloud matrix run twice (1961 tests, 0 failures, 163 skipped each). Both runs wrote the default structural codec (see "How to update this doc"). Not re-run since; every cell is unverified on the current code.
---

# Cloud-store compatibility matrix

Status of `FileIOCatalog` cloud test suites across the
**cloud-store × commit-mode × inline-mode** grid:

- **Commit mode** — how `ProtoCatalogFormat` commits the catalog file:
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
| `OK*` | passed on 2026-05-12, not re-run since (see below)           |
| `NR`  | not run: the suite exists, but this codec was not exercised  |
| `WIP` | partial — known failures tracked in `errata.md`              |
| `BAD` | broken                                                       |
| `--`  | no suite for this cell yet                                   |
| `NA`  | unsupported by the store                                     |

## Matrix

| Store | CAS · file | CAS · TM  | CAS · TM+ML | append · file | append · TM | append · TM+ML |
|-------|------------|-----------|-------------|---------------|-------------|----------------|
| GCS   | OK*        | NR / OK*  | NR / OK*    | NA            | NA          | NA             |
| S3    | OK*        | NR / OK*  | NR / OK*    | OK*           | NR / OK*    | NR / OK*       |
| ADLS  | OK*        | NR / OK*  | NR / OK*    | OK*           | NR / OK*    | NR / OK*       |

Since the 2026-05-12 run the code under every cell has changed: for
all stores, the fork's rebase onto Iceberg 1.12.0 (9296e10) and the
fixes for Bugs 10-64; for ADLS, the catalog file kept an Append Blob
with CAS by stage and rename (fork 7a0d5edf2); for GCS, each CAS in one
request (fork 8f94c5100). The emulator runs also moved from MinIO to
versitygw (a549bcf).

## Notes

### GCS

- **Append is unsupported.** `GCSOutputFile.prepare` rejects
  `Strategy.APPEND` with a `Preconditions` check; the only honest
  implementation would already be a CAS replace. `GCSFileIO.supportsAppend()`
  returns false, and `ProtoCatalogFormat` checks it on each commit and
  commits by CAS whatever `max.append.count` says.
- **CAS · file (OK*)** — `GCSCatalogTest`,
  `GCSFileIOCatalogTransactionTests`.
- **CAS · TM (OK*)** — `GCSCatalogTestInlineTM`,
  `GCSFileIOCatalogTransactionTestsInlineTM`.
- **CAS · TM+ML (OK*)** — `GCSCatalogTestInlineML`,
  `GCSFileIOCatalogTransactionTestsInlineML`.

### S3

- **CAS · file (OK*)** — `TestS3CatalogCAS`,
  `TestS3FileIOCatalogTransactionCAS`.
- **append · file (OK*)** — `TestS3Catalog`,
  `TestS3FileIOCatalogTransaction`. Runs against an S3 Express One Zone
  bucket (the only S3 surface that natively supports
  `WriteOffsetBytes`).
- **CAS · TM (OK*)** — `TestS3CatalogCASInlineTM`,
  `TestS3FileIOCatalogTransactionCASInlineTM`.
- **CAS · TM+ML (OK*)** — `TestS3CatalogCASInlineML`,
  `TestS3FileIOCatalogTransactionCASInlineML`.
- **append · TM (OK*)** — `TestS3CatalogInlineTM`,
  `TestS3FileIOCatalogTransactionInlineTM`.
- **append · TM+ML (OK*)** — `TestS3CatalogInlineML`,
  `TestS3FileIOCatalogTransactionInlineML`.

### ADLS

- **append · file (OK*)** — `ADLSCatalogTest`,
  `ADLSFileIOCatalogTransactionTests`. The default `ProtoCatalogFormat`
  configuration leaves `max.append.count` at 10 000. `ADLSFileIO` keeps
  the catalog file an Append Blob: `Strategy.APPEND` is one conditional
  `AppendBlock`, and CAS stages a temp Append Blob and renames it onto
  the catalog file (fork 7a0d5edf2, after the 2026-05-12 run).
- **CAS · file (OK*)** — `ADLSCatalogTestCAS`,
  `ADLSFileIOCatalogTransactionTestsCAS`. One-line subclasses that flip
  `maxAppendCount()` to 0, mirroring the S3 CAS suites.
- **CAS · TM (OK*)** — `ADLSCatalogTestCASInlineTM`,
  `ADLSFileIOCatalogTransactionTestsCASInlineTM`.
- **append · TM (OK*)** — `ADLSCatalogTestInlineTM`,
  `ADLSFileIOCatalogTransactionTestsInlineTM`.
- **CAS · TM+ML (OK*)** — `ADLSCatalogTestCASInlineML`,
  `ADLSFileIOCatalogTransactionTestsCASInlineML`.
- **append · TM+ML (OK*)** — `ADLSCatalogTestInlineML`,
  `ADLSFileIOCatalogTransactionTestsInlineML`.

## How to update this doc

After running a meaningful slice of the cloud matrix:

```bash
cd /home/chris/work/catalog/iceberg && \
  ./gradlew publishToMavenLocal -x test -x integrationTest

cd /home/chris/work/catalog/fileio-catalog

# The -Preal-cloud profile fails fast if any provider's env vars are missing.
mvn verify -Preal-cloud > target/compat.log 2>&1
```

A `-Dfileio.catalog.inline.tm.codec=<codec>` flag on this command has
no effect: `FileIOCatalog` reads the codec only from its catalog
properties, and the cloud test bases do not pass the system property
through, so every cloud run writes the default `structural` codec. A
`gzip` column needs the bases to set `fileio.catalog.inline.tm.codec`
in their catalog properties.

Update the cell codes and bump `last-verified` in the frontmatter to
match the date of the run. Keep notes terse — link to `errata.md` for
details rather than duplicating them here.
