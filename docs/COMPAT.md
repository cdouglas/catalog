---
last-verified: 2026-05-12 — full real-cloud matrix run under both gzip and structural inline-TM codecs (1961 tests, 0 failures, 163 skipped per codec).
---

# Cloud-store compatibility matrix

Status of `FileIOCatalog` cloud test suites across the
**atomic-mode × inline-mode** grid:

- **Atomic mode** — how `ProtoCatalogFormat` commits the catalog file:
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
  Cells in the matrix below are reported as `gzip / structural`. The two
  codecs share the same code paths apart from the encode/decode methods,
  so a regression in one is generally also a regression in the other; the
  per-codec column is a redundancy check.

Cell legend:

| Code  | Meaning                                                      |
|-------|--------------------------------------------------------------|
| `OK`  | green — full `CatalogTests` + `CatalogTransactionTests` pass |
| `WIP` | partial — known failures tracked in `INLINE_STABILIZATION.md`|
| `BAD` | broken                                                       |
| `--`  | no suite for this cell yet                                   |
| `NA`  | unsupported by the store                                     |

## Matrix

| Store | CAS · file | CAS · TM | CAS · TM+ML | append · file | append · TM | append · TM+ML |
|-------|------------|----------|-------------|---------------|-------------|----------------|
| GCS   | OK         | OK / OK  | OK / OK     | NA            | NA          | NA             |
| S3    | OK         | OK / OK  | OK / OK     | OK            | OK / OK     | OK / OK        |
| ADLS  | OK         | OK / OK  | OK / OK     | OK            | OK / OK     | OK / OK        |

## Notes

### GCS

- **Append is unsupported.** `GCSOutputFile.prepare` rejects
  `Strategy.APPEND` with a `Preconditions` check; the only honest
  implementation would already be a CAS replace. `ProtoCatalogFormat`
  detects this at init and coerces `max.append.count=0`.
- **CAS · file (OK)** — `GCSCatalogTest`,
  `GCSFileIOCatalogTransactionTests`.
- **CAS · TM (OK)** — `GCSCatalogTestInlineTM`,
  `GCSFileIOCatalogTransactionTestsInlineTM`.
- **CAS · TM+ML (OK)** — `GCSCatalogTestInlineML`,
  `GCSFileIOCatalogTransactionTestsInlineML`.

### S3

- **CAS · file (OK)** — `TestS3CatalogCAS`,
  `TestS3FileIOCatalogTransactionCAS`.
- **append · file (OK)** — `TestS3Catalog`,
  `TestS3FileIOCatalogTransaction`. Runs against an S3 Express One Zone
  bucket (the only S3 surface that natively supports
  `WriteOffsetBytes`).
- **CAS · TM (OK)** — `TestS3CatalogCASInlineTM`,
  `TestS3FileIOCatalogTransactionCASInlineTM`.
- **CAS · TM+ML (OK)** — `TestS3CatalogCASInlineML`,
  `TestS3FileIOCatalogTransactionCASInlineML`.
- **append · TM (OK)** — `TestS3CatalogInlineTM`,
  `TestS3FileIOCatalogTransactionInlineTM`.
- **append · TM+ML (OK)** — `TestS3CatalogInlineML`,
  `TestS3FileIOCatalogTransactionInlineML`.

### ADLS

- **append · file (OK)** — `ADLSCatalogTest`,
  `ADLSFileIOCatalogTransactionTests`. The default `ProtoCatalogFormat`
  configuration leaves `max.append.count` at 10 000, and `ADLSFileIO`
  honours `Strategy.APPEND` via lease-protected appends in
  `ADLSOutputFile`.
- **CAS · file (OK)** — `ADLSCatalogTestCAS`,
  `ADLSFileIOCatalogTransactionTestsCAS`. One-line subclasses that flip
  `maxAppendCount()` to 0, mirroring the S3 CAS suites.
- **CAS · TM (OK)** — `ADLSCatalogTestCASInlineTM`,
  `ADLSFileIOCatalogTransactionTestsCASInlineTM`.
- **append · TM (OK)** — `ADLSCatalogTestInlineTM`,
  `ADLSFileIOCatalogTransactionTestsInlineTM`.
- **CAS · TM+ML (OK)** — `ADLSCatalogTestCASInlineML`,
  `ADLSFileIOCatalogTransactionTestsCASInlineML`.
- **append · TM+ML (OK)** — `ADLSCatalogTestInlineML`,
  `ADLSFileIOCatalogTransactionTestsInlineML`.

## How to update this doc

After running a meaningful slice of the cloud matrix:

```bash
cd /home/chris/work/catalog/iceberg && \
  ./gradlew publishToMavenLocal -x test -x integrationTest -x generateGitProperties

cd /home/chris/work/catalog/fileio-catalog

# Run once per codec (gzip then structural). The same inline-TM suites run
# under both — only the encoded bytes on disk change. The -Preal-cloud profile
# fails fast if any provider's env vars are missing.
for c in gzip structural; do
  mvn verify -Preal-cloud -Dfileio.catalog.inline.tm.codec=$c \
    > target/compat-$c.log 2>&1
done
```

The `inline.tm.codec` system property only affects the encode path. Decode
is always driven by the per-record codec discriminator written on the wire,
so the run verifies that bytes *written* with the configured codec
round-trip correctly through every cell in the matrix.

Update the cell codes and bump `last-verified` in the frontmatter to
match the date of the run. Keep notes terse — link to the work-tracking
doc (`INLINE_STABILIZATION.md`, `R4.md`, `errata.md`) for details rather
than duplicating them here.
