---
last-verified: 2026-05-05 (matrix re-verification)
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
| GCS   | OK         | OK       | WIP         | NA            | NA          | NA             |
| S3    | OK         | OK       | WIP         | OK            | OK          | --             |
| ADLS  | OK         | OK       | --          | OK            | OK          | --             |

## Notes

### GCS

- **Append is unsupported.** `GCSOutputFile.prepare` rejects
  `Strategy.APPEND` with a `Preconditions` check; the only honest
  implementation would already be a CAS replace. `ProtoCatalogFormat`
  detects this at init and coerces `max.append.count=0` (commits
  `e15051d`, `a4cff40`).
- **CAS · file (OK)** — `GCSCatalogTest`,
  `GCSFileIOCatalogTransactionTests`.
- **CAS · TM (OK)** — `GCSCatalogTestInlineTM`,
  `GCSFileIOCatalogTransactionTestsInlineTM`. Verified 2026-05-05 after
  the inline-replay determinism fixes (commits `804465b`, `45c88c5`,
  `b616131`, `baac33b`) and the v1 sequence-number gate (`ec220c2`).
- **CAS · TM+ML (WIP)** — `GCSCatalogTestInlineML`,
  `GCSFileIOCatalogTransactionTestsInlineML`. Not exercised in the
  current matrix run; revisit after we extend verification to the ML
  cells.

### S3

- **CAS · file (OK)** — `TestS3CatalogCAS`,
  `TestS3FileIOCatalogTransactionCAS`.
- **append · file (OK)** — `TestS3Catalog`,
  `TestS3FileIOCatalogTransaction`. Runs against an S3 Express One Zone
  bucket (the only S3 surface that natively supports
  `WriteOffsetBytes`).
- **CAS · TM (OK)** — `TestS3CatalogCASInlineTM`,
  `TestS3FileIOCatalogTransactionCASInlineTM`. 102/102 + 28/28 verified
  2026-05-05.
- **CAS · TM+ML (WIP)** — `TestS3CatalogCASInlineML`,
  `TestS3FileIOCatalogTransactionCASInlineML`. Not exercised in the
  current matrix run.
- **append · TM (OK)** — `TestS3CatalogInlineTM`,
  `TestS3FileIOCatalogTransactionInlineTM` (added 2026-05-05).
  Uncovered the `addInlineTable` nextTableId-monotonicity bug
  (commit `85bd9ac`); now 102/102 + 28/28.
- **append · TM+ML (--)** — no suite yet; trivial to add as a one-line
  override on `TestS3CatalogInlineTM` once the CAS · TM+ML cell goes
  green.

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
  `ADLSFileIOCatalogTransactionTestsCASInlineTM` (added 2026-05-05).
- **append · TM (OK)** — `ADLSCatalogTestInlineTM`,
  `ADLSFileIOCatalogTransactionTestsInlineTM` (added 2026-05-05).
- **TM+ML for both atomic modes (--)** — no suites yet. The TM cells'
  health on both atomic modes makes adding them mechanical when we get
  to inline-ML matrix verification.

## How to update this doc

After running a meaningful slice of the cloud matrix:

```bash
cd /home/chris/work/catalog/iceberg && \
  ./gradlew :iceberg-core:publishToMavenLocal -x test -x integrationTest -x generateGitProperties

cd /home/chris/work/catalog/fileio-catalog && \
  mvn test -Dtest='TestS3Catalog,TestS3CatalogCAS,TestS3CatalogCASInlineTM,TestS3CatalogCASInlineML,TestS3FileIOCatalogTransaction,TestS3FileIOCatalogTransactionCAS,TestS3FileIOCatalogTransactionCASInlineTM,TestS3FileIOCatalogTransactionCASInlineML,GCSCatalogTest,GCSCatalogTestInlineTM,GCSCatalogTestInlineML,GCSFileIOCatalogTransactionTests,GCSFileIOCatalogTransactionTestsInlineTM,GCSFileIOCatalogTransactionTestsInlineML,ADLSCatalogTest,ADLSFileIOCatalogTransactionTests'
```

Update the cell codes and bump `last-verified` in the frontmatter to
match the date of the run. Keep notes terse — link to the work-tracking
doc (`INLINE_STABILIZATION.md`, `R4.md`, `errata.md`) for details rather
than duplicating them here.
