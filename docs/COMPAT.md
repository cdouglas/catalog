---
last-verified: 2026-05-05
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
| GCS   | OK         | WIP      | WIP         | NA            | NA          | NA             |
| S3    | OK         | WIP      | WIP         | OK            | --          | --             |
| ADLS  | OK         | --       | --          | OK            | --          | --             |

## Notes

### GCS

- **Append is unsupported.** `GCSOutputFile.prepare` rejects
  `Strategy.APPEND` with a `Preconditions` check; the only honest
  implementation would already be a CAS replace. `ProtoCatalogFormat`
  detects this at init and coerces `max.append.count=0` (commits
  `e15051d`, `a4cff40`).
- **CAS · file (OK)** — `GCSCatalogTest`,
  `GCSFileIOCatalogTransactionTests`. Part of the green non-inlined
  matrix noted in `INLINE_STABILIZATION.md`.
- **CAS · TM / CAS · TM+ML (WIP)** — `GCSCatalogTestInlineTM`,
  `GCSCatalogTestInlineML`, `GCSFileIOCatalogTransactionTestsInlineTM`,
  `GCSFileIOCatalogTransactionTestsInlineML`. Tracks the same 8 inline
  bugs as the S3 CAS-inline cells; see `INLINE_STABILIZATION.md` for the
  per-bug plan (Steps 1–6).

### S3

- **CAS · file (OK)** — `TestS3CatalogCAS`,
  `TestS3FileIOCatalogTransactionCAS`. Greened in `42c23f9` and
  `6fab0b7`.
- **append · file (OK)** — `TestS3Catalog`,
  `TestS3FileIOCatalogTransaction`. Runs against an S3 Express One Zone
  bucket (the only S3 surface that natively supports
  `WriteOffsetBytes`). Greened in `621b1cb`. Together with CAS · file
  this is the "260 + 130 = 390/390" non-inlined baseline.
- **CAS · TM / CAS · TM+ML (WIP)** — `TestS3CatalogCASInlineTM`,
  `TestS3CatalogCASInlineML`, `TestS3FileIOCatalogTransactionCASInlineTM`,
  `TestS3FileIOCatalogTransactionCASInlineML`. After scaffolding
  (`69649a3`) and the first two determinism fixes,
  `TestS3CatalogCASInlineTM` reports 8 failures (file-counting
  incompatibility + 5 real bugs). `INLINE_STABILIZATION.md` is the
  working plan; the inline-ML subclasses inherit the same overrides.
- **append · TM / append · TM+ML (--)** — no suites yet. The plan is to
  add them after the CAS-inline cells go green, since the append code
  path shares `commitInline` and the manifest pool with CAS.

### ADLS

- **append · file (OK)** — `ADLSCatalogTest`,
  `ADLSFileIOCatalogTransactionTests`. The default `ProtoCatalogFormat`
  configuration leaves `max.append.count` at 10 000, and `ADLSFileIO`
  honours `Strategy.APPEND` via lease-protected appends in
  `ADLSOutputFile`. Verified 2026-05-05: `CatalogTests` 103/103 (10
  skipped, pre-existing) and `CatalogTransactionTests` 28/28 against a
  remote ADLS Gen2 account.
- **CAS · file (OK)** — `ADLSCatalogTestCAS`,
  `ADLSFileIOCatalogTransactionTestsCAS`. One-line subclasses that flip
  `maxAppendCount()` to 0, mirroring the S3 CAS suites. Verified
  2026-05-05: same counts as the append-mode runs.
- **TM / TM+ML for both atomic modes (--)** — no suites yet. Will be
  added once the S3/GCS inline-CAS cells go green via
  `INLINE_STABILIZATION.md` Steps B–F; ADLS inline cells should then be
  cheap subclasses analogous to `*InlineTM` / `*InlineML`.

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
