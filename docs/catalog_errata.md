# Catalog Format Errata

Prioritized plan for remaining work in the log-based catalog format and the
inline manifest-list extension. Items are grouped by priority; inside each
group, order is suggestive, not strict.

## Invariants

These hold in the current code (mostly) and must not regress. Every item
below is an invariant violation or a hole that threatens one.

**I1. One intention record per commit.** A single `doCommit` / `commitTransaction`
produces exactly one transaction-level action record per affected table,
encoded into one transaction and written to storage in one atomic operation
(append or CAS). Every configuration is subject to this rule:
- **ref** (pointer): one `UpdateTableLocation`/`CreateTable` action referencing
  a pre-written `v<N>.metadata.json`;
- **TM inline**: one `UpdateTableInline` action carrying delta or full metadata;
- **TM+ML inline**: one `UpdateTableInline` action whose `deltaBytes` carries
  the TM updates *and* the per-snapshot `AddManifestUpdate` /
  `RemoveManifestUpdate` entries in the same blob.
A compaction or crash landing mid-flight must not be able to observe a
half-applied commit. Specifically: we never write an ML action separately
from the TM action that introduces the snapshot it belongs to.

**I2. Actions are idempotent on replay.** Each action's `verify` checks
preconditions (table version, namespace version, read-set versions); each
action's `apply` mutates state so that a replay of the same record against
the post-apply state fails `verify`. This is the second line of defense
alongside `committedTxn` dedup — both must hold.

**I3. Inline metadata rotation preserves the manifest pool.** Replaying an
inline TM update (delta, full, or pointer-eviction) must never silently
drop the catalog's per-table manifest pool unless the eviction is
explicitly to pointer mode. `updateInlineMetadata` preserves the pool;
`removeInlineMetadata` clears it. Callers pick by intent, not convenience.

### Operation × Operation Conflict Matrix

Each row is the **first** action applied (A); each column is a **second**
action (B) whose preconditions were captured against the pre-A state and
is applied to the post-A state. Cells hold:

- **✓** — the pair composes; B.verify succeeds.
- a field name — B.verify fails because A bumped that field.
- **—** — the pair is not meaningful (e.g., target doesn't exist).

Assumptions: all namespace/table ops target the **same** namespace or
table unless otherwise noted. Ops on *different* namespaces/tables
always compose. `CN↓` = create child under the same parent; `CN↑` =
drop the same parent; etc.

Derived from `TestProtoActions$ConflictTests`; every row/column below
is backed by a named test.

|       | CN↓ | DN  | SP  | RP  | CT  | CTI | DT  | UT  | RT  |
|-------|-----|-----|-----|-----|-----|-----|-----|-----|-----|
| **CN↓** | ns v | ns v | ns v | ns v | ns v | ns v | ✓   | ✓   | ✓   |
| **DN**  | —   | ns v | ns v | ns v | ns v | ns v | —   | —   | —   |
| **SP**  | ns v | ns v | ns v | ns v | ns v | ns v | ✓   | ✓   | ✓   |
| **RP**  | ns v | ns v | ns v | ns v | ns v | ns v | ✓   | ✓   | ✓   |
| **CT**  | ns v | ns v | ns v | ns v | ns v | ns v | ✓   | ✓   | ✓   |
| **CTI** | ns v | ns v | ns v | ns v | ns v | ns v | ✓   | ✓   | ✓   |
| **DT**  | ✓   | —   | ✓   | ✓   | ✓   | ✓   | —   | tbl v | tbl v |
| **UT**  | ✓   | —   | ✓   | ✓   | ✓   | ✓   | tbl v | tbl v | tbl v |
| **RT**  | ✓   | ✓   | ✓   | ✓   | ✓   | ✓   | tbl v | tbl v | ✓   |

Legend: **CN↓** `CreateNamespace` (child under parent), **DN**
`DropNamespace`, **SP** `SetNamespaceProperty`, **RP**
`RemoveNamespaceProperty`, **CT** `CreateTable`, **CTI**
`CreateTableInline`, **DT** `DropTable`, **UT** `UpdateTableLocation`
/ `UpdateTableInline`, **RT** `ReadTable`. "ns v" = the namespace's
version; "tbl v" = the table's version.

Invariants enforced by the matrix:
1. All ns-level mutations under the same parent serialize (row/col
   block of CN↓/DN/SP/RP/CT/CTI is diagonal-ish "ns v").
2. `DT` and `UT` on the **same table** serialize on tbl v. Neither
   conflicts with unrelated ns-level ops except through the parent's
   children-set rule.
3. `DT` and `UT` on *different* tables in the same ns compose — table
   operations don't serialize through the parent's ns version even
   though they bump it. This is by design: bumping on `DT`/`CT` protects
   ns-level ops; it doesn't pessimize unrelated table ops, because
   those carry table-level preconditions that are more precise.
4. `ReadTable` is the read side of optimistic concurrency — it
   conflicts with any update to the same table, nothing else.

## P0 — Correctness

### P0.1 *(resolved)* Action.apply() bumps ns version to enforce I2

ProtoCodec's `apply` methods now bump the namespace version for every
action that mutates the namespace's children set or properties:
`CreateNamespaceAction`, `DropNamespaceAction`, `SetNamespacePropertyAction`,
`RemoveNamespacePropertyAction`, `CreateTableAction`,
`CreateTableInlineAction`, and `DropTableAction`. `UpdateTableLocationAction`
and `UpdateTableInlineAction` already bumped the table version.

This closes the I2 gap: previously, a table-create intention record could
`verify` true more than once against the same ns-version snapshot, because
`apply` never bumped the parent ns version that `verify` checks. LCF had a
`TODO XXX` at `LogCatalogFormat.java:469-471` (pre-removal) flagging the
same defect; the PB port inherited it.

The fix also established the conflict matrix above as the *enforced* rule,
not an aspirational one. Coverage:
- `TestProtoActions$IdempotencyTests` — per-action tests assert that
  replaying the same logical intention record with a fresh UUID (bypassing
  `committedTxn` dedup) is rejected. Includes the user-specified scenario
  "create A, drop A, re-append A does not recreate the table."
- `TestProtoActions$ConflictTests` — one test per cell in the matrix
  establishing composition vs. rejection for each operation pair.

Two pre-existing tests (`IntegrationTests.dropAndRecreateDifferentNamespace`,
`setAndRemovePropertyInSequentialTransactions`) were relying on the bug
and have been updated to capture the bumped ns version for the
second-in-sequence action.

## P1 — Integration Debt

### P1.1 `TransactionTableOperations` sink forwarding (I1 violation)

The intention record itself is already atomic in `commitTransaction`, but
the *workflow* violates I1: `BaseTransaction.TransactionTableOperations`
does not forward `ManifestListSink`, so `SnapshotProducer` writes a
transient `snap-*.avro` **before** the catalog commit. The catalog then
reads that file back via `allManifests(io)` to reconstruct the ML delta
into the single atomic intention record. The end state on reload is
correct, but:
- a crash between the Avro write and the catalog commit orphans the Avro;
- a checkpoint landing in that window captures no record of the Avro,
  making cleanup harder;
- it violates the spirit of I1 even though the intention record itself
  is a single atomic write.

**Plan:**
1. Add sink-capability forwarding in `BaseTransaction.TransactionTableOperations`
   (iceberg-core change on `vldb-1.10.1` / `vldb-1.10.1-ml`) so the sink
   reaches `SnapshotProducer` through the wrapper.
2. Update `SnapshotProducer`'s sink-detection to walk through the wrapper.
3. Simplify the commitTransaction ML-extraction path in `FileIOCatalog`
   (:685-736) to drain from the sink rather than re-read `allManifests`.
4. Extend `TestInlineManifestEndToEnd#commitTransactionWithML` to assert
   no `snap-*.avro` is written (parallels `noSnapAvroFilesWritten`).

### P1.2 Remove `TableMetadata.Builder.replaceSnapshots` core fork

`FileIOCatalog.wrapInlineManifests` (FileIOCatalog.java:493) uses
`builder.replaceSnapshots(Map<Long, Snapshot>)` on `TableMetadata.Builder`
(iceberg-core fork at TableMetadata.java:1416). This is load-bearing: it
swaps `BaseSnapshot` instances for `InlineSnapshot` wrappers after parsing.

**Plan:**
1. Evaluate moving the wrap into a parser-level `SnapshotParser` hook or a
   post-parse visitor so builder mutation is unnecessary.
2. If viable, revert the `TableMetadata.Builder` change in iceberg-core and
   rewrite `wrapInlineManifests` against the alternative.
3. If not viable, upstream the builder change properly (javadoc, tests)
   rather than carry it as a quiet fork.

## P2 — Test Coverage

### P2.1 Inline ML operation coverage for `CherryPickOperation` and `BaseRowDelta`

`TmMlTests` covers MergeAppend, Overwrite, ReplacePartitions, Delete, and
RewriteManifests. CherryPick and RowDelta paths are untested end-to-end
through the catalog and require more involved setup (staged snapshots +
parent lineage for cherry-pick; position-delete writers for RowDelta).

**Plan:**
1. Add `cherryPick` test: stage an append on a side branch, cherry-pick to
   main, assert ML round-trips via catalog reload.
2. Add `rowDelta` test: write data, then position-delete a row, assert
   delete-manifest handles round-trip through the pool.

### P2.2 Cloud integration test automation

S3, ADLS, and GCS integration tests are skipped without credentials. We
currently rely on developer-run emulators; CI coverage is best-effort.

**Plan:**
1. Add Testcontainers lifecycle for LocalStack (S3), Azurite (ADLS), and
   fake-gcs-server (GCS) in the `verify` profile.
2. Gate container startup behind a profile flag so `mvn test` stays fast
   and offline.
3. Document the matrix and required Docker state in `CLAUDE.md`.

## P3 — Hardening

### P3.1 Strict mixed-mode rejection

`wrapInlineManifests` currently logs a warning when a table has both
inline and pointer-mode snapshots. Production deployments should be able to
opt into hard rejection.

**Plan:**
1. Thread the existing `fileio.catalog.inline` property (or add a new
   `fileio.catalog.inline.strict`) through `FileIOCatalog` to the static
   inner class that hosts `wrapInlineManifests`.
2. On strict mode, throw `ValidationException` instead of logging.
3. Add a test with a hand-constructed mixed-mode `CatalogFile` that
   asserts both behaviors.

### P3.2 Cross-project abstract test synchronization

`CatalogTests` and `CatalogTransactionTests` are duplicated between
`iceberg/core/src/test/` and `fileio-catalog/src/test/`. The copies have
already diverged (verified 2026-04-22: fileio-catalog's `CatalogTests`
lacks imports and assertions added upstream).

**Plan (pick one):**
1. Publish iceberg's test-jar and consume it from fileio-catalog (current
   intended approach — needs the publish step wired into the Gradle task).
2. Extract `catalog-tests` into a standalone module published separately.
3. If neither is acceptable, at minimum add a CI check that diffs the two
   files and fails on drift.

## Deferred — Revisit When Motivated

These were opened as open questions; none blocks current work and none has a
driving requirement yet. Revisit when the stated trigger condition fires.

### Forward compatibility for columnar regions

Protobuf's unknown-field preservation handles sparse records but not
columnar-encoded regions within the checkpoint. Not actionable until we
actually ship a columnar region — today's checkpoint is row-oriented.
**Trigger:** first columnar region lands in the checkpoint format.

### Advanced encodings (RLE, delta-RLE, prefix compression, UUIDv7 delta)

Optimizations for version columns, sequential IDs, path dictionaries, and
committed-transaction timestamps. Defer until base format stabilizes and
benchmarks show I/O or size pressure.
**Trigger:** benchmark shows checkpoint size or decode CPU as a bottleneck.
