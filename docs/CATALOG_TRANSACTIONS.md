# Catalog transactions through Iceberg's API

Status (2026-11-15): implemented for FileIOCatalog, JDBC and REST (REST: SNAPSHOT table updates
only, as its protocol allows). FileIOCatalog
judges a scan read by its ref (a read witness), not yet by its filter (errata D12). It replaces
errata S2's prototype API and narrows D12's serializable reads.

A catalog transaction groups changes to several tables and to the catalog itself, and commits
them all or none. FileIOCatalog has done that natively since the start: a `CatalogMutation`
stages namespace and table creates, drops, renames, property changes, table updates and reads on
one read of the log, and its intention record applies all of them where it lands, or none
(`spec/TestMultiWriterProperties` fuzzes that layer directly, with a serial re-execution and an
effects oracle). No engine calls `CatalogMutation`. Engines would use Iceberg's API:
`SupportsCatalogTransactions.createTransaction(isolation)`, a client-side `BaseCatalogTransaction`
that stages changes, and one `commitTransaction` call at the end. That API came from upstream's
prototype (apache/iceberg#6948, closed unmerged), and the fork kept two of its limits:

1. Its commit carries only table updates and reads. `asCatalog().dropTable` and `renameTable` went
   straight to the catalog, each its own commit at once, outside the transaction; `newTransaction()`
   on a transaction's table committed directly; creates and namespace changes threw.
2. Under SERIALIZABLE, every table the transaction loaded was a read of its `main` branch, so an
   append-only transaction conflicted with every change that moved `main` (E5w: the read witness
   changed nothing on mix F).

This design widens the commit so a transaction's catalog changes travel with its table updates, and
makes a scan, not a load, the read.

## Where the API lives

`CatalogTransaction`, `SupportsCatalogTransactions`, `BaseCatalogTransaction` and the suite
`CatalogTransactionTests` move from the fork's `atomicio` to `core-perf`, the Iceberg every catalog
in catalog-bench runs. `JdbcCatalog` and `RESTCatalog` implement the SPI there, so JDBC and
FileIOCatalog run the same client staging and the same test suite, and catalog-bench's adapters
all go through `BaseCatalogTransaction`. `atomicio` sits on `core-perf` and `FileIOCatalog`
implements the same SPI.

## Semantics

**SNAPSHOT.** Each table the transaction writes is judged by its updates, as the catalog judges a
table commit: FileIOCatalog by its conflict encoding from the version the transaction first read
(errata D12), JDBC by the whole version (the metadata location it read). Each catalog change is
judged by its own condition (below). Nothing the transaction only read is checked.

**SERIALIZABLE.** As SNAPSHOT, and every read the transaction made must read the same at the point
the commit takes effect: then that point is a serial order for the whole transaction. A read-only
transaction is checked the same way at its commit, against a fresh read of the catalog, with
nothing written. (The prototype committed read-only transactions unchecked: that allows read skew
between tables loaded at different times.)

**What a read is.** As in single-table SERIALIZABLE (Spark's `SparkWrite` and
`SparkPositionDeltaWrite`: from the scan's snapshot, under the scan's filter, no conflicting data
or deletes), a read is what the transaction scanned:

- A *scan read*: the rows of a branch or tag under a row filter, as of the snapshot the scan read
  (none, if the branch had none). `TableScan` and `BatchScan` planning record it: the ref
  (`useRef`, else `main`), the snapshot, the filter and its case sensitivity. A scan of a fixed
  snapshot (`useSnapshot`, `asOfTime`) records nothing: its rows cannot change. It is unchanged at
  the commit if no data or delete file matching the filter was added to the ref since its snapshot,
  no data file matching it was removed except by a rewrite that keeps the rows (Iceberg's
  `REPLACE`), and the ref was not reset to a snapshot that does not descend from it.
- A *declared read*: `CatalogTransaction.declareRead(table)`, the whole version of a table as the
  transaction first read it, for engines that decide on a table's schema, properties or snapshots
  without scanning. Unchanged if the table's version is.

A load is not a read. Neither is a listing (`listTables`, `listNamespaces`): a serializable
transaction that acts on a listing is not protected against tables or namespaces appearing or
disappearing (a gap, as in single-table Iceberg, where listings are never validated).

A scan of a table the transaction has already written reads its own staged changes; its read is of
the branch as the transaction first read it, plus those changes, so it is recorded against the
first-read snapshot, not the staged one. A scan read of a table the transaction also appends to is
kept beside the append: the append stays blind (a `FastAppend` validates nothing), and the read is
judged on its own.

**No false negatives.** A catalog may judge a read more coarsely than above, and reject more, but
never admit a read whose rows changed. FileIOCatalog judges a scan read by the conflict encoding's
code for validated writes (`partition-vector`, `manifest-window`: by the filter; `concern-vector`
and its descendants: by the ref's advance and reset stamps; `lamport`: by the version). JDBC judges
every read by the table's whole version. The client no longer checks reads before the commit (its
check was not atomic with the commit, so it caught nothing the catalog's misses, and only added
false rejections: 2,042 of E5w's failed attempts).

**Catalog changes.** `asCatalog()` stages them, and also implements `SupportsNamespaces` when the
catalog does:

| Change | Condition where the commit takes effect |
|---|---|
| create namespace (with properties) | absent (missing parents are created with it) |
| drop namespace | present, holds no tables or namespaces but those the transaction drops or renames out of it |
| set or remove namespace properties | present |
| create table (`buildTable(...).create()`, `createTable`, `createTransaction()`) | absent; namespace present |
| drop table (purge or not) | present, and the same table (uuid) the transaction read, if it read it |
| rename table | source present (same uuid, if read), target absent, target namespace present |
| `newTransaction()` of a transaction's table | stages into the catalog transaction |

FileIOCatalog checks a name a change binds (a table create, a rename's target, a namespace create)
by the catalog's `fileio.catalog.create.condition`, against the read its commit stages on. By
`name`, the default, it checks the table's condition as stated, and one more: no name in the
namespace was freed (a table dropped or renamed out of it, a child namespace dropped) between that
read and the point the commit takes effect. A rejection then stays one, so a late copy of the
record never applies once the name is free again (SPEC.md, "Action Verification Rules"); a
transaction whose read a sibling's drop overtook fails, as one that conflicts does. By
`namespace`, it checks that the namespace's version is the one that read held, which any change
there moves (a sibling created or dropped, a property set): a stricter condition than the table
states, under which creates in one namespace land one at a time.

The transaction sees its own changes: a dropped table no longer loads, a renamed one loads under its
new name only, a created one loads and takes updates, listings include them. The commit carries the
transaction's net effect against the catalog as the transaction found it, which is what
`CatalogMutation` records: tables created (with their final metadata), tables updated (by their
names at the start, with the version first read), renamed (start name to final name) and dropped,
and namespaces created, changed and dropped. A purge reads the dropped version's history before
the commit and deletes the table's files after it lands; a file it cannot read or delete then is
left and logged, and the transaction still reports its commit (Bug 96). A writer that fails after
the commit leaves the files for a GC by listing. A drop that keeps an inline table's files writes
the table to its metadata directory before the commit (SPEC.md, "Commits"); if it cannot,
the transaction fails and deletes what it wrote, unless `fileio.catalog.drop.force`, which drops the table anyway and leaves
its files as a purge interrupted after its commit does. Replace transactions, `registerTable` and views stay unsupported in a catalog transaction
and throw.

## The SPI

```java
public interface SupportsCatalogTransactions {
  CatalogTransaction createTransaction(IsolationLevel isolationLevel);

  /** Applies every change in commit where every condition and read holds, or none. */
  void commitTransaction(CatalogCommit commit);
}
```

`CatalogCommit` (immutable): the isolation level; the reads (`TableRead`: the table, the metadata
first read, and either a scan read's ref, snapshot id, filter and case sensitivity or a declared
read); the table changes (`TableChange`: the name at the start, the metadata first read, the
`TableCommit` of its updates if any, the final name if renamed, dropped and purge); the created
tables (name and metadata); and the namespace changes. Failures: `CommitFailedException` for a
condition or read that no longer holds, `ValidationException` for an update's own validation,
`CommitStateUnknownException` when the outcome is unknown, `UnsupportedOperationException` for what
the catalog cannot do. On a log that cannot replace itself (the segmented log on Append Blobs),
FileIOCatalog sends a transaction's largest inline table updates to metadata files until its
record fits in one append (SPEC_TM.md "Size Budget"), and throws `RecordTooLargeException` for one
that does not fit with none left.

| Catalog | SNAPSHOT | SERIALIZABLE | Catalog changes |
|---|---|---|---|
| FileIOCatalog | yes, table updates by the conflict encoding | yes, scan reads by the encoding | yes, in the one record |
| JDBC (`core-perf`) | yes, by whole version | yes, by whole version (`SELECT ... FOR UPDATE` of each read table in the database transaction) | yes, in the database transaction |
| REST (Polaris) | yes, by the requirements of each table commit | no: throws | no: throws (the REST spec's `CommitTransactionRequest` carries table changes only) |

## Tests

- `CatalogTransactionTests` (`core-perf`): each catalog change committed with table updates, all or
  none when one fails; the transaction's own view; drops with purge deleting files only after a
  commit; loads that are not reads; scan reads that conflict (rows the scan matched) and that do
  not (another partition, another branch, a property, a rewrite); read-only transactions checked at
  commit. Run on JdbcCatalog (SQLite; Postgres by hand), on every FileIOCatalog mode, and on RESTCatalog (SNAPSHOT
  only; the rest throw).
- No false negatives (fileio-catalog): the rebase oracle for reads
  (`TestRebaseProperties.anAdmittedReadSeesTheRowsItRead`). A serializable transaction scans a ref
  under a filter on the column partitioned tables partition by (by value, set, range, bucket or
  truncation), writes another table, the same one or nothing, and commits after a window of commits
  it did not see, before its commit's read, before its record, or both; if admitted, the rows of its
  ref under its filter where it took effect must be the rows at its snapshot (`TableViews.rows`:
  rows a rewrite keeps, each with the delete files that delete from it). The oracle is the same
  whether the catalog judges a read by its ref or by its filter; false positives are reported per encoding and table
  storage. `fuzz/mutants/reads.sh` calibrates it: a read unchecked at the commit, unheld by the
  record, a witness blind to its ref's advances, to its resets, or to everything, and a filter
  judged by the data files added alone.
- The race fuzzer's catalog transactions draw scans and catalog changes, under its serial-run oracle;
  `TableScript`'s catalog transactions likewise.
