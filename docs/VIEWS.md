# Views (design)

Status: stage 1, views in the catalog (V1-V12), is built and on main (2026-11-19): the format,
state and actions, `LogCatalog` as a `ViewCatalog`, the fuzzers' view kinds and the calibration
set `fuzz/mutants/views.sh` ([Testing](#testing)), and the TLA+ models' tables and views by name
(spec/tla/README.md "Tables and views"). Designed 2026-11-18, its open questions settled by the
user the same day ([Decisions on the open questions](#decisions-on-the-open-questions)); M1 is
measured ([Sizes](#sizes)). Stage 2, views in catalog transactions
([Stage 2](#stage-2-views-in-catalog-transactions)), is built (2026-11-19): the API on the fork's
`core-perf`, `atomicio` restacked onto it, FileIOCatalog's staging. catalog-bench has the view
operations and E7's tree (M2), not yet run.

## Summary

`LogCatalog` becomes Iceberg's `ViewCatalog` through `BaseMetastoreViewCatalog`.
- **A kind of its own.** A view is an entity of its own kind under a namespace, with its own
  ids and state. A namespace's tables and views share one set of names.
- **Both forms.** A view is stored in either of the two forms tables have: a pointer to a
  metadata file, or its metadata inline in the catalog. The writer's mode picks, as it does
  for tables.
- **Whole versions.** Every view commit carries the view's whole metadata, or a pointer to a
  file that holds it. There are no view deltas. So a view can change form on any commit.
- **Conflicts.** A view update applies only at the version its writer read, under every
  conflict encoding.
- **Acceptance.** Iceberg's own `ViewCatalogTests` (49 tests, in the fork's core test jar)
  runs in every `LocalCatalogMode`, as `CatalogTests` does.

## Why

- **Engines.** Spark's `SparkCatalog` turns on its view support only when the Iceberg catalog
  is a `ViewCatalog` (`SparkCatalog.java`, v4.0, `asViewCatalog`). So `CREATE VIEW` on a
  FileIOCatalog fails today. JDBC, REST and `InMemoryCatalog` all have views.
- **E7.** Polaris' benchmark dataset (polaris-tools, `CreateTreeDataset`) creates views
  beside tables in each namespace: three per namespace by default. catalog-bench models that
  workload through the `Catalog` API (catalog-bench ERRATA G5). Without views, every catalog
  would have to run it with none.

## What Iceberg's views are

- **Metadata.** A view's metadata (`ViewMetadata`, view spec format 1) holds:
  - a uuid, a location and properties;
  - its schemas;
  - its versions, each with:
    - an id, a timestamp and a summary;
    - a schema id;
    - its SQL representations, one per dialect;
    - a default catalog and a default namespace;
  - a version log.

  It holds no snapshots, manifests or data. Its history is bounded:
  `version.history.num-entries` (`ViewProperties`), 10 by default.
- **Names.** A view is named by a `TableIdentifier` in a namespace. A namespace's tables and
  views share one set of names. `ViewCatalogTests` checks every collision across the two:
  `createViewThatAlreadyExistsAsTable`, `createTableThatAlreadyExistsAsView`,
  `createTableViaTransactionThatAlreadyExistsAsView`, `renameViewTargetAlreadyExistsAsTable`,
  `renameTableTargetAlreadyExistsAsView`, and loads of either name as the other kind.
- **Commits.**
  - `BaseViewOperations.commit(base, metadata)` checks `base` against the current metadata,
    then calls the catalog's `doCommit`.
  - A pointer catalog writes a new metadata file per commit, then swaps the pointer.
  - The file is named `%05d-<random UUID><ext>` in `write.metadata.path`, else in
    `<location>/metadata/`.
  - The view's location is fixed at create: the builder's `withLocation`, else the catalog's
    `defaultWarehouseLocation(identifier)`.
- **Register.** `BaseMetastoreViewCatalog.registerView` adopts an existing metadata file as a
  new view (`ops.commit(null, metadata)`).
- **Drop.** `ViewCatalog.dropView` has no purge flag. JDBC's drop deletes the view's row, then
  its metadata (`CatalogUtil.dropViewMetadata`). Iceberg has no drop that keeps a view's files.
- **References are names.** A view's SQL names the tables it reads. The engine resolves them at
  query time against the version's default catalog and namespace.
  - No catalog records which tables a view reads.
  - Nothing in `ViewBuilder` would let it: the builder takes a schema, SQL text per dialect,
    defaults and properties, and the catalog does not parse SQL.

## Decisions

**V1. Views are their own kind under a namespace.**
- A namespace has three kinds of children: child namespaces, with their own names; and tables
  and views, which share one set of names.
- Views get their own id counter (`next_view_id`), map and lookup in `CatalogState`. A view
  keeps its id across renames, as a table does.
- One name check covers both kinds.
- Why not one generic entry with a kind field:
  - A table carries state a view never has: inline metadata, the manifest pool, lists,
    sealed blocks and conflict states.
  - Separate maps leave the table code paths untouched.
  - The shared name is one function, `nameTaken(nsId, name)`.

**V2. A view's version is whole.**
- Every view commit carries the whole `ViewMetadata`, inline, or a pointer to a file holding
  it. A create, a replace (a new version) and a property change are all the same action.
- No deltas. Whole metadata is 0.8-4.7 KB gzipped, 13 KB at most at 10 distinct versions of
  10 KB of SQL ([Sizes](#sizes)), and views are written rarely.
- So a view can move between pointer and inline on any commit, as its writers' modes differ.
  Tables, by contrast, return inline through import pages.

**V3. The writer's mode picks the form.**
- `fileio.catalog.inline` picks it, as it does for tables: an inline writer sends the bytes,
  a pointer writer writes a file and sends its location.
- `registerView` always sends a pointer: it names a file.
- An inline view goes to a file in two cases:
  - its update exceeds `fileio.catalog.inline.max.update.size`;
  - on a log that bounds a record, the record budget evicts it. Its update registers as
    `evictable`, as an inline table update does.

**V4. One inline codec.** The inline form is Iceberg's `ViewMetadataParser` JSON, gzipped at
the default level (a view is encoded once, by its writer: [Sizes](#sizes)), tagged
`CODEC_JSON_GZIP`. The structural codec is a table codec, and a view refuses it.

**V5. Pointers are full strings.** That is how pointer tables store theirs. Compact pointers,
for tables and views alike, are errata D28 (pointer locations are full strings).

**V6. Views conflict on their version.** A view update applies only at the version its writer
read, under every conflict encoding. Views keep no conflict state. Two replaces of one view:
the second fails with `CommitFailedException`. Iceberg's retry then reapplies its change on a
fresh read, as JDBC's does.

**V7. Names bind by the writer's create condition, across both kinds.**
- View creates, `registerView` and renames into a namespace send the oneof table creates send
  (`fileio.catalog.create.condition`).
- `ProtoCodec.bindingProblem`'s name check covers both kinds:
  - a view create fails where a table holds the name;
  - a table create, a table rename and a `CreateTableInline` fail where a view holds it.
- This changes the verification of existing table actions. It is Iceberg's rule, and a
  catalog transaction's table creates get it for free.
- **Counters.** View creates and renames into a namespace bump its version. View drops and
  renames out bump its removals.
- **S2 still holds.** A name is freed only by an action that adds to its namespace's removals,
  of either kind. So a binding by name that failed fails on every later state. That includes a
  late copy of a table create after a view of that name is dropped (the create ABA).

**V8. A namespace drop counts views.**
- The writer refuses to drop a namespace that holds a view (`NamespaceNotEmptyException`), as
  JDBC does.
- `DropNamespace` keeps its verification: the namespace's version is unchanged. View creates
  and renames in bump that version, so a drop staged on an empty read never lands over a view
  created since.

**V9. A drop deletes a pointer view's files.**
- A drop deletes a pointer view's metadata once the drop has landed, as JDBC does
  (`CatalogUtil.dropViewMetadata`, under `gc.enabled`). An inline view has none.
- Its files are deleted after an acknowledged drop only:
  - an unknown outcome keeps them;
  - a definitely failed drop wrote nothing.
- With `gc.enabled` false, Iceberg keeps a dropped view's metadata file, for `registerView` to
  bring it back. An inline view has none, so its drop first writes its metadata to a file in its
  metadata directory, as a non-purge drop of an inline table does
  (`fileio.catalog.drop.materialize`, default true), and logs it. A write that fails fails the
  drop (`fileio.catalog.drop.force` drops it anyway); a drop that definitely failed has the file
  deleted.
- A pointer commit that definitely failed hands its new file to `CommitCleanup`, as table
  commits do (Bug 119 (failed attempts leave metadata files)).

**V10. Late-bound ids.** A record that creates a view names it by a negative id, bound at
replay, as tables and namespaces are.

**V11. The inline location.** An inline view's metadata location is a sentinel, as an inline
table's is. Its shape must be one that the table sentinel's parser refuses
(`inline-view://<view>#<id>.<version>`), so neither kind can be read as the other.

**V12. Out of scope.**
- Materialized views.
- Records of which tables a view reads: errata D27 (view names tables only in SQL;
  [Identity](#identity-what-by-name-means-here)).

Views in catalog transactions are stage 2, below.

## Format

catalog.proto (as built; [SPEC.md](SPEC.md) "View Action Messages"):

```proto
message Checkpoint {
  ...
  int32 next_view_id = 7;
  repeated View views = 14;          // in order of id
}

message View {
  int32 id = 1;
  int32 version = 2;
  int32 namespace_id = 3;
  string name = 4;
  oneof metadata {
    bytes  inline_metadata   = 5;    // ViewMetadataParser JSON, by codec
    string metadata_location = 6;    // a view metadata file
  }
  InlineMetadataCodec codec = 7;     // with inline_metadata: CODEC_JSON_GZIP only
}

// Action's oneof gains:
//   CreateView create_view = 14; UpdateView update_view = 15;
//   DropView drop_view = 16;     RenameView rename_view = 17;

// Binds its name as CreateTable does, across tables and views. Apply bumps
// its namespace's version.
message CreateView {
  int32 id = 1;                      // late-bound (negative)
  int32 version = 2;                 // the new view's version (1)
  int32 namespace_id = 3;            // negative if created in this transaction
  oneof namespace_condition {
    int32 namespace_version  = 4;    // must match; -1 if late-bound
    int32 namespace_removals = 5;    // must match, and the name free; -1 if late-bound
  }
  string name = 6;
  oneof metadata {
    bytes  inline_metadata   = 7;
    string metadata_location = 8;
  }
  InlineMetadataCodec codec = 9;
}

// The view's whole next metadata (a replace or a property change).
message UpdateView {
  int32 id = 1;
  int32 version = 2;                 // must match current
  oneof metadata {
    bytes  inline_metadata   = 3;
    string metadata_location = 4;
  }
  InlineMetadataCodec codec = 5;
}

// Apply bumps its namespace's version and removals.
message DropView {
  int32 id = 1;
  int32 version = 2;
}

// As RenameTable: its new name binds as a create's does, across both kinds.
// Apply bumps the view's version, both namespaces' versions, and the removals
// of the namespace it leaves.
message RenameView {
  int32 id = 1;
  int32 version = 2;
  int32 new_namespace_id = 3;
  oneof new_namespace_condition {
    int32 new_namespace_version  = 4;
    int32 new_namespace_removals = 6;
  }
  string new_name = 5;
}
```

- **Reader version.** A checkpoint that holds a view says `min_reader_version` 2. There are no
  users to keep compatible with, but an older reader should refuse a catalog with views rather
  than silently drop them.
- **Invariant.** design.md I13: no namespace holds a table and a view of one name.
  `WellFormed` checks it after every apply.
- **Next view id.** `next_view_id` is absent until the catalog's first view, and a checkpoint
  without a view says `min_reader_version` 1: a catalog without views writes the bytes it wrote
  before views.

## Sizes

M1 measured these with Iceberg's own `ViewMetadataParser`, on views made through Iceberg's view
API with the summary Iceberg puts on each version and the properties Spark's `CREATE VIEW` adds
(`ViewMetadataSizeProbe`, test scope; its output prints every shape). A version's SQL is either
distinct from the last version's (the worst case for gzip) or the last version's with one
predicate changed (edited). gzip is at the default level, as a pointer view's file holds it
(views default to gzip) and as the inline form does (V4).

| View | JSON | gzip | each version after the first, gzip |
|---|---:|---:|---:|
| 10 columns, 100 B of SQL, 1 version | 2.0 KB | 806 B | |
| 10 columns, 1 KB, 1 version | 2.8 KB | 1.0 KB | |
| 10 columns, 1 KB, 10 versions edited | 15.7 KB | 1.3 KB | 35 B |
| 10 columns, 1 KB, 10 versions distinct | 15.4 KB | 2.5 KB | 167 B |
| 10 columns, 10 KB, 10 versions edited | 105 KB | 3.5 KB | 127 B |
| 10 columns, 10 KB, 10 versions distinct | 106 KB | 13.2 KB | 1.2 KB |
| 50 columns, 1 KB, 1 version | 7.2 KB | 1.8 KB | |
| 50 columns, 10 KB, 10 versions edited, 3 schemas | 132 KB | 4.7 KB | 166 B |

- **Records.** A create sends the view's first version: 0.8-3.4 KB inline, 172 B by a full
  pointer. A replace or a property change sends the whole view: 1.1-1.5 KB inline at 10 columns,
  1 KB of SQL and 10 versions, and 13 KB at most (10 distinct versions of 10 KB). A pointer sends
  145 B, and a compact one would send 29 B. That is far below the inline bound (~4 MiB) and
  small beside a table commit's record, so V2 (no deltas) holds.
- **The level.** gzip's fastest level, which tables' inline codec uses because a checkpoint
  re-encodes changed tables, makes a view 5-130% larger (10 KB of SQL edited over 10 versions:
  8.1 KB against 3.5 KB). A checkpoint never re-encodes a view, so a view is gzipped at
  the default level, once, by its writer.
- **Pointers.** A full pointer is 129 B here
  (`s3://analytics-prod-lake/warehouse/sales_db/daily_revenue_v0/metadata/00003-<uuid>.gz.metadata.json`),
  a checkpoint row 161 B. A compact one (errata D28) would be 17 B, a row 46 B.
- **Per checkpoint.**

  | Views | of | inline | full pointers | compact pointers |
  |---:|---|---:|---:|---:|
  | 24 (Polaris' default tree) | 10 columns, 1 KB, 1 version | 25 KB | 3.8 KB | 1.1 KB |
  | 24 | 10 columns, 1 KB, 10 versions distinct | 60 KB | 3.8 KB | 1.1 KB |
  | 10,000 | 10 columns, 1 KB, 1 version | 10.2 MB | 1.5 MB | 0.45 MB |
  | 10,000 | 10 columns, 1 KB, 10 versions edited | 13.2 MB | 1.5 MB | 0.45 MB |
  | 10,000 | 10 columns, 1 KB, 10 versions distinct | 24.4 MB | 1.5 MB | 0.45 MB |
  | 10,000 | 50 columns, 10 KB, 10 versions edited, 3 schemas | 45.5 MB | 1.5 MB | 0.45 MB |

  Inline views cost a checkpoint 7-30 times what pointers do: at 10,000 views, 10-45 MB that
  every compaction writes again, as inline tables' metadata is. The design mock had estimated
  6 MB. A catalog of many views and frequent compactions should keep its views as pointers
  (`fileio.catalog.inline` false, which also keeps its tables as pointers, Q3).
- **Per commit.** Inline sends the view in the record, with no PUT. A pointer view sends 145 B
  and makes a PUT of its file, and every load of it makes a GET.

## Catalog API

- **Class.** `LogCatalog extends BaseMetastoreViewCatalog`, which extends
  `BaseMetastoreCatalog` and implements `ViewCatalog`, and it keeps `SupportsNamespaces` and
  `SupportsCatalogTransactions`. It implements `newViewOps`, `listViews`, `dropView` and
  `renameView`. The base class provides `buildView`, `loadView`, `viewExists` and
  `registerView`. Its table builder extends `BaseMetastoreViewCatalogTableBuilder`, whose
  replace transactions refuse a view's name.
- **`LogViewOperations extends BaseViewOperations`.**
  - `doRefresh` reads the view from a fresh read: inline, by decoding; a pointer, by reading
    its file. A pointer view's file deleted by a drop that landed after the read is read again
    from a fresh read (`BaseViewOperations`' refresh on `NotFoundException`).
  - `doCommit` stages `CreateView` (base null) or `UpdateView` on the read the view was loaded
    from (an update built on another version fails as stale), and commits through
    `CatalogLog`:
    - an inline writer sends the bytes if they fit in `fileio.catalog.inline.max.update.size`;
    - otherwise, and for a pointer writer, the file is written first
      (`writeNewMetadataIfRequired`), and a registered view names its own file.
  - **After a commit** the operations take the replica it landed in as the view's state, as a
    table's do (errata D20): a pointer view holds the metadata it wrote rather than reading it
    back, so a commit reads the catalog once.
  - **Retries.** A create is staged again on a fresh read after a conflict, waiting as the
    commit loop does, up to `OPERATION_ATTEMPTS`, as a table's create is. A create whose name
    is taken by the time it lands throws `AlreadyExistsException`, with Iceberg's message for
    the kind that holds it (Bug 107 (create conflict reported as exists)). An update that
    conflicts fails (`CommitFailedException`); Iceberg's view updates retry it on a fresh read.
  - **Outcomes.** They map as table commits' do: `CommitFailedException`,
    `CommitStateUnknownException` (files kept), and `CommitCleanup` for what definitely failed.
- **`dropView` and `renameView`** are staged on a fresh read and again after a conflict, as a
  table's drop and rename are; they report a conflict in every attempt as
  `ServiceUnavailableException`.
- **Table-side changes.**
  - `createTable`, `registerTable` and `renameTable` onto a view's name throw
    `AlreadyExistsException` (`registerTable` checks before it reads the metadata file, as
    JDBC's does).
  - `loadTable` of a view's name throws `NoSuchTableException`, and `loadView` of a table's
    name throws `NoSuchViewException`.
  - `listTables` lists no views, and `listViews` lists no tables.
  - `dropNamespace` counts views.
- **Unchanged.** Catalog transactions keep their API. Their table creates fail on a view's
  name by V7.

## Identity: what "by name" means here

Iceberg ties a view to its namespace by name, and to the tables it reads by names inside
dialect SQL. No catalog can do better through Iceberg's API, because the engine never tells
the catalog what the SQL resolved to. Inside Batavia, entries are ids: a rename moves the id,
and a view's metadata carries its own uuid. What Batavia could add, outside Iceberg's API:

1. **Views in catalog transactions** (stage 2). A view and the tables it reads change
   atomically: create a table and the view over it, or swap a view to a new table, in one
   record.
   - JDBC does the same in one database transaction.
   - REST cannot: its `CommitTransactionRequest` carries table changes only.
   - This is also what puts E7's whole tree, views included, in one transaction.
2. **Dependency records** (errata D27). A view version binds each table it reads, by id or by
   name:
   - **By id:** its SQL refers to the table through the binding, and a load names the table's
     current identifier, so a rename keeps the view valid.
   - **By name:** a drop or rename out of the name is refused unless the same record binds the
     name again, so a transaction can swap a table in place.

   Either way, a table a live view depends on can't be dropped, as a namespace with children
   can't. A dependent view's create bumps the table's version, so the refusal is stable (S2),
   as `DropNamespace`'s is. What blocks it is the input: no engine passes the tables its query
   resolved through `ViewBuilder`.

## Testing

- **Acceptance.** `TestLocalViewCatalog` runs `ViewCatalogTests<LogCatalog>` in every
  `LocalCatalogMode`, the MEMLOG and AZLOG modes included, with `tableCatalog()` the same
  catalog: 50 tests a mode pass, and `listViewsInEmptyNamespace` skips, as the catalog has no
  empty namespace. Four tests assume a view's metadata is a file; an inline view runs a variant
  of each: `createViewWithCustomMetadataLocation` (a pointer writer's commit puts its file under
  `write.metadata.path`), `registerView` (the file a kept view's drop writes, V9), and
  `renameView` and `renameViewUsingDifferentNamespace` (the metadata is unchanged but for its
  location, which names the view's version). `AzureLogCatalogIT` runs it on a real account
  later.
- **Actions** (`TestViewActions`): each action's preconditions and effects, the shared names
  (V7) and the cross-kind create ABA, the codec, and staging with Iceberg's messages.
- **Regressions** (`TestCatalogRegressions$Views`):
  - a table create and a view create of one name racing: the second is rejected, with
    Iceberg's message for the kind that holds the name;
  - a view drop, then a late copy of a table create of its name (the cross-kind create ABA);
  - a namespace drop racing a view create;
  - one view written by pointer and inline writers in turn;
  - a pointer view's file is deleted after its drop, and kept on an unknown outcome;
  - an inline view kept by its drop (`gc.enabled` false) is registered again;
  - two updates of one view staged on one read, under three encodings: the second is rejected.
- **Format properties** (`spec/`):
  - N1 (`TestVerifyStabilityProperties`) generates view creates (pointer and inline), updates,
    drops and renames beside the table actions, of the names tables take, from a seed with a
    view of each form.
  - The multi-writer fuzzer models views (`CREATE_VIEW`, `UPDATE_VIEW`, `DROP_VIEW`,
    `RENAME_VIEW`) in its serial re-execution, and
    `aRejectedBindingStaysRejectedOnceItsNameIsFree` draws creates, drops and renames of both
    kinds, so a rejected binding of either is copied once a drop or rename of either frees its
    name.
  - `WellFormed` checks I13 (no table and view share a name) after every apply.
- **Checks on every catalog file.**
  - `CheckpointRoundTrip`'s contents hold views, and replay equals decode.
  - `CatalogProto` and `TestFormatExplorer` see the new fields.
  - The explorer's fixtures are regenerated, with a views scenario, and it decodes views.
- **Fault fuzzer** (`TestCatalogFaultProperties`):
  - kinds `CREATE_VIEW`, `REPLACE_VIEW`, `VIEW_PROPERTIES`, `RENAME_VIEW`, `DROP_VIEW`,
    `REGISTER_VIEW`, against a model of each view (SQL, version id, properties, `gc.enabled`), of
    the names tables take; each view loads alike through the checker and a fresh reader;
  - an acknowledged drop of a kept view must leave a metadata file holding it, which
    `REGISTER_VIEW` brings back;
  - writers' modes are drawn as now, so views move between forms.
- **Race fuzzer** (`TestCatalogRaceProperties`): view creates (of the names tables take),
  drops, renames, property changes and replaces as the raced operation and as racers, from
  setups with views; the state compared holds each view's properties, SQL and version.
- **Calibration set `fuzz/mutants/views.sh`:**
  - `viewnotable`: a view create or rename ignores table names;
  - `tablenoview`: a table create or rename ignores view names;
  - `viewdropnoremovals`: a view drop does not bump removals (the ABA);
  - `viewnobump`: a view create does not bump its namespace's version (a namespace drop lands
    over it);
  - `viewstale`: an `UpdateView` applies on a stale version;
  - `viewdeletefirst`: a drop deletes the files before the drop lands.

  Locally, each oracle alone at its default tries, every mutant is caught: the regressions and
  `TestViewActions` catch all but `viewdeletefirst`, which the regressions alone catch; N1 the
  five format mutants (`viewstale` at try 159, the others within 13); the multi-writer fuzzer all
  but `viewdeletefirst` (`viewdropnoremovals` only by the binding property, at try 321 of 3,000);
  the fault and race fuzzers one each. The script's header has each oracle's tries.
- **TLA+:** `spec/tla`'s models hold tables and views by name (README "Tables and views"):
  creates by name or by version, a view's update, drops, renames and a namespace's drop, with
  `WellNamed` (I13). Switches for `viewnotable`, `tablenoview`, `viewnobump` and `viewstale`
  break `Serializable` on either log; `viewdropnoremovals` breaks `HonestFailed` on the shared
  log, through a late copy of a rejected create, which one object's conditional writes never
  land.
- **havoc:** one round with the view kinds, after the merge.

## Stage 2: views in catalog transactions

- **Where the API is.** On the fork's `core-perf` (20f42d53b4), where the transaction API lives:
  `SupportsCatalogTransactions`, `CatalogTransaction`, `CatalogCommit`, `BaseCatalogTransaction`,
  with JDBC's and REST's implementations. `atomicio` sits on it (restacked; its head before is
  tag `atomicio-pre-views-txn`).
- **API.** `CatalogCommit` carries `viewChanges` (`ViewChange`: the view's version as the
  transaction read it, its metadata after the transaction, its new name, or its drop) and
  `createdViews`. A commit applies the drops, then the updates, then the renames, then the
  creates, of tables and views alike. Each view change applies only to the view at the version
  read (V6). `CatalogTransaction.asViewCatalog()` stages them; `asCatalog()` is a `ViewCatalog`
  too where the catalog has views.
- **Implementations.**
  - `BaseCatalogTransaction` stages a view's create and replace through the catalog's own
    builder (`BaseMetastoreViewCatalog.buildView(identifier, ops)`, so its default location and
    view properties apply), and its property and location changes, renames and drops. A
    staged table or view keeps its name from the other kind. A namespace's drop counts its
    views; a view created after the transaction staged the drop fails the commit
    (`TestCatalogRegressions$Views`).
  - JDBC commits them in its one database transaction, on the V1 schema. It writes each view's
    next metadata file first, and deletes a dropped view's metadata once the commit lands, as
    `dropView` does.
  - `RESTCatalog`'s transaction throws, since its request has no view changes.
  - FileIOCatalog stages `CreateView`, `UpdateView`, `DropView` and `RenameView` into the
    transaction's record (`LogCatalog.stageViewChanges`). Each view must be at the version the
    transaction read. A drop treats files as `dropView` does (V9). An update or create goes
    inline or to a metadata file, as a view's commit sends it. A view's create or rename takes
    no name the state before the record holds, though the transaction frees it, and a table
    takes no name a view held there (errata D29 (txn takes no freed name)).
- **Tests.** `CatalogTransactionTests` gains view cases. JDBC runs them on V1, REST its refusal,
  and FileIOCatalog runs them in every `LocalCatalogMode` (`takesNamesItFreesAcrossKinds` false:
  D29 (txn takes no freed name)). The fuzzers' transactions stage no view changes yet (errata T8
  (fuzzer transactions lack views)).
- **Cost.** The baselines' classpath digest changed with `core-perf`, so their packs age again.
  No table commit path changed.

## Measurement

- **M1** ([Sizes](#sizes)): `ViewMetadataSizeProbe` (test scope, `exec:java`) prints the bytes
  of each view's metadata, of each record (create, replace, property change) and of each
  checkpoint row, inline, by full pointer and by compact pointer, for 5, 10 and 50 columns, 100 B
  to 10 KB of SQL per version, 1 and 10 versions, distinct or edited, and one or three schemas;
  and a checkpoint's views at Polaris' default tree and at 10,000 views. It checked V2 (no
  deltas), set V4's gzip level, and gives D28 its numbers.
- **M2 (catalog-bench, with E7).**
  - **Driver operations:** `create_view`, `load_view`, `replace_view`, `set_view_properties`,
    `rename_view`, `drop_view`, on the tree (G5) at Polaris' read and update ratios.
  - **Readings:** latency, requests by class, and bytes per operation.
  - **Catalogs:**
    - FileIOCatalog pointer and inline;
    - JDBC, which supports views only on the V1 schema: its SUT sets `jdbc.schema-version` to
      V1;
    - Polaris.

## Plan

| Step | What | Estimate |
|---|---|---|
| 1 | M1 | 0.5 day |
| 2 | Format, state, actions, codecs; SPEC.md, design.md invariant, explorer fixtures | 1.5 days |
| 3 | Catalog API, view operations, table-side checks; `ViewCatalogTests` green in every mode | 1.5 days |
| 4 | N1, multi-writer, fault and race fuzzers, regressions, `views.sh` calibrated | 2 days |
| 5 | Merge onto main (2026-11-19); a sharded havoc round and the `views` and `creates` calibrations on the merged head | |
| 6 | catalog-bench's view operations, with E7's tree (G5): done 2026-11-19 (catalog-bench 446e321..3ec4ac0) | 1 day |
| 7 | Stage 2: `ViewChange` on the fork's `core-perf`, with `BaseCatalogTransaction`'s, JDBC's and FileIOCatalog's implementations; restack and republish: done 2026-11-19 | 2 days |

## Decisions on the open questions

The user settled these on 2026-11-18:
- **Q1. Views in catalog transactions:** yes, as stage 2, with the API change on `core-perf`.
- **Q2. Compact pointers:** not now. Recorded as errata D28 (pointer locations are full
  strings), for tables and views alike.
- **Q3. A separate switch for views:** no. `fileio.catalog.inline` turns inlining on or off for
  tables and views together.
- **Q4. Dependency records:** a potential extension, errata D27 (view names tables only in
  SQL), with its two bindings (by id, by name) and the drop it refuses.
