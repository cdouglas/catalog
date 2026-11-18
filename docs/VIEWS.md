# Views (design)

Status: designed 2026-11-18 on branch `views` (from main 0c8bdf5), its open questions settled
by the user the same day ([Decisions on the open questions](#decisions-on-the-open-questions)).
Nothing is built. Stage 1 is views in the catalog (V1-V12). Stage 2 is views in catalog
transactions ([Stage 2](#stage-2-views-in-catalog-transactions)).

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

## Decisions (proposed)

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
- No deltas. Whole metadata is 0.5-2 KB gzipped ([Sizes](#sizes)), and views are written
  rarely.
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

**V4. One inline codec.** The inline form is Iceberg's `ViewMetadataParser` JSON, gzipped,
tagged `CODEC_JSON_GZIP`. The structural codec is a table codec, and a view refuses it.

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
- A drop deletes a pointer view's metadata once the drop has landed, as JDBC does. An inline
  view has none.
- Its files are deleted after an acknowledged drop only:
  - an unknown outcome keeps them;
  - a definitely failed drop wrote nothing.
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

catalog.proto, proposed:

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
- **Invariant.** design.md gains one: no namespace holds a table and a view of one name.
  `WellFormed` checks it after every apply.

## Sizes

These are estimates from a Python mock of Iceberg's view JSON, not kept. M1 replaces them with
measurements from Iceberg's own parser.

| View | JSON | gzip |
|---|---|---|
| 5 columns, 100 B of SQL, 1 version | 1.0 KB | 478 B |
| 10 columns, 200 B, 1 version | 1.4 KB | 548 B |
| 10 columns, 1 KB, 10 versions | 14 KB | 771 B |
| 50 columns, 2 KB, 1 version | 5.7 KB | 895 B |
| 50 columns, 10 KB, 10 versions, 3 schemas | 113 KB | 1.7 KB |

- The mock's versions repeat one SQL text, so a history of distinct texts compresses less.
- A full pointer is ~115 B (`s3://<bucket>/<warehouse>/<ns>/<view>/metadata/00003-<uuid>.gz.metadata.json`).
  A compact one would be ~18 B: a 16-byte nonce and the version, under the view's location,
  which is recorded once.
- **Per checkpoint.** Polaris' default tree (24 views): inline ~15 KB, pointers 2.8 KB.
  10,000 views: inline ~6 MB, full pointers 1.1 MB, compact ones 0.2 MB.
- **Per commit.** Inline sends 0.5-2 KB in the record, with no PUT. A pointer view sends
  ~115 B and makes a PUT, and every load of it makes a GET.

## Catalog API

- **Class.** `LogCatalog extends BaseMetastoreViewCatalog`, which extends
  `BaseMetastoreCatalog` and implements `ViewCatalog`, and it keeps `SupportsNamespaces` and
  `SupportsCatalogTransactions`. It implements `newViewOps`, `listViews`, `dropView` and
  `renameView`. The base class provides `buildView`, `loadView`, `viewExists` and
  `registerView`.
- **`LogViewOperations extends BaseViewOperations`.**
  - `doRefresh` reads the view from the replica: inline, by decoding; a pointer, by reading
    its file.
  - `doCommit` stages `CreateView` (base null) or `UpdateView` on a fresh read, and commits
    through `CatalogLog`:
    - a pointer writer writes its file first (`writeNewMetadataIfRequired`);
    - an inline writer sends bytes.
  - **Retries.** Creates retry with the create backoff and give up as table creates do. A
    create whose name is taken by the time it lands throws `AlreadyExistsException` (Bug 107
    (create conflict reported as exists)).
  - **Outcomes.** They map as table commits' do: `CommitFailedException`,
    `CommitStateUnknownException` (files kept), and `CommitCleanup` for what definitely failed.
- **Table-side changes.**
  - `createTable`, `registerTable` and `renameTable` onto a view's name throw
    `AlreadyExistsException`.
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
   - JDBC can do the same in one database transaction.
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
  catalog. `AzureLogCatalogIT` runs it on a real account later.
- **Regressions** (`TestCatalogRegressions$Views`):
  - a table create and a view create of one name racing: exactly one lands;
  - a view drop, then a late copy of a table create of its name (the cross-kind create ABA);
  - a namespace drop racing a view create;
  - one view written by pointer and inline writers in turn;
  - a pointer view's files are deleted after its drop, and kept on an unknown outcome.
- **Format properties** (`spec/`):
  - N1 (`TestVerifyStabilityProperties`) generates the four new actions beside the table
    actions.
  - The multi-writer `aRejectedBindingStaysRejectedOnceItsNameIsFree` draws views and tables
    for both the rejected binding and the action that frees its name.
  - `WellFormed` checks the new invariant (no table and view share a name) after every apply.
- **Checks on every catalog file.**
  - `CheckpointRoundTrip`'s contents hold views, and replay equals decode.
  - `CatalogProto` and `TestFormatExplorer` see the new fields.
  - The explorer's fixtures are regenerated, and it decodes views.
- **Fault fuzzer** (`TestCatalogFaultProperties`):
  - kinds `CREATE_VIEW`, `REPLACE_VIEW`, `VIEW_PROPERTIES`, `RENAME_VIEW`, `DROP_VIEW`,
    `REGISTER_VIEW`, against a model of each view (name, version, SQL, properties);
  - writers' modes are drawn as now, so views move between forms.
- **Race fuzzer** (`TestCatalogRaceProperties`): a view operation as the raced operation and
  as a racer.
- **Calibration set `fuzz/mutants/views.sh`:**
  - `viewnotable`: a view create ignores table names;
  - `tablenoview`: a table create ignores view names;
  - `viewdropnoremovals`: a view drop does not bump removals (the ABA);
  - `viewnobump`: a view create does not bump its namespace's version (a namespace drop lands
    over it);
  - `viewstale`: an `UpdateView` applies on a stale version;
  - `viewdeletefirst`: a drop deletes the files before the drop lands.
- **TLA+:** no change. `RSM.tla`'s actions are abstract, and verification stability (S2) is
  N1's.
- **havoc:** one round with the view kinds, after the merge.

## Stage 2: views in catalog transactions

- **Where the API change goes.** On the fork's `core-perf`, where the transaction API lives:
  `SupportsCatalogTransactions`, `CatalogTransaction`, `CatalogCommit`, `BaseCatalogTransaction`,
  with JDBC's and REST's implementations. The baselines run core-perf, and JDBC implements the
  API, so the change belongs there and not on `atomicio`, which holds the atomic storage
  operations and sits on core-perf.
- **API.** `CatalogCommit` gains `ViewChange`s (create, replace, drop, rename), and
  `CatalogTransaction` a view catalog to stage them through.
- **Implementations.**
  - `BaseCatalogTransaction` stages view changes as it stages table creates and drops.
  - JDBC commits them in its one database transaction, on the V1 schema.
  - `RESTCatalog`'s transaction throws, since its request has no view changes.
  - FileIOCatalog stages `CreateView`, `UpdateView`, `DropView` and `RenameView` into the
    transaction's record.
- **Cost of the change.**
  - It restacks `atomicio` onto the new `core-perf`, with a tag for its old head.
  - Both are republished, and rmow-bench builds on `atomicio`, so the publish waits for its
    campaigns.
  - The baselines' classpath digest changes, so their packs age again. It should land before
    the package that catalog-bench's O1 campaign runs, or wait until after O1.

## Measurement

- **M1 (first, before the format is written).** Measured with `ViewMetadataParser`:
  - **What:** bytes per record (create, replace, property change) and per checkpoint entry.
  - **Encodings:** full pointers, compact pointers and inline JSON-gzip.
  - **Views:** 5, 10 and 50 columns; 100 B to 10 KB of distinct SQL per version; 1 and 10
    versions.
  - **Checkpoints:** bytes at Polaris' default tree and at 10,000 views.

  It checks V2 (no deltas), and gives D28 its numbers.
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
| 5 | Merge onto main after the current fuzz round and phase 1; a havoc round | |
| 6 | catalog-bench's view operations, with E7's tree (G5) | 1 day |
| 7 | Stage 2: `ViewChange` on the fork's `core-perf`, with `BaseCatalogTransaction`'s, JDBC's and FileIOCatalog's implementations; restack and republish | 2 days |

## Decisions on the open questions

The user settled these on 2026-11-18:
- **Q1. Views in catalog transactions:** yes, as stage 2, with the API change on `core-perf`.
- **Q2. Compact pointers:** not now. Recorded as errata D28 (pointer locations are full
  strings), for tables and views alike.
- **Q3. A separate switch for views:** no. `fileio.catalog.inline` turns inlining on or off for
  tables and views together.
- **Q4. Dependency records:** a potential extension, errata D27 (view names tables only in
  SQL), with its two bindings (by id, by name) and the drop it refuses.
