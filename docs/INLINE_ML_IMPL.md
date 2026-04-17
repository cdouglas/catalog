# Inline Manifest List -- Implementation Plan

Tracks implementation of [INLINE_ML.md](../INLINE_ML.md) for
`ProtoCatalogFormat`. Each stage is independently testable; all tests pass
between stages. Builds on inline table metadata (TM) from
[INLINE_IMPL.md](./INLINE_IMPL.md), which is fully complete.

## Prerequisites

- Iceberg core hooks on `vldb-1.10.1-ml` branch (done):
  - `ManifestListSink` interface (`iceberg/core/.../ManifestListSink.java`)
  - `InlineSnapshot` class (`iceberg/core/.../InlineSnapshot.java`, public)
  - `SnapshotProducer.apply()` routes to sink when `ops instanceof ManifestListSink`
  - Tests: `TestManifestListSink`, `TestManifestListDelta`
- Publish iceberg SNAPSHOT artifacts:
  ```bash
  cd iceberg && ./gradlew publishToMavenLocal -x test -x generateGitProperties
  ```

## Design Decisions

### DD1. Independent feature flags

Two catalog properties control inline behavior independently:

| Property                         | Default | Description                     |
|----------------------------------|---------|---------------------------------|
| `fileio.catalog.inline`          | `false` | Inline table metadata (TM)     |
| `fileio.catalog.inline.manifests`| `false` | Inline manifest lists (ML)     |

Valid combinations:

| `inline` | `inline.manifests` | Behavior                          |
|----------|--------------------|-----------------------------------|
| false    | false              | Baseline: pointer TM, Avro ML    |
| true     | false              | TM-only: inline TM, Avro ML      |
| true     | true               | TM+ML: inline TM + inline ML     |
| false    | true               | **Rejected at config time**       |

ML requires TM because manifest list deltas are carried inside TM intention
records. Setting `inline.manifests=true` without `inline=true` raises
`IllegalArgumentException` at catalog initialization.

### DD2. Sink activation by subclass

`FileIOCatalog.newTableOps()` instantiates `InlineManifestTableOperations`
(extends `FileIOTableOperations`, implements `ManifestListSink`) when
`inline.manifests=true`. Otherwise it uses the existing `FileIOTableOperations`.
This ensures `SnapshotProducer` only takes the sink path when the catalog is
prepared to store ML deltas. The existing class is not modified.

### DD3. In-memory state: path-keyed maps, wire-format: positional arrays

In-memory (`ProtoCatalogFile`):
- `Map<Integer, Map<String, ManifestFile>> manifestPool` -- tblId -> path -> entry
- `Map<Integer, Map<Long, List<String>>> snapshotManifests` -- tblId -> snapId -> paths

Wire format (checkpoint protobuf):
- `repeated ManifestFileEntry manifest_pool` -- positional array per table
- `repeated SnapshotManifestRefs snapshot_refs` -- snapId + `repeated int32` indices

Conversion happens during checkpoint encode/decode only. This avoids
index-stability problems when snapshots are expired (see errata E-ML5).

### DD4. Independent delta update types

Two new `InlineDeltaCodec.DeltaUpdate` subclasses carry ML changes:
- `AddManifestUpdate(snapshotId, ManifestFile)` -- adds to pool + snapshot refs
- `RemoveManifestUpdate(snapshotId, manifestPathSuffix)` -- removes from snapshot refs

These are independent of `AddSnapshotUpdate`, not embedded in it. This keeps
`AddSnapshot` wire-compatible and allows ML deltas to be tested in isolation.
The association is by `snapshotId`: each `AddManifestUpdate` references the
snapshot it belongs to.

### DD5. No pointer-mode eviction for ML in V1

If TM falls back to pointer mode, any snapshot with inline ML keeps its ML
data in the checkpoint. Pointer-mode eviction writes `metadata.json`, not
`snap-*.avro`. We do not retroactively materialize Avro manifest list files.

### DD6. Gradual migration

Existing snapshots keep their `manifestListLocation` (pointer). Only new
snapshots produced while `inline.manifests=true` get inline ML. The loader
handles mixed snapshots: `InlineSnapshot` for those with inline data,
`BaseSnapshot` for those with Avro pointers.

---

## Stages

### Stage 1: Proto schema + ManifestFile codec

**Goal:** `catalog.proto` has the new messages; `ProtoCodec` can round-trip
`ManifestFile` objects to/from `ManifestFileEntry` protobuf.

**Deliverables:**

- `catalog.proto`: add messages:
  ```protobuf
  message ManifestFileEntry {
    string manifest_path_suffix = 1;
    int64  manifest_length      = 2;
    int32  partition_spec_id    = 3;
    int32  content              = 4;   // 0=data, 1=deletes
    int64  sequence_number      = 5;
    int64  min_sequence_number  = 6;
    int64  added_snapshot_id    = 7;
    int32  added_files_count    = 8;
    int32  existing_files_count = 9;
    int32  deleted_files_count  = 10;
    int64  added_rows_count     = 11;
    int64  existing_rows_count  = 12;
    int64  deleted_rows_count   = 13;
    repeated PartitionFieldSummary partitions = 14;
    bytes  key_metadata         = 15;
    int64  first_row_id         = 16;
  }

  message PartitionFieldSummary {
    bool  contains_null = 1;
    bool  contains_nan  = 2;
    bytes lower_bound   = 3;
    bytes upper_bound   = 4;
  }

  message SnapshotManifestRefs {
    int64          snapshot_id  = 1;
    repeated int32 pool_indices = 2;
  }
  ```
- Extend `InlineTable`:
  ```protobuf
  message InlineTable {
    // ... existing fields 1-6 ...
    repeated ManifestFileEntry manifest_pool    = 7;
    repeated SnapshotManifestRefs snapshot_refs = 8;
  }
  ```
- `ProtoCodec`: `encodeManifestFile(ManifestFile, String prefix)` and
  `decodeManifestFile(bytes, String prefix)` methods.
- **Tests:** round-trip a `GenericManifestFile` through encode/decode;
  verify all 16 fields survive including partition summaries.

**Status:** Complete

---

### Stage 2: ProtoCatalogFile state model extensions

**Goal:** The in-memory catalog state can hold per-table manifest pools and
per-snapshot manifest references. Checkpoint read/write includes ML data.

**Deliverables:**

- `ProtoCatalogFile`: add fields:
  - `Map<Integer, Map<String, ManifestFile>> manifestPool`
  - `Map<Integer, Map<Long, List<String>>> snapshotManifests`
- Accessors:
  - `List<ManifestFile> inlineManifests(int tblId, long snapshotId)`
  - `boolean hasInlineManifests(int tblId, long snapshotId)`
  - Add analogous `TableIdentifier`-accepting overloads
- `ProtoCatalogFile.Builder`: extend `addInlineTable(...)` to accept optional
  manifest pool + snapshot refs. Backward-compatible: existing callers still
  work (empty pool = no inline ML for that table).
- `ProtoCodec`: checkpoint encode/decode for `InlineTable` fields 7-8.
  Encode: path-keyed map -> positional array. Decode: positional array ->
  path-keyed map.
- **Tests:** `TestProtoActions.InlineManifestListTests` nested class:
  - Checkpoint round-trip with 1 table, 3 manifests, 2 snapshots sharing
    manifests via pool
  - Empty manifest pool round-trip (backward compat)
  - Multiple tables with independent pools

**Status:** Complete

---

### Stage 3: InlineDeltaCodec ML extensions

**Goal:** Delta updates can carry manifest list additions and removals.
Encode/decode round-trips. Apply updates to `ProtoCatalogFile` state.

**Deliverables:**

- New `DeltaUpdate` subclasses:
  ```java
  class AddManifestUpdate implements DeltaUpdate {
    long snapshotId;
    ManifestFile manifest;    // finalized entry from SnapshotProducer
  }

  class RemoveManifestUpdate implements DeltaUpdate {
    long snapshotId;
    String manifestPathSuffix; // identifies manifest to remove
  }
  ```
- `catalog.proto`: two new `TableMetadataUpdate` oneof fields:
  ```protobuf
  message TableMetadataUpdate {
    oneof update {
      // ... existing 1-11 ...
      AddManifestDelta    add_manifest    = 12;
      RemoveManifestDelta remove_manifest = 13;
    }
  }

  message AddManifestDelta {
    int64 snapshot_id                        = 1;
    ManifestFileEntry manifest               = 2;
  }

  message RemoveManifestDelta {
    int64  snapshot_id           = 1;
    string manifest_path_suffix  = 2;
  }
  ```
- `InlineDeltaCodec`: extend `encodeDelta`, `decodeDelta` for the new types.
- `InlineDeltaCodec`: extend `applyUpdates` -- `AddManifestUpdate` adds to
  pool + snapshot refs; `RemoveManifestUpdate` removes from snapshot refs.
  Note: `applyUpdates` currently operates on `TableMetadata.Builder`, but ML
  state lives on `ProtoCatalogFile`. Either:
  - (a) `applyUpdates` returns ML deltas as a side channel, or
  - (b) Introduce `applyDeltaToProto(ProtoCatalogFile, tblId, deltaBytes)`
    that applies both TM and ML updates.
  Option (b) is cleaner; evaluate during implementation.
- `attachManifestDelta(List<DeltaUpdate>, snapshotId, List<ManifestFile> added, List<String> removedPaths)`:
  helper that appends `AddManifestUpdate` / `RemoveManifestUpdate` entries.
- `AddSnapshotUpdate.applyTo`: when `manifestListSuffix` is null/empty,
  produce an `InlineSnapshot` instead of a `BaseSnapshot` (see errata E-ML4).
- **Tests:** `TestInlineDelta` extensions:
  - FastAppend delta: 1 `AddManifestUpdate`, 0 removes
  - Delete delta: 1 add (rewritten manifest) + 1 remove (original)
  - Encode/decode round-trip for both types
  - Apply: pool grows on add, snapshot refs update on remove

**Status:** Complete

---

### Stage 4: InlineManifestTableOperations + FileIOCatalog config

**Goal:** A new `TableOperations` subclass implements `ManifestListSink`
and stages manifest list deltas. The catalog property gates its activation.

**Deliverables:**

- `InlineManifestTableOperations extends FileIOTableOperations implements ManifestListSink`:
  ```java
  private final Map<Long, ManifestListDelta> stagedDeltas = new LinkedHashMap<>();

  @Override
  public void stageManifestListDelta(
      long sequenceNumber, long snapshotId, Long parentSnapshotId,
      Long nextRowId, ManifestListDelta delta, Long nextRowIdAfter) {
    stagedDeltas.put(snapshotId, delta);
  }

  Map<Long, ManifestListDelta> drainStagedDeltas() {
    Map<Long, ManifestListDelta> result = new LinkedHashMap<>(stagedDeltas);
    stagedDeltas.clear();
    return result;
  }
  ```
- `FileIOCatalog`:
  - New constant: `INLINE_MANIFESTS = "fileio.catalog.inline.manifests"`
  - Validation in `initialize()`: reject `inline.manifests=true` without
    `inline=true`
  - `newTableOps()`: pick subclass based on config
- **Tests:** Unit test that `ManifestListSink.stageManifestListDelta` stashes
  deltas correctly; drain returns and clears.

**Status:** Complete

---

### Stage 5: commitInline ML integration

**Goal:** The inline commit path drains staged ML deltas and attaches them
to the intention record alongside the TM delta.

**Deliverables:**

- `commitInline` in `FileIOTableOperations` (or the subclass override):
  after `InlineDeltaCodec.computeDelta(base, metadata, prefix)`, walk new
  snapshots in `metadata`, pull `ManifestListDelta` from `drainStagedDeltas()`,
  call `attachManifestDelta(delta, snapshotId, added, removedPaths)`.
- `selectMode`: inline ML deltas are small (~150-300 B per add); they don't
  change mode selection materially. No change needed.
- **Tests:** mock the sink, verify that a FastAppend commit through the
  inline path produces a delta with both TM and ML updates.

**Status:** Complete

---

### Stage 6: loadFromCatalogFile ML integration

**Goal:** When loading an inline table whose snapshots have inline ML,
wrap each such snapshot with `InlineSnapshot`.

**Deliverables:**

- Extend `loadFromCatalogFile` inline branch: after parsing `TableMetadata`
  from JSON, post-process snapshots. For each snapshot where
  `proto.hasInlineManifests(tblId, snapshotId)` is true:
  - Resolve `List<ManifestFile>` from `proto.inlineManifests(tblId, snapId)`
  - Construct `InlineSnapshot` with the snapshot's fields + resolved manifests
  - Replace the snapshot in the `TableMetadata` (see errata E-ML3 for the
    builder approach; prototype and determine safest replacement strategy)
- Snapshots without inline ML (older pointer-mode snapshots) remain as
  `BaseSnapshot` with their original `manifestListLocation`.
- **Tests:** end-to-end: commit via inline ML path, re-read catalog,
  verify `snapshot.allManifests(io)` returns correct list without reading
  any `snap-*.avro` file.

**Status:** Complete

---

### Stage 7: End-to-end tests + parameterized test suite

**Goal:** Full test coverage across all inline configurations. Validate that
all catalog operations produce identical results regardless of inline mode.

See "Parameterized Test Strategy" section below for details.

**Status:** Complete

---

## Parameterized Test Strategy

### Goal

Run identical catalog test scenarios across all three valid inline
configurations, ensuring behavioral equivalence:

| Config     | `inline` | `inline.manifests` | Description              |
|------------|----------|--------------------|--------------------------|
| `BASELINE` | false    | false              | Pointer TM, Avro ML      |
| `TM_ONLY`  | true     | false              | Inline TM, Avro ML       |
| `TM_ML`    | true     | true               | Inline TM + inline ML    |

### JUnit 5 infrastructure

The project uses JUnit 5.10.1 with `junit-jupiter-params`. Key features:

- `@ParameterizedTest` + `@EnumSource` -- already used in
  `CatalogTransactionTests` for isolation levels
- `@MethodSource` -- available but not yet used in the project
- `@Nested` classes -- used extensively in `TestProtoActions`, `TestInlineDelta`

### Approach: `InlineConfig` enum + `@MethodSource`

```java
public enum InlineConfig {
  BASELINE(false, false),
  TM_ONLY(true, false),
  TM_ML(true, true);

  public final boolean inlineTM;
  public final boolean inlineML;

  InlineConfig(boolean inlineTM, boolean inlineML) {
    this.inlineTM = inlineTM;
    this.inlineML = inlineML;
  }

  public Map<String, String> catalogProperties() {
    Map<String, String> props = new HashMap<>();
    props.put("fileio.catalog.inline", String.valueOf(inlineTM));
    props.put("fileio.catalog.inline.manifests", String.valueOf(inlineML));
    return props;
  }
}
```

### Test layers

**Layer 1: Format-level tests (TestProtoActions, TestInlineDelta)**

These test the protobuf codec and catalog state machine directly, without
`FileIOCatalog`. They exercise the checkpoint + transaction replay path.

*Not parameterized* -- they test a single code path (the proto format)
regardless of feature flags. ML extensions get their own `@Nested` classes
alongside the existing `InlineTableTests`.

**Layer 2: Catalog API tests (CatalogTests / CatalogTransactionTests)**

These are the ~150+ abstract test methods in `CatalogTests<C>` and the ~20
parameterized-by-isolation-level methods in `CatalogTransactionTests<C>`.
Currently only exercised by cloud-provider subclasses in pointer mode.

*Parameterized by `InlineConfig`:*

```java
public class TestFileIOCatalogInline {

  /**
   * Base class that wires up a MockIO-backed FileIOCatalog with a given config.
   */
  abstract static class ConfiguredCatalogTests extends CatalogTests<FileIOCatalog> {
    private FileIOCatalog catalog;

    abstract InlineConfig config();

    @BeforeEach
    void setUp(TestInfo info) {
      MockIO io = new MockIO();  // in-memory atomic FileIO
      Map<String, String> props = new HashMap<>(config().catalogProperties());
      props.put(CatalogProperties.WAREHOUSE_LOCATION, "mem:///warehouse");
      catalog = new FileIOCatalog(
          "test", "mem:///catalog", null, new ProtoCatalogFormat(), io, props);
      catalog.initialize(info.getTestMethod().get().getName(), props);
    }

    @Override
    protected FileIOCatalog catalog() { return catalog; }

    @Override
    protected boolean requiresNamespaceCreate() { return true; }
  }

  @Nested class Baseline extends ConfiguredCatalogTests {
    @Override InlineConfig config() { return InlineConfig.BASELINE; }
  }

  @Nested class TmOnly extends ConfiguredCatalogTests {
    @Override InlineConfig config() { return InlineConfig.TM_ONLY; }
  }

  @Nested class TmMl extends ConfiguredCatalogTests {
    @Override InlineConfig config() { return InlineConfig.TM_ML; }
  }
}
```

This gives 3 x 150+ = 450+ test executions from the same abstract suite,
validating behavioral equivalence. Each `@Nested` class appears as a
separate test group in IDE runners and CI output.

The `@Nested`-subclass pattern is chosen over `@ParameterizedTest` at the
method level because `CatalogTests` uses `@Test` annotations (not
`@ParameterizedTest`) and JUnit 5 doesn't support parameterizing an entire
inherited test class via `@MethodSource`. The `@Nested` approach inherits
all `@Test` methods and only overrides setup.

The same pattern applies to `CatalogTransactionTests`:

```java
public class TestFileIOCatalogTransactionInline {
  abstract static class ConfiguredTxnTests
      extends CatalogTransactionTests<FileIOCatalog> {
    abstract InlineConfig config();
    // ... similar setUp ...
  }

  @Nested class Baseline extends ConfiguredTxnTests { ... }
  @Nested class TmOnly extends ConfiguredTxnTests { ... }
  @Nested class TmMl extends ConfiguredTxnTests { ... }
}
```

**Layer 3: ML-specific tests**

Tests that only apply to inline ML (not parameterized across configs):

- `TestInlineManifestActions` -- proto-level: manifest pool checkpoint
  round-trip, ML delta encode/decode, delta application to catalog state
- `TestInlineManifestEndToEnd` -- catalog-level: commit with ML sink active,
  verify no `snap-*.avro` written, verify `allManifests()` returns correct
  list after catalog reload
- `TestInlineManifestOperations` -- per-operation-type: FastAppend (1 add),
  Delete (add + remove), MergeAppend (multiple adds + removes), snapshot
  expiration (removes only), compaction (adds + removes)
- `TestInlineManifestMixed` -- gradual migration: table starts with pointer
  ML, switches to inline, verify old snapshots still load from Avro

**Layer 4: Cloud integration tests (deferred)**

The existing `GCSCatalogTest`, `TestS3Catalog`, `ADLSCatalogTest` can be
extended with the same `@Nested`-per-config pattern once local tests pass.
Deferred until the in-memory test suite is green.

### MockIO for local testing

`TestProtoCommitKnobs` already has a `MockIO` (lines 53-150) that implements
`SupportsAtomicOperations` with in-memory storage. This needs to be extracted
to a shared test utility and potentially extended to:

- Track which files were written (to assert no `snap-*.avro` files created)
- Support multiple concurrent catalog files (for transaction tests)

### Validation assertions

For parameterized tests, the key assertion is **behavioral equivalence**:
the same sequence of catalog operations produces the same observable state
(tables, namespaces, properties, metadata, snapshot lists) regardless of
inline config. The internal representation differs (pointer vs inline), but
the `CatalogTests` API-level assertions already cover the external contract.

For ML-specific tests, additional assertions:

- `snapshot.manifestListLocation()` is `null` for inline ML snapshots
- `snapshot.allManifests(io)` returns the expected manifest list
- No `snap-*.avro` files exist in the table's metadata directory
- Manifest files (`.avro`) themselves are still written externally
- After catalog reload (checkpoint + log replay), ML state is preserved

---

## Experiment Matrix

Once all stages are complete, the following configurations exercise the
three modes for benchmarking:

| Experiment                  | Config   | What we measure                        |
|-----------------------------|----------|----------------------------------------|
| Baseline (no inline)        | BASELINE | Write cost floor: 3 files per commit   |
| TM-only                     | TM_ONLY  | Savings from inlining metadata.json    |
| TM+ML                       | TM_ML    | Combined savings: 1 write per commit   |
| TM+ML, high manifest count  | TM_ML    | Checkpoint size with 100-1000 manifests|
| TM+ML, random ops mix       | TM_ML    | Non-FastAppend operations (delete, merge, rewrite) |

Key metrics per experiment:
- Bytes written per commit (catalog + metadata + ML files)
- Number of storage operations per commit
- Checkpoint size growth rate
- Catalog read latency (checkpoint + log replay)

---

## Progress Log

| Date | Stage | Notes |
|------|-------|-------|
| 2026-04-16 | Stage 1 | ManifestFileEntry/PartitionFieldSummaryEntry/SnapshotManifestRefs protos, ProtoCodec encode/decode, DecodedManifestFile. 6 tests |
| 2026-04-16 | Stage 2 | ProtoCatalogFile manifest pool + snapshot refs. Checkpoint encode/decode. 3 tests |
| 2026-04-17 | Stage 3 | AddManifestUpdate/RemoveManifestUpdate DeltaUpdate types, attachManifestDelta helper. 5 tests |
| 2026-04-17 | Stage 4 | InlineManifestTableOperations + ManifestListSink, feature flag wiring, config validation. 3 tests |
| 2026-04-17 | Stage 5 | commitInline drains staged ML deltas, attaches to intention record |
| 2026-04-17 | Stage 6 | loadFromCatalogFile wraps snapshots with InlineSnapshot via wrapInlineManifests() |
| 2026-04-17 | Stage 7 | MemoryFileIO, InlineConfig enum, end-to-end tests per config (BASELINE/TM_ONLY/TM_ML). 4 tests |
