# Catalog Format Errata

Pending and deferred design decisions for the log-based catalog format.

## Deferred: Forward Compatibility for Columnar Regions

Protobuf's unknown field preservation works well for sparse/structural data (headers,
transaction records), but doesn't directly apply to columnar-encoded regions within
the checkpoint.

**Open questions:**
- How should readers handle unknown columns within a region?
- Should each column carry its own schema version/tag?
- Can we use a protobuf envelope around columnar data to get unknown field semantics?

**Potential approaches:**
1. Region-level versioning with fallback to raw bytes
2. Column descriptors with type tags (similar to Parquet schema)
3. Hybrid: protobuf message with `bytes` fields for columnar data

## Deferred: Inlined Table Metadata

Embedding JSON table metadata directly in the catalog file could reduce I/O for
common operations and enable cross-table deduplication.

**Optimization opportunities:**
- String dictionary for repeated schema field names, column names, types
- Location templates: `s3://bucket/warehouse/{ns}/{table}/metadata/{version}.metadata.json`
- Delta-encoded partition specs across similar tables
- Shared schema fragments (many tables share identical column definitions)

**Open questions:**
- Parse and re-encode JSON, or deduplicate raw strings?
- Granularity: full metadata vs. hot fields (schema, partition spec, current snapshot)?
- Invalidation: how to handle metadata that changes outside the catalog?
- Size budget: when does inlining hurt more than help?

## Deferred: Advanced Encodings

Potential columnar optimizations for future consideration:

- **RLE for version columns**: Many entries at version 1
- **Delta-RLE for sequential IDs**: nsid, tbl_id often sequential
- **Prefix compression for string dictionary**: Common path prefixes
- **UUIDv7 timestamp delta encoding**: Committed transactions from similar timeframes

These are deferred until the base format is stable and benchmarks indicate need.

## Pending: Cross-Project Test Synchronization

The abstract test classes `CatalogTests` and `CatalogTransactionTests` are duplicated between:
- `iceberg/core/src/test/java/org/apache/iceberg/catalog/`
- `fileio-catalog/src/test/java/org/apache/iceberg/catalog/`

**Current state:**
- fileio-catalog copies these from iceberg to avoid circular dependencies
- Changes in iceberg must be manually synced to fileio-catalog

**Potential solutions:**
1. Extract shared test infrastructure to a separate module
2. Use Git submodule or subtree for shared test code
3. Publish iceberg test-jar and depend on it (current approach, but fragile)

## Resolved: LogCatalogFormat and CASCatalogFormat Removed

LogCatalogFormat and CASCatalogFormat (including `LogCatalogFile`, `LogSerializer`,
`LogMutator`, `LogIdManager`) have been removed. ProtoCatalogFormat is now the only
catalog format. The known LCF serialization issues (naive DataOutputStream encoding,
no schema evolution, no per-record checksums) are addressed by protobuf's wire
format and the separated `ProtoCodec` / `InlineDeltaCodec` classes.

## Pending: Cloud Provider Integration Tests

Integration tests for AWS S3 and Azure ADLS require:
- **S3**: LocalStack or real AWS credentials
- **Azure**: Azurite emulator or real Azure credentials
- **GCS**: fake-gcs-server or real GCP credentials

Currently, tests are skipped without credentials. Consider:
1. Testcontainers setup for automated emulator lifecycle
2. CI/CD pipeline with secret management
3. Mock-based unit tests as fallback

## INLINE_ML.md Errata (2026-04-16)

Discrepancies between INLINE_ML.md and the actual codebase, discovered during
implementation planning for inline manifest lists.

### E-ML1. `InlineSnapshot` access modifier is already resolved

The spec (INLINE_ML.md lines 928-938) discusses that `InlineSnapshot` is
package-private and proposes making it public. Commit `72159117e` on
`vldb-1.10.1-ml` already made it public. The alternatives discussion
(factory method, reflection) is moot.

### E-ML2. `manifestListPrefix` vs `manifest_path_prefix` naming

The spec uses three names interchangeably:

- `manifest_list_prefix` -- field 6 in `InlineTable` (catalog.proto:75),
  returned by `ProtoCatalogFile.manifestListPrefix(tblId)`
- `manifestPathPrefix` -- proposed `InlineTable` extension (INLINE_ML.md:443)
- `manifest_path_prefix` -- proposed protobuf field (INLINE_ML.md:1019)

These are conceptually the same: a shared prefix for manifest file paths
within a table's metadata directory. The existing name `manifest_list_prefix`
is slightly misleading since it prefixes manifest *file* paths, not manifest
*list* paths.

**Decision:** Keep existing `manifest_list_prefix` / `manifestListPrefix`
to avoid a migration. Add a clarifying comment.

### E-ML3. `TableMetadata.Builder` snapshot replacement API

The spec (INLINE_ML.md lines 902-916) proposes:

```java
builder.removeSnapshots(ImmutableList.of(s));
builder.addSnapshot(wrapped);
```

But `removeSnapshots()` takes `List<Long>` (snapshot IDs), not
`List<Snapshot>`. The corrected call is:

```java
builder.removeSnapshots(ImmutableList.of(s.snapshotId()));
builder.addSnapshot(wrapped);
```

Even corrected, removing and re-adding snapshots may trigger builder side
effects (snapshot log updates, ref revalidation). Needs prototyping during
implementation to determine if a lower-level approach is required.

### E-ML4. `AddSnapshotUpdate.applyTo` needs ML-aware path

The existing `InlineDeltaCodec.AddSnapshotUpdate` (InlineDeltaCodec.java:572)
requires `manifestListSuffix` to construct the snapshot's
`manifestListLocation`. With inline ML, new snapshots have no manifest list
file. `applyTo` needs a code path where `manifestListSuffix` is null/empty,
producing an `InlineSnapshot` instead of a `BaseSnapshot`. This is a
functional change to the existing class, not just a new class.

### E-ML5. Pool index stability on snapshot deletion

The pool + index design (INLINE_ML.md lines 803-813) stores unique
ManifestFile entries by position. Expiring a snapshot can't simply delete
pool entries (indices would shift). Options:

1. Tombstone slots (null, skip during iteration)
2. Rebuild pool on checkpoint write
3. Path-keyed maps in memory, convert to index arrays only for serialization

**Decision:** Option 3. Use `Map<String, ManifestFile>` keyed by path in
memory. Convert to positional arrays only during checkpoint write. This
avoids the index-stability problem entirely and simplifies the Java API.

### E-ML6. No end-to-end inline test coverage via `FileIOCatalog`

The existing tests exercise inline at the proto-format level
(`TestProtoActions.InlineTableTests`, `TestInlineDelta`) but never set
`fileio.catalog.inline=true` on a `FileIOCatalog` instance. All cloud
integration tests (`GCSCatalogTest`, `TestS3Catalog`, `ADLSCatalogTest`)
use pointer mode exclusively. The `loadFromCatalogFile` inline path and
`commitInline` path have no end-to-end test coverage through the catalog API.

This is a pre-existing gap. The parameterized test strategy addresses it.
