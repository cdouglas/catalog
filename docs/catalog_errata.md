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
