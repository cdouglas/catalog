# Inline Table Metadata -- Errata

Design questions, discovered issues, and deferred decisions encountered during
implementation of inline table metadata for the PB catalog format.

## Pre-existing Issues (discovered during test development)

### E1: readInternal does not update nextNamespaceId / nextTableId

`ProtoCatalogFormat.readInternal()` applies transactions to the builder but
does not advance the `nextNamespaceId` / `nextTableId` counters when
`CreateNamespaceAction` or `CreateTableAction` allocate new IDs. This means
a `ProtoCatalogFile` read from bytes with applied transactions has stale
counters.

**Impact:** Any code that re-checkpoints such a catalog (e.g., compaction) or
builds new transactions from it must independently compute correct next IDs.
The real commit path handles this via `ProtoIdManager`, but test helpers and
any future code that reads + re-writes must be aware.

**Recommendation:** Either (a) update counters during action apply, or
(b) compute max(existing IDs) + 1 at `build()` time. Deferred since the
real commit path is correct.

### E2: Root namespace (id=0) must exist for Mut.createNamespace()

`CatalogFile.Mut.createNamespace()` auto-creates ancestor namespaces when
they don't exist in `original`. If root is missing, it creates a
`CreateNamespaceAction` with `name=""`, which on replay becomes
`Namespace.of("")` (1 level) instead of `Namespace.empty()` (0 levels).

**Impact:** All PB catalogs must include root namespace (id=0) in the
checkpoint. The random test generator was fixed to include it. Production
`ProtoCatalogFile.empty()` should also ensure root is present.

**Status:** Test generator fixed. Production path should be audited.

### E3: ProtoCatalogFile lacks equals()

Unlike `LogCatalogFile`, `ProtoCatalogFile` has no `equals()` / `hashCode()`.
Tests use `assertCatalogEquals()` to compare observable state field-by-field.

**Recommendation:** Add `equals()` / `hashCode()` to `ProtoCatalogFile` once
the state model stabilizes (after inline metadata is added, the fields will
change).

## Inline-Specific Design Questions

### E4: Opaque bytes vs parsed TableMetadata in state model

Stage 1 stores inline table metadata as `byte[]` (opaque JSON). This avoids
coupling the checkpoint codec to `TableMetadataParser` and keeps the state
model simple. However, delta application (Stage 4+) requires parsed
`TableMetadata` objects.

**Options:**
1. Parse lazily on first delta apply, cache the result.
2. Always parse at checkpoint load time.
3. Store both: bytes in checkpoint, parsed in memory.

Option 1 is cleanest for the checkpoint path but adds complexity. Option 2
is simpler but slower for catalogs with many inline tables that are only read,
never updated. Decide at Stage 4.

### E5: Inline table in `location()` API

The existing `CatalogFile.location(TableIdentifier)` returns a metadata file
path string. For inline tables, there is no external file. Options:

1. Return `null` -- callers must check `inlineMetadata()` separately.
2. Return a synthetic URI (e.g., `catalog://inline/<tblId>`).
3. Add a new method `tableMetadata(TableIdentifier)` that returns either
   inline metadata or reads from the pointer location.

Option 3 is the cleanest API but requires broader refactoring of
`BaseMetastoreCatalog` integration. Option 1 is simplest for initial stages.
Decide at Stage 1.

### E6: Manifest list prefix computation

The spec defines `manifest_list_prefix` as the shared prefix for all manifest
list paths in a table (typically `<table-location>/metadata/snap-`). This
must be computed at:

- **Table creation:** derived from the table location.
- **Compaction (pointer->inline):** derived from existing snapshot paths.
- **Delta application:** used to reconstruct full paths from suffixes.

Edge cases: tables with no snapshots yet (prefix is empty or derived from
location), tables whose manifest list paths don't share a common prefix
(fall back to empty prefix, suffixes carry full paths).

### E8: Integration path -- no base class changes needed

`BaseMetastoreTableOperations` exposes a 4-argument overload:

```java
protected void refreshFromMetadataLocation(
    String newLocation,
    Predicate<Exception> shouldRetry,
    int numRetries,
    Function<String, TableMetadata> metadataLoader)
```

For inline tables, `FileIOTableOperations.updateVersionAndMetadata()` can call
this with a custom `metadataLoader` that deserializes from the inline bytes
instead of reading from storage. A synthetic location string (e.g.,
`inline://<tblId>#v<version>`) satisfies the location-change detection.

Similarly, `doCommit()` can serialize new metadata to bytes and produce
`CreateTableInline` / `UpdateTableInline` actions instead of writing an
external file and producing pointer actions.

**No modifications to `BaseMetastoreCatalog` or `BaseMetastoreTableOperations`
in the iceberg/ fork are required.** All interception happens in
`FileIOTableOperations` (our inner class) and `CatalogFile.Mut` (our code).

### E7: StringDictionary (checkpoint field 14)

The catalog-wide string dictionary is marked optional in the spec. It adds
complexity (prefix-coded strings with `\x00` + varint index) for a modest
space saving (shared warehouse prefix). 

**Recommendation:** Defer to a later stage. Implement without the dictionary
first; add it as an optimization if checkpoint size becomes an issue.
