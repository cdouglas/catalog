/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.io;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestListSink;
import org.apache.iceberg.ManifestListSink.ManifestListDelta;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.InlineSnapshot;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.BaseCatalogTransaction;
import org.apache.iceberg.catalog.CatalogTransaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsCatalogTransactions;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.LocationUtil;

public class FileIOCatalog extends BaseMetastoreCatalog
    implements SupportsNamespaces, SupportsCatalogTransactions {
  // TODO audit loadTable in BaseMetastoreCatalog
  // TODO buildTable overridden in BaseMetastoreCatalog?

  private static final String INLINE_ENABLED = "fileio.catalog.inline";
  private static final String INLINE_MANIFESTS = "fileio.catalog.inline.manifests";
  // When true, loadTable rejects a TableMetadata that has both inline-ML and
  // pointer-mode snapshots instead of warning. Default is the lenient warn path
  // because rejection breaks tables mid-migration.
  private static final String INLINE_STRICT = "fileio.catalog.inline.strict";

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(FileIOCatalog.class);

  private String catalogName = "fileio";
  private String catalogLocation;
  private String warehouseLocation;
  private CatalogFormat<?, ?> format;
  private SupportsAtomicOperations fileIO;
  private final Map<String, String> catalogProperties;

  // Per-thread map of tables loaded via loadTable() to the ops instance that
  // received their staged ManifestListSink deltas. Populated lazily on
  // loadTable, drained by commitTransaction (multi-table catalog commit), then
  // cleared. The single-table commitInline path drains the sink directly off
  // `this` and does not consult this map. See iceberg_refine.md R2.
  private final ThreadLocal<Map<TableIdentifier, InlineManifestTableOperations>>
      txnSinkOps = ThreadLocal.withInitial(HashMap::new);

  @SuppressWarnings("unused") // reflection cstr
  FileIOCatalog() {
    // XXX Isn't using Maps.newHashMap() deprecated after Java7?
    catalogProperties = Maps.newHashMap();
  }

  public FileIOCatalog(
      String catalogName,
      String catalogLocation,
      CatalogFormat<?, ?> format,
      SupportsAtomicOperations fileIO,
      Map<String, String> catalogProperties) {
    this.catalogName = catalogName;
    this.catalogLocation = catalogLocation;
    this.format = format;
    this.fileIO = fileIO;
    this.catalogProperties = catalogProperties;
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    super.initialize(name, properties);
    Preconditions.checkNotNull(properties, "Properties are required");
    catalogProperties.putAll(properties);

    if (name != null) {
      catalogName = name;
    }

    // Validate config: inline.manifests requires inline
    boolean inlineEnabled = Boolean.parseBoolean(
        properties.getOrDefault(INLINE_ENABLED, "false"));
    boolean inlineManifests = Boolean.parseBoolean(
        properties.getOrDefault(INLINE_MANIFESTS, "false"));
    Preconditions.checkArgument(
        !inlineManifests || inlineEnabled,
        "fileio.catalog.inline.manifests=true requires fileio.catalog.inline=true");

    warehouseLocation = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
    if (null == catalogLocation) {
      Preconditions.checkArgument(
          !Strings.isNullOrEmpty(warehouseLocation),
          "Cannot initialize FileIOCatalog because warehousePath must not be null or empty");
      catalogLocation = LocationUtil.stripTrailingSlash(warehouseLocation) + "/catalog";
    }
    if (null == format) {
      format = new ProtoCatalogFormat(properties);
    }
    if (null == fileIO) {
      // TODO check warehouseLocation schema?
      String fileIOImpl =
          properties.getOrDefault(
              CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.gcp.gcs.GCSFileIO");

      // TODO handle this more gracefully; use listings/HadoopCatalog?
      // TODO remove generics
      fileIO = (SupportsAtomicOperations) CatalogUtil.loadFileIO(fileIOImpl, properties, null);
    }
    final InputFile catalogFile = fileIO.newInputFile(catalogLocation);
    if (!catalogFile.exists()) {
      format.empty(catalogFile).commit(fileIO);
    }
  }

  @Override
  protected Map<String, String> properties() {
    return catalogProperties;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    if (!namespaceExists(namespace) && !namespace.isEmpty()) {
      throw new NoSuchNamespaceException(
          "Cannot list tables for namespace. Namespace does not exist: %s", namespace);
    }
    return getCatalogFile().tables().stream()
        .filter(t -> t.namespace().isEmpty() || t.namespace().equals(namespace))
        .sorted(Comparator.comparing(TableIdentifier::toString))
        .collect(Collectors.toList());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    final InputFile catalog = fileIO.newInputFile(catalogLocation);
    final CatalogFile catalogFile = format.read(fileIO, catalog);
    try {
      format.from(catalogFile).dropTable(identifier).commit(fileIO);
      return true;
    } catch (CommitFailedException | NoSuchTableException e) {
      return false;
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    final InputFile catalog = fileIO.newInputFile(catalogLocation);
    final CatalogFile catalogFile = format.read(fileIO, catalog);
    format.from(catalogFile).renameTable(from, to).commit(fileIO);
  }

  @Override
  public Table loadTable(TableIdentifier identifier) {
    Table tbl = super.loadTable(identifier);
    // Track InlineManifestTableOperations instances so commitTransaction can
    // drain the staged ManifestListSink deltas directly, mirroring the
    // single-table commitInline path. The check is intentionally permissive:
    // if the loaded table doesn't expose ops, or the ops isn't a sink, we
    // simply don't track it; commitTransaction tolerates a missing entry.
    if (tbl instanceof HasTableOperations) {
      TableOperations ops = ((HasTableOperations) tbl).operations();
      if (ops instanceof InlineManifestTableOperations) {
        txnSinkOps.get().put(identifier, (InlineManifestTableOperations) ops);
      }
    }
    return tbl;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    boolean inline = Boolean.parseBoolean(
        catalogProperties.getOrDefault(INLINE_ENABLED, "false"));
    boolean inlineManifests = Boolean.parseBoolean(
        catalogProperties.getOrDefault(INLINE_MANIFESTS, "false"));
    boolean inlineStrict = Boolean.parseBoolean(
        catalogProperties.getOrDefault(INLINE_STRICT, "false"));
    if (inlineManifests) {
      return new InlineManifestTableOperations(
          tableIdentifier, catalogLocation, format, fileIO, inline, inlineStrict);
    }
    return new FileIOTableOperations(
        tableIdentifier, catalogLocation, format, fileIO, inline, inlineStrict);
  }

  FileIOTableOperations newTableOps(TableIdentifier tableIdentifier, CatalogFile catalogFile) {
    boolean inline = Boolean.parseBoolean(
        catalogProperties.getOrDefault(INLINE_ENABLED, "false"));
    boolean inlineManifests = Boolean.parseBoolean(
        catalogProperties.getOrDefault(INLINE_MANIFESTS, "false"));
    boolean inlineStrict = Boolean.parseBoolean(
        catalogProperties.getOrDefault(INLINE_STRICT, "false"));
    if (inlineManifests) {
      return new InlineManifestTableOperations(
          tableIdentifier, catalogLocation, format, fileIO, inline, inlineStrict, catalogFile);
    }
    return new FileIOTableOperations(
        tableIdentifier, catalogLocation, format, fileIO, inline, inlineStrict, catalogFile);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    return String.join(
        "/", defaultNamespaceLocation(tableIdentifier.namespace()), tableIdentifier.name());
  }

  private String defaultNamespaceLocation(Namespace namespace) {
    if (namespace.isEmpty()) {
      return warehouseLocation;
    }
    return warehouseLocation + "/" + String.join("/", namespace.levels());
  }

  private CatalogFile getCatalogFile() {
    final InputFile catalog = fileIO.newInputFile(catalogLocation);
    return format.read(fileIO, catalog);
  }

  //
  // SupportsNamespaces
  //

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    final InputFile catalog = fileIO.newInputFile(catalogLocation);
    final CatalogFile catalogFile = format.read(fileIO, catalog);
    format.from(catalogFile).createNamespace(namespace, metadata).commit(fileIO);
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    CatalogFile catalogFile = getCatalogFile();
    if (!namespace.isEmpty() && !catalogFile.namespaces().contains(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    return catalogFile.namespaces().stream()
        .filter(ns -> !ns.isEmpty())
        .filter(ns -> ns.length() == namespace.length() + 1)
        .filter(ns -> {
          // Direct child: prefix matches `namespace` and has exactly one extra level
          for (int i = 0; i < namespace.length(); i++) {
            if (!ns.level(i).equals(namespace.level(i))) {
              return false;
            }
          }
          return true;
        })
        .sorted(Comparator.comparing(Namespace::toString))
        .collect(Collectors.toList());
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    CatalogFile catalogFile = getCatalogFile();
    if (catalogFile.namespaces().contains(namespace)) {
      return catalogFile.namespaceProperties(namespace);
    } else {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    final InputFile catalog = fileIO.newInputFile(catalogLocation);
    // XXX TODO wait, wtf?
    //     TODO catalog ops must also follow the refresh cycle, or only TableOperations detect
    // concurrent changes?
    final CatalogFile catalogFile = format.read(fileIO, catalog);
    try {
      format.from(catalogFile).dropNamespace(namespace).commit(fileIO);
    } catch (NoSuchNamespaceException e) {
      return false; // sigh.
    }
    return true;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    final CatalogFile catalogFile = getCatalogFile();
    try {
      format.from(catalogFile).updateProperties(namespace, properties).commit(fileIO);
    } catch (CommitFailedException e) {
      return false; // sigh.
    }
    return true;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties)
      throws NoSuchNamespaceException {
    return setProperties(
        namespace,
        properties.stream().collect(Maps::newHashMap, (m, k) -> m.put(k, null), Map::putAll));
  }

  static class FileIOTableOperations extends BaseMetastoreTableOperations {
    private final String catalogLocation;
    private final TableIdentifier tableId;
    private final CatalogFormat format;
    private final SupportsAtomicOperations fileIO;
    private final boolean inlineEnabled;
    private final boolean inlineStrict;
    private volatile CatalogFile lastCatalogFile = null;

    FileIOTableOperations(
        TableIdentifier tableId,
        String catalogLocation,
        CatalogFormat format,
        SupportsAtomicOperations fileIO,
        boolean inlineEnabled,
        boolean inlineStrict) {
      this(tableId, catalogLocation, format, fileIO, inlineEnabled, inlineStrict, null);
    }

    FileIOTableOperations(
        TableIdentifier tableId,
        String catalogLocation,
        CatalogFormat format,
        SupportsAtomicOperations fileIO,
        boolean inlineEnabled,
        boolean inlineStrict,
        CatalogFile catalogFile) {
      this.fileIO = fileIO;
      this.format = format;
      this.tableId = tableId;
      this.catalogLocation = catalogLocation;
      this.inlineEnabled = inlineEnabled;
      this.inlineStrict = inlineStrict;
      this.lastCatalogFile = catalogFile;
      if (catalogFile != null) {
        loadFromCatalogFile(catalogFile);
        disableRefresh();
      }
    }

    @Override
    public SupportsAtomicOperations io() {
      return fileIO;
    }

    /**
     * Loads table metadata from a CatalogFile, handling both pointer and inline tables.
     */
    private synchronized void loadFromCatalogFile(CatalogFile catalogFile) {
      String metadataFile = catalogFile.location(tableId);
      if (metadataFile != null) {
        // Pointer table: load metadata from external file
        refreshFromMetadataLocation(metadataFile);
      } else if (catalogFile.isInlineTable(tableId)) {
        // Inline table: load metadata from catalog bytes.
        // The synthetic location is the cache key for
        // BaseMetastoreTableOperations.refreshFromMetadataLocation: it must be
        // identical across reads of the same logical state and distinct after
        // any commit. The catalog already tracks both — TblEntry.version bumps
        // on every commit and is the authoritative monotonic counter — so we
        // derive the location from (tableId, version) directly. No content
        // hashing, no collision worry.
        byte[] inlineMeta = catalogFile.inlineMetadata(tableId);
        String json = new String(inlineMeta, StandardCharsets.UTF_8);
        int version = -1;
        if (catalogFile instanceof ProtoCatalogFile) {
          ProtoCatalogFile proto = (ProtoCatalogFile) catalogFile;
          Integer numId = proto.tableId(tableId);
          if (numId != null) {
            version = proto.tableVersion(numId);
          }
        }
        String syntheticLoc = "inline://" + tableId + "#" + version;
        refreshFromMetadataLocation(syntheticLoc, null, 0,
            loc -> wrapInlineManifests(
                TableMetadataParser.fromJson(loc, json), catalogFile));
      } else if (currentMetadataLocation() != null) {
        // Table used to exist but is now missing
        throw new NoSuchTableException("Table %s was deleted", tableId);
      } else {
        // Table does not exist yet
        disableRefresh();
      }
    }

    @Override
    protected String tableName() {
      return tableId.toString();
    }

    @Override
    protected void doRefresh() {
      final CatalogFile updatedCatalogFile =
          format.read(fileIO, io().newInputFile(catalogLocation));
      lastCatalogFile = updatedCatalogFile;
      loadFromCatalogFile(updatedCatalogFile);
    }

    // visible from commitTransaction
    String writeUpdateMetadata(boolean isCreate, TableMetadata metadata) {
      return writeNewMetadataIfRequired(isCreate, metadata);
    }

    private boolean shouldInline() {
      return inlineEnabled;
    }

    /**
     * Wraps {@link BaseMetastoreTableOperations#commit} so the post-commit
     * metadata-file cleanup step (driven by
     * {@code TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED}) skips
     * {@code inline://} pseudo-URIs. Those entries appear in
     * {@link TableMetadata#previousFiles} for inline TM tables but don't
     * correspond to real storage objects; handing them to
     * {@code FileIO.deleteFiles} surfaces as a {@code BulkDeletionFailure} on
     * S3 / GCS / ADLS. We re-implement the upstream commit logic instead of
     * post-filtering inside {@link CatalogUtil#deleteRemovedMetadataFiles}
     * because the upstream method consumes {@link TableMetadata} directly.
     */
    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      if (base != current()) {
        if (base != null) {
          throw new CommitFailedException("Cannot commit: stale table metadata");
        } else {
          throw new org.apache.iceberg.exceptions.AlreadyExistsException(
              "Table already exists: %s", tableName());
        }
      }
      if (base == metadata) {
        return;
      }
      doCommit(base, metadata);
      deleteRemovedRealMetadataFiles(base, metadata);
      requestRefresh();
    }

    private void deleteRemovedRealMetadataFiles(TableMetadata base, TableMetadata metadata) {
      if (base == null) {
        return;
      }
      boolean deleteAfterCommit =
          metadata.propertyAsBoolean(
              org.apache.iceberg.TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED,
              org.apache.iceberg.TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT);
      if (!deleteAfterCommit) {
        return;
      }
      Set<TableMetadata.MetadataLogEntry> removed = new HashSet<>(base.previousFiles());
      removed.removeAll(metadata.previousFiles());
      // Drop inline:// pseudo-URIs -- they don't correspond to real files.
      removed.removeIf(entry -> entry.file() != null && entry.file().startsWith("inline://"));
      if (removed.isEmpty()) {
        return;
      }
      Iterable<String> paths =
          removed.stream().map(TableMetadata.MetadataLogEntry::file)::iterator;
      if (io() instanceof org.apache.iceberg.io.SupportsBulkOperations) {
        ((org.apache.iceberg.io.SupportsBulkOperations) io()).deleteFiles(paths);
      } else {
        for (String path : paths) {
          try {
            io().deleteFile(path);
          } catch (RuntimeException e) {
            LOG.warn("Delete failed for previous metadata file: {}", path, e);
          }
        }
      }
    }

    @Override
    public void doCommit(TableMetadata base, TableMetadata metadata) {
      final boolean isCreate = null == base;
      try {
        if (shouldInline()) {
          commitInline(base, metadata, isCreate);
        } else {
          commitPointer(base, metadata, isCreate);
        }
      } catch (SupportsAtomicOperations.CASException e) {
        throw new CommitFailedException(e, "Failed to commit metadata for table %s", tableId);
      }
    }

    private void commitInline(TableMetadata base, TableMetadata metadata, boolean isCreate) {
      if (isCreate) {
        String json = TableMetadataParser.toJson(metadata);
        byte[] metadataBytes = json.getBytes(StandardCharsets.UTF_8);
        format.from(lastCatalogFile)
            .createTableInline(tableId, metadataBytes)
            .commit(io());
      } else {
        // Try delta mode first, fall back to full or pointer
        String manifestPrefix = "";
        if (lastCatalogFile.isInlineTable(tableId)) {
          Integer tblId = null;
          if (lastCatalogFile instanceof ProtoCatalogFile) {
            tblId = ((ProtoCatalogFile) lastCatalogFile).tableId(tableId);
            if (tblId != null) {
              String p = ((ProtoCatalogFile) lastCatalogFile).manifestListPrefix(tblId);
              if (p != null) { manifestPrefix = p; }
            }
          }
        }

        java.util.List<InlineDeltaCodec.DeltaUpdate> delta =
            InlineDeltaCodec.computeDelta(base, metadata, manifestPrefix);

        // Attach manifest list deltas if this ops implements ManifestListSink
        boolean hasMLDeltas = false;
        if (this instanceof InlineManifestTableOperations) {
          Map<Long, InlineManifestTableOperations.StagedSnapshotData> mlDeltas =
              ((InlineManifestTableOperations) this).drainStagedDeltas();
          java.util.Set<Long> oldSnapIds = new java.util.HashSet<>();
          if (base != null) {
            for (org.apache.iceberg.Snapshot s : base.snapshots()) {
              oldSnapIds.add(s.snapshotId());
            }
          }
          for (org.apache.iceberg.Snapshot snap : metadata.snapshots()) {
            if (!oldSnapIds.contains(snap.snapshotId())) {
              InlineManifestTableOperations.StagedSnapshotData staged =
                  mlDeltas.get(snap.snapshotId());
              if (staged != null && delta != null) {
                InlineDeltaCodec.attachManifestDelta(
                    delta, snap.snapshotId(),
                    staged.delta.added(), staged.delta.removedPaths(), manifestPrefix);
                hasMLDeltas = true;
              }
            }
          }
        }

        String mode = InlineDeltaCodec.selectMode(delta, metadata, 0);
        // Force delta mode in two cases:
        // 1. hasMLDeltas: the current commit staged new ML deltas; full/pointer
        //    lose them because they serialize TableMetadata (InlineSnapshot ->
        //    v1 embedded-manifests branch in SnapshotParser.toJson).
        // 2. hasMLPool: the table already has an ML pool from a prior commit; a
        //    full-mode write rewrites the TM JSON with `inline://` sentinel
        //    manifest-list locations, and even though the replay-side full-mode
        //    fix (§1.1) preserves the pool, we still avoid writing bloated full
        //    TM blobs on ML-populated tables. See ML_INLINE_REVIEW2.md §1.1.
        boolean hasMLPool = false;
        if (lastCatalogFile instanceof ProtoCatalogFile) {
          ProtoCatalogFile proto = (ProtoCatalogFile) lastCatalogFile;
          Integer tblIdNum = proto.tableId(tableId);
          if (tblIdNum != null) {
            hasMLPool = !proto.manifestPool(tblIdNum).isEmpty();
          }
        }
        // Don't force delta when computeDelta returned null (e.g. a stats-only
        // change with no representable delta updates) — encodeDelta(null) NPEs
        // and full mode is the correct fallback. See errata D1.
        if ((hasMLDeltas || hasMLPool) && !"delta".equals(mode) && delta != null) {
          mode = "delta";
        }

        CatalogFile.Mut<?, ?> mut = format.from(lastCatalogFile);
        switch (mode) {
          case "delta":
            byte[] deltaBytes =
                InlineDeltaCodec.encodeDelta(delta, metadata.lastUpdatedMillis());
            mut.updateTableInlineDelta(tableId, deltaBytes);
            break;
          case "full":
            String json = TableMetadataParser.toJson(metadata);
            mut.updateTableInline(tableId, json.getBytes(StandardCharsets.UTF_8));
            break;
          case "pointer":
            String loc = writeUpdateMetadata(false, metadata);
            mut.updateTable(tableId, loc);
            break;
        }
        mut.commit(io());
      }
    }

    /**
     * Replaces snapshots with InlineSnapshot when the catalog has inline manifest list data.
     * Snapshots without inline ML data keep their original manifestListLocation (Avro pointer).
     */
    private TableMetadata wrapInlineManifests(TableMetadata parsed, CatalogFile catalogFile) {
      if (!(catalogFile instanceof ProtoCatalogFile)) {
        return parsed;
      }
      ProtoCatalogFile proto = (ProtoCatalogFile) catalogFile;
      Integer tblId = proto.tableId(tableId);
      if (tblId == null) {
        return parsed;
      }

      // §2.3: Any snapshot with an inline:// sentinel manifest-list location
      // MUST have a pool entry — the sentinel is only written when the sink
      // path is active, and the pool and sentinel must be committed atomically.
      // A sentinel without a pool entry means the pool was clobbered (§1.1 bug
      // or similar), and loading the BaseSnapshot as-is would result in
      // FileNotFound when allManifests() tries to read `inline://<id>`. Fail
      // loudly instead of silently corrupting the read.
      for (Snapshot s : parsed.snapshots()) {
        String loc = s.manifestListLocation();
        if (loc != null && loc.startsWith("inline://")
            && !proto.hasInlineManifests(tblId, s.snapshotId())) {
          throw new IllegalStateException(
              "Inline snapshot " + s.snapshotId() + " has sentinel manifest-list "
                  + "location '" + loc + "' but no pool entry — catalog state corrupt");
        }
      }

      // Check if any snapshot has inline manifests, and detect mixed mode
      // (some inline, some pointer-mode in the same table).
      boolean hasAny = false;
      boolean hasInline = false;
      boolean hasPointer = false;
      for (Snapshot s : parsed.snapshots()) {
        if (proto.hasInlineManifests(tblId, s.snapshotId())) {
          hasAny = true;
          hasInline = true;
        } else if (s.manifestListLocation() != null
            && !s.manifestListLocation().startsWith("inline://")) {
          hasPointer = true;
        }
      }
      if (hasInline && hasPointer) {
        // §2.7: mixed-mode table. Default behavior is warn-and-proceed because
        // rejection would break tables mid-migration. Production deployments
        // can opt into hard rejection via fileio.catalog.inline.strict=true,
        // which throws ValidationException here.
        if (inlineStrict) {
          throw new ValidationException(
              "Table %s is in mixed mode: has both inline and pointer-mode "
                  + "snapshots. Strict mode (fileio.catalog.inline.strict=true) "
                  + "rejects mixed-mode loads.",
              tableId);
        }
        LOG.warn(
            "Table {} is in mixed mode: has both inline and pointer-mode snapshots. "
                + "Reload is supported but migration utilities may misbehave.",
            tableId);
      }
      if (!hasAny) {
        return parsed;
      }

      // Build a snapshot replacement map (id -> InlineSnapshot)
      Map<Long, Snapshot> replacements = new java.util.HashMap<>();
      for (Snapshot s : parsed.snapshots()) {
        if (proto.hasInlineManifests(tblId, s.snapshotId())) {
          List<ManifestFile> manifests = proto.inlineManifests(tblId, s.snapshotId());
          replacements.put(s.snapshotId(), new InlineSnapshot(
              s.sequenceNumber(), s.snapshotId(), s.parentId(),
              s.timestampMillis(), s.operation(), s.summary(),
              s.schemaId(), s.firstRowId(), s.addedRows(), s.keyId(),
              manifests));
        }
      }

      // Replace snapshots in-place without using removeSnapshots (which clears refs
      // and doesn't reset lastSequenceNumber, causing validation failures on re-add).
      TableMetadata.Builder builder = TableMetadata.buildFrom(parsed);
      builder.replaceSnapshots(replacements);
      return builder.discardChanges().build();
    }

    private void commitPointer(TableMetadata base, TableMetadata metadata, boolean isCreate) {
      final String newMetadataLocation = writeUpdateMetadata(isCreate, metadata);
      if (isCreate) {
        format.from(lastCatalogFile)
            .createTable(tableId, newMetadataLocation)
            .commit(io());
      } else {
        format.from(lastCatalogFile)
            .updateTable(tableId, newMetadataLocation)
            .commit(io());
      }
    }
  }

  /**
   * Extension of FileIOTableOperations that also implements ManifestListSink,
   * enabling SnapshotProducer to hand off manifest list deltas instead of writing
   * snap-*.avro files. Activated when fileio.catalog.inline.manifests=true.
   */
  static class InlineManifestTableOperations extends FileIOTableOperations
      implements ManifestListSink {

    /** Staged per-snapshot sink data, keyed by snapshot id. */
    static final class StagedSnapshotData {
      final ManifestListDelta delta;
      final Long parentSnapshotId;
      final Long nextRowId;
      final Long nextRowIdAfter;

      StagedSnapshotData(ManifestListDelta delta, Long parentSnapshotId,
          Long nextRowId, Long nextRowIdAfter) {
        this.delta = delta;
        this.parentSnapshotId = parentSnapshotId;
        this.nextRowId = nextRowId;
        this.nextRowIdAfter = nextRowIdAfter;
      }
    }

    private final Map<Long, StagedSnapshotData> stagedDeltas = new LinkedHashMap<>();

    InlineManifestTableOperations(
        TableIdentifier tableId, String catalogLocation,
        CatalogFormat format, SupportsAtomicOperations fileIO,
        boolean inlineEnabled, boolean inlineStrict) {
      super(tableId, catalogLocation, format, fileIO, inlineEnabled, inlineStrict);
    }

    InlineManifestTableOperations(
        TableIdentifier tableId, String catalogLocation,
        CatalogFormat format, SupportsAtomicOperations fileIO,
        boolean inlineEnabled, boolean inlineStrict, CatalogFile catalogFile) {
      super(tableId, catalogLocation, format, fileIO, inlineEnabled, inlineStrict, catalogFile);
    }

    @Override
    public void stageManifestListDelta(
        long sequenceNumber, long snapshotId, Long parentSnapshotId,
        Long nextRowId, ManifestListDelta delta, Long nextRowIdAfter) {
      stagedDeltas.put(snapshotId,
          new StagedSnapshotData(delta, parentSnapshotId, nextRowId, nextRowIdAfter));
    }

    /** Returns and clears all staged sink data (keyed by snapshot id). */
    Map<Long, StagedSnapshotData> drainStagedDeltas() {
      Map<Long, StagedSnapshotData> result = new LinkedHashMap<>(stagedDeltas);
      stagedDeltas.clear();
      return result;
    }
  }

  // SupportsCatalogTransaction

  @Override
  public void commitTransaction(List<TableIdentifier> readTables, List<TableCommit> commits) {
    // TODO XXX This is wrong. The read version needs to be extracted from the spaghetti
    //      XXX in BaseCatalogTransaction and resolved against table identifiers, but I'm
    //      XXX tired of tinkering with this fucking codebase.
    // TableCommit validations check the table UUID and snapshot ref for each table
    // if all validations pass for the current CatalogFile, then attempt atomic replace
    final Map<TableIdentifier, InlineManifestTableOperations> sinkOpsByTable =
        txnSinkOps.get();
    try {
      final CatalogFile current = getCatalogFile();
      final CatalogFile.Mut<?, ?> newCatalog = format.from(current);
      for (TableIdentifier readTable : readTables) {
        final FileIOTableOperations ops = newTableOps(readTable, current);
        newCatalog.readTable(readTable);
      }
      for (TableCommit commit : commits) {
        final TableIdentifier tableId = commit.identifier();
        // use fixed catalog snapshot for validation
        FileIOTableOperations ops = newTableOps(tableId, current);
        final TableMetadata currentMetadata = ops.current();
        commit.requirements().forEach(req -> req.validate(currentMetadata));

        TableMetadata.Builder newMetadataBuilder = TableMetadata.buildFrom(currentMetadata);
        commit.updates().forEach(update -> update.applyTo(newMetadataBuilder));
        final TableMetadata newMetadata = newMetadataBuilder.build();
        if (newMetadata.changes().isEmpty()) {
          continue;
        }
        boolean inline = Boolean.parseBoolean(
            catalogProperties.getOrDefault(INLINE_ENABLED, "false"));
        boolean inlineManifests = Boolean.parseBoolean(
            catalogProperties.getOrDefault(INLINE_MANIFESTS, "false"));
        if (inline) {
          // Compute delta from current to new metadata
          String manifestPrefix = "";
          ProtoCatalogFile proto = null;
          Integer tblIdNum = null;
          if (current instanceof ProtoCatalogFile) {
            proto = (ProtoCatalogFile) current;
            tblIdNum = proto.tableId(tableId);
            if (tblIdNum != null) {
              String p = proto.manifestListPrefix(tblIdNum);
              if (p != null) { manifestPrefix = p; }
            }
          }
          java.util.List<InlineDeltaCodec.DeltaUpdate> delta =
              InlineDeltaCodec.computeDelta(currentMetadata, newMetadata, manifestPrefix);

          // Drain the per-snapshot ManifestListSink deltas staged on the
          // user-loaded ops during the transaction. iceberg-core R2 forwards
          // SnapshotProducer's stageManifestListDelta calls through
          // BaseTransaction.TransactionTableOperationsWithSink to the
          // underlying InlineManifestTableOperations — which is the same
          // instance loadTable() recorded in txnSinkOps. The added/removed
          // sets are the exact ones SnapshotProducer would have written into
          // a snap-*.avro; we attach them directly without rereading them.
          // Single-table commitInline does the same drain off `this`. See
          // iceberg_refine.md R2.
          boolean hasMLPool = proto != null && tblIdNum != null
              && !proto.manifestPool(tblIdNum).isEmpty();
          boolean hasMLDeltas = false;
          if (inlineManifests) {
            InlineManifestTableOperations sinkOps = sinkOpsByTable.get(tableId);
            if (sinkOps != null) {
              Map<Long, InlineManifestTableOperations.StagedSnapshotData> staged =
                  sinkOps.drainStagedDeltas();
              Set<Long> oldSnapIds = new HashSet<>();
              for (Snapshot s : currentMetadata.snapshots()) {
                oldSnapIds.add(s.snapshotId());
              }
              for (Snapshot snap : newMetadata.snapshots()) {
                if (oldSnapIds.contains(snap.snapshotId())) continue;
                InlineManifestTableOperations.StagedSnapshotData data =
                    staged.get(snap.snapshotId());
                if (data != null && delta != null) {
                  InlineDeltaCodec.attachManifestDelta(
                      delta, snap.snapshotId(),
                      data.delta.added(), data.delta.removedPaths(), manifestPrefix);
                  hasMLDeltas = true;
                }
              }
            }
          }

          String mode = InlineDeltaCodec.selectMode(delta, newMetadata, 0);
          // See commitInline — full-mode avoidance, but never with null delta
          // (encodeDelta would NPE; full is the correct fallback). See errata D1.
          if ((hasMLDeltas || hasMLPool) && !"delta".equals(mode) && delta != null) {
            mode = "delta";
          }
          switch (mode) {
            case "delta":
              newCatalog.updateTableInlineDelta(
                  tableId,
                  InlineDeltaCodec.encodeDelta(delta, newMetadata.lastUpdatedMillis()));
              break;
            case "full":
              String json = TableMetadataParser.toJson(newMetadata);
              newCatalog.updateTableInline(tableId, json.getBytes(StandardCharsets.UTF_8));
              break;
            default:
              String loc = ops.writeUpdateMetadata(false, newMetadata);
              newCatalog.updateTable(tableId, loc);
          }
        } else {
          final String newLocation = ops.writeUpdateMetadata(false, newMetadata);
          newCatalog.updateTable(tableId, newLocation);
        }
      }
      try {
        newCatalog.commit(fileIO);
      } catch (SupportsAtomicOperations.CASException e) {
        throw new CommitFailedException(e, "Failed to commit metadata for multi-table commit");
      }
    } finally {
      // Clear the per-thread map. Subsequent loadTable calls will repopulate
      // it. This keeps the map bounded and prevents stale ops references
      // outliving their useful window.
      txnSinkOps.remove();
    }
  }

  @Override
  public CatalogTransaction createTransaction(CatalogTransaction.IsolationLevel isolationLevel) {
    return new BaseCatalogTransaction(this, isolationLevel);
  }
}
