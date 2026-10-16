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

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.ManifestListSink;
import org.apache.iceberg.ManifestListSink.ManifestListDelta;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.InlineSnapshot;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.BaseCatalogTransaction;
import org.apache.iceberg.catalog.CatalogTransaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsCatalogTransactions;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
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

  /**
   * Selects the codec used to encode full inline {@link TableMetadata} bytes
   * (CreateTableInline / UpdateTableInline.full / checkpoint InlineTable).
   * Valid values are "structural" (default — see
   * {@link StructuralInlineMetadataCodec}) and "gzip" (Iceberg-style
   * gzip(JSON), see {@link GzipInlineMetadataCodec}). Decode is always driven
   * by the per-record codec discriminator on the wire, regardless of this
   * setting.
   */
  static final String INLINE_TM_CODEC = "fileio.catalog.inline.tm.codec";
  static final String DEFAULT_INLINE_TM_CODEC = "structural";

  /**
   * The largest inline table update the catalog writes, in bytes: a delta, or the full metadata
   * with the manifest lists of new snapshots. A table whose update does not fit gets a metadata
   * file instead and leaves inline storage until a later update fits in full. Defaults to {@link
   * InlineDeltaCodec#APPEND_LIMIT}, sized to the smallest provider append (ADLS AppendBlock).
   */
  static final String INLINE_MAX_UPDATE_SIZE = "fileio.catalog.inline.max.update.size";

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(FileIOCatalog.class);

  private String catalogName = "fileio";
  private String catalogLocation;
  private String warehouseLocation;
  private CatalogFormat<?, ?> format;
  private SupportsAtomicOperations fileIO;
  private final Map<String, String> catalogProperties;

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

    inlineSettings(); // validates the inline properties

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
      try {
        format.empty(catalogFile).commit(fileIO);
      } catch (CommitFailedException | CommitStateUnknownException e) {
        // Another catalog created it since (two processes starting on a new warehouse), or this
        // one's create landed though its reply was lost: either way, use it.
        if (!fileIO.newInputFile(catalogLocation).exists()) {
          throw e;
        }
      }
    }
  }

  @Override
  protected Map<String, String> properties() {
    return catalogProperties;
  }

  /** Tables live in a namespace; the empty namespace is not supported. */
  @Override
  protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
    return !tableIdentifier.namespace().isEmpty();
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    if (!namespaceExists(namespace) && !namespace.isEmpty()) {
      throw new NoSuchNamespaceException(
          "Cannot list tables for namespace. Namespace does not exist: %s", namespace);
    }
    return getCatalogFile().tables().stream()
        .filter(t -> t.namespace().equals(namespace))
        .sorted(Comparator.comparing(TableIdentifier::toString))
        .collect(Collectors.toList());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    final InputFile catalog = fileIO.newInputFile(catalogLocation);
    // A drop does not depend on the table's state, but its record names the version it read, so
    // one that conflicts with a concurrent commit to the table is retried on a fresh read. false
    // means only that the table did not exist.
    for (int attempt = 1; ; attempt++) {
      final CatalogFile catalogFile = format.read(fileIO, catalog);
      if (!catalogFile.containsTable(identifier)) {
        return false;
      }
      // The metadata a purge deletes the files of: the version this drop removes.
      TableMetadata dropped = purge ? newTableOps(identifier, catalogFile).current() : null;
      try {
        format.from(catalogFile).dropTable(identifier).commit(fileIO);
      } catch (NoSuchTableException e) {
        return false;
      } catch (CommitFailedException e) {
        if (attempt >= org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT + 1) {
          throw e;
        }
        continue;
      }
      if (dropped != null) {
        // The drop has landed: a file the purge cannot read or delete is left behind, not
        // reported as a failed drop.
        try {
          CatalogUtil.dropTableData(skippingInlinePaths(fileIO), dropped);
        } catch (RuntimeException e) {
          LOG.warn("Dropped table {}, but could not delete all of its files", identifier, e);
        }
      }
      return true;
    }
  }

  /**
   * {@code io}, ignoring deletes of the inline:// pseudo-locations an inline table's metadata
   * names.
   */
  private static FileIO skippingInlinePaths(FileIO io) {
    return new FileIO() {
      @Override
      public InputFile newInputFile(String path) {
        return io.newInputFile(path);
      }

      @Override
      public OutputFile newOutputFile(String path) {
        return io.newOutputFile(path);
      }

      @Override
      public void deleteFile(String path) {
        if (!path.startsWith("inline://")) {
          io.deleteFile(path);
        }
      }
    };
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    commitOnAFreshRead(catalogFile -> format.from(catalogFile).renameTable(from, to));
  }

  /**
   * Commits what {@code stage} stages on a fresh read of the catalog, and on another if it
   * conflicts with a concurrent change. A catalog operation does not depend on what its caller
   * read, but its record names the versions it read: of its namespace, or of the parent of one it
   * creates or drops, which any change there bumps (a table created in it, a sibling created, its
   * properties set), and of a table it renames. {@code stage} checks existence on each read.
   * Throws the conflict after Iceberg's default number of attempts.
   */
  private void commitOnAFreshRead(Function<CatalogFile, CatalogFile.Mut<?, ?>> stage) {
    final InputFile catalog = fileIO.newInputFile(catalogLocation);
    for (int attempt = 1; ; attempt++) {
      try {
        stage.apply(format.read(fileIO, catalog)).commit(fileIO);
        return;
      } catch (CommitFailedException e) {
        if (attempt >= org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT + 1) {
          throw e;
        }
      }
    }
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return newTableOps(tableIdentifier, null);
  }

  FileIOTableOperations newTableOps(TableIdentifier tableIdentifier, CatalogFile catalogFile) {
    InlineSettings inline = inlineSettings();
    if (inline.manifests) {
      return new InlineManifestTableOperations(
          tableIdentifier, catalogLocation, format, fileIO, inline, catalogFile);
    }
    return new FileIOTableOperations(
        tableIdentifier, catalogLocation, format, fileIO, inline, catalogFile);
  }

  /** How this catalog stores table metadata, from its {@code fileio.catalog.inline*} properties. */
  private InlineSettings inlineSettings() {
    return new InlineSettings(
        Boolean.parseBoolean(catalogProperties.getOrDefault(INLINE_ENABLED, "false")),
        Boolean.parseBoolean(catalogProperties.getOrDefault(INLINE_MANIFESTS, "false")),
        Boolean.parseBoolean(catalogProperties.getOrDefault(INLINE_STRICT, "false")),
        configuredInlineCodec(),
        Integer.parseInt(
            catalogProperties.getOrDefault(
                INLINE_MAX_UPDATE_SIZE, String.valueOf(InlineDeltaCodec.APPEND_LIMIT))));
  }

  /** How a catalog stores table metadata: in the catalog file (inline) or in metadata files. */
  static final class InlineSettings {
    /** Inline TableMetadata: table updates go to the catalog file, as long as they fit. */
    final boolean metadata;
    /** Inline manifest lists too: snapshots stage their lists instead of writing Avro files. */
    final boolean manifests;
    /** Reject loading a table with both inline and file manifest lists, instead of warning. */
    final boolean strict;
    /** Codec for full inline TableMetadata. */
    final InlineMetadataCodec codec;
    /** The largest inline table update, in bytes; a larger one writes a metadata file. */
    final int maxUpdateSize;

    InlineSettings(
        boolean metadata,
        boolean manifests,
        boolean strict,
        InlineMetadataCodec codec,
        int maxUpdateSize) {
      Preconditions.checkArgument(
          !manifests || metadata,
          "fileio.catalog.inline.manifests=true requires fileio.catalog.inline=true");
      Preconditions.checkArgument(
          maxUpdateSize > 0, "Invalid %s: %s", INLINE_MAX_UPDATE_SIZE, maxUpdateSize);
      this.metadata = metadata;
      this.manifests = manifests;
      this.strict = strict;
      this.codec = codec;
      this.maxUpdateSize = maxUpdateSize;
    }
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

  /**
   * The location {@code table}'s metadata loads from in {@code catalogFile}, or null if it has no
   * table of that name. It names the table's version: a pointer table gets a new metadata file on
   * every commit, and an inline table's location is {@code inline://<table>#<id>.<version>}. The
   * table id tells apart tables that held the name in turn, whose versions each count from 1:
   * BaseMetastoreTableOperations reloads only when the location changes, so a handle to a table
   * that was dropped or renamed away must not see the same location for its successor.
   */
  static String metadataLocation(CatalogFile catalogFile, TableIdentifier table) {
    String location = catalogFile.location(table);
    if (location != null || !catalogFile.isInlineTable(table)) {
      return location;
    }
    String version = "-1";
    if (catalogFile instanceof ProtoCatalogFile) {
      ProtoCatalogFile proto = (ProtoCatalogFile) catalogFile;
      Integer numId = proto.tableId(table);
      if (numId != null) {
        version = numId + "." + proto.tableVersion(numId);
      }
    }
    return "inline://" + table + "#" + version;
  }

  private CatalogFile getCatalogFile() {
    final InputFile catalog = fileIO.newInputFile(catalogLocation);
    return format.read(fileIO, catalog);
  }

  /**
   * Resolves the codec used for new full inline-TM writes from the catalog
   * properties (defaults to {@link #DEFAULT_INLINE_TM_CODEC}). Decode is
   * always driven by the per-record codec discriminator on the wire — this
   * setting only affects encode paths.
   */
  private InlineMetadataCodec configuredInlineCodec() {
    String shortName = catalogProperties.getOrDefault(INLINE_TM_CODEC, DEFAULT_INLINE_TM_CODEC);
    return InlineMetadataCodecs.byShortName(shortName);
  }

  /**
   * Returns the codec tag recorded for the given inline table. Callers must
   * have already verified the table is inline via
   * {@link CatalogFile#isInlineTable(TableIdentifier)}.
   */
  private static byte inlineCodecTag(CatalogFile catalogFile, TableIdentifier table) {
    ProtoCatalogFile proto = (ProtoCatalogFile) catalogFile;
    return proto.inlineMetadataCodec(proto.tableId(table));
  }

  //
  // SupportsNamespaces
  //

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    commitOnAFreshRead(
        catalogFile -> format.from(catalogFile).createNamespace(namespace, metadata));
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
    try {
      commitOnAFreshRead(catalogFile -> format.from(catalogFile).dropNamespace(namespace));
    } catch (NoSuchNamespaceException e) {
      return false;
    }
    return true;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    commitOnAFreshRead(
        catalogFile -> format.from(catalogFile).updateProperties(namespace, properties));
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
    private final InlineSettings inline;
    private volatile CatalogFile lastCatalogFile = null;

    FileIOTableOperations(
        TableIdentifier tableId,
        String catalogLocation,
        CatalogFormat format,
        SupportsAtomicOperations fileIO,
        InlineSettings inline) {
      this(tableId, catalogLocation, format, fileIO, inline, null);
    }

    FileIOTableOperations(
        TableIdentifier tableId,
        String catalogLocation,
        CatalogFormat format,
        SupportsAtomicOperations fileIO,
        InlineSettings inline,
        CatalogFile catalogFile) {
      this.fileIO = fileIO;
      this.format = format;
      this.tableId = tableId;
      this.catalogLocation = catalogLocation;
      this.inline = inline;
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
        // any commit, and distinct for another table that later holds the
        // name. The catalog already tracks all three — TblEntry.version bumps
        // on every commit, and each table has its own id — so we derive the
        // location from (name, id, version) directly; see metadataLocation.
        // No content hashing, no collision worry.
        byte[] inlineMeta = catalogFile.inlineMetadata(tableId);
        byte codecTag = inlineCodecTag(catalogFile, tableId);
        InlineMetadataCodec codec = InlineMetadataCodecs.byTag(codecTag);
        String syntheticLoc = metadataLocation(catalogFile, tableId);
        refreshFromMetadataLocation(syntheticLoc, null, 0,
            loc -> wrapInlineManifests(codec.decodeFull(inlineMeta, loc), catalogFile));
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

    /**
     * Writes {@code metadata} to a new metadata file and returns its location. A metadata file can
     * name only manifest list files, so first each inline-ML snapshot's list, which the catalog
     * held inline or a producer staged, is written to one: the table is leaving inline storage, or
     * a writer that does not inline is committing to it.
     */
    String writeUpdateMetadata(boolean isCreate, TableMetadata metadata) {
      return writeNewMetadataIfRequired(isCreate, withManifestListFiles(metadata));
    }

    /**
     * Where a file of {@code metadata}'s metadata directory goes, as {@link
     * BaseMetastoreTableOperations} places metadata files. Taken from the new metadata, not {@link
     * #current()}, which a create does not have.
     */
    private static String metadataDirectoryFile(TableMetadata metadata, String filename) {
      String dir =
          metadata.properties().get(org.apache.iceberg.TableProperties.WRITE_METADATA_LOCATION);
      return dir != null
          ? LocationUtil.stripTrailingSlash(dir) + "/" + filename
          : metadata.location() + "/metadata/" + filename;
    }

    private TableMetadata withManifestListFiles(TableMetadata metadata) {
      List<Snapshot> inlineSnapshots = Lists.newArrayList();
      for (Snapshot s : metadata.snapshots()) {
        if (s instanceof InlineSnapshot) {
          inlineSnapshots.add(s);
        } else if (s.manifestListLocation() != null
            && s.manifestListLocation().startsWith("inline://")) {
          throw new IllegalStateException(
              "Snapshot " + s.snapshotId() + " of " + tableId + " has no manifest list: "
                  + s.manifestListLocation());
        }
      }
      if (inlineSnapshots.isEmpty()) {
        return metadata;
      }
      Map<Long, Snapshot> replacements = new java.util.concurrent.ConcurrentHashMap<>();
      org.apache.iceberg.util.Tasks.foreach(inlineSnapshots)
          .stopOnFailure()
          .throwFailureWhenFinished()
          .executeWith(org.apache.iceberg.util.ThreadPools.getWorkerPool())
          .run(
              s -> {
                String name =
                    String.format("snap-%d-%s.avro", s.snapshotId(), java.util.UUID.randomUUID());
                String file = metadataDirectoryFile(metadata, name);
                replacements.put(
                    s.snapshotId(),
                    org.apache.iceberg.ManifestListFiles.write(
                        metadata.formatVersion(), s, s.allManifests(io()),
                        io().newOutputFile(file)));
              });
      TableMetadata.Builder builder = TableMetadata.buildFrom(metadata);
      builder.replaceSnapshots(replacements);
      builder.withMetadataLocation(metadata.metadataFileLocation());
      return builder.discardChanges().build();
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
      try {
        if (base == null) {
          commitCreate(metadata);
          return;
        }
        CatalogFile.Mut<?, ?> mut = format.from(lastCatalogFile);
        stageUpdate(mut, base, metadata);
        mut.commit(io());
      } catch (SupportsAtomicOperations.CASException e) {
        throw new CommitFailedException(e, "Failed to commit metadata for table %s", tableId);
      }
    }

    /**
     * Commits the creation of this table with {@code metadata}. Its record names the version of
     * the namespace it read, which any change there bumps (another table created or dropped in
     * it, its properties set), and Iceberg takes a create's conflict to mean the table exists. So
     * one that conflicts is staged again on a fresh read, which fails if the table exists by then.
     * Throws the conflict after Iceberg's default number of attempts.
     */
    private void commitCreate(TableMetadata metadata) {
      Consumer<CatalogFile.Mut<?, ?>> create = stageCreate(metadata);
      CatalogFile catalogFile = lastCatalogFile;
      for (int attempt = 1; ; attempt++) {
        CatalogFile.Mut<?, ?> mut = format.from(catalogFile);
        create.accept(mut);
        try {
          mut.commit(io());
          return;
        } catch (CommitFailedException e) {
          if (attempt >= org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT + 1) {
            throw e;
          }
          catalogFile = format.read(fileIO, fileIO.newInputFile(catalogLocation));
        }
      }
    }

    /**
     * Stages the creation of this table with {@code metadata}: inline, or in a metadata file that
     * it writes now, so the step stages the same create on any catalog file.
     */
    private Consumer<CatalogFile.Mut<?, ?>> stageCreate(TableMetadata metadata) {
      if (inline.metadata) {
        // Errata D9: an inline-ML createTransaction with one or more
        // newFastAppend calls puts InlineSnapshots into `metadata`. Two things
        // must happen for those snapshots to round-trip on reload:
        //   1. The encoded TableMetadata must carry a sentinel manifest-list
        //      location (inline://<snapId>) — not v1-embedded `manifests`
        //      paths — so wrapInlineManifests can swap in the pool's
        //      InlineSnapshot (rewriteInlineSnapshotsForFullEncode).
        //   2. The catalog's manifest pool and per-snapshot ref list must be
        //      populated atomically with the create. We achieve this by
        //      bundling AddManifestUpdate entries (built from the lists the
        //      InlineSnapshots hold) into a delta payload that
        //      CreateTableInlineAction.apply feeds through
        //      InlineDeltaCodec.applyManifestsOnly — the same hook a full-mode
        //      update uses.
        List<InlineDeltaCodec.DeltaUpdate> manifests =
            manifestUpdates(null, null, metadata, "");
        byte[] manifestBytes =
            manifests == null
                ? null
                : InlineDeltaCodec.encodeDelta(manifests, metadata.lastUpdatedMillis());
        byte[] metadataBytes = inline.codec.encode(rewriteInlineSnapshotsForFullEncode(metadata));
        if (metadataBytes.length + (manifestBytes != null ? manifestBytes.length : 0)
                <= inline.maxUpdateSize
            && !restatesAManifest(null, null, metadata)) {
          if (manifestBytes != null) {
            return mut -> mut.createTableInlineWithManifests(
                tableId, metadataBytes, inline.codec.tag(), manifestBytes);
          }
          return mut -> mut.createTableInline(tableId, metadataBytes, inline.codec.tag());
        }
      }
      String location = writeUpdateMetadata(true, metadata);
      return mut -> mut.createTable(tableId, location);
    }

    /**
     * Stages on {@code mut} the commit of {@code metadata}, built on {@code base}, to this table as
     * {@link #lastCatalogFile} holds it. In an inline catalog the update takes the first form that
     * fits in
     * {@code fileio.catalog.inline.max.update.size}: a delta, if the table is inline (the common
     * case); the full metadata, inline, which re-inlines a pointer table; else a metadata file,
     * which evicts the table from inline storage.
     */
    void stageUpdate(CatalogFile.Mut<?, ?> mut, TableMetadata base, TableMetadata metadata) {
      if (inline.metadata) {
        ProtoCatalogFile proto = (ProtoCatalogFile) lastCatalogFile;
        Integer tblId = proto.tableId(tableId);
        boolean isInline = tblId != null && proto.isInlineTable(tblId);
        String prefix = isInline && proto.manifestListPrefix(tblId) != null
            ? proto.manifestListPrefix(tblId)
            : "";
        List<InlineDeltaCodec.DeltaUpdate> manifests =
            manifestUpdates(isInline ? proto : null, base, metadata, prefix);
        if (manifests != null && restatesAManifest(isInline ? proto : null, base, metadata)) {
          mut.updateTable(tableId, writeUpdateMetadata(false, metadata));
          return;
        }
        if (isInline) {
          // Null if delta encoding cannot express the change (errata D1, D6): full mode then.
          List<InlineDeltaCodec.DeltaUpdate> delta =
              InlineDeltaCodec.computeDelta(base, metadata, prefix);
          if (delta != null) {
            if (manifests != null) {
              delta.addAll(manifests);
            }
            byte[] deltaBytes = InlineDeltaCodec.encodeDelta(delta, metadata.lastUpdatedMillis());
            if (deltaBytes.length <= inline.maxUpdateSize) {
              mut.updateTableInlineDelta(tableId, deltaBytes);
              return;
            }
          }
        }
        // Full mode keeps the manifest pool (I3) and lands the new snapshots' lists with the
        // metadata: the replay registers each new inline snapshot on its parent's list, even one
        // with no updates of its own.
        byte[] fullBytes = inline.codec.encode(rewriteInlineSnapshotsForFullEncode(metadata));
        byte[] manifestBytes =
            manifests == null
                ? null
                : InlineDeltaCodec.encodeDelta(manifests, metadata.lastUpdatedMillis());
        if (fullBytes.length + (manifestBytes != null ? manifestBytes.length : 0)
            <= inline.maxUpdateSize) {
          if (manifestBytes != null) {
            mut.updateTableInlineWithManifests(
                tableId, fullBytes, inline.codec.tag(), manifestBytes);
          } else {
            mut.updateTableInline(tableId, fullBytes, inline.codec.tag());
          }
          return;
        }
      }
      mut.updateTable(tableId, writeUpdateMetadata(false, metadata));
    }

    /**
     * The manifest list updates for the inline-ML snapshots {@code metadata} adds, those not in
     * {@code base}. Each is an {@link InlineSnapshot} that its producer returned, holding its whole
     * list; a committed snapshot's list never changes, so one in {@code base} sends nothing. The
     * catalog builds a new snapshot's list on its parent's inline list ({@code
     * InlineDeltaCodec.NewSnapshotManifests}), so a snapshot sends the manifests it adds to and
     * removes from its parent's list: one new in this commit, or one inline in {@code proto} (the
     * table's state the update applies to, null if the table is not inline). A snapshot whose
     * parent has no inline list sends its whole list: its parent's list is a file, written by a
     * writer that does not inline manifest lists or by an eviction.
     *
     * <p>Returns null if the commit adds no inline snapshot; else the updates, which are none if
     * the new snapshots change no manifests (a delete that matched nothing) but must still be
     * sent, so that the catalog registers their lists.
     */
    private List<InlineDeltaCodec.DeltaUpdate> manifestUpdates(
        ProtoCatalogFile proto, TableMetadata base, TableMetadata metadata, String prefix) {
      List<InlineDeltaCodec.DeltaUpdate> updates = null;
      for (Snapshot snapshot : metadata.snapshots()) {
        if (!isNew(snapshot, base)) {
          continue;
        }
        if (updates == null) {
          updates = new java.util.ArrayList<>();
        }
        List<ManifestFile> manifests = snapshot.allManifests(io());
        List<ManifestFile> parent = parentList(proto, base, metadata, snapshot.parentId());
        Set<String> paths = manifests.stream().map(ManifestFile::path).collect(Collectors.toSet());
        // The list is rebuilt in order: the parent's, less what the snapshot removes, with what it
        // adds at the positions it has. If the manifests it keeps are not in the parent's order,
        // it replaces the parent's list whole.
        List<String> kept =
            parent == null
                ? List.of()
                : parent.stream()
                    .map(ManifestFile::path)
                    .filter(paths::contains)
                    .collect(Collectors.toList());
        Set<String> keptPaths = new HashSet<>(kept);
        boolean inOrder =
            kept.equals(
                manifests.stream()
                    .map(ManifestFile::path)
                    .filter(keptPaths::contains)
                    .collect(Collectors.toList()));
        List<ManifestFile> added = new java.util.ArrayList<>();
        List<Integer> positions = new java.util.ArrayList<>();
        for (int i = 0; i < manifests.size(); i++) {
          if (!inOrder || !keptPaths.contains(manifests.get(i).path())) {
            added.add(manifests.get(i));
            positions.add(i);
          }
        }
        List<String> removed =
            parent == null
                ? List.of()
                : parent.stream()
                    .map(ManifestFile::path)
                    .filter(path -> !inOrder || !paths.contains(path))
                    .collect(Collectors.toList());
        InlineDeltaCodec.attachManifestDelta(
            updates, snapshot.snapshotId(), added, positions, removed, prefix);
      }
      return updates;
    }

    /**
     * Whether a snapshot {@code metadata} adds lists a manifest with another first row id than
     * the table's inline lists, or an earlier new snapshot, hold for its path: the first snapshot
     * of a table upgraded to v3, which gives row ids to the data manifests it keeps. The inline
     * lists share one entry per path, and changing it would change the lists that hold it (design.md
     * I3a), so the commit writes a metadata file, which holds each snapshot's list as it is.
     */
    private boolean restatesAManifest(
        ProtoCatalogFile proto, TableMetadata base, TableMetadata metadata) {
      Integer tblId = proto != null ? proto.tableId(tableId) : null;
      Map<String, Long> firstRowIds = new HashMap<>();
      if (tblId != null) {
        proto.manifestPool(tblId).forEach((path, m) -> firstRowIds.put(path, m.firstRowId()));
      }
      for (Snapshot snapshot : metadata.snapshots()) {
        if (!isNew(snapshot, base)) {
          continue;
        }
        for (ManifestFile m : snapshot.allManifests(io())) {
          if (firstRowIds.containsKey(m.path())
              && !java.util.Objects.equals(firstRowIds.get(m.path()), m.firstRowId())) {
            return true;
          }
          firstRowIds.put(m.path(), m.firstRowId());
        }
      }
      return false;
    }

    /** Whether {@code snapshot} is an inline-ML snapshot that a commit on {@code base} adds. */
    private static boolean isNew(Snapshot snapshot, TableMetadata base) {
      return snapshot instanceof InlineSnapshot
          && (base == null || base.snapshot(snapshot.snapshotId()) == null);
    }

    /**
     * The inline list a new snapshot's list is built on: none for a snapshot without a parent, a
     * parent's new in this commit, or a parent's inline in {@code proto}. Null if the parent's
     * list is not inline.
     */
    private List<ManifestFile> parentList(
        ProtoCatalogFile proto, TableMetadata base, TableMetadata metadata, Long parentId) {
      if (parentId == null) {
        return List.of();
      }
      Snapshot parent = metadata.snapshot(parentId);
      if (parent != null && isNew(parent, base)) {
        return parent.allManifests(io());
      }
      Integer tblId = proto != null ? proto.tableId(tableId) : null;
      return tblId != null && proto.hasInlineManifests(tblId, parentId)
          ? proto.inlineManifests(tblId, parentId)
          : null;
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
        if (inline.strict) {
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
          // GenericManifestFile copies: Iceberg compares manifests by equality, which for
          // GenericManifestFile is by path. Expiry's ReachableFileCleanup finds the manifests that
          // retained snapshots still use that way; the catalog's own entries, equal only to
          // themselves, were never found in use, and their files were deleted.
          List<ManifestFile> manifests = new java.util.ArrayList<>();
          for (ManifestFile m : proto.inlineManifests(tblId, s.snapshotId())) {
            manifests.add(org.apache.iceberg.GenericManifestFile.copyOf(m).build());
          }
          replacements.put(s.snapshotId(), new InlineSnapshot(
              s.sequenceNumber(), s.snapshotId(), s.parentId(),
              s.timestampMillis(), s.operation(), s.summary(),
              s.schemaId(), s.firstRowId(), s.addedRows(), s.keyId(),
              manifests));
        }
      }

      // Replace snapshots in-place without using removeSnapshots (which clears refs
      // and doesn't reset lastSequenceNumber, causing validation failures on re-add).
      // withMetadataLocation preserves parsed.metadataFileLocation() across the
      // rebuild — without it, BaseCatalogTransaction.validateSerializableIsolation
      // NPEs on .equals() against a null metadataFileLocation.
      TableMetadata.Builder builder = TableMetadata.buildFrom(parsed);
      builder.replaceSnapshots(replacements);
      builder.withMetadataLocation(parsed.metadataFileLocation());
      return builder.discardChanges().build();
    }
  }

  /**
   * Table operations of a catalog that inlines manifest lists (fileio.catalog.inline.manifests).
   * As a {@link ManifestListSink}, they make {@link org.apache.iceberg.SnapshotProducer} write no
   * manifest list file but return an {@link InlineSnapshot} that holds the list, from which the
   * commit takes it ({@link FileIOTableOperations#manifestUpdates}). The delta the producer
   * offers is not kept: a commit would have to find it through the handle that received it, and
   * a catalog transaction's commit has only the snapshots.
   */
  static class InlineManifestTableOperations extends FileIOTableOperations
      implements ManifestListSink {

    InlineManifestTableOperations(
        TableIdentifier tableId, String catalogLocation,
        CatalogFormat format, SupportsAtomicOperations fileIO, InlineSettings inline) {
      super(tableId, catalogLocation, format, fileIO, inline);
    }

    InlineManifestTableOperations(
        TableIdentifier tableId, String catalogLocation,
        CatalogFormat format, SupportsAtomicOperations fileIO, InlineSettings inline,
        CatalogFile catalogFile) {
      super(tableId, catalogLocation, format, fileIO, inline, catalogFile);
    }

    @Override
    public void stageManifestListDelta(
        long sequenceNumber, long snapshotId, Long parentSnapshotId,
        Long nextRowId, ManifestListDelta delta, Long nextRowIdAfter) {}
  }

  /**
   * Rewrites any {@link InlineSnapshot} (i.e. snapshots whose
   * {@link Snapshot#manifestListLocation()} is {@code null}) to a
   * {@link org.apache.iceberg.BaseSnapshot} carrying the
   * {@code inline://<snapshotId>} sentinel, then returns a rebuilt
   * {@link TableMetadata}. Called immediately before
   * {@code InlineMetadataCodec.encode} on the full-mode commit paths
   * ({@code stageCreate} and {@code stageUpdate}) so the on-disk bytes are
   * codec-agnostic.
   *
   * <p>Without this rewrite the gzip codec's {@code SnapshotParser.toJson}
   * falls into the v1-embedded-manifests branch (no {@code manifest-list}
   * field, an explicit {@code "manifests": [...]} array), and any reader
   * that decodes those bytes without the catalog's {@code wrapInlineManifests}
   * rewrap reconstructs {@code GenericManifestFile(specId=0, ...)} — multi-spec
   * tables silently lose {@code partitionSpecId}. The structural codec
   * already writes the sentinel via {@code planPaths}; this aligns the gzip
   * encoding with the structural codec and matches the create-path rewrite
   * (errata D9).
   */
  private static TableMetadata rewriteInlineSnapshotsForFullEncode(TableMetadata metadata) {
    Map<Long, Snapshot> sentinelReplacements = null;
    for (Snapshot s : metadata.snapshots()) {
      if (s.manifestListLocation() != null) {
        continue;
      }
      if (sentinelReplacements == null) {
        sentinelReplacements = new HashMap<>();
      }
      Map<String, String> summary = s.summary() != null
          ? new HashMap<>(s.summary())
          : new HashMap<>();
      if (s.operation() != null && !summary.containsKey("operation")) {
        summary.put("operation", s.operation());
      }
      Snapshot replacement = org.apache.iceberg.InlineDeltaSnapshots.create(
          s.sequenceNumber(), s.snapshotId(), s.parentId(),
          s.timestampMillis(), summary, s.schemaId(),
          "inline://" + s.snapshotId(),
          s.firstRowId(), s.addedRows(), s.keyId());
      sentinelReplacements.put(s.snapshotId(), replacement);
    }
    if (sentinelReplacements == null) {
      return metadata;
    }
    TableMetadata.Builder b = TableMetadata.buildFrom(metadata);
    b.replaceSnapshots(sentinelReplacements);
    b.withMetadataLocation(metadata.metadataFileLocation());
    return b.discardChanges().build();
  }

  // SupportsCatalogTransaction

  @Override
  public void commitTransaction(
      Map<BaseCatalogTransaction.TableRef, TableMetadata> reads, List<TableCommit> commits) {
    // TableCommit validations check the table UUID and snapshot ref for each table
    // if all validations pass for the current CatalogFile, then attempt atomic replace
    final CatalogFile current = getCatalogFile();
    final CatalogFile.Mut<?, ?> newCatalog = format.from(current);
    // Each branch the transaction read must be as it read it, from then until the commit
    // lands. BaseCatalogTransaction checked that before calling, but a commit may have landed
    // since; check again on the state this commit builds on, and readTable makes the commit
    // fail if the table changes before it lands.
    for (Map.Entry<BaseCatalogTransaction.TableRef, TableMetadata> read : reads.entrySet()) {
      TableIdentifier readTable = read.getKey().identifier();
      String ref = read.getKey().ref();
      TableMetadata now = newTableOps(readTable, current).current();
      if (!BaseCatalogTransaction.unchangedSinceRead(ref, read.getValue(), now)) {
        throw new ValidationException(
            "SERIALIZABLE isolation violation: Found table metadata updates to table '%s' after it was read on branch '%s'",
            readTable, ref);
      }
      newCatalog.readTable(readTable);
    }
    for (TableCommit commit : commits) {
      final TableIdentifier tableId = commit.identifier();
      // use fixed catalog snapshot for validation
      FileIOTableOperations ops = newTableOps(tableId, current);
      final TableMetadata currentMetadata = ops.current();
      if (currentMetadata == null) {
        throw new NoSuchTableException("Table does not exist: %s", tableId);
      }
      commit.requirements().forEach(req -> req.validate(currentMetadata));

      TableMetadata.Builder newMetadataBuilder = TableMetadata.buildFrom(currentMetadata);
      commit.updates().forEach(update -> update.applyTo(newMetadataBuilder));
      final TableMetadata newMetadata = newMetadataBuilder.build();
      if (newMetadata.changes().isEmpty()) {
        continue;
      }
      // The table's new inline-ML snapshots hold their manifest lists.
      ops.stageUpdate(newCatalog, currentMetadata, newMetadata);
    }
    try {
      newCatalog.commit(fileIO);
    } catch (SupportsAtomicOperations.CASException e) {
      throw new CommitFailedException(e, "Failed to commit metadata for multi-table commit");
    }
  }

  @Override
  public CatalogTransaction createTransaction(CatalogTransaction.IsolationLevel isolationLevel) {
    return new BaseCatalogTransaction(this, isolationLevel);
  }
}
