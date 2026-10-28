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
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.InlineSnapshot;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.intentlog.Replica;
import org.intentlog.SharedLog;

/**
 * An Iceberg catalog whose state is replicated through a shared log ({@link SharedLog}): its
 * namespaces, their properties, and its tables, each with a pointer to a metadata file or (with
 * {@code fileio.catalog.inline}) its metadata inline. A read replays the log ({@link CatalogLog});
 * a change appends a transaction's intention to it, which takes effect where it lands if what it
 * read is unchanged there. Multi-table transactions commit the same way. Table files go to a
 * {@link FileIO} of their own.
 *
 * <p>A catalog is given its log and FileIO, or a subclass opens them from the catalog's
 * properties ({@link #openLog}): {@link FileIOCatalog} keeps the log in one object of a store
 * with conditional writes.
 */
public class LogCatalog extends BaseMetastoreCatalog
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
      org.slf4j.LoggerFactory.getLogger(LogCatalog.class);

  private String catalogName;
  private String warehouseLocation;
  private SharedLog sharedLog;
  private FileIO io;
  private CatalogLog log;
  private final Map<String, String> catalogProperties;
  // Whether its table commits pin what Iceberg commits once (FileIOTableOperations.doCommit)
  private boolean pinsCommitOnce = true;
  // The InlineSnapshots this catalog's loads of inline tables made, for later loads to reuse
  private final InlineSnapshots inlineSnapshots = new InlineSnapshots();

  /** A catalog whose subclass opens its log and FileIO when it is initialized ({@link #openLog}). */
  protected LogCatalog(String catalogName, Map<String, String> catalogProperties) {
    this.catalogName = catalogName;
    this.catalogProperties = catalogProperties;
  }

  /**
   * A catalog replicated through {@code log}, with its tables' files in {@code io}. {@link
   * #initialize} completes it, and creates the log if it does not exist.
   */
  public LogCatalog(
      String catalogName, SharedLog log, FileIO io, Map<String, String> catalogProperties) {
    this(catalogName, catalogProperties);
    open(log, io);
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
    String conflictEncoding = LogCatalogProperties.conflictEncoding(catalogProperties);

    warehouseLocation = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
    if (sharedLog == null) {
      openLog(properties);
    }
    Preconditions.checkState(
        sharedLog != null && io != null, "%s opened no log", getClass().getSimpleName());
    log = new CatalogLog(sharedLog, LogCatalogProperties.runtimeConfig(catalogProperties));
    // Another catalog may create the log at once (two processes starting on a new warehouse):
    // either way, this one uses it.
    log.bootstrap(conflictEncoding);
  }

  /**
   * Opens the catalog's log and the FileIO of its tables' files from its {@code properties}
   * ({@link #open}), for a catalog that was not given them.
   */
  protected void openLog(Map<String, String> properties) {
    throw new IllegalArgumentException(
        "Cannot initialize " + getClass().getSimpleName() + ": it has no log");
  }

  /** Uses {@code log} for the catalog's state, and {@code tableIO} for its tables' files. */
  protected final void open(SharedLog log, FileIO tableIO) {
    this.sharedLog = log;
    this.io = tableIO;
  }

  /** The catalog's log, as {@link #initialize} opened it. */
  CatalogLog log() {
    return log;
  }

  /** The FileIO of the catalog's tables' files. */
  protected FileIO io() {
    return io;
  }

  @Override
  public String name() {
    return catalogName;
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
    return state().tables().stream()
        .filter(t -> t.namespace().equals(namespace))
        .sorted(Comparator.comparing(TableIdentifier::toString))
        .collect(Collectors.toList());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    // A drop does not depend on the table's state, but its record names the version it read, so
    // one that conflicts with a concurrent commit to the table is retried on a fresh read. false
    // means only that the table did not exist.
    for (int attempt = 1; ; attempt++) {
      final Replica<CatalogState> read = log.read();
      if (!read.state().containsTable(identifier)) {
        return false;
      }
      // The metadata a purge deletes the files of: the version this drop removes.
      TableMetadata dropped = purge ? newTableOps(identifier, read).current() : null;
      try {
        new CatalogMutation(read).dropTable(identifier).commit(log);
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
          CatalogUtil.dropTableData(skippingInlinePaths(io), dropped);
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
    commitOnAFreshRead(read -> new CatalogMutation(read).renameTable(from, to));
  }

  /**
   * Commits what {@code stage} stages on a fresh read of the catalog, and on another if it
   * conflicts with a concurrent change. A catalog operation does not depend on what its caller
   * read, but its record names the versions it read: of its namespace, or of the parent of one it
   * creates or drops, which any change there bumps (a table created in it, a sibling created, its
   * properties set), and of a table it renames. {@code stage} checks existence on each read.
   * Throws the conflict after Iceberg's default number of attempts.
   */
  private void commitOnAFreshRead(Function<Replica<CatalogState>, CatalogMutation> stage) {
    for (int attempt = 1; ; attempt++) {
      try {
        stage.apply(log.read()).commit(log);
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

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new LogTableBuilder(identifier, schema);
  }

  /** Iceberg's table builder, whose replace transactions commit as replaces (Bug 81). */
  private class LogTableBuilder extends BaseMetastoreCatalogTableBuilder {
    LogTableBuilder(TableIdentifier identifier, Schema schema) {
      super(identifier, schema);
    }

    @Override
    public Transaction replaceTransaction() {
      return replacing(super.replaceTransaction());
    }

    @Override
    public Transaction createOrReplaceTransaction() {
      return replacing(super.createOrReplaceTransaction());
    }

    private Transaction replacing(Transaction transaction) {
      ((FileIOTableOperations) ((BaseTransaction) transaction).underlyingOps()).replaces = true;
      return transaction;
    }
  }

  /**
   * Operations on {@code tableIdentifier}: as {@code read} holds it, without refresh, or, if null,
   * as the next refresh finds it.
   */
  FileIOTableOperations newTableOps(TableIdentifier tableIdentifier, Replica<CatalogState> read) {
    InlineSettings inline = inlineSettings();
    FileIOTableOperations ops =
        inline.manifests
            ? new InlineManifestTableOperations(
                tableIdentifier, log, io, inline, inlineSnapshots, read)
            : new FileIOTableOperations(tableIdentifier, log, io, inline, inlineSnapshots, read);
    ops.pinsCommitOnce = pinsCommitOnce;
    return ops;
  }

  /**
   * Makes this catalog's table commits leave what Iceberg commits once unpinned, as a writer that
   * relies on replay to tell it from the delta ({@link TableChange#commitOnce}): for tests of that.
   * A catalog transaction still pins the tables it writes.
   */
  LogCatalog leavingCommitOnceUnpinned() {
    this.pinsCommitOnce = false;
    return this;
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
    boolean unique =
        PropertyUtil.propertyAsBoolean(
            catalogProperties,
            CatalogProperties.UNIQUE_TABLE_LOCATION,
            CatalogProperties.UNIQUE_TABLE_LOCATION_DEFAULT);
    return String.join(
        "/",
        defaultNamespaceLocation(tableIdentifier.namespace()),
        LocationUtil.tableLocation(tableIdentifier, unique));
  }

  private String defaultNamespaceLocation(Namespace namespace) {
    if (namespace.isEmpty()) {
      return warehouseLocation;
    }
    return warehouseLocation + "/" + String.join("/", namespace.levels());
  }

  /**
   * The location {@code table}'s metadata loads from in {@code state}, or null if it has no table
   * of that name. It names the table's version: a pointer table gets a new metadata file on
   * every commit, and an inline table's location is {@code inline://<table>#<id>.<version>}. The
   * table id tells apart tables that held the name in turn, whose versions each count from 1:
   * BaseMetastoreTableOperations reloads only when the location changes, so a handle to a table
   * that was dropped or renamed away must not see the same location for its successor.
   */
  static String metadataLocation(CatalogState state, TableIdentifier table) {
    String location = state.location(table);
    if (location != null || !state.isInlineTable(table)) {
      return location;
    }
    Integer numId = state.tableId(table);
    return "inline://" + table + "#" + numId + "." + state.tableVersion(numId);
  }

  /** The catalog's state, as a fresh read of its log finds it. */
  private CatalogState state() {
    return log.read().state();
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

  //
  // SupportsNamespaces
  //

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    commitOnAFreshRead(read -> new CatalogMutation(read).createNamespace(namespace, metadata));
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    CatalogState state = state();
    if (!namespace.isEmpty() && !state.namespaces().contains(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    return state.namespaces().stream()
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
    CatalogState state = state();
    if (state.namespaces().contains(namespace)) {
      return state.namespaceProperties(namespace);
    } else {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    try {
      commitOnAFreshRead(read -> new CatalogMutation(read).dropNamespace(namespace));
    } catch (NoSuchNamespaceException e) {
      return false;
    }
    return true;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    commitOnAFreshRead(read -> new CatalogMutation(read).updateProperties(namespace, properties));
    return true;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties)
      throws NoSuchNamespaceException {
    return setProperties(
        namespace,
        properties.stream().collect(Maps::newHashMap, (m, k) -> m.put(k, null), Map::putAll));
  }

  /**
   * Operations on a table of the catalog. Each holds the read of the catalog's log its table was
   * loaded from, which a commit to the table is staged on.
   */
  static class FileIOTableOperations extends BaseMetastoreTableOperations {
    private final TableIdentifier tableId;
    private final CatalogLog log;
    private final InlineMetadataFileIO tableIO;
    private final InlineSettings inline;
    private final InlineSnapshots inlineSnapshots;
    private volatile Replica<CatalogState> lastRead = null;
    // Whether a read found the table: once one has, a read that does not finds it dropped
    private boolean loaded = false;
    // Whether a commit of what Iceberg commits once pins it (LogCatalog.leavingCommitOnceUnpinned)
    private boolean pinsCommitOnce = true;
    // Whether these are a replace transaction's, whose commit restates the table (Bug 81)
    private boolean replaces = false;

    FileIOTableOperations(
        TableIdentifier tableId,
        CatalogLog log,
        FileIO io,
        InlineSettings inline,
        InlineSnapshots inlineSnapshots,
        Replica<CatalogState> read) {
      this.tableIO = InlineMetadataFileIO.of(io, this::inlineMetadataLength);
      this.log = log;
      this.tableId = tableId;
      this.inline = inline;
      this.inlineSnapshots = inlineSnapshots;
      this.lastRead = read;
      if (read != null) {
        loadFrom(read.state());
        disableRefresh();
      }
    }

    /** The catalog's FileIO, through which this table's current inline metadata has a length. */
    @Override
    public FileIO io() {
      return tableIO;
    }

    /** The stored length of this table's inline metadata, if {@code location} is its current. */
    private Long inlineMetadataLength(String location) {
      Replica<CatalogState> read = lastRead;
      if (read == null || !read.state().isInlineTable(tableId)
          || !location.equals(metadataLocation(read.state(), tableId))) {
        return null;
      }
      return (long) read.state().inlineMetadata(tableId).length;
    }

    /**
     * Loads the table's metadata as {@code state} holds it: a pointer table's, or an inline one's.
     * Throws NoSuchTableException if an earlier read found the table and {@code state} has none:
     * {@link BaseMetastoreTableOperations#refresh} drops the metadata when it throws, and the
     * next read must not take the table for one not created yet, which has no metadata to update
     * (Bug 78: an update read a null base) and whose commit creates it.
     */
    private synchronized void loadFrom(CatalogState state) {
      String metadataFile = state.location(tableId);
      if (metadataFile != null) {
        loaded = true;
        // Pointer table: load metadata from external file
        refreshFromMetadataLocation(metadataFile);
      } else if (state.isInlineTable(tableId)) {
        // Inline table: load metadata from catalog bytes.
        // The synthetic location is the cache key for
        // BaseMetastoreTableOperations.refreshFromMetadataLocation: it must be
        // identical across reads of the same logical state and distinct after
        // any commit, and distinct for another table that later holds the
        // name. The catalog already tracks all three — TblEntry.version bumps
        // on every commit, and each table has its own id — so we derive the
        // location from (name, id, version) directly; see metadataLocation.
        // No content hashing, no collision worry.
        loaded = true;
        InlineTable inlineTable = state.inlineTable(state.tableId(tableId));
        String syntheticLoc = metadataLocation(state, tableId);
        refreshFromMetadataLocation(syntheticLoc, null, 0,
            loc -> inlineTable.loaded(loc, parsed -> wrapInlineManifests(parsed, state)));
      } else if (loaded || currentMetadataLocation() != null) {
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
      final Replica<CatalogState> read = log.read();
      lastRead = read;
      loadFrom(read.state());
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
      // The commit landed: a file that cannot be deleted is logged, not thrown.
      CatalogUtil.deleteFiles(
          io(),
          removed.stream().map(TableMetadata.MetadataLogEntry::file).collect(Collectors.toSet()),
          "metadata");
    }

    @Override
    public void doCommit(TableMetadata base, TableMetadata metadata) {
      if (base == null) {
        commitCreate(metadata);
        return;
      }
      CatalogMutation mut = new CatalogMutation(lastRead);
      stageUpdate(mut, base, metadata);
      if (pinsCommitOnce && (replaces || commitsOnce(metadata.changes()))) {
        mut.pin(tableId);
      }
      mut.commit(log);
    }

    /**
     * Whether Iceberg commits {@code changes}, the updates of a commit (all of a table
     * transaction's), only on the version of the table they read: it fails them on any other, so no
     * conflict encoding may apply them there. A schema or partition-spec update does, and a ref
     * change through {@code manageSnapshots}: any but a snapshot producer's move of a branch to the
     * snapshot it just added (a ref created at, or moved to, an existing snapshot, removed, or given
     * new retention).
     */
    static boolean commitsOnce(List<org.apache.iceberg.MetadataUpdate> changes) {
      org.apache.iceberg.MetadataUpdate previous = null;
      for (org.apache.iceberg.MetadataUpdate change : changes) {
        if (change instanceof org.apache.iceberg.MetadataUpdate.AddSchema
            || change instanceof org.apache.iceberg.MetadataUpdate.SetCurrentSchema
            || change instanceof org.apache.iceberg.MetadataUpdate.AddPartitionSpec
            || change instanceof org.apache.iceberg.MetadataUpdate.SetDefaultPartitionSpec
            || change instanceof org.apache.iceberg.MetadataUpdate.RemoveSnapshotRef) {
          return true;
        }
        if (change instanceof org.apache.iceberg.MetadataUpdate.SetSnapshotRef) {
          long target = ((org.apache.iceberg.MetadataUpdate.SetSnapshotRef) change).snapshotId();
          boolean producers =
              previous instanceof org.apache.iceberg.MetadataUpdate.AddSnapshot
                  && ((org.apache.iceberg.MetadataUpdate.AddSnapshot) previous)
                          .snapshot()
                          .snapshotId()
                      == target;
          if (!producers) {
            return true;
          }
        }
        previous = change;
      }
      return false;
    }

    /**
     * Commits the creation of this table with {@code metadata}. Its record names the version of
     * the namespace it read, which any change there bumps (another table created or dropped in
     * it, its properties set), and Iceberg takes a create's conflict to mean the table exists. So
     * one that conflicts is staged again on a fresh read, which fails if the table exists by then.
     * Throws the conflict after Iceberg's default number of attempts.
     */
    private void commitCreate(TableMetadata metadata) {
      Consumer<CatalogMutation> create = stageCreate(metadata);
      Replica<CatalogState> read = lastRead;
      for (int attempt = 1; ; attempt++) {
        CatalogMutation mut = new CatalogMutation(read);
        create.accept(mut);
        try {
          mut.commit(log);
          return;
        } catch (CommitFailedException e) {
          if (attempt >= org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT + 1) {
            throw e;
          }
          read = log.read();
        }
      }
    }

    /**
     * Stages the creation of this table with {@code metadata}: inline, or in a metadata file that
     * it writes now, so the step stages the same create on any read of the catalog.
     */
    private Consumer<CatalogMutation> stageCreate(TableMetadata metadata) {
      if (inline.metadata) {
        // An inline-ML createTransaction with one or more
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
            manifestUpdates(null, null, metadata, "", true);
        byte[] manifestBytes =
            manifests == null
                ? null
                : InlineDeltaCodec.encodeDelta(manifests, metadata.lastUpdatedMillis());
        byte[] metadataBytes = inline.codec.encode(rewriteInlineSnapshotsForFullEncode(metadata));
        if (metadataBytes.length + (manifestBytes != null ? manifestBytes.length : 0)
                <= inline.maxUpdateSize
            && !restatesAManifest(null, null, metadata, true)) {
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
     * {@link #lastRead} holds it. In an inline catalog the update takes the first form that
     * fits in
     * {@code fileio.catalog.inline.max.update.size}: a delta, if the table is inline (the common
     * case); the full metadata, inline, which re-inlines a pointer table; else a metadata file,
     * which evicts the table from inline storage.
     */
    void stageUpdate(CatalogMutation mut, TableMetadata base, TableMetadata metadata) {
      if (inline.metadata) {
        CatalogState proto = lastRead.state();
        Integer tblId = proto.tableId(tableId);
        boolean isInline = tblId != null && proto.isInlineTable(tblId);
        String prefix = isInline && proto.manifestListPrefix(tblId) != null
            ? proto.manifestListPrefix(tblId)
            : "";
        List<InlineDeltaCodec.DeltaUpdate> manifests =
            manifestUpdates(isInline ? proto : null, base, metadata, prefix, false);
        if (manifests != null
            && restatesAManifest(isInline ? proto : null, base, metadata, false)) {
          mut.updateTable(tableId, writeUpdateMetadata(false, metadata));
          return;
        }
        if (isInline) {
          // Null if delta encoding cannot express the change (a statistics change, errata D9; a
          // concurrent replace's id collision, SPEC_TM.md): full mode then.
          List<InlineDeltaCodec.DeltaUpdate> delta =
              InlineDeltaCodec.computeDelta(base, metadata, prefix);
          if (delta != null) {
            if (replaces) {
              restateReplacement(delta, metadata);
            }
            if (manifests != null) {
              delta.addAll(manifests);
            }
            byte[] deltaBytes =
                InlineDeltaCodec.encodeDelta(delta, metadata.lastUpdatedMillis(), base);
            if (deltaBytes.length <= inline.maxUpdateSize) {
              mut.updateTableInlineDelta(tableId, deltaBytes);
              return;
            }
          }
        }
        // Full mode keeps the manifest pool (I3) and lands the new snapshots' lists with the
        // metadata: the replay registers each new inline snapshot on its parent's list, even one
        // with no updates of its own. It also sends the list of any inline snapshot the table
        // holds no list for, which the full metadata names by its sentinel (Bug 62).
        manifests = manifestUpdates(isInline ? proto : null, base, metadata, prefix, true);
        if (manifests != null
            && restatesAManifest(isInline ? proto : null, base, metadata, true)) {
          mut.updateTable(tableId, writeUpdateMetadata(false, metadata));
          return;
        }
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
     * Adds to a replace's {@code delta} the current schema, default partition spec and default
     * sort order of its replacement, {@code metadata}, where the delta does not set them. A replace
     * restates them, but a delta from the version it read carries them only where they differ
     * there: replacing a table that has no snapshot with its own schema gives the delta of a first
     * append to an empty table, which a merging encoding may land on a version where another writer
     * changed the schema, keeping the change (Bug 81). Replay commits a delta that sets any of them
     * only on the version it read (TableChange), as Iceberg's REST client restates them for a
     * server that applies a replace's updates to the table it holds.
     */
    private static void restateReplacement(
        List<InlineDeltaCodec.DeltaUpdate> delta, TableMetadata metadata) {
      boolean schema = false;
      boolean spec = false;
      boolean order = false;
      for (InlineDeltaCodec.DeltaUpdate update : delta) {
        schema |= update instanceof InlineDeltaCodec.SetCurrentSchemaUpdate;
        spec |= update instanceof InlineDeltaCodec.SetDefaultSpecUpdate;
        order |= update instanceof InlineDeltaCodec.SetDefaultSortOrderUpdate;
      }
      if (!schema) {
        delta.add(new InlineDeltaCodec.SetCurrentSchemaUpdate(metadata.currentSchemaId()));
      }
      if (!spec) {
        delta.add(new InlineDeltaCodec.SetDefaultSpecUpdate(metadata.defaultSpecId()));
      }
      if (!order) {
        delta.add(new InlineDeltaCodec.SetDefaultSortOrderUpdate(metadata.defaultSortOrderId()));
      }
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
     * <p>With {@code full}, for a commit of the full metadata, which replaces the table's, the
     * updates also hold the list of each inline snapshot in {@code base} that the table holds no
     * inline list for (see {@link #sendsList}).
     *
     * <p>Returns null if the commit adds no inline snapshot; else the updates, which are none if
     * the new snapshots change no manifests (a delete that matched nothing) but must still be
     * sent, so that the catalog registers their lists.
     */
    private List<InlineDeltaCodec.DeltaUpdate> manifestUpdates(
        CatalogState proto,
        TableMetadata base,
        TableMetadata metadata,
        String prefix,
        boolean full) {
      List<InlineDeltaCodec.DeltaUpdate> updates = null;
      for (Snapshot snapshot : metadata.snapshots()) {
        if (!sendsList(snapshot, proto, base, full)) {
          continue;
        }
        if (updates == null) {
          updates = new java.util.ArrayList<>();
        }
        List<ManifestFile> manifests = snapshot.allManifests(io());
        List<ManifestFile> parent = parentList(proto, base, metadata, snapshot.parentId(), full);
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
        CatalogState proto, TableMetadata base, TableMetadata metadata, boolean full) {
      Integer tblId = proto != null ? proto.tableId(tableId) : null;
      Map<String, Long> firstRowIds = new HashMap<>();
      if (tblId != null) {
        proto.manifestPool(tblId).forEach((path, m) -> firstRowIds.put(path, m.firstRowId()));
      }
      for (Snapshot snapshot : metadata.snapshots()) {
        if (!sendsList(snapshot, proto, base, full)) {
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

    /**
     * Whether a commit of {@code metadata} on {@code base} sends {@code snapshot}'s list: if it is
     * an inline-ML snapshot the commit adds, or, with {@code full}, one the table, as {@code proto}
     * holds it (null if the table is not inline), has no inline list for. Full metadata names
     * every inline snapshot by its sentinel, and a commit's handle can hold one whose list the
     * table no longer has: it loaded the table inline, and another writer has since evicted it,
     * which wrote the lists to files and dropped them (Bug 62: a replace transaction commits the
     * snapshots it loaded). A delta keeps each snapshot in {@code base} as the table has it.
     */
    private boolean sendsList(
        Snapshot snapshot, CatalogState proto, TableMetadata base, boolean full) {
      if (isNew(snapshot, base)) {
        return true;
      }
      if (!full || !(snapshot instanceof InlineSnapshot)) {
        return false;
      }
      Integer tblId = proto != null ? proto.tableId(tableId) : null;
      return tblId == null || !proto.hasInlineManifests(tblId, snapshot.snapshotId());
    }

    /** Whether {@code snapshot} is an inline-ML snapshot that a commit on {@code base} adds. */
    private static boolean isNew(Snapshot snapshot, TableMetadata base) {
      return snapshot instanceof InlineSnapshot
          && (base == null || base.snapshot(snapshot.snapshotId()) == null);
    }

    /**
     * The inline list a new snapshot's list is built on: none for a snapshot without a parent, a
     * parent's this commit sends (a new one, or with {@code full}, one whose list the table has
     * lost: Bug 68), or a parent's inline in {@code proto}. Null if the parent's list is not
     * inline.
     */
    private List<ManifestFile> parentList(
        CatalogState proto,
        TableMetadata base,
        TableMetadata metadata,
        Long parentId,
        boolean full) {
      if (parentId == null) {
        return List.of();
      }
      Snapshot parent = metadata.snapshot(parentId);
      if (parent != null && sendsList(parent, proto, base, full)) {
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
    private TableMetadata wrapInlineManifests(TableMetadata parsed, CatalogState proto) {
      Integer tblId = proto.tableId(tableId);
      if (tblId == null) {
        return parsed;
      }

      // Any snapshot with an inline:// sentinel manifest-list location MUST
      // have a pool entry — the sentinel is only written when the sink path is
      // active, and the pool and sentinel must be committed atomically. A
      // sentinel without a pool entry means the pool was lost (a format bug),
      // and loading the BaseSnapshot as-is would result in
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

      List<Snapshot> snapshots = inlineSnapshots.wrap(proto, tblId, parsed.snapshots());

      // The same metadata, at the same location (BaseCatalogTransaction.validateSerializableIsolation
      // compares it), with these snapshots in place of the others
      return TableFields.of(parsed).withSnapshots(snapshots).toMetadata(parsed.metadataFileLocation());
    }
  }

  /**
   * The {@link InlineSnapshot}s a catalog's loads of inline tables made, which later loads reuse.
   * A load makes every snapshot with an inline list an InlineSnapshot holding it, of
   * GenericManifestFile copies: Iceberg compares manifests by equality, which for
   * GenericManifestFile is by path. Expiry's ReachableFileCleanup finds the manifests that retained
   * snapshots still use that way; the catalog's own entries, equal only to themselves, were never
   * found in use, and their files were deleted. One copy per pool entry, which every list naming
   * it shares.
   *
   * <p>A committed snapshot's list never changes, and a state shares the lists that did not change
   * with the state before it (the same objects), so a load reuses the InlineSnapshot it made for a
   * snapshot whose list is still the same object, unless an entry of the table's pool was replaced
   * since ({@link CatalogState#poolReplacements}), when a path may resolve to another entry. It
   * makes only the snapshots that are new since the last load, where it used to make every
   * retained snapshot's list on each load (most of a writer's CPU under fast appends).
   */
  static final class InlineSnapshots {
    private final Map<Integer, TableSnapshots> tables =
        new java.util.concurrent.ConcurrentHashMap<>();

    /** What loads of one table made. */
    private static final class TableSnapshots {
      private long poolReplacements = -1L;
      private final Map<Long, Made> made = new java.util.HashMap<>();
      // pool entry (by identity) -> its copy
      private final Map<ManifestFile, ManifestFile> copies = new java.util.IdentityHashMap<>();
    }

    /** An InlineSnapshot, and the list of manifest paths it was made from. */
    private static final class Made {
      private final List<String> paths;
      private final InlineSnapshot snapshot;

      Made(List<String> paths, InlineSnapshot snapshot) {
        this.paths = paths;
        this.snapshot = snapshot;
      }
    }

    /**
     * {@code parsed}, a load of inline table {@code tblId} in {@code state}, with each snapshot that
     * has an inline list as an InlineSnapshot holding it.
     */
    List<Snapshot> wrap(CatalogState state, int tblId, List<Snapshot> parsed) {
      TableSnapshots table = tables.computeIfAbsent(tblId, id -> new TableSnapshots());
      Map<Long, List<String>> lists = state.snapshotManifests(tblId);
      Map<String, ManifestFile> pool = state.manifestPool(tblId);
      synchronized (table) {
        long replacements = state.poolReplacements(tblId);
        if (replacements != table.poolReplacements
            || table.copies.size() > 2 * pool.size() + 64) {
          table.made.clear();
          table.copies.clear();
          table.poolReplacements = replacements;
        }
        List<Snapshot> snapshots = new java.util.ArrayList<>(parsed.size());
        java.util.Set<Long> loaded = new java.util.HashSet<>();
        for (Snapshot s : parsed) {
          List<String> paths = lists.get(s.snapshotId());
          if (paths == null) {
            snapshots.add(s);
            continue;
          }
          loaded.add(s.snapshotId());
          Made made = table.made.get(s.snapshotId());
          if (made == null || made.paths != paths || !sameSnapshot(made.snapshot, s)) {
            List<ManifestFile> manifests = new java.util.ArrayList<>(paths.size());
            for (String path : paths) {
              ManifestFile entry = pool.get(path);
              if (entry != null) {
                manifests.add(table.copies.computeIfAbsent(
                    entry, m -> org.apache.iceberg.GenericManifestFile.copyOf(m).build()));
              }
            }
            made = new Made(paths, new InlineSnapshot(
                s.sequenceNumber(), s.snapshotId(), s.parentId(),
                s.timestampMillis(), s.operation(), s.summary(),
                s.schemaId(), s.firstRowId(), s.addedRows(), s.keyId(),
                manifests));
            table.made.put(s.snapshotId(), made);
          }
          snapshots.add(made.snapshot);
        }
        table.made.keySet().retainAll(loaded);
        return snapshots;
      }
    }

    /** Whether an InlineSnapshot made before holds what {@code parsed} would make it with. */
    private static boolean sameSnapshot(InlineSnapshot made, Snapshot parsed) {
      return made.sequenceNumber() == parsed.sequenceNumber()
          && made.timestampMillis() == parsed.timestampMillis()
          && java.util.Objects.equals(made.parentId(), parsed.parentId())
          && java.util.Objects.equals(made.schemaId(), parsed.schemaId())
          && java.util.Objects.equals(made.firstRowId(), parsed.firstRowId())
          && java.util.Objects.equals(made.addedRows(), parsed.addedRows())
          && java.util.Objects.equals(made.keyId(), parsed.keyId())
          && java.util.Objects.equals(made.operation(), parsed.operation())
          && made.summary().equals(parsed.summary());
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
        TableIdentifier tableId,
        CatalogLog log,
        FileIO io,
        InlineSettings inline,
        InlineSnapshots inlineSnapshots,
        Replica<CatalogState> read) {
      super(tableId, log, io, inline, inlineSnapshots, read);
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
   * ({@code stageCreate}).
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
    // if all validations pass on the current catalog, then commit them in one transaction
    final Replica<CatalogState> current = log.read();
    final CatalogMutation newCatalog = new CatalogMutation(current);
    // Each branch the transaction read must be as it read it, from then until the commit
    // lands. BaseCatalogTransaction checked that before calling, but a commit may have landed
    // since; check again on the state this commit builds on, and readTable makes the commit
    // fail if the table changes before it lands. Each table it writes (a null ref) must be the
    // version it first read, checked with the table's commit below (errata D12).
    Map<TableIdentifier, TableMetadata> written = new HashMap<>();
    for (Map.Entry<BaseCatalogTransaction.TableRef, TableMetadata> read : reads.entrySet()) {
      TableIdentifier readTable = read.getKey().identifier();
      String ref = read.getKey().ref();
      if (ref == null) {
        written.put(readTable, read.getValue());
        continue;
      }
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
      // after the requirements, which say more specifically what changed; stageUpdate pins the
      // version until the commit lands
      TableMetadata firstRead = written.get(tableId);
      if (firstRead != null
          && !BaseCatalogTransaction.unchangedSinceRead(null, firstRead, currentMetadata)) {
        throw new CommitFailedException(
            "Cannot commit transaction: table %s changed since the transaction read it", tableId);
      }

      TableMetadata.Builder newMetadataBuilder = TableMetadata.buildFrom(currentMetadata);
      commit.updates().forEach(update -> update.applyTo(newMetadataBuilder));
      final TableMetadata newMetadata = newMetadataBuilder.build();
      if (newMetadata.changes().isEmpty()) {
        continue;
      }
      checkAddedSnapshots(tableId, currentMetadata, newMetadata, ops.io());
      // The table's new inline-ML snapshots hold their manifest lists.
      ops.stageUpdate(newCatalog, currentMetadata, newMetadata);
      // Whatever the catalog's conflict encoding, the update applies only on the version it read.
      newCatalog.pin(tableId);
    }
    // Not retried: a transaction conflicts on the whole version of each table it pins, so a
    // conflict fails it (errata D12).
    newCatalog.commit(log);
  }

  /**
   * Checks that each snapshot a transaction adds to {@code tableId} names a schema and partition
   * specs that {@code updated} has. The transaction built its snapshots on the version of the
   * table it read, and its updates apply to the current version, checked only against the
   * requirements Iceberg derives from them; an append's are that its branch is unchanged. An
   * expiry that cleaned expired metadata since may have removed the schema or a partition spec a
   * snapshot uses, and the table could not read it (Bug 69).
   */
  private static void checkAddedSnapshots(
      TableIdentifier tableId, TableMetadata current, TableMetadata updated, FileIO io) {
    for (Snapshot snapshot : updated.snapshots()) {
      if (current.snapshot(snapshot.snapshotId()) != null) {
        continue;
      }
      Integer schemaId = snapshot.schemaId();
      if (schemaId != null && !updated.schemasById().containsKey(schemaId)) {
        throw new ValidationException(
            "Cannot commit snapshot %s of %s: its schema %s was removed concurrently",
            snapshot.snapshotId(), tableId, schemaId);
      }
      for (ManifestFile manifest : snapshot.allManifests(io)) {
        if (!updated.specsById().containsKey(manifest.partitionSpecId())) {
          throw new ValidationException(
              "Cannot commit snapshot %s of %s: its partition spec %s was removed concurrently",
              snapshot.snapshotId(), tableId, manifest.partitionSpecId());
        }
      }
    }
  }

  @Override
  public CatalogTransaction createTransaction(CatalogTransaction.IsolationLevel isolationLevel) {
    return new BaseCatalogTransaction(this, isolationLevel);
  }
}
