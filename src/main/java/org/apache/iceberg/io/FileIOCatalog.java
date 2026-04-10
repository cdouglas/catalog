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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.LocationUtil;

public class FileIOCatalog extends BaseMetastoreCatalog
    implements Configurable, SupportsNamespaces, SupportsCatalogTransactions {
  // TODO audit loadTable in BaseMetastoreCatalog
  // TODO buildTable overridden in BaseMetastoreCatalog?

  private static final String INLINE_ENABLED = "fileio.catalog.inline";

  private Configuration conf; // TODO: delete
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
      Configuration conf,
      CatalogFormat<?, ?> format,
      SupportsAtomicOperations fileIO,
      Map<String, String> catalogProperties) {
    this.catalogName = catalogName;
    this.catalogLocation = catalogLocation;
    this.conf = conf;
    this.format = format;
    this.fileIO = fileIO;
    this.catalogProperties = catalogProperties;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    super.initialize(name, properties);
    Preconditions.checkNotNull(properties, "Properties are required");
    catalogProperties.putAll(properties);

    if (name != null) {
      catalogName = name;
    }

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
      fileIO = (SupportsAtomicOperations) CatalogUtil.loadFileIO(fileIOImpl, properties, getConf());
    }
    final InputFile catalogFile = fileIO.newInputFile(catalogLocation);
    if (!catalogFile.exists()) {
      format.empty(catalogFile).commit(fileIO);
    }
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
    format
        .from(catalogFile)
        .dropTable(from)
        .createTable(to, catalogFile.location(from))
        .commit(fileIO);
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    boolean inline = Boolean.parseBoolean(
        catalogProperties.getOrDefault(INLINE_ENABLED, "false"));
    return new FileIOTableOperations(
        tableIdentifier, catalogLocation, format, fileIO, inline);
  }

  FileIOTableOperations newTableOps(TableIdentifier tableIdentifier, CatalogFile catalogFile) {
    boolean inline = Boolean.parseBoolean(
        catalogProperties.getOrDefault(INLINE_ENABLED, "false"));
    return new FileIOTableOperations(
        tableIdentifier, catalogLocation, format, fileIO, inline, catalogFile);
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
    return Lists.newArrayList(getCatalogFile().namespaces().iterator());
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
    private volatile CatalogFile lastCatalogFile = null;

    FileIOTableOperations(
        TableIdentifier tableId,
        String catalogLocation,
        CatalogFormat format,
        SupportsAtomicOperations fileIO,
        boolean inlineEnabled) {
      this(tableId, catalogLocation, format, fileIO, inlineEnabled, null);
    }

    FileIOTableOperations(
        TableIdentifier tableId,
        String catalogLocation,
        CatalogFormat format,
        SupportsAtomicOperations fileIO,
        boolean inlineEnabled,
        CatalogFile catalogFile) {
      this.fileIO = fileIO;
      this.format = format;
      this.tableId = tableId;
      this.catalogLocation = catalogLocation;
      this.inlineEnabled = inlineEnabled;
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
        // Inline table: load metadata from catalog bytes
        byte[] inlineMeta = catalogFile.inlineMetadata(tableId);
        String json = new String(inlineMeta, StandardCharsets.UTF_8);
        // Use synthetic location for change detection; custom loader skips file I/O
        String syntheticLoc = "inline://" + tableId + "#v" + System.nanoTime();
        refreshFromMetadataLocation(syntheticLoc, null, 0,
            loc -> TableMetadataParser.fromJson(loc, json));
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
        String mode = InlineDeltaCodec.selectMode(delta, metadata, 0);

        CatalogFile.Mut<?, ?> mut = format.from(lastCatalogFile);
        switch (mode) {
          case "delta":
            byte[] deltaBytes = InlineDeltaCodec.encodeDelta(delta);
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

  // SupportsCatalogTransaction

  @Override
  public void commitTransaction(List<TableIdentifier> readTables, List<TableCommit> commits) {
    // TODO XXX This is wrong. The read version needs to be extracted from the spaghetti
    //      XXX in BaseCatalogTransaction and resolved against table identifiers, but I'm
    //      XXX tired of tinkering with this fucking codebase.
    // TableCommit validations check the table UUID and snapshot ref for each table
    // if all validations pass for the current CatalogFile, then attempt atomic replace
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
      if (inline) {
        // Compute delta from current to new metadata
        String manifestPrefix = "";
        if (current instanceof ProtoCatalogFile) {
          ProtoCatalogFile proto = (ProtoCatalogFile) current;
          Integer tblIdNum = proto.tableId(tableId);
          if (tblIdNum != null) {
            String p = proto.manifestListPrefix(tblIdNum);
            if (p != null) { manifestPrefix = p; }
          }
        }
        java.util.List<InlineDeltaCodec.DeltaUpdate> delta =
            InlineDeltaCodec.computeDelta(currentMetadata, newMetadata, manifestPrefix);
        String mode = InlineDeltaCodec.selectMode(delta, newMetadata, 0);
        switch (mode) {
          case "delta":
            newCatalog.updateTableInlineDelta(tableId, InlineDeltaCodec.encodeDelta(delta));
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
  }

  @Override
  public CatalogTransaction createTransaction(CatalogTransaction.IsolationLevel isolationLevel) {
    return new BaseCatalogTransaction(this, isolationLevel);
  }
}
