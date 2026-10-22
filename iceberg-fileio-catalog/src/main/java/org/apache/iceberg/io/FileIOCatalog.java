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

import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.LocationUtil;
import org.intentlog.fileio.AtomicFileLog;

/**
 * A catalog kept in one object of a store with conditional writes ({@link
 * SupportsAtomicOperations}: S3, S3 Express, GCS, ADLS), beside its tables' files: a {@link
 * LogCatalog} over an {@link AtomicFileLog}. The object is at {@code <warehouse>/catalog} unless
 * the catalog is given a location; the store is the catalog's FileIO ({@code io-impl}).
 *
 * <p>Configuration (with {@link LogCatalogProperties} for how commits append and compact):
 *
 * <ul>
 *   <li>{@code fileio.catalog.inline}: inline table metadata (default false)
 *   <li>{@code fileio.catalog.inline.manifests}: inline manifest lists too (default false)
 *   <li>{@code fileio.catalog.inline.max.update.size}: the largest inline table update, in bytes
 * </ul>
 */
public class FileIOCatalog extends LogCatalog {

  private String catalogLocation;
  private SupportsAtomicOperations fileIO;

  @SuppressWarnings("unused") // reflection cstr
  FileIOCatalog() {
    super("fileio", Maps.newHashMap());
  }

  /**
   * A catalog in the object at {@code catalogLocation} of {@code fileIO}, which also holds its
   * tables' files. {@link #initialize} completes it, and creates the object if it does not exist.
   */
  public FileIOCatalog(
      String catalogName,
      String catalogLocation,
      SupportsAtomicOperations fileIO,
      Map<String, String> catalogProperties) {
    super(catalogName, catalogProperties);
    this.catalogLocation = catalogLocation;
    this.fileIO = fileIO;
  }

  @Override
  protected void openLog(Map<String, String> properties) {
    if (null == catalogLocation) {
      String warehouseLocation = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
      Preconditions.checkArgument(
          !Strings.isNullOrEmpty(warehouseLocation),
          "Cannot initialize FileIOCatalog because warehousePath must not be null or empty");
      catalogLocation = LocationUtil.stripTrailingSlash(warehouseLocation) + "/catalog";
    }
    if (null == fileIO) {
      // TODO check warehouseLocation schema?
      String fileIOImpl =
          properties.getOrDefault(
              CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.gcp.gcs.GCSFileIO");

      // TODO handle this more gracefully; use listings/HadoopCatalog?
      fileIO = (SupportsAtomicOperations) CatalogUtil.loadFileIO(fileIOImpl, properties, null);
    }
    open(new AtomicFileLog(fileIO, catalogLocation), fileIO);
  }
}
