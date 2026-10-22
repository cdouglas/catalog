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

/**
 * The FileIO of a {@link FileIOCatalog} table: its catalog's, except that the synthetic location
 * of the table's current inline metadata ({@code inline://<table>#<id>.<version>}) reads as a file
 * of the metadata's stored length. Iceberg's metadata tables (snapshots, history, refs,
 * partitions, manifests, all_manifests, metadata_log_entries) record the length of the table's
 * metadata file, and failed on an inline table without it. No metadata file is there, so it
 * cannot be opened. Everything else goes to the catalog's FileIO; {@link #of} keeps the prefix and
 * bulk operations it supports.
 */
class InlineMetadataFileIO implements FileIO {

  /** The stored length of the table's current inline metadata, if {@code location} names it. */
  @FunctionalInterface
  interface Lengths {
    Long of(String location);
  }

  private final FileIO io;
  // Only where the table was loaded: a copy serialized to a worker is the catalog's FileIO.
  private final transient Lengths lengths;

  static InlineMetadataFileIO of(FileIO io, Lengths lengths) {
    return io instanceof DelegateFileIO
        ? new WithPrefixAndBulk(io, lengths)
        : new InlineMetadataFileIO(io, lengths);
  }

  private InlineMetadataFileIO(FileIO io, Lengths lengths) {
    this.io = io;
    this.lengths = lengths;
  }

  FileIO delegate() {
    return io;
  }

  @Override
  public InputFile newInputFile(String path) {
    Long length = lengths != null && path.startsWith("inline://") ? lengths.of(path) : null;
    return length != null ? new InlineMetadataFile(path, length) : io.newInputFile(path);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return path.startsWith("inline://") ? newInputFile(path) : io.newInputFile(path, length);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return io.newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    io.deleteFile(path);
  }

  @Override
  public Map<String, String> properties() {
    return io.properties();
  }

  @Override
  public void initialize(Map<String, String> properties) {
    io.initialize(properties);
  }

  @Override
  public void close() {
    io.close();
  }

  /** The table's inline metadata, as a file that has a length but no readable content. */
  private static final class InlineMetadataFile implements InputFile {
    private final String location;
    private final long length;

    InlineMetadataFile(String location, long length) {
      this.location = location;
      this.length = length;
    }

    @Override
    public long getLength() {
      return length;
    }

    @Override
    public SeekableInputStream newStream() {
      throw new UnsupportedOperationException(
          "Inline table metadata is stored in the catalog, not in a file: " + location);
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public boolean exists() {
      return true;
    }
  }

  /** For a catalog FileIO with prefix and bulk operations, which this passes on. */
  private static final class WithPrefixAndBulk extends InlineMetadataFileIO
      implements DelegateFileIO {

    private WithPrefixAndBulk(FileIO io, Lengths lengths) {
      super(io, lengths);
    }

    private DelegateFileIO prefixAndBulk() {
      return (DelegateFileIO) delegate();
    }

    @Override
    public Iterable<FileInfo> listPrefix(String prefix) {
      return prefixAndBulk().listPrefix(prefix);
    }

    @Override
    public void deletePrefix(String prefix) {
      prefixAndBulk().deletePrefix(prefix);
    }

    @Override
    public void deleteFiles(Iterable<String> pathsToDelete) {
      prefixAndBulk().deleteFiles(pathsToDelete);
    }
  }
}
