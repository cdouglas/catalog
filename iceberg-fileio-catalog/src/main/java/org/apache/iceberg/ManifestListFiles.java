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
package org.apache.iceberg;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Writes the manifest list of a snapshot whose list the catalog holds inline to an Avro file, as
 * {@link SnapshotProducer} would have written it. A table's metadata.json can name only manifest
 * list files, so a table that leaves inline storage needs one for each of its inline snapshots.
 * {@link ManifestLists#write} and {@link BaseSnapshot}'s constructor are package-private, hence
 * this class's package.
 */
public final class ManifestListFiles {
  private ManifestListFiles() {}

  /**
   * Writes {@code manifests}, {@code snapshot}'s manifest list, to {@code file} and returns the
   * snapshot backed by that file. The manifests are already final (sequence numbers and first row
   * ids assigned), so the writer copies them unchanged. A snapshot of a v3 table without a first
   * row id was written before the table was upgraded to v3, and gets the v2 list it had then.
   */
  public static Snapshot write(
      int formatVersion, Snapshot snapshot, List<ManifestFile> manifests, OutputFile file) {
    Preconditions.checkArgument(
        snapshot.keyId() == null, "Cannot write an encrypted manifest list: %s", snapshot);
    Long firstRowId = formatVersion >= 3 ? snapshot.firstRowId() : null;
    int listVersion = formatVersion >= 3 && firstRowId == null ? 2 : formatVersion;
    try (ManifestListWriter writer =
        ManifestLists.write(
            listVersion,
            file,
            PlaintextEncryptionManager.instance(),
            snapshot.snapshotId(),
            snapshot.parentId(),
            snapshot.sequenceNumber(),
            firstRowId)) {
      writer.addAll(manifests);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write manifest list file: %s", file.location());
    }
    return new BaseSnapshot(
        snapshot.sequenceNumber(),
        snapshot.snapshotId(),
        snapshot.parentId(),
        snapshot.timestampMillis(),
        snapshot.operation(),
        snapshot.summary(),
        snapshot.schemaId(),
        file.location(),
        snapshot.firstRowId(),
        snapshot.addedRows(),
        snapshot.keyId());
  }
}
