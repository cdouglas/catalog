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
package org.apache.iceberg.azure.adlsv2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.azure.core.util.Context;
import com.azure.storage.blob.models.AppendBlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.SpecializedBlobClientBuilder;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import java.io.ByteArrayInputStream;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

/**
 * Canaries for Azurite's <em>current</em> behaviour, as it bears on {@code ADLSFileIO}'s atomic
 * writes: the catalog file is an Append Blob, appended with a conditional {@code AppendBlock} and
 * replaced (CAS) by renaming a staged temp onto it through the Data Lake API.
 *
 * <ul>
 *   <li>Gap canary: Azurite has no Data Lake endpoint, so the rename fails. Every catalog write
 *       starts with a CAS (creating the catalog file), so {@link ADLSCatalogTest} and {@link
 *       ADLSFileIOCatalogTransactionTests} skip on Azurite.
 *   <li>Positive canary: Azurite enforces the {@code AppendBlock} conditions the APPEND path
 *       relies on. If it stopped, append tests would pass for the wrong reason once the gap
 *       closes.
 * </ul>
 */
@DisabledIfEnvironmentVariable(named = "AZURE_STORAGE_ACCOUNT", matches = ".+")
public class AzuriteEmulatorCanaries {

  // Pinned here rather than taking iceberg-azure's AzuriteContainer default (3.30.0): Azurite
  // rejects any x-ms-version newer than it knows, and the azure-sdk-bom this module imports
  // sends 2025-07-05. 3.37.0 accepts up to 2026-06-06. Bumping this tag (or the BOM) is the
  // trigger to re-check these canaries.
  public static final String AZURITE_IMAGE = "mcr.microsoft.com/azure-storage/azurite:3.37.0";

  private static AzuriteContainer container;

  @BeforeAll
  static void setUp() {
    container = new AzuriteContainer(AZURITE_IMAGE);
    container.start();
    container.createStorageContainer();
  }

  @AfterAll
  static void tearDown() {
    if (container != null) {
      try {
        container.deleteStorageContainer();
      } catch (RuntimeException ignored) {
        // best-effort cleanup
      }
      container.stop();
      container = null;
    }
  }

  /**
   * CANARY: Azurite rejects a Data Lake rename (HTTP 400) and leaves the source in place. Real
   * ADLS renames atomically, honouring the destination's {@code If-Match} / {@code If-None-Match}.
   *
   * <p>When this fails, Azurite gained a Data Lake endpoint: check that the rename is atomic and
   * enforces destination conditions, then drop the {@code assumeRealCloud(...)} skips in {@link
   * ADLSCatalogTest#before} and {@link ADLSFileIOCatalogTransactionTests#before} and delete this
   * canary.
   */
  @Test
  void azuriteRejectsDataLakeRename() {
    DataLakeFileClient temp = container.fileClient("canary/rename/" + UUID.randomUUID() + ".tmp");
    AppendBlobClient blob = appendBlob(temp);
    blob.create();
    String etag = append(blob, "v1", 0, null);
    assertThatThrownBy(
            () ->
                temp.renameWithResponse(
                    null,
                    "canary/rename/" + UUID.randomUUID(),
                    new DataLakeRequestConditions().setIfMatch(etag),
                    new DataLakeRequestConditions().setIfNoneMatch("*"),
                    null,
                    Context.NONE))
        .as(
            "Azurite accepted a Data Lake rename. Verify it is atomic and honours destination "
                + "conditions, then drop the assumeRealCloud(AZURITE, \"DataLake rename...\") "
                + "skips and delete this canary.")
        .isInstanceOf(DataLakeStorageException.class)
        .satisfies(
            ex -> assertThat(((DataLakeStorageException) ex).getStatusCode()).isEqualTo(400));
    assertThat(temp.exists()).isTrue();
  }

  /**
   * CANARY (positive): Azurite must enforce the {@code AppendBlock} conditions: a stale {@code
   * If-Match} ETag and a wrong append position are each rejected with 412, and neither block lands.
   */
  @Test
  void azuriteEnforcesAppendBlockConditions() {
    AppendBlobClient blob = appendBlob(container.fileClient("canary/append/" + UUID.randomUUID()));
    blob.create();
    String e1 = append(blob, "ckpt|", 0, null);
    append(blob, "r1|", 5, e1);
    assertThatThrownBy(() -> append(blob, "stale|", 8, e1))
        .as("Azurite accepted an AppendBlock with a stale If-Match ETag")
        .isInstanceOf(BlobStorageException.class)
        .satisfies(ex -> assertThat(((BlobStorageException) ex).getStatusCode()).isEqualTo(412));
    String e2 = blob.getProperties().getETag();
    assertThatThrownBy(() -> append(blob, "wrong-pos|", 5, e2))
        .as("Azurite accepted an AppendBlock at the wrong append position")
        .isInstanceOf(BlobStorageException.class)
        .satisfies(ex -> assertThat(((BlobStorageException) ex).getStatusCode()).isEqualTo(412));
    assertThat(blob.downloadContent().toString()).isEqualTo("ckpt|r1|");
  }

  private static AppendBlobClient appendBlob(DataLakeFileClient file) {
    // Same construction as ADLSOutputFile: the Data Lake client's pipeline, Blob API client.
    // Azurite serves one endpoint, so the URL needs no dfs -> blob rewrite.
    return new SpecializedBlobClientBuilder()
        .pipeline(file.getHttpPipeline())
        .endpoint(file.getFileUrl())
        .buildAppendBlobClient();
  }

  private static String append(AppendBlobClient blob, String data, long position, String etag) {
    byte[] bytes = data.getBytes();
    return blob.appendBlockWithResponse(
            new ByteArrayInputStream(bytes),
            bytes.length,
            null,
            new AppendBlobRequestConditions().setAppendPosition(position).setIfMatch(etag),
            null,
            Context.NONE)
        .getValue()
        .getETag();
  }
}
