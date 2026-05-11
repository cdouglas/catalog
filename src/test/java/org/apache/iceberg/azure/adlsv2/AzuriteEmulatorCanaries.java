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

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.options.DataLakeFileOutputStreamOptions;
import com.azure.storage.file.datalake.options.FileParallelUploadOptions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

/**
 * Canaries for Azurite's <em>current</em> behaviour. The {@link ADLSCatalogTest} suite
 * runs unmodified against Azurite today — no skips, no workarounds — so this class
 * holds only positive canaries that guard against silent regressions in the
 * preconditions the catalog's CAS path depends on.
 *
 * <p>If a future Azurite image breaks {@code If-Match} enforcement, the catalog's CAS
 * tests would pass for the wrong reason. The canary below writes v1, overwrites to v2
 * without preconditions, then attempts a v3 write pinned to v1's stale ETag and
 * asserts it is rejected. A failure here after an image bump means the run cannot
 * be trusted.
 *
 * <p>New negative canaries belong here when a workaround for an Azurite gap is added
 * to the test suite — for example, if AppendBlob flush-position validation turns out
 * to be unimplemented and we end up skipping the append-mode tests on Azurite.
 */
@DisabledIfEnvironmentVariable(named = "AZURE_SAS_CREDENTIALS_FILE", matches = ".+")
public class AzuriteEmulatorCanaries {

  private static AzuriteContainer container;

  @BeforeAll
  static void setUp() {
    container = new AzuriteContainer();
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
   * CANARY (positive): Azurite must enforce {@code If-Match} preconditions on
   * {@code DataLakeFileClient} writes. The catalog's CAS path pins each write to the
   * file's current ETag; if Azurite silently ignores the precondition, every CAS test
   * in {@link ADLSCatalogTest} passes for the wrong reason. This canary asserts the
   * precondition is rejected with HTTP 412 on a stale ETag.
   */
  /**
   * CANARY: Azurite returns HTTP 400 (empty body) for {@code uploadWithResponse} on a
   * path that does not yet exist. Real ADLS atomically creates the file as part of the
   * upload. This is the gap that forces {@link ADLSCatalogTest#before} (and the
   * transaction-test mirror) to skip every test under {@link CloudMode#AZURITE}: the
   * catalog's very first commit writes a non-existent catalog file via
   * {@code ADLSOutputFile.replaceDestObj}, which delegates to
   * {@code uploadWithResponse}.
   *
   * <p>If this canary fails, Azurite started accepting the upload (or returning a more
   * specific status). Verify that {@code uploadWithResponse} now creates the file
   * atomically (write content, flush, read back), drop the {@code AZURITE} skips in
   * {@link ADLSCatalogTest#before} and the transaction-test mirror, and delete this
   * canary.
   */
  @Test
  void azuriteRejectsUploadToNonExistentPath() {
    String name = "canary/no-create-then-upload/" + UUID.randomUUID();
    DataLakeFileClient file = container.fileClient(name);
    byte[] data = "v1".getBytes();
    assertThatThrownBy(
            () ->
                file.uploadWithResponse(
                    new FileParallelUploadOptions(new ByteArrayInputStream(data), data.length),
                    null,
                    null))
        .as(
            "Azurite accepted uploadWithResponse on a non-existent path. Real ADLS does "
                + "this too — verify the file exists with the expected content, then drop "
                + "the assumeRealCloud(AZURITE, \"DataLake uploadWithResponse...\") skips "
                + "in ADLSCatalogTest#before and ADLSFileIOCatalogTransactionTests#before "
                + "and delete this canary.")
        .isInstanceOf(DataLakeStorageException.class)
        .satisfies(
            ex -> assertThat(((DataLakeStorageException) ex).getStatusCode()).isEqualTo(400));
  }

  @Test
  void azuriteEnforcesIfMatchPrecondition() {
    String name = "canary/if-match/" + UUID.randomUUID();
    container.createFile(name, "v1".getBytes());
    DataLakeFileClient file = container.fileClient(name);
    String staleEtag = file.getProperties().getETag();
    container.createFile(name, "v2".getBytes()); // overwrites, ETag advances
    DataLakeFileOutputStreamOptions options =
        new DataLakeFileOutputStreamOptions()
            .setRequestConditions(new DataLakeRequestConditions().setIfMatch(staleEtag));
    assertThatThrownBy(
            () -> {
              try (OutputStream out = file.getOutputStream(options)) {
                out.write("v3".getBytes());
              }
            })
        .as(
            "Azurite accepted a write with a stale If-Match ETag. CAS semantics in "
                + "ADLSCatalogTest rely on this precondition. Treat the run as "
                + "unreliable until Azurite is fixed or replaced.")
        // The DataLake OutputStream wraps a blob-level BlobStorageException in IOException;
        // match by HTTP status code in the message to stay decoupled from the wrapping shape.
        .isInstanceOf(IOException.class)
        .satisfies(ex -> assertThat(ex).hasMessageContaining("412"));
  }
}
