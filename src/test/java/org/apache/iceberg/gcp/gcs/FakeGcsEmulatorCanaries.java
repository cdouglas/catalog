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
package org.apache.iceberg.gcp.gcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Canaries for fake-gcs-server's <em>current</em> behaviour. The catalog itself uses
 * standard-bucket CAS via {@code BlobWriteSession} with {@code generationMatch}; on the
 * happy path fake-gcs-server emulates this and the {@link GCSCatalogTest} suite needs
 * no skips. The single positive canary below guards against silent regressions where
 * a future image accepts our writes but ignores the generation precondition — which
 * would turn the CAS suite into uniform false-green.
 *
 * <p>When this canary fires after a fake-gcs-server image bump in
 * {@link GCSCatalogTest}, treat the run as untrusted until the cause is understood:
 * either the emulator regressed, or our generation tracking changed shape.
 */
@DisabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
public class FakeGcsEmulatorCanaries {

  // Pin a specific fake-gcs-server image tag so canary results are reproducible.
  // Bumping this tag triggers the canary — when it fires, fake-gcs-server changed.
  private static final String FAKE_GCS_IMAGE = "fsouza/fake-gcs-server:1.49.2";
  private static final int FAKE_GCS_PORT = 4443;
  private static final String BUCKET = "canary-bucket";

  private static GenericContainer<?> container;
  private static Storage storage;

  @BeforeAll
  static void setUp() throws IOException {
    container =
        new GenericContainer<>(DockerImageName.parse(FAKE_GCS_IMAGE))
            .withExposedPorts(FAKE_GCS_PORT)
            .withCommand(
                "-scheme", "http",
                "-port", String.valueOf(FAKE_GCS_PORT));
    container.start();
    String endpoint =
        "http://" + container.getHost() + ":" + container.getMappedPort(FAKE_GCS_PORT);
    updateFakeGcsExternalUrl(endpoint);
    storage =
        StorageOptions.newBuilder()
            .setHost(endpoint)
            .setProjectId("test-project")
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getService();
    storage.create(BucketInfo.of(BUCKET));
  }

  private static void updateFakeGcsExternalUrl(String externalUrl) throws IOException {
    URL url = new URL(externalUrl + "/_internal/config");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("PUT");
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setDoOutput(true);
    byte[] body = ("{\"externalUrl\":\"" + externalUrl + "\"}").getBytes();
    try (OutputStream out = conn.getOutputStream()) {
      out.write(body);
    }
    int code = conn.getResponseCode();
    if (code / 100 != 2) {
      throw new IOException("fake-gcs-server /_internal/config returned HTTP " + code);
    }
  }

  @AfterAll
  static void tearDown() {
    if (container != null) {
      container.stop();
      container = null;
    }
  }

  /**
   * CANARY (positive): fake-gcs-server must enforce {@code ifGenerationMatch}. The
   * catalog's CAS path pins each write to the object's current generation; if the
   * emulator silently ignores the precondition, every CAS test in {@link GCSCatalogTest}
   * passes for the wrong reason. This canary writes v1, overwrites to v2 without
   * preconditions, then attempts a v3 PUT pinned to v1's stale generation — and
   * asserts the precondition is rejected with HTTP 412.
   */
  @Test
  void fakeGcsEnforcesGenerationMatch() {
    String name = "canary/gen-match/" + UUID.randomUUID();
    BlobInfo info = BlobInfo.newBuilder(BUCKET, name).build();
    Blob v1 = storage.create(info, "v1".getBytes());
    long staleGen = v1.getGeneration();
    storage.create(info, "v2".getBytes()); // overwrites, advances generation
    assertThatThrownBy(
            () ->
                storage.create(
                    info,
                    "v3".getBytes(),
                    Storage.BlobTargetOption.generationMatch(staleGen)))
        .as(
            "fake-gcs-server accepted a create() pinned to a stale generation. CAS "
                + "semantics in GCSCatalogTest rely on this precondition. Treat the run "
                + "as unreliable until the emulator is fixed or replaced.")
        .isInstanceOf(StorageException.class)
        .satisfies(ex -> assertThat(((StorageException) ex).getCode()).isEqualTo(412));
  }
}
