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
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Canaries for fake-gcs-server's <em>current</em> behaviour. The catalog's standard-bucket
 * CAS pins each write with {@code generationMatch}. fake-gcs-server enforces that on
 * multipart uploads ({@code storage.create}) and, since 1.55.0, on resumable uploads
 * ({@code BlobWriteSession}, {@code WriteChannel}); 1.49.2 silently ignored it on the
 * resumable path, so every conflict test in {@link GCSCatalogTest} failed on the emulator.
 * Both canaries below are positive: they guard against a future image that accepts our
 * writes but ignores the precondition, which would turn the CAS suite into uniform
 * false-green (or, as with 1.49.2, uniform false-red).
 *
 * <p>When a canary fires after a fake-gcs-server image bump in {@link GCSCatalogTest},
 * treat the run as untrusted until the cause is understood: either the emulator
 * regressed, or our generation tracking changed shape.
 */
@DisabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
public class FakeGcsEmulatorCanaries {

  // Pin a specific fake-gcs-server image tag so canary results are reproducible.
  // Bumping this tag triggers the canary — when it fires, fake-gcs-server changed.
  private static final String FAKE_GCS_IMAGE = "fsouza/fake-gcs-server:1.56.1";
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

  /**
   * CANARY (positive): fake-gcs-server must enforce {@code ifGenerationMatch} on resumable
   * uploads too. The precondition travels on the upload's initiate request and is applied
   * at finalize; fake-gcs-server before 1.55.0 dropped it there and overwrote the stale
   * generation. Same shape as {@link #fakeGcsEnforcesGenerationMatch}, through {@code
   * BlobWriteSession}.
   */
  @Test
  void fakeGcsEnforcesGenerationMatchOnResumableUpload() throws IOException {
    String name = "canary/gen-match-resumable/" + UUID.randomUUID();
    BlobInfo info = BlobInfo.newBuilder(BUCKET, name).build();
    Blob v1 = storage.create(info, "v1".getBytes());
    long staleGen = v1.getGeneration();
    storage.create(info, "v2".getBytes()); // overwrites, advances generation
    BlobWriteSession session =
        storage.blobWriteSession(info, Storage.BlobWriteOption.generationMatch(staleGen));
    try (WritableByteChannel channel = session.open()) {
      channel.write(ByteBuffer.wrap("v3".getBytes()));
    } catch (StorageException e) {
      // Some SDK versions surface the finalize failure from close(); getResult() below
      // carries the same failure either way.
    }
    assertThatThrownBy(() -> session.getResult().get())
        .as(
            "fake-gcs-server accepted a resumable upload pinned to a stale generation. "
                + "GCSFileIO's CAS writes go through BlobWriteSession. Treat the run as "
                + "unreliable until the emulator is fixed or replaced.")
        .isInstanceOf(ExecutionException.class)
        .cause()
        .isInstanceOf(StorageException.class)
        .satisfies(ex -> assertThat(((StorageException) ex).getCode()).isEqualTo(412));
    assertThat(new String(storage.readAllBytes(info.getBlobId()))).isEqualTo("v2");
  }
}
