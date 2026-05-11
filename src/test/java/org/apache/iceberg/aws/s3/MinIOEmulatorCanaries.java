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
package org.apache.iceberg.aws.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.testcontainers.containers.MinIOContainer;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Canaries that assert MinIO's <em>current</em> behaviour. Each test exists because
 * other tests in this module skip on MinIO and depend on the gap remaining open.
 *
 * <p>When a canary starts failing, MinIO has caught up: remove the matching
 * {@code mode.assumeRealCloud(...)} skip and delete the canary. The image tag is
 * pinned via {@link org.apache.iceberg.aws.s3.MinioUtil#LATEST_TAG} (this class
 * pins explicitly below to avoid surprises from the catalog test's container
 * lifecycle); a canary fires on a deliberate image bump, which is exactly when we
 * want to revisit our workarounds.
 *
 * <p>The class is skipped when {@code AWS_ACCESS_KEY_ID} is set — under real S3 the
 * gaps don't exist and the canary's negative assertions would themselves fail.
 */
@DisabledIfEnvironmentVariable(named = "AWS_ACCESS_KEY_ID", matches = ".+")
public class MinIOEmulatorCanaries {

  // Pin a specific MinIO image tag so canary results are reproducible. Bumping this
  // tag is the trigger that fires the canaries — when a canary fails after a bump,
  // MinIO gained the feature being canaried.
  private static final String MINIO_TAG = "RELEASE.2024-12-18T13-15-44Z";

  private static MinIOContainer container;
  private static S3Client s3;
  private static String bucket;

  @BeforeAll
  static void setUp() {
    container = MinioUtil.createContainer(MINIO_TAG, null);
    container.start();
    s3 = MinioUtil.createS3Client(container);
    bucket = "canary-" + UUID.randomUUID().toString().substring(0, 8);
    s3.createBucket(b -> b.bucket(bucket));
  }

  @AfterAll
  static void tearDown() {
    if (s3 != null) {
      s3.close();
      s3 = null;
    }
    if (container != null) {
      container.stop();
      container = null;
    }
  }

  /**
   * CANARY: MinIO accepts {@code writeOffsetBytes} on PutObject but does not enforce
   * the offset precondition. Real S3 directory buckets reject a PutObject whose
   * {@code writeOffsetBytes} does not equal the object's current length with
   * {@code InvalidWriteOffset}; MinIO silently accepts and overwrites the object.
   * Either behaviour breaks the catalog's APPEND-mode commit invariants, so on MinIO
   * we skip every test that runs with {@code maxAppendCount > 0} — see the
   * {@code assumeRealCloud(...)} call in {@link TestS3Catalog#before} and the mirror
   * in {@link TestS3FileIOCatalogTransaction}.
   *
   * <p>This canary writes v1, then issues a PutObject with a deliberately wrong
   * {@code writeOffsetBytes} value and asserts the request succeeds and overwrites
   * v1. When MinIO starts enforcing the precondition, the second PutObject will fail
   * (or behave like an append) and this canary will fail — at which point verify the
   * full APPEND semantics work, drop the APPEND-mode skips on MinIO, and delete this
   * canary.
   */
  @Test
  void minioIgnoresWriteOffsetBytes() {
    String key = "canary/write-offset-bytes/" + UUID.randomUUID();
    s3.putObject(
        PutObjectRequest.builder().bucket(bucket).key(key).build(),
        RequestBody.fromString("v1"));
    // Deliberately wrong offset: object is 2 bytes, claim it's 99 bytes.
    s3.putObject(
        PutObjectRequest.builder().bucket(bucket).key(key).writeOffsetBytes(99L).build(),
        RequestBody.fromString("v2"));
    String body =
        s3.getObjectAsBytes(GetObjectRequest.builder().bucket(bucket).key(key).build())
            .asUtf8String();
    assertThat(body)
        .as(
            "MinIO appears to be enforcing writeOffsetBytes — the wrong-offset PUT no "
                + "longer overwrites v1 with v2. Verify the full APPEND protocol works "
                + "(write, then atomic append at the correct offset), drop the "
                + "assumeRealCloud(\"S3 Express writeOffsetBytes...\") skips in "
                + "TestS3Catalog#before and TestS3FileIOCatalogTransaction#before, then "
                + "delete this canary.")
        .isEqualTo("v2");
  }

  /**
   * CANARY (positive): MinIO must enforce If-Match preconditions. The catalog's CAS path
   * depends on {@code PutObject(If-Match=etag)} returning {@code 412 PreconditionFailed}
   * when the object's current ETag does not match. If MinIO ever silently ignores the
   * precondition (returns 200 on mismatch), every CAS test in the module produces false
   * green. This canary asserts the precondition is honoured; if it starts failing, the
   * suite's CAS guarantees are no longer being validated against MinIO and the run
   * should be treated as unreliable until the emulator is fixed or replaced.
   */
  @Test
  void minioEnforcesIfMatchPrecondition() {
    String key = "canary/if-match/" + UUID.randomUUID();
    String v1Etag =
        s3.putObject(
                PutObjectRequest.builder().bucket(bucket).key(key).build(),
                RequestBody.fromString("v1"))
            .eTag();
    // Overwrite to v2 without preconditions, so the live ETag is no longer v1Etag.
    s3.putObject(
        PutObjectRequest.builder().bucket(bucket).key(key).build(),
        RequestBody.fromString("v2"));
    // Stale If-Match: must be rejected with 412.
    assertThatThrownBy(
            () ->
                s3.putObject(
                    PutObjectRequest.builder().bucket(bucket).key(key).ifMatch(v1Etag).build(),
                    RequestBody.fromString("v3")))
        .as(
            "MinIO accepted a PutObject with a stale If-Match ETag. CAS semantics in this "
                + "module rely on the precondition being honoured. If this fails after an "
                + "image bump, investigate before trusting the CAS tests.")
        .isInstanceOf(S3Exception.class)
        .satisfies(
            ex -> assertThat(((S3Exception) ex).statusCode()).isEqualTo(412));
  }
}
