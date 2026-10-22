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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Canaries that assert versitygw's <em>current</em> behaviour (image pinned in {@link
 * VersityGwContainer#IMAGE}). One is a gap canary: other tests in this module skip on
 * versitygw and depend on the gap remaining open. The rest are positive canaries for the
 * preconditions the catalog's CAS path depends on; if one of those fails, the CAS tests
 * would pass for the wrong reason and the run cannot be trusted.
 *
 * <p>When the gap canary starts failing, versitygw has caught up: remove the matching
 * {@code mode.assumeRealCloud(...)} skips and delete the canary.
 *
 * <p>The class is skipped when {@code AWS_ACCESS_KEY_ID} is set — under real S3 the gap
 * doesn't exist and the gap canary would itself fail.
 */
@DisabledIfEnvironmentVariable(named = "AWS_ACCESS_KEY_ID", matches = ".+")
public class VersityGwEmulatorCanaries {

  private static VersityGwContainer container;
  private static S3Client s3;
  private static String bucket;

  @BeforeAll
  static void setUp() {
    container = new VersityGwContainer();
    container.start();
    s3 = VersityGwContainer.createS3Client(container.endpoint());
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
   * CANARY: versitygw accepts {@code writeOffsetBytes} on PutObject and ignores it. Real S3
   * directory buckets append at the offset, and reject one that is not the object's current
   * length with {@code InvalidWriteOffset}; versitygw overwrites the object instead. So every
   * test that runs with {@code maxAppendCount > 0} skips on versitygw — see the {@code
   * assumeRealCloud(...)} calls in {@link TestS3Catalog#before} and {@link
   * TestS3FileIOCatalogTransaction#before}.
   *
   * <p>When this fails, verify the full APPEND protocol (append at the correct offset,
   * reject a wrong one, honour If-Match together with the offset), then drop those skips
   * and delete this canary.
   */
  @Test
  void versitygwIgnoresWriteOffsetBytes() {
    String key = "canary/write-offset-bytes/" + UUID.randomUUID();
    put(key, "v1", b -> {});
    // Deliberately wrong offset: the object is 2 bytes, claim it's 99.
    put(key, "v2", b -> b.writeOffsetBytes(99L));
    assertThat(get(key))
        .as(
            "versitygw no longer overwrites on a wrong writeOffsetBytes. Verify the APPEND "
                + "protocol works, drop the assumeRealCloud(\"S3 Express writeOffsetBytes...\") "
                + "skips in TestS3Catalog#before and TestS3FileIOCatalogTransaction#before, "
                + "then delete this canary.")
        .isEqualTo("v2");
  }

  /** CANARY (positive): a PutObject with a stale {@code If-Match} must be rejected (412). */
  @Test
  void versitygwEnforcesIfMatch() {
    String key = "canary/if-match/" + UUID.randomUUID();
    String v1Etag = put(key, "v1", b -> {});
    put(key, "v2", b -> {}); // the live ETag is no longer v1's
    assertThatThrownBy(() -> put(key, "v3", b -> b.ifMatch(v1Etag)))
        .as("versitygw accepted a PutObject with a stale If-Match ETag")
        .isInstanceOf(S3Exception.class)
        .satisfies(ex -> assertThat(((S3Exception) ex).statusCode()).isEqualTo(412));
    assertThat(get(key)).isEqualTo("v2");
  }

  /** CANARY (positive): {@code If-None-Match: *} on an existing key must be rejected (412). */
  @Test
  void versitygwEnforcesIfNoneMatch() {
    String key = "canary/if-none-match/" + UUID.randomUUID();
    put(key, "v1", b -> b.ifNoneMatch("*"));
    assertThatThrownBy(() -> put(key, "v2", b -> b.ifNoneMatch("*")))
        .as("versitygw accepted If-None-Match: * on an existing key")
        .isInstanceOf(S3Exception.class)
        .satisfies(ex -> assertThat(((S3Exception) ex).statusCode()).isEqualTo(412));
    assertThat(get(key)).isEqualTo("v1");
  }

  /**
   * CANARY (positive): concurrent conditional PUTs against the same ETag must have exactly
   * one winner, and the winner's bytes must be the ones stored. The catalog's conflict tests
   * depend on the check and the write being atomic, which a per-request check alone does
   * not give.
   */
  @Test
  void versitygwConditionalPutHasOneWinner() throws Exception {
    int writers = 16;
    ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      for (int round = 0; round < 10; round++) {
        String key = "canary/race/" + UUID.randomUUID();
        String etag = put(key, "base", b -> {});
        CountDownLatch go = new CountDownLatch(1);
        List<Future<String>> results = Lists.newArrayList();
        for (int w = 0; w < writers; w++) {
          String body = "writer-" + w;
          results.add(
              pool.submit(
                  () -> {
                    go.await();
                    try {
                      put(key, body, b -> b.ifMatch(etag));
                      return body;
                    } catch (S3Exception e) {
                      if (e.statusCode() == 412 || e.statusCode() == 409) {
                        return null;
                      }
                      throw e;
                    }
                  }));
        }
        go.countDown();
        List<String> winners = Lists.newArrayList();
        for (Future<String> result : results) {
          if (result.get() != null) {
            winners.add(result.get());
          }
        }
        assertThat(winners).as("writers whose conditional PUT succeeded").hasSize(1);
        assertThat(get(key)).isEqualTo(winners.get(0));
      }
    } finally {
      pool.shutdownNow();
    }
  }

  private static String put(String key, String body, Consumer<PutObjectRequest.Builder> conds) {
    PutObjectRequest.Builder request = PutObjectRequest.builder().bucket(bucket).key(key);
    conds.accept(request);
    return s3.putObject(request.build(), RequestBody.fromString(body)).eTag();
  }

  private static String get(String key) {
    return s3.getObjectAsBytes(b -> b.bucket(bucket).key(key)).asUtf8String();
  }
}
