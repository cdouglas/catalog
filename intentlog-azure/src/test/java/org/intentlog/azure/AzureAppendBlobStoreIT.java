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
package org.intentlog.azure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpPipelineNextSyncPolicy;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceVersion;
import com.azure.storage.blob.models.BlobDownloadResponse;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.DownloadRetryOptions;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.intentlog.azure.AppendBlobStore.Blob;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import reactor.core.publisher.Mono;

/**
 * {@link AzureAppendBlobStore} on a real account ({@link CloudAccount}): the store contract
 * ({@link AppendBlobStoreContract}; each test under a prefix of its own), and reads at a blob's
 * end, where Azurite differs: a GET whose range starts at the blob's length is 416 InvalidRange
 * (Azurite answers 206 with no bytes; AZURE_LOG.md Q6), so {@link AzureAppendBlobStore#read}
 * starts at the byte before and a read from the end is one request. Run explicitly: {@code mvn
 * -o -pl intentlog-azure test -Dtest=AzureAppendBlobStoreIT}.
 */
@EnabledIfEnvironmentVariable(named = "AZURE_LOG_IT_CONTAINER_URL", matches = ".+")
class AzureAppendBlobStoreIT extends AppendBlobStoreContract {
  private static final String PREFIX = CloudAccount.RUN + "/store";
  private static final int BIG = 1000;

  private AppendBlobStore store;
  private String test;

  @BeforeEach
  void prefixOfItsOwn() {
    test = PREFIX + "/" + UUID.randomUUID() + "/";
    store = new PrefixedAppendBlobStore(CloudAccount.store(), test);
  }

  @Override
  protected AppendBlobStore store() {
    return store;
  }

  /** A client pinned to a service version before 2022-11-02: blocks of at most 4 MiB. */
  @Override
  protected AppendBlobStore storeOfSmallBlocks() {
    return new PrefixedAppendBlobStore(
        new AzureAppendBlobStore(CloudAccount.container(BlobServiceVersion.V2021_12_02)), test);
  }

  @AfterAll
  static void deleteWhatItWrote() {
    CloudAccount.deleteAll(PREFIX);
  }

  private static byte[] bigBlock(char c) {
    byte[] b = new byte[BIG];
    Arrays.fill(b, (byte) c);
    return b;
  }

  @Test
  void aGetFromABlobsEndIsOutOfRange() {
    String name = PREFIX + "/raw";
    CloudAccount.store().createAppendBlob(name, Map.of());
    CloudAccount.store().appendBlock(name, bigBlock('a'), 0, -1);
    BlobClient blob = CloudAccount.container().getBlobClient(name);
    DownloadRetryOptions once = new DownloadRetryOptions().setMaxRetryRequests(0);

    assertThatThrownBy(() -> blob.downloadStreamWithResponse(
            new ByteArrayOutputStream(), new BlobRange(BIG), once, null, false, null, Context.NONE))
        .isInstanceOfSatisfying(BlobStorageException.class, e -> {
          assertThat(e.getStatusCode()).isEqualTo(416);
          assertThat(e.getErrorCode()).isEqualTo(BlobErrorCode.INVALID_RANGE);
        });

    ByteArrayOutputStream body = new ByteArrayOutputStream();
    BlobDownloadResponse last = blob.downloadStreamWithResponse(
        body, new BlobRange(BIG - 1), once, null, false, null, Context.NONE);
    assertThat(last.getStatusCode()).isEqualTo(206);
    assertThat(body.toByteArray()).containsExactly((byte) 'a');
  }

  @Test
  void aReadFromTheEndIsOneRequest() {
    AtomicInteger requests = new AtomicInteger();
    HttpPipelinePolicy counting = new HttpPipelinePolicy() {
      @Override
      public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
        requests.incrementAndGet();
        return next.process();
      }

      @Override
      public HttpResponse processSync(
          HttpPipelineCallContext context, HttpPipelineNextSyncPolicy next) {
        requests.incrementAndGet();
        return next.processSync();
      }
    };
    AppendBlobStore counted = new AzureAppendBlobStore(CloudAccount.container(counting));
    String name = PREFIX + "/seg";
    counted.createAppendBlob(name, Map.of("k", "v"));
    counted.appendBlock(name, bigBlock('a'), 0, -1);

    requests.set(0);
    Blob end = counted.read(name, BIG);
    assertThat(requests).hasValue(1);
    assertThat(end.bytes()).isEmpty();
    assertThat(end.length()).isEqualTo(BIG);
    assertThat(end.blocks()).isEqualTo(1);
    assertThat(end.metadata()).isEqualTo(Map.of("k", "v"));

    // Past the end: the byte before is past it too, so a 416 and a Get Blob Properties
    requests.set(0);
    Blob past = counted.read(name, BIG + 5);
    assertThat(requests).hasValue(2);
    assertThat(past.bytes()).isEmpty();
    assertThat(past.length()).isEqualTo(BIG);
  }
}
