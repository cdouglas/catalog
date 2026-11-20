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

import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpPipelineNextSyncPolicy;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.BlobServiceVersion;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.intentlog.azure.AppendBlobStore.Blob;
import org.intentlog.azure.AppendBlobStore.Refused;
import org.intentlog.azure.AppendBlobStore.Throttled;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * {@link AzureAppendBlobStore} against Azurite, in a fresh container per test: the store contract
 * ({@link AppendBlobStoreContract}), and what is the client's own (endpoints, throttling, a read
 * from the end in one request). It runs when {@code AZURITE_BLOB_ENDPOINT} names Azurite's blob
 * endpoint with its account (for example {@code http://127.0.0.1:10000/devstoreaccount1}), and is
 * skipped otherwise; the endpoint checks run either way.
 */
class TestAzureAppendBlobStore extends AppendBlobStoreContract {

  private static final String ENDPOINT = System.getenv("AZURITE_BLOB_ENDPOINT");
  private static final String ACCOUNT = "devstoreaccount1";
  /** Azurite's well-known account key. */
  private static final String KEY =
      "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
  private static final StorageSharedKeyCredential CREDENTIAL =
      new StorageSharedKeyCredential(ACCOUNT, KEY);

  private BlobContainerClient container;
  private AppendBlobStore store;

  @BeforeEach
  void freshContainer() {
    if (ENDPOINT == null || ENDPOINT.isBlank()) {
      return;
    }
    String name = "t" + UUID.randomUUID().toString().replace("-", "");
    String url = ENDPOINT.replaceAll("/+$", "") + "/" + name;
    container = new BlobContainerClientBuilder().endpoint(url).credential(CREDENTIAL).buildClient();
    container.create();
    store = AzureAppendBlobStore.create(url, CREDENTIAL);
  }

  @AfterEach
  void dropContainer() {
    if (container != null) {
      container.deleteIfExists();
    }
  }

  private void azurite() {
    Assumptions.assumeTrue(store != null, "AZURITE_BLOB_ENDPOINT is not set");
  }

  @Override
  protected AppendBlobStore store() {
    azurite();
    return store;
  }

  /** A client pinned to a service version before 2022-11-02: blocks of at most 4 MiB. */
  @Override
  protected AppendBlobStore storeOfSmallBlocks() {
    azurite();
    return new AzureAppendBlobStore(new BlobContainerClientBuilder()
        .endpoint(container.getBlobContainerUrl())
        .credential(CREDENTIAL)
        .serviceVersion(BlobServiceVersion.V2021_12_02)
        .buildClient());
  }

  @Test
  void refusesGeoSecondaryEndpoints() {
    assertThatThrownBy(
            () -> AzureAppendBlobStore.create(
                "https://acct-secondary.blob.core.windows.net/logs", CREDENTIAL))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("secondary");
    assertThatThrownBy(
            () -> AzureAppendBlobStore.create(
                "http://127.0.0.1:10000/devstoreaccount1-secondary/logs", CREDENTIAL))
        .isInstanceOf(IllegalArgumentException.class);
    BlobContainerClient secondary =
        new BlobContainerClientBuilder()
            .endpoint("https://acct-secondary.blob.core.windows.net/logs")
            .credential(CREDENTIAL)
            .buildClient();
    assertThatThrownBy(() -> new AzureAppendBlobStore(secondary))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> AzureAppendBlobStore.create("https://acct.blob.core.windows.net", CREDENTIAL))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No container");
    AppendBlobStore primary =
        AzureAppendBlobStore.create("https://acct.blob.core.windows.net/logs", CREDENTIAL);
    assertThat(primary.location("seg/0001"))
        .isEqualTo("https://acct.blob.core.windows.net/logs/seg/0001");
    assertThat(primary.maxBlocks()).isEqualTo(50_000);
    assertThat(primary.maxBlockBytes()).isEqualTo(100L << 20);
  }

  /**
   * A {@code 429} (too many requests) is backpressure: {@code Throttled}, which the log sends
   * again after a backoff, not {@code Refused}, which ends the commit as refused. So is a {@code
   * 408} (the request did not arrive whole in time: transient). A {@code 403} is refused. No
   * service: a policy answers every request.
   */
  @Test
  void aTooManyRequestsAnswerIsThrottled() {
    AppendBlobStore busy = answering(429, "TooManyRequests");
    assertThatThrownBy(() -> busy.appendBlock("seg", block('a'), 0, -1))
        .isInstanceOf(Throttled.class);
    assertThatThrownBy(() -> busy.properties("seg")).isInstanceOf(Throttled.class);
    AppendBlobStore timedOut = answering(408, "RequestTimeout");
    assertThatThrownBy(() -> timedOut.appendBlock("seg", block('a'), 0, -1))
        .isInstanceOf(Throttled.class);
    AppendBlobStore forbidden = answering(403, "AuthorizationFailure");
    assertThatThrownBy(() -> forbidden.appendBlock("seg", block('a'), 0, -1))
        .isInstanceOf(Refused.class);
  }

  /** A store whose every request is answered {@code status}, with {@code errorCode}. */
  private static AppendBlobStore answering(int status, String errorCode) {
    HttpPipelinePolicy answer = new HttpPipelinePolicy() {
      @Override
      public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
        return Mono.just(answer(context.getHttpRequest(), status, errorCode));
      }

      @Override
      public HttpResponse processSync(
          HttpPipelineCallContext context, HttpPipelineNextSyncPolicy next) {
        return answer(context.getHttpRequest(), status, errorCode);
      }
    };
    return new AzureAppendBlobStore(
        new BlobContainerClientBuilder().endpoint("https://acct.blob.core.windows.net/logs")
            .credential(CREDENTIAL)
            .retryOptions(new RequestRetryOptions(RetryPolicyType.FIXED, 1, (Duration) null, null, null, null))
            .addPolicy(answer)
            .buildClient());
  }

  private static HttpResponse answer(HttpRequest request, int status, String errorCode) {
    HttpHeaders headers = new HttpHeaders()
        .set(HttpHeaderName.fromString("x-ms-error-code"), errorCode)
        .set(HttpHeaderName.CONTENT_LENGTH, "0");
    return new HttpResponse(request) {
      @Override
      public int getStatusCode() {
        return status;
      }

      @Override
      @SuppressWarnings("deprecation")
      public String getHeaderValue(String name) {
        return headers.getValue(name);
      }

      @Override
      public HttpHeaders getHeaders() {
        return headers;
      }

      @Override
      public Flux<ByteBuffer> getBody() {
        return Flux.empty();
      }

      @Override
      public Mono<byte[]> getBodyAsByteArray() {
        return Mono.just(new byte[0]);
      }

      @Override
      public Mono<String> getBodyAsString() {
        return Mono.just("");
      }

      @Override
      public Mono<String> getBodyAsString(Charset charset) {
        return Mono.just("");
      }
    };
  }

  /** A location names its blob back, its name decoded; another container's names none. */
  @Test
  void aLocationNamesItsBlob() {
    AppendBlobStore primary =
        AzureAppendBlobStore.create("https://acct.blob.core.windows.net/logs", CREDENTIAL);
    String name = "old prefix/seg-00000000000000000001-00000000000000ff";
    assertThat(primary.location(name)).contains("old%20prefix/seg-");
    assertThat(primary.nameAt(primary.location(name))).isEqualTo(name);
    assertThatThrownBy(() -> primary.nameAt("https://acct.blob.core.windows.net/other/seg"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> primary.nameAt("https://acct.blob.core.windows.net/logs/"))
        .isInstanceOf(IllegalArgumentException.class);
    InMemoryAppendBlobStore memory = new InMemoryAppendBlobStore();
    assertThat(memory.nameAt(memory.location(name))).isEqualTo(name);
    assertThatThrownBy(() -> memory.nameAt(primary.location(name)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * A read from a blob's end, as a tail read that finds nothing new is, takes one request (one
   * byte before the end, dropped), not a 416 and a Get Blob Properties; past the end it is empty
   * and says the length.
   */
  @Test
  void aReadFromTheEndIsOneRequest() {
    azurite();
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
    AppendBlobStore counted = new AzureAppendBlobStore(
        new BlobContainerClientBuilder().endpoint(container.getBlobContainerUrl())
            .credential(CREDENTIAL)
            .retryOptions(new RequestRetryOptions(RetryPolicyType.FIXED, 1, (Duration) null, null, null, null))
            .addPolicy(counting)
            .buildClient());
    counted.createAppendBlob("seg", Map.of("k", "v"));
    counted.appendBlock("seg", block('a'), 0, -1);

    requests.set(0);
    Blob end = counted.read("seg", N);
    assertThat(requests).hasValue(1);
    assertThat(end.bytes()).isEmpty();
    assertThat(end.length()).isEqualTo(N);
    assertThat(end.blocks()).isEqualTo(1);
    assertThat(end.metadata()).isEqualTo(Map.of("k", "v"));

    Blob past = counted.read("seg", N + 5);
    assertThat(past.bytes()).isEmpty();
    assertThat(past.length()).isEqualTo(N);
  }

  @Test
  void locationsAreBlobUrls() {
    azurite();
    assertThat(store.location("log/seg-1"))
        .isEqualTo(container.getBlobContainerUrl() + "/log/seg-1");
  }
}
