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

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import org.intentlog.azure.AppendBlobStore.Blob;
import org.intentlog.azure.AppendBlobStore.Entry;
import org.intentlog.azure.AppendBlobStore.Full;
import org.intentlog.azure.AppendBlobStore.NotFound;
import org.intentlog.azure.AppendBlobStore.PositionMismatch;
import org.intentlog.azure.AppendBlobStore.Props;
import org.intentlog.azure.AppendBlobStore.Refused;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * {@link AzureAppendBlobStore} against Azurite, in a fresh container per test. It runs when
 * {@code AZURITE_BLOB_ENDPOINT} names Azurite's blob endpoint with its account (for example
 * {@code http://127.0.0.1:10000/devstoreaccount1}), and is skipped otherwise; the endpoint checks
 * run either way.
 */
class TestAzureAppendBlobStore {

  private static final String ENDPOINT = System.getenv("AZURITE_BLOB_ENDPOINT");
  private static final String ACCOUNT = "devstoreaccount1";
  /** Azurite's well-known account key. */
  private static final String KEY =
      "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
  private static final StorageSharedKeyCredential CREDENTIAL =
      new StorageSharedKeyCredential(ACCOUNT, KEY);
  private static final int N = 10;

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

  private static byte[] block(char c) {
    byte[] b = new byte[N];
    Arrays.fill(b, (byte) c);
    return b;
  }

  private static byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static String string(byte[] b) {
    return new String(b, StandardCharsets.UTF_8);
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

  @Test
  void createsAnAppendBlobOnceWithItsMetadata() {
    azurite();
    Map<String, String> metadata = Map.of("generation", "7", "kind", "segment");
    assertThat(store.createAppendBlob("seg/1", metadata)).isTrue();
    assertThat(store.createAppendBlob("seg/1", Map.of("generation", "8"))).isFalse();
    Props props = store.properties("seg/1");
    assertThat(props.length()).isZero();
    assertThat(props.blocks()).isZero();
    assertThat(props.metadata()).isEqualTo(metadata);
    assertThat(props.lastModified())
        .isBetween(Instant.now().minus(5, ChronoUnit.MINUTES), Instant.now().plusSeconds(300));
    assertThat(store.read("seg/1", 0).metadata()).isEqualTo(metadata);
  }

  @Test
  void appendsLandWhereTheyAreToldTheyLanded() {
    azurite();
    store.createAppendBlob("seg", Map.of());
    assertThat(store.appendBlock("seg", block('a'), 0, -1)).isEqualTo(0);
    assertThat(store.appendBlock("seg", block('b'), 0, N)).isEqualTo(N);
    assertThat(store.appendBlock("seg", block('c'), 3 * N, -1)).isEqualTo(2 * N);
    Props props = store.properties("seg");
    assertThat(props.length()).isEqualTo(3 * N);
    assertThat(props.blocks()).isEqualTo(3);
    assertThat(string(store.read("seg", 0).bytes()))
        .isEqualTo("a".repeat(N) + "b".repeat(N) + "c".repeat(N));
  }

  @Test
  void anAppendPastTheSizeConditionIsFullAndLandsNothing() {
    azurite();
    store.createAppendBlob("seg", Map.of());
    store.appendBlock("seg", block('a'), 0, -1);
    assertThatThrownBy(() -> store.appendBlock("seg", block('b'), 2 * N - 1, -1))
        .isInstanceOf(Full.class);
    assertThatThrownBy(() -> store.appendBlock("seg", block('b'), 2 * N - 1, N))
        .isInstanceOf(Full.class);
    Props props = store.properties("seg");
    assertThat(props.length()).isEqualTo(N);
    assertThat(props.blocks()).isEqualTo(1);
    // exactly at the limit lands
    assertThat(store.appendBlock("seg", block('b'), 2 * N, -1)).isEqualTo(N);
  }

  @Test
  void anAppendAtTheWrongPositionIsAMismatchAndLandsNothing() {
    azurite();
    store.createAppendBlob("seg", Map.of());
    store.appendBlock("seg", block('a'), 0, -1);
    assertThatThrownBy(() -> store.appendBlock("seg", block('b'), 0, 0))
        .isInstanceOf(PositionMismatch.class);
    assertThatThrownBy(() -> store.appendBlock("seg", block('b'), 0, 2 * N))
        .isInstanceOf(PositionMismatch.class);
    assertThat(store.properties("seg").length()).isEqualTo(N);
  }

  @Test
  void anAppendToAMissingBlobIsNotFound() {
    azurite();
    assertThatThrownBy(() -> store.appendBlock("missing", block('a'), 0, -1))
        .isInstanceOf(NotFound.class);
    assertThatThrownBy(() -> store.appendBlock("missing", block('a'), 4 * N, 0))
        .isInstanceOf(NotFound.class);
    assertThat(store.properties("missing")).isNull();
  }

  @Test
  void anAppendToABlockBlobIsRefused() {
    azurite();
    store.put("block", bytes("whole"));
    assertThatThrownBy(() -> store.appendBlock("block", block('a'), 0, -1))
        .isInstanceOf(Refused.class);
  }

  @Test
  void readsFromAnyOffsetToTheEnd() {
    azurite();
    store.createAppendBlob("seg", Map.of("k", "v"));
    store.appendBlock("seg", block('a'), 0, -1);
    store.appendBlock("seg", block('b'), 0, -1);
    String whole = "a".repeat(N) + "b".repeat(N);
    String etag = store.read("seg", 0).etag();
    assertThat(etag).isNotBlank();

    Blob all = store.read("seg", 0);
    assertThat(string(all.bytes())).isEqualTo(whole);
    assertThat(all.length()).isEqualTo(2 * N);
    assertThat(all.blocks()).isEqualTo(2);
    assertThat(all.metadata()).isEqualTo(Map.of("k", "v"));

    for (int from : new int[] {1, N - 1, N, N + 3, 2 * N - 1}) {
      Blob tail = store.read("seg", from);
      assertThat(string(tail.bytes())).as("from %s", from).isEqualTo(whole.substring(from));
      assertThat(tail.length()).as("from %s", from).isEqualTo(2 * N);
      assertThat(tail.blocks()).as("from %s", from).isEqualTo(2);
      assertThat(tail.metadata()).as("from %s", from).isEqualTo(Map.of("k", "v"));
      assertThat(tail.etag()).as("from %s", from).isEqualTo(etag);
    }
  }

  @Test
  void aReadFromTheEndIsEmptyAndSaysTheLength() {
    azurite();
    store.createAppendBlob("seg", Map.of("k", "v"));
    store.appendBlock("seg", block('a'), 0, -1);
    String etag = store.read("seg", 0).etag();

    Blob end = store.read("seg", N);
    assertThat(end.bytes()).isEmpty();
    assertThat(end.length()).isEqualTo(N);
    assertThat(end.blocks()).isEqualTo(1);
    assertThat(end.metadata()).isEqualTo(Map.of("k", "v"));
    assertThat(end.etag()).isEqualTo(etag);

    Blob past = store.read("seg", 3 * N);
    assertThat(past.bytes()).isEmpty();
    assertThat(past.length()).isEqualTo(N);
  }

  @Test
  void readsOfAnEmptyAppendBlob() {
    azurite();
    store.createAppendBlob("empty", Map.of("k", "v"));
    for (long from : new long[] {0, 1}) {
      Blob empty = store.read("empty", from);
      assertThat(empty.bytes()).as("from %s", from).isEmpty();
      assertThat(empty.length()).as("from %s", from).isZero();
      assertThat(empty.blocks()).as("from %s", from).isZero();
      assertThat(empty.metadata()).as("from %s", from).isEqualTo(Map.of("k", "v"));
      assertThat(empty.etag()).as("from %s", from).isNotBlank();
    }
  }

  @Test
  void aReadOfAMissingBlobIsNull() {
    azurite();
    assertThat(store.read("missing", 0)).isNull();
    assertThat(store.read("missing", 5)).isNull();
  }

  @Test
  void blockBlobsArePutOnceOrByEtag() {
    azurite();
    assertThat(store.putIfAbsent("head", bytes("one"))).isTrue();
    assertThat(store.putIfAbsent("head", bytes("two"))).isFalse();
    Blob head = store.read("head", 0);
    assertThat(string(head.bytes())).isEqualTo("one");
    assertThat(head.blocks()).isZero();

    String next = store.putIfMatch("head", bytes("three"), head.etag());
    assertThat(next).isNotNull().isNotEqualTo(head.etag());
    assertThat(store.putIfMatch("head", bytes("stale"), head.etag())).isNull();
    Blob read = store.read("head", 0);
    assertThat(string(read.bytes())).isEqualTo("three");
    assertThat(read.etag()).isEqualTo(next);
    assertThat(store.putIfMatch("missing", bytes("x"), next)).isNull();
    assertThat(store.read("missing", 0)).isNull();

    store.put("head", bytes("four"));
    store.put("other", bytes("five"));
    assertThat(string(store.read("head", 0).bytes())).isEqualTo("four");
    assertThat(string(store.read("other", 2).bytes())).isEqualTo("ve");
    assertThat(store.read("other", 2).length()).isEqualTo(4);
  }

  @Test
  void deletesTwice() {
    azurite();
    store.put("doomed", bytes("x"));
    store.delete("doomed");
    assertThat(store.read("doomed", 0)).isNull();
    store.delete("doomed");
    store.delete("never");
  }

  @Test
  void listsByPrefixWithoutDirectories() {
    azurite();
    store.createAppendBlob("log/seg-1", Map.of());
    store.createAppendBlob("log/seg-2", Map.of());
    store.put("log/head", bytes("h"));
    store.put("other/x", bytes("x"));
    // a directory placeholder, as a hierarchical namespace lists one
    store.createAppendBlob("log/dir", Map.of("hdi_isfolder", "true"));

    assertThat(store.list("log/"))
        .extracting(Entry::name)
        .containsExactlyInAnyOrder("log/seg-1", "log/seg-2", "log/head");
    assertThat(store.list("log/seg"))
        .extracting(Entry::name)
        .containsExactlyInAnyOrder("log/seg-1", "log/seg-2");
    assertThat(store.list("nothing/")).isEmpty();
    assertThat(store.list("log/")).allSatisfy(e -> assertThat(e.lastModified()).isNotNull());
  }

  @Test
  void locationsAreBlobUrls() {
    azurite();
    assertThat(store.location("log/seg-1"))
        .isEqualTo(container.getBlobContainerUrl() + "/log/seg-1");
  }
}
