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

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Map;
import org.intentlog.azure.AppendBlobStore.Blob;
import org.intentlog.azure.AppendBlobStore.Entry;
import org.intentlog.azure.AppendBlobStore.Full;
import org.intentlog.azure.AppendBlobStore.Landed;
import org.intentlog.azure.AppendBlobStore.NotFound;
import org.intentlog.azure.AppendBlobStore.PositionMismatch;
import org.intentlog.azure.AppendBlobStore.Props;
import org.intentlog.azure.AppendBlobStore.Refused;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * What every {@link AppendBlobStore} does, as the segmented log relies on it (docs/AZURE_LOG.md,
 * LOG_SEGMENTS.md): run on {@link InMemoryAppendBlobStore} ({@code TestInMemoryAppendBlobStore}),
 * on Azurite ({@code TestAzureAppendBlobStore}) and on a real account ({@code
 * AzureAppendBlobStoreIT}), so the store every segmented-log fuzzer runs on answers as the
 * service does (the 2026-11-17 review's API2). Each test has an empty store of its own.
 */
abstract class AppendBlobStoreContract {

  static final int N = 10;

  /** The store under test, empty, the same through one test; skips the test if it cannot run. */
  protected abstract AppendBlobStore store();

  /**
   * A store like {@link #store} that takes smaller blocks than the service's default (Azure: a
   * client pinned to a service version before 2022-11-02, 4 MiB); null if there is none.
   */
  protected abstract AppendBlobStore storeOfSmallBlocks();

  /**
   * A store like {@link #store} whose Append Blobs hold at most {@code blocks} blocks; null where
   * the limit is the service's (Azure: 50,000, which the real-cloud append canary checks).
   */
  protected AppendBlobStore storeOfFewBlocks(int blocks) {
    return null;
  }

  static byte[] block(char c) {
    byte[] b = new byte[N];
    Arrays.fill(b, (byte) c);
    return b;
  }

  static byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  static String string(byte[] b) {
    return new String(b, StandardCharsets.UTF_8);
  }

  @Test
  void createsAnAppendBlobOnceWithItsMetadata() {
    AppendBlobStore store = store();
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
    AppendBlobStore store = store();
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

  /**
   * A counted append says the blob's committed block count with its block, the block's index,
   * which decides whether a segment's record is past its end (LOG_SEGMENTS.md "Writing"): the
   * count counts every block, counted or not.
   */
  @Test
  void aCountedAppendSaysItsBlocksIndex() {
    AppendBlobStore store = store();
    store.createAppendBlob("seg", Map.of());
    for (int i = 1; i <= 3; i++) {
      Landed landed = store.appendBlockCounted("seg", block((char) ('a' + i)));
      assertThat(landed.offset()).as("block %s", i).isEqualTo((long) (i - 1) * N);
      assertThat(landed.blocks()).as("block %s", i).isEqualTo(i);
    }
    store.appendBlock("seg", block('z'), 0, -1);
    Landed fifth = store.appendBlockCounted("seg", block('y'));
    assertThat(fifth.offset()).isEqualTo(4L * N);
    assertThat(fifth.blocks()).isEqualTo(5);
    assertThatThrownBy(() -> store.appendBlockCounted("missing", block('a')))
        .isInstanceOf(NotFound.class);
  }

  @Test
  void anAppendPastTheSizeConditionIsFullAndLandsNothing() {
    AppendBlobStore store = store();
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

  /**
   * A block larger than the largest one the store takes ({@link AppendBlobStore#maxBlockBytes})
   * is refused ({@code 413 RequestBodyTooLarge}), and lands nothing; one of that size lands.
   */
  @Test
  void aBlockPastTheLargestIsRefusedAndLandsNothing() {
    AppendBlobStore store = storeOfSmallBlocks();
    Assumptions.assumeTrue(store != null, "no store of small blocks");
    int max = Math.toIntExact(store.maxBlockBytes());
    store.createAppendBlob("seg", Map.of());
    assertThatThrownBy(() -> store.appendBlock("seg", new byte[max + 1], 0, -1))
        .isInstanceOf(Refused.class);
    assertThat(store.properties("seg").length()).isZero();
    assertThat(store.appendBlock("seg", new byte[max], 0, -1)).isZero();
    assertThat(store.properties("seg").blocks()).isEqualTo(1);
  }

  /**
   * An Append Blob holds at most {@link AppendBlobStore#maxBlocks} blocks: an append past them is
   * Full ({@code 409 BlockCountExceedsLimit}), counted or not, and lands nothing.
   */
  @Test
  void anAppendPastTheBlockLimitIsFullAndLandsNothing() {
    AppendBlobStore store = storeOfFewBlocks(3);
    Assumptions.assumeTrue(store != null, "the block limit is the service's");
    assertThat(store.maxBlocks()).isEqualTo(3);
    store.createAppendBlob("seg", Map.of());
    for (int i = 0; i < 3; i++) {
      store.appendBlock("seg", block('a'), 0, -1);
    }
    assertThatThrownBy(() -> store.appendBlock("seg", block('b'), 0, -1))
        .isInstanceOf(Full.class);
    assertThatThrownBy(() -> store.appendBlockCounted("seg", block('b')))
        .isInstanceOf(Full.class);
    assertThat(store.properties("seg").blocks()).isEqualTo(3);
    assertThat(store.properties("seg").length()).isEqualTo(3L * N);
  }

  @Test
  void anAppendAtTheWrongPositionIsAMismatchAndLandsNothing() {
    AppendBlobStore store = store();
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
    AppendBlobStore store = store();
    assertThatThrownBy(() -> store.appendBlock("missing", block('a'), 0, -1))
        .isInstanceOf(NotFound.class);
    assertThatThrownBy(() -> store.appendBlock("missing", block('a'), 4 * N, 0))
        .isInstanceOf(NotFound.class);
    assertThat(store.properties("missing")).isNull();
  }

  @Test
  void anAppendToABlockBlobIsRefused() {
    AppendBlobStore store = store();
    store.put("block", bytes("whole"));
    assertThatThrownBy(() -> store.appendBlock("block", block('a'), 0, -1))
        .isInstanceOf(Refused.class);
  }

  @Test
  void readsFromAnyOffsetToTheEnd() {
    AppendBlobStore store = store();
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
    AppendBlobStore store = store();
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
    AppendBlobStore store = store();
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
    AppendBlobStore store = store();
    assertThat(store.read("missing", 0)).isNull();
    assertThat(store.read("missing", 5)).isNull();
  }

  @Test
  void blockBlobsArePutOnceOrByEtag() {
    AppendBlobStore store = store();
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
    AppendBlobStore store = store();
    store.put("doomed", bytes("x"));
    store.delete("doomed");
    assertThat(store.read("doomed", 0)).isNull();
    store.delete("doomed");
    store.delete("never");
  }

  @Test
  void listsByPrefixWithoutDirectories() {
    AppendBlobStore store = store();
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
}
