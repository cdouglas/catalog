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

import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/** Segments named by their Blob URLs, in the containers those name ({@link BlobUrlSegments}). */
class TestBlobUrlSegments {

  private final Map<String, InMemoryAppendBlobStore> containers = new TreeMap<>();
  private final BlobUrlSegments store = new BlobUrlSegments(url -> {
    if (url.contains("//other.")) {
      throw new IllegalArgumentException("no credentials for " + url);
    }
    return containers.computeIfAbsent(url, u -> new InMemoryAppendBlobStore());
  });

  @Test
  void aUrlIsItsContainersAndANameInIt() {
    assertThat(BlobUrlSegments.blob("https://ACCT.blob.core.windows.net/Logs/a%20b/c+d?sv=1"))
        .isEqualTo(new BlobUrlSegments.Blob("https://acct.blob.core.windows.net/logs", "a b/c+d"));
    assertThat(BlobUrlSegments.canonical("https://acct.blob.core.windows.net/logs/a%20b/c+d"))
        .isEqualTo("https://acct.blob.core.windows.net/logs/a%20b/c%2Bd");
    assertThat(BlobUrlSegments.blob("http://127.0.0.1:10000/devstoreaccount1/dev/x/y"))
        .isEqualTo(new BlobUrlSegments.Blob("http://127.0.0.1:10000/devstoreaccount1/dev", "x/y"));
    String canonical = BlobUrlSegments.canonical("https://acct.blob.core.windows.net/l/p q/~x");
    assertThat(BlobUrlSegments.canonical(canonical)).isEqualTo(canonical);
    assertThatThrownBy(() -> BlobUrlSegments.blob("https://acct.blob.core.windows.net/logs"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * Each segment is in the container its URL names, and a listing names what it finds by URL, under
   * the prefix it was given.
   */
  @Test
  void segmentsAreInTheContainersTheirUrlsName() {
    String a = "https://acct.blob.core.windows.net/one/my%20log/id/seg-1";
    String b = "https://acct.blob.core.windows.net/two/log/id/seg-1";
    assertThat(store.create(a)).isTrue();
    assertThat(store.create(b)).isTrue();
    store.append(a, new byte[] {1});
    assertThat(containers).containsOnlyKeys("https://acct.blob.core.windows.net/one",
        "https://acct.blob.core.windows.net/two");
    assertThat(containers.get("https://acct.blob.core.windows.net/one").names())
        .containsExactly("my log/id/seg-1");
    assertThat(store.read(a, 0).bytes()).containsExactly(1);
    assertThat(store.list("https://acct.blob.core.windows.net/one/my%20log/id/seg-"))
        .containsExactly(a);
    assertThat(store.nameAt(store.location(a))).isEqualTo(a);
    store.delete(a);
    assertThat(store.read(a, 0)).isNull();
    assertThatThrownBy(() -> store.read("https://other.blob.core.windows.net/c/x", 0))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("no credentials");
  }
}
