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
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** Opening the catalog's Azure log from its properties ({@link AzureLogs}). */
class TestAzureLogs {

  /** The container is the location's, whatever the account's name holds. */
  @Test
  void theContainerIsTheOneTheLocationNames() {
    assertThat(AzureLogs.containerUrl(
            "https://logsprod.blob.core.windows.net/logs/warehouse/catalog.log"))
        .isEqualTo("https://logsprod.blob.core.windows.net/logs");
    assertThat(AzureLogs.containerUrl(
            "https://acct.blob.core.windows.net/acct/catalog.log"))
        .isEqualTo("https://acct.blob.core.windows.net/acct");
    assertThat(AzureLogs.containerUrl("http://127.0.0.1:10000/devstoreaccount1/dev/catalog.log"))
        .isEqualTo("http://127.0.0.1:10000/devstoreaccount1/dev");
  }

  @Test
  void aLocationNeedsAContainerAndAPath() {
    assertThatThrownBy(() -> AzureLogs.containerUrl("https://acct.blob.core.windows.net/logs"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("no container and path");
  }
}
