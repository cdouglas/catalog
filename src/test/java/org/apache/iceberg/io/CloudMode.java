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

import org.junit.jupiter.api.Assumptions;

/**
 * Which backing storage an integration test is running against. The cloud providers
 * (S3, ADLS, GCS) each have two modes: a real cloud account when credentials are
 * present in the environment, and a Testcontainers-managed emulator otherwise.
 *
 * <p>Emulators do not implement every feature of the real service. Where a test relies
 * on behaviour the emulator skips or misimplements, call {@link #assumeRealCloud} to
 * skip the test cleanly. Each such skip must be paired with a canary in the matching
 * {@code *EmulatorCanaries} class that asserts the gap is still present — when the
 * canary fails (typically because the pinned image tag was bumped), the workaround
 * can be removed.
 */
public enum CloudMode {
  REAL_S3,
  MINIO,
  REAL_ADLS,
  AZURITE,
  REAL_GCS,
  FAKE_GCS;

  public boolean isEmulator() {
    return this == MINIO || this == AZURITE || this == FAKE_GCS;
  }

  public boolean isRealCloud() {
    return !isEmulator();
  }

  /**
   * Skip the calling test if this mode is an emulator, with a message naming the feature
   * the emulator does not implement. The message points future readers at the canary
   * class that asserts the gap.
   */
  public void assumeRealCloud(String featureName) {
    Assumptions.assumeTrue(
        isRealCloud(),
        () ->
            String.format(
                "skipping on %s: emulator does not implement %s. "
                    + "See *EmulatorCanaries for the canary that guards this skip.",
                this, featureName));
  }
}
