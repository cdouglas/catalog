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

/**
 * Environment-variable contract for cloud integration tests. Mirrors the pattern in
 * {@code iceberg/aws/.../AwsIntegTestUtil}: one place for variable names, one place
 * for "required" reads, one place to point readers when something is missing.
 *
 * <p>See {@code docs/INTEGRATION_TESTING.md} for the full contract and example values.
 *
 * <p>The {@code -Preal-cloud} Maven profile sets the {@code require.real.cloud} system
 * property; {@link CloudMode#assumeRealCloud(String)} reads it to decide whether a
 * missing emulator feature is a skip (default) or a hard fail (real-cloud profile).
 */
public final class IntegTestEnv {

  // AWS credentials — standard AWS SDK env vars. Presence of AWS_ACCESS_KEY_ID gates
  // real-cloud mode for the S3 suite.
  public static final String AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID";
  public static final String AWS_REGION = "AWS_REGION";
  // Test-specific. The bucket must exist; tests do not create it.
  public static final String AWS_TEST_BUCKET = "AWS_TEST_BUCKET";

  // GCP credentials — standard env var (path to service account JSON). Presence gates
  // real-cloud mode for the GCS suite.
  public static final String GOOGLE_APPLICATION_CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS";
  public static final String GCS_TEST_BUCKET = "GCS_TEST_BUCKET";

  // Azure SAS credentials — used because SAS auth is significantly faster than the
  // default credential chain for this workload. Presence of AZURE_STORAGE_ACCOUNT
  // gates real-cloud mode for the ADLS suite.
  public static final String AZURE_STORAGE_ACCOUNT = "AZURE_STORAGE_ACCOUNT";
  public static final String AZURE_STORAGE_SAS_TOKEN = "AZURE_STORAGE_SAS_TOKEN";
  public static final String AZURE_TEST_CONTAINER = "AZURE_TEST_CONTAINER";

  // System property set by the -Preal-cloud Maven profile.
  public static final String REQUIRE_REAL_CLOUD_PROP = "require.real.cloud";

  private IntegTestEnv() {}

  /** True when {@code -Preal-cloud} is active. Emulator fallbacks must fail loudly. */
  public static boolean requireRealCloud() {
    return Boolean.getBoolean(REQUIRE_REAL_CLOUD_PROP);
  }

  /** Whether the env var is set to a non-blank value. */
  public static boolean isSet(String var) {
    String v = System.getenv(var);
    return v != null && !v.isBlank();
  }

  /**
   * Read the env var; throw with a doc-link message if missing or blank. Use in
   * {@code @BeforeAll} when a code path requires the value (e.g., real-S3 mode is
   * selected and we need the bucket name to proceed).
   */
  public static String require(String var) {
    String v = System.getenv(var);
    if (v == null || v.isBlank()) {
      throw new IllegalStateException(
          "Required environment variable "
              + var
              + " is not set. See docs/INTEGRATION_TESTING.md for the integration-test "
              + "env-var contract.");
    }
    return v;
  }
}
