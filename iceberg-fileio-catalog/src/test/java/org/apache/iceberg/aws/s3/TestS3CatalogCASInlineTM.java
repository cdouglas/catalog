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

import org.apache.iceberg.io.InlineCompatAssertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * S3 + CAS-only commit policy + inline TableMetadata (manifest lists still written as separate
 * snap-*.avro files). Companion to {@link TestS3CatalogCAS}, which is non-inlined.
 *
 * <p>The upstream {@code CatalogTests} cases that resolve {@code inline://} URIs as filesystem
 * paths get inline-equivalent overrides delegating to {@link InlineCompatAssertions}; the
 * register-from-disk round-trip stays disabled (errata T4).
 */
@ExtendWith(TestS3Catalog.SuccessCleanupExtension.class)
public class TestS3CatalogCASInlineTM extends TestS3CatalogCAS {
  @Override
  protected boolean inlineTM() {
    return true;
  }

  @Override
  @Test
  public void testMetadataFileLocationsRemovalAfterCommit() {
    InlineCompatAssertions.runMetadataFileLocationsRemovalAfterCommit(
        catalog(), TABLE, NS, SCHEMA, requiresNamespaceCreate());
  }

  @Override
  @Test
  @Disabled(
      "drop+registerTable requires register-from-bytes API for inline TM; "
          + "no equivalent on-disk metadata.json exists after drop. See errata T4.")
  public void testRegisterTable() {}
}
