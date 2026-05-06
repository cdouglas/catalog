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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * S3 + CAS-only commit policy + inline TableMetadata (manifest lists still written as separate
 * snap-*.avro files). Companion to {@link TestS3CatalogCAS}, which is non-inlined.
 *
 * <p>Several upstream {@code CatalogTests} cases are skipped here because they assert on artefacts
 * that inline-TM intentionally doesn't write (metadata.json files on disk) or pass an
 * {@code inline://} URI through APIs that only accept disk metadata files. These will be
 * revisited; for now we keep the rest of the suite green so the inline path stays exercised.
 */
@ExtendWith(TestS3Catalog.SuccessCleanupExtension.class)
public class TestS3CatalogCASInlineTM extends TestS3CatalogCAS {
  @Override
  protected boolean inlineTM() {
    return true;
  }

  // ============================================================
  // Disabled: assertPreviousMetadataFileCount counts metadata.json files on disk.
  // Inline TM stores metadata in the catalog blob -- nothing to count.
  // ============================================================

  @Override
  @Test
  @Disabled("inline TM has no metadata.json files on disk; assertPreviousMetadataFileCount N/A")
  public void testReplaceTransaction() {}

  @Override
  @Test
  @Disabled("inline TM has no metadata.json files on disk; assertPreviousMetadataFileCount N/A")
  public void testCompleteReplaceTransaction() {}

  @Override
  @Test
  @Disabled("inline TM has no metadata.json files on disk; assertPreviousMetadataFileCount N/A")
  public void testCompleteCreateOrReplaceTransactionReplace() {}

  @Override
  @Test
  @Disabled("inline TM has no metadata.json files on disk; assertPreviousMetadataFileCount N/A")
  public void testCreateOrReplaceReplaceTransactionReplace() {}

  @Override
  @Test
  @Disabled(
      "inline TM has no metadata.json files on disk; "
          + "testMetadataFileLocationsRemovalAfterCommit counts file deletions")
  public void testMetadataFileLocationsRemovalAfterCommit() {}

  // ============================================================
  // Disabled: testRegisterTable feeds a metadata location string back into
  // catalog.registerTable(). Inline TM uses an inline:// pseudo-URI that
  // BaseMetastoreCatalog.registerTable rejects with IllegalArgumentException
  // ("not a valid metadata file"). Register-from-disk is incompatible with
  // an inline-only catalog.
  // ============================================================

  @Override
  @Test
  @Disabled("registerTable expects an on-disk metadata.json; inline TM exposes inline:// only")
  public void testRegisterTable() {}

  @Override
  @Test
  @Disabled("registerTable expects an on-disk metadata.json; inline TM exposes inline:// only")
  public void testRegisterExistingTable() {}
}
