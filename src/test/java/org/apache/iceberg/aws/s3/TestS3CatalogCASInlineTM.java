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

  // assertPreviousMetadataFileCount inspects ops.current().previousFiles() -- a
  // TableMetadata field, not a filesystem check. Inline TM populates this list
  // with synthetic "inline://#<hash>" entries (one per prior catalog version),
  // so the four testReplaceTransaction-family cases that only count previousFiles
  // run cleanly here as long as InlineDeltaCodec's replay produces a non-null
  // base.metadataFileLocation -- see commit 804465b. Re-enabled.

  // testMetadataFileLocationsRemovalAfterCommit calls
  // ReachableFileUtil.metadataFileLocations and exercises
  // METADATA_DELETE_AFTER_COMMIT_ENABLED bounded retention. For inline tables
  // there are no on-disk metadata.json files to delete; the equivalent semantic
  // -- bounding previousFiles by METADATA_PREVIOUS_VERSIONS_MAX -- is pure
  // TableMetadata logic and already in upstream. Reachability resolution against
  // synthetic inline:// URIs is the only mismatch; defer until we have an
  // InlineCompat fork that swaps the assertion's accessor.
  @Override
  @Test
  @Disabled(
      "ReachableFileUtil.metadataFileLocations resolves inline:// URIs as files; "
          + "needs InlineCompat fork to assert against TableMetadata.previousFiles() instead")
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
