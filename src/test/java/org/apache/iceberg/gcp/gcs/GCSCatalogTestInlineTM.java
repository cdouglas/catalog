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
package org.apache.iceberg.gcp.gcs;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * GCS + CAS (forced by {@code GCSFileIO.supportsAppend()=false}) + inline TableMetadata. Manifest
 * lists still written as separate snap-*.avro files.
 *
 * <p>Same disable-list as {@link
 * org.apache.iceberg.aws.s3.TestS3CatalogCASInlineTM}: the upstream {@code CatalogTests} cases
 * that count metadata.json files on disk or feed metadata locations through register APIs cannot
 * pass against an inline catalog.
 */
@ExtendWith(GCSCatalogTest.SuccessCleanupExtension.class)
public class GCSCatalogTestInlineTM extends GCSCatalogTest {
  @Override
  protected boolean inlineTM() {
    return true;
  }

  // assertPreviousMetadataFileCount inspects ops.current().previousFiles() -- a
  // TableMetadata field, not a filesystem check. Inline TM populates this with
  // synthetic "inline://#<hash>" entries (one per prior catalog version), so the
  // four testReplaceTransaction-family cases run cleanly here as long as
  // InlineDeltaCodec's replay produces a non-null base.metadataFileLocation --
  // see commit 804465b. Re-enabled.

  // testMetadataFileLocationsRemovalAfterCommit calls
  // ReachableFileUtil.metadataFileLocations and exercises bounded retention.
  // For inline tables there are no on-disk metadata.json files; the equivalent
  // semantic (METADATA_PREVIOUS_VERSIONS_MAX bounding previousFiles) is upstream
  // logic. Reachability resolution against synthetic inline:// URIs is the only
  // mismatch; defer until the InlineCompat fork swaps the assertion's accessor.
  @Override
  @Test
  @Disabled(
      "ReachableFileUtil.metadataFileLocations resolves inline:// URIs as files; "
          + "needs InlineCompat fork to assert against TableMetadata.previousFiles() instead")
  public void testMetadataFileLocationsRemovalAfterCommit() {}

  @Override
  @Test
  @Disabled("registerTable expects an on-disk metadata.json; inline TM exposes inline:// only")
  public void testRegisterTable() {}

  @Override
  @Test
  @Disabled("registerTable expects an on-disk metadata.json; inline TM exposes inline:// only")
  public void testRegisterExistingTable() {}
}
