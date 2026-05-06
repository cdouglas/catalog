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
 * S3 Express + append-mode commit policy + inline TableMetadata. Companion to
 * {@link TestS3CatalogCASInlineTM} (CAS-only commit policy), exercising the
 * native {@code WriteOffsetBytes} append path with inline-TM bytes.
 *
 * <p>Same {@code @Disabled} list as {@link TestS3CatalogCASInlineTM}: the
 * upstream {@code CatalogTests} cases that resolve {@code inline://} URIs as
 * filesystem paths or round-trip a metadata-location string back through
 * {@code catalog.registerTable} cannot pass against an inline catalog without
 * the {@code InlineCompatCatalogTests} fork (errata T4).
 */
@ExtendWith(TestS3Catalog.SuccessCleanupExtension.class)
public class TestS3CatalogInlineTM extends TestS3Catalog {
  @Override
  protected boolean inlineTM() {
    return true;
  }

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
