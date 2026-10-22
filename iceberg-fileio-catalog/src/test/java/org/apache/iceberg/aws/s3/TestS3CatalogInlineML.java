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

import org.junit.jupiter.api.extension.ExtendWith;

/**
 * S3 Express + append-mode commit policy + inline TableMetadata + inline manifest lists.
 * Companion to {@link TestS3CatalogCASInlineML} (CAS-only commit policy), exercising the
 * append path with the full inline matrix.
 */
@ExtendWith(TestS3Catalog.SuccessCleanupExtension.class)
public class TestS3CatalogInlineML extends TestS3CatalogInlineTM {
  @Override
  protected boolean inlineML() {
    return true;
  }
}
