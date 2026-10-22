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
 * S3 + CAS-only commit policy + inline TableMetadata + inline manifest lists. Extends
 * {@link TestS3CatalogCASInlineTM} so the inline-TM {@code @Disabled} overrides flow through
 * (every test that inline-TM has to skip is also incompatible with inline-ML).
 */
@ExtendWith(TestS3Catalog.SuccessCleanupExtension.class)
public class TestS3CatalogCASInlineML extends TestS3CatalogCASInlineTM {
  @Override
  protected boolean inlineML() {
    return true;
  }
}
