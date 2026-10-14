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

import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Runs Iceberg's {@link CatalogTests} against {@link FileIOCatalog} in every {@link
 * LocalCatalogMode}, over {@link InMemoryAtomicFileIO}. The cloud drivers ({@code TestS3Catalog*},
 * {@code GCSCatalogTest*}, {@code ADLSCatalogTest*}) run the same suite against storage, but only
 * under {@code mvn verify} and, for append mode, only on real S3 Express.
 */
public class TestLocalCatalog {

  abstract static class Suite extends CatalogTests<FileIOCatalog> {
    private InMemoryAtomicFileIO io;
    private String warehouse;
    private FileIOCatalog catalog;

    abstract LocalCatalogMode mode();

    @BeforeEach
    public void before() {
      io = new InMemoryAtomicFileIO();
      warehouse = "mem:///warehouse-" + UUID.randomUUID();
      catalog = mode().catalog(io, warehouse, "test", Map.of());
    }

    @AfterEach
    public void checkCommitPolicy() {
      mode().assertCommitPolicy(io);
    }

    @Override
    protected FileIOCatalog catalog() {
      return catalog;
    }

    @Override
    protected FileIOCatalog initCatalog(String name, Map<String, String> additionalProperties) {
      return mode().catalog(io, warehouse, name, additionalProperties);
    }

    @Override
    protected String baseTableLocation(TableIdentifier identifier) {
      return warehouse + "/" + identifier.namespace() + "/" + identifier.name();
    }

    @Override
    protected boolean requiresNamespaceCreate() {
      return true;
    }

    @Override
    protected boolean supportsNestedNamespaces() {
      return true;
    }

    @Override
    @Test
    public void testRegisterTable() {
      Assumptions.assumeFalse(
          mode().inlineTM, "inline TM has no metadata.json to re-register; errata T4");
      super.testRegisterTable();
    }

    // Inline tables have no metadata.json files to track; use the inline-aware variant.
    @Override
    @Test
    public void testMetadataFileLocationsRemovalAfterCommit() {
      if (mode().inlineTM) {
        InlineCompatAssertions.runMetadataFileLocationsRemovalAfterCommit(
            catalog(), TABLE, NS, SCHEMA, requiresNamespaceCreate());
      } else {
        super.testMetadataFileLocationsRemovalAfterCommit();
      }
    }
  }

  @Nested
  class Append extends Suite {
    @Override
    LocalCatalogMode mode() {
      return LocalCatalogMode.APPEND;
    }
  }

  @Nested
  class AppendInlineTM extends Suite {
    @Override
    LocalCatalogMode mode() {
      return LocalCatalogMode.APPEND_TM;
    }
  }

  @Nested
  class AppendInlineML extends Suite {
    @Override
    LocalCatalogMode mode() {
      return LocalCatalogMode.APPEND_ML;
    }
  }

  @Nested
  class Cas extends Suite {
    @Override
    LocalCatalogMode mode() {
      return LocalCatalogMode.CAS;
    }
  }

  @Nested
  class CasInlineTM extends Suite {
    @Override
    LocalCatalogMode mode() {
      return LocalCatalogMode.CAS_TM;
    }
  }

  @Nested
  class CasInlineML extends Suite {
    @Override
    LocalCatalogMode mode() {
      return LocalCatalogMode.CAS_ML;
    }
  }

  @Nested
  class Compact extends Suite {
    @Override
    LocalCatalogMode mode() {
      return LocalCatalogMode.COMPACT;
    }
  }

  @Nested
  class CompactInlineTM extends Suite {
    @Override
    LocalCatalogMode mode() {
      return LocalCatalogMode.COMPACT_TM;
    }
  }

  @Nested
  class CompactInlineML extends Suite {
    @Override
    LocalCatalogMode mode() {
      return LocalCatalogMode.COMPACT_ML;
    }
  }
}
