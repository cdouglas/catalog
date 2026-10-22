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
import org.apache.iceberg.catalog.CatalogTransactionTests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;

/**
 * Runs Iceberg's {@link CatalogTransactionTests} against {@link FileIOCatalog} in every {@link
 * LocalCatalogMode}, over {@link InMemoryAtomicFileIO}. Companion to {@link TestLocalCatalog}.
 */
public class TestLocalCatalogTransaction {

  abstract static class Suite extends CatalogTransactionTests<FileIOCatalog> {
    private InMemoryAtomicFileIO io;
    private FileIOCatalog catalog;

    abstract LocalCatalogMode mode();

    @BeforeEach
    public void before() {
      io = new InMemoryAtomicFileIO();
      catalog = mode().catalog(io, "mem:///warehouse-" + UUID.randomUUID(), "test", Map.of());
    }

    @AfterEach
    public void checkCommitPolicy() {
      mode().assertCommitPolicy(io);
      CheckpointRoundTrip.assertEveryWrite(io);
    }

    @Override
    protected boolean conflictsOnTableVersions() {
      // a transaction conflicts on the whole version of each table it writes (errata D12)
      return true;
    }

    @Override
    protected FileIOCatalog catalog() {
      return catalog;
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
