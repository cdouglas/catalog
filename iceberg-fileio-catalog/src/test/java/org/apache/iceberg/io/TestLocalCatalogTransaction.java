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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.catalog.CatalogTransaction;
import org.apache.iceberg.catalog.CatalogTransactionTests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Runs Iceberg's {@link CatalogTransactionTests} against {@link LogCatalog} in every {@link
 * LocalCatalogMode}, over {@link InMemoryAtomicFileIO} (with the catalog's log in an {@link
 * org.intentlog.memory.InMemorySharedLog} in the {@code MEMLOG} modes). Companion to {@link
 * TestLocalCatalog}.
 */
public class TestLocalCatalogTransaction {

  abstract static class Suite extends CatalogTransactionTests<LogCatalog> {
    private InMemoryAtomicFileIO io;
    private LogCatalog catalog;

    abstract LocalCatalogMode mode();

    /** As {@link TestLocalCatalog.Suite#logProperties}. */
    Map<String, String> logProperties(String warehouse, String name) {
      return Map.of();
    }

    @BeforeEach
    public void before() {
      io = new InMemoryAtomicFileIO();
      String warehouse = "mem:///warehouse-" + UUID.randomUUID();
      catalog = mode().catalog(io, warehouse, "test", logProperties(warehouse, "test"));
    }

    @AfterEach
    public void checkCommitPolicy() {
      mode().assertCommitPolicy(io);
      CheckpointRoundTrip.assertEveryWrite(io);
    }

    @Override
    protected boolean conflictsOnTableVersions() {
      // an inline table's snapshot changes are judged where they land; a pointer table's update
      // applies only on the version the transaction first read (errata D12)
      return !mode().inlineTM;
    }

    @Override
    protected boolean conflictsOnTableVersionsForCommitOnceUpdates() {
      // a ref, schema or partition-spec change applies only on the version first read (errata D12)
      return true;
    }

    /**
     * A transaction's snapshot changes to an inline table are judged where its record lands
     * (errata D12): a snapshot isolation transaction's appends to two inline-ML tables, one of
     * which another writer appended to first, commit together, as a retry of them would. Iceberg's
     * REST server fails the transaction on the raced table's requirements. Here it fails where the
     * append cannot move onto the branch's new head: an inline-TM table's manifest list is a file
     * built on the old one (the encoding's {@code ref: advanced}), a pointer table's update applies
     * only on the version the transaction first read, and a serializable transaction read the
     * branch that moved. Then neither table changes.
     */
    @ParameterizedTest
    @EnumSource(CatalogTransaction.IsolationLevel.class)
    public void aRacedAppendCommitsWhereARetryWould(CatalogTransaction.IsolationLevel isolation) {
      TableIdentifier raced = TableIdentifier.of("ns", "raced");
      TableIdentifier other = TableIdentifier.of("ns", "other");
      catalog.createNamespace(Namespace.of("ns"));
      catalog.buildTable(raced, SCHEMA).withPartitionSpec(SPEC).create()
          .newFastAppend().appendFile(FILE_A).commit();
      catalog.buildTable(other, SCHEMA).withPartitionSpec(SPEC).create();

      CatalogTransaction txn = catalog.createTransaction(isolation);
      txn.asCatalog().loadTable(raced).newFastAppend().appendFile(FILE_B).commit();
      txn.asCatalog().loadTable(other).newFastAppend().appendFile(FILE_D).commit();
      // another writer appends to the first table, moving its main branch
      catalog.loadTable(raced).newFastAppend().appendFile(FILE_C).commit();

      if (mode().inlineML && isolation == CatalogTransaction.IsolationLevel.SNAPSHOT) {
        txn.commitTransaction();
        assertThat(files(raced))
            .containsExactlyInAnyOrder(FILE_A.location(), FILE_B.location(), FILE_C.location());
        assertThat(files(other)).containsExactly(FILE_D.location());
      } else {
        assertThatThrownBy(txn::commitTransaction)
            .isInstanceOfAny(CommitFailedException.class, ValidationException.class);
        assertThat(files(raced))
            .containsExactlyInAnyOrder(FILE_A.location(), FILE_C.location());
        assertThat(files(other)).isEmpty();
      }
    }

    private Set<String> files(TableIdentifier table) {
      Set<String> files = new TreeSet<>();
      try (CloseableIterable<FileScanTask> tasks =
          catalog.loadTable(table).newScan().planFiles()) {
        tasks.forEach(task -> files.add(task.file().location()));
      } catch (IOException e) {
        throw new java.io.UncheckedIOException(e);
      }
      return files;
    }

    @Override
    protected LogCatalog catalog() {
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

  @Nested
  class SharedLog extends Suite {
    @Override
    LocalCatalogMode mode() {
      return LocalCatalogMode.MEMLOG;
    }
  }

  @Nested
  class SharedLogInlineTM extends Suite {
    @Override
    LocalCatalogMode mode() {
      return LocalCatalogMode.MEMLOG_TM;
    }
  }

  @Nested
  class SharedLogInlineML extends Suite {
    @Override
    LocalCatalogMode mode() {
      return LocalCatalogMode.MEMLOG_ML;
    }
  }

  @Nested
  class AzureLog extends Suite {
    @Override
    LocalCatalogMode mode() {
      return LocalCatalogMode.AZLOG;
    }
  }

  @Nested
  class AzureLogInlineTM extends Suite {
    @Override
    LocalCatalogMode mode() {
      return LocalCatalogMode.AZLOG_TM;
    }
  }

  @Nested
  class AzureLogInlineML extends Suite {
    @Override
    LocalCatalogMode mode() {
      return LocalCatalogMode.AZLOG_ML;
    }
  }
}
