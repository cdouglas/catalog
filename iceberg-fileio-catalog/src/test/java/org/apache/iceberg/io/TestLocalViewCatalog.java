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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewCatalogTests;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Runs Iceberg's {@link ViewCatalogTests} against {@link LogCatalog} in every {@link
 * LocalCatalogMode}, as {@link TestLocalCatalog} runs its {@code CatalogTests}: the view catalog
 * is its own table catalog, so every collision of a table's name and a view's goes through one
 * catalog (docs/VIEWS.md "Testing"). A mode that inlines tables inlines views too, so these run
 * views in both forms. Four tests assume a view's metadata is a file; an inline view's runs a
 * variant that checks the same of its form: its location is the catalog's sentinel, which names its
 * version, and a drop that keeps its metadata ({@code gc.enabled} false) writes it to a file.
 */
public class TestLocalViewCatalog {

  abstract static class Suite extends ViewCatalogTests<LogCatalog> {
    private InMemoryAtomicFileIO io;
    private LogCatalog catalog;

    abstract LocalCatalogMode mode();

    /**
     * Properties of the log of the catalog {@code name} in {@code warehouse}, beyond the mode's:
     * none here.
     */
    Map<String, String> logProperties(String warehouse, String name) {
      return Map.of();
    }

    private String warehouse;

    @BeforeEach
    public void before() {
      io = new InMemoryAtomicFileIO();
      warehouse = "mem:///warehouse-" + UUID.randomUUID();
      catalog = mode().catalog(io, warehouse, "test", properties(Map.of()));
    }

    /**
     * The catalog's properties: its log's, the view properties {@code ViewCatalogTests} expects a
     * catalog to default and to enforce, and {@code extra}.
     */
    private Map<String, String> properties(Map<String, String> extra) {
      Map<String, String> properties = new HashMap<>(logProperties(warehouse, "test"));
      properties.put(CatalogProperties.VIEW_DEFAULT_PREFIX + "key1", "catalog-default-key1");
      properties.put(CatalogProperties.VIEW_DEFAULT_PREFIX + "key2", "catalog-default-key2");
      properties.put(CatalogProperties.VIEW_DEFAULT_PREFIX + "key3", "catalog-default-key3");
      properties.put(CatalogProperties.VIEW_OVERRIDE_PREFIX + "key3", "catalog-override-key3");
      properties.put(CatalogProperties.VIEW_OVERRIDE_PREFIX + "key4", "catalog-override-key4");
      properties.putAll(extra);
      return properties;
    }

    /** A view of {@code identifier} as the suite's tests create it. */
    private View createView(TableIdentifier identifier, Map<String, String> properties) {
      return catalog().buildView(identifier)
          .withSchema(SCHEMA)
          .withDefaultNamespace(identifier.namespace())
          .withDefaultCatalog(catalog().name())
          .withQuery("spark", "select * from ns.tbl")
          .withProperties(properties)
          .create();
    }

    private static ViewMetadata metadata(View view) {
      return ((BaseView) view).operations().current();
    }

    /**
     * An inline view has no metadata file, so no file under {@code write.metadata.path}: the
     * property is kept, and a pointer writer's commit to the view writes its file there.
     */
    @Override
    @Test
    public void createViewWithCustomMetadataLocation() {
      if (!mode().inlineTM) {
        super.createViewWithCustomMetadataLocation();
        return;
      }
      TableIdentifier identifier = TableIdentifier.of("ns", "view");
      catalog().createNamespace(identifier.namespace());
      String customLocation = viewLocation("custom-location");
      View view = catalog().buildView(identifier)
          .withSchema(SCHEMA)
          .withDefaultNamespace(identifier.namespace())
          .withQuery("spark", "select * from ns.tbl")
          .withProperty(ViewProperties.WRITE_METADATA_LOCATION, customLocation)
          .withLocation(viewLocation())
          .create();

      assertThat(view.properties()).containsEntry("write.metadata.path", customLocation);
      assertThat(metadata(view).metadataFileLocation()).startsWith("inline-view://");

      LogCatalog pointer = mode().catalog(io, warehouse, "test",
          properties(Map.of("fileio.catalog.inline", "false",
              "fileio.catalog.inline.manifests", "false")));
      pointer.loadView(identifier).updateProperties().set("k", "v").commit();
      assertThat(metadata(pointer.loadView(identifier)).metadataFileLocation())
          .startsWith(customLocation);
      assertThat(metadata(catalog().loadView(identifier)).properties()).containsEntry("k", "v");
    }

    /**
     * The view registered after its drop is the one the drop wrote to a file, its {@code
     * gc.enabled} being false: an inline view has no file before.
     */
    @Override
    @Test
    public void registerView() {
      if (!mode().inlineTM) {
        super.registerView();
        return;
      }
      TableIdentifier identifier = TableIdentifier.of("ns", "view");
      catalog().createNamespace(identifier.namespace());
      View original = createView(identifier, Map.of(TableProperties.GC_ENABLED, "false"));
      assertThat(metadata(original).metadataFileLocation()).startsWith("inline-view://");

      assertThat(catalog().dropView(identifier)).isTrue();
      String newest = null;
      long newestAt = Long.MIN_VALUE;
      for (FileInfo info : io.listPrefix(original.location() + "/metadata/")) {
        if (info.location().endsWith(".metadata.json") && info.createdAtMillis() >= newestAt) {
          newest = info.location();
          newestAt = info.createdAtMillis();
        }
      }
      assertThat(newest).as("the drop wrote the view to a metadata file").isNotNull();

      View registered = catalog().registerView(identifier, newest);
      assertThat(catalog().viewExists(identifier)).isTrue();
      assertThat(registered.versions()).isEqualTo(original.versions());
      assertThat(registered.history()).isEqualTo(original.history());
      assertThat(registered.properties()).isEqualTo(original.properties());
      assertThat(registered.schema().asStruct()).isEqualTo(SCHEMA.asStruct());
      assertThat(metadata(registered).metadataFileLocation()).isEqualTo(newest);
      assertThat(catalog().dropView(identifier)).isTrue();
      assertThat(io.newInputFile(newest).exists()).as("gc.enabled is false").isTrue();
    }

    @Override
    @Test
    public void renameView() {
      if (!mode().inlineTM) {
        super.renameView();
        return;
      }
      renameInline(TableIdentifier.of("ns", "view"), TableIdentifier.of("ns", "renamedView"));
    }

    @Override
    @Test
    public void renameViewUsingDifferentNamespace() {
      if (!mode().inlineTM) {
        super.renameViewUsingDifferentNamespace();
        return;
      }
      renameInline(
          TableIdentifier.of("ns", "view"), TableIdentifier.of("other_ns", "renamedView"));
    }

    /**
     * A rename keeps an inline view's metadata, but moves its location, which names the view's
     * name and version, as a rename bumps it.
     */
    private void renameInline(TableIdentifier from, TableIdentifier to) {
      catalog().createNamespace(from.namespace());
      if (!to.namespace().equals(from.namespace())) {
        catalog().createNamespace(to.namespace());
      }
      ViewMetadata original = metadata(createView(from, Map.of()));

      catalog().renameView(from, to);

      assertThat(catalog().viewExists(from)).isFalse();
      assertThat(catalog().viewExists(to)).isTrue();
      ViewMetadata renamed = metadata(catalog().loadView(to));
      assertThat(renamed)
          .usingRecursiveComparison()
          .ignoringFieldsOfTypes(Schema.class)
          .ignoringFields("metadataFileLocation")
          .isEqualTo(original);
      assertThat(renamed.metadataFileLocation())
          .isNotEqualTo(original.metadataFileLocation())
          .startsWith("inline-view://" + to + "#");
      assertThat(catalog().dropView(from)).isFalse();
      assertThat(catalog().dropView(to)).isTrue();
      assertThat(catalog().viewExists(to)).isFalse();
    }

    @AfterEach
    public void checkCommitPolicy() {
      mode().assertCommitPolicy(io);
      CheckpointRoundTrip.assertEveryWrite(io);
    }

    @Override
    protected LogCatalog catalog() {
      return catalog;
    }

    @Override
    protected Catalog tableCatalog() {
      return catalog;
    }

    @Override
    protected boolean requiresNamespaceCreate() {
      return true;
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
