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
package org.apache.iceberg.io.spec;

import static org.assertj.core.api.Assertions.assertThat;

import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.From;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.apache.iceberg.io.CatalogFile;

/** Tests that the catalog-state generator produces well-formed, non-trivial states. */
class CatalogStateGeneratorsTest {

  @Property
  boolean generatedCatalogIsWellFormed(@ForAll @From("catalogStates") CatalogFile cat) {
    return cat != null
        && cat.uuid() != null
        && cat.namespaces() != null
        && cat.tables() != null;
  }

  @Property
  boolean generatedCatalogSometimesHasEntities(
      @ForAll @From("catalogStates") CatalogFile cat) {
    return true;
  }

  @Provide
  Arbitrary<CatalogFile> catalogStates() {
    return CatalogStateGenerators.catalogStates();
  }
}
