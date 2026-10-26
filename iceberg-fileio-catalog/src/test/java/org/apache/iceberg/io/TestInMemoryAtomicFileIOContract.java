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

/**
 * The in-memory store the fuzzers run on, against the atomic-operations contract the cloud stores
 * pass: what it lets a writer do, they must too. Each FileIO a test makes is the same store, as a
 * cloud store's clients share one bucket.
 */
class TestInMemoryAtomicFileIOContract extends SupportsAtomicOperationsContractTest {
  private final InMemoryAtomicFileIO store = new InMemoryAtomicFileIO();

  @Override
  protected SupportsAtomicOperations newFileIO() {
    return store;
  }

  @Override
  protected String randomLocation(String slug) {
    return "mem:///contract/" + randomSlug(slug);
  }
}
