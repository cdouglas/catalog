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
package org.intentlog;

import java.util.UUID;
import org.intentlog.wire.PageRef;

/**
 * Thrown by {@link CommittedSet#contains} for an id in (H, F], between the set's horizon and its
 * floor, that a page may hold: the set holds a reference to the page, not its ids. {@link
 * LogRuntime} fetches the page through the log's {@link PageStore} and answers from it
 * (docs/COMMITTED_PAGING.md "Lookups and the commit loop"). Whatever the page says is final: an
 * id at or below the floor never applies again.
 */
public class PagedOutException extends RuntimeException {
  private final UUID queryId;
  private final transient PageRef page;

  public PagedOutException(UUID queryId, PageRef page) {
    super(String.format("%s is at or below the committed set's floor: page %s holds it if it"
        + " committed", queryId, page.name()));
    this.queryId = queryId;
    this.page = page;
  }

  public UUID queryId() {
    return queryId;
  }

  /** The page that holds the id, if it committed. */
  public PageRef page() {
    return page;
  }
}
