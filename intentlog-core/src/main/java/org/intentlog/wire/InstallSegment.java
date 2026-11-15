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
package org.intentlog.wire;

/**
 * A segmented log's Install of segment {@code ordinal} (log.proto {@code InstallSegment};
 * docs/LOG_SEGMENTS.md): its name in the log's segment store (or a {@code location} in its
 * place; empty for none) and its record limit, which the Install sets before the segment takes
 * any record.
 */
public record InstallSegment(long ordinal, String name, String location, long limit) {

  public InstallSegment {
    if (ordinal < 0 || limit < 1) {
      throw new IllegalArgumentException("No segment " + ordinal + " with limit " + limit);
    }
    name = name == null ? "" : name;
    location = location == null ? "" : location;
  }
}
