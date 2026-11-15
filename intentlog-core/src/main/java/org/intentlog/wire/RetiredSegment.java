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

import java.util.Arrays;

/**
 * A segment a checkpoint covers wholly (log.proto {@code RetiredSegment}; docs/LOG_SEGMENTS.md):
 * kept named in the catalog object until the sweep deletes it, once the retirement TTL has passed
 * since {@code retiredAtMs}. {@code end} is the position after its last record.
 */
public record RetiredSegment(
    long ordinal, String name, String location, byte[] end, long retiredAtMs) {

  public RetiredSegment {
    name = name == null ? "" : name;
    location = location == null ? "" : location;
    end = end == null ? new byte[0] : end;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof RetiredSegment r && r.ordinal == ordinal && r.name.equals(name)
        && r.location.equals(location) && Arrays.equals(r.end, end)
        && r.retiredAtMs == retiredAtMs;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(ordinal) * 31 + name.hashCode();
  }

  @Override
  public String toString() {
    return "RetiredSegment[ordinal=" + ordinal + ", name=" + name + ", location=" + location
        + ", retiredAtMs=" + retiredAtMs + "]";
  }
}
