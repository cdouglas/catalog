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
package org.intentlog.segments;

import java.util.List;

/**
 * Where a segmented log keeps its segments (docs/LOG_SEGMENTS.md): append-only objects that take
 * blind appends, one block per append, read from an offset. Names are paths in one namespace.
 * Azure Append Blobs are the first; {@code InMemorySegmentStore} has their semantics in memory.
 *
 * <p>Failures are the exceptions below, by what they say of a write: {@link Full}, {@link
 * NotFound} and {@link Refused} mean nothing landed; {@link Throttled} that it may have (the same
 * request may be sent again); any other exception leaves a write's outcome unknown and means a
 * read failed. A store sends each request once: a retry inside it could land an append twice and
 * report only the second.
 */
public interface SegmentStore {

  /** Where an append landed: its offset, and the segment's block count with it, its index. */
  record Appended(long offset, long blocks) {}

  /** A segment read from an offset: the bytes from there to its end, its length and blocks. */
  record Segment(byte[] bytes, long length, long blocks) {}

  /** Creates an empty segment, if none of that name exists. Returns false if one does. */
  boolean create(String name);

  /**
   * Appends {@code frame} to the segment {@code name} as one block, with no condition.
   *
   * @throws Full if the segment holds as many blocks or bytes as it may
   * @throws NotFound if the segment does not exist
   */
  Appended append(String name, byte[] frame);

  /**
   * The segment from {@code from} to its end; null if it does not exist. A segment no longer than
   * {@code from} reads as no bytes, with its length.
   */
  Segment read(String name, long from);

  /** Deletes the segment {@code name}, if it exists. */
  void delete(String name);

  /** The segments whose names start with {@code prefix}. */
  List<String> list(String prefix);

  /** Where {@code name} is, as a URL; null if the store cannot say. */
  default String location(String name) {
    return null;
  }

  /** The most blocks a segment holds (Azure: 50,000). */
  long maxBlocks();

  /** The largest block one append takes. */
  long maxBlockBytes();

  /** The most bytes a segment holds. */
  long maxSegmentBytes();

  /** The append did not land: the segment holds as many blocks or bytes as it may. */
  final class Full extends RuntimeException {
    public Full(String message) {
      super(message);
    }
  }

  /** The segment does not exist; nothing landed. */
  final class NotFound extends RuntimeException {
    public NotFound(String message) {
      super(message);
    }
  }

  /** The store refused the request ({@code 400}, {@code 403}, ...); nothing landed. */
  final class Refused extends RuntimeException {
    public Refused(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /** The store is busy or timed out: a write may have landed, and may be sent again. */
  final class Throttled extends RuntimeException {
    public Throttled(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
