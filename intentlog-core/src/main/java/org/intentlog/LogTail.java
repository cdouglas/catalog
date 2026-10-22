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

/**
 * The end of a log as a read or probe saw it: the precondition a conditional write names. Opaque
 * to the runtime but for what it asks here.
 */
public interface LogTail {

  /** Whether the log exists (a log that does not is created by a {@link SharedLog#replace}). */
  boolean exists();

  /** The size of the log in bytes, checkpoint included: what the size knob compares. */
  long bytes();

  /**
   * Whether this tail, a later one, continues {@code earlier}: records were appended after it. If
   * not, the log was replaced (compacted) since.
   */
  boolean continues(LogTail earlier);
}
