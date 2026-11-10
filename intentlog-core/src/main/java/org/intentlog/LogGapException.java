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
 * A read whose records do not start where it says they must: a view whose checkpoint covers one
 * position and whose records follow another, or a view of records alone that does not follow the
 * tail it was read after. The records between are missing (a gap: a log that trimmed past its
 * checkpoint, or lost them) or are another log's (a splice), and replaying the view would hold a
 * state the log never held, so the read fails (P1, docs/TAIL_READS.md "Positions"; errata S4).
 */
public class LogGapException extends IllegalStateException {
  public LogGapException(String message) {
    super(message);
  }
}
