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

import java.util.List;
import java.util.Optional;

/** A read of a log: its latest checkpoint and the records after it, in log order. */
public interface LogView {

  /** The tail the read ended at. */
  LogTail tail();

  /** The checkpoint (a log.proto LogCheckpoint); empty for a log that has none. */
  Optional<byte[]> checkpoint();

  /** The records after the checkpoint (log.proto LogRecords), in log order. */
  List<byte[]> records();
}
