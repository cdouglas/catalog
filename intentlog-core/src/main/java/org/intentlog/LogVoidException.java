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
 * An append that landed where the log will never hold it: past the end of a segmented log's
 * segment (docs/LOG_SEGMENTS.md D4). Unlike a failed condition, this is proof the write is in no
 * view of the log, now or later: a lost race that leaves nothing to resolve.
 */
public class LogVoidException extends LogConflictException {
  public LogVoidException(String message, Throwable cause) {
    super(message, cause);
  }
}
