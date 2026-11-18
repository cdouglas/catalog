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

import org.apache.iceberg.exceptions.CleanableFailure;

/**
 * A commit whose record is larger than the catalog's log stores in one append, on a log that cannot
 * replace itself (the segmented log on Azure Append Blobs; a shared log with a record limit): it
 * did not take effect, nothing was sent, and no retry of the same commit can land (the commit
 * loop's {@code TOO_LARGE}, docs/LOG.md "The commit loop"). Iceberg retries only a {@link
 * org.apache.iceberg.exceptions.CommitFailedException}, so it does not retry this one, and cleans
 * up what the commit wrote, as it does after any {@link CleanableFailure}.
 */
public class RecordTooLargeException extends RuntimeException implements CleanableFailure {

  public RecordTooLargeException(Throwable cause, String message) {
    super(message, cause);
  }
}
