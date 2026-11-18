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
 * A commit whose write the catalog's store refused with an error the same write gets again (a
 * client error: credentials or permissions denied, a malformed request, a body too large, a feature
 * the store does not implement): it did not take effect, and a retry of it is refused too until
 * whatever the error names is fixed (the commit loop's {@code REFUSED}, docs/LOG.md "Failures").
 * Its cause chain holds the store's error. Iceberg retries only a {@link
 * org.apache.iceberg.exceptions.CommitFailedException}, so it does not retry this one, and cleans
 * up what the commit wrote, as it does after any {@link CleanableFailure}.
 */
public class CommitRefusedException extends RuntimeException implements CleanableFailure {

  public CommitRefusedException(Throwable cause, String message) {
    super(message, cause);
  }
}
