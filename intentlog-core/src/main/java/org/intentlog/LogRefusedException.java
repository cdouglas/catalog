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
 * The store refused a write with an error the same request gets again (a client error: denied
 * credentials or permissions, a malformed request, a feature the store does not have): nothing
 * landed, and sending the write again cannot help until whatever the error names is fixed. A
 * provider throws it only where the error proves that nothing landed: not for an error that may
 * follow an earlier attempt of the write that landed (an SDK's retries end on the last attempt's
 * error). The commit loop rejects the commit at once ({@link
 * CommitRejectedException.Reason#REFUSED}), unless an earlier write of it may have landed.
 */
public class LogRefusedException extends LogNotSentException {
  public LogRefusedException(String message, Throwable cause) {
    super(message, cause);
  }
}
