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
 * Storage pushed back on a write: backpressure (a 429, a per-object update rate) or a transient
 * server error (a 5xx). The write may have landed even so: a 5xx can follow a write the service
 * applied, and an SDK's own retries report only their last attempt. The runtime sends the same
 * write again after a backoff, and counts it as a write that may have landed.
 *
 * <p>A provider throws it only for a write that lands at most once in effect, which is what makes
 * sending it again safe: a conditional write, whose sends all name one tail, so at most one lands
 * (P4); a blind append of a record replay applies once (an intention, by its id; a GC record or a
 * PageOut, which applied twice changes nothing); or a write whose repeat leaves only garbage (a
 * segment created under a fresh name, which the sweep deletes if no Install names it). {@code
 * AtomicFileLog} throws it for a conditional write the FileIO reports as {@code
 * StorageThrottleException}; {@code SegmentedLog} for an append to a segment and the creation of a
 * segment or of the log, and for its catalog object's writes as {@code AtomicFileLog} does; {@code
 * InMemorySharedLog} for any write its hooks throttle. docs/LOG.md "Failures" lists the stores'
 * errors behind each.
 */
public class LogThrottledException extends RuntimeException {
  public LogThrottledException(String message, Throwable cause) {
    super(message, cause);
  }
}
