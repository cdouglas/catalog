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
 * The commit did not take effect, and never will: its record was rejected where it landed, or
 * none of its writes can have landed.
 */
public class CommitRejectedException extends RuntimeException {

  /** Why the commit was rejected. */
  public enum Reason {
    /** Its preconditions do not hold on {@link #state()}, the state it lost a race to. */
    CONFLICT,
    /** Its id is at or below the committed set's horizon, so replay rejects its record. */
    BEYOND_HORIZON,
    /** It would create the log, and another writer created it first. */
    CREATE_LOST,
    /** It ran out of attempts, with no write that may have landed. */
    EXHAUSTED,
    /** A write failed before anything was sent, with no earlier write that may have landed. */
    NOT_SENT,
    /**
     * Its record is larger than one append stores ({@link Capabilities#maxRecordBytes}), on a log
     * that cannot replace itself. Nothing was sent.
     */
    TOO_LARGE
  }

  private final Reason reason;
  private final transient Object state;

  public CommitRejectedException(Reason reason, Object state, String message, Throwable cause) {
    super(message, cause);
    this.reason = reason;
    this.state = state;
  }

  public Reason reason() {
    return reason;
  }

  /**
   * For {@link Reason#CONFLICT} and {@link Reason#CREATE_LOST}, the state the commit lost to (the
   * {@link Replica} of a fresh read), for the caller to explain the conflict; else null.
   */
  @SuppressWarnings("unchecked")
  public <S> Replica<S> state() {
    return (Replica<S>) state;
  }
}
