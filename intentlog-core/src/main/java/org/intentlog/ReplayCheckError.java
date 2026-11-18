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
 * A self-check that failed: a fold that differs from a replay of the whole view, a native replay
 * that differs from Iceberg's builder, an encoding that does not round-trip. The checks run only
 * when a test asks for them ({@code -Dintentlog.replay.check}, {@code
 * -Dfileio.catalog.replay.check}, {@code -Dintentlog.fileio.tail.check}), and a failed one is a bug
 * in the code checked, never an outcome of the operation that ran into it. It is an {@link Error}
 * so that no {@code catch (RuntimeException)} on the way out (the commit loop's, which maps what
 * it cannot classify to an unknown outcome, or a caller's) turns it into one.
 */
public final class ReplayCheckError extends AssertionError {
  private static final long serialVersionUID = 1L;

  public ReplayCheckError(String message) {
    super(message);
  }

  public ReplayCheckError(String message, Throwable cause) {
    super(message, cause);
  }
}
