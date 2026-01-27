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

/**
 * Handles mutation and verification logic for log catalog actions.
 *
 * <p>This class encapsulates the logic for:
 *
 * <ul>
 *   <li>Verifying that an action can be applied (optimistic concurrency check)
 *   <li>Applying an action to modify catalog state
 * </ul>
 *
 * <p>The mutator works with a {@link LogCatalogFormat.Mut} instance to apply changes and verify
 * constraints.
 */
public class LogMutator {

  private final LogCatalogFormat.Mut mut;

  /**
   * Creates a new mutator for the given mutable catalog state.
   *
   * @param mut the mutable catalog state
   */
  public LogMutator(LogCatalogFormat.Mut mut) {
    this.mut = mut;
  }

  /**
   * Verifies that an action can be applied without conflicts.
   *
   * @param action the action to verify
   * @return true if the action can be applied, false if there's a conflict
   */
  public boolean verify(LogCatalogFormat.LogAction action) {
    return action.verify(mut);
  }

  /**
   * Applies an action to modify the catalog state.
   *
   * @param action the action to apply
   */
  public void apply(LogCatalogFormat.LogAction action) {
    action.apply(mut);
  }

  /**
   * Verifies and applies a transaction.
   *
   * @param txn the transaction to process
   * @return true if the transaction was verified and applied, false if verification failed
   */
  public boolean verifyAndApply(LogCatalogFormat.LogAction.Transaction txn) {
    if (txn.verify(mut)) {
      txn.apply(mut);
      return true;
    }
    return false;
  }
}
