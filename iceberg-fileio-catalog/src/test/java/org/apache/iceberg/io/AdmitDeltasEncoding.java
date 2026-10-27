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
 * A test encoding that admits every table action but what Iceberg commits once ({@link
 * TableChange#commitOnce}): the framework then applies any unpinned delta its late-bound fields
 * can be finalized for ({@link Landing#unmergeable}), on whatever version of the table its record
 * lands. Not a conflict encoding (it detects no conflict), but a probe of the merge: where the
 * operation is one whose Iceberg retry succeeds on any table (an append that validates nothing),
 * the merged record must be what the retry commits. Whatever it admits besides (a window's
 * command on a stale handle) must still not be what Iceberg fails.
 */
final class AdmitDeltasEncoding implements ConflictEncoding<Boolean> {

  static final String NAME = "test-admit-deltas";

  static final AdmitDeltasEncoding INSTANCE = new AdmitDeltasEncoding();

  static {
    ConflictEncodings.register(INSTANCE);
  }

  private AdmitDeltasEncoding() {}

  /** Makes the encoding known; a catalog may then name it. */
  static String register() {
    return INSTANCE.name();
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Boolean witness(TableAction action, ReplayView landing) {
    if (action.coarse() || landing.version() == action.baseVersion() || !landing.inline()) {
      return true;
    }
    try {
      return !TableChange.of(action, landing::table).commitOnce;
    } catch (RuntimeException e) {
      return false;
    }
  }

  @Override
  public boolean admits(Boolean witness, ReplayView landing, EncodingState state) {
    return witness;
  }

  @Override
  public void observe(
      TableAction applied, ReplayView before, ReplayView after, EncodingState state) {}

  @Override
  public EncodingState.Codec stateCodec() {
    return EncodingState.Codec.NONE;
  }
}
