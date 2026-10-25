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

import java.util.BitSet;
import java.util.Set;

/**
 * The smallest separation of a table's concerns: its properties, and everything else. A record
 * that does not touch properties applies on a table whose properties alone changed since its
 * writer read it; any other change is a conflict, as under the Lamport clock.
 *
 * <p>The state is two stamps: the version at which properties last changed, and the version at
 * which anything else did. A change of properties alone moves the first; any other change moves
 * the second (and the first, if it touches properties too).
 */
final class LamportMinusPropertiesEncoding extends ConcernEncoding {

  static final String NAME = "lamport-minus-properties";

  static final LamportMinusPropertiesEncoding INSTANCE = new LamportMinusPropertiesEncoding();

  private static final int PROPERTIES = 0;
  private static final int REST = 1;

  private LamportMinusPropertiesEncoding() {
    super(2, 0);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  Witness reads(int base, TableChange change) {
    if (change.concerns.contains(TableChange.Concern.PROPERTIES)) {
      return null; // a record that touches properties conflicts with any change
    }
    BitSet concerns = new BitSet();
    concerns.set(REST);
    return new Witness(base, false, concerns, Set.of());
  }

  @Override
  void stamp(TableChange change, int version, ConcernStamps stamps) {
    if (change.concerns.contains(TableChange.Concern.PROPERTIES)) {
      stamps.stampConcern(PROPERTIES, version);
    }
    if (!change.onlyProperties()) {
      stamps.stampConcern(REST, version);
    }
  }
}
