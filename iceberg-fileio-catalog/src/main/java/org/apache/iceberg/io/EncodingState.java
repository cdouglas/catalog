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
 * What a conflict encoding keeps of one table, beyond the table itself: evidence that only
 * accumulates, such as the version at which each of the table's concerns last changed. Replay
 * mutates it ({@link ConflictEncoding#observe}); a checkpoint holds it ({@link Codec}). A table
 * the encoding has not observed since it was created has its codec's {@link Codec#empty} state.
 */
interface EncodingState {

  /** A copy that later changes to this one do not reach. */
  EncodingState copy();

  /** The state of an encoding that keeps none. */
  EncodingState NONE =
      new EncodingState() {
        @Override
        public EncodingState copy() {
          return this;
        }

        @Override
        public String toString() {
          return "none";
        }
      };

  /** Encodes and decodes an encoding's state of a table, the bytes a checkpoint holds. */
  interface Codec {
    /** The state of a table the encoding has not observed. */
    EncodingState empty();

    /** The bytes of {@code state}; empty for the {@link #empty} state, which a checkpoint omits. */
    byte[] encode(EncodingState state);

    EncodingState decode(byte[] bytes);

    /** The codec of an encoding that keeps no state. */
    Codec NONE =
        new Codec() {
          @Override
          public EncodingState empty() {
            return EncodingState.NONE;
          }

          @Override
          public byte[] encode(EncodingState state) {
            return new byte[0];
          }

          @Override
          public EncodingState decode(byte[] bytes) {
            return EncodingState.NONE;
          }
        };
  }
}
