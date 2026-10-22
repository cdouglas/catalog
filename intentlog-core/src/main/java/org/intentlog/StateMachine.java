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
 * A deterministic state machine replicated through a log of intentions. Replay folds the records
 * after a checkpoint into the state the checkpoint holds: it applies an intention whose
 * preconditions ({@link Intention#verify}) hold on the state where its record lands, and skips
 * one whose do not.
 *
 * <p>The runtime owns what is common to every state machine: the envelope of checkpoints and
 * records (log.proto), record ids and the committed set that applies each record once, GC of the
 * set, and the commit protocol. A state machine owns its state and the bytes of its checkpoints
 * and intentions, which the runtime treats as opaque.
 *
 * <p>Obligations, on which the commit protocol's honesty rests:
 *
 * <ul>
 *   <li>S1: {@code verify} and {@code apply} are deterministic, whatever the clock or the reader.
 *   <li>S2: {@code verify} is stable: once false at a point of a replay, it is false at every later
 *       point. A record that lost a race then never applies later, so a writer that reads past it
 *       knows its outcome for good, even with a copy of it still in flight.
 *   <li>S3: {@code apply} bumps the version of every entity it changes (S2 rests on it).
 *   <li>S4: a checkpoint round-trips: {@code decodeCheckpoint(encodeCheckpoint(s)).build()} equals
 *       {@code s}.
 * </ul>
 *
 * @param <S> the state: immutable, what a read returns
 * @param <B> a builder of the state: what replay mutates
 * @param <I> an intention
 */
public interface StateMachine<S, B extends StateMachine.Builder<S>, I extends Intention<B>> {

  /** A builder of the state of a log with no checkpoint, before its first record. */
  B empty();

  /** A builder of the state a checkpoint payload holds. */
  B decodeCheckpoint(byte[] state);

  /** The checkpoint payload of {@code state} (S4). */
  byte[] encodeCheckpoint(S state);

  I decodeIntention(byte[] intention);

  byte[] encodeIntention(I intention);

  /** A builder that continues from {@code state}, to replay more records onto it. */
  default B toBuilder(S state) {
    return decodeCheckpoint(encodeCheckpoint(state));
  }

  /** What replay mutates; {@link #build} freezes it into a state. */
  interface Builder<S> {
    S build();
  }
}
