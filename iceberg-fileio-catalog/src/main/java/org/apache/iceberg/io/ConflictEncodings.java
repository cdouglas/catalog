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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The conflict encodings this build knows, by name, and the framework's hooks into replay: the
 * verify of a table action asks its catalog's encoding whether it admits the action where its
 * record lands ({@link #admits}), and its apply lets the encoding observe the change ({@link
 * #observe}).
 */
final class ConflictEncodings {

  private static final Map<String, ConflictEncoding<?>> ENCODINGS = new ConcurrentHashMap<>();

  static {
    register(LamportEncoding.INSTANCE);
    register(LamportMinusPropertiesEncoding.INSTANCE);
    register(ConcernVectorEncoding.INSTANCE);
    register(BlindAppendEncoding.INSTANCE);
    register(PartitionVectorEncoding.INSTANCE);
    register(ManifestWindowEncoding.INSTANCE);
  }

  private ConflictEncodings() {}

  /** Makes {@code encoding} known by its name. */
  static void register(ConflictEncoding<?> encoding) {
    ENCODINGS.put(encoding.name(), encoding);
  }

  /** The names of the encodings this build knows. */
  static Set<String> names() {
    return Set.copyOf(ENCODINGS.keySet());
  }

  static boolean known(String name) {
    return ENCODINGS.containsKey(name);
  }

  /**
   * The encoding named {@code name}.
   *
   * @throws IllegalStateException if this build does not know it: a catalog created under it
   *     cannot be replayed here
   */
  @SuppressWarnings("unchecked")
  static ConflictEncoding<Object> byName(String name) {
    ConflictEncoding<?> encoding = ENCODINGS.get(name);
    if (encoding == null) {
      throw new IllegalStateException(
          "Unknown conflict encoding: " + name + " (this build knows " + names() + ")");
    }
    return (ConflictEncoding<Object>) encoding;
  }

  /**
   * Whether {@code action} applies where its record lands, on {@code builder}'s state: the
   * verify of a table action. The table must exist; the catalog's encoding must admit the action;
   * and an action on a version of the table other than its base must be an unpinned delta whose
   * late-bound fields can be finalized there ({@link Landing#unmergeable}), so that apply, which
   * must never throw, can merge it.
   */
  static boolean admits(TableAction action, CatalogState.Builder builder) {
    ReplayView landing = ReplayView.of(builder, action.tableId());
    if (!landing.exists()) {
      return false;
    }
    ConflictEncoding<Object> encoding = builder.conflictEncoding();
    Object witness = encoding.witness(action, landing);
    if (!encoding.admits(witness, landing, builder.conflictStateOrEmpty(action.tableId()))) {
      return false;
    }
    if (landing.version() != action.baseVersion()) {
      if (action.pinned() || !landing.inline()
          || Landing.unmergeable(
                  action.delta(), landing.metadata(), builder, action.tableId())
              != null) {
        return false;
      }
    }
    action.verifiedOn(landing);
    return true;
  }

  /**
   * Lets the catalog's encoding observe {@code action}, which changed its table from {@code
   * before} to what {@code builder} holds now.
   */
  static void observe(TableAction action, ReplayView before, CatalogState.Builder builder) {
    builder
        .conflictEncoding()
        .observe(
            action,
            before,
            ReplayView.of(builder, action.tableId()),
            builder.conflictState(action.tableId()));
  }
}
