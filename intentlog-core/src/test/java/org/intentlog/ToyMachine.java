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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A toy state machine for the runtime's tests: keys with versions, and intentions that bump a key
 * from the version they read. A bump of a key another bump moved since conflicts; bumps of
 * different keys compose. It meets S1-S4.
 */
final class ToyMachine implements StateMachine<Map<String, Integer>, ToyMachine.Versions, ToyMachine.Bump> {

  static final ToyMachine INSTANCE = new ToyMachine();

  /** A key's version, bumped by an intention that names the version it read. */
  static final class Versions implements StateMachine.Builder<Map<String, Integer>> {
    final Map<String, Integer> versions = new TreeMap<>();

    @Override
    public Map<String, Integer> build() {
      return new TreeMap<>(versions);
    }
  }

  static final class Bump implements Intention<Versions> {
    final String key;
    final int read;

    Bump(String key, int read) {
      this.key = key;
      this.read = read;
    }

    /** A bump of {@code key} from the version {@code state} has. */
    static Bump of(Map<String, Integer> state, String key) {
      return new Bump(key, state.getOrDefault(key, 0));
    }

    @Override
    public boolean verify(Versions state) {
      return state.versions.getOrDefault(key, 0) == read;
    }

    @Override
    public void apply(Versions state) {
      state.versions.merge(key, 1, Integer::sum);
    }
  }

  @Override
  public Versions empty() {
    return new Versions();
  }

  @Override
  public Versions decodeCheckpoint(byte[] state) {
    Versions versions = new Versions();
    String text = new String(state, StandardCharsets.UTF_8);
    for (String entry : text.isEmpty() ? new String[0] : text.split(",")) {
      String[] kv = entry.split("=");
      versions.versions.put(kv[0], Integer.parseInt(kv[1]));
    }
    return versions;
  }

  @Override
  public byte[] encodeCheckpoint(Map<String, Integer> state) {
    List<String> entries = new ArrayList<>();
    state.forEach((k, v) -> entries.add(k + "=" + v));
    return String.join(",", entries).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public Bump decodeIntention(byte[] intention) {
    String[] kv = new String(intention, StandardCharsets.UTF_8).split("@");
    return new Bump(kv[0], Integer.parseInt(kv[1]));
  }

  @Override
  public byte[] encodeIntention(Bump bump) {
    return (bump.key + "@" + bump.read).getBytes(StandardCharsets.UTF_8);
  }
}
