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
package org.intentlog.testkit;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.intentlog.Intention;
import org.intentlog.StateMachine;

/**
 * The state of the TLA+ models (spec/tla/RSM.tla) as a state machine: entities with versions, and
 * transactions that read some entities at the versions their snapshot had and write some (bumping
 * their versions). A transaction applies where its reads still hold. It need not read what it
 * writes: a blind write applies wherever it lands, so only the log's ids keep a copy of it from
 * applying twice.
 *
 * <p>Each transaction carries a tag, the commit it belongs to, and the state keeps the tags it
 * applied, in order (the models' ghost history), so an oracle can check from the state alone that
 * no commit applied twice and that the state is their serial execution. It meets S1-S4.
 */
public final class EntityVersionsMachine
    implements StateMachine<EntityVersionsMachine.State, EntityVersionsMachine.Builder,
        EntityVersionsMachine.Txn> {

  public static final EntityVersionsMachine INSTANCE = new EntityVersionsMachine();

  private EntityVersionsMachine() {}

  /** Entity versions (an entity absent is at version 0), and the tags applied, in order. */
  public static final class State {
    private final Map<String, Integer> versions;
    private final List<String> history;

    State(Map<String, Integer> versions, List<String> history) {
      this.versions = Collections.unmodifiableMap(new TreeMap<>(versions));
      this.history = List.copyOf(history);
    }

    public static State empty() {
      return new State(Map.of(), List.of());
    }

    public int version(String entity) {
      return versions.getOrDefault(entity, 0);
    }

    public Map<String, Integer> versions() {
      return versions;
    }

    /** The tags of the transactions applied, in the order they applied. */
    public List<String> history() {
      return history;
    }

    /** This state with {@code txn} applied, whether or not it holds here. */
    public State apply(Txn txn) {
      Builder builder = new Builder(this);
      txn.apply(builder);
      return builder.build();
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof State
          && versions.equals(((State) o).versions)
          && history.equals(((State) o).history);
    }

    @Override
    public int hashCode() {
      return Objects.hash(versions, history);
    }

    @Override
    public String toString() {
      return versions + " after " + history;
    }
  }

  public static final class Builder implements StateMachine.Builder<State> {
    private final Map<String, Integer> versions;
    private final List<String> history;

    Builder(State state) {
      this.versions = new TreeMap<>(state.versions);
      this.history = new ArrayList<>(state.history);
    }

    @Override
    public State build() {
      return new State(versions, history);
    }
  }

  /** A transaction: its tag, the versions it read, and the entities it writes. */
  public static final class Txn implements Intention<Builder> {
    private final String tag;
    private final Map<String, Integer> reads;
    private final Set<String> writes;

    public Txn(String tag, Map<String, Integer> reads, Set<String> writes) {
      if (tag.isEmpty() || tag.contains("|") || tag.contains(",")) {
        throw new IllegalArgumentException("Bad tag: " + tag);
      }
      this.tag = tag;
      this.reads = Collections.unmodifiableMap(new TreeMap<>(reads));
      this.writes = Collections.unmodifiableSet(new TreeSet<>(writes));
    }

    /** A transaction that reads {@code reads} and writes {@code writes}, staged on {@code on}. */
    public static Txn on(State on, String tag, Set<String> reads, Set<String> writes) {
      Map<String, Integer> versions = new TreeMap<>();
      reads.forEach(e -> versions.put(e, on.version(e)));
      return new Txn(tag, versions, writes);
    }

    public String tag() {
      return tag;
    }

    public Map<String, Integer> reads() {
      return reads;
    }

    public Set<String> writes() {
      return writes;
    }

    /** Whether its reads hold on {@code state}. */
    public boolean holdsOn(State state) {
      return reads.entrySet().stream().allMatch(r -> state.version(r.getKey()) == r.getValue());
    }

    @Override
    public boolean verify(Builder state) {
      return reads.entrySet().stream()
          .allMatch(r -> state.versions.getOrDefault(r.getKey(), 0).equals(r.getValue()));
    }

    @Override
    public void apply(Builder state) {
      writes.forEach(e -> state.versions.merge(e, 1, Integer::sum));
      state.history.add(tag);
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof Txn
          && tag.equals(((Txn) o).tag)
          && reads.equals(((Txn) o).reads)
          && writes.equals(((Txn) o).writes);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tag, reads, writes);
    }

    @Override
    public String toString() {
      return tag + "(" + reads + " -> " + writes + ")";
    }
  }

  @Override
  public Builder empty() {
    return new Builder(State.empty());
  }

  // Checkpoint: "e=v,e=v|tag,tag". Intention: "tag|e=v,e=v|e,e".

  @Override
  public Builder decodeCheckpoint(byte[] state) {
    String[] parts = split(new String(state, StandardCharsets.UTF_8), 2);
    return new Builder(new State(versions(parts[0]), list(parts[1])));
  }

  @Override
  public byte[] encodeCheckpoint(State state) {
    return (versions(state.versions) + "|" + String.join(",", state.history))
        .getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public Txn decodeIntention(byte[] intention) {
    String[] parts = split(new String(intention, StandardCharsets.UTF_8), 3);
    return new Txn(parts[0], versions(parts[1]), new TreeSet<>(list(parts[2])));
  }

  @Override
  public byte[] encodeIntention(Txn txn) {
    return (txn.tag + "|" + versions(txn.reads) + "|" + String.join(",", txn.writes))
        .getBytes(StandardCharsets.UTF_8);
  }

  private static String[] split(String text, int parts) {
    String[] split = text.split("\\|", -1);
    if (split.length != parts) {
      throw new IllegalArgumentException("Expected " + parts + " parts: " + text);
    }
    return split;
  }

  private static String versions(Map<String, Integer> versions) {
    List<String> entries = new ArrayList<>();
    versions.forEach((e, v) -> entries.add(e + "=" + v));
    return String.join(",", entries);
  }

  private static Map<String, Integer> versions(String text) {
    Map<String, Integer> versions = new TreeMap<>();
    for (String entry : list(text)) {
      int eq = entry.indexOf('=');
      versions.put(entry.substring(0, eq), Integer.parseInt(entry.substring(eq + 1)));
    }
    return versions;
  }

  private static List<String> list(String text) {
    return text.isEmpty() ? List.of() : List.of(text.split(","));
  }
}
