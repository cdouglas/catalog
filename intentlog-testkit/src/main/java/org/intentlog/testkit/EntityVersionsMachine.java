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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
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
 *
 * <p>A paged machine ({@link #paged}) also pages a copy of its history, as the catalog pages its
 * tables' old snapshots (docs/SNAPSHOT_PAGING.md), so that the runtime's handling of a machine's
 * pages ({@link StateMachine#pages}, {@link StateMachine#droppedPages}, {@link
 * StateMachine#garbagePages}) meets the commit-protocol fuzzer: each time the history completes a
 * chunk of {@code chunk} tags, replay seals the chunk into a page, {@code hist.<k>.<digest>} (the
 * k-th chunk; a digest of its bytes, the tags joined by commas), which the state names until a
 * transaction that writes {@link #TRIM} drops the oldest page it names. The ghost history stays
 * whole: a page is a copy of part of it, which an oracle can check page by page. A checkpoint
 * names the pages, never holds them, and decoding one reads none; a state holds the bytes of each
 * page its own records sealed until {@link #pagesPut} says it is stored.
 */
public final class EntityVersionsMachine
    implements StateMachine<EntityVersionsMachine.State, EntityVersionsMachine.Builder,
        EntityVersionsMachine.Txn> {

  /** The machine that pages nothing. */
  public static final EntityVersionsMachine INSTANCE = new EntityVersionsMachine(0);

  /**
   * The entity whose write also drops the oldest page of the history a paged machine's state
   * names (a version bump like any other's on a machine that pages nothing).
   */
  public static final String TRIM = "!trim";

  /** The prefix of a page of the history. */
  public static final String PAGE_PREFIX = "hist.";

  private final int chunk;

  private EntityVersionsMachine(int chunk) {
    this.chunk = chunk;
  }

  /** A machine that pages its history in chunks of {@code chunk} tags. */
  public static EntityVersionsMachine paged(int chunk) {
    if (chunk < 1) {
      throw new IllegalArgumentException("A chunk holds at least one tag: " + chunk);
    }
    return new EntityVersionsMachine(chunk);
  }

  /** How many tags a page of the history holds; 0 for a machine that pages nothing. */
  public int chunk() {
    return chunk;
  }

  /** The bytes of a page of the history that holds {@code tags}. */
  public static byte[] pageBytes(List<String> tags) {
    return String.join(",", tags).getBytes(StandardCharsets.UTF_8);
  }

  /** The name of the {@code k}-th page of the history, which holds {@code bytes}. */
  public static String pageName(int k, byte[] bytes) {
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
      return PAGE_PREFIX + k + "." + HexFormat.of().formatHex(digest, 0, 8);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  /** The chunk index a page of the history's name names; -1 for any other name. */
  public static int pageIndex(String name) {
    if (!name.startsWith(PAGE_PREFIX)) {
      return -1;
    }
    String[] parts = name.substring(PAGE_PREFIX.length()).split("\\.", -1);
    try {
      return parts.length == 2 ? Integer.parseInt(parts[0]) : -1;
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  /**
   * A page of the history a state names: its chunk index and name, and its bytes while no
   * compactor has stored it (shared by every state a fold continues from the one that sealed it).
   */
  static final class HistoryPage {
    final int index;
    final String name;
    private volatile byte[] pending;

    HistoryPage(int index, String name, byte[] pending) {
      this.index = index;
      this.name = name;
      this.pending = pending;
    }
  }

  /** A page a state's own records dropped, until the runtime has deleted it. */
  static final class DroppedPage {
    final String name;
    private volatile boolean deleted = false;

    DroppedPage(String name) {
      this.name = name;
    }
  }

  /**
   * Entity versions (an entity absent is at version 0), and the tags applied, in order; on a
   * paged machine, the pages of the history it names: the chunks from {@link #trimmed} through
   * {@link #sealed}.
   */
  public static final class State {
    private final Map<String, Integer> versions;
    private final List<String> history;
    private final int chunk;
    private final int trimmed;
    private final List<HistoryPage> pages;
    private final List<DroppedPage> dropped;

    State(Map<String, Integer> versions, List<String> history) {
      this(versions, history, 0, 0, List.of(), List.of());
    }

    State(Map<String, Integer> versions, List<String> history, int chunk, int trimmed,
        List<HistoryPage> pages, List<DroppedPage> dropped) {
      this.versions = Collections.unmodifiableMap(new TreeMap<>(versions));
      this.history = List.copyOf(history);
      this.chunk = chunk;
      this.trimmed = trimmed;
      this.pages = List.copyOf(pages);
      this.dropped = List.copyOf(dropped);
    }

    public static State empty() {
      return new State(Map.of(), List.of());
    }

    /** The chunks of the history dropped: the oldest page it names is the {@code trimmed()}-th. */
    public int trimmed() {
      return trimmed;
    }

    /** How many chunks of the history its records sealed, the trimmed ones among them. */
    public int sealed() {
      return trimmed + pages.size();
    }

    /** The names of the pages of the history it names, oldest first: chunks trimmed() on. */
    public List<String> pageNames() {
      List<String> names = new ArrayList<>(pages.size());
      for (HistoryPage page : pages) {
        names.add(page.name);
      }
      return names;
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

    /** Equal versions, history and pages named (not whether they are stored or deleted). */
    @Override
    public boolean equals(Object o) {
      return o instanceof State
          && versions.equals(((State) o).versions)
          && history.equals(((State) o).history)
          && trimmed == ((State) o).trimmed
          && pageNames().equals(((State) o).pageNames());
    }

    @Override
    public int hashCode() {
      return Objects.hash(versions, history, trimmed);
    }

    @Override
    public String toString() {
      return versions + " after " + history
          + (chunk == 0 ? "" : ", pages " + trimmed + ".." + sealed());
    }
  }

  public static final class Builder implements StateMachine.Builder<State> {
    private final Map<String, Integer> versions;
    private final List<String> history;
    private final int chunk;
    private int trimmed;
    private final List<HistoryPage> pages;
    private final List<DroppedPage> dropped;

    Builder(State state) {
      this.versions = new TreeMap<>(state.versions);
      this.history = new ArrayList<>(state.history);
      this.chunk = state.chunk;
      this.trimmed = state.trimmed;
      this.pages = new ArrayList<>(state.pages);
      this.dropped = new ArrayList<>(state.dropped);
    }

    /**
     * After a transaction applied: seals each chunk the history completed into a page, then, if
     * the transaction trims, drops the oldest page the state names.
     */
    void applied(boolean trim) {
      if (chunk == 0) {
        return;
      }
      for (int k = trimmed + pages.size(); history.size() >= (k + 1) * chunk; k++) {
        byte[] bytes = pageBytes(history.subList(k * chunk, (k + 1) * chunk));
        pages.add(new HistoryPage(k, pageName(k, bytes), bytes));
      }
      if (trim && !pages.isEmpty()) {
        dropped.add(new DroppedPage(pages.remove(0).name));
        trimmed++;
      }
    }

    @Override
    public State build() {
      return new State(versions, history, chunk, trimmed, pages, dropped);
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
      state.applied(writes.contains(TRIM));
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
    return new Builder(new State(Map.of(), List.of(), chunk, 0, List.of(), List.of()));
  }

  // Checkpoint: "e=v,e=v|tag,tag", and on a paged machine "|trimmed|name,name" after it.
  // Intention: "tag|e=v,e=v|e,e".

  /** A state decoded from a checkpoint names only pages that are stored: it holds no bytes. */
  @Override
  public Builder decodeCheckpoint(byte[] state) {
    String[] parts = split(new String(state, StandardCharsets.UTF_8), chunk == 0 ? 2 : 4);
    if (chunk == 0) {
      return new Builder(new State(versions(parts[0]), list(parts[1])));
    }
    int trimmed = Integer.parseInt(parts[2]);
    List<HistoryPage> pages = new ArrayList<>();
    for (String name : list(parts[3])) {
      pages.add(new HistoryPage(trimmed + pages.size(), name, null));
    }
    return new Builder(
        new State(versions(parts[0]), list(parts[1]), chunk, trimmed, pages, List.of()));
  }

  @Override
  public byte[] encodeCheckpoint(State state) {
    String text = versions(state.versions) + "|" + String.join(",", state.history);
    if (chunk != 0) {
      text += "|" + state.trimmed + "|" + String.join(",", state.pageNames());
    }
    return text.getBytes(StandardCharsets.UTF_8);
  }

  /**
   * A builder that continues {@code state}: on a paged machine sharing its pages, so that a page
   * a fold continues from stays unstored until a compactor stores it, and a dropped one undeleted.
   */
  @Override
  public Builder toBuilder(State state) {
    return chunk == 0 ? StateMachine.super.toBuilder(state) : new Builder(state);
  }

  /** The pages of the history {@code state}'s records sealed that no compactor has stored. */
  @Override
  public Map<String, byte[]> pages(State state) {
    Map<String, byte[]> pending = new LinkedHashMap<>();
    for (HistoryPage page : state.pages) {
      byte[] bytes = page.pending;
      if (bytes != null) {
        pending.put(page.name, bytes);
      }
    }
    return pending;
  }

  @Override
  public void pagesPut(State state, Collection<String> names) {
    Set<String> stored = new HashSet<>(names);
    for (HistoryPage page : state.pages) {
      if (stored.contains(page.name)) {
        page.pending = null;
      }
    }
  }

  /** The pages {@code state}'s records trimmed that the runtime has not deleted. */
  @Override
  public Collection<String> droppedPages(State state) {
    List<String> names = new ArrayList<>();
    for (DroppedPage page : state.dropped) {
      if (!page.deleted) {
        names.add(page.name);
      }
    }
    return names;
  }

  @Override
  public void pagesDeleted(State state, Collection<String> names) {
    Set<String> deleted = new HashSet<>(names);
    for (DroppedPage page : state.dropped) {
      if (deleted.contains(page.name)) {
        page.deleted = true;
      }
    }
  }

  /**
   * The pages of the history among {@code names} that are garbage under {@code state} and every
   * state after it: a chunk it trimmed, or one it sealed under another name. A chunk at or past
   * those it sealed is not: a record after it seals that chunk.
   */
  @Override
  public Set<String> garbagePages(State state, Collection<String> names) {
    Set<String> garbage = new HashSet<>();
    for (String name : names) {
      int k = pageIndex(name);
      if (k < 0) {
        continue;
      }
      if (k < state.trimmed) {
        garbage.add(name);
        continue;
      }
      if (k >= state.sealed()) {
        continue; // sealed by a record after this state
      }
      if (!state.pages.get(k - state.trimmed).name.equals(name)) {
        garbage.add(name);
      }
    }
    return garbage;
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
