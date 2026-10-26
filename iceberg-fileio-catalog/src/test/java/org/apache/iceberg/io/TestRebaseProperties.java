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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tuple;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.statistics.Statistics;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.io.InMemoryAtomicFileIO.Fault;

/**
 * The oracle of the conflict encodings: a commit that the catalog admits on a table changed since
 * its writer read it must be what Iceberg's own retry of the operation would commit there.
 *
 * <p>A generated schedule ({@link TableScript}) builds a table, then an operation R runs on it.
 * Just before R's first write to the catalog, a window of one to three other commands commits
 * through another catalog instance on the same storage, so R's record lands on a table that may
 * have changed since R read it. The same run happens twice, on separate storage: once in the
 * subject, a catalog under the encoding being tested, and once in the reference, a catalog that
 * conflicts on the table's whole version, where a changed table fails R's record and Iceberg
 * retries R on a fresh read (refresh, apply, validate, commit).
 *
 * <p>Then:
 *
 * <ul>
 *   <li>a record the subject admitted on a changed table (R committed without a conflict) must be
 *       one the reference's retry committed, and the two tables must be logically equal ({@link
 *       TableViews#logical}): no false negative;
 *   <li>otherwise both runs are Iceberg's retries, and must end alike.
 * </ul>
 *
 * <p>The subject draws its encoding, and whether R's writer pins what Iceberg commits once (a
 * schema or spec update, a ref reset): replay must tell that from the delta too ({@link
 * TableChange#commitOnce}), as it must for a writer that does not pin. The statistics count, per
 * encoding, the conflicts a retry repaired (false positives), those a retry confirmed, and the
 * admitted records. Under the Lamport clock the subject is a reference too, which sets the
 * baseline.
 *
 * <p>{@link #aMergedAppendIsWhatItsRetryWouldCommit} probes the merge ({@link Landing}) on its own:
 * its subject admits every delta that can be finalized where it lands ({@link
 * AdmitDeltasEncoding}), and R is an append that validates nothing, whose retry commits on any
 * table; {@link #aMergedAppendAfterAnUpgradeIsWhatItsRetryWouldCommit} does so on tables upgraded
 * to v3. The other properties aim at what each encoding reads: blind-append's appends, the
 * validated commits partition-vector and manifest-window judge by their checks (also racing the
 * removal of the files they read, and delete files their retries drop), and the guards all of
 * them read whatever a record checks.
 *
 * <p>Each was calibrated against mutants of what it guards (fuzz/mutants).
 */
class TestRebaseProperties {

  private static final String WAREHOUSE = "mem:///warehouse";
  private static final String CATALOG = WAREHOUSE + "/catalog-c";
  // No backoff between Iceberg's retries, which every raced commit of the reference makes.
  private static final Map<String, String> PROPERTIES =
      Map.of(CatalogProperties.TABLE_DEFAULT_PREFIX + TableProperties.COMMIT_MIN_RETRY_WAIT_MS,
          "0");

  @Property(tries = 300)
  void anAdmittedCommitIsWhatItsRetryWouldCommit(
      @ForAll("encodings") String encoding,
      @ForAll @IntRange(min = 2, max = 3) int formatVersion,
      @ForAll boolean partitioned,
      @ForAll("modes") LocalCatalogMode mode,
      @ForAll boolean pinned,
      @ForAll("prefixes") List<TableScript.Cmd> prefix,
      @ForAll("windows") List<TableScript.Cmd> window,
      @ForAll("operations") TableScript.Cmd r) {
    check(encoding, formatVersion, partitioned, mode, pinned, prefix, window, r);
  }

  @Property(tries = 300)
  void aMergedAppendIsWhatItsRetryWouldCommit(
      @ForAll @IntRange(min = 2, max = 3) int formatVersion,
      @ForAll boolean partitioned,
      @ForAll("inlineManifestModes") LocalCatalogMode mode,
      @ForAll("prefixes") List<TableScript.Cmd> prefix,
      @ForAll("windows") List<TableScript.Cmd> window,
      @ForAll("appends") TableScript.Cmd r) {
    check(AdmitDeltasEncoding.register(), formatVersion, partitioned, mode, true, prefix, window,
        r);
  }

  /**
   * The merge on a table upgraded to v3: a v2 table's data and branches, its upgrade, then
   * appends racing appends. A v3 commit gives row ids to the data manifests it keeps that have
   * none (written before the upgrade), so a record carries ids for manifests it did not write,
   * which the merge must move with its own, or refuse where the table's lists hold the manifest
   * with others (design.md I3a).
   */
  @Property(tries = 300)
  void aMergedAppendAfterAnUpgradeIsWhatItsRetryWouldCommit(
      @ForAll boolean partitioned,
      @ForAll("inlineManifestModes") LocalCatalogMode mode,
      @ForAll("upgrades") List<TableScript.Cmd> prefix,
      @ForAll("upgradeWindows") List<TableScript.Cmd> window,
      @ForAll("appends") TableScript.Cmd r) {
    check(AdmitDeltasEncoding.register(), 2, partitioned, mode, true, prefix, window, r);
  }

  /**
   * Blind-append's own case: appends, and appends that validate (an exactly-once sink's checkpoint
   * append, an insert-only row delta), racing appends on inline-ML tables.
   */
  @Property(tries = 300)
  void aBlindAppendIsWhatItsRetryWouldCommit(
      @ForAll @IntRange(min = 2, max = 3) int formatVersion,
      @ForAll boolean partitioned,
      @ForAll("inlineManifestModes") LocalCatalogMode mode,
      @ForAll("prefixes") List<TableScript.Cmd> prefix,
      @ForAll("appendWindows") List<TableScript.Cmd> window,
      @ForAll("appendLike") TableScript.Cmd r) {
    check(BlindAppendEncoding.NAME, formatVersion, partitioned, mode, true, prefix, window, r);
  }

  /**
   * Partition-vector's and manifest-window's case: operations whose producers say what their
   * retries check (overwrites, row deltas, deletes, rewrites, dynamic overwrites, and appends),
   * racing data commits, half of them in the same partitions, on inline-ML tables.
   */
  @Property(tries = 300)
  void aValidatedCommitIsWhatItsRetryWouldCommit(
      @ForAll("validatedEncodings") String encoding,
      @ForAll @IntRange(min = 2, max = 3) int formatVersion,
      @ForAll boolean partitioned,
      @ForAll("inlineManifestModes") LocalCatalogMode mode,
      @ForAll("prefixes") List<TableScript.Cmd> prefix,
      @ForAll("dataWindows") List<TableScript.Cmd> window,
      @ForAll("validated") TableScript.Cmd r) {
    check(encoding, formatVersion, partitioned, mode, true, prefix, window, r);
  }

  /**
   * What the merging encodings read whatever a record checks (their guards), and what an opaque
   * producer's retry checks: a blind, validated or opaque commit racing its branch's reset (a
   * rollback, a fast-forward), snapshots removed (an expiry, past which a retry that validates
   * from an expired snapshot cannot see), the table or its specs changed, or a checkpoint
   * committed, and data commits, on inline-ML tables. Concern-vector reads the removal too.
   */
  @Property(tries = 300)
  void aCommitRacingItsGuardsIsWhatItsRetryWouldCommit(
      @ForAll("guardedEncodings") String encoding,
      @ForAll @IntRange(min = 2, max = 3) int formatVersion,
      @ForAll boolean partitioned,
      @ForAll("inlineManifestModes") LocalCatalogMode mode,
      @ForAll("guardPrefixes") List<TableScript.Cmd> prefix,
      @ForAll("guardWindows") List<TableScript.Cmd> window,
      @ForAll("guarded") TableScript.Cmd r) {
    check(encoding, formatVersion, partitioned, mode, true, prefix, window, r);
  }

  /**
   * What the removal of snapshots guards for a validated commit: its retry validates the history
   * from the snapshot it read to its branch's head, which an expiry can cut while a ref keeps that
   * snapshot (and its list, which the merge builds on). A branch at main's head; then commits to
   * main and an expiry of all but each branch's head race a commit that validates from main's head.
   */
  @Property(tries = 300)
  void aValidatedCommitWhoseHistoryExpiredIsWhatItsRetryWouldCommit(
      @ForAll("validatedEncodings") String encoding,
      @ForAll @IntRange(min = 2, max = 3) int formatVersion,
      @ForAll("inlineManifestModes") LocalCatalogMode mode,
      @ForAll("keptHeads") List<TableScript.Cmd> prefix,
      @ForAll("expiredHistories") List<TableScript.Cmd> window,
      @ForAll("validatingFromTheHead") TableScript.Cmd r) {
    check(encoding, formatVersion, true, mode, true, prefix, window, r);
  }

  /**
   * What a validated commit's checks read of the files commits since removed, and what its retry
   * drops: data in one partition; then commits that remove its files (an overwrite, a delete) or
   * add delete files in another partition and move the table on race a commit on that partition
   * that validates its files exist (a row delta, a rewrite) or removes them all (the partition's
   * delete, overwrite or replacement). A merging retry drops the delete files older than every data
   * file it keeps.
   */
  @Property(tries = 300)
  void aValidatedCommitRacingRemovalsIsWhatItsRetryWouldCommit(
      @ForAll("validatedEncodings") String encoding,
      @ForAll @IntRange(min = 2, max = 3) int formatVersion,
      @ForAll("inlineManifestModes") LocalCatalogMode mode,
      @ForAll("removals") List<List<TableScript.Cmd>> sample) {
    check(encoding, formatVersion, true, mode, true, sample.get(0), sample.get(1),
        sample.get(2).get(0));
  }

  private static void check(
      String encoding,
      int formatVersion,
      boolean partitioned,
      LocalCatalogMode mode,
      boolean pinned,
      List<TableScript.Cmd> prefix,
      List<TableScript.Cmd> window,
      TableScript.Cmd r) {
    String[] outcome =
        verify(encoding, formatVersion, partitioned, mode, pinned, prefix, window, r);
    Statistics.label("R on a raced table")
        .collect(pinned ? encoding : encoding + ", unpinned", outcome[0]);
    Statistics.label("R").collect(outcome[1]);
  }

  /** One try ({@code verify} with {@code pinned}), R's writer pinning what it commits once. */
  static String[] verify(
      String encoding,
      int formatVersion,
      boolean partitioned,
      LocalCatalogMode mode,
      List<TableScript.Cmd> prefix,
      List<TableScript.Cmd> window,
      TableScript.Cmd r) {
    return verify(encoding, formatVersion, partitioned, mode, true, prefix, window, r);
  }

  /**
   * One try: R on the table the prefix built, the window run just before its first write, in the
   * subject, whose writer pins what Iceberg commits once if {@code pinned}, and in the reference.
   * Returns how R's record went in the subject, and R's kind (with " failed" if it failed).
   */
  static String[] verify(
      String encoding,
      int formatVersion,
      boolean partitioned,
      LocalCatalogMode mode,
      boolean pinned,
      List<TableScript.Cmd> prefix,
      List<TableScript.Cmd> window,
      TableScript.Cmd r) {
    StringBuilder trace = new StringBuilder(String.format(
        "v%d %s, %s, %s%s%nprefix %s%nwindow %s%nR %s%n", formatVersion,
        partitioned ? "partitioned" : "unpartitioned", mode, encoding,
        pinned ? "" : ", R's writer leaves commit-once unpinned", prefix, window, r));
    Run subject =
        Run.of(encoding, mode, formatVersion, partitioned, pinned, prefix, window, r, trace);
    Run reference = Run.of(LamportEncoding.NAME, mode, formatVersion, partitioned, true, prefix,
        window, r, new StringBuilder());

    String outcome;
    if (!subject.raced) {
      outcome = "R wrote nothing";
    } else if (!subject.changed) {
      outcome = "window left the table as R read it";
    } else if (subject.conflicts == 0 && subject.failure == null) {
      outcome = "admitted on a changed table";
      if (reference.failure != null) {
        throw new AssertionError(String.format(
            "the subject admitted R on a changed table, but Iceberg's retry fails it: %s%n%s",
            reference.failure, trace));
      }
    } else if (subject.conflicts > 0 && subject.failure == null) {
      outcome = "conflict, repaired by the retry (false positive)";
    } else if (subject.conflicts > 0
        && subject.failure instanceof org.apache.iceberg.exceptions.CommitFailedException) {
      // an update Iceberg commits once (a schema, spec or ref change) fails on a changed table
      outcome = "conflict, not retried (" + subject.failure.getClass().getSimpleName() + ")";
    } else if (subject.conflicts > 0) {
      outcome = "conflict, confirmed by the retry ("
          + subject.failure.getClass().getSimpleName() + ")";
    } else {
      outcome = "failed without a conflict (" + subject.failure.getClass().getSimpleName() + ")";
    }

    // Both runs end alike: what the subject admitted is what the retry committed.
    if ((subject.failure == null) != (reference.failure == null)) {
      throw new AssertionError(String.format(
          "R %s in the subject but %s in the reference%n%s",
          subject.failure == null ? "committed" : "failed (" + subject.failure + ")",
          reference.failure == null ? "committed" : "failed (" + reference.failure + ")", trace));
    }
    List<String> differences =
        TableViews.differences(reference.view, subject.view, "reference", "subject");
    if (!differences.isEmpty()) {
      throw new AssertionError("the subject's table differs from the reference's:\n  "
          + String.join("\n  ", differences.subList(0, Math.min(4, differences.size())))
          + "\n" + trace);
    }
    CheckpointRoundTrip.assertEveryWrite(subject.io);
    return new String[] {outcome, r.kind + (subject.failure == null ? "" : " failed")};
  }

  @Provide
  Arbitrary<LocalCatalogMode> modes() {
    return Arbitraries.of(LocalCatalogMode.fileModes());
  }

  /** The conflict encodings a catalog can be created with. */
  @Provide
  Arbitrary<String> encodings() {
    return Arbitraries.of(LocalCatalogMode.ENCODINGS);
  }

  /**
   * The modes whose tables' updates are inline deltas, which can merge, with their manifest lists
   * inline. A manifest list file hides the partition specs of its manifests, which a window may
   * remove (an expiry that cleans up metadata): only an encoding, which stamps spec changes, can
   * tell that it did.
   */
  @Provide
  Arbitrary<LocalCatalogMode> inlineManifestModes() {
    return Arbitraries.of(
        LocalCatalogMode.fileModes().stream().filter(m -> m.inlineML).toList());
  }

  /** Appends, and operations that add data as an append does but validate it. */
  @Provide
  Arbitrary<TableScript.Cmd> appendLike() {
    return TableScript.commands(APPEND_LIKE::contains).withoutEdgeCases();
  }

  /** The encodings that judge validated commits by their checks. */
  @Provide
  Arbitrary<String> validatedEncodings() {
    return Arbitraries.of(PartitionVectorEncoding.NAME, ManifestWindowEncoding.NAME);
  }

  /** Operations whose producers' footprints are validated (or blind). */
  @Provide
  Arbitrary<TableScript.Cmd> validated() {
    return TableScript.commands(VALIDATED::contains).withoutEdgeCases();
  }

  /** Windows of data commits, half the time, else of any single command. */
  @Provide
  Arbitrary<List<TableScript.Cmd>> dataWindows() {
    return Arbitraries.oneOf(
            TableScript.commands(VALIDATED::contains),
            TableScript.windowCommands(TestRebaseProperties::single))
        .withoutEdgeCases()
        .list().ofMinSize(1).ofMaxSize(3);
  }

  private static final java.util.Set<TableScript.Kind> VALIDATED =
      java.util.EnumSet.of(TableScript.Kind.FAST_APPEND, TableScript.Kind.MERGE_APPEND,
          TableScript.Kind.BRANCH_APPEND, TableScript.Kind.INSERT_ROW_DELTA,
          TableScript.Kind.DELETE_FILE, TableScript.Kind.EQUALITY_DELETE,
          TableScript.Kind.POSITION_DELETE, TableScript.Kind.REWRITE_FILES,
          TableScript.Kind.OVERWRITE, TableScript.Kind.REPLACE_PARTITIONS,
          TableScript.Kind.OVERWRITE_PARTITION, TableScript.Kind.MERGE_ROW_DELTA,
          TableScript.Kind.DELETE_PARTITION, TableScript.Kind.LONG_OVERWRITE);

  /** Windows of appends, half the time, else of any single command. */
  @Provide
  Arbitrary<List<TableScript.Cmd>> appendWindows() {
    return Arbitraries.oneOf(
            TableScript.commands(APPEND_LIKE::contains),
            TableScript.windowCommands(TestRebaseProperties::single))
        .withoutEdgeCases()
        .list().ofMinSize(1).ofMaxSize(3);
  }

  private static final java.util.Set<TableScript.Kind> APPEND_LIKE =
      java.util.EnumSet.of(TableScript.Kind.FAST_APPEND, TableScript.Kind.MERGE_APPEND,
          TableScript.Kind.BRANCH_APPEND, TableScript.Kind.CHECKPOINT_APPEND,
          TableScript.Kind.INSERT_ROW_DELTA);

  /** The encodings that read a table's concerns apart, which their guards are among. */
  @Provide
  Arbitrary<String> guardedEncodings() {
    return Arbitraries.of(ConcernVectorEncoding.NAME, BlindAppendEncoding.NAME,
        PartitionVectorEncoding.NAME, ManifestWindowEncoding.NAME);
  }

  /**
   * Blind, validated and opaque commits; half the time one whose retry a guard's change can fail:
   * an overwrite that validates from its snapshot's parent, a checkpoint append (which validates
   * that no ancestor committed its checkpoint), or a cherry-pick (of a snapshot an expiry can
   * remove).
   */
  @Provide
  Arbitrary<TableScript.Cmd> guarded() {
    return Arbitraries.oneOf(
            TableScript.commands(
                kind -> VALIDATED.contains(kind) || APPEND_LIKE.contains(kind)
                    || OPAQUE.contains(kind)),
            TableScript.commands(
                kind -> kind == TableScript.Kind.LONG_OVERWRITE
                    || kind == TableScript.Kind.CHECKPOINT_APPEND
                    || kind == TableScript.Kind.CHERRY_PICK))
        .withoutEdgeCases();
  }

  /**
   * Prefixes that end, a third of the time each, as they are, with a branch fast-forwarded to
   * main's head (which keeps R's parent through an expiry of main's history), or with a staged
   * append and an append to main (a snapshot off main's history, which an expiry removes).
   */
  @Provide
  Arbitrary<List<TableScript.Cmd>> guardPrefixes() {
    Arbitrary<Integer> small = Arbitraries.integers().between(0, 11);
    Arbitrary<List<TableScript.Cmd>> atTheHead = Combinators.combine(small, small, small, small)
        .as((a, b, c, d) -> List.of(new TableScript.Cmd(TableScript.Kind.CREATE_BRANCH, a, b, c),
            new TableScript.Cmd(TableScript.Kind.FAST_FORWARD, d, b, c * 2 + 1)));
    Arbitrary<List<TableScript.Cmd>> staged = Combinators.combine(small, small, small, small)
        .as((a, b, c, d) -> List.of(new TableScript.Cmd(TableScript.Kind.STAGED_APPEND, a, b, c),
            new TableScript.Cmd(TableScript.Kind.FAST_APPEND, d, b, c + 1)));
    Arbitrary<List<TableScript.Cmd>> ends =
        Arbitraries.oneOf(Arbitraries.just(List.of()), atTheHead, staged);
    return Combinators.combine(prefixes(), ends).as((prefix, end) -> {
      List<TableScript.Cmd> all = new ArrayList<>(prefix);
      all.addAll(end);
      return all;
    });
  }

  /**
   * Windows of a table upgraded to v3: appends, rollbacks (perhaps to a snapshot from before the
   * upgrade, whose manifests have no row ids), and any single command.
   */
  @Provide
  Arbitrary<List<TableScript.Cmd>> upgradeWindows() {
    return Arbitraries.frequencyOf(
            Tuple.of(2, TableScript.commands(APPEND_LIKE::contains)),
            Tuple.of(1, TableScript.commands(kind -> kind == TableScript.Kind.ROLLBACK)),
            Tuple.of(1, TableScript.windowCommands(TestRebaseProperties::single)))
        .withoutEdgeCases()
        .list().ofMinSize(1).ofMaxSize(3);
  }

  /**
   * Samples of {@link #aValidatedCommitRacingRemovalsIsWhatItsRetryWouldCommit}, as prefix, window
   * and R: appends to partition p; a window of removals of p's files, delete files in another
   * partition q, and appends to q (to main or a branch); R on p.
   */
  @Provide
  Arbitrary<List<List<TableScript.Cmd>>> removals() {
    Arbitrary<Integer> small = Arbitraries.integers().between(0, 11);
    Arbitrary<Integer> partitions = Arbitraries.integers().between(0, 3);
    return Combinators.combine(partitions, Arbitraries.integers().between(1, 3), small.list()
                .ofMinSize(1).ofMaxSize(3), small.list().ofMinSize(1).ofMaxSize(3), small, small,
            Arbitraries.of(REMOVAL_R))
        .as((p, offset, appends, window, b, j, kind) -> {
          int q = (p + offset) % 4;
          List<TableScript.Cmd> prefix = new ArrayList<>();
          for (int a : appends) {
            prefix.add(new TableScript.Cmd(
                a % 2 == 0 ? TableScript.Kind.FAST_APPEND : TableScript.Kind.MERGE_APPEND, a, b,
                p + 4 * (a % 3)));
          }
          List<TableScript.Cmd> racing = new ArrayList<>();
          for (int w : window) {
            switch (w % 6) {
              case 0:
                racing.add(new TableScript.Cmd(TableScript.Kind.DELETE_PARTITION, w, b, p));
                break;
              case 1:
                racing.add(new TableScript.Cmd(TableScript.Kind.OVERWRITE, w, b, q));
                break;
              case 2:
              case 3:
                racing.add(new TableScript.Cmd(TableScript.Kind.EQUALITY_DELETE, w, b, q));
                break;
              case 4:
                racing.add(new TableScript.Cmd(TableScript.Kind.FAST_APPEND, w, b, q));
                break;
              default:
                racing.add(new TableScript.Cmd(TableScript.Kind.BRANCH_APPEND, w, b, q));
            }
          }
          TableScript.Cmd r = new TableScript.Cmd(kind, j, b, p + 4 * (j % 3));
          return List.of(prefix, racing, List.of(r));
        })
        .withoutEdgeCases();
  }

  /** Commits on one partition that validate its files exist, or remove them all. */
  private static final List<TableScript.Kind> REMOVAL_R =
      List.of(TableScript.Kind.MERGE_ROW_DELTA, TableScript.Kind.REWRITE_FILES,
          TableScript.Kind.DELETE_PARTITION, TableScript.Kind.OVERWRITE_PARTITION,
          TableScript.Kind.REPLACE_PARTITIONS);

  /** Appends to main, then a branch fast-forwarded to main's head. */
  @Provide
  Arbitrary<List<TableScript.Cmd>> keptHeads() {
    Arbitrary<Integer> small = Arbitraries.integers().between(0, 11);
    Arbitrary<List<TableScript.Cmd>> appends = TableScript.commands(
            kind -> kind == TableScript.Kind.FAST_APPEND || kind == TableScript.Kind.MERGE_APPEND)
        .withoutEdgeCases().list().ofMinSize(1).ofMaxSize(4);
    return Combinators.combine(appends, small, small, small, small).as((prefix, a, b, c, d) -> {
      List<TableScript.Cmd> all = new ArrayList<>(prefix);
      all.add(new TableScript.Cmd(TableScript.Kind.CREATE_BRANCH, a, b, c));
      all.add(new TableScript.Cmd(TableScript.Kind.FAST_FORWARD, d, b, c * 2 + 1));
      return all;
    });
  }

  /** Two appends to main, then an expiry of all but each branch's head. */
  @Provide
  Arbitrary<List<TableScript.Cmd>> expiredHistories() {
    Arbitrary<Integer> small = Arbitraries.integers().between(0, 11);
    Arbitrary<TableScript.Cmd> append = TableScript.commands(
        kind -> kind == TableScript.Kind.FAST_APPEND || kind == TableScript.Kind.MERGE_APPEND);
    Arbitrary<TableScript.Cmd> headsOnly = Combinators.combine(small, small)
        .as((a, c) -> new TableScript.Cmd(TableScript.Kind.EXPIRE_SNAPSHOTS, a, 0, c * 2 + 1));
    return Combinators.combine(append, append, headsOnly).as(List::of).withoutEdgeCases();
  }

  /** Commits whose retries validate the history from the snapshot they read. */
  @Provide
  Arbitrary<TableScript.Cmd> validatingFromTheHead() {
    return TableScript.commands(
            kind -> kind == TableScript.Kind.OVERWRITE_PARTITION
                || kind == TableScript.Kind.MERGE_ROW_DELTA
                || kind == TableScript.Kind.INSERT_ROW_DELTA
                || kind == TableScript.Kind.LONG_OVERWRITE)
        .withoutEdgeCases();
  }

  /**
   * Windows of the changes the guards read, of data commits, and of expiries that remove all but
   * each branch's head and nothing else (no unused schema or spec), which only the removal of
   * snapshots stamps; half the time, two data commits and such an expiry, which leaves no history
   * between the head and an older snapshot a ref keeps.
   */
  @Provide
  Arbitrary<List<TableScript.Cmd>> guardWindows() {
    Arbitrary<Integer> small = Arbitraries.integers().between(0, 11);
    Arbitrary<TableScript.Cmd> headsOnly = Combinators.combine(small, small)
        .as((a, c) -> new TableScript.Cmd(TableScript.Kind.EXPIRE_SNAPSHOTS, a, 0, c * 2 + 1));
    Arbitrary<TableScript.Cmd> data = TableScript.commands(
        kind -> VALIDATED.contains(kind) || APPEND_LIKE.contains(kind));
    Arbitrary<List<TableScript.Cmd>> mixed = Arbitraries.frequencyOf(
            Tuple.of(2, TableScript.commands(GUARDS::contains)),
            Tuple.of(1, headsOnly),
            Tuple.of(2, data))
        .list().ofMinSize(1).ofMaxSize(3);
    Arbitrary<List<TableScript.Cmd>> expiringHistory =
        Combinators.combine(data, data, headsOnly).as(List::of);
    return Arbitraries.oneOf(mixed, expiringHistory).withoutEdgeCases();
  }

  /** Producers whose footprint is opaque: they validate their ancestry, or pick a snapshot's. */
  private static final java.util.Set<TableScript.Kind> OPAQUE =
      java.util.EnumSet.of(TableScript.Kind.CHECKPOINT_APPEND, TableScript.Kind.CHERRY_PICK);

  /**
   * The changes that the merging encodings read whatever a record checks (a branch reset,
   * snapshots removed, the table or its specs changed), and a checkpoint committed.
   */
  private static final java.util.Set<TableScript.Kind> GUARDS =
      java.util.EnumSet.of(TableScript.Kind.ROLLBACK, TableScript.Kind.FAST_FORWARD,
          TableScript.Kind.CHERRY_PICK, TableScript.Kind.REMOVE_REF,
          TableScript.Kind.EXPIRE_SNAPSHOTS, TableScript.Kind.SET_LOCATION,
          TableScript.Kind.UPGRADE, TableScript.Kind.ADD_PARTITION_FIELD,
          TableScript.Kind.REMOVE_PARTITION_FIELD, TableScript.Kind.CHECKPOINT_APPEND);

  /**
   * A v2 table's history before its upgrade (appends to main and branches, branches, staged
   * appends), the upgrade, then up to four other commands.
   */
  @Provide
  Arbitrary<List<TableScript.Cmd>> upgrades() {
    Arbitrary<List<TableScript.Cmd>> before =
        TableScript.commands(BEFORE_UPGRADE::contains).withoutEdgeCases()
            .list().ofMinSize(1).ofMaxSize(4);
    Arbitrary<List<TableScript.Cmd>> after =
        TableScript.commands(kind -> kind != TableScript.Kind.STALE).withoutEdgeCases()
            .list().ofMaxSize(4);
    return Combinators.combine(before, after).as((b, a) -> {
      List<TableScript.Cmd> prefix = new ArrayList<>(b);
      prefix.add(new TableScript.Cmd(TableScript.Kind.UPGRADE, 0, 0, 0));
      prefix.addAll(a);
      return prefix;
    });
  }

  private static final java.util.Set<TableScript.Kind> BEFORE_UPGRADE =
      java.util.EnumSet.of(TableScript.Kind.FAST_APPEND, TableScript.Kind.MERGE_APPEND,
          TableScript.Kind.BRANCH_APPEND, TableScript.Kind.CREATE_BRANCH,
          TableScript.Kind.STAGED_APPEND);

  /** Appends whose retry validates nothing, and so commits on any version of the table. */
  @Provide
  Arbitrary<TableScript.Cmd> appends() {
    return TableScript.commands(
            kind -> kind == TableScript.Kind.FAST_APPEND || kind == TableScript.Kind.MERGE_APPEND
                || kind == TableScript.Kind.BRANCH_APPEND
                || kind == TableScript.Kind.STAGED_APPEND)
        .withoutEdgeCases();
  }

  @Provide
  Arbitrary<List<TableScript.Cmd>> prefixes() {
    return TableScript.commands(kind -> kind != TableScript.Kind.STALE).withoutEdgeCases()
        .list().ofMinSize(1).ofMaxSize(12);
  }

  @Provide
  Arbitrary<List<TableScript.Cmd>> windows() {
    return TableScript.windowCommands(TestRebaseProperties::single).withoutEdgeCases()
        .list().ofMinSize(1).ofMaxSize(3);
  }

  @Provide
  Arbitrary<TableScript.Cmd> operations() {
    return TableScript.commands(TestRebaseProperties::single).withoutEdgeCases();
  }

  /** A kind that runs as one command, on a handle of its own. */
  private static boolean single(TableScript.Kind kind) {
    return kind != TableScript.Kind.TRANSACTION && kind != TableScript.Kind.CATALOG_TXN
        && kind != TableScript.Kind.STALE;
  }

  /** One run of a try: the table after R, and how R's commit went. */
  private static final class Run {
    final InMemoryAtomicFileIO io;
    final RuntimeException failure; // what failed R, or null if it committed
    final boolean raced; // whether R wrote to the catalog, so the window ran before its write
    final boolean changed; // whether the window changed the table
    final long conflicts; // how many of R's commits conflicted where they landed
    final Map<String, String> view;

    private Run(InMemoryAtomicFileIO io, RuntimeException failure, boolean raced,
        boolean changed, long conflicts, Map<String, String> view) {
      this.io = io;
      this.failure = failure;
      this.raced = raced;
      this.changed = changed;
      this.conflicts = conflicts;
      this.view = view;
    }

    static Run of(String encoding, LocalCatalogMode mode, int formatVersion, boolean partitioned,
        boolean pinned, List<TableScript.Cmd> prefix, List<TableScript.Cmd> window,
        TableScript.Cmd r, StringBuilder trace) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      Map<String, String> properties = new java.util.HashMap<>(PROPERTIES);
      properties.put(LogCatalogProperties.CONFLICT_ENCODING, encoding);
      LogCatalog catalog = mode.catalog(io, WAREHOUSE, "c", properties);
      if (!pinned) {
        catalog.leavingCommitOnceUnpinned();
      }
      catalog.createNamespace(Namespace.of("ns"));
      TableScript.Runner writer =
          new TableScript.Runner(catalog, formatVersion, partitioned, trace);
      for (List<TableScript.Cmd> step : TableScript.steps(prefix)) {
        writer.run(step);
      }
      TableScript.Runner racer =
          new TableScript.Runner(mode.catalog(io, WAREHOUSE, "c", PROPERTIES), "w", trace);
      boolean[] raced = {false};
      String[] before = {null};
      String[] after = {null};
      // the table just before R's write: its snapshots, which R's commit leaves as they are, and
      // its next row id, from which R's commit gives ids (TableViews.logical)
      TableMetadata[] beforeR = {current(io)};
      List<String> results = new ArrayList<>();
      io.injectFaults((location, strategy) -> {
        if (!raced[0] && location.equals(CATALOG)) {
          raced[0] = true;
          trace.append("window, just before R's first write:\n");
          before[0] = version(io);
          for (List<TableScript.Cmd> step : TableScript.steps(window)) {
            results.add(racer.run(step) == null ? "ok" : "rejected");
          }
          after[0] = version(io);
          beforeR[0] = current(io);
        }
        return Fault.NONE;
      });
      long conflicts = catalog.log().counts().conflicts();
      RuntimeException failure;
      try {
        trace.append("R:\n");
        failure = writer.run(List.of(r));
      } finally {
        io.injectFaults((location, strategy) -> Fault.NONE);
      }
      conflicts = catalog.log().counts().conflicts() - conflicts;
      trace.append(String.format("R %s after %d conflicts; window %s%n",
          failure == null ? "committed" : "failed", conflicts, results));
      return new Run(io, failure, raced[0], raced[0] && !before[0].equals(after[0]), conflicts,
          TableViews.logical(reader(io).loadTable(TableScript.ID),
              beforeR[0].snapshots().size(), beforeR[0].nextRowId()));
    }

    /** The table's metadata location as a fresh reader finds it: another after any commit. */
    private static String version(InMemoryAtomicFileIO io) {
      return current(io).metadataFileLocation();
    }

    /** The table as a fresh reader finds it. */
    private static TableMetadata current(InMemoryAtomicFileIO io) {
      return ((HasTableOperations) reader(io).loadTable(TableScript.ID)).operations().current();
    }

    private static LogCatalog reader(InMemoryAtomicFileIO io) {
      return LocalCatalogMode.APPEND.catalog(io, WAREHOUSE, "c", Map.of());
    }
  }
}
