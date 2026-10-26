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
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
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
 * <p>The subject draws its encoding. The statistics count, per encoding, the conflicts a retry
 * repaired (false positives), those a retry confirmed, and the admitted records. Under the Lamport
 * clock the subject is a reference too, which sets the baseline.
 *
 * <p>{@link #aMergedAppendIsWhatItsRetryWouldCommit} probes the merge ({@link Landing}) on its own:
 * its subject admits every delta that can be finalized where it lands ({@link
 * AdmitDeltasEncoding}), and R is an append that validates nothing, whose retry commits on any
 * table.
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
      @ForAll("prefixes") List<TableScript.Cmd> prefix,
      @ForAll("windows") List<TableScript.Cmd> window,
      @ForAll("operations") TableScript.Cmd r) {
    check(encoding, formatVersion, partitioned, mode, prefix, window, r);
  }

  @Property(tries = 300)
  void aMergedAppendIsWhatItsRetryWouldCommit(
      @ForAll @IntRange(min = 2, max = 3) int formatVersion,
      @ForAll boolean partitioned,
      @ForAll("inlineManifestModes") LocalCatalogMode mode,
      @ForAll("prefixes") List<TableScript.Cmd> prefix,
      @ForAll("windows") List<TableScript.Cmd> window,
      @ForAll("appends") TableScript.Cmd r) {
    check(AdmitDeltasEncoding.register(), formatVersion, partitioned, mode, prefix, window, r);
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
    check(BlindAppendEncoding.NAME, formatVersion, partitioned, mode, prefix, window, r);
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
    check(encoding, formatVersion, partitioned, mode, prefix, window, r);
  }

  private static void check(
      String encoding,
      int formatVersion,
      boolean partitioned,
      LocalCatalogMode mode,
      List<TableScript.Cmd> prefix,
      List<TableScript.Cmd> window,
      TableScript.Cmd r) {
    String[] outcome = verify(encoding, formatVersion, partitioned, mode, prefix, window, r);
    Statistics.label("R on a raced table").collect(encoding, outcome[0]);
    Statistics.label("R").collect(outcome[1]);
  }

  /**
   * One try: R on the table the prefix built, the window run just before its first write, in the
   * subject and in the reference. Returns how R's record went in the subject, and R's kind (with
   * " failed" if it failed).
   */
  static String[] verify(
      String encoding,
      int formatVersion,
      boolean partitioned,
      LocalCatalogMode mode,
      List<TableScript.Cmd> prefix,
      List<TableScript.Cmd> window,
      TableScript.Cmd r) {
    StringBuilder trace = new StringBuilder(String.format(
        "v%d %s, %s, %s%nprefix %s%nwindow %s%nR %s%n", formatVersion,
        partitioned ? "partitioned" : "unpartitioned", mode, encoding, prefix, window, r));
    Run subject = Run.of(encoding, mode, formatVersion, partitioned, prefix, window, r, trace);
    Run reference = Run.of(LamportEncoding.NAME, mode, formatVersion, partitioned, prefix, window,
        r, new StringBuilder());

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
        List<TableScript.Cmd> prefix, List<TableScript.Cmd> window, TableScript.Cmd r,
        StringBuilder trace) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      Map<String, String> properties = new java.util.HashMap<>(PROPERTIES);
      properties.put(LogCatalogProperties.CONFLICT_ENCODING, encoding);
      LogCatalog catalog = mode.catalog(io, WAREHOUSE, "c", properties);
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
          TableViews.logical(reader(io).loadTable(TableScript.ID)));
    }

    /** The table's metadata location as a fresh reader finds it: another after any commit. */
    private static String version(InMemoryAtomicFileIO io) {
      TableMetadata current = ((HasTableOperations) reader(io).loadTable(TableScript.ID))
          .operations().current();
      return current.metadataFileLocation();
    }

    private static LogCatalog reader(InMemoryAtomicFileIO io) {
      return LocalCatalogMode.APPEND.catalog(io, WAREHOUSE, "c", Map.of());
    }
  }
}
