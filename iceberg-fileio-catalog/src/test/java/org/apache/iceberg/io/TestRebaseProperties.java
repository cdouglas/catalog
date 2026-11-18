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
import net.jqwik.api.Example;
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
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
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
 * <p>An expiry is the exception: the subject's merge of one onto a changed table removes the
 * snapshots it chose on the table it read (errata D17), where Iceberg's retry chooses again on the
 * table the window left (a retained count counts the window's snapshots too). Its reference runs
 * the window, then removes exactly the snapshots R removes after the prefix alone (matched by
 * sequence number), as Iceberg's builder does: a merge that removes a snapshot a ref names, which
 * the builder refuses, fails it ({@link #aMergedExpiryRemovesWhatItChoseOnItsBase}).
 *
 * <p>The subject draws its encoding, and whether R's writer pins what Iceberg commits once (a
 * schema or spec update, a ref reset): replay must tell that from the delta too ({@link
 * TableChange#commitOnce}), as it must for a writer that does not pin. Half the tries merge
 * manifests (C2 (retry oracle never merges manifests)): the table's {@code
 * commit.manifest.min-count-to-merge} is 2, so a merging producer (a merge append, an overwrite, a
 * delete, a row delta) rewrites its branch's manifests into one with its own, and data commands
 * add one to three files, so a commit that removes one rewrites a manifest that keeps the others:
 * a manifest R wrote then keeps older entries, whose sequence numbers and row ids the merge must
 * leave as they were ({@link TableViews#logical}'s manifests). The statistics count, per
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
 * <p>{@link #anAdmittedReadSeesTheRowsItRead} is the oracle of serializable reads
 * (docs/CATALOG_TRANSACTIONS.md, "No false negatives"): R is a serializable catalog transaction
 * that scans a ref of the table under a row filter and writes another table, the same one, or
 * nothing, and the window commits on the table before R's commit reads the catalog, just before
 * its record is written, or both. Its oracle needs no reference: if the catalog admits R, the
 * rows of the ref under the filter where R took effect ({@link TableViews#rows}: tokens that a
 * rewrite keeps, each with the delete files that delete from it, and the schema the scan reads
 * them with) must be the rows R's scan read, with the schema it read them with.
 * It judges a catalog that judges reads by their filters as it does one that judges them by their
 * refs; the statistics count, per encoding and table storage, the reads rejected whose rows had
 * not changed (false positives), with their reasons.
 *
 * <p>Each was calibrated against mutants of what it guards (fuzz/mutants; the reads oracle,
 * fuzz/mutants/reads.sh).
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
      @ForAll("operations") TableScript.Cmd r,
      @ForAll boolean merging) {
    check(encoding, formatVersion, partitioned, mode, pinned, merging, prefix, window, r);
  }

  @Property(tries = 300)
  void aMergedAppendIsWhatItsRetryWouldCommit(
      @ForAll @IntRange(min = 2, max = 3) int formatVersion,
      @ForAll boolean partitioned,
      @ForAll("inlineManifestModes") LocalCatalogMode mode,
      @ForAll("prefixes") List<TableScript.Cmd> prefix,
      @ForAll("windows") List<TableScript.Cmd> window,
      @ForAll("appends") TableScript.Cmd r,
      @ForAll boolean merging) {
    check(AdmitDeltasEncoding.register(), formatVersion, partitioned, mode, true, merging, prefix,
        window, r);
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
      @ForAll("appends") TableScript.Cmd r,
      @ForAll boolean merging) {
    check(AdmitDeltasEncoding.register(), 2, partitioned, mode, true, merging, prefix, window, r);
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
      @ForAll("appendLike") TableScript.Cmd r,
      @ForAll boolean merging) {
    check(BlindAppendEncoding.NAME, formatVersion, partitioned, mode, true, merging, prefix, window,
        r);
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
      @ForAll("validated") TableScript.Cmd r,
      @ForAll boolean merging) {
    check(encoding, formatVersion, partitioned, mode, true, merging, prefix, window, r);
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
      @ForAll("guarded") TableScript.Cmd r,
      @ForAll boolean merging) {
    check(encoding, formatVersion, partitioned, mode, true, merging, prefix, window, r);
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
      @ForAll("validatingFromTheHead") TableScript.Cmd r,
      @ForAll boolean merging) {
    check(encoding, formatVersion, true, mode, true, merging, prefix, window, r);
  }

  /**
   * An expiry R racing a window of data commits (to main or a branch), resets (rollbacks,
   * fast-forwards, cherry-picks), refs created on snapshots the table has or removed, other
   * expiries, or any single command, on a table with branches and staged snapshots. An encoding
   * that merges an expiry onto a changed table (errata D17) must do so only where no ref came to
   * name a snapshot it chose, and leave the table as the window, then the removal of exactly those
   * snapshots, would ({@link #verify}'s expiry reference); one it rejects, Iceberg's retry commits.
   */
  @Property(tries = 300)
  void aMergedExpiryRemovesWhatItChoseOnItsBase(
      @ForAll("encodings") String encoding,
      @ForAll @IntRange(min = 2, max = 3) int formatVersion,
      @ForAll boolean partitioned,
      @ForAll("inlineModes") LocalCatalogMode mode,
      @ForAll boolean pinned,
      @ForAll("guardPrefixes") List<TableScript.Cmd> prefix,
      @ForAll("expiryWindows") List<TableScript.Cmd> window,
      @ForAll("expiries") TableScript.Cmd r,
      @ForAll boolean merging) {
    check(encoding, formatVersion, partitioned, mode, pinned, merging, prefix, window, r);
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
      @ForAll("removals") List<List<TableScript.Cmd>> sample,
      @ForAll boolean merging) {
    check(encoding, formatVersion, true, mode, true, merging, sample.get(0), sample.get(1),
        sample.get(2).get(0));
  }

  /**
   * The oracle of serializable reads (docs/CATALOG_TRANSACTIONS.md, "No false negatives"): R, a
   * serializable catalog transaction, scans a ref of table T (main, another branch or a tag) under
   * a row filter on {@code cat}, then appends to table U, appends to T, or writes nothing. A window
   * of one to four commands commits on T through another catalog after R read it: some before R's
   * commit reads the catalog (which checks R's read), the rest just before R's record is written
   * (where the conflict encoding judges its read witness or pin); a read-only R writes no record,
   * so its whole window runs before its commit. If the catalog admits R, the rows of its ref under
   * its filter where R took effect ({@link TableViews#rows}) must be the rows its scan read, read
   * with the same schema: no false negative. A catalog may reject more: the statistics count, per encoding and table
   * storage, the reads it rejected whose rows had not changed (false positives), with their
   * reasons.
   */
  @Property(tries = 500)
  void anAdmittedReadSeesTheRowsItRead(
      @ForAll("encodings") String encoding,
      @ForAll @IntRange(min = 2, max = 3) int formatVersion,
      @ForAll boolean partitioned,
      @ForAll("modes") LocalCatalogMode mode,
      @ForAll("readWitnesses") boolean witness,
      @ForAll("readPrefixes") List<TableScript.Cmd> prefix,
      @ForAll("readers") Reader r,
      @ForAll("readWindows") List<TableScript.Cmd> window,
      @ForAll("placements") Placement placement,
      @ForAll boolean merging) {
    String[] outcome = verifyRead(encoding, formatVersion, partitioned, mode, witness, merging,
        prefix, r, window, placement);
    String storage = mode.inlineML ? "inline ML" : mode.inlineTM ? "inline TM" : "pointer";
    if (outcome[1].startsWith(Writes.SAME_TABLE.name())) {
      // a rejection may be its append's: a conflict of the delta, or of an update pinned
      Statistics.label("R's read beside its append to T").collect(encoding, storage, outcome[0]);
    } else {
      Statistics.label("R's read, " + encoding).collect(storage, outcome[0]);
    }
    Statistics.label("R").collect(outcome[1]);
    Statistics.label("rows R read").collect(outcome[2]);
    Statistics.label("R's ref in the window").collect(outcome[4], outcome[0]);
    if (outcome[3] != null) {
      Statistics.label("false positives").collect(encoding, outcome[3]);
    }
  }

  /**
   * Round p12-r0p's rebase job, shrunk (seed -6056536446781838245): a prefix that partitions by
   * {@code day(ts)}, replaces the table with a schema without {@code ts} (the old spec stays), then
   * cherry-picks a snapshot of the old spec onto main and rewrites one of its files. The rewrite's
   * manifest of the removed file carries the old spec and the new schema, which do not bind
   * together, and the oracle's lineage, reading the rewrite's files with the spec each manifest
   * carries, failed: {@code ValidationException: Cannot find source column for partition field:
   * 1001: ts_day: day(4)}.
   */
  @Example
  void aRewriteInASpecWhoseSourceColumnAReplaceDropped() {
    List<TableScript.Cmd> prefix = new ArrayList<>();
    prefix.add(new TableScript.Cmd(TableScript.Kind.ADD_PARTITION_FIELD, 0, 3, 0));
    for (int i = 0; i < 5; i++) {
      prefix.add(new TableScript.Cmd(TableScript.Kind.FAST_APPEND, 0, 0, 0));
    }
    prefix.add(new TableScript.Cmd(TableScript.Kind.REPLACE, 1, 0, 0));
    prefix.add(new TableScript.Cmd(TableScript.Kind.FAST_APPEND, 0, 0, 8));
    prefix.add(new TableScript.Cmd(TableScript.Kind.CHERRY_PICK, 5, 1, 5));
    prefix.add(new TableScript.Cmd(TableScript.Kind.REWRITE_FILES, 10, 3, 9));
    prefix.add(new TableScript.Cmd(TableScript.Kind.CREATE_BRANCH, 2, 11, 11));
    prefix.add(new TableScript.Cmd(TableScript.Kind.FAST_FORWARD, 10, 11, 23));
    List<TableScript.Cmd> window = List.of(
        new TableScript.Cmd(TableScript.Kind.EQUALITY_DELETE, 8, 10, 10),
        new TableScript.Cmd(TableScript.Kind.OVERWRITE, 1, 11, 3),
        new TableScript.Cmd(TableScript.Kind.CREATE_TAG, 7, 0, 10));
    verifyRead(LamportEncoding.NAME, 2, true, LocalCatalogMode.APPEND, true, false, prefix,
        new Reader(1, Expressions.alwaysTrue(), true, Writes.OTHER_TABLE, 1), window,
        Placement.BEFORE_THE_COMMIT);
  }

  /**
   * One try of {@link #anAdmittedReadSeesTheRowsItRead}: R's read of the table the prefix built,
   * the window run where {@code placement} says (all of it before R's commit, if R writes
   * nothing), on a table that merges manifests of several files if {@code merging} (as {@link
   * #verify}'s). Returns how R's read went (by its rows), what R wrote and where the window ran,
   * whether R read rows, why a read whose rows had not changed was rejected (null if none was),
   * and how the window moved R's ref.
   */
  static String[] verifyRead(
      String encoding,
      int formatVersion,
      boolean partitioned,
      LocalCatalogMode mode,
      boolean witness,
      boolean merging,
      List<TableScript.Cmd> prefix,
      Reader r,
      List<TableScript.Cmd> window,
      Placement placement) {
    StringBuilder trace = new StringBuilder(String.format(
        "v%d %s, %s, %s%s%s%nprefix %s%nR %s%nwindow %s, %s%n", formatVersion,
        partitioned ? "partitioned" : "unpartitioned", mode, encoding,
        witness ? "" : ", reads pinned", merging ? ", merging manifests of several files" : "",
        prefix, r, window, placement));
    InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    Map<String, String> properties = Run.properties(merging);
    properties.put(LogCatalogProperties.CONFLICT_ENCODING, encoding);
    properties.put(LogCatalogProperties.TXN_READ_WITNESS, String.valueOf(witness));
    LogCatalog catalog = mode.catalog(io, WAREHOUSE, "c", properties);
    catalog.createNamespace(Namespace.of("ns"));
    TableScript.Runner writer =
        new TableScript.Runner(catalog, formatVersion, partitioned, trace).severalFiles(merging);
    TableViews.Lineage lineage = new TableViews.Lineage();
    for (List<TableScript.Cmd> step : TableScript.steps(prefix)) {
      writer.run(step);
      lineage.observe(Run.reader(io).loadTable(TableScript.ID));
    }
    TableScript.Runner racer =
        new TableScript.Runner(mode.catalog(io, WAREHOUSE, "c", PROPERTIES), "w", trace)
            .severalFiles(merging);
    TableScript.Runner files = new TableScript.Runner(catalog, "r", trace);

    // R scans T, as the transaction first read it
    org.apache.iceberg.catalog.CatalogTransaction txn = catalog.createTransaction(
        org.apache.iceberg.catalog.CatalogTransaction.IsolationLevel.SERIALIZABLE);
    org.apache.iceberg.Table t = txn.asCatalog().loadTable(TableScript.ID);
    List<String> others = new ArrayList<>(t.refs().keySet());
    others.remove(org.apache.iceberg.SnapshotRef.MAIN_BRANCH);
    others.sort(String::compareTo);
    String ref = r.ref < 4 || others.isEmpty() ? org.apache.iceberg.SnapshotRef.MAIN_BRANCH
        : others.get(r.ref % others.size());
    org.apache.iceberg.TableScan scan = t.newScan();
    if (!ref.equals(org.apache.iceberg.SnapshotRef.MAIN_BRANCH)) {
      scan = scan.useRef(ref);
    }
    scan = scan.filter(r.filter).caseSensitive(r.caseSensitive);
    try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
        scan.planFiles()) {
      tasks.forEach(task -> { });
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
    org.apache.iceberg.Table atRead = Run.reader(io).loadTable(TableScript.ID);
    org.apache.iceberg.Snapshot scanned = t.snapshot(ref);
    org.apache.iceberg.Snapshot there = atRead.snapshot(ref);
    if (!java.util.Objects.equals(scanned == null ? null : scanned.snapshotId(),
        there == null ? null : there.snapshotId())) {
      throw new IllegalStateException("R read " + ref + " at another version than the table's");
    }
    if (!TableViews.schemaRead(atRead, ref).equals("schema " + scan.schema().asStruct())) {
      throw new IllegalStateException("R read " + ref + " with another schema than the view's: "
          + scan.schema().asStruct());
    }
    List<String> read = TableViews.rows(atRead, ref, r.filter, r.caseSensitive, lineage);
    String versionRead = Run.version(io);
    trace.append(String.format("R scans %s (%s) under %s: %s%n", ref,
        scanned == null ? "no snapshot" : "snapshot " + scanned.snapshotId(), r.filter, read));

    Writes writes = r.writes;
    try {
      switch (writes) {
        case OTHER_TABLE:
          org.apache.iceberg.Table u = txn.asCatalog().loadTable(TableScript.OTHER);
          u.newFastAppend().appendFile(files.newFile(u, r.partition)).commit();
          break;
        case SAME_TABLE:
          t.newFastAppend().appendFile(files.newFile(t, r.partition)).commit();
          break;
        default:
      }
    } catch (RuntimeException e) {
      // a file TableScript cannot write in the table's spec (as its runner skips the command)
      if (!TableScript.rejects(e)) {
        throw new AssertionError(
            "R's write threw what no rejection throws: " + e + "\n" + trace, e);
      }
      trace.append("R's write skipped: ").append(e).append('\n');
      writes = Writes.NOTHING;
    }

    List<List<TableScript.Cmd>> steps = TableScript.steps(window);
    int before = writes == Writes.NOTHING || placement == Placement.BEFORE_THE_COMMIT
        ? steps.size() : placement == Placement.BEFORE_THE_WRITE ? 0 : (steps.size() + 1) / 2;
    trace.append("window, before R's commit:\n");
    List<String> moves = new ArrayList<>();
    window(racer, steps.subList(0, before), io, lineage, ref, "before the commit", moves);
    // the table where R takes effect: after the window
    String[] landed = {Run.version(io)};
    List<List<String>> landedRows = new ArrayList<>(List.of(TableViews.rows(
        Run.reader(io).loadTable(TableScript.ID), ref, r.filter, r.caseSensitive, lineage)));
    boolean[] raced = {false};
    io.injectFaults((location, strategy) -> {
      if (!raced[0] && location.equals(CATALOG)) {
        raced[0] = true;
        trace.append("window, just before R's first write:\n");
        window(racer, steps.subList(before, steps.size()), io, lineage, ref, "before the write",
            moves);
        landed[0] = Run.version(io);
        landedRows.set(0, TableViews.rows(
            Run.reader(io).loadTable(TableScript.ID), ref, r.filter, r.caseSensitive, lineage));
      }
      return Fault.NONE;
    });
    Map<String, Long> reasons = catalog.log().counts().conflictReasons();
    RuntimeException failure = null;
    try {
      trace.append("R commits\n");
      txn.commitTransaction();
    } catch (org.apache.iceberg.exceptions.CommitFailedException
        | org.apache.iceberg.exceptions.ValidationException e) {
      failure = e;
    } finally {
      io.injectFaults((location, strategy) -> Fault.NONE);
    }
    List<String> rows = landedRows.get(0);
    trace.append(String.format("R %s; where it took effect, %s under %s: %s%n",
        failure == null ? "committed" : "failed (" + failure + ")", ref, r.filter, rows));

    String outcome;
    String why = null;
    if (landed[0].equals(versionRead)) {
      outcome = failure == null ? "table as read, admitted"
          : "table as read, rejected (false positive)";
    } else if (rows.equals(read)) {
      outcome = failure == null ? "rows as read, admitted"
          : "rows as read, rejected (false positive)";
    } else if (failure != null) {
      outcome = "rows changed, rejected";
    } else {
      List<String> gone = new ArrayList<>(read);
      gone.removeAll(rows);
      List<String> came = new ArrayList<>(rows);
      came.removeAll(read);
      throw new AssertionError(String.format(
          "the catalog admitted R, but the rows it read changed: gone %s, new %s%n%s", gone, came,
          trace));
    }
    if (failure != null && outcome.endsWith("(false positive)")) {
      List<String> grown = new ArrayList<>();
      catalog.log().counts().conflictReasons().forEach((reason, count) -> {
        if (count > reasons.getOrDefault(reason, 0L)) {
          grown.add(reason);
        }
      });
      why = !grown.isEmpty() ? String.join(", ", grown)
          : failure.getMessage().contains("since the transaction read it on ")
              ? "the read, at the commit's read"
              : failure.getMessage().contains("since the transaction read it")
                  ? "a pinned update, at the commit's read"
                  : failure.getMessage().replaceAll("[0-9]{4,}", "#");
    }
    CheckpointRoundTrip.assertEveryWrite(io);
    String placed = before == steps.size() ? "window before the commit"
        : before == 0 ? "window before its write" : "window split";
    return new String[] {outcome,
        (writes == r.writes ? writes : writes + " (" + r.writes + " skipped)") + ", " + placed,
        read.size() == 1 ? "none" : "some", why,
        moves.isEmpty() ? "none" : String.join(", ", new java.util.TreeSet<>(moves))};
  }

  /**
   * Runs {@code steps} of a window, observing each rewrite ({@link TableViews.Lineage}), and adds
   * to {@code moves} how each moved {@code ref}, {@code where} it ran: advanced (to a new
   * snapshot), reset (to one the table had), removed or created; and whether it changed the schema
   * a scan of {@code ref} reads with.
   */
  private static void window(TableScript.Runner racer, List<List<TableScript.Cmd>> steps,
      InMemoryAtomicFileIO io, TableViews.Lineage lineage, String ref, String where,
      List<String> moves) {
    org.apache.iceberg.Table table = Run.reader(io).loadTable(TableScript.ID);
    for (List<TableScript.Cmd> step : steps) {
      org.apache.iceberg.Snapshot was = table.snapshot(ref);
      String schema = TableViews.schemaRead(table, ref);
      java.util.Set<Long> had = new java.util.HashSet<>();
      table.snapshots().forEach(snapshot -> had.add(snapshot.snapshotId()));
      racer.run(step);
      table = Run.reader(io).loadTable(TableScript.ID);
      lineage.observe(table);
      if (!schema.equals(TableViews.schemaRead(table, ref))) {
        moves.add(where + ": schema changed");
      }
      org.apache.iceberg.Snapshot is = table.snapshot(ref);
      if (was == null && is != null) {
        moves.add(where + ": created");
      } else if (was != null && is == null) {
        moves.add(where + ": removed");
      } else if (was != null && was.snapshotId() != is.snapshotId()) {
        moves.add(where + (had.contains(is.snapshotId()) ? ": reset" : ": advanced"));
      }
    }
  }

  private static void check(
      String encoding,
      int formatVersion,
      boolean partitioned,
      LocalCatalogMode mode,
      boolean pinned,
      boolean merging,
      List<TableScript.Cmd> prefix,
      List<TableScript.Cmd> window,
      TableScript.Cmd r) {
    String[] outcome =
        verify(encoding, formatVersion, partitioned, mode, pinned, merging, prefix, window, r);
    Statistics.label("R on a raced table")
        .collect(pinned ? encoding : encoding + ", unpinned", outcome[0]);
    Statistics.label("R").collect(outcome[1]);
    Statistics.label("manifests").collect(outcome[2]);
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

  /** One try ({@code verify} with {@code merging}), R's manifests never merged. */
  static String[] verify(
      String encoding,
      int formatVersion,
      boolean partitioned,
      LocalCatalogMode mode,
      boolean pinned,
      List<TableScript.Cmd> prefix,
      List<TableScript.Cmd> window,
      TableScript.Cmd r) {
    return verify(encoding, formatVersion, partitioned, mode, pinned, false, prefix, window, r);
  }

  /**
   * One try: R on the table the prefix built, the window run just before its first write, in the
   * subject, whose writer pins what Iceberg commits once if {@code pinned}, and in the reference.
   * If {@code merging}, the table merges manifests ({@code commit.manifest.min-count-to-merge} 2,
   * so a merging producer rewrites its branch's manifests into one with its own, whose older
   * entries keep their sequence numbers and row ids), and data commands add one to three files
   * ({@link TableScript.Runner#severalFiles}), so a commit that removes one rewrites a manifest
   * that keeps the others. Returns how R's record went in the subject, R's kind (with " failed"
   * if it failed), and whether it merged and wrote a manifest of older files.
   */
  static String[] verify(
      String encoding,
      int formatVersion,
      boolean partitioned,
      LocalCatalogMode mode,
      boolean pinned,
      boolean merging,
      List<TableScript.Cmd> prefix,
      List<TableScript.Cmd> window,
      TableScript.Cmd r) {
    StringBuilder trace = new StringBuilder(String.format(
        "v%d %s, %s, %s%s%s%nprefix %s%nwindow %s%nR %s%n", formatVersion,
        partitioned ? "partitioned" : "unpartitioned", mode, encoding,
        pinned ? "" : ", R's writer leaves commit-once unpinned",
        merging ? ", merging manifests of several files" : "", prefix, window, r));
    Run subject = Run.of(encoding, mode, formatVersion, partitioned, pinned, merging, prefix,
        window, r, trace);
    Run reference = Run.of(LamportEncoding.NAME, mode, formatVersion, partitioned, true, merging,
        prefix, window, r, new StringBuilder());
    if (r.kind == TableScript.Kind.EXPIRE_SNAPSHOTS && subject.raced && subject.changed
        && subject.conflicts == 0 && subject.failure == null) {
      // merged where it landed: the window, then what R chose on the prefix's table, removed
      reference = Run.expiryReference(mode, formatVersion, partitioned, merging, prefix, window,
          r, trace);
    }

    String outcome;
    if (!subject.raced) {
      outcome = "R wrote nothing";
    } else if (!subject.changed) {
      outcome = "window left the table as R read it";
    } else if (subject.conflicts == 0 && subject.failure == null) {
      outcome = "admitted on a changed table";
      if (reference.failure != null) {
        throw new AssertionError(String.format(
            "the subject admitted R on a changed table, but %s fails it: %s%n%s",
            r.kind == TableScript.Kind.EXPIRE_SNAPSHOTS
                ? "the removal of what it chose, after the window," : "Iceberg's retry",
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

    // Both runs end alike: what the subject admitted is what the retry committed, and what it
    // failed the retry fails as
    if ((subject.failure == null) != (reference.failure == null)
        || (subject.failure != null
            && subject.failure.getClass() != reference.failure.getClass())) {
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
    return new String[] {outcome, r.kind + (subject.failure == null ? "" : " failed"),
        !merging ? "not merging"
            : subject.keptOlderFiles ? "merging, R wrote a manifest of older files"
                : "merging, R wrote no manifest of older files"};
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

  /** The modes whose tables' updates are inline deltas (inline TM or ML), which can merge. */
  @Provide
  Arbitrary<LocalCatalogMode> inlineModes() {
    return Arbitraries.of(
        LocalCatalogMode.fileModes().stream().filter(m -> m.inlineTM || m.inlineML).toList());
  }

  /**
   * Expiries: of all but each branch's last one to three snapshots and every snapshot no ref
   * keeps, mostly with no unused schema or spec removed (which Iceberg retries, a record that
   * removes them having the Lamport witness).
   */
  @Provide
  Arbitrary<TableScript.Cmd> expiries() {
    Arbitrary<Integer> small = Arbitraries.integers().between(0, 11);
    return Combinators.combine(small, small, Arbitraries.integers().between(0, 3))
        .as((a, b, c) -> new TableScript.Cmd(TableScript.Kind.EXPIRE_SNAPSHOTS, a, b,
            c == 0 ? 0 : c * 2 + 1));
  }

  /**
   * Windows of one to three commands that an expiry's witness reads or lets by: data commits, to
   * main or a branch; resets; refs created (on a snapshot the table has) or removed; expiries; any
   * single command.
   */
  @Provide
  Arbitrary<List<TableScript.Cmd>> expiryWindows() {
    return Arbitraries.frequencyOf(
            Tuple.of(4, TableScript.commands(
                kind -> VALIDATED.contains(kind) || APPEND_LIKE.contains(kind))),
            Tuple.of(2, TableScript.commands(RESETS::contains)),
            Tuple.of(2, TableScript.commands(REF_MOVES::contains)),
            Tuple.of(1, TableScript.commands(kind -> kind == TableScript.Kind.EXPIRE_SNAPSHOTS)),
            Tuple.of(2, TableScript.windowCommands(TestRebaseProperties::single)))
        .withoutEdgeCases()
        .list().ofMinSize(1).ofMaxSize(3);
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

  /** Whether a serializable transaction's catalog holds its reads by read witness: mostly. */
  @Provide
  Arbitrary<Boolean> readWitnesses() {
    return Arbitraries.of(true, true, true, false).withoutEdgeCases();
  }

  /**
   * Prefixes of single commands, half the time after appends to each partition, a branch and a
   * tag; a third of the time each, they end as they are, with a snapshot staged on main's head
   * (which a cherry-pick fast-forwards main to), or with a branch fast-forwarded to main's head
   * (which an append to it lets main fast-forward to). A transaction or a stale handle's command is
   * left out: a rewrite whose parent an expiry removed in the same commit would hide the rows it
   * kept ({@link TableViews.Lineage}).
   */
  @Provide
  Arbitrary<List<TableScript.Cmd>> readPrefixes() {
    Arbitrary<Integer> small = Arbitraries.integers().between(0, 11);
    Arbitrary<List<TableScript.Cmd>> any = TableScript.commands(TestRebaseProperties::single)
        .withoutEdgeCases().list().ofMinSize(1).ofMaxSize(12);
    Arbitrary<List<TableScript.Cmd>> seeded = Combinators.combine(small, small,
            TableScript.commands(TestRebaseProperties::single).withoutEdgeCases().list()
                .ofMaxSize(8))
        .as((b, c, rest) -> {
          List<TableScript.Cmd> all = new ArrayList<>();
          for (int p = 0; p < 4; p++) {
            all.add(new TableScript.Cmd(TableScript.Kind.FAST_APPEND, p, b, p));
          }
          all.add(new TableScript.Cmd(TableScript.Kind.CREATE_BRANCH, 0, b, c));
          all.add(new TableScript.Cmd(TableScript.Kind.CREATE_TAG, 0, b, c));
          all.addAll(rest);
          return all;
        });
    Arbitrary<List<TableScript.Cmd>> staged = Combinators.combine(small, small, small)
        .as((a, b, c) -> List.of(new TableScript.Cmd(TableScript.Kind.STAGED_APPEND, a, b, c)));
    Arbitrary<List<TableScript.Cmd>> atTheHead = Combinators.combine(small, small, small, small)
        .as((a, b, c, d) -> List.of(new TableScript.Cmd(TableScript.Kind.CREATE_BRANCH, a, b, c),
            new TableScript.Cmd(TableScript.Kind.FAST_FORWARD, d, b, c * 2 + 1)));
    return Combinators.combine(Arbitraries.oneOf(any, seeded),
            Arbitraries.oneOf(Arbitraries.just(List.<TableScript.Cmd>of()), staged, atTheHead))
        .as((prefix, end) -> {
          List<TableScript.Cmd> all = new ArrayList<>(prefix);
          all.addAll(end);
          return all;
        });
  }

  /**
   * Windows of data commits (to main or a branch, in any partition), of the changes the guards
   * read (resets, a ref's removal, expiries, spec and table changes), of resets (rollbacks,
   * fast-forwards, cherry-picks), of a ref removed or created, of expiries of all but each
   * branch's head, of schema changes (which change what a branch's scan reads, and leave the
   * branch where it was), and of any single command. A sixth of them are a rollback alone: a ref
   * reset with nothing that advances it or changes the schema, which only the reset stamps of a
   * read witness see (calibration set reads' readnoreset needs these, and the rest of the windows
   * rarely make one).
   */
  @Provide
  Arbitrary<List<TableScript.Cmd>> readWindows() {
    Arbitrary<Integer> small = Arbitraries.integers().between(0, 11);
    Arbitrary<TableScript.Cmd> headsOnly = Combinators.combine(small, small)
        .as((a, c) -> new TableScript.Cmd(TableScript.Kind.EXPIRE_SNAPSHOTS, a, 0, c * 2 + 1));
    Arbitrary<List<TableScript.Cmd>> mixed = Arbitraries.frequencyOf(
            Tuple.of(4, TableScript.commands(
                kind -> VALIDATED.contains(kind) || APPEND_LIKE.contains(kind))),
            Tuple.of(2, TableScript.commands(GUARDS::contains)),
            Tuple.of(3, TableScript.commands(RESETS::contains)),
            Tuple.of(1, TableScript.commands(REF_MOVES::contains)),
            Tuple.of(1, headsOnly),
            Tuple.of(1, TableScript.commands(SCHEMA_CHANGES::contains)),
            Tuple.of(3, TableScript.windowCommands(TestRebaseProperties::single)))
        .withoutEdgeCases()
        .list().ofMinSize(1).ofMaxSize(4);
    Arbitrary<List<TableScript.Cmd>> rollback =
        TableScript.commands(kind -> kind == TableScript.Kind.ROLLBACK).withoutEdgeCases()
            .list().ofSize(1);
    return Arbitraries.frequencyOf(Tuple.of(5, mixed), Tuple.of(1, rollback));
  }

  /** Commands that change a table's schema: a column added, dropped, renamed or promoted. */
  private static final java.util.Set<TableScript.Kind> SCHEMA_CHANGES =
      java.util.EnumSet.of(TableScript.Kind.ADD_COLUMN, TableScript.Kind.DELETE_COLUMN,
          TableScript.Kind.RENAME_COLUMN, TableScript.Kind.UPDATE_COLUMN);

  /** Commands that move a ref to a snapshot the table has (or, a cherry-pick, to a new one). */
  private static final java.util.Set<TableScript.Kind> RESETS =
      java.util.EnumSet.of(TableScript.Kind.ROLLBACK, TableScript.Kind.FAST_FORWARD,
          TableScript.Kind.CHERRY_PICK);

  /** Commands that remove or create a ref. */
  private static final java.util.Set<TableScript.Kind> REF_MOVES =
      java.util.EnumSet.of(TableScript.Kind.REMOVE_REF, TableScript.Kind.CREATE_BRANCH,
          TableScript.Kind.CREATE_TAG);

  /**
   * What R reads (a ref, a filter on {@code cat}, case sensitive or not) and what it writes: half
   * the time an append to U, which leaves its read to the read witness where the record lands.
   */
  @Provide
  Arbitrary<Reader> readers() {
    Arbitrary<Writes> writes = Arbitraries.frequency(Tuple.of(1, Writes.NOTHING),
        Tuple.of(2, Writes.OTHER_TABLE), Tuple.of(1, Writes.SAME_TABLE));
    return Arbitraries.of(true, true, true, false).flatMap(caseSensitive -> Combinators.combine(
            Arbitraries.integers().between(0, 7), catFilters(caseSensitive ? "cat" : "CAT"),
            writes, Arbitraries.integers().between(0, 3))
        .as((ref, filter, written, partition) ->
            new Reader(ref, filter, caseSensitive, written, partition)))
        .withoutEdgeCases();
  }

  /**
   * Row filters on the column {@code name} names, {@code cat}: everything, one partition's rows or
   * several's (by value, by a set, by a range, by a bucket or truncation of {@code cat}, which no
   * spec partitions by), or none (a value no row has, null).
   */
  private static Arbitrary<Expression> catFilters(String name) {
    Arbitrary<Integer> value = Arbitraries.integers().between(1, 5);
    return Arbitraries.frequencyOf(
        Tuple.of(2, Arbitraries.just(Expressions.alwaysTrue()).map(Expression.class::cast)),
        Tuple.of(5, value.map(v -> (Expression) Expressions.equal(name, v))),
        Tuple.of(2, Combinators.combine(value, value)
            .as((v, w) -> (Expression) Expressions.in(name, v, w))),
        Tuple.of(1, value.map(v -> (Expression) Expressions.notEqual(name, v))),
        Tuple.of(1, value.map(v -> (Expression) Expressions.lessThan(name, v))),
        Tuple.of(1, value.map(v -> (Expression) Expressions.greaterThanOrEqual(name, v))),
        Tuple.of(2, Arbitraries.integers().between(0, 1)
            .map(b -> (Expression) Expressions.equal(Expressions.<Integer>bucket(name, 2), b))),
        Tuple.of(1, Arbitraries.of(0, 2, 4)
            .map(w -> (Expression) Expressions.equal(Expressions.<Integer>truncate(name, 2), w))),
        Tuple.of(1, Arbitraries.just(Expressions.isNull(name)).map(Expression.class::cast)));
  }

  /**
   * Where the window runs, if R writes: before R's commit reads the catalog (which checks R's
   * read), just before R's record is written (where replay judges its witness or pin), or its
   * first half before the commit and the rest before the write.
   */
  enum Placement {
    BEFORE_THE_COMMIT,
    BEFORE_THE_WRITE,
    SPLIT
  }

  /** Where the window runs: half the time, all of it just before R's record is written. */
  @Provide
  Arbitrary<Placement> placements() {
    return Arbitraries.frequency(Tuple.of(1, Placement.BEFORE_THE_COMMIT),
            Tuple.of(2, Placement.BEFORE_THE_WRITE), Tuple.of(1, Placement.SPLIT))
        .withoutEdgeCases();
  }

  /** What R writes besides its read: nothing, an append to U, or an append to T's main. */
  enum Writes {
    NOTHING,
    OTHER_TABLE,
    SAME_TABLE
  }

  /** R of {@link #anAdmittedReadSeesTheRowsItRead}. */
  static final class Reader {
    final int ref; // main below 4 (or if T has no other ref), else the ref-th of the others
    final Expression filter;
    final boolean caseSensitive;
    final Writes writes;
    final int partition; // of the file R appends

    Reader(int ref, Expression filter, boolean caseSensitive,
        Writes writes, int partition) {
      this.ref = ref;
      this.filter = filter;
      this.caseSensitive = caseSensitive;
      this.writes = writes;
      this.partition = partition;
    }

    @Override
    public String toString() {
      return String.format("scan(ref %d, %s%s), writes %s%s", ref, filter,
          caseSensitive ? "" : ", case insensitive", writes,
          writes == Writes.NOTHING ? "" : " in partition " + partition);
    }
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
    // whether a snapshot R added wrote a manifest that keeps files of earlier commits
    final boolean keptOlderFiles;

    private Run(InMemoryAtomicFileIO io, RuntimeException failure, boolean raced,
        boolean changed, long conflicts, Map<String, String> view, boolean keptOlderFiles) {
      this.io = io;
      this.failure = failure;
      this.raced = raced;
      this.changed = changed;
      this.conflicts = conflicts;
      this.view = view;
      this.keptOlderFiles = keptOlderFiles;
    }

    static Run of(String encoding, LocalCatalogMode mode, int formatVersion, boolean partitioned,
        boolean pinned, boolean merging, List<TableScript.Cmd> prefix,
        List<TableScript.Cmd> window, TableScript.Cmd r, StringBuilder trace) {
      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      Map<String, String> properties = properties(merging);
      properties.put(LogCatalogProperties.CONFLICT_ENCODING, encoding);
      LogCatalog catalog = mode.catalog(io, WAREHOUSE, "c", properties);
      if (!pinned) {
        catalog.leavingCommitOnceUnpinned();
      }
      catalog.createNamespace(Namespace.of("ns"));
      TableScript.Runner writer = new TableScript.Runner(catalog, formatVersion, partitioned,
          trace).severalFiles(merging);
      for (List<TableScript.Cmd> step : TableScript.steps(prefix)) {
        writer.run(step);
      }
      TableScript.Runner racer =
          new TableScript.Runner(mode.catalog(io, WAREHOUSE, "c", PROPERTIES), "w", trace)
              .severalFiles(merging);
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
              beforeR[0].snapshots().size(), beforeR[0].nextRowId()),
          keptOlderFiles(io, beforeR[0]));
    }

    /**
     * The properties of a run's first catalog, which creates the tables: tables that merge their
     * manifests, if {@code merging}.
     */
    private static Map<String, String> properties(boolean merging) {
      Map<String, String> properties = new java.util.HashMap<>(PROPERTIES);
      if (merging) {
        properties.put(
            CatalogProperties.TABLE_DEFAULT_PREFIX + TableProperties.MANIFEST_MIN_MERGE_COUNT,
            "2");
      }
      return properties;
    }

    /**
     * Whether a snapshot the table has now, and not {@code before}, wrote a manifest that keeps
     * files of earlier commits (a merged one, or one it rewrote to remove files).
     */
    private static boolean keptOlderFiles(InMemoryAtomicFileIO io, TableMetadata before) {
      java.util.Set<Long> had = new java.util.HashSet<>();
      before.snapshots().forEach(s -> had.add(s.snapshotId()));
      org.apache.iceberg.Table table = reader(io).loadTable(TableScript.ID);
      for (org.apache.iceberg.Snapshot s : table.snapshots()) {
        if (had.contains(s.snapshotId())) {
          continue;
        }
        for (org.apache.iceberg.ManifestFile m : s.allManifests(table.io())) {
          if (java.util.Objects.equals(m.snapshotId(), s.snapshotId()) && m.hasExistingFiles()) {
            return true;
          }
        }
      }
      return false;
    }

    /**
     * The reference for an expiry merged onto a changed table: the prefix and the window, run
     * serially, then a removal of exactly the snapshots R removes when it runs right after the
     * prefix (matched by sequence number, which the prefix numbers alike in every run), by
     * Iceberg's builder, as R's record removes them where it lands. Fails where a ref names one
     * of them, as applying Iceberg's remove-snapshots update does (C22's fix; the builder's
     * removeSnapshots would drop the ref with it).
     */
    static Run expiryReference(LocalCatalogMode mode, int formatVersion, boolean partitioned,
        boolean merging, List<TableScript.Cmd> prefix, List<TableScript.Cmd> window,
        TableScript.Cmd r, StringBuilder trace) {
      // What R chooses on the prefix's table
      InMemoryAtomicFileIO scratch = new InMemoryAtomicFileIO();
      LogCatalog chooser = mode.catalog(scratch, WAREHOUSE, "c", properties(merging));
      chooser.createNamespace(Namespace.of("ns"));
      TableScript.Runner chooses = new TableScript.Runner(chooser, formatVersion, partitioned,
          new StringBuilder()).severalFiles(merging);
      for (List<TableScript.Cmd> step : TableScript.steps(prefix)) {
        chooses.run(step);
      }
      java.util.Set<Long> removed = sequenceNumbers(current(scratch));
      chooses.run(List.of(r));
      removed.removeAll(sequenceNumbers(current(scratch)));

      InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
      Map<String, String> properties = properties(merging);
      properties.put(LogCatalogProperties.CONFLICT_ENCODING, LamportEncoding.NAME);
      LogCatalog catalog = mode.catalog(io, WAREHOUSE, "c", properties);
      catalog.createNamespace(Namespace.of("ns"));
      TableScript.Runner writer = new TableScript.Runner(catalog, formatVersion, partitioned,
          new StringBuilder()).severalFiles(merging);
      for (List<TableScript.Cmd> step : TableScript.steps(prefix)) {
        writer.run(step);
      }
      TableScript.Runner racer =
          new TableScript.Runner(mode.catalog(io, WAREHOUSE, "c", PROPERTIES), "w",
              new StringBuilder()).severalFiles(merging);
      for (List<TableScript.Cmd> step : TableScript.steps(window)) {
        racer.run(step);
      }
      TableMetadata beforeR = current(io);
      org.apache.iceberg.TableOperations ops =
          ((HasTableOperations) catalog.loadTable(TableScript.ID)).operations();
      TableMetadata base = ops.refresh();
      List<Long> ids = new ArrayList<>();
      base.snapshots().forEach(s -> {
        if (removed.contains(s.sequenceNumber())) {
          ids.add(s.snapshotId());
        }
      });
      trace.append(String.format(
          "expiry reference: R chose the snapshots of sequence numbers %s; after the window, %s%n",
          new java.util.TreeSet<>(removed), ids.isEmpty() ? "none is left" : "removes " + ids));
      RuntimeException failure = null;
      for (Map.Entry<String, org.apache.iceberg.SnapshotRef> ref : base.refs().entrySet()) {
        if (failure == null && ids.contains(ref.getValue().snapshotId())) {
          failure = new IllegalStateException(String.format(
              "ref %s names snapshot %s, which R chose", ref.getKey(),
              ref.getValue().snapshotId()));
        }
      }
      try {
        if (failure == null && !ids.isEmpty()) {
          ops.commit(base, TableMetadata.buildFrom(base).removeSnapshots(ids).build());
        }
      } catch (RuntimeException e) {
        failure = e;
      }
      return new Run(io, failure, true, true, 0,
          TableViews.logical(reader(io).loadTable(TableScript.ID),
              beforeR.snapshots().size(), beforeR.nextRowId()), false);
    }

    private static java.util.Set<Long> sequenceNumbers(TableMetadata metadata) {
      java.util.Set<Long> numbers = new java.util.HashSet<>();
      metadata.snapshots().forEach(s -> numbers.add(s.sequenceNumber()));
      return numbers;
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
