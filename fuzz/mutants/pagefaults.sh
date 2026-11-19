# Calibration of the fault fuzzer's reach into the log's pages (the 2026-11-17 review's A4 items 5-7
# and B4; the gap plan's items 2.1 and 2.2; anchors as of its commit): plain-request faults (page
# puts, other puts, deletes, listings), sweeps from stale replicas (also between a compactor's page
# puts and its root), the drain, and refs and expiries aimed at the sealing line. Each mutant must
# fail an oracle. They need sealed blocks: run the set at SNAPSHOT_BLOCK=2 SNAPSHOT_WINDOW=1.
# Tries below are of the fault fuzzer's two properties at 2/1, faultyStorageNeverMisreportsACommit's
# first.
#
# pageexists fails at tries 182 and 123: two compactors put one page (pages are content
# addressed), or one puts again a page its lost reply put, and the second put fails: a fused
# commit fails with no fault injected, and the drain's compaction too. putignored fails at try
# 71, faultyStorageNeverMisreportsACommit only (puts fail only there): a root names a page that
# does not exist. sweepstale fails at tries 11 and 21: a sweep from a writer's stale replica, at
# the write after a page put, deletes the page the root then names. dropforced fails at try 791,
# faultyStorageNeverMisreportsACommit only: a kept drop whose metadata file's put failed lands, and
# no metadata file holds the table's last version (a lost reply leaves the file, and a fault on
# its lists, on Iceberg's worker threads, never fires, so few faults reach it).
MUTANTS="pageexists putignored sweepstale dropforced"
EQUIVALENT=""
ORACLES=TestCatalogFaultProperties

prepare() {
  # 8,000: dropforced fails at try 791 (C8: ten times the margin)
  tries $1/$T/TestCatalogFaultProperties.java each 8000
}

RT=intentlog-core/src/main/java/org/intentlog/LogRuntime.java
AFL=intentlog-fileio/src/main/java/org/intentlog/fileio/AtomicFileLog.java

mutant() { # name main-dir test-dir
  local M=$2
  case $1 in
    # a page put that finds its page (its own earlier put, whose reply was lost) fails: the page is
    # put again at every compaction, and none lands (Bug 116's put by conditional create, as if
    # "exists" were a failure)
    pageexists) mutate $AFL "      } catch (SupportsAtomicOperations.CASException e) {
        // the page exists: these bytes, put by another writer or by an earlier put of this one
      } catch (IOException e) {" "      } catch (SupportsAtomicOperations.CASException e) {
        throw e;
      } catch (IOException e) {" ;;
    # a state page whose put failed is taken as put: the root names a page the store lacks
    putignored) mutate $RT "    for (Map.Entry<String, byte[]> page : pages.entrySet()) {
      store.put(page.getKey(), page.getValue());
    }" "    for (Map.Entry<String, byte[]> page : pages.entrySet()) {
      try {
        store.put(page.getKey(), page.getValue());
      } catch (RuntimeException e) {
        // taken as put
      }
    }" ;;
    # TLA+ sweepstale: a sweep takes a page sealed by a record after its replica for garbage, and
    # deletes the page a compactor ahead of it put for its root
    sweepstale) mutate $M/CatalogStateMachine.java "      if (page[1] > table.version) {
        continue; // sealed by a record after this state
      }" "" ;;
    # a drop that keeps an inline table's files goes on when its write of the table's metadata file
    # failed (as fileio.catalog.drop.force does): the table cannot be registered again
    dropforced) mutate $M/LogCatalog.java "    } catch (NoSuchTableException | CommitFailedException e) {
      throw e; // the table changed since the read, which the drop reads again
    } catch (RuntimeException e) {
      if (!LogCatalogProperties.dropForce(catalogProperties)) {
        throw e;
      }" "    } catch (NoSuchTableException | CommitFailedException e) {
      throw e; // the table changed since the read, which the drop reads again
    } catch (RuntimeException e) {
      if (false) {
        throw e;
      }" ;;
  esac
}
