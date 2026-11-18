# Calibration of drops (anchors as of fileio 6a4c762): each mutant must fail an oracle.
# dropnowrite writes nothing before a drop that keeps an inline table's files (errata T4: the
# dropped table cannot be registered); dropnodelete leaves what a drop that lost its race wrote
# (stray metadata and list files beside the ones its retry writes); purgeafter reads the history
# of the version a purge drops only after the drop lands (Bug 94: a compaction after the drop
# collects the table's pages, and the purge leaves every file). purgeafter needs sealed pages:
# run the set at SNAPSHOT_BLOCK=2 SNAPSHOT_WINDOW=1, where the fault fuzzer's purge check, besides
# the regression, should catch it.
MUTANTS="dropnowrite dropnodelete purgeafter"
EQUIVALENT=""
ORACLES=TestCatalogRegressions,TestCatalogRaceProperties,TestCatalogFaultProperties

prepare() {
  tries $1/$T/TestCatalogRaceProperties.java 20000 1000
  tries $1/$T/TestCatalogFaultProperties.java each 1500
}

mutant() { # name main-dir test-dir
  local M=$2
  case $1 in
    # the drop writes nothing before its record
    dropnowrite) mutate $M/LogCatalog.java "      return tableOpsAsRead(identifier, read).materialize(written);" "      return null;" ;;
    # what a drop that did not land wrote stays
    dropnodelete) mutate $M/LogCatalog.java "      } catch (RuntimeException e) {
        cleanup.delete(mut.abandonedFiles());
        if (e instanceof NoSuchTableException) {" "      } catch (RuntimeException e) {
        if (e instanceof NoSuchTableException) {" ;;
    # the purge reads the dropped version's history after the drop lands
    purgeafter) mutate $M/LogCatalog.java \
      "      version.snapshots(); // now, while the catalog names their pages" "" ;;
  esac
}
