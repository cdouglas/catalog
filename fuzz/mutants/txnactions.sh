# Calibration of how replay judges a record of several actions (the 2026-11-17 review's C3, gap
# plan 2.6): an intention applies only if every action verifies on the state before it. The
# mutant judges its first action alone, so a catalog transaction whose later table update (or
# drop, rename, create) conflicts lands anyway. The rebase oracle's R is one table's record (a
# table transaction is one action); the fuzzers that race catalog transactions across tables are
# the fault fuzzer (transactions preempted by another writer's commit) and the race fuzzer
# (CATALOG_TXN racing a fourth writer's operation). Run at SNAPSHOT_BLOCK=2 SNAPSHOT_WINDOW=1.
MUTANTS="firstactiononly"
EQUIVALENT=""
ORACLES=TestCatalogFaultProperties,TestCatalogRaceProperties

prepare() {
  tries $1/$T/TestCatalogFaultProperties.java each 5000
  tries $1/$T/TestCatalogRaceProperties.java 20000 2000
}

mutant() { # name main-dir test-dir
  local M=$2
  case $1 in
    # an intention verifies by its first action only
    firstactiononly) mutate $M/ProtoCodec.java "      if (!selfConsistent(bound)) {
        return false;
      }
      for (Action action : actions) {" "      if (!selfConsistent(bound)) {
        return false;
      }
      for (Action action : actions.subList(0, Math.min(1, actions.size()))) {" ;;
  esac
}
