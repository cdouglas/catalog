# Calibration of the invariants added after the record-format-2 and replay-costs rounds: the
# inline manifest lists' structure (CheckpointRoundTrip.malformedLists), the numbering a commit or
# replay's merge assigns (TestCatalogFaultProperties.checkNumbering), a fresh reader's loads
# against a reader that reuses what it made, and the metadata log's bound, which the fuzzers now
# change (anchors as of the commit that added this set; poolorphan's as of the pool's dropPooled,
# which the expiry's two ways of finding the entries to drop share): each mutant must fail an
# oracle.
MUTANTS="poolorphan seqnoshift windowfloor"
ORACLES=TestCatalogFaultProperties,TestInlineDifferentialProperties

prepare() {
  tries $1/$T/TestCatalogFaultProperties.java each 1500
  tries $1/$T/TestInlineDifferentialProperties.java 600
}

mutant() { # name main-dir test-dir
  local M=$2
  case $1 in
    # an expiry drops a snapshot's list but leaves the pool entries no list names any more, by
    # either way it finds them (from the lists it removed, or from the lists kept: both drop
    # them through dropPooled)
    poolorphan) mutate $M/CatalogState.java "      for (String path : paths) {
        pool = pool.minus(path);
        ordinals = ordinals.minus(path);
      }" "      for (String path : paths) {
      }" ;;
    # a merged record's own manifests keep the sequence number its writer gave them
    seqnoshift) mutate $M/Landing.java "      if (sequenceNumber > baseLastSequenceNumber) {
        sequenceNumber += sequenceShift;
      }" "" ;;
    # replay's metadata log keeps two entries at a bound of 0 or 1, where Iceberg keeps one
    windowfloor) mutate $M/Window.java "    int keep = Math.min(to - from, Math.max(1, bound) - 1);" "    int keep = Math.min(to - from, Math.max(2, bound) - 1);" ;;
  esac
}
