# Calibration of the invariants added after the record-format-2 and replay-costs rounds: the
# inline manifest lists' structure (CheckpointRoundTrip.malformedLists), the numbering a commit or
# replay's merge assigns (TestCatalogFaultProperties.checkNumbering), a fresh reader's loads
# against a reader that reuses what it made, and the metadata log's bound, which the fuzzers now
# change (anchors as of the commit that added this set): each mutant must fail an oracle.
MUTANTS="poolorphan seqnoshift reusestale windowfloor"
ORACLES=TestCatalogFaultProperties,TestInlineDifferentialProperties

prepare() {
  tries $1/$T/TestCatalogFaultProperties.java 1500
  tries $1/$T/TestInlineDifferentialProperties.java 600
}

mutant() { # name main-dir test-dir
  local M=$2
  case $1 in
    # an expiry drops a snapshot's list but leaves the pool entries no list names any more
    poolorphan) mutate $M/CatalogState.java "          pool = pool.minus(path);" "" ;;
    # a merged record's own manifests keep the sequence number its writer gave them
    seqnoshift) mutate $M/Landing.java "      if (sequenceNumber > baseLastSequenceNumber) {
        sequenceNumber += sequenceShift;
      }" "" ;;
    # a load reuses the InlineSnapshots it made after a pool entry was replaced for their path
    reusestale) mutate $M/LogCatalog.java "        if (replacements != table.poolReplacements
            || table.copies.size() > 2 * pool.size() + 64) {" "        if (table.copies.size() > 2 * pool.size() + 64) {" ;;
    # replay's metadata log keeps two entries at a bound of 0 or 1, where Iceberg keeps one
    windowfloor) mutate $M/Window.java "    int keep = Math.min(to - from, Math.max(1, bound) - 1);" "    int keep = Math.min(to - from, Math.max(2, bound) - 1);" ;;
  esac
}
