# Calibration of the log SPI's contract (SharedLogContract; docs/LOG.md "Obligations"): each
# mutant breaks the condition a provider's conditional write lands on, and must fail a provider's
# contract test. P4: a conditional write lands only if the log's tail is the one it names when it
# lands, whatever moved it since, an append or a replace. casoverappend is a replace that lands on a
# tail an append moved, as compaction after the commit would on the version its commit landed in
# when another writer's append extended it; appendoverreplace is an append that lands on a tail a
# replace moved. Both are InMemoryAtomicFileIO's generation check, which AtomicFileLog's conditional
# writes rest on (its CAS replace and its append at an expected tail); the segmented log's catalog
# object is an AtomicFileLog too, but its checkpoint replaces the object a fresh read found, and
# the SPI gives it neither conditional appends nor a fused replace, as it gives InMemorySharedLog no
# replace: only AtomicFileLog's contract runs the race. Locally (2026-11-18) each mutant passed the
# contract before it raced an append against a replace (its two-appends and two-replaces tests
# cannot tell either from a correct store), and now fails TestAtomicFileLogContract$WithAppend:
# casoverappend aReplaceOfATailAnAppendMovedLoses, appendoverreplace
# anAppendToATailAReplaceMovedLoses, each at all three tail sources (a probe's, a read's, a read
# through a write's). Of the fuzzers, TestAtomicFileCommitProtocol catches casoverappend (try 16
# of 5,000); TestSegmentedCommitProtocol misses it in 2,000 tries (a checkpoint replacing the
# catalog object over an Install).
MUTANTS="casoverappend appendoverreplace"
EQUIVALENT=""
ORACLES=TestAtomicFileLogContract,TestInMemorySharedLogContract,TestSegmentedLogContract

IO=intentlog-fileio/src/test/java/org/apache/iceberg/io/InMemoryAtomicFileIO.java

prepare() {
  : # the contract's tests are examples, not properties: no tries to set
}

mutant() { # name main-dir test-dir (the catalog's; these mutants are intentlog's)
  case $1 in
    # a CAS replace lands on an object appended to since the version it names: the version it
    # names, extended
    casoverappend) mutate $IO "        if (expectedGen != liveGen) {" "        if (expectedGen != liveGen && !(strategy == Strategy.CAS && expected != null
            && live != null && live.segments > expected.segments)) {" ;;
    # an append lands at the end of an object a CAS replaced since the version it names
    appendoverreplace) mutate $IO "        if (expectedGen != liveGen) {" "        if (expectedGen != liveGen && !(strategy == Strategy.APPEND && expected != null
            && live != null && live.segments == 1)) {" ;;
  esac
}
