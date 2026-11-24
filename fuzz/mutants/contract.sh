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
#
# Pages (the 2026-11-17 review's API4): pageplain puts a page with a plain write, which rewrites it
# in place (Bug 116 (zonal page re-put not atomic)), not a conditional create that writes nothing
# when the page is there. pagesAreStoredByName passed it (2026-11-20); aPageThereIsNotWrittenAgain
# and twoWritersPutOnePageAtOnce fail it on both of TestAtomicFileLogContract's logs, counting the
# store's landed writes.
#
# A-B-A (API5): fixedinc gives every replace the same incarnation, so a replace with the same
# checkpoint and records writes an earlier version's bytes, which an S3 general-purpose bucket's
# ETag (a hash of them) cannot tell apart. TestAtomicFileLog's
# aReplaceNeverWritesAnEarlierVersionsBytes alone fails it (2026-11-20).
#
# The store's contract (API5, the fork's SupportsAtomicOperationsContractTest, which
# TestInMemoryAtomicFileIOContract runs on this store): casoverappend also fails its
# casFromAVersionAnAppendExtendedFails and casAndAppendRaceConcurrent (fork atomicio 0d7d213f4e;
# locally 2026-11-24). The append limit (maxAppendBytes; the suite's CappedAppends caps it at 1 KiB):
# appendlimitlost reports an append past it as a lost race, which a caller sends again as it is;
# appendlimitlands lands an append a byte past it; appendlimitcas caps a CAS too.
# appendPastMaxAppendBytesIsRefusedAndLandsNothing fails the first two, and the suite's CASes of
# more than the cap the third (2026-11-24).
MUTANTS="casoverappend appendoverreplace pageplain fixedinc appendlimitlost appendlimitlands appendlimitcas"
EQUIVALENT=""
ORACLES=TestAtomicFileLogContract,TestInMemorySharedLogContract,TestSegmentedLogContract,TestAtomicFileLog,TestInMemoryAtomicFileIOContract*

IO=intentlog-fileio/src/test/java/org/apache/iceberg/io/InMemoryAtomicFileIO.java
FILELOG=intentlog-fileio/src/main/java/org/intentlog/fileio/AtomicFileLog.java

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
    # every replace draws the same incarnation
    fixedinc) mutate $FILELOG "    this(io, location, () -> ThreadLocalRandom.current().nextLong(), lookupFirst);" "    this(io, location, () -> 42L, lookupFirst);" ;;
    # an append past the store's limit fails as a lost race, not refused
    appendlimitlost) mutate $IO "              data.length, location, appendByteLimit);
          throw new StorageRefusedException(msg, new IOException(msg));" "              data.length, location, appendByteLimit);
          throw new SupportsAtomicOperations.AppendException(msg, new IOException(msg));" ;;
    # an append a byte past the store's limit lands
    appendlimitlands) mutate $IO "        if (strategy == Strategy.APPEND && appendByteLimit > 0 && data.length > appendByteLimit) {" "        if (strategy == Strategy.APPEND && appendByteLimit > 0 && data.length > appendByteLimit + 1) {" ;;
    # the limit on an append's bytes caps a CAS too
    appendlimitcas) mutate $IO "        if (strategy == Strategy.APPEND && appendByteLimit > 0 && data.length > appendByteLimit) {" "        if (appendByteLimit > 0 && data.length > appendByteLimit) {" ;;
    # a page put rewrites the page in place, a plain write
    pageplain) mutate $FILELOG "        AtomicOutputFile out = io.newOutputFile(io.newInputFile(prefix + name));
        CAS token = out.prepare(() -> new ByteArrayInputStream(page), AtomicOutputFile.Strategy.CAS);
        out.writeAtomic(token, () -> new ByteArrayInputStream(page));" "        try (java.io.OutputStream plain = io.newOutputFile(prefix + name).createOrOverwrite()) {
          plain.write(page);
        }" ;;
  esac
}
