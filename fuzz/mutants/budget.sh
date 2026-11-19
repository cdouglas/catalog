# Calibration of the commit-protocol fuzzers' fault budget (the 2026-11-17 review's B5 and B6, the
# gap plan's item 2.4): each mutant must fail one of the three fuzzers. Faults met only a commit's
# first two writes and one of its first three reads; standalone compactions, sweeps, lookups and
# a preempting commit met none; the segmented log's instances took requests in turn, at a
# retirement TTL of 0 on a clock that never moved.
MUTANTS="throttlereject dropundeleted expiredalways casoverappend"
EQUIVALENT=""
ORACLES=TestMemoryCommitProtocol,TestAtomicFileCommitProtocol,TestSegmentedCommitProtocol

RT=intentlog-core/src/main/java/org/intentlog/LogRuntime.java
SL=intentlog-fileio/src/main/java/org/intentlog/fileio/SegmentedLog.java
IO=intentlog-fileio/src/test/java/org/apache/iceberg/io/InMemoryAtomicFileIO.java

prepare() {
  tries $1/intentlog-memory/src/test/java/org/intentlog/memory/TestMemoryCommitProtocol.java 300000
  tries $1/intentlog-fileio/src/test/java/org/intentlog/fileio/TestAtomicFileCommitProtocol.java 300000
  tries $1/intentlog-fileio/src/test/java/org/intentlog/fileio/TestSegmentedCommitProtocol.java 100000
}

mutant() { # name main-dir test-dir (the catalog's; these mutants are intentlog's)
  case $1 in
    # a write the store throttled on every attempt is rejected, though a throttled send of it may
    # have landed: its outcome is unknown
    throttlereject) mutate $RT "      throw new AmbiguousWrite(
          new IOException(
              String.format(\"Storage throttled %d attempts\", config.maxThrottleAttempts()),
              lastThrottle));" "      throw new CommitRejectedException(CommitRejectedException.Reason.EXHAUSTED, null,
          \"Storage throttled every attempt\", lastThrottle);" ;;
    # a checkpoint drops the Retired entry of a segment whose TTL passed though its delete failed:
    # the entry was the only reference to it, so it leaks
    dropundeleted) mutate $SL "        if (!expired(old, now) || !deleteRetired(c, old, deleted)) {
          records.add(LogEnvelope.encodeRetiredSegment(old));
        }" "        if (!expired(old, now)) {
          records.add(LogEnvelope.encodeRetiredSegment(old));
        } else {
          deleteRetired(c, old, deleted);
        }" ;;
    # a retired segment is deleted before its TTL passed, under a reader still on it
    expiredalways) mutate $SL "    return r.retiredAtMs() + options.retireTtlMs() <= now;" "    return true;" ;;
    # contract.sh's: a CAS replace lands on an object appended to since the version it names (the
    # segmented log's catalog object takes Installs by conditional append)
    casoverappend) mutate $IO "        if (expectedGen != liveGen) {" "        if (expectedGen != liveGen && !(strategy == Strategy.CAS && expected != null
            && live != null && live.segments > expected.segments)) {" ;;
  esac
}
