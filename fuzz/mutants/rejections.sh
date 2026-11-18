# Calibration of justified rejections and progress (the 2026-11-17 review's A5 and B3; the gap
# plan's item 1.1; anchors as of its commit): each mutant must fail an oracle. raceconflict takes
# every lost replace race for a conflict, whether or not the transaction's reads hold on the log
# it lost to: at the catalog Iceberg's retry hides it (a client retry per lost race), so only the
# commit-protocol fuzzers' rule that a conflict's reads fail on the log after it catches it, on
# one object (TestAtomicFileCommitProtocol, at try 2; a shared log loses append races, not
# replace ones, and TestMemoryCommitProtocol passed 30,000 tries). The same mistake on an
# ambiguous write's path the drain oracle already caught (try 32: a delayed copy of the
# "rejected" record applies). stagepinned stages every update of a catalog transaction pinned to
# the version it read, never judged by the conflict encoding (errata D12): an append-only
# transaction of a writer that inlines, on an inline table another writer committed to since,
# fails before it sends a record (the fault fuzzer's progress rule, at tries 128 and 494 of
# 1,500; it passed 1,500 of each before).
#
# Not in the set until the fuzzer reaches it (the gap plan's 2.1): the review's third mutant, a
# handle whose refresh keeps its stale base after a conflict for a sealed snapshot. Iceberg's
# retrying producers refresh before each attempt, so a handle's commit reaches the catalog W
# behind only if a race between its refresh and its write seals its parent: no "sealed:" conflict
# occurred in 3,000 tries at SNAPSHOT_BLOCK=2 SNAPSHOT_WINDOW=1, with or without the mutant
# (staleSealed set by a "sealed:" rejection in doCommit's catch, read in doRefresh).
MUTANTS="raceconflict stagepinned"
EQUIVALENT=""
ORACLES=TestMemoryCommitProtocol,TestAtomicFileCommitProtocol,TestCatalogFaultProperties

RT=intentlog-core/src/main/java/org/intentlog/LogRuntime.java

prepare() {
  tries $1/intentlog-memory/src/test/java/org/intentlog/memory/TestMemoryCommitProtocol.java 30000
  tries $1/intentlog-fileio/src/test/java/org/intentlog/fileio/TestAtomicFileCommitProtocol.java 30000
  # 5,000: stagepinned failed the fault fuzzer at try 494 of 1,500 (C8: ten times the margin)
  tries $1/$T/TestCatalogFaultProperties.java each 5000
}

mutant() { # name main-dir test-dir
  local M=$2
  case $1 in
    # a replace that lost its race is a conflict, though the transaction holds on the fresh read
    raceconflict) mutate $RT "            adopt(fresh);
            if (!holds) {
              throw conflict(fresh, \"Cannot commit: the log changed concurrently\", why);" "            adopt(fresh);
            if (true) {
              throw conflict(fresh, \"Cannot commit: the log changed concurrently\", why);" ;;
    # a catalog transaction's updates are never judged where they land: each pins its version
    stagepinned) mutate $M/LogCatalog.java "    if (FileIOTableOperations.commitsOnce(commit.updates())) {
      return false;
    }" "    if (true) {
      return false;
    }" ;;
  esac
}
