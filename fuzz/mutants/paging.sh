# Calibration of paging the committed set (docs/COMMITTED_PAGING.md stage 4; anchors as of
# d15036e, pageown's as of bc5d500, which renamed its variable): each mutant of intentlog-core
# must fail a commit-protocol fuzzer, on the shared log or on one object. The TLA+ switches are
# floorgapB, absentpage, nodigest, sweepabove, mintH, pageown and missingfail; the rest are
# mistakes the Java could make that the model has no switch for.
MUTANTS="nodroppedreplay nodigest nomerge floorgapB pagedabsent gckeepspages absentpage sweepabove
  pageown mintH nodelete noadmission missingfail"
# Unreachable from the fuzzers, so no oracle can catch them. newest: no compactor proposes the
# newest block (pageable() never offers it), so only a forged PageOut reaches pagedOut's check
# (TestCommittedSetBlocks.theNewestBlockIsNeverSealed forges one). floorgap: the built set's
# admission test; replay admits through its builder, where floorgapB is the same mistake.
EQUIVALENT="newest floorgap"
ORACLES=TestMemoryCommitProtocol,TestAtomicFileCommitProtocol

CS=intentlog-core/src/main/java/org/intentlog/CommittedSet.java
RT=intentlog-core/src/main/java/org/intentlog/LogRuntime.java

prepare() {
  tries $1/intentlog-memory/src/test/java/org/intentlog/memory/TestMemoryCommitProtocol.java 300000
  tries $1/intentlog-fileio/src/test/java/org/intentlog/fileio/TestAtomicFileCommitProtocol.java 300000
}

mutant() { # name main-dir test-dir (the catalog's; these mutants are intentlog-core's)
  case $1 in
    # a compaction deletes only the dropped pages its first view's checkpoint named
    nodroppedreplay) mutate $CS "    public List<PageRef> droppedPages() {
      return Collections.unmodifiableList(dropped);" "    public List<PageRef> droppedPages() {
      return List.of();" ;;
    # a PageOut seals the oldest block whatever its count and digest
    nodigest) mutate $CS "        || oldest.count() != record.count() || !record.hasDigest(oldest.digest())) {" ") {" ;;
    # the newest block can be sealed (unreachable)
    newest) mutate $CS "  public CommittedSet pagedOut(PageOut record) {
    if (blocks.length < 2) {" "  public CommittedSet pagedOut(PageOut record) {
    if (blocks.length < 1) {" ;;
    # no merge after a PageOut: the remaining blocks are not those the rule makes of their ids
    nomerge) mutate $CS "    List<Block> list = new ArrayList<>(Arrays.asList(blocks).subList(1, blocks.length));
    mergeUp(list, blockIds);" "    List<Block> list = new ArrayList<>(Arrays.asList(blocks).subList(1, blocks.length));" ;;
    # TLA+ floorgap in the built set: admission above H, not F (unreachable)
    floorgap) mutate $CS "    return !holds(id) && aboveFloor(id, floor);" "    return !holds(id) && aboveFloor(id, horizon);" ;;
    # TLA+ floorgap in the set's builder, where replay admits
    floorgapB) mutate $CS "      return !added.contains(id) && !base.holds(id) && aboveFloor(id, base.floor);" "      return !added.contains(id) && !base.holds(id) && aboveFloor(id, base.horizon);" ;;
    # an id in (H, F] reads as absent, not paged out
    pagedabsent) mutate $CS "    throw new PagedOutException(id, page);
  }" "    return false;
  }" ;;
    # GC never drops a page
    gckeepspages) mutate $CS "    while (dropPages < pages.length && pages[dropPages].top() <= watermarkMs) {" "    while (false && dropPages < pages.length && pages[dropPages].top() <= watermarkMs) {" ;;
    # TLA+ absentpage: a page that cannot be read reads as not holding the id
    absentpage) mutate $RT "      } catch (RuntimeException e) {
        return unreadablePage(e, earlier);
      }" "      } catch (RuntimeException e) {
        holds = false;
      }" ;;
    # TLA+ sweepabove: a sweep deletes unreferenced pages above the floor too
    sweepabove) mutate $RT "      if (top >= 0L && top <= set.floor() && !referenced.contains(name)) {" "      if (top >= 0L && !referenced.contains(name)) {" ;;
    # TLA+ pageown: a compaction pages the block holding its own record's id
    pageown) mutate $RT "      seals = page(set, pageCeiling(now, ownMs), on.logId());" "      seals = page(set, pageCeiling(now, Long.MAX_VALUE), on.logId());" ;;
    # TLA+ mintH: a writer mints its id above H, not above F
    mintH) mutate $RT "      this.id = UuidV7.newUuidV7After(base.committed().floor(), config.clockMs().getAsLong());" "      this.id = UuidV7.newUuidV7After(base.committed().horizon(), config.clockMs().getAsLong());" ;;
    # a compaction deletes no page
    nodelete) mutate $RT "    if (store == null || (dropped.isEmpty() && !sealed)) {
      return;
    }
    try {" "    if (store != null) {
      return;
    }
    try {" ;;
    # the page ceiling ignores the admission window A
    noadmission) mutate $RT "    return Math.min(window >= now ? -1L : now - window, ceiling);" "    return Math.min(now, ceiling);" ;;
    # TLA+ missingfail: a page found missing fails the lookup at once, without rereads
    missingfail) mutate $RT "        } catch (MissingPage missing) {
          if (rereads >= MAX_PAGE_REREADS) {" "        } catch (MissingPage missing) {
          if (true) {" ;;
  esac
}
