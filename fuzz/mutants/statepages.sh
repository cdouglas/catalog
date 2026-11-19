# Calibration of a machine's pages under the commit-protocol fuzzers (the 2026-11-17 review's B4,
# the gap plan's item 2.2; anchors as of its commit): each mutant must fail one of the three
# fuzzers. The fuzzers' machine pages a copy of its history (CommitProtocol.MACHINE,
# EntityVersionsMachine.paged(4)) and trims it, as the catalog pages tables' old snapshots and
# drops pages, so the runtime puts, deletes and sweeps a machine's pages there beside the
# committed set's; a sweep may run from a stale replica between a compactor's puts and its
# checkpoint's write. Before, the machine had no pages, so no fuzzer ran the runtime's code for
# them, and the runtime mutants below passed by construction.
#
# First failing tries (memory, one object, segmented; shrinking off, laptop): sweepstale 23, 45,
# 28 (an armed sweep); putswallowed 53, 332, 328; staterootfirst 29, 10, 220; sweepnostate 37, 8,
# 40; pagesputfirst passed 50,000 on the memory log and 20,000 on the segmented one and failed
# one object at 9,615 (at the drain): a page is marked stored only in the states a fold shares,
# and on the memory and segmented logs a checkpoint is written only by a compaction, which
# replays its log whole, so the next one offers the page again. One object's writers checkpoint
# their commits' folds. The unmutated fuzzers passed 50,000, 50,000 and 20,000 tries.
#
# Not a mutant: deleting the pages a checkpoint's records dropped before the checkpoint lands.
# A record that drops a page has landed before any compactor folds it, so the page is garbage under
# every state after it; a reader of an older checkpoint whose read lags the record may still name
# it, and finds it collected, as after the delete at any time (the catalog reads the table as it
# is now).
MUTANTS="sweepstale pagesputfirst putswallowed staterootfirst sweepnostate"
EQUIVALENT=""
ORACLES=TestMemoryCommitProtocol,TestAtomicFileCommitProtocol,TestSegmentedCommitProtocol

RT=intentlog-core/src/main/java/org/intentlog/LogRuntime.java
EV=intentlog-testkit/src/main/java/org/intentlog/testkit/EntityVersionsMachine.java

prepare() {
  # Ten times each fuzzer's largest first failing try (C8): 53, 9,615 (pagesputfirst), 328
  tries $1/intentlog-memory/src/test/java/org/intentlog/memory/TestMemoryCommitProtocol.java 5000
  tries $1/intentlog-fileio/src/test/java/org/intentlog/fileio/TestAtomicFileCommitProtocol.java 100000
  tries $1/intentlog-fileio/src/test/java/org/intentlog/fileio/TestSegmentedCommitProtocol.java 5000
}

mutant() { # name main-dir test-dir (the catalog's; these mutants are intentlog's)
  case $1 in
    # the machine's garbagePages without its version guard (as the catalog's without
    # `page[1] > table.version`): a sweep from a replica behind the record that sealed a chunk takes
    # its page, which a compactor ahead has put, for garbage
    sweepstale) mutate $EV "      if (k >= state.sealed()) {
        continue; // sealed by a record after this state
      }" "      if (k >= state.sealed()) {
        garbage.add(name);
        continue;
      }" ;;
    # a checkpoint's pages are marked stored before they are put: one whose put failed is never
    # put again, and a later checkpoint names it
    pagesputfirst) mutate $RT "    for (Map.Entry<String, byte[]> page : pages.entrySet()) {
      store.put(page.getKey(), page.getValue());
    }
    machine.pagesPut(state, pages.keySet());" "    machine.pagesPut(state, pages.keySet());
    for (Map.Entry<String, byte[]> page : pages.entrySet()) {
      store.put(page.getKey(), page.getValue());
    }" ;;
    # a put of a checkpoint's page that fails is taken for stored: the checkpoint names a page that
    # does not exist
    putswallowed) mutate $RT "    for (Map.Entry<String, byte[]> page : pages.entrySet()) {
      store.put(page.getKey(), page.getValue());
    }" "    for (Map.Entry<String, byte[]> page : pages.entrySet()) {
      try {
        store.put(page.getKey(), page.getValue());
      } catch (RuntimeException e) {
        // taken for stored
      }
    }" ;;
    # TLA+ rootfirst for a machine's pages: a checkpoint's pages are put only after it lands
    staterootfirst) mutate $RT "  private byte[] checkpoint(Replica<S> replica) {
    putStatePages(replica.state());" "  private byte[] checkpoint(Replica<S> replica) {" \
      && mutate $RT "  private byte[] checkpoint(Replica<S> replica, LogTail through) {
    putStatePages(replica.state());" "  private byte[] checkpoint(Replica<S> replica, LogTail through) {" \
      && mutate $RT "    log.putCheckpoint(view.tail(), checkpoint(through, view.tail()));" "    log.putCheckpoint(view.tail(), checkpoint(through, view.tail()));
    putStatePages(through.state());" \
      && mutate $RT "        S checkpointed = (S) replace.state;
        deleteDroppedStatePages(checkpointed);
        return;" "        S checkpointed = (S) replace.state;
        putStatePages(checkpointed);
        deleteDroppedStatePages(checkpointed);
        return;" \
      && mutate $RT "              S checkpointed = (S) replace.state;
              deleteDroppedStatePages(checkpointed);
              return validate(written);" "              S checkpointed = (S) replace.state;
              putStatePages(checkpointed);
              deleteDroppedStatePages(checkpointed);
              return validate(written);" ;;
    # a sweep deletes the committed set's orphans only, never a machine's garbage: pages a
    # compactor did not delete stay for good
    sweepnostate) mutate $RT "    garbage.addAll(new java.util.TreeSet<>(machine.garbagePages(replica.state(), names)));" "" ;;
  esac
}
