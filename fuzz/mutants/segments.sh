# Calibration of the segmented log (docs/LOG_SEGMENTS.md "Invariants"): each mutant puts back one
# of the mistakes in SegmentedLog, and must fail an oracle: the commit-protocol fuzzer on the
# in-memory stores (TestSegmentedCommitProtocol: one to three instances, faults on both stores,
# small limits, copies, sweeps at TTL 0) or the log's regressions (TestSegmentedLog). The model's
# catalogseal, limitatnext and sealcount have no Java to mutate: no catalog record states an end,
# an Install sets only its own segment's limit, and a seal frame carries no count. nested is
# replay's refusal of a provider record in a view (TestReplay). Two are the Java's own:
# fastvoid, the read through an append that skips its read when the reply puts the copy at the
# kept view's end, past its segment's limit; and lastcopy, the read through deciding by the last
# copy the log holds rather than the first.
MUTANTS="readahead voidack createonmissing sweepearly sweepinflight sdkretry overlimit fastvoid lastcopy"
EQUIVALENT=""
ORACLES=TestSegmentedCommitProtocol,TestSegmentedLog

SL=intentlog-fileio/src/main/java/org/intentlog/fileio/SegmentedLog.java

prepare() {
  tries $1/intentlog-fileio/src/test/java/org/intentlog/fileio/TestSegmentedCommitProtocol.java 100000
}

mutant() { # name main-dir test-dir (the catalog's; these mutants are intentlog's)
  case $1 in
    # TLA+ readahead: a reader reads on past a segment that hasn't ended, when a later one is
    # installed
    readahead) mutate $SL "      if (!ended) {
        w.at = o;
        w.open = true;
        return w;
      }" "      if (!ended && c.seg(o + 1) == null) {
        w.at = o;
        w.open = true;
        return w;
      }" ;;
    # TLA+ voidack: the read through an append takes a copy past its segment's end for the record
    voidack) mutate $SL "        return VOID; // its segment ended before it" "        return acc.extend(new View(acc.tail, null, acc.tail.position, List.of(record),
            new long[] {cp.ordinal}, new long[] {cp.offset}));" ;;
    # TLA+ createonmissing: a writer that finds its segment gone creates it and sends again
    createonmissing) mutate $SL "        // a stale catalog object: the segment was retired and swept. Never create it (SG4)." "        store.create(path(seg.name()));
        if (Boolean.TRUE) {
          continue;
        }" ;;
    # TLA+ sweepearly: the sweep deletes segments the catalog object names
    sweepearly) mutate $SL "      if (ordinal >= 0 && ordinal <= newest && !keep.contains(name)) {" "      if (ordinal >= 0 && ordinal <= newest) {" ;;
    # TLA+ sweepinflight: the sweep deletes unnamed blobs of any ordinal, an Install's in flight
    sweepinflight) mutate $SL "      if (ordinal >= 0 && ordinal <= newest && !keep.contains(name)) {" "      if (ordinal >= 0 && !keep.contains(name)) {" ;;
    # TLA+ sdkretry: an append whose reply was lost is sent again below the runtime
    sdkretry) mutate $SL "        a = store.append(path(seg.name()), frame);" "        try {
          a = store.append(path(seg.name()), frame);
        } catch (UncheckedIOException | SegmentStore.Throttled retried) {
          a = store.append(path(seg.name()), frame);
        }" ;;
    # TLA+ overlimit: Installs set a limit one past the store's block limit
    overlimit) mutate $SL "    this.limit = options.limit() > 0 ? options.limit() : store.maxBlocks();
    if (limit > store.maxBlocks()) {" "    this.limit = options.limit() > 0 ? options.limit() + 1 : store.maxBlocks();
    if (limit > store.maxBlocks() + 1) {" ;;
    # the read through an append skips its read for a copy at the kept view's end, past the limit
    fastvoid) mutate $SL "          && t.records() + 1 == cp.index && cp.index <= seg.limit()) {" "          && t.records() + 1 == cp.index) {" ;;
    # the read through an append decides by the last copy the log holds
    lastcopy) mutate $SL "    for (Copy cp : r.copies) {" "    List<Copy> order = new ArrayList<>(r.copies);
    java.util.Collections.reverse(order);
    for (Copy cp : order) {" ;;
  esac
}
