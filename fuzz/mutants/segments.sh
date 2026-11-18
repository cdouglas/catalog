# Calibration of the segmented log (docs/LOG_SEGMENTS.md "Invariants"): each mutant puts back one
# of the mistakes in SegmentedLog, and must fail an oracle: the commit-protocol fuzzer on the
# in-memory stores (TestSegmentedCommitProtocol: one to three instances, faults on both stores,
# small limits, copies, sweeps at TTL 0, another log at the same prefix) or the log's regressions
# (TestSegmentedLog). The model's catalogseal, limitatnext and sealcount have no Java to mutate:
# no catalog record states an end, an Install sets only its own segment's limit, and a seal frame
# carries no count. nested is replay's refusal of a provider record in a view (TestReplay). Five
# are the Java's own: fastvoid, the read through an append that skips its read when the reply
# puts the copy at the kept view's end, past its segment's limit; lastcopy, the read through
# deciding by the last copy the log holds rather than the first; dropretired, a checkpoint
# dropping a Retired entry past its TTL without deleting its segment (a leak where the sweep
# can't list); nolocation, an entry's location ignored for a name under the prefix (only a
# migration writes one); and creationleak, a creator that lost keeping its segment 0, under a log
# id no sweep lists (a leak). fastvoid is equivalent (2026-11-15, 2,000 tries locally): an append
# whose reply puts it past the limit is void with no read and never in a receipt, and a receipt
# whose store can't say the index has none for the fast path to match. Locally at 2,000 tries
# (2026-11-17) the other twelve are caught: dropretired, nolocation and creationleak by
# TestSegmentedLog, sweepothers by both, and the rest by the fuzzer.
MUTANTS="readahead voidack createonmissing sweepearly sweepinflight sweepothers sdkretry overlimit fastvoid lastcopy dropretired nolocation creationleak"
EQUIVALENT="fastvoid"
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
    createonmissing) mutate $SL "        // a stale catalog object: the segment was retired and swept. Never create it (SG4)." "        store.create(path(seg));
        if (Boolean.TRUE) {
          continue;
        }" ;;
    # TLA+ sweepearly: the sweep deletes segments the catalog object names
    sweepearly) mutate $SL "      if (ordinal >= 0 && ordinal <= newest && !named.contains(name)) {" "      if (ordinal >= 0 && ordinal <= newest) {" ;;
    # TLA+ sweepinflight: the sweep deletes unnamed blobs of any ordinal, an Install's in flight
    sweepinflight) mutate $SL "      if (ordinal >= 0 && ordinal <= newest && !named.contains(name)) {" "      if (ordinal >= 0 && !named.contains(name)) {" ;;
    # a checkpoint drops a Retired entry past its TTL without deleting its segment
    dropretired) mutate $SL "        if (!expired(old, now) || !deleteRetired(c.logId(), old, deleted)) {" "        if (!expired(old, now)) {" ;;
    # an entry's location ignored: the segment is looked for under the prefix
    nolocation) mutate $SL "    if (location.isEmpty()) {
      return path(logId, name);" "    if (true) {
      return path(logId, name);" ;;
    # TLA+ sweepothers, Bug 100 (sweep deleted another log's segments): the sweep lists the whole
    # prefix, every log's segments, and takes another log's for its own unnamed blobs
    sweepothers) mutate $SL "    for (String full : store.list(path(c.logId(), \"seg-\"))) {
      long ordinal = ordinalAt(prefix, c.logId(), full); // -1: none of this log's segments" "    for (String full : store.list(prefix + \"/\")) {
      long ordinal = ordinalOf(full.substring(full.lastIndexOf('/') + 1));" ;;
    # a creation that found another writer's log keeps its segment 0, which no sweep lists
    creationleak) mutate $SL "      abandon(logId, name);
      created(checkpoint, e);" "      created(checkpoint, e);" ;;
    # TLA+ sdkretry: an append whose reply was lost is sent again below the runtime
    sdkretry) mutate $SL "        a = store.append(path(seg), frame);" "        try {
          a = store.append(path(seg), frame);
        } catch (UncheckedIOException | SegmentStore.Throttled retried) {
          a = store.append(path(seg), frame);
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
