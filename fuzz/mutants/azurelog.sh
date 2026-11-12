# Calibration of the Azure log (docs/AZURE_LOG.md "Mistakes the model must catch"; anchors as of
# the commit that adds intentlog-azure): each mutant puts back one of AzureLog.tla's bug switches
# in AppendBlobLog, and must fail an oracle: the commit-protocol fuzzer on the in-memory store,
# whose writers are one to three instances of the log (stale cached heads), or TestAppendBlobLog.
# The fuzzer preempts commits, never compactions, so no rotation lands between a compaction's read
# and its trim: trimearly only TestAppendBlobLog kills. Two more are the Java's own (2026-11-10):
# skipread, a read through a write that skips the read when records landed before it too
# (AZURE_LOG.md "Reading through a write"), and compactbycheckpoint, writers counting toward
# compaction from the checkpoint each last read rather than the last GC record ("When to
# compact"), which only TestAzureCompaction sees: it makes compactions many, not wrong.
# offsetfromlength was masked until the read through a write trusted the reply's offset to skip
# its read (2026-11-10): readThrough checked the receipt's CRC against the frame at its offset,
# so a wrong offset made an outcome ambiguous, never dishonest. Now an offset that names the kept
# tail while another record landed first returns a view without that record, which the fuzzer's
# oracle of held states and TestAppendBlobLog catch, as the model's AckedAtReceipt does.
# padunseen (2026-11-11, a review's find): a read from the end of a segment a pad closed, before
# the head named its successor, takes the segment for open, so a reader holding that head reads
# nothing new however much the successor takes. TestAppendBlobLog kills it; the fuzzer's oracle of
# stale readers (every read reads on to the log's end at the drain) did not in 2,000 local tries:
# the state needs a rotation that failed between its pad and its CAS, then a read before another
# writer's append repairs it, and no trim of the segment before the drain.
MUTANTS="casbeforeclose nolimit createonmissing noclosedlength sweepnamed trimearly sdkretry skipread compactbycheckpoint offsetfromlength padunseen"
EQUIVALENT=""
ORACLES=TestAzureCommitProtocol,TestAppendBlobLog,TestAzureCompaction

AL=intentlog-azure/src/main/java/org/intentlog/azure/AppendBlobLog.java
LR=intentlog-core/src/main/java/org/intentlog/LogRuntime.java

prepare() {
  tries $1/intentlog-azure/src/test/java/org/intentlog/azure/TestAzureCommitProtocol.java 100000
}

mutant() { # name main-dir test-dir (the catalog's; these mutants are intentlog's)
  case $1 in
    # TLA+ casbeforeclose: rotation CASes the head from the length it read, and closes after
    casbeforeclose) mutate $AL "    long closed = close(full);" "    AppendBlobStore.Props read = store.properties(full.name());
    long closed = read == null ? -1 : read.length();" \
      && mutate $AL "        Head won = new Head(rotated, etag);" "        close(full);
        Head won = new Head(rotated, etag);" ;;
    # TLA+ nolimit: some appends (here, records of an even length) carry no writer limit
    nolimit) mutate $AL "        long offset = store.appendBlock(seg.name(), frame, seg.writerLimit()," "        long offset = store.appendBlock(seg.name(), frame,
            record.length % 2 == 0 ? 0 : seg.writerLimit()," ;;
    # TLA+ createonmissing: a writer that finds its segment gone creates it and sends again
    createonmissing) mutate $AL "        // a stale head: the segment was trimmed and swept. Never create it (AZ4).
        Head fresh = existing();" "        store.createAppendBlob(seg.name(), metadata(h.head().logId(), seg.ordinal(), seg.limit()));
        if (Boolean.TRUE) {
          continue;
        }
        Head fresh = existing();" ;;
    # TLA+ noclosedlength: a reader takes a closed segment to whatever length its read returned
    noclosedlength) mutate $AL "      if (seg.closed() && b.length() != seg.closedLength()) {" "      if (false) {" \
      && mutate $AL "      if (next.base() != seg.base() + end) {" "      if (false) {" ;;
    # TLA+ sweepnamed: the sweep deletes segments the head names
    sweepnamed) mutate $AL "      if (ordinal >= 0 && !named.contains(e.name()) && ordinal <= head.last().ordinal()" "      if (ordinal >= 0 && ordinal <= head.last().ordinal()" ;;
    # TLA+ trimearly: a trim drops every segment but the last, the checkpoint's among them
    trimearly) mutate $AL "      Segment keep = head.at(t.position);" "      Segment keep = head.last();" ;;
    # TLA+ sdkretry: a send whose reply was lost is sent again below the runtime
    sdkretry) mutate $AL "        long offset = store.appendBlock(seg.name(), frame, seg.writerLimit(),
            at == null ? -1 : at.position - seg.base());" "        long offset;
        try {
          offset = store.appendBlock(seg.name(), frame, seg.writerLimit(),
              at == null ? -1 : at.position - seg.base());
        } catch (UncheckedIOException | AppendBlobStore.Throttled retried) {
          offset = store.appendBlock(seg.name(), frame, seg.writerLimit(),
              at == null ? -1 : at.position - seg.base());
        }" ;;
    # TLA+ offsetfromlength: the receipt's offset is the length read before the append
    offsetfromlength) mutate $AL "        long offset = store.appendBlock(seg.name(), frame, seg.writerLimit(),
            at == null ? -1 : at.position - seg.base());" "        AppendBlobStore.Props before = store.properties(seg.name());
        long offset = store.appendBlock(seg.name(), frame, seg.writerLimit(),
            at == null ? -1 : at.position - seg.base());
        offset = before == null ? offset : before.length();" ;;
    # a read through a write reads nothing when the record landed at or after the kept tail
    skipread) mutate $AL "      if (!fresh && r.record != null && kept.tail.position == r.position()) {" "      if (!fresh && r.record != null && kept.tail.position <= r.position()) {" ;;
    # a read from the end of a padded segment takes it for open (it sees no pad there)
    padunseen) mutate $AL "      boolean closed = seg.closed() || padded || end >= seg.limit()" "      boolean closed = seg.closed() || padded" ;;
    # writers count toward compaction from the checkpoint each last read
    compactbycheckpoint) mutate $LR "    CompactionPolicy.Input input = new CompactionPolicy.Input(landed.recordsSinceGc(),
        landed.checkpointBytes(), landed.checkpointBytes() + landed.bytesSinceGc(), 0);" "    CompactionPolicy.Input input = new CompactionPolicy.Input(landed.intentionRecords(),
        landed.checkpointBytes(), landed.tail().bytes(), 0);" ;;
  esac
}
