# Calibration of the trailers, positions and tail reads (docs/TAIL_READS.md steps 2, 3 and 5;
# anchors as of the step 5 commit): each mutant must fail a commit-protocol fuzzer. The TLA+
# switches are lengthonly, notrailer and probeinc (CatalogCommit) and nojoint (SharedLogCommit);
# nochecksum is a mistake the model has no switch for, since its records have no bytes. A splice
# only the checksum sees is inside the checkpoint's frame, of a replace whose checkpoint is as long:
# the protocol fuzzer's checkpoints rarely keep their length across a replace (it missed the
# mutant in 50,000 tries, with a third of its splices inside that frame), so TestAtomicFileLog,
# which splices two such objects at every byte, is an oracle too.
MUTANTS="lengthonly notrailer nochecksum probeinc nojoint"
EQUIVALENT=""
ORACLES=TestMemoryCommitProtocol,TestAtomicFileCommitProtocol,TestAtomicFileLog

FL=intentlog-fileio/src/main/java/org/intentlog/fileio/AtomicFileLog.java
RT=intentlog-core/src/main/java/org/intentlog/LogRuntime.java

prepare() {
  tries $1/intentlog-memory/src/test/java/org/intentlog/memory/TestMemoryCommitProtocol.java 300000
  tries $1/intentlog-fileio/src/test/java/org/intentlog/fileio/TestAtomicFileCommitProtocol.java 300000
}

mutant() { # name main-dir test-dir (the catalog's; these mutants are intentlog's)
  case $1 in
    # TLA+ lengthonly: a tail read takes an object no shorter than its tail for the same one,
    # neither comparing the trailer at N - 20 nor the incarnation of the frames after N
    lengthonly) mutate $FL "    if (!Arrays.equals(bytes, 0, TRAILER_SIZE, held.trailer, 0, TRAILER_SIZE)) {" "    if (false) {" \
      && mutate $FL "      end = frameEnd(bytes, pos, n - TRAILER_SIZE, held.incarnation);" "      end = frameEnd(bytes, pos, n - TRAILER_SIZE, null);" ;;
    # TLA+ notrailer: a read checks no trailer (format 2): a splice at a frame's end decodes
    notrailer) mutate $FL "    int trailer = (int) end - TRAILER_SIZE;
    CRC32C crc = new CRC32C();" "    int trailer = (int) end - TRAILER_SIZE;
    if (Boolean.TRUE) {
      return (int) end;
    }
    CRC32C crc = new CRC32C();" ;;
    # a read checks incarnations and offsets, but no checksum: a splice inside a frame decodes
    nochecksum) mutate $FL "    if ((int) crc.getValue() != getInt(bytes, trailer + 16)) {" "    if (false) {" ;;
    # TLA+ probeinc: a resend after a probe frames its record with an incarnation no read of the
    # probed version gave (the commit loop's probe path: tail reads off)
    probeinc) mutate $FL "    long incarnation = before != null ? before.incarnation : incarnationAt(at);" "    long incarnation = before != null ? before.incarnation : 0L;" ;;
    # TLA+ nojoint: a read's records are replayed wherever they start
    nojoint) mutate $RT "    if (through == null) {
      return;
    }
    byte[] start = view.start();" "    if (through == null || Boolean.TRUE) {
      return;
    }
    byte[] start = view.start();" ;;
  esac
}
