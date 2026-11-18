# Calibration of the history oracles (the 2026-11-17 review's A2 and A3; the gap plan's item 1.2;
# anchors as of its commit): each mutant must fail an oracle. Snapshot paging's mutants need
# sealed blocks: run the set at SNAPSHOT_BLOCK=2 SNAPSHOT_WINDOW=1. Tries below are of the fault
# fuzzer's two properties at 2/1, faultyStorageNeverMisreportsACommit's first.
#
# importdropblock (a re-inlined table loses its oldest block of history) passed 1,000 tries of
# each before; the rule that snapshots leave a table only across an expiry (A2) fails it at tries
# 394 and 115, on whatever commit brings a pointer table back inline. encodeskipblock and
# encodeleaves (a checkpoint without a table's first sealed block; with its leaves counted one too
# many) the paged fault fuzzer already caught by other oracles (a fresh reader loads the table
# differently, at try 1; the root's pages do not hash to its blocks, at tries 9 and 11); the
# round trip's sealed blocks (A3) now fail both at try 1, at the write that made them.
#
# Already caught before by other oracles, so not in the set: the review's importnokept (every
# snapshot kept off a re-inlined head is lost, ref targets too: try 1), decodelastlive (a decoded
# sealed block loses its last live id: try 1, by item 1.1's progress rule, a commit refused as
# "import: block out of order or empty"), encodemerkle (try 1) and the TLA+ switch rootfirst (a
# root's pages put after it lands: try 8, the root names a page that does not exist).
#
# Not reached at 2,000 tries before or after, left for the plan's items 2.1 and 2.2:
# withoutneighbour (a sealed snapshot's expiry also takes its neighbour's live id: three expiries
# of a sealed snapshot in 2,000 tries, even picking sealed ones), missingempty (a page the store
# lacks reads as an empty block: no page goes missing without a sweep), importinlinelost (a
# re-inlining loses the snapshots behind its line that keep inline lists), and the switch
# sweepstale (garbagePages without its version guard: nothing in the fuzzer sweeps).
MUTANTS="importdropblock encodeskipblock encodeleaves"
# A bring-back whose proof is not checked: every writer sends a true proof, so only a writer that
# lies could tell (it passed 1,000 tries of each, before and after)
EQUIVALENT="noproof"
ORACLES=TestCatalogFaultProperties

prepare() {
  # 4,000: importdropblock fails at try 394 (C8: ten times the margin)
  tries $1/$T/TestCatalogFaultProperties.java each 4000
}

RT=intentlog-core/src/main/java/org/intentlog/LogRuntime.java

mutant() { # name main-dir test-dir
  local M=$2
  case $1 in
    # a re-inlined table keeps only the snapshots it seals into import pages, losing the rest
    importnokept) mutate $M/LogCatalog.java "        } else {
          kept.add(snapshot);
        }
      }
      if (byBlock.isEmpty()) {" "        }
      }
      if (byBlock.isEmpty()) {" ;;
    # a re-inlined table loses its oldest block of history: neither imported nor kept
    importdropblock) mutate $M/LogCatalog.java "        } else {
          kept.add(snapshot);
        }
      }
      if (byBlock.isEmpty()) {" "        } else {
          kept.add(snapshot);
        }
      }
      byBlock.pollFirstEntry();
      if (byBlock.isEmpty()) {" ;;
    # a re-inlined table keeps, of the snapshots behind its sealing line that it does not import,
    # only those a ref targets: those with an inline list are lost
    importinlinelost) mutate $M/LogCatalog.java "        } else {
          kept.add(snapshot);
        }
      }
      if (byBlock.isEmpty()) {" "        } else if (k > through || targets.contains(snapshot.snapshotId())) {
          kept.add(snapshot);
        }
      }
      if (byBlock.isEmpty()) {" ;;
    # a checkpoint leaves out a table's first sealed block
    encodeskipblock) mutate $M/ProtoCodec.java "    for (SnapshotPages.SealedBlock block : sealed.values()) {
      writeLengthDelimited(out, INLINE_TBL_SEALED_BLOCKS, encodeSealedBlock(block));" "    boolean skipped = false;
    for (SnapshotPages.SealedBlock block : sealed.values()) {
      if (!skipped) {
        skipped = true;
        continue;
      }
      writeLengthDelimited(out, INLINE_TBL_SEALED_BLOCKS, encodeSealedBlock(block));" ;;
    # a checkpoint counts a sealed block's leaves one too many (a bring-back's proof is checked
    # against them; loads never read them)
    encodeleaves) mutate $M/ProtoCodec.java "    writeVarint(out, SEALED_BLOCK_LEAVES, block.leaves);" "    writeVarint(out, SEALED_BLOCK_LEAVES, block.leaves + 1);" ;;
    # a checkpoint writes a sealed block's digest for its Merkle root
    encodemerkle) mutate $M/ProtoCodec.java "    writeBytes(out, SEALED_BLOCK_MERKLE_ROOT, block.merkleRoot);" "    writeBytes(out, SEALED_BLOCK_MERKLE_ROOT, block.digest);" ;;
    # a decoded sealed block loses its last live id: an unexpired snapshot is gone
    decodelastlive) mutate $M/ProtoCodec.java "    if (live != liveIds.length) {
      liveIds = java.util.Arrays.copyOf(liveIds, live);
    }" "    live = Math.max(0, live - 1);
    if (live != liveIds.length) {
      liveIds = java.util.Arrays.copyOf(liveIds, live);
    }" ;;
    # a sealed snapshot's expiry also takes its neighbour's live id
    withoutneighbour) mutate $M/SnapshotPages.java "      long[] kept = new long[liveIds.length - 1];
      System.arraycopy(liveIds, 0, kept, 0, at);
      System.arraycopy(liveIds, at + 1, kept, at, liveIds.length - at - 1);" "      int drop = at + 1 < liveIds.length ? 2 : 1;
      long[] kept = new long[liveIds.length - drop];
      System.arraycopy(liveIds, 0, kept, 0, at);
      System.arraycopy(liveIds, at + drop, kept, at, liveIds.length - at - drop);" ;;
    # a sealed snapshot comes back without its Merkle proof checked
    noproof) mutate $M/ProtoCodec.java "        if (!back.proves(builder.sealedBlockOf(id, back.snapshotId), prefix != null ? prefix : \"\")) {" "        if (false) {" ;;
    # TLA+ rootfirst: a compaction puts a root's pages only after the root lands
    rootfirst) mutate $RT "  private byte[] checkpoint(Replica<S> replica) {
    putStatePages(replica.state());" "  private byte[] checkpoint(Replica<S> replica) {" \
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
    # a page the store lacks reads as a block of no snapshots
    missingempty) mutate $M/SnapshotPages.java "      bytes = store.get(name).orElseThrow(() -> new MissingPage(name, tableId));" "      bytes = store.get(name).orElse(null);
      if (bytes == null) {
        return new Page(null, tableId, block.block, java.util.List.of(), java.util.Map.of());
      }" ;;
  esac
}
