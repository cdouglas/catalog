# Calibration of the retry oracle's merged manifests (C2 (retry oracle never merges manifests))
# and mixed writer modes (C1 (list-file snapshots stamp nothing)); anchors as of the C1 commit:
# each mutant must fail an oracle. Half of TestRebaseProperties' tries and the contract's merge
# manifests (commit.manifest.min-count-to-merge 2) and add one to three files a commit, and
# TableViews.logical checks each manifest's minimum sequence number against its live entries' and
# its row id range. minseqshift moves the minimum sequence number of every
# manifest a stale record wrote, where only one above its base's last sequence number moves: a
# merged or rewritten manifest's older entries keep theirs, and a later merging commit drops delete
# files older than the minimum. blindunguarded lets a blind record that merged manifests land after
# its branch dropped manifests (blind-append's DROP read gone) even where a manifest it merged away
# is no longer in its branch's head (Landing's check skipped for blind records): the merged manifest
# brings back files the window removed. listfilestamp drops Bug 97's stamp (list-file writers stamp
# no partition): a snapshot with a manifest-list file, which sends no manifests, stamps every
# partition bucket. The oracles draw writers apart from R's mode (TestRebaseProperties
# aValidatedCommitIsWhatItsRetryWouldCommit's races, the contract's racer and extender), and run
# C1's race: an inline-TM writer's commit in R's partition, then an inline-ML writer's append
# elsewhere (the contract's aRecordPastAListFileWritersCommitIsJudgedAlike catches it by S2, at try
# 14 locally; the validated rebase property by Iceberg's retry, at try 156).
# Equivalent: blindnodrop, blind-append's DROP read alone. A merging record lands only where every
# manifest it removes is still in its branch's head (Landing.unmergeable), and a snapshot that drops
# manifests is a merging producer's, which merges or rewrites the manifests the record merged away
# too: a probe of aBlindAppendIsWhatItsRetryWouldCommit (300 tries, 2026-11-18) saw the DROP stamp
# move under a merged blind record 8 times, each refused by Landing ("merge: a manifest it removes
# is not in its branch's head") or a concern; 1,000 tries admitted none past a moved DROP stamp.
# sealedlast undoes Bug 129's fix (sealed rejections misattributed): a table update explains its
# rejection by the conflict encoding's reason before the sealed snapshot it needed, so a merge whose
# parent a window sealed says its parent's summary is gone. The rebase oracle's paged twin (gap plan
# 2.9: in half the tries, an unpaged subject beside a paged twin) found the bug at tries 7 to 27 of
# four properties; it may reject a record the subject admits only for a reason that says sealed.
MUTANTS="minseqshift blindunguarded listfilestamp sealedlast"
EQUIVALENT="blindnodrop"
ORACLES=TestRebaseProperties,TestBlindAppendEncoding,TestPartitionVectorEncoding,TestManifestWindowEncoding

prepare() {
  tries $1/$T/TestRebaseProperties.java each 300
  tries $1/$T/ConflictEncodingContract.java each 400
}

mutant() { # name main-dir test-dir
  local M=$2
  case $1 in
    # every manifest a stale record wrote has its minimum sequence number moved
    minseqshift) mutate $M/Landing.java "      if (minSequenceNumber > baseLastSequenceNumber) {" "      if (true) {" ;;
    # a merged blind record lands where its branch dropped manifests, even ones it merged away
    blindunguarded) mutate $M/BlindAppendEncoding.java "    if (!change.drops.isEmpty()) {
      witness.ref(change.advanced.iterator().next(), ConcernStamps.Ref.DROP);
    }
" "" && mutate $M/Landing.java "            if (headList.stream().noneMatch(path -> path.endsWith(suffix))) {" "            if (headList.stream().noneMatch(path -> path.endsWith(suffix))
                && (adds.get(first).footprint == null
                    || adds.get(first).footprint.kind != Footprint.Kind.BLIND)) {" ;;
    # a snapshot with a manifest-list file stamps no partition
    listfilestamp) mutate $M/BlindAppendEncoding.java "      if (!add.manifestListSuffix.isEmpty()) {" "      if (false) {" ;;
    # a rejection explained by the encoding before the sealed snapshot it needed (Bug 129)
    sealedlast) mutate $M/ProtoCodec.java "      String needed = sealedSnapshotNeeded(builder);
      if (needed != null) {
        return needed;
      }
      String why = ConflictEncodings.rejection(table(), builder);
      if (why != null) {
        return why;
      }" "      String why = ConflictEncodings.rejection(table(), builder);
      if (why != null) {
        return why;
      }
      String needed = sealedSnapshotNeeded(builder);
      if (needed != null) {
        return needed;
      }" ;;
    # blind-append's read of dropped manifests alone (equivalent, above)
    blindnodrop) mutate $M/BlindAppendEncoding.java "    if (!change.drops.isEmpty()) {
      witness.ref(change.advanced.iterator().next(), ConcernStamps.Ref.DROP);
    }
" "" ;;
  esac
}
