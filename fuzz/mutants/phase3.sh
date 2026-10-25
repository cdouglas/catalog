# Calibration of conflict-encoding phases 2 and 3 (lamport-minus-properties, concern-vector,
# blind-append, the merge), on commit 67e91f4 (its anchors): each mutant must fail an oracle.
MUTANTS="basever resetadvance noremoval noshift norest blindall noreparent nototals retention
  blindnoreset blindnoremoval"
ORACLES=TestRebaseProperties,TestLamportMinusPropertiesEncoding,TestConcernVectorEncoding,TestBlindAppendEncoding,TestMultiWriterProperties

# TestRebaseProperties: anAdmitted, aMerged, aBlind
prepare() {
  tries $1/$T/TestRebaseProperties.java 800 1 800
  tries $1/$T/ConflictEncodingContract.java 400
}

mutant() { # name main-dir test-dir
  local M=$2
  case $1 in
    # a table's version set from the record's base, not from where it lands (S3)
    basever) mutate $M/ProtoCodec.java "int next = builder.tableVersion(id) + 1;" "int next = version + 1;" ;;
    # a ref moved to an existing snapshot counts as an advance, not a commit-once reset
    resetadvance) mutate $M/TableChange.java "        } else {
          reset.add(ref.refName);
          commitOnce = true;
        }" "        } else {
          advanced.add(ref.refName);
        }" ;;
    # snapshot removal is never stamped
    noremoval) mutate $M/ConcernVectorEncoding.java "    for (TableChange.Concern concern : change.concerns) {
      stamps.stampConcern(concern.ordinal(), version);" "    for (TableChange.Concern concern : change.concerns) {
      if (concern == TableChange.Concern.REMOVAL) {
        continue;
      }
      stamps.stampConcern(concern.ordinal(), version);" ;;
    # a merged manifest keeps its writer's sequence numbers and row ids
    noshift) mutate $M/Landing.java "    if (!added.contains(manifest.snapshotId()) || (sequenceShift == 0 && rowShift == 0)) {" "    if (true) {" ;;
    # lamport-minus-properties reads nothing but properties
    norest) mutate $M/LamportMinusPropertiesEncoding.java "    return Witness.of(base).concern(REST);" "    return Witness.of(base);" ;;
    # every producer is blind
    blindall) mutate $M/TableChange.java "      blind &= add.footprint != null
          && add.footprint.kind() == org.apache.iceberg.CommitFootprint.Kind.BLIND;" "" ;;
    # a merged snapshot keeps its writer's parent
    noreparent) mutate $M/Landing.java "            parents.put(add.snapshotId, head);" "" ;;
    # a merged snapshot keeps its writer's totals
    nototals) mutate $M/Landing.java "    return summary != null ? summary : add.summary;" "    return add.summary;" ;;
    # a merged ref keeps its writer's retention
    retention) mutate $M/Landing.java "    if (!stale || !added.contains(update.snapshotId)) {" "    if (true) {" ;;
    # a blind record does not read its branch's resets
    blindnoreset) mutate $M/BlindAppendEncoding.java "            .ref(branch, ConcernStamps.Ref.RESET);" ";" ;;
    # a blind record does not read snapshot removal
    blindnoremoval) mutate $M/BlindAppendEncoding.java "            .concern(TableChange.Concern.REMOVAL)" "" ;;
  esac
}
