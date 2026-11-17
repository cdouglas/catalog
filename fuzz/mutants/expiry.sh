# Calibration of an expiry's merge onto a changed table (errata D17's fix; anchors as of its
# commit): each mutant must fail an oracle. expirynoreset drops the witness's read of every ref's
# resets, admitting an expiry after a ref was reset that names none of what it removes (a rejection
# that would not stay one, S2: a tag that came and went); expiryrefnamed also takes a record that
# removes a snapshot a ref names on the table it lands on for an expiry, which then removes the
# snapshot and the ref; expirynomerge refuses every removal where the record lands again, so no
# expiry merges.
MUTANTS="expirynoreset expiryrefnamed expirynomerge"
EQUIVALENT=""
ORACLES=TestConflictMatrix,TestConflictAttribution,TestRebaseProperties,TestConcernVectorEncoding,TestManifestWindowEncoding

prepare() {
  tries $1/$T/TestRebaseProperties.java each 300
  tries $1/$T/ConflictEncodingContract.java each 400
}

mutant() { # name main-dir test-dir
  local M=$2
  case $1 in
    # an expiry reads no ref's resets
    expirynoreset) mutate $M/ConcernVectorEncoding.java "          .concern(TableChange.Concern.SPECS)
          .anyRef(ConcernStamps.Ref.RESET);" "          .concern(TableChange.Concern.SPECS);" ;;
    # ... nor whether a ref names a snapshot it removes where it lands
    expiryrefnamed) mutate $M/ConcernVectorEncoding.java "          .concern(TableChange.Concern.SPECS)
          .anyRef(ConcernStamps.Ref.RESET);" "          .concern(TableChange.Concern.SPECS);" && mutate $M/TableChange.java "    return concerns.equals(EnumSet.of(Concern.REMOVAL))
        && advanced.isEmpty()
        && reset.isEmpty()" "    return concerns.equals(EnumSet.of(Concern.REMOVAL))
        && advanced.isEmpty()" ;;
    # a record that removes snapshots never merges
    expirynomerge) mutate $M/Landing.java "      } else if (update instanceof InlineDeltaCodec.RemoveSchemasUpdate
          || update instanceof InlineDeltaCodec.RemovePartitionSpecsUpdate) {" "      } else if (update instanceof InlineDeltaCodec.RemoveSchemasUpdate
          || update instanceof InlineDeltaCodec.RemoveSnapshotsUpdate
          || update instanceof InlineDeltaCodec.RemovePartitionSpecsUpdate) {" ;;
  esac
}
