# Calibration of the merge's refusals (Landing.unmergeable; the 2026-11-17 review's C6, gap plan
# 2.8): a record admitted on a table changed since its read is finalized where it lands only when
# the merge can be (a conflict encoding judges whether it should). Each mutant drops one refusal,
# so such a record merges anyway: one that adds more than one snapshot to a table that has moved
# (moresnapshots), whose list is a file written for its base (listfile), that stages a snapshot
# on a branch that has moved (stagedmoved), that starts a fresh history on a branch with a head
# (freshhistory), or that removes a manifest its branch's head no longer has (removednothead).
# The rebase oracle compares what merged with Iceberg's retry; the contracts judge each rejection
# against copies of it landing later (S2), steering the extension to commits on main after a
# "merge:" rejection.
#
# Locally at 300 rebase tries (2/1): listfile caught by anAdmittedCommit at try 26; removednothead
# by five properties at tries 4 to 223; moresnapshots only once R could be a table transaction
# (gap plan 2.6: anAdmittedTransaction at try 194 of 1,000; a record of one snapshot never adds
# several). stagedmoved and freshhistory passed 300 tries of every property before their reach
# was added: a snapshot staged on a branch (TableScript's STAGED_APPEND, half the time), whose
# parent the merge would take for main's head, and a table's first commit racing others (the
# merge probe's empty prefixes). The contracts' steering seldom fires: their records are rejected
# as "merge:" in about one try of 300.
MUTANTS="moresnapshots listfile stagedmoved freshhistory removednothead"
EQUIVALENT=""
ORACLES=TestRebaseProperties,TestBlindAppendEncoding,TestPartitionVectorEncoding,TestManifestWindowEncoding,TestConcernVectorEncoding

prepare() {
  tries $1/$T/TestRebaseProperties.java each 300
  tries $1/$T/ConflictEncodingContract.java each 400
}

mutant() { # name main-dir test-dir
  local M=$2
  case $1 in
    # a record that adds more than one snapshot merges onto a table that has moved
    moresnapshots) mutate $M/Landing.java "    if (adds.size() > 1 && (moved || !landing.parents.isEmpty())) {" "    if (false) {" ;;
    # a snapshot whose list is a file, written for its base, merges
    listfile) mutate $M/Landing.java "      if (!add.manifestListSuffix.isEmpty()
          && (moved || landing.parents.containsKey(add.snapshotId))) {" "      if (false) {" ;;
    # a staged snapshot merges onto a branch that has moved
    stagedmoved) mutate $M/Landing.java "      if (!branches.containsKey(first(add.snapshotId, adds))
          && landing.parents.containsKey(add.snapshotId)) {" "      if (false) {" ;;
    # a snapshot with no parent merges onto a branch that has a head
    freshhistory) mutate $M/Landing.java "      if (parent == null && head.getValue() != null) {" "      if (false) {" ;;
    # a manifest it removes need not be in its branch's head
    removednothead) mutate $M/Landing.java "            if (headList.stream().noneMatch(path -> path.endsWith(suffix))) {" "            if (false) {" ;;
  esac
}
