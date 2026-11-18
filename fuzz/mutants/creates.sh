# Calibration of creates by name (fileio.catalog.create.condition name; anchors as of the commit
# that added them): each mutant must fail an oracle. absentonly drops the check of the namespace's
# removals, so a create rejected because its name was taken applies once the name is freed, from a
# late copy of its record (the ABA a rejection that does not stay one allows, S2); noabsence drops
# the check that the name is free, so two creates of one name staged on one read both apply;
# dropnobump, renamenobump and nsdropnobump free a name (a table dropped, renamed out of its name,
# a child namespace dropped) without counting it in the namespace's removals, which lets the ABA
# through again. The oracles: N1 (TestVerifyStabilityProperties: a rejected action must stay
# rejected on every later state, and every state must be well-formed), the regressions
# (TestCatalogRegressions' CreatesByName, and TestProtoActions' ByNameConflictTests), and the
# multi-writer fuzzer, whose copies of landed records (DUPLICATE) and late landings (DELIVER) must
# change nothing. Locally (2026-11-17) N1 at 1,000 tries catches each (first at try 5, 1, 2, 6 and
# 54 in the order above), the regressions catch each, and the multi-writer fuzzer's general
# properties only noabsence (at try 18 and 201 of its two properties on one object): the ABA
# mutants need a create by name that a log of blind appends took and replay rejected, and a copy
# of it once its name is free, which their schedules did not reach for absentonly in 4 x 3,000
# tries (a create is 4% of the ops they stage). Its property aimed at that shape,
# aRejectedBindingStaysRejectedOnceItsNameIsFree (a few names bound and freed on a shared log of
# blind appends, a copy of each rejected binding once its names are free), alone at 20,000 tries
# (2026-11-18) catches absentonly at try 1, noabsence at 72, dropnobump at 1,209, renamenobump at
# 6,027 and nsdropnobump at 6,400 (earlier runs of it: absentonly at 8 and 268, dropnobump at 1,
# renamenobump at 486, nsdropnobump at 274, and the last two not in 3,000 once each), and fails
# nothing unmutated in 20,000 tries.
MUTANTS="absentonly noabsence dropnobump renamenobump nsdropnobump"
EQUIVALENT=""
ORACLES=TestVerifyStabilityProperties,TestCatalogRegressions,TestProtoActions,TestMultiWriterProperties

prepare() {
  tries $1/$T/spec/TestVerifyStabilityProperties.java 1000
  tries $1/$T/spec/TestMultiWriterProperties.java 300 300 300 300 20000
}

mutant() { # name main-dir test-dir
  local M=$2
  case $1 in
    # a create by name holds whatever names were freed since its read
    absentonly) mutate $M/ProtoCodec.java "    if (builder.namespaceRemovals(nsId) != read) {
      return \"name: a name freed since\";
    }" "" ;;
    # a create by name holds whether or not its name is taken
    noabsence) mutate $M/ProtoCodec.java "    if (table ? builder.tableNameTaken(nsId, name) : builder.namespaceNameTaken(nsId, name)) {
      return \"name: taken\";
    }" "" ;;
    # a table's drop frees its name without counting it
    dropnobump) mutate $M/ProtoCodec.java "      builder.bumpNamespaceRemovals(parentNsId);
" "" ;;
    # a rename frees the table's old name without counting it
    renamenobump) mutate $M/ProtoCodec.java "      builder.bumpNamespaceRemovals(oldNsId);
" "" ;;
    # a namespace's drop frees its name in its parent without counting it
    nsdropnobump) mutate $M/ProtoCodec.java "      builder.bumpNamespaceRemovals(parentId);
" "" ;;
  esac
}
