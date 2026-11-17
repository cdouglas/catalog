# Calibration of a drop that keeps an inline table's files (errata T4's fix; anchors as of its
# commit): each mutant must fail an oracle. dropnowrite writes nothing first (the dropped table
# cannot be registered); dropnodelete leaves what a drop that lost its race wrote (stray metadata
# and list files beside the ones its retry writes).
MUTANTS="dropnowrite dropnodelete"
EQUIVALENT=""
ORACLES=TestCatalogRegressions,TestCatalogRaceProperties,TestCatalogFaultProperties

prepare() {
  tries $1/$T/TestCatalogRaceProperties.java 20000 1000
  tries $1/$T/TestCatalogFaultProperties.java each 1500
}

mutant() { # name main-dir test-dir
  local M=$2
  case $1 in
    # the drop writes nothing before its record
    dropnowrite) mutate $M/LogCatalog.java "    return tableOpsAsRead(identifier, read).materialize();" "    return List.of();" ;;
    # what a drop that did not land wrote stays
    dropnodelete) mutate $M/LogCatalog.java "        io.deleteFile(file);" "" ;;
  esac
}
