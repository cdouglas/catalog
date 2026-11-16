# Calibration of a commit's adoption of the catalog it landed in (errata D20) and, for a pointer
# table, of the metadata file it wrote (errata D26: the table's operations hold that file's
# metadata rather than reading it back, where the landed catalog names that file; anchors as of
# the D26 commit): each mutant must fail an oracle. adoptany holds the written metadata whatever
# file the landed catalog names (a replica that folded another writer's later update of the table
# would keep this commit's metadata under that writer's file: the next commit on the handle loses
# that writer's update); adoptunlisted holds the metadata as the commit staged it, before its
# inline-ML snapshots' lists were written to files (what a table leaving inline storage, or a
# writer that does not inline committing to an inline-ML table, names differs from its file).
MUTANTS="adoptany adoptunlisted"
EQUIVALENT=""
ORACLES=TestCatalogFaultProperties,TestCatalogRaceProperties,TestInlineDifferentialProperties,TestCommitReads

prepare() {
  tries $1/$T/TestCatalogFaultProperties.java each 1500
  tries $1/$T/TestCatalogRaceProperties.java 20000 1000
  tries $1/$T/TestInlineDifferentialProperties.java 600
}

mutant() { # name main-dir test-dir
  local M=$2
  case $1 in
    # the held metadata stands for whatever file the landed catalog names
    adoptany) mutate $M/LogCatalog.java "        if (written != null && metadataFile.equals(written.metadataFileLocation())) {" "        if (written != null) {" ;;
    # the held metadata is the commit's, before its inline-ML snapshots' lists were written
    adoptunlisted) mutate $M/LogCatalog.java "      written = TableMetadata.buildFrom(file)" "      written = TableMetadata.buildFrom(metadata)" ;;
  esac
}
