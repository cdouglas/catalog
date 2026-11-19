# Calibration of views (docs/VIEWS.md; anchors as of the commit that added them): each mutant must
# fail an oracle. viewnotable lets a view's create or rename take a table's name, and tablenoview a
# table's create or rename take a view's (V7: the two kinds share a namespace's names, which only
# the record's check where it lands holds against a writer that read before the other landed);
# viewdropnoremovals frees a view's name without counting it in its namespace's removals, so a
# create by name rejected while the view held it applies from a late copy once the view is dropped
# (the cross-kind create ABA); viewnobump creates a view without moving its namespace's version,
# so a namespace drop staged on a read without it lands over it (V8); viewstale applies a view's
# update on a version other than the one it read (V6); viewdeletefirst deletes a pointer view's
# metadata file before its drop lands, so a drop whose outcome is unknown, or that does not land,
# leaves a view whose metadata is gone (V9); txnviewstale stages a catalog transaction's view
# change on the version its commit reads, not the one the transaction loaded (VIEWS.md "Stage 2");
# txnkeptview drops a kept inline view in a catalog transaction without writing it to a file. The
# oracles: N1 (TestVerifyStabilityProperties: a
# rejected action stays rejected, every state well-formed, design.md I13 among it), the multi-writer
# fuzzer (serial re-execution, copies of rejected bindings once their names are free), the fault
# and race fuzzers through Iceberg's API, the regressions (TestViewActions, TestCatalogRegressions'
# Views) and Iceberg's ViewCatalogTests in every mode (TestLocalViewCatalog).
#
# Locally (2026-11-18, each oracle alone, default tries, shrinking off): TestViewActions and
# TestCatalogRegressions' Views catch every mutant but viewdeletefirst, which only the latter
# catches (aPointerViewsFileGoesWithAnAcknowledgedDropOnly). N1 at 1,000 tries catches viewnotable
# (try 2), tablenoview (13), viewdropnoremovals (12), viewnobump (5) and viewstale (159). The
# multi-writer fuzzer at 300 tries catches viewnotable (aRejectedBindingStaysRejectedOnceItsNameIsFree
# at 17), tablenoview (the shared log's history at 23, the binding property at 188), viewnobump
# (the binding property at 53) and viewstale (the shared log's history at 71); viewdropnoremovals
# only the binding property, at try 321 of 3,000. The fault fuzzer at 100 tries catches viewnotable
# (catalogMatchesModel at 68); the race fuzzer at 1,000 tablenoview (at 386); ViewCatalogTests
# tablenoview. Neither fuzzer caught viewdeletefirst, the fault fuzzer not in 1,000 tries of each
# property: it needs a drop of a pointer view whose gc is on, that a fault leaves unlanded.
# Locally (2026-11-19, fileio d071ec8, errata T8 (fuzzer transactions lack views) fixed):
# txnviewstale fails the fault fuzzer's catalogMatchesModel at try 1,426 of 2,000 (an acknowledged
# transaction updated a view changed since it loaded it) and CatalogTransactionTests'
# aViewChangeFailsWhereTheViewChangedSince in every mode; not the race fuzzer in 3,000 tries of its
# transaction property, whose racers run after the commit's own read, where the record's version
# binding rejects the change anyway. txnkeptview fails catalogMatchesModel at try 52,
# faultyStorageNeverMisreportsACommit at 753, and TestCatalogRegressions'
# aTransactionsViewDropTreatsItsFilesAsDropViewDoes.
MUTANTS="viewnotable tablenoview viewdropnoremovals viewnobump viewstale viewdeletefirst txnviewstale txnkeptview"
EQUIVALENT=""
ORACLES=TestVerifyStabilityProperties,TestMultiWriterProperties,TestCatalogFaultProperties,TestCatalogRaceProperties,TestViewActions,TestCatalogRegressions,TestLocalViewCatalog

prepare() {
  tries $1/$T/spec/TestVerifyStabilityProperties.java 1000
  tries $1/$T/spec/TestMultiWriterProperties.java 300 300 300 300 3000
  tries $1/$T/TestCatalogFaultProperties.java each 300
  tries $1/$T/TestCatalogRaceProperties.java 1000 100
}

mutant() { # name main-dir test-dir
  local M=$2
  case $1 in
    # a view's create or rename takes a name a table holds
    viewnotable) mutate $M/ProtoCodec.java "        return builder.viewNameTaken(nsId, name) || builder.tableNameTaken(nsId, name);" \
      "        return builder.viewNameTaken(nsId, name);" ;;
    # a table's create or rename takes a name a view holds
    tablenoview) mutate $M/ProtoCodec.java "        return builder.tableNameTaken(nsId, name) || builder.viewNameTaken(nsId, name);" \
      "        return builder.tableNameTaken(nsId, name);" ;;
    # a view's drop frees its name without counting it
    viewdropnoremovals) mutate $M/ProtoCodec.java "      builder.bumpNamespaceRemovals(nsId);
" "" ;;
    # a view's create leaves its namespace's version
    viewnobump) mutate $M/ProtoCodec.java "      // As a table create: the namespace's children changed
      builder.bumpNamespaceVersion(namespaceId);
" "" ;;
    # a view's update applies on any version of the view
    viewstale) mutate $M/ProtoCodec.java "      return metadata.wellFormed() && builder.viewVersion(id) == version;" \
      "      return metadata.wellFormed() && builder.viewVersion(id) >= 0;" ;;
    # a drop deletes a pointer view's metadata file before its record lands
    viewdeletefirst) mutate $M/LogCatalog.java "      ViewMetadata dropped = entry.isInline() ? null : droppedMetadata(identifier, entry);
" "      ViewMetadata dropped = entry.isInline() ? null : droppedMetadata(identifier, entry);
      if (dropped != null) {
        CatalogUtil.dropViewMetadata(io, dropped);
      }
" ;;
    # a catalog transaction changes a view on the version the commit reads, not the one it loaded
    txnviewstale) mutate $M/LogCatalog.java "      if (!java.util.Objects.equals(
          LogViewOperations.location(current.state(), viewId)," "      if (false && !java.util.Objects.equals(
          LogViewOperations.location(current.state(), viewId)," ;;
    # a catalog transaction's drop of a kept inline view writes it to no file
    txnkeptview) mutate $M/LogCatalog.java "          keptView(viewId, entry, newCatalog::wroteAhead);
" "" ;;
  esac
}
