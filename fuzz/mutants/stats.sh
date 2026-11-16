# Calibration of statistics files in a delta (errata D9's fix; anchors as of its commit): each
# mutant must fail an oracle. statsnoremove sends no removal of a statistics file (the replayed
# table keeps a file its writer's dropped); statsnativeset makes native replay drop a set
# statistics file, which the builder sets (the replay check, -Dfileio.catalog.replay.check, compares
# them on every apply); statspartition sends a partition statistics file as a table statistics
# removal of its snapshot (the replayed table lacks the writer's partition file).
MUTANTS="statsnoremove statsnativeset statspartition"
EQUIVALENT=""
ORACLES=TestInlineDeltaProperties,TestInlineDifferentialProperties

prepare() {
  tries $1/$T/TestInlineDeltaProperties.java 3000
  tries $1/$T/TestInlineDifferentialProperties.java 600
}

mutant() { # name main-dir test-dir
  local M=$2
  case $1 in
    # a statistics file the commit dropped of a snapshot it keeps is not removed
    statsnoremove) mutate $M/InlineDeltaCodec.java "      if (!kept.contains(snapshotId) && !gone.contains(snapshotId)) {
        updates.add(new RemoveStatisticsUpdate(snapshotId));" "      if (false) {
        updates.add(new RemoveStatisticsUpdate(snapshotId));" ;;
    # native replay drops a statistics file its delta sets
    statsnativeset) mutate $M/NativeDelta.java "        statistics.put(file.snapshotId(), ImmutableList.of(file));" "" ;;
    # a partition statistics file goes as a removal of its snapshot's table statistics
    statspartition) mutate $M/InlineDeltaCodec.java "        updates.add(new SetPartitionStatisticsUpdate(file));" "        updates.add(new RemoveStatisticsUpdate(file.snapshotId()));" ;;
  esac
}
