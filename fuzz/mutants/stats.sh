# Calibration of statistics files in a delta (errata D9 (statistics force full mode)'s fix; anchors
# as of this set's last commit): each mutant must fail an oracle. statsnoremove sends no removal of
# a statistics file of a snapshot the commit keeps: a delta with another update replays to a table
# that keeps the file its writer dropped, and a removal alone leaves the delta empty, so the commit
# goes as full metadata, which carries it (D9 again, which TestFullUpdates'
# aStatisticsChangeIsADelta refuses); pstatsnoremove is its partition statistics twin.
# statsnativeset makes native replay drop a set statistics file, which the builder sets (the replay
# check, -Dfileio.catalog.replay.check, compares them on every apply); statspartition sends a
# partition statistics file as a table statistics removal of its snapshot (the replayed table lacks
# the writer's partition file). The removals reach the properties through TableScript's statistics
# steps, which remove a file the table has and, in half of the removals, set another snapshot's in
# the same commit. The differential misses statsnoremove now and then in 600 tries (a table out of
# inline storage sends no delta); the delta property is the one sized for it.
MUTANTS="statsnoremove pstatsnoremove statsnativeset statspartition"
EQUIVALENT=""
ORACLES=TestInlineDeltaProperties,TestInlineDifferentialProperties,TestFullUpdates

prepare() {
  # 5,000: statsnoremove fails the delta property at try 384 (C8: ten times the margin)
  tries $1/$T/TestInlineDeltaProperties.java 5000
  tries $1/$T/TestInlineDifferentialProperties.java 600
}

mutant() { # name main-dir test-dir
  local M=$2
  case $1 in
    # a statistics file the commit dropped of a snapshot it keeps is not removed
    statsnoremove) mutate $M/InlineDeltaCodec.java "      if (!kept.contains(snapshotId) && !gone.contains(snapshotId)) {
        updates.add(new RemoveStatisticsUpdate(snapshotId));" "      if (false) {
        updates.add(new RemoveStatisticsUpdate(snapshotId));" ;;
    # ... nor a partition statistics file
    pstatsnoremove) mutate $M/InlineDeltaCodec.java "      if (!keptPartition.contains(snapshotId) && !gone.contains(snapshotId)) {
        updates.add(new RemovePartitionStatisticsUpdate(snapshotId));" "      if (false) {
        updates.add(new RemovePartitionStatisticsUpdate(snapshotId));" ;;
    # native replay drops a statistics file its delta sets
    statsnativeset) mutate $M/NativeDelta.java "        statistics.put(file.snapshotId(), ImmutableList.of(file));" "" ;;
    # a partition statistics file goes as a removal of its snapshot's table statistics
    statspartition) mutate $M/InlineDeltaCodec.java "        updates.add(new SetPartitionStatisticsUpdate(file));" "        updates.add(new RemoveStatisticsUpdate(file.snapshotId()));" ;;
  esac
}
