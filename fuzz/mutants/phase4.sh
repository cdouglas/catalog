# Calibration of conflict-encoding phase 4 (partition-vector, manifest-window), on its commit
# (anchors as of ae7ec8e): each mutant must fail an oracle.
MUTANTS="pvnochecks pvrange pvnoremoved pvlower mwnowindow mwskiphead mwdataonly opaqueok"
ORACLES=TestRebaseProperties,TestPartitionVectorEncoding,TestManifestWindowEncoding,TestFootprints,TestMultiWriterProperties

# TestRebaseProperties: anAdmitted, aMerged (off), aBlind (off), aValidated
prepare() {
  tries $1/$T/TestRebaseProperties.java 800 1 1 800
  tries $1/$T/ConflictEncodingContract.java 400
}

mutant() { # name main-dir test-dir
  local M=$2
  case $1 in
    # partition-vector reads none of a validated record's checks
    pvnochecks) mutate $M/PartitionVectorEncoding.java "    for (Footprint.Check check : change.checks) {" "    for (Footprint.Check check : java.util.List.<Footprint.Check>of()) {" ;;
    # a filter that names no partitions of a spec reads none of its buckets, not every one
    pvrange) mutate $M/PartitionKey.java "          if (partitions == null) {
            return Set.of(ALL);
          }" "          if (partitions == null) {
            continue;
          }" ;;
    # data files removed are never stamped
    pvnoremoved) mutate $M/BlindAppendEncoding.java "        if (manifest.hasDeletedFiles()) {" "        if (false) {" ;;
    # a manifest that spans partitions is stamped in its lower bound's bucket only
    pvlower) mutate $M/PartitionKey.java "      if (field.containsNull() || lower == null || !lower.equals(upper)" "      if (field.containsNull() || lower == null" ;;
    # manifest-window admits whatever its window holds
    mwnowindow) mutate $M/ManifestWindowEncoding.java "      TableMetadata table = landing.metadata();
      SnapshotRef ref = table.ref(branch);" "      if (landing != null) {
        return true;
      }
      TableMetadata table = landing.metadata();
      SnapshotRef ref = table.ref(branch);" ;;
    # manifest-window skips the window's newest snapshot (the head)
    mwskiphead) mutate $M/ManifestWindowEncoding.java "      for (Snapshot snapshot : window) {" "      for (Snapshot snapshot : window.subList(Math.min(1, window.size()), window.size())) {" ;;
    # manifest-window reads only data added
    mwdataonly) mutate $M/ManifestWindowEncoding.java "          case NO_DELETES_ADDED:
            did = manifest.content() == ManifestContent.DELETES && manifest.hasAddedFiles();
            break;
          default:
            did = manifest.content() == ManifestContent.DATA && manifest.hasDeletedFiles();" "          default:
            did = false;" ;;
    # an opaque producer's snapshot counts as validated, with no checks
    opaqueok) mutate $M/TableChange.java "      validated &= kind != Footprint.Kind.OPAQUE;" "" ;;
  esac
}
