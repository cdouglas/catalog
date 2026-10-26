# Calibration of conflict-encoding phase 4 (partition-vector, manifest-window), and of the merge's
# rules for Bugs 75 and 76 (anchors as of ae7ec8e, and of the Bug 76 commit for those): each
# mutant must fail an oracle.
MUTANTS="pvnochecks pvrange pvnoremoved pvlower mwnowindow mwskiphead mwdataonly opaqueok
  nodropcheck nonullrowids noparentlist"
# Equivalent under this code's writers, so no oracle can catch them: Bug 76's rules for a record
# that gives row ids to a manifest it kept. A commit that would change a manifest an inline list
# holds writes a metadata file instead (LogCatalog.restatesAManifest), so a delta does so only
# for a manifest no inline list held when its writer read the table: its parent's list was a
# file. Where it lands, that parent still has none (noparentlist's rule refuses it), or a return
# to inline storage listed the manifest again without row ids (nonullrowids' rule refuses it). A
# probe of every rebase property (300 tries each, 2026-10-26) admitted no record where either
# rule held. They guard against other writers' records.
EQUIVALENT="norestate noshiftkept"
ORACLES=TestRebaseProperties,TestPartitionVectorEncoding,TestManifestWindowEncoding,TestFootprints,TestMultiWriterProperties

prepare() {
  tries $1/$T/TestRebaseProperties.java 800
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
    # Bug 75: the merge keeps delete files older than every data file, which a retry drops
    nodropcheck) mutate $M/Landing.java "        return \"its retry would drop delete files older than every data file it keeps\";" "        return null;" ;;
    # Bug 76: the merge lands on a head whose data manifests have no row ids
    nonullrowids) mutate $M/Landing.java "        if (manifest.content() == ManifestContent.DATA && manifest.firstRowId() == null) {" "        if (false) {" ;;
    # Bug 76: a record changes the entry the table's lists share for a manifest's path
    norestate) mutate $M/Landing.java "        if (held != null && !Objects.equals(held.firstRowId(), manifest.firstRowId())) {" "        if (false) {" ;;
    # Bug 76: the row ids a record gave the manifests it kept do not move with its own
    noshiftkept) mutate $M/Landing.java "    if (firstRowId != null && baseNextRowId != null && firstRowId >= baseNextRowId) {" "    if (firstRowId != null && baseNextRowId != null && firstRowId >= baseNextRowId
        && added.contains(manifest.snapshotId())) {" ;;
    # Bug 79: the merge builds a snapshot's list on a parent's inline list that is gone
    noparentlist) mutate $M/Landing.java "      if (add.manifestListSuffix.isEmpty() && parent != null && !adds.containsKey(parent)" "      if (false && parent != null && !adds.containsKey(parent)" ;;
  esac
}
