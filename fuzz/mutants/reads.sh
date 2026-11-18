# Calibration of the oracle of serializable reads (TestRebaseProperties
# .anAdmittedReadSeesTheRowsItRead; docs/CATALOG_TRANSACTIONS.md "No false negatives"; anchors as
# of its commit): each mutant admits reads whose rows changed, and must fail it. A read is checked
# twice: at the commit's read of the catalog (TableRead.unchangedIn, from the version the
# transaction first read), then where the record lands (a read witness, which the encoding judges,
# or a read pin, from the commit's read). readunchecked skips the first; readunheld writes neither
# witness nor pin; readnoadvance and readnoreset make concern-vector's witness (and its
# descendants') read only a ref's resets or only its advances; lmpreadall makes
# lamport-minus-properties' witness read nothing. readfilter judges a scan read at the commit's
# read by its filter, as the encodings are to judge it (errata D12), but reads only the data files
# added to the ref, not its delete files, removals or resets: what the oracle must catch once the
# encodings judge filters.
MUTANTS="readunchecked readunheld readnoadvance readnoreset lmpreadall readfilter"
EQUIVALENT=""
ORACLES=TestRebaseProperties#anAdmittedReadSeesTheRowsItRead

prepare() {
  tries $1/$T/TestRebaseProperties.java each 3000
}

mutant() { # name main-dir test-dir
  local M=$2
  case $1 in
    # the commit's read does not check the reads
    readunchecked) mutate $M/LogCatalog.java "      if (!read.unchangedIn(now) || scanSchemaChanged(read, now)) {" "      if (false) {" ;;
    # the record holds no read where it lands
    readunheld) mutate $M/LogCatalog.java "      if (witness && then != null) {
        newCatalog.readRef(read.identifier(), read.ref());
      } else {
        newCatalog.readTable(read.identifier());
      }" "" ;;
    # a read witness misses its refs' advances
    readnoadvance) mutate $M/ConcernVectorEncoding.java "      witness.ref(ref, ConcernStamps.Ref.ADVANCE, ConcernStamps.Ref.RESET);" "      witness.ref(ref, ConcernStamps.Ref.RESET);" ;;
    # a read witness misses its refs' resets
    readnoreset) mutate $M/ConcernVectorEncoding.java "      witness.ref(ref, ConcernStamps.Ref.ADVANCE, ConcernStamps.Ref.RESET);" "      witness.ref(ref, ConcernStamps.Ref.ADVANCE);" ;;
    # lamport-minus-properties' read witness reads nothing
    lmpreadall) mutate $M/LamportMinusPropertiesEncoding.java "  Witness readWitness(int base, java.util.List<String> refs) {
    return Witness.of(base).concern(REST);" "  Witness readWitness(int base, java.util.List<String> refs) {
    return Witness.of(base);" ;;
    # a scan read is unchanged at the commit's read where no data file added to its ref since its
    # snapshot might match its filter: delete files, removals and resets go unread
    readfilter) mutate $M/LogCatalog.java "      if (!read.unchangedIn(now) || scanSchemaChanged(read, now)) {" "      if ((!read.unchangedIn(now) && !addsNoDataUnder(read, now))
          || scanSchemaChanged(read, now)) {" \
      && mutate $M/LogCatalog.java "  private TableMetadata currentMetadata(TableIdentifier table, Replica<CatalogState> current) {" "  private boolean addsNoDataUnder(TableRead read, TableMetadata now) {
    if (now == null || !read.isScan() || read.snapshotId() == null
        || !java.util.Objects.equals(now.uuid(), read.metadata().uuid())) {
      return false;
    }
    SnapshotRef ref = now.ref(read.ref());
    if (ref == null || !org.apache.iceberg.util.SnapshotUtil.isAncestorOf(
        ref.snapshotId(), read.snapshotId(), now::snapshot)) {
      return false;
    }
    org.apache.iceberg.expressions.InclusiveMetricsEvaluator under =
        new org.apache.iceberg.expressions.InclusiveMetricsEvaluator(
            now.schema(), read.filter(), read.caseSensitive());
    for (Snapshot s : org.apache.iceberg.util.SnapshotUtil.ancestorsBetween(
        ref.snapshotId(), read.snapshotId(), now::snapshot)) {
      for (org.apache.iceberg.DataFile file : s.addedDataFiles(io)) {
        if (under.eval(file)) {
          return false;
        }
      }
    }
    return true;
  }

  private TableMetadata currentMetadata(TableIdentifier table, Replica<CatalogState> current) {" ;;
  esac
}
