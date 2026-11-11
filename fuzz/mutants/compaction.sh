# Calibration of the compaction after the commit on one object (docs/TAIL_READS.md "Compaction
# after the commit"; anchors as of its commit): each mutant must fail the one-object commit-protocol
# fuzzer, whose writers compact after their commits in the runs whose seeds draw it, and the
# multi-writer fuzzer, which checks a compaction apart from the commit before it (each caught in
# 5,000 and 300 tries on the dev box; the model's oracle matrix says the fault and race fuzzers
# reach compactbase alone). They are the TLA+ switches of CatalogCommit: compactbase (a checkpoint
# of the replica the commit staged on, before its record, conditioned on the version its record
# landed in), compactresend (here twice: compactprobe, a compaction conditioned on the object as a
# probe finds it, not on the version it checkpoints; and compactnocopy, one sent again on the log
# as it grew without the records that beat it), compactfail (a lost compaction fails the commit),
# compactcopyafter (one sent again with those records after its GC record and PageOuts) and
# compactreplaced (one sent again on a log that was replaced since, read whole, its records past
# the count the lost version had taken for growth).
MUTANTS="compactbase compactprobe compactnocopy compactfail compactcopyafter compactreplaced"
EQUIVALENT=""
ORACLES=TestAtomicFileCommitProtocol,TestMultiWriterProperties

RT=intentlog-core/src/main/java/org/intentlog/LogRuntime.java

prepare() {
  tries $1/intentlog-fileio/src/test/java/org/intentlog/fileio/TestAtomicFileCommitProtocol.java 300000
  tries $1/iceberg-fileio-catalog/src/test/java/org/apache/iceberg/io/spec/TestMultiWriterProperties.java 3000
}

mutant() { # name main-dir test-dir (the catalog's; these mutants are intentlog's)
  case $1 in
    compactbase) mutate $RT "      compactAfter(landed);
" "      compactAfter(new Replica<>(base.state(), base.committed(), landed.tail(),
          landed.intentionRecords(), landed.logId()));
" ;;
    compactprobe) mutate $RT "          sendThrottled(() -> log.replace(target.tail(), replace.checkpoint, records));" "          sendThrottled(() -> log.replace(log.probe(), replace.checkpoint, records));" ;;
    compactnocopy) mutate $RT "          copied.addAll(grown.records);
" "" ;;
    compactfail) mutate $RT "        } catch (LogConflictException e) {
          CommitTrace.event(CommitTrace.Kind.LOST, 0, 0);
          if (resends >= MAX_COMPACTION_RESENDS) {" "        } catch (LogConflictException e) {
          if (true) {
            throw new CommitRejectedException(CommitRejectedException.Reason.CONFLICT, null, \"lost\", e);
          }
          if (resends >= MAX_COMPACTION_RESENDS) {" && mutate $RT "    } catch (CommitOutcomeUnknownException e) {
      // interrupted in a backoff" "    } catch (CommitRejectedException e) {
      throw e;
    } catch (CommitOutcomeUnknownException e) {
      // interrupted in a backoff" ;;
    compactcopyafter) mutate $RT "copied.isEmpty() ? replace.records : concat(copied, replace.records);" "copied.isEmpty() ? replace.records : concat(replace.records, copied);" ;;
    compactreplaced) mutate $RT "      int from = continued(lost.view(), view, null);
      if (from < 0) {
        return null;
      }" "      int from = lost.view() == null ? 0
          : Math.min(lost.view().records().size(), view.records().size());" ;;
  esac
}
