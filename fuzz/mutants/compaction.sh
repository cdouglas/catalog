# Calibration of the compaction after the commit on one object (docs/TAIL_READS.md "Compaction
# after the commit"; anchors as of its commit): each mutant must fail the one-object commit-protocol
# fuzzer, whose writers compact after their commits in the runs whose seeds draw it, and the
# multi-writer fuzzer, which checks a compaction apart from the commit before it (each caught in
# 5,000 and 300 tries on the dev box; the model's oracle matrix says the fault and race fuzzers
# reach compactbase alone). They are the
# TLA+ switches of CatalogCommit: compactbase (a checkpoint of the replica the commit staged on,
# before its record, conditioned on the version its record landed in), compactresend (here a
# compaction conditioned on the object as a probe finds it, not on the version it checkpoints, as a
# resend after a lost race would be) and compactfail (a lost compaction fails the commit).
MUTANTS="compactbase compactprobe compactfail"
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
    compactprobe) mutate $RT "      sendThrottled(() -> log.replace(landed.tail(), replace.checkpoint, replace.records));" "      sendThrottled(() -> log.replace(log.probe(), replace.checkpoint, replace.records));" ;;
    compactfail) mutate $RT "    } catch (LogConflictException e) {
      CommitTrace.event(CommitTrace.Kind.LOST, 0, 0);
    } catch (CommitOutcomeUnknownException e) {" "    } catch (LogConflictException e) {
      throw new CommitRejectedException(CommitRejectedException.Reason.CONFLICT, null, \"lost\", e);
    } catch (CommitOutcomeUnknownException e) {" ;;
  esac
}
