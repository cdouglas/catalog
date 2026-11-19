# Calibration of the replicas a commit returns and the reads that continue them (the 2026-11-17
# review's B2, the gap plan's item 2.3): each mutant must fail a commit-protocol fuzzer. A
# catalog's handles live on the replica its commit returned, and its loads and refreshes read on
# from the replica they hold (read(floor), refresh, newer), so a wrong one is a stale or
# impossible state a handle stages its next transaction on.
MUTANTS="validatecurrent readnonewer refreshnofollows"
EQUIVALENT=""
ORACLES=TestMemoryCommitProtocol,TestAtomicFileCommitProtocol

RT=intentlog-core/src/main/java/org/intentlog/LogRuntime.java

prepare() {
  tries $1/intentlog-memory/src/test/java/org/intentlog/memory/TestMemoryCommitProtocol.java 300000
  tries $1/intentlog-fileio/src/test/java/org/intentlog/fileio/TestAtomicFileCommitProtocol.java 300000
}

mutant() { # name main-dir test-dir (the catalog's; these mutants are intentlog's)
  case $1 in
    # a commit returns the replica it last adopted, not the one its record landed in
    validatecurrent) mutate $RT "      return result.replica();
    }

    private CommitRejectedException conflict(" "      return current;
    }

    private CommitRejectedException conflict(" ;;
    # a read with a floor returns a read that lags behind it (Bug 72's monotonic reads)
    readnonewer) mutate $RT "    return newer(floor, read);" "    return read;" ;;
    # a refresh folds records onto a replica whose tail they do not follow
    refreshnofollows) mutate $RT "    // Records alone: fold them only onto the tail they follow
    checkFollows(view, prior.tail());" "    // Records alone: fold them only onto the tail they follow" ;;
  esac
}
