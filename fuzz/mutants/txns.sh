# Calibration of catalog transactions beyond appends (the 2026-11-17 review's A6, and C3's catalog
# half; the gap plan's item 1.4; anchors as of its commit, renamebeforeuuid's as of Bug 124's, which
# stages renames after the loop, and of views' stage 2, whose view loop stages its renames alike): each mutant must fail an oracle. Run at
# SNAPSHOT_BLOCK=2 SNAPSHOT_WINDOW=1. Before the item no fuzzer staged a drop, rename, create or
# namespace change in a catalog transaction: each mutant passed 300 tries of each fault property
# and 300/100 of the race fuzzer's. Tries below are the fault fuzzer's,
# faultyStorageNeverMisreportsACommit's first.
#
# nsdropfirst (a transaction's namespace drops staged before its table drops) fails at tries 261
# and 70: the progress rule (a commit refused as "not empty" whose effect applies, nothing racing
# it). txnpurgeafter, Bug 96's twin of drops.sh's purgeafter (the version a transaction purges read
# only after its commit lands, when the catalog no longer has it), fails at 43 and 993: the purge
# left the table's manifests and lists.
#
# Not reached at 1,000 tries of each (and the race fuzzer's 1,000/200), so not in the set:
# keptunknown (a transaction's kept versions deleted when its outcome is unknown too: it needs a
# non-purge drop in a transaction whose commit is unknown yet landed, which the registerable check
# would catch), and renamereplaced (a transaction's rename applied to a table replaced since it was
# loaded: it needs a drop and a create of the name between the load and the commit).
MUTANTS="nsdropfirst txnpurgeafter"
# The plan's "a staged rename applied before the uuid check": a failed check throws before the
# mutation commits, and the mutation keys a table's updates by the name the transaction read, so
# the order of staging changes nothing (it passed 1,000 tries of each, after)
EQUIVALENT="renamebeforeuuid"
ORACLES=TestCatalogFaultProperties

prepare() {
  # 10,000: txnpurgeafter fails catalogMatchesModel at try 993 (C8: ten times the margin)
  tries $1/$T/TestCatalogFaultProperties.java each 10000
}

mutant() { # name main-dir test-dir
  local M=$2
  case $1 in
    # a transaction's namespace drops are staged before its table drops
    nsdropfirst) mutate $M/LogCatalog.java "        case DROP:
          break;
      }
    }

    // The tables dropped keeping their files" "        case DROP:
          newCatalog.dropNamespace(change.namespace());
          break;
      }
    }

    // The tables dropped keeping their files" \
      && mutate $M/LogCatalog.java "    for (NamespaceChange change : commit.namespaceChanges()) {
      if (change.kind() == NamespaceChange.Kind.DROP) {
        newCatalog.dropNamespace(change.namespace());
      }
    }
" "" ;;
    # a transaction's rename is staged before the check that the table is the one it read, and
    # before the table's updates
    renamebeforeuuid) mutate $M/LogCatalog.java "      if (!java.util.Objects.equals(currentMetadata.uuid(), change.base().uuid())) {
        throw new CommitFailedException(
            \"Cannot commit transaction: table %s was replaced since the transaction read it\",
            tableId);
      }

      if (change.dropped()) {" "      if (change.renamedTo() != null) {
        newCatalog.renameTable(tableId, change.renamedTo());
      }
      if (!java.util.Objects.equals(currentMetadata.uuid(), change.base().uuid())) {
        throw new CommitFailedException(
            \"Cannot commit transaction: table %s was replaced since the transaction read it\",
            tableId);
      }

      if (change.dropped()) {" \
      && mutate $M/LogCatalog.java "          newCatalog.pin(tableId);
        }
      }

      if (change.renamedTo() != null) {
        renames.add(change);
      }
    }" "          newCatalog.pin(tableId);
        }
      }
    }" ;;
    # a transaction's rename applies to whatever table has the name when it commits, though it was
    # replaced since the transaction loaded it (its uuid unchecked)
    renamereplaced) mutate $M/LogCatalog.java "      if (!java.util.Objects.equals(currentMetadata.uuid(), change.base().uuid())) {
        throw new CommitFailedException(
            \"Cannot commit transaction: table %s was replaced since the transaction read it\",
            tableId);
      }

      if (change.dropped()) {" "      if (change.renamedTo() == null
          && !java.util.Objects.equals(currentMetadata.uuid(), change.base().uuid())) {
        throw new CommitFailedException(
            \"Cannot commit transaction: table %s was replaced since the transaction read it\",
            tableId);
      }

      if (change.dropped()) {" ;;
    # the version a transaction's drop kept is deleted when the commit's outcome is unknown too
    keptunknown) mutate $M/LogCatalog.java "  // SupportsCatalogTransaction
" "  static final List<String> KEPT = new java.util.concurrent.CopyOnWriteArrayList<>();

  // SupportsCatalogTransaction
" \
      && mutate $M/LogCatalog.java "      keptVersion(tableId, current, newCatalog::wroteAhead);" "      keptVersion(tableId, current, file -> {
        newCatalog.wroteAhead(file);
        KEPT.add(file);
      });" \
      && mutate $M/LogCatalog.java "    } catch (RuntimeException e) {
      cleanup.delete(newCatalog.abandonedFiles());
      throw e;
    }

    // The commit landed: a purge" "    } catch (RuntimeException e) {
      cleanup.delete(newCatalog.abandonedFiles());
      if (e instanceof org.apache.iceberg.exceptions.CommitStateUnknownException) {
        cleanup.delete(List.copyOf(KEPT));
      }
      KEPT.clear();
      throw e;
    }
    KEPT.clear();

    // The commit landed: a purge" ;;
    # Bug 96's twin of drops.sh's purgeafter: a transaction reads the version it purges only
    # after its commit lands, when the catalog no longer has it
    txnpurgeafter) mutate $M/LogCatalog.java "          purged.put(tableId, purgedVersion(tableId, ops::current));" "          purged.put(tableId, null);" \
      && mutate $M/LogCatalog.java "    purged.forEach(this::purge);" "    purged.keySet().forEach(id -> {
      TableMetadata version = null;
      try {
        version = purgedVersion(id, log.read());
      } catch (RuntimeException e) {
        // gone
      }
      purge(id, version);
    });" ;;
  esac
}
