#!/usr/bin/env python3
"""Runs TLC on the model configurations and checks each verdict.

Usage: spec/tla/check.py [-k SUBSTRING | -n NAME,...] [--list] [--keep]
                         [--witnesses | --matrix [--tight]]

Each configuration is a base (BASE for CatalogCommit, through MC; LOG_BASE for
SharedLogCommit, through MCShared, for the configurations named log-*) with
overrides. `expect` is "pass", or the name of the property TLC must report
violated (or a set of names, any of which will do). The configurations are
written to states/cfg/ and the TLC output to states/out/<name>.txt.
"""
import argparse
import os
import re
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))

BASE = {
    "Writers": "{1, 2}",
    "Entities": "<- Ents",
    "Shapes": "<- Contend",
    "AppendSupported": "TRUE",
    "MaxCount": "2",
    "MaxSize": "100",
    "SegLimit": "0",
    "MaxAttempts": "3",
    "MaxThrottle": "2",
    "Retention": "<- GcOff",
    "MaxTime": "0",
    "Skew": "<- NoSkew2",
    "MaxFaults": "0",
    "FaultKinds": "{}",
    "ClientRetries": "2",
    "Strict": "FALSE",
    "Sched": '"free"',
    "Paging": "FALSE",
    "Admission": "0",
    "MaxSweep": "0",
    "TailReads": "TRUE",
    "CompactAfter": "FALSE",
    "CompactResends": "1",
    "BlockIds": "1",
    "GridBits": "3",
    "SealEvery": "0",
    "Bugs": "{}",
}

SAFETY = ["Readable", "NoUnheldState", "ExactlyOnce", "Serializable", "UniqueIds",
          "HonestCommitted", "HonestFailed", "UnknownExplained", "SoloCommits", "StampsCount",
          "PagesExist", "PagesExact", "Shape", "OnGrid", "StatePagesExist"]
ACTIONS = ["HistGrows", "CasAppliesItsRecord", "CheckpointIsReplay"]

ALL_FAULTS = ('{"failBefore", "lostReply", "landedConflict", "throttled", '
              '"landedThrottled", "inflight", "readFail"}')
FUZZER_FAULTS = ('{"failBefore", "lostReply", "landedConflict", "throttled", '
                 '"landedThrottled", "readFail"}')
# Paging (COMMITTED_PAGING.md): every CAS pages what its ceiling allows (A = 0, N = 1, a grid
# of 8), a tick so that ids land in two blocks, one sweep.
FPAGE = {"Paging": "TRUE", "Shapes": "<- TwoEach", "MaxCount": "1", "MaxTime": "1",
         "MaxSweep": "1"}
SPAGE = {"Paging": "TRUE", "Shapes": "<- Long", "MaxCount": "1", "MaxTime": "1",
         "MaxSweep": "2", "SealEvery": "1"}
FPAGE_FAULTS = ('{"failBefore", "lostReply", "landedConflict", "throttled", '
                '"landedThrottled", "inflight", "readFail", "pageFail", "putFail", "putLost"}')
# What the catalog fuzzers draw (no late landings), with the page store's faults.
FUZZER_PAGE_FAULTS = ('{"failBefore", "lostReply", "landedConflict", "throttled", '
                      '"landedThrottled", "readFail", "pageFail", "putFail", "putLost"}')

# Compaction after the commit (TAIL_READS.md "Compaction after the commit"): past the limits a
# commit appends, and its writer may compact the version its record landed in (or not: the model
# takes both, as the drawn points do). Every configuration above runs with compaction fused with
# the commit (the ablation, and the path of a full object), as before; the after-* family runs the
# same kinds of schedule with compaction after it. Its count is 1, so that each commit may compact.
# A compaction that lost to appends may be sent again with the records that beat it in front of
# its GC record and PageOuts, once (CompactResends; after-resend* twice).
AFTER = {"CompactAfter": "TRUE", "MaxCount": "1"}

# name -> (overrides, expect)
CONFIGS = {
    "base": ({}, "pass"),
    "cas-only": ({"MaxCount": "0"}, "pass"),
    "gcs": ({"AppendSupported": "FALSE"}, "pass"),
    "size": ({"MaxSize": "3"}, "pass"),
    "two-each": ({"Shapes": "<- TwoEach", "MaxCount": "1"}, "pass"),
    "creates": ({"Shapes": "<- Creates"}, "pass"),
    "faults": ({"MaxFaults": "1", "FaultKinds": ALL_FAULTS}, "pass"),
    "faults-strict": ({"MaxFaults": "1", "FaultKinds": ALL_FAULTS, "Strict": "TRUE"}, "pass"),
    "gc0": ({"Retention": "0", "MaxTime": "2", "Shapes": "<- TwoEach", "MaxCount": "1"}, "pass"),
    "seg": ({"SegLimit": "2", "MaxCount": "10"}, "pass"),
    # Everything at once, at small bounds.
    "mix": ({"Shapes": "<- TwoEach", "MaxCount": "1", "MaxSize": "4", "SegLimit": "3",
             "Retention": "0", "MaxTime": "2", "MaxFaults": "1", "FaultKinds": ALL_FAULTS,
             "Strict": "TRUE"}, "pass"),
    "mix-lenient": ({"Shapes": "<- TwoEach", "MaxCount": "2", "MaxSize": "3", "SegLimit": "2",
                     "Retention": "0", "MaxTime": "2", "MaxFaults": "1",
                     "FaultKinds": ALL_FAULTS}, "pass"),
    "skew": ({"Shapes": "<- TwoEach", "MaxCount": "1", "Retention": "1", "MaxTime": "3",
              "Skew": "<- Behind2", "MaxFaults": "1", "FaultKinds": FUZZER_FAULTS}, "pass"),
    "long": ({"Shapes": "<- Long", "MaxCount": "2", "MaxAttempts": "4"}, "pass"),
    "long-faults": ({"Shapes": "<- Long", "MaxCount": "2", "MaxAttempts": "4", "MaxFaults": "1",
                     "FaultKinds": ALL_FAULTS, "Strict": "TRUE", "Retention": "0",
                     "MaxTime": "1"}, "pass"),
    "faults2": ({"Shapes": "<- TwoEach", "MaxCount": "1", "MaxFaults": "2",
                 "FaultKinds": ALL_FAULTS, "Strict": "TRUE"}, "pass"),
    "three": ({"Writers": "{1, 2, 3}", "Shapes": "<- Three", "Skew": "<- NoSkew3",
               "MaxCount": "1", "MaxFaults": "1", "FaultKinds": ALL_FAULTS, "Strict": "TRUE"},
              "pass"),
    # The fuzzers' schedules (the oracle matrix runs them with each bug).
    "sched-mw": ({"Sched": '"mw"', "Shapes": "<- TwoEach", "MaxCount": "1", "Strict": "TRUE",
                  "MaxFaults": "1", "FaultKinds": FUZZER_FAULTS}, "pass"),
    "sched-fault": ({"Sched": '"fault"', "Shapes": "<- OpsAndTxn", "MaxCount": "1",
                     "Strict": "TRUE", "MaxFaults": "1", "FaultKinds": FUZZER_FAULTS}, "pass"),
    "sched-race": ({"Sched": '"race"', "Writers": "{1, 2, 3}", "Skew": "<- NoSkew3",
                    "Shapes": "<- RaceTxn", "MaxCount": "1", "Strict": "TRUE"}, "pass"),
    "sched-race-op": ({"Sched": '"race"', "Writers": "{1, 2, 3}", "Skew": "<- NoSkew3",
                       "Shapes": "<- RaceOp", "MaxCount": "1"}, "pass"),
    # Conflict encodings: a table's concerns as entities, and blind appends.
    "concerns": ({"Writers": "{1, 2, 3}", "Skew": "<- NoSkew3", "Entities": "<- CEnts",
                  "Shapes": "<- Concerns", "MaxCount": "1"}, "pass"),
    "concerns-faults": ({"Entities": "<- CEnts", "Shapes": "<- Concerns2", "MaxCount": "1",
                         "MaxFaults": "1", "FaultKinds": ALL_FAULTS, "Strict": "TRUE"}, "pass"),
    "blind-appends": ({"Entities": "<- CEnts", "Shapes": "<- BlindAppends", "MaxCount": "1",
                       "MaxFaults": "1", "FaultKinds": ALL_FAULTS}, "pass"),
    "validated": ({"Writers": "{1, 2, 3}", "Skew": "<- NoSkew3", "Entities": "<- PEnts",
                   "Shapes": "<- Validated", "MaxCount": "1"}, "pass"),
    # Paging: a CAS pages blocks; with GC; CAS only; every fault; skewed clocks.
    "page": ({**FPAGE}, "pass"),
    "page-cas": ({**FPAGE, "MaxCount": "0", "MaxTime": "2"}, "pass"),
    # A CAS pages the ids in (clock - R, min(clock - A, own stamp - 1)] (it pages the view after
    # its own GC), so with A = 0 it pages nothing below R = 2.
    "page-gc": ({**FPAGE, "Retention": "2", "MaxTime": "3"}, "pass"),
    "page-faults": ({**FPAGE, "MaxFaults": "1", "FaultKinds": FPAGE_FAULTS, "Strict": "TRUE"},
                    "pass"),
    "page-skew": ({**FPAGE, "Skew": "<- Behind2"}, "pass"),
    # Tail reads (TAIL_READS.md): after a lost append race the loop tail-reads (TailReads, the
    # default) or probes and resends framed from the probed version's last trailer; with GC a
    # replace can make the object longer than the version a writer read.
    "probe": ({"TailReads": "FALSE"}, "pass"),
    "probe-gc": ({"Retention": "0", "MaxTime": "2", "Shapes": "<- TwoEach", "MaxCount": "1",
                  "TailReads": "FALSE"}, "pass"),
    "probe-faults": ({"MaxFaults": "1", "FaultKinds": ALL_FAULTS, "Strict": "TRUE",
                      "TailReads": "FALSE"}, "pass"),
    "tail-long": ({"Shapes": "<- Long", "MaxCount": "2", "MaxAttempts": "4", "Retention": "0",
                   "MaxTime": "1"}, "pass"),
    # Their switches: a tail read that compares lengths only (FileTail.continues before step 5),
    # a whole read whose frames are not checked (format 2: a splice decodes), a resend after a
    # probe framed with the incarnation the writer last read.
    "lengthonly": ({"Retention": "0", "MaxTime": "2", "Shapes": "<- TwoEach", "MaxCount": "1",
                    "Bugs": '{"lengthonly"}'}, {"NoUnheldState", "Readable"}),
    "notrailer": ({"Retention": "0", "MaxTime": "2", "Shapes": "<- TwoEach", "MaxCount": "1",
                   "Bugs": '{"notrailer"}'}, {"NoUnheldState"}),
    "probeinc": ({"Retention": "0", "MaxTime": "2", "Shapes": "<- TwoEach", "MaxCount": "1",
                  "TailReads": "FALSE", "Bugs": '{"probeinc"}'}, {"Readable"}),
    # Calibration: each bug, put back, must break a property. Bugs 26, 64a and floorgap need a
    # resend after a probe, without a read (TailReads FALSE): with tail reads the loop reads, and
    # judges the record, before it resends.
    "basever": ({"Entities": "<- CEnts", "Shapes": "<- Concerns2", "MaxCount": "1",
                 "Bugs": '{"basever"}'}, {"StampsCount", "HonestFailed", "Serializable"}),
    "bug10": ({"Shapes": "<- Creates", "Bugs": '{"10"}'}, {"UniqueIds", "Serializable"}),
    "bug12": ({"MaxSize": "2", "Bugs": '{"12"}', "Shapes": "<- TwoEach"},
              {"HonestCommitted", "HistGrows", "Serializable"}),
    "bug16": ({"Bugs": '{"16"}'}, {"Serializable"}),
    "bug17": ({"MaxFaults": "1", "FaultKinds": ALL_FAULTS, "Bugs": '{"17"}'}, {"HonestFailed"}),
    "bug18": ({"MaxFaults": "2", "FaultKinds": FUZZER_FAULTS, "Bugs": '{"18"}'},
              {"HonestFailed"}),
    "bug25": ({"MaxFaults": "1", "MaxThrottle": "1", "FaultKinds": ALL_FAULTS, "Bugs": '{"25"}'},
              {"HonestFailed"}),
    "bug26": ({"TailReads": "FALSE", "Strict": "TRUE", "MaxSize": "2", "Shapes": "<- TwoEach",
               "Bugs": '{"26"}'}, {"UnknownExplained"}),
    "bug64a": ({"TailReads": "FALSE", "Retention": "0", "MaxTime": "2", "Shapes": "<- TwoEach",
                "MaxCount": "1", "Bugs": '{"64a"}'}, {"Readable"}),
    "bug64b": ({"Retention": "0", "MaxTime": "2", "Shapes": "<- TwoEach", "MaxCount": "1",
                "Bugs": '{"64b"}'}, {"CasAppliesItsRecord"}),
    "bug64c": ({"Retention": "0", "MaxTime": "2", "Shapes": "<- TwoEach", "MaxCount": "1",
                "Skew": "<- Behind2", "Bugs": '{"64c"}'}, {"SoloCommits", "CasAppliesItsRecord"}),
    "bug64d": ({"Retention": "0", "MaxTime": "2", "Shapes": "<- TwoEach", "MaxCount": "1",
                "MaxFaults": "1", "FaultKinds": FUZZER_FAULTS, "Bugs": '{"64d"}'},
               {"HonestFailed"}),
    "bug66": ({"SegLimit": "2", "MaxCount": "10", "Shapes": "<- TwoEach", "Bugs": '{"66"}'},
              {"UnknownExplained", "SoloCommits"}),
    # Not a bug the fuzzers found: the design before GC records, a CAS whose checkpoint is
    # filtered at its writer's clock rather than a replay of the log.
    "ckgc": ({"Retention": "0", "MaxTime": "2", "Shapes": "<- TwoEach", "MaxCount": "1",
              "Bugs": '{"ckgc"}'}, {"CheckpointIsReplay"}),
    # Paging's switches (COMMITTED_PAGING.md "Testing"), and one of the file's own: a CAS's
    # PageOuts not kept below its own record.
    "floorgap": ({"TailReads": "FALSE", **FPAGE, "MaxFaults": "1", "FaultKinds": FPAGE_FAULTS,
                  "Bugs": '{"floorgap"}'}, {"ExactlyOnce", "HonestFailed", "PagesExact"}),
    # On one object a fused CAS's PageOuts land only on the version they were computed on (the
    # CAS is conditional): the digest check never refuses one, and the switch breaks nothing. A
    # resent compaction's do not (after-nodigest).
    "nodigest": ({**FPAGE, "MaxFaults": "1", "FaultKinds": FPAGE_FAULTS, "Bugs": '{"nodigest"}'},
                 "pass"),
    "absentpage": ({**FPAGE, "MaxFaults": "2", "FaultKinds": FPAGE_FAULTS,
                    "Bugs": '{"absentpage"}'}, {"HonestFailed"}),
    "sweepabove": ({**FPAGE, "Bugs": '{"sweepabove"}'}, {"PagesExist"}),
    "mintH": ({**FPAGE, "Skew": "<- Behind2", "Bugs": '{"mintH"}'},
              {"CasAppliesItsRecord", "SoloCommits"}),
    "pageown": ({**FPAGE, "Bugs": '{"pageown"}'}, {"CasAppliesItsRecord"}),
    # Compaction after the commit.
    "after-base": ({**AFTER}, "pass"),
    "after-cas-only": ({**AFTER, "MaxCount": "0"}, "pass"),
    "after-gcs": ({**AFTER, "AppendSupported": "FALSE"}, "pass"),
    "after-size": ({**AFTER, "MaxCount": "100", "MaxSize": "3", "Shapes": "<- TwoEach"}, "pass"),
    "after-two-each": ({**AFTER, "Shapes": "<- TwoEach"}, "pass"),
    "after-long": ({**AFTER, "Shapes": "<- Long", "MaxCount": "2", "MaxAttempts": "4"}, "pass"),
    "after-faults": ({**AFTER, "MaxFaults": "1", "FaultKinds": ALL_FAULTS}, "pass"),
    "after-faults-strict": ({**AFTER, "MaxFaults": "1", "FaultKinds": ALL_FAULTS,
                             "Strict": "TRUE"}, "pass"),
    "after-faults2": ({**AFTER, "Shapes": "<- TwoEach", "MaxFaults": "2", "FaultKinds": ALL_FAULTS,
                       "Strict": "TRUE"}, "pass"),
    "after-gc0": ({**AFTER, "Retention": "0", "MaxTime": "2", "Shapes": "<- TwoEach"}, "pass"),
    "after-skew": ({**AFTER, "Shapes": "<- TwoEach", "Retention": "1", "MaxTime": "3",
                    "Skew": "<- Behind2", "MaxFaults": "1", "FaultKinds": FUZZER_FAULTS}, "pass"),
    # A segment cap: a full object makes the commit replace it with its record (fused), and
    # compactions after commits reset the cap too.
    "after-seg": ({**AFTER, "Shapes": "<- TwoEach", "MaxCount": "2", "SegLimit": "2"}, "pass"),
    "after-long-faults": ({**AFTER, "Shapes": "<- Long", "MaxCount": "2", "MaxAttempts": "4",
                           "MaxFaults": "1", "FaultKinds": ALL_FAULTS, "Strict": "TRUE",
                           "Retention": "0", "MaxTime": "1"}, "pass"),
    "after-three": ({**AFTER, "Writers": "{1, 2, 3}", "Shapes": "<- Three", "Skew": "<- NoSkew3",
                     "MaxFaults": "1", "FaultKinds": ALL_FAULTS, "Strict": "TRUE"}, "pass"),
    "after-probe": ({**AFTER, "Shapes": "<- TwoEach", "Retention": "0", "MaxTime": "2",
                     "TailReads": "FALSE"}, "pass"),
    "after-probe-faults": ({**AFTER, "MaxFaults": "1", "FaultKinds": ALL_FAULTS, "Strict": "TRUE",
                            "TailReads": "FALSE"}, "pass"),
    "after-sched-mw": ({**AFTER, "Sched": '"mw"', "Shapes": "<- TwoEach", "Strict": "TRUE",
                        "MaxFaults": "1", "FaultKinds": FUZZER_FAULTS}, "pass"),
    "after-sched-fault": ({**AFTER, "Sched": '"fault"', "Shapes": "<- OpsAndTxn",
                           "Strict": "TRUE", "MaxFaults": "1", "FaultKinds": FUZZER_FAULTS}, "pass"),
    "after-sched-race": ({**AFTER, "Sched": '"race"', "Writers": "{1, 2, 3}",
                          "Skew": "<- NoSkew3", "Shapes": "<- RaceTxn", "Strict": "TRUE"}, "pass"),
    "after-sched-race-op": ({**AFTER, "Sched": '"race"', "Writers": "{1, 2, 3}",
                             "Skew": "<- NoSkew3", "Shapes": "<- RaceOp"}, "pass"),
    "after-concerns-faults": ({**AFTER, "Entities": "<- CEnts", "Shapes": "<- Concerns2",
                               "MaxFaults": "1", "FaultKinds": ALL_FAULTS, "Strict": "TRUE"},
                              "pass"),
    "after-blind-appends": ({**AFTER, "Entities": "<- CEnts", "Shapes": "<- BlindAppends",
                             "MaxFaults": "1", "FaultKinds": ALL_FAULTS}, "pass"),
    "after-page": ({**FPAGE, **AFTER}, "pass"),
    "after-page-gc": ({**FPAGE, **AFTER, "Retention": "2", "MaxTime": "3"}, "pass"),
    "after-page-faults": ({**FPAGE, **AFTER, "MaxFaults": "1", "FaultKinds": FPAGE_FAULTS,
                           "Strict": "TRUE"}, "pass"),
    "after-page-skew": ({**FPAGE, **AFTER, "Skew": "<- Behind2"}, "pass"),
    "after-statepages": ({**SPAGE, **AFTER}, "pass"),
    # Resends: twice, on two and three writers, with faults, GC, a skewed clock and paging.
    "after-resend": ({**AFTER, "Shapes": "<- TwoEach", "CompactResends": "2"}, "pass"),
    "after-resend-three": ({**AFTER, "Writers": "{1, 2, 3}", "Shapes": "<- Three",
                            "Skew": "<- NoSkew3", "CompactResends": "2"}, "pass"),
    "after-resend-faults": ({**AFTER, "Shapes": "<- TwoEach", "CompactResends": "2",
                             "MaxFaults": "1", "FaultKinds": ALL_FAULTS, "Strict": "TRUE"}, "pass"),
    "after-resend-gc": ({**AFTER, "Shapes": "<- TwoEach", "CompactResends": "2", "Retention": "0",
                         "MaxTime": "2", "Skew": "<- Behind2"}, "pass"),
    "after-resend-page": ({**FPAGE, **AFTER, "CompactResends": "2", "Skew": "<- Behind2"}, "pass"),
    # Its switches: a compaction that checkpoints the replica the commit staged on, before its
    # record; one that is resent on the object as it is after losing its race (as if the records
    # that beat it were in its checkpoint); one whose loss fails the commit.
    "compactbase": ({**AFTER, "Shapes": "<- TwoEach", "Bugs": '{"compactbase"}'},
                    {"CheckpointIsReplay", "HistGrows", "HonestCommitted"}),
    "compactresend": ({**AFTER, "Shapes": "<- TwoEach", "Bugs": '{"compactresend"}'},
                      {"CheckpointIsReplay", "HistGrows", "HonestCommitted"}),
    "compactfail": ({**AFTER, "Shapes": "<- TwoEach", "Bugs": '{"compactfail"}'},
                    {"HonestFailed"}),
    # A resend whose copied records follow its GC record and PageOuts (judged on a view that
    # dropped or paged ids they were judged without); one that takes a replace as long as the
    # version it lost on for growth.
    "compactcopyafter": ({**AFTER, "Shapes": "<- TwoEach", "Retention": "0", "MaxTime": "2",
                          "Skew": "<- Behind2", "Bugs": '{"compactcopyafter"}'},
                         {"CheckpointIsReplay", "HistGrows", "HonestCommitted"}),
    "compactreplaced": ({**AFTER, "Shapes": "<- TwoEach", "Bugs": '{"compactreplaced"}'},
                        {"CheckpointIsReplay", "HistGrows", "HonestCommitted"}),
    # A resend's PageOuts were computed before the records it copies: one whose block they changed
    # must be refused (the digest check), which on one object only a resend needs.
    "after-nodigest": ({**FPAGE, **AFTER, "CompactResends": "2", "Skew": "<- Behind2",
                        "Bugs": '{"nodigest"}'}, {"PagesExact"}),
    # The state's pages (SNAPSHOT_PAGING.md "Testing"): an entity's history seals into a page
    # at each version and drops one behind (SealEvery 1); writer 1 writes "a" three times.
    "statepages": (SPAGE, "pass"),
    # A root written before its pages are put.
    "rootfirst": ({**SPAGE, "Bugs": '{"rootfirst"}'}, {"StatePagesExist"}),
    # A sweep from a stale view that deletes every page it does not name, whatever version.
    "sweepstale": ({**SPAGE, "Bugs": '{"sweepstale"}'}, {"StatePagesExist"}),
}


# The shared-log model (SharedLogCommit, through MCShared).
LOG_BASE = {
    "Writers": "{1, 2}",
    "Entities": "<- Ents",
    "Shapes": "<- Contend",
    "MaxAttempts": "3",
    "MaxThrottle": "2",
    "Retention": "<- GcOff",
    "MaxTime": "0",
    "Skew": "<- NoSkew2",
    "CompSkews": "{0}",
    "MaxGc": "0",
    "MaxCkpt": "0",
    "MaxTrim": "0",
    "MaxFaults": "0",
    "FaultKinds": "{}",
    "Lag": "FALSE",
    "Admission": "0",
    "MaxPage": "0",
    "MaxSweep": "0",
    "BlockIds": "1",
    "GridBits": "3",
    "SealEvery": "0",
    "Bugs": "{}",
}

LOG_SAFETY = ["StampsCount", "Readable", "Deterministic", "CheckpointIsReplay", "ExactlyOnce",
              "Serializable",
              "UniqueIds", "HonestCommitted", "HonestFailed", "UnknownExplained", "SoloCommits",
              "PagesExist", "PagesExact", "Shape", "OnGrid"]
LOG_ACTIONS = ["HistGrows"]

LOG_FAULTS = ('{"failBefore", "notSent", "lostReply", "throttled", "landedThrottled", '
              '"inflight", "readFail"}')
# Compaction: a GC record, checkpoints and a trim, with retention 0; with faults, one checkpoint.
COMPACT = {"Retention": "0", "MaxTime": "1", "MaxGc": "1", "MaxCkpt": "2", "MaxTrim": "1"}
COMPACT1 = {**COMPACT, "MaxCkpt": "1"}
# Paging (COMMITTED_PAGING.md): a tick, so that ids land in two blocks; a page put (then its
# PageOut sent, or never: a crash) and a sweep; A = 0, N = 1, stamps on a grid of 8.
PAGE = {"Shapes": "<- TwoEach", "MaxTime": "1", "MaxPage": "1", "MaxSweep": "1"}
# One transaction each still lands two ids in two blocks (a tick between them): for the larger
# configurations.
PAGE1 = {"MaxTime": "1", "MaxPage": "1", "MaxSweep": "1"}
PAGE_FAULTS = ('{"failBefore", "notSent", "lostReply", "throttled", "landedThrottled", '
               '"inflight", "readFail", "pageFail", "putFail", "putLost"}')

LOG_CONFIGS = {
    "log-base": ({}, "pass"),
    "log-creates": ({"Shapes": "<- Creates"}, "pass"),
    "log-blind": ({"Shapes": "<- Blind"}, "pass"),
    "log-two-each": ({"Shapes": "<- TwoEach"}, "pass"),
    "log-faults": ({"MaxFaults": "1", "FaultKinds": LOG_FAULTS}, "pass"),
    "log-faults2": ({"MaxFaults": "2", "FaultKinds": LOG_FAULTS, "Shapes": "<- Blind",
                     "MaxAttempts": "2"}, "pass"),
    "log-ckpt": ({"MaxCkpt": "2", "MaxTrim": "2"}, "pass"),
    "log-gc": ({**COMPACT}, "pass"),
    # A compactor's read served with a gap after its checkpoint (S4): the runtime refuses it
    # (TAIL_READS.md step 3); with "nojoint" it checkpoints a state the log never held.
    "log-nojoint": ({**COMPACT, "Shapes": "<- TwoEach", "Bugs": '{"nojoint"}'},
                    {"CheckpointIsReplay", "Deterministic"}),
    "log-lag": ({**COMPACT, "Lag": "TRUE"}, "pass"),
    "log-skew": ({**COMPACT, "Retention": "1", "MaxTime": "2", "Skew": "<- Behind2",
                  "CompSkews": "{0, 2}"}, "pass"),
    "log-gc-faults": ({**COMPACT1, "MaxFaults": "1", "FaultKinds": LOG_FAULTS}, "pass"),
    "log-blind-faults": ({**COMPACT1, "Shapes": "<- Blind", "MaxFaults": "1",
                          "FaultKinds": LOG_FAULTS}, "pass"),
    # Everything at once, at small bounds.
    "log-mix": ({**COMPACT1, "MaxFaults": "1", "FaultKinds": LOG_FAULTS, "Lag": "TRUE"}, "pass"),
    "log-three": ({**COMPACT1, "Writers": "{1, 2, 3}", "Shapes": "<- Three",
                   "Skew": "<- NoSkew3"}, "pass"),
    "log-concerns": ({"Writers": "{1, 2, 3}", "Skew": "<- NoSkew3", "Entities": "<- CEnts",
                      "Shapes": "<- Concerns"}, "pass"),
    "log-concerns-faults": ({"Entities": "<- CEnts", "Shapes": "<- Concerns2",
                             "MaxFaults": "1", "FaultKinds": LOG_FAULTS}, "pass"),
    "log-blind-appends": ({**COMPACT1, "Entities": "<- CEnts", "Shapes": "<- BlindAppends",
                           "MaxFaults": "1", "FaultKinds": LOG_FAULTS}, "pass"),
    "log-validated": ({"Writers": "{1, 2, 3}", "Skew": "<- NoSkew3", "Entities": "<- PEnts",
                       "Shapes": "<- Validated"}, "pass"),
    # Paging.
    "log-page": ({**PAGE}, "pass"),
    "log-page2": ({**PAGE, "MaxPage": "2"}, "pass"),
    "log-page-n2": ({**PAGE, "BlockIds": "2"}, "pass"),
    "log-page-gc": ({**COMPACT1, **PAGE1}, "pass"),
    "log-page-faults": ({**PAGE1, "MaxCkpt": "1", "MaxFaults": "1", "FaultKinds": PAGE_FAULTS},
                        "pass"),
    "log-page-lag": ({**COMPACT1, **PAGE1, "Lag": "TRUE", "MaxPage": "2"}, "pass"),
    "log-page-skew": ({**COMPACT1, **PAGE1, "Retention": "1", "Skew": "<- Behind2",
                       "CompSkews": "{0, 2}"}, "pass"),
    # Calibration: each bug, put back, must break a property.
    "log-basever": ({"Entities": "<- CEnts", "Shapes": "<- Concerns2", "Bugs": '{"basever"}'},
                    {"StampsCount", "HonestFailed", "Serializable"}),
    "log-ckclock": ({**COMPACT, "MaxGc": "0", "MaxTrim": "0", "Bugs": '{"ckclock"}'},
                    {"Deterministic"}),
    "log-setonly": ({**COMPACT, "MaxCkpt": "0", "MaxTrim": "0", "Bugs": '{"setonly"}'},
                    {"HonestFailed"}),
    "log-trimearly": ({"MaxTrim": "1", "Bugs": '{"trimearly"}'}, {"Readable"}),
    "log-noverify": ({"MaxFaults": "1", "FaultKinds": LOG_FAULTS, "Bugs": '{"noverify"}'},
                     {"HonestFailed"}),
    "log-freshid": ({"Shapes": "<- Blind", "MaxFaults": "1", "FaultKinds": LOG_FAULTS,
                     "Bugs": '{"freshid"}'}, {"ExactlyOnce"}),
    "log-p3": ({**COMPACT, "MaxTrim": "0", "Bugs": '{"p3"}'}, {"HonestFailed"}),
    "log-dup": ({**COMPACT, "MaxTrim": "0", "MaxFaults": "1", "FaultKinds": LOG_FAULTS,
                 "Bugs": '{"dup"}'}, {"HonestFailed"}),
    "log-lagread": ({"Lag": "TRUE", "MaxFaults": "1", "FaultKinds": LOG_FAULTS,
                     "Bugs": '{"lagread"}'}, {"HonestFailed"}),
    # Paging's switches (COMMITTED_PAGING.md "Testing").
    "log-floorgap": ({**PAGE, "MaxFaults": "1", "FaultKinds": LOG_FAULTS,
                      "Bugs": '{"floorgap"}'}, {"ExactlyOnce", "PagesExact"}),
    "log-nodigest": ({**PAGE, "Bugs": '{"nodigest"}'}, {"PagesExact"}),
    "log-absentpage": ({**PAGE, "MaxCkpt": "1", "MaxFaults": "2", "FaultKinds": PAGE_FAULTS,
                        "Bugs": '{"absentpage"}'}, {"HonestFailed"}),
    "log-sweepabove": ({**PAGE, "Bugs": '{"sweepabove"}'}, {"PagesExist"}),
    "log-mintH": ({**PAGE, "Skew": "<- Behind2", "CompSkews": "{2}", "Bugs": '{"mintH"}'},
                  {"SoloCommits"}),
    # Not in the plan: a lookup whose page was deleted since its read (GC dropped it) fails
    # instead of reading again.
    "log-missingfail": ({**COMPACT1, **PAGE1, "Lag": "TRUE", "MaxFaults": "1",
                         "FaultKinds": LOG_FAULTS, "Bugs": '{"missingfail"}'},
                        {"UnknownExplained"}),
}

# The Azure log (AzureLog, through MCAzure; docs/AZURE_LOG.md), configurations az-*: two writers
# with one record each, a segment of one record (W = 1, L = 2), one rotation.
AZ_BASE = {
    "Writers": "{1, 2}",
    "Recs": "<- OneEach",
    "WLimit": "1",
    "MaxBlocks": "5",
    "MaxOrd": "1",
    "MaxSends": "2",
    "MaxFaults": "0",
    "FaultKinds": "{}",
    "MaxCkpt": "0",
    "MaxTrim": "0",
    "MaxSweep": "0",
    "Readers": "{1}",
    "MaxReads": "1",
    "Bugs": "{}",
    "Pad": "Pad",
}
AZ_SAFETY = ["ClosedSegments", "NothingPastClose", "ReadsAreSlices", "AckedAtReceipt",
             "NamedExist", "Readable", "CheckpointsStable", "CopiesKnown"]
AZ_ACTIONS = ["AbstractGrows"]
AZ_FAULTS = '{"lostReply", "notSent", "inflight", "shortRead"}'
# A checkpoint, a trim and two sweeps, over two rotations.
AZ_COMPACT = {"MaxOrd": "2", "MaxCkpt": "1", "MaxTrim": "1", "MaxSweep": "2"}

AZ_CONFIGS = {
    "az-base": ({}, "pass"),
    "az-two-each": ({"Recs": "<- TwoEach", "MaxOrd": "2"}, "pass"),
    "az-w2": ({"Recs": "<- TwoEach", "WLimit": "2", "MaxOrd": "1"}, "pass"),
    "az-blocks": ({"Recs": "<- TwoEach", "WLimit": "3", "MaxBlocks": "2"}, "pass"),
    "az-three": ({"Writers": "{1, 2, 3}", "Recs": "<- Three"}, "pass"),
    "az-compact": ({**AZ_COMPACT, "Recs": "<- TwoOne", "MaxReads": "2"}, "pass"),
    "az-faults": ({"MaxFaults": "1", "FaultKinds": AZ_FAULTS, "MaxReads": "2"}, "pass"),
    "az-faults-compact": ({"MaxCkpt": "1", "MaxTrim": "1", "MaxSweep": "1", "MaxFaults": "1",
                           "FaultKinds": AZ_FAULTS}, "pass"),
    # The mistakes (AZURE_LOG.md "Mistakes the model must catch"): each must break a property.
    "az-casbeforeclose": ({"Bugs": '{"casbeforeclose"}'},
                          {"NothingPastClose", "AckedAtReceipt", "ClosedSegments",
                           "ReadsAreSlices", "AbstractGrows"}),
    "az-nolimit": ({"Recs": "<- TwoEach", "MaxReads": "0", "Bugs": '{"nolimit"}'},
                   {"NothingPastClose", "AckedAtReceipt", "ClosedSegments"}),
    "az-createonmissing": ({"Recs": "<- TwoOne", "MaxCkpt": "1", "MaxTrim": "1",
                            "MaxSweep": "1", "MaxReads": "0", "Bugs": '{"createonmissing"}'},
                           {"NothingPastClose", "AckedAtReceipt", "AbstractGrows",
                            "CheckpointsStable"}),
    "az-noclosedlength": ({"MaxFaults": "1", "FaultKinds": '{"shortRead"}',
                           "Bugs": '{"noclosedlength"}'}, {"ReadsAreSlices"}),
    "az-sweepnamed": ({"MaxSweep": "1", "Bugs": '{"sweepnamed"}'}, {"NamedExist"}),
    "az-trimearly": ({"MaxCkpt": "1", "MaxTrim": "1", "Bugs": '{"trimearly"}'}, {"Readable"}),
    "az-sdkretry": ({"WLimit": "2", "MaxFaults": "1", "Bugs": '{"sdkretry"}'}, {"CopiesKnown"}),
    "az-offsetfromlength": ({"WLimit": "2", "MaxReads": "0", "Bugs": '{"offsetfromlength"}'},
                            {"AckedAtReceipt"}),
}

MODELS = {"MC": (BASE, SAFETY, ACTIONS), "MCShared": (LOG_BASE, LOG_SAFETY, LOG_ACTIONS),
          "MCAzure": (AZ_BASE, AZ_SAFETY, AZ_ACTIONS)}


def model_of(name):
    if name.startswith("az-"):
        return "MCAzure"
    return "MCShared" if name.startswith("log-") else "MC"


# Witnesses: (config, Not_ invariant); TLC must report it violated.
WITNESSES = [
    ("long", "Not_Shrink"), ("mix", "Not_CasAfterFreshRead"), ("cas-only", "Not_Rebase"),
    ("two-each", "Not_StaleAppend"), ("faults", "Not_Throttled"), ("seg", "Not_Full"),
    ("faults2", "Not_FinalRead"), ("faults", "Not_LateLanding"),
    ("faults", "Not_DuplicateRecord"), ("base", "Not_FailedButLanded"),
    ("gc0", "Not_Horizon"), ("gc0", "Not_GcRecord"), ("gc0", "Not_UnknownWithoutFault"),
    ("faults", "Not_Unknown"),
    ("faults", "Not_CommittedAfterFault"), ("two-each", "Not_Overshoot"),
    ("faults2", "Not_Exhausted"),
    ("sched-mw", "Not_Preempted"), ("sched-fault", "Not_Preempted"),
    ("sched-race", "Not_TwoRacers"), ("sched-race", "Not_RaceFailedA"),
    ("sched-race-op", "Not_Restaged"), ("sched-fault", "Not_Restaged"),
    ("faults", "Not_LateLandingAfterEnd"), ("skew", "Not_ClockBehindHorizon"),
    ("mix", "Not_ResolveAfterGc"),
    ("log-ckpt", "Not_LaterCheckpoint"), ("log-ckpt", "Not_TwoUsable"),
    ("log-ckpt", "Not_Trimmed"), ("log-ckpt", "Not_ReadBackTrimmed"),
    ("log-gc", "Not_GcBeforeReadBack"), ("log-gc", "Not_RecordBelowHorizon"),
    ("log-gc", "Not_Horizon"), ("log-gc", "Not_UnknownWithoutFault"),
    ("log-faults", "Not_DuplicateRecord"), ("log-faults", "Not_LateLandingAfterEnd"),
    ("log-base", "Not_FailedButLanded"), ("log-faults2", "Not_Unknown"),
    ("log-faults", "Not_Error"), ("log-faults", "Not_Throttled"),
    ("log-faults2", "Not_Exhausted"), ("log-lag", "Not_LaggingRead"),
    # Paging.
    ("page", "Not_Floor"), ("page", "Not_PageOutInLog"), ("page-faults", "Not_PagedOwnId"),
    ("page-faults", "Not_Orphan"), ("page-gc", "Not_Swept"), ("page-cas", "Not_TwoPages"),
    ("page-gc", "Not_PageBelowHorizon"), ("page-skew", "Not_Floor"),
    ("log-page", "Not_Floor"), ("log-page", "Not_PageOutRefused"), ("log-page", "Not_Orphan"),
    ("log-page-gc", "Not_Swept"), ("log-page2", "Not_Swept"), ("log-page", "Not_PagedOwnId"),
    ("log-page", "Not_FailedBelowFloor"),
    ("log-page", "Not_TwoBlocks"), ("log-page-n2", "Not_WideBlock"),
    ("log-page-gc", "Not_PageDropped"), ("log-page-lag", "Not_DuplicatePageOut"),
    ("log-page-faults", "Not_CopyAfterPageOut"),
    # Compaction after the commit.
    ("after-two-each", "Not_CompactionLanded"), ("after-two-each", "Not_CompactionLost"),
    ("after-long", "Not_LostToCompaction"), ("after-seg", "Not_Full"),
    ("after-sched-mw", "Not_CompactionLanded"), ("after-sched-race", "Not_CompactionLanded"),
    ("after-resend", "Not_CompactionResent"), ("after-resend-gc", "Not_CompactionResent"),
    ("after-resend-page", "Not_CompactionResent"), ("after-resend-page", "Not_ResentPageOutRefused"),
    ("after-page", "Not_PageOutInLog"), ("after-faults", "Not_LateLanding"),
    # The Azure log.
    ("az-base", "Not_Rotated"), ("az-two-each", "Not_TwoRotations"), ("az-base", "Not_Pad"),
    ("az-two-each", "Not_CasLost"), ("az-blocks", "Not_BlockClosed"), ("az-base", "Not_ReadNext"),
    ("az-compact", "Not_CheckpointInHead"), ("az-compact", "Not_Trimmed"),
    ("az-compact", "Not_Swept"), ("az-compact", "Not_Writer404"),
    ("az-compact", "Not_Reader404"), ("az-faults", "Not_LateLanding"), ("az-faults", "Not_Copy"),
]


# The oracle matrix: for each fuzzer's oracle (on its schedules) and each bug put back, whether
# the bug breaks a property on those schedules (safety), whether the oracle reports it (oracle),
# and whether some schedule passes the oracle with a property broken (adequate).
ORACLE_SHAPES = {
    "mw": {"Shapes": "<- TwoEach", "Writers": "{1, 2}", "Skew": "<- NoSkew2"},
    "fault": {"Shapes": "<- OpsAndTxn", "Writers": "{1, 2}", "Skew": "<- NoSkew2"},
    "race": {"Shapes": "<- RaceTxn", "Writers": "{1, 2, 3}", "Skew": "<- NoSkew3"},
    "race1": {"Shapes": "<- RaceTxn1", "Writers": "{1, 2}", "Skew": "<- NoSkew2"},
}
ORACLE_DEF = {"mw": "<- OracleMW", "fault": "<- OracleFault", "race": None, "race1": None}
GC = {"Retention": "0", "MaxTime": "2", "MaxCount": "1"}
F1 = {"MaxFaults": "1", "FaultKinds": FUZZER_FAULTS}
BUG_KNOBS = {
    "none": {"MaxCount": "1", "Strict": "TRUE", **F1},
    "none-gc": {**GC, **F1},
    "none-seg": {"SegLimit": "2", "MaxCount": "10", **F1},
    "10": {"creates": True},
    "12": {"MaxSize": "2"},
    "16": {},
    "17": {**F1},
    "18": {"MaxFaults": "2", "FaultKinds": FUZZER_FAULTS},
    "25": {**F1, "MaxThrottle": "1"},
    "26": {"Strict": "TRUE", "MaxSize": "2"},
    "64a": {**GC},
    "64b": {**GC},
    "64c": {**GC, "skew": True},
    "64d": {**GC, **F1},
    "66": {"SegLimit": "2", "MaxCount": "10"},
    # Paging's switches (a CAS pages what its ceiling allows; one sweep).
    "none-page": {"Paging": "TRUE", "MaxTime": "1", "MaxSweep": "1", "MaxCount": "1",
                  "MaxFaults": "1", "FaultKinds": FUZZER_PAGE_FAULTS},
    "floorgap": {"Paging": "TRUE", "MaxTime": "1", "MaxSweep": "1", "MaxCount": "1",
                 "MaxFaults": "1", "FaultKinds": FUZZER_PAGE_FAULTS},
    "absentpage": {"Paging": "TRUE", "MaxTime": "1", "MaxSweep": "1", "MaxCount": "1",
                   "MaxFaults": "2", "FaultKinds": FUZZER_PAGE_FAULTS},
    "sweepabove": {"Paging": "TRUE", "MaxTime": "1", "MaxSweep": "1", "MaxCount": "1"},
    "mintH": {"Paging": "TRUE", "MaxTime": "1", "MaxSweep": "1", "MaxCount": "1", "skew": True},
    "pageown": {"Paging": "TRUE", "MaxTime": "1", "MaxSweep": "1", "MaxCount": "1"},
    # Compaction after the commit, and its switches.
    "none-after": {**AFTER, "Strict": "TRUE", **F1},
    "compactbase": {**AFTER},
    "compactresend": {**AFTER},
    "compactfail": {**AFTER},
    "compactcopyafter": {**AFTER, **GC, "skew": True},
    "compactreplaced": {**AFTER},
}
CREATES = {"mw": "<- Creates", "fault": "<- Creates", "race": "<- RaceCreates",
           "race1": "<- RaceCreates1"}

# The shared-log oracles (SharedLogOracles), on free schedules: "now" judges each commit as it
# ends; "drain" also judges the whole run once what was in flight has landed.
LOG_ORACLES = ["now", "drain"]
LOG_F1 = {"MaxFaults": "1", "FaultKinds": LOG_FAULTS}
LOG_BUG_KNOBS = {
    "none": {**COMPACT1, **LOG_F1},
    "none-blind": {**COMPACT1, **LOG_F1, "Shapes": "<- Blind"},
    "none-lag": {**LOG_F1, "Lag": "TRUE"},
    "ckclock": LOG_CONFIGS["log-ckclock"][0],
    "setonly": LOG_CONFIGS["log-setonly"][0],
    "trimearly": LOG_CONFIGS["log-trimearly"][0],
    "noverify": LOG_CONFIGS["log-noverify"][0],
    "freshid": LOG_CONFIGS["log-freshid"][0],
    "p3": LOG_CONFIGS["log-p3"][0],
    "dup": LOG_CONFIGS["log-dup"][0],
    "lagread": LOG_CONFIGS["log-lagread"][0],
    # Paging: one transaction each where the switch is reachable so (the oracle runs explore
    # the whole space when they pass). floorgap needs a copy that verifies again (blind
    # appends); nodigest a late id and mintH a writer staging after the floor rose, so two each.
    "none-page": {**PAGE1, "MaxCkpt": "1", "MaxFaults": "1", "FaultKinds": PAGE_FAULTS},
    "floorgap": {**PAGE1, "Entities": "<- CEnts", "Shapes": "<- BlindAppends", "MaxFaults": "1",
                 "FaultKinds": LOG_FAULTS, "Bugs": '{"floorgap"}'},
    "nodigest": LOG_CONFIGS["log-nodigest"][0],
    "absentpage": {**PAGE1, "MaxCkpt": "1", "MaxFaults": "2", "FaultKinds": PAGE_FAULTS,
                   "Bugs": '{"absentpage"}'},
    "sweepabove": {**PAGE1, "Bugs": '{"sweepabove"}'},
    "mintH": LOG_CONFIGS["log-mintH"][0],
}
SKEWED = {"mw": "<- Behind2", "fault": "<- Behind2", "race": "<- Behind3", "race1": "<- Behind2"}


def matrix_overrides(oracle, bug):
    knobs = dict(BUG_KNOBS[bug])
    o = dict(ORACLE_SHAPES[oracle])
    if knobs.pop("creates", False):
        o["Shapes"] = CREATES[oracle]
    if knobs.pop("skew", False):
        o["Skew"] = SKEWED[oracle]
    o.update(knobs)
    o["Sched"] = '"race"' if oracle.startswith("race") else f'"{oracle}"'
    if oracle.startswith("race"):
        # The race property injects no faults, reads leniently and draws no segment cap.
        o.update({"MaxFaults": "0", "FaultKinds": "{}", "Strict": "FALSE", "SegLimit": "0"})
    if not bug.startswith("none"):
        o["Bugs"] = '{"%s"}' % bug
    if ORACLE_DEF[oracle]:
        o["OracleAtEnd"] = ORACLE_DEF[oracle]
    return o


def cfg_text(overrides, invariants=None, properties=None, model="MC"):
    base, safety, actions = MODELS[model]
    consts = dict(base)
    consts.update(overrides)
    lines = ["SPECIFICATION Spec", "CONSTANTS", "  None = None"]
    for k, v in consts.items():
        lines.append(f"  {k} {v}" if v.startswith("<-") else f"  {k} = {v}")
    lines.append("INVARIANTS")
    lines += [f"  {p}" for p in (safety if invariants is None else invariants)]
    props = actions if properties is None else properties
    if props:
        lines.append("PROPERTIES")
        lines += [f"  {p}" for p in props]
    return "\n".join(lines) + "\n"


def run(name, overrides, expect, keep, invariants=None, properties=None):
    model = model_of(name)
    os.makedirs(os.path.join(HERE, "states", "cfg"), exist_ok=True)
    os.makedirs(os.path.join(HERE, "states", "out"), exist_ok=True)
    cfg = os.path.join("states", "cfg", name + ".cfg")
    with open(os.path.join(HERE, cfg), "w") as f:
        f.write(cfg_text(overrides, invariants, properties, model))
    start = time.time()
    proc = subprocess.run(
        ["java", "-XX:+UseParallelGC", "-Xmx" + os.environ.get("TLC_HEAP", "2g"),
         "-cp", "tools/tla2tools.jar", "tlc2.TLC",
         "-workers", os.environ.get("TLC_WORKERS", "2"), "-noGenerateSpecTE",
         "-metadir", os.path.join("states", "meta", name),
         "-config", cfg, model],
        cwd=HERE, capture_output=True, text=True)
    out = proc.stdout + proc.stderr
    with open(os.path.join(HERE, "states", "out", name + ".txt"), "w") as f:
        f.write(out)
    m = re.search(r"(?:Invariant|Temporal property|Action property) (\w+) is violated", out)
    if m is None:
        m = re.search(r"Error: Action property (\w+)", out)
    states = re.findall(r"([\d,]+) distinct states found", out)
    if m:
        verdict = m.group(1)
    elif "No error has been found" in out:
        verdict = "pass"
    else:
        err = re.search(r"Error: (.*)", out)
        verdict = "ERROR: " + (err.group(1) if err else "see output")
    ok = verdict == expect if isinstance(expect, str) else verdict in expect
    shown = expect if isinstance(expect, str) else "/".join(sorted(expect))
    print(f"{'ok  ' if ok else 'FAIL'} {name:16} {verdict:22} (expect {shown}) "
          f"{states[-1] if states else '?':>12} states {time.time() - start:6.1f}s",
          flush=True)
    return ok


def run_quiet(name, overrides, invariants, properties, model="MC"):
    """Runs one configuration; returns its verdict. An unreadable catalog stops every writer, so
    a run that checks no Readable may deadlock: deadlock is not checked."""
    os.makedirs(os.path.join(HERE, "states", "cfg"), exist_ok=True)
    os.makedirs(os.path.join(HERE, "states", "out"), exist_ok=True)
    cfg = os.path.join("states", "cfg", name + ".cfg")
    with open(os.path.join(HERE, cfg), "w") as f:
        f.write(cfg_text(overrides, invariants, properties, model))
    proc = subprocess.run(
        ["java", "-XX:+UseParallelGC", "-Xmx" + os.environ.get("TLC_HEAP", "2g"),
         "-cp", "tools/tla2tools.jar", "tlc2.TLC",
         "-workers", os.environ.get("TLC_WORKERS", "2"), "-noGenerateSpecTE",
         "-deadlock", "-metadir", os.path.join("states", "meta", name), "-config", cfg, model],
        cwd=HERE, capture_output=True, text=True)
    out = proc.stdout + proc.stderr
    with open(os.path.join(HERE, "states", "out", name + ".txt"), "w") as f:
        f.write(out)
    m = re.search(r"(?:Invariant|Temporal property|Action property) (\w+) is violated", out)
    if m:
        return m.group(1)
    if "No error has been found" in out:
        return "pass"
    err = re.search(r"Error: (.*)", out)
    return "ERROR: " + (err.group(1) if err else "see output")


ALL = {**CONFIGS, **LOG_CONFIGS, **AZ_CONFIGS}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-k", default="")
    ap.add_argument("-n", default="", help="exact names, comma-separated (configurations, "
                    "witnesses as config.Not_X, matrix cells as oracle.bug)")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--witnesses", action="store_true", help="check the witnesses instead")
    ap.add_argument("--matrix", action="store_true", help="run the oracle matrix instead")
    ap.add_argument("--tight", action="store_true",
                    help="with --matrix: check the tightened oracles (OracleHolds2) only")
    args = ap.parse_args()
    exact = set(filter(None, args.n.split(",")))

    def wanted(name):
        return name in exact if exact else args.k in name
    if args.matrix:
        for oracle in ORACLE_SHAPES:
            for bug in BUG_KNOBS:
                name = f"{oracle}.{bug}"
                if not wanted(name):
                    continue
                o = matrix_overrides(oracle, bug)
                if args.tight:
                    if "OracleAtEnd" in o:
                        o["OracleAtEnd"] = o["OracleAtEnd"] + "2"
                    print(f"{name:16} tightened oracle "
                          f"{run_quiet(name + '.oracle2', o, ['OracleHolds2'], [])}", flush=True)
                    continue
                verdicts = []
                for what, invs in (("safety", SAFETY), ("oracle", ["OracleHolds"]),
                                   ("adequate", ["Adequate"])):
                    verdicts.append(run_quiet(f"{name}.{what}", o, invs, []))
                print(f"{name:16} safety {verdicts[0]:18} oracle {verdicts[1]:12} "
                      f"adequate {verdicts[2]}", flush=True)
        if args.tight:
            return 0
        for oracle in LOG_ORACLES:
            for bug in LOG_BUG_KNOBS:
                name = f"log-{oracle}.{bug}"
                if not wanted(name):
                    continue
                o = dict(LOG_BUG_KNOBS[bug])
                o["OracleAtEnd"] = "<- OracleNow"
                suffix = oracle.capitalize()
                verdicts = []
                for what, invs in (("safety", LOG_SAFETY), ("oracle", ["OracleHolds" + suffix]),
                                   ("adequate", ["Adequate" + suffix])):
                    verdicts.append(run_quiet(f"{name}.{what}", o, invs, [], "MCShared"))
                print(f"{name:20} safety {verdicts[0]:18} oracle {verdicts[1]:20} "
                      f"adequate {verdicts[2]}", flush=True)
        return 0
    if args.witnesses:
        failed = []
        for cfg, inv in WITNESSES:
            name = f"{cfg}.{inv}"
            if wanted(name) and not run(name, ALL[cfg][0], inv, args.keep, [inv], []):
                failed.append(name)
        if failed:
            print("unreached:", ", ".join(failed))
        return 1 if failed else 0
    names = [n for n in ALL if wanted(n)]
    if args.list:
        for n in names:
            print(n, ALL[n])
        return 0
    failed = [n for n in names if not run(n, *ALL[n], args.keep)]
    if failed:
        print("unexpected verdicts:", ", ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
