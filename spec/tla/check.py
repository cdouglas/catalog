#!/usr/bin/env python3
"""Runs TLC on the CatalogCommit model configurations and checks each verdict.

Usage: spec/tla/check.py [-k SUBSTRING] [--list] [--keep]

Each configuration is a base (see BASE) with overrides. `expect` is "pass", or
the name of the property TLC must report violated. The configurations are
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

    "Bugs": "{}",
}

SAFETY = ["Readable", "ExactlyOnce", "Serializable", "UniqueIds", "HonestCommitted",
          "HonestFailed", "UnknownExplained", "SoloCommits"]
ACTIONS = ["HistGrows", "CasAppliesItsRecord", "CheckpointIsReplay"]

ALL_FAULTS = ('{"failBefore", "lostReply", "landedConflict", "throttled", '
              '"landedThrottled", "inflight", "readFail"}')
FUZZER_FAULTS = ('{"failBefore", "lostReply", "landedConflict", "throttled", '
                 '"landedThrottled", "readFail"}')

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
    # Calibration: each bug, put back, must break a property.
    "bug10": ({"Shapes": "<- Creates", "Bugs": '{"10"}'}, {"UniqueIds", "Serializable"}),
    "bug12": ({"MaxSize": "2", "Bugs": '{"12"}', "Shapes": "<- TwoEach"},
              {"HonestCommitted", "HistGrows", "Serializable"}),
    "bug16": ({"Bugs": '{"16"}'}, {"Serializable"}),
    "bug17": ({"MaxFaults": "1", "FaultKinds": ALL_FAULTS, "Bugs": '{"17"}'}, {"HonestFailed"}),
    "bug18": ({"MaxFaults": "2", "FaultKinds": FUZZER_FAULTS, "Bugs": '{"18"}'},
              {"HonestFailed"}),
    "bug25": ({"MaxFaults": "1", "MaxThrottle": "1", "FaultKinds": ALL_FAULTS, "Bugs": '{"25"}'},
              {"HonestFailed"}),
    "bug26": ({"Strict": "TRUE", "MaxSize": "2", "Shapes": "<- TwoEach", "Bugs": '{"26"}'},
              {"UnknownExplained"}),
    "bug64a": ({"Retention": "0", "MaxTime": "2", "Shapes": "<- TwoEach", "MaxCount": "1",
                "Bugs": '{"64a"}'}, {"Readable"}),
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
}


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
}
CREATES = {"mw": "<- Creates", "fault": "<- Creates", "race": "<- RaceCreates",
           "race1": "<- RaceCreates1"}
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


def cfg_text(overrides, invariants=None, properties=None):
    consts = dict(BASE)
    consts.update(overrides)
    lines = ["SPECIFICATION Spec", "CONSTANTS", "  None = None"]
    for k, v in consts.items():
        lines.append(f"  {k} {v}" if v.startswith("<-") else f"  {k} = {v}")
    lines.append("INVARIANTS")
    lines += [f"  {p}" for p in (SAFETY if invariants is None else invariants)]
    props = ACTIONS if properties is None else properties
    if props:
        lines.append("PROPERTIES")
        lines += [f"  {p}" for p in props]
    return "\n".join(lines) + "\n"


def run(name, overrides, expect, keep, invariants=None, properties=None):
    os.makedirs(os.path.join(HERE, "states", "cfg"), exist_ok=True)
    os.makedirs(os.path.join(HERE, "states", "out"), exist_ok=True)
    cfg = os.path.join("states", "cfg", name + ".cfg")
    with open(os.path.join(HERE, cfg), "w") as f:
        f.write(cfg_text(overrides, invariants, properties))
    start = time.time()
    proc = subprocess.run(
        ["java", "-XX:+UseParallelGC", "-Xmx" + os.environ.get("TLC_HEAP", "2g"),
         "-cp", "tools/tla2tools.jar", "tlc2.TLC",
         "-workers", os.environ.get("TLC_WORKERS", "2"), "-noGenerateSpecTE",
         "-metadir", os.path.join("states", "meta", name),
         "-config", cfg, "MC"],
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
    print(f"{'ok  ' if ok else 'FAIL'} {name:12} {verdict:22} (expect {shown}) "
          f"{states[-1] if states else '?':>12} states {time.time() - start:6.1f}s",
          flush=True)
    return ok


def run_quiet(name, overrides, invariants, properties):
    """Runs one configuration; returns its verdict. An unreadable catalog stops every writer, so
    a run that checks no Readable may deadlock: deadlock is not checked."""
    os.makedirs(os.path.join(HERE, "states", "cfg"), exist_ok=True)
    os.makedirs(os.path.join(HERE, "states", "out"), exist_ok=True)
    cfg = os.path.join("states", "cfg", name + ".cfg")
    with open(os.path.join(HERE, cfg), "w") as f:
        f.write(cfg_text(overrides, invariants, properties))
    proc = subprocess.run(
        ["java", "-XX:+UseParallelGC", "-Xmx" + os.environ.get("TLC_HEAP", "2g"),
         "-cp", "tools/tla2tools.jar", "tlc2.TLC",
         "-workers", os.environ.get("TLC_WORKERS", "2"), "-noGenerateSpecTE",
         "-deadlock", "-metadir", os.path.join("states", "meta", name), "-config", cfg, "MC"],
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-k", default="")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--witnesses", action="store_true", help="check the witnesses instead")
    ap.add_argument("--matrix", action="store_true", help="run the oracle matrix instead")
    ap.add_argument("--tight", action="store_true",
                    help="with --matrix: check the tightened oracles (OracleHolds2) only")
    args = ap.parse_args()
    if args.matrix:
        for oracle in ORACLE_SHAPES:
            for bug in BUG_KNOBS:
                name = f"{oracle}.{bug}"
                if args.k not in name:
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
        return 0
    if args.witnesses:
        failed = []
        for cfg, inv in WITNESSES:
            name = f"{cfg}.{inv}"
            if args.k in name and not run(name, CONFIGS[cfg][0], inv, args.keep, [inv], []):
                failed.append(name)
        if failed:
            print("unreached:", ", ".join(failed))
        return 1 if failed else 0
    names = [n for n in CONFIGS if args.k in n]
    if args.list:
        for n in names:
            print(n, CONFIGS[n])
        return 0
    failed = [n for n in names if not run(n, *CONFIGS[n], args.keep)]
    if failed:
        print("unexpected verdicts:", ", ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
