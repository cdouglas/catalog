#!/usr/bin/env bash
# Usage: spec/tla/run-tlc.sh <config.cfg>
#
# TLC checks for deadlock by default; the -deadlock flag would turn that check
# off, so it is not passed. Terminated runs are not reported as deadlocks
# because the PlusCal translation's `Terminating` disjunct lets a finished
# system stutter.
set -euo pipefail

CFG="${1:-MC_small.cfg}"
cd "$(dirname "$0")"
java -cp tools/tla2tools.jar tlc2.TLC \
  -workers auto \
  -config "$CFG" \
  FileIOCatalog
