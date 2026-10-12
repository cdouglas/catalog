#!/usr/bin/env bash
# Usage: spec/tla/run-tlc.sh <config.cfg>
#
# NOTE: -deadlock (which enables deadlock *checking*) is intentionally omitted
# in this bootstrap because the trivial Next == UNCHANGED storage action would
# otherwise be reported as a deadlock. Task 10 introduces real transitions, at
# which point deadlock checking will be re-enabled.
set -euo pipefail

CFG="${1:-MC_small.cfg}"
cd "$(dirname "$0")"
java -cp tools/tla2tools.jar tlc2.TLC \
  -workers auto \
  -config "$CFG" \
  FileIOCatalog
