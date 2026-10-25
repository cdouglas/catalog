#!/bin/bash
# Runs fileio-catalog's fuzz image on a host with Docker (rootless is fine). See README.md.
#
#   run.sh build                                  build the image from ./ctx (pulls the base image)
#   run.sh round <fork> <commit> <name>           a phase-exit round (nine jobs), detached
#   run.sh calib <fork> <commit> <name> <set>     a calibration (mutants/<set>.sh), detached
#   run.sh test <fork> <commit> <name> <tests>    one test job, detached
#   run.sh status [name]                          each run's jobs and results, from $OUT
#   run.sh logs <name>                            follow a run's container
#   run.sh stop <name>                            stop a run (its logs stay in $OUT)
#   run.sh forks                                  the fork builds and the base in the image
#
# Env: OUT, the results directory (default ./out); BUNDLE, the repository bundle (default
# ./fileio.bundle); IMAGE (default ce-fuzz:latest); passed to the container: JOBS (jobs at once,
# default all), HEAP (each test JVM's heap, default 2g), SMOKE=1 (3 tries per property: checks
# the image, not the code), REBASE_TRIES.
set -euo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUT=${OUT:-$HERE/out}
BUNDLE=${BUNDLE:-$HERE/fileio.bundle}
IMAGE=${IMAGE:-ce-fuzz:latest}
usage() { sed -n '2,19p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }

cmd=${1:-}; shift || true
case $cmd in
  build)
    docker build -t "$IMAGE" "$HERE/ctx" ;;
  round|calib|test)
    [ $# -ge 3 ] || usage
    name=$3
    [ -f "$BUNDLE" ] || { echo "run.sh: no bundle at $BUNDLE" >&2; exit 2; }
    mkdir -p "$OUT"
    if loginctl show-user "$USER" -p Linger 2>/dev/null | grep -q 'Linger=no'; then
      echo "run.sh: $USER does not linger: a rootless Docker daemon (and this run) stops at" \
        "logout unless you run 'loginctl enable-linger'" >&2
    fi
    docker run -d --init --name "ce-fuzz-$name" \
      -v "$OUT:/out" -v "$(readlink -f "$BUNDLE"):/in/fileio.bundle:ro" \
      -e JOBS -e HEAP -e SMOKE -e REBASE_TRIES \
      "$IMAGE" "$cmd" "$@"
    echo "started ce-fuzz-$name: run.sh status $name, run.sh logs $name" ;;
  status)
    docker run --rm -v "$OUT:/out:ro" "$IMAGE" status "$@" ;;
  logs)
    [ $# -eq 1 ] || usage
    docker logs -f "ce-fuzz-$1" ;;
  stop)
    [ $# -eq 1 ] || usage
    docker rm -f "ce-fuzz-$1" ;;
  forks)
    docker run --rm --entrypoint cat "$IMAGE" /opt/ce-fuzz/provenance.txt ;;
  *) usage ;;
esac
