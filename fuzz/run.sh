#!/bin/bash
# Runs fileio-catalog's fuzz image on a host with Docker (rootless is fine). See README.md.
#
#   run.sh build                                  build the image from ./ctx (pulls the base image)
#   run.sh round <fork> <commit> <name>           a phase-exit round (nine jobs), detached
#   run.sh calib <fork> <commit> <name> <set>     a calibration (mutants/<set>.sh), detached
#   run.sh test <fork> <commit> <name> <tests>    one test job, detached
#   run.sh shrink <fork> <commit> <name> <run>/<job>
#                                                 shrink what failed in a job, detached
#   run.sh status [name]                          each run's jobs and results, from $OUT
#   run.sh mem                                    each run's memory and CPU, and the host's
#   run.sh logs <name>                            follow a run's container
#   run.sh stop <name>                            stop a run (its logs stay in $OUT)
#   run.sh forks                                  the fork builds and the base in the image
#
# Env: OUT, the results directory (default ./out); BUNDLE, the repository bundle (default
# ./fileio.bundle); IMAGE (default ce-fuzz:latest); MEM, the container's memory limit (default:
# its jobs at once times their heap and 2g, and 2g); passed to the container: JOBS (jobs at once,
# default all; a shrink's 1), HEAP (each test JVM's heap, default 4g), SHRINKING (default OFF),
# SHRINK_HEAP (default 16g), SHRINK_SECONDS (default 1800), JOB_TIMEOUT, SMOKE=1 (3 tries per
# property: checks the image, not the code), REBASE_TRIES.
set -euo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUT=${OUT:-$HERE/out}
BUNDLE=${BUNDLE:-$HERE/fileio.bundle}
IMAGE=${IMAGE:-ce-fuzz:latest}
usage() { sed -n '2,21p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }

# gigabytes <size>: a JVM heap size (4g, 512m) in whole gigabytes, rounded up
gigabytes() {
  case $1 in
    *[gG]) echo "${1%[gG]}" ;;
    *[mM]) echo $(( (${1%[mM]} + 1023) / 1024 )) ;;
    *) echo "run.sh: cannot read the size $1" >&2; exit 2 ;;
  esac
}

# limit <cmd> <args...>: the container's memory limit, unless MEM sets it: each job it runs at
# once (JOBS, else a round's nine, a calibration's mutants and its unmutated run, one test or
# shrink) with its heap and 2g for the JVM's other memory and Maven's, and 2g
limit() {
  if [ -n "${MEM:-}" ]; then echo "$MEM"; return; fi
  local cmd=$1 slots heap=${HEAP:-4g}
  case $cmd in
    round) slots=9 ;;
    calib) slots=$(( $(cd "$HERE/ctx" && bash -c '. mutants/'"$5"'.sh && echo $MUTANTS' | wc -w) + 1 )) ;;
    shrink) slots=1; heap=${SHRINK_HEAP:-16g} ;;
    *) slots=1 ;;
  esac
  if [ -n "${JOBS:-}" ] && [ "$JOBS" -lt "$slots" ]; then slots=$JOBS; fi
  echo "$(( slots * ($(gigabytes "$heap") + 2) + 2 ))g"
}

cmd=${1:-}; shift || true
case $cmd in
  build)
    docker build -t "$IMAGE" "$HERE/ctx" ;;
  round|calib|test|shrink)
    [ $# -ge 3 ] || usage
    name=$3
    mem=$(limit "$cmd" "$@")
    [ -f "$BUNDLE" ] || { echo "run.sh: no bundle at $BUNDLE" >&2; exit 2; }
    mkdir -p "$OUT"
    if loginctl show-user "$USER" -p Linger 2>/dev/null | grep -q 'Linger=no'; then
      echo "run.sh: $USER does not linger: a rootless Docker daemon (and this run) stops at" \
        "logout unless you run 'loginctl enable-linger'" >&2
    fi
    # A hard limit, without swap: on a shared host, the kernel's OOM killer then kills a JVM of
    # this run, and only of this run, rather than anyone's process; and should the host run out
    # of memory, it picks this run's first (the highest OOM score).
    docker run -d --init --name "ce-fuzz-$name" \
      --memory "$mem" --memory-swap "$mem" --oom-score-adj 1000 \
      -v "$OUT:/out" -v "$(readlink -f "$BUNDLE"):/in/fileio.bundle:ro" \
      -e JOBS -e HEAP -e SHRINKING -e SHRINK_HEAP -e SHRINK_SECONDS -e JOB_TIMEOUT \
      -e SMOKE -e REBASE_TRIES \
      "$IMAGE" "$cmd" "$@"
    held=$(docker exec "ce-fuzz-$name" cat /sys/fs/cgroup/memory.max 2>/dev/null || true)
    if [ -z "$held" ] || [ "$held" = max ]; then
      echo "run.sh: WARNING: the container has no memory limit (${held:-unreadable}): rootless" \
        "Docker needs the memory controller delegated to the user's cgroup; 'run.sh stop $name'" \
        "unless JOBS x HEAP fits the host with room to spare" >&2
    fi
    echo "started ce-fuzz-$name, memory limit $mem: run.sh status $name, run.sh logs $name" ;;
  mem)
    ids=$(docker ps -q --filter name=ce-fuzz-)
    if [ -n "$ids" ]; then
      # shellcheck disable=SC2086
      docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.CPUPerc}}' $ids
    fi
    free -g ;;
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
