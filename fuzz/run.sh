#!/bin/bash
# Runs fileio-catalog's fuzz image on a host with Docker (rootless is fine). See README.md.
#
#   run.sh build                                  build the image from ./ctx (pulls the base image)
#   run.sh round <fork> <commit> <name>           a phase-exit round (eleven jobs, or ROUND_JOBS), detached
#   run.sh calib <fork> <commit> <name> <set>     a calibration (mutants/<set>.sh), detached
#   run.sh test <fork> <commit> <name> <tests>    one test job, detached
#   run.sh shrink <fork> <commit> <name> <run>/<job>
#                                                 shrink what failed in a job, detached
#   run.sh status [name]                          each run's jobs and results, from $OUT
#   run.sh mem                                    each run's memory and CPU, and the host's
#   run.sh logs <name>                            follow a run's container
#   run.sh stop <name>                            stop a run (its logs stay in $OUT)
#   run.sh forks                                  the fork builds and the base in the image
#   run.sh plan <cmd> <args...>                   the JVMs, memory limit and CPUs a run would take
#
# Env: OUT, the results directory (default ./out); BUNDLE, the repository bundle (default
# ./fileio.bundle); IMAGE (default ce-fuzz:latest); MEM, the container's memory limit (default:
# its JVMs at once times their heap and 2g, and 2g); CPUS, its CPU limit (docker --cpus; default
# one and a half per JVM, at most the host's); CPU_SHARES, its weight against other cgroups when
# the CPUs are all busy (docker --cpu-shares; default 1024; 2 runs it only on cycles no one else
# wants: a soak); passed to the container: JOBS (jobs at once, default all; a shrink's 1), SHARDS
# (JVMs per round job, default ce-fuzz's), STAGGER (seconds between job starts, default 10),
# HEAP (each test JVM's heap, default 4g), SHRINKING (default OFF),
# SHRINK_HEAP (default 16g), SHRINK_SECONDS (default 1800), JOB_TIMEOUT, SMOKE=1 (3 tries per
# property: checks the image, not the code), REBASE_TRIES, ROUND_JOBS (a round's jobs, to rerun
# some: "race", "faultA1 faultB1"), SNAPSHOT_BLOCK and SNAPSHOT_WINDOW (the snapshot paging the
# tests' catalogs are created with; unset, the catalog's default; 0, none; 4 and 8 page
# constantly, 2 and 1 reach re-inlining's import pages).
set -euo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUT=${OUT:-$HERE/out}
BUNDLE=${BUNDLE:-$HERE/fileio.bundle}
IMAGE=${IMAGE:-ce-fuzz:latest}
usage() { sed -n '2,28p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }

# gigabytes <size>: a JVM heap size (4g, 512m) in whole gigabytes, rounded up
gigabytes() {
  case $1 in
    *[gG]) echo "${1%[gG]}" ;;
    *[mM]) echo $(( (${1%[mM]} + 1023) / 1024 )) ;;
    *) echo "run.sh: cannot read the size $1" >&2; exit 2 ;;
  esac
}

# slots <cmd> <args...>: the JVMs a run runs at once: a round's jobs' shards (ROUND_JOBS and
# SHARDS, as the image's driver counts them, so a job or shard it adds counts), a calibration's
# mutants and its unmutated run, one test or shrink; JOBS at most
slots() {
  local n
  case $1 in
    round) n=$(bash "$HERE/ctx/ce-fuzz" slots) || exit 2 ;;
    calib) n=$(( $(cd "$HERE/ctx" && bash -c '. mutants/'"$5"'.sh && echo $MUTANTS' | wc -w) + 1 )) ;;
    *) n=1 ;;
  esac
  if [ -n "${JOBS:-}" ] && [ "$JOBS" -lt "$n" ]; then n=$JOBS; fi
  echo "$n"
}

# limit <cmd> <slots>: the container's memory limit, unless MEM sets it: each JVM it runs at once
# with its heap (a shrink's SHRINK_HEAP) and 2g for the JVM's other memory and Maven's, and 2g
limit() {
  if [ -n "${MEM:-}" ]; then echo "$MEM"; return; fi
  local heap=${HEAP:-4g}
  [ "$1" = shrink ] && heap=${SHRINK_HEAP:-16g}
  echo "$(( $2 * ($(gigabytes "$heap") + 2) + 2 ))g"
}

# cpus <slots>: the container's CPU limit, unless CPUS sets it: one and a half per JVM (a job
# runs one thread of tests, and the JVM's compilers and collectors), at least 2, at most the host's
cpus() {
  if [ -n "${CPUS:-}" ]; then echo "$CPUS"; return; fi
  local c=$(( ($1 * 3 + 1) / 2 )) n; n=$(nproc)
  [ $c -lt 2 ] && c=2
  [ $c -gt "$n" ] && c=$n
  echo $c
}

cmd=${1:-}; shift || true
case $cmd in
  build)
    docker build -t "$IMAGE" "$HERE/ctx" ;;
  plan)
    [ $# -ge 4 ] || usage
    slots=$(slots "$@") || exit 2
    echo "slots=$slots mem=$(limit "$1" "$slots") cpus=$(cpus "$slots")" ;;
  round|calib|test|shrink)
    [ $# -ge 3 ] || usage
    name=$3
    slots=$(slots "$cmd" "$@") || exit 2
    mem=$(limit "$cmd" "$slots"); cpu=$(cpus "$slots")
    [ -f "$BUNDLE" ] || { echo "run.sh: no bundle at $BUNDLE" >&2; exit 2; }
    mkdir -p "$OUT"
    if loginctl show-user "$USER" -p Linger 2>/dev/null | grep -q 'Linger=no'; then
      echo "run.sh: $USER does not linger: a rootless Docker daemon (and this run) stops at" \
        "logout unless you run 'loginctl enable-linger'" >&2
    fi
    # A hard limit, without swap: on a shared host, the kernel's OOM killer then kills a JVM of
    # this run, and only of this run, rather than anyone's process; and should the host run out
    # of memory, it picks this run's first (the highest OOM score).
    # A CPU limit, so a run of many JVMs leaves the host's other users their share however its
    # JVMs' compilers and collectors burst; CPU_SHARES 2 makes it yield to every other cgroup.
    docker run -d --init --name "ce-fuzz-$name" \
      --memory "$mem" --memory-swap "$mem" --oom-score-adj 1000 \
      --cpus "$cpu" ${CPU_SHARES:+--cpu-shares "$CPU_SHARES"} \
      -v "$OUT:/out" -v "$(readlink -f "$BUNDLE"):/in/fileio.bundle:ro" \
      -e JOBS -e HEAP -e SHRINKING -e SHRINK_HEAP -e SHRINK_SECONDS -e JOB_TIMEOUT \
      -e SMOKE -e REBASE_TRIES -e ROUND_JOBS -e SNAPSHOT_BLOCK -e SNAPSHOT_WINDOW \
      -e SHARDS -e STAGGER \
      "$IMAGE" "$cmd" "$@"
    held=$(docker exec "ce-fuzz-$name" cat /sys/fs/cgroup/memory.max 2>/dev/null || true)
    if [ -z "$held" ] || [ "$held" = max ]; then
      echo "run.sh: WARNING: the container has no memory limit (${held:-unreadable}): rootless" \
        "Docker needs the memory controller delegated to the user's cgroup; 'run.sh stop $name'" \
        "unless JOBS x HEAP fits the host with room to spare" >&2
    fi
    echo "started ce-fuzz-$name, $slots JVMs, memory limit $mem, CPUs $cpu${CPU_SHARES:+ at weight $CPU_SHARES}:" \
      "run.sh status $name, run.sh logs $name" ;;
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
