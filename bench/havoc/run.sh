#!/bin/bash
# Runs contention-driver campaigns on a host with Docker (rootless is fine). See README.md.
#
#   run.sh build                          build the image from ./ctx on BASE (pulls it if absent)
#   run.sh campaign <name> <configs...>   run configs/<file>s as JOBS shards, detached
#   run.sh ingest <name> <configs...>     the ingest benchmark on configs/<file>s, one container
#                                         (CPUS 16, HEAP 6g, MEM 12g unless set: a capture's 16
#                                         writers)
#   run.sh jmh <name> <campaign...> -- <JMH args...>
#                                         IngestFoldBench.fold over every log the campaigns
#                                         captured ($OUT/<campaign>/captures), and the JMH args
#                                         (results: JSON in $OUT/<name>/)
#   run.sh status [name]                  each shard's container, and the runs finished
#   run.sh mem                            each shard's memory and CPU, and the host's
#   run.sh logs <name> <shard>            follow a shard's container
#   run.sh stop <name>                    stop a campaign's shards (their results stay in $OUT)
#
# Env: OUT (default ./out; a campaign's runs go to $OUT/<name>/), IMAGE (default ce-bench:latest;
# one image per build lets builds run side by side), BASE (the image built on, any with Java 17;
# default eclipse-temurin:17-jre, or one the host has), JOBS (shards at once, default 8), HEAP
# (each JVM's heap, default 3g), MEM (each container's hard memory limit, default HEAP + 2g;
# ingest: HEAP + 6g, as an 8g limit killed h1's i8 ingest at a 6g heap; jmh: HEAP + 8g, for the 6g
# fork), CPUS (each container's CPUs, default 4).
set -euo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUT=${OUT:-$HERE/out}
IMAGE=${IMAGE:-ce-bench:latest}
BASE=${BASE:-eclipse-temurin:17-jre}
JOBS=${JOBS:-8}
usage() { sed -n '2,23p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
gigabytes() {
  case $1 in
    *[gG]) echo "${1%[gG]}" ;;
    *[mM]) echo $(( (${1%[mM]} + 1023) / 1024 )) ;;
    *) echo "run.sh: cannot read the size $1" >&2; exit 2 ;;
  esac
}
warn_linger() {
  if loginctl show-user "$USER" -p Linger 2>/dev/null | grep -q 'Linger=no'; then
    echo "run.sh: $USER does not linger: a rootless Docker daemon stops at logout unless you" \
      "run 'loginctl enable-linger'" >&2
  fi
}
check_limit() {
  held=$(docker exec "$1" cat /sys/fs/cgroup/memory.max 2>/dev/null || true)
  if [ -z "$held" ] || [ "$held" = max ]; then
    echo "run.sh: WARNING: the containers have no memory limit (${held:-unreadable}): rootless" \
      "Docker needs the memory controller delegated to the user's cgroup" >&2
  fi
}
# One container, detached, with a hard limit: run_one <container> <name> <env...> -- <args...>
run_one() {
  container=$1; name=$2; shift 2
  envs=()
  while [ "$1" != -- ]; do envs+=(-e "$1"); shift; done
  shift
  mkdir -p "$OUT/$name"
  # A hard limit without swap and the highest OOM score: on a shared host the kernel kills a
  # JVM of this campaign, and only of it, should it outgrow its limit or the host run short.
  docker run -d --init --name "$container" \
    --memory "$MEM" --memory-swap "$MEM" --oom-score-adj 1000 --cpus "$CPUS" \
    -v "$OUT/$name:/out" -v "$OUT:/results:ro" -v "$HERE/configs:/configs:ro" \
    -e HEAP="$HEAP" -e CPUS="$CPUS" "${envs[@]}" "$IMAGE" "$@" > /dev/null
}

cmd=${1:-}; shift || true
case $cmd in
  ingest) HEAP=${HEAP:-6g}; CPUS=${CPUS:-16}; MEM=${MEM:-$(( $(gigabytes "$HEAP") + 6 ))g} ;;
  jmh) HEAP=${HEAP:-1g}; CPUS=${CPUS:-4}; MEM=${MEM:-$(( $(gigabytes "$HEAP") + 8 ))g} ;;
  *) HEAP=${HEAP:-3g}; CPUS=${CPUS:-4} ;;
esac
MEM=${MEM:-$(( $(gigabytes "$HEAP") + 2 ))g}
case $cmd in
  build)
    docker build --build-arg BASE="$BASE" -t "$IMAGE" "$HERE/ctx" ;;
  campaign)
    [ $# -ge 2 ] || usage
    name=$1; shift
    files=()
    for f in "$@"; do
      [ -f "$HERE/configs/$(basename "$f")" ] || { echo "run.sh: no configs/$(basename "$f")" >&2; exit 2; }
      files+=("/configs/$(basename "$f")")
    done
    warn_linger
    for ((i = 0; i < JOBS; i++)); do
      run_one "ce-bench-$name-$i" "$name" -- "$i/$JOBS" "${files[@]}"
    done
    check_limit "ce-bench-$name-0"
    echo "started $JOBS shards of $name (memory $MEM, $CPUS CPUs each): run.sh status $name" ;;
  ingest)
    [ $# -ge 2 ] || usage
    name=$1; shift
    files=()
    for f in "$@"; do
      [ -f "$HERE/configs/$(basename "$f")" ] || { echo "run.sh: no configs/$(basename "$f")" >&2; exit 2; }
      files+=("/configs/$(basename "$f")")
    done
    warn_linger
    run_one "ce-bench-$name-0" "$name" MAIN=IngestBench -- "${files[@]}"
    check_limit "ce-bench-$name-0"
    echo "started $name (memory $MEM, $CPUS CPUs): run.sh logs $name 0" ;;
  jmh)
    [ $# -ge 2 ] || usage
    name=$1; shift
    logs=()
    while [ $# -gt 0 ] && [ "$1" != -- ]; do
      for log in "$OUT/$1"/captures/*/log.bin; do
        [ -f "$log" ] && logs+=("/results/${log#"$OUT"/}")
      done
      shift
    done
    [ "${1:-}" = -- ] && shift
    [ ${#logs[@]} -gt 0 ] || { echo "run.sh: no captures under those campaigns" >&2; exit 2; }
    joined=$(IFS=,; echo "${logs[*]}")
    run_one "ce-bench-$name-0" "$name" MAIN=jmh -- \
      IngestFoldBench.fold -p log="$joined" -rf json -rff "/out/$name.json" "$@"
    check_limit "ce-bench-$name-0"
    echo "started $name over ${#logs[@]} captures (memory $MEM, $CPUS CPUs): run.sh logs $name 0" ;;
  status)
    filter=ce-bench-${1:-}
    docker ps -a --filter "name=$filter" --format 'table {{.Names}}\t{{.Status}}'
    for d in "$OUT"/${1:-*}; do
      [ -d "$d" ] || continue
      manifests=$(find "$d" -mindepth 3 -maxdepth 3 -name manifest.json)
      total=$(echo "$manifests" | grep -c . || true)
      done_=0
      failed=0
      if [ -n "$manifests" ]; then
        # shellcheck disable=SC2086
        done_=$(grep -l '"finished"' $manifests | wc -l || true)
        # shellcheck disable=SC2086
        failed=$(grep -l '"failed"' $manifests | wc -l || true)
      fi
      echo "$(basename "$d"): $done_ finished, $failed failed, $((total - done_ - failed)) running"
    done ;;
  mem)
    ids=$(docker ps -q --filter name=ce-bench-)
    if [ -n "$ids" ]; then
      # shellcheck disable=SC2086
      docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.CPUPerc}}' $ids
    fi
    free -g ;;
  logs)
    [ $# -eq 2 ] || usage
    docker logs -f "ce-bench-$1-$2" ;;
  stop)
    [ $# -eq 1 ] || usage
    docker ps -aq --filter "name=ce-bench-$1-" | xargs -r docker rm -f ;;
  *) usage ;;
esac
