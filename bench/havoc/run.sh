#!/bin/bash
# Runs contention-driver campaigns on a host with Docker (rootless is fine). See README.md.
#
#   run.sh build                          build the image from ./ctx (pulls eclipse-temurin:17-jre)
#   run.sh campaign <name> <configs...>   run configs/<file>s as JOBS shards, detached
#   run.sh status [name]                  each shard's container, and the runs finished
#   run.sh mem                            each shard's memory and CPU, and the host's
#   run.sh logs <name> <shard>            follow a shard's container
#   run.sh stop <name>                    stop a campaign's shards (their results stay in $OUT)
#
# Env: OUT (default ./out; a campaign's runs go to $OUT/<name>/), IMAGE (default ce-bench:latest),
# JOBS (shards at once, default 8), HEAP (each JVM's heap, default 3g), MEM (each container's
# hard memory limit, default HEAP + 2g), CPUS (each container's CPUs, default 4).
set -euo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUT=${OUT:-$HERE/out}
IMAGE=${IMAGE:-ce-bench:latest}
JOBS=${JOBS:-8}
HEAP=${HEAP:-3g}
CPUS=${CPUS:-4}
usage() { sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
gigabytes() {
  case $1 in
    *[gG]) echo "${1%[gG]}" ;;
    *[mM]) echo $(( (${1%[mM]} + 1023) / 1024 )) ;;
    *) echo "run.sh: cannot read the size $1" >&2; exit 2 ;;
  esac
}
MEM=${MEM:-$(( $(gigabytes "$HEAP") + 2 ))g}

cmd=${1:-}; shift || true
case $cmd in
  build)
    docker build -t "$IMAGE" "$HERE/ctx" ;;
  campaign)
    [ $# -ge 2 ] || usage
    name=$1; shift
    files=()
    for f in "$@"; do
      [ -f "$HERE/configs/$(basename "$f")" ] || { echo "run.sh: no configs/$(basename "$f")" >&2; exit 2; }
      files+=("/configs/$(basename "$f")")
    done
    mkdir -p "$OUT/$name"
    if loginctl show-user "$USER" -p Linger 2>/dev/null | grep -q 'Linger=no'; then
      echo "run.sh: $USER does not linger: a rootless Docker daemon stops at logout unless you" \
        "run 'loginctl enable-linger'" >&2
    fi
    for ((i = 0; i < JOBS; i++)); do
      # A hard limit without swap and the highest OOM score: on a shared host the kernel kills a
      # JVM of this campaign, and only of it, should it outgrow its limit or the host run short.
      docker run -d --init --name "ce-bench-$name-$i" \
        --memory "$MEM" --memory-swap "$MEM" --oom-score-adj 1000 --cpus "$CPUS" \
        -v "$OUT/$name:/out" -v "$HERE/configs:/configs:ro" -e HEAP="$HEAP" \
        "$IMAGE" "$i/$JOBS" "${files[@]}" > /dev/null
    done
    held=$(docker exec "ce-bench-$name-0" cat /sys/fs/cgroup/memory.max 2>/dev/null || true)
    if [ -z "$held" ] || [ "$held" = max ]; then
      echo "run.sh: WARNING: the containers have no memory limit (${held:-unreadable}): rootless" \
        "Docker needs the memory controller delegated to the user's cgroup" >&2
    fi
    echo "started $JOBS shards of $name (memory $MEM, $CPUS CPUs each): run.sh status $name" ;;
  status)
    filter=ce-bench-${1:-}
    docker ps -a --filter "name=$filter" --format 'table {{.Names}}\t{{.Status}}'
    for d in "$OUT"/${1:-*}; do
      [ -d "$d" ] || continue
      total=$(find "$d" -mindepth 3 -maxdepth 3 -name manifest.json | wc -l)
      done_=$(grep -l '"finished"' $(find "$d" -mindepth 3 -maxdepth 3 -name manifest.json) 2>/dev/null | wc -l)
      failed=$(grep -l '"failed"' $(find "$d" -mindepth 3 -maxdepth 3 -name manifest.json) 2>/dev/null | wc -l)
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
