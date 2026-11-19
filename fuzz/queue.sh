#!/bin/bash
# Starts run.sh's runs from a queue file as a shared host has room for them, and soaks the host's
# idle cores with rounds when it has nothing queued. Runs next to run.sh (README.md).
#
#   queue.sh <file>   runs until killed; start it detached, e.g.
#                     setsid nohup ./queue.sh q.txt > out/queue.log 2>&1 < /dev/null &
#
# Each line of <file> is a run.sh command and its arguments after any environment assignments,
# quoted as bash quotes them (JOBS=4 SHARDS="azure=16" round p16 36b7e98 p16f-r0); # starts a
# comment. Lines start in order, each once the host's 5-minute load average (counting each run
# started in the last RAMP seconds, default 600, at its JVMs, as it ramps up; and not counting
# soak rounds older than that, which yield) plus the run's JVMs is at most LOAD_MAX (default 120),
# and the host's available memory, less what every running ce-fuzz container may still grow by
# (its limit less its use), exceeds the run's memory limit by MEM_RESERVE gigabytes (default 64):
# were every container to reach its limit, the host would keep the reserve. Lines appended while
# it runs are taken in turn; each line started is recorded in <file>.done, so a restarted queue
# skips it (a line is its text: give each run its own name).
#
# A line "soak <fork> <commit> <prefix>" (after assignments, as for a round) makes the queue, when
# nothing queued is waiting, start rounds <prefix>-1, <prefix>-2, ... of <commit>, at most
# SOAK_MAX at once (default 1), each when the load allows it under SOAK_LOAD (default 100) and
# the memory as above, at CPU_SHARES 2: they take only cycles no other process wants. The file's
# last soak line holds ("soak off" ends soaking); soak rounds already started run on. Every round
# draws fresh random seeds.
set -uo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUT=${OUT:-$HERE/out}
LOAD_MAX=${LOAD_MAX:-120}
SOAK_LOAD=${SOAK_LOAD:-100}
MEM_RESERVE=${MEM_RESERVE:-64}
RAMP=${RAMP:-600}
SOAK_MAX=${SOAK_MAX:-1}
[ $# = 1 ] && [ -f "$1" ] || { sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
Q=$(readlink -f "$1"); DONE=$Q.done
touch "$DONE"
say() { echo "$(date -u +%FT%TZ) $*"; }

# parse <line>: sets ENVS (the line's assignments) and ARGS (the rest), as bash splits and quotes
parse() {
  ENVS=(); ARGS=()
  eval "set -- $1" || return 1
  while [ $# -gt 0 ] && [[ $1 =~ ^[A-Z_][A-Z0-9_]*= ]]; do ENVS+=("$1"); shift; done
  ARGS=("$@")
}

# plan: "slots mem(g)" of the parsed run (CPUs aside)
plan() {
  env "${ENVS[@]}" "$HERE/run.sh" plan "${ARGS[@]}" \
    | sed -E 's/^slots=([0-9]+) mem=([0-9]+)g.*/\1 \2/'
}

# recent: the JVMs of runs this queue started in the last RAMP seconds (STARTED: "epoch slots")
STARTED=()
recent() {
  local now e s n=0; now=$(date +%s)
  for e in "${STARTED[@]}"; do s=${e#* }; [ $(( now - ${e%% *} )) -lt "$RAMP" ] && n=$(( n + s )); done
  echo $n
}

# soaking <prefix>: the soak rounds of <prefix> running, as "started-epoch" lines
soaking() {
  local c
  for c in $(docker ps --format '{{.Names}}' | grep -E "^ce-fuzz-$1-[0-9]+$"); do
    date -d "$(docker inspect -f '{{.State.StartedAt}}' "$c")" +%s
  done
}

# headroom: the gigabytes the running ce-fuzz containers may still take, each its memory limit
# less what it uses
headroom() {
  docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' 2>/dev/null | awk '
    function g(v) { u = v; sub(/[0-9.]+/, "", u); v += 0
      return u == "TiB" ? v * 1024 : u == "GiB" ? v : u == "MiB" ? v / 1024 : u == "KiB" ? v / 1048576 : v / 1073741824 }
    $1 ~ /^ce-fuzz-/ { h += g($4) - g($2) } END { printf "%d\n", h }'
}

# fits <slots> <mem> <discount> <max>: whether the host has room for the run now under the load
# <max>, discount being the load to set aside (old soak rounds'); prints why not
fits() {
  local load avail head
  load=$(cut -d' ' -f2 /proc/loadavg)
  head=$(headroom)
  avail=$(( $(free -g | awk '/^Mem:/ {print $7}') - ${head:-0} ))
  if ! awk -v l="$load" -v r="$(recent)" -v s="$1" -v d="$3" -v m="$4" \
      'BEGIN { exit !(l - d + r + s <= m) }'; then
    echo "load $load - $3 soaking + $(recent) ramping + $1 > $4"; return 1
  fi
  if [ $(( avail - $2 )) -lt "$MEM_RESERVE" ]; then
    echo "available less headroom ${avail}g - ${2}g < ${MEM_RESERVE}g reserve"; return 1
  fi
}

# start <name-for-the-log>: runs the parsed run; records its start
start() {
  local p slots
  p=$(plan) || return 1; slots=${p% *}
  if env "${ENVS[@]}" "$HERE/run.sh" "${ARGS[@]}"; then
    STARTED+=("$(date +%s) $slots"); return 0
  fi
  return 1
}

say "queue $Q (LOAD_MAX $LOAD_MAX, SOAK_LOAD $SOAK_LOAD, MEM_RESERVE ${MEM_RESERVE}g, RAMP ${RAMP}s, SOAK_MAX $SOAK_MAX)"
waiting=""; last_said=0
while true; do
  next=""; soak=""
  while IFS= read -r line || [ -n "$line" ]; do
    l=$(echo "$line" | sed -E 's/^[[:space:]]+|[[:space:]]+$//g')
    case $l in ''|'#'*) continue ;; esac
    if parse "$l" 2> /dev/null && [ "${ARGS[0]:-}" = soak ]; then soak=$l; continue; fi
    [ -z "$next" ] && ! grep -qxF "$l" "$DONE" && next=$l
  done < "$Q"

  # the soak rounds' state: those older than RAMP are set aside from the load (they yield)
  sprefix=""; sslots=0; old=0; running=0
  if [ -n "$soak" ] && parse "$soak" && [ "${ARGS[1]:-}" != off ] && [ ${#ARGS[@]} = 4 ]; then
    sprefix=${ARGS[3]}; senvs=("${ENVS[@]}" CPU_SHARES=2); sfork=${ARGS[1]}; scommit=${ARGS[2]}
    ENVS=("${senvs[@]}"); ARGS=(round "$sfork" "$scommit" "$sprefix-0")
    sslots=$(plan | cut -d' ' -f1); smem=$(plan | cut -d' ' -f2)
  elif [ -n "$soak" ]; then
    soak=""
  fi
  if [ -n "$sprefix" ]; then
    now=$(date +%s)
    for t in $(soaking "$sprefix"); do
      running=$(( running + 1 )); [ $(( now - t )) -ge "$RAMP" ] && old=$(( old + 1 ))
    done
  fi

  if [ -n "$next" ]; then
    if ! parse "$next"; then
      say "cannot parse: $next"; echo "$next" >> "$DONE"; continue
    fi
    p=$(plan)
    if [ -z "$p" ]; then say "no plan for: $next (skipped)"; echo "$next" >> "$DONE"; continue; fi
    if why=$(fits "${p% *}" "${p#* }" $(( old * sslots )) "$LOAD_MAX"); then
      say "starting: $next"
      echo "$next" >> "$DONE"
      start || say "FAILED to start: $next"
      waiting=""
    elif [ "$waiting" != "$next" ] || [ $(( $(date +%s) - last_said )) -ge 1800 ]; then
      say "waiting: $next ($why)"; waiting=$next; last_said=$(date +%s)
    fi
  elif [ -n "$sprefix" ] && [ "$running" -lt "$SOAK_MAX" ]; then
    if why=$(fits "$sslots" "$smem" 0 "$SOAK_LOAD"); then
      n=1; while [ -e "$OUT/$sprefix-$n" ] || docker inspect "ce-fuzz-$sprefix-$n" > /dev/null 2>&1; do n=$(( n + 1 )); done
      ENVS=("${senvs[@]}"); ARGS=(round "$sfork" "$scommit" "$sprefix-$n")
      say "soak: starting $sprefix-$n ($sfork $scommit, ${ENVS[*]})"
      start || say "FAILED to start soak round $sprefix-$n"
    elif [ "$waiting" != soak ] || [ $(( $(date +%s) - last_said )) -ge 1800 ]; then
      say "soak waiting: $why"; waiting=soak; last_said=$(date +%s)
    fi
  fi
  sleep "${POLL:-60}"
done
