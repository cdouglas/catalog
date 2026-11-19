#!/bin/bash
# Starts run.sh's runs from a queue file as a shared host has room for them, and soaks the host's
# spare cores and memory with rounds when it has nothing queued, stopping them as soon as anyone
# needs the room. Runs next to run.sh (README.md).
#
#   queue.sh <file>   runs until killed; start it detached, e.g.
#                     setsid -f nohup ./queue.sh q.txt >> out/queue.log 2>&1 < /dev/null
#
# Each line of <file> is a run.sh command and its arguments after any environment assignments,
# quoted as bash quotes them (JOBS=4 SHARDS="azure=16" round p16 36b7e98 p16f-r0); # starts a
# comment. Lines start in order, each once there is room for it:
#   load    the host's 5-minute load average plus the run's JVMs is at most LOAD_MAX (default
#           120), counting each run started in the last RAMP seconds (default 600) at its JVMs, as
#           the average lags, and not counting soak rounds older than that, which yield the CPUs;
#   memory  the host's available memory (with the ZFS ARC above its floor, which the kernel gives
#           back under pressure), less what every running ce-fuzz container may still grow by (its
#           limit less its use), exceeds the run's limit by MEM_RESERVE gigabytes (default 64):
#           were every container to reach its limit, the host would keep the reserve. Our runs
#           never oversubscribe the host; a soak round stands aside: if stopping soak rounds
#           would make the room, the newest is stopped.
# Lines appended while it runs are taken in turn; each line started is recorded in <file>.done,
# so a restarted queue skips it (a line is its text: give each run its own name).
#
# A line "soak <fork> <commit> <prefix>" (after assignments, as for a round) makes the queue, when
# nothing queued is waiting, start rounds <prefix>-1, <prefix>-2, ... of <commit>, at most
# SOAK_MAX at once (default 1), each when the load is under SOAK_LOAD (default 100) and the memory
# has room as above, at CPU_SHARES 2: they take only cycles no other process wants. Memory does
# not yield so: when the room falls below the reserve (another user's demand, or ours, grew), the
# newest soak round is stopped (docker rm -f; its logs stay, its unfinished shards' seeds are
# lost), and no soak round starts for SOAK_BACKOFF seconds (default 1800). The file's last soak
# line holds ("soak off" ends soaking); soak rounds already started run on. Every round draws
# fresh random seeds.
set -uo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUT=${OUT:-$HERE/out}
LOAD_MAX=${LOAD_MAX:-120}
SOAK_LOAD=${SOAK_LOAD:-100}
MEM_RESERVE=${MEM_RESERVE:-64}
RAMP=${RAMP:-600}
SOAK_MAX=${SOAK_MAX:-1}
SOAK_BACKOFF=${SOAK_BACKOFF:-1800}
usage_end=$(grep -n '^set -uo pipefail' "$0" | cut -d: -f1)
[ $# = 1 ] && [ -f "$1" ] || { sed -n "2,$(( usage_end - 1 ))p" "$0" | sed 's/^# \{0,1\}//'; exit 2; }
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

# soaking <prefix>: the soak rounds of <prefix> running, newest first, as "started-epoch name"
soaking() {
  local c
  for c in $(docker ps --format '{{.Names}}' | grep -E "^ce-fuzz-$1-[0-9]+$"); do
    echo "$(date -d "$(docker inspect -f '{{.State.StartedAt}}' "$c")" +%s) $c"
  done | sort -rn
}

# memory <prefix>: sets ROOM, the gigabytes our runs may still take without oversubscribing the
# host (its available memory, with the ZFS ARC above its floor, less what every running ce-fuzz
# container may still grow by), and SOAKS, what stopping the soak rounds of <prefix> gives back
# (their limits)
memory() {
  local avail arc
  avail=$(free -g | awk '/^Mem:/ {print $7}')
  arc=$(awk '$1 == "size" {s = $3} $1 == "c_min" {m = $3}
    END {printf "%d", (s > m ? s - m : 0) / 1073741824}' /proc/spl/kstat/zfs/arcstats 2>/dev/null)
  read -r ROOM SOAKS < <(docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' 2>/dev/null \
    | awk -v avail=$(( avail + ${arc:-0} )) -v soak="^ce-fuzz-${1:-none}-[0-9]+$" '
      function g(v,  u) { u = v; sub(/[0-9.]+/, "", u); v += 0
        return u == "TiB" ? v * 1024 : u == "GiB" ? v : u == "MiB" ? v / 1024 : u == "KiB" ? v / 1048576 : v / 1073741824 }
      $1 ~ /^ce-fuzz-/ { h += g($4) - g($2) }
      $1 ~ soak { s += g($4) }
      END { printf "%d %d\n", avail - h, s }')
}

# loaded <slots> <discount> <max>: whether the load leaves room for <slots> more JVMs under <max>,
# <discount> being the load to set aside (old soak rounds'); prints why not
loaded() {
  local load; load=$(cut -d' ' -f2 /proc/loadavg)
  awk -v l="$load" -v r="$(recent)" -v s="$1" -v d="$2" -v m="$3" \
    'BEGIN { exit !(l - d + r + s <= m) }' && return 0
  echo "load $load - $2 soaking + $(recent) ramping + $1 > $3"; return 1
}

# start: runs the parsed run; records its start
start() {
  local p slots
  p=$(plan) || return 1; slots=${p% *}
  if env "${ENVS[@]}" "$HERE/run.sh" "${ARGS[@]}"; then
    STARTED+=("$(date +%s) $slots"); return 0
  fi
  return 1
}

# preempt <prefix> <why>: stops the newest soak round of <prefix>
preempt() {
  local name; name=$(soaking "$1" | head -1 | cut -d' ' -f2)
  [ -n "$name" ] || return 1
  say "soak: stopping $name: $2"
  docker rm -f "$name" > /dev/null
  [ -d "$OUT/${name#ce-fuzz-}" ] \
    && echo "stopped $(date -u +%FT%TZ) by queue.sh: $2" >> "$OUT/${name#ce-fuzz-}/summary.txt"
}

say "queue $Q (LOAD_MAX $LOAD_MAX, SOAK_LOAD $SOAK_LOAD, MEM_RESERVE ${MEM_RESERVE}g, RAMP ${RAMP}s," \
  "SOAK_MAX $SOAK_MAX, SOAK_BACKOFF ${SOAK_BACKOFF}s)"
waiting=""; last_said=0; backoff_until=0
while true; do
  next=""; soak=""
  while IFS= read -r line || [ -n "$line" ]; do
    l=$(echo "$line" | sed -E 's/^[[:space:]]+|[[:space:]]+$//g')
    case $l in ''|'#'*) continue ;; esac
    if parse "$l" 2> /dev/null && [ "${ARGS[0]:-}" = soak ]; then soak=$l; continue; fi
    [ -z "$next" ] && ! grep -qxF "$l" "$DONE" && next=$l
  done < "$Q"

  # the soak line: its rounds' prefix and plan; after "soak off", or another prefix, the rounds
  # of the last prefix still running still yield and stand aside, but none starts
  sprefix=""; old=0; running=0
  if [ -n "$soak" ] && parse "$soak" && [ "${ARGS[1]:-}" != off ] && [ ${#ARGS[@]} = 4 ]; then
    sprefix=${ARGS[3]}; senvs=("${ENVS[@]}" CPU_SHARES=2); sfork=${ARGS[1]}; scommit=${ARGS[2]}
    ENVS=("${senvs[@]}"); ARGS=(round "$sfork" "$scommit" "$sprefix-0")
    sp=$(plan); sslots=${sp% *}; smem=${sp#* }
    [ -n "${lastprefix:-}" ] && [ "$lastprefix" != "$sprefix" ] && [ -n "$(soaking "$lastprefix")" ] \
      && say "soak: rounds of $lastprefix run on, untracked, as the soak line names $sprefix"
    lastprefix=$sprefix
  fi
  tprefix=${lastprefix:-}
  now=$(date +%s)
  if [ -n "$tprefix" ]; then
    while read -r t _; do
      [ -n "$t" ] || continue
      running=$(( running + 1 )); [ $(( now - t )) -ge "$RAMP" ] && old=$(( old + 1 ))
    done < <(soaking "$tprefix")
  fi
  ROOM=""; memory "$tprefix"
  if [ -z "$ROOM" ]; then
    say "cannot read the host's memory (docker stats); trying again"; sleep "${POLL:-60}"; continue
  fi

  # memory pressure: a soak round stands aside first
  if [ "$running" -gt 0 ] && [ "$ROOM" -lt "$MEM_RESERVE" ]; then
    preempt "$tprefix" "room ${ROOM}g < ${MEM_RESERVE}g reserve"
    backoff_until=$(( now + SOAK_BACKOFF ))
    sleep "${POLL:-60}"; continue
  fi

  if [ -n "$next" ]; then
    if ! parse "$next"; then
      say "cannot parse: $next"; echo "$next" >> "$DONE"; continue
    fi
    p=$(plan)
    if [ -z "$p" ]; then say "no plan for: $next (skipped)"; echo "$next" >> "$DONE"; continue; fi
    if why=$(loaded "${p% *}" $(( old * ${sslots:-0} )) "$LOAD_MAX"); then
      if [ $(( ROOM - ${p#* } )) -ge "$MEM_RESERVE" ]; then
        say "starting: $next"
        echo "$next" >> "$DONE"
        start || say "FAILED to start: $next"
        waiting=""
        sleep "${POLL:-60}"; continue
      fi
      why="room ${ROOM}g - ${p#* }g < ${MEM_RESERVE}g reserve"
      if [ "$running" -gt 0 ] && [ $(( ROOM + SOAKS - ${p#* } )) -ge "$MEM_RESERVE" ]; then
        preempt "$tprefix" "room for $next"
        sleep 5; continue
      fi
    fi
    if [ "$waiting" != "$next" ] || [ $(( now - last_said )) -ge 1800 ]; then
      say "waiting: $next ($why)"; waiting=$next; last_said=$now
    fi
  elif [ -n "$sprefix" ] && [ "$running" -lt "$SOAK_MAX" ] && [ "$now" -ge "$backoff_until" ]; then
    if ! why=$(loaded "$sslots" 0 "$SOAK_LOAD"); then
      :
    elif [ $(( ROOM - smem )) -lt "$MEM_RESERVE" ]; then
      why="room ${ROOM}g - ${smem}g < ${MEM_RESERVE}g reserve"
    else
      n=1; while [ -e "$OUT/$sprefix-$n" ] || docker inspect "ce-fuzz-$sprefix-$n" > /dev/null 2>&1; do n=$(( n + 1 )); done
      ENVS=("${senvs[@]}"); ARGS=(round "$sfork" "$scommit" "$sprefix-$n")
      say "soak: starting $sprefix-$n ($sfork $scommit, ${ENVS[*]})"
      start || say "FAILED to start soak round $sprefix-$n"
      why=""
    fi
    if [ -n "$why" ] && { [ "$waiting" != soak ] || [ $(( now - last_said )) -ge 1800 ]; }; then
      say "soak waiting: $why"; waiting=soak; last_said=$now
    fi
  fi
  sleep "${POLL:-60}"
done
