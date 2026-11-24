#!/bin/bash
# soak-guard.sh <queue file> <deadline> [low GiB] [high GiB]: keeps a queue's soak line (queue.sh)
# within the host's disk. While /data has less than <low> GiB free (default 40) it appends "soak
# off", so no soak round starts (rounds already running run on); once it has <high> GiB (default
# 60) again it appends the soak line back, so soaking resumes. At <deadline> (date -d) it appends
# "soak off" and exits. The soak line is the file's last one when the guard starts.
set -u
Q=$1; DEADLINE=$(date -d "$2" +%s); LOW=${3:-40}; HIGH=${4:-60}; POLL=${POLL:-300}
LINE=$(grep -E '^[^#]*\bsoak [^ ]+ [^ ]+ [^ ]+' "$Q" | grep -v 'soak off' | tail -1)
[ -n "$LINE" ] || { echo "soak-guard: no soak line in $Q" >&2; exit 1; }
say() { echo "$(date -u +%FT%TZ) $*"; }
paused=0
say "guarding: $LINE (low ${LOW}G, high ${HIGH}G, until $2)"
while :; do
  free=$(df --output=avail -BG /data | tail -1 | tr -dc 0-9)
  if [ "$(date +%s)" -ge "$DEADLINE" ]; then
    echo "# $(date -u +%FT%TZ) soak-guard: deadline" >> "$Q"; echo "soak off" >> "$Q"
    say "deadline: soak off (${free}G free)"; exit 0
  fi
  if [ "$paused" = 0 ] && [ "$free" -lt "$LOW" ]; then
    echo "# $(date -u +%FT%TZ) soak-guard: /data ${free}G free, pausing" >> "$Q"; echo "soak off" >> "$Q"
    paused=1; say "paused (${free}G free)"
  elif [ "$paused" = 1 ] && [ "$free" -ge "$HIGH" ]; then
    echo "# $(date -u +%FT%TZ) soak-guard: /data ${free}G free, resuming" >> "$Q"; echo "$LINE" >> "$Q"
    paused=0; say "resumed (${free}G free)"
  fi
  sleep "$POLL"
done
