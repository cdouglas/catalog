#!/usr/bin/env bash
# Packs the contention driver for another host (havoc): a build of this tree, its jars, the
# campaign configurations, a JRE image's context and the host-side run.sh, in
# bench/build/bench-<UTC stamp>.tar. On the host: tar xf, ./run.sh build, ./run.sh campaign ...
# (README.md).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
build=$("$root/bench/run.sh" --build-only | tail -1)
stamp=$(date -u +%Y%m%dT%H%M%SZ)
pack="$root/bench/build/bench-$stamp"
mkdir -p "$pack/ctx/app/lib" "$pack/configs"
cp -r "$build/classes" "$pack/ctx/app/classes"
cp -r "$build/test-classes" "$pack/ctx/app/test-classes"
cp "$build/provenance.env" "$pack/ctx/app/"
cp_rel="/app/test-classes:/app/classes"
IFS=: read -ra entries < "$build/classpath.snapshot" || true
n=0
for e in "${entries[@]}"; do
  [ -n "$e" ] || continue
  n=$((n + 1))
  name=$(printf '%03d-%s' "$n" "$(basename "$e")")
  cp -r "$e" "$pack/ctx/app/lib/$name"
  cp_rel="$cp_rel:/app/lib/$name"
done
printf '%s\n' "$cp_rel" > "$pack/ctx/app/classpath"
cp "$here/Dockerfile" "$here/bench-entry" "$pack/ctx/"
cp "$here/run.sh" "$here/README.md" "$pack/"
cp "$root"/bench/configs/*.json "$pack/configs/"
tar -C "$root/bench/build" -cf "$pack.tar" "bench-$stamp"
du -sh "$pack.tar"
echo "packed $pack.tar ($(cat "$build/provenance.env" | tr '\n' ' '))"
