#!/usr/bin/env bash
# Runs the multi-writer contention driver (org.apache.iceberg.io.contention.ContentionBench) over
# configuration files, writing each run under bench/results/<label>-<hash>/<UTC start>/.
#
#   bench/run.sh [--heap 4g] [--out DIR] [--dry-run] bench/configs/<campaign>.json...
#
# It builds once, snapshots the compiled classes under bench/results/builds/ (so a rebuild during a
# campaign changes nothing it runs), and records the commit (and whether the tree was dirty) in
# each run's manifest. Results are read by labbook (lab/).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/.." && pwd)
heap=4g
out="$here/results"
args=()
while [ $# -gt 0 ]; do
  case "$1" in
    --heap) heap="$2"; shift 2 ;;
    --out) out="$2"; shift 2 ;;
    --dry-run) args+=(--dry-run); shift ;;
    *) args+=("$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"); shift ;;
  esac
done

cd "$root"
git_sha=$(git rev-parse HEAD)
dirty=false
[ -n "$(git status --porcelain -- . ':!bench/results' ':!lab')" ] && dirty=true
stamp=$(date -u +%Y%m%dT%H%M%SZ)
build="$out/builds/$stamp-${git_sha:0:12}$([ "$dirty" = true ] && echo -dirty)"
mkdir -p "$build/deps"

MAVEN_OPTS=${MAVEN_OPTS:--Xmx768m} mvn -q -o test-compile dependency:build-classpath \
  -pl iceberg-fileio-catalog -am -Dmdep.includeScope=test -Dmdep.outputFile="$build/classpath.txt"
cp -r iceberg-fileio-catalog/target/classes "$build/classes"
cp -r iceberg-fileio-catalog/target/test-classes "$build/test-classes"
# reactor modules on the classpath are their target directories: snapshot those too
cp_snap=""
IFS=: read -ra entries < "$build/classpath.txt" || true
for e in "${entries[@]}"; do
  case "$e" in
    "$root"/*/target/*)
      dest="$build/deps/$(echo "${e#"$root"/}" | tr / _)"
      [ -e "$dest" ] || cp -r "$e" "$dest"
      cp_snap="$cp_snap:$dest" ;;
    *) cp_snap="$cp_snap:$e" ;;
  esac
done
echo "build: $build ($git_sha dirty=$dirty)"

export FILEIO_GIT="$git_sha" FILEIO_DIRTY="$dirty"
exec java -Xmx"$heap" -XX:+UseG1GC \
  -Diceberg.worker.num-threads=256 \
  -Dorg.slf4j.simpleLogger.defaultLogLevel=error \
  -cp "$build/test-classes:$build/classes$cp_snap" \
  org.apache.iceberg.io.contention.ContentionBench --out "$out" "${args[@]}"
