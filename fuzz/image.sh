#!/bin/bash
# Makes the fuzz image's build context on the dev box (Docker, rootful or rootless), builds the
# image here, and packs the context with a bundle of the repository and run.sh, for another host
# to build and run. See README.md.
#
#   fuzz/image.sh <name>=<maven repo> ...   e.g. p4=$HOME/.m2/repository
#
# Each <name>=<repo> bakes in the fork jars (1.12.0-atomicio-SNAPSHOT) of that local Maven
# repository as fork build <name>; the last, which HEAD must build with, also seeds the base
# repository, which an online build of this checkout's HEAD fills with every other dependency.
# Env: IMAGE (default ce-fuzz:latest), PACK=0 to skip packing. The pack is
# fuzz/build/ce-fuzz-<date>.tar: ctx/ (the build context), fileio.bundle (every local branch),
# run.sh, README.md.
set -euo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/.." && pwd)
IMAGE=${IMAGE:-ce-fuzz:latest}
BASE_IMAGE=$(sed -n 's/^FROM //p' "$HERE/Dockerfile")
VERSION=1.12.0-atomicio-SNAPSHOT
# the fork's modules the build resolves, and their own dependencies among the fork's
MODULES="api aws azure bundled-guava common core gcp data parquet orc bom"
[ $# -ge 1 ] || { sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }

CTX=$HERE/build/ctx
rm -rf "$CTX"; mkdir -p "$CTX/base" "$CTX/forks" "$CTX/seed"

forkjars() { # <repo> <dest>: the fork's jars and poms, not its sources or javadoc
  for m in $MODULES; do
    d=$1/org/apache/iceberg/iceberg-$m/$VERSION
    [ -d "$d" ] || continue
    mkdir -p "$2/org/apache/iceberg/iceberg-$m/$VERSION"
    find "$d" -maxdepth 1 -type f ! -name '*-sources.jar' ! -name '*-javadoc.jar' \
      -exec cp {} "$2/org/apache/iceberg/iceberg-$m/$VERSION/" \;
  done
  [ -f "$2/org/apache/iceberg/iceberg-core/$VERSION/iceberg-core-$VERSION.jar" ] \
    || { echo "image.sh: no iceberg-core $VERSION in $1" >&2; exit 1; }
}

: > "$CTX/provenance.txt"
for spec in "$@"; do
  name=${spec%%=*}; repo=${spec#*=}
  forkjars "$repo" "$CTX/forks/$name"
  commit=$(unzip -p "$CTX/forks/$name/org/apache/iceberg/iceberg-core/$VERSION/iceberg-core-$VERSION.jar" \
    iceberg-build.properties 2>/dev/null | sed -n 's/^git.commit.id=//p')
  echo "fork $name: iceberg $VERSION at ${commit:-unknown}, from $repo" | tee -a "$CTX/provenance.txt"
done

# The base: an online build and a test run of HEAD in the base image, on the last fork's jars,
# fill it with every plugin, dependency and surefire provider; then the fork's jars come out.
last=${!#}; last=${last%%=*}
cp -r "$CTX/forks/$last/." "$CTX/base/"
git -C "$ROOT" archive HEAD | tar -x -C "$CTX/seed"
# as the invoking user, so that rootful Docker leaves no root-owned files behind
docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp -e MAVEN_CONFIG=/tmp/.m2 \
  -v "$CTX/base:/m2" -v "$CTX/seed:/src" -w /src "$BASE_IMAGE" \
  mvn -B -q -Dmaven.repo.local=/m2 test -DargLine=-Xmx1g -Dsurefire.failIfNoSpecifiedTests=false \
    -Dtest='TestFootprints,TestInMemorySharedLogContract*'
rm -rf "$CTX"/base/org/apache/iceberg/iceberg-*/$VERSION "$CTX/seed"
echo "base: $(du -sh "$CTX/base" | cut -f1), seeded by $(git -C "$ROOT" rev-parse --short HEAD)" \
  | tee -a "$CTX/provenance.txt"

cp -r "$HERE/Dockerfile" "$HERE/ce-fuzz" "$HERE/mutants" "$CTX/"
docker build -t "$IMAGE" "$CTX"
docker run --rm "$IMAGE" status > /dev/null

[ "${PACK:-1}" = 1 ] || exit 0
PACKDIR=$HERE/build/ce-fuzz-$(date +%Y%m%d)
rm -rf "$PACKDIR"; mkdir -p "$PACKDIR"
cp -r "$CTX" "$PACKDIR/ctx"
git -C "$ROOT" bundle create "$PACKDIR/fileio.bundle" --branches
cp "$HERE/run.sh" "$HERE/README.md" "$PACKDIR/"
tar -C "$HERE/build" -cf "$PACKDIR.tar" "$(basename "$PACKDIR")"
rm -rf "$PACKDIR"
ls -la "$PACKDIR.tar"
