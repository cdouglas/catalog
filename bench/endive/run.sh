#!/usr/bin/env bash
# Runs endive (~/work/catalog/endive, from its source) on the configurations gen.py wrote, from
# bench/results/endive, where endive writes experiments/<label>-<hash>/<seed>/results.parquet.
# A venv of its simulation's dependencies is made there on first use (uv), so endive's own tree
# is left as it is.
#
#   bench/endive/run.sh [bench/results/endive/configs/*.toml]
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
endive=${ENDIVE:-$root/../endive}
out="$root/bench/results/endive"
configs=()
for c in "$@"; do configs+=("$(readlink -f "$c")"); done
[ ${#configs[@]} -gt 0 ] || configs=("$out"/configs/*.toml)
cd "$out"
if [ ! -x .venv/bin/python ]; then
  uv venv -q .venv --python 3.12
  uv pip install -q --python .venv/bin/python numpy==2.2.0 pandas==2.2.3 pyarrow==22.0.0 simpy==4.1.1
fi
git -C "$endive" rev-parse HEAD > endive-version.txt
for c in "${configs[@]}"; do
  echo "== $c"
  PYTHONPATH="$endive" .venv/bin/python -m endive.main "$c" --yes > /dev/null
done
