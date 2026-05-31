#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership. The ASF
# licenses this file to you under the Apache License, Version 2.0.
#
# Generate the catalog-footprint samples for a given seed and render the report.
#
#   docs/sample/gen.sh <seed>          # seed is decimal or 0x-hex
#
# Output (not checked in -- regenerable from the seed) lands under:
#
#   docs/sample/figures/<seed>/
#     <example>/<mode>/files/...   byte-exact catalog artifacts
#     <example>/{manifest.json,sizes.csv,config.json}
#     figures/<example>_{footprint,per-commit}.png
#     REPORT.md                    tables + figures for all examples
set -euo pipefail

SEED="${1:-}"
if [[ -z "$SEED" ]]; then
  echo "usage: $0 <seed>   (decimal or 0x-hex, e.g. 42 or 0xC0FFEE)" >&2
  exit 2
fi

# Repo root (fileio-catalog) is two levels up from this script.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT="docs/sample/figures/$SEED"

cd "$ROOT"
echo ">> generating samples for seed $SEED into $OUT"
mvn -q test-compile exec:java \
  -Dexec.mainClass=org.apache.iceberg.io.sample.SampleGenerator \
  -Dexec.classpathScope=test \
  -Dexec.args="--seed $SEED --out $OUT"

echo ">> rendering tables + figures"
python3 docs/sample/analyze.py --root "$OUT"

echo ">> done: open $OUT/REPORT.md"
