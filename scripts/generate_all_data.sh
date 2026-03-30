#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"

declare -A SF_MAP=([0.01]=sf001 [0.1]=sf010 [1]=sf100 [5]=sf500 [10]=sf1000 [15]=sf1500 [20]=sf2000)

for sf in 0.01 0.1 1 5 10 15 20; do
  dir="$ROOT/substrait_test/tpch/data/${SF_MAP[$sf]}"
  [[ -d "$dir" && $(ls "$dir"/*.tbl 2>/dev/null | wc -l) -gt 0 ]] \
    && echo "SKIP tpch sf=$sf" && continue
  ../TPC-H/generate.sh -s "$sf" -o "$dir"
  # Strip trailing | from dbgen output
  for f in "$dir"/*.tbl; do sed -i 's/|$//' "$f"; done
done

for sf in 0.01 0.1 1 5 10 15 20; do
  dir="$ROOT/substrait_test/tpcds/data/${SF_MAP[$sf]}"
  [[ -d "$dir" && $(ls "$dir"/*.dat 2>/dev/null | wc -l) -gt 0 ]] \
    && echo "SKIP tpcds sf=$sf" && continue
  ../TPC-DS/generate.sh -s "$sf" -o "$dir"
  # Strip trailing | from dsdgen output
  for f in "$dir"/*.dat; do sed -i 's/|$//' "$f"; done
done
