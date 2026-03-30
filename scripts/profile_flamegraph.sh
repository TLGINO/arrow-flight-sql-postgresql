#!/bin/bash
set -e

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
FLAMEGRAPH_DIR="/tmp/FlameGraph"
PERF_DATA="/tmp/perf_tpch.data"
OUT_DIR="$REPO_DIR/substrait_test/flamegraph"
cd "$REPO_DIR"

# Clone FlameGraph tools if missing
if [ ! -d "$FLAMEGRAPH_DIR" ]; then
    echo "Cloning FlameGraph tools..."
    git clone --depth 1 https://github.com/brendangregg/FlameGraph "$FLAMEGRAPH_DIR"
fi

mkdir -p "$OUT_DIR"

# Get all postgres PIDs
PIDS=$(pgrep -d, postgres)
if [ -z "$PIDS" ]; then
    echo "ERROR: no postgres processes found"
    exit 1
fi
echo "Profiling postgres PIDs: $PIDS"

# Start perf recording in background
rm -f "$PERF_DATA"
perf record -g --call-graph dwarf,32768 -F 99 -p "$PIDS" -o "$PERF_DATA" &
PERF_PID=$!
echo "perf record started (pid=$PERF_PID)"

# Run substrait-only TPC-H SF1
echo "Running TPC-H SF1 substrait..."
python3 substrait_test/test_substrait.py --benchmark tpch --sf 10 --run 1
echo "Test complete"

# Stop perf
kill -INT "$PERF_PID" 2>/dev/null || true
wait "$PERF_PID" 2>/dev/null || true
echo "perf stopped"

# Generate full flamegraph
echo "Generating flamegraphs..."
perf script -i "$PERF_DATA" > /tmp/perf_tpch.script

"$FLAMEGRAPH_DIR/stackcollapse-perf.pl" /tmp/perf_tpch.script | \
    "$FLAMEGRAPH_DIR/flamegraph.pl" \
    --title "Arrow Flight SQL Adapter - TPC-H SF1 (all)" \
    --width 1600 \
    > "$OUT_DIR/flamegraph_full.svg"
echo "  -> $OUT_DIR/flamegraph_full.svg"

# Adapter-only: filter stacks containing afs/arrow_flight_sql symbols
"$FLAMEGRAPH_DIR/stackcollapse-perf.pl" /tmp/perf_tpch.script | \
    grep -E 'arrow_flight_sql|afs_|SharedRingBuffer|FlightSql|ArrowFlightSql' | \
    "$FLAMEGRAPH_DIR/flamegraph.pl" \
    --title "Arrow Flight SQL Adapter - TPC-H SF1 (adapter only)" \
    --width 1600 \
    > "$OUT_DIR/flamegraph_adapter.svg"
echo "  -> $OUT_DIR/flamegraph_adapter.svg"

rm -f /tmp/perf_tpch.script

echo "Done. Open SVGs in a browser for interactive flamegraphs."
echo "Raw perf data: $PERF_DATA"
