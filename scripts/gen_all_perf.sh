#!/bin/bash
set -e

BG=false
[ "$1" = "--bg" ] && BG=true

run() {
    python3 substrait_test/test_substrait.py --run 7 --benchmark tpch --sf 1
    python3 substrait_test/test_substrait.py --run 7 --benchmark tpch --sf 5
    python3 substrait_test/test_substrait.py --run 7 --benchmark tpch --sf 10
    python3 substrait_test/test_substrait.py --run 7 --benchmark tpch --sf 15
    python3 substrait_test/test_substrait.py --run 7 --benchmark tpch --sf 20
}

if $BG; then
    LOG="gen_all_perf_$(date +%Y%m%d_%H%M%S).log"
    nohup bash -c "$(declare -f run); run" > "$LOG" 2>&1 &
    echo "Backgrounded (pid=$!), output -> $LOG"
else
    run
fi
