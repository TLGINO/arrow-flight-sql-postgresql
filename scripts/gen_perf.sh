#!/bin/bash
set -e

BG=false
[ "$1" = "--bg" ] && BG=true

run() {
    python3 substrait_test/test_substrait.py --run 7 --benchmark tpch 01 03 05 06 09 18 --sf 1
    python3 substrait_test/test_substrait.py --run 7 --benchmark tpch 01 03 05 06 09 18 --sf 5
    python3 substrait_test/test_substrait.py --run 7 --benchmark tpch 01 03 05 06 09 18 --sf 10
    python3 substrait_test/test_substrait.py --run 7 --benchmark tpch 01 03 05 06 09 18 --sf 15
    python3 substrait_test/test_substrait.py --run 7 --benchmark tpch 01 03 05 06 09 18 --sf 20
}

if $BG; then
    LOG="gen_perf_$(date +%Y%m%d_%H%M%S).log"
    nohup bash -c "$(declare -f run); run" > "$LOG" 2>&1 &
    echo "Backgrounded (pid=$!), output -> $LOG"
else
    run
fi
