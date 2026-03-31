#!/usr/bin/env python3
"""
Substrait integration tests — TPC-H and TPC-DS.

Run:   python3 substrait_test/test_substrait.py [--run N] [--explain N] [--sf SF] [query_labels...]

--run N      bitmask: 4=pgsql 2=arrow 1=substrait  (default 5 = pgsql+substrait)
--explain N  bitmask: 4=pgsql 2=arrow 1=substrait  (default if bare: 7=all)

Common: --run 1  substrait only | --run 5  pgsql+substrait | --run 7  all three
"""

import argparse
import csv
import decimal
import os
import subprocess
import sys
import concurrent.futures
import time

import pyarrow.ipc as ipc

# ============================================================
# Config
# ============================================================

PGUSER = os.environ.get("PGUSER", os.environ.get("USER", "postgres"))
PGHOST = os.environ.get("PGHOST", "127.0.0.1")
PGDATABASE = os.environ.get("PGDATABASE", "postgres")

PSQL_CMD = ["psql", "-h", PGHOST, "-U", PGUSER, "-d", PGDATABASE,
            "-v", "ON_ERROR_STOP=1"]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

SF_DIR_MAP = {"1": "sf100", "5": "sf500", "10": "sf1000",
              "15": "sf1500", "20": "sf2000"}

# Features the converter currently handles.
IMPLEMENTED = {"read", "aggregate", "project", "filter", "sort",
               "cast", "interval_day", "interval_year", "cross",
               "like", "extract", "if_then", "fetch", "join",
               "subquery"}

# ============================================================
# Benchmark configs
# ============================================================

TPCH_TABLES = ["customer", "lineitem", "nation", "orders",
               "part", "partsupp", "region", "supplier"]

TPCH_REQUIRES = {
    "01": {"aggregate", "filter", "project", "read", "sort", "cast",
           "interval_day"},
    "02": {"aggregate", "filter", "project", "read", "sort", "fetch",
           "cross", "cast", "subquery", "like"},
    "03": {"aggregate", "filter", "project", "read", "sort", "fetch",
           "cross", "cast"},
    "04": {"aggregate", "filter", "project", "read", "sort", "subquery",
           "interval_year"},
    "05": {"aggregate", "filter", "project", "read", "sort", "cross", "cast"},
    "06": {"aggregate", "filter", "project", "read", "interval_year"},
    "07": {"aggregate", "filter", "project", "read", "sort", "cross", "cast",
           "extract"},
    "08": {"aggregate", "filter", "project", "read", "sort", "cross",
           "cast", "if_then", "extract"},
    "09": {"aggregate", "filter", "project", "read", "sort", "cross", "cast",
           "like", "extract"},
    "10": {"aggregate", "filter", "project", "read", "sort", "fetch",
           "cross", "cast"},
    "11": {"aggregate", "filter", "project", "read", "sort", "cross",
           "cast", "subquery"},
    "12": {"aggregate", "filter", "project", "read", "sort", "cross",
           "cast", "if_then"},
    "13": {"aggregate", "filter", "project", "read", "sort", "join", "cast",
           "like"},
    "14": {"aggregate", "filter", "project", "read", "cross", "cast",
           "if_then", "like"},
    "15": {"aggregate", "filter", "project", "read", "sort", "cross",
           "cast", "subquery"},
    "16": {"aggregate", "filter", "project", "read", "sort", "cross",
           "cast", "subquery", "like"},
    "17": {"aggregate", "filter", "project", "read", "cross", "cast",
           "subquery"},
    "18": {"aggregate", "filter", "project", "read", "sort", "fetch",
           "cross", "subquery"},
    "19": {"aggregate", "filter", "project", "read", "cross", "cast"},
    "20": {"aggregate", "filter", "project", "read", "sort", "cross",
           "cast", "subquery", "like"},
    "21": {"aggregate", "filter", "project", "read", "sort", "fetch",
           "cross", "subquery"},
    "22": {"aggregate", "filter", "project", "read", "sort", "subquery"},
}

LINEITEM_ROWS = {"1": 6001215, "5": 29999795, "10": 59986052,
                 "15": 89989977, "20": 119994608}

TPCDS_TABLES = [
    "dbgen_version", "customer_address", "customer_demographics",
    "date_dim", "warehouse", "ship_mode", "time_dim", "reason",
    "income_band", "item", "store", "call_center", "customer",
    "web_site", "store_returns", "household_demographics", "web_page",
    "promotion", "catalog_page", "inventory", "catalog_returns",
    "web_returns", "web_sales", "catalog_sales", "store_sales",
]

TPCDS_SKIP = set()

# Queries skipped due to isthmus plan-generation bugs
TPCDS_ISTHMUS_SKIP = {
    "17": "isthmus: stddev_samp NULL",
    "35": "isthmus: stddev_samp NULL",
}

STORE_SALES_ROWS = {"1": 2880404, "5": 14401460, "10": 28800991,
                    "15": 43201221, "20": 57603868}

BENCHMARKS = {
    "tpch": {
        "dir": os.path.join(SCRIPT_DIR, "tpch"),
        "schema": "tpch",
        "tables": TPCH_TABLES,
        "data_ext": ".tbl",
        "requires": TPCH_REQUIRES,
        "skip": set(),
        "isthmus_skip": {},
        "check_table": "lineitem",
        "check_col": "l_orderkey",
        "check_rows": LINEITEM_ROWS,
    },
    "tpcds": {
        "dir": os.path.join(SCRIPT_DIR, "tpcds"),
        "schema": "tpcds",
        "tables": TPCDS_TABLES,
        "data_ext": ".dat",
        "requires": {},
        "skip": TPCDS_SKIP,
        "isthmus_skip": TPCDS_ISTHMUS_SKIP,
        "check_table": "store_sales",
        "check_col": "ss_item_sk",
        "check_rows": STORE_SALES_ROWS,
    },
}


def _discover_queries(bench):
    """Discover available query labels from plan files."""
    plans_dir = os.path.join(bench["dir"], "plans")
    labels = []
    for f in sorted(os.listdir(plans_dir)):
        if f.endswith(".proto"):
            labels.append(f[:-6])  # e.g. "01", "14a", "14b"
    return labels


# ============================================================
# Value parsing / normalization
# ============================================================

def parse_csv_value(v):
    v = v.strip()
    if v == "" or v == "\\N":
        return None
    try:
        return int(v)
    except ValueError:
        try:
            return round(float(v), 4)
        except ValueError:
            return v


def normalize_value(v):
    if isinstance(v, float):
        return round(v, 4)
    if isinstance(v, decimal.Decimal):
        return round(float(v), 4)
    if isinstance(v, str):
        v = v.strip()
        try:
            return int(v)
        except ValueError:
            try:
                return round(float(v), 4)
            except ValueError:
                return v
    if hasattr(v, 'isoformat'):
        return v.isoformat()
    return v


def table_to_rows(table):
    rows = []
    for i in range(table.num_rows):
        row = tuple(normalize_value(table.column(c)[i].as_py())
                    for c in range(table.num_columns))
        rows.append(row)
    return rows


# ============================================================
# Helpers — PostgreSQL
# ============================================================

def psql_run(sql):
    r = subprocess.run(PSQL_CMD, input=sql.encode(), capture_output=True)
    if r.returncode != 0:
        raise RuntimeError(f"psql error:\n{r.stderr.decode()}")
    return r.stdout.decode()


def psql_csv(sql, search_path=None):
    sep = "\x01"
    if search_path:
        sql = f"SET search_path TO {search_path};\n{sql}"
    cmd = PSQL_CMD + ["-t", "-A", "-q", "-F", sep]
    r = subprocess.run(cmd, input=sql.encode(), capture_output=True)
    if r.returncode != 0:
        raise RuntimeError(f"psql error:\n{r.stderr.decode()}")
    raw = r.stdout.decode()
    if not raw:
        return []
    # Strip exactly one trailing newline (psql always appends one);
    # preserve blank lines — they represent all-NULL rows.
    if raw.endswith('\n'):
        raw = raw[:-1]
    rows = []
    for line in raw.split("\n"):
        rows.append(tuple(parse_csv_value(v) for v in line.split(sep)))
    return rows


def _psql_json(sql):
    """Run SQL via psql with -tA flags, return raw JSON output."""
    cmd = PSQL_CMD + ["-t", "-A"]
    r = subprocess.run(cmd, input=sql.encode(), capture_output=True)
    if r.returncode != 0:
        raise RuntimeError(f"psql error:\n{r.stderr.decode()}")
    out = r.stdout.decode().strip()
    idx = out.find("[")
    return out[idx:] if idx >= 0 else out


# ============================================================
# Helpers — Flight SQL (single persistent session)
# ============================================================

_flight_helper = None


def _get_flight_helper():
    global _flight_helper
    if _flight_helper is None or _flight_helper.poll() is not None:
        helper = os.path.join(SCRIPT_DIR, "_flight_helper.py")
        _flight_helper = subprocess.Popen(
            [sys.executable, helper],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
    return _flight_helper


def _stop_flight_helper():
    global _flight_helper
    if _flight_helper is not None and _flight_helper.poll() is None:
        _flight_helper.stdin.write(b"\n")
        _flight_helper.stdin.flush()
        _flight_helper.wait(timeout=5)
        _flight_helper = None


def _read_exact(stream, n):
    """Read exactly n bytes from a binary stream."""
    buf = bytearray()
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk:
            raise RuntimeError("flight helper: unexpected EOF")
        buf.extend(chunk)
    return bytes(buf)


def _flight_exec(proc, header, payload):
    """Send command + payload, read Arrow IPC response inline."""
    proc.stdin.write(f"{header} {len(payload)}\n".encode())
    proc.stdin.write(payload)
    proc.stdin.flush()

    resp_line = proc.stdout.readline().decode().strip()
    if resp_line.startswith("ERR "):
        raise RuntimeError(resp_line[4:])
    if not resp_line.startswith("OK "):
        raise RuntimeError(f"unexpected response: {resp_line}")
    nbytes = int(resp_line[3:])
    ipc_data = _read_exact(proc.stdout, nbytes)
    reader = ipc.open_stream(ipc_data)
    return reader.read_all()


def flight_exec_sql(sql):
    proc = _get_flight_helper()
    return _flight_exec(proc, "SQL", sql.encode("utf-8"))


def flight_exec_substrait(plan_bytes):
    proc = _get_flight_helper()
    return _flight_exec(proc, "PLAN", plan_bytes)


# ============================================================
# Ground truth & Substrait execution
# ============================================================

def ground_truth_pg(bench, qlabel, arrow=False):
    pgsql_dir = os.path.join(bench["dir"], "queries", "pgsql")
    query_file = os.path.join(pgsql_dir, f"{qlabel}.sql")
    with open(query_file) as f:
        sql = f.read().strip()
    if sql.endswith(";"):
        sql = sql[:-1]
    if arrow:
        return table_to_rows(flight_exec_sql(sql))
    return psql_csv(sql, search_path=bench["schema"])


def substrait_pg(plan_bytes):
    return table_to_rows(flight_exec_substrait(plan_bytes))


# ============================================================
# Result comparison
# ============================================================

def rows_equal(expected, actual, ordered=False):
    if not ordered:
        expected = sorted(expected)
        actual = sorted(actual)
    if expected == actual:
        return True, ""
    lines = [f"row count: expected={len(expected)}, actual={len(actual)}"]
    for i, (e, a) in enumerate(zip(expected, actual)):
        if e != a:
            lines.append(f"first diff at row {i}: expected={e}, actual={a}")
            break
    return False, "\n      ".join(lines)


# ============================================================
# PG data loading (generic)
# ============================================================

_loaded = {}


def _already_loaded(bench, sf):
    schema = bench["schema"]
    check_table = bench["check_table"]
    check_rows = bench["check_rows"]
    try:
        rows = psql_csv(f'SELECT count(*) FROM {schema}.{check_table}')
        return rows and rows[0][0] == check_rows.get(sf, -1)
    except RuntimeError:
        return False


def _ensure_search_path(bench):
    """Set database search_path for the benchmark's schema.
    The Flight SQL adapter (PG background worker) picks this up on new connections.
    If the adapter is already running, PG must be restarted for the change to take effect.
    """
    schema = bench["schema"]
    psql_run(f"ALTER DATABASE postgres SET search_path TO public, {schema};")


def _create_indexes_parallel(post_sql, schema):
    """Run each CREATE INDEX statement in parallel."""
    with open(post_sql) as f:
        stmts = []
        for line in f:
            line = line.strip()
            if line.upper().startswith("CREATE INDEX") or line.upper().startswith("CREATE UNIQUE INDEX"):
                stmts.append(line)
            elif line.upper().startswith("SET "):
                # Apply SET statements (e.g. maintenance_work_mem) per-session
                pass  # will prepend to each worker
    # Prepend maintenance_work_mem to each statement
    prefix = "SET maintenance_work_mem = '1GB'; "
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futs = [pool.submit(psql_run, prefix + stmt) for stmt in stmts]
        for fut in concurrent.futures.as_completed(futs):
            fut.result()
    # Run non-index statements (ALTER DATABASE etc.) serially
    with open(post_sql) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("--") and not line.upper().startswith(("CREATE INDEX", "CREATE UNIQUE INDEX", "SET ")):
                psql_run(line)


def _copy_table(schema, ext, data_dir, table):
    """COPY a single table — called from thread pool."""
    data_file = os.path.abspath(os.path.join(data_dir, f"{table}{ext}"))
    if not os.path.exists(data_file):
        raise RuntimeError(f"Missing data file: {data_file}")
    copy_sql = (f"COPY {schema}.{table} FROM '{data_file}' "
                f"WITH (FORMAT text, DELIMITER '|', NULL '');")
    psql_run(copy_sql)


def load_data(bench, sf):
    key = bench["schema"]
    if key in _loaded:
        return

    _ensure_search_path(bench)

    if _already_loaded(bench, sf):
        print(f"  {key} sf={sf} already loaded (cached)")
        _loaded[key] = True
        return

    t0 = time.time()
    print(f"  Loading {key} sf={sf} into PostgreSQL ... ", end="", flush=True)

    sf_dir = SF_DIR_MAP[sf]
    data_dir = os.path.join(bench["dir"], "data", sf_dir)
    init_sql = os.path.join(bench["dir"], "pg_init.sql")
    post_sql = os.path.join(bench["dir"], "pg_post.sql")

    # 1. Schema + tables (no indexes)
    cmd = PSQL_CMD + ["-f", init_sql]
    r = subprocess.run(cmd, capture_output=True)
    if r.returncode != 0:
        raise RuntimeError(f"pg_init.sql failed:\n{r.stderr.decode()}")

    schema = bench["schema"]
    ext = bench["data_ext"]

    # 2. Parallel COPY (tables are UNLOGGED — no WAL overhead)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futs = {pool.submit(_copy_table, schema, ext, data_dir, t): t
                for t in bench["tables"]}
        for fut in concurrent.futures.as_completed(futs):
            fut.result()  # propagate exceptions

    # 3. Create indexes in parallel
    if os.path.exists(post_sql):
        _create_indexes_parallel(post_sql, schema)

    # 4. Parallel ANALYZE
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futs = [pool.submit(psql_run, f"ANALYZE {schema}.{table};")
                for table in bench["tables"]]
        for fut in concurrent.futures.as_completed(futs):
            fut.result()

    _loaded[key] = True
    print(f"done ({time.time() - t0:.1f}s)")


# ============================================================
# Test runner (generic)
# ============================================================

_results = {"pass": 0, "fail": 0, "skip": 0, "error": 0}
_timing_log = []  # list of (qlabel, rows, status, pg_time, arrow_time, sub_time)


def _has_order_by(bench, qlabel):
    pgsql_dir = os.path.join(bench["dir"], "queries", "pgsql")
    query_file = os.path.join(pgsql_dir, f"{qlabel}.sql")
    with open(query_file) as f:
        return "order by" in f.read().lower()


def run_query(bench, qlabel, run=5, explain=0, explain_dir=None):
    """Run a query.  run/explain bitmask: 4=pgsql 2=arrow 1=substrait."""
    plans_dir = os.path.join(bench["dir"], "plans")
    plan_file = os.path.join(plans_dir, f"{qlabel}.proto")
    with open(plan_file, "rb") as f:
        plan_bytes = f.read()

    ordered = _has_order_by(bench, qlabel)

    # Clean stale adapter explain files
    if explain & 3:
        afs_dir = os.environ.get("AFS_EXPLAIN_DIR", "/tmp/afs_explain")
        for stale in ("select_sql.json", "latest.json"):
            p = os.path.join(afs_dir, stale)
            if os.path.exists(p):
                os.remove(p)

    pg_time_s = arrow_time_s = substrait_time_s = 0.0
    expected = actual = None
    parts = []  # printed so far

    # -- pgsql ground truth (bit 4) --
    if run & 4:
        t = time.monotonic()
        expected = ground_truth_pg(bench, qlabel, arrow=False)
        pg_time_s = time.monotonic() - t
        parts.append(f"pg={pg_time_s:.3f}s ({len(expected)} rows)")

    # -- arrow ground truth (bit 2) --
    if run & 2:
        t = time.monotonic()
        arrow_result = ground_truth_pg(bench, qlabel, arrow=True)
        arrow_time_s = time.monotonic() - t
        if expected is None:
            expected = arrow_result
        parts.append(f"arrow={arrow_time_s:.3f}s")

    if parts:
        print(f"  {', '.join(parts)}", end="", flush=True)

    # -- pgsql EXPLAIN ANALYZE (bit 4 of explain) --
    explain_parts = []
    if explain & 4:
        pgsql_dir = os.path.join(bench["dir"], "queries", "pgsql")
        query_file = os.path.join(pgsql_dir, f"{qlabel}.sql")
        with open(query_file) as f:
            sql = f.read().strip().rstrip(";")
        explain_out = _psql_json(
            f"SET search_path TO {bench['schema']};\n"
            f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql};")
        explain_parts.append(("pgsql", explain_out))

    # -- substrait (bit 1) --
    if run & 1:
        sep = ", " if parts else "  "
        print(f"{sep}substrait...", end=" ", flush=True)

        try:
            t = time.monotonic()
            actual = substrait_pg(plan_bytes)
            substrait_time_s = time.monotonic() - t
        except Exception as e:
            substrait_time_s = time.monotonic() - t
            print(f"FAIL (adapter error: {e}) sub={substrait_time_s:.3f}s")
            _results["fail"] += 1
            _timing_log.append((qlabel, 0, "FAIL",
                                pg_time_s, arrow_time_s or None,
                                substrait_time_s))
            return

    # -- collect adapter explain outputs --
    if explain & 3:
        afs_dir = os.environ.get("AFS_EXPLAIN_DIR", "/tmp/afs_explain")
        if explain & 2:
            p = os.path.join(afs_dir, "select_sql.json")
            if os.path.exists(p):
                with open(p) as ef:
                    explain_parts.append(("arrow", ef.read()))
        if explain & 1:
            p = os.path.join(afs_dir, "latest.json")
            if os.path.exists(p):
                with open(p) as ef:
                    explain_parts.append(("substrait", ef.read()))

    if explain_parts:
        if explain_dir:
            for label, text in explain_parts:
                fp = os.path.join(explain_dir, f"Q{qlabel}_{label}.json")
                with open(fp, "w") as f:
                    f.write(text)
            print(", ".join(l for l, _ in explain_parts))

    # -- result comparison + reporting --
    nrows = len(actual) if actual is not None else (len(expected) if expected else 0)
    if expected is not None and actual is not None:
        ok, diff = rows_equal(expected, actual, ordered=ordered)
        status = "PASS" if ok else "FAIL"
        _results["pass" if ok else "fail"] += 1
        detail = f"sub={substrait_time_s:.3f}s diff={substrait_time_s - pg_time_s:+.3f}s"
        if ok:
            print(f"PASS {detail}")
        else:
            print(f"FAIL ({diff}) {detail}")
    else:
        status = "PASS"
        _results["pass"] += 1
        times = []
        if run & 4:
            times.append(f"pg={pg_time_s:.3f}s")
        if run & 1:
            times.append(f"sub={substrait_time_s:.3f}s")
        print(f"OK {' '.join(times)} ({nrows} rows)")

    _timing_log.append((qlabel, nrows, status, pg_time_s,
                        arrow_time_s if (run & 2) else None,
                        substrait_time_s))


def run_benchmark(bench, sf, queries=None, run=5, explain=0,
                  explain_dir=None):
    name = bench["schema"].upper()
    requires = bench["requires"]
    skip = bench["skip"]
    isthmus_skip = bench.get("isthmus_skip", {})

    print(f"\n{name}")
    print("-" * 60)

    if queries is None:
        queries = _discover_queries(bench)

    any_runnable = any(
        q not in skip and q not in isthmus_skip
        and not (requires.get(q, set()) - IMPLEMENTED)
        for q in queries
    )
    if any_runnable:
        load_data(bench, sf)

    for qlabel in queries:
        print(f"\n{name} Q{qlabel}")

        if qlabel in skip:
            print(f"  SKIP (no plan)")
            _results["skip"] += 1
            continue

        if qlabel in isthmus_skip:
            print(f"  SKIP ({isthmus_skip[qlabel]})")
            _results["skip"] += 1
            continue

        required = requires.get(qlabel, set())
        missing = required - IMPLEMENTED
        if missing:
            print(f"  SKIP (need {missing})")
            _results["skip"] += 1
            continue

        try:
            run_query(bench, qlabel, run=run, explain=explain,
                      explain_dir=explain_dir)
        except Exception as e:
            print(f"  ERROR: {e}")
            _results["error"] += 1


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Substrait integration tests")
    parser.add_argument("--benchmark", default="tpch",
                        choices=BENCHMARKS.keys(),
                        help="Benchmark to run (default: tpch)")
    parser.add_argument("--sf", default="1",
                        choices=["1", "5", "10", "15", "20"],
                        help="Scale factor (default: 1)")
    parser.add_argument("--run", type=int, default=None,
                        help="Bitmask: 4=pgsql 2=arrow 1=substrait (default: 5=pgsql+substrait)")
    parser.add_argument("--explain", type=int, nargs="?", const=7, default=0,
                        help="Bitmask: 4=pgsql 2=arrow 1=substrait (sets --run when --run not given)")
    parser.add_argument("queries", nargs="*", type=str,
                        help="query labels to run (default: all)")
    args = parser.parse_args()

    bench = BENCHMARKS[args.benchmark]
    queries = args.queries or None

    if args.run is not None:
        run = args.run
    elif args.explain:
        run = args.explain
    else:
        run = 5

    if args.explain & 3:
        psql_run("ALTER SYSTEM SET arrow_flight_sql.explain = 2; SELECT pg_reload_conf();")

    _explain_dir = None
    if args.explain:
        _explain_dir = os.path.join(SCRIPT_DIR, "explain",
                                    f"{args.benchmark}_sf{args.sf}")
        os.makedirs(_explain_dir, exist_ok=True)

    labels = []
    if run & 4: labels.append("pgsql")
    if run & 2: labels.append("arrow")
    if run & 1: labels.append("substrait")

    print("Substrait integration tests")
    print("=" * 60)
    print(f"Benchmark: {args.benchmark}")
    print(f"Implemented: {sorted(IMPLEMENTED)}")
    print(f"Scale factor: {args.sf}")
    print(f"Run: {' + '.join(labels)} ({run})")

    run_benchmark(bench, args.sf, queries, run=run,
                  explain=args.explain, explain_dir=_explain_dir)

    if args.explain & 3:
        psql_run("ALTER SYSTEM RESET arrow_flight_sql.explain; SELECT pg_reload_conf();")

    _stop_flight_helper()

    p, f, s, e = _results["pass"], _results["fail"], \
                  _results["skip"], _results["error"]
    print("\n" + "=" * 60)
    print(f"{p} passed, {f} failed, {s} skipped, {e} errors")

    # Timing comparison table (skip when only explaining)
    if _timing_log and not args.explain:
        has_arrow = any(t[4] is not None for t in _timing_log)
        if has_arrow:
            print(f"\n{'Query':<8} {'Status':<6} {'Rows':>8} "
                  f"{'PG':>9} {'Arrow':>9} {'Substr':>9} "
                  f"{'Sub/PG':>7} {'Sub/Arr':>8} {'Arr/PG':>7}")
            print("-" * 82)
            tot_pg = tot_arr = tot_sub = 0.0
            for qlabel, rows, st, pg_t, arr_t, sub_t in _timing_log:
                ratio_pg = sub_t / pg_t if pg_t > 0 else float('inf')
                ratio_arr = sub_t / arr_t if arr_t and arr_t > 0 else 0
                arr_pg = arr_t / pg_t if arr_t and pg_t > 0 else 0
                arr_s = f"{arr_t:>8.3f}s" if arr_t else f"{'n/a':>9}"
                r_arr = f"{ratio_arr:>7.1f}x" if arr_t else f"{'n/a':>8}"
                r_apg = f"{arr_pg:>6.1f}x" if arr_t else f"{'n/a':>7}"
                print(f"Q{qlabel:<7} {st:<6} {rows:>8} "
                      f"{pg_t:>8.3f}s {arr_s} {sub_t:>8.3f}s "
                      f"{ratio_pg:>6.1f}x {r_arr} {r_apg}")
                tot_pg += pg_t
                tot_arr += arr_t or 0
                tot_sub += sub_t
            print("-" * 82)
            if tot_pg > 0:
                print(f"{'TOTAL':<8} {'':6} {'':>8} "
                      f"{tot_pg:>8.3f}s {tot_arr:>8.3f}s {tot_sub:>8.3f}s "
                      f"{tot_sub/tot_pg:>6.1f}x "
                      f"{tot_sub/tot_arr if tot_arr > 0 else 0:>7.1f}x "
                      f"{tot_arr/tot_pg if tot_pg > 0 else 0:>6.1f}x")
        else:
            print(f"\n{'Query':<8} {'Status':<6} {'Rows':>8} "
                  f"{'PG':>9} {'Substr':>9} {'Sub/PG':>7}")
            print("-" * 55)
            tot_pg = tot_sub = 0.0
            for qlabel, rows, st, pg_t, _, sub_t in _timing_log:
                ratio = sub_t / pg_t if pg_t > 0 else float('inf')
                print(f"Q{qlabel:<7} {st:<6} {rows:>8} "
                      f"{pg_t:>8.3f}s {sub_t:>8.3f}s {ratio:>6.1f}x")
                tot_pg += pg_t
                tot_sub += sub_t
            print("-" * 55)
            if tot_pg > 0:
                print(f"{'TOTAL':<8} {'':6} {'':>8} "
                      f"{tot_pg:>8.3f}s {tot_sub:>8.3f}s "
                      f"{tot_sub/tot_pg:>6.1f}x")

        # Write timing CSV
        timing_dir = os.path.join(SCRIPT_DIR, "timing")
        os.makedirs(timing_dir, exist_ok=True)
        csv_path = os.path.join(timing_dir,
                                f"timing_{args.benchmark}_sf{args.sf}.csv")
        with open(csv_path, "w", newline="") as cf:
            w = csv.writer(cf)
            w.writerow(["sf", "query", "status", "rows",
                         "pg_time_s", "arrow_time_s", "substrait_time_s",
                         "sub_pg_ratio"])
            for qlabel, rows, st, pg_t, arr_t, sub_t in _timing_log:
                ratio = sub_t / pg_t if pg_t > 0 else 0
                w.writerow([args.sf, f"Q{qlabel}", st, rows,
                            f"{pg_t:.3f}",
                            f"{arr_t:.3f}" if arr_t else "",
                            f"{sub_t:.3f}", f"{ratio:.2f}"])
        print(f"\nTiming CSV: {csv_path}")

    sys.exit(0 if f == 0 and e == 0 else 1)


if __name__ == "__main__":
    main()
