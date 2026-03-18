#!/usr/bin/env python3
"""
Substrait integration tests — TPC-H & TPC-DS.

Prebuilt Substrait binary plans from substrait_test/{tpch,tpcds}/plans/.
PostgreSQL (via psql) provides expected results; Flight SQL provides actual.

Run: python3 substrait_test/test_substrait.py
"""

import os
import subprocess
import sys
import tempfile
import time

import pyarrow.flight as flight

# ============================================================
# Config
# ============================================================

FLIGHT_URI = "grpc://127.0.0.1:15432"
PSQL_CMD = ["psql", "-h", "127.0.0.1", "-U", "martin", "-d", "postgres",
            "-v", "ON_ERROR_STOP=1"]

# CLI: --sf 0.1, --suite tpch/tpcds, --query 16, --benchmark, --csv, --explain
SCALE_FACTOR = os.environ.get("SCALE_FACTOR", "1")
FILTER_SUITE = None   # None = all
FILTER_QUERY = None   # None = all
BENCHMARK = False
CSV_OUTPUT = False
EXPLAIN_MODE = False
for i, a in enumerate(sys.argv[1:], 1):
    if a == "--sf" and i < len(sys.argv) - 1:
        SCALE_FACTOR = sys.argv[i + 1]
    elif a == "--suite" and i < len(sys.argv) - 1:
        FILTER_SUITE = sys.argv[i + 1]
    elif a == "--query" and i < len(sys.argv) - 1:
        FILTER_QUERY = int(sys.argv[i + 1])
    elif a == "--benchmark":
        BENCHMARK = True
    elif a == "--csv":
        CSV_OUTPUT = True
    elif a == "--explain":
        EXPLAIN_MODE = True

AFS_EXPLAIN_DIR = os.environ.get("AFS_EXPLAIN_DIR", "/tmp/afs_explain")

TPCH_DATA_DIR = os.environ.get(
    "TPCH_DATA_DIR", f"/tmp/tpch_sf{SCALE_FACTOR}")

TPCDS_DATA_DIR = os.environ.get(
    "TPCDS_DATA_DIR", f"/tmp/tpcds_sf{SCALE_FACTOR}")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TPCH_DIR = os.path.join(SCRIPT_DIR, "tpch")
TPCDS_DIR = os.path.join(SCRIPT_DIR, "tpcds")

# Features the converter currently handles.
IMPLEMENTED = {"read", "aggregate", "project", "filter", "sort",
               "cast", "interval_day", "interval_year", "cross",
               "like", "extract", "if_then", "fetch", "join",
               "subquery"}

# TPC-H: features each query requires (from Isthmus plan analysis).
TPCH_REQUIRES = {
    1:  {"aggregate", "filter", "project", "read", "sort", "cast",
         "interval_day"},
    2:  {"aggregate", "filter", "project", "read", "sort", "fetch",
         "cross", "cast", "subquery", "like"},
    3:  {"aggregate", "filter", "project", "read", "sort", "fetch",
         "cross", "cast"},
    4:  {"aggregate", "filter", "project", "read", "sort", "subquery",
         "interval_year"},
    5:  {"aggregate", "filter", "project", "read", "sort", "cross", "cast"},
    6:  {"aggregate", "filter", "project", "read", "interval_year"},
    7:  {"aggregate", "filter", "project", "read", "sort", "cross", "cast",
         "extract"},
    8:  {"aggregate", "filter", "project", "read", "sort", "cross",
         "cast", "if_then", "extract"},
    9:  {"aggregate", "filter", "project", "read", "sort", "cross", "cast",
         "like", "extract"},
    10: {"aggregate", "filter", "project", "read", "sort", "fetch",
         "cross", "cast"},
    11: {"aggregate", "filter", "project", "read", "sort", "cross",
         "cast", "subquery"},
    12: {"aggregate", "filter", "project", "read", "sort", "cross",
         "cast", "if_then"},
    13: {"aggregate", "filter", "project", "read", "sort", "join", "cast",
         "like"},
    14: {"aggregate", "filter", "project", "read", "cross", "cast",
         "if_then", "like"},
    15: {"aggregate", "filter", "project", "read", "sort", "cross",
         "cast", "subquery"},
    16: {"aggregate", "filter", "project", "read", "sort", "cross",
         "cast", "subquery", "like"},
    17: {"aggregate", "filter", "project", "read", "cross", "cast",
         "subquery"},
    18: {"aggregate", "filter", "project", "read", "sort", "fetch",
         "cross", "subquery"},
    19: {"aggregate", "filter", "project", "read", "cross", "cast"},
    20: {"aggregate", "filter", "project", "read", "sort", "cross",
         "cast", "subquery", "like"},
    21: {"aggregate", "filter", "project", "read", "sort", "fetch",
         "cross", "subquery"},
    22: {"aggregate", "filter", "project", "read", "sort", "subquery"},
}


# ============================================================
# Helpers — PostgreSQL
# ============================================================

def psql_run(sql):
    r = subprocess.run(PSQL_CMD, input=sql.encode(), capture_output=True)
    if r.returncode != 0:
        raise RuntimeError(f"psql error:\n{r.stderr.decode()}")
    return r.stdout.decode()


def psql_csv(sql):
    """Run query via psql, return rows as list of tuples."""
    sep = "\x01"  # avoid splitting on commas inside data
    cmd = PSQL_CMD + ["-t", "-A", "-F", sep, "-c", sql]
    r = subprocess.run(cmd, capture_output=True, timeout=60)
    if r.returncode != 0:
        raise RuntimeError(f"psql error:\n{r.stderr.decode()}")
    text = r.stdout.decode().strip()
    if not text:
        return []
    rows = []
    for line in text.split("\n"):
        if not line:
            continue
        rows.append(tuple(parse_csv_value(v) for v in line.split(sep)))
    return rows


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


# ============================================================
# Helpers — Flight SQL
# ============================================================

def varint(v):
    r = []
    while v > 0x7F:
        r.append((v & 0x7F) | 0x80)
        v >>= 7
    r.append(v & 0x7F)
    return bytes(r)


def pb_field(n, data):
    return varint((n << 3) | 2) + varint(len(data)) + data


_flight_client = None
_flight_options = None

def _get_flight():
    global _flight_client, _flight_options
    if _flight_client is None:
        _flight_client = flight.FlightClient(FLIGHT_URI)
        token_pair = _flight_client.authenticate_basic_token("martin", "")
        _flight_options = flight.FlightCallOptions(headers=[token_pair])
    return _flight_client, _flight_options

def flight_reset():
    global _flight_client, _flight_options
    if _flight_client:
        _flight_client.close()
    _flight_client = None
    _flight_options = None

def flight_exec_substrait(plan_bytes):
    client, options = _get_flight()
    cmd = pb_field(
        1, b"type.googleapis.com/arrow.flight.protocol.sql.CommandStatementSubstraitPlan"
    ) + pb_field(2, pb_field(1, pb_field(1, plan_bytes)))
    info = client.get_flight_info(flight.FlightDescriptor.for_command(cmd), options)
    reader = client.do_get(info.endpoints[0].ticket, options)
    return reader.read_all()


# ============================================================
# EXPLAIN capture
# ============================================================

EXPLAIN_DIR = os.path.join(SCRIPT_DIR, "explain")


def save_explain_pg(query_name, sql):
    """Run EXPLAIN ANALYZE via psql and save output."""
    explain_sql = f"EXPLAIN ANALYZE {sql}"
    cmd = PSQL_CMD + ["-c", explain_sql]
    r = subprocess.run(cmd, capture_output=True, timeout=60)
    if r.returncode != 0:
        return
    path = os.path.join(EXPLAIN_DIR, f"{query_name}_pg.txt")
    with open(path, "w") as f:
        f.write(r.stdout.decode())


def save_explain_substrait(query_name):
    """Read adapter's EXPLAIN ANALYZE output from AFS_EXPLAIN_DIR/latest.txt."""
    src = os.path.join(AFS_EXPLAIN_DIR, "latest.txt")
    if not os.path.exists(src):
        return
    dst = os.path.join(EXPLAIN_DIR, f"{query_name}_substrait.txt")
    with open(src) as f:
        content = f.read()
    with open(dst, "w") as f:
        f.write(content)


# ============================================================
# Result comparison
# ============================================================

def normalize_value(v):
    if isinstance(v, float):
        return round(v, 4)
    if isinstance(v, str):
        v = v.strip()
        try:
            return int(v)
        except ValueError:
            try:
                return round(float(v), 4)
            except ValueError:
                return v
    # Arrow returns datetime.date for DATE columns
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
    return False, "\n    ".join(lines)


# ============================================================
# TPC-H
# ============================================================

TPCH_TABLES = ["REGION", "NATION", "PART", "SUPPLIER", "PARTSUPP",
               "CUSTOMER", "ORDERS", "LINEITEM"]

_tpch_loaded = False


def load_tpch_into_pg():
    global _tpch_loaded
    if _tpch_loaded:
        return

    print(f"  Loading TPC-H SF{SCALE_FACTOR} into PostgreSQL ... ", end="", flush=True)

    init_sql = os.path.join(TPCH_DIR, "pg_init.sql")
    cmd = PSQL_CMD + ["-f", init_sql]
    r = subprocess.run(cmd, capture_output=True)
    if r.returncode != 0:
        raise RuntimeError(f"pg_init.sql failed:\n{r.stderr.decode()}")

    tbl_name_map = {
        "REGION": "region", "NATION": "nation", "PART": "part",
        "SUPPLIER": "supplier", "PARTSUPP": "partsupp",
        "CUSTOMER": "customer", "ORDERS": "orders", "LINEITEM": "lineitem",
    }
    for table in TPCH_TABLES:
        tbl_file = os.path.join(TPCH_DATA_DIR, f"{tbl_name_map[table]}.tbl")
        if not os.path.exists(tbl_file):
            raise RuntimeError(f"Missing data file: {tbl_file}")
        # Strip trailing '|' (dbgen artifact)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.tbl',
                                         delete=False) as tmp:
            with open(tbl_file) as src:
                for line in src:
                    stripped = line.rstrip()
                    if stripped.endswith('|'):
                        stripped = stripped[:-1]
                    tmp.write(stripped + '\n')
            tmp_path = tmp.name
        copy_sql = (f"\\COPY \"{table}\" FROM '{tmp_path}' "
                    f"WITH (FORMAT csv, DELIMITER '|');")
        psql_run(copy_sql)
        os.unlink(tmp_path)

    _tpch_loaded = True
    print("done")


def cleanup_tpch():
    if not _tpch_loaded:
        return
    drop_sql = "".join(f'DROP TABLE IF EXISTS "{t}" CASCADE;\n'
                       for t in reversed(TPCH_TABLES))
    psql_run(drop_sql)


# ============================================================
# Test runner
# ============================================================

_results = {"pass": 0, "fail": 0, "skip": 0}
_timings = []  # list of (query_name, pg_time_s, substrait_time_s)


def _wait_for_pg(max_wait=30):
    """Block until PG accepts connections (after crash recovery)."""
    import time
    for _ in range(max_wait):
        try:
            subprocess.run(PSQL_CMD + ["-c", "SELECT 1"],
                           capture_output=True, timeout=5)
            return True
        except Exception:
            pass
        time.sleep(1)
    return False


def run_test(name, requires, fn):
    missing = set(requires) - IMPLEMENTED
    if missing:
        print(f"  {name} ... SKIP (need {missing})")
        _results["skip"] += 1
        return

    print(f"  {name} ... ", end="", flush=True)
    try:
        fn()
        print("PASS")
        _results["pass"] += 1
    except Exception as e:
        msg = str(e).split('\n')[0][:120]
        print(f"FAIL\n    {msg}")
        _results["fail"] += 1
        # Reset Flight client in case PG crashed / connection broke
        flight_reset()
        _wait_for_pg()


# ============================================================
# TPC-H test
# ============================================================

def run_tpch_test(qnr):
    plan_file = os.path.join(TPCH_DIR, "plans", f"{qnr:02d}.proto")
    with open(plan_file, "rb") as f:
        plan = f.read()

    query_file = os.path.join(TPCH_DIR, "queries", f"{qnr:02d}.sql")
    with open(query_file) as f:
        query_sql = f.read().strip()
    if query_sql.endswith(";"):
        query_sql = query_sql[:-1]

    t0 = time.monotonic()
    expected = psql_csv(query_sql)
    pg_time = time.monotonic() - t0

    t0 = time.monotonic()
    actual = table_to_rows(flight_exec_substrait(plan))
    substrait_time = time.monotonic() - t0

    if EXPLAIN_MODE:
        name = f"tpch_q{qnr:02d}"
        save_explain_pg(name, query_sql)
        save_explain_substrait(name)

    if BENCHMARK:
        _timings.append((f"TPC-H Q{qnr:02d}", pg_time, substrait_time))

    has_sort = "order by" in query_sql.lower()
    ok, diff = rows_equal(expected, actual, ordered=has_sort)
    if not ok:
        raise AssertionError(f"Mismatch: {diff}")


def tpch_tests():
    print("\nTPC-H")
    print("-" * 40)

    any_runnable = any(
        not (TPCH_REQUIRES.get(q, set()) - IMPLEMENTED)
        for q in range(1, 23)
    )
    if any_runnable:
        load_tpch_into_pg()

    for qnr in range(1, 23):
        if FILTER_QUERY is not None and qnr != FILTER_QUERY:
            continue
        required = TPCH_REQUIRES.get(qnr, set())
        run_test(f"TPC-H Q{qnr:02d}", required, lambda q=qnr:
            run_tpch_test(q))


# ============================================================
# TPC-DS
# ============================================================

# dsdgen file name -> UPPERCASE PG table name
TPCDS_TABLES = [
    "CALL_CENTER", "CATALOG_PAGE", "CATALOG_RETURNS", "CATALOG_SALES",
    "CUSTOMER", "CUSTOMER_ADDRESS", "CUSTOMER_DEMOGRAPHICS", "DATE_DIM",
    "DBGEN_VERSION", "HOUSEHOLD_DEMOGRAPHICS", "INCOME_BAND", "INVENTORY",
    "ITEM", "PROMOTION", "REASON", "SHIP_MODE", "STORE", "STORE_RETURNS",
    "STORE_SALES", "TIME_DIM", "WAREHOUSE", "WEB_PAGE", "WEB_RETURNS",
    "WEB_SALES", "WEB_SITE",
]

_tpcds_loaded = False


def load_tpcds_into_pg():
    global _tpcds_loaded
    if _tpcds_loaded:
        return

    print(f"  Loading TPC-DS SF{SCALE_FACTOR} into PostgreSQL ... ", end="", flush=True)

    init_sql = os.path.join(TPCDS_DIR, "pg_init.sql")
    cmd = PSQL_CMD + ["-f", init_sql]
    r = subprocess.run(cmd, capture_output=True)
    if r.returncode != 0:
        raise RuntimeError(f"pg_init.sql failed:\n{r.stderr.decode()}")

    for table in TPCDS_TABLES:
        dat_file = os.path.join(TPCDS_DATA_DIR, f"{table.lower()}.dat")
        if not os.path.exists(dat_file):
            raise RuntimeError(f"Missing data file: {dat_file}")
        # dsdgen produces trailing '|' — strip it for COPY
        with tempfile.NamedTemporaryFile(mode='w', suffix='.dat',
                                         delete=False) as tmp:
            with open(dat_file) as src:
                for line in src:
                    stripped = line.rstrip()
                    if stripped.endswith('|'):
                        stripped = stripped[:-1]
                    tmp.write(stripped + '\n')
            tmp_path = tmp.name
        copy_sql = (f"\\COPY \"tpcds\".\"{table}\" FROM '{tmp_path}' "
                    f"WITH (FORMAT csv, DELIMITER '|', NULL '');")
        psql_run(copy_sql)
        os.unlink(tmp_path)

    _tpcds_loaded = True
    print("done")


def cleanup_tpcds():
    if not _tpcds_loaded:
        return
    psql_run('DROP SCHEMA IF EXISTS "tpcds" CASCADE;\n')


def run_tpcds_test(qnr):
    plan_file = os.path.join(TPCDS_DIR, "plans", f"{qnr:02d}.proto")
    with open(plan_file, "rb") as f:
        plan = f.read()

    query_file = os.path.join(TPCDS_DIR, "queries", f"{qnr:02d}.sql")
    with open(query_file) as f:
        query_sql = f.read().strip()
    if query_sql.endswith(";"):
        query_sql = query_sql[:-1]

    t0 = time.monotonic()
    expected = psql_csv(query_sql)
    pg_time = time.monotonic() - t0

    t0 = time.monotonic()
    actual = table_to_rows(flight_exec_substrait(plan))
    substrait_time = time.monotonic() - t0

    if EXPLAIN_MODE:
        name = f"tpcds_q{qnr:02d}"
        save_explain_pg(name, query_sql)
        save_explain_substrait(name)

    if BENCHMARK:
        _timings.append((f"TPC-DS Q{qnr:02d}", pg_time, substrait_time))

    has_sort = "order by" in query_sql.lower()
    ok, diff = rows_equal(expected, actual, ordered=has_sort)
    if not ok:
        raise AssertionError(f"Mismatch: {diff}")


def tpcds_tests():
    print("\nTPC-DS")
    print("-" * 40)

    # Discover available plans
    plans_dir = os.path.join(TPCDS_DIR, "plans")
    available = sorted(
        int(f[:2]) for f in os.listdir(plans_dir) if f.endswith(".proto")
    )
    if not available:
        print("  No TPC-DS plans found, skipping.")
        return

    load_tpcds_into_pg()

    for qnr in available:
        if FILTER_QUERY is not None and qnr != FILTER_QUERY:
            continue
        run_test(f"TPC-DS Q{qnr:02d}", set(), lambda q=qnr:
            run_tpcds_test(q))


# ============================================================
# Main
# ============================================================

def print_benchmark():
    if not _timings:
        return
    if CSV_OUTPUT:
        print("query,pg_time_s,substrait_time_s,ratio,status")
        for name, pg_t, sub_t in _timings:
            ratio = sub_t / pg_t if pg_t > 0 else float('inf')
            status = "regressed" if ratio > 2.0 else "ok"
            print(f"{name},{pg_t:.4f},{sub_t:.4f},{ratio:.2f},{status}")
        return

    print("\nBenchmark Results")
    print("=" * 70)
    print(f"{'query':<16} {'pg_time_s':>10} {'substrait_time_s':>16} {'ratio':>8} {'status':>10}")
    print("-" * 70)
    for name, pg_t, sub_t in _timings:
        ratio = sub_t / pg_t if pg_t > 0 else float('inf')
        status = "REGRESSED" if ratio > 2.0 else "ok"
        print(f"{name:<16} {pg_t:>10.4f} {sub_t:>16.4f} {ratio:>8.2f} {status:>10}")
    print("-" * 70)
    regressed = sum(1 for _, pg_t, sub_t in _timings
                    if pg_t > 0 and sub_t / pg_t > 2.0)
    print(f"{len(_timings)} queries, {regressed} regressed (>2x)")


def main():
    print(f"Substrait integration tests (SF{SCALE_FACTOR})")
    print("=" * 40)
    print(f"Implemented: {sorted(IMPLEMENTED)}")

    if EXPLAIN_MODE:
        os.makedirs(EXPLAIN_DIR, exist_ok=True)
        print(f"EXPLAIN output -> {EXPLAIN_DIR}")

    if FILTER_SUITE in (None, "tpch"):
        tpch_tests()
    if FILTER_SUITE in (None, "tpcds"):
        tpcds_tests()

    if BENCHMARK:
        print_benchmark()

    if FILTER_SUITE in (None, "tpch"):
        cleanup_tpch()
    if FILTER_SUITE in (None, "tpcds"):
        cleanup_tpcds()
    flight_reset()

    p, f, s = _results["pass"], _results["fail"], _results["skip"]
    print("\n" + "=" * 40)
    print(f"{p} passed, {f} failed, {s} skipped")
    sys.exit(0 if f == 0 else 1)


if __name__ == "__main__":
    main()
