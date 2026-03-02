#!/usr/bin/env python3
"""
Substrait integration tests — TPC-H.

Prebuilt Substrait binary plans from substrait_test/tpch/plans/.
PostgreSQL (via psql) provides expected results; Flight SQL provides actual.

Run: python3 substrait_test/test_substrait.py
"""

import os
import subprocess
import sys

import pyarrow.flight as flight

# ============================================================
# Config
# ============================================================

FLIGHT_URI = "grpc://127.0.0.1:15432"
PSQL_CMD = ["psql", "-h", "127.0.0.1", "-U", "martin", "-d", "postgres",
            "-v", "ON_ERROR_STOP=1"]

TPCH_DATA_DIR = os.environ.get(
    "TPCH_DATA_DIR",
    os.path.expanduser("~/Downloads/tpch/tpch/data"))

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TPCH_DIR = os.path.join(SCRIPT_DIR, "tpch")

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

    print("  Loading TPC-H sf=0.01 into PostgreSQL ... ", end="", flush=True)

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
        copy_sql = (f"\\COPY \"{table}\" FROM '{tbl_file}' "
                    f"WITH (FORMAT csv, DELIMITER '|');")
        psql_run(copy_sql)

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
        print(f"FAIL\n    {e}")
        _results["fail"] += 1


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

    expected = psql_csv(query_sql)
    actual = table_to_rows(flight_exec_substrait(plan))

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
        required = TPCH_REQUIRES.get(qnr, set())
        run_test(f"TPC-H Q{qnr:02d}", required, lambda q=qnr:
            run_tpch_test(q))


# ============================================================
# Main
# ============================================================

def main():
    print("Substrait integration tests")
    print("=" * 40)
    print(f"Implemented: {sorted(IMPLEMENTED)}")

    tpch_tests()

    cleanup_tpch()
    flight_reset()

    p, f, s = _results["pass"], _results["fail"], _results["skip"]
    print("\n" + "=" * 40)
    print(f"{p} passed, {f} failed, {s} skipped")
    sys.exit(0 if f == 0 else 1)


if __name__ == "__main__":
    main()
