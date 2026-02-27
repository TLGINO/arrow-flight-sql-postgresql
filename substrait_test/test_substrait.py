#!/usr/bin/env python3
"""
Substrait integration tests — unit operators + TPC-H.

Unit tests use Isthmus (substrait-java) CLI to generate plans on-the-fly.
TPC-H tests use prebuilt Isthmus binary plans from substrait_test/tpch/plans/.
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

# Isthmus CLI via Gradle wrapper
ISTHMUS_JAVA_HOME = os.environ.get(
    "ISTHMUS_JAVA_HOME", "/usr/lib/jvm/java-21-openjdk")
ISTHMUS_DIR = os.environ.get(
    "ISTHMUS_DIR",
    os.path.expanduser("~/Documents/substrait-java"))

TPCH_DATA_DIR = os.environ.get(
    "TPCH_DATA_DIR",
    os.path.expanduser("~/Downloads/tpch/tpch/data"))

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TPCH_DIR = os.path.join(SCRIPT_DIR, "tpch")

# Features the converter currently handles.
IMPLEMENTED = {"read", "aggregate", "project", "filter", "sort",
               "cast", "interval_day", "interval_year"}

# TPC-H: features each query requires (from Isthmus plan analysis).
TPCH_REQUIRES = {
    1:  {"aggregate", "filter", "project", "read", "sort", "cast",
         "interval_day"},
    2:  {"aggregate", "filter", "project", "read", "sort", "fetch",
         "cross", "cast", "subquery"},
    3:  {"aggregate", "filter", "project", "read", "sort", "fetch",
         "cross", "cast"},
    4:  {"aggregate", "filter", "project", "read", "sort", "subquery",
         "interval_year"},
    5:  {"aggregate", "filter", "project", "read", "sort", "cross", "cast"},
    6:  {"aggregate", "filter", "project", "read", "interval_year"},
    7:  {"aggregate", "filter", "project", "read", "sort", "cross", "cast"},
    8:  {"aggregate", "filter", "project", "read", "sort", "cross",
         "cast", "if_then"},
    9:  {"aggregate", "filter", "project", "read", "sort", "cross", "cast"},
    10: {"aggregate", "filter", "project", "read", "sort", "fetch",
         "cross", "cast"},
    11: {"aggregate", "filter", "project", "read", "sort", "cross",
         "cast", "subquery"},
    12: {"aggregate", "filter", "project", "read", "sort", "cross",
         "cast", "if_then"},
    13: {"aggregate", "filter", "project", "read", "sort", "join", "cast"},
    14: {"aggregate", "filter", "project", "read", "cross", "cast",
         "if_then"},
    15: {"aggregate", "filter", "project", "read", "sort", "cross",
         "cast", "subquery"},
    16: {"aggregate", "filter", "project", "read", "sort", "cross",
         "cast", "subquery"},
    17: {"aggregate", "filter", "project", "read", "cross", "cast",
         "subquery"},
    18: {"aggregate", "filter", "project", "read", "sort", "fetch",
         "cross", "subquery"},
    19: {"aggregate", "filter", "project", "read", "cross", "cast"},
    20: {"aggregate", "filter", "project", "read", "sort", "cross",
         "cast", "subquery"},
    21: {"aggregate", "filter", "project", "read", "sort", "fetch",
         "cross", "subquery"},
    22: {"aggregate", "filter", "project", "read", "sort", "subquery"},
}


# ============================================================
# Helpers — Isthmus
# ============================================================

_isthmus_available = None

def check_isthmus():
    global _isthmus_available
    if _isthmus_available is not None:
        return _isthmus_available
    gradlew = os.path.join(ISTHMUS_DIR, "gradlew")
    javac = os.path.join(ISTHMUS_JAVA_HOME, "bin", "javac")
    _isthmus_available = os.path.isfile(gradlew) and os.path.isfile(javac)
    return _isthmus_available


def isthmus_plan(create_stmts, query):
    """Run isthmus CLI → binary substrait plan."""
    inner_args = []
    for c in create_stmts:
        inner_args.append(f'-c "{c}"')
    inner_args.append("--outputformat BINARY")
    inner_args.append(f'"{query}"')

    env = os.environ.copy()
    env["JAVA_HOME"] = ISTHMUS_JAVA_HOME

    cmd = [
        os.path.join(ISTHMUS_DIR, "gradlew"),
        "-p", ISTHMUS_DIR,
        ":isthmus-cli:run",
        "--args", " ".join(inner_args),
    ]
    r = subprocess.run(cmd, capture_output=True, timeout=120, env=env)
    if r.returncode != 0:
        raise RuntimeError(f"Isthmus error:\n{r.stderr.decode()}")
    return r.stdout


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
    cmd = PSQL_CMD + ["-t", "-A", "-F,", "-c", sql]
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
        rows.append(tuple(parse_csv_value(v) for v in line.split(",")))
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


def flight_exec_substrait(plan_bytes):
    client = flight.FlightClient(FLIGHT_URI)
    token_pair = client.authenticate_basic_token("martin", "")
    options = flight.FlightCallOptions(headers=[token_pair])
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
# TPC-H data loading
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
# Unit test helpers
# ============================================================

def run_unit_test(pg_setup_sql, pg_query, create_stmts, isthmus_query):
    """
    pg_setup_sql: DROP/CREATE/INSERT for PG (UPPERCASE quoted names)
    pg_query: reference query for PG (UPPERCASE quoted names)
    create_stmts: CREATE TABLE stmts for isthmus (lowercase, unquoted)
    isthmus_query: query for isthmus (lowercase, unquoted)
    """
    psql_run(pg_setup_sql)
    expected = psql_csv(pg_query)

    plan = isthmus_plan(create_stmts, isthmus_query)
    actual = table_to_rows(flight_exec_substrait(plan))

    ok, diff = rows_equal(expected, actual)
    if not ok:
        raise AssertionError(f"Mismatch: {diff}")


# (isthmus_create, pg_create, insert)
TABLES = {
    "t1": ('CREATE TABLE t1(id INTEGER, name VARCHAR(50))',
           'CREATE TABLE "T1"("ID" INTEGER, "NAME" VARCHAR(50))',
           'INSERT INTO "T1" VALUES (1,\'alice\'),(2,\'bob\'),(3,\'charlie\')'),
    "t2": ('CREATE TABLE t2(x INTEGER)',
           'CREATE TABLE "T2"("X" INTEGER)',
           'INSERT INTO "T2" VALUES (1),(2),(3),(4),(5)'),
    "t3": ('CREATE TABLE t3(color VARCHAR(50))',
           'CREATE TABLE "T3"("COLOR" VARCHAR(50))',
           'INSERT INTO "T3" VALUES (\'red\'),(\'blue\'),(\'red\'),(\'green\'),(\'blue\'),(\'red\')'),
    "t4": ('CREATE TABLE t4(x INTEGER)',
           'CREATE TABLE "T4"("X" INTEGER)',
           'INSERT INTO "T4" VALUES (10),(20),(30),(40),(50)'),
    "t5": ('CREATE TABLE t5(t1_id INTEGER, val VARCHAR(50))',
           'CREATE TABLE "T5"("T1_ID" INTEGER, "VAL" VARCHAR(50))',
           'INSERT INTO "T5" VALUES (1,\'x\'),(2,\'y\'),(3,\'z\'),(1,\'w\')'),
}


def setup(*names):
    """Return (pg_setup_sql, isthmus_creates) for given tables."""
    pg_setup = "\n".join(
        f'DROP TABLE IF EXISTS "{n.upper()}";\n{TABLES[n][1]};\n{TABLES[n][2]};'
        for n in names)
    isthmus_creates = [TABLES[n][0] for n in names]
    return pg_setup, isthmus_creates


def quote_upper(sql):
    """Convert lowercase-identifier SQL to PG double-quoted UPPERCASE SQL.

    Simple heuristic: quote words that look like identifiers (after FROM,
    table.col patterns). Good enough for our simple unit test queries.
    """
    import re
    # Quote table names after FROM/JOIN/INTO
    sql = re.sub(r'\b(FROM|JOIN|INTO)\s+(\w+)', lambda m: f'{m.group(1)} "{m.group(2).upper()}"', sql, flags=re.IGNORECASE)
    # Quote column references (standalone words that aren't SQL keywords)
    keywords = {'SELECT', 'FROM', 'WHERE', 'GROUP', 'BY', 'ORDER', 'AS',
                'AND', 'OR', 'NOT', 'IN', 'ON', 'JOIN', 'INNER', 'LEFT',
                'RIGHT', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'ASC', 'DESC',
                'LIMIT', 'OFFSET', 'HAVING', 'DISTINCT', 'INTO', 'INSERT',
                'VALUES', 'NULL', 'TRUE', 'FALSE', 'CASE', 'WHEN', 'THEN',
                'ELSE', 'END', 'BETWEEN', 'LIKE', 'IS', 'EXISTS', 'ALL',
                'ANY', 'SOME', 'UNION', 'INTERSECT', 'EXCEPT', 'WITH',
                'CAST', 'INTERVAL', 'DATE', 'TIMESTAMP', 'INTEGER', 'VARCHAR',
                'DECIMAL', 'BIGINT', 'CHAR', 'BOOLEAN', 'FLOAT', 'DOUBLE'}
    return sql


def pg_query_for(query):
    """Convert a lowercase unquoted query to PG-compatible UPPERCASE quoted query."""
    import re

    # Tokenize: keep strings, numbers, operators, and words
    tokens = re.findall(r"'[^']*'|\d+(?:\.\d+)?|[(),;*+\-/<>=!]+|\w+", query)

    keywords = {'select', 'from', 'where', 'group', 'by', 'order', 'as',
                'and', 'or', 'not', 'in', 'on', 'join', 'inner', 'left',
                'right', 'count', 'sum', 'avg', 'min', 'max', 'asc', 'desc',
                'limit', 'offset', 'having', 'distinct'}

    result = []
    for tok in tokens:
        if tok.startswith("'"):
            result.append(tok)
        elif tok.lower() in keywords:
            result.append(tok.upper())
        elif re.match(r'^[a-zA-Z_]\w*$', tok):
            result.append(f'"{tok.upper()}"')
        else:
            result.append(tok)

    return " ".join(result)


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


# ============================================================
# Tier 1 — Unit operator tests
# ============================================================

def tier1_tests():
    print("\nTier 1 — Unit operators")
    print("-" * 40)

    if not check_isthmus():
        print("  (Isthmus not available — skipping unit tests)")
        print("  Set ISTHMUS_DIR and ensure JDK is installed")
        return

    def unit(name, requires, tables, query):
        requires = requires | {"isthmus"}
        pg_setup, isthmus_creates = setup(*tables)
        pq = pg_query_for(query)
        run_test(name, requires, lambda:
            run_unit_test(pg_setup, pq, isthmus_creates, query))

    # ReadRel
    unit("SELECT *", {"read"},
         ["t1"], "SELECT * FROM t1")

    # AggregateRel
    unit("count(*)", {"read", "aggregate"},
         ["t2"], "SELECT count(*) FROM t2")

    unit("GROUP BY + count", {"read", "aggregate"},
         ["t3"], "SELECT color, count(*) FROM t3 GROUP BY color")

    unit("sum/avg/min/max", {"read", "aggregate"},
         ["t4"], "SELECT sum(x), avg(x), min(x), max(x) FROM t4")

    # FilterRel
    unit("WHERE simple", {"read", "filter"},
         ["t2"], "SELECT * FROM t2 WHERE x > 3")

    unit("WHERE + agg", {"read", "filter", "aggregate"},
         ["t2"], "SELECT count(*) FROM t2 WHERE x > 2")

    # SortRel
    unit("ORDER BY", {"read", "sort"},
         ["t1"], "SELECT * FROM t1 ORDER BY id")

    # Expressions
    unit("Arith expression", {"read", "project", "filter"},
         ["t2"], "SELECT x, x * 2 + 1 AS doubled FROM t2 WHERE x > 2")


# ============================================================
# Tier 2 — TPC-H queries
# ============================================================

def tier2_tests():
    print("\nTier 2 — TPC-H")
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

    if check_isthmus():
        IMPLEMENTED.add("isthmus")
    print(f"Implemented: {sorted(IMPLEMENTED)}")

    tier1_tests()
    tier2_tests()

    # Cleanup
    try:
        drop = "; ".join(f'DROP TABLE IF EXISTS "{n.upper()}"'
                         for n in TABLES.keys())
        psql_run(drop + ";")
    except Exception:
        pass
    cleanup_tpch()

    p, f, s = _results["pass"], _results["fail"], _results["skip"]
    print("\n" + "=" * 40)
    print(f"{p} passed, {f} failed, {s} skipped")
    sys.exit(0 if f == 0 else 1)


if __name__ == "__main__":
    main()
