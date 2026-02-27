#!/usr/bin/env python3
"""
End-to-end substrait integration tests.

Compares DuckDB (source of truth) results against Flight SQL adapter results
for the same substrait plan.

Requires:
  - DuckDB with substrait extension at DUCKDB_PATH
  - PostgreSQL accessible via psql
  - Flight SQL adapter running on grpc://127.0.0.1:15432
"""

import subprocess
import sys
import pyarrow.flight as flight

DUCKDB = "/home/martin/Documents/duckdb-substrait-extension/build/release/duckdb"
FLIGHT_URI = "grpc://127.0.0.1:15432"
PSQL_CMD = ["psql", "-h", "127.0.0.1", "-U", "martin", "-d", "postgres", "-v", "ON_ERROR_STOP=1"]


# --- helpers ---

def duckdb_run(sql, csv=False):
    """Run SQL in DuckDB, return stdout bytes."""
    cmd = [DUCKDB]
    if csv:
        cmd += ["-csv", "-noheader"]
    result = subprocess.run(cmd, input=sql.encode(), capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(f"DuckDB error:\n{result.stderr.decode()}")
    return result.stdout


def duckdb_query_csv(setup_sql, query):
    """Run query in DuckDB with setup, return CSV lines (header + rows)."""
    sql = f"LOAD substrait;\n{setup_sql}\n.mode csv\n{query};"
    cmd = [DUCKDB, "-csv"]
    result = subprocess.run(cmd, input=sql.encode(), capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(f"DuckDB error:\n{result.stderr.decode()}")
    return result.stdout.decode().strip()


def duckdb_get_substrait(setup_sql, query):
    """Get raw substrait protobuf bytes for a query from DuckDB."""
    sql = f"LOAD substrait;\n{setup_sql}\nSELECT * FROM get_substrait('{query}');"
    raw = duckdb_run(sql, csv=True)
    return unescape_duckdb(raw.rstrip(b"\r\n"))


def unescape_duckdb(data):
    """Convert DuckDB hex-escaped blob (\\xNN) to raw bytes."""
    result = bytearray()
    i = 0
    while i < len(data):
        if data[i:i+2] == b'\\x' and i + 4 <= len(data):
            hex_digits = data[i+2:i+4]
            try:
                result.append(int(hex_digits, 16))
                i += 4
            except ValueError:
                result.append(data[i])
                i += 1
        else:
            result.append(data[i])
            i += 1
    return bytes(result)


def psql_run(sql):
    """Run SQL via psql."""
    result = subprocess.run(PSQL_CMD, input=sql.encode(), capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(f"psql error:\n{result.stderr.decode()}")
    return result.stdout.decode()


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
    """Send substrait plan to Flight SQL adapter, return pyarrow Table."""
    client = flight.FlightClient(FLIGHT_URI)
    token_pair = client.authenticate_basic_token("martin", "")
    options = flight.FlightCallOptions(headers=[token_pair])

    cmd = pb_field(
        1, b"type.googleapis.com/arrow.flight.protocol.sql.CommandStatementSubstraitPlan"
    ) + pb_field(2, pb_field(1, pb_field(1, plan_bytes)))

    info = client.get_flight_info(flight.FlightDescriptor.for_command(cmd), options)
    reader = client.do_get(info.endpoints[0].ticket, options)
    return reader.read_all()


def table_to_sorted_rows(table):
    """Convert pyarrow Table to sorted list of tuples for comparison."""
    rows = []
    for i in range(table.num_rows):
        row = []
        for col in range(table.num_columns):
            v = table.column(col)[i].as_py()
            # Normalize numeric types for comparison
            if isinstance(v, float):
                v = round(v, 6)
            row.append(v)
        rows.append(tuple(row))
    return sorted(rows)


def duckdb_csv_to_rows(csv_text):
    """Parse DuckDB CSV output (with header) into sorted list of tuples."""
    lines = csv_text.strip().split("\n")
    if len(lines) < 2:
        return []
    rows = []
    for line in lines[1:]:
        vals = []
        for v in line.split(","):
            v = v.strip()
            try:
                vals.append(int(v))
            except ValueError:
                try:
                    vals.append(round(float(v), 6))
                except ValueError:
                    vals.append(v)
        rows.append(tuple(vals))
    return sorted(rows)


# --- test cases ---

TESTS = []

def test_case(name, setup_sql, pg_setup_sql, query):
    """Register a test case."""
    TESTS.append((name, setup_sql, pg_setup_sql, query))


# 1. SELECT * — plain ReadRel
test_case(
    "SELECT * (ReadRel)",
    setup_sql="CREATE TABLE t1(id INTEGER, name VARCHAR);\n"
              "INSERT INTO t1 VALUES (1,'alice'),(2,'bob'),(3,'charlie');",
    pg_setup_sql="DROP TABLE IF EXISTS t1;\n"
                 "CREATE TABLE t1(id INTEGER, name VARCHAR(50));\n"
                 "INSERT INTO t1 VALUES (1,'alice'),(2,'bob'),(3,'charlie');",
    query="SELECT * FROM t1",
)

# 2. count(*) — AggregateRel
test_case(
    "count(*) (AggregateRel)",
    setup_sql="CREATE TABLE t2(x INTEGER);\n"
              "INSERT INTO t2 VALUES (1),(2),(3),(4),(5);",
    pg_setup_sql="DROP TABLE IF EXISTS t2;\n"
                 "CREATE TABLE t2(x INTEGER);\n"
                 "INSERT INTO t2 VALUES (1),(2),(3),(4),(5);",
    query="SELECT count(*) FROM t2",
)

# 3. GROUP BY — AggregateRel with grouping
test_case(
    "GROUP BY with count (AggregateRel)",
    setup_sql="CREATE TABLE t3(color VARCHAR);\n"
              "INSERT INTO t3 VALUES ('red'),('blue'),('red'),('green'),('blue'),('red');",
    pg_setup_sql="DROP TABLE IF EXISTS t3;\n"
                 "CREATE TABLE t3(color VARCHAR(50));\n"
                 "INSERT INTO t3 VALUES ('red'),('blue'),('red'),('green'),('blue'),('red');",
    query="SELECT color, count(*) FROM t3 GROUP BY color",
)

# 4. Multiple aggregates — sum, avg, min, max
test_case(
    "sum/avg/min/max (multiple aggregates)",
    setup_sql="CREATE TABLE t4(x INTEGER);\n"
              "INSERT INTO t4 VALUES (10),(20),(30),(40),(50);",
    pg_setup_sql="DROP TABLE IF EXISTS t4;\n"
                 "CREATE TABLE t4(x INTEGER);\n"
                 "INSERT INTO t4 VALUES (10),(20),(30),(40),(50);",
    query="SELECT sum(x), avg(x), min(x), max(x) FROM t4",
)


def run_test(name, setup_sql, pg_setup_sql, query):
    print(f"  {name} ... ", end="", flush=True)

    # 1. DuckDB baseline
    duckdb_csv = duckdb_query_csv(setup_sql, query)
    expected = duckdb_csv_to_rows(duckdb_csv)

    # 2. Get substrait plan from DuckDB
    plan = duckdb_get_substrait(setup_sql, query)

    # 3. Setup PG table
    psql_run(pg_setup_sql)

    # 4. Execute via Flight SQL
    try:
        result_table = flight_exec_substrait(plan)
        actual = table_to_sorted_rows(result_table)
    except Exception as e:
        print(f"FAIL (Flight SQL error: {e})")
        return False

    # 5. Compare
    if expected == actual:
        print("OK")
        return True
    else:
        print("FAIL")
        print(f"    expected: {expected}")
        print(f"    actual:   {actual}")
        return False


def cleanup_pg():
    """Drop test tables."""
    psql_run("DROP TABLE IF EXISTS t1, t2, t3, t4;")


def main():
    print("Substrait integration tests")
    print("=" * 40)

    passed = 0
    failed = 0
    for name, setup_sql, pg_setup_sql, query in TESTS:
        if run_test(name, setup_sql, pg_setup_sql, query):
            passed += 1
        else:
            failed += 1

    cleanup_pg()

    print("=" * 40)
    print(f"{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
