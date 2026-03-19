#!/usr/bin/env python3
"""
Substrait integration tests — TPC-H and TPC-DS.

Ground truth: SQL on PostgreSQL via psql.
Substrait execution: PG Flight SQL adapter.

Run:   python3 substrait_test/test_substrait.py [--benchmark tpch|tpcds] [--sf SF] [query_numbers...]
"""

import argparse
import csv
import decimal
import os
import subprocess
import sys
import tempfile
import threading
import time

import psutil
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

SF_DIR_MAP = {"0.01": "sf001", "0.1": "sf010", "1": "sf100",
              "5": "sf500", "10": "sf1000"}

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

LINEITEM_ROWS = {"0.01": 60175, "0.1": 600572, "1": 6001215,
                 "5": 29999795, "10": 59986052}

TPCDS_TABLES = [
    "dbgen_version", "customer_address", "customer_demographics",
    "date_dim", "warehouse", "ship_mode", "time_dim", "reason",
    "income_band", "item", "store", "call_center", "customer",
    "web_site", "store_returns", "household_demographics", "web_page",
    "promotion", "catalog_page", "inventory", "catalog_returns",
    "web_returns", "web_sales", "catalog_sales", "store_sales",
]

TPCDS_SKIP = set()  # previously skipped GROUPING queries (27,36,70,86)

STORE_SALES_ROWS = {"0.01": 28810, "0.1": 288464, "1": 2880404}

BENCHMARKS = {
    "tpch": {
        "dir": os.path.join(SCRIPT_DIR, "tpch"),
        "schema": "tpch",
        "tables": TPCH_TABLES,
        "data_ext": ".tbl",
        "num_queries": 22,
        "requires": TPCH_REQUIRES,
        "skip": set(),
        "check_table": "lineitem",
        "check_col": "l_orderkey",
        "check_rows": LINEITEM_ROWS,
    },
    "tpcds": {
        "dir": os.path.join(SCRIPT_DIR, "tpcds"),
        "schema": "tpcds",
        "tables": TPCDS_TABLES,
        "data_ext": ".dat",
        "num_queries": 99,
        "requires": {},
        "skip": TPCDS_SKIP,
        "check_table": "store_sales",
        "check_col": "ss_item_sk",
        "check_rows": STORE_SALES_ROWS,
    },
}


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
            stderr=subprocess.PIPE, text=True)
    return _flight_helper


def _stop_flight_helper():
    global _flight_helper
    if _flight_helper is not None and _flight_helper.poll() is None:
        _flight_helper.stdin.write("\n")
        _flight_helper.stdin.flush()
        _flight_helper.wait(timeout=5)
        _flight_helper = None


def flight_exec_sql(sql):
    with tempfile.NamedTemporaryFile(suffix=".sql", delete=False, mode="w") as sf:
        sf.write(sql)
        sql_path = sf.name
    out_path = sql_path + ".arrow"

    proc = _get_flight_helper()
    try:
        proc.stdin.write(f"SQL {sql_path} {out_path}\n")
        proc.stdin.flush()
        resp = proc.stdout.readline().strip()
        if resp.startswith("ERR "):
            raise RuntimeError(resp[4:])
        if resp != "OK":
            raise RuntimeError(f"unexpected response: {resp}")
        reader = ipc.open_file(out_path)
        return reader.read_all()
    finally:
        for p in (sql_path, out_path):
            try:
                os.unlink(p)
            except OSError:
                pass


def flight_exec_substrait(plan_bytes):
    with tempfile.NamedTemporaryFile(suffix=".proto", delete=False) as pf:
        pf.write(plan_bytes)
        plan_path = pf.name
    out_path = plan_path + ".arrow"

    proc = _get_flight_helper()
    try:
        proc.stdin.write(f"{plan_path} {out_path}\n")
        proc.stdin.flush()
        resp = proc.stdout.readline().strip()
        if resp.startswith("ERR "):
            raise RuntimeError(resp[4:])
        if resp != "OK":
            raise RuntimeError(f"unexpected response: {resp}")
        reader = ipc.open_file(out_path)
        return reader.read_all()
    finally:
        for p in (plan_path, out_path):
            try:
                os.unlink(p)
            except OSError:
                pass


# ============================================================
# Resource monitoring (PG server process)
# ============================================================

_monitor = None
_monitor_log = []  # list of (qnr, rows, peak_rss_bytes, peak_cpu, status)


def _find_pg_pid():
    for p in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            if p.info["name"] == "postgres":
                cmd = p.info["cmdline"] or []
                if any("-D" in a for a in cmd):
                    return p.info["pid"]
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return None


class ResourceMonitor:
    def __init__(self, pid):
        self.proc = psutil.Process(pid)
        self.ncpus = psutil.cpu_count() or 1
        self.peak_rss = 0
        self.peak_cpu = 0.0
        self.overall_peak_rss = 0
        self.overall_peak_cpu = 0.0
        self._running = False
        self._thread = None

    def start(self):
        self.peak_rss = 0
        self.peak_cpu = 0.0
        self.proc.cpu_percent()  # prime the counter
        self._running = True
        self._thread = threading.Thread(target=self._poll, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join()
        self.overall_peak_rss = max(self.overall_peak_rss, self.peak_rss)
        self.overall_peak_cpu = max(self.overall_peak_cpu, self.peak_cpu)
        return self.peak_rss, self.peak_cpu

    def _poll(self):
        # Keep Process objects alive across polls so cpu_percent() has a
        # previous measurement to diff against (first call always returns 0).
        tracked = {}  # pid -> psutil.Process
        while self._running:
            try:
                pids = {self.proc.pid}
                for c in self.proc.children(recursive=True):
                    pids.add(c.pid)
                # add new, remove dead
                for pid in pids - tracked.keys():
                    try:
                        p = psutil.Process(pid)
                        p.cpu_percent()  # prime
                        tracked[pid] = p
                    except psutil.NoSuchProcess:
                        pass
                for pid in list(tracked.keys() - pids):
                    del tracked[pid]

                mem = 0
                cpu = 0.0
                for p in tracked.values():
                    try:
                        mem += p.memory_info().rss
                        cpu += p.cpu_percent()
                    except psutil.NoSuchProcess:
                        pass
                cpu /= self.ncpus  # normalize to 0-100%
                self.peak_rss = max(self.peak_rss, mem)
                self.peak_cpu = max(self.peak_cpu, cpu)
            except psutil.NoSuchProcess:
                break
            time.sleep(0.1)


def _fmt_mb(nbytes):
    return f"{nbytes / (1024 * 1024):.1f} MB"


# ============================================================
# Ground truth & Substrait execution
# ============================================================

def ground_truth_pg(bench, qnr, arrow=False):
    pgsql_dir = os.path.join(bench["dir"], "queries", "pgsql")
    query_file = os.path.join(pgsql_dir, f"{qnr:02d}.sql")
    with open(query_file) as f:
        sql = f.read().strip()
    if sql.endswith(";"):
        sql = sql[:-1]
    if arrow:
        sql_with_path = f"SET search_path TO {bench['schema']};\n{sql}"
        return table_to_rows(flight_exec_sql(sql_with_path))
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


def load_data(bench, sf):
    key = bench["schema"]
    if key in _loaded:
        return

    _ensure_search_path(bench)

    if _already_loaded(bench, sf):
        print(f"  {key} sf={sf} already loaded (cached)")
        _loaded[key] = True
        return

    print(f"  Loading {key} sf={sf} into PostgreSQL ... ", end="", flush=True)

    sf_dir = SF_DIR_MAP[sf]
    data_dir = os.path.join(bench["dir"], "data", sf_dir)
    init_sql = os.path.join(bench["dir"], "pg_init.sql")

    cmd = PSQL_CMD + ["-f", init_sql]
    r = subprocess.run(cmd, capture_output=True)
    if r.returncode != 0:
        raise RuntimeError(f"pg_init.sql failed:\n{r.stderr.decode()}")

    schema = bench["schema"]
    ext = bench["data_ext"]

    for table in bench["tables"]:
        data_file = os.path.join(data_dir, f"{table}{ext}")
        if not os.path.exists(data_file):
            raise RuntimeError(f"Missing data file: {data_file}")
        copy_sql = (f"\\COPY {schema}.{table} FROM '{data_file}' "
                    f"WITH (FORMAT csv, DELIMITER '|', NULL '');")
        psql_run(copy_sql)

    for table in bench["tables"]:
        psql_run(f"ANALYZE {schema}.{table};")

    _loaded[key] = True
    print("done")


# ============================================================
# Test runner (generic)
# ============================================================

_results = {"pass": 0, "fail": 0, "skip": 0, "error": 0}


def _has_order_by(bench, qnr):
    pgsql_dir = os.path.join(bench["dir"], "queries", "pgsql")
    query_file = os.path.join(pgsql_dir, f"{qnr:02d}.sql")
    with open(query_file) as f:
        return "order by" in f.read().lower()


def run_query(bench, qnr, explain=False, arrow=False):
    plans_dir = os.path.join(bench["dir"], "plans")
    plan_file = os.path.join(plans_dir, f"{qnr:02d}.proto")
    with open(plan_file, "rb") as f:
        plan_bytes = f.read()

    ordered = _has_order_by(bench, qnr)

    t0 = time.monotonic()
    expected = ground_truth_pg(bench, qnr, arrow=arrow)
    pg_time_s = time.monotonic() - t0
    gt_label = "arrow" if arrow else "pg"
    print(f"  {gt_label}={pg_time_s:.3f}s ({len(expected)} rows), running substrait...",
          end=" ", flush=True)

    if explain:
        pgsql_dir = os.path.join(bench["dir"], "queries", "pgsql")
        query_file = os.path.join(pgsql_dir, f"{qnr:02d}.sql")
        with open(query_file) as f:
            sql = f.read().strip().rstrip(";")
        explain_out = psql_run(
            f"SET search_path TO {bench['schema']};\n"
            f"EXPLAIN (ANALYZE, FORMAT TEXT) {sql};")
        print(f"\n  -- PG EXPLAIN --\n{explain_out}")

    if _monitor:
        _monitor.start()
    try:
        t1 = time.monotonic()
        actual = substrait_pg(plan_bytes)
        substrait_time_s = time.monotonic() - t1
    except Exception as e:
        substrait_time_s = time.monotonic() - t1
        if _monitor:
            peak_rss, peak_cpu = _monitor.stop()
            _monitor_log.append((qnr, 0, peak_rss, peak_cpu, "FAIL",
                                 pg_time_s, substrait_time_s))
        diff_s = substrait_time_s - pg_time_s
        print(f"FAIL (adapter error: {e}) "
              f"sub={substrait_time_s:.3f}s diff={diff_s:+.3f}s")
        _results["fail"] += 1
        return

    peak_rss = peak_cpu = 0
    if _monitor:
        peak_rss, peak_cpu = _monitor.stop()

    diff_s = substrait_time_s - pg_time_s
    ok, diff = rows_equal(expected, actual, ordered=ordered)
    if ok:
        print(f"PASS sub={substrait_time_s:.3f}s diff={diff_s:+.3f}s")
        _results["pass"] += 1
        if _monitor:
            _monitor_log.append((qnr, len(expected), peak_rss, peak_cpu, "PASS",
                                 pg_time_s, substrait_time_s))
    else:
        print(f"FAIL ({diff}) sub={substrait_time_s:.3f}s diff={diff_s:+.3f}s")
        _results["fail"] += 1
        if _monitor:
            _monitor_log.append((qnr, len(expected), peak_rss, peak_cpu, "FAIL",
                                 pg_time_s, substrait_time_s))


def run_benchmark(bench, sf, queries=None, explain=False, arrow=False):
    name = bench["schema"].upper()
    num_queries = bench["num_queries"]
    requires = bench["requires"]
    skip = bench["skip"]

    print(f"\n{name}")
    print("-" * 60)

    if queries is None:
        queries = list(range(1, num_queries + 1))

    any_runnable = any(
        q not in skip and not (requires.get(q, set()) - IMPLEMENTED)
        for q in queries
    )
    if any_runnable:
        load_data(bench, sf)

    for qnr in queries:
        print(f"\n{name} Q{qnr:02d}")

        if qnr in skip:
            print(f"  SKIP (no plan)")
            _results["skip"] += 1
            continue

        required = requires.get(qnr, set())
        missing = required - IMPLEMENTED
        if missing:
            print(f"  SKIP (need {missing})")
            _results["skip"] += 1
            continue

        try:
            run_query(bench, qnr, explain=explain, arrow=arrow)
        except Exception as e:
            print(f"  ERROR: {e}")
            _results["error"] += 1


# ============================================================
# Main
# ============================================================

def main():
    global _monitor

    parser = argparse.ArgumentParser(description="Substrait integration tests")
    parser.add_argument("--benchmark", default="tpch",
                        choices=BENCHMARKS.keys(),
                        help="Benchmark to run (default: tpch)")
    parser.add_argument("--sf", default="0.01", choices=SF_DIR_MAP.keys(),
                        help="Scale factor (default: 0.01)")
    parser.add_argument("--monitor", action="store_true",
                        help="Log peak memory/CPU of PG server per query")
    parser.add_argument("--explain", action="store_true",
                        help="Print EXPLAIN ANALYZE for PG ground truth queries")
    parser.add_argument("--arrow", action="store_true",
                        help="Run ground truth SQL via Flight SQL adapter instead of psql")
    parser.add_argument("queries", nargs="*", type=int,
                        help="query numbers to run (default: all)")
    args = parser.parse_args()

    if args.monitor:
        pg_pid = _find_pg_pid()
        if pg_pid is None:
            print("WARNING: could not find PG server process, disabling monitor")
        else:
            _monitor = ResourceMonitor(pg_pid)
            print(f"Monitoring PG server pid={pg_pid}")

    bench = BENCHMARKS[args.benchmark]
    queries = args.queries or None

    if args.explain:
        os.environ["AFS_EXPLAIN"] = "1"

    print("Substrait integration tests")
    print("=" * 60)
    print(f"Benchmark: {args.benchmark}")
    print(f"Implemented: {sorted(IMPLEMENTED)}")
    print(f"Scale factor: {args.sf}")
    if args.arrow:
        print("Ground truth: Flight SQL (--arrow)")

    run_benchmark(bench, args.sf, queries, explain=args.explain, arrow=args.arrow)

    _stop_flight_helper()

    p, f, s, e = _results["pass"], _results["fail"], \
                  _results["skip"], _results["error"]
    print("\n" + "=" * 60)
    print(f"{p} passed, {f} failed, {s} skipped, {e} errors")

    if _monitor and _monitor_log:
        csv_path = os.path.join(SCRIPT_DIR,
                                f"monitor_{args.benchmark}_sf{args.sf}.csv")
        with open(csv_path, "w", newline="") as cf:
            w = csv.writer(cf)
            w.writerow(["sf", "query", "status", "rows",
                         "peak_rss_mb", "peak_cpu_pct",
                         "pg_time_s", "substrait_time_s", "diff_s"])
            for qnr, rows, rss, cpu, status, pg_t, sub_t in _monitor_log:
                w.writerow([args.sf, f"Q{qnr:02d}", status, rows,
                            f"{rss / (1024*1024):.1f}", f"{cpu:.0f}",
                            f"{pg_t:.3f}", f"{sub_t:.3f}",
                            f"{sub_t - pg_t:+.3f}"])

        print(f"\n{'Query':<8} {'Status':<8} {'Rows':>8} "
              f"{'Peak RSS':>10} {'Peak CPU':>10} "
              f"{'PG Time':>9} {'Sub Time':>9} {'Diff':>9}")
        print("-" * 75)
        for qnr, rows, rss, cpu, status, pg_t, sub_t in _monitor_log:
            print(f"Q{qnr:02d}      {status:<8} {rows:>8} "
                  f"{_fmt_mb(rss):>10} {cpu:>9.0f}% "
                  f"{pg_t:>8.3f}s {sub_t:>8.3f}s {sub_t - pg_t:>+8.3f}s")
        print("-" * 75)
        print(f"{'TOTAL':<8} {'':8} {'':>8} "
              f"{_fmt_mb(_monitor.overall_peak_rss):>10} "
              f"{_monitor.overall_peak_cpu:>9.0f}%")
        print(f"\nCSV written to {csv_path}")

    sys.exit(0 if f == 0 and e == 0 else 1)


if __name__ == "__main__":
    main()
