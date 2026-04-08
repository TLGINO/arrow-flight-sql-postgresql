# Apache Arrow Flight SQL + Substrait Adapter for PostgreSQL

PostgreSQL extension that receives [Substrait][substrait] plans over [Arrow Flight SQL][flight-sql], converts them to PostgreSQL Query trees, executes via `standard_planner`, and returns Arrow record batches.

```
Flight SQL gRPC → Substrait protobuf → PG Query* → standard_planner → executor → Arrow batches
```

## Architecture

![Adapter Architecture](doc/images/architecture.svg)

## Substrait Support

### Relation Types

| Relation | Notes |
|---|---|
| ReadRel | Named tables, virtual tables (VALUES) |
| FilterRel | WHERE clauses |
| ProjectRel | Projections, window functions |
| AggregateRel | GROUP BY, ROLLUP/CUBE (via SetRel), DISTINCT aggregates |
| SortRel | ASC/DESC, NULLS FIRST/LAST |
| FetchRel | LIMIT/OFFSET |
| JoinRel | INNER, LEFT, RIGHT, FULL |
| CrossRel | Flattened to multi-table FROM |
| SetRel | UNION/INTERSECT/EXCEPT, ALL/DISTINCT |

### SQL Features

- Window functions: `rank`, `dense_rank`, `row_number`, `lag`, `lead`, `first_value`, `last_value`, `ntile`
- Scalar and correlated subqueries, `EXISTS`, `IN`
- CTE deduplication (auto-detected duplicate subtrees)
- `CASE`/`WHEN`, `COALESCE`, `CAST`
- Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV`, `VARIANCE`, `BOOL_AND`/`OR`

## Build

Dependencies: Meson >= 1.1.0, C++17, Arrow Flight SQL C++, protobuf, PostgreSQL >= 15.

```sh
meson setup builddir
ninja -C builddir
sudo ninja -C builddir install
```

Installs `arrow_flight_sql.so` into the PostgreSQL `pkglibdir`.

## Quick Start

### 1. Load the Extension

Add to `postgresql.conf` and restart PostgreSQL:

```
shared_preload_libraries = 'arrow_flight_sql'
```

The adapter starts a gRPC server on `grpc://127.0.0.1:15432`. Authentication uses `pg_hba.conf` — set the Flight SQL client IP to `password` or `trust` (SCRAM/MD5 not yet supported).

### 2. Run a SQL Query

```python
import pyarrow.flight as flight

client = flight.FlightClient("grpc://127.0.0.1:15432")
token = client.authenticate_basic_token("postgres", "")
opts = flight.FlightCallOptions(headers=[token])

# protobuf wire helpers (tag + length-delimited field)
def varint(v):
    r = []
    while v > 0x7F:
        r.append((v & 0x7F) | 0x80); v >>= 7
    r.append(v & 0x7F)
    return bytes(r)

def pb(n, data):
    return varint((n << 3) | 2) + varint(len(data)) + data

sql = "SELECT 1 AS n, 'hello' AS greeting"
cmd = pb(1, b"type.googleapis.com/arrow.flight.protocol.sql"
            b".CommandStatementQuery") \
    + pb(2, pb(1, sql.encode()))

info = client.get_flight_info(flight.FlightDescriptor.for_command(cmd), opts)
reader = client.do_get(info.endpoints[0].ticket, opts)
print(reader.read_all().to_pandas())
#    n greeting
# 0  1    hello
```

### 3. Execute a Substrait Plan

```python
# read a pre-generated Substrait plan (binary protobuf)
with open("substrait_test/tpch/plans/01.proto", "rb") as f:
    plan_bytes = f.read()

# wrap in Flight SQL CommandStatementSubstraitPlan
cmd = pb(1, b"type.googleapis.com/arrow.flight.protocol.sql"
            b".CommandStatementSubstraitPlan") \
    + pb(2, pb(1, pb(1, plan_bytes)))

info = client.get_flight_info(flight.FlightDescriptor.for_command(cmd), opts)
reader = client.do_get(info.endpoints[0].ticket, opts)
table = reader.read_all()
print(f"{table.num_rows} rows, {table.num_columns} cols")
print(table.to_pandas().head())
```

Plans are pre-generated Substrait protobuf via [isthmus-cli][isthmus]. See `substrait_test/{tpch,tpcds}/plans/` for all TPC-H and TPC-DS plans.

## Configuration

Add to `postgresql.conf`:

```
shared_preload_libraries = 'arrow_flight_sql'
```

| GUC | Default | Description |
|---|---|---|
| `arrow_flight_sql.uri` | `grpc://127.0.0.1:15432` | Flight SQL endpoint URI |
| `arrow_flight_sql.session_timeout` | `300` (seconds) | Max session duration (-1 = no timeout) |
| `arrow_flight_sql.max_sessions` | `256` | Max concurrent sessions (-1 = unlimited) |
| `arrow_flight_sql.ring_buffer_size` | `8MB` | Shared ring buffer per session (1MB-1GB) |
| `arrow_flight_sql.max_n_rows_per_record_batch` | `1048576` | Max rows per Arrow record batch |
| `arrow_flight_sql.ipc_compression` | `false` | ZSTD compression for Arrow IPC batches |
| `arrow_flight_sql.explain` | `0` | EXPLAIN mode: 0=off, 1=plan only, 2=analyze |
| `arrow_flight_sql.explain_dir` | `/tmp/afs_explain` | Directory for EXPLAIN JSON output |

## Testing

![Test Architecture](doc/images/test-architecture.svg)

Correctness is verified by running each query through three independent execution paths and comparing results row-by-row:

1. **PG ground truth** — native `psql` CSV output (the reference)
2. **Arrow Flight SQL** — same SQL sent over Flight SQL, returned as Arrow batches
3. **Substrait** — pre-generated binary Substrait plan sent over Flight SQL

All three paths must produce identical results after normalization (floats rounded to 4 decimals, dates to ISO, nulls unified). The `--run` bitmask selects which paths to execute: `4`=pgsql, `2`=arrow, `1`=substrait (default `5`=pgsql+substrait, `7`=all three).

TPC-H: 22 queries, 22 pass. TPC-DS: 99 queries, 98 pass.

```sh
python3 substrait_test/test_substrait.py --benchmark tpch --sf 1          # pgsql + substrait
python3 substrait_test/test_substrait.py --benchmark tpch --sf 1 --run 7  # all three paths
python3 substrait_test/test_substrait.py --benchmark tpcds --sf 1
```

Scale factors: `1`, `5`, `10`, `15`, `20`. Data in `substrait_test/{benchmark}/data/`, plans (pre-generated via [isthmus-cli][isthmus]) in `substrait_test/{benchmark}/plans/`.

### EXPLAIN Plan Comparison

Generate JSON explain plans for all three execution paths (pgsql, arrow, substrait) and visualize side-by-side:

```sh
python3 substrait_test/test_substrait.py --explain 7 --benchmark tpch --sf 1
python3 substrait_test/plot_explain.py tpch
# open substrait_test/explain/tpch.html
```

`--explain` bitmask: 4=pgsql, 2=arrow, 1=substrait (7=all). Sets `--run` automatically when `--run` not given. Plans are written as individual JSON files per query per method in `substrait_test/explain/{benchmark}_sf{sf}/`. The HTML viewer shows query tabs, SF sub-tabs, and parallel worker badges (G=gather, W=worker).

### Performance

```sh
scripts/gen_all_perf.sh        # run all SFs, all methods
scripts/gen_perf.sh            # subset of queries
python3 substrait_test/plot_tpch_performance.py
```

## Debug

- **`AFS_DEBUG`**: Auto-enabled in debug builds. Logs `substrait_to_query`, `standard_planner`, and execution timings via `elog(LOG)`.

## License

Apache License 2.0. See [LICENSE.txt](LICENSE.txt).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

[flight-sql]: https://arrow.apache.org/docs/format/FlightSql.html
[substrait]: https://substrait.io/
[isthmus]: https://github.com/substrait-io/substrait-java
