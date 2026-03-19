# Apache Arrow Flight SQL + Substrait Adapter for PostgreSQL

PostgreSQL extension that receives [Substrait][substrait] plans over [Arrow Flight SQL][flight-sql], converts them to PostgreSQL Query trees, executes via `standard_planner`, and returns Arrow record batches.

```
Flight SQL gRPC → Substrait protobuf → PG Query* → standard_planner → executor → Arrow batches
```

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

## Configuration

Add to `postgresql.conf`:

```
shared_preload_libraries = 'arrow_flight_sql'
```

| GUC | Default | Description |
|---|---|---|
| `arrow_flight_sql.uri` | `grpc://127.0.0.1:15432` | Flight SQL endpoint URI |
| `arrow_flight_sql.session_timeout` | `300` (seconds) | Max session duration (-1 = no timeout) |
| `arrow_flight_sql.max_n_rows_per_record_batch` | `1048576` | Max rows per Arrow record batch |

## Testing

TPC-H (22 queries, 22 pass) and TPC-DS (99 queries, 98 pass).

Ground truth: psql CSV output vs Flight SQL adapter, row-by-row comparison with decimal rounding and null normalization.

```sh
python3 substrait_test/test_substrait.py --benchmark tpch --sf 0.01
python3 substrait_test/test_substrait.py --benchmark tpcds --sf 0.01
```

Scale factors: `0.01`, `0.1`, `1`, `5`, `10`. Data lives in `substrait_test/{benchmark}/data/`. Plans are pre-generated Substrait protobuf via [isthmus-cli][isthmus] in `substrait_test/{benchmark}/plans/`.

## Debug

- **`AFS_DEBUG`**: Auto-enabled in debug builds. Logs `substrait_to_query`, `standard_planner`, and execution timings via `elog(LOG)`.
- **`AFS_EXPLAIN=analyze`**: Set before PG starts. Writes `EXPLAIN ANALYZE` output to `/tmp/afs_explain/`.

## License

Apache License 2.0. See [LICENSE.txt](LICENSE.txt).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

[flight-sql]: https://arrow.apache.org/docs/format/FlightSql.html
[substrait]: https://substrait.io/
[isthmus]: https://github.com/substrait-io/substrait-java
